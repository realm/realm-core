////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "global_notifier.hpp"

#include "impl/realm_coordinator.hpp"
#include "impl/transact_log_handler.hpp"
#include "object_store.hpp"
#include "object_schema.hpp"
#include "util/event_loop_signal.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/file.hpp>

#include <utility>
#include <stdexcept>
#include <vector>

using namespace realm;
using namespace realm::_impl;

GlobalNotifier::GlobalNotifier(std::unique_ptr<Callback> async_target,
                               std::string local_root_dir, std::string server_base_url,
                               std::shared_ptr<SyncUser> user, std::function<SyncBindSessionHandler> bind_callback,
                               std::shared_ptr<ChangesetTransformer> transformer)
: m_admin(std::make_shared<AdminRealmListener>(local_root_dir, server_base_url, user, bind_callback))
, m_target(std::move(async_target))
, m_server_base_url(std::move(server_base_url))
, m_user(std::move(user))
, m_bind_callback(std::move(bind_callback))
, m_regular_realms_dir(util::File::resolve("realms", local_root_dir)) // Throws
, m_transformer(transformer)
{
    util::try_make_dir(m_regular_realms_dir); // Throws
}

std::shared_ptr<GlobalNotifier> GlobalNotifier::shared_notifier(std::unique_ptr<Callback> callback, std::string local_root_dir,
                                                                std::string server_base_url, std::shared_ptr<SyncUser> user,
                                                                std::function<SyncBindSessionHandler> bind_callback,
                                                                std::shared_ptr<ChangesetTransformer> transformer) {
    auto notifier = std::shared_ptr<GlobalNotifier>(new GlobalNotifier(std::move(callback), local_root_dir, server_base_url, user, std::move(bind_callback), transformer));
    notifier->m_signal = std::make_shared<util::EventLoopSignal<SignalCallback>>(SignalCallback{std::weak_ptr<GlobalNotifier>(notifier), &GlobalNotifier::on_change});
    return notifier;
}

GlobalNotifier::~GlobalNotifier()
{
    {
        std::unique_lock<std::mutex> l(m_work_queue_mutex);
        m_shutdown = true;
        m_work_queue_cv.notify_all();
    }
    if (m_work_thread.joinable()) // will be false if `start()` was never called
        m_work_thread.join();
}

void GlobalNotifier::start()
{
    std::weak_ptr<GlobalNotifier> weak_self = shared_from_this();
    m_admin->start([weak_self](auto&& realms) {
        if (auto self = weak_self.lock())
            self->register_realms(std::move(realms));
    });
    if (!m_work_thread.joinable()) {
        m_work_thread = std::thread([this] { calculate(); });
    }
}

void GlobalNotifier::calculate()
{
    while (true) {
        std::unique_lock<std::mutex> l(m_work_queue_mutex);
        m_work_queue_cv.wait(l, [=] { return m_shutdown || !m_work_queue.empty(); });
        if (m_shutdown)
            return;

        auto next = std::move(m_work_queue.front());
        m_work_queue.pop();
        l.unlock();

        auto& realm = *next.realm;
        auto& sg = Realm::Internal::get_shared_group(realm);

        auto config = realm.config();
        config.cache = false;
        auto realm2 = Realm::make_shared_realm(config);
        Realm::Internal::begin_read(*realm2, sg->get_version_of_current_transaction());
        auto& sg2 = Realm::Internal::get_shared_group(*realm2);
        Group const& g = realm2->read_group();

        std::unordered_map<std::string, CollectionChangeSet> changes;
        if (!m_transformer) {
            _impl::TransactionChangeInfo info;
            info.track_all = true;
            _impl::transaction::advance(*sg2, info, next.target_version);

            changes.reserve(info.tables.size());
            for (size_t i = 0; i < info.tables.size(); ++i) {
                auto& change = info.tables[i];
                if (!change.empty()) {
                    auto name = ObjectStore::object_type_for_table_name(g.get_table_name(i));
                    if (name) {
                        changes[name] = std::move(change).finalize();
                    }
                }
            }
            if (changes.empty())
                continue; // nothing to notify about
        }

        std::lock_guard<std::mutex> l2(m_deliver_queue_mutex);
        m_pending_deliveries.push({
            next.path,
            sg->get_version_of_current_transaction(),
            next.target_version,
            std::move(next.realm),
            std::move(changes)
        });
        m_signal->notify();
    }
}

realm::Realm::Config GlobalNotifier::get_config(std::string const& path, util::Optional<Schema> schema) {
    Realm::Config config;
    if (schema) {
        config.schema = std::move(schema);
        config.schema_version = 0;
    }

    std::string file_path = m_regular_realms_dir + path + ".realm";
    for (size_t pos = m_regular_realms_dir.size(); pos != file_path.npos; pos = file_path.find('/', pos + 1)) {
        file_path[pos] = '\0';
        util::try_make_dir(file_path);
        file_path[pos] = '/';
    }

    config.path = std::move(file_path);
    config.sync_config = std::shared_ptr<SyncConfig>(
        new SyncConfig{m_user, m_server_base_url + path.data(), SyncSessionStopPolicy::AfterChangesUploaded,
                       m_bind_callback, nullptr, m_transformer}
    );
    config.schema_mode = SchemaMode::Additive;
    config.cache = false;
    return config;
}

void GlobalNotifier::register_realm(const std::string& path) {
    auto config = get_config(path);
    auto coordinator = _impl::RealmCoordinator::get_coordinator(config);
    m_listen_entries[path] = coordinator;

    auto realm = coordinator->get_realm(std::move(config));
    if (realm->read_group().is_empty())
        realm = nullptr;

    auto unowned_coordinator = coordinator.get();

    std::weak_ptr<GlobalNotifier> weak_self = shared_from_this();
    coordinator->set_transaction_callback([weak_self, unowned_coordinator, path](VersionID old_version, VersionID new_version) {
        auto config = unowned_coordinator->get_config();
        config.schema = util::none;
        auto realm = Realm::make_shared_realm(std::move(config));
        Realm::Internal::begin_read(*realm, old_version);
        REALM_ASSERT(!realm->config().schema);

        if (auto self = weak_self.lock()) {
            std::lock_guard<std::mutex> l(self->m_work_queue_mutex);
            self->m_work_queue.push(RealmToCalculate{path, std::move(realm), new_version});
            self->m_work_queue_cv.notify_one();
        }
    });
}

void GlobalNotifier::register_realms(std::vector<std::string> realms)
{
    std::vector<bool> monitor = m_target->available(realms);

    for (size_t i = 0; i < realms.size(); i++) {
        if (monitor[i] && m_listen_entries.count(realms[i]) == 0) {
            register_realm(realms[i]);
        }
    }
}

void GlobalNotifier::on_change()
{
    while (!m_waiting) {
        GlobalNotifier::ChangeNotification change;
        {
            std::lock_guard<std::mutex> l(m_deliver_queue_mutex);
            if (m_pending_deliveries.empty())
                return;
            change = std::move(m_pending_deliveries.front());
            m_pending_deliveries.pop();
        }

        m_target->realm_changed(std::move(change));
        // FIXME: needs to actually close the notification pipe at some point
    }
}

void GlobalNotifier::pause()
{
    m_waiting = true;
}

void GlobalNotifier::resume()
{
    m_waiting = false;
    on_change();
}

bool GlobalNotifier::has_pending()
{
    std::lock_guard<std::mutex> l(m_deliver_queue_mutex);
    return !m_pending_deliveries.empty();
}

GlobalNotifier::ChangeNotification::ChangeNotification(std::string path,
                                                       VersionID old_version,
                                                       VersionID new_version,
                                                       SharedRealm realm,
                                                       std::unordered_map<std::string, CollectionChangeSet> changes)
: realm_path(std::move(path))
, m_old_version(old_version)
, m_new_version(new_version)
, m_realm(std::move(realm))
, m_changes(std::move(changes))
{
}

SharedRealm GlobalNotifier::ChangeNotification::get_old_realm() const
{
    if (const_cast<VersionID&>(m_old_version) == VersionID{})
        return nullptr;

    auto config = m_realm->config();
    config.cache = false;
    auto old_realm = Realm::get_shared_realm(std::move(config));
    Realm::Internal::begin_read(*old_realm, m_old_version);
    return old_realm;
}

SharedRealm GlobalNotifier::ChangeNotification::get_new_realm() const
{
    auto config = m_realm->config();
    config.cache = false;
    auto new_realm = Realm::get_shared_realm(std::move(config));
    Realm::Internal::begin_read(*new_realm, m_new_version);
    return new_realm;
}

GlobalNotifier::Callback::~Callback() = default;
