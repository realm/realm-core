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
#include "results.hpp"
#include "object_schema.hpp"
#include "util/event_loop_signal.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/file.hpp>
#include <realm/util/uri.hpp>
#include <realm/lang_bind_helper.hpp>

#include <regex>
#include <utility>
#include <stdexcept>
#include <vector>

using namespace realm;
using namespace realm::_impl;

GlobalNotifier::GlobalNotifier(std::unique_ptr<Callback> async_target,
                               std::string local_root_dir, std::string server_base_url,
                               std::shared_ptr<SyncUser> user, std::shared_ptr<ChangesetTransformer> transformer)
: m_admin(local_root_dir, server_base_url, user)
, m_target(std::move(async_target))
, m_server_base_url(std::move(server_base_url))
, m_user(std::move(user))
, m_regular_realms_dir(util::File::resolve("realms", local_root_dir)) // Throws
, m_transformer(transformer)
{
    util::try_make_dir(m_regular_realms_dir); // Throws
}

std::shared_ptr<GlobalNotifier> GlobalNotifier::shared_notifier(std::unique_ptr<Callback> callback, std::string local_root_dir,
                                                                           std::string server_base_url, std::shared_ptr<SyncUser> user,
                                                                           std::shared_ptr<ChangesetTransformer> transformer) {
    auto notifier = std::shared_ptr<GlobalNotifier>(new GlobalNotifier(std::move(callback), local_root_dir, server_base_url, user, transformer));
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
    m_admin.start([this](auto&& realms) {
        this->register_realms(std::move(realms));
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
        auto& sg2 = Realm::Internal::get_shared_group(*realm2);

        Group const& g = sg2.begin_read(sg.get_version_of_current_transaction());

        std::unordered_map<std::string, CollectionChangeSet> changes;
        if (!m_transformer) {
            _impl::TransactionChangeInfo info;
            info.track_all = true;
            _impl::transaction::advance(sg2, info, next.target_version);

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
            if (changes.empty()) // && !realm.read_group().is_empty())
                continue; // nothing to notify about
        }
        else {
            //LangBindHelper::advance_read(sg2, next.target_version);
        }

        std::lock_guard<std::mutex> l2(m_deliver_queue_mutex);
        m_pending_deliveries.push({
            next.info,
            sg.get_version_of_current_transaction(),
            next.target_version,
            std::move(next.realm),
            std::move(changes)
        });
        m_signal->notify();
    }
}

Realm::Config GlobalNotifier::get_config(std::string realm_path)
{
    Realm::Config config;
    auto realm_id = m_realm_ids[realm_path];
    config.path =  util::File::resolve(realm_id + ".realm", m_regular_realms_dir);
    config.sync_config = std::shared_ptr<SyncConfig>(
        new SyncConfig{m_user, m_server_base_url + realm_path.data(), SyncSessionStopPolicy::AfterChangesUploaded,
            [&](auto, const auto& config, auto session) {
                session->bind_with_admin_token(config.user->refresh_token(), config.realm_url);
            }, 
            [&](auto, auto) {
            }, 
            m_transformer}
    );
    config.schema_mode = SchemaMode::Additive;
    config.cache = false;
    return config;
}

void GlobalNotifier::register_realm(RealmInfo info) {
    auto config = get_config(info.second);
    auto coordinator = _impl::RealmCoordinator::get_coordinator(config);
    m_listen_entries[info.first] = coordinator;

    auto realm = Realm::make_shared_realm(std::move(config));
    if (realm->read_group().is_empty())
        realm = nullptr;

    auto unowned_coordinator = coordinator.get();

    std::weak_ptr<GlobalNotifier> weak_self = shared_from_this();
    coordinator->set_transaction_callback([weak_self, unowned_coordinator, info](VersionID old_version, VersionID new_version) {
        auto config = unowned_coordinator->get_config();
        config.schema = util::none;
        auto realm = Realm::make_shared_realm(std::move(config));
        Realm::Internal::begin_read(*realm, old_version);
        REALM_ASSERT(!realm->config().schema);

        if (auto self = weak_self.lock()) {
            std::lock_guard<std::mutex> l(self->m_work_queue_mutex);
            self->m_work_queue.push(RealmToCalculate{info, std::move(realm), new_version});
            self->m_work_queue_cv.notify_one();
        }
    });
}

void GlobalNotifier::register_realms(std::vector<AdminRealmListener::RealmInfo> realms)
{
    std::vector<bool> new_realms;
    for (auto &realm_info : realms) {
        m_realm_ids[realm_info.second] = realm_info.first;
    }

    std::vector<bool> monitor = m_target->available(realms);

    for (size_t i = 0; i < realms.size(); i++) {
        if (monitor[i] && m_listen_entries.count(realms[i].first) == 0) {
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

GlobalNotifier::ChangeNotification::ChangeNotification(RealmInfo info,
                                                       VersionID old_version,
                                                       VersionID new_version,
                                                       SharedRealm realm,
                                                       std::unordered_map<std::string, CollectionChangeSet> changes)
: realm_info(info)
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

std::string GlobalNotifier::ChangeNotification::get_path() const
{
    return util::Uri(m_realm->config().sync_config->realm_url).get_path();
}

GlobalNotifier::Callback::~Callback() = default;
