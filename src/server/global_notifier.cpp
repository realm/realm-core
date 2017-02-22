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

AdminRealmManager::AdminRealmManager(std::string local_root, std::string server_base_url, std::shared_ptr<SyncUser> user)
: m_regular_realms_dir(util::File::resolve("realms", local_root)) // Throws
, m_server_base_url(std::move(server_base_url))
, m_user(std::move(user))
{
    util::try_make_dir(m_regular_realms_dir); // Throws

    Realm::Config config;
    config.cache = false;
    config.path = util::File::resolve("__admin.realm", local_root);
    config.schema_mode = SchemaMode::Additive;
    config.sync_config = std::shared_ptr<SyncConfig>(new SyncConfig{m_user, m_server_base_url + "/__admin",
                                                                    SyncSessionStopPolicy::AfterChangesUploaded,
                                                                    bind_sync_session });
    config.schema = Schema{
        {"RealmFile", {
            {"id", PropertyType::String, "", "", true, true, false},
            {"path", PropertyType::String, "", "", false, false, false},
            {"owner", PropertyType::Object, "User", "", false, false, true},
        }},
        {"User", {
            {"id", PropertyType::String, "", "", true, true, false},
            {"accounts", PropertyType::Array, "Account", "", false, false, false},
            {"isAdmin", PropertyType::Bool, "", "", false, false, false}
        }},
        {"Account", {
            {"provider", PropertyType::String, "", "", false, false, false},
            {"provider_id", PropertyType::String, "", "", false, true, false},
            {"data", PropertyType::String, "", "", false, false, true},
            {"tokens", PropertyType::Array, "Token", "", false, false, false},
            {"user", PropertyType::Object, "User", "", false, false, true}
        }},
        {"Token", {
            {"token", PropertyType::String, "", "", true, true, false},
            {"expires", PropertyType::Date, "", "", false, false, false},
            {"revoked", PropertyType::Date, "", "", false, false, true},
            {"files", PropertyType::Array, "RealmFile", "", false, false, false},
            {"account", PropertyType::Object, "Account", "", false, false, true},
            {"app_id", PropertyType::String, "", "", false, false, false},
        }}
    };
    config.schema_version = 2;
    m_realm = Realm::get_shared_realm(std::move(config));
}

void AdminRealmManager::start(std::function<void(std::vector<RealmInfo>, bool)> callback)
{
    m_results = Results(m_realm, *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile"));
    m_notification_token = m_results.add_notification_callback([=](CollectionChangeSet changes, std::exception_ptr) {
        auto& table = *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile");
        size_t id_col_ndx   = table.get_column_index("id");
        size_t name_col_ndx = table.get_column_index("path");
        std::vector<RealmInfo> realms;
        if (changes.empty() || m_first) {
            for (size_t i = 0, size = table.size(); i < size; ++i) {
                realms.emplace_back(table.get_string(id_col_ndx, i), table.get_string(name_col_ndx, i));
            }
            callback(std::move(realms), true);
            m_first = false;
        }
        else {
            for (auto i : changes.insertions.as_indexes()) {
                realms.emplace_back(table.get_string(id_col_ndx, i), table.get_string(name_col_ndx, i));
            }
            callback(std::move(realms), false);
        }
    });
}

Realm::Config AdminRealmManager::get_config(StringData realm_id, StringData realm_name)
{
    Realm::Config config;
    config.path =  util::File::resolve(std::string(realm_id) + ".realm", m_regular_realms_dir);
    config.sync_config = std::shared_ptr<SyncConfig>(new SyncConfig{m_user, m_server_base_url + realm_name.data(),
                                                                    SyncSessionStopPolicy::AfterChangesUploaded,
                                                                    bind_sync_session });
    config.schema_mode = SchemaMode::Additive;
    return config;
}

void AdminRealmManager::create_realm(StringData realm_id, StringData realm_name)
{
    m_realm->begin_transaction();
    auto& table = *ObjectStore::table_for_object_type(m_realm->read_group(), "RealmFile");
    size_t row_ndx      = table.add_empty_row();
    size_t id_col_ndx   = table.get_column_index("id");
    size_t name_col_ndx = table.get_column_index("path");
    table.set_string(id_col_ndx, row_ndx, realm_id);
    table.set_string(name_col_ndx, row_ndx, realm_name);
    m_realm->commit_transaction();
}

void AdminRealmManager::bind_sync_session(const std::string&, const SyncConfig& config, std::shared_ptr<SyncSession> session)
{
    session->bind_with_admin_token(config.user->refresh_token(), config.realm_url);
}

GlobalNotifier::GlobalNotifier(std::unique_ptr<Callback> async_target,
                               std::string local_root_dir, std::string server_base_url,
                               std::shared_ptr<SyncUser> user)
: m_admin(local_root_dir, server_base_url, std::move(user))
, m_target(std::move(async_target))
{
}

std::shared_ptr<GlobalNotifier> GlobalNotifier::shared_notifier(std::unique_ptr<Callback> callback, std::string local_root_dir,
                                                                           std::string server_base_url, std::shared_ptr<SyncUser> user) {
    auto notifier = std::shared_ptr<GlobalNotifier>(new GlobalNotifier(std::move(callback), local_root_dir, server_base_url, user));
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
    m_admin.start([this](auto&& realms, bool all) {
        this->register_realms(std::move(realms), all);
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

        Group const& g = sg2->begin_read(sg->get_version_of_current_transaction());
        _impl::TransactionChangeInfo info;
        info.track_all = true;
        _impl::transaction::advance(*sg2, info, next.target_version);

        std::unordered_map<std::string, CollectionChangeSet> changes;
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

        std::lock_guard<std::mutex> l2(m_deliver_queue_mutex);
        m_pending_deliveries.push({
            sg->get_version_of_current_transaction(),
            next.target_version,
            std::move(next.realm),
            std::move(changes)
        });
        m_signal->notify();
    }
}

void GlobalNotifier::register_realms(std::vector<AdminRealmManager::RealmInfo> realms, bool all)
{
    std::vector<bool> new_realms;
    for (auto &realm_info : realms) {
        new_realms.push_back(m_listen_entries.count(realm_info.first) == 0);
    }
    std::vector<bool> monitor = m_target->available(realms, new_realms, all);

    for (size_t i = 0; i < realms.size(); i++) {
        if (!monitor[i])
            continue;

        std::string &realm_id = realms[i].first, &realm_name = realms[i].second;
        auto config = m_admin.get_config(realm_id, realm_name);
        auto coordinator = _impl::RealmCoordinator::get_coordinator(config);
        m_listen_entries[realm_id] = coordinator;

        auto realm = Realm::make_shared_realm(std::move(config));
        if (realm->read_group().is_empty())
            realm = nullptr;

        auto unowned_coordinator = coordinator.get();

        std::weak_ptr<GlobalNotifier> weak_self = shared_from_this();
        coordinator->set_transaction_callback([weak_self, unowned_coordinator](VersionID old_version, VersionID new_version) {
            auto config = unowned_coordinator->get_config();
            config.schema = util::none;
            auto realm = Realm::make_shared_realm(std::move(config));
            Realm::Internal::begin_read(*realm, old_version);
            REALM_ASSERT(!realm->config().schema);

            if (auto self = weak_self.lock()) {
                std::lock_guard<std::mutex> l(self->m_work_queue_mutex);
                self->m_work_queue.push(RealmToCalculate{std::move(realm), new_version});
                self->m_work_queue_cv.notify_one();
            }
        });
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

GlobalNotifier::ChangeNotification::ChangeNotification(VersionID old_version,
                                                       VersionID new_version,
                                                       SharedRealm realm,
                                                       std::unordered_map<std::string, CollectionChangeSet> changes)
: m_old_version(old_version)
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
