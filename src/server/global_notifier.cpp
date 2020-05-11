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

#include "admin_realm.hpp"

#include "impl/realm_coordinator.hpp"
#include "impl/transact_log_handler.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "util/event_loop_dispatcher.hpp"

#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/global_key.hpp>

#include <json.hpp>

#include <condition_variable>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace realm;
using namespace realm::_impl;

namespace realm {
static void to_json(nlohmann::json& j, VersionID v)
{
    j = {{"version", v.version}, {"index", v.index}};
}
static void from_json(nlohmann::json const& j, VersionID& v)
{
    v.version = j["version"];
    v.index = j["index"];
}

static void to_json(nlohmann::json& j, GlobalKey id)
{
    j = id.to_string();
}
static void from_json(nlohmann::json const& j, GlobalKey& v)
{
    std::string str = j;
    v = GlobalKey::from_string(str);
}
}

class GlobalNotifier::Impl final : public AdminRealmListener {
public:
    Impl(std::unique_ptr<Callback>,
         std::string local_root_dir, SyncConfig sync_config_template);

    util::Optional<ChangeNotification> next_changed_realm();
    void release_version(GlobalKey id, VersionID old_version, VersionID new_version);

public:
    void register_realm(GlobalKey, StringData virtual_path) override;
    void unregister_realm(GlobalKey, StringData) override;
    void error(std::exception_ptr err) override { m_target->error(err); }
    void download_complete() override { m_target->download_complete(); }

    const std::unique_ptr<util::Logger> m_logger;
    const std::unique_ptr<Callback> m_target;

    std::mutex m_work_queue_mutex;
    struct RealmToCalculate {
        GlobalKey realm_id;
        std::string virtual_path;
        std::shared_ptr<_impl::RealmCoordinator> coordinator;
        TransactionRef transaction;
        std::queue<VersionID> versions;
        bool pending_deletion = false;

        // constructor to make GCC 4.9 happy
        RealmToCalculate(GlobalKey realm_id, std::string virtual_path)
        : realm_id(realm_id)
        , virtual_path(std::move(virtual_path))
        {
        }
    };
    std::queue<RealmToCalculate*> m_work_queue;
    std::unordered_map<GlobalKey, RealmToCalculate> m_realms;

    std::shared_ptr<util::Scheduler> m_scheduler = util::Scheduler::make_default();
};

GlobalNotifier::Impl::Impl(std::unique_ptr<Callback> async_target,
                           std::string local_root_dir, SyncConfig sync_config_template)
: AdminRealmListener(local_root_dir, std::move(sync_config_template))
, m_logger(SyncManager::shared().make_logger())
, m_target(std::move(async_target))
{
}

void GlobalNotifier::Impl::register_realm(GlobalKey id, StringData path) {
    auto info = &m_realms.emplace(id, RealmToCalculate{id, path}).first->second;
    m_realms.emplace(id, RealmToCalculate{id, path});

    auto strid = id.to_string();
    if (!m_target->realm_available(strid, path)) {
        m_logger->trace("Global notifier: not watching %1", path);
        return;
    }
    if (info->coordinator) {
        m_logger->trace("Global notifier: already watching %1", path);
        return;
    }

    m_logger->trace("Global notifier: watching %1", path);
    auto config = get_config(path, strid);
    info->coordinator = _impl::RealmCoordinator::get_coordinator(config);

    std::weak_ptr<Impl> weak_self = std::static_pointer_cast<Impl>(shared_from_this());
    info->coordinator->set_transaction_callback([=, weak_self = std::move(weak_self)](VersionID old_version, VersionID new_version) {
        auto self = weak_self.lock();
        if (!self)
            return;

        std::lock_guard<std::mutex> l(m_work_queue_mutex);
        if (info->transaction) {
            m_logger->trace("Global notifier: sync transaction on (%1): Realm already open", info->virtual_path);
        }
        else {
            m_logger->trace("Global notifier: sync transaction on (%1): opening Realm", info->virtual_path);
            std::unique_ptr<Group> read_only_group;
            auto config = info->coordinator->get_config();
            config.force_sync_history = true; // FIXME: needed?
            config.schema = util::none;
            info->coordinator->open_with_config(config);
            auto group_ptr = info->coordinator->begin_read(old_version);
            REALM_ASSERT(std::dynamic_pointer_cast<Transaction>(group_ptr));
            info->transaction = std::static_pointer_cast<Transaction>(group_ptr);
        }
        info->versions.push(new_version);
        if (info->versions.size() == 1) {
            m_logger->trace("Global notifier: Signaling main thread");
            m_work_queue.push(info);
            m_scheduler->notify();
        }
    });
}

void GlobalNotifier::Impl::unregister_realm(GlobalKey id, StringData path) {
    auto realm = m_realms.find(id);
    if (realm == m_realms.end()) {
        m_logger->trace("Global notifier: unwatched Realm at (%1) was deleted", path);
        return;
    }

    std::lock_guard<std::mutex> l(m_work_queue_mutex);
    // Otherwise we need to defer closing the Realm until we're done with our current work.
    m_logger->trace("Global notifier: enqueuing deletion of Realm at (%1)", path);
    if (realm->second.coordinator)
        realm->second.coordinator->set_transaction_callback(nullptr);
    realm->second.pending_deletion = true;

    if (realm->second.versions.empty()) {
        m_work_queue.push(&realm->second);
        m_scheduler->notify();
    }
}

void GlobalNotifier::Impl::release_version(GlobalKey id, VersionID old_version, VersionID new_version)
{
    std::lock_guard<std::mutex> l(m_work_queue_mutex);

    auto it = m_realms.find(id);
    REALM_ASSERT(it != m_realms.end());
    auto& info = it->second;

    if (info.pending_deletion && old_version == VersionID()) {
        m_logger->trace("Global notifier: completing pending deletion of (%1)", info.virtual_path);
        if (info.coordinator) {
            std::string path = info.coordinator->get_config().path;
            m_realms.erase(it);
            File::try_remove(path);
        }
        else {
            m_realms.erase(it);
        }
    }
    else {
        Transaction& tr = *info.transaction.get();
        REALM_ASSERT(tr.get_version_of_current_transaction() == old_version);

        REALM_ASSERT(!info.versions.empty() && info.versions.front() == new_version);
        info.versions.pop();

        if (info.versions.empty()) {
            info.transaction = nullptr;
            m_logger->trace("Global notifier: release version on (%1): no pending versions", info.virtual_path);

            if (info.pending_deletion) {
                m_logger->trace("Global notifier: enqueuing deletion notification for (%1)", info.virtual_path);
                m_work_queue.push(&info);
            }
        }
        else {
            tr.advance_read(new_version);
            m_work_queue.push(&info);
            m_logger->trace("Global notifier: release version on (%1): enqueuing next version", info.virtual_path);
        }
    }

    if (!m_work_queue.empty()) {
        m_logger->trace("Global notifier: Signaling main thread");
        m_scheduler->notify();
    }
}

GlobalNotifier::GlobalNotifier(std::unique_ptr<Callback> async_target,
                               std::string local_root_dir, SyncConfig sync_config_template)
: m_impl(std::make_shared<GlobalNotifier::Impl>(std::move(async_target),
                                                std::move(local_root_dir),
                                                std::move(sync_config_template)))
{
    std::weak_ptr<GlobalNotifier::Impl> weak_impl = m_impl;
    m_impl->m_scheduler->set_notify_callback([weak_impl] {
        if (auto impl = weak_impl.lock()) {
            GlobalNotifier notifier(impl);
            impl->m_target->realm_changed(&notifier);
        }
    });
}

void GlobalNotifier::start()
{
    m_impl->m_logger->trace("Global notifier: start()");
    m_impl->start();
}

GlobalNotifier::~GlobalNotifier() = default;

util::Optional<GlobalNotifier::ChangeNotification> GlobalNotifier::next_changed_realm()
{
    std::lock_guard<std::mutex> l(m_impl->m_work_queue_mutex);
    if (m_impl->m_work_queue.empty()) {
        m_impl->m_logger->trace("Global notifier: next_changed_realm(): no realms pending");
        return util::none;
    }

    auto next = m_impl->m_work_queue.front();
    m_impl->m_work_queue.pop();
    m_impl->m_logger->trace("Global notifier: notifying for realm at %1", next->virtual_path);

    if (next->versions.empty() && next->pending_deletion) {
        return ChangeNotification(m_impl, next->virtual_path, next->realm_id);
    }

    auto old_version = next->transaction->get_version_of_current_transaction();
    return ChangeNotification(m_impl, next->virtual_path, next->realm_id,
                              next->coordinator->get_config(),
                              old_version, next->versions.front());
}

GlobalNotifier::Callback& GlobalNotifier::target()
{
    return *m_impl->m_target;
}

GlobalNotifier::ChangeNotification::ChangeNotification(std::shared_ptr<GlobalNotifier::Impl> notifier,
                                                       std::string virtual_path,
                                                       GlobalKey realm_id,
                                                       Realm::Config config,
                                                       VersionID old_version,
                                                       VersionID new_version)
: realm_path(std::move(virtual_path))
, type(Type::Change)
, m_realm_id(realm_id)
, m_config(std::move(config))
, m_old_version(old_version)
, m_new_version(new_version)
, m_notifier(std::move(notifier))
{
}

GlobalNotifier::ChangeNotification::ChangeNotification(std::shared_ptr<GlobalNotifier::Impl> notifier,
                                                       std::string virtual_path,
                                                       GlobalKey realm_id)
: realm_path(std::move(virtual_path))
, type(Type::Delete)
, m_realm_id(realm_id)
, m_notifier(std::move(notifier))
{
}

GlobalNotifier::ChangeNotification::~ChangeNotification()
{
    if (m_notifier)
        m_notifier->release_version(m_realm_id, m_old_version, m_new_version);
    if (m_old_realm)
        m_old_realm->close();
    if (m_new_realm)
        m_new_realm->close();
}

std::string GlobalNotifier::ChangeNotification::serialize() const
{
    nlohmann::json ret;
    ret["virtual_path"] = realm_path;
    ret["realm_id"] = m_realm_id;
    if (type == Type::Change) {
        ret["path"] = m_config.path;
        ret["old_version"] = m_old_version;
        ret["new_version"] = m_new_version;
        ret["is_change"] = true;
    }
    return ret.dump();
}

GlobalNotifier::ChangeNotification::ChangeNotification(std::string const& serialized)
{
    auto parsed = nlohmann::json::parse(serialized);
    parsed["virtual_path"].get_to(realm_path);
    parsed["realm_id"].get_to(m_realm_id);

    if (!parsed["is_change"].is_null()) {
        type = Type::Change;
        parsed["old_version"].get_to(m_old_version);
        parsed["new_version"].get_to(m_new_version);

        parsed["path"].get_to(m_config.path);
        m_config.force_sync_history = true;
        m_config.schema_mode = SchemaMode::Additive;
        m_config.automatic_change_notifications = false;
    }
    else {
        type = Type::Delete;
    }
}

SharedRealm GlobalNotifier::ChangeNotification::get_old_realm() const
{
    if (const_cast<VersionID&>(m_old_version) == VersionID{})
        return nullptr;
    if (m_old_realm)
        return m_old_realm;

    m_old_realm = Realm::get_shared_realm(m_config);
    Realm::Internal::begin_read(*m_old_realm, m_old_version);
    return m_old_realm;
}

SharedRealm GlobalNotifier::ChangeNotification::get_new_realm() const
{
    if (m_new_realm)
        return m_new_realm;
    m_new_realm = Realm::get_shared_realm(m_config);
    Realm::Internal::begin_read(*m_new_realm, m_new_version);
    return m_new_realm;
}

std::unordered_map<std::string, ObjectChangeSet> const& GlobalNotifier::ChangeNotification::get_changes() const
{
    if (m_have_calculated_changes)
        return m_changes;

    Realm::Config config;
    config.path = m_config.path;
    config.force_sync_history = true;
    config.automatic_change_notifications = false;

    auto realm = Realm::get_shared_realm(config);

    Realm::Internal::begin_read(*realm, m_old_version);
    Group& g = realm->read_group();

    _impl::TransactionChangeInfo info;
    info.track_all = true;
    _impl::transaction::advance(static_cast<Transaction&>(g), info, m_new_version);

    m_changes.reserve(info.tables.size());
    auto table_keys = g.get_table_keys();
    for (auto table_key : table_keys) {
        auto& change = info.tables[table_key.value];
        if (!change.empty()) {
            auto name = ObjectStore::object_type_for_table_name(g.get_table_name(table_key));
            if (name) {
                m_changes[name] = std::move(change);
            }
        }
    }

    m_have_calculated_changes = true;
    return m_changes;
}

GlobalNotifier::Callback::~Callback() = default;
