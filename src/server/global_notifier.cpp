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
#include "event_loop_dispatcher.hpp"
#include "results.hpp"
#include "util/event_loop_signal.hpp"

#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/sync_session.hpp"

#include <realm/lang_bind_helper.hpp>
#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>

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
void to_json(nlohmann::json& j, VersionID v)
{
    j = {{"version", v.version}, {"index", v.index}};
}
void from_json(nlohmann::json const& j, VersionID& v)
{
    v.version = j["version"];
    v.index = j["index"];
}
}

namespace {
static SyncLoggerFactory* s_logger_factory;
std::unique_ptr<util::Logger> make_logger()
{
    auto log_level = SyncManager::shared().log_level();
    if (s_logger_factory) {
        return s_logger_factory->make_logger(log_level); // Throws
    }
    auto logger = std::make_unique<util::StderrLogger>(); // Throws
    logger->set_level_threshold(log_level);
    return std::unique_ptr<util::Logger>(logger.release());
}
}

class GlobalNotifier::Impl : public AdminRealmListener {
public:
    Impl(std::unique_ptr<Callback>, std::string local_root_dir,
         std::string server_base_url, std::shared_ptr<SyncUser> user,
         std::function<SyncBindSessionHandler> bind_callback);

    Realm::Config get_config(StringData virtual_path, util::Optional<Schema> schema) const;
    util::Optional<ChangeNotification> next_changed_realm();
    void release_version(std::string const& id, VersionID old_version, VersionID new_version);

public:
    void register_realm(StringData, StringData virtual_path) override;
    void unregister_realm(StringData, StringData) override;
    void error(std::exception_ptr err) override { m_target->error(err); }
    void download_complete() override { m_target->download_complete(); }

    const std::unique_ptr<util::Logger> m_logger;
    const std::unique_ptr<Callback> m_target;
    const std::string m_server_base_url;
    std::shared_ptr<SyncUser> m_user;
    std::function<SyncBindSessionHandler> m_bind_callback;
    std::string m_regular_realms_dir;

    std::mutex m_work_queue_mutex;
    struct RealmToCalculate {
        std::string realm_id;
        std::string virtual_path;
        std::shared_ptr<_impl::RealmCoordinator> coordinator;
        std::unique_ptr<Replication> history;
        std::unique_ptr<SharedGroup> shared_group;
        std::queue<VersionID> versions;
        bool pending_deletion = false;

        // constructor to make GCC 4.9 happy
        RealmToCalculate(std::string realm_id, std::string virtual_path, std::shared_ptr<_impl::RealmCoordinator> coordinator)
            : realm_id(std::move(realm_id))
            , virtual_path(std::move(virtual_path))
            , coordinator(std::move(coordinator))
        {
        }
    };
    std::queue<RealmToCalculate*> m_work_queue;
    std::unordered_map<std::string, RealmToCalculate> m_realms;

    struct SignalCallback {
        std::weak_ptr<Impl> notifier;
        void operator()()
        {
            if (auto alive = notifier.lock()) {
                GlobalNotifier notifier(alive);
                alive->m_target->realm_changed(&notifier);
            }
        }
    };

    std::shared_ptr<util::EventLoopSignal<SignalCallback>> m_signal;
};

GlobalNotifier::Impl::Impl(std::unique_ptr<Callback> async_target,
                           std::string local_root_dir, std::string server_base_url,
                           std::shared_ptr<SyncUser> user, std::function<SyncBindSessionHandler> bind_callback)
: AdminRealmListener(local_root_dir, server_base_url, user, bind_callback)
, m_logger(make_logger())
, m_target(std::move(async_target))
, m_server_base_url(std::move(server_base_url))
, m_user(std::move(user))
, m_bind_callback(std::move(bind_callback))
, m_regular_realms_dir(util::File::resolve("realms", local_root_dir)) // Throws
{
    util::try_make_dir(m_regular_realms_dir); // Throws
}

Realm::Config GlobalNotifier::Impl::get_config(StringData virtual_path, util::Optional<Schema> schema) const {
    Realm::Config config;
    if (schema) {
        config.schema = std::move(schema);
        config.schema_version = 0;
    }

    std::string file_path = m_regular_realms_dir + virtual_path.data() + ".realm";
    for (size_t pos = m_regular_realms_dir.size(); pos != file_path.npos; pos = file_path.find('/', pos + 1)) {
        file_path[pos] = '\0';
        util::try_make_dir(file_path);
        file_path[pos] = '/';
    }

    config.path = std::move(file_path);
    config.sync_config = std::make_unique<SyncConfig>(m_user, m_server_base_url + virtual_path.data());
    config.sync_config->bind_session_handler = m_bind_callback;
    config.schema_mode = SchemaMode::Additive;
    config.cache = false;
    config.automatic_change_notifications = false;
    return config;
}

void GlobalNotifier::Impl::register_realm(StringData id, StringData path) {
    if (!m_target->realm_available(path)) {
        m_logger->trace("Global notifier: not watching %1", path);
        return;
    }
    m_logger->trace("Global notifier: watching %1", path);

    auto config = get_config(path, util::none);
    auto coordinator = _impl::RealmCoordinator::get_coordinator(config);
    auto info = &m_realms.emplace(id, RealmToCalculate{id, path, coordinator}).first->second;

    std::weak_ptr<Impl> weak_self = std::static_pointer_cast<Impl>(shared_from_this());
    coordinator->set_transaction_callback([=, weak_self = std::move(weak_self)](VersionID old_version, VersionID new_version) {
        auto self = weak_self.lock();
        if (!self)
            return;

        std::lock_guard<std::mutex> l(m_work_queue_mutex);
        if (info->shared_group) {
            m_logger->trace("Global notifier: sync transaction on (%1): Realm already open", info->virtual_path);
        }
        else {
            m_logger->trace("Global notifier: sync transaction on (%1): opening Realm", info->virtual_path);
            std::unique_ptr<Group> read_only_group;
            auto config = info->coordinator->get_config();
            config.force_sync_history = true; // FIXME: needed?
            config.schema = util::none;
            Realm::open_with_config(config, info->history, info->shared_group, read_only_group, nullptr);
            info->shared_group->begin_read(old_version);
        }
        info->versions.push(new_version);
        if (info->versions.size() == 1) {
            m_logger->trace("Global notifier: Signaling main thread");
            m_work_queue.push(info);
            m_signal->notify();
        }
    });
}

void GlobalNotifier::Impl::unregister_realm(StringData id, StringData path) {
    auto realm = m_realms.find(id);
    if (realm == m_realms.end()) {
        m_logger->trace("Global notifier: unwatched Realm at (%1) was deleted", path);
        return;
    }

    std::lock_guard<std::mutex> l(m_work_queue_mutex);
    if (realm->second.versions.empty()) {
        // No notifications currently in progress, so we can just close the Realm and be done with it.
        m_logger->trace("Global notifier: watched Realm at (%1) was deleted", path);
        std::string path = realm->second.coordinator->get_config().path;
        m_realms.erase(realm);
        File::remove(path);
        return;
    }

    // Otherwise we need to defer closing the Realm until we're done with our current work.
    m_logger->trace("Global notifier: watched Realm at (%1) will be deleted once all notifications are processed", path);
    realm->second.coordinator->set_transaction_callback(nullptr);
    realm->second.pending_deletion = true;
}

void GlobalNotifier::Impl::release_version(std::string const& id, VersionID old_version, VersionID new_version)
{
    std::lock_guard<std::mutex> l(m_work_queue_mutex);

    auto it = m_realms.find(id);
    REALM_ASSERT(it != m_realms.end());
    auto& info = it->second;

    auto& sg = info.shared_group;
    REALM_ASSERT(sg->get_version_of_current_transaction() == old_version);

    REALM_ASSERT(!info.versions.empty() && info.versions.front() == new_version);
    info.versions.pop();

    if (info.versions.empty()) {
        info.shared_group = nullptr;
        info.history = nullptr;
        m_logger->trace("Global notifier: release version on (%1): no pending versions", info.virtual_path);

        if (info.pending_deletion) {
            m_logger->trace("Global notifier: completing pending deletion of (%1)", info.virtual_path);
            std::string path = info.coordinator->get_config().path;
            m_realms.erase(it);
            File::remove(path);
        }
    }
    else {
        LangBindHelper::advance_read(*sg, new_version);
        m_work_queue.push(&info);
        m_logger->trace("Global notifier: release version on (%1): enqueuing next version", info.virtual_path);
    }

    if (!m_work_queue.empty()) {
        m_logger->trace("Global notifier: Signaling main thread");
        m_signal->notify();
    }
}

GlobalNotifier::GlobalNotifier(std::unique_ptr<Callback> async_target,
                               std::string local_root_dir, std::string server_base_url,
                               std::shared_ptr<SyncUser> user, std::function<SyncBindSessionHandler> bind_callback)
: m_impl(std::make_shared<GlobalNotifier::Impl>(std::move(async_target),
                                                std::move(local_root_dir),
                                                std::move(server_base_url),
                                                std::move(user),
                                                std::move(bind_callback)))
{
    std::weak_ptr<GlobalNotifier::Impl> weak_impl = m_impl;
    m_impl->m_signal = std::make_shared<util::EventLoopSignal<Impl::SignalCallback>>(Impl::SignalCallback{weak_impl});
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

    auto old_version = next->shared_group->get_version_of_current_transaction();
    return ChangeNotification(m_impl, next->virtual_path, next->realm_id,
                              next->coordinator->get_config(),
                              old_version, next->versions.front());
}

GlobalNotifier::Callback& GlobalNotifier::target()
{
    return *m_impl->m_target;
}

void GlobalNotifier::set_logger_factory(SyncLoggerFactory* factory)
{
    s_logger_factory = factory;
}

GlobalNotifier::ChangeNotification::ChangeNotification(std::shared_ptr<GlobalNotifier::Impl> notifier,
                                                       std::string virtual_path,
                                                       std::string realm_id,
                                                       Realm::Config config,
                                                       VersionID old_version,
                                                       VersionID new_version)
: realm_path(std::move(virtual_path))
, m_realm_id(std::move(realm_id))
, m_config(std::move(config))
, m_old_version(old_version)
, m_new_version(new_version)
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
    ret["path"] = m_config.path;
    ret["old_version"] = m_old_version;
    ret["new_version"] = m_new_version;
    return ret.dump();
}

GlobalNotifier::ChangeNotification::ChangeNotification(std::string const& serialized)
{
    auto parsed = nlohmann::json::parse(serialized);
    realm_path = parsed["virtual_path"];
    m_realm_id = parsed["realm_id"];
    m_old_version = parsed["old_version"];
    m_new_version = parsed["new_version"];

    m_config.path = parsed["path"];
    m_config.force_sync_history = true;
    m_config.schema_mode = SchemaMode::Additive;
    m_config.cache = false;
    m_config.automatic_change_notifications = false;
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

std::unordered_map<std::string, CollectionChangeSet> const& GlobalNotifier::ChangeNotification::get_changes() const
{
    if (m_have_calculated_changes)
        return m_changes;

    Realm::Config config;
    config.path = m_config.path;
    config.cache = false;
    config.force_sync_history = true;
    config.automatic_change_notifications = false;

    auto realm = Realm::get_shared_realm(config);

    auto& sg = Realm::Internal::get_shared_group(*realm);
    Realm::Internal::begin_read(*realm, m_old_version);
    Group const& g = realm->read_group();

    _impl::TransactionChangeInfo info;
    info.track_all = true;
    _impl::transaction::advance(*sg, info, m_new_version);

    m_changes.reserve(info.tables.size());
    for (size_t i = 0; i < info.tables.size(); ++i) {
        auto& change = info.tables[i];
        if (!change.empty()) {
            auto name = ObjectStore::object_type_for_table_name(g.get_table_name(i));
            if (name) {
                m_changes[name] = std::move(change).finalize();
            }
        }
    }

    m_have_calculated_changes = true;
    return m_changes;
}

GlobalNotifier::Callback::~Callback() = default;
