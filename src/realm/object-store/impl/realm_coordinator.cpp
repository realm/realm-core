////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#include <realm/object-store/impl/realm_coordinator.hpp>

#include <realm/object-store/impl/collection_notifier.hpp>
#include <realm/object-store/impl/external_commit_helper.hpp>
#include <realm/object-store/impl/transact_log_handler.hpp>
#include <realm/object-store/impl/weak_realm_notifier.hpp>
#include <realm/object-store/audit.hpp>
#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/scheduler.hpp>

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#endif

#include <realm/db.hpp>
#include <realm/history.hpp>
#include <realm/string_data.hpp>
#include <realm/util/fifo_helper.hpp>
#include <realm/sync/config.hpp>

#include <algorithm>
#include <unordered_map>

using namespace realm;
using namespace realm::_impl;

static auto& s_coordinator_mutex = *new std::mutex;
static auto& s_coordinators_per_path = *new std::unordered_map<std::string, std::weak_ptr<RealmCoordinator>>;

std::shared_ptr<RealmCoordinator> RealmCoordinator::get_coordinator(StringData path)
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);

    auto& weak_coordinator = s_coordinators_per_path[path];
    if (auto coordinator = weak_coordinator.lock()) {
        return coordinator;
    }

    auto coordinator = std::make_shared<RealmCoordinator>(Private());
    weak_coordinator = coordinator;
    return coordinator;
}

std::shared_ptr<RealmCoordinator>
RealmCoordinator::get_coordinator(const Realm::Config& config) NO_THREAD_SAFETY_ANALYSIS
{
    auto coordinator = get_coordinator(config.path);
    util::CheckedLockGuard lock(coordinator->m_realm_mutex);
    coordinator->set_config(config);
    coordinator->open_db();
    return coordinator;
}

std::shared_ptr<RealmCoordinator> RealmCoordinator::get_existing_coordinator(StringData path)
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    return s_coordinators_per_path[path].lock();
}

void RealmCoordinator::set_config(const Realm::Config& config)
{
    if (config.encryption_key.data() && config.encryption_key.size() != 64)
        throw InvalidEncryptionKey();
    if (config.schema_mode == SchemaMode::Immutable && config.sync_config)
        throw InvalidArgument(ErrorCodes::IllegalCombination,
                              "Synchronized Realms cannot be opened in immutable mode");
    if ((config.schema_mode == SchemaMode::AdditiveDiscovered ||
         config.schema_mode == SchemaMode::AdditiveExplicit) &&
        config.migration_function)
        throw InvalidArgument(ErrorCodes::IllegalCombination,
                              "Realms opened in Additive-only schema mode do not use a migration function");
    if (config.schema_mode == SchemaMode::Immutable && config.migration_function)
        throw InvalidArgument(ErrorCodes::IllegalCombination,
                              "Realms opened in immutable mode do not use a migration function");
    if (config.schema_mode == SchemaMode::ReadOnly && config.migration_function)
        throw InvalidArgument(ErrorCodes::IllegalCombination,
                              "Realms opened in read-only mode do not use a migration function");
    if (config.schema_mode == SchemaMode::Immutable && config.initialization_function)
        throw InvalidArgument(ErrorCodes::IllegalCombination,
                              "Realms opened in immutable mode do not use an initialization function");
    if (config.schema_mode == SchemaMode::ReadOnly && config.initialization_function)
        throw InvalidArgument(ErrorCodes::IllegalCombination,
                              "Realms opened in read-only mode do not use an initialization function");
    if (config.schema && config.schema_version == ObjectStore::NotVersioned)
        throw InvalidArgument(ErrorCodes::IllegalCombination,
                              "A schema version must be specified when the schema is specified");
    if (!config.realm_data.is_null() && (!config.immutable() || !config.in_memory))
        throw InvalidArgument(
            ErrorCodes::IllegalCombination,
            "In-memory realms initialized from memory buffers can only be opened in read-only mode");
    if (!config.realm_data.is_null() && !config.path.empty())
        throw InvalidArgument(ErrorCodes::IllegalCombination, "Specifying both memory buffer and path is invalid");
    if (!config.realm_data.is_null() && !config.encryption_key.empty())
        throw InvalidArgument(ErrorCodes::IllegalCombination, "Memory buffers do not support encryption");
    if (config.in_memory && !config.encryption_key.empty()) {
        throw InvalidArgument(ErrorCodes::IllegalCombination, "Encryption is not supported for in-memory realms");
    }
    // ResetFile also won't use the migration function, but specifying one is
    // allowed to simplify temporarily switching modes during development

#if REALM_ENABLE_SYNC
    if (config.sync_config) {
        if (config.sync_config->flx_sync_requested && !config.sync_config->partition_value.empty()) {
            throw InvalidArgument(ErrorCodes::IllegalCombination,
                                  "Cannot specify a partition value when flexible sync is enabled");
        }
        if (!config.sync_config->user) {
            throw InvalidArgument(ErrorCodes::IllegalCombination,
                                  "A user must be provided to open a synchronized Realm.");
        }
    }
#endif

    bool no_existing_realm =
        std::all_of(begin(m_weak_realm_notifiers), end(m_weak_realm_notifiers), [](auto& notifier) {
            return notifier.expired();
        });
    if (no_existing_realm) {
        m_config = config;
        m_config.scheduler = nullptr;
    }
    else {
        if (m_config.immutable() != config.immutable()) {
            throw LogicError(
                ErrorCodes::MismatchedConfig,
                util::format("Realm at path '%1' already opened with different read permissions.", config.path));
        }
        if (m_config.in_memory != config.in_memory) {
            throw LogicError(
                ErrorCodes::MismatchedConfig,
                util::format("Realm at path '%1' already opened with different inMemory settings.", config.path));
        }
        if (m_config.encryption_key != config.encryption_key) {
            throw LogicError(
                ErrorCodes::MismatchedConfig,
                util::format("Realm at path '%1' already opened with a different encryption key.", config.path));
        }
        if (m_config.schema_mode != config.schema_mode) {
            throw LogicError(
                ErrorCodes::MismatchedConfig,
                util::format("Realm at path '%1' already opened with a different schema mode.", config.path));
        }
        util::CheckedLockGuard lock(m_schema_cache_mutex);
        if (config.schema && m_schema_version != ObjectStore::NotVersioned &&
            m_schema_version != config.schema_version) {
            throw LogicError(
                ErrorCodes::MismatchedConfig,
                util::format("Realm at path '%1' already opened with different schema version.", config.path));
        }

#if REALM_ENABLE_SYNC
        if (bool(m_config.sync_config) != bool(config.sync_config)) {
            throw LogicError(
                ErrorCodes::MismatchedConfig,
                util::format("Realm at path '%1' already opened with different sync configurations.", config.path));
        }

        if (config.sync_config) {
            auto old_user = m_config.sync_config->user;
            auto new_user = config.sync_config->user;
            if (old_user != new_user) {
                throw LogicError(
                    ErrorCodes::MismatchedConfig,
                    util::format("Realm at path '%1' already opened with different sync user.", config.path));
            }
            if (m_config.sync_config->partition_value != config.sync_config->partition_value) {
                throw LogicError(
                    ErrorCodes::MismatchedConfig,
                    util::format("Realm at path '%1' already opened with different partition value.", config.path));
            }
            if (m_config.sync_config->flx_sync_requested != config.sync_config->flx_sync_requested) {
                throw LogicError(ErrorCodes::MismatchedConfig,
                                 util::format("Realm at path '%1' already opened in a different synchronization mode",
                                              config.path));
            }
        }
#endif
        // Mixing cached and uncached Realms is allowed
        m_config.cache = config.cache;

        // Realm::update_schema() handles complaining about schema mismatches
    }
}

std::shared_ptr<Realm> RealmCoordinator::get_cached_realm(Realm::Config const& config,
                                                          std::shared_ptr<util::Scheduler> scheduler)
{
    if (!config.cache)
        return nullptr;
    util::CheckedUniqueLock lock(m_realm_mutex);
    return do_get_cached_realm(config, scheduler);
}

std::shared_ptr<Realm> RealmCoordinator::do_get_cached_realm(Realm::Config const& config,
                                                             std::shared_ptr<util::Scheduler> scheduler)
{
    if (!config.cache)
        return nullptr;

    if (!scheduler) {
        scheduler = config.scheduler;
    }

    if (!scheduler)
        return nullptr;

    for (auto& cached_realm : m_weak_realm_notifiers) {
        if (!cached_realm.is_cached_for_scheduler(scheduler))
            continue;
        // can be null if we jumped in between ref count hitting zero and
        // unregister_realm() getting the lock
        if (auto realm = cached_realm.realm()) {
            // If the file is uninitialized and was opened without a schema,
            // do the normal schema init
            if (realm->schema_version() == ObjectStore::NotVersioned)
                break;

            // Otherwise if we have a realm schema it needs to be an exact
            // match (even having the same properties but in different
            // orders isn't good enough)
            if (config.schema && realm->schema() != *config.schema)
                throw LogicError(
                    ErrorCodes::MismatchedConfig,
                    util::format("Realm at path '%1' already opened on current thread with different schema.",
                                 config.path));

            return realm;
        }
    }
    return nullptr;
}

std::shared_ptr<Realm> RealmCoordinator::get_realm(Realm::Config config, util::Optional<VersionID> version)
{
    REALM_ASSERT(!version || *version != VersionID());
    if (!config.scheduler)
        config.scheduler = version ? util::Scheduler::make_frozen(*version) : util::Scheduler::make_default();
    // realm must be declared before lock so that the mutex is released before
    // we release the strong reference to realm, as Realm's destructor may want
    // to acquire the same lock
    std::shared_ptr<Realm> realm;
    util::CheckedUniqueLock lock(m_realm_mutex);
    set_config(config);
    if ((realm = do_get_cached_realm(config))) {
        REALM_ASSERT(!version || realm->read_transaction_version() == *version);
        return realm;
    }
    do_get_realm(std::move(config), realm, version, lock);
    if (version) {
        realm->read_group();
    }
    return realm;
}

std::shared_ptr<Realm> RealmCoordinator::get_realm(std::shared_ptr<util::Scheduler> scheduler, bool first_time_open)
{
    std::shared_ptr<Realm> realm;
    util::CheckedUniqueLock lock(m_realm_mutex);
    auto config = m_config;
    config.scheduler = scheduler ? scheduler : util::Scheduler::make_default();
    if ((realm = do_get_cached_realm(config))) {
        return realm;
    }
    do_get_realm(std::move(config), realm, none, lock, first_time_open);
    return realm;
}

std::shared_ptr<Realm> RealmCoordinator::freeze_realm(const Realm& source_realm)
{
    std::shared_ptr<Realm> realm;
    util::CheckedUniqueLock lock(m_realm_mutex);

    auto version = source_realm.read_transaction_version();
    auto scheduler = util::Scheduler::make_frozen(version);
    if ((realm = do_get_cached_realm(source_realm.config(), scheduler))) {
        return realm;
    }

    auto config = source_realm.config();
    config.scheduler = scheduler;
    realm = Realm::make_shared_realm(std::move(config), version, shared_from_this());
    Realm::Internal::copy_schema(*realm, source_realm);
    m_weak_realm_notifiers.emplace_back(realm, config.cache);
    return realm;
}

ThreadSafeReference RealmCoordinator::get_unbound_realm()
{
    std::shared_ptr<Realm> realm;
    util::CheckedUniqueLock lock(m_realm_mutex);
    do_get_realm(RealmConfig(m_config), realm, none, lock);
    return ThreadSafeReference(realm);
}

void RealmCoordinator::do_get_realm(RealmConfig&& config, std::shared_ptr<Realm>& realm,
                                    util::Optional<VersionID> version, util::CheckedUniqueLock& realm_lock,
                                    bool first_time_open)
{
    const auto db_created = open_db();
#ifdef REALM_ENABLE_SYNC
    SyncConfig::SubscriptionInitializerCallback subscription_function = nullptr;
    bool rerun_on_open = false;
    if (config.sync_config && config.sync_config->flx_sync_requested &&
        config.sync_config->subscription_initializer) {
        subscription_function = config.sync_config->subscription_initializer;
        rerun_on_open = config.sync_config->rerun_init_subscription_on_open;
    }
#else
    static_cast<void>(first_time_open);
    static_cast<void>(db_created);
#endif

    auto schema = std::move(config.schema);
    auto migration_function = std::move(config.migration_function);
    auto initialization_function = std::move(config.initialization_function);
    config.schema = {};

    realm = Realm::make_shared_realm(std::move(config), version, shared_from_this());
    m_weak_realm_notifiers.emplace_back(realm, config.cache);

#ifdef REALM_ENABLE_SYNC
    if (m_sync_session && m_sync_session->user()->is_logged_in())
        m_sync_session->revive_if_needed();

    if (realm->config().audit_config) {
        if (m_audit_context)
            m_audit_context->update_metadata(realm->config().audit_config->metadata);
        else
            m_audit_context = make_audit_context(m_db, realm->config());
    }
#else
    if (realm->config().audit_config)
        REALM_TERMINATE("Cannot use Audit interface if Realm Core is built without Sync");
#endif

    // Cached frozen Realms need to initialize their schema before releasing
    // the lock as otherwise they could be read from the cache on another thread
    // before the schema initialization happens. They'll never perform a write
    // transaction, so unlike with live Realms this is safe to do.
    if (config.cache && version && schema) {
        realm->update_schema(std::move(*schema));
        schema.reset();
    }

    realm_lock.unlock_unchecked();
    if (schema) {
        realm->update_schema(std::move(*schema), config.schema_version, std::move(migration_function),
                             std::move(initialization_function));
    }

#ifdef REALM_ENABLE_SYNC
    // run subscription initializer if the SDK has instructed core to do so. The subscription callback will be run if:
    // 1. this is the first time we are creating the realm file
    // 2. the database was already created, but this is the first time we are opening the db and the flag
    // rerun_on_open was set
    if (subscription_function) {
        const auto current_subscription = realm->get_latest_subscription_set();
        const auto subscription_version = current_subscription.version();
        // in case we are hitting this check while during a normal open, we need to take in
        // consideration if the db was created during this call. Since this may be the first time
        // we are actually creating a realm. For async open this does not apply, in fact db_created
        // will always be false.
        if (!first_time_open)
            first_time_open = db_created;
        if (subscription_version == 0 || (first_time_open && rerun_on_open)) {
            bool was_in_read = realm->is_in_read_transaction();
            subscription_function(realm);
            if (!was_in_read)
                realm->invalidate();
        }
    }
#endif
}

void RealmCoordinator::bind_to_context(Realm& realm)
{
    util::CheckedLockGuard lock(m_realm_mutex);
    for (auto& cached_realm : m_weak_realm_notifiers) {
        if (!cached_realm.is_for_realm(&realm))
            continue;
        cached_realm.bind_to_scheduler();
        return;
    }
    REALM_TERMINATE("Invalid Realm passed to bind_to_context()");
}

#if REALM_ENABLE_SYNC
std::shared_ptr<AsyncOpenTask> RealmCoordinator::get_synchronized_realm(Realm::Config config)
{
    if (!config.sync_config)
        throw LogicError(ErrorCodes::IllegalOperation,
                         "This method is only available for fully synchronized Realms.");

    util::CheckedLockGuard lock(m_realm_mutex);
    set_config(config);
    const auto db_open_first_time = open_db();
    return std::make_shared<AsyncOpenTask>(AsyncOpenTask::Private(), shared_from_this(), m_sync_session,
                                           db_open_first_time);
}

#endif

bool RealmCoordinator::open_db()
{
    if (m_db)
        return false;

#if REALM_ENABLE_SYNC
    if (m_config.sync_config) {
        REALM_ASSERT(m_config.sync_config->user);
        // If we previously opened this Realm, we may have a lingering sync
        // session which outlived its RealmCoordinator. If that happens we
        // want to reuse it instead of creating a new DB.
        if (auto sync_manager = m_config.sync_config->user->sync_manager()) {
            m_sync_session = sync_manager->get_existing_session(m_config.path);
        }
        if (m_sync_session) {
            m_db = SyncSession::Internal::get_db(*m_sync_session);
            init_external_helpers();
            return false;
        }
    }
#endif

    bool server_synchronization_mode = m_config.sync_config || m_config.force_sync_history;
    bool schema_mode_reset_file =
        m_config.schema_mode == SchemaMode::SoftResetFile || m_config.schema_mode == SchemaMode::HardResetFile;
    try {
        if (m_config.immutable() && m_config.realm_data) {
            m_db = DB::create(m_config.realm_data, false);
            return true;
        }
        std::unique_ptr<Replication> history;
        if (server_synchronization_mode) {
#if REALM_ENABLE_SYNC
            bool apply_server_changes = !m_config.sync_config || m_config.sync_config->apply_server_changes;
            history = std::make_unique<sync::ClientReplication>(apply_server_changes);
#else
            REALM_TERMINATE("Realm was not built with sync enabled");
#endif
        }
        else if (!m_config.immutable()) {
            history = make_in_realm_history();
        }

        DBOptions options;
#ifndef __EMSCRIPTEN__
        options.enable_async_writes = true;
#endif
        options.durability = m_config.in_memory ? DBOptions::Durability::MemOnly : DBOptions::Durability::Full;
        options.is_immutable = m_config.immutable();
        options.logger = util::Logger::get_default_logger();

        if (!m_config.fifo_files_fallback_path.empty()) {
            options.temp_dir = util::normalize_dir(m_config.fifo_files_fallback_path);
        }
        options.encryption_key = m_config.encryption_key.data();
        options.allow_file_format_upgrade = !m_config.disable_format_upgrade && !schema_mode_reset_file;
        options.clear_on_invalid_file = m_config.clear_on_invalid_file;
        if (history) {
            options.backup_at_file_format_change = m_config.backup_at_file_format_change;
#ifdef __EMSCRIPTEN__
            // Force the DB to be created in memory-only mode, ignoring the filesystem path supplied in the config.
            // This is so we can run an SDK on top without having to solve the browser persistence problem yet,
            // or teach RealmConfig and SDKs about pure in-memory realms.
            m_db = DB::create_in_memory(std::move(history), m_config.path, options);
#else
            if (m_config.path.size()) {
                m_db = DB::create(std::move(history), m_config.path, options);
            }
            else {
                m_db = DB::create(std::move(history), options);
            }
#endif
        }
        else {
            options.no_create = true;
            m_db = DB::create(m_config.path, options);
        }
    }
    catch (realm::FileFormatUpgradeRequired const&) {
        if (!schema_mode_reset_file) {
            throw;
        }
        util::File::remove(m_config.path);
        return open_db();
    }
    catch (UnsupportedFileFormatVersion const&) {
        if (!schema_mode_reset_file) {
            throw;
        }
        util::File::remove(m_config.path);
        return open_db();
    }

    if (m_config.should_compact_on_launch_function) {
        size_t free_space = 0;
        size_t used_space = 0;
        if (auto tr = m_db->start_write(true)) {
            tr->commit();
            m_db->get_stats(free_space, used_space);
        }
        if (free_space > 0 && m_config.should_compact_on_launch_function(free_space + used_space, used_space))
            m_db->compact();
    }

    init_external_helpers();
    return true;
}

void RealmCoordinator::init_external_helpers()
{
    // There's a circular dependency between SyncSession and ExternalCommitHelper
    // where sync commits notify ECH and other commits notify sync via ECH. This
    // happens on background threads, so to avoid needing locking on every access
    // we have to wire things up in a specific order.
#if REALM_ENABLE_SYNC
    // We may have reused an existing sync session that outlived its original
    // RealmCoordinator. If not, we need to create a new one now.
    if (m_config.sync_config && !m_sync_session) {
        if (!m_config.sync_config->user || m_config.sync_config->user->state() == SyncUser::State::Removed) {
            throw app::AppError(
                ErrorCodes::ClientUserNotFound,
                util::format("Cannot start a sync session for user '%1' because this user has been removed.",
                             m_config.sync_config->user->user_id()));
        }
        if (auto sync_manager = m_config.sync_config->user->sync_manager()) {
            m_sync_session = sync_manager->get_session(m_db, m_config);
        }
    }
#endif

    if (!m_notifier && !m_config.immutable() && m_config.automatic_change_notifications) {
        try {
            m_notifier = std::make_unique<ExternalCommitHelper>(*this, m_config);
        }
        catch (std::system_error const& ex) {
            throw FileAccessError(ErrorCodes::FileOperationFailed,
                                  util::format("Failed to create ExternalCommitHelper: %1", ex.what()), get_path(),
                                  ex.code().value());
        }
    }
    m_db->add_commit_listener(this);
}

void RealmCoordinator::close()
{
    m_db->close();
    m_db = nullptr;
}

void RealmCoordinator::delete_and_reopen()
{
    util::CheckedLockGuard lock(m_realm_mutex);
    close();
    util::File::remove(m_config.path);
    open_db();
}

TransactionRef RealmCoordinator::begin_read(VersionID version, bool frozen_transaction)
{
    REALM_ASSERT(m_db);
    return frozen_transaction ? m_db->start_frozen(version) : m_db->start_read(version);
}

uint64_t RealmCoordinator::get_schema_version() const noexcept
{
    util::CheckedLockGuard lock(m_schema_cache_mutex);
    return m_schema_version;
}

bool RealmCoordinator::get_cached_schema(Schema& schema, uint64_t& schema_version,
                                         uint64_t& transaction) const noexcept
{
    util::CheckedLockGuard lock(m_schema_cache_mutex);
    if (!m_cached_schema)
        return false;
    schema = *m_cached_schema;
    schema_version = m_schema_version;
    transaction = m_schema_transaction_version_max;
    return true;
}

void RealmCoordinator::cache_schema(Schema const& new_schema, uint64_t new_schema_version,
                                    uint64_t transaction_version)
{
    util::CheckedLockGuard lock(m_schema_cache_mutex);
    if (transaction_version < m_schema_transaction_version_max)
        return;
    if (new_schema.empty() || new_schema_version == ObjectStore::NotVersioned)
        return;

    m_cached_schema = new_schema;
    m_schema_version = new_schema_version;
    m_schema_transaction_version_min = transaction_version;
    m_schema_transaction_version_max = transaction_version;
}

void RealmCoordinator::clear_schema_cache_and_set_schema_version(uint64_t new_schema_version)
{
    util::CheckedLockGuard lock(m_schema_cache_mutex);
    m_cached_schema = util::none;
    m_schema_version = new_schema_version;
}

void RealmCoordinator::advance_schema_cache(uint64_t previous, uint64_t next)
{
    util::CheckedLockGuard lock(m_schema_cache_mutex);
    if (!m_cached_schema)
        return;
    REALM_ASSERT(previous <= m_schema_transaction_version_max);
    if (next < m_schema_transaction_version_min)
        return;
    m_schema_transaction_version_min = std::min(previous, m_schema_transaction_version_min);
    m_schema_transaction_version_max = std::max(next, m_schema_transaction_version_max);
}

RealmCoordinator::RealmCoordinator(Private) {}

RealmCoordinator::~RealmCoordinator()
{
    {
        std::lock_guard<std::mutex> coordinator_lock(s_coordinator_mutex);
        for (auto it = s_coordinators_per_path.begin(); it != s_coordinators_per_path.end();) {
            if (it->second.expired()) {
                it = s_coordinators_per_path.erase(it);
            }
            else {
                ++it;
            }
        }
    }

    if (m_db) {
        m_db->remove_commit_listener(this);
    }

    // Waits for the worker thread to join
    m_notifier.reset();

    // If there's any active NotificationTokens they'll keep the notifiers alive,
    // so tell the notifiers to release their Transactions so that the DB can
    // be closed immediately.
    // No locking needed here because the worker thread is gone
    for (auto& notifier : m_new_notifiers)
        notifier->release_data();
    for (auto& notifier : m_notifiers)
        notifier->release_data();
}

void RealmCoordinator::unregister_realm(Realm* realm)
{
    util::CheckedLockGuard lock(m_realm_mutex);
    // Normally results notifiers are cleaned up by the background worker thread
    // but if that's disabled we need to ensure that any notifiers from this
    // Realm get cleaned up
    if (!m_config.automatic_change_notifications) {
        util::CheckedLockGuard lock(m_notifier_mutex);
        clean_up_dead_notifiers();
    }
    {
        auto new_end = remove_if(begin(m_weak_realm_notifiers), end(m_weak_realm_notifiers), [=](auto& notifier) {
            return notifier.expired() || notifier.is_for_realm(realm);
        });
        m_weak_realm_notifiers.erase(new_end, end(m_weak_realm_notifiers));
    }
}

// Thread-safety analysis doesn't reasonably handle calling functions on different
// instances of this type
void RealmCoordinator::clear_cache() NO_THREAD_SAFETY_ANALYSIS
{
    std::vector<std::shared_ptr<RealmCoordinator>> coordinators;
    {
        std::lock_guard<std::mutex> lock(s_coordinator_mutex);
        for (auto& weak_coordinator : s_coordinators_per_path) {
            if (auto coordinator = weak_coordinator.second.lock()) {
                coordinators.push_back(coordinator);
            }
        }
        s_coordinators_per_path.clear();
    }

    for (auto& coordinator : coordinators) {
        coordinator->m_notifier = nullptr;

        std::vector<std::shared_ptr<Realm>> realms_to_close;
        {
            // Gather a list of all of the realms which will be removed
            util::CheckedLockGuard lock(coordinator->m_realm_mutex);
            for (auto& weak_realm_notifier : coordinator->m_weak_realm_notifiers) {
                if (auto realm = weak_realm_notifier.realm()) {
                    realms_to_close.push_back(realm);
                }
            }
        }

        // Close all of the previously cached Realms. This can't be done while
        // locks are held as it may try to re-lock them.
        for (auto& realm : realms_to_close)
            realm->close();
    }
}

void RealmCoordinator::clear_all_caches()
{
    std::vector<std::weak_ptr<RealmCoordinator>> to_clear;
    {
        std::lock_guard<std::mutex> lock(s_coordinator_mutex);
        for (auto iter : s_coordinators_per_path) {
            to_clear.push_back(iter.second);
        }
    }
    for (auto weak_coordinator : to_clear) {
        if (auto coordinator = weak_coordinator.lock()) {
            coordinator->clear_cache();
        }
    }
}

void RealmCoordinator::assert_no_open_realms() noexcept
{
#ifdef REALM_DEBUG
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    REALM_ASSERT(s_coordinators_per_path.empty());
#endif
}

void RealmCoordinator::wake_up_notifier_worker()
{
    if (m_notifier) {
        // FIXME: this wakes up the notification workers for all processes and
        // not just us. This might be worth optimizing in the future.
        m_notifier->notify_others();
    }
}

void RealmCoordinator::commit_write(Realm& realm, bool commit_to_disk)
{
    REALM_ASSERT(!m_config.immutable());
    REALM_ASSERT(realm.is_in_transaction());

    Transaction& tr = Realm::Internal::get_transaction(realm);
    VersionID new_version;
    {
        // Need to acquire this lock before committing or another process could
        // perform a write and notify us before we get the chance to set the
        // skip version
        util::CheckedLockGuard l(m_notifier_mutex);
        new_version = tr.commit_and_continue_as_read(commit_to_disk);

        // The skip version must always be the notifier transaction's current
        // version plus one, as we can only skip a prefix and not intermediate
        // transactions. If we have a notifier for the current Realm, then we
        // waited until it finished running in begin_transaction() and this
        // invariant holds. If we don't have any notifiers then we don't need
        // to set the skip version, but more importantly *can't* because we
        // didn't block when starting the write and the notifier transaction
        // may still be on an older version.
        //
        // Note that this relies on the fact that callbacks cannot be added from
        // within write transactions. If they could be, we could hit this point
        // with an implicit-created notifier which ran (and so is in m_notifiers
        // and not m_new_notifiers) but didn't have a callback at the start of
        // the write so we didn't block for it then, but does now have a callback.
        // If we add support for that, we'll need to update this logic.
        bool have_notifiers = std::any_of(m_notifiers.begin(), m_notifiers.end(), [&](auto&& notifier) {
            return notifier->is_for_realm(realm) && notifier->have_callbacks();
        });
        if (have_notifiers) {
            REALM_ASSERT(!m_notifier_skip_version);
            REALM_ASSERT(m_notifier_transaction);
            REALM_ASSERT_3(m_notifier_transaction->get_transact_stage(), ==, DB::transact_Reading);
            REALM_ASSERT_3(m_notifier_transaction->get_version() + 1, ==, new_version.version);
            m_notifier_skip_version = tr.duplicate();
        }
    }

    if (realm.m_binding_context) {
        realm.m_binding_context->did_change({}, {});
    }
    // note: no longer safe to access `realm` or `this` after this point as
    // did_change() may have closed the Realm.
}

void RealmCoordinator::enable_wait_for_change()
{
    m_db->enable_wait_for_change();
}

bool RealmCoordinator::wait_for_change(std::shared_ptr<Transaction> tr)
{
    return m_db->wait_for_change(tr);
}

void RealmCoordinator::wait_for_change_release()
{
    m_db->wait_for_change_release();
}

bool RealmCoordinator::can_advance(Realm& realm)
{
    return realm.last_seen_transaction_version() != m_db->get_version_of_latest_snapshot();
}

// Thread-safety analysis doesn't reasonably handle calling functions on different
// instances of this type
void RealmCoordinator::register_notifier(std::shared_ptr<CollectionNotifier> notifier) NO_THREAD_SAFETY_ANALYSIS
{
    auto& self = Realm::Internal::get_coordinator(*notifier->get_realm());
    {
        util::CheckedLockGuard lock(self.m_notifier_mutex);
        notifier->set_initial_transaction(self.m_new_notifiers);
        self.m_new_notifiers.push_back(std::move(notifier));
    }
}

void RealmCoordinator::clean_up_dead_notifiers()
{
    auto swap_remove = [&](auto& container) {
        bool did_remove = false;
        for (size_t i = 0; i < container.size(); ++i) {
            if (container[i]->is_alive())
                continue;

            // Ensure the notifier is destroyed here even if there's lingering refs
            // to the async notifier elsewhere
            container[i]->release_data();

            if (container.size() > i + 1)
                container[i] = std::move(container.back());
            container.pop_back();
            --i;
            did_remove = true;
        }
        return did_remove;
    };

    if (swap_remove(m_notifiers) && m_notifiers.empty()) {
        m_notifier_transaction = nullptr;
        m_notifier_handover_transaction = nullptr;
        m_notifier_skip_version.reset();
    }
    swap_remove(m_new_notifiers);
}

void RealmCoordinator::on_commit(DB::version_type)
{
    if (m_notifier) {
        m_notifier->notify_others();
    }
}

void RealmCoordinator::on_change()
{
#if REALM_ENABLE_SYNC
    // Invoke realm sync if another process has notified for a change
    if (m_sync_session) {
        auto version = m_db->get_version_of_latest_snapshot();
        SyncSession::Internal::nonsync_transact_notify(*m_sync_session, version);
    }
#endif

    {
        util::CheckedUniqueLock lock(m_running_notifiers_mutex);
        run_async_notifiers();
    }

    util::CheckedLockGuard lock(m_realm_mutex);
    for (auto& realm : m_weak_realm_notifiers) {
        realm.notify();
    }
}

void RealmCoordinator::run_async_notifiers()
{
    util::CheckedUniqueLock lock(m_notifier_mutex);

    clean_up_dead_notifiers();

    if (m_notifiers.empty() && m_new_notifiers.empty()) {
        REALM_ASSERT(!m_notifier_skip_version);
        return;
    }

    if (!m_notifier_transaction) {
        REALM_ASSERT(m_notifiers.empty());
        REALM_ASSERT(!m_notifier_skip_version);
        m_notifier_transaction = m_db->start_read();
    }

    // We need to pick the final version to advance to while the lock is held
    // as otherwise if a commit is made while new notifiers are being advanced
    // we could end up advancing over the skip version. We create a transaction
    // object for it to make sure the version stays valid
    TransactionRef newest_transaction = m_db->start_read();
    VersionID version = newest_transaction->get_version_of_current_transaction();

    auto skip_version = std::move(m_notifier_skip_version);

    // Make a copy of the notifiers vector and then release the lock to avoid
    // blocking other threads trying to register or unregister notifiers while we run them
    decltype(m_notifiers) notifiers;
    if (version != m_notifier_transaction->get_version_of_current_transaction()) {
        // We only want to rerun the existing notifiers if the version has changed.
        // This is both a minor optimization and required for notification
        // skipping to work. The skip logic assumes that the notifier can't be
        // running when suppress_next() is called because it can only be called
        // from within a write transaction, and starting the write transaction
        // would have blocked until the notifier is done running. However, if we
        // run the notifiers at a point where the version isn't changing, that
        // could happen concurrently with a call to suppress_next(), and we
        // could unset skip_next on a callback from that zero-version run
        // rather than the intended one.
        //
        // Spurious wakeups can happen in a few ways: adding a new notifier,
        // adding a new notifier in a different process sharing this Realm file,
        // closing the Realm in a different process, and possibly some other cases.
        notifiers = m_notifiers;
    }
    else {
        REALM_ASSERT(!skip_version);
        if (m_new_notifiers.empty()) {
            // We were spuriously woken up and there isn't actually anything to do
            return;
        }
    }

    auto new_notifiers = std::move(m_new_notifiers);
    m_new_notifiers.clear();
    m_notifiers.insert(m_notifiers.end(), new_notifiers.begin(), new_notifiers.end());
    lock.unlock();

    // Advance all of the new notifiers to the most recent version, if any
    std::vector<TransactionChangeInfo> new_notifier_change_info;
    if (!new_notifiers.empty()) {
        new_notifier_change_info.reserve(new_notifiers.size());
        for (auto& notifier : new_notifiers) {
            if (notifier->version() == version)
                continue;
            new_notifier_change_info.emplace_back();
            notifier->add_required_change_info(new_notifier_change_info.back());
            transaction::parse(*newest_transaction, new_notifier_change_info.back(), notifier->version().version,
                               version.version);
        }
    }

    // If the skip version is set and we have more than one version to process,
    // we need to start with just the skip version so that any suppressed
    // callbacks can ignore the changes from it without missing changes from
    // later versions. If the skip version is set and there aren't any more
    // versions after it, we just want to process with normal processing. See
    // the above note about spurious wakeups for why this is required for
    // correctness and not just a very minor optimization.
    if (skip_version && skip_version->get_version_of_current_transaction() != version) {
        REALM_ASSERT(!notifiers.empty());
        REALM_ASSERT(version >= skip_version->get_version_of_current_transaction());
        TransactionChangeInfo info;
        for (auto& notifier : notifiers)
            notifier->add_required_change_info(info);
        transaction::advance(*m_notifier_transaction, info, skip_version->get_version_of_current_transaction());
        for (auto& notifier : notifiers)
            notifier->run();

        util::CheckedLockGuard lock(m_notifier_mutex);
        for (auto& notifier : notifiers)
            notifier->prepare_handover();
    }

    // Advance the non-new notifiers to the same version as we advanced the new
    // ones to (or the latest if there were no new ones)
    TransactionChangeInfo change_info;
    for (auto& notifier : notifiers) {
        notifier->add_required_change_info(change_info);
    }
    transaction::advance(*m_notifier_transaction, change_info, version);

    {
        // If there's multiple notifiers for a single collection, we only populate
        // the data for the first one during parsing and need to copy it to the
        // others. This is a reverse scan where each collection looks for the
        // first collection with the same id. It is O(N^2), but typically the
        // number of collections observed will be very small.
        auto id = [](auto const& c) {
            return std::tie(c.table_key, c.path, c.obj_key);
        };
        auto& collections = change_info.collections;
        for (size_t i = collections.size(); i > 0; --i) {
            for (size_t j = 0; j < i - 1; ++j) {
                if (id(collections[i - 1]) == id(collections[j])) {
                    collections[i - 1].changes->merge(CollectionChangeBuilder{*collections[j].changes});
                    break;
                }
            }
        }
    }

    // Now that they're at the same version, switch the new notifiers over to
    // the main Transaction used for background work rather than the temporary one
    for (auto& notifier : new_notifiers) {
        notifier->attach_to(m_notifier_transaction);
        notifier->run();
    }

    // Change info is now all ready, so the notifiers can now perform their
    // background work
    for (auto& notifier : notifiers) {
        notifier->run();
    }

    // Reacquire the lock while updating the fields that are actually read on
    // other threads
    util::CheckedLockGuard lock2(m_notifier_mutex);
    for (auto& notifier : new_notifiers) {
        notifier->prepare_handover();
    }
    for (auto& notifier : notifiers) {
        notifier->prepare_handover();
    }
    clean_up_dead_notifiers();
    if (!m_notifiers.empty())
        m_notifier_handover_transaction = m_db->start_read(version);
}

void RealmCoordinator::advance_to_ready(Realm& realm)
{
    // If callbacks close the Realm the last external reference may go away
    // while we're in this function
    auto self = shared_from_this();
    auto tr = Realm::Internal::get_transaction_ref(realm);
    auto current_version = tr->get_version_of_current_transaction();

    std::vector<std::shared_ptr<_impl::CollectionNotifier>> notifiers;

    // Transaction which will pin the version we're packaging for deliver to,
    // to ensure it's not cleaned up between when we release the mutex and when
    // we actually advance (which is not done while holding a lock).
    std::shared_ptr<Transaction> handover_version_tr;
    {
        util::CheckedLockGuard lock(m_notifier_mutex);

        // If there are any new notifiers for this Realm then by definition they
        // haven't run yet and aren't ready
        for (auto& notifier : m_new_notifiers) {
            if (notifier->is_for_realm(realm))
                return;
        }

        for (auto& notifier : m_notifiers) {
            if (!notifier->is_for_realm(realm))
                continue;
            // If the notifier hasn't run it isn't ready and we should do nothing
            if (!notifier->has_run())
                return;
            // package_for_delivery() returning false indicates that it's been
            // unregistered but not yet cleaned up, so it effectively doesn't exist
            if (!notifier->package_for_delivery())
                continue;
            notifiers.push_back(notifier);
        }

        handover_version_tr = m_notifier_handover_transaction;
    }

    if (notifiers.empty()) {
        // If we have no notifiers for this Realm, just advance to latest
        return transaction::advance(tr, realm.m_binding_context.get(), {});
    }

    // If we have notifiers but no transaction, then they've never run before.
    if (!handover_version_tr)
        return;

    auto notifier_version = handover_version_tr->get_version_of_current_transaction();
    // If the most recent write was performed via the Realm instance being
    // advanced, the notifiers can be at an older version than the Realm.
    // This means that there's no advancing to do
    if (notifier_version < current_version)
        return;

    // We can have notifications for the current version if it's the initial
    // notification for a newly added callback or if the write was performed
    // on this Realm instance. There might also be a newer version but we ignore
    // it if so.
    if (notifier_version == current_version) {
        if (realm.m_binding_context)
            realm.m_binding_context->will_send_notifications();
        if (realm.is_closed())
            return;
        for (auto& notifier : notifiers)
            notifier->after_advance();
        if (realm.is_closed())
            return;
        if (realm.m_binding_context)
            realm.m_binding_context->did_send_notifications();
        return;
    }

    // We have notifiers for a newer version, so advance to that
    transaction::advance(tr, realm.m_binding_context.get(),
                         _impl::NotifierPackage(std::move(notifiers), handover_version_tr));
}

std::vector<std::shared_ptr<_impl::CollectionNotifier>> RealmCoordinator::notifiers_for_realm(Realm& realm)
{
    auto pred = [&](auto& notifier) {
        return notifier->is_for_realm(realm);
    };
    std::vector<std::shared_ptr<_impl::CollectionNotifier>> ret;
    std::copy_if(m_new_notifiers.begin(), m_new_notifiers.end(), std::back_inserter(ret), pred);
    std::copy_if(m_notifiers.begin(), m_notifiers.end(), std::back_inserter(ret), pred);
    return ret;
}

bool RealmCoordinator::advance_to_latest(Realm& realm)
{
    // If callbacks close the Realm the last external reference may go away
    // while we're in this function
    auto self = shared_from_this();
    auto tr = Realm::Internal::get_transaction_ref(realm);

    NotifierVector notifiers;
    {
        util::CheckedUniqueLock lock(m_notifier_mutex);
        notifiers = notifiers_for_realm(realm);
    }
    auto pin_tr = package_notifiers(notifiers, m_db->get_version_of_latest_snapshot());

    auto prev_version = tr->get_version_of_current_transaction();
    transaction::advance(tr, realm.m_binding_context.get(), _impl::NotifierPackage(std::move(notifiers), pin_tr));
    return !realm.is_closed() && prev_version != tr->get_version_of_current_transaction();
}

void RealmCoordinator::promote_to_write(Realm& realm)
{
    REALM_ASSERT(!realm.is_in_transaction());
    // If callbacks close the Realm the last external reference may go away
    // while we're in this function
    auto self = shared_from_this();

    util::CheckedUniqueLock lock(m_notifier_mutex);
    auto notifiers = notifiers_for_realm(realm);
    lock.unlock();

    transaction::begin(Realm::Internal::get_transaction_ref(realm), realm.m_binding_context.get(),
                       {std::move(notifiers), this});
}

void RealmCoordinator::process_available_async(Realm& realm)
{
    REALM_ASSERT(!realm.is_in_transaction());
    // If callbacks close the Realm the last external reference may go away
    // while we're in this function
    auto self = shared_from_this();

    auto current_version = realm.current_transaction_version();
    std::vector<std::shared_ptr<_impl::CollectionNotifier>> notifiers;

    {
        util::CheckedLockGuard lock(m_notifier_mutex);
        // No handover transaction means there can't be anything waiting to deliver
        if (!m_notifier_handover_transaction)
            return;
        // If we have a read transaction, it needs to be an exact match in version
        // to the notifications as we're only delivering initial notifications
        // and not advancing.
        if (current_version &&
            current_version != m_notifier_handover_transaction->get_version_of_current_transaction())
            return;

        for (auto& notifier : m_notifiers) {
            if (!notifier->is_for_realm(realm) || !notifier->has_run() || !notifier->package_for_delivery())
                continue;
            notifiers.push_back(notifier);
        }
    }
    if (notifiers.empty())
        return;

    if (realm.m_binding_context)
        realm.m_binding_context->will_send_notifications();
    if (realm.is_closed()) // i.e. the Realm was closed in the callback above
        return;
    for (auto& notifier : notifiers)
        notifier->after_advance();
    if (realm.is_closed()) // i.e. the Realm was closed in the callback above
        return;
    if (realm.m_binding_context)
        realm.m_binding_context->did_send_notifications();
}

TransactionRef RealmCoordinator::package_notifiers(NotifierVector& notifiers, VersionID::version_type target_version)
{
    auto ready = [&] {
        util::CheckedUniqueLock notifier_lock(m_notifier_mutex);
        bool up_to_date =
            m_notifier_handover_transaction &&
            m_notifier_handover_transaction->get_version_of_current_transaction().version >= target_version;
        return std::all_of(begin(notifiers), end(notifiers), [&](auto const& n) {
            return !n->have_callbacks() || (n->has_run() && up_to_date);
        });
    };

    if (!ready()) {
        util::CheckedUniqueLock lock(m_running_notifiers_mutex);
        // The worker thread may have run the notifiers we need while we were
        // waiting for the lock, so re-check
        if (!ready())
            run_async_notifiers();
    }

    util::CheckedUniqueLock notifier_lock(m_notifier_mutex);
    // If the notifiers are still out of date, that means none of them have callbacks
    // so we don't want to block the calling thread to run them.
    if (!m_notifier_handover_transaction ||
        m_notifier_handover_transaction->get_version_of_current_transaction().version < target_version) {
        notifiers.clear();
        return nullptr;
    }

    auto package = [&](auto& notifier) {
        return !notifier->has_run() || !notifier->package_for_delivery();
    };
    notifiers.erase(std::remove_if(begin(notifiers), end(notifiers), package), end(notifiers));
    return notifiers.empty() ? nullptr : m_notifier_handover_transaction;
}

bool RealmCoordinator::compact()
{
    return m_db->compact();
}

void RealmCoordinator::write_copy(std::string_view path, const char* key)
{
    m_db->write_copy(path, key);
}

void RealmCoordinator::async_request_write_mutex(Realm& realm)
{
    auto tr = Realm::Internal::get_transaction_ref(realm);
    m_db->async_request_write_mutex(tr, [realm = realm.shared_from_this()]() mutable {
        auto& scheduler = *realm->scheduler();
        scheduler.invoke([realm = std::move(realm)] {
            Realm::Internal::run_writes(*realm);
        });
    });
}
