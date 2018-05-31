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

#include "shared_realm.hpp"

#include "impl/collection_notifier.hpp"
#include "impl/realm_coordinator.hpp"
#include "impl/transact_log_handler.hpp"

#include "binding_context.hpp"
#include "list.hpp"
#include "object.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "thread_safe_reference.hpp"

#include <realm/db.hpp>
#include <realm/util/scope_exit.hpp>

#if REALM_ENABLE_SYNC
#include "sync/impl/sync_file.hpp"
#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"

#include <realm/sync/history.hpp>
#include <realm/sync/permissions.hpp>
#else
namespace realm {
namespace sync {
    struct PermissionsCache {};
}
}
#endif

using namespace realm;
using namespace realm::_impl;

Realm::Realm(Config config, std::shared_ptr<_impl::RealmCoordinator> coordinator, MakeSharedTag)
: m_config(std::move(config))
, m_execution_context(m_config.execution_context)
{
    if (!coordinator->get_cached_schema(m_schema, m_schema_version, m_schema_transaction_version)) {
        m_group = coordinator->begin_read();
        read_schema_from_group_if_needed();
        coordinator->cache_schema(m_schema, m_schema_version, m_schema_transaction_version);
        m_group = nullptr;
    }

    m_coordinator = std::move(coordinator);
}

#if REALM_ENABLE_SYNC
static bool is_nonupgradable_history(IncompatibleHistories const& ex)
{
    // FIXME: Replace this with a proper specific exception type once Core adds support for it.
    return ex.what() == std::string("Incompatible histories. Nonupgradable history schema");
}
#endif

Realm::~Realm()
{
    if (m_coordinator) {
        m_coordinator->unregister_realm(this);
    }
}

bool Realm::is_partial() const noexcept
{
#if REALM_ENABLE_SYNC
    return m_config.sync_config && m_config.sync_config->is_partial;
#else
    return false;
#endif
}

Group& Realm::read_group()
{
    verify_open();

    if (!m_group)
        begin_read(VersionID{});
    return *m_group;
}

Transaction& Realm::transaction()
{
    REALM_ASSERT(!m_config.immutable());
    return static_cast<Transaction&>(read_group());
}

Transaction& Realm::transaction() const
{
    REALM_ASSERT(!m_config.immutable());
    // FIXME: read_group() is not even remotly const
    return static_cast<Transaction&>(const_cast<Realm*>(this)->read_group());
}

std::shared_ptr<Transaction> Realm::transaction_ref()
{
    return std::static_pointer_cast<Transaction>(m_group);
}

void Realm::Internal::begin_read(Realm& realm, VersionID version_id)
{
    realm.begin_read(version_id);
}

void Realm::begin_read(VersionID version_id)
{
    REALM_ASSERT(!m_group);
    m_group = m_coordinator->begin_read(version_id);
    add_schema_change_handler();
    read_schema_from_group_if_needed();
}

SharedRealm Realm::get_shared_realm(Config config)
{
    auto coordinator = RealmCoordinator::get_coordinator(config.path);
    return coordinator->get_realm(std::move(config));
}

void Realm::set_schema(Schema const& reference, Schema schema)
{
    m_dynamic_schema = false;
    schema.copy_table_columns_from(reference);
    m_schema = std::move(schema);
    notify_schema_changed();
}

void Realm::read_schema_from_group_if_needed()
{
    if (m_config.immutable()) {
        REALM_ASSERT(m_group);
        if (m_schema.empty()) {
            m_schema_version = ObjectStore::get_schema_version(*m_group);
            m_schema = ObjectStore::schema_from_group(*m_group);
        }
        return;
    }

    Group& group = read_group();
    auto current_version = transaction().get_version_of_current_transaction().version;
    if (m_schema_transaction_version == current_version)
        return;

    m_schema_transaction_version = current_version;
    m_schema_version = ObjectStore::get_schema_version(group);
    auto schema = ObjectStore::schema_from_group(group);
    if (m_coordinator)
        m_coordinator->cache_schema(schema, m_schema_version,
                                    m_schema_transaction_version);

    if (m_dynamic_schema) {
        if (m_schema == schema) {
            // The structure of the schema hasn't changed. Bring the table column indices up to date.
            m_schema.copy_table_columns_from(schema);
        }
        else {
            // The structure of the schema has changed, so replace our copy of the schema.
            m_schema = std::move(schema);
        }
    }
    else {
        ObjectStore::verify_valid_external_changes(m_schema.compare(schema));
        m_schema.copy_table_columns_from(schema);
    }
    notify_schema_changed();
}

bool Realm::reset_file(Schema& schema, std::vector<SchemaChange>& required_changes)
{
    // FIXME: this does not work if multiple processes try to open the file at
    // the same time, or even multiple threads if there is not any external
    // synchronization. The latter is probably fixable, but making it
    // multi-process-safe requires some sort of multi-process exclusive lock
    m_group = nullptr;
    m_coordinator->close();
    util::File::remove(m_config.path);

    m_schema = ObjectStore::schema_from_group(read_group());
    m_schema_version = ObjectStore::get_schema_version(read_group());
    required_changes = m_schema.compare(schema);
    m_coordinator->clear_schema_cache_and_set_schema_version(m_schema_version);
    return false;
}

bool Realm::schema_change_needs_write_transaction(Schema& schema,
                                                  std::vector<SchemaChange>& changes,
                                                  uint64_t version)
{
    if (version == m_schema_version && changes.empty())
        return false;

    switch (m_config.schema_mode) {
        case SchemaMode::Automatic:
            if (version < m_schema_version && m_schema_version != ObjectStore::NotVersioned)
                throw InvalidSchemaVersionException(m_schema_version, version);
            return true;

        case SchemaMode::Immutable:
            if (version != m_schema_version)
                throw InvalidSchemaVersionException(m_schema_version, version);
            REALM_FALLTHROUGH;
        case SchemaMode::ReadOnlyAlternative:
            ObjectStore::verify_compatible_for_immutable_and_readonly(changes);
            return false;

        case SchemaMode::ResetFile:
            if (m_schema_version == ObjectStore::NotVersioned)
                return true;
            if (m_schema_version == version && !ObjectStore::needs_migration(changes))
                return true;
            reset_file(schema, changes);
            return true;

        case SchemaMode::Additive: {
            bool will_apply_index_changes = version > m_schema_version;
            if (ObjectStore::verify_valid_additive_changes(changes, will_apply_index_changes))
                return true;
            return version != m_schema_version;
        }

        case SchemaMode::Manual:
            if (version < m_schema_version && m_schema_version != ObjectStore::NotVersioned)
                throw InvalidSchemaVersionException(m_schema_version, version);
            if (version == m_schema_version) {
                ObjectStore::verify_no_changes_required(changes);
                REALM_UNREACHABLE(); // changes is non-empty so above line always throws
            }
            return true;
    }
    REALM_COMPILER_HINT_UNREACHABLE();
}

Schema Realm::get_full_schema()
{
    if (!m_config.immutable())
        refresh();

    // If the user hasn't specified a schema previously then m_schema is always
    // the full schema
    if (m_dynamic_schema)
        return m_schema;

    // Otherwise we may have a subset of the file's schema, so we need to get
    // the complete thing to calculate what changes to make
    if (m_config.immutable())
        return ObjectStore::schema_from_group(read_group());

    Schema actual_schema;
    uint64_t actual_version;
    uint64_t version = -1;
    bool got_cached = m_coordinator->get_cached_schema(actual_schema, actual_version, version);
    if (!got_cached || version != transaction().get_version_of_current_transaction().version)
        return ObjectStore::schema_from_group(read_group());
    return actual_schema;
}

void Realm::set_schema_subset(Schema schema)
{
    REALM_ASSERT(m_dynamic_schema);
    REALM_ASSERT(m_schema_version != ObjectStore::NotVersioned);

    std::vector<SchemaChange> changes = m_schema.compare(schema);
    switch (m_config.schema_mode) {
        case SchemaMode::Automatic:
        case SchemaMode::ResetFile:
            ObjectStore::verify_no_migration_required(changes);
            break;

        case SchemaMode::Immutable:
        case SchemaMode::ReadOnlyAlternative:
            ObjectStore::verify_compatible_for_immutable_and_readonly(changes);
            break;

        case SchemaMode::Additive:
            ObjectStore::verify_valid_additive_changes(changes);
            break;

        case SchemaMode::Manual:
            ObjectStore::verify_no_changes_required(changes);
            break;
    }

    set_schema(m_schema, std::move(schema));
}

void Realm::update_schema(Schema schema, uint64_t version, MigrationFunction migration_function,
                          DataInitializationFunction initialization_function, bool in_transaction)
{
    schema.validate();

    Schema actual_schema = get_full_schema();
    std::vector<SchemaChange> required_changes = actual_schema.compare(schema);

    if (!schema_change_needs_write_transaction(schema, required_changes, version)) {
        set_schema(actual_schema, std::move(schema));
        return;
    }
    // Either the schema version has changed or we need to do non-migration changes

    if (!in_transaction) {
        transaction().promote_to_write();

        // Beginning the write transaction may have advanced the version and left
        // us with nothing to do if someone else initialized the schema on disk
        if (m_new_schema) {
            actual_schema = *m_new_schema;
            required_changes = actual_schema.compare(schema);
            if (!schema_change_needs_write_transaction(schema, required_changes, version)) {
                cancel_transaction();
                cache_new_schema();
                set_schema(actual_schema, std::move(schema));
                return;
            }
        }
        cache_new_schema();
    }

    // Cancel the write transaction if we exit this function before committing it
    auto cleanup = util::make_scope_exit([&]() noexcept {
        // When in_transaction is true, caller is responsible to cancel the transaction.
        if (!in_transaction && is_in_transaction())
            cancel_transaction();
    });

    uint64_t old_schema_version = m_schema_version;
    bool additive = m_config.schema_mode == SchemaMode::Additive;
    if (migration_function && !additive) {
        auto wrapper = [&] {
            auto config = m_config;
            config.schema_mode = SchemaMode::ReadOnlyAlternative;
            config.schema = util::none;
            // Don't go through the normal codepath for opening a Realm because
            // we're using a mismatched config
            auto old_realm = std::make_shared<Realm>(std::move(config),
                                                     m_coordinator, MakeSharedTag{});
            migration_function(old_realm, shared_from_this(), m_schema);
        };

        // migration function needs to see the target schema on the "new" Realm
        std::swap(m_schema, schema);
        std::swap(m_schema_version, version);
        m_in_migration = true;
        auto restore = util::make_scope_exit([&]() noexcept {
            std::swap(m_schema, schema);
            std::swap(m_schema_version, version);
            m_in_migration = false;
        });

        ObjectStore::apply_schema_changes(read_group(), version, m_schema, m_schema_version,
                                          m_config.schema_mode, required_changes, util::none, wrapper);
    }
    else {
        util::Optional<std::string> sync_user_id;
#if REALM_ENABLE_SYNC
        if (m_config.sync_config && m_config.sync_config->is_partial)
            sync_user_id = m_config.sync_config->user->identity();
#endif
        ObjectStore::apply_schema_changes(read_group(), m_schema_version, schema, version,
                                          m_config.schema_mode, required_changes, std::move(sync_user_id));
        REALM_ASSERT_DEBUG(additive || (required_changes = ObjectStore::schema_from_group(read_group()).compare(schema)).empty());
    }

    if (initialization_function && old_schema_version == ObjectStore::NotVersioned) {
        // Initialization function needs to see the latest schema
        uint64_t temp_version = ObjectStore::get_schema_version(read_group());
        std::swap(m_schema, schema);
        std::swap(m_schema_version, temp_version);
        auto restore = util::make_scope_exit([&]() noexcept {
            std::swap(m_schema, schema);
            std::swap(m_schema_version, temp_version);
        });
        initialization_function(shared_from_this());
    }

    if (!in_transaction) {
        commit_transaction();
    }

    m_schema = std::move(schema);
    m_schema_version = ObjectStore::get_schema_version(read_group());
    m_dynamic_schema = false;
    m_coordinator->clear_schema_cache_and_set_schema_version(version);
    notify_schema_changed();
}

void Realm::add_schema_change_handler()
{
    if (m_config.immutable())
        return;
    m_group->set_schema_change_notification_handler([&] {
        m_new_schema = ObjectStore::schema_from_group(read_group());
        m_schema_version = ObjectStore::get_schema_version(read_group());
        if (m_dynamic_schema) {
            m_schema = *m_new_schema;
        }
        else
            m_schema.copy_table_columns_from(*m_new_schema);

        notify_schema_changed();
    });
}

void Realm::cache_new_schema()
{
    auto new_version = transaction().get_version_of_current_transaction().version;
    if (m_new_schema)
        m_coordinator->cache_schema(std::move(*m_new_schema),
                                    m_schema_version, new_version);
    else
        m_coordinator->advance_schema_cache(m_schema_transaction_version, new_version);
    m_schema_transaction_version = new_version;
    m_new_schema = util::none;
}

void Realm::translate_schema_error()
{
    // Read the new (incompatible) schema without changing our read transaction
    auto new_schema = ObjectStore::schema_from_group(*m_coordinator->begin_read());

    // Should always throw
    ObjectStore::verify_valid_external_changes(m_schema.compare(new_schema, true));

    // Something strange happened so just rethrow the old exception
    throw;
}

void Realm::notify_schema_changed()
{
    if (m_binding_context) {
        m_binding_context->schema_did_change(m_schema);
    }
}

static void check_read_write(Realm *realm)
{
    if (realm->config().immutable()) {
        throw InvalidTransactionException("Can't perform transactions on read-only Realms.");
    }
}

static void check_write(Realm* realm)
{
    if (realm->config().immutable() || realm->config().read_only_alternative()) {
        throw InvalidTransactionException("Can't perform transactions on read-only Realms.");
    }
}

void Realm::verify_thread() const
{
    if (!m_execution_context.contains<std::thread::id>())
        return;

    auto thread_id = m_execution_context.get<std::thread::id>();
    if (thread_id != std::this_thread::get_id())
        throw IncorrectThreadException();
}

void Realm::verify_in_write() const
{
    if (!is_in_transaction()) {
        throw InvalidTransactionException("Cannot modify managed objects outside of a write transaction.");
    }
}

void Realm::verify_open() const
{
    if (is_closed()) {
        throw ClosedRealmException();
    }
}

bool Realm::is_in_transaction() const noexcept
{
    return !m_config.immutable()
        && !is_closed()
        && transaction().get_transact_stage() == DB::transact_Writing;
}

void Realm::begin_transaction()
{
    verify_thread();
    check_write(this);

    if (is_in_transaction()) {
        throw InvalidTransactionException("The Realm is already in a write transaction");
    }

    // Any of the callbacks to user code below could drop the last remaining
    // strong reference to `this`
    auto retain_self = shared_from_this();

    // If we're already in the middle of sending notifications, just begin the
    // write transaction without sending more notifications. If this actually
    // advances the read version this could leave the user in an inconsistent
    // state, but that's unavoidable.
    if (m_is_sending_notifications) {
        _impl::NotifierPackage notifiers;
        transaction::begin(transaction_ref(), m_binding_context.get(), notifiers);
        return;
    }

    // make sure we have a read transaction
    read_group();

    m_is_sending_notifications = true;
    auto cleanup = util::make_scope_exit([this]() noexcept {
        m_is_sending_notifications = false;
    });

    try {
        m_coordinator->promote_to_write(*this);
    }
    catch (_impl::UnsupportedSchemaChange const&) {
        translate_schema_error();
    }
    cache_new_schema();
}

void Realm::commit_transaction()
{
    check_write(this);
    verify_thread();

    if (!is_in_transaction()) {
        throw InvalidTransactionException("Can't commit a non-existing write transaction");
    }

    m_coordinator->commit_write(*this);
    cache_new_schema();
    invalidate_permission_cache();
}

void Realm::cancel_transaction()
{
    check_write(this);
    verify_thread();

    if (!is_in_transaction()) {
        throw InvalidTransactionException("Can't cancel a non-existing write transaction");
    }

    transaction::cancel(transaction(), m_binding_context.get());
    invalidate_permission_cache();
}

void Realm::invalidate()
{
    verify_open();
    verify_thread();
    check_read_write(this);

    if (m_is_sending_notifications) {
        return;
    }

    if (is_in_transaction()) {
        cancel_transaction();
    }

    m_permissions_cache = nullptr;
    m_group = nullptr;
}

bool Realm::compact()
{
    verify_thread();

    if (m_config.immutable() || m_config.read_only_alternative()) {
        throw InvalidTransactionException("Can't compact a read-only Realm");
    }
    if (is_in_transaction()) {
        throw InvalidTransactionException("Can't compact a Realm within a write transaction");
    }

    m_group = nullptr;
//    return m_shared_group->compact();
    return false;
}

void Realm::write_copy(StringData path, BinaryData key)
{
    if (key.data() && key.size() != 64) {
        throw InvalidEncryptionKeyException();
    }
    verify_thread();
    try {
        read_group().write(path, key.data());
    }
    catch (...) {
        _impl::translate_file_exception(path);
    }
}

OwnedBinaryData Realm::write_copy()
{
    verify_thread();
    BinaryData buffer = read_group().write_to_mem();

    // Since OwnedBinaryData does not have a constructor directly taking
    // ownership of BinaryData, we have to do this to avoid copying the buffer
    return OwnedBinaryData(std::unique_ptr<char[]>((char*)buffer.data()), buffer.size());
}

void Realm::notify()
{
    if (is_closed() || is_in_transaction()) {
        return;
    }

    verify_thread();
    invalidate_permission_cache();

    // Any of the callbacks to user code below could drop the last remaining
    // strong reference to `this`
    auto retain_self = shared_from_this();

    if (m_binding_context) {
        m_binding_context->before_notify();
    }

    auto cleanup = util::make_scope_exit([this]() noexcept { m_is_sending_notifications = false; });
    if (!m_coordinator->can_advance(*this)) {
        m_is_sending_notifications = true;
        m_coordinator->process_available_async(*this);
        return;
    }

    if (m_binding_context) {
        m_binding_context->changes_available();

        // changes_available() may have advanced the read version, and if
        // so we don't need to do anything further
        if (!m_coordinator->can_advance(*this))
            return;
    }

    m_is_sending_notifications = true;
    if (m_auto_refresh) {
        if (m_group) {
            try {
                m_coordinator->advance_to_ready(*this);
            }
            catch (_impl::UnsupportedSchemaChange const&) {
                translate_schema_error();
            }
            cache_new_schema();
        }
        else  {
            if (m_binding_context) {
                m_binding_context->did_change({}, {});
            }
            if (!is_closed()) {
                m_coordinator->process_available_async(*this);
            }
        }
    }
}

bool Realm::refresh()
{
    verify_thread();
    check_read_write(this);

    // can't be any new changes if we're in a write transaction
    if (is_in_transaction()) {
        return false;
    }
    // don't advance if we're already in the process of advancing as that just
    // makes things needlessly complicated
    if (m_is_sending_notifications) {
        return false;
    }
    invalidate_permission_cache();

    // Any of the callbacks to user code below could drop the last remaining
    // strong reference to `this`
    auto retain_self = shared_from_this();

    m_is_sending_notifications = true;
    auto cleanup = util::make_scope_exit([this]() noexcept { m_is_sending_notifications = false; });

    if (m_binding_context) {
        m_binding_context->before_notify();
    }
    if (m_group) {
        try {
            bool version_changed = m_coordinator->advance_to_latest(*this);
            if (is_closed())
                return false;
            cache_new_schema();
            return version_changed;
        }
        catch (_impl::UnsupportedSchemaChange const&) {
            translate_schema_error();
        }
    }

    // No current read transaction, so just create a new one
    read_group();
    m_coordinator->process_available_async(*this);
    return true;
}

bool Realm::can_deliver_notifications() const noexcept
{
    if (m_config.immutable()) {
        return false;
    }

    if (m_binding_context && !m_binding_context->can_deliver_notifications()) {
        return false;
    }

    return true;
}

uint64_t Realm::get_schema_version(const Realm::Config &config)
{
    auto coordinator = RealmCoordinator::get_coordinator(config);
    auto version = coordinator->get_schema_version();
    if (version == ObjectStore::NotVersioned)
        version = ObjectStore::get_schema_version(*coordinator->begin_read());
    return version;
}

void Realm::close()
{
    if (m_coordinator) {
        m_coordinator->unregister_realm(this);
    }
    if (!m_config.immutable()) {
        transaction().end_read();
    }

    m_permissions_cache = nullptr;
    m_group = nullptr;
    m_binding_context = nullptr;
    m_coordinator = nullptr;
}

util::Optional<int> Realm::file_format_upgraded_from_version() const
{
    if (upgrade_initial_version != upgrade_final_version) {
        return upgrade_initial_version;
    }
    return util::none;
}

template <typename T>
realm::ThreadSafeReference<T> Realm::obtain_thread_safe_reference(T const& value)
{
    verify_thread();
    if (is_in_transaction()) {
        throw InvalidTransactionException("Cannot obtain thread safe reference during a write transaction.");
    }
    return ThreadSafeReference<T>(value);
}

template ThreadSafeReference<Object> Realm::obtain_thread_safe_reference(Object const& value);
template ThreadSafeReference<List> Realm::obtain_thread_safe_reference(List const& value);
template ThreadSafeReference<Results> Realm::obtain_thread_safe_reference(Results const& value);

template <typename T>
T Realm::resolve_thread_safe_reference(ThreadSafeReference<T> reference)
{
    verify_thread();
    if (is_in_transaction()) {
        throw InvalidTransactionException("Cannot resolve thread safe reference during a write transaction.");
    }
    invalidate_permission_cache();

    // Any of the callbacks to user code below could drop the last remaining
    // strong reference to `this`
    auto retain_self = shared_from_this();

//    VersionID reference_version(reference.m_version_id);
    VersionID reference_version; // FIXME

    // Ensure we're on the same version as the reference
    if (!m_group) {
        // A read transaction doesn't yet exist, so create at the reference's version
        begin_read(reference_version);
    }
    else {
        // A read transaction does exist, but let's make sure that its version matches the reference's
        auto current_version = transaction().get_version_of_current_transaction();
        if (reference_version == current_version) {
            return std::move(reference).import_into(transaction());
        }

        refresh();

        current_version = transaction().get_version_of_current_transaction();

        // If the reference's version is behind, advance it to our version
        if (reference_version < current_version) {
            auto transaction = std::static_pointer_cast<Transaction>(m_coordinator->begin_read(reference_version));
            T imported_value = std::move(reference).import_into(*transaction);
            transaction->advance_read(current_version);
            if (!imported_value.is_valid())
                return T{};
            reference = ThreadSafeReference<T>(imported_value);
        }
    }

    return std::move(reference).import_into(transaction());
}

template Object Realm::resolve_thread_safe_reference(ThreadSafeReference<Object> reference);
template List Realm::resolve_thread_safe_reference(ThreadSafeReference<List> reference);
template Results Realm::resolve_thread_safe_reference(ThreadSafeReference<Results> reference);

#if REALM_ENABLE_SYNC
static_assert(static_cast<int>(ComputedPrivileges::Read) == static_cast<int>(sync::Privilege::Read), "");
static_assert(static_cast<int>(ComputedPrivileges::Update) == static_cast<int>(sync::Privilege::Update), "");
static_assert(static_cast<int>(ComputedPrivileges::Delete) == static_cast<int>(sync::Privilege::Delete), "");
static_assert(static_cast<int>(ComputedPrivileges::SetPermissions) == static_cast<int>(sync::Privilege::SetPermissions), "");
static_assert(static_cast<int>(ComputedPrivileges::Query) == static_cast<int>(sync::Privilege::Query), "");
static_assert(static_cast<int>(ComputedPrivileges::Create) == static_cast<int>(sync::Privilege::Create), "");
static_assert(static_cast<int>(ComputedPrivileges::ModifySchema) == static_cast<int>(sync::Privilege::ModifySchema), "");

static constexpr const uint8_t s_allRealmPrivileges = sync::Privilege::Read
                                                    | sync::Privilege::Update
                                                    | sync::Privilege::SetPermissions
                                                    | sync::Privilege::ModifySchema;
static constexpr const uint8_t s_allClassPrivileges = sync::Privilege::Read
                                                    | sync::Privilege::Update
                                                    | sync::Privilege::Create
                                                    | sync::Privilege::Query
                                                    | sync::Privilege::SetPermissions;
static constexpr const uint8_t s_allObjectPrivileges = sync::Privilege::Read
                                                     | sync::Privilege::Update
                                                     | sync::Privilege::Delete
                                                     | sync::Privilege::SetPermissions;

bool Realm::init_permission_cache()
{
    verify_thread();

    if (m_permissions_cache) {
        // Rather than trying to track changes to permissions tables, just skip the caching
        // entirely within write transactions for now
        if (is_in_transaction())
            m_permissions_cache->clear();
        return true;
    }

    // Admin users bypass permissions checks outside of the logic in PermissionsCache
    if (m_config.sync_config && m_config.sync_config->is_partial && !m_config.sync_config->user->is_admin()) {
        m_permissions_cache = std::make_unique<sync::PermissionsCache>(read_group(), m_config.sync_config->user->identity());
        return true;
    }
    return false;
}

void Realm::invalidate_permission_cache()
{
    if (m_permissions_cache)
        m_permissions_cache->clear();
}

ComputedPrivileges Realm::get_privileges()
{
    if (!init_permission_cache())
        return static_cast<ComputedPrivileges>(s_allRealmPrivileges);
    return static_cast<ComputedPrivileges>(m_permissions_cache->get_realm_privileges() & s_allRealmPrivileges);
}

static uint8_t inherited_mask(uint32_t privileges)
{
    uint8_t mask = ~0;
    if (!(privileges & sync::Privilege::Read))
        mask = 0;
    else if (!(privileges & sync::Privilege::Update))
        mask = static_cast<uint8_t>(sync::Privilege::Read | sync::Privilege::Query);
    return mask;
}

ComputedPrivileges Realm::get_privileges(StringData object_type)
{
    if (!init_permission_cache())
        return static_cast<ComputedPrivileges>(s_allClassPrivileges);
    auto privileges = inherited_mask(m_permissions_cache->get_realm_privileges())
                    & m_permissions_cache->get_class_privileges(object_type);
    return static_cast<ComputedPrivileges>(privileges & s_allClassPrivileges);
}

ComputedPrivileges Realm::get_privileges(RowExpr row)
{
    if (!init_permission_cache())
        return static_cast<ComputedPrivileges>(s_allObjectPrivileges);

    auto& table = *row.get_table();
    auto object_type = ObjectStore::object_type_for_table_name(table.get_name());
    sync::GlobalID global_id{object_type, sync::object_id_for_row(read_group(), table, row.get_index())};
    auto privileges = inherited_mask(m_permissions_cache->get_realm_privileges())
                    & inherited_mask(m_permissions_cache->get_class_privileges(object_type))
                    & m_permissions_cache->get_object_privileges(global_id);
    return static_cast<ComputedPrivileges>(privileges & s_allObjectPrivileges);
}
#else
void Realm::invalidate_permission_cache() { }
#endif

MismatchedConfigException::MismatchedConfigException(StringData message, StringData path)
: std::logic_error(util::format(message.data(), path)) { }

MismatchedRealmException::MismatchedRealmException(StringData message)
: std::logic_error(message.data()) { }
