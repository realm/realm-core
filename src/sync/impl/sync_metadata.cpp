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

#include "sync/impl/sync_metadata.hpp"
#include "impl/realm_coordinator.hpp"

#include "object_schema.hpp"
#include "object_store.hpp"
#include "property.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "util/uuid.hpp"
#if REALM_PLATFORM_APPLE
#include "impl/apple/keychain_helper.hpp"
#endif

#include <realm/db.hpp>
#include <realm/table.hpp>

namespace {
static const char * const c_sync_userMetadata = "UserMetadata";
static const char * const c_sync_identityMetadata = "UserIdentity";
static const char * const c_sync_app_metadata = "AppMetadata";

static const char * const c_sync_current_user_identity = "current_user_identity";

/* User keys*/
static const char * const c_sync_marked_for_removal = "marked_for_removal";
static const char * const c_sync_identity = "identity";
static const char * const c_sync_local_uuid = "local_uuid";
static const char * const c_sync_refresh_token = "refresh_token";
static const char * const c_sync_access_token = "access_token";
static const char * const c_sync_identities = "identities";
static const char * const c_sync_state = "state";
static const char * const c_sync_device_id = "device_id";

/* User Profile keys */
static const char * const c_sync_profile = "profile";
static const char * const c_sync_profile_name = "name";
static const char * const c_sync_profile_first_name = "first_name";
static const char * const c_sync_profile_last_name = "last_name";
static const char * const c_sync_profile_picture_url = "picture_url";
static const char * const c_sync_profile_email = "email";
static const char * const c_sync_profile_gender = "gender";
static const char * const c_sync_profile_birthday = "birthday";
static const char * const c_sync_profile_min_age = "min_age";
static const char * const c_sync_profile_max_age = "max_age";

/* Identity keys */
static const char * const c_sync_user_id = "id";
static const char * const c_sync_provider_type = "provider_type";

static const char * const c_sync_fileActionMetadata = "FileActionMetadata";
static const char * const c_sync_original_name = "original_name";
static const char * const c_sync_new_name = "new_name";
static const char * const c_sync_action = "action";
static const char * const c_sync_url = "url";

static const char * const c_sync_clientMetadata = "ClientMetadata";
static const char * const c_sync_uuid = "uuid";

static const char * const c_sync_app_metadata_id = "id";
static const char * const c_sync_app_metadata_deployment_model = "deployment_model";
static const char * const c_sync_app_metadata_location         = "location";
static const char * const c_sync_app_metadata_hostname         = "hostname";
static const char * const c_sync_app_metadata_ws_hostname      = "ws_hostname";

realm::Schema make_schema()
{
    using namespace realm;
    return Schema{
        {c_sync_identityMetadata, {
            {c_sync_user_id, PropertyType::String},
            {c_sync_provider_type, PropertyType::String}
        }},
        {c_sync_profile, {
            {c_sync_profile_name, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_first_name, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_last_name, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_picture_url, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_gender, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_birthday, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_email, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_max_age, PropertyType::String|PropertyType::Nullable},
            {c_sync_profile_min_age, PropertyType::String|PropertyType::Nullable}
        }},
        {c_sync_userMetadata, {
            {c_sync_identity, PropertyType::String},
            {c_sync_local_uuid, PropertyType::String},
            {c_sync_marked_for_removal, PropertyType::Bool},
            {c_sync_refresh_token, PropertyType::String|PropertyType::Nullable},
            {c_sync_provider_type, PropertyType::String},
            {c_sync_access_token, PropertyType::String|PropertyType::Nullable},
            {c_sync_identities, PropertyType::Object|PropertyType::Array, c_sync_identityMetadata},
            {c_sync_profile, PropertyType::Object|PropertyType::Nullable, c_sync_profile},
            {c_sync_state, PropertyType::Int},
            {c_sync_device_id, PropertyType::String}
        }},
        {c_sync_fileActionMetadata, {
            {c_sync_original_name, PropertyType::String, Property::IsPrimary{true}},
            {c_sync_new_name, PropertyType::String|PropertyType::Nullable},
            {c_sync_action, PropertyType::Int},
            {c_sync_url, PropertyType::String},
            {c_sync_identity, PropertyType::String},
        }},
        {c_sync_clientMetadata, {
            {c_sync_uuid, PropertyType::String},
        }},
        {c_sync_current_user_identity, {
            {c_sync_current_user_identity, PropertyType::String}
        }},
        {c_sync_app_metadata, {
            {c_sync_app_metadata_id, PropertyType::Int, Property::IsPrimary{true}},
            {c_sync_app_metadata_deployment_model, PropertyType::String},
            {c_sync_app_metadata_location, PropertyType::String},
            {c_sync_app_metadata_hostname, PropertyType::String},
            {c_sync_app_metadata_ws_hostname, PropertyType::String}
        }}
    };
}

} // anonymous namespace

namespace realm {

// MARK: - Sync metadata manager

SyncMetadataManager::SyncMetadataManager(std::string path,
                                         bool should_encrypt,
                                         util::Optional<std::vector<char>> encryption_key)
{
    constexpr uint64_t SCHEMA_VERSION = 4;

    Realm::Config config;
    config.automatic_change_notifications = false;
    config.path = path;
    config.schema = make_schema();
    config.schema_version = SCHEMA_VERSION;
    config.schema_mode = SchemaMode::Automatic;
#if REALM_PLATFORM_APPLE
    if (should_encrypt && !encryption_key) {
        encryption_key = keychain::metadata_realm_encryption_key(util::File::exists(path));
    }
#endif
    if (should_encrypt) {
        if (!encryption_key) {
            throw std::invalid_argument("Metadata Realm encryption was specified, but no encryption key was provided.");
        }
        config.encryption_key = std::move(*encryption_key);
    }

    config.migration_function = [](SharedRealm old_realm, SharedRealm realm, Schema&) {
        if (old_realm->schema_version() < 2) {
            TableRef old_table = ObjectStore::table_for_object_type(old_realm->read_group(), c_sync_userMetadata);
            TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_userMetadata);

            // Column indices.
            ColKey old_idx_identity = old_table->get_column_key(c_sync_identity);
            ColKey old_idx_url = old_table->get_column_key(c_sync_provider_type);
            ColKey idx_local_uuid = table->get_column_key(c_sync_local_uuid);
            ColKey idx_url = table->get_column_key(c_sync_provider_type);

            auto to = table->begin();
            for (auto& from : *old_table) {
                REALM_ASSERT(to != table->end());
                // Set the UUID equal to the user identity for existing users.
                auto identity = from.get<String>(old_idx_identity);
                to->set(idx_local_uuid, identity);
                // Migrate the auth server URLs to a non-nullable property.
                auto url = from.get<String>(old_idx_url);
                to->set<String>(idx_url, url.is_null() ? "" : url);
                ++to;
            }
        }
    };

    SharedRealm realm = Realm::get_shared_realm(config);

    // Get data about the (hardcoded) schemas
    auto object_schema = realm->schema().find(c_sync_userMetadata);
    m_user_schema = {
        object_schema->persisted_properties[0].column_key,
        object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key,
        object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key,
        object_schema->persisted_properties[5].column_key,
        object_schema->persisted_properties[6].column_key,
        object_schema->persisted_properties[7].column_key,
        object_schema->persisted_properties[8].column_key,
        object_schema->persisted_properties[9].column_key
    };

    object_schema = realm->schema().find(c_sync_fileActionMetadata);
    m_file_action_schema = {
        object_schema->persisted_properties[0].column_key,
        object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key,
        object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key,
    };

    object_schema = realm->schema().find(c_sync_clientMetadata);
    m_client_schema = {
        object_schema->persisted_properties[0].column_key,
    };

    object_schema = realm->schema().find(c_sync_current_user_identity);
    m_current_user_identity_schema = {
        object_schema->persisted_properties[0].column_key
    };

    object_schema = realm->schema().find(c_sync_profile);
    m_profile_schema = {
        object_schema->persisted_properties[0].column_key,
        object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key,
        object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key,
        object_schema->persisted_properties[5].column_key,
        object_schema->persisted_properties[6].column_key,
        object_schema->persisted_properties[7].column_key
    };

    object_schema = realm->schema().find(c_sync_app_metadata);
    m_app_metadata_schema = {
        object_schema->persisted_properties[0].column_key,
        object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key,
        object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key
    };

    m_metadata_config = std::move(config);

    m_client_uuid = [&]() -> std::string {
        TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_clientMetadata);
        if (table->is_empty()) {
            realm->begin_transaction();
            if (table->is_empty()) {
                auto uuid = util::uuid_string();
                table->create_object().set(m_client_schema.idx_uuid, uuid);
                realm->commit_transaction();
                return uuid;
            }
            realm->cancel_transaction();
        }
        return table->begin()->get<String>(m_client_schema.idx_uuid);
    }();
}

SyncUserMetadataResults SyncMetadataManager::all_unmarked_users() const
{
    return get_users(false);
}

SyncUserMetadataResults SyncMetadataManager::all_users_marked_for_removal() const
{
    return get_users(true);
}

SyncUserMetadataResults SyncMetadataManager::get_users(bool marked) const
{
    auto realm = get_realm();
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_userMetadata);
    Query query = table->where().equal(m_user_schema.idx_marked_for_removal, marked);

    Results results(realm, std::move(query));
    return SyncUserMetadataResults(std::move(results), std::move(realm), m_user_schema);
}

util::Optional<std::string> SyncMetadataManager::get_current_user_identity() const
{
    auto realm = get_realm();
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_current_user_identity);

    if (!table->is_empty()) {
        auto first = table->begin();
        return util::Optional<std::string>(first->get<String>(c_sync_current_user_identity));
    }

    return util::Optional<std::string>();
}

SyncFileActionMetadataResults SyncMetadataManager::all_pending_actions() const
{
    auto realm = get_realm();
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_fileActionMetadata);
    Results results(realm, table->where());
    return SyncFileActionMetadataResults(std::move(results), std::move(realm), m_file_action_schema);
}

void SyncMetadataManager::set_current_user_identity(const std::string& identity)
{
    auto realm = get_realm();

    realm->begin_transaction();

    TableRef currentUserIdentityTable = ObjectStore::table_for_object_type(realm->read_group(),
                                                                           c_sync_current_user_identity);

    Obj currentUserIdentityObj;
    if (currentUserIdentityTable->is_empty())
        currentUserIdentityObj = currentUserIdentityTable->create_object();
    else
        currentUserIdentityObj = *currentUserIdentityTable->begin();

    currentUserIdentityObj.set<String>(c_sync_current_user_identity, identity);

    realm->commit_transaction();
}

util::Optional<SyncUserMetadata> SyncMetadataManager::get_or_make_user_metadata(const std::string& identity,
                                                                                const std::string& provider_type,
                                                                                bool make_if_absent) const
{
    auto realm = get_realm();
    auto& schema = m_user_schema;

    // Retrieve or create the row for this object.
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_userMetadata);
    Query query = table->where().equal(schema.idx_identity, identity)
                                .equal(schema.idx_provider_type, provider_type);
    Results results(realm, std::move(query));
    REALM_ASSERT_DEBUG(results.size() < 2);
    auto row = results.first();

    if (!row) {
        if (!make_if_absent)
            return none;

        realm->begin_transaction();
        // Check the results again.
        row = results.first();
        if (!row) {
            // Because "making this user" is our last action, set this new user as the current user
            TableRef currentUserIdentityTable = ObjectStore::table_for_object_type(realm->read_group(),
                                                                                   c_sync_current_user_identity);

            Obj currentUserIdentityObj;
            if (currentUserIdentityTable->is_empty())
                currentUserIdentityObj = currentUserIdentityTable->create_object();
            else
                currentUserIdentityObj = *currentUserIdentityTable->begin();

            auto obj = table->create_object();

            currentUserIdentityObj.set<String>(c_sync_current_user_identity, identity);

            std::string uuid = util::uuid_string();
            obj.set(schema.idx_identity, identity);
            obj.set(schema.idx_provider_type, provider_type);
            obj.set(schema.idx_local_uuid, uuid);
            obj.set(schema.idx_marked_for_removal, false);
            obj.set(schema.idx_state, (int64_t)SyncUser::State::LoggedIn);
            realm->commit_transaction();
            return SyncUserMetadata(schema, std::move(realm), std::move(obj));
        } else {
            // Someone beat us to adding this user.
            if (row->get<bool>(schema.idx_marked_for_removal)) {
                // User is dead. Revive or return none.
                if (make_if_absent) {
                    row->set(schema.idx_marked_for_removal, false);
                    realm->commit_transaction();
                } else {
                    realm->cancel_transaction();
                    return none;
                }
            } else {
                // User is alive, nothing else to do.
                realm->cancel_transaction();
            }
            return SyncUserMetadata(schema, std::move(realm), std::move(*row));
        }
    }

    // Got an existing user.
    if (row->get<bool>(schema.idx_marked_for_removal)) {
        // User is dead. Revive or return none.
        if (make_if_absent) {
            realm->begin_transaction();
            row->set(schema.idx_marked_for_removal, false);
            realm->commit_transaction();
        } else {
            return none;
        }
    }

    return SyncUserMetadata(schema, std::move(realm), std::move(*row));
}

void SyncMetadataManager::make_file_action_metadata(StringData original_name,
                                                    StringData partition_key_value,
                                                    StringData local_uuid,
                                                    SyncFileActionMetadata::Action action,
                                                    StringData new_name) const
{
    // This function can't use get_shared_realm() because it's called on a
    // background thread and that's currently not supported by the libuv
    // implementation of EventLoopSignal
    auto coordinator = _impl::RealmCoordinator::get_coordinator(m_metadata_config);
    auto group_ptr = coordinator->begin_read();
    auto& group = *group_ptr;
    REALM_ASSERT(typeid(group) == typeid(Transaction));
    auto& transaction = static_cast<Transaction&>(group);
    transaction.promote_to_write();

    // Retrieve or create the row for this object.
    TableRef table = ObjectStore::table_for_object_type(group, c_sync_fileActionMetadata);

    auto& schema = m_file_action_schema;
    Obj obj = table->create_object_with_primary_key(original_name);

    obj.set(schema.idx_new_name, new_name);
    obj.set(schema.idx_action, static_cast<int64_t>(action));
    obj.set(schema.idx_url, partition_key_value);
    obj.set(schema.idx_user_identity, local_uuid);
    transaction.commit();
}

util::Optional<SyncFileActionMetadata> SyncMetadataManager::get_file_action_metadata(StringData original_name) const
{
    auto realm = get_realm();
    auto& schema = m_file_action_schema;
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_fileActionMetadata);
    auto row_idx = table->find_first_string(schema.idx_original_name, original_name);
    if (!row_idx)
        return none;

    return SyncFileActionMetadata(std::move(schema), std::move(realm), table->get_object(row_idx));
}

std::shared_ptr<Realm> SyncMetadataManager::get_realm() const
{
    auto realm = Realm::get_shared_realm(m_metadata_config);
    realm->refresh();
    return realm;
}

/// Magic key to fetch app metadata, which there should always only be one of.
static const auto app_metadata_pk = 1;

void SyncMetadataManager::set_app_metadata(const std::string& deployment_model,
                                           const std::string& location,
                                           const std::string& hostname,
                                           const std::string& ws_hostname) const
{
    if (m_app_metadata) {
        return;
    }

    auto realm = get_realm();
    auto& schema = m_app_metadata_schema;

    realm->begin_transaction();

    auto table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_app_metadata);
    auto obj = table->create_object_with_primary_key(app_metadata_pk);
    obj.set(schema.idx_deployment_model, deployment_model);
    obj.set(schema.idx_location, location);
    obj.set(schema.idx_hostname, hostname);
    obj.set(schema.idx_ws_hostname, ws_hostname);

    realm->commit_transaction();
}

util::Optional<SyncAppMetadata> SyncMetadataManager::get_app_metadata()
{
    if (!m_app_metadata) {
        auto realm = get_realm();
        auto table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_app_metadata);
        if (!table->size())
            return util::none;

        auto obj = table->get_object_with_primary_key(app_metadata_pk);
        auto& schema = m_app_metadata_schema;
        m_app_metadata = SyncAppMetadata {
            obj.get<String>(schema.idx_deployment_model),
            obj.get<String>(schema.idx_location),
            obj.get<String>(schema.idx_hostname),
            obj.get<String>(schema.idx_ws_hostname)
        };
    }

    return m_app_metadata;
}

// MARK: - Sync user metadata

SyncUserMetadata::SyncUserMetadata(Schema schema, SharedRealm realm, const Obj& obj)
: m_realm(std::move(realm))
, m_schema(std::move(schema))
, m_obj(obj)
{ }

std::string SyncUserMetadata::identity() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.idx_identity);
}

SyncUser::State SyncUserMetadata::state() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return SyncUser::State(m_obj.get<int64_t>(m_schema.idx_state));
}

std::string SyncUserMetadata::local_uuid() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.idx_local_uuid);
}

std::string SyncUserMetadata::refresh_token() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    StringData result = m_obj.get<String>(m_schema.idx_refresh_token);
    return result.is_null() ? "" : std::string(result);
}

std::string SyncUserMetadata::access_token() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    StringData result = m_obj.get<String>(m_schema.idx_access_token);
    return result.is_null() ? "" : std::string(result);
}

std::string SyncUserMetadata::device_id() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    StringData result = m_obj.get<String>(m_schema.idx_device_id);
    return result.is_null() ? "" : std::string(result);
}

inline SyncUserIdentity user_identity_from_obj(const ConstObj& obj)
{
    return SyncUserIdentity(obj.get<String>(c_sync_user_id), obj.get<String>(c_sync_provider_type));
}

std::vector<SyncUserIdentity> SyncUserMetadata::identities() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    auto linklist = m_obj.get_linklist(m_schema.idx_identities);

    std::vector<SyncUserIdentity> identities;
    for (size_t i = 0; i < linklist.size(); i++)
    {
        auto obj_key = linklist.get(i);
        auto obj = linklist.get_target_table()->get_object(obj_key);
        identities.push_back(user_identity_from_obj(obj));
    }

    return identities;
}

std::string SyncUserMetadata::provider_type() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.idx_provider_type);
}

void SyncUserMetadata::set_refresh_token(const std::string& refresh_token)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set<String>(m_schema.idx_refresh_token, refresh_token);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_state(SyncUser::State state)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set<int64_t>(m_schema.idx_state, (int64_t)state);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_identities(std::vector<SyncUserIdentity> identities)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();

    auto link_list = m_obj.get_linklist(m_schema.idx_identities);

    link_list.clear();

    for (size_t i = 0; i < identities.size(); i++)
    {
        auto obj = link_list.get_target_table()->create_object();
        obj.set<String>(c_sync_user_id, identities[i].id);
        obj.set<String>(c_sync_provider_type, identities[i].provider_type);
        link_list.add(obj.get_key());
    }

    m_realm->commit_transaction();
}

void SyncUserMetadata::set_access_token(const std::string& user_token)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set(m_schema.idx_access_token, user_token);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_device_id(const std::string& device_id)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set(m_schema.idx_device_id, device_id);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_user_profile(const SyncUserProfile& profile)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();

    Obj obj;
    if (m_obj.is_null(m_schema.idx_profile)) {
        obj = m_obj.create_and_set_linked_object(m_schema.idx_profile);
    } else {
        obj = m_obj.get_linked_object(m_schema.idx_profile);
    }

    if (profile.name)
        obj.set(c_sync_profile_name, *profile.name);
    if (profile.first_name)
        obj.set(c_sync_profile_first_name, *profile.first_name);
    if (profile.last_name)
        obj.set(c_sync_profile_last_name, *profile.last_name);
    if (profile.gender)
        obj.set(c_sync_profile_gender, *profile.gender);
    if (profile.picture_url)
        obj.set(c_sync_profile_picture_url, *profile.picture_url);
    if (profile.birthday)
        obj.set(c_sync_profile_birthday, *profile.birthday);
    if (profile.min_age)
        obj.set(c_sync_profile_min_age, *profile.min_age);
    if (profile.max_age)
        obj.set(c_sync_profile_max_age, *profile.max_age);
    if (profile.email)
        obj.set(c_sync_profile_email, *profile.email);

    m_realm->commit_transaction();
}

void SyncUserMetadata::mark_for_removal()
{
    if (m_invalid)
        return;

    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set(m_schema.idx_marked_for_removal, true);
    m_realm->commit_transaction();
}

void SyncUserMetadata::remove()
{
    m_invalid = true;
    m_realm->begin_transaction();
    m_obj.remove();
    m_realm->commit_transaction();
    m_realm = nullptr;
}

// MARK: - File action metadata

SyncFileActionMetadata::SyncFileActionMetadata(Schema schema, SharedRealm realm, const Obj& obj)
: m_realm(std::move(realm))
, m_schema(std::move(schema))
, m_obj(obj)
{ }

std::string SyncFileActionMetadata::original_name() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.idx_original_name);
}

util::Optional<std::string> SyncFileActionMetadata::new_name() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    StringData result =m_obj.get<String>(m_schema.idx_new_name);
    return result.is_null() ? util::none : util::make_optional(std::string(result));
}

std::string SyncFileActionMetadata::user_local_uuid() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.idx_user_identity);
}

SyncFileActionMetadata::Action SyncFileActionMetadata::action() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return static_cast<SyncFileActionMetadata::Action>(m_obj.get<Int>(m_schema.idx_action));
}

std::string SyncFileActionMetadata::url() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.idx_url);
}

void SyncFileActionMetadata::remove()
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.remove();
    m_realm->commit_transaction();
    m_realm = nullptr;
}

} // namespace realm
