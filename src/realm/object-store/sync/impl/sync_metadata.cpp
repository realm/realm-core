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

#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>

#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/util/uuid.hpp>
#include <realm/object-store/util/scheduler.hpp>
#if REALM_PLATFORM_APPLE
#include <realm/object-store/impl/apple/keychain_helper.hpp>
#endif

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/table.hpp>

namespace {
static const char* const c_sync_userMetadata = "UserMetadata";
static const char* const c_sync_identityMetadata = "UserIdentity";
static const char* const c_sync_app_metadata = "AppMetadata";

static const char* const c_sync_current_user_identity = "current_user_identity";

/* User keys*/
static const char* const c_sync_marked_for_removal = "marked_for_removal";
static const char* const c_sync_identity = "identity";
static const char* const c_sync_local_uuid = "local_uuid";
static const char* const c_sync_refresh_token = "refresh_token";
static const char* const c_sync_access_token = "access_token";
static const char* const c_sync_identities = "identities";
static const char* const c_sync_state = "state";
static const char* const c_sync_device_id = "device_id";
static const char* const c_sync_profile_data = "profile_data";
static const char* const c_sync_local_realm_paths = "local_realm_paths";

/* Identity keys */
static const char* const c_sync_user_id = "id";
static const char* const c_sync_provider_type = "provider_type";

static const char* const c_sync_fileActionMetadata = "FileActionMetadata";
static const char* const c_sync_original_name = "original_name";
static const char* const c_sync_new_name = "new_name";
static const char* const c_sync_action = "action";
static const char* const c_sync_url = "url";

static const char* const c_sync_clientMetadata = "ClientMetadata";
static const char* const c_sync_uuid = "uuid";

static const char* const c_sync_app_metadata_id = "id";
static const char* const c_sync_app_metadata_deployment_model = "deployment_model";
static const char* const c_sync_app_metadata_location = "location";
static const char* const c_sync_app_metadata_hostname = "hostname";
static const char* const c_sync_app_metadata_ws_hostname = "ws_hostname";

realm::Schema make_schema()
{
    using namespace realm;
    return Schema{{c_sync_identityMetadata,
                   {{c_sync_user_id, PropertyType::String}, {c_sync_provider_type, PropertyType::String}}},
                  {c_sync_userMetadata,
                   {{c_sync_identity, PropertyType::String},
                    {c_sync_local_uuid, PropertyType::String},
                    {c_sync_marked_for_removal, PropertyType::Bool},
                    {c_sync_refresh_token, PropertyType::String | PropertyType::Nullable},
                    {c_sync_provider_type, PropertyType::String},
                    {c_sync_access_token, PropertyType::String | PropertyType::Nullable},
                    {c_sync_identities, PropertyType::Object | PropertyType::Array, c_sync_identityMetadata},
                    {c_sync_state, PropertyType::Int},
                    {c_sync_device_id, PropertyType::String},
                    {c_sync_profile_data, PropertyType::String},
                    {c_sync_local_realm_paths, PropertyType::Set | PropertyType::String}}},
                  {c_sync_fileActionMetadata,
                   {
                       {c_sync_original_name, PropertyType::String, Property::IsPrimary{true}},
                       {c_sync_new_name, PropertyType::String | PropertyType::Nullable},
                       {c_sync_action, PropertyType::Int},
                       {c_sync_url, PropertyType::String},
                       {c_sync_identity, PropertyType::String},
                   }},
                  {c_sync_clientMetadata,
                   {
                       {c_sync_uuid, PropertyType::String},
                   }},
                  {c_sync_current_user_identity, {{c_sync_current_user_identity, PropertyType::String}}},
                  {c_sync_app_metadata,
                   {{c_sync_app_metadata_id, PropertyType::Int, Property::IsPrimary{true}},
                    {c_sync_app_metadata_deployment_model, PropertyType::String},
                    {c_sync_app_metadata_location, PropertyType::String},
                    {c_sync_app_metadata_hostname, PropertyType::String},
                    {c_sync_app_metadata_ws_hostname, PropertyType::String}}}};
}

} // anonymous namespace

namespace realm {

// MARK: - Sync metadata manager

SyncMetadataManager::SyncMetadataManager(std::string path, bool should_encrypt,
                                         util::Optional<std::vector<char>> encryption_key)
{
    constexpr uint64_t SCHEMA_VERSION = 6;

    if (!REALM_PLATFORM_APPLE && should_encrypt && !encryption_key)
        throw std::invalid_argument("Metadata Realm encryption was specified, but no encryption key was provided.");

    m_metadata_config.automatic_change_notifications = false;
    m_metadata_config.path = path;
    m_metadata_config.schema = make_schema();
    m_metadata_config.schema_version = SCHEMA_VERSION;
    m_metadata_config.schema_mode = SchemaMode::Automatic;
    m_metadata_config.scheduler = util::Scheduler::make_dummy();
    if (encryption_key)
        m_metadata_config.encryption_key = std::move(*encryption_key);

    auto realm = open_realm(should_encrypt, encryption_key != none);

    // Get data about the (hardcoded) schemas
    auto object_schema = realm->schema().find(c_sync_userMetadata);
    m_user_schema = {
        object_schema->persisted_properties[0].column_key, object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key, object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key, object_schema->persisted_properties[5].column_key,
        object_schema->persisted_properties[6].column_key, object_schema->persisted_properties[7].column_key,
        object_schema->persisted_properties[8].column_key, object_schema->persisted_properties[9].column_key,
        object_schema->persisted_properties[10].column_key};

    object_schema = realm->schema().find(c_sync_fileActionMetadata);
    m_file_action_schema = {
        object_schema->persisted_properties[0].column_key, object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key, object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key,
    };

    object_schema = realm->schema().find(c_sync_clientMetadata);
    m_client_schema = {
        object_schema->persisted_properties[0].column_key,
    };

    object_schema = realm->schema().find(c_sync_current_user_identity);
    m_current_user_identity_schema = {object_schema->persisted_properties[0].column_key};

    object_schema = realm->schema().find(c_sync_app_metadata);
    m_app_metadata_schema = {
        object_schema->persisted_properties[0].column_key, object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key, object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key};
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
    Query query = table->where().equal(m_user_schema.marked_for_removal_col, marked);

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

    TableRef currentUserIdentityTable =
        ObjectStore::table_for_object_type(realm->read_group(), c_sync_current_user_identity);

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
    Query query = table->where()
                      .equal(schema.identity_col, StringData(identity))
                      .equal(schema.provider_type_col, StringData(provider_type));
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
            TableRef currentUserIdentityTable =
                ObjectStore::table_for_object_type(realm->read_group(), c_sync_current_user_identity);

            Obj currentUserIdentityObj;
            if (currentUserIdentityTable->is_empty())
                currentUserIdentityObj = currentUserIdentityTable->create_object();
            else
                currentUserIdentityObj = *currentUserIdentityTable->begin();

            auto obj = table->create_object();

            currentUserIdentityObj.set<String>(c_sync_current_user_identity, identity);

            std::string uuid = util::uuid_string();
            obj.set(schema.identity_col, identity);
            obj.set(schema.provider_type_col, provider_type);
            obj.set(schema.local_uuid_col, uuid);
            obj.set(schema.marked_for_removal_col, false);
            obj.set(schema.state_col, (int64_t)SyncUser::State::LoggedIn);
            realm->commit_transaction();
            return SyncUserMetadata(schema, std::move(realm), std::move(obj));
        }
        else {
            // Someone beat us to adding this user.
            if (row->get<bool>(schema.marked_for_removal_col)) {
                // User is dead. Revive or return none.
                if (make_if_absent) {
                    row->set(schema.marked_for_removal_col, false);
                    realm->commit_transaction();
                }
                else {
                    realm->cancel_transaction();
                    return none;
                }
            }
            else {
                // User is alive, nothing else to do.
                realm->cancel_transaction();
            }
            return SyncUserMetadata(schema, std::move(realm), std::move(*row));
        }
    }

    // Got an existing user.
    if (row->get<bool>(schema.marked_for_removal_col)) {
        // User is dead. Revive or return none.
        if (make_if_absent) {
            realm->begin_transaction();
            row->set(schema.marked_for_removal_col, false);
            realm->commit_transaction();
        }
        else {
            return none;
        }
    }

    return SyncUserMetadata(schema, std::move(realm), std::move(*row));
}

void SyncMetadataManager::make_file_action_metadata(StringData original_name, StringData partition_key_value,
                                                    StringData local_uuid, SyncFileActionMetadata::Action action,
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

std::shared_ptr<Realm> SyncMetadataManager::try_get_realm() const
{
    try {
        return get_realm();
    }
    catch (const RealmFileException& e) {
        if (e.kind() != RealmFileException::Kind::AccessError)
            throw;
        return nullptr;
    }
}

std::shared_ptr<Realm> SyncMetadataManager::open_realm(bool should_encrypt, bool caller_supplied_key)
{
    if (caller_supplied_key || !should_encrypt || !REALM_PLATFORM_APPLE) {
        if (auto realm = try_get_realm())
            return realm;

        // Encryption key changed, so delete the existing metadata realm and
        // recreate it
        util::File::remove(m_metadata_config.path);
        return get_realm();
    }

#if REALM_PLATFORM_APPLE
    // This logic is all a giant race condition once we have multi-process sync.
    // Wrapping it all (including the keychain accesses) in DB::call_with_lock()
    // might suffice.

    // First try to open the Realm with a key already stored in the keychain.
    // This works for both the case where everything is sensible and valid and
    // when we have a key but no metadata Realm.
    auto key = keychain::get_existing_metadata_realm_key();
    if (key) {
        m_metadata_config.encryption_key = *key;
        if (auto realm = try_get_realm())
            return realm;
    }

    // If we have an existing file and either no key or the key didn't work to
    // decrypt it, then we might have an unencrypted metadata Realm resulting
    // from a previous run being unable to access the keychain.
    if (util::File::exists(m_metadata_config.path)) {
        m_metadata_config.encryption_key.clear();
        if (auto realm = try_get_realm())
            return realm;

        // We weren't able to open the existing file with either the stored key
        // or no key, so just delete it.
        util::File::remove(m_metadata_config.path);
    }

    // We now have no metadata Realm. If we don't have an existing stored key,
    // try to create and store a new one. This might fail, in which case we
    // just create an unencrypted Realm file.
    if (!key)
        key = keychain::create_new_metadata_realm_key();
    if (key)
        m_metadata_config.encryption_key = std::move(*key);
    return get_realm();
#else  // REALM_PLATFORM_APPLE
    REALM_UNREACHABLE();
#endif // REALM_PLATFORM_APPLE
}

/// Magic key to fetch app metadata, which there should always only be one of.
static const auto app_metadata_pk = 1;

void SyncMetadataManager::set_app_metadata(const std::string& deployment_model, const std::string& location,
                                           const std::string& hostname, const std::string& ws_hostname) const
{
    if (m_app_metadata) {
        return;
    }

    auto realm = get_realm();
    auto& schema = m_app_metadata_schema;

    realm->begin_transaction();

    auto table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_app_metadata);
    auto obj = table->create_object_with_primary_key(app_metadata_pk);
    obj.set(schema.deployment_model_col, deployment_model);
    obj.set(schema.location_col, location);
    obj.set(schema.hostname_col, hostname);
    obj.set(schema.ws_hostname_col, ws_hostname);

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
        m_app_metadata =
            SyncAppMetadata{obj.get<String>(schema.deployment_model_col), obj.get<String>(schema.location_col),
                            obj.get<String>(schema.hostname_col), obj.get<String>(schema.ws_hostname_col)};
    }

    return m_app_metadata;
}

// MARK: - Sync user metadata

SyncUserMetadata::SyncUserMetadata(Schema schema, SharedRealm realm, const Obj& obj)
    : m_realm(std::move(realm))
    , m_schema(std::move(schema))
    , m_obj(obj)
{
}

std::string SyncUserMetadata::identity() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.identity_col);
}

SyncUser::State SyncUserMetadata::state() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return SyncUser::State(m_obj.get<int64_t>(m_schema.state_col));
}

std::string SyncUserMetadata::local_uuid() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    return m_obj.get<String>(m_schema.local_uuid_col);
}

std::string SyncUserMetadata::refresh_token() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    StringData result = m_obj.get<String>(m_schema.refresh_token_col);
    return result.is_null() ? "" : std::string(result);
}

std::string SyncUserMetadata::access_token() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    StringData result = m_obj.get<String>(m_schema.access_token_col);
    return result.is_null() ? "" : std::string(result);
}

std::string SyncUserMetadata::device_id() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    StringData result = m_obj.get<String>(m_schema.device_id_col);
    return result.is_null() ? "" : std::string(result);
}

inline SyncUserIdentity user_identity_from_obj(const Obj& obj)
{
    return SyncUserIdentity(obj.get<String>(c_sync_user_id), obj.get<String>(c_sync_provider_type));
}

std::vector<SyncUserIdentity> SyncUserMetadata::identities() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    auto linklist = m_obj.get_linklist(m_schema.identities_col);

    std::vector<SyncUserIdentity> identities;
    for (size_t i = 0; i < linklist.size(); i++) {
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
    return m_obj.get<String>(m_schema.provider_type_col);
}

void SyncUserMetadata::set_refresh_token(const std::string& refresh_token)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set<String>(m_schema.refresh_token_col, refresh_token);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_state(SyncUser::State state)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set<int64_t>(m_schema.state_col, (int64_t)state);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_state_and_tokens(SyncUser::State state, const std::string& access_token,
                                            const std::string& refresh_token)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set(m_schema.state_col, static_cast<int64_t>(state));
    m_obj.set(m_schema.access_token_col, access_token);
    m_obj.set(m_schema.refresh_token_col, refresh_token);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_identities(std::vector<SyncUserIdentity> identities)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();

    auto link_list = m_obj.get_linklist(m_schema.identities_col);
    auto identities_table = link_list.get_target_table();
    auto col_user_id = identities_table->get_column_key(c_sync_user_id);
    auto col_provider_type = identities_table->get_column_key(c_sync_provider_type);
    link_list.clear();

    for (auto& ident : identities) {
        ObjKey obj_key = identities_table->where()
                             .equal(col_user_id, StringData(ident.id))
                             .equal(col_provider_type, StringData(ident.provider_type))
                             .find();
        if (!obj_key) {
            auto obj = link_list.get_target_table()->create_object();
            obj.set<String>(c_sync_user_id, ident.id);
            obj.set<String>(c_sync_provider_type, ident.provider_type);
            obj_key = obj.get_key();
        }
        link_list.add(obj_key);
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
    m_obj.set(m_schema.access_token_col, user_token);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_device_id(const std::string& device_id)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set(m_schema.device_id_col, device_id);
    m_realm->commit_transaction();
}

SyncUserProfile SyncUserMetadata::profile() const
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    StringData result = m_obj.get<String>(m_schema.profile_dump_col);
    if (result.size() == 0) {
        return SyncUserProfile();
    }
    return SyncUserProfile(static_cast<bson::BsonDocument>(bson::parse(std::string(result))));
}

void SyncUserMetadata::set_user_profile(const SyncUserProfile& profile)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    std::stringstream data;
    data << profile.data();
    m_obj.set(m_schema.profile_dump_col, data.str());
    m_realm->commit_transaction();
}

std::vector<std::string> SyncUserMetadata::realm_file_paths() const
{
    if (m_invalid)
        return {};

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->refresh();
    Set<StringData> paths = m_obj.get_set<StringData>(m_schema.realm_file_paths_col);
    return std::vector<std::string>(paths.begin(), paths.end());
}

void SyncUserMetadata::add_realm_file_path(const std::string& path)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    Set<StringData> paths = m_obj.get_set<StringData>(m_schema.realm_file_paths_col);
    paths.insert(path);
    m_realm->commit_transaction();
}

void SyncUserMetadata::mark_for_removal()
{
    if (m_invalid)
        return;

    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set(m_schema.marked_for_removal_col, true);
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
{
}

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
    StringData result = m_obj.get<String>(m_schema.idx_new_name);
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

void SyncFileActionMetadata::set_action(Action new_action)
{
    REALM_ASSERT(m_realm);
    m_realm->verify_thread();
    m_realm->begin_transaction();
    m_obj.set<Int>(m_schema.idx_action, static_cast<Int>(new_action));
    m_realm->commit_transaction();
}

} // namespace realm
