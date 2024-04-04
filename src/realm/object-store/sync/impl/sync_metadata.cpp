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
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/util/uuid.hpp>
#include <realm/object-store/util/scheduler.hpp>
#if REALM_PLATFORM_APPLE
#include <realm/object-store/impl/apple/keychain_helper.hpp>
#endif

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/table.hpp>

using namespace realm;

namespace {
static const char* const c_sync_userMetadata = "UserMetadata";
static const char* const c_sync_identityMetadata = "UserIdentity";

static const char* const c_sync_current_user_identity = "current_user_identity";

/* User keys */
static const char* const c_sync_identity = "identity";
static const char* const c_sync_legacy_uuids = "legacy_uuids";
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
static const char* const c_sync_partition = "url";

realm::Schema make_schema()
{
    using namespace realm;
    return Schema{
        {c_sync_identityMetadata,
         ObjectSchema::ObjectType::Embedded,
         {
             {c_sync_user_id, PropertyType::String},
             {c_sync_provider_type, PropertyType::String},
         }},
        {c_sync_userMetadata,
         {{c_sync_identity, PropertyType::String},
          {c_sync_legacy_uuids, PropertyType::String | PropertyType::Array},
          {c_sync_refresh_token, PropertyType::String | PropertyType::Nullable},
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
             {c_sync_partition, PropertyType::String}, // unused and should be removed in v8
             {c_sync_identity, PropertyType::String},  // unused and should be removed in v8
         }},
        {c_sync_current_user_identity,
         {
             {c_sync_current_user_identity, PropertyType::String},
         }},
    };
}

void migrate_to_v7(Realm& old_realm, Realm& realm)
{
    // Before schema version 7 there may have been multiple UserMetadata entries
    // for a single user_id with different provider types, so we need to merge
    // any duplicates together

    TableRef table = ObjectStore::table_for_object_type(realm.read_group(), c_sync_userMetadata);
    TableRef old_table = ObjectStore::table_for_object_type(old_realm.read_group(), c_sync_userMetadata);
    if (table->is_empty())
        return;
    REALM_ASSERT(table->size() == old_table->size());

    ColKey id_col = table->get_column_key(c_sync_identity);
    ColKey old_uuid_col = old_table->get_column_key("local_uuid");
    ColKey new_uuid_col = table->get_column_key(c_sync_legacy_uuids);
    ColKey state_col = table->get_column_key(c_sync_state);

    std::unordered_map<std::string, Obj> users;
    for (size_t i = 0, j = 0; i < table->size(); ++j) {
        auto obj = table->get_object(i);

        // Move the local uuid from the old column to the list
        auto old_obj = old_table->get_object(j);
        obj.get_list<String>(new_uuid_col).add(old_obj.get<String>(old_uuid_col));

        // Check if we've already seen an object with the same id. If not, store
        // this one and move on
        std::string user_id = obj.get<String>(id_col);
        auto& existing = users[obj.get<String>(id_col)];
        if (!existing.is_valid()) {
            existing = obj;
            ++i;
            continue;
        }

        // We have a second object for the same id, so we need to merge them.
        // First we merge the state: if one is logged in and the other isn't,
        // we'll use the logged-in state and tokens. If both are logged in, we'll
        // use the more recent login. If one is logged out and the other is
        // removed we'll use the logged out state. If both are logged out or
        // both are removed then it doesn't matter which we pick.
        using State = SyncUser::State;
        auto state = State(obj.get<int64_t>(state_col));
        auto existing_state = State(existing.get<int64_t>(state_col));
        if (state == existing_state) {
            if (state == State::LoggedIn) {
                RealmJWT token_1(existing.get<StringData>(c_sync_access_token));
                RealmJWT token_2(obj.get<StringData>(c_sync_access_token));
                if (token_1.issued_at < token_2.issued_at) {
                    existing.set(c_sync_refresh_token, obj.get<StringData>(c_sync_refresh_token));
                    existing.set(c_sync_access_token, obj.get<StringData>(c_sync_access_token));
                }
            }
        }
        else if (state == State::LoggedIn || existing_state == State::Removed) {
            existing.set(c_sync_state, int64_t(state));
            existing.set(c_sync_refresh_token, obj.get<StringData>(c_sync_refresh_token));
            existing.set(c_sync_access_token, obj.get<StringData>(c_sync_access_token));
        }

        // Next we merge the list properties (identities, legacy uuids, realm file paths)
        {
            auto dest = existing.get_linklist(c_sync_identities);
            auto src = obj.get_linklist(c_sync_identities);
            for (size_t i = 0, size = src.size(); i < size; ++i) {
                if (dest.find_first(src.get(i)) == npos) {
                    dest.add(src.get(i));
                }
            }
        }
        {
            auto dest = existing.get_list<String>(c_sync_legacy_uuids);
            auto src = obj.get_list<String>(c_sync_legacy_uuids);
            for (size_t i = 0, size = src.size(); i < size; ++i) {
                if (dest.find_first(src.get(i)) == npos) {
                    dest.add(src.get(i));
                }
            }
        }
        {
            auto dest = existing.get_set<String>(c_sync_local_realm_paths);
            auto src = obj.get_set<String>(c_sync_local_realm_paths);
            for (size_t i = 0, size = src.size(); i < size; ++i) {
                dest.insert(src.get(i));
            }
        }


        // Finally we delete the duplicate object. We don't increment `i` as it's
        // now the index of the object just after the one we're deleting.
        obj.remove();
    }
}

void migrate_to_v8(Realm&, Realm& realm)
{
    if (auto app_metadata_table = realm.read_group().get_table("class_AppMetadata")) {
        realm.read_group().remove_table(app_metadata_table->get_key());
    }
}

} // anonymous namespace

// MARK: - Sync metadata manager

SyncMetadataManager::SyncMetadataManager(std::string path, bool should_encrypt,
                                         util::Optional<std::vector<char>> encryption_key)
{
    constexpr uint64_t SCHEMA_VERSION = 7;

    if (!REALM_PLATFORM_APPLE && should_encrypt && !encryption_key)
        throw InvalidArgument("Metadata Realm encryption was specified, but no encryption key was provided.");

    m_metadata_config.automatic_change_notifications = false;
    m_metadata_config.path = path;
    m_metadata_config.schema = make_schema();
    m_metadata_config.schema_version = SCHEMA_VERSION;
    m_metadata_config.schema_mode = SchemaMode::Automatic;
    m_metadata_config.scheduler = util::Scheduler::make_dummy();
    if (encryption_key)
        m_metadata_config.encryption_key = std::move(*encryption_key);
    m_metadata_config.automatically_handle_backlinks_in_migrations = true;
    m_metadata_config.migration_function = [](std::shared_ptr<Realm> old_realm, std::shared_ptr<Realm> realm,
                                              Schema&) {
        if (old_realm->schema_version() < 7) {
            migrate_to_v7(*old_realm, *realm);
        }
        // note that the schema version has not yet been bumped to 8
        if (old_realm->schema_version() < 8) {
            migrate_to_v8(*old_realm, *realm);
        }
    };

    auto realm = open_realm(should_encrypt, encryption_key != none);

    // Get data about the (hardcoded) schemas
    auto object_schema = realm->schema().find(c_sync_userMetadata);
    m_user_schema = {
        object_schema->persisted_properties[0].column_key, object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key, object_schema->persisted_properties[3].column_key,
        object_schema->persisted_properties[4].column_key, object_schema->persisted_properties[5].column_key,
        object_schema->persisted_properties[6].column_key, object_schema->persisted_properties[7].column_key,
        object_schema->persisted_properties[8].column_key};

    object_schema = realm->schema().find(c_sync_fileActionMetadata);
    m_file_action_schema = {
        object_schema->persisted_properties[0].column_key,
        object_schema->persisted_properties[1].column_key,
        object_schema->persisted_properties[2].column_key,
    };
}

void SyncMetadataManager::perform_launch_actions(SyncFileManager& file_manager) const
{
    auto realm = get_realm();

    // Perform our "on next startup" actions such as deleting Realm files
    // which we couldn't delete immediately due to them being in use
    auto actions_table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_fileActionMetadata);
    for (auto file_action : *actions_table) {
        SyncFileActionMetadata md(m_file_action_schema, realm, file_action);
        run_file_action(file_manager, md);
    }

    // Delete any users marked for death.
    auto users_table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_userMetadata);
    for (auto user : *users_table) {
        if (user.get<int64_t>(m_user_schema.state_col) != int64_t(SyncUser::State::Removed))
            continue;
        try {
            SyncUserMetadata data(m_user_schema, realm, user);
            file_manager.remove_user_realms(data.identity(), data.realm_file_paths());
            realm->begin_transaction();
            user.remove();
            realm->commit_transaction();
        }
        catch (FileAccessError const&) {
            continue;
        }
    }
}

bool SyncMetadataManager::run_file_action(SyncFileManager& file_manager, SyncFileActionMetadata& md) const
{
    switch (md.action()) {
        case SyncFileActionMetadata::Action::DeleteRealm:
            // Delete all the files for the given Realm.
            if (file_manager.remove_realm(md.original_name())) {
                md.remove();
                return true;
            }
            break;
        case SyncFileActionMetadata::Action::BackUpThenDeleteRealm:
            // Copy the primary Realm file to the recovery dir, and then delete the Realm.
            auto new_name = md.new_name();
            auto original_name = md.original_name();
            if (!util::File::exists(original_name)) {
                // The Realm file doesn't exist anymore.
                md.remove();
                return false;
            }
            if (new_name && !util::File::exists(*new_name) &&
                file_manager.copy_realm_file(original_name, *new_name)) {
                // We successfully copied the Realm file to the recovery directory.
                bool did_remove = file_manager.remove_realm(original_name);
                // if the copy succeeded but not the delete, then running BackupThenDelete
                // a second time would fail, so change this action to just delete the original file.
                if (did_remove) {
                    md.remove();
                    return true;
                }
                md.set_action(SyncFileActionMetadata::Action::DeleteRealm);
            }
            break;
    }
    return false;
}

// Some of our string columns are nullable. They never should actually be
// null as we store "" rather than null when the value isn't present, but
// be safe and handle it anyway.
static std::string_view get_string(const Obj& obj, ColKey col)
{
    auto str = obj.get<String>(col);
    return str.is_null() ? "" : std::string_view(str);
}

static bool is_valid_user(const SyncUserMetadata::Schema& schema, const Obj& obj)
{
    // This is overly cautious and merely checking the state should suffice,
    // but because this is a persisted file that can be modified it's possible
    // to get invalid combinations of data.
    return obj && obj.get<int64_t>(schema.state_col) == int64_t(SyncUser::State::LoggedIn) &&
           RealmJWT::validate(get_string(obj, schema.access_token_col)) &&
           RealmJWT::validate(get_string(obj, schema.refresh_token_col));
}

std::vector<SyncUserMetadata> SyncMetadataManager::all_logged_in_users() const
{
    auto realm = get_realm();
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_userMetadata);
    std::vector<SyncUserMetadata> users;
    users.reserve(table->size());
    for (auto obj : *table) {
        if (is_valid_user(m_user_schema, obj)) {
            users.emplace_back(m_user_schema, realm, obj);
        }
    }
    return users;
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
    Query query;
    if (marked) {
        query = table->where().equal(m_user_schema.state_col, int64_t(SyncUser::State::Removed));
    }
    else {
        query = table->where().not_equal(m_user_schema.state_col, int64_t(SyncUser::State::Removed));
    }
    return SyncUserMetadataResults(Results(realm, std::move(query)), m_user_schema);
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
    return SyncFileActionMetadataResults(Results(realm, table), m_file_action_schema);
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
                                                                                bool make_if_absent) const
{
    auto realm = get_realm();
    auto& schema = m_user_schema;

    // Retrieve or create the row for this object.
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_userMetadata);
    Query query = table->where().equal(schema.identity_col, StringData(identity));
    Results results(realm, std::move(query));
    REALM_ASSERT_DEBUG(results.size() < 2);
    auto obj = results.first();

    if (!obj) {
        if (!make_if_absent)
            return none;

        realm->begin_transaction();
        // Check the results again.
        obj = results.first();
    }
    if (!obj) {
        // Because "making this user" is our last action, set this new user as the current user
        TableRef currentUserIdentityTable =
            ObjectStore::table_for_object_type(realm->read_group(), c_sync_current_user_identity);

        Obj currentUserIdentityObj;
        if (currentUserIdentityTable->is_empty())
            currentUserIdentityObj = currentUserIdentityTable->create_object();
        else
            currentUserIdentityObj = *currentUserIdentityTable->begin();

        obj = table->create_object();

        currentUserIdentityObj.set<String>(c_sync_current_user_identity, identity);

        obj->set(schema.identity_col, identity);
        obj->set(schema.state_col, (int64_t)SyncUser::State::LoggedIn);
        realm->commit_transaction();
        return SyncUserMetadata(schema, std::move(realm), *obj);
    }

    // Got an existing user.
    if (obj->get<int64_t>(schema.state_col) == int64_t(SyncUser::State::Removed)) {
        // User is dead. Revive or return none.
        if (!make_if_absent) {
            return none;
        }

        if (!realm->is_in_transaction())
            realm->begin_transaction();
        obj->set(schema.state_col, (int64_t)SyncUser::State::LoggedIn);
        realm->commit_transaction();
    }

    return SyncUserMetadata(schema, std::move(realm), std::move(*obj));
}

void SyncMetadataManager::make_file_action_metadata(StringData original_name, SyncFileActionMetadata::Action action,
                                                    StringData new_name) const
{
    auto realm = get_realm();
    realm->begin_transaction();
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), c_sync_fileActionMetadata);

    auto& schema = m_file_action_schema;
    Obj obj = table->create_object_with_primary_key(original_name);

    obj.set(schema.idx_new_name, new_name);
    obj.set(schema.idx_action, static_cast<int64_t>(action));
    realm->commit_transaction();
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

bool SyncMetadataManager::perform_file_actions(SyncFileManager& file_manager, StringData path) const
{
    if (auto md = get_file_action_metadata(path)) {
        return run_file_action(file_manager, *md);
    }
    return false;
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
    catch (const InvalidDatabase&) {
        return nullptr;
    }
}

std::shared_ptr<Realm> SyncMetadataManager::open_realm(bool should_encrypt, bool caller_supplied_key)
{
    if (caller_supplied_key || !should_encrypt || !REALM_PLATFORM_APPLE) {
        m_metadata_config.clear_on_invalid_file = true;
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
        // or no key, so just recreate it
        m_metadata_config.clear_on_invalid_file = true;
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
    m_realm->refresh();
    return m_obj.get<String>(m_schema.identity_col);
}

SyncUser::State SyncUserMetadata::state() const
{
    REALM_ASSERT(m_realm);
    m_realm->refresh();
    return SyncUser::State(m_obj.get<int64_t>(m_schema.state_col));
}

std::vector<std::string> SyncUserMetadata::legacy_identities() const
{
    REALM_ASSERT(m_realm);
    m_realm->refresh();
    std::vector<std::string> uuids;
    auto list = m_obj.get_list<String>(m_schema.legacy_uuids_col);
    for (size_t i = 0, size = list.size(); i < size; ++i) {
        uuids.push_back(list.get(i));
    }
    return uuids;
}

std::string SyncUserMetadata::refresh_token() const
{
    REALM_ASSERT(m_realm);
    m_realm->refresh();
    StringData result = m_obj.get<String>(m_schema.refresh_token_col);
    return result.is_null() ? "" : std::string(result);
}

std::string SyncUserMetadata::access_token() const
{
    REALM_ASSERT(m_realm);
    StringData result = m_obj.get<String>(m_schema.access_token_col);
    return result.is_null() ? "" : std::string(result);
}

std::string SyncUserMetadata::device_id() const
{
    REALM_ASSERT(m_realm);
    StringData result = m_obj.get<String>(m_schema.device_id_col);
    return result.is_null() ? "" : std::string(result);
}

std::vector<SyncUserIdentity> SyncUserMetadata::identities() const
{
    REALM_ASSERT(m_realm);
    m_realm->refresh();
    auto linklist = m_obj.get_linklist(m_schema.identities_col);

    std::vector<SyncUserIdentity> identities;
    for (size_t i = 0; i < linklist.size(); i++) {
        auto obj = linklist.get_object(i);
        identities.emplace_back(obj.get<String>(c_sync_user_id), obj.get<String>(c_sync_provider_type));
    }

    return identities;
}

SyncUserProfile SyncUserMetadata::profile() const
{
    REALM_ASSERT(m_realm);
    m_realm->refresh();
    StringData result = m_obj.get<String>(m_schema.profile_dump_col);
    if (result.size() == 0) {
        return SyncUserProfile();
    }
    return SyncUserProfile(static_cast<bson::BsonDocument>(bson::parse(std::string_view(result))));
}

void SyncUserMetadata::set_refresh_token(const std::string& refresh_token)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->begin_transaction();
    m_obj.set<String>(m_schema.refresh_token_col, refresh_token);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_state(SyncUser::State state)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
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
    m_realm->begin_transaction();

    auto link_list = m_obj.get_linklist(m_schema.identities_col);
    auto identities_table = link_list.get_target_table();
    auto col_user_id = identities_table->get_column_key(c_sync_user_id);
    auto col_provider_type = identities_table->get_column_key(c_sync_provider_type);
    link_list.clear();

    for (auto& ident : identities) {
        auto obj = link_list.create_and_insert_linked_object(link_list.size());
        obj.set<String>(col_user_id, ident.id);
        obj.set<String>(col_provider_type, ident.provider_type);
    }

    m_realm->commit_transaction();
}

void SyncUserMetadata::set_access_token(const std::string& user_token)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->begin_transaction();
    m_obj.set(m_schema.access_token_col, user_token);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_device_id(const std::string& device_id)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->begin_transaction();
    m_obj.set(m_schema.device_id_col, device_id);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_legacy_identities(const std::vector<std::string>& uuids)
{
    m_realm->begin_transaction();
    auto list = m_obj.get_list<String>(m_schema.legacy_uuids_col);
    list.clear();
    for (auto& uuid : uuids)
        list.add(uuid);
    m_realm->commit_transaction();
}

void SyncUserMetadata::set_user_profile(const SyncUserProfile& profile)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
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
    m_realm->refresh();
    Set<StringData> paths = m_obj.get_set<StringData>(m_schema.realm_file_paths_col);
    return std::vector<std::string>(paths.begin(), paths.end());
}

void SyncUserMetadata::add_realm_file_path(const std::string& path)
{
    if (m_invalid)
        return;

    REALM_ASSERT_DEBUG(m_realm);
    m_realm->begin_transaction();
    Set<StringData> paths = m_obj.get_set<StringData>(m_schema.realm_file_paths_col);
    paths.insert(path);
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
    m_realm->refresh();
    return m_obj.get<String>(m_schema.idx_original_name);
}

util::Optional<std::string> SyncFileActionMetadata::new_name() const
{
    REALM_ASSERT(m_realm);
    m_realm->refresh();
    StringData result = m_obj.get<String>(m_schema.idx_new_name);
    return result.is_null() ? util::none : util::make_optional(std::string(result));
}

SyncFileActionMetadata::Action SyncFileActionMetadata::action() const
{
    REALM_ASSERT(m_realm);
    m_realm->refresh();
    return static_cast<SyncFileActionMetadata::Action>(m_obj.get<Int>(m_schema.idx_action));
}

void SyncFileActionMetadata::remove()
{
    REALM_ASSERT(m_realm);
    m_realm->begin_transaction();
    m_obj.remove();
    m_realm->commit_transaction();
    m_realm = nullptr;
}

void SyncFileActionMetadata::set_action(Action new_action)
{
    REALM_ASSERT(m_realm);
    m_realm->begin_transaction();
    m_obj.set<Int>(m_schema.idx_action, static_cast<Int>(new_action));
    m_realm->commit_transaction();
}
