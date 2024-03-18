////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
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

#include <realm/object-store/sync/impl/app_metadata.hpp>

#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/util/scheduler.hpp>
#if REALM_PLATFORM_APPLE
#include <realm/object-store/impl/apple/keychain_helper.hpp>
#endif

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/table.hpp>

using namespace realm;
using realm::app::UserData;

namespace {

struct CurrentUserSchema {
    TableKey table_key;
    ColKey user_id;

    static constexpr const char* table_name = "current_user_identity";

    void read(Realm& realm)
    {
        auto object_schema = realm.schema().find(table_name);
        table_key = object_schema->table_key;
        user_id = object_schema->persisted_properties[0].column_key;
    }

    static ObjectSchema object_schema()
    {
        return {table_name, {{table_name, PropertyType::String}}};
    }
};

struct UserIdentitySchema {
    TableKey table_key;
    ColKey user_id;
    ColKey provider_id;

    static constexpr const char* table_name = "UserIdentity";

    void read(Realm& realm)
    {
        auto object_schema = realm.schema().find(table_name);
        table_key = object_schema->table_key;
        user_id = object_schema->persisted_properties[0].column_key;
        provider_id = object_schema->persisted_properties[1].column_key;
    }

    static ObjectSchema object_schema()
    {
        return {table_name,
                ObjectSchema::ObjectType::Embedded,
                {
                    {"id", PropertyType::String},
                    {"provider_type", PropertyType::String},
                }};
    }
};

struct SyncUserSchema {
    TableKey table_key;

    // The server-supplied user_id for the user. Unique per server instance.
    ColKey user_id_col;
    // Locally generated UUIDs for the user. These are tracked to be able
    // to open pre-existing Realm files, but are no longer generated or
    // used for anything else.
    ColKey legacy_uuids_col;
    // The cached refresh token for this user.
    ColKey refresh_token_col;
    // The cached access token for this user.
    ColKey access_token_col;
    // The identities for this user.
    ColKey identities_col;
    // The current state of this user.
    ColKey state_col;
    // The device id of this user.
    ColKey device_id_col;
    // Any additional profile attributes, formatted as a bson string.
    ColKey profile_dump_col;
    // The set of absolute file paths to Realms belonging to this user.
    ColKey realm_file_paths_col;

    static constexpr const char* table_name = "UserMetadata";

    void read(Realm& realm)
    {
        auto object_schema = realm.schema().find(table_name);
        table_key = object_schema->table_key;
        user_id_col = object_schema->persisted_properties[0].column_key;
        legacy_uuids_col = object_schema->persisted_properties[1].column_key;
        refresh_token_col = object_schema->persisted_properties[2].column_key;
        access_token_col = object_schema->persisted_properties[3].column_key;
        identities_col = object_schema->persisted_properties[4].column_key;
        state_col = object_schema->persisted_properties[5].column_key;
        device_id_col = object_schema->persisted_properties[6].column_key;
        profile_dump_col = object_schema->persisted_properties[7].column_key;
        realm_file_paths_col = object_schema->persisted_properties[8].column_key;
    }

    static ObjectSchema object_schema()
    {
        return {table_name,
                {{"identity", PropertyType::String},
                 {"legacy_uuids", PropertyType::String | PropertyType::Array},
                 {"refresh_token", PropertyType::String | PropertyType::Nullable},
                 {"access_token", PropertyType::String | PropertyType::Nullable},
                 {"identities", PropertyType::Object | PropertyType::Array, UserIdentitySchema::table_name},
                 {"state", PropertyType::Int},
                 {"device_id", PropertyType::String},
                 {"profile_data", PropertyType::String},
                 {"local_realm_paths", PropertyType::Set | PropertyType::String}}};
    }
};

struct FileActionSchema {
    TableKey table_key;

    // The original path on disk of the file (generally, the main file for an on-disk Realm).
    ColKey idx_original_name;
    // A new path on disk for a file to be written to. Context-dependent.
    ColKey idx_new_name;
    // An enum describing the action to take.
    ColKey idx_action;
    // The partition key of the Realm.
    ColKey idx_partition;
    // The user_id of the user to whom the file action applies (despite the internal column name).
    ColKey idx_user_identity;

    static constexpr const char* table_name = "FileActionMetadata";

    void read(Realm& realm)
    {
        auto object_schema = realm.schema().find(table_name);
        table_key = object_schema->table_key;
        idx_original_name = object_schema->persisted_properties[0].column_key;
        idx_new_name = object_schema->persisted_properties[1].column_key;
        idx_action = object_schema->persisted_properties[2].column_key;
        idx_partition = object_schema->persisted_properties[3].column_key;
        idx_user_identity = object_schema->persisted_properties[4].column_key;
    }

    static ObjectSchema object_schema()
    {
        return {table_name,
                {
                    {"original_name", PropertyType::String, Property::IsPrimary{true}},
                    {"new_name", PropertyType::String | PropertyType::Nullable},
                    {"action", PropertyType::Int},
                    {"url", PropertyType::String},      // actually partition key
                    {"identity", PropertyType::String}, // actually user id
                }};
    }
};

void migrate_to_v7(std::shared_ptr<Realm> old_realm, std::shared_ptr<Realm> realm)
{
    // Before schema version 7 there may have been multiple UserMetadata entries
    // for a single user_id with different provider types, so we need to merge
    // any duplicates together

    SyncUserSchema schema;
    schema.read(*realm);

    TableRef table = realm->read_group().get_table(schema.table_key);
    TableRef old_table = ObjectStore::table_for_object_type(old_realm->read_group(), SyncUserSchema::table_name);
    if (table->is_empty())
        return;
    REALM_ASSERT(table->size() == old_table->size());

    ColKey old_uuid_col = old_table->get_column_key("local_uuid");

    std::unordered_map<std::string, Obj> users;
    for (size_t i = 0, j = 0; i < table->size(); ++j) {
        auto obj = table->get_object(i);

        // Move the local uuid from the old column to the list
        auto old_obj = old_table->get_object(j);
        obj.get_list<String>(schema.legacy_uuids_col).add(old_obj.get<String>(old_uuid_col));

        // Check if we've already seen an object with the same id. If not, store
        // this one and move on
        std::string user_id = obj.get<String>(schema.user_id_col);
        auto& existing = users[obj.get<String>(schema.user_id_col)];
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
        auto state = State(obj.get<int64_t>(schema.state_col));
        auto existing_state = State(existing.get<int64_t>(schema.state_col));
        if (state == existing_state) {
            if (state == State::LoggedIn) {
                RealmJWT token_1(existing.get<StringData>(schema.access_token_col));
                RealmJWT token_2(obj.get<StringData>(schema.access_token_col));
                if (token_1.issued_at < token_2.issued_at) {
                    existing.set(schema.refresh_token_col, obj.get<StringData>(schema.refresh_token_col));
                    existing.set(schema.access_token_col, obj.get<StringData>(schema.access_token_col));
                }
            }
        }
        else if (state == State::LoggedIn || existing_state == State::Removed) {
            existing.set(schema.state_col, int64_t(state));
            existing.set(schema.refresh_token_col, obj.get<StringData>(schema.refresh_token_col));
            existing.set(schema.access_token_col, obj.get<StringData>(schema.access_token_col));
        }

        // Next we merge the list properties (identities, legacy uuids, realm file paths)
        {
            auto dest = existing.get_linklist(schema.identities_col);
            auto src = obj.get_linklist(schema.identities_col);
            for (size_t i = 0, size = src.size(); i < size; ++i) {
                if (dest.find_first(src.get(i)) == npos) {
                    dest.add(src.get(i));
                }
            }
        }
        {
            auto dest = existing.get_list<String>(schema.legacy_uuids_col);
            auto src = obj.get_list<String>(schema.legacy_uuids_col);
            for (size_t i = 0, size = src.size(); i < size; ++i) {
                if (dest.find_first(src.get(i)) == npos) {
                    dest.add(src.get(i));
                }
            }
        }
        {
            auto dest = existing.get_set<String>(schema.realm_file_paths_col);
            auto src = obj.get_set<String>(schema.realm_file_paths_col);
            for (size_t i = 0, size = src.size(); i < size; ++i) {
                dest.insert(src.get(i));
            }
        }

        // Finally we delete the duplicate object. We don't increment `i` as it's
        // now the index of the object just after the one we're deleting.
        obj.remove();
    }
}

std::shared_ptr<Realm> open_realm(RealmConfig& config, const app::AppConfig& app_config)
{
    bool should_encrypt = app_config.metadata_mode == app::AppConfig::MetadataMode::Encryption;
    if (!REALM_PLATFORM_APPLE && should_encrypt && !app_config.custom_encryption_key)
        throw InvalidArgument("Metadata Realm encryption was specified, but no encryption key was provided.");

    if (app_config.custom_encryption_key && should_encrypt)
        config.encryption_key = *app_config.custom_encryption_key;
    if (app_config.custom_encryption_key || !should_encrypt || !REALM_PLATFORM_APPLE) {
        config.clear_on_invalid_file = true;
        return Realm::get_shared_realm(config);
    }

#if REALM_PLATFORM_APPLE
    auto try_get_realm = [&]() -> std::shared_ptr<Realm> {
        try {
            return Realm::get_shared_realm(config);
        }
        catch (const InvalidDatabase&) {
            return nullptr;
        }
    };

    // This logic is all a giant race condition once we have multi-process sync.
    // Wrapping it all (including the keychain accesses) in DB::call_with_lock()
    // might suffice.

    // First try to open the Realm with a key already stored in the keychain.
    // This works for both the case where everything is sensible and valid and
    // when we have a key but no metadata Realm.
    auto key = keychain::get_existing_metadata_realm_key(app_config.app_id, app_config.security_access_group);
    if (key) {
        config.encryption_key = *key;
        if (auto realm = try_get_realm())
            return realm;
    }

    // If we have an existing file and either no key or the key didn't work to
    // decrypt it, then we might have an unencrypted metadata Realm resulting
    // from a previous run being unable to access the keychain.
    if (util::File::exists(config.path)) {
        config.encryption_key.clear();
        if (auto realm = try_get_realm())
            return realm;

        // We weren't able to open the existing file with either the stored key
        // or no key, so just recreate it
        config.clear_on_invalid_file = true;
    }

    // We now have no metadata Realm. If we don't have an existing stored key,
    // try to create and store a new one. This might fail, in which case we
    // just create an unencrypted Realm file.
    if (!key)
        key = keychain::create_new_metadata_realm_key(app_config.app_id, app_config.security_access_group);
    if (key)
        config.encryption_key = std::move(*key);
    return try_get_realm();
#else  // REALM_PLATFORM_APPLE
    REALM_UNREACHABLE();
#endif // REALM_PLATFORM_APPLE
}

struct PersistedSyncMetadataManager : public app::MetadataStore {
    RealmConfig m_config;
    SyncUserSchema m_user_schema;
    FileActionSchema m_file_action_schema;
    UserIdentitySchema m_user_identity_schema;
    CurrentUserSchema m_current_user_schema;

    PersistedSyncMetadataManager(std::string path, const app::AppConfig& app_config, SyncFileManager& file_manager)
    {
        // Note that there are several deferred schema changes which don't
        // justify bumping the schema version by themself, but should be done
        // the next time something does justify a migration.
        // These include:
        // - remove FileActionSchema url and identity columns
        // - rename current_user_identity to CurrentUserId
        // - change most of the nullable columns to non-nullable
        constexpr uint64_t SCHEMA_VERSION = 7;

        m_config.automatic_change_notifications = false;
        m_config.path = std::move(path);
        m_config.schema = Schema{
            UserIdentitySchema::object_schema(),
            SyncUserSchema::object_schema(),
            FileActionSchema::object_schema(),
            CurrentUserSchema::object_schema(),
        };

        m_config.schema_version = SCHEMA_VERSION;
        m_config.schema_mode = SchemaMode::Automatic;
        m_config.scheduler = util::Scheduler::make_dummy();
        m_config.automatically_handle_backlinks_in_migrations = true;
        m_config.migration_function = [](std::shared_ptr<Realm> old_realm, std::shared_ptr<Realm> realm, Schema&) {
            if (old_realm->schema_version() < 7) {
                migrate_to_v7(old_realm, realm);
            }
        };

        auto realm = open_realm(m_config, app_config);
        m_user_schema.read(*realm);
        m_file_action_schema.read(*realm);
        m_user_identity_schema.read(*realm);
        m_current_user_schema.read(*realm);

        realm->begin_transaction();
        perform_file_actions(*realm, file_manager);
        remove_dead_users(*realm, file_manager);
        realm->commit_transaction();
    }

    std::shared_ptr<Realm> get_realm() const
    {
        return Realm::get_shared_realm(m_config);
    }

    void remove_dead_users(Realm& realm, SyncFileManager& file_manager)
    {
        auto& schema = m_user_schema;
        TableRef table = realm.read_group().get_table(schema.table_key);
        for (auto obj : *table) {
            if (static_cast<SyncUser::State>(obj.get<int64_t>(schema.state_col)) == SyncUser::State::Removed) {
                delete_user_realms(file_manager, obj);
            }
        }
    }

    void delete_user_realms(SyncFileManager& file_manager, Obj& obj)
    {
        Set<StringData> paths = obj.get_set<StringData>(m_user_schema.realm_file_paths_col);
        bool any_failed = false;
        for (auto path : paths) {
            if (!file_manager.remove_realm(path))
                any_failed = true;
        }
        try {
            file_manager.remove_user_realms(obj.get<String>(m_user_schema.user_id_col));
        }
        catch (FileAccessError const&) {
            any_failed = true;
        }

        // Only remove the object if all of the tracked realms no longer exist,
        // and otherwise try again to delete them on the next launch
        if (!any_failed) {
            obj.remove();
        }
    }

    bool perform_file_action(SyncFileManager& file_manager, Obj& obj)
    {
        auto& schema = m_file_action_schema;
        switch (static_cast<SyncFileAction>(obj.get<int64_t>(schema.idx_action))) {
            case SyncFileAction::DeleteRealm:
                // Delete all the files for the given Realm.
                return file_manager.remove_realm(obj.get<String>(schema.idx_original_name));

            case SyncFileAction::BackUpThenDeleteRealm:
                // Copy the primary Realm file to the recovery dir, and then delete the Realm.
                auto new_name = obj.get<String>(schema.idx_new_name);
                auto original_name = obj.get<String>(schema.idx_original_name);
                if (!util::File::exists(original_name)) {
                    // The Realm file doesn't exist anymore, which is fine
                    return true;
                }

                if (new_name && file_manager.copy_realm_file(original_name, new_name)) {
                    // We successfully copied the Realm file to the recovery directory.
                    bool did_remove = file_manager.remove_realm(original_name);
                    // if the copy succeeded but not the delete, then running BackupThenDelete
                    // a second time would fail, so change this action to just delete the original file.
                    if (did_remove) {
                        return true;
                    }
                    obj.set(schema.idx_action, static_cast<int64_t>(SyncFileAction::DeleteRealm));
                }
        }
        return false;
    }

    void perform_file_actions(Realm& realm, SyncFileManager& file_manager)
    {
        TableRef table = realm.read_group().get_table(m_file_action_schema.table_key);
        if (table->is_empty())
            return;

        for (auto obj : *table) {
            if (perform_file_action(file_manager, obj))
                obj.remove();
        }
    }

    bool immediately_run_file_actions(SyncFileManager& file_manager, std::string_view realm_path) override
    {
        auto realm = get_realm();
        realm->begin_transaction();
        TableRef table = realm->read_group().get_table(m_file_action_schema.table_key);
        auto key = table->where().equal(m_file_action_schema.idx_original_name, StringData(realm_path)).find();
        if (!key) {
            return false;
        }
        auto obj = table->get_object(key);
        bool did_run = perform_file_action(file_manager, obj);
        if (did_run)
            obj.remove();
        realm->commit_transaction();
        return did_run;
    }

    bool has_logged_in_user(std::string_view user_id) override
    {
        auto realm = get_realm();
        auto obj = find_user(*realm, user_id);
        return is_valid_user(obj);
    }

    std::optional<UserData> get_user(std::string_view user_id) override
    {
        auto realm = get_realm();
        return read_user(find_user(*realm, user_id));
    }

    void create_user(std::string_view user_id, std::string_view refresh_token, std::string_view access_token,
                     std::string_view device_id) override
    {
        auto realm = get_realm();
        realm->begin_transaction();

        auto& schema = m_user_schema;
        Obj obj = find_user(*realm, user_id);
        if (!obj) {
            obj = realm->read_group().get_table(m_user_schema.table_key)->create_object();
            obj.set<String>(schema.user_id_col, user_id);

            // Mark the user we just created as the current user
            Obj current_user = current_user_obj(*realm);
            current_user.set<String>(m_current_user_schema.user_id, user_id);
        }

        obj.set(schema.state_col, (int64_t)SyncUser::State::LoggedIn);
        obj.set<String>(schema.refresh_token_col, refresh_token);
        obj.set<String>(schema.access_token_col, access_token);
        obj.set<String>(schema.device_id_col, device_id);

        realm->commit_transaction();
    }

    void update_user(std::string_view user_id, util::FunctionRef<void(UserData&)> update_fn) override
    {
        auto realm = get_realm();
        realm->begin_transaction();
        auto& schema = m_user_schema;
        Obj obj = find_user(*realm, user_id);
        auto opt_data = read_user(obj);
        if (!opt_data) {
            realm->cancel_transaction();
            return;
        }

        auto& data = *opt_data;
        update_fn(data);

        obj.set(schema.state_col,
                int64_t(data.access_token ? SyncUser::State::LoggedIn : SyncUser::State::LoggedOut));
        obj.set<String>(schema.refresh_token_col, data.refresh_token.token);
        obj.set<String>(schema.access_token_col, data.access_token.token);
        obj.set<String>(schema.device_id_col, data.device_id);

        std::stringstream profile;
        profile << data.profile.data();
        obj.set(schema.profile_dump_col, profile.str());

        auto identities_list = obj.get_linklist(schema.identities_col);
        identities_list.clear();

        for (auto& ident : data.identities) {
            auto obj = identities_list.create_and_insert_linked_object(identities_list.size());
            obj.set<String>(m_user_identity_schema.user_id, ident.id);
            obj.set<String>(m_user_identity_schema.provider_id, ident.provider_type);
        }

        // intentionally does not update `legacy_identities` as that field is
        // read-only and no longer used

        realm->commit_transaction();
    }

    Obj current_user_obj(Realm& realm) const
    {
        TableRef current_user_table = realm.read_group().get_table(m_current_user_schema.table_key);
        Obj obj;
        if (!current_user_table->is_empty())
            obj = *current_user_table->begin();
        else if (realm.is_in_transaction())
            obj = current_user_table->create_object();
        return obj;
    }

    // Some of our string columns are nullable. They never should actually be
    // null as we store "" rather than null when the value isn't present, but
    // be safe and handle it anyway.
    static std::string get_string(const Obj& obj, ColKey col)
    {
        auto str = obj.get<String>(col);
        return str.is_null() ? "" : str;
    }

    std::optional<app::UserData> read_user(const Obj& obj) const
    {
        if (!obj) {
            return {};
        }
        auto state = SyncUser::State(obj.get<Int>(m_user_schema.state_col));
        if (state == SyncUser::State::Removed) {
            return {};
        }

        UserData data;
        if (state == SyncUser::State::LoggedIn) {
            try {
                data.access_token = RealmJWT(get_string(obj, m_user_schema.access_token_col));
                data.refresh_token = RealmJWT(get_string(obj, m_user_schema.refresh_token_col));
            }
            catch (...) {
                // Invalid stored token results in a logged-out user
                data.access_token = {};
                data.refresh_token = {};
            }
        }

        data.device_id = get_string(obj, m_user_schema.device_id_col);
        if (auto profile = obj.get<String>(m_user_schema.profile_dump_col); profile.size()) {
            data.profile = static_cast<bson::BsonDocument>(bson::parse(std::string_view(profile)));
        }

        auto identities_list = obj.get_linklist(m_user_schema.identities_col);
        auto identities_table = identities_list.get_target_table();
        data.identities.reserve(identities_list.size());
        for (size_t i = 0, size = identities_list.size(); i < size; ++i) {
            auto obj = identities_table->get_object(identities_list.get(i));
            data.identities.push_back({obj.get<String>(m_user_identity_schema.user_id),
                                       obj.get<String>(m_user_identity_schema.provider_id)});
        }

        auto legacy_identities = obj.get_list<String>(m_user_schema.legacy_uuids_col);
        data.legacy_identities.reserve(legacy_identities.size());
        for (size_t i = 0, size = legacy_identities.size(); i < size; ++i) {
            data.legacy_identities.push_back(legacy_identities.get(i));
        }

        return data;
    }

    void update_current_user(Realm& realm, std::string_view removed_user_id)
    {
        auto current_user = current_user_obj(realm);
        if (current_user.get<String>(m_current_user_schema.user_id) == removed_user_id) {
            // Set to either empty or the first still logged in user
            current_user.set(m_current_user_schema.user_id, get_current_user());
        }
    }

    void log_out(std::string_view user_id, SyncUser::State new_state) override
    {
        REALM_ASSERT(new_state != SyncUser::State::LoggedIn);
        auto realm = get_realm();
        realm->begin_transaction();
        if (auto obj = find_user(*realm, user_id)) {
            obj.set(m_user_schema.state_col, (int64_t)new_state);
            obj.set<String>(m_user_schema.access_token_col, "");
            obj.set<String>(m_user_schema.refresh_token_col, "");
            update_current_user(*realm, user_id);
        }
        realm->commit_transaction();
    }

    void delete_user(SyncFileManager& file_manager, std::string_view user_id) override
    {
        auto realm = get_realm();
        realm->begin_transaction();
        if (auto obj = find_user(*realm, user_id)) {
            delete_user_realms(file_manager, obj); // also removes obj
            update_current_user(*realm, user_id);
        }
        realm->commit_transaction();
    }

    void add_realm_path(std::string_view user_id, std::string_view path) override
    {
        auto realm = get_realm();
        realm->begin_transaction();
        if (auto obj = find_user(*realm, user_id)) {
            obj.get_set<String>(m_user_schema.realm_file_paths_col).insert(path);
        }
        realm->commit_transaction();
    }

    bool is_valid_user(Obj& obj)
    {
        // This is overly cautious and merely checking the state should suffice,
        // but because this is a persisted file that can be modified it's possible
        // to get invalid combinations of data.
        return obj && obj.get<int64_t>(m_user_schema.state_col) == int64_t(SyncUser::State::LoggedIn) &&
               RealmJWT::validate(get_string(obj, m_user_schema.access_token_col)) &&
               RealmJWT::validate(get_string(obj, m_user_schema.refresh_token_col));
    }

    std::vector<std::string> get_all_users() override
    {
        auto realm = get_realm();
        auto table = realm->read_group().get_table(m_user_schema.table_key);
        std::vector<std::string> users;
        users.reserve(table->size());
        for (auto& obj : *table) {
            if (obj.get<int64_t>(m_user_schema.state_col) != int64_t(SyncUser::State::Removed)) {
                users.emplace_back(obj.get<String>(m_user_schema.user_id_col));
            }
        }
        return users;
    }

    std::string get_current_user() override
    {
        auto realm = get_realm();
        if (auto obj = current_user_obj(*realm)) {
            auto user_id = obj.get<String>(m_current_user_schema.user_id);
            auto user_obj = find_user(*realm, user_id);
            if (is_valid_user(user_obj)) {
                return user_id;
            }
        }

        auto table = realm->read_group().get_table(m_user_schema.table_key);
        for (auto& obj : *table) {
            if (is_valid_user(obj)) {
                return obj.get<String>(m_user_schema.user_id_col);
            }
        }

        return "";
    }

    void set_current_user(std::string_view user_id) override
    {
        auto realm = get_realm();
        realm->begin_transaction();
        current_user_obj(*realm).set<String>(m_current_user_schema.user_id, user_id);
        realm->commit_transaction();
    }

    void create_file_action(SyncFileAction action, std::string_view original_path,
                            std::string_view recovery_path) override
    {
        REALM_ASSERT(action != SyncFileAction::BackUpThenDeleteRealm || !recovery_path.empty());

        auto realm = get_realm();
        realm->begin_transaction();
        TableRef table = realm->read_group().get_table(m_file_action_schema.table_key);
        Obj obj = table->create_object_with_primary_key(original_path);
        obj.set(m_file_action_schema.idx_new_name, recovery_path);
        obj.set(m_file_action_schema.idx_action, static_cast<int64_t>(action));
        // There's also partition and user_id fields in the schema, but they
        // aren't actually used for anything and are never read
        realm->commit_transaction();
    }

    Obj find_user(Realm& realm, StringData user_id) const
    {
        Obj obj;
        if (user_id.size() == 0)
            return obj;

        auto table = realm.read_group().get_table(m_user_schema.table_key);
        Query q = table->where().equal(m_user_schema.user_id_col, user_id);
        REALM_ASSERT_DEBUG(q.count() < 2); // user_id_col ought to be a primary key
        if (auto key = q.find())
            obj = table->get_object(key);
        return obj;
    }
};

class InMemoryMetadataStorage : public app::MetadataStore {
    std::mutex m_mutex;
    std::map<std::string, UserData, std::less<>> m_users;
    std::map<std::string, std::set<std::string>, std::less<>> m_realm_paths;
    std::string m_active_user;
    struct FileAction {
        SyncFileAction action;
        std::string backup_path;
    };
    std::map<std::string, FileAction, std::less<>> m_file_actions;

    bool has_logged_in_user(std::string_view user_id) override
    {
        std::lock_guard lock(m_mutex);
        auto it = m_users.find(user_id);
        return it != m_users.end() && it->second.access_token;
    }

    std::optional<UserData> get_user(std::string_view user_id) override
    {
        std::lock_guard lock(m_mutex);
        if (auto it = m_users.find(user_id); it != m_users.end()) {
            return it->second;
        }
        return {};
    }

    void create_user(std::string_view user_id, std::string_view refresh_token, std::string_view access_token,
                     std::string_view device_id) override
    {
        std::lock_guard lock(m_mutex);
        auto it = m_users.find(user_id);
        if (it == m_users.end()) {
            it = m_users.insert({std::string(user_id), UserData{}}).first;
            m_active_user = user_id;
        }
        auto& user = it->second;
        user.device_id = device_id;
        try {
            user.refresh_token = RealmJWT(refresh_token);
            user.access_token = RealmJWT(access_token);
        }
        catch (...) {
            user.refresh_token = {};
            user.access_token = {};
        }
    }

    void update_user(std::string_view user_id, util::FunctionRef<void(UserData&)> update_fn) override
    {
        std::lock_guard lock(m_mutex);
        auto it = m_users.find(user_id);
        if (it == m_users.end()) {
            return;
        }

        update_fn(it->second);
        it->second.legacy_identities.clear();
    }

    void log_out(std::string_view user_id, SyncUser::State new_state) override
    {
        std::lock_guard lock(m_mutex);
        if (auto it = m_users.find(user_id); it != m_users.end()) {
            if (new_state == SyncUser::State::Removed) {
                m_users.erase(it);
            }
            else {
                auto& user = it->second;
                user.access_token = {};
                user.refresh_token = {};
                user.device_id.clear();
            }
        }
    }

    void delete_user(SyncFileManager& file_manager, std::string_view user_id) override
    {
        std::lock_guard lock(m_mutex);
        if (auto it = m_users.find(user_id); it != m_users.end()) {
            m_users.erase(it);
        }
        if (auto it = m_realm_paths.find(user_id); it != m_realm_paths.end()) {
            for (auto& path : it->second) {
                file_manager.remove_realm(path);
            }
        }
    }

    std::string get_current_user() override
    {
        std::lock_guard lock(m_mutex);
        if (auto it = m_users.find(m_active_user); it != m_users.end() && it->second.access_token) {
            return m_active_user;
        }

        for (auto& [user_id, data] : m_users) {
            if (data.access_token) {
                m_active_user = user_id;
                return user_id;
            }
        }

        return "";
    }

    void set_current_user(std::string_view user_id) override
    {
        std::lock_guard lock(m_mutex);
        m_active_user = user_id;
    }

    std::vector<std::string> get_all_users() override
    {
        std::lock_guard lock(m_mutex);
        std::vector<std::string> users;
        for (auto& [user_id, _] : m_users) {
            users.push_back(user_id);
        }
        return users;
    }

    void add_realm_path(std::string_view user_id, std::string_view path) override
    {
        std::lock_guard lock(m_mutex);
        m_realm_paths[std::string(user_id)].insert(std::string(path));
    }

    bool immediately_run_file_actions(SyncFileManager& file_manager, std::string_view path) override
    {
        std::lock_guard lock(m_mutex);
        auto it = m_file_actions.find(path);
        if (it == m_file_actions.end())
            return false;
        auto& old_path = it->first;
        switch (it->second.action) {
            case SyncFileAction::DeleteRealm:
                if (file_manager.remove_realm(old_path)) {
                    m_file_actions.erase(it);
                    return true;
                }
                return false;

            case SyncFileAction::BackUpThenDeleteRealm:
                if (!util::File::exists(old_path)) {
                    m_file_actions.erase(it);
                    return true;
                }
                auto& new_path = it->second.backup_path;
                if (!file_manager.copy_realm_file(old_path, new_path)) {
                    return false;
                }
                if (file_manager.remove_realm(old_path)) {
                    m_file_actions.erase(it);
                    return true;
                }
                it->second.action = SyncFileAction::DeleteRealm;
                return false;
        }
        return false;
    }

    void create_file_action(SyncFileAction action, std::string_view path, std::string_view backup_path) override
    {
        std::lock_guard lock(m_mutex);
        REALM_ASSERT(action != SyncFileAction::BackUpThenDeleteRealm || !backup_path.empty());
        m_file_actions[std::string(path)] = FileAction{action, std::string(backup_path)};
    }
};

} // anonymous namespace

app::MetadataStore::~MetadataStore() = default;

std::unique_ptr<app::MetadataStore> app::create_metadata_store(const AppConfig& config, SyncFileManager& file_manager)
{
    if (config.metadata_mode == AppConfig::MetadataMode::InMemory) {
        return std::make_unique<InMemoryMetadataStorage>();
    }
    return std::make_unique<PersistedSyncMetadataManager>(file_manager.metadata_path(), config, file_manager);
}
