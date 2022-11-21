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

#include "sync_test_utils.hpp"

#include "util/baas_admin_api.hpp"

#include <realm/object-store/object_store.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/hex_dump.hpp>
#include <realm/util/sha_crypto.hpp>

namespace realm {

std::ostream& operator<<(std::ostream& os, util::Optional<app::AppError> error)
{
    if (!error) {
        os << "(none)";
    }
    else {
        os << "AppError(error_code=" << error->error_code
           << ", http_status_code=" << error->http_status_code.value_or(0) << ", message=\"" << error->message
           << "\", link_to_server_logs=\"" << error->link_to_server_logs << "\")";
    }
    return os;
}

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity,
                           const std::string& provider_type)
{
    for (size_t i = 0; i < results.size(); i++) {
        auto this_result = results.get(i);
        if (this_result.identity() == identity && this_result.provider_type() == provider_type) {
            return true;
        }
    }
    return false;
}

bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name)
{
    for (size_t i = 0; i < results.size(); i++) {
        if (results.get(i).original_name() == original_name) {
            return true;
        }
    }
    return false;
}

void timed_wait_for(util::FunctionRef<bool()> condition, std::chrono::milliseconds max_ms)
{
    const auto wait_start = std::chrono::steady_clock::now();
    util::EventLoop::main().run_until([&] {
        if (std::chrono::steady_clock::now() - wait_start > max_ms) {
            throw std::runtime_error(util::format("timed_wait_for exceeded %1 ms", max_ms.count()));
        }
        return condition();
    });
}

void timed_sleeping_wait_for(util::FunctionRef<bool()> condition, std::chrono::milliseconds max_ms,
                             std::chrono::milliseconds sleep_ms)
{
    const auto wait_start = std::chrono::steady_clock::now();
    while (!condition()) {
        if (std::chrono::steady_clock::now() - wait_start > max_ms) {
            throw std::runtime_error(util::format("timed_sleeping_wait_for exceeded %1 ms", max_ms.count()));
        }
        std::this_thread::sleep_for(sleep_ms);
    }
}

auto do_hash = [](const std::string& name) -> std::string {
    std::array<unsigned char, 32> hash;
    util::sha256(name.data(), name.size(), hash.data());
    return util::hex_dump(hash.data(), hash.size(), "");
};

ExpectedRealmPaths::ExpectedRealmPaths(const std::string& base_path, const std::string& app_id,
                                       const std::string& identity, const std::string& local_identity,
                                       const std::string& partition)
{
    // This is copied from SyncManager.cpp string_from_partition() in order to prevent
    // us changing that function and therefore breaking user's existing paths unknowingly.
    std::string cleaned_partition;
    bson::Bson partition_value = bson::parse(partition);
    switch (partition_value.type()) {
        case bson::Bson::Type::Int32:
            cleaned_partition = util::format("i_%1", static_cast<int32_t>(partition_value));
            break;
        case bson::Bson::Type::Int64:
            cleaned_partition = util::format("l_%1", static_cast<int64_t>(partition_value));
            break;
        case bson::Bson::Type::String:
            cleaned_partition = util::format("s_%1", static_cast<std::string>(partition_value));
            break;
        case bson::Bson::Type::ObjectId:
            cleaned_partition = util::format("o_%1", static_cast<ObjectId>(partition_value).to_string());
            break;
        case bson::Bson::Type::Uuid:
            cleaned_partition = util::format("u_%1", static_cast<UUID>(partition_value).to_string());
            break;
        case bson::Bson::Type::Null:
            cleaned_partition = "null";
            break;
        default:
            REALM_ASSERT(false);
    }

    std::string clean_name = cleaned_partition;
    std::string cleaned_app_id = util::make_percent_encoded_string(app_id);
    const auto manager_path = fs::path{base_path}.make_preferred() / "mongodb-realm" / cleaned_app_id;
    const auto preferred_name = manager_path / identity / clean_name;
    current_preferred_path = preferred_name.string() + ".realm";
    fallback_hashed_path = (manager_path / do_hash(preferred_name.string())).string() + ".realm";
    legacy_sync_directories_to_make.push_back((manager_path / local_identity).string());
    std::string encoded_partition = util::make_percent_encoded_string(partition);
    legacy_local_id_path = (manager_path / local_identity / encoded_partition).concat(".realm").string();
    auto dir_builder = manager_path / "realm-object-server";
    legacy_sync_directories_to_make.push_back(dir_builder.string());
    dir_builder /= local_identity;
    legacy_sync_directories_to_make.push_back(dir_builder.string());
    legacy_sync_path = (dir_builder / cleaned_partition).string();
}

#if REALM_ENABLE_SYNC

#if REALM_ENABLE_AUTH_TESTS

#ifdef REALM_MONGODB_ENDPOINT
std::string get_base_url()
{
    // allows configuration with or without quotes
    std::string base_url = REALM_QUOTE(REALM_MONGODB_ENDPOINT);
    if (base_url.size() > 0 && base_url[0] == '"') {
        base_url.erase(0, 1);
    }
    if (base_url.size() > 0 && base_url[base_url.size() - 1] == '"') {
        base_url.erase(base_url.size() - 1);
    }
    return base_url;
}
#endif // REALM_MONGODB_ENDPOINT

AutoVerifiedEmailCredentials::AutoVerifiedEmailCredentials()
{
    // emails with this prefix will pass through the baas app due to the register function
    email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
    password = random_string(10);
    static_cast<AppCredentials&>(*this) = AppCredentials::username_password(email, password);
}

AutoVerifiedEmailCredentials create_user_and_log_in(app::SharedApp app)
{
    REQUIRE(app);
    AutoVerifiedEmailCredentials creds;
    app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
        creds.email, creds.password, [&](util::Optional<app::AppError> error) {
            REQUIRE(!error);
        });
    app->log_in_with_credentials(realm::app::AppCredentials::username_password(creds.email, creds.password),
                                 [&](std::shared_ptr<realm::SyncUser> user, util::Optional<app::AppError> error) {
                                     REQUIRE(user);
                                     REQUIRE(!error);
                                 });
    return creds;
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC

class TestHelper {
public:
    static DBRef& get_db(SharedRealm const& shared_realm)
    {
        return Realm::Internal::get_db(*shared_realm);
    }
};

namespace reset_utils {

Obj create_object(Realm& realm, StringData object_type, util::Optional<ObjectId> primary_key,
                  util::Optional<Partition> partition)
{
    auto table = realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
    REQUIRE(table);
    FieldValues values = {};
    if (partition) {
        ColKey col = table->get_column_key(partition->property_name);
        REALM_ASSERT(col);
        values.insert(col, Mixed{partition->value});
    }
    return table->create_object_with_primary_key(primary_key ? *primary_key : ObjectId::gen(), std::move(values));
}

namespace {

TableRef get_table(Realm& realm, StringData object_type)
{
    return realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

// Run through the client reset steps manually without involving a sync server.
// Useful for speed and when integration testing is not available on a platform.
struct FakeLocalClientReset : public TestClientReset {
    FakeLocalClientReset(const Realm::Config& local_config, const Realm::Config& remote_config)
        : TestClientReset(local_config, remote_config)
    {
        REALM_ASSERT(m_local_config.sync_config);
        m_mode = m_local_config.sync_config->client_resync_mode;
        REALM_ASSERT(m_mode == ClientResyncMode::DiscardLocal || m_mode == ClientResyncMode::Recover);
        // Turn off real sync. But we still need a SyncClientHistory for recovery mode so fake it.
        m_local_config.sync_config = {};
        m_remote_config.sync_config = {};
        m_local_config.force_sync_history = true;
        m_remote_config.force_sync_history = true;
        m_local_config.in_memory = true;
        m_local_config.encryption_key = std::vector<char>();
        m_remote_config.in_memory = true;
        m_remote_config.encryption_key = std::vector<char>();
    }

    void run() override
    {
        m_did_run = true;
        auto local_realm = Realm::get_shared_realm(m_local_config);
        if (m_on_setup) {
            local_realm->begin_transaction();
            m_on_setup(local_realm);
            local_realm->commit_transaction();

            // Update the sync history to mark this initial setup state as if it
            // has been uploaded so that it doesn't replay during recovery.
            auto history_local =
                dynamic_cast<sync::ClientHistory*>(local_realm->read_group().get_replication()->_get_history_write());
            REALM_ASSERT(history_local);
            sync::version_type current_version;
            sync::SaltedFileIdent file_ident;
            sync::SyncProgress progress;
            history_local->get_status(current_version, file_ident, progress);
            progress.upload.client_version = current_version;
            progress.upload.last_integrated_server_version = current_version;
            sync::VersionInfo info_out;
            history_local->set_sync_progress(progress, nullptr, info_out);
        }
        {
            local_realm->begin_transaction();
            auto obj = create_object(*local_realm, "object", m_pk_driving_reset);
            auto col = obj.get_table()->get_column_key("value");
            obj.set(col, 1);
            obj.set(col, 2);
            obj.set(col, 3);
            local_realm->commit_transaction();

            local_realm->begin_transaction();
            obj.set(col, 4);
            if (m_make_local_changes) {
                m_make_local_changes(local_realm);
            }
            local_realm->commit_transaction();
            if (m_on_post_local) {
                m_on_post_local(local_realm);
            }
        }

        {
            auto remote_realm = Realm::get_shared_realm(m_remote_config);
            remote_realm->begin_transaction();
            if (m_on_setup) {
                m_on_setup(remote_realm);
            }

            // fake a sync by creating an object with the same pk
            create_object(*remote_realm, "object", m_pk_driving_reset);

            for (int i = 0; i < 2; ++i) {
                auto table = get_table(*remote_realm, "object");
                auto col = table->get_column_key("value");
                table->begin()->set(col, i + 5);
            }

            if (m_make_remote_changes) {
                m_make_remote_changes(remote_realm);
            }
            remote_realm->commit_transaction();

            sync::SaltedFileIdent fake_ident{1, 123456789};
            auto local_db = TestHelper::get_db(local_realm);
            auto remote_db = TestHelper::get_db(remote_realm);
            util::StderrLogger logger(realm::util::Logger::Level::TEST_ENABLE_SYNC_LOGGING_LEVEL);
            using _impl::client_reset::perform_client_reset_diff;
            constexpr bool recovery_is_allowed = true;
            perform_client_reset_diff(local_db, remote_db, fake_ident, logger, m_mode, recovery_is_allowed, nullptr,
                                      nullptr, nullptr);

            remote_realm->close();
            if (m_on_post_reset) {
                m_on_post_reset(local_realm);
            }
        }
    }

private:
    ClientResyncMode m_mode;
};
} // anonymous namespace

#if REALM_ENABLE_SYNC

#if REALM_ENABLE_AUTH_TESTS

static void wait_for_object_to_persist(std::shared_ptr<SyncUser> user, const AppSession& app_session,
                                       const std::string& schema_name, const bson::BsonDocument& filter_bson)
{
    // While at this point the object has been sync'd successfully, we must also
    // wait for it to appear in the backing database before terminating sync
    // otherwise the translator may be terminated before it has a chance to
    // integrate it into the backing database. If the server were to change
    // the meaning of "upload complete" to include writing to atlas then this would
    // not be necessary.
    app::MongoClient remote_client = user->mongo_client("BackingDB");
    app::MongoDatabase db = remote_client.db(app_session.config.mongo_dbname);
    app::MongoCollection object_coll = db[schema_name];

    timed_sleeping_wait_for(
        [&]() -> bool {
            auto pf = util::make_promise_future<uint64_t>();
            object_coll.count(filter_bson, [promise = std::move(pf.promise)](
                                               uint64_t count, util::Optional<app::AppError> error) mutable {
                REQUIRE(!error);
                if (error) {
                    promise.set_error({ErrorCodes::RuntimeError, error->message});
                }
                else {
                    promise.emplace_value(count);
                }
            });
            return pf.future.get() > 0;
        },
        std::chrono::minutes(15), std::chrono::milliseconds(100));
}

struct BaasClientReset : public TestClientReset {
    BaasClientReset(const Realm::Config& local_config, const Realm::Config& remote_config,
                    TestAppSession& test_app_session)
        : TestClientReset(local_config, remote_config)
        , m_test_app_session(test_app_session)
    {
    }

    void run() override
    {
        m_did_run = true;
        const AppSession& app_session = m_test_app_session.app_session();
        auto sync_manager = m_test_app_session.app()->sync_manager();
        std::string partition_value = m_local_config.sync_config->partition_value;
        REALM_ASSERT(partition_value.size() > 2 && *partition_value.begin() == '"' &&
                     *(partition_value.end() - 1) == '"');
        partition_value = partition_value.substr(1, partition_value.size() - 2);
        Partition partition = {app_session.config.partition_key.name, partition_value};

        // There is a race in PBS where if initial sync is still in-progress while you're creating the initial
        // object below, you may end up creating it in your local realm, uploading it, have the translator process
        // the upload, then initial sync the processed object, and then send it back to you as an erase/create
        // object instruction.
        //
        // So just don't try to do anything until initial sync is done and we're sure the server is in a stable
        // state.
        timed_sleeping_wait_for([&] {
            return app_session.admin_api.is_initial_sync_complete(app_session.server_app_id);
        });

        auto realm = Realm::get_shared_realm(m_local_config);
        auto session = sync_manager->get_existing_session(realm->config().path);
        const std::string object_schema_name = "object";
        {
            wait_for_download(*realm);
            realm->begin_transaction();

            if (m_on_setup) {
                m_on_setup(realm);
            }

            auto obj = create_object(*realm, object_schema_name, {m_pk_driving_reset}, {partition});
            auto table = obj.get_table();
            auto col = table->get_column_key("value");
            std::string pk_col_name = table->get_column_name(table->get_primary_key_column());
            obj.set(col, 1);
            obj.set(col, 2);
            constexpr int64_t last_synced_value = 3;
            obj.set(col, last_synced_value);
            realm->commit_transaction();
            wait_for_upload(*realm);
            wait_for_download(*realm);

            wait_for_object_to_persist(m_local_config.sync_config->user, app_session, object_schema_name,
                                       {{pk_col_name, m_pk_driving_reset}, {"value", last_synced_value}});

            session->log_out();

            realm->begin_transaction();
            obj.set(col, 4);
            if (m_make_local_changes) {
                m_make_local_changes(realm);
            }
            realm->commit_transaction();
        }

        // cause a client reset by restarting the sync service
        // this causes the server's sync history to be resynthesized
        auto baas_sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);
        auto baas_sync_config = app_session.admin_api.get_config(app_session.server_app_id, baas_sync_service);
        REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        app_session.admin_api.disable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        timed_sleeping_wait_for([&] {
            return app_session.admin_api.is_sync_terminated(app_session.server_app_id);
        });
        app_session.admin_api.enable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        if (app_session.config.dev_mode_enabled) { // dev mode is not sticky across a reset
            app_session.admin_api.set_development_mode_to(app_session.server_app_id, true);
        }

        timed_sleeping_wait_for([&] {
            return app_session.admin_api.is_initial_sync_complete(app_session.server_app_id);
        });

        {
            auto realm2 = Realm::get_shared_realm(m_remote_config);
            wait_for_download(*realm2);

            timed_sleeping_wait_for(
                [&]() -> bool {
                    realm2->begin_transaction();
                    auto table = get_table(*realm2, object_schema_name);
                    auto objkey = table->find_primary_key({m_pk_driving_reset});
                    realm2->cancel_transaction();
                    return bool(objkey);
                },
                std::chrono::seconds(60));

            // expect the last sync'd object to be in place
            realm2->begin_transaction();
            auto table = get_table(*realm2, object_schema_name);
            REQUIRE(table->size() >= 1);
            auto obj = table->get_object_with_primary_key({m_pk_driving_reset});
            REQUIRE(obj.is_valid());
            auto col = table->get_column_key("value");
            REQUIRE(obj.get_any(col) == Mixed{3});

            // make a change
            table->begin()->set(col, 6);
            realm2->commit_transaction();
            wait_for_upload(*realm2);
            wait_for_download(*realm2);

            realm2->begin_transaction();
            if (m_make_remote_changes) {
                m_make_remote_changes(realm2);
            }
            realm2->commit_transaction();
            wait_for_upload(*realm2);
            wait_for_download(*realm2);
            realm2->close();
        }

        // Resuming sync on the first realm should now result in a client reset
        session->revive_if_needed();
        if (m_on_post_local) {
            m_on_post_local(realm);
        }
        if (!m_wait_for_reset_completion) {
            return;
        }
        wait_for_upload(*realm);
        if (m_on_post_reset) {
            m_on_post_reset(realm);
        }
    }

private:
    TestAppSession& m_test_app_session;
};

struct BaasFLXClientReset : public TestClientReset {
    BaasFLXClientReset(const Realm::Config& local_config, const Realm::Config& remote_config,
                       const TestAppSession& test_app_session)
        : TestClientReset(local_config, remote_config)
        , m_test_app_session(test_app_session)
    {
        REALM_ASSERT(m_local_config.sync_config->flx_sync_requested);
        REALM_ASSERT(m_remote_config.sync_config->flx_sync_requested);
        REALM_ASSERT(m_local_config.schema->find(c_object_schema_name) != m_local_config.schema->end());
    }

    void run() override
    {
        m_did_run = true;
        const AppSession& app_session = m_test_app_session.app_session();

        auto realm = Realm::get_shared_realm(m_local_config);
        auto session = realm->sync_session();
        const ObjectId pk_of_added_object("123456789000000000000000");
        {
            if (m_on_setup) {
                m_on_setup(realm);
            }
            constexpr bool create_object = true;
            subscribe_to_object_by_id(realm, pk_of_added_object, create_object);

            wait_for_object_to_persist(m_local_config.sync_config->user, app_session,
                                       std::string(c_object_schema_name),
                                       {{std::string(c_id_col_name), pk_of_added_object}});
            session->log_out();

            if (m_make_local_changes) {
                m_make_local_changes(realm);
            }
        }

        // cause a client reset by restarting the sync service
        // this causes the server's sync history to be resynthesized
        auto baas_sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);
        auto baas_sync_config = app_session.admin_api.get_config(app_session.server_app_id, baas_sync_service);
        REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        app_session.admin_api.disable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        timed_sleeping_wait_for([&] {
            return app_session.admin_api.is_sync_terminated(app_session.server_app_id);
        });
        app_session.admin_api.enable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        if (app_session.config.dev_mode_enabled) { // dev mode is not sticky across a reset
            app_session.admin_api.set_development_mode_to(app_session.server_app_id, true);
        }

        {
            auto realm2 = Realm::get_shared_realm(m_remote_config);
            wait_for_download(*realm2);
            load_initial_data(realm2);

            timed_sleeping_wait_for(
                [&]() -> bool {
                    realm2->begin_transaction();
                    auto table = get_table(*realm2, c_object_schema_name);
                    auto objkey = table->find_primary_key({pk_of_added_object});
                    realm2->cancel_transaction();
                    return bool(objkey);
                },
                std::chrono::seconds(60));

            // expect the last sync'd object to be in place
            realm2->begin_transaction();
            auto table = get_table(*realm2, c_object_schema_name);
            REQUIRE(table->size() >= 1);
            auto obj = table->get_object_with_primary_key({pk_of_added_object});
            REQUIRE(obj.is_valid());
            realm2->commit_transaction();

            if (m_make_remote_changes) {
                m_make_remote_changes(realm2);
            }
            wait_for_upload(*realm2);
            auto subs = realm2->get_latest_subscription_set();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            realm2->close();
        }

        // Resuming sync on the first realm should now result in a client reset
        session->revive_if_needed();
        if (m_on_post_local) {
            m_on_post_local(realm);
        }
        wait_for_upload(*realm);
        if (m_on_post_reset) {
            m_on_post_reset(realm);
        }
    }

private:
    void subscribe_to_object_by_id(SharedRealm realm, ObjectId pk, bool create_object = false)
    {
        auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
        Group::TableNameBuffer buffer;
        auto class_name = Group::class_name_to_table_name(c_object_schema_name, buffer);
        TableRef table = realm->read_group().get_table(class_name);
        REALM_ASSERT(table);
        ColKey id_col = table->get_column_key(c_id_col_name);
        REALM_ASSERT(id_col);
        ColKey str_col = table->get_column_key(c_str_col_name);
        REALM_ASSERT(str_col);
        Query query_for_added_object = table->where().equal(id_col, pk);
        mut_subs.insert_or_assign(query_for_added_object);
        auto subs = std::move(mut_subs).commit();
        if (create_object) {
            realm->begin_transaction();
            table->create_object_with_primary_key(pk, {{str_col, "initial value"}});
            realm->commit_transaction();
        }
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    }

    void load_initial_data(SharedRealm realm)
    {
        auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
        for (const auto& table : realm->schema()) {
            Query query_for_table(realm->read_group().get_table(table.table_key));
            mut_subs.insert_or_assign(query_for_table);
        }
        auto subs = std::move(mut_subs).commit();
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    }

    const TestAppSession& m_test_app_session;
    constexpr static std::string_view c_object_schema_name = "TopLevel";
    constexpr static std::string_view c_id_col_name = "_id";
    constexpr static std::string_view c_str_col_name = "queryable_str_field";
};

std::unique_ptr<TestClientReset> make_baas_client_reset(const Realm::Config& local_config,
                                                        const Realm::Config& remote_config,
                                                        TestAppSession& test_app_session)
{
    return std::make_unique<BaasClientReset>(local_config, remote_config, test_app_session);
}

std::unique_ptr<TestClientReset> make_baas_flx_client_reset(const Realm::Config& local_config,
                                                            const Realm::Config& remote_config,
                                                            const TestAppSession& session)
{
    return std::make_unique<BaasFLXClientReset>(local_config, remote_config, session);
}

#endif // REALM_ENABLE_AUTH_TESTS

#endif // REALM_ENABLE_SYNC


TestClientReset::TestClientReset(const Realm::Config& local_config, const Realm::Config& remote_config)
    : m_local_config(local_config)
    , m_remote_config(remote_config)
{
}
TestClientReset::~TestClientReset()
{
    // make sure we didn't forget to call run()
    REALM_ASSERT(m_did_run || !(m_make_local_changes || m_make_remote_changes || m_on_post_local || m_on_post_reset));
}

TestClientReset* TestClientReset::setup(Callback&& on_setup)
{
    m_on_setup = std::move(on_setup);
    return this;
}
TestClientReset* TestClientReset::make_local_changes(Callback&& changes_local)
{
    m_make_local_changes = std::move(changes_local);
    return this;
}
TestClientReset* TestClientReset::make_remote_changes(Callback&& changes_remote)
{
    m_make_remote_changes = std::move(changes_remote);
    return this;
}
TestClientReset* TestClientReset::on_post_local_changes(Callback&& post_local)
{
    m_on_post_local = std::move(post_local);
    return this;
}
TestClientReset* TestClientReset::on_post_reset(Callback&& post_reset)
{
    m_on_post_reset = std::move(post_reset);
    return this;
}

void TestClientReset::set_pk_of_object_driving_reset(const ObjectId& pk)
{
    m_pk_driving_reset = pk;
}

ObjectId TestClientReset::get_pk_of_object_driving_reset() const
{
    return m_pk_driving_reset;
}

void TestClientReset::disable_wait_for_reset_completion()
{
    m_wait_for_reset_completion = false;
}

std::unique_ptr<TestClientReset> make_fake_local_client_reset(const Realm::Config& local_config,
                                                              const Realm::Config& remote_config)
{
    return std::make_unique<FakeLocalClientReset>(local_config, remote_config);
}

} // namespace reset_utils

} // namespace realm
