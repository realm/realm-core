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

void timed_wait_for(std::function<bool()> condition, std::chrono::milliseconds max_ms)
{
    const auto wait_start = std::chrono::steady_clock::now();
    util::EventLoop::main().run_until([&] {
        if (std::chrono::steady_clock::now() - wait_start > max_ms) {
            throw std::runtime_error(util::format("timed_wait_for exceeded %1 ms", max_ms.count()));
        }
        return condition();
    });
}

void timed_sleeping_wait_for(std::function<bool()> condition, std::chrono::milliseconds max_ms)
{
    const auto wait_start = std::chrono::steady_clock::now();
    while (!condition()) {
        if (std::chrono::steady_clock::now() - wait_start > max_ms) {
            throw std::runtime_error(util::format("timed_sleeping_wait_for exceeded %1 ms", max_ms.count()));
        }
        millisleep(1);
    }
}

auto do_hash = [](const std::string& name) -> std::string {
    std::array<unsigned char, 32> hash;
    util::sha256(name.data(), name.size(), hash.data());
    return util::hex_dump(hash.data(), hash.size(), "");
};

ExpectedRealmPaths::ExpectedRealmPaths(const std::string& base_path, const std::string& app_id,
                                       const std::string& identity, const std::string& local_identity,
                                       const std::string& partition, util::Optional<std::string> name)
{
    // This is copied from SyncManager.cpp string_from_partition() in order to prevent
    // us changing that function and therefore breaking user's existing paths unknowingly.
    std::string cleaned_partition = partition;
    try {
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
    }
    catch (...) {
        // if the partition is not a bson string then it was from old sync tests and is a server path.
    }
    std::string clean_name = name ? util::make_percent_encoded_string(*name) : cleaned_partition;
    std::string cleaned_app_id = util::make_percent_encoded_string(app_id);
    std::string manager_path = util::format("%1mongodb-realm/%2/", base_path, cleaned_app_id);
    std::string preferred_name = util::format("%1%2/%3", manager_path, identity, clean_name);
    current_preferred_path = util::format("%1.realm", preferred_name);
    fallback_hashed_path = util::format("%1%2.realm", manager_path, do_hash(preferred_name));

    legacy_sync_directories_to_make.push_back(util::format("%1%2", manager_path, local_identity));
    std::string encoded_partition = util::make_percent_encoded_string(partition);
    legacy_local_id_path = util::format("%1%2/%3.realm", manager_path, local_identity,
                                        name ? util::make_percent_encoded_string(*name) : encoded_partition);
    std::string dir_builder = util::format("%1realm-object-server", manager_path);
    legacy_sync_directories_to_make.push_back(dir_builder);
    dir_builder = util::format("%1/%2", dir_builder, local_identity);
    legacy_sync_directories_to_make.push_back(dir_builder);
    legacy_sync_path =
        util::format("%1/%2", dir_builder, name ? util::make_percent_encoded_string(*name) : cleaned_partition);
}

#if REALM_ENABLE_SYNC

void wait_for_sync_changes(std::shared_ptr<SyncSession> session)
{
    std::atomic<bool> called{false};
    session->wait_for_upload_completion([&](std::error_code err) {
        REQUIRE(err == std::error_code{});
        called.store(true);
    });
    REQUIRE_NOTHROW(timed_wait_for([&] {
        return called.load();
    }));
    REQUIRE(called);
    called.store(false);
    session->wait_for_download_completion([&](std::error_code err) {
        REQUIRE(err == std::error_code{});
        called.store(true);
    });
    REQUIRE_NOTHROW(timed_wait_for([&] {
        return called.load();
    }));
}

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
            CHECK(!error);
        });
    app->log_in_with_credentials(realm::app::AppCredentials::username_password(creds.email, creds.password),
                                 [&](std::shared_ptr<realm::SyncUser> user, util::Optional<app::AppError> error) {
                                     REQUIRE(user);
                                     CHECK(!error);
                                 });
    return creds;
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC

namespace reset_utils {

struct Partition {
    std::string property_name;
    std::string value;
};

TableRef get_table(Realm& realm, StringData object_type)
{
    return realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

Obj create_object(Realm& realm, StringData object_type, util::Optional<int64_t> primary_key = util::none,
                  util::Optional<Partition> partition = util::none)
{
    auto table = get_table(realm, object_type);
    REQUIRE(table);
    static int64_t pk = 0;
    FieldValues values = {};
    if (partition) {
        ColKey col = table->get_column_key(partition->property_name);
        REALM_ASSERT(col);
        values.push_back({col, Mixed{partition->value}});
    }
    return table->create_object_with_primary_key(primary_key ? *primary_key : pk++, std::move(values));
}

// fake discard local mode by turning off sync and calling transfer group directly
struct FakeLocalClientReset : public TestClientReset {
    FakeLocalClientReset(realm::Realm::Config local_config, realm::Realm::Config remote_config)
        : TestClientReset(local_config, remote_config)
    {
        REALM_ASSERT(m_local_config.sync_config);
        REALM_ASSERT(m_local_config.sync_config->client_resync_mode == ClientResyncMode::DiscardLocal);
        // turn off sync, we only fake it
        m_local_config.sync_config = {};
        m_remote_config.sync_config = {};
    }

    void run() override
    {
        m_did_run = true;
        auto realm = Realm::get_shared_realm(m_local_config);
        realm->begin_transaction();
        if (m_on_setup) {
            m_on_setup(realm);
        }
        realm->commit_transaction();
        constexpr int64_t shared_pk = -42;
        {
            realm->begin_transaction();
            auto obj = create_object(*realm, "object", shared_pk);
            auto col = obj.get_table()->get_column_key("value");
            obj.set(col, 1);
            obj.set(col, 2);
            obj.set(col, 3);
            realm->commit_transaction();

            realm->begin_transaction();
            obj.set(col, 4);
            if (m_make_local_changes) {
                m_make_local_changes(realm);
            }
            realm->commit_transaction();
            if (m_on_post_local) {
                m_on_post_local(realm);
            }
        }

        {
            auto realm2 = Realm::get_shared_realm(m_remote_config);
            realm2->begin_transaction();
            if (m_on_setup) {
                m_on_setup(realm2);
            }

            // fake a sync by creating an object with the same pk
            create_object(*realm2, "object", shared_pk);

            for (int i = 0; i < 2; ++i) {
                auto table = get_table(*realm2, "object");
                auto col = table->get_column_key("value");
                table->begin()->set(col, i + 5);
            }

            if (m_make_remote_changes) {
                m_make_remote_changes(realm2);
            }

            realm->begin_transaction();

            TestLogger logger;
            _impl::client_reset::transfer_group((Transaction&)realm2->read_group(), (Transaction&)realm->read_group(),
                                                logger);
            realm->commit_transaction();
            realm2->cancel_transaction();
            realm2->close();
            if (m_on_post_reset) {
                m_on_post_reset(realm);
            }
        }
    }
};

#if REALM_ENABLE_SYNC

struct TestServerClientReset : public TestClientReset {
    TestServerClientReset(realm::Realm::Config local_config, realm::Realm::Config remote_config,
                          TestSyncManager& test_sync_manager)
        : TestClientReset(local_config, remote_config)
        , m_test_sync_manager(test_sync_manager)
    {
    }

    void run() override
    {
        m_did_run = true;
        using namespace std::literals::chrono_literals;
        auto& server = m_test_sync_manager.sync_server();
        auto sync_manager = m_test_sync_manager.app()->sync_manager();
        constexpr int64_t pk = 0;

        auto realm = Realm::get_shared_realm(m_local_config);
        auto session = sync_manager->get_existing_session(realm->config().path);
        {
            realm->begin_transaction();

            if (m_on_setup) {
                m_on_setup(realm);
            }

            auto obj = create_object(*realm, "object", {pk});
            auto col = obj.get_table()->get_column_key("value");
            obj.set(col, 1);
            obj.set(col, 2);
            obj.set(col, 3);
            realm->commit_transaction();

            wait_for_upload(*realm);
            session->log_out();

            // Make a change while offline so that log compaction will cause a
            // client reset
            realm->begin_transaction();
            obj.set(col, 4);
            if (m_make_local_changes) {
                m_make_local_changes(realm);
            }
            realm->commit_transaction();
        }

        // Make writes from another client while advancing the time so that
        // the server performs log compaction
        {
            auto realm2 = Realm::get_shared_realm(m_remote_config);
            auto session2 = sync_manager->get_existing_session(realm2->config().path);

            for (int i = 0; i < 2; ++i) {
                wait_for_download(*realm2);
                realm2->begin_transaction();
                auto table = get_table(*realm2, "object");
                auto col = table->get_column_key("value");
                table->begin()->set(col, i + 5);
                if (i == 1 && m_make_remote_changes) {
                    m_make_remote_changes(realm2);
                }
                realm2->commit_transaction();
                wait_for_upload(*realm2);
                server.advance_clock(10s);
            }
            server.advance_clock(10s);
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
    TestSyncManager& m_test_sync_manager;
};

std::unique_ptr<TestClientReset> make_test_server_client_reset(Realm::Config local_config,
                                                               Realm::Config remote_config,
                                                               TestSyncManager& test_sync_manager)
{
    return std::make_unique<TestServerClientReset>(local_config, remote_config, test_sync_manager);
}

#if REALM_ENABLE_AUTH_TESTS

struct BaasClientReset : public TestClientReset {
    BaasClientReset(realm::Realm::Config local_config, realm::Realm::Config remote_config,
                    TestSyncManager& test_sync_manager)
        : TestClientReset(local_config, remote_config)
        , m_test_sync_manager(test_sync_manager)
    {
    }

    void run() override
    {
        m_did_run = true;
        AppSession* app_session = m_test_sync_manager.app_session();
        REALM_ASSERT(app_session);
        auto sync_manager = m_test_sync_manager.app()->sync_manager();
        std::string partition_value = m_local_config.sync_config->partition_value;
        REALM_ASSERT(partition_value.size() > 2 && *partition_value.begin() == '"' &&
                     *(partition_value.end() - 1) == '"');
        partition_value = partition_value.substr(1, partition_value.size() - 2);
        Partition partition = {app_session->config.partition_key.name, partition_value};

        auto realm = Realm::get_shared_realm(m_local_config);
        auto session = sync_manager->get_existing_session(realm->config().path);
        constexpr int64_t pk = 0;
        const std::string object_schema_name = "object";
        {
            wait_for_download(*realm);
            realm->begin_transaction();

            if (m_on_setup) {
                m_on_setup(realm);
            }

            auto obj = create_object(*realm, object_schema_name, {pk}, {partition});
            auto table = obj.get_table();
            auto col = table->get_column_key("value");
            std::string pk_col_name = table->get_column_name(table->get_primary_key_column());
            obj.set(col, 1);
            obj.set(col, 2);
            obj.set(col, 3);
            realm->commit_transaction();
            wait_for_upload(*realm);
            wait_for_download(*realm);

            // While at this point the object has been sync'd successfully, we must also
            // wait for it to appear in the backing database before terminating sync
            // otherwise the translator may be terminated before it has a chance to
            // integrate it into the backing database. If the server were to change
            // the meaning of "upload complete" to include writing to atlas then this would
            // not be necessary.
            app::MongoClient remote_client = m_local_config.sync_config->user->mongo_client("BackingDB");
            app::MongoDatabase db = remote_client.db(app_session->config.mongo_dbname);
            app::MongoCollection object_coll = db[object_schema_name];
            uint64_t count_external = 0;

            timed_sleeping_wait_for(
                [&]() -> bool {
                    if (count_external == 0) {
                        object_coll.count({{pk_col_name, pk}},
                                          [&](uint64_t count, util::Optional<app::AppError> error) {
                                              REQUIRE(!error);
                                              count_external = count;
                                          });
                    }
                    if (count_external == 0) {
                        millisleep(2000); // don't spam the server too much
                    }
                    return count_external > 0;
                },
                std::chrono::minutes(5));
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
        auto baas_sync_service = app_session->admin_api.get_sync_service(app_session->server_app_id);
        auto baas_sync_config = app_session->admin_api.get_config(app_session->server_app_id, baas_sync_service);
        REQUIRE(app_session->admin_api.is_sync_enabled(app_session->server_app_id));
        app_session->admin_api.disable_sync(app_session->server_app_id, baas_sync_service.id, baas_sync_config);
        REQUIRE(!app_session->admin_api.is_sync_enabled(app_session->server_app_id));
        app_session->admin_api.enable_sync(app_session->server_app_id, baas_sync_service.id, baas_sync_config);
        REQUIRE(app_session->admin_api.is_sync_enabled(app_session->server_app_id));
        if (app_session->config.dev_mode_enabled) { // dev mode is not sticky across a reset
            app_session->admin_api.set_development_mode_to(app_session->server_app_id, true);
        }

        {
            auto realm2 = Realm::get_shared_realm(m_remote_config);
            wait_for_download(*realm2);

            timed_sleeping_wait_for(
                [&]() -> bool {
                    realm2->begin_transaction();
                    auto table = get_table(*realm2, object_schema_name);
                    auto objkey = table->find_primary_key({pk});
                    realm2->cancel_transaction();
                    return bool(objkey);
                },
                std::chrono::seconds(60));

            // expect the last sync'd object to be in place
            realm2->begin_transaction();
            auto table = get_table(*realm2, object_schema_name);
            REQUIRE(table->size() >= 1);
            auto obj = table->get_object_with_primary_key({pk});
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
        wait_for_upload(*realm);
        if (m_on_post_reset) {
            m_on_post_reset(realm);
        }
    }

private:
    TestSyncManager& m_test_sync_manager;
};

std::unique_ptr<TestClientReset> make_baas_client_reset(Realm::Config local_config, Realm::Config remote_config,
                                                        TestSyncManager& test_sync_manager)
{
    return std::make_unique<BaasClientReset>(local_config, remote_config, test_sync_manager);
}

#endif // REALM_ENABLE_AUTH_TESTS

#endif // REALM_ENABLE_SYNC


TestClientReset::TestClientReset(realm::Realm::Config local_config, realm::Realm::Config remote_config)
    : m_local_config(local_config)
    , m_remote_config(remote_config)
{
}
TestClientReset::~TestClientReset()
{
    // make sure we didn't forget to call run()
    REALM_ASSERT(m_did_run || !(m_make_local_changes || m_make_remote_changes || m_on_post_local || m_on_post_reset));
}

TestClientReset* TestClientReset::setup(callback_t&& on_setup)
{
    m_on_setup = std::move(on_setup);
    return this;
}
TestClientReset* TestClientReset::make_local_changes(callback_t&& changes_local)
{
    m_make_local_changes = std::move(changes_local);
    return this;
}
TestClientReset* TestClientReset::make_remote_changes(callback_t&& changes_remote)
{
    m_make_remote_changes = std::move(changes_remote);
    return this;
}
TestClientReset* TestClientReset::on_post_local_changes(callback_t&& post_local)
{
    m_on_post_local = std::move(post_local);
    return this;
}
TestClientReset* TestClientReset::on_post_reset(callback_t&& post_reset)
{
    m_on_post_reset = std::move(post_reset);
    return this;
}

std::unique_ptr<TestClientReset> make_fake_local_client_reset(Realm::Config local_config, Realm::Config remote_config)
{
    return std::make_unique<FakeLocalClientReset>(local_config, remote_config);
}

} // namespace reset_utils

} // namespace realm
