////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#pragma once

#include <realm/object-store/property.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>

#include <realm/string_data.hpp>

#include <realm/object-store/object_store.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_user.hpp>

#include "../sync/common_utils.hpp"

#include "external/json/json.hpp"
#include "external/mpark/variant.hpp"

class TestAppSession;

namespace realm {
struct AppSession;

}

using DeleteApp = realm::util::TaggedBool<struct DeleteAppTag>;
class TestAppSession {
public:
    TestAppSession();
    TestAppSession(realm::AppSession, std::shared_ptr<realm::app::GenericNetworkTransport> = nullptr,
                   DeleteApp = true);
    ~TestAppSession();

    std::shared_ptr<realm::app::App> app() const noexcept
    {
        return m_app;
    }
    const realm::AppSession& app_session() const noexcept
    {
        return *m_app_session;
    }
    realm::app::GenericNetworkTransport* transport()
    {
        return m_transport.get();
    }

private:
    std::shared_ptr<realm::app::App> m_app;
    std::unique_ptr<realm::AppSession> m_app_session;
    std::string m_base_file_path;
    bool m_delete_app = true;
    std::shared_ptr<realm::app::GenericNetworkTransport> m_transport;
};

namespace realm {

class AdminAPIEndpoint {
public:
    app::Response get(const std::vector<std::pair<std::string, std::string>>& params = {}) const;
    app::Response patch(std::string body) const;
    app::Response post(std::string body) const;
    app::Response put(std::string body) const;
    app::Response del() const;
    nlohmann::json get_json(const std::vector<std::pair<std::string, std::string>>& params = {}) const;
    nlohmann::json patch_json(nlohmann::json body) const;
    nlohmann::json post_json(nlohmann::json body) const;
    nlohmann::json put_json(nlohmann::json body) const;

    AdminAPIEndpoint operator[](StringData name) const;

protected:
    friend class AdminAPISession;
    AdminAPIEndpoint(std::string url, std::string access_token)
        : m_url(std::move(url))
        , m_access_token(std::move(access_token))
    {
    }

    app::Response do_request(app::Request request) const;

private:
    std::string m_url;
    std::string m_access_token;
};

class AdminAPISession {
public:
    static AdminAPISession login(const std::string& base_url, const std::string& username,
                                 const std::string& password);

    AdminAPIEndpoint apps() const;
    void revoke_user_sessions(const std::string& user_id, const std::string& app_id) const;
    void disable_user_sessions(const std::string& user_id, const std::string& app_id) const;
    void enable_user_sessions(const std::string& user_id, const std::string& app_id) const;
    bool verify_access_token(const std::string& access_token, const std::string& app_id) const;
    void set_development_mode_to(const std::string& app_id, bool enable) const;
    void delete_app(const std::string& app_id) const;

    struct Service {
        std::string id;
        std::string name;
        std::string type;
        int64_t version;
        int64_t last_modified;
    };
    struct ServiceConfig {
        enum class SyncMode { Partitioned, Flexible } mode = SyncMode::Partitioned;
        std::string database_name;
        util::Optional<nlohmann::json> partition;
        util::Optional<nlohmann::json> queryable_field_names;
        util::Optional<nlohmann::json> permissions;
        std::string state;
        bool recovery_is_disabled = false;
        std::string_view sync_service_name()
        {
            if (mode == SyncMode::Flexible) {
                return "flexible_sync";
            }
            else {
                return "sync";
            }
        }
    };
    std::vector<Service> get_services(const std::string& app_id) const;
    std::vector<std::string> get_errors(const std::string& app_id) const;
    Service get_sync_service(const std::string& app_id) const;
    ServiceConfig get_config(const std::string& app_id, const Service& service) const;
    ServiceConfig disable_sync(const std::string& app_id, const std::string& service_id,
                               ServiceConfig sync_config) const;
    ServiceConfig pause_sync(const std::string& app_id, const std::string& service_id,
                             ServiceConfig sync_config) const;
    ServiceConfig enable_sync(const std::string& app_id, const std::string& service_id,
                              ServiceConfig sync_config) const;
    ServiceConfig set_disable_recovery_to(const std::string& app_id, const std::string& service_id,
                                          ServiceConfig sync_config, bool disable) const;
    bool is_sync_enabled(const std::string& app_id) const;
    bool is_sync_terminated(const std::string& app_id) const;

    const std::string& base_url() const noexcept
    {
        return m_base_url;
    }

private:
    AdminAPISession(std::string base_url, std::string access_token, std::string group_id)
        : m_base_url(std::move(base_url))
        , m_access_token(std::move(access_token))
        , m_group_id(std::move(group_id))
    {
    }

    AdminAPIEndpoint service_config_endpoint(const std::string& app_id, const std::string& service_id) const;

    std::string m_base_url;
    std::string m_access_token;
    std::string m_group_id;
};

struct AppCreateConfig {
    struct FunctionDef {
        std::string name;
        std::string source;
        bool is_private;
    };

    struct UserPassAuthConfig {
        bool auto_confirm;
        std::string confirm_email_subject;
        std::string confirmation_function_name;
        std::string email_confirmation_url;
        std::string reset_function_name;
        std::string reset_password_subject;
        std::string reset_password_url;
        bool run_confirmation_function;
        bool run_reset_function;
    };

    struct FLXSyncRole {
        std::string name;
        nlohmann::json apply_when = nlohmann::json::object();
        mpark::variant<bool, nlohmann::json> read;
        mpark::variant<bool, nlohmann::json> write;
    };

    struct FLXSyncConfig {
        std::vector<std::string> queryable_fields;
        std::vector<FLXSyncRole> default_roles;
    };

    std::string app_name;
    std::string base_url;
    std::string admin_username;
    std::string admin_password;

    std::string mongo_uri;
    std::string mongo_dbname;

    Schema schema;
    Property partition_key;
    bool dev_mode_enabled;
    util::Optional<FLXSyncConfig> flx_sync_config;

    std::vector<FunctionDef> functions;

    util::Optional<UserPassAuthConfig> user_pass_auth;
    util::Optional<std::string> custom_function_auth;
    bool enable_api_key_auth = false;
    bool enable_anonymous_auth = false;
    bool enable_custom_token_auth = false;
};

AppCreateConfig default_app_config(const std::string& base_url);
AppCreateConfig minimal_app_config(const std::string& base_url, const std::string& name, const Schema& schema);

struct AppSession {
    std::string client_app_id;
    std::string server_app_id;
    AdminAPISession admin_api;
    AppCreateConfig config;
};

#if REALM_ENABLE_AUTH_TESTS

namespace reset_utils {

namespace {

TableRef get_table(Realm& realm, StringData object_type)
{
    return realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

} // anonymous namespace

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
    uint64_t count_external = 0;

    timed_sleeping_wait_for(
        [&]() -> bool {
            if (count_external == 0) {
                object_coll.count(filter_bson, [&](uint64_t count, util::Optional<app::AppError> error) {
                    REALM_ASSERT(!error);
                    count_external = count;
                });
            }
            if (count_external == 0) {
                millisleep(2000); // don't spam the server too much
            }
            return count_external > 0;
        },
        std::chrono::minutes(15));
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
        REALM_ASSERT(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        app_session.admin_api.disable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        timed_sleeping_wait_for([&] {
            return app_session.admin_api.is_sync_terminated(app_session.server_app_id);
        });
        app_session.admin_api.enable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        REALM_ASSERT(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        if (app_session.config.dev_mode_enabled) { // dev mode is not sticky across a reset
            app_session.admin_api.set_development_mode_to(app_session.server_app_id, true);
        }

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
            REALM_ASSERT(table->size() >= 1);
            auto obj = table->get_object_with_primary_key({m_pk_driving_reset});
            REALM_ASSERT(obj.is_valid());
            auto col = table->get_column_key("value");
            REALM_ASSERT(obj.get_any(col) == Mixed{3});

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
        REALM_ASSERT(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
        app_session.admin_api.disable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        timed_sleeping_wait_for([&] {
            return app_session.admin_api.is_sync_terminated(app_session.server_app_id);
        });
        app_session.admin_api.enable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
        REALM_ASSERT(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
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
            REALM_ASSERT(table->size() >= 1);
            auto obj = table->get_object_with_primary_key({pk_of_added_object});
            REALM_ASSERT(obj.is_valid());
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
                                                        TestAppSession& test_app_session);

std::unique_ptr<TestClientReset> make_baas_flx_client_reset(const Realm::Config& local_config,
                                                            const Realm::Config& remote_config,
                                                            const TestAppSession& test_app_session);

} // namespace reset_utils
#endif // REALM_ENABLE_AUTH_TESTS

app::Response do_http_request(app::Request&& request);

AppSession create_app(const AppCreateConfig& config);

class SynchronousTestTransport : public app::GenericNetworkTransport {
public:
    void send_request_to_server(app::Request&& request,
                                util::UniqueFunction<void(const app::Response&)>&& completion) override
    {
        {
            std::lock_guard barrier(m_mutex);
        }
        completion(do_http_request(std::move(request)));
    }

    void block()
    {
        m_mutex.lock();
    }
    void unblock()
    {
        m_mutex.unlock();
    }

private:
    std::mutex m_mutex;
};

// This will create a new test app in the baas server at base_url
// to be used in tests.
AppSession get_runtime_app_session(std::string base_url);

template <typename Factory>
inline app::App::Config get_config(Factory factory, const AppSession& app_session)
{
    return {app_session.client_app_id,
            factory,
            app_session.admin_api.base_url(),
            util::none,
            util::Optional<std::string>("A Local App Version"),
            util::none,
            "Object Store Platform Tests",
            "Object Store Platform Version Blah",
            "An sdk version"};
}

std::string get_base_url();

struct AutoVerifiedEmailCredentials : app::AppCredentials {
    AutoVerifiedEmailCredentials();
    std::string email;
    std::string password;
};

AutoVerifiedEmailCredentials create_user_and_log_in(app::SharedApp app);

} // namespace realm
