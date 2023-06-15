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

#include "util/baas_test_utils.hpp"

#include "util/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/hex_dump.hpp>
#include <realm/util/sha_crypto.hpp>

#if REALM_ENABLE_SYNC

#if REALM_ENABLE_AUTH_TESTS

namespace realm {

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

void wait_for_advance(Realm& realm)
{
    struct Context : BindingContext {
        Realm& realm;
        DB::version_type target_version;
        bool& done;
        Context(Realm& realm, bool& done)
            : realm(realm)
            , target_version(*realm.latest_snapshot_version())
            , done(done)
        {
        }

        void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
        {
            if (realm.read_transaction_version().version >= target_version) {
                done = true;
            }
        }
    };

    bool done = false;
    realm.m_binding_context = std::make_unique<Context>(realm, done);
    timed_wait_for([&] {
        return done;
    });
    realm.m_binding_context = nullptr;
}

namespace reset_utils {

void wait_for_object_to_persist_to_atlas(std::shared_ptr<SyncUser> user, const AppSession& app_session,
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
                    promise.set_error({ErrorCodes::RuntimeError, error->reason()});
                }
                else {
                    promise.emplace_value(count);
                }
            });
            return pf.future.get() > 0;
        },
        std::chrono::minutes(15), std::chrono::milliseconds(500));
}

void wait_for_num_objects_in_atlas(std::shared_ptr<SyncUser> user, const AppSession& app_session,
                                   const std::string& schema_name, size_t expected_size)
{
    app::MongoClient remote_client = user->mongo_client("BackingDB");
    app::MongoDatabase db = remote_client.db(app_session.config.mongo_dbname);
    app::MongoCollection object_coll = db[schema_name];

    const bson::BsonDocument& filter_bson{};
    timed_sleeping_wait_for(
        [&]() -> bool {
            auto pf = util::make_promise_future<uint64_t>();
            object_coll.count(filter_bson, [promise = std::move(pf.promise)](
                                               uint64_t count, util::Optional<app::AppError> error) mutable {
                REQUIRE(!error);
                if (error) {
                    promise.set_error({ErrorCodes::RuntimeError, error->reason()});
                }
                else {
                    promise.emplace_value(count);
                }
            });
            return pf.future.get() >= expected_size;
        },
        std::chrono::minutes(15), std::chrono::milliseconds(500));
}

void trigger_client_reset(const AppSession& app_session)
{
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

    // In FLX sync, the server won't let you connect until the initial sync is complete. With PBS tho, we need
    // to make sure we've actually copied all the data from atlas into the realm history before we do any of
    // our remote changes.
    if (!app_session.config.flx_sync_config) {
        timed_sleeping_wait_for([&] {
            return app_session.admin_api.is_initial_sync_complete(app_session.server_app_id);
        });
    }
}

void trigger_client_reset(const AppSession& app_session, const SharedRealm& realm)
{
    auto file_ident = SyncSession::OnlyForTesting::get_file_ident(*realm->sync_session());
    REQUIRE(file_ident.ident != 0);
    app_session.admin_api.trigger_client_reset(app_session.server_app_id, file_ident.ident);
}

namespace {

TableRef get_table(Realm& realm, StringData object_type)
{
    return realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

} // anonymous namespace

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

            session->pause();

            realm->begin_transaction();
            obj.set(col, 4);
            if (m_make_local_changes) {
                m_make_local_changes(realm);
            }
            realm->commit_transaction();
        }

        trigger_client_reset(app_session, realm);

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
        session->resume();
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
        if (m_on_setup) {
            m_on_setup(realm);
        }

        ObjectId pk_of_added_object = [&] {
            if (m_populate_initial_object) {
                return m_populate_initial_object(realm);
            }

            auto ret = ObjectId::gen();
            constexpr bool create_object = true;
            subscribe_to_object_by_id(realm, ret, create_object);
            return ret;
        }();

        session->pause();

        if (m_make_local_changes) {
            m_make_local_changes(realm);
        }

        trigger_client_reset(app_session, realm);

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
        session->resume();
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
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        if (create_object) {
            realm->begin_transaction();
            table->create_object_with_primary_key(pk, {{str_col, "initial value"}});
            realm->commit_transaction();
        }
        wait_for_upload(*realm);
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

} // namespace reset_utils

} // namespace realm

#endif // REALM_ENABLE_AUTH_TESTS

#endif // REALM_ENABLE_SYNC
