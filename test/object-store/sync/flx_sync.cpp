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

#if REALM_ENABLE_AUTH_TESTS

#include <catch2/catch_all.hpp>

#include <util/test_file.hpp>
#include <util/crypt_key.hpp>
#include <util/sync/flx_sync_harness.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object_id.hpp>
#include <realm/query_expression.hpp>

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/util/bson/bson.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/sync/client_base.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/sync/noinst/pending_bootstrap_store.hpp>
#include <realm/sync/noinst/server/access_token.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/subscriptions.hpp>

#include <realm/util/future.hpp>
#include <realm/util/logger.hpp>

#include <filesystem>
#include <iostream>
#include <stdexcept>

using namespace std::string_literals;

namespace realm {

class TestHelper {
public:
    static DBRef& get_db(SharedRealm const& shared_realm)
    {
        return Realm::Internal::get_db(*shared_realm);
    }
};

} // namespace realm

namespace realm::app {

namespace {
const Schema g_minimal_schema{
    {"TopLevel",
     {
         {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
     }},
};

const Schema g_large_array_schema{
    ObjectSchema("TopLevel",
                 {
                     {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                     {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
                     {"list_of_strings", PropertyType::Array | PropertyType::String},
                 }),
};

const Schema g_simple_embedded_obj_schema{
    {"TopLevel",
     {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
      {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
      {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "TopLevel_embedded_obj"}}},
    {"TopLevel_embedded_obj",
     ObjectSchema::ObjectType::Embedded,
     {
         {"str_field", PropertyType::String | PropertyType::Nullable},
     }},
};

// Populates a FLXSyncTestHarness with the g_large_array_schema with objects that are large enough that
// they are guaranteed to fill multiple bootstrap download messages. Currently this means generating 5
// objects each with 1024 array entries of 1024 bytes each.
//
// Returns a list of the _id values for the objects created.
std::vector<ObjectId> fill_large_array_schema(FLXSyncTestHarness& harness)
{
    std::vector<ObjectId> ret;
    REQUIRE(harness.schema() == g_large_array_schema);
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        for (int i = 0; i < 5; ++i) {
            auto id = ObjectId::gen();
            auto obj = Object::create(c, realm, "TopLevel",
                                      std::any(AnyDict{{"_id", id},
                                                       {"list_of_strings", AnyVector{}},
                                                       {"queryable_int_field", static_cast<int64_t>(i * 5)}}));
            List str_list(obj, realm->schema().find("TopLevel")->property_for_name("list_of_strings"));
            for (int j = 0; j < 1024; ++j) {
                str_list.add(c, std::any(std::string(1024, 'a' + (j % 26))));
            }

            ret.push_back(id);
        }
    });
    return ret;
}

} // namespace

TEST_CASE("flx: connect to FLX-enabled app", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("basic_flx_connect");

    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s}}));
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", bar_obj_id},
                                        {"queryable_str_field", "bar"s},
                                        {"queryable_int_field", static_cast<int64_t>(10)},
                                        {"non_queryable_field", "non queryable 2"s}}));
    });


    harness.do_with_new_realm([&](SharedRealm realm) {
        {
            auto empty_subs = realm->get_latest_subscription_set();
            CHECK(empty_subs.size() == 0);
            CHECK(empty_subs.version() == 0);
            empty_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        auto table = realm->read_group().get_table("class_TopLevel");
        auto col_key = table->get_column_key("queryable_str_field");
        Query query_foo(table);
        query_foo.equal(col_key, "foo");
        {
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(query_foo);
            auto subs = new_subs.commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(*realm);
            Results results(realm, table);
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == foo_obj_id);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            Query new_query_bar(table);
            new_query_bar.equal(col_key, "bar");
            mut_subs.insert_or_assign(new_query_bar);
            auto subs = mut_subs.commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(*realm);
            Results results(realm, Query(table));
            CHECK(results.size() == 2);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            CHECK(mut_subs.erase(query_foo));
            Query new_query_bar(table);
            new_query_bar.equal(col_key, "bar");
            mut_subs.insert_or_assign(new_query_bar);
            auto subs = mut_subs.commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(*realm);
            Results results(realm, Query(table));
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == bar_obj_id);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            auto subs = mut_subs.commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(*realm);
            Results results(realm, table);
            CHECK(results.size() == 0);
        }
    });
}

TEST_CASE("flx: test commands work", "[sync][flx][test command][baas]") {
    FLXSyncTestHarness harness("test_commands");
    harness.do_with_new_realm([&](const SharedRealm& realm) {
        wait_for_upload(*realm);
        nlohmann::json command_request = {
            {"command", "PAUSE_ROUTER_SESSION"},
        };
        auto resp_body =
            SyncSession::OnlyForTesting::send_test_command(*realm->sync_session(), command_request.dump()).get();
        REQUIRE(resp_body == "{}");

        auto bad_status =
            SyncSession::OnlyForTesting::send_test_command(*realm->sync_session(), "foobar: }").get_no_throw();
        REQUIRE(bad_status.get_status() == ErrorCodes::LogicError);
        REQUIRE_THAT(bad_status.get_status().reason(),
                     Catch::Matchers::ContainsSubstring("Invalid json input to send_test_command"));

        bad_status =
            SyncSession::OnlyForTesting::send_test_command(*realm->sync_session(), "{\"cmd\": \"\"}").get_no_throw();
        REQUIRE_FALSE(bad_status.is_ok());
        REQUIRE(bad_status.get_status() == ErrorCodes::LogicError);
        REQUIRE(bad_status.get_status().reason() ==
                "Must supply command name in \"command\" field of test command json object");
    });
}

static auto make_error_handler()
{
    auto [error_promise, error_future] = util::make_promise_future<SyncError>();
    auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
    auto fn = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>, SyncError err) {
        error_promise->emplace_value(std::move(err));
    };
    return std::make_pair(std::move(error_future), std::move(fn));
}

TEST_CASE("app: error handling integration test", "[sync][flx][baas]") {
    static std::optional<FLXSyncTestHarness> harness{"error_handling"};
    create_user_and_log_in(harness->app());
    SyncTestFile config(harness->app()->current_user(), harness->schema(), SyncConfig::FLXSyncEnabled{});
    auto&& [error_future, error_handler] = make_error_handler();
    config.sync_config->error_handler = std::move(error_handler);
    config.sync_config->client_resync_mode = ClientResyncMode::Manual;

    SECTION("Resuming while waiting for session to auto-resume") {
        enum class TestState { InitialSuspend, InitialResume, SecondBind, SecondSuspend, SecondResume, Done };
        TestingStateMachine<TestState> state(TestState::InitialSuspend);
        config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession>,
                                                            const SyncClientHookData& data) {
            std::optional<TestState> wait_for;
            auto event = data.event;
            state.transition_with([&](TestState state) -> std::optional<TestState> {
                if (state == TestState::InitialSuspend && event == SyncClientHookEvent::SessionSuspended) {
                    // If we're getting suspended for the first time, notify the test thread that we're
                    // ready to be resumed.
                    wait_for = TestState::SecondBind;
                    return TestState::InitialResume;
                }
                else if (state == TestState::SecondBind && data.event == SyncClientHookEvent::BindMessageSent) {
                    return TestState::SecondSuspend;
                }
                else if (state == TestState::SecondSuspend && event == SyncClientHookEvent::SessionSuspended) {
                    wait_for = TestState::Done;
                    return TestState::SecondResume;
                }
                return std::nullopt;
            });
            if (wait_for) {
                state.wait_for(*wait_for);
            }
            return SyncClientHookAction::NoAction;
        };
        auto r = Realm::get_shared_realm(config);
        wait_for_upload(*r);
        nlohmann::json error_body = {
            {"tryAgain", true},           {"message", "fake error"},
            {"shouldClientReset", false}, {"isRecoveryModeDisabled", false},
            {"action", "Transient"},      {"backoffIntervalSec", 900},
            {"backoffMaxDelaySec", 900},  {"backoffMultiplier", 1},
        };
        nlohmann::json test_command = {{"command", "ECHO_ERROR"},
                                       {"args", nlohmann::json{{"errorCode", 229}, {"errorBody", error_body}}}};

        // First we trigger a retryable transient error that should cause the client to try to resume the
        // session in 5 minutes.
        auto test_cmd_res =
            wait_for_future(SyncSession::OnlyForTesting::send_test_command(*r->sync_session(), test_command.dump()))
                .get();
        REQUIRE(test_cmd_res == "{}");

        // Wait for the
        state.wait_for(TestState::InitialResume);

        // Once we're suspended, immediately tell the sync client to resume the session. This should cancel the
        // timer that would have auto-resumed the session.
        r->sync_session()->handle_reconnect();
        state.transition_with([&](TestState cur_state) {
            REQUIRE(cur_state == TestState::InitialResume);
            return TestState::SecondBind;
        });
        state.wait_for(TestState::SecondSuspend);

        // Once we're connected again trigger another retryable transient error. Before RCORE-1770 the timer
        // to auto-resume the session would have still been active here and we would crash when trying to start
        // a second timer to auto-resume after this error.
        test_cmd_res =
            wait_for_future(SyncSession::OnlyForTesting::send_test_command(*r->sync_session(), test_command.dump()))
                .get();
        REQUIRE(test_cmd_res == "{}");
        state.wait_for(TestState::SecondResume);

        // Finally resume the session again which should cancel the second timer and the session should auto-resume
        // normally without crashing.
        r->sync_session()->handle_reconnect();
        state.transition_with([&](TestState cur_state) {
            REQUIRE(cur_state == TestState::SecondResume);
            return TestState::Done;
        });
        wait_for_download(*r);
    }

    SECTION("handles unknown errors gracefully") {
        auto r = Realm::get_shared_realm(config);
        wait_for_download(*r);
        nlohmann::json error_body = {
            {"tryAgain", false},         {"message", "fake error"},
            {"shouldClientReset", true}, {"isRecoveryModeDisabled", false},
            {"action", "ClientReset"},
        };
        nlohmann::json test_command = {{"command", "ECHO_ERROR"},
                                       {"args", nlohmann::json{{"errorCode", 299}, {"errorBody", error_body}}}};
        auto test_cmd_res =
            wait_for_future(SyncSession::OnlyForTesting::send_test_command(*r->sync_session(), test_command.dump()))
                .get();
        REQUIRE(test_cmd_res == "{}");
        auto error = wait_for_future(std::move(error_future)).get();
        REQUIRE(error.status == ErrorCodes::UnknownError);
        REQUIRE(error.server_requests_action == sync::ProtocolErrorInfo::Action::ClientReset);
        REQUIRE(error.is_fatal);
        REQUIRE_THAT(error.status.reason(),
                     Catch::Matchers::ContainsSubstring("Unknown sync protocol error code 299"));
        REQUIRE_THAT(error.status.reason(), Catch::Matchers::ContainsSubstring("fake error"));
    }

    SECTION("unknown errors without actions are application bugs") {
        auto r = Realm::get_shared_realm(config);
        wait_for_download(*r);
        nlohmann::json error_body = {
            {"tryAgain", false},
            {"message", "fake error"},
            {"shouldClientReset", false},
            {"isRecoveryModeDisabled", false},
        };
        nlohmann::json test_command = {{"command", "ECHO_ERROR"},
                                       {"args", nlohmann::json{{"errorCode", 299}, {"errorBody", error_body}}}};
        auto test_cmd_res =
            wait_for_future(SyncSession::OnlyForTesting::send_test_command(*r->sync_session(), test_command.dump()))
                .get();
        REQUIRE(test_cmd_res == "{}");
        auto error = wait_for_future(std::move(error_future)).get();
        REQUIRE(error.status == ErrorCodes::UnknownError);
        REQUIRE(error.server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
        REQUIRE(error.is_fatal);
        REQUIRE_THAT(error.status.reason(),
                     Catch::Matchers::ContainsSubstring("Unknown sync protocol error code 299"));
        REQUIRE_THAT(error.status.reason(), Catch::Matchers::ContainsSubstring("fake error"));
    }

    SECTION("handles unknown actions gracefully") {
        auto r = Realm::get_shared_realm(config);
        wait_for_download(*r);
        nlohmann::json error_body = {
            {"tryAgain", false},
            {"message", "fake error"},
            {"shouldClientReset", true},
            {"isRecoveryModeDisabled", false},
            {"action", "FakeActionThatWillNeverExist"},
        };
        nlohmann::json test_command = {{"command", "ECHO_ERROR"},
                                       {"args", nlohmann::json{{"errorCode", 201}, {"errorBody", error_body}}}};
        auto test_cmd_res =
            wait_for_future(SyncSession::OnlyForTesting::send_test_command(*r->sync_session(), test_command.dump()))
                .get();
        REQUIRE(test_cmd_res == "{}");
        auto error = wait_for_future(std::move(error_future)).get();
        REQUIRE(error.status == ErrorCodes::RuntimeError);
        REQUIRE(error.server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
        REQUIRE(error.is_fatal);
        REQUIRE_THAT(error.status.reason(), !Catch::Matchers::ContainsSubstring("Unknown sync protocol error code"));
        REQUIRE_THAT(error.status.reason(), Catch::Matchers::ContainsSubstring("fake error"));
    }


    SECTION("unknown connection-level errors are still errors") {
        auto r = Realm::get_shared_realm(config);
        wait_for_download(*r);
        nlohmann::json error_body = {{"tryAgain", false},
                                     {"message", "fake error"},
                                     {"shouldClientReset", false},
                                     {"isRecoveryModeDisabled", false},
                                     {"action", "ApplicationBug"}};
        nlohmann::json test_command = {{"command", "ECHO_ERROR"},
                                       {"args", nlohmann::json{{"errorCode", 199}, {"errorBody", error_body}}}};
        auto test_cmd_res =
            wait_for_future(SyncSession::OnlyForTesting::send_test_command(*r->sync_session(), test_command.dump()))
                .get();
        REQUIRE(test_cmd_res == "{}");
        auto error = wait_for_future(std::move(error_future)).get();
        REQUIRE(error.status == ErrorCodes::SyncProtocolInvariantFailed);
        REQUIRE(error.server_requests_action == sync::ProtocolErrorInfo::Action::ProtocolViolation);
        REQUIRE(error.is_fatal);
    }

    SECTION("client reset errors") {
        auto r = Realm::get_shared_realm(config);
        wait_for_download(*r);
        nlohmann::json error_body = {{"tryAgain", false},
                                     {"message", "fake error"},
                                     {"shouldClientReset", true},
                                     {"isRecoveryModeDisabled", false},
                                     {"action", "ClientReset"}};
        auto code = GENERATE(sync::ProtocolError::bad_client_file_ident, sync::ProtocolError::bad_server_version,
                             sync::ProtocolError::diverging_histories);
        nlohmann::json test_command = {{"command", "ECHO_ERROR"},
                                       {"args", nlohmann::json{{"errorCode", code}, {"errorBody", error_body}}}};
        auto test_cmd_res =
            wait_for_future(SyncSession::OnlyForTesting::send_test_command(*r->sync_session(), test_command.dump()))
                .get();
        REQUIRE(test_cmd_res == "{}");
        auto error = wait_for_future(std::move(error_future)).get();
        REQUIRE(error.status == ErrorCodes::SyncClientResetRequired);
        REQUIRE(error.server_requests_action == sync::ProtocolErrorInfo::Action::ClientReset);
        REQUIRE(error.is_client_reset_requested());
        REQUIRE(error.is_fatal);
    }


    SECTION("teardown") {
        harness.reset();
    }
}


TEST_CASE("flx: client reset", "[sync][flx][client reset][baas]") {
    std::vector<ObjectSchema> schema{
        {"TopLevel",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
             {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
             {"non_queryable_field", PropertyType::String | PropertyType::Nullable},
             {"list_of_ints_field", PropertyType::Int | PropertyType::Array},
             {"sum_of_list_field", PropertyType::Int},
             {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
             {"dictionary_mixed", PropertyType::Dictionary | PropertyType::Mixed | PropertyType::Nullable},
         }},
        {"TopLevel2",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
         }},
    };

    // some of these tests make additive schema changes which is only allowed in dev mode
    constexpr bool dev_mode = true;
    FLXSyncTestHarness harness("flx_client_reset",
                               {schema, {"queryable_str_field", "queryable_int_field"}, {}, dev_mode});

    auto add_object = [](SharedRealm realm, std::string str_field, int64_t int_field,
                         ObjectId oid = ObjectId::gen()) {
        CppContext c(realm);
        realm->begin_transaction();

        int64_t r1 = random_int();
        int64_t r2 = random_int();
        int64_t r3 = random_int();
        int64_t sum = uint64_t(r1) + r2 + r3;

        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", oid},
                                        {"queryable_str_field", str_field},
                                        {"queryable_int_field", int_field},
                                        {"non_queryable_field", "non queryable 1"s},
                                        {"list_of_ints_field", std::vector<std::any>{r1, r2, r3}},
                                        {"sum_of_list_field", sum}}));
        realm->commit_transaction();
    };

    auto subscribe_to_and_add_objects = [&](SharedRealm realm, size_t num_objects) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto id_col = table->get_primary_key_column();
        auto sub_set = realm->get_latest_subscription_set();
        for (size_t i = 0; i < num_objects; ++i) {
            auto oid = ObjectId::gen();
            auto mut_sub = sub_set.make_mutable_copy();
            mut_sub.clear();
            mut_sub.insert_or_assign(Query(table).equal(id_col, oid));
            sub_set = mut_sub.commit();
            add_object(realm, util::format("added _id='%1'", oid), 0, oid);
        }
    };

    auto add_subscription_for_new_object = [&](SharedRealm realm, std::string str_field,
                                               int64_t int_field) -> sync::SubscriptionSet {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto sub_set = realm->get_latest_subscription_set().make_mutable_copy();
        sub_set.insert_or_assign(Query(table).equal(queryable_str_field, StringData(str_field)));
        auto resulting_set = sub_set.commit();
        add_object(realm, str_field, int_field);
        return resulting_set;
    };

    auto add_invalid_subscription = [](SharedRealm realm) -> sync::SubscriptionSet {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("non_queryable_field");
        auto sub_set = realm->get_latest_subscription_set().make_mutable_copy();
        sub_set.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
        auto resulting_set = sub_set.commit();
        return resulting_set;
    };

    auto count_queries_with_str = [](sync::SubscriptionSet subs, std::string_view str) {
        size_t count = 0;
        for (auto sub : subs) {
            if (sub.query_string.find(str) != std::string::npos) {
                ++count;
            }
        }
        return count;
    };
    create_user_and_log_in(harness.app());
    auto user1 = harness.app()->current_user();
    create_user_and_log_in(harness.app());
    auto user2 = harness.app()->current_user();
    SyncTestFile config_local(user1, harness.schema(), SyncConfig::FLXSyncEnabled{});
    config_local.path += ".local";
    SyncTestFile config_remote(user2, harness.schema(), SyncConfig::FLXSyncEnabled{});
    config_remote.path += ".remote";
    const std::string str_field_value = "foo";
    const int64_t local_added_int = 100;
    const int64_t local_added_int2 = 150;
    const int64_t remote_added_int = 200;
    size_t before_reset_count = 0;
    size_t after_reset_count = 0;
    config_local.sync_config->notify_before_client_reset = [&before_reset_count](SharedRealm) {
        ++before_reset_count;
    };
    config_local.sync_config->notify_after_client_reset = [&after_reset_count](SharedRealm, ThreadSafeReference,
                                                                               bool) {
        ++after_reset_count;
    };

    config_local.sync_config->on_sync_client_event_hook = [](std::weak_ptr<SyncSession> weak_session,
                                                             const SyncClientHookData& data) {
        // To prevent the upload cursors from becoming out of sync when the local realm assumes
        // the client file ident of the fresh realm, UPLOAD messages are not allowed during the
        // fresh realm download so the server's upload cursor versions start at 0 when the
        // local realm resumes after the client reset.
        if (data.event == SyncClientHookEvent::UploadMessageSent) {
            // If this is an UPLOAD message event, check to see if the fresh realm is being downloaded
            if (auto session = weak_session.lock()) {
                // Check for a "fresh" path to determine if this is a client reset fresh download session
                if (_impl::client_reset::is_fresh_path(session->path())) {
                    FAIL("UPLOAD messages are not allowed during client reset fresh realm download");
                }
            }
        }
        return SyncClientHookAction::NoAction;
    };

    SECTION("Recover: offline writes and subscription (single subscription)") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->populate_initial_object([&](SharedRealm realm) {
                auto pk_of_added_object = ObjectId::gen();
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table(ObjectStore::table_name_for_object_type("TopLevel"));
                REALM_ASSERT(table);
                mut_subs.insert_or_assign(Query(table));
                mut_subs.commit();

                realm->begin_transaction();
                CppContext c(realm);
                int64_t r1 = random_int();
                int64_t r2 = random_int();
                int64_t r3 = random_int();
                int64_t sum = uint64_t(r1) + r2 + r3;

                Object::create(c, realm, "TopLevel",
                               std::any(AnyDict{{"_id"s, pk_of_added_object},
                                                {"queryable_str_field"s, "initial value"s},
                                                {"list_of_ints_field", std::vector<std::any>{r1, r2, r3}},
                                                {"sum_of_list_field", sum}}));

                realm->commit_transaction();
                wait_for_upload(*realm);
                return pk_of_added_object;
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                add_object(local_realm, str_field_value, local_added_int);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                wait_for_advance(*local_realm);
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::Recover);
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto str_col = table->get_column_key("queryable_str_field");
                auto int_col = table->get_column_key("queryable_int_field");
                auto tv = table->where().equal(str_col, StringData(str_field_value)).find_all();
                tv.sort(int_col);
                // the object we created while offline was recovered, and the remote object was downloaded
                REQUIRE(tv.size() == 2);
                CHECK(tv.get_object(0).get<Int>(int_col) == local_added_int);
                CHECK(tv.get_object(1).get<Int>(int_col) == remote_added_int);
            })
            ->run();
    }

    SECTION("Recover: subscription and offline writes after client reset failure") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [error_future, error_handler] = make_error_handler();
        config_local.sync_config->error_handler = error_handler;

        std::string fresh_path = realm::_impl::client_reset::get_fresh_path_for(config_local.path);
        // create a non-empty directory that we'll fail to delete
        util::make_dir(fresh_path);
        util::File(util::File::resolve("file", fresh_path), util::File::mode_Write);

        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                auto mut_sub = local_realm->get_latest_subscription_set().make_mutable_copy();
                auto table = local_realm->read_group().get_table("class_TopLevel2");
                mut_sub.insert_or_assign(Query(table));
                mut_sub.commit();

                CppContext c(local_realm);
                local_realm->begin_transaction();
                Object::create(c, local_realm, "TopLevel2",
                               std::any(AnyDict{{"_id"s, ObjectId::gen()}, {"queryable_str_field"s, "foo"s}}));
                local_realm->commit_transaction();
            })
            ->on_post_reset([](SharedRealm local_realm) {
                // Verify offline subscription was not removed.
                auto subs = local_realm->get_latest_subscription_set();
                auto table = local_realm->read_group().get_table("class_TopLevel2");
                REQUIRE(subs.find(Query(table)));
            })
            ->run();

        // Remove the folder preventing the completion of a client reset.
        util::try_remove_dir_recursive(fresh_path);

        RealmConfig config_copy = config_local;
        config_copy.sync_config = std::make_shared<SyncConfig>(*config_copy.sync_config);
        config_copy.sync_config->error_handler = nullptr;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_copy.sync_config->notify_after_client_reset = reset_handler;

        // Attempt to open the realm again.
        // This time the client reset succeeds and the offline subscription and writes are recovered.
        auto realm = Realm::get_shared_realm(config_copy);
        ClientResyncMode mode = reset_future.get();
        REQUIRE(mode == ClientResyncMode::Recover);

        auto table = realm->read_group().get_table("class_TopLevel2");
        auto str_col = table->get_column_key("queryable_str_field");
        REQUIRE(table->size() == 1);
        REQUIRE(table->get_object(0).get<String>(str_col) == "foo");
    }

    SECTION("Recover: offline writes and subscriptions (multiple subscriptions)") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::Recover);
                auto subs = local_realm->get_latest_subscription_set();
                subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                subs.refresh();
                // make sure that the subscription for "foo" survived the reset
                size_t count_of_foo = count_queries_with_str(subs, util::format("\"%1\"", str_field_value));
                REQUIRE(subs.state() == sync::SubscriptionSet::State::Complete);
                REQUIRE(count_of_foo == 1);
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto str_col = table->get_column_key("queryable_str_field");
                auto int_col = table->get_column_key("queryable_int_field");
                auto tv = table->where().equal(str_col, StringData(str_field_value)).find_all();
                tv.sort(int_col);
                // the object we created while offline was recovered, and the remote object was downloaded
                REQUIRE(tv.size() == 2);
                CHECK(tv.get_object(0).get<Int>(int_col) == local_added_int);
                CHECK(tv.get_object(1).get<Int>(int_col) == remote_added_int);
            })
            ->run();
    }

    SECTION("Recover: offline writes interleaved with subscriptions and empty writes") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                // The sequence of events bellow generates five changesets:
                //  1. create sub1 => empty changeset
                //  2. create local_added_int object
                //  3. create empty changeset
                //  4. create sub2 => empty changeset
                //  5. create local_added_int2 object
                //
                // Before sending 'sub2' to the server, an UPLOAD message is sent first.
                // The upload message contains changeset 2. (local_added_int) with the cursor
                // of changeset 3. (empty changeset).

                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
                // Commit empty changeset.
                local_realm->begin_transaction();
                local_realm->commit_transaction();
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int2);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::Recover);
                auto subs = local_realm->get_latest_subscription_set();
                subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                // make sure that the subscription for "foo" survived the reset
                size_t count_of_foo = count_queries_with_str(subs, util::format("\"%1\"", str_field_value));
                subs.refresh();
                REQUIRE(subs.state() == sync::SubscriptionSet::State::Complete);
                REQUIRE(count_of_foo == 1);
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto str_col = table->get_column_key("queryable_str_field");
                auto int_col = table->get_column_key("queryable_int_field");
                auto tv = table->where().equal(str_col, StringData(str_field_value)).find_all();
                tv.sort(int_col);
                // the objects we created while offline was recovered, and the remote object was downloaded
                REQUIRE(tv.size() == 3);
                CHECK(tv.get_object(0).get<Int>(int_col) == local_added_int);
                CHECK(tv.get_object(1).get<Int>(int_col) == local_added_int2);
                CHECK(tv.get_object(2).get<Int>(int_col) == remote_added_int);
            })
            ->run();
    }

    auto validate_integrity_of_arrays = [](TableRef table) -> size_t {
        auto sum_col = table->get_column_key("sum_of_list_field");
        auto array_col = table->get_column_key("list_of_ints_field");
        auto query = table->column<Lst<Int>>(array_col).sum() == table->column<Int>(sum_col) &&
                     table->column<Lst<Int>>(array_col).size() > 0;
        return query.count();
    };

    SECTION("Recover: offline writes with associated subscriptions in the correct order") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        constexpr size_t num_objects_added = 20;
        constexpr size_t num_objects_added_by_harness = 1; // BaasFLXClientReset.run()
        constexpr size_t num_objects_added_by_remote = 1;  // make_remote_changes()
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                subscribe_to_and_add_objects(local_realm, num_objects_added);
                auto table = local_realm->read_group().get_table("class_TopLevel");
                REQUIRE(table->size() == num_objects_added + num_objects_added_by_harness);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::Recover);
                local_realm->refresh();
                auto latest_subs = local_realm->get_latest_subscription_set();
                auto state = latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                REQUIRE(state == sync::SubscriptionSet::State::Complete);
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                if (table->size() != 1) {
                    table->to_json(std::cout);
                }
                REQUIRE(table->size() == 1);
                auto mut_sub = latest_subs.make_mutable_copy();
                mut_sub.clear();
                mut_sub.insert_or_assign(Query(table));
                latest_subs = mut_sub.commit();
                latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                local_realm->refresh();
                REQUIRE(table->size() ==
                        num_objects_added + num_objects_added_by_harness + num_objects_added_by_remote);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added + num_objects_added_by_remote);
            })
            ->run();
    }

    SECTION("Recover: incompatible property changes are rejected") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        constexpr size_t num_objects_added_before = 2;
        constexpr size_t num_objects_added_after = 2;
        constexpr size_t num_objects_added_by_harness = 1; // BaasFLXClientReset.run()
        constexpr std::string_view added_property_name = "new_property";
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                subscribe_to_and_add_objects(local_realm, num_objects_added_before);
                Schema local_update = schema;
                Schema::iterator it = local_update.find("TopLevel");
                REQUIRE(it != local_update.end());
                it->persisted_properties.push_back(
                    {std::string(added_property_name), PropertyType::Float | PropertyType::Nullable});
                local_realm->update_schema(local_update);
                subscribe_to_and_add_objects(local_realm, num_objects_added_after);
                auto table = local_realm->read_group().get_table("class_TopLevel");
                REQUIRE(table->size() ==
                        num_objects_added_before + num_objects_added_after + num_objects_added_by_harness);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added_before + num_objects_added_after);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                Schema remote_update = schema;
                Schema::iterator it = remote_update.find("TopLevel");
                REQUIRE(it != remote_update.end());
                it->persisted_properties.push_back(
                    {std::string(added_property_name), PropertyType::UUID | PropertyType::Nullable});
                remote_realm->update_schema(remote_update);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, err_future = std::move(error_future)](SharedRealm local_realm) mutable {
                auto sync_error = wait_for_future(std::move(err_future)).get();
                REQUIRE(before_reset_count == 1);
                REQUIRE(after_reset_count == 0);
                REQUIRE(sync_error.status == ErrorCodes::AutoClientResetFailed);
                REQUIRE(sync_error.status.reason().find(
                            "SyncClientResetRequired: Bad client file identifier (IDENT)") != std::string::npos);
                REQUIRE(sync_error.is_client_reset_requested());
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                // since schema validation happens in the first recovery commit, that whole commit is rolled back
                // and the final state here is "pre reset"
                REQUIRE(table->size() ==
                        num_objects_added_before + num_objects_added_by_harness + num_objects_added_after);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added_before + num_objects_added_after);
            })
            ->run();
    }

    SECTION("unsuccessful replay of local changes") {
        constexpr size_t num_objects_added_before = 2;
        constexpr size_t num_objects_added_after = 2;
        constexpr size_t num_objects_added_by_harness = 1; // BaasFLXClientReset.run()
        constexpr std::string_view added_property_name = "new_property";
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;

        // The local changes here are a bit contrived because removing a column is disallowed
        // at the object store layer for sync'd Realms. The only reason a recovery should fail in production
        // during the apply stage is due to programmer error or external factors such as out of disk space.
        // Any schema discrepancies are caught by the initial diff, so the way to make a recovery fail here is
        // to add and remove a column at the core level such that the schema diff passes, but instructions are
        // generated which will fail when applied.
        auto make_local_changes_that_will_fail = [&](SharedRealm local_realm) {
            subscribe_to_and_add_objects(local_realm, num_objects_added_before);
            auto table = local_realm->read_group().get_table("class_TopLevel");
            REQUIRE(table->size() == num_objects_added_before + num_objects_added_by_harness);
            size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
            REQUIRE(count_of_valid_array_data == num_objects_added_before);
            local_realm->begin_transaction();
            ColKey added = table->add_column(type_Int, added_property_name);
            table->remove_column(added);
            local_realm->commit_transaction();
            subscribe_to_and_add_objects(local_realm, num_objects_added_after); // these are lost!
        };

        VersionID expected_version;

        auto store_pre_reset_state = [&](SharedRealm local_realm) {
            expected_version = local_realm->read_transaction_version();
        };

        auto verify_post_reset_state = [&, err_future = std::move(error_future)](SharedRealm local_realm) {
            auto sync_error = err_future.get();
            REQUIRE(before_reset_count == 1);
            REQUIRE(after_reset_count == 0);
            REQUIRE(sync_error.status == ErrorCodes::AutoClientResetFailed);
            REQUIRE(sync_error.is_client_reset_requested());

            // All changes should have been rolled back when recovery hit remove_column(),
            // leaving the Realm in the pre-reset state
            local_realm->refresh();
            auto table = local_realm->read_group().get_table("class_TopLevel");
            ColKey added = table->get_column_key(added_property_name);
            REQUIRE(!added);
            const size_t expected_added_objects = num_objects_added_before + num_objects_added_after;
            REQUIRE(table->size() == num_objects_added_by_harness + expected_added_objects);
            size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
            REQUIRE(count_of_valid_array_data == expected_added_objects);

            // The attempted client reset should have been recorded so that we
            // don't attempt it again
            REQUIRE(local_realm->read_transaction_version().version == expected_version.version + 1);
        };

        SECTION("Recover: unsuccessful recovery leads to a manual reset") {
            config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
            auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
            test_reset->make_local_changes(make_local_changes_that_will_fail)
                ->on_post_local_changes(store_pre_reset_state)
                ->on_post_reset(std::move(verify_post_reset_state))
                ->run();
            RealmConfig config_copy = config_local;
            auto&& [error_future2, err_handler2] = make_error_handler();
            config_copy.sync_config->error_handler = err_handler2;
            auto realm_post_reset = Realm::get_shared_realm(config_copy);
            auto sync_error = wait_for_future(std::move(error_future2)).get();
            REQUIRE(before_reset_count == 2);
            REQUIRE(after_reset_count == 0);
            REQUIRE(sync_error.status == ErrorCodes::AutoClientResetFailed);
            REQUIRE(sync_error.is_client_reset_requested());
        }

        SECTION("RecoverOrDiscard: unsuccessful reapply leads to discard") {
            config_local.sync_config->client_resync_mode = ClientResyncMode::RecoverOrDiscard;
            auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
            test_reset->make_local_changes(make_local_changes_that_will_fail)
                ->on_post_local_changes(store_pre_reset_state)
                ->on_post_reset(std::move(verify_post_reset_state))
                ->run();

            RealmConfig config_copy = config_local;
            auto&& [client_reset_future, reset_handler] = reset_utils::make_client_reset_handler();
            config_copy.sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError err) {
                REALM_ASSERT_EX(!err.is_fatal, err.status);
                CHECK(err.server_requests_action == sync::ProtocolErrorInfo::Action::Transient);
            };
            config_copy.sync_config->notify_after_client_reset = reset_handler;
            auto realm_post_reset = Realm::get_shared_realm(config_copy);
            ClientResyncMode mode = wait_for_future(std::move(client_reset_future)).get();
            REQUIRE(mode == ClientResyncMode::DiscardLocal);
            realm_post_reset->refresh();
            auto table = realm_post_reset->read_group().get_table("class_TopLevel");
            ColKey added = table->get_column_key(added_property_name);
            REQUIRE(!added);                                        // reverted local changes
            REQUIRE(table->size() == num_objects_added_by_harness); // discarded all offline local changes
        }
    }

    SECTION("DiscardLocal: offline writes and subscriptions are lost") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) mutable {
                ClientResyncMode mode = wait_for_future(std::move(client_reset_future)).get();
                REQUIRE(mode == ClientResyncMode::DiscardLocal);
                auto subs = local_realm->get_latest_subscription_set();
                wait_for_future(subs.get_state_change_notification(sync::SubscriptionSet::State::Complete)).get();
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto queryable_str_field = table->get_column_key("queryable_str_field");
                auto queryable_int_field = table->get_column_key("queryable_int_field");
                auto tv = table->where().equal(queryable_str_field, StringData(str_field_value)).find_all();
                // the object we created while offline was discarded, and the remote object was not downloaded
                REQUIRE(tv.size() == 0);
                size_t count_of_foo = count_queries_with_str(subs, util::format("\"%1\"", str_field_value));
                // make sure that the subscription for "foo" did not survive the reset
                REQUIRE(count_of_foo == 0);
                REQUIRE(subs.state() == sync::SubscriptionSet::State::Complete);

                // adding data and subscriptions to a reset Realm works as normal
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
                auto latest_subs = local_realm->get_latest_subscription_set();
                REQUIRE(latest_subs.version() > subs.version());
                wait_for_future(latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete))
                    .get();
                local_realm->refresh();
                count_of_foo = count_queries_with_str(latest_subs, util::format("\"%1\"", str_field_value));
                REQUIRE(count_of_foo == 1);
                tv = table->where().equal(queryable_str_field, StringData(str_field_value)).find_all();
                REQUIRE(tv.size() == 2);
                tv.sort(queryable_int_field);
                REQUIRE(tv.get_object(0).get<int64_t>(queryable_int_field) == local_added_int);
                REQUIRE(tv.get_object(1).get<int64_t>(queryable_int_field) == remote_added_int);
            })
            ->run();
    }

    SECTION("DiscardLocal: an invalid subscription made while offline becomes superseded") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        std::unique_ptr<sync::SubscriptionSet> invalid_sub;
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                invalid_sub = std::make_unique<sync::SubscriptionSet>(add_invalid_subscription(local_realm));
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                local_realm->refresh();
                sync::SubscriptionSet::State actual =
                    invalid_sub->get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                REQUIRE(actual == sync::SubscriptionSet::State::Superseded);
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::DiscardLocal);
            })
            ->run();
    }

    SECTION("DiscardLocal: an error is produced if a previously successful query becomes invalid due to "
            "server changes across a reset") {
        // Disable dev mode so non-queryable fields are not automatically added as queryable
        const AppSession& app_session = harness.session().app_session();
        app_session.admin_api.set_development_mode_to(app_session.server_app_id, false);
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->setup([&](SharedRealm realm) {
                if (realm->sync_session()->path() == config_local.path) {
                    auto added_sub = add_subscription_for_new_object(realm, str_field_value, 0);
                    added_sub.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                }
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                add_object(local_realm, str_field_value, local_added_int);
                // Make "queryable_str_field" not a valid query field.
                // Pre-reset, the Realm had a successful query on it, but now when the client comes back online
                // and tries to reset, the fresh Realm download will fail with a query error.
                const AppSession& app_session = harness.session().app_session();
                auto baas_sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);
                auto baas_sync_config =
                    app_session.admin_api.get_config(app_session.server_app_id, baas_sync_service);
                REQUIRE(baas_sync_config.queryable_field_names->is_array());
                auto it = baas_sync_config.queryable_field_names->begin();
                for (; it != baas_sync_config.queryable_field_names->end(); ++it) {
                    if (*it == "queryable_str_field") {
                        break;
                    }
                }
                REQUIRE(it != baas_sync_config.queryable_field_names->end());
                baas_sync_config.queryable_field_names->erase(it);
                app_session.admin_api.enable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
            })
            ->on_post_reset([&, err_future = std::move(error_future)](SharedRealm) mutable {
                auto sync_error = wait_for_future(std::move(err_future)).get();
                INFO(sync_error.status);
                CHECK(sync_error.status == ErrorCodes::AutoClientResetFailed);
                REQUIRE(sync_error.status.reason().find(
                            "SyncClientResetRequired: Bad client file identifier (IDENT)") != std::string::npos);
            })
            ->run();
    }

    SECTION("DiscardLocal: completion callbacks fire after client reset even when there is no data to download") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->on_post_local_changes([&](SharedRealm realm) {
                wait_for_upload(*realm);
                wait_for_download(*realm);
            })
            ->run();
    }

    SECTION("DiscardLocal: open realm after client reset failure") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [error_future, error_handler] = make_error_handler();
        config_local.sync_config->error_handler = error_handler;

        std::string fresh_path = realm::_impl::client_reset::get_fresh_path_for(config_local.path);
        // create a non-empty directory that we'll fail to delete
        util::make_dir(fresh_path);
        util::File(util::File::resolve("file", fresh_path), util::File::mode_Write);

        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset->run();

        // Client reset fails due to sync client not being able to create the fresh realm.
        auto sync_error = wait_for_future(std::move(error_future)).get();
        REQUIRE(sync_error.status == ErrorCodes::AutoClientResetFailed);

        // Open the realm again. This should not crash.
        auto&& [err_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = std::move(err_handler);

        auto realm_post_reset = Realm::get_shared_realm(config_local);
        sync_error = wait_for_future(std::move(err_future)).get();
        REQUIRE(sync_error.status == ErrorCodes::AutoClientResetFailed);
    }

    enum class ResetMode { NoReset, InitiateClientReset };
    auto seed_realm = [&harness, &subscribe_to_and_add_objects](RealmConfig config, ResetMode reset_mode) {
        config.sync_config->error_handler = [path = config.path](std::shared_ptr<SyncSession>, SyncError err) {
            // ignore spurious failures on this instance
            util::format(std::cout, "spurious error while seeding a Realm at '%1': %2\n", path, err.status);
        };
        SharedRealm realm = Realm::get_shared_realm(config);
        subscribe_to_and_add_objects(realm, 1);
        auto subs = realm->get_latest_subscription_set();
        auto result = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        CHECK(result == sync::SubscriptionSet::State::Complete);
        if (reset_mode == ResetMode::InitiateClientReset) {
            reset_utils::trigger_client_reset(harness.session().app_session(), realm);
        }
        realm->close();
    };

    auto setup_reset_handlers_for_schema_validation =
        [&before_reset_count, &after_reset_count](RealmConfig& config, Schema expected_schema) {
            auto& sync_config = *config.sync_config;
            sync_config.error_handler = [](std::shared_ptr<SyncSession>, SyncError err) {
                FAIL(err.status);
            };
            sync_config.notify_before_client_reset = [&before_reset_count,
                                                      expected = expected_schema](SharedRealm frozen_before) {
                ++before_reset_count;
                REQUIRE(frozen_before->schema().size() > 0);
                REQUIRE(frozen_before->schema_version() != ObjectStore::NotVersioned);
                REQUIRE(frozen_before->schema() == expected);
            };

            auto [promise, future] = util::make_promise_future<void>();
            sync_config.notify_after_client_reset =
                [&after_reset_count, promise = util::CopyablePromiseHolder<void>(std::move(promise)), expected_schema,
                 reset_mode = config.sync_config->client_resync_mode, has_schema = config.schema.has_value()](
                    SharedRealm frozen_before, ThreadSafeReference after_ref, bool did_recover) mutable {
                    ++after_reset_count;
                    REQUIRE(frozen_before->schema().size() > 0);
                    REQUIRE(frozen_before->schema_version() != ObjectStore::NotVersioned);
                    REQUIRE(frozen_before->schema() == expected_schema);
                    SharedRealm after = Realm::get_shared_realm(std::move(after_ref), util::Scheduler::make_dummy());
                    if (!has_schema) {
                        after->set_schema_subset(expected_schema);
                    }
                    REQUIRE(after);
                    REQUIRE(after->schema() == expected_schema);
                    // the above check is sufficient unless operator==() is changed to not care about ordering
                    // so future proof that by explicitly checking the order of properties here as well
                    REQUIRE(after->schema().size() == frozen_before->schema().size());
                    auto after_it = after->schema().find("TopLevel");
                    auto before_it = frozen_before->schema().find("TopLevel");
                    REQUIRE(after_it != after->schema().end());
                    REQUIRE(before_it != frozen_before->schema().end());
                    REQUIRE(after_it->name == before_it->name);
                    REQUIRE(after_it->persisted_properties.size() == before_it->persisted_properties.size());
                    REQUIRE(after_it->persisted_properties[1].name == "queryable_int_field");
                    REQUIRE(after_it->persisted_properties[2].name == "queryable_str_field");
                    REQUIRE(before_it->persisted_properties[1].name == "queryable_int_field");
                    REQUIRE(before_it->persisted_properties[2].name == "queryable_str_field");
                    REQUIRE(did_recover == (reset_mode == ClientResyncMode::Recover));
                    promise.get_promise().emplace_value();
                };
            return std::move(future); // move is not redundant here because of how destructing works
        };

    SECTION("Recover: schema indexes match in before and after states") {
        seed_realm(config_local, ResetMode::InitiateClientReset);
        // reorder a property such that it does not match the on disk property order
        std::vector<ObjectSchema> local_schema = schema;
        std::swap(local_schema[0].persisted_properties[1], local_schema[0].persisted_properties[2]);
        local_schema[0].persisted_properties.push_back(
            {"queryable_oid_field", PropertyType::ObjectId | PropertyType::Nullable});
        config_local.schema = local_schema;
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto future = setup_reset_handlers_for_schema_validation(config_local, local_schema);
        SharedRealm realm = Realm::get_shared_realm(config_local);
        future.get();
        CHECK(before_reset_count == 1);
        CHECK(after_reset_count == 1);
    }

    SECTION("Adding a local property matching a server addition is allowed") {
        auto mode = GENERATE(ClientResyncMode::DiscardLocal, ClientResyncMode::Recover);
        config_local.sync_config->client_resync_mode = mode;
        CHECK_NOTHROW(seed_realm(config_local, ResetMode::InitiateClientReset));
        std::vector<ObjectSchema> changed_schema = schema;
        changed_schema[0].persisted_properties.push_back(
            {"queryable_oid_field", PropertyType::ObjectId | PropertyType::Nullable});
        // In a separate Realm, make the property addition.
        // Since this is dev mode, it will be added to the server's schema.
        config_remote.schema = changed_schema;
        CHECK_NOTHROW(seed_realm(config_remote, ResetMode::NoReset));
        std::swap(changed_schema[0].persisted_properties[1], changed_schema[0].persisted_properties[2]);
        config_local.schema = changed_schema;
        auto future = setup_reset_handlers_for_schema_validation(config_local, changed_schema);
        successfully_async_open_realm(config_local);
        CHECK_NOTHROW(future.get());
        CHECK(before_reset_count == 1);
        CHECK(after_reset_count == 1);
    }

    SECTION("Adding a local property matching a server addition inside the before reset callback is allowed") {
        auto mode = GENERATE(ClientResyncMode::DiscardLocal, ClientResyncMode::Recover);
        config_local.sync_config->client_resync_mode = mode;
        seed_realm(config_local, ResetMode::InitiateClientReset);
        std::vector<ObjectSchema> changed_schema = schema;
        changed_schema[0].persisted_properties.push_back(
            {"queryable_oid_field", PropertyType::ObjectId | PropertyType::Nullable});
        // In a separate Realm, make the property addition.
        // Since this is dev mode, it will be added to the server's schema.
        config_remote.schema = changed_schema;
        seed_realm(config_remote, ResetMode::NoReset);
        std::swap(changed_schema[0].persisted_properties[1], changed_schema[0].persisted_properties[2]);
        config_local.schema.reset();
        config_local.sync_config->freeze_before_reset_realm = false;
        auto future = setup_reset_handlers_for_schema_validation(config_local, changed_schema);

        auto notify_before = std::move(config_local.sync_config->notify_before_client_reset);
        config_local.sync_config->notify_before_client_reset = [=](std::shared_ptr<Realm> realm) {
            realm->update_schema(changed_schema);
            notify_before(realm);
        };

        auto notify_after = std::move(config_local.sync_config->notify_after_client_reset);
        config_local.sync_config->notify_after_client_reset = [=](std::shared_ptr<Realm> before,
                                                                  ThreadSafeReference after, bool did_recover) {
            before->set_schema_subset(changed_schema);
            notify_after(before, std::move(after), did_recover);
        };

        successfully_async_open_realm(config_local);
        future.get();
        CHECK(before_reset_count == 1);
        CHECK(after_reset_count == 1);
    }

    auto make_additive_changes = [](std::vector<ObjectSchema> schema) {
        schema[0].persisted_properties.push_back(
            {"added_oid_field", PropertyType::ObjectId | PropertyType::Nullable});
        std::swap(schema[0].persisted_properties[1], schema[0].persisted_properties[2]);
        schema.push_back({"AddedClass",
                          {
                              {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                              {"str_field", PropertyType::String | PropertyType::Nullable},
                          }});
        return schema;
    };
    SECTION("Recover: additive schema changes are recovered in dev mode") {
        const AppSession& app_session = harness.session().app_session();
        app_session.admin_api.set_development_mode_to(app_session.server_app_id, true);
        seed_realm(config_local, ResetMode::InitiateClientReset);
        std::vector<ObjectSchema> changed_schema = make_additive_changes(schema);
        config_local.schema = changed_schema;
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        ThreadSafeReference ref_async;
        auto future = setup_reset_handlers_for_schema_validation(config_local, changed_schema);
        {
            auto realm = successfully_async_open_realm(config_local);
            future.get();
            CHECK(before_reset_count == 1);
            CHECK(after_reset_count == 1);

            // make changes to the newly added property
            realm->begin_transaction();
            auto table = realm->read_group().get_table("class_TopLevel");
            ColKey new_col = table->get_column_key("added_oid_field");
            REQUIRE(new_col);
            for (auto it = table->begin(); it != table->end(); ++it) {
                it->set(new_col, ObjectId::gen());
            }
            realm->commit_transaction();
            // subscribe to the new Class and add an object
            auto new_table = realm->read_group().get_table("class_AddedClass");
            auto sub_set = realm->get_latest_subscription_set();
            auto mut_sub = sub_set.make_mutable_copy();
            mut_sub.insert_or_assign(Query(new_table));
            mut_sub.commit();
            realm->begin_transaction();
            REQUIRE(new_table);
            new_table->create_object_with_primary_key(ObjectId::gen());
            realm->commit_transaction();
            auto result = realm->get_latest_subscription_set()
                              .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                              .get();
            CHECK(result == sync::SubscriptionSet::State::Complete);
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }
        REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(config_local.path));
        {
            // ensure that an additional schema change after the successful reset is also accepted by the server
            changed_schema[0].persisted_properties.push_back(
                {"added_oid_field_second", PropertyType::ObjectId | PropertyType::Nullable});
            changed_schema.push_back({"AddedClassSecond",
                                      {
                                          {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                          {"str_field_2", PropertyType::String | PropertyType::Nullable},
                                      }});
            config_local.schema = changed_schema;
            auto realm = Realm::get_shared_realm(config_local);
            auto table = realm->read_group().get_table("class_AddedClassSecond");
            ColKey new_col = table->get_column_key("str_field_2");
            REQUIRE(new_col);
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(Query(table).equal(new_col, "hello"));
            auto subs = new_subs.commit();
            realm->begin_transaction();
            table->create_object_with_primary_key(Mixed{ObjectId::gen()}, {{new_col, "hello"}});
            realm->commit_transaction();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_advance(*realm);
            REQUIRE(table->size() == 1);
        }
    }

    SECTION("DiscardLocal: additive schema changes not allowed") {
        seed_realm(config_local, ResetMode::InitiateClientReset);
        std::vector<ObjectSchema> changed_schema = make_additive_changes(schema);
        config_local.schema = changed_schema;
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;
        auto status = async_open_realm(config_local);
        REQUIRE_FALSE(status.is_ok());
        REQUIRE_THAT(status.get_status().reason(),
                     Catch::Matchers::ContainsSubstring(
                         "'Client reset cannot recover when classes have been removed: {AddedClass}'"));
        error_future.get();
        CHECK(before_reset_count == 1);
        CHECK(after_reset_count == 0);
    }

    SECTION("Recover: incompatible schema changes on async open are an error") {
        seed_realm(config_local, ResetMode::InitiateClientReset);
        std::vector<ObjectSchema> changed_schema = schema;
        changed_schema[0].persisted_properties[0].type = PropertyType::UUID; // incompatible type change
        config_local.schema = changed_schema;
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;
        auto status = async_open_realm(config_local);
        REQUIRE_FALSE(status.is_ok());
        REQUIRE_THAT(
            status.get_status().reason(),
            Catch::Matchers::ContainsSubstring(
                "'The following changes cannot be made in additive-only schema mode:\n"
                "- Property 'TopLevel._id' has been changed from 'object id' to 'uuid'.\nIf your app is running in "
                "development mode, you can delete the realm and restart the app to update your schema.'"));
        error_future.get();
        CHECK(before_reset_count == 0); // we didn't even get this far because opening the frozen realm fails
        CHECK(after_reset_count == 0);
    }

    SECTION("Recover: additive schema changes without dev mode produce an error after client reset") {
        const AppSession& app_session = harness.session().app_session();
        app_session.admin_api.set_development_mode_to(app_session.server_app_id, true);
        seed_realm(config_local, ResetMode::InitiateClientReset);
        // Disable dev mode so that schema changes are not allowed
        app_session.admin_api.set_development_mode_to(app_session.server_app_id, false);
        auto cleanup = util::make_scope_exit([&]() noexcept {
            const AppSession& app_session = harness.session().app_session();
            app_session.admin_api.set_development_mode_to(app_session.server_app_id, true);
        });

        std::vector<ObjectSchema> changed_schema = make_additive_changes(schema);
        config_local.schema = changed_schema;
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        (void)setup_reset_handlers_for_schema_validation(config_local, changed_schema);
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;
        auto realm = successfully_async_open_realm(config_local);
        // make changes to the new property
        realm->begin_transaction();
        auto table = realm->read_group().get_table("class_TopLevel");
        ColKey new_col = table->get_column_key("added_oid_field");
        REQUIRE(new_col);
        for (auto it = table->begin(); it != table->end(); ++it) {
            it->set(new_col, ObjectId::gen());
        }
        realm->commit_transaction();
        auto err = error_future.get();
        std::string property_err = "Invalid schema change (UPLOAD): non-breaking schema change: adding "
                                   "\"ObjectID\" column at field \"added_oid_field\" in schema \"TopLevel\", "
                                   "schema changes from clients are restricted when developer mode is disabled";
        std::string class_err = "Invalid schema change (UPLOAD): non-breaking schema change: adding schema "
                                "for Realm table \"AddedClass\", schema changes from clients are restricted when "
                                "developer mode is disabled";
        REQUIRE_THAT(err.status.reason(), Catch::Matchers::ContainsSubstring(property_err) ||
                                              Catch::Matchers::ContainsSubstring(class_err));
        CHECK(before_reset_count == 1);
        CHECK(after_reset_count == 1);
    }

    SECTION("Recover: inserts in collections in mixed - collections cleared remotely") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->populate_initial_object([&](SharedRealm realm) {
                subscribe_to_all_and_bootstrap(*realm);
                auto pk_of_added_object = ObjectId::gen();
                auto table = realm->read_group().get_table("class_TopLevel");

                realm->begin_transaction();
                CppContext c(realm);
                auto obj = Object::create(c, realm, "TopLevel",
                                          std::any(AnyDict{{"_id"s, pk_of_added_object},
                                                           {"queryable_str_field"s, "initial value"s},
                                                           {"sum_of_list_field"s, int64_t(42)}}));
                auto col_any = table->get_column_key("any_mixed");
                obj.get_obj().set_collection(col_any, CollectionType::List);
                auto list = obj.get_obj().get_list_ptr<Mixed>(col_any);
                list->add(1);
                auto dict = obj.get_obj().get_dictionary("dictionary_mixed");
                dict.insert("key", 42);
                realm->commit_transaction();
                wait_for_upload(*realm);
                return pk_of_added_object;
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                local_realm->begin_transaction();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto col_any = table->get_column_key("any_mixed");
                auto obj = table->get_object(0);
                auto list = obj.get_list_ptr<Mixed>(col_any);
                list->add(2);
                auto dict = obj.get_dictionary("dictionary_mixed");
                dict.insert("key2", "value");
                local_realm->commit_transaction();
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                remote_realm->begin_transaction();
                auto table = remote_realm->read_group().get_table("class_TopLevel");
                auto col_any = table->get_column_key("any_mixed");
                auto obj = table->get_object(0);
                auto list = obj.get_list_ptr<Mixed>(col_any);
                list->clear();
                auto dict = obj.get_dictionary("dictionary_mixed");
                dict.clear();
                remote_realm->commit_transaction();
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                wait_for_advance(*local_realm);
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::Recover);
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto obj = table->get_object(0);
                auto col_any = table->get_column_key("any_mixed");
                auto list = obj.get_list_ptr<Mixed>(col_any);
                CHECK(list->size() == 1);
                CHECK(list->get_any(0).get_int() == 2);
                auto dict = obj.get_dictionary("dictionary_mixed");
                CHECK(dict.size() == 1);
                CHECK(dict.get("key2").get_string() == "value");
            })
            ->run();
    }
}

TEST_CASE("flx: creating an object on a class with no subscription throws", "[sync][flx][subscription][baas]") {
    FLXSyncTestHarness harness("flx_bad_query", {g_simple_embedded_obj_schema, {"queryable_str_field"}});
    harness.do_with_new_user([&](auto user) {
        SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
        auto [error_promise, error_future] = util::make_promise_future<SyncError>();
        auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
        config.sync_config->error_handler = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>,
                                                                                        SyncError err) {
            CHECK(err.server_requests_action == sync::ProtocolErrorInfo::Action::Transient);
            error_promise->emplace_value(std::move(err));
        };

        auto realm = Realm::get_shared_realm(config);
        CppContext c(realm);
        realm->begin_transaction();
        REQUIRE_THROWS_AS(
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_str_field", "foo"s}})),
            NoSubscriptionForWrite);
        realm->cancel_transaction();

        auto table = realm->read_group().get_table("class_TopLevel");

        REQUIRE(table->is_empty());
        auto col_key = table->get_column_key("queryable_str_field");
        {
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(Query(table).equal(col_key, "foo"));
            auto subs = new_subs.commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        realm->begin_transaction();
        auto obj = Object::create(c, realm, "TopLevel",
                                  std::any(AnyDict{{"_id", ObjectId::gen()},
                                                   {"queryable_str_field", "foo"s},
                                                   {"embedded_obj", AnyDict{{"str_field", "bar"s}}}}));
        realm->commit_transaction();

        realm->begin_transaction();
        auto embedded_obj = util::any_cast<Object&&>(obj.get_property_value<std::any>(c, "embedded_obj"));
        embedded_obj.set_property_value(c, "str_field", std::any{"baz"s});
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);
    });
}

TEST_CASE("flx: uploading an object that is out-of-view results in compensating write",
          "[sync][flx][compensating write][baas]") {
    static std::optional<FLXSyncTestHarness> harness;
    if (!harness) {
        Schema schema{{"TopLevel",
                       {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                        {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                        {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "TopLevel_embedded_obj"}}},
                      {"TopLevel_embedded_obj",
                       ObjectSchema::ObjectType::Embedded,
                       {{"str_field", PropertyType::String | PropertyType::Nullable}}},
                      {"Int PK",
                       {
                           {"_id", PropertyType::Int, Property::IsPrimary{true}},
                           {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                       }},
                      {"String PK",
                       {
                           {"_id", PropertyType::String, Property::IsPrimary{true}},
                           {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                       }},
                      {"UUID PK",
                       {
                           {"_id", PropertyType::UUID, Property::IsPrimary{true}},
                           {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                       }}};

        AppCreateConfig::ServiceRole role{"compensating_write_perms"};
        role.document_filters.write = {{"queryable_str_field", {{"$in", nlohmann::json::array({"foo", "bar"})}}}};

        FLXSyncTestHarness::ServerSchema server_schema{schema, {"queryable_str_field"}, {role}};
        harness.emplace("flx_bad_query", server_schema);
    }

    create_user_and_log_in(harness->app());
    auto user = harness->app()->current_user();

    auto make_error_handler = [] {
        auto [error_promise, error_future] = util::make_promise_future<SyncError>();
        auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
        auto fn = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>, SyncError err) mutable {
            if (!error_promise) {
                util::format(std::cerr,
                             "An unexpected sync error was caught by the default SyncTestFile handler: '%1'\n",
                             err.status);
                abort();
            }
            error_promise->emplace_value(std::move(err));
            error_promise.reset();
        };

        return std::make_pair(std::move(error_future), std::move(fn));
    };

    auto validate_sync_error = [&](const SyncError& sync_error, Mixed expected_pk, const char* expected_object_name,
                                   const std::string& error_msg_fragment) {
        CHECK(sync_error.status == ErrorCodes::SyncCompensatingWrite);
        CHECK(!sync_error.is_client_reset_requested());
        CHECK(sync_error.compensating_writes_info.size() == 1);
        CHECK(sync_error.server_requests_action == sync::ProtocolErrorInfo::Action::Warning);
        auto write_info = sync_error.compensating_writes_info[0];
        CHECK(write_info.primary_key == expected_pk);
        CHECK(write_info.object_name == expected_object_name);
        CHECK_THAT(write_info.reason, Catch::Matchers::ContainsSubstring(error_msg_fragment));
    };

    SyncTestFile config(user, harness->schema(), SyncConfig::FLXSyncEnabled{});
    auto&& [error_future, err_handler] = make_error_handler();
    config.sync_config->error_handler = err_handler;
    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_TopLevel");

    auto create_subscription = [&](StringData table_name, auto make_query) {
        auto table = realm->read_group().get_table(table_name);
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(make_query(Query(table), queryable_str_field));
        new_query.commit();
    };

    SECTION("compensating write because of permission violation") {
        create_subscription("class_TopLevel", [](auto q, auto col) {
            return q.equal(col, "bizz");
        });

        CppContext c(realm);
        realm->begin_transaction();
        auto invalid_obj = ObjectId::gen();
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", invalid_obj}, {"queryable_str_field", "bizz"s}}));
        realm->commit_transaction();

        validate_sync_error(
            std::move(error_future).get(), invalid_obj, "TopLevel",
            util::format("write to ObjectID(\"%1\") in table \"TopLevel\" not allowed", invalid_obj.to_string()));

        wait_for_advance(*realm);

        auto top_level_table = realm->read_group().get_table("class_TopLevel");
        REQUIRE(top_level_table->is_empty());
    }

    SECTION("compensating write because of permission violation with write on embedded object") {
        create_subscription("class_TopLevel", [](auto q, auto col) {
            return q.equal(col, "bizz").Or().equal(col, "foo");
        });

        CppContext c(realm);
        realm->begin_transaction();
        auto invalid_obj = ObjectId::gen();
        auto obj = Object::create(c, realm, "TopLevel",
                                  std::any(AnyDict{{"_id", invalid_obj},
                                                   {"queryable_str_field", "foo"s},
                                                   {"embedded_obj", AnyDict{{"str_field", "bar"s}}}}));
        realm->commit_transaction();
        realm->begin_transaction();
        obj.set_property_value(c, "queryable_str_field", std::any{"bizz"s});
        realm->commit_transaction();
        realm->begin_transaction();
        auto embedded_obj = util::any_cast<Object&&>(obj.get_property_value<std::any>(c, "embedded_obj"));
        embedded_obj.set_property_value(c, "str_field", std::any{"baz"s});
        realm->commit_transaction();

        validate_sync_error(
            std::move(error_future).get(), invalid_obj, "TopLevel",
            util::format("write to ObjectID(\"%1\") in table \"TopLevel\" not allowed", invalid_obj.to_string()));

        wait_for_advance(*realm);

        obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any(invalid_obj));
        embedded_obj = util::any_cast<Object&&>(obj.get_property_value<std::any>(c, "embedded_obj"));
        REQUIRE(util::any_cast<std::string&&>(obj.get_property_value<std::any>(c, "queryable_str_field")) == "foo");
        REQUIRE(util::any_cast<std::string&&>(embedded_obj.get_property_value<std::any>(c, "str_field")) == "bar");

        realm->begin_transaction();
        embedded_obj.set_property_value(c, "str_field", std::any{"baz"s});
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);

        wait_for_advance(*realm);
        obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any(invalid_obj));
        embedded_obj = util::any_cast<Object&&>(obj.get_property_value<std::any>(c, "embedded_obj"));
        REQUIRE(embedded_obj.get_column_value<StringData>("str_field") == "baz");
    }

    SECTION("compensating write for writing a top-level object that is out-of-view") {
        create_subscription("class_TopLevel", [](auto q, auto col) {
            return q.equal(col, "foo");
        });

        CppContext c(realm);
        realm->begin_transaction();
        auto valid_obj = ObjectId::gen();
        auto invalid_obj = ObjectId::gen();
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{
                           {"_id", valid_obj},
                           {"queryable_str_field", "foo"s},
                       }));
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{
                           {"_id", invalid_obj},
                           {"queryable_str_field", "bar"s},
                       }));
        realm->commit_transaction();

        validate_sync_error(std::move(error_future).get(), invalid_obj, "TopLevel",
                            "object is outside of the current query view");

        wait_for_advance(*realm);

        auto top_level_table = realm->read_group().get_table("class_TopLevel");
        REQUIRE(top_level_table->size() == 1);
        REQUIRE(top_level_table->get_object_with_primary_key(valid_obj));

        // Verify that a valid object afterwards does not produce an error
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{
                           {"_id", ObjectId::gen()},
                           {"queryable_str_field", "foo"s},
                       }));
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);
    }

    SECTION("compensating writes for each primary key type") {
        SECTION("int") {
            create_subscription("class_Int PK", [](auto q, auto col) {
                return q.equal(col, "foo");
            });
            realm->begin_transaction();
            realm->read_group().get_table("class_Int PK")->create_object_with_primary_key(123456);
            realm->commit_transaction();

            validate_sync_error(std::move(error_future).get(), 123456, "Int PK",
                                "write to 123456 in table \"Int PK\" not allowed");
        }

        SECTION("short string") {
            create_subscription("class_String PK", [](auto q, auto col) {
                return q.equal(col, "foo");
            });
            realm->begin_transaction();
            realm->read_group().get_table("class_String PK")->create_object_with_primary_key("short");
            realm->commit_transaction();

            validate_sync_error(std::move(error_future).get(), "short", "String PK",
                                "write to \"short\" in table \"String PK\" not allowed");
        }

        SECTION("long string") {
            create_subscription("class_String PK", [](auto q, auto col) {
                return q.equal(col, "foo");
            });
            realm->begin_transaction();
            const char* pk = "long string which won't fit in the SSO buffer";
            realm->read_group().get_table("class_String PK")->create_object_with_primary_key(pk);
            realm->commit_transaction();

            validate_sync_error(std::move(error_future).get(), pk, "String PK",
                                util::format("write to \"%1\" in table \"String PK\" not allowed", pk));
        }

        SECTION("uuid") {
            create_subscription("class_UUID PK", [](auto q, auto col) {
                return q.equal(col, "foo");
            });
            realm->begin_transaction();
            UUID pk("01234567-9abc-4def-9012-3456789abcde");
            realm->read_group().get_table("class_UUID PK")->create_object_with_primary_key(pk);
            realm->commit_transaction();

            validate_sync_error(std::move(error_future).get(), pk, "UUID PK",
                                util::format("write to UUID(%1) in table \"UUID PK\" not allowed", pk));
        }
    }

    // Clear the Realm afterwards as we're reusing an app
    realm->begin_transaction();
    table->clear();
    realm->commit_transaction();
    wait_for_upload(*realm);
    realm.reset();

    // Add new sections before this
    SECTION("teardown") {
        harness->app()->sync_manager()->wait_for_sessions_to_terminate();
        harness.reset();
    }
}

TEST_CASE("flx: query on non-queryable field results in query error message", "[sync][flx][query][baas]") {
    static std::optional<FLXSyncTestHarness> harness;
    if (!harness) {
        harness.emplace("flx_bad_query");
    }

    auto create_subscription = [](SharedRealm realm, StringData table_name, StringData column_name, auto make_query) {
        auto table = realm->read_group().get_table(table_name);
        auto queryable_field = table->get_column_key(column_name);
        auto new_query = realm->get_active_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(make_query(Query(table), queryable_field));
        return new_query.commit();
    };

    auto check_status = [](auto status) {
        CHECK(!status.is_ok());
        std::string reason = status.get_status().reason();
        // Depending on the version of baas used, it may return 'Invalid query:' or
        // 'Client provided query with bad syntax:'
        if ((reason.find("Invalid query:") == std::string::npos &&
             reason.find("Client provided query with bad syntax:") == std::string::npos) ||
            reason.find("\"TopLevel\": key \"non_queryable_field\" is not a queryable field") == std::string::npos) {
            FAIL(util::format("Error reason did not match expected: `%1`", reason));
        }
    };

    SECTION("Good query after bad query") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            auto subs = create_subscription(realm, "class_TopLevel", "non_queryable_field", [](auto q, auto c) {
                return q.equal(c, "bar");
            });
            auto sub_res = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
            check_status(sub_res);

            CHECK(realm->get_active_subscription_set().version() == 0);
            CHECK(realm->get_latest_subscription_set().version() == 1);

            subs = create_subscription(realm, "class_TopLevel", "queryable_str_field", [](auto q, auto c) {
                return q.equal(c, "foo");
            });
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            CHECK(realm->get_active_subscription_set().version() == 2);
            CHECK(realm->get_latest_subscription_set().version() == 2);
        });
    }

    SECTION("Bad query after bad query") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            auto sync_session = realm->sync_session();
            sync_session->pause();

            auto subs = create_subscription(realm, "class_TopLevel", "non_queryable_field", [](auto q, auto c) {
                return q.equal(c, "bar");
            });
            auto subs2 = create_subscription(realm, "class_TopLevel", "non_queryable_field", [](auto q, auto c) {
                return q.equal(c, "bar");
            });

            sync_session->resume();

            auto sub_res = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
            auto sub_res2 =
                subs2.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();

            check_status(sub_res);
            check_status(sub_res2);

            CHECK(realm->get_active_subscription_set().version() == 0);
            CHECK(realm->get_latest_subscription_set().version() == 2);
        });
    }

    // Test for issue #6839, where wait for download after committing a new subscription and then
    // wait for the subscription complete notification was leading to a garbage reason value in the
    // status provided to the subscription complete callback.
    SECTION("Download during bad query") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            // Wait for steady state before committing the new subscription
            REQUIRE(!wait_for_download(*realm));

            auto subs = create_subscription(realm, "class_TopLevel", "non_queryable_field", [](auto q, auto c) {
                return q.equal(c, "bar");
            });
            // Wait for download is actually waiting for the subscription to be applied after it was committed
            REQUIRE(!wait_for_download(*realm));
            // After subscription is complete or fails during wait for download, this function completes
            // without blocking
            auto result = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
            // Verify error occurred
            check_status(result);
        });
    }

    // Add new sections before this
    SECTION("teardown") {
        harness->app()->sync_manager()->wait_for_sessions_to_terminate();
        harness.reset();
    }
}

#if REALM_ENABLE_GEOSPATIAL
TEST_CASE("flx: geospatial", "[sync][flx][geospatial][baas]") {
    static std::optional<FLXSyncTestHarness> harness;
    if (!harness) {
        Schema schema{
            {"restaurant",
             {
                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                 {"queryable_str_field", PropertyType::String},
                 {"location", PropertyType::Object | PropertyType::Nullable, "geoPointType"},
                 {"array", PropertyType::Object | PropertyType::Array, "geoPointType"},
             }},
            {"geoPointType",
             ObjectSchema::ObjectType::Embedded,
             {
                 {"type", PropertyType::String},
                 {"coordinates", PropertyType::Double | PropertyType::Array},
             }},
        };
        FLXSyncTestHarness::ServerSchema server_schema{schema, {"queryable_str_field", "location"}};
        harness.emplace("flx_geospatial", server_schema);
    }

    auto create_subscription = [](SharedRealm realm, StringData table_name, StringData column_name, auto make_query) {
        auto table = realm->read_group().get_table(table_name);
        auto queryable_field = table->get_column_key(column_name);
        auto new_query = realm->get_active_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(make_query(Query(table), queryable_field));
        return new_query.commit();
    };

    SECTION("Server supports a basic geowithin FLX query") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            const realm::AppSession& app_session = harness->session().app_session();
            auto sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);

            AdminAPISession::ServiceConfig config =
                app_session.admin_api.get_config(app_session.server_app_id, sync_service);
            auto subs = create_subscription(realm, "class_restaurant", "location", [](Query q, ColKey c) {
                GeoBox area{GeoPoint{0.2, 0.2}, GeoPoint{0.7, 0.7}};
                Query query = q.get_table()->column<Link>(c).geo_within(area);
                std::string ser = query.get_description();
                return query;
            });
            auto sub_res = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
            CHECK(sub_res.is_ok());
            CHECK(realm->get_active_subscription_set().version() == 1);
            CHECK(realm->get_latest_subscription_set().version() == 1);
        });
    }

    SECTION("geospatial query consistency: local/server/FLX") {
        harness->do_with_new_user([&](std::shared_ptr<SyncUser> user) {
            SyncTestFile config(user, harness->schema(), SyncConfig::FLXSyncEnabled{});
            auto error_pf = util::make_promise_future<SyncError>();
            config.sync_config->error_handler =
                [promise = std::make_shared<util::Promise<SyncError>>(std::move(error_pf.promise))](
                    std::shared_ptr<SyncSession>, SyncError error) {
                    promise->emplace_value(std::move(error));
                };

            auto realm = Realm::get_shared_realm(config);

            auto subs = create_subscription(realm, "class_restaurant", "queryable_str_field", [](Query q, ColKey c) {
                return q.equal(c, "synced");
            });
            auto make_polygon_filter = [&](const GeoPolygon& polygon) -> bson::BsonDocument {
                bson::BsonArray inner{};
                REALM_ASSERT_3(polygon.points.size(), ==, 1);
                for (auto& point : polygon.points[0]) {
                    inner.push_back(bson::BsonArray{point.longitude, point.latitude});
                }
                bson::BsonArray coords;
                coords.push_back(inner);
                bson::BsonDocument geo_bson{{{"type", "Polygon"}, {"coordinates", coords}}};
                bson::BsonDocument filter{
                    {"location", bson::BsonDocument{{"$geoWithin", bson::BsonDocument{{"$geometry", geo_bson}}}}}};
                return filter;
            };
            auto make_circle_filter = [&](const GeoCircle& circle) -> bson::BsonDocument {
                bson::BsonArray coords{circle.center.longitude, circle.center.latitude};
                bson::BsonArray inner;
                inner.push_back(coords);
                inner.push_back(circle.radius_radians);
                bson::BsonDocument filter{
                    {"location", bson::BsonDocument{{"$geoWithin", bson::BsonDocument{{"$centerSphere", inner}}}}}};
                return filter;
            };
            auto run_query_on_server = [&](const bson::BsonDocument& filter,
                                           std::optional<std::string> expected_error = {}) -> size_t {
                auto remote_client = harness->app()->current_user()->mongo_client("BackingDB");
                auto db = remote_client.db(harness->session().app_session().config.mongo_dbname);
                auto restaurant_collection = db["restaurant"];
                bool processed = false;
                constexpr int64_t limit = 1000;
                size_t matches = 0;
                restaurant_collection.count(filter, limit, [&](uint64_t count, util::Optional<AppError> error) {
                    processed = true;
                    if (error) {
                        if (!expected_error) {
                            util::format(std::cout, "query error: %1\n", error->reason());
                            FAIL(error);
                        }
                        else {
                            std::string reason = std::string(error->reason());
                            std::transform(reason.begin(), reason.end(), reason.begin(), toLowerAscii);
                            std::transform(expected_error->begin(), expected_error->end(), expected_error->begin(),
                                           toLowerAscii);
                            auto pos = reason.find(*expected_error);
                            if (pos == std::string::npos) {
                                util::format(std::cout, "mismatch error: '%1' and '%2'\n", reason, *expected_error);
                                FAIL(reason);
                            }
                        }
                    }
                    matches = size_t(count);
                });
                REQUIRE(processed);
                return matches;
            };
            auto sub_res = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
            CHECK(sub_res.is_ok());
            CHECK(realm->get_active_subscription_set().version() == 1);
            CHECK(realm->get_latest_subscription_set().version() == 1);

            CppContext c(realm);
            int64_t pk = 0;
            auto add_point = [&](GeoPoint p) {
                Object::create(
                    c, realm, "restaurant",
                    std::any(AnyDict{
                        {"_id", ++pk},
                        {"queryable_str_field", "synced"s},
                        {"location", AnyDict{{"type", "Point"s},
                                             {"coordinates", std::vector<std::any>{p.longitude, p.latitude}}}}}));
            };
            std::vector<GeoPoint> points = {
                GeoPoint{-74.006, 40.712800000000001},            // New York city
                GeoPoint{12.568300000000001, 55.676099999999998}, // Copenhagen
                GeoPoint{12.082599999999999, 55.628},             // ragnarok, Roskilde
                GeoPoint{-180.1, -90.1},                          // invalid
                GeoPoint{0, 90},                                  // north pole
                GeoPoint{-82.68193, 84.74653},                    // northern point that falls within a box later
                GeoPoint{82.55243, 84.54981}, // another northern point, but on the other side of the pole
                GeoPoint{2129, 89},           // invalid
            };
            constexpr size_t invalids_to_be_compensated = 2; // 4, 8
            realm->begin_transaction();
            for (auto& point : points) {
                add_point(point);
            }
            realm->commit_transaction();
            const auto& error = error_pf.future.get();
            REQUIRE(!error.is_fatal);
            REQUIRE(error.status == ErrorCodes::SyncCompensatingWrite);
            REQUIRE(error.compensating_writes_info.size() == invalids_to_be_compensated);
            REQUIRE_THAT(error.compensating_writes_info[0].reason,
                         Catch::Matchers::ContainsSubstring("in table \"restaurant\" will corrupt geojson data"));
            REQUIRE_THAT(error.compensating_writes_info[1].reason,
                         Catch::Matchers::ContainsSubstring("in table \"restaurant\" will corrupt geojson data"));

            {
                auto table = realm->read_group().get_table("class_restaurant");
                CHECK(table->size() == points.size());
                Obj obj = table->get_object_with_primary_key(Mixed{1});
                REQUIRE(obj);
                Geospatial geo = obj.get<Geospatial>("location");
                REQUIRE(geo.get_type_string() == "Point");
                REQUIRE(geo.get_type() == Geospatial::Type::Point);
                GeoPoint point = geo.get<GeoPoint>();
                REQUIRE(point.longitude == points[0].longitude);
                REQUIRE(point.latitude == points[0].latitude);
                REQUIRE(!point.get_altitude());
                ColKey location_col = table->get_column_key("location");
                auto run_query_locally = [&table, &location_col](Geospatial bounds) -> size_t {
                    Query query = table->column<Link>(location_col).geo_within(Geospatial(bounds));
                    return query.find_all().size();
                };
                auto run_query_as_flx = [&](Geospatial bounds) -> size_t {
                    size_t num_objects = 0;
                    harness->do_with_new_realm([&](SharedRealm realm) {
                        auto subs =
                            create_subscription(realm, "class_restaurant", "location", [&](Query q, ColKey c) {
                                return q.get_table()->column<Link>(c).geo_within(Geospatial(bounds));
                            });
                        auto sub_res =
                            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
                        CHECK(sub_res.is_ok());
                        CHECK(realm->get_active_subscription_set().version() == 1);
                        realm->refresh();
                        num_objects = realm->get_class("restaurant").num_objects();
                    });
                    return num_objects;
                };

                reset_utils::wait_for_num_objects_in_atlas(harness->app()->current_user(),
                                                           harness->session().app_session(), "restaurant",
                                                           points.size() - invalids_to_be_compensated);

                {
                    GeoPolygon bounds{
                        {{GeoPoint{-80, 40.7128}, GeoPoint{20, 60}, GeoPoint{20, 20}, GeoPoint{-80, 40.7128}}}};
                    size_t local_matches = run_query_locally(bounds);
                    size_t server_results = run_query_on_server(make_polygon_filter(bounds));
                    size_t flx_results = run_query_as_flx(bounds);
                    CHECK(flx_results == local_matches);
                    CHECK(server_results == local_matches);
                }
                {
                    GeoCircle circle{.5, GeoPoint{0, 90}};
                    size_t local_matches = run_query_locally(circle);
                    size_t server_results = run_query_on_server(make_circle_filter(circle));
                    size_t flx_results = run_query_as_flx(circle);
                    CHECK(server_results == local_matches);
                    CHECK(flx_results == local_matches);
                }
                { // a ring with 3 points without a matching begin/end is an error
                    GeoPolygon open_bounds{{{GeoPoint{-80, 40.7128}, GeoPoint{20, 60}, GeoPoint{20, 20}}}};
                    CHECK_THROWS_WITH(run_query_locally(open_bounds),
                                      "Invalid region in GEOWITHIN query for parameter 'GeoPolygon({[-80, 40.7128], "
                                      "[20, 60], [20, 20]})': 'Ring is not closed, first vertex 'GeoPoint([-80, "
                                      "40.7128])' does not equal last vertex 'GeoPoint([20, 20])''");
                    run_query_on_server(make_polygon_filter(open_bounds), "(BadValue) Loop is not closed");
                }
                {
                    GeoCircle circle = GeoCircle::from_kms(10, GeoPoint{-180.1, -90.1});
                    CHECK_THROWS_WITH(run_query_locally(circle),
                                      "Invalid region in GEOWITHIN query for parameter 'GeoCircle([-180.1, -90.1], "
                                      "0.00156787)': 'Longitude/latitude is out of bounds, lng: -180.1 lat: -90.1'");
                    run_query_on_server(make_circle_filter(circle), "(BadValue) longitude/latitude is out of bounds");
                }
                {
                    GeoCircle circle = GeoCircle::from_kms(-1, GeoPoint{0, 0});
                    CHECK_THROWS_WITH(run_query_locally(circle),
                                      "Invalid region in GEOWITHIN query for parameter 'GeoCircle([0, 0], "
                                      "-0.000156787)': 'The radius of a circle must be a non-negative number'");
                    run_query_on_server(make_circle_filter(circle),
                                        "(BadValue) radius must be a non-negative number");
                }
                {
                    // This box is from Gershøj to CPH airport. It includes CPH and Ragnarok but not NYC.
                    std::vector<Geospatial> valid_box_variations = {
                        GeoBox{GeoPoint{11.97575, 55.71601},
                               GeoPoint{12.64773, 55.61211}}, // Gershøj, CPH Airport (Top Left, Bottom Right)
                        GeoBox{GeoPoint{12.64773, 55.61211},
                               GeoPoint{11.97575, 55.71601}}, // CPH Airport, Gershøj (Bottom Right, Top Left)
                        GeoBox{GeoPoint{12.64773, 55.71601},
                               GeoPoint{11.97575, 55.61211}}, // Upper Right, Bottom Left
                        GeoBox{GeoPoint{11.97575, 55.61211},
                               GeoPoint{12.64773, 55.71601}}, // Bottom Left, Upper Right
                    };
                    constexpr size_t expected_results = 2;
                    for (auto& geo : valid_box_variations) {
                        size_t local_matches = run_query_locally(geo);
                        size_t server_matches =
                            run_query_on_server(make_polygon_filter(geo.get<GeoBox>().to_polygon()));
                        size_t flx_matches = run_query_as_flx(geo);
                        CHECK(local_matches == expected_results);
                        CHECK(server_matches == expected_results);
                        CHECK(flx_matches == expected_results);
                    }
                    std::vector<Geospatial> invalid_boxes = {
                        GeoBox{GeoPoint{11.97575, 55.71601}, GeoPoint{11.97575, 55.71601}}, // same point twice
                        GeoBox{GeoPoint{11.97575, 55.71601},
                               GeoPoint{11.97575, 57.0}}, // two points on the same longitude
                        GeoBox{GeoPoint{11.97575, 55.71601},
                               GeoPoint{12, 55.71601}}, // two points on the same latitude
                    };
                    for (auto& geo : invalid_boxes) {
                        REQUIRE_THROWS_CONTAINING(run_query_locally(geo),
                                                  "Invalid region in GEOWITHIN query for parameter 'GeoPolygon");
                        run_query_on_server(make_polygon_filter(geo.get<GeoBox>().to_polygon()),
                                            "(BadValue) Loop must have at least 3 different vertices");
                    }
                }
                { // a box region that wraps the north pole. It contains the north pole point
                    // and two others, one each on distinct sides of the globe.
                    constexpr double lat = 82.83799;
                    Geospatial north_pole_box =
                        GeoPolygon{{{GeoPoint{-78.33951, lat}, GeoPoint{-90.33951, lat}, GeoPoint{90.33951, lat},
                                     GeoPoint{78.33951, lat}, GeoPoint{-78.33951, lat}}}};
                    constexpr size_t num_matching_points = 3;
                    size_t local_matches = run_query_locally(north_pole_box);
                    size_t server_matches =
                        run_query_on_server(make_polygon_filter(north_pole_box.get<GeoPolygon>()));
                    size_t flx_matches = run_query_as_flx(north_pole_box);
                    CHECK(local_matches == num_matching_points);
                    CHECK(server_matches == num_matching_points);
                    CHECK(flx_matches == num_matching_points);
                }
            }
        });
    }

    // Add new sections before this
    SECTION("teardown") {
        harness->app()->sync_manager()->wait_for_sessions_to_terminate();
        harness.reset();
    }
}
#endif // REALM_ENABLE_GEOSPATIAL

TEST_CASE("flx: interrupted bootstrap restarts/recovers on reconnect", "[sync][flx][bootstrap][baas]") {
    FLXSyncTestHarness harness("flx_bootstrap_reconnect", {g_large_array_schema, {"queryable_int_field"}});

    std::vector<ObjectId> obj_ids_at_end = fill_large_array_schema(harness);
    SyncTestFile interrupted_realm_config(harness.app()->current_user(), harness.schema(),
                                          SyncConfig::FLXSyncEnabled{});

    {
        auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
        Realm::Config config = interrupted_realm_config;
        config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
        auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
        config.sync_config->on_sync_client_event_hook =
            [promise = std::move(shared_promise), seen_version_one = false](std::weak_ptr<SyncSession> weak_session,
                                                                            const SyncClientHookData& data) mutable {
                if (data.event != SyncClientHookEvent::DownloadMessageReceived) {
                    return SyncClientHookAction::NoAction;
                }

                auto session = weak_session.lock();
                if (!session) {
                    return SyncClientHookAction::NoAction;
                }

                // If we haven't seen at least one download message for query version 1, then do nothing yet.
                if (data.query_version == 0 || (data.query_version == 1 && !std::exchange(seen_version_one, true))) {
                    return SyncClientHookAction::NoAction;
                }

                REQUIRE(data.query_version == 1);
                REQUIRE(data.batch_state == sync::DownloadBatchState::MoreToCome);
                auto latest_subs = session->get_flx_subscription_store()->get_latest();
                REQUIRE(latest_subs.version() == 1);
                REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping);

                session->close();
                promise->emplace_value();

                return SyncClientHookAction::TriggerReconnect;
            };

        auto realm = Realm::get_shared_realm(config);
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            auto table = realm->read_group().get_table("class_TopLevel");
            mut_subs.insert_or_assign(Query(table));
            mut_subs.commit();
        }

        interrupted.get();
        realm->sync_session()->shutdown_and_wait();
    }

    {
        // Verify that the file was fully closed
        auto empty = [](auto&) {};
        REQUIRE(DB::call_with_lock(interrupted_realm_config.path, empty));
    }

    {
        DBOptions options;
        options.encryption_key = test_util::crypt_key();
        auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path, options);
        auto sub_store = sync::SubscriptionStore::create(realm);
        auto version_info = sub_store->get_version_info();
        REQUIRE(version_info.active == 0);
        REQUIRE(version_info.latest == 1);
        auto latest_subs = sub_store->get_latest();
        REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping);
        REQUIRE(latest_subs.size() == 1);
        REQUIRE(latest_subs.at(0).object_class_name == "TopLevel");
    }

    auto realm = Realm::get_shared_realm(interrupted_realm_config);
    auto table = realm->read_group().get_table("class_TopLevel");
    realm->get_latest_subscription_set().get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    wait_for_advance(*realm);
    REQUIRE(table->size() == obj_ids_at_end.size());
    for (auto& id : obj_ids_at_end) {
        REQUIRE(table->find_primary_key(Mixed{id}));
    }

    auto active_subs = realm->get_active_subscription_set();
    auto latest_subs = realm->get_latest_subscription_set();
    REQUIRE(active_subs.version() == latest_subs.version());
    REQUIRE(active_subs.version() == int64_t(1));
}

TEST_CASE("flx: dev mode uploads schema before query change", "[sync][flx][query][baas]") {
    FLXSyncTestHarness::ServerSchema server_schema;
    auto default_schema = FLXSyncTestHarness::default_server_schema();
    server_schema.queryable_fields = default_schema.queryable_fields;
    server_schema.dev_mode_enabled = true;
    server_schema.schema = Schema{};

    FLXSyncTestHarness harness("flx_dev_mode", server_schema);
    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            // auto queryable_str_field = table->get_column_key("queryable_str_field");
            // auto queryable_int_field = table->get_column_key("queryable_int_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table));
            new_query.commit();

            CppContext c(realm);
            realm->begin_transaction();
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{{"_id", foo_obj_id},
                                            {"queryable_str_field", "foo"s},
                                            {"queryable_int_field", static_cast<int64_t>(5)},
                                            {"non_queryable_field", "non queryable 1"s}}));
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{{"_id", bar_obj_id},
                                            {"queryable_str_field", "bar"s},
                                            {"queryable_int_field", static_cast<int64_t>(10)},
                                            {"non_queryable_field", "non queryable 2"s}}));
            realm->commit_transaction();

            wait_for_upload(*realm);
        },
        default_schema.schema);

    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            auto queryable_int_field = table->get_column_key("queryable_int_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table).greater_equal(queryable_int_field, int64_t(5)));
            auto subs = new_query.commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_download(*realm);
            Results results(realm, table);

            realm->refresh();
            CHECK(results.size() == 2);
            CHECK(table->get_object_with_primary_key({foo_obj_id}).is_valid());
            CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
        },
        default_schema.schema);
}

// This is a test case for the server's fix for RCORE-969
TEST_CASE("flx: change-of-query history divergence", "[sync][flx][query][baas]") {
    FLXSyncTestHarness harness("flx_coq_divergence");

    // first we create an object on the server and upload it.
    auto foo_obj_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "created as initial data seed"s}}));
    });

    // Now create another realm and wait for it to be fully synchronized with bootstrap version zero. i.e.
    // our progress counters should be past the history entry containing the object created above.
    auto test_file_config = harness.make_test_file();
    auto realm = Realm::get_shared_realm(test_file_config);
    auto table = realm->read_group().get_table("class_TopLevel");
    auto queryable_str_field = table->get_column_key("queryable_str_field");

    realm->get_latest_subscription_set().get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    wait_for_upload(*realm);
    wait_for_download(*realm);

    // Now disconnect the sync session
    realm->sync_session()->pause();

    // And move the "foo" object created above into view and create a different diverging copy of it locally.
    auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
    mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
    auto subs = mut_subs.commit();

    realm->begin_transaction();
    CppContext c(realm);
    Object::create(c, realm, "TopLevel",
                   std::any(AnyDict{{"_id", foo_obj_id},
                                    {"queryable_str_field", "foo"s},
                                    {"queryable_int_field", static_cast<int64_t>(10)},
                                    {"non_queryable_field", "created locally"s}}));
    realm->commit_transaction();

    // Reconnect the sync session and wait for the subscription that moved "foo" into view to be fully synchronized.
    realm->sync_session()->resume();
    wait_for_upload(*realm);
    wait_for_download(*realm);
    subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

    wait_for_advance(*realm);

    // The bootstrap should have erase/re-created our object and we should have the version from the server
    // locally.
    auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any{foo_obj_id});
    REQUIRE(obj.get_obj().get<int64_t>("queryable_int_field") == 5);
    REQUIRE(obj.get_obj().get<StringData>("non_queryable_field") == "created as initial data seed");

    // Likewise, if we create a new realm and download all the objects, we should see the initial server version
    // in the new realm rather than the "created locally" one.
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);

        auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any{foo_obj_id});
        REQUIRE(obj.get_obj().get<int64_t>("queryable_int_field") == 5);
        REQUIRE(obj.get_obj().get<StringData>("non_queryable_field") == "created as initial data seed");
    });
}

TEST_CASE("flx: writes work offline", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("flx_offline_writes");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto sync_session = realm->sync_session();
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto queryable_int_field = table->get_column_key("queryable_int_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(Query(table));
        new_query.commit();

        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();

        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s}}));
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", bar_obj_id},
                                        {"queryable_str_field", "bar"s},
                                        {"queryable_int_field", static_cast<int64_t>(10)},
                                        {"non_queryable_field", "non queryable 2"s}}));
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);
        sync_session->pause();

        // Make it so the subscriptions only match the "foo" object
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            mut_subs.commit();
        }

        // Make foo so that it will match the next subscription update. This checks whether you can do
        // multiple subscription set updates offline and that the last one eventually takes effect when
        // you come back online and fully synchronize.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        // Update our subscriptions so that both foo/bar will be included
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            mut_subs.commit();
        }

        // Make foo out of view for the current subscription.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 0);
            realm->commit_transaction();
        }

        sync_session->resume();
        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        Results results(realm, table);
        CHECK(results.size() == 1);
        CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
    });
}

TEST_CASE("flx: writes work without waiting for sync", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("flx_offline_writes");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto queryable_int_field = table->get_column_key("queryable_int_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(Query(table));
        new_query.commit();

        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();

        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s}}));
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", bar_obj_id},
                                        {"queryable_str_field", "bar"s},
                                        {"queryable_int_field", static_cast<int64_t>(10)},
                                        {"non_queryable_field", "non queryable 2"s}}));
        realm->commit_transaction();

        wait_for_upload(*realm);

        // Make it so the subscriptions only match the "foo" object
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            mut_subs.commit();
        }

        // Make foo so that it will match the next subscription update. This checks whether you can do
        // multiple subscription set updates without waiting and that the last one eventually takes effect when
        // you fully synchronize.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        // Update our subscriptions so that both foo/bar will be included
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            mut_subs.commit();
        }

        // Make foo out-of-view for the current subscription.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 0);
            realm->commit_transaction();
        }

        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        Results results(realm, table);
        CHECK(results.size() == 1);
        Obj obj = results.get(0);
        CHECK(obj.get_primary_key().get_object_id() == bar_obj_id);
        CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
    });
}

TEST_CASE("flx: verify websocket protocol number and prefixes", "[sync][protocol]") {
    // Update the expected value whenever the protocol version is updated - this ensures
    // that the current protocol version does not change unexpectedly.
    REQUIRE(14 == sync::get_current_protocol_version());
    // This was updated in Protocol V8 to use '#' instead of '/' to support the Web SDK
    REQUIRE("com.mongodb.realm-sync#" == sync::get_pbs_websocket_protocol_prefix());
    REQUIRE("com.mongodb.realm-query-sync#" == sync::get_flx_websocket_protocol_prefix());
}

// TODO: remote-baas: This test fails consistently with Windows remote baas server - to be fixed in RCORE-1674
#ifndef _WIN32
TEST_CASE("flx: subscriptions persist after closing/reopening", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("flx_bad_query");
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        auto orig_realm = Realm::get_shared_realm(config);
        auto mut_subs = orig_realm->get_latest_subscription_set().make_mutable_copy();
        mut_subs.insert_or_assign(Query(orig_realm->read_group().get_table("class_TopLevel")));
        mut_subs.commit();
        orig_realm->close();
    }

    {
        auto new_realm = Realm::get_shared_realm(config);
        auto latest_subs = new_realm->get_latest_subscription_set();
        CHECK(latest_subs.size() == 1);
        latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    }
}
#endif

TEST_CASE("flx: no subscription store created for PBS app", "[sync][flx][baas]") {
    auto server_app_config = minimal_app_config("flx_connect_as_pbs", g_minimal_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app()->current_user(), bson::Bson{}, g_minimal_schema);

    auto realm = Realm::get_shared_realm(config);
    CHECK(!wait_for_download(*realm));
    CHECK(!wait_for_upload(*realm));

    CHECK(!realm->sync_session()->get_flx_subscription_store());

    CHECK_THROWS_AS(realm->get_active_subscription_set(), IllegalOperation);
    CHECK_THROWS_AS(realm->get_latest_subscription_set(), IllegalOperation);
}

TEST_CASE("flx: connect to FLX as PBS returns an error", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");
    SyncTestFile config(harness.app()->current_user(), bson::Bson{}, harness.schema());
    std::mutex sync_error_mutex;
    util::Optional<SyncError> sync_error;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) mutable {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        sync_error = std::move(error);
    };
    auto realm = Realm::get_shared_realm(config);
    timed_wait_for([&] {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        return static_cast<bool>(sync_error);
    });

    CHECK(sync_error->status == ErrorCodes::WrongSyncType);
    CHECK(sync_error->server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
}

TEST_CASE("flx: connect to FLX with partition value returns an error", "[sync][flx][protocol][baas]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    config.sync_config->partition_value = "\"foobar\"";

    REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                      "Cannot specify a partition value when flexible sync is enabled");
}

TEST_CASE("flx: connect to PBS as FLX returns an error", "[sync][flx][protocol][baas]") {
    auto server_app_config = minimal_app_config("flx_connect_as_pbs", g_minimal_schema);
    TestAppSession session(create_app(server_app_config));
    auto app = session.app();
    auto user = app->current_user();

    SyncTestFile config(user, g_minimal_schema, SyncConfig::FLXSyncEnabled{});

    std::mutex sync_error_mutex;
    util::Optional<SyncError> sync_error;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) mutable {
        std::lock_guard lk(sync_error_mutex);
        sync_error = std::move(error);
    };
    auto realm = Realm::get_shared_realm(config);
    timed_wait_for([&] {
        std::lock_guard lk(sync_error_mutex);
        return static_cast<bool>(sync_error);
    });

    CHECK(sync_error->status == ErrorCodes::WrongSyncType);
    CHECK(sync_error->server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
}

TEST_CASE("flx: commit subscription while refreshing the access token", "[sync][flx][token][baas]") {
    auto transport = std::make_shared<HookedTransport<>>();
    FLXSyncTestHarness harness("flx_wait_access_token2", FLXSyncTestHarness::default_server_schema(), transport);
    auto app = harness.app();
    std::shared_ptr<User> user = app->current_user();
    REQUIRE(user);
    REQUIRE(!user->access_token_refresh_required());
    // Set a bad access token, with an expired time. This will trigger a refresh initiated by the client.
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    using namespace std::chrono_literals;
    auto expires = std::chrono::system_clock::to_time_t(now - 30s);
    user->update_data_for_testing([&](UserData& data) {
        data.access_token = RealmJWT(encode_fake_jwt("fake_access_token", expires));
    });
    REQUIRE(user->access_token_refresh_required());

    bool seen_waiting_for_access_token = false;
    // Commit a subcription set while there is no sync session.
    // A session is created when the access token is refreshed.
    transport->request_hook = [&](const Request&) {
        auto user = app->current_user();
        REQUIRE(user);
        for (auto& session : app->sync_manager()->get_all_sessions_for(*user)) {
            if (session->state() == SyncSession::State::WaitingForAccessToken) {
                REQUIRE(!seen_waiting_for_access_token);
                seen_waiting_for_access_token = true;

                auto store = session->get_flx_subscription_store();
                REQUIRE(store);
                auto mut_subs = store->get_latest().make_mutable_copy();
                mut_subs.commit();
            }
        }
        return std::nullopt;
    };
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    // This triggers the token refresh.
    auto r = Realm::get_shared_realm(config);
    REQUIRE(seen_waiting_for_access_token);
}

TEST_CASE("flx: bootstrap batching prevents orphan documents", "[sync][flx][bootstrap][baas]") {
    struct NovelException : public std::exception {
        const char* what() const noexcept override
        {
            return "Oh no, a really weird exception happened!";
        }
    };

    FLXSyncTestHarness harness("flx_bootstrap_batching", {g_large_array_schema, {"queryable_int_field"}});

    std::vector<ObjectId> obj_ids_at_end = fill_large_array_schema(harness);
    SyncTestFile interrupted_realm_config(harness.app()->current_user(), harness.schema(),
                                          SyncConfig::FLXSyncEnabled{});

    auto check_interrupted_state = [&](const DBRef& realm) {
        auto tr = realm->start_read();
        auto top_level = tr->get_table("class_TopLevel");
        REQUIRE(top_level);
        REQUIRE(top_level->is_empty());

        auto sub_store = sync::SubscriptionStore::create(realm);
        auto version_info = sub_store->get_version_info();
        REQUIRE(version_info.latest == 1);
        REQUIRE(version_info.active == 0);
        auto latest_subs = sub_store->get_latest();
        REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping);
        REQUIRE(latest_subs.size() == 1);
        REQUIRE(latest_subs.at(0).object_class_name == "TopLevel");
    };

    auto peek_pending_state = [](const DBRef& db) {
        auto logger = util::Logger::get_default_logger();
        sync::PendingBootstrapStore bootstrap_store(db, *logger, nullptr);
        REQUIRE(bootstrap_store.has_pending());
        return bootstrap_store.peek_pending(*db->start_read(), 1024 * 1024 * 16);
    };

    auto mutate_realm = [&] {
        harness.load_initial_data([&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            Results res(realm, Query(table).greater(table->get_column_key("queryable_int_field"), int64_t(10)));
            REQUIRE(res.size() == 2);
            res.clear();
        });
    };

    SECTION("unknown exception occurs during bootstrap application on session startup") {
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
            config.sync_config->on_sync_client_event_hook =
                [promise = std::move(shared_promise)](std::weak_ptr<SyncSession> weak_session,
                                                      const SyncClientHookData& data) mutable {
                    if (data.event != SyncClientHookEvent::BootstrapMessageProcessed) {
                        return SyncClientHookAction::NoAction;
                    }
                    auto session = weak_session.lock();
                    if (!session) {
                        return SyncClientHookAction::NoAction;
                    }

                    if (data.query_version == 1 && data.batch_state == sync::DownloadBatchState::LastInBatch) {
                        session->close();
                        promise->emplace_value();
                        return SyncClientHookAction::EarlyReturn;
                    }
                    return SyncClientHookAction::NoAction;
                };
            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                mut_subs.commit();
            }

            interrupted.get();
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(interrupted_realm_config.path));

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            DBOptions options;
            options.encryption_key = test_util::crypt_key();
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path, options);
            auto pending_batch = peek_pending_state(realm);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(pending_batch.progress);

            check_interrupted_state(realm);
        }

        auto error_pf = util::make_promise_future<SyncError>();
        interrupted_realm_config.sync_config->error_handler =
            [promise = std::make_shared<util::Promise<SyncError>>(std::move(error_pf.promise))](
                std::shared_ptr<SyncSession>, SyncError error) {
                promise->emplace_value(std::move(error));
            };

        interrupted_realm_config.sync_config->on_sync_client_event_hook =
            [&, download_message_received = false](std::weak_ptr<SyncSession>,
                                                   const SyncClientHookData& data) mutable {
                if (data.event == SyncClientHookEvent::DownloadMessageReceived) {
                    download_message_received = true;
                }
                if (data.event != SyncClientHookEvent::BootstrapBatchAboutToProcess) {
                    return SyncClientHookAction::NoAction;
                }

                REQUIRE(!download_message_received);
                throw NovelException{};
                return SyncClientHookAction::NoAction;
            };

        auto realm = Realm::get_shared_realm(interrupted_realm_config);
        const auto& error = error_pf.future.get();
        REQUIRE(!error.is_fatal);
        REQUIRE(error.server_requests_action == sync::ProtocolErrorInfo::Action::Warning);
        REQUIRE(error.status == ErrorCodes::UnknownError);
        REQUIRE_THAT(error.status.reason(),
                     Catch::Matchers::ContainsSubstring("Oh no, a really weird exception happened!"));
    }

    SECTION("exception occurs during bootstrap application") {
        Status error_status(ErrorCodes::OutOfMemory, "no more memory!");
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_session,
                                                                const SyncClientHookData& data) mutable {
                if (data.event != SyncClientHookEvent::BootstrapBatchAboutToProcess) {
                    return SyncClientHookAction::NoAction;
                }
                auto session = weak_session.lock();
                if (!session) {
                    return SyncClientHookAction::NoAction;
                }

                if (data.query_version == 1 && data.batch_state == sync::DownloadBatchState::MoreToCome) {
                    throw sync::IntegrationException(error_status);
                }
                return SyncClientHookAction::NoAction;
            };
            auto error_pf = util::make_promise_future<SyncError>();
            config.sync_config->error_handler =
                [promise = std::make_shared<util::Promise<SyncError>>(std::move(error_pf.promise))](
                    std::shared_ptr<SyncSession>, SyncError error) {
                    promise->emplace_value(std::move(error));
                };


            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                mut_subs.commit();
            }

            auto error = error_pf.future.get();
            REQUIRE(error.status.reason() == error_status.reason());
            REQUIRE(error.status == error_status);
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(interrupted_realm_config.path));

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            DBOptions options;
            options.encryption_key = test_util::crypt_key();
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path, options);
            auto pending_batch = peek_pending_state(realm);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(pending_batch.progress);

            check_interrupted_state(realm);
        }

        auto realm = Realm::get_shared_realm(interrupted_realm_config);
        auto table = realm->read_group().get_table("class_TopLevel");
        realm->get_latest_subscription_set()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get();

        wait_for_advance(*realm);

        REQUIRE(table->size() == obj_ids_at_end.size());
        for (auto& id : obj_ids_at_end) {
            REQUIRE(table->find_primary_key(Mixed{id}));
        }
    }

    SECTION("interrupted before final bootstrap message") {
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
            config.sync_config->on_sync_client_event_hook =
                [promise = std::move(shared_promise)](std::weak_ptr<SyncSession> weak_session,
                                                      const SyncClientHookData& data) mutable {
                    if (data.event != SyncClientHookEvent::BootstrapMessageProcessed) {
                        return SyncClientHookAction::NoAction;
                    }
                    auto session = weak_session.lock();
                    if (!session) {
                        return SyncClientHookAction::NoAction;
                    }

                    if (data.query_version == 1 && data.batch_state == sync::DownloadBatchState::MoreToCome) {
                        session->force_close();
                        promise->emplace_value();
                        return SyncClientHookAction::TriggerReconnect;
                    }
                    return SyncClientHookAction::NoAction;
                };
            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                mut_subs.commit();
            }

            interrupted.get();
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(interrupted_realm_config.path));

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            DBOptions options;
            options.encryption_key = test_util::crypt_key();
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path, options);
            auto pending_batch = peek_pending_state(realm);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(!pending_batch.progress);
            REQUIRE(pending_batch.remaining_changesets == 0);
            REQUIRE(pending_batch.changesets.size() == 1);

            check_interrupted_state(realm);
        }

        // Now we'll open a different realm and make some changes that would leave orphan objects on the client
        // if the bootstrap batches weren't being cached until lastInBatch were true.
        mutate_realm();

        // Finally re-open the realm whose bootstrap we interrupted and just wait for it to finish downloading.
        auto realm = Realm::get_shared_realm(interrupted_realm_config);
        auto table = realm->read_group().get_table("class_TopLevel");
        realm->get_latest_subscription_set()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get();

        wait_for_advance(*realm);
        auto expected_obj_ids = util::Span<ObjectId>(obj_ids_at_end).sub_span(0, 3);

        REQUIRE(table->size() == expected_obj_ids.size());
        for (auto& id : expected_obj_ids) {
            REQUIRE(table->find_primary_key(Mixed{id}));
        }
    }

    SECTION("interrupted after final bootstrap message before processing") {
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
            config.sync_config->on_sync_client_event_hook =
                [promise = std::move(shared_promise)](std::weak_ptr<SyncSession> weak_session,
                                                      const SyncClientHookData& data) mutable {
                    if (data.event != SyncClientHookEvent::BootstrapMessageProcessed) {
                        return SyncClientHookAction::NoAction;
                    }
                    auto session = weak_session.lock();
                    if (!session) {
                        return SyncClientHookAction::NoAction;
                    }

                    if (data.query_version == 1 && data.batch_state == sync::DownloadBatchState::LastInBatch) {
                        session->force_close();
                        promise->emplace_value();
                        return SyncClientHookAction::TriggerReconnect;
                    }
                    return SyncClientHookAction::NoAction;
                };
            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                mut_subs.commit();
            }

            interrupted.get();
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(interrupted_realm_config.path));

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            DBOptions options;
            options.encryption_key = test_util::crypt_key();
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path, options);
            auto pending_batch = peek_pending_state(realm);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(static_cast<bool>(pending_batch.progress));
            REQUIRE(pending_batch.remaining_changesets == 0);
            REQUIRE(pending_batch.changesets.size() == 6);

            check_interrupted_state(realm);
        }

        // Now we'll open a different realm and make some changes that would leave orphan objects on the client
        // if the bootstrap batches weren't being cached until lastInBatch were true.
        mutate_realm();

        auto [saw_valid_state_promise, saw_valid_state_future] = util::make_promise_future<void>();
        auto shared_saw_valid_state_promise =
            std::make_shared<decltype(saw_valid_state_promise)>(std::move(saw_valid_state_promise));
        // This hook will let us check what the state of the realm is before it's integrated any new download
        // messages from the server. This should be the full 5 object bootstrap that was received before we
        // called mutate_realm().
        interrupted_realm_config.sync_config->on_sync_client_event_hook =
            [&, promise = std::move(shared_saw_valid_state_promise)](std::weak_ptr<SyncSession> weak_session,
                                                                     const SyncClientHookData& data) {
                if (data.event != SyncClientHookEvent::DownloadMessageReceived) {
                    return SyncClientHookAction::NoAction;
                }
                auto session = weak_session.lock();
                if (!session) {
                    return SyncClientHookAction::NoAction;
                }

                if (data.query_version != 1 || data.batch_state == sync::DownloadBatchState::MoreToCome) {
                    return SyncClientHookAction::NoAction;
                }

                auto latest_sub_set = session->get_flx_subscription_store()->get_latest();
                auto active_sub_set = session->get_flx_subscription_store()->get_active();
                auto version_info = session->get_flx_subscription_store()->get_version_info();
                REQUIRE(version_info.pending_mark == active_sub_set.version());
                REQUIRE(version_info.active == active_sub_set.version());
                REQUIRE(version_info.latest == latest_sub_set.version());
                REQUIRE(latest_sub_set.version() == active_sub_set.version());
                REQUIRE(active_sub_set.state() == sync::SubscriptionSet::State::AwaitingMark);

                auto db = SyncSession::OnlyForTesting::get_db(*session);
                auto tr = db->start_read();

                auto table = tr->get_table("class_TopLevel");
                REQUIRE(table->size() == obj_ids_at_end.size());
                for (auto& id : obj_ids_at_end) {
                    REQUIRE(table->find_primary_key(Mixed{id}));
                }

                promise->emplace_value();
                return SyncClientHookAction::NoAction;
            };

        // Finally re-open the realm whose bootstrap we interrupted and just wait for it to finish downloading.
        auto realm = Realm::get_shared_realm(interrupted_realm_config);
        saw_valid_state_future.get();
        auto table = realm->read_group().get_table("class_TopLevel");
        realm->get_latest_subscription_set()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get();

        wait_for_advance(*realm);
        auto expected_obj_ids = util::Span<ObjectId>(obj_ids_at_end).sub_span(0, 3);

        // After we've downloaded all the mutations there should only by 3 objects left.
        REQUIRE(table->size() == expected_obj_ids.size());
        for (auto& id : expected_obj_ids) {
            REQUIRE(table->find_primary_key(Mixed{id}));
        }
    }
}

// Check that a document with the given id is present and has the expected fields
static void check_document(const std::vector<bson::BsonDocument>& documents, ObjectId id,
                           std::initializer_list<std::pair<const char*, bson::Bson>> fields)
{
    auto it = std::find_if(documents.begin(), documents.end(), [&](auto&& doc) {
        auto val = doc.find("_id");
        REQUIRE(val);
        return *val == id;
    });
    REQUIRE(it != documents.end());
    auto& doc = *it;
    for (auto& [name, expected_value] : fields) {
        auto val = doc.find(name);
        REQUIRE(val);

        // bson documents are ordered  but Realm dictionaries aren't, so the
        // document might validly be in a different order than we expected and
        // we need to do a comparison that doesn't check order.
        if (expected_value.type() == bson::Bson::Type::Document) {
            REQUIRE(static_cast<const bson::BsonDocument&>(*val) ==
                    static_cast<const bson::BsonDocument&>(expected_value));
        }
        else {
            REQUIRE(*val == expected_value);
        }
    }
}

TEST_CASE("flx: data ingest", "[sync][flx][data ingest][baas]") {
    using namespace ::realm::bson;

    static auto server_schema = [] {
        FLXSyncTestHarness::ServerSchema server_schema;
        server_schema.queryable_fields = {"queryable_str_field"};
        server_schema.schema = {
            {"Asymmetric",
             ObjectSchema::ObjectType::TopLevelAsymmetric,
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"location", PropertyType::String | PropertyType::Nullable},
                 {"embedded obj", PropertyType::Object | PropertyType::Nullable, "Embedded"},
                 {"embedded list", PropertyType::Object | PropertyType::Array, "Embedded"},
                 {"embedded dictionary", PropertyType::Object | PropertyType::Nullable | PropertyType::Dictionary,
                  "Embedded"},
                 {"link obj", PropertyType::Object | PropertyType::Nullable, "TopLevel"},
                 {"link list", PropertyType::Object | PropertyType::Array, "TopLevel"},
                 {"link dictionary", PropertyType::Object | PropertyType::Nullable | PropertyType::Dictionary,
                  "TopLevel"},
             }},
            {"Embedded", ObjectSchema::ObjectType::Embedded, {{"value", PropertyType::String}}},
            {"TopLevel",
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"value", PropertyType::Int},
             }},
        };
        return server_schema;
    }();
    static auto harness = std::make_unique<FLXSyncTestHarness>("asymmetric_sync", server_schema);

    // We reuse a single app for each section, so tests will see the documents
    // created by previous tests and we need to add those documents to the count
    // we're waiting for
    static std::unordered_map<std::string, size_t> previous_count;
    auto get_documents = [&](const char* name, size_t expected_count) {
        auto& count = previous_count[name];
        auto documents =
            harness->session().get_documents(*harness->app()->current_user(), name, count + expected_count);
        count = documents.size();
        return documents;
    };

    SECTION("basic object construction") {
        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();
        harness->do_with_new_realm([&](SharedRealm realm) {
            realm->begin_transaction();
            CppContext c(realm);
            Object::create(c, realm, "Asymmetric", std::any(AnyDict{{"_id", foo_obj_id}, {"location", "foo"s}}));
            Object::create(c, realm, "Asymmetric", std::any(AnyDict{{"_id", bar_obj_id}, {"location", "bar"s}}));
            realm->commit_transaction();

            auto documents = get_documents("Asymmetric", 2);
            check_document(documents, foo_obj_id, {{"location", "foo"}});
            check_document(documents, bar_obj_id, {{"location", "bar"}});
        });

        harness->do_with_new_realm([&](SharedRealm realm) {
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
            // Cannot query asymmetric tables.
            CHECK_THROWS_AS(Query(table), LogicError);
        });
    }

    SECTION("do not allow objects with same key within the same transaction") {
        auto foo_obj_id = ObjectId::gen();
        harness->do_with_new_realm([&](SharedRealm realm) {
            realm->begin_transaction();
            CppContext c(realm);
            Object::create(c, realm, "Asymmetric", std::any(AnyDict{{"_id", foo_obj_id}, {"location", "foo"s}}));
            CHECK_THROWS_WITH(
                Object::create(c, realm, "Asymmetric", std::any(AnyDict{{"_id", foo_obj_id}, {"location", "bar"s}})),
                "Attempting to create an object of type 'Asymmetric' with an existing primary key value 'not "
                "implemented'");
            realm->commit_transaction();

            auto documents = get_documents("Asymmetric", 1);
            check_document(documents, foo_obj_id, {{"location", "foo"}});
        });
    }

    SECTION("create multiple objects - separate commits") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            std::vector<ObjectId> obj_ids;
            for (int i = 0; i < 100; ++i) {
                realm->begin_transaction();
                obj_ids.push_back(ObjectId::gen());
                Object::create(c, realm, "Asymmetric",
                               std::any(AnyDict{
                                   {"_id", obj_ids.back()},
                                   {"location", util::format("foo_%1", i)},
                               }));
                realm->commit_transaction();
            }

            wait_for_upload(*realm);
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);

            auto documents = get_documents("Asymmetric", 100);
            for (int i = 0; i < 100; ++i) {
                check_document(documents, obj_ids[i], {{"location", util::format("foo_%1", i)}});
            }
        });
    }

    SECTION("create multiple objects - same commit") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            realm->begin_transaction();
            std::vector<ObjectId> obj_ids;
            for (int i = 0; i < 100; ++i) {
                obj_ids.push_back(ObjectId::gen());
                Object::create(c, realm, "Asymmetric",
                               std::any(AnyDict{
                                   {"_id", obj_ids.back()},
                                   {"location", util::format("bar_%1", i)},
                               }));
            }
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);

            auto documents = get_documents("Asymmetric", 100);
            for (int i = 0; i < 100; ++i) {
                check_document(documents, obj_ids[i], {{"location", util::format("bar_%1", i)}});
            }
        });
    }

    SECTION("open with schema mismatch on IsAsymmetric") {
        auto schema = server_schema.schema;
        schema.find("Asymmetric")->table_type = ObjectSchema::ObjectType::TopLevel;

        harness->do_with_new_user([&](std::shared_ptr<SyncUser> user) {
            SyncTestFile config(user, schema, SyncConfig::FLXSyncEnabled{});
            auto [error_promise, error_future] = util::make_promise_future<SyncError>();
            auto error_count = 0;
            auto err_handler = [promise = util::CopyablePromiseHolder(std::move(error_promise)),
                                &error_count](std::shared_ptr<SyncSession>, SyncError err) mutable {
                ++error_count;
                if (error_count == 1) {
                    // Bad changeset detected by the client.
                    CHECK(err.status == ErrorCodes::BadChangeset);
                }
                else if (error_count == 2) {
                    // Server asking for a client reset.
                    CHECK(err.status == ErrorCodes::SyncClientResetRequired);
                    CHECK(err.is_client_reset_requested());
                    promise.get_promise().emplace_value(std::move(err));
                }
            };

            config.sync_config->error_handler = err_handler;
            auto realm = Realm::get_shared_realm(config);

            auto err = error_future.get();
            CHECK(error_count == 2);
        });
    }

    SECTION("basic embedded object construction") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            auto obj_id = ObjectId::gen();
            realm->begin_transaction();
            CppContext c(realm);
            Object::create(c, realm, "Asymmetric",
                           std::any(AnyDict{
                               {"_id", obj_id},
                               {"embedded obj", AnyDict{{"value", "foo"s}}},
                           }));
            realm->commit_transaction();
            wait_for_upload(*realm);

            auto documents = get_documents("Asymmetric", 1);
            check_document(documents, obj_id, {{"embedded obj", BsonDocument{{"value", "foo"}}}});
        });

        harness->do_with_new_realm([&](SharedRealm realm) {
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
        });
    }

    SECTION("replace embedded object") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            auto foo_obj_id = ObjectId::gen();

            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric",
                           std::any(AnyDict{
                               {"_id", foo_obj_id},
                               {"embedded obj", AnyDict{{"value", "foo"s}}},
                           }));
            realm->commit_transaction();

            // Update embedded field to `null`. The server discards this write
            // as asymmetric sync can only create new objects.
            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric",
                           std::any(AnyDict{
                               {"_id", foo_obj_id},
                               {"embedded obj", std::any()},
                           }));
            realm->commit_transaction();

            // create a second object so that we can know when the translator
            // has processed everything
            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric", std::any(AnyDict{{"_id", ObjectId::gen()}, {}}));
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);

            auto documents = get_documents("Asymmetric", 2);
            check_document(documents, foo_obj_id, {{"embedded obj", BsonDocument{{"value", "foo"}}}});
        });
    }

    SECTION("embedded collections") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            auto obj_id = ObjectId::gen();

            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric",
                           std::any(AnyDict{
                               {"_id", obj_id},
                               {"embedded list", AnyVector{AnyDict{{"value", "foo"s}}, AnyDict{{"value", "bar"s}}}},
                               {"embedded dictionary",
                                AnyDict{
                                    {"key1", AnyDict{{"value", "foo"s}}},
                                    {"key2", AnyDict{{"value", "bar"s}}},
                                }},
                           }));
            realm->commit_transaction();

            auto documents = get_documents("Asymmetric", 1);
            check_document(
                documents, obj_id,
                {
                    {"embedded list", BsonArray{BsonDocument{{"value", "foo"}}, BsonDocument{{"value", "bar"}}}},
                    {"embedded dictionary",
                     BsonDocument{
                         {"key1", BsonDocument{{"value", "foo"}}},
                         {"key2", BsonDocument{{"value", "bar"}}},
                     }},
                });
        });
    }

    SECTION("asymmetric table not allowed in PBS") {
        Schema schema{
            {"Asymmetric2",
             ObjectSchema::ObjectType::TopLevelAsymmetric,
             {
                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                 {"location", PropertyType::Int},
                 {"reading", PropertyType::Int},
             }},
        };

        SyncTestFile config(harness->app()->current_user(), Bson{}, schema);
        REQUIRE_EXCEPTION(
            Realm::get_shared_realm(config), SchemaValidationFailed,
            Catch::Matchers::ContainsSubstring("Asymmetric table 'Asymmetric2' not allowed in partition based sync"));
    }

    SECTION("links to top-level objects") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            subscribe_to_all_and_bootstrap(*realm);

            ObjectId obj_id = ObjectId::gen();
            std::array<ObjectId, 5> target_obj_ids;
            for (auto& id : target_obj_ids) {
                id = ObjectId::gen();
            }

            realm->begin_transaction();
            CppContext c(realm);
            Object::create(c, realm, "Asymmetric",
                           std::any(AnyDict{
                               {"_id", obj_id},
                               {"link obj", AnyDict{{"_id", target_obj_ids[0]}, {"value", INT64_C(10)}}},
                               {"link list",
                                AnyVector{
                                    AnyDict{{"_id", target_obj_ids[1]}, {"value", INT64_C(11)}},
                                    AnyDict{{"_id", target_obj_ids[2]}, {"value", INT64_C(12)}},
                                }},
                               {"link dictionary",
                                AnyDict{
                                    {"key1", AnyDict{{"_id", target_obj_ids[3]}, {"value", INT64_C(13)}}},
                                    {"key2", AnyDict{{"_id", target_obj_ids[4]}, {"value", INT64_C(14)}}},
                                }},
                           }));
            realm->commit_transaction();
            wait_for_upload(*realm);

            auto docs1 = get_documents("Asymmetric", 1);
            check_document(docs1, obj_id,
                           {{"link obj", target_obj_ids[0]},
                            {"link list", BsonArray{{target_obj_ids[1], target_obj_ids[2]}}},
                            {
                                "link dictionary",
                                BsonDocument{
                                    {"key1", target_obj_ids[3]},
                                    {"key2", target_obj_ids[4]},
                                },
                            }});

            auto docs2 = get_documents("TopLevel", 5);
            for (int64_t i = 0; i < 5; ++i) {
                check_document(docs2, target_obj_ids[i], {{"value", 10 + i}});
            }
        });
    }

    // Add any new test sections above this point

    SECTION("teardown") {
        harness.reset();
    }
}

TEST_CASE("flx: data ingest - dev mode", "[sync][flx][data ingest][baas]") {
    FLXSyncTestHarness::ServerSchema server_schema;
    server_schema.dev_mode_enabled = true;
    server_schema.schema = Schema{};

    auto schema = Schema{{"Asymmetric",
                          ObjectSchema::ObjectType::TopLevelAsymmetric,
                          {
                              {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                              {"location", PropertyType::String | PropertyType::Nullable},
                          }},
                         {"TopLevel",
                          {
                              {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                              {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                          }}};

    FLXSyncTestHarness harness("asymmetric_sync", server_schema);

    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();

    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            CppContext c(realm);
            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric", std::any(AnyDict{{"_id", foo_obj_id}, {"location", "foo"s}}));
            Object::create(c, realm, "Asymmetric", std::any(AnyDict{{"_id", bar_obj_id}, {"location", "bar"s}}));
            realm->commit_transaction();
            User* user = dynamic_cast<User*>(realm->config().sync_config->user.get());
            REALM_ASSERT(user);
            auto docs = harness.session().get_documents(*user, "Asymmetric", 2);
            check_document(docs, foo_obj_id, {{"location", "foo"}});
            check_document(docs, bar_obj_id, {{"location", "bar"}});
        },
        schema);
}

TEST_CASE("flx: data ingest - write not allowed", "[sync][flx][data ingest][baas]") {
    AppCreateConfig::ServiceRole role{"asymmetric_write_perms"};
    role.document_filters.write = false;

    Schema schema({
        {"Asymmetric",
         ObjectSchema::ObjectType::TopLevelAsymmetric,
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"location", PropertyType::String | PropertyType::Nullable},
             {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "Embedded"},
         }},
        {"Embedded",
         ObjectSchema::ObjectType::Embedded,
         {
             {"value", PropertyType::String | PropertyType::Nullable},
         }},
    });
    FLXSyncTestHarness::ServerSchema server_schema{schema, {}, {role}};
    FLXSyncTestHarness harness("asymmetric_sync", server_schema);

    auto error_received_pf = util::make_promise_future<void>();
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    config.sync_config->on_sync_client_event_hook =
        [promise = util::CopyablePromiseHolder(std::move(error_received_pf.promise))](
            std::weak_ptr<SyncSession> weak_session, const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }
            auto session = weak_session.lock();
            REQUIRE(session);

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);

            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return SyncClientHookAction::NoAction;
            }

            REQUIRE(error_code == sync::ProtocolError::write_not_allowed);
            REQUIRE_FALSE(data.error_info->compensating_write_server_version.has_value());
            REQUIRE_FALSE(data.error_info->compensating_writes.empty());
            promise.get_promise().emplace_value();

            return SyncClientHookAction::EarlyReturn;
        };

    auto realm = Realm::get_shared_realm(config);

    // Create an asymmetric object and upload it to the server.
    {
        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "Asymmetric",
                       std::any(AnyDict{{"_id", ObjectId::gen()}, {"embedded_obj", AnyDict{{"value", "foo"s}}}}));
        realm->commit_transaction();
    }

    error_received_pf.future.get();
    realm->close();
}

TEST_CASE("flx: send client error", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("flx_client_error");

    // An integration error is simulated while bootstrapping.
    // This results in the client sending an error message to the server.
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    config.sync_config->simulate_integration_error = true;

    auto [error_promise, error_future] = util::make_promise_future<SyncError>();
    auto error_count = 0;
    auto err_handler = [promise = util::CopyablePromiseHolder(std::move(error_promise)),
                        &error_count](std::shared_ptr<SyncSession>, SyncError err) mutable {
        ++error_count;
        if (error_count == 1) {
            // Bad changeset detected by the client.
            CHECK(err.status == ErrorCodes::BadChangeset);
        }
        else if (error_count == 2) {
            // Server asking for a client reset.
            CHECK(err.status == ErrorCodes::SyncClientResetRequired);
            CHECK(err.is_client_reset_requested());
            promise.get_promise().emplace_value(std::move(err));
        }
    };

    config.sync_config->error_handler = err_handler;
    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_TopLevel");
    auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
    new_query.insert_or_assign(Query(table));
    new_query.commit();

    auto err = error_future.get();
    CHECK(error_count == 2);
}

TEST_CASE("flx: bootstraps contain all changes", "[sync][flx][bootstrap][baas]") {
    FLXSyncTestHarness harness("bootstrap_full_sync");

    auto setup_subs = [](SharedRealm& realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.clear();
        auto col = table->get_column_key("queryable_str_field");
        new_query.insert_or_assign(Query(table).equal(col, StringData("bar")).Or().equal(col, StringData("bizz")));
        return new_query.commit();
    };

    auto bar_obj_id = ObjectId::gen();
    auto bizz_obj_id = ObjectId::gen();
    auto setup_and_poison_cache = [&] {
        harness.load_initial_data([&](SharedRealm realm) {
            CppContext c(realm);
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{{"_id", bar_obj_id},
                                            {"queryable_str_field", std::string{"bar"}},
                                            {"queryable_int_field", static_cast<int64_t>(10)},
                                            {"non_queryable_field", std::string{"non queryable 2"}}}));
        });

        harness.do_with_new_realm([&](SharedRealm realm) {
            // first set a subscription to force the creation/caching of a broker snapshot on the server.
            setup_subs(realm).get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_advance(*realm);
            auto table = realm->read_group().get_table("class_TopLevel");
            REQUIRE(table->find_primary_key(bar_obj_id));

            // Then create an object that won't be in the cached snapshot - this is the object that if we didn't
            // wait for a MARK message to come back, we'd miss it in our results.
            CppContext c(realm);
            realm->begin_transaction();
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{{"_id", bizz_obj_id},
                                            {"queryable_str_field", std::string{"bizz"}},
                                            {"queryable_int_field", static_cast<int64_t>(15)},
                                            {"non_queryable_field", std::string{"non queryable 3"}}}));
            realm->commit_transaction();
            wait_for_upload(*realm);
        });
    };

    SECTION("regular subscription change") {
        SyncTestFile triggered_config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
        std::atomic<bool> saw_truncated_bootstrap{false};
        triggered_config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_sess,
                                                                      const SyncClientHookData& data) {
            auto sess = weak_sess.lock();
            if (!sess || data.event != SyncClientHookEvent::BootstrapProcessed || data.query_version != 1) {
                return SyncClientHookAction::NoAction;
            }

            auto latest_subs = sess->get_flx_subscription_store()->get_latest();
            REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::AwaitingMark);
            REQUIRE(data.num_changesets == 1);
            auto db = SyncSession::OnlyForTesting::get_db(*sess);
            auto read_tr = db->start_read();
            auto table = read_tr->get_table("class_TopLevel");
            REQUIRE(table->find_primary_key(bar_obj_id));
            REQUIRE_FALSE(table->find_primary_key(bizz_obj_id));
            saw_truncated_bootstrap.store(true);

            return SyncClientHookAction::NoAction;
        };
        auto problem_realm = Realm::get_shared_realm(triggered_config);

        // Setup the problem realm by waiting for it to be fully synchronized with an empty query, so the router
        // on the server should have no new history entries, and then pause the router so it doesn't get any of
        // the changes we're about to create.
        wait_for_upload(*problem_realm);
        wait_for_download(*problem_realm);

        nlohmann::json command_request = {
            {"command", "PAUSE_ROUTER_SESSION"},
        };
        auto resp_body =
            SyncSession::OnlyForTesting::send_test_command(*problem_realm->sync_session(), command_request.dump())
                .get();
        REQUIRE(resp_body == "{}");

        // Put some data into the server, this will be the data that will be in the broker cache.
        setup_and_poison_cache();

        // Setup queries on the problem realm to bootstrap from the cached object. Bootstrapping will also resume
        // the router, so all we need to do is wait for the subscription set to be complete and notifications to be
        // processed.
        setup_subs(problem_realm).get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        wait_for_advance(*problem_realm);

        REQUIRE(saw_truncated_bootstrap.load());
        auto table = problem_realm->read_group().get_table("class_TopLevel");
        REQUIRE(table->find_primary_key(bar_obj_id));
        REQUIRE(table->find_primary_key(bizz_obj_id));
    }

// TODO: remote-baas: This test fails intermittently with Windows remote baas server - to be fixed in RCORE-1674
#ifndef _WIN32
    SECTION("disconnect between bootstrap and mark") {
        SyncTestFile triggered_config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
        auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
        triggered_config.sync_config->on_sync_client_event_hook =
            [promise = util::CopyablePromiseHolder(std::move(interrupted_promise)), &bizz_obj_id,
             &bar_obj_id](std::weak_ptr<SyncSession> weak_sess, const SyncClientHookData& data) mutable {
                auto sess = weak_sess.lock();
                if (!sess || data.event != SyncClientHookEvent::BootstrapProcessed || data.query_version != 1) {
                    return SyncClientHookAction::NoAction;
                }

                auto latest_subs = sess->get_flx_subscription_store()->get_latest();
                REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::AwaitingMark);
                REQUIRE(data.num_changesets == 1);
                auto db = SyncSession::OnlyForTesting::get_db(*sess);
                auto read_tr = db->start_read();
                auto table = read_tr->get_table("class_TopLevel");
                REQUIRE(table->find_primary_key(bar_obj_id));
                REQUIRE_FALSE(table->find_primary_key(bizz_obj_id));

                sess->pause();
                promise.get_promise().emplace_value();
                return SyncClientHookAction::NoAction;
            };
        auto problem_realm = Realm::get_shared_realm(triggered_config);

        // Setup the problem realm by waiting for it to be fully synchronized with an empty query, so the router
        // on the server should have no new history entries, and then pause the router so it doesn't get any of
        // the changes we're about to create.
        wait_for_upload(*problem_realm);
        wait_for_download(*problem_realm);

        nlohmann::json command_request = {
            {"command", "PAUSE_ROUTER_SESSION"},
        };
        auto resp_body =
            SyncSession::OnlyForTesting::send_test_command(*problem_realm->sync_session(), command_request.dump())
                .get();
        REQUIRE(resp_body == "{}");

        // Put some data into the server, this will be the data that will be in the broker cache.
        setup_and_poison_cache();

        // Setup queries on the problem realm to bootstrap from the cached object. Bootstrapping will also resume
        // the router, so all we need to do is wait for the subscription set to be complete and notifications to be
        // processed.
        auto sub_set = setup_subs(problem_realm);
        auto sub_complete_future = sub_set.get_state_change_notification(sync::SubscriptionSet::State::Complete);

        interrupted.get();
        problem_realm->sync_session()->shutdown_and_wait();
        REQUIRE(sub_complete_future.is_ready());
        sub_set.refresh();
        REQUIRE(sub_set.state() == sync::SubscriptionSet::State::AwaitingMark);

        sub_complete_future = sub_set.get_state_change_notification(sync::SubscriptionSet::State::Complete);
        problem_realm->sync_session()->resume();
        sub_complete_future.get();
        wait_for_advance(*problem_realm);

        sub_set.refresh();
        REQUIRE(sub_set.state() == sync::SubscriptionSet::State::Complete);
        auto table = problem_realm->read_group().get_table("class_TopLevel");
        REQUIRE(table->find_primary_key(bar_obj_id));
        REQUIRE(table->find_primary_key(bizz_obj_id));
    }
#endif
    SECTION("error/suspend between bootstrap and mark") {
        SyncTestFile triggered_config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
        triggered_config.sync_config->on_sync_client_event_hook =
            [&bizz_obj_id, &bar_obj_id](std::weak_ptr<SyncSession> weak_sess, const SyncClientHookData& data) {
                auto sess = weak_sess.lock();
                if (!sess || data.event != SyncClientHookEvent::BootstrapProcessed || data.query_version != 1) {
                    return SyncClientHookAction::NoAction;
                }

                auto latest_subs = sess->get_flx_subscription_store()->get_latest();
                REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::AwaitingMark);
                REQUIRE(data.num_changesets == 1);
                auto db = SyncSession::OnlyForTesting::get_db(*sess);
                auto read_tr = db->start_read();
                auto table = read_tr->get_table("class_TopLevel");
                REQUIRE(table->find_primary_key(bar_obj_id));
                REQUIRE_FALSE(table->find_primary_key(bizz_obj_id));

                return SyncClientHookAction::TriggerReconnect;
            };
        auto problem_realm = Realm::get_shared_realm(triggered_config);

        // Setup the problem realm by waiting for it to be fully synchronized with an empty query, so the router
        // on the server should have no new history entries, and then pause the router so it doesn't get any of
        // the changes we're about to create.
        wait_for_upload(*problem_realm);
        wait_for_download(*problem_realm);

        nlohmann::json command_request = {
            {"command", "PAUSE_ROUTER_SESSION"},
        };
        auto resp_body =
            SyncSession::OnlyForTesting::send_test_command(*problem_realm->sync_session(), command_request.dump())
                .get();
        REQUIRE(resp_body == "{}");

        // Put some data into the server, this will be the data that will be in the broker cache.
        setup_and_poison_cache();

        // Setup queries on the problem realm to bootstrap from the cached object. Bootstrapping will also resume
        // the router, so all we need to do is wait for the subscription set to be complete and notifications to be
        // processed.
        auto sub_set = setup_subs(problem_realm);
        auto sub_complete_future = sub_set.get_state_change_notification(sync::SubscriptionSet::State::Complete);

        sub_complete_future.get();
        wait_for_advance(*problem_realm);

        sub_set.refresh();
        REQUIRE(sub_set.state() == sync::SubscriptionSet::State::Complete);
        auto table = problem_realm->read_group().get_table("class_TopLevel");
        REQUIRE(table->find_primary_key(bar_obj_id));
        REQUIRE(table->find_primary_key(bizz_obj_id));
    }
}

TEST_CASE("flx: convert flx sync realm to bundled realm", "[app][flx][baas]") {
    static auto foo_obj_id = ObjectId::gen();
    static auto bar_obj_id = ObjectId::gen();
    static auto bizz_obj_id = ObjectId::gen();
    static std::optional<FLXSyncTestHarness> harness;
    if (!harness) {
        harness.emplace("bundled_flx_realms");
        harness->load_initial_data([&](SharedRealm realm) {
            CppContext c(realm);
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{{"_id", foo_obj_id},
                                            {"queryable_str_field", "foo"s},
                                            {"queryable_int_field", static_cast<int64_t>(5)},
                                            {"non_queryable_field", "non queryable 1"s}}));
            Object::create(c, realm, "TopLevel",
                           std::any(AnyDict{{"_id", bar_obj_id},
                                            {"queryable_str_field", "bar"s},
                                            {"queryable_int_field", static_cast<int64_t>(10)},
                                            {"non_queryable_field", "non queryable 2"s}}));
        });
    }

    SECTION("flx to flx (should succeed)") {
        create_user_and_log_in(harness->app());
        SyncTestFile target_config(harness->app()->current_user(), harness->schema(), SyncConfig::FLXSyncEnabled{});
        harness->do_with_new_realm([&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.insert_or_assign(Query(table).greater(table->get_column_key("queryable_int_field"), 5));
            auto subs = std::move(mut_subs).commit();

            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_advance(*realm);

            realm->convert(target_config);
        });

        auto target_realm = Realm::get_shared_realm(target_config);

        target_realm->begin_transaction();
        CppContext c(target_realm);
        Object::create(c, target_realm, "TopLevel",
                       std::any(AnyDict{{"_id", bizz_obj_id},
                                        {"queryable_str_field", "bizz"s},
                                        {"queryable_int_field", static_cast<int64_t>(15)},
                                        {"non_queryable_field", "non queryable 3"s}}));
        target_realm->commit_transaction();

        wait_for_upload(*target_realm);
        wait_for_download(*target_realm);

        auto latest_subs = target_realm->get_active_subscription_set();
        auto table = target_realm->read_group().get_table("class_TopLevel");
        REQUIRE(latest_subs.size() == 1);
        REQUIRE(latest_subs.at(0).object_class_name == "TopLevel");
        REQUIRE(latest_subs.at(0).query_string ==
                Query(table).greater(table->get_column_key("queryable_int_field"), 5).get_description());

        REQUIRE(table->size() == 2);
        REQUIRE(table->find_primary_key(bar_obj_id));
        REQUIRE(table->find_primary_key(bizz_obj_id));
        REQUIRE_FALSE(table->find_primary_key(foo_obj_id));
    }

    SECTION("flx to local (should succeed)") {
        TestFile target_config;

        harness->do_with_new_realm([&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.insert_or_assign(Query(table).greater(table->get_column_key("queryable_int_field"), 5));
            auto subs = std::move(mut_subs).commit();

            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_advance(*realm);

            target_config.schema = realm->schema();
            target_config.schema_version = realm->schema_version();
            realm->convert(target_config);
        });

        auto target_realm = Realm::get_shared_realm(target_config);
        REQUIRE_THROWS(target_realm->get_active_subscription_set());

        auto table = target_realm->read_group().get_table("class_TopLevel");
        REQUIRE(table->size() == 2);
        REQUIRE(table->find_primary_key(bar_obj_id));
        REQUIRE(table->find_primary_key(bizz_obj_id));
        REQUIRE_FALSE(table->find_primary_key(foo_obj_id));
    }

    SECTION("flx to pbs (should fail to convert)") {
        create_user_and_log_in(harness->app());
        SyncTestFile target_config(harness->app()->current_user(), "12345"s, harness->schema());
        harness->do_with_new_realm([&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.insert_or_assign(Query(table).greater(table->get_column_key("queryable_int_field"), 5));
            auto subs = std::move(mut_subs).commit();

            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_advance(*realm);

            REQUIRE_THROWS(realm->convert(target_config));
        });
    }

    SECTION("pbs to flx (should fail to convert)") {
        create_user_and_log_in(harness->app());
        SyncTestFile target_config(harness->app()->current_user(), harness->schema(), SyncConfig::FLXSyncEnabled{});

        auto pbs_app_config = minimal_app_config("pbs_to_flx_convert", harness->schema());

        TestAppSession pbs_app_session(create_app(pbs_app_config));
        SyncTestFile source_config(pbs_app_session.app()->current_user(), "54321"s, pbs_app_config.schema);
        auto realm = Realm::get_shared_realm(source_config);

        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s}}));
        realm->commit_transaction();

        REQUIRE_THROWS(realm->convert(target_config));
    }

    SECTION("local to flx (should fail to convert)") {
        TestFile source_config;
        source_config.schema = harness->schema();
        source_config.schema_version = 1;

        auto realm = Realm::get_shared_realm(source_config);
        auto foo_obj_id = ObjectId::gen();

        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s}}));
        realm->commit_transaction();

        create_user_and_log_in(harness->app());
        SyncTestFile target_config(harness->app()->current_user(), harness->schema(), SyncConfig::FLXSyncEnabled{});

        REQUIRE_THROWS(realm->convert(target_config));
    }

    // Add new sections before this
    SECTION("teardown") {
        harness->app()->sync_manager()->wait_for_sessions_to_terminate();
        harness.reset();
    }
}

TEST_CASE("flx: compensating write errors get re-sent across sessions", "[sync][flx][compensating write][baas]") {
    AppCreateConfig::ServiceRole role{"compensating_write_perms"};
    role.document_filters.write = {
        {"queryable_str_field", nlohmann::json{{"$in", nlohmann::json::array({"foo", "bar"})}}}};

    FLXSyncTestHarness::ServerSchema server_schema{
        g_simple_embedded_obj_schema, {"queryable_str_field", "queryable_int_field"}, {role}};
    FLXSyncTestHarness::Config harness_config("flx_bad_query", server_schema);
    harness_config.reconnect_mode = ReconnectMode::testing;
    FLXSyncTestHarness harness(std::move(harness_config));

    auto test_obj_id_1 = ObjectId::gen();
    auto test_obj_id_2 = ObjectId::gen();

    create_user_and_log_in(harness.app());
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        auto error_received_pf = util::make_promise_future<void>();
        config.sync_config->on_sync_client_event_hook =
            [promise = util::CopyablePromiseHolder(std::move(error_received_pf.promise))](
                std::weak_ptr<SyncSession> weak_session, const SyncClientHookData& data) mutable {
                if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                    return SyncClientHookAction::NoAction;
                }
                auto session = weak_session.lock();
                REQUIRE(session);

                auto error_code = sync::ProtocolError(data.error_info->raw_error_code);

                if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                    return SyncClientHookAction::NoAction;
                }

                REQUIRE(error_code == sync::ProtocolError::compensating_write);
                REQUIRE_FALSE(data.error_info->compensating_writes.empty());
                promise.get_promise().emplace_value();

                return SyncClientHookAction::TriggerReconnect;
            };

        auto realm = Realm::get_shared_realm(config);
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(Query(table).equal(queryable_str_field, "bizz"));
        std::move(new_query).commit();

        wait_for_upload(*realm);
        wait_for_download(*realm);

        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{
                           {"_id", test_obj_id_1},
                           {"queryable_str_field", std::string{"foo"}},
                       }));
        realm->commit_transaction();

        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{
                           {"_id", test_obj_id_2},
                           {"queryable_str_field", std::string{"baz"}},
                       }));
        realm->commit_transaction();

        error_received_pf.future.get();
        realm->sync_session()->shutdown_and_wait();
        config.sync_config->on_sync_client_event_hook = {};
    }

    _impl::RealmCoordinator::clear_all_caches();

    std::mutex errors_mutex;
    std::condition_variable new_compensating_write;
    std::vector<std::pair<ObjectId, sync::version_type>> error_to_download_version;
    std::vector<sync::CompensatingWriteErrorInfo> compensating_writes;
    sync::version_type download_version;

    config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_session,
                                                        const SyncClientHookData& data) mutable {
        auto session = weak_session.lock();
        if (!session) {
            return SyncClientHookAction::NoAction;
        }

        if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
            if (data.event == SyncClientHookEvent::DownloadMessageReceived) {
                download_version = data.progress.download.server_version;
            }

            return SyncClientHookAction::NoAction;
        }

        auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
        REQUIRE(error_code == sync::ProtocolError::compensating_write);
        REQUIRE(!data.error_info->compensating_writes.empty());
        std::lock_guard<std::mutex> lk(errors_mutex);
        for (const auto& compensating_write : data.error_info->compensating_writes) {
            error_to_download_version.emplace_back(compensating_write.primary_key.get_object_id(),
                                                   *data.error_info->compensating_write_server_version);
        }

        return SyncClientHookAction::NoAction;
    };

    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
        std::unique_lock<std::mutex> lk(errors_mutex);
        REQUIRE(error.status == ErrorCodes::SyncCompensatingWrite);
        for (const auto& compensating_write : error.compensating_writes_info) {
            auto tracked_error = std::find_if(error_to_download_version.begin(), error_to_download_version.end(),
                                              [&](const auto& pair) {
                                                  return pair.first == compensating_write.primary_key.get_object_id();
                                              });
            REQUIRE(tracked_error != error_to_download_version.end());
            CHECK(tracked_error->second <= download_version);
            compensating_writes.push_back(compensating_write);
        }
        new_compensating_write.notify_one();
    };

    auto realm = Realm::get_shared_realm(config);

    wait_for_upload(*realm);
    wait_for_download(*realm);

    std::unique_lock<std::mutex> lk(errors_mutex);
    new_compensating_write.wait_for(lk, std::chrono::seconds(30), [&] {
        return compensating_writes.size() == 2;
    });

    REQUIRE(compensating_writes.size() == 2);
    auto& write_info = compensating_writes[0];
    CHECK(write_info.primary_key.is_type(type_ObjectId));
    CHECK(write_info.primary_key.get_object_id() == test_obj_id_1);
    CHECK(write_info.object_name == "TopLevel");
    CHECK_THAT(write_info.reason, Catch::Matchers::ContainsSubstring("object is outside of the current query view"));

    write_info = compensating_writes[1];
    REQUIRE(write_info.primary_key.is_type(type_ObjectId));
    REQUIRE(write_info.primary_key.get_object_id() == test_obj_id_2);
    REQUIRE(write_info.object_name == "TopLevel");
    REQUIRE(write_info.reason ==
            util::format("write to ObjectID(\"%1\") in table \"TopLevel\" not allowed", test_obj_id_2));
    auto top_level_table = realm->read_group().get_table("class_TopLevel");
    REQUIRE(top_level_table->is_empty());
}

TEST_CASE("flx: compensating write errors are not duplicated", "[sync][flx][compensating write][baas]") {
    FLXSyncTestHarness harness("flx_compensating_writes");
    auto config = harness.make_test_file();

    auto test_obj_id_1 = ObjectId::gen();
    auto test_obj_id_2 = ObjectId::gen();

    enum class TestState { Start, FirstError, SecondError, Resume, ThirdError, FourthError };
    TestingStateMachine<TestState> state(TestState::Start);

    std::mutex errors_mutex;
    std::vector<std::pair<ObjectId, sync::version_type>> error_to_download_version;
    std::vector<sync::CompensatingWriteErrorInfo> compensating_writes;
    sync::version_type download_version;

    config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_session,
                                                        const SyncClientHookData& data) {
        if (auto session = weak_session.lock(); !session) {
            return SyncClientHookAction::NoAction;
        }
        SyncClientHookAction action = SyncClientHookAction::NoAction;
        state.transition_with([&](TestState cur_state) -> std::optional<TestState> {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                // Before the session is resumed, ignore the download messages received
                // to undo the out-of-view writes.
                if (data.event == SyncClientHookEvent::DownloadMessageReceived &&
                    (cur_state == TestState::FirstError || cur_state == TestState::SecondError)) {
                    action = SyncClientHookAction::EarlyReturn;
                }
                else if (data.event == SyncClientHookEvent::DownloadMessageReceived &&
                         (cur_state == TestState::Resume || cur_state == TestState::ThirdError)) {
                    download_version = data.progress.download.server_version;
                }
                else if (data.event == SyncClientHookEvent::BindMessageSent && cur_state == TestState::SecondError) {
                    return TestState::Resume;
                }
                return std::nullopt;
            }

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return std::nullopt;
            }

            REQUIRE(error_code == sync::ProtocolError::compensating_write);
            REQUIRE_FALSE(data.error_info->compensating_writes.empty());

            if (cur_state == TestState::Start) {
                return TestState::FirstError;
            }
            else if (cur_state == TestState::FirstError) {
                // Return early so the second compensating write error is not saved
                // by the sync client.
                // This is so server versions received to undo the out-of-view writes are
                // [x, x, y] instead of [x, y, x, y] (server versions don't increase
                // monotonically as the client expects).
                action = SyncClientHookAction::EarlyReturn;
                return TestState::SecondError;
            }
            // Save third and fourth compensating write errors after resume.
            std::lock_guard<std::mutex> lk(errors_mutex);
            for (const auto& compensating_write : data.error_info->compensating_writes) {
                error_to_download_version.emplace_back(compensating_write.primary_key.get_object_id(),
                                                       *data.error_info->compensating_write_server_version);
            }
            return std::nullopt;
        });
        return action;
    };

    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
        std::unique_lock<std::mutex> lk(errors_mutex);
        REQUIRE(error.status == ErrorCodes::SyncCompensatingWrite);
        for (const auto& compensating_write : error.compensating_writes_info) {
            auto tracked_error = std::find_if(error_to_download_version.begin(), error_to_download_version.end(),
                                              [&](const auto& pair) {
                                                  return pair.first == compensating_write.primary_key.get_object_id();
                                              });
            REQUIRE(tracked_error != error_to_download_version.end());
            CHECK(tracked_error->second <= download_version);
            compensating_writes.push_back(compensating_write);
        }
        lk.unlock();

        state.transition_with([&](TestState cur_state) -> std::optional<TestState> {
            if (cur_state == TestState::Resume) {
                return TestState::ThirdError;
            }
            else if (cur_state == TestState::ThirdError) {
                return TestState::FourthError;
            }
            return std::nullopt;
        });
    };

    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_TopLevel");
    auto queryable_str_field = table->get_column_key("queryable_str_field");
    auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
    new_query.insert_or_assign(Query(table).equal(queryable_str_field, "bizz"));
    std::move(new_query).commit();

    wait_for_upload(*realm);
    wait_for_download(*realm);

    CppContext c(realm);
    realm->begin_transaction();
    Object::create(c, realm, "TopLevel",
                   util::Any(AnyDict{
                       {"_id", test_obj_id_1},
                       {"queryable_str_field", std::string{"foo"}},
                   }));
    realm->commit_transaction();

    realm->begin_transaction();
    Object::create(c, realm, "TopLevel",
                   util::Any(AnyDict{
                       {"_id", test_obj_id_2},
                       {"queryable_str_field", std::string{"baz"}},
                   }));
    realm->commit_transaction();
    state.wait_for(TestState::SecondError);

    nlohmann::json error_body = {
        {"tryAgain", true},           {"message", "fake error"},
        {"shouldClientReset", false}, {"isRecoveryModeDisabled", false},
        {"action", "Transient"},
    };
    nlohmann::json test_command = {{"command", "ECHO_ERROR"},
                                   {"args", nlohmann::json{{"errorCode", 229}, {"errorBody", error_body}}}};

    // Trigger a retryable transient error to resume the session.
    auto test_cmd_res =
        wait_for_future(SyncSession::OnlyForTesting::send_test_command(*realm->sync_session(), test_command.dump()))
            .get();
    CHECK(test_cmd_res == "{}");
    state.wait_for(TestState::FourthError);

    REQUIRE(compensating_writes.size() == 2);
    auto& write_info = compensating_writes[0];
    CHECK(write_info.primary_key.is_type(type_ObjectId));
    CHECK(write_info.primary_key.get_object_id() == test_obj_id_1);
    CHECK(write_info.object_name == "TopLevel");
    CHECK(write_info.reason == util::format("write to ObjectID(\"%1\") in table \"TopLevel\" not allowed; object is "
                                            "outside of the current query view",
                                            test_obj_id_1));

    write_info = compensating_writes[1];
    CHECK(write_info.primary_key.is_type(type_ObjectId));
    CHECK(write_info.primary_key.get_object_id() == test_obj_id_2);
    CHECK(write_info.object_name == "TopLevel");
    CHECK(write_info.reason == util::format("write to ObjectID(\"%1\") in table \"TopLevel\" not allowed; object is "
                                            "outside of the current query view",
                                            test_obj_id_2));
    realm->refresh();
    auto top_level_table = realm->read_group().get_table("class_TopLevel");
    CHECK(top_level_table->is_empty());
}

TEST_CASE("flx: bootstrap changesets are applied continuously", "[sync][flx][bootstrap][baas]") {
    FLXSyncTestHarness harness("flx_bootstrap_ordering", {g_large_array_schema, {"queryable_int_field"}});
    fill_large_array_schema(harness);

    std::unique_ptr<std::thread> th;
    sync::version_type user_commit_version = UINT_FAST64_MAX;
    sync::version_type bootstrap_version = UINT_FAST64_MAX;
    SharedRealm realm;
    std::condition_variable cv;
    std::mutex mutex;
    bool allow_to_commit = false;

    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
    auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
    config.sync_config->on_sync_client_event_hook =
        [promise = std::move(shared_promise), &th, &realm, &user_commit_version, &bootstrap_version, &cv, &mutex,
         &allow_to_commit](std::weak_ptr<SyncSession> weak_session, const SyncClientHookData& data) {
            if (data.query_version == 0) {
                return SyncClientHookAction::NoAction;
            }
            if (data.event != SyncClientHookEvent::DownloadMessageIntegrated) {
                return SyncClientHookAction::NoAction;
            }
            auto session = weak_session.lock();
            if (!session) {
                return SyncClientHookAction::NoAction;
            }
            if (data.batch_state != sync::DownloadBatchState::MoreToCome) {
                // Read version after bootstrap is done.
                auto db = TestHelper::get_db(realm);
                ReadTransaction rt(db);
                bootstrap_version = rt.get_version();
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    allow_to_commit = true;
                }
                cv.notify_one();
                session->force_close();
                promise->emplace_value();
                return SyncClientHookAction::NoAction;
            }

            if (th) {
                return SyncClientHookAction::NoAction;
            }

            auto func = [&] {
                // Attempt to commit a local change after the first bootstrap batch was committed.
                auto db = TestHelper::get_db(realm);
                WriteTransaction wt(db);
                TableRef table = wt.get_table("class_TopLevel");
                table->create_object_with_primary_key(ObjectId::gen());
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    // Wait to commit until we read the final bootstrap version.
                    cv.wait(lock, [&] {
                        return allow_to_commit;
                    });
                }
                user_commit_version = wt.commit();
            };
            th = std::make_unique<std::thread>(std::move(func));

            return SyncClientHookAction::NoAction;
        };

    realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_TopLevel");
    Query query(table);
    {
        auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
        new_subs.insert_or_assign(query);
        new_subs.commit();
    }
    interrupted.get();
    th->join();

    // The user commit is the last one.
    CHECK(user_commit_version == bootstrap_version + 1);
}

TEST_CASE("flx: open realm + register subscription callback while bootstrapping",
          "[sync][flx][bootstrap][async open][baas]") {
    FLXSyncTestHarness harness("flx_bootstrap_and_subscribe");
    auto foo_obj_id = ObjectId::gen();
    int64_t foo_obj_queryable_int = 5;
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", foo_obj_queryable_int},
                                        {"non_queryable_field", "created as initial data seed"s}}));
    });
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    std::atomic<bool> subscription_invoked = false;
    auto subscription_pf = util::make_promise_future<bool>();
    // create a subscription to commit when realm is open for the first time or asked to rerun on open
    auto init_subscription_callback_with_promise =
        [&, promise_holder = util::CopyablePromiseHolder(std::move(subscription_pf.promise))](
            std::shared_ptr<Realm> realm) mutable {
            REQUIRE(realm);
            auto table = realm->read_group().get_table("class_TopLevel");
            Query query(table);
            auto subscription = realm->get_latest_subscription_set();
            auto mutable_subscription = subscription.make_mutable_copy();
            mutable_subscription.insert_or_assign(query);
            auto promise = promise_holder.get_promise();
            mutable_subscription.commit();
            subscription_invoked = true;
            promise.emplace_value(true);
        };
    // verify that the subscription has changed the database
    auto verify_subscription = [](SharedRealm realm) {
        REQUIRE(realm);
        auto table_ref = realm->read_group().get_table("class_TopLevel");
        REQUIRE(table_ref);
        REQUIRE(table_ref->get_column_count() == 4);
        REQUIRE(table_ref->get_column_key("_id"));
        REQUIRE(table_ref->get_column_key("queryable_str_field"));
        REQUIRE(table_ref->get_column_key("queryable_int_field"));
        REQUIRE(table_ref->get_column_key("non_queryable_field"));
        REQUIRE(table_ref->size() == 1);
        auto str_col = table_ref->get_column_key("queryable_str_field");
        REQUIRE(table_ref->get_object(0).get<String>(str_col) == "foo");
        return true;
    };

    SECTION("Sync open") {
        // sync open with subscription callback. Subscription will be run, since this is the first time that realm is
        // opened
        subscription_invoked = false;
        config.sync_config->subscription_initializer = init_subscription_callback_with_promise;
        auto realm = Realm::get_shared_realm(config);
        REQUIRE(subscription_pf.future.get());
        auto sb = realm->get_latest_subscription_set();
        auto future = sb.get_state_change_notification(realm::sync::SubscriptionSet::State::Complete);
        auto state = future.get();
        REQUIRE(state == realm::sync::SubscriptionSet::State::Complete);
        realm->refresh(); // refresh is needed otherwise table_ref->size() would be 0
        REQUIRE(verify_subscription(realm));
    }

    SECTION("Sync Open + Async Open") {
        {
            subscription_invoked = false;
            config.sync_config->subscription_initializer = init_subscription_callback_with_promise;
            auto realm = Realm::get_shared_realm(config);
            REQUIRE(subscription_pf.future.get());
            auto sb = realm->get_latest_subscription_set();
            auto future = sb.get_state_change_notification(realm::sync::SubscriptionSet::State::Complete);
            auto state = future.get();
            REQUIRE(state == realm::sync::SubscriptionSet::State::Complete);
            realm->refresh(); // refresh is needed otherwise table_ref->size() would be 0
            REQUIRE(verify_subscription(realm));
        }
        {
            auto subscription_pf_async = util::make_promise_future<bool>();
            auto init_subscription_asyc_callback =
                [promise_holder_async = util::CopyablePromiseHolder(std::move(subscription_pf_async.promise))](
                    std::shared_ptr<Realm> realm) mutable {
                    REQUIRE(realm);
                    auto table = realm->read_group().get_table("class_TopLevel");
                    Query query(table);
                    auto subscription = realm->get_latest_subscription_set();
                    auto mutable_subscription = subscription.make_mutable_copy();
                    mutable_subscription.insert_or_assign(query);
                    auto promise = promise_holder_async.get_promise();
                    mutable_subscription.commit();
                    promise.emplace_value(true);
                };
            auto open_realm_pf = util::make_promise_future<bool>();
            auto open_realm_completed_callback = [&](ThreadSafeReference ref, std::exception_ptr err) mutable {
                bool result = false;
                if (!err) {
                    result =
                        verify_subscription(Realm::get_shared_realm(std::move(ref), util::Scheduler::make_dummy()));
                }
                open_realm_pf.promise.emplace_value(result);
            };

            config.sync_config->subscription_initializer = init_subscription_asyc_callback;
            config.sync_config->rerun_init_subscription_on_open = true;
            auto async_open = Realm::get_synchronized_realm(config);
            async_open->start(open_realm_completed_callback);
            REQUIRE(open_realm_pf.future.get());
            REQUIRE(subscription_pf_async.future.get());
            config.sync_config->rerun_init_subscription_on_open = false;
            auto realm = Realm::get_shared_realm(config);
            REQUIRE(realm->get_latest_subscription_set().version() == 2);
            REQUIRE(realm->get_active_subscription_set().version() == 2);
        }
    }

    SECTION("Async open") {
        SECTION("Initial async open with no rerun on open set") {
            // subscription will be run since this is the first time we are opening the realm file.
            auto open_realm_pf = util::make_promise_future<bool>();
            auto open_realm_completed_callback = [&](ThreadSafeReference ref, std::exception_ptr err) mutable {
                bool result = false;
                if (!err) {
                    result =
                        verify_subscription(Realm::get_shared_realm(std::move(ref), util::Scheduler::make_dummy()));
                }
                open_realm_pf.promise.emplace_value(result);
            };

            config.sync_config->subscription_initializer = init_subscription_callback_with_promise;
            auto async_open = Realm::get_synchronized_realm(config);
            async_open->start(open_realm_completed_callback);
            REQUIRE(open_realm_pf.future.get());
            REQUIRE(subscription_pf.future.get());

            SECTION("rerun on open = false. Subscription not run") {
                subscription_invoked = false;
                auto async_open = Realm::get_synchronized_realm(config);
                auto open_realm_pf = util::make_promise_future<bool>();
                auto open_realm_completed_callback = [&](ThreadSafeReference, std::exception_ptr e) mutable {
                    // no need to verify if the subscription has changed the db, since it has not run as we test
                    // below
                    open_realm_pf.promise.emplace_value(!e);
                };
                async_open->start(open_realm_completed_callback);
                REQUIRE(open_realm_pf.future.get());
                REQUIRE_FALSE(subscription_invoked.load());
            }

            SECTION("rerun on open = true. Subscription not run cause realm already opened once") {
                subscription_invoked = false;
                auto realm = Realm::get_shared_realm(config);
                auto init_subscription = [&subscription_invoked](std::shared_ptr<Realm> realm) mutable {
                    REQUIRE(realm);
                    auto table = realm->read_group().get_table("class_TopLevel");
                    Query query(table);
                    auto subscription = realm->get_latest_subscription_set();
                    auto mutable_subscription = subscription.make_mutable_copy();
                    mutable_subscription.insert_or_assign(query);
                    mutable_subscription.commit();
                    subscription_invoked.store(true);
                };
                config.sync_config->rerun_init_subscription_on_open = true;
                config.sync_config->subscription_initializer = init_subscription;
                auto async_open = Realm::get_synchronized_realm(config);
                auto open_realm_pf = util::make_promise_future<bool>();
                auto open_realm_completed_callback = [&](ThreadSafeReference, std::exception_ptr e) mutable {
                    // no need to verify if the subscription has changed the db, since it has not run as we test
                    // below
                    open_realm_pf.promise.emplace_value(!e);
                };
                async_open->start(open_realm_completed_callback);
                REQUIRE(open_realm_pf.future.get());
                REQUIRE_FALSE(subscription_invoked.load());
                REQUIRE(realm->get_latest_subscription_set().version() == 1);
                REQUIRE(realm->get_active_subscription_set().version() == 1);
            }
        }

        SECTION("rerun on open set for multiple async open tasks (subscription runs only once)") {
            auto init_subscription = [](std::shared_ptr<Realm> realm) mutable {
                REQUIRE(realm);
                auto table = realm->read_group().get_table("class_TopLevel");
                Query query(table);
                auto subscription = realm->get_latest_subscription_set();
                auto mutable_subscription = subscription.make_mutable_copy();
                mutable_subscription.insert_or_assign(query);
                mutable_subscription.commit();
            };

            auto open_task1_pf = util::make_promise_future<SharedRealm>();
            auto open_task2_pf = util::make_promise_future<SharedRealm>();
            auto open_callback1 = [&](ThreadSafeReference ref, std::exception_ptr err) mutable {
                REQUIRE_FALSE(err);
                open_task1_pf.promise.emplace_value(
                    Realm::get_shared_realm(std::move(ref), util::Scheduler::make_dummy()));
            };
            auto open_callback2 = [&](ThreadSafeReference ref, std::exception_ptr err) mutable {
                REQUIRE_FALSE(err);
                open_task2_pf.promise.emplace_value(
                    Realm::get_shared_realm(std::move(ref), util::Scheduler::make_dummy()));
            };

            config.sync_config->rerun_init_subscription_on_open = true;
            config.sync_config->subscription_initializer = init_subscription;

            SECTION("Realm was already created, but we want to rerun on first open using multiple tasks") {
                {
                    subscription_invoked = false;
                    auto realm = Realm::get_shared_realm(config);
                    auto sb = realm->get_latest_subscription_set();
                    auto future = sb.get_state_change_notification(realm::sync::SubscriptionSet::State::Complete);
                    auto state = future.get();
                    REQUIRE(state == realm::sync::SubscriptionSet::State::Complete);
                    realm->refresh(); // refresh is needed otherwise table_ref->size() would be 0
                    REQUIRE(verify_subscription(realm));
                    REQUIRE(realm->get_latest_subscription_set().version() == 1);
                    REQUIRE(realm->get_active_subscription_set().version() == 1);
                }

                auto async_open_task1 = Realm::get_synchronized_realm(config);
                auto async_open_task2 = Realm::get_synchronized_realm(config);
                async_open_task1->start(open_callback1);
                async_open_task2->start(open_callback2);

                auto realm1 = open_task1_pf.future.get();
                auto realm2 = open_task2_pf.future.get();

                const auto version_expected = 2;
                auto r1_latest = realm1->get_latest_subscription_set().version();
                auto r1_active = realm1->get_active_subscription_set().version();
                REQUIRE(realm2->get_latest_subscription_set().version() == r1_latest);
                REQUIRE(realm2->get_active_subscription_set().version() == r1_active);
                REQUIRE(r1_latest == version_expected);
                REQUIRE(r1_active == version_expected);
            }
            SECTION("First time realm is created but opened via open async. Both tasks could run the subscription") {
                auto async_open_task1 = Realm::get_synchronized_realm(config);
                auto async_open_task2 = Realm::get_synchronized_realm(config);
                async_open_task1->start(open_callback1);
                async_open_task2->start(open_callback2);
                auto realm1 = open_task1_pf.future.get();
                auto realm2 = open_task2_pf.future.get();

                auto r1_latest = realm1->get_latest_subscription_set().version();
                auto r1_active = realm1->get_active_subscription_set().version();
                REQUIRE(realm2->get_latest_subscription_set().version() == r1_latest);
                REQUIRE(realm2->get_active_subscription_set().version() == r1_active);
                // the callback may be run twice, if task1 is the first task to open realm
                // but it is scheduled after tasks2, which have opened realm later but
                // by the time it runs, subscription version is equal to 0 (realm creation).
                // This can only happen the first time that realm is created. All the other times
                // the init_sb callback is guaranteed to run once.
                REQUIRE(r1_latest >= 1);
                REQUIRE(r1_latest <= 2);
                REQUIRE(r1_active >= 1);
                REQUIRE(r1_active <= 2);
            }
        }

        SECTION("Wait to bootstrap all pending subscriptions even when subscription_initializer is not used") {
            // Client 1
            {
                auto realm = Realm::get_shared_realm(config);
                // Create subscription (version = 1) and bootstrap data.
                subscribe_to_all_and_bootstrap(*realm);
                realm->sync_session()->shutdown_and_wait();

                // Create a new subscription (version = 2) while the session is closed.
                // The new subscription does not match the object bootstrapped at version 1.
                auto mutable_subscription = realm->get_latest_subscription_set().make_mutable_copy();
                mutable_subscription.clear();
                auto table = realm->read_group().get_table("class_TopLevel");
                auto queryable_int_field = table->get_column_key("queryable_int_field");
                mutable_subscription.insert_or_assign(
                    Query(table).not_equal(queryable_int_field, foo_obj_queryable_int));
                mutable_subscription.commit();

                realm->close();
            }

            REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(config.path));

            // Client 2 uploads data matching Client 1's subscription at version 1
            harness.load_initial_data([&](SharedRealm realm) {
                CppContext c(realm);
                Object::create(c, realm, "TopLevel",
                               std::any(AnyDict{{"_id", ObjectId::gen()},
                                                {"queryable_str_field", "bar"s},
                                                {"queryable_int_field", 2 * foo_obj_queryable_int},
                                                {"non_queryable_field", "some data"s}}));
            });

            // Client 1 opens the realm asynchronously and expects the task to complete
            // when the subscription at version 2 finishes bootstrapping.
            auto realm = successfully_async_open_realm(config);

            // Check subscription at version 2 is marked Complete.
            CHECK(realm->get_latest_subscription_set().version() == 2);
            CHECK(realm->get_active_subscription_set().version() == 2);
        }
    }
}
TEST_CASE("flx sync: Client reset during async open", "[sync][flx][client reset][async open][baas]") {
    FLXSyncTestHarness harness("flx_bootstrap_reset");
    auto foo_obj_id = ObjectId::gen();
    std::atomic<bool> subscription_invoked = false;
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "created as initial data seed"s}}));
    });
    SyncTestFile realm_config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    auto subscription_callback = [&](std::shared_ptr<Realm> realm) {
        REQUIRE(realm);
        auto table = realm->read_group().get_table("class_TopLevel");
        Query query(table);
        auto subscription = realm->get_latest_subscription_set();
        auto mutable_subscription = subscription.make_mutable_copy();
        mutable_subscription.insert_or_assign(query);
        subscription_invoked = true;
        mutable_subscription.commit();
    };

    realm_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    realm_config.sync_config->subscription_initializer = subscription_callback;

    bool client_reset_triggered = false;
    realm_config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_sess,
                                                              const SyncClientHookData& event_data) {
        auto sess = weak_sess.lock();
        if (!sess) {
            return SyncClientHookAction::NoAction;
        }
        if (sess->path() != realm_config.path) {
            return SyncClientHookAction::NoAction;
        }

        if (event_data.event != SyncClientHookEvent::DownloadMessageReceived) {
            return SyncClientHookAction::NoAction;
        }

        if (client_reset_triggered) {
            return SyncClientHookAction::NoAction;
        }

        client_reset_triggered = true;
        reset_utils::trigger_client_reset(harness.session().app_session(), *sess);
        return SyncClientHookAction::SuspendWithRetryableError;
    };

    auto before_callback_called = util::make_promise_future<void>();
    realm_config.sync_config->notify_before_client_reset = [&](std::shared_ptr<Realm> realm) {
        CHECK(realm->schema_version() == 0);
        before_callback_called.promise.emplace_value();
    };

    auto after_callback_called = util::make_promise_future<void>();
    realm_config.sync_config->notify_after_client_reset = [&](std::shared_ptr<Realm> realm, ThreadSafeReference,
                                                              bool) {
        CHECK(realm->schema_version() == 0);
        after_callback_called.promise.emplace_value();
    };

    auto realm_task = Realm::get_synchronized_realm(realm_config);
    auto realm_future = realm_task->start();
    auto realm = Realm::get_shared_realm(std::move(realm_future).get(), util::Scheduler::make_dummy());
    before_callback_called.future.get();
    after_callback_called.future.get();
    REQUIRE(subscription_invoked.load());
}

// Test that resending pending subscription sets does not cause any inconsistencies in the progress cursors.
TEST_CASE("flx sync: resend pending subscriptions when reconnecting", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("flx_pending_subscriptions", {g_large_array_schema, {"queryable_int_field"}});

    std::vector<ObjectId> obj_ids_at_end = fill_large_array_schema(harness);
    SyncTestFile interrupted_realm_config(harness.app()->current_user(), harness.schema(),
                                          SyncConfig::FLXSyncEnabled{});

    {
        auto pf = util::make_promise_future<void>();
        Realm::Config config = interrupted_realm_config;
        config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
        config.sync_config->on_sync_client_event_hook =
            [promise = util::CopyablePromiseHolder(std::move(pf.promise))](std::weak_ptr<SyncSession> weak_session,
                                                                           const SyncClientHookData& data) mutable {
                if (data.event != SyncClientHookEvent::BootstrapMessageProcessed &&
                    data.event != SyncClientHookEvent::BootstrapProcessed) {
                    return SyncClientHookAction::NoAction;
                }
                auto session = weak_session.lock();
                if (!session) {
                    return SyncClientHookAction::NoAction;
                }
                if (data.query_version != 1) {
                    return SyncClientHookAction::NoAction;
                }

                // Commit a subscriptions set whenever a bootstrap message is received for query version 1.
                if (data.event == SyncClientHookEvent::BootstrapMessageProcessed) {
                    auto latest_subs = session->get_flx_subscription_store()->get_latest().make_mutable_copy();
                    latest_subs.commit();
                    return SyncClientHookAction::NoAction;
                }
                // At least one subscription set was created.
                CHECK(session->get_flx_subscription_store()->get_latest().version() > 1);
                promise.get_promise().emplace_value();
                // Reconnect once query version 1 is bootstrapped.
                return SyncClientHookAction::TriggerReconnect;
            };

        auto realm = Realm::get_shared_realm(config);
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            auto table = realm->read_group().get_table("class_TopLevel");
            mut_subs.insert_or_assign(Query(table));
            mut_subs.commit();
        }
        pf.future.get();
        realm->sync_session()->shutdown_and_wait();
        realm->close();
    }

    REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(interrupted_realm_config.path));

    // Check at least one subscription set needs to be resent.
    {
        DBOptions options;
        options.encryption_key = test_util::crypt_key();
        auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path, options);
        auto sub_store = sync::SubscriptionStore::create(realm);
        auto version_info = sub_store->get_version_info();
        REQUIRE(version_info.latest > version_info.active);
    }

    // Resend the pending subscriptions.
    auto realm = Realm::get_shared_realm(interrupted_realm_config);
    wait_for_upload(*realm);
    wait_for_download(*realm);
}

TEST_CASE("flx: fatal errors and session becoming inactive cancel pending waits", "[sync][flx][baas]") {
    std::vector<ObjectSchema> schema{
        {"TopLevel",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
         }},
    };

    FLXSyncTestHarness harness("flx_cancel_pending_waits", {schema, {"queryable_int_field"}});
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    auto check_status = [](auto status) {
        CHECK(!status.is_ok());
        std::string reason = status.get_status().reason();
        // Subscription notification is cancelled either because the sync session is inactive, or because a fatal
        // error is received from the server.
        if (reason.find("Sync session became inactive") == std::string::npos &&
            reason.find("Invalid schema change (UPLOAD): non-breaking schema change: adding \"Int\" column at field "
                        "\"other_col\" in schema \"TopLevel\", schema changes from clients are restricted when "
                        "developer mode is disabled") == std::string::npos) {
            FAIL(reason);
        }
    };

    auto create_subscription = [](auto realm) -> realm::sync::SubscriptionSet {
        auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
        auto table = realm->read_group().get_table("class_TopLevel");
        mut_subs.insert_or_assign(Query(table));
        return mut_subs.commit();
    };

    auto [error_occured_promise, error_occurred] = util::make_promise_future<void>();
    config.sync_config->error_handler = [promise = util::CopyablePromiseHolder(std::move(error_occured_promise))](
                                            std::shared_ptr<SyncSession>, SyncError) mutable {
        promise.get_promise().emplace_value();
    };

    auto realm = Realm::get_shared_realm(config);
    wait_for_download(*realm);

    auto subs = create_subscription(realm);
    auto subs_future = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete);

    realm->sync_session()->pause();
    auto state = subs_future.get_no_throw();
    check_status(state);

    auto [download_complete_promise, download_complete] = util::make_promise_future<void>();
    realm->sync_session()->wait_for_upload_completion([promise = std::move(download_complete_promise)](auto) mutable {
        promise.emplace_value();
    });
    schema[0].persisted_properties.push_back({"other_col", PropertyType::Int | PropertyType::Nullable});
    realm->update_schema(schema);

    subs = create_subscription(realm);
    subs_future = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete);

    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", ObjectId::gen()},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"other_col", static_cast<int64_t>(42)}}));
    });

    realm->sync_session()->resume();
    download_complete.get();
    error_occurred.get();
    state = subs_future.get_no_throw();
    check_status(state);
}

TEST_CASE("flx: pause and resume bootstrapping at query version 0", "[sync][flx][baas]") {
    FLXSyncTestHarness harness("flx_pause_resume_bootstrap");
    SyncTestFile triggered_config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
    std::mutex download_message_mutex;
    int download_message_integrated_count = 0;
    triggered_config.sync_config->on_sync_client_event_hook =
        [promise = util::CopyablePromiseHolder(std::move(interrupted_promise)), &download_message_integrated_count,
         &download_message_mutex](std::weak_ptr<SyncSession> weak_sess, const SyncClientHookData& data) mutable {
            auto sess = weak_sess.lock();
            if (!sess || data.event != SyncClientHookEvent::DownloadMessageIntegrated) {
                return SyncClientHookAction::NoAction;
            }

            std::lock_guard<std::mutex> lk(download_message_mutex);
            // Pause and resume the first session after the bootstrap message is integrated.
            if (download_message_integrated_count == 0) {
                sess->pause();
                sess->resume();
            }
            // Complete the test when the second session integrates the empty download
            // message it receives.
            else {
                promise.get_promise().emplace_value();
            }
            ++download_message_integrated_count;
            return SyncClientHookAction::NoAction;
        };
    auto realm = Realm::get_shared_realm(triggered_config);
    interrupted.get();
    std::lock_guard<std::mutex> lk(download_message_mutex);
    CHECK(download_message_integrated_count == 2);
    auto active_sub_set = realm->sync_session()->get_flx_subscription_store()->get_active();
    REQUIRE(active_sub_set.version() == 0);
    REQUIRE(active_sub_set.state() == sync::SubscriptionSet::State::Complete);
}

TEST_CASE("flx: collections in mixed - merge lists", "[sync][flx][baas]") {
    Schema schema{{"TopLevel",
                   {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                    {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                    {"any", PropertyType::Mixed | PropertyType::Nullable}}}};

    FLXSyncTestHarness::ServerSchema server_schema{schema, {"queryable_str_field"}};
    FLXSyncTestHarness harness("flx_collections_in_mixed", server_schema);
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    auto set_list_and_insert_element = [](Obj& obj, ColKey col_any, Mixed value) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list_ptr<Mixed>(col_any);
        list->add(value);
    };

    // Client 1 creates an object and sets property 'any' to an integer value.
    auto foo_obj_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id}, {"queryable_str_field", "foo"s}, {"any", 42}}));
    });

    // Client 2 opens the realm and downloads schema and object created by Client 1.
    auto realm = Realm::get_shared_realm(config);
    subscribe_to_all_and_bootstrap(*realm);
    realm->sync_session()->pause();

    // Client 3 sets property 'any' to List and inserts two integers in the list.
    harness.load_initial_data([&](SharedRealm realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
        auto col_any = table->get_column_key("any");
        set_list_and_insert_element(obj, col_any, 1);
        set_list_and_insert_element(obj, col_any, 2);
    });

    // While its session is paused, Client 2 sets property 'any' to List and inserts two integers in the list.
    CppContext c(realm);
    realm->begin_transaction();
    auto table = realm->read_group().get_table("class_TopLevel");
    auto obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
    auto col_any = table->get_column_key("any");
    set_list_and_insert_element(obj, col_any, 3);
    set_list_and_insert_element(obj, col_any, 4);
    realm->commit_transaction();

    realm->sync_session()->resume();
    wait_for_upload(*realm);
    wait_for_download(*realm);
    wait_for_advance(*realm);

    // Client 2 ends up with four integers in the list (in the correct order).
    auto list = obj.get_list_ptr<Mixed>(col_any);
    CHECK(list->size() == 4);
    CHECK(list->get(0) == 1);
    CHECK(list->get(1) == 2);
    CHECK(list->get(2) == 3);
    CHECK(list->get(3) == 4);
}

TEST_CASE("flx: nested collections in mixed", "[sync][flx][baas]") {
    Schema schema{{"TopLevel",
                   {{"_id", PropertyType::Int, Property::IsPrimary{true}},
                    {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                    {"any", PropertyType::Mixed | PropertyType::Nullable}}}};

    FLXSyncTestHarness::ServerSchema server_schema{schema, {"queryable_str_field"}};
    FLXSyncTestHarness harness("flx_collections_in_mixed", server_schema);
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    // Client 1: {_id: 1, any: ["abc", [42]]}
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        auto obj = Object::create(c, realm, "TopLevel",
                                  std::any(AnyDict{{"_id", INT64_C(1)}, {"queryable_str_field", "foo"s}}));
        auto table = realm->read_group().get_table("class_TopLevel");
        auto col_any = table->get_column_key("any");
        obj.get_obj().set_collection(col_any, CollectionType::List);
        List list(obj, obj.get_object_schema().property_for_name("any"));
        list.insert_any(0, "abc");
        list.insert_collection(1, CollectionType::List);
        auto list2 = list.get_list(1);
        list2.insert_any(0, 42);
    });

    // Client 2 opens the realm and downloads schema and object created by Client 1.
    // {_id: 1, any: ["abc", [42]]}
    auto realm = Realm::get_shared_realm(config);
    subscribe_to_all_and_bootstrap(*realm);
    realm->sync_session()->pause();

    // Client 3 adds a dictionary with an element to list 'any'
    // {_id: 1, any: [{{"key": 6}}, "abc", [42]]}
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any(INT64_C(1)));
        List list(obj, obj.get_object_schema().property_for_name("any"));
        list.insert_collection(PathElement(0), CollectionType::Dictionary);
        auto dict = list.get_dictionary(PathElement(0));
        dict.insert_any("key", INT64_C(6));
    });

    // While its session is paused, Client 2 makes a change to a nested list
    // {_id: 1, any: ["abc", [42, "foo"]]}
    CppContext c(realm);
    realm->begin_transaction();
    auto obj = Object::get_for_primary_key(c, realm, "TopLevel", std::any(INT64_C(1)));
    List list(obj, obj.get_object_schema().property_for_name("any"));
    List list2 = list.get_list(PathElement(1));
    list2.insert_any(list2.size(), "foo");
    realm->commit_transaction();

    realm->sync_session()->resume();
    wait_for_upload(*realm);
    wait_for_download(*realm);
    wait_for_advance(*realm);

    // Client 2 after the session is resumed
    // {_id: 1, any: [{{"key": 6}}, "abc", [42, "foo"]]}
    CHECK(list.size() == 3);
    auto nested_dict = list.get_dictionary(0);
    CHECK(nested_dict.size() == 1);
    CHECK(nested_dict.get<Int>("key") == 6);

    CHECK(list.get_any(1) == "abc");

    auto nested_list = list.get_list(2);
    CHECK(nested_list.size() == 2);
    CHECK(nested_list.get_any(0) == 42);
    CHECK(nested_list.get_any(1) == "foo");
}

TEST_CASE("flx: no upload during bootstraps", "[sync][flx][bootstrap][baas]") {
    FLXSyncTestHarness harness("flx_bootstrap_no_upload", {g_large_array_schema, {"queryable_int_field"}});
    fill_large_array_schema(harness);
    auto config = harness.make_test_file();
    enum class TestState { Start, BootstrapInProgress, BootstrapProcessed, BootstrapAck };
    TestingStateMachine<TestState> state(TestState::Start);
    config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession>, const SyncClientHookData& data) {
        if (data.query_version == 0) {
            return SyncClientHookAction::NoAction;
        }
        state.transition_with([&](TestState cur_state) -> std::optional<TestState> {
            // Check no upload messages are sent during bootstrap.
            if (data.event == SyncClientHookEvent::BootstrapMessageProcessed) {
                CHECK((cur_state == TestState::Start || cur_state == TestState::BootstrapInProgress));
                return TestState::BootstrapInProgress;
            }
            else if (data.event == SyncClientHookEvent::DownloadMessageIntegrated &&
                     data.batch_state == sync::DownloadBatchState::LastInBatch) {
                CHECK(cur_state == TestState::BootstrapInProgress);
                return TestState::BootstrapProcessed;
            }
            else if (data.event == SyncClientHookEvent::UploadMessageSent) {
                // Uploads are allowed before a bootstrap starts.
                if (cur_state == TestState::Start) {
                    return std::nullopt; // Don't transition
                }
                CHECK(cur_state == TestState::BootstrapProcessed);
                return TestState::BootstrapAck;
            }
            return std::nullopt;
        });
        return SyncClientHookAction::NoAction;
    };

    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_TopLevel");
    auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
    new_subs.insert_or_assign(Query(table));
    new_subs.commit();
    state.wait_for(TestState::BootstrapAck);

    // Commiting an empty changeset does not upload a message.
    realm->begin_transaction();
    realm->commit_transaction();
    // Give the sync client the chance to send an upload after mark.
    wait_for_download(*realm);
}

TEST_CASE("flx: bootstrap is properly applied when the connection is reestablished", "[sync][flx][bootstrap][baas]") {
    FLXSyncTestHarness harness("flx_bootstrap", {g_large_array_schema, {"queryable_int_field"}});
    auto& app_session = harness.session().app_session();
    REQUIRE(app_session.admin_api.patch_app_settings(app_session.server_app_id,
                                                     {{"sync", {{"num_objects_before_bootstrap_flush", 1}}}}));
    REQUIRE(app_session.admin_api.patch_app_settings(
        app_session.server_app_id, {{"sync", {{"qbs_download_changeset_soft_max_byte_size", 1000}}}}));

    fill_large_array_schema(harness);
    auto config = harness.make_test_file();
    bool once = false;
    config.sync_config->on_sync_client_event_hook = [&once](std::weak_ptr<SyncSession>,
                                                            const SyncClientHookData& data) {
        if (data.query_version != 1) {
            return SyncClientHookAction::NoAction;
        }
        if (data.event == SyncClientHookEvent::BootstrapMessageProcessed && !once) {
            once = true;
            return SyncClientHookAction::TriggerReconnect;
        }
        // The batch of changesets added to the PendingBoostrapStore before disconnect
        // were removed when the connection was reestablished.
        // There are 5 changesets in 5 download messages and one additional download message
        // with an empty changeset (as per server design).
        if (data.event == SyncClientHookEvent::BootstrapProcessed) {
            CHECK(data.num_changesets == 6);
        }
        return SyncClientHookAction::NoAction;
    };

    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_TopLevel");
    auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
    new_subs.insert_or_assign(Query(table));
    auto subs = new_subs.commit();
    subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
}

} // namespace realm::app

#endif // REALM_ENABLE_AUTH_TESTS
