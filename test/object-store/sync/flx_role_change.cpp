////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 MongoDB Inc.
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

#ifdef REALM_ENABLE_AUTH_TESTS

#include <catch2/catch_all.hpp>

#include <util/test_file.hpp>
#include <util/sync/flx_sync_harness.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object_id.hpp>
#include <realm/query_expression.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/sync/client_base.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/subscriptions.hpp>

#include <realm/util/future.hpp>
#include <realm/util/logger.hpp>

#include <filesystem>
#include <iostream>
#include <stdexcept>

using namespace realm;
using namespace realm::app;

namespace {

const Schema g_person_schema{{"Person",
                              {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                               {"role", PropertyType::String},
                               {"name", PropertyType::String},
                               {"emp_id", PropertyType::Int}}}};

auto fill_person_schema = [](SharedRealm realm, std::string role, size_t count) {
    CppContext c(realm);
    for (size_t i = 0; i < count; ++i) {
        auto obj = Object::create(c, realm, "Person",
                                  std::any(AnyDict{
                                      {"_id", ObjectId::gen()},
                                      {"role", role},
                                      {"name", util::format("%1-%2", role, i)},
                                      {"emp_id", static_cast<int64_t>(i)},
                                  }));
    }
};

struct TestParams {
    size_t num_emps = 150;
    size_t num_mgrs = 25;
    size_t num_dirs = 10;
    std::optional<size_t> num_objects = 10;
    std::optional<size_t> max_download_bytes = 4096;
    std::optional<size_t> sleep_millis = std::nullopt;
};

std::unique_ptr<FLXSyncTestHarness> setup_harness(std::string app_name, TestParams params)
{
    auto harness = std::make_unique<FLXSyncTestHarness>(
        app_name, FLXSyncTestHarness::ServerSchema{g_person_schema, {"role", "name"}});

    auto& app_session = harness->session().app_session();

    if (params.num_objects) {
        REQUIRE(app_session.admin_api.patch_app_settings(
            app_session.server_app_id, {{"sync", {{"num_objects_before_bootstrap_flush", *params.num_objects}}}}));
    }

    if (params.max_download_bytes) {
        REQUIRE(app_session.admin_api.patch_app_settings(
            app_session.server_app_id,
            {{"sync", {{"qbs_download_changeset_soft_max_byte_size", *params.max_download_bytes}}}}));
    }

    if (params.sleep_millis) {
        REQUIRE(app_session.admin_api.patch_app_settings(
            app_session.server_app_id, {{"sync", {{"download_loop_sleep_millis", *params.sleep_millis}}}}));
    }

    // Initialize the realm with some data
    harness->load_initial_data([&](SharedRealm realm) {
        fill_person_schema(realm, "employee", params.num_emps);
        fill_person_schema(realm, "manager", params.num_mgrs);
        fill_person_schema(realm, "director", params.num_dirs);
    });
    // Return the unique_ptr for the newly created harness
    return harness;
}

void teardown_harness(std::unique_ptr<FLXSyncTestHarness>& harness)
{
    if (!harness) {
        return; // nothing to do
    }
    harness->app()->sync_manager()->wait_for_sessions_to_terminate();
    harness.reset();
}

void update_role(nlohmann::json& rule, nlohmann::json doc_filter)
{
    rule["roles"][0]["document_filters"]["read"] = doc_filter;
    rule["roles"][0]["document_filters"]["write"] = doc_filter;
}

void set_up_realm(SharedRealm& setup_realm, size_t expected_cnt)
{
    // Set up the initial subscription
    auto table = setup_realm->read_group().get_table("class_Person");
    auto new_subs = setup_realm->get_latest_subscription_set().make_mutable_copy();
    new_subs.insert_or_assign(Query(table));
    auto subs = new_subs.commit();

    // Wait for subscription update and sync to complete
    subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    REQUIRE(!wait_for_download(*setup_realm));
    REQUIRE(!wait_for_upload(*setup_realm));
    wait_for_advance(*setup_realm);

    // Verify the data was downloaded
    table = setup_realm->read_group().get_table("class_Person");
    Results results(setup_realm, Query(table));
    REQUIRE(results.size() == expected_cnt);
}

void verify_records(SharedRealm& check_realm, size_t emps, size_t mgrs, size_t dirs)
{
    // Validate the expected number of entries for each role type after the role change
    auto table = check_realm->read_group().get_table("class_Person");
    REQUIRE(table->size() == (emps + mgrs + dirs));
    auto role_col = table->get_column_key("role");
    auto table_query = Query(table).equal(role_col, "employee");
    auto results = Results(check_realm, table_query);
    CHECK(results.size() == emps);
    table_query = Query(table).equal(role_col, "manager");
    results = Results(check_realm, table_query);
    CHECK(results.size() == mgrs);
    table_query = Query(table).equal(role_col, "director");
    results = Results(check_realm, table_query);
    CHECK(results.size() == dirs);
}

} // namespace

TEST_CASE("flx: role change bootstraps", "[sync][flx][baas][role change][bootstrap]") {
    auto logger = util::Logger::get_default_logger();
    static std::unique_ptr<FLXSyncTestHarness> harness;

    auto pause_download_builder = [](std::weak_ptr<SyncSession> weak_session, bool pause) {
        if (auto session = weak_session.lock()) {
            nlohmann::json test_command = {{"command", pause ? "PAUSE_DOWNLOAD_BUILDER" : "RESUME_DOWNLOAD_BUILDER"}};
            SyncSession::OnlyForTesting::send_test_command(*session, test_command.dump())
                .get_async([](StatusWith<std::string> result) {
                    REQUIRE(result.is_ok());             // Future completed successfully
                    REQUIRE(result.get_value() == "{}"); // Command completed successfully
                });
        }
    };

    enum BootstrapMode { NoReconnect, None, SingleMessage, SingleMessageMulti, MultiMessage, Any };
    struct ExpectedResults {
        BootstrapMode bootstrap;
        size_t emps;
        size_t mgrs;
        size_t dirs;
    };

    enum TestState {
        not_ready,
        start,
        reconnect_received,
        session_resumed,
        ident_message,
        downloading,
        downloaded,
        complete
    };

    TestingStateMachine<TestState> state_machina(TestState::not_ready);
    int64_t query_version = 0;
    BootstrapMode bootstrap_mode = BootstrapMode::None;
    size_t download_msg_count = 0;
    size_t bootstrap_msg_count = 0;
    bool role_change_bootstrap = false;
    bool send_test_command = false;

    auto setup_config_callbacks = [&](SyncTestFile& config) {
        // Use the sync client event hook to check for the error received and for tracking
        // download messages and bootstraps
        config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_session,
                                                            const SyncClientHookData& data) {
            state_machina.transition_with([&](TestState cur_state) -> std::optional<TestState> {
                if (cur_state == TestState::not_ready || cur_state == TestState::complete)
                    return std::nullopt;

                using BatchState = sync::DownloadBatchState;
                using Event = SyncClientHookEvent;
                switch (data.event) {
                    case Event::ErrorMessageReceived:
                        REQUIRE(cur_state == TestState::start);
                        REQUIRE(data.error_info);
                        REQUIRE(data.error_info->raw_error_code == 200);
                        REQUIRE(data.error_info->server_requests_action ==
                                sync::ProtocolErrorInfo::Action::Transient);
                        REQUIRE_FALSE(data.error_info->is_fatal);
                        return TestState::reconnect_received;

                    case Event::SessionConnected:
                        // Handle the reconnect if session multiplexing is disabled
                        [[fallthrough]];
                    case Event::SessionResumed:
                        if (send_test_command) {
                            REQUIRE(cur_state == TestState::reconnect_received);
                            logger->trace("ROLE CHANGE: sending PAUSE test command after resumed");
                            pause_download_builder(weak_session, true);
                        }
                        return TestState::session_resumed;

                    case Event::IdentMessageSent:
                        if (send_test_command) {
                            REQUIRE(cur_state == TestState::session_resumed);
                            logger->trace("ROLE CHANGE: sending RESUME test command after ident message sent");
                            pause_download_builder(weak_session, false);
                        }
                        return TestState::ident_message;

                    case Event::DownloadMessageReceived: {
                        // Skip unexpected download messages
                        if (cur_state != TestState::ident_message && cur_state != TestState::downloading) {
                            return std::nullopt;
                        }
                        ++download_msg_count;
                        // A multi-message bootstrap is in progress..
                        if (data.batch_state == BatchState::MoreToCome) {
                            // More than 1 bootstrap message, always a multi-message
                            bootstrap_mode = BootstrapMode::MultiMessage;
                            logger->trace("ROLE CHANGE: detected multi-message bootstrap");
                            return TestState::downloading;
                        }
                        // single bootstrap message or last message in the multi-message bootstrap
                        else if (data.batch_state == BatchState::LastInBatch) {
                            if (download_msg_count == 1) {
                                if (data.num_changesets == 1) {
                                    logger->trace("ROLE CHANGE: detected single-message/single-changeset bootstrap");
                                    bootstrap_mode = BootstrapMode::SingleMessage;
                                }
                                else {
                                    logger->trace("ROLE CHANGE: detected single-message/multi-changeset bootstrap");
                                    bootstrap_mode = BootstrapMode::SingleMessageMulti;
                                }
                            }
                            return TestState::downloaded;
                        }
                        return std::nullopt;
                    }

                    // A bootstrap message was processed
                    case Event::BootstrapMessageProcessed: {
                        REQUIRE(data.batch_state != BatchState::SteadyState);
                        REQUIRE((cur_state == TestState::downloading || cur_state == TestState::downloaded));
                        ++bootstrap_msg_count;
                        if (data.query_version == query_version) {
                            role_change_bootstrap = true;
                        }
                        return std::nullopt;
                    }
                    // The bootstrap has been received and processed
                    case Event::BootstrapProcessed:
                        REQUIRE(cur_state == TestState::downloaded);
                        return TestState::complete;

                    default:
                        return std::nullopt;
                }
            });
            return SyncClientHookAction::NoAction;
        };

        // Add client reset callback to verify a client reset doesn't happen
        config.sync_config->notify_before_client_reset = [&](std::shared_ptr<Realm>) {
            // Make sure a client reset did not occur while waiting for the role change to
            // be applied
            FAIL("Client reset is not expected when the role/rules/permissions are changed");
        };
    };

    auto update_perms_and_verify = [&](FLXSyncTestHarness& harness, SharedRealm check_realm, nlohmann::json new_rules,
                                       ExpectedResults expected) {
        // Reset the state machine
        state_machina.transition_with([&](TestState cur_state) {
            REQUIRE(cur_state == TestState::not_ready);
            bootstrap_msg_count = 0;
            download_msg_count = 0;
            role_change_bootstrap = false;
            query_version = check_realm->get_active_subscription_set().version();
            if (expected.bootstrap == BootstrapMode::SingleMessageMulti) {
                send_test_command = true;
            }
            return TestState::start;
        });

        // Update the permissions on the server - should send an error to the client to force
        // it to reconnect
        auto& app_session = harness.session().app_session();
        logger->debug("ROLE CHANGE: Updating rule definitions: %1", new_rules);
        app_session.admin_api.update_default_rule(app_session.server_app_id, new_rules);

        if (expected.bootstrap != BootstrapMode::NoReconnect) {
            // After updating the permissions (if they are different), the server should send an
            // error that will disconnect/reconnect the session - verify the reconnect occurs.
            // Make sure at least the reconnect state (or later) has been reached
            auto state_reached = state_machina.wait_until([](TestState cur_state) {
                return static_cast<int>(cur_state) >= static_cast<int>(TestState::reconnect_received);
            });
            REQUIRE(state_reached);
        }

        // Assuming the session disconnects and reconnects, the server initiated role change
        // bootstrap download will take place when the session is re-established and will
        // complete before the server sends the initial MARK response.
        REQUIRE(!wait_for_download(*check_realm));
        REQUIRE(!wait_for_upload(*check_realm));

        // Now that the server initiated bootstrap should be complete, verify the operation
        // performed matched what was expected.
        state_machina.transition_with([&](TestState cur_state) {
            switch (expected.bootstrap) {
                case BootstrapMode::NoReconnect:
                    // Confirm that the session did receive an error and a bootstrap did not occur
                    REQUIRE(cur_state == TestState::start);
                    REQUIRE_FALSE(role_change_bootstrap);
                    break;
                case BootstrapMode::None:
                    // Confirm that a bootstrap nor a client reset did not occur
                    REQUIRE(cur_state == TestState::reconnect_received);
                    REQUIRE_FALSE(role_change_bootstrap);
                    break;
                case BootstrapMode::Any:
                    // Doesn't matter which one, just that a bootstrap occurred and not a client reset
                    REQUIRE(cur_state == TestState::complete);
                    REQUIRE(role_change_bootstrap);
                    break;
                default:
                    // By the time the MARK response is received and wait_for_download()
                    // returns, the bootstrap should have already been applied.
                    REQUIRE(expected.bootstrap == bootstrap_mode);
                    REQUIRE(role_change_bootstrap);
                    REQUIRE(cur_state == TestState::complete);
                    if (expected.bootstrap == BootstrapMode::SingleMessageMulti ||
                        expected.bootstrap == BootstrapMode::SingleMessage) {
                        REQUIRE(bootstrap_msg_count == 1);
                    }
                    else if (expected.bootstrap == BootstrapMode::MultiMessage) {
                        REQUIRE(bootstrap_msg_count > 1);
                    }
                    break;
            }
            return std::nullopt; // Don't transition
        });

        // Validate the expected number of entries for each role type after the role change
        wait_for_advance(*check_realm);
        verify_records(check_realm, expected.emps, expected.mgrs, expected.dirs);

        // Reset the state machine to "not ready" before leaving
        state_machina.transition_to(TestState::not_ready);
    };

    auto setup_test = [&](FLXSyncTestHarness& harness, nlohmann::json initial_rules, size_t initial_count) {
        // If an intial set of rules are provided, then set them now
        auto& app_session = harness.session().app_session();
        // If the rules are empty, then reset to the initial default state
        if (initial_rules.empty()) {
            initial_rules = app_session.admin_api.get_default_rule(app_session.server_app_id);
            AppCreateConfig::ServiceRole general_role{"default"};
            initial_rules["roles"] = {};
            initial_rules["roles"][0] = transform_service_role(general_role);
        }
        logger->debug("ROLE CHANGE: Initial rule definitions: %1", initial_rules);
        app_session.admin_api.update_default_rule(app_session.server_app_id, initial_rules);

        // Create and set up a new realm to be returned; wait for data sync
        auto config = harness.make_test_file();
        setup_config_callbacks(config);
        auto setup_realm = Realm::get_shared_realm(config);
        set_up_realm(setup_realm, initial_count);
        return setup_realm;
    };

    // 150 emps, 25 mgrs, 10 dirs
    // 10 objects before flush
    // 4096 download soft max bytes
    TestParams params{};
    if (!harness) {
        harness = setup_harness("flx_role_change_bootstraps", params);
    }
    REQUIRE(harness);

    size_t num_total = params.num_emps + params.num_mgrs + params.num_dirs;
    auto realm_1 = setup_test(*harness, {}, num_total);
    // Get the current rules so it can be updated during the test
    auto& app_session = harness->session().app_session();
    auto test_rules = app_session.admin_api.get_default_rule(app_session.server_app_id);

    SECTION("Role changes lead to objects in/out of view without client reset") {
        // Single message bootstrap - remove employees, keep mgrs/dirs
        logger->trace("ROLE CHANGE: Updating rules to remove employees");
        update_role(test_rules, {{"role", {{"$in", {"manager", "director"}}}}});
        update_perms_and_verify(*harness, realm_1, test_rules,
                                {BootstrapMode::SingleMessage, 0, params.num_mgrs, params.num_dirs});
        // Write the same rules again - the client should not receive the reconnect (200) error
        logger->trace("ROLE CHANGE: Updating same rules again and verify reconnect doesn't happen");
        update_perms_and_verify(*harness, realm_1, test_rules,
                                {BootstrapMode::NoReconnect, 0, params.num_mgrs, params.num_dirs});
        // Multi-message bootstrap - add employeees, remove managers and directors
        logger->trace("ROLE CHANGE: Updating rules to add back the employees and remove mgrs/dirs");
        update_role(test_rules, {{"role", "employee"}});
        update_perms_and_verify(*harness, realm_1, test_rules, {BootstrapMode::MultiMessage, params.num_emps, 0, 0});
        // Single message/multi-changeset bootstrap - add back the managers and directors
        logger->trace("ROLE CHANGE: Updating rules to allow all records");
        update_role(test_rules, true);
        update_perms_and_verify(
            *harness, realm_1, test_rules,
            {BootstrapMode::SingleMessageMulti, params.num_emps, params.num_mgrs, params.num_dirs});
    }
    SECTION("Role changes for one user do not change unaffected user") {
        // Get the config for the first user
        auto config_1 = harness->make_test_file();

        // Start with a default rule that only allows access to the employee records
        AppCreateConfig::ServiceRole general_role{"default"};
        general_role.document_filters.read = {{"role", "employee"}};
        general_role.document_filters.write = {{"role", "employee"}};

        test_rules["roles"][0] = {transform_service_role(general_role)};
        harness->do_with_new_realm([&](SharedRealm new_realm) {
            set_up_realm(new_realm, num_total);

            // Add the initial rule and verify the data in realm 1 and 2 (both should just have the employees)
            update_perms_and_verify(*harness, realm_1, test_rules, {BootstrapMode::Any, params.num_emps, 0, 0});
            REQUIRE(!wait_for_download(*new_realm));
            REQUIRE(!wait_for_upload(*new_realm));
            wait_for_advance(*new_realm);
            verify_records(new_realm, params.num_emps, 0, 0);
        });
        {
            // Create another user and a new realm config for that user
            create_user_and_log_in(harness->app());
            auto config_2 = harness->make_test_file();
            REQUIRE(config_1.sync_config->user->user_id() != config_2.sync_config->user->user_id());
            std::atomic<bool> test_started = false;

            // Reopen realm 2 and add a hook callback to check for bootstraps, which should not happen
            // on this realm
            config_2.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession>,
                                                                  const SyncClientHookData& data) {
                using Event = SyncClientHookEvent;
                if (!test_started.load()) {
                    return SyncClientHookAction::NoAction; // Not checking yet
                }
                // If a download message was received or bootstrap was processed, then fail the test
                if ((data.event == Event::DownloadMessageReceived &&
                     data.batch_state != sync::DownloadBatchState::SteadyState) ||
                    data.event == Event::BootstrapMessageProcessed || data.event == Event::BootstrapProcessed) {
                    FAIL("Bootstrap occurred on the second realm, which was not expected");
                }
                return SyncClientHookAction::NoAction;
            };
            auto realm_2 = Realm::get_shared_realm(config_2);
            set_up_realm(realm_2, params.num_emps);

            test_started = true;
            // The first rule allows access to all records for user 1
            AppCreateConfig::ServiceRole user1_role{"user 1 role"};
            user1_role.apply_when = {{"%%user.id", config_1.sync_config->user->user_id()}};
            // Add two rules, the first applies to user 1 and the second applies to other users
            test_rules["roles"] = {transform_service_role(user1_role), transform_service_role(general_role)};
            update_perms_and_verify(*harness, realm_1, test_rules,
                                    {BootstrapMode::Any, params.num_emps, params.num_mgrs, params.num_dirs});

            // Realm 2 data should not change (and there shouldn't be any bootstrap messages)
            verify_records(realm_2, params.num_emps, 0, 0);

            // The first rule will be updated to only have access to employee and managers
            AppCreateConfig::ServiceRole user1_role_2 = user1_role;
            user1_role_2.document_filters.read = {{"role", {{"$in", {"employee", "manager"}}}}};
            user1_role_2.document_filters.write = {{"role", {{"$in", {"employee", "manager"}}}}};
            // Update the first rule for user 1 and verify the data after the rule is applied
            test_rules["roles"][0] = {transform_service_role(user1_role_2)};
            update_perms_and_verify(*harness, realm_1, test_rules,
                                    {BootstrapMode::Any, params.num_emps, params.num_mgrs, 0});

            // Realm 2 data should not change (and there shouldn't be any bootstrap messages)
            verify_records(realm_2, params.num_emps, 0, 0);
        }
    }

    // Add new sections before this
    SECTION("Pending changes are lost if not allowed after role change") {
        std::vector<ObjectId> emp_ids;
        std::vector<ObjectId> mgr_ids;
        auto config = harness->make_test_file();
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            REQUIRE(!error.is_fatal); // No fatal errors please
            // Expecting a compensating write error
            REQUIRE(error.status == ErrorCodes::SyncCompensatingWrite);
        };
        auto test_realm = Realm::get_shared_realm(config);
        set_up_realm(test_realm, num_total);
        // Perform the local updates offline
        test_realm->sync_session()->shutdown_and_wait();
        // Modify a set of records with new roles and create some new records as well
        // This should be called offline so the changes aren't sync'ed prematurely
        auto update_records = [](SharedRealm update_realm, std::string_view role_to_change,
                                 std::vector<ObjectId>& saved_ids, size_t num_to_modify, size_t num_to_create) {
            update_realm->begin_transaction();
            auto table = update_realm->read_group().get_table("class_Person");
            auto id_col = table->get_column_key("_id");
            auto role_col = table->get_column_key("role");
            auto name_col = table->get_column_key("name");
            auto empid_col = table->get_column_key("emp_id");
            auto table_query = Query(table).equal(role_col, role_to_change.data());
            auto results = Results(update_realm, table_query);
            REQUIRE(results.size() > 0);
            // Modify the role of some existing objects
            for (size_t i = 0; i < num_to_modify; i++) {
                auto obj = results.get(i);
                saved_ids.push_back(obj.get<ObjectId>(id_col));
                obj.set(role_col, "worker-bee");
            }
            // And create some new objects
            for (size_t i = 0; i < num_to_create; i++) {
                auto obj = table->create_object_with_primary_key(ObjectId::gen());
                obj.set(role_col, role_to_change.data());
                obj.set(name_col, util::format("%1-%2(new)", role_to_change.data(), i));
                obj.set(empid_col, static_cast<int64_t>(i + 2500)); // actual # doesnt matter
            }
            update_realm->commit_transaction();
        };
        auto do_update_rules = [&](nlohmann::json new_rules) {
            update_role(test_rules, new_rules);
            logger->debug("ROLE CHANGE: Updating rule definitions: %1", test_rules);
            app_session.admin_api.update_default_rule(app_session.server_app_id, test_rules);
        };
        auto do_verify = [](SharedRealm realm, size_t cnt, std::vector<ObjectId>& saved_ids,
                            std::optional<std::string_view> expected = std::nullopt) {
            REQUIRE(!wait_for_download(*realm));
            REQUIRE(!wait_for_upload(*realm));
            wait_for_advance(*realm);
            // Verify none of the records modified above exist in the realm
            auto table = realm->read_group().get_table("class_Person");
            REQUIRE(table->size() == cnt);
            auto id_col = table->get_column_key("_id");
            auto role_col = table->get_column_key("role");
            for (auto& id : saved_ids) {
                auto objkey = table->find_first(id_col, id);
                if (expected) {
                    REQUIRE(objkey);
                    auto obj = table->get_object(objkey);
                    REQUIRE(obj.get<String>(role_col) == *expected);
                }
                else {
                    REQUIRE(!objkey);
                }
            }
        };
        // Update the rules so employees are not allowed and removed from view
        // This will also remove the existing changes to the 10 employee records
        // and the 5 new employee records.
        size_t num_to_create = 5;
        // Update 10 employees to worker-bee and create 5 new employees
        update_records(test_realm, "employee", emp_ids, 10, num_to_create);
        // Update 5 managers to worker-bee and create 5 new managers
        update_records(test_realm, "manager", mgr_ids, 5, num_to_create);
        // Update the allowed roles to "manager" and "worker-bee"
        do_update_rules({{"role", {{"$in", {"manager", "worker-bee"}}}}});
        // Resume the session and verify none of the new/modified employee
        // records are present
        test_realm->sync_session()->resume();
        // Verify none of the employee object IDs are present in the local data
        do_verify(test_realm, params.num_mgrs + num_to_create, emp_ids, std::nullopt);
        // Verify all of the manager object IDs are present in the local data
        do_verify(test_realm, params.num_mgrs + num_to_create, mgr_ids, "worker-bee");

        // Update the allowed roles to "employee"
        do_update_rules({{"role", "employee"}});
        // Verify the items with the object IDs are still listed as employees
        do_verify(test_realm, params.num_emps, emp_ids, "employee");

        // Tear down the app since some of the records were added and modified
        teardown_harness(harness);
    }
    SECTION("teardown") {
        teardown_harness(harness);
    }
}

TEST_CASE("flx: role changes during bootstrap complete successfully", "[sync][flx][baas][role change][bootstrap]") {
    auto logger = util::Logger::get_default_logger();
    static std::unique_ptr<FLXSyncTestHarness> harness;

    // 150 emps, 25 mgrs, 10 dirs
    // 10 objects before flush
    // 1536 download soft max bytes
    TestParams params{};
    params.max_download_bytes = 1536;
    if (!harness) {
        harness = setup_harness("flx_role_change_during_bs", params);
    }
    REQUIRE(harness);

    // Get the current rules so it can be updated during the test
    auto& app_session = harness->session().app_session();
    auto default_rule = app_session.admin_api.get_default_rule(app_session.server_app_id);

    // Make sure the rules are reset back to the original value (all records allowed)
    update_role(default_rule, true);
    logger->debug("ROLE CHANGE: Initial rule definitions: %1", default_rule);
    REQUIRE(app_session.admin_api.update_default_rule(app_session.server_app_id, default_rule));

    enum BootstrapTestState {
        not_ready,
        start,
        ident_sent,
        reconnect_received,
        downloading,
        downloaded,
        integrating,
        integration_complete,
        complete
    };

    BootstrapTestState update_role_state = BootstrapTestState::not_ready;
    int update_msg_count = -1;
    int bootstrap_count = 0;
    int bootstrap_msg_count = 0;
    bool session_restarted = false;
    std::optional<util::Future<void>> role_change_future;
    TestingStateMachine<BootstrapTestState> bootstrap_state(BootstrapTestState::not_ready);

    auto setup_config_callbacks = [&](SyncTestFile& config) {
        // Use the sync client event hook to check for the error received and for tracking
        // download messages and bootstraps
        config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession>,
                                                            const SyncClientHookData& data) {
            bootstrap_state.transition_with([&](BootstrapTestState cur_state) -> std::optional<BootstrapTestState> {
                using BatchState = sync::DownloadBatchState;
                using Event = SyncClientHookEvent;
                // Keep track of the number of bootstraps that have occurred, regardless of cur state
                if (data.event == Event::BootstrapProcessed) {
                    bootstrap_count++;
                }

                // Has the test started?
                if (cur_state == BootstrapTestState::not_ready)
                    return std::nullopt;

                std::optional<BootstrapTestState> new_state = std::nullopt;

                switch (data.event) {
                    case Event::IdentMessageSent:
                        new_state = BootstrapTestState::ident_sent;
                        break;

                    case Event::ErrorMessageReceived:
                        REQUIRE(data.error_info);
                        if (data.error_info->raw_error_code == 200) {
                            REQUIRE(data.error_info->server_requests_action ==
                                    sync::ProtocolErrorInfo::Action::Transient);
                            REQUIRE_FALSE(data.error_info->is_fatal);
                            session_restarted = true;
                        }
                        break;

                    // A bootstrap message was processed
                    case Event::BootstrapMessageProcessed:
                        bootstrap_msg_count++;
                        if (data.batch_state == BatchState::LastInBatch) {
                            new_state = BootstrapTestState::downloaded;
                        }
                        else if (data.batch_state == BatchState::MoreToCome) {
                            new_state = BootstrapTestState::downloading;
                        }
                        break;

                    case SyncClientHookEvent::DownloadMessageIntegrated:
                        if (data.batch_state == BatchState::SteadyState) {
                            break;
                        }
                        REQUIRE((cur_state == BootstrapTestState::downloaded ||
                                 cur_state == BootstrapTestState::integrating));
                        new_state = BootstrapTestState::integrating;
                        break;

                    // The bootstrap has been received and processed
                    case Event::BootstrapProcessed:
                        REQUIRE(cur_state == BootstrapTestState::integrating);
                        new_state = BootstrapTestState::integration_complete;
                        break;

                    default:
                        break;
                }
                // If the state is changing and a role change is requested for that state, then
                // update the role now.
                if (new_state && new_state == update_role_state &&
                    update_role_state != BootstrapTestState::not_ready && bootstrap_msg_count >= update_msg_count) {
                    logger->debug("ROLE CHANGE: Updating rule definitions: %1", default_rule);
                    REQUIRE(app_session.admin_api.update_default_rule(app_session.server_app_id, default_rule));
                    update_role_state = BootstrapTestState::not_ready; // Bootstrap tracking is complete
                }
                return new_state;
            });
            return SyncClientHookAction::NoAction;
        };

        // Add client reset callback to verify a client reset doesn't happen
        config.sync_config->notify_before_client_reset = [&](std::shared_ptr<Realm>) {
            // Make sure a client reset did not occur while waiting for the role change to
            // be applied
            FAIL("Client reset is not expected when the role/rules/permissions are changed");
        };
    };

    auto set_role_change_state = [&](BootstrapTestState change_state, int msg_count = -1) {
        bootstrap_state.transition_with([&](BootstrapTestState) {
            bootstrap_count = 0;
            bootstrap_msg_count = 0;
            update_role_state = change_state;
            update_msg_count = msg_count;
            return BootstrapTestState::start;
        });
    };

    // Create the shared realm and configure a subscription for the manager and director records
    auto config = harness->make_test_file();
    setup_config_callbacks(config);

    SECTION("Role change during initial schema bootstrap") {
        set_role_change_state(BootstrapTestState::ident_sent); // Only one download message
        auto realm_1 = Realm::get_shared_realm(config);
        REQUIRE(!wait_for_download(*realm_1));
        REQUIRE(!wait_for_upload(*realm_1));
        bootstrap_state.transition_with([&](BootstrapTestState) {
            // Only the initial schema bootstrap with 1 download message should take place
            // without restarting the session
            REQUIRE(bootstrap_count == 1);
            REQUIRE(bootstrap_msg_count == 1);
            REQUIRE_FALSE(session_restarted);
            return std::nullopt;
        });
    }
    SECTION("Role change during subscription bootstrap") {
        auto realm_1 = Realm::get_shared_realm(config);
        bool initial_subscription = GENERATE(false, true);

        if (initial_subscription) {
            auto table = realm_1->read_group().get_table("class_Person");
            auto role_col = table->get_column_key("role");
            auto sub_query = Query(table).equal(role_col, "manager").Or().equal(role_col, "director");
            auto new_subs = realm_1->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(sub_query);
            auto subs = new_subs.commit();

            // Wait for subscription bootstrap to and sync to complete
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            REQUIRE(!wait_for_download(*realm_1));
            REQUIRE(!wait_for_upload(*realm_1));
            wait_for_advance(*realm_1);

            // Verify the data was downloaded and only includes managers and directors
            verify_records(realm_1, 0, params.num_mgrs, params.num_dirs);
        }

        // The test will update the rule to change access from all records to only the employee
        // records while a new subscription for all Person entries is being bootstrapped.
        update_role(default_rule, {{"role", "employee"}});

        // Set up a new bootstrap while offline
        realm_1->sync_session()->shutdown_and_wait();
        {
            auto table = realm_1->read_group().get_table("class_Person");
            auto new_subs = realm_1->get_latest_subscription_set().make_mutable_copy();
            new_subs.clear();
            new_subs.insert_or_assign(Query(table));
            auto subs = new_subs.commit();
            SECTION("During bootstrap download") {
                logger->debug("ROLE CHANGE: Role change during %1 query bootstrap download",
                              initial_subscription ? "second" : "first");
                // Wait for the downloading state and 3 messages have been downloaded
                set_role_change_state(BootstrapTestState::downloading, 3);
            }
            SECTION("After bootstrap downloaded") {
                logger->debug("ROLE CHANGE: Role change after %1 query bootstrap download",
                              initial_subscription ? "second" : "first");
                // Wait for the downloaded state
                set_role_change_state(BootstrapTestState::downloaded);
            }
            SECTION("During bootstrap integration") {
                logger->debug("ROLE CHANGE: Role change during %1 query bootstrap integration",
                              initial_subscription ? "second" : "first");
                // Wait for bootstrap messages to be integrated
                set_role_change_state(BootstrapTestState::integrating);
            }
            SECTION("After bootstrap integration") {
                logger->debug("ROLE CHANGE: Role change after %1 query bootstrap integration",
                              initial_subscription ? "second" : "first");
                // Wait for the end of the bootstrap integration
                set_role_change_state(BootstrapTestState::integration_complete);
            }

            // Resume the session an wait for subscription bootstrap to and sync to complete
            realm_1->sync_session()->resume();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            REQUIRE(!wait_for_download(*realm_1));
            REQUIRE(!wait_for_upload(*realm_1));
            wait_for_advance(*realm_1);

            bootstrap_state.transition_with([&](BootstrapTestState) {
                // Two bootstraps occurred (role change and subscription)
                // and the session was restarted with 200 error.
                REQUIRE(session_restarted);
                REQUIRE(bootstrap_count == 2);
                REQUIRE(bootstrap_msg_count > 1);
                return std::nullopt;
            });

            // Verify the data was downloaded/updated (only the employee records)
            verify_records(realm_1, params.num_emps, 0, 0);
        }
    }
    SECTION("teardown") {
        teardown_harness(harness);
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
