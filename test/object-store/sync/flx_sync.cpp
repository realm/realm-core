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

#include <catch2/catch.hpp>

#include "sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"
#include "util/test_file.hpp"

#if REALM_ENABLE_AUTH_TESTS && REALM_ENABLE_FLX_SYNC

namespace realm::app {
TEST_CASE("flx: connect to FLX-enabled app", "[sync][flx][app]") {
    const std::string base_url = get_base_url();

    Schema schema{
        ObjectSchema("TopLevel",
                     {
                         {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                         {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                         {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
                         {"non_queryable_field", PropertyType::String | PropertyType::Nullable},
                     }),
    };

    auto server_app_config = minimal_app_config(base_url, "flx_connect", schema);
    AppCreateConfig::FLXSyncConfig flx_config;
    flx_config.queryable_fields["TopLevel"] = {"queryable_int_field", "queryable_str_field"};
    server_app_config.flx_sync_config = std::move(flx_config);
    auto app_session = create_app(server_app_config);
    auto app_config = get_config(instance_of<SynchronousTestTransport>, app_session);

    TestSyncManager::Config smc(app_config);
    smc.verbose_sync_client_logging = true;
    TestSyncManager sync_manager(std::move(smc), {});
    auto app = sync_manager.app();

    auto creds = create_user_and_log_in(app);
    std::shared_ptr<SyncUser> user;
    app->log_in_with_credentials(creds, [&](std::shared_ptr<SyncUser> user_arg, util::Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
        REQUIRE(user_arg);
        user = std::move(user_arg);
    });
    REQUIRE(user);

    SyncTestFile config(app, bson::Bson{}, schema);
    auto realm = Realm::get_shared_realm(config);
    CHECK(!wait_for_download(*realm));
    CHECK(!wait_for_upload(*realm));

    CHECK(realm->sync_session()->has_flx_subscription_store());
    CHECK(realm->get_latest_subscription_set().size() == 0);
    CHECK(realm->get_active_subscription_set().size() == 0);
}

TEST_CASE("flx: no subscription store created for PBS app", "[sync][flx][app]") {
    const std::string base_url = get_base_url();

    Schema schema{
        ObjectSchema("TopLevel",
                     {
                         {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                     }),
    };

    auto server_app_config = minimal_app_config(base_url, "flx_connect_as_pbs", schema);
    auto app_session = create_app(server_app_config);
    auto app_config = get_config(instance_of<SynchronousTestTransport>, app_session);

    TestSyncManager::Config smc(app_config);
    TestSyncManager sync_manager(std::move(smc), {});
    auto app = sync_manager.app();

    auto creds = create_user_and_log_in(app);
    std::shared_ptr<SyncUser> user;
    app->log_in_with_credentials(creds, [&](std::shared_ptr<SyncUser> user_arg, util::Optional<app::AppError> error) {
        REQUIRE_FALSE(error);
        REQUIRE(user_arg);
        user = std::move(user_arg);
    });
    REQUIRE(user);

    SyncTestFile config(app, bson::Bson{}, schema);
    auto realm = Realm::get_shared_realm(config);
    CHECK(!wait_for_download(*realm));
    CHECK(!wait_for_upload(*realm));

    CHECK(!realm->sync_session()->has_flx_subscription_store());
}
} // namespace realm::app

#endif
