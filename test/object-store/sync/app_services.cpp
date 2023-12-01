////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#include "util/sync/sync_test_utils.hpp"
#include "util/unit_test_transport.hpp"
#include "util/test_file.hpp"

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/uuid.hpp>

#include <catch2/catch_all.hpp>

using namespace realm;
using namespace realm::app;

namespace {
std::shared_ptr<SyncUser> log_in(std::shared_ptr<App> app, AppCredentials credentials = AppCredentials::anonymous())
{
    if (auto transport = dynamic_cast<UnitTestTransport*>(app->config().transport.get())) {
        transport->set_provider_type(credentials.provider_as_string());
    }
    std::shared_ptr<SyncUser> user;
    app->log_in_with_credentials(credentials,
                                 [&](std::shared_ptr<SyncUser> user_arg, util::Optional<AppError> error) {
                                     REQUIRE_FALSE(error);
                                     REQUIRE(user_arg);
                                     user = std::move(user_arg);
                                 });
    REQUIRE(user);
    return user;
}

AppError failed_log_in(std::shared_ptr<App> app, AppCredentials credentials = AppCredentials::anonymous())
{
    util::Optional<AppError> err;
    app->log_in_with_credentials(credentials, [&](std::shared_ptr<SyncUser> user, util::Optional<AppError> error) {
        REQUIRE(error);
        REQUIRE_FALSE(user);
        err = error;
    });
    REQUIRE(err);
    return *err;
}

} // namespace

namespace realm {
class TestHelper {
public:
    static DBRef get_db(Realm& realm)
    {
        return Realm::Internal::get_db(realm);
    }
};
} // namespace realm

#if !REALM_ENABLE_AUTH_TESTS || !defined(REALM_MONGODB_ENDPOINT)
static_assert(false, "These tests require a MongoDB instance")
#endif

    TEST_CASE("app services: log in integration", "[app][user][baas][services]")
{
    SECTION("login") {
        TestAppSession session;
        auto app = session.app();
        app->log_out([](auto) {});

        int subscribe_processed = 0;
        auto token = app->subscribe([&subscribe_processed](auto& app) {
            if (!subscribe_processed) {
                REQUIRE(app.current_user());
            }
            else {
                REQUIRE_FALSE(app.current_user());
            }
            subscribe_processed++;
        });

        auto user = log_in(app);
        CHECK(!user->device_id().empty());
        CHECK(user->has_device_id());

        bool processed = false;
        app->log_out([&](auto error) {
            REQUIRE_FALSE(error);
            processed = true;
        });

        CHECK(processed);
        CHECK(subscribe_processed == 2);

        app->unsubscribe(token);
    }
}
