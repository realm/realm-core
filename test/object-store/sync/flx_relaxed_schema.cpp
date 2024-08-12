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

TEST_CASE("flx: relaxed schema disabled in app returns error", "[sync][flx][relaxed schema][baas]") {
    // Relaxed schema feature is disabled by default
    FLXSyncTestHarness harness("relaxed-schema-disabled");

    auto config = harness.make_test_file();
    auto&& [error_future, error_handler] = make_error_handler();

    config.flexible_schema = true;
    config.sync_config->error_handler = error_handler;

    auto realm = Realm::get_shared_realm(config);
    auto error = wait_for_future(std::move(error_future)).get_no_throw();
    REQUIRE(error.is_ok());
    REQUIRE_FALSE(error.get_value().is_fatal);
    REQUIRE(error.get_value().status == ErrorCodes::SyncRelaxedSchemaError);
}

} // namespace realm::app

#endif // REALM_ENABLE_AUTH_TESTS
