////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include "feature_checks.hpp"

#if REALM_HAVE_SYNC_STABLE_IDS

#include "sync_test_utils.hpp"

#include "shared_realm.hpp"
#include "object.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"

#include "impl/object_accessor_impl.hpp"
#include "sync/partial_sync.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include <realm/util/optional.hpp>

using namespace realm;

using TypeATuple = std::tuple<size_t, size_t, std::string>;
using TypeBTuple = std::tuple<size_t, std::string, std::string>;

enum class PartialSyncTestObjects { A, B };

// Test helpers.
namespace {

Schema partial_sync_schema()
{
    return Schema{
        {"partial_sync_object_a", {
            {"first_number", PropertyType::Int},
            {"second_number", PropertyType::Int},
            {"string", PropertyType::String}
        }},
        {"partial_sync_object_b", {
            {"number", PropertyType::Int},
            {"first_string", PropertyType::String},
            {"second_string", PropertyType::String},
        }}
    };
}

void populate_realm(Realm::Config& config, std::vector<TypeATuple> a={}, std::vector<TypeBTuple> b={})
{    
    auto r = Realm::get_shared_realm(config);
    r->begin_transaction();
    {   
        const auto& object_schema = *r->schema().find("partial_sync_object_a");
        const auto& first_number_prop = *object_schema.property_for_name("first_number");
        const auto& second_number_prop = *object_schema.property_for_name("second_number");
        const auto& string_prop = *object_schema.property_for_name("string");
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "partial_sync_object_a");
        for (size_t i = 0; i < a.size(); ++i) {
            auto current = a[i];
            size_t row_idx = sync::create_object(r->read_group(), *table);
            table->set_int(first_number_prop.table_column, row_idx, std::get<0>(current));
            table->set_int(second_number_prop.table_column, row_idx, std::get<1>(current));
            table->set_string(string_prop.table_column, row_idx, std::get<2>(current));
        }
    }
    {
        const auto& object_schema = *r->schema().find("partial_sync_object_b");
        const auto& number_prop = *object_schema.property_for_name("number");
        const auto& first_string_prop = *object_schema.property_for_name("first_string");
        const auto& second_string_prop = *object_schema.property_for_name("second_string");
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "partial_sync_object_b");
        for (size_t i = 0; i < b.size(); ++i) {
            auto current = b[i];
            size_t row_idx = sync::create_object(r->read_group(), *table);
            table->set_int(number_prop.table_column, row_idx, std::get<0>(current));
            table->set_string(first_string_prop.table_column, row_idx, std::get<1>(current));
            table->set_string(second_string_prop.table_column, row_idx, std::get<2>(current));
        }
    }
    r->commit_transaction();
    // Wait for uploads
    std::atomic<bool> upload_done(false);
    auto session = SyncManager::shared().get_existing_active_session(config.path);
    session->wait_for_download_completion([&](auto) { upload_done = true; });
    EventLoop::main().run_until([&] { return upload_done.load(); });
}

/// Run a partial sync query, wait for the results, and then perform checks.
void run_query(const std::string& query, const Realm::Config& partial_config, PartialSyncTestObjects type, std::function<void(Results)> check)
{
    std::atomic<bool> partial_sync_done(false);
    auto r = Realm::get_shared_realm(partial_config);
    Results results;
    partial_sync::register_query(r,
                                 type == PartialSyncTestObjects::A ? "partial_sync_object_a" : "partial_sync_object_b",
                                 query,
                                 [&](Results r, std::exception_ptr) {
                                     partial_sync_done = true;
                                     results = std::move(r);
                                 });
    EventLoop::main().run_until([&] { return partial_sync_done.load(); });
    check(std::move(results));
}

bool results_contains(Results& r, TypeATuple a)
{
    CppContext ctx;
    SharedRealm realm = r.get_realm();
    const ObjectSchema os = *realm->schema().find("partial_sync_object_a");
    for (size_t i=0; i<r.size(); ++i) {
        Object obj(realm, os, r.get(i));
        size_t first = any_cast<int64_t>(obj.get_property_value<util::Any>(ctx, "first_number"));
        size_t second = any_cast<int64_t>(obj.get_property_value<util::Any>(ctx, "second_number"));
        auto str = any_cast<std::string>(obj.get_property_value<util::Any>(ctx, "string"));
        if (first == std::get<0>(a) && second == std::get<1>(a) && str == std::get<2>(a))
            return true;
    }
    return false;
}

bool results_contains(Results& r, TypeBTuple b)
{
    CppContext ctx;
    SharedRealm realm = r.get_realm();
    const ObjectSchema os = *realm->schema().find("partial_sync_object_b");
    for (size_t i=0; i<r.size(); ++i) {
        Object obj(realm, os, r.get(i));
        size_t number = any_cast<int64_t>(obj.get_property_value<util::Any>(ctx, "number"));
        auto first_str = any_cast<std::string>(obj.get_property_value<util::Any>(ctx, "first_string"));
        auto second_str = any_cast<std::string>(obj.get_property_value<util::Any>(ctx, "second_string"));
        if (number == std::get<0>(b) && first_str == std::get<1>(b) && second_str == std::get<2>(b))
            return true;
    }
    return false;
}

}

TEST_CASE("Partial sync", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    SyncServer server;
    SyncTestFile config(server, "test", partial_sync_schema());
    SyncTestFile partial_config(server, "test/__partial/123456", partial_sync_schema(), true);
    // Add some objects for test purposes.
    populate_realm(config, 
        {{1, 10, "partial"}, {2, 2, "partial"}, {3, 8, "sync"}},
        {{3, "meela", "orange"}, {4, "jyaku", "kiwi"}, {5, "meela", "cherry"}, {6, "meela", "kiwi"}, {7, "jyaku", "orange"}}
        );

    SECTION("works in the most basic case") {
        // Open the partially synced Realm and run a query.
        run_query("string = \"partial\"", partial_config, PartialSyncTestObjects::A, [](Results results) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {1, 10, "partial"}));
            REQUIRE(results_contains(results, {2, 2, "partial"}));
        });
    }

    SECTION("works when multiple queries are made on the same property") {
        run_query("first_number > 1", partial_config, PartialSyncTestObjects::A, [](Results results) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });

        run_query("first_number = 1", partial_config, PartialSyncTestObjects::A, [](Results results) {
            REQUIRE(results.size() == 1);
            REQUIRE(results_contains(results, {1, 10, "partial"}));
        });
    }

    SECTION("works when queries are made on different properties") {
        run_query("first_string = \"jyaku\"", partial_config, PartialSyncTestObjects::B, [](Results results) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {4, "jyaku", "kiwi"}));
            REQUIRE(results_contains(results, {7, "jyaku", "orange"}));
        });

        run_query("second_string = \"cherry\"", partial_config, PartialSyncTestObjects::B, [](Results results) {
            REQUIRE(results.size() == 1);
            REQUIRE(results_contains(results, {5, "meela", "cherry"}));
        });
    }

    SECTION("works when queries are made on different object types") {
        run_query("second_number < 9", partial_config, PartialSyncTestObjects::A, [](Results results) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });

        run_query("first_string = \"meela\"", partial_config, PartialSyncTestObjects::B, [](Results results) {
            REQUIRE(results.size() == 3);
            REQUIRE(results_contains(results, {3, "meela", "orange"}));
            REQUIRE(results_contains(results, {5, "meela", "cherry"}));
            REQUIRE(results_contains(results, {6, "meela", "kiwi"}));
        });
    }
}

#endif // REALM_HAVE_SYNC_STABLE_IDS
