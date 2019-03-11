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

#include "sync_test_utils.hpp"

#include "keypath_helpers.hpp"
#include "object.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "shared_realm.hpp"

#include "impl/object_accessor_impl.hpp"
#include "sync/partial_sync.hpp"
#include "sync/subscription_state.hpp"
#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/parser/parser.hpp>
#include <realm/parser/query_builder.hpp>
#include <realm/util/optional.hpp>

using namespace realm;
using namespace std::string_literals;

struct TypeA {
    size_t number;
    size_t second_number;
    std::string string;
    size_t link_id = realm::npos;
};

struct TypeB {
    size_t number;
    std::string string;
    std::string second_string;
};

struct TypeC {
    size_t number;
};

enum class PartialSyncTestObjects { A, B };

// Test helpers.
namespace {

// Creates a timestamp representing `now` as defined by the system clock.
Timestamp now()
{
    auto now = std::chrono::system_clock::now();
    int64_t ns_since_epoch = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    int64_t s_arg = ns_since_epoch / static_cast<int64_t>(realm::Timestamp::nanoseconds_per_second);
    int32_t ns_arg = ns_since_epoch % Timestamp::nanoseconds_per_second;
    return Timestamp(s_arg, ns_arg);
}

// Creates lowest possible date expressible.
Timestamp min()
{
    return Timestamp(INT64_MIN, -Timestamp::nanoseconds_per_second + 1);
}

// Creates highest possible date expressible.
Timestamp max()
{
    return Timestamp(INT64_MAX, Timestamp::nanoseconds_per_second - 1);
}

// Return a copy of this timestamp that has been adjusted by the given number of seconds. If the Timestamp
// overflows in a positive direction it clamps to Timestamp::max(). If it overflows in negative direction it clamps
// to Timestamp::min().
Timestamp add_seconds(Timestamp& ts, int64_t s)
{
    int64_t seconds = ts.get_seconds();
    if (util::int_add_with_overflow_detect(seconds, s)) {
        return (s < 0) ? min() : max();
    }
    else {
        return Timestamp(seconds, ts.get_nanoseconds());
    }
}

Schema partial_sync_schema()
{
    return Schema{
        {"object_a", {
            {"number", PropertyType::Int},
            {"second_number", PropertyType::Int},
            {"string", PropertyType::String},
            {"link", PropertyType::Object|PropertyType::Nullable, "link_target"},
        }},
        {"object_b", {
            {"number", PropertyType::Int},
            {"string", PropertyType::String},
            {"second_string", PropertyType::String},
        }},
        {"link_target", {
                {"id", PropertyType::Int}
            },{
                {"parents", PropertyType::LinkingObjects|PropertyType::Array, "object_a", "link"},
            }
        }
    };
}

void populate_realm(Realm::Config& config, std::vector<TypeA> a={}, std::vector<TypeB> b={}, std::vector<TypeC> c={})
{
    auto r = Realm::get_shared_realm(config);
    r->begin_transaction();
    {
        const auto& object_schema = *r->schema().find("link_target");
        const auto& id_prop = *object_schema.property_for_name("id");
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "link_target");
        for (auto& current : c) {
            size_t row_idx = sync::create_object(r->read_group(), *table);
            table->set_int(id_prop.table_column, row_idx, current.number);
        }
    }
    {
        auto find_row_ndx_for_link = [&r](int64_t link_id) {
            const auto& object_schema = *r->schema().find("link_target");
            const auto& id_prop = *object_schema.property_for_name("id");
            TableRef table = ObjectStore::table_for_object_type(r->read_group(), "link_target");
            size_t row_ndx = table->find_first_int(id_prop.table_column, link_id);
            if (row_ndx == realm::not_found) {
                throw std::runtime_error(util::format("Invalid test schema, cannot find 'link_target' with id %1", link_id));
            }
            return row_ndx;
        };
        const auto& object_schema = *r->schema().find("object_a");
        const auto& number_prop = *object_schema.property_for_name("number");
        const auto& second_number_prop = *object_schema.property_for_name("second_number");
        const auto& string_prop = *object_schema.property_for_name("string");
        const auto& link_prop = *object_schema.property_for_name("link");
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "object_a");
        for (auto& current : a) {
            size_t row_idx = sync::create_object(r->read_group(), *table);
            table->set_int(number_prop.table_column, row_idx, current.number);
            table->set_int(second_number_prop.table_column, row_idx, current.second_number);
            table->set_string(string_prop.table_column, row_idx, current.string);
            if (current.link_id != realm::npos) {
                table->set_link(link_prop.table_column, row_idx, find_row_ndx_for_link(current.link_id));
            }
        }
    }
    {
        const auto& object_schema = *r->schema().find("object_b");
        const auto& number_prop = *object_schema.property_for_name("number");
        const auto& string_prop = *object_schema.property_for_name("string");
        const auto& second_string_prop = *object_schema.property_for_name("second_string");
        TableRef table = ObjectStore::table_for_object_type(r->read_group(), "object_b");
        for (auto& current : b) {
            size_t row_idx = sync::create_object(r->read_group(), *table);
            table->set_int(number_prop.table_column, row_idx, current.number);
            table->set_string(string_prop.table_column, row_idx, current.string);
            table->set_string(second_string_prop.table_column, row_idx, current.second_string);
        }
    }
    r->commit_transaction();
    // Wait for uploads
    std::atomic<bool> upload_done(false);
    auto session = SyncManager::shared().get_existing_active_session(config.path);
    session->wait_for_upload_completion([&](auto) { upload_done = true; });
    EventLoop::main().run_until([&] { return upload_done.load(); });
}

auto results_for_query(std::string const& query_string, Realm::Config const& config, std::string const& object_type)
{
    auto realm = Realm::get_shared_realm(config);
    auto table = ObjectStore::table_for_object_type(realm->read_group(), object_type);
    Query query = table->where();
    auto parser_result = realm::parser::parse(query_string);
    query_builder::NoArguments no_args;
    query_builder::apply_predicate(query, parser_result.predicate, no_args);

    DescriptorOrdering ordering;
    query_builder::apply_ordering(ordering, table, parser_result.ordering);
    return Results(std::move(realm), std::move(query), std::move(ordering));
}

partial_sync::Subscription subscribe_and_wait(Results results, partial_sync::SubscriptionOptions options, std::function<void(Results, std::exception_ptr)> check)
{
    auto subscription = partial_sync::subscribe(results, options);

    bool partial_sync_done = false;
    std::exception_ptr exception;
    auto token = subscription.add_notification_callback([&] {
        switch (subscription.state()) {
            case partial_sync::SubscriptionState::Creating:
            case partial_sync::SubscriptionState::Pending:
                // Ignore these. They're temporary states.
                break;
            case partial_sync::SubscriptionState::Error:
                exception = subscription.error();
                partial_sync_done = true;
                break;
            case partial_sync::SubscriptionState::Complete:
            case partial_sync::SubscriptionState::Invalidated:
                partial_sync_done = true;
                break;
            default:
                throw std::logic_error(util::format("Unexpected state: %1", static_cast<uint8_t>(subscription.state())));
        }
    });
    EventLoop::main().run_until([&] { return partial_sync_done; });
    check(std::move(results), std::move(exception));
    return subscription;
}

partial_sync::Subscription subscribe_and_wait(Results results, util::Optional<std::string> name, util::Optional<int64_t> ttl,
                                              bool update, std::function<void(Results, std::exception_ptr)> check)
{
    partial_sync::SubscriptionOptions options{name, ttl, update};
    return subscribe_and_wait(results, options, check);

}

partial_sync::Subscription subscribe_and_wait(Results results, util::Optional<std::string> name, std::function<void(Results, std::exception_ptr)> check)
{
    return subscribe_and_wait(results, name, none, false, check);
}

partial_sync::Subscription subscribe_and_wait(std::string const& query, Realm::Config const& partial_config,
                                              std::string const& object_type, util::Optional<std::string> name,
                                              util::Optional<int64_t> ttl, bool update,
                                              std::function<void(Results, std::exception_ptr)> check)
{
    auto results = results_for_query(query, partial_config, object_type);
    return subscribe_and_wait(std::move(results), std::move(name), std::move(ttl), update, std::move(check));
}

/// Run a Query-based Sync query, wait for the results, and then perform checks.
partial_sync::Subscription subscribe_and_wait(std::string const& query, Realm::Config const& partial_config,
                        std::string const& object_type, util::Optional<std::string> name,
                        std::function<void(Results, std::exception_ptr)> check)
{
    return subscribe_and_wait(query, partial_config, object_type, name, none, false, check);
}

partial_sync::Subscription subscribe_and_wait(std::string const& query, Realm::Config const& partial_config,
                                              std::string const& object_type, partial_sync::SubscriptionOptions options,
                                              std::function<void(Results, std::exception_ptr)> check)
{
    auto results = results_for_query(query, partial_config, object_type);
    return subscribe_and_wait(results, options, check);
}

partial_sync::Subscription subscription_with_query(std::string const& query, Realm::Config const& partial_config,
                             std::string const& object_type, util::Optional<std::string> name)
{
    auto results = results_for_query(query, partial_config, object_type);
    return partial_sync::subscribe(std::move(results), {name});
}

bool results_contains(Results& r, TypeA a)
{
    CppContext ctx;
    SharedRealm realm = r.get_realm();
    const ObjectSchema os = *realm->schema().find("object_a");
    for (size_t i = 0; i < r.size(); ++i) {
        Object obj(realm, os, r.get(i));
        size_t first = any_cast<int64_t>(obj.get_property_value<util::Any>(ctx, "number"));
        size_t second = any_cast<int64_t>(obj.get_property_value<util::Any>(ctx, "second_number"));
        auto str = any_cast<std::string>(obj.get_property_value<util::Any>(ctx, "string"));
        if (first == a.number && second == a.second_number && str == a.string)
            return true;
    }
    return false;
}

bool results_contains(Results& r, TypeB b)
{
    CppContext ctx;
    SharedRealm realm = r.get_realm();
    const ObjectSchema os = *realm->schema().find("object_b");
    for (size_t i = 0;  i < r.size(); ++i) {
        Object obj(realm, os, r.get(i));
        size_t number = any_cast<int64_t>(obj.get_property_value<util::Any>(ctx, "number"));
        auto first_str = any_cast<std::string>(obj.get_property_value<util::Any>(ctx, "string"));
        auto second_str = any_cast<std::string>(obj.get_property_value<util::Any>(ctx, "second_string"));
        if (number == b.number && first_str == b.string && second_str == b.second_string)
            return true;
    }
    return false;
}

bool verify_results(SharedRealm realm, std::vector<TypeA> a_results, std::vector<TypeB> b_results, std::vector<TypeC> c_results)
{
    CppContext ctx;
    const ObjectSchema os_a = *realm->schema().find("object_a");
    const ObjectSchema os_b = *realm->schema().find("object_b");
    const ObjectSchema os_c = *realm->schema().find("link_target");
    TableRef table_a = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
    TableRef table_b = ObjectStore::table_for_object_type(realm->read_group(), "object_b");
    TableRef table_c = ObjectStore::table_for_object_type(realm->read_group(), "link_target");
    {
        if (table_a->size() != a_results.size()) {
            return false;
        }
        const auto& number_prop = *os_a.property_for_name("number");
        const auto& second_number_prop = *os_a.property_for_name("second_number");
        const auto& string_prop = *os_a.property_for_name("string");
        const auto& link_prop = *os_a.property_for_name("link");

        for (auto& a : a_results) {
            size_t a_ndx = table_a->find_first_int(number_prop.table_column, a.number);
            if (a_ndx == realm::not_found ||
                table_a->get_int(second_number_prop.table_column, a_ndx) != int64_t(a.second_number) ||
                table_a->get_string(string_prop.table_column, a_ndx) != a.string) {
                return false;
            }
            const auto& c_id_prop = *os_c.property_for_name("id");
            if (table_a->is_null_link(link_prop.table_column, a_ndx)) {
                if (a.link_id != realm::npos) {
                    return false;
                }
            } else {
                if (table_c->get_int(c_id_prop.table_column, table_a->get_link(link_prop.table_column, a_ndx))
                    != int64_t(a.link_id)) {
                    return false;
                }
            }
        }
    }
    {
        if (table_b->size() != b_results.size()) {
            return false;
        }
        const auto& number_prop = *os_b.property_for_name("number");
        const auto& string_prop = *os_b.property_for_name("string");
        const auto& second_string_prop = *os_b.property_for_name("second_string");

        for (auto& b : b_results) {
            size_t b_ndx = table_b->find_first_int(number_prop.table_column, b.number);
            if (b_ndx == realm::not_found ||
                table_b->get_string(string_prop.table_column, b_ndx) != b.string ||
                table_b->get_string(second_string_prop.table_column, b_ndx) != b.second_string) {
                return false;
            }
        }
    }
    {
        if (table_c->size() != c_results.size()) {
            return false;
        }
        const auto& id_prop = *os_c.property_for_name("id");
        for (auto& c : c_results) {
            if (table_c->find_first_int(id_prop.table_column, c.number) == realm::not_found) {
                return false;
            }
        }
    }

    return true;
}

}

TEST_CASE("Query-based Sync", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    SyncManager::shared().configure(tmp_dir(), SyncManager::MetadataMode::NoEncryption);

    SyncServer server;
    SyncTestFile config(server, "test");
    config.schema = partial_sync_schema();
    SyncTestFile partial_config(server, "test", true);
    partial_config.schema = partial_sync_schema();
    // Add some objects for test purposes.
    populate_realm(config,
        {{1, 10, "partial"}, {2, 2, "partial"}, {3, 8, "sync"}},
        {{3, "meela", "orange"}, {4, "jyaku", "kiwi"}, {5, "meela", "cherry"}, {6, "meela", "kiwi"}, {7, "jyaku", "orange"}}
        );

    SECTION("works in the most basic case") {
        // Open the partially synced Realm and run a query.
        auto subscription = subscribe_and_wait("string = \"partial\"", partial_config, "object_a", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {1, 10, "partial"}));
            REQUIRE(results_contains(results, {2, 2, "partial"}));
        });
    }

    SECTION("works when multiple queries are made on the same property") {
        subscribe_and_wait("number > 1", partial_config, "object_a", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });

        subscribe_and_wait("number = 1", partial_config, "object_a", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 1);
            REQUIRE(results_contains(results, {1, 10, "partial"}));
        });
    }

    SECTION("works when sort ascending and distinct are applied") {
        auto realm = Realm::get_shared_realm(partial_config);
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_b");
        bool ascending = true;
        Results partial_conditions(realm, *table);
        partial_conditions = partial_conditions.sort({{"number", ascending}}).distinct({"string"});
        partial_sync::Subscription subscription = subscribe_and_wait(partial_conditions, util::none, [](Results results, std::exception_ptr) {
                REQUIRE(results.size() == 2);
                REQUIRE(results_contains(results, {3, "meela", "orange"}));
                REQUIRE(results_contains(results, {4, "jyaku", "kiwi"}));
        });
        auto partial_realm = Realm::get_shared_realm(partial_config);
        auto partial_table = ObjectStore::table_for_object_type(partial_realm->read_group(), "object_b");
        REQUIRE(partial_table);
        REQUIRE(partial_table->size() == 2);
        Results partial_results(partial_realm, *partial_table);
        REQUIRE(partial_results.size() == 2);
        REQUIRE(results_contains(partial_results, {3, "meela", "orange"}));
        REQUIRE(results_contains(partial_results, {4, "jyaku", "kiwi"}));
    }

    SECTION("works when sort descending and distinct are applied") {
        auto realm = Realm::get_shared_realm(partial_config);
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_b");
        bool ascending = false;
        Results partial_conditions(realm, *table);
        partial_conditions = partial_conditions.sort({{"number", ascending}}).distinct({"string"});
        subscribe_and_wait(partial_conditions, util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {6, "meela", "kiwi"}));
            REQUIRE(results_contains(results, {7, "jyaku", "orange"}));
        });
        auto partial_realm = Realm::get_shared_realm(partial_config);
        auto partial_table = ObjectStore::table_for_object_type(partial_realm->read_group(), "object_b");
        REQUIRE(partial_table);
        REQUIRE(partial_table->size() == 2);
        Results partial_results(partial_realm, *partial_table);
        REQUIRE(partial_results.size() == 2);
        REQUIRE(results_contains(partial_results, {6, "meela", "kiwi"}));
        REQUIRE(results_contains(partial_results, {7, "jyaku", "orange"}));
    }

    SECTION("works when queries are made on different properties") {
        subscribe_and_wait("string = \"jyaku\"", partial_config, "object_b", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {4, "jyaku", "kiwi"}));
            REQUIRE(results_contains(results, {7, "jyaku", "orange"}));
        });

        subscribe_and_wait("second_string = \"cherry\"", partial_config, "object_b", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 1);
            REQUIRE(results_contains(results, {5, "meela", "cherry"}));
        });
    }

    SECTION("works when queries are made on different object types") {
        subscribe_and_wait("second_number < 9", partial_config, "object_a", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });

        subscribe_and_wait("string = \"meela\"", partial_config, "object_b", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 3);
            REQUIRE(results_contains(results, {3, "meela", "orange"}));
            REQUIRE(results_contains(results, {5, "meela", "cherry"}));
            REQUIRE(results_contains(results, {6, "meela", "kiwi"}));
        });
    }

    SECTION("re-registering the same query with no name on the same type should succeed") {
        subscribe_and_wait("number > 1", partial_config, "object_a", util::none, [](Results results, std::exception_ptr error) {
            REQUIRE(!error);
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });

        subscribe_and_wait("number > 1", partial_config, "object_a", util::none, [](Results results, std::exception_ptr error) {
            REQUIRE(!error);
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });
    }

    SECTION("re-registering the same query with the same name on the same type should succeed") {
        subscribe_and_wait("number > 1", partial_config, "object_a", "query"s, [](Results results, std::exception_ptr error) {
            REQUIRE(!error);
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });

        subscribe_and_wait("number > 1", partial_config, "object_a", "query"s, [](Results results, std::exception_ptr error) {
            REQUIRE(!error);
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {2, 2, "partial"}));
            REQUIRE(results_contains(results, {3, 8, "sync"}));
        });
    }

    SECTION("unnamed query can be unsubscribed while in creating state") {
        auto subscription = subscription_with_query("number > 1", partial_config, "object_a", util::none);

        bool partial_sync_done = false;
        auto token = subscription.add_notification_callback([&] {
            using SubscriptionState = partial_sync::SubscriptionState;

            switch (subscription.state()) {
                case SubscriptionState::Complete:
                case SubscriptionState::Creating:
                    partial_sync::unsubscribe(subscription);
                    break;

                case SubscriptionState::Pending:
                case SubscriptionState::Error:
                    break;

                case SubscriptionState::Invalidated:
                    partial_sync_done = true;
                    break;
            }
        });
        EventLoop::main().run_until([&] { return partial_sync_done; });
    }

    SECTION("unnamed query can be unsubscribed while in pending state") {
        auto subscription = subscription_with_query("number > 1", partial_config, "object_a", util::none);

        bool partial_sync_done = false;
        auto token = subscription.add_notification_callback([&] {
            using SubscriptionState = partial_sync::SubscriptionState;

            switch (subscription.state()) {
                case SubscriptionState::Pending:
                    partial_sync::unsubscribe(subscription);
                    break;

                case SubscriptionState::Creating:
                case SubscriptionState::Error:
                case SubscriptionState::Complete:
                    break;

                case SubscriptionState::Invalidated:
                    partial_sync_done = true;
                    break;
            }
        });
        EventLoop::main().run_until([&] { return partial_sync_done; });
    }

    SECTION("unnamed query can be unsubscribed while in complete state") {
        auto subscription = subscription_with_query("number > 1", partial_config, "object_a", util::none);

        bool partial_sync_done = false;
        auto token = subscription.add_notification_callback([&] {
            using SubscriptionState = partial_sync::SubscriptionState;

            switch (subscription.state()) {
                case SubscriptionState::Complete:
                    partial_sync::unsubscribe(subscription);
                    break;

                case SubscriptionState::Creating:
                case SubscriptionState::Pending:
                case SubscriptionState::Error:
                    break;

                case SubscriptionState::Invalidated:
                    partial_sync_done = true;
                    break;
            }
        });
        EventLoop::main().run_until([&] { return partial_sync_done; });
    }

    SECTION("unnamed query can be unsubscribed while in invalidated state") {
        auto subscription = subscription_with_query("number > 1", partial_config, "object_a", util::none);
        partial_sync::unsubscribe(subscription);

        bool partial_sync_done = false;
        auto token = subscription.add_notification_callback([&] {
            using SubscriptionState = partial_sync::SubscriptionState;

            switch (subscription.state()) {
                case SubscriptionState::Creating:
                case SubscriptionState::Pending:
                case SubscriptionState::Complete:
                case SubscriptionState::Error:
                    break;

                case SubscriptionState::Invalidated:
                    // We're only testing that this doesn't blow up since it should have no effect.
                    partial_sync::unsubscribe(subscription);
                    partial_sync_done = true;
                    break;
            }
        });
        EventLoop::main().run_until([&] { return partial_sync_done; });
    }

    SECTION("unnamed query can be unsubscribed while in error state") {
        auto subscription_1 = subscription_with_query("number != 1", partial_config, "object_a", "query"s);
        auto subscription_2 = subscription_with_query("number > 1", partial_config, "object_a", "query"s);

        bool partial_sync_done = false;
        auto token = subscription_2.add_notification_callback([&] {
            using SubscriptionState = partial_sync::SubscriptionState;

            switch (subscription_2.state()) {
                case SubscriptionState::Error:
                    partial_sync::unsubscribe(subscription_2);
                    break;

                case SubscriptionState::Creating:
                case SubscriptionState::Pending:
                case SubscriptionState::Complete:
                    break;

                case SubscriptionState::Invalidated:
                    partial_sync_done = true;
                    break;
            }
        });
        EventLoop::main().run_until([&] { return partial_sync_done; });
    }

    SECTION("named query can be unsubscribed while in creating state without holding a strong reference to the subscription") {
        // Hold the write lock on the Realm so that the subscription can't actually be created
        auto config2 = partial_config;
        config2.cache = false;
        auto realm = Realm::get_shared_realm(config2);
        realm->begin_transaction();
        {
            // Create and immediately unsubscribe from the query
            auto subscription = subscription_with_query("number > 1", partial_config, "object_a", "subscription"s);
            partial_sync::unsubscribe(subscription);
        }
        realm->cancel_transaction();

        // Create another subscription with the same name but a different query
        // to verify that the first subscription was actually removed
        auto subscription2 = subscription_with_query("number > 2", partial_config, "object_a", "subscription"s);
        bool partial_sync_done = false;
        auto token = subscription2.add_notification_callback([&] {
            if (subscription2.state() != partial_sync::SubscriptionState::Creating)
                partial_sync_done = true;
        });
        EventLoop::main().run_until([&] { return partial_sync_done; });
    }

    SECTION("named query can be unsubscribed by looking up the object in the Realm") {
        auto subscription = subscription_with_query("number != 1", partial_config, "object_a", "query"s);
        EventLoop::main().run_until([&] { return subscription.state() == partial_sync::SubscriptionState::Complete; });

        auto realm = Realm::get_shared_realm(partial_config);
        auto table = ObjectStore::table_for_object_type(realm->read_group(), partial_sync::result_sets_type_name);
        ObjectSchema object_schema(realm->read_group(), partial_sync::result_sets_type_name);
        size_t row = table->find_first(table->get_column_index("name"), StringData("query"));
        Object subscription_object(realm, object_schema, table->get(row));

        partial_sync::unsubscribe(std::move(subscription_object));
        EventLoop::main().run_until([&] { return subscription.state() != partial_sync::SubscriptionState::Complete; });
    }

    SECTION("clearing a `Results` backed by a table works with Query-based sync") {
        // The `ClearTable` instruction emitted by `Table::clear` won't be supported on partially-synced Realms
        // going forwards. Currently it gives incorrect results. Verify that `Results::clear` backed by a table
        // uses something other than `Table::clear` and gives the results we expect.

        // Subscribe to a subset of `object_a` objects.
        auto subscription = subscribe_and_wait("number > 1", partial_config, "object_a", util::none, [&](Results results, std::exception_ptr error) {
            REQUIRE(!error);
            REQUIRE(results.size() == 2);

            // Remove all objects that matched our subscription.
            auto realm = results.get_realm();
            auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
            realm->begin_transaction();
            Results(realm, *table).clear();
            realm->commit_transaction();

            std::atomic<bool> upload_done(false);
            auto session = SyncManager::shared().get_existing_active_session(partial_config.path);
            session->wait_for_upload_completion([&](auto) { upload_done = true; });
            EventLoop::main().run_until([&] { return upload_done.load(); });
        });
        partial_sync::unsubscribe(subscription);

        // Ensure that all objects that matched our subscription above were removed, and that
        // the non-matching objects remain.
        subscribe_and_wait("TRUEPREDICATE", partial_config, "object_a", util::none, [](Results results, std::exception_ptr error) {
            REQUIRE(!error);
            REQUIRE(results.size() == 1);
        });
    }

    SECTION("works with Realm opened using `asyncOpen`") {
        // Perform an asynchronous open like bindings do by first opening the Realm without any schema,
        // waiting for the initial download to complete, and then re-opening the Realm with the correct schema.
        {
            Realm::Config async_partial_config(partial_config);
            async_partial_config.schema = {};
            async_partial_config.cache = false;

            auto async_realm = Realm::get_shared_realm(async_partial_config);
            std::atomic<bool> download_done(false);
            auto session = SyncManager::shared().get_existing_active_session(partial_config.path);
            session->wait_for_download_completion([&](auto) {
                download_done = true;
            });
            EventLoop::main().run_until([&] { return download_done.load(); });
        }

        subscribe_and_wait("string = \"partial\"", partial_config, "object_a", util::none, [](Results results, std::exception_ptr) {
            REQUIRE(results.size() == 2);
            REQUIRE(results_contains(results, {1, 10, "partial"}));
            REQUIRE(results_contains(results, {2, 2, "partial"}));
        });
    }

    SECTION("Updating a subscriptions query will download new data and remove old data") {
        auto realm = Realm::get_shared_realm(partial_config);
        subscribe_and_wait("truepredicate", partial_config, "object_a", "query"s, [&](Results, std::exception_ptr error) {
           REQUIRE(!error);
           auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
           REQUIRE(table->size() == 3);
        });

        subscribe_and_wait("number = 3", partial_config, "object_a", "query"s, none, true, [&](Results, std::exception_ptr error) {
            REQUIRE(!error);
            auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
            REQUIRE(table->size() == 1);
        });
    }

    SECTION("The same subscription state should not be reported twice until the Complete state ") {
        auto results = results_for_query("number > 1", partial_config, "object_a");
        auto subscription = partial_sync::subscribe(results, {"sub"s});
        bool partial_sync_done = false;
        std::exception_ptr exception;
        util::Optional<partial_sync::SubscriptionState> last_state = none;
        auto token = subscription.add_notification_callback([&] {
            auto new_state = subscription.state();
            if (last_state) {
                REQUIRE(last_state.value() != new_state);
            }
            last_state = new_state;
            switch (new_state) {
                case partial_sync::SubscriptionState::Creating:
                case partial_sync::SubscriptionState::Pending:
                case partial_sync::SubscriptionState::Error:
                case partial_sync::SubscriptionState::Invalidated:
                    break;
                case partial_sync::SubscriptionState::Complete:
                    partial_sync_done = true;
                    break;
                default:
                    throw std::logic_error(util::format("Unexpected state: %1", static_cast<uint8_t>(subscription.state())));
            }
        });

        // Also create the same subscription on the UI thread to force the subscription notifications to run.
        // This could potentially trigger the Pending state twice if this isn't prevented by the notification
        // handling.
        auto realm = Realm::get_shared_realm(partial_config);
        realm->begin_transaction();
        partial_sync::subscribe_blocking(results, "sub"s);
        realm->commit_transaction();

        EventLoop::main().run_until([&] { return partial_sync_done; });
    }

    SECTION("Manually deleting a Subscription also triggers the Invalidated state") {
        auto results = results_for_query("number > 1", partial_config, "object_a");
        auto subscription = partial_sync::subscribe(results, {"sub"s});
        bool subscription_created = false;
        bool subscription_deleted = false;
        std::exception_ptr exception;
        auto token = subscription.add_notification_callback([&] {
            if (subscription_created)
                // Next state after creating the subscription should be that it is deleted
                REQUIRE(subscription.state() == partial_sync::SubscriptionState::Invalidated);

            switch (subscription.state()) {
                case partial_sync::SubscriptionState::Creating:
                case partial_sync::SubscriptionState::Pending:
                case partial_sync::SubscriptionState::Error:
                    break;
                case partial_sync::SubscriptionState::Complete:
                    subscription_created = true;
                    break;
                case partial_sync::SubscriptionState::Invalidated:
                    subscription_deleted = true;
                    break;
                default:
                    throw std::logic_error(util::format("Unexpected state: %1", static_cast<uint8_t>(subscription.state())));
            }
        });

        EventLoop::main().run_until([&] { return subscription_created; });

        auto realm = Realm::get_shared_realm(partial_config);
        realm->begin_transaction();
        Results subs = results_for_query("name = 'sub'", partial_config, partial_sync::result_sets_type_name);
        subs.clear();
        realm->commit_transaction();

        EventLoop::main().run_until([&] { return subscription_deleted; });
    }

    SECTION("Updating a subscription will not report Complete from a previous subscription") {
        // Due to the asynchronous nature of updating subscriptions and listening to changes
        // in the query that returns the subscription there is a small chance that the query
        // returns before the update does. In that case, the previous state of the subscription
        // will be returned, i.e you might see `Complete` before it goes into `Pending`.
        // This test
        auto realm = Realm::get_shared_realm(partial_config);

        // Create initial subscription
        subscribe_and_wait("number > 1", partial_config, "object_a", "query"s, [&](Results, std::exception_ptr error) {
            REQUIRE(!error);
            auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
            REQUIRE(table->size() == 2);
        });

        // Start an update and verify that Complete is not called before Pending
        // Note: This is racy, so not 100% reproducible
        for (size_t i = 0; i < 100; ++i) {
            auto results = results_for_query((i % 2 == 0) ? "truepredicate" : "falsepredicate", partial_config, "object_a");
            auto subscription = partial_sync::subscribe(results, {"query"s, none, true});
            bool seen_completed_state = false;
            bool seen_pending_state = false;
            bool seen_complete_before_pending = false;
            auto token = subscription.add_notification_callback([&] {
                switch (subscription.state()) {
                    case partial_sync::SubscriptionState::Creating:
                    case partial_sync::SubscriptionState::Error:
                    case partial_sync::SubscriptionState::Invalidated:
                        break;
                    case partial_sync::SubscriptionState::Pending:
                        seen_complete_before_pending = seen_completed_state;
                        seen_pending_state = true;
                        break;
                    case partial_sync::SubscriptionState::Complete:
                        seen_completed_state = true;
                        break;
                    default:
                        throw std::logic_error(util::format("Unexpected state: %1", static_cast<uint8_t>(subscription.state())));
                }
            });
            EventLoop::main().run_until([&] { return seen_pending_state; });
            REQUIRE(!seen_complete_before_pending);
            EventLoop::main().run_until([&] { return seen_completed_state; });
            REQUIRE(results.size() == ((i % 2 == 0) ? 3 : 0));
        }
    }
}

TEST_CASE("Query-based Sync link behaviour", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    SyncManager::shared().configure(tmp_dir(), SyncManager::MetadataMode::NoEncryption);

    SyncServer server;
    SyncTestFile config(server, "test");
    config.schema = partial_sync_schema();
    SyncTestFile partial_config(server, "test", true);
    partial_config.schema = partial_sync_schema();
    // Add some objects for test purposes.
    std::vector<TypeA> a_objects = {{1, 10, "alpha", 1}, {2, 2, "bravo", 1}, {3, 8, "delta", 3}, {4, 10, "gamma"}};
    std::vector<TypeB> b_objects = {{100, "meela", "orange"}};
    std::vector<TypeC> c_objects = {{1}, {2}, {3}};
    populate_realm(config, a_objects, b_objects, c_objects);

    SECTION("subscribe to objects with no links") {
        auto subscription = subscribe_and_wait("TRUEPREDICATE", partial_config, "object_b", util::none, [&b_objects](Results results, std::exception_ptr) {
            // no a objects, all b objects, no c objects
            REQUIRE(verify_results(results.get_realm(), {}, b_objects, {}));
        });
    }
    SECTION("basic forward link closure") {
        auto subscription = subscribe_and_wait("TRUEPREDICATE", partial_config, "object_a", util::none, [&a_objects](Results results, std::exception_ptr) {
            // all a objects, no b objects, only c objects with a parent
            REQUIRE(verify_results(results.get_realm(), a_objects, {}, {{1}, {3}}));
        });
    }
    SECTION("link targets do not bring in backlinked parents by default") {
        auto subscription = subscribe_and_wait("TRUEPREDICATE", partial_config, "link_target", util::none, [&c_objects](Results results, std::exception_ptr) {
            // no a objects, no b objects, all c objects
            REQUIRE(verify_results(results.get_realm(), {}, {}, c_objects));
        });
    }
    SECTION("link targets bring in backlinked parents if requested") {
        auto realm = Realm::get_shared_realm(config);
        const ObjectSchema os_a = *realm->schema().find("object_a");
        TableRef table_a = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        TableRef table_c = ObjectStore::table_for_object_type(realm->read_group(), "link_target");
        const auto& link_prop = *os_a.property_for_name("link");
        partial_sync::SubscriptionOptions options;
        options.inclusions = IncludeDescriptor(*table_c, {{LinkPathPart(link_prop.table_column, table_a)}});
        auto subscription = subscribe_and_wait("TRUEPREDICATE", partial_config, "link_target", options, [&c_objects](Results results, std::exception_ptr) {
            // all a objects that have a valid link, no b objects, all c objects
            REQUIRE(verify_results(results.get_realm(),
                                   {{1, 10, "alpha", 1}, {2, 2, "bravo", 1}, {3, 8, "delta", 3}}, {}, c_objects));
        });
    }
    SECTION("link targets bring in backlinked parents if requested via verbose string property names") {
        auto realm = Realm::get_shared_realm(config);
        const ObjectSchema os_a = *realm->schema().find("object_a");
        const ObjectSchema os_c = *realm->schema().find("link_target");
        TableRef table_a = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        TableRef table_c = ObjectStore::table_for_object_type(realm->read_group(), "link_target");
        const auto& link_prop = *os_a.property_for_name("link");
        partial_sync::SubscriptionOptions options;
        std::vector<StringData> keypaths = { "@links.class_object_a.link" };
        parser::KeyPathMapping mapping;
        options.inclusions = generate_include_from_keypaths(keypaths, *realm, os_c, mapping);
        auto subscription = subscribe_and_wait("TRUEPREDICATE", partial_config, "link_target", options, [&c_objects](Results results, std::exception_ptr) {
            // all a objects that have a valid link, no b objects, all c objects
            REQUIRE(verify_results(results.get_realm(),
                                   {{1, 10, "alpha", 1}, {2, 2, "bravo", 1}, {3, 8, "delta", 3}}, {}, c_objects));
        });
    }
    SECTION("link targets bring in backlinked parents if requested via user defined string property names") {
        auto realm = Realm::get_shared_realm(config);
        const ObjectSchema os_a = *realm->schema().find("object_a");
        const ObjectSchema os_c = *realm->schema().find("link_target");
        TableRef table_a = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        TableRef table_c = ObjectStore::table_for_object_type(realm->read_group(), "link_target");
        const auto& link_prop = *os_a.property_for_name("link");
        partial_sync::SubscriptionOptions options;
        std::vector<StringData> keypaths = { "parents" };
        parser::KeyPathMapping mapping;
        alias_backlinks(mapping, *realm);
        options.inclusions = generate_include_from_keypaths(keypaths, *realm, os_c, mapping);
        auto subscription = subscribe_and_wait("TRUEPREDICATE", partial_config, "link_target", options, [&c_objects](Results results, std::exception_ptr) {
            // all a objects that have a valid link, no b objects, all c objects
            REQUIRE(verify_results(results.get_realm(),
                                   {{1, 10, "alpha", 1}, {2, 2, "bravo", 1}, {3, 8, "delta", 3}}, {}, c_objects));
        });
    }
    SECTION("inclusion generation for unaliased link targets are not found and will throw") {
        auto realm = Realm::get_shared_realm(config);
        const ObjectSchema os_a = *realm->schema().find("object_a");
        const ObjectSchema os_c = *realm->schema().find("link_target");
        TableRef table_a = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        TableRef table_c = ObjectStore::table_for_object_type(realm->read_group(), "link_target");
        const auto& link_prop = *os_a.property_for_name("link");
        partial_sync::SubscriptionOptions options;
        std::vector<StringData> keypaths = { "parents" };
        parser::KeyPathMapping mapping;
        // mapping is not populated by partial_sync::alias_backlinks(mapping, realm);
        REQUIRE_THROWS_WITH(generate_include_from_keypaths(keypaths, *realm, os_c, mapping),
                            "No property 'parents' on object of type 'link_target'");
    }
    SECTION("inclusion generation for link targets which are not a link will throw") {
        auto realm = Realm::get_shared_realm(config);
        const ObjectSchema os_a = *realm->schema().find("object_a");
        const ObjectSchema os_c = *realm->schema().find("link_target");
        TableRef table_a = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        TableRef table_c = ObjectStore::table_for_object_type(realm->read_group(), "link_target");
        const auto& link_prop = *os_a.property_for_name("link");
        partial_sync::SubscriptionOptions options;
        std::vector<StringData> keypaths = { "id" };
        parser::KeyPathMapping mapping;
        alias_backlinks(mapping, *realm);
        REQUIRE_THROWS_WITH(generate_include_from_keypaths(keypaths, *realm, os_c, mapping),
                            "Property 'id' is not a link in object of type 'link_target' in 'INCLUDE' clause");
    }
    SECTION("inclusion generation for link targets which do not exist will throw") {
        auto realm = Realm::get_shared_realm(config);
        const ObjectSchema os_a = *realm->schema().find("object_a");
        const ObjectSchema os_c = *realm->schema().find("link_target");
        TableRef table_a = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        TableRef table_c = ObjectStore::table_for_object_type(realm->read_group(), "link_target");
        const auto& link_prop = *os_a.property_for_name("link");
        partial_sync::SubscriptionOptions options;
        std::vector<StringData> keypaths = { "a_property_which_does_not_exist" };
        parser::KeyPathMapping mapping;
        alias_backlinks(mapping, *realm);
        REQUIRE_THROWS_WITH(generate_include_from_keypaths(keypaths, *realm, os_c, mapping),
                            "No property 'a_property_which_does_not_exist' on object of type 'link_target'");
    }
}

TEST_CASE("Query-based Sync error checking", "[sync]") {
    SyncManager::shared().configure(tmp_dir(), SyncManager::MetadataMode::NoEncryption);

    SECTION("API misuse throws an exception from `subscribe`") {
        SECTION("non-synced Realm") {
            TestFile config;
            config.schema = partial_sync_schema();
            auto realm = Realm::get_shared_realm(config);
            auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
            CHECK_THROWS(subscribe_and_wait(Results(realm, *table), util::none, [](Results, std::exception_ptr) { }));
        }

        SECTION("synced, non-partial Realm") {
            SyncServer server;
            SyncTestFile config(server, "test");
            config.schema = partial_sync_schema();
            auto realm = Realm::get_shared_realm(config);
            auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
            CHECK_THROWS(subscribe_and_wait(Results(realm, *table), util::none, [](Results, std::exception_ptr) { }));
        }
    }

    SECTION("subscription error handling") {
        SyncServer server;
        SyncTestFile config(server, "test");
        config.schema = partial_sync_schema();
        SyncTestFile partial_config(server, "test", true);
        partial_config.schema = partial_sync_schema();
        // Add some objects for test purposes.
        populate_realm(config,
            {{1, 10, "partial"}, {2, 2, "partial"}, {3, 8, "sync"}},
            {{3, "meela", "orange"}, {4, "jyaku", "kiwi"}, {5, "meela", "cherry"}, {6, "meela", "kiwi"}, {7, "jyaku", "orange"}},
            {{0}});

        SECTION("reusing the same name for different queries should raise an error") {
            subscribe_and_wait("number > 0", partial_config, "object_a", "query"s, [](Results results, std::exception_ptr error) {
                REQUIRE(!error);
                REQUIRE(results.size() == 3);
            });

            subscribe_and_wait("number <= 0", partial_config, "object_a", "query"s, [](Results, std::exception_ptr error) {
                REQUIRE(error);
            });
        }

        SECTION("reusing the same name for identical queries on different types should raise an error") {
            subscribe_and_wait("number > 0", partial_config, "object_a", "query"s, [](Results results, std::exception_ptr error) {
                REQUIRE(!error);
                REQUIRE(results.size() == 3);
            });

            subscribe_and_wait("number > 0", partial_config, "object_b", "query"s, [](Results, std::exception_ptr error) {
                REQUIRE(error);
            });

            // Trying to update the query will also fail
            subscribe_and_wait("number > 0", partial_config, "object_b", "query"s, none, true, [](Results, std::exception_ptr error) {
                REQUIRE(error);
            });

        }

        SECTION("unsupported queries should raise an error") {
            // To test handling of invalid queries, we rely on the fact that core does not yet support `links_to`
            // queries as it cannot serialize an object reference until we have stable ID support.

            // Ensure that the placeholder object in `link_target` is available.
            subscribe_and_wait("TRUEPREDICATE", partial_config, "link_target", util::none, [](Results results, std::exception_ptr error) {
                REQUIRE(!error);
                REQUIRE(results.size() == 1);
            });

            auto r = Realm::get_shared_realm(partial_config);
            const auto& object_schema = r->schema().find("object_a");
            auto source_table = ObjectStore::table_for_object_type(r->read_group(), "object_a");
            auto target_table = ObjectStore::table_for_object_type(r->read_group(), "link_target");

            // Attempt to subscribe to a `links_to` query.
            Query q = source_table->where().links_to(object_schema->property_for_name("link")->table_column,
                                                     target_table->get(0));
            CHECK_THROWS(partial_sync::subscribe(Results(r, q), {util::none}));
        }
    }
}

TEST_CASE("Creating/Updating subscriptions synchronously", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    using namespace std::string_literals;

    SyncManager::shared().configure(tmp_dir(), SyncManager::MetadataMode::NoEncryption);

    SyncServer server;
    SyncTestFile config(server, "test");
    config.schema = partial_sync_schema();
    SyncTestFile partial_config(server, "test", true);
    partial_config.schema = partial_sync_schema();

    auto realm = Realm::get_shared_realm(partial_config);
    auto subscription_table = ObjectStore::table_for_object_type(realm->read_group(), "__ResultSets");
    Results subscriptions(realm, *subscription_table);

    // Wait for the server-created subscriptions to be downloaded
    EventLoop::main().run_until([&] { return subscriptions.size() == 5; });

    size_t query_ndx = subscription_table->get_column_index(partial_sync::property_query);
    size_t name_ndx = subscription_table->get_column_index(partial_sync::property_name);
    size_t created_at_ndx = subscription_table->get_column_index(partial_sync::property_created_at);
    size_t updated_at_ndx = subscription_table->get_column_index(partial_sync::property_updated_at);
    size_t time_to_live_ndx = subscription_table->get_column_index(partial_sync::property_time_to_live);
    size_t expires_at_ndx = subscription_table->get_column_index(partial_sync::property_expires_at);

    SECTION("Create new unnamed subscription") {
        realm->begin_transaction();
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        Results user_query(realm, *table);
        partial_sync::subscribe_blocking(user_query, none);
        realm->commit_transaction();

        CHECK(subscriptions.size() == 6);
        auto sub = subscriptions.get(5);
        CHECK(sub.get_string(name_ndx) == "[object_a] TRUEPREDICATE"); // Name of subscription
        CHECK(sub.get_int(3) == static_cast<int64_t>(partial_sync::SubscriptionState::Pending));
        CHECK(sub.get_timestamp(created_at_ndx) == sub.get_timestamp(updated_at_ndx));
        CHECK(sub.is_null(time_to_live_ndx) == true);
        CHECK(sub.is_null(expires_at_ndx) == true);
    }

    SECTION("Create a new subscription with time-to-live")  {
        realm->begin_transaction();
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        Results user_query(realm, *table);
        Timestamp current_time = now();
        partial_sync::subscribe_blocking(user_query, "ttl-test"s, util::Optional<int64_t>(10000));
        realm->commit_transaction();

        CHECK(subscriptions.size() == 6);
        auto sub = subscriptions.get(5);
        CHECK(sub.get_string(name_ndx) == "ttl-test");
        CHECK(sub.get_timestamp(created_at_ndx) == sub.get_timestamp(updated_at_ndx));
        CHECK(sub.get_int(time_to_live_ndx) == 10000);
        CHECK(sub.get_timestamp(expires_at_ndx) < add_seconds(current_time, 11));
        CHECK(add_seconds(current_time, 9) < sub.get_timestamp(expires_at_ndx));
    }

    SECTION("Create existing subscription returns old row") {
        subscribe_and_wait("truepredicate", partial_config, "object_a", "sub"s, [](Results, std::exception_ptr error) {
            REQUIRE(!error);
        });

        CHECK(subscriptions.size() == 6);
        RowExpr old_sub = subscriptions.get(5);
        Timestamp old_updated = old_sub.get_timestamp(updated_at_ndx);
        Timestamp old_expires_at = old_sub.get_timestamp(expires_at_ndx);

        realm->begin_transaction();
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        Results user_query(realm, *table);
        RowExpr new_sub = partial_sync::subscribe_blocking(user_query, "sub"s);
        realm->commit_transaction();

        CHECK(subscriptions.size() == 6);
        CHECK(old_sub.get_index() == new_sub.get_index());
        CHECK(old_updated < new_sub.get_timestamp(updated_at_ndx));
        CHECK(old_expires_at == new_sub.get_timestamp(expires_at_ndx));
    }

    SECTION("Returning existing row updates expires_at") {
        realm->begin_transaction();
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        Results user_query(realm, *table);
        RowExpr old_sub = partial_sync::subscribe_blocking(user_query, "sub"s, util::Optional<int64_t>(1000));
        Timestamp old_updated = old_sub.get_timestamp(updated_at_ndx);
        Timestamp old_expires_at = old_sub.get_timestamp(expires_at_ndx);
        RowExpr new_sub = partial_sync::subscribe_blocking(user_query, "sub"s, util::Optional<int64_t>(1000));
        CHECK(old_sub.get_index() == new_sub.get_index());
        CHECK(old_updated < new_sub.get_timestamp(updated_at_ndx));
        CHECK(old_expires_at < new_sub.get_timestamp(expires_at_ndx));
    }

    SECTION("Create subscription outside write transaction throws") {
        auto table = ObjectStore::table_for_object_type(realm->read_group(), "object_a");
        Results user_query(realm, *table);
        CHECK_THROWS(partial_sync::subscribe_blocking(user_query, none));
    }

    SECTION("Update subscription") {
        realm->begin_transaction();
        auto user_query = results_for_query("number > 0", partial_config, "object_a");
        RowExpr old_sub = partial_sync::subscribe_blocking(user_query, "update-test"s, util::Optional<int64_t>(1000));
        CHECK(subscriptions.size() == 6);
        CHECK(old_sub.get_string(query_ndx) == "number > 0");
        Timestamp old_created_at = old_sub.get_timestamp(created_at_ndx);
        Timestamp old_updated_at = old_sub.get_timestamp(updated_at_ndx);
        Timestamp old_expires_at = old_sub.get_timestamp(expires_at_ndx);
        int64_t old_ttl = old_sub.get_int(time_to_live_ndx);

        user_query = results_for_query("number > 10", partial_config, "object_a");
        RowExpr new_sub = partial_sync::subscribe_blocking(user_query, "update-test"s, util::Optional<int64_t>(5000), true);
        CHECK(subscriptions.size() == 6);
        CHECK(new_sub.get_string(query_ndx) == "number > 10");
        CHECK(old_created_at == new_sub.get_timestamp(created_at_ndx));
        CHECK(old_updated_at < new_sub.get_timestamp(updated_at_ndx));
        CHECK(old_expires_at < new_sub.get_timestamp(expires_at_ndx));
        CHECK(old_ttl == 1000);
        CHECK(new_sub.get_int(time_to_live_ndx) == 5000);
    }

    SECTION("Update subscription with query on different type throws") {
        realm->begin_transaction();
        auto user_query1 = results_for_query("number > 0", partial_config, "object_a");
        partial_sync::subscribe_blocking(user_query1, "update-wrong-typetest"s);
        auto user_query2 = results_for_query("number > 0", partial_config, "object_b");
        CHECK_THROWS(partial_sync::subscribe_blocking(user_query2, "update-wrong-typetest"s, none, true));
    }

    SECTION("Creating/Updating new subscription cleans up expired subscriptions") {
        realm->begin_transaction();
        auto user_query1 = results_for_query("number > 0", partial_config, "object_a");
        partial_sync::subscribe_blocking(user_query1, none, util::Optional<int64_t>(0));
        realm->commit_transaction();

        CHECK(subscriptions.size() == 6);
        CHECK(subscriptions.get(5).get_string(name_ndx) == "[object_a] number > 0");

        realm->begin_transaction();
        auto user_query2 = results_for_query("number > 0", partial_config, "object_b");
        partial_sync::subscribe_blocking(user_query2, none, util::Optional<int64_t>(0));
        realm->commit_transaction();

        CHECK(subscriptions.size() == 6);
        CHECK(subscriptions.get(5).get_string(name_ndx) == "[object_b] number > 0");
    }
}
