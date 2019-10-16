////////////////////////////////////////////////////////////////////////////
//
// Copyright 2019 Realm Inc.
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

#define CATCH_CONFIG_ENABLE_BENCHMARKING

#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include "impl/object_accessor_impl.hpp"
#include "impl/realm_coordinator.hpp"
#include "binding_context.hpp"
#include "object_schema.hpp"
#include "property.hpp"
#include "results.hpp"
#include "schema.hpp"

#include <realm/group_shared.hpp>
#include <realm/query_engine.hpp>
#include <realm/query_expression.hpp>

using namespace realm;

struct TestContext : CppContext {
    std::map<std::string, AnyDict> defaults;

    using CppContext::CppContext;
    TestContext(TestContext& parent, realm::Property const& prop)
    : CppContext(parent, prop)
    , defaults(parent.defaults)
    { }

    util::Optional<util::Any>
    default_value_for_property(ObjectSchema const& object, Property const& prop)
    {
        auto obj_it = defaults.find(object.name);
        if (obj_it == defaults.end())
            return util::none;
        auto prop_it = obj_it->second.find(prop.name);
        if (prop_it == obj_it->second.end())
            return util::none;
        return prop_it->second;
    }

    void will_change(Object const&, Property const&) {}
    void did_change() {}
    std::string print(util::Any) { return "not implemented"; }
    bool allow_missing(util::Any) { return false; }
};



TEST_CASE("Benchmark object", "[benchmark]") {
    using namespace std::string_literals;
    using AnyVec = std::vector<util::Any>;
    using AnyDict = std::map<std::string, util::Any>;
    _impl::RealmCoordinator::assert_no_open_realms();

    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.cache = false;
    config.schema = Schema{
        {"all types", {
            {"pk", PropertyType::Int, Property::IsPrimary{true}},
            {"bool", PropertyType::Bool},
            {"int", PropertyType::Int},
            {"float", PropertyType::Float},
            {"double", PropertyType::Double},
            {"string", PropertyType::String},
            {"data", PropertyType::Data},
            {"date", PropertyType::Date},
            {"object", PropertyType::Object|PropertyType::Nullable, "link target"},

            {"bool array", PropertyType::Array|PropertyType::Bool},
            {"int array", PropertyType::Array|PropertyType::Int},
            {"float array", PropertyType::Array|PropertyType::Float},
            {"double array", PropertyType::Array|PropertyType::Double},
            {"string array", PropertyType::Array|PropertyType::String},
            {"data array", PropertyType::Array|PropertyType::Data},
            {"date array", PropertyType::Array|PropertyType::Date},
            {"object array", PropertyType::Array|PropertyType::Object, "array target"},
        }},
        {"link target", {
            {"value", PropertyType::Int},
        }, {
            {"origin", PropertyType::LinkingObjects|PropertyType::Array, "all types", "object"},
        }},
        {"array target", {
            {"value", PropertyType::Int},
        }},
        {"person", {
            {"name", PropertyType::String, Property::IsPrimary{true}},
            {"age", PropertyType::Int},
            {"scores", PropertyType::Array|PropertyType::Int},
            {"assistant", PropertyType::Object|PropertyType::Nullable, "person"},
            {"team", PropertyType::Array|PropertyType::Object, "person"},
        }},
    };

    config.schema_version = 0;
    auto r = Realm::get_shared_realm(config);
    TestContext d(r);

    auto create_person = [&](util::Any&& value, bool update, bool update_only_diff = false) {
        r->begin_transaction();
        auto obj = Object::create(d, r, *r->schema().find("person"), value, update, update_only_diff);
        r->commit_transaction();
        return obj;
    };

    SECTION("create object") {
        r->begin_transaction();
        ObjectSchema all_types = *r->schema().find("all types");
        constexpr bool update = false;
        constexpr bool update_only_diff = false;

        int64_t benchmark_pk = 0;
        BENCHMARK("create object") {
            return Object::create(d, r, all_types, util::Any(AnyDict{
                {"pk", benchmark_pk++},
                {"bool", true},
                {"int", INT64_C(5)},
                {"float", 2.2f},
                {"double", 3.3},
                {"string", "hello"s},
                {"data", "olleh"s},
                {"date", Timestamp(10, 20)},
                {"object", AnyDict{{"value", INT64_C(10)}}},

                {"bool array", AnyVec{true, false}},
                {"int array", AnyVec{INT64_C(5), INT64_C(6)}},
                {"float array", AnyVec{1.1f, 2.2f}},
                {"double array", AnyVec{3.3, 4.4}},
                {"string array", AnyVec{"a"s, "b"s, "c"s}},
                {"data array", AnyVec{"d"s, "e"s, "f"s}},
                {"date array", AnyVec{}},
                {"object array", AnyVec{AnyDict{{"value", INT64_C(20)}}}},
            }), update, update_only_diff);
        };
        r->commit_transaction();
    }

    SECTION("update object") {
        auto table = r->read_group().get_table("class_all types");
        r->begin_transaction();
        ObjectSchema all_types = *r->schema().find("all types");
        constexpr bool update = false;
        constexpr bool update_only_diff = false;
        auto obj = Object::create(d, r, all_types, util::Any(AnyDict{
            {"pk", INT64_C(0)},
            {"bool", true},
            {"int", INT64_C(5)},
            {"float", 2.2f},
            {"double", 3.3},
            {"string", "hello"s},
            {"data", "olleh"s},
            {"date", Timestamp(10, 20)},
            {"object", AnyDict{{"value", INT64_C(10)}}},

            {"bool array", AnyVec{true, false}},
            {"int array", AnyVec{INT64_C(5), INT64_C(6)}},
            {"float array", AnyVec{1.1f, 2.2f}},
            {"double array", AnyVec{3.3, 4.4}},
            {"string array", AnyVec{"a"s, "b"s, "c"s}},
            {"data array", AnyVec{"d"s, "e"s, "f"s}},
            {"date array", AnyVec{}},
            {"object array", AnyVec{AnyDict{{"value", INT64_C(20)}}}},
        }), update, update_only_diff);
        r->commit_transaction();

        Results result(r, *table);
        size_t num_modifications = 0;
        auto token = result.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
            num_modifications += c.modifications.count();
        });

        advance_and_notify(*r);
        int64_t update_int = 1;
        BENCHMARK_ADVANCED("update object")(Catch::Benchmark::Chronometer meter) {
            r->begin_transaction();
            meter.measure([&d, &all_types, &update_int, &r] {
                auto shadow = Object::create(d, r, all_types, util::Any(AnyDict{
                    {"pk", INT64_C(0)},
                    {"int", update_int},
                }), true, true);
            });
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(result.size() == 1);
            REQUIRE(result.get(0).get_int(2) == update_int);
            update_int++;
        };
    }

    SECTION("change notifications reporting") {
        auto table = r->read_group().get_table("class_person");
        Results result(r, *table);
        size_t num_calls = 0;
        size_t num_insertions = 0;
        auto token = result.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
            num_insertions += c.insertions.count();
            ++num_calls;
        });

        advance_and_notify(*r);
        int64_t pk = 0;
        BENCHMARK_ADVANCED("create notifications")(Catch::Benchmark::Chronometer meter) {
            std::stringstream name;
            name << "person_" << pk++;
            AnyDict person {
                {"name", name.str()},
                {"age", pk},
            };
            create_person(person, true, false);
            meter.measure([&r] {
                advance_and_notify(*r);
            });
            REQUIRE(static_cast<int64_t>(num_calls) == pk + 1);
            REQUIRE(static_cast<int64_t>(num_insertions) == pk);
        };
    }
}

