////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
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

#include <catch2/catch_all.hpp>

#include "util/index_helpers.hpp"
#include "util/test_file.hpp"

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/list.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/sectioned_results.hpp>

#include <realm/util/any.hpp>

using namespace realm;
using namespace realm::util;

namespace realm::sectioned_results_fixtures {

template <PropertyType prop_type, typename T>
struct Base {
    using Type = T;

    static PropertyType property_type()
    {
        return prop_type;
    }
};

struct Int : Base<PropertyType::Int, int64_t> {
    static std::vector<int64_t> values()
    {
        return {1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6};
    }

    static std::vector<int64_t> expected_sorted()
    {
        return {1, 1, 3, 3, 5, 5, 2, 2, 4, 4, 6};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {1, 0};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Section odd and even numbers.
        if (value.is_null()) {
            return Mixed();
        }
        return value.get_int() % 2;
    }

    static size_t expected_size()
    {
        return 2;
    }
};

struct Bool : Base<PropertyType::Bool, bool> {
    static std::vector<bool> values()
    {
        return {true, false, true, false};
    }

    static std::vector<bool> expected_sorted()
    {
        return {false, false, true, true};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {false, true};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Section true from false
        if (value.is_null()) {
            return Mixed();
        }
        return value.get_bool();
    }

    static size_t expected_size()
    {
        return 2;
    }
};

struct Float : Base<PropertyType::Float, float> {
    static std::vector<float> values()
    {
        return {1.1f, 2.2f, 3.3f, 4.4f, 6.6f, 5.5f, 1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
    }

    static std::vector<float> expected_sorted()
    {
        return {1.1f, 1.1f, 3.3f, 3.3f, 5.5f, 5.5f, 2.2f, 2.2f, 4.4f, 4.4f, 6.6f};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {1, 0};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Section odd and even numbers.
        if (value.is_null()) {
            return Mixed();
        }
        return (int(value.get_float()) % 2) ? 1.0 : 0.0;
    }

    static size_t expected_size()
    {
        return 2;
    }
};

struct Double : Base<PropertyType::Double, double> {
    static std::vector<double> values()
    {
        return {1.1, 2.2, 3.3, 4.4, 5.5, 1.2, 2.3, 3.4, 4.5, 5.6, 6.6};
    }

    static std::vector<double> expected_sorted()
    {
        return {1.1, 1.2, 3.3, 3.4, 5.5, 5.6, 2.2, 2.3, 4.4, 4.5, 6.6};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {1, 0};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Section odd and even numbers.
        if (value.is_null()) {
            return Mixed();
        }
        return (int(value.get_double()) % 2) ? 1.0 : 0.0;
    }

    static size_t expected_size()
    {
        return 2;
    }
};

struct String : Base<PropertyType::String, StringData> {
    using Boxed = std::string;
    static std::vector<StringData> values()
    {
        return {"apple",  "banana",  "cherry",   "dragon fruit",   "elderberry",
                "apples", "bananas", "cherries", "dragon fruit's", "elderberries"};
    }

    static std::vector<StringData> expected_sorted()
    {
        return {"apple",  "apples",       "banana",         "bananas",      "cherries",
                "cherry", "dragon fruit", "dragon fruit's", "elderberries", "elderberry"};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {"a", "b", "c", "d", "e"};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Return first char of string.
        if (value.is_null()) {
            return Mixed();
        }
        auto str = value.get_string();
        return str.size() > 0 ? str.prefix(1) : str;
    }

    static size_t expected_size()
    {
        return 5;
    }
};

struct Binary : Base<PropertyType::Data, BinaryData> {
    using Boxed = std::string;
    static std::vector<BinaryData> values()
    {
        return {BinaryData("a", 1),  BinaryData("aa", 2), BinaryData("b", 1), BinaryData("bb", 2), BinaryData("c", 1),
                BinaryData("cc", 2), BinaryData("a", 1),  BinaryData("b", 1), BinaryData("c", 1)};
    }

    static std::vector<BinaryData> expected_sorted()
    {
        return {BinaryData("a", 1),  BinaryData("a", 1), BinaryData("aa", 2), BinaryData("b", 1), BinaryData("b", 1),
                BinaryData("bb", 2), BinaryData("c", 1), BinaryData("c", 1),  BinaryData("cc", 2)};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {BinaryData("a", 1),  BinaryData("aa", 2), BinaryData("b", 1),
                BinaryData("bb", 2), BinaryData("c", 1),  BinaryData("cc", 2)};
    }

    static Mixed comparison_value(Mixed value)
    {
        if (value.is_null()) {
            return Mixed();
        }
        return value.get_binary();
    }

    static size_t expected_size()
    {
        return 6;
    }
};

struct Date : Base<PropertyType::Date, Timestamp> {
    static std::vector<Timestamp> values()
    {
        return {Timestamp(1, 1),  Timestamp(20, 2), Timestamp(3, 1),  Timestamp(40, 2), Timestamp(5, 1),
                Timestamp(10, 2), Timestamp(2, 1),  Timestamp(30, 2), Timestamp(4, 1),  Timestamp(50, 2)};
    }

    static std::vector<Timestamp> expected_sorted()
    {
        return {Timestamp(1, 1),  Timestamp(2, 1),  Timestamp(3, 1),  Timestamp(4, 1),  Timestamp(5, 1),
                Timestamp(10, 2), Timestamp(20, 2), Timestamp(30, 2), Timestamp(40, 2), Timestamp(50, 2)};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {Timestamp(1, 1), Timestamp(2, 1)};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Seperate by size of data
        if (value.is_null()) {
            return Mixed();
        }
        return value.get_timestamp().get_seconds() < 10 ? Timestamp(1, 1) : Timestamp(2, 1);
    }

    static size_t expected_size()
    {
        return 2;
    }
};

struct MixedVal : Base<PropertyType::Mixed, realm::Mixed> {
    static std::vector<realm::Mixed> values()
    {
        return {Mixed{realm::UUID()},      Mixed{int64_t(1)},
                Mixed{util::none},         Mixed{"hello world"},
                Mixed{Timestamp(1, 1)},    Mixed{Decimal128("300")},
                Mixed{double(2.2)},        Mixed{float(3.3)},
                Mixed{BinaryData("a", 1)}, Mixed{ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")}};
    }

    static std::vector<realm::Mixed> expected_sorted()
    {
        return {Mixed{util::none},    Mixed{int64_t(1)},        Mixed{double(2.2)},
                Mixed{float(3.3)},    Mixed{Decimal128("300")}, Mixed{BinaryData("a", 1)},
                Mixed{"hello world"}, Mixed{Timestamp(1, 1)},   Mixed{ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")},
                Mixed{realm::UUID()}};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {Mixed(), "Numerics", "Alphanumeric"};
    }

    static Mixed comparison_value(Mixed value)
    {
        if (value.is_null()) {
            return Mixed();
        }
        // Seperate numeric from non numeric
        return Mixed::is_numeric(value.get_type()) ? "Numerics" : "Alphanumeric";
    }

    static PropertyType property_type()
    {
        return PropertyType::Mixed | PropertyType::Nullable;
    }

    static size_t expected_size()
    {
        return 3;
    }
};

struct OID : Base<PropertyType::ObjectId, ObjectId> {
    static std::vector<ObjectId> values()
    {
        return {ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
                ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
                ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
                ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
                ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")};
    }

    static std::vector<ObjectId> expected_sorted()
    {
        return {ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
                ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
                ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
                ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
                ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Seperate by sections containing the same ObjectId's
        if (value.is_null()) {
            return Mixed();
        }
        return value.get_object_id();
    }

    static size_t expected_size()
    {
        return 2;
    }
};

struct UUID : Base<PropertyType::UUID, realm::UUID> {
    static std::vector<realm::UUID> values()
    {
        return {
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"), realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"), realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"), realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"), realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"), realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
        };
    }

    static std::vector<realm::UUID> expected_sorted()
    {
        return {
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"), realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"), realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"), realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"), realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"), realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999")};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
                realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999")};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Seperate by sections containing the same UUID's
        if (value.is_null()) {
            return Mixed();
        }
        return value.get_uuid();
    }

    static size_t expected_size()
    {
        return 2;
    }
};

struct Decimal : Base<PropertyType::Decimal, Decimal128> {
    static std::vector<Decimal128> values()
    {
        return {
            Decimal128("876.54e32"), Decimal128("123.45e6"), Decimal128("876.54e32"), Decimal128("123.45e6"),
            Decimal128("876.54e32"), Decimal128("123.45e6"), Decimal128("876.54e32"), Decimal128("123.45e6"),
            Decimal128("876.54e32"), Decimal128("123.45e6"),
        };
    }

    static std::vector<Decimal128> expected_sorted()
    {
        return {Decimal128("123.45e6"),  Decimal128("123.45e6"),  Decimal128("123.45e6"),  Decimal128("123.45e6"),
                Decimal128("123.45e6"),  Decimal128("876.54e32"), Decimal128("876.54e32"), Decimal128("876.54e32"),
                Decimal128("876.54e32"), Decimal128("876.54e32")};
    }

    static std::vector<Mixed> expected_keys()
    {
        return {Decimal128("1"), Decimal128("0")};
    }

    static Mixed comparison_value(Mixed value)
    {
        // Seperate smaller values
        if (value.is_null()) {
            return Mixed();
        }
        return value.get_decimal() < Decimal128("876.54e32") ? Decimal128("1") : Decimal128("0");
    }

    static size_t expected_size()
    {
        return 2;
    }
};

template <typename BaseT>
struct BoxedOptional : BaseT {
    using Type = util::Optional<typename BaseT::Type>;
    using Boxed = Type;
    static PropertyType property_type()
    {
        return BaseT::property_type() | PropertyType::Nullable;
    }
    static std::vector<Type> values()
    {
        std::vector<Type> ret;
        for (auto v : BaseT::values())
            ret.push_back(Type(v));
        ret.push_back(util::none);
        return ret;
    }

    static size_t expected_size()
    {
        return BaseT::expected_size() + 1;
    }

    static std::vector<Type> expected_sorted()
    {
        std::vector<Type> ret;
        for (auto v : BaseT::expected_sorted())
            ret.push_back(Type(v));
        ret.insert(ret.begin(), util::none);
        return ret;
    }

    static std::vector<Mixed> expected_keys()
    {
        auto exp_keys = BaseT::expected_keys();
        exp_keys.insert(exp_keys.begin(), Mixed());
        return exp_keys;
    }
};

template <typename BaseT>
struct UnboxedOptional : BaseT {
    enum { is_optional = true };
    static PropertyType property_type()
    {
        return BaseT::property_type() | PropertyType::Nullable;
    }
    static auto values() -> decltype(BaseT::values())
    {
        auto ret = BaseT::values();
        if constexpr (std::is_same_v<BaseT, sectioned_results_fixtures::Decimal>) {
            // The default Decimal128 ctr is 0, but we want a null value
            ret.push_back(Decimal128(realm::null()));
        }
        else {
            ret.push_back(typename BaseT::Type());
        }
        return ret;
    }

    static size_t expected_size()
    {
        return BaseT::expected_size() + 1;
    }

    static auto expected_sorted() -> decltype(BaseT::values())
    {
        auto ret = BaseT::expected_sorted();
        if constexpr (std::is_same_v<BaseT, sectioned_results_fixtures::Decimal>) {
            // The default Decimal128 ctr is 0, but we want a null value
            ret.insert(ret.begin(), Decimal128(realm::null()));
        }
        else {
            ret.insert(ret.begin(), typename BaseT::Type());
        }
        return ret;
    }

    static std::vector<Mixed> expected_keys()
    {
        auto exp_keys = BaseT::expected_keys();
        exp_keys.insert(exp_keys.begin(), Mixed());
        return exp_keys;
    }
};
} // namespace realm::sectioned_results_fixtures

TEST_CASE("sectioned results", "[sectioned_results]") {
    _impl::RealmCoordinator::assert_no_open_realms();

    InMemoryTestFile config;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({{"object",
                       {{"name_col", PropertyType::String},
                        {"int_col", PropertyType::Int},
                        {"array_string_col", PropertyType::String | PropertyType::Array},
                        {"array_int_col", PropertyType::Int | PropertyType::Array}}}});

    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");
    auto name_col = table->get_column_key("name_col");
    auto int_col = table->get_column_key("int_col");
    auto array_string_col = table->get_column_key("array_string_col");

    r->begin_transaction();
    auto o1 = table->create_object();
    o1.set(name_col, "banana");
    o1.set(int_col, 3);
    auto o2 = table->create_object();
    o2.set(name_col, "apricot");
    o2.set(int_col, 2);
    auto o3 = table->create_object();
    o3.set(name_col, "apple");
    o3.set(int_col, 1);
    auto o4 = table->create_object();
    o4.set(name_col, "orange");
    o4.set(int_col, 2);
    auto o5 = table->create_object();
    o5.set(name_col, "apples");
    o5.set(int_col, 3);
    r->commit_transaction();

    Results results(r, table);
    auto sorted = results.sort({{"name_col", true}});
    int algo_run_count = 0;
    auto sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm realm) {
        algo_run_count++;
        auto obj = Object(realm, value.get_link());
        auto v = obj.get_column_value<StringData>("name_col");
        return v.prefix(1);
    });
    REQUIRE(algo_run_count == 0);

    SECTION("sorts results correctly") {
        REQUIRE(sectioned_results.size() == 3);
        REQUIRE(sectioned_results[0].size() == 3);
        REQUIRE(sectioned_results[1].size() == 1);
        REQUIRE(sectioned_results[2].size() == 1);
        REQUIRE(algo_run_count == 5);
        std::vector<std::string> expected{"apple", "apples", "apricot", "banana", "orange"};

        int count = 0;
        for (size_t i = 0; i < sectioned_results.size(); i++) {
            auto section = sectioned_results[i];
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }
        REQUIRE(algo_run_count == 5);
        REQUIRE(count == 5);
    }

    SECTION("sorts results correctly after update") {
        REQUIRE(sectioned_results.size() == 3);
        REQUIRE(sectioned_results[0].size() == 3);
        REQUIRE(sectioned_results[1].size() == 1);
        REQUIRE(sectioned_results[2].size() == 1);
        REQUIRE(algo_run_count == 5);
        // reset the callback count as it will need to run once we add new objects;
        algo_run_count = 0;

        coordinator->on_change();
        r->begin_transaction();
        table->create_object().set(name_col, "safari");
        table->create_object().set(name_col, "mail");
        table->create_object().set(name_col, "car");
        table->create_object().set(name_col, "stocks");
        table->create_object().set(name_col, "cake");
        r->commit_transaction();

        REQUIRE(sectioned_results.size() == 6);
        REQUIRE(algo_run_count == 10);
        std::vector<std::string> expected{"apple", "apples", "apricot", "banana", "cake",
                                          "car",   "mail",   "orange",  "safari", "stocks"};

        int count = 0;
        for (size_t i = 0; i < sectioned_results.size(); i++) {
            auto section = sectioned_results[i];
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }
        REQUIRE(algo_run_count == 10);
        REQUIRE(count == 10);
    }

    SECTION("key subscript") {
        REQUIRE(sectioned_results.size() == 3);
        REQUIRE(sectioned_results["a"].size() == 3);
        REQUIRE(sectioned_results["b"].size() == 1);
        REQUIRE(sectioned_results["o"].size() == 1);
        REQUIRE_THROWS(sectioned_results["x"]);
        REQUIRE(algo_run_count == 5);
    }

    SECTION("reset section callback") {
        sectioned_results.reset_section_callback([&](Mixed value, SharedRealm realm) {
            algo_run_count++;
            auto obj = Object(realm, value.get_link());
            auto v = obj.get_column_value<StringData>("name_col");
            return v.prefix(2);
        });
        REQUIRE(algo_run_count == 0);
        REQUIRE(sectioned_results.size() == 3);
        REQUIRE(algo_run_count == 5);
        REQUIRE(sectioned_results["ap"].size() == 3);
        REQUIRE(sectioned_results["ba"].size() == 1);
        REQUIRE(sectioned_results["or"].size() == 1);
        REQUIRE_THROWS(sectioned_results["a"]);
        REQUIRE(algo_run_count == 5);
    }

    SECTION("reset section callback after initializing with previous callback") {
        REQUIRE(sectioned_results.size() == 3);
        REQUIRE(algo_run_count == 5);
        algo_run_count = 0;

        sectioned_results.reset_section_callback([&](Mixed value, SharedRealm realm) {
            algo_run_count++;
            auto obj = Object(realm, value.get_link());
            return obj.get_column_value<StringData>("name_col").contains("o");
        });
        REQUIRE(algo_run_count == 0);
        REQUIRE(sectioned_results.size() == 2);
        REQUIRE(algo_run_count == 5);
        REQUIRE(sectioned_results[true].size() == 2);
        REQUIRE(sectioned_results[false].size() == 3);
        REQUIRE(algo_run_count == 5);
    }

    SECTION("correctly asserts key") {
        // Should throw on Object being a section key.
        auto sr = sorted.sectioned_results([](Mixed value, SharedRealm) {
            return value.get_link();
        });
        REQUIRE_THROWS(sr.size()); // Trigger calculation
        // Even after sectioning has failed, the sectioned results
        // object should left in a sensible state.
        REQUIRE(sr.is_valid());

        r->begin_transaction();
        table->clear();
        auto col_typed_link = table->add_column(type_TypedLink, "typed_link_col");
        auto linked = table->create_object();
        table->create_object(ObjKey{}, {{col_typed_link, linked.get_link()}});
        r->commit_transaction();

        // Should throw on `type_TypedLink` being a section key.
        sr = sorted.sectioned_results([&](Mixed value, SharedRealm realm) {
            auto obj = Object(realm, value.get_link());
            return Mixed(obj.obj().get<ObjLink>(col_typed_link));
        });
        REQUIRE_THROWS(sr.size()); // Trigger calculation
        REQUIRE(sr.is_valid());
    }

    SECTION("FirstLetter builtin with link") {
        auto sr = sorted.sectioned_results(Results::SectionedResultsOperator::FirstLetter,
                                           util::Optional<StringData>("name_col"));

        REQUIRE(sr.size() == 3);
        REQUIRE(sr[0].size() == 3);
        REQUIRE(sr[1].size() == 1);
        REQUIRE(sr[2].size() == 1);

        std::vector<std::string> expected{"apple", "apples", "apricot", "banana", "orange"};

        std::vector<std::string> expected_keys{"a", "b", "o"};

        int section_count = 0;
        int element_count = 0;
        for (size_t i = 0; i < sr.size(); i++) {
            auto section = sr[i];
            REQUIRE(section.key().get_string() == expected_keys[section_count]);
            section_count++;
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[element_count] == val);
                element_count++;
            }
        }
        REQUIRE(section_count == 3);
        REQUIRE(element_count == 5);

        algo_run_count = 0;
        // Insert empty string
        coordinator->on_change();
        r->begin_transaction();
        table->create_object().set(name_col, "");
        r->commit_transaction();

        expected.insert(expected.begin(), 1, "");
        expected_keys.insert(expected_keys.begin(), 1, "");

        section_count = 0;
        element_count = 0;
        for (size_t i = 0; i < sr.size(); i++) {
            auto section = sr[i];
            REQUIRE(section.key().get_string() == expected_keys[section_count]);
            section_count++;
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[element_count] == val);
                element_count++;
            }
        }
        REQUIRE(section_count == 4);
        REQUIRE(element_count == 6);
    }

    SECTION("FirstLetter builtin with primitive") {
        r->begin_transaction();
        auto o1 = table->create_object();
        auto str_list = o1.get_list<StringData>(array_string_col);
        str_list.add("apple");
        str_list.add("apples");
        str_list.add("apricot");
        str_list.add("banana");
        str_list.add("orange");
        r->commit_transaction();
        List lst(r, o1, array_string_col);
        auto sr = lst.sort({{"self", true}}).sectioned_results(Results::SectionedResultsOperator::FirstLetter);

        REQUIRE(sr.size() == 3);
        REQUIRE(sr[0].size() == 3);
        REQUIRE(sr[1].size() == 1);
        REQUIRE(sr[2].size() == 1);

        std::vector<std::string> expected{"apple", "apples", "apricot", "banana", "orange"};
        std::vector<std::string> expected_keys{"a", "b", "o"};

        int section_count = 0;
        int element_count = 0;
        for (size_t i = 0; i < sr.size(); i++) {
            auto section = sr[i];
            REQUIRE(section.key().get_string() == expected_keys[section_count]);
            section_count++;
            for (size_t y = 0; y < section.size(); y++) {
                auto val = section[y].get_string();
                REQUIRE(expected[element_count] == val);
                element_count++;
            }
        }
        REQUIRE(section_count == 3);
        REQUIRE(element_count == 5);

        // Insert empty string
        coordinator->on_change();
        r->begin_transaction();
        lst.add(StringData(""));
        r->commit_transaction();
        advance_and_notify(*r);

        expected.insert(expected.begin(), 1, "");
        expected_keys.insert(expected_keys.begin(), 1, "");

        section_count = 0;
        element_count = 0;
        for (size_t i = 0; i < sr.size(); i++) {
            auto section = sr[i];
            REQUIRE(section.key().get_string() == expected_keys[section_count]);
            section_count++;
            for (size_t y = 0; y < section.size(); y++) {
                auto val = section[y].get_string();
                REQUIRE(expected[element_count] == val);
                element_count++;
            }
        }
        REQUIRE(section_count == 4);
        REQUIRE(element_count == 6);
    }

    SECTION("notifications") {
        SectionedResultsChangeSet changes;
        auto token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes = c;
        });

        coordinator->on_change();
        REQUIRE(algo_run_count == 0);
        algo_run_count = 0;

        // Insertions
        r->begin_transaction();
        REQUIRE(algo_run_count == 5); // Initial evaluation will be kicked off.
        algo_run_count = 0;
        auto o1 = table->create_object().set(name_col, "safari");
        auto o2 = table->create_object().set(name_col, "mail");
        auto o3 = table->create_object().set(name_col, "czar");
        auto o4 = table->create_object().set(name_col, "stocks");
        auto o5 = table->create_object().set(name_col, "cake");
        auto o6 = table->create_object().set(name_col, "any");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 11);

        REQUIRE(changes.sections_to_insert.count() == 3);
        REQUIRE(changes.sections_to_delete.count() == 0);
        REQUIRE_INDICES(changes.sections_to_insert, 2, 3, 5);

        REQUIRE(changes.insertions.size() == 4);
        // Section 0 is 'A'
        REQUIRE_INDICES(changes.insertions[0], 0);
        REQUIRE(changes.insertions[0].count() == 1);
        // Section 2 is 'C'
        REQUIRE(changes.insertions[2].count() == 2);
        REQUIRE_INDICES(changes.insertions[2], 0, 1);
        // Section 3 is 'M'
        REQUIRE(changes.insertions[3].count() == 1);
        REQUIRE_INDICES(changes.insertions[3], 0);
        // Section 5 is 'S'
        REQUIRE(changes.insertions[5].count() == 2);
        REQUIRE_INDICES(changes.insertions[5], 0, 1);
        REQUIRE(changes.modifications.empty());
        REQUIRE(changes.deletions.empty());

        algo_run_count = 0;
        // Modifications
        r->begin_transaction();
        o4.set(name_col, "stocksss");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(changes.sections_to_insert.count() == 0);
        REQUIRE(changes.sections_to_delete.count() == 0);

        REQUIRE(changes.modifications.size() == 1);
        REQUIRE_INDICES(changes.modifications[5], 1);
        REQUIRE(changes.insertions.empty());
        REQUIRE(changes.deletions.empty());
        REQUIRE(algo_run_count == 11);

        algo_run_count = 0;
        // Deletions
        r->begin_transaction();
        table->remove_object(o2.get_key());
        table->remove_object(o3.get_key());
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(changes.sections_to_insert.count() == 0);
        REQUIRE(changes.sections_to_delete.count() == 1);

        REQUIRE(changes.deletions.size() == 1);
        REQUIRE_INDICES(changes.deletions[2], 1);
        REQUIRE(changes.insertions.empty());
        REQUIRE(changes.modifications.empty());
        REQUIRE(algo_run_count == 9);

        // Test moving objects from one section to a new one.
        // delete all objects starting with 'S'
        algo_run_count = 0;
        r->begin_transaction();
        o1.set(name_col, "elephant");
        o4.set(name_col, "erie");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(changes.sections_to_insert.count() == 1);
        REQUIRE(changes.sections_to_delete.count() == 1);
        REQUIRE_INDICES(changes.sections_to_delete, 4);
        REQUIRE_INDICES(changes.sections_to_insert, 3);

        REQUIRE(changes.deletions.size() == 1);
        REQUIRE(changes.insertions.size() == 2);
        REQUIRE(changes.modifications.empty());
        REQUIRE_INDICES(changes.deletions[3], 0);
        REQUIRE_INDICES(changes.insertions[3], 0, 1);
        REQUIRE_INDICES(changes.insertions[4], 0);
        REQUIRE(algo_run_count == 9);

        // Test moving objects from one section to an existing one.
        // move all objects starting with 'E'
        algo_run_count = 0;
        r->begin_transaction();
        o1.set(name_col, "asimov");
        o4.set(name_col, "animal");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(changes.sections_to_insert.empty());
        REQUIRE(changes.sections_to_delete.count() == 1);
        REQUIRE_INDICES(changes.sections_to_delete, 3);

        REQUIRE(changes.deletions.empty());
        REQUIRE(changes.insertions.size() == 1);
        REQUIRE(changes.modifications.empty());
        REQUIRE_INDICES(changes.insertions[0], 0, 5);
        REQUIRE(algo_run_count == 9);

        // Test clearing all from the table
        algo_run_count = 0;
        r->begin_transaction();
        table->clear();
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 0);
        REQUIRE(changes.sections_to_insert.empty());
        REQUIRE(changes.sections_to_delete.count() == 4);
        REQUIRE_INDICES(changes.sections_to_delete, 0, 1, 2, 3);

        REQUIRE(changes.deletions.empty());
        REQUIRE(changes.insertions.empty());
        REQUIRE(changes.modifications.empty());

        algo_run_count = 0;
        r->begin_transaction();
        o1 = table->create_object().set(name_col, "any");
        o2 = table->create_object().set(name_col, "any");
        o3 = table->create_object().set(name_col, "any");
        o4 = table->create_object().set(name_col, "beans");
        o5 = table->create_object().set(name_col, "duck");
        o6 = table->create_object().set(name_col, "goat");
        auto o7 = table->create_object().set(name_col, "zebra");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 7);
        REQUIRE(changes.sections_to_insert.count() == 5);
        REQUIRE(changes.sections_to_delete.empty());
        REQUIRE_INDICES(changes.sections_to_insert, 0, 1, 2, 3, 4);

        REQUIRE(changes.deletions.empty());
        REQUIRE(changes.insertions.size() == 5);
        REQUIRE(changes.modifications.empty());
        REQUIRE_INDICES(changes.insertions[0], 0, 1, 2);
        REQUIRE_INDICES(changes.insertions[1], 0);
        REQUIRE_INDICES(changes.insertions[2], 0);
        REQUIRE_INDICES(changes.insertions[3], 0);
        REQUIRE_INDICES(changes.insertions[4], 0);

        algo_run_count = 0;
        r->begin_transaction();
        o1.set(name_col, "banana");
        o2.set(name_col, "melon");
        o3.set(name_col, "calender");
        o4.set(name_col, "apricot");
        o5.set(name_col, "duck"); // stays the same
        o6.set(name_col, "duck");
        o7.set(name_col, "apple");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 7);
        REQUIRE(changes.sections_to_insert.count() == 2);
        REQUIRE(changes.sections_to_delete.count() == 2);
        REQUIRE_INDICES(changes.sections_to_insert, 2, 4);
        REQUIRE_INDICES(changes.sections_to_delete, 3, 4);

        REQUIRE(changes.deletions.size() == 2);
        REQUIRE(changes.insertions.size() == 5);
        REQUIRE(changes.modifications.size() == 1);
        REQUIRE_INDICES(changes.insertions[0], 0, 1);
        REQUIRE_INDICES(changes.insertions[1], 0);
        REQUIRE_INDICES(changes.insertions[2], 0);
        REQUIRE_INDICES(changes.insertions[3], 1);
        REQUIRE_INDICES(changes.insertions[4], 0);

        REQUIRE_INDICES(changes.deletions[0], 0, 1, 2);
        REQUIRE_INDICES(changes.deletions[1], 0);

        REQUIRE_INDICES(changes.modifications[2], 0);

        algo_run_count = 0;
        r->begin_transaction();
        o1.set(name_col, "any");
        o2.set(name_col, "apple");
        o3.set(name_col, "apricot");
        o4.set(name_col, "cake");
        o5.set(name_col, "duck");
        o6.set(name_col, "duck");
        o7.set(name_col, "melon");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 7);
        REQUIRE(changes.sections_to_insert.empty());
        REQUIRE(changes.sections_to_delete.count() == 1);
        REQUIRE_INDICES(changes.sections_to_delete, 1);

        REQUIRE(changes.deletions.size() == 3);
        REQUIRE(changes.insertions.size() == 3);
        REQUIRE(changes.modifications.size() == 1);

        REQUIRE_INDICES(changes.insertions[0], 0, 1, 2);
        REQUIRE_INDICES(changes.insertions[1], 0);
        REQUIRE_INDICES(changes.insertions[3], 0);

        REQUIRE_INDICES(changes.deletions[0], 0, 1);
        REQUIRE_INDICES(changes.deletions[2], 0);
        REQUIRE_INDICES(changes.deletions[4], 0);

        REQUIRE_INDICES(changes.modifications[3], 0, 1);

        algo_run_count = 0;
        r->begin_transaction();
        o1.set(name_col, "calender");
        o2.set(name_col, "apricot");
        o3.set(name_col, "goat");
        o4.set(name_col, "zebra");
        o5.set(name_col, "goat");
        o6.set(name_col, "fire");
        o7.set(name_col, "calender");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 7);
        REQUIRE(changes.sections_to_insert.count() == 3);
        REQUIRE(changes.sections_to_delete.count() == 2);
        REQUIRE_INDICES(changes.sections_to_insert, 2, 3, 4);
        REQUIRE_INDICES(changes.sections_to_delete, 2, 3);

        REQUIRE(changes.deletions.size() == 1);
        REQUIRE(changes.insertions.size() == 5);
        REQUIRE(changes.modifications.size() == 1);

        REQUIRE_INDICES(changes.insertions[0], 0);
        REQUIRE_INDICES(changes.insertions[1], 1);
        REQUIRE_INDICES(changes.insertions[2], 0);
        REQUIRE_INDICES(changes.insertions[3], 0, 1);
        REQUIRE_INDICES(changes.insertions[4], 0);

        REQUIRE_INDICES(changes.modifications[1], 0);

        REQUIRE_INDICES(changes.deletions[0], 0, 1, 2);
    }

    SECTION("notifications ascending / decsending") {
        // Ascending
        SectionedResultsChangeSet changes;
        auto token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes = c;
        });

        coordinator->on_change();
        algo_run_count = 0;

        r->begin_transaction();
        table->clear();
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 5);
        algo_run_count = 0;

        r->begin_transaction();
        auto o1 = table->create_object().set(name_col, "apple");
        auto o2 = table->create_object().set(name_col, "banana");
        auto o3 = table->create_object().set(name_col, "beans");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 3);

        algo_run_count = 0;
        r->begin_transaction();
        o1.set(name_col, "fire");
        o2.set(name_col, "banana");
        o3.set(name_col, "fred");
        auto o4 = table->create_object().set(name_col, "box");
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(algo_run_count == 4);
        REQUIRE(changes.sections_to_insert.count() == 1);
        REQUIRE(changes.sections_to_delete.count() == 1);
        REQUIRE_INDICES(changes.sections_to_insert, 1);
        REQUIRE_INDICES(changes.sections_to_delete, 0);

        REQUIRE(changes.deletions.size() == 1);
        REQUIRE(changes.insertions.size() == 2);
        REQUIRE(changes.modifications.empty());

        REQUIRE_INDICES(changes.insertions[0], 0, 1);
        REQUIRE_INDICES(changes.insertions[1], 0, 1);
        REQUIRE_INDICES(changes.deletions[1], 0, 1);

        // Descending
        sorted = results.sort({{"name_col", false}});
        sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm realm) {
            algo_run_count++;
            auto obj = Object(realm, value.get_link());
            auto v = obj.get_column_value<StringData>("name_col");
            return v.prefix(1);
        });

        token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes = c;
        });

        coordinator->on_change();
        algo_run_count = 0;

        r->begin_transaction();
        REQUIRE(algo_run_count == 4); // Initial evaluation will be kicked off.
        algo_run_count = 0;
        table->clear();
        o1 = table->create_object().set(name_col, "apple");
        o2 = table->create_object().set(name_col, "banana");
        o3 = table->create_object().set(name_col, "beans");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 3);

        algo_run_count = 0;
        r->begin_transaction();
        o1.set(name_col, "fire");
        o2.set(name_col, "banana");
        o3.set(name_col, "fred");
        o4 = table->create_object().set(name_col, "box");
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(algo_run_count == 4);
        REQUIRE(changes.sections_to_insert.count() == 1);
        REQUIRE(changes.sections_to_delete.count() == 1);
        REQUIRE_INDICES(changes.sections_to_insert, 0);
        REQUIRE_INDICES(changes.sections_to_delete, 1);

        REQUIRE(changes.deletions.size() == 1);
        REQUIRE(changes.insertions.size() == 2);
        REQUIRE(changes.modifications.size() == 1);

        REQUIRE_INDICES(changes.insertions[0], 0, 1);
        REQUIRE_INDICES(changes.insertions[1], 0);
        REQUIRE_INDICES(changes.deletions[0], 0);
        REQUIRE_INDICES(changes.modifications[0], 1);
    }

    SECTION("notifications ascending / decsending primitive") {
        // Ascending
        r->begin_transaction();
        auto o1 = table->create_object();
        auto str_list = o1.get_list<StringData>(array_string_col);
        r->commit_transaction();
        List lst(r, o1, array_string_col);
        sectioned_results = lst.sort({{"self", true}}).sectioned_results([&algo_run_count](Mixed value, SharedRealm) {
            algo_run_count++;
            auto v = value.get_string();
            return v.prefix(1);
        });

        SectionedResultsChangeSet changes;
        auto token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes = c;
        });

        coordinator->on_change();
        algo_run_count = 0;

        r->begin_transaction();
        lst.delete_all();
        lst.add(StringData("apple"));
        lst.add(StringData("banana"));
        lst.add(StringData("beans"));
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 3);

        algo_run_count = 0;
        r->begin_transaction();
        lst.set(0, StringData("fire"));
        lst.set(1, StringData("banana"));
        lst.set(2, StringData("fred"));
        lst.add(StringData("box"));
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(algo_run_count == 4);
        REQUIRE(changes.sections_to_insert.count() == 1);
        REQUIRE(changes.sections_to_delete.count() == 1);
        REQUIRE_INDICES(changes.sections_to_insert, 1);
        REQUIRE_INDICES(changes.sections_to_delete, 0);

        REQUIRE(changes.deletions.size() == 1);
        REQUIRE(changes.insertions.size() == 2);
        REQUIRE(changes.modifications.empty());

        REQUIRE_INDICES(changes.insertions[0], 0, 1);
        REQUIRE_INDICES(changes.insertions[1], 0, 1);
        REQUIRE_INDICES(changes.deletions[1], 0, 1);

        // Descending
        sectioned_results =
            lst.sort({{"self", false}}).sectioned_results([&algo_run_count](Mixed value, SharedRealm) {
                algo_run_count++;
                auto v = value.get_string();
                return v.prefix(1);
            });

        token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes = c;
        });

        coordinator->on_change();
        algo_run_count = 0;

        // Insertions
        r->begin_transaction();
        REQUIRE(algo_run_count == 4); // Initial evaluation will be kicked off.
        algo_run_count = 0;
        lst.delete_all();
        lst.add(StringData("apple"));
        lst.add(StringData("banana"));
        lst.add(StringData("beans"));
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 3);

        algo_run_count = 0;
        r->begin_transaction();
        lst.set(0, StringData("fire"));
        lst.set(1, StringData("banana"));
        lst.set(2, StringData("fred"));
        lst.add(StringData("box"));
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(algo_run_count == 4);
        REQUIRE(changes.sections_to_insert.count() == 1);
        REQUIRE(changes.sections_to_delete.count() == 1);
        REQUIRE_INDICES(changes.sections_to_insert, 0);
        REQUIRE_INDICES(changes.sections_to_delete, 1);

        REQUIRE(changes.deletions.size() == 1);
        REQUIRE(changes.insertions.size() == 2);
        REQUIRE(changes.modifications.size() == 1);

        REQUIRE_INDICES(changes.insertions[0], 0, 1);
        REQUIRE_INDICES(changes.insertions[1], 0);
        REQUIRE_INDICES(changes.deletions[0], 0);
        REQUIRE_INDICES(changes.modifications[0], 1);
    }

    SECTION("notifications on section") {
        auto section1 = sectioned_results[0];
        int section1_notification_calls = 0;
        SectionedResultsChangeSet section1_changes;
        auto token1 = section1.add_notification_callback([&](SectionedResultsChangeSet c) {
            section1_changes = c;
            ++section1_notification_calls;
        });

        auto section2 = sectioned_results[1];
        int section2_notification_calls = 0;
        SectionedResultsChangeSet section2_changes;
        auto token2 = section2.add_notification_callback([&](SectionedResultsChangeSet c) {
            section2_changes = c;
            ++section2_notification_calls;
        });

        coordinator->on_change();
        // Insertions
        r->begin_transaction();
        REQUIRE(algo_run_count == 5);
        algo_run_count = 0;
        REQUIRE(section1_notification_calls == 1); // Initial callback.
        REQUIRE(section2_notification_calls == 1); // Initial callback.
        section1_notification_calls = 0;
        section2_notification_calls = 0;
        auto o1 = table->create_object().set(name_col, "any");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 6);

        REQUIRE(section1_notification_calls == 1);
        REQUIRE(section2_notification_calls == 0);
        REQUIRE(section1_changes.insertions.size() == 1);
        REQUIRE(section1_changes.insertions[0].count() == 1);
        REQUIRE_INDICES(section1_changes.insertions[0], 0);
        REQUIRE(section1_changes.modifications.empty());
        REQUIRE(section1_changes.deletions.empty());
        algo_run_count = 0;

        r->begin_transaction();
        auto o2 = table->create_object().set(name_col, "box");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(section1_notification_calls == 1);
        REQUIRE(section2_notification_calls == 1);
        REQUIRE(section2_changes.insertions.size() == 1);
        REQUIRE(section2_changes.insertions[1].count() == 1);
        REQUIRE_INDICES(section2_changes.insertions[1], 1);
        REQUIRE(section2_changes.modifications.empty());
        REQUIRE(section2_changes.deletions.empty());
        REQUIRE(algo_run_count == 7);
        algo_run_count = 0;

        // Modifications
        r->begin_transaction();
        o1.set(name_col, "anyyy");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(section1_notification_calls == 2);
        REQUIRE(section2_notification_calls == 1);
        REQUIRE(section1_changes.modifications.size() == 1);
        REQUIRE_INDICES(section1_changes.modifications[0], 0);
        REQUIRE(section1_changes.insertions.empty());
        REQUIRE(section1_changes.deletions.empty());
        REQUIRE(algo_run_count == 7);
        algo_run_count = 0;
        // Modify the column value to now be in a diff section
        r->begin_transaction();
        o1.set(name_col, "zebra");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(section1_notification_calls == 3);
        REQUIRE(section2_notification_calls == 1);
        REQUIRE(section1_changes.modifications.empty());
        REQUIRE(section1_changes.insertions.empty());
        REQUIRE_INDICES(section1_changes.deletions[0], 0);
        REQUIRE(algo_run_count == 7);
        algo_run_count = 0;

        // Deletions
        r->begin_transaction();
        table->remove_object(o2.get_key());
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(section1_notification_calls == 3);
        REQUIRE(section2_notification_calls == 2);
        REQUIRE(section2_changes.deletions.size() == 1);
        REQUIRE_INDICES(section2_changes.deletions[1], 1);
        REQUIRE(section2_changes.insertions.empty());
        REQUIRE(section2_changes.modifications.empty());
        REQUIRE(algo_run_count == 6);
        algo_run_count = 0;

        r->begin_transaction();
        table->remove_object(o5.get_key());
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(section1_notification_calls == 4);
        REQUIRE(section2_notification_calls == 2);
        REQUIRE(section1_changes.deletions.size() == 1);
        REQUIRE_INDICES(section1_changes.deletions[0], 1);
        REQUIRE(section1_changes.insertions.empty());
        REQUIRE(section1_changes.modifications.empty());
        REQUIRE(algo_run_count == 5);
    }

    SECTION("notifications on section where section is deleted") {
        auto section1 = sectioned_results[0]; // Refers to key 'a'
        int section1_notification_calls = 0;
        SectionedResultsChangeSet section1_changes;
        auto token1 = section1.add_notification_callback([&](SectionedResultsChangeSet c) {
            section1_changes = c;
            ++section1_notification_calls;
        });

        auto section2 = sectioned_results[1]; // Refers to key 'b'
        int section2_notification_calls = 0;
        SectionedResultsChangeSet section2_changes;
        auto token2 = section2.add_notification_callback([&](SectionedResultsChangeSet c) {
            section2_changes = c;
            ++section2_notification_calls;
        });

        coordinator->on_change();
        // Delete all objects from section1
        r->begin_transaction();
        REQUIRE(algo_run_count == 5);
        algo_run_count = 0;
        section1_notification_calls = 0;
        section2_notification_calls = 0;
        REQUIRE(section2.index() == 1);
        std::vector<ObjKey> objs_to_delete;
        for (size_t i = 0; i < section1.size(); i++) {
            objs_to_delete.push_back(section1[i].get_link().get_obj_key());
        }
        for (auto& o : objs_to_delete) {
            table->remove_object(o);
        }
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(section1_notification_calls == 1);
        REQUIRE(section2_notification_calls == 0);
        REQUIRE(section1_changes.deletions.empty());
        REQUIRE(section1_changes.insertions.empty());
        REQUIRE(section1_changes.modifications.empty());
        REQUIRE_INDICES(section1_changes.sections_to_delete, 0);
        REQUIRE(algo_run_count == 2);

        r->begin_transaction();
        REQUIRE(algo_run_count == 2);
        algo_run_count = 0;
        section1_notification_calls = 0;
        section2_notification_calls = 0;
        table->create_object().set(name_col, "book");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(algo_run_count == 3);

        REQUIRE(section1_notification_calls == 0);
        REQUIRE(section2_notification_calls == 1);
        REQUIRE(section2_changes.deletions.empty());
        // Section2 will now be at index 0 as all values begining with 'a' have been deleted.
        REQUIRE_INDICES(section2_changes.insertions[0], 1);
        REQUIRE(section2_changes.modifications.empty());
        REQUIRE(section2.index() == 0);
        REQUIRE(algo_run_count == 3);

        // Insert values back into section1
        REQUIRE_FALSE(section1.is_valid());
        r->begin_transaction();
        REQUIRE(algo_run_count == 3);
        algo_run_count = 0;
        section1_notification_calls = 0;
        section2_notification_calls = 0;
        table->create_object().set(name_col, "apple");
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(algo_run_count == 4);
        REQUIRE(section1_notification_calls == 1);
        REQUIRE(section2_notification_calls == 0);
        REQUIRE(section1_changes.deletions.empty());
        REQUIRE(section1_changes.insertions.size() == 1);
        REQUIRE_INDICES(section1_changes.insertions[0], 0);
        REQUIRE(section1_changes.modifications.empty());
        REQUIRE_INDICES(section1_changes.sections_to_insert, 0);
        REQUIRE(section1_changes.sections_to_delete.empty());
        REQUIRE(section1.is_valid());
    }

    SECTION("snapshot") {
        auto sr_snapshot = sectioned_results.snapshot();

        REQUIRE(sr_snapshot.size() == 3);
        REQUIRE(sr_snapshot[0].size() == 3);
        REQUIRE(sr_snapshot[1].size() == 1);
        REQUIRE(sr_snapshot[2].size() == 1);
        REQUIRE(algo_run_count == 5);
        std::vector<std::string> expected{"apple", "apples", "apricot", "banana", "orange"};

        int count = 0;
        for (size_t i = 0; i < sr_snapshot.size(); i++) {
            auto section = sr_snapshot[i];
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }
        REQUIRE(algo_run_count == 5);
        REQUIRE(count == 5);

        r->begin_transaction();
        table->create_object().set(name_col, "any");
        table->create_object().set(name_col, "zebra");
        r->commit_transaction();

        // results should stay the same.
        count = 0;
        for (size_t i = 0; i < sr_snapshot.size(); i++) {
            auto section = sr_snapshot[i];
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }
        REQUIRE(algo_run_count == 5);
        REQUIRE(count == 5);
    }

    SECTION("frozen") {
        auto frozen_realm = r->freeze();
        REQUIRE(!sectioned_results.is_frozen());
        auto sr_frozen = sectioned_results.freeze(frozen_realm);
        REQUIRE(sr_frozen.is_frozen());
        REQUIRE(sr_frozen.size() == 3);
        REQUIRE(sr_frozen[0].size() == 3);
        REQUIRE(sr_frozen[1].size() == 1);
        REQUIRE(sr_frozen[2].size() == 1);
        REQUIRE(algo_run_count == 5);
        std::vector<std::string> expected{"apple", "apples", "apricot", "banana", "orange"};

        int count = 0;
        for (size_t i = 0; i < sr_frozen.size(); i++) {
            auto section = sr_frozen[i];
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }
        REQUIRE(algo_run_count == 5);
        REQUIRE(count == 5);

        r->begin_transaction();
        table->create_object().set(name_col, "any");
        table->create_object().set(name_col, "zebra");
        r->commit_transaction();

        std::atomic<bool> signal(false);
        std::thread t1 = std::thread([&]() {
            // results should stay the same & work across threads.
            count = 0;
            for (size_t i = 0; i < sr_frozen.size(); i++) {
                auto section = sr_frozen[i];
                for (size_t y = 0; y < section.size(); y++) {
                    auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                    REQUIRE(expected[count] == val);
                    count++;
                }
            }
            REQUIRE(algo_run_count == 5);
            REQUIRE(count == 5);
            signal = true;
        });
        t1.join();
        while (!signal) {
        }

        // Remove all objects and ensure that string buffers work.
        // Clear the current buffer.
        r->begin_transaction();
        table->clear();
        r->commit_transaction();
        sectioned_results.size();
        // Clear the previous buffer.
        r->begin_transaction();
        table->clear();
        r->commit_transaction();
        sectioned_results.size();

        auto exp_keys = std::vector<std::string>{"a", "b", "o"};
        count = 0;
        for (size_t i = 0; i < sr_frozen.size(); i++) {
            auto section = sr_frozen[i];
            REQUIRE(section.is_valid());
            REQUIRE(section.key().get_string() == exp_keys[i]);
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(frozen_realm, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }
        REQUIRE(algo_run_count == 5);
        REQUIRE(count == 5);
    }
}

TEST_CASE("sectioned results link notification bug", "[sectioned_results]") {
    _impl::RealmCoordinator::assert_no_open_realms();

    InMemoryTestFile config;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema(
        {{"Transaction",
          {{"_id", PropertyType::String, Property::IsPrimary{true}},
           {"date", PropertyType::Date},
           {"account", PropertyType::Object | PropertyType::Nullable, "Account"}}},
         {"Account", {{"_id", PropertyType::String, Property::IsPrimary{true}}, {"name", PropertyType::String}}}});

    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    auto transaction_table = r->read_group().get_table("class_Transaction");
    transaction_table->get_column_key("date");
    auto account_col = transaction_table->get_column_key("account");
    auto account_table = r->read_group().get_table("class_Account");
    auto account_name_col = account_table->get_column_key("name");

    r->begin_transaction();
    auto t1 = transaction_table->create_object_with_primary_key("t");
    auto a1 = account_table->create_object_with_primary_key("a");
    t1.set(account_col, a1.get_key());
    r->commit_transaction();

    Results results(r, transaction_table);
    auto sorted = results.sort({{"date", false}});
    auto sectioned_results = sorted.sectioned_results([](Mixed value, SharedRealm realm) {
        auto obj = Object(realm, value.get_link());
        auto ts = obj.get_column_value<Timestamp>("date");
        auto tp = ts.get_time_point();
        auto day = std::chrono::floor<std::chrono::hours>(tp);
        return Timestamp{day};
    });

    REQUIRE(sectioned_results.size() == 1);
    REQUIRE(sectioned_results[0].size() == 1);

    SectionedResultsChangeSet changes;
    size_t callback_count = 0;
    auto token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c) {
        changes = c;
        ++callback_count;
    });
    coordinator->on_change();
    advance_and_notify(*r);
    REQUIRE(callback_count == 1);
    REQUIRE(changes.sections_to_insert.empty());
    REQUIRE(changes.sections_to_delete.empty());
    REQUIRE(changes.insertions.size() == 0);
    REQUIRE(changes.deletions.size() == 0);
    REQUIRE(changes.modifications.size() == 0);

    r->begin_transaction();
    a1.set(account_name_col, "a2");
    r->commit_transaction();
    advance_and_notify(*r);

    REQUIRE(callback_count == 2);
    REQUIRE(changes.sections_to_insert.empty());
    REQUIRE(changes.sections_to_delete.empty());
    REQUIRE(changes.insertions.size() == 0);
    REQUIRE(changes.deletions.size() == 0);
    REQUIRE(changes.modifications.size() == 1);
    REQUIRE_INDICES(changes.modifications[0], 0);
}

namespace cf = realm::sectioned_results_fixtures;

TEMPLATE_TEST_CASE("sectioned results primitive types", "[sectioned_results]", cf::MixedVal, cf::Int, cf::Bool,
                   cf::Float, cf::Double, cf::String, cf::Binary, cf::Date, cf::OID, cf::Decimal, cf::UUID,
                   cf::BoxedOptional<cf::Int>, cf::BoxedOptional<cf::Bool>, cf::BoxedOptional<cf::Float>,
                   cf::BoxedOptional<cf::Double>, cf::BoxedOptional<cf::OID>, cf::BoxedOptional<cf::UUID>,
                   cf::UnboxedOptional<cf::String>, cf::UnboxedOptional<cf::Binary>, cf::UnboxedOptional<cf::Date>,
                   cf::UnboxedOptional<cf::Decimal>)
{
    using T = typename TestType::Type;

    _impl::RealmCoordinator::assert_no_open_realms();

    InMemoryTestFile config;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object",
         {{"value_col", TestType::property_type()}, {"array_col", PropertyType::Array | TestType::property_type()}}},
    });

    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");
    auto array_col = table->get_column_key("array_col");

    auto values = TestType::values();
    auto exp_keys = TestType::expected_keys();
    auto exp_values_sorted = TestType::expected_sorted();

    r->begin_transaction();
    auto o = table->create_object();
    auto list = o.get_list<T>(array_col);
    for (size_t i = 0; i < values.size(); ++i) {
        list.add(T(values[i]));
    }
    r->commit_transaction();
    List lst(r, o, array_col);
    auto results = lst.as_results();
    auto algo_run_count = 0;

    SECTION("primitives section correctly with sort ascending") {
        auto sorted = results.sort({{"self", true}});
        auto sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm) -> Mixed {
            algo_run_count++;
            return TestType::comparison_value(value);
        });
        REQUIRE(sectioned_results.size() == TestType::expected_size());
        auto size = sectioned_results.size();
        auto results_idx = 0;
        for (size_t section_idx = 0; section_idx < size; section_idx++) {
            auto section = sectioned_results[section_idx];
            ResultsSection section_copy;
            // Ensure copy assignment works.
            section_copy = std::move(section);
            REQUIRE(exp_keys[section_idx] == section_copy.key());
            REQUIRE(section_idx == section_copy.index());
            for (size_t element_idx = 0; element_idx < section_copy.size(); element_idx++) {
                auto element = sectioned_results[section_idx][element_idx];
                Mixed value = T(exp_values_sorted[results_idx]);
                REQUIRE(element == value);
                results_idx++;
            }
        }
        REQUIRE(algo_run_count == (int)exp_values_sorted.size());
    }

    SECTION("primitives section correctly with sort decending") {
        auto sorted = results.sort({{"self", false}});
        auto sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm) -> Mixed {
            algo_run_count++;
            return TestType::comparison_value(value);
        });
        std::reverse(exp_values_sorted.begin(), exp_values_sorted.end());
        std::reverse(exp_keys.begin(), exp_keys.end());
        REQUIRE(sectioned_results.size() == TestType::expected_size());
        auto size = sectioned_results.size();
        auto results_idx = 0;
        for (size_t section_idx = 0; section_idx < size; section_idx++) {
            auto section = sectioned_results[section_idx];
            REQUIRE(exp_keys[section_idx] == section.key());
            REQUIRE(section_idx == section.index());
            for (size_t element_idx = 0; element_idx < section.size(); element_idx++) {
                auto element = sectioned_results[section_idx][element_idx];
                Mixed value = T(exp_values_sorted[results_idx]);
                REQUIRE(element == value);
                results_idx++;
            }
        }
        REQUIRE(algo_run_count == (int)exp_values_sorted.size());
    }

    SECTION("notifications") {
        auto sorted = results.sort({{"self", true}});
        auto sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm) -> Mixed {
            algo_run_count++;
            return TestType::comparison_value(value);
        });

        SectionedResultsChangeSet changes;
        auto token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes = c;
        });

        coordinator->on_change();
        r->begin_transaction();
        lst.remove_all();
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(changes.insertions.empty());
        REQUIRE(changes.deletions.empty());
        REQUIRE(changes.modifications.empty());
        REQUIRE(changes.sections_to_insert.empty());
        REQUIRE(changes.sections_to_delete.count() == exp_keys.size());
    }

    SECTION("notifications on section") {
        auto sorted = results.sort({{"self", true}});
        auto sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm) -> Mixed {
            algo_run_count++;
            return TestType::comparison_value(value);
        });
        auto section1 = sectioned_results[0];
        auto section2 = sectioned_results[1];

        SectionedResultsChangeSet changes1, changes2;
        auto token1 = section1.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes1 = c;
        });
        auto token2 = section2.add_notification_callback([&](SectionedResultsChangeSet c) {
            changes2 = c;
        });

        coordinator->on_change();
        r->begin_transaction();
        lst.remove_all();
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(changes1.insertions.empty());
        REQUIRE(changes1.deletions.empty());
        REQUIRE(changes1.modifications.empty());
        REQUIRE(changes1.sections_to_insert.empty());
        REQUIRE(changes1.sections_to_delete.count() == 1);

        REQUIRE(changes2.insertions.empty());
        REQUIRE(changes2.deletions.empty());
        REQUIRE(changes2.modifications.empty());
        REQUIRE(changes2.sections_to_insert.empty());
        REQUIRE(changes2.sections_to_delete.count() == 1);

        coordinator->on_change();
        r->begin_transaction();
        for (size_t i = 0; i < values.size(); ++i) {
            lst.add(T(values[i]));
        }
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(changes1.insertions.size() == 1);
        REQUIRE(!changes1.insertions[0].empty());
        REQUIRE(changes1.deletions.empty());
        REQUIRE(changes1.modifications.empty());
        REQUIRE_INDICES(changes1.sections_to_insert, 0);
        REQUIRE(changes1.sections_to_delete.empty());

        REQUIRE(changes2.insertions.size() == 1);
        REQUIRE(!changes2.insertions[1].empty());
        REQUIRE(changes2.deletions.empty());
        REQUIRE(changes2.modifications.empty());
        REQUIRE_INDICES(changes2.sections_to_insert, 1);
        REQUIRE(changes2.sections_to_delete.empty());
    }

    SECTION("frozen primitive") {
        auto sorted = results.sort({{"self", true}});
        auto sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm) -> Mixed {
            algo_run_count++;
            return TestType::comparison_value(value);
        });
        auto frozen_realm = r->freeze();
        auto frozen_sr = sectioned_results.freeze(frozen_realm);
        auto size = frozen_sr.size();
        REQUIRE(size == TestType::expected_size());
        auto results_idx = 0;
        for (size_t section_idx = 0; section_idx < size; section_idx++) {
            auto section = frozen_sr[section_idx];
            REQUIRE(exp_keys[section_idx] == section.key());
            REQUIRE(section_idx == section.index());
            for (size_t element_idx = 0; element_idx < section.size(); element_idx++) {
                auto element = frozen_sr[section_idx][element_idx];
                Mixed value = T(exp_values_sorted[results_idx]);
                REQUIRE(element == value);
                results_idx++;
            }
        }
        REQUIRE(algo_run_count == (int)exp_values_sorted.size());
    }

    SECTION("frozen results primitive") {
        auto frozen_realm = r->freeze();
        auto sorted = results.sort({{"self", true}}).freeze(frozen_realm);
        auto sectioned_results = sorted.sectioned_results([&algo_run_count](Mixed value, SharedRealm) -> Mixed {
            algo_run_count++;
            return TestType::comparison_value(value);
        });
        auto size = sectioned_results.size();
        REQUIRE(size == TestType::expected_size());
        auto results_idx = 0;
        for (size_t section_idx = 0; section_idx < size; section_idx++) {
            auto section = sectioned_results[section_idx];
            REQUIRE(exp_keys[section_idx] == section.key());
            REQUIRE(section_idx == section.index());
            for (size_t element_idx = 0; element_idx < section.size(); element_idx++) {
                auto element = sectioned_results[section_idx][element_idx];
                Mixed value = T(exp_values_sorted[results_idx]);
                REQUIRE(element == value);
                results_idx++;
            }
        }
        REQUIRE(algo_run_count == (int)exp_values_sorted.size());
    }
}
