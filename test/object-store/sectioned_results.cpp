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

#include <catch2/catch.hpp>

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
    using Wrapped = T;
    using Boxed = T;

    static PropertyType property_type()
    {
        return prop_type;
    }
    static util::Any to_any(T value)
    {
        return value;
    }

    template <typename Fn>
    static auto unwrap(T value, Fn&& fn)
    {
        return fn(value);
    }

    static Mixed comparison_value(Mixed value) {
        return value;
    }
};

struct Int : Base<PropertyType::Int, int64_t> {
    static std::vector<int64_t> values()
    {
        return {1, 2, 3, 4, 5, 1, 2, 3, 4, 5};
    }

    static Mixed comparison_value(Mixed value) {
        // Section odd and even numbers.
        return value.get_int() % 2;
    }
};

struct Bool : Base<PropertyType::Bool, bool> {
    static std::vector<bool> values()
    {
        return {true, false, true, false};
    }

    static Mixed comparison_value(Mixed value) {
        // Section true from false
        return value.get_bool();
    }
};

struct Float : Base<PropertyType::Float, float> {
    static std::vector<float> values()
    {
        return {1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
    }

    static Mixed comparison_value(Mixed value) {
        // Section odd and even numbers.
        return int(value.get_float()) % 2;
    }
};

struct Double : Base<PropertyType::Double, double> {
    static std::vector<double> values()
    {
        return {1.1, 2.2, 3.3, 4.4, 5.5, 1.2, 2.3, 3.4, 4.5, 5.6};
    }

    static Mixed comparison_value(Mixed value) {
        // Section odd and even numbers.
        return int(value.get_double()) % 2;
    }
};

struct String : Base<PropertyType::String, StringData> {
    using Boxed = std::string;
    static std::vector<StringData> values()
    {
        return {
            "apple", "banana", "cherry", "dragon fruit", "elderberry",
            "apples", "bananas", "cherries", "dragon fruit's", "elderberries"
        };
    }

    static Mixed comparison_value(Mixed value) {
        // Return first char of string.
        auto str = value.get_string();
        return str.size() > 0 ? str.prefix(1) : str;
    }
};

struct Binary : Base<PropertyType::Data, BinaryData> {
    using Boxed = std::string;
    static std::vector<BinaryData> values()
    {
        return {
            BinaryData("a", 1), BinaryData("b", 1), BinaryData("c", 1), BinaryData("d", 1), BinaryData("e", 1),
            BinaryData("aa", 2), BinaryData("bb", 2), BinaryData("cc", 2), BinaryData("dd", 2), BinaryData("ee", 2)
        };
    }

    static Mixed comparison_value(Mixed value) {
        // Seperate by size of data
        return (int)value.get_binary().size();
    }
};

struct Date : Base<PropertyType::Date, Timestamp> {
    static std::vector<Timestamp> values()
    {
        return {
            Timestamp(1, 1), Timestamp(2, 1), Timestamp(3, 1), Timestamp(4, 1), Timestamp(5, 1),
            Timestamp(10, 2), Timestamp(20, 2), Timestamp(30, 2), Timestamp(40, 2), Timestamp(50, 2)
        };
    }

    static Mixed comparison_value(Mixed value) {
        // Seperate by size of data
        return value.get_timestamp().get_seconds() < 10 ? "shorter" : "longer";
    }
};

struct MixedVal : Base<PropertyType::Mixed, realm::Mixed> {
    static std::vector<realm::Mixed> values()
    {
        return {
            Mixed{realm::UUID()}, Mixed{int64_t(1)}, Mixed{}, Mixed{"hello world"}, Mixed{Timestamp(1, 1)},
            Mixed{Decimal128("300")}, Mixed{double(2.2)}, Mixed{float(3.3)}, Mixed{BinaryData("a", 1)}, Mixed{ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")}
        };
    }

    static Mixed comparison_value(Mixed value) {
        // Seperate numeric from non numeric
        return Mixed::is_numeric(value.get_type()) ? "Numerics" : "Alphanumeric";
    }
};

struct OID : Base<PropertyType::ObjectId, ObjectId> {
    static std::vector<ObjectId> values()
    {
        return {
            ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
            ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
            ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
            ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
            ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"),
            ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
            ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
            ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
            ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"),
            ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")
        };
    }

    static Mixed comparison_value(Mixed value) {
        // Seperate by sections containing the same ObjectId's
        return value.get_object_id();
    }
};

struct UUID : Base<PropertyType::UUID, realm::UUID> {
    static std::vector<realm::UUID> values()
    {
        return {
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1a241101-e2bb-4255-8caf-4136c566a962"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999"),
            realm::UUID("1b241101-a2b3-4255-8caf-4136c566a999")
        };
    }

    static Mixed comparison_value(Mixed value) {
        // Seperate by sections containing the same UUID's
        return value.get_uuid();
    }
};

struct Decimal : Base<PropertyType::Decimal, Decimal128> {
    static std::vector<Decimal128> values()
    {
        return {
            Decimal128("876.54e32"), Decimal128("123.45e6"),
            Decimal128("876.54e32"), Decimal128("123.45e6"),
            Decimal128("876.54e32"), Decimal128("123.45e6"),
            Decimal128("876.54e32"), Decimal128("123.45e6"),
            Decimal128("876.54e32"), Decimal128("123.45e6"),
        };
    }

    static Mixed comparison_value(Mixed value) {
        // Seperate smaller values
        return value.get_decimal() < Decimal128("876.54e32");
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
    static auto unwrap(Type value)
    {
        return *value;
    }
    static util::Any to_any(Type value)
    {
        return value ? util::Any(*value) : util::Any();
    }

    template <typename Fn>
    static auto unwrap(Type value, Fn&& fn)
    {
        return value ? fn(*value) : fn(null());
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
};
}

TEST_CASE("sectioned results") {
    _impl::RealmCoordinator::assert_no_open_realms();

    InMemoryTestFile config;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {
            {"name_col", PropertyType::String},
            {"int_col", PropertyType::Int},
            {"array_string_col", PropertyType::String | PropertyType::Array},
            {"array_int_col", PropertyType::Int | PropertyType::Array}
        }},
    });

    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");
    auto name_col = table->get_column_key("name_col");
    auto int_col = table->get_column_key("int_col");
    auto array_string_col = table->get_column_key("array_string_col");
    auto array_int_col = table->get_column_key("array_int_col");

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
    auto sectioned_results = sorted.sectioned_results([r](Mixed value) {
        auto obj = Object(r, value.get_link());
        auto v = obj.get_column_value<StringData>("name_col");
        return v.prefix(1);
    });

    auto make_local_change = [&] {
        r->begin_transaction();
        table->begin()->set(name_col, "foo");
        r->commit_transaction();
    };

    auto make_remote_change = [&] {
        auto r2 = coordinator->get_realm();
        r2->begin_transaction();
        r2->read_group().get_table("class_object")->begin()->set(name_col, "bar");
        r2->commit_transaction();
    };

    SECTION("sorts results correctly") {
        REQUIRE(sectioned_results.size() == 3);
        REQUIRE(sectioned_results[0].size() == 3);
        REQUIRE(sectioned_results[1].size() == 1);
        REQUIRE(sectioned_results[2].size() == 1);


        std::vector<std::string> expected {
            "apple",
            "apples",
            "apricot",
            "banana",
            "orange"
        };

        int count = 0;
        for (size_t i = 0; i < sectioned_results.size(); i++) {
            auto section = sectioned_results[i];
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }

        REQUIRE(count == 5);
    }

    SECTION("sorts results correctly after update") {
        REQUIRE(sectioned_results.size() == 3);
        REQUIRE(sectioned_results[0].size() == 3);
        REQUIRE(sectioned_results[1].size() == 1);
        REQUIRE(sectioned_results[2].size() == 1);

        coordinator->on_change();
        r->begin_transaction();
        table->create_object().set(name_col, "safari");
        table->create_object().set(name_col, "mail");
        table->create_object().set(name_col, "car");
        table->create_object().set(name_col, "stocks");
        table->create_object().set(name_col, "cake");
        r->commit_transaction();

        REQUIRE(sectioned_results.size() == 6);
        std::vector<std::string> expected {
            "apple",
            "apples",
            "apricot",
            "banana",
            "cake",
            "car",
            "mail",
            "orange",
            "safari",
            "stocks"
        };

        int count = 0;
        for (size_t i = 0; i < sectioned_results.size(); i++) {
            auto section = sectioned_results[i];
            for (size_t y = 0; y < section.size(); y++) {
                auto val = Object(r, section[y].get_link()).get_column_value<StringData>("name_col");
                REQUIRE(expected[count] == val);
                count++;
            }
        }

        REQUIRE(count == 10);
    }

    SECTION("notifications") {
        SectionedResultsChangeSet changes;
        auto token = sectioned_results.add_notification_callback([&](SectionedResultsChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            changes = c;
        });

        coordinator->on_change();

        // Insertions
        r->begin_transaction();
        auto o1 = table->create_object().set(name_col, "safari");
        auto o2 = table->create_object().set(name_col, "mail");
        auto o3 = table->create_object().set(name_col, "czar");
        auto o4 = table->create_object().set(name_col, "stocks");
        auto o5 = table->create_object().set(name_col, "cake");
        auto o6 = table->create_object().set(name_col, "any");
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(changes.insertions.size() == 4);
        // Section 0 is 'A'
        REQUIRE(changes.insertions[0].size() == 1);
        REQUIRE(changes.insertions[0][0] == 0);
        // Section 2 is 'C'
        REQUIRE(changes.insertions[2].size() == 2);
        REQUIRE(changes.insertions[2][0] == 0);
        REQUIRE(changes.insertions[2][1] == 1);
        // Section 3 is 'M'
        REQUIRE(changes.insertions[3].size() == 1);
        REQUIRE(changes.insertions[3][0] == 0);
        // Section 5 is 'S'
        REQUIRE(changes.insertions[5].size() == 2);
        REQUIRE(changes.insertions[5][0] == 0);
        REQUIRE(changes.insertions[5][1] == 1);
        REQUIRE(changes.modifications.empty());
        REQUIRE(changes.deletions.empty());

        // Modifications
        r->begin_transaction();
        o4.set(name_col, "stocksss");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(changes.modifications.size() == 1);
        REQUIRE(changes.modifications[5][0] == 1);
        REQUIRE(changes.insertions.empty());
        REQUIRE(changes.deletions.empty());

        // Deletions
//        r->begin_transaction();
//        table->remove_object(o3.get_key());
//        r->commit_transaction();
//        advance_and_notify(*r);
//        REQUIRE(changes.deletions.size() == 1);
//        REQUIRE(changes.deletions[2][0] == 1);
//        REQUIRE(changes.insertions.empty());
//        REQUIRE(changes.modifications.empty());
    }

    SECTION("notifications on section") {

        auto section1 = sectioned_results[0];
        int section1_notification_calls = 0;
        SectionedResultsChangeSet section1_changes;
        auto token1 = section1.add_notification_callback([&](SectionedResultsChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            section1_changes = c;
            ++section1_notification_calls;
        });

        auto section2 = sectioned_results[1];
        int section2_notification_calls = 0;
        SectionedResultsChangeSet section2_changes;
        auto token2 = section2.add_notification_callback([&](SectionedResultsChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            section2_changes = c;
            ++section2_notification_calls;
        });

        coordinator->on_change();

        // Insertions
        r->begin_transaction();
        auto o1 = table->create_object().set(name_col, "any");
        r->commit_transaction();
        advance_and_notify(*r);

        REQUIRE(section1_notification_calls == 1);
        REQUIRE(section2_notification_calls == 0);
        REQUIRE(section1_changes.insertions.size() == 1);
        REQUIRE(section1_changes.insertions[0].size() == 1);
        REQUIRE(section1_changes.insertions[0][0] == 0);
        REQUIRE(section1_changes.modifications.empty());
        REQUIRE(section1_changes.deletions.empty());

        r->begin_transaction();
        auto o2 = table->create_object().set(name_col, "box");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(section1_notification_calls == 1);
        REQUIRE(section2_notification_calls == 1);
        REQUIRE(section2_changes.insertions.size() == 1);
        REQUIRE(section2_changes.insertions[1].size() == 1);
        REQUIRE(section2_changes.insertions[1][0] == 1);
        REQUIRE(section2_changes.modifications.empty());
        REQUIRE(section2_changes.deletions.empty());

        // Modifications
        r->begin_transaction();
        o1.set(name_col, "anyyy");
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(section1_notification_calls == 2);
        REQUIRE(section2_notification_calls == 1);
        REQUIRE(section1_changes.modifications.size() == 1);
        REQUIRE(section1_changes.modifications[0][0] == 0);
        REQUIRE(section1_changes.insertions.empty());
        REQUIRE(section1_changes.deletions.empty());

        // Deletions
//        r->begin_transaction();
//        table->remove_object(o2.get_key());
//        r->commit_transaction();
//        advance_and_notify(*r);
//        REQUIRE(section1_notification_calls == 2);
//        REQUIRE(section2_notification_calls == 2);
//        REQUIRE(section1_changes.deletions.size() == 1);
//        REQUIRE(section1_changes.deletions[0][0] == 1);
//        REQUIRE(section1_changes.insertions.empty());
//        REQUIRE(section1_changes.modifications.empty());
    }

    SECTION("primitive collection") {

        r->begin_transaction();
        auto o1 = table->create_object();
        auto string_list = o1.get_list<StringData>(array_string_col);
        string_list.add("aac");
        string_list.add("aab");
        string_list.add("aaa");
        string_list.add("bca");
        string_list.add("baa");
        string_list.add("bma");
        r->commit_transaction();
        List lst(r, o1, array_string_col);

        auto results = lst.sort({{"self", true}});
        auto sectioned_primitives = results.sectioned_results([](Mixed first/*, Mixed second*/) {
            return first.get_string().prefix(1);
        });

        REQUIRE(sectioned_primitives.size() == 2);

    }

    SECTION("primitive collection odd even") {

        r->begin_transaction();
        auto o1 = table->create_object();
        auto int_list = o1.get_list<Int>(array_int_col);
        int_list.add(1);
        int_list.add(2);
        int_list.add(3);
        int_list.add(4);
        int_list.add(5);
        int_list.add(6);
        r->commit_transaction();
        List lst(r, o1, array_int_col);

        auto results = lst.sort({{"self", true}});
        auto sectioned_primitives = results.sectioned_results([](Mixed value) {
            return value.get_int() % 2;
        });

        REQUIRE(sectioned_primitives.size() == 2);
        REQUIRE(sectioned_primitives[0][0] == 2);
        REQUIRE(sectioned_primitives[0][1] == 4);
        REQUIRE(sectioned_primitives[0][2] == 6);
        REQUIRE(sectioned_primitives[1][0] == 1);
        REQUIRE(sectioned_primitives[1][1] == 3);
        REQUIRE(sectioned_primitives[1][2] == 5);
    }
}

namespace cf = realm::sectioned_results_fixtures;

TEMPLATE_TEST_CASE("results types", "[sectioned_results]", cf::MixedVal, cf::Int, cf::Bool, cf::Float, cf::Double,
                   cf::String, cf::Binary, cf::Date, cf::OID, cf::Decimal, cf::UUID, cf::BoxedOptional<cf::Int>,
                   cf::BoxedOptional<cf::Bool>, cf::BoxedOptional<cf::Float>, cf::BoxedOptional<cf::Double>,
                   cf::BoxedOptional<cf::OID>, cf::BoxedOptional<cf::UUID>, cf::UnboxedOptional<cf::String>,
                   cf::UnboxedOptional<cf::Binary>, cf::UnboxedOptional<cf::Date>, cf::UnboxedOptional<cf::Decimal>)
{
    using T = typename TestType::Type;
    using Boxed = typename TestType::Boxed;
    using W = typename TestType::Wrapped;

    _impl::RealmCoordinator::assert_no_open_realms();

    InMemoryTestFile config;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {
            {"value_col", TestType::property_type()},
            {"array_col", PropertyType::Array | TestType::property_type()}
        }},
    });

    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");
    auto value_col = table->get_column_key("value_col");
    auto array_col = table->get_column_key("array_col");

    auto values = TestType::values();
    r->begin_transaction();
    for (size_t i = 0; i < values.size(); ++i) {
        auto o = table->create_object();
        o.set(value_col, T(values[i]));
    }
    r->commit_transaction();

    Results results(r, table);
//    auto sorted = results.sort({{"name_col", true}});
//    auto sectioned_results = results.sectioned_results([r](Mixed value) {
//        auto obj = Object(r, value.get_link());
//        auto v = obj.get_column_value<StringData>("name_col");
//        return v.prefix(1);
//    });

    SECTION("primitives section correctly") {
        auto sectioned_results = results.sectioned_results([r](Mixed value) -> Mixed {
            return TestType::comparison_value(value);
        });
        REQUIRE(sectioned_results.size() == 2);

    }

//    SECTION("key type") {
//        REQUIRE(keys_as_results.get_type() == PropertyType::String);
//    }
//
//    SECTION("value type") {
//        REQUIRE(values_as_results.get_type() == TestType::property_type());
//    }
//
//    SECTION("size()") {
//        REQUIRE(dict.size() == keys.size());
//        dict.remove_all();
//        REQUIRE(dict.size() == 0);
//    }
//
//    SECTION("is_valid()") {
//        object_store::Dictionary unattached;
//        REQUIRE(dict.is_valid());
//        REQUIRE(!unattached.is_valid());
//    }
//
//    SECTION("verify_attached()") {
//        object_store::Dictionary unattached;
//        REQUIRE_NOTHROW(dict.verify_attached());
//        REQUIRE_THROWS_WITH(unattached.verify_attached(), "Access to invalidated Dictionary object");
//    }
//
//    SECTION("verify_in_transaction()") {
//        object_store::Dictionary unattached;
//        REQUIRE_THROWS_AS(unattached.verify_in_transaction(), realm::List::InvalidatedException);
//        REQUIRE_NOTHROW(dict.verify_in_transaction());
//        r->commit_transaction();
//        REQUIRE_THROWS_AS(dict.verify_in_transaction(), InvalidTransactionException);
//        REQUIRE_THROWS_AS(unattached.verify_in_transaction(), realm::List::InvalidatedException);
//    }
//
//    SECTION("clear()") {
//        REQUIRE(dict.size() == keys.size());
//        values_as_results.clear();
//        REQUIRE(dict.size() == 0);
//        REQUIRE(values_as_results.size() == 0);
//    }
//
//    SECTION("equality and assign") {
//        REQUIRE(dict == dict);
//        REQUIRE(!(dict != dict));
//        object_store::Dictionary same(r, obj, col);
//        REQUIRE(dict == same);
//        REQUIRE(!(dict != same));
//        object_store::Dictionary other(r, obj1, col);
//        REQUIRE(!(dict == other));
//        REQUIRE(dict != other);
//        REQUIRE(other == other);
//        REQUIRE(!(other != other));
//
//        for (size_t i = 0; i < keys.size(); ++i) {
//            other.insert(keys[i], T(values[i]));
//        }
//        REQUIRE(!(dict == other));
//        REQUIRE(dict != other);
//
//        other = dict;
//        REQUIRE(dict == other);
//        REQUIRE(!(dict != other));
//    }
}
