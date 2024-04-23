////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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

#include "collection_fixtures.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"
#include "util/index_helpers.hpp"

#include <realm/object-store/dictionary.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <numeric>

using namespace realm;
using namespace realm::util;

namespace Catch {
template <>
struct StringMaker<object_store::Dictionary> {
    static std::string convert(const object_store::Dictionary& dict)
    {
        if (dict.size() == 0) {
            return "{}";
        }

        std::stringstream ss;
        ss << "{";
        for (auto [key, value] : dict) {
            ss << '{' << key << ',' << value << "}, ";
        }
        auto str = ss.str();
        str.pop_back();
        str.back() = '}';
        return str;
    }
};
} // namespace Catch

namespace cf = realm::collection_fixtures;

TEST_CASE("nested dictionary in mixed", "[dictionary]") {
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema = Schema{{"any_collection", {{"any", PropertyType::Mixed | PropertyType::Nullable}}}};

    auto r = Realm::get_shared_realm(config);

    auto table_any = r->read_group().get_table("class_any_collection");
    r->begin_transaction();

    Obj any_obj = table_any->create_object();
    ColKey col_any = table_any->get_column_key("any");
    any_obj.set_collection(col_any, CollectionType::Dictionary);
    object_store::Dictionary dict_mixed(r, any_obj, col_any);
    r->commit_transaction();

    CollectionChangeSet change_dictionary;
    auto token_dict = dict_mixed.add_notification_callback([&](CollectionChangeSet c) {
        change_dictionary = c;
    });

    auto write = [&](auto&& f) {
        r->begin_transaction();
        f();
        r->commit_transaction();
        advance_and_notify(*r);
    };

    write([&] {
        dict_mixed.insert_collection("list", CollectionType::List);
        dict_mixed.insert_collection("dictionary", CollectionType::Dictionary);
    });

    REQUIRE(change_dictionary.insertions.count() == 2);

    auto list = dict_mixed.get_list("list");

    SECTION("notification on nested list") {
        CollectionChangeSet change;

        auto require_change = [&] {
            auto token = list.add_notification_callback([&](CollectionChangeSet c) {
                change = c;
            });
            advance_and_notify(*r);
            return token;
        };

        SECTION("adding values") {
            auto token = require_change();
            write([&] {
                list.add(Mixed{5});
                list.add(Mixed{6});
            });
            REQUIRE_INDICES(change.insertions, 0, 1);
            REQUIRE_INDICES(change_dictionary.modifications, 1);
        }

        SECTION("adding list before") {
            // for keys in dictionary insertion in front of the previous key should not matter.
            CollectionChangeSet change_list_after_insert;
            write([&] {
                dict_mixed.insert_collection("A", CollectionType::List);
            });

            auto new_list = dict_mixed.get_list("A");
            auto token_new_list = new_list.add_notification_callback([&](CollectionChangeSet c) {
                change_list_after_insert = c;
            });
            write([&] {
                new_list.add(Mixed{42});
            });

            REQUIRE_INDICES(change_list_after_insert.insertions, 0);
        }
        SECTION("erase from containing dictionary") {
            auto token = require_change();
            write([&] {
                list.add(Mixed{5});
                list.add(Mixed{6});
            });
            REQUIRE_INDICES(change.insertions, 0, 1);
            write([&] {
                dict_mixed.insert("list", 42);
            });
            REQUIRE_INDICES(change.deletions, 0, 1);
            REQUIRE_INDICES(change_dictionary.modifications, 1);
            REQUIRE(change.collection_root_was_deleted);
        }
        SECTION("erase containing dictionary") {
            auto token = require_change();
            write([&] {
                list.add(Mixed{5});
                list.add(Mixed{6});
            });
            REQUIRE_INDICES(change.insertions, 0, 1);
            write([&] {
                any_obj.set(col_any, Mixed(42));
            });
            REQUIRE_INDICES(change.deletions, 0, 1);
            REQUIRE(change.collection_root_was_deleted);
        }
        SECTION("erase containing object") {
            auto token = require_change();
            write([&] {
                list.add(Mixed{5});
                list.add(Mixed{6});
            });
            REQUIRE_INDICES(change.insertions, 0, 1);
            write([&] {
                any_obj.remove();
            });
            REQUIRE_INDICES(change.deletions, 0, 1);
            REQUIRE(change.collection_root_was_deleted);
        }
    }
    SECTION("dictionary as Results") {
        auto results = dict_mixed.get_values();

        auto val = results.get<Mixed>(0);
        REQUIRE(val.is_type(type_Dictionary));
        auto dict = results.get_dictionary(0);
        REQUIRE(dict.is_valid());

        val = results.get<Mixed>(1);
        REQUIRE(val.is_type(type_List));
        auto list = results.get_list(1);
        REQUIRE(list.is_valid());

        CppContext ctx(r);
        CHECK(util::any_cast<object_store::Dictionary&&>(results.get(ctx, 0)).is_valid());
        CHECK(util::any_cast<List&&>(results.get(ctx, 1)).is_valid());
    }
}

TEMPLATE_TEST_CASE("dictionary types", "[dictionary]", cf::MixedVal, cf::Int, cf::Bool, cf::Float, cf::Double,
                   cf::String, cf::Binary, cf::Date, cf::OID, cf::Decimal, cf::UUID, cf::BoxedOptional<cf::Int>,
                   cf::BoxedOptional<cf::Bool>, cf::BoxedOptional<cf::Float>, cf::BoxedOptional<cf::Double>,
                   cf::BoxedOptional<cf::OID>, cf::BoxedOptional<cf::UUID>, cf::UnboxedOptional<cf::String>,
                   cf::UnboxedOptional<cf::Binary>, cf::UnboxedOptional<cf::Date>, cf::UnboxedOptional<cf::Decimal>)
{
    using T = typename TestType::Type;
    using Boxed = typename TestType::Boxed;
    using W = typename TestType::Wrapped;

    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object",
         {{"value", PropertyType::Dictionary | TestType::property_type},
          {"links", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"}}},
        {"target",
         {{"value", PropertyType::Int}, {"self_link", PropertyType::Object | PropertyType::Nullable, "target"}}},
        {"source", {{"link", PropertyType::Object | PropertyType::Nullable, "object"}}}};

    auto r = Realm::get_shared_realm(config);
    auto r2 = Realm::get_shared_realm(config);

    auto table = r->read_group().get_table("class_object");
    auto target = r->read_group().get_table("class_target");
    auto source = r->read_group().get_table("class_source");
    auto table2 = r2->read_group().get_table("class_object");
    r->begin_transaction();
    Obj obj = table->create_object();
    Obj obj1 = table->create_object(); // empty dictionary
    Obj another = target->create_object();
    Obj source_obj0 = source->create_object();
    Obj source_obj1 = source->create_object();
    ColKey col = table->get_column_key("value");
    ColKey col_links = table->get_column_key("links");
    ColKey col_source_link = source->get_column_key("link");
    ColKey col_target_value = target->get_column_key("value");

    source_obj0.set(col_source_link, obj.get_key());
    source_obj1.set(col_source_link, obj1.get_key());

    object_store::Dictionary dict(r, obj, col);
    object_store::Dictionary links(r, obj, col_links);
    auto keys_as_results = dict.get_keys();
    auto values_as_results = dict.get_values();
    CppContext ctx(r, &links.get_object_schema());

    auto values = TestType::values();
    std::vector<std::string> keys;
    for (size_t i = 0; i < values.size(); ++i) {
        keys.push_back(util::format("key_%1", i));
    }

    for (size_t i = 0; i < values.size(); ++i) {
        dict.insert(keys[i], T(values[i]));
    }

    auto verify_keys_ordered = [&keys](Results& r) {
        REQUIRE(r.size() == keys.size());
        for (size_t i = 0; i < r.size(); ++i) {
            REQUIRE(r.get<StringData>(i) == keys[i]);
            REQUIRE(r.get_any(i) == keys[i]);
        }
    };

    auto verify_values_ordered = [&values](Results& r) {
        REQUIRE(r.size() == values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            REQUIRE(r.get<T>(i) == values[i]);
            REQUIRE(r.get_any(i) == Mixed(values[i]));
        }
    };

    SECTION("get_realm()") {
        REQUIRE(dict.get_realm() == r);
        REQUIRE(values_as_results.get_realm() == r);
    }

    SECTION("key type") {
        REQUIRE(keys_as_results.get_type() == PropertyType::String);
    }

    SECTION("value type") {
        REQUIRE(values_as_results.get_type() == TestType::property_type);
    }

    SECTION("size()") {
        REQUIRE(dict.size() == keys.size());
        dict.remove_all();
        REQUIRE(dict.size() == 0);
    }

    SECTION("is_valid()") {
        object_store::Dictionary unattached;
        REQUIRE(dict.is_valid());
        REQUIRE(!unattached.is_valid());
    }

    SECTION("verify_attached()") {
        object_store::Dictionary unattached;
        REQUIRE_NOTHROW(dict.verify_attached());
        REQUIRE_EXCEPTION(unattached.verify_attached(), InvalidatedObject,
                          "Dictionary was never initialized and is invalid.");
        r->invalidate();
        REQUIRE_EXCEPTION(dict.verify_attached(), InvalidatedObject,
                          "Dictionary is no longer valid. Either the parent object was deleted or the containing "
                          "Realm has been invalidated or closed.");
    }

    SECTION("verify_in_transaction()") {
        object_store::Dictionary unattached;
        REQUIRE_EXCEPTION(unattached.verify_in_transaction(), InvalidatedObject,
                          "Dictionary was never initialized and is invalid.");
        REQUIRE_NOTHROW(dict.verify_in_transaction());
        r->commit_transaction();
        REQUIRE_EXCEPTION(dict.verify_in_transaction(), WrongTransactionState,
                          "Cannot modify managed Dictionary outside of a write transaction.");
        REQUIRE_EXCEPTION(unattached.verify_in_transaction(), InvalidatedObject,
                          "Dictionary was never initialized and is invalid.");
    }

    SECTION("clear()") {
        REQUIRE(dict.size() == keys.size());
        values_as_results.clear();
        REQUIRE(dict.size() == 0);
        REQUIRE(values_as_results.size() == 0);
    }

    SECTION("equality and assign") {
        REQUIRE(dict == dict);
        REQUIRE(!(dict != dict));
        object_store::Dictionary same(r, obj, col);
        REQUIRE(dict == same);
        REQUIRE(!(dict != same));
        object_store::Dictionary other(r, obj1, col);
        REQUIRE(!(dict == other));
        REQUIRE(dict != other);
        REQUIRE(other == other);
        REQUIRE(!(other != other));

        for (size_t i = 0; i < keys.size(); ++i) {
            other.insert(keys[i], T(values[i]));
        }
        REQUIRE(!(dict == other));
        REQUIRE(dict != other);

        other = dict;
        REQUIRE(dict == other);
        REQUIRE(!(dict != other));
    }

    SECTION("insert()") {
        for (size_t i = 0; i < values.size(); ++i) {
            auto rev = values.size() - i - 1;
            dict.insert(keys[i], T(values[rev]));
            REQUIRE(dict.get<T>(keys[i]) == values[rev]);
        }
        for (size_t i = 0; i < values.size(); ++i) {
            dict.insert(ctx, keys[i], TestType::to_any(values[i]));
            REQUIRE(dict.get<T>(keys[i]) == values[i]);
        }
    }

    SECTION("get()") {
        for (size_t i = 0; i < values.size(); ++i) {
            REQUIRE(dict.get<T>(keys[i]) == values[i]);
            auto val = dict.get(ctx, keys[i]);
            REQUIRE(util::any_cast<Boxed>(val) == Boxed(values[i]));
            REQUIRE(dict.get_any(keys[i]) == Mixed{values[i]});
            REQUIRE(*dict.try_get_any(keys[i]) == Mixed{values[i]});
        }
    }

    SECTION("erase()") {
        for (auto key : keys) {
            REQUIRE(dict.contains(key));
            dict.erase(key);
            REQUIRE(!dict.contains(key));
            REQUIRE_EXCEPTION(dict.erase(key), KeyNotFound,
                              util::format("Cannot remove key \"%1\" from dictionary: key not found", key));
        }
        REQUIRE(dict.size() == 0);
        REQUIRE_EXCEPTION(dict.erase(keys[0]), KeyNotFound,
                          "Cannot remove key \"key_0\" from dictionary: key not found");
    }

    SECTION("try_erase()") {
        for (auto key : keys) {
            REQUIRE(dict.contains(key));
            REQUIRE(dict.try_erase(key));
            REQUIRE(!dict.contains(key));
            REQUIRE_FALSE(dict.try_erase(key));
        }
        REQUIRE(dict.size() == 0);
        REQUIRE_FALSE(dict.try_erase(keys[0]));
    }

    SECTION("contains()") {
        for (auto key : keys) {
            REQUIRE(dict.contains(key));
        }
        dict.remove_all();
        for (auto key : keys) {
            REQUIRE(!dict.contains(key));
        }
    }

    SECTION("find_any()") {
        for (auto val : values) {
            auto ndx = dict.find_any(Mixed{val});
            REQUIRE(ndx != realm::not_found);
        }
        dict.remove_all();
        for (auto val : values) {
            REQUIRE(dict.find_any(Mixed{val}) == realm::not_found);
        }
    }

    SECTION("get_pair()") {
        std::vector<Mixed> mixed_values;
        for (auto val : values) {
            mixed_values.push_back(Mixed{val});
        }
        std::vector<std::string> found_keys;
        std::vector<Mixed> found_values;
        for (size_t i = 0; i < keys.size(); ++i) {
            auto pair = dict.get_pair(i);
            auto results_pair = values_as_results.get_dictionary_element(i);
            REQUIRE(pair == results_pair);
            found_keys.push_back(pair.first);
            found_values.push_back(pair.second);
        }
        std::sort(begin(keys), end(keys), std::less<>());
        std::sort(begin(mixed_values), end(mixed_values), std::less<>());
        std::sort(begin(found_keys), end(found_keys), std::less<>());
        std::sort(begin(found_values), end(found_values), std::less<>());
        REQUIRE(keys == found_keys);
        REQUIRE(mixed_values == found_values);
    }

    SECTION("index_of() keys") {
        std::vector<size_t> found;
        for (auto key : keys) {
            size_t ndx = keys_as_results.index_of(StringData(key));
            REQUIRE(ndx < keys.size());
            size_t ndx_ctx = keys_as_results.index_of(ctx, std::any(key));
            REQUIRE(ndx_ctx == ndx);
            found.push_back(ndx);
        }
        std::sort(begin(found), end(found), std::less<size_t>());
        std::vector<size_t> expected(keys.size());
        std::iota(begin(expected), end(expected), 0);
        REQUIRE(found == expected);
    }

    SECTION("index_of() values") {
        std::vector<size_t> found;
        for (auto val : values) {
            size_t ndx = values_as_results.index_of(T(val));
            REQUIRE(ndx < values.size());
            size_t ndx_ctx = values_as_results.index_of(ctx, TestType::to_any(T(val)));
            REQUIRE(ndx_ctx == ndx);
            found.push_back(ndx);
        }
        std::sort(begin(found), end(found), std::less<size_t>());
        std::vector<size_t> expected(values.size());
        std::iota(begin(expected), end(expected), 0);
        REQUIRE(found == expected);
    }

    SECTION("links") {
        links.insert(ctx, "foo", std::any(another));
        links.insert(ctx, "m", std::any());
    }

    SECTION("iteration") {
        for (size_t i = 0; i < values.size(); ++i) {
            auto ndx = dict.find_any(T(values[i]));
            REQUIRE(ndx != realm::not_found);
            Dictionary::Iterator it = dict.begin() + ndx;
            REQUIRE((*it).first.get_string() == keys[i]);
            Mixed val_i{values[i]};
            REQUIRE((*it).second == val_i);
            auto element = values_as_results.get_dictionary_element(ndx);
            REQUIRE(element.second == val_i);
            REQUIRE(element.first == keys[i]);
            std::string key = keys_as_results.get<StringData>(ndx);
            REQUIRE(key == keys[i]);
            Mixed m = keys_as_results.get_any(ndx);
            REQUIRE(m.get_string() == keys[i]);
        }
    }

    SECTION("keys sorted") {
        SECTION("ascending") {
            auto sorted = keys_as_results.sort({{"self", true}});
            std::sort(begin(keys), end(keys), std::less<>());
            verify_keys_ordered(sorted);
            // check the same but by generic descriptor
            DescriptorOrdering ordering;
            ordering.append_sort(SortDescriptor({{ColKey{}}}, {true}));
            sorted = keys_as_results.apply_ordering(std::move(ordering));
            verify_keys_ordered(sorted);
        }
        SECTION("descending") {
            auto sorted = keys_as_results.sort({{"self", false}});
            std::sort(begin(keys), end(keys), std::greater<>());
            verify_keys_ordered(sorted);
            // check the same but by descriptor
            DescriptorOrdering ordering;
            ordering.append_sort(SortDescriptor({{ColKey{}}}, {false}));
            sorted = keys_as_results.apply_ordering(std::move(ordering));
            verify_keys_ordered(sorted);
        }
    }
    SECTION("values sorted") {
        SECTION("ascending") {
            auto sorted = values_as_results.sort({{"self", true}});
            std::sort(begin(values), end(values), std::less<>());
            verify_values_ordered(sorted);
        }
        SECTION("descending") {
            auto sorted = values_as_results.sort({{"self", false}});
            std::sort(begin(values), end(values), std::greater<>());
            verify_values_ordered(sorted);
        }
    }

    SECTION("keys distinct") {
        keys.clear();
        // set keys up in dictionary order
        for (size_t i = 0; i < keys_as_results.size(); ++i) {
            keys.push_back(std::string(keys_as_results.get<StringData>(i)));
        }
        auto distinct = keys_as_results.distinct({{"self"}});
        verify_keys_ordered(distinct);
    }

    SECTION("values distinct") {
        // make some duplicate values
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i == 0) {
                dict.insert(keys[i], T(values[0]));
            }
            else {
                dict.insert(keys[i], T(values[1]));
            }
        }
        auto distinct = values_as_results.distinct({{"self"}});
        REQUIRE(distinct.size() == 2);
    }

    SECTION("values sort and distinct") {
        // make some duplicate values
        size_t num_keys = keys.size();
        for (size_t i = 0; i < num_keys; ++i) {
            if (i == 0) {
                dict.insert(keys[i], T(values[0]));
            }
            else {
                dict.insert(keys[i], T(values[1]));
            }
        }
        SECTION("ascending") {
            auto sorted_and_distinct = values_as_results.distinct({{"self"}}).sort({{"self", true}});
            REQUIRE(sorted_and_distinct.size() == 2);
            REQUIRE(sorted_and_distinct.get<T>(0) == T(values[1]));
            REQUIRE(sorted_and_distinct.get<T>(1) == T(values[0]));
            // check the same but by ordering
            DescriptorOrdering ordering;
            ordering.append_distinct(DistinctDescriptor({{ColKey{}}}));
            ordering.append_sort(SortDescriptor({{ColKey{}}}, {true}));
            sorted_and_distinct = values_as_results.apply_ordering(std::move(ordering));
            REQUIRE(sorted_and_distinct.size() == 2);
            REQUIRE(sorted_and_distinct.get<T>(0) == T(values[1]));
            REQUIRE(sorted_and_distinct.get<T>(1) == T(values[0]));
        }
        SECTION("descending") {
            auto sorted_and_distinct = values_as_results.distinct({{"self"}}).sort({{"self", false}});
            REQUIRE(sorted_and_distinct.size() == 2);
            REQUIRE(sorted_and_distinct.get<T>(0) == T(values[0]));
            REQUIRE(sorted_and_distinct.get<T>(1) == T(values[1]));
            // check the same but by ordering
            DescriptorOrdering ordering;
            ordering.append_distinct(DistinctDescriptor({{ColKey{}}}));
            ordering.append_sort(SortDescriptor({{ColKey{}}}, {true}));
            sorted_and_distinct = values_as_results.apply_ordering(std::move(ordering));
            REQUIRE(sorted_and_distinct.size() == 2);
            REQUIRE(sorted_and_distinct.get<T>(0) == T(values[1]));
            REQUIRE(sorted_and_distinct.get<T>(1) == T(values[0]));
        }
    }

    SECTION("first") {
        SECTION("key") {
            auto expected = keys_as_results.get<String>(0);
            REQUIRE(keys_as_results.first<String>() == expected);
            REQUIRE(util::any_cast<std::string>(*keys_as_results.first(ctx)) == expected);
            keys_as_results.clear();
            REQUIRE(!keys_as_results.first<String>());
            REQUIRE(!keys_as_results.first(ctx));
        }
        SECTION("value") {
            auto expected = values_as_results.get<T>(0);
            REQUIRE(*values_as_results.first<T>() == expected);
            REQUIRE(util::any_cast<Boxed>(*values_as_results.first(ctx)) == Boxed(expected));
            values_as_results.clear();
            REQUIRE(!values_as_results.first<T>());
            REQUIRE(!values_as_results.first(ctx));
        }
    }

    SECTION("last") {
        SECTION("key") {
            auto expected = keys_as_results.get<String>(keys_as_results.size() - 1);
            REQUIRE(keys_as_results.last<String>() == expected);
            REQUIRE(util::any_cast<std::string>(*keys_as_results.last(ctx)) == expected);
            keys_as_results.clear();
            REQUIRE(!keys_as_results.last<String>());
            REQUIRE(!keys_as_results.last(ctx));
        }
        SECTION("value") {
            auto expected = values_as_results.get<T>(values_as_results.size() - 1);
            REQUIRE(*values_as_results.last<T>() == expected);
            REQUIRE(util::any_cast<Boxed>(*values_as_results.last(ctx)) == Boxed(expected));
            values_as_results.clear();
            REQUIRE(!values_as_results.last<T>());
            REQUIRE(!values_as_results.last(ctx));
        }
    }

    SECTION("min()") {
        if constexpr (!TestType::can_minmax) {
            REQUIRE_EXCEPTION(
                dict.min(), IllegalOperation,
                util::format("Operation 'min' not supported for %1 dictionary 'object.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.min(), IllegalOperation,
                util::format("Operation 'min' not supported for %1 dictionary 'object.value'", TestType::name));
        }
        else {
            REQUIRE(Mixed(TestType::min()) == values_as_results.min());
            dict.remove_all();
            REQUIRE(!values_as_results.min());
        }
    }

    SECTION("max()") {
        if constexpr (!TestType::can_minmax) {
            REQUIRE_EXCEPTION(
                dict.max(), IllegalOperation,
                util::format("Operation 'max' not supported for %1 dictionary 'object.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.max(), IllegalOperation,
                util::format("Operation 'max' not supported for %1 dictionary 'object.value'", TestType::name));
        }
        else {
            REQUIRE(Mixed(TestType::max()) == values_as_results.max());
            dict.remove_all();
            REQUIRE(!values_as_results.max());
        }
    }

    SECTION("sum()") {
        if constexpr (!TestType::can_sum) {
            REQUIRE_EXCEPTION(
                dict.sum(), IllegalOperation,
                util::format("Operation 'sum' not supported for %1 dictionary 'object.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.sum(), IllegalOperation,
                util::format("Operation 'sum' not supported for %1 dictionary 'object.value'", TestType::name));
        }
        else {
            REQUIRE(cf::get<W>(*values_as_results.sum()) == TestType::sum());
            dict.remove_all();
            REQUIRE(values_as_results.sum() == 0);
        }
    }

    SECTION("average()") {
        if constexpr (!TestType::can_average) {
            REQUIRE_EXCEPTION(
                dict.average(), IllegalOperation,
                util::format("Operation 'average' not supported for %1 dictionary 'object.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.average(), IllegalOperation,
                util::format("Operation 'average' not supported for %1 dictionary 'object.value'", TestType::name));
        }
        else {
            REQUIRE(cf::get<typename TestType::AvgType>(*values_as_results.average()) == TestType::average());
            dict.remove_all();
            REQUIRE(!values_as_results.average());
        }
    }

    SECTION("handover") {
        r->commit_transaction();

        auto dict2 = ThreadSafeReference(dict).resolve<object_store::Dictionary>(r);
        REQUIRE(dict == dict2);
        ThreadSafeReference ref(values_as_results);
        auto results2 = ref.resolve<Results>(r).sort({{"self", true}});
        std::sort(begin(values), end(values), std::less<>());
        for (size_t i = 0; i < values.size(); ++i) {
            REQUIRE(results2.get<T>(i) == values[i]);
        }
        r->begin_transaction();
        obj.remove();
        r->commit_transaction();
        results2 = ref.resolve<Results>(r);
        REQUIRE(!results2.is_valid());
    }

    SECTION("notifications") {
        r->commit_transaction();

        auto sorted = values_as_results.sort({{"self", true}});

        size_t calls = 0;
        CollectionChangeSet change, rchange, srchange;
        auto token = dict.add_notification_callback([&](CollectionChangeSet c) {
            change = c;
            ++calls;
        });
        auto rtoken = values_as_results.add_notification_callback([&](CollectionChangeSet c) {
            rchange = c;
            ++calls;
        });
        auto srtoken = sorted.add_notification_callback([&](CollectionChangeSet c) {
            srchange = c;
            ++calls;
        });

        SECTION("add value to dictionary") {
            // Remove the existing copy of this value so that the sorted list
            // doesn't have dupes resulting in an unstable order
            advance_and_notify(*r);
            r->begin_transaction();
            dict.erase(keys[0]);
            r->commit_transaction();

            advance_and_notify(*r);
            r->begin_transaction();
            dict.insert(keys[0], T(values[0]));
            r->commit_transaction();

            advance_and_notify(*r);
            auto ndx = values_as_results.index_of(T(values[0]));
            REQUIRE_INDICES(change.insertions, ndx);
            REQUIRE_INDICES(rchange.insertions, ndx);
            // values[0] is max(), so it ends up at the end of the sorted list
            REQUIRE_INDICES(srchange.insertions, values.size() - 1);
        }

        SECTION("replace value in dictionary") {
            // Remove the existing copy of this value so that the sorted list
            // doesn't have dupes resulting in an unstable order
            advance_and_notify(*r);
            r->begin_transaction();
            dict.erase(keys[0]);
            r->commit_transaction();

            advance_and_notify(*r);
            r->begin_transaction();
            dict.insert(keys[1], T(values[0]));
            r->commit_transaction();

            advance_and_notify(*r);
            auto ndx = values_as_results.index_of(T(values[0]));
            REQUIRE_INDICES(change.insertions);
            REQUIRE_INDICES(change.modifications, ndx);
            REQUIRE_INDICES(change.deletions);
        }

        SECTION("remove value from dictionary") {
            advance_and_notify(*r);
            auto ndx = values_as_results.index_of(T(values[0]));
            auto ndx_sorted = sorted.index_of(T(values[0]));
            r->begin_transaction();
            dict.erase(keys[0]);
            r->commit_transaction();

            advance_and_notify(*r);
            REQUIRE_INDICES(change.deletions, ndx);
            REQUIRE_INDICES(rchange.deletions, ndx);
            REQUIRE_INDICES(srchange.deletions, ndx_sorted);
        }

        SECTION("key based notification") {
            DictionaryChangeSet key_change;
            auto token = dict.add_key_based_notification_callback([&key_change](DictionaryChangeSet c) {
                key_change = c;
            });
            advance_and_notify(*r);

            r->begin_transaction();
            dict.insert(keys[0], T(values[1]));
            dict.erase(keys[1]);
            r->commit_transaction();

            advance_and_notify(*r);
            CHECK(key_change.insertions.size() == 0);
            REQUIRE(key_change.deletions.size() == 1);
            REQUIRE(key_change.modifications.size() == 1);
            CHECK(key_change.deletions[0].get_string() == keys[1]);
            CHECK(key_change.modifications[0].get_string() == keys[0]);

            r->begin_transaction();
            dict.insert(keys[1], T(values[1]));
            dict.erase(keys[0]);
            r->commit_transaction();

            advance_and_notify(*r);
            REQUIRE(key_change.insertions[0].get_string() == keys[1]);
            REQUIRE(key_change.deletions[0].get_string() == keys[0]);
            REQUIRE(key_change.modifications.size() == 0);

            r->begin_transaction();
            obj.remove();
            r->commit_transaction();

            advance_and_notify(*r);
            REQUIRE(key_change.insertions.size() == 0);
            REQUIRE(key_change.deletions.size() == values.size() - 1);
            REQUIRE(key_change.modifications.size() == 0);
            REQUIRE(key_change.collection_root_was_deleted);
        }

        SECTION("clear list") {
            DictionaryChangeSet key_change;
            auto token = dict.add_key_based_notification_callback([&key_change](DictionaryChangeSet c) {
                key_change = c;
            });
            advance_and_notify(*r);

            r->begin_transaction();
            dict.remove_all();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(change.deletions.count() == values.size());
            REQUIRE(rchange.deletions.count() == values.size());
            REQUIRE(srchange.deletions.count() == values.size());
            REQUIRE(key_change.collection_was_cleared);
        }

        SECTION("delete containing row") {
            advance_and_notify(*r);
            REQUIRE(calls == 3);
            REQUIRE(!change.collection_root_was_deleted);

            r->begin_transaction();
            obj.remove();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(calls == 6);
            REQUIRE(change.deletions.count() == values.size());
            REQUIRE(rchange.deletions.count() == values.size());
            REQUIRE(srchange.deletions.count() == values.size());
            REQUIRE(change.collection_root_was_deleted);

            r->begin_transaction();
            table->create_object();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(calls == 6);
        }

        SECTION("deleting containing row before first run of notifier") {
            r2->begin_transaction();
            table2->begin()->remove();
            r2->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(change.deletions.count() == values.size());
            REQUIRE(change.collection_root_was_deleted);
        }

        SECTION("deleting a row with an empty dictionary triggers notifications") {
            advance_and_notify(*r);
            REQUIRE(calls == 3);
            r->begin_transaction();
            REQUIRE(dict.size() == values.size());
            values_as_results.clear();
            REQUIRE(dict.size() == 0);
            REQUIRE(values_as_results.size() == 0);
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(change.deletions.count() == values.size());
            REQUIRE(!change.collection_root_was_deleted);
            REQUIRE(calls == 6);

            r->begin_transaction();
            obj.remove();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(change.deletions.count() == 0);
            REQUIRE(change.collection_root_was_deleted);
            REQUIRE(calls == 9);
        }

        SECTION("now with links") {
            auto objectschema = &*r->schema().find("target");
            auto res = links.get_values();
            REQUIRE(&res.get_object_schema() == objectschema);

            CollectionChangeSet local_change;
            auto x = links.add_notification_callback([&local_change](CollectionChangeSet c) {
                local_change = c;
            });
            advance_and_notify(*r);

            r->begin_transaction();
            links.insert("l", another.get_key());
            links.insert("m", ObjKey());
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(local_change.insertions.count() == 2);

            SECTION("with links on frozen Realm") {
                // this could have deadlocked
                auto frozen = r->freeze();
                auto frozen_table = frozen->read_group().get_table("class_object");
                ColKey col_frozen_links = frozen_table->get_column_key("links");
                object_store::Dictionary frozen_links(frozen, *frozen_table->begin(), col_frozen_links);
                auto frozen_results = frozen_links.get_values();
                size_t frozen_links_counter = 0;
                // Implementation of the hashing function for dictionaries vary between 32 and 64 bit.
                // Order is not preserved and assumptions around at which position an object is inside the Set is
                // wrong and cannot be used for testing.
                // TODO : fix the implementation for Dicitionaries in order to match order between 32 and 64 bit archs
                for (size_t i = 0; i < frozen_results.size(); ++i) {
                    if (frozen_results.get(i)) {
                        frozen_links_counter += 1;
                        REQUIRE(frozen_results.get(i).get_key() == another.get_key());
                    }
                }
                REQUIRE(frozen_links_counter == 1);
            }
            size_t frozen_links_counter = 0;
            for (size_t i = 0; i < res.size(); ++i) {
                if (res.get(i)) {
                    frozen_links_counter += 1;
                }
            }
            REQUIRE(frozen_links_counter == 1);
            r->begin_transaction();
            another.remove();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(local_change.modifications.count() == 1);
        }

        SECTION("source links") {
            Results all_sources(r, source->where());
            REQUIRE(all_sources.size() == 2);
            CollectionChangeSet local_changes;
            auto x = all_sources.add_notification_callback([&local_changes](CollectionChangeSet c) {
                local_changes = c;
            });
            advance_and_notify(*r);

            SECTION("direct insertion") {
                r->begin_transaction();
                source->create_object();
                r->commit_transaction();
                advance_and_notify(*r);
                REQUIRE(local_changes.insertions.count() == 1);
                REQUIRE(local_changes.modifications.count() == 0);
                REQUIRE(local_changes.deletions.count() == 0);
            }
            SECTION("indirect insertion to dictionary link") {
                r->begin_transaction();
                links.insert("new key", ObjKey());
                r->commit_transaction();
                advance_and_notify(*r);
                REQUIRE(local_changes.insertions.count() == 0);
                REQUIRE(local_changes.modifications.count() == 1);
                REQUIRE(local_changes.deletions.count() == 0);
            }
            SECTION("no change for non linked insertion") {
                r->begin_transaction();
                table->create_object();
                r->commit_transaction();
                advance_and_notify(*r);
                REQUIRE(local_changes.insertions.count() == 0);
                REQUIRE(local_changes.modifications.count() == 0);
                REQUIRE(local_changes.deletions.count() == 0);
            }
            SECTION("modification marked for change to linked object through dictionary") {
                r->begin_transaction();
                links.insert("l", another.get_key());
                links.insert("m", ObjKey());
                r->commit_transaction();
                advance_and_notify(*r);
                REQUIRE(local_changes.insertions.count() == 0);
                REQUIRE(local_changes.modifications.count() == 1);
                REQUIRE(local_changes.deletions.count() == 0);
                local_changes = {};

                r->begin_transaction();
                another.set_any(col_target_value, {42});
                r->commit_transaction();
                advance_and_notify(*r);
                REQUIRE(local_changes.insertions.count() == 0);
                REQUIRE(local_changes.modifications.count() == 1);
                REQUIRE(local_changes.deletions.count() == 0);
            }
        }
    }

    SECTION("snapshot") {
        SECTION("keys") {
            auto new_keys = keys_as_results.snapshot();
            REQUIRE(new_keys.size() == keys.size());
            dict.remove_all();
            REQUIRE(new_keys.size() == 0);
        }
        SECTION("values") {
            auto new_values = values_as_results.snapshot();
            REQUIRE(new_values.size() == values.size());
            dict.remove_all();
            REQUIRE(new_values.size() == 0);
        }
    }
}

TEST_CASE("embedded dictionary", "[dictionary]") {
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema =
        Schema{{"origin",
                {{"_id", PropertyType::Int, Property::IsPrimary{true}},
                 {"links", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"}}},
               {"target", ObjectSchema::ObjectType::Embedded, {{"value", PropertyType::Int}}}};

    auto r = Realm::get_shared_realm(config);

    auto origin = r->read_group().get_table("class_origin");
    auto target = r->read_group().get_table("class_target");

    r->begin_transaction();
    Obj obj = origin->create_object_with_primary_key(1);
    ColKey col_links = origin->get_column_key("links");
    ColKey col_value = target->get_column_key("value");

    object_store::Dictionary dict(r, obj, col_links);
    for (int i = 0; i < 10; ++i)
        dict.insert_embedded(util::to_string(i));
    dict.insert("null", ObjKey());

    r->commit_transaction();

    CppContext ctx(r);
    auto initial_target_size = target->size();

    SECTION("insert(Context)") {
        CppContext ctx(r, &dict.get_object_schema());
        r->begin_transaction();

        SECTION("rejects boxed Obj and Object") {
            REQUIRE_EXCEPTION(dict.insert(ctx, "foo", std::any(target->get_object(5))), IllegalOperation,
                              "Cannot add an existing managed embedded object to a Dictionary.");
            REQUIRE_EXCEPTION(dict.insert(ctx, "foo", std::any(Object(r, target->get_object(5)))), IllegalOperation,
                              "Cannot add an existing managed embedded object to a Dictionary.");
        }

        SECTION("creates new object for dictionary") {
            dict.insert(ctx, "foo", std::any(AnyDict{{"value", INT64_C(20)}}));
            REQUIRE(dict.size() == 12);
            REQUIRE(target->size() == initial_target_size + 1);
            REQUIRE(dict.get_object("foo").get<Int>(col_value) == 20);
        }

        SECTION("overwrite null value") {
            dict.insert(ctx, "null", std::any(AnyDict{{"value", INT64_C(17)}}), CreatePolicy::UpdateModified);
            REQUIRE(dict.size() == 11);
            REQUIRE(target->size() == initial_target_size + 1);
            REQUIRE(dict.get_object("null").get<Int>(col_value) == 17);
        }

        SECTION("mutates the existing object for update mode Modified") {
            auto old_object = dict.get<Obj>("0");
            dict.insert(ctx, "0", std::any(AnyDict{{"value", INT64_C(20)}}), CreatePolicy::UpdateModified);
            REQUIRE(dict.size() == 11);
            REQUIRE(target->size() == initial_target_size);
            REQUIRE(dict.get_object("0").get<Int>(col_value) == 20);
            REQUIRE(old_object.is_valid());
        }

        r->cancel_transaction();
    }
}

TEMPLATE_TEST_CASE("dictionary of objects", "[dictionary][links]", cf::MixedVal, cf::Int, cf::Bool, cf::Float,
                   cf::Double, cf::String, cf::Binary, cf::Date, cf::OID, cf::Decimal, cf::UUID,
                   cf::BoxedOptional<cf::Int>, cf::BoxedOptional<cf::Bool>, cf::BoxedOptional<cf::Float>,
                   cf::BoxedOptional<cf::Double>, cf::BoxedOptional<cf::OID>, cf::BoxedOptional<cf::UUID>,
                   cf::UnboxedOptional<cf::String>, cf::UnboxedOptional<cf::Binary>, cf::UnboxedOptional<cf::Date>,
                   cf::UnboxedOptional<cf::Decimal>)
{
    using T = typename TestType::Type;
    using W = typename TestType::Wrapped;
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object", {{"links", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"}}},
        {"target", {{"value", TestType::property_type}}},
    };

    auto r = Realm::get_shared_realm(config);
    auto table = r->read_group().get_table("class_object");
    auto target = r->read_group().get_table("class_target");
    r->begin_transaction();
    Obj obj = table->create_object();
    table->create_object(); // empty dictionary
    target->create_object();
    ColKey col_links = table->get_column_key("links");
    ColKey col_target_value = target->get_column_key("value");

    object_store::Dictionary dict(r, obj, col_links);
    auto keys_as_results = dict.get_keys();
    auto values_as_results = dict.get_values();

    auto values = TestType::values();
    std::vector<std::string> keys;
    for (size_t i = 0; i < values.size(); ++i) {
        keys.push_back(util::format("key_%1", i));
    }

    for (size_t i = 0; i < values.size(); ++i) {
        Obj target_obj = target->create_object().set(col_target_value, T(values[i]));
        dict.insert(keys[i], target_obj);
    }

    r->commit_transaction();
    r->begin_transaction();

    SECTION("min()") {
        if constexpr (!TestType::can_minmax) {
            REQUIRE_EXCEPTION(
                dict.min(col_target_value), IllegalOperation,
                util::format("Operation 'min' not supported for %1 property 'target.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.min(col_target_value), IllegalOperation,
                util::format("Operation 'min' not supported for %1 property 'target.value'", TestType::name));
        }
        else {
            REQUIRE(Mixed(TestType::min()) == dict.min(col_target_value));
            REQUIRE(Mixed(TestType::min()) == values_as_results.min(col_target_value));
            dict.remove_all();
            REQUIRE(!dict.min(col_target_value));
            REQUIRE(!values_as_results.min(col_target_value));
        }
    }

    SECTION("max()") {
        if constexpr (!TestType::can_minmax) {
            REQUIRE_EXCEPTION(
                dict.max(col_target_value), IllegalOperation,
                util::format("Operation 'max' not supported for %1 property 'target.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.max(col_target_value), IllegalOperation,
                util::format("Operation 'max' not supported for %1 property 'target.value'", TestType::name));
        }
        else {
            REQUIRE(Mixed(TestType::max()) == dict.max(col_target_value));
            REQUIRE(Mixed(TestType::max()) == values_as_results.max(col_target_value));
            dict.remove_all();
            REQUIRE(!dict.max(col_target_value));
            REQUIRE(!values_as_results.max(col_target_value));
        }
    }

    SECTION("sum()") {
        if constexpr (!TestType::can_sum) {
            REQUIRE_EXCEPTION(
                dict.sum(col_target_value), IllegalOperation,
                util::format("Operation 'sum' not supported for %1 property 'target.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.sum(col_target_value), IllegalOperation,
                util::format("Operation 'sum' not supported for %1 property 'target.value'", TestType::name));
        }
        else {
            REQUIRE(cf::get<W>(dict.sum(col_target_value)) == TestType::sum());
            REQUIRE(cf::get<W>(*values_as_results.sum(col_target_value)) == TestType::sum());
            dict.remove_all();
            REQUIRE(dict.sum(col_target_value) == 0);
            REQUIRE(values_as_results.sum(col_target_value) == 0);
        }
    }

    SECTION("average()") {
        if constexpr (!TestType::can_average) {
            REQUIRE_EXCEPTION(
                dict.average(col_target_value), IllegalOperation,
                util::format("Operation 'average' not supported for %1 property 'target.value'", TestType::name));
            REQUIRE_EXCEPTION(
                values_as_results.average(col_target_value), IllegalOperation,
                util::format("Operation 'average' not supported for %1 property 'target.value'", TestType::name));
        }
        else {
            REQUIRE(cf::get<typename TestType::AvgType>(*dict.average(col_target_value)) == TestType::average());
            REQUIRE(cf::get<typename TestType::AvgType>(*values_as_results.average(col_target_value)) ==
                    TestType::average());
            dict.remove_all();
            REQUIRE(!dict.average(col_target_value));
            REQUIRE(!values_as_results.average(col_target_value));
        }
    }
}


TEST_CASE("dictionary with mixed links", "[dictionary]") {
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Dictionary | PropertyType::Mixed | PropertyType::Nullable}}},
        {"target1",
         {{"value1", PropertyType::Int}, {"link1", PropertyType::Object | PropertyType::Nullable, "target1"}}},
        {"target2",
         {{"value2", PropertyType::Int}, {"link2", PropertyType::Object | PropertyType::Nullable, "target2"}}}};

    auto r = Realm::get_shared_realm(config);

    auto table = r->read_group().get_table("class_object");
    auto target1 = r->read_group().get_table("class_target1");
    auto target2 = r->read_group().get_table("class_target2");
    ColKey col_value1 = target1->get_column_key("value1");
    ColKey col_value2 = target2->get_column_key("value2");
    ColKey col_link1 = target1->get_column_key("link1");
    r->begin_transaction();
    Obj obj = table->create_object();
    table->create_object(); // empty dictionary
    Obj target1_obj = target1->create_object().set(col_value1, 100);
    Obj target2_obj = target2->create_object().set(col_value2, 200);
    ColKey col = table->get_column_key("value");

    object_store::Dictionary dict(r, obj, col);
    CppContext ctx(r);

    dict.insert("key_a", Mixed{ObjLink(target1->get_key(), target1_obj.get_key())});
    dict.insert("key_b", Mixed{});
    dict.insert("key_c", Mixed{});
    dict.insert("key_d", Mixed{int64_t{42}});
    r->commit_transaction();

    Results all_objects(r, table->where());
    REQUIRE(all_objects.size() == 2);
    CollectionChangeSet local_changes;
    auto x = all_objects.add_notification_callback([&local_changes](CollectionChangeSet c) {
        local_changes = c;
    });
    advance_and_notify(*r);
    local_changes = {};

    SECTION("insertion") {
        r->begin_transaction();
        table->create_object();
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(local_changes.insertions.count() == 1);
        REQUIRE(local_changes.modifications.count() == 0);
        REQUIRE(local_changes.deletions.count() == 0);
    }
    SECTION("insert to dictionary is a modification") {
        r->begin_transaction();
        dict.insert("key_e", Mixed{"hello"});
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(local_changes.insertions.count() == 0);
        REQUIRE(local_changes.modifications.count() == 1);
        REQUIRE(local_changes.deletions.count() == 0);
    }
    SECTION("modify an existing key is a modification") {
        r->begin_transaction();
        dict.insert("key_a", Mixed{});
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(local_changes.insertions.count() == 0);
        REQUIRE(local_changes.modifications.count() == 1);
        REQUIRE(local_changes.deletions.count() == 0);
    }
    SECTION("modify a linked object is a modification") {
        r->begin_transaction();
        target1_obj.set(col_value1, 1000);
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(local_changes.insertions.count() == 0);
        REQUIRE(local_changes.modifications.count() == 1);
        REQUIRE(local_changes.deletions.count() == 0);
    }
    SECTION("modify a linked object once removed is a modification") {
        r->begin_transaction();
        auto target1_obj2 = target1->create_object().set(col_value1, 1000);
        target1_obj.set(col_link1, target1_obj2.get_key());
        r->commit_transaction();
        advance_and_notify(*r);
        r->begin_transaction();
        target1_obj2.set(col_value1, 2000);
        r->commit_transaction();
        local_changes = {};
        advance_and_notify(*r);
        REQUIRE(local_changes.insertions.count() == 0);
        REQUIRE(local_changes.modifications.count() == 1);
        REQUIRE(local_changes.deletions.count() == 0);
    }
    SECTION("adding a link to a new table is a modification") {
        r->begin_transaction();
        dict.insert("key_b", Mixed{ObjLink(target2->get_key(), target2_obj.get_key())});
        r->commit_transaction();
        advance_and_notify(*r);
        REQUIRE(local_changes.insertions.count() == 0);
        REQUIRE(local_changes.modifications.count() == 1);
        REQUIRE(local_changes.deletions.count() == 0);

        SECTION("changing a property from the newly linked table is a modification") {
            r->begin_transaction();
            target2_obj.set(col_value2, 42);
            r->commit_transaction();
            local_changes = {};
            advance_and_notify(*r);
            REQUIRE(local_changes.insertions.count() == 0);
            REQUIRE(local_changes.modifications.count() == 1);
            REQUIRE(local_changes.deletions.count() == 0);
        }
    }
    SECTION("adding a link to a new table and rolling back is not a modification") {
        r->begin_transaction();
        dict.insert("key_b", Mixed{ObjLink(target2->get_key(), target2_obj.get_key())});
        r->cancel_transaction();
        advance_and_notify(*r);
        REQUIRE(local_changes.insertions.count() == 0);
        REQUIRE(local_changes.modifications.count() == 0);
        REQUIRE(local_changes.deletions.count() == 0);

        SECTION("changing a property from rollback linked table is not a modification") {
            r->begin_transaction();
            target2_obj.set(col_value2, 42);
            r->commit_transaction();
            local_changes = {};
            advance_and_notify(*r);
            REQUIRE(local_changes.insertions.count() == 0);
            REQUIRE(local_changes.modifications.count() == 0);
            REQUIRE(local_changes.deletions.count() == 0);
        }
    }
}

TEST_CASE("dictionary nullify", "[dictionary]") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"DictionaryObject",
         {
             {"intDictionary", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "IntObject"},
         }},
        {"IntObject", {{"intCol", PropertyType::Int}}},
    };

    auto r = Realm::get_shared_realm(config);
    CppContext ctx(r);

    r->begin_transaction();
    auto obj = Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                              Any{AnyDict{{"intDictionary", AnyDict{{"0", Any(AnyDict{{"intCol", INT64_C(0)}})},
                                                                    {"1", Any(AnyDict{{"intCol", INT64_C(1)}})},
                                                                    {"2", Any(AnyDict{{"intCol", INT64_C(2)}})}}}}});
    auto obj1 = Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                               Any{AnyDict{{"intDictionary", AnyDict{{"null", Any()}}}}});
    r->commit_transaction();

    r->begin_transaction();
    SECTION("clear dictionary") {
        // Before fix, we would crash here
        r->read_group().get_table("class_IntObject")->clear();
        // r->read_group().to_json(std::cout);
    }

    SECTION("overwrite null value") {
        obj1.set_property_value(ctx, "intDictionary", Any(AnyDict{{"null", Any(AnyDict{{"intCol", INT64_C(3)}})}}),
                                CreatePolicy::UpdateModified);
        auto dict =
            util::any_cast<object_store::Dictionary&&>(obj1.get_property_value<std::any>(ctx, "intDictionary"));
        REQUIRE(dict.get_object("null").get<Int>("intCol") == 3);
    }
    r->commit_transaction();
}

TEST_CASE("nested collection set by Object::create", "[dictionary]") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"DictionaryObject",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"any", PropertyType::Mixed | PropertyType::Nullable},
         }},
    };

    auto r = Realm::get_shared_realm(config);
    CppContext ctx(r);

    Any value{AnyDict{{"_id", INT64_C(5)},
                      {"any", AnyDict{{"0", Any(AnyDict{{"zero", INT64_C(0)}})},
                                      {"1", Any(AnyVector{std::string("one"), INT64_C(1)})},
                                      {"2", Any(AnyDict{{"two", INT64_C(2)}, {"three", INT64_C(3)}})}}}}};
    r->begin_transaction();
    auto obj = Object::create(ctx, r, *r->schema().find("DictionaryObject"), value);
    r->commit_transaction();

    auto dict = util::any_cast<object_store::Dictionary&&>(obj.get_property_value<std::any>(ctx, "any"));

    auto dict0 = util::any_cast<object_store::Dictionary&&>(dict.get(ctx, "0"));
    auto list1 = util::any_cast<List&&>(dict.get(ctx, "1"));
    auto dict2 = dict.get_dictionary("2");
    CHECK(dict0.get_any("zero") == Mixed(0));
    CHECK(list1.get_any(0) == Mixed("one"));
    CHECK(list1.get_any(1) == Mixed(1));
    CHECK(dict2.get_any("two") == Mixed(2));
    CHECK(dict2.get_any("three") == Mixed(3));

    SECTION("modify list only") {
        Any new_value{
            AnyDict{{"_id", INT64_C(5)}, {"any", AnyDict{{"1", Any(AnyVector{std::string("seven"), INT64_C(7)})}}}}};

        r->begin_transaction();
        Object::create(ctx, r, *r->schema().find("DictionaryObject"), new_value, CreatePolicy::UpdateModified);
        r->commit_transaction();
        CHECK(list1.get_any(0) == Mixed("seven"));
        CHECK(list1.get_any(1) == Mixed(7));
    }

    SECTION("update with less data") {
        Any new_value{
            AnyDict{{"_id", INT64_C(5)}, {"any", AnyDict{{"1", Any(AnyVector{std::string("seven"), INT64_C(7)})}}}}};

        r->begin_transaction();
        Object::create(ctx, r, *r->schema().find("DictionaryObject"), new_value, CreatePolicy::UpdateAll);
        r->commit_transaction();
        CHECK(dict.size() == 1);
        list1 = dict.get_list("1");
        CHECK(list1.get_any(0) == Mixed("seven"));
        CHECK(list1.get_any(1) == Mixed(7));
    }

    SECTION("replace list with dictionary") {
        Any new_value{AnyDict{{"_id", INT64_C(5)}, {"any", AnyDict{{"1", Any(AnyDict{{"seven", INT64_C(7)}})}}}}};

        r->begin_transaction();
        Object::create(ctx, r, *r->schema().find("DictionaryObject"), new_value, CreatePolicy::UpdateModified);
        r->commit_transaction();
        auto dict1 = dict.get_dictionary("1");
        CHECK(dict1.get_any("seven") == Mixed(7));
    }

    SECTION("replace dictionary with list on top level") {
        value = Any{AnyDict{
            {"_id", INT64_C(5)},
            {"any", AnyVector{Any(AnyDict{{"zero", INT64_C(0)}}), Any(AnyVector{std::string("one"), INT64_C(1)}),
                              Any(AnyDict{{"two", INT64_C(2)}, {"three", INT64_C(3)}})}}}};

        r->begin_transaction();
        Object::create(ctx, r, *r->schema().find("DictionaryObject"), value, CreatePolicy::UpdateModified);
        r->commit_transaction();
        auto list = util::any_cast<List&&>(obj.get_property_value<std::any>(ctx, "any"));
        dict0 = util::any_cast<object_store::Dictionary&&>(list.get(ctx, 0));
        CHECK(dict0.get_any("zero") == Mixed(0));

        SECTION("modify dictionary only") {
            Any new_value{
                AnyDict{{"_id", INT64_C(5)}, {"any", AnyVector{Any(AnyDict{{std::string("seven"), INT64_C(7)}})}}}};

            r->begin_transaction();
            Object::create(ctx, r, *r->schema().find("DictionaryObject"), new_value, CreatePolicy::UpdateModified);
            r->commit_transaction();
            CHECK(dict0.get_any("seven") == Mixed(7));
        }

        SECTION("replace dictionary with list") {
            Any new_value{
                AnyDict{{"_id", INT64_C(5)}, {"any", AnyVector{Any(AnyVector{std::string("seven"), INT64_C(7)})}}}};

            r->begin_transaction();
            Object::create(ctx, r, *r->schema().find("DictionaryObject"), new_value, CreatePolicy::UpdateModified);
            r->commit_transaction();
            auto list0 = util::any_cast<List&&>(list.get(ctx, 0));
            CHECK(list0.get_any(0) == Mixed("seven"));
            CHECK(list0.get_any(1) == Mixed(7));
        }

        SECTION("assign dictionary directly to nested list") {
            r->begin_transaction();
            list.set(ctx, 1, Any(AnyDict{{std::string("ten"), INT64_C(10)}}));
            r->commit_transaction();
            auto dict0 = list.get_dictionary(1);
            CHECK(dict0.get_any("ten") == Mixed(10));
        }

        SECTION("assign list directly to nested list") {
            r->begin_transaction();
            list.set(ctx, 0, Any(AnyVector{std::string("ten"), INT64_C(10)}));
            r->commit_transaction();
            auto list0 = list.get_list(0);
            CHECK(list0.get_any(0) == Mixed("ten"));
            CHECK(list0.get_any(1) == Mixed(10));
        }
    }
}

TEST_CASE("dictionary assign", "[dictionary]") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"DictionaryObject",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"intDictionary", PropertyType::Dictionary | PropertyType::Int | PropertyType::Nullable},
         }},
    };

    auto r = Realm::get_shared_realm(config);
    CppContext ctx(r);

    r->begin_transaction();

    auto obj = Object::create(
        ctx, r, *r->schema().find("DictionaryObject"),
        Any{AnyDict{{"_id", INT64_C(0)},
                    {"intDictionary", AnyDict{{"0", INT64_C(0)}, {"1", INT64_C(1)}, {"2", INT64_C(2)}}}}});
    object_store::Dictionary dict(obj, r->schema().find("DictionaryObject")->property_for_name("intDictionary"));
    REQUIRE(dict.size() == 3);
    REQUIRE(dict.get<Int>("0") == 0);
    REQUIRE(dict.get<Int>("1") == 1);
    REQUIRE(dict.get<Int>("2") == 2);

    SECTION("UpdateAll replaces an entire dictionary") {
        CreatePolicy policy = CreatePolicy::UpdateAll;
        Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                       Any{AnyDict{{"_id", INT64_C(0)}, {"intDictionary", AnyDict{{"2", INT64_C(22)}}}}}, policy);
        REQUIRE(dict.size() == 1);
        REQUIRE(dict.get<Int>("2") == 22);
    }

    SECTION("UpdateAll with no value clears the dictionary") {
        CreatePolicy policy = CreatePolicy::UpdateAll;
        Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                       Any{AnyDict{{"_id", INT64_C(0)}, {"intDictionary", AnyDict{}}}}, policy);
        REQUIRE(dict.size() == 0);
    }

    SECTION("UpdateModified changes an existing value") {
        CreatePolicy policy = CreatePolicy::UpdateModified;
        Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                       Any{AnyDict{{"_id", INT64_C(0)}, {"intDictionary", AnyDict{{"2", INT64_C(22)}}}}}, policy);
        REQUIRE(dict.size() == 3);
        REQUIRE(dict.get<Int>("0") == 0);
        REQUIRE(dict.get<Int>("1") == 1);
        REQUIRE(dict.get<Int>("2") == 22);
    }

    SECTION("UpdateModified with a new value adds it in") {
        CreatePolicy policy = CreatePolicy::UpdateModified;
        Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                       Any{AnyDict{{"_id", INT64_C(0)}, {"intDictionary", AnyDict{{"3", INT64_C(3)}}}}}, policy);
        REQUIRE(dict.size() == 4);
        REQUIRE(dict.get<Int>("0") == 0);
        REQUIRE(dict.get<Int>("1") == 1);
        REQUIRE(dict.get<Int>("2") == 2);
        REQUIRE(dict.get<Int>("3") == 3);
    }

    SECTION("UpdateModified with null clears the dictionary") {
        CreatePolicy policy = CreatePolicy::UpdateAll;
        Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                       Any{AnyDict{{"_id", INT64_C(0)}, {"intDictionary", AnyDict{}}}}, policy);
        REQUIRE(dict.size() == 0);
    }

    r->commit_transaction();
}

TEST_CASE("dictionary comparison different realm", "[dictionary]") {
    TestFile config1;
    TestFile config2;
    Schema schema{
        {"object", {{"value", PropertyType::Dictionary | PropertyType::Int}}},
    };
    config1.schema = schema;
    config2.schema = schema;

    auto r1 = Realm::get_shared_realm(config1);
    auto r2 = Realm::get_shared_realm(config2);

    CppContext ctx1(r1);
    CppContext ctx2(r2);

    AnyDict dict_content{{"val1", INT64_C(10)}};
    Any init{AnyDict{{"value", dict_content}}};
    r1->begin_transaction();
    r2->begin_transaction();
    auto obj1 = Object::create(ctx1, r1, *r1->schema().find("object"), init);
    auto obj2 = Object::create(ctx2, r2, *r2->schema().find("object"), init);
    auto prop1 = r1->schema().find("object")->property_for_name("value");
    auto prop2 = r2->schema().find("object")->property_for_name("value");
    r1->commit_transaction();
    r2->commit_transaction();

    object_store::Dictionary dict1(obj1, prop1);
    object_store::Dictionary dict2(obj2, prop2);
    REQUIRE(dict1 != dict2);
}

TEST_CASE("dictionary snapshot null", "[dictionary]") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"}}},
        {"target", {{"id", PropertyType::Int}}},
    };

    auto r = Realm::get_shared_realm(config);
    CppContext ctx(r);

    r->begin_transaction();
    auto obj = Object::create(ctx, r, *r->schema().find("object"), Any{AnyDict{{"value", AnyDict{{"val", Any()}}}}});
    auto prop = r->schema().find("object")->property_for_name("value");
    r->commit_transaction();

    object_store::Dictionary dict(obj, prop);
    auto values = dict.get_values();
    auto size1 = values.size();

    // both should include the null link
    auto snapshot = values.snapshot();
    auto size2 = snapshot.size();
    REQUIRE(size1 == size2);

    // a snapshot retains a null link after it is deleted
    r->begin_transaction();
    dict.remove_all();
    r->commit_transaction();
    REQUIRE(values.size() == 0);
    REQUIRE(snapshot.size() == 1);
    REQUIRE(snapshot.get_any(0) == Mixed{});

    // a snaphot remains the same when a new link is added
    snapshot = values.snapshot();
    r->begin_transaction();
    StringData new_key("foo");

    auto target_obj = Object::create(ctx, r, *r->schema().find("target"), Any{AnyDict{{"id", Any{int64_t(42)}}}});
    dict.insert(new_key, target_obj.get_obj().get_key());
    r->commit_transaction();
    REQUIRE(values.size() == 1);
    REQUIRE(snapshot.size() == 0);

    // a snapshot retains an entry for a link when the link is removed
    snapshot = values.snapshot();
    r->begin_transaction();
    dict.remove_all();
    r->commit_transaction();
    REQUIRE(values.size() == 0);
    REQUIRE(snapshot.size() == 1);
    auto obj_link = ObjLink{target_obj.get_obj().get_table()->get_key(), target_obj.get_obj().get_key()};
    REQUIRE(snapshot.get_any(0) == Mixed{obj_link});

    // a snapshot retains an entry for a link when the underlying object is deleted
    // but the snapshot link is nullified
    r->begin_transaction();
    target_obj.get_obj().remove();
    r->commit_transaction();
    REQUIRE(values.size() == 0);
    REQUIRE(snapshot.size() == 1);
    REQUIRE(snapshot.get_any(0) == Mixed{});
}

TEST_CASE("dictionary aggregate", "[dictionary][aggregate]") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"DictionaryObject",
         {
             {"intDictionary", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "IntObject"},
         }},
        {"IntObject", {{"intCol", PropertyType::Int}}},
    };

    auto r = Realm::get_shared_realm(config);
    CppContext ctx(r);

    r->begin_transaction();
    auto obj = Object::create(ctx, r, *r->schema().find("DictionaryObject"),
                              Any{AnyDict{{"intDictionary", AnyDict{{"0", Any(AnyDict{{"intCol", INT64_C(5)}})},
                                                                    {"1", Any(AnyDict{{"intCol", INT64_C(3)}})},
                                                                    {"2", Any(AnyDict{{"intCol", INT64_C(8)}})}}}}});
    auto prop = r->schema().find("DictionaryObject")->property_for_name("intDictionary");
    r->commit_transaction();
    object_store::Dictionary dict(obj, prop);
    auto res = dict.get_values();
    auto sum = res.sum("intCol");
    REQUIRE(*sum == 16);
}

TEST_CASE("callback with empty keypatharray", "[dictionary]") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"object", {{"links", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"}}},
        {"target", {{"value", PropertyType::Int}}},
    };

    auto r = Realm::get_shared_realm(config);
    auto table = r->read_group().get_table("class_object");
    auto target = r->read_group().get_table("class_target");

    r->begin_transaction();
    Obj obj = table->create_object();
    ColKey col_links = table->get_column_key("links");
    ColKey col_target_value = target->get_column_key("value");
    object_store::Dictionary dict(r, obj, col_links);
    auto key = "key";
    Obj target_obj = target->create_object().set(col_target_value, 1);
    dict.insert(key, target_obj);
    r->commit_transaction();

    CollectionChangeSet change;
    auto write = [&](auto&& f) {
        r->begin_transaction();
        f();
        r->commit_transaction();
        advance_and_notify(*r);
    };

    auto shallow_require_change = [&] {
        auto token = dict.add_notification_callback(
            [&](CollectionChangeSet c) {
                change = c;
            },
            KeyPathArray());
        advance_and_notify(*r);
        return token;
    };

    auto shallow_require_no_change = [&] {
        bool first = true;
        auto token = dict.add_notification_callback(
            [&first](CollectionChangeSet) mutable {
                REQUIRE(first);
                first = false;
            },
            KeyPathArray());
        advance_and_notify(*r);
        return token;
    };

    SECTION("insertion DOES send notification") {
        auto token = shallow_require_change();
        write([&] {
            Obj target_obj = target->create_object().set(col_target_value, 1);
            dict.insert("foo", target_obj);
        });
        REQUIRE_FALSE(change.insertions.empty());
    }
    SECTION("deletion DOES send notification") {
        auto token = shallow_require_change();
        write([&] {
            dict.erase(key);
        });
        REQUIRE_FALSE(change.deletions.empty());
    }
    SECTION("replacement DOES send notification") {
        auto token = shallow_require_change();
        write([&] {
            Obj target_obj = target->create_object().set(col_target_value, 1);
            dict.insert(key, target_obj);
        });
        REQUIRE_FALSE(change.modifications.empty());
    }
    SECTION("modification does NOT send notification") {
        auto token = shallow_require_no_change();
        write([&] {
            dict.get<Obj>(key).set(col_target_value, 2);
        });
    }
}

TEST_CASE("dictionary sort by keyPath value", "[dictionary]") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"DictionaryObject",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"s1", PropertyType::Int},
             {"intDictionary", PropertyType::Dictionary | PropertyType::Int | PropertyType::Nullable},
         }},
    };

    auto r = Realm::get_shared_realm(config);
    CppContext ctx(r);

    r->begin_transaction();

    auto table = r->read_group().get_table("class_DictionaryObject");
    auto col_id = table->get_column_key("_id");
    auto col_s1 = table->get_column_key("s1");
    auto col_dict = table->get_column_key("intDictionary");

    const ObjectSchema& dict_obj_schema = *r->schema().find("DictionaryObject");

    Object::create(ctx, r, dict_obj_schema,
                   Any{AnyDict{{"_id", INT64_C(0)},
                               {"s1", INT64_C(10)},
                               {"intDictionary", AnyDict{{"a", INT64_C(0)}, {"b", INT64_C(1)}, {"c", INT64_C(2)}}}}});

    Object::create(ctx, r, dict_obj_schema,
                   Any{AnyDict{{"_id", INT64_C(2)},
                               {"s1", INT64_C(10)},
                               {"intDictionary", AnyDict{{"a", INT64_C(3)}, {"b", INT64_C(4)}, {"c", INT64_C(5)}}}}});

    Object::create(ctx, r, dict_obj_schema,
                   Any{AnyDict{{"_id", INT64_C(3)},
                               {"s1", INT64_C(20)},
                               {"intDictionary", AnyDict{{"a", INT64_C(2)}, {"b", INT64_C(6)}, {"c", INT64_C(7)}}}}});

    Results all_values(r, table->where());

    SECTION("sort by dict field 'a' using path ascending") {

        ExtendedColumnKey child_col_a(col_dict, "a");

        auto sorted = all_values.sort(std::vector<std::pair<std::string, bool>>{{"intDictionary[a]", true}});
        REQUIRE(sorted.size() == 3);
        REQUIRE(sorted.get(0).get<Int>(col_id) == 0);
        REQUIRE(sorted.get(1).get<Int>(col_id) == 3);
        REQUIRE(sorted.get(2).get<Int>(col_id) == 2);
    }

    SECTION("sort by dict field 'a' using ColKey ascending") {

        ExtendedColumnKey child_col_a(col_dict, "a");

        auto sorted =
            all_values.sort({std::vector<std::vector<ExtendedColumnKey>>{{child_col_a}}, std::vector<bool>{true}});
        REQUIRE(sorted.size() == 3);
        REQUIRE(sorted.get(0).get<Int>(col_id) == 0);
        REQUIRE(sorted.get(1).get<Int>(col_id) == 3);
        REQUIRE(sorted.get(2).get<Int>(col_id) == 2);
    }

    SECTION("sort by dict field 'a' using ColKey descending") {
        ExtendedColumnKey child_col_a(col_dict, "a");

        auto sorted =
            all_values.sort({std::vector<std::vector<ExtendedColumnKey>>{{child_col_a}}, std::vector<bool>{false}});
        REQUIRE(sorted.size() == 3);
        REQUIRE(sorted.get(0).get<Int>(col_id) == 2);
        REQUIRE(sorted.get(1).get<Int>(col_id) == 3);
        REQUIRE(sorted.get(2).get<Int>(col_id) == 0);
    }

    SECTION("sort by dict using multiple ColKey descending") {
        ExtendedColumnKey child_col_a(col_dict, "a");

        auto sorted = all_values.sort(
            {std::vector<std::vector<ExtendedColumnKey>>{{col_s1}, {child_col_a}}, std::vector<bool>{true, false}});
        REQUIRE(sorted.size() == 3);
        REQUIRE(sorted.get(0).get<Int>(col_id) == 2);
        REQUIRE(sorted.get(1).get<Int>(col_id) == 0);
        REQUIRE(sorted.get(2).get<Int>(col_id) == 3);
    }

    SECTION("sort by dict using multiple ColKey ascending") {
        ExtendedColumnKey child_col_a(col_dict, "a");

        auto sorted = all_values.sort(
            {std::vector<std::vector<ExtendedColumnKey>>{{col_s1}, {child_col_a}}, std::vector<bool>{true, true}});
        REQUIRE(sorted.size() == 3);
        REQUIRE(sorted.get(0).get<Int>(col_id) == 0);
        REQUIRE(sorted.get(1).get<Int>(col_id) == 2);
        REQUIRE(sorted.get(2).get<Int>(col_id) == 3);
    }

    r->commit_transaction();
}

TEST_CASE("dictionary sort by linked object value", "[dictionary]") {
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object",
         {{"value", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"},
          {"id", PropertyType::Int}}},
        {"target", {{"value", PropertyType::Int}}},
    };

    auto r = Realm::get_shared_realm(config);

    auto table = r->read_group().get_table("class_object");
    auto target = r->read_group().get_table("class_target");
    ColKey id_value = table->get_column_key("id");
    ColKey col_value = target->get_column_key("value");

    r->begin_transaction();
    Obj obj1 = table->create_object().set(id_value, 100);
    Obj obj2 = table->create_object().set(id_value, 200);
    Obj target_obj1 = target->create_object().set(col_value, 100);
    Obj target_obj2 = target->create_object().set(col_value, 200);

    ColKey col = table->get_column_key("value");

    CppContext ctx(r);

    object_store::Dictionary dict1(r, obj1, col);
    dict1.insert("key_a", Mixed{ObjLink(target->get_key(), target_obj1.get_key())});

    object_store::Dictionary dict2(r, obj2, col);
    dict2.insert("key_a", Mixed{ObjLink(target->get_key(), target_obj2.get_key())});
    r->commit_transaction();

    SECTION("sort by dict field 'a' using ColKey ascending") {
        Results all_values(r, table->where());

        ExtendedColumnKey child_col_a(col, "key_a");

        auto sorted = all_values.sort(
            {std::vector<std::vector<ExtendedColumnKey>>{{child_col_a, col_value}}, std::vector<bool>{true}});
        REQUIRE(sorted.size() == 2);
        REQUIRE(sorted.get(0).get<Int>(id_value) == 100);
        REQUIRE(sorted.get(1).get<Int>(id_value) == 200);
    }

    SECTION("sort by dict field 'a' using path") {
        Results all_values(r, table->where());

        auto sorted = all_values.sort(std::vector<std::pair<std::string, bool>>{{"value[key_a].value", true}});
        REQUIRE(sorted.size() == 2);
        REQUIRE(sorted.get(0).get<Int>(id_value) == 100);
        REQUIRE(sorted.get(1).get<Int>(id_value) == 200);
    }

    SECTION("sort by dict field 'a' using ColKey descending") {
        Results all_values(r, table->where());

        ExtendedColumnKey child_col_a(col, "key_a");

        auto sorted = all_values.sort(
            {std::vector<std::vector<ExtendedColumnKey>>{{child_col_a, col_value}}, std::vector<bool>{false}});
        REQUIRE(sorted.size() == 2);
        REQUIRE(sorted.get(0).get<Int>(id_value) == 200);
        REQUIRE(sorted.get(1).get<Int>(id_value) == 100);
    }
}
