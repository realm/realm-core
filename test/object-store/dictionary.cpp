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

#include <catch2/catch.hpp>

#include "util/test_file.hpp"
#include "util/index_helpers.hpp"

#include <realm/object-store/dictionary.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm.hpp>
#include <realm/query_expression.hpp>

using namespace realm;
using namespace realm::util;

namespace Catch {
template <>
struct StringMaker<object_store::Dictionary> {
    static std::string convert(const object_store::Dictionary& dict)
    {
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

TEST_CASE("dictionary") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Dictionary | PropertyType::String}}},
    };
    auto r = Realm::get_shared_realm(config);
    auto r2 = Realm::get_shared_realm(config);

    auto table = r->read_group().get_table("class_object");
    auto table2 = r2->read_group().get_table("class_object");
    r->begin_transaction();
    Obj obj = table->create_object();
    ColKey col = table->get_column_key("value");

    object_store::Dictionary dict(r, obj, col);
    auto keys_as_results = dict.get_keys();
    auto values_as_results = dict.get_values();
    CppContext ctx(r);

    SECTION("get_realm()") {
        REQUIRE(dict.get_realm() == r);
        REQUIRE(values_as_results.get_realm() == r);
    }

    std::vector<std::string> keys = {"a", "b", "c"};
    std::vector<std::string> values = {"apple", "banana", "clementine"};

    for (size_t i = 0; i < values.size(); ++i) {
        dict.insert(keys[i], values[i]);
    }


    SECTION("clear()") {
        REQUIRE(dict.size() == 3);
        values_as_results.clear();
        REQUIRE(dict.size() == 0);
        REQUIRE(values_as_results.size() == 0);
    }

    SECTION("get()") {
        for (size_t i = 0; i < values.size(); ++i) {
            REQUIRE(dict.get<String>(keys[i]) == values[i]);
            auto val = dict.get(ctx, keys[i]);
            REQUIRE(any_cast<std::string>(val) == values[i]);
        }
    }

    SECTION("insert()") {
        for (size_t i = 0; i < values.size(); ++i) {
            auto rev = values.size() - i - 1;
            dict.insert(keys[i], values[rev]);
            REQUIRE(dict.get<StringData>(keys[i]) == values[rev]);
        }
        for (size_t i = 0; i < values.size(); ++i) {
            dict.insert(ctx, keys[i], util::Any(values[i]));
            REQUIRE(dict.get<StringData>(keys[i]) == values[i]);
        }
    }

    SECTION("iteration") {
        for (size_t i = 0; i < values.size(); ++i) {
            auto ndx = dict.find_any(values[i]);
            Dictionary::Iterator it = dict.begin() + ndx;
            REQUIRE((*it).first.get_string() == keys[i]);
            REQUIRE((*it).second.get_string() == values[i]);
            auto element = values_as_results.get_dictionary_element(ndx);
            REQUIRE(element.first == keys[i]);
            REQUIRE(element.second.get_string() == values[i]);
            std::string key = keys_as_results.get<StringData>(ndx);
            REQUIRE(key == keys[i]);
            Mixed m = keys_as_results.get_any(ndx);
            REQUIRE(m.get_string() == keys[i]);
        }
    }

    SECTION("keys sorted") {
        auto sorted = keys_as_results.sort({{"self", true}});
        std::string key = sorted.get<StringData>(0);
        REQUIRE(key == "a");
        Mixed m = sorted.get_any(0);
        REQUIRE(m.get_string() == "a");
        m = sorted.get_any(4);
        REQUIRE(m.is_null());
    }

    SECTION("handover") {
        r->commit_transaction();

        auto dict2 = ThreadSafeReference(dict).resolve<object_store::Dictionary>(r);
        REQUIRE(dict == dict2);
        ThreadSafeReference ref(values_as_results);
        auto results2 = ref.resolve<Results>(r).sort({{"self", true}});
        for (size_t i = 0; i < values.size(); ++i) {
            REQUIRE(results2.get<String>(i) == values[i]);
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
        auto token = dict.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
            change = c;
            ++calls;
        });
        auto rtoken = values_as_results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
            rchange = c;
            ++calls;
        });
        auto srtoken = sorted.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
            srchange = c;
            ++calls;
        });

        SECTION("add value to dictionary") {
            // Remove the existing copy of this value so that the sorted list
            // doesn't have dupes resulting in an unstable order
            advance_and_notify(*r);
            r->begin_transaction();
            dict.erase("b");
            r->commit_transaction();

            advance_and_notify(*r);
            r->begin_transaction();
            dict.insert("d", "dade");
            r->commit_transaction();

            advance_and_notify(*r);
            auto ndx = values_as_results.index_of(StringData("dade"));
            REQUIRE_INDICES(change.insertions, ndx);
            REQUIRE_INDICES(rchange.insertions, ndx);
            // "dade" ends up at the end of the sorted list
            REQUIRE_INDICES(srchange.insertions, values.size() - 1);
        }

        SECTION("replace value in dictionary") {
            advance_and_notify(*r);
            r->begin_transaction();
            dict.insert("b", "blueberry");
            r->commit_transaction();

            advance_and_notify(*r);
            auto ndx = values_as_results.index_of(StringData("blueberry"));
            REQUIRE_INDICES(change.insertions);
            REQUIRE_INDICES(change.modifications, ndx);
            REQUIRE_INDICES(change.deletions);
        }

        SECTION("remove value from list") {
            advance_and_notify(*r);
            auto ndx = values_as_results.index_of(StringData("apple"));
            r->begin_transaction();
            dict.erase("a");
            r->commit_transaction();

            advance_and_notify(*r);
            REQUIRE_INDICES(change.deletions, ndx);
            REQUIRE_INDICES(rchange.deletions, ndx);
            // apple comes first in the sorted result.
            REQUIRE_INDICES(srchange.deletions, 0);
        }

        SECTION("clear list") {
            advance_and_notify(*r);

            r->begin_transaction();
            dict.remove_all();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(change.deletions.count() == values.size());
            REQUIRE(rchange.deletions.count() == values.size());
            REQUIRE(srchange.deletions.count() == values.size());
        }

        SECTION("delete containing row") {
            advance_and_notify(*r);
            REQUIRE(calls == 3);

            r->begin_transaction();
            obj.remove();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(calls == 6);
            REQUIRE(change.deletions.count() == values.size());
            REQUIRE(rchange.deletions.count() == values.size());
            REQUIRE(srchange.deletions.count() == values.size());

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
        }
    }
}
