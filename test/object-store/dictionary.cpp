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

#include "collection_fixtures.hpp"
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

namespace cf = realm::collection_fixtures;

TEMPLATE_TEST_CASE("dictionary types", "[dictionary]", cf::MixedVal, cf::Int, cf::Bool, cf::Float, cf::Double,
                   cf::String, cf::Binary, cf::Date, cf::OID, cf::Decimal, cf::UUID, cf::BoxedOptional<cf::Int>,
                   cf::BoxedOptional<cf::Bool>, cf::BoxedOptional<cf::Float>, cf::BoxedOptional<cf::Double>,
                   cf::BoxedOptional<cf::OID>, cf::BoxedOptional<cf::UUID>, cf::UnboxedOptional<cf::String>,
                   cf::UnboxedOptional<cf::Binary>, cf::UnboxedOptional<cf::Date>, cf::UnboxedOptional<cf::Decimal>)
{
    using T = typename TestType::Type;
    using W = typename TestType::Wrapped;
    using Boxed = typename TestType::Boxed;

    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema =
        Schema{{"object",
                {{"value", PropertyType::Dictionary | TestType::property_type()},
                 {"links", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"}}},
               {"target", {{"value", PropertyType::Int}}}};

    auto r = Realm::get_shared_realm(config);
    auto r2 = Realm::get_shared_realm(config);

    auto table = r->read_group().get_table("class_object");
    auto target = r->read_group().get_table("class_target");
    auto table2 = r2->read_group().get_table("class_object");
    r->begin_transaction();
    Obj obj = table->create_object();
    Obj another = target->create_object();
    ColKey col = table->get_column_key("value");
    ColKey col_links = table->get_column_key("links");

    object_store::Dictionary dict(r, obj, col);
    object_store::Dictionary links(r, obj, col_links);
    auto keys_as_results = dict.get_keys();
    auto values_as_results = dict.get_values();
    CppContext ctx(r);

    SECTION("get_realm()") {
        REQUIRE(dict.get_realm() == r);
        REQUIRE(values_as_results.get_realm() == r);
    }

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

    SECTION("clear()") {
        REQUIRE(dict.size() == keys.size());
        values_as_results.clear();
        REQUIRE(dict.size() == 0);
        REQUIRE(values_as_results.size() == 0);
    }

    SECTION("get()") {
        for (size_t i = 0; i < values.size(); ++i) {
            REQUIRE(dict.get<T>(keys[i]) == values[i]);
            auto val = dict.get(ctx, keys[i]);
            REQUIRE(any_cast<Boxed>(val) == Boxed(values[i]));
        }
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
        REQUIRE(keys_as_results.get_type() == PropertyType::String);
        SECTION("ascending") {
            auto sorted = keys_as_results.sort({{"self", true}});
            std::sort(begin(keys), end(keys), cf::less());
            verify_keys_ordered(sorted);
        }
        SECTION("descending") {
            auto sorted = keys_as_results.sort({{"self", false}});
            std::sort(begin(keys), end(keys), cf::greater());
            verify_keys_ordered(sorted);
        }
    }
    SECTION("values sorted") {
        REQUIRE(values_as_results.get_type() == TestType::property_type());
        SECTION("ascending") {
            auto sorted = values_as_results.sort({{"self", true}});
            std::sort(begin(values), end(values), cf::less());
            verify_values_ordered(sorted);
        }
        SECTION("descending") {
            auto sorted = values_as_results.sort({{"self", false}});
            std::sort(begin(values), end(values), cf::greater());
            verify_values_ordered(sorted);
        }
    }

    SECTION("keys distinct") {
        REQUIRE(keys_as_results.get_type() == PropertyType::String);
        keys.clear();
        // set keys up in dictionary order
        for (size_t i = 0; i < keys_as_results.size(); ++i) {
            keys.push_back(std::string(keys_as_results.get<StringData>(i)));
        }
        auto distinct = keys_as_results.distinct({{"self"}});
        verify_keys_ordered(distinct);
    }

    SECTION("values distinct") {
        REQUIRE(values_as_results.get_type() == TestType::property_type());
        SECTION("original") {
            auto distinct = values_as_results.distinct({{"self"}});
            values.clear();
            // set values up in dictionary order
            for (size_t i = 0; i < values_as_results.size(); ++i) {
                values.push_back(values_as_results.get<T>(i));
            }
            verify_values_ordered(distinct);
        }
    }

    SECTION("handover") {
        r->commit_transaction();

        auto dict2 = ThreadSafeReference(dict).resolve<object_store::Dictionary>(r);
        REQUIRE(dict == dict2);
        ThreadSafeReference ref(values_as_results);
        auto results2 = ref.resolve<Results>(r).sort({{"self", true}});
        std::sort(begin(values), end(values), cf::less());
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
            auto token =
                dict.add_key_based_notification_callback([&key_change](DictionaryChangeSet c, std::exception_ptr) {
                    key_change = c;
                });
            advance_and_notify(*r);

            r->begin_transaction();
            dict.insert(keys[0], T(values[1]));
            r->commit_transaction();

            advance_and_notify(*r);
            REQUIRE(key_change.insertions.size() == 0);
            REQUIRE(key_change.deletions.size() == 0);
            REQUIRE(key_change.modifications[0].get_string() == keys[0]);
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
            auto x = links.add_notification_callback([&local_change](CollectionChangeSet c, std::exception_ptr) {
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
                auto frozen_obj = frozen_results.get(0);
                REQUIRE(!frozen_obj);
                frozen_obj = frozen_results.get(1);
                REQUIRE(frozen_obj);
                REQUIRE(frozen_obj.get_key() == another.get_key());
            }

            auto obj = res.get(0);
            REQUIRE(!obj);
            obj = res.get(1);
            REQUIRE(obj);
            REQUIRE(obj.get_key() == another.get_key());
            r->begin_transaction();
            another.remove();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(local_change.modifications.count() == 1);
        }
    }
}

TEST_CASE("embedded dictionary", "[dictionary]") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"origin", {{"links", PropertyType::Dictionary | PropertyType::Object | PropertyType::Nullable, "target"}}},
        {"target", ObjectSchema::IsEmbedded{true}, {{"value", PropertyType::Int}}}};

    auto r = Realm::get_shared_realm(config);

    auto origin = r->read_group().get_table("class_origin");
    auto target = r->read_group().get_table("class_target");

    r->begin_transaction();
    Obj obj = origin->create_object();
    ColKey col_links = origin->get_column_key("links");
    ColKey col_value = target->get_column_key("value");

    object_store::Dictionary dict(r, obj, col_links);
    for (int i = 0; i < 10; ++i)
        dict.insert_embedded(util::to_string(i));

    r->commit_transaction();

    CppContext ctx(r);
    auto initial_target_size = target->size();

    SECTION("insert(Context)") {
        CppContext ctx(r, &dict.get_object_schema());
        r->begin_transaction();

        SECTION("rejects boxed Obj and Object") {
            REQUIRE_THROWS_AS(dict.insert(ctx, "foo", util::Any(target->get_object(5))),
                              List::InvalidEmbeddedOperationException);
            REQUIRE_THROWS_AS(dict.insert(ctx, "foo", util::Any(Object(r, target->get_object(5)))),
                              List::InvalidEmbeddedOperationException);
        }

        SECTION("creates new object for dictionary") {
            dict.insert(ctx, "foo", util::Any(AnyDict{{"value", INT64_C(20)}}));
            REQUIRE(dict.size() == 11);
            REQUIRE(target->size() == initial_target_size + 1);
            REQUIRE(dict.get_object("foo").get<Int>(col_value) == 20);
        }

        r->cancel_transaction();
    }
}
