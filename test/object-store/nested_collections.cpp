////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#include "util/test_file.hpp"
// #include "util/test_utils.hpp"
// #include "util/event_loop.hpp"
// #include "util/index_helpers.hpp"

// #include <realm/object-store/binding_context.hpp>
// #include <realm/object-store/list.hpp>
// #include <realm/object-store/object.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
// #include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>

// #include <realm/object-store/impl/realm_coordinator.hpp>
// #include <realm/object-store/impl/object_accessor_impl.hpp>

// #include <realm/version.hpp>
#include <realm/db.hpp>
#include <realm/collection_list.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>
#include <realm/dictionary.hpp>

// #include <cstdint>

using namespace realm;

TEST_CASE("nested-list", "[nested-collections]") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"list_of_list", {{"nested_list", {PropertyType::Array, PropertyType::Array, PropertyType::Int}}}},
        {"list_of_set", {{"nested_set", {PropertyType::Array, PropertyType::Set, PropertyType::Int}}}},
        {"list_of_list_list",
         {{"nested_list_list", {PropertyType::Array, PropertyType::Array, PropertyType::Array, PropertyType::Int}}}},
        {"list_of_dictonary_list",
         {{"nested_dict_list",
           {PropertyType::Array, PropertyType::Dictionary, PropertyType::Array, PropertyType::Int}}}},
    });
    auto table1 = r->read_group().get_table("class_list_of_list");
    auto table2 = r->read_group().get_table("class_list_of_set");
    auto table3 = r->read_group().get_table("class_list_of_list_list");
    auto table4 = r->read_group().get_table("class_list_of_dictonary_list");
    REQUIRE(table1);
    REQUIRE(table2);
    REQUIRE(table3);
    REQUIRE(table4);

    // TODO use object store API
    r->begin_transaction();
    // list of list
    auto nested_obj = table1->create_object();
    auto list_col_key = table1->get_column_key("nested_list");
    auto list1 = nested_obj.get_collection_list(list_col_key);
    CHECK(list1->is_empty());
    auto collection_list1 = list1->insert_collection(0);
    auto storage_list = dynamic_cast<Lst<Int>*>(collection_list1.get());
    storage_list->add(5);
    REQUIRE(storage_list->size() == 1);

    // list of set
    auto nested_obj2 = table2->create_object();
    auto set_col_key = table2->get_column_key("nested_set");
    auto list2 = nested_obj2.get_collection_list(set_col_key);
    CHECK(list2->is_empty());
    auto collection_set = list2->insert_collection(0);
    auto storage_set = dynamic_cast<Set<Int>*>(collection_set.get());
    storage_set->insert(5);
    REQUIRE(storage_set->size() == 1);

    // list of list of list
    auto nested_obj3 = table3->create_object();
    auto list_list_col_key = table3->get_column_key("nested_list_list");
    auto list3 = nested_obj3.get_collection_list(list_list_col_key);
    CHECK(list3->is_empty());
    auto collection_list3 = list3->insert_collection_list(0);
    auto collection3 = collection_list3->insert_collection(0);
    auto storage_list3 = dynamic_cast<Lst<Int>*>(collection3.get());
    storage_list3->add(5);
    REQUIRE(storage_list3->size() == 1);
    REQUIRE(collection_list3->size() == 1);

    // list of dictionary of list
    auto nested_obj4 = table4->create_object();
    auto nested_dict_col_key = table4->get_column_key("nested_dict_list");
    REQUIRE(table4->get_nesting_levels(nested_dict_col_key) == 2);
    auto list4 = nested_obj4.get_collection_list(nested_dict_col_key);
    CHECK(list4->is_empty());
    auto collection4_dict = list4->insert_collection_list(0);
    auto collection4 = collection4_dict->insert_collection("Test");
    auto storage_list4 = dynamic_cast<Lst<Int>*>(collection4.get());
    storage_list4->add(5);
    REQUIRE(storage_list4->size() == 1);
    REQUIRE(collection4->size() == 1);
    REQUIRE(collection4_dict->size() == 1);

    r->commit_transaction();
}

TEST_CASE("nested-dictionary", "[nested-collections]") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"dictionary_of_list", {{"nested_list", {PropertyType::Dictionary, PropertyType::Array, PropertyType::Int}}}},
        {"dictionary_of_set", {{"nested_set", {PropertyType::Dictionary, PropertyType::Set, PropertyType::Int}}}},
        {"dictionary_of_list_of_dictionary",
         {{"nested_list_dict",
           {PropertyType::Dictionary, PropertyType::Array, PropertyType::Dictionary, PropertyType::Int}}}},
    });
    auto table1 = r->read_group().get_table("class_dictionary_of_list");
    auto table2 = r->read_group().get_table("class_dictionary_of_set");
    auto table3 = r->read_group().get_table("class_dictionary_of_list_of_dictionary");
    REQUIRE(table1);
    REQUIRE(table2);
    REQUIRE(table3);

    // TODO use object store API
    r->begin_transaction();

    auto nested_obj = table1->create_object();
    auto nested_col_key = table1->get_column_key("nested_list");
    auto dict = nested_obj.get_collection_list(nested_col_key);
    auto collection = dict->insert_collection("Foo");
    auto scollection = dynamic_cast<Lst<Int>*>(collection.get());
    scollection->add(5);
    REQUIRE(scollection->size() == 1);

    auto nested_obj2 = table2->create_object();
    auto nested_col_key2 = table2->get_column_key("nested_set");
    auto dict2 = nested_obj2.get_collection_list(nested_col_key2);
    auto collection2 = dict2->insert_collection("Foo");
    auto scollection2 = dynamic_cast<Set<Int>*>(collection2.get());
    scollection2->insert(5);
    REQUIRE(scollection2->size() == 1);

    auto nested_obj3 = table3->create_object();
    auto nested_col_key3 = table3->get_column_key("nested_list_dict");
    auto dict3 = nested_obj3.get_collection_list(nested_col_key3);
    auto nested_array = dict3->insert_collection_list("Foo");
    auto collection3 = nested_array->insert_collection(0);
    auto scollection3 = dynamic_cast<Dictionary*>(collection3.get());
    scollection3->insert("hello", 5);
    REQUIRE(scollection3->size() == 1);


    r->commit_transaction();
}

TEST_CASE("nested-set", "[nested-collections]") {
    // sets can't be parent nodes. Updating the schema should fail.
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    REQUIRE_THROWS(r->update_schema(
        {{"set_of_list", {{"nested", {PropertyType::Set, PropertyType::Array, PropertyType::Int}}}}}));
    REQUIRE_THROWS(r->update_schema(
        {{"set_of_dictionary", {{"nested", {PropertyType::Set, PropertyType::Dictionary, PropertyType::Int}}}}}));
    REQUIRE_THROWS(r->update_schema(
        {{"list_set_list",
          {{"nested", {PropertyType::Array, PropertyType::Set, PropertyType::Array, PropertyType::Int}}}}}));
    REQUIRE_THROWS(r->update_schema(
        {{"dictionary_set_list",
          {{"nested", {PropertyType::Dictionary, PropertyType::Set, PropertyType::Array, PropertyType::Int}}}}}));
}