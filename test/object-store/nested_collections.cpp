////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>

#include <realm/object-store/list.hpp>
#include <realm/object-store/dictionary.hpp>

#include <realm/db.hpp>
#include <realm/collection_list.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>
#include <realm/dictionary.hpp>

using namespace realm;

TEST_CASE("nested-list-mixed", "[nested-colllections]") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"any", {{"any_val", PropertyType::Mixed | PropertyType::Nullable}}},
    });

    r->begin_transaction();

    auto table = r->read_group().get_table("class_any");
    auto obj = table->create_object();
    auto col = table->get_column_key("any_val");

    // List
    List list_os{obj, col, r};
    list_os.set_list();
    list_os.insert_list(0);
    list_os.insert_list(1);
    list_os.insert_dictionary(2);
    auto nested_list = list_os.get_list(0);
    nested_list.add(Mixed{5});
    nested_list.add(Mixed{10});
    nested_list.add(Mixed{"Hello"});
    auto nested_list1 = list_os.get_list(1);
    nested_list1.add(Mixed{6});
    nested_list1.add(Mixed{7});
    nested_list1.add(Mixed{"World"});
    const char* json_doc_list = "{\"_key\":0,\"any_val\":[[5,10,\"Hello\"],[6,7,\"World\"],{}]}";
    REQUIRE(list_os.get_impl().get_obj().to_string() == json_doc_list);

    // Dictionary.
    object_store::Dictionary dict_os{obj, col, r};
    dict_os.set_dictionary();
    dict_os.insert_dictionary("Dict");
    auto nested_dict = dict_os.get_dictionary("Dict");
    nested_dict.insert({"Test"}, Mixed{10});
    nested_dict.insert({"Test1"}, Mixed{11});
    dict_os.insert_list("List");
    auto nested_list_dict = dict_os.get_list("List");
    nested_list_dict.add(Mixed{"value"});
    const char* json_doc_dict =
        "{\"_key\":0,\"any_val\":{\"Dict\":{},\"List\":[\"value\"],\"Test\":10,\"Test1\":11}}";
    REQUIRE(dict_os.get_impl().get_obj().to_string() == json_doc_dict);

    r->commit_transaction();
}

TEST_CASE("nested-list", "[nested-collections]") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"target", {{"value", PropertyType::Int}}},
        {"list_of_linklist",
         {{"nested_linklist", PropertyType::Array | PropertyType::Object, {CollectionType::List}, "target"}}},
        {"list_of_list", {{"nested_list", PropertyType::Array | PropertyType::Int, {CollectionType::List}}}},
        {"list_of_set", {{"nested_set", PropertyType::Set | PropertyType::Int, {CollectionType::List}}}},
        {"list_of_list_list",
         {{"nested_list_list",
           PropertyType::Array | PropertyType::Int,
           {CollectionType::List, CollectionType::List}}}},
        {"list_of_dictonary_list",
         {{"nested_dict_list",
           PropertyType::Array | PropertyType::Int,
           {CollectionType::List, CollectionType::Dictionary}}}},
    });

    auto target = r->read_group().get_table("class_target");
    auto table1 = r->read_group().get_table("class_list_of_list");
    auto table2 = r->read_group().get_table("class_list_of_set");
    auto table3 = r->read_group().get_table("class_list_of_list_list");
    auto table4 = r->read_group().get_table("class_list_of_dictonary_list");
    auto table5 = r->read_group().get_table("class_list_of_linklist");
    REQUIRE(target);
    REQUIRE(table1);
    REQUIRE(table2);
    REQUIRE(table3);
    REQUIRE(table4);
    REQUIRE(table5);

    // TODO use object store API
    r->begin_transaction();
    /*
        // list of list
        auto nested_obj = table1->create_object();
        auto list_col_key = table1->get_column_key("nested_list");
        auto list1 = nested_obj.get_collection_list(list_col_key);
        CHECK(list1->is_empty());
        auto collection_list1 = list1->insert_collection(0ul);
        auto storage_list = dynamic_cast<Lst<Int>*>(collection_list1.get());
        storage_list->add(5);
        REQUIRE(storage_list->size() == 1);

        // list of set
        auto nested_obj2 = table2->create_object();
        auto set_col_key = table2->get_column_key("nested_set");
        auto list2 = nested_obj2.get_collection_list(set_col_key);
        CHECK(list2->is_empty());
        auto collection_set = list2->insert_collection(0ul);
        auto storage_set = dynamic_cast<Set<Int>*>(collection_set.get());
        storage_set->insert(5);
        REQUIRE(storage_set->size() == 1);

        // list of list of list
        auto nested_obj3 = table3->create_object();
        auto list_list_col_key = table3->get_column_key("nested_list_list");
        auto list3 = nested_obj3.get_collection_list(list_list_col_key);
        CHECK(list3->is_empty());
        list3->insert_collection_list(0ul);
        auto collection_list3 = list3->get_collection_list(0ul);
        auto collection3 = collection_list3->insert_collection(0ul);
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
        list4->insert_collection_list(0ul);
        auto collection4_dict = list4->get_collection_list(0ul);
        auto collection4 = collection4_dict->insert_collection("Test");
        auto storage_list4 = dynamic_cast<Lst<Int>*>(collection4.get());
        storage_list4->add(5);
        REQUIRE(storage_list4->size() == 1);
        REQUIRE(collection4->size() == 1);
        REQUIRE(collection4_dict->size() == 1);

        // list of linklist
        auto target_obj = target->create_object();
        auto link_obj = table5->create_object();
        auto link_col_key = table5->get_column_key("nested_linklist");
        auto list5 = link_obj.get_collection_list(link_col_key);
        CHECK(list5->is_empty());
        auto collection_list5 = list5->insert_collection(0);
        auto link_list = dynamic_cast<LnkLst*>(collection_list5.get());
        link_list->add(target_obj.get_key());
        REQUIRE(link_list->size() == 1);
    */
    r->commit_transaction();
}

TEST_CASE("nested-dictionary", "[nested-collections]") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"dictionary_of_list",
         {{"nested_list", PropertyType::Array | PropertyType::Int, {CollectionType::Dictionary}}}},
        {"dictionary_of_set", {{"nested_set", PropertyType::Set | PropertyType::Int, {CollectionType::Dictionary}}}},
        {"dictionary_of_list_of_dictionary",
         {{"nested_list_dict",
           PropertyType::Dictionary | PropertyType::Int,
           {
               CollectionType::Dictionary,
               CollectionType::List,
           }}}},
    });
    auto table1 = r->read_group().get_table("class_dictionary_of_list");
    auto table2 = r->read_group().get_table("class_dictionary_of_set");
    auto table3 = r->read_group().get_table("class_dictionary_of_list_of_dictionary");
    REQUIRE(table1);
    REQUIRE(table2);
    REQUIRE(table3);

    // TODO use object store API
    r->begin_transaction();
    /*

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
        dict3->insert_collection_list("Foo");
        auto nested_array = dict3->get_collection_list("Foo");
        auto collection3 = nested_array->insert_collection(0);
        auto scollection3 = dynamic_cast<Dictionary*>(collection3.get());
        scollection3->insert("hello", 5);
        REQUIRE(scollection3->size() == 1);
    */

    r->commit_transaction();
}

TEST_CASE("nested-set", "[nested-collections]") {
    // sets can't be parent nodes. Updating the schema should fail.
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    REQUIRE_NOTHROW(r->update_schema({{"set", {{"no_nested", PropertyType::Set | PropertyType::Int}}}}));
    REQUIRE_THROWS(r->update_schema(
        {{"set_of_set", {{"nested", PropertyType::Set | PropertyType::Int, {CollectionType::Set}}}}}));
    REQUIRE_THROWS(r->update_schema(
        {{"set_of_list", {{"nested", PropertyType::Array | PropertyType::Int, {CollectionType::Set}}}}}));
    REQUIRE_THROWS(r->update_schema(
        {{"set_of_dictionary", {{"nested", PropertyType::Dictionary | PropertyType::Int, {CollectionType::Set}}}}}));
    REQUIRE_THROWS(r->update_schema(
        {{"list_set_list",
          {{"nested", PropertyType::Array | PropertyType::Int, {CollectionType::List, CollectionType::Set}}}}}));
    REQUIRE_THROWS(r->update_schema({{"dictionary_set_list",
                                      {{"nested",
                                        PropertyType::Array | PropertyType::Int,
                                        {CollectionType::Dictionary, CollectionType::Set}}}}}));
}
