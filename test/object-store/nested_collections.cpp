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
#include <realm/object-store/set.hpp>
#include <realm/object-store/dictionary.hpp>

#include <realm/db.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>
#include <realm/dictionary.hpp>

using namespace realm;

TEST_CASE("nested-list-mixed", "[nested-collections]") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({{
        "any",
        {{"any_val", PropertyType::Mixed | PropertyType::Nullable}},
    }});

    r->begin_transaction();

    auto table = r->read_group().get_table("class_any");
    auto obj = table->create_object();
    auto col = table->get_column_key("any_val");

    // List
    {
        obj.set_collection(col, CollectionType::List);
        List list_os{r, obj, col};
        list_os.insert_collection(0, CollectionType::List);
        list_os.insert_collection(1, CollectionType::List);
        list_os.insert_collection(2, CollectionType::Dictionary);
        auto nested_list = list_os.get_list(0);
        nested_list.add(Mixed{5});
        nested_list.add(Mixed{10});
        nested_list.add(Mixed{"Hello"});
        auto nested_list1 = list_os.get_list(1);
        nested_list1.add(Mixed{6});
        nested_list1.add(Mixed{7});
        nested_list1.add(Mixed{"World"});
        auto nested_dict = list_os.get_dictionary(2);
        nested_dict.insert("Test", Mixed{"val"});
        nested_dict.insert_collection("Set", CollectionType::Set);
        auto nested_set_dict = nested_dict.get_set("Set");
        nested_set_dict.insert(Mixed{10});
        const char* json_doc_list =
            "{\"_key\":0,\"any_val\":[[5,10,\"Hello\"],[6,7,\"World\"],{\"Set\":[10],\"Test\":\"val\"}]}";
        REQUIRE(list_os.get_impl().get_obj().to_string() == json_doc_list);
        // add a set, this is doable, but it cannnot contain a nested collection in it.
        list_os.insert_collection(3, CollectionType::Set);
        auto nested_set = list_os.get_set(3);
        nested_set.insert(Mixed{5});
        nested_set.insert(Mixed{"Hello"});
        const char* json_doc_list_with_set = "{\"_key\":0,\"any_val\":[[5,10,\"Hello\"],[6,7,\"World\"],{\"Set\":[10]"
                                             ",\"Test\":\"val\"},[5,\"Hello\"]]}";
        REQUIRE(list_os.get_impl().get_obj().to_string() == json_doc_list_with_set);
    }

    // Set
    {
        obj.set_collection(col, CollectionType::Set);
        object_store::Set set{r, obj, col};
        // this should fail, sets cannot have nested collections, thus can only be leaf collections
        REQUIRE_THROWS(set.insert_collection(0, CollectionType::List));

        // create a set and try to add the previous set as Mixed that contains a link to an object.
        auto col2 = table->add_column(type_Mixed, "any_val2");
        obj.set_collection(col2, CollectionType::Set);
        object_store::Set set2{r, obj, col2};
        set2.insert_any(set.get_impl().get_obj());
        // trying to get a collection from a SET is not allowed.
        REQUIRE_THROWS(set2.get_set(0));

        // try to extract the obj and construct the SET, this is technically doable if you know the index.
        auto mixed = set2.get_any(0);
        auto link = mixed.get_link();
        auto hidden_obj = table->get_object(link.get_obj_key());
        object_store::Set other_set{r, hidden_obj, col};
        REQUIRE_THROWS(other_set.insert_collection(0, CollectionType::List));
        other_set.insert_any(Mixed{42});

        const char* json_doc =
            "{\"_key\":0,\"any_val\":[42],\"any_val2\":[{ \"table\": \"class_any\", \"key\": 0 }]}";
        REQUIRE(set.get_impl().get_obj().to_string() == json_doc);

        table->remove_column(col2);
    }

    // Dictionary.
    {
        obj.set_collection(col, CollectionType::Dictionary);
        object_store::Dictionary dict_os{r, obj, col};
        dict_os.insert_collection("Dict", CollectionType::Dictionary);
        auto nested_dict = dict_os.get_dictionary("Dict");
        nested_dict.insert({"Test"}, Mixed{10}); // this crashes..
        nested_dict.insert({"Test1"}, Mixed{11});
        dict_os.insert_collection("List", CollectionType::List);
        auto nested_list_dict = dict_os.get_list("List");
        nested_list_dict.add(Mixed{"value"});
        const char* json_doc_dict =
            "{\"_key\":0,\"any_val\":{\"Dict\":{\"Test\":10,\"Test1\":11},\"List\":[\"value\"]}}";
        REQUIRE(dict_os.get_impl().get_obj().to_string() == json_doc_dict);
    }

    r->commit_transaction();
}
