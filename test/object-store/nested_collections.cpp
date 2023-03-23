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
#include <realm/dictionary.hpp>

// #include <cstdint>

using namespace realm;

TEST_CASE("nested-list") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        //{"list", {{"value", PropertyType::Array | PropertyType::Mixed | PropertyType::Nullable}}},
        {"list_of_list", {{"nested_list", {PropertyType::Array, PropertyType::Array, PropertyType::Int}}}},
    });
    auto table = r->read_group().get_table("class_list_of_list");
    // TODO use object store API
    r->begin_transaction();
    auto nested_obj = table->create_object();
    auto nested_col_key = table->get_column_key("nested_list");
    auto list = nested_obj.get_collection_list(nested_col_key);
    CHECK(list->is_empty());
    auto collection = list->insert_collection(0);
    auto storage_list = dynamic_cast<Lst<Int>*>(collection.get());
    storage_list->add(5);
    REQUIRE(storage_list->size() == 1);
}