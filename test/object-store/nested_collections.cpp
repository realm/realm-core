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
#include "util/test_utils.hpp"
#include "util/event_loop.hpp"
#include "util/index_helpers.hpp"

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/list.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm/version.hpp>
#include <realm/db.hpp>

#include <cstdint>

using namespace realm;

TEST_CASE("nested-list") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema(
        {{"list_of_list", {{"nested_list", {PropertyType::Array, PropertyType::Array, PropertyType::Int}}}}});
    auto origin = r->read_group().get_table("class_list_of_list");
    REQUIRE(origin);

    // auto& coordinator = *_impl::RealmCoordinator::get_coordinator(config.path);
    //  auto target = r->read_group().get_table("class_target");
    //  ColKey col_link = origin->get_column_key("array");
    //  //ColKey col_target_value = target->get_column_key("value");

    // r->begin_transaction();

    // std::vector<ObjKey> target_keys;
    // target->create_objects(10, target_keys);
    // for (int i = 0; i < 10; ++i)
    //     target->get_object(target_keys[i]).set_all(i);

    // Obj obj = origin->create_object();
    // auto lv = obj.get_linklist_ptr(col_link);
    // for (int i = 0; i < 10; ++i)
    //     lv->add(target_keys[i]);
    // auto lv2 = origin->create_object().get_linklist_ptr(col_link);
    // for (int i = 0; i < 10; ++i)
    //     lv2->add(target_keys[i]);


    // r->commit_transaction();

    // auto write = [&](auto&& f) {
    //     r->begin_transaction();
    //     f();
    //     r->commit_transaction();
    //     advance_and_notify(*r);
    // };
}