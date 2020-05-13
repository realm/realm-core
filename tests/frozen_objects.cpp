////////////////////////////////////////////////////////////////////////////
//
// Copyright 2019 Realm Inc.
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

#include "catch2/catch.hpp"

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include "binding_context.hpp"
#include "object_accessor.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "property.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "thread_safe_reference.hpp"

#include "impl/object_accessor_impl.hpp"
#include "impl/realm_coordinator.hpp"

#include <realm/db.hpp>
#include <realm/query_expression.hpp>

#if REALM_ENABLE_SYNC
#include "sync/async_open_task.hpp"
#endif

#include <realm/util/scope_exit.hpp>

namespace realm {
class TestHelper {
public:
    static DBRef& get_db(SharedRealm const& shared_realm)
    {
        return Realm::Internal::get_db(*shared_realm);
    }

    static void begin_read(SharedRealm const& shared_realm, VersionID version)
    {
        Realm::Internal::begin_read(*shared_realm, version);
    }
};
}

using namespace realm;

TEST_CASE("Construct frozen Realm") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int}
        }},
    };

    SECTION("Create frozen Realm directly") {
        auto realm = Realm::get_shared_realm(config);
        realm->read_group();
        auto frozen_realm = Realm::get_frozen_realm(config, realm->read_transaction_version());
        REQUIRE(frozen_realm->is_frozen());
        REQUIRE(realm->read_transaction_version() == *frozen_realm->current_transaction_version());
    }
}

TEST_CASE("Freeze Realm", "[freeze_realm]") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int}
        }},
    };

    auto realm = Realm::get_shared_realm(config);
    realm->read_group();
    auto frozen_realm = Realm::get_frozen_realm(config, realm->read_transaction_version());

    SECTION("is_frozen") {
        REQUIRE(frozen_realm->is_frozen());
    }

    SECTION("refresh() returns false") {
        REQUIRE(!frozen_realm->refresh());
    }

    SECTION("wait_for_change() returns false") {
        REQUIRE(!frozen_realm->wait_for_change());
    }

    SECTION("auto_refresh") {
        REQUIRE(!frozen_realm->auto_refresh());
        REQUIRE_THROWS(frozen_realm->set_auto_refresh(true));
        REQUIRE(!frozen_realm->auto_refresh());
    }

    SECTION("begin_transaction() throws") {
        REQUIRE_THROWS(frozen_realm->begin_transaction());
    }

    SECTION("can call methods on another thread") {
        JoiningThread thread([&] {
            // Smoke-test
            REQUIRE_NOTHROW(frozen_realm->write_copy());
            REQUIRE_NOTHROW(frozen_realm->read_transaction_version());
        });
    }

    SECTION("release all locks") {
        frozen_realm->close();
        realm->close();
        REQUIRE(DB::call_with_lock(config.path, [](auto) {}));
    }
}

TEST_CASE("Freeze Results", "[freeze_results]") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int},
            {"int_array", PropertyType::Array|PropertyType::Int},
            {"object_array", PropertyType::Array|PropertyType::Object, "linked to object"}
        }},
        {"linked to object", {
            {"value", PropertyType::Int}
        }}

    };

    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_object");
    auto linked_table = realm->read_group().get_table("class_linked to object");
    auto value_col = table->get_column_key("value");
    auto object_link_col = table->get_column_key("object_array");
    auto int_link_col = table->get_column_key("int_array");
    auto linked_object_value_col = linked_table->get_column_key("value");

    realm->begin_transaction();
    for (int i = 0; i < 8; ++i) {
        Obj obj = table->create_object();
        obj.set(value_col, (i + 2));
        std::shared_ptr<LnkLst> object_link_view = obj.get_linklist_ptr(object_link_col);
        auto int_list = List(realm, obj, int_link_col);
        for (int j = 0; j < 5; ++j) {
            auto child_obj = linked_table->create_object();
            child_obj.set(linked_object_value_col, j + 10);
            object_link_view->add(child_obj.get_key());
            int_list.add(static_cast<Int>(j + 42));
        }
    }
    realm->commit_transaction();

    Results results(realm, table);
    auto frozen_realm = Realm::get_frozen_realm(config, realm->read_transaction_version());
    Results frozen_results = results.freeze(frozen_realm);

    SECTION("is_frozen") {
        REQUIRE(!results.is_frozen());
        REQUIRE(frozen_results.is_frozen());
        JoiningThread thread([&] {
            // Check is_frozen across threads
            REQUIRE(!results.is_frozen());
            REQUIRE(frozen_results.is_frozen());
        });
    }

    SECTION("add_notification throws") {
        REQUIRE_THROWS(frozen_results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {}));
    }

    SECTION("Result constructor - Empty") {
        Results res = Results();
        REQUIRE(res.is_frozen()); // All Results are considered frozen
        Results frozen_res = res.freeze(frozen_realm);
        JoiningThread thread([&] {
            REQUIRE(frozen_res.is_frozen());
            REQUIRE(frozen_res.size() == 0);
        });
    }

    SECTION("Result constructor - Table") {
        Results res = Results(frozen_realm, frozen_realm->read_group().get_table("class_object"));
        Results frozen_res = results.freeze(frozen_realm);
        JoiningThread thread([&] {
            auto obj = frozen_res.get(0);
            REQUIRE(obj.is_valid());
            REQUIRE(Object(frozen_realm, obj).is_frozen());
            REQUIRE(frozen_res.get(0).get<int64_t>(value_col) == 2);
            REQUIRE(frozen_res.first()->get<int64_t>(value_col) == 2);
        });
    }

    SECTION("Result constructor - Primitive list") {
        const List list = List(frozen_realm, table->get_object(0), int_link_col);
        auto list_results = list.as_results();

        Results frozen_res = list_results.freeze(frozen_realm);
        JoiningThread thread1([&] {
            REQUIRE(frozen_res.is_frozen());
            REQUIRE(frozen_res.size() == 5);
            REQUIRE(frozen_res.get<Int>(0) == 42);
        });

        Results sorted_frozen_res = list.sort({{"self", false}}).freeze(frozen_realm);
        JoiningThread thread2([&] {
            REQUIRE(sorted_frozen_res.is_frozen());
            REQUIRE(sorted_frozen_res.size() == 5);
            REQUIRE(sorted_frozen_res.get<Int>(0) == 46);
        });
    }

    SECTION("Result constructor - Query") {
        Query q = table->column<Int>(value_col) > 0;
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{value_col}}, {false}));
        Results query_results(realm, std::move(q), ordering);
        Results frozen_res = query_results.freeze(frozen_realm);
        JoiningThread thread([&] {
            auto obj = frozen_res.get(0);
            REQUIRE(obj.is_valid());
            REQUIRE(Object(frozen_realm, obj).is_frozen());
            REQUIRE(frozen_res.get(0).get<Int>(value_col) == 9);
            REQUIRE(frozen_res.first()->get<Int>(value_col) == 9);
        });
    }

    SECTION("Result constructor - TableView") {
        Query q = table->column<Int>(value_col) > 2;
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{value_col}}, {false}));
        TableView tv = q.find_all();
        Results query_results(realm, tv, ordering);
        auto obj = query_results.get(0);
        Results frozen_res = query_results.freeze(frozen_realm);
        JoiningThread thread([&] {
            REQUIRE(frozen_res.is_frozen());
            REQUIRE(frozen_res.get(0).get<int64_t>(value_col) == 3);
            REQUIRE(frozen_res.first()->get<int64_t>(value_col) == 3);
        });
    }

    SECTION("Result constructor - LinkList") {
        Obj obj = results.get(0);
        std::shared_ptr<LnkLst> link_list = obj.get_linklist_ptr(object_link_col);
        Results res = Results(realm, link_list);
        Results frozen_res = res.freeze(frozen_realm);
        JoiningThread thread([&] {
            REQUIRE(frozen_res.is_frozen());
            REQUIRE(frozen_res.size() == 5);
            Object o = Object(frozen_realm, frozen_res.get(0));
            REQUIRE(o.is_frozen());
            REQUIRE(o.get_column_value<Int>("value") == 10);
            REQUIRE(frozen_res.get(0).get<Int>(linked_object_value_col) == 10);
            REQUIRE(frozen_res.first()->get<Int>(linked_object_value_col) == 10);
        });
    }

    SECTION("release all locks") {
        frozen_realm->close();
        realm->close();
        REQUIRE(DB::call_with_lock(config.path, [](auto) {}));
    }
}

TEST_CASE("Freeze List", "[freeze_list]") {

    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int},
            {"int_array", PropertyType::Array|PropertyType::Int},
            {"object_array", PropertyType::Array|PropertyType::Object, "linked to object"}
        }},
        {"linked to object", {
            {"value", PropertyType::Int}
        }}

    };

    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_object");
    auto linked_table = realm->read_group().get_table("class_linked to object");
    auto value_col = table->get_column_key("value");
    auto object_link_col = table->get_column_key("object_array");
    auto int_link_col = table->get_column_key("int_array");
    auto linked_object_value_col = linked_table->get_column_key("value");

    realm->begin_transaction();
    Obj obj = table->create_object();
    obj.set(value_col, 100);
    std::shared_ptr<LnkLst> object_link_view = obj.get_linklist_ptr(object_link_col);
    auto int_list = List(realm, obj, int_link_col);
    for (int j = 0; j < 5; ++j) {
        auto child_obj = linked_table->create_object();
        child_obj.set(linked_object_value_col, j + 10);
        object_link_view->add(child_obj.get_key());
        int_list.add(static_cast<Int>(j + 42));
    }
    realm->commit_transaction();

    Results results(realm, table);
    auto frozen_realm = Realm::get_frozen_realm(config, realm->read_transaction_version());

    std::shared_ptr<LnkLst> link_list = results.get(0).get_linklist_ptr(object_link_col);
    List frozen_link_list = List(realm, *link_list).freeze(frozen_realm);
    List frozen_primitive_list = List(realm, table->get_object(0), int_link_col).freeze(frozen_realm);

    SECTION("is_frozen") {
        REQUIRE(frozen_primitive_list.is_frozen());
        REQUIRE(frozen_link_list.is_frozen());
        JoiningThread thread([&] {
            REQUIRE(frozen_primitive_list.is_frozen());
            REQUIRE(frozen_link_list.is_frozen());
        });
    }

    SECTION("add_notification throws") {
        REQUIRE_THROWS(frozen_link_list.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {}));
        REQUIRE_THROWS(frozen_primitive_list.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {}));
    }

    SECTION("read across threads") {
        JoiningThread thread([&] {
            REQUIRE(frozen_primitive_list.size() == 5);
            REQUIRE(frozen_link_list.size() == 5);
            REQUIRE(frozen_primitive_list.get<Int>(0) == 42);
            REQUIRE(frozen_link_list.get(0).get<Int>(linked_object_value_col) == 10);
            REQUIRE(frozen_primitive_list.get<Int>(0) == 42);
            REQUIRE(frozen_link_list.get(0).get<Int>(linked_object_value_col) == 10);
        });
    }

    SECTION("release all locks") {
        frozen_realm->close();
        realm->close();
        REQUIRE(DB::call_with_lock(config.path, [](auto) {}));
    }

}

TEST_CASE("Freeze Object", "[freeze_object]") {

    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int},
            {"int_array", PropertyType::Array|PropertyType::Int},
            {"object_array", PropertyType::Array|PropertyType::Object, "linked to object"}
        }},
        {"linked to object", {
            {"value", PropertyType::Int}
        }}

    };

    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_object");
    auto linked_table = realm->read_group().get_table("class_linked to object");
    auto value_col = table->get_column_key("value");
    auto object_link_col = table->get_column_key("object_array");
    auto int_link_col = table->get_column_key("int_array");
    auto linked_object_value_col = linked_table->get_column_key("value");

    realm->begin_transaction();
    Obj obj = table->create_object();
    obj.set(value_col, 100);
    std::shared_ptr<LnkLst> object_link_view = obj.get_linklist_ptr(object_link_col);
    auto int_list = List(realm, obj, int_link_col);
    for (int j = 0; j < 5; ++j) {
        auto child_obj = linked_table->create_object();
        child_obj.set(linked_object_value_col, j + 10);
        object_link_view->add(child_obj.get_key());
        int_list.add(static_cast<Int>(j + 42));
    }
    realm->commit_transaction();

    Results results(realm, table);
    auto frozen_realm = Realm::get_frozen_realm(config, realm->read_transaction_version());
    Object frozen_obj = Object(realm, table->get_object(0)).freeze(frozen_realm);
    CppContext ctx(frozen_realm);

    SECTION("is_frozen") {
        REQUIRE(frozen_obj.is_frozen());
    }

    SECTION("add_notification throws") {
        REQUIRE_THROWS(frozen_obj.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {}));
    }

    SECTION("read across threads") {
        JoiningThread thread([&] {
            REQUIRE(frozen_obj.is_valid());
            REQUIRE(any_cast<Int>(frozen_obj.get_property_value<util::Any>(ctx, "value")) == 100);
            auto object_list = any_cast<List&&>(frozen_obj.get_property_value<util::Any>(ctx, "object_array"));
            REQUIRE(object_list.is_frozen());
            REQUIRE(object_list.is_valid());
            REQUIRE(object_list.get(0).get<Int>(linked_object_value_col) == 10);
        });
    }

    SECTION("release all locks") {
        frozen_realm->close();
        realm->close();
        REQUIRE(DB::call_with_lock(config.path, [](auto) {}));
    }
}
