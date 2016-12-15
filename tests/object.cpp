
////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include "catch.hpp"

#include "util/test_file.hpp"
#include "util/index_helpers.hpp"

#include "collection_notifications.hpp"
#include "object.hpp"
#include "object_schema.hpp"
#include "property.hpp"
#include "schema.hpp"

#include "impl/realm_coordinator.hpp"

#include <realm/group_shared.hpp>
#include <realm/link_view.hpp>

using namespace realm;

TEST_CASE("object") {
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.cache = false;
    config.schema = Schema{
        {"table", {
            {"value 1", PropertyType::Int},
            {"value 2", PropertyType::Int},
        }},
    };
    config.schema_version = 0;
    auto r = Realm::get_shared_realm(config);

    auto& coordinator = *_impl::RealmCoordinator::get_existing_coordinator(config.path);

    auto table = r->read_group().get_table("class_table");
    r->begin_transaction();

    table->add_empty_row(10);
    for (int i = 0; i < 10; ++i)
        table->set_int(0, i, i);
    r->commit_transaction();

    auto r2 = coordinator.get_realm();

    SECTION("add_notification_block()") {
        CollectionChangeSet change;
        Row row = table->get(0);

        auto write = [&](auto&& f) {
            r->begin_transaction();
            f();
            r->commit_transaction();

            advance_and_notify(*r);
        };

        auto require_change = [&] {
            auto token = Object(r, *r->schema().find("table"), row).add_notification_block([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            advance_and_notify(*r);
            return token;
        };

        auto require_no_change = [&] {
            bool first = true;
            auto token = Object(r, *r->schema().find("table"), row).add_notification_block([&](CollectionChangeSet, std::exception_ptr) {
                REQUIRE(first);
                first = false;
            });
            advance_and_notify(*r);
            return token;
        };

        SECTION("deleting the object sends a change notification") {
            auto token = require_change();
            write([&] { row.move_last_over(); });
            REQUIRE_INDICES(change.deletions, 0);
        }

        SECTION("modifying the object sends a change notification") {
            auto token = require_change();

            write([&] { row.set_int(0, 10); });
            REQUIRE_INDICES(change.modifications, 0);
            REQUIRE(change.columns.size() == 1);
            REQUIRE_INDICES(change.columns[0], 0);

            write([&] { row.set_int(1, 10); });
            REQUIRE_INDICES(change.modifications, 0);
            REQUIRE(change.columns.size() == 2);
            REQUIRE(change.columns[0].empty());
            REQUIRE_INDICES(change.columns[1], 0);
        }

        SECTION("modifying a different object") {
            auto token = require_no_change();
            write([&] { table->get(1).set_int(0, 10); });
        }

        SECTION("moving the object") {
            auto token = require_no_change();
            write([&] { table->swap_rows(0, 5); });
        }

        SECTION("subsuming the object") {
            auto token = require_change();
            write([&] {
                table->insert_empty_row(0);
                table->merge_rows(row.get_index(), 0);
                row.set_int(0, 10);
            });
            REQUIRE(change.columns.size() == 1);
            REQUIRE_INDICES(change.columns[0], 0);
        }

        SECTION("multiple write transactions") {
            auto token = require_change();

            auto r2row = r2->read_group().get_table("class_table")->get(0);
            r2->begin_transaction();
            r2row.set_int(0, 1);
            r2->commit_transaction();
            r2->begin_transaction();
            r2row.set_int(1, 2);
            r2->commit_transaction();

            advance_and_notify(*r);
            REQUIRE(change.columns.size() == 2);
            REQUIRE_INDICES(change.columns[0], 0);
            REQUIRE_INDICES(change.columns[1], 0);
        }
    }
}
