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

#include "util/index_helpers.hpp"
#include "util/test_file.hpp"

#include "impl/realm_coordinator.hpp"
#include "object_schema.hpp"
#include "property.hpp"
#include "results.hpp"
#include "schema.hpp"

#include <realm/group_shared.hpp>
#include <realm/link_view.hpp>
#include <realm/query_engine.hpp>

#include <unistd.h>

using namespace realm;

TEST_CASE("results: notifications") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {
            {"value", PropertyType::Int},
            {"link", PropertyType::Object, "linked to object", "", false, false, true}
        }},
        {"other object", {
            {"value", PropertyType::Int}
        }},
        {"linking object", {
            {"link", PropertyType::Object, "object", "", false, false, true}
        }},
        {"linked to object", {
            {"value", PropertyType::Int}
        }}
    });

    auto coordinator = _impl::RealmCoordinator::get_existing_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");

    r->begin_transaction();
    table->add_empty_row(10);
    for (int i = 0; i < 10; ++i)
        table->set_int(0, i, i * 2);
    r->commit_transaction();

    Results results(r, table->where().greater(0, 0).less(0, 10));

    SECTION("unsorted notifications") {
        int notification_calls = 0;
        CollectionChangeSet change;
        auto token = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            change = c;
            ++notification_calls;
        });

        advance_and_notify(*r);

        auto write = [&](auto&& f) {
            r->begin_transaction();
            f();
            r->commit_transaction();
            advance_and_notify(*r);
        };

        SECTION("initial results are delivered") {
            REQUIRE(notification_calls == 1);
        }

        SECTION("notifications are sent asynchronously") {
            r->begin_transaction();
            table->set_int(0, 0, 4);
            r->commit_transaction();

            REQUIRE(notification_calls == 1);
            advance_and_notify(*r);
            REQUIRE(notification_calls == 2);
        }

        SECTION("notifications are not delivered when the token is destroyed before they are calculated") {
            r->begin_transaction();
            table->set_int(0, 0, 4);
            r->commit_transaction();

            REQUIRE(notification_calls == 1);
            token = {};
            advance_and_notify(*r);
            REQUIRE(notification_calls == 1);
        }

        SECTION("notifications are not delivered when the token is destroyed before they are delivered") {
            r->begin_transaction();
            table->set_int(0, 0, 4);
            r->commit_transaction();

            REQUIRE(notification_calls == 1);
            coordinator->on_change();
            token = {};
            r->notify();
            REQUIRE(notification_calls == 1);
        }

        SECTION("notifications are delivered when a new callback is added from within a callback") {
            NotificationToken token2, token3;
            bool called = false;
            token2 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
                token3 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
                    called = true;
                });
            });

            advance_and_notify(*r);
            REQUIRE(called);
        }

        SECTION("notifications are not delivered when a callback is removed from within a callback") {
            NotificationToken token2, token3;
            token2 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
                token3 = {};
            });
            token3 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
                REQUIRE(false);
            });

            advance_and_notify(*r);
        }

        SECTION("removing the current callback does not stop later ones from being called") {
            NotificationToken token2, token3;
            bool called = false;
            token2 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
                token2 = {};
            });
            token3 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
                called = true;
            });

            advance_and_notify(*r);

            REQUIRE(called);
        }

        SECTION("modifications to unrelated tables do not send notifications") {
            write([&] {
                r->read_group().get_table("class_other object")->add_empty_row();
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("irrelevant modifications to linked tables do not send notifications") {
            write([&] {
                r->read_group().get_table("class_linked to object")->add_empty_row();
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("irrelevant modifications to linking tables do not send notifications") {
            write([&] {
                r->read_group().get_table("class_linking object")->add_empty_row();
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("modifications that leave a non-matching row non-matching do not send notifications") {
            write([&] {
                table->set_int(0, 6, 13);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("deleting non-matching rows does not send a notification") {
            write([&] {
                table->move_last_over(0);
                table->move_last_over(6);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("swapping adjacent matching and non-matching rows does not send notifications") {
            write([&] {
                table->swap_rows(0, 1);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("swapping non-adjacent matching and non-matching rows send a single insert/delete pair") {
            write([&] {
                table->swap_rows(0, 2);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 1);
            REQUIRE_INDICES(change.insertions, 0);
        }

        SECTION("swapping matching rows sends insert/delete pairs") {
            write([&] {
                table->swap_rows(1, 4);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 0, 3);
            REQUIRE_INDICES(change.insertions, 0, 3);

            write([&] {
                table->swap_rows(1, 2);
                table->swap_rows(2, 3);
                table->swap_rows(3, 4);
            });
            REQUIRE(notification_calls == 3);
            REQUIRE_INDICES(change.deletions, 1, 2, 3);
            REQUIRE_INDICES(change.insertions, 0, 1, 2);
        }

        SECTION("swap does not inhibit move collapsing after removals") {
            write([&] {
                table->swap_rows(2, 3);
                table->set_int(0, 3, 100);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 1);
            REQUIRE(change.insertions.empty());
        }

        SECTION("modifying a matching row and leaving it matching marks that row as modified") {
            write([&] {
                table->set_int(0, 1, 3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.modifications, 0);
            REQUIRE_INDICES(change.modifications_new, 0);
        }

        SECTION("modifying a matching row to no longer match marks that row as deleted") {
            write([&] {
                table->set_int(0, 2, 0);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 1);
        }

        SECTION("modifying a non-matching row to match marks that row as inserted, but not modified") {
            write([&] {
                table->set_int(0, 7, 3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.insertions, 4);
            REQUIRE(change.modifications.empty());
            REQUIRE(change.modifications_new.empty());
        }

        SECTION("deleting a matching row marks that row as deleted") {
            write([&] {
                table->move_last_over(3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 2);
        }

        SECTION("moving a matching row via deletion marks that row as moved") {
            write([&] {
                table->where().greater_equal(0, 10).find_all().clear(RemoveMode::unordered);
                table->move_last_over(0);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_MOVES(change, {3, 0});
        }

        SECTION("moving a matching row via subsumption marks that row as modified") {
            write([&] {
                table->where().greater_equal(0, 10).find_all().clear(RemoveMode::unordered);
                table->move_last_over(0);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_MOVES(change, {3, 0});
        }

        SECTION("modifications from multiple transactions are collapsed") {
            r->begin_transaction();
            table->set_int(0, 0, 6);
            r->commit_transaction();

            coordinator->on_change();

            r->begin_transaction();
            table->set_int(0, 1, 0);
            r->commit_transaction();

            REQUIRE(notification_calls == 1);
            coordinator->on_change();
            r->notify();
            REQUIRE(notification_calls == 2);
        }

        SECTION("inserting a row then modifying it in a second transaction does not report it as modified") {
            r->begin_transaction();
            size_t ndx = table->add_empty_row();
            table->set_int(0, ndx, 6);
            r->commit_transaction();

            coordinator->on_change();

            r->begin_transaction();
            table->set_int(0, ndx, 7);
            r->commit_transaction();

            advance_and_notify(*r);

            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.insertions, 4);
            REQUIRE(change.modifications.empty());
            REQUIRE(change.modifications_new.empty());
        }

        SECTION("modification indices are pre-insert/delete") {
            r->begin_transaction();
            table->set_int(0, 2, 0);
            table->set_int(0, 3, 6);
            r->commit_transaction();
            advance_and_notify(*r);

            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 1);
            REQUIRE_INDICES(change.modifications, 2);
            REQUIRE_INDICES(change.modifications_new, 1);
        }

        SECTION("notifications are not delivered when collapsing transactions results in no net change") {
            r->begin_transaction();
            size_t ndx = table->add_empty_row();
            table->set_int(0, ndx, 5);
            r->commit_transaction();

            coordinator->on_change();

            r->begin_transaction();
            table->move_last_over(ndx);
            r->commit_transaction();

            REQUIRE(notification_calls == 1);
            coordinator->on_change();
            r->notify();
            REQUIRE(notification_calls == 1);
        }

        SECTION("the first call of a notification can include changes if it previously ran for a different callback") {
            auto token2 = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                REQUIRE(!c.empty());
            });

            write([&] {
                table->set_int(0, table->add_empty_row(), 5);
            });
        }
    }

    SECTION("before/after change callback") {
        struct Callback {
            size_t before_calls = 0;
            size_t after_calls = 0;
            CollectionChangeSet before_change;
            CollectionChangeSet after_change;
            std::function<void(void)> on_before = []{};
            std::function<void(void)> on_after = []{};

            void before(CollectionChangeSet c) {
                before_change = c;
                ++before_calls;
                on_before();
            }
            void after(CollectionChangeSet c) {
                after_change = c;
                ++after_calls;
                on_after();
            }
            void error(std::exception_ptr) {
                FAIL("error() should not be called");
            }
        } callback;
        auto token = results.add_notification_callback(&callback);
        advance_and_notify(*r);

        SECTION("only after() is called for initial results") {
            REQUIRE(callback.before_calls == 0);
            REQUIRE(callback.after_calls == 1);
            REQUIRE(callback.after_change.empty());
        }

        auto write = [&](auto&& func) {
            auto r2 = Realm::get_shared_realm(config);
            r2->begin_transaction();
            func(*r2->read_group().get_table("class_object"));
            r2->commit_transaction();
            advance_and_notify(*r);
        };

        SECTION("both are called after a write") {
            write([&](auto&& t) {
                t.set_int(0, t.add_empty_row(), 5);
            });
            REQUIRE(callback.before_calls == 1);
            REQUIRE(callback.after_calls == 2);
            REQUIRE_INDICES(callback.before_change.insertions, 4);
            REQUIRE_INDICES(callback.after_change.insertions, 4);
        }

        SECTION("deleted objects are usable in before()") {
            callback.on_before = [&] {
                REQUIRE(results.size() == 4);
                REQUIRE_INDICES(callback.before_change.deletions, 0);
                REQUIRE(results.get(0).is_attached());
                REQUIRE(results.get(0).get_int(0) == 2);
            };
            write([&](auto&& t) {
                t.move_last_over(results.get(0).get_index());
            });
            REQUIRE(callback.before_calls == 1);
            REQUIRE(callback.after_calls == 2);
        }

        SECTION("inserted objects are usable in after()") {
            callback.on_after = [&] {
                REQUIRE(results.size() == 5);
                REQUIRE_INDICES(callback.after_change.insertions, 4);
                REQUIRE(results.last()->get_int(0) == 5);
            };
            write([&](auto&& t) {
                t.set_int(0, t.add_empty_row(), 5);
            });
            REQUIRE(callback.before_calls == 1);
            REQUIRE(callback.after_calls == 2);
        }
    }

    // Sort in descending order
    results = results.sort({*table, {{0}}, {false}});

    SECTION("sorted notifications") {
        int notification_calls = 0;
        CollectionChangeSet change;
        auto token = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            change = c;
            ++notification_calls;
        });

        advance_and_notify(*r);

        auto write = [&](auto&& f) {
            r->begin_transaction();
            f();
            r->commit_transaction();
            advance_and_notify(*r);
        };

        SECTION("swapping rows does not send notifications") {
            write([&] {
                table->swap_rows(2, 3);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("modifications that leave a non-matching row non-matching do not send notifications") {
            write([&] {
                table->set_int(0, 6, 13);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("deleting non-matching rows does not send a notification") {
            write([&] {
                table->move_last_over(0);
                table->move_last_over(6);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("modifying a matching row and leaving it matching marks that row as modified") {
            write([&] {
                table->set_int(0, 1, 3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.modifications, 3);
            REQUIRE_INDICES(change.modifications_new, 3);
        }

        SECTION("swapping leaves modified rows marked as modified") {
            write([&] {
                table->set_int(0, 1, 3);
                table->swap_rows(1, 2);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.modifications, 3);
            REQUIRE_INDICES(change.modifications_new, 3);

            write([&] {
                table->swap_rows(3, 1);
                table->set_int(0, 1, 7);
            });
            REQUIRE(notification_calls == 3);
            REQUIRE_INDICES(change.modifications, 1);
            REQUIRE_INDICES(change.modifications_new, 1);
        }

        SECTION("modifying a matching row to no longer match marks that row as deleted") {
            write([&] {
                table->set_int(0, 2, 0);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 2);
        }

        SECTION("modifying a non-matching row to match marks that row as inserted") {
            write([&] {
                table->set_int(0, 7, 3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.insertions, 3);
        }

        SECTION("deleting a matching row marks that row as deleted") {
            write([&] {
                table->move_last_over(3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 1);
        }

        SECTION("moving a matching row via deletion does not send a notification") {
            write([&] {
                table->where().greater_equal(0, 10).find_all().clear(RemoveMode::unordered);
                table->move_last_over(0);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("modifying a matching row to change its position sends insert+delete") {
            write([&] {
                table->set_int(0, 2, 9);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 2);
            REQUIRE_INDICES(change.insertions, 0);
        }

        SECTION("modifications from multiple transactions are collapsed") {
            r->begin_transaction();
            table->set_int(0, 0, 5);
            r->commit_transaction();

            r->begin_transaction();
            table->set_int(0, 1, 0);
            r->commit_transaction();

            REQUIRE(notification_calls == 1);
            advance_and_notify(*r);
            REQUIRE(notification_calls == 2);
        }

        SECTION("moving a matching row by deleting all other rows") {
            r->begin_transaction();
            table->clear();
            table->add_empty_row(2);
            table->set_int(0, 0, 15);
            table->set_int(0, 1, 5);
            r->commit_transaction();
            advance_and_notify(*r);

            write([&] {
                table->move_last_over(0);
                table->add_empty_row();
                table->set_int(0, 1, 3);
            });

            REQUIRE(notification_calls == 3);
            REQUIRE(change.deletions.empty());
            REQUIRE_INDICES(change.insertions, 1);
        }
    }
}

#if REALM_PLATFORM_APPLE
TEST_CASE("results: async error handling") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {
            {"value", PropertyType::Int},
        }},
    });

    auto coordinator = _impl::RealmCoordinator::get_existing_coordinator(config.path);
    Results results(r, *r->read_group().get_table("class_object"));

    class OpenFileLimiter {
    public:
        OpenFileLimiter()
        {
            // Set the max open files to zero so that opening new files will fail
            getrlimit(RLIMIT_NOFILE, &m_old);
            rlimit rl = m_old;
            rl.rlim_cur = 0;
            setrlimit(RLIMIT_NOFILE, &rl);
        }

        ~OpenFileLimiter()
        {
            setrlimit(RLIMIT_NOFILE, &m_old);
        }

    private:
        rlimit m_old;
    };

    SECTION("error when opening the advancer SG") {
        OpenFileLimiter limiter;

        SECTION("error is delivered asynchronously") {
            bool called = false;
            auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
                REQUIRE(err);
                called = true;
            });

            REQUIRE(!called);
            coordinator->on_change();
            REQUIRE(!called);
            r->notify();
            REQUIRE(called);
        }

        SECTION("adding another callback does not send the error again") {
            bool called = false;
            auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
                REQUIRE(err);
                REQUIRE_FALSE(called);
                called = true;
            });

            advance_and_notify(*r);

            bool called2 = false;
            auto token2 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
                REQUIRE(err);
                REQUIRE_FALSE(called2);
                called2 = true;
            });

            advance_and_notify(*r);
            REQUIRE(called2);
        }
    }

    SECTION("error when opening the executor SG") {
        SECTION("error is delivered asynchronously") {
            bool called = false;
            auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
                REQUIRE(err);
                called = true;
            });
            OpenFileLimiter limiter;

            REQUIRE(!called);
            coordinator->on_change();
            REQUIRE(!called);
            r->notify();
            REQUIRE(called);
        }

        SECTION("adding another callback does not send the error again") {
            bool called = false;
            auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
                REQUIRE(err);
                REQUIRE_FALSE(called);
                called = true;
            });
            OpenFileLimiter limiter;

            advance_and_notify(*r);

            bool called2 = false;
            auto token2 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
                REQUIRE(err);
                REQUIRE_FALSE(called2);
                called2 = true;
            });

            advance_and_notify(*r);

            REQUIRE(called2);
        }
    }
}
#endif

TEST_CASE("results: notifications after move") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {
            {"value", PropertyType::Int},
        }},
    });

    auto table = r->read_group().get_table("class_object");
    auto results = std::make_unique<Results>(r, *table);

    int notification_calls = 0;
    auto token = results->add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
        REQUIRE_FALSE(err);
        ++notification_calls;
    });

    advance_and_notify(*r);

    auto write = [&](auto&& f) {
        r->begin_transaction();
        f();
        r->commit_transaction();
        advance_and_notify(*r);
    };

    SECTION("notifications continue to work after Results is moved (move-constructor)") {
        Results r(std::move(*results));
        results.reset();

        write([&] {
            table->set_int(0, table->add_empty_row(), 1);
        });
        REQUIRE(notification_calls == 2);
    }

    SECTION("notifications continue to work after Results is moved (move-assignment)") {
        Results r;
        r = std::move(*results);
        results.reset();

        write([&] {
            table->set_int(0, table->add_empty_row(), 1);
        });
        REQUIRE(notification_calls == 2);
    }
}

TEST_CASE("results: error messages") {
    InMemoryTestFile config;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::String},
        }},
    };

    auto r = Realm::get_shared_realm(config);
    auto table = r->read_group().get_table("class_object");
    Results results(r, *table);

    r->begin_transaction();
    table->add_empty_row();
    r->commit_transaction();

    SECTION("out of bounds access") {
        REQUIRE_THROWS_WITH(results.get(5), "Requested index 5 greater than max 1");
    }

    SECTION("unsupported aggregate operation") {
        REQUIRE_THROWS_WITH(results.sum(0), "Cannot sum property 'value': operation not supported for 'string' properties");
    }
}

TEST_CASE("results: snapshots") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int},
            {"array", PropertyType::Array, "linked to object"}
        }},
        {"linked to object", {
            {"value", PropertyType::Int}
        }}
    };

    auto r = Realm::get_shared_realm(config);

    auto write = [&](auto&& f) {
        r->begin_transaction();
        f();
        r->commit_transaction();
        advance_and_notify(*r);
    };

    SECTION("snapshot of empty Results") {
        Results results;
        auto snapshot = results.snapshot();
        REQUIRE(snapshot.size() == 0);
    }

    SECTION("snapshot of Results based on Table") {
        auto table = r->read_group().get_table("class_object");
        Results results(r, *table);

        {
            // A newly-added row should not appear in the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 0);
            write([=]{
                table->add_empty_row();
            });
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 0);
        }

        {
            // Removing a row present in the snapshot should not affect the size of the snapshot,
            // but will result in the snapshot returning a detached row accessor.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 1);
            write([=]{
                table->move_last_over(0);
            });
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());

            // Adding a row at the same index that was formerly present in the snapshot shouldn't
            // affect the state of the snapshot.
            write([=]{
                table->add_empty_row();
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());
        }
    }

    SECTION("snapshot of Results based on LinkView") {
        auto object = r->read_group().get_table("class_object");
        auto linked_to = r->read_group().get_table("class_linked to object");

        write([=]{
            object->add_empty_row();
        });

        LinkViewRef lv = object->get_linklist(1, 0);
        Results results(r, lv);

        {
            // A newly-added row should not appear in the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 0);
            write([=]{
                lv->add(linked_to->add_empty_row());
            });
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 0);
        }

        {
            // Removing a row from the link list should not affect the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 1);
            write([=]{
                lv->remove(0);
            });
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 1);
            REQUIRE(snapshot.get(0).is_attached());

            // Removing a row present in the snapshot from its table should result in the snapshot
            // returning a detached row accessor.
            write([=]{
                linked_to->remove(0);
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());

            // Adding a new row to the link list shouldn't affect the state of the snapshot.
            write([=]{
                lv->add(linked_to->add_empty_row());
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());
        }
    }

    SECTION("snapshot of Results based on Query") {
        auto table = r->read_group().get_table("class_object");
        Query q = table->column<Int>(0) > 0;
        Results results(r, std::move(q));

        {
            // A newly-added row should not appear in the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 0);
            write([=]{
                table->set_int(0, table->add_empty_row(), 1);
            });
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 0);
        }

        {
            // Updating a row to no longer match the query criteria should not affect the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 1);
            write([=]{
                table->set_int(0, 0, 0);
            });
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 1);
            REQUIRE(snapshot.get(0).is_attached());

            // Removing a row present in the snapshot from its table should result in the snapshot
            // returning a detached row accessor.
            write([=]{
                table->remove(0);
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());

            // Adding a new row that matches the query criteria shouldn't affect the state of the snapshot.
            write([=]{
                table->set_int(0, table->add_empty_row(), 1);
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());
        }
    }

    SECTION("snapshot of Results based on TableView from query") {
        auto table = r->read_group().get_table("class_object");
        Query q = table->column<Int>(0) > 0;
        Results results(r, q.find_all());

        {
            // A newly-added row should not appear in the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 0);
            write([=]{
                table->set_int(0, table->add_empty_row(), 1);
            });
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 0);
        }

        {
            // Updating a row to no longer match the query criteria should not affect the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 1);
            write([=]{
                table->set_int(0, 0, 0);
            });
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 1);
            REQUIRE(snapshot.get(0).is_attached());

            // Removing a row present in the snapshot from its table should result in the snapshot
            // returning a detached row accessor.
            write([=]{
                table->remove(0);
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());

            // Adding a new row that matches the query criteria shouldn't affect the state of the snapshot.
            write([=]{
                table->set_int(0, table->add_empty_row(), 1);
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());
        }
    }

    SECTION("snapshot of Results based on TableView from backlinks") {
        auto object = r->read_group().get_table("class_object");
        auto linked_to = r->read_group().get_table("class_linked to object");

        write([=]{
            linked_to->add_empty_row();
        });

        TableView backlinks = linked_to->get_backlink_view(0, object.get(), 1);
        Results results(r, std::move(backlinks));

        auto lv = object->get_linklist(1, object->add_empty_row());

        {
            // A newly-added row should not appear in the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 0);
            write([=]{
                lv->add(0);
            });
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 0);
        }

        {
            // Removing the link should not affect the snapshot.
            auto snapshot = results.snapshot();
            REQUIRE(results.size() == 1);
            REQUIRE(snapshot.size() == 1);
            write([=]{
                lv->remove(0);
            });
            REQUIRE(results.size() == 0);
            REQUIRE(snapshot.size() == 1);
            REQUIRE(snapshot.get(0).is_attached());

            // Removing a row present in the snapshot from its table should result in the snapshot
            // returning a detached row accessor.
            write([=]{
                object->remove(0);
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());

            // Adding a new link shouldn't affect the state of the snapshot.
            write([=]{
                object->add_empty_row();
                auto lv = object->get_linklist(1, object->add_empty_row());
                lv->add(0);
            });
            REQUIRE(snapshot.size() == 1);
            REQUIRE(!snapshot.get(0).is_attached());
        }
    }

    SECTION("snapshot of Results with notification callback registered") {
        auto table = r->read_group().get_table("class_object");
        Query q = table->column<Int>(0) > 0;
        Results results(r, q.find_all());

        auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
        });
        advance_and_notify(*r);

        SECTION("snapshot of lvalue") {
            auto snapshot = results.snapshot();
            write([=] {
                table->set_int(0, table->add_empty_row(), 1);
            });
            REQUIRE(snapshot.size() == 0);
        }

        SECTION("snapshot of rvalue") {
            auto snapshot = std::move(results).snapshot();
            write([=] {
                table->set_int(0, table->add_empty_row(), 1);
            });
            REQUIRE(snapshot.size() == 0);
        }
    }

    SECTION("adding notification callback to snapshot throws") {
        auto table = r->read_group().get_table("class_object");
        Query q = table->column<Int>(0) > 0;
        Results results(r, q.find_all());
        auto snapshot = results.snapshot();
        CHECK_THROWS(snapshot.add_notification_callback([](CollectionChangeSet, std::exception_ptr) {}));
    }
}
