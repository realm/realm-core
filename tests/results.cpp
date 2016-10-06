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

class joining_thread {
public:
    template<typename... Args>
    joining_thread(Args&&... args) : m_thread(std::forward<Args>(args)...) { }
    ~joining_thread() { if (m_thread.joinable()) m_thread.join(); }
    void join() { m_thread.join(); }

private:
    std::thread m_thread;
};

TEST_CASE("notifications: async delivery") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {
            {"value", PropertyType::Int}
        }},
    });

    auto coordinator = _impl::RealmCoordinator::get_existing_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");

    r->begin_transaction();
    table->add_empty_row(10);
    for (int i = 0; i < 10; ++i)
        table->set_int(0, i, i * 2);
    r->commit_transaction();

    Results results(r, table->where().greater(0, 0).less(0, 10));

    int notification_calls = 0;
    auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
        REQUIRE_FALSE(err);
        ++notification_calls;
    });

    auto make_local_change = [&] {
        r->begin_transaction();
        table->set_int(0, 0, 4);
        r->commit_transaction();
    };

    auto make_remote_change = [&] {
        auto r2 = coordinator->get_realm();
        r2->begin_transaction();
        r2->read_group().get_table("class_object")->set_int(0, 0, 5);
        r2->commit_transaction();
    };

    SECTION("initial notification") {
        SECTION("is delivered on notify()") {
            REQUIRE(notification_calls == 0);
            advance_and_notify(*r);
            REQUIRE(notification_calls == 1);
        }

        SECTION("is delivered on refresh()") {
            coordinator->on_change();
            REQUIRE(notification_calls == 0);
            r->refresh();
            REQUIRE(notification_calls == 1);
        }

        SECTION("is delivered on begin_transaction()") {
            coordinator->on_change();
            REQUIRE(notification_calls == 0);
            r->begin_transaction();
            REQUIRE(notification_calls == 1);
            r->cancel_transaction();
        }

        SECTION("is delivered on notify() even with autorefresh disabled") {
            r->set_auto_refresh(false);
            REQUIRE(notification_calls == 0);
            advance_and_notify(*r);
            REQUIRE(notification_calls == 1);
        }

        SECTION("refresh() blocks due to initial results not being ready") {
            REQUIRE(notification_calls == 0);
            joining_thread thread([&] {
                usleep(5000);
                coordinator->on_change();
            });
            r->refresh();
            REQUIRE(notification_calls == 1);
        }

        SECTION("begin_transaction() blocks due to initial results not being ready") {
            REQUIRE(notification_calls == 0);
            joining_thread thread([&] {
                usleep(5000);
                coordinator->on_change();
            });
            r->begin_transaction();
            REQUIRE(notification_calls == 1);
            r->cancel_transaction();
        }

        SECTION("notify() does not block due to initial results not being ready") {
            REQUIRE(notification_calls == 0);
            r->notify();
            REQUIRE(notification_calls == 0);
        }

        SECTION("is delivered after invalidate()") {
            r->invalidate();

            SECTION("notify()") {
                coordinator->on_change();
                REQUIRE_FALSE(r->is_in_read_transaction());
                r->notify();
                REQUIRE(notification_calls == 1);
            }

            SECTION("notify() without autorefresh") {
                r->set_auto_refresh(false);
                coordinator->on_change();
                REQUIRE_FALSE(r->is_in_read_transaction());
                r->notify();
                REQUIRE(notification_calls == 1);
            }

            SECTION("refresh()") {
                coordinator->on_change();
                REQUIRE_FALSE(r->is_in_read_transaction());
                r->refresh();
                REQUIRE(notification_calls == 1);
            }

            SECTION("begin_transaction()") {
                coordinator->on_change();
                REQUIRE_FALSE(r->is_in_read_transaction());
                r->begin_transaction();
                REQUIRE(notification_calls == 1);
                r->cancel_transaction();
            }
        }
    }

    advance_and_notify(*r);

    SECTION("notifications for local changes") {
        make_local_change();
        coordinator->on_change();
        REQUIRE(notification_calls == 1);

        SECTION("notify()") {
            r->notify();
            REQUIRE(notification_calls == 2);
        }

        SECTION("notify() without autorefresh") {
            r->set_auto_refresh(false);
            r->notify();
            REQUIRE(notification_calls == 2);
        }

        SECTION("refresh()") {
            r->refresh();
            REQUIRE(notification_calls == 2);
        }

        SECTION("begin_transaction()") {
            r->begin_transaction();
            REQUIRE(notification_calls == 2);
            r->cancel_transaction();
        }
    }

    SECTION("notifications for remote changes") {
        make_remote_change();
        coordinator->on_change();
        REQUIRE(notification_calls == 1);

        SECTION("notify()") {
            r->notify();
            REQUIRE(notification_calls == 2);
        }

        SECTION("notify() without autorefresh") {
            r->set_auto_refresh(false);
            r->notify();
            REQUIRE(notification_calls == 1);
            r->refresh();
            REQUIRE(notification_calls == 2);
        }

        SECTION("refresh()") {
            r->refresh();
            REQUIRE(notification_calls == 2);
        }

        SECTION("begin_transaction()") {
            r->begin_transaction();
            REQUIRE(notification_calls == 2);
            r->cancel_transaction();
        }
    }

    SECTION("notifications are not delivered when the token is destroyed before they are calculated") {
        make_remote_change();
        REQUIRE(notification_calls == 1);
        token = {};
        advance_and_notify(*r);
        REQUIRE(notification_calls == 1);
    }

    SECTION("notifications are not delivered when the token is destroyed before they are delivered") {
        make_remote_change();
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

    SECTION("the first call of a notification can include changes if it previously ran for a different callback") {
        r->begin_transaction();
        auto token2 = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
            REQUIRE(!c.empty());
        });

        table->set_int(0, table->add_empty_row(), 5);
        r->commit_transaction();
        advance_and_notify(*r);
    }

    SECTION("handling of results not ready") {
        make_remote_change();

        SECTION("notify() does nothing") {
            r->notify();
            REQUIRE(notification_calls == 1);
            coordinator->on_change();
            r->notify();
            REQUIRE(notification_calls == 2);
        }

        SECTION("refresh() blocks") {
            REQUIRE(notification_calls == 1);
            joining_thread thread([&] {
                usleep(5000);
                coordinator->on_change();
            });
            r->refresh();
            REQUIRE(notification_calls == 2);
        }

        SECTION("refresh() advances to the first version with notifiers ready that is at least a recent as the newest at the time it is called") {
            joining_thread thread([&] {
                usleep(5000);
                make_remote_change();
                coordinator->on_change();
                make_remote_change();
            });
            // advances to the version after the one it was waiting for, but still
            // not the latest
            r->refresh();
            REQUIRE(notification_calls == 2);

            thread.join();
            REQUIRE(notification_calls == 2);

            // now advances to the latest
            coordinator->on_change();
            r->refresh();
            REQUIRE(notification_calls == 3);
        }

        SECTION("begin_transaction() blocks") {
            REQUIRE(notification_calls == 1);
            joining_thread thread([&] {
                usleep(5000);
                coordinator->on_change();
            });
            r->begin_transaction();
            REQUIRE(notification_calls == 2);
            r->cancel_transaction();
        }

        SECTION("refresh() does not block for results without callbacks") {
            token = {};
            // this would deadlock if it waits for the notifier to be ready
            r->refresh();
        }

        SECTION("begin_transaction() does not block for results without callbacks") {
            token = {};
            // this would deadlock if it waits for the notifier to be ready
            r->begin_transaction();
            r->cancel_transaction();
        }

        SECTION("begin_transaction() does not block for Results for different Realms") {
            // this would deadlock if beginning the write on the secondary Realm
            // waited for the primary Realm to be ready
            make_remote_change();

            // sanity check that the notifications never did run
            r->notify();
            REQUIRE(notification_calls == 1);
        }
    }

    SECTION("handling of stale results") {
        make_remote_change();
        coordinator->on_change();
        make_remote_change();

        SECTION("notify() uses the older version") {
            r->notify();
            REQUIRE(notification_calls == 2);
            coordinator->on_change();
            r->notify();
            REQUIRE(notification_calls == 3);
            r->notify();
            REQUIRE(notification_calls == 3);
        }

        SECTION("refresh() blocks") {
            REQUIRE(notification_calls == 1);
            joining_thread thread([&] {
                usleep(5000);
                coordinator->on_change();
            });
            r->refresh();
            REQUIRE(notification_calls == 2);
        }

        SECTION("begin_transaction() blocks") {
            REQUIRE(notification_calls == 1);
            joining_thread thread([&] {
                usleep(5000);
                coordinator->on_change();
            });
            r->begin_transaction();
            REQUIRE(notification_calls == 2);
            r->cancel_transaction();
        }
    }

    SECTION("updates are delivered after invalidate()") {
        r->invalidate();
        make_remote_change();

        SECTION("notify()") {
            coordinator->on_change();
            REQUIRE_FALSE(r->is_in_read_transaction());
            r->notify();
            REQUIRE(notification_calls == 2);
        }

        SECTION("notify() without autorefresh") {
            r->set_auto_refresh(false);
            coordinator->on_change();
            REQUIRE_FALSE(r->is_in_read_transaction());
            r->notify();
            REQUIRE(notification_calls == 1);
            r->refresh();
            REQUIRE(notification_calls == 2);
        }

        SECTION("refresh()") {
            coordinator->on_change();
            REQUIRE_FALSE(r->is_in_read_transaction());
            r->refresh();
            REQUIRE(notification_calls == 2);
        }

        SECTION("begin_transaction()") {
            coordinator->on_change();
            REQUIRE_FALSE(r->is_in_read_transaction());
            r->begin_transaction();
            REQUIRE(notification_calls == 2);
            r->cancel_transaction();
        }
    }

    SECTION("refresh() from within a notification is a no-op") {
        token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            REQUIRE_FALSE(r->refresh()); // would deadlock if it actually tried to refresh
        });
        advance_and_notify(*r);
        make_remote_change(); // 1
        coordinator->on_change();
        make_remote_change(); // 2
        r->notify(); // advances to version from 1
        coordinator->on_change();
        REQUIRE(r->refresh()); // advances to version from 2
        REQUIRE_FALSE(r->refresh()); // does not advance since it's now up-to-date
    }

    SECTION("begin_transaction() from within a notification does not send notifications immediately") {
        bool first = true;
        auto token2 = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            if (first)
                first = false;
            else {
                // would deadlock if it tried to send notifications as they aren't ready yet
                r->begin_transaction();
                r->cancel_transaction();
            }
        });
        advance_and_notify(*r);

        make_remote_change(); // 1
        coordinator->on_change();
        make_remote_change(); // 2
        r->notify(); // advances to version from 1
        REQUIRE(notification_calls == 2);
        coordinator->on_change();
        REQUIRE_FALSE(r->refresh()); // we made the commit locally, so no advancing here
        REQUIRE(notification_calls == 3);
    }

    SECTION("is_in_transaction() is reported correctly within a notification from begin_transaction() and changes can be made") {
        bool first = true;
        token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            if (first) {
                REQUIRE_FALSE(r->is_in_transaction());
                first = false;
            }
            else {
                REQUIRE(r->is_in_transaction());
                table->set_int(0, 0, 100);
            }
        });
        advance_and_notify(*r);
        make_remote_change();
        coordinator->on_change();
        r->begin_transaction();
        REQUIRE(table->get_int(0, 0) == 100);
        r->cancel_transaction();
        REQUIRE(table->get_int(0, 0) != 100);
    }

    SECTION("invalidate() from within notification ends the write transaction started by begin_transaction()") {
        token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            r->invalidate();
        });
        advance_and_notify(*r);
        REQUIRE_FALSE(r->is_in_read_transaction());
        make_remote_change();
        coordinator->on_change();
        r->begin_transaction();
        REQUIRE_FALSE(r->is_in_transaction());
    }

    SECTION("cancel_transaction() from within notification ends the write transaction started by begin_transaction()") {
        token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            if (r->is_in_transaction())
                r->cancel_transaction();
        });
        advance_and_notify(*r);
        make_remote_change();
        coordinator->on_change();
        r->begin_transaction();
        REQUIRE_FALSE(r->is_in_transaction());
    }
}

#if REALM_PLATFORM_APPLE
TEST_CASE("notifications: async error handling") {
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

    auto r2 = Realm::get_shared_realm(config);

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

        bool called = false;
        auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE(err);
            REQUIRE_FALSE(called);
            called = true;
        });
        REQUIRE(!called);

        SECTION("error is delivered on notify() without changes") {
            coordinator->on_change();
            REQUIRE(!called);
            r->notify();
            REQUIRE(called);
        }

        SECTION("error is delivered on notify() with changes") {
            r2->begin_transaction(); r2->commit_transaction();
            REQUIRE(!called);
            coordinator->on_change();
            REQUIRE(!called);
            r->notify();
            REQUIRE(called);
        }

        SECTION("error is delivered on refresh() without changes") {
            coordinator->on_change();
            REQUIRE(!called);
            r->refresh();
            REQUIRE(called);
        }

        SECTION("error is delivered on refresh() with changes") {
            r2->begin_transaction(); r2->commit_transaction();
            REQUIRE(!called);
            coordinator->on_change();
            REQUIRE(!called);
            r->refresh();
            REQUIRE(called);
        }

        SECTION("error is delivered on begin_transaction() without changes") {
            coordinator->on_change();
            REQUIRE(!called);
            r->begin_transaction();
            REQUIRE(called);
            r->cancel_transaction();
        }

        SECTION("error is delivered on begin_transaction() with changes") {
            r2->begin_transaction(); r2->commit_transaction();
            REQUIRE(!called);
            coordinator->on_change();
            REQUIRE(!called);
            r->begin_transaction();
            REQUIRE(called);
            r->cancel_transaction();
        }

        SECTION("adding another callback does not send the error again") {
            advance_and_notify(*r);
            REQUIRE(called);

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

TEST_CASE("notifications: results") {
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

    auto r2 = coordinator->get_realm();
    auto r2_table = r2->read_group().get_table("class_object");

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
            r2->begin_transaction();
            r2_table->set_int(0, 0, 6);
            r2->commit_transaction();

            coordinator->on_change();

            r2->begin_transaction();
            r2_table->set_int(0, 1, 0);
            r2->commit_transaction();

            REQUIRE(notification_calls == 1);
            coordinator->on_change();
            r->notify();
            REQUIRE(notification_calls == 2);
        }

        SECTION("inserting a row then modifying it in a second transaction does not report it as modified") {
            r2->begin_transaction();
            size_t ndx = r2_table->add_empty_row();
            r2_table->set_int(0, ndx, 6);
            r2->commit_transaction();

            coordinator->on_change();

            r2->begin_transaction();
            r2_table->set_int(0, ndx, 7);
            r2->commit_transaction();

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
            r2->begin_transaction();
            size_t ndx = r2_table->add_empty_row();
            r2_table->set_int(0, ndx, 5);
            r2->commit_transaction();

            coordinator->on_change();

            r2->begin_transaction();
            r2_table->move_last_over(ndx);
            r2->commit_transaction();

            REQUIRE(notification_calls == 1);
            coordinator->on_change();
            r->notify();
            REQUIRE(notification_calls == 1);
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
            r2->begin_transaction();
            func(*r2_table);
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
            r2->begin_transaction();
            r2_table->set_int(0, 0, 5);
            r2->commit_transaction();

            r2->begin_transaction();
            r2_table->set_int(0, 1, 0);
            r2->commit_transaction();

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
