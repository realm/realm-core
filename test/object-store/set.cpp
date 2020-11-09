#include "catch2/catch.hpp"

#include "util/test_file.hpp"
#include "util/index_helpers.hpp"

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/set.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm/version.hpp>
#include <realm/db.hpp>

using namespace realm;

TEST_CASE("set")
{
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {
            "table",
            {
                {"int_set", PropertyType::Set | PropertyType::Int},
                {"link_set", PropertyType::Set | PropertyType::Object, "table2"},
            },

        },
        {
            "table2",
            {
                {"id", PropertyType::Int, Property::IsPrimary{true}},
            },
        },
    });

    auto& coordinator = *_impl::RealmCoordinator::get_coordinator(config.path);
    static_cast<void>(coordinator);

    auto table = r->read_group().get_table("class_table");
    auto table2 = r->read_group().get_table("class_table2");
    ColKey col_int_set = table->get_column_key("int_set");
    ColKey col_link_set = table->get_column_key("link_set");

    auto write = [&](auto&& f) {
        r->begin_transaction();
        if constexpr (std::is_void_v<decltype(f())>) {
            f();
            r->commit_transaction();
            advance_and_notify(*r);
        }
        else {
            auto result = f();
            r->commit_transaction();
            advance_and_notify(*r);
            return result;
        }
    };

    auto obj = write([&] {
        return table->create_object();
    });

    SECTION("basics")
    {
        object_store::Set set{r, obj, col_int_set};

        write([&]() {
            CHECK(set.insert(123).second);
            CHECK(set.insert(456).second);
            CHECK(set.insert(0).second);
            CHECK(set.insert(-1).second);
            CHECK(!set.insert(456).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 4);
        CHECK(set.find(-1) == 0);
        CHECK(set.find(0) == 1);
        CHECK(set.find(123) == 2);
        CHECK(set.find(456) == 3);
        CHECK(set.find(999) == size_t(-1));

        write([&]() {
            CHECK(set.remove(123).second);
            CHECK(!set.remove(123).second);
            CHECK(set.remove(-1).second);
        });

        CHECK(set.size() == 2);

        write([&]() {
            obj.remove();
        });
        CHECK(!set.is_valid());
    }

    SECTION("objects / links")
    {
        object_store::Set set{r, obj, col_link_set};

        Obj target1, target2, target3;
        write([&]() {
            target1 = table2->create_object_with_primary_key(123);
            target2 = table2->create_object_with_primary_key(456);
            target3 = table2->create_object_with_primary_key(789);
        });

        write([&]() {
            CHECK(set.insert(target1).second);
            CHECK(!set.insert(target1).second);
            CHECK(set.insert(target2).second);
            CHECK(set.insert(target3).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 3);

        CHECK(set.find(target1) != size_t(-1));
        CHECK(set.find(target2) != size_t(-1));
        CHECK(set.find(target3) != size_t(-1));

        write([&]() {
            target2.invalidate();
        });

        // Invalidating the object changes the reported size of the set.
        CHECK(set.size() == 2);

        CHECK_THROWS(set.find(target2));

        // Resurrect the tombstone of target2.
        write([&]() {
            target2 = table2->create_object_with_primary_key(456);
        });
        CHECK(set.find(target2));
        CHECK(set.size() == 3);
    }

    SECTION("max / min / sum / avg") {
        object_store::Set set{r, obj, col_int_set};

        write([&]() {
            CHECK(set.insert(123).second);
            CHECK(set.insert(456).second);
            CHECK(set.insert(0).second);
            CHECK(set.insert(-1).second);
        });

        auto x = set.min();

        REQUIRE(set.is_valid());
        CHECK(set.sum(col_int_set) == 578);
        CHECK(set.min(col_int_set) == -1);
        CHECK(set.max(col_int_set) == 456);
        CHECK(set.average(col_int_set) == 144.5);
    }

    SECTION("add_notification_block()")
    {
        CollectionChangeSet change;
        object_store::Set link_set{r, obj, col_link_set};
        object_store::Set int_set{r, obj, col_int_set};

        auto require_change = [&] {
            auto token =
                link_set.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                    change = c;
                });
            advance_and_notify(*r);
            return token;
        };

        auto require_no_change = [&] {
            bool first = true;
            auto token = link_set.add_notification_callback([&, first](CollectionChangeSet, std::exception_ptr) mutable {
                REQUIRE(first);
                first = false;
            });
            advance_and_notify(*r);
            return token;
        };

        SECTION("modifying the set sends change notifications")
        {
            Obj target1, target2, target3;
            write([&]() {
                target1 = table2->create_object_with_primary_key(123);
                target2 = table2->create_object_with_primary_key(456);
                target3 = table2->create_object_with_primary_key(789);
            });

            auto token = require_change();

            write([&]() {
                CHECK(link_set.insert(target1).second);
                CHECK(!link_set.insert(target1).second);
                CHECK(link_set.insert(target2).second);
                CHECK(link_set.insert(target3).second);
            });
            // current_version is not bumped
//            write([&]() {
//                CHECK(link_set.insert(target1).second);
//                CHECK(!link_set.insert(target1).second);
//                CHECK(link_set.insert(target2).second);
//                CHECK(link_set.insert(target3).second);
//            });

            write([&] {
                CHECK(link_set.size() == 3);
                REQUIRE(link_set.remove(target2).second);
            });
            CHECK(link_set.size() == 2);
            REQUIRE_INDICES(change.deletions, 1);
        }

        SECTION("modifying a different set doesn't send a change notification")
        {
            auto token = require_no_change();
            write([&] {
                CHECK(int_set.insert(123).second);
            });
        }

        SECTION("deleting the set sends change notification")
        {
            auto token = require_change();

            Obj target1, target2, target3;
            write([&]() {
                target1 = table2->create_object_with_primary_key(123);
                target2 = table2->create_object_with_primary_key(456);
                target3 = table2->create_object_with_primary_key(789);
            });

            write([&]() {
                CHECK(link_set.insert(target1).second);
                CHECK(!link_set.insert(target1).second);
                CHECK(link_set.insert(target2).second);
                CHECK(link_set.insert(target3).second);
            });

            write([&] { link_set.remove_all(); });
            REQUIRE_INDICES(change.deletions, 0, 1, 2);

            // Should not resend delete all notification after another commit
            change = {};
            write([&] { table->create_object(); });
            REQUIRE(change.empty());
        }
/*
        SECTION("deleting list before first run of notifier reports deletions")
        {
            auto token =
                lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) { change = c; });
            advance_and_notify(*r);
            write([&] { origin->begin()->remove(); });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        }

        SECTION("modifying one of the target rows sends a change notification")
        {
            auto token = require_change();
            write([&] { lst.get(5).set(col_value, 6); });
            REQUIRE_INDICES(change.modifications, 5);
        }

        SECTION("deleting a target row sends a change notification")
        {
            auto token = require_change();
            write([&] { target->remove_object(target_keys[5]); });
            REQUIRE_INDICES(change.deletions, 5);
        }

        SECTION("adding a row and then modifying the target row does not mark the row as modified")
        {
            auto token = require_change();
            write([&] {
                Obj obj = target->get_object(target_keys[5]);
                lst.add(obj);
                obj.set(col_value, 10);
            });
            REQUIRE_INDICES(change.insertions, 10);
            REQUIRE_INDICES(change.modifications, 5);
        }

        SECTION("modifying and then moving a row reports move/insert but not modification")
        {
            auto token = require_change();
            write([&] {
                target->get_object(target_keys[5]).set(col_value, 10);
                lst.move(5, 8);
            });
            REQUIRE_INDICES(change.insertions, 8);
            REQUIRE_INDICES(change.deletions, 5);
            REQUIRE_MOVES(change, {5, 8});
            REQUIRE(change.modifications.empty());
        }

        SECTION("modifying a row which appears multiple times in a list marks them all as modified")
        {
            r->begin_transaction();
            lst.add(target_keys[5]);
            r->commit_transaction();

            auto token = require_change();
            write([&] { target->get_object(target_keys[5]).set(col_value, 10); });
            REQUIRE_INDICES(change.modifications, 5, 10);
        }

        SECTION("deleting a row which appears multiple times in a list marks them all as modified")
        {
            r->begin_transaction();
            lst.add(target_keys[5]);
            r->commit_transaction();

            auto token = require_change();
            write([&] { target->remove_object(target_keys[5]); });
            REQUIRE_INDICES(change.deletions, 5, 10);
        }

        SECTION("clearing the target table sends a change notification")
        {
            auto token = require_change();
            write([&] { target->clear(); });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        }

        SECTION("moving a target row does not send a change notification")
        {
            // Remove a row from the LV so that we have one to delete that's not in the list
            r->begin_transaction();
            if (lv->size() > 2)
                lv->remove(2);
            r->commit_transaction();

            auto token = require_no_change();
            write([&] { target->remove_object(target_keys[2]); });
        }

        SECTION("multiple LinkViews for the same LinkList can get notifications")
        {
            r->begin_transaction();
            target->clear();
            std::vector<ObjKey> keys;
            target->create_objects(5, keys);
            r->commit_transaction();

            auto get_list = [&] {
                auto r = Realm::get_shared_realm(config);
                auto obj = r->read_group().get_table("class_origin")->get_object(0);
                return List(r, obj, col_link);
            };
            auto change_list = [&] {
                r->begin_transaction();
                if (lv->size()) {
                    target->get_object(lv->size() - 1).set(col_value, int64_t(lv->size()));
                }
                lv->add(keys[lv->size()]);
                r->commit_transaction();
            };

            List lists[3];
            NotificationToken tokens[3];
            CollectionChangeSet changes[3];

            for (int i = 0; i < 3; ++i) {
                lists[i] = get_list();
                tokens[i] = lists[i].add_notification_callback(
                    [i, &changes](CollectionChangeSet c, std::exception_ptr) { changes[i] = std::move(c); });
                change_list();
            }

            // Each of the Lists now has a different source version and state at
            // that version, so they should all see different changes despite
            // being for the same LinkList
            for (auto& list : lists)
                advance_and_notify(*list.get_realm());

            REQUIRE_INDICES(changes[0].insertions, 0, 1, 2);
            REQUIRE(changes[0].modifications.empty());

            REQUIRE_INDICES(changes[1].insertions, 1, 2);
            REQUIRE_INDICES(changes[1].modifications, 0);

            REQUIRE_INDICES(changes[2].insertions, 2);
            REQUIRE_INDICES(changes[2].modifications, 1);

            // After making another change, they should all get the same notification
            change_list();
            for (auto& list : lists)
                advance_and_notify(*list.get_realm());

            for (int i = 0; i < 3; ++i) {
                REQUIRE_INDICES(changes[i].insertions, 3);
                REQUIRE_INDICES(changes[i].modifications, 2);
            }
        }*/
    }
}
