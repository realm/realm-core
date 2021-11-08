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

#include <catch2/catch.hpp>

#include "util/test_file.hpp"
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
using util::any_cast;

TEST_CASE("list") {
    InMemoryTestFile config;
    config.cache = false;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"origin", {{"array", PropertyType::Array | PropertyType::Object, "target"}}},
        {"target", {{"value", PropertyType::Int}, {"value2", PropertyType::Int}}},
        {"other_origin", {{"array", PropertyType::Array | PropertyType::Object, "other_target"}}},
        {"other_target", {{"value", PropertyType::Int}}},
    });

    auto& coordinator = *_impl::RealmCoordinator::get_coordinator(config.path);

    auto origin = r->read_group().get_table("class_origin");
    auto target = r->read_group().get_table("class_target");
    auto other_origin = r->read_group().get_table("class_other_origin");
    auto other_target = r->read_group().get_table("class_other_target");
    ColKey col_link = origin->get_column_key("array");
    ColKey col_target_value = target->get_column_key("value");
    ColKey other_col_link = other_origin->get_column_key("array");
    ColKey other_col_value = other_target->get_column_key("value");

    r->begin_transaction();

    std::vector<ObjKey> target_keys;
    target->create_objects(10, target_keys);
    for (int i = 0; i < 10; ++i)
        target->get_object(target_keys[i]).set_all(i);

    Obj obj = origin->create_object();
    auto lv = obj.get_linklist_ptr(col_link);
    for (int i = 0; i < 10; ++i)
        lv->add(target_keys[i]);
    auto lv2 = origin->create_object().get_linklist_ptr(col_link);
    for (int i = 0; i < 10; ++i)
        lv2->add(target_keys[i]);

    ObjKeys other_target_keys({3, 5, 7, 9, 11, 13, 15, 17, 19, 21});
    other_target->create_objects(other_target_keys);
    for (int i = 0; i < 10; ++i)
        other_target->get_object(other_target_keys[i]).set_all(i);

    Obj other_obj = other_origin->create_object();
    auto other_lv = other_obj.get_linklist_ptr(other_col_link);
    for (int i = 0; i < 10; ++i)
        other_lv->add(other_target_keys[i]);

    r->commit_transaction();

    auto r2 = coordinator.get_realm();
    auto r2_lv = r2->read_group().get_table("class_origin")->get_object(0).get_linklist_ptr(col_link);

    auto write = [&](auto&& f) {
        r->begin_transaction();
        f();
        r->commit_transaction();
        advance_and_notify(*r);
    };

    SECTION("add_notification_block()") {
        CollectionChangeSet change;
        List lst(r, obj, col_link);

        auto require_change = [&] {
            auto token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            advance_and_notify(*r);
            return token;
        };

        auto require_no_change = [&] {
            bool first = true;
            auto token = lst.add_notification_callback([&, first](CollectionChangeSet, std::exception_ptr) mutable {
                REQUIRE(first);
                first = false;
            });
            advance_and_notify(*r);
            return token;
        };

        SECTION("modifying the list sends a change notifications") {
            auto token = require_change();
            write([&] {
                if (lv2->size() > 5)
                    lst.remove(5);
            });
            REQUIRE_INDICES(change.deletions, 5);
        }

        SECTION("modifying a different list doesn't send a change notification") {
            auto token = require_no_change();
            write([&] {
                if (lv2->size() > 5)
                    lv2->remove(5);
            });
        }

        SECTION("deleting the list sends a change notification") {
            auto token = require_change();
            write([&] {
                obj.remove();
            });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // Should not resend delete all notification after another commit
            change = {};
            write([&] {
                target->create_object();
            });
            REQUIRE(change.empty());
        }

        SECTION("deleting list before first run of notifier reports deletions") {
            auto token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            advance_and_notify(*r);
            write([&] {
                origin->begin()->remove();
            });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            REQUIRE(change.collection_root_was_deleted);
        }

        SECTION("deleting an empty list triggers the notifier") {
            size_t notifier_count = 0;
            auto token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
                ++notifier_count;
            });
            advance_and_notify(*r);
            write([&] {
                lst.delete_all();
            });
            REQUIRE(!change.collection_root_was_deleted);
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            REQUIRE(notifier_count == 2);
            REQUIRE(lst.size() == 0);

            write([&] {
                origin->begin()->remove();
            });
            REQUIRE(change.deletions.count() == 0);
            REQUIRE(change.collection_root_was_deleted);
            REQUIRE(notifier_count == 3);

            // Should not resend delete notification after another commit
            change = {};
            write([&] {
                target->create_object();
            });
            REQUIRE(change.empty());
        }

        SECTION("modifying one of the target rows sends a change notification") {
            auto token = require_change();
            write([&] {
                lst.get(5).set(col_target_value, 6);
            });
            REQUIRE_INDICES(change.modifications, 5);
        }

        SECTION("deleting a target row sends a change notification") {
            auto token = require_change();
            write([&] {
                target->remove_object(target_keys[5]);
            });
            REQUIRE_INDICES(change.deletions, 5);
        }

        SECTION("adding a row and then modifying the target row does not mark the row as modified") {
            auto token = require_change();
            write([&] {
                Obj obj = target->get_object(target_keys[5]);
                lst.add(obj);
                obj.set(col_target_value, 10);
            });
            REQUIRE_INDICES(change.insertions, 10);
            REQUIRE_INDICES(change.modifications, 5);
        }

        SECTION("modifying and then moving a row reports move/insert but not modification") {
            auto token = require_change();
            write([&] {
                target->get_object(target_keys[5]).set(col_target_value, 10);
                lst.move(5, 8);
            });
            REQUIRE_INDICES(change.insertions, 8);
            REQUIRE_INDICES(change.deletions, 5);
            REQUIRE_MOVES(change, {5, 8});
            REQUIRE(change.modifications.empty());
        }

        SECTION("modifying a row which appears multiple times in a list marks them all as modified") {
            r->begin_transaction();
            lst.add(target_keys[5]);
            r->commit_transaction();

            auto token = require_change();
            write([&] {
                target->get_object(target_keys[5]).set(col_target_value, 10);
            });
            REQUIRE_INDICES(change.modifications, 5, 10);
        }

        SECTION("deleting a row which appears multiple times in a list marks them all as modified") {
            r->begin_transaction();
            lst.add(target_keys[5]);
            r->commit_transaction();

            auto token = require_change();
            write([&] {
                target->remove_object(target_keys[5]);
            });
            REQUIRE_INDICES(change.deletions, 5, 10);
        }

        SECTION("clearing the target table sends a change notification") {
            auto token = require_change();
            write([&] {
                target->clear();
            });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        }

        SECTION("moving a target row does not send a change notification") {
            // Remove a row from the LV so that we have one to delete that's not in the list
            r->begin_transaction();
            if (lv->size() > 2)
                lv->remove(2);
            r->commit_transaction();

            auto token = require_no_change();
            write([&] {
                target->remove_object(target_keys[2]);
            });
        }

        SECTION("multiple LinkViews for the same LinkList can get notifications") {
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
                    target->get_object(lv->size() - 1).set(col_target_value, int64_t(lv->size()));
                }
                lv->add(keys[lv->size()]);
                r->commit_transaction();
            };

            List lists[3];
            NotificationToken tokens[3];
            CollectionChangeSet changes[3];

            for (int i = 0; i < 3; ++i) {
                lists[i] = get_list();
                tokens[i] =
                    lists[i].add_notification_callback([i, &changes](CollectionChangeSet c, std::exception_ptr) {
                        changes[i] = std::move(c);
                    });
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
        }

        SECTION("multiple callbacks for the same Lists can be skipped individually") {
            auto token = require_no_change();
            auto token2 = require_change();

            r->begin_transaction();
            lv->add(target_keys[0]);
            token.suppress_next();
            r->commit_transaction();

            advance_and_notify(*r);
            REQUIRE_INDICES(change.insertions, 10);
        }

        SECTION("multiple Lists for the same LinkView can be skipped individually") {
            auto token = require_no_change();

            List list2(r, obj, col_link);
            auto token2 = list2.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            advance_and_notify(*r);

            r->begin_transaction();
            lv->add(target_keys[0]);
            token.suppress_next();
            r->commit_transaction();

            advance_and_notify(*r);
            REQUIRE_INDICES(change.insertions, 10);
        }

        SECTION("skipping only effects the current transaction even if no notification would occur anyway") {
            auto token = require_change();

            // would not produce a notification even if it wasn't skipped because no changes were made
            r->begin_transaction();
            token.suppress_next();
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE(change.empty());

            // should now produce a notification
            r->begin_transaction();
            lv->add(target_keys[0]);
            r->commit_transaction();
            advance_and_notify(*r);
            REQUIRE_INDICES(change.insertions, 10);
        }

        SECTION("modifying a different table does not send a change notification") {
            auto token = require_no_change();
            write([&] {
                other_lv->add(other_target_keys[0]);
            });
        }

        SECTION("changes are reported correctly for multiple tables") {
            List list2(r, *other_lv);
            CollectionChangeSet other_changes;
            auto token1 = list2.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                other_changes = std::move(c);
            });
            auto token2 = require_change();

            write([&] {
                lv->add(target_keys[1]);

                other_origin->create_object();
                if (other_lv->size() > 0)
                    other_lv->insert(1, other_target_keys[0]);

                lv->add(target_keys[2]);
            });
            REQUIRE_INDICES(change.insertions, 10, 11);
            REQUIRE_INDICES(other_changes.insertions, 1);

            write([&] {
                lv->add(target_keys[3]);
                other_obj.remove();
                lv->add(target_keys[4]);
            });
            REQUIRE_INDICES(change.insertions, 12, 13);
            REQUIRE_INDICES(other_changes.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

            write([&] {
                lv->add(target_keys[5]);
                other_origin->clear();
                lv->add(target_keys[6]);
            });
            REQUIRE_INDICES(change.insertions, 14, 15);
        }

        SECTION("tables-of-interest are tracked properly for multiple source versions") {
            // Add notifiers for different tables at different versions to verify
            // that the tables of interest are updated correctly as we process
            // new notifiers
            CollectionChangeSet changes1, changes2;
            auto token1 = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                changes1 = std::move(c);
            });

            r2->begin_transaction();
            r2->read_group().get_table("class_target")->get_object(target_keys[0]).set(col_target_value, 10);
            r2->read_group()
                .get_table("class_other_target")
                ->get_object(other_target_keys[1])
                .set(other_col_value, 10);
            r2->commit_transaction();

            List list2(r2, r2->read_group().get_table("class_other_origin")->get_object(0), other_col_link);
            auto token2 = list2.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                changes2 = std::move(c);
            });

            auto r3 = coordinator.get_realm();
            r3->begin_transaction();
            r3->read_group().get_table("class_target")->get_object(target_keys[2]).set(col_target_value, 10);
            r3->read_group()
                .get_table("class_other_target")
                ->get_object(other_target_keys[3])
                .set(other_col_value, 10);
            r3->commit_transaction();

            advance_and_notify(*r);
            advance_and_notify(*r2);

            REQUIRE_INDICES(changes1.modifications, 0, 2);
            REQUIRE_INDICES(changes2.modifications, 3);
        }

        SECTION("modifications are reported for rows that are moved and then moved back in a second transaction") {
            auto token = require_change();

            r2->begin_transaction();
            r2_lv->get_object(5).set(col_target_value, 10);
            r2_lv->get_object(1).set(col_target_value, 10);
            r2_lv->move(5, 8);
            r2_lv->move(1, 2);
            r2->commit_transaction();

            coordinator.on_change();

            r2->begin_transaction();
            if (r2_lv->size() > 8)
                r2_lv->move(8, 5);
            r2->commit_transaction();
            advance_and_notify(*r);

            REQUIRE_INDICES(change.deletions, 1);
            REQUIRE_INDICES(change.insertions, 2);
            REQUIRE_INDICES(change.modifications, 5);
            REQUIRE_MOVES(change, {1, 2});
        }

        SECTION("changes are sent in initial notification") {
            auto token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            r2->begin_transaction();
            r2_lv->remove(5);
            r2->commit_transaction();
            advance_and_notify(*r);
            REQUIRE_INDICES(change.deletions, 5);
        }

        SECTION("changes are sent in initial notification after removing and then re-adding callback") {
            auto token = lst.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
                REQUIRE(false);
            });
            token = {};

            auto write = [&] {
                r2->begin_transaction();
                r2_lv->remove(5);
                r2->commit_transaction();
            };

            SECTION("add new callback before transaction") {
                token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                    change = c;
                });

                write();

                advance_and_notify(*r);
                REQUIRE_INDICES(change.deletions, 5);
            }

            SECTION("add new callback after transaction") {
                write();

                token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                    change = c;
                });

                advance_and_notify(*r);
                REQUIRE_INDICES(change.deletions, 5);
            }

            SECTION("add new callback after transaction and after changeset was calculated") {
                write();
                coordinator.on_change();

                token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                    change = c;
                });

                advance_and_notify(*r);
                REQUIRE_INDICES(change.deletions, 5);
            }
        }
    }

    SECTION("sorted add_notification_block()") {
        List lst(r, *lv);
        Results results = lst.sort({{{col_target_value}}, {false}});

        int notification_calls = 0;
        CollectionChangeSet change;
        auto token = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            change = c;
            ++notification_calls;
        });

        advance_and_notify(*r);

        SECTION("add duplicates") {
            write([&] {
                lst.add(target_keys[5]);
                lst.add(target_keys[5]);
                lst.add(target_keys[5]);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.insertions, 5, 6, 7);
        }

        SECTION("change order by modifying target") {
            write([&] {
                lst.get(5).set(col_target_value, 15);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 4);
            REQUIRE_INDICES(change.insertions, 0);
        }

        SECTION("swap") {
            write([&] {
                lst.swap(1, 2);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("move") {
            write([&] {
                lst.move(5, 3);
            });
            REQUIRE(notification_calls == 1);
        }
    }

    SECTION("filtered add_notification_block()") {
        List lst(r, *lv);
        Results results = lst.filter(target->where().less(col_target_value, 9));

        int notification_calls = 0;
        CollectionChangeSet change;
        auto token = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            change = c;
            ++notification_calls;
        });

        advance_and_notify(*r);

        SECTION("add duplicates") {
            write([&] {
                lst.add(target_keys[5]);
                lst.add(target_keys[5]);
                lst.add(target_keys[5]);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.insertions, 9, 10, 11);
        }

        SECTION("swap") {
            write([&] {
                lst.swap(1, 2);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 2);
            REQUIRE_INDICES(change.insertions, 1);

            write([&] {
                lst.swap(5, 8);
            });
            REQUIRE(notification_calls == 3);
            REQUIRE_INDICES(change.deletions, 5, 8);
            REQUIRE_INDICES(change.insertions, 5, 8);
        }

        SECTION("move") {
            write([&] {
                lst.move(5, 3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 5);
            REQUIRE_INDICES(change.insertions, 3);
        }

        SECTION("move non-matching entry") {
            write([&] {
                lst.move(9, 3);
            });
            REQUIRE(notification_calls == 1);
        }
    }

    SECTION("Keypath filtered change notifications") {
        ColKey col_target_value2 = target->get_column_key("value2");
        List list(r, obj, col_link);

        // Creating KeyPathArrays:
        // 1. Property pairs
        std::pair<TableKey, ColKey> pair_origin_link(origin->get_key(), col_link);
        std::pair<TableKey, ColKey> pair_target_value(target->get_key(), col_target_value);
        std::pair<TableKey, ColKey> pair_target_value2(target->get_key(), col_target_value2);
        // 2. KeyPaths
        auto key_path_origin_link = {pair_origin_link};
        auto key_path_target_value = {pair_target_value};
        auto key_path_target_value2 = {pair_target_value2};
        // 3. Aggregated `KeyPathArray`
        KeyPathArray key_path_array_origin_to_target_value = {key_path_origin_link, key_path_target_value};
        KeyPathArray key_path_array_target_value = {key_path_target_value};
        KeyPathArray key_path_array_target_value2 = {key_path_target_value2};

        // For the keypath filtered notifications we need to check three scenarios:
        // - no callbacks have filters (this part is covered by other sections)
        // - some callbacks have filters
        // - all callbacks have filters
        CollectionChangeSet collection_change_set_without_filter;
        CollectionChangeSet collection_change_set_with_filter_on_target_value;

        // Note that in case not all callbacks have filters we do accept false positive notifications by design.
        // Distinguishing between these two cases would be a big change for little value.
        SECTION("some callbacks have filters") {
            auto require_change_no_filter = [&] {
                auto token = list.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr error) {
                    REQUIRE_FALSE(error);
                    collection_change_set_without_filter = c;
                });
                advance_and_notify(*r);
                return token;
            };

            auto require_change_target_value_filter = [&] {
                auto token = list.add_notification_callback(
                    [&](CollectionChangeSet c, std::exception_ptr error) {
                        REQUIRE_FALSE(error);
                        collection_change_set_with_filter_on_target_value = c;
                    },
                    key_path_array_target_value);
                advance_and_notify(*r);
                return token;
            };

            SECTION("modifying table 'target', property 'value' "
                    "-> DOES send a notification") {
                auto token1 = require_change_no_filter();
                auto token2 = require_change_target_value_filter();
                write([&] {
                    list.get(0).set(col_target_value, 42);
                });
                REQUIRE_INDICES(collection_change_set_without_filter.modifications, 0);
                REQUIRE_INDICES(collection_change_set_without_filter.modifications_new, 0);
                REQUIRE_INDICES(collection_change_set_with_filter_on_target_value.modifications, 0);
                REQUIRE_INDICES(collection_change_set_with_filter_on_target_value.modifications_new, 0);
            }

            SECTION("modifying table 'target', property 'value2' "
                    "-> DOES send a notification") {
                auto token1 = require_change_no_filter();
                auto token2 = require_change_target_value_filter();
                write([&] {
                    list.get(0).set(col_target_value2, 42);
                });
                REQUIRE_INDICES(collection_change_set_without_filter.modifications, 0);
                REQUIRE_INDICES(collection_change_set_without_filter.modifications_new, 0);
                REQUIRE_INDICES(collection_change_set_with_filter_on_target_value.modifications, 0);
                REQUIRE_INDICES(collection_change_set_with_filter_on_target_value.modifications_new, 0);
            }
        }

        // In case all callbacks do have filters we expect every callback to only get called when the corresponding
        // filter is hit. Compared to the above 'some callbacks have filters' case we do not expect false positives
        // here.
        SECTION("all callbacks have filters") {
            auto require_change = [&] {
                auto token = list.add_notification_callback(
                    [&](CollectionChangeSet c, std::exception_ptr error) {
                        REQUIRE_FALSE(error);
                        collection_change_set_with_filter_on_target_value = c;
                    },
                    key_path_array_target_value);
                advance_and_notify(*r);
                return token;
            };

            auto require_no_change = [&] {
                bool first = true;
                auto token = list.add_notification_callback(
                    [&, first](CollectionChangeSet, std::exception_ptr error) mutable {
                        REQUIRE_FALSE(error);
                        REQUIRE(first);
                        first = false;
                    },
                    key_path_array_target_value2);
                advance_and_notify(*r);
                return token;
            };

            SECTION("modifying table 'target', property 'value' "
                    "-> DOES send a notification for 'value'") {
                auto token = require_change();
                write([&] {
                    list.get(0).set(col_target_value, 42);
                });
                REQUIRE_INDICES(collection_change_set_with_filter_on_target_value.modifications, 0);
                REQUIRE_INDICES(collection_change_set_with_filter_on_target_value.modifications_new, 0);
            }

            SECTION("modifying table 'target', property 'value' "
                    "-> does NOT send a notification for 'value'") {
                auto token = require_no_change();
                write([&] {
                    list.get(0).set(col_target_value, 42);
                });
            }
        }

        SECTION("linked filter") {
            CollectionChangeSet collection_change_set_linked_filter;
            Object object(r, obj);

            auto require_change_origin_to_target = [&] {
                auto token = object.add_notification_callback(
                    [&](CollectionChangeSet c, std::exception_ptr error) {
                        REQUIRE_FALSE(error);
                        collection_change_set_linked_filter = c;
                    },
                    key_path_array_origin_to_target_value);
                advance_and_notify(*r);
                return token;
            };

            auto token = require_change_origin_to_target();

            write([&] {
                auto foo = obj.get_linklist(col_link);
                ObjKey obj_key = foo.get(0);
                TableRef target_table = foo.get_target_table();
                Obj target_object = target_table->get_object(obj_key);
                target_object.set(col_target_value, 42);
            });
            REQUIRE_INDICES(collection_change_set_linked_filter.modifications, 0);
            REQUIRE_INDICES(collection_change_set_linked_filter.modifications_new, 0);
        }
    }

    SECTION("sort()") {
        auto objectschema = &*r->schema().find("target");
        List list(r, *lv);
        auto results = list.sort({{{col_target_value}}, {false}});

        REQUIRE(&results.get_object_schema() == objectschema);
        REQUIRE(results.get_mode() == Results::Mode::Collection);
        REQUIRE(results.size() == 10);

        // Aggregates don't inherently have to convert to TableView, but do
        // because aggregates aren't implemented for Collection
        REQUIRE(results.sum(col_target_value) == 45);
        REQUIRE(results.get_mode() == Results::Mode::TableView);

        // Reset to Collection mode to test implicit conversion to TableView on get()
        results = list.sort({{{col_target_value}}, {false}});
        for (size_t i = 0; i < 10; ++i)
            REQUIRE(results.get(i).get_key() == target_keys[9 - i]);
        REQUIRE_THROWS_WITH(results.get(10), "Requested index 10 greater than max 9");
        REQUIRE(results.get_mode() == Results::Mode::TableView);

        // Zero sort columns should leave it in Collection mode
        results = list.sort(SortDescriptor());
        for (size_t i = 0; i < 10; ++i)
            REQUIRE(results.get(i).get_key() == target_keys[i]);
        REQUIRE_THROWS_WITH(results.get(10), "Requested index 10 greater than max 9");
        REQUIRE(results.get_mode() == Results::Mode::Collection);
    }

    SECTION("distinct()") {
        // Make it so that there's actually duplicate values in the target
        write([&] {
            for (int i = 0; i < 10; ++i)
                target->get_object(i).set_all(i / 2);
        });

        auto objectschema = &*r->schema().find("target");
        List list(r, *lv);
        auto results = list.as_results().distinct(DistinctDescriptor({{col_target_value}}));
        REQUIRE(&results.get_object_schema() == objectschema);
        REQUIRE(results.get_mode() == Results::Mode::Collection);

        SECTION("size()") {
            REQUIRE(results.size() == 5);
        }

        SECTION("aggregates") {
            REQUIRE(results.sum(col_target_value) == 10);
        }

        SECTION("get()") {
            for (size_t i = 0; i < 5; ++i)
                REQUIRE(results.get(i).get_key() == target_keys[i * 2]);
            REQUIRE_THROWS_WITH(results.get(5), "Requested index 5 greater than max 4");
            REQUIRE(results.get_mode() == Results::Mode::TableView);
        }

        SECTION("clear()") {
            REQUIRE(target->size() == 10);
            write([&] {
                results.clear();
            });
            REQUIRE(target->size() == 5);

            // After deleting the first object with each distinct value, the
            // results should now contain the second object with each distinct
            // value (which in this case means that the size hasn't changed)
            REQUIRE(results.size() == 5);
            for (size_t i = 0; i < 5; ++i)
                REQUIRE(results.get(i).get_key() == target_keys[(i + 1) * 2 - 1]);
        }

        SECTION("empty distinct descriptor does nothing") {
            results = list.as_results().distinct(DistinctDescriptor());
            for (size_t i = 0; i < 10; ++i)
                REQUIRE(results.get(i).get_key() == target_keys[i]);
            REQUIRE_THROWS_WITH(results.get(10), "Requested index 10 greater than max 9");
            REQUIRE(results.get_mode() == Results::Mode::Collection);
        }
    }

    SECTION("filter()") {
        auto objectschema = &*r->schema().find("target");
        List list(r, *lv);
        auto results = list.filter(target->where().greater(col_target_value, 5));

        REQUIRE(&results.get_object_schema() == objectschema);
        REQUIRE(results.get_mode() == Results::Mode::Query);
        REQUIRE(results.size() == 4);

        for (size_t i = 0; i < 4; ++i) {
            REQUIRE(results.get(i).get_key() == target_keys[i + 6]);
        }
    }

    SECTION("snapshot()") {
        auto objectschema = &*r->schema().find("target");
        List list(r, *lv);

        auto snapshot = list.snapshot();
        REQUIRE(&snapshot.get_object_schema() == objectschema);
        REQUIRE(snapshot.get_mode() == Results::Mode::TableView);
        REQUIRE(snapshot.size() == 10);

        r->begin_transaction();
        for (size_t i = 0; i < 5; ++i) {
            list.remove(0);
        }
        REQUIRE(snapshot.size() == 10);
        for (size_t i = 0; i < snapshot.size(); ++i) {
            REQUIRE(snapshot.get(i).is_valid());
        }
        for (size_t i = 0; i < 5; ++i) {
            target->remove_object(target_keys[i]);
        }
        REQUIRE(snapshot.size() == 10);
        for (size_t i = 0; i < 5; ++i) {
            REQUIRE(!snapshot.get(i).is_valid());
        }
        for (size_t i = 5; i < 10; ++i) {
            REQUIRE(snapshot.get(i).is_valid());
        }
        list.add(target_keys[5]);
        REQUIRE(snapshot.size() == 10);
    }

    SECTION("snapshot() after deletion") {
        List list(r, *lv);

        auto snapshot = list.snapshot();

        for (size_t i = 0; i < snapshot.size(); ++i) {
            r->begin_transaction();
            Obj obj = snapshot.get<Obj>(i);
            obj.remove();
            r->commit_transaction();
        }

        auto snapshot2 = list.snapshot();
        CHECK(snapshot2.size() == 0);
        CHECK(list.size() == 0);
    }

    SECTION("get_object_schema()") {
        List list(r, *lv);
        auto objectschema = &*r->schema().find("target");
        REQUIRE(&list.get_object_schema() == objectschema);
    }

    SECTION("delete_at()") {
        List list(r, *lv);
        r->begin_transaction();
        auto initial_view_size = lv->size();
        auto initial_target_size = target->size();
        list.delete_at(1);
        REQUIRE(lv->size() == initial_view_size - 1);
        REQUIRE(target->size() == initial_target_size - 1);
        r->cancel_transaction();
    }

    SECTION("delete_all()") {
        List list(r, *lv);
        r->begin_transaction();
        list.delete_all();
        REQUIRE(lv->size() == 0);
        REQUIRE(target->size() == 0);
        r->cancel_transaction();
    }

    SECTION("as_results().clear()") {
        List list(r, *lv);
        r->begin_transaction();
        list.as_results().clear();
        REQUIRE(lv->size() == 0);
        REQUIRE(target->size() == 0);
        r->cancel_transaction();
    }

    SECTION("snapshot().clear()") {
        List list(r, *lv);
        r->begin_transaction();
        auto snapshot = list.snapshot();
        snapshot.clear();
        REQUIRE(snapshot.size() == 10);
        REQUIRE(list.size() == 0);
        REQUIRE(lv->size() == 0);
        REQUIRE(target->size() == 0);
        r->cancel_transaction();
    }

    SECTION("add(RowExpr)") {
        List list(r, *lv);
        r->begin_transaction();
        SECTION("adds rows from the correct table") {
            list.add(target_keys[5]);
            REQUIRE(list.size() == 11);
            REQUIRE(list.get(10).get_key() == target_keys[5]);
        }

        SECTION("throws for rows from the wrong table") {
            REQUIRE_THROWS(list.add(obj));
        }
        r->cancel_transaction();
    }

    SECTION("insert(RowExpr)") {
        List list(r, *lv);
        r->begin_transaction();

        SECTION("insert rows from the correct table") {
            list.insert(0, target_keys[5]);
            REQUIRE(list.size() == 11);
            REQUIRE(list.get(0).get_key() == target_keys[5]);
        }

        SECTION("throws for rows from the wrong table") {
            REQUIRE_THROWS(list.insert(0, obj));
        }

        SECTION("throws for out of bounds insertions") {
            REQUIRE_THROWS(list.insert(11, target_keys[5]));
            REQUIRE_NOTHROW(list.insert(10, target_keys[5]));
        }
        r->cancel_transaction();
    }

    SECTION("set(RowExpr)") {
        List list(r, *lv);
        r->begin_transaction();

        SECTION("assigns for rows from the correct table") {
            list.set(0, target_keys[5]);
            REQUIRE(list.size() == 10);
            REQUIRE(list.get(0).get_key() == target_keys[5]);
        }

        SECTION("throws for rows from the wrong table") {
            REQUIRE_THROWS(list.set(0, obj));
        }

        SECTION("throws for out of bounds sets") {
            REQUIRE_THROWS(list.set(10, target_keys[5]));
        }
        r->cancel_transaction();
    }

    SECTION("find(RowExpr)") {
        List list(r, *lv);
        Obj obj1 = target->get_object(target_keys[1]);
        Obj obj5 = target->get_object(target_keys[5]);

        SECTION("returns index in list for values in the list") {
            REQUIRE(list.find(obj5) == 5);
        }

        SECTION("returns index in list and not index in table") {
            r->begin_transaction();
            list.remove(1);
            REQUIRE(list.find(obj5) == 4);
            REQUIRE(list.as_results().index_of(obj5) == 4);
            r->cancel_transaction();
        }

        SECTION("returns npos for values not in the list") {
            r->begin_transaction();
            list.remove(1);
            REQUIRE(list.find(obj1) == npos);
            REQUIRE(list.as_results().index_of(obj1) == npos);
            r->cancel_transaction();
        }

        SECTION("throws for row in wrong table") {
            REQUIRE_THROWS(list.find(obj));
            REQUIRE_THROWS(list.as_results().index_of(obj));
        }
    }

    SECTION("find(Query)") {
        List list(r, *lv);

        SECTION("returns index in list for values in the list") {
            REQUIRE(list.find(std::move(target->where().equal(col_target_value, 5))) == 5);
        }

        SECTION("returns index in list and not index in table") {
            r->begin_transaction();
            list.remove(1);
            REQUIRE(list.find(std::move(target->where().equal(col_target_value, 5))) == 4);
            r->cancel_transaction();
        }

        SECTION("returns npos for values not in the list") {
            REQUIRE(list.find(std::move(target->where().equal(col_target_value, 11))) == npos);
        }
    }

    SECTION("add(Context)") {
        List list(r, *lv);
        CppContext ctx(r, &list.get_object_schema());
        r->begin_transaction();

        SECTION("adds boxed RowExpr") {
            list.add(ctx, util::Any(target->get_object(target_keys[5])));
            REQUIRE(list.size() == 11);
            REQUIRE(list.get(10).get_key().value == 5);
        }

        SECTION("adds boxed realm::Object") {
            realm::Object obj(r, list.get_object_schema(), target->get_object(target_keys[5]));
            list.add(ctx, util::Any(obj));
            REQUIRE(list.size() == 11);
            REQUIRE(list.get(10).get_key() == target_keys[5]);
        }

        SECTION("creates new object for dictionary") {
            list.add(ctx, util::Any(AnyDict{{"value", INT64_C(20)}, {"value2", INT64_C(20)}}));
            REQUIRE(list.size() == 11);
            REQUIRE(target->size() == 11);
            REQUIRE(list.get(10).get<Int>(col_target_value) == 20);
        }

        SECTION("throws for object in wrong table") {
            REQUIRE_THROWS(list.add(ctx, util::Any(origin->get_object(0))));
            realm::Object object(r, *r->schema().find("origin"), origin->get_object(0));
            REQUIRE_THROWS(list.add(ctx, util::Any(object)));
        }

        r->cancel_transaction();
    }

    SECTION("find(Context)") {
        List list(r, *lv);
        CppContext ctx(r, &list.get_object_schema());

        SECTION("returns index in list for boxed RowExpr") {
            REQUIRE(list.find(ctx, util::Any(target->get_object(target_keys[5]))) == 5);
        }

        SECTION("returns index in list for boxed Object") {
            realm::Object obj(r, *r->schema().find("origin"), target->get_object(target_keys[5]));
            REQUIRE(list.find(ctx, util::Any(obj)) == 5);
        }

        SECTION("does not insert new objects for dictionaries") {
            REQUIRE(list.find(ctx, util::Any(AnyDict{{"value", INT64_C(20)}})) == npos);
            REQUIRE(target->size() == 10);
        }

        SECTION("throws for object in wrong table") {
            REQUIRE_THROWS(list.find(ctx, util::Any(obj)));
        }
    }

    SECTION("get(Context)") {
        List list(r, *lv);
        CppContext ctx(r, &list.get_object_schema());

        Object obj;
        REQUIRE_NOTHROW(obj = any_cast<Object&&>(list.get(ctx, 1)));
        REQUIRE(obj.is_valid());
        REQUIRE(obj.obj().get_key() == target_keys[1]);
    }
}

TEST_CASE("embedded List") {
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"origin",
         {{"pk", PropertyType::Int, Property::IsPrimary{true}},
          {"array", PropertyType::Array | PropertyType::Object, "target"}}},
        {"target", ObjectSchema::IsEmbedded{true}, {{"value", PropertyType::Int}}},
        {"other_origin", {{"array", PropertyType::Array | PropertyType::Object, "other_target"}}},
        {"other_target", ObjectSchema::IsEmbedded{true}, {{"value", PropertyType::Int}}},
    });

    auto& coordinator = *_impl::RealmCoordinator::get_coordinator(config.path);

    auto origin = r->read_group().get_table("class_origin");
    auto target = r->read_group().get_table("class_target");
    auto other_origin = r->read_group().get_table("class_other_origin");
    ColKey col_link = origin->get_column_key("array");
    ColKey col_value = target->get_column_key("value");
    ColKey other_col_link = other_origin->get_column_key("array");

    r->begin_transaction();

    Obj obj = origin->create_object_with_primary_key(0);
    auto lv = obj.get_linklist_ptr(col_link);
    for (int i = 0; i < 10; ++i)
        lv->create_and_insert_linked_object(i).set_all(i);
    auto lv2 = origin->create_object_with_primary_key(1).get_linklist_ptr(col_link);
    for (int i = 0; i < 10; ++i)
        lv2->create_and_insert_linked_object(i).set_all(i);


    Obj other_obj = other_origin->create_object();
    auto other_lv = other_obj.get_linklist_ptr(other_col_link);
    for (int i = 0; i < 10; ++i)
        other_lv->create_and_insert_linked_object(i).set_all(i);

    r->commit_transaction();
    lv->size();
    lv2->size();
    other_lv->size();

    auto r2 = coordinator.get_realm();
    auto r2_lv = r2->read_group().get_table("class_origin")->get_object(0).get_linklist_ptr(col_link);

    auto write = [&](auto&& f) {
        r->begin_transaction();
        f();
        r->commit_transaction();
        advance_and_notify(*r);
    };

    SECTION("add_notification_block()") {
        CollectionChangeSet change;
        List lst(r, obj, col_link);

        auto require_change = [&] {
            auto token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            advance_and_notify(*r);
            return token;
        };

        auto require_no_change = [&] {
            bool first = true;
            auto token = lst.add_notification_callback([&, first](CollectionChangeSet, std::exception_ptr) mutable {
                REQUIRE(first);
                first = false;
            });
            advance_and_notify(*r);
            return token;
        };

        SECTION("modifying the list sends a change notifications") {
            auto token = require_change();
            write([&] {
                lst.remove(5);
            });
            REQUIRE_INDICES(change.deletions, 5);
        }

        SECTION("modifying a different list doesn't send a change notification") {
            auto token = require_no_change();
            write([&] {
                lv2->remove(5);
            });
        }

        SECTION("deleting the list sends a change notification") {
            auto token = require_change();
            write([&] {
                obj.remove();
            });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // Should not resend delete all notification after another commit
            change = {};
            write([&] {
                lv2->size();
                lv2->create_and_insert_linked_object(0);
            });
            REQUIRE(change.empty());
        }

        SECTION("deleting list before first run of notifier reports deletions") {
            auto token = lst.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            advance_and_notify(*r);
            write([&] {
                origin->begin()->remove();
            });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        }

        SECTION("modifying one of the target rows sends a change notification") {
            auto token = require_change();
            write([&] {
                lst.get(5).set(col_value, 6);
            });
            REQUIRE_INDICES(change.modifications, 5);
        }

        SECTION("deleting a target row sends a change notification") {
            auto token = require_change();
            write([&] {
                target->remove_object(lv->get(5));
            });
            REQUIRE_INDICES(change.deletions, 5);
        }

        SECTION("modifying and then moving a row reports move/insert but not modification") {
            auto token = require_change();
            write([&] {
                target->get_object(lv->get(5)).set(col_value, 10);
                lst.move(5, 8);
            });
            REQUIRE_INDICES(change.insertions, 8);
            REQUIRE_INDICES(change.deletions, 5);
            REQUIRE_MOVES(change, {5, 8});
            REQUIRE(change.modifications.empty());
        }

        SECTION("clearing the target table sends a change notification") {
            auto token = require_change();
            write([&] {
                target->clear();
            });
            REQUIRE_INDICES(change.deletions, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        }
    }

    SECTION("sorted add_notification_block()") {
        List lst(r, *lv);
        Results results = lst.sort({{{col_value}}, {false}});

        int notification_calls = 0;
        CollectionChangeSet change;
        auto token = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            change = c;
            ++notification_calls;
        });

        advance_and_notify(*r);

        SECTION("change order by modifying target") {
            write([&] {
                lst.get(5).set(col_value, 15);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 4);
            REQUIRE_INDICES(change.insertions, 0);
        }

        SECTION("swap") {
            write([&] {
                lst.swap(1, 2);
            });
            REQUIRE(notification_calls == 1);
        }

        SECTION("move") {
            write([&] {
                lst.move(5, 3);
            });
            REQUIRE(notification_calls == 1);
        }
    }

    SECTION("filtered add_notification_block()") {
        List lst(r, *lv);
        Results results = lst.filter(target->where().less(col_value, 9));

        int notification_calls = 0;
        CollectionChangeSet change;
        auto token = results.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            change = c;
            ++notification_calls;
        });

        advance_and_notify(*r);

        SECTION("swap") {
            write([&] {
                lst.swap(1, 2);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 2);
            REQUIRE_INDICES(change.insertions, 1);

            write([&] {
                lst.swap(5, 8);
            });
            REQUIRE(notification_calls == 3);
            REQUIRE_INDICES(change.deletions, 5, 8);
            REQUIRE_INDICES(change.insertions, 5, 8);
        }

        SECTION("move") {
            write([&] {
                lst.move(5, 3);
            });
            REQUIRE(notification_calls == 2);
            REQUIRE_INDICES(change.deletions, 5);
            REQUIRE_INDICES(change.insertions, 3);
        }

        SECTION("move non-matching entry") {
            write([&] {
                lst.move(9, 3);
            });
            REQUIRE(notification_calls == 1);
        }
    }

    auto initial_view_size = lv->size();
    auto initial_target_size = target->size();
    SECTION("delete_at()") {
        List list(r, *lv);
        r->begin_transaction();
        list.delete_at(1);
        REQUIRE(lv->size() == initial_view_size - 1);
        REQUIRE(target->size() == initial_target_size - 1);
        r->cancel_transaction();
    }

    SECTION("delete_all()") {
        List list(r, *lv);
        r->begin_transaction();
        list.delete_all();
        REQUIRE(lv->size() == 0);
        REQUIRE(target->size() == initial_target_size - 10);
        r->cancel_transaction();
    }

    SECTION("as_results().clear()") {
        List list(r, *lv);
        r->begin_transaction();
        list.as_results().clear();
        REQUIRE(lv->size() == 0);
        REQUIRE(target->size() == initial_target_size - 10);
        r->cancel_transaction();
    }

    SECTION("snapshot().clear()") {
        List list(r, *lv);
        r->begin_transaction();
        auto snapshot = list.snapshot();
        snapshot.clear();
        REQUIRE(snapshot.size() == 10);
        REQUIRE(list.size() == 0);
        REQUIRE(lv->size() == 0);
        REQUIRE(target->size() == initial_target_size - 10);
        r->cancel_transaction();
    }

    SECTION("add(), insert(), and set() to existing object is not allowed") {
        List list(r, *lv);
        r->begin_transaction();
        REQUIRE_THROWS_AS(list.add(target->get_object(0)), List::InvalidEmbeddedOperationException);
        REQUIRE_THROWS_AS(list.insert(0, target->get_object(0)), List::InvalidEmbeddedOperationException);
        REQUIRE_THROWS_AS(list.set(0, target->get_object(0)), List::InvalidEmbeddedOperationException);
        r->cancel_transaction();
    }

    SECTION("find(RowExpr)") {
        List list(r, *lv);
        Obj obj1 = target->get_object(1);
        Obj obj5 = target->get_object(5);

        SECTION("returns index in list for values in the list") {
            REQUIRE(list.find(obj5) == 5);
        }

        SECTION("returns index in list and not index in table") {
            r->begin_transaction();
            list.remove(1);
            REQUIRE(list.find(obj5) == 4);
            REQUIRE(list.as_results().index_of(obj5) == 4);
            r->cancel_transaction();
        }

        SECTION("returns npos for values not in the list") {
            r->begin_transaction();
            list.remove(1);
            REQUIRE(list.find(obj1) == npos);
            REQUIRE_THROWS_AS(list.as_results().index_of(obj1), Results::DetatchedAccessorException);
            r->cancel_transaction();
        }

        SECTION("throws for row in wrong table") {
            REQUIRE_THROWS(list.find(obj));
            REQUIRE_THROWS(list.as_results().index_of(obj));
        }
    }

    SECTION("find(Query)") {
        List list(r, *lv);

        SECTION("returns index in list for values in the list") {
            REQUIRE(list.find(std::move(target->where().equal(col_value, 5))) == 5);
        }

        SECTION("returns index in list and not index in table") {
            r->begin_transaction();
            list.remove(1);
            REQUIRE(list.find(std::move(target->where().equal(col_value, 5))) == 4);
            r->cancel_transaction();
        }

        SECTION("returns npos for values not in the list") {
            REQUIRE(list.find(std::move(target->where().equal(col_value, 11))) == npos);
        }
    }

    SECTION("add(Context)") {
        List list(r, *lv);
        CppContext ctx(r, &list.get_object_schema());
        r->begin_transaction();

        auto initial_target_size = target->size();
        SECTION("rejects boxed Obj and Object") {
            REQUIRE_THROWS_AS(list.add(ctx, util::Any(target->get_object(5))),
                              List::InvalidEmbeddedOperationException);
            REQUIRE_THROWS_AS(list.add(ctx, util::Any(Object(r, target->get_object(5)))),
                              List::InvalidEmbeddedOperationException);
        }

        SECTION("creates new object for dictionary") {
            list.add(ctx, util::Any(AnyDict{{"value", INT64_C(20)}}));
            REQUIRE(list.size() == 11);
            REQUIRE(target->size() == initial_target_size + 1);
            REQUIRE(list.get(10).get<Int>(col_value) == 20);
        }

        r->cancel_transaction();
    }

    SECTION("set(Context)") {
        List list(r, *lv);
        CppContext ctx(r, &list.get_object_schema());
        r->begin_transaction();

        auto initial_target_size = target->size();
        SECTION("rejects boxed Obj and Object") {
            REQUIRE_THROWS_AS(list.set(ctx, 0, util::Any(target->get_object(5))),
                              List::InvalidEmbeddedOperationException);
            REQUIRE_THROWS_AS(list.set(ctx, 0, util::Any(Object(r, target->get_object(5)))),
                              List::InvalidEmbeddedOperationException);
        }

        SECTION("creates new object for update mode All") {
            auto old_object = list.get<Obj>(0);
            list.set(ctx, 0, util::Any(AnyDict{{"value", INT64_C(20)}}));
            REQUIRE(list.size() == 10);
            REQUIRE(target->size() == initial_target_size);
            REQUIRE(list.get(0).get<Int>(col_value) == 20);
            REQUIRE_FALSE(old_object.is_valid());
        }

        SECTION("mutates the existing object for update mode Modified") {
            auto old_object = list.get<Obj>(0);
            list.set(ctx, 0, util::Any(AnyDict{{"value", INT64_C(20)}}), CreatePolicy::UpdateModified);
            REQUIRE(list.size() == 10);
            REQUIRE(target->size() == initial_target_size);
            REQUIRE(list.get(0).get<Int>(col_value) == 20);
            REQUIRE(old_object.is_valid());
            REQUIRE(list.get(0) == old_object);
        }

        r->cancel_transaction();
    }

    SECTION("find(Context)") {
        List list(r, *lv);
        CppContext ctx(r, &list.get_object_schema());

        SECTION("returns index in list for boxed Obj") {
            REQUIRE(list.find(ctx, util::Any(list.get(5))) == 5);
        }

        SECTION("returns index in list for boxed Object") {
            realm::Object obj(r, *r->schema().find("origin"), list.get(5));
            REQUIRE(list.find(ctx, util::Any(obj)) == 5);
        }

        SECTION("does not insert new objects for dictionaries") {
            auto initial_target_size = target->size();
            REQUIRE(list.find(ctx, util::Any(AnyDict{{"value", INT64_C(20)}})) == npos);
            REQUIRE(target->size() == initial_target_size);
        }

        SECTION("throws for object in wrong table") {
            REQUIRE_THROWS(list.find(ctx, util::Any(obj)));
        }
    }

    SECTION("get(Context)") {
        List list(r, *lv);
        CppContext ctx(r, &list.get_object_schema());

        Object obj;
        REQUIRE_NOTHROW(obj = any_cast<Object&&>(list.get(ctx, 1)));
        REQUIRE(obj.is_valid());
        REQUIRE(obj.obj().get<int64_t>(col_value) == 1);
    }
}


TEST_CASE("list of embedded objects") {
    Schema schema{
        {"parent",
         {
             {"array", PropertyType::Object | PropertyType::Array, "embedded"},
         }},
        {"embedded",
         ObjectSchema::IsEmbedded{true},
         {
             {"value", PropertyType::Int},
         }},
    };

    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.schema_mode = SchemaMode::Automatic;
    config.schema = schema;
    auto realm = Realm::get_shared_realm(config);
    auto parent_table = realm->read_group().get_table("class_parent");
    ColKey col_array = parent_table->get_column_key("array");
    auto embedded_table = realm->read_group().get_table("class_embedded");
    ColKey col_value = embedded_table->get_column_key("value");
    realm->begin_transaction();
    auto parent = parent_table->create_object();
    realm->commit_transaction();

    auto list = List(realm, parent, col_array);

    auto add_two_elements = [&] {
        auto first = list.add_embedded();
        first.set(col_value, 1);

        auto second = list.add_embedded();
        second.set(col_value, 2);
    };

    auto insert_three_elements = [&] {
        // Insert at position 0, shifting all elements back
        auto beginning = list.insert_embedded(0);
        beginning.set(col_value, 0);

        // Insert at position 2, so it's between the originally inserted items
        auto middle = list.insert_embedded(2);
        middle.set(col_value, 10);

        // Insert at the end of the list (i.e. list.size())
        auto end = list.insert_embedded(4);
        end.set(col_value, 20);
    };

    SECTION("add to list") {
        realm->begin_transaction();
        add_two_elements();
        realm->commit_transaction();

        REQUIRE(list.size() == 2);
        REQUIRE(list.get(0).get<int64_t>(col_value) == 1);
        REQUIRE(list.get(1).get<int64_t>(col_value) == 2);
    }

    SECTION("insert in list") {
        realm->begin_transaction();
        add_two_elements();
        insert_three_elements();
        realm->commit_transaction();

        REQUIRE(list.size() == 5);
        REQUIRE(list.get(0).get<int64_t>(col_value) == 0);  // inserted beginning
        REQUIRE(list.get(1).get<int64_t>(col_value) == 1);  // added first
        REQUIRE(list.get(2).get<int64_t>(col_value) == 10); // inserted middle
        REQUIRE(list.get(3).get<int64_t>(col_value) == 2);  // added second
        REQUIRE(list.get(4).get<int64_t>(col_value) == 20); // inserted end
    }

    SECTION("set in list") {
        realm->begin_transaction();

        add_two_elements();
        insert_three_elements();

        auto originalAt2 = list.get(2);
        auto newAt2 = list.set_embedded(2);
        newAt2.set(col_value, 100);

        realm->commit_transaction();

        REQUIRE(originalAt2.is_valid() == false);
        REQUIRE(newAt2.is_valid() == true);

        REQUIRE(list.size() == 5);
        REQUIRE(list.get(0).get<int64_t>(col_value) == 0);   // inserted at beginning
        REQUIRE(list.get(1).get<int64_t>(col_value) == 1);   // added first
        REQUIRE(list.get(2).get<int64_t>(col_value) == 100); // set at 2
        REQUIRE(list.get(3).get<int64_t>(col_value) == 2);   // added second
        REQUIRE(list.get(4).get<int64_t>(col_value) == 20);  // inserted at end
    }

    SECTION("invalid indices") {
        // Insertions
        REQUIRE_THROWS(list.insert_embedded(-1)); // Negative
        REQUIRE_THROWS(list.insert_embedded(1));  // At index > size()

        // Sets
        REQUIRE_THROWS(list.set_embedded(-1)); // Negative
        REQUIRE_THROWS(list.set_embedded(0));  // At index == size()
        REQUIRE_THROWS(list.set_embedded(1));  // At index > size()
    }
}
