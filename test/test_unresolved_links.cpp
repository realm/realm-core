/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"
#ifdef TEST_LINKS

#include <realm.hpp>
#include <realm/util/file.hpp>
#include <realm/array_key.hpp>
#include <realm/history.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

TEST(Unresolved_Basic)
{
    ObjKey k;

    CHECK_NOT(k);
    CHECK_NOT(k.get_unresolved());

    SHARED_GROUP_TEST_PATH(path);
    auto hist = make_in_realm_history();
    DBRef db = DB::create(*hist, path);
    ColKey col_price;
    ColKey col_owns;
    ColKey col_has;
    ColKey col_part;

    {
        // Sync operations
        auto wt = db->start_write();
        auto cars = wt->add_table_with_primary_key("Car", type_String, "model");
        col_price = cars->add_column(type_Decimal, "price");
        auto persons = wt->add_table_with_primary_key("Person", type_String, "e-mail");
        col_owns = persons->add_column(*cars, "car");
        auto dealers = wt->add_table_with_primary_key("Dealer", type_Int, "cvr");
        col_has = dealers->add_column_list(*cars, "stock");
        auto parts = wt->add_table("Parts"); // No primary key
        col_part = cars->add_column(*parts, "part");

        auto finn = persons->create_object_with_primary_key("finn.schiermer-andersen@mongodb.com");
        auto mathias = persons->create_object_with_primary_key("mathias@10gen.com");
        auto joergen = dealers->create_object_with_primary_key(18454033);

        // Sync should use Lst<ObjKey> interface which gives access to all
        // links directly
        auto stock = joergen.get_list<ObjKey>(col_has);

        auto skoda = cars->create_object_with_primary_key("Skoda Fabia").set(col_price, Decimal128("149999.5"));
        auto thingamajig = parts->create_object();
        skoda.set(col_part, thingamajig.get_key());

        auto new_tesla = cars->get_objkey_from_primary_key("Tesla 10");
        CHECK(new_tesla.is_unresolved());
        finn.set(col_owns, new_tesla);
        mathias.set(col_owns, new_tesla);

        auto another_tesla = cars->get_objkey_from_primary_key("Tesla 10");
        stock.insert(0, another_tesla);
        stock.insert(1, skoda.get_key());

        // Create a tombstone implicitly
        auto doodad = parts->get_objkey_from_global_key(GlobalKey{999, 999});
        CHECK(doodad.is_unresolved());
        CHECK_EQUAL(parts->nb_unresolved(), 1);

        wt->commit();
    }

    auto rt = db->start_read();
    auto cars = rt->get_table("Car");
    auto persons = rt->get_table("Person");
    auto dealers = rt->get_table("Dealer");
    auto finn = persons->get_object_with_primary_key("finn.schiermer-andersen@mongodb.com");
    CHECK_NOT(finn.get<ObjKey>(col_owns));
    CHECK(finn.is_unresolved(col_owns));
    auto stock = dealers->get_object_with_primary_key(18454033).get_linklist(col_has);
    CHECK(stock.has_unresolved());
    CHECK_EQUAL(stock.size(), 1);
    CHECK_EQUAL(stock.get(0), cars->get_object_with_primary_key("Skoda Fabia").get_key());
    CHECK_EQUAL(cars->size(), 1);
    auto q = cars->column<Decimal128>(col_price) < Decimal128("300000");
    CHECK_EQUAL(q.count(), 1);

    {
        // Sync operations
        auto wt = db->start_write();
        wt->get_table("Car")->create_object_with_primary_key("Tesla 10").set(col_price, Decimal128("499999.5"));
        wt->commit();
    }

    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(cars->nb_unresolved(), 0);
    CHECK_EQUAL(cars->get_object_with_primary_key("Tesla 10").get_backlink_count(), 3);
    CHECK_EQUAL(stock.size(), 2);
    CHECK_EQUAL(cars->size(), 2);
    CHECK(finn.get<ObjKey>(col_owns));

    {
        // Sync operations
        auto wt = db->start_write();
        auto t = wt->get_table("Car");
        auto car = cars->get_objkey_from_primary_key("Tesla 10");
        CHECK_NOT(car.is_unresolved());
        t->invalidate_object(car);
        wt->commit();
    }

    rt->advance_read();
    rt->verify();
    CHECK(finn.is_unresolved(col_owns));
    CHECK_EQUAL(stock.size(), 1);
    CHECK_EQUAL(stock.get(0), cars->get_object_with_primary_key("Skoda Fabia").get_key());
    CHECK_EQUAL(cars->size(), 1);

    {
        // Sync operations
        auto wt = db->start_write();
        auto parts = wt->get_table("Parts");
        auto tesla = wt->get_table("Car")->create_object_with_primary_key("Tesla 10");
        tesla.set(col_price, Decimal128("499999.5"));
        auto doodad = parts->create_object(GlobalKey{999, 999});
        auto doodad1 = parts->create_object(GlobalKey{999, 999}); // Check idempotency
        CHECK_EQUAL(doodad.get_key(), doodad1.get_key());
        CHECK_EQUAL(doodad.get_object_id(), doodad1.get_object_id());
        tesla.set(col_part, doodad.get_key());
        auto doodad_key = parts->get_objkey_from_global_key(GlobalKey{999, 999});
        CHECK(!doodad_key.is_unresolved());
        CHECK_EQUAL(wt->get_table("Parts")->nb_unresolved(), 0);

        wt->commit();
    }

    rt->advance_read();
    CHECK_EQUAL(stock.size(), 2);
    CHECK_EQUAL(cars->size(), 2);
    CHECK(finn.get<ObjKey>(col_owns));
}


TEST(Unresolved_InvalidateObject)
{
    Group g;

    auto wheels = g.add_embedded_table("Wheels");
    auto cars = g.add_table_with_primary_key("Car", type_String, "model");
    auto col_wheels = cars->add_column_list(*wheels, "wheels");
    auto col_price = cars->add_column(type_Decimal, "price");
    auto dealers = g.add_table("Dealer");
    auto col_has = dealers->add_column_list(*cars, "stock");
    auto organization = g.add_table("Organization");
    auto col_members = organization->add_column_list(*dealers, "members");

    auto dealer1 = dealers->create_object();
    auto dealer2 = dealers->create_object();
    auto org = organization->create_object();

    auto members = org.get_linklist(col_members);
    members.add(dealer1.get_key());
    members.add(dealer2.get_key());

    auto create_car = [&](const char* name, const char* price) {
        Obj car = cars->create_object_with_primary_key(name).set(col_price, Decimal128(price));
        auto list = car.get_linklist(col_wheels);
        for (int i = 0; i < 4; i++) {
            list.create_and_insert_linked_object(i);
        }
        return car;
    };

    auto skoda = create_car("Skoda Fabia", "149999.5");
    auto tesla = create_car("Tesla 10", "499999.5");

    auto stock = dealer1.get_linklist(col_has);
    stock.add(tesla.get_key());
    stock.add(skoda.get_key());

    CHECK_EQUAL(stock.size(), 2);
    CHECK_EQUAL(members.size(), 2);
    CHECK_EQUAL(cars->size(), 2);
    CHECK_EQUAL(wheels->size(), 8);

    // Tesla goes to the grave. Too expensive
    cars->invalidate_object(tesla.get_key());

    auto tesla_key = cars->get_objkey_from_primary_key("Tesla 10");
    CHECK(tesla_key.is_unresolved());

    CHECK_EQUAL(stock.size(), 1);
    CHECK_EQUAL(stock.get(0), skoda.get_key());
    CHECK_EQUAL(cars->size(), 1);
    CHECK_EQUAL(wheels->size(), 4);

    // One dealer goes bankrupt
    dealer2.invalidate();
    CHECK_EQUAL(members.size(), 1);
    CHECK_EQUAL(dealers->nb_unresolved(), 1);

    // resurrect the tesla
    create_car("Tesla 10", "399999.5");
    CHECK_EQUAL(stock.size(), 2);
    CHECK_EQUAL(cars->size(), 2);
    CHECK_EQUAL(wheels->size(), 8);
}

TEST(Unresolved_LinkList)
{
    Group g;

    auto cars = g.add_table_with_primary_key("Car", type_String, "model");
    auto dealers = g.add_table_with_primary_key("Dealer", type_Int, "cvr");
    auto col_has = dealers->add_column_list(*cars, "stock");

    auto dealer = dealers->create_object_with_primary_key(18454033);
    auto stock1 = dealer.get_linklist(col_has);
    auto stock2 = dealer.get_linklist(col_has);

    auto skoda = cars->create_object_with_primary_key("Skoda Fabia");
    auto tesla = cars->create_object_with_primary_key("Tesla 10");
    auto volvo = cars->create_object_with_primary_key("Volvo XC90");
    auto bmw = cars->create_object_with_primary_key("BMW 750");
    auto mercedes = cars->create_object_with_primary_key("Mercedes SLC500");

    stock1.add(skoda.get_key());
    stock1.add(tesla.get_key());
    stock1.add(volvo.get_key());
    stock1.add(bmw.get_key());

    CHECK_EQUAL(stock1.size(), 4);
    CHECK_EQUAL(stock2.size(), 4);
    tesla.invalidate();
    CHECK_EQUAL(stock1.size(), 3);
    CHECK_EQUAL(stock2.size(), 3);

    stock1.add(mercedes.get_key());
    // If REALM_MAX_BPNODE_SIZE is 4, we test that context flag is copied over when replacing root
    CHECK_EQUAL(stock1.size(), 4);
    CHECK_EQUAL(stock2.size(), 4);

    LnkLst stock_copy{stock1};
    CHECK_EQUAL(stock_copy.get(3), mercedes.get_key());
}

TEST(Unresolved_NullKey)
{
    Group group;
    auto table = group.add_table_with_primary_key("table", type_UUID, "_id", true);
    auto list_col = table->add_column_list(*table, "links");

    UUID pk2;
    {
        auto obj = table->create_object_with_primary_key(Mixed{}); // null is a valid key
        auto list = obj.get_linklist(list_col);
        list.insert(0, obj.get_key());
        auto obj2 = table->create_object_with_primary_key(pk2);
        auto list2 = obj2.get_linklist(list_col);
        list2.insert(0, obj.get_key());
        table->invalidate_object(obj.get_key());
        CHECK_EQUAL(table->size(), 1);
        auto unresolved_obj_key = table->get_objkey_from_primary_key(Mixed{});
        CHECK(unresolved_obj_key.is_unresolved());
    }

    {
        CHECK_EQUAL(table->size(), 1);
        auto obj2 = table->get_object_with_primary_key(pk2);
        CHECK(obj2);
        auto list2 = obj2.get_linklist(list_col);
        CHECK_EQUAL(list2.size(), 0); // the tombstoned object has removed itself from any forward links
        auto obj_resurrected = table->create_object_with_primary_key(Mixed{});
        CHECK_NOT(obj_resurrected.get_key().is_unresolved());
        CHECK_EQUAL(table->size(), 2);
        CHECK_EQUAL(list2.size(), 1); // the forward link was populated again after resurrection
        CHECK_EQUAL(list2.get(0), obj_resurrected.get_key());
    }
}

TEST(Unresolved_MixedIndexed)
{
    Group group;
    auto table = group.add_table_with_primary_key("table", type_UUID, "_id", true);
    auto mixed_col = table->add_column(type_Mixed, "mixed", true);
    table->add_search_index(mixed_col);

    UUID pk2;
    {
        auto src_obj = table->create_object_with_primary_key(Mixed{});
        auto dst_obj = table->create_object_with_primary_key(pk2);
        CHECK_EQUAL(src_obj.get<Mixed>(mixed_col), Mixed{});
        src_obj.set<Mixed>(mixed_col, Mixed{ObjLink{table->get_key(), dst_obj.get_key()}});
        dst_obj.set<Mixed>(mixed_col, Mixed{ObjLink{table->get_key(), src_obj.get_key()}});
        table->invalidate_object(dst_obj.get_key());
        CHECK_EQUAL(table->size(), 1);
        auto unresolved_obj_key = table->get_objkey_from_primary_key(pk2);
        CHECK(unresolved_obj_key.is_unresolved());
    }

    {
        auto obj_resurrected = table->create_object_with_primary_key(pk2);
        CHECK_NOT(obj_resurrected.get_key().is_unresolved());
        CHECK_EQUAL(obj_resurrected.get<Mixed>(mixed_col), Mixed{});
        CHECK_EQUAL(table->size(), 2);
        auto src_obj = table->get_object_with_primary_key(Mixed{});
        CHECK(src_obj);
        Mixed expected{ObjLink{table->get_key(), obj_resurrected.get_key()}};
        CHECK_EQUAL(src_obj.get<Mixed>(mixed_col), expected);
    }
}


TEST(Unresolved_QueryOverLinks)
{
    Group g;

    auto cars = g.add_table_with_primary_key("Car", type_String, "model");
    auto col_price = cars->add_column(type_Decimal, "price");
    auto persons = g.add_table_with_primary_key("Person", type_String, "e-mail");
    auto col_owns = persons->add_column(*cars, "car");
    auto dealers = g.add_table_with_primary_key("Dealer", type_Int, "cvr");
    auto col_has = dealers->add_column_list(*cars, "stock");

    auto finn = persons->create_object_with_primary_key("finn.schiermer-andersen@mongodb.com");
    auto mathias = persons->create_object_with_primary_key("mathias@10gen.com");
    auto bilcentrum = dealers->create_object_with_primary_key(18454033);
    auto bilmekka = dealers->create_object_with_primary_key(26293995);
    auto skoda = cars->create_object_with_primary_key("Skoda Fabia").set(col_price, Decimal128("149999.5"));
    auto tesla = cars->create_object_with_primary_key("Tesla 3").set(col_price, Decimal128("449999.5"));
    auto volvo = cars->create_object_with_primary_key("Volvo XC90").set(col_price, Decimal128("1056000"));
    auto bmw = cars->create_object_with_primary_key("BMW 750").set(col_price, Decimal128("2088188"));
    auto mercedes = cars->create_object_with_primary_key("Mercedes SLC500").set(col_price, Decimal128("2355103"));

    finn.set(col_owns, skoda.get_key());
    mathias.set(col_owns, bmw.get_key());

    {
        auto stock = bilcentrum.get_linklist(col_has);
        stock.add(skoda.get_key());
        stock.add(tesla.get_key());
        stock.add(volvo.get_key());
    }
    {
        auto stock = bilmekka.get_linklist(col_has);
        stock.add(volvo.get_key());
        stock.add(bmw.get_key());
        stock.add(mercedes.get_key());
    }

    auto q = dealers->link(col_has).column<Decimal128>(col_price) < Decimal128("1000000");
    CHECK_EQUAL(q.count(), 1);

    auto new_tesla = cars->get_objkey_from_primary_key("Tesla 10");
    bilmekka.get_list<ObjKey>(col_has).insert(0, new_tesla);
    CHECK_EQUAL(q.count(), 1);

    q = persons->link(col_owns).column<Decimal128>(col_price) < Decimal128("1000000");
    CHECK_EQUAL(q.count(), 1);
    mathias.set(col_owns, new_tesla);
    CHECK_EQUAL(q.count(), 1);

    auto stock = bilmekka.get_linklist(col_has);
    q = cars->where(stock).and_query(cars->column<Decimal128>(col_price) < Decimal128("2000000"));
    CHECK_EQUAL(q.count(), 1);
}

TEST(Unresolved_PrimaryKeyInt)
{
    Group g;

    auto foo = g.add_table_with_primary_key("foo", type_Int, "id");
    auto bar = g.add_table("bar");
    auto col = bar->add_column(*foo, "link");

    auto obj = bar->create_object();
    auto unres = foo->get_objkey_from_primary_key(5);
    obj.set(col, unres);
    CHECK_NOT(obj.get<ObjKey>(col));
    CHECK_EQUAL(foo->nb_unresolved(), 1);
    auto lazarus = foo->create_object_with_primary_key(5);
    CHECK_EQUAL(obj.get<ObjKey>(col), lazarus.get_key());
}

TEST(Unresolved_GarbageCollect)
{
    Group g;

    auto cars = g.add_table_with_primary_key("Car", type_String, "model");
    auto persons = g.add_table_with_primary_key("Person", type_String, "e-mail");
    auto col_owns = persons->add_column(*cars, "car");

    auto finn = persons->create_object_with_primary_key("finn.schiermer-andersen@mongodb.com");
    auto mathias = persons->create_object_with_primary_key("mathias@10gen.com");

    auto new_tesla = cars->get_objkey_from_primary_key("Tesla 10");

    finn.set(col_owns, new_tesla);
    mathias.set(col_owns, new_tesla);
    CHECK_EQUAL(cars->nb_unresolved(), 1);
    finn.set_null(col_owns);
    CHECK_EQUAL(cars->nb_unresolved(), 1);
    mathias.set_null(col_owns);
    CHECK_EQUAL(cars->nb_unresolved(), 0);

    // Try the same with linklists. Here you have to clear the lists in order to
    // remove the unresolved links
    auto dealers = g.add_table_with_primary_key("Dealer", type_Int, "cvr");
    auto col_has = dealers->add_column_list(*cars, "stock");
    auto bilcentrum = dealers->create_object_with_primary_key(18454033);
    auto bilmekka = dealers->create_object_with_primary_key(26293995);

    new_tesla = cars->get_objkey_from_primary_key("Tesla 10");

    bilcentrum.get_list<ObjKey>(col_has).insert(0, new_tesla);
    bilmekka.get_list<ObjKey>(col_has).insert(0, new_tesla);
    CHECK_EQUAL(cars->nb_unresolved(), 1);

    bilcentrum.get_linklist(col_has).clear();
    CHECK_EQUAL(cars->nb_unresolved(), 1);
    bilmekka.get_linklist(col_has).clear();
    CHECK_EQUAL(cars->nb_unresolved(), 0);

    new_tesla = cars->get_objkey_from_primary_key("Tesla 10");
    bilcentrum.get_list<ObjKey>(col_has).insert(0, new_tesla);
    CHECK_EQUAL(cars->nb_unresolved(), 1);
    bilcentrum.remove();
    CHECK_EQUAL(cars->nb_unresolved(), 0);
}

TEST(Unresolved_PkCollission)
{
    Group g;

    auto t = g.add_table_with_primary_key("Table", type_Int, "id");
    auto col_str = t->add_column(type_String, "str");
    t->add_search_index(col_str);

    // This pk will collide with plain '7'
    int64_t pk7 = int64_t(7 + (1ull << 63));
    auto k1 = t->get_objkey_from_primary_key(pk7);
    CHECK(k1.is_unresolved());
    auto k2 = t->create_object_with_primary_key(7, {{col_str, "Foo"}}).get_key();
    CHECK_EQUAL(t->nb_unresolved(), 1);
    CHECK_EQUAL(t->size(), 1);
    auto k3 = t->create_object_with_primary_key(pk7, {{col_str, "Bar"}}).get_key();
    CHECK_NOT_EQUAL(k2, k3);
    CHECK_EQUAL(t->nb_unresolved(), 0);
    CHECK_EQUAL(t->size(), 2);

    // This pk will collide with plain '9'
    int64_t pk9 = int64_t(9 + (1ull << 63));
    k2 = t->create_object_with_primary_key(pk9, {{col_str, "Foo"}}).get_key();
    k1 = t->get_objkey_from_primary_key(9);
    CHECK(k1.is_unresolved());
    CHECK_EQUAL(t->nb_unresolved(), 1);
    CHECK_EQUAL(t->size(), 3);
    k3 = t->create_object_with_primary_key(9, {{col_str, "Bar"}}).get_key();
    CHECK_NOT_EQUAL(k2, k3);
    CHECK_EQUAL(t->nb_unresolved(), 0);
    CHECK_EQUAL(t->size(), 4);

    // This pk will collide with plain '5'
    int64_t pk5 = int64_t(5 + (1ull << 63));
    k1 = t->get_objkey_from_primary_key(pk5);
    k2 = t->get_objkey_from_primary_key(5);
    CHECK_NOT_EQUAL(k1, k2);
    CHECK_EQUAL(t->nb_unresolved(), 2);
    t->create_object_with_primary_key(pk5, {{col_str, "Foo"}}).get_key();
    k2 = t->create_object_with_primary_key(5, {{col_str, "Bar"}}).get_key();
    CHECK_EQUAL(t->nb_unresolved(), 0);
    CHECK_EQUAL(t->size(), 6);
    t->clear();
    k3 = t->create_object_with_primary_key(5, {{col_str, "Bar"}}).get_key();
    // Collision table should have be cleared
    CHECK_NOT_EQUAL(k2, k3);
}

TEST(Unresolved_CondensedIndices)
{
    Group g;
    auto t1 = g.add_table_with_primary_key("Table", type_Int, "id");
    auto t2 = g.add_table_with_primary_key("Table2", type_Int, "id");
    t1->add_column_list(*t2, "t2s");

    auto obj123 = t2->create_object_with_primary_key(123);
    auto obj456 = t2->create_object_with_primary_key(456);
    auto obj789 = t1->create_object_with_primary_key(789);
    auto ll = obj789.get_linklist("t2s");
    ll.insert(0, obj123.get_key());
    ll.insert(1, obj456.get_key());

    obj123.invalidate();

    CHECK_EQUAL(obj789.get_linklist("t2s").size(), 1);

    const Obj const_obj789 = obj789;
    LnkLst list1 = const_obj789.get_linklist("t2s");
    LnkLst list2;
    CHECK_EQUAL(list1.size(), 1);
    CHECK_EQUAL(list1.get_object(0).get_key(), obj456.get_key());
    list2 = list1;
    CHECK_EQUAL(list2.size(), 1);

    auto key_list = obj789.get_list<ObjKey>("t2s");
    CHECK_EQUAL(key_list.size(), 2);

    // Check that find methods return condensed indices.

    CHECK_EQUAL(list1.find_first(obj123.get_key()), not_found);
    CHECK_EQUAL(list1.find_first(obj456.get_key()), 0);

    std::vector<size_t> found_indices;
    list1.find_all(obj123.get_key(), [&](size_t index) {
        found_indices.push_back(index);
    });
    CHECK_EQUAL(found_indices.size(), 0);
    found_indices.clear();
    list1.find_all(obj456.get_key(), [&](size_t index) {
        found_indices.push_back(index);
    });
    CHECK_EQUAL(found_indices.size(), 1);
    CHECK_EQUAL(found_indices[0], 0);

    // Check that the list of unresolved indices remains consistent over
    // insertion to the middle. list1 currently considers index 0 to be
    // unresolved, because obj123 was invalidated above. Insertion to index 0
    // should bump the unresolved index to 1.
    CHECK_EQUAL(key_list.get(0), obj123.get_key());
    CHECK_EQUAL(key_list.get(1), obj456.get_key());
    key_list.insert(2, obj123.get_key());
    key_list.insert(3, obj456.get_key());
    // Uncondensed list is now: (obj123, obj456, obj123, obj456)
    // Condensed list is now: (obj456, obj456)
    list1.insert(0, obj456.get_key());
    // Uncondensed list is now: (obj123, obj456, obj456, obj123, obj456)
    // Condensed list should now be: (obj456, obj456, obj456)
    CHECK_EQUAL(list1.get(0), obj456.get_key());
    CHECK_EQUAL(list1.get(1), obj456.get_key());
    CHECK_EQUAL(list1.get(2), obj456.get_key());
}

TEST(Unresolved_Recursive)
{
    Group g;
    auto table = g.add_table_with_primary_key("RecursiveNode", type_ObjectId, "_id");
    // Create two link columns. This will create two backlink columns in the target table
    // When a tombstone is resurrected, and a backlink in the first backlink column is
    // removed and the tombstone is thereby deleted, we should not attempt to find backlinks
    // in the second backlink column.
    auto col_next = table->add_column(*table, "NextNode");
    auto col_children = table->add_column_list(*table, "children");

    auto key = table->get_objkey_from_primary_key(ObjectId("5fc929bac4a3964b6d603f4e"));
    key = table->create_object_with_primary_key(ObjectId("5fc929bac4a3964b6d603f4d")).set(col_next, key).get_key();

    table->create_object_with_primary_key(ObjectId("5fc929bac4a3964b6d603f4c")).set(col_next, key);

    // This will delete the tombstone for "5fc929bac4a3964b6d603f4e"
    table->create_object_with_primary_key(ObjectId("5fc929bac4a3964b6d603f4e"));

    // The following will ensure that objects will be turned into tombstones when invalidated
    auto obj = table->create_object_with_primary_key(ObjectId("5fc929bac4a3964b6d603f4b"));
    auto ll = obj.get_linklist(col_children);
    ll.add(table->get_objkey_from_primary_key(ObjectId("5fc929bac4a3964b6d603f4c")));
    ll.add(table->get_objkey_from_primary_key(ObjectId("5fc929bac4a3964b6d603f4d")));
    ll.add(table->get_objkey_from_primary_key(ObjectId("5fc929bac4a3964b6d603f4e")));

    g.verify();
    CHECK_EQUAL(table->nb_unresolved(), 0);
    table->get_object_with_primary_key(ObjectId("5fc929bac4a3964b6d603f4c")).invalidate();
    table->get_object_with_primary_key(ObjectId("5fc929bac4a3964b6d603f4d")).invalidate();
    CHECK_EQUAL(table->nb_unresolved(), 2);
    g.verify();
}

TEST(Links_ManyObjects)
{
    SHARED_GROUP_TEST_PATH(path);
    auto hist = make_in_realm_history();
    DBRef db = DB::create(*hist, path);

    auto tr = db->start_write();
    TableRef table = tr->add_table_with_primary_key("table", type_Int, "id");
    auto col = table->add_column_list(*table, "l");
    Obj obj = table->create_object_with_primary_key(0xcafe);
    auto link_list = obj.get_list<ObjKey>(col);

    for (int64_t i = 0; i < 1500; i++) {
        link_list.add(table->get_objkey_from_primary_key(i));
    }
    for (int64_t i = 0; i < 1500; i++) {
        table->create_object_with_primary_key(i);
    }
    // now the context flag of the root node of the BPlusTree be cleared as all
    // objects are now created. The context flags of the leaves is still set.
    tr->commit_and_continue_as_read();
    tr->promote_to_write();

    // When the first leaf becomes root, it has to have the context flag cleared
    // which will trigger a COW. If the parent array is not updated, we will have a
    // double free when the array is COW next time.
    link_list.clear();

    tr->commit();
}

#endif
