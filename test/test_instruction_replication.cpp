#include "test.hpp"
#include "util/compare_groups.hpp"

#include <realm/db.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/changeset_parser.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::test_util;

namespace {

struct Fixture {
    unit_test::TestContext& test_context;
    DBTestPathGuard path_1;
    DBTestPathGuard path_2;
    std::unique_ptr<ClientReplication> history_1;
    std::unique_ptr<ClientReplication> history_2;
    DBRef sg_1;
    DBRef sg_2;

    explicit Fixture(unit_test::TestContext& test_context)
        : test_context(test_context)
        , path_1(realm::test_util::get_test_path(test_context, ".path_1.realm"))
        , path_2(realm::test_util::get_test_path(test_context, ".path_2.realm"))
        , history_1(make_client_replication(path_1))
        , history_2(make_client_replication(path_2))
        , sg_1(DB::create(*history_1))
        , sg_2(DB::create(*history_2))
    {
        // This is to ensure that peer IDs in Object IDs are populated.
        bool fix_up_object_ids = true;
        history_1->set_client_file_ident({1, 123}, fix_up_object_ids);
    }

    void replay_transactions()
    {

        Changeset result;
        const auto& buffer = history_1->get_instruction_encoder().buffer();
        _impl::SimpleNoCopyInputStream stream{buffer.data(), buffer.size()};
        sync::parse_changeset(stream, result);

        WriteTransaction wt{sg_2};
        TableInfoCache table_info_cache{wt};
        InstructionApplier applier{wt, table_info_cache};
        applier.apply(result, &test_context.logger);
        wt.commit();
    }

    void check_equal()
    {
        ReadTransaction rt_1{sg_1};
        ReadTransaction rt_2{sg_2};
        compare_groups(rt_1, rt_2);
    }
};

} // unnamed namespace


TEST(InstructionReplication_AddTable)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        sync::create_table(wt, "class_foo");
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
    }
}

// This test is disabled because EraseTable instruction is unsupported by merge algorithm.
TEST_IF(InstructionReplication_EraseTable, false)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        sync::create_table(wt, "class_foo");
        wt.get_group().remove_table("class_foo");
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(!rt.has_table("class_foo"));
    }
}

TEST(InstructionReplication_ClearTable)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        auto t = wt.get_or_add_table("class_foo");
        for (size_t i = 0; i < 10; i++)
            t->create_object();
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        auto t = rt.get_table("class_foo");
        CHECK(t);
        CHECK_EQUAL(t->size(), 10);
    }

    {
        WriteTransaction wt{fixture.sg_1};
        auto t = wt.get_table("class_foo");
        t->clear();
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        auto t = rt.get_table("class_foo");
        CHECK(t);
        CHECK_EQUAL(t->size(), 0);
    }
}

TEST(InstructionReplication_CreateObject)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = sync::create_table(wt, "class_foo");
        ColKey col_ndx = foo->add_column(type_Int, "i");
        sync::create_object(wt, *foo).set(col_ndx, 123);
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        ConstTableRef foo = rt.get_table("class_foo");
        CHECK_EQUAL(foo->size(), 1);
        ColKey col_ndx = foo->get_column_key("i");
        CHECK_EQUAL(foo->begin()->get<Int>(col_ndx), 123);
    }
}

TEST(InstructionReplication_CreateObjectNullStringPK)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        bool nullable = true;
        TableRef foo = sync::create_table_with_primary_key(wt, "class_foo", type_String, "pk", nullable);
        Obj obj = sync::create_object_with_primary_key(wt, *foo, StringData{});
        ColKey col_ndx = foo->get_column_key("pk");
        CHECK(obj.is_null(col_ndx));
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        ConstTableRef foo = rt.get_table("class_foo");
        CHECK_EQUAL(foo->size(), 1);
        ColKey col_ndx = foo->get_column_key("pk");
        CHECK(foo->begin()->is_null(col_ndx));
    }
}

TEST(InstructionReplication_CreateObjectObjectIdPK)
{
    ObjectId id("cafebabedeadbeef00000000");
    Decimal128 cost("10.50");
    Fixture fixture{test_context};
    ObjKey key;
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = sync::create_table_with_primary_key(wt, "class_foo", type_ObjectId, "_id", false);
        auto col_dec = foo->add_column(type_Decimal, "cost");
        key = foo->create_object_with_primary_key(id).set(col_dec, cost).get_key();
        foo->create_object_with_primary_key(ObjectId::gen());
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        ConstTableRef foo = rt.get_table("class_foo");
        CHECK_EQUAL(foo->size(), 2);
        ColKey col_ndx = foo->get_column_key("_id");
        ColKey col_dec = foo->get_column_key("cost");
        auto obj = foo->get_object(key);
        CHECK_EQUAL(obj.get<ObjectId>(col_ndx), id);
        CHECK_EQUAL(obj.get<Decimal128>(col_dec), cost);
    }
}

TEST(InstructionReplication_CreateEmbedded)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef car = sync::create_table_with_primary_key(wt, "class_Car", type_String, "id", false);
        auto wheel = wt.add_embedded_table("class_Wheel");
        auto col_position = wheel->add_column(type_String, "position");
        auto col_wheels = car->add_column_link(type_LinkList, "wheels", *wheel);
        Obj volvo = car->create_object_with_primary_key("Volvo");

        auto list = volvo.get_linklist(col_wheels);
        list.create_and_insert_linked_object(0).set(col_position, "FR");
        list.create_and_insert_linked_object(1).set(col_position, "FL");
        list.create_and_insert_linked_object(2).set(col_position, "RR");
        list.create_and_insert_linked_object(3).set(col_position, "RL");
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        ConstTableRef car = rt.get_table("class_Car");
        ConstTableRef wheel = rt.get_table("class_Wheel");
        auto col_wheels = car->get_column_key("wheels");
        auto col_position = wheel->get_column_key("position");
        CHECK_NOT(car->is_embedded());
        CHECK(wheel->is_embedded());
        CHECK_EQUAL(car->size(), 1);
        auto list = car->begin()->get_linklist(col_wheels);
        CHECK_EQUAL(list.size(), 4);
        CHECK_EQUAL(list.get_object(0).get<String>(col_position), "FR");
    }
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef car = wt.get_table("class_Car");
        TableRef wheel = wt.get_table("class_Wheel");
        auto col_wheels = car->get_column_key("wheels");
        auto col_position = wheel->get_column_key("position");

        auto list = car->begin()->get_linklist(col_wheels);
        list.create_and_set_linked_object(0).set(col_position, "FR replacement");
        list.remove(2);
        CHECK_EQUAL(wheel->size(), 3);
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        ConstTableRef car = rt.get_table("class_Car");
        ConstTableRef wheel = rt.get_table("class_Wheel");
        CHECK_EQUAL(car->size(), 1);
        CHECK_EQUAL(wheel->size(), 3);
    }
}

TEST(InstructionReplication_EraseObject)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = wt.get_or_add_table("class_foo");
        ColKey col_ndx = foo->add_column(type_Int, "i");
        TableRef bar = wt.get_or_add_table("class_bar");
        ColKey col_link = bar->add_column_link(type_Link, "link", *foo);

        Obj obj = foo->create_object().set(col_ndx, 123);
        // Create link to object soon to be deleted
        bar->create_object().set(col_link, obj.get_key());

        foo->create_object().set(col_ndx, 456);
        obj.remove();
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        ConstTableRef foo = rt.get_table("class_foo");
        CHECK_EQUAL(foo->size(), 1);
        // Links were removed before object was invalidated
        CHECK_EQUAL(foo->nb_unresolved(), 0);
        ColKey col_ndx = foo->get_column_key("i");
        CHECK_EQUAL(foo->begin()->get<Int>(col_ndx), 456);
    }
}

TEST(InstructionReplication_InvalidateObject)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = wt.get_or_add_table("class_foo");
        ColKey col_ndx = foo->add_column(type_Int, "i");
        TableRef bar = wt.get_or_add_table("class_bar");
        ColKey col_link = bar->add_column_link(type_Link, "link", *foo);
        ColKey col_linklist = bar->add_column_link(type_LinkList, "linklist", *foo);

        Obj obj = foo->create_object().set(col_ndx, 123);
        // Create link to object soon to be deleted
        bar->create_object().set(col_link, obj.get_key()).get_linklist(col_linklist).add(obj.get_key());

        foo->create_object().set(col_ndx, 456);
        obj.invalidate();
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        auto foo = rt.get_table("class_foo");
        CHECK_EQUAL(foo->size(), 1);
        CHECK_EQUAL(foo->nb_unresolved(), 1);
        ColKey col_ndx = foo->get_column_key("i");
        CHECK_EQUAL(foo->begin()->get<Int>(col_ndx), 456);
        auto bar = rt.get_table("class_bar");
        ColKey col_link = bar->get_column_key("link");
        ColKey col_linklist = bar->get_column_key("linklist");
        CHECK(bar->begin()->is_unresolved(col_link));
        CHECK(bar->begin()->get_linklist(col_linklist).has_unresolved());
    }
}

TEST(InstructionReplication_SetLink)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = sync::create_table(wt, "class_foo");
        TableRef bar = sync::create_table(wt, "class_bar");
        ColKey foo_i = foo->add_column(type_Int, "i");
        ColKey bar_l = bar->add_column_link(type_Link, "l", *foo);

        auto foo_1 = sync::create_object(wt, *foo).set(foo_i, 123).get_key();
        auto foo_2 = sync::create_object(wt, *foo).set(foo_i, 456).get_key();

        sync::create_object(wt, *bar).set(bar_l, foo_1);
        sync::create_object(wt, *bar).set(bar_l, foo_2);


        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        CHECK(rt.has_table("class_bar"));
        ConstTableRef foo = rt.get_table("class_foo");
        ConstTableRef bar = rt.get_table("class_bar");
        CHECK_EQUAL(foo->size(), 2);
        CHECK_EQUAL(bar->size(), 2);

        ColKey foo_i = foo->get_column_key("i");
        ColKey bar_l = bar->get_column_key("l");
        ObjKey bar_0 = bar->begin()->get_key();
        ObjKey bar_1 = (bar->begin() + 1)->get_key();
        CHECK_EQUAL(foo->get_object(bar->get_object(bar_0).get<ObjKey>(bar_l)).get<Int>(foo_i), 123);
        CHECK_EQUAL(foo->get_object(bar->get_object(bar_1).get<ObjKey>(bar_l)).get<Int>(foo_i), 456);
    }
}

TEST(InstructionReplication_AddInteger)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = sync::create_table(wt, "class_foo");
        ColKey col_ndx = foo->add_column(type_Int, "i");
        sync::create_object(wt, *foo).add_int(col_ndx, 123);
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        ConstTableRef foo = rt.get_table("class_foo");
        CHECK_EQUAL(foo->size(), 1);
        ColKey col_ndx = foo->get_column_key("i");
        CHECK_EQUAL(foo->begin()->get<Int>(col_ndx), 123);
    }
}

TEST(InstructionReplication_ListSwap)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = sync::create_table(wt, "class_foo");
        ColKey col_list = foo->add_column_list(type_Int, "i");
        auto list = foo->create_object().get_list<Int>(col_list);
        list.add(1);
        list.add(5);
        list.add(7);
        list.add(11);    /* 1, 5, 7, 11 */
        list.swap(0, 1); /* 5, 1, 7, 11 */
        list.swap(3, 2); /* 5, 1, 11, 7 */
        list.swap(3, 0); /* 7, 1, 11, 5 */
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        ConstTableRef foo = rt.get_table("class_foo");
        ColKey col_list = foo->get_column_key("i");
        CHECK_EQUAL(foo->size(), 1);
        auto list = foo->begin()->get_list<Int>(col_list);
        CHECK_EQUAL(list.size(), 4);
        CHECK_EQUAL(list.get(0), 7);
        CHECK_EQUAL(list.get(1), 1);
        CHECK_EQUAL(list.get(2), 11);
        CHECK_EQUAL(list.get(3), 5);
    }
}

TEST(InstructionReplication_LinkLists)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = sync::create_table(wt, "class_foo");
        TableRef bar = sync::create_table(wt, "class_bar");
        ColKey foo_i = foo->add_column(type_Int, "i");
        ColKey bar_ll = bar->add_column_link(type_LinkList, "ll", *foo);

        ObjKey foo_1 = sync::create_object(wt, *foo).set(foo_i, 123).get_key();
        ObjKey foo_2 = sync::create_object(wt, *foo).set(foo_i, 456).get_key();

        Obj bar_1 = sync::create_object(wt, *bar);
        Obj bar_2 = sync::create_object(wt, *bar);

        bar_1.get_linklist(bar_ll).insert(0, foo_1);
        bar_1.get_linklist(bar_ll).insert(1, foo_1);

        bar_2.get_linklist(bar_ll).insert(0, foo_2);
        bar_2.get_linklist(bar_ll).insert(1, foo_2);

        bar_1.get_linklist(bar_ll).set(0, foo_2);
        bar_2.get_linklist(bar_ll).set(1, foo_1);

        foo->remove_object(foo_1);

        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        CHECK(rt.has_table("class_bar"));
        ConstTableRef foo = rt.get_table("class_foo");
        ConstTableRef bar = rt.get_table("class_bar");
        CHECK_EQUAL(foo->size(), 1);
        CHECK_EQUAL(bar->size(), 2);

        ColKey foo_i = foo->get_column_key("i");
        ColKey bar_ll = bar->get_column_key("ll");

        CHECK_EQUAL(foo->begin()->get<Int>(foo_i), 456);
        Obj bar_1 = *bar->begin();
        Obj bar_2 = *(bar->begin() + 1);
        CHECK_EQUAL(bar_1.get_linklist(bar_ll).size(), 1);
        CHECK_EQUAL(bar_2.get_linklist(bar_ll).size(), 1);
    }
}

TEST(InstructionReplication_NullablePrimaryKeys)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        bool nullable = true;
        TableRef t = sync::create_table_with_primary_key(wt, "class_t", type_Int, "pk", nullable);
        ColKey col_ndx = t->add_column(type_Int, "i");

        sync::create_object_with_primary_key(wt, *t, 123).set(col_ndx, 123);
        ObjKey first = t->find_first<util::Optional<int64_t>>(t->get_primary_key_column(), 123);
        REALM_ASSERT(first);

        sync::create_object_with_primary_key(wt, *t, util::none).set(col_ndx, 456);
        ObjKey second = t->find_first_null(t->get_primary_key_column());
        REALM_ASSERT(second);

        sync::create_object_with_primary_key(wt, *t, 789).set(col_ndx, 789);
        ObjKey third = t->find_first<util::Optional<int64_t>>(t->get_primary_key_column(), 789);
        REALM_ASSERT(third);

        first = t->find_first<util::Optional<int64_t>>(t->get_primary_key_column(), 123);
        REALM_ASSERT(first);
        second = t->find_first_null(t->get_primary_key_column());
        REALM_ASSERT(second);
        third = t->find_first<util::Optional<int64_t>>(t->get_primary_key_column(), 789);
        REALM_ASSERT(third);

        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_t"));
        ConstTableRef t = rt.get_table("class_t");
        CHECK_EQUAL(t->size(), 3);
        ColKey pk_ndx = t->get_column_key("pk");
        ColKey col_ndx = t->get_column_key("i");

        ObjKey first_key = t->find_first<util::Optional<int64_t>>(pk_ndx, 123);
        ObjKey second_key = t->find_first_null(pk_ndx);
        ObjKey third_key = t->find_first<util::Optional<int64_t>>(pk_ndx, 789);

        CHECK(first_key);
        CHECK(second_key);
        CHECK(third_key);

        ConstObj first = t->get_object(first_key);
        ConstObj second = t->get_object(second_key);
        ConstObj third = t->get_object(third_key);

        CHECK_EQUAL(first.get<int64_t>(col_ndx), 123);
        CHECK_EQUAL(second.get<int64_t>(col_ndx), 456);
        CHECK_EQUAL(third.get<int64_t>(col_ndx), 789);
    }
}
