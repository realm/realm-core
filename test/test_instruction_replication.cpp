#include "test.hpp"
#include "util/compare_groups.hpp"

#include <realm.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

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
        , path_1(realm::test_util::get_test_path(test_context.get_test_name(), ".path_1.realm"))
        , path_2(realm::test_util::get_test_path(test_context.get_test_name(), ".path_2.realm"))
        , history_1(make_client_replication())
        , history_2(make_client_replication())
        , sg_1(DB::create(*history_1, path_1))
        , sg_2(DB::create(*history_2, path_2))
    {
        // This is to ensure that peer IDs in Object IDs are populated.
        bool fix_up_object_ids = true;
        history_1->get_history().set_client_file_ident({1, 123}, fix_up_object_ids);
    }

    void replay_transactions()
    {
        Changeset result;
        const auto& buffer = history_1->get_instruction_encoder().buffer();
        _impl::SimpleNoCopyInputStream stream{buffer.data(), buffer.size()};
        sync::parse_changeset(stream, result);

        WriteTransaction wt{sg_2};
        InstructionApplier applier{wt};
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
        wt.add_table("class_foo");
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
    }
}

TEST(InstructionReplication_AddColumnTwice)
{
    std::vector<DataType> basic_types = {
        type_Int,   type_Bool,   type_String,  type_Binary,   type_Mixed, type_Timestamp,
        type_Float, type_Double, type_Decimal, type_ObjectId, type_UUID,
    };

    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = wt.add_table("class_types");
        for (auto type : basic_types) {
            foo->add_column(type, util::format("simple_%1", type));
            foo->add_column_list(type, util::format("list_of_%1", type));
            foo->add_column_dictionary(type, util::format("dictionary_of_%1", type));
            foo->add_column_set(type, util::format("set_of_%1", type));
        }
        foo->add_column(*foo, "link");
        foo->add_column(*foo, "linklist");
        foo->add_column_dictionary(*foo, "dictionary_of_links");
        foo->add_column_set(*foo, "set_of_links");
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    // creating same table/columns twice have no effect or error as long as they are the same type
    fixture.replay_transactions();
    fixture.check_equal();
}


TEST(InstructionReplication_EraseTable)
{
    Fixture fixture{test_context};
    {
        auto wt = fixture.sg_1->start_write();
        auto tk = wt->add_table_with_primary_key("class_foo", type_Int, "id")->get_key();
        wt->remove_table(tk);
        wt->commit();
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
        TableRef foo = wt.add_table("class_foo");
        ColKey col_ndx = foo->add_column(type_Int, "i");
        foo->create_object().set(col_ndx, 123);
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
        TableRef foo = wt.get_group().add_table_with_primary_key("class_foo", type_String, "pk", nullable);
        Obj obj = foo->create_object_with_primary_key(StringData{});
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
    Decimal128 large(uint64_t(0x123456789abcdef0));
    large *= Decimal128(0x100000);
    large += Decimal128(0x123);
    Decimal128 large_w0_zero(uint64_t(0x1234500000000000));
    large_w0_zero *= Decimal128(0x1000000);
    large_w0_zero += Decimal128(0x23);
    Fixture fixture{test_context};
    ObjKey key;
    ObjKey key2;
    ObjKey key3;
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = wt.get_group().add_table_with_primary_key("class_foo", type_ObjectId, "_id", false);
        auto col_dec = foo->add_column(type_Decimal, "cost");
        key = foo->create_object_with_primary_key(id).set(col_dec, cost).get_key();
        key2 = foo->create_object_with_primary_key(ObjectId::gen()).set(col_dec, large).get_key();
        key3 = foo->create_object_with_primary_key(ObjectId::gen()).set(col_dec, large_w0_zero).get_key();
        wt.commit();
    }
    fixture.replay_transactions();
    fixture.check_equal();
    {
        ReadTransaction rt{fixture.sg_2};
        CHECK(rt.has_table("class_foo"));
        ConstTableRef foo = rt.get_table("class_foo");
        CHECK_EQUAL(foo->size(), 3);
        ColKey col_ndx = foo->get_column_key("_id");
        ColKey col_dec = foo->get_column_key("cost");
        auto obj = foo->get_object(key);
        CHECK_EQUAL(obj.get<ObjectId>(col_ndx), id);
        CHECK_EQUAL(obj.get<Decimal128>(col_dec), cost);
        obj = foo->get_object(key2);
        CHECK_EQUAL(obj.get<Decimal128>(col_dec), large);
        obj = foo->get_object(key3);
        CHECK_EQUAL(obj.get<Decimal128>(col_dec), large_w0_zero);
    }
}

TEST(InstructionReplication_CreateEmbedded)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef car = wt.get_group().add_table_with_primary_key("class_Car", type_String, "id", false);
        auto wheel = wt.add_embedded_table("class_Wheel");
        auto col_position = wheel->add_column(type_String, "position");
        auto col_wheels = car->add_column_list(*wheel, "wheels");
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
        ColKey col_link = bar->add_column(*foo, "link");

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
        ColKey col_link = bar->add_column(*foo, "link");
        ColKey col_linklist = bar->add_column_list(*foo, "linklist");

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
        TableRef foo = wt.add_table("class_foo");
        TableRef bar = wt.add_table("class_bar");
        ColKey foo_i = foo->add_column(type_Int, "i");
        ColKey bar_l = bar->add_column(*foo, "l");

        auto foo_1 = foo->create_object().set(foo_i, 123).get_key();
        auto foo_2 = foo->create_object().set(foo_i, 456).get_key();

        bar->create_object().set(bar_l, foo_1);
        bar->create_object().set(bar_l, foo_2);


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
        TableRef foo = wt.add_table("class_foo");
        ColKey col_ndx = foo->add_column(type_Int, "i");
        ColKey col_mixed = foo->add_column(type_Mixed, "m");
        auto obj = foo->create_object();
        obj.set(col_mixed, Mixed(100));
        obj.add_int(col_ndx, 123);
        obj.add_int(col_mixed, 42);
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
        ColKey col_mixed = foo->get_column_key("m");
        CHECK_EQUAL(foo->begin()->get<Int>(col_ndx), 123);
        CHECK_EQUAL(foo->begin()->get_any(col_mixed).get_int(), 142);
    }
}

TEST(InstructionReplication_ListSwap)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = wt.add_table("class_foo");
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
        TableRef foo = wt.add_table("class_foo");
        TableRef bar = wt.add_table("class_bar");
        ColKey foo_i = foo->add_column(type_Int, "i");
        ColKey bar_ll = bar->add_column_list(*foo, "ll");

        ObjKey foo_1 = foo->create_object().set(foo_i, 123).get_key();
        ObjKey foo_2 = foo->create_object().set(foo_i, 456).get_key();

        Obj bar_1 = bar->create_object();
        Obj bar_2 = bar->create_object();

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

TEST(InstructionReplication_LinkSets)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        TableRef foo = wt.add_table("class_foo");
        TableRef bar = wt.add_table("class_bar");
        ColKey foo_i = foo->add_column(type_Int, "int");
        ColKey bar_ls = bar->add_column_set(*foo, "link set");

        ObjKey foo_1 = foo->create_object().set(foo_i, 123).get_key();
        ObjKey foo_2 = foo->create_object().set(foo_i, 456).get_key();
        ObjKey foo_3 = foo->create_object().set(foo_i, 789).get_key();

        Obj bar_1 = bar->create_object();
        Obj bar_2 = bar->create_object();

        auto ls1 = bar_1.get_linkset(bar_ls);
        ls1.insert(foo_1);
        ls1.insert(foo_2);
        ls1.insert(foo_1);

        auto ls2 = bar_2.get_linkset(bar_ls);
        ls2.insert(foo_3);
        ls2.insert(foo_1);
        ls2.insert(foo_3);

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
        CHECK_EQUAL(foo->size(), 2);
        CHECK_EQUAL(bar->size(), 2);

        ColKey foo_i = foo->get_column_key("int");
        ColKey bar_ls = bar->get_column_key("link set");

        CHECK_EQUAL(foo->begin()->get<Int>(foo_i), 456);
        Obj bar_1 = *bar->begin();
        Obj bar_2 = *(bar->begin() + 1);
        CHECK_EQUAL(bar_1.get_linkset(bar_ls).size(), 1);
        CHECK_EQUAL(bar_2.get_linkset(bar_ls).size(), 1);
    }
}

TEST(InstructionReplication_NullablePrimaryKeys)
{
    Fixture fixture{test_context};
    {
        WriteTransaction wt{fixture.sg_1};
        bool nullable = true;
        TableRef t = wt.get_group().add_table_with_primary_key("class_t", type_Int, "pk", nullable);
        ColKey col_ndx = t->add_column(type_Int, "i");

        t->create_object_with_primary_key(123).set(col_ndx, 123);
        ObjKey first = t->find_first<util::Optional<int64_t>>(t->get_primary_key_column(), 123);
        REALM_ASSERT(first);

        t->create_object_with_primary_key(util::none).set(col_ndx, 456);
        ObjKey second = t->find_first_null(t->get_primary_key_column());
        REALM_ASSERT(second);

        t->create_object_with_primary_key(789).set(col_ndx, 789);
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

        const Obj first = t->get_object(first_key);
        const Obj second = t->get_object(second_key);
        const Obj third = t->get_object(third_key);

        CHECK_EQUAL(first.get<int64_t>(col_ndx), 123);
        CHECK_EQUAL(second.get<int64_t>(col_ndx), 456);
        CHECK_EQUAL(third.get<int64_t>(col_ndx), 789);
    }
}
