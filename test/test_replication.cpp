#include "testsettings.hpp"
#ifdef TEST_REPLICATION

#include <algorithm>

#include <tightdb.hpp>
#include <tightdb/util/features.h>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/file.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

#include "test.hpp"

#ifdef TIGHTDB_ENABLE_REPLICATION

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;
using unit_test::TestResults;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


namespace {

class MyTrivialReplication: public TrivialReplication {
public:
    MyTrivialReplication(string path): TrivialReplication(path) {}

    ~MyTrivialReplication() TIGHTDB_NOEXCEPT
    {
        typedef TransactLogs::const_iterator iter;
        iter end = m_transact_logs.end();
        for (iter i = m_transact_logs.begin(); i != end; ++i)
            delete[] i->data();
    }

    void replay_transacts(SharedGroup& target, ostream* replay_log = 0)
    {
        typedef TransactLogs::const_iterator iter;
        iter end = m_transact_logs.end();
        for (iter i = m_transact_logs.begin(); i != end; ++i)
            apply_transact_log(i->data(), i->size(), target, replay_log);
        for (iter i = m_transact_logs.begin(); i != end; ++i)
            delete[] i->data();
        m_transact_logs.clear();
    }

private:
    void handle_transact_log(const char* data, size_t size, version_type) TIGHTDB_OVERRIDE
    {
        UniquePtr<char[]> log(new char[size]); // Throws
        copy(data, data+size, log.get());
        m_transact_logs.push_back(BinaryData(log.get(), size)); // Throws
        log.release();
    }

    typedef vector<BinaryData> TransactLogs;
    TransactLogs m_transact_logs;
};

TIGHTDB_TABLE_1(MySubsubsubtable,
                i, Int)

TIGHTDB_TABLE_3(MySubsubtable,
                a, Int,
                b, Subtable<MySubsubsubtable>,
                c, Int)

TIGHTDB_TABLE_1(MySubtable,
                t, Subtable<MySubsubtable>)

TIGHTDB_TABLE_9(MyTable,
                my_int,       Int,
                my_bool,      Bool,
                my_float,     Float,
                my_double,    Double,
                my_string,    String,
                my_binary,    Binary,
                my_date_time, DateTime,
                my_subtable,  Subtable<MySubtable>,
                my_mixed,     Mixed)


TEST(Replication_General)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    MyTrivialReplication repl(path_1);
    SharedGroup sg_1(repl);
    {
        WriteTransaction wt(sg_1);
        MyTable::Ref table = wt.add_table<MyTable>("my_table");
        table->add();
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        MyTable::Ref table = wt.get_table<MyTable>("my_table");
        char buf[] = { '1' };
        BinaryData bin(buf);
        Mixed mix;
        mix.set_int(1);
        table->set    (0, 2, true, 2.0f, 2.0, "xx",  bin, 728, 0, mix);
        table->add    (3, true, 3.0f, 3.0, "xxx", bin, 729, 0, mix);
        table->insert (0, 1, true, 1.0f, 1.0, "x",   bin, 727, 0, mix);

        table->add(3, true, 3.0f, 0.0, "", bin, 729, 0, mix);               // empty string
        table->add(3, true, 3.0f, 1.0, StringData(), bin, 729, 0, mix);     // null string
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        MyTable::Ref table = wt.get_table<MyTable>("my_table");
        table[0].my_int = 9;
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        MyTable::Ref table = wt.get_table<MyTable>("my_table");
        table[0].my_int = 10;
        wt.commit();
    }
    // Test Table::move_last_over()
    {
        WriteTransaction wt(sg_1);
        MyTable::Ref table = wt.get_table<MyTable>("my_table");
        char buf[] = { '9' };
        BinaryData bin(buf);
        Mixed mix;
        mix.set_float(9.0f);
        table->insert (2, 8, false, 8.0f, 8.0, "y8", bin, 282, 0, mix);
        table->insert (1, 9, false, 9.0f, 9.0, "y9", bin, 292, 0, mix);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        MyTable::Ref table = wt.get_table<MyTable>("my_table");
        table->move_last_over(1);
        wt.commit();
    }

    ostream* replay_log = 0;
//    replay_log = &cout;
    SharedGroup sg_2(path_2);
    repl.replay_transacts(sg_2, replay_log);

    {
        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        rt_1.get_group().Verify();
        rt_2.get_group().Verify();
        CHECK(rt_1.get_group() == rt_2.get_group());
        MyTable::ConstRef table = rt_2.get_table<MyTable>("my_table");
        CHECK_EQUAL(6, table->size());
        CHECK_EQUAL(10, table[0].my_int);
        CHECK_EQUAL(3,  table[1].my_int);
        CHECK_EQUAL(2,  table[2].my_int);
        CHECK_EQUAL(8,  table[3].my_int);

        StringData sd1 = table[4].my_string.get();
        StringData sd2 = table[5].my_string.get();

        if (NULLS) {
            CHECK(!table[4].my_string.get().is_null());
            CHECK(table[1].my_string.get().is_null());  // The null was in last row which got move_last_over(1)
        }
    }
}

/*
TEST(Replication_Links)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    MyTrivialReplication repl(path_1);
    SharedGroup sg_1(repl);
    {
        WriteTransaction wt(sg_1);
        TableRef origin = wt.add_table("origin");
        TableRef target = wt.add_table("target");
        origin->add_column_link(type_Link,     "a", *target);
        origin->add_column_link(type_LinkList, "b", *target);
        wt.commit();
    }

    ostream* replay_log = 0;
//    replay_log = &cout;
    SharedGroup sg_2(path_2);
    repl.replay_transacts(sg_2, replay_log);

    {
        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        rt_1.get_group().Verify();
        rt_2.get_group().Verify();
        CHECK(rt_1.get_group() == rt_2.get_group());
        ConstTableRef origin = rt_2.get_table("origin");
        if (CHECK_EQUAL(2, origin->get_column_count())) {
            CHECK_EQUAL(type_Link,     origin->get_column_type(0));
            CHECK_EQUAL(type_LinkList, origin->get_column_type(1));
        }
    }

    {
        WriteTransaction wt(sg_1);
        TableRef origin = wt.get_table("origin");
        TableRef target = wt.get_table("target");
        target->add_column(type_Int, "i");
        origin->add_empty_row(2);
        target->add_empty_row(2);
        origin->set_link(0, 0, 1);
        origin->set_link(0, 1, 0);
        target->set_int(0, 0, 5);
        target->set_int(0, 1, 13);
        wt.commit();
    }

    repl.replay_transacts(sg_2, replay_log);

    {
        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        rt_1.get_group().Verify();
        rt_2.get_group().Verify();
        CHECK(rt_1.get_group() == rt_2.get_group());
    }
}
*/


void check(TestResults& test_results, SharedGroup& sg_1, const ReadTransaction& rt_2)
{
    ReadTransaction rt_1(sg_1);
    rt_1.get_group().Verify();
    rt_2.get_group().Verify();
    CHECK(rt_1.get_group() == rt_2.get_group());
}


TEST(Replication_Links)
{
    // This test checks that all the links-related stuff works through
    // replication. It does that in a chained manner where the output of one
    // test acts as the input of the next one. This is to save boilerplate code,
    // and to make the test scenarios slightly more varied and realistic.
    //
    // The following operations are covered (for cyclic stuff, see
    // Replication_LinkCycles):
    //
    // - add_empty_row to origin table
    // - add_empty_row to target table
    // - insert link + link list
    // - change link
    // - nullify link
    // - insert link into list
    // - remove link from list
    // - move link inside list
    // - clear link list
    // - move_last_over on origin table
    // - move_last_over on target table
    // - clear origin table
    // - clear target table
    // - insert and remove non-link-type columns in origin table
    // - Insert and remove link-type columns in origin table
    // - Insert and remove columns in target table

    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    ostream* replay_log = 0;
//    replay_log = &cout;

    MyTrivialReplication repl(path_1);
    SharedGroup sg_1(repl);
    SharedGroup sg_2(path_2);

    // First create two origin tables and two target tables, and add some links
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.add_table("origin_1");
        TableRef origin_2 = wt.add_table("origin_2");
        TableRef target_1 = wt.add_table("target_1");
        TableRef target_2 = wt.add_table("target_2");
        target_1->add_column(type_Int, "t_1");
        target_2->add_column(type_Int, "t_2");
        target_1->add_empty_row(2);
        target_2->add_empty_row(2);
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_log);
    {
        ReadTransaction rt(sg_2);
        check(test_results, sg_1, rt);
    }
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        TableRef target_1 = wt.get_table("target_1");
        origin_1->add_column_link(type_LinkList, "o_1_ll_1", *target_1);
        origin_2->add_column(type_Int, "o_2_f_1");
        origin_2->add_empty_row(2);
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_log);
    // O_1: LL_1->T_1
    // O_2: F_1
    {
        ReadTransaction rt(sg_2);
        check(test_results, sg_1, rt);
    }
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        TableRef target_1 = wt.get_table("target_1");
        origin_1->insert_column(0, type_Int, "o_1_f_2");
        origin_2->insert_column_link(0, type_Link, "o_2_l_2", *target_1);
        origin_2->set_link(0, 0, 1); // O_2_L_2[0] -> T_1[1]
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_log);
    // O_1: F_2   LL_1->T_1
    // O_2: L_2->T_1   F_1
    {
        ReadTransaction rt(sg_2);
        check(test_results, sg_1, rt);
    }
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        TableRef target_1 = wt.get_table("target_1");
        TableRef target_2 = wt.get_table("target_2");
        origin_1->insert_column_link(0, type_Link, "o_1_l_3", *target_1);
        origin_2->add_column_link(type_LinkList, "o_2_ll_3", *target_2);
        origin_2->get_linklist(2, 0)->add(1); // O_2_LL_3[0] -> T_2[1]
        origin_2->get_linklist(2, 1)->add(0); // O_2_LL_3[1] -> T_2[0]
        origin_2->get_linklist(2, 1)->add(1); // O_2_LL_3[1] -> T_2[1]
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_log);
    // O_1: L_3->T_1   F_2   LL_1->T_1
    // O_2: L_2->T_1   F_1   LL_3->T_2
    {
        ReadTransaction rt(sg_2);
        check(test_results, sg_1, rt);
    }
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        TableRef target_2 = wt.get_table("target_2");
        origin_1->insert_column_link(2, type_Link, "o_1_l_4", *target_2);
        origin_2->add_column_link(type_Link, "o_2_l_4", *target_2);
        origin_2->set_link(3, 0, 1); // O_2_L_4[0] -> T_2[1]
        origin_2->set_link(3, 1, 0); // O_2_L_4[1] -> T_2[0]
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_log);
    // O_1: L_3->T_1   F_2   L_4->T_2   LL_1->T_1
    // O_2: L_2->T_1   F_1   LL_3->T_2   L_4->T_2
    {
        ReadTransaction rt(sg_2);
        check(test_results, sg_1, rt);
    }
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        TableRef target_1 = wt.get_table("target_1");
        TableRef target_2 = wt.get_table("target_2");
        origin_1->insert_column(3, type_Int, "o_1_f_5");
        origin_2->insert_column(3, type_Int, "o_2_f_5");
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_log);
    // O_1: L_3->T_1   F_2   L_4->T_2   F_5   LL_1->T_1
    // O_2: L_2->T_1   F_1   LL_3->T_2   F_5   L_4->T_2
    {
        ReadTransaction rt(sg_2);
        check(test_results, sg_1, rt);
    }
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        origin_1->add_empty_row(2);
        origin_1->set_link(0, 1, 0); // O_1_L_3[1] -> T_1[0]
        origin_1->set_link(2, 0, 0); // O_1_L_4[0] -> T_2[0]
        origin_1->set_link(2, 1, 1); // O_1_L_4[1] -> T_2[1]
        origin_1->get_linklist(4, 1)->add(0); // O_1_LL_1[1] -> T_1[0]
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_log);
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    {
        ReadTransaction rt(sg_2);
        check(test_results, sg_1, rt);
        CHECK_EQUAL(4, rt.get_group().size());
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");
        CHECK(origin_1->is_attached());
        CHECK(origin_2->is_attached());
        CHECK(target_1->is_attached());
        CHECK(target_2->is_attached());
        CHECK_EQUAL(2, origin_1->size());
        CHECK_EQUAL(2, origin_2->size());
        CHECK_EQUAL(2, target_1->size());
        CHECK_EQUAL(2, target_2->size());
        CHECK_EQUAL(5, origin_1->get_column_count());
        CHECK_EQUAL(5, origin_2->get_column_count());
        CHECK_EQUAL(1, target_1->get_column_count());
        CHECK_EQUAL(1, target_2->get_column_count());
        CHECK_EQUAL(type_Link,     origin_1->get_column_type(0));
        CHECK_EQUAL(type_Int,      origin_1->get_column_type(1));
        CHECK_EQUAL(type_Link,     origin_1->get_column_type(2));
        CHECK_EQUAL(type_Int,      origin_1->get_column_type(3));
        CHECK_EQUAL(type_LinkList, origin_1->get_column_type(4));
        CHECK_EQUAL(type_Link,     origin_2->get_column_type(0));
        CHECK_EQUAL(type_Int,      origin_2->get_column_type(1));
        CHECK_EQUAL(type_LinkList, origin_2->get_column_type(2));
        CHECK_EQUAL(type_Int,      origin_2->get_column_type(3));
        CHECK_EQUAL(type_Link,     origin_2->get_column_type(4));
        CHECK_EQUAL(target_1, origin_1->get_link_target(0));
        CHECK_EQUAL(target_2, origin_1->get_link_target(2));
        CHECK_EQUAL(target_1, origin_1->get_link_target(4));
        CHECK_EQUAL(target_1, origin_2->get_link_target(0));
        CHECK_EQUAL(target_2, origin_2->get_link_target(2));
        CHECK_EQUAL(target_2, origin_2->get_link_target(4));
        CHECK(origin_1->is_null_link(0,0));
        CHECK_EQUAL(0, origin_1->get_link(0,1));
        CHECK_EQUAL(0, origin_1->get_link(2,0));
        CHECK_EQUAL(1, origin_1->get_link(2,1));
        CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
        CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
        CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
        CHECK_EQUAL(1, origin_2->get_link(0,0));
        CHECK(origin_2->is_null_link(0,1));
        CHECK_EQUAL(1, origin_2->get_linklist(2,0)->size());
        CHECK_EQUAL(1, origin_2->get_linklist(2,0)->get(0).get_index());
        CHECK_EQUAL(2, origin_2->get_linklist(2,1)->size());
        CHECK_EQUAL(0, origin_2->get_linklist(2,1)->get(0).get_index());
        CHECK_EQUAL(1, origin_2->get_linklist(2,1)->get(1).get_index());
        CHECK_EQUAL(1, origin_2->get_link(4,0));
        CHECK_EQUAL(0, origin_2->get_link(4,1));
        CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
        CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
        CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
        CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 0));
        CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
        CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
        CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
        CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
        CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
        CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
        CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
        CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    }

    // FIXME: Reproduce the rest of the subtests from
    // LangBindHelper_AdvanceReadTransact_Links.
}

} // anonymous namespace

#endif // TIGHTDB_ENABLE_REPLICATION
#endif // TEST_REPLICATION
