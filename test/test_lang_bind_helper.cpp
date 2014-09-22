#include <map>
#include <sstream>

#include "testsettings.hpp"
#ifdef TEST_LANG_BIND_HELPER

#include <tightdb/descriptor.hpp>
#include <tightdb/table_macros.hpp>
#include <tightdb/lang_bind_helper.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#  include <tightdb/commit_log.hpp>
#  include <tightdb/util/bind.hpp>
#endif

#include "test.hpp"

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


TEST(LangBindHelper_InsertSubtable)
{
    Table t1;
    DescriptorRef s;
    t1.add_column(type_Table, "sub", &s);
    s->add_column(type_Int, "i1");
    s->add_column(type_Int, "i2");
    s.reset();

    Table t2;
    t2.add_column(type_Int, "i1");
    t2.add_column(type_Int, "i2");
    t2.insert_int(0, 0, 10);
    t2.insert_int(1, 0, 120);
    t2.insert_done();
    t2.insert_int(0, 1, 12);
    t2.insert_int(1, 1, 100);
    t2.insert_done();

    LangBindHelper::insert_subtable(t1, 0, 0, t2);
    t1.insert_done();

    TableRef sub = t1.get_subtable(0, 0);

    CHECK_EQUAL(t2.get_column_count(), sub->get_column_count());
    CHECK_EQUAL(t2.size(), sub->size());
    CHECK(t2 == *sub);
}


// FIXME: Move this test to test_table.cpp
TEST(LangBindHelper_SetSubtable)
{
    Table t1;
    DescriptorRef s;
    t1.add_column(type_Table, "sub", &s);
    s->add_column(type_Int, "i1");
    s->add_column(type_Int, "i2");
    s.reset();
    t1.add_empty_row();

    Table t2;
    t2.add_column(type_Int, "i1");
    t2.add_column(type_Int, "i2");
    t2.insert_int(0, 0, 10);
    t2.insert_int(1, 0, 120);
    t2.insert_done();
    t2.insert_int(0, 1, 12);
    t2.insert_int(1, 1, 100);
    t2.insert_done();

    t1.set_subtable( 0, 0, &t2);

    TableRef sub = t1.get_subtable(0, 0);

    CHECK_EQUAL(t2.get_column_count(), sub->get_column_count());
    CHECK_EQUAL(t2.size(), sub->size());
    CHECK(t2 == *sub);
}


TEST(LangBindHelper_LinkView)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    origin->add_empty_row();
    target->add_empty_row();
    Row row = origin->get(0);
    LinkView* link_view = LangBindHelper::get_linklist_ptr(row, 0);
    link_view->add(0);
    LangBindHelper::unbind_linklist_ptr(link_view);
    CHECK_EQUAL(1, origin->get_link_count(0,0));
}


#ifdef TIGHTDB_ENABLE_REPLICATION

namespace {

TIGHTDB_TABLE_4(TestTableShared,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, String)

TIGHTDB_TABLE_1(TestTableInts,
                first,  Int)

class ShortCircuitTransactLogManager:
        public TrivialReplication,
        public LangBindHelper::TransactLogRegistry {
public:
    typedef Replication::version_type version_type;

    ShortCircuitTransactLogManager(const string& database_file):
        TrivialReplication(database_file)
    {
    }

    ~ShortCircuitTransactLogManager() TIGHTDB_NOEXCEPT
    {
        typedef TransactLogs::const_iterator iter;
        iter end = m_transact_logs.end();
        for (iter i = m_transact_logs.begin(); i != end; ++i)
            delete[] i->second.data();
    }

    void handle_transact_log(const char* data, size_t size, Replication::version_type new_version)
        TIGHTDB_OVERRIDE
    {
        UniquePtr<char[]> log(new char[size]); // Throws
        copy(data, data+size, log.get());
        m_transact_logs[new_version] = BinaryData(log.get(), size); // Throws
        log.release();
    }

    void get_commit_entries(uint_fast64_t from_version, uint_fast64_t to_version, BinaryData* logs_buffer)
        TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        size_t n = to_version - from_version;
        for (size_t i = 0; i != n; ++i) {
            uint_fast64_t version = from_version + i + 1;
            logs_buffer[i] = m_transact_logs[version];
        }
    }

    void release_commit_entries(uint_fast64_t) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
    }

private:
    typedef map<uint_fast64_t, BinaryData> TransactLogs;
    TransactLogs m_transact_logs;
};

} // anonymous namespace


TEST(LangBindHelper_AdvanceReadTransact_Basics)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Try to advance without anything having happened
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Try to advance after an empty write transaction
    {
        WriteTransaction wt(sg_w);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Try to advance after a superfluous rollback
    {
        WriteTransaction wt(sg_w);
        // Implicit rollback
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Try to advance after a propper rollback
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("bad");
        // Implicit rollback
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Create a table via the other SharedGroup
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("foo");
        foo_w->add_column(type_Int, "i");
        foo_w->add_empty_row();
        wt.commit();
    }

    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, group.size());
    ConstTableRef foo = group.get_table("foo");
    CHECK_EQUAL(1, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(1, foo->size());
    CHECK_EQUAL(0, foo->get_int(0,0));


    // Modify the table via the other SharedGroup
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("foo");
        foo_w->add_column(type_String, "s");
        foo_w->add_empty_row();
        foo_w->set_int(0, 0, 1);
        foo_w->set_int(0, 1, 2);
        foo_w->set_string(1, 0, "a");
        foo_w->set_string(1, 1, "b");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, foo->get_int(0,0));
    CHECK_EQUAL(2, foo->get_int(0,1));
    CHECK_EQUAL("a", foo->get_string(1,0));
    CHECK_EQUAL("b", foo->get_string(1,1));
    CHECK_EQUAL(foo, group.get_table("foo"));

    // Again, with no change
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, foo->get_int(0,0));
    CHECK_EQUAL(2, foo->get_int(0,1));
    CHECK_EQUAL("a", foo->get_string(1,0));
    CHECK_EQUAL("b", foo->get_string(1,1));
    CHECK_EQUAL(foo, group.get_table("foo"));

    // Perform several write transactions before advancing the read transaction
    {
        WriteTransaction wt(sg_w);
        TableRef bar_w = wt.add_table("bar");
        bar_w->add_column(type_Int, "a");
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        TableRef bar_w = wt.get_table("bar");
        bar_w->add_column(type_Float, "b");
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        // Implicit rollback
    }
    {
        WriteTransaction wt(sg_w);
        TableRef bar_w = wt.get_table("bar");
        bar_w->add_column(type_Double, "c");
        wt.commit();
    }

    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, group.size());
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, foo->get_int(0,0));
    CHECK_EQUAL(2, foo->get_int(0,1));
    CHECK_EQUAL("a", foo->get_string(1,0));
    CHECK_EQUAL("b", foo->get_string(1,1));
    CHECK_EQUAL(foo, group.get_table("foo"));
    ConstTableRef bar = group.get_table("bar");
    CHECK_EQUAL(3, bar->get_column_count());
    CHECK_EQUAL(type_Int,    bar->get_column_type(0));
    CHECK_EQUAL(type_Float,  bar->get_column_type(1));
    CHECK_EQUAL(type_Double, bar->get_column_type(2));

    // Clear tables
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("foo");
        foo_w->clear();
        TableRef bar_w = wt.get_table("bar");
        bar_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, group.size());
    CHECK(foo->is_attached());
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(0, foo->size());
    CHECK(bar->is_attached());
    CHECK_EQUAL(3, bar->get_column_count());
    CHECK_EQUAL(type_Int,    bar->get_column_type(0));
    CHECK_EQUAL(type_Float,  bar->get_column_type(1));
    CHECK_EQUAL(type_Double, bar->get_column_type(2));
    CHECK_EQUAL(0, bar->size());
    CHECK_EQUAL(foo, group.get_table("foo"));
    CHECK_EQUAL(bar, group.get_table("bar"));
}


TEST(LangBindHelper_AdvanceReadTransact_LinkListSort)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create a table via the other SharedGroup
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("foo");
        foo_w->add_column(type_Int, "i");
        foo_w->add_empty_row();
        wt.commit();
    }

    // Verify that sorting a LinkList works
    size_t link_col;
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("links");
        link_col = foo_w->add_column_link(type_LinkList, "links", *foo_w); // just link to self
        size_t val_col = foo_w->add_column(type_Int, "vals"); // just link to self
        foo_w->add_empty_row(4);
        foo_w->set_int(val_col, 0, 40);
        foo_w->set_int(val_col, 1, 20);
        foo_w->set_int(val_col, 2, 10);
        foo_w->set_int(val_col, 3, 30);
        LinkViewRef lvr = foo_w->get_linklist(link_col, 0);
        lvr->add(0);
        lvr->add(1);
        lvr->add(2);
        lvr->add(3);
        lvr->sort(val_col); // sort such that they links become 2, 1, 3, 0
        wt.commit();
    }

    LangBindHelper::advance_read(sg, tlm);

    // Verify sorted LinkList (see above)
    ConstTableRef linktable = group.get_table("links");
    ConstLinkViewRef lvr = linktable->get_linklist(link_col, 0);
    CHECK_EQUAL(2, lvr->get(0).get_index());
    CHECK_EQUAL(1, lvr->get(1).get_index());
    CHECK_EQUAL(3, lvr->get(2).get_index());
    CHECK_EQUAL(0, lvr->get(3).get_index());
}

TEST(LangBindHelper_AdvanceReadTransact_ColumnRootTypeChange)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create a table for strings and one for other types
    {
        WriteTransaction wt(sg_w);
        TableRef strings_w = wt.add_table("strings");
        strings_w->add_column(type_String, "a");
        strings_w->add_column(type_Binary, "b");
        strings_w->add_column(type_Mixed,  "c"); // Strings
        strings_w->add_column(type_Mixed,  "d"); // Binary data
        strings_w->add_empty_row();
        TableRef other_w = wt.add_table("other");
        other_w->add_column(type_Int,   "A");
        other_w->add_column(type_Float, "B");
        other_w->add_column(type_Table, "C");
        other_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, group.size());
    ConstTableRef strings = group.get_table("strings");
    CHECK(strings->is_attached());
    CHECK_EQUAL(4, strings->get_column_count());
    CHECK_EQUAL(type_String, strings->get_column_type(0));
    CHECK_EQUAL(type_Binary, strings->get_column_type(1));
    CHECK_EQUAL(type_Mixed,  strings->get_column_type(2));
    CHECK_EQUAL(type_Mixed,  strings->get_column_type(3));
    CHECK_EQUAL(1, strings->size());
    ConstTableRef other = group.get_table("other");
    CHECK(other->is_attached());
    CHECK_EQUAL(3, other->get_column_count());
    CHECK_EQUAL(type_Int,   other->get_column_type(0));
    CHECK_EQUAL(type_Float, other->get_column_type(1));
    CHECK_EQUAL(type_Table, other->get_column_type(2));
    CHECK_EQUAL(1, other->size());

    size_t leaf_x4    = 4 * TIGHTDB_MAX_BPNODE_SIZE;
    size_t leaf_x4p16 = leaf_x4 + 16;

    // Change root type in various string columns (including mixed)
    struct Step { size_t m_str_size, m_num_rows; };
    Step steps[] = {
        // 1->max->1
        { 1,    1 }, { 8191, 1 }, { 1,    1 },
        // rising, falling
        { 3,    1 }, { 7,    1 }, { 11,   1 }, { 15,   1 }, { 23,   1 }, { 31,   1 }, { 47,   1 },
        { 63,   1 }, { 95,   1 }, { 127,  1 }, { 191,  1 }, { 255,  1 }, { 383,  1 }, { 511,  1 },
        { 767,  1 }, { 1023, 1 }, { 1535, 1 }, { 2047, 1 }, { 3071, 1 }, { 4095, 1 }, { 6143, 1 },
        { 8191, 1 }, { 6143, 1 }, { 4095, 1 }, { 3071, 1 }, { 2047, 1 }, { 1535, 1 }, { 1023, 1 },
        { 767,  1 }, { 511,  1 }, { 383,  1 }, { 255,  1 }, { 191,  1 }, { 127,  1 }, { 95,   1 },
        { 63,   1 }, { 47,   1 }, { 31,   1 }, { 23,   1 }, { 15,   1 }, { 11,   1 }, { 7,    1 },
        { 3,    1 }, { 1,    1 },
        // rising -> inner node -> rising
        { 0, leaf_x4 }, { 3,    1 }, { 0, leaf_x4 }, { 7,    1 }, { 0, leaf_x4 }, { 11,   1 },
        { 0, leaf_x4 }, { 15,   1 }, { 0, leaf_x4 }, { 23,   1 }, { 0, leaf_x4 }, { 31,   1 },
        { 0, leaf_x4 }, { 47,   1 }, { 0, leaf_x4 }, { 63,   1 }, { 0, leaf_x4 }, { 95,   1 },
        { 0, leaf_x4 }, { 127,  1 }, { 0, leaf_x4 }, { 191,  1 }, { 0, leaf_x4 }, { 255,  1 },
        { 0, leaf_x4 }, { 383,  1 }, { 0, leaf_x4 }, { 511,  1 }, { 0, leaf_x4 }, { 767,  1 },
        { 0, leaf_x4 }, { 1023, 1 }, { 0, leaf_x4 }, { 1535, 1 }, { 0, leaf_x4 }, { 2047, 1 },
        { 0, leaf_x4 }, { 3071, 1 }, { 0, leaf_x4 }, { 4095, 1 }, { 0, leaf_x4 }, { 6143, 1 },
        { 0, leaf_x4 }, { 8191, 1 }
    };
    ostringstream out;
    out << left;

    for (size_t i = 0; i < sizeof steps / sizeof *steps; ++i) {
        Step step = steps[i];
        out.str("");
        out << setfill('x') << setw(int(step.m_str_size)) << "A";
        string str_1 = out.str();
        StringData str(str_1);
        out.str("");
        out << setfill('x') << setw(int(step.m_str_size)) << "B";
        string str_2 = out.str();
        BinaryData bin(str_2);
        out.str("");
        out << setfill('x') << setw(int(step.m_str_size)) << "C";
        string str_3 = out.str();
        StringData str_mix(str_3);
        out.str("");
        out << setfill('x') << setw(int(step.m_str_size)) << "D";
        string str_4 = out.str();
        BinaryData bin_mix(str_4);
        {
            WriteTransaction wt(sg_w);
            TableRef strings_w = wt.get_table("strings");
            if (step.m_num_rows > strings_w->size()) {
                strings_w->add_empty_row(step.m_num_rows - strings_w->size());
            }
            else if (step.m_num_rows < strings_w->size()) {
                strings_w->clear();
                strings_w->add_empty_row(step.m_num_rows);
            }
            strings_w->set_string (0, 0, str);
            strings_w->set_binary (1, 0, bin);
            strings_w->set_mixed  (2, 0, str_mix);
            strings_w->set_mixed  (3, 0, bin_mix);
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK_EQUAL(2, group.size());
        CHECK(strings->is_attached());
        CHECK_EQUAL(4, strings->get_column_count());
        CHECK_EQUAL(type_String, strings->get_column_type(0));
        CHECK_EQUAL(type_Binary, strings->get_column_type(1));
        CHECK_EQUAL(type_Mixed,  strings->get_column_type(2));
        CHECK_EQUAL(type_Mixed,  strings->get_column_type(3));
        CHECK_EQUAL(step.m_num_rows, strings->size());
        CHECK_EQUAL(str,     strings->get_string (0,0));
        CHECK_EQUAL(bin,     strings->get_binary (1,0));
        CHECK_EQUAL(str_mix, strings->get_mixed  (2,0));
        CHECK_EQUAL(bin_mix, strings->get_mixed  (3,0));
        if (step.m_num_rows >= 2) {
            CHECK_EQUAL(StringData(), strings->get_string (0,1));
            CHECK_EQUAL(BinaryData(), strings->get_binary (1,1));
            CHECK_EQUAL(int64_t(),    strings->get_mixed  (2,1));
            CHECK_EQUAL(int64_t(),    strings->get_mixed  (3,1));
        }
    }

    // Change root type from leaf to inner node in non-string columns
    CHECK_EQUAL(2, group.size());
    CHECK(other->is_attached());
    CHECK_EQUAL(3, other->get_column_count());
    CHECK_EQUAL(type_Int,   other->get_column_type(0));
    CHECK_EQUAL(type_Float, other->get_column_type(1));
    CHECK_EQUAL(type_Table, other->get_column_type(2));
    CHECK_EQUAL(1, other->size());
    {
        WriteTransaction wt(sg_w);
        TableRef other_w = wt.get_table("other");
        other_w->add_empty_row(leaf_x4p16 - 1);
        other_w->set_int      (0, (leaf_x4p16-16)/3+1, 7);
        other_w->set_float    (1, (leaf_x4p16-16)/3+2, 13.0f);
        other_w->set_subtable (2, (leaf_x4p16-16)/3+3, 0); // FIXME: Set something
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, group.size());
    CHECK(other->is_attached());
    CHECK_EQUAL(3, other->get_column_count());
    CHECK_EQUAL(type_Int,   other->get_column_type(0));
    CHECK_EQUAL(type_Float, other->get_column_type(1));
    CHECK_EQUAL(type_Table, other->get_column_type(2));
    CHECK_EQUAL(leaf_x4p16, other->size());
    CHECK_EQUAL(0,     other->get_int      (0, (leaf_x4p16-16)/3+0));
    CHECK_EQUAL(0.0f,  other->get_float    (1, (leaf_x4p16-16)/3+1));
//    CHECK_EQUAL(???,   other->get_subtable (2, (leaf_x4p16-16)/3+2));
    CHECK_EQUAL(7,     other->get_int      (0, (leaf_x4p16-16)/3+1));
    CHECK_EQUAL(13.0f, other->get_float    (1, (leaf_x4p16-16)/3+2));
//    CHECK_EQUAL(???,   other->get_subtable (2, (leaf_x4p16-16)/3+3));
    CHECK_EQUAL(0,     other->get_int      (0, (leaf_x4p16-16)/3+2));
    CHECK_EQUAL(0.0f,  other->get_float    (1, (leaf_x4p16-16)/3+3));
//    CHECK_EQUAL(???,   other->get_subtable (2, (leaf_x4p16-16)/3+4));

    // Change root type from inner node to leaf in non-string columns
    {
        WriteTransaction wt(sg_w);
        TableRef other_w = wt.get_table("other");
        other_w->clear();
        other_w->add_empty_row(1);
        other_w->set_int      (0, 0, 9);
        other_w->set_float    (1, 0, 17.0f);
        other_w->set_subtable (2, 0, 0); // FIXME: Set something
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, group.size());
    CHECK(other->is_attached());
    CHECK_EQUAL(3, other->get_column_count());
    CHECK_EQUAL(type_Int,   other->get_column_type(0));
    CHECK_EQUAL(type_Float, other->get_column_type(1));
    CHECK_EQUAL(type_Table, other->get_column_type(2));
    CHECK_EQUAL(1, other->size());
    CHECK_EQUAL(9,     other->get_int      (0,0));
    CHECK_EQUAL(17.0f, other->get_float    (1,0));
//    CHECK_EQUAL(???,   other->get_subtable (2,0));
}


TEST(LangBindHelper_AdvanceReadTransact_MixedColumn)
{
    // FIXME: Exercise the mixed column
}


TEST(LangBindHelper_AdvanceReadTransact_EnumeratedStrings)
{
    // FIXME: Check introduction and modification of enumerated strings column
}


TEST(LangBindHelper_AdvanceReadTransact_SearchIndex)
{
    // FIXME: Check introduction and modification of search index
    // FIXME: Check that it is correctly moved when columns are inserted or removed at lower column index.
}


TEST(LangBindHelper_AdvanceReadTransact_RegularSubtables)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create one degenerate subtable
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.add_table("parent");
        DescriptorRef subdesc;
        parent_w->add_column(type_Table, "a", &subdesc);
        subdesc->add_column(type_Int, "x");
        parent_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, group.size());
    ConstTableRef parent = group.get_table("parent");
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(1, parent->size());
    ConstTableRef subtab_0_0 = parent->get_subtable(0,0);
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_0->size());

    // Expand to 4 subtables in a 2-by-2 parent.
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        DescriptorRef subdesc;
        parent_w->add_column(type_Table, "b", &subdesc);
        subdesc->add_column(type_Int, "x");
        parent_w->add_empty_row();
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    ConstTableRef subtab_0_1 = parent->get_subtable(0,1);
    CHECK_EQUAL(1, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_1->size());
    ConstTableRef subtab_1_0 = parent->get_subtable(1,0);
    CHECK_EQUAL(1, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_0->size());
    ConstTableRef subtab_1_1 = parent->get_subtable(1,1);
    CHECK_EQUAL(1, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_1->size());

    // Check that subtables get their specs correctly updated
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        DescriptorRef subdesc;
        subdesc = parent_w->get_subdescriptor(0);
        subdesc->add_column(type_Float, "f");
        subdesc = parent_w->get_subdescriptor(1);
        subdesc->add_column(type_Double, "d");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_0->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_1->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_1->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_1->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_0->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_0->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_1->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_1->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_1->get_column_name(1));

    // Check that cell changes in subtables are visible
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_empty_row();
        subtab_0_0_w->set_int    (0, 0, 10000);
        subtab_0_0_w->set_float  (1, 0, 10010.0f);
        subtab_1_1_w->set_int    (0, 0, 11100);
        subtab_1_1_w->set_double (1, 0, 11110.0);
        parent_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(3, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10000,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10010.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11100,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11110.0,  subtab_1_1->get_double (1,0));

    // Insert a row and a column before all the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_column(0, type_Table, "dummy_1");
        parent_w->insert_empty_row(0);
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(2,2);
        subtab_0_0_w->set_int    (0, 0, 10001);
        subtab_0_0_w->set_float  (1, 0, 10011.0f);
        subtab_1_1_w->set_int    (0, 0, 11101);
        subtab_1_1_w->set_double (1, 0, 11111.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(4, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10001,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10011.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11101,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11111.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Insert a row and a column between the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_column(2, type_Int, "dummy_2");
        parent_w->insert_empty_row(2);
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(3,3);
        subtab_0_0_w->set_int    (0, 0, 10002);
        subtab_0_0_w->set_float  (1, 0, 10012.0f);
        subtab_1_1_w->set_int    (0, 0, 11102);
        subtab_1_1_w->set_double (1, 0, 11112.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10002,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10012.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11102,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11112.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Insert a column after the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_column(4, type_Table, "dummy_3");
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(3,3);
        subtab_0_0_w->set_int    (0, 0, 10003);
        subtab_0_0_w->set_float  (1, 0, 10013.0f);
        subtab_1_1_w->set_int    (0, 0, 11103);
        subtab_1_1_w->set_double (1, 0, 11113.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(type_Table, parent->get_column_type(4));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10003,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10013.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11103,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11113.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Remove the row and the column between the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(2);
        parent_w->remove(2);
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(2,2);
        subtab_0_0_w->set_int    (0, 0, 10004);
        subtab_0_0_w->set_float  (1, 0, 10014.0f);
        subtab_1_1_w->set_int    (0, 0, 11104);
        subtab_1_1_w->set_double (1, 0, 11114.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(4, parent->size());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10004,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10014.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11104,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11114.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Remove the row and the column before the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(0);
        parent_w->remove(0);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_0_0_w->set_int    (0, 0, 10005);
        subtab_0_0_w->set_float  (1, 0, 10015.0f);
        subtab_1_1_w->set_int    (0, 0, 11105);
        subtab_1_1_w->set_double (1, 0, 11115.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(3, parent->size());
    CHECK_EQUAL(10005,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10015.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11105,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11115.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Remove the row and the column after the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(2);
        parent_w->remove(2);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_0_0_w->set_int    (0, 0, 10006);
        subtab_0_0_w->set_float  (1, 0, 10016.0f);
        subtab_1_1_w->set_int    (0, 0, 11106);
        subtab_1_1_w->set_double (1, 0, 11116.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK_EQUAL(10006,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10016.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11106,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11116.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Check that subtable accessors are detached when the subtables are removed
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(1);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->set_int   (0, 0, 10007);
        subtab_0_0_w->set_float (1, 0, 10017.0f);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK( subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10007,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10017.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(1);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->set_int   (0, 0, 10008);
        subtab_0_0_w->set_float (1, 0, 10018.0f);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10008,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10018.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));

    // Clear subtable
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear_subtable(0,0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(0, subtab_0_0->size());
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));

    // Clear parent table
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());

    // Insert 4 new subtables, then remove some of them in a different way
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        DescriptorRef subdesc;
        parent_w->add_column(type_Table, "c", &subdesc);
        subdesc->add_column(type_String, "x");
        parent_w->add_empty_row(2);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_empty_row();
        subtab_1_1_w->set_string(0, 0, "pneumonoultramicroscopicsilicovolcanoconiosis");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(2, parent->size());
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(0, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(0);
        parent_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    subtab_1_1 = parent->get_subtable(0,0);
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK( subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));

    // Insert 2x2 new subtables, then remove them all together
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        DescriptorRef subdesc;
        parent_w->add_column(type_Table, "d", &subdesc);
        subdesc->add_column(type_String, "x");
        parent_w->add_empty_row(2);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_empty_row();
        subtab_1_1_w->set_string(0, 0, "supercalifragilisticexpialidocious");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last row
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_empty_row(1);
        parent_w->remove_column(0);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_empty_row(1);
        subtab_0_0_w->set_string(0, 0, "brahmaputra");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    subtab_0_0 = parent->get_subtable(0,0);
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("brahmaputra", subtab_0_0->get_string(0,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last column
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_empty_row(1);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_empty_row(1);
        subtab_0_0_w->set_string(0, 0, "baikonur");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    subtab_0_0 = parent->get_subtable(0,0);
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("baikonur", subtab_0_0->get_string(0,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_MixedSubtables)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create one degenerate subtable
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.add_table("parent");
        parent_w->add_column(type_Mixed, "a");
        parent_w->add_empty_row();
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_column(type_Int, "x");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, group.size());
    ConstTableRef parent = group.get_table("parent");
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(1, parent->size());
    ConstTableRef subtab_0_0 = parent->get_subtable(0,0);
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_0->size());

    // Expand to 4 subtables in a 2-by-2 parent.
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_empty_row();
        parent_w->add_column(type_Mixed, "b");
        parent_w->set_mixed(1, 0, Mixed::subtable_tag());
        TableRef subtab_1_0_w = parent_w->get_subtable(1,0);
        subtab_1_0_w->add_column(type_Int, "x");
        parent_w->add_empty_row();
        parent_w->set_mixed(0, 1, Mixed::subtable_tag());
        TableRef subtab_0_1_w = parent_w->get_subtable(0,1);
        subtab_0_1_w->add_column(type_Int, "x");
        parent_w->set_mixed(1, 1, Mixed::subtable_tag());
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_column(type_Int, "x");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    ConstTableRef subtab_0_1 = parent->get_subtable(0,1);
    CHECK_EQUAL(1, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_1->size());
    ConstTableRef subtab_1_0 = parent->get_subtable(1,0);
    CHECK_EQUAL(1, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_0->size());
    ConstTableRef subtab_1_1 = parent->get_subtable(1,1);
    CHECK_EQUAL(1, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_1->size());

    // Check that subtables get their specs correctly updated
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_column(type_Float, "f");
        TableRef subtab_0_1_w = parent_w->get_subtable(0,1);
        subtab_0_1_w->add_column(type_Float, "f");
        TableRef subtab_1_0_w = parent_w->get_subtable(1,0);
        subtab_1_0_w->add_column(type_Double, "d");
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_column(type_Double, "d");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_0->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_1->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_1->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_1->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_0->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_0->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_1->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_1->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_1->get_column_name(1));

    // Check that cell changes in subtables are visible
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_empty_row();
        subtab_0_0_w->set_int    (0, 0, 10000);
        subtab_0_0_w->set_float  (1, 0, 10010.0f);
        subtab_1_1_w->set_int    (0, 0, 11100);
        subtab_1_1_w->set_double (1, 0, 11110.0);
        parent_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(3, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10000,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10010.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11100,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11110.0,  subtab_1_1->get_double (1,0));

    // Insert a row and a column before all the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_column(0, type_Table, "dummy_1");
        parent_w->insert_empty_row(0);
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(2,2);
        subtab_0_0_w->set_int    (0, 0, 10001);
        subtab_0_0_w->set_float  (1, 0, 10011.0f);
        subtab_1_1_w->set_int    (0, 0, 11101);
        subtab_1_1_w->set_double (1, 0, 11111.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(2));
    CHECK_EQUAL(4, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10001,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10011.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11101,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11111.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Insert a row and a column between the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_column(2, type_Int, "dummy_2");
        parent_w->insert_empty_row(2);
        parent_w->set_mixed(3, 2, "Lopadotemachoselachogaleokranioleipsanodrimhypotrimmatosilphio"
                            "paraomelitokatakechy­menokichlepikossyphophattoperisteralektryonopte"
                            "kephalliokigklopeleiolagoiosiraiobaphetraganopterygon");
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(3,3);
        subtab_0_0_w->set_int    (0, 0, 10002);
        subtab_0_0_w->set_float  (1, 0, 10012.0f);
        subtab_1_1_w->set_int    (0, 0, 11102);
        subtab_1_1_w->set_double (1, 0, 11112.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(3));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10002,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10012.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11102,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11112.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Insert a column after the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_column(4, type_Table, "dummy_3");
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(3,3);
        subtab_0_0_w->set_int    (0, 0, 10003);
        subtab_0_0_w->set_float  (1, 0, 10013.0f);
        subtab_1_1_w->set_int    (0, 0, 11103);
        subtab_1_1_w->set_double (1, 0, 11113.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(3));
    CHECK_EQUAL(type_Table, parent->get_column_type(4));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10003,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10013.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11103,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11113.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Remove the row and the column between the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(2);
        parent_w->remove(2);
        TableRef subtab_0_0_w = parent_w->get_subtable(1,1);
        TableRef subtab_1_1_w = parent_w->get_subtable(2,2);
        subtab_0_0_w->set_int    (0, 0, 10004);
        subtab_0_0_w->set_float  (1, 0, 10014.0f);
        subtab_1_1_w->set_int    (0, 0, 11104);
        subtab_1_1_w->set_double (1, 0, 11114.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(4, parent->size());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10004,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10014.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11104,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11114.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Remove the row and the column before the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(0);
        parent_w->remove(0);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_0_0_w->set_int    (0, 0, 10005);
        subtab_0_0_w->set_float  (1, 0, 10015.0f);
        subtab_1_1_w->set_int    (0, 0, 11105);
        subtab_1_1_w->set_double (1, 0, 11115.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(3, parent->size());
    CHECK_EQUAL(10005,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10015.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11105,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11115.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Remove the row and the column after the subtables
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(2);
        parent_w->remove(2);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_0_0_w->set_int    (0, 0, 10006);
        subtab_0_0_w->set_float  (1, 0, 10016.0f);
        subtab_1_1_w->set_int    (0, 0, 11106);
        subtab_1_1_w->set_double (1, 0, 11116.0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK_EQUAL(10006,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10016.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11106,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11116.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Check that subtable accessors are detached when the subtables are removed
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(1);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->set_int   (0, 0, 10007);
        subtab_0_0_w->set_float (1, 0, 10017.0f);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK( subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10007,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10017.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(1);
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->set_int   (0, 0, 10008);
        subtab_0_0_w->set_float (1, 0, 10018.0f);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10008,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10018.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));

    // Remove subtable
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear_subtable(0,0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Clear parent table
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 4 new subtables, then remove some of them in a different way
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_column(type_Mixed, "c");
        parent_w->add_empty_row(2);
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        parent_w->set_mixed(0, 1, Mixed::subtable_tag());
        parent_w->set_mixed(1, 0, Mixed::subtable_tag());
        parent_w->set_mixed(1, 1, Mixed::subtable_tag());
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_column(type_String, "x");
        subtab_1_1_w->add_empty_row();
        subtab_1_1_w->set_string(0, 0, "pneumonoultramicroscopicsilicovolcanoconiosis");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(2, parent->size());
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    CHECK(subtab_0_0 && subtab_0_0->is_attached());
    CHECK(subtab_0_1 && subtab_0_1->is_attached());
    CHECK(subtab_1_0 && subtab_1_0->is_attached());
    CHECK(subtab_1_1 && subtab_1_1->is_attached());
    CHECK_EQUAL(0, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(0);
        parent_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    subtab_1_1 = parent->get_subtable(0,0);
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK( subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));

    // Insert 2x2 new subtables, then remove them all together
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_column(type_Mixed, "d");
        parent_w->add_empty_row(2);
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        parent_w->set_mixed(0, 1, Mixed::subtable_tag());
        parent_w->set_mixed(1, 0, Mixed::subtable_tag());
        parent_w->set_mixed(1, 1, Mixed::subtable_tag());
        TableRef subtab_1_1_w = parent_w->get_subtable(1,1);
        subtab_1_1_w->add_column(type_String, "x");
        subtab_1_1_w->add_empty_row();
        subtab_1_1_w->set_string(0, 0, "supercalifragilisticexpialidocious");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last row
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_empty_row(1);
        parent_w->remove_column(0);
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_column(type_String, "x");
        subtab_0_0_w->add_empty_row(1);
        subtab_0_0_w->set_string(0, 0, "brahmaputra");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    subtab_0_0 = parent->get_subtable(0,0);
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("brahmaputra", subtab_0_0->get_string(0,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last column
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_empty_row(1);
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_column(type_String, "x");
        subtab_0_0_w->add_empty_row(1);
        subtab_0_0_w->set_string(0, 0, "baikonur");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    subtab_0_0 = parent->get_subtable(0,0);
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("baikonur", subtab_0_0->get_string(0,0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_MultilevelSubtables)
{
    // FIXME: Regular in regular, mixed in mixed, mixed in regular, and regular in mixed
}


TEST(LangBindHelper_AdvanceReadTransact_Descriptor)
{
    // FIXME: Insert and remove columns before and after a subdescriptor accessor
}


TEST(LangBindHelper_AdvanceReadTransact_RowAccessors)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create a table with two rows
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.add_table("parent");
        parent_w->add_column(type_Int, "a");
        parent_w->add_empty_row(2);
        parent_w->set_int(0, 0, 27);
        parent_w->set_int(0, 1, 227);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    ConstTableRef parent = rt.get_table("parent");
    CHECK_EQUAL(2, parent->size());
    ConstRow row_1 = (*parent)[0];
    ConstRow row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());

    // Check that row insertion does not detach the row accessors, and that the
    // row indexes is properly adjusted
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_empty_row(1); // Between
        parent_w->add_empty_row();     // After
        parent_w->insert_empty_row(0); // Before
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(1, row_1.get_index());
    CHECK_EQUAL(3, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_empty_row(1); // Immediately before row_1
        parent_w->insert_empty_row(5); // Immediately after  row_2
        parent_w->insert_empty_row(3); // Immediately after  row_1
        parent_w->insert_empty_row(5); // Immediately before row_2
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(9, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(2, row_1.get_index());
    CHECK_EQUAL(6, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of rows (other than row_1 and row_2) does not detach
    // the row accessors, and that the row indexes is properly adjusted
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(3); // Immediately after  row_1
        parent_w->remove(1); // Immediately before row_1
        parent_w->remove(3); // Immediately before row_2
        parent_w->remove(4); // Immediately after  row_2
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(1, row_1.get_index());
    CHECK_EQUAL(3, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(4); // After
        parent_w->remove(0); // Before
        parent_w->remove(1); // Between
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of first row detaches row_1
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_2.get_index());
    CHECK_EQUAL(227, row_2.get_int(0));
    // Restore first row and recover row_1
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->insert_empty_row(0);
        parent_w->set_int(0, 0, 27);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    row_1 = (*parent)[0];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of second row detaches row_2
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove(1);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->size());
    CHECK(row_1.is_attached());
    CHECK(!row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(27, row_1.get_int(0));
    // Restore second row and recover row_2
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_empty_row();
        parent_w->set_int(0, 1, 227);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that descriptor modifications do not affect the row accessors (as
    // long as we do not remove the last column)
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_column(type_String, "x");
        parent_w->insert_column(0, type_Float, "y");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(1));
    CHECK_EQUAL(227, row_2.get_int(1));
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(0);
        parent_w->remove_column(1);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of the last column detaches all row accessors
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(!row_2.is_attached());
    // Restore rows and recover row accessors
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_column(type_Int, "a");
        parent_w->add_empty_row(2);
        parent_w->set_int(0, 0, 27);
        parent_w->set_int(0, 1, 227);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    row_1 = (*parent)[0];
    row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());

    // Check that clearing of the table detaches all row accessors
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(!row_2.is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_SubtableRowAccessors)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create a mixed and a regular subtable each with one row
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.add_table("parent");
        parent_w->add_column(type_Mixed, "a");
        parent_w->add_column(type_Table, "b");
        DescriptorRef subdesc = parent_w->get_subdescriptor(1);
        subdesc->add_column(type_Int, "regular");
        parent_w->add_empty_row();
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        TableRef mixed_w = parent_w->get_subtable(0,0);
        mixed_w->add_column(type_Int, "mixed");
        mixed_w->add_empty_row();
        mixed_w->set_int(0, 0, 19);
        TableRef regular_w = parent_w->get_subtable(1,0);
        regular_w->add_empty_row();
        regular_w->set_int(0, 0, 29);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    ConstTableRef parent = rt.get_table("parent");
    ConstTableRef mixed   = parent->get_subtable(0,0);
    ConstTableRef regular = parent->get_subtable(1,0);
    CHECK(mixed   && mixed->is_attached()   && mixed->size()   == 1);
    CHECK(regular && regular->is_attached() && regular->size() == 1);
    ConstRow row_m = (*mixed)[0];
    ConstRow row_r = (*regular)[0];
    CHECK_EQUAL(19, row_m.get_int(0));
    CHECK_EQUAL(29, row_r.get_int(0));

    // Check that all row accessors in a mixed subtable are detached if the
    // subtable is overridden
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->set_mixed(0, 0, Mixed("foo"));
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(!mixed->is_attached());
    CHECK(regular->is_attached());
    CHECK(!row_m.is_attached());
    CHECK(row_r.is_attached());
    // Restore mixed
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        TableRef mixed_w = parent_w->get_subtable(0,0);
        mixed_w->add_column(type_Int, "mixed_2");
        mixed_w->add_empty_row();
        mixed_w->set_int(0, 0, 19);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    mixed = parent->get_subtable(0,0);
    CHECK(mixed);
    CHECK(mixed->is_attached());
    CHECK(regular->is_attached());
    CHECK_EQUAL(1, mixed->size());
    CHECK_EQUAL(1, regular->size());
    row_m = (*mixed)[0];
    CHECK_EQUAL(19, row_m.get_int(0));
    CHECK_EQUAL(29, row_r.get_int(0));

    // Check that all row accessors in a regular subtable are detached if the
    // subtable is overridden
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->set_subtable(1, 0, 0); // Clear
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(mixed->is_attached());
    CHECK(regular->is_attached());
    CHECK(row_m.is_attached());
    CHECK(!row_r.is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_MoveLastOver)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create three parent tables, each with with 5 rows, and each row
    // containing one regular and one mixed subtable
    {
        WriteTransaction wt(sg_w);
        for (int i = 0; i < 3; ++i) {
            const char* table_name = i == 0 ? "parent_1" : i == 1 ? "parent_2" : "parent_3";
            TableRef parent_w = wt.add_table(table_name);
            parent_w->add_column(type_Table, "a");
            parent_w->add_column(type_Mixed, "b");
            DescriptorRef subdesc = parent_w->get_subdescriptor(0);
            subdesc->add_column(type_Int, "regular");
            parent_w->add_empty_row(5);
            for (int row_ndx = 0; row_ndx < 5; ++row_ndx) {
                TableRef regular_w = parent_w->get_subtable(0, row_ndx);
                regular_w->add_empty_row();
                regular_w->set_int(0, 0, 10 + row_ndx);
                parent_w->set_mixed(1, row_ndx, Mixed::subtable_tag());
                TableRef mixed_w = parent_w->get_subtable(1, row_ndx);
                mixed_w->add_column(type_Int, "mixed");
                mixed_w->add_empty_row();
                mixed_w->set_int(0, 0, 20 + row_ndx);
            }
        }
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    // Use first table to check with accessors on row indexes 0, 1, and 4, but
    // none at index 2 and 3.
    {
        ConstTableRef parent = rt.get_table("parent_1");
        ConstRow row_0 = (*parent)[0];
        ConstRow row_1 = (*parent)[1];
        ConstRow row_4 = (*parent)[4];
        ConstTableRef regular_0 = parent->get_subtable(0,0);
        ConstTableRef regular_1 = parent->get_subtable(0,1);
        ConstTableRef regular_4 = parent->get_subtable(0,4);
        ConstTableRef   mixed_0 = parent->get_subtable(1,0);
        ConstTableRef   mixed_1 = parent->get_subtable(1,1);
        ConstTableRef   mixed_4 = parent->get_subtable(1,4);
        CHECK(row_0.is_attached());
        CHECK(row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(4, row_4.get_index());
        CHECK(regular_0->is_attached());
        CHECK(regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(10, regular_0->get_int(0,0));
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_4 && mixed_4->is_attached());
        CHECK_EQUAL(20, mixed_0->get_int(0,0));
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(24, mixed_4->get_int(0,0));

        // Perform two 'move last over' operations which brings the number of
        // rows down from 5 to 3
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_1");
            parent_w->move_last_over(2); // Move row at index 4 to index 2
            parent_w->move_last_over(0); // Move row at index 3 to index 0
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(!row_0.is_attached());
        CHECK(row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(2, row_4.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        CHECK_EQUAL(regular_1, parent->get_subtable(0,1));
        CHECK_EQUAL(regular_4, parent->get_subtable(0,2));
        CHECK(!mixed_0->is_attached());
        CHECK(mixed_1->is_attached());
        CHECK(mixed_4->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(24, mixed_4->get_int(0,0));
        CHECK_EQUAL(mixed_1, parent->get_subtable(1,1));
        CHECK_EQUAL(mixed_4, parent->get_subtable(1,2));

        // Perform two more 'move last over' operations which brings the number
        // of rows down from 3 to 1
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_1");
            parent_w->move_last_over(1); // Move row at index 2 to index 1
            parent_w->move_last_over(0); // Move row at index 1 to index 0
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(!row_0.is_attached());
        CHECK(!row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(0, row_4.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        CHECK_EQUAL(regular_4, parent->get_subtable(0,0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_1->is_attached());
        CHECK(mixed_4->is_attached());
        CHECK_EQUAL(24, mixed_4->get_int(0,0));
        CHECK_EQUAL(mixed_4, parent->get_subtable(1,0));
    }

    // Use second table to check with accessors on row indexes 0, 2, and 3, but
    // none at index 1 and 4.
    {
        ConstTableRef parent = rt.get_table("parent_2");
        ConstRow row_0 = (*parent)[0];
        ConstRow row_2 = (*parent)[2];
        ConstRow row_3 = (*parent)[3];
        ConstTableRef regular_0 = parent->get_subtable(0,0);
        ConstTableRef regular_2 = parent->get_subtable(0,2);
        ConstTableRef regular_3 = parent->get_subtable(0,3);
        ConstTableRef   mixed_0 = parent->get_subtable(1,0);
        ConstTableRef   mixed_2 = parent->get_subtable(1,2);
        ConstTableRef   mixed_3 = parent->get_subtable(1,3);
        CHECK(row_0.is_attached());
        CHECK(row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(2, row_2.get_index());
        CHECK_EQUAL(3, row_3.get_index());
        CHECK(regular_0->is_attached());
        CHECK(regular_2->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(10, regular_0->get_int(0,0));
        CHECK_EQUAL(12, regular_2->get_int(0,0));
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_2 && mixed_2->is_attached());
        CHECK(mixed_3 && mixed_3->is_attached());
        CHECK_EQUAL(20, mixed_0->get_int(0,0));
        CHECK_EQUAL(22, mixed_2->get_int(0,0));
        CHECK_EQUAL(23, mixed_3->get_int(0,0));

        // Perform two 'move last over' operations which brings the number of
        // rows down from 5 to 3
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_2");
            parent_w->move_last_over(2); // Move row at index 4 to index 2
            parent_w->move_last_over(0); // Move row at index 3 to index 0
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(!row_0.is_attached());
        CHECK(!row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_2->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one more 'move last over' operation which brings the number
        // of rows down from 3 to 2
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_2");
            parent_w->move_last_over(1); // Move row at index 2 to index 1
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(!row_0.is_attached());
        CHECK(!row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_2->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one final 'move last over' operation which brings the number
        // of rows down from 2 to 1
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_2");
            parent_w->move_last_over(0); // Move row at index 1 to index 0
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(!row_0.is_attached());
        CHECK(!row_2.is_attached());
        CHECK(!row_3.is_attached());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_2->is_attached());
        CHECK(!regular_3->is_attached());
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(!mixed_3->is_attached());
    }

    // Use third table to check with accessors on row indexes 1 and 3, but none
    // at index 0, 2, and 4.
    {
        ConstTableRef parent = rt.get_table("parent_3");
        ConstRow row_1 = (*parent)[1];
        ConstRow row_3 = (*parent)[3];
        ConstTableRef regular_1 = parent->get_subtable(0,1);
        ConstTableRef regular_3 = parent->get_subtable(0,3);
        ConstTableRef   mixed_1 = parent->get_subtable(1,1);
        ConstTableRef   mixed_3 = parent->get_subtable(1,3);
        CHECK(row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(3, row_3.get_index());
        CHECK(regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_3 && mixed_3->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(23, mixed_3->get_int(0,0));

        // Perform two 'move last over' operations which brings the number of
        // rows down from 5 to 3
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_3");
            parent_w->move_last_over(2); // Move row at index 4 to index 2
            parent_w->move_last_over(0); // Move row at index 3 to index 0
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_1, parent->get_subtable(0,1));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(mixed_1->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_1, parent->get_subtable(1,1));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one more 'move last over' operation which brings the number
        // of rows down from 3 to 2
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_3");
            parent_w->move_last_over(1); // Move row at index 2 to index 1
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(!row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(!regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(!mixed_1->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one final 'move last over' operation which brings the number
        // of rows down from 2 to 1
        {
            WriteTransaction wt(sg_w);
            TableRef parent_w = wt.get_table("parent_3");
            parent_w->move_last_over(0); // Move row at index 1 to index 0
            wt.commit();
        }
        LangBindHelper::advance_read(sg, tlm);
        group.Verify();
        CHECK(!row_1.is_attached());
        CHECK(!row_3.is_attached());
        CHECK(!regular_1->is_attached());
        CHECK(!regular_3->is_attached());
        CHECK(!mixed_1->is_attached());
        CHECK(!mixed_3->is_attached());
    }
}


TEST(LangBindHelper_AdvanceReadTransact_Links)
{
    // This test checks that all the links-related stuff works across
    // transaction boundaries (advance transaction). It does that in a chained
    // manner where the output of one test acts as the input of the next
    // one. This is to save boilerplate code, and to make the test scenarios
    // slightly more varied and realistic.
    //
    // The following operations are covered (for cyclic stuff, see
    // LangBindHelper_AdvanceReadTransact_LinkCycles):
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

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create two origin tables and two target tables, and add some links
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.add_table("origin_1");
        TableRef origin_2_w = wt.add_table("origin_2");
        TableRef target_1_w = wt.add_table("target_1");
        TableRef target_2_w = wt.add_table("target_2");
        target_1_w->add_column(type_Int, "t_1");
        target_2_w->add_column(type_Int, "t_2");
        target_1_w->add_empty_row(2);
        target_2_w->add_empty_row(2);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    ConstTableRef origin_1 = rt.get_table("origin_1");
    ConstTableRef origin_2 = rt.get_table("origin_2");
    ConstTableRef target_1 = rt.get_table("target_1");
    ConstTableRef target_2 = rt.get_table("target_2");
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        origin_1_w->add_column_link(type_LinkList, "o_1_ll_1", *target_1_w);
        origin_2_w->add_column(type_Int, "o_2_f_1");
        origin_2_w->add_empty_row(2);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1: LL_1->T_1
    // O_2: F_1
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        origin_1_w->insert_column(0, type_Int, "o_1_f_2");
        origin_2_w->insert_column_link(0, type_Link, "o_2_l_2", *target_1_w);
        origin_2_w->set_link(0, 0, 1); // O_2_L_2[0] -> T_1[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1: F_2   LL_1->T_1
    // O_2: L_2->T_1   F_1
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        origin_1_w->insert_column_link(0, type_Link, "o_1_l_3", *target_1_w);
        origin_2_w->add_column_link(type_LinkList, "o_2_ll_3", *target_2_w);
        origin_2_w->get_linklist(2, 0)->add(1); // O_2_LL_3[0] -> T_2[1]
        origin_2_w->get_linklist(2, 1)->add(0); // O_2_LL_3[1] -> T_2[0]
        origin_2_w->get_linklist(2, 1)->add(1); // O_2_LL_3[1] -> T_2[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1: L_3->T_1   F_2   LL_1->T_1
    // O_2: L_2->T_1   F_1   LL_3->T_2
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_2_w = wt.get_table("target_2");
        origin_1_w->insert_column_link(2, type_Link, "o_1_l_4", *target_2_w);
        origin_2_w->add_column_link(type_Link, "o_2_l_4", *target_2_w);
        origin_2_w->set_link(3, 0, 1); // O_2_L_4[0] -> T_2[1]
        origin_2_w->set_link(3, 1, 0); // O_2_L_4[1] -> T_2[0]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1: L_3->T_1   F_2   L_4->T_2   LL_1->T_1
    // O_2: L_2->T_1   F_1   LL_3->T_2   L_4->T_2
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        origin_1_w->insert_column(3, type_Int, "o_1_f_5");
        origin_2_w->insert_column(3, type_Int, "o_2_f_5");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1: L_3->T_1   F_2   L_4->T_2   F_5   LL_1->T_1
    // O_2: L_2->T_1   F_1   LL_3->T_2   F_5   L_4->T_2
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        origin_1_w->add_empty_row(2);
        origin_1_w->set_link(0, 1, 0); // O_1_L_3[1] -> T_1[0]
        origin_1_w->set_link(2, 0, 0); // O_1_L_4[0] -> T_2[0]
        origin_1_w->set_link(2, 1, 1); // O_1_L_4[1] -> T_2[1]
        origin_1_w->get_linklist(4, 1)->add(0); // O_1_LL_1[1] -> T_1[0]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    CHECK_EQUAL(4, group.size());
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

    // Check that an empty row can be added to an origin table
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        origin_1_w->add_empty_row();
        origin_1_w->set_int(1, 2, 13);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    // null       null       []
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(13, origin_1->get_int(1,2));
    CHECK(origin_1->is_null_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK(origin_1->is_null_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
    CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
    CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
    CHECK_EQUAL(0, origin_1->get_linklist(4,2)->size());
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

    // Check that an empty row can be added to a target table
    {
        WriteTransaction wt(sg_w);
        TableRef target_1_w = wt.get_table("target_1");
        target_1_w->add_empty_row();
        target_1_w->set_int(0, 2, 17);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    // null       null       []
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(17, target_1->get_int(0,2));
    CHECK(origin_1->is_null_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK(origin_1->is_null_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
    CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
    CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
    CHECK_EQUAL(0, origin_1->get_linklist(4,2)->size());
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
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));

    // Check that a non-empty row can be added to an origin table
    {
        WriteTransaction wt(sg_w);
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_2_w->insert_link(0, 2, 1);  // O_2_L_2[2] -> T_1[1]
        origin_2_w->insert_int(1, 2, 19);
        origin_2_w->insert_linklist(2, 2);
        origin_2_w->insert_int(3, 2, 0);
        origin_2_w->insert_link(4, 2, 0);  // O_2_L_4[2] -> T_2[0]
        origin_2_w->insert_done();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    // null       null       []                     T_1[1]     []                     T_2[0]
    CHECK_EQUAL(3, origin_2->size());
    CHECK_EQUAL(19, origin_2->get_int(1,2));
    CHECK(origin_1->is_null_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK(origin_1->is_null_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
    CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
    CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
    CHECK_EQUAL(0, origin_1->get_linklist(4,2)->size());
    CHECK_EQUAL(1, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->size());
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->get(0).get_index());
    CHECK_EQUAL(2, origin_2->get_linklist(2,1)->size());
    CHECK_EQUAL(0, origin_2->get_linklist(2,1)->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_linklist(2,1)->get(1).get_index());
    CHECK_EQUAL(0, origin_2->get_linklist(2,2)->size());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK_EQUAL(0, origin_2->get_link(4,1));
    CHECK_EQUAL(0, origin_2->get_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(2, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));

    // Check that a link can be changed
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->set_link(0, 2, 1);  // null -> non-null
        origin_2_w->nullify_link(0, 2); // non-null -> null
        origin_2_w->set_link(4, 2, 1);  // non-null -> non-null
        // Removes O_2_L_2[2] -> T_1[1]  and  O_2_L_4[2] -> T_2[0]
        // Adds    O_1_L_3[2] -> T_1[1]  and  O_2_L_4[2] -> T_2[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    // T_1[1]     null       []                     null       []                     T_2[1]
    CHECK(origin_1->is_null_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK_EQUAL(1, origin_1->get_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
    CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
    CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
    CHECK_EQUAL(0, origin_1->get_linklist(4,2)->size());
    CHECK_EQUAL(1, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->size());
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->get(0).get_index());
    CHECK_EQUAL(2, origin_2->get_linklist(2,1)->size());
    CHECK_EQUAL(0, origin_2->get_linklist(2,1)->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_linklist(2,1)->get(1).get_index());
    CHECK_EQUAL(0, origin_2->get_linklist(2,2)->size());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK_EQUAL(0, origin_2->get_link(4,1));
    CHECK_EQUAL(1, origin_2->get_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 4));

    // Check that a link can be added to an empty link list
    ConstLinkViewRef link_list_1_2 = origin_1->get_linklist(4,2);
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        LinkViewRef link_list_1_2_w = origin_1_w->get_linklist(4,2);
        LinkViewRef link_list_2_2_w = origin_2_w->get_linklist(2,2);
        link_list_1_2_w->add(0); // O_1_LL_1[2] -> T_1[0]
        link_list_1_2_w->add(1); // O_1_LL_1[2] -> T_1[1]
        link_list_2_2_w->add(0); // O_2_LL_3[2] -> T_2[0]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    // T_1[1]     null       [ T_1[0], T_1[1] ]     null       [ T_2[0] ]             T_2[1]
    ConstLinkViewRef link_list_2_2 = origin_2->get_linklist(2,2);
    CHECK(origin_1->is_null_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK_EQUAL(1, origin_1->get_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
    CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
    CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
    CHECK(link_list_1_2->is_attached());
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(2, link_list_1_2->size());
    CHECK_EQUAL(0, link_list_1_2->get(0).get_index());
    CHECK_EQUAL(1, link_list_1_2->get(1).get_index());
    CHECK_EQUAL(1, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->size());
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->get(0).get_index());
    CHECK_EQUAL(2, origin_2->get_linklist(2,1)->size());
    CHECK_EQUAL(0, origin_2->get_linklist(2,1)->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_linklist(2,1)->get(1).get_index());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK_EQUAL(0, origin_2->get_link(4,1));
    CHECK_EQUAL(1, origin_2->get_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 4));

    // Check that a link can be removed from a link list, and that a link can be
    // added to a non-empty link list
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        LinkViewRef link_list_1_2_w = origin_1_w->get_linklist(4,2);
        LinkViewRef link_list_2_2_w = origin_2_w->get_linklist(2,2);
        link_list_1_2_w->remove(0); // Remove  O_1_LL_1[2] -> T_1[0]
        link_list_2_2_w->add(1);    // Add     O_2_LL_3[2] -> T_2[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    // T_1[1]     null       [ T_1[1] ]             null       [ T_2[0], T_2[1] ]     T_2[1]
    CHECK(origin_1->is_null_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK_EQUAL(1, origin_1->get_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
    CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
    CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
    CHECK(link_list_1_2->is_attached());
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(1, link_list_1_2->size());
    CHECK_EQUAL(1, link_list_1_2->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->size());
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->get(0).get_index());
    CHECK_EQUAL(2, origin_2->get_linklist(2,1)->size());
    CHECK_EQUAL(0, origin_2->get_linklist(2,1)->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_linklist(2,1)->get(1).get_index());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(2, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_2->get(1).get_index());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK_EQUAL(0, origin_2->get_link(4,1));
    CHECK_EQUAL(1, origin_2->get_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(3, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 4));

    ConstLinkViewRef link_list_1_0 = origin_1->get_linklist(4,0);
    ConstLinkViewRef link_list_1_1 = origin_1->get_linklist(4,1);
    ConstLinkViewRef link_list_2_0 = origin_2->get_linklist(2,0);
    ConstLinkViewRef link_list_2_1 = origin_2->get_linklist(2,1);

    // Check that a link list can be cleared, and that a link can be moved
    // inside a link list
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        LinkViewRef link_list_1_2_w = origin_1_w->get_linklist(4,2);
        LinkViewRef link_list_2_2_w = origin_2_w->get_linklist(2,2);
        link_list_1_2_w->clear(); // Remove  O_1_LL_1[2] -> T_1[1]
        link_list_2_2_w->move(0,2); // [ 0, 1 ] -> [ 1, 0 ]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    // T_1[1]     null       []                     null       [ T_2[1], T_2[0] ]     T_2[1]
    CHECK(origin_1->is_null_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK_EQUAL(1, origin_1->get_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, origin_1->get_linklist(4,0)->size());
    CHECK_EQUAL(1, origin_1->get_linklist(4,1)->size());
    CHECK_EQUAL(0, origin_1->get_linklist(4,1)->get(0).get_index());
    CHECK(link_list_1_2->is_attached());
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(1, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->size());
    CHECK_EQUAL(1, origin_2->get_linklist(2,0)->get(0).get_index());
    CHECK_EQUAL(2, origin_2->get_linklist(2,1)->size());
    CHECK_EQUAL(0, origin_2->get_linklist(2,1)->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_linklist(2,1)->get(1).get_index());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(2, link_list_2_2->size());
    CHECK_EQUAL(1, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, link_list_2_2->get(1).get_index());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK_EQUAL(0, origin_2->get_link(4,1));
    CHECK_EQUAL(1, origin_2->get_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(3, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 4));

    // Check that an origin-side row can be deleted by a "move last over"
    // operation
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->move_last_over(0); // [ 0, 1, 2 ] -> [ 2, 1 ]
        origin_2_w->move_last_over(2); // [ 0, 1, 2 ] -> [ 0, 1 ]
        // Removes  O_1_L_4[0]  -> T_2[0]  and  O_1_L_3[2]  -> T_1[1]  and
        //          O_2_LL_3[2] -> T_2[0]  and  O_2_LL_3[2] -> T_2[1]  and  O_2_L_4[2] -> T_2[1]
        // Adds     O_1_L_3[0]  -> T_1[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     null       []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    CHECK_EQUAL(2, origin_1->size());
    CHECK_EQUAL(2, origin_2->size());
    CHECK(!link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(!link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    link_list_1_0 = link_list_1_2;
    link_list_1_2.reset();
    link_list_2_2.reset();
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK(origin_1->is_null_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(1, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(2, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_1->get(1).get_index());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK_EQUAL(0, origin_2->get_link(4,1));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->add_empty_row();   // [ 2, 1 ] -> [ 2, 1, 3 ]
        origin_1_w->set_link(2, 2, 0);
        origin_2_w->move_last_over(0); // [ 0, 1 ] -> [ 1 ]
        // Removes  O_2_L_2[0]  -> T_1[1]  and  O_2_LL_3[1] -> T_2[0]  and
        //          O_2_LL_3[1] -> T_2[1]  and  O_2_L_4[0]  -> T_2[1]  and  O_2_L_4[1] -> T_2[0]
        // Adds     O_1_L_4[2]  -> T_2[0]  and  O_2_LL_3[0] -> T_2[0]  and  O_2_L_4[0] -> T_2[0]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     null       []                     null       [ T_2[0], T_2[1] ]     T_2[0]
    // T_1[0]     T_2[1]     [ T_1[0] ]
    // null       T_2[0]     []
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(1, origin_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(!link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,0));
    link_list_1_2 = origin_1->get_linklist(4,2);
    link_list_2_0 = link_list_2_1;
    link_list_2_1.reset();
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK_EQUAL(0, origin_1->get_link(0,1));
    CHECK(origin_1->is_null_link(0,2));
    CHECK(origin_1->is_null_link(2,0));
    CHECK_EQUAL(1, origin_1->get_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK(origin_2->is_null_link(0,0));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(0, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->move_last_over(1); // [ 2, 1, 3 ] -> [ 2, 3 ]
        origin_2_w->move_last_over(0); // [ 1 ] -> []
        // Removes  O_1_L_3[1]  -> T_1[0]  and  O_1_L_4[1]  -> T_2[1]  and
        //          O_1_LL_1[1] -> T_1[0]  and  O_1_L_4[2]  -> T_2[0]  and
        //          O_2_LL_3[0] -> T_2[0]  and  O_2_LL_3[0] -> T_2[1]  and  O_2_L_4[0]  -> T_2[0]
        // Adds     O_1_L_4[1]  -> T_2[0]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     null       []
    // null       T_2[0]     []
    CHECK_EQUAL(2, origin_1->size());
    CHECK_EQUAL(0, origin_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(!link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(!link_list_2_0->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,1));
    link_list_1_1 = link_list_1_2;
    link_list_1_2.reset();
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK(origin_1->is_null_link(2,0));
    CHECK_EQUAL(0, origin_1->get_link(2,1));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(0, link_list_1_1->size());
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->move_last_over(1); // [ 2, 3 ] -> [ 2 ]
        // Removes  O_1_L_4[1] -> T_2[0]
        origin_2_w->add_empty_row(3); // [] -> [ 3, 4, 5 ]
        origin_2_w->set_link(0,0,0);           // O_2_L_2[0]  -> T_1[0]
        origin_2_w->set_link(0,2,1);           // O_2_L_2[2]  -> T_1[1]
        origin_2_w->get_linklist(2,0)->add(1); // O_2_LL_3[0] -> T_2[1]
        origin_2_w->get_linklist(2,1)->add(0); // O_2_LL_3[1] -> T_2[0]
        origin_2_w->get_linklist(2,1)->add(1); // O_2_LL_3[1] -> T_2[1]
        origin_2_w->get_linklist(2,2)->add(1); // O_2_LL_3[2] -> T_2[1]
        origin_2_w->get_linklist(2,2)->add(0); // O_2_LL_3[2] -> T_2[0]
        origin_2_w->set_link(4,0,1);           // O_2_L_4[0]  -> T_2[1]
        origin_2_w->set_link(4,2,0);           // O_2_L_4[2]  -> T_2[0]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     null       []                     T_1[0]     [ T_2[1] ]             T_2[1]
    //                                              null       [ T_2[0], T_2[1] ]     null
    //                                              T_1[1]     [ T_2[1], T_2[0] ]     T_2[0]
    CHECK_EQUAL(1, origin_1->size());
    CHECK_EQUAL(3, origin_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(!link_list_1_1->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    link_list_1_1.reset();
    link_list_2_0 = origin_2->get_linklist(2,0);
    link_list_2_1 = origin_2->get_linklist(2,1);
    link_list_2_2 = origin_2->get_linklist(2,2);
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(2,0));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(1, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(2, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_1->get(1).get_index());
    CHECK_EQUAL(2, link_list_2_2->size());
    CHECK_EQUAL(1, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, link_list_2_2->get(1).get_index());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK(origin_2->is_null_link(4,1));
    CHECK_EQUAL(0, origin_2->get_link(4,2));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(3, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        origin_1_w->add_empty_row(2); // [ 2 ] -> [ 2, 4, 5 ]
        origin_1_w->set_link(0,2,0); // O_1_L_3[2] -> T_1[0]
        origin_1_w->set_link(2,0,1); // O_1_L_4[0] -> T_2[1]
        origin_1_w->set_link(2,2,0); // O_1_L_4[2] -> T_2[0]
        origin_1_w->get_linklist(4,1)->add(0); // O_1_LL_1[1] -> T_1[0]
        origin_1_w->get_linklist(4,1)->add(0); // O_1_LL_1[1] -> T_1[0] (double)
        origin_1_w->get_linklist(4,2)->add(1); // O_1_LL_1[2] -> T_1[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[1]     []                     T_1[0]     [ T_2[1] ]             T_2[1]
    // null       null       [ T_1[0], T_1[0] ]     null       [ T_2[0], T_2[1] ]     null
    // T_1[0]     T_2[0]     [ T_1[1] ]             T_1[1]     [ T_2[1], T_2[0] ]     T_2[0]
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(3, origin_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    link_list_1_1 = origin_1->get_linklist(4,1);
    link_list_1_2 = origin_1->get_linklist(4,2);
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(2, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_1->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_2->size());
    CHECK_EQUAL(1, link_list_1_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(1, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(2, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_1->get(1).get_index());
    CHECK_EQUAL(2, link_list_2_2->size());
    CHECK_EQUAL(1, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, link_list_2_2->get(1).get_index());
    CHECK_EQUAL(1, origin_2->get_link(4,0));
    CHECK(origin_2->is_null_link(4,1));
    CHECK_EQUAL(0, origin_2->get_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(3, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));

    // Check that an target-side row can be deleted by a "move last over"
    // operation
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_2_w = wt.get_table("target_2");
        target_2_w->add_empty_row();
        origin_1_w->get_linklist(4,1)->set(0,2);
        origin_2_w->get_linklist(2,2)->set(1,2);
        origin_2_w->set_link(4,0,2);
        // Removes  O_1_LL_1[1] -> T_1[0]  and  O_2_LL_3[2] -> T_2[0]  and  O_2_L_4[0] -> T_2[1]
        // Adds     O_1_LL_1[1] -> T_1[2]  and  O_2_LL_3[2] -> T_2[2]  and  O_2_L_4[0] -> T_2[2]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[1]     []                     T_1[0]     [ T_2[1] ]             T_2[2]
    // null       null       [ T_1[2], T_1[0] ]     null       [ T_2[0], T_2[1] ]     null
    // T_1[0]     T_2[0]     [ T_1[1] ]             T_1[1]     [ T_2[1], T_2[2] ]     T_2[0]
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(3, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(2, link_list_1_1->size());
    CHECK_EQUAL(2, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_1->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_2->size());
    CHECK_EQUAL(1, link_list_1_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(1, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(2, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_1->get(1).get_index());
    CHECK_EQUAL(2, link_list_2_2->size());
    CHECK_EQUAL(1, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(2, link_list_2_2->get(1).get_index());
    CHECK_EQUAL(2, origin_2->get_link(4,0));
    CHECK(origin_2->is_null_link(4,1));
    CHECK_EQUAL(0, origin_2->get_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(3, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(2, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        target_1_w->move_last_over(0); // [ 0, 1, 2 ] -> [ 2, 1 ]
        target_2_w->move_last_over(2); // [ 0, 1, 2 ] -> [ 0, 1 ]
        // Removes  O_1_L_3[2] -> T_1[0]  and  O_1_LL_1[1] -> T_1[2]  and
        //          O_2_L_2[0] -> T_1[0]  and  O_2_LL_3[2] -> T_2[2]  and  O_2_L_4[0] -> T_2[2]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[1]     []                     null       [ T_2[1] ]             null
    // null       null       [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     null
    // null       T_2[0]     [ T_1[1] ]             T_1[1]     [ T_2[1] ]             T_2[0]
    CHECK_EQUAL(2, target_1->size());
    CHECK_EQUAL(2, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK(origin_1->is_null_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_1_2->size());
    CHECK_EQUAL(1, link_list_1_2->get(0).get_index());
    CHECK(origin_2->is_null_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(1, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(2, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_1->get(1).get_index());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(1, link_list_2_2->get(0).get_index());
    CHECK(origin_2->is_null_link(4,0));
    CHECK(origin_2->is_null_link(4,1));
    CHECK_EQUAL(0, origin_2->get_link(4,2));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(3, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        target_1_w->add_empty_row();   // [ 2, 1 ] -> [ 2, 1, 3 ]
        origin_1_w->set_link(0,2,2);           // O_1_L_3[2]  -> T_1[2]
        origin_1_w->get_linklist(4,1)->add(2); // O_1_LL_1[1] -> T_1[2]
        origin_2_w->set_link(0,0,2);           // O_2_L_2[0]  -> T_1[2]
        target_2_w->move_last_over(0); // [ 0, 1 ] -> [ 1 ]
        // Removes  O_1_L_4[0]  -> T_2[1]  and  O_1_L_4[2]  -> T_2[0]  and
        //          O_2_LL_3[0] -> T_2[1]  and  O_2_LL_3[1] -> T_2[1]  and
        //          O_2_LL_3[2] -> T_2[1]  and  O_2_L_4[2]  -> T_2[0]
        // Adds     O_1_L_4[0]  -> T_2[0]  and  O_2_LL_3[0] -> T_2[0]  and
        //          O_2_LL_3[2] -> T_2[0]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[0]     []                     T_1[2]     [ T_2[0] ]             null
    // null       null       [ T_1[0], T_1[2] ]     null       [ T_2[0] ]             null
    // T_1[2]     null       [ T_1[1] ]             T_1[1]     [ T_2[0] ]             null
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(1, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(2, origin_1->get_link(0,2));
    CHECK_EQUAL(0, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(2, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(2, link_list_1_1->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_2->size());
    CHECK_EQUAL(1, link_list_1_2->get(0).get_index());
    CHECK_EQUAL(2, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(1, link_list_2_0->size());
    CHECK_EQUAL(0, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK(origin_2->is_null_link(4,0));
    CHECK(origin_2->is_null_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(3, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        target_1_w->move_last_over(1); // [ 2, 1, 3 ] -> [ 2, 3 ]
        target_2_w->move_last_over(0); // [ 1 ] -> []
        // Removes  O_1_L_3[0]  -> T_1[1]  and  O_1_L_3[2]  -> T_1[2]  and
        //          O_1_L_4[0]  -> T_2[0]  and  O_1_LL_1[1] -> T_1[2]  and
        //          O_1_LL_1[2] -> T_1[1]  and  O_2_L_2[0]  -> T_1[2]  and
        //          O_2_L_2[2]  -> T_1[1]  and  O_2_LL_3[0] -> T_2[0]  and
        //          O_2_LL_3[1] -> T_2[0]  and  O_2_LL_3[2] -> T_2[0]
        // Adds     O_1_L_3[2]  -> T_1[1]  and  O_1_LL_1[1] -> T_1[1]  and
        //          O_2_L_2[0]  -> T_1[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       null       []                     T_1[1]     []                     null
    // null       null       [ T_1[0], T_1[1] ]     null       []                     null
    // T_1[1]     null       []                     null       []                     null
    CHECK_EQUAL(2, target_1->size());
    CHECK_EQUAL(0, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK(origin_1->is_null_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(1, origin_1->get_link(0,2));
    CHECK(origin_1->is_null_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(2, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(1, link_list_1_1->get(1).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(1, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(0, link_list_2_0->size());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_2->size());
    CHECK(origin_2->is_null_link(4,0));
    CHECK(origin_2->is_null_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        target_1_w->move_last_over(1); // [ 2, 3 ] -> [ 2 ]
        // Removes  O_1_L_3[2] -> T_1[1]  and  O_1_LL_1[1] -> T_1[1]  and  O_2_L_2[0] -> T_1[1]
        target_2_w->add_empty_row(3); // [] -> [ 3, 4, 5 ]
        origin_1_w->set_link(2,0,1);           // O_1_L_4[0]  -> T_2[1]
        origin_1_w->set_link(2,2,0);           // O_1_L_4[2]  -> T_2[0]
        origin_2_w->get_linklist(2,0)->add(1); // O_2_LL_3[0] -> T_2[1]
        origin_2_w->get_linklist(2,0)->add(1); // O_2_LL_3[0] -> T_2[1]
        origin_2_w->get_linklist(2,2)->add(0); // O_2_LL_3[2] -> T_2[0]
        origin_2_w->set_link(4,0,0);           // O_2_L_4[0]  -> T_2[0]
        origin_2_w->set_link(4,1,1);           // O_2_L_4[1]  -> T_2[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[1]     []                     null       [ T_2[1], T_2[1] ]     T_2[0]
    // null       null       [ T_1[0] ]             null       []                     T_2[1]
    // null       T_2[0]     []                     null       [ T_2[0] ]             null
    CHECK_EQUAL(1, target_1->size());
    CHECK_EQUAL(3, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK(origin_1->is_null_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK(origin_1->is_null_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK(origin_2->is_null_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef target_1_w = wt.get_table("target_1");
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        target_1_w->add_empty_row(2); // [ 2 ] -> [ 2, 4, 5 ]
        origin_1_w->set_link(0,0,1); // O_1_L_3[0] -> T_1[1]
        origin_1_w->set_link(0,2,0); // O_1_L_3[2] -> T_1[0]
        origin_1_w->get_linklist(4,0)->add(1); // O_1_LL_1[0] -> T_1[1]
        origin_1_w->get_linklist(4,0)->add(0); // O_1_LL_1[0] -> T_1[0]
        origin_2_w->set_link(0,0,0); // O_2_L_2[0] -> T_1[0]
        origin_2_w->set_link(0,2,1); // O_2_L_2[2] -> T_1[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[1]     [ T_1[1], T_1[0] ]     T_1[0]     [ T_2[1], T_2[1] ]     T_2[0]
    // null       null       [ T_1[0] ]             null       []                     T_2[1]
    // T_1[0]     T_2[0]     []                     T_1[1]     [ T_2[0] ]             null
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(3, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));

    // Check that an origin-side table can be cleared
    {
        WriteTransaction wt(sg_w);
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_2_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[1]     [ T_1[1], T_1[0] ]
    // null       null       [ T_1[0] ]
    // T_1[0]     T_2[0]     []
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(0, origin_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(!link_list_2_0->is_attached());
    CHECK(!link_list_2_1->is_attached());
    CHECK(!link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    link_list_2_0.reset();
    link_list_2_1.reset();
    link_list_2_2.reset();
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_2_w->add_empty_row(3);
        origin_2_w->set_link(0,0,0);
        origin_2_w->set_link(0,2,1);
        origin_2_w->get_linklist(2,0)->add(1);
        origin_2_w->get_linklist(2,0)->add(1);
        origin_2_w->get_linklist(2,2)->add(0);
        origin_2_w->set_link(4,0,0);
        origin_2_w->set_link(4,1,1);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[1]     [ T_1[1], T_1[0] ]     T_1[0]     [ T_2[1], T_2[1] ]     T_2[0]
    // null       null       [ T_1[0] ]             null       []                     T_2[1]
    // T_1[0]     T_2[0]     []                     T_1[1]     [ T_2[0] ]             null
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(3, origin_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    link_list_2_0 = origin_2->get_linklist(2,0);
    link_list_2_1 = origin_2->get_linklist(2,1);
    link_list_2_2 = origin_2->get_linklist(2,2);
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));

    // Check that a target-side table can be cleared
    {
        WriteTransaction wt(sg_w);
        TableRef target_2_w = wt.get_table("target_2");
        target_2_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     null       [ T_1[1], T_1[0] ]     T_1[0]     []                     null
    // null       null       [ T_1[0] ]             null       []                     null
    // T_1[0]     null       []                     T_1[1]     []                     null
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(3, origin_2->size());
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(0, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK(origin_1->is_null_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK(origin_1->is_null_link(2,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(0, link_list_2_0->size());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(0, link_list_2_2->size());
    CHECK(origin_2->is_null_link(4,0));
    CHECK(origin_2->is_null_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_2_w = wt.get_table("target_2");
        target_2_w->add_empty_row(3);
        origin_1_w->set_link(2,0,1);
        origin_1_w->set_link(2,2,0);
        origin_2_w->get_linklist(2,0)->add(1);
        origin_2_w->get_linklist(2,0)->add(1);
        origin_2_w->get_linklist(2,2)->add(0);
        origin_2_w->set_link(4,0,0);
        origin_2_w->set_link(4,1,1);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[1]     [ T_1[1], T_1[0] ]     T_1[0]     [ T_2[1], T_2[1] ]     T_2[0]
    // null       null       [ T_1[0] ]             null       []                     T_2[1]
    // T_1[0]     T_2[0]     []                     T_1[1]     [ T_2[0] ]             null
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(3, target_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));

    // Check that non-link columns can be inserted into origin table and removed
    // from it
    CHECK_EQUAL(5, origin_1->get_column_count());
    CHECK_EQUAL(5, origin_2->get_column_count());
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
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->insert_column(2, type_Table,  "foo_1");
        origin_2_w->insert_column(0, type_Table,  "foo_2");
        origin_2_w->insert_column(6, type_String, "foo_3");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(6, origin_1->get_column_count());
    CHECK_EQUAL(7, origin_2->get_column_count());
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(0));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(1));
    CHECK_EQUAL(type_Table,    origin_1->get_column_type(2));
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(3));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(4));
    CHECK_EQUAL(type_LinkList, origin_1->get_column_type(5));
    CHECK_EQUAL(type_Table,    origin_2->get_column_type(0));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(1));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(2));
    CHECK_EQUAL(type_LinkList, origin_2->get_column_type(3));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(4));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(5));
    CHECK_EQUAL(type_String,   origin_2->get_column_type(6));
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(3, origin_2->size());
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(5,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(5,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(5,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(3,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(3,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(3,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(3,0));
    CHECK(origin_1->is_null_link(3,1));
    CHECK_EQUAL(0, origin_1->get_link(3,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, origin_2->get_link(1,0));
    CHECK(origin_2->is_null_link(1,1));
    CHECK_EQUAL(1, origin_2->get_link(1,2));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(5,0));
    CHECK_EQUAL(1, origin_2->get_link(5,1));
    CHECK(origin_2->is_null_link(5,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 5));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 1));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 5));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 1));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 5));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 1));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 5));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 3));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 5));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 3));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 3));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 5));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->insert_column(4, type_Mixed, "foo_4");
        origin_2_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(7, origin_1->get_column_count());
    CHECK_EQUAL(6, origin_2->get_column_count());
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(0));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(1));
    CHECK_EQUAL(type_Table,    origin_1->get_column_type(2));
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(3));
    CHECK_EQUAL(type_Mixed,    origin_1->get_column_type(4));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(5));
    CHECK_EQUAL(type_LinkList, origin_1->get_column_type(6));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(0));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(1));
    CHECK_EQUAL(type_LinkList, origin_2->get_column_type(2));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(3));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(4));
    CHECK_EQUAL(type_String,   origin_2->get_column_type(5));
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(6,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(6,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(6,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(3,0));
    CHECK(origin_1->is_null_link(3,1));
    CHECK_EQUAL(0, origin_1->get_link(3,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 6));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 6));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 6));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 3));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 3));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->remove_column(2);
        origin_1_w->remove_column(3);
        origin_2_w->remove_column(5);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, origin_1->get_column_count());
    CHECK_EQUAL(5, origin_2->get_column_count());
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
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));

    // Check that link columns can be inserted into origin table and removed
    // from it
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        origin_1_w->insert_column_link(2, type_LinkList, "bar_1", *target_2_w);
        origin_2_w->insert_column_link(0, type_Link,     "bar_2", *target_1_w);
        origin_2_w->insert_column_link(6, type_LinkList, "bar_3", *target_2_w);
        origin_2_w->set_link(0,0,2);
        origin_2_w->set_link(0,1,0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(6, origin_1->get_column_count());
    CHECK_EQUAL(7, origin_2->get_column_count());
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(0));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(1));
    CHECK_EQUAL(type_LinkList, origin_1->get_column_type(2));
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(3));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(4));
    CHECK_EQUAL(type_LinkList, origin_1->get_column_type(5));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(0));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(1));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(2));
    CHECK_EQUAL(type_LinkList, origin_2->get_column_type(3));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(4));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(5));
    CHECK_EQUAL(type_LinkList, origin_2->get_column_type(6));
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(3,0));
    CHECK(origin_1->is_null_link(3,1));
    CHECK_EQUAL(0, origin_1->get_link(3,2));
    CHECK_EQUAL(2, origin_2->get_link(0,0));
    CHECK_EQUAL(0, origin_2->get_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(0, origin_2->get_link(1,0));
    CHECK(origin_2->is_null_link(1,1));
    CHECK_EQUAL(1, origin_2->get_link(1,2));
    CHECK_EQUAL(0, origin_2->get_link(5,0));
    CHECK_EQUAL(1, origin_2->get_link(5,1));
    CHECK(origin_2->is_null_link(5,2));
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(5,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(5,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(5,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(3,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(3,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(3,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    ConstLinkViewRef link_list_1_0_x = origin_1->get_linklist(2,0);
    ConstLinkViewRef link_list_1_1_x = origin_1->get_linklist(2,1);
    ConstLinkViewRef link_list_1_2_x = origin_1->get_linklist(2,2);
    ConstLinkViewRef link_list_2_0_x = origin_2->get_linklist(6,0);
    ConstLinkViewRef link_list_2_1_x = origin_2->get_linklist(6,1);
    ConstLinkViewRef link_list_2_2_x = origin_2->get_linklist(6,2);
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0_x->size());
    CHECK_EQUAL(0, link_list_1_1_x->size());
    CHECK_EQUAL(0, link_list_1_2_x->size());
    CHECK_EQUAL(0, link_list_2_0_x->size());
    CHECK_EQUAL(0, link_list_2_1_x->size());
    CHECK_EQUAL(0, link_list_2_2_x->size());
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 5));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 1));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 5));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 1));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 5));
    CHECK_EQUAL(1, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 1));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 5));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_2, 6));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 3));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 5));
    CHECK_EQUAL(0, target_2->get_backlink_count(1, *origin_2, 6));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 3));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 3));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 5));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 6));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        TableRef target_1_w = wt.get_table("target_1");
        origin_1_w->insert_column_link(4, type_Link, "bar_4", *target_1_w);
        origin_2_w->remove_column(0);
        origin_1_w->set_link(4,1,2);
        origin_1_w->set_link(4,2,0);
        origin_1_w->get_linklist(2,1)->add(2);
        origin_1_w->get_linklist(2,1)->add(1);
        origin_1_w->get_linklist(2,1)->add(2);
        origin_1_w->get_linklist(2,2)->add(1);
        origin_2_w->get_linklist(5,0)->add(1);
        origin_2_w->get_linklist(5,2)->add(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(7, origin_1->get_column_count());
    CHECK_EQUAL(6, origin_2->get_column_count());
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(0));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(1));
    CHECK_EQUAL(type_LinkList, origin_1->get_column_type(2));
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(3));
    CHECK_EQUAL(type_Link,     origin_1->get_column_type(4));
    CHECK_EQUAL(type_Int,      origin_1->get_column_type(5));
    CHECK_EQUAL(type_LinkList, origin_1->get_column_type(6));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(0));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(1));
    CHECK_EQUAL(type_LinkList, origin_2->get_column_type(2));
    CHECK_EQUAL(type_Int,      origin_2->get_column_type(3));
    CHECK_EQUAL(type_Link,     origin_2->get_column_type(4));
    CHECK_EQUAL(type_LinkList, origin_2->get_column_type(5));
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(3,0));
    CHECK(origin_1->is_null_link(3,1));
    CHECK_EQUAL(0, origin_1->get_link(3,2));
    CHECK(origin_1->is_null_link(4,0));
    CHECK_EQUAL(2, origin_1->get_link(4,1));
    CHECK_EQUAL(0, origin_1->get_link(4,2));
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_1_0_x->is_attached());
    CHECK(link_list_1_1_x->is_attached());
    CHECK(link_list_1_2_x->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK(link_list_2_0_x->is_attached());
    CHECK(link_list_2_1_x->is_attached());
    CHECK(link_list_2_2_x->is_attached());
    CHECK_EQUAL(link_list_1_0,   origin_1->get_linklist(6,0));
    CHECK_EQUAL(link_list_1_1,   origin_1->get_linklist(6,1));
    CHECK_EQUAL(link_list_1_2,   origin_1->get_linklist(6,2));
    CHECK_EQUAL(link_list_1_0_x, origin_1->get_linklist(2,0));
    CHECK_EQUAL(link_list_1_1_x, origin_1->get_linklist(2,1));
    CHECK_EQUAL(link_list_1_2_x, origin_1->get_linklist(2,2));
    CHECK_EQUAL(link_list_2_0,   origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1,   origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2,   origin_2->get_linklist(2,2));
    CHECK_EQUAL(link_list_2_0_x, origin_2->get_linklist(5,0));
    CHECK_EQUAL(link_list_2_1_x, origin_2->get_linklist(5,1));
    CHECK_EQUAL(link_list_2_2_x, origin_2->get_linklist(5,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_1_0_x->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1_x->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2_x->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0_x->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1_x->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2_x->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(0, link_list_1_0_x->size());
    CHECK_EQUAL(3, link_list_1_1_x->size());
    CHECK_EQUAL(2, link_list_1_1_x->get(0).get_index());
    CHECK_EQUAL(1, link_list_1_1_x->get(1).get_index());
    CHECK_EQUAL(2, link_list_1_1_x->get(2).get_index());
    CHECK_EQUAL(1, link_list_1_2_x->size());
    CHECK_EQUAL(1, link_list_1_2_x->get(0).get_index());
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0_x->size());
    CHECK_EQUAL(1, link_list_2_0_x->get(0).get_index());
    CHECK_EQUAL(0, link_list_2_1_x->size());
    CHECK_EQUAL(1, link_list_2_2_x->size());
    CHECK_EQUAL(0, link_list_2_2_x->get(0).get_index());
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 6));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 6));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 6));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(0, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 3));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 5));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 3));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 5));
    CHECK_EQUAL(2, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 3));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 5));
    {
        WriteTransaction wt(sg_w);
        TableRef origin_1_w = wt.get_table("origin_1");
        TableRef origin_2_w = wt.get_table("origin_2");
        origin_1_w->remove_column(2);
        origin_1_w->remove_column(3);
        origin_2_w->remove_column(5);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, origin_1->get_column_count());
    CHECK_EQUAL(5, origin_2->get_column_count());
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
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(3, origin_2->size());
    CHECK_EQUAL(1, origin_1->get_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK_EQUAL(0, origin_1->get_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK_EQUAL(0, origin_2->get_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK_EQUAL(1, origin_2->get_link(0,2));
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK(!link_list_1_0_x->is_attached());
    CHECK(!link_list_1_1_x->is_attached());
    CHECK(!link_list_1_2_x->is_attached());
    CHECK(!link_list_2_0_x->is_attached());
    CHECK(!link_list_2_1_x->is_attached());
    CHECK(!link_list_2_2_x->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_0->size());
    CHECK_EQUAL(1, link_list_1_0->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_0->get(1).get_index());
    CHECK_EQUAL(1, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_1->get(0).get_index());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));

    // Check that columns can be inserted into target table and removed from it
    {
        WriteTransaction wt(sg_w);
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        target_1_w->insert_column(0, type_Mixed, "t_3");
        target_2_w->insert_column_link(1, type_Link, "t_4", *target_1_w);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, target_1->get_column_count());
    CHECK_EQUAL(2, target_2->get_column_count());
    CHECK_EQUAL(type_Mixed, target_1->get_column_type(0));
    CHECK_EQUAL(type_Int,   target_1->get_column_type(1));
    CHECK_EQUAL(type_Int,   target_2->get_column_type(0));
    CHECK_EQUAL(type_Link,  target_2->get_column_type(1));
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(3, target_2->size());
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));
    {
        WriteTransaction wt(sg_w);
        TableRef target_1_w = wt.get_table("target_1");
        TableRef target_2_w = wt.get_table("target_2");
        target_1_w->remove_column(1);
        target_2_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, target_1->get_column_count());
    CHECK_EQUAL(1, target_2->get_column_count());
    CHECK_EQUAL(type_Mixed, target_1->get_column_type(0));
    CHECK_EQUAL(type_Link,  target_2->get_column_type(0));
    CHECK_EQUAL(3, target_1->size());
    CHECK_EQUAL(3, target_2->size());
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_1, 0));
    CHECK_EQUAL(2, target_1->get_backlink_count(0, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(0, *origin_2, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 0));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_1, 4));
    CHECK_EQUAL(1, target_1->get_backlink_count(1, *origin_2, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 0));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_1, 4));
    CHECK_EQUAL(0, target_1->get_backlink_count(2, *origin_2, 0));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));

    // Check that when the last column is removed from a target column, then its
    // size (number of rows) jumps to zero, and all links to it a removed or
    // nullified.
    {
        WriteTransaction wt(sg_w);
        TableRef target_1_w = wt.get_table("target_1");
        target_1_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, target_1->get_column_count());
    CHECK_EQUAL(1, target_2->get_column_count());
    CHECK_EQUAL(type_Link,  target_2->get_column_type(0));
    CHECK_EQUAL(3, origin_1->size());
    CHECK_EQUAL(3, origin_2->size());
    CHECK_EQUAL(0, target_1->size());
    CHECK_EQUAL(3, target_2->size());
    CHECK(origin_1->is_null_link(0,0));
    CHECK(origin_1->is_null_link(0,1));
    CHECK(origin_1->is_null_link(0,2));
    CHECK_EQUAL(1, origin_1->get_link(2,0));
    CHECK(origin_1->is_null_link(2,1));
    CHECK_EQUAL(0, origin_1->get_link(2,2));
    CHECK(origin_2->is_null_link(0,0));
    CHECK(origin_2->is_null_link(0,1));
    CHECK(origin_2->is_null_link(0,2));
    CHECK_EQUAL(0, origin_2->get_link(4,0));
    CHECK_EQUAL(1, origin_2->get_link(4,1));
    CHECK(origin_2->is_null_link(4,2));
    CHECK(link_list_1_0->is_attached());
    CHECK(link_list_1_1->is_attached());
    CHECK(link_list_1_2->is_attached());
    CHECK(link_list_2_0->is_attached());
    CHECK(link_list_2_1->is_attached());
    CHECK(link_list_2_2->is_attached());
    CHECK_EQUAL(link_list_1_0, origin_1->get_linklist(4,0));
    CHECK_EQUAL(link_list_1_1, origin_1->get_linklist(4,1));
    CHECK_EQUAL(link_list_1_2, origin_1->get_linklist(4,2));
    CHECK_EQUAL(link_list_2_0, origin_2->get_linklist(2,0));
    CHECK_EQUAL(link_list_2_1, origin_2->get_linklist(2,1));
    CHECK_EQUAL(link_list_2_2, origin_2->get_linklist(2,2));
    CHECK_EQUAL(0, link_list_1_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_1_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_1_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_2_0->get_origin_row_index());
    CHECK_EQUAL(1, link_list_2_1->get_origin_row_index());
    CHECK_EQUAL(2, link_list_2_2->get_origin_row_index());
    CHECK_EQUAL(0, link_list_1_0->size());
    CHECK_EQUAL(0, link_list_1_1->size());
    CHECK_EQUAL(0, link_list_1_2->size());
    CHECK_EQUAL(2, link_list_2_0->size());
    CHECK_EQUAL(1, link_list_2_0->get(0).get_index());
    CHECK_EQUAL(1, link_list_2_0->get(1).get_index());
    CHECK_EQUAL(0, link_list_2_1->size());
    CHECK_EQUAL(1, link_list_2_2->size());
    CHECK_EQUAL(0, link_list_2_2->get(0).get_index());
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_1, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(0, *origin_2, 4));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_1, 2));
    CHECK_EQUAL(2, target_2->get_backlink_count(1, *origin_2, 2));
    CHECK_EQUAL(1, target_2->get_backlink_count(1, *origin_2, 4));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_1, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 2));
    CHECK_EQUAL(0, target_2->get_backlink_count(2, *origin_2, 4));
}


TEST(LangBindHelper_AdvanceReadTransact_LinkCycles)
{
    // This test checks that cyclic link relationships work across transaction
    // boundaries (advance transaction). The simplest cyclic link relationship
    // (shortest cycle) is when a table has a link column whose links point to
    // rows in the same table, but longer cycles are also checked.

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Test that a table can refer to itself. First check that it works when the
    // link column is added to a pre-existing table, then check that it works
    // when the table and the link column is created in the same transaction.
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.add_table("table");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    ConstTableRef table = group.get_table("table");
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("table");
        table_w->add_column_link(type_Link,     "foo", *table_w);
        table_w->add_column_link(type_LinkList, "bar", *table_w);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK_EQUAL(2, table->get_column_count());
    CHECK_EQUAL(type_Link,     table->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table->get_column_type(1));
    CHECK_EQUAL(table, table->get_link_target(0));
    CHECK_EQUAL(table, table->get_link_target(1));
    CHECK(table->is_empty());
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("table");
        table_w->add_empty_row();
        table_w->set_link(0,0,0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK_EQUAL(table, table->get_link_target(0));
    CHECK_EQUAL(table, table->get_link_target(1));
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(0, table->get_link(0,0));
    ConstLinkViewRef link_list = table->get_linklist(1,0);
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK(link_list->is_empty());
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 0));
    CHECK_EQUAL(0, table->get_backlink_count(0, *table, 1));
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("table");
        table_w->get_linklist(1,0)->add(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK_EQUAL(table, table->get_link_target(0));
    CHECK_EQUAL(table, table->get_link_target(1));
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(0, table->get_link(0,0));
    CHECK(link_list->is_attached());
    CHECK_EQUAL(link_list, table->get_linklist(1,0));
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK_EQUAL(1, link_list->size());
    ConstRow row = link_list->get(0);
    CHECK_EQUAL(table, row.get_table());
    CHECK_EQUAL(0, row.get_index());
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 0));
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 1));
    {
        WriteTransaction wt(sg_w);
        TableRef table_2_w = wt.add_table("table_2");
        table_2_w->add_column_link(type_Link,     "foo", *table_2_w);
        table_2_w->add_column_link(type_LinkList, "bar", *table_2_w);
        table_2_w->add_empty_row();
        table_2_w->set_link(0,0,0);
        table_2_w->get_linklist(1,0)->add(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    ConstTableRef table_2 = group.get_table("table_2");
    CHECK_EQUAL(2, table_2->get_column_count());
    CHECK_EQUAL(type_Link,     table_2->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table_2->get_column_type(1));
    CHECK_EQUAL(table_2, table_2->get_link_target(0));
    CHECK_EQUAL(table_2, table_2->get_link_target(1));
    CHECK_EQUAL(1, table_2->size());
    CHECK_EQUAL(0, table_2->get_link(0,0));
    ConstLinkViewRef link_list_2 = table_2->get_linklist(1,0);
    CHECK_EQUAL(table_2, &link_list_2->get_origin_table());
    CHECK_EQUAL(table_2, &link_list_2->get_target_table());
    CHECK_EQUAL(1, link_list_2->size());
    ConstRow row_2 = link_list_2->get(0);
    CHECK_EQUAL(table_2, row_2.get_table());
    CHECK_EQUAL(0, row_2.get_index());
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table_2, 0));
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table_2, 1));

    // Test that a table A can refer to table B, and B to A. First check that it
    // works when the link columns are added to pre-existing tables, then check
    // that it works when the tables and the link columns are created in the
    // same transaction.
    {
        WriteTransaction wt(sg_w);
        TableRef table_w   = wt.get_table("table");
        TableRef table_2_w = wt.get_table("table_2");
        table_w->add_column_link(type_Link,       "foobar", *table_2_w);
        table_2_w->add_column_link(type_LinkList, "barfoo", *table_w);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK(table_2->is_attached());
    CHECK_EQUAL(3, table->get_column_count());
    CHECK_EQUAL(3, table_2->get_column_count());
    CHECK_EQUAL(type_Link,     table->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table->get_column_type(1));
    CHECK_EQUAL(type_Link,     table->get_column_type(2));
    CHECK_EQUAL(type_Link,     table_2->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table_2->get_column_type(1));
    CHECK_EQUAL(type_LinkList, table_2->get_column_type(2));
    CHECK_EQUAL(table,   table->get_link_target(0));
    CHECK_EQUAL(table,   table->get_link_target(1));
    CHECK_EQUAL(table_2, table->get_link_target(2));
    CHECK_EQUAL(table_2, table_2->get_link_target(0));
    CHECK_EQUAL(table_2, table_2->get_link_target(1));
    CHECK_EQUAL(table,   table_2->get_link_target(2));
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(1, table_2->size());
    CHECK_EQUAL(0, table->get_link(0,0));
    CHECK(table->is_null_link(2,0));
    CHECK(link_list->is_attached());
    CHECK_EQUAL(link_list, table->get_linklist(1,0));
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK_EQUAL(1, link_list->size());
    row = link_list->get(0);
    CHECK_EQUAL(table, row.get_table());
    CHECK_EQUAL(0, row.get_index());
    CHECK_EQUAL(0, table_2->get_link(0,0));
    CHECK(link_list_2->is_attached());
    CHECK_EQUAL(link_list_2, table_2->get_linklist(1,0));
    CHECK_EQUAL(table_2, &link_list_2->get_origin_table());
    CHECK_EQUAL(table_2, &link_list_2->get_target_table());
    CHECK_EQUAL(1, link_list_2->size());
    row_2 = link_list_2->get(0);
    CHECK_EQUAL(table_2, row_2.get_table());
    CHECK_EQUAL(0, row_2.get_index());
    ConstLinkViewRef link_list_3 = table_2->get_linklist(2,0);
    CHECK_EQUAL(table_2, &link_list_3->get_origin_table());
    CHECK_EQUAL(table,   &link_list_3->get_target_table());
    CHECK(link_list_3->is_empty());
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 0));
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 1));
    CHECK_EQUAL(0, table->get_backlink_count(0, *table_2, 2));
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table_2, 0));
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table_2, 1));
    CHECK_EQUAL(0, table_2->get_backlink_count(0, *table, 2));
    {
        WriteTransaction wt(sg_w);
        TableRef table_w   = wt.get_table("table");
        TableRef table_2_w = wt.get_table("table_2");
        table_w->set_link(2,0,0);
        table_2_w->get_linklist(2,0)->add(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK(table_2->is_attached());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(1, table_2->size());
    CHECK_EQUAL(0, table->get_link(0,0));
    CHECK_EQUAL(0, table->get_link(2,0));
    CHECK(link_list->is_attached());
    CHECK_EQUAL(link_list, table->get_linklist(1,0));
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK_EQUAL(1, link_list->size());
    row = link_list->get(0);
    CHECK_EQUAL(table, row.get_table());
    CHECK_EQUAL(0, row.get_index());
    CHECK_EQUAL(0, table_2->get_link(0,0));
    CHECK(link_list_2->is_attached());
    CHECK_EQUAL(link_list_2, table_2->get_linklist(1,0));
    CHECK_EQUAL(table_2, &link_list_2->get_origin_table());
    CHECK_EQUAL(table_2, &link_list_2->get_target_table());
    CHECK_EQUAL(1, link_list_2->size());
    row_2 = link_list_2->get(0);
    CHECK_EQUAL(table_2, row_2.get_table());
    CHECK_EQUAL(0, row_2.get_index());
    CHECK(link_list_3->is_attached());
    CHECK_EQUAL(link_list_3, table_2->get_linklist(2,0));
    CHECK_EQUAL(table_2, &link_list_3->get_origin_table());
    CHECK_EQUAL(table,   &link_list_3->get_target_table());
    CHECK_EQUAL(1, link_list_3->size());
    ConstRow row_3 = link_list_3->get(0);
    CHECK_EQUAL(table, row_3.get_table());
    CHECK_EQUAL(0, row_3.get_index());
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 0));
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 1));
    CHECK_EQUAL(1, table->get_backlink_count(0, *table_2, 2));
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table_2, 0));
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table_2, 1));
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table, 2));
    {
        WriteTransaction wt(sg_w);
        TableRef table_3_w = wt.add_table("table_3");
        TableRef table_4_w = wt.add_table("table_4");
        table_3_w->add_column_link(type_LinkList, "foobar_2", *table_4_w);
        table_4_w->add_column_link(type_Link,     "barfoo_2", *table_3_w);
        table_3_w->add_empty_row();
        table_4_w->add_empty_row();
        table_3_w->get_linklist(0,0)->add(0);
        table_4_w->set_link(0,0,0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    ConstTableRef table_3 = group.get_table("table_3");
    ConstTableRef table_4 = group.get_table("table_4");
    CHECK_EQUAL(1, table_3->get_column_count());
    CHECK_EQUAL(1, table_4->get_column_count());
    CHECK_EQUAL(type_LinkList, table_3->get_column_type(0));
    CHECK_EQUAL(type_Link,     table_4->get_column_type(0));
    CHECK_EQUAL(table_4, table_3->get_link_target(0));
    CHECK_EQUAL(table_3, table_4->get_link_target(0));
    CHECK_EQUAL(1, table_3->size());
    CHECK_EQUAL(1, table_4->size());
    ConstLinkViewRef link_list_4 = table_3->get_linklist(0,0);
    CHECK_EQUAL(table_3, &link_list_4->get_origin_table());
    CHECK_EQUAL(table_4, &link_list_4->get_target_table());
    CHECK_EQUAL(1, link_list_4->size());
    ConstRow row_4 = link_list_4->get(0);
    CHECK_EQUAL(table_4, row_4.get_table());
    CHECK_EQUAL(0, row_4.get_index());
    CHECK_EQUAL(0, table_4->get_link(0,0));
    CHECK_EQUAL(1, table_3->get_backlink_count(0, *table_4, 0));
    CHECK_EQUAL(1, table_4->get_backlink_count(0, *table_3, 0));

    // Check that columns can be removed even when they are part of link
    // relationship cycles
    {
        WriteTransaction wt(sg_w);
        TableRef table_w   = wt.get_table("table");
        TableRef table_2_w = wt.get_table("table_2");
        TableRef table_3_w = wt.get_table("table_3");
        table_w->remove_column(0);
        table_2_w->remove_column(0);
        table_2_w->remove_column(0);
        table_3_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK(table_2->is_attached());
    CHECK(table_3->is_attached());
    CHECK(table_4->is_attached());
    CHECK_EQUAL(2, table->get_column_count());
    CHECK_EQUAL(1, table_2->get_column_count());
    CHECK_EQUAL(0, table_3->get_column_count());
    CHECK_EQUAL(1, table_4->get_column_count());
    CHECK_EQUAL(type_LinkList, table->get_column_type(0));
    CHECK_EQUAL(type_Link,     table->get_column_type(1));
    CHECK_EQUAL(type_LinkList, table_2->get_column_type(0));
    CHECK_EQUAL(type_Link,     table_4->get_column_type(0));
    CHECK_EQUAL(table,   table->get_link_target(0));
    CHECK_EQUAL(table_2, table->get_link_target(1));
    CHECK_EQUAL(table,   table_2->get_link_target(0));
    CHECK_EQUAL(table_3, table_4->get_link_target(0));
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(1, table_2->size());
    CHECK_EQUAL(0, table_3->size());
    CHECK_EQUAL(1, table_4->size());
    CHECK(link_list->is_attached());
    CHECK_EQUAL(link_list, table->get_linklist(0,0));
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK_EQUAL(1, link_list->size());
    row = link_list->get(0);
    CHECK_EQUAL(table, row.get_table());
    CHECK_EQUAL(0, row.get_index());
    CHECK_EQUAL(0, table->get_link(1,0));
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 0));
    CHECK_EQUAL(1, table->get_backlink_count(0, *table_2, 0));
    CHECK(!link_list_2->is_attached());
    CHECK(link_list_3->is_attached());
    CHECK_EQUAL(link_list_3, table_2->get_linklist(0,0));
    CHECK_EQUAL(table_2, &link_list_3->get_origin_table());
    CHECK_EQUAL(table,   &link_list_3->get_target_table());
    CHECK_EQUAL(1, link_list_3->size());
    row_3 = link_list_3->get(0);
    CHECK_EQUAL(table, row_3.get_table());
    CHECK_EQUAL(0, row_3.get_index());
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table, 1));
    CHECK(!link_list_4->is_attached());
    CHECK(table_4->is_null_link(0,0));
    {
        WriteTransaction wt(sg_w);
        TableRef table_w   = wt.get_table("table");
        TableRef table_2_w = wt.get_table("table_2");
        TableRef table_4_w = wt.get_table("table_4");
        table_w->remove_column(1);
        table_2_w->remove_column(0);
        table_4_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK(table_2->is_attached());
    CHECK(table_3->is_attached());
    CHECK(table_4->is_attached());
    CHECK_EQUAL(1, table->get_column_count());
    CHECK_EQUAL(0, table_2->get_column_count());
    CHECK_EQUAL(0, table_3->get_column_count());
    CHECK_EQUAL(0, table_4->get_column_count());
    CHECK_EQUAL(type_LinkList, table->get_column_type(0));
    CHECK_EQUAL(table, table->get_link_target(0));
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(0, table_2->size());
    CHECK_EQUAL(0, table_3->size());
    CHECK_EQUAL(0, table_4->size());
    CHECK(link_list->is_attached());
    CHECK_EQUAL(link_list, table->get_linklist(0,0));
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK_EQUAL(1, link_list->size());
    row = link_list->get(0);
    CHECK_EQUAL(table, row.get_table());
    CHECK_EQUAL(0, row.get_index());
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 0));
    CHECK(!link_list_3->is_attached());
    {
        WriteTransaction wt(sg_w);
        TableRef table_w   = wt.get_table("table");
        table_w->remove_column(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK(table_2->is_attached());
    CHECK(table_3->is_attached());
    CHECK(table_4->is_attached());
    CHECK_EQUAL(0, table->get_column_count());
    CHECK_EQUAL(0, table_2->get_column_count());
    CHECK_EQUAL(0, table_3->get_column_count());
    CHECK_EQUAL(0, table_4->get_column_count());
    CHECK_EQUAL(0, table->size());
    CHECK_EQUAL(0, table_2->size());
    CHECK_EQUAL(0, table_3->size());
    CHECK_EQUAL(0, table_4->size());
    CHECK(!link_list->is_attached());

    // Check that a row can be removed even when it participates in a link cycle
    {
        WriteTransaction wt(sg_w);
        TableRef table_w   = wt.get_table("table");
        table_w->add_column_link(type_Link,     "a", *table_w);
        table_w->add_column_link(type_LinkList, "b", *table_w);
        table_w->add_empty_row();
        table_w->set_link(0,0,0);
        table_w->get_linklist(1,0)->add(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK_EQUAL(2, table->get_column_count());
    CHECK_EQUAL(type_Link,     table->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table->get_column_type(1));
    CHECK_EQUAL(table, table->get_link_target(0));
    CHECK_EQUAL(table, table->get_link_target(1));
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(0, table->get_link(0,0));
    CHECK_NOT_EQUAL(link_list, table->get_linklist(1,0));
    link_list = table->get_linklist(1,0);
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK_EQUAL(1, link_list->size());
    row = link_list->get(0);
    CHECK_EQUAL(table, row.get_table());
    CHECK_EQUAL(0, row.get_index());
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("table");
        table_w->add_empty_row(2);
        table_w->move_last_over(0);
        table_w->set_link(0,0,1);
        table_w->set_link(0,1,0);
        table_w->get_linklist(1,0)->add(1);
        table_w->get_linklist(1,1)->add(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK_EQUAL(2, table->get_column_count());
    CHECK_EQUAL(type_Link,     table->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table->get_column_type(1));
    CHECK_EQUAL(table, table->get_link_target(0));
    CHECK_EQUAL(table, table->get_link_target(1));
    CHECK_EQUAL(2, table->size());
    CHECK_EQUAL(1, table->get_link(0,0));
    CHECK_EQUAL(0, table->get_link(0,1));
    CHECK(!link_list->is_attached());
    CHECK_NOT_EQUAL(link_list, table->get_linklist(1,0));
    link_list   = table->get_linklist(1,0);
    link_list_2 = table->get_linklist(1,1);
    CHECK_EQUAL(1, link_list->size());
    CHECK_EQUAL(1, link_list_2->size());
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 0));
    CHECK_EQUAL(1, table->get_backlink_count(0, *table, 1));
    CHECK_EQUAL(1, table->get_backlink_count(1, *table, 0));
    CHECK_EQUAL(1, table->get_backlink_count(1, *table, 1));
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("table");
        table_w->move_last_over(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table->is_attached());
    CHECK_EQUAL(2, table->get_column_count());
    CHECK_EQUAL(type_Link,     table->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table->get_column_type(1));
    CHECK_EQUAL(table, table->get_link_target(0));
    CHECK_EQUAL(table, table->get_link_target(1));
    CHECK_EQUAL(1, table->size());
    CHECK(table->is_null_link(0,0));
    CHECK(!link_list->is_attached());
    CHECK(link_list_2->is_attached());
    CHECK_EQUAL(link_list_2, table->get_linklist(1,0));
    link_list = link_list_2;
    link_list_2.reset();
    CHECK_EQUAL(table, &link_list->get_origin_table());
    CHECK_EQUAL(table, &link_list->get_target_table());
    CHECK(link_list->is_empty());
    CHECK_EQUAL(0, table->get_backlink_count(0, *table, 0));
    CHECK_EQUAL(0, table->get_backlink_count(0, *table, 1));
    {
        WriteTransaction wt(sg_w);
        TableRef table_2_w = wt.get_table("table_2");
        TableRef table_3_w = wt.get_table("table_3");
        table_2_w->add_column_link(type_Link,     "col_1", *table_3_w);
        table_3_w->add_column_link(type_LinkList, "col_2", *table_2_w);
        table_2_w->add_empty_row();
        table_3_w->add_empty_row();
        table_2_w->set_link(0,0,0);
        table_3_w->get_linklist(0,0)->add(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table_2->is_attached());
    CHECK(table_3->is_attached());
    CHECK_EQUAL(1, table_2->get_column_count());
    CHECK_EQUAL(1, table_3->get_column_count());
    CHECK_EQUAL(type_Link,     table_2->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table_3->get_column_type(0));
    CHECK_EQUAL(table_3, table_2->get_link_target(0));
    CHECK_EQUAL(table_2, table_3->get_link_target(0));
    CHECK_EQUAL(1, table_2->size());
    CHECK_EQUAL(1, table_3->size());
    CHECK_EQUAL(0, table_2->get_link(0,0));
    link_list_3 = table_3->get_linklist(0,0);
    CHECK_EQUAL(table_3, &link_list_3->get_origin_table());
    CHECK_EQUAL(table_2, &link_list_3->get_target_table());
    CHECK_EQUAL(1, link_list_3->size());
    row_3 = link_list_3->get(0);
    CHECK_EQUAL(table_2, row_3.get_table());
    CHECK_EQUAL(0, row_3.get_index());
    CHECK_EQUAL(1, table_2->get_backlink_count(0, *table_3, 0));
    CHECK_EQUAL(1, table_3->get_backlink_count(0, *table_2, 0));
    {
        WriteTransaction wt(sg_w);
        TableRef table_2_w = wt.get_table("table_2");
        table_2_w->move_last_over(0);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    CHECK(table_2->is_attached());
    CHECK(table_3->is_attached());
    CHECK_EQUAL(1, table_2->get_column_count());
    CHECK_EQUAL(1, table_3->get_column_count());
    CHECK_EQUAL(type_Link,     table_2->get_column_type(0));
    CHECK_EQUAL(type_LinkList, table_3->get_column_type(0));
    CHECK_EQUAL(table_3, table_2->get_link_target(0));
    CHECK_EQUAL(table_2, table_3->get_link_target(0));
    CHECK(table_2->is_empty());
    CHECK_EQUAL(1, table_3->size());
    CHECK(link_list_3->is_attached());
    CHECK_EQUAL(link_list_3, table_3->get_linklist(0,0));
    CHECK_EQUAL(table_3, &link_list_3->get_origin_table());
    CHECK_EQUAL(table_2, &link_list_3->get_target_table());
    CHECK(link_list_3->is_empty());
    CHECK_EQUAL(0, table_3->get_backlink_count(0, *table_2, 0));
}


TEST(LangBindHelper_AdvanceReadTransact_InsertLink)
{
    // This test checks that Table::insert_link() works across transaction
    // boundaries (advance transaction).

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    {
        WriteTransaction wt(sg_w);
        TableRef origin_w = wt.add_table("origin");
        TableRef target_w = wt.add_table("target");
        origin_w->add_column_link(type_Link, "", *target_w);
        target_w->add_column(type_Int, "");
        target_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
    ConstTableRef origin = group.get_table("origin");
    ConstTableRef target = group.get_table("target");
    {
        WriteTransaction wt(sg_w);
        TableRef origin_w = wt.get_table("origin");
        origin_w->insert_link(0,0,0);
        origin_w->insert_done();
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();
}


TEST(LangBindHelper_AdvanceReadTransact_RemoveTableWithColumns)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    {
        WriteTransaction wt(sg_w);
        TableRef alpha_w   = wt.add_table("alpha");
        TableRef beta_w    = wt.add_table("beta");
        TableRef gamma_w   = wt.add_table("gamma");
        TableRef delta_w   = wt.add_table("delta");
        TableRef epsilon_w = wt.add_table("epsilon");
        alpha_w->add_column(type_Int, "alpha-1");
        beta_w->add_column_link(type_Link, "beta-1", *delta_w);
        gamma_w->add_column_link(type_Link, "gamma-1", *gamma_w);
        delta_w->add_column(type_Int, "delta-1");
        epsilon_w->add_column_link(type_Link, "epsilon-1", *delta_w);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    CHECK_EQUAL(5, group.size());
    ConstTableRef alpha   = group.get_table("alpha");
    ConstTableRef beta    = group.get_table("beta");
    ConstTableRef gamma   = group.get_table("gamma");
    ConstTableRef delta   = group.get_table("delta");
    ConstTableRef epsilon = group.get_table("epsilon");

    // Remove table with columns, but no link columns, and table is not a link
    // target.
    {
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table("alpha");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    CHECK_EQUAL(4, group.size());
    CHECK_NOT(alpha->is_attached());
    CHECK(beta->is_attached());
    CHECK(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());

    // Remove table with link column, and table is not a link target.
    {
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table("beta");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    CHECK_EQUAL(3, group.size());
    CHECK_NOT(beta->is_attached());
    CHECK(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());

    // Remove table with self-link column, and table is not a target of link
    // columns of other tables.
    {
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table("gamma");
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    CHECK_EQUAL(2, group.size());
    CHECK_NOT(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());

    // Try, but fail to remove table which is a target of link columns of other
    // tables.
    {
        WriteTransaction wt(sg_w);
        CHECK_THROW(wt.get_group().remove_table("delta"), CrossTableLinkTarget);
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    CHECK_EQUAL(2, group.size());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_RemoveTableMovesTableWithLinksOver)
{
    // Create a scenario where a table is removed from the group, and the last
    // table in the group (which will be moved into the vacated slot) has both
    // link and backlink columns.

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, test_util::key);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm, SharedGroup::durability_Full, test_util::key);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    string names[4];
    {
        WriteTransaction wt(sg_w);
        wt.add_table("alpha");
        wt.add_table("beta");
        wt.add_table("gamma");
        wt.add_table("delta");
        names[0] = wt.get_group().get_table_name(0);
        names[1] = wt.get_group().get_table_name(1);
        names[2] = wt.get_group().get_table_name(2);
        names[3] = wt.get_group().get_table_name(3);
        TableRef first_w  = wt.get_table(names[0]);
        TableRef third_w  = wt.get_table(names[2]);
        TableRef fourth_w = wt.get_table(names[3]);
        first_w->add_column_link(type_Link,  "one",   *third_w);
        third_w->add_column_link(type_Link,  "two",   *fourth_w);
        third_w->add_column_link(type_Link,  "three", *third_w);
        fourth_w->add_column_link(type_Link, "four",  *first_w);
        fourth_w->add_column_link(type_Link, "five",  *third_w);
        first_w->add_empty_row(2);
        third_w->add_empty_row(2);
        fourth_w->add_empty_row(2);
        first_w->set_link(0,0,0);  // first[0].one   = third[0]
        first_w->set_link(0,1,1);  // first[1].one   = third[1]
        third_w->set_link(0,0,1);  // third[0].two   = fourth[1]
        third_w->set_link(0,1,0);  // third[1].two   = fourth[0]
        third_w->set_link(1,0,1);  // third[0].three = third[1]
        third_w->set_link(1,1,1);  // third[1].three = third[1]
        fourth_w->set_link(0,0,0); // fourth[0].four = first[0]
        fourth_w->set_link(0,1,0); // fourth[1].four = first[0]
        fourth_w->set_link(1,0,0); // fourth[0].five = third[0]
        fourth_w->set_link(1,1,1); // fourth[1].five = third[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    ConstTableRef first  = group.get_table(names[0]);
    ConstTableRef second = group.get_table(names[1]);
    ConstTableRef third  = group.get_table(names[2]);
    ConstTableRef fourth = group.get_table(names[3]);

    {
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table(1); // Second
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    CHECK_EQUAL(3, group.size());
    CHECK(first->is_attached());
    CHECK_NOT(second->is_attached());
    CHECK(third->is_attached());
    CHECK(fourth->is_attached());
    CHECK_EQUAL(1, first->get_column_count());
    CHECK_EQUAL("one", first->get_column_name(0));
    CHECK_EQUAL(third, first->get_link_target(0));
    CHECK_EQUAL(2, third->get_column_count());
    CHECK_EQUAL("two",   third->get_column_name(0));
    CHECK_EQUAL("three", third->get_column_name(1));
    CHECK_EQUAL(fourth, third->get_link_target(0));
    CHECK_EQUAL(third,  third->get_link_target(1));
    CHECK_EQUAL(2, fourth->get_column_count());
    CHECK_EQUAL("four", fourth->get_column_name(0));
    CHECK_EQUAL("five", fourth->get_column_name(1));
    CHECK_EQUAL(first, fourth->get_link_target(0));
    CHECK_EQUAL(third, fourth->get_link_target(1));

    {
        WriteTransaction wt(sg_w);
        TableRef first_w  = wt.get_table(names[0]);
        TableRef third_w  = wt.get_table(names[2]);
        TableRef fourth_w = wt.get_table(names[3]);
        third_w->set_link(0,0,0);  // third[0].two   = fourth[0]
        fourth_w->set_link(0,1,1); // fourth[1].four = first[1]
        first_w->set_link(0,0,1);  // first[0].one   = third[1]
        wt.commit();
    }
    LangBindHelper::advance_read(sg, tlm);
    group.Verify();

    CHECK_EQUAL(2, first->size());
    CHECK_EQUAL(1, first->get_link(0,0));
    CHECK_EQUAL(1, first->get_link(0,1));
    CHECK_EQUAL(1, first->get_backlink_count(0, *fourth, 0));
    CHECK_EQUAL(1, first->get_backlink_count(1, *fourth, 0));
    CHECK_EQUAL(2, third->size());
    CHECK_EQUAL(0, third->get_link(0,0));
    CHECK_EQUAL(0, third->get_link(0,1));
    CHECK_EQUAL(1, third->get_link(1,0));
    CHECK_EQUAL(1, third->get_link(1,1));
    CHECK_EQUAL(0, third->get_backlink_count(0, *first,  0));
    CHECK_EQUAL(2, third->get_backlink_count(1, *first,  0));
    CHECK_EQUAL(0, third->get_backlink_count(0, *third,  1));
    CHECK_EQUAL(2, third->get_backlink_count(1, *third,  1));
    CHECK_EQUAL(1, third->get_backlink_count(0, *fourth, 1));
    CHECK_EQUAL(1, third->get_backlink_count(1, *fourth, 1));
    CHECK_EQUAL(2, fourth->size());
    CHECK_EQUAL(0, fourth->get_link(0,0));
    CHECK_EQUAL(1, fourth->get_link(0,1));
    CHECK_EQUAL(0, fourth->get_link(1,0));
    CHECK_EQUAL(1, fourth->get_link(1,1));
    CHECK_EQUAL(2, fourth->get_backlink_count(0, *third, 0));
    CHECK_EQUAL(0, fourth->get_backlink_count(1, *third, 0));
}


TEST(LangBindHelper_ImplicitTransactions)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    {
        WriteTransaction wt(sg);
        wt.add_table<TestTableShared>("table")->add_empty_row();
        wt.commit();
    }
    UniquePtr<Replication> repl2(makeWriteLogCollector(path));
    SharedGroup sg2(*repl, SharedGroup::durability_Full, test_util::key);
    Group& g = const_cast<Group&>(sg.begin_read());
    TestTableShared::Ref table = g.get_table<TestTableShared>("table");
    for (int i = 0; i<100; i++) {
        {
            // change table in other context
            WriteTransaction wt(sg2);
            wt.get_table<TestTableShared>("table")[0].first += 100;
            wt.commit();
        }
        // verify we can't see the update
        CHECK_EQUAL(i, table[0].first);
        LangBindHelper::advance_read(sg, *wlr);
        // now we CAN see it, and through the same accessor
        CHECK(table->is_attached());
        CHECK_EQUAL(i + 100, table[0].first);
        {
            // change table in other context
            WriteTransaction wt(sg2);
            wt.get_table<TestTableShared>("table")[0].first += 10000;
            wt.commit();
        }
        // can't see it:
        CHECK_EQUAL(i + 100, table[0].first);
        LangBindHelper::promote_to_write(sg, *wlr);
        // CAN see it:
        CHECK(table->is_attached());
        CHECK_EQUAL(i + 10100, table[0].first);
        table[0].first -= 10100;
        table[0].first += 1;
        LangBindHelper::commit_and_continue_as_read(sg);
        CHECK(table->is_attached());
        CHECK_EQUAL(i+1, table[0].first);
    }
    sg.end_read();
}


TEST(LangBindHelper_RollbackAndContinueAsRead)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    {
        Group* group = const_cast<Group*>(&sg.begin_read());
       {
            LangBindHelper::promote_to_write(sg, *wlr);
            TableRef origin = group->get_or_add_table("origin");
            origin->add_column(type_Int, "");
            origin->add_empty_row();
            origin->set_int(0,0,42);
            LangBindHelper::commit_and_continue_as_read(sg);
        }
        group->Verify();
        {
            // rollback of group level table insertion
            LangBindHelper::promote_to_write(sg, *wlr);
            TableRef o = group->get_or_add_table("nullermand");
            TableRef o2 = group->get_table("nullermand");
            TIGHTDB_ASSERT(o2);
            LangBindHelper::rollback_and_continue_as_read(sg);
            TableRef o3 = group->get_table("nullermand");
            TIGHTDB_ASSERT(!o3);
            TIGHTDB_ASSERT(o2->is_attached() == false);
        }

        TableRef origin = group->get_table("origin");
        Row row = origin->get(0);
        CHECK_EQUAL(42, origin->get_int(0,0));

        {
            LangBindHelper::promote_to_write(sg, *wlr);
            origin->insert_empty_row(0);
            origin->set_int(0,0,5746);
            CHECK_EQUAL(42, origin->get_int(0,1));
            CHECK_EQUAL(5746, origin->get_int(0,0));
            CHECK_EQUAL(42, row.get_int(0));
            CHECK_EQUAL(2, origin->size());
            group->Verify();
            LangBindHelper::rollback_and_continue_as_read(sg);
        }
        CHECK_EQUAL(1, origin->size());
        group->Verify();
        CHECK_EQUAL(42, origin->get_int(0,0));
        CHECK_EQUAL(42, row.get_int(0));

        {
            LangBindHelper::promote_to_write(sg, *wlr);
            origin->add_empty_row();
            origin->set_int(0,1,42);
            LangBindHelper::commit_and_continue_as_read(sg);
        }
        Row row2 = origin->get(1);
        CHECK_EQUAL(2, origin->size());

        {
            LangBindHelper::promote_to_write(sg, *wlr);
            origin->move_last_over(0);
            CHECK_EQUAL(1, origin->size());
            CHECK_EQUAL(42, row2.get_int(0));
            CHECK_EQUAL(42, origin->get_int(0,0));
            group->Verify();
            LangBindHelper::rollback_and_continue_as_read(sg);
        }
        CHECK_EQUAL(2, origin->size());
        group->Verify();
        CHECK_EQUAL(42, row2.get_int(0));
        CHECK_EQUAL(42, origin->get_int(0,1));
        sg.end_read();
    }
}


TEST(LangBindHelper_RollbackAndContinueAsReadGroupLevelTableRemoval)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());
    {
        LangBindHelper::promote_to_write(sg, *wlr);
        TableRef origin = group->get_or_add_table("a_table");
        LangBindHelper::commit_and_continue_as_read(sg);
    }
    group->Verify();
    {
        // rollback of group level table delete
        LangBindHelper::promote_to_write(sg, *wlr);
        TableRef o2 = group->get_table("a_table");
        TIGHTDB_ASSERT(o2);
        group->remove_table("a_table");
        TableRef o3 = group->get_table("a_table");
        TIGHTDB_ASSERT(!o3);
        LangBindHelper::rollback_and_continue_as_read(sg);
        TableRef o4 = group->get_table("a_table");
        TIGHTDB_ASSERT(o4);
    }
    group->Verify();
}


TEST(LangBindHelper_RollbackAndContinueAsReadColumnAdd)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());
    TableRef t;
    {
        LangBindHelper::promote_to_write(sg, *wlr);
        t = group->get_or_add_table("a_table");
        t->add_column(type_Int, "lorelei");
        t->insert_empty_row(0);
        t->set_int(0,0,43);
        CHECK_EQUAL(1, t->get_descriptor()->get_column_count());
        LangBindHelper::commit_and_continue_as_read(sg);
    }
    group->Verify();
    {
        // add a column and regret it again
        LangBindHelper::promote_to_write(sg, *wlr);
        t->add_column(type_Int, "riget");
        t->set_int(1,0,44);
        CHECK_EQUAL(2, t->get_descriptor()->get_column_count());
        group->Verify();
        LangBindHelper::rollback_and_continue_as_read(sg);
        group->Verify();
        CHECK_EQUAL(1, t->get_descriptor()->get_column_count());
    }
    group->Verify();
}


TEST(LangBindHelper_RollbackAndContinueAsReadColumnRemove)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());
    TableRef t;
    {
        LangBindHelper::promote_to_write(sg, *wlr);
        t = group->get_or_add_table("a_table");
        t->add_column(type_Int, "lorelei");
        t->add_column(type_Int, "riget");
        t->insert_empty_row(0);
        t->set_int(0,0,43);
        t->set_int(1,0,44);
        CHECK_EQUAL(2, t->get_descriptor()->get_column_count());
        LangBindHelper::commit_and_continue_as_read(sg);
    }
    group->Verify();
    {
        // remove a column but regret it
        LangBindHelper::promote_to_write(sg, *wlr);
        CHECK_EQUAL(2, t->get_descriptor()->get_column_count());
        t->remove_column(0);
        group->Verify();
        LangBindHelper::rollback_and_continue_as_read(sg);
        group->Verify();
        CHECK_EQUAL(2, t->get_descriptor()->get_column_count());
    }
    group->Verify();
}


TEST(LangBindHelper_RollbackAndContinueAsReadLinkList)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());
    LangBindHelper::promote_to_write(sg, *tlr);
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    origin->add_empty_row();
    target->add_empty_row();
    target->add_empty_row();
    target->add_empty_row();
    LinkViewRef link_list = origin->get_linklist(0,0);
    link_list->add(0);
    LangBindHelper::commit_and_continue_as_read(sg);
    CHECK_EQUAL(1, link_list->size());
    group->Verify();
    // now change a link in link list and roll back the change
    LangBindHelper::promote_to_write(sg, *tlr);
    link_list->add(1);
    link_list->add(2);
    CHECK_EQUAL(3, link_list->size());
    LangBindHelper::rollback_and_continue_as_read(sg);
    CHECK_EQUAL(1, link_list->size());
    LangBindHelper::promote_to_write(sg, *tlr);
    link_list->remove(0);
    CHECK_EQUAL(0, link_list->size());
    LangBindHelper::rollback_and_continue_as_read(sg);
    CHECK_EQUAL(1, link_list->size());
    // verify that we can do move last over - first set link to last entry in target:
    LangBindHelper::promote_to_write(sg, *tlr);
    link_list->set(0,2); // link list holds single link to end of target
    LangBindHelper::commit_and_continue_as_read(sg);
    // then we test move last over:
    LangBindHelper::promote_to_write(sg, *tlr);
    CHECK_EQUAL(2, link_list->get(0).get_index()); // link restored
    target->move_last_over(0);
    CHECK_EQUAL(0, link_list->get(0).get_index()); // link was changed to 0 due to move last over
    LangBindHelper::rollback_and_continue_as_read(sg);
    CHECK_EQUAL(2, link_list->get(0).get_index()); // link restored
}


TEST(LangBindHelper_RollbackAndContinueAsReadLink)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());
    LangBindHelper::promote_to_write(sg, *tlr);
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    origin->add_column_link(type_Link, "", *target);
    target->add_column(type_Int, "");
    origin->add_empty_row();
    target->add_empty_row();
    target->add_empty_row();
    target->add_empty_row();
    origin->set_link(0, 0, 2); // points to last row in target
    CHECK_EQUAL(2, origin->get_link(0,0));
    LangBindHelper::commit_and_continue_as_read(sg);
    // verify that we can reverse a move last over:
    CHECK_EQUAL(2, origin->get_link(0,0));
    LangBindHelper::promote_to_write(sg, *tlr);
    target->move_last_over(1);
    CHECK_EQUAL(1, origin->get_link(0,0));
    LangBindHelper::rollback_and_continue_as_read(sg);
    CHECK_EQUAL(2, origin->get_link(0,0));
    // verify that we can revert a link change:
    LangBindHelper::promote_to_write(sg, *tlr);
    origin->set_link(0, 0, 1);
    LangBindHelper::rollback_and_continue_as_read(sg);
    CHECK_EQUAL(2, origin->get_link(0,0));
    // verify that we can revert addition of a row in target table
    LangBindHelper::promote_to_write(sg, *tlr);
    target->add_empty_row();
    LangBindHelper::rollback_and_continue_as_read(sg);
    CHECK_EQUAL(2, origin->get_link(0,0));
}

TEST(LangBindHelper_RollbackAndContinueAsRead_MoveLastOverSubtables)
{
    // adapted from earlier move last over test
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());

    CHECK_EQUAL(0, group->size());

    // Create three parent tables, each with with 5 rows, and each row
    // containing one regular and one mixed subtable
    {
        LangBindHelper::promote_to_write(sg, *tlr);
        for (int i = 0; i < 3; ++i) {
            const char* table_name = i == 0 ? "parent_1" : i == 1 ? "parent_2" : "parent_3";
            TableRef parent_w = group->add_table(table_name);
            parent_w->add_column(type_Table, "a");
            parent_w->add_column(type_Mixed, "b");
            DescriptorRef subdesc = parent_w->get_subdescriptor(0);
            subdesc->add_column(type_Int, "regular");
            parent_w->add_empty_row(5);
            for (int row_ndx = 0; row_ndx < 5; ++row_ndx) {
                TableRef regular_w = parent_w->get_subtable(0, row_ndx);
                regular_w->add_empty_row();
                regular_w->set_int(0, 0, 10 + row_ndx);
                parent_w->set_mixed(1, row_ndx, Mixed::subtable_tag());
                TableRef mixed_w = parent_w->get_subtable(1, row_ndx);
                mixed_w->add_column(type_Int, "mixed");
                mixed_w->add_empty_row();
                mixed_w->set_int(0, 0, 20 + row_ndx);
            }
        }
        LangBindHelper::commit_and_continue_as_read(sg);
    }
    group->Verify();

    // Use first table to check with accessors on row indexes 0, 1, and 4, but
    // none at index 2 and 3.
    {
        ConstTableRef parent = group->get_table("parent_1");
        ConstRow row_0 = (*parent)[0];
        ConstRow row_1 = (*parent)[1];
        ConstRow row_4 = (*parent)[4];
        ConstTableRef regular_0 = parent->get_subtable(0,0);
        ConstTableRef regular_1 = parent->get_subtable(0,1);
        ConstTableRef regular_4 = parent->get_subtable(0,4);
        ConstTableRef   mixed_0 = parent->get_subtable(1,0);
        ConstTableRef   mixed_1 = parent->get_subtable(1,1);
        ConstTableRef   mixed_4 = parent->get_subtable(1,4);
        CHECK(row_0.is_attached());
        CHECK(row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(4, row_4.get_index());
        CHECK(regular_0->is_attached());
        CHECK(regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(10, regular_0->get_int(0,0));
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_4 && mixed_4->is_attached());
        CHECK_EQUAL(20, mixed_0->get_int(0,0));
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(24, mixed_4->get_int(0,0));

        // Perform two 'move last over' operations which brings the number of
        // rows down from 5 to 3 ... then rollback to earlier state and verify
        {
            LangBindHelper::promote_to_write(sg, *tlr);
            TableRef parent_w = group->get_table("parent_1");
            parent_w->move_last_over(2); // Move row at index 4 to index 2
            parent_w->move_last_over(0); // Move row at index 3 to index 0
            LangBindHelper::rollback_and_continue_as_read(sg);
        }
        // even though we rollback, accessors to row_0 should have become
        // detached as part of the changes done before reverting, and once
        // detached, they are not magically attached again.
        CHECK(!row_0.is_attached());
        CHECK(row_1.is_attached());
        CHECK(row_4.is_attached());
        //CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(4, row_4.get_index());
        //CHECK(regular_0->is_attached());
        CHECK(regular_1->is_attached());
        CHECK(regular_4->is_attached());
        //CHECK_EQUAL(10, regular_0->get_int(0,0));
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        //CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_4 && mixed_4->is_attached());
        //CHECK_EQUAL(20, mixed_0->get_int(0,0));
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(24, mixed_4->get_int(0,0));
    }
}


TEST(LangBindHelper_ImplicitTransactions_OverSharedGroupDestruction)
{
    SHARED_GROUP_TEST_PATH(path);
    // we hold on to write log collector and registry across a complete
    // shutdown/initialization of shared group.
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
    {
        SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
        {
            WriteTransaction wt(sg);
            TableRef tr = wt.add_table("table");
            tr->add_column(type_Int, "first");
            for (int i=0; i<20; i++)
                tr->add_empty_row();
            wt.commit();
        }
        // no valid shared group anymore
    }
    {
        UniquePtr<Replication> repl(makeWriteLogCollector(path));
        UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
        SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
        {
            WriteTransaction wt(sg);
            TableRef tr = wt.get_table("table");
            for (int i=0; i<20; i++)
                tr->add_empty_row();
            wt.commit();
        }
    }
}


TEST(LangBindHelper_ImplicitTransactions_LinkList)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());
    LangBindHelper::promote_to_write(sg, *tlr);
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    origin->add_empty_row();
    target->add_empty_row();
    LinkViewRef link_list = origin->get_linklist(0,0);
    link_list->add(0);
    LangBindHelper::commit_and_continue_as_read(sg);
    group->Verify();
}


TEST(LangBindHelper_ImplicitTransactions_StringIndex)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group* group = const_cast<Group*>(&sg.begin_read());
    LangBindHelper::promote_to_write(sg, *tlr);
    TableRef table = group->add_table("a");
    table->add_column(type_String, "b");
    table->add_search_index(0);
    group->Verify();
    LangBindHelper::commit_and_continue_as_read(sg);
    group->Verify();
}


namespace {

void multiple_trackers_writer_thread(string path)
{
    Random random(random_int<unsigned long>());
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    for (int i = 0; i < 10; ++i) {
        WriteTransaction wt(sg);
        TestTableInts::Ref tr = wt.get_table<TestTableInts>("table");

        size_t idx = tr->is_empty() ? 0 : random.draw_int_mod(tr->size());

        if (tr[idx].first == 42) {
            // do nothing
        }
        else {
            tr->insert(idx, 0);
        }
        wt.commit();
        sched_yield();
    }
}

void multiple_trackers_reader_thread(TestResults* test_results_ptr, string path)
{
    TestResults& test_results = *test_results_ptr;
    Random random(random_int<unsigned long>());

    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> wlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group& g = const_cast<Group&>(sg.begin_read());
    TableRef tr = g.get_table("table");
    Query q = tr->where().equal(0, 42);
    size_t row_ndx = q.find();
    Row row = tr->get(row_ndx);
    TableView tv = q.find_all();
    for (;;) {
        int_fast64_t val = row.get_int(0);
        tv.sync_if_needed();
        if (val == 43)
            break;
        CHECK_EQUAL(42, val);
        CHECK_EQUAL(1,tv.size());
        CHECK_EQUAL(42, tv.get_int(0,0));
        while (!sg.has_changed())
            sched_yield();
        LangBindHelper::advance_read(sg, *wlr);
    }
    CHECK_EQUAL(0, tv.size());
    sg.end_read();
}

} // anonymous namespace


TEST(LangBindHelper_ImplicitTransactions_MultipleTrackers)
{
    const int write_thread_count = 3;
    const int read_thread_count = 3;

    SHARED_GROUP_TEST_PATH(path);

    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    {
        WriteTransaction wt(sg);
        TableRef tr = wt.add_table("table");
        tr->add_column(type_Int, "first");
        for (int i = 0; i < 200; ++i)
            tr->add_empty_row();
        tr->set_int(0, 100, 42);
        wt.commit();
    }
    Thread threads[write_thread_count + read_thread_count];
    for (int i = 0; i < write_thread_count; ++i)
        threads[i].start(bind(multiple_trackers_writer_thread, string(path)));
    sched_yield();
    for (int i = 0; i < read_thread_count; ++i) {
        threads[write_thread_count + i].start(bind(multiple_trackers_reader_thread,
                                                   &test_results, string(path)));
    }

    // Wait for all writer threads to complete
    for (int i = 0; i < write_thread_count; ++i)
        threads[i].join();

    // signal to all readers to complete
    {
        WriteTransaction wt(sg);
        TableRef tr = wt.get_table("table");
        Query q = tr->where().equal(0, 42);
        size_t idx = q.find();
        tr->set_int(0, idx, 43);
        wt.commit();
    }
    // Wait for all reader threads to complete
    for (int i = 0; i < read_thread_count; ++i)
        threads[write_thread_count + i].join();

    // cleanup
    sg.end_read();
}


TEST(LangBindHelper_ImplicitTransactions_NoExtremeFileSpaceLeaks)
{
    SHARED_GROUP_TEST_PATH(path);

    for (int i = 0; i < 100; ++i) {
        UniquePtr<Replication> repl(makeWriteLogCollector(path));
        UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
        SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
        sg.begin_read();
        LangBindHelper::promote_to_write(sg, *tlr);
        LangBindHelper::commit_and_continue_as_read(sg);
        sg.end_read();
    }

    CHECK_LESS_EQUAL(File(path).get_size(), 8*1024);
}


TEST(LangBindHelper_ImplicitTransactions_DetachRowAccessorOnMoveLastOver)
{
    SHARED_GROUP_TEST_PATH(path);

    Row rows[10];

    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    Group& group = const_cast<Group&>(sg.begin_read());

    LangBindHelper::promote_to_write(sg, *tlr);
    TableRef table = group.add_table("table");
    table->add_column(type_Int, "");
    table->add_empty_row(10);
    for (int i = 0; i < 10; ++i)
        table->set_int(0, i, i);
    LangBindHelper::commit_and_continue_as_read(sg);

    for (int i = 0; i < 10; ++i)
        rows[i] = table->get(i);

    Random random(random_int<unsigned long>());

    LangBindHelper::promote_to_write(sg, *tlr);
    for (int i = 0; i < 10; ++i) {
        size_t row_ndx = random.draw_int_mod(table->size());
        int_fast64_t value = table->get_int(0, row_ndx);
        table->move_last_over(row_ndx);
        CHECK_EQUAL(tightdb::not_found, table->find_first_int(0, value));
        for (int j = 0; j < 10; ++j) {
            bool should_be_attached = table->find_first_int(0, j) != tightdb::not_found;
            CHECK_EQUAL(should_be_attached, rows[j].is_attached());
        }
    }
    LangBindHelper::commit_and_continue_as_read(sg);

    sg.end_read();
}


TEST(LangBindHelper_ImplicitTransactions_ContinuedUseOfTable)
{
    SHARED_GROUP_TEST_PATH(path);

    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    const Group& group = sg.begin_read();

    UniquePtr<Replication> repl_w(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr_w(getWriteLogs(path));
    SharedGroup sg_w(*repl_w, SharedGroup::durability_Full, test_util::key);
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w, *tlr_w);
    TableRef table_w = group_w.add_table("table");
    table_w->add_column(type_Int, "");
    table_w->add_empty_row();
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.Verify();

    LangBindHelper::advance_read(sg, *tlr);
    ConstTableRef table = group.get_table("table");
    CHECK_EQUAL(0, table->get_int(0,0));
    group.Verify();

    LangBindHelper::promote_to_write(sg_w, *tlr_w);
    table_w->set_int(0,0,1);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.Verify();

    LangBindHelper::advance_read(sg, *tlr);
    CHECK_EQUAL(1, table->get_int(0,0));
    group.Verify();

    sg.end_read();
    sg_w.end_read();
}


TEST(LangBindHelper_ImplicitTransactions_ContinuedUseOfDescriptor)
{
    SHARED_GROUP_TEST_PATH(path);

    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    const Group& group = sg.begin_read();

    UniquePtr<Replication> repl_w(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr_w(getWriteLogs(path));
    SharedGroup sg_w(*repl_w, SharedGroup::durability_Full, test_util::key);
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w, *tlr_w);
    TableRef table_w = group_w.add_table("table");
    DescriptorRef desc_w = table_w->get_descriptor();
    desc_w->add_column(type_Int, "1");
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.Verify();

    LangBindHelper::advance_read(sg, *tlr);
    ConstTableRef table = group.get_table("table");
    CHECK_EQUAL(1, table->get_column_count());
    group.Verify();

    LangBindHelper::promote_to_write(sg_w, *tlr_w);
    desc_w->add_column(type_Int, "2");
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.Verify();

    LangBindHelper::advance_read(sg, *tlr);
    CHECK_EQUAL(2, table->get_column_count());
    group.Verify();

    sg.end_read();
    sg_w.end_read();
}


TEST(LangBindHelper_ImplicitTransactions_ContinuedUseOfLinkList)
{
    SHARED_GROUP_TEST_PATH(path);

    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    SharedGroup sg(*repl, SharedGroup::durability_Full, test_util::key);
    const Group& group = sg.begin_read();

    UniquePtr<Replication> repl_w(makeWriteLogCollector(path));
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr_w(getWriteLogs(path));
    SharedGroup sg_w(*repl_w, SharedGroup::durability_Full, test_util::key);
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w, *tlr_w);
    TableRef table_w = group_w.add_table("table");
    table_w->add_column_link(type_LinkList, "", *table_w);
    table_w->add_empty_row();
    LinkViewRef link_list_w = table_w->get_linklist(0,0);
    link_list_w->add(0);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.Verify();

    LangBindHelper::advance_read(sg, *tlr);
    ConstTableRef table = group.get_table("table");
    ConstLinkViewRef link_list = table->get_linklist(0,0);
    CHECK_EQUAL(1, link_list->size());
    group.Verify();

    LangBindHelper::promote_to_write(sg_w, *tlr_w);
    link_list_w->add(0);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.Verify();

    LangBindHelper::advance_read(sg, *tlr);
    CHECK_EQUAL(2, link_list->size());
    group.Verify();

    sg.end_read();
    sg_w.end_read();
}

TEST(LangBindHelper_MemOnly)
{
    SHARED_GROUP_TEST_PATH(path);

    // Verify that the db is empty after populating and then re-opening a file
    {
        ShortCircuitTransactLogManager tlm(path);
        SharedGroup sg(tlm, SharedGroup::durability_MemOnly);
        WriteTransaction wt(sg);
        wt.add_table("table");
        wt.commit();
    }
    {
        ShortCircuitTransactLogManager tlm(path);
        SharedGroup sg(tlm, SharedGroup::durability_MemOnly);
        ReadTransaction rt(sg);
        CHECK(rt.get_group().is_empty());
    }

    // Verify that basic replication functionality works

    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_r(tlm, SharedGroup::durability_MemOnly);
    SharedGroup sg_w(tlm, SharedGroup::durability_MemOnly);
    ReadTransaction rt(sg_r);

    {
        WriteTransaction wt(sg_w);
        wt.add_table("table");
        wt.commit();
    }

    CHECK(rt.get_group().is_empty());
    LangBindHelper::advance_read(sg_r, tlm);
    CHECK(!rt.get_group().is_empty());
}

#endif // TIGHTDB_ENABLE_REPLICATION

#endif
