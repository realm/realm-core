#include <map>
#include <sstream>

#include "testsettings.hpp"
#ifdef TEST_LANG_BIND_HELPER

#include <tightdb/lang_bind_helper.hpp>
#include <tightdb/descriptor.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;


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


#ifdef TIGHTDB_ENABLE_REPLICATION

namespace {

class ShortCircuitTransactLogManager:
        public TrivialReplication,
        public LangBindHelper::TransactLogRegistry {
public:
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

    void handle_transact_log(const char* data, size_t size, version_type new_version)
        TIGHTDB_OVERRIDE
    {
        UniquePtr<char[]> log(new char[size]); // Throws
        copy(data, data+size, log.get());
        m_transact_logs[new_version] = BinaryData(log.get(), size); // Throws
        log.release();
    }

    void get(uint_fast64_t from_version, uint_fast64_t to_version, BinaryData* logs_buffer)
        TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        size_t n = to_version - from_version;
        for (size_t i = 0; i != n; ++i) {
            uint_fast64_t version = from_version + i + 1;
            logs_buffer[i] = m_transact_logs[version];
        }
    }

    void release(uint_fast64_t, uint_fast64_t) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
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
    SharedGroup sg(path);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Try to advance without anything having happened
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Try to advance after an empty write transaction
    {
        WriteTransaction wt(sg_w);
        wt.commit();
    }
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Try to advance after a superfluous rollback
    {
        WriteTransaction wt(sg_w);
        // Implicit rollback
    }
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Try to advance after a propper rollback
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("bad");
        // Implicit rollback
    }
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, group.size());

    // Create a table via the other SharedGroup
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("foo");
        foo_w->add_column(type_Int, "i");
        foo_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
        TableRef bar_w = wt.get_table("bar");
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

    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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


TEST(LangBindHelper_AdvanceReadTransact_ColumnRootTypeChange)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create a table string and one for other types
    {
        WriteTransaction wt(sg_w);
        TableRef strings_w = wt.get_table("strings");
        strings_w->add_column(type_String, "a");
        strings_w->add_column(type_Binary, "b");
        strings_w->add_column(type_Mixed,  "c"); // Strings
        strings_w->add_column(type_Mixed,  "d"); // Binary data
        strings_w->add_empty_row();
        TableRef other_w = wt.get_table("other");
        other_w->add_column(type_Int,   "A");
        other_w->add_column(type_Float, "B");
        other_w->add_column(type_Table, "C");
        other_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read_transact(sg, tlm);
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

    size_t leaf_x4    = 4 * TIGHTDB_MAX_LIST_SIZE;
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
        out << setfill('x') << setw(step.m_str_size) << "A";
        string str_1 = out.str();
        StringData str(str_1);
        out.str("");
        out << setfill('x') << setw(step.m_str_size) << "B";
        string str_2 = out.str();
        BinaryData bin(str_2);
        out.str("");
        out << setfill('x') << setw(step.m_str_size) << "C";
        string str_3 = out.str();
        StringData str_mix(str_3);
        out.str("");
        out << setfill('x') << setw(step.m_str_size) << "D";
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
        LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    SharedGroup sg(path);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create one degenerate subtable
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        DescriptorRef subdesc;
        parent_w->add_column(type_Table, "a", &subdesc);
        subdesc->add_column(type_Int, "x");
        parent_w->add_empty_row();
        wt.commit();
    }
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_MixedSubtables)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create one degenerate subtable
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_column(type_Mixed, "a");
        parent_w->add_empty_row();
        parent_w->set_mixed(0, 0, Mixed::subtable_tag());
        TableRef subtab_0_0_w = parent_w->get_subtable(0,0);
        subtab_0_0_w->add_column(type_Int, "x");
        wt.commit();
    }
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    SharedGroup sg(path);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create a table with two rows
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->add_column(type_Int, "a");
        parent_w->add_empty_row(2);
        parent_w->set_int(0, 0, 27);
        parent_w->set_int(0, 1, 227);
        wt.commit();
    }
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    ConstTableRef parent = rt.get_table("parent");
    CHECK_EQUAL(2, parent->size());
    ConstRow row_1 = (*parent)[0];
    ConstRow row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(9, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(5, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    row_1 = (*parent)[0];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(1, parent->size());
    CHECK(row_1.is_attached());
    CHECK(!row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(2, parent->size());
    row_1 = (*parent)[0];
    row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), &(row_1.get_table()));
    CHECK_EQUAL(parent.get(), &(row_2.get_table()));
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());

    // Check that clearing of the table detaches all row accessors
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
        parent_w->clear();
        wt.commit();
    }
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK_EQUAL(0, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(!row_2.is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_SubtableRowAccessors)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path);
    ShortCircuitTransactLogManager tlm(path);
    SharedGroup sg_w(tlm);

    // Start a read transaction (to be repeatedly advanced)
    ReadTransaction rt(sg);
    const Group& group = rt.get_group();
    CHECK_EQUAL(0, group.size());

    // Create a mixed and a regular subtable each with one row
    {
        WriteTransaction wt(sg_w);
        TableRef parent_w = wt.get_table("parent");
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
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
    LangBindHelper::advance_read_transact(sg, tlm);
    group.Verify();
    CHECK(mixed->is_attached());
    CHECK(regular->is_attached());
    CHECK(row_m.is_attached());
    CHECK(!row_r.is_attached());
}


TEST(LangBindHelper_AdvanceReadTransact_MoveLastOver)
{
    // FIXME: Check that both subtable and row accessors are detached on target row.
    // FIXME: Check that both subtable and row accessors are retained even when they are moved over.
}

#endif // TIGHTDB_ENABLE_REPLICATION

#endif
