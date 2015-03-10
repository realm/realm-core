#include "testsettings.hpp"

#include <algorithm>

#include <tightdb.hpp>
#include <tightdb/commit_log.hpp>
#include <tightdb/util/features.h>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/file.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

#include "test.hpp"

#include <unistd.h> // usleep
#include <tightdb/impl/merge_index_map.hpp>

using namespace tightdb;
using namespace tightdb::_impl;
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

void sync_commits(SharedGroup& from_group, SharedGroup& to_group)
{
    typedef SharedGroup::version_type version_type;

    Replication* from_r = from_group.get_replication();
    Replication* to_r = to_group.get_replication();

    // Figure out which versions to sync:
    version_type v0 = to_r->get_last_peer_version(1);
    if (v0 == 0)
        v0 = 1;
    version_type v1 = from_group.get_current_version();
    uint_fast64_t self_peer_id = &from_group > &to_group ? 1 : 2;
    uint_fast64_t peer_id      = &from_group > &to_group ? 2 : 1;
    //std::cout << "\nSYNC: " << &from_group << " -> " << &to_group << " (v0 = " << v0 << ", v1 = " << v1 << ")\n";
    if (v1 <= v0)
        return; // Already in sync

    // Get the relevant commits:
    std::vector<Replication::CommitLogEntry> entries(v1 - v0);
    from_r->get_commit_entries(v0, v1, &*entries.begin());
    TIGHTDB_ASSERT(from_group.get_current_version() == v0 + entries.size());

    // Send all local commits to the remote end:
    for (version_type i = 0; i < entries.size(); ++i) {
        if (entries[i].peer_id != 0)
            continue;
        version_type commit_version = v0 + i + 1;
        to_r->apply_foreign_changeset(to_group,
            self_peer_id,
            entries[i].peer_version,
            entries[i].log_data,
            entries[i].timestamp,
            peer_id,
            commit_version);
    }
}

const char g_table_name[] = "t0";

void create_table(SharedGroup& group)
{
    WriteTransaction tr(group);
    TableRef t = tr.add_table(g_table_name);
    t->add_column(type_Int, "c0");
    tr.commit();
}

void insert(SharedGroup& group, size_t row_ndx, int64_t value)
{
    WriteTransaction tr(group);
    TableRef t = tr.get_table(g_table_name);
    t->insert_empty_row(row_ndx);
    t->set_int(0, row_ndx, value);
    tr.commit();
}

void set(SharedGroup& group, size_t row_ndx, int64_t value)
{
    WriteTransaction tr(group);
    TableRef t = tr.get_table(g_table_name);
    t->set_int(0, row_ndx, value);
    tr.commit();
}

int64_t get(SharedGroup& group, size_t row_ndx)
{
    ReadTransaction tr(group);
    ConstTableRef t = tr.get_table(g_table_name);
    return t->get_int(0, row_ndx);
}

void bump_timestamp()
{
    usleep(1);
}

TIGHTDB_UNUSED void dump_values(SharedGroup& group)
{
    ReadTransaction tr(group);
    ConstTableRef t = tr.get_table(g_table_name);
    std::cout << "[";
    for (size_t i = 0; i < t->size();) {
        std::cout << t->get_int(0, i);
        ++i;
        if (i != t->size()) {
            std::cout << ", ";
        }
    }
    std::cout << "]\n";
}

void check_equality(TestResults& test_results, SharedGroup& a, SharedGroup& b)
{
    ReadTransaction tr_a(a);
    ReadTransaction tr_b(b);
    ConstTableRef ta = tr_a.get_table(g_table_name);
    ConstTableRef tb = tr_b.get_table(g_table_name);
    CHECK_EQUAL(ta->size(), tb->size());
    size_t len = ta->size();
    for (size_t i = 0; i < len; ++i) {
        CHECK_EQUAL(ta->get_int(0, i), tb->get_int(0, i));
    }
}

} // anonymous namespace


TEST(Sync_MergeWrites)
{
    SHARED_GROUP_TEST_PATH(logfile1);
    SHARED_GROUP_TEST_PATH(logfile2);

    Replication* ra = makeWriteLogCollector(logfile1, true);
    Replication* rb = makeWriteLogCollector(logfile2, true);

    SharedGroup a(*ra);
    SharedGroup b(*rb);

    // First, create some entries in ra.
    create_table(a);
    insert(a, 0, 123);
    sync_commits(a, b);

    // Check that we have the same basic structure.
    CHECK_EQUAL(123, get(b, 0));

    // Insert some things on b
    insert(b, 0, 456);
    sync_commits(b, a);

    // Check that a received the updates from b
    check_equality(test_results, a, b);

    // NOW LET'S GENERATE SOME CONFLICTS!
    insert(a, 0, 999);
    bump_timestamp();
    insert(b, 0, 333);
    sync_commits(a, b);
    sync_commits(b, a);

    CHECK_EQUAL(333, get(a, 0));
    CHECK_EQUAL(999, get(a, 1));
    CHECK_EQUAL(333, get(b, 0)); // fails here if merge doesn't work
    CHECK_EQUAL(999, get(b, 1));
    check_equality(test_results, a, b);

    insert(a, 0, 999);
    bump_timestamp();
    insert(b, 0, 333);
    sync_commits(b, a);
    sync_commits(a, b);
    CHECK_EQUAL(333, get(a, 0));
    CHECK_EQUAL(999, get(a, 1));
    CHECK_EQUAL(333, get(b, 0));
    CHECK_EQUAL(999, get(b, 1));
    check_equality(test_results, a, b);

    // Now let's try the same, but with commits arriving out of order:
    insert(a, 0, 888);
    bump_timestamp();
    insert(b, 0, 444);
    sync_commits(a, b);
    sync_commits(b, a);
    CHECK_EQUAL(444, get(a, 0)); // fails here if merge doesn't work
    CHECK_EQUAL(444, get(b, 0));
    check_equality(test_results, a, b);

    /// PENDING SET SUPPORT!

    // Conflicting set operations:
    // set(a, 0, 999);
    // bump_timestamp();
    // set(b, 0, 1001);
    // sync_commits(a, b);
    // sync_commits(b, a);
    // CHECK_EQUAL(999, get(a, 0));
    // CHECK_EQUAL(999, get(b, 0));
    // check_equality(test_results, a, b);

    // Conflicting set operations out of order:
    // set(b, 0, 1002);
    // bump_timestamp();
    // set(a, 0, 1111);
    // sync_commits(a, b);
    // sync_commits(b, a);
    // CHECK_EQUAL(1111, get(a, 0));
    // CHECK_EQUAL(1111, get(b, 0));
    // check_equality(test_results, a, b);

    // Insert at different indices:
    insert(a, 0, 12221);
    insert(b, 5, 21112);
    sync_commits(a, b);
    CHECK_EQUAL(12221, get(b, 0));
    sync_commits(b, a);
    CHECK_EQUAL(21112, get(a, 6));
    check_equality(test_results, a, b);

    // Insert at different indices, out of order:
    insert(a, 0, 12221);
    insert(b, 5, 21112);
    sync_commits(b, a);
    CHECK_EQUAL(21112, get(a, 6));
    sync_commits(a, b);
    CHECK_EQUAL(12221, get(b, 0));
    check_equality(test_results, a, b);

    // Insert-then-set at different indices, mixed order:
    insert(a, 0, 23332);
    insert(b, 1, 34443);
    set(a, 0, 45554);
    set(b, 1, 56665);
    sync_commits(a, b);
    CHECK_EQUAL(45554, get(b, 0));
    CHECK_EQUAL(56665, get(b, 2));
    sync_commits(b, a);
    CHECK_EQUAL(45554, get(a, 0));
    CHECK_EQUAL(56665, get(a, 2));
    check_equality(test_results, a, b);

    // Many set, different times:
    // set(a, 4, 123);
    // set(a, 4, 234);
    // set(b, 4, 345);
    // set(a, 4, 456);
    // set(a, 4, 567);
    // sync_commits(a, b);
    // sync_commits(b, a);
    // CHECK_EQUAL(567, get(a, 4));
    // CHECK_EQUAL(567, get(b, 4));
    // check_equality(test_results, a, b);

    // Many set, different times, other order:
    // set(a, 4, 123);
    // set(a, 4, 234);
    // set(b, 4, 345);
    // set(a, 4, 456);
    // set(a, 4, 567);
    // sync_commits(b, a);
    // sync_commits(a, b);
    // CHECK_EQUAL(567, get(a, 4));
    // CHECK_EQUAL(567, get(b, 4));
    // check_equality(test_results, a, b);

    // Insert on both ends
    insert(a, 1, 0xaa);
    insert(b, 0, 0xcc);
    insert(b, 1, 0xdd);
    sync_commits(b, a);
    sync_commits(a, b);
    check_equality(test_results, a, b);
}

TEST(Sync_MergeIndexMap)
{
    uint64_t self_id = 0;
    uint64_t peer_id = 1;
    MergeIndexMap map(0);

    CHECK_EQUAL(0, map.transform_insert(0, 1, 0, peer_id));

    map.clear();
    map.unknown_insertion_at(0, 1, 0, self_id);
    map.unknown_insertion_at(0, 1, 1, self_id);
    size_t i0 = map.transform_insert(0, 1, 2, peer_id);
    CHECK_EQUAL(2, i0);

    map.clear();
    map.known_insertion_at(1, 1);
    //map.debug_print();
    size_t i1 = map.transform_insert(3, 1, 3, peer_id);
    CHECK_EQUAL(3, i1);

    map.clear();
    map.unknown_insertion_at(0, 1, 0, self_id);
    map.known_insertion_at(0, 1);
    map.unknown_insertion_at(1, 1, 1, self_id);
    map.known_insertion_at(1, 1);
    size_t i2 = map.transform_insert(2, 1, 2, peer_id);
    CHECK_EQUAL(4, i2);
}
