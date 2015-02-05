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

static void
sync_commits(SharedGroup& from_group, SharedGroup& to_group)
{
    
    typedef SharedGroup::version_type version_type;

    Replication* from_r = from_group.get_replication();
    Replication* to_r = to_group.get_replication();

    // Figure out which versions to sync:
    version_type v0 = to_r->get_last_peer_version(1);
    version_type v1 = from_group.get_current_version();
    uint_fast64_t peer_id = 1;
    std::cout << "\nSYNC: " << &from_group << " -> " << &to_group << " (v0 = " << v0 << ", v1 = " << v1 << ")\n";
    if (v1 <= v0)
        return; // Already in sync

    // Get the relevant commits:
    std::vector<Replication::CommitLogEntry> entries(v1 - v0);
    from_r->get_commit_entries(v0, v1, entries.data());
    TIGHTDB_ASSERT(from_group.get_current_version() == v0 + entries.size());

    // Send all local commits to the remote end:
    for (version_type i = 0; i < entries.size(); ++i) {
        if (entries[i].peer_id != 0)
            continue;
        version_type commit_version = v0 + i + 1;
        to_r->apply_foreign_changeset(to_group,
            entries[i].peer_version,
            entries[i].log_data,
            entries[i].timestamp,
            peer_id,
            commit_version);
    }
}

void bump_timestamp()
{
    usleep(1);
}

void dump_ints(ConstTableRef tr)
{
    std::cout << "[";
    for (size_t i = 0; i < tr->size();) {
        std::cout << tr->get_int(0, i);
        ++i;
        if (i != tr->size()) {
            std::cout << ", ";
        }
    }
    std::cout << "]\n";
}


ONLY(Sync_MergeWrites)
{
    typedef SharedGroup::version_type version_type;

    SHARED_GROUP_TEST_PATH(logfile1);
    SHARED_GROUP_TEST_PATH(logfile2);

    Replication* ra = makeWriteLogCollector(logfile1, true);
    Replication* rb = makeWriteLogCollector(logfile2, true);

    SharedGroup a(*ra);
    SharedGroup b(*rb);

    // First, create some entries in ra.
    {
        WriteTransaction tr(a);
        TableRef t0 = tr.add_table("t0");
        t0->add_column(type_Int, "c0");
        t0->add_empty_row();
        t0->set_int(0, 0, 123);
        tr.commit();
    }

    sync_commits(a, b);

    // Check that we have the same basic structure.
    {
        ReadTransaction tr(b);
        ConstTableRef t0 = tr.get_table("t0");
        CHECK(t0);
        CHECK_EQUAL(t0->get_int(0, 0), 123);
    }

    // Insert some things on b
    {
        WriteTransaction tr(b);
        TableRef t0 = tr.get_table("t0");
        t0->insert_empty_row(0);
        t0->set_int(0, 0, 456);
        tr.commit();
    }

    sync_commits(b, a);

    // Check that a received the updates from b
    {
        ReadTransaction tr(a);
        ConstTableRef t0 = tr.get_table("t0");
        CHECK_EQUAL(456, t0->get_int(0, 0));
    }

    // NOW LET'S GENERATE SOME CONFLICTS!
    std::cout << "\n=> CONFLICT 1:";
    {
        WriteTransaction tr(a);
        TableRef t0 = tr.get_table("t0");
        t0->insert_empty_row(0);
        t0->set_int(0, 0, 999);
        tr.commit();
    }
    bump_timestamp();
    {
        WriteTransaction tr(b);
        TableRef t0 = tr.get_table("t0");
        t0->insert_empty_row(0);
        t0->set_int(0, 0, 333);
        tr.commit();
    }

    sync_commits(a, b);
    sync_commits(b, a);

    // Because a's commits "came first", we expect row 0 in a to have 999 from a.
    {
        ReadTransaction tr(a);
        ConstTableRef t0 = tr.get_table("t0");
        dump_ints(t0);
        CHECK_EQUAL(999, t0->get_int(0, 0));
        CHECK_EQUAL(333, t0->get_int(0, 1));

        ReadTransaction tr2(b);
        ConstTableRef t1 = tr2.get_table("t0");
        dump_ints(t1);
        CHECK_EQUAL(999, t1->get_int(0, 0)); // fails here if merge doesn't work
        CHECK_EQUAL(333, t1->get_int(0, 1));
    }

    std::cout << "\n=> CONFLICT 2:";
    {
        WriteTransaction tr(a);
        TableRef t0 = tr.get_table("t0");
        t0->insert_empty_row(0);
        t0->set_int(0, 0, 999);
        tr.commit();
    }
    bump_timestamp();
    {
        WriteTransaction tr(b);
        TableRef t0 = tr.get_table("t0");
        t0->insert_empty_row(0);
        t0->set_int(0, 0, 333);
        tr.commit();
    }

    sync_commits(b, a);
    sync_commits(a, b);

    // Because a's commits "came first", we expect row 0 in a to have 999 from a.
    {
        ReadTransaction tr(a);
        ConstTableRef t0 = tr.get_table("t0");
        dump_ints(t0);
        CHECK_EQUAL(999, t0->get_int(0, 0));
        CHECK_EQUAL(333, t0->get_int(0, 1));

        ReadTransaction tr2(b);
        ConstTableRef t1 = tr2.get_table("t0");
        dump_ints(t1);
        CHECK_EQUAL(999, t1->get_int(0, 0)); // fails here if merge doesn't work
        CHECK_EQUAL(333, t1->get_int(0, 1));
    }

    // Now let's try the same, but with commits arriving out of order:

    {
        WriteTransaction tr(a);
        TableRef t0 = tr.get_table("t0");
        t0->insert_empty_row(0);
        t0->set_int(0, 0, 888);
        tr.commit();
    }
    bump_timestamp();
    {
        WriteTransaction tr(b);
        TableRef t0 = tr.get_table("t0");
        t0->insert_empty_row(0);
        t0->set_int(0, 0, 444);
        tr.commit();
    }
    

    sync_commits(a, b);
    sync_commits(b, a);

    // Because b's commits "came before" a's, we expect row 0 to have 888 from a.
    {
        ReadTransaction tr(a);
        ConstTableRef t0 = tr.get_table("t0");
        CHECK_EQUAL(t0->get_int(0, 0), 888); // fails here if merge doesn't work

        ReadTransaction tr2(b);
        ConstTableRef t1 = tr2.get_table("t0");
        CHECK_EQUAL(t1->get_int(0, 0), 888);
    }

    // Conflicting set operations:
    {
        WriteTransaction tr(a);
        TableRef t0 = tr.get_table("t0");
        t0->set_int(0, 0, 999);
        tr.commit();
    }
    bump_timestamp();
    {
        WriteTransaction tr(b);
        TableRef t0 = tr.get_table("t0");
        t0->set_int(0, 0, 1001);
        tr.commit();
    }

    sync_commits(a, b);
    sync_commits(b, a);

    {
        ReadTransaction tr(a);
        ConstTableRef t0 = tr.get_table("t0");
        CHECK_EQUAL(t0->get_int(0, 0), 1001);

        ReadTransaction tr2(b);
        ConstTableRef t1 = tr2.get_table("t0");
        CHECK_EQUAL(t1->get_int(0, 0), 1001);
    }

    // Conflicting set operations out of order:
    {
        WriteTransaction tr(b);
        TableRef t0 = tr.get_table("t0");
        t0->set_int(0, 0, 1001);
        tr.commit();
    }
    bump_timestamp();
    {
        WriteTransaction tr(a);
        TableRef t0 = tr.get_table("t0");
        t0->set_int(0, 0, 999);
        tr.commit();
    }

    sync_commits(a, b);
    sync_commits(b, a);

    {
        ReadTransaction tr(a);
        ConstTableRef t0 = tr.get_table("t0");
        CHECK_EQUAL(t0->get_int(0, 0), 999);

        ReadTransaction tr2(b);
        ConstTableRef t1 = tr2.get_table("t0");
        CHECK_EQUAL(t1->get_int(0, 0), 999);
    }
}
