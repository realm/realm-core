#include "testsettings.hpp"
#ifdef TEST_SHARED

#include <streambuf>
#include <fstream>

// Need fork() and waitpid() for Shared_RobustAgainstDeathDuringWrite
#ifndef _WIN32
#  include <unistd.h>
#  include <sys/wait.h>
#  include <signal.h>
#  define ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE
#else
#  define NOMINMAX
#  include <windows.h>
#endif

#include <tightdb.hpp>
#include <tightdb/util/features.h>
#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/bind.hpp>
#include <tightdb/util/terminate.hpp>
#include <tightdb/util/file.hpp>
#include <tightdb/util/thread.hpp>
#include "util/thread_wrapper.hpp"

#include "test.hpp"
#include "crypt_key.hpp"

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


TEST(Shared_Unattached)
{
    SharedGroup sg((SharedGroup::unattached_tag()));
}


namespace {

// async deamon does not start when launching unit tests from osx, so async is currently disabled on osx.
// Also: async requires interprocess communication, which does not work with our current encryption support.
#if !defined(_WIN32) && !defined(__APPLE__)
#  if TIGHTDB_ANDROID || defined DISABLE_ASYNC || defined TIGHTDB_ENABLE_ENCRYPTION
bool allow_async = false;
#  else
bool allow_async = true;
#  endif
#endif


TIGHTDB_TABLE_4(TestTableShared,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, String)



void writer(string path, int id, int limit)
{
    // cerr << "Started pid " << getpid() << endl;
    try {
        SharedGroup sg(path, true, SharedGroup::durability_Full);
        // cerr << "Opened sg, pid " << getpid() << endl;
        for (int i=0; limit==0 || i < limit; ++i) {
            // cerr << "       - " << getpid() << endl;
            WriteTransaction wt(sg);
            if (i & 1) {
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1[id].first = 1 + t1[id].first;
            }
            sched_yield(); // increase chance of signal arriving in the middle of a transaction
            wt.commit();
        }
        // cerr << "Ended pid " << getpid() << endl;
    } catch (...) {
        cerr << "Exception from " << getpid() << endl;
    }
}


#if !defined(__APPLE__) && !defined(_WIN32) && !defined TIGHTDB_ENABLE_ENCRYPTION

void killer(TestResults& test_results, int pid, string path, int id)
{
    {
        SharedGroup sg(path, true, SharedGroup::durability_Full);
        bool done = false;
        do {
            sched_yield();
            // pseudo randomized wait (to prevent unwanted synchronization effects of yield):
            int n = random() % 10000;
            volatile int thing = 0;
            while (n--) thing += random();
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            done = 10 < t1[id].first;
        } while (!done);
    }
    kill(pid, 9);
    int stat_loc = 0;
    int options = 0;
    int ret_pid = waitpid(pid, &stat_loc, options);
    if (ret_pid == pid_t(-1)) {
        if (errno == EINTR)
            cerr << "waitpid was interrupted" << endl;
        if (errno == EINVAL)
            cerr << "waitpid got bad arguments" << endl;
        if (errno == ECHILD)
            cerr << "waitpid tried to wait for the wrong child: " << pid << endl;
        TIGHTDB_TERMINATE("waitpid failed");
    }
    bool child_exited_from_signal = WIFSIGNALED(stat_loc);
    CHECK(child_exited_from_signal);
    int child_exit_status = WEXITSTATUS(stat_loc);
    CHECK_EQUAL(0, child_exit_status);
    {
        // Verify that we surely did kill the process before it could do all it's commits.
        SharedGroup sg(path, true, SharedGroup::durability_Full);
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
        CHECK(10 < t1[id].first);
    }
}
#endif

} // anonymous namespace

#if !defined(__APPLE__) && !defined(_WIN32)&& !defined TIGHTDB_ENABLE_ENCRYPTION

TEST(Shared_PipelinedWritesWithKills)
{
    CHECK(RobustMutex::is_robust_on_this_platform());
    const int num_processes = 50;
    SHARED_GROUP_TEST_PATH(path);
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full);
        // Create table entries
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
        for (int i = 0; i < num_processes; ++i)
        {
            t1->add(0, i, false, "test");
        }
        wt.commit();
    }
    int pid = fork();
    if (pid == -1)
        TIGHTDB_TERMINATE("fork() failed");
    if (pid == 0) {
        // first writer!
        writer(path, 0, 0);
        _exit(0);
    }
    else {
        for (int k=1; k < num_processes; ++k) {
            int pid2 = pid;
            pid = fork();
            if (pid == pid_t(-1))
                TIGHTDB_TERMINATE("fork() failed");
            if (pid == 0) {
                writer(path, k, 0);
                _exit(0);
            }
            else {
                // cerr << "New process " << pid << " killing old " << pid2 << endl;
                killer(test_results, pid2, path, k-1);
            }
        }
        // cerr << "Killing last one: " << pid << endl;
        killer(test_results, pid, path, num_processes-1);
    }
}
#endif

TEST(Shared_CompactingOnTheFly)
{
    SHARED_GROUP_TEST_PATH(path);
    string old_path = path;
    string tmp_path = string(path)+".tmp";
    Thread writer_thread;
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full);
        // Create table entries
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
        for (int i = 0; i < 100; ++i)
        {
            t1->add(0, i, false, "test");
        }
        wt.commit();
        {
            ReadTransaction rt(sg);
            writer_thread.start(bind(&writer, old_path, 42, 100));
            sleep(1);
        }
        // we cannot compact while a writer is still running:
        CHECK_EQUAL(false, sg.compact());
    }
    writer_thread.join();
    {
        SharedGroup sg2(path, true, SharedGroup::durability_Full);
        CHECK_EQUAL(true, sg2.compact());
        ReadTransaction rt2(sg2);
        rt2.get_group().Verify();
    }
}

#ifdef LOCKFILE_CLEANUP
// The following two tests are now disabled, as we have abandoned the requirement to
// clean up the .lock file after use.
TEST(Shared_NoCreateCleanupLockFileAfterFailure)
{
    SHARED_GROUP_TEST_PATH(path);

    bool no_create = true;
    CHECK_THROW(SharedGroup(path, no_create, SharedGroup::durability_Full), File::NotFound);

    CHECK(!File::exists(path));

    // Verify that the `lock` file is not left behind
    CHECK(!File::exists(path.get_lock_path()));
}


// FIXME: The following test seems really weird. The previous test
// checks that no `lock` file is left behind, yet this test seems to
// anticipate a case where it is left behind. What is going on?
TEST(Shared_NoCreateCleanupLockFileAfterFailure2)
{
    SHARED_GROUP_TEST_PATH(path);

    bool no_create = true;
    CHECK_THROW(SharedGroup(path, no_create, SharedGroup::durability_Full), File::NotFound);

    CHECK(!File::exists(path));

    if (!File::exists(path.get_lock_path())) {
        try {
            // Let's see if any leftover `lock` file is correctly removed or reinitialized
            no_create = false;
            SharedGroup sg(path, no_create, SharedGroup::durability_Full);
        }
        catch (runtime_error&) {
            CHECK(false);
        }
    }

    // Verify that the `lock` file is not left behind
    CHECK(!File::exists(path.get_lock_path()));
}
#endif

TEST(Shared_Initial)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

        // Verify that new group is empty
        {
            ReadTransaction rt(sg);
            CHECK(rt.get_group().is_empty());
        }

    }

#ifdef LOCKFILE_CLEANUP
    // Verify that the `lock` file is not left behind
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


#ifdef LOCKFILE_CLEANUP
TEST(Shared_StaleLockFileFaked)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // create fake lock file
        File lock(path.get_lock_path(), File::mode_Write);
        const char buf[] = { 0, 0, 0, 0 };
        lock.write(buf);
    }
    bool no_create = false;
    CHECK_THROW(SharedGroup(path, no_create, SharedGroup::durability_Full),
                SharedGroup::PresumablyStaleLockFile);
    File::try_remove(path.get_lock_path());
}


// FIXME:
// At the moment this test does not work on windows when run as a virtual machine.
TEST(Shared_StaleLockFileRenamed)
{
    SHARED_GROUP_TEST_PATH(path);
    string lock_path   = path.get_lock_path();
    string lock_path_2 = path.get_lock_path() + ".backup";
    File::try_remove(lock_path_2);
    bool no_create = false;
    {
        // create lock file
        SharedGroup sg(path, no_create, SharedGroup::durability_Full, crypt_key());
#ifdef _WIN32
        // Requires ntfs to work
        if (!CreateHardLinkA(lock_path_2.c_str(), lock_path.c_str(), 0)) {
            cerr << "Creating a hard link failed, test abandoned" << endl;
            return;
        }
#else
        if (link(lock_path.c_str(), lock_path_2.c_str())) {
            cerr << "Creating a hard link failed, test abandoned" << endl;
            return;
        }
#endif
    }
    File::move(lock_path_2, lock_path);
    // FIXME: Why is it ok to replace the lock file with a new file?
    // Why must it be ok? Explanation is needed here!
    {
        SharedGroup sg(path, no_create, SharedGroup::durability_Full, crypt_key());
    }

    // Verify that the `lock` file is not left behind
    CHECK(!File::exists(lock_path));
}
#endif

TEST(Shared_InitialMem)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroup::durability_MemOnly);

        // Verify that new group is empty
        {
            ReadTransaction rt(sg);
            CHECK(rt.get_group().is_empty());
        }

    }

    // In MemOnly mode, the database file must be automatically
    // removed.
    CHECK(!File::exists(path));

#ifdef LOCKFILE_CLEANUP
    // Verify that the `lock` file is not left behind
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


TEST(Shared_Initial2)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

        {
            // Open the same db again (in empty state)
            SharedGroup sg2(path, false, SharedGroup::durability_Full, crypt_key());

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                wt.get_group().Verify();
                TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", t1[0].fourth);
        }
    }

#ifdef LOCKFILE_CLEANUP
    // Verify that the `lock` file is not left behind
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


TEST(Shared_Initial2_Mem)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroup::durability_MemOnly);

        {
            // Open the same db again (in empty state)
            SharedGroup sg2(path, no_create, SharedGroup::durability_MemOnly);

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                wt.get_group().Verify();
                TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", t1[0].fourth);
        }
    }

#ifdef LOCKFILE_CLEANUP
    // Verify that the `lock` file is not left behind
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


TEST(Shared_1)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            wt.commit();
        }

        // Open same db again
        SharedGroup sg2(path, false, SharedGroup::durability_Full, crypt_key());
        {
            ReadTransaction rt(sg2);
            rt.get_group().Verify();

            // Verify that last set of changes are commited
            TestTableShared::ConstRef t2 = rt.get_table<TestTableShared>("test");
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);

            // Do a new change while stil having current read transaction open
            {
                WriteTransaction wt(sg);
                wt.get_group().Verify();
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1->add(2, 3, true, "more test");
                wt.commit();
            }

            // Verify that that the read transaction does not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);

            // Do one more new change while stil having current read transaction open
            // so we know that it does not overwrite data held by
            {
                WriteTransaction wt(sg);
                wt.get_group().Verify();
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1->add(0, 1, false, "even more test");
                wt.commit();
            }

            // Verify that that the read transaction does still not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);
        }

        // Start a new read transaction and verify that it can now see the changes
        {
            ReadTransaction rt(sg2);
            rt.get_group().Verify();
            TestTableShared::ConstRef t3 = rt.get_table<TestTableShared>("test");

            CHECK(t3->size() == 3);
            CHECK_EQUAL(1, t3[0].first);
            CHECK_EQUAL(2, t3[0].second);
            CHECK_EQUAL(false, t3[0].third);
            CHECK_EQUAL("test", t3[0].fourth);
            CHECK_EQUAL(2, t3[1].first);
            CHECK_EQUAL(3, t3[1].second);
            CHECK_EQUAL(true, t3[1].third);
            CHECK_EQUAL("more test", t3[1].fourth);
            CHECK_EQUAL(0, t3[2].first);
            CHECK_EQUAL(1, t3[2].second);
            CHECK_EQUAL(false, t3[2].third);
            CHECK_EQUAL("even more test", t3[2].fourth);
        }
    }

#ifdef LOCKFILE_CLEANUP
    // Verify that lock file was deleted after use
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


TEST(Shared_Rollback)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

        // Create first table in group (but rollback)
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            // Note: Implicit rollback
        }

        // Verify that no changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            CHECK(!rt.get_group().has_table("test"));
        }

        // Really create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            CHECK(t->size() == 1);
            CHECK_EQUAL(1, t[0].first);
            CHECK_EQUAL(2, t[0].second);
            CHECK_EQUAL(false, t[0].third);
            CHECK_EQUAL("test", t[0].fourth);
        }

        // Greate more changes (but rollback)
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(0, 0, true, "more test");
            // Note: Implicit rollback
        }

        // Verify that no changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            CHECK(t->size() == 1);
            CHECK_EQUAL(1, t[0].first);
            CHECK_EQUAL(2, t[0].second);
            CHECK_EQUAL(false, t[0].third);
            CHECK_EQUAL("test", t[0].fourth);
        }
    }

#ifdef LOCKFILE_CLEANUP
    // Verify that lock file was deleted after use
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


TEST(Shared_Writes)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(0, 2, false, "test");
            wt.commit();
        }

        // Do a lot of repeated write transactions
        for (size_t i = 0; i < 100; ++i) {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[0].first += 1;
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            const int64_t v = t[0].first;
            CHECK_EQUAL(100, v);
        }
    }

#ifdef LOCKFILE_CLEANUP
    // Verify that lock file was deleted after use
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


TEST(Shared_AddColumnToSubspec)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    // Create table with a non-empty subtable
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        DescriptorRef sub_1;
        table->add_column(type_Table, "subtable", &sub_1);
        sub_1->add_column(type_Int,   "int");
        table->add_empty_row();
        TableRef subtable = table->get_subtable(0,0);
        subtable->add_empty_row();
        subtable->set_int(0, 0, 789);
        wt.commit();
    }

    // Modify subtable spec, then access the subtable. This is to see
    // that the subtable column accessor continues to work after the
    // subspec has been modified.
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        DescriptorRef subdesc = table->get_subdescriptor(0);
        subdesc->add_column(type_Int, "int_2");
        TableRef subtable = table->get_subtable(0,0);
        CHECK_EQUAL(2, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(type_Int, subtable->get_column_type(1));
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0,0));
        subtable->add_empty_row();
        CHECK_EQUAL(2, subtable->size());
        subtable->set_int(1, 1, 654);
        CHECK_EQUAL(654, subtable->get_int(1,1));
        wt.commit();
    }

    // Check that the subtable continues to have the right contents
    {
        ReadTransaction rt(sg);
        ConstTableRef table = rt.get_table("table");
        ConstTableRef subtable = table->get_subtable(0,0);
        CHECK_EQUAL(2, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(type_Int, subtable->get_column_type(1));
        CHECK_EQUAL(2, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0,0));
        CHECK_EQUAL(0,   subtable->get_int(0,1));
        CHECK_EQUAL(0,   subtable->get_int(1,0));
        CHECK_EQUAL(654, subtable->get_int(1,1));
    }
}


TEST(Shared_RemoveColumnBeforeSubtableColumn)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    // Create table with a non-empty subtable in a subtable column
    // that is preceded by another column
    {
        WriteTransaction wt(sg);
        DescriptorRef sub_1;
        TableRef table = wt.add_table("table");
        table->add_column(type_Int,   "int");
        table->add_column(type_Table, "subtable", &sub_1);
        sub_1->add_column(type_Int,   "int");
        table->add_empty_row();
        TableRef subtable = table->get_subtable(1,0);
        subtable->add_empty_row();
        subtable->set_int(0, 0, 789);
        wt.commit();
    }

    // Remove a column that precedes the subtable column
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        table->remove_column(0);
        TableRef subtable = table->get_subtable(0,0);
        CHECK_EQUAL(1, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0,0));
        subtable->add_empty_row();
        CHECK_EQUAL(2, subtable->size());
        subtable->set_int(0, 1, 654);
        CHECK_EQUAL(654, subtable->get_int(0,1));
        wt.commit();
    }

    // Check that the subtable continues to have the right contents
    {
        ReadTransaction rt(sg);
        ConstTableRef table = rt.get_table("table");
        ConstTableRef subtable = table->get_subtable(0,0);
        CHECK_EQUAL(1, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(2, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0,0));
        CHECK_EQUAL(654, subtable->get_int(0,1));
    }
}


TEST(Shared_ManyReaders)
{
    // This test was written primarily to expose a former bug in
    // SharedGroup::end_read(), where the lock-file was not remapped
    // after ring-buffer expansion.

    const int chunk_1_size = 251;
    char chunk_1[chunk_1_size];
    for (int i = 0; i < chunk_1_size; ++i)
        chunk_1[i] = (i + 3) % 251;
    const int chunk_2_size = 123;
    char chunk_2[chunk_2_size];
    for (int i = 0; i < chunk_2_size; ++i)
        chunk_2[i] = (i + 11) % 241;

#if TEST_DURATION < 1
    // Mac OS X 10.8 cannot handle more than 15 due to its default ulimit settings.
    int rounds[] = { 3, 5, 7, 9, 11, 13, 15 };
#else
    int rounds[] = { 3, 5, 11, 17, 23, 27, 31, 47, 59 };
#endif
    const int num_rounds = sizeof rounds / sizeof *rounds;

    const int max_N = 64;
    CHECK(max_N >= rounds[num_rounds-1]);
    UniquePtr<SharedGroup> shared_groups[8 * max_N];
    UniquePtr<ReadTransaction> read_transactions[8 * max_N];

    for (int round = 0; round < num_rounds; ++round) {
        int N = rounds[round];

        SHARED_GROUP_TEST_PATH(path);

        bool no_create = false;
        SharedGroup root_sg(path, no_create, SharedGroup::durability_MemOnly);

        // Add two tables
        {
            WriteTransaction wt(root_sg);
            wt.get_group().Verify();
            TableRef test_1 = wt.get_or_add_table("test_1");
            test_1->add_column(type_Int, "i");
            test_1->insert_int(0,0,0);
            test_1->insert_done();
            TableRef test_2 = wt.get_or_add_table("test_2");
            test_2->add_column(type_Binary, "b");
            wt.commit();
        }


        // Create 8*N shared group accessors
        for (int i = 0; i < 8*N; ++i)
            shared_groups[i].reset(new SharedGroup(path, no_create, SharedGroup::durability_MemOnly));

        // Initiate 2*N read transactions with progressive changes
        for (int i = 0; i < 2*N; ++i) {
            read_transactions[i].reset(new ReadTransaction(*shared_groups[i]));
            read_transactions[i]->get_group().Verify();
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                CHECK_EQUAL(i, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            {
                WriteTransaction wt(root_sg);
                wt.get_group().Verify();
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->insert_binary(0, 0, BinaryData(chunk_1));
                test_2->insert_done();
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
                wt.get_group().Verify();
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->insert_binary(0, test_2->size(), BinaryData(chunk_2));
                    test_2->insert_done();
                }
                wt.commit();
            }
        }

        // Check isolation between read transactions
        for (int i = 0; i < 2*N; ++i) {
            ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(i, test_1->get_int(0,0));
            ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
            int n_1 = i *  1;
            int n_2 = i * 18;
            CHECK_EQUAL(n_1+n_2, test_2->size());
            for (int j = 0; j < n_1; ++j)
                CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
            for (int j = n_1; j < n_1+n_2; ++j)
                CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
        }

        // End the first half of the read transactions during further
        // changes
        for (int i = N-1; i >= 0; --i) {
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0  // These .Verify() calls are horribly slow on Windows
                wt.get_group().Verify();
#endif
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(i, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            read_transactions[i].reset();
        }

        // Initiate 6*N extra read transactionss with further progressive changes
        for (int i = 2*N; i < 8*N; ++i) {
            read_transactions[i].reset(new ReadTransaction(*shared_groups[i]));
#if !defined(_WIN32) || TEST_DURATION > 0 
            read_transactions[i]->get_group().Verify();
#endif
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                int i_2 = 2*N + i;
                CHECK_EQUAL(i_2, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0 
                wt.get_group().Verify();
#endif
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->insert_binary(0, 0, BinaryData(chunk_1));
                test_2->insert_done();
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0 
                wt.get_group().Verify();
#endif
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->insert_binary(0, test_2->size(), BinaryData(chunk_2));
                    test_2->insert_done();
                }
                wt.commit();
            }
        }

        // End all remaining read transactions during further changes
        for (int i = 1*N; i < 8*N; ++i) {
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0 
                wt.get_group().Verify();
#endif
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                int i_2 = i<2*N ? i : 2*N + i;
                CHECK_EQUAL(i_2, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            read_transactions[i].reset();
        }

        // Check final state via each shared group, then destroy it
        for (int i=0; i<8*N; ++i) {
            {
                ReadTransaction rt(*shared_groups[i]);
#if !defined(_WIN32) || TEST_DURATION > 0 
                rt.get_group().Verify();
#endif
                ConstTableRef test_1 = rt.get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(3*8*N, test_1->get_int(0,0));
                ConstTableRef test_2 = rt.get_table("test_2");
                int n_1 = 8*N *  1;
                int n_2 = 8*N * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            shared_groups[i].reset();
        }

        // Check final state via new shared group
        {
            SharedGroup sg(path, no_create, SharedGroup::durability_MemOnly);
            ReadTransaction rt(sg);
#if !defined(_WIN32) || TEST_DURATION > 0 
            rt.get_group().Verify();
#endif
            ConstTableRef test_1 = rt.get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(3*8*N, test_1->get_int(0,0));
            ConstTableRef test_2 = rt.get_table("test_2");
            int n_1 = 8*N *  1;
            int n_2 = 8*N * 18;
            CHECK_EQUAL(n_1+n_2, test_2->size());
            for (int j = 0; j < n_1; ++j)
                CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
            for (int j = n_1; j < n_1+n_2; ++j)
                CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
        }
    }
}


namespace {

TIGHTDB_TABLE_1(MyTable_SpecialOrder, first,  Int)

} // anonymous namespace

TEST(Shared_WritesSpecialOrder)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    const int num_rows = 5; // FIXME: Should be strictly greater than TIGHTDB_MAX_BPNODE_SIZE, but that takes a loooooong time!
    const int num_reps = 25;

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        MyTable_SpecialOrder::Ref table = wt.add_table<MyTable_SpecialOrder>("test");
        for (int i=0; i<num_rows; ++i) {
            table->add(0);
        }
        wt.commit();
    }

    for (int i=0; i<num_rows; ++i) {
        for (int j=0; j<num_reps; ++j) {
            {
                WriteTransaction wt(sg);
                wt.get_group().Verify();
                MyTable_SpecialOrder::Ref table = wt.get_table<MyTable_SpecialOrder>("test");
                CHECK_EQUAL(j, table[i].first);
                ++table[i].first;
                wt.commit();
            }
        }
    }

    {
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        MyTable_SpecialOrder::ConstRef table = rt.get_table<MyTable_SpecialOrder>("test");
        for (int i=0; i<num_rows; ++i) {
            CHECK_EQUAL(num_reps, table[i].first);
        }
    }
}

namespace  {

void writer_threads_thread(TestResults* test_results_ptr, string path, size_t row_ndx)
{
    TestResults& test_results = *test_results_ptr;

    // Open shared db
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    for (size_t i = 0; i < 100; ++i) {
        // Increment cell
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[row_ndx].first += 1;
            // FIXME: For some reason this takes ages when running
            // inside valgrind, it is probably due to the "extreme
            // overallocation" bug. The 1000 transactions performed
            // here can produce a final database file size of more
            // than 1 GiB. Really! And that is a table with only 10
            // rows. It is about 1 MiB per transaction.
            wt.commit();
        }

        // Verify in new transaction so that we interleave
        // read and write transactions
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            int64_t v = t[row_ndx].first;
            int64_t expected = i+1;
            CHECK_EQUAL(expected, v);
        }
    }
}

} // anonymous namespace

TEST(Shared_WriterThreads)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

        const size_t thread_count = 10;
        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            for (size_t i = 0; i < thread_count; ++i)
                t1->add(0, 2, false, "test");
            wt.commit();
        }

        Thread threads[thread_count];

        // Create all threads
        for (size_t i = 0; i < thread_count; ++i)
            threads[i].start(bind(&writer_threads_thread, &test_results, string(path), i));

        // Wait for all threads to complete
        for (size_t i = 0; i < thread_count; ++i)
            threads[i].join();

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                int64_t v = t[i].first;
                CHECK_EQUAL(100, v);
            }
        }
    }

#ifdef LOCKFILE_CLEANUP
    // Verify that lock file was deleted after use
    CHECK(!File::exists(path.get_lock_path()));
#endif
}


#if defined TEST_ROBUSTNESS && defined ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE && !defined TIGHTDB_ENABLE_ENCRYPTION
// Not supported on Windows in particular? Keywords: winbug
TEST(Shared_RobustAgainstDeathDuringWrite)
{
    // Abort if robust mutexes are not supported on the current
    // platform. Otherwise we would probably get into a dead-lock.
    if (!RobustMutex::is_robust_on_this_platform())
        return;

    // This test can only be conducted by spawning independent
    // processes which can then be terminated individually.

    SHARED_GROUP_TEST_PATH(path);

    for (int i = 0; i < 10; ++i) {
        pid_t pid = fork();
        if (pid == pid_t(-1))
            TIGHTDB_TERMINATE("fork() failed");
        if (pid == 0) {
            // Child
            SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TableRef table = wt.add_table("alpha");
            _exit(0); // Die with an active write transaction
        }
        else {
            // Parent
            int stat_loc = 0;
            int options = 0;
            pid = waitpid(pid, &stat_loc, options);
            if (pid == pid_t(-1))
                TIGHTDB_TERMINATE("waitpid() failed");
            bool child_exited_normaly = WIFEXITED(stat_loc);
            CHECK(child_exited_normaly);
            int child_exit_status = WEXITSTATUS(stat_loc);
            CHECK_EQUAL(0, child_exit_status);
        }

        // Check that we can continue without dead-locking
        {
            SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TableRef table = wt.add_table("beta");
            if (table->is_empty()) {
                table->add_column(type_Int, "i");
                table->insert_int(0,0,0);
                table->insert_done();
            }
            table->add_int(0,1);
            wt.commit();
        }
    }

    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        CHECK(!rt.has_table("alpha"));
        CHECK(rt.has_table("beta"));
        ConstTableRef table = rt.get_table("beta");
        CHECK_EQUAL(10, table->get_int(0,0));
    }
}

#endif // defined TEST_ROBUSTNESS && defined ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE && !defined TIGHTDB_ENABLE_ENCRYPTION


TEST(Shared_FormerErrorCase1)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
    {
        DescriptorRef sub_1, sub_2;
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        TableRef table = wt.add_table("my_table");
        table->add_column(type_Int,      "alpha");
        table->add_column(type_Bool,     "beta");
        table->add_column(type_Int,      "gamma");
        table->add_column(type_DateTime, "delta");
        table->add_column(type_String,   "epsilon");
        table->add_column(type_Binary,   "zeta");
        table->add_column(type_Table,    "eta", &sub_1);
        table->add_column(type_Mixed,    "theta");
        sub_1->add_column(type_Int,        "foo");
        sub_1->add_column(type_Table,      "bar", &sub_2);
        sub_2->add_column(type_Int,          "value");
        table->insert_empty_row(0, 1);
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 2);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            table2->insert_int(0, 0, 0);
            table2->insert_subtable(1, 0);
            table2->insert_done();
        }
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 3);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 4);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->insert_empty_row(0, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->insert_empty_row(1, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->set_int(0, 0, 0);
        }
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 5);
        }
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            table2->set_int(0, 0, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        TableRef table = wt.get_table("my_table");
        table = table->get_subtable(6, 0);
        table = table->get_subtable(1, 0);
        table->set_int(0, 1, 1);
        table = wt.get_table("my_table");
        table->set_int(0, 0, 6);
        table = wt.get_table("my_table");
        table = table->get_subtable(6, 0);
        table->set_int(0, 0, 2);
        wt.commit();
    }
}



namespace {

TIGHTDB_TABLE_1(FormerErrorCase2_Subtable,
                value,  Int)

TIGHTDB_TABLE_1(FormerErrorCase2_Table,
                bar, Subtable<FormerErrorCase2_Subtable>)

} // namespace

TEST(Shared_FormerErrorCase2)
{
    SHARED_GROUP_TEST_PATH(path);
    for (int i=0; i<10; ++i) {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        FormerErrorCase2_Table::Ref table = wt.get_or_add_table<FormerErrorCase2_Table>("table");
        table->add();
        table->add();
        table->add();
        table->add();
        table->add();
        table->clear();
        table->add();
        table[0].bar->add();
        wt.commit();
    }
}

namespace {

TIGHTDB_TABLE_1(OverAllocTable,
                text, String)

} // namespace

TEST(Shared_SpaceOveruse)
{
#if TEST_DURATION < 1
    int n_outer = 300;
    int n_inner = 21;
#else
    int n_outer = 3000;
    int n_inner = 42;
#endif

    // Many transactions
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    // Do a lot of sequential transactions
    for (int i = 0; i != n_outer; ++i) {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        OverAllocTable::Ref table = wt.get_or_add_table<OverAllocTable>("my_table");
        for (int j = 0; j != n_inner; ++j)
            table->add("x");
        wt.commit();
    }

    // Verify that all was added correctly
    {
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        OverAllocTable::ConstRef table = rt.get_table<OverAllocTable>("my_table");

        size_t n = table->size();
        CHECK_EQUAL(n_outer * n_inner, n);

        for (size_t i = 0; i != n; ++i)
            CHECK_EQUAL("x", table[i].text);

        table->Verify();
    }
}


TEST(Shared_Notifications)
{
    // Create a new shared db
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    // No other instance have changed db since last transaction
    CHECK(!sg.has_changed());

    {
        // Open the same db again (in empty state)
        SharedGroup sg2(path, false, SharedGroup::durability_Full, crypt_key());

        // Verify that new group is empty
        {
            ReadTransaction rt(sg2);
            CHECK(rt.get_group().is_empty());
        }

        // No other instance have changed db since last transaction
        CHECK(!sg2.has_changed());

        // Add a new table
        {
            WriteTransaction wt(sg2);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            wt.commit();
        }
    }

    // Db has been changed by other instance
    CHECK(sg.has_changed());

    // Verify that the new table has been added
    {
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
        CHECK_EQUAL(1, t1->size());
        CHECK_EQUAL(1, t1[0].first);
        CHECK_EQUAL(2, t1[0].second);
        CHECK_EQUAL(false, t1[0].third);
        CHECK_EQUAL("test", t1[0].fourth);
    }

    // No other instance have changed db since last transaction
    CHECK(!sg.has_changed());
}


TEST(Shared_FromSerialized)
{
    SHARED_GROUP_TEST_PATH(path);

    // Create new group and serialize to disk
    {
        Group g1;
        TestTableShared::Ref t1 = g1.add_table<TestTableShared>("test");
        t1->add(1, 2, false, "test");
        g1.write(path, crypt_key());
    }

    // Open same file as shared group
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    // Verify that contents is there when shared
    {
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
        CHECK_EQUAL(1, t1->size());
        CHECK_EQUAL(1, t1[0].first);
        CHECK_EQUAL(2, t1[0].second);
        CHECK_EQUAL(false, t1[0].third);
        CHECK_EQUAL("test", t1[0].fourth);
    }
}


TEST_IF(Shared_StringIndexBug1, TEST_DURATION >= 1)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup db(path, false, SharedGroup::durability_Full, crypt_key());

    {
        Group& group = db.begin_write();
        TableRef table = group.add_table("users");
        table->add_column(type_String, "username");
        table->add_search_index(0);
        for (int i = 0; i < TIGHTDB_MAX_BPNODE_SIZE + 1; ++i)
            table->add_empty_row();
        for (int i = 0; i < TIGHTDB_MAX_BPNODE_SIZE + 1; ++i)
            table->remove(0);
        db.commit();
    }

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("users");
        table->add_empty_row();
        db.commit();
    }
}


TEST(Shared_StringIndexBug2)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        TableRef table = wt.add_table("a");
        table->add_column(type_String, "b");
        table->add_search_index(0);  // Not adding index makes it work
        table->add_empty_row();
        wt.commit();
    }

    {
        ReadTransaction rt(sg);
        rt.get_group().Verify();
    }
}


namespace {

void rand_str(Random& random, char* res, size_t len)
{
    for (size_t i = 0; i < len; ++i)
        res[i] = char(int('a') + random.draw_int_mod(10));
}

} // anonymous namespace

TEST(Shared_StringIndexBug3)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup db(path, false, SharedGroup::durability_Full, crypt_key());

    {
        Group& group = db.begin_write();
        TableRef table = group.add_table("users");
        table->add_column(type_String, "username");
        table->add_search_index(0);  // Disabling index makes it work
        db.commit();
    }

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    size_t transactions = 0;

    for (size_t n = 0; n < 100; ++n) {
        const uint64_t action = random.draw_int_mod(1000);

        transactions++;

        if (action <= 500) {
            // delete random user
            Group& group = db.begin_write();
            TableRef table = group.get_table("users");
            if (table->size() > 0) {
                size_t del = random.draw_int_mod(table->size());
                //cerr << "-" << del << ": " << table->get_string(0, del) << endl;
                table->remove(del);
                table->Verify();
            }
            db.commit();
        }
        else {
            // add new user
            Group& group = db.begin_write();
            TableRef table = group.get_table("users");
            table->add_empty_row();
            char txt[100];
            rand_str(random, txt, 8);
            txt[8] = 0;
            //cerr << "+" << txt << endl;
            table->set_string(0, table->size() - 1, txt);
            table->Verify();
            db.commit();
        }
    }
}


TEST(Shared_ClearColumnWithBasicArrayRootLeaf)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        WriteTransaction wt(sg);
        TableRef test = wt.add_table("Test");
        test->add_column(type_Double, "foo");
        test->clear();
        test->add_empty_row();
        test->set_double(0, 0, 727.2);
        wt.commit();
    }
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        ReadTransaction rt(sg);
        ConstTableRef test = rt.get_table("Test");
        CHECK_EQUAL(727.2, test->get_double(0,0));
    }
}

// disable shared async on windows and any Apple operating system
// TODO: enable async daemon for OS X - think how to do it in XCode (no issue for build.sh)
#if !defined(_WIN32) && !defined(__APPLE__)
// Todo. Keywords: winbug
TEST_IF(Shared_Async, allow_async)
{
    SHARED_GROUP_TEST_PATH(path);

    // Do some changes in a async db
    {
        bool no_create = false;
        SharedGroup db(path, no_create, SharedGroup::durability_Async);

        for (size_t i = 0; i < 100; ++i) {
//            cout << "t "<<n<<"\n";
            WriteTransaction wt(db);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.get_or_add_table<TestTableShared>("test");
            t1->add(1, i, false, "test");
            wt.commit();
        }
    }

    // Wait for async_commit process to shutdown
    // FIXME: we need a way to determine properly if the daemon has shot down instead of just sleeping
    sleep(1);

    // Read the db again in normal mode to verify
    {
        SharedGroup db(path);

        ReadTransaction rt(db);
        rt.get_group().Verify();
        TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
        CHECK_EQUAL(100, t1->size());
    }
}


namespace  {

#define multiprocess_increments 100

void multiprocess_thread(TestResults* test_results_ptr, string path, size_t row_ndx)
{
    TestResults& test_results = *test_results_ptr;

    // Open shared db
    bool no_create = false;
    SharedGroup sg(path, no_create, SharedGroup::durability_Async);

    for (size_t i = 0; i != multiprocess_increments; ++i) {
        // Increment cell
        {

            WriteTransaction wt(sg);
            wt.get_group().Verify();
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[row_ndx].first += 1;
            // FIXME: For some reason this takes ages when running
            // inside valgrind, it is probably due to the "extreme
            // overallocation" bug. The 1000 transactions performed
            // here can produce a final database file size of more
            // than 1 GiB. Really! And that is a table with only 10
            // rows. It is about 1 MiB per transaction.
            wt.commit();
        }
        // Verify in new transaction so that we interleave
        // read and write transactions
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            int64_t v = t[row_ndx].first;
            int64_t expected = i+1;
            CHECK_EQUAL(expected, v);
        }
    }
}


void multiprocess_make_table(string path, string lock_path, string alone_path, size_t rows)
{
    static_cast<void>(lock_path);
    // Create first table in group
#if 1
    static_cast<void>(alone_path);
#  if 0
    {
        SharedGroup sgr(path);
        SharedGroup sgw(path);
        {
            ReadTransaction rt0(sgr);
            WriteTransaction wt0(sgw);
            wt0.commit();
        }
        ReadTransaction rt(sgr);
        {
        }
        WriteTransaction wt(sgw);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
        WriteTransaction wt2(sgw);
        TestTableShared::Ref t2 = wt2.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t2->add(0, 2, false, "test");
        }
        wt2.commit();
    }
#  else
#    if 0
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#    else
    {
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroup::durability_Async);
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_or_add_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#    endif
#  endif
    // Wait for async_commit process to shutdown
    // FIXME: No good way of doing this
    sleep(1);
#else
    {
        Group g(alone_path, Group::mode_ReadWrite);
        TestTableShared::Ref t1 = g.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i)
            t1->add(0, 2, false, "test");
        printf("Writing db\n");
        g.commit();
    }
#endif
}

void multiprocess_threaded(TestResults& test_results, string path, size_t num_threads, size_t base)
{
    // Do some changes in a async db
    UniquePtr<test_util::ThreadWrapper[]> threads;
    threads.reset(new test_util::ThreadWrapper[num_threads]);

    // Start threads
    for (size_t i = 0; i != num_threads; ++i)
        threads[i].start(bind(&multiprocess_thread, &test_results, path, base+i));

    // Wait for threads to finish
    for (size_t i = 0; i != num_threads; ++i) {
        bool thread_has_thrown = false;
        string except_msg;
        if (threads[i].join(except_msg)) {
            cerr << "Exception thrown in thread "<<i<<": "<<except_msg<<"\n";
            thread_has_thrown = true;
        }
        CHECK(!thread_has_thrown);
    }

    // Verify that the changes were made
    {
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroup::durability_Async);
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

        for (size_t i = 0; i != num_threads; ++i) {
            int64_t v = t[i+base].first;
            CHECK_EQUAL(multiprocess_increments, v);
        }
    }
}

void multiprocess_validate_and_clear(TestResults& test_results, string path, string lock_path,
                                     size_t rows, int result)
{
    // Wait for async_commit process to shutdown
    // FIXME: this is not apropriate
    static_cast<void>(lock_path);
    sleep(1);

    // Verify - once more, in sync mode - that the changes were made
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        TestTableShared::Ref t = wt.get_table<TestTableShared>("test");

        for (size_t i = 0; i != rows; ++i) {
            int64_t v = t[i].first;
            t[i].first = 0;
            CHECK_EQUAL(result, v);
        }
        wt.commit();
    }
}

void multiprocess(TestResults& test_results, string path, int num_procs, size_t num_threads)
{
    int* pids = new int[num_procs];
    for (int i = 0; i != num_procs; ++i) {
        if (0 == (pids[i] = fork())) {
            multiprocess_threaded(test_results, path, num_threads, i*num_threads);
            _exit(0);
        }
    }
    int status = 0;
    for (int i = 0; i != num_procs; ++i)
        waitpid(pids[i], &status, 0);
    delete[] pids;
}

} // anonymous namespace


TEST_IF(Shared_AsyncMultiprocess, allow_async)
{
    SHARED_GROUP_TEST_PATH(path);
    SHARED_GROUP_TEST_PATH(alone_path);

    // wait for any daemon hanging around to exit
    usleep(100); // FIXME: Weird! Is this really acceptable?

#if TEST_DURATION < 1
    multiprocess_make_table(path, path.get_lock_path(), alone_path, 4);

    multiprocess_threaded(test_results, path, 2, 0);
    multiprocess_validate_and_clear(test_results, path, path.get_lock_path(),
                                    2, multiprocess_increments);

    for (int k = 1; k < 3; ++k) {
        multiprocess(test_results, path, 2, 2);
        multiprocess_validate_and_clear(test_results, path, path.get_lock_path(),
                                        4, multiprocess_increments);
    }
#else
    multiprocess_make_table(path, path.get_lock_path(), alone_path, 100);

    multiprocess_threaded(test_results, path, 10, 0);
    multiprocess_validate_and_clear(test_results, path, path.get_lock_path(),
                                    10, multiprocess_increments);

    for (int k = 1; k < 10; ++k) {
        multiprocess(test_results, path, 10, 10);
        multiprocess_validate_and_clear(test_results, path, path.get_lock_path(),
                                        100, multiprocess_increments);
    }
#endif
}

static const int num_threads = 3;
static int shared_state[num_threads];
static Mutex muu;
void waiter(string path, int i)
{
    SharedGroup sg(path, true, SharedGroup::durability_Full);
    {
        LockGuard l(muu);
        shared_state[i] = 1;
    }
    sg.wait_for_change();
    {
        LockGuard l(muu);
        shared_state[i] = 2; // this state should not be observed by the writer
    }
    sg.wait_for_change(); // we'll fall right through here, because we haven't advanced our readlock
    {
        LockGuard l(muu);
        shared_state[i] = 3;
    }
    sg.begin_read();
    sg.end_read();
    sg.wait_for_change(); // this time we'll wait because state hasn't advanced since we did.
    {
        LockGuard l(muu);
        shared_state[i] = 4;
    }
    // works within a read transaction as well
    sg.begin_read();
    sg.wait_for_change();
    sg.end_read();
    {
        LockGuard l(muu);
        shared_state[i] = 5;
    }
}

TEST(Shared_WaitForChange)
{
    SHARED_GROUP_TEST_PATH(path);
    for (int j=0; j < num_threads; j++)
        shared_state[j] = 0;
    SharedGroup sg(path, false, SharedGroup::durability_Full);
    Thread threads[num_threads];
    for (int j=0; j < num_threads; j++)
        threads[j].start(bind(&waiter, string(path), j));
    sleep(1);
    for (int j=0; j < num_threads; j++) {
        LockGuard l(muu);
        CHECK_EQUAL(1, shared_state[j]);
    }

    sg.begin_write();
    sg.commit();
    sleep(1);
    for (int j=0; j < num_threads; j++) {
        LockGuard l(muu);
        CHECK_EQUAL(3, shared_state[j]);
    }
    sg.begin_write();
    sg.commit();
    sleep(1);
    for (int j=0; j < num_threads; j++) {
        LockGuard l(muu);
        CHECK_EQUAL(4, shared_state[j]);
    }
    sg.begin_write();
    sg.commit();
    sleep(1);
    for (int j=0; j < num_threads; j++) {
        LockGuard l(muu);
        CHECK_EQUAL(5, shared_state[j]);
    }
    for (int j=0; j < num_threads; j++)
        threads[j].join();
}

#endif // endif not on windows

TEST(Shared_MultipleSharersOfStreamingFormat)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create non-empty file without free-space tracking
        Group g;
        g.add_table("x");
        g.write(path, crypt_key());
    }
    {
        // See if we can handle overlapped accesses through multiple shared groups
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        SharedGroup sg2(path, false, SharedGroup::durability_Full, crypt_key());
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            CHECK(rt.has_table("x"));
            CHECK(!rt.has_table("gnyf"));
            CHECK(!rt.has_table("baz"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            wt.add_table("baz"); // Add table "baz"
            wt.commit();
        }
        {
            WriteTransaction wt2(sg2);
            wt2.get_group().Verify();
            wt2.add_table("gnyf"); // Add table "gnyf"
            wt2.commit();
        }
    }
}

TEST(Shared_MixedWithNonShared)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create empty file without free-space tracking
        Group g;
        g.write(path, crypt_key());
    }
    {
        // See if we can modify with non-shared group
        Group g(path, crypt_key(), Group::mode_ReadWrite);
        g.add_table("foo"); // Add table "foo"
        g.commit();
    }

    File::try_remove(path);
    {
        // Create non-empty file without free-space tracking
        Group g;
        g.add_table("x");
        g.write(path, crypt_key());
    }
    {
        // See if we can modify with non-shared group
        Group g(path, crypt_key(), Group::mode_ReadWrite);
        g.add_table("foo"); // Add table "foo"
        g.commit();
    }

    File::try_remove(path);
    {
        // Create empty file without free-space tracking
        Group g;
        g.write(path, crypt_key());
    }
    {
        // See if we can read and modify with shared group
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            CHECK(!rt.has_table("foo"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            wt.add_table("foo"); // Add table "foo"
            wt.commit();
        }
    }

    File::try_remove(path);
    {
        // Create non-empty file without free-space tracking
        Group g;
        g.add_table("x");
        g.write(path, crypt_key());
    }
    {
        // See if we can read and modify with shared group
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            CHECK(!rt.has_table("foo"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            wt.add_table("foo"); // Add table "foo"
            wt.commit();
        }
    }
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            CHECK(rt.has_table("foo"));
        }
    }
    {
        // Access using non-shared group
        Group g(path, crypt_key(), Group::mode_ReadWrite);
        g.commit();
    }
    {
        // Modify using non-shared group
        Group g(path, crypt_key(), Group::mode_ReadWrite);
        g.add_table("bar"); // Add table "bar"
        g.commit();
    }
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        {
            ReadTransaction rt(sg);
            rt.get_group().Verify();
            CHECK(rt.has_table("bar"));
        }
    }

#ifndef TIGHTDB_ENABLE_ENCRYPTION // encrpted buffers aren't supported
    // The empty group created initially by a shared group accessor is special
    // in that it contains no nodes, and the root-ref is therefore zero. The
    // following block checks that the contents of such a file is still
    // perceived as valid when placed in a memory buffer, and then opened.
    File::try_remove(path);
    {
        {
            SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key()); // Create the very empty group
        }
        ifstream in(path.c_str());
        string buffer((istreambuf_iterator<char>(in)), istreambuf_iterator<char>());
        bool take_ownership = false;
        Group group(BinaryData(buffer), take_ownership);
        group.Verify();
        CHECK(group.is_empty());
        group.add_table("x");
        group.Verify();
        CHECK_EQUAL(1, group.size());
    }
#endif
}

// @Finn, fixme, find out why it fails on Windows
#if !defined(_WIN32)
TEST(Shared_VersionCount)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg_w(path);
    SharedGroup sg_r(path);
    CHECK_EQUAL(1, sg_r.get_number_of_versions());
    sg_r.begin_read();
    sg_w.begin_write();
    CHECK_EQUAL(1, sg_r.get_number_of_versions());
    sg_w.commit();
    CHECK_EQUAL(2, sg_r.get_number_of_versions());
    sg_w.begin_write();
    sg_w.commit();
    CHECK_EQUAL(3, sg_r.get_number_of_versions());
    sg_r.end_read();
    CHECK_EQUAL(3, sg_r.get_number_of_versions());
    sg_w.begin_write();
    sg_w.commit();
    // both the last and the second-last commit is kept, so once
    // you've committed anything, you will never get back to having
    // just a single version.
    CHECK_EQUAL(2, sg_r.get_number_of_versions());
}
#endif

TEST(Shared_MultipleRollbacks)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
    sg.begin_write();
    sg.rollback();
    sg.rollback();
}


TEST(Shared_MultipleEndReads)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
    sg.begin_read();
    sg.end_read();
    sg.end_read();
}


TEST(Shared_ReserveDiskSpace)
{
    // SharedGroup::reserve() has no effect unless file preallocation
    // is supported.
    if (!File::is_prealloc_supported())
        return;

    SHARED_GROUP_TEST_PATH(path);
    {
        SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
        size_t orig_file_size = size_t(File(path).get_size());

        // Check that reserve() does not change the file size if the
        // specified size is less than the actual file size.
        size_t reserve_size_1 = orig_file_size / 2;
        sg.reserve(reserve_size_1);
        size_t new_file_size_1 = size_t(File(path).get_size());
        CHECK_EQUAL(orig_file_size, new_file_size_1);

        // Check that reserve() does not change the file size if the
        // specified size is equal to the actual file size.
        size_t reserve_size_2 = orig_file_size;
        sg.reserve(reserve_size_2);
        size_t new_file_size_2 = size_t(File(path).get_size());
        CHECK_EQUAL(orig_file_size, new_file_size_2);

        // Check that reserve() does change the file size if the
        // specified size is greater than the actual file size, and
        // that the new size is at least as big as the requested size.
        size_t reserve_size_3 = orig_file_size + 1;
        sg.reserve(reserve_size_3);
        size_t new_file_size_3 = size_t(File(path).get_size());
        CHECK(new_file_size_3 >= reserve_size_3);

        // Check that disk space reservation is independent of transactions
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            wt.add_table<TestTableShared>("table_1")->add_empty_row(2000);
            wt.commit();
        }
        orig_file_size = size_t(File(path).get_size());
        size_t reserve_size_4 = 2 * orig_file_size + 1;
        sg.reserve(reserve_size_4);
        size_t new_file_size_4 = size_t(File(path).get_size());
        CHECK(new_file_size_4 >= reserve_size_4);
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        wt.add_table<TestTableShared>("table_2")->add_empty_row(2000);
        orig_file_size = size_t(File(path).get_size());
        size_t reserve_size_5 = orig_file_size + 333;
        sg.reserve(reserve_size_5);
        size_t new_file_size_5 = size_t(File(path).get_size());
        CHECK(new_file_size_5 >= reserve_size_5);
        wt.add_table<TestTableShared>("table_3")->add_empty_row(2000);
        wt.commit();
        orig_file_size = size_t(File(path).get_size());
        size_t reserve_size_6 = orig_file_size + 459;
        sg.reserve(reserve_size_6);
        size_t new_file_size_6 = size_t(File(path).get_size());
        CHECK(new_file_size_6 >= reserve_size_6);
        {
            WriteTransaction wt(sg);
            wt.get_group().Verify();
            wt.commit();
        }
    }
}


TEST(Shared_MovingEnumStringColumn)
{
    // Test that the 'index in parent' property of the column of unique strings
    // in a ColumnStringEnum is properly adjusted when other string enumeration
    // columns are inserted or removed before it. Note that the parent of the
    // column of unique strings in a ColumnStringEnum is a child of an array
    // node in the Spec class.

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("foo");
        table->add_column(type_String, "");
        table->add_empty_row(64);
        for (int i = 0; i < 64; ++i)
            table->set_string(0, i, "foo");
        table->optimize();
        CHECK_EQUAL(1, table->get_descriptor()->get_num_unique_values(0));
        wt.commit();
    }
    // Insert new string enumeration column
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("foo");
        CHECK_EQUAL(1, table->get_descriptor()->get_num_unique_values(0));
        table->insert_column(0, type_String, "");
        for (int i = 0; i < 64; ++i)
            table->set_string(0, i, i%2 == 0 ? "a" : "b");
        table->optimize();
        wt.get_group().Verify();
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(1, table->get_descriptor()->get_num_unique_values(1));
        table->set_string(1, 0, "bar0");
        table->set_string(1, 1, "bar1");
        wt.get_group().Verify();
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        wt.commit();
    }
    {
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        ConstTableRef table = rt.get_table("foo");
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        for (int i = 0; i < 64; ++i) {
            string value = table->get_string(0, i);
            if (i%2 == 0) {
                CHECK_EQUAL("a", value);
            }
            else {
                CHECK_EQUAL("b", value);
            }
            value = table->get_string(1, i);
            if (i == 0) {
                CHECK_EQUAL("bar0", value);
            }
            else if (i == 1) {
                CHECK_EQUAL("bar1", value);
            }
            else {
                CHECK_EQUAL("foo", value);
            }
        }
    }
    // Remove the recently inserted string enumeration column
    {
        WriteTransaction wt(sg);
        wt.get_group().Verify();
        TableRef table = wt.get_table("foo");
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        table->remove_column(0);
        wt.get_group().Verify();
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(0));
        table->set_string(0, 2, "bar2");
        wt.get_group().Verify();
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(0));
        wt.commit();
    }
    {
        ReadTransaction rt(sg);
        rt.get_group().Verify();
        ConstTableRef table = rt.get_table("foo");
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(0));
        for (int i = 0; i < 64; ++i) {
            string value = table->get_string(0, i);
            if (i == 0) {
                CHECK_EQUAL("bar0", value);
            }
            else if (i == 1) {
                CHECK_EQUAL("bar1", value);
            }
            else if (i == 2) {
                CHECK_EQUAL("bar2", value);
            }
            else {
                CHECK_EQUAL("foo", value);
            }
        }
    }
}


TEST(Shared_MovingSearchIndex)
{
    // Test that the 'index in parent' property of search indexes is properly
    // adjusted when columns are inserted or removed at a lower column_index.

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());

    // Create a regular string column and an enumeration strings column, and
    // equip both with search indexes.
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("foo");
        table->add_column(type_String, "regular");
        table->add_column(type_String, "enum");
        table->add_empty_row(64);
        for (int i = 0; i < 64; ++i) {
            ostringstream out;
            out << "foo" << i;
            table->set_string(0, i, out.str());
            table->set_string(1, i, "bar");
        }
        table->set_string(1, 63, "bar63");
        table->optimize();
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(1));
        table->add_search_index(0);
        table->add_search_index(1);
        wt.get_group().Verify();
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        wt.commit();
    }
    // Insert a new column before the two string columns.
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("foo");
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        table->insert_column(0, type_Int, "i");
        wt.get_group().Verify();
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(62, table->find_first_string(1, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(2, "bar63"));
        table->set_string(1, 0, "foo_X");
        table->set_string(2, 0, "bar_X");
        wt.get_group().Verify();
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(2, "bad"));
        CHECK_EQUAL(0,  table->find_first_string(1, "foo_X"));
        CHECK_EQUAL(31, table->find_first_string(1, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(1, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(1, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "foo63"));
        CHECK_EQUAL(0,  table->find_first_string(2, "bar_X"));
        CHECK_EQUAL(1,  table->find_first_string(2, "bar"));
        CHECK_EQUAL(63, table->find_first_string(2, "bar63"));
        wt.commit();
    }
    // Remove the recently inserted column
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("foo");
        CHECK(table->has_search_index(1) && table->has_search_index(2));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(2, "bad"));
        CHECK_EQUAL(0,  table->find_first_string(1, "foo_X"));
        CHECK_EQUAL(31, table->find_first_string(1, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(1, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(1, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "foo63"));
        CHECK_EQUAL(0,  table->find_first_string(2, "bar_X"));
        CHECK_EQUAL(1,  table->find_first_string(2, "bar"));
        CHECK_EQUAL(63, table->find_first_string(2, "bar63"));
        table->remove_column(0);
        wt.get_group().Verify();
        CHECK(table->has_search_index(0) && table->has_search_index(1));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(0, "bad"));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(0,  table->find_first_string(0, "foo_X"));
        CHECK_EQUAL(31, table->find_first_string(0, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(0, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(0, "foo63"));
        CHECK_EQUAL(0,  table->find_first_string(1, "bar_X"));
        CHECK_EQUAL(1,  table->find_first_string(1, "bar"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        table->set_string(0, 1, "foo_Y");
        table->set_string(1, 1, "bar_Y");
        wt.get_group().Verify();
        CHECK(table->has_search_index(0) && table->has_search_index(1));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(0, "bad"));
        CHECK_EQUAL(tightdb::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(0,  table->find_first_string(0, "foo_X"));
        CHECK_EQUAL(1,  table->find_first_string(0, "foo_Y"));
        CHECK_EQUAL(31, table->find_first_string(0, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(0, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(0, "foo63"));
        CHECK_EQUAL(0,  table->find_first_string(1, "bar_X"));
        CHECK_EQUAL(1,  table->find_first_string(1, "bar_Y"));
        CHECK_EQUAL(2,  table->find_first_string(1, "bar"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        wt.commit();
    }
}


TEST_IF(Shared_ArrayEraseBug, TEST_DURATION >= 1)
{
    // This test only makes sense when we can insert a number of rows
    // equal to the square of the maximum B+-tree node size.
    size_t max_node_size = TIGHTDB_MAX_BPNODE_SIZE;
    size_t max_node_size_squared = max_node_size;
    if (int_multiply_with_overflow_detect(max_node_size_squared, max_node_size))
        return;

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroup::durability_Full, crypt_key());
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        table->add_column(type_Int, "");
        for (size_t i = 0; i < max_node_size_squared; ++i)
            table->insert_empty_row(0);
        wt.commit();
    }
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        size_t row_ndx = max_node_size_squared - max_node_size - max_node_size/2;
        table->insert_empty_row(row_ndx);
        wt.commit();
    }
}

#endif // TEST_SHARED
