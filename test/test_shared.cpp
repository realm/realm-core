/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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
#ifdef TEST_SHARED

#include <streambuf>
#include <fstream>
#include <tuple>

// Need fork() and waitpid() for Shared_RobustAgainstDeathDuringWrite
#ifndef _WIN32
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <csignal>
#include <sched.h>
#define ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE
#else
#define NOMINMAX
#include <windows.h>
#endif

#include <realm.hpp>
#include <realm/util/features.h>
#include <realm/util/safe_int_ops.hpp>
#include <memory>
#include <realm/util/terminate.hpp>
#include <realm/util/file.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/to_string.hpp>
#include <realm/impl/simulated_failure.hpp>

#include "fuzz_group.hpp"

#include "test.hpp"

extern unsigned int unit_test_random_seed;

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;


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
#if !defined(_WIN32) && !REALM_PLATFORM_APPLE
#if REALM_ANDROID || defined DISABLE_ASYNC || REALM_ENABLE_ENCRYPTION
bool allow_async = false;
#else
bool allow_async = true;
#endif
#endif


REALM_TABLE_4(TestTableShared, first, Int, second, Int, third, Bool, fourth, String)

REALM_TABLE_5(TestTableSharedTimestamp, first, Int, second, Int, third, Bool, fourth, String, fifth, Timestamp)

void writer(std::string path, int id)
{
    // std::cerr << "Started writer " << std::endl;
    try {
        bool done = false;
        SharedGroup sg(path, true, SharedGroupOptions(crypt_key()));
        // std::cerr << "Opened sg " << std::endl;
        for (int i = 0; !done; ++i) {
            // std::cerr << "       - " << getpid() << std::endl;
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            done = t1[id].third;
            if (i & 1) {
                t1[id].first = 1 + t1[id].first;
            }
            sched_yield(); // increase chance of signal arriving in the middle of a transaction
            wt.commit();
        }
        // std::cerr << "Ended pid " << getpid() << std::endl;
    }
    catch (...) {
        // std::cerr << "Exception from " << getpid() << std::endl;
        REALM_ASSERT(false);
    }
}


#if !defined(_WIN32) && !REALM_ENABLE_ENCRYPTION

void killer(TestContext& test_context, int pid, std::string path, int id)
{
    {
        SharedGroup sg(path, true, SharedGroupOptions(crypt_key()));
        bool done = false;
        do {
            sched_yield();
            // pseudo randomized wait (to prevent unwanted synchronization effects of yield):
            int n = random() % 10000;
            volatile int thing = 0;
            while (n--)
                thing += random();
            ReadTransaction rt(sg);
            rt.get_group().verify();
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
            std::cerr << "waitpid was interrupted" << std::endl;
        if (errno == EINVAL)
            std::cerr << "waitpid got bad arguments" << std::endl;
        if (errno == ECHILD)
            std::cerr << "waitpid tried to wait for the wrong child: " << pid << std::endl;
        REALM_TERMINATE("waitpid failed");
    }
    bool child_exited_from_signal = WIFSIGNALED(stat_loc);
    CHECK(child_exited_from_signal);
    int child_exit_status = WEXITSTATUS(stat_loc);
    CHECK_EQUAL(0, child_exit_status);
    {
        // Verify that we surely did kill the process before it could do all it's commits.
        SharedGroup sg(path, true);
        ReadTransaction rt(sg);
        rt.get_group().verify();
        TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
        CHECK(10 < t1[id].first);
    }
}
#endif

} // anonymous namespace

#if !defined(_WIN32) && !REALM_ENABLE_ENCRYPTION && !REALM_ANDROID

TEST_IF(Shared_PipelinedWritesWithKills, false)
{
    // FIXME: This test was disabled because it has a strong tendency to leave
    // rogue child processes behind after the root test process aborts. If these
    // orphanned child processes are not manually searched for and killed, they
    // will run indefinitely. Additionally, these child processes will typically
    // grow a Realm file to gigantic sizes over time (100 gigabytes per 20
    // minutes).
    //
    // Idea for solution: Install a custom signal handler for SIGABRT and
    // friends, and kill all spawned child processes from it. See `man abort`.

    CHECK(RobustMutex::is_robust_on_this_platform());
    const int num_processes = 50;
    SHARED_GROUP_TEST_PATH(path);
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        // Create table entries
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
        for (int i = 0; i < num_processes; ++i) {
            t1->add(0, i, false, "test");
        }
        wt.commit();
    }
    int pid = fork();
    if (pid == -1)
        REALM_TERMINATE("fork() failed");
    if (pid == 0) {
        // first writer!
        writer(path, 0);
        _Exit(0);
    }
    else {
        for (int k = 1; k < num_processes; ++k) {
            int pid2 = pid;
            pid = fork();
            if (pid == pid_t(-1))
                REALM_TERMINATE("fork() failed");
            if (pid == 0) {
                writer(path, k);
                _Exit(0);
            }
            else {
                // std::cerr << "New process " << pid << " killing old " << pid2 << std::endl;
                killer(test_context, pid2, path, k - 1);
            }
        }
        // std::cerr << "Killing last one: " << pid << std::endl;
        killer(test_context, pid, path, num_processes - 1);
    }
    // We need to wait cleaning up til the killed processes have exited.
    sleep(1);
}
#endif


TEST(Shared_CompactingOnTheFly)
{
    SHARED_GROUP_TEST_PATH(path);
    Thread writer_thread;
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        // Create table entries
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            for (int i = 0; i < 100; ++i) {
                t1->add(0, i, false, "test");
            }
            wt.commit();
        }
        {
            writer_thread.start(std::bind(&writer, std::string(path), 41));

            // make sure writer has started:
            bool waiting = true;
            while (waiting) {
                sched_yield();
                ReadTransaction rt(sg);
                TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
                waiting = t1[41].first == 0;
                // std::cerr << t1[41].first << std::endl;
            }

            // since the writer is running, we cannot compact:
            CHECK(sg.compact() == false);
        }
        {
            // make the writer thread terminate:
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[41].third = true;
            wt.commit();
        }
    }
    writer_thread.join();
    {
        SharedGroup sg2(path, true, SharedGroupOptions(crypt_key()));
        {
            sg2.begin_write();
            sg2.commit();
        }
        CHECK_EQUAL(true, sg2.compact());

        ReadTransaction rt2(sg2);
        TestTableShared::ConstRef table = rt2.get_table<TestTableShared>("test");
        CHECK(table);
        CHECK_EQUAL(table->size(), 100);
        rt2.get_group().verify();
        sg2.close();
    }
    {
        SharedGroup sg2(path, true, SharedGroupOptions(crypt_key()));
        ReadTransaction rt2(sg2);
        TestTableShared::ConstRef table = rt2.get_table<TestTableShared>("test");
        CHECK(table);
        CHECK_EQUAL(table->size(), 100);
        rt2.get_group().verify();
    }
}


TEST(Shared_Initial)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

        // Verify that new group is empty
        {
            ReadTransaction rt(sg);
            CHECK(rt.get_group().is_empty());
        }
    }
}


TEST(Shared_InitialMem)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));

        // Verify that new group is empty
        {
            ReadTransaction rt(sg);
            CHECK(rt.get_group().is_empty());
        }
    }

    // In MemOnly mode, the database file must be automatically
    // removed.
    CHECK(!File::exists(path));
}


TEST(Shared_InitialMem_StaleFile)
{
    SHARED_GROUP_TEST_PATH(path);

    // On platforms which do not support automatically deleting a file when it's
    // closed, MemOnly files won't be deleted if the process crashes, and so any
    // existing file at the given path should be overwritten if no one has the
    // file open

    // Create a MemOnly realm at the path so that a lock file gets initialized
    {
        bool no_create = false;
        SharedGroup(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));
    }
    CHECK(!File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    // Create a file at the DB path to fake a process crashing and failing to
    // delete it
    {
        File f(path, File::mode_Write);
        f.write("text");
    }
    CHECK(File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    // Verify that we can still open the path as a MemOnly SharedGroup and that
    // it's cleaned up afterwards
    {
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));
        CHECK(File::exists(path));
    }
    CHECK(!File::exists(path));
    CHECK(File::exists(path.get_lock_path()));
}


TEST(Shared_Initial2)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

        {
            // Open the same db again (in empty state)
            SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                wt.get_group().verify();
                TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", t1[0].fourth);
        }
    }
}


TEST(Shared_Initial2_Mem)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));

        {
            // Open the same db again (in empty state)
            SharedGroup sg2(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                wt.get_group().verify();
                TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", t1[0].fourth);
        }
    }
}


TEST(Shared_1)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        Timestamp first_timestamp_value{1, 1};

        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TestTableSharedTimestamp::Ref t1 = wt.add_table<TestTableSharedTimestamp>("test");
            t1->add(1, 2, false, "test", Timestamp{1, 1});
            wt.commit();
        }

        // Open same db again
        SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));
        {
            ReadTransaction rt(sg2);
            rt.get_group().verify();

            // Verify that last set of changes are commited
            TestTableSharedTimestamp::ConstRef t2 = rt.get_table<TestTableSharedTimestamp>("test");
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);
            CHECK_EQUAL(first_timestamp_value, t2[0].fifth);

            // Do a new change while stil having current read transaction open
            {
                WriteTransaction wt(sg);
                wt.get_group().verify();
                TestTableSharedTimestamp::Ref t1 = wt.get_table<TestTableSharedTimestamp>("test");
                t1->add(2, 3, true, "more test", Timestamp{2, 2});
                wt.commit();
            }

            // Verify that that the read transaction does not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);
            CHECK_EQUAL(first_timestamp_value, t2[0].fifth);
            // Do one more new change while stil having current read transaction open
            // so we know that it does not overwrite data held by
            {
                WriteTransaction wt(sg);
                wt.get_group().verify();
                TestTableSharedTimestamp::Ref t1 = wt.get_table<TestTableSharedTimestamp>("test");
                t1->add(0, 1, false, "even more test", Timestamp{3, 3});
                wt.commit();
            }

            // Verify that that the read transaction does still not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);
            CHECK_EQUAL(first_timestamp_value, t2[0].fifth);
        }

        // Start a new read transaction and verify that it can now see the changes
        {
            ReadTransaction rt(sg2);
            rt.get_group().verify();
            TestTableSharedTimestamp::ConstRef t3 = rt.get_table<TestTableSharedTimestamp>("test");

            CHECK(t3->size() == 3);
            CHECK_EQUAL(1, t3[0].first);
            CHECK_EQUAL(2, t3[0].second);
            CHECK_EQUAL(false, t3[0].third);
            CHECK_EQUAL("test", t3[0].fourth);
            CHECK_EQUAL(first_timestamp_value, t3[0].fifth);
            CHECK_EQUAL(2, t3[1].first);
            CHECK_EQUAL(3, t3[1].second);
            CHECK_EQUAL(true, t3[1].third);
            CHECK_EQUAL("more test", t3[1].fourth);
            Timestamp second_timestamp_value{2, 2};
            CHECK_EQUAL(second_timestamp_value, t3[1].fifth);
            CHECK_EQUAL(0, t3[2].first);
            CHECK_EQUAL(1, t3[2].second);
            CHECK_EQUAL(false, t3[2].third);
            CHECK_EQUAL("even more test", t3[2].fourth);
            Timestamp third_timestamp_value{3, 3};
            CHECK_EQUAL(third_timestamp_value, t3[2].fifth);
        }
    }
}


TEST(Shared_Rollback)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

        // Create first table in group (but rollback)
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            // Note: Implicit rollback
        }

        // Verify that no changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            CHECK(!rt.get_group().has_table("test"));
        }

        // Really create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
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
            wt.get_group().verify();
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(0, 0, true, "more test");
            // Note: Implicit rollback
        }

        // Verify that no changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            CHECK(t->size() == 1);
            CHECK_EQUAL(1, t[0].first);
            CHECK_EQUAL(2, t[0].second);
            CHECK_EQUAL(false, t[0].third);
            CHECK_EQUAL("test", t[0].fourth);
        }
    }
}


TEST(Shared_Writes)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            t1->add(0, 2, false, "test");
            wt.commit();
        }

        // Do a lot of repeated write transactions
        for (size_t i = 0; i < 100; ++i) {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[0].first += 1;
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            const int64_t v = t[0].first;
            CHECK_EQUAL(100, v);
        }
    }
}


TEST(Shared_AddColumnToSubspec)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    // Create table with a non-empty subtable
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        DescriptorRef sub_1;
        table->add_column(type_Table, "subtable", &sub_1);
        sub_1->add_column(type_Int, "int");
        table->add_empty_row();
        TableRef subtable = table->get_subtable(0, 0);
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
        TableRef subtable = table->get_subtable(0, 0);
        CHECK_EQUAL(2, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(type_Int, subtable->get_column_type(1));
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0, 0));
        subtable->add_empty_row();
        CHECK_EQUAL(2, subtable->size());
        subtable->set_int(1, 1, 654);
        CHECK_EQUAL(654, subtable->get_int(1, 1));
        wt.commit();
    }

    // Check that the subtable continues to have the right contents
    {
        ReadTransaction rt(sg);
        ConstTableRef table = rt.get_table("table");
        ConstTableRef subtable = table->get_subtable(0, 0);
        CHECK_EQUAL(2, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(type_Int, subtable->get_column_type(1));
        CHECK_EQUAL(2, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0, 0));
        CHECK_EQUAL(0, subtable->get_int(0, 1));
        CHECK_EQUAL(0, subtable->get_int(1, 0));
        CHECK_EQUAL(654, subtable->get_int(1, 1));
    }
}


TEST(Shared_RemoveColumnBeforeSubtableColumn)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    // Create table with a non-empty subtable in a subtable column
    // that is preceded by another column
    {
        WriteTransaction wt(sg);
        DescriptorRef sub_1;
        TableRef table = wt.add_table("table");
        table->add_column(type_Int, "int");
        table->add_column(type_Table, "subtable", &sub_1);
        sub_1->add_column(type_Int, "int");
        table->add_empty_row();
        TableRef subtable = table->get_subtable(1, 0);
        subtable->add_empty_row();
        subtable->set_int(0, 0, 789);
        wt.commit();
    }

    // Remove a column that precedes the subtable column
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        table->remove_column(0);
        TableRef subtable = table->get_subtable(0, 0);
        CHECK_EQUAL(1, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0, 0));
        subtable->add_empty_row();
        CHECK_EQUAL(2, subtable->size());
        subtable->set_int(0, 1, 654);
        CHECK_EQUAL(654, subtable->get_int(0, 1));
        wt.commit();
    }

    // Check that the subtable continues to have the right contents
    {
        ReadTransaction rt(sg);
        ConstTableRef table = rt.get_table("table");
        ConstTableRef subtable = table->get_subtable(0, 0);
        CHECK_EQUAL(1, subtable->get_column_count());
        CHECK_EQUAL(type_Int, subtable->get_column_type(0));
        CHECK_EQUAL(2, subtable->size());
        CHECK_EQUAL(789, subtable->get_int(0, 0));
        CHECK_EQUAL(654, subtable->get_int(0, 1));
    }
}

namespace {

void add_int(Table& table, size_t col_ndx, int_fast64_t diff)
{
    for (size_t i = 0; i < table.size(); ++i) {
        table.set_int(col_ndx, i, table.get_int(col_ndx, i) + diff);
    }
}

} // anonymous namespace


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
    int rounds[] = {3, 5, 7, 9, 11, 13};
#else
    int rounds[] = {3, 5, 11, 15, 17, 23, 27, 31, 47, 59};
#endif
    const int num_rounds = sizeof rounds / sizeof *rounds;

    const int max_N = 64;
    CHECK(max_N >= rounds[num_rounds - 1]);
    std::unique_ptr<SharedGroup> shared_groups[8 * max_N];
    std::unique_ptr<ReadTransaction> read_transactions[8 * max_N];

    for (int round = 0; round < num_rounds; ++round) {
        int N = rounds[round];

        SHARED_GROUP_TEST_PATH(path);

        bool no_create = false;
        SharedGroup root_sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));

        // Add two tables
        {
            WriteTransaction wt(root_sg);
            wt.get_group().verify();
            TableRef test_1 = wt.get_or_add_table("test_1");
            test_1->add_column(type_Int, "i");
            test_1->insert_empty_row(0);
            test_1->set_int(0, 0, 0);
            TableRef test_2 = wt.get_or_add_table("test_2");
            test_2->add_column(type_Binary, "b");
            wt.commit();
        }


        // Create 8*N shared group accessors
        for (int i = 0; i < 8 * N; ++i)
            shared_groups[i].reset(
                new SharedGroup(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly)));

        // Initiate 2*N read transactions with progressive changes
        for (int i = 0; i < 2 * N; ++i) {
            read_transactions[i].reset(new ReadTransaction(*shared_groups[i]));
            read_transactions[i]->get_group().verify();
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                CHECK_EQUAL(i, test_1->get_int(0, 0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0, j));
                for (int j = n_1; j < n_1 + n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0, j));
            }
            {
                WriteTransaction wt(root_sg);
                wt.get_group().verify();
                TableRef test_1 = wt.get_table("test_1");
                add_int(*test_1, 0, 1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->insert_empty_row(0);
                test_2->set_binary(0, 0, BinaryData(chunk_1));
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
                wt.get_group().verify();
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->insert_empty_row(test_2->size());
                    test_2->set_binary(0, test_2->size() - 1, BinaryData(chunk_2));
                }
                wt.commit();
            }
        }

        // Check isolation between read transactions
        for (int i = 0; i < 2 * N; ++i) {
            ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(i, test_1->get_int(0, 0));
            ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
            int n_1 = i * 1;
            int n_2 = i * 18;
            CHECK_EQUAL(n_1 + n_2, test_2->size());
            for (int j = 0; j < n_1; ++j)
                CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0, j));
            for (int j = n_1; j < n_1 + n_2; ++j)
                CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0, j));
        }

        // End the first half of the read transactions during further
        // changes
        for (int i = N - 1; i >= 0; --i) {
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0 // These .verify() calls are horribly slow on Windows
                wt.get_group().verify();
#endif
                TableRef test_1 = wt.get_table("test_1");
                add_int(*test_1, 0, 2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(i, test_1->get_int(0, 0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0, j));
                for (int j = n_1; j < n_1 + n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0, j));
            }
            read_transactions[i].reset();
        }

        // Initiate 6*N extra read transactionss with further progressive changes
        for (int i = 2 * N; i < 8 * N; ++i) {
            read_transactions[i].reset(new ReadTransaction(*shared_groups[i]));
#if !defined(_WIN32) || TEST_DURATION > 0
            read_transactions[i]->get_group().verify();
#endif
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                int i_2 = 2 * N + i;
                CHECK_EQUAL(i_2, test_1->get_int(0, 0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0, j));
                for (int j = n_1; j < n_1 + n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0, j));
            }
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0
                wt.get_group().verify();
#endif
                TableRef test_1 = wt.get_table("test_1");
                add_int(*test_1, 0, 1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->insert_empty_row(0);
                test_2->set_binary(0, 0, BinaryData(chunk_1));
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0
                wt.get_group().verify();
#endif
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->insert_empty_row(test_2->size());
                    test_2->set_binary(0, test_2->size() - 1, BinaryData(chunk_2));
                }
                wt.commit();
            }
        }

        // End all remaining read transactions during further changes
        for (int i = 1 * N; i < 8 * N; ++i) {
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0
                wt.get_group().verify();
#endif
                TableRef test_1 = wt.get_table("test_1");
                add_int(*test_1, 0, 2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                int i_2 = i < 2 * N ? i : 2 * N + i;
                CHECK_EQUAL(i_2, test_1->get_int(0, 0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0, j));
                for (int j = n_1; j < n_1 + n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0, j));
            }
            read_transactions[i].reset();
        }

        // Check final state via each shared group, then destroy it
        for (int i = 0; i < 8 * N; ++i) {
            {
                ReadTransaction rt(*shared_groups[i]);
#if !defined(_WIN32) || TEST_DURATION > 0
                rt.get_group().verify();
#endif
                ConstTableRef test_1 = rt.get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(3 * 8 * N, test_1->get_int(0, 0));
                ConstTableRef test_2 = rt.get_table("test_2");
                int n_1 = 8 * N * 1;
                int n_2 = 8 * N * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0, j));
                for (int j = n_1; j < n_1 + n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0, j));
            }
            shared_groups[i].reset();
        }

        // Check final state via new shared group
        {
            SharedGroup sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::MemOnly));
            ReadTransaction rt(sg);
#if !defined(_WIN32) || TEST_DURATION > 0
            rt.get_group().verify();
#endif
            ConstTableRef test_1 = rt.get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(3 * 8 * N, test_1->get_int(0, 0));
            ConstTableRef test_2 = rt.get_table("test_2");
            int n_1 = 8 * N * 1;
            int n_2 = 8 * N * 18;
            CHECK_EQUAL(n_1 + n_2, test_2->size());
            for (int j = 0; j < n_1; ++j)
                CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0, j));
            for (int j = n_1; j < n_1 + n_2; ++j)
                CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0, j));
        }
    }
}

#ifndef _WIN32 // FIXME: Some times crashes on Windows

// This test is a minimal repro. of core issue #842.
TEST(Many_ConcurrentReaders)
{
    SHARED_GROUP_TEST_PATH(path);
    const std::string path_str = path;

    // setup
    SharedGroup sg_w(path_str);
    WriteTransaction wt(sg_w);
    TableRef t = wt.add_table("table");
    size_t col_ndx = t->add_column(type_String, "column");
    t->add_empty_row(1);
    t->set_string(col_ndx, 0, StringData("string"));
    wt.commit();
    sg_w.close();

    auto reader = [path_str]() {
        try {
            for (int i = 0; i < 1000; ++i) {
                SharedGroup sg_r(path_str);
                ReadTransaction rt(sg_r);
                rt.get_group().verify();
            }
        }
        catch (...) {
            REALM_ASSERT(false);
        }
    };

    constexpr int num_threads = 4;
    Thread threads[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        threads[i].start(reader);
    }
    for (int i = 0; i < num_threads; ++i) {
        threads[i].join();
    }
}

#endif // #ifndef _WIN32

namespace {

REALM_TABLE_1(MyTable_SpecialOrder, first, Int)

} // anonymous namespace

TEST(Shared_WritesSpecialOrder)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    const int num_rows =
        5; // FIXME: Should be strictly greater than REALM_MAX_BPNODE_SIZE, but that takes too long time.
    const int num_reps = 25;

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        MyTable_SpecialOrder::Ref table = wt.add_table<MyTable_SpecialOrder>("test");
        for (int i = 0; i < num_rows; ++i) {
            table->add(0);
        }
        wt.commit();
    }

    for (int i = 0; i < num_rows; ++i) {
        for (int j = 0; j < num_reps; ++j) {
            {
                WriteTransaction wt(sg);
                wt.get_group().verify();
                MyTable_SpecialOrder::Ref table = wt.get_table<MyTable_SpecialOrder>("test");
                CHECK_EQUAL(j, table[i].first);
                ++table[i].first;
                wt.commit();
            }
        }
    }

    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        MyTable_SpecialOrder::ConstRef table = rt.get_table<MyTable_SpecialOrder>("test");
        for (int i = 0; i < num_rows; ++i) {
            CHECK_EQUAL(num_reps, table[i].first);
        }
    }
}

namespace {

void writer_threads_thread(TestContext& test_context, std::string path, size_t row_ndx)
{
    // Open shared db
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    for (size_t i = 0; i < 100; ++i) {
        // Increment cell
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
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
            rt.get_group().verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            int64_t v = t[row_ndx].first;
            int64_t expected = i + 1;
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
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

        const size_t thread_count = 10;
        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TestTableShared::Ref t1 = wt.add_table<TestTableShared>("test");
            for (size_t i = 0; i < thread_count; ++i)
                t1->add(0, 2, false, "test");
            wt.commit();
        }

        Thread threads[thread_count];

        // Create all threads
        for (size_t i = 0; i < thread_count; ++i)
            threads[i].start([this, &path, i] { writer_threads_thread(test_context, path, i); });

        // Wait for all threads to complete
        for (size_t i = 0; i < thread_count; ++i)
            threads[i].join();

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                int64_t v = t[i].first;
                CHECK_EQUAL(100, v);
            }
        }
    }
}


#if !REALM_ENABLE_ENCRYPTION && defined(ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE)
// this unittest has issues that has not been fully understood, but could be
// related to interaction between posix robust mutexes and the fork() system call.
// it has so far only been seen failing on Linux, so we enable it on ios.
#if REALM_PLATFORM_APPLE

// Not supported on Windows in particular? Keywords: winbug
TEST(Shared_RobustAgainstDeathDuringWrite)
{
    // Abort if robust mutexes are not supported on the current
    // platform. Otherwise we would probably get into a dead-lock.
    if (!RobustMutex::is_robust_on_this_platform())
        return;

    // This test can only be conducted by spawning independent
    // processes which can then be terminated individually.
    const int process_count = 100;
    SHARED_GROUP_TEST_PATH(path);

    for (int i = 0; i < process_count; ++i) {
        pid_t pid = fork();
        if (pid == pid_t(-1))
            REALM_TERMINATE("fork() failed");
        if (pid == 0) {
            // Child
            SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TableRef table = wt.get_or_add_table("alpha");
            _Exit(42); // Die hard with an active write transaction
        }
        else {
            // Parent
            int stat_loc = 0;
            int options = 0;
            pid = waitpid(pid, &stat_loc, options);
            if (pid == pid_t(-1))
                REALM_TERMINATE("waitpid() failed");
            bool child_exited_normaly = WIFEXITED(stat_loc);
            CHECK(child_exited_normaly);
            int child_exit_status = WEXITSTATUS(stat_loc);
            CHECK_EQUAL(42, child_exit_status);
        }

        // Check that we can continue without dead-locking
        {
            SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TableRef table = wt.get_or_add_table("beta");
            if (table->is_empty()) {
                table->add_column(type_Int, "i");
                table->insert_empty_row(0);
                table->set_int(0, 0, 0);
            }
            add_int(*table, 0, 1);
            wt.commit();
        }
    }

    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        ReadTransaction rt(sg);
        rt.get_group().verify();
        CHECK(!rt.has_table("alpha"));
        CHECK(rt.has_table("beta"));
        ConstTableRef table = rt.get_table("beta");
        CHECK_EQUAL(process_count, table->get_int(0, 0));
    }
}

#endif // on apple
#endif // encryption enabled

// not ios or android
//#endif // defined TEST_ROBUSTNESS && defined ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE && !REALM_ENABLE_ENCRYPTION


TEST(Shared_FormerErrorCase1)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
    {
        DescriptorRef sub_1, sub_2;
        WriteTransaction wt(sg);
        wt.get_group().verify();
        TableRef table = wt.add_table("my_table");
        table->add_column(type_Int, "alpha");
        table->add_column(type_Bool, "beta");
        table->add_column(type_Int, "gamma");
        table->add_column(type_OldDateTime, "delta");
        table->add_column(type_String, "epsilon");
        table->add_column(type_Binary, "zeta");
        table->add_column(type_Table, "eta", &sub_1);
        table->add_column(type_Mixed, "theta");
        sub_1->add_column(type_Int, "foo");
        sub_1->add_column(type_Table, "bar", &sub_2);
        sub_2->add_column(type_Int, "value");
        table->insert_empty_row(0, 1);
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 2);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            table2->insert_empty_row(0);
            table2->set_int(0, 0, 0);
        }
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 3);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 4);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
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
        wt.get_group().verify();
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
        wt.get_group().verify();
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
        wt.get_group().verify();
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

REALM_TABLE_1(FormerErrorCase2_Subtable, value, Int)

REALM_TABLE_1(FormerErrorCase2_Table, bar, Subtable<FormerErrorCase2_Subtable>)

} // namespace

TEST(Shared_FormerErrorCase2)
{
    SHARED_GROUP_TEST_PATH(path);
    for (int i = 0; i < 10; ++i) {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.get_group().verify();
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

REALM_TABLE_1(OverAllocTable, text, String)

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
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    // Do a lot of sequential transactions
    for (int i = 0; i != n_outer; ++i) {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        OverAllocTable::Ref table = wt.get_or_add_table<OverAllocTable>("my_table");
        for (int j = 0; j != n_inner; ++j)
            table->add("x");
        wt.commit();
    }

    // Verify that all was added correctly
    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        OverAllocTable::ConstRef table = rt.get_table<OverAllocTable>("my_table");

        size_t n = table->size();
        CHECK_EQUAL(n_outer * n_inner, n);

        for (size_t i = 0; i != n; ++i)
            CHECK_EQUAL("x", table[i].text);

        table->verify();
    }
}


TEST(Shared_Notifications)
{
    // Create a new shared db
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    // No other instance have changed db since last transaction
    CHECK(!sg.has_changed());

    {
        // Open the same db again (in empty state)
        SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));

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
            wt.get_group().verify();
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
        rt.get_group().verify();
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
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    // Verify that contents is there when shared
    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
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
    SharedGroup db(path, false, SharedGroupOptions(crypt_key()));

    {
        Group& group = db.begin_write();
        TableRef table = group.add_table("users");
        table->add_column(type_String, "username");
        table->add_search_index(0);
        for (int i = 0; i < REALM_MAX_BPNODE_SIZE + 1; ++i)
            table->add_empty_row();
        for (int i = 0; i < REALM_MAX_BPNODE_SIZE + 1; ++i)
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
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        TableRef table = wt.add_table("a");
        table->add_column(type_String, "b");
        table->add_search_index(0); // Not adding index makes it work
        table->add_empty_row();
        wt.commit();
    }

    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
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
    SharedGroup db(path, false, SharedGroupOptions(crypt_key()));

    {
        Group& group = db.begin_write();
        TableRef table = group.add_table("users");
        table->add_column(type_String, "username");
        table->add_search_index(0); // Disabling index makes it work
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
                // cerr << "-" << del << ": " << table->get_string(0, del) << std::endl;
                table->remove(del);
                table->verify();
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
            // cerr << "+" << txt << std::endl;
            table->set_string(0, table->size() - 1, txt);
            table->verify();
            db.commit();
        }
    }
}


TEST(Shared_ClearColumnWithBasicArrayRootLeaf)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        WriteTransaction wt(sg);
        TableRef test = wt.add_table("Test");
        test->add_column(type_Double, "foo");
        test->clear();
        test->add_empty_row();
        test->set_double(0, 0, 727.2);
        wt.commit();
    }
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        ReadTransaction rt(sg);
        ConstTableRef test = rt.get_table("Test");
        CHECK_EQUAL(727.2, test->get_double(0, 0));
    }
}

// disable shared async on windows and any Apple operating system
// TODO: enable async daemon for OS X - think how to do it in XCode (no issue for build.sh)
#if !defined(_WIN32) && !REALM_PLATFORM_APPLE
// Todo. Keywords: winbug
TEST_IF(Shared_Async, allow_async)
{
    SHARED_GROUP_TEST_PATH(path);

    // Do some changes in a async db
    {
        bool no_create = false;
        SharedGroup db(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::Async));

        for (size_t i = 0; i < 100; ++i) {
            //            std::cout << "t "<<n<<"\n";
            WriteTransaction wt(db);
            wt.get_group().verify();
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
        rt.get_group().verify();
        TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
        CHECK_EQUAL(100, t1->size());
    }
}


namespace {

#define multiprocess_increments 100

void multiprocess_thread(TestContext& test_context, std::string path, size_t row_ndx)
{
    // Open shared db
    bool no_create = false;
    SharedGroup sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::Async));

    for (size_t i = 0; i != multiprocess_increments; ++i) {
        // Increment cell
        {

            WriteTransaction wt(sg);
            wt.get_group().verify();
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
            rt.get_group().verify();
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            int64_t v = t[row_ndx].first;
            int64_t expected = i + 1;
            CHECK_EQUAL(expected, v);
        }
    }
}


void multiprocess_make_table(std::string path, std::string lock_path, std::string alone_path, size_t rows)
{
    static_cast<void>(lock_path);
// Create first table in group
#if 1
    static_cast<void>(alone_path);
#if 0
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
#else
#if 0
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#else
    {
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::Async));
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_or_add_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#endif
#endif
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

void multiprocess_threaded(TestContext& test_context, std::string path, size_t num_threads, size_t base)
{
    // Do some changes in a async db
    std::unique_ptr<test_util::ThreadWrapper[]> threads;
    threads.reset(new test_util::ThreadWrapper[num_threads]);

    // Start threads
    for (size_t i = 0; i != num_threads; ++i) {
        threads[i].start([&test_context, &path, base, i] { multiprocess_thread(test_context, path, base + i); });
    }

    // Wait for threads to finish
    for (size_t i = 0; i != num_threads; ++i) {
        bool thread_has_thrown = false;
        std::string except_msg;
        if (threads[i].join(except_msg)) {
            std::cerr << "Exception thrown in thread " << i << ": " << except_msg << "\n";
            thread_has_thrown = true;
        }
        CHECK(!thread_has_thrown);
    }

    // Verify that the changes were made
    {
        bool no_create = false;
        SharedGroup sg(path, no_create, SharedGroupOptions(SharedGroupOptions::Durability::Async));
        ReadTransaction rt(sg);
        rt.get_group().verify();
        TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

        for (size_t i = 0; i != num_threads; ++i) {
            int64_t v = t[i + base].first;
            CHECK_EQUAL(multiprocess_increments, v);
        }
    }
}

void multiprocess_validate_and_clear(TestContext& test_context, std::string path, std::string lock_path, size_t rows,
                                     int result)
{
    // Wait for async_commit process to shutdown
    // FIXME: this is not apropriate
    static_cast<void>(lock_path);
    sleep(1);

    // Verify - once more, in sync mode - that the changes were made
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.get_group().verify();
        TestTableShared::Ref t = wt.get_table<TestTableShared>("test");

        for (size_t i = 0; i != rows; ++i) {
            int64_t v = t[i].first;
            t[i].first = 0;
            CHECK_EQUAL(result, v);
        }
        wt.commit();
    }
}

void multiprocess(TestContext& test_context, std::string path, int num_procs, size_t num_threads)
{
    int* pids = new int[num_procs];
    for (int i = 0; i != num_procs; ++i) {
        if (0 == (pids[i] = fork())) {
            multiprocess_threaded(test_context, path, num_threads, i * num_threads);
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
    usleep(100); // FIXME: Is this really acceptable?

#if TEST_DURATION < 1
    multiprocess_make_table(path, path.get_lock_path(), alone_path, 4);

    multiprocess_threaded(test_context, path, 2, 0);
    multiprocess_validate_and_clear(test_context, path, path.get_lock_path(), 2, multiprocess_increments);

    for (int k = 1; k < 3; ++k) {
        multiprocess(test_context, path, 2, 2);
        multiprocess_validate_and_clear(test_context, path, path.get_lock_path(), 4, multiprocess_increments);
    }
#else
    multiprocess_make_table(path, path.get_lock_path(), alone_path, 100);

    multiprocess_threaded(test_context, path, 10, 0);
    multiprocess_validate_and_clear(test_context, path, path.get_lock_path(), 10, multiprocess_increments);

    for (int k = 1; k < 10; ++k) {
        multiprocess(test_context, path, 10, 10);
        multiprocess_validate_and_clear(test_context, path, path.get_lock_path(), 100, multiprocess_increments);
    }
#endif
}

#endif // !defined(_WIN32) && !REALM_PLATFORM_APPLE

#if !defined(_WIN32)
// this test does not work with valgrind:
#if 0

// This test will hang infinitely instead of failing!!!
TEST(Shared_WaitForChange)
{
    const int num_threads = 3;
    Mutex mutex;
    int shared_state[num_threads];
    SharedGroup* sgs[num_threads];

    auto waiter = [&](std::string path, int i) {
        SharedGroup* sg = new SharedGroup(path, true);
        {
            LockGuard l(mutex);
            shared_state[i] = 1;
            sgs[i] = sg;
        }
        sg->begin_read(); // open a transaction at least once to make "changed" well defined
        sg->end_read();
        sg->wait_for_change();
        {
            LockGuard l(mutex);
            shared_state[i] = 2; // this state should not be observed by the writer
        }
        sg->wait_for_change(); // we'll fall right through here, because we haven't advanced our readlock
        {
            LockGuard l(mutex);
            shared_state[i] = 3;
        }
        sg->begin_read();
        sg->end_read();
        sg->wait_for_change(); // this time we'll wait because state hasn't advanced since we did.
        {
            LockGuard l(mutex);
            shared_state[i] = 4;
        }
        // works within a read transaction as well
        sg->begin_read();
        sg->wait_for_change();
        sg->end_read();
        {
            LockGuard l(mutex);
            shared_state[i] = 5;
        }
        sg->begin_read();
        sg->end_read();
        sg->wait_for_change(); // wait until wait_for_change is released
        {
            LockGuard l(mutex);
            shared_state[i] = 6;
        }
    };

    SHARED_GROUP_TEST_PATH(path);
    for (int j = 0; j < num_threads; j++)
        shared_state[j] = 0;
    SharedGroup sg(path, false);
    Thread threads[num_threads];
    for (int j = 0; j < num_threads; j++)
        threads[j].start([waiter, &path, j] { waiter(path, j); });
    bool try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (shared_state[j] < 1) try_again = true;
            CHECK(shared_state[j] < 2);
        }
    }

    // This write transaction should allow all readers to run again
    sg.begin_write();
    sg.commit();

    // All readers should pass through state 2 to state 3, so wait
    // for all to reach state 3:
    try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (3 != shared_state[j]) try_again = true;
            CHECK(shared_state[j] < 4);
        }
    }

    sg.begin_write();
    sg.commit();
    try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (4 != shared_state[j]) try_again = true;
        }
    }
    sg.begin_write();
    sg.commit();
    try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (5 != shared_state[j]) try_again = true;
        }
    }
    try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (sgs[j]) {
                sgs[j]->wait_for_change_release();
            }
            if (6 != shared_state[j]) {
                try_again = true;
            }
        }
    }
    for (int j = 0; j < num_threads; j++)
        threads[j].join();
    for (int j = 0; j < num_threads; j++) {
        delete sgs[j];
        sgs[j] = 0;
    }
}


#endif // test is disabled
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
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            CHECK(rt.has_table("x"));
            CHECK(!rt.has_table("gnyf"));
            CHECK(!rt.has_table("baz"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            wt.add_table("baz"); // Add table "baz"
            wt.commit();
        }
        {
            WriteTransaction wt2(sg2);
            wt2.get_group().verify();
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
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            CHECK(!rt.has_table("foo"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            wt.add_table("foo"); // Add table "foo"
            wt.commit();
        }
    }

    File::try_remove(path);
    {
        // Create non-empty file without free-space tracking
        Group g;
        g.verify();
        g.add_table("x");
        g.verify();
        g.write(path, crypt_key());
    }
    {
        // See if we can read and modify with shared group
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            CHECK(!rt.has_table("foo"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            wt.add_table("foo"); // Add table "foo"
            wt.get_group().verify();
            wt.commit();
        }
    }
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            CHECK(rt.has_table("foo"));
        }
    }
    {
        // Access using non-shared group
        Group g(path, crypt_key(), Group::mode_ReadWrite);
        g.verify();
        g.commit();
        g.verify();
    }
    {
        // Modify using non-shared group
        Group g(path, crypt_key(), Group::mode_ReadWrite);
        g.verify();
        g.add_table("bar"); // Add table "bar"
        g.verify();
        g.commit();
        g.verify();
    }
    {
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            CHECK(rt.has_table("bar"));
        }
    }

#if !REALM_ENABLE_ENCRYPTION // encrpted buffers aren't supported
    // The empty group created initially by a shared group accessor is special
    // in that it contains no nodes, and the root-ref is therefore zero. The
    // following block checks that the contents of such a file is still
    // perceived as valid when placed in a memory buffer, and then opened.
    File::try_remove(path);
    {
        {
            SharedGroup sg(path, false, SharedGroupOptions(crypt_key())); // Create the very empty group
        }
        std::ifstream in(path.c_str());
        std::string buffer((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        bool take_ownership = false;
        Group group(BinaryData(buffer), take_ownership);
        group.verify();
        CHECK(group.is_empty());
        group.add_table("x");
        group.verify();
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
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
    sg.begin_write();
    sg.rollback();
    sg.rollback();
}


TEST(Shared_MultipleEndReads)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
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
        SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
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
        if (crypt_key()) {
            // For encrypted files, reserve() may actually grow the file
            // with a page sized header.
            CHECK(orig_file_size <= new_file_size_2 && (orig_file_size + page_size()) >= new_file_size_2);
        }
        else {
            CHECK_EQUAL(orig_file_size, new_file_size_2);
        }

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
            wt.get_group().verify();
            wt.add_table<TestTableShared>("table_1")->add_empty_row(2000);
            wt.commit();
        }
        orig_file_size = size_t(File(path).get_size());
        size_t reserve_size_4 = 2 * orig_file_size + 1;
        sg.reserve(reserve_size_4);
        size_t new_file_size_4 = size_t(File(path).get_size());
        CHECK(new_file_size_4 >= reserve_size_4);
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            wt.add_table<TestTableShared>("table_2")->add_empty_row(2000);
            orig_file_size = size_t(File(path).get_size());
            size_t reserve_size_5 = orig_file_size + 333;
            sg.reserve(reserve_size_5);
            size_t new_file_size_5 = size_t(File(path).get_size());
            CHECK(new_file_size_5 >= reserve_size_5);
            wt.add_table<TestTableShared>("table_3")->add_empty_row(2000);
            wt.commit();
        }
        orig_file_size = size_t(File(path).get_size());
        size_t reserve_size_6 = orig_file_size + 459;
        sg.reserve(reserve_size_6);
        size_t new_file_size_6 = size_t(File(path).get_size());
        CHECK(new_file_size_6 >= reserve_size_6);
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            wt.commit();
        }
    }
}


TEST(Shared_MovingEnumStringColumn)
{
    // Test that the 'index in parent' property of the column of unique strings
    // in a StringEnumColumn is properly adjusted when other string enumeration
    // columns are inserted or removed before it. Note that the parent of the
    // column of unique strings in a StringEnumColumn is a child of an array
    // node in the Spec class.

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

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
            table->set_string(0, i, i % 2 == 0 ? "a" : "b");
        table->optimize();
        wt.get_group().verify();
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(1, table->get_descriptor()->get_num_unique_values(1));
        table->set_string(1, 0, "bar0");
        table->set_string(1, 1, "bar1");
        wt.get_group().verify();
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        wt.commit();
    }
    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        ConstTableRef table = rt.get_table("foo");
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        for (int i = 0; i < 64; ++i) {
            std::string value = table->get_string(0, i);
            if (i % 2 == 0) {
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
        wt.get_group().verify();
        TableRef table = wt.get_table("foo");
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        table->remove_column(0);
        wt.get_group().verify();
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(0));
        table->set_string(0, 2, "bar2");
        wt.get_group().verify();
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(0));
        wt.commit();
    }
    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        ConstTableRef table = rt.get_table("foo");
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(0));
        for (int i = 0; i < 64; ++i) {
            std::string value = table->get_string(0, i);
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
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));

    // Create a regular string column and an enumeration strings column, and
    // equip both with search indexes.
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("foo");
        table->add_column(type_String, "regular");
        table->add_column(type_String, "enum");
        table->add_empty_row(64);
        for (int i = 0; i < 64; ++i) {
            std::string out(std::string("foo") + util::to_string(i));
            table->set_string(0, i, out);
            table->set_string(1, i, "bar");
        }
        table->set_string(1, 63, "bar63");
        table->optimize();
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(1));
        table->add_search_index(0);
        table->add_search_index(1);
        wt.get_group().verify();
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
        wt.get_group().verify();
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(2, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(62, table->find_first_string(1, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(2, "bar63"));
        table->set_string(1, 0, "foo_X");
        table->set_string(2, 0, "bar_X");
        wt.get_group().verify();
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(realm::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(realm::not_found, table->find_first_string(2, "bad"));
        CHECK_EQUAL(0, table->find_first_string(1, "foo_X"));
        CHECK_EQUAL(31, table->find_first_string(1, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(1, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(1, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "foo63"));
        CHECK_EQUAL(0, table->find_first_string(2, "bar_X"));
        CHECK_EQUAL(1, table->find_first_string(2, "bar"));
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
        CHECK_EQUAL(realm::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(realm::not_found, table->find_first_string(2, "bad"));
        CHECK_EQUAL(0, table->find_first_string(1, "foo_X"));
        CHECK_EQUAL(31, table->find_first_string(1, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(1, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(1, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "foo63"));
        CHECK_EQUAL(0, table->find_first_string(2, "bar_X"));
        CHECK_EQUAL(1, table->find_first_string(2, "bar"));
        CHECK_EQUAL(63, table->find_first_string(2, "bar63"));
        table->remove_column(0);
        wt.get_group().verify();
        CHECK(table->has_search_index(0) && table->has_search_index(1));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(3, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(realm::not_found, table->find_first_string(0, "bad"));
        CHECK_EQUAL(realm::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(0, table->find_first_string(0, "foo_X"));
        CHECK_EQUAL(31, table->find_first_string(0, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(0, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(0, "foo63"));
        CHECK_EQUAL(0, table->find_first_string(1, "bar_X"));
        CHECK_EQUAL(1, table->find_first_string(1, "bar"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        table->set_string(0, 1, "foo_Y");
        table->set_string(1, 1, "bar_Y");
        wt.get_group().verify();
        CHECK(table->has_search_index(0) && table->has_search_index(1));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(realm::not_found, table->find_first_string(0, "bad"));
        CHECK_EQUAL(realm::not_found, table->find_first_string(1, "bad"));
        CHECK_EQUAL(0, table->find_first_string(0, "foo_X"));
        CHECK_EQUAL(1, table->find_first_string(0, "foo_Y"));
        CHECK_EQUAL(31, table->find_first_string(0, "foo31"));
        CHECK_EQUAL(61, table->find_first_string(0, "foo61"));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(0, "foo63"));
        CHECK_EQUAL(0, table->find_first_string(1, "bar_X"));
        CHECK_EQUAL(1, table->find_first_string(1, "bar_Y"));
        CHECK_EQUAL(2, table->find_first_string(1, "bar"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        wt.commit();
    }
    // Insert a column after the string columns and remove the indexes
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("foo");
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));

        table->insert_column(2, type_Int, "i");
        for (size_t i = 0; i < table->size(); ++i)
            table->set_int(2, i, i);
        wt.get_group().verify();
        table->remove_search_index(0);
        wt.get_group().verify();
        table->remove_search_index(1);
        wt.get_group().verify();

        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        CHECK_EQUAL(60, table->find_first_int(2, 60));
        wt.commit();
    }
    // add and remove the indexes in reverse order
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("foo");

        wt.get_group().verify();
        table->add_search_index(1);
        wt.get_group().verify();
        table->add_search_index(0);
        wt.get_group().verify();

        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        CHECK_EQUAL(60, table->find_first_int(2, 60));

        wt.get_group().verify();
        table->remove_search_index(1);
        wt.get_group().verify();
        table->remove_search_index(0);
        wt.get_group().verify();

        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(0));
        CHECK_EQUAL(4, table->get_descriptor()->get_num_unique_values(1));
        CHECK_EQUAL(0, table->get_descriptor()->get_num_unique_values(2));
        CHECK_EQUAL(62, table->find_first_string(0, "foo62"));
        CHECK_EQUAL(63, table->find_first_string(1, "bar63"));
        CHECK_EQUAL(60, table->find_first_int(2, 60));
        wt.commit();
    }
}


TEST_IF(Shared_ArrayEraseBug, TEST_DURATION >= 1)
{
    // This test only makes sense when we can insert a number of rows
    // equal to the square of the maximum B+-tree node size.
    size_t max_node_size = REALM_MAX_BPNODE_SIZE;
    size_t max_node_size_squared = max_node_size;
    if (int_multiply_with_overflow_detect(max_node_size_squared, max_node_size))
        return;

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
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
        size_t row_ndx = max_node_size_squared - max_node_size - max_node_size / 2;
        table->insert_empty_row(row_ndx);
        wt.commit();
    }
}


TEST_IF(Shared_BeginReadFailure, _impl::SimulatedFailure::is_enabled())
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path);
    using sf = _impl::SimulatedFailure;
    sf::OneShotPrimeGuard pg(sf::shared_group__grow_reader_mapping);
    CHECK_THROW(sg.begin_read(), sf);
}


TEST(Shared_SessionDurabilityConsistency)
{
    // Check that we can reliably detect inconsist durability choices across
    // concurrent session participants.

    // Errors of this kind are considered as incorrect API usage, and will lead
    // to throwing of LogicError exceptions.

    SHARED_GROUP_TEST_PATH(path);
    {
        bool no_create = false;
        SharedGroupOptions::Durability durability_1 = SharedGroupOptions::Durability::Full;
        SharedGroup sg(path, no_create, SharedGroupOptions(durability_1));

        SharedGroupOptions::Durability durability_2 = SharedGroupOptions::Durability::MemOnly;
        CHECK_LOGIC_ERROR(SharedGroup(path, no_create, SharedGroupOptions(durability_2)),
                          LogicError::mixed_durability);
    }
}


TEST(Shared_WriteEmpty)
{
    SHARED_GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);
    {
        SharedGroup sg(path_1);
        ReadTransaction rt(sg);
        rt.get_group().write(path_2);
    }
}


TEST(Shared_CompactEmpty)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        SharedGroup sg(path);
        CHECK(sg.compact());
    }
}


TEST(Shared_VersionOfBoundSnapshot)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup::version_type version;
    SharedGroup sg(path);
    {
        ReadTransaction rt(sg);
        version = rt.get_version();
    }
    {
        ReadTransaction rt(sg);
        CHECK_EQUAL(version, rt.get_version());
    }
    {
        WriteTransaction wt(sg);
        CHECK_EQUAL(version, wt.get_version());
    }
    {
        WriteTransaction wt(sg);
        CHECK_EQUAL(version, wt.get_version());
        wt.commit(); // Increment version
    }
    {
        ReadTransaction rt(sg);
        CHECK_LESS(version, rt.get_version());
        version = rt.get_version();
    }
    {
        WriteTransaction wt(sg);
        CHECK_EQUAL(version, wt.get_version());
        wt.commit(); // Increment version
    }
    {
        ReadTransaction rt(sg);
        CHECK_LESS(version, rt.get_version());
    }
}


// This test is valid, but because it requests all available memory,
// it does not play nicely with valgrind and so is disabled.
/*
#if !defined(_WIN32)
// Check what happens when Realm cannot allocate more virtual memory
// We should throw an AddressSpaceExhausted exception.
// This will try to use all available memory allowed for this process
// so don't run it concurrently with other tests.
NONCONCURRENT_TEST(Shared_OutOfMemory)
{
    size_t string_length = 1024 * 1024;
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        table->add_column(type_String, "string_col");
        std::string long_string(string_length, 'a');
        table->add_empty_row();
        table->set_string(0, 0, long_string);
        wt.commit();
    }
    sg.close();

    std::vector<std::pair<void*, size_t>> memory_list;
    // Reserve enough for 5*100000 Gb, but in practice the vector is only ever around size 10.
    // Do this here to avoid the (small) chance that adding to the vector will request new virtual memory
    memory_list.reserve(500);
    size_t chunk_size = size_t(1024) * 1024 * 1024 * 100000;
    while (chunk_size > string_length) {
        void* addr = ::mmap(nullptr, chunk_size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
        if (addr == MAP_FAILED) {
            chunk_size /= 2;
        }
        else {
            memory_list.push_back(std::pair<void*, size_t>(addr, chunk_size));
        }
    }

    bool expected_exception_caught = false;
    // Attempt to open Realm, should fail because we hold too much already.
    try {
        SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));
    }
    catch (AddressSpaceExhausted& e) {
        expected_exception_caught = true;
    }
    CHECK(expected_exception_caught);

    // Release memory manually.
    for (auto it = memory_list.begin(); it != memory_list.end(); ++it) {
        ::munmap(it->first, it->second);
    }

    // Realm should succeed to open now.
    expected_exception_caught = false;
    try {
        SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));
    }
    catch (AddressSpaceExhausted& e) {
        expected_exception_caught = true;
    }
    CHECK(!expected_exception_caught);
}
#endif // !win32
*/

// Run some (repeatable) random checks through the fuzz tester.
// For a comprehensive fuzz test, afl should be run. To do this see test/fuzzy/README.md
// If this check fails for some reason, you can find the problem by changing
// the parse_and_apply_instructions call to use std::cerr which will print out
// the instructions used to duplicate the failure.
TEST(Shared_StaticFuzzTestRunSanityCheck)
{
    // Either provide a crash file generated by AFL to reproduce a crash, or leave it blank in order to run
    // a very simple fuzz test that just uses a random generator for generating Realm actions.
    std::string filename = "";
    // std::string filename = "/findings/hangs/id:000041,src:000000,op:havoc,rep:64";
    // std::string filename = "d:/crash3";

    if (filename != "") {
        const char* tmp[] = {"", filename.c_str(), "--log"};
        run_fuzzy(sizeof(tmp) / sizeof(tmp[0]), tmp);
    }
    else {
        // Number of fuzzy tests
        const size_t iterations = 100;

        // Number of instructions in each test
        // Changing this strongly affects the test suite run time
        const size_t instructions = 200;

        for (size_t counter = 0; counter < iterations; counter++) {
            // You can use your own seed if you have observed a crashing unit test that
            // printed out some specific seed (the "Unit test random seed:" part that appears).
            // fastrand(534653645, true);
            fastrand(unit_test_random_seed + counter, true);

            std::string instr;

            // "fastlog" is because logging to a stream is very very slow. Logging the sequence of
            // bytes lets you perform many more tests per second.
            std::string fastlog = "char[] instr2 = {";

            for (size_t t = 0; t < instructions; t++) {
                char c = static_cast<char>(fastrand());
                instr += c;
                std::string tmp;
                unit_test::to_string(static_cast<int>(c), tmp);
                fastlog += tmp;
                if (t + 1 < instructions) {
                    fastlog += ", ";
                }
                else {
                    fastlog += "}; instr = string(instr2);";
                }
            }
            // Scope guard of "path" is inside the loop to clean up files per iteration
            SHARED_GROUP_TEST_PATH(path);
            // If using std::cerr, you can copy/paste the console output into a unit test
            // to get a reproduction test case
            // parse_and_apply_instructions(instr, path, std::cerr);
            parse_and_apply_instructions(instr, path, util::none);
        }
    }
}

// Scaled down stress test. (Use string length ~15MB for max stress)
NONCONCURRENT_TEST(Shared_BigAllocations)
{
    size_t string_length = 64 * 1024;
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
    std::string long_string(string_length, 'a');
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        table->add_column(type_String, "string_col");
        wt.commit();
    }
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        for (int i = 0; i < 32; ++i) {
            table->add_empty_row();
            table->set_string(0, i, long_string);
        }
        wt.commit();
    }
    for (int k = 0; k < 10; ++k) {
        // sg.compact(); // <--- enable this if you want to stress with compact()
        for (int j = 0; j < 20; ++j) {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            for (int i = 0; i < 20; ++i) {
                table->set_string(0, i, long_string);
            }
            wt.commit();
        }
    }
    sg.close();
}

// Repro case for: Assertion failed: top_size == 3 || top_size == 5 || top_size == 7 [0, 3, 0, 5, 0, 7]
NONCONCURRENT_TEST(Shared_BigAllocationsMinimized)
{
    // String length at 2K will not trigger the error.
    // all lengths >= 4K (that were tried) trigger the error
    size_t string_length = 4 * 1024;
    SHARED_GROUP_TEST_PATH(path);
    std::string long_string(string_length, 'a');
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
    {
        {
            WriteTransaction wt(sg);
            TableRef table = wt.add_table("table");
            table->add_column(type_String, "string_col");
            table->add_empty_row();
            table->set_string(0, 0, long_string);
            wt.commit();
        }
        sg.compact(); // <- required to provoke subsequent failures
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TableRef table = wt.get_table("table");
            table->set_string(0, 0, long_string);
            wt.get_group().verify();
            wt.commit();
        }
    }
    {
        WriteTransaction wt(sg); // <---- fails here
        wt.get_group().verify();
        TableRef table = wt.get_table("table");
        table->set_string(0, 0, long_string);
        wt.get_group().verify();
        wt.commit();
    }
    sg.close();
}

// Found by AFL (on a heavy hint from Finn that we should add a compact() instruction
NONCONCURRENT_TEST(Shared_TopSizeNotEqualNine)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path, false, SharedGroupOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg.begin_write());

    TableRef t = g.add_table("");
    t->add_column(type_Double, "");
    t->add_empty_row(241);
    sg.commit();
    REALM_ASSERT_RELEASE(sg.compact());
    SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));
    sg2.begin_write();
    sg2.commit();
    sg2.begin_read(); // <- does not fail
    SharedGroup sg3(path, false, SharedGroupOptions(crypt_key()));
    sg3.begin_read(); // <- does not fail
    sg.begin_read();  // <- does fail
}

// Found by AFL after adding the compact instruction
// after further manual simplification, this test no longer triggers
// the double free, but crashes in a different way
TEST(Shared_Bptree_insert_failure)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg_w(path, false, SharedGroupOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_write());

    g.add_table("");
    g.get_table(0)->add_column(type_Double, "dgrpn", true);
    g.get_table(0)->add_empty_row(246);
    sg_w.commit();
    REALM_ASSERT_RELEASE(sg_w.compact());
#if 0
    {
        // This intervening sg can do the same operation as the one doing compact,
        // but without failing:
        SharedGroup sg2(path, false, SharedGroupOptions(crypt_key()));
        Group& g2 = const_cast<Group&>(sg2.begin_write());
        g2.get_table(0)->add_empty_row(396);
    }
#endif
    sg_w.begin_write();
    g.get_table(0)->add_empty_row(396);
}

#endif // TEST_SHARED
