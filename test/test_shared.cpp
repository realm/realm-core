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

#include <condition_variable>
#include <streambuf>
#include <fstream>
#include <tuple>
#include <iostream>
#include <fstream>
#include <thread>

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
#include <windows.h>
#endif

#include <realm/history.hpp>
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
#include "test_table_helper.hpp"

extern unsigned int unit_test_random_seed;

using namespace std;
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

#if 0
// Sorting benchmark
ONLY(Query_QuickSort2)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

                                                // Triggers QuickSort because range > len
    Table ttt;
    auto ints = ttt.add_column(type_Int, "1");
    auto strings = ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 10000; t++) {
        Obj o = ttt.create_object();
        //        o.set<int64_t>(ints, random.draw_int_mod(1100));
        o.set<StringData>(strings, "a");
    }

    Query q = ttt.where();

    std::cerr << "GO";

    for (size_t t = 0; t < 1000; t++) {
        TableView tv = q.find_all();
        tv.sort(strings);
        //        tv.ints(strings);
    }
}
#endif

#if REALM_WINDOWS
namespace {
// NOTE: This does not work like on POSIX: The child will begin execution from
// the unit test entry point, not from where fork() took place.
//
DWORD winfork(std::string unit_test_name)
{
    if (getenv("REALM_FORKED"))
        return GetCurrentProcessId();

    char filename[MAX_PATH];
    DWORD success = GetModuleFileNameA(nullptr, filename, MAX_PATH);
    if (success == 0 || success == MAX_PATH) {
        DWORD err = GetLastError();
        REALM_ASSERT_EX(false, err, MAX_PATH, filename);
    }

    GetModuleFileNameA(nullptr, filename, MAX_PATH);

    StringBuffer environment;
    environment.append("REALM_FORKED=1");
    environment.append("\0", 1);
    environment.append("UNITTEST_FILTER=" + unit_test_name);
    environment.append("\0\0", 2);

    PROCESS_INFORMATION process;
    ZeroMemory(&process, sizeof(process));
    STARTUPINFO info;
    ZeroMemory(&info, sizeof(info));
    info.cb = sizeof(info);

    BOOL b = CreateProcessA(filename, nullptr, 0, 0, false, 0, environment.data(), nullptr, &info, &process);
    REALM_ASSERT_RELEASE(b);

    CloseHandle(process.hProcess);
    CloseHandle(process.hThread);
    return process.dwProcessId;
}
}
#endif


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


namespace {

std::vector<ColKey> test_table_add_columns(TableRef t)
{
    std::vector<ColKey> res;
    res.push_back(t->add_column(type_Int, "first"));
    res.push_back(t->add_column(type_Int, "second"));
    res.push_back(t->add_column(type_Bool, "third"));
    res.push_back(t->add_column(type_String, "fourth"));
    res.push_back(t->add_column(type_Timestamp, "fifth"));
    return res;
}
}

void writer(DBRef sg, uint64_t id)
{
    // std::cerr << "Started writer " << std::endl;
    try {
        auto tr = sg->start_read();
        bool done = false;
        // std::cerr << "Opened sg " << std::endl;
        for (int i = 0; !done; ++i) {
            // std::cerr << "       - " << getpid() << std::endl;
            tr->promote_to_write();
            auto t1 = tr->get_table("test");
            ColKeys _cols = t1->get_column_keys();
            std::vector<ColKey> cols;
            for (auto e : _cols) cols.push_back(e);
            Obj obj = t1->get_object(ObjKey(id));
            done = obj.get<Bool>(cols[2]);
            if (i & 1) {
                obj.add_int(cols[0], 1);
            }
            std::this_thread::yield(); // increase chance of signal arriving in the middle of a transaction
            tr->commit_and_continue_as_read();
        }
        // std::cerr << "Ended pid " << getpid() << std::endl;
        tr->end_read();
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
        DBRef sg = DB::create(path, true, DBOptions(crypt_key()));
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
            auto t1 = rt.get_table("test");
            auto cols = t1->get_column_keys();
            auto obj = t1->get_object(ObjKey(id));
            done = 10 < obj.get<Int>(cols[0]);
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
        DBRef sg = DB::create(path, true);
        ReadTransaction rt(sg);
        rt.get_group().verify();
        auto t1 = rt.get_table("test");
        auto cols = t1->get_column_keys();
        auto obj = t1->get_object(ObjKey(id));
        CHECK(10 < obj.get<Int>(cols[0]));
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
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
    {
        // Create table entries
        WriteTransaction wt(sg);
        auto t1 = wt.add_table("test");
        test_table_add_columns(t1);
        for (int i = 0; i < num_processes; ++i) {
            t1->create_object().set_all(0, i, false, "test");
        }
        wt.commit();
    }
    int pid = fork();
    if (pid == -1)
        REALM_TERMINATE("fork() failed");
    if (pid == 0) {
        // first writer!
        writer(sg, 0);
        _Exit(0);
    }
    else {
        for (int k = 1; k < num_processes; ++k) {
            int pid2 = pid;
            pid = fork();
            if (pid == pid_t(-1))
                REALM_TERMINATE("fork() failed");
            if (pid == 0) {
                writer(sg, k);
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
    millisleep(1000);
}

#endif

#if 0

// This unit test will test the case where the .realm file exceeds the available disk space. To run it, do
// following:
//
// 1: Create a drive that has around 10 MB free disk space *after* the realm-tests binary has been copied to it
// (you can fill up the drive with random data files until you hit 10 MB).
//
// Repeatedly run the realm-tests binary in a loop, like from a bash script. You can even make the bash script
// invoke `pkill realm-tests` with some intervals to test robustness too (if so, start the unit tests with `&`,
// i.e. `realm-tests&` so it runs in the background.

ONLY(Shared_DiskSpace)
{
    for (;;) {
        if (!File::exists("x")) {
            File f("x", realm::util::File::mode_Write);
            f.write(std::string(18 * 1024 * 1024, 'x'));
            f.close();
        }

        std::string path = "test.realm";

        SharedGroup sg(path, false, DBOptions("1234567890123456789012345678901123456789012345678901234567890123"));
        //    SharedGroup sg(path, false, SharedGroupOptions(nullptr));

        int seed = time(0);
        fastrand(seed, true);

        int foo = fastrand(100);
        if (foo > 50) {
            const Group& g = sg.begin_read();
            g.verify();
            continue;
        }

        int action = fastrand(100);

        WriteTransaction wt(sg);
        auto t1 = wt.get_or_add_table("test");

        t1->verify();

        if (t1->size() == 0) {
            t1->add_column(type_String, "name");
        }

        std::string str(fastrand(3000), 'a');

        size_t rows = fastrand(3000);

        for (int64_t i = 0; i < rows; ++i) {
            if (action < 55) {
                t1->add_empty_row();
                t1->set_string(0, t1->size() - 1, str.c_str());
            }
            else {
                if (t1->size() > 0) {
                    t1->remove(0);
                }
            }
        }

        if (fastrand(100) < 5) {
            File::try_remove("y");
            t1->clear();
            File::copy("x", "y");
        }

        if (fastrand(100) < 90) {
            wt.commit();
        }

        if (fastrand(100) < 5) {
            // Sometimes a special situation occurs where we cannot commit a t1-clear() due to low disk space, and where
            // compact also won't work because it has no space to write the new compacted file. The only way out of this
            // is to temporarely free up some disk space
            File::try_remove("y");
            sg.compact();
            File::copy("x", "y");
        }

    }
}

#endif // Only disables above special unit test


TEST(Shared_CompactingOnTheFly)
{
    SHARED_GROUP_TEST_PATH(path);
    Thread writer_thread;
    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        // Create table entries
        std::vector<ColKey> cols; // unsafe to hold colkeys across transaction! FIXME: should be reported
        {
            WriteTransaction wt(sg);
            auto t1 = wt.add_table("test");
            test_table_add_columns(t1);
            ColKeys _cols = t1->get_column_keys();
            for (auto e : _cols) cols.push_back(e);
            for (int i = 0; i < 100; ++i) {
                t1->create_object(ObjKey(i)).set_all(0, i, false, "test");
            }
            wt.commit();
        }
        {
            writer_thread.start(std::bind(&writer, sg, 41));

            // make sure writer has started:
            bool waiting = true;
            while (waiting) {
                std::this_thread::yield();
                ReadTransaction rt(sg);
                auto t1 = rt.get_table("test");
                ConstObj obj = t1->get_object(ObjKey(41));
                waiting = obj.get<Int>(cols[0]) == 0;
                // std::cerr << t1->get_int(0, 41) << std::endl;
            }

            // since the writer is running, we cannot compact:
            CHECK(sg->compact() == false);
        }
        {
            // make the writer thread terminate:
            WriteTransaction wt(sg);
            auto t1 = wt.get_table("test");
            t1->get_object(ObjKey(41)).set(cols[2], true);
            wt.commit();
        }
        // we must join before the DB object goes out of scope
        writer_thread.join();
    }
    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg2 = DB::create(*hist, DBOptions(crypt_key()));
        {
            WriteTransaction wt(sg2);
            wt.commit();
        }
        CHECK_EQUAL(true, sg2->compact());

        ReadTransaction rt2(sg2);
        auto table = rt2.get_table("test");
        CHECK(table);
        CHECK_EQUAL(table->size(), 100);
        rt2.get_group().verify();
    }
    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg2 = DB::create(*hist, DBOptions(crypt_key()));
        ReadTransaction rt2(sg2);
        auto table = rt2.get_table("test");
        CHECK(table);
        CHECK_EQUAL(table->size(), 100);
        rt2.get_group().verify();
    }
}


TEST(Shared_EncryptedRemap)
{
    // Attempts to trigger code coverage in util::mremap() for the case where the file is encrypted.
    // This requires a "non-encrypted database size" (not physical file sise) which is non-divisible
    // by page_size() *and* is bigger than current allocated section. Following row count and payload
    // seems to work on both Windows+Linux
    const int64_t rows = 12;
    SHARED_GROUP_TEST_PATH(path);
    {
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        // Create table entries

        WriteTransaction wt(sg);
        auto t1 = wt.add_table("test");
        test_table_add_columns(t1);
        std::string str(100000, 'a');
        for (int64_t i = 0; i < rows; ++i) {
            t1->create_object().set_all(0, i, false, str.c_str());
        }
        wt.commit();
    }

    DBRef sg2 = DB::create(path, true, DBOptions(crypt_key()));

    CHECK_EQUAL(true, sg2->compact());
    ReadTransaction rt2(sg2);
    auto table = rt2.get_table("test");
    CHECK(table);
    CHECK_EQUAL(table->size(), rows);
    rt2.get_group().verify();
}


TEST(Shared_Initial)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

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
        DBRef sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));

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
        DBRef r = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));
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
        DBRef sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));
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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

        {
            // Open the same db again (in empty state)
            DBRef sg2 = DB::create(path, false, DBOptions(crypt_key()));

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                wt.get_group().verify();
                auto t1 = wt.add_table("test");
                test_table_add_columns(t1);
                t1->create_object(ObjKey(7)).set_all(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            auto t1 = rt.get_table("test");
            auto cols = t1->get_column_keys();
            CHECK_EQUAL(1, t1->size());
            ConstObj obj = t1->get_object(ObjKey(7));
            CHECK_EQUAL(1, obj.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj.get<String>(cols[3]));
        }
    }
}


TEST(Shared_Initial2_Mem)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        bool no_create = false;
        DBRef sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));

        {
            // Open the same db again (in empty state)
            DBRef sg2 = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                wt.get_group().verify();
                auto t1 = wt.add_table("test");
                test_table_add_columns(t1);
                t1->create_object(ObjKey(7)).set_all(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            auto t1 = rt.get_table("test");
            auto cols = t1->get_column_keys();
            CHECK_EQUAL(1, t1->size());
            ConstObj obj = t1->get_object(ObjKey(7));
            CHECK_EQUAL(1, obj.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj.get<String>(cols[3]));
        }
    }
}

TEST(Shared_1)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        Timestamp first_timestamp_value{1, 1};
        std::vector<ColKey> cols;

        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.add_table("test");
            cols = test_table_add_columns(t1);
            t1->create_object(ObjKey(7)).set_all(1, 2, false, "test", Timestamp{1, 1});
            wt.commit();
        }
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();

            // Verify that last set of changes are commited
            auto t2 = rt.get_table("test");
            CHECK(t2->size() == 1);
            ConstObj obj = t2->get_object(ObjKey(7));
            CHECK_EQUAL(1, obj.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj.get<String>(cols[3]));
            CHECK_EQUAL(first_timestamp_value, obj.get<Timestamp>(cols[4]));

            // Do a new change while still having current read transaction open
            {
                WriteTransaction wt(sg);
                wt.get_group().verify();
                auto t1 = wt.get_table("test");
                t1->create_object(ObjKey(8)).set_all(2, 3, true, "more test", Timestamp{2, 2});
                wt.commit();
            }

            // Verify that that the read transaction does not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, obj.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj.get<String>(cols[3]));
            CHECK_EQUAL(first_timestamp_value, obj.get<Timestamp>(cols[4]));
            // Do one more new change while still having current read transaction open
            // so we know that it does not overwrite data held by
            {
                WriteTransaction wt(sg);
                wt.get_group().verify();
                auto t1 = wt.get_table("test");
                t1->create_object(ObjKey(9)).set_all(0, 1, false, "even more test", Timestamp{3, 3});
                wt.commit();
            }

            // Verify that that the read transaction does still not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, obj.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj.get<String>(cols[3]));
            CHECK_EQUAL(first_timestamp_value, obj.get<Timestamp>(cols[4]));
        }

        // Start a new read transaction and verify that it can now see the changes
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            auto t3 = rt.get_table("test");

            CHECK(t3->size() == 3);
            ConstObj obj7 = t3->get_object(ObjKey(7));
            CHECK_EQUAL(1, obj7.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj7.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj7.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj7.get<String>(cols[3]));
            CHECK_EQUAL(first_timestamp_value, obj7.get<Timestamp>(cols[4]));

            ConstObj obj8 = t3->get_object(ObjKey(8));
            CHECK_EQUAL(2, obj8.get<Int>(cols[0]));
            CHECK_EQUAL(3, obj8.get<Int>(cols[1]));
            CHECK_EQUAL(true, obj8.get<Bool>(cols[2]));
            CHECK_EQUAL("more test", obj8.get<String>(cols[3]));
            Timestamp second_timestamp_value{2, 2};
            CHECK_EQUAL(second_timestamp_value, obj8.get<Timestamp>(cols[4]));

            ConstObj obj9 = t3->get_object(ObjKey(9));
            CHECK_EQUAL(0, obj9.get<Int>(cols[0]));
            CHECK_EQUAL(1, obj9.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj9.get<Bool>(cols[2]));
            CHECK_EQUAL("even more test", obj9.get<String>(cols[3]));
            Timestamp third_timestamp_value{3, 3};
            CHECK_EQUAL(third_timestamp_value, obj9.get<Timestamp>(cols[4]));
        }
    }
}

TEST(Shared_try_begin_write)
{
    SHARED_GROUP_TEST_PATH(path);
    // Create a new shared db
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
    std::mutex thread_obtains_write_lock;
    std::condition_variable cv;
    std::mutex cv_lock;
    bool init_complete = false;

    auto do_async = [&]() {

        auto tr = sg->start_write(true);
        bool success = bool(tr);
        CHECK(success);
        {
            std::lock_guard<std::mutex> lock(cv_lock);
            init_complete = true;
        }
        cv.notify_one();
        TableRef t = tr->add_table(StringData("table"));
        t->add_column(type_String, StringData("string_col"));
        std::vector<ObjKey> keys;
        t->create_objects(1000, keys);
        thread_obtains_write_lock.lock();
        tr->commit();
        thread_obtains_write_lock.unlock();
    };

    thread_obtains_write_lock.lock();
    Thread async_writer;
    async_writer.start(do_async);

    // wait for the thread to start a write transaction
    std::unique_lock<std::mutex> lock(cv_lock);
    cv.wait(lock, [&]{ return init_complete; });

    // Try to also obtain a write lock. This should fail but not block.
    auto tr = sg->start_write(true);
    bool success = bool(tr);
    CHECK(!success);

    // Let the async thread finish its write transaction.
    thread_obtains_write_lock.unlock();
    async_writer.join();

    {
        // Verify that the thread transaction commit succeeded.
        auto rt = sg->start_read();
        ConstTableRef t = rt->get_table(rt->get_table_keys()[0]);
        CHECK(t->get_name() == StringData("table"));
        CHECK(t->get_column_name(t->get_column_keys()[0]) == StringData("string_col"));
        CHECK(t->size() == 1000);
        CHECK(rt->size() == 1);
    }

    // Now try to start a transaction without any contenders.
    tr = sg->start_write(true);
    success = bool(tr);
    CHECK(success);
    CHECK(tr->size() == 1);
    tr->verify();

    // Add some data and finish the transaction.
    auto t2k = tr->add_table(StringData("table 2"))->get_key();
    CHECK(tr->size() == 2);
    tr->commit();

    {
        // Verify that the main thread transaction now succeeded.
        ReadTransaction rt(sg);
        const Group& gr = rt.get_group();
        CHECK(gr.size() == 2);
        CHECK(gr.get_table(t2k)->get_name() == StringData("table 2"));
    }
}

TEST(Shared_Rollback)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        std::vector<ColKey> cols;

        // Create first table in group (but rollback)
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.add_table("test");
            cols = test_table_add_columns(t1);
            t1->create_object().set_all(1, 2, false, "test");
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
            auto t1 = wt.add_table("test");
            cols = test_table_add_columns(t1);
            t1->create_object(ObjKey(7)).set_all(1, 2, false, "test");
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            auto t = rt.get_table("test");
            CHECK(t->size() == 1);
            ConstObj obj = t->get_object(ObjKey(7));
            CHECK_EQUAL(1, obj.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj.get<String>(cols[3]));
        }

        // Greate more changes (but rollback)
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.get_table("test");
            t1->create_object(ObjKey(8)).set_all(0, 0, true, "more test");
            // Note: Implicit rollback
        }

        // Verify that no changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            auto t = rt.get_table("test");
            CHECK(t->size() == 1);
            ConstObj obj = t->get_object(ObjKey(7));
            CHECK_EQUAL(1, obj.get<Int>(cols[0]));
            CHECK_EQUAL(2, obj.get<Int>(cols[1]));
            CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
            CHECK_EQUAL("test", obj.get<String>(cols[3]));
        }
    }
}

TEST(Shared_Writes)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        std::vector<ColKey> cols;

        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.add_table("test");
            cols = test_table_add_columns(t1);
            t1->create_object(ObjKey(7)).set_all(0, 2, false, "test");
            wt.commit();
        }

        // Do a lot of repeated write transactions
        for (size_t i = 0; i < 100; ++i) {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.get_table("test");
            t1->get_object(ObjKey(7)).add_int(cols[0], 1);
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            auto t = rt.get_table("test");
            const int64_t v = t->get_object(ObjKey(7)).get<Int>(cols[0]);
            CHECK_EQUAL(100, v);
        }
    }
}

namespace {

void add_int(Table& table, ColKey col, int64_t diff)
{
    for (auto& o : table) {
        o.add_int(col, diff);
    }
}

} // anonymous namespace

#if !REALM_ANDROID // FIXME
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
    DBRef shared_groups[8 * max_N];
    TransactionRef read_transactions[8 * max_N];
    ColKey col_int;
    ColKey col_bin;

    for (int round = 0; round < num_rounds; ++round) {
        int N = rounds[round];

        SHARED_GROUP_TEST_PATH(path);

        bool no_create = false;
        auto root_sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));

        // Add two tables
        {
            WriteTransaction wt(root_sg);
            wt.get_group().verify();
            bool was_added = false;
            TableRef test_1 = wt.get_or_add_table("test_1", &was_added);
            if (was_added) {
                col_int = test_1->add_column(type_Int, "i");
            }
            test_1->create_object().set(col_int, 0);
            TableRef test_2 = wt.get_or_add_table("test_2", &was_added);
            if (was_added) {
                col_bin = test_2->add_column(type_Binary, "b");
            }
            wt.commit();
        }


        // Create 8*N shared group accessors
        for (int i = 0; i < 8 * N; ++i)
            shared_groups[i] = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));

        // Initiate 2*N read transactions with progressive changes
        for (int i = 0; i < 2 * N; ++i) {
            read_transactions[i] = shared_groups[i]->start_read();
            read_transactions[i]->verify();
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                CHECK_EQUAL(i, test_1->begin()->get<Int>(col_int));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1 + n_2; ++j) {
                    if (j % 19 == 0) {
                        CHECK_EQUAL(BinaryData(chunk_1), test_2->get_object(j).get<Binary>(col_bin));
                    }
                    else {
                        CHECK_EQUAL(BinaryData(chunk_2), test_2->get_object(j).get<Binary>(col_bin));
                    }
                }
            }
            {
                WriteTransaction wt(root_sg);
                wt.get_group().verify();
                TableRef test_1 = wt.get_table("test_1");
                add_int(*test_1, col_int, 1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->create_object().set(col_bin, BinaryData(chunk_1));
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
                wt.get_group().verify();
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->create_object().set(col_bin, BinaryData(chunk_2));
                }
                wt.commit();
            }
        }

        // Check isolation between read transactions
        for (int i = 0; i < 2 * N; ++i) {
            ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(i, test_1->begin()->get<Int>(col_int));
            ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
            int n_1 = i * 1;
            int n_2 = i * 18;
            CHECK_EQUAL(n_1 + n_2, test_2->size());
            for (int j = 0; j < n_1 + n_2; ++j) {
                if (j % 19 == 0) {
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_object(j).get<Binary>(col_bin));
                }
                else {
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_object(j).get<Binary>(col_bin));
                }
            }
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
                add_int(*test_1, col_int, 2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(i, test_1->begin()->get<Int>(col_int));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1 + n_2; ++j) {
                    if (j % 19 == 0) {
                        CHECK_EQUAL(BinaryData(chunk_1), test_2->get_object(j).get<Binary>(col_bin));
                    }
                    else {
                        CHECK_EQUAL(BinaryData(chunk_2), test_2->get_object(j).get<Binary>(col_bin));
                    }
                }
            }
            read_transactions[i] = nullptr;
        }

        // Initiate 6*N extra read transactionss with further progressive changes
        for (int i = 2 * N; i < 8 * N; ++i) {
            read_transactions[i] = shared_groups[i]->start_read();
#if !defined(_WIN32) || TEST_DURATION > 0
            read_transactions[i]->verify();
#endif
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                int i_2 = 2 * N + i;
                CHECK_EQUAL(i_2, test_1->begin()->get<Int>(col_int));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1 + n_2; ++j) {
                    if (j % 19 == 0) {
                        CHECK_EQUAL(BinaryData(chunk_1), test_2->get_object(j).get<Binary>(col_bin));
                    }
                    else {
                        CHECK_EQUAL(BinaryData(chunk_2), test_2->get_object(j).get<Binary>(col_bin));
                    }
                }
            }
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0
                wt.get_group().verify();
#endif
                TableRef test_1 = wt.get_table("test_1");
                add_int(*test_1, col_int, 1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->create_object().set(col_bin, BinaryData(chunk_1));
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
#if !defined(_WIN32) || TEST_DURATION > 0
                wt.get_group().verify();
#endif
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->create_object().set(col_bin, BinaryData(chunk_2));
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
                add_int(*test_1, col_int, 2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                int i_2 = i < 2 * N ? i : 2 * N + i;
                CHECK_EQUAL(i_2, test_1->begin()->get<Int>(col_int));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i * 1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1 + n_2; ++j) {
                    if (j % 19 == 0) {
                        CHECK_EQUAL(BinaryData(chunk_1), test_2->get_object(j).get<Binary>(col_bin));
                    }
                    else {
                        CHECK_EQUAL(BinaryData(chunk_2), test_2->get_object(j).get<Binary>(col_bin));
                    }
                }
            }
            read_transactions[i] = nullptr;
        }

        // Check final state via each shared group, then destroy it
        for (int i = 0; i < 8 * N; ++i) {
            {
                ReadTransaction rt(shared_groups[i]);
#if !defined(_WIN32) || TEST_DURATION > 0
                rt.get_group().verify();
#endif
                ConstTableRef test_1 = rt.get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(3 * 8 * N, test_1->begin()->get<Int>(col_int));
                ConstTableRef test_2 = rt.get_table("test_2");
                int n_1 = 8 * N * 1;
                int n_2 = 8 * N * 18;
                CHECK_EQUAL(n_1 + n_2, test_2->size());
                for (int j = 0; j < n_1 + n_2; ++j) {
                    if (j % 19 == 0) {
                        CHECK_EQUAL(BinaryData(chunk_1), test_2->get_object(j).get<Binary>(col_bin));
                    }
                    else {
                        CHECK_EQUAL(BinaryData(chunk_2), test_2->get_object(j).get<Binary>(col_bin));
                    }
                }
            }
            shared_groups[i] = nullptr;
        }

        // Check final state via new shared group
        {
            DBRef sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::MemOnly));
            ReadTransaction rt(sg);
#if !defined(_WIN32) || TEST_DURATION > 0
            rt.get_group().verify();
#endif
            ConstTableRef test_1 = rt.get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(3 * 8 * N, test_1->begin()->get<Int>(col_int));
            ConstTableRef test_2 = rt.get_table("test_2");
            int n_1 = 8 * N * 1;
            int n_2 = 8 * N * 18;
            CHECK_EQUAL(n_1 + n_2, test_2->size());
            for (int j = 0; j < n_1 + n_2; ++j) {
                if (j % 19 == 0) {
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_object(j).get<Binary>(col_bin));
                }
                else {
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_object(j).get<Binary>(col_bin));
                }
            }
        }
    }
}
#endif

// This test is a minimal repro. of core issue #842.
TEST(Many_ConcurrentReaders)
{
    SHARED_GROUP_TEST_PATH(path);
    const std::string path_str = path;

    // setup
    DBRef sg_w = DB::create(path_str);
    WriteTransaction wt(sg_w);
    TableRef t = wt.add_table("table");
    auto col_ndx = t->add_column(type_String, "column");
    t->create_object().set(col_ndx, StringData("string"));
    wt.commit();
    sg_w->close();

    auto reader = [path_str]() {
        try {
            for (int i = 0; i < 1000; ++i) {
                DBRef sg_r = DB::create(path_str);
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



TEST(Shared_WritesSpecialOrder)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

    const int num_rows =
        5; // FIXME: Should be strictly greater than REALM_MAX_BPNODE_SIZE, but that takes too long time.
    const int num_reps = 25;

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        auto table = wt.add_table("test");
        auto col = table->add_column(type_Int, "first");
        for (int i = 0; i < num_rows; ++i) {
            table->create_object(ObjKey(i)).set(col, 0);
        }
        wt.commit();
    }

    for (int i = 0; i < num_rows; ++i) {
        for (int j = 0; j < num_reps; ++j) {
            {
                WriteTransaction wt(sg);
                wt.get_group().verify();
                auto table = wt.get_table("test");
                auto col = table->get_column_key("first");
                Obj obj = table->get_object(ObjKey(i));
                CHECK_EQUAL(j, obj.get<Int>(col));
                obj.add_int(col, 1);
                wt.commit();
            }
        }
    }

    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        auto table = rt.get_table("test");
        auto col = table->get_column_key("first");
        for (int i = 0; i < num_rows; ++i) {
            CHECK_EQUAL(num_reps, table->get_object(ObjKey(i)).get<Int>(col));
        }
    }
}

namespace {

void writer_threads_thread(TestContext& test_context, std::string path, ObjKey key)
{
    // Open shared db
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

    for (size_t i = 0; i < 100; ++i) {
        // Increment cell
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.get_table("test");
            auto cols = t1->get_column_keys();
            t1->get_object(key).add_int(cols[0], 1);
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
            auto t = rt.get_table("test");
            auto cols = t->get_column_keys();
            int64_t v = t->get_object(key).get<Int>(cols[0]);
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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

        const int thread_count = 10;
        // Create first table in group
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.add_table("test");
            test_table_add_columns(t1);
            for (int i = 0; i < thread_count; ++i)
                t1->create_object(ObjKey(i)).set_all(0, 2, false, "test");
            wt.commit();
        }

        Thread threads[thread_count];

        // Create all threads
        for (int i = 0; i < thread_count; ++i)
            threads[i].start([this, &path, i] { writer_threads_thread(test_context, path, ObjKey(i)); });

        // Wait for all threads to complete
        for (int i = 0; i < thread_count; ++i)
            threads[i].join();

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            rt.get_group().verify();
            auto t = rt.get_table("test");
            auto col = t->get_column_keys()[0];

            for (int i = 0; i < thread_count; ++i) {
                int64_t v = t->get_object(ObjKey(i)).get<Int>(col);
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
            DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
            DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

    // Do a lot of sequential transactions
    for (int i = 0; i != n_outer; ++i) {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        auto table = wt.get_or_add_table("my_table");

        if (table->is_empty()) {
            REALM_ASSERT(table);
            table->add_column(type_String, "text");
        }
        auto cols = table->get_column_keys();

        for (int j = 0; j != n_inner; ++j) {
            REALM_ASSERT(table);
            table->create_object().set(cols[0], "x");
        }
        wt.commit();
    }

    // Verify that all was added correctly
    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        auto table = rt.get_table("my_table");
        auto col = table->get_column_keys()[0];
        size_t n = table->size();
        CHECK_EQUAL(n_outer * n_inner, n);

        for (auto it : *table) {
            CHECK_EQUAL("x", it.get<String>(col));
        }

        table->verify();
    }
}


TEST(Shared_Notifications)
{
    // Create a new shared db
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
    TransactionRef tr1 = sg->start_read();

    // No other instance have changed db since last transaction
    CHECK(!sg->has_changed(tr1));

    {
        // Open the same db again (in empty state)
        DBRef sg2 = DB::create(path, false, DBOptions(crypt_key()));

        // Verify that new group is empty
        {
            TransactionRef reader = sg2->start_read();
            CHECK(reader->is_empty());
            CHECK(!sg2->has_changed(reader));
        }

        // No other instance have changed db since last transaction

        // Add a new table
        {
            WriteTransaction wt(sg2);
            wt.get_group().verify();
            auto t1 = wt.add_table("test");
            test_table_add_columns(t1);
            t1->create_object(ObjKey(7)).set_all(1, 2, false, "test");
            wt.commit();
        }
    }

    // Db has been changed by other instance
    CHECK(sg->has_changed(tr1));
    tr1 = sg->start_read();
    // Verify that the new table has been added
    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        auto t1 = rt.get_table("test");
        CHECK_EQUAL(1, t1->size());
        ConstObj obj = t1->get_object(ObjKey(7));
        auto cols = t1->get_column_keys();
        CHECK_EQUAL(1, obj.get<Int>(cols[0]));
        CHECK_EQUAL(2, obj.get<Int>(cols[1]));
        CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
        CHECK_EQUAL("test", obj.get<String>(cols[3]));
    }

    // No other instance have changed db since last transaction
    CHECK(!sg->has_changed(tr1));
}


TEST(Shared_FromSerialized)
{
    SHARED_GROUP_TEST_PATH(path);

    // Create new group and serialize to disk
    {
        Group g1;
        auto t1 = g1.add_table("test");
        test_table_add_columns(t1);
        t1->create_object(ObjKey(7)).set_all(1, 2, false, "test");
        g1.write(path, crypt_key());
    }

    // Open same file as shared group
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

    // Verify that contents is there when shared
    {
        ReadTransaction rt(sg);
        rt.get_group().verify();
        auto t1 = rt.get_table("test");
        CHECK_EQUAL(1, t1->size());
        ConstObj obj = t1->get_object(ObjKey(7));
        auto cols = t1->get_column_keys();
        CHECK_EQUAL(1, obj.get<Int>(cols[0]));
        CHECK_EQUAL(2, obj.get<Int>(cols[1]));
        CHECK_EQUAL(false, obj.get<Bool>(cols[2]));
        CHECK_EQUAL("test", obj.get<String>(cols[3]));
    }
}

TEST_IF(Shared_StringIndexBug1, TEST_DURATION >= 1)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path, false, DBOptions(crypt_key()));

    {
        auto tr = db->start_write();
        TableRef table = tr->add_table("users");
        auto col = table->add_column(type_String, "username");
        table->add_search_index(col);
        for (int i = 0; i < REALM_MAX_BPNODE_SIZE + 1; ++i)
            table->create_object();
        for (int i = 0; i < REALM_MAX_BPNODE_SIZE + 1; ++i)
            table->remove_object(table->begin());
        tr->commit();
    }

    {
        auto tr = db->start_write();
        TableRef table = tr->get_table("users");
        table->create_object();
        tr->commit();
    }
}


TEST(Shared_StringIndexBug2)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

    {
        WriteTransaction wt(sg);
        wt.get_group().verify();
        TableRef table = wt.add_table("a");
        auto col = table->add_column(type_String, "b");
        table->add_search_index(col); // Not adding index makes it work
        table->create_object();
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
    DBRef db = DB::create(path, false, DBOptions(crypt_key()));
    ColKey col;
    {
        auto tr = db->start_write();
        TableRef table = tr->add_table("users");
        col = table->add_column(type_String, "username");
        table->add_search_index(col); // Disabling index makes it work
        tr->commit();
    }

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    size_t transactions = 0;
    std::vector<ObjKey> keys;
    for (size_t n = 0; n < 100; ++n) {
        const uint64_t action = random.draw_int_mod(1000);

        transactions++;

        if (action <= 500) {
            // delete random user
            auto tr = db->start_write();
            TableRef table = tr->get_table("users");
            if (table->size() > 0) {
                size_t del = random.draw_int_mod(table->size());
                // cerr << "-" << del << ": " << table->get_string(0, del) << std::endl;
                table->remove_object(keys[del]);
                keys.erase(keys.begin() + del);
                table->verify();
            }
            tr->commit();
        }
        else {
            // add new user
            auto tr = db->start_write();
            TableRef table = tr->get_table("users");
            char txt[100];
            rand_str(random, txt, 8);
            txt[8] = 0;
            // cerr << "+" << txt << std::endl;
            auto key = table->create_object().set_all(txt).get_key();
            keys.push_back(key);
            table->verify();
            tr->commit();
        }
    }
}

TEST(Shared_ClearColumnWithBasicArrayRootLeaf)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        TableRef test = wt.add_table("Test");
        auto col = test->add_column(type_Double, "foo");
        test->clear();
        test->create_object(ObjKey(7)).set(col, 727.2);
        wt.commit();
    }
    {
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        ReadTransaction rt(sg);
        ConstTableRef test = rt.get_table("Test");
        auto col = test->get_column_key("foo");
        CHECK_EQUAL(727.2, test->get_object(ObjKey(7)).get<Double>(col));
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
        DBRef db = DB::create(path, no_create, DBOptions(DBOptions::Durability::Async));

        for (int i = 0; i < 100; ++i) {
            //            std::cout << "t "<<n<<"\n";
            WriteTransaction wt(db);
            wt.get_group().verify();
            auto t1 = wt.get_or_add_table("test");
            if (t1->is_empty()) {
                test_table_add_columns(t1);
            }

            t1->create_object().set_all(1, i, false, "test");
            wt.commit();
        }
    }

    // Wait for async_commit process to shutdown
    // FIXME: we need a way to determine properly if the daemon has shot down instead of just sleeping
    millisleep(1000);

    // Read the db again in normal mode to verify
    {
        DBRef db = DB::create(path);

        ReadTransaction rt(db);
        rt.get_group().verify();
        auto t1 = rt.get_table("test");
        CHECK_EQUAL(100, t1->size());
    }
}


namespace {

#define multiprocess_increments 100

void multiprocess_thread(TestContext& test_context, std::string path, ObjKey key)
{
    // Open shared db
    bool no_create = false;
    DBRef sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::Async));

    for (size_t i = 0; i != multiprocess_increments; ++i) {
        // Increment cell
        {

            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t1 = wt.get_table("test");
            auto cols = t1->get_column_keys();
            t1->get_object(key).add_int(cols[0], 1);
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
            auto t = rt.get_table("test");
            auto cols = t->get_column_keys();
            int64_t v = t->get_object(key).get<Int>(cols[0]);
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
        DB sgr(path);
        DB sgw(path);
        {
            ReadTransaction rt0(sgr);
            WriteTransaction wt0(sgw);
            wt0.commit();
        }
        ReadTransaction rt(sgr);
        {
        }
        WriteTransaction wt(sgw);
        auto t1 = wt.get_table("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->create_object().set_all(0, 2, false, "test");
        }
        wt.commit();
        WriteTransaction wt2(sgw);
        auto t2 = wt2.get_table("test");
        for (size_t i = 0; i < rows; ++i) {
            t2->add(0, 2, false, "test");
        }
        wt2.commit();
    }
#else
#if 0
    {
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        auto t1 = wt.get_table("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->create_object().set_all(0, 2, false, "test");
        }
        wt.commit();
    }
#else
    {
        bool no_create = false;
        DBRef sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::Async));
        WriteTransaction wt(sg);
        auto t1 = wt.get_or_add_table("test");
        if (t1->is_empty()) {
            test_table_add_columns(t1);
        }
        for (size_t i = 0; i < rows; ++i) {
            t1->create_object().set_all(0, 2, false, "test");
        }
        wt.commit();
    }
#endif
#endif
    // Wait for async_commit process to shutdown
    // FIXME: No good way of doing this
    millisleep(1000);
#else
    {
        Group g(alone_path, Group::mode_ReadWrite);
        auto t1 = g.get_table("test");
        for (size_t i = 0; i < rows; ++i)
            t1->create_object().set_all(0, 2, false, "test");
        printf("Writing db\n");
        g.commit();
    }
#endif
}

void multiprocess_threaded(TestContext& test_context, std::string path, int64_t num_threads, int64_t base)
{
    // Do some changes in a async db
    std::unique_ptr<test_util::ThreadWrapper[]> threads;
    threads.reset(new test_util::ThreadWrapper[num_threads]);

    // Start threads
    for (int64_t i = 0; i != num_threads; ++i) {
        threads[i].start(
            [&test_context, &path, base, i] { multiprocess_thread(test_context, path, ObjKey(base + i)); });
    }

    // Wait for threads to finish
    for (int64_t i = 0; i != num_threads; ++i) {
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
        DBRef sg = DB::create(path, no_create, DBOptions(DBOptions::Durability::Async));
        ReadTransaction rt(sg);
        rt.get_group().verify();
        auto t = rt.get_table("test");
        auto col = t->get_column_keys()[0];
        for (int64_t i = 0; i != num_threads; ++i) {
            int64_t v = t->get_object(ObjKey(i + base)).get<Int>(col);
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
    millisleep(1000);

    // Verify - once more, in sync mode - that the changes were made
    {
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.get_group().verify();
        auto t = wt.get_table("test");
        auto cols = t->get_column_keys();
        auto it = t->begin();
        for (size_t i = 0; i != rows; ++i) {
            int64_t v = it->get<Int>(cols[0]);
            it->set(cols[0], 0);
            CHECK_EQUAL(result, v);
            ++it;
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
    millisleep(1); // FIXME: Is this really acceptable?

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

#ifdef _WIN32
#if 0

TEST(Shared_WaitForChangeAfterOwnCommit)
{
    SHARED_GROUP_TEST_PATH(path);

    DB* sg = new DB(path);
    sg->begin_write();
    sg->commit();
    bool b = sg->wait_for_change();
}

#endif

NONCONCURRENT_TEST(Shared_InterprocessWaitForChange)
{
    // We can't use SHARED_GROUP_TEST_PATH() because it will attempt to clean up the .realm file at the end,
    // and hence throw if the other processstill has the .realm file open
    std::string path = get_test_path("Shared_InterprocessWaitForChange", ".realm");

    // This works differently from POSIX: Here, the child process begins execution from the start of this unit
    // test and not from the place of fork().
    DWORD pid = winfork("Shared_InterprocessWaitForChange");

    if (pid == -1) {
        CHECK(false);
        return;
    }

    auto sg = DB::create(path);

    // An old .realm file with random contents can exist (such as a leftover from earlier crash) with random
    // data, so we always initialize the database
    {
        auto tr = sg->start_write();
        Group& g(*tr);
        if (g.size() == 1) {
            g.remove_table("data");
            TableRef table = g.add_table("data");
            auto col = table->add_column(type_Int, "ints");
            table->create_object().set(col, 0);
        }
        tr->commit();
        sg->wait_for_change(tr);
    }

    bool first = false;
    fastrand(time(0), true);

    // By turn, incremenet the counter and wait for the other to increment it too
    for (int i = 0; i < 10; i++)
    {
        auto tr = sg->start_write();
        Group& g(*tr);
        if (g.size() == 1) {
            TableRef table = g.get_table("data");
            auto col = table->get_column_key("ints");
            auto first_obj = table->begin();
            int64_t v = first_obj->get<int64_t>(col);

            if (i == 0 && v == 0)
                first = true;

            // Note: If this fails in child process (pid != 0) it might go undetected. This is not
            // critical since it will most likely result in a failure in the parent process also.
            CHECK_EQUAL(v - (first ? 0 : 1), 2 * i);
            first_obj->set(col, v + 1);
        }

        // millisleep(0) might yield time slice on certain OS'es, so we use fastrand() to get cases
        // of 0 delay, because non-yieldig is also an important test case.
        if(fastrand(1))
            millisleep((time(0) % 10) * 10);

        tr->commit();

        if (fastrand(1))
            millisleep((time(0) % 10) * 10);

        sg->wait_for_change(tr);

        if (fastrand(1))
            millisleep((time(0) % 10) * 10);
    }

    // Wake up other process so it will exit too
    auto tr = sg->start_write();
    tr->commit();
}
#endif

// This test will hang infinitely instead of failing!!!
TEST(Shared_WaitForChange)
{
    const int num_threads = 3;
    Mutex mutex;
    int shared_state[num_threads];
    for (int j = 0; j < num_threads; j++)
        shared_state[j] = 0;
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false);

    auto waiter = [&](DBRef db, int i) {
        TransactionRef tr;
        {
            LockGuard l(mutex);
            shared_state[i] = 1;
            tr = db->start_read();
        }
        db->wait_for_change(tr);
        {
            LockGuard l(mutex);
            shared_state[i] = 2; // this state should not be observed by the writer
        }
        db->wait_for_change(tr); // we'll fall right through here, because we haven't advanced our readlock
        {
            LockGuard l(mutex);
            tr->end_read();
            tr = db->start_read();
            shared_state[i] = 3;
        }
        db->wait_for_change(tr); // this time we'll wait because state hasn't advanced since we did.
        {
            tr = db->start_read();
            {
                LockGuard l(mutex);
                shared_state[i] = 4;
            }
            db->wait_for_change(tr); // everybody waits in state 4
            {
                LockGuard l(mutex);
                tr->end_read();
                tr = db->start_read();
                shared_state[i] = 5;
            }
        }
        db->wait_for_change(tr); // wait until wait_for_change is released
        {
            LockGuard l(mutex);
            shared_state[i] = 6;
        }
    };

    Thread threads[num_threads];
    for (int j = 0; j < num_threads; j++)
        threads[j].start([waiter, sg, j] { waiter(sg, j); });
    bool try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (shared_state[j] < 1) try_again = true;
            CHECK(shared_state[j] < 2);
        }
    }
    // At this point all transactions have progress to state 1,
    // and none of them has progressed further.
    // This write transaction should allow all readers to run again
    {
        WriteTransaction wt(sg);
        wt.commit();
    }

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
    // all readers now waiting before entering state 4
    {
        WriteTransaction wt(sg);
        wt.commit();
    }
    try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (4 != shared_state[j]) try_again = true;
        }
    }
    // all readers now waiting in stage 4
    {
        WriteTransaction wt(sg);
        wt.commit();
    }
    // readers racing into stage 5
    try_again = true;
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (5 != shared_state[j]) try_again = true;
        }
    }
    // everybod reached stage 5 and waiting
    try_again = true;
    sg->wait_for_change_release();
    while (try_again) {
        try_again = false;
        for (int j = 0; j < num_threads; j++) {
            LockGuard l(mutex);
            if (6 != shared_state[j]) {
                try_again = true;
            }
        }
    }
    for (int j = 0; j < num_threads; j++)
        threads[j].join();
}

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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        DBRef sg2 = DB::create(path, false, DBOptions(crypt_key()));
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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
            DBRef sg = DB::create(path, false, DBOptions(crypt_key())); // Create the very empty group
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


#if REALM_ENABLE_ENCRYPTION
// verify that even though different threads share the same encrypted pages,
// a thread will not get access without the key.
TEST(Shared_EncryptionKeyCheck)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key(true)));
    bool ok = false;
    try {
        DBRef sg_2 = DB::create(path, false, DBOptions());
    } catch (std::runtime_error&) {
        ok = true;
    }
    CHECK(ok);
    DBRef sg3 = DB::create(path, false, DBOptions(crypt_key(true)));
}

// opposite - if opened unencrypted, attempt to share it encrypted
// will throw an error.
TEST(Shared_EncryptionKeyCheck_2)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions());
    bool ok = false;
    try {
        DBRef sg_2 = DB::create(path, false, DBOptions(crypt_key(true)));
    } catch (std::runtime_error&) {
        ok = true;
    }
    CHECK(ok);
    DBRef sg3 = DB::create(path, false, DBOptions());
}

// if opened by one key, it cannot be opened by a different key
// disabled for now... needs to add a check in the encryption layer
// based on a hash of the key.
#if 0 // in principle this should be implemented.....
ONLY(Shared_EncryptionKeyCheck_3)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* first_key = crypt_key(true);
    char second_key[64];
    memcpy(second_key, first_key, 64);
    second_key[3] = ~second_key[3];
    DBRef sg = DB::create(path, false, DBOptions(first_key));
    bool ok = false;
    try {
        DBRef sg_2 = DB::create(path, false, DBOptions(second_key));
    } catch (std::runtime_error&) {
        ok = true;
    }
    CHECK(ok);
    DBRef sg3 = DB::create(path, false, DBOptions(first_key));
}
#endif

#endif

TEST(Shared_VersionCount)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);
    CHECK_EQUAL(1, sg->get_number_of_versions());
    TransactionRef reader = sg->start_read();
    {
        WriteTransaction wt(sg);
        CHECK_EQUAL(1, sg->get_number_of_versions());
        wt.commit();
    }
    CHECK_EQUAL(2, sg->get_number_of_versions());
    {
        WriteTransaction wt(sg);
        wt.commit();
    }
    CHECK_EQUAL(3, sg->get_number_of_versions());
    reader->close();
    CHECK_EQUAL(3, sg->get_number_of_versions());
    {
        WriteTransaction wt(sg);
        wt.commit();
    }
    // both the last and the second-last commit is kept, so once
    // you've committed anything, you will never get back to having
    // just a single version.
    CHECK_EQUAL(2, sg->get_number_of_versions());
}

TEST(Shared_MultipleRollbacks)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
    TransactionRef wt = sg->start_write();
    wt->rollback();
    wt->rollback();
}


TEST(Shared_MultipleEndReads)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
    TransactionRef reader = sg->start_read();
    reader->end_read();
    reader->end_read();
}

#ifdef REALM_DEBUG
// SharedGroup::reserve() is a debug method only available in debug mode
TEST(Shared_ReserveDiskSpace)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
        size_t orig_file_size = size_t(File(path).get_size());

        // Check that reserve() does not change the file size if the
        // specified size is less than the actual file size.
        size_t reserve_size_1 = orig_file_size / 2;
        sg->reserve(reserve_size_1);
        size_t new_file_size_1 = size_t(File(path).get_size());
        CHECK_EQUAL(orig_file_size, new_file_size_1);

        // Check that reserve() does not change the file size if the
        // specified size is equal to the actual file size.
        size_t reserve_size_2 = orig_file_size;
        sg->reserve(reserve_size_2);
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
        sg->reserve(reserve_size_3);
        size_t new_file_size_3 = size_t(File(path).get_size());
        CHECK(new_file_size_3 >= reserve_size_3);
        ObjKeys keys;

        // Check that disk space reservation is independent of transactions
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t = wt.add_table("table_1");
            test_table_add_columns(t);
            t->create_objects(2000, keys);
            wt.commit();
        }
        orig_file_size = size_t(File(path).get_size());
        size_t reserve_size_4 = 2 * orig_file_size + 1;
        sg->reserve(reserve_size_4);
        size_t new_file_size_4 = size_t(File(path).get_size());
        CHECK(new_file_size_4 >= reserve_size_4);
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            auto t = wt.add_table("table_2");
            test_table_add_columns(t);
            t->create_objects(2000, keys);
            orig_file_size = size_t(File(path).get_size());
            size_t reserve_size_5 = orig_file_size + 333;
            sg->reserve(reserve_size_5);
            size_t new_file_size_5 = size_t(File(path).get_size());
            CHECK(new_file_size_5 >= reserve_size_5);
            t = wt.add_table("table_3");
            test_table_add_columns(t);
            t->create_objects(2000, keys);
            wt.commit();
        }
        orig_file_size = size_t(File(path).get_size());
        size_t reserve_size_6 = orig_file_size + 459;
        sg->reserve(reserve_size_6);
        size_t new_file_size_6 = size_t(File(path).get_size());
        CHECK(new_file_size_6 >= reserve_size_6);
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            wt.commit();
        }
    }
}
#endif

TEST(Shared_MovingSearchIndex)
{
    // Test that the 'index in parent' property of search indexes is properly
    // adjusted when columns are inserted or removed at a lower column_index.

    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

    // Create an int column, regular string column, and an enumeration strings
    // column, and equip them with search indexes.
    ColKey int_col, str_col, enum_col, padding_col;
    std::vector<ObjKey> obj_keys;
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("foo");
        padding_col = table->add_column(type_Int, "padding");
        int_col = table->add_column(type_Int, "int");
        str_col = table->add_column(type_String, "regular");
        enum_col = table->add_column(type_String, "enum");

        table->create_objects(64, obj_keys);
        for (int i = 0; i < 64; ++i) {
            auto obj = table->get_object(obj_keys[i]);
            std::string out(std::string("foo") + util::to_string(i));
            obj.set<Int>(int_col, i);
            obj.set<String>(str_col, out);
            obj.set<String>(enum_col, "bar");
        }
        table->get_object(obj_keys.back()).set<String>(enum_col, "bar63");
        table->enumerate_string_column(enum_col);
        CHECK_EQUAL(0, table->get_num_unique_values(int_col));
        CHECK_EQUAL(0, table->get_num_unique_values(str_col));
        CHECK_EQUAL(2, table->get_num_unique_values(enum_col));

        table->add_search_index(int_col);
        table->add_search_index(str_col);
        table->add_search_index(enum_col);

        wt.get_group().verify();

        CHECK_EQUAL(obj_keys[61], table->find_first_int(int_col, 61));
        CHECK_EQUAL(obj_keys[62], table->find_first_string(str_col, "foo62"));
        CHECK_EQUAL(obj_keys[63], table->find_first_string(enum_col, "bar63"));
        wt.commit();
    }

    // Remove the padding column to shift the indexed columns
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("foo");

        CHECK(table->has_search_index(int_col));
        CHECK(table->has_search_index(str_col));
        CHECK(table->has_search_index(enum_col));
        CHECK_EQUAL(0, table->get_num_unique_values(int_col));
        CHECK_EQUAL(0, table->get_num_unique_values(str_col));
        CHECK_EQUAL(2, table->get_num_unique_values(enum_col));
        CHECK_EQUAL(ObjKey(), table->find_first_int(int_col, 100));
        CHECK_EQUAL(ObjKey(), table->find_first_string(str_col, "bad"));
        CHECK_EQUAL(ObjKey(), table->find_first_string(enum_col, "bad"));
        CHECK_EQUAL(obj_keys[41], table->find_first_int(int_col, 41));
        CHECK_EQUAL(obj_keys[42], table->find_first_string(str_col, "foo42"));
        CHECK_EQUAL(obj_keys[0], table->find_first_string(enum_col, "bar"));

        table->remove_column(padding_col);
        wt.get_group().verify();

        CHECK(table->has_search_index(int_col));
        CHECK(table->has_search_index(str_col));
        CHECK(table->has_search_index(enum_col));
        CHECK_EQUAL(0, table->get_num_unique_values(int_col));
        CHECK_EQUAL(0, table->get_num_unique_values(str_col));
        CHECK_EQUAL(2, table->get_num_unique_values(enum_col));
        CHECK_EQUAL(ObjKey(), table->find_first_int(int_col, 100));
        CHECK_EQUAL(ObjKey(), table->find_first_string(str_col, "bad"));
        CHECK_EQUAL(ObjKey(), table->find_first_string(enum_col, "bad"));
        CHECK_EQUAL(obj_keys[41], table->find_first_int(int_col, 41));
        CHECK_EQUAL(obj_keys[42], table->find_first_string(str_col, "foo42"));
        CHECK_EQUAL(obj_keys[0], table->find_first_string(enum_col, "bar"));

        auto obj = table->get_object(obj_keys[1]);
        obj.set<Int>(int_col, 101);
        obj.set<String>(str_col, "foo_Y");
        obj.set<String>(enum_col, "bar_Y");
        wt.get_group().verify();

        CHECK(table->has_search_index(int_col));
        CHECK(table->has_search_index(str_col));
        CHECK(table->has_search_index(enum_col));
        CHECK_EQUAL(0, table->get_num_unique_values(int_col));
        CHECK_EQUAL(0, table->get_num_unique_values(str_col));
        CHECK_EQUAL(3, table->get_num_unique_values(enum_col));
        CHECK_EQUAL(ObjKey(), table->find_first_int(int_col, 100));
        CHECK_EQUAL(ObjKey(), table->find_first_string(str_col, "bad"));
        CHECK_EQUAL(ObjKey(), table->find_first_string(enum_col, "bad"));
        CHECK_EQUAL(obj_keys[41], table->find_first_int(int_col, 41));
        CHECK_EQUAL(obj_keys[42], table->find_first_string(str_col, "foo42"));
        CHECK_EQUAL(obj_keys[0], table->find_first_string(enum_col, "bar"));
        CHECK_EQUAL(obj_keys[1], table->find_first_int(int_col, 101));
        CHECK_EQUAL(obj_keys[1], table->find_first_string(str_col, "foo_Y"));
        CHECK_EQUAL(obj_keys[1], table->find_first_string(enum_col, "bar_Y"));
        CHECK_EQUAL(obj_keys[63], table->find_first_string(enum_col, "bar63"));

        wt.commit();
    }
}

TEST_IF(Shared_BeginReadFailure, _impl::SimulatedFailure::is_enabled())
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);
    using sf = _impl::SimulatedFailure;
    sf::OneShotPrimeGuard pg(sf::shared_group__grow_reader_mapping);
    CHECK_THROW(sg->start_read(), sf);
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
        DBOptions::Durability durability_1 = DBOptions::Durability::Full;
        DBRef sg = DB::create(path, no_create, DBOptions(durability_1));

        DBOptions::Durability durability_2 = DBOptions::Durability::MemOnly;
        CHECK_LOGIC_ERROR(DB::create(path, no_create, DBOptions(durability_2)), LogicError::mixed_durability);
    }
}


TEST(Shared_WriteEmpty)
{
    SHARED_GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);
    {
        DBRef sg = DB::create(path_1);
        ReadTransaction rt(sg);
        rt.get_group().write(path_2);
    }
}


TEST(Shared_CompactEmpty)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        DBRef sg = DB::create(path);
        CHECK(sg->compact());
    }
}


TEST(Shared_VersionOfBoundSnapshot)
{
    SHARED_GROUP_TEST_PATH(path);
    DB::version_type version;
    DBRef sg = DB::create(path);
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
    sg->close();

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
NONCONCURRENT_TEST(Shared_StaticFuzzTestRunSanityCheck)
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
        const size_t iterations = 3;

        // Number of instructions in each test
        // Changing this strongly affects the test suite run time
        const size_t instructions = 2000;

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

#if 0 // not suitable for automatic testing
// This test checks what happens when a version is pinned and there are many
// large write transactions that grow the file quickly. It takes a long time
// and can make very very large files so it is not suited to automatic testing.
TEST_IF(Shared_encrypted_pin_and_write, false)
{
    const size_t num_rows = 1000;
    const size_t num_transactions = 1000000;
    const size_t num_writer_threads = 8;
    SHARED_GROUP_TEST_PATH(path);

    { // initial table structure setup on main thread
        DBRef sg = DB::create(path, false, DBOptions(crypt_key(true)));
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef t = group.add_table("table");
        t->add_column(type_String, "string_col", true);
        t->add_empty_row(num_rows);
        wt.commit();
    }

    DB sg_reader(path, false, DBOptions(crypt_key(true)));
    ReadTransaction rt(sg_reader); // hold first version

    auto do_many_writes = [&]() {
        DBRef sg = DB::create(path, false, DBOptions(crypt_key(true)));
        const size_t base_size = 100000;
        std::string base(base_size, 'a');
        // write many transactions to grow the file
        // around 4.6 GB seems to be the breaking size
        for (size_t t = 0; t < num_transactions; ++t) {
            std::vector<std::string> rows(num_rows);
            // change a character so there's no storage optimizations
            for (size_t row = 0; row < num_rows; ++row) {
                base[(t * num_rows + row)%base_size] = 'a' + (row % 52);
                rows[row] = base;
            }
            WriteTransaction wt(sg);
            Group& g = wt.get_group();
            auto keys = g.get_keys();
            TableRef table = g.get_table(keys[0]);
            for (size_t row = 0; row < num_rows; ++row) {
                StringData c(rows[row]);
                table->set_string(0, row, c);
            }
            wt.commit();
        }
    };

    Thread threads[num_writer_threads];
    for (size_t i = 0; i < num_writer_threads; ++i)
        threads[i].start(do_many_writes);

    for (size_t i = 0; i < num_writer_threads; ++i) {
        threads[i].join();
    }
}
#endif


// Scaled down stress test. (Use string length ~15MB for max stress)
NONCONCURRENT_TEST(Shared_BigAllocations)
{
    size_t string_length = 64 * 1024;
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
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
        auto cols = table->get_column_keys();
        for (int i = 0; i < 32; ++i) {
            table->create_object(ObjKey(i)).set(cols[0], long_string.c_str());
        }
        wt.commit();
    }
    for (int k = 0; k < 10; ++k) {
        // sg.compact(); // <--- enable this if you want to stress with compact()
        for (int j = 0; j < 20; ++j) {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            auto cols = table->get_column_keys();
            for (int i = 0; i < 20; ++i) {
                table->get_object(ObjKey(i)).set(cols[0], long_string.c_str());
            }
            wt.commit();
        }
    }
    sg->close();
}

#if !REALM_ANDROID // FIXME
TEST_IF(Shared_CompactEncrypt, REALM_ENABLE_ENCRYPTION)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key1 = "KdrL2ieWyspILXIPetpkLD6rQYKhYnS6lvGsgk4qsJAMr1adQnKsYo3oTEYJDIfa";
    const char* key2 = "ti6rOKviXrwxSGMPVk35Dp9Q4eku8Cu8YTtnnZKAejOTNIEv7TvXrYdjOPSNexMR";
    {
        auto db = DB::create(path, false, DBOptions(key1));
        auto tr = db->start_write();
        TableRef t = tr->add_table("table");
        auto col = t->add_column(type_String, "Strings");
        for (size_t i = 0; i < 10000; i++) {
            std::string str = "Shared_CompactEncrypt" + util::to_string(i);
            t->create_object().set(col, StringData(str));
        }
        tr->commit();

        CHECK(db->compact());
        {
            auto rt = db->start_read();
            CHECK(rt->has_table("table"));
        }

        bool bump_version_number = true;
        CHECK(db->compact(bump_version_number, key2));
        {
            auto rt = db->start_read();
            CHECK(rt->has_table("table"));
        }

        CHECK(db->compact(bump_version_number, nullptr));
        {
            auto rt = db->start_read();
            CHECK(rt->has_table("table"));
        }
    }
    {
        auto db = DB::create(path, true, DBOptions());
        {
            auto rt = db->start_read();
            CHECK(rt->has_table("table"));
        }
    }
}
#endif

// Repro case for: Assertion failed: top_size == 3 || top_size == 5 || top_size == 7 [0, 3, 0, 5, 0, 7]
NONCONCURRENT_TEST(Shared_BigAllocationsMinimized)
{
    // String length at 2K will not trigger the error.
    // all lengths >= 4K (that were tried) trigger the error
    size_t string_length = 4 * 1024;
    SHARED_GROUP_TEST_PATH(path);
    std::string long_string(string_length, 'a');
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
    {
        {
            WriteTransaction wt(sg);
            TableRef table = wt.add_table("table");
            table->add_column(type_String, "string_col");
            auto cols = table->get_column_keys();
            table->create_object(ObjKey(0)).set(cols[0], long_string.c_str());
            wt.commit();
        }
        sg->compact(); // <- required to provoke subsequent failures
        {
            WriteTransaction wt(sg);
            wt.get_group().verify();
            TableRef table = wt.get_table("table");
            auto cols = table->get_column_keys();
            table->get_object(ObjKey(0)).set(cols[0], long_string.c_str());
            wt.get_group().verify();
            wt.commit();
        }
    }
    {
        WriteTransaction wt(sg); // <---- fails here
        wt.get_group().verify();
        TableRef table = wt.get_table("table");
        auto cols = table->get_column_keys();
        table->get_object(ObjKey(0)).set(cols[0], long_string.c_str());
        wt.get_group().verify();
        wt.commit();
    }
    sg->close();
}

// Found by AFL (on a heavy hint from Finn that we should add a compact() instruction
NONCONCURRENT_TEST(Shared_TopSizeNotEqualNine)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path, false, DBOptions(crypt_key()));
    {
        TransactionRef writer = sg->start_write();

        TableRef t = writer->add_table("foo");
        t->add_column(type_Double, "doubles");
        std::vector<ObjKey> keys;
        t->create_objects(241, keys);
        writer->commit();
    }
    REALM_ASSERT_RELEASE(sg->compact());
    DBRef sg2 = DB::create(path, false, DBOptions(crypt_key()));
    {
        TransactionRef writer = sg2->start_write();
        writer->commit();
    }
    TransactionRef reader2 = sg2->start_read();
    DBRef sg3 = DB::create(path, false, DBOptions(crypt_key()));
    TransactionRef reader3 = sg3->start_read();
    TransactionRef reader = sg->start_read();
}

// Found by AFL after adding the compact instruction
// after further manual simplification, this test no longer triggers
// the double free, but crashes in a different way
TEST(Shared_Bptree_insert_failure)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg_w = DB::create(path, false, DBOptions(crypt_key()));
    TransactionRef writer = sg_w->start_write();

    auto tk = writer->add_table("")->get_key();
    writer->get_table(tk)->add_column(type_Double, "dgrpn", true);
    std::vector<ObjKey> keys;
    writer->get_table(tk)->create_objects(246, keys);
    writer->commit();
    REALM_ASSERT_RELEASE(sg_w->compact());
#if 0
    {
        // This intervening sg can do the same operation as the one doing compact,
        // but without failing:
        DB sg2(path, false, DBOptions(crypt_key()));
        Group& g2 = const_cast<Group&>(sg2.begin_write());
        g2.get_table(tk)->add_empty_row(396);
    }
#endif
    {
        TransactionRef writer2 = sg_w->start_write();
        writer2->get_table(tk)->create_objects(396, keys);
    }
}

NONCONCURRENT_TEST(SharedGroupOptions_tmp_dir)
{
    const std::string initial_system_dir = DBOptions::get_sys_tmp_dir();

    const std::string test_dir = "/test-temp";
    DBOptions::set_sys_tmp_dir(test_dir);
    CHECK(DBOptions::get_sys_tmp_dir().compare(test_dir) == 0);

    // Without specifying the temp dir, sys_tmp_dir should be used.
    DBOptions options;
    CHECK(options.temp_dir.compare(test_dir) == 0);

    // Should use the specified temp dir.
    const std::string test_dir2 = "/test2-temp";
    DBOptions options2(DBOptions::Durability::Full, nullptr, true, std::function<void(int, int)>(), test_dir2);
    CHECK(options2.temp_dir.compare(test_dir2) == 0);

    DBOptions::set_sys_tmp_dir(initial_system_dir);
}


namespace {

void wait_for(size_t expected, std::mutex& mutex, size_t& test_value)
{
    while (true) {
        millisleep(1);
        std::lock_guard<std::mutex> guard(mutex);
        if (test_value == expected) {
            return;
        }
    }
}

} // end anonymous namespace

TEST(Shared_LockFileInitSpinsOnZeroSize)
{
    SHARED_GROUP_TEST_PATH(path);

    bool no_create = false;
    DBOptions options;
    options.encryption_key = crypt_key();
    DBRef sg = DB::create(path, no_create, options);
    sg->close();

    CHECK(File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    std::mutex mutex;
    size_t test_stage = 0;

    Thread t;
    auto do_async = [&]() {
        File f(path.get_lock_path(), File::mode_Write);
        f.lock_shared();
        File::UnlockGuard ug(f);

        CHECK(f.is_attached());

        f.resize(0);
        f.sync();

        mutex.lock();
        test_stage = 1;
        mutex.unlock();

        millisleep(100);
        // the lock is then released and the other thread will be able to initialise properly
    };
    t.start(do_async);

    wait_for(1, mutex, test_stage);

    // we'll spin here without error until we can obtain the exclusive lock and initialise it ourselves
    sg = DB::create(path, no_create, options);
    CHECK(sg->is_attached());
    sg->close();

    t.join();
}


TEST(Shared_LockFileSpinsOnInitComplete)
{
    SHARED_GROUP_TEST_PATH(path);

    bool no_create = false;
    DBOptions options;
    options.encryption_key = crypt_key();
    DBRef sg = DB::create(path, no_create, options);
    sg->close();

    CHECK(File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    std::mutex mutex;
    size_t test_stage = 0;

    Thread t;
    auto do_async = [&]() {
        File f(path.get_lock_path(), File::mode_Write);
        f.lock_shared();
        File::UnlockGuard ug(f);

        CHECK(f.is_attached());

        f.resize(1); // ftruncate will write 0 to init_complete
        f.sync();

        mutex.lock();
        test_stage = 1;
        mutex.unlock();

        millisleep(100);
        // the lock is then released and the other thread will be able to initialise properly
    };
    t.start(do_async);

    wait_for(1, mutex, test_stage);

    // we'll spin here without error until we can obtain the exclusive lock and initialise it ourselves
    sg = DB::create(path, no_create, options);
    CHECK(sg->is_attached());
    sg->close();

    t.join();
}


TEST(Shared_LockFileOfWrongSizeThrows)
{
    // NOTE: This unit test attempts to mimic the initialization of the .lock file as it takes place inside
    // the SharedGroup::do_open() method. NOTE: If the layout of SharedGroup::SharedInfo should change,
    // this unit test might stop working.

    SHARED_GROUP_TEST_PATH(path);

    bool no_create = false;
    DBOptions options;
    options.encryption_key = crypt_key();
    DBRef sg = DB::create(path, no_create, options);
    sg->close();

    CHECK(File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    std::mutex mutex;
    size_t test_stage = 0;

    Thread t;
    auto do_async = [&]() {
        File f(path.get_lock_path(), File::mode_Write);
        f.lock_shared();
        File::UnlockGuard ug(f);

        CHECK(f.is_attached());

        size_t wrong_size = 100; // < sizeof(SharedInfo)
        f.resize(wrong_size); // ftruncate will fill with 0, which will set the init_complete flag to 0.
        f.seek(0);

        // On Windows, we implement a shared lock on a file by locking the first byte of the file. Since
        // you cannot write to a locked region using WriteFile(), we use memory mapping which works fine, and
        // which is also the same method used by the .lock file initialization in SharedGroup::do_open()
        char* mem = static_cast<char*>(f.map(realm::util::File::access_ReadWrite, 1));

        // set init_complete flag to 1 and sync
        mem[0] = 1;
        f.sync();

        CHECK_EQUAL(f.get_size(), wrong_size);

        mutex.lock();
        test_stage = 1;
        mutex.unlock();

        wait_for(2, mutex, test_stage); // hold the lock until other thread finished an open attempt
    };
    t.start(do_async);

    wait_for(1, mutex, test_stage);

    // we expect to throw if init_complete = 1 but the file is not the expected size (< sizeof(SharedInfo))
    // we go through 10 retry attempts before throwing
    CHECK_THROW(DB::create(path, no_create, options), IncompatibleLockFile);
    CHECK(!sg->is_attached());

    mutex.lock();
    test_stage = 2;
    mutex.unlock();

    t.join();
}


TEST(Shared_LockFileOfWrongVersionThrows)
{
    SHARED_GROUP_TEST_PATH(path);

    bool no_create = false;
    DBOptions options;
    options.encryption_key = crypt_key();
    DBRef sg = DB::create(path, no_create, options);

    CHECK(File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    std::mutex mutex;
    size_t test_stage = 0;

    Thread t;
    auto do_async = [&]() {
        CHECK(File::exists(path.get_lock_path()));

        File f;
        f.open(path.get_lock_path(), File::access_ReadWrite, File::create_Auto, 0); // Throws

        f.lock_shared();
        File::UnlockGuard ug(f);

        CHECK(f.is_attached());
        f.seek(6);
        char bad_version = 0;
        f.write(&bad_version, 1);
        f.sync();

        mutex.lock();
        test_stage = 1;
        mutex.unlock();

        wait_for(2, mutex, test_stage); // hold the lock until other thread finished an open attempt
    };
    t.start(do_async);

    wait_for(1, mutex, test_stage);
    sg->close();

    // we expect to throw if info->shared_info_version != g_shared_info_version
    CHECK_THROW(DB::create(path, no_create, options), IncompatibleLockFile);
    CHECK(!sg->is_attached());

    mutex.lock();
    test_stage = 2;
    mutex.unlock();

    t.join();
}


TEST(Shared_LockFileOfWrongMutexSizeThrows)
{
    SHARED_GROUP_TEST_PATH(path);

    bool no_create = false;
    DBOptions options;
    options.encryption_key = crypt_key();
    DBRef sg = DB::create(path, no_create, options);

    CHECK(File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    std::mutex mutex;
    size_t test_stage = 0;

    Thread t;
    auto do_async = [&]() {
        File f;
        f.open(path.get_lock_path(), File::access_ReadWrite, File::create_Auto, 0); // Throws
        f.lock_shared();
        File::UnlockGuard ug(f);

        CHECK(f.is_attached());

        char bad_mutex_size = sizeof(InterprocessMutex::SharedPart) + 1;
        f.seek(1);
        f.write(&bad_mutex_size, 1);
        f.sync();

        mutex.lock();
        test_stage = 1;
        mutex.unlock();

        wait_for(2, mutex, test_stage); // hold the lock until other thread finished an open attempt
    };
    t.start(do_async);

    wait_for(1, mutex, test_stage);

    sg->close();

    // we expect to throw if the mutex size is incorrect
    CHECK_THROW(DB::create(path, no_create, options), IncompatibleLockFile);
    CHECK(!sg->is_attached());

    mutex.lock();
    test_stage = 2;
    mutex.unlock();

    t.join();
}


TEST(Shared_LockFileOfWrongCondvarSizeThrows)
{
    SHARED_GROUP_TEST_PATH(path);

    bool no_create = false;
    DBOptions options;
    options.encryption_key = crypt_key();
    DBRef sg = DB::create(path, no_create, options);

    CHECK(File::exists(path));
    CHECK(File::exists(path.get_lock_path()));

    std::mutex mutex;
    size_t test_stage = 0;

    Thread t;
    auto do_async = [&]() {
        File f;
        f.open(path.get_lock_path(), File::access_ReadWrite, File::create_Auto, 0); // Throws
        f.lock_shared();
        File::UnlockGuard ug(f);

        CHECK(f.is_attached());

        char bad_condvar_size = sizeof(InterprocessCondVar::SharedPart) + 1;
        f.seek(2);
        f.write(&bad_condvar_size, 1);
        f.sync();

        mutex.lock();
        test_stage = 1;
        mutex.unlock();

        wait_for(2, mutex, test_stage); // hold the lock until other thread finished an open attempt
    };
    t.start(do_async);

    wait_for(1, mutex, test_stage);
    sg->close();

    // we expect to throw if the condvar size is incorrect
    CHECK_THROW(DB::create(path, no_create, options), IncompatibleLockFile);
    CHECK(!sg->is_attached());

    mutex.lock();
    test_stage = 2;
    mutex.unlock();

    t.join();
}

TEST(Shared_ConstObject)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg_w = DB::create(path);
    TransactionRef writer = sg_w->start_write();
    TableRef t = writer->add_table("Foo");
    auto c = t->add_column(type_Int, "integers");
    t->create_object(ObjKey(47)).set(c, 5);
    writer->commit();

    TransactionRef reader = sg_w->start_read();
    ConstTableRef t2 = reader->get_table("Foo");
    ConstObj obj = t2->get_object(ObjKey(47));
    CHECK_EQUAL(obj.get<int64_t>(c), 5);
}

TEST(Shared_ConstObjectIterator)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);
    ColKey col;
    {
        TransactionRef writer = sg->start_write();
        TableRef t = writer->add_table("Foo");
        col = t->add_column(type_Int, "integers");
        t->create_object(ObjKey(47)).set(col, 5);
        t->create_object(ObjKey(99)).set(col, 8);
        writer->commit();
    }
    {
        TransactionRef writer = sg->start_write();
        TableRef t2 = writer->get_or_add_table("Foo");
        auto i1 = t2->begin();
        auto i2 = t2->begin();
        CHECK_EQUAL(i1->get<int64_t>(col), 5);
        CHECK_EQUAL(i2->get<int64_t>(col), 5);
        i1->set(col, 7);
        CHECK_EQUAL(i2->get<int64_t>(col), 7);
        ++i1;
        CHECK_EQUAL(i1->get<int64_t>(col), 8);
        writer->commit();
    }

    // Now ensure that we can create a ConstIterator
    TransactionRef reader = sg->start_read();
    ConstTableRef t3 = reader->get_table("Foo");
    auto i3 = t3->begin();
    CHECK_EQUAL(i3->get<int64_t>(col), 7);
    ++i3;
    CHECK_EQUAL(i3->get<int64_t>(col), 8);
}

TEST(Shared_ConstList)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);
    TransactionRef writer = sg->start_write();

    TableRef t = writer->add_table("Foo");
    auto list_col = t->add_column_list(type_Int, "int_list");
    t->create_object(ObjKey(47)).get_list<int64_t>(list_col).add(47);
    writer->commit();

    TransactionRef reader = sg->start_read();
    ConstTableRef t2 = reader->get_table("Foo");
    ConstObj obj = t2->get_object(ObjKey(47));
    auto list1 = obj.get_list<int64_t>(list_col);

    CHECK_EQUAL(list1.get(0), 47);
    CHECK_EQUAL(obj.get_listbase_ptr(list_col)->size(), 1);
}

#ifdef LEGACY_TESTS

// Test if we can successfully open an existing encrypted file (generated by Core 4.0.3)
#if !REALM_ANDROID // FIXME
TEST_IF(Shared_DecryptExisting, REALM_ENABLE_ENCRYPTION)
{
    // Page size of system that reads the .realm file must be the same as on the system
    // that created it, because we are running with encryption
    std::string path = test_util::get_test_resource_path() + "test_shared_decrypt_" +
                        realm::util::to_string(page_size() / 1024) + "k_page.realm";

#if 0 // set to 1 to generate the .realm file
    {
        File::try_remove(path);
        //DB db(path, false, DBOptions(crypt_key(true)));
        auto db = DB::create(path, false, DBOptions(crypt_key(true)));
        auto rt = db->start_write();
        //Group& group = db.begin_write();
        TableRef table = rt->add_table("table");
        auto c0 = table->add_column(type_String, "string");
        auto o1 = table->create_object();
        std::string s = std::string(size_t(1.5 * page_size()), 'a');
        o1.set(c0, s);
        rt->commit();
    }
#else
    {
        SHARED_GROUP_TEST_PATH(temp_copy);
        File::copy(path, temp_copy);
        // Use history as we will now have to upgrade file
        std::unique_ptr<Replication> hist_w(make_in_realm_history(temp_copy));
        auto db = DB::create(*hist_w, DBOptions(crypt_key(true)));
        auto rt = db->start_read();
        ConstTableRef table = rt->get_table("table");
        auto o1 = *table->begin();
        std::string s1 = o1.get<StringData>(table->get_column_key("string"));
        std::string s2 = std::string(size_t(1.5 * page_size()), 'a');
        CHECK_EQUAL(s1, s2);
        rt->verify();
    }
#endif
}
#endif
#endif // LEGACY_TESTS

TEST(Shared_RollbackFirstTransaction)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef db = DB::create(*hist_w);
    auto wt = db->start_write();

    wt->add_table("table");
    wt->rollback_and_continue_as_read();
}

TEST(Shared_SimpleTransaction)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));

    {
        DBRef db_w = DB::create(*hist_w);
        auto wt = db_w->start_write();
        wt->verify();
        wt->commit();
        wt = nullptr;
        wt = db_w->start_write();
        wt->verify();
        wt->commit();
    }
    DBRef db_r = DB::create(*hist_r);
    {
        auto rt = db_r->start_read();
        rt->verify();
    }
}

TEST(Shared_OpenAfterClose)
{
    // Test case generated in [realm-core-6.0.0-rc1] on Wed Apr 11 16:08:05 2018.
    // REALM_MAX_BPNODE_SIZE is 4
    // ----------------------------------------------------------------------
    SHARED_GROUP_TEST_PATH(path);
    const char* key = nullptr;
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef db_w = DB::create(*hist_w, DBOptions(key));
    auto wt = db_w->start_write();

    wt = nullptr;
    db_w->close();
    db_w = DB::create(path, true, DBOptions(key));
    wt = db_w->start_write();
    wt = nullptr;
    db_w->close();
}

TEST(Shared_RemoveTableWithEnumAndLinkColumns)
{
    // Test case generated with fuzzer
    SHARED_GROUP_TEST_PATH(path);
    DBRef db_w = DB::create(path);
    TableKey tk;
    {
        auto wt = db_w->start_write();
        wt->add_table("Table_2");
        wt->commit();
    }
    {
        auto wt = db_w->start_write();
        auto table = wt->get_table("Table_2");
        tk = table->get_key();
        auto col_key = table->add_column(DataType(2), "string_3", false);
        table->enumerate_string_column(col_key);
        table->add_column_link(type_Link, "link_5", *table);
        table->add_search_index(col_key);
        wt->commit();
    }
    {
        auto wt = db_w->start_write();
        wt->remove_table(tk);
        wt->commit();
    }
}

TEST(Shared_GenerateObjectIdAfterRollback)
{
    // Test case generated in [realm-core-6.0.0-alpha.0] on Mon Aug 13 14:43:06 2018.
    // REALM_MAX_BPNODE_SIZE is 1000
    // ----------------------------------------------------------------------
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef db_w = DB::create(*hist_w);

    auto wt = db_w->start_write();

    wt->add_table("Table_0");
    {
        std::vector<ObjKey> keys;
        wt->get_table(TableKey(0))->create_objects(254, keys);
    }
    wt->commit_and_continue_as_read();

    wt->promote_to_write();
    try {
        wt->remove_table(TableKey(0));
    }
    catch (const CrossTableLinkTarget&) {
    }
    // Table accessor recycled
    wt->rollback_and_continue_as_read();

    wt->promote_to_write();
    // New table accessor created with m_next_key_value == -1
    wt->get_table(TableKey(0))->clear();
    {
        std::vector<ObjKey> keys;
        wt->get_table(TableKey(0))->create_objects(11, keys);
    }
    // table->m_next_key_value is now 11
    wt->get_table(TableKey(0))->add_column(DataType(9), "float_1", false);
    wt->rollback_and_continue_as_read();

    wt->promote_to_write();
    // Should not try to create object with key == 11
    {
        std::vector<ObjKey> keys;
        wt->get_table(TableKey(0))->create_objects(22, keys);
    }
}

TEST(Shared_UpgradeBinArray)
{
    // Checks that parent is updated appropriately when upgrading binary array
    SHARED_GROUP_TEST_PATH(path);
    DBRef db_w = DB::create(path);
    ColKey col;
    {
        auto wt = db_w->start_write();
        auto table = wt->add_table("Table_0");
        std::vector<ObjKey> keys;
        table->create_objects(65, keys);
        col = table->add_column(type_Binary, "binary_0", true);
        Obj obj = table->get_object(keys[0]);
        // This will upgrade from small to big blobs. Parent should be updated.
        obj.set(col, BinaryData{"dgrpnpgmjbchktdgagmqlihjckcdhpjccsjhnqlcjnbtersepknglaqnckqbffehqfgjnr"});
        wt->commit();
    }

    auto rt = db_w->start_read();
    CHECK_NOT(rt->get_table(TableKey(0))->get_object(ObjKey(0)).is_null(col));
    CHECK(rt->get_table(TableKey(0))->get_object(ObjKey(54)).is_null(col));
}

#if !REALM_ANDROID // FIXME
TEST_IF(Shared_MoreVersionsInUse, REALM_ENABLE_ENCRYPTION)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = "1234567890123456789012345678901123456789012345678901234567890123";
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    ColKey col;
    {
        DBRef db = DB::create(*hist, DBOptions(key));
        {
            auto wt = db->start_write();
            auto t = wt->add_table("Table_0");
            col = t->add_column(type_Int, "Integers");
            t->create_object();
            wt->add_table("Table_1");
            wt->commit();
        }
        // Create a number of versions
        for (int i = 0; i < 10; i++) {
            auto wt = db->start_write();
            auto t = wt->get_table("Table_1");
            for (int j = 0; j < 200; j++) {
                t->create_object();
            }
            wt->commit();
        }
    }
    {
        DBRef db = DB::create(*hist, DBOptions(key));

        // rt will now hold onto version 12
        auto rt = db->start_read();
        // Create table accessor to Table_0 - will use initial mapping
        auto table_r = rt->get_table("Table_0");
        {
            auto wt = db->start_write();
            auto t = wt->get_table("Table_1");
            // This will require extention of the mappings
            t->add_column(type_String, "Strings");
            // Here the mappings no longer required will be purged
            // Before the fix, this would delete the mapping used by
            // table_r accessor
            wt->commit();
        }

        auto obj = table_r->get_object(0);
        // Here we will need to translate a ref
        auto i = obj.get<Int>(col);
        CHECK_EQUAL(i, 0);
    }
}

TEST_IF(Shared_LinksToSameCluster, REALM_ENABLE_ENCRYPTION)
{
    // There was a problem when a link referred an object living in the same
    // Cluster as the origin object.
    SHARED_GROUP_TEST_PATH(path);
    const char* key = "1234567890123456789012345678901123456789012345678901234567890123";
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef db = DB::create(*hist, DBOptions(key));
    std::vector<ObjKey> keys;
    {
        auto wt = db->start_write();
        auto rt = db->start_read();
        std::vector<TableView> table_views;

        auto t = wt->add_table("Table_0");
        // Create more object that can be held in a single cluster
        t->create_objects(267, keys);
        auto col = t->add_column_link(type_Link, "link_0", *wt->get_table("Table_0"));

        // Create two links
        Obj obj = t->get_object(keys[48]);
        obj.set<ObjKey>(col, keys[0]);
        obj = t->get_object(keys[49]);
        obj.set<ObjKey>(col, keys[1]);
        wt->commit();
    }
    {
        auto wt = db->start_write();
        // Delete origin obj
        wt->get_table("Table_0")->remove_object(keys[48]);
        wt->verify();
        wt->commit();
    }
    {
        auto wt = db->start_write();
        // Delete target obj
        wt->get_table("Table_0")->remove_object(keys[1]);
        wt->verify();
        wt->commit();
    }
}
#endif

// Not much of a test. Mostly to exercise the code paths.
TEST(Shared_GetCommitSize)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path, false, DBOptions(crypt_key()));
    size_t size_before;
    size_t commit_size;
    {
        auto wt = db->start_write();
        size_before = wt->get_used_space();
        auto t = wt->add_table("foo");
        auto col_int = t->add_column(type_Int, "Integers");
        auto col_string = t->add_column(type_String, "Strings");
        for (int i = 0; i < 10000; i++) {
            std::string str = "Shared_CompactEncrypt" + util::to_string(i);
            t->create_object().set(col_int, i + 0x10000).set(col_string, StringData(str));
        }
        commit_size = wt->get_commit_size();
        auto allocated_size = db->get_allocated_size();
        CHECK_LESS(commit_size, allocated_size);
        wt->commit();
    }
    {
        ReadTransaction rt(db);
        auto size_after = rt.get_group().get_used_space();
        // Commit size will always be bigger than actual size
        CHECK_LESS(size_after - size_before, commit_size);
    }
}

#endif // TEST_SHARED
