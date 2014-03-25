#include "testsettings.hpp"
#ifdef TEST_TRANSACTIONS_LASSE

#include <cstdlib>
#include <iostream>

#ifdef _WIN32
#  define NOMINMAX
#  include <windows.h> // Sleep(), sched_yield()
#  include <pthread.h> // pthread_win32_process_attach_np()
#else
#  include <sched.h>  // sched_yield()
#  include <unistd.h> // usleep()
#endif

#include <tightdb.hpp>
#include <tightdb/column.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/util/bind.hpp>
#include <tightdb/util/file.hpp>

#include "util/thread_wrapper.hpp"
#include "util/unit_test.hpp"
#include "util/test_only.hpp"

using namespace std;
using namespace tightdb;


// The tests in this file are run if you #define STRESSTEST1 and/or #define STRESSTEST2. Please define them in testsettings.hpp

namespace {

TIGHTDB_FORCEINLINE void rand_sleep()
{
    const int64_t ms = 500000;
    unsigned char r = rand();

    if (r <= 244)
        return;
    else if (r <= 248) {
        // Busyloop for 0 - 1 ms (on a 2 ghz), probably resume in current time slice
        int64_t t = (rand() * rand() * rand()) % ms; // rand can be just 16 bit
        for (volatile int64_t i = 0; i < t; i++) {
        }
    }
    else if (r <= 250) {
        // Busyloop for 0 - 20 ms (on a 2 ghz), maybe resume in different time slice
        int64_t t = ms * (rand() % 20);
        for (volatile int64_t i = 0; i < t; i++) {
        }
    }
    else if (r <= 252) {
        // Release current time slice but get next available
        sched_yield();
    }
    else if (r <= 254) {
        // Release current time slice and get time slice according to normal scheduling
#ifdef _MSC_VER
        Sleep(0);
#else
        usleep(0);
#endif
    }
    else {
        // Release time slices for at least 200 ms
#ifdef _MSC_VER
        Sleep(200);
#else
        usleep(200);
#endif
    }
}

} // anonymous namespace



// *************************************************************************************
// *
// *        Stress test 1
// *
// *************************************************************************************

namespace {

const int ITER1 =    2000;
const int READERS1 =   10;
const int WRITERS1 =   10;

void write_thread(int thread_ndx)
{
    int_least64_t w = thread_ndx;
    SharedGroup sg("database.tightdb");

    for (int i = 0; i < ITER1; ++i) {
        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            table->set_int(0, 0, w);
            rand_sleep();
            int64_t r = table->get_int(0, 0);
            CHECK_EQUAL(r, w);
            wt.commit();
        }

        // All writes by all threads must be unique so that it can be detected if they're spurious
        w += 1000;
    }
}

void read_thread()
{
    SharedGroup sg("database.tightdb");
    for (size_t i = 0; i < ITER1; ++i) {
        ReadTransaction rt(sg);
        int64_t r1 = rt.get_table("table")->get_int(0, 0);
        rand_sleep();
        int64_t r2 = rt.get_table("table")->get_int(0, 0);
        CHECK_EQUAL(r1, r2);
    }
}

} // anonymous namespace


TEST(Transactions_Stress1)
{
    test_util::ThreadWrapper read_threads[READERS1];
    test_util::ThreadWrapper write_threads[WRITERS1];

    srand(123);

    File::try_remove("database.tightdb");
    File::try_remove("database.tightdb.lock");

    {
        SharedGroup sg("database.tightdb");
        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            Spec& spec = table->get_spec();
            spec.add_column(type_Int, "row");
            table->update_from_spec();
            table->insert_empty_row(0, 1);
            table->set_int(0, 0, 0);
            wt.commit();
        }

        #if defined(_WIN32) || defined(__WIN32__) || defined(_WIN64)
            pthread_win32_process_attach_np ();
        #endif

        for (int i = 0; i < READERS1; ++i)
            read_threads[i].start(&read_thread);

        for (int i = 0; i < WRITERS1; ++i)
            write_threads[i].start(util::bind(write_thread, i));

        for (int i = 0; i < READERS1; ++i) {
            bool reader_has_thrown = read_threads[i].join();
            CHECK(!reader_has_thrown);
        }

        for (int i = 0; i < WRITERS1; ++i) {
            bool writer_has_thrown = write_threads[i].join();
            CHECK(!writer_has_thrown);
        }
    }

    File::try_remove("database.tightdb");
}


// *************************************************************************************
// *
// *        Stress test 2
// *
// *************************************************************************************

namespace {

const int      THREADS2 = 30;
const int      ITER2    = 2000;
const unsigned GROUPS2  = 30;

void create_groups()
{
    std::vector<SharedGroup*> groups;

    for (int i = 0; i < ITER2; ++i) {
        // Repeatedly create a group or destroy a group or do nothing
        int action = rand() % 2;

        if (action == 0 && groups.size() < GROUPS2) {
            groups.push_back(new SharedGroup("database.tightdb"));
        }
        else if (action == 1 && groups.size() > 0) {
            size_t g = rand() % groups.size();
            delete groups[g];
            groups.erase(groups.begin() + g);
        }
    }

    // Delete any remaining group to avoid memory and lock file leaks
    for (size_t i = 0; i < groups.size(); ++i)
        delete groups[i];
}

} // anonymous namespace

TEST(Transactions_Stress2)
{
    srand(123);
    test_util::ThreadWrapper threads[THREADS2];

    File::try_remove("database.tightdb");
    File::try_remove("database.tightdb.lock");

    for (int i = 0; i < THREADS2; ++i)
        threads[i].start(&create_groups);

    for (int i = 0; i < THREADS2; ++i) {
        bool thread_has_thrown = threads[i].join();
        CHECK(!thread_has_thrown);
    }

    File::try_remove("database.tightdb");
}


// *************************************************************************************
// *
// *        Stress test 3
// *
// *************************************************************************************

namespace {

unsigned int fast_rand()
{
    // Must be fast because important edge case is 0 delay. Not thread safe, but that just adds randomnes.
    static unsigned int u = 1;
    static unsigned int v = 1;
    v = 36969*(v & 65535) + (v >> 16);
    u = 18000*(u & 65535) + (u >> 16);
    return (v << 16) + u;
}

const int ITER3 =     20;
const int WRITERS3 =   4;
const int READERS3 =   4;
const size_t ROWS3 = 1*1000*1000 + 1000; // + 1000 to add extra depth level if TIGHTDB_MAX_LIST_SIZE = 1000
volatile bool terminate3 = false;

void write_thread3()
{
    SharedGroup sg("database.tightdb");

    for (int i = 0; i < ITER3; ++i) {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        size_t s = table->size();

        if (rand() % 2 == 0 && s > 0) {
            size_t from = fast_rand() % s;
            size_t n = fast_rand() % (s - from + 1);
            for (size_t t = 0; t < n; ++t)
                table->remove(from);
        }
        else if (s < ROWS3 / 2) {
            size_t at = fast_rand() % (s + 1);
            size_t n = fast_rand() % ROWS3;
            for (size_t t = 0; t < n; ++t) {
                table->insert_empty_row(at);
                table->set_int(0, at, fast_rand() % 80);
            }
        }

        wt.commit();
    }
}

void read_thread3()
{
    SharedGroup sg("database.tightdb");
    while (!terminate3) { // FIXME: Oops - this 'read' participates in a data race - http://stackoverflow.com/questions/12878344/volatile-in-c11
        ReadTransaction rt(sg);
        if(rt.get_table("table")->size() > 0) {
            int64_t r1 = rt.get_table("table")->get_int(0,0);
            rand_sleep();
            int64_t r2 = rt.get_table("table")->get_int(0,0);
            CHECK_EQUAL(r1, r2);
        }
    }
}

} // anonymous namespace


TEST(Transactions_Stress3)
{
    test_util::ThreadWrapper write_threads[WRITERS3];
    test_util::ThreadWrapper read_threads[READERS3];

    srand(123);

    File::try_remove("database.tightdb");
    File::try_remove("database.tightdb.lock");
    {
        SharedGroup sg("database.tightdb");

        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            Spec& spec = table->get_spec();
            spec.add_column(type_Int, "row");
            table->update_from_spec();
            wt.commit();
        }

        #if defined(_WIN32) || defined(__WIN32__) || defined(_WIN64)
            pthread_win32_process_attach_np ();
        #endif

        for (int i = 0; i < WRITERS3; ++i)
            write_threads[i].start(&write_thread3);

        for (int i = 0; i < READERS3; ++i)
            read_threads[i].start(&read_thread3);

        for (int i = 0; i < WRITERS3; ++i) {
            bool writer_has_thrown = write_threads[i].join();
            CHECK(!writer_has_thrown);
        }

        // Terminate reader threads cleanly
        terminate3 = true; // FIXME: Oops - this 'write' participates in a data race - http://stackoverflow.com/questions/12878344/volatile-in-c11
        for (int i = 0; i < READERS3; ++i) {
            bool reader_has_thrown = read_threads[i].join();
            CHECK(!reader_has_thrown);
        }
    }
    File::try_remove("database.tightdb");
}


// *************************************************************************************
// *
// *        Stress test 4. Identical to 1 except that readers keep reading until all
// *        writers are done instead of terminating prematurely ("bug" in test 1)
// *
// *************************************************************************************

namespace {

const int ITER4 =    2000;
const int READERS4 =   20;
const int WRITERS4 =   20;
volatile bool terminate4 = false;

void write_thread4(int thread_ndx)
{
    int_least64_t w = thread_ndx;
    SharedGroup sg("database.tightdb");

    for (int i = 0; i < ITER4; ++i) {
        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            table->set_int(0, 0, w);
            rand_sleep();
            int64_t r = table->get_int(0, 0);
            CHECK_EQUAL(r, w);
            wt.commit();
        }

        // All writes by all threads must be unique so that it can be detected if they're spurious
        w += 1000;
    }
}

void read_thread4()
{
    SharedGroup sg("database.tightdb");
    while (!terminate4) { // FIXME: Oops - this 'read' participates in a data race - http://stackoverflow.com/questions/12878344/volatile-in-c11
        ReadTransaction rt(sg);
        int64_t r1 = rt.get_table("table")->get_int(0, 0);
        rand_sleep();
        int64_t r2 = rt.get_table("table")->get_int(0, 0);
        CHECK_EQUAL(r1, r2);
    }
}

} // anonymous namespace


TEST(Transactions_Stress4)
{
    test_util::ThreadWrapper read_threads[READERS4];
    test_util::ThreadWrapper write_threads[WRITERS4];

    srand(123);

    File::try_remove("database.tightdb");
    File::try_remove("database.tightdb.lock");

    {
        SharedGroup sg("database.tightdb");

        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            Spec& spec = table->get_spec();
            spec.add_column(type_Int, "row");
            table->update_from_spec();
            table->insert_empty_row(0, 1);
            table->set_int(0, 0, 0);
            wt.commit();
        }

        #if defined(_WIN32) || defined(__WIN32__) || defined(_WIN64)
            pthread_win32_process_attach_np ();
        #endif

        for (int i = 0; i < READERS4; ++i)
            read_threads[i].start(&read_thread4);

        for (int i = 0; i < WRITERS4; ++i)
            write_threads[i].start(util::bind(write_thread4, i));

        for (int i = 0; i < WRITERS4; ++i) {
            bool writer_has_thrown = write_threads[i].join();
            CHECK(!writer_has_thrown);
        }

        terminate4 = true; // FIXME: Oops - this 'write' participates in a data race - http://stackoverflow.com/questions/12878344/volatile-in-c11
        for (int i = 0; i < READERS4; ++i) {
            bool reader_has_thrown = read_threads[i].join();
            CHECK(!reader_has_thrown);
        }
    }

    File::try_remove("database.tightdb");
}

#endif // TEST_TRANSACTIONS_LASSE
