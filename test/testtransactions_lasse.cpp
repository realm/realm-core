#include <cstdarg>
#include <cstring>
#include <iostream>

#include <sys/stat.h>
#include <pthread.h>

#ifdef _WIN32
#  define NOMINMAX
#  include <windows.h>
#else
#  include <unistd.h>
#endif

#include <UnitTest++.h>

#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/column.hpp>
#include <tightdb/utilities.hpp>

#include "testsettings.hpp"

using namespace std;
using namespace tightdb;

// The tests in this file are run if you #define STRESSTEST1 and/or #define STRESSTEST2. Please define them in testsettings.hpp

unsigned int fastrand()
{
    // Must be fast because important edge case is 0 delay. Not thread safe, but that just adds randomnes.
    static unsigned int u = 1;
    static unsigned int v = 1;
    v = 36969*(v & 65535) + (v >> 16);
    u = 18000*(u & 65535) + (u >> 16);
    return (v << 16) + u;
}

TIGHTDB_FORCEINLINE void randsleep(void)
{
    const int64_t ms = 500000;
    unsigned char r = rand();

    if(r <= 244)
        return;
    else if(r <= 248) {
        // Busyloop for 0 - 1 ms (on a 2 ghz), probably resume in current time slice
        int64_t t = (rand() * rand() * rand()) % ms; // rand can be just 16 bit
        for(volatile int64_t i = 0; i < t; i++) {
        }
    }
    else if(r <= 250) {
        // Busyloop for 0 - 20 ms (on a 2 ghz), maybe resume in different time slice
        int64_t t = ms * (rand() % 20);
        for(volatile int64_t i = 0; i < t; i++) {
        }
    }
    else if(r <= 252) {
        // Release current time slice but get next available
        sched_yield();
    }
    else if(r <= 254) {
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

void deletefile(const char* file)
{
    struct stat buf;
    remove(file);
    CHECK(stat(file, &buf) != 0);
}

#ifdef STRESSTEST1

// *************************************************************************************
// *
// *        Stress test 1
// *
// *************************************************************************************

const size_t ITER1 =    2000;
const size_t READERS1 =   20;
const size_t WRITERS1 =   20;

void* write_thread(void* arg)
{
    int64_t w = int64_t(arg);
    int id = w;
    (void)id;
    SharedGroup sg("database.tightdb");

    for(size_t t = 0; t < ITER1; ++t) {
        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            table->set_int(0, 0, w);
            randsleep();
            int64_t r = table->get_int(0, 0);
            CHECK_EQUAL(r, w);
            wt.commit();
        }

        // All writes by all threads must be unique so that it can be detected if they're spurious
        w += 1000;
    }
    return 0;
}

void* read_thread(void* arg)
{
    (void)arg;
    int64_t r1;
    int64_t r2;

    SharedGroup sg("database.tightdb");
    for(size_t t = 0; t < ITER1; ++t) {
        ReadTransaction rt(sg);
        r1 = rt.get_table("table")->get_int(0, 0);
        randsleep();
        r2 = rt.get_table("table")->get_int(0, 0);
        CHECK_EQUAL(r1, r2);
    }

    return 0;
}

TEST(Transactions_Stress1)
{
    pthread_t read_threads[READERS1];
    pthread_t write_threads[WRITERS1];

    srand(123);

    deletefile("database.tightdb");
    deletefile("database.tightdb.lock");

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

    for(size_t t = 0; t < READERS1; t++)
        pthread_create(&read_threads[t], NULL, read_thread, (void*)t);

    for(size_t t = 0; t < WRITERS1; t++)
        pthread_create(&write_threads[t], NULL, write_thread, (void*)t);

    for(size_t t = 0; t < READERS1; t++)
        pthread_join(read_threads[t], NULL);

    for(size_t t = 0; t < WRITERS1; t++)
        pthread_join(write_threads[t], NULL);
}

#endif



#ifdef STRESSTEST2

// *************************************************************************************
// *
// *        Stress test 2
// *
// *************************************************************************************

const size_t THREADS2 =   30;
const size_t ITER2 =    2000;
const size_t GROUPS2 =    30;

void* create_groups(void* arg)
{
    (void)arg;

    std::vector<SharedGroup*> group;

    for(size_t t = 0; t < ITER2; ++t) {
        // Repeatedly create a group or destroy a group or do nothing
        int action = rand() % 2;

        if(action == 0 && group.size() < GROUPS2) {
            group.push_back(new SharedGroup("database.tightdb"));
        }
        else if(action == 1 && group.size() > 0) {
            size_t g = rand() % group.size();
            delete group[g];
            group.erase(group.begin() + g);
        }
    }
    return NULL;
}

TEST(Transactions_Stress2)
{
    srand(123);
    pthread_t threads[THREADS2];

    deletefile("database.tightdb");
    deletefile("database.tightdb.lock");

    for(size_t t = 0; t < THREADS2; t++)
        pthread_create(&threads[t], NULL, create_groups, (void*)t);

    for(size_t t = 0; t < THREADS2; t++)
        pthread_join(threads[t], NULL);
}
#endif



#ifdef STRESSTEST3

// *************************************************************************************
// *
// *        Stress test 3
// *
// *************************************************************************************

const size_t ITER3 =     20;
const size_t WRITERS3 =   4;
const size_t READERS3 =   4;
const size_t ROWS3 = 1*1000*1000 + 1000; // + 1000 to add extra depth level if TIGHTDB_MAX_LIST_SIZE = 1000
bool terminate3 = false;

void* write_thread3(void* arg)
{
    int64_t w = int64_t(arg);
    int id = w;
    (void)id;
    SharedGroup sg("database.tightdb");

    for(size_t t = 0; t < ITER3; ++t) {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        size_t s = table->size();

        if(rand() % 2 == 0 && s > 0) {
            size_t from = fastrand() % s;
            size_t n = fastrand() % (s - from + 1);
            for(size_t t = 0; t < n; ++t)
                table->remove(from);
        }
        else if(s < ROWS3 / 2) {
            size_t at = fastrand() % (s + 1);
            size_t n = fastrand() % ROWS3;
            for(size_t t = 0; t < n; ++t) {
                table->insert_empty_row(at);
                table->set_int(0, at, fastrand() % 80);
            }
        }

        wt.commit();
    }
    return 0;

}

void* read_thread3(void* arg)
{
    (void)arg;
    int64_t r1;
    int64_t r2;

    SharedGroup sg("database.tightdb");
    while(!terminate3) {
        ReadTransaction rt(sg);
        r1 = rt.get_table("table")->get_int(0, 0);
        randsleep();
        r2 = rt.get_table("table")->get_int(0, 0);
        CHECK_EQUAL(r1, r2);
    }

    return 0;
}

TEST(Transactions_Stress3)
{
    pthread_t write_threads3[WRITERS3];
    pthread_t read_threads3[READERS3];

    srand(123);

    deletefile("database.tightdb");
    deletefile("database.tightdb.lock");

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

    for(size_t t = 0; t < WRITERS3; t++)
        pthread_create(&write_threads3[t], NULL, write_thread3, (void*)t);

    for(size_t t = 0; t < READERS3; t++)
        pthread_create(&read_threads3[t], NULL, read_thread3, (void*)t);

    for(size_t t = 0; t < WRITERS3; t++)
        pthread_join(write_threads3[t], NULL);

    // Terminate reader threads cleanly
    terminate3 = true;
    for(size_t t = 0; t < READERS3; t++)
        pthread_join(read_threads3[t], NULL);
}


#ifdef STRESSTEST4

// *************************************************************************************
// *
// *        Stress test 4. Identical to 1 except that readers keep reading until all
// *        writers are done instead of terminating prematurely ("bug" in test 1)
// *
// *************************************************************************************

const size_t ITER4 =    2000;
const size_t READERS4 =   20;
const size_t WRITERS4 =   20;
bool terminate4 = false;

void* write_thread4(void* arg)
{
    int64_t w = int64_t(arg);
    int id = w;
    (void)id;
    SharedGroup sg("database.tightdb");

    for(size_t t = 0; t < ITER4; ++t) {
        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("table");
            table->set_int(0, 0, w);
            randsleep();
            int64_t r = table->get_int(0, 0);
            CHECK_EQUAL(r, w);
            wt.commit();
        }

        // All writes by all threads must be unique so that it can be detected if they're spurious
        w += 1000;
    }
    return 0;
}

void* read_thread4(void* arg)
{
    (void)arg;
    int64_t r1;
    int64_t r2;

    SharedGroup sg("database.tightdb");
    while(!terminate4) {
        ReadTransaction rt(sg);
        r1 = rt.get_table("table")->get_int(0, 0);
        randsleep();
        r2 = rt.get_table("table")->get_int(0, 0);
        CHECK_EQUAL(r1, r2);
    }

    return 0;
}

TEST(Transactions_Stress4)
{
    pthread_t read_threads[READERS4];
    pthread_t write_threads[WRITERS4];

    srand(123);

    deletefile("database.tightdb");
    deletefile("database.tightdb.lock");

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

    for(size_t t = 0; t < READERS4; t++)
        pthread_create(&read_threads[t], NULL, read_thread4, (void*)t);

    for(size_t t = 0; t < WRITERS4; t++)
        pthread_create(&write_threads[t], NULL, write_thread4, (void*)t);

    for(size_t t = 0; t < WRITERS4; t++)
        pthread_join(write_threads[t], NULL);

    terminate4 = true;
    for(size_t t = 0; t < READERS4; t++)
        pthread_join(read_threads[t], NULL);
}

#endif

#endif
