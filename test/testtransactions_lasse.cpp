#include <cstring>
#include <iostream>
#include "tightdb.hpp"
#include "tightdb/group_shared.hpp"
#include <UnitTest++.h>
#include <tightdb/column.hpp>
#include <stdarg.h>
#include <sys/stat.h>
#include "tightdb/utilities.hpp"
#include "testsettings.hpp"

using namespace std;
using namespace tightdb;

// The tests in this file are run if you #define STRESSTEST1 and/or #define STRESSTEST2. Please define them in testsettings.hpp

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

void* write_thread(void* arg)
{
    int64_t w = int64_t(arg);
    int id = w;
    (void)id;
    SharedGroup db("database.tdb");

    for(;;)
    {
        {
            Group& group = db.begin_write(); 
            TableRef table = group.get_table("table");
            table->set_int(0, 0, w);
            randsleep();
            int64_t r = table->get_int(0, 0);
            CHECK_EQUAL(r, w);
            db.commit();
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

    SharedGroup db("database.tdb");
    for(;;)
    {
        {
            const Group& group = db.begin_read();
            r1 = group.get_table("table")->get_int(0, 0);
            randsleep();
            r2 = group.get_table("table")->get_int(0, 0);
            CHECK_EQUAL(r1, r2);
            db.end_read();        
        }
    }

    return 0;
}

TEST(Transactions_Stress1)
{ 
    const size_t READERS = 20;
    const size_t WRITERS = 20;

    pthread_t read_threads[READERS];
    pthread_t write_threads[WRITERS];

    srand(123);

    deletefile("database.tdb");
    deletefile("database.tdb.lock");

    SharedGroup db("database.tdb");

    {
        Group& group = db.begin_write(); 
        TableRef table = group.get_table("table");
        Spec& spec = table->get_spec();
        spec.add_column(COLUMN_TYPE_INT, "row");
        table->update_from_spec();
        table->insert_empty_row(0, 1);
        table->set_int(0, 0, 0);
    }

    db.commit();

    #if defined(_WIN32) || defined(__WIN32__) || defined(_WIN64)
        pthread_win32_process_attach_np ();
    #endif

    for(size_t t = 0; t < READERS; t++)
        pthread_create(&read_threads[t], NULL, read_thread, (void*)t);

    for(size_t t = 0; t < WRITERS; t++)
        pthread_create(&write_threads[t], NULL, write_thread, (void*)t);

    for(size_t t = 0; t < READERS; t++)
        pthread_join(read_threads[t], NULL);

    for(size_t t = 0; t < WRITERS; t++)
        pthread_join(write_threads[t], NULL);

}

#endif



#ifdef STRESSTEST2

// *************************************************************************************
// *
// *        Stress test 2
// *
// *************************************************************************************

void* create_groups(void* arg)
{
    (void)arg;
    const size_t ITER = 2000;
    const size_t GROUPS = 30;

    std::vector<SharedGroup*> group;

    for(size_t t = 0; t < ITER; ++t) {
        // Repeatedly create a group or destroy a group or do nothing
        int action = rand() % 2;

        if(action == 0 && group.size() < GROUPS) {
            group.push_back(new SharedGroup("database.tdb"));
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
    const size_t THREADS = 30;
    srand(123);
    pthread_t threads[THREADS];

    deletefile("database.tdb");
    deletefile("database.tdb.lock");

    for(size_t t = 0; t < THREADS; t++)
        pthread_create(&threads[t], NULL, create_groups, (void*)t);

    for(size_t t = 0; t < THREADS; t++)
        pthread_join(threads[t], NULL);
}
#endif



#ifdef STRESSTEST3

// *************************************************************************************
// *
// *        Stress test 3
// *
// *************************************************************************************

void* create_groups3(void* arg)
{
    const size_t ITER = 2000;
    const size_t GROUPS = 30;

    std::vector<SharedGroup*> group;

    for(size_t t = 0; t < ITER; ++t) {
        // Repeatedly create a group or destroy a group or do nothing
        int action = rand() % 2;

        if(action == 0 && group.size() < GROUPS) {
            group.push_back(new SharedGroup("database.tdb"));
        }
        else if(action == 1 && group.size() > 0) {
            size_t g = rand() % group.size();
            delete group[g];
            group.erase(group.begin() + g);
        }
    }
}

TEST(Transactions_Stress3)
{
    const size_t THREADS = 30;
    srand(123);
    pthread_t threads[THREADS];

    deletefile("database.tdb");
    deletefile("database.tdb.lock");

    for(size_t t = 0; t < THREADS; t++)
        pthread_create(&threads[t], NULL, create_groups, (void*)t);

    for(size_t t = 0; t < THREADS; t++)
        pthread_join(threads[t], NULL);
}
#endif
