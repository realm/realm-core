/*
 * Transaction benchmark for SQLite 3 and TightDB
 * 
 * (C) Copyright 2012 by TightDB, Inc. <http://www.tightdb.com/>
 *
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <sqlite3.h>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <iostream>

using namespace tightdb;
using namespace std;

TIGHTDB_TABLE_2(TestTable,
                x, Int,
                y, Int)


struct thread_info {
    pthread_t thread_id;
    int       thread_num;
    char     *datfile;
};

static bool runnable = true;
static pthread_mutex_t mtx_runnable = PTHREAD_MUTEX_INITIALIZER;


static double dt_readers;
static long iteration_readers;
static pthread_mutex_t mtx_readers = PTHREAD_MUTEX_INITIALIZER;
static double dt_writers;
static long iteration_writers;
static pthread_mutex_t mtx_writers = PTHREAD_MUTEX_INITIALIZER;

void usage(const char *msg) {
    if (strlen(msg) > 0)
        cout << "Error: " << msg << endl << endl;
    cout << "Usage:" << endl;
    cout << " -h   : this text" << endl;
    cout << " -w   : number of writers" << endl;
    cout << " -r   : number of readers" << endl;
    cout << " -f   : database file" << endl;
    cout << " -d   : database (tdb or sqlite)" << endl;
    cout << " -t   : duration (in secs)" << endl;
    cout << " -v   : verbose" << endl;
    exit(-1);
}

double delta_time(struct timespec ts_1, struct timespec ts_2) {
    return (double)ts_2.tv_sec+1e-9*(double)ts_2.tv_nsec - ((double)ts_1.tv_sec+1e-9*(double)ts_1.tv_nsec);
}

void update_reader(struct timespec ts_1, struct timespec ts_2) {
    double dt = delta_time(ts_1, ts_2);
    pthread_mutex_lock(&mtx_readers);
    dt_readers += dt;
    iteration_readers++;
    pthread_mutex_unlock(&mtx_readers);
}


void update_writer(struct timespec ts_1, struct timespec ts_2) {
    double dt = delta_time(ts_1, ts_2);
    pthread_mutex_lock(&mtx_writers);
    dt_writers += dt;
    iteration_writers++;
    pthread_mutex_unlock(&mtx_writers);
}


static void *sqlite_reader(void *arg) {
    struct timespec ts_1, ts_2;
    sqlite3 *db;
    long int randy;
    char *errmsg;
    char sql[128];

    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);
    sqlite3_open(tinfo->datfile, &db);
    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction
        sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errmsg);
        randy = random() % 1000;
        sprintf(sql, "SELECT COUNT(*) FROM test WHERE y = %ld", randy);
        sqlite3_exec(db, sql, NULL, NULL, &errmsg);
        sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg);

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_reader(ts_1, ts_2);
    }        
    sqlite3_close(db);
    return NULL;
}

static void *tdb_reader(void *arg) {
    struct timespec ts_1, ts_2;
    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);
    clock_gettime(CLOCK_REALTIME, &ts_1);
    SharedGroup shared(tinfo->datfile);
    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction
        const Group& g = shared.begin_read();
        TestTable::ConstRef t = g.get_table<TestTable>("test");
        long randy = random() % 1000;
        size_t count = t->where().y.equal(randy).count();
        shared.end_read();

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_reader(ts_1, ts_2);
    }
    return NULL;
}
 
static void *sqlite_writer(void *arg) {
    sqlite3 *db;
    long int randx, randy;
    char *errmsg;
    char sql[128];
    struct timespec ts_1, ts_2;

    struct thread_info *tinfo = (struct thread_info *) arg;
    srandom(tinfo->thread_num);

    sqlite3_open(tinfo->datfile, &db);
    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction        
        sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errmsg);
        randx = random() % 1000;
        randy = random() % 1000;
        sprintf(sql, "UPDATE test VALUES (%ld, %ld) WHERE y = %ld", randx, randy, randy);
        sqlite3_exec(db, sql, NULL, NULL, &errmsg);
        sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg);

        // update statistics
      
  clock_gettime(CLOCK_REALTIME, &ts_2);
        update_writer(ts_1, ts_2);
    }        
    sqlite3_close(db);
    return NULL;
}

static void *tdb_writer(void *arg) {
    struct timespec ts_1, ts_2;
    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);
    SharedGroup shared(tinfo->datfile);
    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction
        Group& g = shared.begin_write();
        BasicTableRef<TestTable> t = g.get_table<TestTable>("test");
        long randx = random() % 1000;
        long randy = random() % 1000;
        TestTable::View tv = t->where().y.equal(randy).find_all();
        for(size_t j=0; j<tv.size(); ++j) {
            tv[j].x = randx;
        }
        shared.commit();

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_writer(ts_1, ts_2);
    }
    return NULL;
}

void sqlite_create(const char *f, long n) {
    int      i;
    long     randx, randy;
    char     sql[128];
    sqlite3 *db;
    char    *errmsg;

    remove(f);
    srandom(1);
    sqlite3_open(f, &db);
    sqlite3_exec(db, "CREATE TABLE test (x INT, y INT)", NULL, NULL, &errmsg);
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errmsg);
    for(i=0; i<n; ++i) {
        randx = random() % 1000;
        randy = random() % 1000;
        sprintf(sql, "INSERT INTO test VALUES (%ld, %ld)", randx, randy);
        sqlite3_exec(db, sql, NULL, NULL, &errmsg);
    }
    sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg);
    sqlite3_close(db);
}

void tdb_create(const char *f, long n) {
    remove(f);
    remove((string(f)+".lock").c_str());
    SharedGroup shared(f);
    Group& g = shared.begin_write();
    BasicTableRef<TestTable> t = g.get_table<TestTable>("test");

    srandom(1);
    for(int i=0; i<n; ++i) {
        long randx = random() % 1000;
        long randy = random() % 1000;
        t->add(randx, randy);
    }
    shared.commit();
}


int main(int argc, char *argv[]) {
    int i, j, c;
    int database = -1;
    long n_readers = -1, n_writers = -1, n_records = -1;
    unsigned int duration = 0;
    bool verbose = false;
    extern char *optarg;
    void *res;
    char *datfile = NULL;
    pthread_attr_t attr;
    struct thread_info *tinfo;
    double tps_readers = 0.0, tps_writers = 0.0;

    dt_readers = 0.0;
    dt_writers = 0.0;
    iteration_readers = 0;
    iteration_writers = 0;

    while ((c = getopt(argc, argv, "hr:w:f:n:t:d:v")) != EOF) {
        switch (c) {
        case 'h':
            usage("");
        case 'r':
            n_readers = atoi(optarg);
            break;
        case 'w':
            n_writers = atoi(optarg);
            break;
        case 'f':
            datfile = strdup(optarg);
            break;
        case 'n':
            n_records = atoi(optarg);
            break;
        case 't':
            duration = atoi(optarg);
            break;
        case 'd':
            database = (strcmp(optarg, "tdb") == 0);
            break;
        case 'v':
            verbose = true;
            break;
        default:
            usage("Wrong option");
        }
    }

    if (n_writers == -1) 
        usage("-w missing");
    if (n_readers == -1) 
        usage("-r missing");
    if (n_records == -1)
        usage("-n missing");
    if (duration < 1)
        usage("-t missing");
    if (database == -1)
        usage("-d missing");
    if (datfile == NULL)
        usage("-f missing");


    assert(pthread_attr_init(&attr) == 0);
    assert((tinfo = (struct thread_info *)calloc(sizeof(struct thread_info), n_readers+n_writers)) != NULL);
   
    if (verbose)
        std::cout << "Creating test data" << std::endl;
    if (database) 
        tdb_create(datfile, n_records);
    else 
        sqlite_create(datfile, n_records);

    for(i=0; i<(n_readers+n_writers); ++i) {
        tinfo[i].thread_num = i+1;
        tinfo[i].datfile = strdup(datfile);
    }

    if (verbose)
        std::cout << "Starting threads" << std::endl;
    for(i=0; i<n_readers; ++i) {
        if (database)
            pthread_create(&tinfo[i].thread_id, &attr, &tdb_reader, &tinfo[i]);
        else
            pthread_create(&tinfo[i].thread_id, &attr, &sqlite_reader, &tinfo[i]);
    }
    for(i=0; i<n_writers; ++i) {
        j = i+n_readers;
        if (database)
            pthread_create(&tinfo[j].thread_id, &attr, &tdb_writer, &tinfo[j]);
        else
            pthread_create(&tinfo[j].thread_id, &attr, &sqlite_writer, &tinfo[j]);
    }


    if (verbose)
        cout << "Waiting for " << duration << " seconds" << endl;
    sleep(duration);

    if (verbose)
        cout << "Cancelling threads" << endl;
    pthread_mutex_lock(&mtx_runnable);
    runnable = false;
    pthread_mutex_unlock(&mtx_runnable);
 
    if (verbose)
        cout << "Waiting for threads" << endl;
    for(i=0; i<n_readers; ++i) {
        pthread_join(tinfo[i].thread_id, &res);
    }
    if (n_readers > 0) 
        tps_readers = (double)(iteration_readers)/dt_readers;

    for(i=0; i<n_writers; ++i) {
        j = i+n_readers;
        pthread_join(tinfo[j].thread_id, &res);
    }
    if (n_writers > 0)
        tps_writers = (double)(iteration_writers)/dt_writers;    

    std::cout << tps_readers << " " << tps_writers << std::endl;
}
