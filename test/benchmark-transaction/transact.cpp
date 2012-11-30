/*
 * Transaction benchmark for SQLite 3 and TightDB
 * 
 * (C) Copyright 2012 by TightDB, Inc. <http://www.tightdb.com/>
 *
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
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


static bool verbose;

// Shared variables and mutex to protect them
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
    cout << " -s   : single run" << endl;
    exit(-1);
}


// minimal version: no error checks!
void copy(const char *src, const char *dst) {
    int fd_to, fd_from;
    char buf[4096];
    ssize_t nread;

    fd_from = open(src, O_RDONLY);
    fd_to = open(dst, O_WRONLY | O_CREAT | O_EXCL, 0666);
    while ((nread = read(fd_from, buf, sizeof(buf))) > 0) {
        char *out_ptr = buf;
        ssize_t nwritten;
        do {
            nwritten = write(fd_to, out_ptr, nread);
            if (nwritten >= 0) {
                nread -= nwritten;
                out_ptr += nwritten;
            }
        } while (nread > 0);
    }

    close(fd_from);
    close(fd_to);
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



int db_retry(void *data, int i) {
    return 1;
}


static void *sqlite_reader(void *arg) {
    struct timespec ts_1, ts_2;
    sqlite3 *db;
    long int randy;
    char *errmsg;
    char sql[128];

    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);
    //sqlite3_open_v2(tinfo->datfile, &db, SQLITE_OPEN_FULLMUTEX, "unix");
    sqlite3_open(tinfo->datfile, &db);
    sqlite3_busy_handler(db, &db_retry, NULL);
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
        if (sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg) == SQLITE_BUSY) {
            cout << "Oops" << endl;
        }

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

    //sqlite3_open_v2(tinfo->datfile, &db, SQLITE_OPEN_FULLMUTEX, "unix");
    sqlite3_open(tinfo->datfile, &db);
    sqlite3_busy_handler(db, &db_retry, NULL);
    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction        
        sqlite3_exec(db, "BEGIN EXCLUSIVE TRANSACTION", NULL, NULL, &errmsg);
        randx = random() % 1000;
        randy = random() % 1000;
        sprintf(sql, "UPDATE test SET x= %ld, y=%ld WHERE y = %ld", randx, randy, randy);
        if (sqlite3_exec(db, sql, NULL, NULL, &errmsg) != SQLITE_OK) 
			cout << errmsg;
		sqlite3_free(errmsg);
        if (sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg) == SQLITE_BUSY) {
            cout << "Ooops" << endl;
        }

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

    if (sqlite3_config(SQLITE_CONFIG_MULTITHREAD, 1) == SQLITE_ERROR) {
        cout << "SQLite has no multi-threading support" << endl;
    }
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


void benchmark(int database, const char *datfile, long n_readers, long n_writers, unsigned int duration, 
                double& tps_readers, double& tps_writers) {
    pthread_attr_t attr;
    struct thread_info *tinfo;
    void *res;

    dt_readers        = 0.0;
    iteration_readers = 0;
    dt_writers        = 0.0;
    iteration_writers = 0;
    runnable          = true;

    if (verbose) cout << "Copying file" << endl;
    unlink(("tmp"+string(datfile)).c_str());
    unlink(("tmp"+string(datfile)+".lock").c_str());
    copy(datfile, ("tmp"+string(datfile)).c_str());

    assert(pthread_attr_init(&attr) == 0);
    assert((tinfo = (struct thread_info *)calloc(sizeof(struct thread_info), n_readers+n_writers)) != NULL);
   
    for(int i=0; i<(n_readers+n_writers); ++i) {
        tinfo[i].thread_num = i+1;
        tinfo[i].datfile = strdup(("tmp"+string(datfile)).c_str());
    }

    if (verbose) cout << "Starting threads" << endl;
    for(int i=0; i<n_readers; ++i) {
        switch (database) {
        case 0:
            pthread_create(&tinfo[i].thread_id, &attr, &tdb_reader, &tinfo[i]);
            break;
        case 1:
            pthread_create(&tinfo[i].thread_id, &attr, &sqlite_reader, &tinfo[i]);
            break;
        }
    }
    for(int i=0; i<n_writers; ++i) {
        int j = i+n_readers;
        switch (database) {
        case 0:
            pthread_create(&tinfo[j].thread_id, &attr, &tdb_writer, &tinfo[j]);
            break;
        case 1:
            pthread_create(&tinfo[j].thread_id, &attr, &sqlite_writer, &tinfo[j]);
            break;
        }
    }

    if (verbose) cout << "Waiting for " << duration << " seconds" << endl;
    sleep(duration);

    if (verbose) cout << "Cancelling threads" << endl;
    pthread_mutex_lock(&mtx_runnable);
    runnable = false;
    pthread_mutex_unlock(&mtx_runnable);
 
    if (verbose)
        cout << "Waiting for threads" << endl;
    for(int i=0; i<n_readers; ++i) {
        pthread_join(tinfo[i].thread_id, &res);
    }

    if (n_readers > 0) 
        tps_readers = (double)(iteration_readers)/dt_readers;
    else
        tps_readers = 0.0;

    for(int i=0; i<n_writers; ++i) {
        int j = i+n_readers;
        pthread_join(tinfo[j].thread_id, &res);
    }
    if (n_writers > 0)
        tps_writers = (double)(iteration_writers)/dt_writers;    
    else
        tps_writers = 0.0;

    unlink(("tmp"+string(datfile)).c_str());
    unlink(("tmp"+string(datfile)+".lock").c_str());
    free((void *)tinfo); 
}

int main(int argc, char *argv[]) {
    int c;
    long n_readers = -1, n_writers = -1, n_records = -1;
    unsigned int duration = 0;
    int database = -1;
    bool single  = false;
    extern char *optarg;
    char *datfile = NULL;

    verbose = false;
    while ((c = getopt(argc, argv, "hr:w:f:n:t:d:vs")) != EOF) {
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
            if (strcmp(optarg, "tdb") == 0) {
                database = 0;
            }
            else {
                database = 1;
            }
            break;
        case 'v':
            verbose = true;
            break;
        case 's':
            single = true;
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
    if (database == -1 && single)
        usage("-d missing");
    if (datfile == NULL && single)
        usage("-f missing");

	sqlite3_config(SQLITE_CONFIG_SERIALIZED);

    if (verbose) cout << "Creating test data" << endl;
    if (single) {
        switch (database) {
        case 0: 
            tdb_create(datfile, n_records);
            break;
        case 1:
            sqlite_create(datfile, n_records);
            break;
        }
    }
    else {
        tdb_create("test.tdb", n_records);
        sqlite_create("test.sqlite", n_records);
    }

    if (single) {
        double tps_readers, tps_writers;
        benchmark(database, datfile, n_readers, n_writers, duration, tps_readers, tps_writers);
        cout << tps_readers << " " << tps_writers << endl;
    }
    else {
        for(int i=0; i<=n_readers; ++i) {
            for(int j=0; j<=n_writers; ++j) {
                double tps_readers_sqlite, tps_writers_sqlite;
                double tps_readers_tdb, tps_writers_tdb;
                benchmark(0, "test.tdb", i, j, duration, tps_readers_tdb, tps_writers_tdb);
                benchmark(1, "test.sqlite", i, j, duration, tps_readers_sqlite, tps_writers_sqlite);
                cout << i << " " << j << " ";
                if (i == 0)
                    cout << "0.0";
                else
                    cout << tps_readers_tdb/tps_readers_sqlite;
                cout << " ";
                if (j == 0)
                    cout << "0.0";
                else
                    cout << tps_writers_tdb/tps_writers_sqlite;
                cout << endl;
            }
        }
    }       
}
