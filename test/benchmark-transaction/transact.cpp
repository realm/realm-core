/*
 * Transaction benchmark for SQLite 3 and TightDB
 *
 * (C) Copyright 2012 by TightDB, Inc. <http://www.tightdb.com/>
 *
 */

#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <sqlite3.h>
#include <mysql/mysql.h>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>
#include <tightdb/group_shared.hpp>

using namespace std;
using namespace tightdb;


// which databases are possible
#define DB_TIGHTDB 0
#define DB_SQLITE  1
#define DB_MYSQL   2
#define DB_SQLITE_WAL 3

// database parameters - primarily for MySQL
#define DB_HOST "localhost"
#define DB_USER "root"
#define DB_PASS "root"
#define DB_NAME "benchmark"


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

double wall_time;

void usage(const char *msg)
{
    if (strlen(msg) > 0)
        cout << "Error: " << msg << endl << endl;
    cout << "Usage:" << endl;
    cout << " -h   : this text" << endl;
    cout << " -w   : number of writers" << endl;
    cout << " -r   : number of readers" << endl;
    cout << " -f   : database file" << endl;
    cout << " -d   : database (tdb, sqlite, sqlite-wal or mysql)" << endl;
    cout << " -t   : duration (in secs)" << endl;
    cout << " -n   : number of rows" << endl;
    cout << " -v   : verbose" << endl;
    cout << " -s   : single run" << endl;
    exit(-1);
}


// minimal version: no error checks!
void copy(const char *src, const char *dst)
{
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

// copy table in mysql
void copy_db(const char *src, const char *dst)
{
    MYSQL    *db;
    char sql[128];

    db = mysql_init(NULL);
    mysql_real_connect(db, DB_HOST, DB_USER, DB_PASS, DB_NAME, 0, NULL, 0);
    sprintf(sql, "DROP TABLE IF EXISTS %s", dst);
    mysql_query(db, sql);
    sprintf(sql, "CREATE TABLE %s LIKE %s", dst, src);
    mysql_query(db, sql);
    sprintf(sql, "INSERT INTO %s SELECT * FROM %s", dst, src);
    mysql_query(db, sql);
    mysql_close(db);
}

double delta_time(struct timespec ts_1, struct timespec ts_2)
{
    return (double)ts_2.tv_sec+1e-9*(double)ts_2.tv_nsec - ((double)ts_1.tv_sec+1e-9*(double)ts_1.tv_nsec);
}

void update_reader(struct timespec ts_1, struct timespec ts_2)
{
    double dt = delta_time(ts_1, ts_2);
    pthread_mutex_lock(&mtx_readers);
    dt_readers += dt;
    iteration_readers++;
    pthread_mutex_unlock(&mtx_readers);
}


void update_writer(struct timespec ts_1, struct timespec ts_2)
{
    double dt = delta_time(ts_1, ts_2);
    pthread_mutex_lock(&mtx_writers);
    dt_writers += dt;
    iteration_writers++;
    pthread_mutex_unlock(&mtx_writers);
}


// keep retrying
int db_retry(void *data, int i)
{
    return 1;
}

static void *sqlite_reader(void *arg)
{
    struct timespec ts_1, ts_2;
    sqlite3 *db;
    long int randy;
    char *errmsg;
    char sql[128];
    sqlite3_stmt *s;
    char *tail;
    long int c = 0;

    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);
    sqlite3_open(tinfo->datfile, &db);
    sqlite3_busy_handler(db, &db_retry, NULL);

    sprintf(sql, "SELECT COUNT(*) FROM test WHERE y = @Y");
    sqlite3_prepare_v2(db, sql, 128, &s, (const char **)&tail);
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
        sqlite3_bind_int(s, 1, randy);
        sqlite3_step(s);
        c += sqlite3_column_int(s, 0);

        if (sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg) == SQLITE_BUSY) {
            cout << "Oops" << endl;
        }
        sqlite3_clear_bindings(s);
        sqlite3_reset(s);

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_reader(ts_1, ts_2);
    }
    sqlite3_close(db);
    if (verbose) {
        cout << "Reader threads " << tinfo->thread_num << ":" << c << endl;
    }
    return NULL;
}

static void *mysql_reader(void *arg)
{
    struct timespec ts_1, ts_2;
    long int randy;
    char sql[128];
    MYSQL *db;
    long int c = 0;

    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);

    // open mysql
    db = mysql_init(NULL);
    if (mysql_real_connect(db, DB_HOST, DB_USER, DB_PASS, DB_NAME, 0, NULL, 0) == NULL) {
        cout << "Cannot connect to MySQL" << endl;
    }
    mysql_autocommit(db, 0);

    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction
        if (mysql_query(db, "START TRANSACTION;")) {
            cout << "MySQL error: " << mysql_errno(db) << endl;
        }
        randy = random() % 1000;
        sprintf(sql, "SELECT COUNT(*) FROM %s WHERE y = %ld", tinfo->datfile, randy);
        if (mysql_query(db, sql)) {
            cout << "MySQL error in " << sql << ": " << mysql_errno(db) << endl;
        }
        MYSQL_RES *res = mysql_use_result(db);
        MYSQL_ROW row = mysql_fetch_row(res);
        c += atoi(row[0]);
        mysql_free_result(res);
        if (mysql_query(db, "COMMIT;")) {
            cout << "Cannot commit" << endl;
        }

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_reader(ts_1, ts_2);
    }
    if (verbose) {
        cout << "Reader threads " << tinfo->thread_num << ":" << c << endl;
    }
    mysql_close(db);
}

static void *tdb_reader(void *arg)
{
    struct timespec ts_1, ts_2;
    struct thread_info *tinfo = (struct thread_info *)arg;
    size_t c = 0;
    srandom(tinfo->thread_num);
    clock_gettime(CLOCK_REALTIME, &ts_1);
    SharedGroup sg(tinfo->datfile);
    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction
        {
            ReadTransaction rt(sg);
            TestTable::ConstRef t = rt.get_table<TestTable>("test");
            long randy = random() % 1000;
            c += t->where().y.equal(randy).count();
        }

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_reader(ts_1, ts_2);
    }
    if (verbose) {
        cout << "Reader threads " << tinfo->thread_num << ":" << c << endl;
    }
    return NULL;
}

static void *sqlite_writer(void *arg)
{
    sqlite3 *db;
    long int randx, randy;
    char *errmsg;
    char sql[128];
    struct timespec ts_1, ts_2;
    sqlite3_stmt *s;
    char *tail;

    struct thread_info *tinfo = (struct thread_info *) arg;
    srandom(tinfo->thread_num);

    sqlite3_open(tinfo->datfile, &db);
    sqlite3_busy_handler(db, &db_retry, NULL);
    sprintf(sql, "UPDATE test SET x=@X WHERE y = @Y");
    sqlite3_prepare_v2(db, sql, 128, &s, (const char **)&tail);
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
        sqlite3_bind_int(s, 1, randx);
        sqlite3_bind_int(s, 2, randy);
        sqlite3_step(s);
        sqlite3_free(errmsg);
        if (sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errmsg) == SQLITE_BUSY) {
            cout << "Ooops" << endl;
        }
        sqlite3_clear_bindings(s);
        sqlite3_reset(s);

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_writer(ts_1, ts_2);
    }
    sqlite3_close(db);
    return NULL;
}

static void *mysql_writer(void *arg)
{
    struct timespec ts_1, ts_2;
    long int randx, randy;
    char sql[128];
    MYSQL *db;

    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);

    // open mysql
    db = mysql_init(NULL);
    mysql_real_connect(db, DB_HOST, DB_USER, DB_PASS, DB_NAME, 0, NULL, 0);

    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction
        if (mysql_query(db, "START TRANSACTION;")) {
            cout << "MySQL error: " << mysql_errno(db) << endl;
        }
        randx = random() % 1000;
        randy = random() % 1000;
        sprintf(sql, "UPDATE %s SET x=%ld WHERE y = %ld", tinfo->datfile, randx, randy);
        if (mysql_query(db, sql)) {
            cout << "MySQL error in " << sql << ": " << mysql_errno(db) << endl;
        }
        if (mysql_query(db, "COMMIT;")) {
            cout << "Cannot commit" << endl;
        }

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_writer(ts_1, ts_2);
    }
    mysql_close(db);
    return NULL;
}

static void *tdb_writer(void *arg)
{
    struct timespec ts_1, ts_2;
    struct thread_info *tinfo = (struct thread_info *)arg;
    srandom(tinfo->thread_num);
    SharedGroup sg(tinfo->datfile);
    while (true) {
        pthread_mutex_lock(&mtx_runnable);
        bool local_runnable = runnable;
        pthread_mutex_unlock(&mtx_runnable);
        if (!local_runnable) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts_1);

        // execute transaction
        {
            WriteTransaction wt(sg);
            BasicTableRef<TestTable> t = wt.get_or_add_table<TestTable>("test");
            long randx = random() % 1000;
            long randy = random() % 1000;
            TestTable::View tv = t->where().y.equal(randy).find_all();
            for(size_t j=0; j<tv.size(); ++j) {
                tv[j].x = randx;
            }
            wt.commit();
        }

        // update statistics
        clock_gettime(CLOCK_REALTIME, &ts_2);
        update_writer(ts_1, ts_2);
    }
    return NULL;
}

void sqlite_create(const char *f, long n, bool wal)
{
    int      i;
    long     randx, randy;
    char     sql[128];
    sqlite3 *db;
    char    *errmsg;

    File::try_remove(f);
    srandom(1);

    if (sqlite3_config(SQLITE_CONFIG_MULTITHREAD, 1) == SQLITE_ERROR) {
        cout << "SQLite has no multi-threading support" << endl;
    }
    sqlite3_open(f, &db);
    if (wal) {
        sqlite3_exec(db, "CREATE TABLE test (x INT, y INT)", NULL, NULL, &errmsg);
        sqlite3_exec(db, "PRAGMA journal_mode=wal", NULL, NULL, &errmsg);
    }
    else {
        sqlite3_exec(db, "CREATE TABLE test (x INT, y INT)", NULL, NULL, &errmsg);
    }
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

void mysql_create(const char *f, long n)
{
    int      i;
    long     randx, randy;
    char     sql[128];
    MYSQL    *db;

    db = mysql_init(NULL);
    mysql_real_connect(db, DB_HOST, DB_USER, DB_PASS, DB_NAME, 0, NULL, 0);
    mysql_query(db, "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    sprintf(sql, "DROP TABLE IF EXISTS %s", f);
    mysql_query(db, sql);
    sprintf(sql, "CREATE TABLE %s (x INT, y INT) ENGINE=innodb", f);
    mysql_query(db, sql);
    if (mysql_query(db, "START TRANSACTION;")) {
        cout << "MySQL error: " << mysql_errno(db) << endl;
    }
    for(i=0; i<n; ++i) {
        randx = random() % 1000;
        randy = random() % 1000;
        sprintf(sql, "INSERT INTO %s VALUES (%ld, %ld)", f, randx, randy);
        mysql_query(db, sql);
    }
    if (mysql_query(db, "COMMIT;")) {
        cout << "Cannot commit" << endl;
    }
    mysql_close(db);
}

void tdb_create(const char *f, long n)
{
    File::try_remove(f);
    File::try_remove(string(f)+".lock");
    SharedGroup sg(f);
    {
        WriteTransaction wt(sg);
        BasicTableRef<TestTable> t = wt.get_or_add_table<TestTable>("test");

        srandom(1);
        for(int i=0; i<n; ++i) {
            long randx = random() % 1000;
            long randy = random() % 1000;
            t->add(randx, randy);
        }
        wt.commit();
    }
}


void benchmark(bool single, int database, const char *datfile, long n_readers, long n_writers, unsigned int duration)
{
    pthread_attr_t attr;
    struct thread_info *tinfo;
    void *res;

    wall_time         = 0.0;
    dt_readers        = 0.0;
    iteration_readers = 0;
    dt_writers        = 0.0;
    iteration_writers = 0;
    runnable          = true;

    if (!single) {
        if (verbose) cout << "Copying file" << endl;
        if (database == DB_MYSQL) {
            copy_db(datfile, ("tmp"+string(datfile)).c_str());
        }
        else {
            copy(datfile, ("tmp"+string(datfile)).c_str());
        }
    }
    if (database != DB_MYSQL) {
        unlink(("tmp"+string(datfile)).c_str());
        unlink(("tmp"+string(datfile)+".lock").c_str());
    }

    assert(pthread_attr_init(&attr) == 0);
    assert((tinfo = (struct thread_info *)calloc(sizeof(struct thread_info), n_readers+n_writers)) != NULL);

    for(int i=0; i<(n_readers+n_writers); ++i) {
        tinfo[i].thread_num = i+1;
        if (single) {
            tinfo[i].datfile = strdup(datfile);
        }
        else {
            tinfo[i].datfile = strdup(("tmp"+string(datfile)).c_str());
        }
    }

    if (verbose) cout << "Starting threads" << endl;
    struct timespec ts_1;
    clock_gettime(CLOCK_REALTIME, &ts_1);
    for(int i=0; i<n_readers; ++i) {
        switch (database) {
        case DB_TIGHTDB:
            pthread_create(&tinfo[i].thread_id, &attr, &tdb_reader, &tinfo[i]);
            break;
        case DB_SQLITE:
        case DB_SQLITE_WAL:
            pthread_create(&
            tinfo[i].thread_id, &attr, &sqlite_reader, &tinfo[i]);
            break;
        case DB_MYSQL:
            pthread_create(&tinfo[i].thread_id, &attr, &mysql_reader, &tinfo[i]);
            break;
        }
    }
    for(int i=0; i<n_writers; ++i) {
        int j = i+n_readers;
        switch (database) {
        case DB_TIGHTDB:
            pthread_create(&tinfo[j].thread_id, &attr, &tdb_writer, &tinfo[j]);
            break;
        case DB_SQLITE:
        case DB_SQLITE_WAL:
            pthread_create(&tinfo[j].thread_id, &attr, &sqlite_writer, &tinfo[j]);
            break;
        case DB_MYSQL:
            pthread_create(&tinfo[j].thread_id, &attr, &mysql_writer, &tinfo[j]);
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

    for(int i=0; i<n_writers; ++i) {
        int j = i+n_readers;
        pthread_join(tinfo[j].thread_id, &res);
    }
    struct timespec ts_2;
    clock_gettime(CLOCK_REALTIME, &ts_2);
    wall_time = delta_time(ts_1, ts_2);

    if (database == DB_TIGHTDB || database == DB_SQLITE) {
        unlink(("tmp"+string(datfile)).c_str());
        unlink(("tmp"+string(datfile)+".lock").c_str());
    }
    free((void *)tinfo);
}

int main(int argc, char *argv[])
{
    int c;
    long n_readers = -1, n_writers = -1, n_records = -1;
    unsigned int duration = 0;
    int database = -1;
    bool single  = true;
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
                database = DB_TIGHTDB;
            }
            if (strcmp(optarg, "sqlite") == 0) {
                database = DB_SQLITE;
            }
            if (strcmp(optarg, "sqlite-wal") == 0) {
                database = DB_SQLITE_WAL;
            }
            if (strcmp(optarg, "mysql") == 0) {
                database = DB_MYSQL;
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
        n_writers = 2;
    if (n_readers == -1)
        n_readers = 2;
    if (n_records == -1)
        n_records = 10000;
    if (duration < 1)
        duration = 10;
    if (database == -1 && single)
        usage("-d missing");
    if (datfile == NULL)
        datfile = strdup("test_db");

    if (database == DB_SQLITE) {
        sqlite3_config(SQLITE_CONFIG_SERIALIZED);
    }

    if (verbose) cout << "Creating test data for " << database << endl;
    switch (database) {
    case DB_TIGHTDB:
        tdb_create(datfile, n_records);
        break;
    case DB_SQLITE:
    case DB_SQLITE_WAL:
        sqlite_create(datfile, n_records, database == DB_SQLITE_WAL);
        break;
    case DB_MYSQL:
        mysql_create(datfile, n_records);
        break;
    }

    // SQLite WAL is used
    if (database == DB_SQLITE_WAL) {
        database = DB_SQLITE;
    }

    if (single) {
        benchmark(true, database, datfile, n_readers, n_writers, duration);
        cout << wall_time << " " << iteration_readers << " " << dt_readers
             << " " << iteration_writers << " " << dt_writers << endl;
    }
    else {
        cout << "# Columns: "                   << endl;
        cout << "# 1. number of readers"        << endl;
        cout << "# 2. number of writers"        << endl;
        cout << "# 3. wall time"                << endl;
        cout << "# 4. read transactions"        << endl;
        cout << "# 5. read time"                << endl;
        cout << "# 6. writer transactions"      << endl;
        cout << "# 7. writer time"              << endl;
        for(int i=0; i<=n_readers; ++i) {
            for(int j=0; j<=n_writers; ++j) {
                benchmark(false, database, "test_db", i, j, duration);
                cout << i << " " << j << " ";
                cout << wall_time << " " << iteration_readers
                     << " " << dt_readers << " " << iteration_writers
                     << " " << dt_writers << endl;
            }
        }
    }
}
