#include <stdio.h>
#include <UnitTest++.h>
#include "sqlite3.h"
#include <string>
#include "../Support/mem.hpp"
#include "../Support/number_names.hpp"
using namespace std;

int main()
{
    // Open sqlite in-memory db
    sqlite3 *db = NULL;
    int rc = sqlite3_open(":memory:", &db);
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        exit(1);
    }

    // Create table
    char *zErrMsg = NULL;
    rc = sqlite3_exec(db, "create table t1 (first INTEGER, second VARCHAR(100), third INTEGER, fourth INTEGER);", NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }

    // Prepare insert statement
    sqlite3_stmt *ppStmt = NULL;
    rc = sqlite3_prepare(db, "INSERT INTO t1 VALUES(?1, ?2, ?3, ?4);", -1, &ppStmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
    }

    // Fill with data
    for (size_t i = 0; i < 250000; ++i) {
        // create random string
        const size_t n = rand() % 1000;// * 10 + rand();
        const string s = number_name(n);

        sqlite3_reset(ppStmt);
        sqlite3_bind_int(ppStmt, 1, n);
        sqlite3_bind_text(ppStmt, 2, s.c_str(), -1, NULL);
        sqlite3_bind_int(ppStmt, 3, 1);
        sqlite3_bind_int(ppStmt, 4, 2);

        rc = sqlite3_step(ppStmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        }
    }
    sqlite3_finalize(ppStmt); // Cleanup

    const size_t memUsed = GetMemUsage();
    printf("Memory usage: %ld bytes\n", long(memUsed));

    UnitTest::Timer timer;

    // Search small integer column
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "SELECT * FROM t1 WHERE fourth=1;", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        }

        timer.Start();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            sqlite3_reset(ppStmt);
            rc = sqlite3_step(ppStmt);
            if (rc != SQLITE_DONE) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
            }
        }

        const int search_time = timer.GetTimeInMs();
        printf("Search (small integer): %dms\n", search_time);

        sqlite3_finalize(ppStmt); // Cleanup
    }

    // Search string column
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "SELECT * FROM t1 WHERE second='abcde';", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        }

        timer.Start();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < 100; ++i) {
            rc = sqlite3_step(ppStmt);
            if (rc != SQLITE_DONE) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
            }
        }

        const int search_time = timer.GetTimeInMs();
        printf("Search (string): %dms\n", search_time);

        sqlite3_finalize(ppStmt); // Cleanup
    }

    // Create index on first column
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "CREATE INDEX i1a ON t1(first);", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        }

        timer.Start();

        // Do a search over entire column (value not found)
        rc = sqlite3_step(ppStmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }

        const int search_time = timer.GetTimeInMs();
        printf("Add index: %dms\n", search_time);

        sqlite3_finalize(ppStmt); // Cleanup
    }

    printf("Memory usage2: %ld bytes\n", long(GetMemUsage()));

    // Search with index
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "SELECT * FROM t1 WHERE fourth=?1;", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        }

        timer.Start();

        // Do a search over entire column
        for (size_t i = 0; i < 100; ++i) {
            const size_t n = rand() % 1000;

            sqlite3_reset(ppStmt);
            sqlite3_bind_int(ppStmt, 1, n);
            rc = sqlite3_step(ppStmt);
            if (rc == SQLITE_ERROR) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
            }
        }

        const int search_time = timer.GetTimeInMs();
        printf("Search index: %dms\n", search_time);

        sqlite3_finalize(ppStmt); // Cleanup
    }

    sqlite3_close(db);
#ifdef _MSC_VER
    getchar();
#endif
    return 0;
}
