#include <cstdlib>
#include <string>
#include <iostream>

#include "sqlite3.h"

#include "../util/timer.hpp"
#include "../util/mem.hpp"
#include "../util/number_names.hpp"

using namespace realm;

int main()
{
    const size_t ROWS = 250000;
    const size_t TESTS = 100;

    // Open sqlite in-memory db
    sqlite3 *db = NULL;
    int rc = sqlite3_open(":memory:", &db);
    if( rc ){
        std::cerr << "Can't open database: "<<sqlite3_errmsg(db)<<"\n";
        sqlite3_close(db);
        exit(1);
    }

    // Create table
    char *zErrMsg = NULL;
    rc = sqlite3_exec(db, "create table t1 (first INTEGER, second VARCHAR(100), third INTEGER, fourth INTEGER);", NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: "<<zErrMsg<<"\n";
        sqlite3_free(zErrMsg);
    }

    // Prepare insert statement
    sqlite3_stmt *ppStmt = NULL;
    rc = sqlite3_prepare(db, "INSERT INTO t1 VALUES(?1, ?2, ?3, ?4);", -1, &ppStmt, NULL);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: "<<sqlite3_errmsg(db)<<"\n";
    }

    std::cout << "Create random content with "<<ROWS<<" rows.\n\n";

    // Fill with data
    for (size_t i = 0; i < ROWS; ++i) {
        // create random string
        const size_t n = rand() % 1000;// * 10 + rand();
        const std::string s = test_util::number_name(n);

        sqlite3_reset(ppStmt);
        sqlite3_bind_int(ppStmt, 1, n);
        sqlite3_bind_text(ppStmt, 2, s.c_str(), -1, NULL);
        sqlite3_bind_int(ppStmt, 3, 1);
        sqlite3_bind_int(ppStmt, 4, 2);

        rc = sqlite3_step(ppStmt);
        if (rc != SQLITE_DONE) {
            std::cerr << "SQL error: "<<sqlite3_errmsg(db)<<"\n";
        }
    }
    sqlite3_finalize(ppStmt); // Cleanup

    std::cout << "Memory usage:\t\t"<<test_util::get_mem_usage()<<" bytes\n";

    test_util::Timer timer;

    // Search small integer column
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "SELECT * FROM t1 WHERE fourth=1;", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            std::cerr << "SQL error: "<<sqlite3_errmsg(db)<<"\n";
        }

        timer.reset();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < TESTS; ++i) {
            sqlite3_reset(ppStmt);
            rc = sqlite3_step(ppStmt);
            if (rc != SQLITE_DONE) {
                std::cerr << "SQL error: "<<zErrMsg<<"\n";
                sqlite3_free(zErrMsg);
            }
        }

        std::cout << "Search (small integer):\t"<<timer<<"\n";

        sqlite3_finalize(ppStmt); // Cleanup
    }

    // Search string column
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "SELECT * FROM t1 WHERE second='abcde';", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            std::cerr << "SQL error: "<<sqlite3_errmsg(db)<<"\n";
        }

        timer.reset();

        // Do a search over entire column (value not found)
        for (size_t i = 0; i < TESTS; ++i) {
            rc = sqlite3_step(ppStmt);
            if (rc != SQLITE_DONE) {
                std::cerr << "SQL error: "<<zErrMsg<<"\n";
                sqlite3_free(zErrMsg);
            }
        }

        std::cout << "Search (string):\t"<<timer<<"\n";

        sqlite3_finalize(ppStmt); // Cleanup
    }

    // Create index on first column
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "CREATE INDEX i1a ON t1(first);", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            std::cerr << "SQL error: "<<sqlite3_errmsg(db)<<"\n";
        }

        timer.reset();

        // Do a search over entire column (value not found)
        rc = sqlite3_step(ppStmt);
        if (rc != SQLITE_DONE) {
            std::cerr << "SQL error: "<<zErrMsg<<"\n";
            sqlite3_free(zErrMsg);
        }

        std::cout << "\nAdd index:\t\t"<<timer<<"\n";

        sqlite3_finalize(ppStmt); // Cleanup
    }

    std::cout << "Memory usage2:\t\t"<<test_util::get_mem_usage()<<" bytes\n";

    // Search with index
    {
        // Prepare select statement
        rc = sqlite3_prepare(db, "SELECT * FROM t1 WHERE first=?1;", -1, &ppStmt, NULL);
        if (rc != SQLITE_OK) {
            std::cerr << "SQL error: "<<sqlite3_errmsg(db)<<"\n";
        }

        timer.reset();

        // Do a search over entire column
        for (size_t i = 0; i < TESTS*10; ++i) {
            const size_t n = rand() % 1000;

            sqlite3_reset(ppStmt);
            sqlite3_bind_int(ppStmt, 1, n);
            rc = sqlite3_step(ppStmt);
            if (rc == SQLITE_ERROR) {
                std::cerr << "SQL error: "<<zErrMsg<<"\n";
                sqlite3_free(zErrMsg);
            }
        }

        std::cout << "Search index:\t\t"<<timer<<"\n";

        sqlite3_finalize(ppStmt); // Cleanup
    }

    sqlite3_close(db);
    std::cout << "\nDone.\n";

#ifdef _MSC_VER
    std::cin.get();
#endif
    return 0;
}
