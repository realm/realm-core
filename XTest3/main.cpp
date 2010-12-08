#include <stdio.h>
#include <UnitTest++.h>
#include "sqlite3.h"


int main() {
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
	rc = sqlite3_exec(db, "create table t1 (first INTEGER, second INTEGER, third INTEGER, fourth INTEGER);", NULL, NULL, &zErrMsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	// Prepare insert statement
	sqlite3_stmt *ppStmt = NULL;
	rc = sqlite3_prepare(db, "INSERT INTO t1 VALUES(0, 10, 1, 2);", -1, &ppStmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
	}

	// Fill with data
	for (size_t i = 0; i < 10000000; ++i) {
		rc = sqlite3_step(ppStmt);
		if (rc != SQLITE_DONE) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}
	}
	sqlite3_finalize(ppStmt); // Cleanup

	printf("Table complete. Press key to continue...\n");
	getchar(); // wait for key

	// Prepare select statement
	rc = sqlite3_prepare(db, "SELECT * FROM t1 WHERE fourth=1;", -1, &ppStmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
	}

	UnitTest::Timer timer;
	timer.Start();

	// Do a search over entire column (value not found)
	rc = sqlite3_step(ppStmt);
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	const int search_time = timer.GetTimeInMs();
	printf("Search time: %dms\n", search_time);

	sqlite3_finalize(ppStmt); // Cleanup

	getchar(); // wait for key
	sqlite3_close(db);

	return 1;
}