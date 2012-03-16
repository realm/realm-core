#include <stdio.h>
#include "../UnitTest++/src/UnitTest++.h"
#include "sqlite3.h"
#include <string>
#include <vector>
#include "../Support/mem.h"
#include "../Support/number_names.h"
#include "../../src/win32/stdint.h"
using namespace std;

uint64_t rand2() { 
	static int64_t seed = 2862933555777941757ULL; 
	static int64_t seed2 = 0;
	seed = (2862933555777941757ULL * seed + 3037000493ULL); 
	seed2++;
	return seed * seed2 + seed2; 
}

int ITEMS = 50000;
int RANGE = 50000;

// NOT FINISHED 
// NOT FINISHED 
// NOT FINISHED 
// NOT FINISHED 
// NOT FINISHED 
// NOT FINISHED 
// NOT FINISHED 
// NOT FINISHED 

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
	rc = sqlite3_exec(db, "create table t1 (find INTEGER);", NULL, NULL, &zErrMsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
	}
	
	// Prepare insert statement
	sqlite3_stmt *ppStmt = NULL;
	rc = sqlite3_prepare(db, "INSERT INTO t1 VALUES(?1);", -1, &ppStmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
	}

	// Fill with data
	for (size_t i = 0; i < ITEMS; ++i) {
		// create random string
		const size_t n = rand() % RANGE;// * 10 + rand();
		sqlite3_reset(ppStmt);
sqlite3_bind_int(ppStmt, 1, n);
		rc = sqlite3_step(ppStmt);
		if (rc != SQLITE_DONE) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}
	}
	sqlite3_finalize(ppStmt); // Cleanup

	

	// Create table
	rc = sqlite3_exec(db, "create table t2 (id INTEGER, value INTEGER);", NULL, NULL, &zErrMsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
	}

	// Prepare insert statement
	rc = sqlite3_prepare(db, "INSERT INTO t2 VALUES(?1, ?2);", -1, &ppStmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
	}

	// Fill with data
	for (size_t i = 0; i < ITEMS; ++i) {
		// create random string
		const size_t n = rand() % RANGE;// * 10 + rand();
		sqlite3_reset(ppStmt);
		sqlite3_bind_int(ppStmt, 1, i);
		sqlite3_bind_int(ppStmt, 2, n);

		rc = sqlite3_step(ppStmt);
		if (rc != SQLITE_DONE) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}
	}
	sqlite3_finalize(ppStmt); // Cleanup


	for(int indexed = 0; indexed < 2; indexed++)
	{
		if(indexed == 1)
		{
			// Index
			rc = sqlite3_prepare(db, "CREATE INDEX sefhskjlfsdhk ON t2(value);", -1, &ppStmt, NULL);
			if (rc != SQLITE_OK) {
				fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
			}
			// Do a search over entire column (value not found)
			rc = sqlite3_step(ppStmt);
			if (rc != SQLITE_DONE) {
				fprintf(stderr, "SQL error: %s\n", zErrMsg);
				sqlite3_free(zErrMsg);
			}

		}



		UnitTest::Timer timer;

	
		// Prepare select statement
		rc = sqlite3_prepare(db, "SELECT t2.id FROM t2 where t2.value = ?1;", -1, &ppStmt, NULL);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}
		timer.Start();
		// Do a search over entire column (value not found)
		for (size_t i = 0; i < (indexed ? ITEMS : ITEMS / 1000); ++i) {
			sqlite3_reset(ppStmt);
			size_t n = rand2() % RANGE;
			sqlite3_bind_int(ppStmt, 1, n);
			rc = sqlite3_step(ppStmt);
		//	if (rc != SQLITE_ROW) {
		//		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		//	}
			// Sanity test
			//printf("%s, ", (char*)sqlite3_column_text(ppStmt, 0)); 
		}
		if(indexed)
			printf("Indexed FindAll: %dms\n", timer.GetTimeInMs());
		else
			printf("FindAll: %dms\n", timer.GetTimeInMs() * 1000);





		// Prepare select statement
		rc = sqlite3_prepare(db, "SELECT t2.id FROM t2 where t2.value = ?1 LIMIT 1;", -1, &ppStmt, NULL);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}
		timer.Start();
		// Do a search over entire column (value not found)
		for (size_t i = 0; i < (indexed ? ITEMS : ITEMS / 1000); ++i) {
			sqlite3_reset(ppStmt);
			size_t n = rand2() % RANGE;
			sqlite3_bind_int(ppStmt, 1, n);
			rc = sqlite3_step(ppStmt);
	//		if (rc != SQLITE_ROW) {
	//			fprintf(stderr, "SQL error: %s\n", zErrMsg);
	//		}
			// Sanity test
			//printf("%s, ", (char*)sqlite3_column_text(ppStmt, 0)); 
		}
		if(indexed)
			printf("Indexed Find: %dms\n", timer.GetTimeInMs());
		else
			printf("Find: %dms\n", timer.GetTimeInMs() * 1000);



		sqlite3_finalize(ppStmt); // Cleanup

	
/*
		// Prepare select statement
		rc = sqlite3_prepare(db, "SELECT sum(t2.id) FROM t1, t2 where t1.find = t2.value;", -1, &ppStmt, NULL);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}
		timer.Start();
		// Do a search over entire column (value not found)
		for (size_t i = 0; i < 1; ++i) {
			sqlite3_reset(ppStmt);
			rc = sqlite3_step(ppStmt);
			if (rc != SQLITE_ROW) {
				fprintf(stderr, "SQL error: %s\n", zErrMsg);
				sqlite3_free(zErrMsg);
			}
	//		printf("%s, ", (char*)sqlite3_column_text(ppStmt, 0)); 

		}
		printf("Non-indexed Join: %dms\n", timer.GetTimeInMs());
		sqlite3_finalize(ppStmt); // Cleanup
		*/











		

			printf("rrrr");
						// new benchmark 
	// Create table
	char *zErrMsg = NULL;
	rc = sqlite3_exec(db, "create table t9 (first INTEGER, second VARCHAR(100));", NULL, NULL, &zErrMsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
	}
	
	// Prepare insert statement
	sqlite3_stmt *ppStmt = NULL;
	rc = sqlite3_prepare(db, "INSERT INTO t9 VALUES(?1, ?2);", -1, &ppStmt, NULL);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
	}

	// Fill with data
	for (size_t i = 0; i < 5000000; ++i) {
		// create random string
		const size_t n = 3;// * 10 + rand();
		sqlite3_reset(ppStmt);
		sqlite3_bind_int(ppStmt, 1, n);
		sqlite3_bind_text(ppStmt, 2, "test string", -1, NULL);
		rc = sqlite3_step(ppStmt);
		if (rc != SQLITE_DONE) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}
	}
	sqlite3_finalize(ppStmt); // Cleanup
			// Prepare select statement
		rc = sqlite3_prepare(db, "SELECT t9.first FROM t9 WHERE t9.first = 5 or t9.first > 10;", -1, &ppStmt, NULL);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
		}

		timer.Start();

		printf("hej");

		// Do a search over entire column (value not found)
			sqlite3_reset(ppStmt);
			size_t n = rand2() % RANGE;
			sqlite3_bind_int(ppStmt, 1, n);
			rc = sqlite3_step(ppStmt);
		//	if (rc != SQLITE_ROW) {
		//		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		//	}
			// Sanity test
			//printf("%s, ", (char*)sqlite3_column_text(ppStmt, 0)); 
		
		printf("SELECT: %dms\n", timer.GetTimeInMs());

	printf("done");
	getchar();
	exit(1);

	return 1;









	}	





}
