#include "Column.h"
#include <UnitTest++.h>
#include <vector>
#include <algorithm>
#include "testsettings.h"

struct db_setup {
	static Column c;
};

// Pre-declare local functions

Column db_setup::c;

TEST_FIXTURE(db_setup, Column_Add0) {
	c.Add(0);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Size(), (size_t)1);
}

TEST_FIXTURE(db_setup, Column_Add1) {
	c.Add(1);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Size(), 2);
}

TEST_FIXTURE(db_setup, Column_Add2) {
	c.Add(2);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Size(), 3);
}

TEST_FIXTURE(db_setup, Column_Add3) {
	c.Add(3);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Size(), 4);
}

TEST_FIXTURE(db_setup, Column_Add4) {
	c.Add(4);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Size(), 5);
}

TEST_FIXTURE(db_setup, Column_Add5) {
	c.Add(16);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Size(), 6);
}

TEST_FIXTURE(db_setup, Column_Add6) {
	c.Add(256);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Get(6), 256);
	CHECK_EQUAL(c.Size(), 7);
}

TEST_FIXTURE(db_setup, Column_Add7) {
	c.Add(65536);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Get(6), 256);
	CHECK_EQUAL(c.Get(7), 65536);
	CHECK_EQUAL(c.Size(), 8);
}

TEST_FIXTURE(db_setup, Column_Add8) {
	c.Add(4294967296);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Get(6), 256);
	CHECK_EQUAL(c.Get(7), 65536);
	CHECK_EQUAL(c.Get(8), 4294967296LL);
	CHECK_EQUAL(c.Size(), 9);
}

TEST_FIXTURE(db_setup, Column_AddNeg1) {
	c.Clear();
	c.Add(-1);

	CHECK_EQUAL(c.Size(), 1);
	CHECK_EQUAL(c.Get(0), -1);
}

TEST_FIXTURE(db_setup, Column_AddNeg2) {
	c.Add(-256);

	CHECK_EQUAL(c.Size(), 2);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
}

TEST_FIXTURE(db_setup, Column_AddNeg3) {
	c.Add(-65536);

	CHECK_EQUAL(c.Size(), 3);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
	CHECK_EQUAL(c.Get(2), -65536);
}

TEST_FIXTURE(db_setup, Column_AddNeg4) {
	c.Add(-4294967296);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
	CHECK_EQUAL(c.Get(2), -65536);
	CHECK_EQUAL(c.Get(3), -4294967296LL);
}

TEST_FIXTURE(db_setup, Column_Set) {
	c.Set(0, 3);
	c.Set(1, 2);
	c.Set(2, 1);
	c.Set(3, 0);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), 3);
	CHECK_EQUAL(c.Get(1), 2);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 0);
}

TEST_FIXTURE(db_setup, Column_Insert1) {
	// Set up some initial values
	c.Clear();
	c.Add(0);
	c.Add(1);
	c.Add(2);
	c.Add(3);

	// Insert in middle
	c.Insert(2, 16);

	CHECK_EQUAL(c.Size(), 5);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 16);
	CHECK_EQUAL(c.Get(3), 2);
	CHECK_EQUAL(c.Get(4), 3);
}

TEST_FIXTURE(db_setup, Column_Insert2) {
	// Insert at top
	c.Insert(0, 256);

	CHECK_EQUAL(c.Size(), 6);
	CHECK_EQUAL(c.Get(0), 256);
	CHECK_EQUAL(c.Get(1), 0);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 16);
	CHECK_EQUAL(c.Get(4), 2);
	CHECK_EQUAL(c.Get(5), 3);
}

TEST_FIXTURE(db_setup, Column_Insert3) {
	// Insert at bottom
	c.Insert(6, 65536);

	CHECK_EQUAL(c.Size(), 7);
	CHECK_EQUAL(c.Get(0), 256);
	CHECK_EQUAL(c.Get(1), 0);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 16);
	CHECK_EQUAL(c.Get(4), 2);
	CHECK_EQUAL(c.Get(5), 3);
	CHECK_EQUAL(c.Get(6), 65536);
}
/*
TEST_FIXTURE(db_setup, Column_Index1) {
	// Create index
	Column index;
	c.BuildIndex(index);

	CHECK_EQUAL(0, c.FindWithIndex(256));
	CHECK_EQUAL(1, c.FindWithIndex(0));
	CHECK_EQUAL(2, c.FindWithIndex(1));
	CHECK_EQUAL(3, c.FindWithIndex(16));
	CHECK_EQUAL(4, c.FindWithIndex(2));
	CHECK_EQUAL(5, c.FindWithIndex(3));
	CHECK_EQUAL(6, c.FindWithIndex(65536));
	
	c.ClearIndex();
}
*/
TEST_FIXTURE(db_setup, Column_Delete1) {
	// Delete from middle
	c.Delete(3);

	CHECK_EQUAL(c.Size(), 6);
	CHECK_EQUAL(c.Get(0), 256);
	CHECK_EQUAL(c.Get(1), 0);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 2);
	CHECK_EQUAL(c.Get(4), 3);
	CHECK_EQUAL(c.Get(5), 65536);
}

TEST_FIXTURE(db_setup, Column_Delete2) {
	// Delete from top
	c.Delete(0);

	CHECK_EQUAL(c.Size(), 5);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 65536);
}

TEST_FIXTURE(db_setup, Column_Delete3) {
	// Delete from bottom
	c.Delete(4);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
}

TEST_FIXTURE(db_setup, Column_DeleteAll) {
	// Delete all items one at a time
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);

	CHECK(c.IsEmpty());
	CHECK_EQUAL(0, c.Size());
}


TEST_FIXTURE(db_setup, Column_Find1) {
	// Look for a non-existing value
	size_t res = c.Find(10);

	CHECK_EQUAL(res, -1);
}

TEST_FIXTURE(db_setup, Column_Find2) {
	// zero-bit width
	c.Clear();
	c.Add(0);
	c.Add(0);

	size_t res = c.Find(0);
	CHECK_EQUAL(res, 0);
}

TEST_FIXTURE(db_setup, Column_Find3) {
	// expand to 1-bit width
	c.Add(1);

	size_t res = c.Find(1);
	CHECK_EQUAL(res, 2);
}

TEST_FIXTURE(db_setup, Column_Find4) {
	// expand to 2-bit width
	c.Add(2);

	size_t res = c.Find(2);
	CHECK_EQUAL(res, 3);
}

TEST_FIXTURE(db_setup, Column_Find5) {
	// expand to 4-bit width
	c.Add(4);

	size_t res = c.Find(4);
	CHECK_EQUAL(res, 4);
}

TEST_FIXTURE(db_setup, Column_Find6) {
	// expand to 8-bit width
	c.Add(16);

	// Add some more to make sure we
	// can search in 64bit chunks
	c.Add(16);
	c.Add(7);

	size_t res = c.Find(7);
	CHECK_EQUAL(7, res);
}

TEST_FIXTURE(db_setup, Column_Find7) {
	// expand to 16-bit width
	c.Add(256);

	size_t res = c.Find(256);
	CHECK_EQUAL(8, res);
}

TEST_FIXTURE(db_setup, Column_Find8) {
	// expand to 32-bit width
	c.Add(65536);

	size_t res = c.Find(65536);
	CHECK_EQUAL(9, res);
}

TEST_FIXTURE(db_setup, Column_Find9) {
	// expand to 64-bit width
	c.Add(4294967296);

	size_t res = c.Find(4294967296);
	CHECK_EQUAL(10, res);
}

/* Partial find is not fully implemented yet
#define PARTIAL_COUNT 100
TEST_FIXTURE(db_setup, Column_PartialFind1) {
	c.Clear();

	for (size_t i = 0; i < PARTIAL_COUNT; ++i) {
		c.Add(i);
	}

	CHECK_EQUAL(-1, c.Find(PARTIAL_COUNT+1, 0, PARTIAL_COUNT));
	CHECK_EQUAL(-1, c.Find(0, 1, PARTIAL_COUNT));
	CHECK_EQUAL(PARTIAL_COUNT-1, c.Find(PARTIAL_COUNT-1, PARTIAL_COUNT-1, PARTIAL_COUNT));
}*/


TEST_FIXTURE(db_setup, Column_HeaderParse) {
	Column column(c.GetRef(), (Array*)NULL, 0);
	const bool isEqual = (c == column);
	CHECK(isEqual);
}

TEST_FIXTURE(db_setup, Column_Destroy) {
	// clean up (ALWAYS PUT THIS LAST)
	c.Destroy();
}

TEST(Column_Sort) {
	// Create Column with random values
	Column a;
	a.Add(25);
	a.Add(12);
	a.Add(50);
	a.Add(3);
	a.Add(34);
	a.Add(0);
	a.Add(17);
	a.Add(51);
	a.Add(2);
	a.Add(40);

	a.Sort();

	CHECK_EQUAL(0, a.Get(0));
	CHECK_EQUAL(2, a.Get(1));
	CHECK_EQUAL(3, a.Get(2));
	CHECK_EQUAL(12, a.Get(3));
	CHECK_EQUAL(17, a.Get(4));
	CHECK_EQUAL(25, a.Get(5));
	CHECK_EQUAL(34, a.Get(6));
	CHECK_EQUAL(40, a.Get(7));
	CHECK_EQUAL(50, a.Get(8));
	CHECK_EQUAL(51, a.Get(9));

	// Cleanup
	a.Destroy();
}

/** FindAll() int tests spread out over bitwidth
 *
 */

TEST(Column_FindAll_IntMin){
	Column c;
	Column r;

	const int value = 0;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		c.Add(0);
	}

	c.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < c.Size()){
		if(c.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	c.Destroy();
	r.Destroy();
}

TEST(Column_FindAll_IntMax){
	Column c;
	Column r;

	const int64_t value = 4300000003ULL;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		// 64 bitwidth
		c.Add(4300000000ULL);
		c.Add(4300000001ULL);
		c.Add(4300000002ULL);
		c.Add(4300000003ULL);
	}

	c.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < c.Size()){
		if(c.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	c.Destroy();
	r.Destroy();
}

TEST(Column_FindHamming) {
	Column col;
	for (size_t i = 0; i < 10; ++i) {
		col.Add(0x5555555555555555LL);
		col.Add(0x3333333333333333LL);
	}

	Column res;
	col.FindAllHamming(res, 0x3333333333333332LL, 2);

	CHECK_EQUAL(10, res.Size()); // Half should match

	// Clean up
	col.Destroy();
	res.Destroy();
}

TEST(Column_Sum) {
	Column c;
	int64_t sum = 0;

	// Sum of 0 elements
	CHECK_EQUAL(0, c.Sum());

	// Sum of 1 elements
	c.Add(123);
	CHECK_EQUAL(123, c.Sum());
	
	c.Clear();

	for(int i = 0; i < 100; i++) {
		c.Add(i);
	}

	// Sum of entire range, using default args
	sum = 0;
	for(int i = 0; i < 100; i++) {
		sum += c.Get(i);
	}
	CHECK_EQUAL(sum, c.Sum());

	// Sum of entire range, given explicit range
	sum = 0;
	for(int i = 0; i < 100; i++) {
		sum += c.Get(i);
	}
	CHECK_EQUAL(sum, c.Sum(0, 100));

	// Start to N
	sum = 0;
	for(int i = 0; i < 63; i++) {
		sum += c.Get(i);
	}
	CHECK_EQUAL(sum, c.Sum(0, 63));

	// N to end
	sum = 0;
	for(int i = 47; i < 100; i++) {
		sum += c.Get(i);
	}
	CHECK_EQUAL(sum, c.Sum(47, 100));

	// N to M
	sum = 0;
	for(int i = 55; i < 79; i++) {
		sum += c.Get(i);
	}
	CHECK_EQUAL(sum, c.Sum(55, 79));

	c.Destroy();

}

#if TEST_DURATION > 0

TEST(Column_prepend_many) {
	// Test against a "Assertion failed: start < m_len, file src\Array.cpp, line 276" bug
	Column a;

	for (size_t items = 0; items < 3000; ++items) {
		a.Clear();
		for (int j = 0; j < items + 1; ++j) {
			a.Insert(0, j);
		}
		a.Insert(items, 444);
	}
	a.Destroy();
}

#endif