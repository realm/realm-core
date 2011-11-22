
#include "Array.h"
#include "Column.h"
#include <UnitTest++.h>
#include <vector>
#include <algorithm>
#include "testsettings.h"
#include <map>
#include <string>

struct db_setup_array {
	static Array c;
};

Array db_setup_array::c;

// Pre-declare local functions
uint64_t rand2(void);
template<class T, class U> bool vector_eq_array(const std::vector<T>& v, const U& a);
template<class T, class U> bool findall_test(std::vector<T>& v, U& a, T val);
template<class T> std::vector<size_t> findall_vector(std::vector<T>& v, T val);

TEST_FIXTURE(db_setup_array, Array_Add0) {
	c.Add(0);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Size(), (size_t)1);
	CHECK_EQUAL(0, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add1) {
	c.Add(1);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Size(), 2);
	CHECK_EQUAL(1, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add2) {
	c.Add(2);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Size(), 3);
	CHECK_EQUAL(2, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add3) {
	c.Add(3);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(2, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add4) {
	c.Add(4);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Size(), 5);
	CHECK_EQUAL(4, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add5) {
	c.Add(16);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Size(), 6);
	CHECK_EQUAL(8, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add6) {
	c.Add(256);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Get(6), 256);
	CHECK_EQUAL(c.Size(), 7);
	CHECK_EQUAL(16, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add7) {
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
	CHECK_EQUAL(32, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Add8) {
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
	CHECK_EQUAL(64, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_AddNeg1) {
	c.Clear();
	c.Add(-1);

	CHECK_EQUAL(c.Size(), 1);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(8, c.GetBitWidth());
}

TEST(Array_AddNeg1_1) {
	Array c;

	c.Add(1);
	c.Add(2);
	c.Add(3);
	c.Add(-128);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), 1);
	CHECK_EQUAL(c.Get(1), 2);
	CHECK_EQUAL(c.Get(2), 3);
	CHECK_EQUAL(c.Get(3), -128);
	CHECK_EQUAL(8, c.GetBitWidth());

	// Cleanup
	c.Destroy();
}

TEST_FIXTURE(db_setup_array, Array_AddNeg2) {
	c.Add(-256);

	CHECK_EQUAL(c.Size(), 2);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
	CHECK_EQUAL(16, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_AddNeg3) {
	c.Add(-65536);

	CHECK_EQUAL(c.Size(), 3);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
	CHECK_EQUAL(c.Get(2), -65536);
	CHECK_EQUAL(32, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_AddNeg4) {
	c.Add(-4294967296);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
	CHECK_EQUAL(c.Get(2), -65536);
	CHECK_EQUAL(c.Get(3), -4294967296LL);
	CHECK_EQUAL(64, c.GetBitWidth());
}

TEST_FIXTURE(db_setup_array, Array_Set) {
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

TEST_FIXTURE(db_setup_array, Array_Insert1) {
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

TEST_FIXTURE(db_setup_array, Array_Insert2) {
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

TEST_FIXTURE(db_setup_array, Array_Insert3) {
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
TEST_FIXTURE(db_setup_array, Array_Index1) {
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
TEST_FIXTURE(db_setup_array, Array_Delete1) {
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

TEST_FIXTURE(db_setup_array, Array_Delete2) {
	// Delete from top
	c.Delete(0);

	CHECK_EQUAL(c.Size(), 5);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 65536);
}

TEST_FIXTURE(db_setup_array, Array_Delete3) {
	// Delete from bottom
	c.Delete(4);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
}

TEST_FIXTURE(db_setup_array, Array_DeleteAll) {
	// Delete all items one at a time
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);

	CHECK(c.IsEmpty());
	CHECK_EQUAL(0, c.Size());
}

TEST_FIXTURE(db_setup_array, Array_Find1) {
	// Look for a non-existing value
	size_t res = c.Find(10);

	CHECK_EQUAL(res, -1);
}

TEST_FIXTURE(db_setup_array, Array_Find2) {
	// zero-bit width
	c.Clear();
	c.Add(0);
	c.Add(0);

	size_t res = c.Find(0);
	CHECK_EQUAL(res, 0);
}

TEST_FIXTURE(db_setup_array, Array_Find3) {
	// expand to 1-bit width
	c.Add(1);

	size_t res = c.Find(1);
	CHECK_EQUAL(res, 2);
}

TEST_FIXTURE(db_setup_array, Array_Find4) {
	// expand to 2-bit width
	c.Add(2);

	size_t res = c.Find(2);
	CHECK_EQUAL(res, 3);
}

TEST_FIXTURE(db_setup_array, Array_Find5) {
	// expand to 4-bit width
	c.Add(4);

	size_t res = c.Find(4);
	CHECK_EQUAL(res, 4);
}

TEST_FIXTURE(db_setup_array, Array_Find6) {
	// expand to 8-bit width
	c.Add(16);

	// Add some more to make sure we
	// can search in 64bit chunks
	c.Add(16);
	c.Add(7);

	size_t res = c.Find(7);
	CHECK_EQUAL(7, res);
}

TEST_FIXTURE(db_setup_array, Array_Find7) {
	// expand to 16-bit width
	c.Add(256);

	size_t res = c.Find(256);
	CHECK_EQUAL(8, res);
}

TEST_FIXTURE(db_setup_array, Array_Find8) {
	// expand to 32-bit width
	c.Add(65536);

	size_t res = c.Find(65536);
	CHECK_EQUAL(9, res);
}

TEST_FIXTURE(db_setup_array, Array_Find9) {
	// expand to 64-bit width
	c.Add(4294967296);

	size_t res = c.Find(4294967296LL);
	CHECK_EQUAL(10, res);
}

/* Partial find is not fully implemented yet
#define PARTIAL_COUNT 100
TEST_FIXTURE(db_setup_array, Array_PartialFind1) {
	c.Clear();

	for (size_t i = 0; i < PARTIAL_COUNT; ++i) {
		c.Add(i);
	}

	CHECK_EQUAL(-1, c.Find(PARTIAL_COUNT+1, 0, PARTIAL_COUNT));
	CHECK_EQUAL(-1, c.Find(0, 1, PARTIAL_COUNT));
	CHECK_EQUAL(PARTIAL_COUNT-1, c.Find(PARTIAL_COUNT-1, PARTIAL_COUNT-1, PARTIAL_COUNT));
}*/

TEST_FIXTURE(db_setup_array, Array_Destroy) {
	// clean up (ALWAYS PUT THIS LAST)
	c.Destroy();
}

TEST(Array_Sort) {
	// Create Array with random values
	Array a;
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

TEST(findallint0){
	Array a;
	Column r;

	const int value = 0;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		a.Add(0);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

TEST(findallint1){
	Array a;
	Column r;

	const int value = 1;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		a.Add(0);
		a.Add(0);
		a.Add(1);
		a.Add(0);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

TEST(findallint2){
	Array a;
	Column r;

	const int value = 3;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		a.Add(0);
		a.Add(1);
		a.Add(2);
		a.Add(3);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

TEST(findallint3){
	Array a;
	Column r;

	const int value = 10;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		a.Add(10);
		a.Add(11);
		a.Add(12);
		a.Add(13);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

TEST(findallint4){
	Array a;
	Column r;

	const int value = 20;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		// 8 bitwidth
		a.Add(20);
		a.Add(21);
		a.Add(22);
		a.Add(23);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

TEST(findallint5){
	Array a;
	Column r;

	const int value = 303;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		// 16 bitwidth
		a.Add(300);
		a.Add(301);
		a.Add(302);
		a.Add(303);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

TEST(findallint6){
	Array a;
	Column r;

	const int value = 70000;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		// 32 bitwidth
		a.Add(70000);
		a.Add(70001);
		a.Add(70002);
		a.Add(70003);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

TEST(findallint7){
	Array a;
	Column r;

	const int64_t value = 4300000003ULL;
	const int vReps = 5;

	for(size_t i = 0; i < vReps; i++){
		// 64 bitwidth
		a.Add(4300000000ULL);
		a.Add(4300000001ULL);
		a.Add(4300000002ULL);
		a.Add(4300000003ULL);
	}

	a.FindAll(r, value);
	CHECK_EQUAL(vReps, r.Size());

	size_t i = 0;
	size_t j = 0;
	while(i < a.Size()){
		if(a.Get(i) == value)
			CHECK_EQUAL(i, r.Get(j++));
		i += 1;
	}

	// Cleanup
	a.Destroy();
	r.Destroy();
}

void hasZeroByte(int64_t value, size_t reps)
{
	Array a;
	Column r;

	for(size_t i = 0; i < reps - 1; i++){
		a.Add(value);
	}

	a.Add(0);

	size_t t = a.Find(0);
	CHECK_EQUAL(a.Size() - 1, t);

	r.Clear();
	a.FindAll(r, 0);
	CHECK_EQUAL(a.Size() - 1, r.Get(0));

	// Cleanup
	a.Destroy();
	r.Destroy();
}

// Tests the case where a value does *not* exist in one entire 64-bit chunk (triggers the 'if (hasZeroByte) break;' condition)
TEST(FindhasZeroByte)
{
	// we want at least 1 entire 64-bit chunk-test, and we also want a remainder-test, so we chose n to be a prime > 64
	size_t n = 73;
	hasZeroByte(1, n); // width = 1
	hasZeroByte(3, n); // width = 2
	hasZeroByte(13, n); // width = 4
	hasZeroByte(100, n); // 8
	hasZeroByte(10000, n); // 16
	hasZeroByte(100000, n); // 32
	hasZeroByte(8000000000LL, n); // 64
}




// Support functions for monkey test

uint64_t rand2(void) {
	const uint64_t i = (int64_t)rand() | (uint64_t)rand() << 8 | (uint64_t)rand() << 2*8 | (uint64_t)rand() << 3*8 | (uint64_t)rand() << 4*8 | (uint64_t)rand() << 5*8 | (uint64_t)rand() << 6*8 | (uint64_t)rand() << 7*8;
	return i;
}

template<class T, class U> bool vector_eq_array(const std::vector<T>& v, const U& a) {
	if (a.Size() != v.size()) return false;

	for(size_t t = 0; t < v.size(); ++t) {
		if (v[t] != a.Get(t)) return false;
	}
	return true;
}

template<class T> std::vector<size_t> findall_vector(std::vector<T>& v, T val) {
	std::vector<int64_t>::iterator it = v.begin();
	std::vector<size_t> results;
	while(it != v.end()) {
		it = std::find(it, v.end(), val);
		size_t index = std::distance(v.begin(), it);
		if(index < v.size())
		{
			results.push_back(index);
			it++;
		}
	}
	return results;
}
	
template<class T, class U> bool findall_test(std::vector<T>& v, U& a, T val) {
	std::vector<size_t> results;
	results = findall_vector(v, val);

	// sanity test - in the beginning, results.size() == v.size() (all elements are 0), later results.size() < v.size()
//	if(rand2() % 100 == 0)
//		printf("%d out of %d\n", (int)results.size(), (int)v.size()); 
	
	Column c;
	a.FindAll(c, val);

	const bool res = vector_eq_array(results, c);

	// Cleanup
	c.Destroy();

	return res;
}


TEST(monkeytest1) {
	const uint64_t DURATION = UNITTEST_DURATION*1000;
	const uint64_t SEED = 123;

	Array a;
	std::vector<int64_t> v;

	srand(SEED);
	const uint64_t nums_per_bitwidth = DURATION;
	size_t current_bitwidth = 0;
	unsigned int trend = 5;

	for(current_bitwidth = 0; current_bitwidth < 65; current_bitwidth++) {
		//		printf("Input bitwidth around ~%d, a.GetBitWidth()=%d, a.Size()=%d\n", (int)current_bitwidth, (int)a.GetBitWidth(), (int)a.Size());

		current_bitwidth = current_bitwidth;

		while(rand2() % nums_per_bitwidth != 0) {
			if (!(rand2() % (DURATION / 10)))
				trend = (unsigned int)rand2() % 10;

			// Sanity test
/*			if(rand2() % 1000 == 0)	{
				for(int j = 0; j < v.size(); j++)
					printf("%lld ", v[j]);
				printf("%d\n", v.size());
			}*/


			if (rand2() % 10 > trend) {
				// Insert
				uint64_t l = rand2();
				const uint64_t mask = ((1ULL << current_bitwidth) - 1ULL);
				l &= mask;

				const size_t pos = rand2() % (a.Size() + 1);
				a.Insert(pos, l);
				v.insert(v.begin() + pos, l);
			}

			else {
				// Delete
				if(a.Size() > 0) {
					const size_t i = rand2() % a.Size();
					a.Delete(i);
					v.erase(v.begin() + i);
				}
			}


			// Verify
			if(rand2() % 100 == 0) {
				bool b = vector_eq_array(v, a);
				CHECK_EQUAL(true, b);
				if(a.Size() > 0) {
					b = findall_test(v, a, a.Get(rand2() % a.Size()));
					CHECK_EQUAL(true, b);
				}
			}
		}
	}

	// Cleanup
	a.Destroy();
}




