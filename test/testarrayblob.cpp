#include "ArrayBlob.hpp"
#include <UnitTest++.h>

using namespace tightdb;

TEST(ArrayBlob) {
	ArrayBlob blob;

	const char* const t1 = "aaa";
	const char* const t2 = "bbbbbb";
	const char* const t3 = "ccccccccccc";
	const char* const t4 = "xxx";
	const size_t l1 = strlen(t1)+1;
	const size_t l2 = strlen(t2)+1;
	const size_t l3 = strlen(t3)+1;

	// Test add
	blob.Add((uint8_t*)t1, l1);
	blob.Add((uint8_t*)t2, l2);
	blob.Add((uint8_t*)t3, l3);

	CHECK_EQUAL(t1, (const char*)blob.Get(0));
	CHECK_EQUAL(t2, (const char*)blob.Get(l1));
	CHECK_EQUAL(t3, (const char*)blob.Get(l1 + l2));

	// Test insert
	blob.Insert(0, (uint8_t*)t3, l3);
	blob.Insert(l3, (uint8_t*)t2, l2);

	CHECK_EQUAL(t3, (const char*)blob.Get(0));
	CHECK_EQUAL(t2, (const char*)blob.Get(l3));
	CHECK_EQUAL(t1, (const char*)blob.Get(l3 + l2));
	CHECK_EQUAL(t2, (const char*)blob.Get(l3 + l2 + l1));
	CHECK_EQUAL(t3, (const char*)blob.Get(l3 + l2 + l1 + l2));

	// Test replace
	blob.Replace(l3, l3 + l2, (uint8_t*)t1, l1); // replace with smaller
	blob.Replace(l3 + l1 + l1, l3 + l1 + l1 + l2, (uint8_t*)t3, l3); // replace with bigger
	blob.Replace(l3 + l1, l3 + l1 + l1, (uint8_t*)t4, l1); // replace with same

	CHECK_EQUAL(t3, (const char*)blob.Get(0));
	CHECK_EQUAL(t1, (const char*)blob.Get(l3));
	CHECK_EQUAL(t4, (const char*)blob.Get(l3 + l1));
	CHECK_EQUAL(t3, (const char*)blob.Get(l3 + l1 + l1));
	CHECK_EQUAL(t3, (const char*)blob.Get(l3 + l1 + l1 + l3));

	// Test delete
	blob.Delete(0, l3);                 // top
	blob.Delete(l1, l1 + l1);           // middle
	blob.Delete(l1 + l3, l1 + l3 + l3); // bottom

	CHECK_EQUAL(t1, (const char*)blob.Get(0));
	CHECK_EQUAL(t3, (const char*)blob.Get(l1));
	CHECK_EQUAL(l1 + l3, blob.Size());

	// Delete all
	blob.Delete(0, l1 + l3);
	CHECK(blob.IsEmpty());

	// Cleanup
	blob.Destroy();
}
