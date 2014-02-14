#include "testsettings.hpp"
#ifdef TEST_ARRAY_BLOB

#include <cstring>

#include <UnitTest++.h>

#include <tightdb/array_blob.hpp>
#include <tightdb/column_string.hpp>

using namespace std;
using namespace tightdb;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

TEST(ArrayBlob_AddEmpty)
{
    ArrayBlob blob;

    blob.add("", 0);

    // Cleanup
    blob.destroy();
}

TEST(ArrayBlob)
{
    ArrayBlob blob;

    const char* const t1 = "aaa";
    const char* const t2 = "bbbbbb";
    const char* const t3 = "ccccccccccc";
    const char* const t4 = "xxx";
    const size_t l1 = strlen(t1)+1;
    const size_t l2 = strlen(t2)+1;
    const size_t l3 = strlen(t3)+1;

    // Test add
    blob.add(t1, l1);
    blob.add(t2, l2);
    blob.add(t3, l3);

    CHECK_EQUAL(t1, blob.get(0));
    CHECK_EQUAL(t2, blob.get(l1));
    CHECK_EQUAL(t3, blob.get(l1 + l2));

    // Test insert
    blob.insert(0, t3, l3);
    blob.insert(l3, t2, l2);

    CHECK_EQUAL(t3, blob.get(0));
    CHECK_EQUAL(t2, blob.get(l3));
    CHECK_EQUAL(t1, blob.get(l3 + l2));
    CHECK_EQUAL(t2, blob.get(l3 + l2 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l2 + l1 + l2));

    // Test replace
    blob.replace(l3, l3 + l2, t1, l1); // replace with smaller
    blob.replace(l3 + l1 + l1, l3 + l1 + l1 + l2, t3, l3); // replace with bigger
    blob.replace(l3 + l1, l3 + l1 + l1, t4, l1); // replace with same

    CHECK_EQUAL(t3, blob.get(0));
    CHECK_EQUAL(t1, blob.get(l3));
    CHECK_EQUAL(t4, blob.get(l3 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l1 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l1 + l1 + l3));

    // Test delete
    blob.erase(0, l3);                 // top
    blob.erase(l1, l1 + l1);           // middle
    blob.erase(l1 + l3, l1 + l3 + l3); // bottom

    CHECK_EQUAL(t1, blob.get(0));
    CHECK_EQUAL(t3, blob.get(l1));
    CHECK_EQUAL(l1 + l3, blob.size());

    // Delete all
    blob.erase(0, l1 + l3);
    CHECK(blob.is_empty());

    // Cleanup
    blob.destroy();
}

TEST(AdaptiveStringLeak)
{
    AdaptiveStringColumn col;
    for (size_t i = 0; i != 2 * TIGHTDB_MAX_LIST_SIZE; ++i)
        col.insert(0, string(100, 'a'));  // use constant larger than 'medium_string_max_size'

    col.destroy();
}

#endif // TEST_ARRAY_BLOB
