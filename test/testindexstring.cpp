#include "tightdb/index_string.hpp"
#include <UnitTest++.h>

using namespace tightdb;

namespace {
    // strings used by tests
    const char s1[] = "John";
    const char s2[] = "Brian";
    const char s3[] = "Samantha";
    const char s4[] = "Tom";
    const char s5[] = "Johnathan";
    const char s6[] = "Johnny";
    const char s7[] = "Sam";
    
} // namespace

TEST(StringIndex_BuildIndex)
{
    // Create a column with string values
    AdaptiveStringColumn col;
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix
    
    // Create a new index on column
    StringIndex ndx(col);
    ndx.BuildIndex();
    
    const size_t r1 = ndx.find_first(s1);
    const size_t r2 = ndx.find_first(s2);
    const size_t r3 = ndx.find_first(s3);
    const size_t r4 = ndx.find_first(s4);
    const size_t r5 = ndx.find_first(s5);
    const size_t r6 = ndx.find_first(s6);
    
    CHECK_EQUAL(0, r1);
    CHECK_EQUAL(1, r2);
    CHECK_EQUAL(2, r3);
    CHECK_EQUAL(3, r4);
    CHECK_EQUAL(5, r5);
    CHECK_EQUAL(6, r6);
    
    // Clean up
    col.Destroy();
    ndx.Destroy();
}

TEST(StringIndex_DeleteAll)
{
    // Create a column with string values
    AdaptiveStringColumn col;
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Create a new index on column
    StringIndex ndx(col);
    ndx.BuildIndex();

    // Delete all entries
    // (reverse order to avoid ref updates)
    ndx.Delete(6, s6, true);
    ndx.Delete(5, s5, true);
    ndx.Delete(4, s1, true);
    ndx.Delete(3, s4, true);
    ndx.Delete(2, s3, true);
    ndx.Delete(1, s2, true);
    ndx.Delete(0, s1, true);
#ifdef _DEBUG
    CHECK(ndx.is_empty());
#endif

    // Re-insert values
    ndx.BuildIndex();

    // Delete all entries
    // (in order to force constant ref updating)
    ndx.Delete(0, s1);
    ndx.Delete(0, s2);
    ndx.Delete(0, s3);
    ndx.Delete(0, s4);
    ndx.Delete(0, s1);
    ndx.Delete(0, s5);
    ndx.Delete(0, s6);
#ifdef _DEBUG
    CHECK(ndx.is_empty());
#endif

    // Clean up
    col.Destroy();
    ndx.Destroy();
}

TEST(StringIndex_Delete)
{
    // Create a column with random values
    AdaptiveStringColumn col;
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    StringIndex ndx(col);
    ndx.BuildIndex();

    // Delete first item (in index)
    col.Delete(1);
    ndx.Delete(1, s2); // opt for last item

    CHECK_EQUAL(0, ndx.find_first(s1));
    CHECK_EQUAL(1, ndx.find_first(s3));
    CHECK_EQUAL(2, ndx.find_first(s4));
    CHECK_EQUAL(not_found, ndx.find_first(s2));

    // Delete last item (in index)
    col.Delete(2);
    ndx.Delete(2, s4);

    CHECK_EQUAL(0, ndx.find_first(s1));
    CHECK_EQUAL(1, ndx.find_first(s3));
    CHECK_EQUAL(not_found, ndx.find_first(s4));
    CHECK_EQUAL(not_found, ndx.find_first(s2));

    // Delete middle item (in index)
    col.Delete(1);
    ndx.Delete(1, s3);

    CHECK_EQUAL(0, ndx.find_first(s1));
    CHECK_EQUAL(not_found, ndx.find_first(s3));
    CHECK_EQUAL(not_found, ndx.find_first(s4));
    CHECK_EQUAL(not_found, ndx.find_first(s2));

    // Delete all items
    col.Delete(0);
    ndx.Delete(0, s1);
    col.Delete(0);
    ndx.Delete(0, s1);
#ifdef _DEBUG
    CHECK(ndx.is_empty());
#endif

    // Clean up
    col.Destroy();
    ndx.Destroy();
}

TEST(StringIndex_Insert)
{
    // Create a column with random values
    AdaptiveStringColumn col;
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    StringIndex ndx(col);
    ndx.BuildIndex();

    // Insert item in top of column
    col.Insert(0, s5);
    ndx.Insert(0, s5);

    CHECK_EQUAL(0, ndx.find_first(s5));
    CHECK_EQUAL(1, ndx.find_first(s1));
    CHECK_EQUAL(2, ndx.find_first(s2));
    CHECK_EQUAL(3, ndx.find_first(s3));
    CHECK_EQUAL(4, ndx.find_first(s4));
    //CHECK_EQUAL(5, ndx.find_first(s1)); // duplicate

    // Append item in end of column
    col.Insert(6, s6);
    ndx.Insert(6, s6, true); // opt for last item

    CHECK_EQUAL(0, ndx.find_first(s5));
    CHECK_EQUAL(1, ndx.find_first(s1));
    CHECK_EQUAL(2, ndx.find_first(s2));
    CHECK_EQUAL(3, ndx.find_first(s3));
    CHECK_EQUAL(4, ndx.find_first(s4));
    CHECK_EQUAL(6, ndx.find_first(s6));

    // Insert item in middle
    col.Insert(3, s7);
    ndx.Insert(3, s7);

    CHECK_EQUAL(0, ndx.find_first(s5));
    CHECK_EQUAL(1, ndx.find_first(s1));
    CHECK_EQUAL(2, ndx.find_first(s2));
    CHECK_EQUAL(3, ndx.find_first(s7));
    CHECK_EQUAL(4, ndx.find_first(s3));
    CHECK_EQUAL(5, ndx.find_first(s4));
    CHECK_EQUAL(7, ndx.find_first(s6));

    // Clean up
    col.Destroy();
    ndx.Destroy();
}

TEST(StringIndex_Set)
{
    // Create a column with random values
    AdaptiveStringColumn col;
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    StringIndex ndx(col);
    ndx.BuildIndex();

    // Set top value
    col.Set(0, s5);
    ndx.Set(0, s1, s5);

    CHECK_EQUAL(0, ndx.find_first(s5));
    CHECK_EQUAL(1, ndx.find_first(s2));
    CHECK_EQUAL(2, ndx.find_first(s3));
    CHECK_EQUAL(3, ndx.find_first(s4));
    CHECK_EQUAL(4, ndx.find_first(s1));

    // Set bottom value
    ndx.Set(4, s1, s6);

    CHECK_EQUAL(not_found, ndx.find_first(s1));
    CHECK_EQUAL(0, ndx.find_first(s5));
    CHECK_EQUAL(1, ndx.find_first(s2));
    CHECK_EQUAL(2, ndx.find_first(s3));
    CHECK_EQUAL(3, ndx.find_first(s4));
    CHECK_EQUAL(4, ndx.find_first(s6));

    // Set middle value
    ndx.Set(2, s3, s7);

    CHECK_EQUAL(not_found, ndx.find_first(s3));
    CHECK_EQUAL(not_found, ndx.find_first(s1));
    CHECK_EQUAL(0, ndx.find_first(s5));
    CHECK_EQUAL(1, ndx.find_first(s2));
    CHECK_EQUAL(2, ndx.find_first(s7));
    CHECK_EQUAL(3, ndx.find_first(s4));
    CHECK_EQUAL(4, ndx.find_first(s6));

    // Clean up
    col.Destroy();
    ndx.Destroy();
}
