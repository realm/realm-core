#include "tightdb/index_string.hpp"
#include <UnitTest++.h>

using namespace tightdb;

TEST(StringIndex_Test1)
{
    const char s1[] = "John";
    const char s2[] = "Brian";
    const char s3[] = "Samantha";
    const char s4[] = "Tom";
    const char s5[] = "Johnathan";
    const char s6[] = "Johnny";
    
    
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

    ndx.Insert(0, s1);
    ndx.Insert(1, s2);
    ndx.Insert(2, s3);
    ndx.Insert(3, s4);
    ndx.Insert(4, s1);
    ndx.Insert(5, s5);
    ndx.Insert(6, s6);
    
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

