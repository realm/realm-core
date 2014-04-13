#include "testsettings.hpp"
#ifdef TEST_COLUMN_BASIC

#include <tightdb/column_basic.hpp>

#include "test.hpp"

using namespace tightdb;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


TEST(ColumnBasic_LowerUpperBound)
{
    // Create column with sorted members
    BasicColumn<int> col;
    col.add(5);
    for (int i = 5; i < 100; i += 5) {
        col.add(i);
    }

    // before first entry
    CHECK_EQUAL(0, col.lower_bound(0));
    CHECK_EQUAL(0, col.upper_bound(0));

    // first entry (duplicate)
    CHECK_EQUAL(0, col.lower_bound(5));
    CHECK_EQUAL(2, col.upper_bound(5));

    // middle entry
    CHECK_EQUAL(10, col.lower_bound(50));
    CHECK_EQUAL(11, col.upper_bound(50));

    // non-existent middle entry
    CHECK_EQUAL(11, col.lower_bound(52));
    CHECK_EQUAL(11, col.upper_bound(52));

    // last entry
    CHECK_EQUAL(19, col.lower_bound(95));
    CHECK_EQUAL(20, col.upper_bound(95));

    // beyond last entry
    CHECK_EQUAL(20, col.lower_bound(96));
    CHECK_EQUAL(20, col.upper_bound(96));

    // Clean up
    col.destroy();
}

#endif // TEST_COLUMN_BASIC
