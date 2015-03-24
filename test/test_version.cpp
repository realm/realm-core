#include <string>
#include <iostream>

#include <realm/version.hpp>

#include "test.hpp"

using namespace std;
using namespace realm;


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


TEST(Version_General)
{
    CHECK_EQUAL(REALM_VER_MAJOR, Version::get_major());
    CHECK_EQUAL(REALM_VER_MINOR, Version::get_minor());
    CHECK_EQUAL(REALM_VER_PATCH, Version::get_patch());
    CHECK_EQUAL(REALM_VER_PATCH, Version::get_patch());

    CHECK_EQUAL(true, Version::is_at_least(0,0,0));
    CHECK_EQUAL(true, Version::is_at_least(0,1,5));
    CHECK_EQUAL(true, Version::is_at_least(0,1,6));
    // Below might have to be updated when the version is incremented
    CHECK_EQUAL(true, Version::is_at_least(0,1,9));
    CHECK_EQUAL(false, Version::is_at_least(1,0,0));
    CHECK_EQUAL(true, Version::is_at_least(0,2,0));
}
