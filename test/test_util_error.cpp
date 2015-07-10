#include "testsettings.hpp"
#ifdef TEST_UTIL_ERROR

#include <realm/util/basic_system_errors.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

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

TEST(BasicSystemErrors_Category)
{
    std::error_code err = make_error_code(error::operation_aborted);
    CHECK_EQUAL(err.category().name(), "realm.basic_system");
}


TEST(BasicSystemErrors_Messages)
{
    {
        std::error_code err = make_error_code(error::address_family_not_supported);
        CHECK_GREATER(std::strlen(err.message().c_str()), 0);
        CHECK(err.message() != "Unknown error");
    }
    {
        std::error_code err = make_error_code(error::invalid_argument);
        CHECK_GREATER(std::strlen(err.message().c_str()), 0);
        CHECK(err.message() != "Unknown error");
    }
    {
        std::error_code err = make_error_code(error::no_memory);
        CHECK_GREATER(std::strlen(err.message().c_str()), 0);
        CHECK(err.message() != "Unknown error");
    }
    {
        std::error_code err = make_error_code(error::operation_aborted);
        CHECK_GREATER(std::strlen(err.message().c_str()), 0);
        CHECK(err.message() != "Unknown error");
    }

    // Ensure that if we pass an unknown error code, we get some error reporting
    // This may potentially pass on some operating system. If this test starts failing, simply change the
    // magic number below.
    {
        std::error_code err = make_error_code(static_cast<error::basic_system_errors>(64532));
        CHECK_EQUAL(err.message(), "Unknown error");
    }
}

#endif // TEST_BASIC_SYSTEM_ERRORS
