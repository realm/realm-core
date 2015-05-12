#include <cstring>
#include <algorithm>

#include <memory>

#include "test.hpp"

using namespace realm::util;
using namespace realm::test_util::unit_test;


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


namespace {

TestList zero_tests_list, zero_checks_list;

TEST_EX(ZeroChecks, zero_checks_list, true)
{
}


TestList one_check_success_list, one_check_failure_list;

TEST_EX(OneCheckSuccess, one_check_success_list, true)
{
    CHECK(true);
}

TEST_EX(OneCheckFailure, one_check_failure_list, true)
{
    CHECK(false);
}


TestList one_test_success_list, one_test_failure_list;

TEST_EX(OneTestSuccess, one_test_success_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK(true); // <--- Success
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(OneTestFailure, one_test_failure_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK(false); // <--- Failure
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}


TestList few_tests_success_list, few_tests_failure_list;

TEST_EX(FewTestsSuccess_1, few_tests_success_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(FewTestsSuccess_2, few_tests_success_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK(true); // <--- Success
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(FewTestsSuccess_3, few_tests_success_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(FewTestsFailure_1, few_tests_failure_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(FewTestsFailure_2, few_tests_failure_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK(false); // <--- Failure
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(FewTestsFailure_3, few_tests_failure_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}


TestList mixed_list;

TEST_EX(Mixed_1_X, mixed_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(Mixed_2_Y, mixed_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_EQUAL(0,1);         // <--- Failure
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}

TEST_EX(Mixed_3_X, mixed_list, true)
{
}

TEST_EX(Mixed_4_Y, mixed_list, true)
{
    CHECK_NOT_EQUAL(0,0);     // <--- Failure
    CHECK_EQUAL(0,1);         // <--- Failure
    CHECK_GREATER_EQUAL(0,1); // <--- Failure
}

TEST_EX(Mixed_5_X, mixed_list, true)
{
    CHECK_NOT_EQUAL(0,0);     // <--- Failure
    CHECK_NOT_EQUAL(0,1);
    CHECK_GREATER_EQUAL(0,1); // <--- Failure
    CHECK_GREATER(1,0);
}

TEST_EX(Mixed_6_Y, mixed_list, true)
{
}

TEST_EX(Mixed_7_Y, mixed_list, true)
{
    CHECK_EQUAL(0,0);
    CHECK_NOT_EQUAL(0,1);
    CHECK_LESS(0,1);
    CHECK_GREATER(1,0);
}


TestList success_list, failure_list;

TEST_EX(Success_Bool, success_list, true) // Test #1, accum checks = 0 + 13 = 13
{
    CHECK(true);
    CHECK_EQUAL(false, false);
    CHECK_EQUAL(true, true);
    CHECK_NOT_EQUAL(false, true);
    CHECK_NOT_EQUAL(true, false);
    CHECK_LESS(false, true);
    CHECK_GREATER(true, false);
    CHECK_LESS_EQUAL(false, false);
    CHECK_LESS_EQUAL(false, true);
    CHECK_LESS_EQUAL(true, true);
    CHECK_GREATER_EQUAL(false, false);
    CHECK_GREATER_EQUAL(true, false);
    CHECK_GREATER_EQUAL(true, true);
}

TEST_EX(Failure_Bool, failure_list, true) // Test #1, accum checks = 0 + 13 = 13
{
    CHECK(false);
    CHECK_EQUAL(false, true);
    CHECK_EQUAL(true, false);
    CHECK_NOT_EQUAL(false, false);
    CHECK_NOT_EQUAL(true, true);
    CHECK_LESS(false, false);
    CHECK_LESS(true, false);
    CHECK_LESS(true, true);
    CHECK_GREATER(false, false);
    CHECK_GREATER(false, true);
    CHECK_GREATER(true, true);
    CHECK_LESS_EQUAL(true, false);
    CHECK_GREATER_EQUAL(false, true);
}

TEST_EX(Success_Int, success_list, true) // Test #2, accum checks = 13 + 12 = 25
{
    CHECK_EQUAL(1,1);
    CHECK_EQUAL(2,2);
    CHECK_NOT_EQUAL(1,2);
    CHECK_NOT_EQUAL(2,1);
    CHECK_LESS(1,2);
    CHECK_GREATER(2,1);
    CHECK_LESS_EQUAL(1,1);
    CHECK_LESS_EQUAL(1,2);
    CHECK_LESS_EQUAL(2,2);
    CHECK_GREATER_EQUAL(1,1);
    CHECK_GREATER_EQUAL(2,1);
    CHECK_GREATER_EQUAL(2,2);
}

TEST_EX(Failure_Int, failure_list, true) // Test #2, accum checks = 13 + 12 = 25
{
    CHECK_EQUAL(1,2);
    CHECK_EQUAL(2,1);
    CHECK_NOT_EQUAL(1,1);
    CHECK_NOT_EQUAL(2,2);
    CHECK_LESS(1,1);
    CHECK_LESS(2,1);
    CHECK_LESS(2,2);
    CHECK_GREATER(1,1);
    CHECK_GREATER(1,2);
    CHECK_GREATER(2,2);
    CHECK_LESS_EQUAL(2,1);
    CHECK_GREATER_EQUAL(1,2);
}

TEST_EX(Success_Float, success_list, true) // Test #3, accum checks = 25 + 32 = 57
{
    CHECK_EQUAL(3.1, 3.1);
    CHECK_EQUAL(3.2, 3.2);
    CHECK_NOT_EQUAL(3.1, 3.2);
    CHECK_NOT_EQUAL(3.2, 3.1);
    CHECK_LESS(3.1, 3.2);
    CHECK_GREATER(3.2, 3.1);
    CHECK_LESS_EQUAL(3.1, 3.1);
    CHECK_LESS_EQUAL(3.1, 3.2);
    CHECK_LESS_EQUAL(3.2, 3.2);
    CHECK_GREATER_EQUAL(3.1, 3.1);
    CHECK_GREATER_EQUAL(3.2, 3.1);
    CHECK_GREATER_EQUAL(3.2, 3.2);

    double eps = 0.5;
    CHECK_APPROXIMATELY_EQUAL(+0.00, +0.00, eps); // Max error = 0.0
    CHECK_APPROXIMATELY_EQUAL(+1.00, +1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(+0.51, +1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(-1.00, -1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(-1.00, -0.51, eps); // Max error = 0.5

    CHECK_ESSENTIALLY_EQUAL(+0.00, +0.00, eps);   // Max error = 0.0
    CHECK_ESSENTIALLY_EQUAL(+1.00, +1.00, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(+1.00, +1.49, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(-1.00, -1.00, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(-1.49, -1.00, eps);   // Max error = 0.5

    CHECK_DEFINITELY_LESS(-1.00, +1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(+0.00, +1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(+0.49, +1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(-1.00, -0.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(-1.00, -0.49, eps);     // Min error = 0.5

    CHECK_DEFINITELY_GREATER(+1.00, -1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(+1.00, +0.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(+1.00, +0.49, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(-0.00, -1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(-0.49, -1.00, eps);  // Min error = 0.5
}

TEST_EX(Failure_Float, failure_list, true) // Test #3, accum checks = 25 + 52 = 77
{
    CHECK_EQUAL(3.1, 3.2);
    CHECK_EQUAL(3.2, 3.1);
    CHECK_NOT_EQUAL(3.1, 3.1);
    CHECK_NOT_EQUAL(3.2, 3.2);
    CHECK_LESS(3.1, 3.1);
    CHECK_LESS(3.2, 3.1);
    CHECK_LESS(3.2, 3.2);
    CHECK_GREATER(3.1, 3.1);
    CHECK_GREATER(3.1, 3.2);
    CHECK_GREATER(3.2, 3.2);
    CHECK_LESS_EQUAL(3.2, 3.1);
    CHECK_GREATER_EQUAL(3.1, 3.2);

    double eps = 0.5;
    CHECK_APPROXIMATELY_EQUAL(-1.00, +1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(+0.00, +1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(+0.49, +1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(-1.00, -0.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(-1.00, -0.49, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(+1.00, -1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(+1.00, +0.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(+1.00, +0.49, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(-0.00, -1.00, eps); // Max error = 0.5
    CHECK_APPROXIMATELY_EQUAL(-0.49, -1.00, eps); // Max error = 0.5

    CHECK_ESSENTIALLY_EQUAL(-1.00, +1.00, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(+0.00, +1.00, eps);   // Max error = 0.0
    CHECK_ESSENTIALLY_EQUAL(+1.00, +1.51, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(-1.00, -0.00, eps);   // Max error = 0.0
    CHECK_ESSENTIALLY_EQUAL(-1.51, -1.00, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(+1.00, -1.00, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(+1.00, +0.00, eps);   // Max error = 0.0
    CHECK_ESSENTIALLY_EQUAL(+1.51, +1.00, eps);   // Max error = 0.5
    CHECK_ESSENTIALLY_EQUAL(-0.00, -1.00, eps);   // Max error = 0.0
    CHECK_ESSENTIALLY_EQUAL(-1.00, -1.51, eps);   // Max error = 0.5

    CHECK_DEFINITELY_LESS(+0.00, +0.00, eps);     // Min error = 0.0
    CHECK_DEFINITELY_LESS(+1.00, +1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(+0.51, +1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(-1.00, -1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(-1.00, -0.51, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(+1.00, -1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(+1.00, +0.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(+1.00, +0.49, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(-0.00, -1.00, eps);     // Min error = 0.5
    CHECK_DEFINITELY_LESS(-0.49, -1.00, eps);     // Min error = 0.5

    CHECK_DEFINITELY_GREATER(+0.00, +0.00, eps);  // Min error = 0.0
    CHECK_DEFINITELY_GREATER(+1.00, +1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(+0.51, +1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(-1.00, -1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(-1.00, -0.51, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(-1.00, +1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(+0.00, +1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(+0.49, +1.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(-1.00, -0.00, eps);  // Min error = 0.5
    CHECK_DEFINITELY_GREATER(-1.00, -0.49, eps);  // Min error = 0.5
}

TEST_EX(Success_String, success_list, true) // Test #4, accum checks = 57 + 16 = 73
{
    const char* s_1 = "";
    const char* s_2 = "x";
    CHECK_EQUAL(s_1, s_1);
    CHECK_EQUAL(s_2, s_2);
    CHECK_NOT_EQUAL(s_1, s_2);
    CHECK_NOT_EQUAL(s_2, s_1);
    CHECK_LESS(s_1, s_2);
    CHECK_GREATER(s_2, s_1);
    CHECK_LESS_EQUAL(s_1, s_1);
    CHECK_LESS_EQUAL(s_1, s_2);
    CHECK_LESS_EQUAL(s_2, s_2);
    CHECK_GREATER_EQUAL(s_1, s_1);
    CHECK_GREATER_EQUAL(s_2, s_1);
    CHECK_GREATER_EQUAL(s_2, s_2);

    // Check that we are not comparing pointers
    const char* t = "foo";
    std::unique_ptr<char[]> t_1(new char[strlen(t)+1]);
    std::unique_ptr<char[]> t_2(new char[strlen(t)+1]);
    std::copy(t, t + strlen(t) + 1, t_1.get());
    std::copy(t, t + strlen(t) + 1, t_2.get());
    CHECK_EQUAL(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_1.get()));
    CHECK_EQUAL(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_2.get()));
    CHECK_LESS_EQUAL(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_2.get()));
    CHECK_GREATER_EQUAL(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_2.get()));
}

TEST_EX(Failure_String, failure_list, true) // Test #4, accum checks = 77 + 16 = 93
{
    const char* s_1 = "";
    const char* s_2 = "x";
    CHECK_EQUAL(s_1, s_2);
    CHECK_EQUAL(s_2, s_1);
    CHECK_NOT_EQUAL(s_1, s_1);
    CHECK_NOT_EQUAL(s_2, s_2);
    CHECK_LESS(s_1, s_1);
    CHECK_LESS(s_2, s_1);
    CHECK_LESS(s_2, s_2);
    CHECK_GREATER(s_1, s_1);
    CHECK_GREATER(s_1, s_2);
    CHECK_GREATER(s_2, s_2);
    CHECK_LESS_EQUAL(s_2, s_1);
    CHECK_GREATER_EQUAL(s_1, s_2);

    // Check that we are not comparing pointers
    const char* t = "foo";
    std::unique_ptr<char[]> t_1(new char[strlen(t)+1]);
    std::unique_ptr<char[]> t_2(new char[strlen(t)+1]);
    std::copy(t, t + strlen(t) + 1, t_1.get());
    std::copy(t, t + strlen(t) + 1, t_2.get());
    CHECK_NOT_EQUAL(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_1.get()));
    CHECK_NOT_EQUAL(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_2.get()));
    CHECK_LESS(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_2.get()));
    CHECK_GREATER(const_cast<const char*>(t_1.get()), const_cast<const char*>(t_2.get()));
}

TEST_EX(Success_Pointer, success_list, true) // Test #5, accum checks = 73 + 12 = 85
{
    int i;
    int* p_1 = 0;
    int* p_2 = &i;
    CHECK_EQUAL(p_1, p_1);
    CHECK_EQUAL(p_2, p_2);
    CHECK_NOT_EQUAL(p_1, p_2);
    CHECK_NOT_EQUAL(p_2, p_1);
    CHECK_LESS(p_1, p_2);
    CHECK_GREATER(p_2, p_1);
    CHECK_LESS_EQUAL(p_1, p_1);
    CHECK_LESS_EQUAL(p_1, p_2);
    CHECK_LESS_EQUAL(p_2, p_2);
    CHECK_GREATER_EQUAL(p_1, p_1);
    CHECK_GREATER_EQUAL(p_2, p_1);
    CHECK_GREATER_EQUAL(p_2, p_2);
}

TEST_EX(Failure_Pointer, failure_list, true) // Test #5, accum checks = 93 + 12 = 105
{
    int i;
    int* p_1 = 0;
    int* p_2 = &i;
    CHECK_EQUAL(p_1, p_2);
    CHECK_EQUAL(p_2, p_1);
    CHECK_NOT_EQUAL(p_1, p_1);
    CHECK_NOT_EQUAL(p_2, p_2);
    CHECK_LESS(p_1, p_1);
    CHECK_LESS(p_2, p_1);
    CHECK_LESS(p_2, p_2);
    CHECK_GREATER(p_1, p_1);
    CHECK_GREATER(p_1, p_2);
    CHECK_GREATER(p_2, p_2);
    CHECK_LESS_EQUAL(p_2, p_1);
    CHECK_GREATER_EQUAL(p_1, p_2);
}

struct FooException {};

struct BarException: std::exception {
    const char* what() const REALM_NOEXCEPT_OR_NOTHROW override
    {
        return "bar";
    }
};

void throw_foo()
{
    throw FooException();
}

void throw_bar()
{
    throw BarException();
}

void throw_nothing()
{
}

TEST_EX(Success_Exception, success_list, true) // Test #6, accum checks = 85 + 2 = 87
{
    CHECK_THROW(throw_foo(), FooException);
    CHECK_THROW(throw_bar(), BarException);
}

TEST_EX(Failure_Exception, failure_list, true) // Test #6, accum checks = 105 + 2 = 107
{
    CHECK_THROW(throw_nothing(), FooException);
    CHECK_THROW(throw_nothing(), BarException);
}


struct SummaryRecorder: Reporter {
    Summary& m_summary;
    SummaryRecorder(Summary& summary):
        m_summary(summary)
    {
    }
    void summary(const Summary& summary) override
    {
        m_summary = summary;
    }
};

void check_summary(TestResults& test_results, TestList& list,
                   int num_included_tests, int num_failed_tests, int num_excluded_tests,
                   int num_checks, int num_failed_checks)
{
    Summary summary;
    SummaryRecorder reporter(summary);
    list.run(&reporter);
    CHECK_EQUAL(num_included_tests, summary.num_included_tests);
    CHECK_EQUAL(num_failed_tests,   summary.num_failed_tests);
    CHECK_EQUAL(num_excluded_tests, summary.num_excluded_tests);
    CHECK_EQUAL(num_checks,         summary.num_checks);
    CHECK_EQUAL(num_failed_checks,  summary.num_failed_checks);
}


void check_filtered_summary(TestResults& test_results, TestList& list, const char* filter_str,
                            int num_included_tests, int num_failed_tests, int num_excluded_tests,
                            int num_checks, int num_failed_checks)
{
    Summary summary;
    SummaryRecorder reporter(summary);
    std::unique_ptr<Filter> filter(create_wildcard_filter(filter_str));
    list.run(&reporter, filter.get());
    CHECK_EQUAL(num_included_tests, summary.num_included_tests);
    CHECK_EQUAL(num_failed_tests,   summary.num_failed_tests);
    CHECK_EQUAL(num_excluded_tests, summary.num_excluded_tests);
    CHECK_EQUAL(num_checks,         summary.num_checks);
    CHECK_EQUAL(num_failed_checks,  summary.num_failed_checks);
}


TEST(Self_Basic)
{
    CHECK(zero_tests_list.run());
    CHECK(zero_checks_list.run());
    CHECK(one_check_success_list.run());
    CHECK(!one_check_failure_list.run());
    CHECK(one_test_success_list.run());
    CHECK(!one_test_failure_list.run());
    CHECK(few_tests_success_list.run());
    CHECK(!few_tests_failure_list.run());
    CHECK(!mixed_list.run());
    CHECK(success_list.run());
    CHECK(!failure_list.run());

    check_summary(test_results, zero_tests_list,        0, 0, 0,   0,   0);
    check_summary(test_results, zero_checks_list,       1, 0, 0,   0,   0);
    check_summary(test_results, one_check_success_list, 1, 0, 0,   1,   0);
    check_summary(test_results, one_check_failure_list, 1, 1, 0,   1,   1);
    check_summary(test_results, one_test_success_list,  1, 0, 0,   5,   0);
    check_summary(test_results, one_test_failure_list,  1, 1, 0,   5,   1);
    check_summary(test_results, few_tests_success_list, 3, 0, 0,  13,   0);
    check_summary(test_results, few_tests_failure_list, 3, 1, 0,  13,   1);
    check_summary(test_results, mixed_list,             7, 3, 0,  19,   6);
    check_summary(test_results, success_list,           6, 0, 0,  87,   0);
    check_summary(test_results, failure_list,           6, 6, 0, 107, 107);

    check_filtered_summary(test_results, mixed_list, "- *",           0, 0, 7,  0, 0);
    check_filtered_summary(test_results, mixed_list, "* - *",         0, 0, 7,  0, 0);
    check_filtered_summary(test_results, mixed_list, "",              7, 3, 0, 19, 6);
    check_filtered_summary(test_results, mixed_list, "*",             7, 3, 0, 19, 6);
    check_filtered_summary(test_results, mixed_list, "* -",           7, 3, 0, 19, 6);
    check_filtered_summary(test_results, mixed_list, "-",             7, 3, 0, 19, 6);
    check_filtered_summary(test_results, mixed_list, "Mixed_*",       7, 3, 0, 19, 6);
    check_filtered_summary(test_results, mixed_list, "Mixed_* -",     7, 3, 0, 19, 6);
    check_filtered_summary(test_results, mixed_list, "Mixed_1_X",     1, 0, 6,  4, 0);
    check_filtered_summary(test_results, mixed_list, "Mixed_2_Y",     1, 1, 6,  4, 1);
    check_filtered_summary(test_results, mixed_list, "Mixed_3_X",     1, 0, 6,  0, 0);
    check_filtered_summary(test_results, mixed_list, "Mixed_4_Y",     1, 1, 6,  3, 3);
    check_filtered_summary(test_results, mixed_list, "Mixed_5_X",     1, 1, 6,  4, 2);
    check_filtered_summary(test_results, mixed_list, "Mixed_6_Y",     1, 0, 6,  0, 0);
    check_filtered_summary(test_results, mixed_list, "Mixed_7_Y",     1, 0, 6,  4, 0);
    check_filtered_summary(test_results, mixed_list, "Mixed_*_X",     3, 1, 4,  8, 2);
    check_filtered_summary(test_results, mixed_list, "Mixed_*_Y",     4, 2, 3, 11, 4);
    check_filtered_summary(test_results, mixed_list, "* - Mixed_*_X", 4, 2, 3, 11, 4);
    check_filtered_summary(test_results, mixed_list, "* - Mixed_*_Y", 3, 1, 4,  8, 2);
    check_filtered_summary(test_results, mixed_list,
                           "Mixed_1_X Mixed_3_X Mixed_5_X",           3, 1, 4,  8, 2);
    check_filtered_summary(test_results, mixed_list,
                           "* - Mixed_1_X Mixed_3_X Mixed_5_X",       4, 2, 3, 11, 4);
}


TEST(Self_CrossTypeCompare)
{
    CHECK_EQUAL(static_cast<signed char>(1), static_cast<unsigned char>(1));
    CHECK_EQUAL(static_cast<signed char>(1), static_cast<unsigned short>(1));
    CHECK_EQUAL(static_cast<signed char>(1), static_cast<unsigned int>(1));
    CHECK_EQUAL(static_cast<signed char>(1), static_cast<unsigned long>(1));
    CHECK_EQUAL(static_cast<short>(1),       static_cast<unsigned short>(1));
    CHECK_EQUAL(static_cast<short>(1),       static_cast<unsigned int>(1));
    CHECK_EQUAL(static_cast<short>(1),       static_cast<unsigned long>(1));
    CHECK_EQUAL(static_cast<int>(1),         static_cast<unsigned int>(1));
    CHECK_EQUAL(static_cast<int>(1),         static_cast<unsigned long>(1));
    CHECK_EQUAL(static_cast<long>(1),        static_cast<unsigned long>(1));

    CHECK_NOT_EQUAL(static_cast<signed char>(-1), static_cast<unsigned char>(-1));
    CHECK_NOT_EQUAL(static_cast<signed char>(-1), static_cast<unsigned short>(-1));
    CHECK_NOT_EQUAL(static_cast<signed char>(-1), static_cast<unsigned int>(-1));
    CHECK_NOT_EQUAL(static_cast<signed char>(-1), static_cast<unsigned long>(-1));
    CHECK_NOT_EQUAL(static_cast<short>(-1),       static_cast<unsigned short>(-1));
    CHECK_NOT_EQUAL(static_cast<short>(-1),       static_cast<unsigned int>(-1));
    CHECK_NOT_EQUAL(static_cast<short>(-1),       static_cast<unsigned long>(-1));
    CHECK_NOT_EQUAL(static_cast<int>(-1),         static_cast<unsigned int>(-1));
    CHECK_NOT_EQUAL(static_cast<int>(-1),         static_cast<unsigned long>(-1));
    CHECK_NOT_EQUAL(static_cast<long>(-1),        static_cast<unsigned long>(-1));

    CHECK_LESS(static_cast<signed char>(-1), static_cast<unsigned char>(-1));
    CHECK_LESS(static_cast<signed char>(-1), static_cast<unsigned short>(-1));
    CHECK_LESS(static_cast<signed char>(-1), static_cast<unsigned int>(-1));
    CHECK_LESS(static_cast<signed char>(-1), static_cast<unsigned long>(-1));
    CHECK_LESS(static_cast<short>(-1),       static_cast<unsigned short>(-1));
    CHECK_LESS(static_cast<short>(-1),       static_cast<unsigned int>(-1));
    CHECK_LESS(static_cast<short>(-1),       static_cast<unsigned long>(-1));
    CHECK_LESS(static_cast<int>(-1),         static_cast<unsigned int>(-1));
    CHECK_LESS(static_cast<int>(-1),         static_cast<unsigned long>(-1));
    CHECK_LESS(static_cast<long>(-1),        static_cast<unsigned long>(-1));
}


} // anonymous namespace
