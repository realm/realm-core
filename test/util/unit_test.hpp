/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_TEST_UTIL_UNIT_TEST_HPP
#define REALM_TEST_UTIL_UNIT_TEST_HPP

#include <stddef.h>
#include <cmath>
#include <cstring>
#include <memory>
#include <algorithm>
#include <vector>
#include <list>
#include <string>
#include <sstream>
#include <ostream>

#include <realm/util/features.h>
#include <realm/util/type_traits.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/util/logger.hpp>


#define TEST(name) TEST_IF(name, true)

/// Allows you to control whether the test will be enabled or
/// disabled. The test will be compiled in both cases. You can pass
/// any expression that would be a valid condition in an `if`
/// statement. The expression is not evaluated until you call
/// TestList::run(). This allows you to base the condition on global
/// variables which can then be adjusted before calling
/// TestList::run().
#define TEST_IF(name, enabled) TEST_EX(name, realm::test_util::unit_test::get_default_test_list(), enabled, true)

/// Add a test that must neither execute concurrently with other tests, nor with
/// itself. These tests will always be executed by the thread that calls
/// TestList::run().
#define NONCONCURRENT_TEST(name) NONCONCURRENT_TEST_IF(name, true)

#define NONCONCURRENT_TEST_IF(name, enabled)                                                                         \
    TEST_EX(name, realm::test_util::unit_test::get_default_test_list(), enabled, false)

#define TEST_EX(name, list, enabled, allow_concur)                                                                   \
    struct Realm_UnitTest__##name : realm::test_util::unit_test::TestBase {                                          \
        static bool test_enabled()                                                                                   \
        {                                                                                                            \
            return bool(enabled);                                                                                    \
        }                                                                                                            \
        Realm_UnitTest__##name(realm::test_util::unit_test::TestContext& c)                                          \
            : TestBase(c)                                                                                            \
        {                                                                                                            \
        }                                                                                                            \
        void test_run();                                                                                             \
    };                                                                                                               \
    realm::test_util::unit_test::RegisterTest<Realm_UnitTest__##name> realm_unit_test_reg__##name(                   \
        (list), (allow_concur), "DefaultSuite", #name, __FILE__, __LINE__);                                          \
    void Realm_UnitTest__##name::test_run()


#define CHECK(cond) test_context.check(bool(cond), __FILE__, __LINE__, #cond)

#define CHECK_NOT(cond) test_context.check_not(bool(cond), __FILE__, __LINE__, #cond)

#define CHECK_EQUAL(a, b) test_context.check_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_NOT_EQUAL(a, b) test_context.check_not_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS(a, b) test_context.check_less((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS_EQUAL(a, b) test_context.check_less_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER(a, b) test_context.check_greater((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER_EQUAL(a, b) test_context.check_greater_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_OR_RETURN(cond)                                                                                        \
    do {                                                                                                             \
        if (!CHECK(cond)) {                                                                                          \
            return;                                                                                                  \
        }                                                                                                            \
    } while (false)

#define CHECK_THROW(expr, exception_class)                                                                           \
    do {                                                                                                             \
        try {                                                                                                        \
            (expr);                                                                                                  \
            test_context.throw_failed(__FILE__, __LINE__, #expr, #exception_class);                                  \
        }                                                                                                            \
        catch (exception_class&) {                                                                                   \
            test_context.check_succeeded();                                                                          \
        }                                                                                                            \
    } while (false)

#define CHECK_THROW_EX(expr, exception_class, exception_cond)                                                        \
    do {                                                                                                             \
        try {                                                                                                        \
            (expr);                                                                                                  \
            test_context.throw_ex_failed(__FILE__, __LINE__, #expr, #exception_class, #exception_cond);              \
        }                                                                                                            \
        catch (exception_class & e) {                                                                                \
            if (exception_cond) {                                                                                    \
                test_context.check_succeeded();                                                                      \
            }                                                                                                        \
            else {                                                                                                   \
                test_context.throw_ex_cond_failed(__FILE__, __LINE__, #expr, #exception_class, #exception_cond);     \
            }                                                                                                        \
        }                                                                                                            \
    } while (false)

#define CHECK_THROW_ANY(expr)                                                                                        \
    do {                                                                                                             \
        try {                                                                                                        \
            (expr);                                                                                                  \
            test_context.throw_any_failed(__FILE__, __LINE__, #expr);                                                \
        }                                                                                                            \
        catch (...) {                                                                                                \
            test_context.check_succeeded();                                                                          \
        }                                                                                                            \
    } while (false)

#if REALM_VALGRIND
static const bool running_with_valgrind = true;
#else
static const bool running_with_valgrind = false;
#endif

#if REALM_TSAN
static const bool running_with_tsan = true;
#else
static const bool running_with_tsan = false;
#endif

#if REALM_ASAN
static const bool running_with_asan = true;
#else
static const bool running_with_asan = false;
#endif


//@{

/// These are the four inexact floating point comparisons defined by
/// Donald. E. Knuth. in volume II of his "The Art of Computer
/// Programming" 3rd edition, section 4.2.2 "Accuracy of Floating
/// Point Arithmetic", definitions (21)-(24):
///
///     approximately equal       |a-b| <= max(|a|, |b|) * epsilon
///     essentially equal         |a-b| <= min(|a|, |b|) * epsilon
///     definitely less than      b - a >  max(|a|, |b|) * epsilon
///     definitely greater than   a - b >  max(|a|, |b|) * epsilon
///
/// In general you should set `epsilon` to some small multiple of the
/// machine epsilon for the floating point type used in your
/// computations (e.g. `std::numeric_limits<double>::epsilon()`). As a
/// general rule, a longer and more complex computation needs a higher
/// multiple of the machine epsilon.

#define CHECK_APPROXIMATELY_EQUAL(a, b, epsilon)                                                                     \
    test_context.check_approximately_equal((a), (b), (epsilon), __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_ESSENTIALLY_EQUAL(a, b, epsilon)                                                                       \
    test_context.check_essentially_equal((a), (b), (epsilon), __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_DEFINITELY_LESS(a, b, epsilon)                                                                         \
    test_context.check_definitely_less((a), (b), (epsilon), __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_DEFINITELY_GREATER(a, b, epsilon)                                                                      \
    test_context.check_definitely_greater((a), (b), (epsilon), __FILE__, __LINE__, #a, #b, #epsilon)

//@}


namespace realm {
namespace test_util {
namespace unit_test {

class TestContext;
class ThreadContext;
class SharedContext;


struct TestDetails {
    const char* suite_name;
    std::string test_name;
    const char* file_name;
    long line_number;
};


struct Summary {
    long num_disabled_tests; // TEST_IF()
    long num_excluded_tests; // Excluded by filtering
    long num_included_tests; // Included by filtering
    long num_executed_tests; // num_included_tests times num_recurrences
    long num_failed_tests;   // Out of num_executed_tests
    long long num_executed_checks;
    long long num_failed_checks; // Out of num_executed_checks
    double elapsed_seconds;
};


class Reporter {
public:
    virtual void thread_begin(const ThreadContext&);
    virtual void begin(const TestContext&);
    virtual void fail(const TestContext&, const char* file_name, long line_number, const std::string& message);
    virtual void end(const TestContext&, double elapsed_seconds);
    virtual void thread_end(const ThreadContext&);
    virtual void summary(const SharedContext&, const Summary&);
    virtual ~Reporter() noexcept
    {
    }
};


class Filter {
public:
    virtual bool include(const TestDetails&) = 0;
    virtual ~Filter() noexcept
    {
    }
};


class TestList {
public:
    /// Call this function to change the order of tests in the list. This order
    /// is the execution order unless you ask for shuffling, or for multiple
    /// execution threads when calling run().
    ///
    /// Within a particular translation unit, the default order is the order in
    /// which the tests occur in the source file. The default order of tests
    /// between translation units is uncertain, but will in general depend on
    /// the order in which the files are linked together. With a suitable
    /// comparison operation, this function can be used to eliminate the
    /// uncertainty of the order. An example of a suitable comparison operation
    /// would be one that compares file names, such as
    /// PatternBasedFileOrder.
    ///
    /// The sorting function should have the following signature:
    ///
    ///     bool compare(const TestDetails& a, const TestDetails& b)
    ///
    /// It must return true if, and only if `a` is less that `b`. The sorting
    /// function is allowed to assume that a particular TestDetails object
    /// remains allocated on the same address across all invocations.
    ///
    ///  The sorting operation is stable.
    template <class Compare>
    void sort(Compare);

    struct Config {
        Config()
        {
        }

        int num_threads = 1;
        int num_repetitions = 1;
        bool shuffle = false;

        /// No filtering by default.
        Filter* filter = nullptr;

        /// No reporting by default.
        Reporter* reporter = nullptr;

        /// The base logger to use for constructing loggers for reporting and
        /// for custom intra test logging. If no base logger is specified, an
        /// instance of util::StderrLogger will be used with the log level set
        /// to util::Logger::Level::info. The log level threshold selected for a
        /// specified base logger will be ignored. The specified base logger
        /// does not have to be thread safe.
        util::Logger* logger = nullptr;

        /// The log level threshold to use for the intra test loggers
        /// (TestContext::logger).
        util::Logger::Level intra_test_log_level = util::Logger::Level::off;

        /// By default, all test threads send log messages through a single
        /// shared logger (\ref logger), but if \ref per_thread_log_path is set
        /// to a nonempty string, then that string is used as a template for log
        /// file paths, and one log file is created for each test thread.
        ///
        /// When specified, it must be a valid path, and contain at least one
        /// `%`, for example `test_thread_%.log`. The test thread number will be
        /// substituted for the last occurrence of `%`.
        std::string per_thread_log_path;

        /// Abort testing process as soon as a check fails or an unexpected
        /// exception is thrown in a test.
        bool abort_on_failure = false;
    };

    /// Run all the tests in this list (or a filtered subset of them).
    bool run(Config = Config());

    /// Short-hand version of run(Config).
    bool run(Reporter* reporter, Filter* filter = nullptr);

    using RunFunc = void (*)(TestContext&);
    using IsEnabledFunc = bool (*)();

    /// Called automatically when you use the `TEST` macro (or one of
    /// its friends).
    void add(RunFunc, IsEnabledFunc, bool allow_concur, const char* suite, const std::string& name, const char* file,
             long line);

    size_t size() const noexcept;
    const TestDetails& get_test_details(size_t i) const noexcept;

private:
    class SharedContextImpl;
    class ThreadContextImpl;

    struct Test {
        RunFunc run_func;
        IsEnabledFunc is_enabled_func;
        bool allow_concur;
        TestDetails details;
    };
    std::list<Test> m_test_storage;
    std::vector<const Test*> m_tests;

    friend class TestContext;
};

TestList& get_default_test_list();


struct PatternBasedFileOrder {
    PatternBasedFileOrder(const char** patterns_begin, const char** patterns_end);

    template <size_t N>
    PatternBasedFileOrder(const char* (&patterns)[N]);

    bool operator()(const TestDetails&, const TestDetails&);

private:
    class state;
    struct wrap {
        util::bind_ptr<state> m_state;
        wrap(const char** patterns_begin, const char** patterns_end);
        ~wrap();
        wrap(const wrap&);
        wrap& operator=(const wrap&);
    };
    wrap m_wrap;
};


class SimpleReporter : public Reporter {
public:
    explicit SimpleReporter(bool report_progress = false);

    void begin(const TestContext&) override;
    void fail(const TestContext&, const char*, long, const std::string&) override;
    void thread_end(const ThreadContext&) override;
    void summary(const SharedContext&, const Summary&) override;

protected:
    bool m_report_progress;
};


/// Generates output that is compatible with the XML output of UnitTest++.
std::unique_ptr<Reporter> create_xml_reporter(std::ostream&);

/// Generates output that is compatible with the XML output of JUnit. See
/// http://llg.cubic.org/docs/junit/
std::unique_ptr<Reporter> create_junit_reporter(std::ostream&);

std::unique_ptr<Reporter> create_twofold_reporter(Reporter& subreporter_1, Reporter& subreporter_2);

/// Run only those tests whose name is both included and not
/// excluded.
///
/// EBNF:
///
///     filter = { include-pattern }, [ '-', { exclude-pattern } ]
///     include-pattern = pattern
///     exclude-pattern = pattern
///
/// Each pattern is a string containing no white-space, and optionally
/// containg `*` wild cards. Each `*` matches zero or more arbitrary
/// characters.
///
/// An empty filter is functionally equivalent to `*` and a filter on
/// the form `- ...` is equivalent to `* - ...`.
///
/// Note that the empty string, `*`, `* -`, and `-` all mean
/// "everything". Likewise, both `- *` and `* - *` means "nothing".
///
/// For example, `Foo Bar*` will inlcude only the `Foo` test and those
/// whose names start with `Bar`. Another example is `Foo* - Foo2 *X`,
/// which will include all tests whose names start with `Foo`, except
/// `Foo2` and those whose names end with an `X`.
std::unique_ptr<Filter> create_wildcard_filter(const std::string&);


class TestContext {
public:
    const ThreadContext& thread_context;
    const TestDetails& test_details;

    /// Index of executing test with respect to the order of the tests in
    /// `test_list` (`thread_context.shared_context.test_list`).
    /// `test_list.size()` specifies the number of distinct tests.
    const size_t test_index;

    /// An index into the sequence of repeated executions of this
    /// test. `thread_context.shared_context.num_recurrences` specifies the
    /// number of requested repetitions.
    const int recurrence_index;

    /// The intra test logger. That is, a logger that is available for custom
    /// use inside the associated unit test. The log level of this logger is
    /// specified via TestList::Config::intra_test_log_level. See also
    /// ThreadContext::report_logger.
    util::Logger& logger;

    bool check_cond(bool cond, const char* file, long line, const char* macro_name, const char* cond_text);

    bool check(bool cond, const char* file, long line, const char* cond_text);

    bool check_not(bool cond, const char* file, long line, const char* cond_text);

    template <class A, class B>
    bool check_compare(bool cond, const A& a, const B& b, const char* file, long line, const char* macro_name,
                       const char* a_text, const char* b_text);

    bool check_inexact_compare(bool cond, long double a, long double b, long double eps, const char* file, long line,
                               const char* macro_name, const char* a_text, const char* b_text, const char* eps_text);

    template <class A, class B>
    bool check_equal(const A& a, const B& b, const char* file, long line, const char* a_text, const char* b_text);

    template <class A, class B>
    bool check_not_equal(const A& a, const B& b, const char* file, long line, const char* a_text, const char* b_text);

    template <class A, class B>
    bool check_less(const A& a, const B& b, const char* file, long line, const char* a_text, const char* b_text);

    template <class A, class B>
    bool check_less_equal(const A& a, const B& b, const char* file, long line, const char* a_text,
                          const char* b_text);

    template <class A, class B>
    bool check_greater(const A& a, const B& b, const char* file, long line, const char* a_text, const char* b_text);

    template <class A, class B>
    bool check_greater_equal(const A& a, const B& b, const char* file, long line, const char* a_text,
                             const char* b_text);

    bool check_approximately_equal(long double a, long double b, long double eps, const char* file, long line,
                                   const char* a_text, const char* b_text, const char* eps_text);

    bool check_essentially_equal(long double a, long double b, long double eps, const char* file, long line,
                                 const char* a_text, const char* b_text, const char* eps_text);

    bool check_definitely_less(long double a, long double b, long double eps, const char* file, long line,
                               const char* a_text, const char* b_text, const char* eps_text);

    bool check_definitely_greater(long double a, long double b, long double eps, const char* file, long line,
                                  const char* a_text, const char* b_text, const char* eps_text);

    void check_succeeded();

    void throw_failed(const char* file, long line, const char* expr_text, const char* exception_name);
    void throw_ex_failed(const char* file, long line, const char* expr_text, const char* exception_name,
                         const char* exception_cond_text);
    void throw_ex_cond_failed(const char* file, long line, const char* expr_text, const char* exception_name,
                              const char* exception_cond_text);
    void throw_any_failed(const char* file, long line, const char* expr_text);

    std::string get_test_name() const;

    TestContext(const TestContext&) = delete;
    TestContext& operator=(const TestContext&) = delete;

private:
    TestList::ThreadContextImpl& m_thread_context;

    TestContext(TestList::ThreadContextImpl&, const TestDetails&, size_t test_index, int recurrence_index);

    REALM_NORETURN void abort();
    void test_failed(const std::string& message);
    void check_failed(const char* file, long line, const std::string& message);
    void cond_failed(const char* file, long line, const char* macro_name, const char* cond_text);
    void compare_failed(const char* file, long line, const char* macro_name, const char* a_text, const char* b_text,
                        const std::string& a_val, const std::string& b_val);
    void inexact_compare_failed(const char* file, long line, const char* macro_name, const char* a_text,
                                const char* b_text, const char* eps_text, long double a, long double b,
                                long double eps);

    friend class TestList::ThreadContextImpl;
};


class ThreadContext {
public:
    const SharedContext& shared_context;

    /// The index of the test thread associated with this
    /// context. `shared_context.num_threads` specifies the total number of test
    /// threads.
    const int thread_index;

    /// The thread specific logger to be used by custom reporters. See also
    /// SharedContext::report_logger and TestContext::logger.
    util::Logger& report_logger;

    ThreadContext(const ThreadContext&) = delete;
    ThreadContext& operator=(const ThreadContext&) = delete;

protected:
    ThreadContext(SharedContext&, int thread_index, util::Logger&);
};


class SharedContext {
public:
    const TestList& test_list;
    const int num_recurrences;
    const int num_threads;

    /// The thread non-specific logger to be used by custom reporters. See also
    /// ThreadContext::report_logger.
    util::Logger& report_logger;

    SharedContext(const SharedContext&) = delete;
    SharedContext& operator=(const SharedContext&) = delete;

protected:
    SharedContext(const TestList&, int num_recurrences, int num_threads, util::Logger&);
};


class TestBase {
protected:
    TestContext& test_context;

    /// Short hand for test_context.logger.info().
    template <class... Params>
    void log(const char* message, Params&&...);

    TestBase(TestContext&);
};


// Implementation

template <class Test>
struct RegisterTest {
    RegisterTest(TestList& list, bool allow_concur, const char* suite, std::string name, const char* file, long line)
    {
        list.add(&RegisterTest::run_test, &Test::test_enabled, allow_concur, suite, name, file, line);
    }
    static void run_test(TestContext& test_context)
    {
        Test test(test_context);
        test.test_run();
    }
};


template <class Compare>
inline void TestList::sort(Compare compare)
{
    auto compare_2 = [&](const Test* a, const Test* b) { return compare(a->details, b->details); };
    std::stable_sort(m_tests.begin(), m_tests.end(), compare_2);
}

inline bool TestList::run(Reporter* reporter, Filter* filter)
{
    Config config;
    config.reporter = reporter;
    config.filter = filter;
    return run(config); // Throws
}

inline size_t TestList::size() const noexcept
{
    return m_tests.size();
}

inline const TestDetails& TestList::get_test_details(size_t i) const noexcept
{
    return m_tests[i]->details;
}

inline PatternBasedFileOrder::PatternBasedFileOrder(const char** patterns_begin, const char** patterns_end)
    : m_wrap(patterns_begin, patterns_end)
{
}

template <size_t N>
inline PatternBasedFileOrder::PatternBasedFileOrder(const char* (&patterns)[N])
    : m_wrap(patterns, patterns + N)
{
}


template <class A, class B, bool both_are_integral>
struct Compare {
    static bool equal(const A& a, const B& b)
    {
        return a == b;
    }
    static bool less(const A& a, const B& b)
    {
        return a < b;
    }
};

template <class A, class B>
struct Compare<A, B, true> {
    static bool equal(const A& a, const B& b)
    {
        return util::int_equal_to(a, b);
    }
    static bool less(const A& a, const B& b)
    {
        return util::int_less_than(a, b);
    }
};


template <class A, class B>
inline bool equal(const A& a, const B& b)
{
    const bool both_are_integral = std::is_integral<A>::value && std::is_integral<B>::value;
    return Compare<A, B, both_are_integral>::equal(a, b);
}

template <class A, class B>
inline bool less(const A& a, const B& b)
{
    const bool both_are_integral = std::is_integral<A>::value && std::is_integral<B>::value;
    return Compare<A, B, both_are_integral>::less(a, b);
}


// Special hooks for comparing zero terminated strings
// FIXME: Also handle (char*, char*), (const char*, char*), (char*, const char*).
// FIXME: Also handle `wchar_t*`.
inline bool equal(const char* a, const char* b)
{
    return std::strcmp(a, b) == 0;
}

inline bool less(const char* a, const char* b)
{
    return std::strcmp(a, b) < 0;
}


// See Donald. E. Knuth, "The Art of Computer Programming", 3rd
// edition, volume II, section 4.2.2 "Accuracy of Floating Point
// Arithmetic", definitions (21)-(24).
inline bool approximately_equal(long double a, long double b, long double epsilon)
{
    return std::abs(a - b) <= std::max(std::abs(a), std::abs(b)) * epsilon;
}

inline bool essentially_equal(long double a, long double b, long double epsilon)
{
    return std::abs(a - b) <= std::min(std::abs(a), std::abs(b)) * epsilon;
}

inline bool definitely_less(long double a, long double b, long double epsilon)
{
    return b - a > std::max(std::abs(a), std::abs(b)) * epsilon;
}


template <class T, bool is_float>
struct SetPrecision {
    static void exec(std::ostream&)
    {
    }
};

template <class T>
struct SetPrecision<T, true> {
    static void exec(std::ostream& out)
    {
        out.precision(std::numeric_limits<T>::digits10 + 1);
    }
};

template <class T>
void to_string(const T& value, std::string& str)
{
    // FIXME: Put string values in quotes, and escape non-printables as well as '"' and '\\'.
    std::ostringstream out;
    SetPrecision<T, std::is_floating_point<T>::value>::exec(out);
    out << value;
    str = out.str();
}


inline bool TestContext::check_cond(bool cond, const char* file, long line, const char* macro_name,
                                    const char* cond_text)
{
    if (REALM_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        cond_failed(file, line, macro_name, cond_text);
    }
    return cond;
}

inline bool TestContext::check(bool cond, const char* file, long line, const char* cond_text)
{
    return check_cond(cond, file, line, "CHECK", cond_text);
}

inline bool TestContext::check_not(bool cond, const char* file, long line, const char* cond_text)
{
    return check_cond(!cond, file, line, "CHECK_NOT", cond_text);
}

template <class A, class B>
inline bool TestContext::check_compare(bool cond, const A& a, const B& b, const char* file, long line,
                                       const char* macro_name, const char* a_text, const char* b_text)
{
    if (REALM_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        std::string a_val, b_val;
        to_string(a, a_val);
        to_string(b, b_val);
        compare_failed(file, line, macro_name, a_text, b_text, a_val, b_val);
    }
    return cond;
}

inline bool TestContext::check_inexact_compare(bool cond, long double a, long double b, long double eps,
                                               const char* file, long line, const char* macro_name,
                                               const char* a_text, const char* b_text, const char* eps_text)
{
    if (REALM_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        inexact_compare_failed(file, line, macro_name, a_text, b_text, eps_text, a, b, eps);
    }
    return cond;
}

template <class A, class B>
inline bool TestContext::check_equal(const A& a, const B& b, const char* file, long line, const char* a_text,
                                     const char* b_text)
{
    bool cond = equal(a, b);
    return check_compare(cond, a, b, file, line, "CHECK_EQUAL", a_text, b_text);
}

template <class A, class B>
inline bool TestContext::check_not_equal(const A& a, const B& b, const char* file, long line, const char* a_text,
                                         const char* b_text)
{
    bool cond = !equal(a, b);
    return check_compare(cond, a, b, file, line, "CHECK_NOT_EQUAL", a_text, b_text);
}

template <class A, class B>
inline bool TestContext::check_less(const A& a, const B& b, const char* file, long line, const char* a_text,
                                    const char* b_text)
{
    bool cond = less(a, b);
    return check_compare(cond, a, b, file, line, "CHECK_LESS", a_text, b_text);
}

template <class A, class B>
inline bool TestContext::check_less_equal(const A& a, const B& b, const char* file, long line, const char* a_text,
                                          const char* b_text)
{
    bool cond = !less(b, a); // Note: Reverse operand order
    return check_compare(cond, a, b, file, line, "CHECK_LESS_EQUAL", a_text, b_text);
}

template <class A, class B>
inline bool TestContext::check_greater(const A& a, const B& b, const char* file, long line, const char* a_text,
                                       const char* b_text)
{
    bool cond = less(b, a); // Note: Reverse operand order
    return check_compare(cond, a, b, file, line, "CHECK_GREATER", a_text, b_text);
}

template <class A, class B>
inline bool TestContext::check_greater_equal(const A& a, const B& b, const char* file, long line, const char* a_text,
                                             const char* b_text)
{
    bool cond = !less(a, b);
    return check_compare(cond, a, b, file, line, "CHECK_GREATER_EQUAL", a_text, b_text);
}

inline bool TestContext::check_approximately_equal(long double a, long double b, long double eps, const char* file,
                                                   long line, const char* a_text, const char* b_text,
                                                   const char* eps_text)
{
    bool cond = approximately_equal(a, b, eps);
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_APPROXIMATELY_EQUAL", a_text, b_text, eps_text);
}

inline bool TestContext::check_essentially_equal(long double a, long double b, long double eps, const char* file,
                                                 long line, const char* a_text, const char* b_text,
                                                 const char* eps_text)
{
    bool cond = essentially_equal(a, b, eps);
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_ESSENTIALLY_EQUAL", a_text, b_text, eps_text);
}

inline bool TestContext::check_definitely_less(long double a, long double b, long double eps, const char* file,
                                               long line, const char* a_text, const char* b_text,
                                               const char* eps_text)
{
    bool cond = definitely_less(a, b, eps);
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_DEFINITELY_LESS", a_text, b_text, eps_text);
}

inline bool TestContext::check_definitely_greater(long double a, long double b, long double eps, const char* file,
                                                  long line, const char* a_text, const char* b_text,
                                                  const char* eps_text)
{
    bool cond = definitely_less(b, a, eps); // Note: Reverse operand order
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_DEFINITELY_GREATER", a_text, b_text, eps_text);
}

inline ThreadContext::ThreadContext(SharedContext& sc, int ti, util::Logger& rl)
    : shared_context(sc)
    , thread_index(ti)
    , report_logger(rl)
{
}

inline SharedContext::SharedContext(const TestList& tl, int nr, int nt, util::Logger& rl)
    : test_list(tl)
    , num_recurrences(nr)
    , num_threads(nt)
    , report_logger(rl)
{
}

inline TestBase::TestBase(TestContext& context)
    : test_context(context)
{
}

template <class... Params>
inline void TestBase::log(const char* message, Params&&... params)
{
    test_context.logger.info(message, std::forward<Params>(params)...); // Throws
}


} // namespace unit_test
} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_UNIT_TEST_HPP
