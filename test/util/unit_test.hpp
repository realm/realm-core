/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_TEST_UTIL_UNIT_TEST_HPP
#define REALM_TEST_UTIL_UNIT_TEST_HPP

#include <cmath>
#include <cstring>
#include <algorithm>
#include <vector>
#include <string>
#include <sstream>
#include <ostream>

#include <realm/util/features.h>
#include <realm/util/type_traits.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/bind_ptr.hpp>


#define TEST(name) TEST_IF(name, true)

/// Allows you to control whether the test will be enabled or
/// disabled. The test will be compiled in both cases. You can pass
/// any expression that would be a valid condition in an `if`
/// statement. The expression is not evaluated until you call
/// TestList::run(). This allows you to base the condition on global
/// variables which can then be adjusted before calling
/// TestList::run().
#define TEST_IF(name, enabled) \
    TEST_EX(name, realm::test_util::unit_test::get_default_test_list(), enabled)

#define TEST_EX(name, list, enabled) \
    struct Realm_UnitTest__##name: realm::test_util::unit_test::Test { \
        bool test_enabled() const { return bool(enabled); } \
        void test_run(); \
    }; \
    Realm_UnitTest__##name realm_unit_test__##name; \
    realm::test_util::unit_test::RegisterTest \
        realm_unit_test_reg__##name((list), realm_unit_test__##name, \
                                      "DefaultSuite", #name, __FILE__, __LINE__); \
    void Realm_UnitTest__##name::test_run()


#define CHECK(cond) \
    test_results.check(bool(cond), __FILE__, __LINE__, #cond)

#define CHECK_NOT(cond) \
    test_results.check_not(bool(cond), __FILE__, __LINE__, #cond)

#define CHECK_EQUAL(a,b) \
    test_results.check_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_NOT_EQUAL(a,b) \
    test_results.check_not_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS(a,b) \
    test_results.check_less((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS_EQUAL(a,b) \
    test_results.check_less_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER(a,b) \
    test_results.check_greater((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER_EQUAL(a,b) \
    test_results.check_greater_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_OR_RETURN(cond) \
    do { \
        if (!CHECK(cond)) { \
            return; \
        } \
    } \
    while(false)

#define CHECK_THROW(expr, exception_class) \
    do { \
        try { \
            (expr); \
            test_results.throw_failed(__FILE__, __LINE__, #expr, #exception_class); \
        } \
        catch (exception_class&) { \
            test_results.check_succeeded(); \
        } \
    } \
    while(false)

#define CHECK_THROW_EX(expr, exception_class, exception_cond) \
    do { \
        try { \
            (expr); \
            test_results.throw_ex_failed(__FILE__, __LINE__, #expr, #exception_class, \
                                         #exception_cond); \
        } \
        catch (exception_class& e) { \
            if (exception_cond) { \
                test_results.check_succeeded(); \
            } \
            else { \
                test_results.throw_ex_cond_failed(__FILE__, __LINE__, #expr, #exception_class, \
                                                  #exception_cond); \
            } \
        } \
    } \
    while(false)

#define CHECK_THROW_ANY(expr) \
    do { \
        try { \
            (expr); \
            test_results.throw_any_failed(__FILE__, __LINE__, #expr); \
        } \
        catch (...) { \
            test_results.check_succeeded(); \
        } \
    } \
    while (false)



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

#define CHECK_APPROXIMATELY_EQUAL(a, b, epsilon) \
    test_results.check_approximately_equal((a), (b), (epsilon), \
                                           __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_ESSENTIALLY_EQUAL(a, b, epsilon) \
    test_results.check_essentially_equal((a), (b), (epsilon), \
                                         __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_DEFINITELY_LESS(a, b, epsilon) \
    test_results.check_definitely_less((a), (b), (epsilon), \
                                       __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_DEFINITELY_GREATER(a, b, epsilon) \
    test_results.check_definitely_greater((a), (b), (epsilon), \
                                          __FILE__, __LINE__, #a, #b, #epsilon)

//@}


namespace realm {
namespace test_util {
namespace unit_test {


class Test;
class TestResults;


struct TestDetails {
    long test_index;
    const char* suite_name;
    std::string test_name;
    const char* file_name;
    long line_number;
};


struct Summary {
    long num_included_tests;
    long num_failed_tests;
    long num_excluded_tests;
    long num_disabled_tests;
    long long num_checks;
    long long num_failed_checks;
    double elapsed_seconds;
};


class Reporter {
public:
    virtual void begin(const TestDetails&);
    virtual void fail(const TestDetails&, const std::string& message);
    virtual void end(const TestDetails&, double elapsed_seconds);
    virtual void summary(const Summary&);
    virtual ~Reporter() REALM_NOEXCEPT {}
};


class Filter {
public:
    virtual bool include(const TestDetails&) = 0;
    virtual ~Filter() REALM_NOEXCEPT {}
};


class TestList {
public:
    /// Call this function to change the underlying order of tests in
    /// this list. The underlying order is the order reflected by
    /// TestDetails::test_index. This is also the execution order
    /// unless you ask for shuffling, or for multiple execution
    /// threads when calling run().
    ///
    /// Within a particular translation unit, the default underlying
    /// order is the order in which the tests occur in the source
    /// file. The default underlying order of tests between
    /// translation units is uncertain, but will in general depend on
    /// the order in which the files are linked together. With a
    /// suitable comparison operation, this function can be used to
    /// eliminate the uncertainty of the underlying order. An example
    /// of a suitable comparison operation would be one that uses the
    /// file name as primary sorting criterium, and the original
    /// underlying order (TestDetails::test_index) as secondary
    /// criterium. See the class PatternBasedFileOrder for a slightly
    /// more advanced alternative.
    template<class Compare> void sort(Compare);

    /// Run all the tests in this list (or a filtered subset of them).
    bool run(Reporter* = 0, Filter* = 0, int num_threads = 1, bool shuffle = false);

    /// Called automatically when you use the `TEST` macro (or one of
    /// its friends).
    void add(Test&, const char* suite, const std::string& name, const char* file, long line);

private:
    class ExecContext;
    template<class Compare> class CompareAdaptor;

    std::vector<Test*> m_tests;

    void reassign_indexes();

    friend class TestResults;
};

TestList& get_default_test_list();



struct PatternBasedFileOrder {
    PatternBasedFileOrder(const char** patterns_begin, const char** patterns_end);

    template<std::size_t N> PatternBasedFileOrder(const char* (&patterns)[N]);

    bool operator()(TestDetails*, TestDetails*);

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


class SimpleReporter: public Reporter {
public:
    explicit SimpleReporter(bool report_progress = false);

    void begin(const TestDetails&) override;
    void fail(const TestDetails&, const std::string&) override;
    void summary(const Summary&) override;

protected:
    bool m_report_progress;
};


/// Generates output that is compatible with the XML output of
/// UnitTest++. Caller receives ownership of the returned reporter.
///
/// FIXME: Consider producing output that conforms to
/// http://windyroad.com.au/dl/Open%20Source/JUnit.xsd.
Reporter* create_xml_reporter(std::ostream&);


/// Run only those tests whose name is both included and not
/// excluded. Caller receives ownership of the returned filter.
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
Filter* create_wildcard_filter(const std::string&);


class TestResults {
public:
    bool check_cond(bool cond, const char* file, long line, const char* macro_name,
                    const char* cond_text);

    bool check(bool cond, const char* file, long line, const char* cond_text);

    bool check_not(bool cond, const char* file, long line, const char* cond_text);

    template<class A, class B>
    bool check_compare(bool cond, const A& a, const B& b,
                       const char* file, long line, const char* macro_name,
                       const char* a_text, const char* b_text);

    bool check_inexact_compare(bool cond, long double a, long double b,
                               long double eps, const char* file, long line,
                               const char* macro_name, const char* a_text,
                               const char* b_text, const char* eps_text);

    template<class A, class B>
    bool check_equal(const A& a, const B& b, const char* file, long line,
                     const char* a_text, const char* b_text);

    template<class A, class B>
    bool check_not_equal(const A& a, const B& b, const char* file, long line,
                         const char* a_text, const char* b_text);

    template<class A, class B>
    bool check_less(const A& a, const B& b, const char* file, long line,
                    const char* a_text, const char* b_text);

    template<class A, class B>
    bool check_less_equal(const A& a, const B& b, const char* file, long line,
                          const char* a_text, const char* b_text);

    template<class A, class B>
    bool check_greater(const A& a, const B& b, const char* file, long line,
                       const char* a_text, const char* b_text);

    template<class A, class B>
    bool check_greater_equal(const A& a, const B& b, const char* file, long line,
                             const char* a_text, const char* b_text);

    bool check_approximately_equal(long double a, long double b, long double eps,
                                   const char* file, long line, const char* a_text,
                                   const char* b_text, const char* eps_text);

    bool check_essentially_equal(long double a, long double b, long double eps,
                                 const char* file, long line, const char* a_text,
                                 const char* b_text, const char* eps_text);

    bool check_definitely_less(long double a, long double b, long double eps,
                               const char* file, long line, const char* a_text,
                               const char* b_text, const char* eps_text);

    bool check_definitely_greater(long double a, long double b, long double eps,
                                  const char* file, long line, const char* a_text,
                                  const char* b_text, const char* eps_text);

    void check_succeeded();

    void throw_failed(const char* file, long line, const char* expr_text,
                      const char* exception_name);
    void throw_ex_failed(const char* file, long line, const char* expr_text,
                         const char* exception_name, const char* exception_cond_text);
    void throw_ex_cond_failed(const char* file, long line, const char* expr_text,
                              const char* exception_name, const char* exception_cond_text);
    void throw_any_failed(const char* file, long line, const char* expr_text);

private:
    Test* m_test;
    TestList* m_list;
    TestList::ExecContext* m_context;

    TestResults();

    void test_failed(const std::string& message);
    void check_failed(const char* file, long line, const std::string& message);
    void cond_failed(const char* file, long line, const char* macro_name, const char* cond_text);
    void compare_failed(const char* file, long line, const char* macro_name,
                        const char* a_text, const char* b_text,
                        const std::string& a_val, const std::string& b_val);
    void inexact_compare_failed(const char* file, long line, const char* macro_name,
                                const char* a_text, const char* b_text, const char* eps_text,
                                long double a, long double b, long double eps);

    friend class Test;
    friend class TestList;
};


class Test {
public:
    virtual bool test_enabled() const = 0;
    virtual void test_run() = 0;

protected:
    TestDetails test_details;
    TestResults test_results;
    Test() {}

private:
    friend class TestList;
    friend class TestResults;
};




// Implementation

struct RegisterTest {
    RegisterTest(TestList& list, Test& test, const char* suite,
                 const char* name, const char* file, long line)
    {
        register_test(list, test, suite, name, file, line);
    }
    static void register_test(TestList& list, Test& test, const char* suite,
                              const std::string& name, const char* file, long line)
    {
        list.add(test, suite, name, file, line);
    }
};


template<class Compare> class TestList::CompareAdaptor {
public:
    CompareAdaptor(const Compare& compare):
        m_compare(compare)
    {
    }

    bool operator()(Test* a, Test* b)
    {
        return m_compare(&a->test_details, &b->test_details);
    }

private:
    Compare m_compare;
};

template<class Compare> inline void TestList::sort(Compare compare)
{
    std::sort(m_tests.begin(), m_tests.end(), CompareAdaptor<Compare>(compare));
    reassign_indexes();
}


inline PatternBasedFileOrder::PatternBasedFileOrder(const char** patterns_begin,
                                                    const char** patterns_end):
    m_wrap(patterns_begin, patterns_end)
{
}

template<std::size_t N>
inline PatternBasedFileOrder::PatternBasedFileOrder(const char* (&patterns)[N]):
    m_wrap(patterns, patterns+N)
{
}


template<class A, class B, bool both_are_integral> struct Compare {
    static bool equal(const A& a, const B& b)
    {
        return a == b;
    }
    static bool less(const A& a, const B& b)
    {
        return a < b;
    }
};

template<class A, class B> struct Compare<A, B, true> {
    static bool equal(const A& a, const B& b)
    {
        return util::int_equal_to(a,b);
    }
    static bool less(const A& a, const B& b)
    {
        return util::int_less_than(a,b);
    }
};


template<class A, class B> inline bool equal(const A& a, const B& b)
{
    const bool both_are_integral = std::is_integral<A>::value && std::is_integral<B>::value;
    return Compare<A, B, both_are_integral>::equal(a,b);
}

template<class A, class B> inline bool less(const A& a, const B& b)
{
    const bool both_are_integral = std::is_integral<A>::value && std::is_integral<B>::value;
    return Compare<A, B, both_are_integral>::less(a,b);
}


// Special hooks for comparing zero terminated strings
// FIXME: Also handle (char*, char*), (const char*, char*), (char*, const char*).
// FIXME: Also handle `wchar_t*`.
inline bool equal(const char* a, const char* b)
{
    return std::strcmp(a,b) == 0;
}

inline bool less(const char* a, const char* b)
{
    return std::strcmp(a,b) < 0;
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


template<class T, bool is_float> struct SetPrecision {
    static void exec(std::ostream&) {}
};

template<class T> struct SetPrecision<T, true> {
    static void exec(std::ostream& out)
    {
        out.precision(std::numeric_limits<T>::digits10 + 1);
    }
};

template<class T> void to_string(const T& value, std::string& str)
{
    // FIXME: Put string values in quotes, and escape non-printables as well as '"' and '\\'.
    std::ostringstream out;
    SetPrecision<T, std::is_floating_point<T>::value>::exec(out);
    out << value;
    str = out.str();
}


inline bool TestResults::check_cond(bool cond, const char* file, long line, const char* macro_name,
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

inline bool TestResults::check(bool cond, const char* file, long line, const char* cond_text)
{
    return check_cond(cond, file, line, "CHECK", cond_text);
}

inline bool TestResults::check_not(bool cond, const char* file, long line, const char* cond_text)
{
    return check_cond(!cond, file, line, "CHECK_NOT", cond_text);
}

template<class A, class B>
inline bool TestResults::check_compare(bool cond, const A& a, const B& b,
                                       const char* file, long line, const char* macro_name,
                                       const char* a_text, const char* b_text)
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

inline bool TestResults::check_inexact_compare(bool cond, long double a, long double b,
                                               long double eps, const char* file, long line,
                                               const char* macro_name, const char* a_text,
                                               const char* b_text, const char* eps_text)
{
    if (REALM_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        inexact_compare_failed(file, line, macro_name, a_text, b_text, eps_text, a, b, eps);
    }
    return cond;
}

template<class A, class B>
inline bool TestResults::check_equal(const A& a, const B& b,
                                     const char* file, long line,
                                     const char* a_text, const char* b_text)
{
    bool cond = equal(a,b);
    return check_compare(cond, a, b, file, line, "CHECK_EQUAL", a_text, b_text);
}

template<class A, class B>
inline bool TestResults::check_not_equal(const A& a, const B& b,
                                         const char* file, long line,
                                         const char* a_text, const char* b_text)
{
    bool cond = !equal(a,b);
    return check_compare(cond, a, b, file, line, "CHECK_NOT_EQUAL", a_text, b_text);
}

template<class A, class B>
inline bool TestResults::check_less(const A& a, const B& b,
                                    const char* file, long line,
                                    const char* a_text, const char* b_text)
{
    bool cond = less(a,b);
    return check_compare(cond, a, b, file, line, "CHECK_LESS", a_text, b_text);
}

template<class A, class B>
inline bool TestResults::check_less_equal(const A& a, const B& b,
                                          const char* file, long line,
                                          const char* a_text, const char* b_text)
{
    bool cond = !less(b,a); // Note: Reverse operand order
    return check_compare(cond, a, b, file, line, "CHECK_LESS_EQUAL", a_text, b_text);
}

template<class A, class B>
inline bool TestResults::check_greater(const A& a, const B& b,
                                       const char* file, long line,
                                       const char* a_text, const char* b_text)
{
    bool cond = less(b,a); // Note: Reverse operand order
    return check_compare(cond, a, b, file, line, "CHECK_GREATER", a_text, b_text);
}

template<class A, class B>
inline bool TestResults::check_greater_equal(const A& a, const B& b,
                                             const char* file, long line,
                                             const char* a_text, const char* b_text)
{
    bool cond = !less(a,b);
    return check_compare(cond, a, b, file, line, "CHECK_GREATER_EQUAL", a_text, b_text);
}

inline bool TestResults::check_approximately_equal(long double a, long double b,
                                                   long double eps, const char* file, long line,
                                                   const char* a_text, const char* b_text,
                                                   const char* eps_text)
{
    bool cond = approximately_equal(a, b, eps);
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_APPROXIMATELY_EQUAL",
                                 a_text, b_text, eps_text);
}

inline bool TestResults::check_essentially_equal(long double a, long double b,
                                                 long double eps, const char* file, long line,
                                                 const char* a_text, const char* b_text,
                                                 const char* eps_text)
{
    bool cond = essentially_equal(a, b, eps);
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_ESSENTIALLY_EQUAL",
                                 a_text, b_text, eps_text);
}

inline bool TestResults::check_definitely_less(long double a, long double b,
                                               long double eps, const char* file, long line,
                                               const char* a_text, const char* b_text,
                                               const char* eps_text)
{
    bool cond = definitely_less(a, b, eps);
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_DEFINITELY_LESS",
                                 a_text, b_text, eps_text);
}

inline bool TestResults::check_definitely_greater(long double a, long double b,
                                                  long double eps, const char* file, long line,
                                                  const char* a_text, const char* b_text,
                                                  const char* eps_text)
{
    bool cond = definitely_less(b, a, eps); // Note: Reverse operand order
    return check_inexact_compare(cond, a, b, eps, file, line, "CHECK_DEFINITELY_GREATER",
                                 a_text, b_text, eps_text);
}


} // namespace unit_test
} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_UNIT_TEST_HPP
