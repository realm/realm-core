/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_TEST_UTIL_UNIT_TEST_HPP
#define TIGHTDB_TEST_UTIL_UNIT_TEST_HPP

#include <cmath>
#include <cstring>
#include <algorithm>
#include <string>
#include <sstream>
#include <ostream>

#include <tightdb/util/features.h>
#include <tightdb/util/type_traits.hpp>
#include <tightdb/util/safe_int_ops.hpp>


#define TEST(name) \
    void Tightdb_UnitTest__##name(); \
    tightdb::test_util::unit_test::RegisterTest \
    tightdb_unit_test__##name(__FILE__, __LINE__, #name, &Tightdb_UnitTest__##name); \
    void Tightdb_UnitTest__##name()


#define CHECK(cond) \
    tightdb::test_util::unit_test::check(bool(cond), __FILE__, __LINE__, #cond)

#define CHECK_EQUAL(a,b) \
    tightdb::test_util::unit_test::check_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_NOT_EQUAL(a,b) \
    tightdb::test_util::unit_test::check_not_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS(a,b) \
    tightdb::test_util::unit_test::check_less((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS_EQUAL(a,b) \
    tightdb::test_util::unit_test::check_less_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER(a,b) \
    tightdb::test_util::unit_test::check_greater((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER_EQUAL(a,b) \
    tightdb::test_util::unit_test::check_greater_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_THROW(expr, exception) \
    do { \
        try { \
            (expr); \
            tightdb::test_util::unit_test::throw_failed(__FILE__, __LINE__, #expr, #exception); \
        } \
        catch (exception&) { \
            tightdb::test_util::unit_test::check_succeeded(); \
        } \
    } \
    while(false)

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
    tightdb::test_util::unit_test::check_approximately_equal((a), (b), (epsilon), \
                                                             __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_ESSENTIALLY_EQUAL(a, b, epsilon) \
    tightdb::test_util::unit_test::check_essentially_equal((a), (b), (epsilon), \
                                                           __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_DEFINITELY_LESS(a, b, epsilon) \
    tightdb::test_util::unit_test::check_definitely_less((a), (b), (epsilon), \
                                                         __FILE__, __LINE__, #a, #b, #epsilon)

#define CHECK_DEFINITELY_GREATER(a, b, epsilon) \
    tightdb::test_util::unit_test::check_definitely_greater((a), (b), (epsilon), \
                                                            __FILE__, __LINE__, #a, #b, #epsilon)

//@}

namespace tightdb {
namespace test_util {
namespace unit_test {

struct RegisterTest {
    RegisterTest(const char* file, long line, const char* name, void (*func)());
};

void cond_failed(const char* file, long line, const char* cond_text);
void compare_failed(const char* file, long line, const char* macro_name,
                    const char* a_text, const char* b_text,
                    const std::string& a_val, const std::string& b_val);
void inexact_compare_failed(const char* file, long line, const char* macro_name,
                            const char* a_text, const char* b_text, const char* eps_text,
                            long double a, long double b, long double eps);
void throw_failed(const char* file, long line, const char* expr_text, const char* exception);

void check_succeeded();


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
    const bool both_are_integral = util::IsIntegral<A>::value && util::IsIntegral<B>::value;
    return Compare<A, B, both_are_integral>::equal(a,b);
}
template<class A, class B> inline bool less(const A& a, const B& b)
{
    const bool both_are_integral = util::IsIntegral<A>::value && util::IsIntegral<B>::value;
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
    using namespace std;
    return abs(a - b) <= max(abs(a), abs(b)) * epsilon;
}
inline bool essentially_equal(long double a, long double b, long double epsilon)
{
    using namespace std;
    return abs(a - b) <= min(abs(a), abs(b)) * epsilon;
}
inline bool definitely_less(long double a, long double b, long double epsilon)
{
    using namespace std;
    return b - a > max(abs(a), abs(b)) * epsilon;
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
    SetPrecision<T, util::IsFloatingPoint<T>::value>::exec(out);
    out << value;
    str = out.str();
}


inline void check(bool cond, const char* file, long line, const char* cond_text)
{
    if (TIGHTDB_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        cond_failed(file, line, cond_text);
    }
}

template<class A, class B>
inline void check_compare(bool cond, const A& a, const B& b, const char* file, long line,
                          const char* macro_name, const char* a_text, const char* b_text)
{
    if (TIGHTDB_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        std::string a_val, b_val;
        to_string(a, a_val);
        to_string(b, b_val);
        compare_failed(file, line, macro_name, a_text, b_text, a_val, b_val);
    }
}

inline void check_inexact_compare(bool cond, long double a, long double b, long double eps,
                                  const char* file, long line, const char* macro_name,
                                  const char* a_text, const char* b_text, const char* eps_text)
{
    if (TIGHTDB_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        inexact_compare_failed(file, line, macro_name, a_text, b_text, eps_text, a, b, eps);
    }
}

template<class A, class B>
inline void check_equal(const A& a, const B& b, const char* file, long line,
                        const char* a_text, const char* b_text)
{
    bool cond = equal(a,b);
    check_compare(cond, a, b, file, line, "CHECK_EQUAL", a_text, b_text);
}

template<class A, class B>
inline void check_not_equal(const A& a, const B& b, const char* file, long line,
                            const char* a_text, const char* b_text)
{
    bool cond = !equal(a,b);
    check_compare(cond, a, b, file, line, "CHECK_NOT_EQUAL", a_text, b_text);
}

template<class A, class B>
inline void check_less(const A& a, const B& b, const char* file, long line,
                       const char* a_text, const char* b_text)
{
    bool cond = less(a,b);
    check_compare(cond, a, b, file, line, "CHECK_LESS", a_text, b_text);
}

template<class A, class B>
inline void check_less_equal(const A& a, const B& b, const char* file, long line,
                             const char* a_text, const char* b_text)
{
    bool cond = !less(b,a);
    check_compare(cond, a, b, file, line, "CHECK_LESS_EQUAL", a_text, b_text);
}

template<class A, class B>
inline void check_greater(const A& a, const B& b, const char* file, long line,
                          const char* a_text, const char* b_text)
{
    bool cond = less(b,a);
    check_compare(cond, a, b, file, line, "CHECK_GREATER", a_text, b_text);
}

template<class A, class B>
inline void check_greater_equal(const A& a, const B& b, const char* file, long line,
                                const char* a_text, const char* b_text)
{
    bool cond = !less(a,b);
    check_compare(cond, a, b, file, line, "CHECK_GREATER_EQUAL", a_text, b_text);
}

inline void check_approximately_equal(long double a, long double b, long double eps,
                                      const char* file, long line,
                                      const char* a_text, const char* b_text,
                                      const char* eps_text)
{
    bool cond = approximately_equal(a, b, eps);
    check_inexact_compare(cond, a, b, eps, file, line, "CHECK_APPROXIMATELY_EQUAL",
                          a_text, b_text, eps_text);
}

inline void check_essentially_equal(long double a, long double b, long double eps,
                                    const char* file, long line,
                                    const char* a_text, const char* b_text,
                                    const char* eps_text)
{
    bool cond = essentially_equal(a, b, eps);
    check_inexact_compare(cond, a, b, eps, file, line, "CHECK_ESSENTIALLY_EQUAL",
                          a_text, b_text, eps_text);
}

inline void check_definitely_less(long double a, long double b, long double eps,
                                  const char* file, long line,
                                  const char* a_text, const char* b_text,
                                  const char* eps_text)
{
    bool cond = definitely_less(a, b, eps);
    check_inexact_compare(cond, a, b, eps, file, line, "CHECK_DEFINITELY_LESS",
                          a_text, b_text, eps_text);
}

inline void check_definitely_greater(long double a, long double b, long double eps,
                                     const char* file, long line,
                                     const char* a_text, const char* b_text,
                                     const char* eps_text)
{
    bool cond = definitely_less(b, a, eps);
    check_inexact_compare(cond, a, b, eps, file, line, "CHECK_DEFINITELY_GREATER",
                          a_text, b_text, eps_text);
}


struct Location {
    const char* test_name;
    const char* file_name;
    long line_number;
};

struct Summary {
    long num_included_tests;
    long num_failed_tests;
    long num_excluded_tests;
    long long num_checks;
    long long num_failed_checks;
    double elapsed_seconds;
};

class Reporter {
public:
    virtual void begin(const Location&);
    virtual void fail(const Location&, const std::string& message);
    virtual void end(const Location&, double elapsed_seconds);
    virtual void summary(const Summary&);
    virtual ~Reporter() TIGHTDB_NOEXCEPT {}
};

class Filter {
public:
    virtual bool include(const Location&) = 0;
    virtual ~Filter() TIGHTDB_NOEXCEPT {}
};

bool run(Reporter* = 0, Filter* = 0);


class SimpleReporter: public Reporter {
public:
    explicit SimpleReporter(bool report_progress = false);

    void begin(const Location&) TIGHTDB_OVERRIDE;
    void fail(const Location&, const std::string&) TIGHTDB_OVERRIDE;
    void summary(const Summary&) TIGHTDB_OVERRIDE;

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
/// An empty filter is functionally equivalent to "*" and a filter on
/// the form "- ..." is equivalent to "* - ...".
///
/// Examples:
///
///     ""                 everything
///     "*"                everything
///     "* -"              everything
///     "-"                everything
///     "- *"              nothing
///     "* - *"            nothing
///     "Array_Count"      only "Array_Count"
///     "- Array_*"        all except those that match "Array_*"
///     "Array_* Column_* - *_Count"
Filter* create_wildcard_filter(const std::string&);


} // namespace unit_test
} // namespace test_util
} // namespace tightdb

#endif // TIGHTDB_TEST_UTIL_UNIT_TEST_HPP
