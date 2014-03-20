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

#include <cstring>
#include <string>
#include <sstream>
#include <ostream>

#include <tightdb/util/features.h>
#include <tightdb/util/type_traits.hpp>
#include <tightdb/util/safe_int_ops.hpp>


#define CHECK(cond) \
    tightdb::test_util::unit_test::test(bool(cond), __FILE__, __LINE__, #cond)

#define CHECK_EQUAL(a,b) \
    tightdb::test_util::unit_test::test_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_NOT_EQUAL(a,b) \
    tightdb::test_util::unit_test::test_not_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS(a,b) \
    tightdb::test_util::unit_test::test_less((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_LESS_EQUAL(a,b) \
    tightdb::test_util::unit_test::test_less_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER(a,b) \
    tightdb::test_util::unit_test::test_greater((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_GREATER_EQUAL(a,b) \
    tightdb::test_util::unit_test::test_greater_equal((a), (b), __FILE__, __LINE__, #a, #b)

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

#define TEST(name) \
    void Tightdb_UnitTest__##name(); \
    tightdb::test_util::unit_test::RegisterTest \
    tightdb_unit_test__##name(__FILE__, __LINE__, #name, &Tightdb_UnitTest__##name); \
    void Tightdb_UnitTest__##name()


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
inline bool equal(const char* a, const char* b)
{
    return std::strcmp(a,b) == 0;
}
inline bool less(const char* a, const char* b)
{
    return std::strcmp(a,b) < 0;
}


inline void test(bool cond, const char* file, long line, const char* cond_text)
{
    if (TIGHTDB_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        cond_failed(file, line, cond_text);
    }
}

template<class A, class B>
inline void test_compare(bool cond, const A& a, const B& b, const char* file, long line,
                         const char* macro_name, const char* a_text, const char* b_text)
{
    if (TIGHTDB_LIKELY(cond)) {
        check_succeeded();
    }
    else {
        std::string a_val, b_val;
        {
            std::ostringstream out;
            out << a;
            a_val = out.str();
        }
        {
            std::ostringstream out;
            out << b;
            b_val = out.str();
        }
        compare_failed(file, line, macro_name, a_text, b_text, a_val, b_val);
    }
}

template<class A, class B>
inline void test_equal(const A& a, const B& b, const char* file, long line,
                       const char* a_text, const char* b_text)
{
    bool cond = equal(a,b);
    test_compare(cond, a, b, file, line, "TEST_EQUAL", a_text, b_text);
}

template<class A, class B>
inline void test_not_equal(const A& a, const B& b, const char* file, long line,
                           const char* a_text, const char* b_text)
{
    bool cond = !equal(a,b);
    test_compare(cond, a, b, file, line, "TEST_NOT_EQUAL", a_text, b_text);
}

template<class A, class B>
inline void test_less(const A& a, const B& b, const char* file, long line,
                      const char* a_text, const char* b_text)
{
    bool cond = less(a,b);
    test_compare(cond, a, b, file, line, "TEST_LESS", a_text, b_text);
}

template<class A, class B>
inline void test_less_equal(const A& a, const B& b, const char* file, long line,
                            const char* a_text, const char* b_text)
{
    bool cond = !less(b,a);
    test_compare(cond, a, b, file, line, "TEST_LESS_EQUAL", a_text, b_text);
}

template<class A, class B>
inline void test_greater(const A& a, const B& b, const char* file, long line,
                         const char* a_text, const char* b_text)
{
    bool cond = less(b,a);
    test_compare(cond, a, b, file, line, "TEST_GREATER", a_text, b_text);
}

template<class A, class B>
inline void test_greater_equal(const A& a, const B& b, const char* file, long line,
                               const char* a_text, const char* b_text)
{
    bool cond = !less(a,b);
    test_compare(cond, a, b, file, line, "TEST_GREATER_EQUAL", a_text, b_text);
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
    long num_checks;
    long num_failed_checks;
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
