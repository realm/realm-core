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
#ifndef TIGHTDB_UNIT_TEST_HPP
#define TIGHTDB_UNIT_TEST_HPP

#include <cstring>
#include <string>
#include <sstream>

#include <tightdb/util/meta.hpp>
#include <tightdb/util/safe_int_ops.hpp>


#define CHECK(cond) tightdb::unit_test::do_cond((cond), __FILE__, __LINE__, #cond)

#define CHECK_EQUAL(a, b) tightdb::unit_test::do_equal((a), (b), __FILE__, __LINE__, #a, #b)

#define CHECK_THROW(expr, exception) \
do { \
    try { \
        (expr); \
        tightdb::unit_test::throw_failed(__FILE__, __LINE__, #expr, #exception); \
    } \
    catch (e&) { \
        tightdb::unit_test::check_suceeded(); \
    } \
} while(false)

#define TEST(name) \
void UnitTest_##name(); \
struct UnitTestReg_##name { \
    UnitTestReg_##name() \
    { \
        tightdb::unit_test::register_test(__FILE__, __LINE__, #name, &UnitTest_##name); \
    } \
}; \
UnitTestReg_##name unit_test_reg_##name; \
void UnitTest_##name()


namespace tightdb {
namespace unit_test {

void register_test(const char* file, long line, const char* name, void (*func)());

void cond_failed(const char* file, long line, const char* cond_text);
void equal_failed(const char* file, long line, const char* a_text, const char* b_text,
                  const std::string& a_val, const std::string& b_val);
void throw_failed(const char* file, long line, const char* expr_text, const char* exception);

void check_succeeded();


template<class A, class B, bool both_are_integral> struct Cmp {
    bool operator()(const A& a, const B& b) { return a == b; }
};
template<class A, class B> struct Cmp<A, B, true> {
    bool operator()(const A& a, const B& b) { return int_equal_to(a,b); }
};

template<class A, class B> inline bool cmp(const A& a, const B& b)
{
    const bool both_are_integral = IsIntegral<A>::value && IsIntegral<B>::value;
    Cmp<A, B, both_are_integral> cmp;
    return cmp(a,b);
}

// Special hook for comparing zero terminated strings
inline bool cmp(const char* a, const char* b) { return std::strcmp(a, b) == 0; }


inline void do_cond(bool cond, const char* file, long line, const char* cond_text)
{
    if (cond) {
        check_succeeded();
    }
    else {
        cond_failed(file, line, cond_text);
    }
}

template<class A, class B>
inline void do_equal(const A& a, const B& b, const char* file, long line,
                     const char* a_text, const char* b_text)
{
    if (cmp(a,b)) {
        check_succeeded();
    }
    else {
        std::string a_val, b_val;
        { std::ostringstream out; out << a; a_val = out.str(); }
        { std::ostringstream out; out << b; b_val = out.str(); }
        equal_failed(file, line, a_text, b_text, a_val, b_val);
    }
}


} // namespace unit_test
} // namespace tightdb

#endif // TIGHTDB_UNIT_TEST_HPP
