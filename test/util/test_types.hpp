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
#ifndef REALM_TEST_UTIL_TEST_TYPES_HPP
#define REALM_TEST_UTIL_TEST_TYPES_HPP

#include "unit_test.hpp"
#include "demangle.hpp"


#define TEST_TYPES(name, ...) \
    TEST_TYPES_IF(name, true, __VA_ARGS__)

#define TEST_TYPES_IF(name, enabled, ...) \
    TEST_TYPES_EX(name, realm::test_util::unit_test::get_default_test_list(), enabled, __VA_ARGS__)

#define TEST_TYPES_EX(name, list, enabled, ...) \
    template<class> struct Realm_UnitTest__##name: realm::test_util::unit_test::Test { \
        bool test_enabled() const { return bool(enabled); } \
        void test_run(); \
    }; \
    realm::test_util::unit_test::TestCons<Realm_UnitTest__##name, __VA_ARGS__> realm_unit_test__##name; \
    realm::test_util::unit_test::RegisterTests \
        realm_unit_test_reg__##name((list), realm_unit_test__##name, \
                                    "DefaultSuite", #name, __FILE__, __LINE__); \
    template<class TEST_TYPE> void Realm_UnitTest__##name<TEST_TYPE>::test_run()


namespace realm {
namespace test_util {
namespace unit_test {


template<template<class> class Test, class...> class TestCons;

template<template<class> class Test, class Type, class... Types>
class TestCons<Test, Type, Types...> {
public:
    Test<Type> head;
    TestCons<Test, Types...> tail;
};
template<template<class> class Test> class TestCons<Test> {};


struct RegisterTests {
    template<template<class> class Test, class... Types>
    RegisterTests(TestList& list, TestCons<Test, Types...>& tests, const char* suite,
                 const char* name, const char* file, long line)
    {
        register_tests(list, tests, suite, name, file, line);
    }
    template<template<class> class Test, class Type, class... Types>
    static void register_tests(TestList& list, TestCons<Test, Type, Types...>& tests,
                               const char* suite, const char* name, const char* file, long line)
    {
        std::string name_2 = name;
        name_2 += '[';
        name_2 += get_type_name<Type>();
        name_2 += ']';
        RegisterTest::register_test(list, tests.head, suite, name_2, file, line);
        register_tests(list, tests.tail, suite, name, file, line);
    }
    template<template<class> class Test>
    static void register_tests(TestList&, TestCons<Test>&,
                               const char*, const char*, const char*, long)
    {
    }
};


} // namespace unit_test
} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_TEST_TYPES_HPP
