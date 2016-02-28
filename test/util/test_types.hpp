/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
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
    TEST_TYPES_EX(name, realm::test_util::unit_test::get_default_test_list(), enabled, true, \
                  __VA_ARGS__)

#define NO_CONCUR_TEST_TYPES(name, ...) \
    NO_CONCUR_TEST_TYPES_IF(name, true, __VA_ARGS__)

#define NO_CONCUR_TEST_TYPES_IF(name, enabled, ...) \
    TEST_TYPES_EX(name, realm::test_util::unit_test::get_default_test_list(), enabled, false, \
                  __VA_ARGS__)

#define TEST_TYPES_EX(name, list, enabled, allow_concur, ...) \
    template<class> \
    struct Realm_UnitTest__##name: realm::test_util::unit_test::TestBase { \
        static bool test_enabled() { return bool(enabled); } \
        Realm_UnitTest__##name(realm::test_util::unit_test::TestContext& c): TestBase(c) {} \
        void test_run(); \
    }; \
    realm::test_util::unit_test::RegisterTypeTests<Realm_UnitTest__##name, __VA_ARGS__> \
        realm_unit_test_reg__##name((list), (allow_concur), "DefaultSuite", \
                                    #name, __FILE__, __LINE__); \
    template<class TEST_TYPE> void Realm_UnitTest__##name<TEST_TYPE>::test_run()


namespace realm {
namespace test_util {
namespace unit_test {

inline std::string sanitize_type_test_name(const char* test_name, std::string type_name)
{
    auto replace = [&](std::string a, std::string b) {
        std::string::size_type offset = 0;
        for (;;) {
            std::string::size_type i = type_name.find(a, offset);
            if (i == std::string::npos)
                break;
            type_name.replace(i, a.size(), b);
            offset = i = b.size();
        }
    };
    replace("(anonymous namespace)", "anon"); // GCC specific
    replace(" >", ">");
    replace(" ", "+"); // "long double" -> "long+double"
    return std::string(test_name) + '<' + type_name + '>';
}

template<template<class> class, class...> struct RegisterTypeTests;
template<template<class> class Test, class Type, class... Types>
struct RegisterTypeTests<Test, Type, Types...> {
    RegisterTypeTests(TestList& list, bool allow_concur, const char* suite, const char* name,
                      const char* file, long line)
    {
        std::string name_2 = sanitize_type_test_name(name, get_type_name<Type>());
        RegisterTest<Test<Type>> dummy_1(list, allow_concur, suite, name_2, file, line);
        RegisterTypeTests<Test, Types...> dummy_2(list, allow_concur, suite, name, file, line);
    }
};
template<template<class> class Test> struct RegisterTypeTests<Test> {
    RegisterTypeTests(TestList&, bool, const char*, const char*, const char*, long)
    {
    }
};

} // namespace unit_test
} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_TEST_TYPES_HPP
