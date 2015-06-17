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
#ifndef REALM_TEST_UTIL_TEST_PATH_HPP
#define REALM_TEST_UTIL_TEST_PATH_HPP

#include <string>

#include <realm/util/features.h>

#include "unit_test.hpp"

#define TEST_PATH_HELPER(class_name, var_name, suffix) \
    class_name var_name(realm::test_util::get_test_path(test_details, "." #var_name "." suffix))

#define TEST_PATH(var_name) \
    TEST_PATH_HELPER(realm::test_util::TestPathGuard, var_name, "test");

#define GROUP_TEST_PATH(var_name) \
    TEST_PATH_HELPER(realm::test_util::TestPathGuard, var_name, "realm");

#define SHARED_GROUP_TEST_PATH(var_name) \
    TEST_PATH_HELPER(realm::test_util::SharedGroupTestPathGuard, var_name, "realm");

namespace realm {
namespace test_util {

/// Disable removal of test files. If called, the call must complete
/// before any TestPathGuard object is created.
void keep_test_files();

/// By default, test files are placed in the current working
/// directory. Use this function to set a path prefix. The specified
/// prefix must contain a final `/`.
void set_test_path_prefix(const std::string&);
std::string get_test_path_prefix();

std::string get_test_path(const unit_test::TestDetails&, const std::string& suffix);

std::string get_test_resource_path();
void set_test_resource_path(const std::string&);


/// Constructor and destructor removes file if it exists.
class TestPathGuard {
public:
    TestPathGuard(const std::string& path);
    ~TestPathGuard() REALM_NOEXCEPT;
    operator std::string() const
    {
        return m_path;
    }
    const char* c_str() const
    {
        return m_path.c_str();
    }
protected:
    std::string m_path;
};

class SharedGroupTestPathGuard: public TestPathGuard {
public:
    SharedGroupTestPathGuard(const std::string& path);
    std::string get_lock_path() const
    {
        return m_path + ".lock";
    }
    ~SharedGroupTestPathGuard();
};

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_TEST_PATH_HPP
