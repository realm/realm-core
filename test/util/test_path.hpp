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

#ifndef REALM_TEST_UTIL_TEST_PATH_HPP
#define REALM_TEST_UTIL_TEST_PATH_HPP

#include <string>

#include <realm/util/features.h>

#define TEST_PATH_HELPER(class_name, var_name, suffix)                                                               \
    class_name var_name(realm::test_util::get_test_path(test_context.get_test_name(), "." #var_name "." suffix))

#define TEST_PATH(var_name) TEST_PATH_HELPER(realm::test_util::TestPathGuard, var_name, "test");

#define TEST_DIR(var_name) TEST_PATH_HELPER(realm::test_util::TestDirGuard, var_name, "test-dir");

#define GROUP_TEST_PATH(var_name) TEST_PATH_HELPER(realm::test_util::TestPathGuard, var_name, "realm");

#define SHARED_GROUP_TEST_PATH(var_name)                                                                             \
    TEST_PATH_HELPER(realm::test_util::SharedGroupTestPathGuard, var_name, "realm");

namespace realm {
namespace test_util {

/// Disable automatic removal of test files.
///
/// This function is **not** thread-safe. If you call it, be sure to call it
/// prior to any execution of the TEST_PATH or TEST_DIR family of macros.
void keep_test_files();


/// This function is thread-safe as long as there are no concurrent invocations
/// of set_test_path_prefix().
std::string get_test_path_prefix();

/// This function is thread-safe as long as there are no concurrent invocations
/// of set_test_path_prefix().
std::string get_test_path(const std::string& path, const std::string& suffix);

/// By default, test files are placed in the current working
/// directory. Use this function to set a path prefix. The specified
/// prefix must contain a final `/`.
///
/// This function is **not** thread-safe. If you call it, be sure to call it
/// prior to any invocation of get_test_path_prefix() or get_test_path(), and
/// prior to any execution of the TEST_PATH or TEST_DIR family of macros.
void set_test_path_prefix(const std::string&);


/// This function is thread-safe as long as there are no concurrent invocations
/// of set_test_resource_path().
std::string get_test_resource_path();


/// Set the path to be returned by get_test_resource_path().
///
/// This function is **not** thread-safe. If you call it, be sure to call it
/// prior to any invocation of get_test_resource_path().
void set_test_resource_path(const std::string&);


// This is an adapter class which replaces dragging in the whole test framework
// by implementing the `get_test_name()` method from the TestContext class.
// It allows use of TestPathGuard and friends outside of a unit test:
// RealmPathInfo test_context { path };
struct RealmPathInfo {
    std::string m_path;
    std::string get_test_name() const { return m_path; }
};


/// Constructor and destructor removes file if it exists.
class TestPathGuard {
public:
    TestPathGuard(const std::string& path);
    ~TestPathGuard() noexcept;
    operator std::string() const
    {
        return m_path;
    }
    const char* c_str() const
    {
        return m_path.c_str();
    }
    TestPathGuard(const TestPathGuard&) = delete;
    TestPathGuard& operator=(const TestPathGuard&) = delete;

protected:
    std::string m_path;
};

/// The constructor creates the directory if it does not already exist, then
/// removes any files already in it. The destructor removes files in the
/// directory, then removes the directory.
class TestDirGuard {
public:
    TestDirGuard(const std::string& path);
    ~TestDirGuard() noexcept;
    operator std::string() const
    {
        return m_path;
    }
    const char* c_str() const
    {
        return m_path.c_str();
    }

private:
    std::string m_path;
    void clean_dir(const std::string& path);
};

class SharedGroupTestPathGuard : public TestPathGuard {
public:
    SharedGroupTestPathGuard(const std::string& path);
    std::string get_lock_path() const
    {
        return m_path + ".lock"; // ".management/access_control";
    }
    ~SharedGroupTestPathGuard() noexcept;

private:
    void cleanup() const noexcept;
};

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_TEST_PATH_HPP
