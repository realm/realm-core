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

#include <algorithm>
#include <sstream>
#include <string>

#include <realm/util/file.hpp>

#include "test_path.hpp"

#if REALM_PLATFORM_APPLE
#include <sys/mount.h>
#include <sys/param.h>
#endif

using namespace realm::util;

namespace {

bool keep_files = false;

std::string path_prefix;
std::string resource_path;

#ifdef _WIN32
std::string sanitize_for_file_name(std::string str)
{
    static const std::string invalid("<>:\"|?*\\/");
    std::transform(str.begin(), str.end(), str.begin(), [](char c) {
        if (invalid.find(c) != std::string::npos)
            return '-';
        return c;
    });
    return str;
}
#else
std::string sanitize_for_file_name(const std::string& str)
{
    return str;
}
#endif

std::locale locale_classic = std::locale::classic();

} // anonymous namespace

namespace realm {
namespace test_util {


void keep_test_files()
{
    keep_files = true;
}

std::string get_test_path(const std::string& test_name, const std::string& suffix)
{
    std::ostringstream out;
    out.imbue(locale_classic);
    out << path_prefix << sanitize_for_file_name(test_name) << suffix;
    return out.str();
}

void set_test_path_prefix(const std::string& prefix)
{
    path_prefix = prefix;
}

std::string get_test_path_prefix()
{
    return path_prefix;
}

bool test_dir_is_exfat()
{
#if REALM_PLATFORM_APPLE
    if (test_util::get_test_path_prefix().empty())
        return false;

    struct statfs fsbuf;
    int ret = statfs(test_util::get_test_path_prefix().c_str(), &fsbuf);
    REALM_ASSERT_RELEASE(ret == 0);
    // The documentation and headers helpfully don't list any of the values of
    // f_type or provide constants for them
    return fsbuf.f_type == 28 /* exFAT */;
#else
    return false;
#endif
}

std::string get_test_resource_path()
{
    return resource_path;
}

void set_test_resource_path(const std::string& path)
{
    resource_path = path;
}

TestPathGuard::TestPathGuard(const std::string& path)
    : m_path(path)
{
    File::try_remove(m_path);
}

TestPathGuard::~TestPathGuard() noexcept
{
    if (keep_files)
        return;
    try {
        if (!m_path.empty())
            File::try_remove(m_path);
    }
    catch (...) {
        // Exception deliberately ignored
    }
}

TestPathGuard::TestPathGuard(TestPathGuard&& other) noexcept
    : m_path(std::move(other.m_path))
{
    other.m_path.clear();
}

TestPathGuard& TestPathGuard::operator=(TestPathGuard&& other) noexcept
{
    m_path = std::move(other.m_path);
    other.m_path.clear();
    return *this;
}


TestDirGuard::TestDirGuard(const std::string& path)
    : m_path(path)
{
    if (!try_make_dir(path)) {
        clean_dir(path);
    }
}

TestDirGuard::~TestDirGuard() noexcept
{
    if (keep_files)
        return;
    try {
        clean_dir(m_path);
        remove_dir(m_path);
    }
    catch (...) {
        // Exception deliberately ignored
    }
}

namespace {
void do_clean_dir(const std::string& path, const std::string& guard_string)
{
    DirScanner ds(path, true);
    std::string name;
    while (ds.next(name)) {
        std::string subpath = File::resolve(name, path);
        if (File::is_dir(subpath)) {
            do_clean_dir(subpath, guard_string);
            remove_dir(subpath);
        }
        else {
            // Try to avoid accidental removal of precious files due to bugs in
            // TestDirGuard or TEST_DIR macro.
            if (subpath.find(guard_string) == std::string::npos)
                throw std::runtime_error("Bad test dir path");
            File::remove(subpath);
        }
    }
}
}

void TestDirGuard::clean_dir(const std::string& path)
{
    do_clean_dir(path, ".test-dir");
}


DBTestPathGuard::DBTestPathGuard(const std::string& path)
    : TestPathGuard(path)
{
    cleanup();
}


DBTestPathGuard::~DBTestPathGuard() noexcept
{
    if (!keep_files && !m_path.empty())
        cleanup();
}

void DBTestPathGuard::cleanup() const noexcept
{
    try {
        do_clean_dir(m_path + ".management", ".management");
        if (File::is_dir(m_path + ".management"))
            remove_dir(m_path + ".management");
        File::try_remove(get_lock_path());
    }
    catch (...) {
        // Exception deliberately ignored
    }
}

TestDirNameGenerator::TestDirNameGenerator(std::string path)
    : m_path{std::move(path)}
{
}

std::string TestDirNameGenerator::next()
{
    return m_path + "/" + std::to_string(m_counter++);
}

} // namespace test_util
} // namespace realm
