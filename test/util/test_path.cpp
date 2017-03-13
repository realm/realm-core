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

#include <sstream>
#include <string>

#include <realm/util/file.hpp>

#include "test_path.hpp"

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
        File::try_remove(m_path);
    }
    catch (...) {
        // Exception deliberately ignored
    }
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


SharedGroupTestPathGuard::SharedGroupTestPathGuard(const std::string& path)
    : TestPathGuard(path)
{
    cleanup();
}


SharedGroupTestPathGuard::~SharedGroupTestPathGuard() noexcept
{
    if (!keep_files)
        cleanup();
}

void SharedGroupTestPathGuard::cleanup() const noexcept
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

} // namespace test_util
} // namespace realm
