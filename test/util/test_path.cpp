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

#include "test_path.hpp"

#include <realm/util/file.hpp>

#include <algorithm>
#include <string>

#if REALM_PLATFORM_APPLE
#include <realm/util/cf_ptr.hpp>

#include <CoreFoundation/CoreFoundation.h>
#include <sys/mount.h>
#include <sys/param.h>
#elif defined(_WIN32)
#include <Windows.h>
// PathCchRemoveFileSpec()
#include <pathcch.h>
#pragma comment(lib, "Pathcch.lib")
#else
#include <unistd.h>
#include <libgen.h>
#endif

using namespace realm::util;

namespace {

bool g_keep_files = false;

std::string g_path_prefix;
std::string g_resource_path;

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

#if REALM_PLATFORM_APPLE
std::string url_to_path(CFURLRef url)
{
    auto absolute = adoptCF(CFURLCopyAbsoluteURL(url));
    auto path = adoptCF(CFURLCopyPath(absolute.get()));
    auto length = CFStringGetLength(path.get());
    std::string ret;
    ret.resize(CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8));
    CFIndex bytes_written;
    CFStringGetBytes(path.get(), {0, length}, kCFStringEncodingUTF8, 0, false, reinterpret_cast<uint8_t*>(ret.data()),
                     ret.size(), &bytes_written);
    REALM_ASSERT(bytes_written);
    ret.resize(bytes_written);
    return ret;
}
#endif

} // anonymous namespace

namespace realm::test_util {

void keep_test_files()
{
    g_keep_files = true;
}

std::string get_test_path(const std::string& test_name, const std::string& suffix)
{
    return g_path_prefix + sanitize_for_file_name(test_name) + suffix;
}

std::string get_test_path_prefix()
{
    return g_path_prefix;
}

bool initialize_test_path(int argc, const char* argv[])
{
#if REALM_PLATFORM_APPLE
    // On Apple platforms we copy everything into a read-only bundle containing
    // the test executable and resource files, and have to create test files in
    // a temporary directory.
#if REALM_APPLE_DEVICE || TARGET_OS_SIMULATOR
    auto home = adoptCF(CFCopyHomeDirectoryURL());
    g_path_prefix = url_to_path(home.get()) + "Documents/";
#else
    g_path_prefix = util::make_temp_dir() + "/";
#endif

    auto resources_url = adoptCF(CFBundleCopyResourcesDirectoryURL(CFBundleGetMainBundle()));
    g_resource_path = url_to_path(resources_url.get());

    // On other platforms we can write to the executable's directory, so we use
    // that as the base path.
#elif defined(_MSC_VER)
    wchar_t path[MAX_PATH];
    if (GetModuleFileName(NULL, path, MAX_PATH) == 0) {
        fprintf(stderr, "Failed to retrieve path to exectuable.\n");
        return false;
    }
    PathCchRemoveFileSpec(path, MAX_PATH);
    SetCurrentDirectory(path);
    g_resource_path = "resources\\";
#else
    char executable[PATH_MAX];
    if (realpath(argv[0], executable) == nullptr) {
        fprintf(stderr, "Failed to retrieve path to exectuable.\n");
        return false;
    }
    const char* directory = dirname(executable);
    if (chdir(directory) < 0) {
        fprintf(stderr, "Failed to change directory.\n");
        return false;
    }
    g_resource_path = "resources/";
#endif

    if (argc > 1) {
        g_path_prefix = argv[1];
    }

    return true;
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
    std::string fs_typename = fsbuf.f_fstypename;
    std::transform(fs_typename.begin(), fs_typename.end(), fs_typename.begin(), toLowerAscii);
    return fs_typename.find(std::string("exfat")) != std::string::npos;
#else
    return false;
#endif
}

std::string get_test_resource_path()
{
    return g_resource_path;
}

TestPathGuard::TestPathGuard(const std::string& path)
    : m_path(path)
{
    File::try_remove(m_path);
}

TestPathGuard::~TestPathGuard() noexcept
{
    if (g_keep_files)
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
    if (g_keep_files)
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
    if (!g_keep_files && !m_path.empty())
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

} // namespace realm::test_util
