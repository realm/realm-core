#include <realm/util/file.hpp>

#include "test_path.hpp"

using namespace realm::util;
using namespace realm::test_util::unit_test;

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
std::string sanitize_for_file_name(const std::string& str) { return str; }
#endif

} // anonymous namespace

namespace realm {
namespace test_util {


void keep_test_files()
{
    keep_files = true;
}

std::string get_test_path(const TestDetails& test_details, const std::string& suffix)
{
    std::string path = path_prefix;
    path += sanitize_for_file_name(test_details.test_name);
    path += suffix;
    return path;
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

TestPathGuard::TestPathGuard(const std::string& path):
    m_path(path)
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


TestDirGuard::TestDirGuard(const std::string& path):
    m_path(path)
{
    if (!File::exists(path)) {
        make_dir(path);
    }
    else {
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

void TestDirGuard::clean_dir(const std::string& path)
{
    DirScanner ds(path);
    std::string name;
    while (ds.next(name)) {
        std::string subpath = File::resolve(name, path);
        if (File::is_dir(subpath)) {
            clean_dir(subpath);
            remove_dir(subpath);
        }
        else {
            // Try to avoid accidental removal of precious files due to bugs in
            // TestDirGuard or TEST_DIR macro.
            if (subpath.find(".test-dir") == std::string::npos)
                throw std::runtime_error("Bad test dir path");
            File::remove(subpath);
        }
    }
}


SharedGroupTestPathGuard::SharedGroupTestPathGuard(const std::string& path):
    TestPathGuard(path)
{
    File::try_remove(get_lock_path());
    File::try_remove(m_path + ".log_a");
    File::try_remove(m_path + ".log_b");
    File::try_remove(m_path + "_bm.lck");
    File::try_remove(m_path + "_cm.lck");
    File::try_remove(m_path + "_wm.lck");
}


SharedGroupTestPathGuard::~SharedGroupTestPathGuard() noexcept
{
    if (keep_files)
        return;
    try {
        File::try_remove(get_lock_path());
        File::try_remove(m_path + ".log_a");
        File::try_remove(m_path + ".log_b");
        File::try_remove(m_path + "_bm.lck");
        File::try_remove(m_path + "_cm.lck");
        File::try_remove(m_path + "_wm.lck");
    }
    catch (...) {
        // Exception deliberately ignored
    }
}

} // namespace test_util
} // namespace realm
