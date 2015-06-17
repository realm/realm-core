#include <realm/util/file.hpp>

#include "test_path.hpp"

using namespace realm::util;
using namespace realm::test_util::unit_test;

namespace {

bool keep_files = false;

std::string path_prefix;
std::string resource_path;

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
    path += test_details.test_name;
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

TestPathGuard::~TestPathGuard() REALM_NOEXCEPT
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


SharedGroupTestPathGuard::SharedGroupTestPathGuard(const std::string& path):
    TestPathGuard(path)
{
    File::try_remove(get_lock_path());
    File::try_remove(m_path + ".log_a");
    File::try_remove(m_path + ".log_b");
}


SharedGroupTestPathGuard::~SharedGroupTestPathGuard()
{
    File::try_remove(get_lock_path());
    File::try_remove(m_path + ".log_a");
    File::try_remove(m_path + ".log_b");
}

} // namespace test_util
} // namespace realm
