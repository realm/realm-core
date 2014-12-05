#include <tightdb/util/file.hpp>

#include "test_path.hpp"

using namespace std;
using namespace tightdb::util;
using namespace tightdb::test_util::unit_test;

namespace {

bool keep_files = false;

string path_prefix;
string resource_path;

} // anonymous namespace

namespace tightdb {
namespace test_util {


void keep_test_files()
{
    keep_files = true;
}

string get_test_path(const TestDetails& test_details, const char* suffix)
{
    string path = path_prefix;
    path += test_details.test_name;
    path += suffix;
    return path;
}

void set_test_path_prefix(const string& prefix)
{
    path_prefix = prefix;
}

string get_test_path_prefix()
{
    return path_prefix;
}

string get_test_resource_path()
{
    return resource_path;
}

void set_test_resource_path(const string& path)
{
    resource_path = path;
}

TestPathGuard::TestPathGuard(const string& path):
    m_path(path)
{
    File::try_remove(m_path);
}

TestPathGuard::~TestPathGuard() TIGHTDB_NOEXCEPT
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


SharedGroupTestPathGuard::SharedGroupTestPathGuard(const string& path):
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
} // namespace tightdb
