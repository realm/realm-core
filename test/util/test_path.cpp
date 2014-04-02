#include <tightdb/util/file.hpp>

#include "test_path.hpp"

using namespace std;
using namespace tightdb::util;
using namespace tightdb::test_util::unit_test;

namespace {

bool keep_files = false;

} // anonymous namespace

namespace tightdb {
namespace test_util {


void keep_test_files()
{
    keep_files = true;
}


string get_test_path(const TestDetails& test_details, const char* suffix)
{
    string path = test_details.test_name;
    path += suffix;
    return path;
}


TestPathGuard::TestPathGuard(const std::string& path):
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


SharedGroupTestPathGuard::SharedGroupTestPathGuard(const std::string& path):
    TestPathGuard(path)
{
    File::try_remove(m_path+".lock");
}


} // namespace test_util
} // namespace tightdb
