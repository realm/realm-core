#include <iostream>

#include <tightdb/util/terminate.hpp>

using namespace std;

namespace tightdb {
namespace util {


TIGHTDB_NORETURN void terminate(string message, const char* file, long line) TIGHTDB_NOEXCEPT
{
    cerr << file << ":" << line << ": " << message << endl;
    abort();
}


} // namespace util
} // namespace tightdb
