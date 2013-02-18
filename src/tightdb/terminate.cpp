#include <iostream>

#include <tightdb/terminate.hpp>

using namespace std;

namespace tightdb {


TIGHTDB_NORETURN void terminate(string message, const char* file, long line) TIGHTDB_NOEXCEPT
{
    cerr << file << ":" << line << ": " << message << endl;
    abort();
}


} // namespace tightdb
