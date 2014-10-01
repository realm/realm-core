#include <iostream>

#ifdef __APPLE__
#include <execinfo.h>
#endif

#include <tightdb/util/terminate.hpp>

using namespace std;

namespace tightdb {
namespace util {


TIGHTDB_NORETURN void terminate(string message, const char* file, long line) TIGHTDB_NOEXCEPT
{
    cerr << file << ":" << line << ": " << message << endl;

#ifdef __APPLE__
    void* callstack[128];
    int frames = backtrace(callstack, 128);
    char** strs = backtrace_symbols(callstack, frames);
    for (int i = 0; i < frames; ++i) {
        cerr << strs[i] << endl;
    }
    free(strs);
#endif

    abort();
}


} // namespace util
} // namespace tightdb
