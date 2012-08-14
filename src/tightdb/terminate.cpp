#include <iostream>

#include <tightdb/terminate.hpp>

using namespace std;

namespace tightdb {


void terminate(string message, const char* file, long line)
{
    cerr << file << ":" << line << ": " << message << endl;
    abort();
}


} // namespace tightdb
