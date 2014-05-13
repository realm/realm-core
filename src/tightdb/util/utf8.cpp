#include <cstring>
#include <algorithm>
#include <vector>
#include <iostream>

#ifdef _WIN32
    #define NOMINMAX
    #include <windows.h>
#else
    #include <ctype.h>
#endif

#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/utf8.hpp>

#if TIGHTDB_HAVE_CXX11
#include <clocale>

#ifdef _MSC_VER
    #include <codecvt>
#else
    #include <locale>
#endif

#endif

using namespace std;

namespace tightdb {

namespace util {



} // namespace util
} // namespace tightdb
