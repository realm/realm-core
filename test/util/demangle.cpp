#include <cstdlib>

#include <tightdb/util/features.h>
#include <tightdb/util/unique_ptr.hpp>

#if TIGHTDB_HAVE_AT_LEAST_GCC(3,2)
#  define TIGHTDB_HAVE_CXXABI_DEMANGLE
#  include <cxxabi.h>
#endif

#include "demangle.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

namespace {

struct Free {
    void operator()(char* p) const
    {
        free(p);
    }
};

} // anonymous namespace

namespace tightdb {
namespace test_util {


// See http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/namespaceabi.html
//
// FIXME: Could use the Autoconf macro 'ax_cxx_gcc_abi_demangle'. See
// http://autoconf-archive.cryp.to.
string demangle(const string& mangled_name)
{
#ifdef TIGHTDB_HAVE_CXXABI_DEMANGLE
    int status = 0;
    std::unique_ptr<char[], Free> buffer(abi::__cxa_demangle(mangled_name.c_str(), 0, 0, &status));
    if (!buffer)
        return mangled_name;
    string demangled_name = buffer.get();
    return demangled_name;
#else
    return mangled_name;
#endif
}


} // namespace test_util
} // namespace tightdb
