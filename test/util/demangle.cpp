#include <cstdlib>

#include <realm/util/features.h>
#include <memory>

#if REALM_HAVE_AT_LEAST_GCC(3,2)
#  define REALM_HAVE_CXXABI_DEMANGLE
#  include <cxxabi.h>
#endif

#include "demangle.hpp"

using namespace realm;

namespace {

struct Free {
    void operator()(char* p) const
    {
        free(p);
    }
};

} // anonymous namespace

namespace realm {
namespace test_util {


// See http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/namespaceabi.html
//
// FIXME: Could use the Autoconf macro 'ax_cxx_gcc_abi_demangle'. See
// http://autoconf-archive.cryp.to.
std::string demangle(const std::string& mangled_name)
{
#ifdef REALM_HAVE_CXXABI_DEMANGLE
    int status = 0;
    std::unique_ptr<char[], Free> buffer(abi::__cxa_demangle(mangled_name.c_str(), 0, 0, &status));
    if (!buffer)
        return mangled_name;
    std::string demangled_name = buffer.get();
    return demangled_name;
#else
    return mangled_name;
#endif
}


} // namespace test_util
} // namespace realm
