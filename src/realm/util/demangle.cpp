#include <cstdlib>
#include <memory>

#include <realm/util/features.h>
#include <realm/util/assert.hpp>
#include <realm/util/demangle.hpp>
#include <realm/util/backtrace.hpp>

#if REALM_HAVE_AT_LEAST_GCC(3, 2)
#define REALM_HAVE_CXXABI_DEMANGLE
#include <cxxabi.h>
#endif


// See http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/namespaceabi.html
//
// FIXME: Could use the Autoconf macro 'ax_cxx_gcc_abi_demangle'. See
// http://autoconf-archive.cryp.to.
std::string realm::util::demangle(const std::string& mangled_name)
{
#ifdef REALM_HAVE_CXXABI_DEMANGLE
    int status = 0;
    char* unmangled_name = abi::__cxa_demangle(mangled_name.c_str(), nullptr, nullptr, &status);
    switch (status) {
        case 0:
            REALM_ASSERT(unmangled_name);
            goto demangled;
        case -1:
            REALM_ASSERT(!unmangled_name);
            throw util::bad_alloc{};
    }
    REALM_ASSERT(!unmangled_name);
    return mangled_name; // Throws
demangled:
    class Free {
    public:
        void operator()(char* p) const
        {
            std::free(p);
        }
    };
    std::unique_ptr<char[], Free> owner{unmangled_name};
    std::string demangled_name_2{unmangled_name}; // Throws
    return demangled_name_2;
#else
    return mangled_name; // Throws
#endif
}
