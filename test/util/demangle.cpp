/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <cstdlib>

#include <realm/util/features.h>
#include <memory>

#if REALM_HAVE_AT_LEAST_GCC(3, 2)
#define REALM_HAVE_CXXABI_DEMANGLE
#include <cxxabi.h>
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
    std::unique_ptr<char[], Free> buffer(abi::__cxa_demangle(mangled_name.c_str(), nullptr, nullptr, &status));
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
