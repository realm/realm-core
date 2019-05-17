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

#include <cerrno>
#include <cstdlib>
#include <stdexcept>
#include <algorithm>

#include <realm/array.hpp>
#include <realm/alloc_slab.hpp>
#include <realm/group.hpp>

using namespace realm;


// FIXME: Casting a pointers to size_t is inherently
// nonportable. For example, systems exist where pointers are 64 bits
// and size_t is 32. One idea would be to use a different type
// for refs such as uintptr_t, the problem with this one is that
// while it is described by the C++11 standard it is not required to
// be present. C++03 does not even mention it. A real working solution
// will be to introduce a new name for the type of refs. The typedef
// can then be made as complex as required to pick out an appropriate
// type on any supported platform.
//
// A better solution may be to use an instance of SlabAlloc. The main
// problem is that SlabAlloc is not thread-safe. Another problem is
// that its free-list management is currently exceedingly slow do to
// linear searches. Another problem is that it is prone to general
// memory corruption due to lack of exception safety when upding
// free-lists. But these problems must be fixed anyway.


namespace {

/// For use with free-standing objects (objects that are not part of a
/// Realm group)
///
/// Note that it is essential that this class is stateless as it may
/// be used by multiple threads. Although it has m_replication, this
/// is not a problem, as there is no way to modify it, so it will
/// remain zero.
class DefaultAllocator : public realm::Allocator {
public:
    DefaultAllocator()
    {
        m_baseline = 0; // Zero is not available .. WHY? FIXME!
    }

    MemRef do_alloc(const size_t size) override
    {
        char* addr = static_cast<char*>(::malloc(size));
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!addr))) {
            REALM_ASSERT_DEBUG(errno == ENOMEM);
            throw util::bad_alloc();
        }
#if REALM_ENABLE_ALLOC_SET_ZERO
        std::fill(addr, addr + size, 0);
#endif
        return MemRef(addr, reinterpret_cast<size_t>(addr), *this);
    }

    MemRef do_realloc(ref_type, char* addr, size_t old_size, size_t new_size) override
    {
        char* new_addr = static_cast<char*>(::realloc(const_cast<char*>(addr), new_size));
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!new_addr))) {
            REALM_ASSERT_DEBUG(errno == ENOMEM);
            throw util::bad_alloc();
        }
#if REALM_ENABLE_ALLOC_SET_ZERO
        std::fill(new_addr + old_size, new_addr + new_size, 0);
#else
        static_cast<void>(old_size);
#endif
        return MemRef(new_addr, reinterpret_cast<size_t>(new_addr), *this);
    }

    void do_free(ref_type, char* addr) override
    {
        ::free(addr);
    }

    char* do_translate(ref_type ref) const noexcept override
    {
        return reinterpret_cast<char*>(ref);
    }

    void verify() const override
    {
    }
};

// This variable is declared such that get_default() can return it. It could be a static local variable, but
// Valgrind/Helgrind gives a false error report because it doesn't recognize gcc's static variable initialization
// mutex
DefaultAllocator default_alloc;

} // anonymous namespace

namespace realm {

Allocator& Allocator::get_default() noexcept
{
    return default_alloc;
}
}
