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
        m_baseline = 0;
    }

    MemRef do_alloc(const size_t size) override
    {
        char* addr = static_cast<char*>(::malloc(size));
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!addr))) {
            // LCOV_EXCL_START
            REALM_ASSERT_DEBUG(errno == ENOMEM);
            throw util::bad_alloc();
            // LCOV_EXCL_STOP
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
            // LCOV_EXCL_START
            REALM_ASSERT_DEBUG(errno == ENOMEM);
            throw util::bad_alloc();
            // LCOV_EXCL_STOP
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

    void verify() const override {}
    void get_or_add_xover_mapping(RefTranslation&, size_t, size_t, size_t) override
    {
        REALM_ASSERT(false);
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

// This function is called to handle translation of a ref which is above the limit for its
// memory mapping. This requires one of three:
// * bumping the limit of the mapping. (if the entire array is inside the mapping)
// * adding a cross-over mapping. (if the array crosses a mapping boundary)
// * using an already established cross-over mapping. (ditto)
// this can proceed concurrently with other calls to translate()
char* Allocator::translate_less_critical(RefTranslation* ref_translation_ptr, ref_type ref) const noexcept
{
    size_t idx = get_section_index(ref);
    RefTranslation& txl = ref_translation_ptr[idx];
    size_t offset = ref - get_section_base(idx);
    char* addr = txl.mapping_addr + offset;
#if REALM_ENABLE_ENCRYPTION
    realm::util::encryption_read_barrier(addr, NodeHeader::header_size, txl.encrypted_mapping, nullptr);
#endif
    auto size = NodeHeader::get_byte_size_from_header(addr);
    bool crosses_mapping = offset + size > (1 << section_shift);
    // Move the limit on use of the existing primary mapping.
    // Take into account that another thread may attempt to change / have changed it concurrently,
    size_t lowest_possible_xover_offset = txl.lowest_possible_xover_offset.load(std::memory_order_relaxed);
    auto new_lowest_possible_xover_offset = offset + (crosses_mapping ? 0 : size);
    while (new_lowest_possible_xover_offset > lowest_possible_xover_offset) {
        if (txl.lowest_possible_xover_offset.compare_exchange_weak(
                lowest_possible_xover_offset, new_lowest_possible_xover_offset, std::memory_order_relaxed))
            break;
    }
    if (REALM_LIKELY(!crosses_mapping)) {
        // Array fits inside primary mapping, no new mapping needed.
#if REALM_ENABLE_ENCRYPTION
        realm::util::encryption_read_barrier(addr, size, txl.encrypted_mapping, nullptr);
#endif
        return addr;
    }
    else {
        // we need a cross-over mapping. If one is already established, use that.
        auto xover_mapping_addr = txl.xover_mapping_addr.load(std::memory_order_acquire);
        if (!xover_mapping_addr) {
            // we need to establish a xover mapping - or wait for another thread to finish
            // establishing one:
            const_cast<Allocator*>(this)->get_or_add_xover_mapping(txl, idx, offset, size);
            // reload (can be relaxed since the call above synchronizes on a mutex)
            xover_mapping_addr = txl.xover_mapping_addr.load(std::memory_order_relaxed);
        }
        // array is now known to be inside the established xover mapping:
        addr = xover_mapping_addr + (offset - txl.xover_mapping_base);
#if REALM_ENABLE_ENCRYPTION
        realm::util::encryption_read_barrier(addr, size, txl.xover_encrypted_mapping, nullptr);
#endif
        return addr;
    }
}
} // namespace realm
