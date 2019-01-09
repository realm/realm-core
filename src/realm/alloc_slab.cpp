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

#include <cinttypes>
#include <type_traits>
#include <exception>
#include <algorithm>
#include <memory>
#include <mutex>
#include <map>

#ifdef REALM_DEBUG
#include <iostream>
#endif

#ifdef REALM_SLAB_ALLOC_DEBUG
#include <cstdlib>
#endif

#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/miscellaneous.hpp>
#include <realm/util/terminate.hpp>
#include <realm/util/thread.hpp>
#include <realm/array.hpp>
#include <realm/alloc_slab.hpp>

using namespace realm;
using namespace realm::util;


namespace {

#ifdef REALM_SLAB_ALLOC_DEBUG
std::map<ref_type, void*> malloc_debug_map;
#endif

class InvalidFreeSpace : std::exception {
public:
    const char* what() const noexcept override
    {
        return "Free space tracking was lost due to out-of-memory";
    }
};

} // anonymous namespace


struct SlabAlloc::MappedFile {

    util::Mutex m_mutex;
    util::File m_file;
#if REALM_ENABLE_ENCRYPTION
    util::SharedFileInfo* m_realm_file_info = nullptr;
#endif
    util::File::Map<char> m_initial_mapping;
    // additional sections beyond those covered by the initial mapping, are
    // managed as separate mmap allocations, each covering one section.
    size_t m_first_additional_mapping = 0;
    size_t m_num_global_mappings = 0;
    size_t m_capacity_global_mappings = 0;
    std::unique_ptr<std::shared_ptr<const util::File::Map<char>>[]> m_global_mappings;

    /// Indicates if attaching to the file was succesfull
    bool m_success = false;

    MappedFile() {}
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    ~MappedFile()
    {
        m_file.close();
    }
};


SlabAlloc::SlabAlloc()
{
    m_initial_section_size = page_size();
    m_section_shifts = log2(m_initial_section_size);
    size_t max = std::numeric_limits<size_t>::max();
    m_num_section_bases = 1 + get_section_index(max);
    // Allocate one more element than necessary, this is so that get_upper_section_boundary() still functions
    // as expected on addresses in the last working base.
    m_section_bases.reset(new size_t[m_num_section_bases + 1]);
    for (size_t i = 0; i < m_num_section_bases; ++i) {
        m_section_bases[i] = compute_section_base(i);
    }
    m_section_bases[m_num_section_bases] = max;
}

util::File& SlabAlloc::get_file()
{
    return m_file_mappings->m_file;
}


const SlabAlloc::Header SlabAlloc::empty_file_header = {
    {0, 0}, // top-refs
    {'T', '-', 'D', 'B'},
    {0, 0}, // undecided file format
    0,      // reserved
    0       // flags (lsb is select bit)
};


void SlabAlloc::init_streaming_header(Header* streaming_header, int file_format_version)
{
    using storage_type = std::remove_reference<decltype(Header::m_file_format[0])>::type;
    REALM_ASSERT(!util::int_cast_has_overflow<storage_type>(file_format_version));
    *streaming_header = {
        {0xFFFFFFFFFFFFFFFFULL, 0}, // top-refs
        {'T', '-', 'D', 'B'},
        {storage_type(file_format_version), 0},
        0, // reserved
        0  // flags (lsb is select bit)
    };
}


class SlabAlloc::ChunkRefEq {
public:
    ChunkRefEq(ref_type ref) noexcept
        : m_ref(ref)
    {
    }
    bool operator()(const Chunk& chunk) const noexcept
    {
        return chunk.ref == m_ref;
    }

private:
    ref_type m_ref;
};


class SlabAlloc::ChunkRefEndEq {
public:
    ChunkRefEndEq(ref_type ref) noexcept
        : m_ref(ref)
    {
    }
    bool operator()(const Chunk& chunk) const noexcept
    {
        return chunk.ref + chunk.size == m_ref;
    }

private:
    ref_type m_ref;
};


class SlabAlloc::SlabRefEndEq {
public:
    SlabRefEndEq(ref_type ref) noexcept
        : m_ref(ref)
    {
    }
    bool operator()(const Slab& slab) const noexcept
    {
        return slab.ref_end == m_ref;
    }

private:
    ref_type m_ref;
};


void SlabAlloc::detach() noexcept
{
    switch (m_attach_mode) {
        case attach_None:
        case attach_UsersBuffer:
            break;
        case attach_OwnedBuffer:
            ::free(const_cast<char*>(m_data));
            break;
        case attach_SharedFile:
        case attach_UnsharedFile:
            m_data = 0;
            m_file_mappings.reset();
            m_local_mappings.reset();
            m_num_local_mappings = 0;
            break;
        default:
            REALM_UNREACHABLE();
    }
    internal_invalidate_cache();

    // Release all allocated memory - this forces us to create new
    // slabs after re-attaching thereby ensuring that the slabs are
    // placed correctly (logically) after the end of the file.
    for (auto& slab : m_slabs) {
        delete[] slab.addr;
    }
    m_slabs.clear();
    clear_freelists();

    m_attach_mode = attach_None;
}


SlabAlloc::~SlabAlloc() noexcept
{
#ifdef REALM_DEBUG
    if (is_attached()) {
        // A shared group does not guarantee that all space is free
        if (m_attach_mode != attach_SharedFile) {
            // No point inchecking if free space info is invalid
            if (m_free_space_state != free_space_Invalid) {
                if (REALM_COVER_NEVER(!is_all_free())) {
                    print();
#ifndef REALM_SLAB_ALLOC_DEBUG
                    std::cerr << "To get the stack-traces of the corresponding allocations,"
                                 "first compile with REALM_SLAB_ALLOC_DEBUG defined,"
                                 "then run under Valgrind with --leak-check=full\n";
                    REALM_TERMINATE("SlabAlloc detected a leak");
#endif
                }
            }
        }
    }
#endif

    if (is_attached())
        detach();
}


MemRef SlabAlloc::do_alloc(size_t size)
{
#ifdef REALM_SLAB_ALLOC_TUNE
    static int64_t memstat_requested = 0;
    static int64_t memstat_slab_size = 0;
    static int64_t memstat_slabs = 0;
    static int64_t memstat_rss = 0;
    static int64_t memstat_rss_ctr = 0;

    {
        double vm;
        double res;
        process_mem_usage(vm, res);
        memstat_rss += res;
        memstat_rss_ctr += 1;
        memstat_requested += size;
    }
#endif

    REALM_ASSERT(0 < size);
    REALM_ASSERT((size & 0x7) == 0); // only allow sizes that are multiples of 8
    REALM_ASSERT(is_attached());
    REALM_ASSERT(size < 1 * 1024 * 1024 * 1024);

    // If we failed to correctly record free space, new allocations cannot be
    // carried out until the free space record is reset.
    if (REALM_COVER_NEVER(m_free_space_state == free_space_Invalid))
        throw InvalidFreeSpace();

    m_free_space_state = free_space_Dirty;

    // minimal allocation is sizeof(FreeListEntry)
    if (size < sizeof(FreeBlock))
        size = sizeof(FreeBlock);
    // align to multipla of 8
    if (size & 0x7)
        size = (size + 7) & ~0x7;

    FreeBlock* entry = allocate_block(static_cast<int>(size));
    mark_allocated(entry);
    ref_type ref = entry->ref;

#ifdef REALM_DEBUG
    if (REALM_COVER_NEVER(m_debug_out))
        std::cerr << "Alloc ref: " << ref << " size: " << size << "\n";
#endif

    char* addr = reinterpret_cast<char*>(entry);
    REALM_ASSERT(addr == translate(ref));

#if REALM_ENABLE_ALLOC_SET_ZERO
    std::fill(addr, addr + size, 0);
#endif
#ifdef REALM_SLAB_ALLOC_DEBUG
    malloc_debug_map[ref] = malloc(1);
#endif
    REALM_ASSERT_EX(ref >= m_baseline, ref, m_baseline);
    return MemRef(addr, ref, *this);
}

SlabAlloc::FreeBlock* SlabAlloc::get_prev_block_if_mergeable(SlabAlloc::FreeBlock* entry)
{
    auto bb = bb_before(entry);
    if (bb->block_before_size <= 0)
        return nullptr; // no prev block, or it is in use
    return block_before(bb);
}

SlabAlloc::FreeBlock* SlabAlloc::get_next_block_if_mergeable(SlabAlloc::FreeBlock* entry)
{
    auto bb = bb_after(entry);
    if (bb->block_after_size <= 0)
        return nullptr; // no next block, or it is in use
    return block_after(bb);
}

SlabAlloc::FreeList SlabAlloc::find(int size)
{
    FreeList retval;
    retval.it = m_block_map.lower_bound(size);
    if (retval.it != m_block_map.end()) {
        retval.size = retval.it->first;
    }
    else {
        retval.size = 0;
    }
    return retval;
}

SlabAlloc::FreeList SlabAlloc::find_larger(FreeList hint, int size)
{
    int needed_size = size + sizeof(BetweenBlocks) + sizeof(FreeBlock);
    while (hint.it != m_block_map.end() && hint.it->first < needed_size)
        ++hint.it;
    if (hint.it == m_block_map.end())
        hint.size = 0; // indicate "not found"
    return hint;
}

SlabAlloc::FreeBlock* SlabAlloc::pop_freelist_entry(FreeList list)
{
    FreeBlock* retval = list.it->second;
    FreeBlock* header = retval->next;
    if (header == retval)
        m_block_map.erase(list.it);
    else
        list.it->second = header;
    retval->unlink();
    return retval;
}

void SlabAlloc::FreeBlock::unlink()
{
    auto _next = next;
    auto _prev = prev;
    _next->prev = prev;
    _prev->next = next;
    clear_links();
}

void SlabAlloc::remove_freelist_entry(FreeBlock* entry)
{
    int size = bb_before(entry)->block_after_size;
    auto it = m_block_map.find(size);
    REALM_ASSERT(it != m_block_map.end());
    auto header = it->second;
    if (header == entry) {
        header = entry->next;
        if (header == entry)
            m_block_map.erase(it);
        else
            it->second = header;
    }
    entry->unlink();
}

void SlabAlloc::push_freelist_entry(FreeBlock* entry)
{
    int size = bb_before(entry)->block_after_size;
    FreeBlock* header;
    auto it = m_block_map.find(size);
    if (it != m_block_map.end()) {
        header = it->second;
        it->second = entry;
        entry->next = header;
        entry->prev = header->prev;
        entry->prev->next = entry;
        entry->next->prev = entry;
    }
    else {
        header = nullptr;
        m_block_map[size] = entry;
        entry->next = entry->prev = entry;
    }
}

void SlabAlloc::mark_freed(FreeBlock* entry, int size)
{
    auto bb = bb_before(entry);
    REALM_ASSERT(bb->block_after_size < 0);
    REALM_ASSERT(bb->block_after_size == -size);
    bb->block_after_size = 0 - bb->block_after_size;
    bb = bb_after(entry);
    REALM_ASSERT(bb->block_before_size < 0);
    REALM_ASSERT(bb->block_before_size == -size);
    bb->block_before_size = 0 - bb->block_before_size;
}

void SlabAlloc::mark_allocated(FreeBlock* entry)
{
    auto bb = bb_before(entry);
    REALM_ASSERT(bb->block_after_size > 0);
    auto bb2 = bb_after(entry);
    bb->block_after_size = 0 - bb->block_after_size;
    REALM_ASSERT(bb2->block_before_size > 0);
    bb2->block_before_size = 0 - bb2->block_before_size;
}

SlabAlloc::FreeBlock* SlabAlloc::allocate_block(int size)
{
    FreeList list = find(size);
    if (list.found_exact(size)) {
        return pop_freelist_entry(list);
    }
    // no exact matches.
    list = find_larger(list, size);
    FreeBlock* block;
    if (list.found_something()) {
        block = pop_freelist_entry(list);
    }
    else {
        block = grow_slab_for(size);
    }
    FreeBlock* remaining = break_block(block, size);
    if (remaining)
        push_freelist_entry(remaining);
    REALM_ASSERT(size_from_block(block) == size);
    return block;
}

SlabAlloc::FreeBlock* SlabAlloc::slab_to_entry(Slab slab, ref_type ref_start)
{
    auto bb = reinterpret_cast<BetweenBlocks*>(slab.addr);
    bb->block_before_size = 0;
    int block_size = static_cast<int>(slab.ref_end - ref_start - 2 * sizeof(BetweenBlocks));
    bb->block_after_size = block_size;
    auto entry = block_after(bb);
    entry->clear_links();
    entry->ref = ref_start + sizeof(BetweenBlocks);
    bb = bb_after(entry);
    bb->block_before_size = block_size;
    bb->block_after_size = 0;
    return entry;
}

void SlabAlloc::clear_freelists()
{
    m_block_map.clear();
}

void SlabAlloc::rebuild_freelists_from_slab()
{
    clear_freelists();
    ref_type ref_start = m_baseline;
    for (auto e : m_slabs) {
        FreeBlock* entry = slab_to_entry(e, ref_start);
        push_freelist_entry(entry);
        ref_start = e.ref_end;
    }
}

SlabAlloc::FreeBlock* SlabAlloc::break_block(FreeBlock* block, int new_size)
{
    int size = size_from_block(block);
    int remaining_size = size - (new_size + sizeof(BetweenBlocks));
    if (remaining_size < static_cast<int>(sizeof(FreeBlock)))
        return nullptr;
    bb_after(block)->block_before_size = remaining_size;
    bb_before(block)->block_after_size = new_size;
    auto bb_between = bb_after(block);
    bb_between->block_before_size = new_size;
    bb_between->block_after_size = remaining_size;
    FreeBlock* remaining_block = block_after(bb_between);
    remaining_block->ref = block->ref + new_size + sizeof(BetweenBlocks);
    remaining_block->clear_links();
    block->clear_links();
    return remaining_block;
}

SlabAlloc::FreeBlock* SlabAlloc::merge_blocks(FreeBlock* first, FreeBlock* last)
{
    int size_first = size_from_block(first);
    int size_last = size_from_block(last);
    int new_size = size_first + size_last + sizeof(BetweenBlocks);
    bb_before(first)->block_after_size = new_size;
    bb_after(last)->block_before_size = new_size;
    return first;
}

SlabAlloc::FreeBlock* SlabAlloc::grow_slab_for(int size)
{
    // Make sure that either a) the allocation matches exactly, or
    // b) there is sufficient room for additional allocation
    size_t new_size = size + 2 * sizeof(BetweenBlocks); // one at each end.
    size_t exact_size = new_size;
    size_t minimal_larger_size = new_size + sizeof(BetweenBlocks) + sizeof(FreeBlock);
    ref_type ref;
    if (m_slabs.empty()) {
        ref = m_baseline;
    }
    else {
        // Find size of memory that has been modified (through copy-on-write) in current write transaction
        ref_type curr_ref_end = to_size_t(m_slabs.back().ref_end);
        REALM_ASSERT_DEBUG_EX(curr_ref_end >= m_baseline, curr_ref_end, m_baseline);
        size_t copy_on_write = curr_ref_end - m_baseline;

        // Allocate 20% of that (for the first few number of slabs the math below will just result in 1 page each)
        size_t min_size = static_cast<size_t>(0.2 * copy_on_write);

        if (new_size < min_size)
            new_size = min_size;

        // cap allocation at 1GB
        if (new_size > 1 * 1024 * 1024 * 1024)
            new_size = 1 * 1024 * 1024 * 1024;
        ref = curr_ref_end;
    }

    // Round upwards to nearest page size
    new_size = ((new_size - 1) | (page_size() - 1)) + 1;
    if (new_size != exact_size && new_size < minimal_larger_size) {
        new_size = minimal_larger_size;
        // round to next page size, then
        new_size = ((new_size - 1) | (page_size() - 1)) + 1;
    }

#ifdef REALM_SLAB_ALLOC_TUNE
    {
        const size_t update = 5000000;
        if ((memstat_slab_size + new_size) / update > memstat_slab_size / update) {
            std::cerr << "Size of all allocated slabs:    " << (memstat_slab_size + new_size) / 1024 << " KB\n"
                      << "Sum of size for do_alloc(size): " << memstat_requested / 1024 << " KB\n"
                      << "Average physical memory usage:  " << memstat_rss / memstat_rss_ctr / 1024 << " KB\n"
                      << "Page size:                      " << page_size() / 1024 << " KB\n"
                      << "Number of all allocated slabs:  " << memstat_slabs << "\n\n";
        }
        memstat_slab_size += new_size;
        memstat_slabs += 1;
    }
#endif

    REALM_ASSERT(0 < new_size);
    size_t ref_end = ref;
    if (REALM_UNLIKELY(int_add_with_overflow_detect(ref_end, new_size))) {
        throw MaximumFileSizeExceeded("AllocSlab slab ref_end size overflow: " + util::to_string(ref) + " + " +
                                      util::to_string(new_size));
    }

    std::unique_ptr<char[]> mem(new char[new_size]); // Throws
    std::fill(mem.get(), mem.get() + new_size, 0);

    // Add to list of slabs
    Slab slab;
    slab.addr = mem.get();
    slab.ref_end = ref_end;
    m_slabs.push_back(slab); // Throws
    mem.release();

    // build a single block from that entry
    return slab_to_entry(slab, ref);
}


void SlabAlloc::do_free(ref_type ref, char* addr) noexcept
{
    REALM_ASSERT_3(translate(ref), ==, addr);

    bool read_only = is_read_only(ref);
#ifdef REALM_SLAB_ALLOC_DEBUG
    free(malloc_debug_map[ref]);
#endif

    // Get size from segment
    size_t size = read_only ? Array::get_byte_size_from_header(addr) : Array::get_capacity_from_header(addr);

#ifdef REALM_DEBUG
    if (REALM_COVER_NEVER(m_debug_out))
        std::cerr << "Free ref: " << ref << " size: " << size << "\n";
#endif

    if (REALM_COVER_NEVER(m_free_space_state == free_space_Invalid))
        return;

    // Mutable memory cannot be freed unless it has first been allocated, and
    // any allocation puts free space tracking into the "dirty" state.
    REALM_ASSERT_3(read_only, ||, m_free_space_state == free_space_Dirty);

    m_free_space_state = free_space_Dirty;

    if (read_only) {
        // Free space in read only segment is tracked separately
        try {
            REALM_ASSERT_RELEASE(ref != 0);
            REALM_ASSERT_RELEASE_EX(!(ref & 7), ref);
            auto next = m_free_read_only.lower_bound(ref);
            if (next != m_free_read_only.end()) {
                REALM_ASSERT_RELEASE_EX(ref + size <= next->first, ref, size, next->first, next->second);
                // See if element can be combined with next element
                if (ref + size == next->first) {
                    size += next->second;
                    next = m_free_read_only.erase(next);
                }
            }
            if (!m_free_read_only.empty() && next != m_free_read_only.begin()) {
                // There must be a previous element - see if we can merge
                auto prev = next;
                prev--;

                REALM_ASSERT_RELEASE_EX(prev->first + prev->second <= ref, ref, size, prev->first, prev->second);
                // See if element can be combined with previous element
                // We can do that just by adding the size
                if (prev->first + prev->second == ref) {
                    prev->second += size;
                    return; // Done!
                }
                m_free_read_only.emplace_hint(prev, ref, size); // Throws
            }
            else {
                m_free_read_only.emplace(ref, size); // Throws
            }
        }
        catch (...) {
            m_free_space_state = free_space_Invalid;
        }
    }
    else {
        // fixup size to take into account the allocator's need to store a FreeBlock in a freed block
        if (size < sizeof(FreeBlock))
            size = sizeof(FreeBlock);
        // align to multipla of 8
        if (size & 0x7)
            size = (size + 7) & ~0x7;

        FreeBlock* e = reinterpret_cast<FreeBlock*>(addr);
        REALM_ASSERT_RELEASE(size < 2UL * 1024 * 1024 * 1024);
        mark_freed(e, static_cast<int>(size));
        free_block(ref, e);
    }
}

void SlabAlloc::free_block(ref_type ref, SlabAlloc::FreeBlock* block)
{
    // merge with surrounding blocks if possible
    block->ref = ref;
    FreeBlock* prev = get_prev_block_if_mergeable(block);
    if (prev) {
        remove_freelist_entry(prev);
        block = merge_blocks(prev, block);
    }
    FreeBlock* next = get_next_block_if_mergeable(block);
    if (next) {
        remove_freelist_entry(next);
        block = merge_blocks(block, next);
    }
    push_freelist_entry(block);
}

size_t SlabAlloc::consolidate_free_read_only()
{
    if (REALM_COVER_NEVER(m_free_space_state == free_space_Invalid))
        throw InvalidFreeSpace();
    return m_free_read_only.size();
}


MemRef SlabAlloc::do_realloc(size_t ref, char* addr, size_t old_size, size_t new_size)
{
    REALM_ASSERT_DEBUG(translate(ref) == addr);
    REALM_ASSERT(0 < new_size);
    REALM_ASSERT((new_size & 0x7) == 0); // only allow sizes that are multiples of 8

    // FIXME: Check if we can extend current space. In that case, remember to
    // check whether m_free_space_state == free_state_Invalid. Also remember to
    // fill with zero if REALM_ENABLE_ALLOC_SET_ZERO is non-zero.

    // Allocate new space
    MemRef new_mem = do_alloc(new_size); // Throws

    // Copy existing segment
    char* new_addr = new_mem.get_addr();
    realm::safe_copy_n(addr, old_size, new_addr);

    // Add old segment to freelist
    do_free(ref, addr);

#ifdef REALM_DEBUG
    if (REALM_COVER_NEVER(m_debug_out)) {
        std::cerr << "Realloc orig_ref: " << ref << " old_size: " << old_size << " new_ref: " << new_mem.get_ref()
                  << " new_size: " << new_size << "\n";
    }
#endif // REALM_DEBUG

    return new_mem;
}


char* SlabAlloc::do_translate(ref_type ref) const noexcept
{
    REALM_ASSERT_DEBUG(is_attached());

    const char* addr = nullptr;

    size_t cache_index = ref ^ ((ref >> 16) >> 16);
    // we shift by 16 two times. On 32-bitters it's undefined to shift by
    // 32. Shifting twice x16 however, is defined and gives zero. On 64-bitters
    // the compiler should reduce it to a single 32 bit shift.
    cache_index = cache_index ^ (cache_index >> 16);
    cache_index = (cache_index ^ (cache_index >> 8)) & 0xFF;
    if (cache[cache_index].ref == ref && cache[cache_index].version == version)
        return const_cast<char*>(cache[cache_index].addr);

    if (ref < m_baseline) {

        const util::File::Map<char>* map;

        // fast path if reference is inside the initial mapping (or buffer):
        if (ref < m_initial_chunk_size) {
            addr = m_data + ref;
            if (m_file_mappings) {
                // Once established, the initial mapping is immutable, so we
                // don't need to grab a lock for access.
                map = &m_file_mappings->m_initial_mapping;
                realm::util::encryption_read_barrier(addr, Array::header_size, map->get_encrypted_mapping(),
                                                     Array::get_byte_size_from_header);
            }
        }
        else {
            // reference must be inside a section mapped later
            size_t section_index = get_section_index(ref);
            REALM_ASSERT_DEBUG(m_file_mappings);

            size_t mapping_index = section_index - m_file_mappings->m_first_additional_mapping;
            size_t section_offset = ref - get_section_base(section_index);
            REALM_ASSERT_DEBUG(m_local_mappings);
            REALM_ASSERT_DEBUG(mapping_index < m_num_local_mappings);
            map = m_local_mappings[mapping_index].get();
            REALM_ASSERT_DEBUG(map->get_addr() != nullptr);
            addr = map->get_addr() + section_offset;
            realm::util::encryption_read_barrier(addr, Array::header_size, map->get_encrypted_mapping(),
                                                 Array::get_byte_size_from_header);
        }
    }
    else {
        typedef slabs::const_iterator iter;
        iter i = upper_bound(m_slabs.begin(), m_slabs.end(), ref, &ref_less_than_slab_ref_end);
        REALM_ASSERT_DEBUG(i != m_slabs.end());

        ref_type slab_ref = i == m_slabs.begin() ? m_baseline : (i - 1)->ref_end;
        addr = i->addr + (ref - slab_ref);
    }
    cache[cache_index].addr = addr;
    cache[cache_index].ref = ref;
    cache[cache_index].version = version;
    REALM_ASSERT_DEBUG(addr != nullptr);
    return const_cast<char*>(addr);
}


int SlabAlloc::get_committed_file_format_version() const noexcept
{
    const Header& header = *reinterpret_cast<const Header*>(m_data);
    int slot_selector = ((header.m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);
    int file_format_version = int(header.m_file_format[slot_selector]);
    return file_format_version;
}

bool SlabAlloc::is_file_on_streaming_form(const Header& header)
{
    int slot_selector = ((header.m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);
    uint_fast64_t ref = uint_fast64_t(header.m_top_ref[slot_selector]);
    return (slot_selector == 0 && ref == 0xFFFFFFFFFFFFFFFFULL);
}

ref_type SlabAlloc::get_top_ref(const char* buffer, size_t len)
{
    const Header& header = reinterpret_cast<const Header&>(*buffer);
    int slot_selector = ((header.m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);
    if (is_file_on_streaming_form(header)) {
        const StreamingFooter& footer = *(reinterpret_cast<const StreamingFooter*>(buffer + len) - 1);
        return ref_type(footer.m_top_ref);
    }
    else {
        return to_ref(header.m_top_ref[slot_selector]);
    }
}


namespace {

// prevent destruction at exit (which can lead to races if other threads are still running)
std::map<std::string, std::weak_ptr<SlabAlloc::MappedFile>>& all_files =
    *new std::map<std::string, std::weak_ptr<SlabAlloc::MappedFile>>;
util::Mutex& all_files_mutex = *new util::Mutex;
} // namespace


ref_type SlabAlloc::attach_file(const std::string& file_path, Config& cfg)
{
    m_cfg = cfg;
    // ExceptionSafety: If this function throws, it must leave the allocator in
    // the detached state.

    REALM_ASSERT(!is_attached());

    // When 'read_only' is true, this function will throw InvalidDatabase if the
    // file exists already but is empty. This can happen if another process is
    // currently creating it. Note however, that it is only legal for multiple
    // processes to access a database file concurrently if it is done via a
    // SharedGroup, and in that case 'read_only' can never be true.
    REALM_ASSERT(!(cfg.is_shared && cfg.read_only));
    // session_initiator can be set *only* if we're shared.
    REALM_ASSERT(cfg.is_shared || !cfg.session_initiator);
    // clear_file can be set *only* if we're the first session.
    REALM_ASSERT(cfg.session_initiator || !cfg.clear_file);

    // Create a deep copy of the file_path string, otherwise it can appear that
    // users are leaking paths because string assignment operator implementations might
    // actually be reference counting with copy-on-write. If our all_files map
    // holds onto these references (since it is still reachable memory) it can appear
    // as a leak in the user application, but it is actually us (and that's ok).
    const std::string path = file_path.c_str();

    using namespace realm::util;
    File::AccessMode access = cfg.read_only ? File::access_ReadOnly : File::access_ReadWrite;
    File::CreateMode create = cfg.read_only || cfg.no_create ? File::create_Never : File::create_Auto;
    {
        std::lock_guard<Mutex> lock(all_files_mutex);
        std::shared_ptr<SlabAlloc::MappedFile> p = all_files[path].lock();
        // In case we're the session initiator, we'll need a new mapping in any case.
        // NOTE: normally, it should not be possible to find an old mapping while being
        // the session initiator, since by definition the session initiator is the first
        // to attach the file. If, however, the user is deleting the .lock file while he
        // has one or more shared groups attached to the database, a session initiator
        // *will* see a stale mapping. From versions 0.99 to 1.1.0 we asserted when detecting
        // this situation, and this lead to many bug reports. It is likely that many of these
        // would otherwise *not* have lead to observable bugs, because the user would not
        // actually touch the stale database anymore, it was just a case of delayed deallocation
        // of a shared group.
        if (cfg.session_initiator || !bool(p)) {
            p = std::make_shared<MappedFile>();
            all_files[path] = p;
        }
        m_file_mappings = p;
    }
    std::unique_lock<Mutex> lock(m_file_mappings->m_mutex);

    // If the file has already been mapped by another thread, reuse all relevant data
    // from the earlier mapping.
    if (m_file_mappings->m_success) {
        // check that encryption keys match if they're used:
        const char* earlier_used_key = m_file_mappings->m_file.get_encryption_key();
        if (earlier_used_key != nullptr || cfg.encryption_key != nullptr) {
            if (earlier_used_key == nullptr && cfg.encryption_key != nullptr) {
                throw std::runtime_error("Encryption key provided, but file already opened as non-encrypted");
            }
            if (earlier_used_key != nullptr && cfg.encryption_key == nullptr) {
                throw std::runtime_error("Missing encryption key, but file already opened with encryption key");
            }
            if (memcmp(earlier_used_key, cfg.encryption_key, 64)) {
                throw std::runtime_error("Encryption key mismatch");
            }
        }
        m_data = m_file_mappings->m_initial_mapping.get_addr();
        m_initial_chunk_size = m_file_mappings->m_initial_mapping.get_size();
        m_attach_mode = cfg.is_shared ? attach_SharedFile : attach_UnsharedFile;
        m_free_space_state = free_space_Invalid;
        if (m_file_mappings->m_num_global_mappings > 0) {
            size_t mapping_index = m_file_mappings->m_num_global_mappings;
            size_t section_index = mapping_index + m_file_mappings->m_first_additional_mapping;
            m_baseline = get_section_base(section_index);
            m_num_local_mappings = m_file_mappings->m_num_global_mappings;
            m_local_mappings.reset(new std::shared_ptr<const util::File::Map<char>>[m_num_local_mappings]);
            for (size_t k = 0; k < m_num_local_mappings; ++k) {
                m_local_mappings[k] = m_file_mappings->m_global_mappings[k];
            }
        }
        else {
            // TODO: m_file_mappings->m_initial_mapping.get_size() may not represent the actual file size
            m_baseline = m_file_mappings->m_initial_mapping.get_size();
        }
        ref_type top_ref = 0;
        // top_ref is useless unless in shared mode as the allocator is not updated to reflect
        // the maybe updated file. So it cannot be used to translate the ref.
        // cfg.read_only implies !cfg.is_shared, so one check if enough
        REALM_ASSERT_DEBUG(!(cfg.read_only && cfg.is_shared));
        if (cfg.read_only)
            top_ref = get_top_ref(m_data, to_size_t(m_file_mappings->m_file.get_size()));
        return top_ref;
    }
    // Even though we're the first to map the file, we cannot assume that we're
    // the session initiator. Another process may have the session initiator.

    m_file_mappings->m_file.open(path.c_str(), access, create, 0); // Throws
    auto physical_file_size = m_file_mappings->m_file.get_size();
    if (cfg.encryption_key) {
        m_file_mappings->m_file.set_encryption_key(cfg.encryption_key);
    }
    File::CloseGuard fcg(m_file_mappings->m_file);

    size_t size = 0;
    // The size of a database file must not exceed what can be encoded in
    // size_t.
    if (REALM_UNLIKELY(int_cast_with_overflow_detect(m_file_mappings->m_file.get_size(), size)))
        throw InvalidDatabase("Realm file too large", path);
    if (cfg.encryption_key && size == 0 && physical_file_size != 0) {
        // The opened file holds data, but is so small it cannot have
        // been created with encryption
        throw std::runtime_error("Attempt to open unencrypted file with encryption key");
    }

    // FIXME: This initialization procedure does not provide sufficient
    // robustness given that processes may be abruptly terminated at any point
    // in time. In unshared mode, we must be able to reliably detect any invalid
    // file as long as its invalidity is due to a terminated serialization
    // process (e.g. due to a power failure). In shared mode we can guarantee
    // that if the database file was ever valid, then it will remain valid,
    // however, there is no way we can ensure that initialization of an empty
    // database file succeeds. Thus, in shared mode we must be able to reliably
    // distiguish between three cases when opening a database file: A) It was
    // never properly initialized. In this case we should simply reinitialize
    // it. B) It looks corrupt. In this case we throw an exception. C) It looks
    // good. In this case we proceede as normal.
    if (size == 0 || cfg.clear_file) {
        if (REALM_UNLIKELY(cfg.read_only))
            throw InvalidDatabase("Read-only access to empty Realm file", path);

        const char* data = reinterpret_cast<const char*>(&empty_file_header);
        m_file_mappings->m_file.write(data, sizeof empty_file_header); // Throws

        // Pre-alloc initial space
        size_t initial_size = m_initial_section_size;
        m_file_mappings->m_file.prealloc(initial_size); // Throws

        bool disable_sync = get_disable_sync_to_disk() || cfg.disable_sync;
        if (!disable_sync)
            m_file_mappings->m_file.sync(); // Throws

        size = initial_size;
    }
    ref_type top_ref;
    try {
        File::Map<char> map(m_file_mappings->m_file, File::access_ReadOnly, size); // Throws
        // we'll read header and (potentially) footer
        realm::util::encryption_read_barrier(map, 0, sizeof(Header));
        realm::util::encryption_read_barrier(map, size - sizeof(Header), sizeof(Header));

        if (!cfg.skip_validate) {
            // Verify the data structures
            validate_buffer(map.get_addr(), size, path); // Throws
        }

        top_ref = get_top_ref(map.get_addr(), size);

        m_data = map.get_addr();
        m_file_mappings->m_initial_mapping = std::move(map);
        m_baseline = size;
        m_initial_chunk_size = size;
        m_file_mappings->m_first_additional_mapping = get_section_index(m_initial_chunk_size);
        m_attach_mode = cfg.is_shared ? attach_SharedFile : attach_UnsharedFile;
    }
    catch (const DecryptionFailed&) {
        throw InvalidDatabase("Realm file decryption failed", path);
    }
    // make sure that any call to begin_read cause any slab to be placed in free
    // lists correctly
    m_free_space_state = free_space_Invalid;

    // Ensure clean up, if we need to back out:
    DetachGuard dg(*this);
    // ensure that the lock is released before destruction of the mutex, in case
    // an exception is thrown. Since lock2 is constructed after the detach guard,
    // it will be destructed first, releasing the lock before the detach guard
    // releases the MappedFile structure containing the mutex.
    std::unique_lock<Mutex> lock2(move(lock));

    // make sure the database is not on streaming format. If we did not do this,
    // a later commit would have to do it. That would require coordination with
    // anybody concurrently joining the session, so it seems easier to do it at
    // session initialization, even if it means writing the database during open.
    const Header& header = *reinterpret_cast<const Header*>(m_data);
    if (cfg.session_initiator && is_file_on_streaming_form(header)) {
        const StreamingFooter& footer = *(reinterpret_cast<const StreamingFooter*>(m_data + size) - 1);
        // Don't compare file format version fields as they are allowed to differ.
        // Also don't compare reserved fields (todo, is it correct to ignore?)
        static_cast<void>(header);
        REALM_ASSERT_3(header.m_flags, ==, 0);
        REALM_ASSERT_3(header.m_mnemonic[0], ==, uint8_t('T'));
        REALM_ASSERT_3(header.m_mnemonic[1], ==, uint8_t('-'));
        REALM_ASSERT_3(header.m_mnemonic[2], ==, uint8_t('D'));
        REALM_ASSERT_3(header.m_mnemonic[3], ==, uint8_t('B'));
        REALM_ASSERT_3(header.m_top_ref[0], ==, 0xFFFFFFFFFFFFFFFFULL);
        REALM_ASSERT_3(header.m_top_ref[1], ==, 0);

        REALM_ASSERT_3(footer.m_magic_cookie, ==, footer_magic_cookie);
        {
            File::Map<Header> writable_map(m_file_mappings->m_file, File::access_ReadWrite, sizeof(Header)); // Throws
            Header& writable_header = *writable_map.get_addr();
            realm::util::encryption_read_barrier(writable_map, 0);
            writable_header.m_top_ref[1] = footer.m_top_ref;
            writable_header.m_file_format[1] = writable_header.m_file_format[0];
            realm::util::encryption_write_barrier(writable_map, 0);
            writable_map.sync();
            realm::util::encryption_read_barrier(writable_map, 0);
            writable_header.m_flags |= flags_SelectBit;
            realm::util::encryption_write_barrier(writable_map, 0);
            writable_map.sync();

            realm::util::encryption_read_barrier(m_file_mappings->m_initial_mapping, 0, sizeof(Header));
        }
    }

    // We can only safely mmap the file, if its size matches a section. If not,
    // we must change the size to match before mmaping it.
    // This can fail due to a race with a concurrent commmit, in which case we
    // must throw allowing the caller to retry, but the common case is to succeed
    // at first attempt

    if (!matches_section_boundary(size)) {
        // The file size did not match a section boundary.
        // We must extend the file to a section boundary (unless already there)
        // The file must be extended to match in size prior to being mmapped,
        // as extending it after mmap has undefined behavior.

        // The mapping of the first part of the file *must* be contiguous, because
        // we do not know if the file was created by a version of the code, that took
        // the section boundaries into account. If it wasn't we cannot map it in sections
        // without risking datastructures that cross a mapping boundary.

        if (cfg.read_only) {

            // If the file is opened read-only, we cannot extend it. This is not a problem,
            // because for a read-only file we assume that it will not change while we use it.
            // This assumption obviously will not hold, if the file is shared by multiple
            // processes or threads with different opening modes.
            // Currently, there is no way to detect if this assumption is violated.
            ;
        }
        else {

            if (cfg.session_initiator || !cfg.is_shared) {

                // We can only safely extend the file if we're the session initiator, or if
                // the file isn't shared at all.

                // resizing the file (as we do here) without actually changing any internal
                // datastructures to reflect the additional free space will work, because the
                // free space management relies on the logical filesize and disregards the
                // actual size of the file.
                size = get_upper_section_boundary(size);
                m_file_mappings->m_file.prealloc(size);
                m_file_mappings->m_initial_mapping.remap(m_file_mappings->m_file, File::access_ReadOnly, size);
                m_data = m_file_mappings->m_initial_mapping.get_addr();
                m_baseline = size;
                m_initial_chunk_size = size;
                m_file_mappings->m_first_additional_mapping = get_section_index(m_initial_chunk_size);

                realm::util::encryption_read_barrier(m_file_mappings->m_initial_mapping, 0, sizeof(Header));
            }
            else {
                // Getting here, we have a file of a size that will not work, and without being
                // allowed to extend it.
                // This can happen in the case where a concurrent commit is extending the file,
                // and we observe it part-way (file extension is not atomic). If so, we
                // need to start all over. The alternative would be to synchronize with commit,
                // and we generally try to avoid this when possible.
                throw Retry();
            }
        }
    }
    dg.release();  // Do not detach
    fcg.release(); // Do not close
#if REALM_ENABLE_ENCRYPTION
    m_file_mappings->m_realm_file_info = util::get_file_info_for_file(m_file_mappings->m_file);
#endif
    m_file_mappings->m_success = true;
    return top_ref;
}

void SlabAlloc::note_reader_start(void* reader_id)
{
#if REALM_ENABLE_ENCRYPTION
    if (m_file_mappings->m_realm_file_info)
        util::encryption_note_reader_start(*m_file_mappings->m_realm_file_info, reader_id);
#else
    static_cast<void>(reader_id);
#endif
}

void SlabAlloc::note_reader_end(void* reader_id)
{
#if REALM_ENABLE_ENCRYPTION
    if (m_file_mappings->m_realm_file_info)
        util::encryption_note_reader_end(*m_file_mappings->m_realm_file_info, reader_id);
#else
    static_cast<void>(reader_id);
#endif
}


ref_type SlabAlloc::attach_buffer(const char* data, size_t size)
{
    // ExceptionSafety: If this function throws, it must leave the allocator in
    // the detached state.

    REALM_ASSERT(!is_attached());

    // Verify the data structures
    std::string path;                  // No path
    validate_buffer(data, size, path); // Throws

    ref_type top_ref = get_top_ref(data, size);

    m_data = data;
    m_baseline = size;
    m_initial_chunk_size = size;
    m_attach_mode = attach_UsersBuffer;

    // Below this point (assignment to `m_attach_mode`), nothing must throw.

    return top_ref;
}


void SlabAlloc::attach_empty()
{
    // ExceptionSafety: If this function throws, it must leave the allocator in
    // the detached state.

    REALM_ASSERT(!is_attached());

    m_attach_mode = attach_OwnedBuffer;
    m_data = nullptr; // Empty buffer

    // Below this point (assignment to `m_attach_mode`), nothing must throw.

    // No ref must ever be less that the header size, so we will use that as the
    // baseline here.
    m_baseline = sizeof(Header);
    m_initial_chunk_size = m_baseline;
}

void SlabAlloc::throw_header_exception(std::string msg, const Header& header, const std::string& path)
{
    char buf[256];
    sprintf(buf,
            ". top_ref[0]: %" PRIX64 ", top_ref[1]: %" PRIX64 ", "
            "mnemonic: %X %X %X %X, fmt[0]: %d, fmt[1]: %d, flags: %X",
            header.m_top_ref[0], header.m_top_ref[1], header.m_mnemonic[0], header.m_mnemonic[1],
            header.m_mnemonic[2], header.m_mnemonic[3], header.m_file_format[0], header.m_file_format[1],
            header.m_flags);
    msg += buf;
    throw InvalidDatabase(msg, path);
}

void SlabAlloc::validate_buffer(const char* data, size_t size, const std::string& path)
{
    const Header& header = *reinterpret_cast<const Header*>(data);

    // Verify that size is sane and 8-byte aligned
    if (REALM_UNLIKELY(size < sizeof(Header) || size % 8 != 0)) {
        std::string msg = "Realm file has bad size (" + util::to_string(size) + ")";
        throw InvalidDatabase(msg, path);
    }

    // First four bytes of info block is file format id
    if (REALM_UNLIKELY(!(char(header.m_mnemonic[0]) == 'T' && char(header.m_mnemonic[1]) == '-' &&
                         char(header.m_mnemonic[2]) == 'D' && char(header.m_mnemonic[3]) == 'B')))
        throw_header_exception("Invalid mnemonic", header, path);

    // Last bit in info block indicates which top_ref block is valid
    int slot_selector = ((header.m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);

    // Top-ref must always point within buffer
    uint_fast64_t top_ref = uint_fast64_t(header.m_top_ref[slot_selector]);
    if (slot_selector == 0 && top_ref == 0xFFFFFFFFFFFFFFFFULL) {
        if (REALM_UNLIKELY(size < sizeof(Header) + sizeof(StreamingFooter))) {
            std::string msg = "Invalid streaming format size (" + util::to_string(size) + ")";
            throw InvalidDatabase(msg, path);
        }
        const StreamingFooter& footer = *(reinterpret_cast<const StreamingFooter*>(data + size) - 1);
        top_ref = footer.m_top_ref;
        if (REALM_UNLIKELY(footer.m_magic_cookie != footer_magic_cookie)) {
            std::string msg = "Invalid streaming format cookie (" + util::to_string(footer.m_magic_cookie) + ")";
            throw InvalidDatabase(msg, path);
        }
    }
    if (REALM_UNLIKELY(top_ref % 8 != 0)) {
        std::string msg = "Top ref not aligned (" + util::to_string(top_ref) + ")";
        throw_header_exception(msg, header, path);
    }
    if (REALM_UNLIKELY(top_ref >= size)) {
        std::string msg = "Top ref outside file (size = " + util::to_string(size) + ")";
        throw_header_exception(msg, header, path);
    }
}


size_t SlabAlloc::get_total_size() const noexcept
{
    return m_slabs.empty() ? m_baseline : m_slabs.back().ref_end;
}


void SlabAlloc::reset_free_space_tracking()
{
    internal_invalidate_cache();
    if (is_free_space_clean())
        return;

    // Free all scratch space (done after all data has
    // been commited to persistent space)
    m_free_read_only.clear();

    clear_freelists();
    rebuild_freelists_from_slab();
    m_free_space_state = free_space_Clean;
}


void SlabAlloc::update_reader_view(size_t file_size)
{
    internal_invalidate_cache();
    if (file_size <= m_baseline) {
        return;
    }
    REALM_ASSERT(file_size % 8 == 0); // 8-byte alignment required
    REALM_ASSERT(m_attach_mode == attach_SharedFile || m_attach_mode == attach_UnsharedFile);
    REALM_ASSERT_DEBUG(is_free_space_clean());

    // Extend mapping by adding sections
    REALM_ASSERT_DEBUG(matches_section_boundary(file_size));
    size_t old_baseline = m_baseline;
    m_baseline = file_size;
    {
        // Serialize manipulations of the shared mappings:
        std::lock_guard<util::Mutex> lock(m_file_mappings->m_mutex);

        // figure out how many mappings we need to match the requested size
        size_t num_sections = get_section_index(file_size);
        size_t num_additional_mappings = num_sections - m_file_mappings->m_first_additional_mapping;

        // If the mapping array is filled to capacity, create a new one and copy over
        // the references to the existing mappings.
        if (num_additional_mappings > m_file_mappings->m_capacity_global_mappings) {
            // FIXME: No harcoded constants here
            m_file_mappings->m_capacity_global_mappings = num_additional_mappings + 128;
            std::unique_ptr<std::shared_ptr<const util::File::Map<char>>[]> new_mappings;
            new_mappings.reset(
                new std::shared_ptr<const util::File::Map<char>>[m_file_mappings->m_capacity_global_mappings]);
            for (size_t j = 0; j < m_file_mappings->m_num_global_mappings; ++j)
                new_mappings[j] = m_file_mappings->m_global_mappings[j];
            m_file_mappings->m_global_mappings = std::move(new_mappings);
        }

        // Add any additional mappings needed to fully map the larger file
        for (size_t k = m_file_mappings->m_num_global_mappings; k < num_additional_mappings; ++k) {
            size_t section_start_offset = get_section_base(k + m_file_mappings->m_first_additional_mapping);
            size_t section_size =
                get_section_base(1 + k + m_file_mappings->m_first_additional_mapping) - section_start_offset;
            m_file_mappings->m_global_mappings[k] = std::make_shared<const util::File::Map<char>>(
                m_file_mappings->m_file, section_start_offset, File::access_ReadOnly, section_size);
        }

        // Share the increased number of mappings. This *must* be a conditional update to ensure
        // that the number of mappings is ever only increased. Multiple threads may want to grow
        // to different file sizes. While the actual growth process is serialized, the target size
        // is determined earlier and without serialization. The largest target size must "win" the race.
        if (num_additional_mappings > m_file_mappings->m_num_global_mappings)
            m_file_mappings->m_num_global_mappings = num_additional_mappings;

        // update local cache of mappings, if global mappings have been extended beyond local
        if (num_additional_mappings > m_num_local_mappings) {
            m_num_local_mappings = num_additional_mappings;
            m_local_mappings.reset(new std::shared_ptr<const util::File::Map<char>>[m_num_local_mappings]);
            for (size_t k = 0; k < m_num_local_mappings; ++k) {
                m_local_mappings[k] = m_file_mappings->m_global_mappings[k];
            }
        }
    }
    // Rebase slabs as m_baseline has moved
    size_t ref_displacement = m_baseline - old_baseline;
    for (auto& e : m_slabs) {
        e.ref_end += ref_displacement;
    }
    // rebuild free list from slabs
    rebuild_freelists_from_slab();
    /*
        size_t slab_ref = file_size;
        size_t n = m_free_space.size();
        REALM_ASSERT(m_slabs.size() == n);
        for (size_t i = 0; i < n; ++i) {
            Chunk& free_chunk = m_free_space[i];
            free_chunk.ref = slab_ref;
            ref_type slab_ref_end = slab_ref + free_chunk.size;
            m_slabs[i].ref_end = slab_ref_end;
            slab_ref = slab_ref_end;
        }
        */
}

const SlabAlloc::Chunks& SlabAlloc::get_free_read_only() const
{
    if (REALM_COVER_NEVER(m_free_space_state == free_space_Invalid))
        throw InvalidFreeSpace();
    return m_free_read_only;
}


// A database file is viewed as a number of sections of exponentially growing size.
// The first 16 sections are 1 x page size, the next 8 sections are 2 x page size,
// then follows 8 sections of 4 x page size, 8 sections of 8 x page size and so forth.
// This layout makes it possible to determine the section number for a given offset
// into the file in constant time using a bit scan intrinsic and a few bit manipulations.
// The get_section_index() method determines the section number from the offset, while
// the get_section_base() does the opposite, giving the starting offset for a given
// section number.
//
// Please note that the file is not necessarily mmapped with a separate mapping
// for each section, multiple sections may be mmapped with a single mmap.

size_t SlabAlloc::get_section_index(size_t pos) const noexcept
{
    // size_t section_base_number = pos/m_initial_section_size;
    size_t section_base_number = pos >> m_section_shifts;
    size_t section_group_number = section_base_number / 16;
    size_t index;
    if (section_group_number == 0) {
        // first 16 entries aligns 1:1
        index = section_base_number;
    }
    else {
        // remaning entries are exponential
        size_t log_index = log2(section_group_number);
        size_t section_index_in_group = (section_base_number >> (1 + log_index)) & 0x7;
        index = (16 + (log_index * 8)) + section_index_in_group;
    }
    return index;
}

size_t SlabAlloc::compute_section_base(size_t index) const noexcept
{
    size_t base;
    if (index < 16) {
        // base = index * m_initial_section_size;
        base = index << m_section_shifts;
    }
    else {
        size_t section_index_in_group = index & 7;
        size_t log_index = (index - section_index_in_group) / 8 - 2;
        size_t section_base_number = (8 + section_index_in_group) << (1 + log_index);
        // base = m_initial_section_size * section_base_number;
        base = section_base_number << m_section_shifts;
    }
    return base;
}

size_t SlabAlloc::find_section_in_range(size_t start_pos, size_t free_chunk_size, size_t request_size) const noexcept
{
    size_t end_of_block = start_pos + free_chunk_size;
    size_t alloc_pos = start_pos;
    while (alloc_pos + request_size <= end_of_block) {
        size_t next_section_boundary = get_upper_section_boundary(alloc_pos);
        if (alloc_pos + request_size <= next_section_boundary) {
            return alloc_pos;
        }
        alloc_pos = next_section_boundary;
    }
    return 0;
}


void SlabAlloc::resize_file(size_t new_file_size)
{
    std::lock_guard<Mutex> lock(m_file_mappings->m_mutex);
    REALM_ASSERT(matches_section_boundary(new_file_size));
    m_file_mappings->m_file.prealloc(new_file_size); // Throws

    bool disable_sync = get_disable_sync_to_disk() || m_cfg.disable_sync;
    if (!disable_sync)
        m_file_mappings->m_file.sync(); // Throws
}

#ifdef REALM_DEBUG
void SlabAlloc::reserve_disk_space(size_t size)
{
    std::lock_guard<Mutex> lock(m_file_mappings->m_mutex);
    if (!matches_section_boundary(size))
        size = get_upper_section_boundary(size);
    m_file_mappings->m_file.prealloc(size); // Throws

    bool disable_sync = get_disable_sync_to_disk() || m_cfg.disable_sync;
    if (!disable_sync)
        m_file_mappings->m_file.sync(); // Throws
}
#endif

void SlabAlloc::verify() const
{
#ifdef REALM_DEBUG
    // Make sure that all free blocks fit within a slab
    /* FIXME
    for (const auto& chunk : m_free_space) {
        slabs::const_iterator slab =
            upper_bound(m_slabs.begin(), m_slabs.end(), chunk.ref, &ref_less_than_slab_ref_end);
        REALM_ASSERT(slab != m_slabs.end());

        ref_type slab_ref_end = slab->ref_end;
        ref_type chunk_ref_end = chunk.ref + chunk.size;
        REALM_ASSERT_3(chunk_ref_end, <=, slab_ref_end);
    }
    */
#endif
}

#ifdef REALM_DEBUG

bool SlabAlloc::is_all_free() const
{
    /*
     * FIXME
    if (m_free_space.size() != m_slabs.size())
        return false;

    // Verify that free space matches slabs
    ref_type slab_ref = m_baseline;
    for (const auto& slab : m_slabs) {
        size_t slab_size = slab.ref_end - slab_ref;
        chunks::const_iterator chunk = find_if(m_free_space.begin(), m_free_space.end(), ChunkRefEq(slab_ref));
        if (chunk == m_free_space.end())
            return false;
        if (slab_size != chunk->size)
            return false;
        slab_ref = slab.ref_end;
    }
    */
    return true;
}


// LCOV_EXCL_START
void SlabAlloc::print() const
{
    /* FIXME
     *

    size_t allocated_for_slabs = m_slabs.empty() ? 0 : m_slabs.back().ref_end - m_baseline;

    size_t free = 0;
    for (const auto& free_block : m_free_space) {
        free += free_block.size;
    }

    size_t allocated = allocated_for_slabs - free;
    std::cout << "Attached: " << (m_data ? m_baseline : 0) << " Allocated: " << allocated << "\n";

    if (!m_slabs.empty()) {
        std::cout << "Slabs: ";
        ref_type first_ref = m_baseline;

        for (const auto& slab : m_slabs) {
            if (&slab != &m_slabs.front())
                std::cout << ", ";

            ref_type last_ref = slab.ref_end - 1;
            size_t size = slab.ref_end - first_ref;
            void* addr = slab.addr;
            std::cout << "(" << first_ref << "->" << last_ref << ", size=" << size << ", addr=" << addr << ")";
            first_ref = slab.ref_end;
        }
        std::cout << "\n";
    }

    if (!m_free_space.empty()) {
        std::cout << "FreeSpace: ";
        for (const auto& free_block : m_free_space) {
            if (&free_block != &m_free_space.front())
                std::cout << ", ";

            ref_type last_ref = free_block.ref + free_block.size - 1;
            std::cout << "(" << free_block.ref << "->" << last_ref << ", size=" << free_block.size << ")";
        }
        std::cout << "\n";
    }
    if (!m_free_read_only.empty()) {
        std::cout << "FreeSpace (ro): ";
        for (const auto& free_block : m_free_read_only) {
            if (&free_block != &m_free_read_only.front())
                std::cout << ", ";

            ref_type last_ref = free_block.ref + free_block.size - 1;
            std::cout << "(" << free_block.ref << "->" << last_ref << ", size=" << free_block.size << ")";
        }
        std::cout << "\n";
    }
    std::cout << std::flush;
    */
}
// LCOV_EXCL_STOP

#endif // REALM_DEBUG
