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


SlabAlloc::SlabAlloc()
{
    m_initial_section_size = 1UL << section_shift; // page_size();
    m_free_space_state = free_space_Clean;
    m_baseline = 0;
}

util::File& SlabAlloc::get_file()
{
    return m_file;
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
    delete[] m_ref_translation_ptr;
    m_ref_translation_ptr.store(nullptr);
    m_translation_table_size = 0;
    set_read_only(true);
    purge_old_mappings(static_cast<uint64_t>(-1), 0);
    m_compatibility_mapping.unmap();
    m_sections_in_compatibility_mapping = 0;
    switch (m_attach_mode) {
        case attach_None:
            break;
        case attach_UsersBuffer:
            break;
        case attach_OwnedBuffer:
            delete[] m_data;
            break;
        case attach_SharedFile:
        case attach_UnsharedFile:
            m_data = 0;
            m_mappings.clear();
            m_youngest_live_version = 0;
            m_file.close();
            break;
        default:
            REALM_UNREACHABLE();
    }

    // Release all allocated memory - this forces us to create new
    // slabs after re-attaching thereby ensuring that the slabs are
    // placed correctly (logically) after the end of the file.
    for (auto& slab : m_slabs) {
        util::munmap(slab.addr, 1UL << section_shift);
    }
    m_slabs.clear();

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


MemRef SlabAlloc::do_alloc(const size_t size)
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
    CriticalSection cs(changes);
    REALM_ASSERT(0 < size);
    REALM_ASSERT((size & 0x7) == 0); // only allow sizes that are multiples of 8
    REALM_ASSERT(is_attached());

    // If we failed to correctly record free space, new allocations cannot be
    // carried out until the free space record is reset.
    if (REALM_COVER_NEVER(m_free_space_state == free_space_Invalid))
        throw InvalidFreeSpace();

    m_free_space_state = free_space_Dirty;

    // Do we have a free space we can reuse?
    {
        typedef chunks::reverse_iterator iter;
        iter rend = m_free_space.rend();
        for (iter i = m_free_space.rbegin(); i != rend; ++i) {
            if (size <= i->size) {

#if REALM_ENABLE_MEMDEBUG
                // Pick a *random* match instead of just the first. This will increase the chance of catching
                // use-after-free bugs in Core. It's chosen such that the mathematical average of all picked
                // positions is the middle of the list.
                iter j = i;
                while (j != rend && (size > j->size || fastrand() % (m_free_space.size() / 2 + 1) != 0)) {
                    j++;
                }
                if (j != rend)
                    i = j;
#endif

                ref_type ref = i->ref;
                size_t rest = i->size - size;

                // Update free list
                if (rest == 0) {
                    // Erase by "move last over"
                    *i = m_free_space.back();
                    m_free_space.pop_back();
                }
                else {
                    i->size = rest;
                    i->ref += size;
                }

#ifdef REALM_DEBUG
                if (REALM_COVER_NEVER(m_debug_out))
                    std::cerr << "Alloc ref: " << ref << " size: " << size << "\n";
#endif

                char* addr = translate(ref);
#if REALM_ENABLE_ALLOC_SET_ZERO
                std::fill(addr, addr + size, 0);
#endif
#ifdef REALM_SLAB_ALLOC_DEBUG
                malloc_debug_map[ref] = malloc(1);
#endif
                REALM_ASSERT_EX(ref >= m_baseline, ref, m_baseline);
                return MemRef(addr, ref, *this);
            }
        }
    }


    // Allocate new slab. We allocate a full section but use mmap, so it'll
    // be paged in on demand.
    size_t new_size = 1UL << section_shift;
    ref_type ref;
    if (m_slabs.empty()) {
        ref = align_size_to_section_boundary(m_baseline.load(std::memory_order_relaxed));
    }
    else {
        // Find size of memory that has been modified (through copy-on-write) in current write transaction
        ref_type curr_ref_end = to_size_t(m_slabs.back().ref_end);
        REALM_ASSERT_DEBUG_EX(curr_ref_end >= m_baseline, curr_ref_end, m_baseline);
        ref = curr_ref_end;
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
        throw MaximumFileSizeExceeded("AllocSlab slab ref_end size overflow: " + util::to_string(ref) + " + "
                                 + util::to_string(new_size));
    }

    REALM_ASSERT(matches_section_boundary(ref));
    REALM_ASSERT(matches_section_boundary(ref_end));
    char* mem = static_cast<char*>(util::mmap_anon(new_size));
#if REALM_ENABLE_ALLOC_SET_ZERO
    std::fill(mem.get(), mem.get() + new_size, 0);
#endif
    // Add to list of slabs
    Slab slab;
    slab.addr = mem;
    slab.ref_end = ref_end;
    std::lock_guard<std::mutex> lock(m_mapping_mutex);
    // FIXME: Leaks if push_back throws
    m_slabs.push_back(slab); // Throws
    extend_fast_mapping_with_slab(mem);
    // Update free list
    size_t unused = new_size - size;
    if (0 < unused) {
        Chunk chunk;
        chunk.ref = ref;
        if (REALM_UNLIKELY(int_add_with_overflow_detect(chunk.ref, size))) {
            throw MaximumFileSizeExceeded("AllocSlab free list ref size overflow: " + util::to_string(ref) + " + "
                                     + util::to_string(size));
        }
        chunk.size = unused;
        m_free_space.push_back(chunk); // Throws
    }

#ifdef REALM_DEBUG
    if (REALM_COVER_NEVER(m_debug_out))
        std::cerr << "Alloc ref: " << ref << " size: " << size << "\n";
#endif

#if REALM_ENABLE_ALLOC_SET_ZERO
    std::fill(slab.addr, slab.addr + size, 0);
#endif
#ifdef REALM_SLAB_ALLOC_DEBUG
    malloc_debug_map[ref] = malloc(1);
#endif
    REALM_ASSERT_EX(ref >= m_baseline, ref, m_baseline);
    return MemRef(slab.addr, ref, *this);
}


void SlabAlloc::do_free(ref_type ref, const char* addr)
{
    REALM_ASSERT_3(translate(ref), ==, addr);
    CriticalSection cs(changes);

    // Free space in read only segment is tracked separately
    bool read_only = is_read_only(ref);
    chunks& free_space = read_only ? m_free_read_only : m_free_space;

#ifdef REALM_SLAB_ALLOC_DEBUG
    free(malloc_debug_map[ref]);
#endif

    // Get size from segment
    size_t size =
        read_only ? NodeHeader::get_byte_size_from_header(addr) : NodeHeader::get_capacity_from_header(addr);
    ref_type ref_end = ref + size;

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

#ifdef REALM_DEBUG
    // Check for double free

    for (auto& c : free_space) {
        if ((ref >= c.ref && ref < (c.ref + c.size)) || (ref < c.ref && ref_end > c.ref)) {
            REALM_ASSERT(false && "Double Free");
        }
    }

#endif

    // Check if we can merge with adjacent succeeding free block
    typedef chunks::iterator iter;
    iter merged_with = free_space.end();
    if (!read_only) {
        {
            iter i = find_if(free_space.begin(), free_space.end(), ChunkRefEq(ref_end));
            if (i != free_space.end()) {
                // No consolidation over slab borders
                if (find_if(m_slabs.begin(), m_slabs.end(), SlabRefEndEq(ref_end)) == m_slabs.end()) {
                    i->ref = ref;
                    i->size += size;
                    merged_with = i;
                }
            }
        }

        // Check if we can merge with adjacent preceeding free block (not if that
        // would cross slab boundary)
        if (find_if(m_slabs.begin(), m_slabs.end(), SlabRefEndEq(ref)) == m_slabs.end()) {
            iter i = find_if(free_space.begin(), free_space.end(), ChunkRefEndEq(ref));
            if (i != free_space.end()) {
                if (merged_with != free_space.end()) {
                    i->size += merged_with->size;
                    // Erase by "move last over"
                    *merged_with = free_space.back();
                    free_space.pop_back();
                }
                else {
                    i->size += size;
                }
                return;
            }
        }
    }

    // Else just add to freelist
    if (merged_with == free_space.end()) {
        try {
            Chunk chunk;
            chunk.ref = ref;
            chunk.size = size;
            free_space.push_back(chunk); // Throws
        }
        catch (...) {
            m_free_space_state = free_space_Invalid;
        }
    }
}


void SlabAlloc::consolidate_free_read_only()
{
    CriticalSection cs(changes);
    if (REALM_COVER_NEVER(m_free_space_state == free_space_Invalid))
        throw InvalidFreeSpace();
    if (m_free_read_only.empty())
        return;

    std::sort(begin(m_free_read_only), end(m_free_read_only), [](auto& a, auto& b) { return a.ref < b.ref; });

    // Combine any adjacent chunks in the freelist, except for when the chunks
    // are on the edge of an allocation slab
    auto prev = m_free_read_only.begin();
    for (auto it = m_free_read_only.begin() + 1; it != m_free_read_only.end(); ++it) {
        if (prev->ref + prev->size != it->ref) {
            prev = it;
            continue;
        }

        prev->size += it->size;
        it->size = 0;
    }

    // Remove all of the now zero-size chunks from the free list
    m_free_read_only.erase(
        std::remove_if(begin(m_free_read_only), end(m_free_read_only), [](auto& chunk) { return chunk.size == 0; }),
        end(m_free_read_only));
}


MemRef SlabAlloc::do_realloc(size_t ref, const char* addr, size_t old_size, size_t new_size)
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


char* SlabAlloc::do_translate(ref_type) const noexcept
{
    REALM_ASSERT(false); // never come here
    return nullptr;
}


int SlabAlloc::get_committed_file_format_version() const noexcept
{
    if (m_mappings.size()) {
        // if we have mapped a file, m_mappings will have at least one mapping and
        // the first will be to the start of the file. Don't come here, if we're
        // just attaching a buffer. They don't have mappings.
        realm::util::encryption_read_barrier(m_mappings[0], 0, sizeof(Header));
    }
    const Header& header = *reinterpret_cast<const Header*>(m_data);
    int slot_selector = ((header.m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);
    int file_format_version = int(header.m_file_format[slot_selector]);
    return file_format_version;
}

bool SlabAlloc::is_file_on_streaming_form(const Header& header)
{
    // LIMITATION: Only come here if we've already had a read barrier for the affected part of the file
    int slot_selector = ((header.m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);
    uint_fast64_t ref = uint_fast64_t(header.m_top_ref[slot_selector]);
    return (slot_selector == 0 && ref == 0xFFFFFFFFFFFFFFFFULL);
}

ref_type SlabAlloc::get_top_ref(const char* buffer, size_t len)
{
    // LIMITATION: Only come here if we've already had a read barrier for the affected part of the file
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

ref_type SlabAlloc::attach_file(const std::string& file_path, Config& cfg)
{
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
    // FIXME: Currently we cannot enforce read-only mode on every allocation
    // in the shared slab allocator, because we always create a minimal group
    // representation in memory, even in a read-transaction, if the file is empty.
    // m_is_read_only = cfg.read_only;
    set_read_only(false);
    m_file.open(path.c_str(), access, create, 0); // Throws
    auto physical_file_size = m_file.get_size();
    // Note that get_size() may (will) return a different size before and after
    // the call below to set_encryption_key.
    if (cfg.encryption_key) {
        m_file.set_encryption_key(cfg.encryption_key);
    }
    File::CloseGuard fcg(m_file);

    size_t size = 0;
    // The size of a database file must not exceed what can be encoded in
    // size_t.
    if (REALM_UNLIKELY(int_cast_with_overflow_detect(m_file.get_size(), size)))
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
        m_file.write(data, sizeof empty_file_header); // Throws

        // Pre-alloc initial space
        size_t initial_size = page_size(); // m_initial_section_size;
        m_file.prealloc(initial_size);     // Throws

        bool disable_sync = get_disable_sync_to_disk();
        if (!disable_sync)
            m_file.sync(); // Throws

        size = initial_size;
    }
    ref_type top_ref;
    File::Map<char> initial_mapping;
    try {
        File::Map<char> map(m_file, File::access_ReadOnly, size); // Throws
        // we'll read header and (potentially) footer
        realm::util::encryption_read_barrier(map, 0, sizeof(Header));
        realm::util::encryption_read_barrier(map, size - sizeof(Header), sizeof(Header));

        validate_header(map.get_addr(), size, path); // Throws

        top_ref = get_top_ref(map.get_addr(), size);

        m_data = map.get_addr();
        initial_mapping = std::move(map); // replace at end of function
        // with correctly sized chunks instead...
        m_baseline = 0;
        m_attach_mode = cfg.is_shared ? attach_SharedFile : attach_UnsharedFile;
    }
    catch (DecryptionFailed) {
        throw InvalidDatabase("Realm file decryption failed", path);
    }
    // make sure that any call to begin_read cause any slab to be placed in free
    // lists correctly
    m_free_space_state = free_space_Invalid; // Odd! FIXME

    // Ensure clean up, if we need to back out:
    DetachGuard dg(*this);

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
            File::Map<Header> writable_map(m_file, File::access_ReadWrite, sizeof(Header)); // Throws
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

            realm::util::encryption_read_barrier(initial_mapping, 0, sizeof(Header));
        }
    }
    int file_format_version = get_committed_file_format_version();
    initial_mapping.unmap();
    m_data = nullptr;

    // We can only safely mmap the file, if its size matches a page boundary. If not,
    // we must change the size to match before mmaping it.
    if (size != round_up_to_page_size(size)) {
        // The file size did not match a page boundary.
        // We must extend the file to a page boundary (unless already there)
        // The file must be extended to match in size prior to being mmapped,
        // as extending it after mmap has undefined behavior.

        // The mapping of the first part of the file *must* be contiguous, because
        // we do not know if the file was created by a version of the code, that took
        // the section boundaries into account. If it wasn't we cannot map it in sections
        // without risking datastructures that cross a mapping boundary.

        // FIXME: This should be replaced by special handling for the older file formats,
        // where we map contiguously but split the mapping into same sized maps afterwards.
        // This will allow os to avoid a lot of mapping manipulations during file open.
        if (cfg.read_only) {

            // If the file is opened read-only, we cannot extend it. This is not a problem,
            // because for a read-only file we assume that it will not change while we use it.
            // This assumption obviously will not hold, if the file is shared by multiple
            // processes or threads with different opening modes.
            // Currently, there is no way to detect if this assumption is violated.
            m_baseline = 0;
            ;
        }
        else {

            if (cfg.session_initiator || !cfg.is_shared) {

                // We can only safely extend the file if we're the session initiator, or if
                // the file isn't shared at all. Extending the file to a page boundary is ONLY
                // done to ensure well defined behavior for memory mappings. It does not matter,
                // that the free space management isn't informed
                size = round_up_to_page_size(size);
                m_file.prealloc(size);
                m_baseline = 0;
            }
            else {
                // Getting here, we have a file of a size that will not work, and without being
                // allowed to extend it. This should not be possible. But allowing a retry is
                // arguably better than giving up and crashing...
                throw Retry();
            }
        }
    }

    reset_free_space_tracking();
    // if the file format is older than version 10 and larger than a section we have
    // to use the compatibility mapping
    // FIXME: For now always use compatibility mapping.
    static_cast<void>(file_format_version); // silence a warning
    if (size > get_section_base(1) /* && file_format_version < 10 */) {
        setup_compatibility_mapping(size);
        m_data = m_compatibility_mapping.get_addr();
    }
    else {
        update_reader_view(size);
        m_data = m_mappings[0].get_addr();
    }
    REALM_ASSERT(m_mappings.size());
    dg.release();  // Do not detach
    fcg.release(); // Do not close
    return top_ref;
}

void SlabAlloc::setup_compatibility_mapping(size_t file_size)
{
    m_sections_in_compatibility_mapping = int(get_section_index(file_size));
    REALM_ASSERT(m_sections_in_compatibility_mapping);
    m_compatibility_mapping = util::File::Map<char>(get_file(), util::File::access_ReadOnly, file_size);
    // fake that we've only mapped the number of full sections in order
    // to allow additional mappings to start aligned to a section boundary,
    // even though the compatibility mapping may extend further.
    m_baseline = get_section_base(m_sections_in_compatibility_mapping);
    update_reader_view(file_size);
}

ref_type SlabAlloc::attach_buffer(const char* data, size_t size)
{
    // ExceptionSafety: If this function throws, it must leave the allocator in
    // the detached state.

    REALM_ASSERT(!is_attached());
    REALM_ASSERT(size <= (1UL << section_shift));
    // Verify the data structures
    std::string path; // No path
    validate_header(data, size, path); // Throws

    ref_type top_ref = get_top_ref(data, size);

    m_data = data;
    size = align_size_to_section_boundary(size);
    m_baseline = size;
    m_attach_mode = attach_UsersBuffer;

    m_translation_table_size = 1;
    m_ref_translation_ptr = new RefTranslation[1];
#if REALM_ENABLE_ENCRYPTION
    m_ref_translation_ptr[0] = {const_cast<char*>(m_data), nullptr};
#else
    m_ref_translation_ptr[0] = {const_cast<char*>(m_data)};
#endif
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

    // No ref must ever be less than the header size, so we will use that as the
    // baseline here.
    size_t size = align_size_to_section_boundary(sizeof(Header));
    m_baseline = size;
    m_translation_table_size = 1;
    m_ref_translation_ptr = new RefTranslation[1];
#if REALM_ENABLE_ENCRYPTION
    m_ref_translation_ptr[0] = {nullptr, nullptr};
#else
    m_ref_translation_ptr[0] = {nullptr};
#endif
}


void SlabAlloc::validate_header(const char* data, size_t size, const std::string& path)
{
    // Verify that size is sane and 8-byte aligned
    if (REALM_UNLIKELY(size < sizeof(Header) || size % 8 != 0))
        throw InvalidDatabase("Realm file has bad size", path);

    const Header& header = *reinterpret_cast<const Header*>(data);

    // First four bytes of info block is file format id
    if (REALM_UNLIKELY(!(char(header.m_mnemonic[0]) == 'T' && char(header.m_mnemonic[1]) == '-' &&
                         char(header.m_mnemonic[2]) == 'D' && char(header.m_mnemonic[3]) == 'B')))
        throw InvalidDatabase("Not a Realm file", path);

    // Last bit in info block indicates which top_ref block is valid
    int slot_selector = ((header.m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);

    // Top-ref must always point within buffer
    uint_fast64_t top_ref = uint_fast64_t(header.m_top_ref[slot_selector]);
    if (slot_selector == 0 && top_ref == 0xFFFFFFFFFFFFFFFFULL) {
        if (REALM_UNLIKELY(size < sizeof(Header) + sizeof(StreamingFooter)))
            throw InvalidDatabase("Realm file in streaming form has bad size", path);
        const StreamingFooter& footer = *(reinterpret_cast<const StreamingFooter*>(data + size) - 1);
        top_ref = footer.m_top_ref;
        if (REALM_UNLIKELY(footer.m_magic_cookie != footer_magic_cookie))
            throw InvalidDatabase("Bad Realm file header (#1)", path);
    }
    if (REALM_UNLIKELY(top_ref % 8 != 0))
        throw InvalidDatabase("Bad Realm file header (#2)", path);
    if (REALM_UNLIKELY(top_ref >= size))
        throw InvalidDatabase("Bad Realm file header (#3)", path);
}


size_t SlabAlloc::get_total_size() const noexcept
{
    return m_slabs.empty() ? size_t(m_baseline.load(std::memory_order_relaxed)) : m_slabs.back().ref_end;
}


void SlabAlloc::reset_free_space_tracking()
{
    CriticalSection cs(changes);
    if (is_free_space_clean())
        return;

    // Free all scratch space (done after all data has
    // been commited to persistent space)
    m_free_read_only.clear();
    m_free_space.clear();

    // Rebuild free list to include all slabs
    Chunk chunk;
    chunk.ref = align_size_to_section_boundary(m_baseline.load(std::memory_order_relaxed));

    for (const auto& slab : m_slabs) {
        chunk.size = slab.ref_end - chunk.ref;
        m_free_space.push_back(chunk); // Throws
        chunk.ref = slab.ref_end;
    }

#ifdef REALM_DEBUG
    REALM_ASSERT_DEBUG(is_all_free());
#endif

    m_free_space_state = free_space_Clean;
}

inline bool randomly_false_in_debug(bool x)
{
#ifdef REALM_DEBUG
    if (x)
        return (std::rand() & 1);
#endif
    return x;
}


/*
  Memory mapping

  To make ref->ptr translation fast while also avoiding to have to memory map the entire file
  contiguously (which is a problem for large files on 32-bit devices and most iOS devices), it is
  essential to map the file in even sized sections.

  These sections must be large enough to hold one or more of the largest arrays, which can be up
  to 16MB. You can only mmap file space which has been allocated to a file. If you mmap a range
  which extends beyond the last page of a file, the result is undefined, so we can't do that.
  We don't want to extend the file in increments as large as the chunk size.

  The approach chosen on unixes is to only reserve a *virtual address range* large enough for
  a chunk, and then gradually re-map it as the file grows. Having reserved it first, we're
  guaranteed that the remapping operation will succeed. This allows us to ensure that any
  existing mapping continues to stay valid even as new mappings are added. This is crucial
  for sharing the mapping between multiple readers.

  Unfortunately, there are no similar guarantees on Windows. On Windows you *can* reserve
  address space for a full chunk, but you cannot remap only a portion of it. Only the full
  section. Furthermore, you have to first release the reserved address space and then map
  the file. This conceivably allows a different thread asking for virtual memory to grab
  it between it is released and the file is mapped. Another complication is that on Windows
  you cannot subdivide or coalesce a mapping range, because established mappings are identified
  solely by their starting address, not as a range.

  On Windows we choose to grow by creating a new larger memory mapping, which replaces the
  old one in the mapping table. However, we must keep the old mapping open, because older
  read transactions will continue to use it. Hence, the replaced mappings are accumulated
  and only cleaned out once we know that no transaction can refer to them anymore.

  Interaction with encryption

  When encryption is enabled, the memory mapping is to temporary memory, not the file.
  The binding to the file is done by software. This allows us to "cheat" and allocate
  entire sections. With encryption, it doesn't matter if the mapped memory logically
  extends beyond the end of file, because it will not be accessed.

  The following "branching out" is placed in "file.cpp":

  Operation:   Encryption:           No encryption, Unixes:     No encryption, Windows:

  reserve      allocate req          reserve chunk              no-op
  extend       no-op                 remap part of reservation  allocate req (*)
  mmap         allocate req          allocate req               allocate req

  (*) If the mapping is already established for part of the requested area, then
      extend() fail, and we must preserve the earlier mapping as described above,
      then create a new one.

  Growing/Changing the mapping table.

  There are two mapping tables:

  * m_mappings: This is the "source of truth" about what the current mapping is.
    It is only accessed under lock.
  * m_fast_mapping: This is generated to match m_mappings, but is also accessed without
    any locking from the translate function. Because of the lock free operation this
    table can only be extended. Entries in it cannot be changed. The fast mapping also
    maps the slab area used for allocations - as mappings are added, the slab area *moves*,
    corresponding to the movement of m_baseline. This movement does not need to trigger
    generation of a new m_fast_mapping table, because it is only relevant to memory
    allocation and release, which is already serialized (since write transactions are
    single threaded).

  When m_mappings is changed due to an extend operation changing a mapping, or when
  it has grown such that it cannot be reflected in m_fast_mapping:

  * A new fast mapping table is created. The old one is not modified.
  * The old one is held in a waiting area until it is no longer relevant because no
    live transaction can refer to it any more.

 */
void SlabAlloc::update_reader_view(size_t file_size)
{
    std::lock_guard<std::mutex> lock(m_mapping_mutex);
    if (file_size <= m_baseline.load(std::memory_order_relaxed)) {
        return;
    }
    REALM_ASSERT(file_size % 8 == 0); // 8-byte alignment required
    REALM_ASSERT(m_attach_mode == attach_SharedFile || m_attach_mode == attach_UnsharedFile);
    REALM_ASSERT_DEBUG(is_free_space_clean());
    bool requires_new_translation = false;

    // Extend mapping by adding sections, or by extending sections
    size_t old_baseline = m_baseline.load(std::memory_order_relaxed);
    auto old_slab_base = align_size_to_section_boundary(old_baseline);
    size_t old_num_sections = get_section_index(old_slab_base);
    REALM_ASSERT(m_mappings.size() == old_num_sections - m_sections_in_compatibility_mapping);
    m_baseline.store(file_size, std::memory_order_relaxed);
    {
        // 0. Special case: figure out if extension is to be done entirely within a single
        // existing mapping. This is the case if the new baseline (which must be larger
        // then the old baseline) is still below the old base of the slab area.
        auto mapping_index = old_num_sections - 1 - m_sections_in_compatibility_mapping;
        if (file_size < old_slab_base) {
            size_t section_start_offset = get_section_base(old_num_sections - 1);
            size_t section_size = file_size - section_start_offset;
            auto ok = m_mappings[mapping_index].extend(m_file, File::access_ReadOnly, section_size);
            ok = randomly_false_in_debug(ok);
            if (!ok) {
                requires_new_translation = true;
                size_t section_reservation = get_section_base(old_num_sections) - section_start_offset;
                // save the old mapping/keep it open
                OldMapping oldie(m_youngest_live_version, m_mappings[mapping_index]);
                m_old_mappings.emplace_back(std::move(oldie));
                m_mappings[mapping_index].reserve(m_file, File::access_ReadOnly, section_start_offset,
                                                  section_reservation);
                ok = m_mappings[mapping_index].extend(m_file, File::access_ReadOnly, section_size);
                m_mapping_version++;
            }
            REALM_ASSERT(ok);
        }
        else { // extension stretches over multiple sections:

            // 1. figure out if there is a partially completed mapping, that we need to extend
            // to cover a full mapping section
            if (old_baseline < old_slab_base) {
                size_t section_start_offset = get_section_base(old_num_sections - 1);
                size_t section_size = old_slab_base - section_start_offset;
                auto ok = m_mappings[mapping_index].extend(m_file, File::access_ReadOnly, section_size);
                ok = randomly_false_in_debug(ok);
                if (!ok) {
                    // we could not extend the old mapping, so replace it with a full, new one
                    requires_new_translation = true;
                    size_t section_reservation = get_section_base(old_num_sections) - section_start_offset;
                    REALM_ASSERT(section_size == section_reservation);
                    // save the old mapping/keep it open
                    OldMapping oldie(m_youngest_live_version, m_mappings[mapping_index]);
                    m_old_mappings.emplace_back(std::move(oldie));
                    m_mappings[mapping_index] =
                        util::File::Map<char>(m_file, section_start_offset, File::access_ReadOnly, section_size);
                    m_mapping_version++;
                }
            }

            // 2. add any full mappings
            //  - figure out how many full mappings we need to match the requested size
            auto new_slab_base = align_size_to_section_boundary(file_size);
            size_t num_full_mappings = get_section_index(file_size) - m_sections_in_compatibility_mapping;
            size_t num_mappings = get_section_index(new_slab_base) - m_sections_in_compatibility_mapping;
            size_t old_num_mappings = old_num_sections - m_sections_in_compatibility_mapping;
            if (num_mappings > old_num_mappings) {
                // we can't just resize the vector since Maps do not support copy constructionn:
                // m_mappings.resize(num_mappings);
                std::vector<util::File::Map<char>> next_mapping(num_mappings);
                for (size_t i = 0; i < old_num_mappings; ++i) {
                    next_mapping[i] = std::move(m_mappings[i]);
                }
                m_mappings = std::move(next_mapping);
            }

            for (size_t k = old_num_mappings; k < num_full_mappings; ++k) {
                size_t section_start_offset = get_section_base(k + m_sections_in_compatibility_mapping);
                size_t section_size =
                    get_section_base(1 + k + m_sections_in_compatibility_mapping) - section_start_offset;
                m_mappings[k] =
                    util::File::Map<char>(m_file, section_start_offset, File::access_ReadOnly, section_size);
            }

            // 3. add a final partial mapping if needed
            if (file_size < new_slab_base) {
                REALM_ASSERT(num_mappings == num_full_mappings + 1);
                size_t section_start_offset =
                    get_section_base(num_full_mappings + m_sections_in_compatibility_mapping);
                size_t section_reservation =
                    get_section_base(num_full_mappings + 1 + m_sections_in_compatibility_mapping) -
                    section_start_offset;
                size_t section_size = file_size - section_start_offset;
                util::File::Map<char> mapping;
                mapping.reserve(m_file, File::access_ReadOnly, section_start_offset, section_reservation);
                auto ok = mapping.extend(m_file, File::access_ReadOnly, section_size);
                REALM_ASSERT(ok); // should allways succeed, as this is the first extend()
                m_mappings[num_full_mappings] = std::move(mapping);
            }
        }
    }
    // Rebase slabs and free list (assumes exactly one entry in m_free_space for
    // each entire slab in m_slabs)
    size_t slab_ref = align_size_to_section_boundary(file_size);
    size_t n = m_free_space.size();
    REALM_ASSERT(m_slabs.size() == n);
    for (size_t i = 0; i < n; ++i) {
        Chunk& free_chunk = m_free_space[i];
        free_chunk.ref = slab_ref;
        ref_type slab_ref_end = slab_ref + free_chunk.size;
        m_slabs[i].ref_end = slab_ref_end;
        slab_ref = slab_ref_end;
    }

    // Build the fast path mapping

    // The fast path mapping is an array which will is used from multiple threads
    // without locking - see translate().

    // Addition of a new mapping may require a completely new fast mapping table.
    //
    // Being used in a multithreaded scenario, the old mappings must be retained open,
    // until the realm version for which they were established has been closed/detached.
    //
    // This assumes that only write transactions call do_alloc() or do_free() or needs to
    // translate refs in the slab area, and that all these uses are serialized, whether
    // that is achieved by being single threaded, interlocked or run from a sequential
    // scheduling queue.
    //
    rebuild_translations(requires_new_translation, old_num_sections);
}

void SlabAlloc::extend_fast_mapping_with_slab(char* address)
{
    ++m_translation_table_size;
    auto new_fast_mapping = new RefTranslation[m_translation_table_size];
    for (size_t i = 0; i < m_translation_table_size - 1; ++i) {
        new_fast_mapping[i] = m_ref_translation_ptr[i];
    }
    m_old_translations.emplace_back(m_youngest_live_version, m_ref_translation_ptr.load());
#if REALM_ENABLE_ENCRYPTION
    new_fast_mapping[m_translation_table_size - 1] = {address, nullptr};
#else
    new_fast_mapping[m_translation_table_size - 1] = {address};
#endif
    m_ref_translation_ptr = new_fast_mapping;
}

void SlabAlloc::rebuild_translations(bool requires_new_translation, size_t old_num_sections)
{
    size_t free_space_size = m_free_space.size();
    auto num_mappings = m_mappings.size();
    if (m_translation_table_size < num_mappings + free_space_size + m_sections_in_compatibility_mapping) {
        requires_new_translation = true;
    }
    RefTranslation* new_translation_table = m_ref_translation_ptr;
    if (requires_new_translation) {
        // we need a new translation table, but must preserve old, as translations using it
        // may be in progress concurrently
        m_old_translations.emplace_back(m_youngest_live_version, m_ref_translation_ptr.load());
        m_translation_table_size = num_mappings + free_space_size + m_sections_in_compatibility_mapping;
        new_translation_table = new RefTranslation[m_translation_table_size];
        for (int i = 0; i < m_sections_in_compatibility_mapping; ++i) {
            new_translation_table[i].mapping_addr = m_compatibility_mapping.get_addr() + get_section_base(i);
#if REALM_ENABLE_ENCRYPTION
            new_translation_table[i].encrypted_mapping = m_compatibility_mapping.get_encrypted_mapping();
#endif
        }
        old_num_sections = 0;
    }
    for (size_t k = old_num_sections; k < num_mappings; ++k) {
        auto i = k + m_sections_in_compatibility_mapping;
        new_translation_table[i].mapping_addr = m_mappings[k].get_addr();
#if REALM_ENABLE_ENCRYPTION
        new_translation_table[i].encrypted_mapping = m_mappings[k].get_encrypted_mapping();
#endif
    }
    for (size_t k = 0; k < free_space_size; ++k) {
        char* base = m_slabs[k].addr;
        auto i = num_mappings + m_sections_in_compatibility_mapping + k;
#if REALM_ENABLE_ENCRYPTION
        new_translation_table[i] = {base, nullptr};
#else
        new_translation_table[i] = {base};
#endif
    }
    m_ref_translation_ptr = new_translation_table;
}

void SlabAlloc::purge_old_mappings(uint64_t oldest_live_version, uint64_t youngest_live_version)
{
    std::lock_guard<std::mutex> lock(m_mapping_mutex);
    for (size_t i = 0; i < m_old_mappings.size();) {
        if (m_old_mappings[i].replaced_at_version >= oldest_live_version) {
            ++i;
            continue;
        }
        // move last over:
        auto oldie = std::move(m_old_mappings[i]);
        m_old_mappings[i] = std::move(m_old_mappings.back());
        m_old_mappings.pop_back();
        oldie.mapping.unmap();
    }

    for (size_t i = 0; i < m_old_translations.size();) {
        if (m_old_translations[i].replaced_at_version >= oldest_live_version) {
            ++i;
            continue;
        }
        // move last over:
        auto oldie = std::move(m_old_translations[i]);
        m_old_translations[i] = std::move(m_old_translations.back());
        m_old_translations.pop_back();
        delete[] oldie.translations;
    }
    m_youngest_live_version = youngest_live_version;
}


const SlabAlloc::chunks& SlabAlloc::get_free_read_only() const
{
    if (REALM_COVER_NEVER(m_free_space_state == free_space_Invalid))
        throw InvalidFreeSpace();
    return m_free_read_only;
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
    REALM_ASSERT(new_file_size == round_up_to_page_size(new_file_size));
    m_file.prealloc(new_file_size); // Throws

    bool disable_sync = get_disable_sync_to_disk();
    if (!disable_sync)
        m_file.sync(); // Throws
}

#ifdef REALM_DEBUG
void SlabAlloc::reserve_disk_space(size_t size)
{
    if (size != round_up_to_page_size(size))
        size = round_up_to_page_size(size);
    m_file.prealloc(size); // Throws

    bool disable_sync = get_disable_sync_to_disk();
    if (!disable_sync)
        m_file.sync(); // Throws
}
#endif

void SlabAlloc::verify() const
{
#ifdef REALM_DEBUG
    // Make sure that all free blocks fit within a slab
    for (const auto& chunk : m_free_space) {
        slabs::const_iterator slab =
            upper_bound(m_slabs.begin(), m_slabs.end(), chunk.ref, &ref_less_than_slab_ref_end);
        REALM_ASSERT(slab != m_slabs.end());

        ref_type slab_ref_end = slab->ref_end;
        ref_type chunk_ref_end = chunk.ref + chunk.size;
        REALM_ASSERT_3(chunk_ref_end, <=, slab_ref_end);
    }
#endif
}

#ifdef REALM_DEBUG

bool SlabAlloc::is_all_free() const
{
    if (m_free_space.size() != m_slabs.size())
        return false;

    // Verify that free space matches slabs
    ref_type slab_ref = align_size_to_section_boundary(m_baseline.load(std::memory_order_relaxed));
    for (const auto& slab : m_slabs) {
        size_t slab_size = slab.ref_end - slab_ref;
        chunks::const_iterator chunk = find_if(m_free_space.begin(), m_free_space.end(), ChunkRefEq(slab_ref));
        if (chunk == m_free_space.end())
            return false;
        if (slab_size != chunk->size)
            return false;
        slab_ref = slab.ref_end;
    }
    return true;
}


// LCOV_EXCL_START
void SlabAlloc::print() const
{
    size_t allocated_for_slabs = m_slabs.empty() ? 0 : m_slabs.back().ref_end - m_baseline;

    size_t free = 0;
    for (const auto& free_block : m_free_space) {
        free += free_block.size;
    }

    size_t allocated = allocated_for_slabs - free;
    std::cout << "Attached: " << (m_data ? size_t(m_baseline) : 0) << " Allocated: " << allocated << "\n";

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
}
// LCOV_EXCL_STOP

#endif // REALM_DEBUG
