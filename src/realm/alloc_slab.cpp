#include <exception>
#include <algorithm>
#include <iostream>

#ifdef REALM_SLAB_ALLOC_DEBUG
#  include <cstdlib>
#  include <map>
#endif

#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/terminate.hpp>
#include <memory>
#include <realm/array.hpp>
#include <realm/alloc_slab.hpp>

using namespace realm;
using namespace realm::util;


namespace {

#ifdef REALM_SLAB_ALLOC_DEBUG
std::map<ref_type, void*> malloc_debug_map;
#endif

class InvalidFreeSpace: std::exception {
public:
    const char* what() const REALM_NOEXCEPT_OR_NOTHROW override
    {
        return "Free space tracking was lost due to out-of-memory";
    }
};

} // anonymous namespace

SlabAlloc::SlabAlloc()
{
    m_initial_section_size = page_size();
    m_section_shifts = log2(m_initial_section_size);
    size_t max = std::numeric_limits<size_t>::max();
    m_num_section_bases = 1 + get_section_index(max);
    m_section_bases.reset( new size_t[m_num_section_bases] );
    for (int i = 0; i < m_num_section_bases; ++i) {
        m_section_bases[i] = compute_section_base(i);
    }
}

const SlabAlloc::Header SlabAlloc::empty_file_header = {
    { 0, 0 }, // top-refs
    { 'T', '-', 'D', 'B' },
    { library_file_format, library_file_format },
    0, // reserved
    0  // select bit
};

const SlabAlloc::Header SlabAlloc::streaming_header = {
    { 0xFFFFFFFFFFFFFFFFULL, 0 }, // top-refs
    { 'T', '-', 'D', 'B' },
    { library_file_format, library_file_format },
    0, // reserved
    0  // select bit
};


class SlabAlloc::ChunkRefEq {
public:
    ChunkRefEq(ref_type ref) REALM_NOEXCEPT:
        m_ref(ref)
    {
    }
    bool operator()(const Chunk& chunk) const REALM_NOEXCEPT
    {
        return chunk.ref == m_ref;
    }
private:
    ref_type m_ref;
};


class SlabAlloc::ChunkRefEndEq {
public:
    ChunkRefEndEq(ref_type ref) REALM_NOEXCEPT:
        m_ref(ref)
    {
    }
    bool operator()(const Chunk& chunk) const REALM_NOEXCEPT
    {
        return chunk.ref + chunk.size == m_ref;
    }
private:
    ref_type m_ref;
};


class SlabAlloc::SlabRefEndEq {
public:
    SlabRefEndEq(ref_type ref) REALM_NOEXCEPT:
        m_ref(ref)
    {
    }
    bool operator()(const Slab& slab) const REALM_NOEXCEPT
    {
        return slab.ref_end == m_ref;
    }
private:
    ref_type m_ref;
};


void SlabAlloc::detach() REALM_NOEXCEPT
{
    switch (m_attach_mode) {
        case attach_None:
        case attach_UsersBuffer:
            goto found;
        case attach_OwnedBuffer:
            ::free(m_data);
            goto found;
        case attach_SharedFile:
        case attach_UnsharedFile:
            File::unmap(m_data, m_initial_mapping_size);
            if (m_additional_mappings) {
                // running the destructors on the mappings will cause them to unmap:
                delete[] m_additional_mappings;
                m_additional_mappings = 0;
            }
            m_file.close();
            goto found;
    }
    REALM_ASSERT(false);
  found:
    m_attach_mode = attach_None;
}


SlabAlloc::~SlabAlloc() REALM_NOEXCEPT
{
#ifdef REALM_DEBUG
    if (is_attached()) {
        // A shared group does not guarantee that all space is free
        if (m_attach_mode != attach_SharedFile) {
            // No point inchecking if free space info is invalid
            if (m_free_space_state != free_space_Invalid) {
                if (!is_all_free()) {
                    print();
#  ifndef REALM_SLAB_ALLOC_DEBUG
                    std::cerr << "To get the stack-traces of the corresponding allocations,"
                        "first compile with REALM_SLAB_ALLOC_DEBUG defined,"
                        "then run under Valgrind with --leak-check=full\n";
                    REALM_TERMINATE("SlabAlloc detected a leak");
#  endif
                }
            }
        }
    }
#endif

    // Release all allocated memory
    for (size_t i = 0; i < m_slabs.size(); ++i)
        delete[] m_slabs[i].addr;

    if (is_attached())
        detach();
}


MemRef SlabAlloc::do_alloc(size_t size)
{
    REALM_ASSERT_DEBUG(0 < size);
    REALM_ASSERT_DEBUG((size & 0x7) == 0); // only allow sizes that are multiples of 8
    REALM_ASSERT_DEBUG(is_attached());

    // If we failed to correctly record free space, new allocations cannot be
    // carried out until the free space record is reset.
    if (m_free_space_state == free_space_Invalid)
        throw InvalidFreeSpace();
    m_free_space_state = free_space_Dirty;

    // Do we have a free space we can reuse?
    {
        typedef chunks::reverse_iterator iter;
        iter rend = m_free_space.rend();
        for (iter i = m_free_space.rbegin(); i != rend; ++i) {
            if (size <= i->size) {
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
                if (m_debug_out)
                    std::cerr << "Alloc ref: " << ref << " size: " << size << "\n";
#endif

                char* addr = translate(ref);
#ifdef REALM_ENABLE_ALLOC_SET_ZERO
                std::fill(addr, addr+size, 0);
#endif
#ifdef REALM_SLAB_ALLOC_DEBUG
                malloc_debug_map[ref] = malloc(1);
#endif
                return MemRef(addr, ref);
            }
        }
    }

    // Else, allocate new slab
    size_t new_size = ((size-1) | 255) + 1; // Round up to nearest multiple of 256
    ref_type ref;
    if (m_slabs.empty()) {
        ref = m_baseline;
    }
    else {
        ref_type curr_ref_end = to_size_t(m_slabs.back().ref_end);
        // Make it at least as big as twice the previous slab
        ref_type prev_ref_end = m_slabs.size() == 1 ? m_baseline :
            to_size_t(m_slabs[m_slabs.size()-2].ref_end);
        size_t min_size = 2 * (curr_ref_end - prev_ref_end);
        if (new_size < min_size)
            new_size = min_size;
        ref = curr_ref_end;
    }
    REALM_ASSERT_DEBUG(0 < new_size);
    std::unique_ptr<char[]> mem(new char[new_size]); // Throws
    std::fill(mem.get(), mem.get()+new_size, 0);

    // Add to list of slabs
    Slab slab;
    slab.addr = mem.get();
    slab.ref_end = ref + new_size;
    m_slabs.push_back(slab); // Throws
    mem.release();

    // Update free list
    size_t unused = new_size - size;
    if (0 < unused) {
        Chunk chunk;
        chunk.ref = ref + size;
        chunk.size = unused;
        m_free_space.push_back(chunk); // Throws
    }

#ifdef REALM_DEBUG
    if (m_debug_out)
        std::cerr << "Alloc ref: " << ref << " size: " << size << "\n";
#endif

#ifdef REALM_ENABLE_ALLOC_SET_ZERO
    std::fill(slab.addr, slab.addr+size, 0);
#endif
#ifdef REALM_SLAB_ALLOC_DEBUG
    malloc_debug_map[ref] = malloc(1);
#endif

    return MemRef(slab.addr, ref);
}


void SlabAlloc::do_free(ref_type ref, const char* addr) REALM_NOEXCEPT
{
    REALM_ASSERT_3(translate(ref), ==, addr);

    // Free space in read only segment is tracked separately
    bool read_only = is_read_only(ref);
    chunks& free_space = read_only ? m_free_read_only : m_free_space;

#ifdef REALM_SLAB_ALLOC_DEBUG
    free(malloc_debug_map[ref]);
#endif

    // Get size from segment
    size_t size = read_only ? Array::get_byte_size_from_header(addr) :
        Array::get_capacity_from_header(addr);
    ref_type ref_end = ref + size;

#ifdef REALM_DEBUG
    if (m_debug_out)
        std::cerr << "Free ref: " << ref << " size: " << size << "\n";
#endif

    if (m_free_space_state == free_space_Invalid)
        return;

    // Mutable memory cannot be freed unless it has first been allocated, and
    // any allocation puts free space tracking into the "dirty" state.
    REALM_ASSERT_3(read_only, ||, m_free_space_state == free_space_Dirty);

    m_free_space_state = free_space_Dirty;

    // Check if we can merge with adjacent succeeding free block
    typedef chunks::iterator iter;
    iter merged_with = free_space.end();
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
        typedef chunks::iterator iter;
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

    // Else just add to freelist
    if (merged_with == free_space.end()) {
        try {
            Chunk chunk;
            chunk.ref  = ref;
            chunk.size = size;
            free_space.push_back(chunk); // Throws
        }
        catch (...) {
            m_free_space_state = free_space_Invalid;
        }
    }
}


MemRef SlabAlloc::do_realloc(size_t ref, const char* addr, size_t old_size, size_t new_size)
{
    REALM_ASSERT_DEBUG(translate(ref) == addr);
    REALM_ASSERT_DEBUG(0 < new_size);
    REALM_ASSERT_DEBUG((new_size & 0x7) == 0); // only allow sizes that are multiples of 8

    // FIXME: Check if we can extend current space. In that case, remember to
    // check whether m_free_space_state == free_state_Invalid. Also remember to
    // fill with zero if REALM_ENABLE_ALLOC_SET_ZERO is defined.

    // Allocate new space
    MemRef new_mem = do_alloc(new_size); // Throws

    // Copy existing segment
    char* new_addr = new_mem.m_addr;
    std::copy(addr, addr+old_size, new_addr);

    // Add old segment to freelist
    do_free(ref, addr);

#ifdef REALM_DEBUG
    if (m_debug_out) {
        std::cerr << "Realloc orig_ref: " << ref << " old_size: " << old_size << " "
            "new_ref: " << new_mem.m_ref << " new_size: " << new_size << "\n";
    }
#endif // REALM_DEBUG

    return new_mem;
}

char* SlabAlloc::do_translate(ref_type ref) const REALM_NOEXCEPT
{
    REALM_ASSERT_DEBUG(is_attached());

    // fast path if reference is inside the initial mapping:
    if (ref < m_initial_mapping_size)
        return m_data + ref;

    if (ref < m_baseline) {

        // reference must be inside a section mapped later
        size_t section_index = get_section_index(ref);
        size_t mapping_index = section_index - m_first_additional_mapping;
        size_t section_offset = ref - get_section_base(section_index);
        REALM_ASSERT_DEBUG(m_additional_mappings);
        REALM_ASSERT_DEBUG(mapping_index < m_num_additional_mappings);
        return m_additional_mappings[mapping_index].get_addr() + section_offset;
    }

    typedef slabs::const_iterator iter;
    iter i = upper_bound(m_slabs.begin(), m_slabs.end(), ref, &ref_less_than_slab_ref_end);
    REALM_ASSERT_DEBUG(i != m_slabs.end());

    ref_type slab_ref = i == m_slabs.begin() ? m_baseline : (i-1)->ref_end;
    return i->addr + (ref - slab_ref);
}

int SlabAlloc::get_committed_file_format() const REALM_NOEXCEPT
{
    Header* header = reinterpret_cast<Header*>(m_data);
    int select_field = header->m_flags & SlabAlloc::flags_SelectBit;
    int file_format = header->m_file_format[select_field];
    return file_format;
}

ref_type SlabAlloc::attach_file(const std::string& path, Config& cfg)
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

    using namespace realm::util;
    File::AccessMode access = cfg.read_only ? File::access_ReadOnly : File::access_ReadWrite;
    File::CreateMode create = cfg.read_only || cfg.no_create ? File::create_Never : File::create_Auto;
    m_file.open(path.c_str(), access, create, 0); // Throws
    if (cfg.encryption_key)
        m_file.set_encryption_key(cfg.encryption_key);
    File::CloseGuard fcg(m_file);

    size_t initial_size = m_initial_section_size;

    size_t initial_size_of_file;
    ref_type top_ref = 0;

    // The size of a database file must not exceed what can be encoded in
    // size_t.
    size_t size;
    bool did_create = false;
    if (REALM_UNLIKELY(int_cast_with_overflow_detect(m_file.get_size(), size)))
        throw InvalidDatabase("Realm file too large", path);

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
    if (size == 0) {
        did_create = true;
        if (REALM_UNLIKELY(cfg.read_only))
            throw InvalidDatabase("Read-only access to empty Realm file", path);

        const char* data = reinterpret_cast<const char*>(&empty_file_header);
        m_file.write(data, sizeof empty_file_header); // Throws

        // Pre-alloc initial space
        m_file.prealloc(0, initial_size); // Throws
        bool disable_sync = get_disable_sync_to_disk();
        if (!disable_sync)
            m_file.sync(); // Throws
        size = initial_size;
    }

    // We must now make sure the filesize matches a page boundary...
    // first, save the original filesize for use during validation and
    // (potentially) conversion from streaming format.
    initial_size_of_file = size;

    // next extend the file to a mmapping boundary (unless already there)
    // The file must be extended prior to being mmapped, as extending it after mmap has
    // undefined behavior.
    // The mapping of the first part of the file *must* be contiguous, because
    // we do not know if the file was created by a version of the code, that took
    // the section boundaries into account. If it wasn't we cannot map it in sections 
    // without risking datastructures that cross a mapping boundary.
    // If the file is opened read-only, we cannot extend it. This is not a problem,
    // because for a read-only file we assume that it will not change while we use it.
    // This assumption obviously will not hold, if the file is shared by multiple
    // processes with different opening modes.
    if (!cfg.read_only && !matches_section_boundary(size)) {

        REALM_ASSERT(cfg.session_initiator || !cfg.is_shared);
        size = get_upper_section_boundary(size);
        m_file.prealloc(0, size);
        // resizing the file (as we do here) without actually changing any internal
        // datastructures to reflect the additional free space will work, because the
        // free space management relies on the logical filesize and disregards the
        // actual size of the file.
    }

    try {
        File::Map<char> map(m_file, File::access_ReadOnly, size); // Throws

        m_file_on_streaming_form = false; // May be updated by validate_buffer()
        if (!cfg.skip_validate) {
            // Verify the data structures
            validate_buffer(map.get_addr(), initial_size_of_file, path, top_ref, cfg.is_shared); // Throws
        }

        if (did_create) {
            File::Map<Header> writable_map(m_file, File::access_ReadWrite, sizeof (Header)); // Throws
            Header* header = writable_map.get_addr();
            header->m_flags |= cfg.server_sync_mode ? flags_ServerSyncMode : 0x0;
        }
        else {
            const Header* header = reinterpret_cast<const Header*>(map.get_addr());
            bool stored_server_sync_mode = (header->m_flags & flags_ServerSyncMode) != 0;
            if (cfg.server_sync_mode &&  !stored_server_sync_mode)
                throw InvalidDatabase("Specified Realm file was not created with support for "
                                      "client/server synchronization", path);
            if (!cfg.server_sync_mode &&  stored_server_sync_mode)
                throw InvalidDatabase("Specified Realm file requires support for client/server "
                                      "synchronization", path);
        }

        {
            const Header* header = reinterpret_cast<const Header*>(map.get_addr());
            int select_field = ((header->m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);
            m_file_format = header->m_file_format[select_field];
        }

        m_data        = map.release();
        m_baseline    = size;
        m_initial_mapping_size = size;
        m_first_additional_mapping = get_section_index(m_initial_mapping_size);
        m_attach_mode = cfg.is_shared ? attach_SharedFile : attach_UnsharedFile;

        // Below this point (assignment to `m_attach_mode`), nothing must throw.
    }
    catch (DecryptionFailed) {
        throw InvalidDatabase("Realm file decryption failed", path);
    }

    // make sure that any call to begin_read cause any slab to be placed in free lists correctly
    m_free_space_state = free_space_Invalid;

    // make sure the database is not on streaming format. This has to be done at
    // session initialization, even if it means writing the database during open.
    if (cfg.session_initiator && m_file_on_streaming_form) {

        Header* header = reinterpret_cast<Header*>(m_data);

        // Don't compare file format version fields as they are allowed to differ. 
        // Also don't compare reserved fields (todo, is it correct to ignore?)
        REALM_ASSERT_3(header->m_flags, == , streaming_header.m_flags);
        REALM_ASSERT_3(header->m_mnemonic[0], == , streaming_header.m_mnemonic[0]);
        REALM_ASSERT_3(header->m_mnemonic[1], == , streaming_header.m_mnemonic[1]);
        REALM_ASSERT_3(header->m_mnemonic[2], == , streaming_header.m_mnemonic[2]);
        REALM_ASSERT_3(header->m_mnemonic[3], == , streaming_header.m_mnemonic[3]);
        REALM_ASSERT_3(header->m_top_ref[0], == , streaming_header.m_top_ref[0]);
        REALM_ASSERT_3(header->m_top_ref[1], == , streaming_header.m_top_ref[1]);

        StreamingFooter* footer = reinterpret_cast<StreamingFooter*>(m_data+initial_size_of_file) - 1;
        REALM_ASSERT_3(footer->m_magic_cookie, ==, footer_magic_cookie);
        {
            File::Map<Header> writable_map(m_file, File::access_ReadWrite, sizeof (Header)); // Throws
            Header* writable_header = writable_map.get_addr();
            writable_header->m_top_ref[1] = footer->m_top_ref;
            writable_map.sync();
            writable_header->m_flags |= flags_SelectBit; // keep bit 1 used for server sync mode unchanged
            m_file_on_streaming_form = false;
            writable_map.sync();
        }
    }

    fcg.release(); // Do not close
    return top_ref;
}

ref_type SlabAlloc::attach_buffer(char* data, size_t size)
{
    // ExceptionSafety: If this function throws, it must leave the allocator in
    // the detached state.

    REALM_ASSERT(!is_attached());

    // Verify the data structures
    m_file_on_streaming_form = false; // May be updated by validate_buffer()
    std::string path; // No path
    ref_type top_ref;
    bool is_shared = false;
    validate_buffer(data, size, path, top_ref, is_shared); // Throws

    {
        const Header* header = reinterpret_cast<const Header*>(data);
        int select_field = ((header->m_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);
        m_file_format = header->m_file_format[select_field];
    }

    m_data        = data;
    m_baseline    = size;
    m_initial_mapping_size = size;
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
    m_baseline = sizeof (Header);
    m_initial_mapping_size = m_baseline;
}

void SlabAlloc::validate_buffer(const char* data, size_t size, const std::string& path,
                                ref_type& top_ref, bool is_shared)
{
    // Verify that size is sane and 8-byte aligned
    if (REALM_UNLIKELY(size < sizeof (Header) || size % 8 != 0))
        throw InvalidDatabase("Realm file has bad size", path);

    // File header is 24 bytes, composed of three 64-bit
    // blocks. The two first being top_refs (only one valid
    // at a time) and the last being the info block.
    const char* file_header = data;

    // First four bytes of info block is file format id
    if (REALM_UNLIKELY(!(file_header[16] == 'T' &&
                         file_header[17] == '-' &&
                         file_header[18] == 'D' &&
                         file_header[19] == 'B')))
        throw InvalidDatabase("Not a Realm file", path);

    // Last bit in info block indicates which top_ref block is valid
    int valid_part = file_header[16 + 7] & 0x1;

    // Byte 4 and 5 (depending on valid_part) in the info block is version
    int file_format = static_cast<unsigned char>(file_header[16 + 4 + valid_part]);
    bool bad_file_format = (file_format != library_file_format);

    // As a special case, allow upgrading from version 2 to 3, but only when
    // accessed through SharedGroup.
    if (file_format == 2 && library_file_format == 3 && is_shared)
        bad_file_format = false;

    if (REALM_UNLIKELY(bad_file_format))
        throw InvalidDatabase("Unsupported Realm file format version", path);

    // Top_ref should always point within buffer
    const uint64_t* top_refs = reinterpret_cast<const uint64_t*>(data);
    uint_fast64_t ref = top_refs[valid_part];
    if (valid_part == 0 && ref == 0xFFFFFFFFFFFFFFFFULL) {
        if (REALM_UNLIKELY(size < sizeof (Header) + sizeof (StreamingFooter)))
            throw InvalidDatabase("Realm file in streaming form has bad size", path);
        const StreamingFooter* footer = reinterpret_cast<const StreamingFooter*>(data+size) - 1;
        ref = footer->m_top_ref;
        if (REALM_UNLIKELY(footer->m_magic_cookie != footer_magic_cookie))
            throw InvalidDatabase("Bad Realm file header (#1)", path);
        m_file_on_streaming_form = true;
    }
    if (REALM_UNLIKELY(ref % 8 != 0))
        throw InvalidDatabase("Bad Realm file header (#2)", path);
    if (REALM_UNLIKELY(ref >= size))
        throw InvalidDatabase("Bad Realm file header (#3)", path);

    top_ref = ref_type(ref);
}


size_t SlabAlloc::get_total_size() const REALM_NOEXCEPT
{
    return m_slabs.empty() ? m_baseline : m_slabs.back().ref_end;
}


void SlabAlloc::reset_free_space_tracking()
{
    if (m_free_space_state == free_space_Clean)
        return;

    // Free all scratch space (done after all data has
    // been commited to persistent space)
    m_free_read_only.clear();
    m_free_space.clear();

    // Rebuild free list to include all slabs
    Chunk chunk;
    chunk.ref = m_baseline;
    typedef slabs::const_iterator iter;
    iter end = m_slabs.end();
    for (iter i = m_slabs.begin(); i != end; ++i) {
        chunk.size = i->ref_end - chunk.ref;
        m_free_space.push_back(chunk); // Throws
        chunk.ref = i->ref_end;
    }

    REALM_ASSERT_DEBUG(is_all_free());

    m_free_space_state = free_space_Clean;
}


void SlabAlloc::remap(size_t file_size)
{
    REALM_ASSERT_DEBUG(file_size % 8 == 0); // 8-byte alignment required
    REALM_ASSERT_DEBUG(m_attach_mode == attach_SharedFile || m_attach_mode == attach_UnsharedFile);
    REALM_ASSERT_DEBUG(m_free_space_state == free_space_Clean);
    REALM_ASSERT_DEBUG(m_baseline <= file_size);

    // Extend mapping by adding sections
    REALM_ASSERT_DEBUG(matches_section_boundary(file_size));
    m_baseline = file_size;
    auto num_sections = get_section_index(file_size);
    auto num_additional_mappings = num_sections - m_first_additional_mapping;

    if (num_additional_mappings > m_capacity_additional_mappings) {
        // FIXME: No harcoded constants here
        m_capacity_additional_mappings = num_additional_mappings + 128;
        std::unique_ptr<util::File::Map<char>[]> new_mappings;
        new_mappings.reset(new util::File::Map<char>[m_capacity_additional_mappings]);
        for (size_t j = 0; j < m_num_additional_mappings; ++j)
            new_mappings[j] = std::move(m_additional_mappings[j]);
        delete[] m_additional_mappings;
        m_additional_mappings = new_mappings.release();
    }
    for (auto k = m_num_additional_mappings; k < num_additional_mappings; ++k)
    {
        auto section_start_offset = get_section_base(k + m_first_additional_mapping);
        auto section_size = get_section_base(1 + k + m_first_additional_mapping) - section_start_offset;
        util::File::Map<char> map(m_file, section_start_offset, File::access_ReadOnly, section_size);
        m_additional_mappings[k] = std::move(map);
    }
    m_num_additional_mappings = num_additional_mappings;

    // Rebase slabs and free list (assumes exactly one entry in m_free_space for
    // each entire slab in m_slabs)
    size_t slab_ref = file_size;
    size_t n = m_free_space.size();
    REALM_ASSERT_DEBUG(m_slabs.size() == n);
    for (size_t i = 0; i < n; ++i) {
        Chunk& free_chunk = m_free_space[i];
        free_chunk.ref = slab_ref;
        ref_type slab_ref_end = slab_ref + free_chunk.size;
        m_slabs[i].ref_end = slab_ref_end;
        slab_ref = slab_ref_end;
    }
}

const SlabAlloc::chunks& SlabAlloc::get_free_read_only() const
{
    if (m_free_space_state == free_space_Invalid)
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

size_t SlabAlloc::get_section_index(size_t pos) const REALM_NOEXCEPT
{
    // size_t section_base_number = pos/m_initial_section_size;
    size_t section_base_number = pos >> m_section_shifts;
    size_t section_group_number = section_base_number/16;
    size_t index;
    if (section_group_number == 0) {
        // first 16 entries aligns 1:1
        index = section_base_number;
    }
    else {
        // remaning entries are exponential
        size_t log_index = log2(section_group_number);
        size_t section_index_in_group = (section_base_number >> (1+log_index)) & 0x7;
        index = (16 + (log_index * 8)) + section_index_in_group;
    }
    return index;
}

size_t SlabAlloc::compute_section_base(size_t index) const REALM_NOEXCEPT
{
    size_t base;
    if (index < 16) {
        // base = index * m_initial_section_size;
        base = index << m_section_shifts;
    }
    else {
        size_t section_index_in_group = index & 7;
        size_t log_index = (index - section_index_in_group)/8 - 2;
        size_t section_base_number = (8 + section_index_in_group)<<(1+log_index);
        // base = m_initial_section_size * section_base_number;
        base = section_base_number << m_section_shifts;
    }
    return base;
}

size_t SlabAlloc::find_section_in_range(size_t start_pos, 
                                             size_t free_chunk_size,
                                             size_t request_size) const REALM_NOEXCEPT
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



#ifdef REALM_DEBUG

bool SlabAlloc::is_all_free() const
{
    if (m_free_space.size() != m_slabs.size())
        return false;

    // Verify that free space matches slabs
    ref_type slab_ref = m_baseline;
    typedef slabs::const_iterator iter;
    iter end = m_slabs.end();
    for (iter slab = m_slabs.begin(); slab != end; ++slab) {
        size_t slab_size = slab->ref_end - slab_ref;
        chunks::const_iterator chunk =
            find_if(m_free_space.begin(), m_free_space.end(), ChunkRefEq(slab_ref));
        if (chunk == m_free_space.end())
            return false;
        if (slab_size != chunk->size)
            return false;
        slab_ref = slab->ref_end;
    }
    return true;
}


void SlabAlloc::verify() const
{
    // Make sure that all free blocks fit within a slab
    typedef chunks::const_iterator iter;
    iter end = m_free_space.end();
    for (iter chunk = m_free_space.begin(); chunk != end; ++chunk) {
        slabs::const_iterator slab =
            upper_bound(m_slabs.begin(), m_slabs.end(), chunk->ref, &ref_less_than_slab_ref_end);
        REALM_ASSERT(slab != m_slabs.end());

        ref_type slab_ref_end = slab->ref_end;
        ref_type chunk_ref_end = chunk->ref + chunk->size;
        REALM_ASSERT_3(chunk_ref_end, <=, slab_ref_end);
    }
}


void SlabAlloc::print() const
{
    size_t allocated_for_slabs = m_slabs.empty() ? 0 : m_slabs.back().ref_end - m_baseline;

    size_t free = 0;
    for (size_t i = 0; i < m_free_space.size(); ++i)
        free += m_free_space[i].size;

    size_t allocated = allocated_for_slabs - free;
    std::cout << "Attached: " << (m_data ? m_baseline : 0) << " Allocated: " << allocated << "\n";

    if (!m_slabs.empty()) {
        std::cout << "Slabs: ";
        ref_type first_ref = m_baseline;
        typedef slabs::const_iterator iter;
        for (iter i = m_slabs.begin(); i != m_slabs.end(); ++i) {
            if (i != m_slabs.begin())
                std::cout << ", ";
            ref_type last_ref = i->ref_end - 1;
            size_t size = i->ref_end - first_ref;
            void* addr = i->addr;
            std::cout << "("<<first_ref<<"->"<<last_ref<<", size="<<size<<", addr="<<addr<<")";
            first_ref = i->ref_end;
        }
        std::cout << "\n";
    }
    if (!m_free_space.empty()) {
        std::cout << "FreeSpace: ";
        typedef chunks::const_iterator iter;
        for (iter i = m_free_space.begin(); i != m_free_space.end(); ++i) {
            if (i != m_free_space.begin())
                std::cout << ", ";
            ref_type last_ref = i->ref + i->size - 1;
            std::cout << "("<<i->ref<<"->"<<last_ref<<", size="<<i->size<<")";
        }
        std::cout << "\n";
    }
    if (!m_free_read_only.empty()) {
        std::cout << "FreeSpace (ro): ";
        typedef chunks::const_iterator iter;
        for (iter i = m_free_read_only.begin(); i != m_free_read_only.end(); ++i) {
            if (i != m_free_read_only.begin())
                std::cout << ", ";
            ref_type last_ref = i->ref + i->size - 1;
            std::cout << "("<<i->ref<<"->"<<last_ref<<", size="<<i->size<<")";
        }
        std::cout << "\n";
    }
    std::cout << std::flush;
}

#endif // REALM_DEBUG
