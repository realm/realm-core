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

#include <sys/mman.h>

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


const SlabAlloc::Header SlabAlloc::empty_file_header = {
    { 0, 0 }, // top-refs
    { 'T', '-', 'D', 'B' },
    { default_file_format_version, default_file_format_version },
    0, // reserved
    0  // select bit
};

const SlabAlloc::Header SlabAlloc::streaming_header = {
    { 0xFFFFFFFFFFFFFFFFULL, 0 }, // top-refs
    { 'T', '-', 'D', 'B' },
    { default_file_format_version, default_file_format_version },
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

    if (ref < m_baseline) {

        // reference is inside the initial mapping:
        if (ref < m_initial_mapping_size)
            return m_data + ref;

        // reference must be inside a chunk mapped later
        REALM_ASSERT_DEBUG(m_chunk_size);
        std::size_t chunk_index = get_chunk_index(ref);
        std::size_t mapping_index = chunk_index - m_first_additional_chunk;
        std::size_t chunk_offset = ref - get_lower_mmap_boundary(ref);
        REALM_ASSERT_DEBUG(m_additional_mappings);
        REALM_ASSERT_DEBUG(mapping_index < m_num_additional_mappings);
        return m_additional_mappings[mapping_index].get_addr() + chunk_offset;
    }

    typedef slabs::const_iterator iter;
    iter i = upper_bound(m_slabs.begin(), m_slabs.end(), ref, &ref_less_than_slab_ref_end);
    REALM_ASSERT_DEBUG(i != m_slabs.end());

    ref_type slab_ref = i == m_slabs.begin() ? m_baseline : (i-1)->ref_end;
    return i->addr + (ref - slab_ref);
}


ref_type SlabAlloc::attach_file(const std::string& path, bool is_shared, bool read_only,
                                bool no_create, bool skip_validate,
                                const char* encryption_key, bool server_sync_mode,
                                bool session_initiator, std::size_t chunk_size)
{
    REALM_ASSERT(!is_attached());
    m_chunk_size = chunk_size;

    // When 'read_only' is true, this function will throw InvalidDatabase if the
    // file exists already but is empty. This can happen if another process is
    // currently creating it. Note however, that it is only legal for multiple
    // processes to access a database file concurrently if it is done via a
    // SharedGroup, and in that case 'read_only' can never be true.
    REALM_ASSERT(!(is_shared && read_only));
    static_cast<void>(is_shared);

    using namespace realm::util;
    File::AccessMode access = read_only ? File::access_ReadOnly : File::access_ReadWrite;
    File::CreateMode create = read_only || no_create ? File::create_Never : File::create_Auto;
    m_file.open(path.c_str(), access, create, 0); // Throws
    if (encryption_key)
        m_file.set_encryption_key(encryption_key);
    File::CloseGuard fcg(m_file);

    size_t initial_size = 4 * 1024; // 4 KiB
    if (m_chunk_size)
        initial_size = m_chunk_size;

    std::size_t size_of_streaming_file;
    ref_type top_ref = 0;

    // The size of a database file must not exceed what can be encoded in
    // std::size_t.
    size_t size;
    bool did_create = false;
    if (int_cast_with_overflow_detect(m_file.get_size(), size))
        goto invalid_database;

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
        if (read_only)
            goto invalid_database;

        const char* data = reinterpret_cast<const char*>(&empty_file_header);
        m_file.write(data, sizeof empty_file_header); // Throws

        // Pre-alloc initial space
        m_file.prealloc(0, initial_size); // Throws
        size = initial_size;
    }

    // We must now make sure the filesize matches a page boundary...
    // first, save the original filesize for use later when converting from streaming format
    size_of_streaming_file = size;

    // next extend the file to a mmapping boundary (unless already there)
    // The file must be extended prior to being mmapped, as extending it after mmap has
    // undefined behavior.
    // The mapping of the first part of the file *must* be contiguous, because
    // we do not know if the file was created without chunking. If it was created 
    // without chunking, we cannot map it in chunks without risking datastructures
    // that cross a mapping boundary.
    if (chunk_mapping_enabled() && !matches_mmap_boundary(size)) {
        size = get_upper_mmap_boundary(size);
        m_file.resize(size);
        // resizing the file (as we do here) without actually changing any internal
        // datastructures to reflect the additional free space will work, because the
        // free space management relies on the logical filesize and disregards the
        // actual size of the file.
    }

    try {
        File::Map<char> map(m_file, File::access_ReadOnly, size); // Throws

        m_file_on_streaming_form = false; // May be updated by validate_buffer()
        if (!skip_validate) {
            // Verify the data structures
            if (!validate_buffer(map.get_addr(), size_of_streaming_file, top_ref))
                goto invalid_database;
        }
        m_data        = map.release();
        m_baseline    = size;
        m_initial_mapping_size = size;
        m_first_additional_chunk = get_chunk_index(m_initial_mapping_size);
        m_attach_mode = is_shared ? attach_SharedFile : attach_UnsharedFile;

        Header* header;

        if (did_create) {
            File::Map<Header> writable_map(m_file, File::access_ReadWrite, sizeof (Header)); // Throws
            header = writable_map.get_addr();
            header->m_flags |= server_sync_mode ? flags_ServerSyncMode : 0x0;
            header = reinterpret_cast<Header*>(m_data);
            bool stored_server_sync_mode = (header->m_flags & flags_ServerSyncMode) != 0;
            if (server_sync_mode != stored_server_sync_mode)
                throw std::runtime_error(path + ": failed to write!");
        }
        else {
            header = reinterpret_cast<Header*>(m_data);
            bool stored_server_sync_mode = (header->m_flags & flags_ServerSyncMode) != 0;
            if (server_sync_mode &&  !stored_server_sync_mode)
                throw std::runtime_error(path + ": expected db in server sync mode, found local mode");
            if (!server_sync_mode &&  stored_server_sync_mode)
                throw std::runtime_error(path + ": found db in server sync mode, expected local mode");
        }

        int select_field = header->m_flags;
        select_field ^= SlabAlloc::flags_SelectBit;
        m_file_format_version = header->m_file_format_version[select_field];

    }
    catch (DecryptionFailed) {
        goto invalid_database;
    }

    // make sure that any call to begin_read cause any slab to be placed in free lists correctly
    m_free_space_state = free_space_Invalid;

    if (m_file_on_streaming_form && read_only && is_shared)
        goto invalid_database;

    // make sure the database is not on streaming format. This has to be done at
    // session initialization, even if it means writing the database during open.
    if (session_initiator && m_file_on_streaming_form) {

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

        StreamingFooter* footer = reinterpret_cast<StreamingFooter*>(m_data+size_of_streaming_file) - 1;
        REALM_ASSERT_3(footer->m_magic_cookie, ==, footer_magic_cookie);
        ::mprotect(header, sizeof(Header), PROT_READ | PROT_WRITE);
        header->m_top_ref[1] = footer->m_top_ref;
        ::msync(header, sizeof(Header), MS_SYNC);
        header->m_flags |= flags_SelectBit; // keep bit 1 used for server sync mode unchanged
        m_file_on_streaming_form = false;
        ::msync(header, sizeof(Header), MS_SYNC);
        ::mprotect(header, sizeof(Header), PROT_READ);
    }

    fcg.release(); // Do not close
    return top_ref;

  invalid_database:
    throw InvalidDatabase();
}

unsigned char SlabAlloc::get_file_format() const
{
    return m_file_format_version;
}

ref_type SlabAlloc::attach_buffer(char* data, size_t size)
{
    REALM_ASSERT(!is_attached());

    // Verify the data structures
    m_file_on_streaming_form = false; // May be updated by validate_buffer()
    ref_type top_ref;
    if (!validate_buffer(data, size, top_ref))
        throw InvalidDatabase();

    m_data        = data;
    m_baseline    = size;
    m_initial_mapping_size = size;
    m_attach_mode = attach_UsersBuffer;

    return top_ref;
}


void SlabAlloc::attach_empty()
{
    REALM_ASSERT(!is_attached());

    m_attach_mode = attach_OwnedBuffer;
    m_data = 0; // Empty buffer

    // No ref must ever be less that the header size, so we will use that as the
    // baseline here.
    m_baseline = sizeof (Header);
    m_initial_mapping_size = m_baseline;
}

bool SlabAlloc::validate_buffer(const char* data, size_t size, ref_type& top_ref)
{
    // Verify that size is sane and 8-byte aligned
    if (size < sizeof (Header) || size % 8 != 0)
        return false;

    // File header is 24 bytes, composed of three 64-bit
    // blocks. The two first being top_refs (only one valid
    // at a time) and the last being the info block.
    const char* file_header = data;

    // First four bytes of info block is file format id
    if (!(file_header[16] == 'T' &&
          file_header[17] == '-' &&
          file_header[18] == 'D' &&
          file_header[19] == 'B'))
        return false; // Not a realm file

    // Last bit in info block indicates which top_ref block is valid
    int valid_part = file_header[16 + 7] & 0x1;

    // Byte 4 and 5 (depending on valid_part) in the info block is version
    int version = static_cast<unsigned char>(file_header[16 + 4 + valid_part]);
    if (version > default_file_format_version)
        return false; // unsupported version

    // Top_ref should always point within buffer
    const uint64_t* top_refs = reinterpret_cast<const uint64_t*>(data);
    uint_fast64_t ref = top_refs[valid_part];
    if (valid_part == 0 && ref == 0xFFFFFFFFFFFFFFFFULL) {
        if (size < sizeof (Header) + sizeof (StreamingFooter))
            return false;
        const StreamingFooter* footer = reinterpret_cast<const StreamingFooter*>(data+size) - 1;
        ref = footer->m_top_ref;
        if (footer->m_magic_cookie != footer_magic_cookie)
            return false;
        m_file_on_streaming_form = true;
    }
    if (ref >= size || ref % 8 != 0 || ref > std::numeric_limits<ref_type>::max())
        return false; // invalid top_ref

    top_ref = ref_type(ref);
    return true;
}


void SlabAlloc::do_prepare_for_update(char* mutable_data, util::File::Map<char>& mapping)
{
    REALM_ASSERT(m_file_on_streaming_form);
    Header* header = reinterpret_cast<Header*>(mutable_data);

    // Don't compare file format version fields as they are allowed to differ. 
    // Also don't compare reserved fields (todo, is it correct to ignore?)
    REALM_ASSERT_3(header->m_flags, == , streaming_header.m_flags);
    REALM_ASSERT_3(header->m_mnemonic[0], == , streaming_header.m_mnemonic[0]);
    REALM_ASSERT_3(header->m_mnemonic[1], == , streaming_header.m_mnemonic[1]);
    REALM_ASSERT_3(header->m_mnemonic[2], == , streaming_header.m_mnemonic[2]);
    REALM_ASSERT_3(header->m_mnemonic[3], == , streaming_header.m_mnemonic[3]);
    REALM_ASSERT_3(header->m_top_ref[0], == , streaming_header.m_top_ref[0]);
    REALM_ASSERT_3(header->m_top_ref[1], == , streaming_header.m_top_ref[1]);

    StreamingFooter* footer = reinterpret_cast<StreamingFooter*>(mutable_data+m_baseline) - 1;
    REALM_ASSERT_3(footer->m_magic_cookie, ==, footer_magic_cookie);
    header->m_top_ref[1] = footer->m_top_ref;
    mapping.sync();
    header->m_flags |= flags_SelectBit; // keep bit 1 used for server sync mode unchanged
    m_file_on_streaming_form = false;
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


bool SlabAlloc::remap(size_t file_size)
{
    //std::cerr << "------------------------- remap(" << file_size << ") --------------------------" << std::endl;

    REALM_ASSERT_DEBUG(file_size % 8 == 0); // 8-byte alignment required
    REALM_ASSERT_DEBUG(m_attach_mode == attach_SharedFile || m_attach_mode == attach_UnsharedFile);
    REALM_ASSERT_DEBUG(m_free_space_state == free_space_Clean);
    REALM_ASSERT_DEBUG(m_baseline <= file_size);

    bool addr_changed;
    if (chunk_mapping_enabled()) {

        // Extend mapping by adding chunks
        REALM_ASSERT_DEBUG(matches_mmap_boundary(file_size));
        m_file.resize(file_size);
        m_baseline = file_size;
        auto num_chunks = get_chunk_index(file_size);
        auto num_additional_mappings = num_chunks - m_first_additional_chunk;

        if (num_additional_mappings > m_capacity_additional_mappings) {
            // FIXME: No harcoded constants here
            m_capacity_additional_mappings = num_additional_mappings + 128;
            util::File::Map<char>* new_mappings = new util::File::Map<char>[m_capacity_additional_mappings];
            for (std::size_t j = 0; j < m_num_additional_mappings; ++j)
                new_mappings[j].move(m_additional_mappings[j]);
            delete[] m_additional_mappings;
            m_additional_mappings = new_mappings;
        }
        for (auto k = m_num_additional_mappings; k < num_additional_mappings; ++k)
        {
            auto chunk_start_offset = get_chunk_base(k + m_first_additional_chunk);
            auto chunk_size = get_chunk_base(1 + k + m_first_additional_chunk) - chunk_start_offset;
            util::File::Map<char> map(m_file, chunk_start_offset, File::access_ReadOnly, chunk_size);
            m_additional_mappings[k].move(map);
        }
        m_num_additional_mappings = num_additional_mappings;
        addr_changed = false; // mappings never change :-)
    }
    else {

        void* addr = m_file.remap(m_data, m_baseline, File::access_ReadOnly, file_size);
        addr_changed = addr != m_data;

        m_data = static_cast<char*>(addr);
        m_baseline = file_size;
        m_initial_mapping_size = file_size;
    }
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

    return addr_changed;
}

const SlabAlloc::chunks& SlabAlloc::get_free_read_only() const
{
    if (m_free_space_state == free_space_Invalid)
        throw InvalidFreeSpace();
    return m_free_read_only;
}


bool SlabAlloc::chunk_mapping_enabled() const REALM_NOEXCEPT
{ 
    return m_chunk_size != 0; 
}

std::size_t SlabAlloc::get_upper_mmap_boundary(std::size_t start_pos) const REALM_NOEXCEPT
{
    return get_chunk_base(1+get_chunk_index(start_pos));
}

std::size_t SlabAlloc::get_lower_mmap_boundary(std::size_t start_pos) const REALM_NOEXCEPT
{
    return get_chunk_base(get_chunk_index(start_pos));
}

bool SlabAlloc::matches_mmap_boundary(std::size_t pos) const REALM_NOEXCEPT
{
    return pos == get_lower_mmap_boundary(pos);
}

std::size_t SlabAlloc::get_chunk_index(std::size_t pos) const REALM_NOEXCEPT
{
    if (!chunk_mapping_enabled()) return 0;
    size_t chunk_base_number = pos/m_chunk_size;
    size_t chunk_group_number = chunk_base_number/16;
    size_t index;
    if (chunk_group_number == 0) {
        // first 16 entries aligns 1:1
        index = chunk_base_number;
    }
    else {
        // remaning entries are exponential
        size_t log_index = log2(chunk_group_number);
        size_t chunk_index_in_group = (chunk_base_number >> (1+log_index)) & 0x7;
        index = (16 + (log_index * 8)) + chunk_index_in_group;
//        std::cerr << "  group_number = " << chunk_group_number
//                  << "  log_index = " << log_index << "  in group = " << chunk_index_in_group << std::endl;
    }
//    std::cerr << "chunk_index( " << pos << " ) -> " << index << std::endl;
    return index;

//    return chunk_mapping_enabled() ? (pos / m_chunk_size) : 0;
}

std::size_t SlabAlloc::get_chunk_base(std::size_t index) const REALM_NOEXCEPT
{
    size_t base;
    if (index < 16) {
        base = index * m_chunk_size;
    }
    else {
        size_t chunk_index_in_group = index & 7;
        size_t log_index = (index - chunk_index_in_group)/8 - 2;
        size_t chunk_base_number = (8 + chunk_index_in_group)<<(1+log_index);
        base = m_chunk_size * chunk_base_number;
    }
//    std::cerr << "                                   chunk_base( " << 
//        index << " ) -> " << base << std::endl;
    return base;
//    return index * m_chunk_size;
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


void SlabAlloc::Verify() const
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
