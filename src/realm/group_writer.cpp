#include <algorithm>
#include <iostream>

#include <realm/util/safe_int_ops.hpp>
#include <realm/group_writer.hpp>
#include <realm/group_shared.hpp>
#include <realm/alloc_slab.hpp>
#include <realm/disable_sync_to_disk.hpp>

using namespace realm;
using namespace realm::util;


GroupWriter::GroupWriter(Group& group):
    m_group(group),
    m_alloc(group.m_alloc),
    m_free_positions(m_alloc),
    m_free_lengths(m_alloc),
    m_free_versions(m_alloc),
    m_current_version(0)
{
    Array& top = m_group.m_top;
    bool is_shared = m_group.m_is_shared;

    m_free_positions.set_parent(&top, 3);
    m_free_lengths.set_parent(&top,   4);
    m_free_versions.set_parent(&top,  5);

    if (top.size() < 5) {
        REALM_ASSERT(top.size() == 3);
        // m_free_positions
        top.add(0); // Throws
        // m_free_lengths
        top.add(0); // Throws
    }

    if (ref_type ref = m_free_positions.get_ref_from_parent()) {
        m_free_positions.init_from_ref(ref);
    }
    else {
        m_free_positions.create(Array::type_Normal); // Throws
        _impl::DestroyGuard<ArrayInteger> dg(&m_free_positions);
        m_free_positions.update_parent(); // Throws
        dg.release();
    }

    if (ref_type ref = m_free_lengths.get_ref_from_parent()) {
        m_free_lengths.init_from_ref(ref);
    }
    else {
        m_free_lengths.create(Array::type_Normal); // Throws
        _impl::DestroyGuard<ArrayInteger> dg(&m_free_lengths);
        m_free_lengths.update_parent(); // Throws
        dg.release();
    }

    if (is_shared) {
        SharedGroup::version_type initial_version = 0;

        if (top.size() < 7) {
            REALM_ASSERT(top.size() == 5);
            // m_free_versions
            top.add(0); // Throws
            // Transaction number / version
            top.add(0); // Throws
        }

        if (ref_type ref = m_free_versions.get_ref_from_parent()) {
            m_free_versions.init_from_ref(ref);
        }
        else {
            int_fast64_t value = int_fast64_t(initial_version); // FIXME: Problematic unsigned -> signed conversion
            top.set(6, 1 + 2*value); // Throws
            size_t n = m_free_positions.size();
            bool context_flag = false;
            m_free_versions.Array::create(Array::type_Normal, context_flag, n, value); // Throws
            _impl::DestroyGuard<ArrayInteger> dg(&m_free_versions);
            m_free_versions.update_parent(); // Throws
            dg.release();
        }
    }
    else { // !is_shared
        if (top.size() > 5) {
            REALM_ASSERT(top.size() == 7);
            top.truncate_and_destroy_children(5);
        }
    }

    m_file_map.map(m_alloc.m_file, File::access_ReadWrite, m_alloc.get_mapped_size()); // Throws
}


ref_type GroupWriter::write_group()
{
    merge_free_space(); // Throws

    Array& top = m_group.m_top;
    bool is_shared = m_group.m_is_shared;

    REALM_ASSERT_3(m_free_positions.size(), ==, m_free_lengths.size());
    REALM_ASSERT(!is_shared || m_free_versions.size() == m_free_lengths.size());

    // Recursively write all changed arrays (but not 'top' and free-lists yet,
    // as they are going to change along the way.) If free space is available in
    // the attached database file, we use it, but this does not include space
    // that has been release during the current transaction (or since the last
    // commit), as that would lead to clobbering of the previous database
    // version.
    bool deep = true, only_if_modified = true;
    ref_type names_ref  = m_group.m_table_names.write(*this, deep, only_if_modified); // Throws
    ref_type tables_ref = m_group.m_tables.write(*this, deep, only_if_modified); // Throws

    int_fast64_t value_1 = int_fast64_t(names_ref); // FIXME: Problematic unsigned -> signed conversion
    int_fast64_t value_2 = int_fast64_t(tables_ref); // FIXME: Problematic unsigned -> signed conversion
    top.set(0, value_1); // Throws
    top.set(1, value_2); // Throws

    if (top.size() >= 8) {
        // FIXME: Are refs correctly handled here? Refs pointing inside
        // the file must have bit 0 cleared.
        if (ref_type sync_history_ref = top.get_as_ref(7)) {
            Allocator& alloc = top.get_alloc();
            ref_type new_sync_history_ref =
                Array::write(sync_history_ref, alloc, *this, only_if_modified); // Throws
            int_fast64_t value_3 = int_fast64_t(new_sync_history_ref); // FIXME: Problematic unsigned -> signed conversion
            top.set(7, value_3); // Throws
        }
    }

    // We now have a bit of a chicken-and-egg problem. We need to write the
    // free-lists to the file, but the act of writing them will consume free
    // space, and thereby change the free-lists. To solve this problem, we
    // calculate an upper bound on the amount af space required for all of the
    // remaining arrays and allocate the space as one big chunk. This way we can
    // finalize the free-lists before writing them to the file.
    size_t max_free_list_size = m_free_positions.size();

    // We need to add to the free-list any space that was freed during the
    // current transaction, but to avoid clobering the previous version, we
    // cannot add it yet. Instead we simply account for the space
    // required. Since we will modify the free-lists themselves, we must ensure
    // that the original arrays used by the free-lists are counted as part of
    // the space that was freed during the current transaction. Note that a
    // copy-on-write on m_free_positions, for example, also implies a
    // copy-on-write on Group::m_top.
    m_free_positions.copy_on_write(); // Throws
    m_free_lengths.copy_on_write(); // Throws
    if (is_shared)
        m_free_versions.copy_on_write(); // Throws
    const SlabAlloc::chunks& new_free_space = m_group.m_alloc.get_free_read_only(); // Throws
    max_free_list_size += new_free_space.size();

    // The final allocation of free space (i.e., the call to
    // reserve_free_space() below) may add extra entries to the free-lists.
    // We reserve room for the worst case scenario, which is as follows:
    // If the database has *max* theoretical fragmentation, it'll need one
    // entry in the free list for every 16 bytes, because both allocated and
    // free chunks are at least 8 bytes in size. In the worst case each
    // free list entry requires 24 bytes (8 for the position, 8 for the version
    // and 8 for the size). The worst case scenario thus needs access
    // to a contiguous address range of 24/16 * the existing database size.
    // The memory mapping grows with one contiguous memory range at a time,
    // each of these being at least 1/16 of the existing database size.
    // To be sure to get a contiguous range of 24/16 of the current database
    // size, the database itself would have to grow x24. This growth requires
    // at the most 5x16 = 80 extension steps, each adding one entry to the free list.
    // (a smaller upper bound could likely be derived here, but it's not that important)
    max_free_list_size += 80;

    int num_free_lists = is_shared ? 3 : 2;
    int max_top_size = 3 + num_free_lists;
    if (is_shared)
        ++max_top_size; // database version (a.k.a. transaction number)
    size_t max_free_space_needed = Array::get_max_byte_size(max_top_size) +
        num_free_lists * Array::get_max_byte_size(max_free_list_size);

    // Reserve space for remaining arrays. We ask for one extra byte beyond the
    // maximum number that is required. This ensures that even if we end up
    // using the maximum size possible, we still do not end up with a zero size
    // free-space chunk as we deduct the actually used size from it.
    std::pair<size_t, size_t> reserve = reserve_free_space(max_free_space_needed + 1); // Throws
    size_t reserve_ndx  = reserve.first;
    size_t reserve_size = reserve.second;
    // At this point we have allocated all the space we need, so we can add to
    // the free-lists any free space created during the current transaction (or
    // since last commit). Had we added it earlier, we would have risked
    // clobering the previous database version. Note, however, that this risk
    // would only have been present in the non-transactionl case where there is
    // no version tracking on the free-space chunks.
    {
        typedef SlabAlloc::chunks::const_iterator iter;
        iter end = new_free_space.end();
        for (iter i = new_free_space.begin(); i != end; ++i) {
            ref_type ref = i->ref;
            size_t size  = i->size;
            // We always want to keep the list of free space in sorted order (by
            // ascending position) to facilitate merge of adjacent segments. We
            // can find the correct insert postion by binary search
            size_t ndx = m_free_positions.lower_bound_int(ref);
            m_free_positions.insert(ndx, ref); // Throws
            m_free_lengths.insert(ndx, size); // Throws
            if (is_shared)
                m_free_versions.insert(ndx, m_current_version); // Throws
            // Adjust reserve_ndx if necessary
            if (ndx <= reserve_ndx)
                ++reserve_ndx;
        }
    }

    // Before we calculate the actual sizes of the free-list arrays, we must
    // make sure that the final adjustments of the free lists (i.e., the
    // deduction of the actually used space from the reserved chunk,) will not
    // change the byte-size of those arrays.
    size_t reserve_pos = to_size_t(m_free_positions.get(reserve_ndx));
    REALM_ASSERT_3(reserve_size, >, max_free_space_needed);
    int_fast64_t value_4 = int_fast64_t(reserve_pos + max_free_space_needed); // FIXME: Problematic unsigned -> signed conversion
    m_free_positions.ensure_minimum_width(value_4); // Throws

    // Get final sizes of free-list arrays
    size_t free_positions_size = m_free_positions.get_byte_size();
    size_t free_sizes_size     = m_free_lengths.get_byte_size();
    size_t free_versions_size  = is_shared ? m_free_versions.get_byte_size() : 0;
    REALM_ASSERT(!is_shared ||
                 Array::get_wtype_from_header(Array::get_header_from_data(m_free_versions.m_data)) ==
                 Array::wtype_Bits);

    // Calculate write positions
    REALM_ASSERT((reserve_pos & 0x2) == 0);
    ref_type reserve_ref        = to_ref(reserve_pos);
    ref_type free_positions_ref = reserve_ref;
    ref_type free_sizes_ref     = free_positions_ref + free_positions_size;
    ref_type free_versions_ref  = free_sizes_ref     + free_sizes_size;
    ref_type top_ref            = free_versions_ref  + free_versions_size;

    // Update top to point to the calculated positions
    int_fast64_t value_5 = int_fast64_t(free_positions_ref); // FIXME: Problematic unsigned -> signed conversion
    int_fast64_t value_6 = int_fast64_t(free_sizes_ref); // FIXME: Problematic unsigned -> signed conversion
    top.set(3, value_5); // Throws
    top.set(4, value_6); // Throws
    if (is_shared) {
        int_fast64_t value_7 = int_fast64_t(free_versions_ref); // FIXME: Problematic unsigned -> signed conversion
        int_fast64_t value_8 = 1 + 2 * int_fast64_t(m_current_version); // FIXME: Problematic unsigned -> signed conversion
        top.set(5, value_7); // Throws
        top.set(6, value_8); // Throws
    }

    // Get final sizes
    size_t top_byte_size = top.get_byte_size();
    ref_type end_ref = top_ref + top_byte_size;
    REALM_ASSERT_3(size_t(end_ref), <=, reserve_pos + max_free_space_needed);

    // Deduct the used space from the reserved chunk. Note that we have made
    // sure that the remaining size is never zero. Also, by the call to
    // m_free_positions.ensure_minimum_width() above, we have made sure that
    // m_free_positions has the capacity to store the new larger value without
    // reallocation.
    size_t rest = reserve_pos + reserve_size - size_t(end_ref);
    size_t used = size_t(end_ref) - reserve_pos;
    REALM_ASSERT_3(rest, >, 0);
    int_fast64_t value_8 = int_fast64_t(end_ref); // FIXME: Problematic unsigned -> signed conversion
    int_fast64_t value_9 = int_fast64_t(rest); // FIXME: Problematic unsigned -> signed conversion
    m_free_positions.set(reserve_ndx, value_8); // Throws
    m_free_lengths.set(reserve_ndx, value_9); // Throws

    // The free-list now have their final form, so we can write them to the file
    char* start_addr = m_file_map.get_addr() + reserve_ref;
    realm::util::encryption_read_barrier(start_addr, used, m_file_map.get_encrypted_mapping());
    write_array_at(free_positions_ref, m_free_positions.get_header(),
                   free_positions_size); // Throws
    write_array_at(free_sizes_ref, m_free_lengths.get_header(),
                   free_sizes_size); // Throws
    if (is_shared) {
        write_array_at(free_versions_ref, m_free_versions.get_header(),
                       free_versions_size); // Throws
    }

    // Write top
    write_array_at(top_ref, top.get_header(), top_byte_size); // Throws
    realm::util::encryption_write_barrier(start_addr, used, m_file_map.get_encrypted_mapping());

    // Return top_ref so that it can be saved in lock file used for coordination
    return top_ref;
}


void GroupWriter::merge_free_space()
{
    bool is_shared = m_group.m_is_shared;

    if (m_free_lengths.is_empty())
        return;

    size_t n = m_free_lengths.size() - 1;
    for (size_t i = 0; i < n; ++i) {
        size_t i2 = i + 1;
        size_t pos1  = to_size_t(m_free_positions.get(i));
        size_t size1 = to_size_t(m_free_lengths.get(i));
        size_t pos2  = to_size_t(m_free_positions.get(i2));
        if (pos2 == pos1 + size1) {
            // If this is a shared db, we can only merge
            // segments where no part is currently in use
            if (is_shared) {
                size_t v1 = to_size_t(m_free_versions.get(i));
                if (v1 >= m_readlock_version)
                    continue;
                size_t v2 = to_size_t(m_free_versions.get(i2));
                if (v2 >= m_readlock_version)
                    continue;
            }

            // FIXME: Performing a series of calls to Array::erase() is
            // unnecessarily expensive because we have to shift the contents
            // above i2 multiple times in general. A more efficient way would be
            // to use assigments only, and then make a final call to
            // Array::truncate() when the new size is know.

            // Merge
            size_t size2 = to_size_t(m_free_lengths.get(i2));
            m_free_lengths.set(i, size1 + size2);
            m_free_positions.erase(i2);
            m_free_lengths.erase(i2);
            if (is_shared)
                m_free_versions.erase(i2);

            --n;
            --i; // May underflow, but that is ok
        }
    }
}


size_t GroupWriter::get_free_space(size_t size)
{
    REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment

    std::pair<size_t, size_t> p = reserve_free_space(size);

    bool is_shared = m_group.m_is_shared;

    // Claim space from identified chunk
    size_t chunk_ndx  = p.first;
    size_t chunk_pos  = to_size_t(m_free_positions.get(chunk_ndx));
    size_t chunk_size = p.second;
    REALM_ASSERT_3(chunk_size, >=, size);
    REALM_ASSERT((chunk_size % 8) == 0);

    size_t rest = chunk_size - size;
    if (rest > 0) {
        // Allocating part of chunk - this alway happens from the beginning
        // of the chunk. The call to reserve_free_space may split chunks
        // in order to make sure that it returns a chunk from which allocation
        // can be done from the beginning
        m_free_positions.set(chunk_ndx, chunk_pos + size); // FIXME: Undefined conversion to signed
        m_free_lengths.set(chunk_ndx, rest); // FIXME: Undefined conversion to signed
    }
    else {
        // Allocating entire chunk
        m_free_positions.erase(chunk_ndx);
        m_free_lengths.erase(chunk_ndx);
        if (is_shared)
            m_free_versions.erase(chunk_ndx);
    }
    REALM_ASSERT((chunk_pos % 8) == 0);
    return chunk_pos;
}


inline size_t GroupWriter::split_freelist_chunk(size_t index, size_t start_pos,
                                                     size_t alloc_pos, size_t chunk_size,
                                                     bool is_shared)
{
    m_free_positions.insert(index, start_pos);
    m_free_lengths.insert(index, alloc_pos - start_pos);
    if (is_shared)
        m_free_versions.insert(index, 0);
    ++index;
    m_free_positions.set(index, alloc_pos);
    chunk_size = start_pos + chunk_size - alloc_pos;
    m_free_lengths.set(index, chunk_size);
    return chunk_size;
}


std::pair<size_t, size_t>
GroupWriter::search_free_space_in_part_of_freelist(size_t size, size_t begin,
                                                   size_t end, bool& found)
{
    bool is_shared = m_group.m_is_shared;
    SlabAlloc& alloc = m_group.m_alloc;
    for (size_t i = begin; i != end; ++i) {
        size_t chunk_size = to_size_t(m_free_lengths.get(i));
        if (chunk_size < size) {
            continue;
        }

        // Only chunks that are not occupied by current readers
        // are allowed to be used.
        if (is_shared) {
            size_t ver = to_size_t(m_free_versions.get(i));
            if (ver >= m_readlock_version) {
                continue;
            }
        }

        // search through the chunk, finding a place within it,
        // where an allocation will not cross a mmap boundary
        size_t start_pos = to_size_t(m_free_positions.get(i));
        size_t alloc_pos = alloc.find_section_in_range(start_pos, chunk_size, size);
        if (alloc_pos == 0) {
            continue;
        }
        // we found a place - if it's not at the beginning of the chunk,
        // we split the chunk so that the allocation can be done from the
        // beginning of the second chunk.
        if (alloc_pos != start_pos) {
            chunk_size = split_freelist_chunk(i, start_pos, alloc_pos, chunk_size, is_shared);
            ++i;
        }
        // Match found!
        found = true;
        return std::make_pair(i, chunk_size);
    }
    // No match
    found = false;
    return std::make_pair(end,0);
}


std::pair<size_t, size_t> GroupWriter::reserve_free_space(size_t size)
{
    typedef std::pair<size_t, size_t> Chunk;
    Chunk chunk;
    bool found;
    // Since we do a first-fit search for small chunks, the top pieces are
    // likely to get smaller and smaller. So when we are looking for bigger
    // chunks we are likely to find them faster by skipping the first half of
    // the list.
    size_t end = m_free_lengths.size();
    if (size < 1024) {
        chunk = search_free_space_in_part_of_freelist(size, 0, end, found);
        if (found) return chunk;
    }
    else {
        chunk = search_free_space_in_part_of_freelist(size, end/2, end, found);
        if (found) return chunk;
        chunk = search_free_space_in_part_of_freelist(size, 0, end/2, found);
        if (found) return chunk;
    }

    // No free space, so we have to extend the file.
    do {
        extend_free_space(size);
        // extending the file will add a new entry at the end of the freelist,
        // so search that particular entry
        end = m_free_lengths.size();
        chunk = search_free_space_in_part_of_freelist(size, end-1, end, found);
    }
    while (!found);
    return chunk;
}

// Extend the free space with at least the requested size.
// Due to mmap constraints, the extension can not be guaranteed to
// allow an allocation of the requested size, so multiple calls to
// extend_free_space may be needed, before an allocation can succeed.
std::pair<size_t, size_t> GroupWriter::extend_free_space(size_t requested_size)
{
    bool is_shared = m_group.m_is_shared;
    SlabAlloc& alloc = m_group.m_alloc;

    // We need to consider the "logical" size of the file here, and not the real
    // size. The real size may have changed without the free space information
    // having been adjusted accordingly. This can happen, for example, if
    // write_group() fails before writing the new top-ref, but after having
    // extended the file size. It can also happen as part of initial file expansion
    // during attach_file().
    size_t logical_file_size = to_size_t(m_group.m_top.get(2) / 2);
    size_t extend_size = requested_size;
    size_t new_file_size = logical_file_size + extend_size;
    if (!alloc.matches_section_boundary(new_file_size)) {
        new_file_size = alloc.get_upper_section_boundary(new_file_size);
    }
    // The size must be a multiple of 8. This is guaranteed as long as
    // the initial size is a multiple of 8.
    REALM_ASSERT_3(new_file_size % 8, ==, 0);
    REALM_ASSERT_3(logical_file_size, <, new_file_size);

    // Note: File::prealloc() may misbehave under race conditions (see
    // documentation of File::prealloc()). Fortunately, no race conditions can
    // occur, because in transactional mode we hold a write lock at this time,
    // and in non-transactional mode it is the responsibility of the user to
    // ensure non-concurrent file mutation.
    m_alloc.resize_file(new_file_size); // Throws

    m_file_map.remap(m_alloc.m_file, File::access_ReadWrite, new_file_size); // Throws

    size_t chunk_ndx  = m_free_positions.size();
    size_t chunk_size = new_file_size - logical_file_size;
    REALM_ASSERT_3(chunk_size % 8, ==, 0); // 8-byte alignment
    m_free_positions.add(logical_file_size);
    m_free_lengths.add(chunk_size);
    if (is_shared)
        m_free_versions.add(0); // new space is always free for writing

    // Update the logical file size
    m_group.m_top.set(2, 1 + 2*new_file_size); // Throws
    REALM_ASSERT(chunk_size != 0);
    REALM_ASSERT((chunk_size % 8) == 0);
    return std::make_pair(chunk_ndx, chunk_size);
}


void GroupWriter::write(const char* data, size_t size)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);
    REALM_ASSERT_3((pos & 0x7), ==, 0); // Write position should always be 64bit aligned

    // Write the block
    char* dest_addr = m_file_map.get_addr() + pos;
    realm::util::encryption_read_barrier(dest_addr, size, m_file_map.get_encrypted_mapping());
    std::copy(data, data+size, dest_addr);
    realm::util::encryption_write_barrier(dest_addr, size, m_file_map.get_encrypted_mapping());
}


ref_type GroupWriter::write_array(const char* data, size_t size, uint_fast32_t checksum)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);
    REALM_ASSERT_3((pos & 0x7), ==, 0); // Write position should always be 64bit aligned

    // Write the block
    char* dest_addr = m_file_map.get_addr() + pos;
    realm::util::encryption_read_barrier(dest_addr, size, m_file_map.get_encrypted_mapping());
#ifdef REALM_DEBUG
    const char* cksum_bytes = reinterpret_cast<const char*>(&checksum);
    std::copy(cksum_bytes, cksum_bytes+4, dest_addr);
    std::copy(data+4, data+size, dest_addr+4);
#else
    static_cast<void>(checksum);
    std::copy(data, data+size, dest_addr);
#endif

    realm::util::encryption_write_barrier(dest_addr, size, m_file_map.get_encrypted_mapping());
    // return ref of the written array
    ref_type ref = to_ref(pos);
    return ref;
}


void GroupWriter::write_array_at(ref_type ref, const char* data, size_t size)
{
    size_t pos = size_t(ref);

    REALM_ASSERT((ref & 0x2) == 0);
    REALM_ASSERT_3(pos + size, <=, to_size_t(m_group.m_top.get(2) / 2));
    REALM_ASSERT_3(pos + size, <=, m_file_map.get_size());
    char* dest_addr = m_file_map.get_addr() + pos;

#ifdef REALM_DEBUG
    uint_fast32_t dummy_checksum = 0x01010101UL;
    const char* cksum_bytes = reinterpret_cast<const char*>(&dummy_checksum);
    std::copy(cksum_bytes, cksum_bytes+4, dest_addr);
    std::copy(data+4, data+size, dest_addr+4);
#else
    std::copy(data, data+size, dest_addr);
#endif
}


void GroupWriter::commit(ref_type new_top_ref)
{
    // File header is 24 bytes, composed of three 64-bit blocks. The two first
    // being top_refs (only one valid at a time) and the last being the info
    // block.
    char* file_header = m_file_map.get_addr();
    realm::util::encryption_read_barrier(file_header, sizeof(SlabAlloc::Header), m_file_map.get_encrypted_mapping());

    // Least significant bit in last byte of info block indicates which top_ref
    // block is valid - other bits remain unchanged
    int select_field = file_header[16+7];
    select_field ^= SlabAlloc::flags_SelectBit;
    int new_valid_ref = select_field & SlabAlloc::flags_SelectBit;

    // FIXME: What rule guarantees that the new top ref is written to physical
    // medium before the swapping bit?

    // Update top ref pointer
    uint64_t* top_refs = reinterpret_cast<uint64_t*>(file_header);
    top_refs[new_valid_ref] = new_top_ref;

    // When running the test suite, device synchronization is disabled
    bool disable_sync = get_disable_sync_to_disk();

    // Make sure that all data and the top pointer is written to stable storage
    realm::util::encryption_write_barrier(file_header, sizeof(SlabAlloc::Header), m_file_map.get_encrypted_mapping());
    if (!disable_sync)
        m_file_map.sync(); // Throws

    // update selector - must happen after write of all data and top pointer
    file_header[16+7] = char(select_field); // swap

    // file format is guaranteed to be at `library_file_format` now
    file_header[16 + 4 + new_valid_ref] = SlabAlloc::library_file_format;

    // Write new selector to disk
    // FIXME: we might optimize this to write of a single page?
    realm::util::encryption_write_barrier(file_header, sizeof(SlabAlloc::Header), m_file_map.get_encrypted_mapping());
    if (!disable_sync)
        m_file_map.sync(); // Throws
}



#ifdef REALM_DEBUG

void GroupWriter::dump()
{
    bool is_shared = m_group.m_is_shared;

    size_t count = m_free_lengths.size();
    std::cout << "count: " << count << ", m_size = " << m_file_map.get_size() << ", "
        "version >= " << m_readlock_version << "\n";
    if (!is_shared) {
        for (size_t i = 0; i < count; ++i) {
            std::cout << i << ": " << m_free_positions.get(i) << ", " << m_free_lengths.get(i) <<
                "\n";
        }
    }
    else {
        for (size_t i = 0; i < count; ++i) {
            std::cout << i << ": " << m_free_positions.get(i) << ", " << m_free_lengths.get(i) <<
                " - " << m_free_versions.get(i) << "\n";
        }
    }
}

#endif
