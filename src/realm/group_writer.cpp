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
            int_fast64_t value = int_fast64_t(initial_version); // FIXME: Problematic unsigned -> signed conversion
            top.add(1 + 2*value); // Throws
        }

        if (ref_type ref = m_free_versions.get_ref_from_parent()) {
            m_free_versions.init_from_ref(ref);
        }
        else {
            size_t n = m_free_positions.size();
            bool context_flag = false;
            int_fast64_t value = int_fast64_t(initial_version); // FIXME: Problematic unsigned -> signed conversion
            m_free_versions.create(Array::type_Normal, context_flag, n, value); // Throws
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

    m_file_map.map(m_alloc.m_file, File::access_ReadWrite, m_alloc.get_baseline()); // Throws
}


size_t GroupWriter::write_group()
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
    bool recurse = true, persist = true;
    size_t names_pos  = m_group.m_table_names.write(*this, recurse, persist); // Throws
    size_t tables_pos = m_group.m_tables.write(*this, recurse, persist); // Throws

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
    // reserve_free_space() below) may add an extra entry to the free-lists.
    ++max_free_list_size;

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
    m_free_positions.ensure_minimum_width(reserve_pos + max_free_space_needed); // Throws

    // Get final sizes of free-list arrays
    size_t free_positions_size = m_free_positions.get_byte_size();
    size_t free_sizes_size     = m_free_lengths.get_byte_size();
    size_t free_versions_size  = is_shared ? m_free_versions.get_byte_size() : 0;
    REALM_ASSERT(!is_shared ||
                 Array::get_wtype_from_header(Array::get_header_from_data(m_free_versions.m_data)) ==
                 Array::wtype_Bits);

    // Calculate write positions
    size_t free_positions_pos = reserve_pos;
    size_t free_sizes_pos     = free_positions_pos + free_positions_size;
    size_t free_versions_pos  = free_sizes_pos     + free_sizes_size;
    size_t top_pos            = free_versions_pos  + free_versions_size;

    // Update top to point to the calculated positions
    top.set(0, names_pos); // Throws
    top.set(1, tables_pos); // Throws
    // Third slot holds the logical file size
    top.set(3, free_positions_pos); // Throws
    top.set(4, free_sizes_pos); // Throws
    if (is_shared) {
        top.set(5, free_versions_pos); // Throws
        // Seventh slot holds the database version (a.k.a. transaction number)
        top.set(6, m_current_version * 2 +1); // Throws
    }

    // Get final sizes
    size_t top_byte_size = top.get_byte_size();
    size_t end_pos = top_pos + top_byte_size;
    REALM_ASSERT_3(end_pos, <=, reserve_pos + max_free_space_needed);

    // Deduct the used space from the reserved chunk. Note that we have made
    // sure that the remaining size is never zero. Also, by the call to
    // m_free_positions.ensure_minimum_width() above, we have made sure that
    // m_free_positions has the capacity to store the new larger value without
    // reallocation.
    size_t rest = reserve_pos + reserve_size - end_pos;
    REALM_ASSERT_3(rest, >, 0);
    m_free_positions.set(reserve_ndx, end_pos); // Throws
    m_free_lengths.set(reserve_ndx, rest); // Throws

    // The free-list now have their final form, so we can write them to the file
    write_array_at(free_positions_pos, m_free_positions.get_header(),
                   free_positions_size); // Throws
    write_array_at(free_sizes_pos, m_free_lengths.get_header(),
                   free_sizes_size); // Throws
    if (is_shared) {
        write_array_at(free_versions_pos, m_free_versions.get_header(),
                       free_versions_size); // Throws
    }

    // Write top
    write_array_at(top_pos, top.get_header(), top_byte_size); // Throws

    // Return top_pos so that it can be saved in lock file used for coordination
    return top_pos;
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

    size_t rest = chunk_size - size;
    if (rest > 0) {
        m_free_positions.set(chunk_ndx, chunk_pos + size); // FIXME: Undefined conversion to signed
        m_free_lengths.set(chunk_ndx, rest); // FIXME: Undefined conversion to signed
    }
    else {
        m_free_positions.erase(chunk_ndx);
        m_free_lengths.erase(chunk_ndx);
        if (is_shared)
            m_free_versions.erase(chunk_ndx);
    }

    return chunk_pos;
}


std::pair<size_t, size_t> GroupWriter::reserve_free_space(size_t size)
{
    bool is_shared = m_group.m_is_shared;

    // Since we do a first-fit search for small chunks, the top pieces are
    // likely to get smaller and smaller. So when we are looking for bigger
    // chunks we are likely to find them faster by skipping the first half of
    // the list.
    size_t end   = m_free_lengths.size();
    size_t begin = size < 1024 ? 0 : end / 2;

    // Do we have a free space we can reuse?
  again:
    for (size_t i = begin; i != end; ++i) {
        size_t chunk_size = to_size_t(m_free_lengths.get(i));
        if (chunk_size >= size) {
            // Only blocks that are not occupied by current readers
            // are allowed to be used.
            if (is_shared) {
                size_t ver = to_size_t(m_free_versions.get(i));
                if (ver >= m_readlock_version)
                    continue;
            }

            // Match found!
            return std::make_pair(i, chunk_size);
        }
    }

    if (begin > 0) {
        end = begin;
        begin = 0;
        goto again;
    }

    // No free space, so we have to extend the file.
    return extend_free_space(size);
}


std::pair<size_t, size_t> GroupWriter::extend_free_space(size_t requested_size)
{
    bool is_shared = m_group.m_is_shared;

    // We need to consider the "logical" size of the file here, and not the real
    // size. The real size may have changed without the free space information
    // having been adjusted accordingly. This can happen, for example, if
    // write_group() fails before writing the new top-ref, but after having
    // extended the file size.
    size_t logical_file_size = to_size_t(m_group.m_top.get(2) / 2);

    // If we already have a free chunk at the end of the file, we only need to
    // extend that chunk. If not, we need to add a new entry to the list of free
    // chunks.
    size_t extend_size = requested_size;
    bool extend_last_chunk = false;
    size_t last_chunk_size = 0;
    if (!m_free_positions.is_empty()) {
        bool in_use = false;
        if (is_shared) {
            size_t ver = to_size_t(m_free_versions.back());
            if (ver >= m_readlock_version)
                in_use = true;
        }
        if (!in_use) {
            size_t last_pos  = to_size_t(m_free_positions.back());
            size_t last_size = to_size_t(m_free_lengths.back());
            REALM_ASSERT_3(last_size, <, extend_size);
            REALM_ASSERT_3(last_pos + last_size, <=, logical_file_size);
            if (last_pos + last_size == logical_file_size) {
                extend_last_chunk = true;
                last_chunk_size = last_size;
                extend_size -= last_size;
            }
        }
    }

    size_t min_file_size = logical_file_size;
    if (int_add_with_overflow_detect(min_file_size, extend_size))
        throw std::runtime_error("File size overflow");

    // We double the size until we reach 'stop_doubling_size'. From then on we
    // increment the size in steps of 'stop_doubling_size'. This is to achieve a
    // reasonable compromise between minimizing fragmentation (maximizing
    // performance) and minimizing over-allocation.
#ifdef REALM_MOBILE
    size_t stop_doubling_size = 16 * (1024*1024L); // = 16 MiB
#else
    size_t stop_doubling_size = 128 * (1024*1024L); // = 128 MiB
#endif
    REALM_ASSERT_3(stop_doubling_size % 8, ==, 0);

    size_t new_file_size = logical_file_size;
    while (new_file_size < min_file_size) {
        if (new_file_size < stop_doubling_size) {
            // The file contains at least a header, so the size can never be
            // zero. We need this to ensure that the number of iterations will
            // be finite.
            REALM_ASSERT_3(new_file_size, !=, 0);
            // Be sure that the doubling does not overflow
            REALM_ASSERT_3(stop_doubling_size, <=, std::numeric_limits<size_t>::max() / 2);
            new_file_size *= 2;
        }
        else {
            if (int_add_with_overflow_detect(new_file_size, stop_doubling_size)) {
                new_file_size = std::numeric_limits<size_t>::max();
                new_file_size &= ~size_t(7); // 8-byte alignment
            }
        }
    }

    // The size must be a multiple of 8. This is guaranteed as long as the
    // initial size is a multiple of 8.
    REALM_ASSERT_3(new_file_size % 8, ==, 0);

    // Note: File::prealloc() may misbehave under race conditions (see
    // documentation of File::prealloc()). Fortunately, no race conditions can
    // occur, because in transactional mode we hold a write lock at this time,
    // and in non-transactional mode it is the responsibility of the user to
    // ensure non-concurrent file mutation.
    m_alloc.resize_file(new_file_size); // Throws

    m_file_map.remap(m_alloc.m_file, File::access_ReadWrite, new_file_size);

    size_t chunk_ndx  = m_free_positions.size();
    size_t chunk_size = new_file_size - logical_file_size;
    if (extend_last_chunk) {
        --chunk_ndx;
        chunk_size += last_chunk_size;
        REALM_ASSERT_3(chunk_size % 8, ==, 0); // 8-byte alignment
        m_free_lengths.set(chunk_ndx, chunk_size);
    }
    else { // Else add new free space
        REALM_ASSERT_3(chunk_size % 8, ==, 0); // 8-byte alignment
        m_free_positions.add(logical_file_size);
        m_free_lengths.add(chunk_size);
        if (is_shared)
            m_free_versions.add(0); // new space is always free for writing
    }

    // Update the logical file size
    m_group.m_top.set(2, 1 + 2*new_file_size); // Throws

    return std::make_pair(chunk_ndx, chunk_size);
}


void GroupWriter::write(const char* data, size_t size)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);
    REALM_ASSERT_3((pos & 0x7), ==, 0); // Write position should always be 64bit aligned

    // Write the block
    char* dest_addr = m_file_map.get_addr() + pos;
    std::copy(data, data+size, dest_addr);
}


size_t GroupWriter::write_array(const char* data, size_t size, uint_fast32_t checksum)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);
    REALM_ASSERT_3((pos & 0x7), ==, 0); // Write position should always be 64bit aligned

    // Write the block
    char* dest_addr = m_file_map.get_addr() + pos;
#ifdef REALM_DEBUG
    const char* cksum_bytes = reinterpret_cast<const char*>(&checksum);
    std::copy(cksum_bytes, cksum_bytes+4, dest_addr);
    std::copy(data+4, data+size, dest_addr+4);
#else
    static_cast<void>(checksum);
    std::copy(data, data+size, dest_addr);
#endif

    // return the position it was written
    return pos;
}


void GroupWriter::write_array_at(size_t pos, const char* data, size_t size)
{
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
    if (!disable_sync)
        m_file_map.sync(); // Throws

    // update selector - must happen after write of all data and top pointer
    file_header[16+7] = char(select_field); // swap

    // file is guaranteed to be of version 'default_file_format_version' now
    file_header[16 + 4 + new_valid_ref] = default_file_format_version;

    // Write new selector to disk
    // FIXME: we might optimize this to write of a single page?
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
