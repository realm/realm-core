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

#include <algorithm>

#ifdef REALM_DEBUG
#include <iostream>
#endif

#include <realm/util/miscellaneous.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/group_writer.hpp>
#include <realm/group_shared.hpp>
#include <realm/alloc_slab.hpp>
#include <realm/disable_sync_to_disk.hpp>
#include <realm/metrics/metric_timer.hpp>

using namespace realm;
using namespace realm::util;
using namespace realm::metrics;

// Class controlling a memory mapped window into a file
class GroupWriter::MapWindow {
public:
    MapWindow(size_t alignment, util::File& f, ref_type start_ref, size_t initial_size);
    ~MapWindow();

    // translate a ref to a pointer
    // inside the window defined during construction.
    char* translate(ref_type ref);
    void encryption_read_barrier(void* start_addr, size_t size);
    void encryption_write_barrier(void* start_addr, size_t size);
    void sync();
    // return true if the specified range is fully visible through
    // the MapWindow
    bool matches(ref_type start_ref, size_t size);
    // return false if the mapping cannot be extended to hold the
    // requested size - extends if possible and then returns true
    bool extends_to_match(util::File& f, ref_type start_ref, size_t size);

private:
    util::File::Map<char> m_map;
    ref_type m_base_ref;
    ref_type aligned_to_mmap_block(ref_type start_ref);
    size_t get_window_size(util::File& f, ref_type start_ref, size_t size);
    size_t m_alignment;
};

// True if a requested block fall within a memory mapping.
bool GroupWriter::MapWindow::matches(ref_type start_ref, size_t size)
{
    if (start_ref < m_base_ref)
        return false;
    if (start_ref + size > m_base_ref + m_map.get_size())
        return false;
    return true;
}

// When determining which part of the file to mmap, We try to pick a 1MB window containing
// the requested block. We align windows on 1MB boundaries. We also align window size at
// 1MB, except in cases where the referenced part of the file straddles a 1MB boundary.
// In that case we choose a larger window.
//
// In cases where a 1MB window would stretch beyond the end of the file, we choose
// a smaller window. Anything mapped after the end of file would be undefined anyways.
ref_type GroupWriter::MapWindow::aligned_to_mmap_block(ref_type start_ref)
{
    // align to 1MB boundary
    size_t page_mask = m_alignment - 1;
    return start_ref & ~page_mask;
}

size_t GroupWriter::MapWindow::get_window_size(util::File& f, ref_type start_ref, size_t size)
{
    size_t window_size = start_ref + size - m_base_ref;
    // always map at least to match alignment
    if (window_size < m_alignment)
        window_size = m_alignment;
    // but never map beyond end of file
    size_t file_size = to_size_t(f.get_size());
    REALM_ASSERT_DEBUG_EX(start_ref + size <= file_size, start_ref + size, file_size);
    if (window_size > file_size - m_base_ref)
        window_size = file_size - m_base_ref;
    return window_size;
}

// The file may grow in increments much smaller than 1MB. This can lead to a stream of requests
// which are each just beyond the end of the last mapping we made. It is important to extend the
// existing window to cover the new request (if possible) as opposed to adding a new window.
// The reason is not obvious: open windows need to be sync'ed to disk at the end of the commit,
// and we really want to use as few calls to msync() as possible.
//
// extends_to_match() will extend an existing mapping to accomodate a new request if possible
// and return true. If the request falls in a different 1MB window, it'll return false.
bool GroupWriter::MapWindow::extends_to_match(util::File& f, ref_type start_ref, size_t size)
{
    size_t aligned_ref = aligned_to_mmap_block(start_ref);
    if (aligned_ref != m_base_ref)
        return false;
    size_t window_size = get_window_size(f, start_ref, size);
    // FIXME: Add a remap which will work with a offset different from 0
    m_map.unmap();
    m_map.map(f, File::access_ReadWrite, window_size, 0, m_base_ref);
    return true;
}

GroupWriter::MapWindow::MapWindow(size_t alignment, util::File& f, ref_type start_ref, size_t size)
    : m_alignment(alignment)
{
    m_base_ref = aligned_to_mmap_block(start_ref);
    size_t window_size = get_window_size(f, start_ref, size);
    m_map.map(f, File::access_ReadWrite, window_size, 0, m_base_ref);
}

GroupWriter::MapWindow::~MapWindow()
{
    m_map.unmap(); /* Apparently no effect - how odd */
}

void GroupWriter::MapWindow::sync()
{
    m_map.sync();
}

char* GroupWriter::MapWindow::translate(ref_type ref)
{
    return m_map.get_addr() + (ref - m_base_ref);
}

void GroupWriter::MapWindow::encryption_read_barrier(void* start_addr, size_t size)
{
    realm::util::encryption_read_barrier(start_addr, size, m_map.get_encrypted_mapping());
}

void GroupWriter::MapWindow::encryption_write_barrier(void* start_addr, size_t size)
{
    realm::util::encryption_write_barrier(start_addr, size, m_map.get_encrypted_mapping());
}


GroupWriter::GroupWriter(Group& group, Durability dura)
    : m_group(group)
    , m_alloc(group.m_alloc)
    , m_free_positions(m_alloc)
    , m_free_lengths(m_alloc)
    , m_free_versions(m_alloc)
    , m_durability(dura)
{
    m_map_windows.reserve(num_map_windows);
#if REALM_IOS
    m_window_alignment = 1 * 1024 * 1024;  // 1M
#else
    if (sizeof(int*) == 4) { // 32 bit address space
        m_window_alignment = 1 * 1024 * 1024; // 1M
    } else {
        // large address space - just choose a size so that we have a single window
        size_t total_size = m_alloc.get_total_size();
        size_t wanted_size = 1;
        while (total_size) { total_size >>= 1; wanted_size <<= 1; }
        if (wanted_size < 1 * 1024 * 1024)
            wanted_size = 1 * 1024 * 1024; // minimum 1M
        m_window_alignment = wanted_size;
    }
#endif
    Array& top = m_group.m_top;
    bool is_shared = m_group.m_is_shared;

    m_free_positions.set_parent(&top, 3);
    m_free_lengths.set_parent(&top, 4);
    m_free_versions.set_parent(&top, 5);

    // Expand top array from 3 to 5 elements. Only Realms written using
    // Group::write() are allowed to have less than 5 elements.
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
        REALM_ASSERT_RELEASE_EX(m_free_positions.size() == m_free_lengths.size(), top.get_ref(),
                                m_free_positions.size(), m_free_lengths.size());
    }
    else {
        m_free_lengths.create(Array::type_Normal); // Throws
        _impl::DestroyGuard<ArrayInteger> dg(&m_free_lengths);
        m_free_lengths.update_parent(); // Throws
        dg.release();
    }

    if (is_shared) {
        SharedGroup::version_type initial_version = 0;

        // Expand top array from 5 to 7 elements. Only nonshared Realms are
        // allowed to have less than 7 elements.
        if (top.size() < 7) {
            REALM_ASSERT(top.size() == 5);
            // m_free_versions
            top.add(0); // Throws
            // Transaction number / version
            top.add(0); // Throws
        }

        if (ref_type ref = m_free_versions.get_ref_from_parent()) {
            m_free_versions.init_from_ref(ref);
            REALM_ASSERT_RELEASE_EX(m_free_versions.size() == m_free_lengths.size(), top.get_ref(),
                                    m_free_versions.size(), m_free_lengths.size());
        }
        else {
            int_fast64_t value = int_fast64_t(initial_version); // FIXME: Problematic unsigned -> signed conversion
            top.set(6, 1 + 2 * uint64_t(initial_version));      // Throws
            size_t n = m_free_positions.size();
            bool context_flag = false;
            m_free_versions.Array::create(Array::type_Normal, context_flag, n, value); // Throws
            _impl::DestroyGuard<ArrayInteger> dg(&m_free_versions);
            m_free_versions.update_parent(); // Throws
            dg.release();
        }
    }
    else { // !is_shared
        // Discard free-space versions and history information.
        if (top.size() > 5) {
            REALM_ASSERT(top.size() >= 7);
            top.truncate_and_destroy_children(5);
        }
    }
}

GroupWriter::~GroupWriter() = default;

size_t GroupWriter::get_file_size() const noexcept
{
    return to_size_t(m_alloc.get_file().get_size());
}

void GroupWriter::sync_all_mappings()
{
    if (m_durability == Durability::Unsafe)
        return;
    for (const auto& window : m_map_windows) {
        window->sync();
    }
}

// Get a window matching a request, either creating a new window or reusing an
// existing one (possibly extended to accomodate the new request). Maintain a
// cache of open windows which are sync'ed and closed following a least recently
// used policy. Entries in the cache are kept in MRU order.
GroupWriter::MapWindow* GroupWriter::get_window(ref_type start_ref, size_t size)
{
    auto match = std::find_if(m_map_windows.begin(), m_map_windows.end(), [=](auto& window) {
        return window->matches(start_ref, size) || window->extends_to_match(m_alloc.get_file(), start_ref, size);
    });
    if (match != m_map_windows.end()) {
        // move matching window to top (to keep LRU order)
        std::rotate(m_map_windows.begin(), match, match + 1);
        return m_map_windows[0].get();
    }
    // no window found, make room for a new one at the top
    if (m_map_windows.size() == num_map_windows) {
        if (m_durability != Durability::Unsafe)
            m_map_windows.back()->sync();
        m_map_windows.pop_back();
    }
    auto new_window = std::make_unique<MapWindow>(m_window_alignment, m_alloc.get_file(), start_ref, size);
    m_map_windows.insert(m_map_windows.begin(), std::move(new_window));
    return m_map_windows[0].get();
}

#define REALM_ALLOC_DEBUG 0

ref_type GroupWriter::write_group()
{
    bool is_shared = m_group.m_is_shared;
#if REALM_METRICS
    std::unique_ptr<MetricTimer> fsync_timer = Metrics::report_write_time(m_group);
#endif // REALM_METRICS

#if REALM_ALLOC_DEBUG
    std::cout << "Commit nr " << m_current_version << "   ( from " << (is_shared ? m_readlock_version : 0) << " )"
              << std::endl;
    std::cout << "    In-file freelist before merge: " << m_free_positions.size();
#endif

    read_in_freelist();
    // Now, 'm_size_map' holds all free elements candidate for recycling

    Array& top = m_group.m_top;
#if REALM_ALLOC_DEBUG
    std::cout << "    In-file freelist after merge:  " << m_size_map.size() << std::endl;
    std::cout << "    Allocating file space for data:" << std::endl;
#endif

    // Recursively write all changed arrays (but not 'top' and free-lists yet,
    // as they are going to change along the way.) If free space is available in
    // the attached database file, we use it, but this does not include space
    // that has been release during the current transaction (or since the last
    // commit), as that would lead to clobbering of the previous database
    // version.
    bool deep = true, only_if_modified = true;
    ref_type names_ref = m_group.m_table_names.write(*this, deep, only_if_modified); // Throws
    ref_type tables_ref = m_group.m_tables.write(*this, deep, only_if_modified);     // Throws

    int_fast64_t value_1 = from_ref(names_ref);
    int_fast64_t value_2 = from_ref(tables_ref);
    top.set(0, value_1); // Throws
    top.set(1, value_2); // Throws

    // If file has a history and is opened in shared mode, write the new history
    // to the file. If the file has a history, but si not opened in shared mode,
    // discard the history, as it could otherwise be left in an inconsistent state.
    if (top.size() >= 8) {
        REALM_ASSERT(top.size() >= 10);
        // In nonshared mode, history must already have been discarded by GroupWriter constructor.
        REALM_ASSERT(is_shared);
        if (ref_type history_ref = top.get_as_ref(8)) {
            Allocator& alloc = top.get_alloc();
            ref_type new_history_ref = Array::write(history_ref, alloc, *this, only_if_modified); // Throws
            int_fast64_t value_3 = from_ref(new_history_ref);
            top.set(8, value_3); // Throws
        }
    }

#if REALM_ALLOC_DEBUG
    std::cout << "    Freelist size after allocations: " << m_size_map.size() << std::endl;
#endif

    // We now have a bit of a chicken-and-egg problem. We need to write the
    // free-lists to the file, but the act of writing them will consume free
    // space, and thereby change the free-lists. To solve this problem, we
    // calculate an upper bound on the amount af space required for all of the
    // remaining arrays and allocate the space as one big chunk. This way we can
    // finalize the free-lists before writing them to the file.
    size_t max_free_list_size = m_size_map.size();

    // We need to add to the free-list any space that was freed during the
    // current transaction, but to avoid clobering the previous version, we
    // cannot add it yet. Instead we simply account for the space
    // required. Since we will modify the free-lists themselves, we must ensure
    // that the original arrays used by the free-lists are counted as part of
    // the space that was freed during the current transaction. Note that a
    // copy-on-write on m_free_positions, for example, also implies a
    // copy-on-write on Group::m_top.
#if REALM_ALLOC_DEBUG
    std::cout << "        In-mem freelist before/after consolidation: " << m_group.m_alloc.m_free_read_only.size();
#endif
    size_t free_read_only_size = m_group.m_alloc.consolidate_free_read_only(); // Throws
#if REALM_ALLOC_DEBUG
    std::cout << "/" << free_read_only_size << std::endl;
#endif
    max_free_list_size += free_read_only_size;
    max_free_list_size += m_not_free_in_file.size();
    // The final allocation of free space (i.e., the call to
    // reserve_free_space() below) may add extra entries to the free-lists.
    // We reserve room for the worst case scenario, which is as follows:
    // If the database has *max* theoretical fragmentation, it'll need one
    // entry in the free list for every 16 bytes, because both allocated and
    // free chunks are at least 8 bytes in size. For databases smaller than 2Gb
    // each free list entry requires 16 bytes (4 for the position, 4 for the
    // size and 8 for the version). The worst case scenario thus needs access
    // to a contiguous address range equal to existing database size.
    // This growth requires at the most 8 extension steps, each adding one entry
    // to the free list. The worst case occurs when you will have to expand the
    // size to over 2 GB where each entry suddenly requires 24 bytes. In this
    // case you will need 2 extra steps.
    // Another limit is due to the fact than an array holds less than 0x1000000
    // entries, so the total free list size will be less than 0x16000000. So for
    // bigger databases the space required for free lists will be relatively less.
    max_free_list_size += 10;

    // If current size is less than 128 MB, the database need not expand above 2 GB
    // which means that the positions and sizes can still be in 32 bit.
    int size_per_entry = ((top.get(2) >> 1) < 0x8000000 ? 8 : 16) + (is_shared ? 8 : 0);
    size_t max_free_space_needed = Array::get_max_byte_size(top.size()) + size_per_entry * max_free_list_size;

#if REALM_ALLOC_DEBUG
    std::cout << "    Allocating file space for freelists:" << std::endl;
#endif
    // Reserve space for remaining arrays. We ask for some extra bytes beyond the
    // maximum number that is required. This ensures that even if we end up
    // using the maximum size possible, we still do not end up with a zero size
    // free-space chunk as we deduct the actually used size from it.
    auto reserve = reserve_free_space(max_free_space_needed + 8); // Throws
    size_t reserve_pos = reserve->second;
    size_t reserve_size = reserve->first;

    // At this point we have allocated all the space we need, so we can add to
    // the free-lists any free space created during the current transaction (or
    // since last commit). Had we added it earlier, we would have risked
    // clobbering the previous database version. Note, however, that this risk
    // would only have been present in the non-transactional case where there is
    // no version tracking on the free-space chunks.

    // Now, let's update the realm-style freelists, which will later be written to file.
    // Function returns index of element holding the space reserved for the free
    // lists in the file.
    size_t reserve_ndx = recreate_freelist(reserve_pos);

#if REALM_ALLOC_DEBUG
    std::cout << "    Freelist size after merge: " << m_free_positions.size()
              << "   freelist space required: " << max_free_space_needed << std::endl << std::endl;
#endif
    // Before we calculate the actual sizes of the free-list arrays, we must
    // make sure that the final adjustments of the free lists (i.e., the
    // deduction of the actually used space from the reserved chunk,) will not
    // change the byte-size of those arrays.
    // size_t reserve_pos = to_size_t(m_free_positions.get(reserve_ndx));
    REALM_ASSERT_3(reserve_size, >, max_free_space_needed);
    int_fast64_t value_4 = to_int64(reserve_pos + max_free_space_needed);

#if REALM_ENABLE_MEMDEBUG
    m_free_positions.m_no_relocation = true;
    m_free_lengths.m_no_relocation = true;
#endif

    // Ensure that this arrays does not reposition itself
    m_free_positions.ensure_minimum_width(value_4); // Throws

    // Get final sizes of free-list arrays
    size_t free_positions_size = m_free_positions.get_byte_size();
    size_t free_sizes_size = m_free_lengths.get_byte_size();
    size_t free_versions_size = is_shared ? m_free_versions.get_byte_size() : 0;
    REALM_ASSERT(!is_shared ||
                 Array::get_wtype_from_header(Array::get_header_from_data(m_free_versions.m_data)) ==
                     Array::wtype_Bits);

    // Calculate write positions
    ref_type reserve_ref = to_ref(reserve_pos);
    ref_type free_positions_ref = reserve_ref;
    ref_type free_sizes_ref = free_positions_ref + free_positions_size;
    ref_type free_versions_ref = free_sizes_ref + free_sizes_size;
    ref_type top_ref = free_versions_ref + free_versions_size;

    // Update top to point to the calculated positions
    int_fast64_t value_5 = from_ref(free_positions_ref);
    int_fast64_t value_6 = from_ref(free_sizes_ref);
    top.set(3, value_5); // Throws
    top.set(4, value_6); // Throws
    if (is_shared) {
        int_fast64_t value_7 = from_ref(free_versions_ref);
        int_fast64_t value_8 =
            1 + 2 * int_fast64_t(m_current_version); // FIXME: Problematic unsigned -> signed conversion
        top.set(5, value_7);                         // Throws
        top.set(6, value_8);                         // Throws
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
    int_fast64_t value_8 = from_ref(end_ref);
    int_fast64_t value_9 = to_int64(rest);

    // value_9 is guaranteed to be smaller than the existing entry in the array and hence will not cause bit expansion
    REALM_ASSERT_3(value_8, <=, Array::ubound_for_width(m_free_positions.get_width()));
    REALM_ASSERT_3(value_9, <=, Array::ubound_for_width(m_free_lengths.get_width()));

    m_free_positions.set(reserve_ndx, value_8); // Throws
    m_free_lengths.set(reserve_ndx, value_9);   // Throws
    m_free_space_size += rest;

    // The free-list now have their final form, so we can write them to the file
    // char* start_addr = m_file_map.get_addr() + reserve_ref;
    MapWindow* window = get_window(reserve_ref, end_ref - reserve_ref);
    char* start_addr = window->translate(reserve_ref);
    window->encryption_read_barrier(start_addr, used);
    write_array_at(window, free_positions_ref, m_free_positions.get_header(), free_positions_size); // Throws
    write_array_at(window, free_sizes_ref, m_free_lengths.get_header(), free_sizes_size);           // Throws
    if (is_shared) {
        write_array_at(window, free_versions_ref, m_free_versions.get_header(), free_versions_size); // Throws
    }

    // Write top
    write_array_at(window, top_ref, top.get_header(), top_byte_size); // Throws
    window->encryption_write_barrier(start_addr, used);
    // Return top_ref so that it can be saved in lock file used for coordination
    return top_ref;
}


void GroupWriter::read_in_freelist()
{
    FreeList free_in_file;

    bool is_shared = m_group.m_is_shared;
    size_t limit = m_free_lengths.size();
    REALM_ASSERT_RELEASE_EX(m_free_positions.size() == limit, limit, m_free_positions.size());
    REALM_ASSERT_RELEASE_EX(!is_shared || m_free_versions.size() == limit, limit, m_free_versions.size());

    if (limit) {
        auto limit_version = is_shared ? m_readlock_version : 0;
        for (size_t idx = 0; idx < limit; ++idx) {
            size_t ref = size_t(m_free_positions.get(idx));
            size_t size = size_t(m_free_lengths.get(idx));

            if (is_shared) {
                uint64_t version = m_free_versions.get(idx);
                // Entries that are freed in still alive versions are not candidates for merge or allocation
                if (version >= limit_version) {
                    m_not_free_in_file.emplace_back(ref, size, version);
                    continue;
                }
            }

            free_in_file.emplace_back(ref, size, 0);
        }

        // This will imply a copy-on-write
        m_free_positions.clear();
        m_free_lengths.clear();
        if (is_shared)
            m_free_versions.clear();
    }
    else {
        // We need to free the space occupied by the free lists
        // If the lists are empty, this has to be done explicitly
        // as clear would not copy-on-write an empty array.
        m_free_positions.copy_on_write();
        m_free_lengths.copy_on_write();
        if (is_shared)
            m_free_versions.copy_on_write();
    }

    free_in_file.merge_adjacent_entries_in_freelist();
    // Previous step produces - potentially - some entries with size of zero. These
    // entries will be skipped in the next step.
    free_in_file.move_free_in_file_to_size_map(m_size_map);
}

size_t GroupWriter::recreate_freelist(size_t reserve_pos)
{
    std::vector<FreeSpaceEntry> free_in_file;
    auto& new_free_space = m_group.m_alloc.get_free_read_only(); // Throws
    auto nb_elements = m_size_map.size() + m_not_free_in_file.size() + new_free_space.size();
    free_in_file.reserve(nb_elements);

    size_t reserve_ndx = realm::npos;
    bool is_shared = m_group.m_is_shared;

    for (const auto& entry : m_size_map) {
        free_in_file.emplace_back(entry.second, entry.first, 0);
    }

    {
        size_t locked_space_size = 0;
        REALM_ASSERT_RELEASE(m_not_free_in_file.empty() || is_shared);
        for (const auto& locked : m_not_free_in_file) {
            free_in_file.emplace_back(locked.ref, locked.size, locked.released_at_version);
            locked_space_size += locked.size;
        }

        for (const auto& free_space : new_free_space) {
            free_in_file.emplace_back(free_space.first, free_space.second, m_current_version);
            locked_space_size += free_space.second;
        }
        m_locked_space_size = locked_space_size;
    }

    REALM_ASSERT(free_in_file.size() == nb_elements);
    std::sort(begin(free_in_file), end(free_in_file), [](auto& a, auto& b) { return a.ref < b.ref; });

    {
        // Copy into arrays while checking consistency
        size_t prev_ref = 0;
        size_t prev_size = 0;
        size_t free_space_size = 0;
        auto limit = free_in_file.size();
        for (size_t i = 0; i < limit; ++i) {
            const auto& free_space = free_in_file[i];
            auto ref = free_space.ref;
            REALM_ASSERT_RELEASE_EX(prev_ref + prev_size <= ref, prev_ref, prev_size, ref, i, limit);
            if (reserve_pos == ref) {
                reserve_ndx = i;
            }
            else {
                // The reserved chunk should not be counted in now. We don't know how much of it
                // will eventually be used.
                free_space_size += free_space.size;
            }
            m_free_positions.add(free_space.ref);
            m_free_lengths.add(free_space.size);
            if (is_shared)
                m_free_versions.add(free_space.released_at_version);
            prev_ref = free_space.ref;
            prev_size = free_space.size;
        }
        REALM_ASSERT_RELEASE(reserve_ndx != realm::npos);

        m_free_space_size = free_space_size;
    }

    return reserve_ndx;
}

void GroupWriter::FreeList::merge_adjacent_entries_in_freelist()
{
    if (size() > 1) {
        // Combine any adjacent chunks in the freelist
        auto prev = begin();
        for (auto it = begin() + 1; it != end(); ++it) {
            REALM_ASSERT(it->ref > prev->ref);
            if (prev->ref + prev->size == it->ref) {
                prev->size += it->size;
                it->size = 0;
            }
            else {
                prev = it;
            }
        }
    }
}

void GroupWriter::FreeList::move_free_in_file_to_size_map(std::multimap<size_t, size_t>& size_map)
{
    for (auto& elem : *this) {
        // Skip elements merged in 'merge_adjacent_entries_in_freelist'
        if (elem.size) {
            REALM_ASSERT_RELEASE_EX(!(elem.size & 7), elem.size);
            REALM_ASSERT_RELEASE_EX(!(elem.ref & 7), elem.ref);
            size_map.emplace(elem.size, elem.ref);
        }
    }
}

size_t GroupWriter::get_free_space(size_t size)
{
    REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment

    auto p = reserve_free_space(size);

    // Claim space from identified chunk
    size_t chunk_pos = p->second;
    size_t chunk_size = p->first;
    REALM_ASSERT_3(chunk_size, >=, size);
    REALM_ASSERT_RELEASE_EX(!(chunk_pos & 7), chunk_pos);
    REALM_ASSERT_RELEASE_EX(!(chunk_size & 7), chunk_size);

    size_t rest = chunk_size - size;
    m_size_map.erase(p);
    if (rest > 0) {
        // Allocating part of chunk - this alway happens from the beginning
        // of the chunk. The call to reserve_free_space may split chunks
        // in order to make sure that it returns a chunk from which allocation
        // can be done from the beginning
        m_size_map.emplace(rest, chunk_pos + size);
    }
    return chunk_pos;
}


inline GroupWriter::FreeListElement GroupWriter::split_freelist_chunk(FreeListElement it, size_t alloc_pos)
{
    size_t start_pos = it->second;
    size_t chunk_size = it->first;
    m_size_map.erase(it);
    REALM_ASSERT_RELEASE_EX(alloc_pos > start_pos, alloc_pos, start_pos);

    REALM_ASSERT_RELEASE_EX(!(alloc_pos & 7), alloc_pos);
    size_t size_first = alloc_pos - start_pos;
    size_t size_second = chunk_size - size_first;
    m_size_map.emplace(size_first, start_pos);
    return m_size_map.emplace(size_second, alloc_pos);
}

GroupWriter::FreeListElement GroupWriter::search_free_space_in_free_list_element(FreeListElement it, size_t size)
{
    SlabAlloc& alloc = m_group.m_alloc;
    size_t chunk_size = it->first;

    // search through the chunk, finding a place within it,
    // where an allocation will not cross a mmap boundary
    size_t start_pos = it->second;
    size_t alloc_pos = alloc.find_section_in_range(start_pos, chunk_size, size);
    if (alloc_pos == 0) {
        return m_size_map.end();
    }
    // we found a place - if it's not at the beginning of the chunk,
    // we split the chunk so that the allocation can be done from the
    // beginning of the second chunk.
    if (alloc_pos != start_pos) {
        it = split_freelist_chunk(it, alloc_pos);
    }
    // Match found!
    return it;
}

GroupWriter::FreeListElement GroupWriter::search_free_space_in_part_of_freelist(size_t size)
{
    auto it = m_size_map.lower_bound(size);
    while (it != m_size_map.end()) {
        // Accept either a perfect match or a block that is twice the size. Tests have shown
        // that this is a good strategy.
        if (it->first == size || it->first >= 2 * size) {
            auto ret = search_free_space_in_free_list_element(it, size);
            if (ret != m_size_map.end()) {
                return ret;
            }
            ++it;
        }
        else {
            // If block was too small, search for the first that is at least twice as big.
            it = m_size_map.lower_bound(2 * size);
        }
    }
    // No match
    return m_size_map.end();
}


GroupWriter::FreeListElement GroupWriter::reserve_free_space(size_t size)
{
    auto chunk = search_free_space_in_part_of_freelist(size);
    while (chunk == m_size_map.end()) {
        // No free space, so we have to extend the file.
        auto new_chunk = extend_free_space(size);
        chunk = search_free_space_in_free_list_element(new_chunk, size);
    }
    return chunk;
}

// Extend the free space with at least the requested size.
// Due to mmap constraints, the extension can not be guaranteed to
// allow an allocation of the requested size, so multiple calls to
// extend_free_space may be needed, before an allocation can succeed.
GroupWriter::FreeListElement GroupWriter::extend_free_space(size_t requested_size)
{
    SlabAlloc& alloc = m_group.m_alloc;

    // We need to consider the "logical" size of the file here, and not the real
    // size. The real size may have changed without the free space information
    // having been adjusted accordingly. This can happen, for example, if
    // write_group() fails before writing the new top-ref, but after having
    // extended the file size. It can also happen as part of initial file expansion
    // during attach_file().
    size_t logical_file_size = to_size_t(m_group.m_top.get(2) / 2);
    size_t new_file_size = logical_file_size;
    if (REALM_UNLIKELY(int_add_with_overflow_detect(new_file_size, requested_size))) {
        throw MaximumFileSizeExceeded("GroupWriter cannot extend free space: " + util::to_string(logical_file_size)
                                      + " + " + util::to_string(requested_size));
    }

    if (!alloc.matches_section_boundary(new_file_size)) {
        new_file_size = alloc.get_upper_section_boundary(new_file_size);
    }
    // The size must be a multiple of 8. This is guaranteed as long as
    // the initial size is a multiple of 8.
    REALM_ASSERT_RELEASE_EX(!(new_file_size & 7), new_file_size);
    REALM_ASSERT_3(logical_file_size, <, new_file_size);

    // Note: resize_file() will call File::prealloc() which may misbehave under
    // race conditions (see documentation of File::prealloc()). Fortunately, no
    // race conditions can occur, because in transactional mode we hold a write
    // lock at this time, and in non-transactional mode it is the responsibility
    // of the user to ensure non-concurrent file mutation.
    m_alloc.resize_file(new_file_size); // Throws
#if REALM_ALLOC_DEBUG
    std::cout << "        ** File extension to " << new_file_size << "     after request for " << requested_size
              << std::endl;
#endif
    //    m_file_map.remap(m_alloc.get_file(), File::access_ReadWrite, new_file_size); // Throws

    size_t chunk_size = new_file_size - logical_file_size;
    REALM_ASSERT_RELEASE_EX(!(chunk_size & 7), chunk_size);
    REALM_ASSERT_RELEASE(chunk_size != 0);
    auto it = m_size_map.emplace(chunk_size, logical_file_size);

    // Update the logical file size
    m_group.m_top.set(2, 1 + 2 * uint64_t(new_file_size)); // Throws

    return it;
}

bool inline is_aligned(char* addr) {
    size_t as_binary = reinterpret_cast<size_t>(addr);
    return (as_binary & 7) == 0;
}

ref_type GroupWriter::write_array(const char* data, size_t size, uint32_t checksum)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);

    // Write the block
    MapWindow* window = get_window(pos, size);
    char* dest_addr = window->translate(pos);
    REALM_ASSERT_RELEASE(is_aligned(dest_addr));
    window->encryption_read_barrier(dest_addr, size);
    memcpy(dest_addr, &checksum, 4);
    memcpy(dest_addr + 4, data + 4, size - 4);

    window->encryption_write_barrier(dest_addr, size);
    // return ref of the written array
    ref_type ref = to_ref(pos);
    return ref;
}


void GroupWriter::write_array_at(MapWindow* window, ref_type ref, const char* data, size_t size)
{
    size_t pos = size_t(ref);

    REALM_ASSERT_3(pos + size, <=, to_size_t(m_group.m_top.get(2) / 2));
    // REALM_ASSERT_3(pos + size, <=, m_file_map.get_size());
    char* dest_addr = window->translate(pos);
    REALM_ASSERT_RELEASE(is_aligned(dest_addr));
 
    uint32_t dummy_checksum = 0x41414141UL; // "AAAA" in ASCII
    memcpy(dest_addr, &dummy_checksum, 4);
    memcpy(dest_addr + 4, data + 4, size - 4);
}


void GroupWriter::commit(ref_type new_top_ref)
{
    MapWindow* window = get_window(0, sizeof(SlabAlloc::Header));
    SlabAlloc::Header& file_header = *reinterpret_cast<SlabAlloc::Header*>(window->translate(0));
    window->encryption_read_barrier(&file_header, sizeof file_header);

    // One bit of the flags field selects which of the two top ref slots are in
    // use (same for file format version slots). The current value of the bit
    // reflects the currently bound snapshot, so we need to invert it for the
    // new snapshot. Other bits must remain unchanged.
    unsigned old_flags = file_header.m_flags;
    unsigned new_flags = old_flags ^ SlabAlloc::flags_SelectBit;
    int slot_selector = ((new_flags & SlabAlloc::flags_SelectBit) != 0 ? 1 : 0);

    // Update top ref and file format version
    int file_format_version = m_group.get_file_format_version();
    using type_1 = std::remove_reference<decltype(file_header.m_file_format[0])>::type;
    REALM_ASSERT(!util::int_cast_has_overflow<type_1>(file_format_version));
    file_header.m_top_ref[slot_selector] = new_top_ref;
    file_header.m_file_format[slot_selector] = type_1(file_format_version);

    // When running the test suite, device synchronization is disabled
    bool disable_sync = get_disable_sync_to_disk() || m_durability == Durability::Unsafe;

#if REALM_METRICS
    std::unique_ptr<MetricTimer> fsync_timer = Metrics::report_fsync_time(m_group);
#endif // REALM_METRICS

    // Make sure that that all data relating to the new snapshot is written to
    // stable storage before flipping the slot selector
    window->encryption_write_barrier(&file_header, sizeof file_header);
    if (!disable_sync)
        sync_all_mappings();

    // Flip the slot selector bit.
    using type_2 = std::remove_reference<decltype(file_header.m_flags)>::type;
    file_header.m_flags = type_2(new_flags);

    // Write new selector to disk
    // FIXME: we might optimize this to write of a single page?
    window->encryption_write_barrier(&file_header, sizeof file_header);
    if (!disable_sync)
        window->sync();
}


#ifdef REALM_DEBUG

void GroupWriter::dump()
{
    bool is_shared = m_group.m_is_shared;

    size_t count = m_free_lengths.size();
    std::cout << "count: " << count << ", m_size = " << m_alloc.get_file().get_size() << ", "
              << "version >= " << m_readlock_version << "\n";
    if (!is_shared) {
        for (size_t i = 0; i < count; ++i) {
            std::cout << i << ": " << m_free_positions.get(i) << ", " << m_free_lengths.get(i) << "\n";
        }
    }
    else {
        for (size_t i = 0; i < count; ++i) {
            std::cout << i << ": " << m_free_positions.get(i) << ", " << m_free_lengths.get(i) << " - "
                      << m_free_versions.get(i) << "\n";
        }
    }
}

#endif
