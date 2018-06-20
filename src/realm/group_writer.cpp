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
    MapWindow(util::File& f, ref_type start_ref, size_t size);
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
    util::File::Map<char> map;
    ref_type base_ref;
    ref_type aligned_to_mmap_block(ref_type start_ref);
    size_t get_window_size(util::File& f, ref_type start_ref, size_t size);
    static const size_t intended_alignment = 0x100000; // 1MB
};

// True if a requested block fall within a memory mapping.
bool GroupWriter::MapWindow::matches(ref_type start_ref, size_t size)
{
    if (start_ref < base_ref)
        return false;
    if (start_ref + size > base_ref + map.get_size())
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
    size_t page_mask = intended_alignment - 1;
    return start_ref & ~page_mask;
}

size_t GroupWriter::MapWindow::get_window_size(util::File& f, ref_type start_ref, size_t size)
{
    size_t window_size = start_ref + size - base_ref;
    // always map at least 1MB
    if (window_size < intended_alignment)
        window_size = intended_alignment;
    // but never map beyond end of file
    size_t file_size = to_size_t(f.get_size());
    REALM_ASSERT_DEBUG_EX(start_ref + size <= file_size, start_ref + size, file_size);
    if (window_size > file_size - base_ref)
        window_size = file_size - base_ref;
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
    if (aligned_ref != base_ref)
        return false;
    size_t window_size = get_window_size(f, start_ref, size);
    // FIXME: Add a remap which will work with a offset different from 0
    map.unmap();
    map.map(f, File::access_ReadWrite, window_size, 0, base_ref);
    return true;
}

GroupWriter::MapWindow::MapWindow(util::File& f, ref_type start_ref, size_t size)
{
    base_ref = aligned_to_mmap_block(start_ref);
    size_t window_size = get_window_size(f, start_ref, size);
    map.map(f, File::access_ReadWrite, window_size, 0, base_ref);
}

GroupWriter::MapWindow::~MapWindow()
{
}

void GroupWriter::MapWindow::sync()
{
    map.sync();
}

char* GroupWriter::MapWindow::translate(ref_type ref)
{
    return map.get_addr() + (ref - base_ref);
}

void GroupWriter::MapWindow::encryption_read_barrier(void* start_addr, size_t size)
{
    realm::util::encryption_read_barrier(start_addr, size, map.get_encrypted_mapping());
}

void GroupWriter::MapWindow::encryption_write_barrier(void* start_addr, size_t size)
{
    realm::util::encryption_write_barrier(start_addr, size, map.get_encrypted_mapping());
}


GroupWriter::GroupWriter(Group& group)
    : m_group(group)
    , m_alloc(group.m_alloc)
    , m_free_positions(m_alloc)
    , m_free_lengths(m_alloc)
    , m_free_versions(m_alloc)
    , m_current_version(0)
    , m_alloc_position(0)
{
    m_map_windows.reserve(num_map_windows);

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
        m_map_windows.back()->sync();
        m_map_windows.pop_back();
    }
    auto new_window = std::make_unique<MapWindow>(m_alloc.get_file(), start_ref, size);
    m_map_windows.insert(m_map_windows.begin(), std::move(new_window));
    return m_map_windows[0].get();
}

#define REALM_ALLOC_DEBUG 0

ref_type GroupWriter::write_group()
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> fsync_timer = Metrics::report_write_time(m_group);
#endif // REALM_METRICS

#if REALM_ALLOC_DEBUG
    std::cout << "Commit nr " << m_current_version << "   ( from " << m_readlock_version << " )" << std::endl;
    std::cout << "    In-file freelist before merge: " << m_free_positions.size();
#endif
    read_in_freelist();
    merge_free_space(); // Throws
    // now recreate the freelist for allocation. Note that the blocks not free yet are left
    // out and merged in later. No allocation can happen from these blocks anyway.
    recreate_freelist();
#if REALM_ALLOC_DEBUG
    std::cout << "    In-file freelist after merge:  " << m_free_positions.size() << std::endl;
    std::cout << "    Allocating file space for data:" << std::endl;
#endif
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
    ref_type names_ref = m_group.m_table_names.write(*this, deep, only_if_modified); // Throws
    ref_type tables_ref = m_group.m_tables.write(*this, deep, only_if_modified);     // Throws

    int_fast64_t value_1 = from_ref(names_ref);
    int_fast64_t value_2 = from_ref(tables_ref);
    top.set(0, value_1); // Throws
    top.set(1, value_2); // Throws

    // If file has a history and is opened in shared mode, write the new history
    // to the file. If the file has a history, but si not opened in shared mode,
    // discard the history, as it could otherwise be left in an inconsisten
    // state.
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
    std::cout << "    Allocating file space for freelists:" << std::endl;
#endif

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
    m_free_lengths.copy_on_write();   // Throws
    if (is_shared)
        m_free_versions.copy_on_write();                                            // Throws
#if REALM_ALLOC_DEBUG
    std::cout << "        In-mem freelist before consolidation: " << m_group.m_alloc.m_free_read_only.size();
#endif
    m_group.m_alloc.consolidate_free_read_only();                                   // Throws
#if REALM_ALLOC_DEBUG
    std::cout << "    In-mem freelist after consolidation:  " << m_group.m_alloc.m_free_read_only.size() << std::endl;
#endif
    const SlabAlloc::chunks& new_free_space = m_group.m_alloc.get_free_read_only(); // Throws
    max_free_list_size += new_free_space.size();
    max_free_list_size += m_not_free_in_file.size();
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
    size_t max_free_space_needed =
        Array::get_max_byte_size(top.size()) +
        num_free_lists * Array::get_max_byte_size(max_free_list_size);

    // Reserve space for remaining arrays. We ask for one extra byte beyond the
    // maximum number that is required. This ensures that even if we end up
    // using the maximum size possible, we still do not end up with a zero size
    // free-space chunk as we deduct the actually used size from it.
    std::pair<size_t, size_t> reserve = reserve_free_space(max_free_space_needed + 1); // Throws
    size_t reserve_ndx = reserve.first;
    size_t reserve_size = reserve.second;
    // At this point we have allocated all the space we need, so we can add to
    // the free-lists any free space created during the current transaction (or
    // since last commit). Had we added it earlier, we would have risked
    // clobering the previous database version. Note, however, that this risk
    // would only have been present in the non-transactional case where there is
    // no version tracking on the free-space chunks.
#if REALM_ALLOC_DEBUG
    std::cout << "    Freelist size after allocations: " << m_free_positions.size() << std::endl;
#endif
    // re-read in the freelist after allocation - we need it for a later check.
    // from here on m_free_in_file acts as a temporary, collecting all chunks for
    // a final validity check.
    read_in_freelist();
    // all entries should end up in the "real" free list (not the not-yet-free list)
    REALM_ASSERT_RELEASE(m_free_in_file.size() == m_free_positions.size());
    // Now, let's update the realm-style freelists, which will later be written to file.
    // do not preserve order of freelist, just append new blocks. Order will be restored
    // anyway by the merge_free_lists in the start of next commit.
    for (const auto& free_space : m_not_free_in_file) {
        m_free_in_file.push_back(free_space); // not actually freed, but added for a later check
        m_free_positions.add(free_space.ref);
        m_free_lengths.add(free_space.size);
        if (is_shared)
            m_free_versions.add(free_space.released_at_version);
    }
    for (const auto& free_space : new_free_space) {
        // not actually freed, but added for a later check
        FreeSpaceEntry entry;
        entry.ref = free_space.ref;
        entry.size = free_space.size;
        entry.released_at_version = m_current_version;
        m_free_in_file.push_back(entry);
        m_free_positions.add(free_space.ref);
        m_free_lengths.add(free_space.size);
        if (is_shared)
            m_free_versions.add(m_current_version);
    }
    // freelist is now back in arrays in form ready to be written. Copy of freelist is also
    // available in m_free_in_file, so let's use it for a consistency check:
    sort_freelist(); // requires sorting.
    // Verify that no block covers part of it's neighbour:
    auto limit = m_free_in_file.size();
    for (size_t i = 1; i < limit; ++i) {
        auto prev_ref = m_free_in_file[i-1].ref;
        auto prev_size = m_free_in_file[i-1].size;
        auto ref = m_free_in_file[i].ref;
        REALM_ASSERT_RELEASE_EX(prev_ref + prev_size <= ref, prev_ref, prev_size, ref, i, limit);
    }
#if REALM_ALLOC_DEBUG
    std::cout << "    Freelist size after merge: " << m_free_positions.size()
              << "   freelist space required: " << max_free_space_needed << std::endl;
#endif
    // Before we calculate the actual sizes of the free-list arrays, we must
    // make sure that the final adjustments of the free lists (i.e., the
    // deduction of the actually used space from the reserved chunk,) will not
    // change the byte-size of those arrays.
    size_t reserve_pos = to_size_t(m_free_positions.get(reserve_ndx));
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

size_t GroupWriter::get_free_space() {
    if (m_free_lengths.is_attached()) {
        size_t sum = 0;
        for (size_t j = 0; j < m_free_lengths.size(); ++j) {
            sum += to_size_t(m_free_lengths.get(j));
        }
        return sum;
    } else {
        return 0;
    }
}

void GroupWriter::read_in_freelist()
{
    bool is_shared = m_group.m_is_shared;
    size_t limit = m_free_lengths.size();
    auto limit_version = m_readlock_version;
    // in non-shared mode fake a version of 1 (instead of 0) to force all entries
    // to be considered truly free (in non shared mode all entries are marked as
    // released at version 0).
    if (!is_shared) limit_version = 1;
    for (size_t idx = 0; idx < limit; ++idx) {
        FreeSpaceEntry entry;
        entry.ref = m_free_positions.get(idx);
        entry.size = m_free_lengths.get(idx);
        if (is_shared)
            entry.released_at_version = m_free_versions.get(idx);
        else
            entry.released_at_version = 0;

        if (entry.released_at_version < limit_version)
            m_free_in_file.push_back(entry);
        else
            m_not_free_in_file.push_back(entry);
    }
}

void GroupWriter::recreate_freelist()
{
    bool is_shared = m_group.m_is_shared;
    m_free_positions.clear();
    m_free_lengths.clear();
    if (is_shared)
        m_free_versions.clear();
    for (auto& e : m_free_in_file) {
        m_free_positions.add(e.ref);
        m_free_lengths.add(e.size);
        if (is_shared)
            m_free_versions.add(e.released_at_version);
    }
    m_free_in_file.clear();
}

void GroupWriter::sort_freelist()
{
    std::sort(begin(m_free_in_file), end(m_free_in_file), [](auto& a, auto& b) { return a.ref < b.ref; });
}

void GroupWriter::merge_adjacent_entries_in_freelist()
{
    if (m_free_in_file.size() == 0)
        return;
    // Combine any adjacent chunks in the freelist
    auto prev = m_free_in_file.begin();
    for (auto it = m_free_in_file.begin() + 1; it != m_free_in_file.end(); ++it) {
        if (prev->ref + prev->size != it->ref) {
            prev = it;
            continue;
        }

        prev->size += it->size;
        it->size = 0;
    }
}

void GroupWriter::filter_empty_entries_in_freelist()
{
    // Remove all of the now zero-size chunks from the free list
    m_free_in_file.erase(
        std::remove_if(begin(m_free_in_file), end(m_free_in_file), [](auto& chunk) { return chunk.size == 0; }),
        end(m_free_in_file));
}

void GroupWriter::merge_free_space()
{
    sort_freelist();
    merge_adjacent_entries_in_freelist();
    filter_empty_entries_in_freelist();
}


size_t GroupWriter::get_free_space(size_t size)
{
    REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment

    std::pair<size_t, size_t> p = reserve_free_space(size);

    bool is_shared = m_group.m_is_shared;

    // Claim space from identified chunk
    size_t chunk_ndx = p.first;
    size_t chunk_pos = to_size_t(m_free_positions.get(chunk_ndx));
    size_t chunk_size = p.second;
    REALM_ASSERT_3(chunk_size, >=, size);
    REALM_ASSERT((chunk_size % 8) == 0);

    size_t rest = chunk_size - size;
    if (rest > 0) {
        // Allocating part of chunk - this alway happens from the beginning
        // of the chunk. The call to reserve_free_space may split chunks
        // in order to make sure that it returns a chunk from which allocation
        // can be done from the beginning
        m_free_positions.set(chunk_ndx, to_int64(chunk_pos + size));
        m_free_lengths.set(chunk_ndx, to_int64(rest));
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


inline size_t GroupWriter::split_freelist_chunk(size_t index, size_t start_pos, size_t alloc_pos, size_t chunk_size,
                                                bool is_shared)
{
    REALM_ASSERT_RELEASE_EX(alloc_pos > start_pos, alloc_pos, start_pos);
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


std::pair<size_t, size_t> GroupWriter::search_free_space_in_part_of_freelist(size_t size, size_t begin, size_t end,
                                                                             bool& found)
{
    bool is_shared = m_group.m_is_shared;
    SlabAlloc& alloc = m_group.m_alloc;
    for (size_t next_start = begin; next_start < end;) {
        size_t i = m_free_lengths.find_first<Greater>(size - 1, next_start);
        if (i == not_found) {
            break;
        }

        next_start = i + 1;

        // Only chunks that are not occupied by current readers
        // are allowed to be used.
        if (is_shared) {
            size_t ver = to_size_t(m_free_versions.get(i));
            if (ver >= m_readlock_version) {
                continue;
            }
        }

        size_t chunk_size = to_size_t(m_free_lengths.get(i));

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
    return std::make_pair(end, 0);
}


std::pair<size_t, size_t> GroupWriter::reserve_free_space(size_t size)
{
    typedef std::pair<size_t, size_t> Chunk;
    Chunk chunk;
    bool found;
    size_t end = m_free_lengths.size();
    if (m_alloc_position >= end)
        m_alloc_position = 0;

    chunk = search_free_space_in_part_of_freelist(size, m_alloc_position, end, found);
    if (!found) {
        chunk = search_free_space_in_part_of_freelist(size, 0, m_alloc_position, found);
    }
    while (!found) {
        // No free space, so we have to extend the file.
        extend_free_space(size);
        // extending the file will add a new entry at the end of the freelist,
        // so search that particular entry
        end = m_free_lengths.size();
        chunk = search_free_space_in_part_of_freelist(size, end - 1, end, found);
    }
    m_alloc_position = chunk.first;
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
    REALM_ASSERT_3(new_file_size % 8, ==, 0);
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

    size_t chunk_ndx = m_free_positions.size();
    size_t chunk_size = new_file_size - logical_file_size;
    REALM_ASSERT_3(chunk_size % 8, ==, 0); // 8-byte alignment
    m_free_positions.add(logical_file_size);
    m_free_lengths.add(chunk_size);
    if (is_shared)
        m_free_versions.add(0); // new space is always free for writing


    // Update the logical file size
    m_group.m_top.set(2, 1 + 2 * uint64_t(new_file_size)); // Throws
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
    MapWindow* window = get_window(pos, size);
    char* dest_addr = window->translate(pos);
    window->encryption_read_barrier(dest_addr, size);
    realm::safe_copy_n(data, size, dest_addr);
    window->encryption_write_barrier(dest_addr, size);
}


ref_type GroupWriter::write_array(const char* data, size_t size, uint32_t checksum)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);
    REALM_ASSERT_3((pos & 0x7), ==, 0); // Write position should always be 64bit aligned

    // Write the block
    MapWindow* window = get_window(pos, size);
    char* dest_addr = window->translate(pos);
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
    bool disable_sync = get_disable_sync_to_disk();

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
