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
#include <set>

#include <iostream>

#include <realm/group_writer.hpp>

#include <realm/alloc_slab.hpp>
#include <realm/transaction.hpp>
#include <realm/disable_sync_to_disk.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/util/safe_int_ops.hpp>

using namespace realm;
using namespace realm::util;

namespace realm {
class InMemoryWriter : public _impl::ArrayWriterBase {
public:
    InMemoryWriter(GroupWriter& owner)
        : m_owner(owner)
        , m_alloc(owner.m_alloc)
    {
    }
    ref_type write_array(const char* data, size_t size, uint32_t checksum) override
    {
        size_t pos = m_owner.get_free_space(size);

        // Write the block
        char* dest_addr = translate(pos);
        REALM_ASSERT_RELEASE(dest_addr && (reinterpret_cast<size_t>(dest_addr) & 7) == 0);
        memcpy(dest_addr, &checksum, 4);
        memcpy(dest_addr + 4, data + 4, size - 4);
        // return ref of the written array
        ref_type ref = to_ref(pos);
        return ref;
    }
    char* translate(ref_type ref)
    {
        return m_alloc.translate_memory_pos(ref);
    }

private:
    GroupWriter& m_owner;
    SlabAlloc& m_alloc;
};
} // namespace realm


// Class controlling a memory mapped window into a file
class WriteWindowMgr::MapWindow {
public:
    MapWindow(size_t alignment, util::File& f, ref_type start_ref, size_t initial_size,
              util::WriteMarker* write_marker = nullptr);
    ~MapWindow();

    // translate a ref to a pointer
    // inside the window defined during construction.
    char* translate(ref_type ref);
    void encryption_read_barrier(void* start_addr, size_t size);
    void encryption_write_barrier(void* start_addr, size_t size);
    // flush from private to shared cache
    void flush();
    // sync to disk (including flush as needed)
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
bool WriteWindowMgr::MapWindow::matches(ref_type start_ref, size_t size)
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
ref_type WriteWindowMgr::MapWindow::aligned_to_mmap_block(ref_type start_ref)
{
    // align to 1MB boundary
    size_t page_mask = m_alignment - 1;
    return start_ref & ~page_mask;
}

size_t WriteWindowMgr::MapWindow::get_window_size(util::File& f, ref_type start_ref, size_t size)
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
bool WriteWindowMgr::MapWindow::extends_to_match(util::File& f, ref_type start_ref, size_t size)
{
    size_t aligned_ref = aligned_to_mmap_block(start_ref);
    if (aligned_ref != m_base_ref)
        return false;
    size_t window_size = get_window_size(f, start_ref, size);
    m_map.sync();
    m_map.unmap();
    m_map.map(f, File::access_ReadWrite, window_size, m_base_ref);
    return true;
}

WriteWindowMgr::MapWindow::MapWindow(size_t alignment, util::File& f, ref_type start_ref, size_t size,
                                     util::WriteMarker* write_marker)
    : m_alignment(alignment)
{
    m_base_ref = aligned_to_mmap_block(start_ref);
    size_t window_size = get_window_size(f, start_ref, size);
    m_map.map(f, File::access_ReadWrite, window_size, m_base_ref);
#if REALM_ENABLE_ENCRYPTION
    if (auto p = m_map.get_encrypted_mapping())
        p->set_marker(write_marker);
#else
    static_cast<void>(write_marker);
#endif
}

WriteWindowMgr::MapWindow::~MapWindow()
{
    m_map.sync();
    m_map.unmap();
}

void WriteWindowMgr::MapWindow::flush()
{
    m_map.flush();
}

void WriteWindowMgr::MapWindow::sync()
{
    flush();
    m_map.sync();
}

char* WriteWindowMgr::MapWindow::translate(ref_type ref)
{
    return m_map.get_addr() + (ref - m_base_ref);
}

void WriteWindowMgr::MapWindow::encryption_read_barrier(void* start_addr, size_t size)
{
    util::encryption_read_barrier_for_write(start_addr, size, m_map.get_encrypted_mapping());
}

void WriteWindowMgr::MapWindow::encryption_write_barrier(void* start_addr, size_t size)
{
    realm::util::encryption_write_barrier(start_addr, size, m_map.get_encrypted_mapping());
}

WriteWindowMgr::WriteWindowMgr(SlabAlloc& alloc, Durability dura, WriteMarker* write_marker)
    : m_alloc(alloc)
    , m_durability(dura)
    , m_write_marker(write_marker)
{
    m_map_windows.reserve(num_map_windows);
#if REALM_PLATFORM_APPLE && REALM_MOBILE
    m_window_alignment = 1 * 1024 * 1024; // 1M
#else
    if (sizeof(int*) == 4) {                  // 32 bit address space
        m_window_alignment = 1 * 1024 * 1024; // 1M
    }
    else {
        // large address space - just choose a size so that we have a single window
        size_t total_size = m_alloc.get_total_size();
        size_t wanted_size = 1;
        while (total_size) {
            total_size >>= 1;
            wanted_size <<= 1;
        }
        if (wanted_size < 1 * 1024 * 1024)
            wanted_size = 1 * 1024 * 1024; // minimum 1M
        m_window_alignment = wanted_size;
    }
#endif
}

GroupCommitter::GroupCommitter(Transaction& group, Durability dura, WriteMarker* write_marker)
    : m_group(group)
    , m_alloc(group.m_alloc)
    , m_durability(dura)
    , m_window_mgr(group.m_alloc, dura, write_marker)
{
}

GroupCommitter::~GroupCommitter() = default;

GroupWriter::GroupWriter(Transaction& group, Durability dura, WriteMarker* write_marker)
    : m_group(group)
    , m_alloc(group.m_alloc)
    , m_durability(dura)
    , m_window_mgr(group.m_alloc, dura, write_marker)
    , m_free_positions(m_alloc)
    , m_free_lengths(m_alloc)
    , m_free_versions(m_alloc)
{
    Array& top = m_group.m_top;
    m_logical_size = size_t(top.get_as_ref_or_tagged(Group::s_file_size_ndx).get_as_int());

    // When we make a commit, we will at least need room for the version
    while (top.size() <= Group::s_version_ndx) {
        top.add(0); // Throws
    }

    m_free_positions.set_parent(&top, Group::s_free_pos_ndx);
    m_free_lengths.set_parent(&top, Group::s_free_size_ndx);
    m_free_versions.set_parent(&top, Group::s_free_version_ndx);

    ref_type free_positions_ref = m_free_positions.get_ref_from_parent();
    if (free_positions_ref) {
        m_free_positions.init_from_ref(free_positions_ref);
    }
    else {
        m_free_positions.create(Array::type_Normal); // Throws
        _impl::DestroyGuard<Array> dg(&m_free_positions);
        m_free_positions.update_parent();            // Throws
        dg.release();
    }

    if (ref_type ref = m_free_lengths.get_ref_from_parent()) {
        m_free_lengths.init_from_ref(ref);
        REALM_ASSERT_RELEASE_EX(m_free_positions.size() == m_free_lengths.size(), top.get_ref(),
                                m_free_positions.size(), m_free_lengths.size());
    }
    else {
        m_free_lengths.create(Array::type_Normal); // Throws
        _impl::DestroyGuard<Array> dg(&m_free_lengths);
        m_free_lengths.update_parent();            // Throws
        dg.release();
    }

    DB::version_type initial_version = 0;

    if (ref_type ref = m_free_versions.get_ref_from_parent()) {
        m_free_versions.init_from_ref(ref);
        REALM_ASSERT_RELEASE_EX(m_free_versions.size() == m_free_lengths.size(), top.get_ref(),
                                m_free_versions.size(), m_free_lengths.size());
    }
    else {
        int_fast64_t value = int_fast64_t(initial_version);
        top.set(6, 1 + 2 * uint64_t(initial_version)); // Throws
        size_t n = m_free_positions.size();
        bool context_flag = false;
        m_free_versions.create(Array::type_Normal, context_flag, n, value); // Throws
        _impl::DestroyGuard<Array> dg(&m_free_versions);
        m_free_versions.update_parent();                                    // Throws
        dg.release();
    }
    m_evacuation_limit = 0;
    m_backoff = 0;
}


void GroupWriter::sync_according_to_durability()
{
    switch (m_durability) {
        case Durability::Full:
        case Durability::Unsafe:
            m_window_mgr.sync_all_mappings();
            break;
        case Durability::MemOnly:
            m_window_mgr.flush_all_mappings();
    }
}

GroupWriter::~GroupWriter() = default;

size_t GroupWriter::get_file_size() const noexcept
{
    auto sz = to_size_t(m_alloc.get_file_size());
    return sz;
}

void WriteWindowMgr::flush_all_mappings()
{
    for (const auto& window : m_map_windows) {
        window->flush();
    }
}

void WriteWindowMgr::sync_all_mappings()
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
WriteWindowMgr::MapWindow* WriteWindowMgr::get_window(ref_type start_ref, size_t size)
{
    auto match = std::find_if(m_map_windows.begin(), m_map_windows.end(), [&](const auto& window) {
        return window->matches(start_ref, size) || window->extends_to_match(m_alloc.get_file(), start_ref, size);
    });
    if (match != m_map_windows.end()) {
        // move matching window to top (to keep LRU order)
        std::rotate(m_map_windows.begin(), match, match + 1);
        return m_map_windows[0].get();
    }
    // no window found, make room for a new one at the top
    if (m_map_windows.size() == num_map_windows) {
        m_map_windows.back()->flush();
        m_map_windows.pop_back();
    }
    auto new_window =
        std::make_unique<MapWindow>(m_window_alignment, m_alloc.get_file(), start_ref, size, m_write_marker);
    m_map_windows.insert(m_map_windows.begin(), std::move(new_window));
    return m_map_windows[0].get();
}

#define REALM_ALLOC_DEBUG 0
#if REALM_ALLOC_DEBUG
#define ALLOC_DBG_COUT(args)                                                                                         \
    {                                                                                                                \
        std::cout << args;                                                                                           \
    }
#else
#define ALLOC_DBG_COUT(args)
#endif

#ifdef REALM_DEBUG
void GroupWriter::map_reachable()
{
    class Collector : public Array::MemUsageHandler {
    public:
        Collector(std::vector<Reachable>& reachable)
            : m_reachable(reachable)
        {
        }
        void handle(ref_type ref, size_t, size_t used) override
        {
            m_reachable.emplace_back(Reachable{ref, used});
        }
        std::vector<Reachable>& m_reachable;
    };
    // collect reachable blocks in all reachable versions
    for (auto& [version, info] : m_top_ref_map) {
        Collector collector(info.reachable_blocks);
        // skip any empty entries
        if (info.top_ref == 0)
            continue;
        Array array(m_alloc);
        array.init_from_ref(info.top_ref);
        array.report_memory_usage(collector);
        std::sort(info.reachable_blocks.begin(), info.reachable_blocks.end(),
                  [](const Reachable& a, const Reachable& b) {
                      return a.pos < b.pos;
                  });
    }

#if REALM_ALLOC_DEBUG
    std::cout << "  Reachable: ";
    // this really should be inverted, showing all versions pr entry instead of all entries pr version
    for (auto& [version, info] : m_top_ref_map) {
        std::cout << std::endl << "    Version: " << version;
        for (auto& i : info.reachable_blocks) {
            std::cout << std::endl << "      " << i.pos << " - " << i.pos + i.size;
        }
    }
    std::cout << std::endl << "  Backdating:";
#endif
}
#endif

void GroupWriter::backdate()
{
    struct FreeList {
        Array positions;
        Array lengths;
        Array versions;
        ref_type top_ref;
        ref_type logical_file_size;
        uint64_t version;
        bool initialized = false;
        FreeList(Allocator& alloc, ref_type top, ref_type logical_file_size, uint64_t version)
            : positions(alloc)
            , lengths(alloc)
            , versions(alloc)
            , top_ref(top)
            , logical_file_size(logical_file_size)
            , version(version)
        {
        }
    };


    using FreeListMap = std::vector<std::unique_ptr<FreeList>>;
    FreeListMap old_freelists;
    old_freelists.reserve(m_top_ref_map.size());
    for (auto& [version, info] : m_top_ref_map) {
        if (version < m_oldest_reachable_version)
            continue;
        auto e = std::make_unique<FreeList>(m_alloc, info.top_ref, info.logical_file_size, version);
        old_freelists.push_back(std::move(e));
    }


    // little helper: get the youngest version older than given
    auto get_earlier = [&](uint64_t version) -> FreeList* {
        auto it = std::lower_bound(old_freelists.begin(), old_freelists.end(), version,
                                   [](const std::unique_ptr<FreeList>& e, uint64_t v) {
                                       return e->version < v;
                                   });
        // There will always be at least one freelist:
        REALM_ASSERT(it != old_freelists.end());
        REALM_ASSERT(it != old_freelists.begin());
        --it;
        REALM_ASSERT((*it)->version < version);
        return it->get();
    };


    // find (if possible) youngest time stamp in any block in a sequence that fully covers a given one.
    auto find_cover_for = [&](const FreeSpaceEntry& entry, FreeList& free_list) -> std::optional<uint64_t> {
        auto entry_end = std::min(entry.ref + entry.size, free_list.logical_file_size);
        if (entry.ref >= entry_end) {
            return 0; // block completely beyond end of file
        }

        if (!free_list.initialized) {
            // setup arrays
            free_list.initialized = true;
            if (free_list.top_ref) {
                Array top_array(m_alloc);
                top_array.init_from_ref(free_list.top_ref);
                if (top_array.size() > Group::s_free_version_ndx) {
                    // we have a freelist with versioning info
                    free_list.positions.init_from_ref(top_array.get_as_ref(Group::s_free_pos_ndx));
                    free_list.lengths.init_from_ref(top_array.get_as_ref(Group::s_free_size_ndx));
                    free_list.versions.init_from_ref(top_array.get_as_ref(Group::s_free_version_ndx));
                }
            }
        }

        if (!free_list.positions.is_attached()) {
            return {}; // no free list associated with that version
        }
        const size_t limit = free_list.positions.size();
        if (limit == 0) {
            return {}; // empty freelist
        }
        const size_t index = free_list.positions.upper_bound_int(entry.ref) - 1;
        if (index == size_t(-1)) {
            return {};               // no free blocks before the 'ref' we are looking for
        }
        REALM_ASSERT(index < limit); // follows from above
        const auto start_pos = static_cast<ref_type>(free_list.positions.get(index));
        REALM_ASSERT(start_pos <= entry.ref);
        auto end_pos = start_pos + static_cast<ref_type>(free_list.lengths.get(index));
        if (end_pos <= entry.ref) {
            return {}; // free block ends before the 'ref' we are looking for
        }
        uint64_t found_version = free_list.versions.get(index);

        // coalesce any subsequent contiguous entries
        for (auto next = index + 1;
             next < limit && free_list.positions.get(next) == (int64_t)end_pos && end_pos < entry_end; ++next) {
            end_pos += static_cast<ref_type>(free_list.lengths.get(next));
            // pick youngest (highest) version of blocks
            found_version = std::max<uint64_t>(found_version, free_list.versions.get(next));
        }
        // is the block fully covered by range established above?
        if (end_pos < entry_end) {
            return {}; // no, it isn't
        }
        REALM_ASSERT(found_version <= entry.released_at_version);
        return found_version;
    };

    // check if a given entry overlaps a reachable block. Only used in debug mode.
    auto is_referenced = [&](FreeSpaceEntry& entry) -> bool {
#ifdef REALM_DEBUG
        bool referenced = false;
        ALLOC_DBG_COUT("    Considering [" << entry.ref << ", " << entry.size << "]-" << entry.released_at_version
                                           << " {");
        auto end = m_top_ref_map.end();
        // go through all versions:
        for (auto top_ref_map = m_top_ref_map.begin(); top_ref_map != end && !referenced; ++top_ref_map) {
            // locate any overlapping block in each versions' freelist
            auto info_begin = top_ref_map->second.reachable_blocks.begin();
            auto info_end = top_ref_map->second.reachable_blocks.end();
            auto it = std::lower_bound(info_begin, info_end, entry.ref, [](const Reachable& a, size_t val) {
                return val > a.pos;
            });
            if (it != info_end) {
                if (it != info_begin)
                    --it;
                while (it != info_end && it->pos < entry.ref + entry.size) {
                    if (it->pos + it->size > entry.ref) {
                        ALLOC_DBG_COUT(top_ref_map->first << " ");
                        referenced = true;
                        break;
                    }
                    ++it;
                }
            }
        }
        if (!referenced) {
            ALLOC_DBG_COUT("none");
        }
        ALLOC_DBG_COUT("} ");
        return referenced;
#else
        static_cast<void>(entry); // silence a warning
        return false;
#endif
    };

    auto backdate_single_entry = [&](FreeSpaceEntry& entry) -> void {
        const auto referenced = is_referenced(entry);
        // references must be to a version before the one we're currently building:
        REALM_ASSERT(entry.released_at_version < m_current_version);
        while (entry.released_at_version) {
            // early out for references before oldest freelist:
            if (entry.released_at_version <= this->m_oldest_reachable_version) {
                REALM_ASSERT_DEBUG(!referenced);
                break;
            }
            auto earlier_it = get_earlier(entry.released_at_version);
            ALLOC_DBG_COUT(" - earlier freelist: " << earlier_it->version);
            if (auto covering_version = find_cover_for(entry, *earlier_it)) {
                ALLOC_DBG_COUT("  backdating [" << entry.ref << ", " << entry.size << "]  version: "
                                                << entry.released_at_version << " -> " << *covering_version);
                REALM_ASSERT_DEBUG(!referenced);
                entry.released_at_version = *covering_version;
            }
            else {
                ALLOC_DBG_COUT("  not free at that point");
                REALM_ASSERT_DEBUG(referenced);
                break;
            }
        }
        ALLOC_DBG_COUT(std::endl);
    };


#ifdef REALM_DEBUG
    map_reachable();
#endif
    for (auto&& entry : m_locked_in_file) {
        backdate_single_entry(entry);
    }
}

void GroupWriter::prepare_evacuation()
{
    Array& top = m_group.m_top;
    if (top.size() > Group::s_evacuation_point_ndx) {
        if (auto val = top.get(Group::s_evacuation_point_ndx)) {
            Array arr(m_alloc);
            if (val & 1) {
                m_evacuation_limit = size_t(val >> 1);
                arr.create(Node::type_Normal);
                arr.add(uint64_t(m_evacuation_limit));
                arr.add(0); // Backoff = false
                top.set_as_ref(Group::s_evacuation_point_ndx, arr.get_ref());
            }
            else {
                arr.init_from_ref(to_ref(val));
                auto sz = arr.size();
                REALM_ASSERT(sz >= 2);
                m_evacuation_limit = size_t(arr.get(0));
                m_backoff = arr.get(1);
                if (m_backoff > 0) {
                    --m_backoff;
                }
                else {
                    for (size_t i = 2; i < sz; i++) {
                        m_evacuation_progress.push_back(size_t(arr.get(i)));
                    }
                }
                // We give up if the freelists were allocated above the evacuation limit
                if (m_evacuation_limit > 0 && m_free_positions.get_ref() > m_evacuation_limit) {
                    // Wait 10 commits until trying again
                    m_backoff = 10;
                    m_evacuation_limit = 0;
                    if (auto logger = m_group.get_logger()) {
                        logger->log(util::Logger::Level::detail, "Give up compaction");
                    }
                }
            }
        }
    }
}

ref_type GroupWriter::write_group()
{
    ALLOC_DBG_COUT("Commit nr " << m_current_version << "   ( from " << m_oldest_reachable_version << " )"
                                << std::endl);

    read_in_freelist();
    verify_freelists();
    // Now, 'm_size_map' holds all free elements candidate for recycling

    Array& top = m_group.m_top;
    ALLOC_DBG_COUT("  Allocating file space for data:" << std::endl);

    // Recursively write all changed arrays (but not 'top' and free-lists yet,
    // as they are going to change along the way.) If free space is available in
    // the attached database file, we use it, but this does not include space
    // that has been release during the current transaction (or since the last
    // commit), as that would lead to clobbering of the previous database
    // version.
    bool deep = true, only_if_modified = true;
    std::unique_ptr<InMemoryWriter> in_memory_writer;
    _impl::ArrayWriterBase* writer = this;
    if (m_alloc.is_in_memory()) {
        in_memory_writer = std::make_unique<InMemoryWriter>(*this);
        writer = in_memory_writer.get();
    }
    ref_type names_ref = m_group.m_table_names.write(*writer, deep, only_if_modified); // Throws
    ref_type tables_ref = m_group.m_tables.write(*writer, deep, only_if_modified);     // Throws

    int_fast64_t value_1 = from_ref(names_ref);
    int_fast64_t value_2 = from_ref(tables_ref);
    top.set(0, value_1); // Throws
    top.set(1, value_2); // Throws
    verify_freelists();

    // If file has a history and is opened in shared mode, write the new history
    // to the file. If the file has a history, but si not opened in shared mode,
    // discard the history, as it could otherwise be left in an inconsistent state.
    if (top.size() > Group::s_hist_ref_ndx) {
        if (ref_type history_ref = top.get_as_ref(Group::s_hist_ref_ndx)) {
            Allocator& alloc = top.get_alloc();
            ref_type new_history_ref = Array::write(history_ref, alloc, *writer, only_if_modified); // Throws
            top.set(Group::s_hist_ref_ndx, from_ref(new_history_ref));                              // Throws
        }
    }
    if (top.size() > Group::s_evacuation_point_ndx) {
        ref_type ref = top.get_as_ref(Group::s_evacuation_point_ndx);
        if (m_evacuation_limit || m_backoff) {
            REALM_ASSERT_RELEASE(ref);
            Array arr(m_alloc);
            arr.init_from_ref(ref);
            arr.truncate(2);

            arr.set(0, int64_t(m_evacuation_limit));
            if (m_backoff == 0 && m_evacuation_progress.empty()) {
                // We have done a scan - Now we should just wait for the nodes still
                // being in the evacuation zone being released by the transactions
                // still holding on to them. This could take many commits.
                m_backoff = 1000;
            }
            arr.set(1, m_backoff); // Backoff from scanning
            for (auto index : m_evacuation_progress) {
                arr.add(int64_t(index));
            }
            ref = arr.write(*writer, false, only_if_modified);
            top.set_as_ref(Group::s_evacuation_point_ndx, ref);
        }
        else if (ref) {
            Array::destroy(ref, m_alloc);
            top.set(Group::s_evacuation_point_ndx, 0);
        }
    }
    verify_freelists();
    ALLOC_DBG_COUT("  Freelist size after allocations: " << m_size_map.size() << std::endl);
    // We now back-date (if possible) any blocks freed in versions which
    // are becoming unreachable.
    if (m_any_new_unreachables) {
        backdate();
        verify_freelists();
    }

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
    ALLOC_DBG_COUT("  In-mem freelist before/after consolidation: " << m_group.m_alloc.m_free_read_only.size());
    size_t free_read_only_size = m_group.m_alloc.consolidate_free_read_only(); // Throws
    ALLOC_DBG_COUT("/" << free_read_only_size << std::endl);
    max_free_list_size += free_read_only_size;
    max_free_list_size += m_locked_in_file.size();
    max_free_list_size += m_under_evacuation.size();
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

    size_t max_free_space_needed =
        Array::get_max_byte_size(top.size()) + size_per_free_list_entry() * max_free_list_size;

    ALLOC_DBG_COUT("  Allocating file space for freelists:" << std::endl);
    // Reserve space for remaining arrays. We ask for some extra bytes beyond the
    // maximum number that is required. This ensures that even if we end up
    // using the maximum size possible, we still do not end up with a zero size
    // free-space chunk as we deduct the actually used size from it.
    auto reserve = reserve_free_space(max_free_space_needed + 8); // Throws
    m_allocation_allowed = false;
    size_t reserve_pos = reserve->second;
    size_t reserve_size = reserve->first;
    verify_freelists();

    // Beyond this point:
    // * Any allocation of disk space (via write()) will modify the freelist and
    //   potentially allocate (from) a block which makes it impossible to find the
    //   reserve_ndx used to pick the block to finally hold the top array and freelist
    //   itself.
    // * Any release of disk space (side effect of copy on write, or explicit destroy)
    //   may add a free block to the freelist. This is OK, because we only fuse free
    //   blocks earlier, when loading the freelist. Had we fused later, we might
    //   have changed the block chosen to hold the top array and freelists, such that
    //   reserve_pos could not be found in recreate_freelist().

    // Now we can check, if we can reduce the logical file size. This can be done
    // when there is only one block in m_under_evacuation, which means that all
    // nodes in this range have been moved
    if (m_under_evacuation.size() == 1) {
        auto& elem = m_under_evacuation.back();
        if (elem.ref + elem.size == m_logical_size) {
            // This is at the end of the file
            size_t pos = elem.ref;
            m_logical_size = util::round_up_to_page_size(pos);
            elem.size = (m_logical_size - pos);
            if (elem.size == 0)
                m_under_evacuation.clear();
            top.set(Group::s_file_size_ndx, RefOrTagged::make_tagged(m_logical_size));
            auto ref = top.get_as_ref(Group::s_evacuation_point_ndx);
            REALM_ASSERT_RELEASE(ref);
            Array::destroy(ref, m_alloc);
            top.set(Group::s_evacuation_point_ndx, 0);
            m_evacuation_limit = 0;

            if (auto logger = m_group.get_logger()) {
                logger->log(util::Logger::Level::detail, "New logical size %1", m_logical_size);
            }
        }
    }

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
    REALM_ASSERT_RELEASE(reserve_pos == m_free_positions.get(reserve_ndx));
    REALM_ASSERT_RELEASE(reserve_size == m_free_lengths.get(reserve_ndx));
    ALLOC_DBG_COUT("  Freelist size after merge: " << m_free_positions.size() << "   freelist space required: "
                                                   << max_free_space_needed << std::endl);

    // At this point the freelists are fixed, except for the special entry chosen
    // by reserve_ndx. If allocation (through write()) happens after this point,
    // it will not be reflected in the freelists saved to disk. If release happens
    // after this point, released blocks will not enter the freelist but be lost.
    // Manipulations to the freelist itself and to the top array is OK, because
    // a) they've all been allocated from slab (see read_in_freelist()), so any
    //    released block is not in the file
    // b) the ultimate placement of these arrays is within the explicitly managed
    //    freeblock at reserve_ndx. The size of the freeblock is computed below
    //    and must be strictly larger than the worst case storage needed.
    //    An assert checks that the total memory used for these arrays does not
    //    exceed the space in said freeblock.
    //
    // Before we calculate the actual sizes of the free-list arrays, we must
    // make sure that the final adjustments of the free lists (i.e., the
    // deduction of the actually used space from the reserved chunk,) will not
    // change the byte-size of those arrays.
    // size_t reserve_pos = to_size_t(m_free_positions.get(reserve_ndx));
    REALM_ASSERT_RELEASE(reserve_size >= max_free_space_needed + 8);
    int_fast64_t value_4 = to_int64(reserve_pos + max_free_space_needed);

#if REALM_ENABLE_MEMDEBUG
    m_free_positions.m_no_relocation = true;
    m_free_lengths.m_no_relocation = true;
#endif

    // Ensure that this arrays does not expand later so that we can trust
    // the use of get_byte_size() below:
    m_free_positions.ensure_minimum_width(value_4); // Throws

    // Get final sizes of free-list arrays
    size_t free_positions_size = m_free_positions.get_byte_size();
    size_t free_sizes_size = m_free_lengths.get_byte_size();
    size_t free_versions_size = m_free_versions.get_byte_size();
    REALM_ASSERT_RELEASE(Array::get_wtype_from_header(Array::get_header_from_data(m_free_versions.m_data)) ==
                         Array::wtype_Bits);

    // Calculate write positions
    ref_type reserve_ref = to_ref(reserve_pos);
    ref_type free_positions_ref = reserve_ref;
    ref_type free_sizes_ref = free_positions_ref + free_positions_size;
    ref_type free_versions_ref = free_sizes_ref + free_sizes_size;
    ref_type top_ref = free_versions_ref + free_versions_size;

    // Update top to point to the calculated positions
    top.set(Group::s_free_pos_ndx, from_ref(free_positions_ref));               // Throws
    top.set(Group::s_free_size_ndx, from_ref(free_sizes_ref));                  // Throws
    top.set(Group::s_free_version_ndx, from_ref(free_versions_ref));            // Throws
    top.set(Group::s_version_ndx, RefOrTagged::make_tagged(m_current_version)); // Throws

    // Compacting files smaller than 1 Mb is not worth the effort. Arbitrary chosen value.
    static constexpr size_t minimal_compaction_size = 0x100000;
    if (m_logical_size >= minimal_compaction_size && m_evacuation_limit == 0 && m_backoff == 0) {
        // We might have allocated a bigger chunk than needed for the free lists, so if we
        // add what we have reserved and subtract what was requested, we get a better measure
        // for what will be free eventually. Also subtract the locked space as this is not
        // actually free.
        size_t free_space = m_free_space_size + reserve_size - max_free_space_needed - m_locked_space_size;
        REALM_ASSERT_RELEASE(m_logical_size > free_space);
        size_t used_space = m_logical_size - free_space;
        if (free_space > 2 * used_space) {
            // Clean up potential
            auto limit = util::round_up_to_page_size(used_space + used_space / 2);

            // If we make the file too small, there is a big chance it will grow immediately afterwards
            static constexpr size_t minimal_evac_limit = 0x10000;
            m_evacuation_limit = std::max(minimal_evac_limit, limit);

            // From now on, we will only allocate below this limit
            // Save the limit in the file
            while (top.size() <= Group::s_evacuation_point_ndx) {
                top.add(0);
            }
            top.set(Group::s_evacuation_point_ndx, RefOrTagged::make_tagged(m_evacuation_limit));
            if (auto logger = m_group.get_logger()) {
                logger->log(util::Logger::Level::detail, "Start compaction with limit %1", m_evacuation_limit);
            }
        }
    }
    // Get final sizes
    size_t top_byte_size = top.get_byte_size();
    ref_type end_ref = top_ref + top_byte_size;
    REALM_ASSERT_RELEASE(size_t(end_ref) <= reserve_pos + max_free_space_needed);

    // Deduct the used space from the reserved chunk. Note that we have made
    // sure that the remaining size is never zero. Also, by the call to
    // m_free_positions.ensure_minimum_width() above, we have made sure that
    // m_free_positions has the capacity to store the new larger value without
    // reallocation.
    size_t rest = reserve_pos + reserve_size - size_t(end_ref);
    size_t used = size_t(end_ref) - reserve_pos;
    REALM_ASSERT_RELEASE(rest > 0);
    int_fast64_t value_8 = from_ref(end_ref);
    int_fast64_t value_9 = to_int64(rest);

    // value_9 is guaranteed to be smaller than the existing entry in the array and hence will not cause bit
    // expansion
    REALM_ASSERT_RELEASE(value_8 <= Array::ubound_for_width(m_free_positions.get_width()));
    REALM_ASSERT_RELEASE(value_9 <= Array::ubound_for_width(m_free_lengths.get_width()));

    m_free_positions.set(reserve_ndx, value_8); // Throws
    m_free_lengths.set(reserve_ndx, value_9);   // Throws
    m_free_space_size += rest;

#if REALM_ALLOC_DEBUG
    std::cout << "  Final Freelist:" << std::endl;
    for (size_t j = 0; j < m_free_positions.size(); ++j) {
        std::cout << "    [" << m_free_positions.get(j) << ".." << m_free_lengths.get(j);
        if (m_free_versions.size()) {
            std::cout << "]: " << m_free_versions.get(j);
        }
    }
    std::cout << std::endl << std::endl;
#endif

    // The free-list now have their final form, so we can write them to the file
    // char* start_addr = m_file_map.get_addr() + reserve_ref;
    if (m_alloc.is_in_memory()) {
        auto translator = in_memory_writer.get();
        write_array_at(translator, free_positions_ref, m_free_positions.get_header(), free_positions_size); // Throws
        write_array_at(translator, free_sizes_ref, m_free_lengths.get_header(), free_sizes_size);           // Throws
        write_array_at(translator, free_versions_ref, m_free_versions.get_header(), free_versions_size);    // Throws

        // Write top
        write_array_at(translator, top_ref, top.get_header(), top_byte_size); // Throws
    }
    else {
        MapWindow* window = m_window_mgr.get_window(reserve_ref, end_ref - reserve_ref);
        char* start_addr = window->translate(reserve_ref);
        window->encryption_read_barrier(start_addr, used);
        write_array_at(window, free_positions_ref, m_free_positions.get_header(), free_positions_size); // Throws
        write_array_at(window, free_sizes_ref, m_free_lengths.get_header(), free_sizes_size);           // Throws
        write_array_at(window, free_versions_ref, m_free_versions.get_header(), free_versions_size);    // Throws
        REALM_ASSERT_EX(
            free_positions_ref >= reserve_ref && free_positions_ref + free_positions_size <= reserve_ref + used,
            reserve_ref, reserve_ref + used, free_positions_ref, free_positions_ref + free_positions_size, top_ref);
        REALM_ASSERT_EX(free_sizes_ref >= reserve_ref && free_sizes_ref + free_sizes_size <= reserve_ref + used,
                        reserve_ref, reserve_ref + used, free_sizes_ref, free_sizes_ref + free_sizes_size, top_ref);
        REALM_ASSERT_EX(
            free_versions_ref >= reserve_ref && free_versions_ref + free_versions_size <= reserve_ref + used,
            reserve_ref, reserve_ref + used, free_versions_ref, free_versions_ref + free_versions_size, top_ref);


        // Write top
        write_array_at(window, top_ref, top.get_header(), top_byte_size); // Throws
        window->encryption_write_barrier(start_addr, used);
    }
    // Return top_ref so that it can be saved in lock file used for coordination
    return top_ref;
}


void GroupWriter::read_in_freelist()
{
    std::vector<FreeSpaceEntry> free_in_file;
    size_t evacuation_limit = m_evacuation_limit ? m_evacuation_limit : size_t(-1);
    REALM_ASSERT(m_free_lengths.is_attached());
    size_t limit = m_free_lengths.size();
    REALM_ASSERT_RELEASE_EX(m_free_positions.size() == limit, limit, m_free_positions.size());
    REALM_ASSERT_RELEASE_EX(m_free_versions.size() == limit, limit, m_free_versions.size());

    if (limit) {
        auto limit_version = m_oldest_reachable_version;
        for (size_t idx = 0; idx < limit; ++idx) {
            size_t ref = size_t(m_free_positions.get(idx));
            size_t size = size_t(m_free_lengths.get(idx));

            uint64_t version = m_free_versions.get(idx);
            // Entries that are freed in later still alive versions are not candidates for merge or allocation
            if (version > limit_version) {
                m_locked_in_file.emplace_back(ref, size, version);
                continue;
            }
            if (ref + size > evacuation_limit) {
                if (ref < evacuation_limit) {
                    // Split entry
                    size_t still_free_size = evacuation_limit - ref;
                    m_under_evacuation.emplace_back(evacuation_limit, size - still_free_size, 0);
                    size = still_free_size;
                }
                else {
                    m_under_evacuation.emplace_back(ref, size, 0);
                    continue;
                }
            }
            free_in_file.emplace_back(ref, size, 0);
        }

        // This will imply a copy-on-write
        m_free_positions.clear();
        m_free_lengths.clear();
        m_free_versions.clear();
    }
    else {
        // We need to free the space occupied by the free lists
        // If the lists are empty, this has to be done explicitly
        // as clear would not copy-on-write an empty array.
        m_free_positions.copy_on_write();
        m_free_lengths.copy_on_write();
        m_free_versions.copy_on_write();
    }
    // At this point the arrays holding the freelist (in the file) has
    // been released and the arrays have been allocated in slab. This ensures
    // that manipulation of the arrays at a later time will NOT trigger a
    // release of free space in the file.
#if REALM_ALLOC_DEBUG
    std::cout << "  Freelist (pinned): ";
    for (auto e : m_not_free_in_file) {
        std::cout << "[" << e.ref << ", " << e.size << "] <" << e.released_at_version << ">  ";
    }
    std::cout << std::endl;
#endif

    merge_adjacent_entries_in_freelist(m_under_evacuation);
    m_under_evacuation.erase(std::remove_if(m_under_evacuation.begin(), m_under_evacuation.end(),
                                            [](const auto& a) {
                                                return a.size == 0;
                                            }),
                             m_under_evacuation.end());
    merge_adjacent_entries_in_freelist(free_in_file);
    // Previous step produces - potentially - some entries with size of zero. These
    // entries will be skipped in the next step.
    move_free_in_file_to_size_map(free_in_file, m_size_map);
}

std::vector<GroupWriter::AugmentedFreeSpaceEntry> GroupWriter::create_combined_freelist()
{
    std::vector<AugmentedFreeSpaceEntry> free_in_file;
    auto& new_free_space = m_group.m_alloc.get_free_read_only(); // Throws
    auto nb_elements =
        m_size_map.size() + m_locked_in_file.size() + m_under_evacuation.size() + new_free_space.size();
    free_in_file.reserve(nb_elements);

    for (const auto& entry : m_size_map) {
        free_in_file.emplace_back(entry.second, entry.first, 0, AugmentedFreeSpaceEntry::Source::FreeInFile);
    }

    {
        size_t locked_space_size = 0;
        for (const auto& locked : m_locked_in_file) {
            free_in_file.emplace_back(locked.ref, locked.size, locked.released_at_version,
                                      AugmentedFreeSpaceEntry::Source::LockedInFile);
            locked_space_size += locked.size;
        }

        for (const auto& free_space : new_free_space) {
            free_in_file.emplace_back(free_space.first, free_space.second, m_current_version,
                                      AugmentedFreeSpaceEntry::Source::InTransaction);
            locked_space_size += free_space.second;
        }
        m_locked_space_size = locked_space_size;
    }

    for (const auto& elem : m_under_evacuation) {
        free_in_file.emplace_back(elem.ref, elem.size, 0, AugmentedFreeSpaceEntry::Source::Evacuating);
    }

    REALM_ASSERT(free_in_file.size() == nb_elements);
    std::sort(begin(free_in_file), end(free_in_file), [](auto& a, auto& b) {
        return a.ref < b.ref;
    });

    return free_in_file;
}

void GroupWriter::verify_no_overlaps(std::vector<AugmentedFreeSpaceEntry>& free_in_file)
{
    size_t prev_ref = 0;
    size_t prev_size = 0;
    auto prev_source = AugmentedFreeSpaceEntry::Source::Unknown;
    auto limit = free_in_file.size();
    for (size_t i = 0; i < limit; ++i) {
        const auto& free_space = free_in_file[i];
        auto ref = free_space.ref;
        auto source = free_space.source;
        // Overlap detected ?:
        REALM_ASSERT_RELEASE_EX(prev_ref + prev_size <= ref, prev_ref, prev_size, ref,
                                AugmentedFreeSpaceEntry::source_to_string[int(prev_source)],
                                AugmentedFreeSpaceEntry::source_to_string[int(source)],
                                m_alloc.get_file_path_for_assertions());
        prev_ref = ref;
        prev_size = free_space.size;
        prev_source = source;
    }
}

void GroupWriter::verify_freelists()
{
    auto freelist = create_combined_freelist();
    verify_no_overlaps(freelist);
}

size_t GroupWriter::recreate_freelist(size_t reserve_pos)
{
    auto free_in_file = create_combined_freelist();
    verify_no_overlaps(free_in_file);
    size_t reserve_ndx = realm::npos;
    // Copy into arrays
    size_t free_space_size = 0;
    auto limit = free_in_file.size();
    for (size_t i = 0; i < limit; ++i) {
        const auto& free_space = free_in_file[i];
        auto ref = free_space.ref;
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
        m_free_versions.add(free_space.released_at_version);
    }
    REALM_ASSERT_RELEASE(reserve_ndx != realm::npos);

    m_free_space_size = free_space_size;

    return reserve_ndx;
}

void GroupWriter::merge_adjacent_entries_in_freelist(std::vector<GroupWriter::FreeSpaceEntry>& list)
{
    if (list.size() > 1) {
        // Combine any adjacent chunks in the freelist
        auto prev = list.begin();
        auto end = list.end();
        for (auto it = list.begin() + 1; it != end; ++it) {
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

void GroupWriter::move_free_in_file_to_size_map(const std::vector<GroupWriter::FreeSpaceEntry>& list,
                                                std::multimap<size_t, size_t>& size_map)
{
    ALLOC_DBG_COUT("  Freelist (true free): ");
    for (auto& elem : list) {
        // Skip elements merged in 'merge_adjacent_entries_in_freelist'
        if (elem.size) {
            REALM_ASSERT_RELEASE_EX(!(elem.size & 7), elem.size);
            REALM_ASSERT_RELEASE_EX(!(elem.ref & 7), elem.ref);
            size_map.emplace(elem.size, elem.ref);
            ALLOC_DBG_COUT("[" << elem.ref << ", " << elem.size << "] ");
        }
    }
    ALLOC_DBG_COUT(std::endl);
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
    ALLOC_DBG_COUT("    alloc [" << alloc_pos << ", " << size << "]" << std::endl);
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
    REALM_ASSERT_RELEASE(m_allocation_allowed);
    auto chunk = search_free_space_in_part_of_freelist(size);
    while (chunk == m_size_map.end()) {
        if (!m_under_evacuation.empty()) {
            // We have been too aggressive in setting the evacuation limit
            // Just give up
            // But first we will release all kept back elements
            for (auto& elem : m_under_evacuation) {
                m_size_map.emplace(elem.size, elem.ref);
            }
            m_under_evacuation.clear();
            m_evacuation_limit = 0;
            m_backoff = 10;
            if (auto logger = m_group.get_logger()) {
                logger->log(util::Logger::Level::detail, "Give up compaction");
            }
            chunk = search_free_space_in_part_of_freelist(size);
        }
        else {
            // No free space, so we have to extend the file.
            auto new_chunk = extend_free_space(size);
            chunk = search_free_space_in_free_list_element(new_chunk, size);
        }
    }
    return chunk;
}

// Extend the free space with at least the requested size.
// Due to mmap constraints, the extension can not be guaranteed to
// allow an allocation of the requested size, so multiple calls to
// extend_free_space may be needed, before an allocation can succeed.
GroupWriter::FreeListElement GroupWriter::extend_free_space(size_t requested_size)
{
    // We need to consider the "logical" size of the file here, and not the real
    // size. The real size may have changed without the free space information
    // having been adjusted accordingly. This can happen, for example, if
    // write_group() fails before writing the new top-ref, but after having
    // extended the file size. It can also happen as part of initial file expansion
    // during attach_file().
    size_t logical_file_size = to_size_t(m_group.m_top.get(2) / 2);
    // find minimal new size according to the following growth ratios:
    // at least 100% (doubling) until we reach 1MB, then just grow with 1MB at a time
    uint64_t minimal_new_size = logical_file_size;
    constexpr uint64_t growth_boundary = 1024 * 1024; // 1MB
    if (minimal_new_size < growth_boundary) {
        minimal_new_size *= 2;
    }
    else {
        minimal_new_size += growth_boundary;
    }
    // grow with at least the growth ratio, but if more is required, grow more
    uint64_t required_new_size = logical_file_size + requested_size;
    if (required_new_size > minimal_new_size) {
        minimal_new_size = required_new_size;
    }
    // Ensure that minimal_new_size is less than 3 GB on a 32 bit device
    if (minimal_new_size > (std::numeric_limits<size_t>::max() / 4 * 3)) {
        throw MaximumFileSizeExceeded("GroupWriter cannot extend free space: " + util::to_string(logical_file_size) +
                                      " + " + util::to_string(requested_size));
    }

    // We now know that it is safe to assign size to something of size_t
    // and we know that the following adjustments are safe to perform
    size_t new_file_size = static_cast<size_t>(minimal_new_size);

    // align to page size, but do not cross a section boundary
    size_t next_boundary = m_alloc.align_size_to_section_boundary(new_file_size);
    new_file_size = util::round_up_to_page_size(new_file_size);
    if (new_file_size > next_boundary) {
        // we cannot cross a section boundary. In this case the allocation will
        // likely fail, then retry and we'll allocate anew from the next section
        new_file_size = next_boundary;
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
    REALM_ASSERT(new_file_size <= get_file_size());
    ALLOC_DBG_COUT("        ** File extension to " << new_file_size << "     after request for " << requested_size
                                                   << std::endl);

    // as new_file_size is larger than logical_file_size, but known to
    // be representable in a size_t, so is the result:
    size_t chunk_size = new_file_size - logical_file_size;
    REALM_ASSERT_RELEASE_EX(!(chunk_size & 7), chunk_size);
    REALM_ASSERT_RELEASE(chunk_size != 0);
    auto it = m_size_map.emplace(chunk_size, logical_file_size);

    // Update the logical file size
    m_logical_size = new_file_size;
    m_group.m_top.set(Group::s_file_size_ndx, RefOrTagged::make_tagged(m_logical_size));

    // std::cout << "New file size = " << std::hex << m_logical_size << std::dec << std::endl;

    return it;
}

bool inline is_aligned(char* addr)
{
    size_t as_binary = reinterpret_cast<size_t>(addr);
    return (as_binary & 7) == 0;
}

ref_type GroupWriter::write_array(const char* data, size_t size, uint32_t checksum)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);

    // Write the block
    MapWindow* window = m_window_mgr.get_window(pos, size);
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

template <class T>
void GroupWriter::write_array_at(T* translator, ref_type ref, const char* data, size_t size)
{
    size_t pos = size_t(ref);

    REALM_ASSERT_RELEASE(pos + size <= to_size_t(m_group.m_top.get(2) / 2));
    // REALM_ASSERT_3(pos + size, <=, m_file_map.get_size());
    char* dest_addr = translator->translate(pos);
    REALM_ASSERT_RELEASE(is_aligned(dest_addr));

    uint32_t dummy_checksum = 0x41414141UL; // "AAAA" in ASCII
    memcpy(dest_addr, &dummy_checksum, 4);
    memcpy(dest_addr + 4, data + 4, size - 4);
}


void GroupCommitter::commit(ref_type new_top_ref)
{
    using _impl::SimulatedFailure;
    SimulatedFailure::trigger(SimulatedFailure::group_writer__commit); // Throws

    MapWindow* window = m_window_mgr.get_window(0, sizeof(SlabAlloc::Header));
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
    // only write the file format field if necessary (optimization)
    if (type_1(file_format_version) != file_header.m_file_format[slot_selector]) {
        // write barrier on the entire `file_header` happens below
        file_header.m_file_format[slot_selector] = type_1(file_format_version);
    }

    // When running the test suite, device synchronization is disabled
    bool disable_sync = get_disable_sync_to_disk() || m_durability == Durability::Unsafe;
    file_header.m_top_ref[slot_selector] = new_top_ref;

    // Make sure that that all data relating to the new snapshot is written to
    // stable storage before flipping the slot selector
    window->encryption_write_barrier(&file_header, sizeof file_header);
    m_window_mgr.flush_all_mappings();
    if (!disable_sync) {
        m_window_mgr.sync_all_mappings();
        m_alloc.get_file().barrier();
    }

    // Flip the slot selector bit.
    window->encryption_read_barrier(&file_header, sizeof file_header);
    using type_2 = std::remove_reference<decltype(file_header.m_flags)>::type;
    file_header.m_flags = type_2(new_flags);

    // Write new selector to disk
    window->encryption_write_barrier(&file_header.m_flags, sizeof(file_header.m_flags));
    window->flush();
    if (!disable_sync) {
        window->sync();
        m_alloc.get_file().barrier();
    }
}


#ifdef REALM_DEBUG

void GroupWriter::dump()
{
    size_t count = m_free_lengths.size();
    std::cout << "count: " << count << ", m_size = " << m_alloc.get_file_size() << ", "
              << "version >= " << m_oldest_reachable_version << "\n";
    for (size_t i = 0; i < count; ++i) {
        std::cout << i << ": " << m_free_positions.get(i) << ", " << m_free_lengths.get(i) << " - "
                  << m_free_versions.get(i) << "\n";
    }
}

#endif
