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

#ifndef REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP
#define REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP

#include <realm/util/file.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/features.h>

#if REALM_ENABLE_ENCRYPTION

typedef size_t (*Header_to_size)(const char* addr);

#include <vector>

namespace realm {
namespace util {

struct SharedFileInfo;
class EncryptedFileMapping;

class EncryptedFileMapping {
public:
    // Adds the newly-created object to file.mappings iff it's successfully constructed
    EncryptedFileMapping(SharedFileInfo& file, size_t file_offset, void* addr, size_t size, File::AccessMode access);
    ~EncryptedFileMapping();

    // Default implementations of copy/assign can trigger multiple destructions
    EncryptedFileMapping(const EncryptedFileMapping&) = delete;
    EncryptedFileMapping& operator=(const EncryptedFileMapping&) = delete;

    // Write all dirty pages to disk and mark them read-only
    // Does not call fsync
    void flush() noexcept;

    // Sync this file to disk
    void sync() noexcept;

    // Make sure that memory in the specified range is synchronized with any
    // changes made globally visible through call to write_barrier
    void read_barrier(const void* addr, size_t size, Header_to_size header_to_size);

    // Ensures that any changes made to memory in the specified range
    // becomes visible to any later calls to read_barrier()
    void write_barrier(const void* addr, size_t size) noexcept;

    // Set this mapping to a new address and size
    // Flushes any remaining dirty pages from the old mapping
    void set(void* new_addr, size_t new_size, size_t new_file_offset);

    // Mark all pages untouched. Later accesses will mark individual pages as touched again
    size_t mark_untouched() noexcept;
    size_t collect_decryption_count() { return m_num_decrypted; }
    // reclaim any untouched pages - this is thread safe with respect to
    // concurrent access/touching of pages - but must be called with the mutex locked.
    size_t reclaim_untouched(size_t& progress_ptr, size_t& accumulated_savings) noexcept;

    // Get statistics
    void count_usage(size_t& touched, size_t& updated, size_t& dirty, size_t& total);

    bool contains_page(size_t page_in_file) const;
    size_t get_local_index_of_address(const void* addr, size_t offset = 0) const;

    size_t get_end() { return m_first_page + m_page_state.size(); }
private:
    SharedFileInfo& m_file;

    size_t m_page_shift;
    size_t m_blocks_per_page;

    void* m_addr = nullptr;

    size_t m_first_page;
    size_t m_num_decrypted; // 1 for every page decrypted

    enum PageState {
    	Touched = 1,
    	UpToDate = 2,
    	PartiallyUpToDate = 4,
    	Dirty = 8
    };
    inline void clear(PageState& ps, int p)
    {
    	ps = PageState(ps & ~p);
    }
    inline bool is_not(PageState& ps, int p)
    {
    	return (ps & p) == 0;
    }
    inline bool is(PageState& ps, int p)
    {
    	return (ps & p) != 0;
    }
    inline void set(PageState& ps, int p)
    {
    	ps = PageState(ps | p);
    }
    std::vector<PageState> m_page_state;
    // m_up_to_date_pages track the state of each page. It can be
    // - unused / not up to data (0), up_to_date (1), partially up to date (2)
    // std::vector<char> m_up_to_date_pages;
    // std::vector<char> m_touched;
    // 1K pages form a chunk - this array allows us to skip entire chunks during scanning
    std::vector<bool> m_chunk_dont_scan;
    // std::vector<bool> m_dirty_pages;

    File::AccessMode m_access;

#ifdef REALM_DEBUG
    std::unique_ptr<char[]> m_validate_buffer;
#endif

    char* page_addr(size_t local_page_ndx) const noexcept;

    void mark_outdated(size_t local_page_ndx) noexcept;
    bool copy_up_to_date_page(size_t local_page_ndx) noexcept;
    void refresh_page(size_t local_page_ndx);
    void write_page(size_t local_page_ndx) noexcept;

    void validate_page(size_t local_page_ndx) noexcept;
    void validate() noexcept;
};

inline size_t EncryptedFileMapping::get_local_index_of_address(const void* addr, size_t offset) const
{
    REALM_ASSERT_EX(addr >= m_addr, addr, m_addr);

    size_t local_ndx = ((reinterpret_cast<uintptr_t>(addr) - reinterpret_cast<uintptr_t>(m_addr) + offset) >> m_page_shift);
    REALM_ASSERT_EX(local_ndx < m_page_state.size(), local_ndx, m_page_state.size());
    return local_ndx;
}

inline bool EncryptedFileMapping::contains_page(size_t page_in_file) const
{
    // first check for (page_in_file >= m_first_page) so that the following
    // subtraction using unsigned types never wraps under 0
    return page_in_file >= m_first_page && page_in_file - m_first_page < m_page_state.size();
}


}
}

#endif // REALM_ENABLE_ENCRYPTION

namespace realm {
namespace util {

/// Thrown by EncryptedFileMapping if a file opened is non-empty and does not
/// contain valid encrypted data
struct DecryptionFailed : util::File::AccessError {
    DecryptionFailed()
        : util::File::AccessError("Decryption failed", std::string())
    {
    }
};
}
}

#endif // REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP
