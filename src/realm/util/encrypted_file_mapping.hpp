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
    void read_barrier(const void* addr, size_t size, UniqueLock& lock, Header_to_size header_to_size);

    // Ensures that any changes made to memory in the specified range
    // becomes visible to any later calls to read_barrier()
    void write_barrier(const void* addr, size_t size) noexcept;

    // Set this mapping to a new address and size
    // Flushes any remaining dirty pages from the old mapping
    void set(void* new_addr, size_t new_size, size_t new_file_offset);

    bool contains_page(size_t page_in_file) const;
    size_t get_num_pages_for_size(size_t size) const;

private:
    SharedFileInfo& m_file;

    size_t m_page_shift;
    size_t m_blocks_per_page;

    void* m_addr = nullptr;

    size_t m_first_page;

    std::vector<bool> m_up_to_date_pages;
    std::vector<bool> m_dirty_pages;

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


inline size_t EncryptedFileMapping::get_num_pages_for_size(size_t size) const
{
    if (size == 0) {
        // If size is zero just refresh 1 page. This is also an early out
        // to avoid subtraction wrapping under zero for (size - 1).
        return 1;
    } else if (size % (1 << m_page_shift) == 0) {
        // When the size is a multiple of the page size a shift is sufficient.
        return size >> m_page_shift;
    } else {
        // In the normal case (size > 0 and not a multiple of the page size),
        // the size is midway through a page. In this case we must add 1 page
        // to the result becuase we must still read that whole page but the
        // shift (division) returns the floor of the result.
        // Use (size - 1) before the shift because we start reading at byte 0.
        return ((size - 1) >> m_page_shift) + 1;
    }
}

inline bool EncryptedFileMapping::contains_page(size_t page_in_file) const
{
    // first check for (page_in_file >= m_first_page) so that the following
    // subtraction using unsigned types never wraps under 0
    return page_in_file >= m_first_page && page_in_file - m_first_page < m_up_to_date_pages.size();
}

inline void EncryptedFileMapping::read_barrier(const void* addr, size_t size, UniqueLock& lock,
                                               Header_to_size header_to_size)
{
    REALM_ASSERT_EX(addr >= m_addr, addr, m_addr);
    size_t first_accessed_local_page = ((reinterpret_cast<uintptr_t>(addr) - reinterpret_cast<uintptr_t>(m_addr)) >> m_page_shift);

    // make sure the first page is available
    // Checking before taking the lock is important to performance.
    if (!m_up_to_date_pages[first_accessed_local_page]) {
        if (!lock.holds_lock())
            lock.lock();
        // after taking the lock, we must repeat the check so that we never
        // call refresh_page() on a page which is already up to date.
        if (!m_up_to_date_pages[first_accessed_local_page])
            refresh_page(first_accessed_local_page);
    }

    if (header_to_size) {

        // We know it's an array, and array headers are 8-byte aligned, so it is
        // included in the first page which was handled above.
        size = header_to_size(static_cast<const char*>(addr));
    }

    size_t num_pages = get_num_pages_for_size(size);
    size_t last_idx = first_accessed_local_page + num_pages;
    size_t up_to_date_pages_size = m_up_to_date_pages.size();

    // We already checked first_accessed_local_page above, so we start the loop
    // at first_accessed_local_page + 1 to check the following page.
    for (size_t idx = first_accessed_local_page + 1; idx <= last_idx && idx < up_to_date_pages_size; ++idx) {
        if (!m_up_to_date_pages[idx]) {
            if (!lock.holds_lock())
                lock.lock();
            // after taking the lock, we must repeat the check so that we never
            // call refresh_page() on a page which is already up to date.
            if (!m_up_to_date_pages[idx])
                refresh_page(idx);
        }
    }
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
