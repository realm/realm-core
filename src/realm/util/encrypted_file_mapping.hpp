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

#include <realm/util/aes_cryptor.hpp>
#include <realm/util/checked_mutex.hpp>
#include <realm/util/file.hpp>

#include <vector>

namespace realm::util {

#if REALM_ENABLE_ENCRYPTION

class EncryptedFileMapping;

class EncryptedFile {
public:
    EncryptedFile(const char* key, FileDesc fd);

    std::unique_ptr<EncryptedFileMapping> add_mapping(File::SizeType file_offset, void* addr, size_t size,
                                                      File::AccessMode access) REQUIRES(!mutex);

    const char* get_key() const noexcept REQUIRES(!mutex)
    {
        // It's safe to return a pointer into cryptor outside the lock because
        // the key doesn't actually change and doesn't need to be guarded by
        // the mutex at all.
        util::CheckedLockGuard lock(mutex);
        return cryptor.get_key();
    }

    void mark_data_as_possibly_stale() REQUIRES(!mutex);

private:
    friend class EncryptedFileMapping;

    CheckedMutex mutex;
    FileDesc fd;
    AESCryptor cryptor GUARDED_BY(mutex);
    std::vector<EncryptedFileMapping*> mappings GUARDED_BY(mutex);
};

class EncryptedFileMapping {
public:
    EncryptedFileMapping(EncryptedFile& file, File::SizeType file_offset, void* addr, size_t size,
                         File::AccessMode access, util::WriteObserver* observer = nullptr,
                         util::WriteMarker* marker = nullptr);
    ~EncryptedFileMapping();

    // Default implementations of copy/assign can trigger multiple destructions
    EncryptedFileMapping(const EncryptedFileMapping&) = delete;
    EncryptedFileMapping& operator=(const EncryptedFileMapping&) = delete;

    // Encrypt all dirty blocks, push them to shared cache and mark them read-only
    // Does not call fsync
    void flush(bool skip_validate) noexcept REQUIRES(!m_file.mutex);

    // Flush and then sync the image of this file in shared cache to disk.
    void sync() noexcept REQUIRES(!m_file.mutex);

    // Make sure that memory in the specified range is synchronized with any
    // changes made globally visible through call to write_barrier or refresh_outdated_pages().
    // Optionally mark the pages for later modification
    void read_barrier(const void* addr, size_t size, bool to_modify) REQUIRES(!m_file.mutex);

    // Ensures that any changes made to memory in the specified range
    // becomes visible to any later calls to read_barrier()
    // Pages selected must have been marked for modification at an earlier read barrier
    void write_barrier(const void* addr, size_t size) noexcept REQUIRES(!m_file.mutex);

    // Set this mapping to a new address and size
    // Flushes any remaining dirty pages from the old mapping
    void set(void* new_addr, size_t new_size, File::SizeType new_file_offset) REQUIRES(!m_file.mutex);

    // Extend the size of this mapping. Memory holding decrypted pages must
    // have been allocated earlier
    void extend_to(File::SizeType offset, size_t new_size) REQUIRES(!m_file.mutex);

    bool contains_page(size_t block_in_file) const noexcept REQUIRES(m_file.mutex);
    size_t get_local_index_of_address(const void* addr, size_t offset = 0) const noexcept REQUIRES(m_file.mutex);
    uint16_t get_offset_of_address(const void* addr) const noexcept REQUIRES(m_file.mutex);

    void set_marker(WriteMarker* marker) noexcept
    {
        m_marker = marker;
    }
    void set_observer(WriteObserver* observer) noexcept
    {
        m_observer = observer;
    }
    std::string print_debug() REQUIRES(!m_file.mutex);

private:
    friend class EncryptedFile;

    EncryptedFile& m_file;
    void* m_addr GUARDED_BY(m_file.mutex) = nullptr;
    size_t m_first_page GUARDED_BY(m_file.mutex);

    enum PageState : uint8_t {
        Clean = 0,
        UpToDate = 1, // the page is fully up to date
        StaleIV = 2,  // the page needs to check the on disk IV for changes by other processes
        Writable = 4, // the page is open for writing
        Dirty = 8     // the page has been modified with respect to what's on file.
    };
    std::vector<PageState> m_page_state GUARDED_BY(m_file.mutex);
    // little helpers:
    static constexpr void clear(PageState& ps, int p)
    {
        ps = PageState(ps & ~p);
    }
    static constexpr bool is_not(PageState& ps, int p)
    {
        return (ps & p) == 0;
    }
    static constexpr bool is(PageState& ps, int p)
    {
        return (ps & p) != 0;
    }
    static constexpr void set(PageState& ps, int p)
    {
        ps = PageState(ps | p);
    }

    const File::AccessMode m_access;
    util::WriteObserver* m_observer = nullptr;
    util::WriteMarker* m_marker = nullptr;
#ifdef REALM_DEBUG
    std::unique_ptr<char[]> m_validate_buffer GUARDED_BY(m_file.mutex);
#endif

    char* page_addr(size_t local_ndx) const noexcept REQUIRES(m_file.mutex);
    File::SizeType page_pos(size_t local_ndx) const noexcept REQUIRES(m_file.mutex);
    bool copy_up_to_date_page(size_t local_ndx) noexcept REQUIRES(m_file.mutex);
    bool check_possibly_stale_page(size_t local_ndx) noexcept REQUIRES(m_file.mutex);
    void refresh_page(size_t local_ndx, bool to_modify) REQUIRES(m_file.mutex);
    void write_and_update_all(size_t local_ndx, uint16_t offset, uint16_t size) noexcept REQUIRES(m_file.mutex);
    void validate_page(size_t local_ndx) noexcept REQUIRES(m_file.mutex);
    void validate() noexcept REQUIRES(m_file.mutex);
    void do_flush(bool skip_validate = false) noexcept REQUIRES(m_file.mutex);
    void do_sync() noexcept REQUIRES(m_file.mutex);
    REALM_NORETURN void throw_decryption_error(size_t ndx, std::string_view msg) REQUIRES(m_file.mutex);

    // Mark pages for later checks of the ivs on disk. If the IVs have changed compared to
    // the in memory versions the page will later need to be refreshed.
    // This is the process by which a reader in a multiprocess scenario detects if its
    // mapping should be refreshed while advancing versions.
    // The pages marked for IV-checks will be refetched and re-decrypted by later calls to read_barrier.
    void mark_pages_for_iv_check() REQUIRES(m_file.mutex);

    void assert_locked() noexcept ASSERT_CAPABILITY(m_file.mutex) {}
};

// LCOV_EXCL_START
inline std::string EncryptedFileMapping::print_debug()
{
#if REALM_DEBUG
    auto state_name = [](const PageState& s) -> std::string {
        if (s == PageState::Clean) {
            return "Clean";
        }
        std::string state = "{";
        if (s & PageState::UpToDate) {
            state += "UpToDate";
        }
        if (s & PageState::StaleIV) {
            state += "StaleIV";
        }
        if (s & PageState::Writable) {
            state += "Writable";
        }
        if (s & PageState::Dirty) {
            state += "Dirty";
        }
        state += "}";
        return state;
    };

    util::CheckedLockGuard lock(m_file.mutex);
    std::string page_states;
    for (PageState& s : m_page_state) {
        if (!page_states.empty()) {
            page_states += ", ";
        }
        page_states += state_name(s);
    }
    return util::format("%1 pages from %2 to %3: %4", m_page_state.size(), m_first_page,
                        m_page_state.size() + m_first_page, page_states);
#else
    return "";
#endif // REALM_DEBUG
}
// LCOV_EXCL_STOP

constexpr inline File::SizeType c_min_encrypted_file_size = 8192;

#else  // REALM_ENABLE_ENCRYPTION
class EncryptedFile {
public:
    static void mark_data_as_possibly_stale() noexcept {}
};
class EncryptedFileMapping {};
#endif // REALM_ENABLE_ENCRYPTION

/// Thrown by EncryptedFileMapping if a file opened is non-empty and does not
/// contain valid encrypted data
struct DecryptionFailed : FileAccessError {
    DecryptionFailed(const std::string& msg)
        : FileAccessError(ErrorCodes::DecryptionFailed, get_message_with_bt(msg), std::string())
    {
    }
    static std::string get_message_with_bt(std::string_view msg);
};
} // namespace realm::util

#endif // REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP
