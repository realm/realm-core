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

#ifndef REALM_UTIL_FILE_MAPPER_HPP
#define REALM_UTIL_FILE_MAPPER_HPP

#include <realm/util/file.hpp>
#include <realm/utilities.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/functional.hpp>

#include <unordered_map>
#include <unordered_set>

namespace realm {
namespace util {

struct FileAttributes {
    FileDesc fd;
    std::string path;
    const char* cause;
    File::AccessMode access;
    const char* encryption_key = nullptr;
};

void* mmap(const FileAttributes& file, size_t size, size_t offset);
void* mmap_fixed(const FileAttributes& file, void* address_request, size_t size, File::AccessMode access,
                 size_t offset, const char* enc_key);
void* mmap_reserve(const FileAttributes& fd, size_t size, size_t offset);
void munmap(void* addr, size_t size);
void* mremap(const FileAttributes& file, size_t file_offset, void* old_addr, size_t old_size, size_t new_size);
void msync(FileDesc fd, void* addr, size_t size);
void* mmap_anon(size_t size, const char* cause);

// A function which may be given to encryption_read_barrier. If present, the read barrier is a
// a barrier for a full array. If absent, the read barrier is a barrier only for the address
// range give as argument. If the barrier is for a full array, it will read the array header
// and determine the address range from the header.
using HeaderToSize = size_t (*)(const char* addr);
class EncryptedFileMapping;

// Retrieves the number of in memory decrypted pages, across all open files.
size_t get_num_decrypted_pages();

#if REALM_ENABLE_ENCRYPTION

// This variant allows the caller to obtain direct access to the encrypted file mapping
// for optimization purposes.
void* mmap(const FileAttributes& file, size_t size, size_t offset, EncryptedFileMapping*& mapping);

void* mmap_reserve(const FileAttributes& file, size_t size, size_t offset, EncryptedFileMapping*& mapping);

EncryptedFileMapping* reserve_mapping(void* addr, const FileAttributes& file, size_t offset);

void extend_encrypted_mapping(EncryptedFileMapping* mapping, void* addr, size_t offset, size_t old_size,
                              size_t new_size);
void remove_encrypted_mapping(void* addr, size_t size);
void do_encryption_read_barrier(const void* addr, size_t size, HeaderToSize header_to_size,
                                EncryptedFileMapping* mapping, bool to_modify);

void do_encryption_write_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping);

void inline encryption_read_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping,
                                    HeaderToSize header_to_size = nullptr, bool to_modify = false)
{
    if (REALM_UNLIKELY(mapping))
        do_encryption_read_barrier(addr, size, header_to_size, mapping, to_modify);
}

void inline encryption_read_barrier_for_write(const void* addr, size_t size, EncryptedFileMapping* mapping)
{
    if (REALM_UNLIKELY(mapping))
        do_encryption_read_barrier(addr, size, nullptr, mapping, true);
}

void inline encryption_write_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping)
{
    if (REALM_UNLIKELY(mapping))
        do_encryption_write_barrier(addr, size, mapping);
}


extern util::Mutex& mapping_mutex;

void inline encryption_flush(EncryptedFileMapping* mapping)
{
    UniqueLock lock(mapping_mutex);
    mapping->flush();
}

inline void do_encryption_read_barrier(const void* addr, size_t size, HeaderToSize header_to_size,
                                       EncryptedFileMapping* mapping, bool to_modify)
{
    UniqueLock lock(mapping_mutex);
    mapping->read_barrier(addr, size, header_to_size, to_modify);
}

inline void do_encryption_write_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping)
{
    LockGuard lock(mapping_mutex);
    mapping->write_barrier(addr, size);
}

#else


size_t inline get_num_decrypted_pages()
{
    return 0;
}

void inline encryption_read_barrier(const void*, size_t, EncryptedFileMapping*, HeaderToSize = nullptr) {}
void inline encryption_read_barrier_for_write(const void*, size_t, EncryptedFileMapping*) {}
void inline encryption_write_barrier(const void*, size_t) {}
void inline encryption_write_barrier(const void*, size_t, EncryptedFileMapping*) {}
void inline do_encryption_read_barrier(const void*, size_t, HeaderToSize, EncryptedFileMapping*, bool) {}
void inline do_encryption_write_barrier(const void*, size_t, EncryptedFileMapping*) {}

#endif

// helpers for encrypted Maps
template <typename T>
void encryption_read_barrier(const File::Map<T>& map, size_t index, size_t num_elements = 1)
{
    if (auto mapping = map.get_encrypted_mapping(); REALM_UNLIKELY(mapping)) {
        do_encryption_read_barrier(map.get_addr() + index, sizeof(T) * num_elements, nullptr, mapping,
                                   map.is_writeable());
    }
}

template <typename T>
void encryption_read_barrier_for_write(const File::Map<T>& map, size_t index, size_t num_elements = 1)
{
    if (auto mapping = map.get_encrypted_mapping(); REALM_UNLIKELY(mapping)) {
        do_encryption_read_barrier(map.get_addr() + index, sizeof(T) * num_elements, nullptr, mapping,
                                   map.is_writeable());
    }
}

template <typename T>
void encryption_write_barrier(const File::Map<T>& map, size_t index, size_t num_elements = 1)
{
    if (auto mapping = map.get_encrypted_mapping(); REALM_UNLIKELY(mapping)) {
        do_encryption_write_barrier(map.get_addr() + index, sizeof(T) * num_elements, mapping);
    }
}
void encryption_mark_pages_for_IV_check(EncryptedFileMapping* mapping);

File::SizeType encrypted_size_to_data_size(File::SizeType size) noexcept;
File::SizeType data_size_to_encrypted_size(File::SizeType size) noexcept;

size_t round_up_to_page_size(size_t size) noexcept;
} // namespace util
} // namespace realm
#endif
