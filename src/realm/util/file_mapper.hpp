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

namespace realm::util {
struct FileAttributes {
    FileDesc fd;
    File::AccessMode access;
    EncryptedFile* encryption;
};

class EncryptedFileMapping;

void* mmap(const FileAttributes& file, size_t size, uint64_t offset, std::unique_ptr<EncryptedFileMapping>& mapping);
void* mmap_fixed(FileDesc fd, void* address_request, size_t size, File::AccessMode access, uint64_t offset);
void munmap(void* addr, size_t size);
void msync(FileDesc fd, void* addr, size_t size);
void* mmap_anon(size_t size);

#if REALM_ENABLE_ENCRYPTION

void* mmap_fixed(FileDesc fd, void* address_request, size_t size, File::AccessMode access, uint64_t offset);

std::unique_ptr<EncryptedFileMapping> reserve_mapping(void* addr, const FileAttributes& file, uint64_t offset);

void do_encryption_read_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping, bool to_modify);
void do_encryption_write_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping);

#else

inline void do_encryption_read_barrier(const void*, size_t, EncryptedFileMapping*, bool) {}
inline void do_encryption_write_barrier(const void*, size_t, EncryptedFileMapping*) {}

#endif

inline void encryption_read_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping)
{
    if (REALM_UNLIKELY(mapping))
        do_encryption_read_barrier(addr, size, mapping, false);
}

inline void encryption_read_barrier_for_write(const void* addr, size_t size, EncryptedFileMapping* mapping)
{
    if (REALM_UNLIKELY(mapping))
        do_encryption_read_barrier(addr, size, mapping, true);
}

inline void encryption_write_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping)
{
    if (REALM_UNLIKELY(mapping))
        do_encryption_write_barrier(addr, size, mapping);
}

// helpers for encrypted Maps
template <typename T>
void encryption_read_barrier(const File::Map<T>& map, size_t index, size_t num_elements = 1)
{
    if (auto mapping = map.get_encrypted_mapping(); REALM_UNLIKELY(mapping)) {
        do_encryption_read_barrier(map.get_addr() + index, sizeof(T) * num_elements, mapping, map.is_writeable());
    }
}

template <typename T>
void encryption_write_barrier(const File::Map<T>& map, size_t index, size_t num_elements = 1)
{
    if (auto mapping = map.get_encrypted_mapping(); REALM_UNLIKELY(mapping)) {
        do_encryption_write_barrier(map.get_addr() + index, sizeof(T) * num_elements, mapping);
    }
}

File::SizeType encrypted_size_to_data_size(File::SizeType size) noexcept;
File::SizeType data_size_to_encrypted_size(File::SizeType size) noexcept;

size_t round_up_to_page_size(size_t size) noexcept;
} // namespace realm::util
#endif
