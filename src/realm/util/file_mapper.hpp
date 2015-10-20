/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_UTIL_FILE_MAPPER_HPP
#define REALM_UTIL_FILE_MAPPER_HPP

#include <realm/util/file.hpp>

namespace realm {
namespace util {

void *mmap(int fd, size_t size, File::AccessMode access, size_t offset, const char *encryption_key);
void munmap(void *addr, size_t size) noexcept;
void* mremap(int fd, size_t file_offset, void* old_addr, size_t old_size, File::AccessMode a, size_t new_size);
void msync(void *addr, size_t size);

#if REALM_ENABLE_ENCRYPTION
void handle_reads(void* addr, size_t size);
void handle_writes(void* addr, size_t size);
#else
void inline handle_reads(void*, size_t) {}
void inline handle_writes(void*, size_t) {}
#endif

// helpers for encrypted Maps
template<typename T>
void handle_reads(File::Map<T>& map, size_t index, size_t num_elements = 1)
{
    T* addr = map.get_addr();
    handle_reads(addr+index, sizeof(T)*num_elements-1);
}

template<typename T>
void handle_writes(File::Map<T>& map, size_t index, size_t num_elements = 1)
{
    T* addr = map.get_addr();
    handle_writes(addr+index, sizeof(T)*num_elements-1);
}

File::SizeType encrypted_size_to_data_size(File::SizeType size) noexcept;
File::SizeType data_size_to_encrypted_size(File::SizeType size) noexcept;

size_t round_up_to_page_size(size_t size) noexcept;

}
}
#endif
