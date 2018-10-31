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

#include <realm/util/features.h>

#include <realm/util/file_mapper.hpp>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <sys/mman.h>
#include <unistd.h>
#endif

#include <realm/util/errno.hpp>
#include <realm/util/to_string.hpp>
#include <realm/exceptions.hpp>
#include <system_error>

#if REALM_ENABLE_ENCRYPTION

#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/aes_cryptor.hpp>

#include <memory>
#include <csignal>
#include <sys/stat.h>
#include <cstring>
#include <atomic>
#include <iostream>
#include <thread>

#include <realm/util/file.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/shared_ptr.hpp>
#include <realm/util/terminate.hpp>
#include <realm/util/thread.hpp>
#include <cstring> // for memset

#if REALM_PLATFORM_APPLE
#include <mach/mach.h>
#include <mach/exc.h>
#endif

#if REALM_ANDROID
#include <linux/unistd.h>
#include <sys/syscall.h>
#endif

#endif // enable encryption

namespace {

inline bool is_mmap_memory_error(int err)
{
    return (err == EAGAIN || err == EMFILE || err == ENOMEM);
}

} // Unnamed namespace

using namespace realm;
using namespace realm::util;

namespace realm {
namespace util {

#if REALM_ENABLE_ENCRYPTION

// A list of all of the active encrypted mappings for a single file
struct mappings_for_file {
#ifdef _WIN32
    HANDLE handle;
#else
    dev_t device;
    ino_t inode;
#endif
    SharedPtr<SharedFileInfo> info;
};

// Group the information we need to map a SIGSEGV address to an
// EncryptedFileMapping for the sake of cache-friendliness with 3+ active
// mappings (and no worse with only two
struct mapping_and_addr {
    SharedPtr<EncryptedFileMapping> mapping;
    void* addr;
    size_t size;
};

// prevent destruction at exit (which can lead to races if other threads are still running)
util::Mutex& mapping_mutex = *new Mutex;
std::vector<mapping_and_addr>& mappings_by_addr = *new std::vector<mapping_and_addr>;
std::vector<mappings_for_file>& mappings_by_file = *new std::vector<mappings_for_file>;
unsigned int file_reclaim_ptr = 0;

void reclaimer_loop();
std::unique_ptr<std::thread> reclaimer_thread = nullptr;
page_reclaim_governor_t governor = nullptr;

void set_page_reclaim_governor(page_reclaim_governor_t new_governor)
{
	UniqueLock lock(mapping_mutex);
	// start worker thread if it hasn't been started earlier
	if (reclaimer_thread == nullptr) {
		reclaimer_thread.reset(new std::thread(reclaimer_loop));
		reclaimer_thread->detach();
	}
	governor = new_governor;
}

void encryption_note_reader_start(SharedFileInfo& info, void* reader_id)
{
	UniqueLock lock(mapping_mutex);
	std::vector<ReaderInfo>::iterator j;
	for (j = info.readers.begin(); j != info.readers.end(); ++j)
		if (j->reader_ID == reader_id) break;
	if (j == info.readers.end()) {
		ReaderInfo i = {reader_id, info.current_version};
		info.readers.push_back(i);
	}
	else {
		j->version = info.current_version;
	}
	++info.current_version;
}

void encryption_note_reader_end(SharedFileInfo& info, void* reader_id)
{
	UniqueLock lock(mapping_mutex);
	for (auto j = info.readers.begin(); j != info.readers.end(); ++j)
		if (j->reader_ID == reader_id) {
			// move last over
			*j = info.readers.back();
			info.readers.pop_back();
			return;
		}
}

size_t collect_total_workload()  // must be called under lock
{
	size_t total = 0;
	for (auto i = mappings_by_file.begin(); i != mappings_by_file.end(); ++i) {
		SharedFileInfo& info = *i->info;
		info.num_decrypted_pages = 0;
		for (auto it = info.mappings.begin(); it != info.mappings.end(); ++it) {
			info.num_decrypted_pages += (*it)->collect_decryption_count();
		}
		total += info.num_decrypted_pages;
	}
	return total;
}

/* Compute the amount of work allowed in an attempt to realize 'potential'.
 * please refer to EncryptedFileMapping::reclaim_untouched() for more details.
 */
size_t get_work_limit(size_t potential, size_t target) // must be called under lock
{
	// FIXME: Far too many magic constants in here
	size_t work_limit = 0;
	size_t increments = target/16;
	size_t base = target - 4 * increments;
	size_t divisor;
	if (potential > target) divisor = 10;
	else if (potential > target - increments) divisor = 20;
	else if (potential > target - 2*increments) divisor = 50;
	else if (potential > target - 3*increments) divisor = 100;
	else if (potential > base) divisor = 200;
	else return 0;
	work_limit = (potential - base) / divisor;
	return work_limit;
}

/* Find the oldest version that is still of interest to somebody */
uint64_t get_oldest_version(SharedFileInfo& info) // must be called under lock
{
	if (info.readers.size() == 0) {
		return info.current_version;
	}
	else {
		auto oldest_version = info.current_version;
		for (auto j = info.readers.begin(); j != info.readers.end(); ++j) {
			if (j->version < oldest_version)
				oldest_version = j->version;
		}
		return oldest_version;
	}
}

// Reclaim pages for ONE file, limited by a given work limit.
// return true if "done for now"
bool reclaim_pages_for_file(SharedFileInfo& info, size_t& work_limit)
{
	uint64_t oldest_version = get_oldest_version(info);
	if (info.readers.size() == 0 || info.last_scanned_version < oldest_version) {
		size_t sum = 0;
		for (auto it = info.mappings.begin(); it != info.mappings.end() && work_limit; ++it) {
			sum += (*it)->reclaim_untouched(info.progress_ptr, work_limit);
		}
		if (info.progress_ptr >= info.mappings.back()->get_end()) { // done
			info.progress_ptr = 0;
			info.last_scanned_version = info.current_version;
			++info.current_version;
			return true;
		}
		return false;
	}
	return true;
}

// Reclaim pages from all files, limited by a work limit that is derived
// from a target for the amount of dirty (decrypted) pages. The target is
// set by the governor function.
void reclaim_pages()
{
	size_t load;
	{
		UniqueLock lock(mapping_mutex);
		if (governor == nullptr)
			return;
		load = collect_total_workload();
	}
	// callback to governor without mutex held
	size_t target = (*governor)(load * page_size()) / page_size();
	{
		UniqueLock lock(mapping_mutex);
		if (target == 0) // temporarily disabled by governor returning 0
			return;
		size_t work_limit = get_work_limit(load, target);
		if (work_limit == 0)
			return; // nothing to do
		if (file_reclaim_ptr >= mappings_by_file.size())
			file_reclaim_ptr = 0;
		while (work_limit > 0) {
			SharedFileInfo& info = *mappings_by_file[file_reclaim_ptr].info;
			auto done_for_now = reclaim_pages_for_file(info, work_limit);
			if (done_for_now) {
				++file_reclaim_ptr;
				if (file_reclaim_ptr >= mappings_by_file.size())
					return;
			}
		}
	}
}


void reclaimer_loop()
{
	for (;;) {
		reclaim_pages();
		sleep(1);
	}
}

mapping_and_addr* find_mapping_for_addr(void* addr, size_t size)
{
    for (size_t i = 0; i < mappings_by_addr.size(); ++i) {
        mapping_and_addr& m = mappings_by_addr[i];
        if (m.addr == addr && m.size == size)
            return &m;
        REALM_ASSERT(m.addr != addr);
    }

    return 0;
}

SharedFileInfo* get_file_info_for_file(File::UniqueID id)
{
    LockGuard lock(mapping_mutex);

    std::vector<mappings_for_file>::iterator it;
    for (it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
#ifdef _WIN32
        if (File::is_same_file_static(it->handle, fd))
            break;
#else
        if (it->inode == id.inode && it->device == id.device)
            break;
#endif
    }
    if (it == mappings_by_file.end())
    	return nullptr;
    else
    	return it->info.get();
}

EncryptedFileMapping* add_mapping(void* addr, size_t size, FileDesc fd, size_t file_offset, File::AccessMode access,
                                  const char* encryption_key)
{
#ifndef _WIN32
    struct stat st;

    if (fstat(fd, &st)) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::system_error(err, std::system_category(), "fstat() failed");
    }
#endif

    size_t fs = to_size_t(File::get_size_static(fd));
    if (fs > 0 && fs < page_size())
        throw DecryptionFailed();

    LockGuard lock(mapping_mutex);

    std::vector<mappings_for_file>::iterator it;
    for (it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
#ifdef _WIN32
        if (File::is_same_file_static(it->handle, fd))
            break;
#else
        if (it->inode == st.st_ino && it->device == st.st_dev)
            break;
#endif
    }

    // Get the potential memory allocation out of the way so that mappings_by_addr.push_back can't throw
    mappings_by_addr.reserve(mappings_by_addr.size() + 1);

    if (it == mappings_by_file.end()) {
        mappings_by_file.reserve(mappings_by_file.size() + 1);

#ifdef _WIN32
        FileDesc fd2;
        if (!DuplicateHandle(GetCurrentProcess(), fd, GetCurrentProcess(), &fd2, 0, FALSE, DUPLICATE_SAME_ACCESS))
            throw std::system_error(GetLastError(), std::system_category(), "DuplicateHandle() failed");
        fd = fd2;
#else
        fd = dup(fd);

        if (fd == -1) {
            int err = errno; // Eliminate any risk of clobbering
            throw std::system_error(err, std::system_category(), "dup() failed");
        }
#endif
        mappings_for_file f;

#ifdef _WIN32
        f.handle = fd;
#else
        f.device = st.st_dev;
        f.inode = st.st_ino;
#endif

        try {
            f.info = new SharedFileInfo(reinterpret_cast<const uint8_t*>(encryption_key), fd);
        }
        catch (...) {
#ifdef _WIN32
            bool b = CloseHandle(fd);
            REALM_ASSERT_RELEASE(b);
#else
            ::close(fd);
#endif
            throw;
        }

        mappings_by_file.push_back(f); // can't throw due to reserve() above
        it = mappings_by_file.end() - 1;
    }

    try {
        mapping_and_addr m;
        m.addr = addr;
        m.size = size;
        EncryptedFileMapping* m_ptr = new EncryptedFileMapping(*it->info, file_offset, addr, size, access);
        m.mapping = m_ptr;
        mappings_by_addr.push_back(m); // can't throw due to reserve() above
        return m_ptr;
    }
    catch (...) {
        if (it->info->mappings.empty()) {
#ifdef _WIN32
            bool b = CloseHandle(it->info->fd);
            REALM_ASSERT_RELEASE(b);
#else
            ::close(it->info->fd);
#endif
            mappings_by_file.erase(it);
        }
        throw;
    }
}

void remove_mapping(void* addr, size_t size)
{
    size = round_up_to_page_size(size);
    LockGuard lock(mapping_mutex);
    mapping_and_addr* m = find_mapping_for_addr(addr, size);
    if (!m)
        return;

    mappings_by_addr.erase(mappings_by_addr.begin() + (m - &mappings_by_addr[0]));

    for (std::vector<mappings_for_file>::iterator it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
        if (it->info->mappings.empty()) {
#ifdef _WIN32
            if (!CloseHandle(it->info->fd))
                throw std::system_error(GetLastError(), std::system_category(), "CloseHandle() failed");
#else
            if (::close(it->info->fd) != 0) {
                int err = errno;                // Eliminate any risk of clobbering
                if (err == EBADF || err == EIO) // FIXME: how do we handle EINTR?
                    throw std::system_error(err, std::system_category(), "close() failed");
            }
#endif
            mappings_by_file.erase(it);
            break;
        }
    }
}

void* mmap_anon(size_t size)
{
#ifdef _WIN32
    HANDLE hMapFile;
    LPCTSTR pBuf;

    ULARGE_INTEGER s;
    s.QuadPart = size;
    
    hMapFile = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, s.HighPart, s.LowPart, nullptr);
    if (hMapFile == NULL) {
        throw std::system_error(GetLastError(), std::system_category(), "CreateFileMapping() failed");
    }

    pBuf = (LPTSTR)MapViewOfFile(hMapFile, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (pBuf == nullptr) {
        throw std::system_error(GetLastError(), std::system_category(), "MapViewOfFile() failed");
    }

    CloseHandle(hMapFile);
    return (void*)pBuf;
#else
    void* addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        if (is_mmap_memory_error(err)) {
            throw AddressSpaceExhausted(get_errno_msg("mmap() failed: ", err) + " size: " + util::to_string(size));
        }
        throw std::system_error(err, std::system_category(),
                                std::string("mmap() failed (size: ") +
                                    util::to_string(size) + ", offset is 0)");
    }
    return addr;
#endif
}

size_t round_up_to_page_size(size_t size) noexcept
{
    return (size + page_size() - 1) & ~(page_size() - 1);
}

void* mmap(FileDesc fd, size_t size, File::AccessMode access, size_t offset, const char* encryption_key,
           EncryptedFileMapping*& mapping)
{
    if (encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        mapping = add_mapping(addr, size, fd, offset, access, encryption_key);
        return addr;
    }
    else {
        mapping = nullptr;
        return mmap(fd, size, access, offset, nullptr);
    }
}

#endif // enable encryption


void* mmap(FileDesc fd, size_t size, File::AccessMode access, size_t offset, const char* encryption_key)
{
#if REALM_ENABLE_ENCRYPTION
    if (encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        add_mapping(addr, size, fd, offset, access, encryption_key);
        return addr;
    }
    else
#else
    REALM_ASSERT(!encryption_key);
#endif
    {

#ifndef _WIN32
        int prot = PROT_READ;
        switch (access) {
            case File::access_ReadWrite:
                prot |= PROT_WRITE;
                break;
            case File::access_ReadOnly:
                break;
        }

        void* addr = ::mmap(nullptr, size, prot, MAP_SHARED, fd, offset);
        if (addr != MAP_FAILED)
            return addr;

        int err = errno; // Eliminate any risk of clobbering
        if (is_mmap_memory_error(err)) {
            throw AddressSpaceExhausted(get_errno_msg("mmap() failed: ", err) + " size: " + util::to_string(size) +
                                        " offset: " + util::to_string(offset));
        }

        throw std::system_error(err, std::system_category(),
                                std::string("mmap() failed (size: ") +
                                    util::to_string(size) +
                                    ", offset: " + util::to_string(offset));

#else
        // FIXME: Is there anything that we must do on Windows to honor map_NoSync?

        DWORD protect = PAGE_READONLY;
        DWORD desired_access = FILE_MAP_READ;
        switch (access) {
            case File::access_ReadOnly:
                break;
            case File::access_ReadWrite:
                protect = PAGE_READWRITE;
                desired_access = FILE_MAP_WRITE;
                break;
        }
        LARGE_INTEGER large_int;
        if (int_cast_with_overflow_detect(offset + size, large_int.QuadPart))
            throw std::runtime_error("Map size is too large");
        HANDLE map_handle = CreateFileMappingFromApp(fd, 0, protect, offset + size, nullptr);
        if (!map_handle)
            throw AddressSpaceExhausted(get_errno_msg("CreateFileMapping() failed: ", GetLastError()) +
                                        " size: " + util::to_string(size) + " offset: " + util::to_string(offset));

        if (int_cast_with_overflow_detect(offset, large_int.QuadPart))
            throw util::overflow_error("Map offset is too large");

        SIZE_T _size = size;
        void* addr = MapViewOfFileFromApp(map_handle, desired_access, offset, _size);
        BOOL r = CloseHandle(map_handle);
        REALM_ASSERT_RELEASE(r);
        if (!addr)
            throw AddressSpaceExhausted(get_errno_msg("MapViewOfFileFromApp() failed: ", GetLastError()) +
                                        " size: " + util::to_string(_size) + " offset: " + util::to_string(offset));

        return addr;
#endif
    }
}

void munmap(void* addr, size_t size)
{
#if REALM_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif

#ifdef _WIN32
    if (!UnmapViewOfFile(addr))
        throw std::system_error(GetLastError(), std::system_category(), "UnmapViewOfFile() failed");

#else
    if (::munmap(addr, size) != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category(), "munmap() failed");
    }
#endif
}

void* mremap(FileDesc fd, size_t file_offset, void* old_addr, size_t old_size, File::AccessMode a, size_t new_size,
             const char* encryption_key)
{
#if REALM_ENABLE_ENCRYPTION
    if (encryption_key) {
        LockGuard lock(mapping_mutex);
        size_t rounded_old_size = round_up_to_page_size(old_size);
        if (mapping_and_addr* m = find_mapping_for_addr(old_addr, rounded_old_size)) {
            size_t rounded_new_size = round_up_to_page_size(new_size);
            if (rounded_old_size == rounded_new_size)
                return old_addr;

            void* new_addr = mmap_anon(rounded_new_size);
            m->mapping->set(new_addr, rounded_new_size, file_offset);
            m->addr = new_addr;
            m->size = rounded_new_size;
#ifdef _WIN32
            if (!UnmapViewOfFile(old_addr))
                throw std::system_error(GetLastError(), std::system_category(), "UnmapViewOfFile() failed");
#else
            if (::munmap(old_addr, rounded_old_size)) {
                int err = errno;
                throw std::system_error(err, std::system_category(), "munmap() failed");
            }
#endif
            return new_addr;
        }
        // If we are using encryption, we must have used mmap and the mapping
        // must have been added to the cache therefore find_mapping_for_addr()
        // will succeed. Otherwise we would continue to mmap it below without
        // the encryption key which is an error.
        REALM_UNREACHABLE();
    }
#else
    static_cast<void>(encryption_key);
#endif

#ifdef _GNU_SOURCE
    {
        void* new_addr = ::mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
        if (new_addr != MAP_FAILED)
            return new_addr;
        int err = errno; // Eliminate any risk of clobbering
        // Do not throw here if mremap is declared as "not supported" by the
        // platform Eg. When compiling with GNU libc on OSX, iOS.
        // In this case fall through to no-mremap case below.
        if (err != ENOTSUP && err != ENOSYS) {
            if (is_mmap_memory_error(err)) {
                throw AddressSpaceExhausted(get_errno_msg("mremap() failed: ", err) + " old size: " +
                                            util::to_string(old_size) + " new size: " + util::to_string(new_size));
            }
            throw std::system_error(err, std::system_category(),
                                    std::string("_gnu_src mmap() failed (") +
                                        "old_size: " + util::to_string(old_size) +
                                        ", new_size: " + util::to_string(new_size) + ")");
        }
    }
#endif

    void* new_addr = mmap(fd, new_size, a, file_offset, nullptr);

#ifdef _WIN32
    if (!UnmapViewOfFile(old_addr))
        throw std::system_error(GetLastError(), std::system_category(), "UnmapViewOfFile() failed");
#else
    if (::munmap(old_addr, old_size) != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category(), "munmap() failed");
    }
#endif

    return new_addr;
}

void msync(FileDesc fd, void* addr, size_t size)
{
#if REALM_ENABLE_ENCRYPTION
    {
        // first check the encrypted mappings
        LockGuard lock(mapping_mutex);
        if (mapping_and_addr* m = find_mapping_for_addr(addr, round_up_to_page_size(size))) {
            m->mapping->flush();
            m->mapping->sync();
            return;
        }
    }
#endif

    // not an encrypted mapping

    // FIXME: on iOS/OSX fsync may not be enough to ensure crash safety.
    // Consider adding fcntl(F_FULLFSYNC). This most likely also applies to msync.
    //
    // See description of fsync on iOS here:
    // https://developer.apple.com/library/ios/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html
    //
    // See also
    // https://developer.apple.com/library/ios/documentation/Cocoa/Conceptual/CoreData/Articles/cdPersistentStores.html
    // for a discussion of this related to core data.

#ifdef _WIN32
    // FlushViewOfFile() is asynchronous and won't flush metadata (file size, etc)
    if (!FlushViewOfFile(addr, size)) {
        throw std::system_error(GetLastError(), std::system_category(), "FlushViewOfFile() failed");
    }
    // Block until data and metadata is written physically to the media
    if (!FlushFileBuffers(fd)) {
        throw std::system_error(GetLastError(), std::system_category(), "FlushFileBuffers() failed");
    }
    return;
#else
    static_cast<void>(fd);
    if (::msync(addr, size, MS_SYNC) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::system_error(err, std::system_category(), "msync() failed");
    }
#endif
}
}
}
