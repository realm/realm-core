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

#include <climits>
#include <limits>
#include <algorithm>
#include <vector>

#include <cerrno>
#include <cstring>
#include <cstddef>
#include <cstdio>
#include <cstdlib>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#include <direct.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h> // BSD / Linux flock()
#endif

#include <realm/util/errno.hpp>
#include <realm/util/file_mapper.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/string_buffer.hpp>
#include <realm/util/features.h>
#include <realm/util/file.hpp>

using namespace realm;
using namespace realm::util;

namespace {
#ifdef _WIN32 // Windows - GetLastError()

#undef max

std::string get_last_error_msg(const char* prefix, DWORD err)
{
    StringBuffer buffer;
    buffer.append_c_str(prefix);
    size_t offset = buffer.size();
    size_t max_msg_size = 1024;
    buffer.resize(offset + max_msg_size);
    DWORD flags = FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
    DWORD language_id = MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT);
    DWORD size =
        FormatMessageA(flags, 0, err, language_id, buffer.data() + offset, static_cast<DWORD>(max_msg_size), 0);
    if (0 < size)
        return std::string(buffer.data(), offset + size);
    buffer.resize(offset);
    buffer.append_c_str("Unknown error");
    return buffer.str();
}

#endif

size_t get_page_size()
{
#ifdef _WIN32
    SYSTEM_INFO sysinfo;
    GetNativeSystemInfo(&sysinfo);
    // DWORD size = sysinfo.dwPageSize;
    // On windows we use the allocation granularity instead
    DWORD size = sysinfo.dwAllocationGranularity;
#else
    long size = sysconf(_SC_PAGESIZE);
#endif
    REALM_ASSERT(size > 0 && size % 4096 == 0);
    return static_cast<size_t>(size);
}

// This variable exists such that page_size() can return the page size without having to make any system calls.
// It could also have been a static local variable, but Valgrind/Helgrind gives a false error on that.
size_t cached_page_size = get_page_size();

} // anonymous namespace


namespace realm {
namespace util {


bool try_make_dir(const std::string& path)
{
#ifdef _WIN32
    if (_mkdir(path.c_str()) == 0)
        return true;
#else // POSIX
    if (::mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0)
        return true;
#endif
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("make_dir() failed: ", err);
    switch (err) {
        case EEXIST:
            return false;
        case EACCES:
        case EROFS:
            throw File::PermissionDenied(msg, path);
        default:
            throw File::AccessError(msg, path); // LCOV_EXCL_LINE
    }
}


void make_dir(const std::string& path)
{
    if (try_make_dir(path))
        return;
    std::string msg = get_errno_msg("make_dir() failed: ", EEXIST);
    throw File::Exists(msg, path);
}


void remove_dir(const std::string& path)
{
#ifdef _WIN32
    if (_rmdir(path.c_str()) == 0)
        return;
#else // POSIX
    if (::rmdir(path.c_str()) == 0)
        return;
#endif
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("remove_dir() failed: ", err);
    switch (err) {
        case EACCES:
        case EROFS:
        case EBUSY:
        case EPERM:
        case EEXIST:
        case ENOTEMPTY:
            throw File::PermissionDenied(msg, path);
        case ENOENT:
            throw File::NotFound(msg, path);
        default:
            throw File::AccessError(msg, path); // LCOV_EXCL_LINE
    }
}


std::string make_temp_dir()
{
#ifdef _WIN32 // Windows version

#if REALM_UWP
    throw std::runtime_error("File::make_temp_dir() not yet supported on Windows 10 UWP");
#else
    StringBuffer buffer1;
    buffer1.resize(MAX_PATH + 1);

    if (GetTempPathA(MAX_PATH + 1, buffer1.data()) == 0)
        throw std::runtime_error("CreateDirectory() failed");

    StringBuffer buffer2;
    buffer2.resize(MAX_PATH);
    for (;;) {
        if (GetTempFileNameA(buffer1.c_str(), "rlm", 0, buffer2.data()) == 0)
            throw std::runtime_error("GetTempFileName() failed");
        if (DeleteFileA(buffer2.c_str()) == 0)
            throw std::runtime_error("DeleteFile() failed");
        if (CreateDirectoryA(buffer2.c_str(), 0) != 0)
            break;
        if (GetLastError() != ERROR_ALREADY_EXISTS)
            throw std::runtime_error("CreateDirectory() failed");
    }
    return std::string(buffer2.c_str());
#endif

#else // POSIX.1-2008 version

#if REALM_ANDROID
    char buffer[] = "/data/local/tmp/realm_XXXXXX";
    if (mkdtemp(buffer) == 0) {
        throw std::runtime_error("mkdtemp() failed"); // LCOV_EXCL_LINE
    }
    return std::string(buffer);
#else
    std::string tmp = std::string(P_tmpdir) + std::string("/realm_XXXXXX") + std::string("\0", 1);
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(tmp.size()); // Throws
    memcpy(buffer.get(), tmp.c_str(), tmp.size());
    if (mkdtemp(buffer.get()) == 0) {
        throw std::runtime_error("mkdtemp() failed"); // LCOV_EXCL_LINE
    }
    return std::string(buffer.get()); 
#endif

#endif
}

size_t page_size()
{
    return cached_page_size;
}


} // namespace util
} // namespace realm


void File::open_internal(const std::string& path, AccessMode a, CreateMode c, int flags, bool* success)
{
    REALM_ASSERT_RELEASE(!is_attached());

#ifdef _WIN32 // Windows version

    DWORD desired_access = GENERIC_READ;
    switch (a) {
        case access_ReadOnly:
            break;
        case access_ReadWrite:
            if (flags & flag_Append) {
                desired_access = FILE_APPEND_DATA;
            }
            else {
                desired_access |= GENERIC_WRITE;
            }
            break;
    }
    // FIXME: Should probably be zero if we are called on behalf of a
    // Group instance that is not managed by a SharedGroup instance,
    // since in this case concurrenct access is prohibited anyway.
    DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD creation_disposition = 0;
    switch (c) {
        case create_Auto:
            creation_disposition = flags & flag_Trunc ? CREATE_ALWAYS : OPEN_ALWAYS;
            break;
        case create_Never:
            creation_disposition = flags & flag_Trunc ? TRUNCATE_EXISTING : OPEN_EXISTING;
            break;
        case create_Must:
            creation_disposition = CREATE_NEW;
            break;
    }
    DWORD flags_and_attributes = 0;
    std::wstring ws(path.begin(), path.end());
    HANDLE handle =
        CreateFile2(ws.c_str(), desired_access, share_mode, creation_disposition, nullptr);
    if (handle != INVALID_HANDLE_VALUE) {
        m_handle = handle;
        m_have_lock = false;
        if (success)
            *success = true;
        return;
    }

    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    if (success && err == ERROR_FILE_EXISTS && c == create_Must) {
        *success = false;
        return;
    }
    if (success && err == ERROR_FILE_NOT_FOUND && c == create_Never) {
        *success = false;
        return;
    }
    std::string error_prefix = "CreateFile(\"" + path + "\") failed: ";
    std::string msg = get_last_error_msg(error_prefix.c_str(), err);
    switch (err) {
        case ERROR_SHARING_VIOLATION:
        case ERROR_ACCESS_DENIED:
            throw PermissionDenied(msg, path);
        case ERROR_FILE_NOT_FOUND:
            throw NotFound(msg, path);
        case ERROR_FILE_EXISTS:
            throw Exists(msg, path);
        default:
            throw AccessError(msg, path);
    }

#else // POSIX version

    int flags2 = 0;
    switch (a) {
        case access_ReadOnly:
            flags2 = O_RDONLY;
            break;
        case access_ReadWrite:
            flags2 = O_RDWR;
            break;
    }
    switch (c) {
        case create_Auto:
            flags2 |= O_CREAT;
            break;
        case create_Never:
            break;
        case create_Must:
            flags2 |= O_CREAT | O_EXCL;
            break;
    }
    if (flags & flag_Trunc)
        flags2 |= O_TRUNC;
    if (flags & flag_Append)
        flags2 |= O_APPEND;
    int fd = ::open(path.c_str(), flags2, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (0 <= fd) {
        m_fd = fd;
        if (success)
            *success = true;
        return;
    }

    int err = errno; // Eliminate any risk of clobbering
    if (success && err == EEXIST && c == create_Must) {
        *success = false;
        return;
    }
    if (success && err == ENOENT && c == create_Never) {
        *success = false;
        return;
    }
    std::string error_prefix = "open(\"" + path + "\") failed: ";
    std::string msg = get_errno_msg(error_prefix.c_str(), err);
    switch (err) {
        case EACCES:
        case EROFS:
        case ETXTBSY:
            throw PermissionDenied(msg, path);
        case ENOENT:
            throw NotFound(msg, path);
        case EEXIST:
            throw Exists(msg, path);
        default:
            throw AccessError(msg, path); // LCOV_EXCL_LINE
    }

#endif
}


void File::close() noexcept
{
#ifdef _WIN32 // Windows version

    if (!m_handle)
        return;
    if (m_have_lock)
        unlock();

    BOOL r = CloseHandle(m_handle);
    REALM_ASSERT_RELEASE(r);
    m_handle = nullptr;

#else // POSIX version

    if (m_fd < 0)
        return;
    int r = ::close(m_fd);
    REALM_ASSERT_RELEASE(r == 0);
    m_fd = -1;

#endif
}


size_t File::read(char* data, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    char* const data_0 = data;
    while (0 < size) {
        DWORD n = std::numeric_limits<DWORD>::max();
        if (int_less_than(size, n))
            n = static_cast<DWORD>(size);
        DWORD r = 0;
        if (!ReadFile(m_handle, data, n, &r, 0))
            goto error;
        if (r == 0)
            break;
        REALM_ASSERT_RELEASE(r <= n);
        size -= size_t(r);
        data += size_t(r);
    }
    return data - data_0;

error:
    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    std::string msg = get_last_error_msg("ReadFile() failed: ", err);
    throw std::runtime_error(msg);

#else // POSIX version

    if (m_encryption_key) {
        off_t pos_original = lseek(m_fd, 0, SEEK_CUR);
        REALM_ASSERT(!int_cast_has_overflow<size_t>(pos_original));
        size_t pos = size_t(pos_original);
        Map<char> read_map(*this, access_ReadOnly, static_cast<size_t>(pos + size));
        realm::util::encryption_read_barrier(read_map, pos, size);
        memcpy(data, read_map.get_addr() + pos, size);
        lseek(m_fd, size, SEEK_CUR);
        return read_map.get_size() - pos;
    }

    char* const data_0 = data;
    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = ::read(m_fd, data, n);
        if (r == 0)
            break;
        if (r < 0)
            goto error; // LCOV_EXCL_LINE
        REALM_ASSERT_RELEASE(size_t(r) <= n);
        size -= size_t(r);
        data += size_t(r);
    }
    return data - data_0;

error:
    // LCOV_EXCL_START
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("read(): failed: ", err);
    throw std::runtime_error(msg);
// LCOV_EXCL_STOP
#endif
}


void File::write(const char* data, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    while (0 < size) {
        DWORD n = std::numeric_limits<DWORD>::max();
        if (int_less_than(size, n))
            n = static_cast<DWORD>(size);
        DWORD r = 0;
        if (!WriteFile(m_handle, data, n, &r, 0))
            goto error;
        REALM_ASSERT_RELEASE(r == n); // Partial writes are not possible.
        size -= size_t(r);
        data += size_t(r);
    }
    return;

error:
    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    std::string msg = get_last_error_msg("WriteFile() failed: ", err);
    throw std::runtime_error(msg);

#else // POSIX version

    if (m_encryption_key) {
        off_t pos_original = lseek(m_fd, 0, SEEK_CUR);
        REALM_ASSERT(!int_cast_has_overflow<size_t>(pos_original));
        size_t pos = size_t(pos_original);
        Map<char> write_map(*this, access_ReadWrite, static_cast<size_t>(pos + size));
        realm::util::encryption_read_barrier(write_map, pos, size);
        memcpy(write_map.get_addr() + pos, data, size);
        realm::util::encryption_write_barrier(write_map, pos, size);
        lseek(m_fd, size, SEEK_CUR);
        return;
    }

    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = ::write(m_fd, data, n);
        if (r < 0)
            goto error; // LCOV_EXCL_LINE
        REALM_ASSERT_RELEASE(r != 0);
        REALM_ASSERT_RELEASE(size_t(r) <= n);
        size -= size_t(r);
        data += size_t(r);
    }
    return;

error:
    // LCOV_EXCL_START
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("write(): failed: ", err);
    throw std::runtime_error(msg);
// LCOV_EXCL_STOP

#endif
}


File::SizeType File::get_size() const
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    LARGE_INTEGER large_int;
    if (GetFileSizeEx(m_handle, &large_int)) {
        SizeType size;
        if (int_cast_with_overflow_detect(large_int.QuadPart, size))
            throw std::runtime_error("File size overflow");
        return size;
    }
    throw std::runtime_error("GetFileSizeEx() failed");

#else // POSIX version

    struct stat statbuf;
    if (::fstat(m_fd, &statbuf) == 0) {
        SizeType size;
        if (int_cast_with_overflow_detect(statbuf.st_size, size))
            throw std::runtime_error("File size overflow");
        if (m_encryption_key)
            return encrypted_size_to_data_size(size);
        return size;
    }
    throw std::runtime_error("fstat() failed");

#endif
}


void File::resize(SizeType size)
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    // Save file position
    SizeType p = get_file_position();

    seek(size);
    if (!SetEndOfFile(m_handle))
        throw std::runtime_error("SetEndOfFile() failed");

    // Restore file position
    seek(p);

#else // POSIX version

    if (m_encryption_key)
        size = data_size_to_encrypted_size(size);

    off_t size2;
    if (int_cast_with_overflow_detect(size, size2))
        throw std::runtime_error("File size overflow");

    // POSIX specifies that introduced bytes read as zero. This is not
    // required by File::resize().
    if (::ftruncate(m_fd, size2) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("ftruncate() failed: ", err));
    }

#endif
}


void File::prealloc(SizeType offset, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

#if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version

    prealloc_if_supported(offset, size);

#else // Non-atomic fallback

    if (int_add_with_overflow_detect(offset, size))
        throw std::runtime_error("File size overflow");
    if (get_size() < offset)
        resize(offset);

#endif
}


void File::prealloc_if_supported(SizeType offset, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

#if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version

    REALM_ASSERT_RELEASE(is_prealloc_supported());

    if (m_encryption_key)
        size = data_size_to_encrypted_size(size);

    off_t size2;
    if (int_cast_with_overflow_detect(size, size2))
        throw std::runtime_error("File size overflow");

    if (::posix_fallocate(m_fd, offset, size2) == 0)
        return;
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("posix_fallocate() failed: ", err);
    throw std::runtime_error(msg);

// FIXME: OS X does not have any version of fallocate, but see
// http://stackoverflow.com/questions/11497567/fallocate-command-equivalent-in-os-x

// FIXME: On Windows one could use a call to CreateFileMapping()
// since it will grow the file if necessary, but never shrink it,
// just like posix_fallocate(). The advantage would be that it
// then becomes an atomic operation (probably).

#else

    static_cast<void>(offset);
    static_cast<void>(size);

    REALM_ASSERT_RELEASE(!is_prealloc_supported());

#endif
}


bool File::is_prealloc_supported()
{
#if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version
    return true;
#else
    return false;
#endif
}


void File::seek(SizeType position)
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    LARGE_INTEGER large_int;
    if (int_cast_with_overflow_detect(position, large_int.QuadPart))
        throw std::runtime_error("File position overflow");

    if (!SetFilePointerEx(m_handle, large_int, 0, FILE_BEGIN))
        throw std::runtime_error("SetFilePointerEx() failed");

#else // POSIX version

    off_t position2;
    if (int_cast_with_overflow_detect(position, position2))
        throw std::runtime_error("File position overflow");

    if (0 <= ::lseek(m_fd, position2, SEEK_SET))
        return;
    throw std::runtime_error("lseek() failed");

#endif
}


// We might be able to use lseek() with offset=0 as cross platform method, because we fortunatly
// do not require to operate on files larger than 4 GB on 32-bit platforms
#ifdef _WIN32 // Windows version
File::SizeType File::get_file_position()
{
    REALM_ASSERT_RELEASE(is_attached());

    LARGE_INTEGER liOfs = {0};
    LARGE_INTEGER liNew = {0};
    if (!SetFilePointerEx(m_handle, liOfs, &liNew, FILE_CURRENT))
        throw std::runtime_error("SetFilePointerEx() failed");
    return liNew.QuadPart;
}
#endif


// FIXME: The current implementation may not guarantee that data is
// actually written to disk. POSIX is rather vague on what fsync() has
// to do unless _POSIX_SYNCHRONIZED_IO is defined. See also
// http://www.humboldt.co.uk/2009/03/fsync-across-platforms.html.
void File::sync()
{
    REALM_ASSERT_RELEASE(is_attached());

#if defined _WIN32 // Windows version

    if (FlushFileBuffers(m_handle))
        return;
    throw std::runtime_error("FlushFileBuffers() failed");

#elif REALM_PLATFORM_APPLE

    if (::fcntl(m_fd, F_FULLFSYNC) == 0)
        return;
    int err = errno; // Eliminate any risk of clobbering
    throw std::runtime_error(get_errno_msg("fcntl() with F_FULLFSYNC failed: ", err));

#else // POSIX version

    if (::fsync(m_fd) == 0)
        return;
    throw std::runtime_error("fsync() failed");

#endif
}


bool File::lock(bool exclusive, bool non_blocking)
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    REALM_ASSERT_RELEASE(!m_have_lock);

    // Under Windows a file lock must be explicitely released before
    // the file is closed. It will eventually be released by the
    // system, but there is no guarantees on the timing.

    DWORD flags = 0;
    if (exclusive)
        flags |= LOCKFILE_EXCLUSIVE_LOCK;
    if (non_blocking)
        flags |= LOCKFILE_FAIL_IMMEDIATELY;
    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof overlapped);
    overlapped.Offset = 0;     // Just for clarity
    overlapped.OffsetHigh = 0; // Just for clarity
    if (LockFileEx(m_handle, flags, 0, 1, 0, &overlapped)) {
        m_have_lock = true;
        return true;
    }
    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    if (err == ERROR_LOCK_VIOLATION)
        return false;
    std::string msg = get_last_error_msg("LockFileEx() failed: ", err);
    throw std::runtime_error(msg);

#else // BSD / Linux flock()

    // NOTE: It would probably have been more portable to use fcntl()
    // based POSIX locks, however these locks are not recursive within
    // a single process, and since a second attempt to acquire such a
    // lock will always appear to succeed, one will easily suffer the
    // 'spurious unlocking issue'. It remains to be determined whether
    // this also applies across distinct threads inside a single
    // process.
    //
    // To make matters worse, flock() may be a simple wrapper around
    // fcntl() based locks on some systems. This is bad news, because
    // the robustness of the Realm API relies in part by the
    // assumption that a single process (even a single thread) can
    // hold multiple overlapping independent shared locks on a single
    // file as long as they are placed via distinct file descriptors.
    //
    // Fortunately, on both Linux and Darwin, flock() does not suffer
    // from this 'spurious unlocking issue'.

    int operation = exclusive ? LOCK_EX : LOCK_SH;
    if (non_blocking)
        operation |= LOCK_NB;
    do {
        if (flock(m_fd, operation) == 0)
            return true;
    } while (errno == EINTR);
    int err = errno; // Eliminate any risk of clobbering
    if (err == EWOULDBLOCK)
        return false;
    std::string msg = get_errno_msg("flock() failed: ", err);
    throw std::runtime_error(msg);

#endif
}


void File::unlock() noexcept
{
#ifdef _WIN32 // Windows version

    if (!m_have_lock)
        return;

    OVERLAPPED overlapped;
    overlapped.hEvent = 0;
    overlapped.OffsetHigh = 0;
    overlapped.Offset = 0;
    overlapped.Pointer = 0;
    BOOL r = UnlockFileEx(m_handle, 0, 1, 0, &overlapped);

    REALM_ASSERT_RELEASE(r);
    m_have_lock = false;

#else // BSD / Linux flock()

    // The Linux man page for flock() does not state explicitely that
    // unlocking is idempotent, however, we will assume it since there
    // is no mention of the error that would be reported if a
    // non-locked file were unlocked.
    int r;
    do {
        r = flock(m_fd, LOCK_UN);
    } while (r != 0 && errno == EINTR);
    REALM_ASSERT_RELEASE(r == 0);

#endif
}


void* File::map(AccessMode a, size_t size, int map_flags, size_t offset) const
{
#ifdef _WIN32 // Windows version

    // FIXME: Is there anything that we must do on Windows to honor map_NoSync?
    static_cast<void>(map_flags);

    DWORD protect = PAGE_READONLY;
    DWORD desired_access = FILE_MAP_READ;
    switch (a) {
        case access_ReadOnly:
            break;
        case access_ReadWrite:
            protect = PAGE_READWRITE;
            desired_access = FILE_MAP_WRITE;
            break;
    }
    LARGE_INTEGER large_int;
    if (int_cast_with_overflow_detect(offset + size, large_int.QuadPart))
        throw std::runtime_error("Map size is too large");
    HANDLE map_handle = CreateFileMappingFromApp(m_handle, 0, protect, offset + size, nullptr);
    if (REALM_UNLIKELY(!map_handle))
        throw std::runtime_error("CreateFileMapping() failed");
    if (int_cast_with_overflow_detect(offset, large_int.QuadPart))
        throw std::runtime_error("Map offset is too large");
    SIZE_T _size = size;
    void* addr = MapViewOfFileFromApp(map_handle, desired_access, offset, _size);
    {
        BOOL r = CloseHandle(map_handle);
        REALM_ASSERT_RELEASE(r);
    }
    if (REALM_LIKELY(addr))
        return addr;
    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    std::string msg = get_last_error_msg("MapViewOfFile() failed: ", err);
    throw std::runtime_error(msg);

#else // POSIX version

    // FIXME: On FreeeBSB and other systems that support it, we should
    // honor map_NoSync by specifying MAP_NOSYNC, but how do we
    // reliably detect these systems?
    static_cast<void>(map_flags);

    return realm::util::mmap(m_fd, size, a, offset, m_encryption_key.get());

#endif
}

#if REALM_ENABLE_ENCRYPTION
#ifdef _WIN32
#error "Encryption is not supported on Windows"
#else
void* File::map(AccessMode a, size_t size, EncryptedFileMapping*& mapping, int map_flags, size_t offset) const
{
    static_cast<void>(map_flags);

    return realm::util::mmap(m_fd, size, a, offset, m_encryption_key.get(), mapping);
}
#endif
#endif

void File::unmap(void* addr, size_t size) noexcept
{
#ifdef _WIN32 // Windows version

    static_cast<void>(size);
    BOOL r = UnmapViewOfFile(addr);
    REALM_ASSERT_RELEASE(r);

#else // POSIX version

    realm::util::munmap(addr, size);

#endif
}


void* File::remap(void* old_addr, size_t old_size, AccessMode a, size_t new_size, int map_flags,
                  size_t file_offset) const
{
#ifdef _WIN32
    void* new_addr = map(a, new_size, map_flags);
    unmap(old_addr, old_size);
    return new_addr;
#else
    static_cast<void>(map_flags);
    return realm::util::mremap(m_fd, file_offset, old_addr, old_size, a, new_size, m_encryption_key.get());
#endif
}


void File::sync_map(void* addr, size_t size)
{
#ifdef _WIN32 // Windows version

    if (FlushViewOfFile(addr, size))
        return;
    throw std::runtime_error("FlushViewOfFile() failed");

#else // POSIX version

    realm::util::msync(addr, size);

#endif
}


bool File::exists(const std::string& path)
{
#ifdef _WIN32
    if (_access(path.c_str(), 0) == 0)
        return true;
#else // POSIX
    if (::access(path.c_str(), F_OK) == 0)
        return true;
#endif
    int err = errno; // Eliminate any risk of clobbering
    switch (err) {
        case EACCES:
        case ENOENT:
        case ENOTDIR:
            return false;
    }
    std::string msg = get_errno_msg("access() failed: ", err);
    throw std::runtime_error(msg);
}


bool File::is_dir(const std::string& path)
{
#ifndef _WIN32
    struct stat statbuf;
    if (::stat(path.c_str(), &statbuf) == 0)
        return S_ISDIR(statbuf.st_mode);
    int err = errno; // Eliminate any risk of clobbering
    switch (err) {
        case EACCES:
        case ENOENT:
        case ENOTDIR:
            return false;
    }
    std::string msg = get_errno_msg("stat() failed: ", err);
    throw std::runtime_error(msg);
#else
    static_cast<void>(path);
    throw std::runtime_error("Not yet supported");
#endif
}


void File::remove(const std::string& path)
{
    if (try_remove(path))
        return;
    int err = ENOENT;
    std::string msg = get_errno_msg("remove() failed: ", err);
    throw NotFound(msg, path);
}


bool File::try_remove(const std::string& path)
{
#ifdef _WIN32
    if (_unlink(path.c_str()) == 0)
        return true;
#else // POSIX
    if (::unlink(path.c_str()) == 0)
        return true;
#endif
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("unlink() failed: ", err);
    switch (err) {
        case EACCES:
        case EROFS:
        case ETXTBSY:
        case EBUSY:
        case EPERM:
            throw PermissionDenied(msg, path);
        case ENOENT:
            return false;
        default:
            throw AccessError(msg, path);
    }
}


void File::move(const std::string& old_path, const std::string& new_path)
{
    int r = rename(old_path.c_str(), new_path.c_str());
    if (r == 0)
        return;
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("rename() failed: ", err);
    switch (err) {
        case EACCES:
        case EROFS:
        case ETXTBSY:
        case EBUSY:
        case EPERM:
        case EEXIST:
        case ENOTEMPTY:
            throw PermissionDenied(msg, old_path);
        case ENOENT:
            throw File::NotFound(msg, old_path);
        default:
            throw AccessError(msg, old_path);
    }
}


void File::copy(const std::string& origin_path, const std::string& target_path)
{
    File origin_file{origin_path, mode_Read};  // Throws
    File target_file{target_path, mode_Write}; // Throws
    size_t buffer_size = 4096;
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(buffer_size); // Throws
    for (;;) {
        size_t n = origin_file.read(buffer.get(), buffer_size); // Throws
        target_file.write(buffer.get(), n);                     // Throws
        if (n < buffer_size)
            break;
    }
}


bool File::compare(const std::string& path_1, const std::string& path_2)
{
    File file_1{path_1}; // Throws
    File file_2{path_2}; // Throws
    size_t buffer_size = 4096;
    std::unique_ptr<char[]> buffer_1 = std::make_unique<char[]>(buffer_size); // Throws
    std::unique_ptr<char[]> buffer_2 = std::make_unique<char[]>(buffer_size); // Throws
    for (;;) {
        size_t n_1 = file_1.read(buffer_1.get(), buffer_size); // Throws
        size_t n_2 = file_2.read(buffer_2.get(), buffer_size); // Throws
        if (n_1 != n_2)
            return false;
        if (!std::equal(buffer_1.get(), buffer_1.get() + n_1, buffer_2.get()))
            return false;
        if (n_1 < buffer_size)
            break;
    }
    return true;
}


bool File::is_same_file(const File& f) const
{
    REALM_ASSERT_RELEASE(is_attached());
    REALM_ASSERT_RELEASE(f.is_attached());
    return f.get_unique_id() == get_unique_id();
}

File::UniqueID File::get_unique_id() const
{
    REALM_ASSERT_RELEASE(is_attached());
#if REALM_UWP
    // UWP does not support GetFileInformationByHandleEx(FileIdInfo) or
    // GetFileInformationByHandle() and does not expose the same information
    // in any other way.
    throw std::runtime_error("Not supported");
#elif defined(_WIN32) // Windows version
    // First try the Windows Server 2012 version
    FILE_ID_INFO file_id_info;
    if (GetFileInformationByHandleEx(m_handle, FileIdInfo, &file_id_info, sizeof file_id_info)) {
        UniqueID id{file_id_info.VolumeSerialNumber};
        memcpy(&id.inode[0], file_id_info.FileId.Identifier, sizeof(file_id_info.FileId.Identifier));
        return id;
    }

    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    if (err != ERROR_INVALID_PARAMETER) {
        std::string msg = get_last_error_msg("GetFileInformationByHandleEx() failed: ", err);
        throw std::runtime_error(msg);
    }

    // Fall back to the older function on other versions of Windows
    BY_HANDLE_FILE_INFORMATION file_info;
    if (!GetFileInformationByHandle(m_handle, &file_info)) {
        DWORD err = GetLastError(); // Eliminate any risk of clobbering
        std::string msg = get_last_error_msg("GetFileInformationByHandle() failed: ", err);
        throw std::runtime_error(msg);
    }

    UniqueID id{file_info.dwVolumeSerialNumber};
    memcpy(&id.inode[0],
           &file_info.nFileIndexHigh, sizeof(file_info.nFileIndexHigh));
    memcpy(&id.inode[sizeof(file_info.nFileIndexHigh)],
           &file_info.nFileIndexLow, sizeof(file_info.nFileIndexLow));
    // Leave the last 8 bytes of the inode zeroed
    return id;
#else // POSIX version
    struct stat statbuf;
    if (::fstat(m_fd, &statbuf) == 0) {
        return {static_cast<uint_fast64_t>(statbuf.st_dev), static_cast<uint_fast64_t>(statbuf.st_ino)};
    }
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("fstat() failed: ", err);
    throw std::runtime_error(msg);
#endif
}

bool File::get_unique_id(const std::string& path, File::UniqueID& uid)
{
#ifdef _WIN32 // Windows version
    File file;
    bool did_open = false;
    file.open_internal(path, access_ReadOnly, create_Never, 0, &did_open);
    if (did_open) {
        uid = file.get_unique_id();
        return true;
    }
    return false;
#else // POSIX version
    struct stat statbuf;
    if (::stat(path.c_str(), &statbuf) == 0) {
        uid.device = statbuf.st_dev;
        uid.inode = statbuf.st_ino;
        return true;
    }
    int err = errno; // Eliminate any risk of clobbering
    // File doesn't exist
    if (err == ENOENT)
        return false;

    std::string msg = get_errno_msg("fstat() failed: ", err);
    throw std::runtime_error(msg);
#endif
}

bool File::is_removed() const
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    return false; // An open file cannot be deleted on Windows

#else // POSIX version

    struct stat statbuf;
    if (::fstat(m_fd, &statbuf) == 0)
        return statbuf.st_nlink == 0;
    throw std::runtime_error("fstat() failed");

#endif
}


std::string File::resolve(const std::string& path, const std::string& base_dir)
{
#ifndef _WIN32
    char dir_sep = '/';
    std::string path_2 = path;
    std::string base_dir_2 = base_dir;
    bool is_absolute = (!path_2.empty() && path_2.front() == dir_sep);
    if (is_absolute)
        return path_2;
    if (path_2.empty())
        path_2 = ".";
    if (!base_dir_2.empty() && base_dir_2.back() != dir_sep)
        base_dir_2.push_back(dir_sep);
    /*
    // Abbreviate
    for (;;) {
        if (base_dir_2.empty()) {
            if (path_2.empty())
                return "./";
            return path_2;
        }
        if (path_2 == ".") {
            remove_trailing_dir_seps(base_dir_2);
            return base_dir_2;
        }
        if (has_prefix(path_2, "./")) {
            remove_trailing_dir_seps(base_dir_2);
            // drop dot
            // transfer slashes
        }

        if (path_2.size() < 2 || path_2[1] != '.')
            break;
        if (path_2.size())
    }
    */
    return base_dir_2 + path_2;
#else
    static_cast<void>(path);
    static_cast<void>(base_dir);
    throw std::runtime_error("Not yet supported");
#endif
}


void File::set_encryption_key(const char* key)
{
#if REALM_ENABLE_ENCRYPTION
    if (key) {
        char* buffer = new char[64];
        memcpy(buffer, key, 64);
        m_encryption_key.reset(static_cast<const char*>(buffer));
    }
    else {
        m_encryption_key.reset();
    }
#else
    if (key) {
        throw std::runtime_error("Encryption not enabled");
    }
#endif
}


#ifndef _WIN32

DirScanner::DirScanner(const std::string& path, bool allow_missing)
{
    m_dirp = opendir(path.c_str());
    if (!m_dirp) {
        int err = errno; // Eliminate any risk of clobbering
        std::string msg = get_errno_msg("opendir() failed: ", err);
        switch (err) {
            case EACCES:
                throw File::PermissionDenied(msg, path);
            case ENOENT:
                if (allow_missing)
                    return;
                throw File::NotFound(msg, path);
            default:
                throw File::AccessError(msg, path);
        }
    }
}

DirScanner::~DirScanner() noexcept
{
    if (m_dirp) {
        int r = closedir(m_dirp);
        REALM_ASSERT_RELEASE(r == 0);
    }
}

bool DirScanner::next(std::string& name)
{
    if (!m_dirp)
        return false;

    const size_t min_dirent_size = offsetof(struct dirent, d_name) + NAME_MAX + 1;
    union {
        struct dirent m_dirent;
        char m_strut[min_dirent_size];
    } u;
    struct dirent* dirent;
    for (;;) {
        int err = readdir_r(m_dirp, &u.m_dirent, &dirent);
        if (err != 0) {
            std::string msg = get_errno_msg("readdir_r() failed: ", err);
            throw std::runtime_error(msg);
        }
        if (!dirent)
            return false; // End of stream
        const char* name_1 = dirent->d_name;
        std::string name_2 = name_1;
        if (name_2 != "." && name_2 != "..") {
            name = name_2;
            return true;
        }
    }
}

#else

DirScanner::DirScanner(const std::string&, bool)
{
    throw std::runtime_error("Not yet supported");
}

DirScanner::~DirScanner() noexcept
{
}

bool DirScanner::next(std::string&)
{
    return false;
}

#endif
