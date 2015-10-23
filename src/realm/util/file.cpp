#include <climits>
#include <limits>
#include <algorithm>
#include <vector>

#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef _WIN32
#  define NOMINMAX
#  include <windows.h>
#  include <io.h>
#  include <direct.h>
#else
#  include <unistd.h>
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <sys/mman.h>
#  include <sys/file.h> // BSD / Linux flock()
#endif

#include <realm/util/errno.hpp>
#include <realm/util/file.hpp>
#include <realm/util/file_mapper.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/string_buffer.hpp>

using namespace realm;
using namespace realm::util;

namespace {
#ifdef _WIN32 // Windows - GetLastError()

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
        FormatMessageA(flags, 0, err, language_id, buffer.data()+offset,
                       static_cast<DWORD>(max_msg_size), 0);
    if (0 < size)
        return std::string(buffer.data(), offset+size);
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
    //DWORD size = sysinfo.dwPageSize;
    // On windows we use the allocation granularity instead
    DWORD size = sysinfo.dwAllocationGranularity;
#else
    long size = sysconf(_SC_PAGESIZE);
#endif
    REALM_ASSERT(size > 0 && size % 4096 == 0);
    return static_cast<size_t>(size);
}


} // anonymous namespace


namespace realm {
namespace util {


void make_dir(const std::string& path)
{
#ifdef _WIN32
    if (_mkdir(path.c_str()) == 0)
        return;
#else // POSIX
    if (::mkdir(path.c_str(), S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH) == 0)
        return;
#endif
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("make_dir() failed: ", err);
    switch (err) {
        case EACCES:
        case EROFS:
            throw File::PermissionDenied(msg, path);
        case EEXIST:
            throw File::Exists(msg, path);
        case ELOOP:
        case EMLINK:
        case ENAMETOOLONG:
        case ENOENT:
        case ENOTDIR:
            throw File::AccessError(msg, path);
        default:
            throw std::runtime_error(msg);
    }
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
        case ELOOP:
        case ENAMETOOLONG:
        case EINVAL:
        case ENOTDIR:
            throw File::AccessError(msg, path);
        default:
            throw std::runtime_error(msg);
    }
}


std::string make_temp_dir()
{
#ifdef _WIN32 // Windows version

    StringBuffer buffer1;
    buffer1.resize(MAX_PATH+1);
    if (GetTempPathA(MAX_PATH+1, buffer1.data()) == 0)
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

#else // POSIX.1-2008 version

    StringBuffer buffer;
    buffer.append_c_str(P_tmpdir "/realm_XXXXXX");
    if (mkdtemp(buffer.c_str()) == 0)
        throw std::runtime_error("mkdtemp() failed");
    return buffer.str();

#endif
}

size_t page_size()
{
    static size_t cached_page_size = get_page_size(); // thread safe in C++11
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
                desired_access  = FILE_APPEND_DATA;
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
    HANDLE handle = CreateFileA(path.c_str(), desired_access, share_mode, 0,
                                creation_disposition, flags_and_attributes, 0);
    if (handle != INVALID_HANDLE_VALUE) {
        m_handle    = handle;
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
    std::string msg = get_last_error_msg("CreateFile() failed: ", err);
    switch (err) {
        case ERROR_SHARING_VIOLATION:
        case ERROR_ACCESS_DENIED:
            throw PermissionDenied(msg, path);
        case ERROR_FILE_NOT_FOUND:
            throw NotFound(msg, path);
        case ERROR_FILE_EXISTS:
            throw Exists(msg, path);
        default:
            throw std::runtime_error(msg);
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
    int fd = ::open(path.c_str(), flags2, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
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
    std::string msg = get_errno_msg("open() failed: ", err);
    switch (err) {
        case EACCES:
        case EROFS:
        case ETXTBSY:
            throw PermissionDenied(msg, path);
        case ENOENT:
            throw NotFound(msg, path);
        case EEXIST:
            throw Exists(msg, path);
        case EISDIR:
        case ELOOP:
        case ENAMETOOLONG:
        case ENOTDIR:
        case ENXIO:
            throw AccessError(msg, path);
        default:
            throw std::runtime_error(msg);
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
    m_handle = 0;

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
        off_t pos = lseek(m_fd, 0, SEEK_CUR);
        Map<char> map(*this, access_ReadOnly, static_cast<size_t>(pos + size));
        memcpy(data, map.get_addr() + pos, size);
        lseek(m_fd, size, SEEK_CUR);
        return map.get_size() - pos;
    }

    char* const data_0 = data;
    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = ::read(m_fd, data, n);
        if (r == 0)
            break;
        if (r < 0)
            goto error;
        REALM_ASSERT_RELEASE(size_t(r) <= n);
        size -= size_t(r);
        data += size_t(r);
    }
    return data - data_0;

error:
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("read(): failed: ", err);
    throw std::runtime_error(msg);

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
        off_t pos = lseek(m_fd, 0, SEEK_CUR);
        Map<char> map(*this, access_ReadWrite, static_cast<size_t>(pos + size));
        memcpy(map.get_addr() + pos, data, size);
        lseek(m_fd, size, SEEK_CUR);
        return;
    }

    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = ::write(m_fd, data, n);
        if (r < 0)
            goto error;
        REALM_ASSERT_RELEASE(r != 0);
        REALM_ASSERT_RELEASE(size_t(r) <= n);
        size -= size_t(r);
        data += size_t(r);
    }
    return;

  error:
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = get_errno_msg("write(): failed: ", err);
    throw std::runtime_error(msg);

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

#if _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version

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

#if _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version

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
#if _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version
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
File::SizeType File::get_file_position()
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version
    LARGE_INTEGER liOfs = { 0 };
    LARGE_INTEGER liNew = { 0 };
    if(!SetFilePointerEx(m_handle, liOfs, &liNew, FILE_CURRENT))
        throw std::runtime_error("SetFilePointerEx() failed");
    return liNew.QuadPart;
#else
    // POSIX version not needed because it's only used by Windows version of resize().
    REALM_ASSERT(false);
    return 0;
#endif
}


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

#elif defined __APPLE__

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
    overlapped.Offset = 0;        // Just for clarity
    overlapped.OffsetHigh = 0;    // Just for clarity
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
        operation |=  LOCK_NB;
    if (flock(m_fd, operation) == 0)
        return true;
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
    BOOL r = UnlockFile(m_handle, 0, 0, 1, 0);
    REALM_ASSERT_RELEASE(r);
    m_have_lock = false;

#else // BSD / Linux flock()

    // The Linux man page for flock() does not state explicitely that
    // unlocking is idempotent, however, we will assume it since there
    // is no mention of the error that would be reported if a
    // non-locked file were unlocked.
    int r = flock(m_fd, LOCK_UN);
    REALM_ASSERT_RELEASE(r == 0);

#endif
}


void* File::map(AccessMode a, size_t size, int map_flags, size_t offset) const
{
#ifdef _WIN32 // Windows version

    // FIXME: Is there anything that we must do on Windows to honor map_NoSync?
    static_cast<void>(map_flags);

    DWORD protect        = PAGE_READONLY;
    DWORD desired_access = FILE_MAP_READ;
    switch (a) {
        case access_ReadOnly:
            break;
        case access_ReadWrite:
            protect        = PAGE_READWRITE;
            desired_access = FILE_MAP_WRITE;
            break;
    }
    LARGE_INTEGER large_int;
    if (int_cast_with_overflow_detect(offset+size, large_int.QuadPart))
        throw std::runtime_error("Map size is too large");
    HANDLE map_handle =
        CreateFileMapping(m_handle, 0, protect, large_int.HighPart, large_int.LowPart, 0);
    if (REALM_UNLIKELY(!map_handle))
        throw std::runtime_error("CreateFileMapping() failed");
    if (int_cast_with_overflow_detect(offset, large_int.QuadPart))
        throw std::runtime_error("Map offset is too large");
    SIZE_T _size = size;
    void* addr = MapViewOfFile(map_handle, desired_access, large_int.HighPart, large_int.LowPart, _size);
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


void* File::remap(void* old_addr, size_t old_size, AccessMode a, size_t new_size,
                  int map_flags, size_t file_offset) const
{
#ifdef _WIN32
    void* new_addr = map(a, new_size, map_flags);
    unmap(old_addr, old_size);
    return new_addr;
#else
    static_cast<void>(map_flags);
    return realm::util::mremap(m_fd, file_offset, old_addr, old_size, a, new_size);
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
        case ELOOP:
        case ENAMETOOLONG:
        case EISDIR: // Returned by Linux when path refers to a directory
        case ENOTDIR:
            throw AccessError(msg, path);
        default:
            throw std::runtime_error(msg);
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
        case ELOOP:
        case EMLINK:
        case ENAMETOOLONG:
        case EINVAL:
        case EISDIR:
        case ENOTDIR:
            throw AccessError(msg, old_path);
        default:
            throw std::runtime_error(msg);
    }
}


bool File::copy(std::string source, std::string destination)
{
    // Quick and dirty file copy, only used for unit tests. Todo, make more robust if used by Core.
    char buf[1024];
    size_t read;
    File::try_remove(destination);
    FILE* src = fopen(source.c_str(), "rb");
    if (!src)
        return false;

    FILE* dst = fopen(destination.c_str(), "wb");
    if (!dst) {
        fclose(src);
        return false;
    }

    while ((read = fread(buf, 1, 1024, src))) {
        fwrite(buf, 1, read, dst);
    }
    fclose(src);
    fclose(dst);
    return true;
}


bool File::is_same_file(const File& f) const
{
    REALM_ASSERT_RELEASE(is_attached());
    REALM_ASSERT_RELEASE(f.is_attached());

#ifdef _WIN32 // Windows version

    // FIXME: This version does not work on ReFS.
    BY_HANDLE_FILE_INFORMATION file_info;
    if (GetFileInformationByHandle(m_handle, &file_info)) {
        DWORD vol_serial_num = file_info.dwVolumeSerialNumber;
        DWORD file_ndx_high  = file_info.nFileIndexHigh;
        DWORD file_ndx_low   = file_info.nFileIndexLow;
        if (GetFileInformationByHandle(f.m_handle, &file_info)) {
            return vol_serial_num == file_info.dwVolumeSerialNumber &&
                file_ndx_high == file_info.nFileIndexHigh &&
                file_ndx_low  == file_info.nFileIndexLow;
        }
    }

/* FIXME: Here is how to do it on Windows Server 2012 and onwards. This new
   solution correctly handles file identification on ReFS.

    FILE_ID_INFO file_id_info;
    if (GetFileInformationByHandleEx(m_handle, FileIdInfo, &file_id_info, sizeof file_id_info)) {
        ULONGLONG vol_serial_num = file_id_info.VolumeSerialNumber;
        EXT_FILE_ID_128 file_id     = file_id_info.FileId;
        if (GetFileInformationByHandleEx(f.m_handle, FileIdInfo, &file_id_info,
                                         sizeof file_id_info)) {
            return vol_serial_num == file_id_info.VolumeSerialNumber &&
                file_id == file_id_info.FileId;
        }
    }
*/

    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    std::string msg = get_last_error_msg("GetFileInformationByHandleEx() failed: ", err);
    throw std::runtime_error(msg);

#else // POSIX version

    struct stat statbuf;
    if (::fstat(m_fd, &statbuf) == 0) {
        dev_t device_id = statbuf.st_dev;
        ino_t inode_num = statbuf.st_ino;
        if (::fstat(f.m_fd, &statbuf) == 0)
            return device_id == statbuf.st_dev && inode_num == statbuf.st_ino;
    }
    int err = errno; // Eliminate any risk of clobbering
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
        char *buffer = new char[64];
        memcpy(buffer, key, 64);
        m_encryption_key.reset(buffer);
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

DirScanner::DirScanner(const std::string& path)
{
    m_dirp = opendir(path.c_str());
    if (!m_dirp) {
        int err = errno; // Eliminate any risk of clobbering
        std::string msg = get_errno_msg("opendir() failed: ", err);
        switch (err) {
            case EACCES:
                throw File::PermissionDenied(msg, path);
            case ENOENT:
                throw File::NotFound(msg, path);
            case ELOOP:
            case ENAMETOOLONG:
            case ENOTDIR:
                throw File::AccessError(msg, path);
            default:
                throw std::runtime_error(msg);
        }
    }
}

DirScanner::~DirScanner() noexcept
{
    int r = closedir(m_dirp);
    REALM_ASSERT_RELEASE(r == 0);
}

bool DirScanner::next(std::string& name)
{
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

DirScanner::DirScanner(const std::string&)
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
