#include <cerrno>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <limits>

#ifdef _WIN32
#  define NOMINMAX
#  include <windows.h>
#else
#  include <unistd.h>
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <sys/mman.h>
#  include <sys/file.h> // BSD / Linux flock()
#  ifdef _GNU_SOURCE
#    include <sys/mman.h> // mremap()
#  endif
#endif

#include <tightdb/assert.hpp>
#include <tightdb/exceptions.hpp>
#include <tightdb/safe_int_ops.hpp>
#include <tightdb/string_buffer.hpp>
#include <tightdb/file.hpp>

using namespace std;
using namespace tightdb;


namespace {


string get_errno_msg(const int errnum)
{
#if defined _BSD_SOURCE || defined _WIN32

    const char* const* errlist;
    int nerr;
#  ifdef _BSD_SOURCE
    errlist = sys_errlist; // BSD <stdio.h>
    nerr    = sys_nerr;
#  else
    errlist = _sys_errlist; // Windows <stdlib.h>
    nerr    = _sys_nerr;
#  endif
    if (TIGHTDB_LIKELY(0 <= errnum || errnum < nerr)) return errlist[errnum];

#else // POSIX <string.h>

    StringBuffer buffer;
    buffer.resize(1024);
    if (TIGHTDB_LIKELY(strerror_r(errnum, buffer.data(), buffer.size()) == 0)) return buffer.str();

#endif

    return "Unknown error";
}


#ifdef _WIN32 // Windows - GetLastError()

string get_last_error_msg(const DWORD errnum)
{
    StringBuffer buffer;
    buffer.resize(1024);
    const DWORD flags = FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
    const DWORD language_id = MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT);
    const DWORD size =
        FormatMessageA(flags, 0, errnum, language_id, buffer.data(),
                       static_cast<DWORD>(buffer.size()), 0);
    if (TIGHTDB_LIKELY(0 < size)) return string(buffer.data(), size);
    return "Unknown error";
}

#endif


} // anonymous namespace


namespace tightdb {


string create_temp_dir()
{
#ifdef _WIN32 // Windows version

    StringBuffer buffer1;
    buffer1.resize(MAX_PATH+1);
    if (GetTempPathA(MAX_PATH+1, buffer1.data()) == 0)
        throw runtime_error("CreateDirectory() failed");
    StringBuffer buffer2;
    buffer2.resize(MAX_PATH);
    for (;;) {
        if (GetTempFileNameA(buffer1.c_str(), "tdb", 0, buffer2.data()) == 0)
            throw runtime_error("GetTempFileName() failed");
        if (DeleteFileA(buffer2.c_str()) == 0)
            throw runtime_error("DeleteFile() failed");
        if (CreateDirectoryA(buffer2.c_str(), 0) != 0) break;
        if (GetLastError() != ERROR_ALREADY_EXISTS)
            throw runtime_error("CreateDirectory() failed");
    }
    return string(buffer2.c_str());

#else // POSIX.1-2008 version

    StringBuffer buffer;
    buffer.append_c_str(P_tmpdir "tightdb_XXXXXX");
    if (mkdtemp(buffer.c_str()) == 0) throw runtime_error("mkdtemp() failed");
    return buffer.str();

#endif
}


void File::open(const string& path, AccessMode a, CreateMode c, int flags)
{
#ifdef _WIN32 // Windows version

    DWORD desired_access = GENERIC_READ;
    switch (a) {
        case access_ReadOnly:
            break;
        case access_ReadWrite:
            if (flags & flag_Append) desired_access  = FILE_APPEND_DATA;
            else                     desired_access |= GENERIC_WRITE;
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
    const HANDLE handle = CreateFileA(path.c_str(), desired_access, share_mode, 0,
                                      creation_disposition, flags_and_attributes, 0);
    if (TIGHTDB_LIKELY(handle != INVALID_HANDLE_VALUE)) {
        m_handle    = handle;
        m_have_lock = false;
        return;
    }

    const DWORD errnum = GetLastError(); // Eliminate any risk of clobbering
    const string msg = get_last_error_msg(errnum);
    switch (errnum) {
        case ERROR_SHARING_VIOLATION:
        case ERROR_ACCESS_DENIED:       throw PermissionDenied(msg);
        case ERROR_FILE_NOT_FOUND:      throw NotFound(msg);
        case ERROR_FILE_EXISTS:         throw Exists(msg);
        case ERROR_TOO_MANY_OPEN_FILES: throw ResourceAllocError(msg);
        default:                        throw runtime_error(msg);
    }

#else // POSIX version

    int flags2 = 0;
    switch (a) {
        case access_ReadOnly:  flags2 = O_RDONLY; break;
        case access_ReadWrite: flags2 = O_RDWR;   break;
    }
    switch (c) {
        case create_Auto:  flags2 |= O_CREAT;          break;
        case create_Never:                             break;
        case create_Must:  flags2 |= O_CREAT | O_EXCL; break;
    }
    if (flags & flag_Trunc)  flags2 |= O_TRUNC;
    if (flags & flag_Append) flags2 |= O_APPEND;
    const int fd = ::open(path.c_str(), flags2, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (TIGHTDB_LIKELY(0 <= fd)) {
        m_fd = fd;
        return;
    }

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_errno_msg(errnum);
    switch (errnum) {
        case EACCES:
        case EROFS:
        case ETXTBSY:       throw PermissionDenied(msg);
        case ENOENT:        throw NotFound(msg);
        case EEXIST:        throw Exists(msg);
        case EISDIR:
        case ENAMETOOLONG:
        case ENOTDIR:
        case ENXIO:         throw OpenError(msg);
        case EMFILE:
        case ENFILE:
        case ENOSR:
        case ENOSPC:
        case ENOMEM:        throw ResourceAllocError(msg);
        default:            throw runtime_error(msg);
    }

#endif
}


void File::close() TIGHTDB_NOEXCEPT
{
#ifdef _WIN32 // Windows version

    if (!m_handle) return;
    if (m_have_lock) unlock();

    const BOOL r = CloseHandle(m_handle);
    TIGHTDB_ASSERT(r);
    static_cast<void>(r);
    m_handle = 0;

#else // POSIX version

    if (m_fd < 0) return;
    const int r = ::close(m_fd);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
    m_fd = -1;

#endif
}


void File::write(const char* data, size_t size)
{
#ifdef _WIN32 // Windows version

    const DWORD max_write = numeric_limits<DWORD>::max();
    while (int_less_than(max_write, size)) {
        write(data, max_write);
        size -= max_write;
        data += max_write;
    }

    DWORD n = 0;
    if (TIGHTDB_LIKELY(WriteFile(m_handle, data, static_cast<DWORD>(size), &n, 0))) {
        TIGHTDB_ASSERT(n == static_cast<DWORD>(size));
        return;
    }

    const DWORD errnum = GetLastError(); // Eliminate any risk of clobbering
    const string msg = get_last_error_msg(errnum);
    throw runtime_error(msg);

#else // POSIX version

    // POSIX requires that size is less than or equal to SSIZE_MAX
    while (int_less_than(SSIZE_MAX, size)) {
        write(data, SSIZE_MAX);
        size -= SSIZE_MAX;
        data += SSIZE_MAX;
    }

    const ssize_t r = ::write(m_fd, data, size);
    if (TIGHTDB_LIKELY(0 <= r)) {
        TIGHTDB_ASSERT(int_equal_to(r, size));
        return;
    }

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_errno_msg(errnum);
    switch (errnum) {
        case ENOSPC:
        case ENOBUFS: throw ResourceAllocError(msg);
        default:      throw runtime_error(msg);
    }

#endif
}


File::SizeType File::get_size() const
{
#ifdef _WIN32 // Windows version

    LARGE_INTEGER large_int;
    if (TIGHTDB_LIKELY(GetFileSizeEx(m_handle, &large_int))) {
        SizeType size;
        if (int_cast_with_overflow_detect(large_int.QuadPart, size))
            throw runtime_error("File size is too large");
        return size;
    }
    throw runtime_error("GetFileSizeEx() failed");

#else // POSIX version

    struct stat statbuf;
    if (TIGHTDB_LIKELY(::fstat(m_fd, &statbuf) == 0)) return statbuf.st_size;
    throw runtime_error("fstat() failed");

#endif
}


void File::resize(SizeType size)
{
#ifdef _WIN32 // Windows version

    seek(size);

    if (TIGHTDB_UNLIKELY(!SetEndOfFile(m_handle)))
        throw runtime_error("SetEndOfFile() failed");

#else // POSIX version

    // POSIX specifies that introduced bytes read as zero. This is not
    // required by File::resize().
    if (TIGHTDB_LIKELY(::ftruncate(m_fd, size) == 0)) return;
    throw runtime_error("ftruncate() failed");

#endif
}


void File::alloc(SizeType offset, size_t size)
{
#if _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version

    if (TIGHTDB_LIKELY(::posix_fallocate(m_fd, offset, size) == 0)) return;
    throw runtime_error("posix_fallocate() failed");

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_errno_msg(errnum);
    switch (errnum) {
        case ENOSPC: throw ResourceAllocError(msg);
        default:     throw runtime_error(msg);
    }

#else // Fallback

    // FIXME: OS X does not have any version of fallocate, but see
    // http://stackoverflow.com/questions/11497567/fallocate-command-equivalent-in-os-x

    if (int_add_with_overflow_detect(offset, size))
        throw runtime_error("File size overflow");
    if (get_size() < offset) resize(offset);

#endif
}


void File::seek(SizeType position)
{
#ifdef _WIN32 // Windows version

    LARGE_INTEGER large_int;
    if (int_cast_with_overflow_detect(position, large_int.QuadPart))
        throw runtime_error("File size is too large");

    if (TIGHTDB_UNLIKELY(!SetFilePointerEx(m_handle, large_int, 0, FILE_BEGIN)))
        throw runtime_error("SetFilePointerEx() failed");

#else // POSIX version

    if (TIGHTDB_LIKELY(0 <= ::lseek(m_fd, position, SEEK_SET))) return;
    throw runtime_error("lseek() failed");

#endif
}


// FIXME: The current implementation may not guarantee that data is
// actually written to disk. POSIX is rather vague on what fsync() has
// to do unless _POSIX_SYNCHRONIZED_IO is defined. See also
// http://www.humboldt.co.uk/2009/03/fsync-across-platforms.html.
void File::sync()
{
#ifdef _WIN32 // Windows version

    if (TIGHTDB_LIKELY(FlushFileBuffers(m_handle))) return;
    throw runtime_error("FlushFileBuffers() failed");

#else // POSIX version

    if (TIGHTDB_LIKELY(::fsync(m_fd) == 0)) return;
    throw runtime_error("fsync() failed");

#endif
}


bool File::lock(bool exclusive, bool non_blocking)
{
#ifdef _WIN32 // Windows version

    // Under Windows a file lock must be explicitely released before
    // the file is closed. It will eventually be released by the
    // system, but there is no guarantees on the timing.

    DWORD flags = 0;
    if (exclusive)    flags |= LOCKFILE_EXCLUSIVE_LOCK;
    if (non_blocking) flags |= LOCKFILE_FAIL_IMMEDIATELY;
    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof overlapped);
    if (TIGHTDB_LIKELY(LockFileEx(m_handle, flags, 0, 1, 0, &overlapped))) {
        m_have_lock = true;
        return true;
    }
    const DWORD errnum = GetLastError(); // Eliminate any risk of clobbering
    if (errnum == ERROR_LOCK_VIOLATION) return false;
    const string msg = get_last_error_msg(errnum);
    throw runtime_error(msg);

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
    // the robustness of the TightDB API relies in part by the
    // assumption that a single process (even a single thread) can
    // hold multiple overlapping independent shared locks on a single
    // file as long as they are placed via distinct file descriptors.
    //
    // Fortunately, on both Linux and Darwin, flock() does not suffer
    // from this 'spurious unlocking issue'.

    int operation = exclusive ? LOCK_EX : LOCK_SH;
    if (non_blocking) operation |=  LOCK_NB;
    if (TIGHTDB_LIKELY(flock(m_fd, operation) == 0)) return true;
    const int errnum = errno; // Eliminate any risk of clobbering
    if (errnum == EWOULDBLOCK) return false;
    const string msg = get_errno_msg(errnum);
    if (errnum == ENOLCK) throw ResourceAllocError(msg);
    throw runtime_error(msg);

#endif
}


void File::unlock() TIGHTDB_NOEXCEPT
{
#ifdef _WIN32 // Windows version

    if (!m_have_lock) return;
    const BOOL r = UnlockFile(m_handle, 0, 0, 1, 0);
    TIGHTDB_ASSERT(r);
    static_cast<void>(r);
    m_have_lock = false;

#else // BSD / Linux flock()

    // The Linux man page for flock() does not state explicitely that
    // unlocking is idempotent, however, we will assume it since there
    // is no mention of the error that would be reported if a
    // non-locked file were unlocked.
    const int r = flock(m_fd, LOCK_UN);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);

#endif
}


void* File::map(AccessMode a, size_t size) const
{
#ifdef _WIN32 // Windows version

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
    if (int_cast_with_overflow_detect(size, large_int.QuadPart))
        throw runtime_error("Map size is too large");
    const HANDLE map_handle =
        CreateFileMapping(m_handle, 0, PAGE_READONLY, large_int.HighPart, large_int.LowPart, 0);
    if (TIGHTDB_UNLIKELY(!map_handle))
        throw runtime_error("CreateFileMapping() failed");
    void* const addr = MapViewOfFile(map_handle, desired_access, 0, 0, 0);
    {
        const BOOL r = CloseHandle(map_handle);
        TIGHTDB_ASSERT(r);
        static_cast<void>(r);
    }
    if (TIGHTDB_UNLIKELY(!addr))
        throw runtime_error("MapViewOfFile() failed");

    return addr;

#else // POSIX version

    int prot = PROT_READ;
    switch (a) {
        case access_ReadWrite: prot |= PROT_WRITE; break;
        case access_ReadOnly:                      break;
    }
    void* const addr = ::mmap(0, size, prot, MAP_SHARED, m_fd, 0);
    if (TIGHTDB_LIKELY(addr != MAP_FAILED)) return addr;

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_errno_msg(errnum);
    switch (errnum) {
        case EAGAIN:
        case EMFILE:
        case ENOMEM: throw ResourceAllocError(msg);
        default:     throw runtime_error(msg);
    }

#endif
}


void* File::remap(void* old_addr, size_t old_size, AccessMode a, size_t new_size) const
{
#ifdef _GNU_SOURCE
    static_cast<void>(a);
    void* const new_addr = ::mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
    if (TIGHTDB_LIKELY(new_addr != MAP_FAILED)) return new_addr;
    unmap(old_addr, old_size);
    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_errno_msg(errnum);
    switch (errnum) {
        case EAGAIN:
        case ENOMEM: throw ResourceAllocError(msg);
        default:     throw runtime_error(msg);
    }
#else
    // FIXME: From the point of view of POSIX, it would probably be a
    // better idea to map the new region before unmapping the old one
    // (assuming that address overlaps are possible). This would also
    // allows us to guarantee that a failure will leave the old region
    // untoched. This is only possible, though, if Windows can work
    // well with the opposite order.
    unmap(old_addr, old_size);
    return map(a, new_size);
#endif
}


void File::unmap(void* addr, size_t size) TIGHTDB_NOEXCEPT
{
#ifdef _WIN32 // Windows version

    static_cast<void>(size);
    const BOOL r = UnmapViewOfFile(addr);
    TIGHTDB_ASSERT(r);
    static_cast<void>(r);

#else // POSIX version

    const int r = ::munmap(addr, size);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);

#endif
}


void File::sync_map(void* addr, size_t size)
{
#ifdef _WIN32 // Windows version

    if (TIGHTDB_LIKELY(FlushViewOfFile(addr, size))) return;
    throw runtime_error("FlushViewOfFile() failed");

#else // POSIX version

    if (TIGHTDB_LIKELY(::msync(addr, size, MS_SYNC) == 0)) return;
    const int errnum = errno; // Eliminate any risk of clobbering
    throw runtime_error(get_errno_msg(errnum));

#endif
}


FILE* File::open_stdio_file(const string& path, Mode m)
{
    const char* mode = 0;
    switch (m) {
        case mode_Read:   mode = "rb";  break;
        case mode_Update: mode = "rb+"; break;
        case mode_Write:  mode = "wb+"; break;
        case mode_Append: mode = "ab+"; break;
    }
    FILE* const file = fopen(path.c_str(), mode);
    if (TIGHTDB_LIKELY(file)) return file;

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_errno_msg(errnum);
    // Note: The following error codes are defined by POSIX, and
    // Windows follows POSIX in this respect, however, Windows
    // probably never produce most of these.
    switch (errnum) {
        case EACCES:
        case EROFS:
        case ETXTBSY:       throw PermissionDenied(msg);
        case ENOENT:        throw NotFound(msg);
        case EISDIR:
        case ENAMETOOLONG:
        case ENOTDIR:
        case ENXIO:         throw OpenError(msg);
        case EMFILE:
        case ENFILE:
        case ENOSR:
        case ENOSPC:
        case ENOMEM:        throw ResourceAllocError(msg);
        default:            throw runtime_error(msg);
    }
}


bool File::is_deleted() const
{
#ifdef _WIN32 // Windows version

    return false; // An open file cannot be deleted on Windows

#else // POSIX version

    struct stat statbuf;
    if (TIGHTDB_LIKELY(::fstat(m_fd, &statbuf) == 0)) return statbuf.st_nlink == 0;
    throw runtime_error("fstat() failed");

#endif
}


} // namespace tightdb
