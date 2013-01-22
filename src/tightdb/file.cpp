#include <cerrno>
#include <cstring>
#include <cstdio>
#include <cstdlib>

#ifdef _WIN32
#else
#  include <unistd.h>
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <sys/mman.h>
#  include <sys/file.h> // Non-POSIX flock()
#endif

#include <tightdb/assert.hpp>
#include <tightdb/exceptions.hpp>
#include <tightdb/safe_int_ops.hpp>
#include <tightdb/string_buffer.hpp>
#include <tightdb/file.hpp>

using namespace std;
using namespace tightdb;


namespace {


string get_sys_err_msg(const int errnum)
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
    if (0 <= errnum || errnum < nerr) return errlist[errnum];

#else // POSIX <string.h>

    StringBuffer buffer;
    buffer.resize(1024);
    if (strerror_r(errnum, buffer.data(), buffer.size()) == 0) return buffer.str();

#endif

    return "Unknown error";
}

inline bool lock_file(int fd, bool exclusive, bool non_blocking)
{
#ifdef _WIN32 // Windows

#else // BSD flock

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
    if (TIGHTDB_LIKELY(flock(fd, operation) == 0)) return true;
    const int errnum = errno; // Eliminate any risk of clobbering
    if (errnum == EWOULDBLOCK) return false;
    const string msg = get_sys_err_msg(errnum);
    if (errnum == ENOLCK) throw ResourceAllocError(msg);
    throw runtime_error(msg);

#endif
}

inline void unlock_file(int fd) TIGHTDB_NOEXCEPT
{
    // The Linux man page for flock() does not state explicitely that
    // unlocking is idempotent, however, we will assume it since there
    // is no mention of the error that would be reported if a
    // non-locked file were unlocked.
    const int r = flock(fd, LOCK_UN);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
}


} // anonymous namespace


namespace tightdb {


string create_temp_dir()
{
#ifndef _MSC_VER // POSIX.1-2008 version

    StringBuffer buffer;
    buffer.append_c_str(P_tmpdir "tightdb_XXXXXX");
    if (mkdtemp(buffer.c_str()) == 0) throw runtime_error("mkdtemp() failed");
    return buffer.str();

#else // Windows version

    StringBuffer buffer1;
    buffer1.resize(MAX_PATH+1);
    if (GetTempPath(buffer1.size(), buffer1) == 0)
        throw runtime_error("CreateDirectory() failed");
    StringBuffer buffer2;
    buffer2.resize(MAX_PATH);
    for (;;) {
        if (GetTempFileName(buffer1.c_str(), "tdb", 0, buffer2.data()) == 0)
            throw runtime_error("GetTempFileName() failed");
        if (DeleteFile(buffer2.c_str()) == 0)
            throw runtime_error("DeleteFile() failed");
        if (CreateDirectory(buffer2.c_str(), 0) != 0) break;
        if (GetLastError() != ERROR_ALREADY_EXISTS)
            throw runtime_error("CreateDirectory() failed");
    }
    return string(buffer2.c_str());

#endif
}


#ifndef _MSC_VER // POSIX.1-2008 version

void File::open(const string& path, AccessMode a, CreateMode c)
{
    int flags = 0;
    switch (a) {
        case access_ReadWrite: flags = O_RDWR;   break;
        case access_ReadOnly:  flags = O_RDONLY; break;
    }
    switch (c) {
        case create_Auto:   flags |= O_CREAT;          break;
        case create_Never:                             break;
        case create_Always: flags |= O_CREAT | O_EXCL; break;
    }
    const int fd = ::open(path.c_str(), flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (TIGHTDB_LIKELY(0 <= fd)) {
        m_fd = fd;
        return;
    }

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_sys_err_msg(errnum);
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
}

void File::close() TIGHTDB_NOEXCEPT
{
    if (m_fd < 0) return;
    const int r = ::close(m_fd);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
    m_fd = -1;
}

void File::write(const char* data, size_t size)
{
    // POSIX requires than size is less than or equal to SSIZE_MAX
    while (int_less_than(SSIZE_MAX, size)) {
        write(data, SSIZE_MAX);
        size -= SSIZE_MAX;
        data += SSIZE_MAX;
    }

    const ssize_t r = ::write(m_fd, data, size);
    if (0 <= r) {
        TIGHTDB_ASSERT(int_equal_to(r, size));
        return;
    }

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_sys_err_msg(errnum);
    switch (errnum) {
        case ENOSPC:
        case ENOBUFS: throw ResourceAllocError(msg);
        default:      throw runtime_error(msg);
    }
}

off_t File::get_size() const
{
    struct stat statbuf;
    if (TIGHTDB_LIKELY(::fstat(m_fd, &statbuf) == 0)) return statbuf.st_size;
    throw runtime_error("fstat() failed");
}

void File::resize(off_t size)
{
    // POSIX specifies that introduced bytes read as zero. This is not
    // required by File::resize().
    if (TIGHTDB_LIKELY(::ftruncate(m_fd, size) == 0)) return;
    throw runtime_error("ftruncate() failed");
}

void File::alloc(off_t offset, size_t size)
{
    if (TIGHTDB_LIKELY(::posix_fallocate(m_fd, offset, size) == 0)) return;
    throw runtime_error("posix_fallocate() failed");

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_sys_err_msg(errnum);
    switch (errnum) {
        case ENOSPC: throw ResourceAllocError(msg);
        default:     throw runtime_error(msg);
    }
}

void File::seek(off_t position)
{
    if (TIGHTDB_LIKELY(0 <= ::lseek(m_fd, position, SEEK_SET))) return;
    throw runtime_error("lseek() failed");
}

void File::sync()
{
    if (TIGHTDB_LIKELY(::fsync(m_fd) == 0)) return;
    throw runtime_error("fsync() failed");
}

FILE* File::open_stdio_file(AccessMode a)
{
    int errnum;
    {
        // First get a new independent file destriptor
        const int new_fd = dup(m_fd);
        if (TIGHTDB_UNLIKELY(new_fd < 0)) {
            errnum = errno;
            goto error;
        }

        const char* mode = 0;
        switch (a) {
            case access_ReadWrite: mode = "r+"; break;
            case access_ReadOnly:  mode = "r";  break;
        }
        FILE* const file = fdopen(new_fd, mode);
        if (TIGHTDB_UNLIKELY(file == 0)) {
            errnum = errno;
            ::close(new_fd);
            goto error;
        }
        return file;
    }

  error:
    const string msg = get_sys_err_msg(errnum);
    switch (errnum) {
        case EMFILE:
        case ENOMEM: throw ResourceAllocError(msg);
        default:     throw runtime_error(msg);
    }
}

void File::lock_exclusive()
{
    lock_file(m_fd, true, false);
}

bool File::try_lock_exclusive()
{
    return lock_file(m_fd, true, true);
}

void File::lock_shared()
{
    lock_file(m_fd, false, false);
}

void File::unlock() TIGHTDB_NOEXCEPT
{
    unlock_file(m_fd);
}

void* File::map(AccessMode a, size_t size) const
{
    int prot = PROT_READ;
    switch (a) {
        case access_ReadWrite: prot |= PROT_WRITE; break;
        case access_ReadOnly:                      break;
    }
    void* const addr = ::mmap(0, size, prot, MAP_SHARED, m_fd, 0);
    if (TIGHTDB_LIKELY(addr != MAP_FAILED)) return addr;

    const int errnum = errno; // Eliminate any risk of clobbering
    const string msg = get_sys_err_msg(errnum);
    switch (errnum) {
        case EAGAIN:
        case EMFILE:
        case ENOMEM: throw ResourceAllocError(msg);
        default:     throw runtime_error(msg);
    }
}

void* File::remap(void* old_addr, size_t old_size, AccessMode a, size_t new_size) const
{
    unmap(old_addr, old_size);
    return map(a, new_size);
}

void File::unmap(void* addr, size_t size) TIGHTDB_NOEXCEPT
{
    const int r = ::munmap(addr, size);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
}

void File::sync_map(void* addr, size_t size)
{
    if (TIGHTDB_LIKELY(::msync(addr, size, MS_SYNC) == 0)) return;
    const int errnum = errno; // Eliminate any risk of clobbering
    throw runtime_error(get_sys_err_msg(errnum));
}

#else // Windows version

/* WinAPI:

OVERLAPPED dummy;
memset(&dummy, 0, sizeof dummy);
LockFileEx(file, (excl?LOCKFILE_EXCLUSIVE_LOCK:0), 0, 1, 0, &dummy); // Success if non-zero

UnlockFile(file, 0, 0, 1, 0); // Success if non-zero

Under Windows a file lock must be explicitely released before the file is close. It will eventually be released by the system, but there is no guarantees on the timing.
For this purpose we need a flag that tells us if the file has been locked.

Shared/unshared memory map in Windows????

*/

#endif


} // namespace tightdb
