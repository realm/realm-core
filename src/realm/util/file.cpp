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

#include <realm/exceptions.hpp>
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

bool for_each_helper(const std::string& path, const std::string& dir, File::ForEachHandler& handler)
{
    DirScanner ds{path}; // Throws
    std::string name;
    while (ds.next(name)) { // Throws
        std::string subpath = File::resolve(name, path); // Throws
        bool go_on;
        if (File::is_dir(subpath)) { // Throws
            std::string subdir = File::resolve(name, dir); // Throws
            go_on = for_each_helper(subpath, subdir, handler); // Throws
        }
        else {
            go_on = handler(name, dir); // Throws
        }
        if (!go_on)
            return false;
    }
    return true;
}

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
    if (try_make_dir(path)) // Throws
        return;
    std::string msg = get_errno_msg("make_dir() failed: ", EEXIST);
    throw File::Exists(msg, path);
}


void remove_dir(const std::string& path)
{
    if (try_remove_dir(path)) // Throws
        return;
    int err = ENOENT;
    std::string msg = get_errno_msg("remove() failed: ", err);
    throw File::NotFound(msg, path);
}


bool try_remove_dir(const std::string& path)
{
#ifdef _WIN32
    if (_rmdir(path.c_str()) == 0)
        return true;
#else // POSIX
    if (::rmdir(path.c_str()) == 0)
        return true;
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
            return false;
        default:
            throw File::AccessError(msg, path); // LCOV_EXCL_LINE
    }
}


void remove_dir_recursive(const std::string& path)
{
    if (try_remove_dir_recursive(path)) // Throws
        return;

    int err = ENOENT;
    std::string msg = get_errno_msg("remove_dir_recursive() failed: ", err);
    throw File::NotFound(msg, path);
}


bool try_remove_dir_recursive(const std::string& path)
{
    {
        bool allow_missing = true;
        DirScanner ds{path, allow_missing}; // Throws
        std::string name;
        while (ds.next(name)) { // Throws
            std::string subpath = File::resolve(name, path); // Throws
            if (File::is_dir(subpath)) { // Throws
                try_remove_dir_recursive(subpath); // Throws
            }
            else {
                File::remove(subpath); // Throws
            }
        }
    }
    return try_remove_dir(path); // Throws
}


std::string make_temp_dir()
{
#ifdef _WIN32 // Windows version
    std::filesystem::path temp = std::filesystem::temp_directory_path();

    wchar_t buffer[MAX_PATH];
    std::filesystem::path path;
    for (;;) {
        if (GetTempFileNameW(temp.c_str(), L"rlm", 0, buffer) == 0)
            throw std::system_error(GetLastError(), std::system_category(), "GetTempFileName() failed");
        path = buffer;
        std::filesystem::remove(path);
        try {
            std::filesystem::create_directory(path);
            break;
        }
        catch (const std::filesystem::filesystem_error& ex) {
            if (ex.code() != std::errc::file_exists)
                throw;
        }
    }
    return path.u8string();

#else // POSIX.1-2008 version

#if REALM_ANDROID
    char buffer[] = "/data/local/tmp/realm_XXXXXX";
    if (mkdtemp(buffer) == 0) {
        throw std::system_error(errno, std::system_category(), "mkdtemp() failed"); // LCOV_EXCL_LINE
    }
    return std::string(buffer);
#else
    std::string tmp = std::string(P_tmpdir) + std::string("/realm_XXXXXX") + std::string("\0", 1);
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(tmp.size()); // Throws
    memcpy(buffer.get(), tmp.c_str(), tmp.size());
    if (mkdtemp(buffer.get()) == 0) {
        throw std::system_error(errno, std::system_category(), "mkdtemp() failed"); // LCOV_EXCL_LINE
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
        m_fd = handle;
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

    if (!m_fd)
        return;
    if (m_have_lock)
        unlock();

    BOOL r = CloseHandle(m_fd);
    REALM_ASSERT_RELEASE(r);
    m_fd = nullptr;

#else // POSIX version

    if (m_fd < 0)
        return;
    int r = ::close(m_fd);
    REALM_ASSERT_RELEASE(r == 0);
    m_fd = -1;

#endif
}

size_t File::read_static(FileDesc fd, char* data, size_t size)
{
#ifdef _WIN32 // Windows version
    char* const data_0 = data;
    while (0 < size) {
        DWORD n = std::numeric_limits<DWORD>::max();
        if (int_less_than(size, n))
            n = static_cast<DWORD>(size);
        DWORD r = 0;
        if (!ReadFile(fd, data, n, &r, 0))
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
    throw std::system_error(err, std::system_category(), "ReadFile() failed");

#else // POSIX version

    char* const data_0 = data;
    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = ::read(fd, data, n);
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
    throw std::system_error(errno, std::system_category(), "read() failed");
    // LCOV_EXCL_STOP
#endif
}


size_t File::read(char* data, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

    if (m_encryption_key) {
        uint64_t pos_original = File::get_file_pos(m_fd);
        REALM_ASSERT(!int_cast_has_overflow<size_t>(pos_original));
        size_t pos = size_t(pos_original);
        Map<char> read_map(*this, access_ReadOnly, static_cast<size_t>(pos + size));
        realm::util::encryption_read_barrier(read_map, pos, size);
        memcpy(data, read_map.get_addr() + pos, size);
        uint64_t cur = File::get_file_pos(m_fd);
        seek_static(m_fd, cur + size);
        return read_map.get_size() - pos;
    }

    return read_static(m_fd, data, size);
}

void File::write_static(FileDesc fd, const char* data, size_t size)
{
#ifdef _WIN32
    while (0 < size) {
        DWORD n = std::numeric_limits<DWORD>::max();
        if (int_less_than(size, n))
            n = static_cast<DWORD>(size);
        DWORD r = 0;
        if (!WriteFile(fd, data, n, &r, 0))
            goto error;
        REALM_ASSERT_RELEASE(r == n); // Partial writes are not possible.
        size -= size_t(r);
        data += size_t(r);
    }
    return;

error:
    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    if (err == ERROR_HANDLE_DISK_FULL || err == ERROR_DISK_FULL) {
        std::string msg = get_last_error_msg("WriteFile() failed: ", err);
        throw OutOfDiskSpace(msg);
    }
    throw std::system_error(err, std::system_category(), "WriteFile() failed");
#else
    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = ::write(fd, data, n);
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
    if (err == ENOSPC || err == EDQUOT) {
        std::string msg = get_errno_msg("write() failed: ", err);
        throw OutOfDiskSpace(msg);
    }
    throw std::system_error(err, std::system_category(), "write() failed");
// LCOV_EXCL_STOP

#endif
}

void File::write(const char* data, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

    if (m_encryption_key) {
        uint64_t pos_original = get_file_pos(m_fd);
        REALM_ASSERT(!int_cast_has_overflow<size_t>(pos_original));
        size_t pos = size_t(pos_original);
        Map<char> write_map(*this, access_ReadWrite, static_cast<size_t>(pos + size));
        realm::util::encryption_read_barrier(write_map, pos, size);
        memcpy(write_map.get_addr() + pos, data, size);
        realm::util::encryption_write_barrier(write_map, pos, size);
        uint64_t cur = get_file_pos(m_fd);
        seek(cur + size);
        return;
    }

    write_static(m_fd, data, size);
}

uint64_t File::get_file_pos(FileDesc fd)
{
#ifdef _WIN32
    LONG high_dword = 0;
    LARGE_INTEGER li;
    LARGE_INTEGER res;
    li.QuadPart = 0;
    bool ok = SetFilePointerEx(fd, li, &res, FILE_CURRENT);
    if (!ok)
        throw std::system_error(GetLastError(), std::system_category(), "SetFilePointer() failed");

    return uint64_t(res.QuadPart);
#else
    return lseek(fd, 0, SEEK_CUR);
#endif
}

File::SizeType File::get_size_static(FileDesc fd)
{
#ifdef _WIN32
    LARGE_INTEGER large_int;
    if (GetFileSizeEx(fd, &large_int)) {
        File::SizeType size;
        if (int_cast_with_overflow_detect(large_int.QuadPart, size))
            throw util::overflow_error("File size overflow");

        return size;
    }
    throw std::system_error(GetLastError(), std::system_category(), "GetFileSizeEx() failed");

#else // POSIX version

    struct stat statbuf;
    if (::fstat(fd, &statbuf) == 0) {
        SizeType size;
        if (int_cast_with_overflow_detect(statbuf.st_size, size))
            throw util::overflow_error("File size overflow");

        return size;
    }
    throw std::system_error(errno, std::system_category(), "fstat() failed");

#endif
}

File::SizeType File::get_size() const
{
    REALM_ASSERT_RELEASE(is_attached());
    File::SizeType size = get_size_static(m_fd);

    if (m_encryption_key)
        return encrypted_size_to_data_size(size);
    else
        return size;
}


void File::resize(SizeType size)
{
    REALM_ASSERT_RELEASE(is_attached());

#ifdef _WIN32 // Windows version

    // Save file position
    SizeType p = get_file_pos(m_fd);

    if (m_encryption_key)
        size = data_size_to_encrypted_size(size);

    // Windows docs say "it is not an error to set the file pointer to a position beyond the end of the file."
    // so seeking with SetFilePointerEx() will not error out even if there is no disk space left.
    // In this scenario though, the following call to SedEndOfFile() will fail if there is no disk space left.
    seek(size);

    if (!SetEndOfFile(m_fd)) {
        DWORD err = GetLastError(); // Eliminate any risk of clobbering
        if (err == ERROR_HANDLE_DISK_FULL || err == ERROR_DISK_FULL) {
            std::string msg = get_last_error_msg("SetEndOfFile() failed: ", err);
            throw OutOfDiskSpace(msg);
        }
        throw std::system_error(err, std::system_category(), "SetEndOfFile() failed");
    }

    // Restore file position
    seek(p);

#else // POSIX version

    if (m_encryption_key)
        size = data_size_to_encrypted_size(size);

    off_t size2;
    if (int_cast_with_overflow_detect(size, size2))
        throw util::overflow_error("File size overflow");

    // POSIX specifies that introduced bytes read as zero. This is not
    // required by File::resize().
    if (::ftruncate(m_fd, size2) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        if (err == ENOSPC || err == EDQUOT) {
            std::string msg = get_errno_msg("ftruncate() failed: ", err);
            throw OutOfDiskSpace(msg);
        }
        throw std::system_error(err, std::system_category(), "ftruncate() failed");
    }

#endif
}


void File::prealloc(size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

    if (size <= to_size_t(get_size())) {
        return;
    }

    size_t new_size = size;
    if (m_encryption_key) {
        new_size = static_cast<size_t>(data_size_to_encrypted_size(size));
        if (new_size < size) {
            throw util::runtime_error("File size overflow: data_size_to_encrypted_size("
                                      + realm::util::to_string(size) + ") == " + realm::util::to_string(new_size));
        }
    }

    auto manually_consume_space = [&]() {
        constexpr size_t chunk_size = 4096;
        int64_t original_size = get_size_static(m_fd);
        seek(original_size);
        size_t num_bytes = size_t(new_size - original_size);
        std::string zeros(chunk_size, '\0');
        while (num_bytes > 0) {
            size_t t = num_bytes > chunk_size ? chunk_size : num_bytes;
            write_static(m_fd, zeros.c_str(), t);
            num_bytes -= t;
        }
    };

#if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version
    // Mostly Linux only
    if (!prealloc_if_supported(0, new_size)) {
        manually_consume_space();
    }
#else // Non-atomic fallback
#if REALM_PLATFORM_APPLE
    // posix_fallocate() is not supported on MacOS or iOS, so use a combination of fcntl(F_PREALLOCATE) and ftruncate().

    struct stat statbuf;
    if (::fstat(m_fd, &statbuf) != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category(), "fstat() inside prealloc() failed");
    }

    size_t allocated_size;
    if (int_cast_with_overflow_detect(statbuf.st_blocks, allocated_size)) {
        throw util::overflow_error("Overflow on block conversion to size_t " + realm::util::to_string(statbuf.st_blocks));
    }
    if (int_multiply_with_overflow_detect(allocated_size, S_BLKSIZE)) {
        throw util::overflow_error("Overflow computing existing file space allocation blocks: "
                                   + realm::util::to_string(allocated_size)
                                   + " block size: " + realm::util::to_string(S_BLKSIZE));
    }

    // Only attempt to preallocate space if there's not already sufficient free space in the file.
    // APFS would fail with EINVAL if we attempted it, and HFS+ would preallocate extra space unnecessarily.
    // See <https://github.com/realm/realm-core/issues/3005> for details.
    if (new_size > allocated_size) {

        off_t to_allocate = static_cast<off_t>(new_size - statbuf.st_size);
        fstore_t store = { F_ALLOCATEALL, F_PEOFPOSMODE, 0, to_allocate, 0 };
        int ret = fcntl(m_fd, F_PREALLOCATE, &store);
        if (ret == -1) {
            int err = errno;

            if (err == EINVAL) {
                // There's a timing sensitive bug on APFS which causes fcntl to sometimes throw EINVAL.
                // This might not be the case, but we'll fall back and attempt to manually allocate all the requested
                // space. Worst case, this might also fail, but there is also a chance it will succeed. We don't
                // call this in the first place because using fcntl(F_PREALLOCATE) will be faster if it works (it has
                // been reliable on HSF+).
                manually_consume_space();
            } else {
                std::string msg = util::format("fcntl() inside prealloc() failed allocating %1 bytes, new_size=%2, cur_size=%3, allocated_size=%4, event: ",
                                               to_allocate, new_size, statbuf.st_size, allocated_size);
                msg += util::make_basic_system_error_code(err).message();
                throw OutOfDiskSpace(msg);
            }
        }
    }

    int ret = 0;

    do {
        ret = ftruncate(m_fd, new_size);
    } while (ret == -1 && errno == EINTR);

    if (ret != 0) {
        int err = errno;
        // by the definition of F_PREALLOCATE, a proceeding ftruncate will not fail due to out of disk space
        // so this is some other runtime error and not OutOfDiskSpace
        throw std::system_error(err, std::system_category(), "ftruncate() inside prealloc() failed");
    }
#elif REALM_ANDROID || defined(_WIN32)

    manually_consume_space();

#else
    #error Please check if/how your OS supports file preallocation
#endif

#endif // !(_POSIX_C_SOURCE >= 200112L)
}


bool File::prealloc_if_supported(SizeType offset, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

#if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L // POSIX.1-2001 version

    REALM_ASSERT_RELEASE(is_prealloc_supported());

    if (size == 0) {
        // calling posix_fallocate with a size of 0 will cause a return of EINVAL
        // since this is a meaningless operation anyway, we just return early here
        return true;
    }

    // posix_fallocate() does not set errno, it returns the error (if any) or zero.
    // It is also possible for it to be interrupted by EINTR according to some man pages (ex fedora 24)
    int status;
    do {
        status = ::posix_fallocate(m_fd, offset, size);
    } while (status == EINTR);

    if (REALM_LIKELY(status == 0)) {
        return true;
    }

    if (status == ENOSPC || status == EDQUOT) {
        std::string msg = get_errno_msg("posix_fallocate() failed: ", status);
        throw OutOfDiskSpace(msg);
    }
    if (status == EINVAL) {
        return false; // Retry with non-atomic version
    }
    throw std::system_error(status, std::system_category(), "posix_fallocate() failed");

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
    return false;
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
#ifdef _WIN32
    seek_static(m_fd, position);
#else
    seek_static(m_fd, position);
#endif
}

void File::seek_static(FileDesc fd, SizeType position)
{
#ifdef _WIN32 // Windows version

    LARGE_INTEGER large_int;
    if (int_cast_with_overflow_detect(position, large_int.QuadPart))
        throw util::overflow_error("File position overflow");

    if (!SetFilePointerEx(fd, large_int, 0, FILE_BEGIN))
        throw std::system_error(GetLastError(), std::system_category(), "SetFilePointerEx() failed");

#else // POSIX version

    off_t position2;
    if (int_cast_with_overflow_detect(position, position2))
        throw util::overflow_error("File position overflow");

    if (0 <= ::lseek(fd, position2, SEEK_SET))
        return;
    throw std::system_error(errno, std::system_category(), "lseek() failed");

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

    if (FlushFileBuffers(m_fd))
        return;
    throw std::system_error(GetLastError(), std::system_category(), "FlushFileBuffers() failed");

#elif REALM_PLATFORM_APPLE

    if (::fcntl(m_fd, F_FULLFSYNC) == 0)
        return;
    throw std::system_error(errno, std::system_category(), "fcntl() with F_FULLSYNC failed");

#else // POSIX version

    if (::fsync(m_fd) == 0)
        return;
    throw std::system_error(errno, std::system_category(), "fsync() failed");

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
    if (LockFileEx(m_fd, flags, 0, 1, 0, &overlapped)) {
        m_have_lock = true;
        return true;
    }
    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    if (err == ERROR_LOCK_VIOLATION)
        return false;
    throw std::system_error(err, std::system_category(), "LockFileEx() failed");

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
    throw std::system_error(err, std::system_category(), "flock() failed");

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
    BOOL r = UnlockFileEx(m_fd, 0, 1, 0, &overlapped);

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
    REALM_ASSERT_RELEASE_EX(r == 0 && "File::unlock()", r, errno);

#endif
}


void* File::map(AccessMode a, size_t size, int /*map_flags*/, size_t offset) const
{
    return realm::util::mmap(m_fd, size, a, offset, m_encryption_key.get());
}

#if REALM_ENABLE_ENCRYPTION
void* File::map(AccessMode a, size_t size, EncryptedFileMapping*& mapping, int /*map_flags*/, size_t offset) const
{
    return realm::util::mmap(m_fd, size, a, offset, m_encryption_key.get(), mapping);
}
#endif

void File::unmap(void* addr, size_t size) noexcept
{
    realm::util::munmap(addr, size);
}


void* File::remap(void* old_addr, size_t old_size, AccessMode a, size_t new_size, int /*map_flags*/,
                  size_t file_offset) const
{
    return realm::util::mremap(m_fd, file_offset, old_addr, old_size, a, new_size, m_encryption_key.get());
}


void File::sync_map(FileDesc fd, void* addr, size_t size)
{
    realm::util::msync(fd, addr, size);
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
    throw std::system_error(err, std::system_category(), "access() failed");
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
    throw std::system_error(err, std::system_category(), "stat() failed");
#elif REALM_HAVE_STD_FILESYSTEM
    return std::filesystem::is_directory(path);
#else
    static_cast<void>(path);
    throw util::runtime_error("Not yet supported");
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
#ifdef _WIN32
    // Can't rename to existing file on Windows
    try_remove(new_path);
#endif
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

bool File::is_same_file_static(FileDesc f1, FileDesc f2)
{
#if defined(_WIN32) // Windows version
    FILE_ID_INFO fi1;
    FILE_ID_INFO fi2;
    if (GetFileInformationByHandleEx(f1, FILE_INFO_BY_HANDLE_CLASS::FileIdInfo, &fi1, sizeof(fi1))) {
        if (GetFileInformationByHandleEx(f2, FILE_INFO_BY_HANDLE_CLASS::FileIdInfo, &fi2, sizeof(fi2))) {
            return memcmp(&fi1.FileId, &fi2.FileId, sizeof(fi1.FileId)) == 0 &&
                   fi1.VolumeSerialNumber == fi2.VolumeSerialNumber;
        }
    }
    throw std::system_error(GetLastError(), std::system_category(), "GetFileInformationByHandleEx() failed");

#else // POSIX version

    struct stat statbuf;
    if (::fstat(f1, &statbuf) == 0) {
        dev_t device_id = statbuf.st_dev;
        ino_t inode_num = statbuf.st_ino;
        if (::fstat(f2, &statbuf) == 0)
            return device_id == statbuf.st_dev && inode_num == statbuf.st_ino;
    }
    throw std::system_error(errno, std::system_category(), "fstat() failed");

#endif
}

bool File::is_same_file(const File& f) const
{
    REALM_ASSERT_RELEASE(is_attached());
    REALM_ASSERT_RELEASE(f.is_attached());
    return is_same_file_static(m_fd, f.m_fd);
}

File::UniqueID File::get_unique_id() const
{
    REALM_ASSERT_RELEASE(is_attached());
#ifdef _WIN32 // Windows version
    throw util::runtime_error("Not yet supported");
#else // POSIX version
    struct stat statbuf;
    if (::fstat(m_fd, &statbuf) == 0) {
        return {statbuf.st_dev, statbuf.st_ino};
    }
    throw std::system_error(errno, std::system_category(), "fstat() failed");
#endif
}

FileDesc File::get_descriptor() const
{
    return m_fd;
}

bool File::get_unique_id(const std::string& path, File::UniqueID& uid)
{
#ifdef _WIN32 // Windows version
    throw std::runtime_error("Not yet supported");
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
    throw std::system_error(err, std::system_category(), "fstat() failed");
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
    throw std::system_error(errno, std::system_category(), "fstat() failed");

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
#elif REALM_HAVE_STD_FILESYSTEM
    std::filesystem::path path_(path.empty() ? "." : path);
    if (path_.is_absolute())
        return path;

    return (std::filesystem::path(base_dir) / path_).u8string();
#else
    static_cast<void>(path);
    static_cast<void>(base_dir);
    throw util::runtime_error("Not yet supported");
#endif
}


bool File::for_each(const std::string& dir_path, ForEachHandler handler)
{
    return for_each_helper(dir_path, "", handler); // Throws
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
        throw util::runtime_error("Encryption not enabled");
    }
#endif
}

const char* File::get_encryption_key()
{
    return m_encryption_key.get();
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
#if !defined(__linux__) && !REALM_PLATFORM_APPLE && !REALM_WINDOWS && !REALM_UWP && !REALM_ANDROID
#error "readdir() is not known to be thread-safe on this platform"
#endif

    if (!m_dirp)
        return false;

    for (;;) {
        // Note: readdir_r() is deprecated. However, all implementations
        // implement readdir() to be thread-safe when operating on separate
        // directory streams.
        // More information: lwn.net/Articles/696475/

        struct dirent* dirent;
        do {
            // readdir() signals both errors and end-of-stream by returning a
            // null pointer. To distinguish between end-of-stream and errors,
            // the manpage recommends setting errno specifically to 0 before
            // calling it...
            errno = 0;

            dirent = readdir(m_dirp);
        }
        while (!dirent && errno == EAGAIN);
        if (errno != 0) {
            throw std::system_error(errno, std::system_category(), "readdir() failed");
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

#elif REALM_HAVE_STD_FILESYSTEM

DirScanner::DirScanner(const std::string& path, bool allow_missing)
{
    try {
        m_iterator = std::filesystem::directory_iterator(path);
    }
    catch (const std::filesystem::filesystem_error& e) {
        if (e.code() != std::errc::no_such_file_or_directory || !allow_missing)
            throw;
    }
}

DirScanner::~DirScanner() = default;

bool DirScanner::next(std::string& name)
{
    const std::filesystem::directory_iterator end;
    if (m_iterator != end) {
        name = m_iterator->path().filename().u8string();
        m_iterator++;
        return true;
    }
    return false;
}

#else

DirScanner::DirScanner(const std::string&, bool)
{
    throw util::runtime_error("Not yet supported");
}

DirScanner::~DirScanner() noexcept
{
}

bool DirScanner::next(std::string&)
{
    return false;
}

#endif
