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

#include <realm/util/file.hpp>

#include <realm/unicode.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/file_mapper.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#include <sys/mman.h>
#include <sys/file.h> // BSD / Linux flock()
#include <sys/statvfs.h>
#endif

#if REALM_PLATFORM_APPLE
#include <sys/attr.h>
#include <sys/clonefile.h>
#endif

using namespace realm::util;

#ifndef _WIN32
// All mainstream platforms other than Windows migrated to 64-bit off_t many
// years ago. Supporting 32-bit off_t is possible, but not currently implemented.
static_assert(sizeof(off_t) == 8 || sizeof(size_t) == 4);
#endif

namespace {
bool for_each_helper(const std::string& path, const std::string& dir, realm::util::File::ForEachHandler& handler)
{
    using File = realm::util::File;
    realm::util::DirScanner ds{path}; // Throws
    std::string name;
    while (ds.next(name)) {                              // Throws
        std::string subpath = File::resolve(name, path); // Throws
        bool go_on;
        if (File::is_dir(subpath)) {                           // Throws
            std::string subdir = File::resolve(name, dir);     // Throws
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

#ifdef _WIN32

std::string get_last_error_msg(const char* prefix, DWORD err)
{
    std::string buffer;
    buffer.append(prefix);
    size_t offset = buffer.size();
    size_t max_msg_size = 1024;
    buffer.resize(offset + max_msg_size);
    DWORD flags = FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
    DWORD language_id = MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT);
    DWORD size =
        FormatMessageA(flags, 0, err, language_id, buffer.data() + offset, static_cast<DWORD>(max_msg_size), 0);
    if (0 < size)
        return buffer;
    buffer.resize(offset);
    buffer.append("Unknown error");
    return buffer;
}

struct WindowsFileHandleHolder {
    WindowsFileHandleHolder() = default;
    explicit WindowsFileHandleHolder(HANDLE h)
        : handle(h)
    {
    }

    WindowsFileHandleHolder(WindowsFileHandleHolder&&) = delete;
    WindowsFileHandleHolder(const WindowsFileHandleHolder&) = delete;
    WindowsFileHandleHolder& operator=(WindowsFileHandleHolder&&) = delete;
    WindowsFileHandleHolder& operator=(const WindowsFileHandleHolder&) = delete;

    operator HANDLE() const noexcept
    {
        return handle;
    }

    ~WindowsFileHandleHolder()
    {
        if (handle != INVALID_HANDLE_VALUE) {
            ::CloseHandle(handle);
        }
    }

    HANDLE handle = INVALID_HANDLE_VALUE;
};

#endif

#if REALM_HAVE_STD_FILESYSTEM
using std::filesystem::u8path;

void throwIfCreateDirectoryError(std::error_code error, const std::string& path)
{
    if (!error)
        return;

    // create_directory doesn't raise an error if the path already exists
    using std::errc;
    if (error == errc::permission_denied || error == errc::read_only_file_system) {
        throw realm::FileAccessError(realm::ErrorCodes::PermissionDenied, error.message(), path);
    }
    else {
        throw realm::FileAccessError(realm::ErrorCodes::FileOperationFailed, error.message(), path);
    }
}

void throwIfFileError(std::error_code error, const std::string& path)
{
    if (!error)
        return;

    using std::errc;
    if (error == errc::permission_denied || error == errc::read_only_file_system ||
        error == errc::device_or_resource_busy || error == errc::operation_not_permitted ||
        error == errc::file_exists || error == errc::directory_not_empty) {
        throw realm::FileAccessError(realm::ErrorCodes::PermissionDenied, error.message(), path);
    }
    else {
        throw realm::FileAccessError(realm::ErrorCodes::FileOperationFailed, error.message(), path);
    }
}
#endif

} // anonymous namespace


namespace realm::util {
namespace {

/// Thrown if create_Always was specified and the file did already
/// exist.
class Exists : public FileAccessError {
public:
    Exists(const std::string& msg, const std::string& path)
        : FileAccessError(ErrorCodes::FileAlreadyExists, msg, path)
    {
    }
};

} // anonymous namespace


bool try_make_dir(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    std::error_code error;
    bool result = std::filesystem::create_directory(u8path(path), error);
    throwIfCreateDirectoryError(error, path);
    return result;
#else // POSIX
    if (::mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0)
        return true;

    int err = errno; // Eliminate any risk of clobbering
    if (err == EEXIST)
        return false;

    auto msg = format_errno("Failed to create directory at '%2': %1", err, path);

    switch (err) {
        case EACCES:
        case EROFS:
            throw FileAccessError(ErrorCodes::PermissionDenied, msg, path, err);
        default:
            throw FileAccessError(ErrorCodes::FileOperationFailed, msg, path, err); // LCOV_EXCL_LINE
    }
#endif
}


void make_dir(const std::string& path)
{
    if (try_make_dir(path)) // Throws
        return;
    throw Exists(format_errno("Failed to create directory at '%2': %1", EEXIST, path), path);
}


void make_dir_recursive(std::string path)
{
#if REALM_HAVE_STD_FILESYSTEM
    std::error_code error;
    std::filesystem::create_directories(u8path(path), error);
    throwIfCreateDirectoryError(error, path);
#else
    // Skip the first separator as we're assuming an absolute path
    size_t pos = path.find_first_of("/\\");
    if (pos == std::string::npos)
        return;
    pos += 1;

    while (pos < path.size()) {
        auto sep = path.find_first_of("/\\", pos);
        char c = 0;
        if (sep < path.size()) {
            c = path[sep];
            path[sep] = 0;
        }
        try_make_dir(path);
        if (c) {
            path[sep] = c;
            pos = sep + 1;
        }
        else {
            break;
        }
    }
#endif
}


void remove_dir(const std::string& path)
{
    if (try_remove_dir(path)) // Throws
        return;
    int err = ENOENT;
    std::string msg = format_errno("Failed to remove directory '%2': %1", err, path);
    throw FileAccessError(ErrorCodes::FileNotFound, msg, path, err);
}


bool try_remove_dir(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    std::error_code error;
    bool result = std::filesystem::remove(u8path(path), error);
    throwIfFileError(error, path);
    return result;
#else // POSIX
    if (::rmdir(path.c_str()) == 0)
        return true;

    int err = errno; // Eliminate any risk of clobbering
    if (err == ENOENT)
        return false;

    std::string msg = format_errno("Failed to remove directory '%2': %1", err, path);
    switch (err) {
        case EACCES:
        case EROFS:
        case EBUSY:
        case EPERM:
        case EEXIST:
        case ENOTEMPTY:
            throw FileAccessError(ErrorCodes::PermissionDenied, msg, path, err);
        default:
            throw FileAccessError(ErrorCodes::FileOperationFailed, msg, path, err); // LCOV_EXCL_LINE
    }
#endif
}


bool try_remove_dir_recursive(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    std::error_code error;
    auto removed_count = std::filesystem::remove_all(u8path(path), error);
    throwIfFileError(error, path);
    return removed_count > 0;
#else
    {
        bool allow_missing = true;
        DirScanner ds{path, allow_missing}; // Throws
        std::string name;
        while (ds.next(name)) {                              // Throws
            std::string subpath = File::resolve(name, path); // Throws
            if (File::is_dir(subpath)) {                     // Throws
                try_remove_dir_recursive(subpath);           // Throws
            }
            else {
                File::remove(subpath); // Throws
            }
        }
    }
    return try_remove_dir(path); // Throws
#endif
}


std::string make_temp_dir()
{
#ifdef _WIN32 // Windows version
    std::filesystem::path temp = std::filesystem::temp_directory_path();

    wchar_t buffer[MAX_PATH];
    std::filesystem::path path;
    for (;;) {
        if (GetTempFileNameW(temp.c_str(), L"rlm", 0, buffer) == 0) {
            DWORD error = GetLastError();
            throw SystemError(error, get_last_error_msg("GetTempFileName() failed: ", error));
        }
        path = buffer;
        std::filesystem::remove(path);

        std::error_code error;
        std::filesystem::create_directory(path, error);
        if (error && error != std::errc::file_exists) {
            throw SystemError(error, util::format("Failed to create temporary directory: %1", error.message()));
        }
        break;
    }
    return path.u8string();

#else // POSIX.1-2008 version

#if REALM_ANDROID
    std::string buffer = "/data/local/tmp/realm_XXXXXX";
#else
    char* tmp_dir_env = getenv("TMPDIR");
    std::string buffer = tmp_dir_env ? tmp_dir_env : std::string(P_tmpdir);
    if (!buffer.empty() && buffer.back() != '/') {
        buffer += "/";
    }
    buffer += "realm_XXXXXX";
#endif

    if (mkdtemp(buffer.data()) == 0) {
        int err = errno;
        throw SystemError(err, util::format("Failed to create temporary directory: %1", err)); // LCOV_EXCL_LINE
    }
    return buffer;
#endif
}

std::string make_temp_file(const char* prefix)
{
#ifdef _WIN32 // Windows version
    std::filesystem::path temp = std::filesystem::temp_directory_path();

    wchar_t buffer[MAX_PATH];
    if (GetTempFileNameW(temp.c_str(), L"rlm", 0, buffer) == 0) {
        DWORD error = GetLastError();
        throw SystemError(error, get_last_error_msg("GetTempFileName() failed: ", error));
    }

    return std::filesystem::path(buffer).u8string();

#else // POSIX.1-2008 version

#if REALM_ANDROID
    std::string base_dir = "/data/local/tmp/";
#else
    char* tmp_dir_env = getenv("TMPDIR");
    std::string base_dir = tmp_dir_env ? tmp_dir_env : std::string(P_tmpdir);
    if (!base_dir.empty() && base_dir[base_dir.length() - 1] != '/') {
        base_dir += '/';
    }
#endif
    std::string filename = util::format("%1%2_XXXXXX", base_dir, prefix);
    auto fd = mkstemp(filename.data());
    if (fd == -1) {
        throw std::system_error(errno, std::system_category(), "mkstemp() failed"); // LCOV_EXCL_LINE
    }
    close(fd);
    return filename;
#endif
}

size_t page_size()
{
    static constexpr size_t c_min_supported_page_size = 4096;
    static size_t page_size = [] {
#ifdef _WIN32
        SYSTEM_INFO sysinfo;
        GetNativeSystemInfo(&sysinfo);
        // DWORD size = sysinfo.dwPageSize;
        // On windows we use the allocation granularity instead
        DWORD size = sysinfo.dwAllocationGranularity;
#else
        long size = sysconf(_SC_PAGESIZE);
#endif
        REALM_ASSERT(size > 0 && size % c_min_supported_page_size == 0);
        return static_cast<size_t>(size);
    }();
    return page_size;
}

File::File() = default;
File::File(std::string_view path, Mode m)
{
    open(path, m);
}

File::~File() noexcept
{
    close();
}

File::File(File&& f) noexcept
{
    m_fd = std::exchange(f.m_fd, invalid_fd);
#ifdef REALM_FILELOCK_EMULATION
    m_pipe_fd = std::exchange(f.m_pipe_fd, invalid_fd);
    m_has_exclusive_lock = std::exchange(f.m_has_exclusive_lock, false);
#endif
    m_have_lock = std::exchange(f.m_have_lock, false);
    m_encryption = std::move(f.m_encryption);
}

File& File::operator=(File&& f) noexcept
{
    close();

    m_fd = std::exchange(f.m_fd, invalid_fd);
#ifdef REALM_FILELOCK_EMULATION
    m_pipe_fd = std::exchange(f.m_pipe_fd, invalid_fd);
    m_has_exclusive_lock = std::exchange(f.m_has_exclusive_lock, false);
#endif
    m_have_lock = std::exchange(f.m_have_lock, false);
    m_encryption = std::move(f.m_encryption);
    return *this;
}


void File::open_internal(std::string_view path, AccessMode a, CreateMode c, int flags, bool* success)
{
    REALM_ASSERT_RELEASE(!is_attached());
    m_path = path; // for error reporting and debugging

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
    m_fd = CreateFile2(u8path(m_path).c_str(), desired_access, share_mode, creation_disposition, nullptr);
    if (m_fd != INVALID_HANDLE_VALUE) {
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
            throw FileAccessError(ErrorCodes::PermissionDenied, msg, m_path, int(err));
        case ERROR_FILE_NOT_FOUND:
        case ERROR_PATH_NOT_FOUND:
            throw FileAccessError(ErrorCodes::FileNotFound, msg, m_path, int(err));
        case ERROR_FILE_EXISTS:
            throw Exists(msg, m_path);
        default:
            throw FileAccessError(ErrorCodes::FileOperationFailed, msg, m_path, int(err));
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
    int fd = ::open(m_path.c_str(), flags2, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (0 <= fd) {
        m_fd = fd;
        m_have_lock = false;
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
    std::string msg = format_errno("Failed to open file at path '%2': %1", err, path);
    switch (err) {
        case EACCES:
        case EPERM:
        case EROFS:
        case ETXTBSY:
            throw FileAccessError(ErrorCodes::PermissionDenied, msg, path, err);
        case ENOENT:
            if (c != create_Never)
                msg = util::format("Failed to open file at path '%1': parent directory does not exist", path);
            throw FileAccessError(ErrorCodes::FileNotFound, msg, path, err);
        case EEXIST:
            throw Exists(msg, m_path);
        case ENOTDIR:
            msg = format("Failed to open file at path '%1': parent path is not a directory", path);
            [[fallthrough]];
        default:
            throw FileAccessError(ErrorCodes::FileOperationFailed, msg, path, err); // LCOV_EXCL_LINE
    }

#endif
}


void File::close() noexcept
{
    m_encryption.reset();
    if (m_fd == invalid_fd)
        return;
    if (m_have_lock)
        unlock();
#ifdef _WIN32 // Windows version
    BOOL r = CloseHandle(m_fd);
    REALM_ASSERT_RELEASE(r);
#else // POSIX version
    int r = ::close(m_fd);
    REALM_ASSERT_RELEASE(r == 0);
#endif

    m_fd = invalid_fd;
}

size_t File::read_static(FileDesc fd, SizeType pos, char* data, size_t size)
{
#ifdef _WIN32 // Windows version
    char* const data_0 = data;
    while (0 < size) {
        DWORD n = std::numeric_limits<DWORD>::max();
        if (int_less_than(size, n))
            n = static_cast<DWORD>(size);
        DWORD r = 0;
        OVERLAPPED o{};
        o.Offset = static_cast<DWORD>(pos);
        o.OffsetHigh = static_cast<DWORD>(pos >> 32);
        if (!ReadFile(fd, data, n, &r, &o)) {
            DWORD err = GetLastError();
            if (err == ERROR_HANDLE_EOF)
                break;
            throw SystemError(int(err), "ReadFile() failed");
        }
        REALM_ASSERT_RELEASE(r > 0 && r <= n);
        size -= size_t(r);
        data += size_t(r);
        pos += r;
    }
    return data - data_0;

#else // POSIX version

    char* const data_0 = data;
    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = pread(fd, data, n, pos);
        if (r == 0)
            break;
        if (r < 0)
            goto error; // LCOV_EXCL_LINE
        REALM_ASSERT_RELEASE(size_t(r) <= n);
        size -= size_t(r);
        data += size_t(r);
        pos += r;
    }
    return data - data_0;

error:
    // LCOV_EXCL_START
    throw SystemError(errno, "read() failed");
// LCOV_EXCL_STOP
#endif
}


size_t File::read(SizeType pos, char* data, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

    if (m_encryption) {
        Map<char> read_map(*this, pos, access_ReadOnly, size);
        util::encryption_read_barrier(read_map, 0, size);
        memcpy(data, read_map.get_addr(), size);
        return size;
    }

    return read_static(m_fd, pos, data, size);
}

void File::write_static(FileDesc fd, SizeType pos, const char* data, size_t size)
{
#ifdef _WIN32
    while (0 < size) {
        DWORD n = std::numeric_limits<DWORD>::max();
        if (int_less_than(size, n))
            n = static_cast<DWORD>(size);
        DWORD r = 0;
        OVERLAPPED o{};
        o.Offset = static_cast<DWORD>(pos);
        o.OffsetHigh = static_cast<DWORD>(pos >> 32);
        if (!WriteFile(fd, data, n, &r, &o))
            goto error;
        REALM_ASSERT_RELEASE(r == n); // Partial writes are not possible.
        size -= size_t(r);
        data += size_t(r);
        pos += r;
    }
    return;

error:
    DWORD err = GetLastError(); // Eliminate any risk of clobbering
    if (err == ERROR_HANDLE_DISK_FULL || err == ERROR_DISK_FULL) {
        std::string msg = get_last_error_msg("WriteFile() failed: ", err);
        throw OutOfDiskSpace(msg);
    }
    throw SystemError(err, "WriteFile() failed");
#else
    while (0 < size) {
        // POSIX requires that 'n' is less than or equal to SSIZE_MAX
        size_t n = std::min(size, size_t(SSIZE_MAX));
        ssize_t r = pwrite(fd, data, n, pos);
        if (r < 0)
            goto error; // LCOV_EXCL_LINE
        REALM_ASSERT_RELEASE(r != 0);
        REALM_ASSERT_RELEASE(size_t(r) <= n);
        size -= size_t(r);
        data += size_t(r);
        pos += off_t(r);
    }
    return;

error:
    // LCOV_EXCL_START
    int err = errno; // Eliminate any risk of clobbering
    auto msg = format_errno("write() failed: %1", err);
    if (err == ENOSPC || err == EDQUOT) {
        throw OutOfDiskSpace(msg);
    }
    throw SystemError(err, msg);
    // LCOV_EXCL_STOP

#endif
}

void File::write(SizeType pos, const char* data, size_t size)
{
    REALM_ASSERT_RELEASE(is_attached());

    if (m_encryption) {
        Map<char> write_map(*this, pos, access_ReadWrite, size);
        util::encryption_read_barrier(write_map, 0, size);
        memcpy(write_map.get_addr(), data, size);
        realm::util::encryption_write_barrier(write_map, 0, size);
        return;
    }

    write_static(m_fd, pos, data, size);
}

File::SizeType File::get_file_pos()
{
#ifdef _WIN32
    LONG high_dword = 0;
    LARGE_INTEGER li;
    LARGE_INTEGER res;
    li.QuadPart = 0;
    bool ok = SetFilePointerEx(m_fd, li, &res, FILE_CURRENT);
    if (!ok)
        throw SystemError(GetLastError(), "SetFilePointer() failed");

    return SizeType(res.QuadPart);
#else
    auto pos = lseek(m_fd, 0, SEEK_CUR);
    if (pos < 0) {
        throw SystemError(errno, "lseek() failed");
    }
    return SizeType(pos);
#endif
}

File::SizeType File::get_size_static(const std::string& path)
{
    File f(path);
    return f.get_size();
}

File::SizeType File::get_size_static(FileDesc fd)
{
#ifdef _WIN32
    LARGE_INTEGER large_int;
    if (GetFileSizeEx(fd, &large_int)) {
        File::SizeType size;
        if (int_cast_with_overflow_detect(large_int.QuadPart, size))
            throw RuntimeError(ErrorCodes::RangeError, "File size overflow");

        return size;
    }
    throw SystemError(GetLastError(), "GetFileSizeEx() failed");

#else // POSIX version

    struct stat statbuf;
    if (::fstat(fd, &statbuf) == 0) {
        SizeType size;
        if (int_cast_with_overflow_detect(statbuf.st_size, size))
            throw RuntimeError(ErrorCodes::RangeError, "File size overflow");

        return size;
    }
    throw SystemError(errno, "fstat() failed");

#endif
}

File::SizeType File::get_size() const
{
    REALM_ASSERT_RELEASE(is_attached());
    File::SizeType size = get_size_static(m_fd);

    if (m_encryption) {
        return encrypted_size_to_data_size(size);
    }
    return size;
}


void File::resize(SizeType size)
{
    REALM_ASSERT_RELEASE(is_attached());

    if (m_encryption)
        size = data_size_to_encrypted_size(size);

#ifdef _WIN32 // Windows version
    FILE_END_OF_FILE_INFO info;
    info.EndOfFile.QuadPart = size;
    if (!SetFileInformationByHandle(m_fd, FileEndOfFileInfo, &info, sizeof(info))) {
        DWORD err = GetLastError(); // Eliminate any risk of clobbering
        if (err == ERROR_HANDLE_DISK_FULL || err == ERROR_DISK_FULL) {
            throw OutOfDiskSpace(get_last_error_msg("SetFileInformationByHandle() failed: ", err));
        }
        throw SystemError(int(err), "SetFileInformationByHandle() failed");
    }
#else // POSIX version

    off_t size2;
    if (int_cast_with_overflow_detect(size, size2))
        throw RuntimeError(ErrorCodes::RangeError, "File size overflow");

    // POSIX specifies that introduced bytes read as zero. This is not
    // required by File::resize().
    if (::ftruncate(m_fd, size2) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        auto msg = format_errno("ftruncate() failed: %1", err);
        if (err == ENOSPC || err == EDQUOT) {
            throw OutOfDiskSpace(msg);
        }
        throw SystemError(err, msg);
    }

#endif
}


void File::prealloc(SizeType size)
{
    REALM_ASSERT_RELEASE(is_attached());
    if (size <= get_size()) {
        return;
    }

    SizeType new_size = size;
    if (m_encryption) {
        new_size = data_size_to_encrypted_size(size);
        REALM_ASSERT(new_size > size);
        REALM_ASSERT(size == encrypted_size_to_data_size(new_size));
    }

    auto manually_consume_space = [&]() {
        constexpr uint16_t chunk_size = 4096;
        SizeType write_pos = get_size_static(m_fd); // raw size
        SizeType num_bytes = new_size - write_pos;
        std::string zeros(chunk_size, '\0');
        while (num_bytes > 0) {
            uint16_t t = uint16_t(std::min<SizeType>(num_bytes, chunk_size));
            write_static(m_fd, write_pos, zeros.c_str(), t);
            num_bytes -= t;
            write_pos += t;
        }
    };

#if REALM_HAVE_POSIX_FALLOCATE
    // Mostly Linux only
    if (!prealloc_if_supported(0, new_size)) {
        manually_consume_space();
    }
#elif REALM_PLATFORM_APPLE // Non-atomic fallback
    // posix_fallocate() is not supported on MacOS or iOS, so use a combination of fcntl(F_PREALLOCATE) and
    // ftruncate().

    struct stat statbuf;
    if (::fstat(m_fd, &statbuf) != 0) {
        int err = errno;
        throw SystemError(err, "fstat() inside prealloc() failed");
    }

    SizeType allocated_size;
    if (int_cast_with_overflow_detect(statbuf.st_blocks, allocated_size)) {
        throw RuntimeError(ErrorCodes::RangeError,
                           util::format("Overflow on block conversion to SizeType %1", statbuf.st_blocks));
    }
    if (int_multiply_with_overflow_detect(allocated_size, S_BLKSIZE)) {
        throw RuntimeError(ErrorCodes::RangeError,
                           util::format("Overflow computing existing file space allocation blocks: %1 block size %2",
                                        allocated_size, S_BLKSIZE));
    }

    // Only attempt to preallocate space if there's not already sufficient free space in the file.
    // APFS would fail with EINVAL if we attempted it, and HFS+ would preallocate extra space unnecessarily.
    // See <https://github.com/realm/realm-core/issues/3005> for details.
    if (new_size > allocated_size) {
        off_t to_allocate = static_cast<off_t>(new_size - statbuf.st_size);
        fstore_t store = {F_ALLOCATEALL, F_PEOFPOSMODE, 0, to_allocate, 0};
        int ret = 0;
        do {
            ret = fcntl(m_fd, F_PREALLOCATE, &store);
        } while (ret == -1 && errno == EINTR);
        if (ret == -1) {
            // Consider fcntl() as an optimization on Apple devices, where if it fails,
            // we fall back to manually consuming space which is slower but may succeed in some
            // cases where fcntl fails. Some known cases are:
            // 1) There's a timing sensitive bug on APFS which causes fcntl to sometimes throw EINVAL.
            // This might not be the case, but we'll fall back and attempt to manually allocate all the requested
            // space. Worst case, this might also fail, but there is also a chance it will succeed. We don't
            // call this in the first place because using fcntl(F_PREALLOCATE) will be faster if it works (it has
            // been reliable on HSF+).
            // 2) fcntl will fail with ENOTSUP on non-supported file systems such as ExFAT. In this case
            // the fallback should succeed.
            // 3) if there is some other error such as no space left (ENOSPC) we will expect to fail again later
            manually_consume_space();
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
        throw SystemError(err, "ftruncate() inside prealloc() failed");
    }
#elif REALM_ANDROID || defined(_WIN32) || defined(__EMSCRIPTEN__)
    manually_consume_space();
#else
#error Please check if/how your OS supports file preallocation
#endif // REALM_HAVE_POSIX_FALLOCATE
}


bool File::prealloc_if_supported(SizeType offset, SizeType size)
{
    REALM_ASSERT_RELEASE(is_attached());

#if REALM_HAVE_POSIX_FALLOCATE

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

    if (status == EINVAL || status == EPERM || status == EOPNOTSUPP) {
        return false; // Retry with non-atomic version
    }

    auto msg = format_errno("posix_fallocate() failed: %1", status);
    if (status == ENOSPC || status == EDQUOT) {
        throw OutOfDiskSpace(msg);
    }
    throw SystemError(status, msg);

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
#if REALM_HAVE_POSIX_FALLOCATE
    return true;
#else
    return false;
#endif
}

void File::seek(SizeType position)
{
    REALM_ASSERT_RELEASE(is_attached());
    seek_static(m_fd, position);
}

void File::seek_static(FileDesc fd, SizeType position)
{
#ifdef _WIN32 // Windows version

    LARGE_INTEGER large_int;
    if (int_cast_with_overflow_detect(position, large_int.QuadPart))
        throw RuntimeError(ErrorCodes::RangeError, "File position overflow");

    if (!SetFilePointerEx(fd, large_int, 0, FILE_BEGIN))
        throw SystemError(GetLastError(), "SetFilePointerEx() failed");

#else // POSIX version

    off_t position2;
    if (int_cast_with_overflow_detect(position, position2))
        throw RuntimeError(ErrorCodes::RangeError, "File position overflow");

    if (0 <= ::lseek(fd, position2, SEEK_SET))
        return;
    throw SystemError(errno, "lseek() failed");

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
    throw SystemError(GetLastError(), "FlushFileBuffers() failed");

#elif REALM_PLATFORM_APPLE

    if (::fcntl(m_fd, F_FULLFSYNC) == 0)
        return;
    throw SystemError(errno, "fcntl() with F_FULLSYNC failed");

#else // POSIX version

    if (::fsync(m_fd) == 0)
        return;
    throw SystemError(errno, "fsync() failed");

#endif
}

void File::barrier()
{
#if REALM_PLATFORM_APPLE
    if (::fcntl(m_fd, F_BARRIERFSYNC) == 0)
        return;
        // If fcntl fails, we fallback to full sync.
        // This is known to occur on exFAT which does not support F_BARRIERSYNC.
#endif
    sync();
}

#ifndef _WIN32
// little helper
static void _unlock(int m_fd)
{
    int r;
    do {
        r = flock(m_fd, LOCK_UN);
    } while (r != 0 && errno == EINTR);
    if (r) {
        throw SystemError(errno, "File::unlock() has failed");
    }
}
#endif

bool File::rw_lock(bool exclusive, bool non_blocking)
{
    // exclusive blocking rw locks not implemented for emulation
    REALM_ASSERT(!exclusive || non_blocking);

#ifndef REALM_FILELOCK_EMULATION
    return lock(exclusive, non_blocking);
#else
    REALM_ASSERT(!m_has_exclusive_lock && !has_shared_lock());

    // First obtain an exclusive lock on the file proper
    int operation = LOCK_EX;
    if (non_blocking)
        operation |= LOCK_NB;
    int status;
    do {
        status = flock(m_fd, operation);
    } while (status != 0 && errno == EINTR);
    if (status != 0 && errno == EWOULDBLOCK)
        return false;
    if (status != 0)
        throw SystemError(errno, "flock() failed");
    m_has_exclusive_lock = true;

    // Every path through this function except for successfully acquiring an
    // exclusive lock needs to release the flock() before returning.
    UnlockGuard ulg(*this);

    // now use a named pipe to emulate locking in conjunction with using exclusive lock
    // on the file proper.
    // exclusively locked: we can't sucessfully write to the pipe.
    //                     AND we continue to hold the exclusive lock.
    //                     (unlock must lift the exclusive lock).
    // shared locked: we CAN succesfully write to the pipe. We open the pipe for reading
    //                before releasing the exclusive lock.
    //                (unlock must close the pipe for reading)
    REALM_ASSERT_RELEASE(m_pipe_fd == -1);
    if (m_fifo_path.empty())
        m_fifo_path = m_path + ".fifo";

    // Due to a bug in iOS 10-12 we need to open in read-write mode for shared
    // or the OS will deadlock when un-suspending the app.
    int mode = exclusive ? O_WRONLY | O_NONBLOCK : O_RDWR | O_NONBLOCK;

    // Optimistically try to open the fifo. This may fail due to the fifo not
    // existing, but since it usually exists this is faster than trying to create
    // it first.
    int fd = ::open(m_fifo_path.c_str(), mode);
    if (fd == -1) {
        int err = errno;
        if (exclusive) {
            if (err == ENOENT || err == ENXIO) {
                // If the fifo either doesn't exist or there's no readers with the
                // other end of the pipe open (ENXIO) then we have an exclusive lock
                // and are done.
                ulg.release();
                return true;
            }

            // Otherwise we got an unexpected error
            throw std::system_error(err, std::system_category(), "opening lock fifo for writing failed");
        }

        if (err == ENOENT) {
            // The fifo doesn't exist and we're opening in shared mode, so we
            // need to create it.
            if (!m_fifo_dir_path.empty())
                try_make_dir(m_fifo_dir_path);
            status = mkfifo(m_fifo_path.c_str(), 0666);
            if (status != 0)
                throw std::system_error(errno, std::system_category(), "creating lock fifo for reading failed");

            // Try again to open the fifo now that it exists
            fd = ::open(m_fifo_path.c_str(), mode);
            err = errno;
        }

        if (fd == -1)
            throw std::system_error(err, std::system_category(), "opening lock fifo for reading failed");
    }

    // We successfully opened the pipe. If we're trying to acquire an exclusive
    // lock that means there's a reader (aka a shared lock) and we've failed.
    // Release the exclusive lock and back out.
    if (exclusive) {
        ::close(fd);
        return false;
    }

    // We're in shared mode, so opening the fifo means we've successfully acquired
    // a shared lock and are done.
    ulg.release();
    rw_unlock();
    m_pipe_fd = fd;
    return true;
#endif // REALM_FILELOCK_EMULATION
}

bool File::lock(bool exclusive, bool non_blocking)
{
    REALM_ASSERT_RELEASE(is_attached());
    REALM_ASSERT_RELEASE(!m_have_lock);

#ifdef _WIN32 // Windows version

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
#else // _WIN32
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
        if (flock(m_fd, operation) == 0) {
            m_have_lock = true;
            return true;
        }
    } while (errno == EINTR);
    int err = errno; // Eliminate any risk of clobbering
    if (err == EWOULDBLOCK)
        return false;
    throw SystemError(err, "flock() failed");
#endif
}

void File::unlock() noexcept
{
    if (!m_have_lock)
        return;

#ifdef _WIN32 // Windows version
    OVERLAPPED overlapped;
    overlapped.hEvent = 0;
    overlapped.OffsetHigh = 0;
    overlapped.Offset = 0;
    overlapped.Pointer = 0;
    BOOL r = UnlockFileEx(m_fd, 0, 1, 0, &overlapped);
    REALM_ASSERT_RELEASE(r);
#else
    _unlock(m_fd);
#endif
    m_have_lock = false;
}

void File::rw_unlock() noexcept
{
#ifndef REALM_FILELOCK_EMULATION
    unlock();
#else
    // Coming here with an exclusive lock, we must release that lock.
    // Coming here with a shared lock, we must close the pipe that we have opened for reading.
    //   - we have to do that under the protection of a proper exclusive lock to serialize
    //     with anybody trying to obtain a lock concurrently.
    if (has_shared_lock()) {
        // shared lock. We need to reacquire the exclusive lock on the file
        int status;
        do {
            status = flock(m_fd, LOCK_EX);
        } while (status != 0 && errno == EINTR);
        REALM_ASSERT(status == 0);
        // close the pipe (== release the shared lock)
        ::close(m_pipe_fd);
        m_pipe_fd = -1;
    }
    else {
        REALM_ASSERT(m_has_exclusive_lock);
    }
    _unlock(m_fd);
    m_has_exclusive_lock = false;
#endif // REALM_FILELOCK_EMULATION
}

bool File::exists(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    return std::filesystem::exists(u8path(path));
#else // POSIX
    if (::access(path.c_str(), F_OK) == 0)
        return true;
    int err = errno; // Eliminate any risk of clobbering
    switch (err) {
        case EACCES:
        case ENOENT:
        case ENOTDIR:
            return false;
    }
    throw SystemError(err, "access() failed");
#endif
}


bool File::is_dir(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    return std::filesystem::is_directory(u8path(path));
#elif !defined(_WIN32)
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
    throw SystemError(err, "stat() failed");
#else
    static_cast<void>(path);
    throw NotImplemented();
#endif
}


void File::remove(const std::string& path)
{
    if (try_remove(path))
        return;
    int err = ENOENT;
    std::string msg = format_errno("Failed to delete file at '%2': %1", err, path);
    throw FileAccessError(ErrorCodes::FileNotFound, msg, path, err);
}


bool File::try_remove(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    std::error_code error;
    bool result = std::filesystem::remove(u8path(path), error);
    throwIfFileError(error, path);
    return result;
#else // POSIX
    if (::unlink(path.c_str()) == 0)
        return true;

    int err = errno; // Eliminate any risk of clobbering
    if (err == ENOENT)
        return false;

    std::string msg = format_errno("Failed to delete file at '%2': %1", err, path);
    switch (err) {
        case EACCES:
        case EROFS:
        case ETXTBSY:
        case EBUSY:
        case EPERM:
            throw FileAccessError(ErrorCodes::PermissionDenied, msg, path, err);
        case ENOENT:
            return false;
        default:
            throw FileAccessError(ErrorCodes::FileOperationFailed, msg, path, err);
    }
#endif
}


void File::move(const std::string& old_path, const std::string& new_path)
{
#if REALM_HAVE_STD_FILESYSTEM
    std::error_code error;
    std::filesystem::rename(u8path(old_path), u8path(new_path), error);

    if (error == std::errc::no_such_file_or_directory) {
        throw FileAccessError(ErrorCodes::FileNotFound, error.message(), old_path);
    }
    throwIfFileError(error, old_path);
#else
    int r = rename(old_path.c_str(), new_path.c_str());
    if (r == 0)
        return;
    int err = errno; // Eliminate any risk of clobbering
    std::string msg = format_errno("Failed to rename file from '%2' to '%3': %1", err, old_path, new_path);
    switch (err) {
        case EACCES:
        case EROFS:
        case ETXTBSY:
        case EBUSY:
        case EPERM:
        case EEXIST:
        case ENOTEMPTY:
            throw FileAccessError(ErrorCodes::PermissionDenied, msg, old_path, err);
        case ENOENT:
            throw FileAccessError(ErrorCodes::FileNotFound, msg, old_path, err);
        default:
            throw FileAccessError(ErrorCodes::FileOperationFailed, msg, old_path, err);
    }
#endif
}


bool File::copy(const std::string& origin_path, const std::string& target_path, bool overwrite_existing)
{
#if REALM_HAVE_STD_FILESYSTEM
    auto options = overwrite_existing ? std::filesystem::copy_options::overwrite_existing
                                      : std::filesystem::copy_options::skip_existing;
    return std::filesystem::copy_file(u8path(origin_path), u8path(target_path), options); // Throws
#else
#if REALM_PLATFORM_APPLE
    // Try to use clonefile and fall back to manual copying if it fails
    if (clonefile(origin_path.c_str(), target_path.c_str(), 0) == 0) {
        return true;
    }
    if (errno == EEXIST && !overwrite_existing) {
        return false;
    }
#endif

    File origin_file{origin_path, mode_Read}; // Throws
    File target_file;
    bool did_create = false;
    target_file.open(target_path, did_create); // Throws
    if (!did_create && !overwrite_existing) {
        return false;
    }

    size_t buffer_size = 4096;
    off_t pos = 0;
    auto buffer = std::make_unique<char[]>(buffer_size); // Throws
    for (;;) {
        size_t n = origin_file.read(pos, buffer.get(), buffer_size); // Throws
        target_file.write(pos, buffer.get(), n);                     // Throws
        pos += n;
        if (n < buffer_size)
            break;
    }

    return true;
#endif
}


bool File::is_same_file_static(FileDesc f1, FileDesc f2, const std::string& path1, const std::string& path2)
{
    return get_unique_id(f1, path1) == get_unique_id(f2, path2);
}

bool File::is_same_file(const File& f) const
{
    REALM_ASSERT_RELEASE(is_attached());
    REALM_ASSERT_RELEASE(f.is_attached());
    return is_same_file_static(m_fd, f.m_fd, m_path, f.m_path);
}

FileDesc File::dup_file_desc(FileDesc fd)
{
    FileDesc fd_duped;
#ifdef _WIN32
    if (!DuplicateHandle(GetCurrentProcess(), fd, GetCurrentProcess(), &fd_duped, 0, FALSE, DUPLICATE_SAME_ACCESS))
        throw std::system_error(GetLastError(), std::system_category(), "DuplicateHandle() failed");
#else
    fd_duped = dup(fd);

    if (fd_duped == -1) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::system_error(err, std::system_category(), "dup() failed");
    }
#endif // conditonal on _WIN32
    return fd_duped;
}

FileDesc File::get_descriptor() const
{
    return m_fd;
}

std::optional<File::UniqueID> File::get_unique_id(const std::string& path)
{
#ifdef _WIN32 // Windows version
    // CreateFile2 with creationDisposition OPEN_EXISTING will return a file handle only if the file exists
    // otherwise it will raise ERROR_FILE_NOT_FOUND. This call will never create a new file.
    WindowsFileHandleHolder fileHandle(::CreateFile2(u8path(path).c_str(), FILE_READ_ATTRIBUTES,
                                                     FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                                                     OPEN_EXISTING, nullptr));

    if (fileHandle == INVALID_HANDLE_VALUE) {
        if (GetLastError() == ERROR_FILE_NOT_FOUND) {
            return none;
        }
        throw SystemError(GetLastError(), "CreateFileW failed");
    }

    return get_unique_id(fileHandle, path);
#else // POSIX version
    struct stat statbuf;
    if (::stat(path.c_str(), &statbuf) == 0) {
        if (statbuf.st_size == 0) {
            // On exFAT systems the inode and device are not populated correctly until the file
            // has been allocated some space. The uid can also be reassigned if the file is
            // truncated to zero. This has led to bugs where a unique id returned here was
            // reused by different files. The following check ensures that this is not
            // happening anywhere else in future code.
            return none;
        }
        return File::UniqueID(statbuf.st_dev, statbuf.st_ino);
    }
    int err = errno; // Eliminate any risk of clobbering
    // File doesn't exist
    if (err == ENOENT)
        return none;
    throw SystemError(err, format_errno("fstat() failed: %1 for '%2'", err, path));
#endif
}

File::UniqueID File::get_unique_id(FileDesc file, const std::string& debug_path)
{
#ifdef _WIN32 // Windows version
    REALM_ASSERT(file != nullptr);
    File::UniqueID ret;
    if (GetFileInformationByHandleEx(file, FileIdInfo, &ret.id_info, sizeof(ret.id_info)) == 0) {
        throw std::system_error(GetLastError(), std::system_category(),
                                util::format("GetFileInformationByHandleEx() failed for '%1'", debug_path));
    }

    return ret;
#else // POSIX version
    REALM_ASSERT(file >= 0);
    struct stat statbuf;
    if (::fstat(file, &statbuf) == 0) {
        // On exFAT systems the inode and device are not populated correctly until the file
        // has been allocated some space. The uid can also be reassigned if the file is
        // truncated to zero. This has led to bugs where a unique id returned here was
        // reused by different files. The following check ensures that this is not
        // happening anywhere else in future code.
        if (statbuf.st_size == 0) {
            throw FileAccessError(
                ErrorCodes::FileOperationFailed,
                util::format("Attempt to get unique id on an empty file. This could be due to an external "
                             "process modifying Realm files. '%1'",
                             debug_path),
                debug_path);
        }
        return UniqueID(statbuf.st_dev, statbuf.st_ino);
    }
    throw std::system_error(errno, std::system_category(), util::format("fstat() failed for '%1'", debug_path));
#endif
}

std::string File::get_path() const
{
    return m_path;
}

std::string File::resolve(const std::string& path, const std::string& base_dir)
{
#if REALM_HAVE_STD_FILESYSTEM
    return (u8path(base_dir) / u8path(path)).lexically_normal().u8string();
#else
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
#endif
}

std::string File::parent_dir(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    return u8path(path).parent_path().u8string(); // Throws
#else
    auto is_sep = [](char c) -> bool {
        return c == '/' || c == '\\';
    };
    auto it = std::find_if(path.rbegin(), path.rend(), is_sep);
    while (it != path.rend() && is_sep(*it))
        ++it;
    return path.substr(0, path.rend() - it);
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
        m_encryption = std::make_unique<util::EncryptedFile>(key, m_fd);
    }
    else {
        m_encryption.reset();
    }
#else
    if (key) {
        throw LogicError(ErrorCodes::NotSupported, "Encryption not enabled");
    }
#endif
}

EncryptedFile* File::get_encryption() const noexcept
{
#if REALM_ENABLE_ENCRYPTION
    return m_encryption.get();
#else
    return nullptr;
#endif
}

File::MapBase::MapBase() noexcept = default;
File::MapBase::~MapBase() noexcept
{
    unmap();
}

File::MapBase::MapBase(MapBase&& other) noexcept
{
    *this = std::move(other);
}

File::MapBase& File::MapBase::operator=(MapBase&& other) noexcept
{
    REALM_ASSERT(this != &other);
    if (m_addr)
        unmap();
    m_addr = std::exchange(other.m_addr, nullptr);
    m_size = std::exchange(other.m_size, 0);
    m_access_mode = other.m_access_mode;
    m_reservation_size = std::exchange(other.m_reservation_size, 0);
    m_offset = std::exchange(other.m_offset, 0);
    m_fd = std::exchange(other.m_fd, invalid_fd);
#if REALM_ENABLE_ENCRYPTION
    m_encrypted_mapping = std::move(other.m_encrypted_mapping);
#endif
    return *this;
}

void File::MapBase::map(const File& f, AccessMode a, size_t size, SizeType offset, util::WriteObserver* observer)
{
    REALM_ASSERT(!m_addr);
#if REALM_ENABLE_ENCRYPTION
    m_addr = mmap({f.m_fd, a, f.m_encryption.get()}, size, offset, m_encrypted_mapping);
    if (observer && m_encrypted_mapping) {
        m_encrypted_mapping->set_observer(observer);
    }
#else
    std::unique_ptr<util::EncryptedFileMapping> dummy_encrypted_mapping;
    m_addr = mmap({f.m_fd, a, nullptr}, size, offset, dummy_encrypted_mapping);
    static_cast<void>(observer);
#endif
    m_size = m_reservation_size = size;
    m_fd = f.m_fd;
    m_offset = offset;
    m_access_mode = a;
}


void File::MapBase::unmap() noexcept
{
    if (!m_addr)
        return;
    REALM_ASSERT(m_reservation_size);
#if REALM_ENABLE_ENCRYPTION
    m_encrypted_mapping = nullptr;
#endif
    munmap(m_addr, m_reservation_size);
    m_addr = nullptr;
    m_size = 0;
    m_reservation_size = 0;
}

bool File::MapBase::try_reserve(const File& file, AccessMode a, size_t size, SizeType offset,
                                util::WriteObserver* observer)
{
#ifdef _WIN32
    static_cast<void>(observer);
    // unsupported for now
    return false;
#else
    void* addr = ::mmap(0, size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (addr == MAP_FAILED)
        return false;
    m_addr = addr;
    REALM_ASSERT(m_size == 0);
    m_access_mode = a;
    m_reservation_size = size;
    m_fd = file.get_descriptor();
    m_offset = offset;
#if REALM_ENABLE_ENCRYPTION
    if (file.m_encryption) {
        m_encrypted_mapping = util::reserve_mapping(addr, {m_fd, a, file.m_encryption.get()}, offset);
        if (observer) {
            m_encrypted_mapping->set_observer(observer);
        }
    }
#else
    static_cast<void>(observer);
#endif
#endif
    return true;
}

bool File::MapBase::try_extend_to(size_t size) noexcept
{
    if (size > m_reservation_size) {
        return false;
    }
#ifndef _WIN32
    char* extension_start_addr = (char*)m_addr + m_size;
    size_t extension_size = size - m_size;
    size_t extension_start_offset = m_offset + m_size;
#if REALM_ENABLE_ENCRYPTION
    if (m_encrypted_mapping) {
        void* got_addr = ::mmap(extension_start_addr, extension_size, PROT_READ | PROT_WRITE,
                                MAP_ANON | MAP_PRIVATE | MAP_FIXED, -1, 0);
        if (got_addr == MAP_FAILED)
            return false;
        REALM_ASSERT(got_addr == extension_start_addr);
        m_size = size;
        m_encrypted_mapping->extend_to(m_offset, size);
        return true;
    }
#endif
    try {
        void* got_addr =
            util::mmap_fixed(m_fd, extension_start_addr, extension_size, m_access_mode, extension_start_offset);
        if (got_addr == extension_start_addr) {
            m_size = size;
            return true;
        }
    }
    catch (...) {
        return false;
    }
#endif
    return false;
}

void File::MapBase::sync()
{
    REALM_ASSERT(m_addr);
#if REALM_ENABLE_ENCRYPTION
    if (m_encrypted_mapping) {
        m_encrypted_mapping->sync();
        return;
    }
#endif

    realm::util::msync(m_fd, m_addr, m_size);
}

void File::MapBase::flush(bool skip_validate)
{
    REALM_ASSERT(m_addr);
#if REALM_ENABLE_ENCRYPTION
    if (m_encrypted_mapping) {
        m_encrypted_mapping->flush(skip_validate);
    }
#else
    static_cast<void>(skip_validate);
#endif
}

std::time_t File::last_write_time(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    auto time = std::filesystem::last_write_time(u8path(path));

    using namespace std::chrono;
#if __cplusplus >= 202002L
    auto system_time = clock_cast<system_clock>(time);
#else
    auto system_time =
        time_point_cast<system_clock::duration>(time - decltype(time)::clock::now() + system_clock::now());
#endif
    return system_clock::to_time_t(system_time);
#else
    struct stat statbuf;
    if (::stat(path.c_str(), &statbuf) != 0) {
        throw SystemError(errno, "stat() failed");
    }
    return statbuf.st_mtime;
#endif
}

File::SizeType File::get_free_space(const std::string& path)
{
#if REALM_HAVE_STD_FILESYSTEM
    return std::filesystem::space(u8path(path)).available;
#else
    struct statvfs stat;
    if (statvfs(path.c_str(), &stat) != 0) {
        throw SystemError(errno, "statvfs() failed");
    }
    return SizeType(stat.f_bavail) * stat.f_bsize;
#endif
}

#if REALM_HAVE_STD_FILESYSTEM

DirScanner::DirScanner(const std::string& path, bool allow_missing)
{
    std::error_code ec;
    m_iterator = std::filesystem::directory_iterator(u8path(path), ec);
    if (ec && (ec != std::errc::no_such_file_or_directory || !allow_missing))
        throw std::filesystem::filesystem_error("directory_iterator::directory_iterator", u8path(path), ec);
}

DirScanner::~DirScanner() = default;

bool DirScanner::next(std::string& name)
{
    const std::filesystem::directory_iterator end;
    if (m_iterator == end)
        return false;
    name = m_iterator->path().filename().u8string();
    m_iterator++;
    return true;
}

#elif !defined(_WIN32)

DirScanner::DirScanner(const std::string& path, bool allow_missing)
{
    m_dirp = opendir(path.c_str());
    if (!m_dirp) {
        int err = errno; // Eliminate any risk of clobbering
        if (allow_missing && err == ENOENT)
            return;

        std::string msg = format_errno("opendir() failed: %1", err);
        switch (err) {
            case EACCES:
                throw FileAccessError(ErrorCodes::PermissionDenied, msg, path, err);
            case ENOENT:
                throw FileAccessError(ErrorCodes::FileNotFound, msg, path, err);
            default:
                throw FileAccessError(ErrorCodes::FileOperationFailed, msg, path, err);
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
#if !defined(__linux__) && !REALM_PLATFORM_APPLE && !REALM_WINDOWS && !REALM_UWP && !REALM_ANDROID &&                \
    !defined(__EMSCRIPTEN__)
#error "readdir() is not known to be thread-safe on this platform"
#endif

    if (!m_dirp)
        return false;

// Use readdir64 if it is available. This is necessary to support filesystems that return dirent fields that don't fit
// in 32-bits.
#if REALM_HAVE_READDIR64
#define REALM_READDIR(...) readdir64(__VA_ARGS__)
#else
#define REALM_READDIR(...) readdir(__VA_ARGS__)
#endif

    for (;;) {
        using DirentPtr = decltype(REALM_READDIR(m_dirp));
        DirentPtr dirent;
        do {
            // readdir() signals both errors and end-of-stream by returning a
            // null pointer. To distinguish between end-of-stream and errors,
            // the manpage recommends setting errno specifically to 0 before
            // calling it...
            errno = 0;

            dirent = REALM_READDIR(m_dirp);
        } while (!dirent && errno == EAGAIN);

        if (!dirent) {
            if (errno != 0)
                throw SystemError(errno, "readdir() failed");
            return false; // End of stream
        }
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
    throw NotImplemented();
}

DirScanner::~DirScanner() noexcept {}

bool DirScanner::next(std::string&)
{
    return false;
}

#endif

} // namespace realm::util
