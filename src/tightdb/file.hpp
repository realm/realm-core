/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_FILE_HPP
#define TIGHTDB_FILE_HPP

#include <cstddef>
#include <cstdio>
#include <stdexcept>
#include <string>

#include <sys/types.h>

#include <tightdb/config.h>

namespace tightdb {


/// Create a new unique directory for temporary files. The absolute
/// path to the new directory is returned without a trailing slash.
std::string create_temp_dir();


/// This class provides a RAII abstraction over the concept of a file
/// descriptor (or file handle).
///
/// Locks are automatically and immediately released when the File
/// instance is closed.
///
/// You can use CloseGuard and UnlockGuard to acheive exception-safe
/// closing or unlocking prior to the File instance being detroyed.
class File {
public:
    enum Mode {
        mode_Read,   // access_ReadOnly,  create_Never
        mode_Update, // access_ReadWrite, create_Auto
        mode_Write,  // access_ReadWrite, create_Auto, flag_Trunc
        mode_Append  // access_ReadWrite, create_Auto, flag_Append
    };

    /// See open(const std::string&, Mode).
    File(const std::string& path, Mode = mode_Read);

    /// Create an instance that does not initially refer to an open
    /// file.
    File() TIGHTDB_NOEXCEPT;

    ~File() TIGHTDB_NOEXCEPT;

    /// Calling this method on an instance that already refers to an
    /// open file has undefined behavior.
    ///
    /// \throw OpenError If the file could not be opened. If the
    /// reason corresponds to one of the exception types that are
    /// derived from OpenError, the derived exception type is thrown
    /// (as long as the underlying system provides the information to
    /// unambiguously distinguish that particular reason).
    void open(const std::string& path, Mode = mode_Read);

    /// This method is idempotent, that is, it is valid to call it
    /// regardless of whether this instance currently refers to an
    /// open file.
    void close() TIGHTDB_NOEXCEPT;

    enum AccessMode {
        access_ReadOnly,
        access_ReadWrite
    };

    enum CreateMode {
        create_Auto,  ///< Create the file if it does not already exist.
        create_Never, ///< Fail if the file does not already exist.
        create_Must   ///< Fail if the file already exists.
    };

    enum {
        flag_Trunc  = 1, ///< Truncate the file if it already exists.
        flag_Append = 2  ///< Move the end of file before each write.
    };

    /// See open(const std::string&, Mode). Specifying access_ReadOnly
    /// together with a create mode that is not create_Never, or with
    /// a \a flags argument that is not zero, results in undefined
    /// behavior. Specifying flag_Trunc together with create_Must
    /// results in undefined behavior.
    void open(const std::string& path, AccessMode, CreateMode, int flags);

    void write(const char* data, std::size_t size);

    void write(const std::string& s) { write(s.data(), s.size()); }

    template<std::size_t N> void write(const char (&data)[N]) { write(data, N); }

    /// Calling this method on an instance that does not refer to an
    /// open file has undefined behavior.
    off_t get_size() const;

    /// If this causes the file to grow, then the new section will
    /// have undefined contents. Setting the size with this method
    /// does not necessarily allocate space on the target device. If
    /// you want to ensure allocation, call alloc(). Calling this
    /// method on an instance that does not refer to an open file has
    /// undefined behavior. Calling this method on a file that is
    /// opened in read-only mode, is an error.
    void resize(off_t);

    /// Allocate space on the target device for the specified region
    /// of the file. If the region extends beyond the current end of
    /// the file, the file is resized accordingly. Calling this method
    /// on an instance that does not refer to an open file has
    /// undefined behavior. Calling this method on a file that is
    /// opened in read-only mode, is an error.
    void alloc(off_t offset, std::size_t size);

    /// Reposition the read/write offset in the file.
    void seek(off_t);

    /// Flush in-kernel buffers to disk. This blocks the caller until
    /// the synchronization operation is complete.
    void sync();

    /// The STDIO file stream is opened in binary mode. Its existence
    /// will be independent of the existence of this File instance. It
    /// is an error to specify an access mode that is less restrictive
    /// than what was passed to open() or the File constructor.
    std::FILE* open_stdio_file(AccessMode);

    /// Place an exclusive lock on this file. This blocks the caller
    /// until all other locks have been released. Calling this method
    /// on an instance that does not refer to an open file, or on an
    /// instance that is already locked has undefined behavior. Locks
    /// acquired on distinct File instances have fully recursive
    /// behavior, even if they are acquired in the same process (or
    /// thread) and refer to the same underlying file.
    void lock_exclusive();

    /// Non-blocking version of lock_exclusive(). Returns true iff it
    /// succeeds.
    bool try_lock_exclusive();

    /// Place an shared lock on this file. This blocks the caller
    /// until all other exclusive locks have been released. Calling
    /// this method on an instance that does not refer to an open
    /// file, or on an instance that is already locked has undefined
    /// behavior. Locks acquired on distinct File instances have fully
    /// recursive behavior, even if they are acquired in the same
    /// process (or thread) and refer to the same underlying file.
    void lock_shared();

    /// Release a previously acquired lock on this file. This method
    /// is idempotent.
    void unlock() TIGHTDB_NOEXCEPT;

    /// Map this file into memory. The file is mapped as shared
    /// memory. This allows two processes to interact under exatly the
    /// same rules as applies to the interaction via regular memory of
    /// multiple threads inside a single process.
    ///
    /// This File instance does not need to remain in existence after
    /// the mapping is established. Calling this method on an instance
    /// that does not refer to an open file has undefined
    /// behavior. Specifying access_ReadWrite for a file that is
    /// opened in read-only mode, is an error.
    void* map(AccessMode, std::size_t size) const;

    /// The same as unmap(old_addr, old_size) followed by map(a,
    /// new_size), but more efficient on some systems.
    ///
    /// The old address range must have been acquired by a call to
    /// map() or remap() on this File instance, the specified access
    /// mode must be the same as the one specified previously, and
    /// this File instance must not have been reopend in the
    /// meantime. Failing to adhere to these rules will result in
    /// undefined behavior.
    ///
    /// IMPORTANT: If this operation fails, the old address range will
    /// have been unmapped.
    void* remap(void* old_addr, std::size_t old_size, AccessMode a, std::size_t new_size) const;

    /// Unmap the specified address range which must have been
    /// previously returned by map().
    static void unmap(void* addr, std::size_t size) TIGHTDB_NOEXCEPT;

    /// Flush in-kernel buffers to disk. This blocks the caller until
    /// the synchronization operation is complete. The specified
    /// address range must be one that was previously returned by
    /// map().
    static void sync_map(void* addr, std::size_t size);

    template<class> class Map;

    class CloseGuard;
    class UnlockGuard;
    class UnmapGuard;

    struct OpenError: std::runtime_error {
        OpenError(const std::string& msg): std::runtime_error(msg) {}
    };

    /// Thrown if the user does not have permission to open or create
    /// the specified file in the specified access mode.
    struct PermissionDenied: OpenError {
        PermissionDenied(const std::string& msg): OpenError(msg) {}
    };

    /// Thrown if the directory part of the specified path was not
    /// found, or create_Never was specified and the file did no
    /// exist.
    struct NotFound: OpenError {
        NotFound(const std::string& msg): OpenError(msg) {}
    };

    /// Thrown if create_Always was specified and the file did already
    /// exist.
    struct Exists: OpenError {
        Exists(const std::string& msg): OpenError(msg) {}
    };

private:
#ifdef _WIN32
    void* m_handle;
#else
    int m_fd;
#endif

    struct MapBase {
        void* m_addr;
        std::size_t m_size;

        void map(const File&, AccessMode, std::size_t size);
        void remap(const File&, AccessMode, std::size_t size);
        void unmap() TIGHTDB_NOEXCEPT;
        void sync();
    };
};



/// This class provides a RAII abstraction over the concept of a
/// memory mapped file.
///
/// The Map instance makes no reference to the File instance from
/// which it was created, and that File instance may be destroyed
/// before the Map instance is destroyed.
///
/// You can use UnmapGuard to acheive exception-safe unmapping prior
/// to the Map instance being detroyed.
template<class T> class File::Map: MapBase {
public:
    /// See map().
    Map(const File&, AccessMode = access_ReadOnly, std::size_t size = sizeof (T));

    /// Create an instance that does not initially refer to a file
    /// mapping.
    Map() TIGHTDB_NOEXCEPT;

    ~Map() TIGHTDB_NOEXCEPT;

    /// See File::map(). Calling this method on a Map instance that
    /// already refers to a file mapping has undefined behavior. The
    /// returned pointer is the same as what will subsequently be
    /// returned by get_addr().
    T* map(const File&, AccessMode = access_ReadOnly, std::size_t size = sizeof (T));

    /// See File::remap(). Calling this method on a Map instance that
    /// does not currently refer to a file mapping has undefined
    /// behavior. The returned pointer is the same as what will
    /// subsequently be returned by get_addr().
    T* remap(const File&, AccessMode = access_ReadOnly, std::size_t size = sizeof (T));

    /// See File::unmap(). This method is idempotent, that is, it is
    /// valid to call it regardless of whether this instance refers to
    /// a file mapping or not.
    void unmap() TIGHTDB_NOEXCEPT;

    /// See File::sync_map(). Calling this method on an instance that
    /// does not currently refer to a file mapping, has undefined
    /// behavior.
    void sync();

    /// Returns a pointer to the beginning of the mapped file, or null
    /// if this instance does not currently refer to a file mapping.
    T* get_addr() const TIGHTDB_NOEXCEPT;

    /// Returns the size of the mapped region, or zero if this
    /// instance does not currently refer to a file mapping. When this
    /// instance refers to a file mapping, the returned value will
    /// always be identical to the size passed to the constructor or
    /// to map().
    std::size_t get_size() const TIGHTDB_NOEXCEPT;

    /// Release the current mapping from this Map instance. The
    /// mapping may then be released later by a call to File::unmap().
    T* release() TIGHTDB_NOEXCEPT;
};



class File::CloseGuard {
public:
    CloseGuard(File& f) TIGHTDB_NOEXCEPT: m_file(&f) {}
    ~CloseGuard()  TIGHTDB_NOEXCEPT { if (m_file) m_file->close(); }
    void release() TIGHTDB_NOEXCEPT { m_file = 0; }
private:
    File* m_file;
};


class File::UnlockGuard {
public:
    UnlockGuard(File& f) TIGHTDB_NOEXCEPT: m_file(&f) {}
    ~UnlockGuard()  TIGHTDB_NOEXCEPT { if (m_file) m_file->unlock(); }
    void release() TIGHTDB_NOEXCEPT { m_file = 0; }
private:
    File* m_file;
};


class File::UnmapGuard {
public:
    template<class T> UnmapGuard(Map<T>& m) TIGHTDB_NOEXCEPT: m_map(&m) {}
    ~UnmapGuard()  TIGHTDB_NOEXCEPT { if (m_map) m_map->unmap(); }
    void release() TIGHTDB_NOEXCEPT { m_map = 0; }
private:
    MapBase* m_map;
};






// Implementation:

inline File::File(const std::string& path, Mode m)
{
    open(path, m);
}

inline File::File() TIGHTDB_NOEXCEPT
{
#ifdef _WIN32
    m_handle = 0;
#else
    m_fd = -1;
#endif
}

inline File::~File() TIGHTDB_NOEXCEPT
{
    close();
}

inline void File::open(const std::string& path, Mode m)
{
    AccessMode a = access_ReadWrite;
    CreateMode c = create_Auto;
    int flags = 0;
    switch (m) {
        case mode_Read:   a = access_ReadOnly; c = create_Never; break;
        case mode_Update:                                        break;
        case mode_Write:  flags = flag_Trunc;                    break;
        case mode_Append: flags = flag_Append;                   break;
    }
    open(path, a, c, flags);
}

inline void File::MapBase::map(const File& f, AccessMode a, std::size_t size)
{
    m_addr = f.map(a, size);
    m_size = size;
}

inline void File::MapBase::remap(const File& f, AccessMode a, std::size_t size)
{
    void* addr = m_addr;
    m_addr = 0; // Because if File::remap fails, the old mapping will have been destroyed
    m_addr = f.remap(addr, m_size, a, size);
    m_size = size;
}

inline void File::MapBase::unmap() TIGHTDB_NOEXCEPT
{
    if (!m_addr) return;
    File::unmap(m_addr, m_size);
    m_addr = 0;
}

inline void File::MapBase::sync()
{
    File::sync_map(m_addr, m_size);
}

template<class T>
inline File::Map<T>::Map(const File& f, AccessMode a, std::size_t size)
{
    map(f, a, size);
}

template<class T> inline File::Map<T>::Map() TIGHTDB_NOEXCEPT
{
    m_addr = 0;
}

template<class T> inline File::Map<T>::~Map() TIGHTDB_NOEXCEPT
{
    unmap();
}

template<class T> inline T* File::Map<T>::map(const File& f, AccessMode a, std::size_t size)
{
    MapBase::map(f, a, size);
    return static_cast<T*>(m_addr);
}

template<class T> inline T* File::Map<T>::remap(const File& f, AccessMode a, std::size_t size)
{
    MapBase::remap(f, a, size);
    return static_cast<T*>(m_addr);
}

template<class T> inline void File::Map<T>::unmap() TIGHTDB_NOEXCEPT
{
    MapBase::unmap();
}

template<class T> inline void File::Map<T>::sync()
{
    MapBase::sync();
}

template<class T> inline T* File::Map<T>::get_addr() const TIGHTDB_NOEXCEPT
{
    return static_cast<T*>(m_addr);
}

template<class T> inline std::size_t File::Map<T>::get_size() const TIGHTDB_NOEXCEPT
{
    return m_addr ? m_size : 0;
}

template<class T> inline T* File::Map<T>::release() TIGHTDB_NOEXCEPT
{
    T* const addr = static_cast<T*>(m_addr);
    m_addr = 0;
    return addr;
}


} // namespace tightdb

#endif // TIGHTDB_FILE_HPP
