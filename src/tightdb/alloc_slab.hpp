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
#ifndef TIGHTDB_ALLOC_SLAB_HPP
#define TIGHTDB_ALLOC_SLAB_HPP

#include <stdint.h> // unint8_t etc
#include <string>

#include <tightdb/file.hpp>
#include <tightdb/table_macros.hpp>

namespace tightdb {


// Pre-declarations
class Group;
class GroupWriter;


/// Thrown by Group and SharedGroup constructors if the specified file
/// (or memory buffer) does not appear to contain a valid TightDB
/// database.
struct InvalidDatabase: File::AccessError {
    InvalidDatabase(): File::AccessError("Invalid database") {}
};


/// The allocator that is used to manage the memory of a TightDB
/// group, i.e., a TightDB database.
///
/// Optionally, it can be attached to an pre-existing database (file
/// or memory buffer) which then becomes an immuatble part of the
/// managed memory.
///
/// To attach a slab allocator to a pre-existing database, call
/// attach_file() or attach_buffer(). To create a new database
/// in-memory, call attach_empty().
///
/// For efficiency, this allocator manages its memory as a set of
/// slabs.
class SlabAlloc: public Allocator {
public:
    /// Construct a slab allocator in the unattached state.
    SlabAlloc();

    ~SlabAlloc() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    /// Attach this allocator to the specified file.
    ///
    /// When used by free-standing Group instances, no concurrency is
    /// allowed. When used on behalf of SharedGroup, concurrency is
    /// allowed, but read_only and no_create must both be false in
    /// this case.
    ///
    /// It is an error to call this function on an attached
    /// allocator. Doing so will result in undefined behavor.
    ///
    /// \param is_shared Must be true if, and only if we are called on
    /// behalf of SharedGroup.
    ///
    /// \param read_only Open the file in read-only mode. This implies
    /// \a no_create.
    ///
    /// \param no_create Fail if the file does not already exist.
    ///
    /// \param bool skip_validate Skip validation of file header. In a
    /// set of overlapping SharedGroups, only the first one (the one
    /// that creates/initlializes the coordination file) may validate
    /// the header, otherwise it will result in a race condition.
    ///
    /// \throw File::AccessError
    void attach_file(const std::string& path, bool is_shared, bool read_only, bool no_create,
                     bool skip_validate);

    /// Attach this allocator to the specified memory buffer.
    ///
    /// It is an error to call this function on an attached
    /// allocator. Doing so will result in undefined behavor.
    ///
    /// \sa own_buffer()
    ///
    /// \throw InvalidDatabase
    void attach_buffer(char* data, std::size_t size);

    /// Attach this allocator to an empty buffer.
    ///
    /// It is an error to call this function on an attached
    /// allocator. Doing so will result in undefined behavor.
    void attach_empty();

    /// Detach from a previously attached file or buffer.
    ///
    /// This function does not reset free space tracking. To
    /// completely reset the allocator, you must also call
    /// reset_free_space_tracking().
    ///
    /// This method has no effect if the allocator is already in the
    /// detached state (idempotency).
    void detach() TIGHTDB_NOEXCEPT;

    class DetachGuard;

    /// If a memory buffer has been attached using attach_buffer(),
    /// mark it as owned by this slab allocator. Behaviour is
    /// undefined if this function is called on a detached allocator,
    /// one that is not attached using attach_buffer(), or one for
    /// which this function has already been called during the latest
    /// attachment.
    void own_buffer() TIGHTDB_NOEXCEPT;

    /// Returns true if, and only if this allocator is currently
    /// in the attached state.
    bool is_attached() const TIGHTDB_NOEXCEPT;

    /// Returns true if, and only if this allocator is currently in
    /// the attached state and attachment was not established using
    /// attach_empty().
    bool nonempty_attachment() const TIGHTDB_NOEXCEPT;

    /// Get the 'ref' corresponding to the current root node.
    ///
    /// It is an error to call this function on a detached allocator,
    /// or one that was attached using attach_empty(). Doing so will
    /// result in undefined behavior.
    ref_type get_top_ref() const TIGHTDB_NOEXCEPT;

    /// Get the size of the attached database file or buffer in number
    /// of bytes. This size is not affected by new allocations. After
    /// attachment, it can only be modified by a call to remap().
    ///
    /// It is an error to call this function on a detached allocator,
    /// or one that was attached using attach_empty(). Doing so will
    /// result in undefined behavior.
    std::size_t get_baseline() const TIGHTDB_NOEXCEPT;

    /// Get the total amount of managed memory. This is the sum of the
    /// size of the attached file and the sizes of each allocated
    /// slab. It includes any free space.
    ///
    /// It is an error to call this function on a detached
    /// allocator. Doing so will result in undefined behavior.
    std::size_t get_total_size() const;

    /// Mark all managed memory (except the attached file) as free
    /// space.
    void reset_free_space_tracking();

    /// Remap the attached file such that a prefix of the specified
    /// size becomes available in memory. If sucessfull,
    /// get_baseline() will return the specified new file size.
    ///
    /// It is an error to call this function on a detached allocator,
    /// or one that was not attached using attach_file(). Doing so
    /// will result in undefined behavior.
    ///
    /// \return True if, and only if the memory address of the first
    /// mapped byte has changed.
    bool remap(std::size_t file_size);

    MemRef alloc(std::size_t size) TIGHTDB_OVERRIDE;
    MemRef realloc_(ref_type, const char*, std::size_t old_size,
                    std::size_t new_size) TIGHTDB_OVERRIDE;
    // FIXME: It would be very nice if we could detect an invalid free operation in debug mode
    void free_(ref_type, const char*) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    char* translate(ref_type) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_read_only(ref_type) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

#ifdef TIGHTDB_DEBUG
    void enable_debug(bool enable) { m_debug_out = enable; }
    void Verify() const;
    bool is_all_free() const;
    void print() const;
#endif

private:
    enum AttachMode {
        attach_None,        // Nothing is attached
        attach_OwnedBuffer, // We own the buffer (m_data = 0 for empty buffer)
        attach_UsersBuffer, // We do not own the buffer
        attach_SharedFile,  // On behalf of SharedGroup
        attach_UnsharedFile // Not on behalf of SharedGroup
    };

    // Define internal tables
    TIGHTDB_TABLE_2(Slabs,
                    ref_end, Int, // One plus last ref targeting this slab
                    addr,    Int) // Memory pointer to this slab
    TIGHTDB_TABLE_2(FreeSpace,
                    ref,    Int,
                    size,   Int)

    static const char default_header[24];

    File m_file;
    char* m_data;
    AttachMode m_attach_mode;

    /// When set to true, the free lists are no longer
    /// up-to-date. This happens if free_() or
    /// reset_free_space_tracking() fails, presumably due to
    /// std::bad_alloc being thrown during updating of the free space
    /// list. In this this case, alloc(), realloc_(), and
    /// get_free_read_only() must throw. This member is deliberately
    /// placed after m_alloc_mode in the hope that it leads to less
    /// padding between members due to alignment requirements.
    bool m_free_space_invalid;

    std::size_t m_baseline; // Also size of memory mapped portion of database file
    Slabs m_slabs;
    FreeSpace m_free_space;
    FreeSpace m_free_read_only;

#ifdef TIGHTDB_DEBUG
    bool m_debug_out;
#endif

    /// Throws if free-lists are no longer valid.
    const FreeSpace& get_free_read_only() const;

    bool validate_buffer(const char* data, std::size_t len) const;

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* get_replication() const TIGHTDB_NOEXCEPT { return m_replication; }
    void set_replication(Replication* r) TIGHTDB_NOEXCEPT { m_replication = r; }
#endif

    friend class Group;
    friend class GroupWriter;
};


class SlabAlloc::DetachGuard {
public:
    DetachGuard(SlabAlloc& alloc) TIGHTDB_NOEXCEPT: m_alloc(&alloc) {}
    ~DetachGuard() TIGHTDB_NOEXCEPT;
    SlabAlloc* release() TIGHTDB_NOEXCEPT;
private:
    SlabAlloc* m_alloc;
};





// Implementation:

inline SlabAlloc::SlabAlloc(): m_attach_mode(attach_None), m_free_space_invalid(false)
{
#ifdef TIGHTDB_DEBUG
    m_debug_out = false;
#endif
}

inline void SlabAlloc::own_buffer() TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_attach_mode == attach_UsersBuffer);
    TIGHTDB_ASSERT(m_data);
    TIGHTDB_ASSERT(!m_file.is_attached());
    m_attach_mode = attach_OwnedBuffer;
}

inline bool SlabAlloc::is_attached() const TIGHTDB_NOEXCEPT
{
    return m_attach_mode != attach_None;
}

inline bool SlabAlloc::nonempty_attachment() const TIGHTDB_NOEXCEPT
{
    return is_attached() && m_data;
}

inline std::size_t SlabAlloc::get_baseline() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_data);
    return m_baseline;
}

inline SlabAlloc::DetachGuard::~DetachGuard() TIGHTDB_NOEXCEPT
{
    if (m_alloc)
        m_alloc->detach();
}

inline SlabAlloc* SlabAlloc::DetachGuard::release() TIGHTDB_NOEXCEPT
{
    SlabAlloc* alloc = m_alloc;
    m_alloc = 0;
    return alloc;
}

} // namespace tightdb

#endif // TIGHTDB_ALLOC_SLAB_HPP
