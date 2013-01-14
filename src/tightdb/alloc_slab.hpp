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

#include <string>

#ifdef _MSC_VER
#include <win32/stdint.h>
#else
#include <stdint.h> // unint8_t etc
#endif

#include <tightdb/exceptions.hpp>
#include <tightdb/table_macros.hpp>

namespace tightdb {

// Constants
const char* const default_header = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0T-DB\0\0\0\0";
const size_t header_len = 24;

// Pre-declarations
class Group;
class GroupWriter;

class SlabAlloc : public Allocator {
public:
    SlabAlloc();
    ~SlabAlloc();

    /// This function is used by free-standing Group instances as well
    /// as by groups that a managed by SharedGroup instances. When
    /// used by free-standing Group instances, no concurrency is
    /// allowed. When used by SharedGroup, concurrency is allowed, but
    /// read_only and no_create must both be false in this case.
    ///
    /// \param is_shared Must be true iff we are called on behalf of SharedGroup.
    ///
    /// \param read_only Open the file in read-only mode. This implies \a no_create.
    ///
    /// \param no_create Fail if the file does not already exist.
    ///
    /// \throw InvalidDatabase
    void map_file(const std::string& path, bool is_shared, bool read_only, bool no_create);

    /// \throw InvalidDatabase
    void set_buffer(char* data, size_t size, bool take_ownership);

    MemRef Alloc(size_t size);
    MemRef ReAlloc(size_t ref, void* p, size_t size);
    void   Free(size_t ref, void* p); // FIXME: It would be very nice if we could detect an invalid free operation in debug mode
    void*  Translate(size_t ref) const;

    bool   IsReadOnly(size_t ref) const;
    size_t GetTopRef() const;
    size_t GetTotalSize() const;

    bool   CanPersist() const;
    size_t GetFileLen() const {return m_baseline;}
    void   FreeAll(size_t filesize=(size_t)-1);
    bool   ReMap(size_t filesize);
    bool   RefreshMapping();

#ifndef _MSC_VER
    int    GetFileDescriptor() {return m_fd;}
#else
    void*  GetFileDescriptor() {return m_fd;}
#endif

#ifdef TIGHTDB_DEBUG
    void EnableDebug(bool enable) {m_debugOut = enable;}
    void Verify() const;
    bool IsAllFree() const;
    void Print() const;
#endif // TIGHTDB_DEBUG

protected:
    friend class Group;
    friend class GroupWriter;

    // Define internal tables
    TIGHTDB_TABLE_2(Slabs,
                    offset,     Int,
                    pointer,    Int)
    TIGHTDB_TABLE_2(FreeSpace,
                    ref,    Int,
                    size,   Int)

    const FreeSpace& GetFreespace() const {return m_freeReadOnly;}
    bool validate_buffer(const char* data, size_t len) const;

    // Member variables
    char*     m_shared;
    bool      m_owned;
    size_t    m_baseline;
    Slabs     m_slabs;
    FreeSpace m_freeSpace;
    FreeSpace m_freeReadOnly;

#ifndef _MSC_VER // POSIX
    int       m_fd;
#else // Windows
    //TODO: Something in a tightdb header won't let us include windows.h, so we can't use HANDLE
    void*     m_file;
    void*     m_map_file;
#endif

#ifdef TIGHTDB_DEBUG
    bool      m_debugOut;
#endif // TIGHTDB_DEBUG

private:
#ifdef TIGHTDB_ENABLE_REPLICATION
    void set_replication(Replication* r) { m_replication = r; }
#endif
};


} // namespace tightdb

#endif // TIGHTDB_ALLOC_SLAB_HPP
