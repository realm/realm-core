#ifndef __TDB_ALLOC_SLAB__
#define __TDB_ALLOC_SLAB__

#include "tightdb.hpp"

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif

namespace tightdb {

class SlabAlloc : public Allocator {
public:
    SlabAlloc();
    ~SlabAlloc();

    bool SetShared(const char* path, bool readOnly=true);
    bool SetSharedBuffer(const char* buffer, size_t len);

    MemRef Alloc(size_t size);
    MemRef ReAlloc(size_t ref, void* p, size_t size);
    void Free(size_t ref, void* p);
    void* Translate(size_t ref) const;

    bool IsReadOnly(size_t ref) const;
    size_t GetTopRef() const;
    size_t GetTotalSize() const;

    bool CanPersist() const;
    size_t GetFileLen() const {return m_baseline;}
    void FreeAll(size_t filesize);

#ifndef _MSC_VER
    int GetFileDescriptor() {return m_fd;}
#else
    void* GetFileDescriptor() {return m_fd;}
#endif

#ifdef _DEBUG
    void EnableDebug(bool enable) {m_debugOut = enable;}
    void Verify() const;
    bool IsAllFree() const;
    void Print() const;
#endif //_DEBUG

private:
    // Define internal tables
    TIGHTDB_TABLE_2(Slabs,
                    offset,     Int,
                    pointer,    Int)
    TIGHTDB_TABLE_2(FreeSpace,
                    ref,    Int,
                    size,   Int)

    // Member variables
    char*     m_shared;
    bool      m_owned;
    size_t    m_baseline;
    Slabs     m_slabs;
    FreeSpace m_freeSpace;

#ifndef _MSC_VER
    int       m_fd;
#else
    //TODO: Something in a tightdb header won't let us include windows.h, so we can't use HANDLE
    void*     m_mapfile;
    void*     m_fd;
#endif

#ifdef _DEBUG
    bool      m_debugOut;
#endif //_DEBUG
};

}

#endif //__TDB_ALLOC_SLAB__
