#include <algorithm>
#include <iostream>

#include <tightdb/safe_int_ops.hpp>
#include <tightdb/alloc_slab.hpp>
#include <tightdb/array.hpp>

using namespace std;
using namespace tightdb;


namespace tightdb {

const char SlabAlloc::default_header[24] = {
        0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,
        'T', '-', 'D', 'B', 0,   0,   0,   0
};

SlabAlloc::~SlabAlloc()
{
#ifdef TIGHTDB_DEBUG
    if (!is_all_free()) {
        m_slabs.print();
        m_freeSpace.print();
        TIGHTDB_ASSERT(false);  // FIXME: Should this assert be here?
    }
#endif // TIGHTDB_DEBUG

    // Release all allocated memory
    for (size_t i = 0; i < m_slabs.size(); ++i) {
        delete[] reinterpret_cast<char*>(m_slabs[i].pointer.get());
    }

    // Release memory
    if (m_data) {
        switch (m_free_mode) {
            case free_Noop:                                     break;
            case free_Unalloc: ::free(m_data);                  break;
            case free_Unmap:   File::unmap(m_data, m_baseline); break;
        }
    }
}

MemRef SlabAlloc::alloc(size_t size)
{
    TIGHTDB_ASSERT((size & 0x7) == 0); // only allow sizes that are multibles of 8

    // Do we have a free space we can reuse?
    size_t count = m_freeSpace.size();
    for (size_t i = 0; i < count; ++i) {
        FreeSpace::Cursor r = m_freeSpace[i];
        if (r.size >= int(size)) {
            ref_type location = to_ref(r.ref);
            size_t rest = size_t(r.size - size);

            // Update free list
            if (rest == 0) m_freeSpace.remove(i);
            else {
                r.size = rest;
                r.ref += unsigned(size);
            }

#ifdef TIGHTDB_DEBUG
            if (m_debugOut) {
                cerr << "Alloc ref: " << location << " size: " << size << "\n";
            }
#endif // TIGHTDB_DEBUG

            char* pointer = translate(location);
            return MemRef(pointer, location);
        }
    }

    // Else, allocate new slab
    size_t multible = 256 * ((size / 256) + 1); // FIXME: Not an english word. Also, is this the intended rounding behavior?
    size_t slabsBack = m_slabs.is_empty() ? m_baseline : size_t(m_slabs.back().offset);
    size_t doubleLast = m_slabs.is_empty() ? 0 :
        (slabsBack - ((m_slabs.size() == 1) ? size_t(0) : size_t(m_slabs.back(-2).offset))) * 2;
    size_t newsize = multible > doubleLast ? multible : doubleLast;

    // Allocate memory
    TIGHTDB_ASSERT(0 < newsize);
    char* slab = new char[newsize]; // Throws

    // Add to slab table
    Slabs::Cursor s = m_slabs.add(); // FIXME: Use the immediate form add()
    s.offset = slabsBack + newsize;
    s.pointer = reinterpret_cast<int64_t>(slab);

    // Update free list
    size_t rest = newsize - size;
    FreeSpace::Cursor f = m_freeSpace.add(); // FIXME: Use the immediate form add()
    f.ref = slabsBack + size;
    f.size = rest;

#ifdef TIGHTDB_DEBUG
    if (m_debugOut) {
        cerr << "Alloc ref: " << slabsBack << " size: " << size << "\n";
    }
#endif // TIGHTDB_DEBUG

    return MemRef(slab, slabsBack);
}

// FIXME: We need to come up with a way to make free() a method that
// never throws. This is essential for exception safety in large parts
// of the TightDB API.
void SlabAlloc::free(ref_type ref, const char* p)
{
    // Free space in read only segment is tracked separately
    bool read_only = is_read_only(ref);
    FreeSpace& free_space = read_only ? m_freeReadOnly : m_freeSpace;

    // Get size from segment
    size_t size = read_only ?
        Array::get_byte_size_from_header(static_cast<const char*>(p)) :
        Array::get_capacity_from_header(static_cast<const char*>(p));
    size_t ref_end = ref + size;
    bool is_merged = false;

#ifdef TIGHTDB_DEBUG
    if (m_debugOut) {
        cerr << "Free ref: " << ref << " size: " << size << "\n";
    }
#endif // TIGHTDB_DEBUG

    // Check if we can merge with start of free block
    size_t n = free_space.column().ref.find_first(ref_end);
    if (n != size_t(-1)) {
        // No consolidation over slab borders
        if (m_slabs.column().offset.find_first(ref_end) == size_t(-1)) {
            free_space[n].ref = ref;
            free_space[n].size += size;
            is_merged = true;
        }
    }

    // Check if we can merge with end of free block
    if (m_slabs.column().offset.find_first(ref) == size_t(-1)) { // avoid slab borders
        size_t count = free_space.size();
        for (size_t i = 0; i < count; ++i) {
            FreeSpace::Cursor c = free_space[i];

            ref_type end = to_ref(c.ref + c.size);
            if (ref == end) {
                if (is_merged) {
                    c.size += free_space[n].size;
                    free_space.remove(n);
                }
                else c.size += size;

                return;
            }
        }
    }

    // Else just add to freelist
    if (!is_merged) free_space.add(ref, size);
}

MemRef SlabAlloc::realloc(size_t ref, const char* p, size_t size)
{
    TIGHTDB_ASSERT((size & 0x7) == 0); // only allow sizes that are multibles of 8

    //TODO: Check if we can extend current space

    // Allocate new space
    MemRef space = alloc(size); // Throws

    /*if (doCopy) {*/  //TODO: allow realloc without copying
        // Get size of old segment
    size_t old_size = Array::get_capacity_from_header(p);

    // Copy existing segment
    copy(p, p+old_size, space.pointer);

    // Add old segment to freelist
    free(ref, p); // FIXME: Unfortunately, this one can throw
    //}

#ifdef TIGHTDB_DEBUG
    if (m_debugOut) {
        cerr << "Realloc origref: " << ref << " oldsize: " << old_size << " "
            "newref: " << space.ref << " newsize: " << size << "\n";
    }
#endif // TIGHTDB_DEBUG

    return space;
}

char* SlabAlloc::translate(ref_type ref) const TIGHTDB_NOEXCEPT
{
    if (ref < m_baseline) return m_data + ref;
    else {
        size_t ndx = m_slabs.column().offset.find_pos(ref);
        TIGHTDB_ASSERT(ndx != not_found);

        size_t offset = ndx ? to_size_t(m_slabs[ndx-1].offset) : m_baseline;
        return reinterpret_cast<char*>(m_slabs[ndx].pointer.get()) + (ref - offset);
    }
}

bool SlabAlloc::is_read_only(ref_type ref) const TIGHTDB_NOEXCEPT
{
    return ref < m_baseline;
}


void SlabAlloc::attach_file(const string& path, bool is_shared, bool read_only, bool no_create)
{
    // When 'read_only' is true, this function will throw
    // InvalidDatabase if the file exists already but is empty. This
    // can happen if another process is currently creating it. Note
    // however, that it is only legal for multiple processes to access
    // a database file concurrently if it is done via a SharedGroup,
    // and in that case 'read_only' can never be true.
    TIGHTDB_ASSERT(!(is_shared && read_only));
    static_cast<void>(is_shared);

    File::AccessMode access = read_only ? File::access_ReadOnly : File::access_ReadWrite;
    File::CreateMode create = read_only || no_create ? File::create_Never : File::create_Auto;
    m_file.open(path.c_str(), access, create, 0);
    File::CloseGuard fcg(m_file);

    const size_t initial_size = 1024 * 1024;

    // The size of a database file must not exceed what can be encoded
    // in std::size_t.
    size_t size;
    if (int_cast_with_overflow_detect(m_file.get_size(), size)) goto invalid_database;

    // FIXME: This initialization procedure does not provide
    // sufficient robustness given that processes may be abruptly
    // terminated at any point in time. In unshared mode, we must be
    // able to reliably detect any invalid file as long as its
    // invalidity is due to a terminated serialization process
    // (e.g. due to a power failure). In shared mode we can guarantee
    // that if the database file was ever valid, then it will remain
    // valid, however, there is no way we can ensure that
    // initialization of an empty database file succeeds. Thus, in
    // shared mode we must be able to reliably distiguish between
    // three cases when opening a database file: A) It was never
    // properly initialized. In this case we should simply
    // reinitialize it. B) It looks corrupt. In this case we throw an
    // exception. C) It looks good. In this case we proceede as
    // normal.
    if (size == 0) {
        if (read_only) goto invalid_database;

        m_file.write(default_header);

        // Pre-alloc initial space
        m_file.alloc(0, initial_size);
        size = initial_size;
    }

    {
        File::Map<char> map(m_file, File::access_ReadOnly, size);

        // Verify the data structures
        if (!validate_buffer(map.get_addr(), size)) goto invalid_database;

        m_data      = map.release();
        m_baseline  = size;
        m_free_mode = free_Unmap;
    }

    fcg.release(); // Do not close
    return;

  invalid_database:
    throw InvalidDatabase();
}


void SlabAlloc::attach_buffer(char* data, size_t size, bool take_ownership)
{
    // Verify the data structures
    if (!validate_buffer(data, size)) throw InvalidDatabase();

    m_data      = data;
    m_baseline  = size;
    m_free_mode = take_ownership ? free_Unalloc : free_Noop;
}


bool SlabAlloc::validate_buffer(const char* data, size_t len) const
{
    // Verify that data is 64bit aligned
    if (len < sizeof default_header || (len & 0x7) != 0)
        return false;

    // File header is 24 bytes, composed of three 64bit
    // blocks. The two first being top_refs (only one valid
    // at a time) and the last being the info block.
    const char* file_header = data;

    // First four bytes of info block is file format id
    if (!(file_header[16] == 'T' &&
          file_header[17] == '-' &&
          file_header[18] == 'D' &&
          file_header[19] == 'B'))
        return false; // Not a tightdb file

    // Last bit in info block indicates which top_ref block is valid
    size_t valid_part = file_header[23] & 0x1;

    // Byte 4 and 5 (depending on valid_part) in the info block is version
    uint8_t version = file_header[valid_part ? 21 : 20];
    if (version != 0)
        return false; // unsupported version

    // Top_ref should always point within buffer
    const uint64_t* top_refs = reinterpret_cast<const uint64_t*>(data);
    ref_type ref = to_ref(top_refs[valid_part]);
    if (ref >= len)
        return false; // invalid top_ref

    return true;
}

// FIXME: We should come up with a better name than 'CanPersist'
bool SlabAlloc::CanPersist() const
{
    return m_data != 0;
}

ref_type SlabAlloc::get_top_ref() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_data && m_baseline > 0);

    // File header is 24 bytes, composed of three 64bit
    // blocks. The two first being top_refs (only one valid
    // at a time) and the last being the info block.
    const char* file_header = m_data;

    // Last bit in info block indicates which top_ref block
    // is valid
    size_t valid_ref = file_header[23] & 0x1;

    const uint64_t* top_refs = reinterpret_cast<uint64_t*>(m_data);
    ref_type ref = to_ref(top_refs[valid_ref]);
    TIGHTDB_ASSERT(ref < m_baseline);

    return ref;
}

size_t SlabAlloc::get_total_size() const
{
    if (m_slabs.is_empty()) {
        return m_baseline;
    }
    else {
        return to_size_t(m_slabs.back().offset);
    }
}

void SlabAlloc::free_all(size_t file_size)
{
    TIGHTDB_ASSERT(file_size >= m_baseline);
    TIGHTDB_ASSERT((file_size & 0x7) == 0 || file_size == size_t(-1)); // 64-bit alignment

    // Free all scratch space (done after all data has
    // been commited to persistent space)
    m_freeReadOnly.clear();
    m_freeSpace.clear();

    // Rebuild free list to include all slabs
    size_t ref = m_baseline;
    size_t count = m_slabs.size();
    for (size_t i = 0; i < count; ++i) {
        Slabs::Cursor c = m_slabs[i];
        size_t size = to_size_t(c.offset - ref);

        m_freeSpace.add(ref, size);

        ref = to_size_t(c.offset);
    }

    // If the file size have changed, we need to remap the readonly buffer
    if (file_size != size_t(-1))
        remap(file_size);

    TIGHTDB_ASSERT(is_all_free());
}

bool SlabAlloc::remap(size_t file_size)
{
    TIGHTDB_ASSERT(m_freeReadOnly.is_empty());
    TIGHTDB_ASSERT(m_slabs.size() == m_freeSpace.size());

    // We only need to remap the readonly buffer
    // if the file size have changed.
    if (file_size == m_baseline) return false;

    TIGHTDB_ASSERT(m_baseline <= file_size);
    TIGHTDB_ASSERT((file_size & 0x7) == 0); // 64bit alignment

    void* addr =
        m_file.remap(m_data, m_baseline, File::access_ReadOnly, file_size);

    m_data = static_cast<char*>(addr);
    m_baseline = file_size;

    // Rebase slabs and free list
    size_t new_offset = file_size;
    size_t count = m_slabs.size();
    for (size_t i = 0; i < count; ++i) {
        FreeSpace::Cursor c = m_freeSpace[i];
        c.ref = new_offset;
        new_offset = to_size_t(new_offset + c.size);

        m_slabs[i].offset = new_offset;
    }

    return true;
}

#ifdef TIGHTDB_DEBUG

bool SlabAlloc::is_all_free() const
{
    if (m_freeSpace.size() != m_slabs.size()) return false;

    // Verify that free space matches slabs
    size_t ref = m_baseline;
    for (size_t i = 0; i < m_slabs.size(); ++i) {
        Slabs::ConstCursor c = m_slabs[i];
        size_t size = to_ref(c.offset) - ref;

        size_t r = m_freeSpace.column().ref.find_first(ref);
        if (r == size_t(-1)) return false;
        if (size != size_t(m_freeSpace[r].size)) return false;

        ref = to_ref(c.offset);
    }
    return true;
}

void SlabAlloc::Verify() const
{
    // Make sure that all free blocks fit within a slab
    size_t count = m_freeSpace.size();
    for (size_t i = 0; i < count; ++i) {
        FreeSpace::ConstCursor c = m_freeSpace[i];
        ref_type ref = to_ref(c.ref);

        size_t ndx = m_slabs.column().offset.find_pos(ref);
        TIGHTDB_ASSERT(ndx != size_t(-1));

        size_t slab_end = to_ref(m_slabs[ndx].offset);
        size_t free_end = ref + to_ref(c.size);

        TIGHTDB_ASSERT(free_end <= slab_end);
    }
}

void SlabAlloc::Print() const
{
    size_t allocated = m_slabs.is_empty() ? 0 : size_t(m_slabs[m_slabs.size()-1].offset);

    size_t free = 0;
    for (size_t i = 0; i < m_freeSpace.size(); ++i) {
        free += to_ref(m_freeSpace[i].size);
    }

    cout << "Base: " << (m_data ? m_baseline : 0) << " Allocated: " << (allocated - free) << "\n";
}

#endif // TIGHTDB_DEBUG

} //namespace tightdb
