#include <algorithm>
#include <iostream>

#include <tightdb/safe_int_ops.hpp>
#include <tightdb/terminate.hpp>
#include <tightdb/array.hpp>
#include <tightdb/alloc_slab.hpp>

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
        m_free_space.print();
        TIGHTDB_TERMINATE("SlabAlloc detected a leak");
    }
#endif // TIGHTDB_DEBUG

    // Release all allocated memory
    for (size_t i = 0; i < m_slabs.size(); ++i) {
        delete[] reinterpret_cast<char*>(m_slabs[i].addr.get());
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
    TIGHTDB_ASSERT(0 < size);
    TIGHTDB_ASSERT((size & 0x7) == 0); // only allow sizes that are multiples of 8

    // Do we have a free space we can reuse?
    {
        size_t n = m_free_space.size();
        for (size_t i = 0; i < n; ++i) {
            FreeSpace::Cursor r = m_free_space[i];
            if (int(size) <= r.size) {
                ref_type ref = to_ref(r.ref);
                size_t rest = size_t(r.size - size);

                // Update free list
                if (rest == 0) {
                    m_free_space.remove(i);
                }
                else {
                    r.size = rest;
                    r.ref += unsigned(size);
                }

#ifdef TIGHTDB_DEBUG
                if (m_debug_out) {
                    cerr << "Alloc ref: " << ref << " size: " << size << "\n";
                }
#endif

                char* addr = translate(ref);
                return MemRef(addr, ref);
            }
        }
    }

    // Else, allocate new slab
    size_t new_size = ((size-1) | 255) + 1; // Round up to nearest multiple of 256
    size_t curr_ref_end;
    if (m_slabs.is_empty()) {
        curr_ref_end = m_baseline;
    }
    else {
        curr_ref_end = to_size_t(m_slabs.back().ref_end);
        // Make it at least as big as twice the previous slab
        size_t prev_ref_end = m_slabs.size() == 1 ? m_baseline :
            to_size_t(m_slabs.back(-2).ref_end);
        size_t min_size = 2 * (curr_ref_end - prev_ref_end);
        if (new_size < min_size)
            new_size = min_size;
    }

    // Allocate memory
    TIGHTDB_ASSERT(0 < new_size);
    char* slab = new char[new_size]; // Throws

    // Add to slab table
    size_t new_ref_end = curr_ref_end + new_size;
    // FIXME: intptr_t is not guaranteed to exists, not even in C++11
    uintptr_t addr = reinterpret_cast<uintptr_t>(slab);
    // FIXME: Dangerous conversions to int64_t here (undefined behavior according to C++11)
    m_slabs.add(int64_t(new_ref_end), int64_t(addr));

    // Update free list
    size_t unused = new_size - size;
    if (0 < unused) {
        size_t ref = curr_ref_end + size;
        // FIXME: Dangerous conversions to int64_t here (undefined behavior according to C++11)
        m_free_space.add(int64_t(ref), int64_t(unused));
    }

    char* addr_2 = slab;
    size_t ref = curr_ref_end;

#ifdef TIGHTDB_DEBUG
    if (m_debug_out) {
        cerr << "Alloc ref: " << ref << " size: " << size << "\n";
    }
#endif

    return MemRef(addr_2, ref);
}


// FIXME: We need to come up with a way to make free() a method that
// never throws. This is essential for exception safety in large parts
// of the TightDB API.
void SlabAlloc::free_(ref_type ref, const char* addr)
{
    TIGHTDB_ASSERT(translate(ref) == addr);
    // Free space in read only segment is tracked separately
    bool read_only = is_read_only(ref);
    FreeSpace& free_space = read_only ? m_free_read_only : m_free_space;

    // Get size from segment
    size_t size = read_only ? Array::get_byte_size_from_header(addr) :
        Array::get_capacity_from_header(addr);
    size_t ref_end = ref + size;
    size_t merged_with = npos;

#ifdef TIGHTDB_DEBUG
    if (m_debug_out) {
        cerr << "Free ref: " << ref << " size: " << size << "\n";
    }
#endif // TIGHTDB_DEBUG

    // Check if we can merge with start of free block
    {
        size_t n = free_space.column().ref.find_first(ref_end);
        if (n != not_found) {
            // No consolidation over slab borders
            if (m_slabs.column().ref_end.find_first(ref_end) == not_found) {
                free_space[n].ref = ref;
                free_space[n].size += size;
                merged_with = n;
            }
        }
    }

    // Check if we can merge with end of free block
    if (m_slabs.column().ref_end.find_first(ref) == not_found) { // avoid slab borders
        size_t n = free_space.size();
        for (size_t i = 0; i < n; ++i) {
            FreeSpace::Cursor c = free_space[i];

            ref_type end = to_ref(c.ref + c.size);
            if (ref == end) {
                if (merged_with != npos) {
                    c.size += free_space[merged_with].size;
                    free_space.remove(merged_with);
                }
                else {
                    c.size += size;
                }
                return;
            }
        }
    }

    // Else just add to freelist
    if (merged_with == npos)
        free_space.add(ref, size); // Throws
}


MemRef SlabAlloc::realloc_(size_t ref, const char* addr, size_t size)
{
    TIGHTDB_ASSERT(translate(ref) == addr);
    TIGHTDB_ASSERT(0 < size);
    TIGHTDB_ASSERT((size & 0x7) == 0); // only allow sizes that are multiples of 8

    //TODO: Check if we can extend current space

    // Allocate new space
    MemRef new_mem = alloc(size); // Throws

    /*if (doCopy) {*/  //TODO: allow realloc without copying
        // Get size of old segment
    size_t old_size = Array::get_capacity_from_header(addr);

    // Copy existing segment
    copy(addr, addr+old_size, new_mem.m_addr);

    // Add old segment to freelist
    free_(ref, addr); // FIXME: Unfortunately, this one can throw
    //}

#ifdef TIGHTDB_DEBUG
    if (m_debug_out) {
        cerr << "Realloc orig_ref: " << ref << " old_size: " << old_size << " "
            "new_ref: " << new_mem.m_ref << " new_size: " << size << "\n";
    }
#endif // TIGHTDB_DEBUG

    return new_mem;
}


char* SlabAlloc::translate(ref_type ref) const TIGHTDB_NOEXCEPT
{
    if (ref < m_baseline) return m_data + ref;
    else {
        size_t ndx = m_slabs.column().ref_end.upper_bound(ref);
        TIGHTDB_ASSERT(ndx != m_slabs.size());

        size_t offset = ndx == 0 ? m_baseline : to_size_t(m_slabs[ndx-1].ref_end);
        intptr_t addr = intptr_t(m_slabs[ndx].addr.get());
        return reinterpret_cast<char*>(addr) + (ref - offset);
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

    size_t initial_size = 1024 * 1024;

    // The size of a database file must not exceed what can be encoded
    // in std::size_t.
    size_t size;
    if (int_cast_with_overflow_detect(m_file.get_size(), size))
        goto invalid_database;

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
        if (read_only)
            goto invalid_database;

        m_file.write(default_header);

        // Pre-alloc initial space
        m_file.prealloc(0, initial_size);
        size = initial_size;
    }

    {
        File::Map<char> map(m_file, File::access_ReadOnly, size);

        // Verify the data structures
        if (!validate_buffer(map.get_addr(), size))
            goto invalid_database;

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
    int valid_part = file_header[16 + 7] & 0x1;

    // Byte 4 and 5 (depending on valid_part) in the info block is version
    uint8_t version = file_header[16 + 4 + valid_part];
    if (version != 0)
        return false; // unsupported version

    // Top_ref should always point within buffer
    const uint64_t* top_refs = reinterpret_cast<const uint64_t*>(data);
    ref_type ref = to_ref(top_refs[valid_part]);
    if (ref >= len)
        return false; // invalid top_ref

    return true;
}


ref_type SlabAlloc::get_top_ref() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_baseline > 0);

    // File header is 24 bytes, composed of three 64bit
    // blocks. The two first being top_refs (only one valid
    // at a time) and the last being the info block.
    const char* file_header = m_data;

    // Last bit in info block indicates which top_ref block
    // is valid
    int valid_ref = file_header[16 + 7] & 0x1;

    const uint64_t* top_refs = reinterpret_cast<uint64_t*>(m_data);
    ref_type ref = to_ref(top_refs[valid_ref]);
    TIGHTDB_ASSERT(ref < m_baseline);

    return ref;
}


size_t SlabAlloc::get_total_size() const
{
    return m_slabs.is_empty() ? m_baseline : to_size_t(m_slabs.back().ref_end);
}


void SlabAlloc::free_all()
{
    // Free all scratch space (done after all data has
    // been commited to persistent space)
    m_free_read_only.clear();
    m_free_space.clear();

    // Rebuild free list to include all slabs
    size_t ref = m_baseline;
    size_t n = m_slabs.size();
    for (size_t i = 0; i < n; ++i) {
        Slabs::Cursor c = m_slabs[i];
        size_t size = to_size_t(c.ref_end - ref);

        m_free_space.add(ref, size);

        ref = to_size_t(c.ref_end);
    }

    TIGHTDB_ASSERT(is_all_free());
}


void SlabAlloc::remap(size_t file_size)
{
    TIGHTDB_ASSERT(m_free_read_only.is_empty());
    TIGHTDB_ASSERT(m_slabs.size() == m_free_space.size());

    TIGHTDB_ASSERT(m_baseline <= file_size);
    TIGHTDB_ASSERT((file_size & 0x7) == 0); // 64bit alignment

    void* addr =
        m_file.remap(m_data, m_baseline, File::access_ReadOnly, file_size);

    m_data = static_cast<char*>(addr);
    m_baseline = file_size;

    // Rebase slabs and free list
    size_t new_offset = file_size;
    size_t n = m_slabs.size();
    for (size_t i = 0; i < n; ++i) {
        FreeSpace::Cursor c = m_free_space[i];
        c.ref = new_offset;
        new_offset = to_size_t(new_offset + c.size);

        m_slabs[i].ref_end = new_offset;
    }
}


#ifdef TIGHTDB_DEBUG

bool SlabAlloc::is_all_free() const
{
    if (m_free_space.size() != m_slabs.size())
        return false;

    // Verify that free space matches slabs
    size_t ref = m_baseline;
    for (size_t i = 0; i < m_slabs.size(); ++i) {
        Slabs::ConstCursor c = m_slabs[i];
        size_t size = to_ref(c.ref_end) - ref;

        size_t r = m_free_space.column().ref.find_first(ref);
        if (r == not_found)
            return false;
        if (size != size_t(m_free_space[r].size))
            return false;

        ref = to_ref(c.ref_end);
    }
    return true;
}


void SlabAlloc::Verify() const
{
    // Make sure that all free blocks fit within a slab
    size_t n = m_free_space.size();
    for (size_t i = 0; i < n; ++i) {
        FreeSpace::ConstCursor c = m_free_space[i];
        ref_type ref = to_ref(c.ref);

        size_t ndx = m_slabs.column().ref_end.upper_bound(ref);
        TIGHTDB_ASSERT(ndx != m_slabs.size());

        size_t slab_end = to_ref(m_slabs[ndx].ref_end);
        size_t free_end = ref + to_ref(c.size);

        TIGHTDB_ASSERT(free_end <= slab_end);
    }
}


void SlabAlloc::print() const
{
    size_t allocated_for_slabs = m_slabs.is_empty() ? 0 :
        size_t(m_slabs[m_slabs.size()-1].ref_end - m_baseline);

    size_t free = 0;
    for (size_t i = 0; i < m_free_space.size(); ++i) {
        free += to_ref(m_free_space[i].size);
    }

    size_t allocated = allocated_for_slabs - free;
    cout << "Attached: " << (m_data ? m_baseline : 0) << " Allocated: " << allocated << "\n";

    {
        size_t n = m_slabs.size();
        if (n > 0) {
            ref_type ref_end = ref_type(m_slabs[0].ref_end);
            void* addr = reinterpret_cast<void*>(uintptr_t(m_slabs[0].addr));
            cout << "Slabs: (" << m_baseline<<"->"<<(ref_end-1) << ", "
                "size="<<(ref_end-m_baseline)<<", addr=" << addr << ")";
            ref_type prev = ref_end;
            for (size_t i = 1; i < n; ++i) {
                ref_end = ref_type(m_slabs[i].ref_end);
                addr = reinterpret_cast<void*>(uintptr_t(m_slabs[i].addr));
                cout << ", (" << prev<<"->"<<(ref_end-1) << ", "
                    "size="<<(ref_end-prev)<<", addr=" << addr << ")";
                prev = ref_end;
            }
            cout << endl;
        }
    }

    {
        size_t n = m_free_space.size();
        if (n > 0) {
            ref_type ref = ref_type(m_free_space[0].ref);
            size_t size = size_t(m_free_space[0].size);
            cout << "FreeSpace: ("<<ref<<"->"<<(ref+size-1)<<", size="<<size<<")";
            for (size_t i = 1; i < n; ++i) {
                ref = ref_type(m_free_space[i].ref);
                size = size_t(m_free_space[i].size);
                cout << ", ("<<ref<<"->"<<(ref+size-1)<<", size="<<size<<")";
            }
            cout << endl;
        }
    }

    {
        size_t n = m_free_read_only.size();
        if (n > 0) {
            ref_type ref = ref_type(m_free_read_only[0].ref);
            size_t size = size_t(m_free_read_only[0].size);
            cout << "FreeSpace (ro): ("<<ref<<"->"<<(ref+size-1)<<", size="<<size<<")";
            for (size_t i = 1; i < n; ++i) {
                ref = ref_type(m_free_read_only[i].ref);
                size = size_t(m_free_read_only[i].size);
                cout << ", ("<<ref<<"->"<<(ref+size-1)<<", size="<<size<<")";
            }
            cout << endl;
        }
    }
}

#endif // TIGHTDB_DEBUG

} //namespace tightdb
