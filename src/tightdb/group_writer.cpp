#include <algorithm>

#include <tightdb/group_writer.hpp>
#include <tightdb/group.hpp>
#include <tightdb/alloc_slab.hpp>

using namespace std;
using namespace tightdb;

// todo, test (int) cast
GroupWriter::GroupWriter(Group& group, bool doPersist) :
    m_group(group), m_alloc(group.get_allocator()), m_doPersist(doPersist)
{
    m_file_map.map(m_alloc.m_file, File::access_ReadWrite, m_alloc.GetFileLen());
}

void GroupWriter::SetVersions(size_t current, size_t readlock)
{
    TIGHTDB_ASSERT(readlock <= current);
    m_current_version  = current;
    m_readlock_version = readlock;
}

size_t GroupWriter::Commit()
{
    merge_free_space();

    Array& top          = m_group.get_top_array();
    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.m_is_shared;
    TIGHTDB_ASSERT(fpositions.Size() == flengths.Size());
    TIGHTDB_ASSERT(!isShared || fversions.Size() == flengths.Size());

    // Ensure that the freelist arrays are are themselves added to
    // (the allocator) free list
    fpositions.CopyOnWrite();
    flengths.CopyOnWrite();
    if (isShared) fversions.CopyOnWrite();

    // Recursively write all changed arrays
    // (but not top yet, as it contains refs to free lists which are changing)
    const size_t n_pos = m_group.m_tableNames.Write(*this, true, true);
    const size_t t_pos = m_group.m_tables.Write(*this, true, true);

    // Add free space created during this commit to free lists
    const SlabAlloc::FreeSpace& freeSpace = m_group.get_allocator().GetFreespace();
    const size_t fcount = freeSpace.size();

    for (size_t i = 0; i < fcount; ++i) {
        SlabAlloc::FreeSpace::ConstCursor r = freeSpace[i];
        add_free_space(r.ref, r.size, m_current_version);
    }

    // We now have a bit of an chicken-and-egg problem. We need to write our free
    // lists to the file, but the act of writing them will affect the amount
    // of free space, changing them.

    // To make sure we have room for top and free list we calculate the absolute
    // largest size they can get:
    // (64bit width + one possible ekstra entry per alloc and header)
    const size_t free_count = fpositions.Size() + 5;
    const size_t top_max_size = (5 + 1) * 8;
    const size_t flist_max_size = (free_count) * 8;
    const size_t total_reserve = top_max_size + (flist_max_size * (isShared ? 3 : 2));

    // Reserve space for each block. We explicitly ask for a bigger space than
    // the blocks can occupy, so that later when we know the real size, we can
    // adjust the segment size, without changing the width.
    const size_t res_ndx = reserve_free_space(total_reserve, m_file_map.get_size());
    const size_t res_pos = fpositions.GetAsSizeT(res_ndx); // top of reserved segments

    // Get final sizes of free lists
    const size_t fp_size  = fpositions.GetByteSize(true);
    const size_t fl_size  = flengths.GetByteSize(true);
    const size_t fv_size  = isShared ? fversions.GetByteSize(true) : 0;

    // Calc write positions
    const size_t fl_pos = res_pos + fp_size;
    const size_t fv_pos = fl_pos + fl_size;
    const size_t top_pos = fv_pos + fv_size;

    // Update top to point to the reserved locations
    top.Set(0, n_pos);
    top.Set(1, t_pos);
    top.Set(2, res_pos);
    top.Set(3, fl_pos);
    if (isShared) top.Set(4, fv_pos);
    else if (top.Size() == 5) top.Delete(4); // versions

    // Get final sizes
    const size_t top_size = top.GetByteSize(true);
    const size_t end_pos = top_pos + top_size;
    const size_t rest = total_reserve - (end_pos - res_pos);

    // Set the correct values for rest space
    fpositions.Set(res_ndx, end_pos);
    flengths.Set(res_ndx, rest);

    // Write free lists
    fpositions.WriteAt(res_pos, *this);
    flengths.WriteAt(fl_pos, *this);
    if (isShared) fversions.WriteAt(fv_pos, *this);

    // Write top
    top.WriteAt(top_pos, *this);

    // In swap-only mode, we just use the file as backing for the shared
    // memory. So we never actually flush the data to disk (the OS may do
    // so for swapping though). Note that this means that the file on disk
    // may very likely be in an invalid state.
    if (m_doPersist)
        DoCommit(top_pos);

    // Clear old allocs
    // and remap if file size has changed
    SlabAlloc& alloc = m_group.get_allocator();
    alloc.FreeAll(m_file_map.get_size());

    // Return top_pos so that it can be saved in lock file used
    // for coordination
    return top_pos;
}

size_t GroupWriter::write(const char* p, size_t n)
{
    // Get position of free space to write in (expanding file if needed)
    const size_t pos = get_free_space(n);
    TIGHTDB_ASSERT((pos & 0x7) == 0); // Write position should always be 64bit aligned

    // Write the block
    char* const dest = m_file_map.get_addr() + pos;
    copy(p, p+n, dest);

    // return the position it was written
    return pos;
}

void GroupWriter::WriteAt(size_t pos, const char* p, size_t n)
{
    char* const dest = m_file_map.get_addr() + pos;

    char* const mmap_end = m_file_map.get_addr() + m_file_map.get_size();
    char* const copy_end = dest + n;
    TIGHTDB_ASSERT(copy_end <= mmap_end);
    static_cast<void>(mmap_end);
    static_cast<void>(copy_end);

    copy(p, p+n, dest);
}

void GroupWriter::DoCommit(uint64_t topPos)
{
    // Write data
    m_file_map.sync();

    // File header is 24 bytes, composed of three 64bit
    // blocks. The two first being top_refs (only one valid
    // at a time) and the last being the info block.
    char* const file_header = m_file_map.get_addr();

    // Last byte in info block indicates which top_ref block
    // is valid
    const size_t current_valid_ref = file_header[23] & 0x1;
    const size_t new_valid_ref = current_valid_ref == 0 ? 1 : 0;

    // Update top ref pointer
    uint64_t* const top_refs = reinterpret_cast<uint64_t*>(m_file_map.get_addr());
    top_refs[new_valid_ref] = topPos;
    file_header[23] = new_valid_ref; // swap

    // Write new header to disk
    m_file_map.sync();
}

void GroupWriter::merge_free_space()
{
    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.m_is_shared;

    if (flengths.is_empty())
        return;

    size_t count = flengths.Size()-1;
    for (size_t i = 0; i < count; ++i) {
        const size_t i2 = i+1;
        const size_t pos1 = fpositions.GetAsSizeT(i);
        const size_t len1 = flengths.GetAsSizeT(i);
        const size_t pos2 = fpositions.GetAsSizeT(i2);

        if (pos2 == pos1 + len1) {
            // If this is a shared db, we can only merge
            // segments where no part is currently in use
            if (isShared) {
                const size_t v1 = fversions.GetAsSizeT(i);
                if (v1 >= m_readlock_version)
                    continue;
                const size_t v2 = fversions.GetAsSizeT(i2);
                if (v2 >= m_readlock_version)
                    continue;
            }

            // Merge
            const size_t len2 = flengths.GetAsSizeT(i2);
            flengths.Set(i, len1 + len2);
            fpositions.Delete(i2);
            flengths.Delete(i2);
            if (isShared) fversions.Delete(i2);

            --count;
            --i;
        }
    }
}

void GroupWriter::add_free_space(size_t pos, size_t len, size_t version)
{
    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.m_is_shared;

    // We always want to keep the list of free space in
    // sorted order (by position) to facilitate merge of
    // adjecendant segments. We can find the correct
    // insert postion by binary search
    const size_t p = fpositions.FindPos2(pos);

    if (p == not_found) {
        fpositions.add(pos);
        flengths.add(len);
        if (isShared) fversions.add(version);
    }
    else {
        fpositions.Insert(p, pos);
        flengths.Insert(p, len);
        if (isShared) fversions.Insert(p, version);
    }
}

size_t GroupWriter::reserve_free_space(size_t len, size_t start)
{
    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.m_is_shared;

    // Do we have a free space we can reuse?
    const size_t count = flengths.Size();
    size_t ndx = not_found;
    for (size_t i = start; i < count; ++i) {
        const size_t free_len = flengths.GetAsSizeT(i);
        if (len <= free_len) {
            // Only blocks that are not occupied by current
            // readers are allowed to be used.
            if (isShared) {
                const size_t v = fversions.GetAsSizeT(i);
                if (v >= m_readlock_version) continue;
            }

            // Match found!
            ndx = i;
            break;
        }
    }

    if (ndx == not_found) {
        // No free space, so we have to extend the file.
        ndx = extend_free_space(len);
    }

    // Split segment so we get exactly what was asked for
    const size_t free_len = flengths.GetAsSizeT(ndx);
    if (len != free_len) {
        flengths.Set(ndx, len);

        const size_t pos = fpositions.GetAsSizeT(ndx) + len;
        const size_t rest = free_len - len;
        fpositions.Insert(ndx+1, pos);
        flengths.Insert(ndx+1, rest);
        if (isShared) fversions.Insert(ndx+1, 0);
    }

    return ndx;
}

size_t GroupWriter::get_free_space(size_t len)
{
    TIGHTDB_ASSERT((len & 0x7) == 0); // 64bit alignment
    TIGHTDB_ASSERT((m_file_map.get_size() & 0x7) == 0); // 64bit alignment

    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.m_is_shared;

    const size_t count = flengths.Size();

    // Since we do a 'first fit' search, the top pieces are likely
    // to get smaller and smaller. So if we are looking for a bigger piece
    // we may find it faster by looking further down in the list.
    const size_t start = len < 1024 ? 0 : count / 2;

    // Do we have a free space we can reuse?
    for (size_t i = start; i < count; ++i) {
        const size_t free_len = flengths.GetAsSizeT(i);
        if (len <= free_len) {
            // Only blocks that are not occupied by current
            // readers are allowed to be used.
            if (isShared) {
                const size_t v = fversions.GetAsSizeT(i);
                if (v >= m_readlock_version) continue;
            }

            const size_t location = fpositions.GetAsSizeT(i);

            // Update free list
            const size_t rest = free_len - len;
            if (rest == 0) {
                fpositions.Delete(i);
                flengths.Delete(i);
                if (isShared)
                    fversions.Delete(i);
            }
            else {
                flengths.Set(i, rest);
                fpositions.Set(i, location + len);
            }

            return location;
        }
    }

    // No free space, so we have to expand the file.
    const size_t old_filesize = m_file_map.get_size();
    const size_t ext_pos = extend_free_space(len);

    // Claim space from new extension
    const size_t end  = old_filesize + len;
    const size_t rest = m_file_map.get_size() - end;
    if (rest) {
        fpositions.Set(ext_pos, end);
        flengths.Set(ext_pos, rest);
    }
    else {
        fpositions.Delete(ext_pos);
        flengths.Delete(ext_pos);
        if (isShared) fversions.Delete(ext_pos);
    }

    return old_filesize;
}

size_t GroupWriter::extend_free_space(size_t len)
{
    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.m_is_shared;

    // we always expand megabytes at a time, both for
    // performance and to avoid excess fragmentation
    const size_t megabyte = 1024 * 1024;
    const size_t old_filesize = m_file_map.get_size();
    const size_t needed_size = old_filesize + len;
    const size_t rest = needed_size % megabyte;
    const size_t new_filesize = rest ? (needed_size + (megabyte - rest)) : needed_size;

    // Extend the file
    m_alloc.m_file.alloc(0, new_filesize);
    m_alloc.m_file.sync(); // FIXME: What does this call to  sync() achieve? Robustness with respect to abrupt process termination?

    m_file_map.remap(m_alloc.m_file, File::access_ReadWrite, new_filesize);

    const size_t ext_len = new_filesize - old_filesize;

    // See if we can merge in new space
    if (!fpositions.is_empty()) {
        const size_t last_ndx = fpositions.Size()-1;
        const size_t last_len = flengths[last_ndx];
        const size_t end  = fpositions[last_ndx] + last_len;
        const size_t ver  = isShared ? fversions[last_ndx] : 0;
        if (end == old_filesize && ver == 0) {
            flengths.Set(last_ndx, last_len + ext_len);
            return last_ndx;
        }
    }

    // Else add new free space
    fpositions.add(old_filesize);
    flengths.add(ext_len);
    if (isShared) fversions.add(0); // new space is always free for writing

    return fpositions.Size()-1;
}


#ifdef TIGHTDB_DEBUG

void GroupWriter::dump()
{
    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.m_is_shared;

    const size_t count = flengths.Size();
    cout << "count: " << count << ", m_len = " << m_file_map.get_size() << ", version >= " << m_readlock_version << "\n";
    if (!isShared) {
        for (size_t i = 0; i < count; ++i) {
            cout << i << ": " << fpositions.Get(i) << ", " << flengths.Get(i) << "\n";
        }
    }
    else {
        for (size_t i = 0; i < count; ++i) {
            cout << i << ": " << fpositions.Get(i) << ", " << flengths.Get(i) << " - " << fversions.Get(i) << "\n";
        }
    }
}

void GroupWriter::ZeroFreeSpace()
{
}

#endif
