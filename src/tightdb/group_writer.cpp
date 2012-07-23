#if !defined(_MSC_VER)
#include <unistd.h>
#include <sys/mman.h>
#endif

#include <tightdb/group_writer.hpp>
#include <tightdb/group.hpp>
#include <tightdb/alloc_slab.hpp>

using namespace tightdb;

// todo, test (int) cast
GroupWriter::GroupWriter(Group& group) :
    m_group(group), m_alloc(group.get_allocator()), m_len(m_alloc.GetFileLen()), m_data((char*)-1)
{
#if !defined(_MSC_VER)
    const int fd = m_alloc.GetFileDescriptor();
    m_data = (char*)mmap(0, m_len, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
#endif
}

GroupWriter::~GroupWriter()
{
#if !defined(_MSC_VER)
    if (m_data != (char*)-1)
        munmap(m_data, m_len);
#endif
}

bool GroupWriter::IsValid() const
{
    return m_data != (char*)-1;
}

void GroupWriter::SetVersions(size_t current, size_t readlock) {
    assert(readlock <= current);
    m_current_version  = current;
    m_readlock_version = readlock;
}

void GroupWriter::Commit()
{
    Array& top          = m_group.get_top_array();
    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.is_shared();
    assert(fpositions.Size() == flengths.Size());

    // Recursively write all changed arrays
    // (but not top yet, as it contains refs to free lists which are changing)
    const size_t n_pos = m_group.m_tableNames.Write(*this, true, true);
    const size_t t_pos = m_group.m_tables.Write(*this, true, true);

    // Add free space created during this commit to free lists
    const SlabAlloc::FreeSpace& freeSpace = m_group.get_allocator().GetFreespace();
    const size_t fcount = freeSpace.size();
    for (size_t i = 0; i < fcount; ++i) {
        SlabAlloc::FreeSpace::ConstCursor r = freeSpace[i];
        fpositions.add(r.ref);
        flengths.add(r.size);
        if (isShared) fversions.add(m_current_version);
    }
    //TODO: Consolidate free list

    // We now have a bit of an chicken-and-egg problem. We need to write our free
    // lists to the file, but the act of writing them will affect the amount
    // of free space, changing them.

    // To make sure we have room for top and free list we calculate the absolute
    // largest size they can get:
    // (64bit width + one possible ekstra entry per alloc and header)
    const size_t free_count = fpositions.Size() + 5;
    size_t top_max_size = (5 + 1) * 8;
    size_t flist_max_size = (free_count) * 8;

    // Reserve space for each block. We explicitly ask for a bigger space than
    // the block can occupy, so that we know that we will have to add the rest
    // space later
    const size_t top_pos = get_free_space(top_max_size, m_len);
    const size_t fp_pos  = get_free_space(flist_max_size, m_len);
    const size_t fl_pos  = get_free_space(flist_max_size, m_len);
    const size_t fv_pos  = isShared ? get_free_space(flist_max_size, m_len) : 0;

    // Update top and make sure that it is big enough to hold any position
    // the free lists can get
    top.Set(0, n_pos);
    top.Set(1, t_pos);
    top.Set(2, m_len); // just to expand width, values for free tracking set later

    // Add dummy values to freelists so we can get the final size.
    // The values are chosen to be so big that we are guaranteed that
    // the list will not expand width when the real values are set later.
    fpositions.add(m_len);
    fpositions.add(m_len);
    fpositions.add(m_len);
    fpositions.add(m_len);
    flengths.add(flist_max_size);
    flengths.add(flist_max_size);
    flengths.add(flist_max_size);
    flengths.add(flist_max_size);
    if (isShared) {
        fversions.add(0);
        fversions.add(0);
        fversions.add(0);
        fversions.add(0);
    }

    // Get final sizes
    const size_t top_size = top.GetByteSize(true);
    const size_t fp_size  = fpositions.GetByteSize(true);
    const size_t fl_size  = flengths.GetByteSize(true);
    const size_t fv_size  = isShared ? flengths.GetByteSize(true) : 0;

    // Set the correct values for rest space
    size_t fc = fpositions.Size()-1;
    if (isShared) {
        fpositions.Set(fc, fv_pos + fv_size);
        flengths.Set(fc--, flist_max_size - fv_size);
    }
    fpositions.Set(fc, fl_pos + fl_size);
    flengths.Set(fc--, flist_max_size - fl_size);
    fpositions.Set(fc, fp_pos + fp_size);
    flengths.Set(fc--, flist_max_size - fp_size);
    fpositions.Set(fc, top_pos + top_size);
    flengths.Set(fc, top_max_size - top_size);

    // Write free lists
    fpositions.WriteAt(fp_pos, *this);
    flengths.WriteAt(fl_pos, *this);
    if (isShared) fversions.WriteAt(fv_pos, *this);

    // Write top
    top.Set(2, fp_pos);
    top.Set(3, fl_pos);
    if (isShared) top.Set(4, fv_pos);
    else if (top.Size() == 5) top.Delete(4); // versions
    top.WriteAt(top_pos, *this);

    // Commit
    DoCommit(top_pos);

    // Clear old allocs
    // and remap if file size has changed
    SlabAlloc& alloc = m_group.get_allocator();
    alloc.FreeAll(m_len);

    // Recusively update refs in all active tables (columns, arrays..)
    m_group.update_refs(top_pos);
}

size_t GroupWriter::write(const char* p, size_t n) {
    // Get position of free space to write in (expanding file if needed)
    const size_t pos = get_free_space(n, m_len);
    assert((pos & 0x7) == 0); // Write position should always be 64bit aligned

    // Write the block
    char* const dest = m_data + pos;
    memcpy(dest, p, n);

    // return the position it was written
    return pos;
}

void GroupWriter::WriteAt(size_t pos, const char* p, size_t n) {
    char* const dest = m_data + pos;

    char* const mmap_end = m_data + m_len;
    char* const copy_end = dest + n;
    assert(copy_end <= mmap_end);
    static_cast<void>(mmap_end);
    static_cast<void>(copy_end);

    memcpy(dest, p, n);
}

void GroupWriter::DoCommit(uint64_t topPos)
{
    // In swap-only mode, we just use the file as backing for the shared
    // memory. So we never actually flush the data to disk (the OS may do
    // so for swapping though). Note that this means that the file on disk
    // may very likely be in an invalid state.
    //
    // In async mode, the file is persisted in regular intervals. This means
    // that the file on disk will always be in a valid state, but it may be
    // slightly out of sync with the latest changes.
    //if (isSwapOnly || isAsync) return;

#if !defined(_MSC_VER) // write persistence
    // Write data
    msync(m_data, m_len, MS_SYNC);

    // Write top pointer
    memcpy(m_data, (const char*)&topPos, 8);
    msync(m_data, m_len, MS_SYNC);
#endif
}

size_t GroupWriter::get_free_space(size_t len, size_t& filesize)
{
    assert((len & 0x7) == 0); // 64bit alignment
    assert((filesize & 0x7) == 0); // 64bit alignment

    Array& fpositions   = m_group.m_freePositions;
    Array& flengths     = m_group.m_freeLengths;
    Array& fversions    = m_group.m_freeVersions;
    const bool isShared = m_group.is_shared();

    // Do we have a free space we can reuse?
    const size_t count = flengths.Size();
    for (size_t i = 0; i < count; ++i) {
        const size_t free_len = flengths.Get(i);
        if (len <= free_len) {
            // Only blocks that are not occupied by current
            // readers are allowed to be used.
            if (isShared) {
                const size_t v = fversions.Get(i);
                if (v >= m_readlock_version) continue;
            }

            const size_t location = fpositions.Get(i);

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
    // we always expand megabytes at a time, both for
    // performance and to avoid excess fragmentation
    const size_t old_filesize = filesize;
    const size_t needed_size = old_filesize + len;
    while (filesize < needed_size) {
#ifdef _DEBUG
        // in debug, increase in small intervals to force overwriting
        filesize += 64;
#else
        filesize += 1024*1024;
#endif
    }

#if !defined(_MSC_VER) // write persistence
    // Extend the file
    const int fd = m_alloc.GetFileDescriptor();
    lseek(fd, filesize-1, SEEK_SET);
    ssize_t r = ::write(fd, "\0", 1);
    static_cast<void>(r); // FIXME: We should probably check for error here!
    fsync(fd);

    // ReMap
    //void* const p = mremap(m_shared, m_baseline, filesize); // linux only
    munmap(m_data, old_filesize);
    m_data = (char*)mmap(0, filesize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    assert(m_data != (char*)-1);
#endif

    // Add new free space
    const size_t end  = old_filesize + len;
    const size_t rest = filesize - end;
    fpositions.add(end);
    flengths.add(rest);
    if (isShared)
        fversions.add(0); // new space is always free for writing

    return old_filesize;
}


#ifdef _DEBUG

void GroupWriter::ZeroFreeSpace()
{
}

#endif
