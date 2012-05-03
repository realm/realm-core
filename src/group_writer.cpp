#include "group_writer.hpp"
#include "group.hpp"
#include "alloc_slab.hpp"

using namespace tightdb;

// todo, test (int) cast
GroupWriter::GroupWriter(Group& group) :
    m_group(group), m_alloc(group.get_allocator()), m_len(m_alloc.GetFileLen()), m_fd((int)m_alloc.GetFileDescriptor())
{
}

bool GroupWriter::IsValid() const
{
    return m_fd != 0;
}

void GroupWriter::Commit()
{
    Array& top        = m_group.get_top_array();
    Array& fpositions = m_group.m_freePositions;
    Array& flengths   = m_group.m_freeLengths;
    
    // Recursively write all changed arrays
    // (but not top yet, as it contains refs to free lists which are changing)
    const size_t n_pos = m_group.m_tableNames.Write(*this, true, true);
    const size_t t_pos = m_group.m_tables.Write(*this, true, true);
    
    // To make sure we have room for top and free list we calculate to absolute
    // largest size they can get:
    // (64bit width + one extra item for each free list, headers and a little rest)
    const size_t max_block = (top.Size() + fpositions.Size() + flengths.Size() + 6) * 8;
    
    // Ensure that we have room for max_block in file
    m_group.get_free_space(max_block, m_len, true);
    
    // Update top and make sure that it is big enough to hold any position
    // the free lists can get
    const size_t max_pos = m_len + max_block;
    top.Set(0, n_pos);
    top.Set(1, t_pos);
    top.Set(2, max_pos);
    top.Set(3, max_pos);
    
    // Reserve space for top
    const size_t top_size = top.GetByteSize();
    const size_t top_pos = m_group.get_free_space(top_size, m_len);
    
    //TODO: Consolidate free list
    
    // The positions list could potentially expand width when we
    // reserve space, so we pre-expand it avoid this
    fpositions.add(max_pos);
    fpositions.Delete(fpositions.Size()-1);
    
    // Reserve space for free lists
    // (ensure rest to avoid list changing size)
    const size_t fp_size = fpositions.GetByteSize();
    const size_t fl_size = flengths.GetByteSize();
    const size_t fp_pos = m_group.get_free_space(fp_size, m_len, false, true);
    const size_t fl_pos = m_group.get_free_space(fl_size, m_len, false, true);
    
    // Write free lists
    fpositions.WriteAt(fp_pos, *this);
    flengths.WriteAt(fl_pos, *this);
    
    // Write top
    top.Set(2, fp_pos);
    top.Set(3, fl_pos);
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
#if !defined(_MSC_VER) // write persistence
    // Get position of free space to write in (expanding file if needed)
    const size_t pos = m_group.get_free_space(n, m_len);
    
    // Write the block
    lseek(m_fd, pos, SEEK_SET);
    ::write(m_fd, p, n);
    
    // return the position it was written
    return pos;
#endif
    return 0;
}

void GroupWriter::WriteAt(size_t pos, const char* p, size_t n) {
#if !defined(_MSC_VER) // write persistence
    lseek(m_fd, pos, SEEK_SET);
    ::write(m_fd, p, n);
#endif
}

void GroupWriter::DoCommit(uint64_t topPos)
{
#if !defined(_MSC_VER) // write persistence
    fsync(m_fd);
    lseek(m_fd, 0, SEEK_SET);
    ::write(m_fd, (const char*)&topPos, 8);
    fsync(m_fd); // Could be fdatasync on Linux
#endif
}
