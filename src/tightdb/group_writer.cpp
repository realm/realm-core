#include <algorithm>

#include <tightdb/group_writer.hpp>
#include <tightdb/group.hpp>
#include <tightdb/alloc_slab.hpp>

using namespace std;
using namespace tightdb;


// todo, test (int) cast
GroupWriter::GroupWriter(Group& group) :
    m_group(group), m_alloc(group.m_alloc), m_current_version(0)
{
    m_file_map.map(m_alloc.m_file, File::access_ReadWrite, m_alloc.get_attached_size());
}


void GroupWriter::set_versions(size_t current, size_t read_lock)
{
    TIGHTDB_ASSERT(read_lock <= current);
    m_current_version  = current;
    m_readlock_version = read_lock;
}


size_t GroupWriter::commit(bool do_sync)
{
    merge_free_space();

    Array& top           = m_group.m_top;
    Array& fpositions    = m_group.m_free_positions;
    Array& flengths      = m_group.m_free_lengths;
    Array& fversions     = m_group.m_free_versions;
    const bool is_shared = m_group.m_is_shared;
    TIGHTDB_ASSERT(fpositions.size() == flengths.size());
    TIGHTDB_ASSERT(!is_shared || fversions.size() == flengths.size());

    // Ensure that the freelist arrays are are themselves added to
    // (the allocator) free list
    fpositions.copy_on_write();
    flengths.copy_on_write();
    if (is_shared)
        fversions.copy_on_write();

    // Recursively write all changed arrays (but not top yet, as it
    // contains refs to free lists which are changing.) If free space
    // is available in the database file, we use it, but this does not
    // include space that has been release during the current
    // transaction (or since the last commit), as that would lead to
    // clobbering of the previous database version.
    bool recurse = true, persist = true;
    const size_t n_pos = m_group.m_table_names.write(*this, recurse, persist);
    const size_t t_pos = m_group.m_tables.write(*this, recurse, persist);

    // Add free space created during this transaction (or since last
    // commit) to free lists
    //
    // FIXME: This is bad, because it opens the posibility of
    // clobering the previous database version when we later write the
    // remaining arrays into the file
    const SlabAlloc::FreeSpace& free_space = m_group.m_alloc.get_free_read_only();
    const size_t fcount = free_space.size();

    for (size_t i = 0; i < fcount; ++i) {
        SlabAlloc::FreeSpace::ConstCursor r = free_space[i];
        add_free_space(to_size_t(r.ref), to_size_t(r.size), to_size_t(m_current_version));
    }

    // We now have a bit of an chicken-and-egg problem. We need to write our free
    // lists to the file, but the act of writing them will affect the amount
    // of free space, changing them.

    // To make sure we have room for top and free list we calculate the absolute
    // largest size they can get:
    // (64bit width + one possible ekstra entry per alloc and header)
    const size_t free_count = fpositions.size() + 5;
    const size_t top_max_size = (5 + 1) * 8;
    const size_t flist_max_size = free_count * 8;
    const size_t total_reserve = top_max_size + (flist_max_size * (is_shared ? 3 : 2));

    // Reserve space for each block. We explicitly ask for a bigger space than
    // the blocks can occupy, so that later when we know the real size, we can
    // adjust the segment size, without changing the width.
    const size_t res_ndx = reserve_free_space(total_reserve);
    const size_t res_pos = to_size_t(fpositions.get(res_ndx)); // top of reserved segments

    // Get final sizes of free lists
    const size_t fp_size  = fpositions.GetByteSize(true);
    const size_t fl_size  = flengths.GetByteSize(true);
    const size_t fv_size  = is_shared ? fversions.GetByteSize(true) : 0;

    // Calc write positions
    const size_t fl_pos = res_pos + fp_size;
    const size_t fv_pos = fl_pos + fl_size;
    const size_t top_pos = fv_pos + fv_size;

    // Update top to point to the reserved locations
    top.set(0, n_pos);
    top.set(1, t_pos);
    top.set(2, res_pos);
    top.set(3, fl_pos);
    if (is_shared)
        top.set(4, fv_pos);
    else if (top.size() == 5)
        top.erase(4); // versions

    // Get final sizes
    size_t top_size = top.GetByteSize(true);
    size_t end_pos = top_pos + top_size;
    size_t rest = total_reserve - (end_pos - res_pos);

    // Set the correct values for rest space
    fpositions.set(res_ndx, end_pos);
    flengths.set(res_ndx, rest);

    // Write free lists
    fpositions.write_at(res_pos, *this);
    flengths.write_at(fl_pos, *this);
    if (is_shared)
        fversions.write_at(fv_pos, *this);

    // Write top
    top.write_at(top_pos, *this);

    // In swap-only mode, we just use the file as backing for the shared
    // memory. So we never actually flush the data to disk (the OS may do
    // so for swapping though). Note that this means that the file on disk
    // may very likely be in an invalid state.
    if (do_sync)
        sync(top_pos);

    // Return top_pos so that it can be saved in lock file used
    // for coordination
    return top_pos;
}


size_t GroupWriter::write(const char* data, size_t size)
{
    // Get position of free space to write in (expanding file if needed)
    size_t pos = get_free_space(size);
    TIGHTDB_ASSERT((pos & 0x7) == 0); // Write position should always be 64bit aligned

    // Write the block
    char* dest = m_file_map.get_addr() + pos;
    copy(data, data+size, dest);

    // return the position it was written
    return pos;
}


void GroupWriter::write_at(size_t pos, const char* data, size_t size)
{
    char* dest = m_file_map.get_addr() + pos;

    char* mmap_end = m_file_map.get_addr() + m_file_map.get_size();
    char* copy_end = dest + size;
    TIGHTDB_ASSERT(copy_end <= mmap_end);
    static_cast<void>(mmap_end);
    static_cast<void>(copy_end);

    copy(data, data+size, dest);
}


void GroupWriter::sync(uint64_t top_pos)
{
    // Write data
    m_file_map.sync();

    // File header is 24 bytes, composed of three 64bit
    // blocks. The two first being top_refs (only one valid
    // at a time) and the last being the info block.
    char* file_header = m_file_map.get_addr();

    // Least significant bit in last byte of info block indicates
    // which top_ref block is valid
    int current_valid_ref = file_header[16+7] & 0x1;
    int new_valid_ref = current_valid_ref ^ 0x1;

    // Update top ref pointer
    uint64_t* top_refs = reinterpret_cast<uint64_t*>(file_header);
    top_refs[new_valid_ref] = top_pos;
    file_header[16+7] = char(new_valid_ref); // swap

    // Write new header to disk
    m_file_map.sync();
}


void GroupWriter::merge_free_space()
{
    Array& positions = m_group.m_free_positions;
    Array& lengths   = m_group.m_free_lengths;
    Array& versions  = m_group.m_free_versions;
    bool is_shared = m_group.m_is_shared;

    if (lengths.is_empty())
        return;

    size_t n = lengths.size() - 1;
    for (size_t i = 0; i < n; ++i) {
        size_t i2 = i+1;
        size_t pos1  = to_size_t(positions.get(i));
        size_t size1 = to_size_t(lengths.get(i));
        size_t pos2  = to_size_t(positions.get(i2));
        if (pos2 == pos1 + size1) {
            // If this is a shared db, we can only merge
            // segments where no part is currently in use
            if (is_shared) {
                size_t v1 = to_size_t(versions.get(i));
                if (v1 >= m_readlock_version)
                    continue;
                size_t v2 = to_size_t(versions.get(i2));
                if (v2 >= m_readlock_version)
                    continue;
            }

            // Merge
            size_t size2 = to_size_t(lengths.get(i2));
            lengths.set(i, size1 + size2);
            positions.erase(i2);
            lengths.erase(i2);
            if (is_shared)
                versions.erase(i2);

            --n;
            --i;
        }
    }
}


void GroupWriter::add_free_space(size_t pos, size_t size, size_t version)
{
    Array& positions = m_group.m_free_positions;
    Array& lengths   = m_group.m_free_lengths;
    Array& versions  = m_group.m_free_versions;
    bool is_shared = m_group.m_is_shared;

    // We always want to keep the list of free space in
    // sorted order (by position) to facilitate merge of
    // adjecendant segments. We can find the correct
    // insert postion by binary search
    size_t p = positions.lower_bound_int(pos);

    if (p == positions.size()) {
        positions.add(pos);
        lengths.add(size);
        if (is_shared)
            versions.add(version);
    }
    else {
        positions.insert(p, pos);
        lengths.insert(p, size);
        if (is_shared)
            versions.insert(p, version);
    }
}


size_t GroupWriter::reserve_free_space(size_t size)
{
    Array& positions    = m_group.m_free_positions;
    Array& lengths      = m_group.m_free_lengths;
    Array& versions     = m_group.m_free_versions;
    bool is_shared = m_group.m_is_shared;

    // Do we have a free space we can reuse?
    size_t ndx = not_found;
    size_t n = lengths.size();
    for (size_t i = 0; i < n; ++i) {
        size_t free_size = to_size_t(lengths.get(i));
        if (size <= free_size) {
            // Only blocks that are not occupied by current
            // readers are allowed to be used.
            if (is_shared) {
                size_t v = to_size_t(versions.get(i));
                if (v >= m_readlock_version)
                    continue;
            }

            // Match found!
            ndx = i;
            break;
        }
    }

    if (ndx == not_found) {
        // No free space, so we have to extend the file.
        ndx = extend_free_space(size);
    }

    // Split segment so we get exactly what was asked for
    size_t free_size = to_size_t(lengths.get(ndx));
    if (size != free_size) {
        lengths.set(ndx, size);

        size_t pos = to_size_t(positions.get(ndx)) + size;
        size_t rest = free_size - size;
        positions.insert(ndx+1, pos);
        lengths.insert(ndx+1, rest);
        if (is_shared)
            versions.insert(ndx+1, 0);
    }

    return ndx;
}


size_t GroupWriter::get_free_space(size_t size)
{
    TIGHTDB_ASSERT((size & 0x7) == 0); // 64-bit alignment
    TIGHTDB_ASSERT((m_file_map.get_size() & 0x7) == 0); // 64-bit alignment

    Array& positions = m_group.m_free_positions;
    Array& lengths   = m_group.m_free_lengths;
    Array& versions  = m_group.m_free_versions;
    bool is_shared = m_group.m_is_shared;

    size_t count = lengths.size();

    // Since we do a 'first fit' search, the top pieces are likely
    // to get smaller and smaller. So if we are looking for a bigger piece
    // we may find it faster by looking further down in the list.
    size_t start = size < 1024 ? 0 : count / 2;

    // Do we have a free space we can reuse?
    for (size_t i = start; i < count; ++i) {
        size_t free_size = to_size_t(lengths.get(i));
        if (size <= free_size) {
            // Only blocks that are not occupied by current
            // readers are allowed to be used.
            if (is_shared) {
                size_t v = to_size_t(versions.get(i));
                if (v >= m_readlock_version)
                    continue;
            }

            size_t pos = to_size_t(positions.get(i));

            // Update free list
            size_t rest = free_size - size;
            if (rest == 0) {
                positions.erase(i);
                lengths.erase(i);
                if (is_shared)
                    versions.erase(i);
            }
            else {
                lengths.set(i, rest);
                positions.set(i, pos + size);
            }

            return pos;
        }
    }

    // No free space, so we have to expand the file.
    size_t old_file_size = m_file_map.get_size();
    size_t ext_pos = extend_free_space(size);

    // Claim space from new extension
    size_t end  = old_file_size + size;
    size_t rest = m_file_map.get_size() - end;
    if (rest) {
        positions.set(ext_pos, end);
        lengths.set(ext_pos, rest);
    }
    else {
        positions.erase(ext_pos);
        lengths.erase(ext_pos);
        if (is_shared)
            versions.erase(ext_pos);
    }

    return old_file_size;
}


size_t GroupWriter::extend_free_space(size_t requested_size)
{
    Array& positions = m_group.m_free_positions;
    Array& lengths   = m_group.m_free_lengths;
    Array& versions  = m_group.m_free_versions;
    const bool is_shared = m_group.m_is_shared;

    // we always expand megabytes at a time, both for
    // performance and to avoid excess fragmentation
    const size_t megabyte = 1024 * 1024;
    const size_t old_file_size = m_file_map.get_size();
    const size_t needed_size = old_file_size + requested_size;
    const size_t rest = needed_size % megabyte;
    const size_t new_file_size = rest ? (needed_size + (megabyte - rest)) : needed_size;

    // Extend the file
    m_alloc.m_file.alloc(0, new_file_size); // Throws

    // FIXME: It is not clear what this call to sync() achieves. In
    // fact, is seems like it acheives nothing at all, because only if
    // the new top 'ref' is successfully instated will we need to see
    // a bigger file on disk. On the other hand, if it does acheive
    // something, what exactly is that?
    m_alloc.m_file.sync();

    m_file_map.remap(m_alloc.m_file, File::access_ReadWrite, new_file_size);

    size_t ext_size = new_file_size - old_file_size;

    // See if we can merge in new space
    if (!positions.is_empty()) {
        size_t last_ndx  = to_size_t(positions.size()-1);
        size_t last_size = to_size_t(lengths[last_ndx]);
        size_t end  = to_size_t(positions[last_ndx] + last_size);
        size_t ver  = to_size_t(is_shared ? versions[last_ndx] : 0);
        if (end == old_file_size && ver == 0) {
            lengths.set(last_ndx, last_size + ext_size);
            return last_ndx;
        }
    }

    // Else add new free space
    positions.add(old_file_size);
    lengths.add(ext_size);
    if (is_shared)
        versions.add(0); // new space is always free for writing

    return positions.size() - 1;
}


#ifdef TIGHTDB_DEBUG

void GroupWriter::dump()
{
    Array& fpositions = m_group.m_free_positions;
    Array& flengths   = m_group.m_free_lengths;
    Array& fversions  = m_group.m_free_versions;
    bool is_shared = m_group.m_is_shared;

    size_t count = flengths.size();
    cout << "count: " << count << ", m_size = " << m_file_map.get_size() << ", "
        "version >= " << m_readlock_version << "\n";
    if (!is_shared) {
        for (size_t i = 0; i < count; ++i) {
            cout << i << ": " << fpositions.get(i) << ", " << flengths.get(i) << "\n";
        }
    }
    else {
        for (size_t i = 0; i < count; ++i) {
            cout << i << ": " << fpositions.get(i) << ", " << flengths.get(i) << " - "
                "" << fversions.get(i) << "\n";
        }
    }
}

#endif
