#include <cerrno>
#include <new>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <fstream>

#include <tightdb/util/memory_stream.hpp>
#include <tightdb/util/thread.hpp>
#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/group_writer.hpp>
#include <tightdb/group.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

using namespace std;
using namespace tightdb;
using namespace tightdb::util;


namespace {

class Initialization {
public:
    Initialization()
    {
        tightdb::cpuid_init();
    }
};

Initialization initialization;

} // anonymous namespace



void Group::open(const string& file_path, OpenMode mode)
{
    TIGHTDB_ASSERT(!is_attached());
    bool is_shared = false;
    bool read_only = mode == mode_ReadOnly;
    bool no_create = mode == mode_ReadWriteNoCreate;
    bool skip_validate = false;
    ref_type top_ref = m_alloc.attach_file(file_path, is_shared, read_only, no_create,
                                           skip_validate); // Throws
    SlabAlloc::DetachGuard dg(m_alloc);
    m_alloc.reset_free_space_tracking(); // Throws
    if (top_ref == 0) {
        // Attaching to a newly created file
        bool add_free_versions = false;
        create(add_free_versions); // Throws
    }
    else {
        // Attaching to a pre-existing database
        init_from_ref(top_ref);
    }
    dg.release(); // Do not detach allocator from file
}


void Group::open(BinaryData buffer, bool take_ownership)
{
    TIGHTDB_ASSERT(!is_attached());
    TIGHTDB_ASSERT(buffer.data());
    // FIXME: Why do we have to pass a const-unqualified data pointer
    // to SlabAlloc::attach_buffer()? It seems unnecessary given that
    // the data is going to become the immutable part of its managed
    // memory.
    char* data = const_cast<char*>(buffer.data());
    ref_type top_ref = m_alloc.attach_buffer(data, buffer.size()); // Throws
    SlabAlloc::DetachGuard dg(m_alloc);
    m_alloc.reset_free_space_tracking(); // Throws
    if (top_ref == 0) {
        bool add_free_versions = false;
        create(add_free_versions); // Throws
    }
    else {
        init_from_ref(top_ref);
    }
    dg.release(); // Do not detach allocator from file
    if (take_ownership)
        m_alloc.own_buffer();
}


void Group::create(bool add_free_versions)
{
    TIGHTDB_ASSERT(!is_attached());

    size_t initial_logical_file_size = sizeof (SlabAlloc::Header);

    try {
        m_top.create(Array::type_HasRefs); // Throws
        m_table_names.create(); // Throws
        m_tables.create(Array::type_HasRefs); // Throws
        m_free_positions.create(Array::type_Normal); // Throws
        m_free_lengths.create(Array::type_Normal); // Throws

        m_top.add(m_table_names.get_ref()); // Throws
        m_top.add(m_tables.get_ref()); // Throws
        m_top.add(1 + 2*initial_logical_file_size); // Throws
        m_top.add(m_free_positions.get_ref()); // Throws
        m_top.add(m_free_lengths.get_ref()); // Throws

        if (add_free_versions) {
            m_free_versions.create(Array::type_Normal); // Throws
            m_top.add(m_free_versions.get_ref()); // Throws
            size_t initial_database_version = 0; // A.k.a. transaction number
            m_top.add(1 + 2*initial_database_version); // Throws
        }
        m_is_attached = true;
    }
    catch (...) {
        m_free_versions.destroy();
        m_free_lengths.destroy();
        m_free_positions.destroy();
        m_table_names.destroy();
        m_tables.destroy_deep();
        m_top.destroy(); // Shallow!
        throw;
    }
}


void Group::init_from_ref(ref_type top_ref) TIGHTDB_NOEXCEPT
{
    m_top.init_from_ref(top_ref);
    size_t top_size = m_top.size();
    TIGHTDB_ASSERT(top_size >= 3);

    m_table_names.init_from_parent();
    m_tables.init_from_parent();
    m_is_attached = true;
    m_size = 0;
    std::size_t limit = ending_index();
    for (std::size_t ndx = first_valid_index(); ndx < limit; ++ndx)
        if (m_tables.get(ndx) != 0)
            ++m_size;

    // Note that the third slot is the logical file size.

    // Files created by Group::write() do not have free-space
    // tracking, and files that are accessed via a stan-along Group do
    // not need version information for free-space tracking.
    if (top_size > 3) {
        TIGHTDB_ASSERT(top_size >= 5);
        m_free_positions.init_from_parent();
        m_free_lengths.init_from_parent();

        if (m_is_shared && top_size > 5) {
            TIGHTDB_ASSERT(top_size >= 7);
            m_free_versions.init_from_parent();
            // Note that the seventh slot is the database version
            // (a.k.a. transaction count,) which is not yet used for
            // anything.
        }
    }
}


void Group::reset_freespace_tracking()
{
    TIGHTDB_ASSERT(m_top.is_attached());
    TIGHTDB_ASSERT(m_is_attached);
    if (m_free_versions.is_attached()) {
        TIGHTDB_ASSERT(m_top.size() == 7);
        // If free space tracking is enabled
        // we just have to reset it
        m_free_versions.set_all_to_zero(); // Throws
        return;
    }

    // Serialized files have no free space tracking at all, so we have
    // to add the basic free lists
    if (m_top.size() == 3) {
        // FIXME: There is a risk that these are already allocated,
        // and that would cause a leak. This could happen if an
        // earlier commit attempt failed.
        TIGHTDB_ASSERT(!m_free_positions.is_attached());
        TIGHTDB_ASSERT(!m_free_lengths.is_attached());
        m_free_positions.create(Array::type_Normal); // Throws
        m_free_lengths.create(Array::type_Normal); // Throws
        m_top.add(m_free_positions.get_ref()); // Throws
        m_top.add(m_free_lengths.get_ref()); // Throws
    }
    TIGHTDB_ASSERT(m_top.size() >= 5);

    // Files that have never been modified via SharedGroup do not
    // have version tracking for the free lists
    if (m_top.size() == 5) {
        // FIXME: There is a risk that this one is already allocated,
        // and that would cause a leak. This could happen if an
        // earlier commit attempt failed.
        TIGHTDB_ASSERT(!m_free_versions.is_attached());
        m_free_versions.create(Array::type_Normal); // Throws
        size_t n = m_free_positions.size();
        for (size_t i = 0; i != n; ++i)
            m_free_versions.add(0); // Throws
        m_top.add(m_free_versions.get_ref()); // Throws
        size_t initial_database_version = 0; // A.k.a. transaction number
        m_top.add(1 + 2*initial_database_version); // Throws
    }
    TIGHTDB_ASSERT(m_top.size() >= 7);
}


Group::~Group() TIGHTDB_NOEXCEPT
{
    if (!is_attached()) {
        if (m_top.is_attached())
            complete_detach();
        return;
    }
    detach_table_accessors();

#ifdef TIGHTDB_DEBUG
    // Recursively deletes entire tree. The destructor in
    // the allocator will verify that all has been deleted.
    m_top.destroy_deep();
#else
    // Just allow the allocator to release all mem in one chunk
    // without having to traverse the entire tree first
    m_alloc.detach();
#endif
}


void Group::detach_table_accessors() TIGHTDB_NOEXCEPT
{
    typedef table_accessors::const_iterator iter;
    iter end = m_table_accessors.end();
    for (iter i = m_table_accessors.begin(); i != end; ++i) {
        if (Table* t = *i) {
            typedef _impl::TableFriend tf;
            tf::detach(*t);
            tf::unbind_ref(*t);
        }
    }
}


void Group::detach() TIGHTDB_NOEXCEPT
{
    detach_but_retain_data();
    complete_detach();
}


void Group::detach_but_retain_data() TIGHTDB_NOEXCEPT
{
    m_is_attached = false;
    detach_table_accessors();
    m_table_accessors.clear();
}


void Group::complete_detach() TIGHTDB_NOEXCEPT
{
    m_top.detach();
    m_tables.detach();
    m_table_names.detach();
    m_free_positions.detach();
    m_free_lengths.detach();
    m_free_versions.detach();
}


Table* Group::get_table_by_ndx(size_t ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(ndx < m_tables.size());
    TIGHTDB_ASSERT(m_table_accessors.empty() || m_table_accessors.size() == m_tables.size());

    if (m_table_accessors.empty())
        m_table_accessors.resize(m_tables.size()); // Throws

    // Get table accessor from cache if it exists, else create
    Table* table = m_table_accessors[ndx];
    if (!table)
        table = create_table_accessor(ndx); // Throws

    return table;
}


void Group::remove_table(size_t table_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(table_ndx < m_tables.size());
    TIGHTDB_ASSERT(m_tables.get(table_ndx) != 0);
    // FSA: check that there no backlinks in this table
    // FSA: how do you actually delete the table data?
    m_tables.set(table_ndx, 0);
    m_table_names.set(table_ndx, StringData());
    Table* accessor = m_table_accessors[table_ndx];
    if (accessor) {
        // FSA: accessor->detach(); is private - what do you do then?
        // FSA: how do you delete the accessor itself?
        m_table_accessors[table_ndx] = 0;
    }
    --m_size;
    if (Replication* repl = m_alloc.get_replication())
        repl->remove_group_level_table(table_ndx); // Throws
}


void Group::rename_table(std::size_t table_ndx, StringData new_name)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(table_ndx < m_table_names.size());
    TIGHTDB_ASSERT(m_tables.get(table_ndx) != 0);
    m_table_names.set(table_ndx, new_name);
    if (Replication* repl = m_alloc.get_replication())
        repl->rename_group_level_table(table_ndx, new_name); // Throws
}


size_t Group::create_table(StringData name)
{
    using namespace _impl;
    typedef TableFriend tf;
    ref_type ref = tf::create_empty_table(m_alloc); // Throws
    size_t ndx;
    if (m_size < m_tables.size()) {
        ndx = m_tables.find_first(0);
        TIGHTDB_ASSERT(ndx != tightdb::not_found);
        TIGHTDB_ASSERT(m_table_names.get(ndx) == "");
    }
    else {
        ndx = m_tables.size();
        m_tables.add(0); // Throws
        m_table_names.add(StringData()); // Throws
        // Need slot for table accessor
        if (!m_table_accessors.empty())
            m_table_accessors.push_back(0); // Throws
    }
    m_tables.set(ndx, ref); // Throws
    m_table_names.set(ndx, name); // Throws
    ++m_size;

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_alloc.get_replication())
        repl->new_group_level_table(name); // Throws
#endif

    return ndx;
}


Table* Group::create_table_accessor(size_t table_ndx)
{
    TIGHTDB_ASSERT(m_table_accessors.empty() || table_ndx < m_table_accessors.size());

    if (m_table_accessors.empty())
        m_table_accessors.resize(m_tables.size()); // Throws

    // Whenever a table has a link column, the column accessor must be set up to
    // refer to the target table accessor, so the target table accessor needs to
    // be created too, if it does not already exist. This, of course, applies
    // recusively, and it applies to the opposide direction of links too (from
    // target side to origin side). This means that whenever we create a table
    // accessor, we actually need to create the entire cluster of table
    // accessors, that is reachable in zero or more steps along links, or
    // backwards along links.
    //
    // To be able to do this, and to handle the cases where the link
    // relathionship graph contains cycles, each table accessor need to be
    // created in the following steps:
    //
    //  1) Create table accessor, but skip creation of column accessors
    //  2) Register incomplete table accessor in group accessor
    //  3) Mark table accessor
    //  4) Create column accessors
    //  5) Unmark table accessor
    //
    // The marking ensures that the establsihment of the connection between link
    // and backlink column accessors is postponed until both column accessors
    // are created. Infinite recursion due to cycles is prevented by the early
    // registration in the group accessor of inclomplete table accessors.

    typedef _impl::TableFriend tf;
    ref_type ref = m_tables.get_as_ref(table_ndx);
    Table* table = tf::create_incomplete_accessor(m_alloc, ref, this, table_ndx); // Throws

    // The new accessor cannot be leaked, because no exceptions can be throws
    // before it becomes referenced from `m_column_accessors`.

    // Increase reference count from 0 to 1 to make the group accessor keep
    // the table accessor alive. This extra reference count will be revoked
    // during destruction of the group accessor.
    tf::bind_ref(*table);

    tf::mark(*table);
    m_table_accessors[table_ndx] = table;
    tf::complete_accessor(*table); // Throws
    tf::unmark(*table);
    return table;
}


class Group::DefaultTableWriter: public Group::TableWriter {
public:
    DefaultTableWriter(const Group& group):
        m_group(group)
    {
    }
    size_t write_names(_impl::OutputStream& out) TIGHTDB_OVERRIDE
    {
        return m_group.m_table_names.write(out); // Throws
    }
    size_t write_tables(_impl::OutputStream& out) TIGHTDB_OVERRIDE
    {
        return m_group.m_tables.write(out); // Throws
    }
private:
    const Group& m_group;
};

void Group::write(ostream& out) const
{
    TIGHTDB_ASSERT(is_attached());
    DefaultTableWriter table_writer(*this);
    write(out, table_writer); // Throws
}

void Group::write(const string& path) const
{
    File file;
    int flags = 0;
    file.open(path, File::access_ReadWrite, File::create_Must, flags);
    File::Streambuf streambuf(&file);
    ostream out(&streambuf);
    write(out);
}


BinaryData Group::write_to_mem() const
{
    TIGHTDB_ASSERT(is_attached());

    // Get max possible size of buffer
    //
    // FIXME: This size could potentially be vastly bigger that what
    // is actually needed.
    size_t max_size = m_alloc.get_total_size();

    char* buffer = static_cast<char*>(malloc(max_size)); // Throws
    if (!buffer)
        throw bad_alloc();
    try {
        MemoryOutputStream out; // Throws
        out.set_buffer(buffer, buffer + max_size);
        write(out); // Throws
        size_t size = out.size();
        return BinaryData(buffer, size);
    }
    catch (...) {
        free(buffer);
        throw;
    }
}


void Group::write(ostream& out, TableWriter& table_writer)
{
    _impl::OutputStream out_2(out);

    // Write the file header
    const char* data = reinterpret_cast<const char*>(&SlabAlloc::streaming_header);
    out_2.write(data, sizeof SlabAlloc::streaming_header);

    // Because we need to include the total logical file size in the
    // top-array, we have to start by writing everything except the
    // top-array, and then finally compute and write a correct version
    // of the top-array. The free-space information of the group will
    // not be included, as it is not needed in the streamed format.
    size_t names_pos  = table_writer.write_names(out_2); // Throws
    size_t tables_pos = table_writer.write_tables(out_2); // Throws
    size_t top_pos = out_2.get_pos();

    // Produce a preliminary version of the top array whose
    // representation is guaranteed to be able to hold the final file
    // size
    int top_size = 3;
    size_t max_top_byte_size = Array::get_max_byte_size(top_size);
    uint64_t max_final_file_size = top_pos + max_top_byte_size;
    Array top(Array::type_HasRefs); // Throws
    // FIXME: Dangerous cast: unsigned -> signed
    top.ensure_minimum_width(1 + 2*max_final_file_size); // Throws
    // FIXME: We really need an alternative to Array::truncate() that is able to expand.
    // FIXME: Dangerous cast: unsigned -> signed
    top.add(names_pos); // Throws
    top.add(tables_pos); // Throws
    top.add(0); // Throws

    // Finalize the top array by adding the projected final file size
    // to it
    size_t top_byte_size = top.get_byte_size();
    size_t final_file_size = top_pos + top_byte_size;
    // FIXME: Dangerous cast: unsigned -> signed
    top.set(2, 1 + 2*final_file_size);

    // Write the top array
    bool recurse = false;
    top.write(out_2, recurse); // Throws
    TIGHTDB_ASSERT(out_2.get_pos() == final_file_size);

    top.destroy(); // Shallow

    // Write streaming footer
    SlabAlloc::StreamingFooter footer;
    footer.m_top_ref = top_pos;
    footer.m_magic_cookie = SlabAlloc::footer_magic_cookie;
    out_2.write(reinterpret_cast<const char*>(&footer), sizeof footer);
}


void Group::commit()
{
    TIGHTDB_ASSERT(is_attached());

    // GroupWriter::write_group() needs free-space tracking
    // information, so if the attached database does not contain it,
    // we must add it now. Empty (newly created) database files and
    // database files created by Group::write() do not have free-space
    // tracking information.
    if (m_free_positions.is_attached()) {
        TIGHTDB_ASSERT(m_top.size() >= 5);
        if (m_top.size() > 5) {
            TIGHTDB_ASSERT(m_top.size() >= 7);
            // Delete free-list version information and database
            // version (a.k.a. transaction number)
            Array::destroy(m_top.get_as_ref(5), m_top.get_alloc());
            m_top.erase(5, 7);
        }
    }
    else {
        TIGHTDB_ASSERT(m_top.size() == 3);
        m_free_positions.create(Array::type_Normal);
        m_free_lengths.create(Array::type_Normal);
        m_top.add(m_free_positions.get_ref());
        m_top.add(m_free_lengths.get_ref());
    }

    GroupWriter out(*this); // Throws

    // Recursively write all changed arrays to the database file. We
    // postpone the commit until we are sure that no exceptions can be
    // thrown.
    ref_type top_ref = out.write_group(); // Throws

    // Since the group is persisiting in single-thread (unshared)
    // mode we have to make sure that the group stays valid after
    // commit

    // Mark all managed space (beyond the attached file) as free.
    //
    // FIXME: Perform this as part of m_alloc.remap(), but that
    // requires that we always call remap().
    m_alloc.reset_free_space_tracking(); // Throws

    size_t old_baseline = m_alloc.get_baseline();

    // Remap file if it has grown
    size_t new_file_size = out.get_file_size();
    if (new_file_size > old_baseline) {
        if (m_alloc.remap(new_file_size)) { // Throws
            // The file was mapped to a new address, so all array
            // accessors must be updated.
            old_baseline = 0;
        }
    }

    out.commit(top_ref); // Throws

    // Recusively update refs in all active tables (columns, arrays..)
    update_refs(top_ref, old_baseline);
}


void Group::update_refs(ref_type top_ref, size_t old_baseline) TIGHTDB_NOEXCEPT
{
    // After Group::commit() we will always have free space tracking
    // info.
    TIGHTDB_ASSERT(m_top.size() >= 5);

    // Array nodes that are part of the previous version of the
    // database will not be overwritten by Group::commit(). This is
    // necessary for robustness in the face of abrupt termination of
    // the process. It also means that we can be sure that an array
    // remains unchanged across a commit if the new ref is equal to
    // the old ref and the ref is below the previous baseline.

    if (top_ref < old_baseline && m_top.get_ref() == top_ref)
        return;

    m_top.init_from_ref(top_ref);

    // Now we can update it's child arrays
    m_table_names.update_from_parent(old_baseline);
    m_free_positions.update_from_parent(old_baseline);
    m_free_lengths.update_from_parent(old_baseline);
    if (m_is_shared)
        m_free_versions.update_from_parent(old_baseline);

    // If m_tables has not been modfied we don't
    // need to update attached table accessors
    if (!m_tables.update_from_parent(old_baseline))
        return;

    // Update all attached table accessors including those attached to
    // subtables.
    typedef table_accessors::const_iterator iter;
    iter end = m_table_accessors.end();
    for (iter i = m_table_accessors.begin(); i != end; ++i) {
        typedef _impl::TableFriend tf;
        if (Table* table = *i)
            tf::update_from_parent(*table, old_baseline);
    }
}

void Group::reattach_from_retained_data()
{
    TIGHTDB_ASSERT(!is_attached());
    TIGHTDB_ASSERT(m_top.is_attached());
    m_is_attached = true;
}

void Group::init_for_transact(ref_type new_top_ref, size_t new_file_size)
{
    TIGHTDB_ASSERT(new_top_ref < new_file_size);
    TIGHTDB_ASSERT(!is_attached());

    if (m_top.is_attached())
        complete_detach();

    // Make all managed memory beyond the attached file available
    // again.
    //
    // FIXME: Perform this as part of m_alloc.remap(), but that
    // requires that we always call remap().
    m_alloc.reset_free_space_tracking(); // Throws

    // Update memory mapping if database file has grown
    if (new_file_size > m_alloc.get_baseline())
        m_alloc.remap(new_file_size); // Throws

    // If the file is empty (probably because it was just created) we
    // need to create a new empty group. If this happens at the
    // beginning of a 'read' transaction (as opposed to at the
    // beginning of a 'write' transaction), the creation of the group
    // serves only to put the group accessor into a valid state, and
    // the allocated memory will be discarded when the 'read'
    // transaction ends (actually not until a new transaction is
    // started).
    if (new_top_ref == 0) {
        bool add_free_versions = true;
        create(add_free_versions); // Throws
    }
    else {
        init_from_ref(new_top_ref);
    }
}


bool Group::operator==(const Group& g) const
{
    size_t n = size();
    if (n != g.size())
        return false;
    for (size_t i = 0; i < n; ++i) {
        const Table* t1 = get_table_by_ndx(i); // Throws
        const Table* t2 = g.get_table_by_ndx(i); // Throws
        if (*t1 != *t2)
            return false;
    }
    return true;
}


void Group::to_string(ostream& out) const
{
    // Calculate widths
    size_t index_width = 4;
    size_t name_width = 10;
    size_t rows_width = 6;
    size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        StringData name = get_table_name(i);
        if (name_width < name.size())
            name_width = name.size();

        ConstTableRef table = get_table(name);
        size_t row_count = table->size();
        if (rows_width < row_count) { // FIXME: should be the number of digits in row_count: floor(log10(row_count+1))
            rows_width = row_count;
        }
    }


    // Print header
    out << setw(int(index_width+1)) << left << " ";
    out << setw(int(name_width+1))  << left << "tables";
    out << setw(int(rows_width))    << left << "rows"    << endl;

    // Print tables
    for (size_t i = 0; i < count; ++i) {
        StringData name = get_table_name(i);
        ConstTableRef table = get_table(name);
        size_t row_count = table->size();

        out << setw(int(index_width)) << right << i           << " ";
        out << setw(int(name_width))  << left  << name.data() << " ";
        out << setw(int(rows_width))  << left  << row_count   << endl;
    }
}


void Group::mark_all_table_accessors() TIGHTDB_NOEXCEPT
{
    size_t num_tables = m_table_accessors.size();
    for (size_t table_ndx = 0; table_ndx != num_tables; ++table_ndx) {
        if (Table* table = m_table_accessors[table_ndx]) {
            typedef _impl::TableFriend tf;
            tf::recursive_mark(*table); // Also all subtable accessors
        }
    }
}


#ifdef TIGHTDB_ENABLE_REPLICATION

namespace {

class MultiLogInputStream: public Replication::InputStream {
public:
    MultiLogInputStream(const BinaryData* logs_begin, const BinaryData* logs_end):
        m_logs_begin(logs_begin), m_logs_end(logs_end)
    {
        if (m_logs_begin != m_logs_end)
            m_curr_buf_remaining_size = m_logs_begin->size();
    }

    ~MultiLogInputStream() TIGHTDB_OVERRIDE
    {
    }

    size_t read(char* buffer, size_t size) TIGHTDB_OVERRIDE
    {
        if (m_logs_begin == m_logs_end)
            return 0;
        for (;;) {
            if (m_curr_buf_remaining_size > 0) {
                size_t offset = m_logs_begin->size() - m_curr_buf_remaining_size;
                const char* data = m_logs_begin->data() + offset;
                size_t size_2 = min(m_curr_buf_remaining_size, size);
                m_curr_buf_remaining_size -= size_2;
                // FIXME: Eliminate the need for copying by changing the API of
                // Replication::InputStream such that blocks can be handed over
                // without copying. This is a straight forward change, but the
                // result is going to be more complicated and less conventional.
                copy(data, data + size_2, buffer);
                return size_2;
            }

            ++m_logs_begin;
            if (m_logs_begin == m_logs_end)
                return 0;
            m_curr_buf_remaining_size = m_logs_begin->size();
        }
    }

private:
    const BinaryData* m_logs_begin;
    const BinaryData* m_logs_end;
    size_t m_curr_buf_remaining_size;
};


class MarkDirtyUpdater: public _impl::TableFriend::AccessorUpdater {
public:
    void update(Table& table) TIGHTDB_OVERRIDE
    {
        typedef _impl::TableFriend tf;
        tf::mark(table);
    }

    void update_parent(Table& table) TIGHTDB_OVERRIDE
    {
        typedef _impl::TableFriend tf;
        tf::mark(table);
    }

    size_t m_col_ndx;
    DataType m_type;
};


class InsertColumnUpdater: public _impl::TableFriend::AccessorUpdater {
public:
    InsertColumnUpdater(size_t col_ndx):
        m_col_ndx(col_ndx)
    {
    }

    void update(Table& table) TIGHTDB_OVERRIDE
    {
        typedef _impl::TableFriend tf;
        tf::adj_insert_column(table, m_col_ndx); // Throws
        tf::mark_link_target_tables(table, m_col_ndx+1);
    }

    void update_parent(Table&) TIGHTDB_OVERRIDE
    {
    }

private:
    size_t m_col_ndx;
};


class EraseColumnUpdater: public _impl::TableFriend::AccessorUpdater {
public:
    EraseColumnUpdater(size_t col_ndx):
        m_col_ndx(col_ndx)
    {
    }

    void update(Table& table) TIGHTDB_OVERRIDE
    {
        typedef _impl::TableFriend tf;
        tf::adj_erase_column(table, m_col_ndx);
        tf::mark_link_target_tables(table, m_col_ndx);
    }

    void update_parent(Table&) TIGHTDB_OVERRIDE
    {
    }

private:
    size_t m_col_ndx;
};

} // anonymous namespace


// In general, this class cannot assume more than minimal accessor consistency
// (See AccessorConcistncyLevels), it can however assume that replication
// instruction arguments are meaningfull with respect to the current state of
// the accessor hierarchy. For example, a column index argument of `i` is known
// to refer to the `i`'th entry of Table::m_cols.
//
// FIXME: There is currently no checking on valid instruction arguments such as
// column index within bounds. Consider whether we can trust the contents of the
// transaction log enough to skip these checks.
class Group::TransactAdvancer {
public:
    TransactAdvancer(Group& group):
        m_group(group)
    {
    }

    bool new_group_level_table(StringData) TIGHTDB_NOEXCEPT
    {
        m_group.m_table_accessors.push_back(0); // Throws
        return true;
    }

    bool rename_group_level_table(std::size_t table_ndx, StringData new_name) TIGHTDB_NOEXCEPT
    {
        // do nothing - renaming is done by changing the data.
        static_cast<void>(table_ndx);
        static_cast<void>(new_name);
        return true;
    }

    bool remove_group_level_table(std::size_t table_ndx) TIGHTDB_NOEXCEPT
    {
        // actual change of data occurs in parallel, but we need to kill any accessor at table_ndx.
        // FSA: FIXME! not done yet
        static_cast<void>(table_ndx);
        return true;
    }

    bool select_table(size_t group_level_ndx, int levels, const size_t* path) TIGHTDB_NOEXCEPT
    {
        m_table.reset();
        if (group_level_ndx < m_group.m_table_accessors.size()) {
            if (Table* table = m_group.m_table_accessors[group_level_ndx]) {
                const size_t* path_begin = path;
                const size_t* path_end = path_begin + 2*levels;
                for (;;) {
                    typedef _impl::TableFriend tf;
                    tf::mark(*table);
                    if (path_begin == path_end) {
                        m_table.reset(table);
                        break;
                    }
                    size_t col_ndx = path_begin[0];
                    size_t row_ndx = path_begin[1];
                    table = tf::get_subtable_accessor(*table, col_ndx, row_ndx);
                    if (!table)
                        break;
                    path_begin += 2;
                }
            }
        }
        return true;
    }

    bool insert_empty_rows(size_t row_ndx, size_t num_rows) TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::adj_accessors_insert_rows(*m_table, row_ndx, num_rows);
        return true;
    }

    bool erase_row(size_t row_ndx) TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::adj_accessors_erase_row(*m_table, row_ndx);
        return true;
    }

    bool move_last_over(size_t target_row_ndx, size_t last_row_ndx) TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::adj_accessors_move_last_over(*m_table, target_row_ndx, last_row_ndx);
        return true;
    }

    bool clear_table() TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::adj_acc_clear_root_table(*m_table);
        // tf::discard_child_accessors(*m_table);
        return true;
    }

    bool insert_int(size_t col_ndx, size_t row_ndx, int_fast64_t) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_bool(size_t col_ndx, size_t row_ndx, bool) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_float(size_t col_ndx, size_t row_ndx, float) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_double(size_t col_ndx, size_t row_ndx, double) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_string(size_t col_ndx, size_t row_ndx, StringData) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_binary(size_t col_ndx, size_t row_ndx, BinaryData) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_date_time(size_t col_ndx, size_t row_ndx, DateTime) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_table(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_mixed(size_t col_ndx, size_t row_ndx, const Mixed&) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool insert_link(size_t col_ndx, size_t row_ndx, size_t) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
       return true;
    }

    bool insert_link_list(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1);
        return true;
    }

    bool row_insert_complete() TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_int(size_t, size_t, int_fast64_t) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_bool(size_t, size_t, bool) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_float(size_t, size_t, float) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_double(size_t, size_t, double) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_string(size_t, size_t, StringData) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_binary(size_t, size_t, BinaryData) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_date_time(size_t, size_t, DateTime) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool set_table(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
    {
        if (m_table) {
            typedef _impl::TableFriend tf;
            if (Table* subtab = tf::get_subtable_accessor(*m_table, col_ndx, row_ndx)) {
                tf::mark(*subtab);
                tf::adj_acc_clear_nonroot_table(*subtab);
            }
        }
        return true;
    }

    bool set_mixed(size_t col_ndx, size_t row_ndx, const Mixed&) TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::discard_subtable_accessor(*m_table, col_ndx, row_ndx);
        return true;
    }

    bool set_link(size_t col_ndx, size_t, size_t) TIGHTDB_NOEXCEPT
    {
        // When links are changed, the link-target table is also affected and
        // its accessor must therefore be marked dirty too. Indeed, when it
        // exists, the link-target table accessor must be marked dirty
        // regardless of whether an accessor exists for the origin table (i.e.,
        // regardless of whether `m_table` is null or not.) This would seem to
        // pose a problem, because there is no easy way to identify the
        // link-target table when there is no accessor for the origin
        // table. Fortunately, due to the fact that back-link column accessors
        // refer to the origin table accessor (and vice versa), it follows that
        // the link-target table accessor exists if, and only if then origin
        // table accessor exists.
        //
        // get_link_target_table_accessor() will return null if the
        // m_table->m_cols[col_ndx] is null, but this can happen only when the
        // column was inserted earlier during this transaction advance, and in
        // that case, we have already marked the target table accesor dirty.
        typedef _impl::TableFriend tf;
        if (m_table) {
            if (Table* target = tf::get_link_target_table_accessor(*m_table, col_ndx))
                tf::mark(*target);
        }
        return true;
    }

    bool add_int_to_column(size_t, int_fast64_t) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool optimize_table() TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool select_descriptor(int levels, const size_t* path)
    {
        m_desc.reset();
        if (m_table) {
            TIGHTDB_ASSERT(!m_table->has_shared_type());
            typedef _impl::TableFriend tf;
            Descriptor* desc = tf::get_root_table_desc_accessor(*m_table);
            int i = 0;
            while (desc) {
                if (i >= levels) {
                    m_desc.reset(desc);
                    break;
                }
                typedef _impl::DescriptorFriend df;
                size_t col_ndx = path[i];
                desc = df::get_subdesc_accessor(*desc, col_ndx);
                ++i;
            }
            m_desc_path_begin = path;
            m_desc_path_end = path + levels;
            MarkDirtyUpdater updater;
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);
        }
        return true;
    }

    bool insert_column(size_t col_ndx, DataType, StringData, size_t link_target_table_ndx)
    {
        if (m_table) {
            typedef _impl::TableFriend tf;
            InsertColumnUpdater updater(col_ndx);
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);

            // See comments on link handling in TransactAdvancer::set_link().
            if (link_target_table_ndx != tightdb::npos) {
                Table* target = m_group.get_table_by_ndx(link_target_table_ndx); // Throws
                tf::adj_add_column(*target); // Throws
                tf::mark(*target);
            }
        }
        typedef _impl::DescriptorFriend df;
        if (m_desc)
            df::adj_insert_column(*m_desc, col_ndx);
        return true;
    }

    bool erase_column(size_t col_ndx, size_t link_target_table_ndx, size_t backlink_col_ndx)
    {
        if (m_table) {
            typedef _impl::TableFriend tf;

            // For link columns we need to handle the backlink column first in
            // case the target table is the same as the origin table (because
            // the backlink column occurs after regular columns.) Also see
            // comments on link handling in TransactAdvancer::set_link().
            if (link_target_table_ndx != tightdb::npos) {
                Table* target = m_group.get_table_by_ndx(link_target_table_ndx); // Throws
                tf::adj_erase_column(*target, backlink_col_ndx); // Throws
                tf::mark(*target);
            }

            EraseColumnUpdater updater(col_ndx);
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);
        }
        typedef _impl::DescriptorFriend df;
        if (m_desc)
            df::adj_erase_column(*m_desc, col_ndx);
        return true;
    }

    bool rename_column(size_t, StringData) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool add_search_index(size_t) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool select_link_list(size_t col_ndx, size_t) TIGHTDB_NOEXCEPT
    {
        // See comments on link handling in TransactAdvancer::set_link().
        typedef _impl::TableFriend tf;
        if (m_table) {
            if (Table* target = tf::get_link_target_table_accessor(*m_table, col_ndx))
                tf::mark(*target);
        }
        return true; // Noop
    }

    bool link_list_set(size_t, size_t) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool link_list_insert(size_t, size_t) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool link_list_move(size_t, size_t) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool link_list_erase(size_t) TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

    bool link_list_clear() TIGHTDB_NOEXCEPT
    {
        return true; // Noop
    }

private:
    Group& m_group;
    TableRef m_table;
    DescriptorRef m_desc;
    const size_t* m_desc_path_begin;
    const size_t* m_desc_path_end;
};


void Group::advance_transact(ref_type new_top_ref, size_t new_file_size,
                             const BinaryData* logs_begin, const BinaryData* logs_end)
{
    // The purpose of this function is to refresh all attached accessors after
    // the underlying node structure has undergone arbitrary change, such as
    // when a read transaction has been advanced to a later snapshot of the
    // database.
    //
    // Initially, when this function is invoked, we cannot assume any
    // correspondance between the accessor state and the underlying node
    // structure. We can assume that the hierarchy is in a state of minimal
    // consistency, and that it can be brought to a state of structural
    // correspondace using information in the transaction logs. When structural
    // correspondace is achieved, we can reliably refresh the accessor hierarchy
    // (Table::refresh_accessor_tree()) to bring it back to a fully concsistent
    // state. See AccessorConsistencyLevels.
    //
    // Much of the information in the transaction logs is not used in this
    // process, because the changes have already been applied to the underlying
    // node structure. All we need to do here is to bring the accessors back
    // into a state where they correctly reflect the underlying structure (or
    // detach them if the underlying entity has been removed.)
    //
    // The consequences of the changes in the transaction logs can be divided
    // into two types; those that need to be applied to the accessors
    // immediately (Table::adj_insert_column()), and those that can be "lumped
    // together" and deduced automatically during a final accessor refresh
    // operation (Table::refresh_accessor_tree()).
    //
    // Most transaction log instructions have consequences of both types. For
    // example, when an "insert column" instruction is seen, we must immediately
    // shift the positions of all existing columns accessors after the point of
    // insertion. For practical reasons, and for efficiency, we will just insert
    // a null pointer into `Table::m_cols` at this time, and then postpone the
    // creation of the column accessor to the final per-table accessor refresh
    // operation.
    //
    // The final per-table refresh operation visits each table accessor
    // recursively starting from the roots (group-level tables). It relies the
    // the per-table accessor dirty flags (Table::m_dirty) to prune the
    // traversal to the set of accessors that were touched by the changes in the
    // transaction logs.

    MultiLogInputStream in(logs_begin, logs_end);
    Replication::TransactLogParser parser(in);
    TransactAdvancer advancer(*this);
    parser.parse(advancer); // Throws

    // Update memory mapping if database file has grown
    if (new_file_size > m_alloc.get_baseline()) {
        m_alloc.reset_free_space_tracking(); // Throws
        m_alloc.remap(new_file_size); // Throws
        // The file was mapped to a new address, so all array accessors must be
        // updated.
        mark_all_table_accessors();
    }

    init_from_ref(new_top_ref);
    m_top.get_alloc().bump_global_version();
    // Refresh all remaining dirty table accessors
    size_t num_tables = m_table_accessors.size();
    for (size_t table_ndx = 0; table_ndx != num_tables; ++table_ndx) {
        if (Table* table = m_table_accessors[table_ndx]) {
            typedef _impl::TableFriend tf;
            if (tf::is_marked(*table)) {
                tf::refresh_accessor_tree(*table); // Throws
                tf::bump_version(*table);
            }
        }
    }
}

#endif // TIGHTDB_ENABLE_REPLICATION


#ifdef TIGHTDB_DEBUG

void Group::Verify() const
{
    TIGHTDB_ASSERT(is_attached());

    m_alloc.Verify();

    // Verify free lists
    if (m_free_positions.is_attached()) {
        TIGHTDB_ASSERT(m_free_lengths.is_attached());

        size_t n = m_free_positions.size();
        TIGHTDB_ASSERT(n == m_free_lengths.size());

        if (m_free_versions.is_attached())
            TIGHTDB_ASSERT(n == m_free_versions.size());

        // We need to consider the "logical" size of the file here,
        // and not the real size. The real size may have changed
        // without the free space information having been adjusted
        // accordingly. This can happen, for example, if commit()
        // fails before writing the new top-ref, but after having
        // extended the file size.
        size_t logical_file_size = to_size_t(m_top.get(2) / 2);

        size_t prev_end = 0;
        for (size_t i = 0; i != n; ++i) {
            size_t pos  = to_size_t(m_free_positions.get(i));
            size_t size = to_size_t(m_free_lengths.get(i));

            TIGHTDB_ASSERT(pos < logical_file_size);
            TIGHTDB_ASSERT(size > 0);
            TIGHTDB_ASSERT(pos + size <= logical_file_size);
            TIGHTDB_ASSERT(prev_end <= pos);

            TIGHTDB_ASSERT(pos  % 8 == 0); // 8-byte alignment
            TIGHTDB_ASSERT(size % 8 == 0); // 8-byte alignment

            prev_end = pos + size;
        }
    }

    // Verify tables
    {
        size_t n = m_tables.size();
        for (size_t i = 0; i != n; ++i) {
            const Table* table = get_table_by_ndx(i);
            TIGHTDB_ASSERT(table->get_index_in_parent() == i);
            table->Verify();
        }
    }
}


MemStats Group::stats()
{
    MemStats stats;
    m_top.stats(stats);

    return stats;
}


void Group::print() const
{
    m_alloc.print();
}


void Group::print_free() const
{
    if (!m_free_positions.is_attached()) {
        cout << "none\n";
        return;
    }
    bool has_versions = m_free_versions.is_attached();

    size_t n = m_free_positions.size();
    for (size_t i = 0; i != n; ++i) {
        size_t pos  = to_size_t(m_free_positions[i]);
        size_t size = to_size_t(m_free_lengths[i]);
        cout << i << ": " << pos << " " << size;

        if (has_versions) {
            size_t version = to_size_t(m_free_versions[i]);
            cout << " " << version;
        }
        cout << "\n";
    }
    cout << "\n";
}


void Group::to_dot(ostream& out) const
{
    out << "digraph G {" << endl;

    out << "subgraph cluster_group {" << endl;
    out << " label = \"Group\";" << endl;

    m_top.to_dot(out, "group_top");
    m_table_names.to_dot(out, "table_names");
    m_tables.to_dot(out, "tables");

    // Tables
    for (size_t i = 0; i < m_tables.size(); ++i) {
        const Table* table = get_table_by_ndx(i);
        StringData name = get_table_name(i);
        table->to_dot(out, name);
    }

    out << "}" << endl;
    out << "}" << endl;
}


void Group::to_dot() const
{
    to_dot(cerr);
}


void Group::to_dot(const char* file_path) const
{
    ofstream out(file_path);
    to_dot(out);
}

pair<ref_type, size_t> Group::get_to_dot_parent(size_t ndx_in_parent) const
{
    return make_pair(m_tables.get_ref(), ndx_in_parent);
}


/*
void Group::zero_free_space(size_t file_size, size_t readlock_version)
{
    static_cast<void>(readlock_version); // FIXME: Why is this parameter not used?

    if (!m_is_shared)
        return;

    File::Map<char> map(m_alloc.m_file, File::access_ReadWrite, file_size);

    size_t count = m_free_positions.size();
    for (size_t i = 0; i < count; ++i) {
        size_t v = to_size_t(m_free_versions.get(i)); // todo, remove assizet when 64 bit
        if (v >= m_readlock_version)
            continue;

        size_t pos = to_size_t(m_free_positions.get(i));
        size_t len = to_size_t(m_free_lengths.get(i));

        char* p = map.get_addr() + pos;
        fill(p, p+len, 0);
    }
}
*/

#endif // TIGHTDB_DEBUG
