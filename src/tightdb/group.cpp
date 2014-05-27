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
    TIGHTDB_ASSERT(!is_attached());

    m_top.init_from_ref(top_ref);
    size_t top_size = m_top.size();
    TIGHTDB_ASSERT(top_size >= 3);

    ref_type names_ref = m_top.get_as_ref(0);
    ref_type tables_ref = m_top.get_as_ref(1);
    m_table_names.init_from_ref(names_ref);
    m_tables.init_from_ref(tables_ref);
    m_is_attached = true;

    // Note that the third slot is the logical file size.

    // File created by Group::write() do not have free space markers
    // at all, and files that are not shared does not need version
    // info for free space.
    if (top_size > 3) {
        TIGHTDB_ASSERT(top_size >= 5);
        ref_type free_positions_ref = m_top.get_as_ref(3);
        ref_type free_sizes_ref     = m_top.get_as_ref(4);
        m_free_positions.init_from_ref(free_positions_ref);
        m_free_lengths.init_from_ref(free_sizes_ref);

        if (m_is_shared && top_size > 5) {
            TIGHTDB_ASSERT(top_size >= 7);
            ref_type free_versions_ref = m_top.get_as_ref(5);
            m_free_versions.init_from_ref(free_versions_ref);
            // Note that the seventh slot is the database version
            // (a.k.a. transaction count,) which is not yet used for
            // anything.
        }
    }
}


void Group::init_shared()
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

    // Files that have nevner been modified via SharedGroup do not
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
            _impl::TableFriend::detach(*t);
            _impl::TableFriend::unbind_ref(*t);
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

    if (m_table_accessors.empty())
        m_table_accessors.resize(m_tables.size()); // Throws

    TIGHTDB_ASSERT(m_table_accessors.size() == m_tables.size());

    // Get table from cache if exists, else create
    Table* table = m_table_accessors[ndx];
    if (!table) {
        ref_type ref = m_tables.get_as_ref(ndx);
        table = _impl::TableFriend::create_ref_counted(m_alloc, ref, this, ndx); // Throws
        m_table_accessors[ndx] = table;
        _impl::TableFriend::bind_ref(*table); // Increase reference count from 0 to 1

        // The link targets in the table has to be initialized
        // after creation to avoid circular initializations
        _impl::TableFriend::initialize_link_targets(*table);
    }
    return table;
}

/*
ref_type Group::create_new_table(StringData name)
{
    // FIXME: This function is exception safe under the assumption
    // that m_tables.insert() and m_table_names.insert() are exception
    // safe. Currently, Array::insert() is not exception safe, but it
    // is expected that it will be in the future. Note that a function
    // is considered exception safe if it produces no visible
    // side-effects when it throws, at least not in any way that
    // matters.

    using namespace _impl;
    DeepArrayRefDestroyGuard ref_dg(TableFriend::create_empty_table(m_alloc), m_alloc); // Throws
    size_t ndx = m_tables.size();
    TIGHTDB_ASSERT(ndx == m_table_names.size());
    m_tables.insert(ndx, ref_dg.get()); // Throws
    try {
        m_table_names.insert(ndx, name); // Throws
        try {
#ifdef TIGHTDB_ENABLE_REPLICATION
            if (Replication* repl = m_alloc.get_replication())
                repl->new_group_level_table(name); // Throws
#endif

            // The rest is guaranteed not to throw
            return ref_dg.release();
        }
        catch (...) {
            m_table_names.erase(ndx); // Guaranteed not to throw
            throw;
        }
    }
    catch (...) {
        m_tables.erase(ndx); // Guaranteed not to throw
        throw;
    }
}
*/

Table* Group::create_new_table_and_accessor(StringData name, SpecSetter spec_setter)
{
    // FIXME: This function is exception safe under the assumption
    // that m_tables.insert() and m_table_names.insert() are exception
    // safe. Currently, Array::insert() is not exception safe, but it
    // is expected that it will be in the future. Note that a function
    // is considered exception safe if it produces no visible
    // side-effects when it throws, at least not in any way that
    // matters.

#ifdef TIGHTDB_ENABLE_REPLICATION
    // FIXME: ExceptionSafety: If this succeeds, but some of the
    // following fails, then we must ask the replication instance to
    // discard everything written to the log since this point. This
    // should probably be handled with a 'scoped guard'.
    if (Replication* repl = m_alloc.get_replication())
        repl->new_group_level_table(name); // Throws
#endif

    using namespace _impl;
    DeepArrayRefDestroyGuard ref_dg(TableFriend::create_empty_table(m_alloc), m_alloc); // Throws
    typedef TableFriend::UnbindGuard TableUnbindGuard;
    TableUnbindGuard table_ug(TableFriend::create_ref_counted(m_alloc, ref_dg.get(),
                                                              null_ptr, 0)); // Throws

    // The table accessor owns the ref until the point below where a
    // parent is set in Table::m_top.
    ref_type ref = ref_dg.release();
    TableFriend::bind_ref(*table_ug); // Increase reference count from 0 to 1

    size_t ndx = m_tables.size();
    m_table_accessors.resize(ndx+1); // Throws
    TableFriend::set_top_parent(*table_ug, this, ndx);
    try {
        if (spec_setter)
            (*spec_setter)(*table_ug); // Throws

        TIGHTDB_ASSERT(ndx == m_table_names.size());
        m_tables.insert(ndx, ref); // Throws
        try {
            m_table_names.insert(ndx, name); // Throws
            // The rest is guaranteed not to throw
            Table* table = table_ug.release();
            m_table_accessors[ndx] = table;
            return table;
        }
        catch (...) {
            m_tables.erase(ndx); // Guaranteed not to throw
            throw;
        }
    }
    catch (...) {
        TableFriend::set_top_parent(*table_ug, 0, 0);
        throw;
    }
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
    TIGHTDB_ASSERT(!m_free_versions.is_attached());

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

    // If m_tables has not been modfied we don't
    // need to update attached table accessors
    if (!m_tables.update_from_parent(old_baseline))
        return;

    // Update all attached table accessors including those attached to
    // subtables.
    typedef table_accessors::const_iterator iter;
    iter end = m_table_accessors.end();
    for (iter i = m_table_accessors.begin(); i != end; ++i) {
        if (Table* table = *i)
            _impl::TableFriend::update_from_parent(*table, old_baseline);
    }
}

void Group::reattach_from_retained_data()
{
    TIGHTDB_ASSERT(!is_attached());
    TIGHTDB_ASSERT(m_top.is_attached());
    m_is_attached = true;
}

void Group::update_from_shared(ref_type new_top_ref, size_t new_file_size)
{
    TIGHTDB_ASSERT(new_top_ref < new_file_size);
    TIGHTDB_ASSERT(!is_attached());

    if (m_top.is_attached()) {

        complete_detach();
    }
    // Make all managed memory beyond the attached file available
    // again.
    //
    // FIXME: Perform this as part of m_alloc.remap(), but that
    // requires that we always call remap().
    m_alloc.reset_free_space_tracking(); // Throws

    // Update memory mapping if database file has grown
    if (new_file_size > m_alloc.get_baseline())
        m_alloc.remap(new_file_size); // Throws

    // If our last look at the file was when it
    // was empty, we may have to re-create the group
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
    for (size_t i=0; i<n; ++i) {
        const Table* t1 = get_table_by_ndx(i);
        const Table* t2 = g.get_table_by_ndx(i);
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


#ifdef TIGHTDB_DEBUG

void Group::Verify() const
{
    TIGHTDB_ASSERT(is_attached());

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
        for (size_t i = 0; i != n; ++i)
            get_table_by_ndx(i)->Verify();
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

#endif // TIGHTDB_DEBUG
