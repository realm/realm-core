#include <cerrno>
#include <new>
#include <algorithm>
#include <set>
#include <iostream>
#include <iomanip>
#include <fstream>

#include <tightdb/util/memory_stream.hpp>
#include <tightdb/util/thread.hpp>
#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/exceptions.hpp>
#include <tightdb/column_linkbase.hpp>
#include <tightdb/column_backlink.hpp>
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


void Group::reset_free_space_versions()
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


Table* Group::do_get_table(size_t table_ndx, DescMatcher desc_matcher)
{
    TIGHTDB_ASSERT(m_table_accessors.empty() || m_table_accessors.size() == m_tables.size());

    if (table_ndx >= m_tables.size())
        throw InvalidArgument();

    if (m_table_accessors.empty())
        m_table_accessors.resize(m_tables.size()); // Throws

    // Get table accessor from cache if it exists, else create
    Table* table = m_table_accessors[table_ndx];
    if (!table)
        table = create_table_accessor(table_ndx); // Throws

    if (desc_matcher) {
        typedef _impl::TableFriend tf;
        if (desc_matcher && !(*desc_matcher)(tf::get_spec(*table)))
            throw DescriptorMismatch();
    }

    return table;
}


Table* Group::do_get_table(StringData name, DescMatcher desc_matcher)
{
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        return 0;

    Table* table = do_get_table(table_ndx, desc_matcher); // Throws
    return table;
}


Table* Group::do_add_table(StringData name, DescSetter desc_setter, bool require_unique_name)
{
    if (require_unique_name && has_table(name))
        throw TableNameInUse();
    return do_add_table(name, desc_setter); // Throws
}


Table* Group::do_add_table(StringData name, DescSetter desc_setter)
{
    size_t table_ndx = create_table(name); // Throws
    Table* table = create_table_accessor(table_ndx); // Throws
    if (desc_setter)
        (*desc_setter)(*table); // Throws
    return table;
}


Table* Group::do_get_or_add_table(StringData name, DescMatcher desc_matcher,
                                  DescSetter desc_setter, bool* was_added)
{
    Table* table;
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found) {
        table = do_add_table(name, desc_setter); // Throws
    }
    else {
        table = do_get_table(table_ndx, desc_matcher); // Throws
    }
    if (was_added)
        *was_added = (table_ndx == not_found);
    return table;
}


size_t Group::create_table(StringData name)
{
    using namespace _impl;
    typedef TableFriend tf;
    ref_type ref = tf::create_empty_table(m_alloc); // Throws
    size_t ndx = m_tables.size();
    TIGHTDB_ASSERT(ndx == m_table_names.size());
    m_tables.add(ref); // Throws
    m_table_names.add(name); // Throws

    // Need slot for table accessor
    if (!m_table_accessors.empty())
        m_table_accessors.push_back(0); // Throws

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_alloc.get_replication())
        repl->insert_group_level_table(ndx, ndx, name); // Throws
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


void Group::remove_table(StringData name)
{
    TIGHTDB_ASSERT(is_attached());
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        throw NoSuchTable();
    remove_table(table_ndx); // Throws
}


void Group::remove_table(size_t table_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TableRef table = get_table(table_ndx);

    // In principle we could remove a table even if it is the target of link
    // columns of other tables, however, to do that, we would have to
    // automatically remove the "offending" link columns from those other
    // tables. Such a behaviour is deemed too obscure, and we shall therefore
    // require that a removed table does not contain foreigh origin backlink
    // columns.
    typedef _impl::TableFriend tf;
    if (tf::is_cross_table_link_target(*table))
        throw CrossTableLinkTarget();

    // There is no easy way for Group::TransactAdvancer to handle removal of
    // tables that contain foreign target table link columns, because that
    // involves removal of the corresponding backlink columns. For that reason,
    // we start by removing all columns, and that will generate individual
    // replication instructions for each column, with sufficient information for
    // Group::TransactAdvancer to handle them.
    size_t n = table->get_column_count();
    for (size_t i = n; i > 0; --i)
        table->remove_column(i-1);

    ref_type ref = m_tables.get(table_ndx);

    // If the specified table is not the last one, it will be removed by moving
    // that last table to the index of the removed one. The movement of the last
    // table requires link column adjustments.
    size_t last_ndx = m_tables.size() - 1;
    if (last_ndx != table_ndx) {
        TableRef last_table = get_table(last_ndx);
        const Spec& last_spec = tf::get_spec(*last_table);
        size_t last_num_cols = last_spec.get_column_count();
        set<Table*> opposite_tables;
        for (size_t i = 0; i < last_num_cols; ++i) {
            Table* opposite_table;
            ColumnType type = last_spec.get_column_type(i);
            if (tf::is_link_type(type)) {
                ColumnBase& col = tf::get_column(*last_table, i);
                TIGHTDB_ASSERT(dynamic_cast<ColumnLinkBase*>(&col));
                ColumnLinkBase& link_col = static_cast<ColumnLinkBase&>(col);
                opposite_table = &link_col.get_target_table();
            }
            else if (type == col_type_BackLink) {
                ColumnBase& col = tf::get_column(*last_table, i);
                TIGHTDB_ASSERT(dynamic_cast<ColumnBackLink*>(&col));
                ColumnBackLink& backlink_col = static_cast<ColumnBackLink&>(col);
                opposite_table = &backlink_col.get_origin_table();
            }
            else {
                continue;
            }
            opposite_tables.insert(opposite_table); // Throws
        }
        typedef set<Table*>::const_iterator iter;
        iter end = opposite_tables.end();
        for (iter i = opposite_tables.begin(); i != end; ++i) {
            Table* table_2 = *i;
            Spec& spec_2 = tf::get_spec(*table_2);
            size_t num_cols_2 = spec_2.get_column_count();
            for (size_t col_ndx_2 = 0; col_ndx_2 < num_cols_2; ++col_ndx_2) {
                ColumnType type_2 = spec_2.get_column_type(col_ndx_2);
                if (type_2 == col_type_Link || type_2 == col_type_LinkList ||
                    type_2 == col_type_BackLink) {
                    size_t table_ndx_2 = spec_2.get_opposite_link_table_ndx(col_ndx_2);
                    if (table_ndx_2 == last_ndx)
                        spec_2.set_opposite_link_table_ndx(col_ndx_2, table_ndx); // Throws
                }
            }
        }
        m_tables.set(table_ndx, m_tables.get(last_ndx)); // Throws
        m_table_names.set(table_ndx, m_table_names.get(last_ndx)); // Throws
        tf::set_ndx_in_parent(*last_table, table_ndx);
    }

    m_tables.erase(last_ndx); // Throws
    m_table_names.erase(last_ndx); // Throws

    m_table_accessors[table_ndx] = m_table_accessors[last_ndx];
    m_table_accessors.pop_back();
    tf::detach(*table);
    tf::unbind_ref(*table);

    // Destroy underlying node structure
    Array::destroy_deep(ref, m_alloc);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_alloc.get_replication())
        repl->erase_group_level_table(table_ndx, last_ndx+1); // Throws
#endif
}


void Group::rename_table(StringData name, StringData new_name, bool require_unique_name)
{
    TIGHTDB_ASSERT(is_attached());
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        throw NoSuchTable();
    rename_table(table_ndx, new_name, require_unique_name); // Throws
}


void Group::rename_table(size_t table_ndx, StringData new_name, bool require_unique_name)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_tables.size() == m_table_names.size());
    if (table_ndx >= m_tables.size())
        throw InvalidArgument();
    if (require_unique_name && has_table(new_name))
        throw TableNameInUse();
    m_table_names.set(table_ndx, new_name);
    if (Replication* repl = m_alloc.get_replication())
        repl->rename_group_level_table(table_ndx, new_name); // Throws
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
    Array top(Allocator::get_default());
    top.create(Array::type_HasRefs); // Throws
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
    m_alloc.reset_free_space_tracking(); // Throws

    size_t old_baseline = m_alloc.get_baseline();

    // Remap file if it has grown
    size_t new_file_size = out.get_file_size();
    if (new_file_size > old_baseline) {
        bool addr_changed = m_alloc.remap(new_file_size); // Throws
        // If the file was mapped to a new address, all array accessors must be
        // updated.
        if (addr_changed)
            old_baseline = 0;
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
        ConstTableRef table_1 = get_table(i); // Throws
        ConstTableRef table_2 = g.get_table(i); // Throws
        if (*table_1 != *table_2)
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

    virtual size_t next_block(const char*& begin, const char*& end) TIGHTDB_OVERRIDE
    {
        if (m_logs_begin == m_logs_end) {
            return 0;
        }
        begin = m_logs_begin->data();
        size_t result = m_logs_begin->size();
        end   = begin + result;
        ++m_logs_begin;
        return result;
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
// (See AccessorConsistencyLevels., it can however assume that replication
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

    bool insert_group_level_table(size_t table_ndx, size_t num_tables, StringData) TIGHTDB_NOEXCEPT
    {
        // for end-insertions, table_ndx will be equal to num_tables
        TIGHTDB_ASSERT(table_ndx <= num_tables);
        m_group.m_table_accessors.push_back(0); // Throws
        size_t last_ndx = num_tables;
        m_group.m_table_accessors[last_ndx] = m_group.m_table_accessors[table_ndx];
        m_group.m_table_accessors[table_ndx] = 0;
        if (Table* moved_table = m_group.m_table_accessors[last_ndx]) {
            typedef _impl::TableFriend tf;
            tf::mark(*moved_table);
            tf::mark_opposite_link_tables(*moved_table);
        }
        return true;
    }

    bool erase_group_level_table(size_t table_ndx, size_t num_tables) TIGHTDB_NOEXCEPT
    {
        TIGHTDB_ASSERT(table_ndx < num_tables);

        // Link target tables do not need to be considered here, since all
        // columns will already have been removed at this point.
        if (Table* table = m_group.m_table_accessors[table_ndx]) {
            typedef _impl::TableFriend tf;
            tf::detach(*table);
            tf::unbind_ref(*table);
        }

        size_t last_ndx = num_tables - 1;
        if (table_ndx < last_ndx) {
            if (Table* moved_table = m_group.m_table_accessors[last_ndx]) {
                typedef _impl::TableFriend tf;
                tf::mark(*moved_table);
                tf::mark_opposite_link_tables(*moved_table);
            }
            m_group.m_table_accessors[table_ndx] = m_group.m_table_accessors[last_ndx];
        }
        m_group.m_table_accessors.pop_back();

        return true;
    }

    bool rename_group_level_table(size_t, StringData) TIGHTDB_NOEXCEPT
    {
        // No-op since table names are properties of the group, and the group
        // accessor is always refreshed
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

    bool insert_empty_rows(size_t row_ndx, size_t num_rows, size_t last_row_ndx, bool unordered) TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        if (unordered) {
            // unordered insertion of multiple rows is not supported (and not needed) currently.
            TIGHTDB_ASSERT(num_rows == 1);
            if (m_table) {
                tf::adj_accessors_move(*m_table, last_row_ndx, row_ndx);
            }
        }
        else {
            if (m_table)
                tf::adj_accessors_insert_rows(*m_table, row_ndx, num_rows);
        }
        return true;
    }

    bool erase_rows(size_t row_ndx, size_t num_rows, size_t tbl_sz, bool unordered) TIGHTDB_NOEXCEPT
    {
        if (unordered) {
            // unordered removal of multiple rows is not supported (and not needed) currently.
            TIGHTDB_ASSERT(num_rows == 1);
            typedef _impl::TableFriend tf;
            if (m_table)
                tf::adj_accessors_move(*m_table, row_ndx, tbl_sz);
        }
        else {
            typedef _impl::TableFriend tf;
            if (m_table)
                while (num_rows--) {
                    tf::adj_accessors_erase_row(*m_table, row_ndx + num_rows);
                }
        }
        return true;
    }

    bool clear_table() TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::adj_acc_clear_root_table(*m_table);
        return true;
    }

    bool insert_int(size_t col_ndx, size_t row_ndx, size_t tbl_sz, int_fast64_t) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_bool(size_t col_ndx, size_t row_ndx, size_t tbl_sz, bool) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_float(size_t col_ndx, size_t row_ndx, size_t tbl_sz, float) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_double(size_t col_ndx, size_t row_ndx, size_t tbl_sz, double) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_string(size_t col_ndx, size_t row_ndx, size_t tbl_sz, StringData) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_binary(size_t col_ndx, size_t row_ndx, size_t tbl_sz, BinaryData) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_date_time(size_t col_ndx, size_t row_ndx, size_t tbl_sz, DateTime) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_table(size_t col_ndx, size_t row_ndx, size_t tbl_sz) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_mixed(size_t col_ndx, size_t row_ndx, size_t tbl_sz, const Mixed&) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool insert_link(size_t col_ndx, size_t row_ndx, size_t tbl_sz, size_t) TIGHTDB_NOEXCEPT
    {
        // The marking dirty of the target table is handled by
        // insert_empty_rows() regardless of whether the link column is the
        // first column or not.
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
       return true;
    }

    bool insert_link_list(size_t col_ndx, size_t row_ndx, size_t tbl_sz) TIGHTDB_NOEXCEPT
    {
        if (col_ndx == 0)
            insert_empty_rows(row_ndx, 1, tbl_sz, false);
        return true;
    }

    bool row_insert_complete() TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool set_int(size_t, size_t, int_fast64_t) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool set_bool(size_t, size_t, bool) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool set_float(size_t, size_t, float) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool set_double(size_t, size_t, double) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool set_string(size_t, size_t, StringData) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool set_binary(size_t, size_t, BinaryData) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool set_date_time(size_t, size_t, DateTime) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
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
        // the link-target table accessor exists if, and only if the origin
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
        return true; // No-op
    }

    bool optimize_table() TIGHTDB_NOEXCEPT
    {
        return true; // No-op
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

    bool insert_column(size_t col_ndx, DataType, StringData)
    {
        if (m_table) {
            typedef _impl::TableFriend tf;
            InsertColumnUpdater updater(col_ndx);
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);
        }
        typedef _impl::DescriptorFriend df;
        if (m_desc)
            df::adj_insert_column(*m_desc, col_ndx);
        return true;
    }

    bool insert_link_column(size_t col_ndx, DataType, StringData, size_t link_target_table_ndx, size_t)
    {
        if (m_table) {
            typedef _impl::TableFriend tf;
            InsertColumnUpdater updater(col_ndx);
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);

            // See comments on link handling in TransactAdvancer::set_link().
            TableRef target = m_group.get_table(link_target_table_ndx); // Throws
            tf::adj_add_column(*target); // Throws
            tf::mark(*target);
        }
        typedef _impl::DescriptorFriend df;
        if (m_desc)
            df::adj_insert_column(*m_desc, col_ndx);
        return true;
    }

    bool erase_column(size_t col_ndx)
    {
        if (m_table) {
            typedef _impl::TableFriend tf;
            EraseColumnUpdater updater(col_ndx);
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);
        }
        typedef _impl::DescriptorFriend df;
        if (m_desc)
            df::adj_erase_column(*m_desc, col_ndx);
        return true;
    }

    bool erase_link_column(size_t col_ndx, size_t link_target_table_ndx, size_t backlink_col_ndx)
    {
        if (m_table) {
            typedef _impl::TableFriend tf;

            // For link columns we need to handle the backlink column first in
            // case the target table is the same as the origin table (because
            // the backlink column occurs after regular columns.) Also see
            // comments on link handling in TransactAdvancer::set_link().
            TableRef target = m_group.get_table(link_target_table_ndx); // Throws
            tf::adj_erase_column(*target, backlink_col_ndx); // Throws
            tf::mark(*target);

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
        return true; // No-op
    }

    bool add_search_index(size_t) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool select_link_list(size_t col_ndx, size_t) TIGHTDB_NOEXCEPT
    {
        // See comments on link handling in TransactAdvancer::set_link().
        typedef _impl::TableFriend tf;
        if (m_table) {
            if (Table* target = tf::get_link_target_table_accessor(*m_table, col_ndx))
                tf::mark(*target);
        }
        return true; // No-op
    }

    bool link_list_set(size_t, size_t) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool link_list_insert(size_t, size_t) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool link_list_move(size_t, size_t) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool link_list_erase(size_t) TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

    bool link_list_clear() TIGHTDB_NOEXCEPT
    {
        return true; // No-op
    }

private:
    Group& m_group;
    TableRef m_table;
    DescriptorRef m_desc;
    const size_t* m_desc_path_begin;
    const size_t* m_desc_path_end;
};

void Group::refresh_dirty_accessors()
{
    m_top.get_alloc().bump_global_version();

    // Refresh all remaining dirty table accessors
    size_t num_tables = m_table_accessors.size();
    for (size_t table_ndx = 0; table_ndx != num_tables; ++table_ndx) {
        if (Table* table = m_table_accessors[table_ndx]) {
            typedef _impl::TableFriend tf;
            tf::set_ndx_in_parent(*table, table_ndx);
            if (tf::is_marked(*table)) {
                tf::refresh_accessor_tree(*table); // Throws
                tf::bump_version(*table);
            }
        }
    }
}


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

    m_alloc.reset_free_space_tracking(); // Throws

    // Update memory mapping if database file has grown
    if (new_file_size > m_alloc.get_baseline()) {
        bool addr_changed = m_alloc.remap(new_file_size); // Throws
        // If the file was mapped to a new address, all array accessors must be
        // updated.
        if (addr_changed)
            mark_all_table_accessors();
    }

    init_from_ref(new_top_ref);
    refresh_dirty_accessors();
}






// Here goes the class which specifies how instructions are to be reversed.

namespace {
// First, to help, we use a special variant of TrivialReplication to gather the
// reversed log:

class ReverseReplication : public TrivialReplication {
public:
    ReverseReplication(const std::string& database_file) : TrivialReplication(database_file)
    {
        prepare_to_write();
    }

    void handle_transact_log(const char*, std::size_t, version_type)
    {
        // we should never get here...
        TIGHTDB_ASSERT(false);
    }
};

} // anonymous namespace

class Group::TransactReverser : public NullHandler  {
public:

    TransactReverser(ReverseReplication& encoder) :
        m_encoder(encoder), current_insn_start(0),
        m_pending_table_select(false), m_pending_descriptor_select(false)
    {
    }

    // override only the instructions which need to be reversed. (NullHandler class provides
    // default implementatins for the rest)
    bool select_table(std::size_t group_level_ndx, size_t levels, const size_t* path)
    {
        sync_table();
        // note that for select table, 'levels' is encoded before 'group_level_ndx'
        // despite the order of arguments
        m_encoder.simple_cmd(Replication::instr_SelectTable, util::tuple(levels, group_level_ndx));
        char* buf;
        m_encoder.transact_log_reserve(&buf, 2*levels*Replication::max_enc_bytes_per_int);
        for (size_t i = 0; i != levels; ++i) {
            buf = m_encoder.encode_int(buf, path[i*2+0]);
            buf = m_encoder.encode_int(buf, path[i*2+1]);
        }
        m_encoder.transact_log_advance(buf);

        m_pending_table_select = true;
        m_pending_ts_insn = get_inst();
        return true;
    }

    bool select_descriptor(size_t levels, const size_t* path)
    {
        sync_descriptor();
        m_encoder.simple_cmd(Replication::instr_SelectDescriptor, util::tuple(levels));
        char* buf;
        m_encoder.transact_log_reserve(&buf, levels*Replication::max_enc_bytes_per_int);
        for (size_t i = 0; i != levels; ++i) {
            buf = m_encoder.encode_int(buf, path[i]);
        }
        m_encoder.transact_log_advance(buf);

        m_pending_descriptor_select = true;
        m_pending_ds_insn = get_inst();
        return true;
    }

    bool insert_group_level_table(std::size_t table_ndx, std::size_t num_tables, StringData)
    {
        m_encoder.simple_cmd(Replication::instr_EraseGroupLevelTable, util::tuple(table_ndx, num_tables + 1));
        append_instruction();
        return true;
    }

    bool erase_group_level_table(std::size_t table_ndx, std::size_t num_tables)
    {
        m_encoder.simple_cmd(Replication::instr_InsertGroupLevelTable, util::tuple(table_ndx, num_tables));
        m_encoder.string_value(0, 0);
        append_instruction();
        return true;
    }

    bool insert_empty_rows(std::size_t idx, std::size_t num_rows, std::size_t tbl_sz, bool unordered)
    {
        m_encoder.simple_cmd(Replication::instr_EraseRows, util::tuple(idx, num_rows, tbl_sz, unordered));
        append_instruction();
        return true;
    }

    bool erase_rows(std::size_t idx, std::size_t num_rows, std::size_t tbl_sz, bool unordered)
    {
        m_encoder.simple_cmd(Replication::instr_InsertEmptyRows, util::tuple(idx, num_rows, tbl_sz, unordered));
        append_instruction();
        return true;
    }

    // helper function, shared by insert_xxx
    bool insert(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz)
    {
        if (col_idx == 0) {
            m_encoder.simple_cmd(Replication::instr_EraseRows, util::tuple(row_idx, 1, tbl_sz, false));
            append_instruction();
        }
        return true;
    }
    bool insert_int(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, int_fast64_t)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_bool(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, bool)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_float(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, float)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_double(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, double)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_string(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, StringData)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_binary(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, BinaryData)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_date_time(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, DateTime)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_table(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_mixed(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, const Mixed&)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_link(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz, std::size_t)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool insert_link_list(std::size_t col_idx, std::size_t row_idx, std::size_t tbl_sz)
    {
        return insert(col_idx, row_idx, tbl_sz);
    }

    bool set_table(size_t col_ndx, size_t row_ndx)
    {
        m_encoder.simple_cmd(Replication::instr_SetTable, util::tuple(col_ndx, row_ndx));
        append_instruction();
        return true;
    }

    bool set_mixed(size_t col_ndx, size_t row_ndx, const Mixed& value)
    {
        m_encoder.mixed_cmd(Replication::instr_SetMixed, col_ndx, row_ndx, value);
        append_instruction();
        return true;
    }

    bool set_link(size_t col_ndx, size_t row_ndx, size_t value)
    {
        m_encoder.simple_cmd(Replication::instr_SetLink, util::tuple(col_ndx, row_ndx, value));
        append_instruction();
        return true;
    }

    bool insert_link_column(std::size_t col_idx, DataType, StringData,
                            std::size_t target_table_idx, std::size_t backlink_col_ndx)
    {
        m_encoder.simple_cmd(Replication::instr_EraseLinkColumn, util::tuple(col_idx, target_table_idx, backlink_col_ndx));
        append_instruction();
        return true;
    }

    bool erase_link_column(std::size_t col_idx, std::size_t target_table_idx,
                           std::size_t backlink_col_idx)
    {
        m_encoder.simple_cmd(Replication::instr_InsertLinkColumn, util::tuple(col_idx, int(DataType())));
        m_encoder.string_value(0, 0);
        m_encoder.append_num(target_table_idx);
        m_encoder.append_num(backlink_col_idx);
        append_instruction();
        return true;
    }

    bool insert_column(std::size_t col_idx, DataType, StringData)
    {
        m_encoder.simple_cmd(Replication::instr_EraseColumn, util::tuple(col_idx));
        // m_encoder.erase_column(col_idx);
        append_instruction();
        return true;
    }

    bool erase_column(std::size_t col_idx)
    {
        //m_encoder.insert_column(col_idx, DataType(), StringData());
        m_encoder.simple_cmd(Replication::instr_InsertColumn, util::tuple(col_idx, int(DataType())));
        m_encoder.string_value(0, 0);
        append_instruction();
        return true;
    }

    bool select_link_list(size_t col_ndx, size_t row_ndx)
    {
        //m_encoder.select_link_list(col_ndx, row_ndx);
        m_encoder.simple_cmd(Replication::instr_SelectLinkList, util::tuple(col_ndx, row_ndx));
        append_instruction();
        return true;
    }

    void execute(Group& group);

private:
    ReverseReplication& m_encoder;
    struct Insn { size_t start; size_t end; };
    std::vector<Insn> m_instructions;
    size_t current_insn_start;
    bool m_pending_table_select;
    bool m_pending_descriptor_select;
    Insn m_pending_ts_insn;
    Insn m_pending_ds_insn;

    Insn get_inst() {
        Insn inst;
        inst.start = current_insn_start;
        current_insn_start = m_encoder.transact_log_size();
        inst.end = current_insn_start;
        return inst;
    }

    void append_instruction() {
        m_instructions.push_back(get_inst());
    }

    void append_instruction(Insn inst) {
        m_instructions.push_back(inst);
    }

    void sync_descriptor() {
        if (m_pending_descriptor_select) {
            m_pending_descriptor_select = false;
            append_instruction(m_pending_ds_insn);
        }
    }

    void sync_table() {
        sync_descriptor();
        if (m_pending_table_select) {
            m_pending_table_select = false;
            append_instruction(m_pending_ts_insn);
        }
    }

    class ReversedInputStream : public Replication::InputStream {
    public:
        ReversedInputStream(const char* buffer, std::vector<Insn>& instruction_order)
            : m_buffer(buffer), m_instruction_order(instruction_order)
        {
            m_current = m_instruction_order.size();
        }
        virtual size_t next_block(const char*& begin, const char*& end)
        {
            if (m_current != 0) {
                m_current--;
                begin = m_buffer + m_instruction_order[m_current].start;
                end   = m_buffer + m_instruction_order[m_current].end;
                return end-begin;
            }
            else
                return 0;
        }
    private:
        const char* m_buffer;
        std::vector<Insn>& m_instruction_order;
        size_t m_current;
    };
};

void Group::TransactReverser::execute(Group& group)
{
    // push any pending select_table or select_descriptor into the buffer:
    sync_table();

    // then execute the instructions in the transformed order
    ReversedInputStream reversed_log(m_encoder.m_transact_log_buffer.data(), m_instructions);
    Replication::TransactLogParser parser(reversed_log);
    TransactAdvancer advancer(group);
    parser.parse(advancer);
}


void Group::reverse_transact(ref_type new_top_ref, const BinaryData& log)
{
    MultiLogInputStream in(&log, (&log)+1);
    Replication::TransactLogParser parser(in);
    ReverseReplication encoder("reversal");
    TransactReverser reverser(encoder);
    parser.parse(reverser);
    reverser.execute(*this);

    // restore group internal arrays to state before transaction (rollback state)
    init_from_ref(new_top_ref);

    // propagate restoration to all relevant accessors:
    refresh_dirty_accessors();
}

#endif // TIGHTDB_ENABLE_REPLICATION


#ifdef TIGHTDB_DEBUG

namespace {

class MemUsageVerifier: public Array::MemUsageHandler {
public:
    MemUsageVerifier(ref_type ref_begin, ref_type immutable_ref_end, ref_type mutable_ref_end, ref_type baseline):
        m_ref_begin(ref_begin),
        m_immutable_ref_end(immutable_ref_end),
        m_mutable_ref_end(mutable_ref_end),
        m_baseline(baseline)
    {
    }
    void add_immutable(ref_type ref, size_t size)
    {
        TIGHTDB_ASSERT(ref  % 8 == 0); // 8-byte alignment
        TIGHTDB_ASSERT(size % 8 == 0); // 8-byte alignment
        TIGHTDB_ASSERT(size > 0);
        TIGHTDB_ASSERT(ref >= m_ref_begin);
        TIGHTDB_ASSERT(size <= m_immutable_ref_end - ref);
        Chunk chunk;
        chunk.ref  = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add_mutable(ref_type ref, size_t size)
    {
        TIGHTDB_ASSERT(ref  % 8 == 0); // 8-byte alignment
        TIGHTDB_ASSERT(size % 8 == 0); // 8-byte alignment
        TIGHTDB_ASSERT(size > 0);
        TIGHTDB_ASSERT(ref >= m_immutable_ref_end);
        TIGHTDB_ASSERT(size <= m_mutable_ref_end - ref);
        Chunk chunk;
        chunk.ref  = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add(ref_type ref, size_t size)
    {
        TIGHTDB_ASSERT(ref  % 8 == 0); // 8-byte alignment
        TIGHTDB_ASSERT(size % 8 == 0); // 8-byte alignment
        TIGHTDB_ASSERT(size > 0);
        TIGHTDB_ASSERT(ref >= m_ref_begin);
        TIGHTDB_ASSERT(size <= (ref < m_baseline ? m_immutable_ref_end : m_mutable_ref_end) - ref);
        Chunk chunk;
        chunk.ref  = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add(const MemUsageVerifier& verifier)
    {
        m_chunks.insert(m_chunks.end(), verifier.m_chunks.begin(), verifier.m_chunks.end());
    }
    void handle(ref_type ref, size_t allocated, size_t) TIGHTDB_OVERRIDE
    {
        add(ref, allocated);
    }
    void canonicalize()
    {
        // Sort the chunks in order of increasing ref, then merge adjacent
        // chunks while checking that there is no overlap
        typedef vector<Chunk>::iterator iter;
        iter i_1 = m_chunks.begin(), end = m_chunks.end();
        iter i_2 = i_1;
        sort(i_1, end);
        if (i_1 != end) {
            while (++i_2 != end) {
                ref_type prev_ref_end = i_1->ref + i_1->size;
                TIGHTDB_ASSERT(prev_ref_end <= i_2->ref);
                if (i_2->ref == prev_ref_end) {
                    i_1->size += i_2->size; // Merge
                }
                else {
                    *++i_1 = *i_2;
                }
            }
            m_chunks.erase(i_1 + 1, end);
        }
    }
    void clear()
    {
        m_chunks.clear();
    }
    void check_total_coverage()
    {
        TIGHTDB_ASSERT(m_chunks.size() == 1);
        TIGHTDB_ASSERT(m_chunks.front().ref == m_ref_begin);
        TIGHTDB_ASSERT(m_chunks.front().size == m_mutable_ref_end - m_ref_begin);
    }
private:
    struct Chunk {
        ref_type ref;
        size_t size;
        bool operator<(const Chunk& c) const
        {
            return ref < c.ref;
        }
    };
    vector<Chunk> m_chunks;
    ref_type m_ref_begin, m_immutable_ref_end, m_mutable_ref_end, m_baseline;
};

} // anonymous namespace

void Group::Verify() const
{
    TIGHTDB_ASSERT(is_attached());

    m_alloc.Verify();

    // Verify tables
    {
        size_t n = m_tables.size();
        for (size_t i = 0; i != n; ++i) {
            ConstTableRef table = get_table(i);
            TIGHTDB_ASSERT(table->get_index_in_group() == i);
            table->Verify();
        }
    }

    size_t logical_file_size = to_size_t(m_top.get(2) / 2);
    size_t ref_begin = sizeof (SlabAlloc::Header);
    ref_type immutable_ref_end = logical_file_size;
    ref_type mutable_ref_end = m_alloc.get_total_size();
    ref_type baseline = m_alloc.get_baseline();

    // Check the concistency of the allocation of used memory
    MemUsageVerifier mem_usage_1(ref_begin, immutable_ref_end, mutable_ref_end, baseline);
    m_top.report_memory_usage(mem_usage_1);
    mem_usage_1.canonicalize();

    // Check concistency of the allocation of the immutable memory that was
    // marked as free before the file was opened.
    MemUsageVerifier mem_usage_2(ref_begin, immutable_ref_end, mutable_ref_end, baseline);
    if (m_free_positions.is_attached()) {
        size_t n = m_free_positions.size();
        TIGHTDB_ASSERT(n == m_free_lengths.size());
        if (m_free_versions.is_attached())
            TIGHTDB_ASSERT(n == m_free_versions.size());
        for (size_t i = 0; i != n; ++i) {
            ref_type ref  = to_ref(m_free_positions.get(i));
            size_t size = to_size_t(m_free_lengths.get(i));
            mem_usage_2.add_immutable(ref, size);
        }
        mem_usage_2.canonicalize();
        mem_usage_1.add(mem_usage_2);
        mem_usage_1.canonicalize();
        mem_usage_2.clear();
    }

    // Check the concistency of the allocation of the immutable memory that has
    // been marked as free after the file was opened
    {
        typedef SlabAlloc::chunks::const_iterator iter;
        iter end = m_alloc.m_free_read_only.end();
        for (iter i = m_alloc.m_free_read_only.begin(); i != end; ++i)
            mem_usage_2.add_immutable(i->ref, i->size);
    }
    mem_usage_2.canonicalize();
    mem_usage_1.add(mem_usage_2);
    mem_usage_1.canonicalize();
    mem_usage_2.clear();

    // Check the concistency of the allocation of the mutable memory that has
    // been marked as free
    {
        typedef SlabAlloc::chunks::const_iterator iter;
        iter end = m_alloc.m_free_space.end();
        for (iter i = m_alloc.m_free_space.begin(); i != end; ++i)
            mem_usage_2.add_mutable(i->ref, i->size);
    }
    mem_usage_2.canonicalize();
    mem_usage_1.add(mem_usage_2);
    mem_usage_1.canonicalize();
    mem_usage_2.clear();

    // Due to a current problem with the baseline not reflecting the logical
    // file size, but the physical file size, there is a potential gap of
    // unusable ref-space between the logical file size and the baseline. We
    // need to take that into account here.
    TIGHTDB_ASSERT(immutable_ref_end <= baseline);
    if (immutable_ref_end < baseline) {
        ref_type ref = immutable_ref_end;
        size_t size = baseline - immutable_ref_end;
        mem_usage_1.add_mutable(ref, size);
        mem_usage_1.canonicalize();
    }

    // At this point we have account for all memory managed by the slab
    // allocator
    mem_usage_1.check_total_coverage();
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
        ConstTableRef table = get_table(i);
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
