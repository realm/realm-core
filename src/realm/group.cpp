#include <new>
#include <algorithm>
#include <set>
#include <iostream>
#include <iomanip>
#include <fstream>

#include <realm/util/file_mapper.hpp>
#include <realm/util/memory_stream.hpp>
#include <realm/util/thread.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/utilities.hpp>
#include <realm/exceptions.hpp>
#include <realm/column_linkbase.hpp>
#include <realm/column_backlink.hpp>
#include <realm/group_writer.hpp>
#include <realm/group.hpp>
#include <realm/replication.hpp>

using namespace realm;
using namespace realm::util;

namespace {

class Initialization {
public:
    Initialization()
    {
        realm::cpuid_init();
    }
};

Initialization initialization;

} // anonymous namespace


void Group::upgrade_file_format()
{
    REALM_ASSERT(is_attached());

    // SlabAlloc::validate_buffer() ensures this
    REALM_ASSERT_RELEASE(m_alloc.get_committed_file_format() == 2);
    REALM_ASSERT_RELEASE(m_alloc.m_file_format == 2);
    REALM_ASSERT_RELEASE(SlabAlloc::library_file_format == 3);

    for (size_t t = 0; t < m_tables.size(); t++) {
        TableRef table = get_table(t);
        table->upgrade_file_format();
    }
}


int Group::get_file_format() const noexcept
{
    return m_alloc.m_file_format;
}


void Group::set_file_format(int file_format) noexcept
{
    m_alloc.m_file_format = file_format;
}


int Group::get_committed_file_format() const noexcept
{
    return m_alloc.get_committed_file_format();
}


void Group::open(const std::string& file_path, const char* encryption_key, OpenMode mode)
{
    if (is_attached() || m_is_shared)
        throw LogicError(LogicError::wrong_group_state);

    SlabAlloc::Config cfg;
    cfg.read_only = mode == mode_ReadOnly;
    cfg.no_create = mode == mode_ReadWriteNoCreate;
    cfg.encryption_key = encryption_key;
    ref_type top_ref = m_alloc.attach_file(file_path, cfg); // Throws

    // Make all dynamically allocated memory (space beyond the attached file) as
    // available free-space.
    reset_free_space_tracking(); // Throws
    SlabAlloc::DetachGuard dg(m_alloc);
    attach(top_ref); // Throws
    dg.release(); // Do not detach allocator from file

    // SlabAlloc::validate_buffer() ensures this.
    REALM_ASSERT_RELEASE(m_alloc.m_file_format == SlabAlloc::library_file_format);
}


void Group::open(BinaryData buffer, bool take_ownership)
{
    REALM_ASSERT(buffer.data());

    if (is_attached() || m_is_shared)
        throw LogicError(LogicError::wrong_group_state);

    // FIXME: Why do we have to pass a const-unqualified data pointer
    // to SlabAlloc::attach_buffer()? It seems unnecessary given that
    // the data is going to become the immutable part of its managed
    // memory.
    char* data = const_cast<char*>(buffer.data());
    ref_type top_ref = m_alloc.attach_buffer(data, buffer.size()); // Throws

    // Make all dynamically allocated memory (space beyond the attached file) as
    // available free-space.
    reset_free_space_tracking(); // Throws

    SlabAlloc::DetachGuard dg(m_alloc);
    attach(top_ref); // Throws
    dg.release(); // Do not detach allocator from file
    if (take_ownership)
        m_alloc.own_buffer();

    // SlabAlloc::validate_buffer() ensures this.
    REALM_ASSERT_RELEASE(m_alloc.m_file_format == SlabAlloc::library_file_format);
}


Group::~Group() noexcept
{
    // If this group accessor is detached at this point in time, it is either
    // because it is SharedGroup::m_group (m_is_shared), or it is a free-stading
    // group accessor that was never successfully opened.
    if (!m_top.is_attached())
        return;

    // Free-standing group accessor

    detach_table_accessors();

    // Just allow the allocator to release all memory in one chunk without
    // having to traverse the entire tree first
    m_alloc.detach();
}


void Group::remap_and_update_refs(ref_type new_top_ref, size_t new_file_size)
{
    size_t old_baseline = m_alloc.get_baseline();

    if (new_file_size > old_baseline) {
        m_alloc.remap(new_file_size); // Throws
    }

    m_alloc.invalidate_cache();
    update_refs(new_top_ref, old_baseline);
}


void Group::attach(ref_type top_ref)
{
    REALM_ASSERT(!m_top.is_attached());

    // If this function throws, it must leave the group accesor in a the
    // unattached state.

    m_tables.detach();
    m_table_names.detach();

    bool create_empty_group = (top_ref == 0);
    if (create_empty_group) {
        m_top.create(Array::type_HasRefs); // Throws
        _impl::DeepArrayDestroyGuard dg_top(&m_top);
        {
            m_table_names.create(); // Throws
            _impl::DestroyGuard<ArrayString> dg(&m_table_names);
            m_top.add(m_table_names.get_ref()); // Throws
            dg.release();
        }
        {
            m_tables.create(Array::type_HasRefs); // Throws
            _impl::DestroyGuard<ArrayInteger> dg(&m_tables);
            m_top.add(m_tables.get_ref()); // Throws
            dg.release();
        }
        size_t initial_logical_file_size = sizeof (SlabAlloc::Header);
        m_top.add(1 + 2*initial_logical_file_size); // Throws
        dg_top.release();
    }
    else {
        m_top.init_from_ref(top_ref);
        size_t top_size = m_top.size();
        static_cast<void>(top_size);

        // FIXME: Use a future REALM_ASSERT_EX
        if (top_size < 8) {
            REALM_ASSERT_11(top_size, ==, 3, ||, top_size, ==, 5, ||, top_size, ==, 7);
        }
        else {
            REALM_ASSERT_3(top_size, ==, 8);
        }

        m_table_names.init_from_parent();
        m_tables.init_from_parent();

        // The 3rd slot in m_top is `1 + 2 * logical_file_size`, and the logical
        // file size must never exceed actual file size.
        REALM_ASSERT_3(size_t(m_top.get(2) / 2), <=, m_alloc.get_baseline());
    }
}


void Group::detach() noexcept
{
    detach_table_accessors();
    m_table_accessors.clear();

    m_table_names.detach();
    m_tables.detach();
    m_top.detach(); // This marks the group accessor as detached
}


void Group::attach_shared(ref_type new_top_ref, size_t new_file_size)
{
    REALM_ASSERT_3(new_top_ref, <, new_file_size);
    REALM_ASSERT(!is_attached());

    // Make all dynamically allocated memory (space beyond the attached file) as
    // available free-space.
    reset_free_space_tracking(); // Throws

    // Update memory mapping if database file has grown
    if (new_file_size > m_alloc.get_baseline())
        m_alloc.remap(new_file_size); // Throws

    attach(new_top_ref); // Throws
}


void Group::detach_table_accessors() noexcept
{
    typedef table_accessors::const_iterator iter;
    iter end = m_table_accessors.end();
    for (iter i = m_table_accessors.begin(); i != end; ++i) {
        if (Table* t = *i) {
            typedef _impl::TableFriend tf;
            tf::detach(*t);
            tf::unbind_ptr(*t);
        }
    }
}


Table* Group::do_get_table(size_t table_ndx, DescMatcher desc_matcher)
{
    REALM_ASSERT(m_table_accessors.empty() || m_table_accessors.size() == m_tables.size());

    if (table_ndx >= m_tables.size())
        throw LogicError(LogicError::table_index_out_of_range);

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


Table* Group::do_insert_table(size_t table_ndx, StringData name, DescSetter desc_setter,
                              bool require_unique_name)
{
    if (require_unique_name && has_table(name))
        throw TableNameInUse();
    return do_insert_table(table_ndx, name, desc_setter); // Throws
}


Table* Group::do_insert_table(size_t table_ndx, StringData name, DescSetter desc_setter)
{
    if (table_ndx > m_tables.size())
        throw LogicError(LogicError::table_index_out_of_range);
    create_and_insert_table(table_ndx, name); // Throws
    Table* table = do_get_table(table_ndx, nullptr); // Throws
    if (desc_setter)
        (*desc_setter)(*table); // Throws
    return table;
}

Table* Group::do_get_or_insert_table(size_t table_ndx, StringData name, DescMatcher desc_matcher,
                                     DescSetter desc_setter, bool* was_added)
{
    Table* table;
    size_t existing_table_ndx = m_table_names.find_first(name);
    if (existing_table_ndx == not_found) {
        table = do_insert_table(table_ndx, name, desc_setter); // Throws
        if (was_added)
            *was_added = true;
    }
    else {
        table = do_get_table(existing_table_ndx, desc_matcher); // Throws
        if (was_added)
            *was_added = false;
    }
    return table;
}


Table* Group::do_get_or_add_table(StringData name, DescMatcher desc_matcher,
                                  DescSetter desc_setter, bool* was_added)
{
    Table* table;
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found) {
        table = do_insert_table(m_tables.size(), name, desc_setter); // Throws
    }
    else {
        table = do_get_table(table_ndx, desc_matcher); // Throws
    }
    if (was_added)
        *was_added = (table_ndx == not_found);
    return table;
}


void Group::create_and_insert_table(size_t table_ndx, StringData name)
{
    if (REALM_UNLIKELY(name.size() > max_table_name_length))
        throw LogicError(LogicError::table_name_too_long);

    using namespace _impl;
    typedef TableFriend tf;
    ref_type ref = tf::create_empty_table(m_alloc); // Throws
    REALM_ASSERT_3(m_tables.size(), ==, m_table_names.size());
    size_t prior_size = m_tables.size();
    m_tables.insert(table_ndx, ref); // Throws
    m_table_names.insert(table_ndx, name); // Throws

    // Need slot for table accessor
    if (!m_table_accessors.empty()) {
        m_table_accessors.insert(m_table_accessors.begin() + table_ndx, nullptr); // Throws
    }

    update_table_indices([&](size_t old_table_ndx) {
        if (old_table_ndx >= table_ndx) {
            return old_table_ndx + 1;
        }
        return old_table_ndx;
    });

    if (Replication* repl = m_alloc.get_replication())
        repl->insert_group_level_table(table_ndx, prior_size, name); // Throws
}


Table* Group::create_table_accessor(size_t table_ndx)
{
    REALM_ASSERT(m_table_accessors.empty() || table_ndx < m_table_accessors.size());

    if (m_table_accessors.empty())
        m_table_accessors.resize(m_tables.size()); // Throws

    // Whenever a table has a link column, the column accessor must be set up to
    // refer to the target table accessor, so the target table accessor needs to
    // be created too, if it does not already exist. This, of course, applies
    // recursively, and it applies to the opposide direction of links too (from
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
    tf::bind_ptr(*table);

    tf::mark(*table);
    m_table_accessors[table_ndx] = table;
    tf::complete_accessor(*table); // Throws
    tf::unmark(*table);
    return table;
}


void Group::remove_table(StringData name)
{
    REALM_ASSERT(is_attached());
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        throw NoSuchTable();
    remove_table(table_ndx); // Throws
}


void Group::remove_table(size_t table_ndx)
{
    REALM_ASSERT(is_attached());
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
    // we start by removing all columns, which will generate individual
    // replication instructions for each column removal with sufficient
    // information for Group::TransactAdvancer to handle them.
    size_t n = table->get_column_count();
    for (size_t i = n; i > 0; --i)
        table->remove_column(i-1);

    ref_type ref = m_tables.get(table_ndx);

    // If the specified table is not the last one, it will be removed by moving
    // that last table to the index of the removed one. The movement of the last
    // table requires link column adjustments.
    size_t last_ndx = m_tables.size() - 1;
    if (last_ndx != table_ndx) {
        m_tables.set(table_ndx, m_tables.get(last_ndx)); // Throws
        m_table_names.set(table_ndx, m_table_names.get(last_ndx)); // Throws
    }

    m_tables.erase(last_ndx); // Throws
    m_table_names.erase(last_ndx); // Throws

    m_table_accessors[table_ndx] = m_table_accessors[last_ndx];
    m_table_accessors.pop_back();

    if (last_ndx != table_ndx) {
        update_table_indices([&](size_t old_table_ndx) {
            if (old_table_ndx == last_ndx) {
                return table_ndx;
            }
            return old_table_ndx;
        });
    }

    tf::detach(*table);
    tf::unbind_ptr(*table);

    // Destroy underlying node structure
    Array::destroy_deep(ref, m_alloc);

    if (Replication* repl = m_alloc.get_replication())
        repl->erase_group_level_table(table_ndx, last_ndx+1); // Throws
}


void Group::rename_table(StringData name, StringData new_name, bool require_unique_name)
{
    REALM_ASSERT(is_attached());
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        throw NoSuchTable();
    rename_table(table_ndx, new_name, require_unique_name); // Throws
}


void Group::rename_table(size_t table_ndx, StringData new_name, bool require_unique_name)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(m_tables.size(), ==, m_table_names.size());
    if (table_ndx >= m_tables.size())
        throw LogicError(LogicError::table_index_out_of_range);
    if (require_unique_name && has_table(new_name))
        throw TableNameInUse();
    m_table_names.set(table_ndx, new_name);
    if (Replication* repl = m_alloc.get_replication())
        repl->rename_group_level_table(table_ndx, new_name); // Throws
}


void Group::move_table(size_t from_ndx, size_t to_ndx)
{
    REALM_ASSERT_3(from_ndx, !=, to_ndx);
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(m_tables.size(), ==, m_table_names.size());
    if (from_ndx >= m_tables.size())
        throw LogicError(LogicError::table_index_out_of_range);
    if (to_ndx >= m_tables.size())
        throw LogicError(LogicError::table_index_out_of_range);

    // Tables between from_ndx and to_ndx change their indices,
    // so link columns have to be adjusted (similar to remove_table).

    // Build a map of all table indices that are going to change:
    std::map<size_t, size_t> moves; // from -> to
    moves[from_ndx] = to_ndx;
    if (from_ndx < to_ndx) {
        // Move up:
        for (size_t i = from_ndx + 1; i <= to_ndx; ++i) {
            moves[i] = i - 1;
        }
    }
    else if (from_ndx > to_ndx) {
        // Move down:
        for (size_t i = to_ndx; i < from_ndx; ++i) {
            moves[i] = i + 1;
        }
    }

    // Move entries in internal data structures.
    m_tables.move_rotate(from_ndx, to_ndx);
    m_table_names.move_rotate(from_ndx, to_ndx);

    // Move accessors.
    using iter = decltype(m_table_accessors.begin());
    iter first, new_first, last;
    if (from_ndx < to_ndx) {
        // Rotate left.
        first     = m_table_accessors.begin() + from_ndx;
        new_first = first + 1;
        last      = m_table_accessors.begin() + to_ndx + 1;
    }
    else { // from_ndx > to_ndx
        // Rotate right.
        first     = m_table_accessors.begin() + to_ndx;
        new_first = m_table_accessors.begin() + from_ndx;
        last      = new_first + 1;
    }
    std::rotate(first, new_first, last);

    update_table_indices([&](size_t old_table_ndx) {
        auto it = moves.find(old_table_ndx);
        if (it != moves.end()) {
            return it->second;
        }
        return old_table_ndx;
    });

    if (Replication* repl = m_alloc.get_replication())
        repl->move_group_level_table(from_ndx, to_ndx); // Throws
}


class Group::DefaultTableWriter: public Group::TableWriter {
public:
    DefaultTableWriter(const Group& group):
        m_group(group)
    {
    }
    ref_type write_names(_impl::OutputStream& out) override
    {
        bool deep = true; // Deep
        bool only_if_modified = false; // Always
        return m_group.m_table_names.write(out, deep, only_if_modified); // Throws
    }
    ref_type write_tables(_impl::OutputStream& out) override
    {
        bool deep = true; // Deep
        bool only_if_modified = false; // Always
        return m_group.m_tables.write(out, deep, only_if_modified); // Throws
    }
private:
    const Group& m_group;
};

void Group::write(std::ostream& out, bool pad) const
{
    write(out, pad, 0);
}

void Group::write(std::ostream& out, bool pad, uint_fast64_t version_number) const
{
    REALM_ASSERT(is_attached());
    DefaultTableWriter table_writer(*this);
    write(out, table_writer, pad, version_number); // Throws
}

void Group::write(const std::string& path, const char* encryption_key) const
{
    write(path, encryption_key, 0);
}

void Group::write(const std::string& path, const char* encryption_key, uint_fast64_t version_number) const
{
    File file;
    int flags = 0;
    file.open(path, File::access_ReadWrite, File::create_Must, flags);
    file.set_encryption_key(encryption_key);
    File::Streambuf streambuf(&file);
    std::ostream out(&streambuf);
    write(out, encryption_key != 0, version_number);
}


BinaryData Group::write_to_mem() const
{
    REALM_ASSERT(is_attached());

    // Get max possible size of buffer
    //
    // FIXME: This size could potentially be vastly bigger that what
    // is actually needed.
    size_t max_size = m_alloc.get_total_size();

    char* buffer = static_cast<char*>(malloc(max_size)); // Throws
    if (!buffer)
        throw std::bad_alloc();
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


void Group::write(std::ostream& out, TableWriter& table_writer,
                  bool pad_for_encryption, uint_fast64_t version_number)
{
    _impl::OutputStream out_2(out);

    // Write the file header
    const char* data = reinterpret_cast<const char*>(&SlabAlloc::streaming_header);
    out_2.write(data, sizeof SlabAlloc::streaming_header);

    // Because we need to include the total logical file size in the
    // top-array, we have to start by writing everything except the
    // top-array, and then finally compute and write a correct version
    // of the top-array. The free-space information of the group will
    // only be included if a non-zero version number is given as parameter,
    // indicating that versioning info is to be saved. This is used from
    // SharedGroup to compact the database by writing only the live data
    // into a separate file.
    ref_type names_ref  = table_writer.write_names(out_2); // Throws
    ref_type tables_ref = table_writer.write_tables(out_2); // Throws
    Allocator& alloc = Allocator::get_default();
    Array top(alloc);
    top.create(Array::type_HasRefs); // Throws
    _impl::ShallowArrayDestroyGuard dg_top(&top);
    // FIXME: We really need an alternative to Array::truncate() that is able to expand.
    int_fast64_t value_1 = int_fast64_t(names_ref); // FIXME: Problematic unsigned -> signed conversion
    int_fast64_t value_2 = int_fast64_t(tables_ref); // FIXME: Problematic unsigned -> signed conversion
    top.add(value_1); // Throws
    top.add(value_2); // Throws
    top.add(0); // Throws

    int top_size = 3;
    if (version_number) {
        Array free_list(alloc);
        Array size_list(alloc);
        Array version_list(alloc);
        free_list.create(Array::type_Normal); // Throws
        _impl::DeepArrayDestroyGuard dg_1(&free_list);
        size_list.create(Array::type_Normal); // Throws
        _impl::DeepArrayDestroyGuard dg_2(&size_list);
        version_list.create(Array::type_Normal); // Throws
        _impl::DeepArrayDestroyGuard dg_3(&version_list);
        bool deep = true; // Deep
        bool only_if_modified = false; // Always
        ref_type free_list_ref = free_list.write(out_2, deep, only_if_modified);
        ref_type size_list_ref = size_list.write(out_2, deep, only_if_modified);
        ref_type version_list_ref = version_list.write(out_2, deep, only_if_modified);
        int_fast64_t value_3 = int_fast64_t(free_list_ref); // FIXME: Problematic unsigned -> signed conversion
        int_fast64_t value_4 = int_fast64_t(size_list_ref); // FIXME: Problematic unsigned -> signed conversion
        int_fast64_t value_5 = int_fast64_t(version_list_ref); // FIXME: Problematic unsigned -> signed conversion
        int_fast64_t value_6 = 1 + 2 * int_fast64_t(version_number); // FIXME: Problematic unsigned -> signed conversion
        top.add(value_3); // Throws
        top.add(value_4); // Throws
        top.add(value_5); // Throws
        top.add(value_6); // Throws
        top_size = 7;
    }
    ref_type top_ref = out_2.get_ref_of_next_array();

    // Produce a preliminary version of the top array whose
    // representation is guaranteed to be able to hold the final file
    // size
    size_t max_top_byte_size = Array::get_max_byte_size(top_size);
    size_t max_final_file_size = size_t(top_ref) + max_top_byte_size;
    int_fast64_t value_7 = 1 + 2*int_fast64_t(max_final_file_size); // FIXME: Problematic unsigned -> signed conversion
    top.ensure_minimum_width(value_7); // Throws

    // Finalize the top array by adding the projected final file size
    // to it
    size_t top_byte_size = top.get_byte_size();
    size_t final_file_size = size_t(top_ref) + top_byte_size;
    int_fast64_t value_8 = 1 + 2*int_fast64_t(final_file_size); // FIXME: Problematic unsigned -> signed conversion
    top.set(2, value_8); // Throws

    // Write the top array
    bool deep = false; // Shallow
    bool only_if_modified = false; // Always
    top.write(out_2, deep, only_if_modified); // Throws
    REALM_ASSERT_3(size_t(out_2.get_ref_of_next_array()), ==, final_file_size);

    dg_top.reset(nullptr); // Destroy now

    // encryption will pad the file to a multiple of the page, so ensure the
    // footer is aligned to the end of a page
    if (pad_for_encryption) {
#if REALM_ENABLE_ENCRYPTION
        size_t unrounded_size = final_file_size + sizeof(SlabAlloc::StreamingFooter);
        size_t rounded_size = round_up_to_page_size(unrounded_size);
        if (rounded_size != unrounded_size) {
            std::unique_ptr<char[]> buffer(new char[rounded_size - unrounded_size]());
            out_2.write(buffer.get(), rounded_size - unrounded_size);
        }
#endif
    }

    // Write streaming footer
    SlabAlloc::StreamingFooter footer;
    footer.m_top_ref = top_ref;
    footer.m_magic_cookie = SlabAlloc::footer_magic_cookie;
    out_2.write(reinterpret_cast<const char*>(&footer), sizeof footer);
}


void Group::commit()
{
    if (!is_attached())
        throw LogicError(LogicError::detached_accessor);
    if (m_is_shared)
        throw LogicError(LogicError::wrong_group_state);

    GroupWriter out(*this); // Throws

    // Recursively write all changed arrays to the database file. We
    // postpone the commit until we are sure that no exceptions can be
    // thrown.
    ref_type top_ref = out.write_group(); // Throws

    // Since the group is persisiting in single-thread (unshared)
    // mode we have to make sure that the group stays valid after
    // commit

    // Mark all managed space (beyond the attached file) as free.
    reset_free_space_tracking(); // Throws

    size_t old_baseline = m_alloc.get_baseline();

    // Remap file if it has grown
    size_t new_file_size = out.get_file_size();
    if (new_file_size > old_baseline) {
        m_alloc.remap(new_file_size); // Throws
    }

    out.commit(top_ref); // Throws

    // Recursively update refs in all active tables (columns, arrays..)
    update_refs(top_ref, old_baseline);
}


void Group::update_refs(ref_type top_ref, size_t old_baseline) noexcept
{
    // After Group::commit() we will always have free space tracking
    // info.
    REALM_ASSERT_3(m_top.size(), >=, 5);

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


void Group::to_string(std::ostream& out) const
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
    out << std::setw(int(index_width+1)) << std::left << " ";
    out << std::setw(int(name_width+1))  << std::left << "tables";
    out << std::setw(int(rows_width))    << std::left << "rows"    << std::endl;

    // Print tables
    for (size_t i = 0; i < count; ++i) {
        StringData name = get_table_name(i);
        ConstTableRef table = get_table(name);
        size_t row_count = table->size();

        out << std::setw(int(index_width)) << std::right << i                 << " ";
        out << std::setw(int(name_width))  << std::left  << std::string(name) << " ";
        out << std::setw(int(rows_width))  << std::left  << row_count         << std::endl;
    }
}


void Group::mark_all_table_accessors() noexcept
{
    size_t num_tables = m_table_accessors.size();
    for (size_t table_ndx = 0; table_ndx != num_tables; ++table_ndx) {
        if (Table* table = m_table_accessors[table_ndx]) {
            typedef _impl::TableFriend tf;
            tf::recursive_mark(*table); // Also all subtable accessors
        }
    }
}


namespace {

class MarkDirtyUpdater: public _impl::TableFriend::AccessorUpdater {
public:
    void update(Table& table) override
    {
        typedef _impl::TableFriend tf;
        tf::mark(table);
    }

    void update_parent(Table& table) override
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

    void update(Table& table) override
    {
        typedef _impl::TableFriend tf;
        tf::adj_insert_column(table, m_col_ndx); // Throws
        tf::mark_link_target_tables(table, m_col_ndx+1);
    }

    void update_parent(Table&) override
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

    void update(Table& table) override
    {
        typedef _impl::TableFriend tf;
        tf::adj_erase_column(table, m_col_ndx);
        tf::mark_link_target_tables(table, m_col_ndx);
    }

    void update_parent(Table&) override
    {
    }

private:
    size_t m_col_ndx;
};


class MoveColumnUpdater: public _impl::TableFriend::AccessorUpdater {
public:
    MoveColumnUpdater(size_t col_ndx_1, size_t col_ndx_2):
        m_col_ndx_1(col_ndx_1), m_col_ndx_2(col_ndx_2)
    {
    }

    void update(Table& table) override
    {
        using tf = _impl::TableFriend;
        tf::adj_move_column(table, m_col_ndx_1, m_col_ndx_2);
    }

    void update_parent(Table&) override
    {
    }

private:
    size_t m_col_ndx_1;
    size_t m_col_ndx_2;
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
    TransactAdvancer(Group& group, bool& schema_changed):
        m_group(group), m_schema_changed(schema_changed)
    {
    }

    bool insert_group_level_table(size_t table_ndx, size_t num_tables, StringData) noexcept
    {
        REALM_ASSERT_3(table_ndx, <=, num_tables);
        REALM_ASSERT(m_group.m_table_accessors.empty() ||
                       m_group.m_table_accessors.size() == num_tables);

        if (!m_group.m_table_accessors.empty()) {
            // for end-insertions, table_ndx will be equal to num_tables
            m_group.m_table_accessors.push_back(nullptr); // Throws
            size_t last_ndx = num_tables;
            m_group.m_table_accessors[last_ndx] = m_group.m_table_accessors[table_ndx];
            m_group.m_table_accessors[table_ndx] = nullptr;
            if (Table* moved_table = m_group.m_table_accessors[last_ndx]) {
                typedef _impl::TableFriend tf;
                tf::mark(*moved_table);
                tf::mark_opposite_link_tables(*moved_table);
            }
        }

        m_schema_changed = true;

        return true;
    }

    bool erase_group_level_table(size_t table_ndx, size_t num_tables) noexcept
    {
        REALM_ASSERT_3(table_ndx, <, num_tables);
        REALM_ASSERT(m_group.m_table_accessors.empty() ||
                       m_group.m_table_accessors.size() == num_tables);

        if (!m_group.m_table_accessors.empty()) {
            // Link target tables do not need to be considered here, since all
            // columns will already have been removed at this point.
            if (Table* table = m_group.m_table_accessors[table_ndx]) {
                typedef _impl::TableFriend tf;
                tf::detach(*table);
                tf::unbind_ptr(*table);
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
        }

        m_schema_changed = true;

        return true;
    }

    bool rename_group_level_table(size_t, StringData) noexcept
    {
        // No-op since table names are properties of the group, and the group
        // accessor is always refreshed
        m_schema_changed = true;
        return true;
    }

    bool move_group_level_table(size_t, size_t) noexcept
    {
        // No-op since table names / table refs are properties of the group, and the group
        // accessor is always refreshed
        m_schema_changed = true;
        return true;
    }

    bool select_table(size_t group_level_ndx, int levels, const size_t* path) noexcept
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

    bool insert_empty_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows,
                           bool unordered) noexcept
    {
        typedef _impl::TableFriend tf;
        if (m_table) {
            if (unordered) {
                // FIXME: Explain what the `num_rows_to_insert == 0` case is all
                // about (Thomas Goyne).
                if (num_rows_to_insert == 0) {
                    tf::mark_opposite_link_tables(*m_table);
                }
                else {
                    // Unordered insertion of multiple rows is not yet supported (and not
                    // yet needed).
                    REALM_ASSERT(num_rows_to_insert == 1);
                    size_t from_row_ndx = row_ndx;
                    size_t to_row_ndx = prior_num_rows;
                    tf::adj_acc_move_over(*m_table, from_row_ndx, to_row_ndx);
                }
            }
            else {
                tf::adj_acc_insert_rows(*m_table, row_ndx, num_rows_to_insert);
            }
        }
        return true;
    }

    bool erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
                    bool unordered) noexcept
    {
        if (unordered) {
            // Unordered removal of multiple rows is not yet supported (and not
            // yet needed).
            REALM_ASSERT_3(num_rows_to_erase, ==, 1);
            typedef _impl::TableFriend tf;
            if (m_table) {
                size_t prior_last_row_ndx = prior_num_rows - 1;
                tf::adj_acc_move_over(*m_table, prior_last_row_ndx, row_ndx);
            }
        }
        else {
            typedef _impl::TableFriend tf;
            if (m_table) {
                for (size_t i = 0; i < num_rows_to_erase; ++i)
                    tf::adj_acc_erase_row(*m_table, row_ndx + num_rows_to_erase - 1 - i);
            }
        }
        return true;
    }

    bool swap_rows(size_t row_ndx_1, size_t row_ndx_2) noexcept
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        using tf = _impl::TableFriend;
        tf::adj_acc_swap_rows(*m_table, row_ndx_1, row_ndx_2);
        return true;
    }

    bool clear_table() noexcept
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::adj_acc_clear_root_table(*m_table);
        return true;
    }

    bool set_int(size_t, size_t, int_fast64_t) noexcept
    {
        return true; // No-op
    }

    bool set_bool(size_t, size_t, bool) noexcept
    {
        return true; // No-op
    }

    bool set_float(size_t, size_t, float) noexcept
    {
        return true; // No-op
    }

    bool set_double(size_t, size_t, double) noexcept
    {
        return true; // No-op
    }

    bool set_string(size_t, size_t, StringData) noexcept
    {
        return true; // No-op
    }

    bool set_binary(size_t, size_t, BinaryData) noexcept
    {
        return true; // No-op
    }

    bool set_date_time(size_t, size_t, DateTime) noexcept
    {
        return true; // No-op
    }

    bool set_table(size_t col_ndx, size_t row_ndx) noexcept
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

    bool set_mixed(size_t col_ndx, size_t row_ndx, const Mixed&) noexcept
    {
        typedef _impl::TableFriend tf;
        if (m_table)
            tf::discard_subtable_accessor(*m_table, col_ndx, row_ndx);
        return true;
    }

    bool set_null(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool set_link(size_t col_ndx, size_t, size_t, size_t) noexcept
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

    bool insert_substring(size_t, size_t, size_t, StringData)
    {
        return true; // No-op
    }

    bool erase_substring(size_t, size_t, size_t, size_t)
    {
        return true; // No-op
    }

    bool optimize_table() noexcept
    {
        return true; // No-op
    }

    bool select_descriptor(int levels, const size_t* path)
    {
        m_desc.reset();
        if (m_table) {
            REALM_ASSERT(!m_table->has_shared_type());
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

    bool insert_column(size_t col_ndx, DataType, StringData, bool nullable)
    {
        static_cast<void>(nullable);
        if (m_table) {
            typedef _impl::TableFriend tf;
            InsertColumnUpdater updater(col_ndx);
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);
        }
        typedef _impl::DescriptorFriend df;
        if (m_desc)
            df::adj_insert_column(*m_desc, col_ndx);

        m_schema_changed = true;

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

        m_schema_changed = true;

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

        m_schema_changed = true;

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

        m_schema_changed = true;

        return true;
    }

    bool rename_column(size_t, StringData) noexcept
    {
        m_schema_changed = true;
        return true; // No-op
    }

    bool move_column(size_t col_ndx_1, size_t col_ndx_2) noexcept
    {
        if (m_table) {
            typedef _impl::TableFriend tf;
            MoveColumnUpdater updater(col_ndx_1, col_ndx_2);
            tf::update_accessors(*m_table, m_desc_path_begin, m_desc_path_end, updater);
        }
        typedef _impl::DescriptorFriend df;
        if (m_desc)
            df::adj_move_column(*m_desc, col_ndx_1, col_ndx_2);

        m_schema_changed = true;

        return true;
    }

    bool add_search_index(size_t) noexcept
    {
        return true; // No-op
    }

    bool remove_search_index(size_t) noexcept
    {
        return true; // No-op
    }

    bool add_primary_key(size_t) noexcept
    {
        return true; // No-op
    }

    bool remove_primary_key() noexcept
    {
        return true; // No-op
    }

    bool set_link_type(size_t, LinkType) noexcept
    {
        return true; // No-op
    }

    bool select_link_list(size_t col_ndx, size_t, size_t) noexcept
    {
        // See comments on link handling in TransactAdvancer::set_link().
        typedef _impl::TableFriend tf;
        if (m_table) {
            if (Table* target = tf::get_link_target_table_accessor(*m_table, col_ndx))
                tf::mark(*target);
        }
        return true; // No-op
    }

    bool link_list_set(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool link_list_insert(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool link_list_move(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool link_list_swap(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool link_list_erase(size_t) noexcept
    {
        return true; // No-op
    }

    bool link_list_clear(size_t) noexcept
    {
        return true; // No-op
    }

    bool nullify_link(size_t, size_t, size_t)
    {
        return true; // No-op
    }

    bool link_list_nullify(size_t)
    {
        return true; // No-op
    }

private:
    Group& m_group;
    TableRef m_table;
    DescriptorRef m_desc;
    const size_t* m_desc_path_begin;
    const size_t* m_desc_path_end;
    bool& m_schema_changed;
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
                bool bump_global = false;
                tf::bump_version(*table, bump_global);
            }
        }
    }
}


template<class F>
void Group::update_table_indices(F&& map_function)
{
    using tf = _impl::TableFriend;

    // Update any link columns.
    for (size_t i = 0; i < m_tables.size(); ++i) {
        Array table_top{m_alloc};
        table_top.set_parent(&m_tables, i);
        table_top.init_from_parent();
        Spec spec{m_alloc};
        size_t spec_ndx_in_parent = 0;
        spec.set_parent(&table_top, spec_ndx_in_parent);
        spec.init_from_parent();

        size_t num_cols = spec.get_column_count();
        bool spec_changed = false;
        for (size_t col_ndx = 0; col_ndx < num_cols; ++col_ndx) {
            ColumnType type = spec.get_column_type(col_ndx);
            if (tf::is_link_type(type) || type == col_type_BackLink) {
                size_t table_ndx = spec.get_opposite_link_table_ndx(col_ndx);
                size_t new_table_ndx = map_function(table_ndx);
                if (new_table_ndx != table_ndx) {
                    spec.set_opposite_link_table_ndx(col_ndx, new_table_ndx);
                    spec_changed = true;
                }
            }
        }

        if (spec_changed && !m_table_accessors.empty() && m_table_accessors[i] != nullptr) {
            tf::mark(*m_table_accessors[i]);
        }
    }

    // Update accessors.
    refresh_dirty_accessors();

    // Table's specs might have changed, so they need to be reinitialized.
    for (size_t i = 0; i < m_table_accessors.size(); ++i) {
        if (Table* t = m_table_accessors[i]) {
            tf::get_spec(*t).init_from_parent();
        }
    }
}


void Group::advance_transact(ref_type new_top_ref, size_t new_file_size,
                             _impl::NoCopyInputStream& in)
{
    REALM_ASSERT(is_attached());

    // Exception safety: If this function throws, the group accessor and all of
    // its subordinate accessors are left in a state that may not be fully
    // consistent. Only minimal consistency is guaranteed (see
    // AccessorConsistencyLevels). In this case, the application is required to
    // either destroy the Group object, forcing all subordinate accessors to
    // become detached, or take some other equivalent action that involves a
    // call to Group::detach(), such as terminating the transaction in progress.
    // such actions will also lead to the detachment of all subordinate
    // accessors. Until then it is an error, and unsafe if the application
    // attempts to access the group one of its subordinate accessors.
    //
    //
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
    // detach them if the underlying object has been removed.)
    //
    // The consequences of the changes in the transaction logs can be divided
    // into two types; those that need to be applied to the accessors
    // immediately (Table::adj_insert_column()), and those that can be "lumped
    // together" and deduced during a final accessor refresh operation
    // (Table::refresh_accessor_tree()).
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
    // recursively starting from the roots (group-level tables). It relies on
    // the the per-table accessor dirty flags (Table::m_dirty) to prune the
    // traversal to the set of accessors that were touched by the changes in the
    // transaction logs.

    bool schema_changed = false;
    _impl::TransactLogParser parser; // Throws
    TransactAdvancer advancer(*this, schema_changed);
    parser.parse(in, advancer); // Throws

    // Make all dynamically allocated memory (space beyond the attached file) as
    // available free-space.
    reset_free_space_tracking(); // Throws

    // Update memory mapping if database file has grown
    if (new_file_size > m_alloc.get_baseline()) {
        m_alloc.remap(new_file_size); // Throws
    }

    m_alloc.invalidate_cache();
    m_top.detach(); // Soft detach
    attach(new_top_ref); // Throws
    refresh_dirty_accessors(); // Throws

    if (schema_changed)
        send_schema_change_notification();
}


#ifdef REALM_DEBUG

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
        REALM_ASSERT_3(ref % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size, >, 0);
        REALM_ASSERT_3(ref, >=, m_ref_begin);
        REALM_ASSERT_3(size, <=, m_immutable_ref_end - ref);
        Chunk chunk;
        chunk.ref  = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add_mutable(ref_type ref, size_t size)
    {
        REALM_ASSERT_3(ref % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size, >, 0);
        REALM_ASSERT_3(ref, >=, m_immutable_ref_end);
        REALM_ASSERT_3(size, <=, m_mutable_ref_end - ref);
        Chunk chunk;
        chunk.ref  = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add(ref_type ref, size_t size)
    {
        REALM_ASSERT_3(ref % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size, >, 0);
        REALM_ASSERT_3(ref, >=, m_ref_begin);
        REALM_ASSERT(size <= (ref < m_baseline ? m_immutable_ref_end : m_mutable_ref_end) - ref);
        Chunk chunk;
        chunk.ref  = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add(const MemUsageVerifier& verifier)
    {
        m_chunks.insert(m_chunks.end(), verifier.m_chunks.begin(), verifier.m_chunks.end());
    }
    void handle(ref_type ref, size_t allocated, size_t) override
    {
        add(ref, allocated);
    }
    void canonicalize()
    {
        // Sort the chunks in order of increasing ref, then merge adjacent
        // chunks while checking that there is no overlap
        typedef std::vector<Chunk>::iterator iter;
        iter i_1 = m_chunks.begin(), end = m_chunks.end();
        iter i_2 = i_1;
        sort(i_1, end);
        if (i_1 != end) {
            while (++i_2 != end) {
                ref_type prev_ref_end = i_1->ref + i_1->size;
                REALM_ASSERT_3(prev_ref_end, <=, i_2->ref);
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
        REALM_ASSERT_3(m_chunks.size(), ==, 1);
        REALM_ASSERT_3(m_chunks.front().ref, ==, m_ref_begin);
        REALM_ASSERT_3(m_chunks.front().size, ==, m_mutable_ref_end - m_ref_begin);
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
    std::vector<Chunk> m_chunks;
    ref_type m_ref_begin, m_immutable_ref_end, m_mutable_ref_end, m_baseline;
};

} // anonymous namespace

void Group::verify() const
{
    REALM_ASSERT(is_attached());

    m_alloc.verify();

    // Verify tables
    {
        size_t n = m_tables.size();
        for (size_t i = 0; i != n; ++i) {
            ConstTableRef table = get_table(i);
            REALM_ASSERT_3(table->get_index_in_group(), ==, i);
            table->verify();
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
    {
        REALM_ASSERT(m_top.size() == 3 || m_top.size() == 5 || m_top.size() == 7);
        Allocator& alloc = m_top.get_alloc();
        ArrayInteger pos(alloc), len(alloc), ver(alloc);
        size_t pos_ndx = 3, len_ndx = 4, ver_ndx = 5;
        pos.set_parent(const_cast<Array*>(&m_top), pos_ndx);
        len.set_parent(const_cast<Array*>(&m_top), len_ndx);
        ver.set_parent(const_cast<Array*>(&m_top), ver_ndx);
        if (m_top.size() > pos_ndx) {
            if (ref_type ref = m_top.get_as_ref(pos_ndx))
                pos.init_from_ref(ref);
        }
        if (m_top.size() > len_ndx) {
            if (ref_type ref = m_top.get_as_ref(len_ndx))
                len.init_from_ref(ref);
        }
        if (m_top.size() > ver_ndx) {
            if (ref_type ref = m_top.get_as_ref(ver_ndx))
                ver.init_from_ref(ref);
        }
        REALM_ASSERT(pos.is_attached() == len.is_attached());
        REALM_ASSERT(pos.is_attached() || !ver.is_attached()); // pos.is_attached() <== ver.is_attached()
        if (pos.is_attached()) {
            size_t n = pos.size();
            REALM_ASSERT_3(n, ==, len.size());
            if (ver.is_attached())
                REALM_ASSERT_3(n, ==, ver.size());
            for (size_t i = 0; i != n; ++i) {
                ref_type ref  = to_ref(pos.get(i));
                size_t size = to_size_t(len.get(i));
                mem_usage_2.add_immutable(ref, size);
            }
            mem_usage_2.canonicalize();
            mem_usage_1.add(mem_usage_2);
            mem_usage_1.canonicalize();
            mem_usage_2.clear();
        }
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
    REALM_ASSERT_3(immutable_ref_end, <=, baseline);
    if (immutable_ref_end < baseline) {
        ref_type ref = immutable_ref_end;
        size_t size = baseline - immutable_ref_end;
        mem_usage_1.add_mutable(ref, size);
        mem_usage_1.canonicalize();
    }

    // At this point we have accounted for all memory managed by the slab
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
    Allocator& alloc = m_top.get_alloc();
    ArrayInteger pos(alloc), len(alloc), ver(alloc);
    size_t pos_ndx = 3, len_ndx = 4, ver_ndx = 5;
    pos.set_parent(const_cast<Array*>(&m_top), pos_ndx);
    len.set_parent(const_cast<Array*>(&m_top), len_ndx);
    ver.set_parent(const_cast<Array*>(&m_top), ver_ndx);
    if (m_top.size() > pos_ndx) {
        if (ref_type ref = m_top.get_as_ref(pos_ndx))
            pos.init_from_ref(ref);
    }
    if (m_top.size() > len_ndx) {
        if (ref_type ref = m_top.get_as_ref(len_ndx))
            len.init_from_ref(ref);
    }
    if (m_top.size() > ver_ndx) {
        if (ref_type ref = m_top.get_as_ref(ver_ndx))
            ver.init_from_ref(ref);
    }

    if (!pos.is_attached()) {
        std::cout << "none\n";
        return;
    }
    bool has_versions = ver.is_attached();

    size_t n = pos.size();
    for (size_t i = 0; i != n; ++i) {
        size_t offset = to_size_t(pos[i]);
        size_t size   = to_size_t(len[i]);
        std::cout << i << ": " << offset << " " << size;

        if (has_versions) {
            size_t version = to_size_t(ver[i]);
            std::cout << " " << version;
        }
        std::cout << "\n";
    }
    std::cout << "\n";
}


void Group::to_dot(std::ostream& out) const
{
    out << "digraph G {" << std::endl;

    out << "subgraph cluster_group {" << std::endl;
    out << " label = \"Group\";" << std::endl;

    m_top.to_dot(out, "group_top");
    m_table_names.to_dot(out, "table_names");
    m_tables.to_dot(out, "tables");

    // Tables
    for (size_t i = 0; i < m_tables.size(); ++i) {
        ConstTableRef table = get_table(i);
        StringData name = get_table_name(i);
        table->to_dot(out, name);
    }

    out << "}" << std::endl;
    out << "}" << std::endl;
}


void Group::to_dot() const
{
    to_dot(std::cerr);
}


void Group::to_dot(const char* file_path) const
{
    std::ofstream out(file_path);
    to_dot(out);
}

std::pair<ref_type, size_t> Group::get_to_dot_parent(size_t ndx_in_parent) const
{
    return std::make_pair(m_tables.get_ref(), ndx_in_parent);
}

#endif // REALM_DEBUG
