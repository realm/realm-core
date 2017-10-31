/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <limits>
#include <stdexcept>

#ifdef REALM_DEBUG
#include <iostream>
#include <iomanip>
#endif

#include <realm/util/features.h>
#include <realm/util/miscellaneous.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/exceptions.hpp>
#include <realm/table.hpp>
#include <realm/descriptor.hpp>
#include <realm/alloc_slab.hpp>
#include <realm/column.hpp>
#include <realm/column_string.hpp>
#include <realm/column_string_enum.hpp>
#include <realm/column_binary.hpp>
#include <realm/column_table.hpp>
#include <realm/column_mixed.hpp>
#include <realm/column_link.hpp>
#include <realm/column_linklist.hpp>
#include <realm/column_backlink.hpp>
#include <realm/index_string.hpp>
#include <realm/group.hpp>
#include <realm/link_view.hpp>
#include <realm/replication.hpp>
#include <realm/table_view.hpp>
#include <realm/query_engine.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_binary.hpp>
#include <realm/array_string.hpp>
#include <realm/array_timestamp.hpp>

/// \page AccessorConsistencyLevels
///
/// These are the three important levels of consistency of a hierarchy of
/// Realm accessors rooted in a common group accessor (tables, columns, rows,
/// descriptors, arrays):
///
/// ### Fully Consistent Accessor Hierarchy (or just "Full Consistency")
///
/// All attached accessors are in a fully valid state and can be freely used by
/// the application. From the point of view of the application, the accessor
/// hierarchy remains in this state as long as no library function throws.
///
/// If a library function throws, and the exception is one that is considered
/// part of the API, such as util::File::NotFound, then the accessor hierarchy
/// remains fully consistent. In all other cases, such as when a library
/// function fails because of memory exhaustion, and it throws std::bad_alloc,
/// the application may no longer assume anything beyond minimal consistency.
///
/// ### Minimally Consistent Accessor Hierarchy (or just "Minimal Consistency")
///
/// No correspondence between the accessor states and the underlying node
/// structure can be assumed, but all parent and child accessor references are
/// valid (i.e., not dangling). There are specific additional guarantees, but
/// only on some parts of the internal accessors states, and only on some parts
/// of the structural state.
///
/// This level of consistency is guaranteed at all times, and it is also the
/// **maximum** that may be assumed by the application after a library function
/// fails by throwing an unexpected exception (such as std::bad_alloc). It is
/// also the **minimum** level of consistency that is required to be able to
/// properly destroy the accessor objects (manually, or as a result of stack
/// unwinding).
///
/// It is supposed to be a library-wide invariant that an accessor hierarchy is
/// at least minimally consistent, but so far, only some parts of the library
/// conform to it.
///
/// Note: With proper use, and maintenance of Minimal Consistency, it is
/// possible to ensure that no memory is leaked after a group accessor is
/// destroyed, even after a library function has failed because of memory
/// exhaustion. This is possible because the underlying nodes are allocated in
/// the context of the group, and they can all be freed by the group when it is
/// destroyed. On the other hand, when working with free-standing tables, each
/// underlying node is allocated individually on the heap, so in this case we
/// cannot prevent memory leaks, because there is no way of knowing what to free
/// when the table accessor is destroyed.
///
/// ### Structurally Correspondent Accessor Hierarchy (or simply "Structural Correspondence")
///
/// The structure of the accessor hierarchy is in agreement with the underlying
/// node structure, but the refs (references to underlying nodes) are generally
/// not valid, and certain other parts of the accessor states are also generally
/// not valid. This state of consistency is important mainly during the
/// advancing of read transactions (implicit transactions), and is not exposed
/// to the application.
///
///
/// Below is a detailed specification of the requirements for Minimal
/// Consistency and for Structural Correspondence.
///
///
/// Minimally Consistent Accessor Hierarchy (accessor destruction)
/// --------------------------------------------------------------
///
/// This section defines a level of accessor consistency known as "Minimally
/// Consistent Accessor Hierarchy". It applies to a set of accessors rooted in a
/// common group. It does not imply any level of correspondance between the
/// state of the accessors and the underlying node structure. It enables safe
/// destruction of the accessor objects by requiring that the following items
/// are valid (the list may not yet be complete):
///
///  - Every allocated accessor is either a group accessor, or occurs as a
///    direct, or an indirect child of a group accessor.
///
///  - No allocated accessor occurs as a child more than once (for example, no
///    doublets are allowed in Group::m_table_accessors).
///
///  - The 'is_attached' property of array accessors (Array::m_data == 0). For
///    example, `Table::m_top` is attached if and only if that table accessor
///    was attached to a table with independent dynamic type.
///
///  - The 'parent' property of array accessors (Array::m_parent), but
///    crucially, **not** the `index_in_parent` property.
///
///  - The list of table accessors in a group accessor
///    (Group::m_table_accessors). All non-null pointers refer to existing table
///    accessors.
///
///  - The list of column accessors in a table acccessor (Table::m_cols). All
///    non-null pointers refer to existing column accessors.
///
///  - The 'root_array' property of a column accessor (ColumnBase::m_array). It
///    always refers to an existing array accessor. The exact type of that array
///    accessor must be determinable from the following properties of itself:
///    `is_inner_bptree_node` (Array::m_is_inner_bptree_node), `has_refs`
///    (Array::m_has_refs), and `context_flag` (Array::m_context_flag). This
///    allows for a column accessor to be properly destroyed.
///
///  - The map of subtable accessors in a column acccessor
///    (SubtableColumnBase:m_subtable_map). All pointers refer to existing
///    subtable accessors, but it is not required that the set of subtable
///    accessors referenced from a particular parent P conincide with the set of
///    subtables accessors specifying P as parent.
///
///  - The `descriptor` property of a table accesor (Table::m_descriptor). If it
///    is not null, then it refers to an existing descriptor accessor.
///
///  - The map of subdescriptor accessors in a descriptor accessor
///    (Descriptor::m_subdesc_map). All non-null pointers refer to existing
///    subdescriptor accessors.
///
///  - The `search_index` property of a column accesor (StringColumn::m_index,
///    StringEnumColumn::m_index). When it is non-null, it refers to an existing
///    search index accessor.
///
///
/// Structurally Correspondent Accessor Hierarchy (accessor reattachment)
/// ---------------------------------------------------------------------
///
/// This section defines what it means for an accessor hierarchy to be
/// "Structurally Correspondent". It applies to a set of accessors rooted in a
/// common group. The general idea is that the structure of the accessors must
/// match the underlying structure to such an extent that there is never any
/// doubt about which underlying node that corresponds with a particular
/// accessor. It is assumed that the accessor tree, and the underlying node
/// structure are structurally sound individually.
///
/// With this level of correspondence, it is possible to reattach the accessor
/// tree to the underlying node structure (Table::refresh_accessor_tree()).
///
/// While all the accessors in the tree must be in the attached state (before
/// reattachement), they are not required to refer to existing underlying nodes;
/// that is, their references **are** allowed to be dangling. Roughly speaking,
/// this means that the accessor tree must have been attached to a node
/// structure at some earlier point in time.
///
//
/// Requirements at group level:
///
///  - The number of tables in the underlying group must be equal to the number
///    of entries in `Group::m_table_accessors` in the group accessor.
///
///  - For each table in the underlying group, the corresponding entry in
///    `Table::m_table_accessors` (at same index) is either null, or points to a
///    table accessor that satisfies all the "requirements for a table".
///
/// Requirements for a table:
///
///  - The corresponding underlying table has independent descriptor if, and
///    only if `Table::m_top` is attached.
///
///  - The row index of every row accessor is strictly less than the number of
///    rows in the underlying table.
///
///  - If `Table::m_columns` is unattached (degenerate table), then
///    `Table::m_cols` is empty, otherwise the number of columns in the
///    underlying table is equal to the number of entries in `Table::m_cols`.
///
///  - Each entry in `Table::m_cols` is either null, or points to a column
///    accessor whose type agrees with the data type (realm::DataType) of the
///    corresponding underlying column (at same index).
///
///  - If a column accessor is of type `StringEnumColumn`, then the
///    corresponding underlying column must be an enumerated strings column (the
///    reverse is not required).
///
///  - If a column accessor is equipped with a search index accessor, then the
///    corresponding underlying column must be equipped with a search index (the
///    reverse is not required).
///
///  - For each entry in the subtable map of a column accessor there must be an
///    underlying subtable at column `i` and row `j`, where `i` is the index of
///    the column accessor in `Table::m_cols`, and `j` is the value of
///    `SubtableColumnBase::SubtableMap::entry::m_subtable_ndx`. The
///    corresponding subtable accessor must satisfy all the "requirements for a
///    table" with respect to that underlying subtable.
///
///  - It the table refers to a descriptor accessor (only possible for tables
///    with independent descriptor), then that descriptor accessor must satisfy
///    all the "requirements for a descriptor" with respect to the underlying
///    spec structure (of this table).
///
/// Requirements for a descriptor:
///
///  - For each entry in the subdescriptor map there must be an underlying
///    subspec at column `i`, where `i` is the value of
///    `Descriptor::subdesc_entry::m_column_ndx`. The corresponding
///    subdescriptor accessor must satisfy all the "requirements for a
///    descriptor" with respect to that underlying subspec.
///
/// The 'ndx_in_parent' property of most array accessors is required to be
/// valid. The exceptions are:
///
///  - The top array accessor of root tables (Table::m_top). Root tables are
///    tables with independent descriptor.
///
///  - The columns array accessor of subtables with shared descriptor
///    (Table::m_columns).
///
///  - The top array accessor of spec objects of subtables with shared
///    descriptor (Table::m_spec->m_top).
///
///  - The root array accessor of table level columns
///    (*Table::m_cols[]->m_array).
///
///  - The root array accessor of the subcolumn of unique strings in an
///    enumerated string column (*StringEnumColumn::m_keys.m_array).
///
///  - The root array accessor of search indexes
///    (*Table::m_cols[]->m_index->m_array).
///
/// Note that Structural Correspondence trivially includes Minimal Consistency,
/// since the latter it an invariant.


using namespace realm;
using namespace realm::util;

const int_fast64_t realm::Table::max_integer;
const int_fast64_t realm::Table::min_integer;


// fixme, we need to gather all these typetraits definitions to just 1 single

// -- Table ---------------------------------------------------------------------------------

size_t Table::add_column(DataType type, StringData name, bool nullable, DescriptorRef* subdesc)
{
    REALM_ASSERT(!has_shared_type());
    return get_descriptor()->add_column(type, name, subdesc, nullable); // Throws
}

size_t Table::add_column_link(DataType type, StringData name, Table& target, LinkType link_type)
{
    return get_descriptor()->add_column_link(type, name, target, link_type); // Throws
}


void Table::insert_column_link(size_t col_ndx, DataType type, StringData name, Table& target, LinkType link_type)
{
    get_descriptor()->insert_column_link(col_ndx, type, name, target, link_type); // Throws
}


size_t Table::get_backlink_count(size_t row_ndx, bool only_strong_links) const noexcept
{
    size_t backlink_columns_begin = m_spec->first_backlink_column_index();
    size_t backlink_columns_end = backlink_columns_begin + m_spec->backlink_column_count();
    size_t ref_count = 0;

    for (size_t i = backlink_columns_begin; i != backlink_columns_end; ++i) {
        const BacklinkColumn& backlink_col = get_column_backlink(i);
        if (only_strong_links) {
            const LinkColumnBase& link_col = backlink_col.get_origin_column();
            if (link_col.get_weak_links())
                continue;
        }
        ref_count += backlink_col.get_backlink_count(row_ndx);
    }

    return ref_count;
}

size_t Table::get_backlink_count(size_t row_ndx, const Table& origin, size_t origin_col_ndx) const noexcept
{
    size_t origin_table_ndx = origin.get_index_in_group();
    size_t backlink_col_ndx = m_spec->find_backlink_column(origin_table_ndx, origin_col_ndx);
    const BacklinkColumn& backlink_col = get_column_backlink(backlink_col_ndx);
    return backlink_col.get_backlink_count(row_ndx);
}


size_t Table::get_backlink(size_t row_ndx, const Table& origin, size_t origin_col_ndx, size_t backlink_ndx) const
    noexcept
{
    size_t origin_table_ndx = origin.get_index_in_group();
    size_t backlink_col_ndx = m_spec->find_backlink_column(origin_table_ndx, origin_col_ndx);
    const BacklinkColumn& backlink_col = get_column_backlink(backlink_col_ndx);
    return backlink_col.get_backlink(row_ndx, backlink_ndx);
}


void Table::connect_opposite_link_columns(size_t link_col_ndx, Table& target_table, size_t backlink_col_ndx) noexcept
{
    LinkColumnBase& link_col = get_column_link_base(link_col_ndx);
    BacklinkColumn& backlink_col = target_table.get_column_backlink(backlink_col_ndx);
    link_col.set_target_table(target_table);
    link_col.set_backlink_column(backlink_col);
    backlink_col.set_origin_table(*this);
    backlink_col.set_origin_column(link_col);
}


void Table::cascade_break_backlinks_to(size_t row_ndx, CascadeState& state)
{
    size_t num_cols = m_spec->get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& col = get_column_base(col_ndx);
        col.cascade_break_backlinks_to(row_ndx, state); // Throws
    }
}


void Table::cascade_break_backlinks_to_all_rows(CascadeState& state)
{
    size_t num_cols = m_spec->get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& col = get_column_base(col_ndx);
        col.cascade_break_backlinks_to_all_rows(m_size, state); // Throws
    }
}


void Table::remove_backlink_broken_rows(const CascadeState& cascade_state)
{
    Group& group = *get_parent_group();

    // Rows are ordered by ascending row index, but we need to remove the rows
    // by descending index to avoid changing the indexes of rows that are not
    // removed yet.
    auto rend = cascade_state.rows.rend();
    for (auto i = cascade_state.rows.rbegin(); i != rend; ++i) {
        typedef _impl::GroupFriend gf;
        bool is_move_last_over = (i->is_ordered_removal == 0);
        Table& table = gf::get_table(group, i->table_ndx);

        bool broken_reciprocal_backlinks = true;
        if (is_move_last_over) {
            table.do_move_last_over(i->row_ndx, broken_reciprocal_backlinks);
        }
        else {
            table.do_remove(i->row_ndx, broken_reciprocal_backlinks);
        }
    }
}


void Table::insert_column(size_t col_ndx, DataType type, StringData name, bool nullable, DescriptorRef* subdesc)
{
    REALM_ASSERT(!has_shared_type());
    get_descriptor()->insert_column(col_ndx, type, name, subdesc, nullable); // Throws
}


void Table::remove_column(size_t col_ndx)
{
    REALM_ASSERT(!has_shared_type());
    get_descriptor()->remove_column(col_ndx); // Throws
}


void Table::rename_column(size_t col_ndx, StringData name)
{
    REALM_ASSERT(!has_shared_type());
    get_descriptor()->rename_column(col_ndx, name); // Throws
}


DescriptorRef Table::get_descriptor()
{
    REALM_ASSERT(is_attached());

    if (has_shared_type()) {
        ArrayParent* array_parent = m_columns.get_parent();
        REALM_ASSERT(dynamic_cast<Parent*>(array_parent));
        Parent* table_parent = static_cast<Parent*>(array_parent);
        size_t col_ndx = 0;
        Table* parent = table_parent->get_parent_table(&col_ndx);
        REALM_ASSERT(parent);
        return parent->get_descriptor()->get_subdescriptor(col_ndx); // Throws
    }

    DescriptorRef desc = m_descriptor.lock();
    if (!desc) {
        typedef _impl::DescriptorFriend df;
        desc = df::create(); // Throws
        DescriptorRef parent = nullptr;
        df::attach(*desc, this, parent, m_spec.get());
        m_descriptor = desc;
    }
    return desc;
}


ConstDescriptorRef Table::get_descriptor() const
{
    return const_cast<Table*>(this)->get_descriptor(); // Throws
}


DescriptorRef Table::get_subdescriptor(size_t col_ndx)
{
    return get_descriptor()->get_subdescriptor(col_ndx); // Throws
}


ConstDescriptorRef Table::get_subdescriptor(size_t col_ndx) const
{
    return get_descriptor()->get_subdescriptor(col_ndx); // Throws
}


DescriptorRef Table::get_subdescriptor(const path_vec& path)
{
    DescriptorRef desc = get_descriptor(); // Throws

    for (const auto& path_part : path) {
        desc = desc->get_subdescriptor(path_part); // Throws
    }

    return desc;
}


ConstDescriptorRef Table::get_subdescriptor(const path_vec& path) const
{
    return const_cast<Table*>(this)->get_subdescriptor(path); // Throws
}


size_t Table::add_subcolumn(const path_vec& path, DataType type, StringData name)
{
    DescriptorRef desc = get_subdescriptor(path); // Throws
    size_t col_ndx = desc->get_column_count();
    desc->insert_column(col_ndx, type, name); // Throws
    return col_ndx;
}


void Table::insert_subcolumn(const path_vec& path, size_t col_ndx, DataType type, StringData name)
{
    get_subdescriptor(path)->insert_column(col_ndx, type, name); // Throws
}


void Table::remove_subcolumn(const path_vec& path, size_t col_ndx)
{
    get_subdescriptor(path)->remove_column(col_ndx); // Throws
}


void Table::rename_subcolumn(const path_vec& path, size_t col_ndx, StringData name)
{
    get_subdescriptor(path)->rename_column(col_ndx, name); // Throws
}


void Table::init(ref_type top_ref, ArrayParent* parent, size_t ndx_in_parent, bool skip_create_column_accessors)
{
    m_mark = false;

    m_version = 0;

    // Load from allocated memory
    m_top.set_parent(parent, ndx_in_parent);
    m_top.init_from_ref(top_ref);

    size_t spec_ndx_in_parent = 0;
    m_spec.manage(new Spec(get_alloc()));
    m_spec->set_parent(&m_top, spec_ndx_in_parent);
    m_spec->init_from_parent();
    size_t columns_ndx_in_parent = 1;
    m_columns.set_parent(&m_top, columns_ndx_in_parent);
    m_columns.init_from_parent();

    if (m_top.size() > 2) {
        size_t keys_ndx_in_parent = 2;
        m_clusters.set_parent(&m_top, keys_ndx_in_parent);
        m_clusters.init_from_parent();
    }

    size_t num_cols = m_spec->get_column_count();
    m_cols.resize(num_cols); // Throws

    if (!skip_create_column_accessors) {
        // Create column accessors and initialize `m_size`
        refresh_column_accessors(); // Throws
    }
}


void Table::init(Spec* shared_spec, ArrayParent* parent_column, size_t parent_row_ndx)
{
    m_mark = false;

    m_version = 0;

    m_spec = shared_spec;
    m_columns.set_parent(parent_column, parent_row_ndx);

    // A degenerate subtable has no underlying columns array and no column
    // accessors yet. They will be created on first modification.
    ref_type columns_ref = m_columns.get_ref_from_parent();
    if (columns_ref != 0) {
        m_columns.init_from_ref(columns_ref);

        size_t num_cols = m_spec->get_column_count();
        m_cols.resize(num_cols); // Throws
    }

    // Create column accessors and initialize `m_size`
    refresh_column_accessors(); // Throws
}


struct Table::InsertSubtableColumns : SubtableUpdater {
    InsertSubtableColumns(size_t i, DataType t, bool nullable)
        : m_column_ndx(i)
        , m_type(t)
        , m_nullable(nullable)
    {
    }
    void update(const SubtableColumn& subtables, Array& subcolumns) override
    {
        size_t row_ndx = subcolumns.get_ndx_in_parent();
        size_t subtable_size = subtables.get_subtable_size(row_ndx);
        Allocator& alloc = subcolumns.get_alloc();
        ref_type column_ref = create_column(ColumnType(m_type), subtable_size, m_nullable, alloc); // Throws
        _impl::DeepArrayRefDestroyGuard dg(column_ref, alloc);
        subcolumns.insert(m_column_ndx, column_ref); // Throws
        dg.release();
    }
    void update_accessor(Table& table) override
    {
        table.adj_insert_column(m_column_ndx);        // Throws
        table.refresh_column_accessors(m_column_ndx); // Throws
        bool bump_global = false;
        table.bump_version(bump_global);
    }

private:
    const size_t m_column_ndx;
    const DataType m_type;
    bool m_nullable;
};


struct Table::EraseSubtableColumns : SubtableUpdater {
    EraseSubtableColumns(size_t i)
        : m_column_ndx(i)
    {
    }
    void update(const SubtableColumn&, Array& subcolumns) override
    {
        ref_type column_ref = to_ref(subcolumns.get(m_column_ndx));
        subcolumns.erase(m_column_ndx); // Throws
        Array::destroy_deep(column_ref, subcolumns.get_alloc());
    }
    void update_accessor(Table& table) override
    {
        table.adj_erase_column(m_column_ndx);
        table.refresh_column_accessors(m_column_ndx);
        bool bump_global = false;
        table.bump_version(bump_global);
    }

private:
    const size_t m_column_ndx;
};


struct Table::RenameSubtableColumns : SubtableUpdater {
    void update(const SubtableColumn&, Array&) override
    {
    }
    void update_accessor(Table& table) override
    {
        bool bump_global = false;
        table.bump_version(bump_global);
    }
};

struct Table::MoveSubtableColumns : SubtableUpdater {
    MoveSubtableColumns(size_t col_ndx_1, size_t col_ndx_2)
        : m_col_ndx_1(col_ndx_1)
        , m_col_ndx_2(col_ndx_2)
    {
    }

    void update(const SubtableColumn&, Array& subcolumns) override
    {
        subcolumns.move_rotate(m_col_ndx_1, m_col_ndx_2);
    }

    void update_accessor(Table& table) override
    {
        table.adj_move_column(m_col_ndx_1, m_col_ndx_2);

        // Refresh column accessors for all affected columns.
        size_t lower_bound = std::min(m_col_ndx_1, m_col_ndx_2);
        table.refresh_column_accessors(lower_bound);
        bool bump_global = true;
        table.bump_version(bump_global);
    }

private:
    const size_t m_col_ndx_1;
    const size_t m_col_ndx_2;
};


void Table::do_insert_column(Descriptor& desc, size_t col_ndx, DataType type, StringData name, LinkTargetInfo& link,
                             bool nullable)
{
    REALM_ASSERT(desc.is_attached());

    if (REALM_UNLIKELY(name.size() > Descriptor::max_column_name_length))
        throw LogicError(LogicError::column_name_too_long);

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::get_root_table(desc);
    REALM_ASSERT(!root_table.has_shared_type());

    if (type == type_Link)
        nullable = true;

    if (desc.is_root()) {
        root_table.bump_version();
        root_table.insert_root_column(col_ndx, type, name, link, nullable); // Throws
    }
    else {
        Spec& spec = df::get_spec(desc);
        ColumnAttr attr = nullable ? col_attr_Nullable : col_attr_None;
        spec.insert_column(col_ndx, ColumnType(type), name, attr); // Throws
        if (!root_table.is_empty()) {
            root_table.m_top.get_alloc().bump_global_version();
            InsertSubtableColumns updater(col_ndx, type, nullable);
            update_subtables(desc, &updater); // Throws
        }
    }

    if (Replication* repl = root_table.get_repl())
        repl->insert_column(desc, col_ndx, type, name, link, nullable); // Throws
}


void Table::do_insert_column_unless_exists(Descriptor& desc, size_t col_ndx, DataType type, StringData name,
                                           LinkTargetInfo& link, bool nullable, bool* was_inserted)
{
    using df = _impl::DescriptorFriend;
    using tf = _impl::TableFriend;

    Spec& spec = df::get_spec(desc);

    size_t existing_ndx = spec.get_column_index(name);
    if (existing_ndx != npos) {
        col_ndx = existing_ndx;
    }

    if (col_ndx < spec.get_public_column_count()) {
        StringData existing_name = spec.get_column_name(col_ndx);
        if (existing_name == name) {
            DataType existing_type = spec.get_public_column_type(col_ndx);
            if (existing_type != type) {
                throw LogicError(LogicError::type_mismatch);
            }
            bool existing_is_nullable = (spec.get_column_attr(col_ndx) & col_attr_Nullable) != 0;
            if (existing_is_nullable != nullable) {
                throw LogicError(LogicError::type_mismatch);
            }
            if (tf::is_link_type(ColumnType(type)) &&
                spec.get_opposite_link_table_ndx(col_ndx) != link.m_target_table->get_index_in_group()) {
                throw LogicError(LogicError::type_mismatch);
            }

            // Column existed, and was identical to the requested column -- all is good.
            if (was_inserted) {
                *was_inserted = false;
            }
            return;
        }
        else {
            REALM_ASSERT_3(spec.get_column_index(name), ==, npos);
        }
    }

    do_insert_column(desc, col_ndx, type, name, link, nullable);
    if (was_inserted) {
        *was_inserted = true;
    }
}


void Table::do_erase_column(Descriptor& desc, size_t col_ndx)
{
    REALM_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::get_root_table(desc);
    REALM_ASSERT(!root_table.has_shared_type());
    REALM_ASSERT_3(col_ndx, <, desc.get_column_count());

    // It is possible that the column to be removed is the last column. If there
    // are no backlink columns, then the removal of the last column is enough to
    // effectively truncate the size (number of rows) to zero, since the number of rows
    // is simply the number of entries in each column. Although the size of the table at
    // this point will be zero (locally), we need to explicitly inject a clear operation
    // so that sync can handle conflicts with adding rows. Additionally, if there
    // are backlink columns, we need to inject a clear operation before
    // the column removal to correctly reproduce the desired effect, namely that
    // the table appears truncated after the removal of the last non-hidden
    // column. The clear operation needs to be submitted to the replication
    // handler as an individual operation, and precede the column removal
    // operation in order to get the right behaviour in
    // Group::advance_transact().
    if (root_table.m_spec->get_public_column_count() == 1)
        root_table.clear(); // Throws

    if (Replication* repl = root_table.get_repl())
        repl->erase_column(desc, col_ndx); // Throws

    if (desc.is_root()) {
        root_table.bump_version();
        root_table.erase_root_column(col_ndx); // Throws
    }
    else {
        Spec& spec = df::get_spec(desc);
        spec.erase_column(col_ndx); // Throws
        if (!root_table.is_empty()) {
            root_table.m_top.get_alloc().bump_global_version();
            EraseSubtableColumns updater(col_ndx);
            update_subtables(desc, &updater); // Throws
        }
    }
}


void Table::do_move_column(Descriptor& desc, size_t col_ndx_1, size_t col_ndx_2)
{
    REALM_ASSERT(desc.is_attached());

    using df = _impl::DescriptorFriend;
    Table& root_table = df::get_root_table(desc);
    REALM_ASSERT(!root_table.has_shared_type());
    REALM_ASSERT_3(col_ndx_1, <, desc.get_column_count());
    REALM_ASSERT_3(col_ndx_2, <, desc.get_column_count());

    if (Replication* repl = root_table.get_repl())
        repl->move_column(desc, col_ndx_1, col_ndx_2);

    if (desc.is_root()) {
        root_table.bump_version();
        root_table.move_root_column(col_ndx_1, col_ndx_2);
    }
    else {
        Spec& spec = df::get_spec(desc);
        spec.move_column(col_ndx_1, col_ndx_2); // Throws
        if (!root_table.is_empty()) {
            root_table.m_top.get_alloc().bump_global_version();
            MoveSubtableColumns updater{col_ndx_1, col_ndx_2};
            update_subtables(desc, &updater);
        }
    }
}


void Table::do_rename_column(Descriptor& desc, size_t col_ndx, StringData name)
{
    REALM_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::get_root_table(desc);
    REALM_ASSERT(!root_table.has_shared_type());
    REALM_ASSERT_3(col_ndx, <, desc.get_column_count());

    Spec& spec = df::get_spec(desc);
    spec.rename_column(col_ndx, name); // Throws

    if (desc.is_root()) {
        root_table.bump_version();
    }
    else {
        if (!root_table.is_empty()) {
            root_table.m_top.get_alloc().bump_global_version();
            RenameSubtableColumns updater;
            update_subtables(desc, &updater); // Throws
        }
    }

    if (Replication* repl = root_table.get_repl())
        repl->rename_column(desc, col_ndx, name); // Throws
}

void Table::do_add_search_index(Descriptor& descr, size_t column_ndx)
{
    typedef _impl::DescriptorFriend df;
    Spec& spec = df::get_spec(descr);

    if (REALM_UNLIKELY(column_ndx >= spec.get_public_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);

    // Early-out of already indexed
    if (descr.has_search_index(column_ndx))
        return;

    Table& root_table = df::get_root_table(descr);
    int attr = spec.get_column_attr(column_ndx);

    if (descr.is_root()) {
        root_table._add_search_index(column_ndx);
    }
    else {
        // Find the root table column index that contains the search index
        size_t parent_col;
        size_t* res = df::record_subdesc_path(descr, &parent_col, &parent_col + 1);
        if (!res) {
            throw LogicError(LogicError::subtable_of_subtable_index);
        }

        // Iterate through all rows of the root table and create an instance of each subtable. We have a special
        // problem though: All subtables share the same common instance of attributes, however while we successively
        // create indexes, we have some subtables that will match the index flag and some that will not, which
        // is an inchoherent state of the database. We rely on the fact that all method calls and operations in the
        // for-loop are safe to call despite of this.

        size_t sz = root_table.size();
        for (size_t r = 0; r < sz; r++) {
            TableRef sub = root_table.get_subtable(parent_col, r);
            // No reason to create search index for a degenerate table
            if (!sub->is_degenerate()) {
                sub->_add_search_index(column_ndx);
                // Clear index bit from shared spec because we're now going to operate on the next subtable
                // object which has no index yet (because various method calls may crash if attributes are
                // wrong)
                spec.set_column_attr(column_ndx, ColumnAttr(attr)); // Throws
            }
        }
    }

    spec.set_column_attr(column_ndx, ColumnAttr(attr | col_attr_Indexed)); // Throws

    if (Replication* repl = root_table.get_repl())
        repl->add_search_index(descr, column_ndx); // Throws
}

void Table::do_remove_search_index(Descriptor& descr, size_t column_ndx)
{
    typedef _impl::DescriptorFriend df;
    Spec& spec = df::get_spec(descr);

    if (REALM_UNLIKELY(column_ndx >= spec.get_public_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);

    // Early-out of non-indexed
    if (!descr.has_search_index(column_ndx))
        return;

    Table& root_table = df::get_root_table(descr);
    int attr = spec.get_column_attr(column_ndx);

    if (descr.is_root()) {
        root_table._remove_search_index(column_ndx);
    }
    else {
        size_t parent_col;
        size_t* res = df::record_subdesc_path(descr, &parent_col, &parent_col + 1);
        if (!res) {
            throw LogicError(LogicError::subtable_of_subtable_index);
        }

        // Iterate through all rows of the root table and create an instance of each subtable. We have a special
        // problem though: All subtables share the same common instance of attributes, however while we successively
        // remove indexes, we have some subtables that will match the index flag and some that will not, which
        // is an inchoherent state of the database. We rely on the fact that all method calls and operations in the
        // for-loop are safe to call despite of this.
        size_t sz = root_table.size();
        for (size_t r = 0; r < sz; r++) {
            // Destroy search index. This will update shared attributes in case refresh_column_accessors()
            // should depend on them being correct
            TableRef sub = root_table.get_subtable(parent_col, r);
            // No reason to remove search index for a degenerate table
            if (!sub->is_degenerate()) {
                sub->_remove_search_index(column_ndx);
                // Set index bit from shared spec because we're now going to operate on the next subtable
                // object which still has an index (because various method calls may crash if attributes are
                // wrong)
                spec.set_column_attr(column_ndx, ColumnAttr(attr)); // Throws
            }
        }
    }

    spec.set_column_attr(column_ndx, ColumnAttr(attr & ~col_attr_Indexed)); // Throws

    if (Replication* repl = root_table.get_repl())
        repl->remove_search_index(descr, column_ndx); // Throws
}

void Table::insert_root_column(size_t col_ndx, DataType type, StringData name, LinkTargetInfo& link_target,
                               bool nullable)
{
    using tf = _impl::TableFriend;

    REALM_ASSERT_3(col_ndx, <=, m_spec->get_public_column_count());

    do_insert_root_column(col_ndx, ColumnType(type), name, nullable); // Throws
    adj_insert_column(col_ndx);                                       // Throws
    update_link_target_tables(col_ndx, col_ndx + 1);                  // Throws

    // When the inserted column is a link-type column, we must also add a
    // backlink column to the target table, however, since the origin column
    // accessor does not yet exist, the connection between the column accessors
    // (Table::connect_opposite_link_columns()) cannot be established yet. The
    // marking of the target table tells Table::refresh_column_accessors() that
    // it should not try to establish the connection yet. The connection will be
    // established by Table::refresh_column_accessors() when it is invoked for
    // the target table below.
    if (link_target.is_valid()) {
        size_t target_table_ndx = link_target.m_target_table->get_index_in_group();
        m_spec->set_opposite_link_table_ndx(col_ndx, target_table_ndx); // Throws
        link_target.m_target_table->mark();
    }

    refresh_column_accessors(col_ndx); // Throws

    if (link_target.is_valid()) {
        link_target.m_target_table->unmark();
        size_t origin_table_ndx = get_index_in_group();
        if (link_target.m_backlink_col_ndx == realm::npos) {
            const Spec& target_spec = tf::get_spec(*(link_target.m_target_table));
            link_target.m_backlink_col_ndx = target_spec.get_column_count(); // insert at back of target
        }
        link_target.m_target_table->insert_backlink_column(origin_table_ndx, col_ndx,
                                                           link_target.m_backlink_col_ndx); // Throws
    }

    refresh_link_target_accessors(col_ndx);
}


void Table::erase_root_column(size_t col_ndx)
{
    REALM_ASSERT_3(col_ndx, <, m_spec->get_public_column_count());

    // For link columns we need to erase the backlink column first in case the
    // target table is the same as the origin table (because the backlink column
    // occurs after regular columns.)
    ColumnType col_type = m_spec->get_column_type(col_ndx);
    if (is_link_type(col_type)) {
        Table* link_target_table = get_link_target_table_accessor(col_ndx);
        size_t origin_table_ndx = get_index_in_group();
        link_target_table->erase_backlink_column(origin_table_ndx, col_ndx); // Throws
    }

    do_erase_root_column(col_ndx); // Throws
    adj_erase_column(col_ndx);
    update_link_target_tables(col_ndx + 1, col_ndx); // Throws
    refresh_column_accessors(col_ndx);
    refresh_link_target_accessors(col_ndx);
}


void Table::move_root_column(size_t from, size_t to)
{
    REALM_ASSERT_3(from, <, m_spec->get_public_column_count());
    REALM_ASSERT_3(to, <, m_spec->get_public_column_count());

    if (from == to)
        return;

    do_move_root_column(from, to);
    adj_move_column(from, to);
    update_link_target_tables_after_column_move(from, to);

    size_t min_ndx = std::min(from, to);
    refresh_column_accessors(min_ndx);
    refresh_link_target_accessors(min_ndx);
}


void Table::do_insert_root_column(size_t ndx, ColumnType type, StringData name, bool nullable)
{
    m_spec->insert_column(ndx, type, name, nullable ? col_attr_Nullable : col_attr_None); // Throws

    Spec::ColumnInfo info = m_spec->get_column_info(ndx);
    size_t ndx_in_parent = info.m_column_ref_ndx;
    ref_type col_ref = create_column(type, m_size, nullable, m_columns.get_alloc()); // Throws
    m_columns.insert(ndx_in_parent, col_ref);                                        // Throws

    if (m_clusters.is_attached()) {
        m_clusters.insert_column(ndx);
    }
}


void Table::do_erase_root_column(size_t ndx)
{
    Spec::ColumnInfo info = m_spec->get_column_info(ndx);
    m_spec->erase_column(ndx); // Throws

    // Remove ref from m_columns, and destroy node structure
    size_t ndx_in_parent = info.m_column_ref_ndx;
    ref_type col_ref = m_columns.get_as_ref(ndx_in_parent);
    Array::destroy_deep(col_ref, m_columns.get_alloc());
    m_columns.erase(ndx_in_parent);

    // If the column had a source index we have to remove and destroy that as
    // well
    if (info.m_has_search_index) {
        ref_type index_ref = m_columns.get_as_ref(ndx_in_parent);
        Array::destroy_deep(index_ref, m_columns.get_alloc());
        m_columns.erase(ndx_in_parent);
    }

    if (m_clusters.is_attached()) {
        m_clusters.remove_column(ndx);
    }
}


void Table::do_move_root_column(size_t from_ndx, size_t to_ndx)
{
    Spec::ColumnInfo from_info = m_spec->get_column_info(from_ndx);
    Spec::ColumnInfo to_info = m_spec->get_column_info(to_ndx);
    m_spec->move_column(from_ndx, to_ndx);

    size_t from = from_info.m_column_ref_ndx;
    size_t to = to_info.m_column_ref_ndx;

    size_t from_width = from_info.m_has_search_index ? 2 : 1;
    if (to_ndx > from_ndx) {
        to = to - from_width + 1;
    }
    m_columns.move_rotate(from, to, from_width);

    // When moving upwards, we need to check if the displaced column
    // has a search index, and if it does, move it down where it belongs.
    if (to_ndx > from_ndx && to_info.m_has_search_index) {
        // Move the search index down where it belongs (next to its owner).
        m_columns.move_rotate(to + from_width, to);
    }
}


void Table::do_set_link_type(size_t col_ndx, LinkType link_type)
{
    bool weak_links = false;
    switch (link_type) {
        case link_Strong:
            break;
        case link_Weak:
            weak_links = true;
            break;
    }

    ColumnAttr attr = m_spec->get_column_attr(col_ndx);
    ColumnAttr new_attr = attr;
    new_attr = ColumnAttr(new_attr & ~col_attr_StrongLinks);
    if (!weak_links)
        new_attr = ColumnAttr(new_attr | col_attr_StrongLinks);
    if (new_attr == attr)
        return;
    m_spec->set_column_attr(col_ndx, new_attr);

    LinkColumnBase& col = get_column_link_base(col_ndx);
    col.set_weak_links(weak_links);

    if (Replication* repl = get_repl())
        repl->set_link_type(this, col_ndx, link_type); // Throws
}


void Table::insert_backlink_column(size_t origin_table_ndx, size_t origin_col_ndx, size_t backlink_col_ndx)
{
    REALM_ASSERT_3(backlink_col_ndx, <=, m_cols.size());
    do_insert_root_column(backlink_col_ndx, col_type_BackLink, "");         // Throws
    adj_insert_column(backlink_col_ndx);                                    // Throws
    m_spec->set_opposite_link_table_ndx(backlink_col_ndx, origin_table_ndx); // Throws
    m_spec->set_backlink_origin_column(backlink_col_ndx, origin_col_ndx);    // Throws
    refresh_column_accessors(backlink_col_ndx);                             // Throws
}


void Table::erase_backlink_column(size_t origin_table_ndx, size_t origin_col_ndx)
{
    size_t backlink_col_ndx = m_spec->find_backlink_column(origin_table_ndx, origin_col_ndx);
    REALM_ASSERT_3(backlink_col_ndx, !=, realm::not_found);
    do_erase_root_column(backlink_col_ndx); // Throws
    adj_erase_column(backlink_col_ndx);
    refresh_column_accessors(backlink_col_ndx); // Throws
}


void Table::update_link_target_tables(size_t old_col_ndx_begin, size_t new_col_ndx_begin)
{
    // Called when columns are inserted or removed.

    // If there are any subsequent link-type columns, the corresponding target
    // tables need to be updated such that their descriptors specify the right
    // origin table column indices.

    size_t num_cols = m_cols.size();

    // If multiple link columns exist to the same table, updating the backlink
    // columns one by one is risky, because we use Spec::find_backlink_column
    // to figure out which backlink column should be updated. If we update them
    // as we find them, the next iteration might find the column that we have
    // just updated, thinking it should be updated once more.
    //
    // Therefore, we figure out which backlink columns need to be updated first,
    // and then we actually update them in the second pass.
    //
    // Tuples are: (target table, backlink column index, new column index).
    std::vector<std::tuple<Table*, size_t, size_t>> update_backlink_columns;

    for (size_t new_col_ndx = new_col_ndx_begin; new_col_ndx < num_cols; ++new_col_ndx) {
        ColumnType type = m_spec->get_column_type(new_col_ndx);
        if (!is_link_type(type))
            continue;
        LinkColumnBase* link_col = static_cast<LinkColumnBase*>(m_cols[new_col_ndx]);
        Table* target_table = &link_col->get_target_table();
        Spec* target_spec = target_table->m_spec.get();
        size_t origin_table_ndx = get_index_in_group();
        size_t old_col_ndx = old_col_ndx_begin + (new_col_ndx - new_col_ndx_begin);
        size_t backlink_col_ndx = target_spec->find_backlink_column(origin_table_ndx, old_col_ndx);
        update_backlink_columns.emplace_back(target_table, backlink_col_ndx, new_col_ndx); // Throws
    }

    for (auto& t : update_backlink_columns) {
        Spec* target_spec = std::get<0>(t)->m_spec.get();
        target_spec->set_backlink_origin_column(std::get<1>(t), std::get<2>(t));
    }
}


void Table::update_link_target_tables_after_column_move(size_t moved_from, size_t moved_to)
{
    // Called when columns are moved.

    // If there are any link-type columns in the range of columns that were shifted
    // as a result of move, the target tables need to be updated such that their
    // descriptors specify the right origin table column indices.

    // This function is called after the move has already been carried out.

    size_t origin_table_ndx = get_index_in_group();

    // If multiple link columns exist to the same table, updating the backlink
    // columns one by one is risky, because we use Spec::find_backlink_column
    // to figure out which backlink column should be updated. If we update them
    // as we find them, the next iteration might find the column that we have
    // just updated, thinking it should be updated once more.
    //
    // Therefore, we figure out which backlink columns need to be updated first,
    // and then we actually update them in the second pass.
    //
    // Tuples are: (target spec, backlink column index, new column index).
    std::vector<std::tuple<Spec*, size_t, size_t>> update_backlink_columns;
    update_backlink_columns.reserve(m_spec->get_public_column_count());

    // Update backlink columns pointing to the column that was moved.
    if (is_link_type(m_spec->get_column_type(moved_to))) {
        LinkColumnBase* link_col = static_cast<LinkColumnBase*>(m_cols[moved_to]);
        Spec* target_spec = link_col->get_target_table().m_spec.get();
        size_t backlink_col_ndx = target_spec->find_backlink_column(origin_table_ndx, moved_from);
        update_backlink_columns.emplace_back(target_spec, backlink_col_ndx, moved_to);
    }

    // Update backlink columns pointing to any link columns between the source and
    // destination column indices.
    if (moved_from < moved_to) {
        // Moved up:
        for (size_t col_ndx = moved_from; col_ndx < moved_to; ++col_ndx) {
            if (!is_link_type(m_spec->get_column_type(col_ndx)))
                continue;
            LinkColumnBase* link_col = static_cast<LinkColumnBase*>(m_cols[col_ndx]);
            Spec* target_spec = link_col->get_target_table().m_spec.get();
            size_t old_col_ndx = col_ndx + 1;
            size_t backlink_col_ndx = target_spec->find_backlink_column(origin_table_ndx, old_col_ndx);
            update_backlink_columns.emplace_back(target_spec, backlink_col_ndx, col_ndx);
        }
    }
    else if (moved_from > moved_to) {
        // Moved down:
        for (size_t col_ndx = moved_to + 1; col_ndx <= moved_from; ++col_ndx) {
            if (!is_link_type(m_spec->get_column_type(col_ndx)))
                continue;
            LinkColumnBase* link_col = static_cast<LinkColumnBase*>(m_cols[col_ndx]);
            Spec* target_spec = link_col->get_target_table().m_spec.get();
            size_t old_col_ndx = col_ndx - 1;
            size_t backlink_col_ndx = target_spec->find_backlink_column(origin_table_ndx, old_col_ndx);
            update_backlink_columns.emplace_back(target_spec, backlink_col_ndx, col_ndx);
        }
    }

    for (auto& t : update_backlink_columns) {
        Spec* target_spec = std::get<0>(t);
        target_spec->set_backlink_origin_column(std::get<1>(t), std::get<2>(t));
    }
}


void Table::register_row_accessor(RowBase* row) const noexcept
{
    LockGuard lock(m_accessor_mutex);
    row->m_prev = nullptr;
    row->m_next = m_row_accessors;
    if (m_row_accessors)
        m_row_accessors->m_prev = row;
    m_row_accessors = row;
}


void Table::unregister_row_accessor(RowBase* row) const noexcept
{
    LockGuard lock(m_accessor_mutex);
    do_unregister_row_accessor(row);
}


void Table::do_unregister_row_accessor(RowBase* row) const noexcept
{
    if (row->m_prev) {
        row->m_prev->m_next = row->m_next;
    }
    else { // is head of list
        m_row_accessors = row->m_next;
    }
    if (row->m_next)
        row->m_next->m_prev = row->m_prev;
}


void Table::discard_row_accessors() noexcept
{
    LockGuard lock(m_accessor_mutex);
    for (RowBase* row = m_row_accessors; row; row = row->m_next)
        row->m_table.reset(); // Detach
    m_row_accessors = nullptr;
}


void Table::update_subtables(Descriptor& desc, SubtableUpdater* updater)
{
    size_t stat_buf[8];
    size_t size = sizeof stat_buf / sizeof *stat_buf;
    size_t* begin = stat_buf;
    size_t* end = begin + size;
    std::unique_ptr<size_t[]> dyn_buf;
    for (;;) {
        typedef _impl::DescriptorFriend df;
        begin = df::record_subdesc_path(desc, begin, end);
        if (REALM_LIKELY(begin)) {
            Table& root_table = df::get_root_table(desc);
            root_table.update_subtables(begin, end, updater); // Throws
            return;
        }
        if (int_multiply_with_overflow_detect(size, 2))
            throw std::runtime_error("Too many subdescriptor nesting levels");
        begin = new size_t[size]; // Throws
        end = begin + size;
        dyn_buf.reset(begin);
    }
}


void Table::update_subtables(const size_t* col_path_begin, const size_t* col_path_end, SubtableUpdater* updater)
{
    size_t col_path_size = col_path_end - col_path_begin;
    REALM_ASSERT_3(col_path_size, >=, 1);

    size_t col_ndx = *col_path_begin;
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Table);

    SubtableColumn& subtables = get_column_table(col_ndx); // Throws
    size_t num_rows = size();
    bool is_parent_of_modify_level = col_path_size == 1;
    for (size_t row_ndx = 0; row_ndx < num_rows; ++row_ndx) {
        // Fetch the subtable accessor, but only if it exists already. Note that
        // it would not be safe to instantiate new accessors for subtables at
        // the modification level, because there would be a mismatch between the
        // shared descriptor and the underlying subtable.
        TableRef subtable(subtables.get_subtable_accessor(row_ndx));
        if (subtable) {
            // If it exists, we need to refresh its shared spec accessor since
            // parts of the underlying shared spec may have been relocated.
            subtable->m_spec->init_from_parent();
        }
        if (is_parent_of_modify_level) {
            // The subtables of the parent at this level are the ones that need
            // to be modified.
            if (!updater)
                continue;
            // If the table is degenerate, there is no underlying subtable to
            // modify, and since a table accessor attached to a degenerate
            // subtable has no cached columns, a preexisting subtable accessor
            // will not have to be refreshed either.
            ref_type subtable_ref = subtables.get_as_ref(row_ndx);
            if (subtable_ref == 0)
                continue;
            if (subtable) {
                updater->update(subtables, subtable->m_columns); // Throws
                updater->update_accessor(*subtable);             // Throws
            }
            else {
                Allocator& alloc = m_columns.get_alloc();
                Array subcolumns(alloc);
                subcolumns.init_from_ref(subtable_ref);
                subcolumns.set_parent(&subtables, row_ndx);
                updater->update(subtables, subcolumns); // Throws
            }
        }
        else {
            // The subtables of the parent at this level are ancestors of the
            // subtables that need to be modified, so we can safely instantiate
            // missing subtable accessors.
            if (subtables.get_as_ref(row_ndx) == 0)
                continue; // Degenerate subatble
            if (!subtable) {
                // If there is no updater, the we only need to refesh
                // preexisting accessors
                if (!updater)
                    continue;
                subtable = subtables.get_subtable_tableref(row_ndx); // Throws
            }
            subtable->update_subtables(col_path_begin + 1, col_path_end, updater); // Throws
        }
    }
}


void Table::update_accessors(const size_t* col_path_begin, const size_t* col_path_end, AccessorUpdater& updater)
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());

    if (col_path_begin == col_path_end) {
        updater.update(*this); // Throws
        return;
    }
    updater.update_parent(*this); // Throws

    size_t col_ndx = col_path_begin[0];
    // If this table is not a degenerate subtable, then `col_ndx` must be a
    // valid index into `m_cols`.
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_cols.size());

    // Early-out if this accessor refers to a degenerate subtable
    if (m_cols.empty())
        return;

    if (ColumnBase* col = m_cols[col_ndx]) {
        REALM_ASSERT(dynamic_cast<SubtableColumn*>(col));
        SubtableColumn* col_2 = static_cast<SubtableColumn*>(col);
        col_2->update_table_accessors(col_path_begin + 1, col_path_end, updater); // Throws
    }
}


void Table::create_degen_subtab_columns()
{
    // Creates columns as well as column accessors for a degenerate
    // subtable. When done, that subtable is no longer degenerate.

    REALM_ASSERT(!m_columns.is_attached());

    m_columns.create(Array::type_HasRefs); // Throws
    m_columns.update_parent();             // Throws

    Allocator& alloc = m_columns.get_alloc();
    size_t num_cols = m_spec->get_column_count();
    for (size_t i = 0; i < num_cols; ++i) {
        ColumnType type = m_spec->get_column_type(i);
        int attr = m_spec->get_column_attr(i);
        bool nullable = (attr & col_attr_Nullable) != 0;
        // Must be 0, else there's no way to create search index for it statically
        size_t init_size = 0;
        ref_type ref = create_column(type, init_size, nullable, alloc); // Throws
        m_columns.add(int_fast64_t(ref));                               // Throws

        // Create empty search index if required and add it to m_columns
        if (attr & col_attr_Indexed) {
            m_columns.add(StringIndex::create_empty(get_alloc()));
        }
    }

    m_cols.resize(num_cols);
    refresh_column_accessors();
}


void Table::detach() noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.
    if (Replication* repl = get_repl())
        repl->on_table_destroyed(this);
    m_spec.detach();

    discard_desc_accessor();

    // This prevents the destructor from deallocating the underlying
    // memory structure, and from attempting to notify the parent. It
    // also causes is_attached() to return false.
    m_columns.set_parent(nullptr, 0);

    discard_child_accessors();
    destroy_column_accessors();
    m_cols.clear();
    // FSA: m_cols.destroy();
    discard_views();
}


void Table::unregister_view(const TableViewBase* view) noexcept
{
    LockGuard lock(m_accessor_mutex);
    // Fixme: O(n) may be unacceptable - if so, put and maintain
    // iterator or index in TableViewBase.
    for (auto& v : m_views) {
        if (v == view) {
            v = m_views.back();
            m_views.pop_back();
            break;
        }
    }
}


void Table::move_registered_view(const TableViewBase* old_addr, const TableViewBase* new_addr) noexcept
{
    LockGuard lock(m_accessor_mutex);
    for (auto& view : m_views) {
        if (view == old_addr) {
            // casting away constness here... all operations on members
            // of  m_views are preserving logical constness on the table views.
            view = const_cast<TableViewBase*>(new_addr);
            return;
        }
    }
    REALM_ASSERT(false);
}


void Table::discard_views() noexcept
{
    LockGuard lock(m_accessor_mutex);
    for (const auto& view : m_views) {
        view->detach();
    }
    m_views.clear();
}


void Table::discard_child_accessors() noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    discard_row_accessors();

    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->discard_child_accessors();
        }
    }
}


void Table::discard_desc_accessor() noexcept
{
    // Must hold a reliable reference count while detaching
    DescriptorRef desc = m_descriptor.lock();
    if (desc) {
        typedef _impl::DescriptorFriend df;
        df::detach(*desc);
        m_descriptor.reset();
    }
}


void Table::instantiate_before_change()
{
    // Degenerate subtables need to be instantiated before first modification
    if (!m_columns.is_attached())
        create_degen_subtab_columns(); // Throws
}


ColumnBase* Table::create_column_accessor(ColumnType col_type, size_t col_ndx, size_t ndx_in_parent)
{
    ColumnBase* col = nullptr;
    ref_type ref = m_columns.get_as_ref(ndx_in_parent);
    Allocator& alloc = m_columns.get_alloc();

    bool nullable = is_nullable(col_ndx);

    REALM_ASSERT_DEBUG(!(
        nullable && (col_type != col_type_String && col_type != col_type_StringEnum && col_type != col_type_Binary &&
                     col_type != col_type_Int && col_type != col_type_Float && col_type != col_type_Double &&
                     col_type != col_type_OldDateTime && col_type != col_type_Timestamp &&
                     col_type != col_type_Bool && col_type != col_type_Link && col_type != col_type_Table)));

    switch (col_type) {
        case col_type_Int:
        case col_type_Bool:
        case col_type_OldDateTime:
            if (nullable) {
                col = new IntNullColumn(alloc, ref, col_ndx); // Throws
            }
            else {
                col = new IntegerColumn(alloc, ref, col_ndx); // Throws
            }
            break;
        case col_type_Float:
            col = new FloatColumn(alloc, ref, col_ndx); // Throws
            break;
        case col_type_Double:
            col = new DoubleColumn(alloc, ref, col_ndx); // Throws
            break;
        case col_type_String:
            col = new StringColumn(alloc, ref, nullable, col_ndx); // Throws
            break;
        case col_type_Binary:
            col = new BinaryColumn(alloc, ref, nullable, col_ndx); // Throws
            break;
        case col_type_StringEnum: {
            ArrayParent* keys_parent;
            size_t keys_ndx_in_parent;
            ref_type keys_ref = m_spec->get_enumkeys_ref(col_ndx, &keys_parent, &keys_ndx_in_parent);
            StringEnumColumn* col_2 = new StringEnumColumn(alloc, ref, keys_ref, nullable, col_ndx); // Throws
            col_2->get_keys().set_parent(keys_parent, keys_ndx_in_parent);
            col = col_2;
            break;
        }
        case col_type_Table:
            col = new SubtableColumn(alloc, ref, this, col_ndx); // Throws
            break;
        case col_type_Mixed:
            col = new MixedColumn(alloc, ref, this, col_ndx); // Throws
            break;
        case col_type_Link:
            // Target table will be set by group after entire table has been created
            col = new LinkColumn(alloc, ref, this, col_ndx); // Throws
            break;
        case col_type_LinkList:
            // Target table will be set by group after entire table has been created
            col = new LinkListColumn(alloc, ref, this, col_ndx); // Throws
            break;
        case col_type_BackLink:
            // Origin table will be set by group after entire table has been created
            col = new BacklinkColumn(alloc, ref, col_ndx); // Throws
            break;
        case col_type_Timestamp:
            // Origin table will be set by group after entire table has been created
            col = new TimestampColumn(nullable, alloc, ref, col_ndx); // Throws
            break;
        case col_type_Reserved4:
            // These have no function yet and are therefore unexpected.
            break;
    }
    REALM_ASSERT(col);
    col->set_parent(&m_columns, ndx_in_parent);
    return col;
}


void Table::destroy_column_accessors() noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    for (auto& col : m_cols) {
        delete col;
    }
    m_cols.clear();
}

std::recursive_mutex* Table::get_parent_accessor_management_lock() const
{
    if (!is_attached())
        return nullptr;
    if (!m_top.is_attached()) {
        ArrayParent* parent = m_columns.get_parent();
        REALM_ASSERT(dynamic_cast<Parent*>(parent));
        return static_cast<Parent*>(parent)->get_accessor_management_lock();
    }
    if (ArrayParent* parent = m_top.get_parent()) {
        REALM_ASSERT(dynamic_cast<Parent*>(parent));
        return static_cast<Parent*>(parent)->get_accessor_management_lock();
    }
    return nullptr;
}

Table::~Table() noexcept
{
    // Whenever this is not a free-standing table, the destructor must be able
    // to operate without assuming more than minimal accessor consistency This
    // means in particular that it cannot access the underlying structure of
    // array nodes. See AccessorConsistencyLevels.

    if (!is_attached()) {
        // This table has been detached.
        REALM_ASSERT_3(m_ref_count.load(), ==, 0);
        return;
    }

    if (Replication* repl = get_repl())
        repl->on_table_destroyed(this);
    m_spec.detach();

    if (!m_top.is_attached()) {
        // This is a subtable with a shared spec, and its lifetime is managed by
        // reference counting, so we must let the parent know about the demise
        // of this subtable.
        ArrayParent* parent = m_columns.get_parent();
        REALM_ASSERT(parent);
        REALM_ASSERT_3(m_ref_count.load(), ==, 0);
        REALM_ASSERT(dynamic_cast<Parent*>(parent));
        static_cast<Parent*>(parent)->child_accessor_destroyed(this);
        destroy_column_accessors();
        m_cols.clear();
        return;
    }

    // This is a table with an independent spec.
    if (ArrayParent* parent = m_top.get_parent()) {
        // This is a table whose lifetime is managed by reference
        // counting, so we must let our parent know about our demise.
        REALM_ASSERT_3(m_ref_count.load(), ==, 0);
        REALM_ASSERT(dynamic_cast<Parent*>(parent));
        static_cast<Parent*>(parent)->child_accessor_destroyed(this);
        destroy_column_accessors();
        m_cols.clear();
        return;
    }

    // This is a freestanding table, so we are responsible for
    // deallocating the underlying memory structure. If the table was
    // created using the public table constructor (a stack allocated
    // table) then the reference count must be strictly positive at
    // this point. Otherwise the table has been created using
    // LangBindHelper::new_table(), and then the reference count must
    // be zero, because that is what has caused the destructor to be
    // called. In the latter case, there can be no descriptors or
    // subtables to detach, because attached ones would have kept
    // their parent alive.
    if (0 < m_ref_count) {
        detach();
    }
    else {
        destroy_column_accessors();
        m_cols.clear();
    }
    m_top.destroy_deep();
}


bool Table::has_search_index(size_t col_ndx) const noexcept
{
    if (has_shared_type()) {
        return get_descriptor()->has_search_index(col_ndx);
    }

    // Check column of `this` which is a root table
    // Utilize the guarantee that m_cols.size() == 0 for a detached table accessor.
    if (REALM_UNLIKELY(col_ndx >= m_cols.size()))
        return false;
    const ColumnBase& col = get_column_base(col_ndx);
    return col.has_search_index();
}


void Table::rebuild_search_index(size_t current_file_format_version)
{
    for (size_t col_ndx = 0; col_ndx < get_column_count(); col_ndx++) {
        if (!has_search_index(col_ndx)) {
            continue;
        }
        ColumnType col_type = get_real_column_type(col_ndx);
        switch (col_type) {
            case col_type_String: {
                StringColumn& col = get_column_string(col_ndx);
                col.get_search_index()->clear();
                col.populate_search_index();
                continue;
            }
            case col_type_Bool:
            case col_type_Int: {
                if (is_nullable(col_ndx)) {
                    IntNullColumn& col = get_column_int_null(col_ndx);
                    col.get_search_index()->clear();
                    col.populate_search_index();
                }
                else {
                    IntegerColumn& col = get_column(col_ndx);
                    col.get_search_index()->clear();
                    col.populate_search_index();
                }
                continue;
            }
            case col_type_StringEnum: {
                StringEnumColumn& col = get_column_string_enum(col_ndx);
                col.get_search_index()->clear();
                col.populate_search_index();
                continue;
            }
            case col_type_Timestamp:
                if (current_file_format_version >= 5) {
                    // If current_file_format_version is less than 5, the index
                    // is created in upgrade_olddatetime function
                    TimestampColumn& col = get_column_timestamp(col_ndx);
                    col.get_search_index()->clear();
                    col.populate_search_index();
                }
                continue;
            case col_type_Binary:
            case col_type_Table:
            case col_type_Mixed:
            case col_type_Float:
            case col_type_Double:
            case col_type_Reserved4:
            case col_type_Link:
            case col_type_LinkList:
            case col_type_BackLink:
                // Indices are not support on these column types
                break;
            case col_type_OldDateTime:
                // This column type should not be found as it should have been converted to col_type_Timestamp
                break;
        }
        REALM_ASSERT(false);
    }
}


void Table::upgrade_olddatetime()
{
    const size_t old_column_count = get_column_count();

    for (size_t col = 0; col < get_column_count(); col++) {
        ColumnType col_type = get_real_column_type(col);

        if (col_type == col_type_OldDateTime) {
            bool nullable = is_nullable(col);
            StringData name = get_column_name(col);

            // Insert new Timestamp column at same position as old column
            const size_t old_col = col + 1;
            const size_t new_col = col;
            insert_column(new_col, type_Timestamp, name, nullable);

            // Copy payload to new column
            for (size_t row = 0; row < size(); row++) {
                if (is_null(old_col, row)) {
                    set_null(new_col, row);
                }
                else {
                    OldDateTime dt = get_olddatetime(old_col, row);
                    Timestamp ts = Timestamp(dt.get_olddatetime(), 0);
                    set_timestamp(new_col, row, ts);
                }
            }

            // If old OldDateTime column had search index, then create one for the new Timestamp column too
            if (has_search_index(old_col)) {
                add_search_index(new_col);
            }

            // Remove old column
            remove_column(old_col);
        }
    }

    REALM_ASSERT_3(old_column_count, ==, get_column_count());
    static_cast<void>(old_column_count);
    for (size_t col = 0; col < get_column_count(); col++) {
        ColumnType col_type = get_real_column_type(col);
        static_cast<void>(col_type);
        REALM_ASSERT(col_type != col_type_OldDateTime);
    }
}


void Table::add_search_index(size_t col_ndx)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    if (REALM_UNLIKELY(has_shared_type()))
        throw LogicError(LogicError::wrong_kind_of_table);

    get_descriptor()->add_search_index(col_ndx);
}


void Table::remove_search_index(size_t col_ndx)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    if (REALM_UNLIKELY(has_shared_type()))
        throw LogicError(LogicError::wrong_kind_of_table);

    get_descriptor()->remove_search_index(col_ndx);
}


void Table::_add_search_index(size_t col_ndx)
{
    ColumnBase& col = get_column_base(col_ndx);

    if (!col.supports_search_index())
        throw LogicError(LogicError::illegal_combination);

    // Create the index
    StringIndex* index = col.create_search_index(); // Throws
    if (!index) {
        throw LogicError(LogicError::illegal_combination);
    }

    // The index goes in the list of column refs immediate after the owning column
    size_t index_pos = m_spec->get_column_info(col_ndx).m_column_ref_ndx + 1;
    index->set_parent(&m_columns, index_pos);
    m_columns.insert(index_pos, index->get_ref()); // Throws

    // Mark the column as having an index
    int attr = m_spec->get_column_attr(col_ndx);
    attr |= col_attr_Indexed;
    m_spec->set_column_attr(col_ndx, ColumnAttr(attr)); // Throws

    // Update column accessors for all columns after the one we just added an
    // index for, as their position in `m_columns` has changed
    refresh_column_accessors(col_ndx + 1); // Throws
}


void Table::_remove_search_index(size_t col_ndx)
{
    // Destroy and remove the index column
    ColumnBase& col = get_column_base(col_ndx);
    col.get_search_index()->destroy();
    col.destroy_search_index();

    // The index is always immediately after the column in m_columns
    size_t index_pos = m_spec->get_column_info(col_ndx).m_column_ref_ndx + 1;
    m_columns.erase(index_pos);

    // Mark the column as no longer having an index
    int attr = m_spec->get_column_attr(col_ndx);
    attr &= ~col_attr_Indexed;
    m_spec->set_column_attr(col_ndx, ColumnAttr(attr)); // Throws

    // Update column accessors for all columns after the one we just removed the
    // index for, as their position in `m_columns` has changed
    refresh_column_accessors(col_ndx + 1); // Throws
}


// FIXME:
//
// Note the two versions of get_column_base(). The difference between
// them is that the non-const version calls
// instantiate_before_change(). This is because a table accessor can
// be created for a subtable that does not yet exist (top-ref = 0),
// and in that case instantiate_before_change() will create the
// missing subtable.
//
// While this on-demand creation of "degenerate" subtables is
// desirebale, the fact that the feature is integrated into
// get_column_base() has turned out to be a bad idea. The problem is
// that every method that calls get_column_base() must also exist in
// two versions, and this applies recursivly all the way out to the
// public methods such as get_subtable().
//
// Rather than having two entirely distinct versions of
// get_subtable(), the const-propagating version should really be a
// thin wrapper around the non-const version. That would be good for
// two reasons, it would reduce the amount of code, and it would make
// it clear to the reader that the two versions really do exactly the
// same thing, apart from the const-propagation. Since get_subtable()
// takes a row index as argument, and a degenerate subtable has no
// rows, there is no way that a valid call to non-const get_subtable()
// can ever end up instantiating a degenrate subtable, so the two
// versions of it perform the exact same function.
//
// Note also that the only Table methods that can ever end up
// instantiating a degenerate table, are those that insert rows,
// because row insertion is the only valid modifying operation on a
// degenerate subtable.
//
// The right thing to do, is therefore to remove the
// instantiate_before_change() call from get_column_base(), and add it
// to the methods that insert rows. This in turn will allow us to
// collapse a large number of methods that currently exist in two
// versions.
//
// Note: get_subtable_ptr() has now been collapsed to one version, but
// the suggested change will still be a significant improvement.

bool Table::is_nullable(size_t col_ndx) const
{
    if (!is_attached()) {
        throw LogicError{LogicError::detached_accessor};
    }

    REALM_ASSERT_DEBUG(col_ndx < m_spec->get_column_count());
    return (m_spec->get_column_attr(col_ndx) & col_attr_Nullable) ||
           m_spec->get_column_type(col_ndx) == col_type_Link;
}

const ColumnBase& Table::get_column_base(size_t ndx) const noexcept
{
    REALM_ASSERT_DEBUG(ndx < m_spec->get_column_count());
    REALM_ASSERT_DEBUG(m_cols.size() == m_spec->get_column_count());
    return *m_cols[ndx];
}

ColumnBase& Table::get_column_base(size_t ndx)
{
    REALM_ASSERT_DEBUG(ndx < m_spec->get_column_count());
    instantiate_before_change();
    REALM_ASSERT_DEBUG(m_cols.size() == m_spec->get_column_count());
    return *m_cols[ndx];
}

const IntegerColumn& Table::get_column(size_t ndx) const noexcept
{
    return get_column<IntegerColumn, col_type_Int>(ndx);
}

IntegerColumn& Table::get_column(size_t ndx)
{
    return get_column<IntegerColumn, col_type_Int>(ndx);
}

const IntNullColumn& Table::get_column_int_null(size_t ndx) const noexcept
{
    return get_column<IntNullColumn, col_type_Int>(ndx);
}

IntNullColumn& Table::get_column_int_null(size_t ndx)
{
    return get_column<IntNullColumn, col_type_Int>(ndx);
}

const StringColumn& Table::get_column_string(size_t ndx) const noexcept
{
    return get_column<StringColumn, col_type_String>(ndx);
}

StringColumn& Table::get_column_string(size_t ndx)
{
    return get_column<StringColumn, col_type_String>(ndx);
}

const StringEnumColumn& Table::get_column_string_enum(size_t ndx) const noexcept
{
    return get_column<StringEnumColumn, col_type_StringEnum>(ndx);
}

StringEnumColumn& Table::get_column_string_enum(size_t ndx)
{
    return get_column<StringEnumColumn, col_type_StringEnum>(ndx);
}

const FloatColumn& Table::get_column_float(size_t ndx) const noexcept
{
    return get_column<FloatColumn, col_type_Float>(ndx);
}

FloatColumn& Table::get_column_float(size_t ndx)
{
    return get_column<FloatColumn, col_type_Float>(ndx);
}

const DoubleColumn& Table::get_column_double(size_t ndx) const noexcept
{
    return get_column<DoubleColumn, col_type_Double>(ndx);
}

DoubleColumn& Table::get_column_double(size_t ndx)
{
    return get_column<DoubleColumn, col_type_Double>(ndx);
}

const BinaryColumn& Table::get_column_binary(size_t ndx) const noexcept
{
    return get_column<BinaryColumn, col_type_Binary>(ndx);
}

BinaryColumn& Table::get_column_binary(size_t ndx)
{
    return get_column<BinaryColumn, col_type_Binary>(ndx);
}

const SubtableColumn& Table::get_column_table(size_t ndx) const noexcept
{
    return get_column<SubtableColumn, col_type_Table>(ndx);
}

SubtableColumn& Table::get_column_table(size_t ndx)
{
    return get_column<SubtableColumn, col_type_Table>(ndx);
}

const MixedColumn& Table::get_column_mixed(size_t ndx) const noexcept
{
    return get_column<MixedColumn, col_type_Mixed>(ndx);
}

MixedColumn& Table::get_column_mixed(size_t ndx)
{
    return get_column<MixedColumn, col_type_Mixed>(ndx);
}

const TimestampColumn& Table::get_column_timestamp(size_t ndx) const noexcept
{
    return get_column<TimestampColumn, col_type_Timestamp>(ndx);
}

TimestampColumn& Table::get_column_timestamp(size_t ndx)
{
    return get_column<TimestampColumn, col_type_Timestamp>(ndx);
}

const LinkColumnBase& Table::get_column_link_base(size_t ndx) const noexcept
{
    const ColumnBase& col_base = get_column_base(ndx);
    REALM_ASSERT(m_spec->get_column_type(ndx) == col_type_Link || m_spec->get_column_type(ndx) == col_type_LinkList);
    const LinkColumnBase& col_link_base = static_cast<const LinkColumnBase&>(col_base);
    return col_link_base;
}

LinkColumnBase& Table::get_column_link_base(size_t ndx)
{
    ColumnBase& col_base = get_column_base(ndx);
    REALM_ASSERT(m_spec->get_column_type(ndx) == col_type_Link || m_spec->get_column_type(ndx) == col_type_LinkList);
    LinkColumnBase& col_link_base = static_cast<LinkColumnBase&>(col_base);
    return col_link_base;
}

const LinkColumn& Table::get_column_link(size_t ndx) const noexcept
{
    return get_column<LinkColumn, col_type_Link>(ndx);
}

LinkColumn& Table::get_column_link(size_t ndx)
{
    return get_column<LinkColumn, col_type_Link>(ndx);
}

const LinkListColumn& Table::get_column_link_list(size_t ndx) const noexcept
{
    return get_column<LinkListColumn, col_type_LinkList>(ndx);
}

LinkListColumn& Table::get_column_link_list(size_t ndx)
{
    return get_column<LinkListColumn, col_type_LinkList>(ndx);
}

const BacklinkColumn& Table::get_column_backlink(size_t ndx) const noexcept
{
    return get_column<BacklinkColumn, col_type_BackLink>(ndx);
}

BacklinkColumn& Table::get_column_backlink(size_t ndx)
{
    return get_column<BacklinkColumn, col_type_BackLink>(ndx);
}


void Table::validate_column_type(const ColumnBase& col, ColumnType col_type, size_t ndx) const
{
    ColumnType real_col_type = get_real_column_type(ndx);
    if (col_type == col_type_Int) {
        REALM_ASSERT(real_col_type == col_type_Int || real_col_type == col_type_Bool ||
                     real_col_type == col_type_OldDateTime);
    }
    else {
        REALM_ASSERT_3(col_type, ==, real_col_type);
    }
    static_cast<void>(col);
}


size_t Table::get_size_from_ref(ref_type spec_ref, ref_type columns_ref, Allocator& alloc) noexcept
{
    ColumnType first_col_type = ColumnType();
    if (!Spec::get_first_column_type_from_ref(spec_ref, alloc, first_col_type))
        return 0;
    const char* columns_header = alloc.translate(columns_ref);
    REALM_ASSERT_3(Array::get_size_from_header(columns_header), !=, 0);
    ref_type first_col_ref = to_ref(Array::get(columns_header, 0));
    Spec spec(alloc);
    spec.init(spec_ref);
    bool nullable = (spec.get_column_attr(0) & col_attr_Nullable) == col_attr_Nullable;
    size_t size = ColumnBase::get_size_from_type_and_ref(first_col_type, first_col_ref, alloc, nullable);
    return size;
}


ref_type Table::create_empty_table(Allocator& alloc)
{
    Array top(alloc);
    _impl::DeepArrayDestroyGuard dg(&top);
    top.create(Array::type_HasRefs); // Throws
    _impl::DeepArrayRefDestroyGuard dg_2(alloc);

    {
        MemRef mem = Spec::create_empty_spec(alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        top.add(v); // Throws
        dg_2.release();
    }
    {
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_HasRefs, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        top.add(v); // Throws
        dg_2.release();
    }
    {
        MemRef mem = ClusterTree::create_empty_cluster(alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        top.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return top.get_ref();
}


ref_type Table::create_column(ColumnType col_type, size_t size, bool nullable, Allocator& alloc)
{
    switch (col_type) {
        case col_type_Int:
        case col_type_Bool:
        case col_type_OldDateTime:
            if (nullable) {
                return IntNullColumn::create(alloc, Array::type_Normal, size); // Throws
            }
            else {
                return IntegerColumn::create(alloc, Array::type_Normal, size); // Throws
            }
        case col_type_Timestamp:
            return TimestampColumn::create(alloc, size, nullable); // Throws
        case col_type_Float: {
            // NOTE: It's very important that 0.0f has the "f" suffix, else the expression will
            // turn into a double and back to float and lose its null-bits on iOS! Dangerous
            // bugs because the bits will be preserved on many other platform and go undetected
            float default_value = nullable ? null::get_null_float<Float>() : 0.0f;
            return FloatColumn::create(alloc, Array::type_Normal, size, default_value); // Throws
        }
        case col_type_Double: {
            double default_value = nullable ? null::get_null_float<Double>() : 0.0;
            return DoubleColumn::create(alloc, Array::type_Normal, size, default_value); // Throws
        }
        case col_type_String:
            return StringColumn::create(alloc, size); // Throws
        case col_type_Binary:
            return BinaryColumn::create(alloc, size, nullable); // Throws
        case col_type_Table:
            return SubtableColumn::create(alloc, size); // Throws
        case col_type_Mixed:
            return MixedColumn::create(alloc, size); // Throws
        case col_type_Link:
            return LinkColumn::create(alloc, size); // Throws
        case col_type_LinkList:
            return LinkListColumn::create(alloc, size); // Throws
        case col_type_BackLink:
            return BacklinkColumn::create(alloc, size); // Throws
        case col_type_StringEnum:
        case col_type_Reserved4:
            break;
    }
    REALM_ASSERT(false);
    return 0;
}


ref_type Table::clone_columns(Allocator& alloc) const
{
    Array new_columns(alloc);
    new_columns.create(Array::type_HasRefs); // Throws
    size_t num_cols = get_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; ++col_ndx) {
        ref_type new_col_ref;
        const ColumnBase* col = &get_column_base(col_ndx);
        MemRef mem = col->clone_deep(alloc);
        new_col_ref = mem.get_ref();
        new_columns.add(int_fast64_t(new_col_ref)); // Throws
    }
    return new_columns.get_ref();
}


ref_type Table::clone(Allocator& alloc) const
{
    if (m_top.is_attached()) {
        MemRef mem = m_top.clone_deep(alloc); // Throws
        return mem.get_ref();
    }

    Array new_top(alloc);
    _impl::DeepArrayDestroyGuard dg(&new_top);
    new_top.create(Array::type_HasRefs); // Throws
    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        MemRef mem = m_spec->m_top.clone_deep(alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        new_top.add(v); // Throws
        dg_2.release();
    }
    {
        MemRef mem = m_columns.clone_deep(alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        new_top.add(v); // Throws
        dg_2.release();
    }
    dg.release();
    return new_top.get_ref();
}


void Table::insert_empty_row(size_t row_ndx, size_t num_rows)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_DEBUG(row_ndx <= m_size);
    REALM_ASSERT_DEBUG(num_rows <= std::numeric_limits<size_t>::max() - row_ndx);

    size_t num_cols = m_spec->get_column_count();
    if (REALM_UNLIKELY(num_cols == 0)) {
        throw LogicError(LogicError::table_has_no_columns);
    }

    bump_version();

    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& col = get_column_base(col_ndx);
        bool insert_nulls = is_nullable(col_ndx);
        col.insert_rows(row_ndx, num_rows, m_size, insert_nulls); // Throws
    }
    if (row_ndx < m_size)
        adj_row_acc_insert_rows(row_ndx, num_rows);
    m_size += num_rows;

    if (Replication* repl = get_repl()) {
        size_t num_rows_to_insert = num_rows;
        size_t prior_num_rows = m_size - num_rows;
        repl->insert_empty_rows(this, row_ndx, num_rows_to_insert, prior_num_rows); // Throws
    }
}

size_t Table::add_row_with_key(size_t key_col_ndx, int64_t key)
{
    size_t num_cols = m_spec->get_column_count();
    size_t row_ndx = m_size;

    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(key_col_ndx, <, num_cols);
    REALM_ASSERT(!is_nullable(key_col_ndx));

    bump_version();

    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        if (col_ndx == key_col_ndx) {
            IntegerColumn& col = get_column(key_col_ndx);
            col.insert(row_ndx, key, 1);
        }
        else {
            ColumnBase& col = get_column_base(col_ndx);
            bool insert_nulls = is_nullable(col_ndx);
            col.insert_rows(row_ndx, 1, m_size, insert_nulls); // Throws
        }
    }
    m_size++;

    if (Replication* repl = get_repl()) {
        size_t prior_num_rows = m_size - 1;
        repl->add_row_with_key(this, row_ndx, prior_num_rows, key_col_ndx, key); // Throws
    }

    return row_ndx;
}


void Table::erase_row(size_t row_ndx, bool is_move_last_over)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(row_ndx, <, m_size);

    bool skip_cascade = !m_spec->has_strong_link_columns();

    // FIXME: Is this really necessary? Waiting for clarification from Thomas
    // Goyne.
    if (Group* g = get_parent_group()) {
        if (g->has_cascade_notification_handler())
            skip_cascade = false;
    }

    if (skip_cascade) {
        bool broken_reciprocal_backlinks = false;
        if (is_move_last_over) {
            do_move_last_over(row_ndx, broken_reciprocal_backlinks); // Throws
        }
        else {
            do_remove(row_ndx, broken_reciprocal_backlinks); // Throws
        }
        return;
    }

    // When the table has strong link columns, row removals may cascade.
    size_t table_ndx = get_index_in_group();
    // Only group-level tables can have link columns
    REALM_ASSERT(table_ndx != realm::npos);

    CascadeState::row row;
    row.is_ordered_removal = (is_move_last_over ? 0 : 1);
    row.table_ndx = table_ndx;
    row.row_ndx = row_ndx;
    CascadeState state;
    state.rows.push_back(row); // Throws

    if (Group* g = get_parent_group())
        state.track_link_nullifications = g->has_cascade_notification_handler();

    cascade_break_backlinks_to(row_ndx, state); // Throws

    if (Group* g = get_parent_group())
        _impl::GroupFriend::send_cascade_notification(*g, state);

    remove_backlink_broken_rows(state); // Throws
}

void Table::remove_recursive(size_t row_ndx)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(row_ndx, <, m_size);

    size_t table_ndx = get_index_in_group();
    // Only group-level tables can have link columns
    REALM_ASSERT(table_ndx != realm::npos);

    CascadeState::row row;
    row.table_ndx = table_ndx;
    row.row_ndx = row_ndx;
    CascadeState state;
    state.rows.push_back(row); // Throws
    state.only_strong_links = false;

    if (Group* g = get_parent_group())
        state.track_link_nullifications = g->has_cascade_notification_handler();

    cascade_break_backlinks_to(row_ndx, state); // Throws

    if (Group* g = get_parent_group())
        _impl::GroupFriend::send_cascade_notification(*g, state);

    remove_backlink_broken_rows(state); // Throws
}

void Table::merge_rows(size_t row_ndx, size_t new_row_ndx)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(row_ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    if (REALM_UNLIKELY(new_row_ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);

    if (Replication* repl = get_repl()) {
        repl->merge_rows(this, row_ndx, new_row_ndx);
    }

    do_merge_rows(row_ndx, new_row_ndx);
}

void Table::batch_erase_rows(const IntegerColumn& row_indexes, bool is_move_last_over)
{
    REALM_ASSERT(is_attached());

    bool skip_cascade = !m_spec->has_strong_link_columns();

    // FIXME: Is this really necessary? Waiting for clarification from Thomas
    // Goyne.
    if (Group* g = get_parent_group()) {
        if (g->has_cascade_notification_handler())
            skip_cascade = false;
    }

    if (skip_cascade) {
        size_t num_rows = row_indexes.size();
        std::vector<size_t> rows;
        rows.reserve(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            int64_t v = row_indexes.get(i);
            if (v != detached_ref) {
                size_t row_ndx = to_size_t(v);
                rows.push_back(row_ndx);
            }
        }
        sort(rows.begin(), rows.end());
        rows.erase(unique(rows.begin(), rows.end()), rows.end());
        // Remove in reverse order to prevent invalidation of recorded row
        // indexes.
        auto rend = rows.rend();
        for (auto i = rows.rbegin(); i != rend; ++i) {
            size_t row_ndx = *i;
            bool broken_reciprocal_backlinks = false;
            if (is_move_last_over) {
                do_move_last_over(row_ndx, broken_reciprocal_backlinks); // Throws
            }
            else {
                do_remove(row_ndx, broken_reciprocal_backlinks); // Throws
            }
        }
        return;
    }

    // When the table has strong link columns, row removals may cascade.
    size_t table_ndx = get_index_in_group();
    // Only group-level tables can have link columns
    REALM_ASSERT(table_ndx != realm::npos);

    CascadeState state;
    size_t num_rows = row_indexes.size();
    state.rows.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        int64_t v = row_indexes.get(i);
        if (v != detached_ref) {
            size_t row_ndx = to_size_t(v);
            CascadeState::row row;
            row.is_ordered_removal = (is_move_last_over ? 0 : 1);
            row.table_ndx = table_ndx;
            row.row_ndx = row_ndx;
            state.rows.push_back(row); // Throws
        }
    }
    sort(std::begin(state.rows), std::end(state.rows));
    state.rows.erase(unique(std::begin(state.rows), std::end(state.rows)), std::end(state.rows));

    if (Group* g = get_parent_group())
        state.track_link_nullifications = g->has_cascade_notification_handler();

    // Iterate over a copy of `rows` since cascading deletes mutate it
    auto rows_copy = state.rows;
    for (auto const& row : rows_copy) {
        cascade_break_backlinks_to(row.row_ndx, state); // Throws
    }

    if (Group* g = get_parent_group())
        _impl::GroupFriend::send_cascade_notification(*g, state);

    remove_backlink_broken_rows(state); // Throws
}


// Replication instruction 'erase-row(unordered=false)' calls this function
// directly with broken_reciprocal_backlinks=false.
void Table::do_remove(size_t row_ndx, bool broken_reciprocal_backlinks)
{
    size_t num_cols = m_spec->get_column_count();
    size_t num_public_cols = m_spec->get_public_column_count();

    // We must start with backlink columns in case the corresponding link
    // columns are in the same table so that the link columns are not updated
    // twice. Backlink columns will nullify the rows in connected link columns
    // first so by the time we get to the link column in this loop, the rows to
    // be removed have already been nullified.
    //
    // This phase also generates replication instructions documenting the side-
    // effects of deleting the object (i.e. link nullifications). These instructions
    // must come before the actual deletion of the object, but at the same time
    // the Replication object may need a consistent view of the row (not including
    // link columns). Therefore we first delete the row in backlink columns, then
    // generate the instruction, and then delete the row in the remaining columns.
    for (size_t col_ndx = num_cols; col_ndx > num_public_cols; --col_ndx) {
        ColumnBase& col = get_column_base(col_ndx - 1);
        size_t prior_num_rows = m_size;
        col.erase_rows(row_ndx, 1, prior_num_rows, broken_reciprocal_backlinks); // Throws
    }

    if (Replication* repl = get_repl()) {
        size_t num_rows_to_erase = 1;
        bool is_move_last_over = false;
        repl->erase_rows(this, row_ndx, num_rows_to_erase, m_size, is_move_last_over); // Throws
    }

    for (size_t col_ndx = num_public_cols; col_ndx > 0; --col_ndx) {
        ColumnBase& col = get_column_base(col_ndx - 1);
        size_t prior_num_rows = m_size;
        col.erase_rows(row_ndx, 1, prior_num_rows, broken_reciprocal_backlinks); // Throws
    }
    adj_row_acc_erase_row(row_ndx);
    --m_size;
    bump_version();
}


// Replication instruction 'erase-row(unordered=true)' calls this function
// directly with broken_reciprocal_backlinks=false.
void Table::do_move_last_over(size_t row_ndx, bool broken_reciprocal_backlinks)
{
    size_t num_cols = m_spec->get_column_count();
    size_t num_public_cols = m_spec->get_public_column_count();

    // We must start with backlink columns in case the corresponding link
    // columns are in the same table so that the link columns are not updated
    // twice. Backlink columns will nullify the rows in connected link columns
    // first so by the time we get to the link column in this loop, the rows to
    // be removed have already been nullified.
    //
    // This phase also generates replication instructions documenting the side-
    // effects of deleting the object (i.e. link nullifications). These instructions
    // must come before the actual deletion of the object, but at the same time
    // the Replication object may need a consistent view of the row (not including
    // link columns). Therefore we first delete the row in backlink columns, then
    // generate the instruction, and then delete the row in the remaining columns.
    for (size_t col_ndx = num_cols; col_ndx > num_public_cols; --col_ndx) {
        ColumnBase& col = get_column_base(col_ndx - 1);
        size_t prior_num_rows = m_size;
        col.move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks); // Throws
    }

    if (Replication* repl = get_repl()) {
        size_t num_rows_to_erase = 1;
        bool is_move_last_over = true;
        repl->erase_rows(this, row_ndx, num_rows_to_erase, m_size, is_move_last_over); // Throws
    }

    for (size_t col_ndx = num_public_cols; col_ndx > 0; --col_ndx) {
        ColumnBase& col = get_column_base(col_ndx - 1);
        size_t prior_num_rows = m_size;
        col.move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks); // Throws
    }

    size_t last_row_ndx = m_size - 1;
    adj_row_acc_move_over(last_row_ndx, row_ndx);
    --m_size;
    bump_version();
}


void Table::do_swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    REALM_ASSERT(row_ndx_1 < row_ndx_2);

    size_t num_cols = m_spec->get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& col = get_column_base(col_ndx);
        col.swap_rows(row_ndx_1, row_ndx_2);
    }
    adj_row_acc_swap_rows(row_ndx_1, row_ndx_2);
    bump_version();
}


void Table::do_move_row(size_t from_ndx, size_t to_ndx)
{
    // If to and from are next to each other we can just convert this to a swap
    if (from_ndx == to_ndx + 1 || to_ndx == from_ndx + 1) {
        if (from_ndx > to_ndx)
            std::swap(from_ndx, to_ndx);
        do_swap_rows(from_ndx, to_ndx);
        return;
    }

    adj_row_acc_move_row(from_ndx, to_ndx);

    // Adjust the row indexes to compensate for the temporary row used
    if (from_ndx > to_ndx)
        ++from_ndx;
    else
        ++to_ndx;

    size_t num_cols = m_spec->get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        bool insert_nulls = m_spec->get_column_type(col_ndx) == col_type_Link;
        bool broken_reciprocal_backlinks = true;

        ColumnBase& col = get_column_base(col_ndx);
        col.insert_rows(to_ndx, 1, m_size, insert_nulls);
        col.swap_rows(from_ndx, to_ndx);
        col.erase_rows(from_ndx, 1, m_size + 1, broken_reciprocal_backlinks);
    }
    bump_version();
}


void Table::do_merge_rows(size_t row_ndx, size_t new_row_ndx)
{
    // This bypasses handling of cascading rows, and we have decided that this is OK, because
    // MergeRows is always followed by MoveLastOver, so breaking the last strong link
    // to a row that is being subsumed will have no observable effect, while honoring the
    // cascading behavior would complicate the calling code somewhat (having to take
    // into account whether or not the row was removed as a consequence of cascade, leading
    // to bugs in case this was forgotten).


    // Since new_row_ndx is guaranteed to be empty at this point, simply swap
    // the rows to get the desired behavior.

    size_t row_ndx_1 = row_ndx, row_ndx_2 = new_row_ndx;
    if (row_ndx_1 > row_ndx_2)
        std::swap(row_ndx_1, row_ndx_2);
    size_t num_cols = m_spec->get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& col = get_column_base(col_ndx);
        if (get_column_type(col_ndx) == type_LinkList) {
            LinkListColumn& link_list_col = static_cast<LinkListColumn&>(col);
            REALM_ASSERT(!link_list_col.has_links(new_row_ndx));
        }
        col.swap_rows(row_ndx_1, row_ndx_2);
    }

    adj_row_acc_merge_rows(row_ndx, new_row_ndx);
    bump_version();
}


void Table::clear()
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    size_t old_size = m_size;

    size_t table_ndx = get_index_in_group();
    if (table_ndx == realm::npos) {
        bool broken_reciprocal_backlinks = false;
        do_clear(broken_reciprocal_backlinks);
    }
    else {
        // Group-level tables may have links, so in those cases we need to
        // discover all the rows that need to be cascade-removed.
        CascadeState state;
        state.stop_on_table = this;
        if (Group* g = get_parent_group())
            state.track_link_nullifications = g->has_cascade_notification_handler();
        cascade_break_backlinks_to_all_rows(state); // Throws

        if (Group* g = get_parent_group())
            _impl::GroupFriend::send_cascade_notification(*g, state);

        bool broken_reciprocal_backlinks = true;
        do_clear(broken_reciprocal_backlinks);

        remove_backlink_broken_rows(state); // Throws
    }

    if (Replication* repl = get_repl())
        repl->clear_table(this, old_size); // Throws
}


// Replication instruction 'clear-table' calls this function
// directly with broken_reciprocal_backlinks=false.
void Table::do_clear(bool broken_reciprocal_backlinks)
{
    size_t num_cols = m_spec->get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& col = get_column_base(col_ndx);
        col.clear(m_size, broken_reciprocal_backlinks); // Throws
    }
    if (m_clusters.is_attached()) {
        m_clusters.clear();
    }

    m_size = 0;

    discard_row_accessors();

    bump_version();
}

void Table::swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(row_ndx_1 >= m_size || row_ndx_2 >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);

    // Internally, core requires that the first row index is strictly less than
    // the second one. The changeset merge mechanism is written to take
    // advantage of it, and it requires it.
    if (row_ndx_1 == row_ndx_2)
        return;
    if (row_ndx_1 > row_ndx_2)
        std::swap(row_ndx_1, row_ndx_2);

    do_swap_rows(row_ndx_1, row_ndx_2);

    if (Replication* repl = get_repl())
        repl->swap_rows(this, row_ndx_1, row_ndx_2);
}

void Table::move_row(size_t from_ndx, size_t to_ndx)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(from_ndx >= m_size || to_ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    if (from_ndx == to_ndx)
        return;

    do_move_row(from_ndx, to_ndx);

    if (Replication* repl = get_repl())
        repl->move_row(this, from_ndx, to_ndx);
}


void Table::set_subtable(size_t col_ndx, size_t row_ndx, const Table* table)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Table);
    REALM_ASSERT_3(row_ndx, <, m_size);
    bump_version();

    SubtableColumn& subtables = get_column_table(col_ndx);
    subtables.set(row_ndx, table);

    // FIXME: Replication is not yet able to handle copying insertion of non-empty tables.
    if (Replication* repl = get_repl())
        repl->set_table(this, col_ndx, row_ndx); // Throws
}


void Table::set_mixed_subtable(size_t col_ndx, size_t row_ndx, const Table* t)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Mixed);
    REALM_ASSERT_3(row_ndx, <, m_size);
    bump_version();

    MixedColumn& mixed_col = get_column_mixed(col_ndx);
    mixed_col.set_subtable(row_ndx, t);

    // FIXME: Replication is not yet able to handle copying assignment of non-empty tables.
    if (Replication* repl = get_repl())
        repl->set_mixed(this, col_ndx, row_ndx, Mixed::subtable_tag()); // Throws
}


TableRef Table::get_subtable_accessor(size_t col_ndx, size_t row_ndx) noexcept
{
    REALM_ASSERT(is_attached());
    // If this table is not a degenerate subtable, then `col_ndx` must be a
    // valid index into `m_cols`.
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_cols.size());
    // The column accessor may not exist yet, but in that case the subtable
    // accessor cannot exist either. Column accessors are missing only during
    // certian operations such as the the updating of the accessor tree when a
    // read transactions is advanced.
    if (m_columns.is_attached()) {
        if (ColumnBase* col = m_cols[col_ndx])
            return col->get_subtable_accessor(row_ndx);
    }
    return {};
}


Table* Table::get_link_target_table_accessor(size_t col_ndx) noexcept
{
    REALM_ASSERT(is_attached());
    // So far, link columns can only exist in group-level tables, so this table
    // cannot be degenerate.
    REALM_ASSERT(m_columns.is_attached());
    REALM_ASSERT_3(col_ndx, <, m_cols.size());
    if (ColumnBase* col = m_cols[col_ndx]) {
        REALM_ASSERT(dynamic_cast<LinkColumnBase*>(col));
        return &static_cast<LinkColumnBase*>(col)->get_target_table();
    }
    return 0;
}


void Table::discard_subtable_accessor(size_t col_ndx, size_t row_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());
    // If this table is not a degenerate subtable, then `col_ndx` must be a
    // valid index into `m_cols`.
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_cols.size());
    if (ColumnBase* col = m_cols[col_ndx])
        col->discard_subtable_accessor(row_ndx);
}


TableRef Table::get_subtable_tableref(size_t col_ndx, size_t row_ndx)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(row_ndx, <, m_size);

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_Table) {
        SubtableColumn& subtables = get_column_table(col_ndx);
        return subtables.get_subtable_tableref(row_ndx); // Throws
    }
    if (type == col_type_Mixed) {
        MixedColumn& subtables = get_column_mixed(col_ndx);
        return subtables.get_subtable_tableref(row_ndx); // Throws
    }
    REALM_ASSERT(false);
    return TableRef();
}


size_t Table::get_subtable_size(size_t col_ndx, size_t row_ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(row_ndx, <, m_size);

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_Table) {
        const SubtableColumn& subtables = get_column_table(col_ndx);
        return subtables.get_subtable_size(row_ndx);
    }
    if (type == col_type_Mixed) {
        const MixedColumn& subtables = get_column_mixed(col_ndx);
        return subtables.get_subtable_size(row_ndx);
    }
    REALM_ASSERT(false);
    return 0;
}


void Table::clear_subtable(size_t col_ndx, size_t row_ndx)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(row_ndx, <=, m_size);
    bump_version();

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_Table) {
        SubtableColumn& subtables = get_column_table(col_ndx);
        subtables.set(row_ndx, 0);

        if (Replication* repl = get_repl())
            repl->set_table(this, col_ndx, row_ndx); // Throws
    }
    else if (type == col_type_Mixed) {
        MixedColumn& subtables = get_column_mixed(col_ndx);
        subtables.set_subtable(row_ndx, nullptr);

        if (Replication* repl = get_repl())
            repl->set_mixed(this, col_ndx, row_ndx, Mixed::subtable_tag()); // Throws
    }
    else {
        REALM_ASSERT(false);
    }
}


const Table* Table::get_parent_table_ptr(size_t* column_ndx_out) const noexcept
{
    REALM_ASSERT_DEBUG(is_attached());
    const Array& real_top = m_top.is_attached() ? m_top : m_columns;
    if (ArrayParent* array_parent = real_top.get_parent()) {
        REALM_ASSERT_DEBUG(dynamic_cast<Parent*>(array_parent));
        Parent* table_parent = static_cast<Parent*>(array_parent);
        return table_parent->get_parent_table(column_ndx_out);
    }
    return 0;
}


size_t Table::get_parent_row_index() const noexcept
{
    REALM_ASSERT(is_attached());
    const Array& real_top = m_top.is_attached() ? m_top : m_columns;
    Parent* parent = static_cast<Parent*>(real_top.get_parent()); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return npos; // Free-standing table
    if (parent->get_parent_group())
        return realm::npos; // Group-level table
    size_t index_in_parent = real_top.get_ndx_in_parent();
    return index_in_parent;
}


Group* Table::get_parent_group() const noexcept
{
    REALM_ASSERT(is_attached());
    if (!m_top.is_attached())
        return 0;                                              // Subtable with shared descriptor
    Parent* parent = static_cast<Parent*>(m_top.get_parent()); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return 0; // Free-standing table
    Group* group = parent->get_parent_group();
    if (!group)
        return 0; // Subtable with independent descriptor
    return group;
}


size_t Table::get_index_in_group() const noexcept
{
    REALM_ASSERT(is_attached());
    if (!m_top.is_attached())
        return realm::npos;                                    // Subtable with shared descriptor
    Parent* parent = static_cast<Parent*>(m_top.get_parent()); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return realm::npos; // Free-standing table
    if (!parent->get_parent_group())
        return realm::npos; // Subtable with independent descriptor
    size_t index_in_parent = m_top.get_ndx_in_parent();
    return index_in_parent;
}

namespace realm {

template <>
bool Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Bool);
    REALM_ASSERT_3(ndx, <, m_size);

    const IntegerColumn& col = get_column(col_ndx);
    return col.get(ndx) != 0;
}

template <>
util::Optional<bool> Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Bool);
    REALM_ASSERT_3(ndx, <, m_size);

    const IntNullColumn& col = get_column_int_null(col_ndx);
    auto value = col.get(ndx);
    return value ? util::some<bool>(*value != 0) : util::none;
}

template <>
int64_t Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Int);
    REALM_ASSERT_3(ndx, <, m_size);

    const IntegerColumn& col = get_column<IntegerColumn, col_type_Int>(col_ndx);
    return col.get(ndx);
}

template <>
util::Optional<int64_t> Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Int);
    REALM_ASSERT_3(ndx, <, m_size);

    const IntNullColumn& col = get_column<IntNullColumn, col_type_Int>(col_ndx);
    return col.get(ndx);
}

template <>
OldDateTime Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_OldDateTime);
    REALM_ASSERT_3(ndx, <, m_size);

    if (is_nullable(col_ndx)) {
        const IntNullColumn& col = get_column<IntNullColumn, col_type_Int>(col_ndx);
        return col.get(ndx).value_or(0);
    }
    else {
        const IntegerColumn& col = get_column<IntegerColumn, col_type_Int>(col_ndx);
        return col.get(ndx);
    }
}

template <>
float Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Float);
    REALM_ASSERT_3(ndx, <, m_size);

    const FloatColumn& col = get_column<FloatColumn, col_type_Float>(col_ndx);
    return col.get(ndx);
}

template <>
util::Optional<float> Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Float);
    REALM_ASSERT_3(ndx, <, m_size);

    const FloatColumn& col = get_column<FloatColumn, col_type_Float>(col_ndx);
    float f = col.get(ndx);
    return null::is_null_float(f) ? util::none : util::make_optional(f);
}

template <>
double Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Double);
    REALM_ASSERT_3(ndx, <, m_size);

    const DoubleColumn& col = get_column<DoubleColumn, col_type_Double>(col_ndx);
    return col.get(ndx);
}

template <>
util::Optional<double> Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Double);
    REALM_ASSERT_3(ndx, <, m_size);

    const DoubleColumn& col = get_column<DoubleColumn, col_type_Double>(col_ndx);
    double d = col.get(ndx);
    return null::is_null_float(d) ? util::none : util::make_optional(d);
}

template <>
StringData Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, m_columns.size());
    REALM_ASSERT_7(get_real_column_type(col_ndx), ==, col_type_String, ||, get_real_column_type(col_ndx), ==,
                   col_type_StringEnum);
    REALM_ASSERT_3(ndx, <, m_size);

    StringData sd;
    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const StringColumn& col = get_column<StringColumn, col_type_String>(col_ndx);
        sd = col.get(ndx);
    }
    else {
        REALM_ASSERT(type == col_type_StringEnum);
        const StringEnumColumn& col = get_column<StringEnumColumn, col_type_StringEnum>(col_ndx);
        sd = col.get(ndx);
    }
    REALM_ASSERT_DEBUG(!(!is_nullable(col_ndx) && sd.is_null()));
    return sd;
}

template <>
BinaryData Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, m_columns.size());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Binary);
    REALM_ASSERT_3(ndx, <, m_size);

    const BinaryColumn& col = get_column<BinaryColumn, col_type_Binary>(col_ndx);
    return col.get(ndx);
}

template <>
Timestamp Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, m_columns.size());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Timestamp);
    REALM_ASSERT_3(ndx, <, m_size);

    const TimestampColumn& col = get_column<TimestampColumn, col_type_Timestamp>(col_ndx);
    return col.get(ndx);
}

template <>
Mixed Table::get(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, m_columns.size());
    REALM_ASSERT_3(ndx, <, m_size);

    const MixedColumn& col = get_column_mixed(col_ndx);

    DataType type = col.get_type(ndx);
    switch (type) {
        case type_Int:
            return Mixed(col.get_int(ndx));
        case type_Bool:
            return Mixed(col.get_bool(ndx));
        case type_OldDateTime:
            return Mixed(OldDateTime(col.get_olddatetime(ndx)));
        case type_Timestamp:
            return Mixed(col.get_timestamp(ndx));
        case type_Float:
            return Mixed(col.get_float(ndx));
        case type_Double:
            return Mixed(col.get_double(ndx));
        case type_String:
            return Mixed(col.get_string(ndx)); // Throws
        case type_Binary:
            return Mixed(col.get_binary(ndx)); // Throws
        case type_Table:
            return Mixed::subtable_tag();
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            break;
    }
    REALM_ASSERT(false);
    return Mixed(int64_t(0));
}

template <>
Table::RowExpr Table::get(size_t col_ndx, size_t row_ndx) const noexcept
{
    REALM_ASSERT_3(row_ndx, <, m_size);
    const LinkColumn& col = get_column_link(col_ndx);
    return RowExpr(&col.get_target_table(), col.get_link(row_ndx));
}

template <>
size_t Table::set_unique(size_t col_ndx, size_t ndx, int_fast64_t value)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(ndx, <, m_size);

    if (!has_search_index(col_ndx)) {
        throw LogicError{LogicError::no_search_index};
    }

    // FIXME: See the definition of check_lists_are_empty() for an explanation
    // of why this is needed.
    check_lists_are_empty(ndx); // Throws

    bump_version();

    bool conflict = false;

    if (is_nullable(col_ndx)) {
        auto& col = get_column_int_null(col_ndx);
        ndx = do_set_unique(col, ndx, value, conflict); // Throws
    }
    else {
        auto& col = get_column(col_ndx);
        ndx = do_set_unique(col, ndx, value, conflict); // Throws
    }

    /*
    // TODO: reintroduce replication
    if (!conflict) {
        if (Replication* repl = get_repl())
            repl->set_int(this, col_ndx, ndx, value, _impl::instr_SetUnique); // Throws
    }
    */

    return ndx;
}

template <>
size_t Table::set_unique(size_t col_ndx, size_t ndx, StringData value)
{
    if (REALM_UNLIKELY(value.size() > max_string_size))
        throw LogicError(LogicError::string_too_big);
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    // For a degenerate subtable, `m_cols.size()` is zero, even when it has a
    // column, however, the previous row index check guarantees that `m_size >
    // 0`, and since `m_size` is also zero for a degenerate subtable, the table
    // cannot be degenerate if we got this far.

    if (!is_nullable(col_ndx) && value.is_null())
        throw LogicError(LogicError::column_not_nullable);

    if (!has_search_index(col_ndx))
        throw LogicError(LogicError::no_search_index);

    // FIXME: See the definition of check_lists_are_empty() for an explanation
    // of why this is needed
    check_lists_are_empty(ndx); // Throws

    bump_version();

    ColumnType actual_type = get_real_column_type(col_ndx);
    REALM_ASSERT(actual_type == ColumnType::col_type_String || actual_type == ColumnType::col_type_StringEnum);

    bool conflict = false;
    // FIXME: String and StringEnum columns should have a common base class
    if (actual_type == ColumnType::col_type_String) {
        StringColumn& col = get_column_string(col_ndx);
        ndx = do_set_unique(col, ndx, value, conflict); // Throws
    }
    else {
        StringEnumColumn& col = get_column_string_enum(col_ndx);
        ndx = do_set_unique(col, ndx, value, conflict); // Throws
    }

    if (!conflict) {
        if (Replication* repl = get_repl())
            repl->set_string(this, col_ndx, ndx, value, _impl::instr_SetUnique); // Throws
    }

    return ndx;
}

template <>
size_t Table::set_unique(size_t col_ndx, size_t row_ndx, null)
{
    if (!is_nullable(col_ndx)) {
        throw LogicError{LogicError::column_not_nullable};
    }
    REALM_ASSERT(!is_link_type(m_spec->get_column_type(col_ndx))); // Use nullify_link().
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(row_ndx, <, m_size);

    if (!has_search_index(col_ndx)) {
        throw LogicError{LogicError::no_search_index};
    }

    // FIXME: See the definition of check_lists_are_empty() for an explanation
    // of why this is needed.
    check_lists_are_empty(row_ndx); // Throws

    bump_version();

    bool conflict = false;

    // Only valid for int columns; use `set_string_unique` to set null strings
    auto& col = get_column_int_null(col_ndx);
    row_ndx = do_set_unique_null(col, row_ndx, conflict); // Throws

    if (!conflict) {
        if (Replication* repl = get_repl())
            repl->set_null(this, col_ndx, row_ndx, _impl::instr_SetUnique); // Throws
    }

    return row_ndx;
}

template <>
void Table::set(size_t col_ndx, size_t ndx, int_fast64_t value, bool /* is_default */)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    if (is_nullable(col_ndx)) {
        auto& col = get_column_int_null(col_ndx);
        col.set(ndx, value);
    }
    else {
        auto& col = get_column(col_ndx);
        col.set(ndx, value);
    }

    /*
    // TODO: reintroduce replication
    if (Replication* repl = get_repl())
        repl->set_int(this, col_ndx, ndx, value, is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
    */
}

template <>
void Table::set(size_t col_ndx, size_t ndx, Timestamp value, bool is_default)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Timestamp);
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    if (!is_nullable(col_ndx) && value.is_null())
        throw LogicError(LogicError::column_not_nullable);

    TimestampColumn& col = get_column<TimestampColumn, col_type_Timestamp>(col_ndx);
    col.set(ndx, value);

    if (Replication* repl = get_repl()) {
        if (value.is_null())
            repl->set_null(this, col_ndx, ndx, is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
        else
            repl->set_timestamp(this, col_ndx, ndx, value,
                                is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
    }
}

template <>
void Table::set(size_t col_ndx, size_t ndx, bool value, bool is_default)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_Bool);
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    if (is_nullable(col_ndx)) {
        IntNullColumn& col = get_column_int_null(col_ndx);
        col.set(ndx, value ? 1 : 0);
    }
    else {
        IntegerColumn& col = get_column(col_ndx);
        col.set(ndx, value ? 1 : 0);
    }

    if (Replication* repl = get_repl())
        repl->set_bool(this, col_ndx, ndx, value, is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

template <>
void Table::set(size_t col_ndx, size_t ndx, OldDateTime value, bool is_default)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(get_real_column_type(col_ndx), ==, col_type_OldDateTime);
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    if (is_nullable(col_ndx)) {
        IntNullColumn& col = get_column_int_null(col_ndx);
        col.set(ndx, value.get_olddatetime());
    }
    else {
        IntegerColumn& col = get_column(col_ndx);
        col.set(ndx, value.get_olddatetime());
    }

    if (Replication* repl = get_repl())
        repl->set_olddatetime(this, col_ndx, ndx, value,
                              is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

template <>
void Table::set(size_t col_ndx, size_t ndx, float value, bool is_default)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    FloatColumn& col = get_column_float(col_ndx);
    col.set(ndx, value);

    if (Replication* repl = get_repl())
        repl->set_float(this, col_ndx, ndx, value, is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

template <>
void Table::set(size_t col_ndx, size_t ndx, double value, bool is_default)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    DoubleColumn& col = get_column_double(col_ndx);
    col.set(ndx, value);

    if (Replication* repl = get_repl())
        repl->set_double(this, col_ndx, ndx, value,
                         is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

template <>
void Table::set(size_t col_ndx, size_t ndx, StringData value, bool is_default)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    // For a degenerate subtable, `m_cols.size()` is zero, even when it has
    // columns, however, the previous row index check guarantees that `m_size >
    // 0`, and since `m_size` is also zero for a degenerate subtable, the table
    // cannot be degenerate if we got this far.
    if (REALM_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);
    if (!is_nullable(col_ndx) && value.is_null())
        throw LogicError(LogicError::column_not_nullable);
    if (REALM_UNLIKELY(value.size() > max_string_size))
        throw LogicError(LogicError::string_too_big);

    bump_version();
    ColumnBase& col = get_column_base(col_ndx);
    col.set_string(ndx, value); // Throws

    if (Replication* repl = get_repl())
        repl->set_string(this, col_ndx, ndx, value,
                         is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

template <>
void Table::set(size_t col_ndx, size_t ndx, BinaryData value, bool is_default)
{
    if (REALM_UNLIKELY(value.size() > ArrayBlob::max_binary_size))
        throw LogicError(LogicError::binary_too_big);
    set_binary_big(col_ndx, ndx, value, is_default);
}

template <>
void Table::set(size_t col_ndx, size_t ndx, Mixed value, bool is_default)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    MixedColumn& col = get_column_mixed(col_ndx);
    DataType type = value.get_type();

    switch (type) {
        case type_Int:
            col.set_int(ndx, value.get_int()); // Throws
            break;
        case type_Bool:
            col.set_bool(ndx, value.get_bool()); // Throws
            break;
        case type_OldDateTime:
            col.set_olddatetime(ndx, value.get_olddatetime()); // Throws
            break;
        case type_Timestamp:
            col.set_timestamp(ndx, value.get_timestamp()); // Throws
            break;
        case type_Float:
            col.set_float(ndx, value.get_float()); // Throws
            break;
        case type_Double:
            col.set_double(ndx, value.get_double()); // Throws
            break;
        case type_String:
            if (REALM_UNLIKELY(value.get_string().size() > max_string_size))
                throw LogicError(LogicError::string_too_big);
            col.set_string(ndx, value.get_string()); // Throws
            break;
        case type_Binary:
            if (REALM_UNLIKELY(value.get_binary().size() > ArrayBlob::max_binary_size))
                throw LogicError(LogicError::binary_too_big);
            col.set_binary(ndx, value.get_binary()); // Throws
            break;
        case type_Table:
            col.set_subtable(ndx, nullptr); // Throws
            break;
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            REALM_ASSERT(false);
            break;
    }

    if (Replication* repl = get_repl())
        repl->set_mixed(this, col_ndx, ndx, value, is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

template <>
void Table::set(size_t col_ndx, size_t row_ndx, null, bool is_default)
{
    if (!is_nullable(col_ndx)) {
        throw LogicError{LogicError::column_not_nullable};
    }
    REALM_ASSERT(!is_link_type(m_spec->get_column_type(col_ndx))); // Use nullify_link().
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(row_ndx, <, m_size);

    bump_version();
    ColumnBase& col = get_column_base(col_ndx);
    col.set_null(row_ndx);

    if (Replication* repl = get_repl())
        repl->set_null(this, col_ndx, row_ndx, is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

template <>
void Table::set(size_t col_ndx, size_t row_ndx, util::Optional<bool> value, bool is_default)
{
    if (value)
        set(col_ndx, row_ndx, *value, is_default);
    else
        set(col_ndx, row_ndx, null(), is_default);
}

template <>
void Table::set(size_t col_ndx, size_t row_ndx, util::Optional<int64_t> value, bool is_default)
{
    if (value)
        set(col_ndx, row_ndx, *value, is_default);
    else
        set(col_ndx, row_ndx, null(), is_default);
}

template <>
void Table::set(size_t col_ndx, size_t row_ndx, util::Optional<float> value, bool is_default)
{
    if (value)
        set(col_ndx, row_ndx, *value, is_default);
    else
        set(col_ndx, row_ndx, null(), is_default);
}

template <>
void Table::set(size_t col_ndx, size_t row_ndx, util::Optional<double> value, bool is_default)
{
    if (value)
        set(col_ndx, row_ndx, *value, is_default);
    else
        set(col_ndx, row_ndx, null(), is_default);
}

} // namespace realm;


template <class ColType, class T>
size_t Table::do_find_unique(ColType& col, size_t ndx, T&& value, bool& conflict)
{
    size_t winner = size_t(-1);

    while (true) {
        winner = col.find_first(value, winner + 1);
        if (winner == ndx)
            continue;
        if (winner == not_found)
            return ndx;
        else
            break;
    }

    conflict = true;

    REALM_ASSERT(winner != not_found);
    REALM_ASSERT(winner != ndx);

    // Delete additional duplicates.
    size_t duplicate = winner;
    while (true) {
        duplicate = col.find_first(value, duplicate + 1);
        if (duplicate == ndx)
            continue;
        if (duplicate == not_found)
            break;
        if (ndx == size() - 1)
            ndx = duplicate;

        adj_row_acc_merge_rows(duplicate, winner);
        move_last_over(duplicate);
        // Re-check moved-last-over
        duplicate -= 1;
    }

    // Delete candidate.
    if (winner == size() - 1)
        winner = ndx;

    adj_row_acc_merge_rows(ndx, winner);
    move_last_over(ndx);

    return winner;
}

template <class ColType>
size_t Table::do_set_unique_null(ColType& col, size_t ndx, bool& conflict)
{
    ndx = do_find_unique(col, ndx, null{}, conflict);
    col.set_null(ndx);
    return ndx;
}

template <class ColType, class T>
size_t Table::do_set_unique(ColType& col, size_t ndx, T&& value, bool& conflict)
{
    ndx = do_find_unique(col, ndx, value, conflict);
    col.set(ndx, value);
    return ndx;
}

void Table::add_int(size_t col_ndx, size_t ndx, int_fast64_t value)
{
    REALM_ASSERT_3(col_ndx, <, get_column_count());
    REALM_ASSERT_3(ndx, <, m_size);
    bump_version();

    auto add_wrap = [](int64_t a, int64_t b) -> int64_t {
        uint64_t ua = uint64_t(a);
        uint64_t ub = uint64_t(b);
        return int64_t(ua + ub);
    };

    if (is_nullable(col_ndx)) {
        auto& col = get_column_int_null(col_ndx);
        Optional<int64_t> old = col.get(ndx);
        if (old) {
            col.set(ndx, add_wrap(*old, value));
        }
        else {
            throw LogicError{LogicError::illegal_combination};
        }
    }
    else {
        auto& col = get_column(col_ndx);
        int64_t old = col.get(ndx);
        col.set(ndx, add_wrap(old, value));
    }

    if (Replication* repl = get_repl())
        repl->add_int(this, col_ndx, ndx, value); // Throws
}

void Table::insert_substring(size_t col_ndx, size_t row_ndx, size_t pos, StringData value)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(row_ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    // For a degenerate subtable, `m_cols.size()` is zero, even when it has
    // columns, however, the previous row index check guarantees that `m_size >
    // 0`, and since `m_size` is also zero for a degenerate subtable, the table
    // cannot be degenerate if we got this far.
    if (REALM_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);

    // FIXME: Loophole: Assertion violation in Table::get_string() on column type mismatch.
    StringData old_value = get_string(col_ndx, row_ndx);
    if (REALM_UNLIKELY(pos > old_value.size()))
        throw LogicError(LogicError::string_position_out_of_range);
    if (REALM_UNLIKELY(value.size() > max_string_size - old_value.size()))
        throw LogicError(LogicError::string_too_big);

    std::string copy_of_value = old_value;                 // Throws
    copy_of_value.insert(pos, value.data(), value.size()); // Throws

    bump_version();
    ColumnBase& col = get_column_base(col_ndx);
    col.set_string(row_ndx, copy_of_value); // Throws

    if (Replication* repl = get_repl())
        repl->insert_substring(this, col_ndx, row_ndx, pos, value); // Throws
}


void Table::remove_substring(size_t col_ndx, size_t row_ndx, size_t pos, size_t substring_size)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(row_ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    // For a degenerate subtable, `m_cols.size()` is zero, even when it has
    // columns, however, the previous row index check guarantees that `m_size >
    // 0`, and since `m_size` is also zero for a degenerate subtable, the table
    // cannot be degenerate if we got this far.
    if (REALM_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);

    // FIXME: Loophole: Assertion violation in Table::get_string() on column type mismatch.
    StringData old_value = get_string(col_ndx, row_ndx);
    if (REALM_UNLIKELY(pos > old_value.size()))
        throw LogicError(LogicError::string_position_out_of_range);

    std::string copy_of_value = old_value;    // Throws
    copy_of_value.erase(pos, substring_size); // Throws

    bump_version();
    ColumnBase& col = get_column_base(col_ndx);
    col.set_string(row_ndx, copy_of_value); // Throws

    if (Replication* repl = get_repl()) {
        size_t actual_size = old_value.size() - copy_of_value.size();
        repl->erase_substring(this, col_ndx, row_ndx, pos, actual_size); // Throws
    }
}

void Table::set_binary_big(size_t col_ndx, size_t ndx, BinaryData value, bool is_default)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    // For a degenerate subtable, `m_cols.size()` is zero, even when it has
    // columns, however, the previous row index check guarantees that `m_size >
    // 0`, and since `m_size` is also zero for a degenerate subtable, the table
    // cannot be degenerate if we got this far.
    if (REALM_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);
    if (!is_nullable(col_ndx) && value.is_null())
        throw LogicError(LogicError::column_not_nullable);
    bump_version();

    // FIXME: Loophole: Assertion violation in Table::get_column_binary() on
    // column type mismatch.
    BinaryColumn& col = get_column_binary(col_ndx);
    col.set(ndx, value);

    if (Replication* repl = get_repl())
        repl->set_binary(this, col_ndx, ndx, value,
                         is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
}

BinaryData Table::get_binary_at(size_t col_ndx, size_t ndx, size_t& pos) const noexcept
{
    return get_column<BinaryColumn, col_type_Binary>(col_ndx).get_at(ndx, pos);
}


DataType Table::get_mixed_type(size_t col_ndx, size_t ndx) const noexcept
{
    REALM_ASSERT_3(col_ndx, <, m_columns.size());
    REALM_ASSERT_3(ndx, <, m_size);

    const MixedColumn& col = get_column_mixed(col_ndx);
    return col.get_type(ndx);
}


size_t Table::get_link(size_t col_ndx, size_t row_ndx) const noexcept
{
    REALM_ASSERT_3(row_ndx, <, m_size);
    const LinkColumn& col = get_column_link(col_ndx);
    return col.get_link(row_ndx);
}


TableRef Table::get_link_target(size_t col_ndx) noexcept
{
    LinkColumnBase& col = get_column_link_base(col_ndx);
    return col.get_target_table().get_table_ref();
}


void Table::set_link(size_t col_ndx, size_t row_ndx, size_t target_row_ndx, bool is_default)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(row_ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    // For a degenerate subtable, `m_cols.size()` is zero, even when it has a
    // column, however, the previous row index check guarantees that `m_size >
    // 0`, and since `m_size` is also zero for a degenerate subtable, the table
    // cannot be degenerate if we got this far.
    if (REALM_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);
    LinkColumn& col = get_column_link(col_ndx);
    Table& target_table = col.get_target_table();
    if (REALM_UNLIKELY(target_row_ndx != realm::npos && target_row_ndx >= target_table.size()))
        throw LogicError(LogicError::target_row_index_out_of_range);

    // FIXME: There is still no proper check for column type mismatch. One
    // solution would be to go in the direction of set_string() (using
    // ColumnBase::set_string()), but that works less well here. The ideal
    // solution seems to be to have a very efficient way of checking the column
    // type. Idea: Introduce `DataType ColumnBase::m_type`.

    if (Replication* repl = get_repl())
        repl->set_link(this, col_ndx, row_ndx, target_row_ndx,
                       is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    size_t old_target_row_ndx = do_set_link(col_ndx, row_ndx, target_row_ndx); // Throws
    if (old_target_row_ndx == realm::npos)
        return;

    if (col.get_weak_links())
        return;

    size_t num_remaining = target_table.get_backlink_count(old_target_row_ndx,
                                                           /* only strong links:*/ true);
    if (num_remaining > 0)
        return;

    CascadeState::row target_row;
    target_row.table_ndx = target_table.get_index_in_group();
    target_row.row_ndx = old_target_row_ndx;
    CascadeState state;
    state.rows.push_back(target_row);

    if (Group* g = get_parent_group())
        state.track_link_nullifications = g->has_cascade_notification_handler();

    target_table.cascade_break_backlinks_to(old_target_row_ndx, state); // Throws

    if (Group* g = get_parent_group())
        _impl::GroupFriend::send_cascade_notification(*g, state);

    remove_backlink_broken_rows(state); // Throws
}


// Replication instruction 'set_link' calls this function directly.
size_t Table::do_set_link(size_t col_ndx, size_t row_ndx, size_t target_row_ndx)
{
    REALM_ASSERT_3(row_ndx, <, m_size);
    LinkColumn& col = get_column_link(col_ndx);
    size_t old_target_row_ndx = col.set_link(row_ndx, target_row_ndx);
    bump_version();
    return old_target_row_ndx;
}


ConstLinkViewRef Table::get_linklist(size_t col_ndx, size_t row_ndx) const
{
    REALM_ASSERT_3(row_ndx, <, m_size);
    const LinkListColumn& col = get_column_link_list(col_ndx);
    return col.get(row_ndx);
}


LinkViewRef Table::get_linklist(size_t col_ndx, size_t row_ndx)
{
    REALM_ASSERT_3(row_ndx, <, m_size);
    LinkListColumn& col = get_column_link_list(col_ndx);
    return col.get(row_ndx);
}


bool Table::linklist_is_empty(size_t col_ndx, size_t row_ndx) const noexcept
{
    REALM_ASSERT_3(row_ndx, <, m_size);
    const LinkListColumn& col = get_column_link_list(col_ndx);
    return !col.has_links(row_ndx);
}


size_t Table::get_link_count(size_t col_ndx, size_t row_ndx) const noexcept
{
    REALM_ASSERT_3(row_ndx, <, m_size);
    const LinkListColumn& col = get_column_link_list(col_ndx);
    return col.get_link_count(row_ndx);
}


bool Table::is_null(size_t col_ndx, size_t row_ndx) const noexcept
{
    if (!is_nullable(col_ndx))
        return false;
    auto& col = get_column_base(col_ndx);
    return col.is_null(row_ndx);
}


// count ----------------------------------------------

size_t Table::count_int(size_t col_ndx, int64_t value) const
{
    if (!m_columns.is_attached())
        return 0;

    const IntegerColumn& col = get_column<IntegerColumn, col_type_Int>(col_ndx);
    return col.count(value);
}
size_t Table::count_float(size_t col_ndx, float value) const
{
    if (!m_columns.is_attached())
        return 0;

    const FloatColumn& col = get_column<FloatColumn, col_type_Float>(col_ndx);
    return col.count(value);
}
size_t Table::count_double(size_t col_ndx, double value) const
{
    if (!m_columns.is_attached())
        return 0;

    const DoubleColumn& col = get_column<DoubleColumn, col_type_Double>(col_ndx);
    return col.count(value);
}
size_t Table::count_string(size_t col_ndx, StringData value) const
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < get_column_count());

    if (!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const StringColumn& col = get_column_string(col_ndx);
        return col.count(value);
    }
    else {
        REALM_ASSERT_3(type, ==, col_type_StringEnum);
        const StringEnumColumn& col = get_column_string_enum(col_ndx);
        return col.count(value);
    }
}

// sum ----------------------------------------------

int64_t Table::sum_int(size_t col_ndx) const
{
    if (!m_columns.is_attached())
        return 0;

    if (is_nullable(col_ndx)) {
        const IntNullColumn& col = get_column<IntNullColumn, col_type_Int>(col_ndx);
        return col.sum();
    }
    else {
        const IntegerColumn& col = get_column<IntegerColumn, col_type_Int>(col_ndx);
        return col.sum();
    }
}
double Table::sum_float(size_t col_ndx) const
{
    if (!m_columns.is_attached())
        return 0.f;

    const FloatColumn& col = get_column<FloatColumn, col_type_Float>(col_ndx);
    return col.sum();
}
double Table::sum_double(size_t col_ndx) const
{
    if (!m_columns.is_attached())
        return 0.;

    const DoubleColumn& col = get_column<DoubleColumn, col_type_Double>(col_ndx);
    return col.sum();
}

// average ----------------------------------------------

double Table::average_int(size_t col_ndx, size_t* value_count) const
{
    if (!m_columns.is_attached())
        return 0;

    if (is_nullable(col_ndx)) {
        const IntNullColumn& col = get_column<IntNullColumn, col_type_Int>(col_ndx);
        return col.average(0, -1, -1, value_count);
    }
    else {
        const IntegerColumn& col = get_column<IntegerColumn, col_type_Int>(col_ndx);
        return col.average(0, -1, -1, value_count);
    }
}
double Table::average_float(size_t col_ndx, size_t* value_count) const
{
    if (!m_columns.is_attached())
        return 0.f;

    const FloatColumn& col = get_column<FloatColumn, col_type_Float>(col_ndx);
    return col.average(0, -1, -1, value_count);
}
double Table::average_double(size_t col_ndx, size_t* value_count) const
{
    if (!m_columns.is_attached())
        return 0.;

    const DoubleColumn& col = get_column<DoubleColumn, col_type_Double>(col_ndx);
    return col.average(0, -1, -1, value_count);
}

// minimum ----------------------------------------------

#define USE_COLUMN_AGGREGATE 1

int64_t Table::minimum_int(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0;

#if USE_COLUMN_AGGREGATE
    if (is_nullable(col_ndx)) {
        const IntNullColumn& col = get_column<IntNullColumn, col_type_Int>(col_ndx);
        return col.minimum(0, npos, npos, return_ndx);
    }
    else {
        const IntegerColumn& col = get_column<IntegerColumn, col_type_Int>(col_ndx);
        return col.minimum(0, npos, npos, return_ndx);
    }
#else
    if (is_empty())
        return 0;

    int64_t mv = get_int(col_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        int64_t v = get_int(col_ndx, i);
        if (v < mv) {
            mv = v;
        }
    }
    return mv;
#endif
}

float Table::minimum_float(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0.f;

    const FloatColumn& col = get_column<FloatColumn, col_type_Float>(col_ndx);
    return col.minimum(0, npos, npos, return_ndx);
}

double Table::minimum_double(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0.;

    const DoubleColumn& col = get_column<DoubleColumn, col_type_Double>(col_ndx);
    return col.minimum(0, npos, npos, return_ndx);
}

OldDateTime Table::minimum_olddatetime(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0;

    if (is_nullable(col_ndx)) {
        const IntNullColumn& col = get_column<IntNullColumn, col_type_OldDateTime>(col_ndx);
        return col.minimum(0, npos, npos, return_ndx);
    }
    else {
        const IntegerColumn& col = get_column<IntegerColumn, col_type_OldDateTime>(col_ndx);
        return col.minimum(0, npos, npos, return_ndx);
    }
}

Timestamp Table::minimum_timestamp(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return Timestamp{};

    const TimestampColumn& col = get_column<TimestampColumn, col_type_Timestamp>(col_ndx);
    return col.minimum(return_ndx);
}

// maximum ----------------------------------------------

int64_t Table::maximum_int(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0;

#if USE_COLUMN_AGGREGATE
    if (is_nullable(col_ndx)) {
        const IntNullColumn& col = get_column_int_null(col_ndx);
        return col.maximum(0, npos, npos, return_ndx);
    }
    else {
        const IntegerColumn& col = get_column(col_ndx);
        return col.maximum(0, npos, npos, return_ndx);
    }

#else
    if (is_empty())
        return 0;

    int64_t mv = get_int(col_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        int64_t v = get_int(col_ndx, i);
        if (v > mv) {
            mv = v;
        }
    }
    return mv;
#endif
}

float Table::maximum_float(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0.f;

    const FloatColumn& col = get_column<FloatColumn, col_type_Float>(col_ndx);
    return col.maximum(0, npos, npos, return_ndx);
}

double Table::maximum_double(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0.;

    const DoubleColumn& col = get_column<DoubleColumn, col_type_Double>(col_ndx);
    return col.maximum(0, npos, npos, return_ndx);
}

OldDateTime Table::maximum_olddatetime(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return 0;

    if (is_nullable(col_ndx)) {
        const IntNullColumn& col = get_column<IntNullColumn, col_type_OldDateTime>(col_ndx);
        return col.maximum(0, npos, npos, return_ndx);
    }
    else {
        const IntegerColumn& col = get_column<IntegerColumn, col_type_OldDateTime>(col_ndx);
        return col.maximum(0, npos, npos, return_ndx);
    }
}


Timestamp Table::maximum_timestamp(size_t col_ndx, size_t* return_ndx) const
{
    if (!m_columns.is_attached())
        return Timestamp{};

    const TimestampColumn& col = get_column<TimestampColumn, col_type_Timestamp>(col_ndx);
    return col.maximum(return_ndx);
}


namespace {

template <class T>
T upgrade_optional_int(T value)
{
    // No conversion
    return value;
}

} // anonymous namespace


namespace realm {
template <class T>
Key Table::find_first(size_t col_ndx, T value) const
{
    Key key;
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    LeafType leaf(get_alloc());
    traverse_clusters([&key, &col_ndx, &value, &leaf](const Cluster* cluster, int64_t key_offset) {
        cluster->init_leaf<LeafType>(col_ndx, &leaf);
        size_t row = leaf.find_first(value, 0, cluster->node_size());
        if (row != realm::npos) {
            key = Key(cluster->get_key(row) + key_offset);
            return true;
        }
        return false;
    });
    return key;
}

template <>
Key Table::find_first(size_t col_ndx, util::Optional<float> value) const
{
    return value ? find_first(col_ndx, *value) : find_first_null(col_ndx);
}

template <>
Key Table::find_first(size_t col_ndx, util::Optional<double> value) const
{
    return value ? find_first(col_ndx, *value) : find_first_null(col_ndx);
}

template <>
Key Table::find_first(size_t col_ndx, null) const
{
    return find_first_null(col_ndx);
}

// Explicitly instantiate the generic case of the template for the types we care about.
template Key Table::find_first(size_t col_ndx, bool) const;
template Key Table::find_first(size_t col_ndx, int64_t) const;
template Key Table::find_first(size_t col_ndx, float) const;
template Key Table::find_first(size_t col_ndx, double) const;
template Key Table::find_first(size_t col_ndx, util::Optional<bool>) const;
template Key Table::find_first(size_t col_ndx, util::Optional<int64_t>) const;
template Key Table::find_first(size_t col_ndx, BinaryData) const;

} // namespace realm

Key Table::find_first_link(size_t) const
{
    /*
        auto target_row = get_link_target(m_link_chain[0])->get(target_row_index);
        size_t ret = where().links_to(m_link_chain[0], target_row).find();
        m_link_chain.clear();
        return ret;
    */
    // TODO
    return null_key;
}

Key Table::find_first_int(size_t col_ndx, int64_t value) const
{
    if (is_nullable(col_ndx))
        return find_first<util::Optional<int64_t>>(col_ndx, value);
    else
        return find_first<int64_t>(col_ndx, value);
}

Key Table::find_first_bool(size_t col_ndx, bool value) const
{
    if (is_nullable(col_ndx))
        return find_first<util::Optional<bool>>(col_ndx, value);
    else
        return find_first<bool>(col_ndx, value);
}

Key Table::find_first_timestamp(size_t col_ndx, Timestamp value) const
{
    return find_first(col_ndx, value);
}

Key Table::find_first_float(size_t col_ndx, float value) const
{
    return find_first<Float>(col_ndx, value);
}

Key Table::find_first_double(size_t col_ndx, double value) const
{
    return find_first<Double>(col_ndx, value);
}

Key Table::find_first_string(size_t col_ndx, StringData value) const
{
    return find_first(col_ndx, value);
}

Key Table::find_first_binary(size_t col_ndx, BinaryData value) const
{
    return find_first<BinaryData>(col_ndx, value);
}

Key Table::find_first_null(size_t) const
{
    // return where().equal(column_ndx, null{}).find();
    // TODO
    return null_key;
}

template <class T>
TableView Table::find_all(size_t col_ndx, T value)
{
    return where().equal(col_ndx, value).find_all();
}

TableView Table::find_all_link(size_t target_row_index)
{
    auto target_row = get_link_target(m_link_chain[0])->get(target_row_index);
    TableView tv = where().links_to(m_link_chain[0], target_row).find_all();
    m_link_chain.clear();
    return tv;
}

ConstTableView Table::find_all_link(size_t target_row_index) const
{
    return const_cast<Table*>(this)->find_all_link(target_row_index);
}

TableView Table::find_all_int(size_t col_ndx, int64_t value)
{
    return find_all<int64_t>(col_ndx, value);
}

ConstTableView Table::find_all_int(size_t col_ndx, int64_t value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_ndx, value);
}

TableView Table::find_all_bool(size_t col_ndx, bool value)
{
    return find_all<bool>(col_ndx, value);
}

ConstTableView Table::find_all_bool(size_t col_ndx, bool value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_ndx, value);
}


TableView Table::find_all_float(size_t col_ndx, float value)
{
    return find_all<float>(col_ndx, value);
}

ConstTableView Table::find_all_float(size_t col_ndx, float value) const
{
    return const_cast<Table*>(this)->find_all<float>(col_ndx, value);
}

TableView Table::find_all_double(size_t col_ndx, double value)
{
    return find_all<double>(col_ndx, value);
}

ConstTableView Table::find_all_double(size_t col_ndx, double value) const
{
    return const_cast<Table*>(this)->find_all<double>(col_ndx, value);
}

TableView Table::find_all_olddatetime(size_t col_ndx, OldDateTime value)
{
    return find_all<int64_t>(col_ndx, int64_t(value.get_olddatetime()));
}

ConstTableView Table::find_all_olddatetime(size_t col_ndx, OldDateTime value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_ndx, int64_t(value.get_olddatetime()));
}

TableView Table::find_all_string(size_t col_ndx, StringData value)
{
    return where().equal(col_ndx, value).find_all();
}

ConstTableView Table::find_all_string(size_t col_ndx, StringData value) const
{
    return const_cast<Table*>(this)->find_all_string(col_ndx, value);
}

TableView Table::find_all_binary(size_t, BinaryData)
{
    // FIXME: Implement this!
    throw std::runtime_error("Not implemented");
}

ConstTableView Table::find_all_binary(size_t, BinaryData) const
{
    // FIXME: Implement this!
    throw std::runtime_error("Not implemented");
}

TableView Table::find_all_null(size_t col_ndx)
{
    return where().equal(col_ndx, null{}).find_all();
}

ConstTableView Table::find_all_null(size_t col_ndx) const
{
    return const_cast<Table*>(this)->find_all_null(col_ndx);
}

TableView Table::get_distinct_view(size_t col_ndx)
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());

    TableView tv(TableView::DistinctView, *this, col_ndx);
    tv.do_sync();
    return tv;
}

ConstTableView Table::get_distinct_view(size_t col_ndx) const
{
    return const_cast<Table*>(this)->get_distinct_view(col_ndx);
}

TableView Table::get_sorted_view(size_t col_ndx, bool ascending)
{
    TableView tv = where().find_all();
    tv.sort(col_ndx, ascending);
    return tv;
}

ConstTableView Table::get_sorted_view(size_t col_ndx, bool ascending) const
{
    return const_cast<Table*>(this)->get_sorted_view(col_ndx, ascending);
}

TableView Table::get_sorted_view(SortDescriptor order)
{
    TableView tv = where().find_all();
    tv.sort(std::move(order));
    return tv;
}

ConstTableView Table::get_sorted_view(SortDescriptor order) const
{
    return const_cast<Table*>(this)->get_sorted_view(std::move(order));
}

const Table* Table::get_link_chain_target(const std::vector<size_t>& link_chain) const
{
    const Table* table = this;
    for (size_t t = 0; t < link_chain.size(); t++) {
        // Link column can be a single Link, LinkList, or BackLink.
        ColumnType type = table->get_real_column_type(link_chain[t]);
        if (type == col_type_LinkList) {
            const LinkListColumn& cll = table->get_column_link_list(link_chain[t]);
            table = &cll.get_target_table();
        }
        else if (type == col_type_Link) {
            const LinkColumn& cl = table->get_column_link(link_chain[t]);
            table = &cl.get_target_table();
        }
        else if (type == col_type_BackLink) {
            const BacklinkColumn& bl = table->get_column_backlink(link_chain[t]);
            table = &bl.get_origin_table();
        }
        else {
            // Only last column in link chain is allowed to be non-link
            if (t + 1 != link_chain.size())
                throw(LogicError::type_mismatch);
        }
    }
    return table;
}


namespace {

struct AggrState {
    AggrState(const Table& target_table)
        : table(target_table)
        , cache(table.get_alloc())
        , added_row(false)
    {
    }

    const Table& table;
    const StringIndex* dst_index;
    size_t group_by_column;

    const StringEnumColumn* enums;
    std::vector<size_t> keys;
    const ArrayInteger* block = nullptr;
    ArrayInteger cache;
    size_t offset;
    size_t block_end;

    bool added_row;
};

typedef size_t (*get_group_fnc)(size_t, AggrState&, Table&);

size_t get_group_ndx(size_t i, AggrState& state, Table& result)
{
    StringData str = state.table.get_string(state.group_by_column, i);
    size_t ndx = state.dst_index->find_first(str);
    if (ndx == not_found) {
        ndx = result.add_empty_row();
        result.set_string(0, ndx, str);
        state.added_row = true;
    }
    return ndx;
}

size_t get_group_ndx_blocked(size_t i, AggrState& state, Table& result)
{
    // We iterate entire blocks at a time by keeping current leaf cached
    if (i >= state.block_end) {
        size_t ndx_in_leaf;
        IntegerColumn::LeafInfo leaf{&state.block, &state.cache};
        state.enums->IntegerColumn::get_leaf(i, ndx_in_leaf, leaf);
        state.offset = i - ndx_in_leaf;
        state.block_end = state.offset + state.block->size();
    }

    // Since we know the exact number of distinct keys,
    // we can use that to avoid index lookups
    int64_t key = state.block->get(i - state.offset);
    size_t ndx = state.keys[to_size_t(key)];

    // Stored position is offset by one, so zero can indicate
    // that no entry have been added yet.
    if (ndx == 0) {
        ndx = result.add_empty_row();
        result.set_string(0, ndx, state.enums->get(i));
        state.keys[to_size_t(key)] = ndx + 1;
        state.added_row = true;
    }
    else
        --ndx;
    return ndx;
}

} // namespace

// Simple pivot aggregate method. Experimental! Please do not document method publicly.
void Table::aggregate(size_t group_by_column, size_t aggr_column, AggrType op, Table& result,
                      const IntegerColumn* viewrefs) const
{
    REALM_ASSERT(result.is_empty() && result.get_column_count() == 0);
    REALM_ASSERT_3(group_by_column, <, m_columns.size());
    REALM_ASSERT_3(aggr_column, <, m_columns.size());

    REALM_ASSERT_3(get_column_type(group_by_column), ==, type_String);
    REALM_ASSERT(op == aggr_count || get_column_type(aggr_column) == type_Int);

    // Add columns to result table
    result.add_column(type_String, get_column_name(group_by_column));

    if (op == aggr_count)
        result.add_column(type_Int, "COUNT()");
    else
        result.add_column(type_Int, get_column_name(aggr_column));

    // Cache columms
    const IntegerColumn& src_column = get_column(aggr_column);
    IntegerColumn& dst_column = result.get_column(1);

    AggrState state(*this);
    get_group_fnc get_group_ndx_fnc = nullptr;

    // When doing grouped aggregates, the column to group on is likely
    // to be auto-enumerated (without a lot of duplicates grouping does not
    // make much sense). So we can use this knowledge to optimize the process.
    ColumnType key_type = get_real_column_type(group_by_column);
    if (key_type == col_type_StringEnum) {
        const StringEnumColumn& enums = get_column_string_enum(group_by_column);
        size_t key_count = enums.get_keys().size();

        state.enums = &enums;
        state.keys.assign(key_count, 0);

        size_t ndx_in_leaf;
        IntegerColumn::LeafInfo leaf{&state.block, &state.cache};
        enums.IntegerColumn::get_leaf(0, ndx_in_leaf, leaf);
        state.offset = 0 - ndx_in_leaf;
        state.block_end = state.offset + state.block->size();
        get_group_ndx_fnc = &get_group_ndx_blocked;
    }
    else {
        // If the group_by column is not auto-enumerated, we have to do
        // (more expensive) direct lookups.
        result.add_search_index(0);
        const StringIndex& dst_index = *result.get_column_string(0).get_search_index();

        state.dst_index = &dst_index;
        state.group_by_column = group_by_column;
        get_group_ndx_fnc = &get_group_ndx;
    }

    if (viewrefs) {
        // Aggregating over a view
        const size_t count = viewrefs->size();

        switch (op) {
            case aggr_count:
                for (size_t r = 0; r < count; ++r) {
                    size_t i = static_cast<size_t>(viewrefs->get(r));
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // Count
                    dst_column.adjust(ndx, 1);
                }
                break;
            case aggr_sum:
                for (size_t r = 0; r < count; ++r) {
                    size_t i = static_cast<size_t>(viewrefs->get(r));
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // Sum
                    int64_t value = src_column.get(i);
                    dst_column.adjust(ndx, value);
                }
                break;
            case aggr_avg: {
                // Add temporary column for counts
                result.add_column(type_Int, "count");
                IntegerColumn& cnt_column = result.get_column(2);

                for (size_t r = 0; r < count; ++r) {
                    size_t i = static_cast<size_t>(viewrefs->get(r));
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // SUM
                    int64_t value = src_column.get(i);
                    dst_column.adjust(ndx, value);

                    // Increment count
                    cnt_column.adjust(ndx, 1);
                }

                // Calculate averages
                result.add_column(type_Double, "average");
                DoubleColumn& mean_column = result.get_column_double(3);
                const size_t res_count = result.size();
                for (size_t i = 0; i < res_count; ++i) {
                    int64_t sum = dst_column.get(i);
                    int64_t item_count = cnt_column.get(i);
                    double res = double(sum) / double(item_count);
                    mean_column.set(i, res);
                }

                // Remove temp columns
                result.remove_column(1); // sums
                result.remove_column(1); // counts
                break;
            }
            case aggr_min:
                for (size_t r = 0; r < count; ++r) {
                    size_t i = static_cast<size_t>(viewrefs->get(r));

                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);
                    int64_t value = src_column.get(i);
                    if (state.added_row) {
                        // Set the real value, to overwrite the default value
                        dst_column.set(ndx, value);
                        state.added_row = false;
                    }
                    else {
                        int64_t current = dst_column.get(ndx);
                        if (value < current)
                            dst_column.set(ndx, value);
                    }
                }
                break;
            case aggr_max:
                for (size_t r = 0; r < count; ++r) {
                    size_t i = static_cast<size_t>(static_cast<size_t>(viewrefs->get(r)));

                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);
                    int64_t value = src_column.get(i);
                    if (state.added_row) {
                        // Set the real value, to overwrite the default value
                        dst_column.set(ndx, value);
                        state.added_row = false;
                    }
                    else {
                        int64_t current = dst_column.get(ndx);
                        if (value > current)
                            dst_column.set(ndx, value);
                    }
                }
                break;
        }
    }
    else {
        const size_t count = size();

        switch (op) {
            case aggr_count:
                for (size_t i = 0; i < count; ++i) {
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // Count
                    dst_column.adjust(ndx, 1);
                }
                break;
            case aggr_sum:
                for (size_t i = 0; i < count; ++i) {
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // Sum
                    int64_t value = src_column.get(i);
                    dst_column.adjust(ndx, value);
                }
                break;
            case aggr_avg: {
                // Add temporary column for counts
                result.add_column(type_Int, "count");
                IntegerColumn& cnt_column = result.get_column(2);

                for (size_t i = 0; i < count; ++i) {
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // SUM
                    int64_t value = src_column.get(i);
                    dst_column.adjust(ndx, value);

                    // Increment count
                    cnt_column.adjust(ndx, 1);
                }

                // Calculate averages
                result.add_column(type_Double, "average");
                DoubleColumn& mean_column = result.get_column_double(3);
                const size_t res_count = result.size();
                for (size_t i = 0; i < res_count; ++i) {
                    int64_t sum = dst_column.get(i);
                    int64_t item_count = cnt_column.get(i);
                    double res = double(sum) / double(item_count);
                    mean_column.set(i, res);
                }

                // Remove temp columns
                result.remove_column(1); // sums
                result.remove_column(1); // counts
                break;
            }
            case aggr_min:
                for (size_t i = 0; i < count; ++i) {
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);
                    int64_t value = src_column.get(i);
                    if (state.added_row) {
                        // Set the real value, to overwrite the default value
                        dst_column.set(ndx, value);
                        state.added_row = false;
                    }
                    else {
                        int64_t current = dst_column.get(ndx);
                        if (value < current)
                            dst_column.set(ndx, value);
                    }
                }
                break;
            case aggr_max:
                for (size_t i = 0; i < count; ++i) {
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);
                    int64_t value = src_column.get(i);
                    if (state.added_row) {
                        // Set the real value, to overwrite the default value
                        dst_column.set(ndx, value);
                        state.added_row = false;
                    }
                    else {
                        int64_t current = dst_column.get(ndx);
                        if (value > current)
                            dst_column.set(ndx, value);
                    }
                }
                break;
        }
    }
}


TableView Table::get_range_view(size_t begin, size_t end)
{
    REALM_ASSERT(!m_columns.is_attached() || end <= size());

    return where().find_all(begin, end);
}

ConstTableView Table::get_range_view(size_t begin, size_t end) const
{
    return const_cast<Table*>(this)->get_range_view(begin, end);
}

TableView Table::get_backlink_view(size_t row_ndx, Table* src_table, size_t src_col_ndx)
{
    REALM_ASSERT(&src_table->get_column_link_base(src_col_ndx).get_target_table() == this);
    TableView tv(src_table, src_col_ndx, get(row_ndx));
    tv.do_sync();
    return tv;
}

size_t Table::lower_bound_int(size_t col_ndx, int64_t value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).lower_bound(value);
}

size_t Table::upper_bound_int(size_t col_ndx, int64_t value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).upper_bound(value);
}

size_t Table::lower_bound_bool(size_t col_ndx, bool value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).lower_bound(value);
}

size_t Table::upper_bound_bool(size_t col_ndx, bool value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).upper_bound(value);
}

size_t Table::lower_bound_float(size_t col_ndx, float value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_float(col_ndx).lower_bound(value);
}

size_t Table::upper_bound_float(size_t col_ndx, float value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_float(col_ndx).upper_bound(value);
}

size_t Table::lower_bound_double(size_t col_ndx, double value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_double(col_ndx).lower_bound(value);
}

size_t Table::upper_bound_double(size_t col_ndx, double value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_double(col_ndx).upper_bound(value);
}

size_t Table::lower_bound_string(size_t col_ndx, StringData value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    if (!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const StringColumn& col = get_column_string(col_ndx);
        return col.lower_bound_string(value);
    }
    REALM_ASSERT_3(type, ==, col_type_StringEnum);
    const StringEnumColumn& col = get_column_string_enum(col_ndx);
    return col.lower_bound_string(value);
}

size_t Table::upper_bound_string(size_t col_ndx, StringData value) const noexcept
{
    REALM_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    if (!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const StringColumn& col = get_column_string(col_ndx);
        return col.upper_bound_string(value);
    }
    REALM_ASSERT_3(type, ==, col_type_StringEnum);
    const StringEnumColumn& col = get_column_string_enum(col_ndx);
    return col.upper_bound_string(value);
}


void Table::optimize(bool enforce)
{
    // At the present time there is only one kind of optimization that
    // we can do, and that is to replace a string column with a string
    // enumeration column. Since this involves changing the spec of
    // the table, it is not something we can do for a subtable with
    // shared spec.
    if (has_shared_type())
        return;

    Allocator& alloc = m_columns.get_alloc();

    size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        ColumnType type_i = get_real_column_type(i);
        if (type_i == col_type_String) {
            StringColumn* column_i = &get_column_string(i);

            ref_type ref, keys_ref;
            bool res = column_i->auto_enumerate(keys_ref, ref, enforce);
            if (!res)
                continue;

            Spec::ColumnInfo info = m_spec->get_column_info(i);
            ArrayParent* keys_parent;
            size_t keys_ndx_in_parent;
            m_spec->upgrade_string_to_enum(i, keys_ref, keys_parent, keys_ndx_in_parent);

            // Upgrading the column may have moved the
            // refs to keylists in other columns so we
            // have to update their parent info
            for (size_t c = i + 1; c < m_cols.size(); ++c) {
                ColumnType type_c = get_real_column_type(c);
                if (type_c == col_type_StringEnum) {
                    StringEnumColumn& column_c = get_column_string_enum(c);
                    column_c.adjust_keys_ndx_in_parent(1);
                }
            }

            // Indexes are also in m_columns, so we need adjusted pos
            size_t ndx_in_parent = m_spec->get_column_ndx_in_parent(i);

            // Replace column
            StringEnumColumn* e = new StringEnumColumn(alloc, ref, keys_ref, is_nullable(i), i); // Throws
            e->set_parent(&m_columns, ndx_in_parent);
            e->get_keys().set_parent(keys_parent, keys_ndx_in_parent);
            m_cols[i] = e;
            m_columns.set(ndx_in_parent, ref); // Throws

            // Inherit any existing index
            if (info.m_has_search_index) {
                e->install_search_index(column_i->release_search_index());
            }

            // Clean up the old column
            column_i->destroy();
            delete column_i;
        }
    }

    if (Replication* repl = get_repl())
        repl->optimize_table(this); // Throws
}


class Table::SliceWriter : public Group::TableWriter {
public:
    SliceWriter(const Table& table, StringData table_name, size_t offset, size_t size) noexcept
        : m_table(table)
        , m_table_name(table_name)
        , m_offset(offset)
        , m_size(size)
    {
    }

    ref_type write_names(_impl::OutputStream& out) override
    {
        Allocator& alloc = Allocator::get_default();
        ArrayStringShort table_names(alloc);
        table_names.create(); // Throws
        _impl::DestroyGuard<ArrayStringShort> dg(&table_names);
        table_names.add(m_table_name);                                 // Throws
        bool deep = true;                                              // Deep
        bool only_if_modified = false;                                 // Always
        ref_type ref = table_names.write(out, deep, only_if_modified); // Throws
        return ref;
    }

    ref_type write_tables(_impl::OutputStream& out) override
    {
        Allocator& alloc = Allocator::get_default();

        // Make a copy of the spec of this table, modify it, and then
        // write it to the output stream
        ref_type spec_ref;
        {
            REALM_ASSERT(m_table.m_spec.is_managed());
            MemRef mem = m_table.m_spec->m_top.clone_deep(alloc); // Throws
            Spec spec(alloc);
            spec.init(mem); // Throws
            _impl::DestroyGuard<Spec> dg(&spec);
            size_t n = spec.get_column_count();
            for (size_t i = 0; i != n; ++i) {
                int attr = spec.get_column_attr(i);
                // Remove any index specifying attributes
                attr &= ~(col_attr_Indexed | col_attr_Unique);
                spec.set_column_attr(i, ColumnAttr(attr)); // Throws
            }
            bool deep = true;                                         // Deep
            bool only_if_modified = false;                            // Always
            spec_ref = spec.m_top.write(out, deep, only_if_modified); // Throws
        }

        // Make a copy of the selected slice of each column
        ref_type columns_ref;
        {
            Array column_refs(alloc);
            column_refs.create(Array::type_HasRefs); // Throws
            _impl::ShallowArrayDestroyGuard dg(&column_refs);
            size_t table_size = m_table.size();
            for (auto& column : m_table.m_cols) {
                ref_type ref = column->write(m_offset, m_size, table_size, out); // Throws
                int_fast64_t ref_2(from_ref(ref));
                column_refs.add(ref_2); // Throws
            }
            bool deep = false;                                            // Shallow
            bool only_if_modified = false;                                // Always
            columns_ref = column_refs.write(out, deep, only_if_modified); // Throws
        }

        // Create a new top array for the table
        ref_type table_top_ref;
        {
            Array table_top(alloc);
            table_top.create(Array::type_HasRefs); // Throws
            _impl::ShallowArrayDestroyGuard dg(&table_top);
            int_fast64_t spec_ref_2(from_ref(spec_ref));
            table_top.add(spec_ref_2); // Throws
            int_fast64_t columns_ref_2(from_ref(columns_ref));
            table_top.add(columns_ref_2);                                 // Throws
            bool deep = false;                                            // Shallow
            bool only_if_modified = false;                                // Always
            table_top_ref = table_top.write(out, deep, only_if_modified); // Throws
        }

        // Create the array of tables of size one
        Array tables(alloc);
        tables.create(Array::type_HasRefs); // Throws
        _impl::ShallowArrayDestroyGuard dg(&tables);
        int_fast64_t table_top_ref_2(from_ref(table_top_ref));
        tables.add(table_top_ref_2);                              // Throws
        bool deep = false;                                        // Shallow
        bool only_if_modified = false;                            // Always
        ref_type ref = tables.write(out, deep, only_if_modified); // Throws
        return ref;
    }

private:
    const Table& m_table;
    const StringData m_table_name;
    const size_t m_offset, m_size;
};


void Table::write(std::ostream& out, size_t offset, size_t slice_size, StringData override_table_name) const
{
    size_t table_size = this->size();
    if (offset > table_size)
        throw std::out_of_range("Offset is out of range");
    size_t remaining_size = table_size - offset;
    size_t size_2 = slice_size;
    if (size_2 > remaining_size)
        size_2 = remaining_size;
    StringData table_name = override_table_name;
    if (!table_name)
        table_name = get_name();
    SliceWriter writer(*this, table_name, offset, size_2);
    bool no_top_array = false;
    bool pad_for_encryption = false;
    uint_fast64_t version_number = 0;
    int file_format_version = 0;
    Group::write(out, file_format_version, writer, no_top_array, pad_for_encryption, version_number); // Throws
}


void Table::update_from_parent(size_t old_baseline) noexcept
{
    REALM_ASSERT(is_attached());
    bool spec_might_have_changed = false;

    // There is no top for sub-tables sharing spec
    if (m_top.is_attached()) {
        if (!m_top.update_from_parent(old_baseline))
            return;

        // subspecs may be deleted here ...
        if (m_spec->update_from_parent(old_baseline)) {
            // ... so get rid of cached entries here
            if (DescriptorRef desc = m_descriptor.lock()) {
                using df = _impl::DescriptorFriend;
                df::detach_subdesc_accessors(*desc);
            }
            // and remember to update mappings in subtable columns
            spec_might_have_changed = true;
        }
        if (m_top.size() > 2) {
            m_clusters.update_from_parent(old_baseline);
        }
    }
    else {
        refresh_spec_accessor();
    }

    if (!m_columns.is_attached())
        return; // Degenerate subtable

    if (m_columns.update_from_parent(old_baseline)) {
        // Update column accessors
        for (auto& col : m_cols) {
            if (col != nullptr) {
                col->update_from_parent(old_baseline);
            }
        }
    }
    else if (spec_might_have_changed) {
        size_t sz = m_cols.size();
        for (size_t i = 0; i < sz; i++) {
            // Only relevant for subtable columns
            if (auto col = dynamic_cast<SubtableColumn*>(m_cols[i])) {
                col->refresh_subtable_map();
            }
        }
    }
}


// to JSON: ------------------------------------------
void Table::to_json_row(size_t row_ndx, std::ostream& out, size_t link_depth,
                        std::map<std::string, std::string>* renames) const
{
    std::map<std::string, std::string> renames2;
    renames = renames ? renames : &renames2;

    std::vector<ref_type> followed;
    to_json_row(row_ndx, out, link_depth, *renames, followed);
}


namespace {

inline void out_olddatetime(std::ostream& out, OldDateTime value)
{
    time_t rawtime = time_t(value.get_olddatetime());
    struct tm* t = gmtime(&rawtime);
    if (t) {
        // We need a buffer for formatting dates (and binary to hex). Max
        // size is 20 bytes (incl zero byte) "YYYY-MM-DD HH:MM:SS"\0
        char buffer[30];
        size_t res = strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", t);
        if (res)
            out << buffer;
    }
}

inline void out_timestamp(std::ostream& out, Timestamp value)
{
    // FIXME: Do we want to output the full precision to json?
    time_t rawtime = time_t(value.get_seconds());
    struct tm* t = gmtime(&rawtime);
    if (t) {
        // We need a buffer for formatting dates (and binary to hex). Max
        // size is 20 bytes (incl zero byte) "YYYY-MM-DD HH:MM:SS"\0
        char buffer[30];
        size_t res = strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", t);
        if (res)
            out << buffer;
    }
}

inline void out_binary(std::ostream& out, const BinaryData bin)
{
    const char* p = bin.data();

    for (size_t i = 0; i < bin.size(); ++i) {
        out << std::setw(2) << std::setfill('0') << std::hex << static_cast<unsigned int>(p[i]) << std::dec;
    }
}

template <class T>
void out_floats(std::ostream& out, T value)
{
    std::streamsize old = out.precision();
    out.precision(std::numeric_limits<T>::digits10 + 1);
    out << std::scientific << value;
    out.precision(old);
}

} // anonymous namespace

void Table::to_json(std::ostream& out, size_t link_depth, std::map<std::string, std::string>* renames) const
{
    // Represent table as list of objects
    out << "[";

    size_t row_count = size();
    for (size_t r = 0; r < row_count; ++r) {
        if (r > 0)
            out << ",";
        to_json_row(r, out, link_depth, renames);
    }

    out << "]";
}

void Table::to_json_row(size_t row_ndx, std::ostream& out, size_t link_depth,
                        std::map<std::string, std::string>& renames, std::vector<ref_type>& followed) const
{
    out << "{";
    size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        if (i > 0)
            out << ",";

        StringData name = get_column_name(i);
        if (renames[name] != "")
            name = renames[name];

        out << "\"" << name << "\":";

        DataType type = get_column_type(i);
        switch (type) {
            case type_Int:
                out << get_int(i, row_ndx);
                break;
            case type_Bool:
                out << (get_bool(i, row_ndx) ? "true" : "false");
                break;
            case type_Float:
                out_floats<float>(out, get_float(i, row_ndx));
                break;
            case type_Double:
                out_floats<double>(out, get_double(i, row_ndx));
                break;
            case type_String:
                out << "\"" << get_string(i, row_ndx) << "\"";
                break;
            case type_OldDateTime:
                out << "\"";
                out_olddatetime(out, get_olddatetime(i, row_ndx));
                out << "\"";
                break;
            case type_Binary:
                out << "\"";
                out_binary(out, get_binary(i, row_ndx));
                out << "\"";
                break;
            case type_Timestamp:
                out << "\"";
                out_timestamp(out, get_timestamp(i, row_ndx));
                out << "\"";
                break;
            case type_Table:
                get_subtable(i, row_ndx)->to_json(out);
                break;
            case type_Mixed: {
                DataType mtype = get_mixed_type(i, row_ndx);
                if (mtype == type_Table) {
                    get_subtable(i, row_ndx)->to_json(out);
                }
                else {
                    Mixed m = get_mixed(i, row_ndx);
                    switch (mtype) {
                        case type_Int:
                            out << m.get_int();
                            break;
                        case type_Bool:
                            out << (m.get_bool() ? "true" : "false");
                            break;
                        case type_Float:
                            out_floats<float>(out, m.get_float());
                            break;
                        case type_Double:
                            out_floats<double>(out, m.get_double());
                            break;
                        case type_String:
                            out << "\"" << m.get_string() << "\"";
                            break;
                        case type_OldDateTime:
                            out << "\"";
                            out_olddatetime(out, m.get_olddatetime());
                            out << "\"";
                            break;
                        case type_Binary:
                            out << "\"";
                            out_binary(out, m.get_binary());
                            out << "\"";
                            break;
                        case type_Timestamp:
                            out << "\"";
                            out_timestamp(out, m.get_timestamp());
                            out << "\"";
                            break;
                        case type_Table:
                        case type_Mixed:
                        case type_Link:
                        case type_LinkList:
                            REALM_ASSERT(false);
                            break;
                    }
                }
                break;
            }
            case type_Link: {
                LinkColumnBase& clb = const_cast<Table*>(this)->get_column_link_base(i);
                LinkColumn& cl = static_cast<LinkColumn&>(clb);
                Table& table = cl.get_target_table();

                if (!cl.is_null_link(row_ndx)) {
                    ref_type lnk = clb.get_ref();
                    if ((link_depth == 0) || (link_depth == not_found &&
                                              std::find(followed.begin(), followed.end(), lnk) != followed.end())) {
                        out << "\"" << cl.get_link(row_ndx) << "\"";
                        break;
                    }
                    else {
                        out << "[";
                        followed.push_back(clb.get_ref());
                        size_t new_depth = link_depth == not_found ? not_found : link_depth - 1;
                        table.to_json_row(cl.get_link(row_ndx), out, new_depth, renames, followed);
                        out << "]";
                    }
                }
                else {
                    out << "[]";
                }

                break;
            }
            case type_LinkList: {
                LinkColumnBase& clb = const_cast<Table*>(this)->get_column_link_base(i);
                LinkListColumn& cll = static_cast<LinkListColumn&>(clb);
                Table& table = cll.get_target_table();
                LinkViewRef lv = cll.get(row_ndx);

                ref_type lnk = clb.get_ref();
                if ((link_depth == 0) ||
                    (link_depth == not_found && std::find(followed.begin(), followed.end(), lnk) != followed.end())) {
                    out << "{\"table\": \"" << cll.get_target_table().get_name() << "\", \"rows\": [";
                    cll.to_json_row(row_ndx, out);
                    out << "]}";
                    break;
                }
                else {
                    out << "[";
                    for (size_t link_ndx = 0; link_ndx < lv->size(); link_ndx++) {
                        if (link_ndx > 0)
                            out << ", ";
                        followed.push_back(lnk);
                        size_t new_depth = link_depth == not_found ? not_found : link_depth - 1;
                        table.to_json_row(lv->get(link_ndx).get_index(), out, new_depth, renames, followed);
                    }
                    out << "]";
                }

                break;
            }
        } // switch ends
    }
    out << "}";
}


// to_string --------------------------------------------------


namespace {

size_t chars_in_int(int64_t v)
{
    size_t count = 0;
    while (v /= 10)
        ++count;
    return count + 1;
}

} // anonymous namespace


void Table::to_string(std::ostream& out, size_t limit) const
{
    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    to_string_header(out, widths);

    // Set limit=-1 to print all rows, otherwise only print to limit
    size_t row_count = size();
    size_t out_count = (limit == size_t(-1)) ? row_count : (row_count < limit) ? row_count : limit;

    // Print rows
    for (size_t i = 0; i < out_count; ++i) {
        to_string_row(i, out, widths);
    }

    if (out_count < row_count) {
        size_t rest = row_count - out_count;
        out << "... and " << rest << " more rows (total " << row_count << ")";
    }
}

void Table::row_to_string(size_t row_ndx, std::ostream& out) const
{
    REALM_ASSERT_3(row_ndx, <, size());

    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    to_string_header(out, widths);

    // Print row contents
    to_string_row(row_ndx, out, widths);
}

void Table::to_string_header(std::ostream& out, std::vector<size_t>& widths) const
{
    size_t column_count = get_column_count();
    size_t row_count = size();
    size_t row_ndx_width = chars_in_int(row_count);
    widths.push_back(row_ndx_width);

    // Empty space over row numbers
    for (size_t i = 0; i < row_ndx_width + 1; ++i) {
        out << " ";
    }

    // Write header
    for (size_t col = 0; col < column_count; ++col) {
        StringData name = get_column_name(col);
        DataType type = get_column_type(col);
        size_t width = 0;
        switch (type) {
            case type_Bool:
                width = 5;
                break;
            case type_OldDateTime:
            case type_Timestamp:
                // FIXME: Probably not correct if we output the full precision
                width = 19;
                break;
            case type_Int:
                width = chars_in_int(maximum_int(col));
                break;
            case type_Float:
                // max chars for scientific notation:
                width = 14;
                break;
            case type_Double:
                width = 14;
                break;
            case type_Table:
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = chars_in_int(get_subtable_size(col, row));
                    width = std::max(width, len + 2);
                }
                width += 2; // space for "[]"
                break;
            case type_Binary:
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = chars_in_int(get_binary(col, row).size()) + 2;
                    width = std::max(width, len);
                }
                width += 6; // space for " bytes"
                break;
            case type_String: {
                // Find max length of the strings
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = get_string(col, row).size();
                    width = std::max(width, len);
                }
                if (width > 20)
                    width = 23; // cut strings longer than 20 chars
                break;
            }
            case type_Mixed:
                // Find max length of the mixed values
                width = 0;
                for (size_t row = 0; row < row_count; ++row) {
                    DataType mtype = get_mixed_type(col, row);
                    if (mtype == type_Table) {
                        size_t len = chars_in_int(get_subtable_size(col, row)) + 2;
                        width = std::max(width, len);
                        continue;
                    }
                    Mixed m = get_mixed(col, row);
                    switch (mtype) {
                        case type_Bool:
                            width = std::max(width, size_t(5));
                            break;
                        case type_OldDateTime:
                        case type_Timestamp:
                            // FIXME: Probably not correct if we output the full precision
                            width = std::max(width, size_t(19));
                            break;
                        case type_Int:
                            width = std::max(width, chars_in_int(m.get_int()));
                            break;
                        case type_Float:
                            width = std::max(width, size_t(14));
                            break;
                        case type_Double:
                            width = std::max(width, size_t(14));
                            break;
                        case type_Binary:
                            width = std::max(width, chars_in_int(m.get_binary().size()) + 6);
                            break;
                        case type_String: {
                            size_t len = m.get_string().size();
                            if (len > 20)
                                len = 23;
                            width = std::max(width, len);
                            break;
                        }
                        case type_Table:
                        case type_Mixed:
                        case type_Link:
                        case type_LinkList:
                            REALM_ASSERT(false);
                            break;
                    }
                }
                break;
            case type_Link:
            case type_LinkList:
                width = 5;
                break;
        }
        // Set width to max of column name and the longest value
        size_t name_len = name.size();
        if (name_len > width)
            width = name_len;

        widths.push_back(width);
        out << "  "; // spacing

        out.width(width);
        out << std::string(name);
    }
    out << "\n";
}


namespace {

inline void out_string(std::ostream& out, const std::string text, const size_t max_len)
{
    out.setf(std::ostream::left, std::ostream::adjustfield);
    if (text.size() > max_len)
        out << text.substr(0, max_len) + "...";
    else
        out << text;
    out.unsetf(std::ostream::adjustfield);
}

inline void out_table(std::ostream& out, const size_t len)
{
    std::streamsize width = out.width() - chars_in_int(len) - 1;
    out.width(width);
    out << "[" << len << "]";
}

} // anonymous namespace


void Table::to_string_row(size_t row_ndx, std::ostream& out, const std::vector<size_t>& widths) const
{
    size_t column_count = get_column_count();
    size_t row_ndx_width = widths[0];

    out << std::scientific; // for float/double
    out.width(row_ndx_width);
    out << row_ndx << ":";

    for (size_t col = 0; col < column_count; ++col) {
        out << "  "; // spacing
        out.width(widths[col + 1]);

        if (is_nullable(col) && is_null(col, row_ndx)) {
            out << "(null)";
            continue;
        }

        DataType type = get_column_type(col);
        switch (type) {
            case type_Bool:
                out << (get_bool(col, row_ndx) ? "true" : "false");
                break;
            case type_Int:
                out << get_int(col, row_ndx);
                break;
            case type_Float:
                out << get_float(col, row_ndx);
                break;
            case type_Double:
                out << get_double(col, row_ndx);
                break;
            case type_String:
                out_string(out, get_string(col, row_ndx), 20);
                break;
            case type_OldDateTime:
                out_olddatetime(out, get_olddatetime(col, row_ndx));
                break;
            case type_Timestamp:
                out_timestamp(out, get_timestamp(col, row_ndx));
                break;
            case type_Table:
                out_table(out, get_subtable_size(col, row_ndx));
                break;
            case type_Binary:
                out.width(widths[col + 1] - 6); // adjust for " bytes" text
                out << get_binary(col, row_ndx).size() << " bytes";
                break;
            case type_Mixed: {
                DataType mtype = get_mixed_type(col, row_ndx);
                if (mtype == type_Table) {
                    out_table(out, get_subtable_size(col, row_ndx));
                }
                else {
                    Mixed m = get_mixed(col, row_ndx);
                    switch (mtype) {
                        case type_Bool:
                            out << (m.get_bool() ? "true" : "false");
                            break;
                        case type_Int:
                            out << m.get_int();
                            break;
                        case type_Float:
                            out << m.get_float();
                            break;
                        case type_Double:
                            out << m.get_double();
                            break;
                        case type_String:
                            out_string(out, m.get_string(), 20);
                            break;
                        case type_OldDateTime:
                            out_olddatetime(out, m.get_olddatetime());
                            break;
                        case type_Timestamp:
                            out_timestamp(out, m.get_timestamp());
                            break;
                        case type_Binary:
                            out.width(widths[col + 1] - 6); // adjust for " bytes" text
                            out << m.get_binary().size() << " bytes";
                            break;
                        case type_Table:
                        case type_Mixed:
                        case type_Link:
                        case type_LinkList:
                            REALM_ASSERT(false);
                            break;
                    }
                }
                break;
            }
            case type_Link:
                // FIXME: print linked row
                out << get_link(col, row_ndx);
                break;
            case type_LinkList:
                // FIXME: print number of links in list
                break;
        }
    }

    out << "\n";
}


size_t Table::compute_aggregated_byte_size() const noexcept
{
    if (!is_attached())
        return 0;
    const Array& real_top = (m_top.is_attached() ? m_top : m_columns);
    MemStats stats_2;
    real_top.stats(stats_2);
    return stats_2.allocated;
}


bool Table::compare_rows(const Table& t) const
{
    // Table accessors attached to degenerate subtables have no column
    // accesssors, so the general comparison scheme is impossible in that case.
    if (m_size == 0)
        return t.m_size == 0;

    // FIXME: The current column comparison implementation is very
    // inefficient, we should use sequential tree accessors when they
    // become available.

    size_t n = get_column_count();
    REALM_ASSERT_3(t.get_column_count(), ==, n);
    for (size_t i = 0; i != n; ++i) {
        ColumnType type = get_real_column_type(i);
        bool nullable = is_nullable(i);
        REALM_ASSERT((type == col_type_String || type == col_type_StringEnum || type == t.get_real_column_type(i)) &&
                     nullable == t.is_nullable(i));

        switch (type) {
            case col_type_Int:
            case col_type_Bool:
            case col_type_OldDateTime: {
                if (nullable) {
                    const IntNullColumn& c1 = get_column_int_null(i);
                    const IntNullColumn& c2 = t.get_column_int_null(i);
                    if (!c1.compare(c2)) {
                        return false;
                    }
                }
                else {
                    const IntegerColumn& c1 = get_column(i);
                    const IntegerColumn& c2 = t.get_column(i);
                    if (!c1.compare(c2))
                        return false;
                }
                continue;
            }
            case col_type_Timestamp: {
                const TimestampColumn& c1 = get_column_timestamp(i);
                const TimestampColumn& c2 = t.get_column_timestamp(i);
                if (!c1.compare(c2))
                    return false;
                continue;
            }
            case col_type_Float: {
                const FloatColumn& c1 = get_column_float(i);
                const FloatColumn& c2 = t.get_column_float(i);
                if (!c1.compare(c2))
                    return false;
                continue;
            }
            case col_type_Double: {
                const DoubleColumn& c1 = get_column_double(i);
                const DoubleColumn& c2 = t.get_column_double(i);
                if (!c1.compare(c2))
                    return false;
                continue;
            }
            case col_type_String: {
                const StringColumn& c1 = get_column_string(i);
                ColumnType type2 = t.get_real_column_type(i);
                if (type2 == col_type_String) {
                    const StringColumn& c2 = t.get_column_string(i);
                    if (!c1.compare_string(c2))
                        return false;
                }
                else {
                    REALM_ASSERT_3(type2, ==, col_type_StringEnum);
                    const StringEnumColumn& c2 = t.get_column_string_enum(i);
                    if (!c2.compare_string(c1))
                        return false;
                }
                continue;
            }
            case col_type_StringEnum: {
                const StringEnumColumn& c1 = get_column_string_enum(i);
                ColumnType type2 = t.get_real_column_type(i);
                if (type2 == col_type_StringEnum) {
                    const StringEnumColumn& c2 = t.get_column_string_enum(i);
                    if (!c1.compare_string(c2))
                        return false;
                }
                else {
                    REALM_ASSERT_3(type2, ==, col_type_String);
                    const StringColumn& c2 = t.get_column_string(i);
                    if (!c1.compare_string(c2))
                        return false;
                }
                continue;
            }
            case col_type_Binary: {
                const BinaryColumn& c1 = get_column_binary(i);
                const BinaryColumn& c2 = t.get_column_binary(i);
                if (!c1.compare_binary(c2))
                    return false;
                continue;
            }
            case col_type_Table: {
                const SubtableColumn& c1 = get_column_table(i);
                const SubtableColumn& c2 = t.get_column_table(i);
                if (!c1.compare_table(c2)) // Throws
                    return false;
                continue;
            }
            case col_type_Mixed: {
                const MixedColumn& c1 = get_column_mixed(i);
                const MixedColumn& c2 = t.get_column_mixed(i);
                if (!c1.compare_mixed(c2))
                    return false;
                continue;
            }
            case col_type_Link: {
                const LinkColumn& c1 = get_column_link(i);
                const LinkColumn& c2 = t.get_column_link(i);
                if (!c1.compare(c2))
                    return false;
                continue;
            }
            case col_type_LinkList: {
                const LinkListColumn& c1 = get_column_link_list(i);
                const LinkListColumn& c2 = t.get_column_link_list(i);
                if (!c1.compare_link_list(c2))
                    return false;
                continue;
            }
            case col_type_BackLink:
            case col_type_Reserved4:
                break;
        }
        REALM_ASSERT(false);
    }
    return true;
}


void Table::check_lists_are_empty(size_t row_ndx) const
{
    // FIXME: Due to a limitation in Sync, it is not legal to change the primary
    // key of a row that contains lists (including linklists) after those lists
    // have been populated. This limitation may be lifted in the future, but for
    // now it is necessary to ensure that all lists are empty before setting a
    // primary key (by way of set_int_unique() or set_string_unique() or set_null_unique()).

    for (size_t i = 0; i < get_column_count(); ++i) {
        if (get_column_type(i) == type_LinkList) {
            const LinkListColumn& col = get_column_link_list(i);
            if (col.get_link_count(row_ndx) != 0) {
                // Violation of the rule that an object receiving a primary key
                // may not contain any non-empty lists.
                throw LogicError{LogicError::illegal_combination};
            }
        }
    }
}


StringData Table::Parent::get_child_name(size_t) const noexcept
{
    return StringData("");
}


Group* Table::Parent::get_parent_group() noexcept
{
    return nullptr;
}


Table* Table::Parent::get_parent_table(size_t*) noexcept
{
    return nullptr;
}

Spec* Table::Parent::get_subtable_spec() noexcept
{
    return nullptr;
}

void Table::adj_acc_insert_rows(size_t row_ndx, size_t num_rows) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    adj_row_acc_insert_rows(row_ndx, num_rows);

    // Adjust column and subtable accessors after insertion of new rows
    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->adj_acc_insert_rows(row_ndx, num_rows);
        }
    }
}


void Table::adj_acc_erase_row(size_t row_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    adj_row_acc_erase_row(row_ndx);

    // Adjust subtable accessors after removal of a row
    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->adj_acc_erase_row(row_ndx);
        }
    }
}

void Table::adj_acc_swap_rows(size_t row_ndx_1, size_t row_ndx_2) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    adj_row_acc_swap_rows(row_ndx_1, row_ndx_2);

    // Adjust subtable accessors after row swap
    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->adj_acc_swap_rows(row_ndx_1, row_ndx_2);
        }
    }
}


void Table::adj_acc_move_row(size_t from_ndx, size_t to_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    adj_row_acc_move_row(from_ndx, to_ndx);

    // Adjust subtable accessors after row move
    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->adj_acc_move_row(from_ndx, to_ndx);
        }
    }
}


void Table::adj_acc_merge_rows(size_t old_row_ndx, size_t new_row_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    adj_row_acc_merge_rows(old_row_ndx, new_row_ndx);

    // Adjust LinkViews for new rows
    for (auto& col : m_cols) {
        if (col) {
            col->adj_acc_merge_rows(old_row_ndx, new_row_ndx);
        }
    }
}


void Table::adj_acc_move_over(size_t from_row_ndx, size_t to_row_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    adj_row_acc_move_over(from_row_ndx, to_row_ndx);

    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->adj_acc_move_over(from_row_ndx, to_row_ndx);
        }
    }
}


void Table::adj_acc_clear_root_table() noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistencyLevels.

    discard_row_accessors();

    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->adj_acc_clear_root_table();
        }
    }
}


void Table::adj_acc_clear_nonroot_table() noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistencyLevels.

    discard_child_accessors();
    destroy_column_accessors();
    m_columns.detach();
}


void Table::adj_row_acc_insert_rows(size_t row_ndx, size_t num_rows) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Adjust row accessors after insertion of new rows
    LockGuard lock(m_accessor_mutex);
    for (RowBase* row = m_row_accessors; row; row = row->m_next) {
        if (row->m_row_ndx >= row_ndx)
            row->m_row_ndx += num_rows;
    }
}


void Table::adj_row_acc_erase_row(size_t row_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Adjust row accessors after removal of a row
    LockGuard lock(m_accessor_mutex);
    RowBase* row = m_row_accessors;
    while (row) {
        RowBase* next = row->m_next;
        if (row->m_row_ndx == row_ndx) {
            row->m_table.reset();
            do_unregister_row_accessor(row);
        }
        else if (row->m_row_ndx > row_ndx) {
            --row->m_row_ndx;
        }
        row = next;
    }
}

void Table::adj_row_acc_swap_rows(size_t row_ndx_1, size_t row_ndx_2) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Adjust row accessors after swap
    LockGuard lock(m_accessor_mutex);
    RowBase* row = m_row_accessors;
    while (row) {
        if (row->m_row_ndx == row_ndx_1) {
            row->m_row_ndx = row_ndx_2;
        }
        else if (row->m_row_ndx == row_ndx_2) {
            row->m_row_ndx = row_ndx_1;
        }
        row = row->m_next;
    }
}


void Table::adj_row_acc_move_row(size_t from_ndx, size_t to_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Adjust row accessors after move
    LockGuard lock(m_accessor_mutex);
    RowBase* row = m_row_accessors;
    while (row) {
        size_t ndx = row->m_row_ndx;
        if (ndx == from_ndx)
            row->m_row_ndx = to_ndx;
        else if (ndx > from_ndx && ndx <= to_ndx)
            row->m_row_ndx = ndx - 1;
        else if (ndx >= to_ndx && ndx < from_ndx)
            row->m_row_ndx = ndx + 1;
        row = row->m_next;
    }
}


void Table::adj_row_acc_merge_rows(size_t old_row_ndx, size_t new_row_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    LockGuard lock(m_accessor_mutex);
    RowBase* row = m_row_accessors;
    while (row) {
        if (row->m_row_ndx == old_row_ndx)
            row->m_row_ndx = new_row_ndx;
        row = row->m_next;
    }
}


void Table::adj_row_acc_move_over(size_t from_row_ndx, size_t to_row_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.
    LockGuard lock(m_accessor_mutex);
    RowBase* row = m_row_accessors;
    while (row) {
        RowBase* next = row->m_next;
        if (row->m_row_ndx == to_row_ndx) {
            row->m_table.reset();
            do_unregister_row_accessor(row);
        }
        else if (row->m_row_ndx == from_row_ndx) {
            row->m_row_ndx = to_row_ndx;
        }
        row = next;
    }
}


void Table::adj_insert_column(size_t col_ndx)
{
    // Beyond the constraints on the specified column index, this function must
    // assume no more than minimal consistency of the accessor hierarchy. This
    // means in particular that it cannot access the underlying node
    // structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());
    bool not_degenerate = m_columns.is_attached();
    if (not_degenerate) {
        REALM_ASSERT_3(col_ndx, <=, m_cols.size());
        m_cols.insert(m_cols.begin() + col_ndx, nullptr); // Throws
    }
}


void Table::adj_erase_column(size_t col_ndx) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());
    bool not_degenerate = m_columns.is_attached();
    if (not_degenerate) {
        REALM_ASSERT_3(col_ndx, <, m_cols.size());
        if (ColumnBase* col = m_cols[col_ndx])
            delete col;
        m_cols.erase(m_cols.begin() + col_ndx);
    }
}

void Table::adj_move_column(size_t from, size_t to) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());
    bool not_degenerate = m_columns.is_attached();
    if (not_degenerate) {
        REALM_ASSERT_3(from, <, m_cols.size());
        REALM_ASSERT_3(to, <, m_cols.size());
        using iter = decltype(m_cols.begin());
        iter first, new_first, last;
        if (from < to) {
            first = m_cols.begin() + from;
            new_first = first + 1;
            last = m_cols.begin() + to + 1;
        }
        else {
            first = m_cols.begin() + to;
            new_first = m_cols.begin() + from;
            last = new_first + 1;
        }
        std::rotate(first, new_first, last);
    }
}


void Table::recursive_mark() noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    mark();

    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->mark(ColumnBase::mark_Recursive);
        }
    }
}


void Table::mark_link_target_tables(size_t col_ndx_begin) noexcept
{
    // Beyond the constraints on the specified column index, this function must
    // assume no more than minimal consistency of the accessor hierarchy. This
    // means in particular that it cannot access the underlying node
    // structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());
    REALM_ASSERT(!m_columns.is_attached() || col_ndx_begin <= m_cols.size());

    size_t n = m_cols.size();
    for (size_t i = col_ndx_begin; i < n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->mark(ColumnBase::mark_LinkTargets);
    }
}


void Table::mark_opposite_link_tables() noexcept
{
    // Beyond the constraints on the specified column index, this function must
    // assume no more than minimal consistency of the accessor hierarchy. This
    // means in particular that it cannot access the underlying node
    // structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());

    for (auto& col : m_cols) {
        if (col != nullptr) {
            col->mark(ColumnBase::mark_LinkOrigins | ColumnBase::mark_LinkTargets);
        }
    }
}


void Table::refresh_accessor_tree()
{
    REALM_ASSERT(is_attached());

    if (m_top.is_attached()) {
        // Root table (free-standing table, group-level table, or subtable with
        // independent descriptor)
        m_top.init_from_parent();
        // subspecs may be deleted here ...
        if (m_spec->init_from_parent()) {
            // ... so get rid of cached entries here
            if (DescriptorRef desc = m_descriptor.lock()) {
                using df = _impl::DescriptorFriend;
                df::detach_subdesc_accessors(*desc);
            }
        }
        m_columns.init_from_parent();
        if (m_top.size() > 2) {
            m_clusters.init_from_parent();
        }
    }
    else {
        // Subtable with shared descriptor
        refresh_spec_accessor();

        // If the underlying table was degenerate, then `m_cols` must still be
        // empty.
        REALM_ASSERT(m_columns.is_attached() || m_cols.empty());

        ref_type columns_ref = m_columns.get_ref_from_parent();
        if (columns_ref != 0) {
            if (!m_columns.is_attached()) {
                // The underlying table is no longer degenerate
                size_t num_cols = m_spec->get_column_count();
                m_cols.resize(num_cols); // Throws
            }
            m_columns.init_from_ref(columns_ref);
        }
        else if (m_columns.is_attached()) {
            // The underlying table has become degenerate
            m_columns.detach();
            destroy_column_accessors();
        }
    }

    refresh_column_accessors(); // Throws
    m_mark = false;
}

void Table::refresh_spec_accessor()
{
    REALM_ASSERT(is_attached());
    if (!m_top.is_attached()) {
        // this is only relevant for subtables

        ArrayParent* array_parent = m_columns.get_parent();
        REALM_ASSERT(dynamic_cast<Parent*>(array_parent));
        Parent* table_parent = static_cast<Parent*>(array_parent);

        Spec* subspec = table_parent->get_subtable_spec();
        m_spec = subspec;
    }
}

void Table::refresh_column_accessors(size_t col_ndx_begin)
{
    // Index of column in Table::m_columns, which is not always equal to the
    // 'logical' column index.
    size_t ndx_in_parent = m_spec->get_column_ndx_in_parent(col_ndx_begin);

    size_t col_ndx_end = m_cols.size();
    for (size_t col_ndx = col_ndx_begin; col_ndx != col_ndx_end; ++col_ndx) {
        ColumnBase* col = m_cols[col_ndx];

        // If there is no search index accessor, but the column has been
        // equipped with a search index, create the accessor now.
        ColumnAttr attr = m_spec->get_column_attr(col_ndx);
        bool column_has_search_index = (attr & col_attr_Indexed) != 0;

        if (!column_has_search_index && col)
            col->destroy_search_index();

        // If the current column accessor is StringColumn, but the underlying
        // column has been upgraded to an enumerated strings column, then we
        // need to replace the accessor with an instance of StringEnumColumn.
        if (dynamic_cast<StringColumn*>(col) != nullptr) {
            ColumnType col_type = m_spec->get_column_type(col_ndx);
            if (col_type == col_type_StringEnum) {
                delete col;
                col = 0;
                // We need to store null in `m_cols` to avoid a crash during
                // destruction of the table accessor in case an error occurs
                // before the refresh operation is complete.
                m_cols[col_ndx] = nullptr;
            }
        } else if (dynamic_cast<StringEnumColumn*>(col) != nullptr) {
            // If the current column accessor is StringEnumColumn, but the
            // underlying column has changed to a StringColumn (which can occur
            // in a rollback), then we need to replace the accessor with an
            // instance of StringColumn.
            ColumnType col_type = m_spec->get_column_type(col_ndx);
            if (col_type == col_type_String) {
                delete col;
                col = nullptr;
                // We need to store null in `m_cols` to avoid a crash during
                // destruction of the table accessor in case an error occurs
                // before the refresh operation is complete.
                m_cols[col_ndx] = nullptr;
            }
        }

        if (col) {
            // Refresh the column accessor
            col->set_ndx_in_parent(ndx_in_parent);
            col->refresh_accessor_tree(col_ndx, *m_spec); // Throws
        }
        else {
            ColumnType col_type = m_spec->get_column_type(col_ndx);
            col = create_column_accessor(col_type, col_ndx, ndx_in_parent); // Throws
            m_cols[col_ndx] = col;
            // In the case of a link-type column, we must establish a connection
            // between it and the corresponding backlink column. This, however,
            // cannot be done until both the origin and the target table
            // accessor have been sufficiently refreshed. The solution is to
            // attempt the connection establishment when the link column is
            // created, and when the backlink column is created. In both cases,
            // if the opposite table accessor is still dirty, the establishment
            // of the connection is postponed.
            typedef _impl::GroupFriend gf;
            if (is_link_type(col_type)) {
                bool weak_links = (attr & col_attr_StrongLinks) == 0;
                LinkColumnBase* link_col = static_cast<LinkColumnBase*>(col);
                link_col->set_weak_links(weak_links);
                Group& group = *get_parent_group();
                size_t target_table_ndx = m_spec->get_opposite_link_table_ndx(col_ndx);
                Table& target_table = gf::get_table(group, target_table_ndx); // Throws
                if (!target_table.is_marked() && &target_table != this) {
                    size_t origin_ndx_in_group = m_top.get_ndx_in_parent();
                    size_t backlink_col_ndx = target_table.m_spec->find_backlink_column(origin_ndx_in_group, col_ndx);
                    connect_opposite_link_columns(col_ndx, target_table, backlink_col_ndx);
                }
            }
            else if (col_type == col_type_BackLink) {
                Group& group = *get_parent_group();
                size_t origin_table_ndx = m_spec->get_opposite_link_table_ndx(col_ndx);
                Table& origin_table = gf::get_table(group, origin_table_ndx); // Throws
                if (!origin_table.is_marked() || &origin_table == this) {
                    size_t link_col_ndx = m_spec->get_origin_column_ndx(col_ndx);
                    origin_table.connect_opposite_link_columns(link_col_ndx, *this, col_ndx);
                }
            }
        }

        if (column_has_search_index) {
            if (col->has_search_index()) {
            }
            else {
                ref_type ref = m_columns.get_as_ref(ndx_in_parent + 1);
                col->set_search_index_ref(ref, &m_columns, ndx_in_parent + 1); // Throws
            }
        }

        ndx_in_parent += (column_has_search_index ? 2 : 1);
    }

    // Set table size
    if (m_cols.empty()) {
        discard_row_accessors();
        m_size = 0;
    }
    else {
        ColumnBase* first_col = m_cols[0];
        m_size = first_col->size();
    }
    size_t sz = m_clusters.size();
    if (sz > m_size) {
        m_size = sz;
    }
}


void Table::refresh_link_target_accessors(size_t col_ndx_begin)
{
    REALM_ASSERT_3(col_ndx_begin, <=, m_spec->get_public_column_count());
    typedef _impl::GroupFriend gf;

    Group* group = get_parent_group();
    // Only update backlink columns that belong to a different table (in the same group).
    // If a table is linked to itself, backlinks will be updated correctly as part
    // of refresh_column_accessors(), so the case of free standing tables is already handled.
    if (group) {
        size_t origin_ndx_in_group = m_top.get_ndx_in_parent();
        size_t col_ndx_end = m_spec->get_public_column_count(); // No need to check backlink columns

        for (size_t col_ndx = col_ndx_begin; col_ndx != col_ndx_end; ++col_ndx) {
            ColumnType col_type = m_spec->get_column_type(col_ndx);
            if (is_link_type(col_type)) {
                size_t target_table_ndx = m_spec->get_opposite_link_table_ndx(col_ndx);
                Table& target_table = gf::get_table(*group, target_table_ndx); // Throws
                ColumnBase* col = m_cols[col_ndx];
                if (col && !target_table.is_marked() && (&target_table != this)) {
                    LinkColumnBase* link_col = static_cast<LinkColumnBase*>(col);
                    BacklinkColumn& backlink_col = link_col->get_backlink_column();
                    size_t backlink_col_ndx = target_table.m_spec->find_backlink_column(origin_ndx_in_group, col_ndx);
                    backlink_col.refresh_accessor_tree(backlink_col_ndx, *target_table.m_spec);
                }
            }
        }
    }
}


bool Table::is_cross_table_link_target() const noexcept
{
    size_t n = m_cols.size();
    for (size_t i = m_spec->get_public_column_count(); i < n; ++i) {
        REALM_ASSERT(dynamic_cast<BacklinkColumn*>(m_cols[i]));
        BacklinkColumn& backlink_col = static_cast<BacklinkColumn&>(*m_cols[i]);
        Table& origin = backlink_col.get_origin_table();
        if (&origin != this)
            return true;
    }
    return false;
}


void Table::generate_patch(const Table* table, std::unique_ptr<HandoverPatch>& patch)
{
    if (table) {
        patch.reset(new Table::HandoverPatch);
        patch->m_table_num = table->get_index_in_group();
        patch->m_is_sub_table = (patch->m_table_num == npos);

        if (patch->m_is_sub_table) {
            auto col = dynamic_cast<SubtableColumn*>(table->m_columns.get_parent());
            if (col) {
                Table* parent_table = col->m_table;
                patch->m_table_num = parent_table->get_index_in_group();
                if (patch->m_table_num == npos)
                    throw std::runtime_error("Table handover failed: only first level subtables supported");
                patch->m_col_ndx = col->get_column_index();
                patch->m_row_ndx = table->m_columns.get_ndx_in_parent();
            }
            else {
                throw std::runtime_error("Table handover failed: not a group level table");
            }
        }
    }
    else {
        patch.reset();
    }
}


TableRef Table::create_from_and_consume_patch(std::unique_ptr<HandoverPatch>& patch, Group& group)
{
    if (patch) {
        TableRef result;
        if (patch->m_is_sub_table) {
            auto parent_table = group.get_table(patch->m_table_num);
            result = parent_table->get_subtable(patch->m_col_ndx, patch->m_row_ndx);
        }
        else {
            result = group.get_table(patch->m_table_num);
        }
        patch.reset();
        return result;
    }
    return TableRef();
}

// LCOV_EXCL_START ignore debug functions

void Table::verify() const
{
#ifdef REALM_DEBUG
    REALM_ASSERT(is_attached());
    if (!m_columns.is_attached())
        return; // Accessor for degenerate subtable

    if (m_top.is_attached())
        m_top.verify();
    m_columns.verify();
    m_spec->verify();
    m_clusters.verify();
#endif
}

#ifdef REALM_DEBUG

void Table::to_dot(std::ostream& out, StringData title) const
{
    if (m_top.is_attached()) {
        out << "subgraph cluster_table_with_spec" << m_top.get_ref() << " {" << std::endl;
        out << " label = \"Table";
        if (0 < title.size())
            out << "\\n'" << title << "'";
        out << "\";" << std::endl;
        m_top.to_dot(out, "table_top");
        m_spec->to_dot(out);
    }
    else {
        out << "subgraph cluster_table_" << m_columns.get_ref() << " {" << std::endl;
        out << " label = \"Table";
        if (0 < title.size())
            out << " " << title;
        out << "\";" << std::endl;
    }

    to_dot_internal(out);

    out << "}" << std::endl;
}


void Table::to_dot_internal(std::ostream& out) const
{
    m_columns.to_dot(out, "columns");

    // Columns
    size_t n = get_column_count();
    for (size_t i = 0; i != n; ++i) {
        const ColumnBase& col = get_column_base(i);
        StringData name = get_column_name(i);
        col.to_dot(out, name);
        if (has_search_index(i)) {
            col.get_search_index()->to_dot_2(out, "");
        }
    }
}


void Table::print() const
{
    // Table header
    std::cout << "Table (name = \"" << std::string(get_name()) << "\",  size = " << m_size << ")\n    ";
    size_t column_count = m_spec->get_column_count(); // We can print backlinks too.
    for (size_t i = 0; i < column_count; ++i) {
        std::string name = "backlink";
        if (i < get_column_count()) {
            name = m_spec->get_column_name(i);
        }
        std::cout << std::left << std::setw(10) << std::string(name).substr(0, 10) << " ";
    }

    // Types
    std::cout << "\n    ";
    for (size_t i = 0; i < column_count; ++i) {
        ColumnType type = get_real_column_type(i);
        switch (type) {
            case col_type_Int:
                std::cout << "Int        ";
                break;
            case col_type_Float:
                std::cout << "Float      ";
                break;
            case col_type_Double:
                std::cout << "Double     ";
                break;
            case col_type_Bool:
                std::cout << "Bool       ";
                break;
            case col_type_String:
                std::cout << "String     ";
                break;
            case col_type_StringEnum:
                std::cout << "String     ";
                break;
            case col_type_Link: {
                size_t target_table_ndx = m_spec->get_opposite_link_table_ndx(i);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_ndx);
                const StringData target_name = target_table->get_name();
                std::cout << "L->" << std::setw(7) << std::string(target_name).substr(0, 7) << " ";
                break;
            }
            case col_type_LinkList: {
                size_t target_table_ndx = m_spec->get_opposite_link_table_ndx(i);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_ndx);
                const StringData target_name = target_table->get_name();
                std::cout << "LL->" << std::setw(6) << std::string(target_name).substr(0, 6) << " ";
                break;
            }
            case col_type_BackLink: {
                size_t target_table_ndx = m_spec->get_opposite_link_table_ndx(i);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_ndx);
                const StringData target_name = target_table->get_name();
                std::cout << "BL->" << std::setw(6) << std::string(target_name).substr(0, 6) << " ";
                break;
            }
            case col_type_Binary:
                std::cout << "Binary     ";
                break;
            case col_type_Table:
                std::cout << "SubTable   ";
                break;
            case col_type_Mixed:
                std::cout << "Mixed      ";
                break;
            case col_type_OldDateTime:
                std::cout << "OldDateTime";
                break;
            case col_type_Timestamp:
                std::cout << "Timestamp  ";
                break;
            case col_type_Reserved4:
                std::cout << "Reserved4  ";
                break;
            default:
                REALM_ASSERT(false);
        }
    }
    std::cout << "\n";

    // Columns
    for (size_t i = 0; i < m_size; ++i) {
        std::cout << std::setw(4) << i;
        for (size_t n = 0; n < column_count; ++n) {
            ColumnType type = get_real_column_type(n);
            if (is_nullable(n) && is_null(n, i)) {
                std::cout << std::setw(10) << "null"
                          << " ";
                continue;
            }
            switch (type) {
                case col_type_Int: {
                    size_t value = to_size_t(get_int(n, i));
                    std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Float: {
                    float value = get_float(n, i);
                    std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Double: {
                    double value = get_double(n, i);
                    std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Bool: {
                    bool value = get_bool(n, i);
                    std::cout << std::setw(10) << (value ? "true" : "false") << " ";
                    break;
                }
                case col_type_String: {
                    std::string value = get_string(n, i);
                    std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_StringEnum: {
                    const StringEnumColumn& col = get_column_string_enum(n);
                    std::cout << std::setw(10) << col.get(i) << " ";
                    break;
                }
                case col_type_Link: {
                    size_t value = get_link(n, i);
                    std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Binary: {
                    BinaryData value = get_binary(n, i);
                    std::cout << "size:" << std::setw(5) << value.size() << " ";
                    break;
                }
                case col_type_Table: {
                    const SubtableColumn& col = get_column_table(n);
                    std::cout << std::setw(10) << col.get(i) << " ";
                    break;
                }
                case col_type_Timestamp: {
                    Timestamp value = get_timestamp(n, i);
                    std::cout << std::setw(5) << value.get_seconds() << std::setw(5) << value.get_nanoseconds()
                              << " ";
                    break;
                }
                case col_type_LinkList: {
                    size_t value = get_link_count(n, i);
                    std::cout << "count:" << std::setw(4) << value << " ";
                    break;
                }
                case col_type_BackLink: {
                    const BacklinkColumn& col = get_column_backlink(n);
                    size_t value = col.get_backlink_count(i);
                    std::cout << "count:" << std::setw(4) << value << " ";
                    break;
                }

                // Not supported
                case col_type_Mixed:
                case col_type_OldDateTime:
                case col_type_Reserved4:
                default:
                    REALM_ASSERT(false);
            }
        }
        std::cout << "\n";
    }
    std::cout << "\n";
}


MemStats Table::stats() const
{
    MemStats mem_stats;
    m_top.stats(mem_stats);
    return mem_stats;
}


void Table::dump_node_structure() const
{
    dump_node_structure(std::cerr, 0);
}

void Table::dump_node_structure(std::ostream& out, int level) const
{
    int indent = level * 2;
    out << std::setw(indent) << ""
        << "Table (top_ref: " << m_top.get_ref() << ")\n";
    size_t n = get_column_count();
    for (size_t i = 0; i != n; ++i) {
        out << std::setw(indent) << ""
            << "  Column " << (i + 1) << "\n";
        const ColumnBase& col = get_column_base(i);
        col.do_dump_node_structure(out, level + 2);
    }
}

#endif // LCOV_EXCL_STOP ignore debug functions

Obj Table::create_object(Key key)
{
    if (key == null_key) {
        if (m_next_key_value == -1) {
            m_next_key_value = m_clusters.get_last_key() + 1;
        }
        key = Key(m_next_key_value++);
    }

    Obj obj = m_clusters.insert(key);
    bump_version();
    m_size++;

    return obj;
}

void Table::do_remove_object(Key key, bool /* broken_reciprocal_backlinks */)
{
    m_clusters.erase(key);
    bump_version();
    m_size--; // TODO: Redundant
}

Table::ConstIterator Table::begin() const
{
    return ConstIterator(m_clusters, 0);
}

Table::ConstIterator Table::end() const
{
    return ConstIterator(m_clusters, size());
}

Table::Iterator Table::begin()
{
    return Iterator(m_clusters, 0);
}

Table::Iterator Table::end()
{
    return Iterator(m_clusters, size());
}
