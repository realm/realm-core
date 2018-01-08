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
#include <realm/alloc_slab.hpp>
#include <realm/column.hpp>
#include <realm/column_string.hpp>
#include <realm/column_string_enum.hpp>
#include <realm/column_binary.hpp>
#include <realm/column_link.hpp>
#include <realm/column_linklist.hpp>
#include <realm/column_backlink.hpp>
#include <realm/column_timestamp.hpp>
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
#include <realm/table_tpl.hpp>

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

size_t Table::add_column(DataType type, StringData name, bool nullable)
{
    size_t col_ndx = get_column_count();
    insert_column(col_ndx, type, name, nullable); // Throws
    return col_ndx;
}

size_t Table::add_column_list(DataType type, StringData name)
{
    size_t col_ndx = get_column_count();

    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    LinkTargetInfo invalid_link;
    do_insert_column(col_ndx, type, name, invalid_link, false, true); // Throws

    return col_ndx;
}

size_t Table::add_column_link(DataType type, StringData name, Table& target, LinkType link_type)
{
    size_t col_ndx = get_column_count();
    insert_column_link(col_ndx, type, name, target, link_type); // Throws
    return col_ndx;
}


void Table::insert_column_link(size_t col_ndx, DataType type, StringData name, Table& target, LinkType link_type)
{
    if (REALM_UNLIKELY(!is_attached() || !target.is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(col_ndx > get_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);
    if (REALM_UNLIKELY(!is_link_type(ColumnType(type))))
        throw LogicError(LogicError::illegal_type);
    // Both origin and target must be group-level tables, and in the same group.
    Group* origin_group = get_parent_group();
    Group* target_group = target.get_parent_group();
    if (!origin_group || !target_group)
        throw LogicError(LogicError::wrong_kind_of_table);
    if (origin_group != target_group)
        throw LogicError(LogicError::group_mismatch);

    LinkTargetInfo link_target_info(&target);
    do_insert_column(col_ndx, type, name, link_target_info, false, type == type_LinkList); // Throws

    set_link_type(col_ndx, link_type); // Throws
}

void Table::remove_recursive(CascadeState& cascade_state)
{
    Group& group = *get_parent_group();
    // We will have to re-evaluate size() after each call to clusters.erase
    for (size_t i = 0; i < cascade_state.rows.size(); ++i) {
        CascadeState::row& row = cascade_state.rows[i];
        typedef _impl::GroupFriend gf;
        Table& table = gf::get_table(group, row.table_key);
        // This might add to the list of objects that should be deleted
        table.m_clusters.erase(row.key, cascade_state);
    }
    if (group.has_cascade_notification_handler())
        _impl::GroupFriend::send_cascade_notification(group, cascade_state);
}


void Table::insert_column(size_t col_ndx, DataType type, StringData name, bool nullable)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(col_ndx > get_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);
    if (REALM_UNLIKELY(is_link_type(ColumnType(type))))
        throw LogicError(LogicError::illegal_type);

    LinkTargetInfo invalid_link;
    do_insert_column(col_ndx, type, name, invalid_link, nullable); // Throws
}


void Table::remove_column(size_t col_ndx)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(col_ndx >= get_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);

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
    if (get_column_count() == 1)
        clear(); // Throws

    if (Replication* repl = get_repl())
        repl->erase_column(this, col_ndx); // Throws

    bump_content_version();
    bump_storage_version();
    erase_root_column(col_ndx); // Throws
}


void Table::rename_column(size_t col_ndx, StringData name)
{
    REALM_ASSERT(is_attached());

    REALM_ASSERT_3(col_ndx, <, get_column_count());

    m_spec->rename_column(col_ndx, name); // Throws

    bump_content_version();
    bump_storage_version();

    if (Replication* repl = get_repl())
        repl->rename_column(this, col_ndx, name); // Throws
}


TableKey Table::get_key_direct(Allocator& alloc, ref_type top_ref)
{
    // well, not quite "direct", more like "almost direct":
    Array table_top(alloc);
    table_top.init_from_ref(top_ref);
    if (table_top.size() > 3) {
        RefOrTagged rot = table_top.get_as_ref_or_tagged(top_position_for_key);
        return TableKey(rot.get_as_int());
    }
    else {
        return TableKey();
    }
}


void Table::init(ref_type top_ref, ArrayParent* parent, size_t ndx_in_parent, bool)
{
    // Load from allocated memory
    m_top.set_parent(parent, ndx_in_parent);
    m_top.init_from_ref(top_ref);

    m_spec.reset(new Spec(get_alloc()));
    m_spec->set_parent(&m_top, top_position_for_spec);
    m_spec->init_from_parent();

    // size_t columns_ndx_in_parent = 1;
    // columns no longer in use
    if (m_top.size() > top_position_for_cluster_tree) {
        m_clusters.set_parent(&m_top, top_position_for_cluster_tree);
        m_clusters.init_from_parent();
    }
    if (m_top.size() > top_position_for_search_indexes) {
        m_index_refs.set_parent(&m_top, top_position_for_search_indexes);
        m_index_refs.init_from_parent();
        m_index_accessors.resize(m_index_refs.size());
    }

    RefOrTagged rot = m_top.get_as_ref_or_tagged(top_position_for_key);
    REALM_ASSERT(rot.is_tagged());
    m_key = TableKey(rot.get_as_int());
}


void Table::do_insert_column(size_t col_ndx, DataType type, StringData name, LinkTargetInfo& link_target_info,
                             bool nullable, bool listtype)
{
    if (type == type_Link)
        nullable = true;

    bump_storage_version();
    insert_root_column(col_ndx, type, name, link_target_info, nullable, listtype); // Throws

    if (Replication* repl = get_repl())
        repl->insert_column(this, col_ndx, type, name, link_target_info, nullable, listtype); // Throws
}


void Table::do_insert_column_unless_exists(size_t col_ndx, DataType type, StringData name,
                                           LinkTargetInfo& link_target_info, bool nullable, bool listtype,
                                           bool* was_inserted)
{
    size_t existing_ndx = get_column_index(name);
    if (existing_ndx != npos) {
        col_ndx = existing_ndx;
    }

    if (col_ndx < get_column_count()) {
        StringData existing_name = get_column_name(col_ndx);
        if (existing_name == name) {
            DataType existing_type = get_column_type(col_ndx);
            if (existing_type != type) {
                throw LogicError(LogicError::type_mismatch);
            }
            bool existing_is_nullable = is_nullable(col_ndx);
            if (existing_is_nullable != nullable) {
                throw LogicError(LogicError::type_mismatch);
            }
            if (is_link_type(ColumnType(type)) &&
                m_spec->get_opposite_link_table_key(col_ndx) != link_target_info.m_target_table->get_key()) {
                throw LogicError(LogicError::type_mismatch);
            }

            // Column existed, and was identical to the requested column -- all is good.
            if (was_inserted) {
                *was_inserted = false;
            }
            return;
        }
        else {
            REALM_ASSERT_3(get_column_index(name), ==, npos);
        }
    }

    do_insert_column(col_ndx, type, name, link_target_info, nullable, listtype || type == type_LinkList);
    if (was_inserted) {
        *was_inserted = true;
    }
}


void Table::add_search_index(size_t column_ndx)
{
    if (REALM_UNLIKELY(column_ndx >= get_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);

    // Early-out of already indexed
    if (has_search_index(column_ndx))
        return;

    ColumnAttrMask attr = m_spec->get_column_attr(column_ndx);
    if (!StringIndex::type_supported(get_column_type(column_ndx))) {
        // FIXME: This is what we used to throw, so keep throwing that for compatibility reasons, even though it
        // should probably be a type mismatch exception instead.
        throw LogicError(LogicError::illegal_combination);
    }

    // m_index_accessors always has the same number of pointers as the number of columns. Columns without search
    // index have 0-entries.
    REALM_ASSERT(m_index_accessors.size() == get_column_count());
    REALM_ASSERT(m_index_accessors[column_ndx] == nullptr);

    // Mark the column as having an index
    attr = m_spec->get_column_attr(column_ndx);
    attr.set(col_attr_Indexed);
    m_spec->set_column_attr(column_ndx, attr); // Throws

    if (Replication* repl = get_repl())
        repl->add_search_index(this, column_ndx); // Throws
}

void Table::remove_search_index(size_t column_ndx)
{
    if (REALM_UNLIKELY(column_ndx >= get_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);

    // Early-out of non-indexed
    if (!has_search_index(column_ndx))
        return;

    ColumnAttrMask attr = m_spec->get_column_attr(column_ndx);

    // Destroy and remove the index column
    m_index_accessors[column_ndx] = nullptr;

    m_index_refs.set(column_ndx, 0);

    // Mark the column as no longer having an index
    attr = m_spec->get_column_attr(column_ndx);
    attr.reset(col_attr_Indexed);
    m_spec->set_column_attr(column_ndx, attr); // Throws

    if (Replication* repl = get_repl())
        repl->remove_search_index(this, column_ndx); // Throws
}

size_t Table::get_num_unique_values(size_t column_ndx) const
{
    REALM_ASSERT(is_attached());
    ColumnType col_type = m_spec->get_column_type(column_ndx);
    if (col_type != col_type_StringEnum)
        return 0;
    ref_type ref = m_spec->get_enumkeys_ref(column_ndx);
    StringColumn col(m_spec->get_alloc(), ref); // Throws
    return col.size();
}

void Table::insert_root_column(size_t col_ndx, DataType type, StringData name, LinkTargetInfo& link_target,
                               bool nullable, bool listtype)
{
    using tf = _impl::TableFriend;

    REALM_ASSERT_3(col_ndx, <=, m_spec->get_public_column_count());

    do_insert_root_column(col_ndx, ColumnType(type), name, nullable, listtype); // Throws

    // When the inserted column is a link-type column, we must also add a
    // backlink column to the target table, however, since the origin column
    // accessor does not yet exist, the connection between the column accessors
    // (Table::connect_opposite_link_columns()) cannot be established yet. The
    // marking of the target table tells Table::refresh_column_accessors() that
    // it should not try to establish the connection yet. The connection will be
    // established by Table::refresh_column_accessors() when it is invoked for
    // the target table below.

    if (link_target.is_valid()) {
        auto target_table_key = link_target.m_target_table->get_key();
        m_spec->set_opposite_link_table_key(col_ndx, target_table_key); // Throws
    }

    if (link_target.is_valid()) {
        auto origin_table_key = get_key();
        if (link_target.m_backlink_col_ndx == realm::npos) {
            const Spec& target_spec = tf::get_spec(*(link_target.m_target_table));
            link_target.m_backlink_col_ndx = target_spec.get_column_count(); // insert at back of target
        }
        std::string backlink_col_name = std::string(get_name()) + "_" + std::string(name);
        // Make sure the new name does not violate the name length limit
        if (backlink_col_name.size() > Table::max_column_name_length) {
            // It does - replace the last 8 characters with a hash value
            size_t hash_length = 8;
            std::hash<std::string> str_hash;
            auto hash = str_hash(backlink_col_name);
            backlink_col_name.resize(Table::max_column_name_length - hash_length);
            backlink_col_name += util::to_string(hash).substr(0, hash_length);
        }
        link_target.m_target_table->insert_backlink_column(origin_table_key, col_ndx, link_target.m_backlink_col_ndx,
                                                           backlink_col_name); // Throws
    }
}


void Table::erase_root_column(size_t col_ndx)
{
    REALM_ASSERT_3(col_ndx, <, m_spec->get_public_column_count());

    do_erase_root_column(col_ndx); // Throws
}


void Table::do_insert_root_column(size_t ndx, ColumnType type, StringData name, bool nullable, bool listtype)
{
    int attr = col_attr_None;
    if (nullable)
        attr |= col_attr_Nullable;
    if (listtype)
        attr |= col_attr_List;
    m_spec->insert_column(ndx, type, name, attr); // Throws

    // Backlink columns don't have search index
    if (type != col_type_BackLink) {
        // Column has no search index
        m_index_refs.insert(ndx, 0);
        m_index_accessors.insert(m_index_accessors.begin() + ndx, nullptr);
    }

    if (m_clusters.is_attached()) {
        m_clusters.insert_column(ndx);
    }
}


void Table::do_erase_root_column(size_t ndx)
{
    bool search_index = has_search_index(ndx);
    m_spec->erase_column(ndx); // Throws

    // If the column had a source index we have to remove and destroy that as well
    if (search_index) {
        ref_type index_ref = m_index_refs.get_as_ref(ndx);
        Array::destroy_deep(index_ref, m_index_refs.get_alloc());
        m_index_refs.erase(ndx);
        m_index_accessors.erase(m_index_accessors.begin() + ndx);
    }

    if (m_clusters.is_attached()) {
        m_clusters.remove_column(ndx);
    }
}


void Table::set_link_type(size_t col_ndx, LinkType link_type)
{
    bool weak_links = false;
    switch (link_type) {
        case link_Strong:
            break;
        case link_Weak:
            weak_links = true;
            break;
    }

    ColumnAttrMask attr = m_spec->get_column_attr(col_ndx);
    ColumnAttrMask new_attr{attr};
    new_attr.reset(col_attr_StrongLinks);
    if (!weak_links)
        new_attr.set(col_attr_StrongLinks);
    if (new_attr == attr)
        return;
    m_spec->set_column_attr(col_ndx, new_attr);

    if (Replication* repl = get_repl())
        repl->set_link_type(this, col_ndx, link_type); // Throws
}


void Table::insert_backlink_column(TableKey origin_table_key, size_t origin_col_ndx, size_t backlink_col_ndx,
                                   StringData name)
{
    do_insert_root_column(backlink_col_ndx, col_type_BackLink, name);       // Throws
    m_spec->set_opposite_link_table_key(backlink_col_ndx, origin_table_key); // Throws
    m_spec->set_backlink_origin_column(backlink_col_ndx, origin_col_ndx);    // Throws
}


void Table::erase_backlink_column(TableKey origin_table_key, size_t origin_col_ndx)
{
    size_t backlink_col_ndx = m_spec->find_backlink_column(origin_table_key, origin_col_ndx);
    REALM_ASSERT_3(backlink_col_ndx, !=, realm::not_found);
    do_erase_root_column(backlink_col_ndx); // Throws
}



void Table::update_accessors(AccessorUpdater& updater)
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    REALM_ASSERT(is_attached());

    updater.update(*this); // Throws
}


void Table::detach() noexcept
{
    if (Replication* repl = get_repl())
        repl->on_table_destroyed(this);
    m_alloc.bump_instance_version();
    m_spec->detach();
    m_top.detach();
}


Table::~Table() noexcept
{
    // If destroyed as a standalone table, destroy all memory allocated
    if (m_top.get_parent() == nullptr) {
        m_top.destroy_deep();
    }
    if (is_attached())
        detach();
}


bool Table::has_search_index(size_t col_ndx) const noexcept
{
    ColumnAttrMask attr = m_spec->get_column_attr(col_ndx);
    return attr.test(col_attr_Indexed);
}


void Table::rebuild_search_index(size_t)
{
    for (size_t col_ndx = 0; col_ndx < get_column_count(); col_ndx++) {
        ColumnAttrMask attr = m_spec->get_column_attr(col_ndx);
        if (attr.test(col_attr_Indexed)) {
            attr.reset(col_attr_Indexed);
            m_spec->set_column_attr(col_ndx, attr);
            add_search_index(col_ndx);
        }
    }
}


bool Table::is_nullable(size_t col_ndx) const
{
    if (!is_attached()) {
        throw LogicError{LogicError::detached_accessor};
    }

    REALM_ASSERT_DEBUG(col_ndx < m_spec->get_column_count());
    return m_spec->get_column_attr(col_ndx).test(col_attr_Nullable) ||
           m_spec->get_column_type(col_ndx) == col_type_Link;
}


ref_type Table::create_empty_table(Allocator& alloc, TableKey key)
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
    RefOrTagged rot = RefOrTagged::make_tagged(key.value);
    top.add(rot);
    {
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_HasRefs, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        top.add(v); // Throws
        dg_2.release();
    }
    dg.release();
    return top.get_ref();
}


void Table::batch_erase_rows(const KeyColumn& keys)
{
    REALM_ASSERT(is_attached());
    size_t num_objs = keys.size();
    std::vector<Key> vec;
    vec.reserve(num_objs);
    for (size_t i = 0; i < num_objs; ++i) {
        Key key = keys.get(i);
        if (key != null_key) {
            vec.push_back(key);
        }
    }
    sort(vec.begin(), vec.end());
    vec.erase(unique(vec.begin(), vec.end()), vec.end());

    std::for_each(vec.begin(), vec.end(), [this](Key k) { remove_object(k); });
}


void Table::clear()
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    bool skip_cascade = !m_spec->has_strong_link_columns();
    size_t old_size = size();

    if (skip_cascade) {
        bool broken_reciprocal_backlinks = false;
        do_clear(broken_reciprocal_backlinks);
    }
    else {
        CascadeState state(CascadeState::Mode::strong);
        Allocator& alloc = get_alloc();
        // Group-level tables may have links, so in those cases we need to
        // discover all the rows that need to be cascade-removed.
        size_t num_cols = m_spec->get_column_count();
        for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
            auto attr = m_spec->get_column_attr(col_ndx);
            auto col_type = m_spec->get_column_type(col_ndx);
            if (attr.test(col_attr_StrongLinks) && is_link_type(col_type)) {

                // This function will add objects that should be deleted to 'state'
                auto func = [col_ndx, col_type, &state, &alloc](const Cluster* cluster) {
                    if (col_type == col_type_Link) {
                        ArrayKey values(alloc);
                        cluster->init_leaf(col_ndx, &values);
                        size_t sz = values.size();
                        for (size_t i = 0; i < sz; i++) {
                            if (Key key = values.get(i)) {
                                cluster->remove_backlinks(cluster->get_real_key(i), col_ndx, {key}, state);
                            }
                        }
                    }
                    else if (col_type == col_type_LinkList) {
                        ArrayInteger values(alloc);
                        cluster->init_leaf(col_ndx, &values);
                        size_t sz = values.size();
                        for (size_t i = 0; i < sz; i++) {
                            if (ref_type ref = values.get_as_ref(i)) {
                                ArrayKey links(alloc);
                                links.init_from_ref(ref);
                                if (links.size() > 0) {
                                    cluster->remove_backlinks(cluster->get_real_key(i), col_ndx, links.get_all(),
                                                              state);
                                }
                            }
                        }
                    }
                    // Continue
                    return false;
                };

                // Go through all clusters
                m_clusters.traverse(func);
            }
        }
        remove_recursive(state);
    }

    if (Replication* repl = get_repl())
        repl->clear_table(this, old_size); // Throws
}


void Table::do_clear(bool /* broken_reciprocal_backlinks */)
{
    if (m_clusters.is_attached()) {
        m_clusters.clear();
    }
    bump_content_version();
    bump_storage_version();
}

const Table* Table::get_parent_table_ptr(size_t* column_ndx_out) const noexcept
{
    REALM_ASSERT_DEBUG(is_attached());
    const Array& real_top = m_top;
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
    const Array& real_top = m_top;
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

TableKey Table::get_key() const noexcept
{
    return m_key;
}

TableRef Table::get_link_target(size_t col_ndx) noexcept
{
    auto target_key = m_spec->get_opposite_link_table_key(col_ndx);
    return get_parent_group()->get_table(target_key);
}

// count ----------------------------------------------

size_t Table::count_int(size_t col_ndx, int64_t value) const
{
    size_t count;
    if (is_nullable(col_ndx)) {
        aggregate<act_Count, util::Optional<int64_t>, int64_t>(col_ndx, value, &count);
    }
    else {
        aggregate<act_Count, int64_t, int64_t>(col_ndx, value, &count);
    }
    return count;
}
size_t Table::count_float(size_t col_ndx, float value) const
{
    size_t count;
    aggregate<act_Count, float, float>(col_ndx, value, &count);
    return count;
}
size_t Table::count_double(size_t col_ndx, double value) const
{
    size_t count;
    aggregate<act_Count, double, double>(col_ndx, value, &count);
    return count;
}
size_t Table::count_string(size_t col_ndx, StringData value) const
{
    size_t count;
    aggregate<act_Count, StringData, StringData>(col_ndx, value, &count);
    return count;
}

// sum ----------------------------------------------

int64_t Table::sum_int(size_t col_ndx) const
{
    if (is_nullable(col_ndx)) {
        return aggregate<act_Sum, util::Optional<int64_t>, int64_t>(col_ndx);
    }
    return aggregate<act_Sum, int64_t, int64_t>(col_ndx);
}
double Table::sum_float(size_t col_ndx) const
{
    return aggregate<act_Sum, float, double>(col_ndx);
}
double Table::sum_double(size_t col_ndx) const
{
    return aggregate<act_Sum, double, double>(col_ndx);
}

// average ----------------------------------------------

double Table::average_int(size_t col_ndx, size_t* value_count) const
{
    if (is_nullable(col_ndx)) {
        return average<util::Optional<int64_t>>(col_ndx, value_count);
    }
    return average<int64_t>(col_ndx, value_count);
}
double Table::average_float(size_t col_ndx, size_t* value_count) const
{
    return average<float>(col_ndx, value_count);
}
double Table::average_double(size_t col_ndx, size_t* value_count) const
{
    return average<double>(col_ndx, value_count);
}

// minimum ----------------------------------------------

#define USE_COLUMN_AGGREGATE 1

int64_t Table::minimum_int(size_t col_ndx, Key* return_ndx) const
{
    if (is_nullable(col_ndx)) {
        return aggregate<act_Min, util::Optional<int64_t>, int64_t>(col_ndx, 0, nullptr, return_ndx);
    }
    return aggregate<act_Min, int64_t, int64_t>(col_ndx, 0, nullptr, return_ndx);
}

float Table::minimum_float(size_t col_ndx, Key* return_ndx) const
{
    return aggregate<act_Min, float, float>(col_ndx, 0.f, nullptr, return_ndx);
}

double Table::minimum_double(size_t col_ndx, Key* return_ndx) const
{
    return aggregate<act_Min, double, double>(col_ndx, 0., nullptr, return_ndx);
}

Timestamp Table::minimum_timestamp(size_t col_ndx, Key* return_ndx) const
{
    return aggregate<act_Min, Timestamp, Timestamp>(col_ndx, Timestamp{}, nullptr, return_ndx);
}

// maximum ----------------------------------------------

int64_t Table::maximum_int(size_t col_ndx, Key* return_ndx) const
{
    if (is_nullable(col_ndx)) {
        return aggregate<act_Max, util::Optional<int64_t>, int64_t>(col_ndx, 0, nullptr, return_ndx);
    }
    return aggregate<act_Max, int64_t, int64_t>(col_ndx, 0, nullptr, return_ndx);
}

float Table::maximum_float(size_t col_ndx, Key* return_ndx) const
{
    return aggregate<act_Max, float, float>(col_ndx, 0.f, nullptr, return_ndx);
}

double Table::maximum_double(size_t col_ndx, Key* return_ndx) const
{
    return aggregate<act_Max, double, double>(col_ndx, 0., nullptr, return_ndx);
}

Timestamp Table::maximum_timestamp(size_t col_ndx, Key* return_ndx) const
{
    return aggregate<act_Max, Timestamp, Timestamp>(col_ndx, Timestamp{}, nullptr, return_ndx);
}

template <class T>
Key Table::find_first(size_t col_ndx, T value) const
{
    Key key;
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    LeafType leaf(get_alloc());
    traverse_clusters([&key, &col_ndx, &value, &leaf](const Cluster* cluster) {
        cluster->init_leaf(col_ndx, &leaf);
        size_t row = leaf.find_first(value, 0, cluster->node_size());
        if (row != realm::npos) {
            key = cluster->get_real_key(row);
            return true;
        }
        return false;
    });
    return key;
}

namespace realm {

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
}

// Explicitly instantiate the generic case of the template for the types we care about.
template Key Table::find_first(size_t col_ndx, bool) const;
template Key Table::find_first(size_t col_ndx, int64_t) const;
template Key Table::find_first(size_t col_ndx, float) const;
template Key Table::find_first(size_t col_ndx, double) const;
template Key Table::find_first(size_t col_ndx, util::Optional<bool>) const;
template Key Table::find_first(size_t col_ndx, util::Optional<int64_t>) const;
template Key Table::find_first(size_t col_ndx, BinaryData) const;


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

TableView Table::find_all_link(Key target_key)
{
    TableView tv = where().links_to(m_link_chain[0], target_key).find_all();
    m_link_chain.clear();
    return tv;
}

ConstTableView Table::find_all_link(Key target_key) const
{
    return const_cast<Table*>(this)->find_all_link(target_key);
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


TableView Table::get_backlink_view(Key key, Table* src_table, size_t src_col_ndx)
{
    // FIXME: Assert not possible as get_column_link_base no longer exists
    // REALM_ASSERT(&src_table->get_column_link_base(src_col_ndx).get_target_table() == this);
    TableView tv(src_table, src_col_ndx, get_object(key));
    tv.do_sync();
    return tv;
}


const Table* Table::get_link_chain_target(const std::vector<size_t>& link_chain) const
{
    const Table* table = this;
    for (size_t t = 0; t < link_chain.size(); t++) {
        // Link column can be a single Link, LinkList, or BackLink.
        ColumnType type = table->get_real_column_type(link_chain[t]);
        if (type == col_type_LinkList || type == col_type_Link || type == col_type_BackLink) {
            auto key = table->m_spec->get_opposite_link_table_key(link_chain[t]);
            table = table->get_parent_group()->get_table(key).get();
        }
        else {
            // Only last column in link chain is allowed to be non-link
            if (t + 1 != link_chain.size())
                throw(LogicError::type_mismatch);
        }
    }
    return table;
}


void Table::optimize(bool)
{
    // At the present time there is only one kind of optimization that
    // we can do, and that is to replace a string column with a string
    // enumeration column.

    REALM_ASSERT(false); // FIXME: Unimplemented
    if (Replication* repl = get_repl())
        repl->optimize_table(this); // Throws
}

void Table::update_from_parent(size_t old_baseline) noexcept
{
    REALM_ASSERT(is_attached());

    // There is no top for sub-tables sharing spec
    if (m_top.is_attached()) {
        if (!m_top.update_from_parent(old_baseline))
            return;

        m_spec->update_from_parent(old_baseline);
        if (m_top.size() > top_position_for_cluster_tree) {
            m_clusters.update_from_parent(old_baseline);
        }
        if (m_top.size() > top_position_for_search_indexes) {
            if (m_index_refs.update_from_parent(old_baseline)) {
                for (auto index : m_index_accessors) {
                    if (index != nullptr) {
                        index->update_from_parent(old_baseline);
                    }
                }
            }
        }
    }
    m_alloc.bump_storage_version();
    m_alloc.bump_content_version();
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

/*
namespace {

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
*/

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

void Table::to_json_row(size_t, std::ostream& out, size_t, std::map<std::string, std::string>& renames,
                        std::vector<ref_type>&) const
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
                // out << get_int(i, row_ndx);
                break;
            case type_Bool:
                // out << (get_bool(i, row_ndx) ? "true" : "false");
                break;
            case type_Float:
                // out_floats<float>(out, get_float(i, row_ndx));
                break;
            case type_Double:
                // out_floats<double>(out, get_double(i, row_ndx));
                break;
            case type_String:
                // out << "\"" << get_string(i, row_ndx) << "\"";
                break;
            case type_Binary:
                out << "\"";
                // out_binary(out, get_binary(i, row_ndx));
                out << "\"";
                break;
            case type_Timestamp:
                out << "\"";
                // out_timestamp(out, get_timestamp(i, row_ndx));
                out << "\"";
                break;
            case type_Link: {
                REALM_ASSERT(false); // FIXME: Unimplemented
                break;
            }
            case type_LinkList: {
                REALM_ASSERT(false); // FIXME: Unimplemented
                break;
            }
            case type_OldDateTime:
            case type_OldTable:
            case type_OldMixed:
                break;
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
    for (auto obj : *this) {
        to_string_row(obj.get_key(), out, widths);
    }

    if (out_count < row_count) {
        size_t rest = row_count - out_count;
        out << "... and " << rest << " more rows (total " << row_count << ")";
    }
}

void Table::row_to_string(Key key, std::ostream& out) const
{
    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    to_string_header(out, widths);

    // Print row contents
    to_string_row(key, out, widths);
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
            case type_Binary:
                // FIXME
                // for (size_t row = 0; row < row_count; ++row) {
                //     size_t len = chars_in_int(get_binary(col, row).size()) + 2;
                //     width = std::max(width, len);
                // }
                width += 6; // space for " bytes"
                break;
            case type_String: {
                // Find max length of the strings
                // FIXME
                // for (size_t row = 0; row < row_count; ++row) {
                //     size_t len = get_string(col, row).size();
                //     width = std::max(width, len);
                // }
                if (width > 20)
                    width = 23; // cut strings longer than 20 chars
                break;
            }
            case type_LinkList:
                width = 5;
                break;
            default:
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

} // anonymous namespace


void Table::to_string_row(Key key, std::ostream& out, const std::vector<size_t>& widths) const
{
    size_t column_count = get_column_count();
    size_t row_ndx_width = widths[0];

    out << std::scientific; // for float/double
    out.width(row_ndx_width);
    out << key.value << ":";

    ConstObj obj = get_object(key);
    for (size_t col = 0; col < column_count; ++col) {
        out << "  "; // spacing
        out.width(widths[col + 1]);

        if (is_nullable(col) && obj.is_null(col)) {
            out << "(null)";
            continue;
        }

        DataType type = get_column_type(col);
        switch (type) {
            case type_Bool:
                out << (obj.get<bool>(col) ? "true" : "false");
                break;
            case type_Int:
                out << obj.get<Int>(col);
                break;
            case type_Float:
                out << obj.get<float>(col);
                break;
            case type_Double:
                out << obj.get<double>(col);
                break;
            case type_String:
                out_string(out, obj.get<String>(col), 20);
                break;
            case type_Timestamp:
                out_timestamp(out, obj.get<Timestamp>(col));
                break;
            case type_Binary:
                out.width(widths[col + 1] - 6); // adjust for " bytes" text
                out << obj.get<Binary>(col).size() << " bytes";
                break;
            case type_Link:
                // FIXME: print linked row
                out << obj.get<Key>(col);
                break;
            case type_LinkList:
                // FIXME: print number of links in list
                REALM_ASSERT(false); // unimplemented
                break;
            default:
                break;
        }
    }

    out << "\n";
}


size_t Table::compute_aggregated_byte_size() const noexcept
{
    if (!is_attached())
        return 0;
    const Array& real_top = (m_top);
    MemStats stats_2;
    real_top.stats(stats_2);
    return stats_2.allocated;
}


bool Table::compare_objects(const Table& t) const
{
    if (size() != t.size()) {
        return false;
    }

    auto it1 = begin();
    auto it2 = t.begin();
    auto e = end();

    while (it1 != e) {
        if (*it1 == *it2) {
            ++it1;
            ++it2;
        }
        else {
            return false;
        }
    }

    return true;
}


void Table::check_lists_are_empty(size_t) const
{
    // FIXME: Due to a limitation in Sync, it is not legal to change the primary
    // key of a row that contains lists (including linklists) after those lists
    // have been populated. This limitation may be lifted in the future, but for
    // now it is necessary to ensure that all lists are empty before setting a
    // primary key (by way of set_int_unique() or set_string_unique() or set_null_unique()).

    REALM_ASSERT(false); // FIXME: Unimplemented
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

void Table::refresh_accessor_tree()
{
    REALM_ASSERT(is_attached());

    if (m_top.is_attached()) {
        // Root table (free-standing table, group-level table, or subtable with
        // independent descriptor)
        m_top.init_from_parent();
        m_spec->init_from_parent();
        if (m_top.size() > 2) {
            m_clusters.init_from_parent();
        }
    }
}

bool Table::is_cross_table_link_target() const noexcept
{
    size_t first_backlink_column = m_spec->first_backlink_column_index();
    size_t end_backlink_column = m_spec->get_column_count();
    for (size_t i = first_backlink_column; i < end_backlink_column; ++i) {
        auto t = m_spec->get_column_type(i);
        // look for a backlink with a different target than ourselves
        if (t == col_type_BackLink && m_spec->get_opposite_link_table_key(i) != get_key())
            return true;
    }
    return false;
}


void Table::generate_patch(const Table* table, std::unique_ptr<HandoverPatch>& patch)
{
    if (table) {
        patch.reset(new Table::HandoverPatch);
        patch->m_table_key = table->get_key();
    }
    else {
        patch.reset();
    }
}


TableRef Table::create_from_and_consume_patch(std::unique_ptr<HandoverPatch>& patch, Group& group)
{
    if (patch) {
        TableRef result;
        result = group.get_table(patch->m_table_key);
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

    if (m_top.is_attached())
        m_top.verify();
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
    to_dot_internal(out);

    out << "}" << std::endl;
}

void Table::to_dot_internal(std::ostream&) const
{
    REALM_ASSERT(false); // FIXME: Unimplemented
    // Columns
}


void Table::print() const
{
    // Table header
    std::cout << "Table (name = \"" << std::string(get_name()) << "\",  size = " << size() << ")\n    ";
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
                auto target_table_key = m_spec->get_opposite_link_table_key(i);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_key);
                const StringData target_name = target_table->get_name();
                std::cout << "L->" << std::setw(7) << std::string(target_name).substr(0, 7) << " ";
                break;
            }
            case col_type_LinkList: {
                auto target_table_key = m_spec->get_opposite_link_table_key(i);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_key);
                const StringData target_name = target_table->get_name();
                std::cout << "LL->" << std::setw(6) << std::string(target_name).substr(0, 6) << " ";
                break;
            }
            case col_type_BackLink: {
                auto target_table_key = m_spec->get_opposite_link_table_key(i);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_key);
                const StringData target_name = target_table->get_name();
                std::cout << "BL->" << std::setw(6) << std::string(target_name).substr(0, 6) << " ";
                break;
            }
            case col_type_Binary:
                std::cout << "Binary     ";
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
    for (auto obj : *this) {
        std::cout << std::setw(4) << obj.get_key().value;

        for (size_t n = 0; n < column_count; ++n) {
            ColumnType type = get_real_column_type(n);
            if (is_nullable(n) && obj.is_null(n)) {
                std::cout << std::setw(10) << "null"
                          << " ";
                continue;
            }
            switch (type) {
                case col_type_Int: {
                    // FIXME size_t value = to_size_t(get_int(n, i));
                    // std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Float: {
                    // FIXME float value = get_float(n, i);
                    // std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Double: {
                    // FIXME double value = get_double(n, i);
                    // std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Bool: {
                    // FIXME bool value = get_bool(n, i);
                    // std::cout << std::setw(10) << (value ? "true" : "false") << " ";
                    break;
                }
                case col_type_StringEnum:
                case col_type_String: {
                    std::string value = obj.get<String>(n);
                    std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Link: {
                    // FIXME size_t value = get_link(n, i);
                    // std::cout << std::setw(10) << value << " ";
                    break;
                }
                case col_type_Binary: {
                    // FIXME BinaryData value = get_binary(n, i);
                    // std::cout << "size:" << std::setw(5) << value.size() << " ";
                    break;
                }
                case col_type_Timestamp: {
                    // FIXME Timestamp value = get_timestamp(n, i);
                    // std::cout << std::setw(5) << value.get_seconds() << std::setw(5) << value.get_nanoseconds()
                    //           << " ";
                    break;
                }
                case col_type_LinkList: {
                    // FIXME size_t value = get_link_count(n, i);
                    // std::cout << "count:" << std::setw(4) << value << " ";
                    break;
                }
                case col_type_BackLink: {
                    // FIXME const BacklinkColumn& col = get_column_backlink(n);
                    // size_t value = col.get_backlink_count(i);
                    // std::cout << "count:" << std::setw(4) << value << " ";
                    break;
                }

                // Not supported
                case col_type_OldTable:
                case col_type_OldMixed:
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
    REALM_ASSERT(false); // FIXME: Unimplemented
}

#endif // LCOV_EXCL_STOP ignore debug functions

Obj Table::create_object(Key key)
{
    if (key == null_key) {
        if (m_next_key_value == -1) {
            m_next_key_value = m_clusters.get_last_key_value() + 1;
        }
        key = Key(m_next_key_value++);
    }

    Obj obj = m_clusters.insert(key);
    bump_content_version();
    bump_storage_version();

    return obj;
}

void Table::create_objects(size_t number, std::vector<Key>& keys)
{
    while (number--) {
        keys.push_back(create_object().get_key());
    }
}

void Table::create_objects(const std::vector<Key>& keys)
{
    for (auto k : keys) {
        create_object(k);
    }
}

// Called by replication with mode = none
void Table::do_remove_object(Key key)
{
    CascadeState state(CascadeState::Mode::none);
    state.rows.emplace_back(m_key, key);
    remove_recursive(state);
}

void Table::remove_object(Key key)
{
    REALM_ASSERT(is_attached());

    Group* g = get_parent_group();

    if (m_spec->has_strong_link_columns() || (g && g->has_cascade_notification_handler())) {
        CascadeState state(CascadeState::Mode::strong);
        state.rows.emplace_back(m_key, key);
        remove_recursive(state);
    }
    else {
        CascadeState state(CascadeState::Mode::none);
        m_clusters.erase(key, state);
    }
}

void Table::remove_object_recursive(Key key)
{
    size_t table_ndx = get_index_in_group();
    if (table_ndx != realm::npos) {
        CascadeState state(CascadeState::Mode::all);
        state.rows.emplace_back(m_key, key);
        remove_recursive(state);
    }
    else {
        // No links in freestanding table
        CascadeState state(CascadeState::Mode::none);
        m_clusters.erase(key, state);
    }
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

TableRef _impl::TableFriend::get_opposite_link_table(const Table& table, size_t col_ndx)
{
    auto target_table_key = table.m_spec->get_opposite_link_table_key(col_ndx);
    return table.get_parent_group()->get_table(target_table_key);
}
