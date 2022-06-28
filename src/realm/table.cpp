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

#include <stdexcept>

#ifdef REALM_DEBUG
#include <iostream>
#include <iomanip>
#endif

#include <realm/util/features.h>
#include <realm/util/miscellaneous.hpp>
#include <realm/util/serializer.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/exceptions.hpp>
#include <realm/table.hpp>
#include <realm/alloc_slab.hpp>
#include <realm/index_string.hpp>
#include <realm/db.hpp>
#include <realm/replication.hpp>
#include <realm/table_view.hpp>
#include <realm/query_engine.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_binary.hpp>
#include <realm/array_string.hpp>
#include <realm/array_timestamp.hpp>
#include <realm/array_decimal128.hpp>
#include <realm/array_fixed_bytes.hpp>
#include <realm/table_tpl.hpp>
#include <realm/dictionary.hpp>

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
///    descriptor (Table::m_spec.m_top).
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

Replication* Table::g_dummy_replication = nullptr;

bool TableVersions::operator==(const TableVersions& other) const
{
    if (size() != other.size())
        return false;
    size_t sz = size();
    for (size_t i = 0; i < sz; i++) {
        REALM_ASSERT_DEBUG(this->at(i).first == other.at(i).first);
        if (this->at(i).second != other.at(i).second)
            return false;
    }
    return true;
}

namespace realm {
const char* get_data_type_name(DataType type) noexcept
{
    switch (type) {
        case type_Int:
            return "int";
        case type_Bool:
            return "bool";
        case type_Float:
            return "float";
        case type_Double:
            return "double";
        case type_String:
            return "string";
        case type_Binary:
            return "binary";
        case type_Timestamp:
            return "timestamp";
        case type_ObjectId:
            return "objectId";
        case type_Decimal:
            return "decimal128";
        case type_UUID:
            return "uuid";
        case type_Mixed:
            return "mixed";
        case type_Link:
            return "link";
        case type_LinkList:
            return "linklist";
        case type_TypedLink:
            return "typedLink";
        default:
            if (type == type_TypeOfValue)
                return "@type";
    }
    return "unknown";
}
} // namespace realm

void LinkChain::add(ColKey ck)
{
    // Link column can be a single Link, LinkList, or BackLink.
    REALM_ASSERT(m_current_table->valid_column(ck));
    ColumnType type = ck.get_type();
    if (type == col_type_LinkList || type == col_type_Link || type == col_type_BackLink) {
        m_current_table = m_current_table->get_opposite_table(ck);
    }
    else {
        // Only last column in link chain is allowed to be non-link
        throw std::runtime_error(util::format("%1.%2 is not an object reference property",
                                              m_current_table->get_name(), m_current_table->get_column_name(ck)));
    }
    m_link_cols.push_back(ck);
}


// -- Table ---------------------------------------------------------------------------------

ColKey Table::add_column(DataType type, StringData name, bool nullable)
{
    if (REALM_UNLIKELY(is_link_type(ColumnType(type))))
        throw LogicError(LogicError::illegal_type);

    Table* invalid_link = nullptr;
    ColumnAttrMask attr;
    if (nullable || type == type_Mixed)
        attr.set(col_attr_Nullable);
    ColKey col_key = generate_col_key(ColumnType(type), attr);

    return do_insert_column(col_key, type, name, invalid_link); // Throws
}

ColKey Table::add_column(Table& target, StringData name)
{
    // Both origin and target must be group-level tables, and in the same group.
    Group* origin_group = get_parent_group();
    Group* target_group = target.get_parent_group();
    if (!origin_group || !target_group)
        throw LogicError(LogicError::wrong_kind_of_table);
    if (origin_group != target_group)
        throw LogicError(LogicError::group_mismatch);
    // Only links to embedded objects are allowed.
    if (is_asymmetric() && !target.is_embedded()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }
    // Incoming links from an asymmetric table are not allowed.
    if (target.is_asymmetric()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }

    m_has_any_embedded_objects.reset();

    ColumnAttrMask attr;
    attr.set(col_attr_Nullable);
    ColKey col_key = generate_col_key(col_type_Link, attr);

    auto retval = do_insert_column(col_key, type_Link, name, &target); // Throws
    return retval;
}

ColKey Table::add_column_list(DataType type, StringData name, bool nullable)
{
    Table* invalid_link = nullptr;
    ColumnAttrMask attr;
    attr.set(col_attr_List);
    if (nullable || type == type_Mixed)
        attr.set(col_attr_Nullable);
    ColKey col_key = generate_col_key(ColumnType(type), attr);
    return do_insert_column(col_key, type, name, invalid_link); // Throws
}

ColKey Table::add_column_set(DataType type, StringData name, bool nullable)
{
    Table* invalid_link = nullptr;
    ColumnAttrMask attr;
    attr.set(col_attr_Set);
    if (nullable || type == type_Mixed)
        attr.set(col_attr_Nullable);
    ColKey col_key = generate_col_key(ColumnType(type), attr);
    return do_insert_column(col_key, type, name, invalid_link); // Throws
}

ColKey Table::add_column_list(Table& target, StringData name)
{
    // Both origin and target must be group-level tables, and in the same group.
    Group* origin_group = get_parent_group();
    Group* target_group = target.get_parent_group();
    if (!origin_group || !target_group)
        throw LogicError(LogicError::wrong_kind_of_table);
    if (origin_group != target_group)
        throw LogicError(LogicError::group_mismatch);
    // Only links to embedded objects are allowed.
    if (is_asymmetric() && !target.is_embedded()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }
    // Incoming links from an asymmetric table are not allowed.
    if (target.is_asymmetric()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }

    m_has_any_embedded_objects.reset();

    ColumnAttrMask attr;
    attr.set(col_attr_List);
    ColKey col_key = generate_col_key(col_type_LinkList, attr);

    return do_insert_column(col_key, type_LinkList, name, &target); // Throws
}

ColKey Table::add_column_set(Table& target, StringData name)
{
    // Both origin and target must be group-level tables, and in the same group.
    Group* origin_group = get_parent_group();
    Group* target_group = target.get_parent_group();
    if (!origin_group || !target_group)
        throw LogicError(LogicError::wrong_kind_of_table);
    if (origin_group != target_group)
        throw LogicError(LogicError::group_mismatch);
    if (target.is_embedded())
        throw LogicError(LogicError::wrong_kind_of_table);
    // Outgoing links from an asymmetric table are not allowed.
    if (is_asymmetric()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }
    // Incoming links from an asymmetric table are not allowed.
    if (target.is_asymmetric()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }

    ColumnAttrMask attr;
    attr.set(col_attr_Set);
    ColKey col_key = generate_col_key(col_type_Link, attr);
    return do_insert_column(col_key, type_Link, name, &target); // Throws
}

ColKey Table::add_column_link(DataType type, StringData name, Table& target)
{
    if (REALM_UNLIKELY(!is_link_type(ColumnType(type))))
        throw LogicError(LogicError::illegal_type);

    if (type == type_LinkList) {
        return add_column_list(target, name);
    }
    else {
        REALM_ASSERT(type == type_Link);
        return add_column(target, name);
    }
}

ColKey Table::add_column_dictionary(DataType type, StringData name, bool nullable, DataType key_type)
{
    Table* invalid_link = nullptr;
    ColumnAttrMask attr;
    attr.set(col_attr_Dictionary);
    if (nullable || type == type_Mixed)
        attr.set(col_attr_Nullable);
    ColKey col_key = generate_col_key(ColumnType(type), attr);
    return do_insert_column(col_key, type, name, invalid_link, key_type); // Throws
}

ColKey Table::add_column_dictionary(Table& target, StringData name, DataType key_type)
{
    // Both origin and target must be group-level tables, and in the same group.
    Group* origin_group = get_parent_group();
    Group* target_group = target.get_parent_group();
    if (!origin_group || !target_group)
        throw LogicError(LogicError::wrong_kind_of_table);
    if (origin_group != target_group)
        throw LogicError(LogicError::group_mismatch);
    // Only links to embedded objects are allowed.
    if (is_asymmetric() && !target.is_embedded()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }
    // Incoming links from an asymmetric table are not allowed.
    if (target.is_asymmetric()) {
        throw LogicError(LogicError::wrong_kind_of_table);
    }

    ColumnAttrMask attr;
    attr.set(col_attr_Dictionary);
    attr.set(col_attr_Nullable);

    ColKey col_key = generate_col_key(ColumnType(col_type_Link), attr);
    return do_insert_column(col_key, type_Link, name, &target, key_type); // Throws
}

void Table::remove_recursive(CascadeState& cascade_state)
{
    Group* group = get_parent_group();
    REALM_ASSERT(group);
    cascade_state.m_group = group;

    do {
        cascade_state.send_notifications();

        for (auto& l : cascade_state.m_to_be_nullified) {
            group->get_table(l.origin_table)
                ->get_object(l.origin_key)
                .nullify_link(l.origin_col_key, l.old_target_link);
        }
        cascade_state.m_to_be_nullified.clear();

        auto to_delete = std::move(cascade_state.m_to_be_deleted);
        for (auto obj : to_delete) {
            auto table = group->get_table(obj.first);
            // This might add to the list of objects that should be deleted
            REALM_ASSERT(!obj.second.is_unresolved());
            table->m_clusters.erase(obj.second, cascade_state);
        }
        nullify_links(cascade_state);
    } while (!cascade_state.m_to_be_deleted.empty() || !cascade_state.m_to_be_nullified.empty());
}

void Table::nullify_links(CascadeState& cascade_state)
{
    Group* group = get_parent_group();
    REALM_ASSERT(group);
    for (auto& to_delete : cascade_state.m_to_be_deleted) {
        auto table = group->get_table(to_delete.first);
        table->m_clusters.nullify_links(to_delete.second, cascade_state);
    }
}


void Table::remove_column(ColKey col_key)
{
    check_column(col_key);

    if (Replication* repl = get_repl())
        repl->erase_column(this, col_key); // Throws

    if (col_key == m_primary_key_col) {
        do_set_primary_key_column(ColKey());
    }
    else {
        REALM_ASSERT_RELEASE(m_primary_key_col.get_index().val != col_key.get_index().val);
    }

    erase_root_column(col_key); // Throws
    m_has_any_embedded_objects.reset();
}


void Table::rename_column(ColKey col_key, StringData name)
{
    check_column(col_key);

    auto col_ndx = colkey2spec_ndx(col_key);
    m_spec.rename_column(col_ndx, name); // Throws

    bump_content_version();
    bump_storage_version();

    if (Replication* repl = get_repl())
        repl->rename_column(this, col_key, name); // Throws
}


TableKey Table::get_key_direct(Allocator& alloc, ref_type top_ref)
{
    // well, not quite "direct", more like "almost direct":
    Array table_top(alloc);
    table_top.init_from_ref(top_ref);
    if (table_top.size() > 3) {
        RefOrTagged rot = table_top.get_as_ref_or_tagged(top_position_for_key);
        return TableKey(int32_t(rot.get_as_int()));
    }
    else {
        return TableKey();
    }
}


void Table::init(ref_type top_ref, ArrayParent* parent, size_t ndx_in_parent, bool is_writable, bool is_frzn)
{
    REALM_ASSERT(!(is_writable && is_frzn));
    m_is_frozen = is_frzn;
    m_alloc.set_read_only(!is_writable);
    // Load from allocated memory
    m_top.set_parent(parent, ndx_in_parent);
    m_top.init_from_ref(top_ref);

    m_spec.init_from_parent();

    while (m_top.size() <= top_position_for_pk_col) {
        m_top.add(0);
    }

    if (m_top.get_as_ref(top_position_for_cluster_tree) == 0) {
        // This is an upgrade - create cluster
        MemRef mem = Cluster::create_empty_cluster(m_top.get_alloc()); // Throws
        m_top.set_as_ref(top_position_for_cluster_tree, mem.get_ref());
    }
    m_clusters.init_from_parent();

    RefOrTagged rot = m_top.get_as_ref_or_tagged(top_position_for_key);
    if (!rot.is_tagged()) {
        // Create table key
        rot = RefOrTagged::make_tagged(ndx_in_parent);
        m_top.set(top_position_for_key, rot);
    }
    m_key = TableKey(int32_t(rot.get_as_int()));

    // index setup relies on column mapping being up to date:
    build_column_mapping();
    if (m_top.get_as_ref(top_position_for_search_indexes) == 0) {
        // This is an upgrade - create the necessary arrays
        bool context_flag = false;
        size_t nb_columns = m_spec.get_column_count();
        MemRef mem = Array::create_array(Array::type_HasRefs, context_flag, nb_columns, 0, m_top.get_alloc());
        m_index_refs.init_from_mem(mem);
        m_index_refs.update_parent();
        mem = Array::create_array(Array::type_Normal, context_flag, nb_columns, TableKey().value, m_top.get_alloc());
        m_opposite_table.init_from_mem(mem);
        m_opposite_table.update_parent();
        mem = Array::create_array(Array::type_Normal, context_flag, nb_columns, ColKey().value, m_top.get_alloc());
        m_opposite_column.init_from_mem(mem);
        m_opposite_column.update_parent();
    }
    else {
        m_opposite_table.init_from_parent();
        m_opposite_column.init_from_parent();
        m_index_refs.init_from_parent();
        m_index_accessors.resize(m_index_refs.size());
    }
    if (!m_top.get_as_ref_or_tagged(top_position_for_column_key).is_tagged()) {
        m_top.set(top_position_for_column_key, RefOrTagged::make_tagged(0));
    }
    auto rot_version = m_top.get_as_ref_or_tagged(top_position_for_version);
    if (!rot_version.is_tagged()) {
        m_top.set(top_position_for_version, RefOrTagged::make_tagged(0));
        m_in_file_version_at_transaction_boundary = 0;
    }
    else
        m_in_file_version_at_transaction_boundary = rot_version.get_as_int();

    auto rot_pk_key = m_top.get_as_ref_or_tagged(top_position_for_pk_col);
    m_primary_key_col = rot_pk_key.is_tagged() ? ColKey(rot_pk_key.get_as_int()) : ColKey();

    if (m_top.size() <= top_position_for_flags) {
        m_table_type = Type::TopLevel;
    }
    else {
        uint64_t flags = m_top.get_as_ref_or_tagged(top_position_for_flags).get_as_int();
        m_table_type = Type(flags & table_type_mask);
    }
    m_has_any_embedded_objects.reset();

    if (m_top.size() > top_position_for_tombstones && m_top.get_as_ref(top_position_for_tombstones)) {
        // Tombstones exists
        if (!m_tombstones) {
            m_tombstones = std::make_unique<TableClusterTree>(this, m_alloc, size_t(top_position_for_tombstones));
        }
        m_tombstones->init_from_parent();
    }
    else {
        m_tombstones = nullptr;
    }
    m_cookie = cookie_initialized;
}


ColKey Table::do_insert_column(ColKey col_key, DataType type, StringData name, Table* target_table, DataType key_type)
{
    col_key = do_insert_root_column(col_key, ColumnType(type), name, key_type); // Throws

    // When the inserted column is a link-type column, we must also add a
    // backlink column to the target table.

    if (target_table) {
        auto backlink_col_key = target_table->do_insert_root_column(ColKey{}, col_type_BackLink, ""); // Throws
        target_table->check_column(backlink_col_key);

        set_opposite_column(col_key, target_table->get_key(), backlink_col_key);
        target_table->set_opposite_column(backlink_col_key, get_key(), col_key);
    }

    if (Replication* repl = get_repl())
        repl->insert_column(this, col_key, type, name, target_table); // Throws

    return col_key;
}


void Table::populate_search_index(ColKey col_key)
{
    auto col_ndx = col_key.get_index().val;
    StringIndex* index = m_index_accessors[col_ndx].get();

    // Insert ref to index
    for (auto o : *this) {
        ObjKey key = o.get_key();
        DataType type = get_column_type(col_key);

        if (type == type_Int) {
            if (is_nullable(col_key)) {
                Optional<int64_t> value = o.get<Optional<int64_t>>(col_key);
                index->insert(key, value); // Throws
            }
            else {
                int64_t value = o.get<int64_t>(col_key);
                index->insert(key, value); // Throws
            }
        }
        else if (type == type_Bool) {
            if (is_nullable(col_key)) {
                Optional<bool> value = o.get<Optional<bool>>(col_key);
                index->insert(key, value); // Throws
            }
            else {
                bool value = o.get<bool>(col_key);
                index->insert(key, value); // Throws
            }
        }
        else if (type == type_String) {
            StringData value = o.get<StringData>(col_key);
            index->insert(key, value); // Throws
        }
        else if (type == type_Timestamp) {
            Timestamp value = o.get<Timestamp>(col_key);
            index->insert(key, value); // Throws
        }
        else if (type == type_ObjectId) {
            if (is_nullable(col_key)) {
                Optional<ObjectId> value = o.get<Optional<ObjectId>>(col_key);
                index->insert(key, value); // Throws
            }
            else {
                ObjectId value = o.get<ObjectId>(col_key);
                index->insert(key, value); // Throws
            }
        }
        else if (type == type_UUID) {
            if (is_nullable(col_key)) {
                Optional<UUID> value = o.get<Optional<UUID>>(col_key);
                index->insert(key, value); // Throws
            }
            else {
                UUID value = o.get<UUID>(col_key);
                index->insert(key, value); // Throws
            }
        }
        else {
            REALM_ASSERT_RELEASE(false && "Data type does not support search index");
        }
    }
}

void Table::erase_from_search_indexes(ObjKey key)
{
    // Tombstones do not use index - will crash if we try to erase values
    if (!key.is_unresolved()) {
        for (auto&& index : m_index_accessors) {
            if (index) {
                index->erase(key);
            }
        }
    }
}

void Table::update_indexes(ObjKey key, const FieldValues& values)
{
    // Tombstones do not use index - will crash if we try to insert values
    if (key.is_unresolved()) {
        return;
    }

    auto sz = m_index_accessors.size();
    // values are sorted by column index - there may be values missing
    auto value = values.begin();
    for (size_t column_ndx = 0; column_ndx < sz; column_ndx++) {
        // Check if initial value is provided
        Mixed init_value;
        if (value != values.end() && value->col_key.get_index().val == column_ndx) {
            // Value for this column is provided
            init_value = value->value;
            ++value;
        }

        if (auto&& index = m_index_accessors[column_ndx]) {
            // There is an index for this column
            auto col_key = m_leaf_ndx2colkey[column_ndx];
            auto type = col_key.get_type();
            auto attr = col_key.get_attrs();
            bool nullable = attr.test(col_attr_Nullable);
            switch (type) {
                case col_type_Int:
                    if (init_value.is_null()) {
                        index->insert(key, ArrayIntNull::default_value(nullable));
                    }
                    else {
                        index->insert(key, init_value.get<int64_t>());
                    }
                    break;
                case col_type_Bool:
                    if (init_value.is_null()) {
                        index->insert(key, ArrayBoolNull::default_value(nullable));
                    }
                    else {
                        index->insert(key, init_value.get<bool>());
                    }
                    break;
                case col_type_String:
                    if (init_value.is_null()) {
                        index->insert(key, ArrayString::default_value(nullable));
                    }
                    else {
                        index->insert(key, init_value.get<String>());
                    }
                    break;
                case col_type_Timestamp:
                    if (init_value.is_null()) {
                        index->insert(key, ArrayTimestamp::default_value(nullable));
                    }
                    else {
                        index->insert(key, init_value.get<Timestamp>());
                    }
                    break;
                case col_type_ObjectId:
                    if (init_value.is_null()) {
                        index->insert(key, ArrayObjectIdNull::default_value(nullable));
                    }
                    else {
                        index->insert(key, init_value.get<ObjectId>());
                    }
                    break;
                case col_type_Mixed:
                    index->insert(key, init_value);
                    break;
                case col_type_UUID:
                    if (init_value.is_null()) {
                        index->insert(key, ArrayUUIDNull::default_value(nullable));
                    }
                    else {
                        index->insert(key, init_value.get<UUID>());
                    }
                    break;
                default:
                    REALM_UNREACHABLE();
            }
        }
    }
}

void Table::clear_indexes()
{
    for (auto&& index : m_index_accessors) {
        if (index) {
            index->clear();
        }
    }
}

void Table::do_add_search_index(ColKey col_key)
{
    size_t column_ndx = col_key.get_index().val;

    // Early-out if already indexed
    if (m_index_accessors[column_ndx] != nullptr)
        return;

    if (!StringIndex::type_supported(DataType(col_key.get_type())) || col_key.is_collection()) {
        // Not ideal, but this is what we used to throw, so keep throwing that for compatibility reasons, even though
        // it should probably be a type mismatch exception instead.
        throw LogicError(LogicError::illegal_combination);
    }

    // m_index_accessors always has the same number of pointers as the number of columns. Columns without search
    // index have 0-entries.
    REALM_ASSERT(m_index_accessors.size() == m_leaf_ndx2colkey.size());
    REALM_ASSERT(m_index_accessors[column_ndx] == nullptr);

    // Create the index
    m_index_accessors[column_ndx] =
        std::make_unique<StringIndex>(ClusterColumn(&m_clusters, col_key), get_alloc()); // Throws
    StringIndex* index = m_index_accessors[column_ndx].get();

    // Insert ref to index
    index->set_parent(&m_index_refs, column_ndx);
    m_index_refs.set(column_ndx, index->get_ref()); // Throws

    populate_search_index(col_key);
}

void Table::add_search_index(ColKey col_key)
{
    check_column(col_key);

    // Check spec
    auto spec_ndx = leaf_ndx2spec_ndx(col_key.get_index());
    auto attr = m_spec.get_column_attr(spec_ndx);
    if (attr.test(col_attr_Indexed)) {
        REALM_ASSERT(has_search_index(col_key));
        return;
    }

    do_add_search_index(col_key);

    // Update spec
    attr.set(col_attr_Indexed);
    m_spec.set_column_attr(spec_ndx, attr); // Throws
}

void Table::remove_search_index(ColKey col_key)
{
    check_column(col_key);
    auto column_ndx = col_key.get_index();

    // Early-out if non-indexed
    if (m_index_accessors[column_ndx.val] == nullptr)
        return;

    // Destroy and remove the index column
    auto& index = m_index_accessors[column_ndx.val];
    REALM_ASSERT(index != nullptr);
    index->destroy();
    index.reset();

    m_index_refs.set(column_ndx.val, 0);

    // update spec
    auto spec_ndx = leaf_ndx2spec_ndx(column_ndx);
    auto attr = m_spec.get_column_attr(spec_ndx);
    attr.reset(col_attr_Indexed);
    m_spec.set_column_attr(spec_ndx, attr); // Throws
}

void Table::enumerate_string_column(ColKey col_key)
{
    check_column(col_key);
    size_t column_ndx = colkey2spec_ndx(col_key);
    ColumnType type = col_key.get_type();
    if (type == col_type_String && !m_spec.is_string_enum_type(column_ndx)) {
        m_clusters.enumerate_string_column(col_key);
    }
}

bool Table::is_enumerated(ColKey col_key) const noexcept
{
    size_t col_ndx = colkey2spec_ndx(col_key);
    return m_spec.is_string_enum_type(col_ndx);
}

size_t Table::get_num_unique_values(ColKey col_key) const
{
    if (!is_enumerated(col_key))
        return 0;

    ArrayParent* parent;
    ref_type ref = const_cast<Spec&>(m_spec).get_enumkeys_ref(colkey2spec_ndx(col_key), parent);
    BPlusTree<StringData> col(get_alloc());
    col.init_from_ref(ref);

    return col.size();
}


void Table::erase_root_column(ColKey col_key)
{
    check_column(col_key);
    ColumnType col_type = col_key.get_type();
    if (is_link_type(col_type)) {
        auto target_table = get_opposite_table(col_key);
        auto target_column = get_opposite_column(col_key);
        target_table->do_erase_root_column(target_column);
    }
    do_erase_root_column(col_key); // Throws
}


ColKey Table::do_insert_root_column(ColKey col_key, ColumnType type, StringData name, DataType key_type)
{
    // if col_key specifies a key, it must be unused
    REALM_ASSERT(!col_key || !valid_column(col_key));

    // locate insertion point: ordinary columns must come before backlink columns
    size_t spec_ndx = (type == col_type_BackLink) ? m_spec.get_column_count() : m_spec.get_public_column_count();

    if (!col_key) {
        col_key = generate_col_key(type, {});
    }

    m_spec.insert_column(spec_ndx, col_key, type, name, col_key.get_attrs().m_value); // Throws
    if (col_key.is_dictionary()) {
        m_spec.set_dictionary_key_type(spec_ndx, key_type);
    }
    auto col_ndx = col_key.get_index().val;
    build_column_mapping();
    REALM_ASSERT(col_ndx <= m_index_refs.size());
    if (col_ndx == m_index_refs.size()) {
        m_index_refs.insert(col_ndx, 0);
    }
    else {
        m_index_refs.set(col_ndx, 0);
    }
    REALM_ASSERT(col_ndx <= m_opposite_table.size());
    if (col_ndx == m_opposite_table.size()) {
        // m_opposite_table and m_opposite_column are always resized together!
        m_opposite_table.insert(col_ndx, TableKey().value);
        m_opposite_column.insert(col_ndx, ColKey().value);
    }
    else {
        m_opposite_table.set(col_ndx, TableKey().value);
        m_opposite_column.set(col_ndx, ColKey().value);
    }
    refresh_index_accessors();
    m_clusters.insert_column(col_key);
    if (m_tombstones) {
        m_tombstones->insert_column(col_key);
    }

    bump_storage_version();

    return col_key;
}


void Table::do_erase_root_column(ColKey col_key)
{
    size_t col_ndx = col_key.get_index().val;
    // If the column had a source index we have to remove and destroy that as well
    ref_type index_ref = m_index_refs.get_as_ref(col_ndx);
    if (index_ref) {
        Array::destroy_deep(index_ref, m_index_refs.get_alloc());
        m_index_refs.set(col_ndx, 0);
        m_index_accessors[col_ndx].reset();
    }
    m_opposite_table.set(col_ndx, TableKey().value);
    m_opposite_column.set(col_ndx, ColKey().value);
    m_index_accessors[col_ndx] = nullptr;
    m_clusters.remove_column(col_key);
    if (m_tombstones)
        m_tombstones->remove_column(col_key);
    size_t spec_ndx = colkey2spec_ndx(col_key);
    m_spec.erase_column(spec_ndx);
    m_top.adjust(top_position_for_column_key, 2);

    build_column_mapping();
    while (m_index_accessors.size() > m_leaf_ndx2colkey.size()) {
        REALM_ASSERT(m_index_accessors.back() == nullptr);
        m_index_accessors.pop_back();
    }
    bump_content_version();
    bump_storage_version();
}

Query Table::where(const DictionaryLinkValues& dictionary_of_links) const
{
    return Query(m_own_ref, dictionary_of_links);
}

void Table::set_table_type(Type table_type)
{
    if (table_type == m_table_type) {
        return;
    }

    if (m_table_type == Type::TopLevelAsymmetric || table_type == Type::TopLevelAsymmetric) {
        throw std::logic_error(util::format("Cannot change '%1' to/from asymmetric.", get_name()));
    }

    REALM_ASSERT_EX(table_type == Type::TopLevel || table_type == Type::Embedded, table_type);
    set_embedded(table_type == Type::Embedded);
}

void Table::set_embedded(bool embedded)
{
    if (Replication* repl = get_repl()) {
        if (repl->get_history_type() == Replication::HistoryType::hist_SyncClient) {
            throw std::logic_error(util::format("Cannot change '%1' to embedded when using Sync.", get_name()));
        }
    }

    if (embedded == false) {
        do_set_table_type(Type::TopLevel);
        return;
    }

    // Embedded objects cannot have a primary key.
    if (get_primary_key_column()) {
        throw std::logic_error(util::format("Cannot change '%1' to embedded when using a primary key.", get_name()));
    }

    // `has_backlink_columns` indicates if the table is embedded in any other table.
    bool has_backlink_columns = false;
    for_each_backlink_column([&has_backlink_columns](ColKey) {
        has_backlink_columns = true;
        return true;
    });
    if (!has_backlink_columns) {
        throw std::logic_error(
            util::format("Cannot change '%1' to embedded without backlink columns. Objects must be embedded in "
                         "at least one other class.",
                         get_name()));
    }
    else if (size() > 0) {
        for (auto object : *this) {
            size_t backlink_count = object.get_backlink_count();
            if (backlink_count == 0) {
                throw std::logic_error(util::format(
                    "At least one object in '%1' does not have a backlink (data would get lost).", get_name()));
            }
            else if (backlink_count > 1) {
                throw std::logic_error(
                    util::format("At least one object in '%1' does have multiple backlinks.", get_name()));
            }
        }
    }

    do_set_table_type(Type::Embedded);
}

void Table::do_set_table_type(Type table_type)
{
    while (m_top.size() <= top_position_for_flags)
        m_top.add(0);

    uint64_t flags = m_top.get_as_ref_or_tagged(top_position_for_flags).get_as_int();
    // reset bits 0-1
    flags &= ~table_type_mask;
    // set table type
    flags |= static_cast<uint8_t>(table_type);
    m_top.set(top_position_for_flags, RefOrTagged::make_tagged(flags));
    m_table_type = table_type;
}


void Table::detach(LifeCycleCookie cookie) noexcept
{
    m_cookie = cookie;
    m_alloc.bump_instance_version();
}

void Table::fully_detach() noexcept
{
    m_spec.detach();
    m_top.detach();
    m_index_refs.detach();
    m_opposite_table.detach();
    m_opposite_column.detach();
    m_index_accessors.clear();
}


Table::~Table() noexcept
{
    if (m_top.is_attached()) {
        // If destroyed as a standalone table, destroy all memory allocated
        if (m_top.get_parent() == nullptr) {
            m_top.destroy_deep();
        }
        fully_detach();
    }
    else {
        REALM_ASSERT(m_index_accessors.size() == 0);
    }
    m_cookie = cookie_deleted;
}


bool Table::has_search_index(ColKey col_key) const noexcept
{
    return m_index_accessors[col_key.get_index().val] != nullptr;
}

void Table::migrate_column_info()
{
    bool changes = false;
    TableKey tk = (get_name() == "pk") ? TableKey(0) : get_key();
    changes |= m_spec.convert_column_attributes();
    changes |= m_spec.convert_column_keys(tk);

    if (changes) {
        build_column_mapping();
    }
}

bool Table::verify_column_keys()
{
    size_t nb_public_columns = m_spec.get_public_column_count();
    size_t nb_columns = m_spec.get_column_count();
    bool modified = false;

    auto check = [&]() {
        for (size_t spec_ndx = nb_public_columns; spec_ndx < nb_columns; spec_ndx++) {
            if (m_spec.get_column_type(spec_ndx) == col_type_BackLink) {
                auto col_key = m_spec.get_key(spec_ndx);
                // This function checks for a specific error in the m_keys array where the
                // backlink column keys are wrong. It can be detected by trying to find the
                // corresponding origin table. If the error exists some of the results will
                // give null TableKeys back.
                if (!get_opposite_table_key(col_key))
                    return false;
                auto t = get_opposite_table(col_key);
                auto c = get_opposite_column(col_key);
                if (!t->valid_column(c))
                    return false;
                if (t->get_opposite_column(c) != col_key) {
                    t->set_opposite_column(c, get_key(), col_key);
                }
            }
        }
        return true;
    };

    if (!check()) {
        m_spec.fix_column_keys(get_key());
        build_column_mapping();
        refresh_index_accessors();
        REALM_ASSERT_RELEASE(check());
        modified = true;
    }
    return modified;
}

// Delete the indexes stored in the columns array and create corresponding
// entries in m_index_accessors array. This also has the effect that the columns
// array after this step does not have extra entries for certain columns
void Table::migrate_indexes(ColKey pk_col_key)
{
    if (ref_type top_ref = m_top.get_as_ref(top_position_for_columns)) {
        Array col_refs(m_alloc);
        col_refs.set_parent(&m_top, top_position_for_columns);
        col_refs.init_from_ref(top_ref);
        auto col_count = m_spec.get_column_count();
        size_t col_ndx = 0;

        // If col_refs.size() equals col_count, there are no indexes to migrate
        while (col_ndx < col_count && col_refs.size() > col_count) {
            if (m_spec.get_column_attr(col_ndx).test(col_attr_Indexed) && !m_index_refs.get(col_ndx)) {
                // Simply delete entry. This will have the effect that we will not have to take
                // extra entries into account
                auto old_index_ref = to_ref(col_refs.get(col_ndx + 1));
                col_refs.erase(col_ndx + 1);
                if (old_index_ref) {
                    // It should not be possible for old_index_ref to be 0, but we have seen some error
                    // reports on freeing a null ref, so just to be sure ...
                    Array::destroy_deep(old_index_ref, m_alloc);
                }

                // Primary key columns does not need an index
                if (m_leaf_ndx2colkey[col_ndx] != pk_col_key) {
                    // Otherwise create new index. Will be updated when objects are created
                    m_index_accessors[col_ndx] = std::make_unique<StringIndex>(
                        ClusterColumn(&m_clusters, m_spec.get_key(col_ndx)), get_alloc()); // Throws
                    auto index = m_index_accessors[col_ndx].get();
                    index->set_parent(&m_index_refs, col_ndx);
                    m_index_refs.set(col_ndx, index->get_ref());
                }
            }
            col_ndx++;
        };
    }
}

// Move information held in the subspec area into the structures managed by Table
// This is information about origin/target tables in relation to links
// This information is now held in "opposite" arrays directly in Table structure
// At the same time the backlink columns are destroyed
// If there is no subspec, this stage is done
void Table::migrate_subspec()
{
    if (!m_spec.has_subspec())
        return;

    ref_type top_ref = m_top.get_as_ref(top_position_for_columns);
    Array col_refs(m_alloc);
    col_refs.set_parent(&m_top, top_position_for_columns);
    col_refs.init_from_ref(top_ref);
    Group* group = get_parent_group();

    for (size_t col_ndx = 0; col_ndx < m_spec.get_column_count(); col_ndx++) {
        ColumnType col_type = m_spec.get_column_type(col_ndx);

        if (is_link_type(col_type)) {
            auto target_key = m_spec.get_opposite_link_table_key(col_ndx);
            auto target_table = group->get_table(target_key);
            Spec& target_spec = _impl::TableFriend::get_spec(*target_table);
            // The target table spec may already be migrated.
            // If it has, the new functions should be used.
            ColKey backlink_col_key = target_spec.has_subspec()
                                          ? target_spec.find_backlink_column(m_key, col_ndx)
                                          : target_table->find_opposite_column(m_spec.get_key(col_ndx));
            REALM_ASSERT(backlink_col_key.get_type() == col_type_BackLink);
            if (m_opposite_table.get(col_ndx) != target_key.value) {
                m_opposite_table.set(col_ndx, target_key.value);
            }
            if (m_opposite_column.get(col_ndx) != backlink_col_key.value) {
                m_opposite_column.set(col_ndx, backlink_col_key.value);
            }
        }
        else if (col_type == col_type_BackLink) {
            auto origin_key = m_spec.get_opposite_link_table_key(col_ndx);
            size_t origin_col_ndx = m_spec.get_origin_column_ndx(col_ndx);
            auto origin_table = group->get_table(origin_key);
            Spec& origin_spec = _impl::TableFriend::get_spec(*origin_table);
            ColKey origin_col_key = origin_spec.get_key(origin_col_ndx);
            REALM_ASSERT(is_link_type(origin_col_key.get_type()));
            if (m_opposite_table.get(col_ndx) != origin_key.value) {
                m_opposite_table.set(col_ndx, origin_key.value);
            }
            if (m_opposite_column.get(col_ndx) != origin_col_key.value) {
                m_opposite_column.set(col_ndx, origin_col_key.value);
            }
        }
    };
    m_spec.destroy_subspec();
}

namespace {

class LegacyStringColumn : public BPlusTree<StringData> {
public:
    LegacyStringColumn(Allocator& alloc, Spec* spec, size_t col_ndx, bool nullable)
        : BPlusTree(alloc)
        , m_spec(spec)
        , m_col_ndx(col_ndx)
        , m_nullable(nullable)
    {
    }

    std::unique_ptr<BPlusTreeLeaf> init_leaf_node(ref_type ref) override
    {
        auto leaf = std::make_unique<LeafNode>(this);
        leaf->ArrayString::set_spec(m_spec, m_col_ndx);
        leaf->set_nullability(m_nullable);
        leaf->init_from_ref(ref);
        return leaf;
    }

    StringData get_legacy(size_t n) const
    {
        if (m_cached_leaf_begin <= n && n < m_cached_leaf_end) {
            return m_leaf_cache.get_legacy(n - m_cached_leaf_begin);
        }
        else {
            StringData value;

            auto func = [&value](BPlusTreeNode* node, size_t ndx) {
                auto leaf = static_cast<LeafNode*>(node);
                value = leaf->get_legacy(ndx);
            };

            m_root->bptree_access(n, func);

            return value;
        }
    }

private:
    Spec* m_spec;
    size_t m_col_ndx;
    bool m_nullable;
};

// We need an accessor that can read old Timestamp columns.
// The new BPlusTree<Timestamp> uses a different layout
class LegacyTS : private Array {
public:
    explicit LegacyTS(Allocator& allocator)
        : Array(allocator)
        , m_seconds(allocator)
        , m_nanoseconds(allocator)
    {
        m_seconds.set_parent(this, 0);
        m_nanoseconds.set_parent(this, 1);
    }

    using Array::set_parent;

    void init_from_parent()
    {
        Array::init_from_parent();
        m_seconds.init_from_parent();
        m_nanoseconds.init_from_parent();
    }

    size_t size() const
    {
        return m_seconds.size();
    }

    Timestamp get(size_t ndx) const
    {
        util::Optional<int64_t> seconds = m_seconds.get(ndx);
        return seconds ? Timestamp(*seconds, int32_t(m_nanoseconds.get(ndx))) : Timestamp{};
    }

private:
    BPlusTree<util::Optional<Int>> m_seconds;
    BPlusTree<Int> m_nanoseconds;
};

// Function that can retrieve a single value from the old columns
Mixed get_val_from_column(size_t ndx, ColumnType col_type, bool nullable, BPlusTreeBase* accessor)
{
    switch (col_type) {
        case col_type_Int:
            if (nullable) {
                auto val = static_cast<BPlusTree<util::Optional<Int>>*>(accessor)->get(ndx);
                return Mixed{val};
            }
            else {
                return Mixed{static_cast<BPlusTree<Int>*>(accessor)->get(ndx)};
            }
        case col_type_Bool:
            if (nullable) {
                auto val = static_cast<BPlusTree<util::Optional<Int>>*>(accessor)->get(ndx);
                return val ? Mixed{bool(*val)} : Mixed{};
            }
            else {
                return Mixed{bool(static_cast<BPlusTree<Int>*>(accessor)->get(ndx))};
            }
        case col_type_Float:
            return Mixed{static_cast<BPlusTree<float>*>(accessor)->get(ndx)};
        case col_type_Double:
            return Mixed{static_cast<BPlusTree<double>*>(accessor)->get(ndx)};
        case col_type_String: {
            auto str = static_cast<LegacyStringColumn*>(accessor)->get_legacy(ndx);
            // This is a workaround for a bug where the length could be -1
            // Seen when upgrading very old file.
            if (str.size() == size_t(-1)) {
                return Mixed("");
            }
            return Mixed{str};
        }
        case col_type_Binary:
            return Mixed{static_cast<BPlusTree<Binary>*>(accessor)->get(ndx)};
        default:
            REALM_UNREACHABLE();
    }
}

template <class T>
void copy_list(ref_type sub_table_ref, Obj& obj, ColKey col, Allocator& alloc)
{
    if (sub_table_ref) {
        // Actual list is in the columns array position 0
        Array cols(alloc);
        cols.init_from_ref(sub_table_ref);
        ref_type list_ref = cols.get_as_ref(0);
        BPlusTree<T> from_list(alloc);
        from_list.init_from_ref(list_ref);
        size_t list_size = from_list.size();
        auto l = obj.get_list<T>(col);
        for (size_t j = 0; j < list_size; j++) {
            l.add(from_list.get(j));
        }
    }
}

template <>
void copy_list<util::Optional<Bool>>(ref_type sub_table_ref, Obj& obj, ColKey col, Allocator& alloc)
{
    if (sub_table_ref) {
        // Actual list is in the columns array position 0
        Array cols(alloc);
        cols.init_from_ref(sub_table_ref);
        BPlusTree<util::Optional<Int>> from_list(alloc);
        from_list.set_parent(&cols, 0);
        from_list.init_from_parent();
        size_t list_size = from_list.size();
        auto l = obj.get_list<util::Optional<Bool>>(col);
        for (size_t j = 0; j < list_size; j++) {
            util::Optional<Bool> val;
            auto int_val = from_list.get(j);
            if (int_val) {
                val = (*int_val != 0);
            }
            l.add(val);
        }
    }
}

template <>
void copy_list<String>(ref_type sub_table_ref, Obj& obj, ColKey col, Allocator& alloc)
{
    if (sub_table_ref) {
        // Actual list is in the columns array position 0
        bool nullable = col.get_attrs().test(col_attr_Nullable);
        Array cols(alloc);
        cols.init_from_ref(sub_table_ref);
        LegacyStringColumn from_list(alloc, nullptr, 0, nullable); // List of strings cannot be enumerated
        from_list.set_parent(&cols, 0);
        from_list.init_from_parent();
        size_t list_size = from_list.size();
        auto l = obj.get_list<String>(col);
        for (size_t j = 0; j < list_size; j++) {
            l.add(from_list.get_legacy(j));
        }
    }
}

template <>
void copy_list<Timestamp>(ref_type sub_table_ref, Obj& obj, ColKey col, Allocator& alloc)
{
    if (sub_table_ref) {
        // Actual list is in the columns array position 0
        Array cols(alloc);
        cols.init_from_ref(sub_table_ref);
        LegacyTS from_list(alloc);
        from_list.set_parent(&cols, 0);
        from_list.init_from_parent();
        size_t list_size = from_list.size();
        auto l = obj.get_list<Timestamp>(col);
        for (size_t j = 0; j < list_size; j++) {
            l.add(from_list.get(j));
        }
    }
}

} // namespace

void Table::create_columns()
{
    size_t cnt;
    auto get_column_cnt = [&cnt](const Cluster* cluster) {
        cnt = cluster->nb_columns();
        return true;
    };
    traverse_clusters(get_column_cnt);

    size_t column_count = m_spec.get_column_count();
    if (cnt != column_count) {
        for (size_t col_ndx = 0; col_ndx < column_count; col_ndx++) {
            m_clusters.insert_column(m_spec.get_key(col_ndx));
        }
    }
}

bool Table::migrate_objects()
{
    size_t nb_public_columns = m_spec.get_public_column_count();
    size_t nb_columns = m_spec.get_column_count();
    if (!nb_columns) {
        // No columns - this means no objects
        return true;
    }

    ref_type top_ref = m_top.get_as_ref(top_position_for_columns);
    if (!top_ref) {
        // Has already been done
        return true;
    }
    Array col_refs(m_alloc);
    col_refs.set_parent(&m_top, top_position_for_columns);
    col_refs.init_from_ref(top_ref);

    /************************ Create column accessors ************************/

    std::map<ColKey, std::unique_ptr<BPlusTreeBase>> column_accessors;
    std::map<ColKey, std::unique_ptr<LegacyTS>> ts_accessors;
    std::map<ColKey, std::unique_ptr<BPlusTree<int64_t>>> list_accessors;
    std::vector<size_t> cols_to_destroy;
    bool has_link_columns = false;

    // helper function to determine the number of objects in the table
    size_t number_of_objects = (nb_columns == 0) ? 0 : size_t(-1);
    auto update_size = [&number_of_objects](size_t s) {
        if (number_of_objects == size_t(-1)) {
            number_of_objects = s;
        }
        else {
            REALM_ASSERT(s == number_of_objects);
        }
    };

    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        if (col_ndx < nb_public_columns && m_spec.get_column_name(col_ndx) == "!ROW_INDEX") {
            // If this column has been added, we can break here
            break;
        }

        ColKey col_key = m_spec.get_key(col_ndx);
        ColumnAttrMask attr = m_spec.get_column_attr(col_ndx);
        ColumnType col_type = m_spec.get_column_type(col_ndx);
        bool nullable = attr.test(col_attr_Nullable);
        std::unique_ptr<BPlusTreeBase> acc;
        std::unique_ptr<LegacyTS> ts_acc;
        std::unique_ptr<BPlusTree<int64_t>> list_acc;

        if (!(col_ndx < col_refs.size())) {
            throw std::runtime_error(
                util::format("Objects in '%1' corrupted by previous upgrade attempt", get_name()));
        }

        if (!col_refs.get(col_ndx)) {
            // This column has been migrated
            continue;
        }

        if (attr.test(col_attr_List) && col_type != col_type_LinkList) {
            list_acc = std::make_unique<BPlusTree<int64_t>>(m_alloc);
        }
        else {
            switch (col_type) {
                case col_type_Int:
                case col_type_Bool:
                    if (nullable) {
                        acc = std::make_unique<BPlusTree<util::Optional<Int>>>(m_alloc);
                    }
                    else {
                        acc = std::make_unique<BPlusTree<Int>>(m_alloc);
                    }
                    break;
                case col_type_Float:
                    acc = std::make_unique<BPlusTree<float>>(m_alloc);
                    break;
                case col_type_Double:
                    acc = std::make_unique<BPlusTree<double>>(m_alloc);
                    break;
                case col_type_String:
                    acc = std::make_unique<LegacyStringColumn>(m_alloc, &m_spec, col_ndx, nullable);
                    break;
                case col_type_Binary:
                    acc = std::make_unique<BPlusTree<Binary>>(m_alloc);
                    break;
                case col_type_Timestamp: {
                    ts_acc = std::make_unique<LegacyTS>(m_alloc);
                    break;
                }
                case col_type_Link:
                case col_type_LinkList: {
                    BPlusTree<int64_t> arr(m_alloc);
                    arr.set_parent(&col_refs, col_ndx);
                    arr.init_from_parent();
                    update_size(arr.size());
                    has_link_columns = true;
                    break;
                }
                case col_type_BackLink: {
                    BPlusTree<int64_t> arr(m_alloc);
                    arr.set_parent(&col_refs, col_ndx);
                    arr.init_from_parent();
                    update_size(arr.size());
                    cols_to_destroy.push_back(col_ndx);
                    break;
                }
                default:
                    break;
            }
        }

        if (acc) {
            acc->set_parent(&col_refs, col_ndx);
            acc->init_from_parent();
            update_size(acc->size());
            column_accessors.emplace(col_key, std::move(acc));
            cols_to_destroy.push_back(col_ndx);
        }
        if (ts_acc) {
            ts_acc->set_parent(&col_refs, col_ndx);
            ts_acc->init_from_parent();
            update_size(ts_acc->size());
            ts_accessors.emplace(col_key, std::move(ts_acc));
            cols_to_destroy.push_back(col_ndx);
        }
        if (list_acc) {
            list_acc->set_parent(&col_refs, col_ndx);
            list_acc->init_from_parent();
            update_size(list_acc->size());
            list_accessors.emplace(col_key, std::move(list_acc));
            cols_to_destroy.push_back(col_ndx);
        }
    }

    REALM_ASSERT(number_of_objects != size_t(-1));

    if (m_clusters.size() == number_of_objects) {
        // We have migrated all objects
        return !has_link_columns;
    }

    // !OID column must not be present. Such columns are only present in syncked
    // realms, which we cannot upgrade.
    REALM_ASSERT(nb_public_columns == 0 || m_spec.get_column_name(0) != "!OID");

    /*************************** Create objects ******************************/

    for (size_t row_ndx = 0; row_ndx < number_of_objects; row_ndx++) {
        // Build a vector of values obtained from the old columns
        FieldValues init_values;
        for (auto& it : column_accessors) {
            auto col_key = it.first;
            auto col_type = col_key.get_type();
            auto nullable = col_key.get_attrs().test(col_attr_Nullable);
            auto val = get_val_from_column(row_ndx, col_type, nullable, it.second.get());
            init_values.insert(col_key, val);
        }
        for (auto& it : ts_accessors) {
            init_values.insert(it.first, Mixed(it.second->get(row_ndx)));
        }

        // Create object with the initial values
        Obj obj = m_clusters.insert(ObjKey(row_ndx), init_values);

        // Then update possible list types
        for (auto& it : list_accessors) {
            switch (it.first.get_type()) {
                case col_type_Int: {
                    if (it.first.get_attrs().test(col_attr_Nullable)) {
                        copy_list<util::Optional<int64_t>>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    }
                    else {
                        copy_list<int64_t>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    }
                    break;
                }
                case col_type_Bool:
                    if (it.first.get_attrs().test(col_attr_Nullable)) {
                        copy_list<util::Optional<Bool>>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    }
                    else {
                        copy_list<Bool>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    }
                    break;
                case col_type_Float:
                    copy_list<float>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    break;
                case col_type_Double:
                    copy_list<double>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    break;
                case col_type_String:
                    copy_list<String>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    break;
                case col_type_Binary:
                    copy_list<Binary>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    break;
                case col_type_Timestamp: {
                    copy_list<Timestamp>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
                    break;
                }
                default:
                    break;
            }
        }
    }

    // Destroy values in the old columns that has been copied.
    // This frees up space in the file
    for (auto ndx : cols_to_destroy) {
        Array::destroy_deep(to_ref(col_refs.get(ndx)), m_alloc);
        col_refs.set(ndx, 0);
    }

    // We need to be sure that the stored 'next sequence number' is bigger than
    // the biggest ObjKey currently used.
    this->set_sequence_number(uint64_t(number_of_objects));

#if 0
    if (fastrand(100) < 20) {
        throw util::runtime_error("Upgrade interrupted");
    }
#endif
    return !has_link_columns;
}

void Table::migrate_links()
{
    ref_type top_ref = m_top.get_as_ref(top_position_for_columns);
    if (!top_ref) {
        // All objects migrated
        return;
    }

    Array col_refs(m_alloc);
    col_refs.set_parent(&m_top, top_position_for_columns);
    col_refs.init_from_ref(top_ref);

    // Cache column accessors and other information
    size_t nb_columns = m_spec.get_public_column_count();
    std::vector<std::unique_ptr<BPlusTree<Int>>> link_column_accessors(nb_columns);
    std::vector<ColKey> col_keys(nb_columns);
    std::vector<ColumnType> col_types(nb_columns);
    std::vector<Table*> target_tables(nb_columns);
    std::vector<ColKey> opposite_orig_row_ndx_col(nb_columns);
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        ColumnType col_type = m_spec.get_column_type(col_ndx);

        if (is_link_type(col_type)) {
            link_column_accessors[col_ndx] = std::make_unique<BPlusTree<int64_t>>(m_alloc);
            link_column_accessors[col_ndx]->set_parent(&col_refs, col_ndx);
            link_column_accessors[col_ndx]->init_from_parent();
            col_keys[col_ndx] = m_spec.get_key(col_ndx);
            col_types[col_ndx] = col_type;
            target_tables[col_ndx] = get_opposite_table(col_keys[col_ndx]).unchecked_ptr();
            opposite_orig_row_ndx_col[col_ndx] = target_tables[col_ndx]->get_column_key("!ROW_INDEX");
        }
    }

    auto orig_row_ndx_col_key = get_column_key("!ROW_INDEX");
    for (auto obj : *this) {
        for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
            if (col_keys[col_ndx]) {
                // If no !ROW_INDEX column is found, the original row index number is
                // equal to the ObjKey value
                size_t orig_row_ndx =
                    size_t(orig_row_ndx_col_key ? obj.get<Int>(orig_row_ndx_col_key) : obj.get_key().value);
                // Get original link value
                int64_t link_val = link_column_accessors[col_ndx]->get(orig_row_ndx);

                Table* target_table = target_tables[col_ndx];
                ColKey search_col = opposite_orig_row_ndx_col[col_ndx];
                auto get_target_key = [target_table, search_col](int64_t orig_link_val) -> ObjKey {
                    if (search_col)
                        return target_table->find_first_int(search_col, orig_link_val);
                    else
                        return ObjKey(orig_link_val);
                };

                if (link_val) {
                    if (col_types[col_ndx] == col_type_Link) {
                        obj.set(col_keys[col_ndx], get_target_key(link_val - 1));
                    }
                    else {
                        auto ll = obj.get_linklist(col_keys[col_ndx]);
                        BPlusTree<Int> links(m_alloc);
                        links.init_from_ref(ref_type(link_val));
                        size_t nb_links = links.size();
                        for (size_t j = 0; j < nb_links; j++) {
                            ll.add(get_target_key(links.get(j)));
                        }
                    }
                }
            }
        }
    }
}

void Table::finalize_migration(ColKey pk_col_key)
{
    if (ref_type ref = m_top.get_as_ref(top_position_for_columns)) {
        Array::destroy_deep(ref, m_alloc);
        m_top.set(top_position_for_columns, 0);
    }

    if (auto orig_row_ndx_col = get_column_key("!ROW_INDEX")) {
        remove_column(orig_row_ndx_col);
    }

    if (auto oid_col = get_column_key("!OID")) {
        remove_column(oid_col);
    }

    REALM_ASSERT_RELEASE(!pk_col_key || valid_column(pk_col_key));
    do_set_primary_key_column(pk_col_key);
}

StringData Table::get_name() const noexcept
{
    const Array& real_top = m_top;
    ArrayParent* parent = real_top.get_parent();
    if (!parent)
        return StringData("");
    REALM_ASSERT(dynamic_cast<Group*>(parent));
    return static_cast<Group*>(parent)->get_table_name(get_key());
}

const char* Table::get_state() const noexcept
{
    switch (m_cookie) {
        case cookie_created:
            return "created";
        case cookie_transaction_ended:
            return "transaction_ended";
        case cookie_initialized:
            return "initialised";
        case cookie_removed:
            return "removed";
        case cookie_void:
            return "void";
        case cookie_deleted:
            return "deleted";
    }
    return "";
}


bool Table::is_nullable(ColKey col_key) const
{
    REALM_ASSERT_DEBUG(valid_column(col_key));
    return col_key.get_attrs().test(col_attr_Nullable);
}

bool Table::is_list(ColKey col_key) const
{
    REALM_ASSERT_DEBUG(valid_column(col_key));
    return col_key.get_attrs().test(col_attr_List);
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
    top.add(0); // Old position for columns
    {
        MemRef mem = Cluster::create_empty_cluster(alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        top.add(v); // Throws
        dg_2.release();
    }

    // Table key value
    RefOrTagged rot = RefOrTagged::make_tagged(key.value);
    top.add(rot);

    // Search indexes
    {
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_HasRefs, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        top.add(v); // Throws
        dg_2.release();
    }
    rot = RefOrTagged::make_tagged(0);
    top.add(rot); // Column key
    top.add(rot); // Version
    dg.release();
    // Opposite keys (table and column)
    {
        bool context_flag = false;
        {
            MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
            dg_2.reset(mem.get_ref());
            int_fast64_t v(from_ref(mem.get_ref()));
            top.add(v); // Throws
            dg_2.release();
        }
        {
            MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
            dg_2.reset(mem.get_ref());
            int_fast64_t v(from_ref(mem.get_ref()));
            top.add(v); // Throws
            dg_2.release();
        }
    }
    top.add(0); // Sequence number
    top.add(0); // Collision_map
    top.add(0); // pk col key
    top.add(0); // flags
    top.add(0); // tombstones

    REALM_ASSERT(top.size() == top_array_size);

    return top.get_ref();
}

void Table::ensure_graveyard()
{
    if (!m_tombstones) {
        while (m_top.size() < top_position_for_tombstones)
            m_top.add(0);
        REALM_ASSERT(!m_top.get(top_position_for_tombstones));
        MemRef mem = Cluster::create_empty_cluster(m_alloc);
        m_top.set_as_ref(top_position_for_tombstones, mem.get_ref());
        m_tombstones = std::make_unique<TableClusterTree>(this, m_alloc, size_t(top_position_for_tombstones));
        m_tombstones->init_from_parent();
        for_each_and_every_column([ts = m_tombstones.get()](ColKey col) {
            ts->insert_column(col);
            return false;
        });
    }
}

void Table::batch_erase_rows(const KeyColumn& keys)
{
    Group* g = get_parent_group();

    size_t num_objs = keys.size();
    std::vector<ObjKey> vec;
    vec.reserve(num_objs);
    for (size_t i = 0; i < num_objs; ++i) {
        ObjKey key = keys.get(i);
        if (key != null_key && is_valid(key)) {
            vec.push_back(key);
        }
    }
    sort(vec.begin(), vec.end());
    vec.erase(unique(vec.begin(), vec.end()), vec.end());

    if (has_any_embedded_objects() || (g && g->has_cascade_notification_handler())) {
        CascadeState state(CascadeState::Mode::Strong, g);
        std::for_each(vec.begin(), vec.end(), [this, &state](ObjKey k) {
            state.m_to_be_deleted.emplace_back(m_key, k);
        });
        nullify_links(state);
        remove_recursive(state);
    }
    else {
        CascadeState state(CascadeState::Mode::None, g);
        for (auto k : vec) {
            if (g) {
                m_clusters.nullify_links(k, state);
            }
            m_clusters.erase(k, state);
        }
    }
}


void Table::clear()
{
    CascadeState state(CascadeState::Mode::Strong, get_parent_group());
    m_clusters.clear(state);
    free_collision_table();
}


Group* Table::get_parent_group() const noexcept
{
    if (!m_top.is_attached())
        return 0;                             // Subtable with shared descriptor
    ArrayParent* parent = m_top.get_parent(); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return 0; // Free-standing table

    return static_cast<Group*>(parent);
}

inline uint64_t Table::get_sync_file_id() const noexcept
{
    Group* g = get_parent_group();
    return g ? g->get_sync_file_id() : 0;
}

size_t Table::get_index_in_group() const noexcept
{
    if (!m_top.is_attached())
        return realm::npos;                   // Subtable with shared descriptor
    ArrayParent* parent = m_top.get_parent(); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return realm::npos; // Free-standing table
    return m_top.get_ndx_in_parent();
}

uint64_t Table::allocate_sequence_number()
{
    RefOrTagged rot = m_top.get_as_ref_or_tagged(top_position_for_sequence_number);
    uint64_t sn = rot.is_tagged() ? rot.get_as_int() : 0;
    rot = RefOrTagged::make_tagged(sn + 1);
    m_top.set(top_position_for_sequence_number, rot);

    return sn;
}

void Table::set_sequence_number(uint64_t seq)
{
    m_top.set(top_position_for_sequence_number, RefOrTagged::make_tagged(seq));
}

void Table::set_collision_map(ref_type ref)
{
    m_top.set(top_position_for_collision_map, RefOrTagged::make_ref(ref));
}

TableRef Table::get_link_target(ColKey col_key) noexcept
{
    return get_opposite_table(col_key);
}

// count ----------------------------------------------

size_t Table::count_int(ColKey col_key, int64_t value) const
{
    if (auto index = this->get_search_index(col_key)) {
        return index->count(value);
    }

    return where().equal(col_key, value).count();
}
size_t Table::count_float(ColKey col_key, float value) const
{
    return where().equal(col_key, value).count();
}
size_t Table::count_double(ColKey col_key, double value) const
{
    return where().equal(col_key, value).count();
}
size_t Table::count_decimal(ColKey col_key, Decimal128 value) const
{
    ArrayDecimal128 leaf(get_alloc());
    size_t cnt = 0;
    bool null_value = value.is_null();
    auto f = [value, &leaf, col_key, null_value, &cnt](const Cluster* cluster) {
        // direct aggregate on the leaf
        cluster->init_leaf(col_key, &leaf);
        auto sz = leaf.size();
        for (size_t i = 0; i < sz; i++) {
            if ((null_value && leaf.is_null(i)) || (leaf.get(i) == value)) {
                cnt++;
            }
        }
        return false;
    };

    traverse_clusters(f);

    return cnt;
}
size_t Table::count_string(ColKey col_key, StringData value) const
{
    if (auto index = this->get_search_index(col_key)) {
        return index->count(value);
    }
    return where().equal(col_key, value).count();
}

// sum ----------------------------------------------


int64_t Table::sum_int(ColKey col_key) const
{
    QueryStateSum<int64_t> st;
    if (is_nullable(col_key)) {
        aggregate<util::Optional<int64_t>>(st, col_key);
    }
    else {
        aggregate<int64_t>(st, col_key);
    }
    return st.result_sum();
}
double Table::sum_float(ColKey col_key) const
{
    QueryStateSum<float> st;
    aggregate<float>(st, col_key);
    return st.result_sum();
}
double Table::sum_double(ColKey col_key) const
{
    QueryStateSum<double> st;
    aggregate<double>(st, col_key);
    return st.result_sum();
}
Decimal128 Table::sum_decimal(ColKey col_key) const
{
    QueryStateSum<Decimal128> st;
    aggregate<Decimal128>(st, col_key);
    return st.result_sum();
}
Decimal128 Table::sum_mixed(ColKey col_key) const
{
    QueryStateSum<Mixed> st;
    aggregate<Mixed>(st, col_key);
    return st.result_sum();
}

// average ----------------------------------------------

double Table::average_int(ColKey col_key, size_t* value_count) const
{
    if (is_nullable(col_key)) {
        return average<util::Optional<int64_t>>(col_key, value_count);
    }
    return average<int64_t>(col_key, value_count);
}
double Table::average_float(ColKey col_key, size_t* value_count) const
{
    return average<float>(col_key, value_count);
}
double Table::average_double(ColKey col_key, size_t* value_count) const
{
    return average<double>(col_key, value_count);
}
Decimal128 Table::average_decimal(ColKey col_key, size_t* value_count) const
{
    QueryStateSum<Decimal128> st;
    aggregate<Decimal128>(st, col_key);
    auto sum = st.result_sum();
    Decimal128 avg(0);
    size_t items_counted = st.result_count();
    if (items_counted != 0)
        avg = sum / items_counted;
    if (value_count)
        *value_count = items_counted;
    return avg;
}

Decimal128 Table::average_mixed(ColKey col_key, size_t* value_count) const
{
    QueryStateSum<Mixed> st;
    aggregate<Mixed>(st, col_key);
    auto sum = st.result_sum();
    Decimal128 avg(0);
    size_t items_counted = st.result_count();
    if (items_counted != 0)
        avg = sum / items_counted;
    if (value_count)
        *value_count = items_counted;
    return avg;
}

// minimum ----------------------------------------------

#define USE_COLUMN_AGGREGATE 1

int64_t Table::minimum_int(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMin<int64_t> st;
    if (is_nullable(col_key)) {
        aggregate<util::Optional<int64_t>>(st, col_key);
    }
    else {
        aggregate<int64_t>(st, col_key);
    }
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_min();
}

float Table::minimum_float(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMin<float> st;
    aggregate<float>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_min();
}

double Table::minimum_double(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMin<double> st;
    aggregate<double>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_min();
}

Decimal128 Table::minimum_decimal(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMin<Decimal128> st;
    aggregate<Decimal128>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_min();
}

Timestamp Table::minimum_timestamp(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMin<Timestamp> st;
    aggregate<Timestamp>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_min();
}

Mixed Table::minimum_mixed(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMin<Mixed> st;
    aggregate<Mixed>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_min();
}


// maximum ----------------------------------------------

int64_t Table::maximum_int(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMax<int64_t> st;
    if (is_nullable(col_key)) {
        aggregate<util::Optional<int64_t>>(st, col_key);
    }
    else {
        aggregate<int64_t>(st, col_key);
    }
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_max();
}

float Table::maximum_float(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMax<float> st;
    aggregate<float>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_max();
}

double Table::maximum_double(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMax<double> st;
    aggregate<double>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_max();
}

Decimal128 Table::maximum_decimal(ColKey col_key, ObjKey* return_ndx) const
{
    ArrayDecimal128 leaf(get_alloc());
    Decimal128 max("-Inf");
    ObjKey ret_key;
    auto f = [&max, &ret_key, &leaf, col_key](const Cluster* cluster) {
        // direct aggregate on the leaf
        cluster->init_leaf(col_key, &leaf);
        auto sz = leaf.size();
        for (size_t i = 0; i < sz; i++) {
            auto val = leaf.get(i);
            if (!val.is_null() && val > max) {
                max = val;
                ret_key = cluster->get_real_key(i);
            }
        }
        return false;
    };

    traverse_clusters(f);
    if (return_ndx) {
        *return_ndx = ObjKey(ret_key);
    }
    return max;
}

Timestamp Table::maximum_timestamp(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMax<Timestamp> st;
    aggregate<Timestamp>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_max();
}

Mixed Table::maximum_mixed(ColKey col_key, ObjKey* return_ndx) const
{
    QueryStateMax<Mixed> st;
    aggregate<Mixed>(st, col_key);
    if (return_ndx) {
        *return_ndx = ObjKey(st.m_minmax_key);
    }
    return st.get_max();
}


template <class T>
ObjKey Table::find_first(ColKey col_key, T value) const
{
    check_column(col_key);

    if (!col_key.is_nullable() && value_is_null(value)) {
        return {}; // this is a precaution/optimization
    }
    // You cannot call GetIndexData on ObjKey
    if constexpr (!std::is_same_v<T, ObjKey>) {
        if (StringIndex* index = get_search_index(col_key)) {
            return index->find_first(value);
        }
        if (col_key == m_primary_key_col) {
            return find_primary_key(value);
        }
    }

    ObjKey key;
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    LeafType leaf(get_alloc());

    auto f = [&key, &col_key, &value, &leaf](const Cluster* cluster) {
        cluster->init_leaf(col_key, &leaf);
        size_t row = leaf.find_first(value, 0, cluster->node_size());
        if (row != realm::npos) {
            key = cluster->get_real_key(row);
            return true;
        }
        return false;
    };

    traverse_clusters(f);

    return key;
}

namespace realm {

template <>
ObjKey Table::find_first(ColKey col_key, util::Optional<float> value) const
{
    return value ? find_first(col_key, *value) : find_first_null(col_key);
}

template <>
ObjKey Table::find_first(ColKey col_key, util::Optional<double> value) const
{
    return value ? find_first(col_key, *value) : find_first_null(col_key);
}

template <>
ObjKey Table::find_first(ColKey col_key, null) const
{
    return find_first_null(col_key);
}
} // namespace realm

// Explicitly instantiate the generic case of the template for the types we care about.
template ObjKey Table::find_first(ColKey col_key, bool) const;
template ObjKey Table::find_first(ColKey col_key, int64_t) const;
template ObjKey Table::find_first(ColKey col_key, float) const;
template ObjKey Table::find_first(ColKey col_key, double) const;
template ObjKey Table::find_first(ColKey col_key, Decimal128) const;
template ObjKey Table::find_first(ColKey col_key, ObjectId) const;
template ObjKey Table::find_first(ColKey col_key, ObjKey) const;
template ObjKey Table::find_first(ColKey col_key, util::Optional<bool>) const;
template ObjKey Table::find_first(ColKey col_key, util::Optional<int64_t>) const;
template ObjKey Table::find_first(ColKey col_key, BinaryData) const;
template ObjKey Table::find_first(ColKey col_key, Mixed) const;
template ObjKey Table::find_first(ColKey col_key, UUID) const;
template ObjKey Table::find_first(ColKey col_key, util::Optional<ObjectId>) const;
template ObjKey Table::find_first(ColKey col_key, util::Optional<UUID>) const;

ObjKey Table::find_first_int(ColKey col_key, int64_t value) const
{
    if (is_nullable(col_key))
        return find_first<util::Optional<int64_t>>(col_key, value);
    else
        return find_first<int64_t>(col_key, value);
}

ObjKey Table::find_first_bool(ColKey col_key, bool value) const
{
    if (is_nullable(col_key))
        return find_first<util::Optional<bool>>(col_key, value);
    else
        return find_first<bool>(col_key, value);
}

ObjKey Table::find_first_timestamp(ColKey col_key, Timestamp value) const
{
    return find_first(col_key, value);
}

ObjKey Table::find_first_object_id(ColKey col_key, ObjectId value) const
{
    return find_first(col_key, value);
}

ObjKey Table::find_first_float(ColKey col_key, float value) const
{
    return find_first<Float>(col_key, value);
}

ObjKey Table::find_first_double(ColKey col_key, double value) const
{
    return find_first<Double>(col_key, value);
}

ObjKey Table::find_first_decimal(ColKey col_key, Decimal128 value) const
{
    return find_first<Decimal128>(col_key, value);
}

ObjKey Table::find_first_string(ColKey col_key, StringData value) const
{
    return find_first(col_key, value);
}

ObjKey Table::find_first_binary(ColKey col_key, BinaryData value) const
{
    return find_first<BinaryData>(col_key, value);
}

ObjKey Table::find_first_null(ColKey col_key) const
{
    return where().equal(col_key, null{}).find();
}

ObjKey Table::find_first_uuid(ColKey col_key, UUID value) const
{
    return find_first(col_key, value);
}

template <class T>
TableView Table::find_all(ColKey col_key, T value)
{
    return where().equal(col_key, value).find_all();
}

TableView Table::find_all_int(ColKey col_key, int64_t value)
{
    return find_all<int64_t>(col_key, value);
}

TableView Table::find_all_int(ColKey col_key, int64_t value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_key, value);
}

TableView Table::find_all_bool(ColKey col_key, bool value)
{
    return find_all<bool>(col_key, value);
}

TableView Table::find_all_bool(ColKey col_key, bool value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_key, value);
}


TableView Table::find_all_float(ColKey col_key, float value)
{
    return find_all<float>(col_key, value);
}

TableView Table::find_all_float(ColKey col_key, float value) const
{
    return const_cast<Table*>(this)->find_all<float>(col_key, value);
}

TableView Table::find_all_double(ColKey col_key, double value)
{
    return find_all<double>(col_key, value);
}

TableView Table::find_all_double(ColKey col_key, double value) const
{
    return const_cast<Table*>(this)->find_all<double>(col_key, value);
}

TableView Table::find_all_string(ColKey col_key, StringData value)
{
    return where().equal(col_key, value).find_all();
}

TableView Table::find_all_string(ColKey col_key, StringData value) const
{
    return const_cast<Table*>(this)->find_all_string(col_key, value);
}

TableView Table::find_all_binary(ColKey, BinaryData)
{
    throw util::runtime_error("Not implemented");
}

TableView Table::find_all_binary(ColKey col_key, BinaryData value) const
{
    return const_cast<Table*>(this)->find_all_binary(col_key, value);
}

TableView Table::find_all_null(ColKey col_key)
{
    return where().equal(col_key, null{}).find_all();
}

TableView Table::find_all_null(ColKey col_key) const
{
    return const_cast<Table*>(this)->find_all_null(col_key);
}

TableView Table::get_sorted_view(ColKey col_key, bool ascending)
{
    TableView tv = where().find_all();
    tv.sort(col_key, ascending);
    return tv;
}

TableView Table::get_sorted_view(ColKey col_key, bool ascending) const
{
    return const_cast<Table*>(this)->get_sorted_view(col_key, ascending);
}

TableView Table::get_sorted_view(SortDescriptor order)
{
    TableView tv = where().find_all();
    tv.sort(std::move(order));
    return tv;
}

TableView Table::get_sorted_view(SortDescriptor order) const
{
    return const_cast<Table*>(this)->get_sorted_view(std::move(order));
}


const Table* Table::get_link_chain_target(const std::vector<ColKey>& link_chain) const
{
    const Table* table = this;
    for (size_t t = 0; t < link_chain.size(); t++) {
        // Link column can be a single Link, LinkList, or BackLink.
        REALM_ASSERT(table->valid_column(link_chain[t]));
        ColumnType type = table->get_real_column_type(link_chain[t]);
        if (type == col_type_LinkList || type == col_type_Link || type == col_type_BackLink) {
            table = table->get_opposite_table(link_chain[t]).unchecked_ptr();
        }
        else {
            // Only last column in link chain is allowed to be non-link
            if (t + 1 != link_chain.size())
                throw(LogicError::type_mismatch);
        }
    }
    return table;
}

// Called after a commit. Table will effectively contain the same as before,
// but now with new refs from the file
void Table::update_from_parent() noexcept
{
    // There is no top for sub-tables sharing spec
    if (m_top.is_attached()) {
        m_top.update_from_parent();
        m_spec.update_from_parent();
        m_clusters.update_from_parent();
        m_index_refs.update_from_parent();
        for (auto&& index : m_index_accessors) {
            if (index != nullptr) {
                index->update_from_parent();
            }
        }

        m_opposite_table.update_from_parent();
        m_opposite_column.update_from_parent();
        if (m_top.size() > top_position_for_flags) {
            uint64_t flags = m_top.get_as_ref_or_tagged(top_position_for_flags).get_as_int();
            m_table_type = Type(flags & table_type_mask);
        }
        else {
            m_table_type = Type::TopLevel;
        }
        if (m_tombstones)
            m_tombstones->update_from_parent();

        refresh_content_version();
        m_has_any_embedded_objects.reset();
    }
    m_alloc.bump_storage_version();
}

void Table::schema_to_json(std::ostream& out, const std::map<std::string, std::string>& renames) const
{
    out << "{";
    auto name = get_name();
    if (renames.count(name))
        name = renames.at(name);
    out << "\"name\":\"" << name << "\"";
    if (this->m_primary_key_col) {
        out << ",";
        out << "\"primaryKey\":\"" << this->get_column_name(m_primary_key_col) << "\"";
    }
    out << ",\"tableType\":\"" << this->get_table_type() << "\"";
    out << ",\"properties\":[";
    auto col_keys = get_column_keys();
    int sz = int(col_keys.size());
    for (int i = 0; i < sz; ++i) {
        auto col_key = col_keys[i];
        name = get_column_name(col_key);
        auto type = col_key.get_type();
        if (renames.count(name))
            name = renames.at(name);
        out << "{";
        out << "\"name\":\"" << name << "\"";
        if (this->is_link_type(type)) {
            out << ",\"type\":\"object\"";
            name = this->get_opposite_table(col_key)->get_name();
            if (renames.count(name))
                name = renames.at(name);
            out << ",\"objectType\":\"" << name << "\"";
        }
        else {
            out << ",\"type\":\"" << get_data_type_name(DataType(type)) << "\"";
        }
        if (col_key.is_list()) {
            out << ",\"isArray\":true";
        }
        else if (col_key.is_set()) {
            out << ",\"isSet\":true";
        }
        else if (col_key.is_dictionary()) {
            out << ",\"isMap\":true";
            auto key_type = get_dictionary_key_type(col_key);
            out << ",\"keyType\":\"" << get_data_type_name(key_type) << "\"";
        }
        if (col_key.is_nullable()) {
            out << ",\"isOptional\":true";
        }
        if (has_search_index(col_key)) {
            out << ",\"isIndexed\":true";
        }
        out << "}";
        if (i < sz - 1) {
            out << ",";
        }
    }
    out << "]}";
}

void Table::to_json(std::ostream& out, size_t link_depth, const std::map<std::string, std::string>& renames,
                    JSONOutputMode output_mode) const
{
    // Represent table as list of objects
    out << "[";
    bool first = true;

    for (auto& obj : *this) {
        if (first) {
            first = false;
        }
        else {
            out << ",";
        }
        obj.to_json(out, link_depth, renames, output_mode);
    }

    out << "]";
}


size_t Table::compute_aggregated_byte_size() const noexcept
{
    if (!m_top.is_attached())
        return 0;
    const Array& real_top = (m_top);
    MemStats stats_2;
    real_top.stats(stats_2);
    return stats_2.allocated;
}

bool Table::operator==(const Table& t) const
{
    if (size() != t.size()) {
        return false;
    }
    // Check columns
    for (auto ck : this->get_column_keys()) {
        auto name = get_column_name(ck);
        auto other_ck = t.get_column_key(name);
        auto attrs = ck.get_attrs();
        if (has_search_index(ck) != t.has_search_index(other_ck))
            return false;

        if (!other_ck || other_ck.get_attrs() != attrs) {
            return false;
        }
    }
    auto pk_col = get_primary_key_column();
    for (auto o : *this) {
        Obj other_o;
        if (pk_col) {
            auto pk = o.get_any(pk_col);
            other_o = t.get_object_with_primary_key(pk);
        }
        else {
            other_o = t.get_object(o.get_key());
        }
        if (!(other_o && o == other_o))
            return false;
    }

    return true;
}


void Table::flush_for_commit()
{
    if (m_top.is_attached() && m_top.size() >= top_position_for_version) {
        if (!m_top.is_read_only()) {
            ++m_in_file_version_at_transaction_boundary;
            auto rot_version = RefOrTagged::make_tagged(m_in_file_version_at_transaction_boundary);
            m_top.set(top_position_for_version, rot_version);
        }
    }
}

void Table::refresh_content_version()
{
    REALM_ASSERT(m_top.is_attached());
    if (m_top.size() >= top_position_for_version) {
        // we have versioning info in the file. Use this to conditionally
        // bump the version counter:
        auto rot_version = m_top.get_as_ref_or_tagged(top_position_for_version);
        REALM_ASSERT(rot_version.is_tagged());
        if (m_in_file_version_at_transaction_boundary != rot_version.get_as_int()) {
            m_in_file_version_at_transaction_boundary = rot_version.get_as_int();
            bump_content_version();
        }
    }
    else {
        // assume the worst:
        bump_content_version();
    }
}


// Called when Group is moved to another version - either a rollback or an advance.
// The content of the table is potentially different, so make no assumptions.
void Table::refresh_accessor_tree()
{
    REALM_ASSERT(m_cookie == cookie_initialized);
    REALM_ASSERT(m_top.is_attached());
    m_top.init_from_parent();
    m_spec.init_from_parent();
    REALM_ASSERT(m_top.size() > top_position_for_pk_col);
    m_clusters.init_from_parent();
    m_index_refs.init_from_parent();
    m_opposite_table.init_from_parent();
    m_opposite_column.init_from_parent();
    auto rot_pk_key = m_top.get_as_ref_or_tagged(top_position_for_pk_col);
    m_primary_key_col = rot_pk_key.is_tagged() ? ColKey(rot_pk_key.get_as_int()) : ColKey();
    if (m_top.size() > top_position_for_flags) {
        auto rot_flags = m_top.get_as_ref_or_tagged(top_position_for_flags);
        m_table_type = Type(rot_flags.get_as_int() & table_type_mask);
    }
    else {
        m_table_type = Type::TopLevel;
    }
    if (m_top.size() > top_position_for_tombstones && m_top.get_as_ref(top_position_for_tombstones)) {
        // Tombstones exists
        if (!m_tombstones) {
            m_tombstones = std::make_unique<TableClusterTree>(this, m_alloc, size_t(top_position_for_tombstones));
        }
        m_tombstones->init_from_parent();
    }
    else {
        m_tombstones = nullptr;
    }
    refresh_content_version();
    bump_storage_version();
    build_column_mapping();
    refresh_index_accessors();
}

void Table::refresh_index_accessors()
{
    // Refresh search index accessors

    // First eliminate any index accessors for eliminated last columns
    size_t col_ndx_end = m_leaf_ndx2colkey.size();
    m_index_accessors.resize(col_ndx_end);

    // Then eliminate/refresh/create accessors within column range
    // we can not use for_each_column() here, since the columns may have changed
    // and the index accessor vector is not updated correspondingly.
    for (size_t col_ndx = 0; col_ndx < col_ndx_end; col_ndx++) {

        bool has_old_accessor = bool(m_index_accessors[col_ndx]);
        ref_type ref = m_index_refs.get_as_ref(col_ndx);

        if (has_old_accessor && ref == 0) { // accessor drop
            m_index_accessors[col_ndx].reset();
        }
        else if (has_old_accessor && ref != 0) { // still there, refresh:
            auto col_key = m_leaf_ndx2colkey[col_ndx];
            ClusterColumn virtual_col(&m_clusters, col_key);
            m_index_accessors[col_ndx]->refresh_accessor_tree(virtual_col);
        }
        else if (!has_old_accessor && ref != 0) { // new index!
            auto col_key = m_leaf_ndx2colkey[col_ndx];
            ClusterColumn virtual_col(&m_clusters, col_key);
            m_index_accessors[col_ndx] =
                std::make_unique<StringIndex>(ref, &m_index_refs, col_ndx, virtual_col, get_alloc());
        }
    }
}

bool Table::is_cross_table_link_target() const noexcept
{
    auto is_cross_link = [this](ColKey col_key) {
        auto t = col_key.get_type();
        // look for a backlink with a different target than ourselves
        return (t == col_type_BackLink && get_opposite_table_key(col_key) != get_key());
    };
    return for_each_backlink_column(is_cross_link);
}

// LCOV_EXCL_START ignore debug functions

void Table::verify() const
{
#ifdef REALM_DEBUG
    if (m_top.is_attached())
        m_top.verify();
    m_spec.verify();
    m_clusters.verify();
    if (nb_unresolved())
        m_tombstones->verify();
#endif
}

#ifdef REALM_DEBUG
MemStats Table::stats() const
{
    MemStats mem_stats;
    m_top.stats(mem_stats);
    return mem_stats;
}
#endif // LCOV_EXCL_STOP ignore debug functions

Obj Table::create_object(ObjKey key, const FieldValues& values)
{
    if (is_embedded() || m_primary_key_col)
        throw LogicError(LogicError::wrong_kind_of_table);
    if (key == null_key) {
        GlobalKey object_id = allocate_object_id_squeezed();
        key = object_id.get_local_key(get_sync_file_id());
        // Check if this key collides with an already existing object
        // This could happen if objects were at some point created with primary keys,
        // but later primary key property was removed from the schema.
        while (m_clusters.is_valid(key)) {
            object_id = allocate_object_id_squeezed();
            key = object_id.get_local_key(get_sync_file_id());
        }
        if (auto repl = get_repl())
            repl->create_object(this, object_id);
    }

    REALM_ASSERT(key.value >= 0);

    Obj obj = m_clusters.insert(key, values); // repl->set()

    return obj;
}

Obj Table::create_linked_object(GlobalKey object_id)
{
    if (!is_embedded())
        throw LogicError(LogicError::wrong_kind_of_table);
    if (!object_id) {
        object_id = allocate_object_id_squeezed();
    }
    ObjKey key = object_id.get_local_key(get_sync_file_id());

    REALM_ASSERT(key.value >= 0);

    Obj obj = m_clusters.insert(key, {});

    return obj;
}

Obj Table::create_object(GlobalKey object_id, const FieldValues& values)
{
    if (is_embedded() || m_primary_key_col)
        throw LogicError(LogicError::wrong_kind_of_table);
    ObjKey key = object_id.get_local_key(get_sync_file_id());

    if (auto repl = get_repl())
        repl->create_object(this, object_id);

    try {
        Obj obj = m_clusters.insert(key, values);
        // Check if tombstone exists
        if (m_tombstones && m_tombstones->is_valid(key.get_unresolved())) {
            auto unres_key = key.get_unresolved();
            // Copy links over
            auto tombstone = m_tombstones->get(unres_key);
            obj.assign_pk_and_backlinks(tombstone);
            // If tombstones had no links to it, it may still be alive
            if (m_tombstones->is_valid(unres_key)) {
                CascadeState state(CascadeState::Mode::None);
                m_tombstones->erase(unres_key, state);
            }
        }

        return obj;
    }
    catch (const KeyAlreadyUsed&) {
        return m_clusters.get(key);
    }
}

Obj Table::create_object_with_primary_key(const Mixed& primary_key, FieldValues&& field_values, UpdateMode mode,
                                          bool* did_create)
{
    auto primary_key_col = get_primary_key_column();
    if (is_embedded() || !primary_key_col)
        throw LogicError(LogicError::wrong_kind_of_table);
    DataType type = DataType(primary_key_col.get_type());
    REALM_ASSERT((primary_key.is_null() && primary_key_col.get_attrs().test(col_attr_Nullable)) ||
                 primary_key.get_type() == type);

    REALM_ASSERT(type == type_String || type == type_ObjectId || type == type_Int || type == type_UUID);

    if (did_create)
        *did_create = false;

    // Check for existing object
    if (ObjKey key = m_index_accessors[primary_key_col.get_index().val]->find_first(primary_key)) {
        if (mode == UpdateMode::never) {
            throw std::logic_error(
                util::format("Attempting to create an object in '%1' with an existing primary key value '%2'.",
                             get_name(), primary_key));
        }
        auto obj = m_clusters.get(key);
        for (auto& val : field_values) {
            if (mode == UpdateMode::all || obj.get_any(val.col_key) != val.value) {
                obj.set_any(val.col_key, val.value, val.is_default);
            }
        }
        return obj;
    }

    ObjKey unres_key;
    if (m_tombstones) {
        // Check for potential tombstone
        GlobalKey object_id{primary_key};
        ObjKey object_key = global_to_local_object_id_hashed(object_id);

        ObjKey key = object_key.get_unresolved();
        if (auto obj = m_tombstones->try_get_obj(key)) {
            auto existing_pk_value = obj.get_any(primary_key_col);

            // If the primary key is the same, the object should be resurrected below
            if (existing_pk_value == primary_key) {
                unres_key = key;
            }
        }
    }

    ObjKey key = get_next_valid_key();

    auto repl = get_repl();
    if (repl) {
        repl->create_object_with_primary_key(this, key, primary_key);
    }
    if (did_create) {
        *did_create = true;
    }

    field_values.insert(primary_key_col, primary_key);
    Obj ret = m_clusters.insert(key, field_values);

    // Check if unresolved exists
    if (unres_key) {
        auto tombstone = m_tombstones->get(unres_key);
        ret.assign_pk_and_backlinks(tombstone);
        // If tombstones had no links to it, it may still be alive
        if (m_tombstones->is_valid(unres_key)) {
            CascadeState state(CascadeState::Mode::None);
            m_tombstones->erase(unres_key, state);
        }
    }
    if (is_asymmetric() && repl && repl->get_history_type() == Replication::HistoryType::hist_SyncClient) {
        get_parent_group()->m_objects_to_delete.emplace_back(this->m_key, ret.get_key());
    }
    return ret;
}

ObjKey Table::find_primary_key(Mixed primary_key) const
{
    auto primary_key_col = get_primary_key_column();
    REALM_ASSERT(primary_key_col);
    DataType type = DataType(primary_key_col.get_type());
    REALM_ASSERT((primary_key.is_null() && primary_key_col.get_attrs().test(col_attr_Nullable)) ||
                 primary_key.get_type() == type);

    if (auto&& index = m_index_accessors[primary_key_col.get_index().val]) {
        return index->find_first(primary_key);
    }

    // This must be file format 11, 20 or 21 as those are the ones we can open in read-only mode
    // so try the old algorithm
    GlobalKey object_id{primary_key};
    ObjKey object_key = global_to_local_object_id_hashed(object_id);

    // Check if existing
    if (auto obj = m_clusters.try_get_obj(object_key)) {
        auto existing_pk_value = obj.get_any(primary_key_col);

        if (existing_pk_value == primary_key) {
            return object_key;
        }
    }
    return {};
}

ObjKey Table::get_objkey_from_primary_key(const Mixed& primary_key)
{
    // Check if existing
    if (auto key = find_primary_key(primary_key)) {
        return key;
    }

    // Object does not exist - create tombstone
    GlobalKey object_id{primary_key};
    ObjKey object_key = global_to_local_object_id_hashed(object_id);
    return get_or_create_tombstone(object_key, m_primary_key_col, primary_key).get_key();
}

ObjKey Table::get_objkey_from_global_key(GlobalKey global_key)
{
    REALM_ASSERT(!m_primary_key_col);
    auto object_key = global_key.get_local_key(get_sync_file_id());

    // Check if existing
    if (m_clusters.is_valid(object_key)) {
        return object_key;
    }

    return get_or_create_tombstone(object_key, {}, {}).get_key();
}

ObjKey Table::get_objkey(GlobalKey global_key) const
{
    ObjKey key;
    REALM_ASSERT(!m_primary_key_col);
    uint32_t max = std::numeric_limits<uint32_t>::max();
    if (global_key.hi() <= max && global_key.lo() <= max) {
        key = global_key.get_local_key(get_sync_file_id());
    }
    if (key && !is_valid(key)) {
        key = realm::null_key;
    }
    return key;
}

GlobalKey Table::get_object_id(ObjKey key) const
{
    auto col = get_primary_key_column();
    if (col) {
        const Obj obj = get_object(key);
        auto val = obj.get_any(col);
        return {val};
    }
    else {
        return {key, get_sync_file_id()};
    }
    return {};
}

Obj Table::get_object_with_primary_key(Mixed primary_key) const
{
    auto primary_key_col = get_primary_key_column();
    REALM_ASSERT(primary_key_col);
    DataType type = DataType(primary_key_col.get_type());
    REALM_ASSERT((primary_key.is_null() && primary_key_col.get_attrs().test(col_attr_Nullable)) ||
                 primary_key.get_type() == type);

    return m_clusters.get(m_index_accessors[primary_key_col.get_index().val]->find_first(primary_key));
}

Mixed Table::get_primary_key(ObjKey key) const
{
    auto primary_key_col = get_primary_key_column();
    REALM_ASSERT(primary_key_col);
    if (key.is_unresolved()) {
        REALM_ASSERT(m_tombstones);
        return m_tombstones->get(key).get_any(primary_key_col);
    }
    else {
        return m_clusters.get(key).get_any(primary_key_col);
    }
}

GlobalKey Table::allocate_object_id_squeezed()
{
    // m_client_file_ident will be zero if we haven't been in contact with
    // the server yet.
    auto peer_id = get_sync_file_id();
    auto sequence = allocate_sequence_number();
    return GlobalKey{peer_id, sequence};
}

namespace {

/// Calculate optimistic local ID that may collide with others. It is up to
/// the caller to ensure that collisions are detected and that
/// allocate_local_id_after_collision() is called to obtain a non-colliding
/// ID.
inline ObjKey get_optimistic_local_id_hashed(GlobalKey global_id)
{
#if REALM_EXERCISE_OBJECT_ID_COLLISION
    const uint64_t optimistic_mask = 0xff;
#else
    const uint64_t optimistic_mask = 0x3fffffffffffffff;
#endif
    static_assert(!(optimistic_mask >> 62), "optimistic Object ID mask must leave the 63rd and 64th bit zero");
    return ObjKey{int64_t(global_id.lo() & optimistic_mask)};
}

inline ObjKey make_tagged_local_id_after_hash_collision(uint64_t sequence_number)
{
    REALM_ASSERT(!(sequence_number >> 62));
    return ObjKey{int64_t(0x4000000000000000 | sequence_number)};
}

} // namespace

ObjKey Table::global_to_local_object_id_hashed(GlobalKey object_id) const
{
    ObjKey optimistic = get_optimistic_local_id_hashed(object_id);

    if (ref_type collision_map_ref = to_ref(m_top.get(top_position_for_collision_map))) {
        Allocator& alloc = m_top.get_alloc();
        Array collision_map{alloc};
        collision_map.init_from_ref(collision_map_ref); // Throws

        Array hi{alloc};
        hi.init_from_ref(to_ref(collision_map.get(s_collision_map_hi))); // Throws

        // Entries are ordered by hi,lo
        size_t found = hi.find_first(object_id.hi());
        if (found != npos && uint64_t(hi.get(found)) == object_id.hi()) {
            Array lo{alloc};
            lo.init_from_ref(to_ref(collision_map.get(s_collision_map_lo))); // Throws
            size_t candidate = lo.find_first(object_id.lo(), found);
            if (candidate != npos && uint64_t(hi.get(candidate)) == object_id.hi()) {
                Array local_id{alloc};
                local_id.init_from_ref(to_ref(collision_map.get(s_collision_map_local_id))); // Throws
                return ObjKey{local_id.get(candidate)};
            }
        }
    }

    return optimistic;
}

ObjKey Table::allocate_local_id_after_hash_collision(GlobalKey incoming_id, GlobalKey colliding_id,
                                                     ObjKey colliding_local_id)
{
    // Possible optimization: Cache these accessors
    Allocator& alloc = m_top.get_alloc();
    Array collision_map{alloc};
    Array hi{alloc};
    Array lo{alloc};
    Array local_id{alloc};

    collision_map.set_parent(&m_top, top_position_for_collision_map);
    hi.set_parent(&collision_map, s_collision_map_hi);
    lo.set_parent(&collision_map, s_collision_map_lo);
    local_id.set_parent(&collision_map, s_collision_map_local_id);

    ref_type collision_map_ref = to_ref(m_top.get(top_position_for_collision_map));
    if (collision_map_ref) {
        collision_map.init_from_parent(); // Throws
    }
    else {
        MemRef mem = Array::create_empty_array(Array::type_HasRefs, false, alloc); // Throws
        collision_map.init_from_mem(mem);                                          // Throws
        collision_map.update_parent();

        ref_type lo_ref = Array::create_array(Array::type_Normal, false, 0, 0, alloc).get_ref();       // Throws
        ref_type hi_ref = Array::create_array(Array::type_Normal, false, 0, 0, alloc).get_ref();       // Throws
        ref_type local_id_ref = Array::create_array(Array::type_Normal, false, 0, 0, alloc).get_ref(); // Throws
        collision_map.add(lo_ref);                                                                     // Throws
        collision_map.add(hi_ref);                                                                     // Throws
        collision_map.add(local_id_ref);                                                               // Throws
    }

    hi.init_from_parent();       // Throws
    lo.init_from_parent();       // Throws
    local_id.init_from_parent(); // Throws

    size_t num_entries = hi.size();
    REALM_ASSERT(lo.size() == num_entries);
    REALM_ASSERT(local_id.size() == num_entries);

    auto lower_bound_object_id = [&](GlobalKey object_id) -> size_t {
        size_t i = hi.lower_bound_int(int64_t(object_id.hi()));
        while (i < num_entries && uint64_t(hi.get(i)) == object_id.hi() && uint64_t(lo.get(i)) < object_id.lo())
            ++i;
        return i;
    };

    auto insert_collision = [&](GlobalKey object_id, ObjKey new_local_id) {
        size_t i = lower_bound_object_id(object_id);
        if (i != num_entries) {
            GlobalKey existing{uint64_t(hi.get(i)), uint64_t(lo.get(i))};
            if (existing == object_id) {
                REALM_ASSERT(new_local_id.value == local_id.get(i));
                return;
            }
        }
        hi.insert(i, int64_t(object_id.hi()));
        lo.insert(i, int64_t(object_id.lo()));
        local_id.insert(i, new_local_id.value);
        ++num_entries;
    };

    auto sequence_number_for_local_id = allocate_sequence_number();
    ObjKey new_local_id = make_tagged_local_id_after_hash_collision(sequence_number_for_local_id);
    insert_collision(incoming_id, new_local_id);
    insert_collision(colliding_id, colliding_local_id);

    return new_local_id;
}

Obj Table::get_or_create_tombstone(ObjKey key, ColKey pk_col, Mixed pk_val)
{
    auto unres_key = key.get_unresolved();

    ensure_graveyard();
    auto tombstone = m_tombstones->try_get_obj(unres_key);
    if (tombstone) {
        if (pk_col) {
            auto existing_pk_value = tombstone.get_any(pk_col);
            // It may just be the same object
            if (existing_pk_value != pk_val) {
                // We have a collision - create new ObjKey
                key = allocate_local_id_after_hash_collision({pk_val}, {existing_pk_value}, key);
                return get_or_create_tombstone(key, pk_col, pk_val);
            }
        }
        return tombstone;
    }
    return m_tombstones->insert(unres_key, {{pk_col, pk_val}});
}

void Table::free_local_id_after_hash_collision(ObjKey key)
{
    if (ref_type collision_map_ref = to_ref(m_top.get(top_position_for_collision_map))) {
        if (key.is_unresolved()) {
            // Keys will always be inserted as resolved
            key = key.get_unresolved();
        }
        // Possible optimization: Cache these accessors
        Array collision_map{m_alloc};
        Array local_id{m_alloc};

        collision_map.set_parent(&m_top, top_position_for_collision_map);
        local_id.set_parent(&collision_map, s_collision_map_local_id);
        collision_map.init_from_ref(collision_map_ref);
        local_id.init_from_parent();
        auto ndx = local_id.find_first(key.value);
        if (ndx != realm::npos) {
            Array hi{m_alloc};
            Array lo{m_alloc};

            hi.set_parent(&collision_map, s_collision_map_hi);
            lo.set_parent(&collision_map, s_collision_map_lo);
            hi.init_from_parent();
            lo.init_from_parent();

            hi.erase(ndx);
            lo.erase(ndx);
            local_id.erase(ndx);
            if (hi.size() == 0) {
                free_collision_table();
            }
        }
    }
}

void Table::free_collision_table()
{
    if (ref_type collision_map_ref = to_ref(m_top.get(top_position_for_collision_map))) {
        Array::destroy_deep(collision_map_ref, m_alloc);
        m_top.set(top_position_for_collision_map, 0);
    }
}

void Table::create_objects(size_t number, std::vector<ObjKey>& keys)
{
    while (number--) {
        keys.push_back(create_object().get_key());
    }
}

void Table::create_objects(const std::vector<ObjKey>& keys)
{
    for (auto k : keys) {
        create_object(k);
    }
}

void Table::dump_objects()
{
    m_clusters.dump_objects();
    if (nb_unresolved())
        m_tombstones->dump_objects();
}

void Table::remove_object(ObjKey key)
{
    Group* g = get_parent_group();

    if (has_any_embedded_objects() || (g && g->has_cascade_notification_handler())) {
        CascadeState state(CascadeState::Mode::Strong, g);
        state.m_to_be_deleted.emplace_back(m_key, key);
        m_clusters.nullify_links(key, state);
        remove_recursive(state);
    }
    else {
        CascadeState state(CascadeState::Mode::None, g);
        if (g) {
            m_clusters.nullify_links(key, state);
        }
        m_clusters.erase(key, state);
    }
}

ObjKey Table::invalidate_object(ObjKey key)
{
    if (is_embedded())
        throw LogicError(LogicError::wrong_kind_of_table);
    REALM_ASSERT(!key.is_unresolved());

    Obj tombstone;
    auto obj = get_object(key);
    if (obj.has_backlinks(false)) {
        // If the object has backlinks, we should make a tombstone
        // and make inward links point to it,
        if (auto primary_key_col = get_primary_key_column()) {
            auto pk = obj.get_any(primary_key_col);
            GlobalKey object_id{pk};
            auto unres_key = global_to_local_object_id_hashed(object_id);
            tombstone = get_or_create_tombstone(unres_key, primary_key_col, pk);
        }
        else {
            tombstone = get_or_create_tombstone(key, {}, {});
        }
        tombstone.assign_pk_and_backlinks(obj);
    }

    remove_object(key);

    return tombstone.get_key();
}

void Table::remove_object_recursive(ObjKey key)
{
    size_t table_ndx = get_index_in_group();
    if (table_ndx != realm::npos) {
        CascadeState state(CascadeState::Mode::All, get_parent_group());
        state.m_to_be_deleted.emplace_back(m_key, key);
        nullify_links(state);
        remove_recursive(state);
    }
    else {
        // No links in freestanding table
        CascadeState state(CascadeState::Mode::None);
        m_clusters.erase(key, state);
    }
}

Table::Iterator Table::begin() const
{
    return Iterator(m_clusters, 0);
}

Table::Iterator Table::end() const
{
    return Iterator(m_clusters, size());
}

TableRef _impl::TableFriend::get_opposite_link_table(const Table& table, ColKey col_key)
{
    TableRef ret;
    if (col_key) {
        return table.get_opposite_table(col_key);
    }
    return ret;
}

const uint64_t Table::max_num_columns;

void Table::build_column_mapping()
{
    // build column mapping from spec
    // TODO: Optimization - Don't rebuild this for every change
    m_spec_ndx2leaf_ndx.clear();
    m_leaf_ndx2spec_ndx.clear();
    m_leaf_ndx2colkey.clear();
    size_t num_spec_cols = m_spec.get_column_count();
    m_spec_ndx2leaf_ndx.resize(num_spec_cols);
    for (size_t spec_ndx = 0; spec_ndx < num_spec_cols; ++spec_ndx) {
        ColKey col_key = m_spec.get_key(spec_ndx);
        unsigned leaf_ndx = col_key.get_index().val;
        if (leaf_ndx >= m_leaf_ndx2colkey.size()) {
            m_leaf_ndx2colkey.resize(leaf_ndx + 1);
            m_leaf_ndx2spec_ndx.resize(leaf_ndx + 1, -1);
        }
        m_spec_ndx2leaf_ndx[spec_ndx] = ColKey::Idx{leaf_ndx};
        m_leaf_ndx2spec_ndx[leaf_ndx] = spec_ndx;
        m_leaf_ndx2colkey[leaf_ndx] = col_key;
    }
}

ColKey Table::generate_col_key(ColumnType tp, ColumnAttrMask attr)
{
    REALM_ASSERT(!attr.test(col_attr_Indexed));
    REALM_ASSERT(!attr.test(col_attr_Unique)); // Must not be encoded into col_key

    int64_t col_seq_number = m_top.get_as_ref_or_tagged(top_position_for_column_key).get_as_int();
    unsigned upper = unsigned(col_seq_number ^ get_key().value);

    // reuse lowest available leaf ndx:
    unsigned lower = unsigned(m_leaf_ndx2colkey.size());
    // look for an unused entry:
    for (unsigned idx = 0; idx < lower; ++idx) {
        if (m_leaf_ndx2colkey[idx] == ColKey()) {
            lower = idx;
            break;
        }
    }
    return ColKey(ColKey::Idx{lower}, tp, attr, upper);
}

Table::BacklinkOrigin Table::find_backlink_origin(StringData origin_table_name,
                                                  StringData origin_col_name) const noexcept
{
    BacklinkOrigin ret;
    auto f = [&](ColKey backlink_col_key) {
        auto origin_table = get_opposite_table(backlink_col_key);
        auto origin_link_col = get_opposite_column(backlink_col_key);
        if (origin_table->get_name() == origin_table_name &&
            origin_table->get_column_name(origin_link_col) == origin_col_name) {
            ret = BacklinkOrigin{{origin_table, origin_link_col}};
            return true;
        }
        return false;
    };
    this->for_each_backlink_column(f);
    return ret;
}

Table::BacklinkOrigin Table::find_backlink_origin(ColKey backlink_col) const noexcept
{
    try {
        TableKey linked_table_key = get_opposite_table_key(backlink_col);
        ColKey linked_column_key = get_opposite_column(backlink_col);
        if (linked_table_key == m_key) {
            return {{m_own_ref, linked_column_key}};
        }
        else {
            Group* current_group = get_parent_group();
            if (current_group) {
                ConstTableRef linked_table_ref = current_group->get_table(linked_table_key);
                return {{linked_table_ref, linked_column_key}};
            }
        }
    }
    catch (...) {
        // backlink column not found, returning empty optional
    }
    return {};
}

std::vector<std::pair<TableKey, ColKey>> Table::get_incoming_link_columns() const noexcept
{
    std::vector<std::pair<TableKey, ColKey>> origins;
    auto f = [&](ColKey backlink_col_key) {
        auto origin_table_key = get_opposite_table_key(backlink_col_key);
        auto origin_link_col = get_opposite_column(backlink_col_key);
        origins.emplace_back(origin_table_key, origin_link_col);
        return false;
    };
    this->for_each_backlink_column(f);
    return origins;
}

ColKey Table::get_primary_key_column() const
{
    return m_primary_key_col;
}

void Table::set_primary_key_column(ColKey col_key)
{
    if (col_key == m_primary_key_col) {
        return;
    }

    if (Replication* repl = get_repl()) {
        if (repl->get_history_type() == Replication::HistoryType::hist_SyncClient) {
            throw std::logic_error(
                util::format("Cannot change primary key property in '%1' when realm is synchronized", get_name()));
        }
    }

    REALM_ASSERT_RELEASE(col_key.value >= 0); // Just to be sure. We have an issue where value seems to be -1

    if (col_key) {
        check_column(col_key);
        validate_column_is_unique(col_key);
        do_set_primary_key_column(col_key);
    }
    else {
        do_set_primary_key_column(col_key);
    }
}


void Table::do_set_primary_key_column(ColKey col_key)
{
    if (m_primary_key_col) {
        // If the search index has not been set explicitly on current pk col, we remove it again
        auto spec_ndx = leaf_ndx2spec_ndx(m_primary_key_col.get_index());
        auto attr = m_spec.get_column_attr(spec_ndx);
        if (!attr.test(col_attr_Indexed)) {
            remove_search_index(m_primary_key_col);
        }
    }

    if (col_key) {
        m_top.set(top_position_for_pk_col, RefOrTagged::make_tagged(col_key.value));
        do_add_search_index(col_key);
    }
    else {
        m_top.set(top_position_for_pk_col, 0);
    }

    m_primary_key_col = col_key;
}

bool Table::contains_unique_values(ColKey col) const
{
    if (has_search_index(col)) {
        auto search_index = get_search_index(col);
        return !search_index->has_duplicate_values();
    }
    else {
        TableView tv = where().find_all();
        tv.distinct(col);
        return tv.size() == size();
    }
}

void Table::validate_column_is_unique(ColKey col) const
{
    if (!contains_unique_values(col)) {
        throw DuplicatePrimaryKeyValueException(get_name(), get_column_name(col));
    }
}

void Table::validate_primary_column()
{
    if (ColKey col = get_primary_key_column()) {
        validate_column_is_unique(col);
    }
}

ObjKey Table::get_next_valid_key()
{
    ObjKey key;
    do {
        key = ObjKey(allocate_sequence_number());
    } while (m_clusters.is_valid(key));

    return key;
}

namespace {
template <class T>
typename util::RemoveOptional<T>::type remove_optional(T val)
{
    return val;
}
template <>
int64_t remove_optional<Optional<int64_t>>(Optional<int64_t> val)
{
    return val.value();
}
template <>
bool remove_optional<Optional<bool>>(Optional<bool> val)
{
    return val.value();
}
template <>
ObjectId remove_optional<Optional<ObjectId>>(Optional<ObjectId> val)
{
    return val.value();
}
template <>
UUID remove_optional<Optional<UUID>>(Optional<UUID> val)
{
    return val.value();
}
} // namespace

template <class F, class T>
void Table::change_nullability(ColKey key_from, ColKey key_to, bool throw_on_null)
{
    Allocator& allocator = this->get_alloc();
    bool from_nullability = is_nullable(key_from);
    auto func = [&](Cluster* cluster) {
        size_t sz = cluster->node_size();

        typename ColumnTypeTraits<F>::cluster_leaf_type from_arr(allocator);
        typename ColumnTypeTraits<T>::cluster_leaf_type to_arr(allocator);
        cluster->init_leaf(key_from, &from_arr);
        cluster->init_leaf(key_to, &to_arr);

        for (size_t i = 0; i < sz; i++) {
            if (from_nullability && from_arr.is_null(i)) {
                if (throw_on_null) {
                    throw std::runtime_error(util::format("Objects in '%1' has null value(s) in property '%2'",
                                                          get_name(), get_column_name(key_from)));
                }
                else {
                    to_arr.set(i, ColumnTypeTraits<T>::cluster_leaf_type::default_value(false));
                }
            }
            else {
                auto v = remove_optional(from_arr.get(i));
                to_arr.set(i, v);
            }
        }
    };

    m_clusters.update(func);
}

template <class F, class T>
void Table::change_nullability_list(ColKey key_from, ColKey key_to, bool throw_on_null)
{
    Allocator& allocator = this->get_alloc();
    bool from_nullability = is_nullable(key_from);
    auto func = [&](Cluster* cluster) {
        size_t sz = cluster->node_size();

        ArrayInteger from_arr(allocator);
        ArrayInteger to_arr(allocator);
        cluster->init_leaf(key_from, &from_arr);
        cluster->init_leaf(key_to, &to_arr);

        for (size_t i = 0; i < sz; i++) {
            ref_type ref_from = to_ref(from_arr.get(i));
            ref_type ref_to = to_ref(to_arr.get(i));
            REALM_ASSERT(!ref_to);

            if (ref_from) {
                BPlusTree<F> from_list(allocator);
                BPlusTree<T> to_list(allocator);
                from_list.init_from_ref(ref_from);
                to_list.create();
                size_t n = from_list.size();
                for (size_t j = 0; j < n; j++) {
                    auto v = from_list.get(j);
                    if (!from_nullability || aggregate_operations::valid_for_agg(v)) {
                        to_list.add(remove_optional(v));
                    }
                    else {
                        if (throw_on_null) {
                            throw std::runtime_error(
                                util::format("Objects in '%1' has null value(s) in list property '%2'", get_name(),
                                             get_column_name(key_from)));
                        }
                        else {
                            to_list.add(ColumnTypeTraits<T>::cluster_leaf_type::default_value(false));
                        }
                    }
                }
                to_arr.set(i, from_ref(to_list.get_ref()));
            }
        }
    };

    m_clusters.update(func);
}

void Table::convert_column(ColKey from, ColKey to, bool throw_on_null)
{
    realm::DataType type_id = get_column_type(from);
    bool _is_list = is_list(from);
    if (_is_list) {
        switch (type_id) {
            case type_Int:
                if (is_nullable(from)) {
                    change_nullability_list<Optional<int64_t>, int64_t>(from, to, throw_on_null);
                }
                else {
                    change_nullability_list<int64_t, Optional<int64_t>>(from, to, throw_on_null);
                }
                break;
            case type_Float:
                change_nullability_list<float, float>(from, to, throw_on_null);
                break;
            case type_Double:
                change_nullability_list<double, double>(from, to, throw_on_null);
                break;
            case type_Bool:
                change_nullability_list<Optional<bool>, Optional<bool>>(from, to, throw_on_null);
                break;
            case type_String:
                change_nullability_list<StringData, StringData>(from, to, throw_on_null);
                break;
            case type_Binary:
                change_nullability_list<BinaryData, BinaryData>(from, to, throw_on_null);
                break;
            case type_Timestamp:
                change_nullability_list<Timestamp, Timestamp>(from, to, throw_on_null);
                break;
            case type_ObjectId:
                if (is_nullable(from)) {
                    change_nullability_list<Optional<ObjectId>, ObjectId>(from, to, throw_on_null);
                }
                else {
                    change_nullability_list<ObjectId, Optional<ObjectId>>(from, to, throw_on_null);
                }
                break;
            case type_Decimal:
                change_nullability_list<Decimal128, Decimal128>(from, to, throw_on_null);
                break;
            case type_UUID:
                if (is_nullable(from)) {
                    change_nullability_list<Optional<UUID>, UUID>(from, to, throw_on_null);
                }
                else {
                    change_nullability_list<UUID, Optional<UUID>>(from, to, throw_on_null);
                }
                break;
            case type_Link:
            case type_TypedLink:
            case type_LinkList:
                // Can't have lists of these types
            case type_Mixed:
                // These types are no longer supported at all
                REALM_UNREACHABLE();
                break;
        }
    }
    else {
        switch (type_id) {
            case type_Int:
                if (is_nullable(from)) {
                    change_nullability<Optional<int64_t>, int64_t>(from, to, throw_on_null);
                }
                else {
                    change_nullability<int64_t, Optional<int64_t>>(from, to, throw_on_null);
                }
                break;
            case type_Float:
                change_nullability<float, float>(from, to, throw_on_null);
                break;
            case type_Double:
                change_nullability<double, double>(from, to, throw_on_null);
                break;
            case type_Bool:
                change_nullability<Optional<bool>, Optional<bool>>(from, to, throw_on_null);
                break;
            case type_String:
                change_nullability<StringData, StringData>(from, to, throw_on_null);
                break;
            case type_Binary:
                change_nullability<BinaryData, BinaryData>(from, to, throw_on_null);
                break;
            case type_Timestamp:
                change_nullability<Timestamp, Timestamp>(from, to, throw_on_null);
                break;
            case type_ObjectId:
                if (is_nullable(from)) {
                    change_nullability<Optional<ObjectId>, ObjectId>(from, to, throw_on_null);
                }
                else {
                    change_nullability<ObjectId, Optional<ObjectId>>(from, to, throw_on_null);
                }
                break;
            case type_Decimal:
                change_nullability<Decimal128, Decimal128>(from, to, throw_on_null);
                break;
            case type_UUID:
                if (is_nullable(from)) {
                    change_nullability<Optional<UUID>, UUID>(from, to, throw_on_null);
                }
                else {
                    change_nullability<UUID, Optional<UUID>>(from, to, throw_on_null);
                }
                break;
            case type_TypedLink:
            case type_Link:
                // Always nullable, so can't convert
            case type_LinkList:
                // Never nullable, so can't convert
            case type_Mixed:
                // These types are no longer supported at all
                REALM_UNREACHABLE();
                break;
        }
    }
}


ColKey Table::set_nullability(ColKey col_key, bool nullable, bool throw_on_null)
{
    if (col_key.is_nullable() == nullable)
        return col_key;

    check_column(col_key);

    bool si = has_search_index(col_key);
    std::string column_name(get_column_name(col_key));
    auto type = col_key.get_type();
    auto attr = col_key.get_attrs();
    bool is_pk_col = (col_key == m_primary_key_col);
    if (nullable) {
        attr.set(col_attr_Nullable);
    }
    else {
        attr.reset(col_attr_Nullable);
    }

    ColKey new_col = generate_col_key(type, attr);
    do_insert_root_column(new_col, type, "__temporary");

    try {
        convert_column(col_key, new_col, throw_on_null);
    }
    catch (...) {
        // remove any partially filled column
        remove_column(new_col);
        throw;
    }

    if (is_pk_col) {
        // If we go from non nullable to nullable, no values change,
        // so it is safe to preserve the pk column. Otherwise it is not
        // safe as a null entry might have been converted to default value.
        do_set_primary_key_column(nullable ? new_col : ColKey{});
    }

    erase_root_column(col_key);
    m_spec.rename_column(colkey2spec_ndx(new_col), column_name);

    if (si)
        do_add_search_index(new_col);

    return new_col;
}

bool Table::has_any_embedded_objects()
{
    if (!m_has_any_embedded_objects) {
        m_has_any_embedded_objects = false;
        for_each_public_column([&](ColKey col_key) {
            auto target_table_key = get_opposite_table_key(col_key);
            if (target_table_key && is_link_type(col_key.get_type())) {
                auto target_table = get_parent_group()->get_table(target_table_key);
                if (target_table->is_embedded()) {
                    m_has_any_embedded_objects = true;
                    return true; // early out
                }
            }
            return false;
        });
    }
    return *m_has_any_embedded_objects;
}

void Table::set_opposite_column(ColKey col_key, TableKey opposite_table, ColKey opposite_column)
{
    m_opposite_table.set(col_key.get_index().val, opposite_table.value);
    m_opposite_column.set(col_key.get_index().val, opposite_column.value);
}

ColKey Table::find_backlink_column(ColKey origin_col_key, TableKey origin_table) const
{
    for (size_t i = 0; i < m_opposite_column.size(); i++) {
        if (m_opposite_column.get(i) == origin_col_key.value && m_opposite_table.get(i) == origin_table.value) {
            return m_spec.get_key(m_leaf_ndx2spec_ndx[i]);
        }
    }

    return {};
}

ColKey Table::find_or_add_backlink_column(ColKey origin_col_key, TableKey origin_table)
{
    ColKey backlink_col_key = find_backlink_column(origin_col_key, origin_table);

    if (!backlink_col_key) {
        backlink_col_key = do_insert_root_column(ColKey{}, col_type_BackLink, "");
        set_opposite_column(backlink_col_key, origin_table, origin_col_key);

        if (Replication* repl = get_repl())
            repl->typed_link_change(get_parent_group()->get_table(origin_table).unchecked_ptr(), origin_col_key,
                                    m_key); // Throws
    }

    return backlink_col_key;
}

TableKey Table::get_opposite_table_key(ColKey col_key) const
{
    return TableKey(int32_t(m_opposite_table.get(col_key.get_index().val)));
}

bool Table::links_to_self(ColKey col_key) const
{
    return get_opposite_table_key(col_key) == m_key;
}

TableRef Table::get_opposite_table(ColKey col_key) const
{
    if (auto k = get_opposite_table_key(col_key)) {
        return get_parent_group()->get_table(k);
    }
    return {};
}

ColKey Table::get_opposite_column(ColKey col_key) const
{
    return ColKey(m_opposite_column.get(col_key.get_index().val));
}

ColKey Table::find_opposite_column(ColKey col_key) const
{
    for (size_t i = 0; i < m_opposite_column.size(); i++) {
        if (m_opposite_column.get(i) == col_key.value) {
            return m_spec.get_key(m_leaf_ndx2spec_ndx[i]);
        }
    }
    return ColKey();
}
