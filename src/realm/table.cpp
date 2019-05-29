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
#include <realm/index_string.hpp>
#include <realm/group.hpp>
#include <realm/replication.hpp>
#include <realm/table_view.hpp>
#include <realm/query_engine.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_binary.hpp>
#include <realm/array_string.hpp>
#include <realm/array_timestamp.hpp>
#include <realm/table_tpl.hpp>
#include <realm/object_id.hpp>

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

const int_fast64_t realm::Table::max_integer;
const int_fast64_t realm::Table::min_integer;
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
        case type_Link:
            return "link";
        case type_LinkList:
            return "linklist";
        default:
            return "unknown";
    }
    return "";
}
} // namespace realm

// -- Table ---------------------------------------------------------------------------------

ColKey Table::add_column(DataType type, StringData name, bool nullable)
{
    return insert_column(ColKey(), type, name, nullable); // Throws
}

ColKey Table::add_column_list(DataType type, StringData name, bool nullable)
{
    LinkTargetInfo invalid_link;
    return do_insert_column(ColKey(), type, name, invalid_link, nullable, true); // Throws
}

ColKey Table::add_column_link(DataType type, StringData name, Table& target, LinkType link_type)
{
    return insert_column_link(ColKey(), type, name, target, link_type); // Throws
}


ColKey Table::insert_column_link(ColKey col_key, DataType type, StringData name, Table& target, LinkType link_type)
{
    if (REALM_UNLIKELY(col_key && !valid_column(col_key)))
        throw InvalidKey("Requested key in use");
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
    auto retval =
        do_insert_column(col_key, type, name, link_target_info, false, type == type_LinkList, link_type); // Throws
    return retval;
}

void Table::remove_recursive(CascadeState& cascade_state)
{
    // recursive remove not relevant for free standing tables
    if (Group* group = get_parent_group()) {
        if (group->has_cascade_notification_handler()) {
            cascade_state.m_group = group;
        }

        while (!cascade_state.m_to_be_deleted.empty()) {
            auto obj = cascade_state.m_to_be_deleted.back();
            auto table = group->get_table(obj.first);
            cascade_state.m_to_be_deleted.pop_back();
            // This might add to the list of objects that should be deleted
            table->m_clusters.erase(obj.second, cascade_state);
        }
    }
}


ColKey Table::insert_column(ColKey col_key, DataType type, StringData name, bool nullable)
{
    if (REALM_UNLIKELY(col_key && !valid_column(col_key)))
        throw InvalidKey("Requested key in use");
    if (REALM_UNLIKELY(is_link_type(ColumnType(type))))
        throw LogicError(LogicError::illegal_type);

    LinkTargetInfo invalid_link;
    return do_insert_column(col_key, type, name, invalid_link, nullable); // Throws
}


void Table::remove_column(ColKey col_key)
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("Non-existing column");

    if (Replication* repl = get_repl())
        repl->erase_column(this, col_key); // Throws

    bump_content_version();
    bump_storage_version();
    erase_root_column(col_key); // Throws
}


void Table::rename_column(ColKey col_key, StringData name)
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("Non-existing column");

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
        return TableKey(rot.get_as_int());
    }
    else {
        return TableKey();
    }
}


void Table::init(ref_type top_ref, ArrayParent* parent, size_t ndx_in_parent, bool is_writable)
{
    m_alloc.set_read_only(!is_writable);
    // Load from allocated memory
    m_top.set_parent(parent, ndx_in_parent);
    m_top.init_from_ref(top_ref);

    m_spec.set_parent(&m_top, top_position_for_spec);
    m_spec.init_from_parent();

    while (m_top.size() < top_array_size) {
        m_top.add(0);
    }

    if (m_top.get_as_ref(top_position_for_cluster_tree) == 0) {
        MemRef mem = ClusterTree::create_empty_cluster(m_top.get_alloc()); // Throws
        m_top.set_as_ref(top_position_for_cluster_tree, mem.get_ref());
    }
    m_clusters.init_from_ref(m_top.get_as_ref(top_position_for_cluster_tree));
    m_clusters.set_parent(&m_top, top_position_for_cluster_tree);

    RefOrTagged rot = m_top.get_as_ref_or_tagged(top_position_for_key);
    if (!rot.is_tagged()) {
        rot = RefOrTagged::make_tagged(ndx_in_parent);
        m_top.set(top_position_for_key, rot);
    }
    m_key = TableKey(rot.get_as_int());

    // index setup relies on column mapping being up to date:
    build_column_mapping();
    m_index_refs.set_parent(&m_top, top_position_for_search_indexes);
    m_opposite_table.set_parent(&m_top, top_position_for_opposite_table);
    m_opposite_column.set_parent(&m_top, top_position_for_opposite_column);
    if (m_top.get_as_ref(top_position_for_search_indexes) == 0) {
        // This is an upgrade - create the necessary arrays
        bool context_flag = false;
        size_t nb_columns = m_spec.get_column_count();
        MemRef mem = Array::create_array(Array::type_HasRefs, context_flag, nb_columns, 0, m_top.get_alloc());
        m_index_refs.init_from_mem(mem);
        m_index_refs.update_parent();
        mem = Array::create_array(Array::type_Normal, context_flag, nb_columns, 0, m_top.get_alloc());
        m_opposite_table.init_from_mem(mem);
        m_opposite_table.update_parent();
        mem = Array::create_array(Array::type_Normal, context_flag, nb_columns, 0, m_top.get_alloc());
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
}


ColKey Table::do_insert_column(ColKey col_key, DataType type, StringData name, LinkTargetInfo& link_target_info,
                               bool nullable, bool listtype, LinkType link_type)
{
    if (type == type_Link)
        nullable = true;

    bump_storage_version();
    col_key = insert_root_column(col_key, type, name, link_target_info, nullable, listtype, link_type); // Throws

    if (Replication* repl = get_repl())
        repl->insert_column(this, col_key, type, name, link_target_info, nullable, listtype, link_type); // Throws

    return col_key;
}


void Table::populate_search_index(ColKey col_key)
{
    auto col_ndx = col_key.get_index().val;
    StringIndex* index = m_index_accessors[col_ndx];

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
        else {
            REALM_ASSERT_RELEASE(false && "Data type does not support search index");
        }
    }
}

void Table::add_search_index(ColKey col_key)
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("No such column");
    size_t column_ndx = col_key.get_index().val;

    // Early-out if already indexed
    if (m_index_accessors[column_ndx] != nullptr)
        return;

    if (!StringIndex::type_supported(DataType(col_key.get_type()))) {
        // FIXME: This is what we used to throw, so keep throwing that for compatibility reasons, even though it
        // should probably be a type mismatch exception instead.
        throw LogicError(LogicError::illegal_combination);
    }

    // m_index_accessors always has the same number of pointers as the number of columns. Columns without search
    // index have 0-entries.
    REALM_ASSERT(m_index_accessors.size() == m_leaf_ndx2colkey.size());
    REALM_ASSERT(m_index_accessors[column_ndx] == nullptr);

    // Create the index
    StringIndex* index = new StringIndex(ClusterColumn(&m_clusters, col_key), get_alloc()); // Throws
    m_index_accessors[column_ndx] = index;

    // Insert ref to index
    index->set_parent(&m_index_refs, column_ndx);
    m_index_refs.set(column_ndx, index->get_ref()); // Throws

    // Update spec
    auto spec_ndx = leaf_ndx2spec_ndx(col_key.get_index());
    auto attr = m_spec.get_column_attr(spec_ndx);
    attr.set(col_attr_Indexed);
    m_spec.set_column_attr(spec_ndx, attr); // Throws

    populate_search_index(col_key);
}

void Table::remove_search_index(ColKey col_key)
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("No such column");
    auto column_ndx = col_key.get_index();

    // Early-out if non-indexed
    if (m_index_accessors[column_ndx.val] == nullptr)
        return;

    // Destroy and remove the index column
    StringIndex* index = m_index_accessors[column_ndx.val];
    REALM_ASSERT(index != nullptr);
    index->destroy();
    delete index;
    m_index_accessors[column_ndx.val] = nullptr;

    m_index_refs.set(column_ndx.val, 0);

    // update spec
    auto spec_ndx = leaf_ndx2spec_ndx(column_ndx);
    auto attr = m_spec.get_column_attr(spec_ndx);
    attr.reset(col_attr_Indexed);
    m_spec.set_column_attr(spec_ndx, attr); // Throws
}

void Table::enumerate_string_column(ColKey col_key)
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("No such column");
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

ColKey Table::insert_root_column(ColKey col_key, DataType type, StringData name, LinkTargetInfo& link_target,
                                 bool nullable, bool listtype, LinkType link_type)
{
    col_key = do_insert_root_column(col_key, ColumnType(type), name, nullable, listtype, link_type); // Throws

    // When the inserted column is a link-type column, we must also add a
    // backlink column to the target table.

    if (link_target.is_valid()) {
        auto target_table_key = link_target.m_target_table->get_key();
        auto origin_table_key = get_key();
        link_target.m_backlink_col_key = link_target.m_target_table->insert_backlink_column(
            origin_table_key, col_key, link_target.m_backlink_col_key); // Throws
        link_target.m_target_table->report_invalid_key(link_target.m_backlink_col_key);
        set_opposite_column(col_key, target_table_key, link_target.m_backlink_col_key);
        // backlink metadata in opposite table is set by insert_backlink_column...
    }
    return col_key;
}


void Table::erase_root_column(ColKey col_key)
{
    REALM_ASSERT(valid_column(col_key));
    ColumnType col_type = col_key.get_type();
    if (is_link_type(col_type)) {
        auto target_table = get_opposite_table(col_key);
        auto target_column = get_opposite_column(col_key);
        target_table->erase_backlink_column(target_column);
    }
    do_erase_root_column(col_key); // Throws
}


ColKey Table::do_insert_root_column(ColKey col_key, ColumnType type, StringData name, bool nullable, bool listtype,
                                    LinkType link_type)
{
    // if col_key specifies a key, it must be unused
    REALM_ASSERT(!col_key || !valid_column(col_key));

    // locate insertion point: ordinary columns must come before backlink columns
    size_t spec_ndx = (type == col_type_BackLink) ? m_spec.get_column_count() : m_spec.get_public_column_count();

    int attr = col_attr_None;
    if (nullable)
        attr |= col_attr_Nullable;
    if (listtype)
        attr |= col_attr_List;
    if (link_type == link_Strong)
        attr |= col_attr_StrongLinks;
    // if col_key does not specify a key, one must be generated
    if (!col_key) {
        col_key = generate_col_key(type, attr);
    }
    else {
        REALM_ASSERT(false); // FIXME
    }

    m_spec.insert_column(spec_ndx, col_key, type, name, attr); // Throws
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

    return col_key;
}


void Table::do_erase_root_column(ColKey col_key)
{
    size_t col_ndx = col_key.get_index().val;
    // If the column had a source index we have to remove and destroy that as well
    ref_type index_ref = m_index_refs.get_as_ref(col_ndx);
    if (index_ref) {
        Array::destroy_deep(index_ref, m_index_refs.get_alloc());
    }
    delete m_index_accessors[col_ndx];
    m_index_refs.set(col_ndx, 0);
    m_opposite_table.set(col_ndx, 0);
    m_opposite_column.set(col_ndx, 0);
    m_index_accessors[col_ndx] = nullptr;
    m_clusters.remove_column(col_key);
    size_t spec_ndx = colkey2spec_ndx(col_key);
    m_spec.erase_column(spec_ndx);
    m_top.adjust(top_position_for_column_key, 2);

    build_column_mapping();
}

LinkType Table::get_link_type(ColKey col_key) const
{
    auto type = col_key.get_type();
    if (!(type == col_type_Link) && !(type == col_type_LinkList)) {
        throw LogicError{LogicError::illegal_type};
    }
    return col_key.get_attrs().test(col_attr_StrongLinks) ? link_Strong : link_Weak;
}

ColKey Table::insert_backlink_column(TableKey origin_table_key, ColKey origin_col_key, ColKey backlink_col_key)
{
    ColKey retval = do_insert_root_column(backlink_col_key, col_type_BackLink, ""); // Throws
    set_opposite_column(retval, origin_table_key, origin_col_key);
    return retval;
}


void Table::erase_backlink_column(ColKey backlink_col_key)
{
    bump_content_version();
    bump_storage_version();
    do_erase_root_column(backlink_col_key); // Throws
}


void Table::detach() noexcept
{
    m_alloc.bump_instance_version();
}

void Table::fully_detach() noexcept
{
    m_next_key_value = -1; // trigger recomputation on next use
    m_spec.detach();
    m_top.detach();
    for (auto& index : m_index_accessors) {
        delete index;
    }
    m_index_refs.detach();
    m_opposite_table.detach();
    m_opposite_column.detach();
    m_index_accessors.clear();
}


Table::~Table() noexcept
{
    // If destroyed as a standalone table, destroy all memory allocated
    if (m_top.get_parent() == nullptr) {
        m_top.destroy_deep();
    }

    if (m_top.is_attached()) {
        fully_detach();
    }

    for (auto& index : m_index_accessors) {
        delete index;
    }
    m_index_accessors.clear();
}


bool Table::has_search_index(ColKey col_key) const noexcept
{
    return m_index_accessors[col_key.get_index().val] != nullptr;
}

void Table::migrate_column_info(std::function<void()> commit_and_continue)
{
    bool changes = false;
    changes |= m_spec.convert_column_attributes();
    changes |= m_spec.convert_column_keys(get_key());

    if (changes) {
        build_column_mapping();
        commit_and_continue();
    }
}

// Delete the indexes stored in the columns array and create corresponding
// entries in m_index_accessors array. This also has the effect that the columns
// array after this step does not have extra entries for certain columns
void Table::migrate_indexes(std::function<void()> commit_and_continue)
{
    if (ref_type top_ref = m_top.get_as_ref(top_position_for_columns)) {
        bool changes = false;
        Array col_refs(m_alloc);
        col_refs.set_parent(&m_top, top_position_for_columns);
        col_refs.init_from_ref(top_ref);

        for (size_t col_ndx = 0; col_ndx < m_spec.get_column_count(); col_ndx++) {
            if (m_spec.get_column_attr(col_ndx).test(col_attr_Indexed) && !m_index_refs.get(col_ndx)) {
                auto old_index_ref = col_refs.get(col_ndx + 1);

                // Delete old index
                Array::destroy_deep(old_index_ref, m_alloc);

                // Create new one
                StringIndex* index =
                    new StringIndex(ClusterColumn(&m_clusters, m_spec.get_key(col_ndx)), get_alloc()); // Throws
                m_index_accessors[col_ndx] = index;
                index->set_parent(&m_index_refs, col_ndx);
                m_index_refs.set(col_ndx, index->get_ref());

                // Simply delete entry. This will have the effect that we will not have to take
                // extra entries into account
                col_refs.erase(col_ndx + 1);
                changes = true;
            }
        };
        if (changes) {
            commit_and_continue();
        }
    }
}

// Move information held in the subspec area into the structures managed by Table
// This is information about origin/target tables in relation to links
// This information is now held in "opposite" arrays directly in Table structure
// At the same time the backlink columns are destroyed
void Table::migrate_subspec(std::function<void()> commit_and_continue)
{
    if (!m_spec.has_subspec())
        return;

    if (ref_type top_ref = m_top.get_as_ref(top_position_for_columns)) {
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
                if (auto ref = col_refs.get(col_ndx)) {
                    Array::destroy_deep(ref, m_alloc);
                    col_refs.set(col_ndx, 0);
                }
            }
        };
    }
    m_spec.destroy_subspec();
    commit_and_continue();
}


void Table::convert_links_from_ndx_to_key(std::function<void()> commit_and_continue)
{
    if (ref_type top_ref = m_top.get_as_ref(top_position_for_columns)) {
        bool changes = false;
        Array col_refs(m_alloc);
        col_refs.set_parent(&m_top, top_position_for_columns);
        col_refs.init_from_ref(top_ref);

        auto convert_links = [&](ColKey col_key) {
            auto col_type = col_key.get_type();
            if (is_link_type(col_type)) {
                auto target_table = get_opposite_table(col_key);
                ColKey oid_col = target_table->get_column_key("!OID");

                // If the target table has an !OID column, all it's objects will be stored
                // with the key present in this column. As links nowadays are represented by
                // target key values - and not indices into the table - we will need to
                // replace all old indices with the corresponding key values.
                if (oid_col) {
                    BPlusTree<Int> oid_column(m_alloc);
                    oid_column.init_from_ref(target_table->get_oid_column_ref());

                    BPlusTree<Int> link_column(m_alloc);
                    link_column.set_parent(&col_refs, col_key.get_index().val);
                    link_column.init_from_parent();
                    auto link_column_size = link_column.size();

                    for (size_t row_ndx = 0; row_ndx < link_column_size; row_ndx++) {
                        if (int64_t val = link_column.get(row_ndx)) {
                            // Link is not null
                            int64_t new_val;
                            if (col_type == col_type_LinkList) {
                                BPlusTree<Int> link_list(m_alloc);
                                link_list.init_from_ref(ref_type(val));
                                size_t link_list_sz = link_list.size();
                                for (size_t j = 0; j < link_list_sz; j++) {
                                    int64_t link = link_list.get(j);
                                    int64_t key_val = oid_column.get(link);
                                    if (key_val != link) {
                                        link_list.set(j, key_val);
                                    }
                                }
                                // If array was cow, the ref has changed
                                new_val = link_list.get_ref();
                            }
                            else {
                                new_val = oid_column.get(val - 1) + 1;
                            }
                            if (new_val != val) {
                                link_column.set(row_ndx, new_val);
                                changes = true;
                            }
                        }
                    }
                }
            }
            return false;
        };

        for_each_public_column(convert_links);

        if (changes) {
            commit_and_continue();
        }
    }
}

ref_type Table::get_oid_column_ref() const
{
    Array col_refs(m_alloc);
    col_refs.init_from_ref(m_top.get(top_position_for_columns));
    return col_refs.get(0);
}

namespace {

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
                if (val) {
                    return Mixed{*val};
                }
            }
            else {
                return Mixed{static_cast<BPlusTree<Int>*>(accessor)->get(ndx)};
            }
            break;
        case col_type_Bool:
            if (nullable) {
                auto val = static_cast<BPlusTree<util::Optional<Int>>*>(accessor)->get(ndx);
                if (val) {
                    return Mixed{bool(*val)};
                }
            }
            else {
                return Mixed{bool(static_cast<BPlusTree<Int>*>(accessor)->get(ndx))};
            }
            break;
        case col_type_Float:
            return Mixed{static_cast<BPlusTree<float>*>(accessor)->get(ndx)};
            break;
        case col_type_Double:
            return Mixed{static_cast<BPlusTree<double>*>(accessor)->get(ndx)};
            break;
        case col_type_String:
            return Mixed{static_cast<BPlusTree<String>*>(accessor)->get(ndx)};
            break;
        case col_type_Binary:
            return Mixed{static_cast<BPlusTree<Binary>*>(accessor)->get(ndx)};
            break;
        default:
            break;
    }

    return Mixed{};
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

void Table::create_columns(std::function<void()> commit_and_continue)
{
    size_t cnt;
    ClusterTree::TraverseFunction get_column_cnt = [&cnt](const Cluster* cluster) {
        cnt = cluster->nb_columns();
        return true;
    };
    traverse_clusters(get_column_cnt);

    size_t column_count = m_spec.get_column_count();
    if (cnt != column_count) {
        for (size_t col_ndx = 0; col_ndx < column_count; col_ndx++) {
            m_clusters.insert_column(m_spec.get_key(col_ndx));
        }
        commit_and_continue();
    }
}

void Table::migrate_objects(std::function<void()> commit_and_continue)
{
    ref_type top_ref = m_top.get_as_ref(top_position_for_columns);
    if (!top_ref) {
        // All objects migrated
        return;
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
    size_t number_of_objects = size_t(-1);
    auto update_size = [&number_of_objects](size_t s) {
        if (number_of_objects == size_t(-1)) {
            number_of_objects = s;
        }
        else {
            REALM_ASSERT(s == number_of_objects);
        }
    };

    size_t nb_columns = m_spec.get_public_column_count();
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        ColKey col_key = m_spec.get_key(col_ndx);
        ColumnAttrMask attr = m_spec.get_column_attr(col_ndx);
        ColumnType col_type = m_spec.get_column_type(col_ndx);
        std::unique_ptr<BPlusTreeBase> acc;
        std::unique_ptr<LegacyTS> ts_acc;
        std::unique_ptr<BPlusTree<int64_t>> list_acc;

        REALM_ASSERT(col_refs.get(col_ndx));

        if (attr.test(col_attr_List) && col_type != col_type_LinkList) {
            list_acc = std::make_unique<BPlusTree<int64_t>>(m_alloc);
        }
        else {
            switch (col_type) {
                case col_type_Int:
                case col_type_Bool:
                    if (attr.test(col_attr_Nullable)) {
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
                    acc = std::make_unique<BPlusTree<String>>(m_alloc);
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

    if (m_clusters.size() == number_of_objects)
        return;

    /******************** Optionally create !OID accessor ********************/

    BPlusTree<Int>* oid_column = nullptr;
    std::unique_ptr<BPlusTreeBase> oid_column_store;
    if (m_spec.get_column_name(0) == "!OID") {
        // The !OID column is needed in the next stage as well, so it must be
        // deleted in this stage, so move it from column_accessors to oid_column_store.
        auto col0 = m_spec.get_key(0);
        oid_column_store = std::move(column_accessors[col0]);

        column_accessors.erase(col0);
        oid_column = dynamic_cast<BPlusTree<Int>*>(oid_column_store.get());
        REALM_ASSERT(oid_column);

        // Delete '0' from cols_to_destroy
        auto first = cols_to_destroy.begin();
        REALM_ASSERT(*first == 0);
        cols_to_destroy.erase(first);
    }

    /*************************** Create objects ******************************/

    for (size_t row_ndx = 0; row_ndx < number_of_objects; row_ndx++) {

        // Build a vector of values obtained from the old columns
        FieldValues init_values;
        for (auto& it : column_accessors) {
            auto col_key = it.first;
            auto col_type = col_key.get_type();
            auto nullable = col_key.get_attrs().test(col_attr_Nullable);
            init_values.emplace_back(col_key, get_val_from_column(row_ndx, col_type, nullable, it.second.get()));
        }
        for (auto& it : ts_accessors) {
            init_values.emplace_back(it.first, Mixed(it.second->get(row_ndx)));
        }

        // Key will either be equal to the old row number or be based on value from !OID column
        ObjKey obj_key(oid_column ? oid_column->get(row_ndx) : row_ndx);
        // Create object with the initial values
        Obj obj = create_object(obj_key, init_values);

        // Then update possible list types
        for (auto& it : list_accessors) {
            switch (it.first.get_type()) {
                case col_type_Int:
                case col_type_Bool:
                    copy_list<int64_t>(to_ref(it.second->get(row_ndx)), obj, it.first, m_alloc);
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

    // Destroy values in the old columns
    for (auto col_ndx : cols_to_destroy) {
        Array::destroy_deep(col_refs.get(col_ndx), m_alloc);
        col_refs.set(col_ndx, 0);
    }

    if (!has_link_columns) {
        // No link columns to update - mark that we are done with this table
        finalize_migration();
    }

    commit_and_continue();
#if 0
    if (fastrand(100) < 20) {
        throw util::runtime_error("Upgrade interrupted");
    }
#endif
}

void Table::migrate_links(std::function<void()> commit_and_continue)
{
    ref_type top_ref = m_top.get_as_ref(top_position_for_columns);
    if (!top_ref) {
        // All objects migrated
        return;
    }

    Array col_refs(m_alloc);
    col_refs.set_parent(&m_top, top_position_for_columns);
    col_refs.init_from_ref(top_ref);

    // Create column accessors
    size_t nb_columns = m_spec.get_public_column_count();
    std::vector<std::unique_ptr<BPlusTree<Int>>> link_column_accessors(nb_columns);
    std::vector<ColKey> col_keys(nb_columns);
    std::vector<ColumnType> col_types(nb_columns);
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        ColumnType col_type = m_spec.get_column_type(col_ndx);

        if (is_link_type(col_type)) {
            link_column_accessors[col_ndx] = std::make_unique<BPlusTree<int64_t>>(m_alloc);
            link_column_accessors[col_ndx]->set_parent(&col_refs, col_ndx);
            link_column_accessors[col_ndx]->init_from_parent();
            col_keys[col_ndx] = m_spec.get_key(col_ndx);
            col_types[col_ndx] = col_type;
        }
    }

    size_t sz = size_t(-1);
    for (auto& acc : link_column_accessors) {
        if (acc) {
            if (sz == size_t(-1)) {
                sz = acc->size();
            }
            else {
                REALM_ASSERT(acc->size() == sz);
            }
        }
    }
    bool use_oid = (m_spec.get_column_name(0) == "!OID");
    if (sz != size_t(-1)) {
        BPlusTree<Int> oid_column(m_alloc);
        if (use_oid) {
            oid_column.init_from_ref(col_refs.get(0));
        }
        for (size_t i = 0; i < sz; i++) {
            ObjKey obj_key(use_oid ? oid_column.get(i) : i);
            Obj obj = get_object(obj_key);
            for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
                if (col_keys[col_ndx]) {
                    int64_t val = link_column_accessors[col_ndx]->get(i);
                    if (val) {
                        if (col_types[col_ndx] == col_type_Link) {
                            obj.set(col_keys[col_ndx], ObjKey(val - 1));
                        }
                        else {
                            auto ll = obj.get_linklist(col_keys[col_ndx]);
                            BPlusTree<Int> links(m_alloc);
                            links.init_from_ref(ref_type(val));
                            size_t nb_links = links.size();
                            for (size_t j = 0; j < nb_links; j++) {
                                ll.add(ObjKey(links.get(j)));
                            }
                        }
                    }
                }
            }
        }
    }

    finalize_migration();
    commit_and_continue();
}

void Table::finalize_migration()
{
    ref_type ref = m_top.get_as_ref(top_position_for_columns);
    if (ref) {
        Array::destroy_deep(ref, m_alloc);
        m_top.set(top_position_for_columns, 0);
    }
    if (m_spec.get_column_name(0) == "!OID") {
        remove_column(m_spec.get_key(0));
    }
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
        MemRef mem = ClusterTree::create_empty_cluster(alloc); // Throws
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
    top.add(rot);
    top.add(rot);
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
    return top.get_ref();
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

    if (m_spec.has_strong_link_columns() || (g && g->has_cascade_notification_handler())) {
        CascadeState state(CascadeState::Mode::strong);
        state.m_group = g;
        std::for_each(vec.begin(), vec.end(),
                      [this, &state](ObjKey k) { state.m_to_be_deleted.emplace_back(m_key, k); });
        remove_recursive(state);
    }
    else {
        CascadeState state(CascadeState::Mode::none);
        std::for_each(vec.begin(), vec.end(), [this, &state](ObjKey k) { m_clusters.erase(k, state); });
    }
}


void Table::clear()
{
    size_t old_size = size();

    m_clusters.clear();

    bump_content_version();
    bump_storage_version();

    if (Replication* repl = get_repl())
        repl->clear_table(this, old_size); // Throws
}


Group* Table::get_parent_group() const noexcept
{
    if (!m_top.is_attached())
        return 0;                                              // Subtable with shared descriptor
    ArrayParent* parent = m_top.get_parent();                  // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return 0; // Free-standing table

    return static_cast<Group*>(parent);
}


size_t Table::get_index_in_group() const noexcept
{
    if (!m_top.is_attached())
        return realm::npos;                                    // Subtable with shared descriptor
    ArrayParent* parent = m_top.get_parent();                  // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return realm::npos; // Free-standing table
    return m_top.get_ndx_in_parent();
}

TableKey Table::get_key() const noexcept
{
    return m_key;
}

TableRef Table::get_link_target(ColKey col_key) noexcept
{
    return get_opposite_table(col_key);
}

// count ----------------------------------------------

size_t Table::count_int(ColKey col_key, int64_t value) const
{
    size_t count;
    if (is_nullable(col_key)) {
        aggregate<act_Count, util::Optional<int64_t>, int64_t>(col_key, value, &count);
    }
    else {
        aggregate<act_Count, int64_t, int64_t>(col_key, value, &count);
    }
    return count;
}
size_t Table::count_float(ColKey col_key, float value) const
{
    size_t count;
    aggregate<act_Count, float, float>(col_key, value, &count);
    return count;
}
size_t Table::count_double(ColKey col_key, double value) const
{
    size_t count;
    aggregate<act_Count, double, double>(col_key, value, &count);
    return count;
}
size_t Table::count_string(ColKey col_key, StringData value) const
{
    size_t count;
    aggregate<act_Count, StringData, StringData>(col_key, value, &count);
    return count;
}

// sum ----------------------------------------------

int64_t Table::sum_int(ColKey col_key) const
{
    if (is_nullable(col_key)) {
        return aggregate<act_Sum, util::Optional<int64_t>, int64_t>(col_key);
    }
    return aggregate<act_Sum, int64_t, int64_t>(col_key);
}
double Table::sum_float(ColKey col_key) const
{
    return aggregate<act_Sum, float, double>(col_key);
}
double Table::sum_double(ColKey col_key) const
{
    return aggregate<act_Sum, double, double>(col_key);
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

// minimum ----------------------------------------------

#define USE_COLUMN_AGGREGATE 1

int64_t Table::minimum_int(ColKey col_key, ObjKey* return_ndx) const
{
    if (is_nullable(col_key)) {
        return aggregate<act_Min, util::Optional<int64_t>, int64_t>(col_key, 0, nullptr, return_ndx);
    }
    return aggregate<act_Min, int64_t, int64_t>(col_key, 0, nullptr, return_ndx);
}

float Table::minimum_float(ColKey col_key, ObjKey* return_ndx) const
{
    return aggregate<act_Min, float, float>(col_key, 0.f, nullptr, return_ndx);
}

double Table::minimum_double(ColKey col_key, ObjKey* return_ndx) const
{
    return aggregate<act_Min, double, double>(col_key, 0., nullptr, return_ndx);
}

Timestamp Table::minimum_timestamp(ColKey col_key, ObjKey* return_ndx) const
{
    return aggregate<act_Min, Timestamp, Timestamp>(col_key, Timestamp{}, nullptr, return_ndx);
}

// maximum ----------------------------------------------

int64_t Table::maximum_int(ColKey col_key, ObjKey* return_ndx) const
{
    if (is_nullable(col_key)) {
        return aggregate<act_Max, util::Optional<int64_t>, int64_t>(col_key, 0, nullptr, return_ndx);
    }
    return aggregate<act_Max, int64_t, int64_t>(col_key, 0, nullptr, return_ndx);
}

float Table::maximum_float(ColKey col_key, ObjKey* return_ndx) const
{
    return aggregate<act_Max, float, float>(col_key, 0.f, nullptr, return_ndx);
}

double Table::maximum_double(ColKey col_key, ObjKey* return_ndx) const
{
    return aggregate<act_Max, double, double>(col_key, 0., nullptr, return_ndx);
}

Timestamp Table::maximum_timestamp(ColKey col_key, ObjKey* return_ndx) const
{
    return aggregate<act_Max, Timestamp, Timestamp>(col_key, Timestamp{}, nullptr, return_ndx);
}

template <class T>
ObjKey Table::find_first(ColKey col_key, T value) const
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("Non-existing column");

    if (StringIndex* index = get_search_index(col_key)) {
        return index->find_first(value);
    }

    ObjKey key;
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    LeafType leaf(get_alloc());

    ClusterTree::TraverseFunction f = [&key, &col_key, &value, &leaf](const Cluster* cluster) {
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
ObjKey Table::find_first(ColKey col_key, ObjKey value) const
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("Non-existing column");

    ObjKey key;
    using LeafType = typename ColumnTypeTraits<ObjKey>::cluster_leaf_type;
    LeafType leaf(get_alloc());

    ClusterTree::TraverseFunction f = [&key, &col_key, &value, &leaf](const Cluster* cluster) {
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
}

// Explicitly instantiate the generic case of the template for the types we care about.
template ObjKey Table::find_first(ColKey col_key, bool) const;
template ObjKey Table::find_first(ColKey col_key, int64_t) const;
template ObjKey Table::find_first(ColKey col_key, float) const;
template ObjKey Table::find_first(ColKey col_key, double) const;
template ObjKey Table::find_first(ColKey col_key, util::Optional<bool>) const;
template ObjKey Table::find_first(ColKey col_key, util::Optional<int64_t>) const;
template ObjKey Table::find_first(ColKey col_key, BinaryData) const;



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

ObjKey Table::find_first_float(ColKey col_key, float value) const
{
    return find_first<Float>(col_key, value);
}

ObjKey Table::find_first_double(ColKey col_key, double value) const
{
    return find_first<Double>(col_key, value);
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

template <class T>
TableView Table::find_all(ColKey col_key, T value)
{
    return where().equal(col_key, value).find_all();
}

TableView Table::find_all_int(ColKey col_key, int64_t value)
{
    return find_all<int64_t>(col_key, value);
}

ConstTableView Table::find_all_int(ColKey col_key, int64_t value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_key, value);
}

TableView Table::find_all_bool(ColKey col_key, bool value)
{
    return find_all<bool>(col_key, value);
}

ConstTableView Table::find_all_bool(ColKey col_key, bool value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_key, value);
}


TableView Table::find_all_float(ColKey col_key, float value)
{
    return find_all<float>(col_key, value);
}

ConstTableView Table::find_all_float(ColKey col_key, float value) const
{
    return const_cast<Table*>(this)->find_all<float>(col_key, value);
}

TableView Table::find_all_double(ColKey col_key, double value)
{
    return find_all<double>(col_key, value);
}

ConstTableView Table::find_all_double(ColKey col_key, double value) const
{
    return const_cast<Table*>(this)->find_all<double>(col_key, value);
}

TableView Table::find_all_string(ColKey col_key, StringData value)
{
    return where().equal(col_key, value).find_all();
}

ConstTableView Table::find_all_string(ColKey col_key, StringData value) const
{
    return const_cast<Table*>(this)->find_all_string(col_key, value);
}

TableView Table::find_all_binary(ColKey, BinaryData)
{
    // FIXME: Implement this!
    throw util::runtime_error("Not implemented");
}

ConstTableView Table::find_all_binary(ColKey, BinaryData) const
{
    // FIXME: Implement this!
    throw util::runtime_error("Not implemented");
}

TableView Table::find_all_null(ColKey col_key)
{
    return where().equal(col_key, null{}).find_all();
}

ConstTableView Table::find_all_null(ColKey col_key) const
{
    return const_cast<Table*>(this)->find_all_null(col_key);
}

TableView Table::get_distinct_view(ColKey col_key)
{
    TableView tv(TableView::DistinctView, *this, col_key);
    tv.do_sync();
    return tv;
}

ConstTableView Table::get_distinct_view(ColKey col_key) const
{
    return const_cast<Table*>(this)->get_distinct_view(col_key);
}

TableView Table::get_sorted_view(ColKey col_key, bool ascending)
{
    TableView tv = where().find_all();
    tv.sort(col_key, ascending);
    return tv;
}

ConstTableView Table::get_sorted_view(ColKey col_key, bool ascending) const
{
    return const_cast<Table*>(this)->get_sorted_view(col_key, ascending);
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


const Table* Table::get_link_chain_target(const std::vector<ColKey>& link_chain) const
{
    const Table* table = this;
    for (size_t t = 0; t < link_chain.size(); t++) {
        // Link column can be a single Link, LinkList, or BackLink.
        REALM_ASSERT(table->valid_column(link_chain[t]));
        ColumnType type = table->get_real_column_type(link_chain[t]);
        if (type == col_type_LinkList || type == col_type_Link || type == col_type_BackLink) {
            table = table->get_opposite_table(link_chain[t]);
        }
        else {
            // Only last column in link chain is allowed to be non-link
            if (t + 1 != link_chain.size())
                throw(LogicError::type_mismatch);
        }
    }
    return table;
}


void Table::update_from_parent(size_t old_baseline) noexcept
{
    // There is no top for sub-tables sharing spec
    if (m_top.is_attached()) {
        if (!m_top.update_from_parent(old_baseline))
            return;

        m_spec.update_from_parent(old_baseline);
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
        if (m_top.size() > top_position_for_opposite_table)
            m_opposite_table.update_from_parent(old_baseline);
        if (m_top.size() > top_position_for_opposite_column)
            m_opposite_column.update_from_parent(old_baseline);
        refresh_content_version();
    }
    m_alloc.bump_storage_version();
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

void Table::refresh_accessor_tree()
{
    if (m_top.is_attached()) {
        // Root table (free-standing table, group-level table, or subtable with
        // independent descriptor)
        m_top.init_from_parent();
        m_spec.init_from_parent();
        if (m_top.size() > top_position_for_cluster_tree) {
            m_clusters.init_from_parent();
        }
        if (m_top.size() > top_position_for_search_indexes) {
            m_index_refs.init_from_parent();
        }
        if (m_top.size() > top_position_for_opposite_table) {
            m_opposite_table.init_from_parent();
        }
        if (m_top.size() > top_position_for_opposite_column) {
            m_opposite_column.init_from_parent();
        }
        refresh_content_version();
        bump_storage_version();
        build_column_mapping();
    }
    refresh_index_accessors();
}

void Table::refresh_index_accessors()
{
    // Refresh search index accessors

    // First eliminate any index accessors for eliminated last columns
    size_t col_ndx_end = m_leaf_ndx2colkey.size();
    for (size_t col_ndx = col_ndx_end; col_ndx < m_index_accessors.size(); col_ndx++) {
        if (m_index_accessors[col_ndx]) {
            delete m_index_accessors[col_ndx];
            m_index_accessors[col_ndx] = nullptr;
        }
    }
    m_index_accessors.resize(col_ndx_end);

    // Then eliminate/refresh/create accessors within column range
    // we can not use for_each_column() here, since the columns may have changed
    // and the index accessor vector is not updated correspondingly.
    for (size_t col_ndx = 0; col_ndx < col_ndx_end; col_ndx++) {

        bool has_old_accessor = m_index_accessors[col_ndx];
        ref_type ref = m_index_refs.get_as_ref(col_ndx);

        if (has_old_accessor && ref == 0) { // accessor drop
            delete m_index_accessors[col_ndx];
            m_index_accessors[col_ndx] = nullptr;
        }
        else if (has_old_accessor && ref != 0) { // still there, refresh:
            m_index_accessors[col_ndx]->refresh_accessor_tree();
        }
        else if (!has_old_accessor && ref != 0) { // new index!
            auto col_key = m_leaf_ndx2colkey[col_ndx];
            ClusterColumn virtual_col(&m_clusters, col_key);
            m_index_accessors[col_ndx] = new StringIndex(ref, &m_index_refs, col_ndx, virtual_col, get_alloc());
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
        m_spec.to_dot(out);
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
    size_t column_count = m_spec.get_column_count(); // We can print backlinks too.
    for (size_t i = 0; i < column_count; ++i) {
        std::string name = "backlink";
        if (i < get_column_count()) {
            name = m_spec.get_column_name(i);
        }
        std::cout << std::left << std::setw(10) << std::string(name).substr(0, 10) << " ";
    }

    // Types
    std::cout << "\n    ";
    for (size_t k = 0; k < column_count; ++k) {
        auto i = spec_ndx2colkey(k);
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
            case col_type_Link: {
                ConstTableRef target_table = get_opposite_table(i);
                const StringData target_name = target_table->get_name();
                std::cout << "L->" << std::setw(7) << std::string(target_name).substr(0, 7) << " ";
                break;
            }
            case col_type_LinkList: {
                ConstTableRef target_table = get_opposite_table(i);
                const StringData target_name = target_table->get_name();
                std::cout << "LL->" << std::setw(6) << std::string(target_name).substr(0, 6) << " ";
                break;
            }
            case col_type_BackLink: {
                ConstTableRef target_table = get_opposite_table(i);
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

        for (size_t k = 0; k < column_count; ++k) {
            auto n = spec_ndx2colkey(k);
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
                case col_type_OldStringEnum:
                case col_type_OldTable:
                case col_type_OldMixed:
                case col_type_OldDateTime:
                case col_type_Reserved4:
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

Obj Table::create_object(ObjKey key, const FieldValues& values)
{
    if (key == null_key) {
        if (m_next_key_value == -1 || is_valid(ObjKey(m_next_key_value))) {
            m_next_key_value = m_clusters.get_last_key_value() + 1;
        }
        key = ObjKey(m_next_key_value++);
    }

    REALM_ASSERT(key.value >= 0);

    bump_content_version();
    bump_storage_version();
    Obj obj = m_clusters.insert(key, values);

    return obj;
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

// Called by replication with mode = none
void Table::do_remove_object(ObjKey key)
{
    CascadeState state(CascadeState::Mode::none);
    state.m_to_be_deleted.emplace_back(m_key, key);
    remove_recursive(state);
}

void Table::remove_object(ObjKey key)
{
    Group* g = get_parent_group();

    if (m_spec.has_strong_link_columns() || (g && g->has_cascade_notification_handler())) {
        CascadeState state(CascadeState::Mode::strong);
        state.m_group = g;
        state.m_to_be_deleted.emplace_back(m_key, key);
        remove_recursive(state);
    }
    else {
        CascadeState state(CascadeState::Mode::none);
        m_clusters.erase(key, state);
    }
}

void Table::remove_object_recursive(ObjKey key)
{
    size_t table_ndx = get_index_in_group();
    if (table_ndx != realm::npos) {
        CascadeState state(CascadeState::Mode::all);
        state.m_to_be_deleted.emplace_back(m_key, key);
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
    // FIXME: Change this to be random number mixed with the TableKey.
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

Table::BacklinkOrigin Table::find_backlink_origin(StringData origin_table_name, StringData origin_col_name) const
    noexcept
{
    try {
        if (get_name() == origin_table_name) {
            ColKey linked_col_key = get_column_key(origin_col_name);
            if (linked_col_key != ColKey()) {
                return {{get_table_ref(), linked_col_key}};
            }
        }
        else {
            Group* current_group = get_parent_group();
            if (current_group) {
                ConstTableRef linked_table = current_group->get_table(origin_table_name);
                if (linked_table) {
                    ColKey linked_col_key = linked_table->get_column_key(origin_col_name);
                    if (linked_col_key != ColKey()) {
                        return {{linked_table, linked_col_key}};
                    }
                }
            }
        }
    }
    catch (...) {
        // not found, returning empty optional
    }
    return {};
}

Table::BacklinkOrigin Table::find_backlink_origin(ColKey backlink_col) const noexcept
{
    try {
        TableKey linked_table_key = get_opposite_table_key(backlink_col);
        ColKey linked_column_key = get_opposite_column(backlink_col);
        if (linked_table_key == m_key) {
            return {{get_table_ref(), linked_column_key}};
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
}

template <class F, class T>
void Table::change_nullability(ColKey key_from, ColKey key_to, bool throw_on_null)
{
    Allocator& allocator = this->get_alloc();
    bool from_nullability = is_nullable(key_from);
    ClusterTree::UpdateFunction func = [key_from, key_to, throw_on_null, from_nullability,
                                        &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();

        typename ColumnTypeTraits<F>::cluster_leaf_type from_arr(allocator);
        typename ColumnTypeTraits<T>::cluster_leaf_type to_arr(allocator);
        cluster->init_leaf(key_from, &from_arr);
        cluster->init_leaf(key_to, &to_arr);

        for (size_t i = 0; i < sz; i++) {
            if (from_nullability && from_arr.is_null(i)) {
                if (throw_on_null) {
                    throw realm::LogicError(realm::LogicError::column_not_nullable);
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
    ClusterTree::UpdateFunction func = [key_from, key_to, throw_on_null, from_nullability,
                                        &allocator](Cluster* cluster) {
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
                    if (!from_nullability || bptree_aggregate_not_null(v)) {
                        to_list.add(remove_optional(v));
                    }
                    else {
                        if (throw_on_null) {
                            throw realm::LogicError(realm::LogicError::column_not_nullable);
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
            default:
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
            default:
                REALM_UNREACHABLE();
                break;
        }
    }
}


ColKey Table::set_nullability(ColKey col_key, bool nullable, bool throw_on_null)
{
    if (is_nullable(col_key) == nullable)
        return col_key;

    bool si = has_search_index(col_key);
    std::string column_name(get_column_name(col_key));
    auto type = get_real_column_type(col_key);
    auto list = is_list(col_key);

    ColKey new_col = do_insert_root_column(ColKey(), type, "__temporary", nullable, list);

    try {
        convert_column(col_key, new_col, throw_on_null);
    }
    catch (LogicError&) {
        // remove any partially filled column
        remove_column(new_col);
        throw;
    }

    bump_content_version();
    bump_storage_version();

    erase_root_column(col_key);
    m_spec.rename_column(colkey2spec_ndx(new_col), column_name);

    if (si)
        add_search_index(new_col);

    return new_col;
}

void Table::set_opposite_column(ColKey col_key, TableKey opposite_table, ColKey opposite_column)
{
    m_opposite_table.set(col_key.get_index().val, opposite_table.value);
    m_opposite_column.set(col_key.get_index().val, opposite_column.value);
}

TableKey Table::get_opposite_table_key(ColKey col_key) const
{
    return TableKey(m_opposite_table.get(col_key.get_index().val));
}

TableRef Table::get_opposite_table(ColKey col_key) const
{
    return get_parent_group()->get_table(get_opposite_table_key(col_key));
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
