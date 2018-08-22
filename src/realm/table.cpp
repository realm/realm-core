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

// fixme, we need to gather all these typetraits definitions to just 1 single

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
    auto retval = do_insert_column(col_key, type, name, link_target_info, false, type == type_LinkList); // Throws

    set_link_type(retval, link_type); // Throws
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

    auto col_ndx = colkey2ndx(col_key);
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

    // size_t columns_ndx_in_parent = 1;
    // columns no longer in use
    while (m_top.size() <= top_position_for_version) {
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

    m_index_refs.set_parent(&m_top, top_position_for_search_indexes);
    if (m_top.get_as_ref(top_position_for_search_indexes) == 0) {
        // This is an upgrade - create the necessary arrays
        bool context_flag = false;
        size_t nb_columns = m_spec.get_public_column_count();
        MemRef mem = Array::create_array(Array::type_HasRefs, context_flag, nb_columns, 0, m_top.get_alloc());
        m_index_refs.init_from_mem(mem);
        m_index_refs.update_parent();
    }
    else {
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

    // update column mapping
    m_ndx2colkey.clear();
    m_colkey2ndx.clear();
    size_t num_cols = m_spec.get_column_count();
    for (size_t ndx = 0; ndx < num_cols; ++ndx) {
        ColKey col_key = m_spec.get_key(ndx);
        insert_col_mapping(ndx, col_key);
    }
}


ColKey Table::do_insert_column(ColKey col_key, DataType type, StringData name, LinkTargetInfo& link_target_info,
                               bool nullable, bool listtype)
{
    if (type == type_Link)
        nullable = true;

    bump_storage_version();
    col_key = insert_root_column(col_key, type, name, link_target_info, nullable, listtype); // Throws

    if (Replication* repl = get_repl())
        repl->insert_column(this, col_key, type, name, link_target_info, nullable, listtype); // Throws

    return col_key;
}


void Table::populate_search_index(ColKey col_key)
{
    size_t col_ndx = colkey2ndx(col_key);
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
    size_t column_ndx = colkey2ndx(col_key);

    ColumnAttrMask attr = m_spec.get_column_attr(column_ndx);

    // Early-out of already indexed
    if (attr.test(col_attr_Indexed))
        return;

    if (!StringIndex::type_supported(get_column_type(col_key))) {
        // FIXME: This is what we used to throw, so keep throwing that for compatibility reasons, even though it
        // should probably be a type mismatch exception instead.
        throw LogicError(LogicError::illegal_combination);
    }

    // m_index_accessors always has the same number of pointers as the number of columns. Columns without search
    // index have 0-entries.
    REALM_ASSERT(m_index_accessors.size() == get_column_count());
    REALM_ASSERT(m_index_accessors[column_ndx] == nullptr);

    // Create the index
    StringIndex* index = new StringIndex(ClusterColumn(&m_clusters, col_key), get_alloc()); // Throws
    m_index_accessors[column_ndx] = index;

    // Insert ref to index
    index->set_parent(&m_index_refs, column_ndx);
    m_index_refs.set(column_ndx, index->get_ref()); // Throws

    populate_search_index(col_key);

    // Mark the column as having an index
    attr = m_spec.get_column_attr(column_ndx);
    attr.set(col_attr_Indexed);
    m_spec.set_column_attr(column_ndx, attr); // Throws
}

void Table::remove_search_index(ColKey col_key)
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("No such column");
    size_t column_ndx = colkey2ndx(col_key);

    ColumnAttrMask attr = m_spec.get_column_attr(column_ndx);

    // Early-out of non-indexed
    if (!attr.test(col_attr_Indexed))
        return;

    // Destroy and remove the index column
    StringIndex* index = m_index_accessors[column_ndx];
    REALM_ASSERT(index != nullptr);
    index->destroy();
    delete index;
    m_index_accessors[column_ndx] = nullptr;

    m_index_refs.set(column_ndx, 0);

    // Mark the column as no longer having an index
    attr = m_spec.get_column_attr(column_ndx);
    attr.reset(col_attr_Indexed);
    m_spec.set_column_attr(column_ndx, attr); // Throws
}

void Table::enumerate_string_column(ColKey col_key)
{
    if (REALM_UNLIKELY(!valid_column(col_key)))
        throw InvalidKey("No such column");
    size_t column_ndx = colkey2ndx(col_key);
    ColumnType type = m_spec.get_column_type(column_ndx);
    if (type == col_type_String && !m_spec.is_string_enum_type(column_ndx)) {
        m_clusters.enumerate_string_column(column_ndx);
    }
}

bool Table::is_enumerated(ColKey col_key) const noexcept
{
    size_t col_ndx = colkey2ndx(col_key);
    return m_spec.is_string_enum_type(col_ndx);
}

size_t Table::get_num_unique_values(ColKey col_key) const
{
    if (!is_enumerated(col_key))
        return 0;

    ArrayParent* parent;
    ref_type ref = const_cast<Spec&>(m_spec).get_enumkeys_ref(colkey2ndx(col_key), parent);
    BPlusTree<StringData> col(get_alloc());
    col.init_from_ref(ref);

    return col.size();
}

ColKey Table::insert_root_column(ColKey col_key, DataType type, StringData name, LinkTargetInfo& link_target,
                                 bool nullable, bool listtype)
{
    col_key = do_insert_root_column(col_key, ColumnType(type), name, nullable, listtype); // Throws
    size_t col_ndx = colkey2ndx(col_key);

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
        m_spec.set_opposite_link_table_key(col_ndx, target_table_key); // Throws
    }

    if (link_target.is_valid()) {
        auto origin_table_key = get_key();
        link_target.m_backlink_col_key = link_target.m_target_table->insert_backlink_column(
            origin_table_key, col_key, link_target.m_backlink_col_key); // Throws
    }
    return col_key;
}


void Table::erase_root_column(ColKey col_key)
{
    REALM_ASSERT(valid_column(col_key));
    size_t col_ndx = colkey2ndx(col_key);
    ColumnType col_type = m_spec.get_column_type(col_ndx);
    if (is_link_type(col_type)) {
        auto target_table_key = m_spec.get_opposite_link_table_key(col_ndx);
        auto link_target_table = get_parent_group()->get_table(target_table_key);
        auto origin_table_key = get_key();
        link_target_table->erase_backlink_column(origin_table_key, col_key); // Throws
    }

    do_erase_root_column(col_key); // Throws
}


ColKey Table::do_insert_root_column(ColKey col_key, ColumnType type, StringData name, bool nullable, bool listtype)
{
    // if col_key specifies a key, it must be unused
    REALM_ASSERT(!col_key || !valid_column(col_key));

    // if col_key does not specify a key, one must be generated
    if (!col_key) {
        col_key = generate_col_key();
    }

    // locate insertion point: ordinary columns must come before backlink columns
    size_t ndx = (type == col_type_BackLink) ? m_spec.get_column_count() : m_spec.get_public_column_count();

    int attr = col_attr_None;
    if (nullable)
        attr |= col_attr_Nullable;
    if (listtype)
        attr |= col_attr_List;
    m_spec.insert_column(ndx, col_key, type, name, attr); // Throws

    // Backlink columns don't have search index
    if (type != col_type_BackLink) {
        // Column has no search index
        m_index_refs.insert(ndx, 0);
        m_index_accessors.insert(m_index_accessors.begin() + ndx, nullptr);
    }

    m_clusters.insert_column(ndx);

    insert_col_mapping(ndx, col_key);
    return col_key;
}


void Table::do_erase_root_column(ColKey col_key)
{
    size_t ndx = colkey2ndx(col_key);
    bool removing_public_column = ndx < m_spec.get_public_column_count(); // cache before changing spec below
    remove_col_mapping(ndx);
    m_spec.erase_column(ndx); // Throws

    if (removing_public_column) {
        // If the column had a source index we have to remove and destroy that as well
        ref_type index_ref = m_index_refs.get_as_ref(ndx);
        if (index_ref) {
            Array::destroy_deep(index_ref, m_index_refs.get_alloc());
        }
        m_index_refs.erase(ndx);
        delete m_index_accessors[ndx];
        m_index_accessors.erase(m_index_accessors.begin() + ndx);
        for (size_t i = ndx; i < m_index_accessors.size(); ++i) {
            if (StringIndex* index = m_index_accessors[i])
                index->set_ndx_in_parent(i);
        }
    }

    m_clusters.remove_column(ndx);
}


void Table::set_link_type(ColKey col_key, LinkType link_type)
{
    bool weak_links = false;
    switch (link_type) {
        case link_Strong:
            break;
        case link_Weak:
            weak_links = true;
            break;
    }

    size_t col_ndx = colkey2ndx(col_key);
    ColumnAttrMask attr = m_spec.get_column_attr(col_ndx);
    ColumnAttrMask new_attr{attr};
    new_attr.reset(col_attr_StrongLinks);
    if (!weak_links)
        new_attr.set(col_attr_StrongLinks);
    if (new_attr == attr)
        return;
    m_spec.set_column_attr(col_ndx, new_attr);

    if (Replication* repl = get_repl())
        repl->set_link_type(this, col_key, link_type); // Throws
}

LinkType Table::get_link_type(ColKey col_key) const
{
    size_t col_ndx = colkey2ndx(col_key);
    if (!(m_spec.get_column_type(col_ndx) == col_type_Link) &&
        !(m_spec.get_column_type(col_ndx) == col_type_LinkList)) {
        throw LogicError{LogicError::illegal_type};
    }
    return m_spec.get_column_attr(col_ndx).test(col_attr_StrongLinks) ? link_Strong : link_Weak;
}

ColKey Table::insert_backlink_column(TableKey origin_table_key, ColKey origin_col_key, ColKey backlink_col_key)
{
    ColKey retval = do_insert_root_column(backlink_col_key, col_type_BackLink, ""); // Throws
    size_t backlink_col_ndx = colkey2ndx(retval);
    m_spec.set_opposite_link_table_key(backlink_col_ndx, origin_table_key); // Throws
    m_spec.set_backlink_origin_column(backlink_col_ndx, origin_col_key);    // Throws
    return retval;
}


void Table::erase_backlink_column(TableKey origin_table_key, ColKey origin_col_key)
{
    size_t backlink_col_ndx = m_spec.find_backlink_column(origin_table_key, origin_col_key);
    REALM_ASSERT_3(backlink_col_ndx, !=, realm::not_found);
    bump_content_version();
    bump_storage_version();
    ColKey backlink_col_key = ndx2colkey(backlink_col_ndx);
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
    size_t col_ndx = colkey2ndx(col_key);
    ColumnAttrMask attr = m_spec.get_column_attr(col_ndx);
    return attr.test(col_attr_Indexed);
}


bool Table::convert_columns()
{
    bool changes = false;
    size_t nb_columns = m_spec.get_column_count();
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        changes |= m_spec.convert_column(col_ndx);
    }
    return changes;
}

namespace {
template <class T>
size_t get_size_from_ref(ref_type ref, Allocator& alloc)
{
    T arr(alloc);
    arr.init_from_ref(ref);
    return arr.size();
}

// Get size from old columns in file format 9 files
size_t get_size_from_ref_and_type(ColumnType col_type, ColumnAttrMask attr, ref_type col_ref, Allocator& alloc)
{
    // Determine the size of the table based on the size of the first column
    size_t sz;
    if (attr.test(col_attr_List)) {
        sz = ::get_size_from_ref<BPlusTree<int64_t>>(col_ref, alloc);
    }
    else {
        switch (col_type) {
            case col_type_Int:
            case col_type_Bool:
                if (attr.test(col_attr_Nullable)) {
                    sz = ::get_size_from_ref<BPlusTree<util::Optional<int64_t>>>(col_ref, alloc);
                }
                else {
                    sz = ::get_size_from_ref<BPlusTree<int64_t>>(col_ref, alloc);
                }
                break;
            case col_type_Float:
            case col_type_Double:
            case col_type_Link:
                // These types are implemented using a standard array
                sz = ::get_size_from_ref<BPlusTree<int64_t>>(col_ref, alloc);
                break;
            case col_type_String:
            case col_type_Binary:
                // These two types are similar in design
                sz = ::get_size_from_ref<BPlusTree<StringData>>(col_ref, alloc);
                break;
            case col_type_Timestamp: {
                Array arr(alloc);
                arr.init_from_ref(col_ref);
                ref_type ref = arr.get_as_ref(0);
                sz = ::get_size_from_ref<BPlusTree<util::Optional<int64_t>>>(ref, alloc);
                break;
            }
            default:
                REALM_UNREACHABLE();
                break;
        }
    }
    return sz;
}
}

bool Table::create_objects()
{
    ref_type ref = m_top.get_as_ref(top_position_for_columns);
    // If this ref is zero, then all columns have been copied.
    if (ref) {
        Array col_refs(m_alloc);
        col_refs.init_from_ref(ref);
        ref_type first_col_ref = col_refs.get_as_ref(0);
        if (first_col_ref) {
            ColumnType first_col_type = m_spec.get_column_type(0);
            auto first_col_attr = m_spec.get_column_attr(0);

            size_t sz = get_size_from_ref_and_type(first_col_type, first_col_attr, first_col_ref, m_alloc);

#ifdef REALM_DEBUG
            // Check that we get the same size for all columns
            size_t nb_cols = m_spec.get_public_column_count();
            size_t ndx_in_parent = 0;
            for (size_t i = 0; i < nb_cols; i++) {
                ref_type col_ref = col_refs.get_as_ref(ndx_in_parent);
                ColumnType col_type = m_spec.get_column_type(i);
                auto col_attr = m_spec.get_column_attr(i);
                size_t val = get_size_from_ref_and_type(col_type, col_attr, col_ref, m_alloc);
                REALM_ASSERT_DEBUG(val == sz);

                ndx_in_parent += m_spec.get_column_attr(i).test(col_attr_Indexed) ? 2 : 1;
            }
#endif

            if (m_clusters.size() != sz) {
                // Create all objects
                ClusterNode::State state;
                for (size_t i = 0; i < sz; i++) {
                    m_clusters.insert_fast(ObjKey(i), {}, state);
                }
                return true;
            }
        }
    }
    // Objects must have been created
    return false;
}

namespace {
template <class T>
void copy_column(ClusterTree& clusters, size_t col_ndx, ref_type col_ref, Allocator& allocator)
{
    BPlusTree<T> from_column(allocator);
    from_column.init_from_ref(col_ref);

    ClusterTree::UpdateFunction func = [col_ndx, &from_column, &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();
        size_t offset = size_t(cluster->get_offset());
        typename ColumnTypeTraits<T>::cluster_leaf_type arr(allocator);
        arr.create();
        for (size_t i = 0; i < sz; i++) {
            auto v = from_column.get(i + offset);
            arr.add(v);
        }
        cluster->add_leaf(col_ndx, arr.get_ref());
    };

    clusters.update(func);
}

template <>
void copy_column<util::Optional<bool>>(ClusterTree& clusters, size_t col_ndx, ref_type col_ref, Allocator& allocator)
{
    BPlusTree<util::Optional<int64_t>> from_column(allocator);
    from_column.init_from_ref(col_ref);

    ClusterTree::UpdateFunction func = [col_ndx, &from_column, &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();
        size_t offset = size_t(cluster->get_offset());
        ArrayBoolNull arr(allocator);
        arr.create();
        for (size_t i = 0; i < sz; i++) {
            util::Optional<bool> val;
            auto opt = from_column.get(i + offset);
            if (opt)
                val = opt.value();
            arr.add(val);
        }
        cluster->add_leaf(col_ndx, arr.get_ref());
    };

    clusters.update(func);
}

template <>
void copy_column<Timestamp>(ClusterTree& clusters, size_t col_ndx, ref_type col_ref, Allocator& allocator)
{
    Array top(allocator);
    top.init_from_ref(col_ref);
    BPlusTree<util::Optional<int64_t>> seconds(allocator);
    BPlusTree<int64_t> nano_seconds(allocator);
    seconds.init_from_ref(top.get_as_ref(0));
    nano_seconds.init_from_ref(top.get_as_ref(1));

    ClusterTree::UpdateFunction func = [col_ndx, &seconds, &nano_seconds, &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();
        size_t offset = size_t(cluster->get_offset());
        ArrayTimestamp arr(allocator);
        arr.create();
        for (size_t i = 0; i < sz; i++) {
            auto s = seconds.get(i + offset);
            if (s) {
                int32_t n = int32_t(nano_seconds.get(i + offset));
                arr.add(Timestamp(s.value(), n));
            }
            else {
                arr.add(Timestamp());
            }
        }
        cluster->add_leaf(col_ndx, arr.get_ref());
    };

    clusters.update(func);
}

void copy_column_backlink(ClusterTree& clusters, size_t col_ndx, ref_type col_ref, Allocator& allocator)
{
    BPlusTree<Int> list_refs(allocator);
    list_refs.init_from_ref(col_ref);

    ClusterTree::UpdateFunction func = [col_ndx, &list_refs, &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();
        size_t offset = size_t(cluster->get_offset());
        ArrayInteger arr(allocator);
        arr.Array::create(NodeHeader::type_HasRefs, false, sz, 0);
        for (size_t i = 0; i < sz; i++) {
            auto v = list_refs.get(i + offset);
            if (v) {
                if (v & 1) {
                    // This is a single link
                    arr.set(i, v);
                }
                else {
                    // This is a list - just clone the list
                    MemRef mem(to_ref(v), allocator);
                    MemRef copy_mem = Array::clone(mem, allocator, allocator); // Throws
                    arr.set_as_ref(i, copy_mem.get_ref());
                }
            }
        }
        cluster->add_leaf(col_ndx, arr.get_ref());
    };

    clusters.update(func);
}

void copy_column_list(ClusterTree& clusters, size_t col_ndx, ref_type col_ref, ColumnType col_type,
                      Allocator& allocator)
{
    BPlusTree<Int> list_refs(allocator);
    list_refs.init_from_ref(col_ref);

    ClusterTree::UpdateFunction func = [col_ndx, col_type, &list_refs, &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();
        size_t offset = size_t(cluster->get_offset());
        ArrayInteger arr(allocator);
        arr.Array::create(NodeHeader::type_HasRefs, false, sz, 0);
        for (size_t i = 0; i < sz; i++) {
            ref_type ref = to_ref(list_refs.get(i + offset));
            if (ref) {
                // List is not null - just clone the list
                if (col_type != col_type_LinkList) {
                    // This is list-of-primitives encoded in subtables
                    // Actual list is in the columns array position 0
                    Array cols(allocator);
                    cols.init_from_ref(ref);
                    ref = cols.get_as_ref(0);
                }
                MemRef mem(ref, allocator);
                MemRef copy_mem = Array::clone(mem, allocator, allocator); // Throws
                arr.set_as_ref(i, copy_mem.get_ref());
            }
        }
        cluster->add_leaf(col_ndx, arr.get_ref());
    };

    clusters.update(func);
}
}

bool Table::copy_content_from_columns(size_t col_ndx)
{
    ref_type ref = m_top.get_as_ref(top_position_for_columns);
    if (!ref) {
        // All columns have already been converted
        return false;
    }
    ColKey col_key(col_ndx);
    Array col_refs(m_alloc);
    col_refs.init_from_ref(ref);
    col_refs.set_parent(&m_top, top_position_for_columns);

    // Calculate index in columns list
    size_t ndx_in_parent = 0;
    for (size_t i = 0; i < col_ndx; i++) {
        ndx_in_parent += m_spec.get_column_attr(i).test(col_attr_Indexed) ? 2 : 1;
    }
    ref_type col_ref = col_refs.get_as_ref(ndx_in_parent);
    if (!col_ref) {
        // Column has already been converted
        return false;
    }

    ColumnAttrMask attr = m_spec.get_column_attr(col_ndx);
    ColumnType col_type = m_spec.get_column_type(col_ndx);

    if (attr.test(col_attr_List)) {
        copy_column_list(m_clusters, col_ndx, col_ref, col_type, m_alloc);
    }
    else {
        switch (col_type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    copy_column<util::Optional<int64_t>>(m_clusters, col_ndx, col_ref, m_alloc);
                }
                else {
                    copy_column<int64_t>(m_clusters, col_ndx, col_ref, m_alloc);
                }
                break;
            case col_type_Bool:
                if (attr.test(col_attr_Nullable)) {
                    copy_column<util::Optional<bool>>(m_clusters, col_ndx, col_ref, m_alloc);
                }
                else {
                    copy_column<bool>(m_clusters, col_ndx, col_ref, m_alloc);
                }
                break;
            case col_type_Float:
                copy_column<float>(m_clusters, col_ndx, col_ref, m_alloc);
                break;
            case col_type_Double:
                copy_column<double>(m_clusters, col_ndx, col_ref, m_alloc);
                break;
            case col_type_String:
                copy_column<String>(m_clusters, col_ndx, col_ref, m_alloc);
                break;
            case col_type_Binary:
                copy_column<Binary>(m_clusters, col_ndx, col_ref, m_alloc);
                break;
            case col_type_Timestamp:
                copy_column<Timestamp>(m_clusters, col_ndx, col_ref, m_alloc);
                break;
            case col_type_Link:
                // Just copy links as integers
                copy_column<int64_t>(m_clusters, col_ndx, col_ref, m_alloc);
                break;
            case col_type_BackLink:
                copy_column_backlink(m_clusters, col_ndx, col_ref, m_alloc);
                break;
            default:
                REALM_UNREACHABLE();
                break;
        }
    }

    if (attr.test(col_attr_Indexed)) {
        // Move index over to new position in table
        ref_type index_ref = col_refs.get_as_ref(ndx_in_parent + 1);
        m_index_refs.set(col_ndx, index_ref);
        col_refs.set(ndx_in_parent + 1, 0);
    }

    if (col_ndx == (m_spec.get_column_count() - 1)) {
        // Last column - destroy column ref array
        col_refs.destroy_deep();
        m_top.set(top_position_for_columns, 0);
    }
    else {
        // Just destroy single column
        Array::destroy_deep(col_ref, m_alloc);
        col_refs.set(ndx_in_parent, 0);
    }

#if 0
    if (fastrand(100) < 20) {
        throw std::runtime_error("Upgrade interrupted");
    }
#endif

    return true;
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
    size_t col_ndx = colkey2ndx(col_key);
    return m_spec.get_column_attr(col_ndx).test(col_attr_Nullable);
}

bool Table::is_list(ColKey col_key) const
{
    REALM_ASSERT_DEBUG(valid_column(col_key));
    size_t col_ndx = colkey2ndx(col_key);
    return m_spec.get_column_attr(col_ndx).test(col_attr_List);
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
    size_t col_ndx = colkey2ndx(col_key);
    auto target_key = m_spec.get_opposite_link_table_key(col_ndx);
    return get_parent_group()->get_table(target_key);
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

    if (has_search_index(col_key)) {
        size_t col_ndx = colkey2ndx(col_key);
        REALM_ASSERT(col_ndx < m_index_accessors.size());
        auto index = m_index_accessors[col_ndx];
        REALM_ASSERT(index);
        return index->find_first(value);
    }

    ObjKey key;
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    LeafType leaf(get_alloc());
    size_t col_ndx = colkey2ndx(col_key);

    ClusterTree::TraverseFunction f = [&key, &col_ndx, &value, &leaf](const Cluster* cluster) {
        cluster->init_leaf(col_ndx, &leaf);
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
    size_t col_ndx = colkey2ndx(col_key);

    ClusterTree::TraverseFunction f = [&key, &col_ndx, &value, &leaf](const Cluster* cluster) {
        cluster->init_leaf(col_ndx, &leaf);
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
    throw std::runtime_error("Not implemented");
}

ConstTableView Table::find_all_binary(ColKey, BinaryData) const
{
    // FIXME: Implement this!
    throw std::runtime_error("Not implemented");
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
        ColumnType type = table->get_real_column_type(link_chain[t]);
        if (type == col_type_LinkList || type == col_type_Link || type == col_type_BackLink) {
            auto key = table->m_spec.get_opposite_link_table_key(table->colkey2ndx(link_chain[t]));
            table = table->get_parent_group()->get_table(key);
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
        refresh_content_version();
        bump_storage_version();
        // update column mapping
        m_ndx2colkey.clear();
        m_colkey2ndx.clear();
        size_t num_cols = m_spec.get_column_count();
        for (size_t ndx = 0; ndx < num_cols; ++ndx) {
            ColKey col_key = m_spec.get_key(ndx);
            insert_col_mapping(ndx, col_key);
        }
    }
    refresh_index_accessors();
}

void Table::refresh_index_accessors()
{
    // Refresh search index accessors
    size_t col_ndx_end = m_spec.get_public_column_count();

    // Move all accessors to a temporary array
    std::vector<std::unique_ptr<StringIndex>> old_index_accessors;
    for (auto& i : m_index_accessors) {
        if (i) {
            old_index_accessors.emplace_back(i);
            i = nullptr;
        }
    }
    m_index_accessors.resize(col_ndx_end);

    for (size_t col_ndx = 0; col_ndx < col_ndx_end; col_ndx++) {
        ColKey col_key = ndx2colkey(col_ndx);
        bool has_index = m_spec.get_column_attr(col_ndx).test(col_attr_Indexed);

        if (has_index) {
            std::unique_ptr<StringIndex> index;
            // Check if we already have an accessor ready
            for (auto& i : old_index_accessors) {
                if (i && i->get_column_key() == col_key) {
                    index = std::move(i);
                    break;
                }
            }
            if (index) {
                index->set_parent(&m_index_refs, col_ndx);
                index->refresh_accessor_tree();
            }
            else if (ref_type ref = m_index_refs.get_as_ref(col_ndx)) {
                ClusterColumn virtual_col(&m_clusters, col_key);
                index.reset(new StringIndex(ref, &m_index_refs, col_ndx, virtual_col, get_alloc()));
            }
            m_index_accessors[col_ndx] = index.release();
        }
    }
}

bool Table::is_cross_table_link_target() const noexcept
{
    size_t first_backlink_column = m_spec.first_backlink_column_index();
    size_t end_backlink_column = m_spec.get_column_count();
    for (size_t i = first_backlink_column; i < end_backlink_column; ++i) {
        auto t = m_spec.get_column_type(i);
        // look for a backlink with a different target than ourselves
        if (t == col_type_BackLink && m_spec.get_opposite_link_table_key(i) != get_key())
            return true;
    }
    return false;
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
        auto i = ndx2colkey(k);
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
                auto target_table_key = m_spec.get_opposite_link_table_key(k);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_key);
                const StringData target_name = target_table->get_name();
                std::cout << "L->" << std::setw(7) << std::string(target_name).substr(0, 7) << " ";
                break;
            }
            case col_type_LinkList: {
                auto target_table_key = m_spec.get_opposite_link_table_key(k);
                ConstTableRef target_table = get_parent_group()->get_table(target_table_key);
                const StringData target_name = target_table->get_name();
                std::cout << "LL->" << std::setw(6) << std::string(target_name).substr(0, 6) << " ";
                break;
            }
            case col_type_BackLink: {
                auto target_table_key = m_spec.get_opposite_link_table_key(k);
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

        for (size_t k = 0; k < column_count; ++k) {
            auto n = ndx2colkey(k);
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
        auto target_table_key = table.m_spec.get_opposite_link_table_key(table.colkey2ndx(col_key));
        ret = table.get_parent_group()->get_table(target_table_key);
    }
    return ret;
}

const uint64_t Table::max_num_columns;

// insert a mapping, moving all later mappings to a higher index
void Table::insert_col_mapping(size_t ndx, ColKey key)
{
    if (ndx >= max_num_columns)
        throw std::runtime_error("Max number of columns exceeded");
    REALM_ASSERT(!valid_column(key));

    // increment index at all entries in key->ndx map pointing at ndx or above
    for (auto& e : m_colkey2ndx) {
        uint64_t e_ndx = e & max_num_columns;
        if ((e_ndx >= ndx) && (e_ndx != max_num_columns))
            ++e;
    }
    // insert new entry into ndx->key
    if (ndx == m_ndx2colkey.size())
        m_ndx2colkey.push_back(key);
    else
        m_ndx2colkey.insert(m_ndx2colkey.begin() + ndx, key);

    // make sure there is a free entry in key->ndx
    size_t idx = key.value & max_num_columns;

    // fill new positions with a blocked entry
    while (idx >= m_colkey2ndx.size()) {
        // auto tmp = max_num_columns;
        // m_colkey2ndx.push_back(tmp);
        m_colkey2ndx.push_back(max_num_columns);
    }
    // store tag of key along with ndx
    m_colkey2ndx[idx] = ndx | (key.value & ~max_num_columns);
}

// remove a mapping, moving all later mappings to a lower index
void Table::remove_col_mapping(size_t ndx)
{
    ColKey key = ndx2colkey(ndx);
    REALM_ASSERT(valid_column(key));

    // decrement index at all entries in key->ndx map pointing above ndx
    for (auto& e : m_colkey2ndx) {
        uint64_t e_ndx = e & max_num_columns;
        if ((e_ndx > ndx) && (e_ndx != max_num_columns))
            --e;
    }
    // remove selected entry
    size_t idx = key.value & max_num_columns;
    m_colkey2ndx[idx] = max_num_columns;

    // and opposite mapping
    m_ndx2colkey.erase(m_ndx2colkey.begin() + ndx);
}

ColKey Table::generate_col_key()
{
    // to generate the next key, we get the upper 48 bits from a dedicated counter,
    // and pick the lower 16 bits to be the first free slot in our
    // table for mapping key->ndx.
    RefOrTagged rot = m_top.get_as_ref_or_tagged(top_position_for_column_key);
    uint64_t upper = rot.get_as_int();
    rot = RefOrTagged::make_tagged(upper + 1);
    m_top.set(top_position_for_column_key, rot);

    uint64_t lower = m_colkey2ndx.size();
    // look for an unused entry in m_colkey2ndx:
    for (size_t idx = 0; idx < lower; ++idx) {
        size_t ndx = m_colkey2ndx[idx] & max_num_columns;
        if (ndx >= max_num_columns) {
            lower = idx;
            break;
        }
    }
    return ColKey((upper << 16) | lower);
}

ColumnAttrMask Table::get_column_attr(ColKey column_key) const noexcept
{
    size_t ndx = colkey2ndx(column_key);
    return m_spec.get_column_attr(ndx);
}

ColKey Table::find_backlink_column(TableKey origin_table_key, ColKey origin_col_key) const noexcept
{
    size_t ndx = m_spec.find_backlink_column(origin_table_key, origin_col_key);
    return ndx2colkey(ndx);
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
        size_t backlink_col_ndx = colkey2ndx(backlink_col);
        TableKey linked_table_key = m_spec.get_opposite_link_table_key(backlink_col_ndx);
        ColKey linked_column_key = m_spec.get_origin_column_key(backlink_col_ndx);
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
    size_t to = colkey2ndx(key_to);
    size_t from = colkey2ndx(key_from);
    ClusterTree::UpdateFunction func = [from, to, throw_on_null, from_nullability, &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();

        typename ColumnTypeTraits<F>::cluster_leaf_type from_arr(allocator);
        typename ColumnTypeTraits<T>::cluster_leaf_type to_arr(allocator);
        cluster->init_leaf(from, &from_arr);
        cluster->init_leaf(to, &to_arr);

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
    size_t to = colkey2ndx(key_to);
    size_t from = colkey2ndx(key_from);
    ClusterTree::UpdateFunction func = [from, to, throw_on_null, from_nullability, &allocator](Cluster* cluster) {
        size_t sz = cluster->node_size();

        ArrayInteger from_arr(allocator);
        ArrayInteger to_arr(allocator);
        cluster->init_leaf(from, &from_arr);
        cluster->init_leaf(to, &to_arr);

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
    m_spec.rename_column(colkey2ndx(new_col), column_name);

    if (si)
        add_search_index(new_col);

    return new_col;
}

// LCOV_EXCL_START
void Table::verify_inv() const
{
#ifdef REALM_DEBUG
    std::cerr << "       ndx -> colkey: ";
    for (size_t ndx = 0; ndx < m_ndx2colkey.size(); ++ndx) {
        ColKey col_key = m_ndx2colkey[ndx];
        // size_t idx = col_key.value & 0xFFFF;
        std::cerr << "{ " << ndx << " -> " << col_key << " } ";
    }
    std::cerr << std::endl;
    std::cerr << "       colkey -> ndx: ";
    for (size_t idx = 0; idx < m_colkey2ndx.size(); ++idx) {
        uint64_t ndx = m_colkey2ndx[idx];
        std::cerr << "{ " << idx << " -> " << (ndx & max_num_columns) << ", " << (ndx & ~max_num_columns) << " } ";
    }
    std::cerr << std::endl;
#endif
    for (size_t ndx = 0; ndx < m_ndx2colkey.size(); ++ndx) {
        ColKey col_key = m_ndx2colkey[ndx];
        size_t idx = col_key.value & max_num_columns;
        REALM_ASSERT(ndx < m_colkey2ndx.size());
        REALM_ASSERT(ndx == (m_colkey2ndx[idx] & max_num_columns));
    }
    for (size_t idx = 0; idx < m_colkey2ndx.size(); ++idx) {
        uint64_t ndx_and_tag = m_colkey2ndx[idx];
        size_t ndx = ndx_and_tag & max_num_columns;
        if (ndx != max_num_columns) {
            // valid entry, must be verified
            REALM_ASSERT(ndx < m_ndx2colkey.size());
            ColKey col_key = m_ndx2colkey[ndx];
            REALM_ASSERT((col_key.value & max_num_columns) == idx);
            REALM_ASSERT((col_key.value ^ ndx_and_tag) <= max_num_columns);
        }
    }
}
// LCOV_EXCL_STOP
