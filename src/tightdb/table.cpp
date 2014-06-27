#define _CRT_SECURE_NO_WARNINGS
#include <limits>
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>

#include <tightdb/util/features.h>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/table.hpp>
#include <tightdb/descriptor.hpp>
#include <tightdb/alloc_slab.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_basic.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_binary.hpp>
#include <tightdb/column_table.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/column_link.hpp>
#include <tightdb/column_linklist.hpp>
#include <tightdb/column_backlink.hpp>
#include <tightdb/index_string.hpp>
#include <tightdb/group.hpp>
#include <tightdb/link_view.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

/// \page AccessorConsistencyLevels
///
/// These are the three important levels of consistency of a hierarchy of
/// TightDB accessors rooted in a common group accessor (tables, columns, rows,
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
/// This is the level of consistency is the **maximum** that may be assumed
/// after a library function fails by throwing an unexpected exception (such as
/// std::bad_alloc), and it is the **minimum** level of consistency that is
/// needed to be able to properly destroy the accessor objects (manually, or as
/// a result of stack unwinding).
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
///    (ColumnSubtableParent::m_subtable_map). All pointers refer to existing
///    subtable accessors, but it is not required that the set of subtable
///    accessors referenced from a particular parent P conincide with the set
///    of subtables accessors specifying P as parent.
///
///  - The `descriptor` property of a table accesor (Table::m_descriptor). If it
///    is not null, then it refers to an existing descriptor accessor.
///
///  - The map of subdescriptor accessors in a descriptor accessor
///    (Descriptor::m_subdesc_map). All non-null pointers refer to existing
///    subdescriptor accessors.
///
///  - The `search_index` property of a column accesor
///    (AdaptiveStringColumn::m_index, ColumnStringEnum::m_index). When it is
///    non-null, it refers to an existing search index accessor.
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
///    accessor whose type agrees with the data type (tightdb::DataType) of the
///    corresponding underlying column (at same index).
///
///  - If a column accessor is of type `ColumnStringEnum`, then the
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
///    `ColumnSubtableParent::SubtableMap::entry::m_subtable_ndx`. The
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
///    enumerated string column (*ColumnStringEnum::m_keys.m_array).
///
///  - The root array accessor of search indexes
///    (*Table::m_cols[]->m_index->m_array).
///
/// Note that Structural Correspondence trivially includes Minimal Consistency,
/// since the latter it an invariant.


using namespace std;
using namespace tightdb;
using namespace tightdb::util;


// fixme, we need to gather all these typetraits definitions to just 1 single
template<class T> struct ColumnTypeTraits3;

template<> struct ColumnTypeTraits3<int64_t> {
    const static ColumnType ct_id = col_type_Int;
    const static ColumnType ct_id_real = col_type_Int;
    typedef Column column_type;
};
template<> struct ColumnTypeTraits3<bool> {
    const static ColumnType ct_id = col_type_Bool;
    const static ColumnType ct_id_real = col_type_Bool;
    typedef Column column_type;
};
template<> struct ColumnTypeTraits3<float> {
    const static ColumnType ct_id = col_type_Float;
    const static ColumnType ct_id_real = col_type_Float;
    typedef ColumnFloat column_type;
};
template<> struct ColumnTypeTraits3<double> {
    const static ColumnType ct_id = col_type_Double;
    const static ColumnType ct_id_real = col_type_Double;
    typedef ColumnDouble column_type;
};
template<> struct ColumnTypeTraits3<DateTime> {
    const static ColumnType ct_id = col_type_DateTime;
    const static ColumnType ct_id_real = col_type_Int;
    typedef Column column_type;
};

// -- Table ---------------------------------------------------------------------------------

size_t Table::add_column(DataType type, StringData name, DescriptorRef* subdesc)
{
    TIGHTDB_ASSERT(!has_shared_type());
    return get_descriptor()->add_column(type, name, subdesc); // Throws
}

size_t Table::add_column_link(DataType type, StringData name, Table& target)
{
    return get_descriptor()->add_column_link(type, name, target); // Throws
}


void Table::insert_column_link(size_t column_ndx, DataType type, StringData name, Table& target)
{
    get_descriptor()->insert_column_link(column_ndx, type, name, target); // Throws
}


void Table::create_backlinks_column(Table& origin, size_t origin_col_ndx)
{
    // Links only work for group-level tables
    TIGHTDB_ASSERT(!has_shared_type());
    TIGHTDB_ASSERT(get_parent_group());

    size_t backlink_col_ndx = m_spec.get_column_count();
    Table* link_target_table = 0;
    insert_root_column(backlink_col_ndx, col_type_BackLink, "", link_target_table); // Throws
    size_t origin_table_ndx = origin.get_index_in_parent();
    m_spec.set_link_target_table(backlink_col_ndx, origin_table_ndx); // Throws
    m_spec.set_backlink_origin_column(backlink_col_ndx, origin_col_ndx); // Throws

    ColumnBackLink& backlink_col = get_column_back_link(backlink_col_ndx);
    backlink_col.set_origin_table(origin);

    ColumnLinkBase& origin_col = origin.get_column_link_base(origin_col_ndx);
    backlink_col.set_origin_column(origin_col);
}


ColumnBackLink& Table::get_backlink_column(size_t origin_table_ndx,
                                           size_t origin_col_ndx) TIGHTDB_NOEXCEPT
{
    size_t backlink_col_ndx = m_spec.find_backlink_column(origin_table_ndx, origin_col_ndx);
    return get_column_back_link(backlink_col_ndx);
}


size_t Table::get_backlink_count(size_t row_ndx, const Table& origin,
                                 size_t origin_col_ndx) const TIGHTDB_NOEXCEPT
{
    size_t origin_table_ndx = origin.get_index_in_parent();
    size_t backlink_col_ndx = m_spec.find_backlink_column(origin_table_ndx, origin_col_ndx);
    const ColumnBackLink& backlink_col = get_column_back_link(backlink_col_ndx);
    return backlink_col.get_backlink_count(row_ndx);
}


size_t Table::get_backlink(size_t row_ndx, const Table& origin, size_t origin_col_ndx,
                           size_t backlink_ndx) const TIGHTDB_NOEXCEPT
{
    size_t origin_table_ndx = origin.get_index_in_parent();
    size_t backlink_col_ndx = m_spec.find_backlink_column(origin_table_ndx, origin_col_ndx);
    const ColumnBackLink& backlink_col = get_column_back_link(backlink_col_ndx);
    return backlink_col.get_backlink(row_ndx, backlink_ndx);
}


void Table::initialize_link_targets(Group& group, size_t table_ndx)
{
    // Links only work for group-level tables
    TIGHTDB_ASSERT(!has_shared_type());
    TIGHTDB_ASSERT(get_parent_group());

    size_t n = m_spec.get_column_count();
    for (size_t i = 0; i < n; ++i) {
        ColumnType column_type = m_spec.get_column_type(i);
        if (column_type == col_type_Link || column_type == col_type_LinkList) {
            // Get the target table from group
            size_t target_table_ndx = m_spec.get_link_target_table(i);
            Table* target_table = group.get_table_by_ndx(target_table_ndx); // Throws

            // Set target table in column
            ColumnLinkBase& column = get_column_link_base(i);
            column.set_target_table(*target_table);
            column.set_backlink_column(target_table->get_backlink_column(table_ndx, i));
        }
        else if (column_type == col_type_BackLink) {
            // Get the origin table from group
            size_t origin_table_ndx = m_spec.get_link_target_table(i);
            Table* origin_table = group.get_table_by_ndx(origin_table_ndx); // Throws

            // Get the columns the links originate from
            size_t origin_col_ndx = m_spec.get_backlink_origin_column(i);
            ColumnLinkBase& origin_col = origin_table->get_column_link_base(origin_col_ndx);

            // Set origination info in backlink column
            ColumnBackLink& backlink_col = get_column_back_link(i);
            backlink_col.set_origin_table(*origin_table);
            backlink_col.set_origin_column(origin_col);
        }
    }
}


void Table::insert_column(size_t column_ndx, DataType type, StringData name,
                          DescriptorRef* subdesc)
{
    TIGHTDB_ASSERT(!has_shared_type());
    get_descriptor()->insert_column(column_ndx, type, name, subdesc); // Throws
}


void Table::remove_column(size_t column_ndx)
{
    TIGHTDB_ASSERT(!has_shared_type());
    get_descriptor()->remove_column(column_ndx); // Throws
}


void Table::rename_column(size_t column_ndx, StringData name)
{
    TIGHTDB_ASSERT(!has_shared_type());
    get_descriptor()->rename_column(column_ndx, name); // Throws
}


DescriptorRef Table::get_descriptor()
{
    TIGHTDB_ASSERT(is_attached());

    if (has_shared_type()) {
        ArrayParent* array_parent = m_columns.get_parent();
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(array_parent));
        Parent* table_parent = static_cast<Parent*>(array_parent);
        size_t column_ndx = 0;
        Table* parent = table_parent->get_parent_table(&column_ndx);
        TIGHTDB_ASSERT(parent);
        return parent->get_descriptor()->get_subdescriptor(column_ndx); // Throws
    }

    DescriptorRef desc;
    if (!m_descriptor) {
        typedef _impl::DescriptorFriend df;
        desc.reset(df::create()); // Throws
        Descriptor* parent = 0;
        df::attach(*desc, this, parent, &m_spec);
        m_descriptor = desc.get();
    }
    else {
        desc.reset(m_descriptor);
    }
    return move(desc);
}


ConstDescriptorRef Table::get_descriptor() const
{
    return const_cast<Table*>(this)->get_descriptor(); // Throws
}


DescriptorRef Table::get_subdescriptor(size_t column_ndx)
{
    return get_descriptor()->get_subdescriptor(column_ndx); // Throws
}


ConstDescriptorRef Table::get_subdescriptor(size_t column_ndx) const
{
    return get_descriptor()->get_subdescriptor(column_ndx); // Throws
}


DescriptorRef Table::get_subdescriptor(const path_vec& path)
{
    DescriptorRef desc = get_descriptor(); // Throws
    typedef path_vec::const_iterator iter;
    iter end = path.end();
    for (iter i = path.begin(); i != end; ++i)
        desc = desc->get_subdescriptor(*i); // Throws
    return desc;
}


ConstDescriptorRef Table::get_subdescriptor(const path_vec& path) const
{
    return const_cast<Table*>(this)->get_subdescriptor(path); // Throws
}


size_t Table::add_subcolumn(const path_vec& path, DataType type, StringData name)
{
    DescriptorRef desc = get_subdescriptor(path); // Throws
    size_t column_ndx = desc->get_column_count();
    desc->insert_column(column_ndx, type, name); // Throws
    return column_ndx;
}


void Table::insert_subcolumn(const path_vec& path, size_t column_ndx,
                             DataType type, StringData name)
{
    get_subdescriptor(path)->insert_column(column_ndx, type, name); // Throws
}


void Table::remove_subcolumn(const path_vec& path, size_t column_ndx)
{
    get_subdescriptor(path)->remove_column(column_ndx); // Throws
}


void Table::rename_subcolumn(const path_vec& path, size_t column_ndx, StringData name)
{
    get_subdescriptor(path)->rename_column(column_ndx, name); // Throws
}



void Table::init_from_ref(ref_type top_ref, ArrayParent* parent, size_t ndx_in_parent)
{
    // Load from allocated memory
    m_top.init_from_ref(top_ref);
    m_top.set_parent(parent, ndx_in_parent);
    TIGHTDB_ASSERT(m_top.size() == 2);

    ref_type spec_ref    = m_top.get_as_ref(0);
    ref_type columns_ref = m_top.get_as_ref(1);

    size_t spec_ndx_in_parent = 0;
    m_spec.set_parent(&m_top, spec_ndx_in_parent);
    m_spec.init(spec_ref);
    m_columns.init_from_ref(columns_ref);
    size_t columns_ndx_in_parent = 1;
    m_columns.set_parent(&m_top, columns_ndx_in_parent);

    // Also initializes m_size
    create_column_accessors(); // Throws
}


void Table::init_from_ref(ConstSubspecRef shared_spec, ref_type columns_ref,
                          ArrayParent* parent, size_t ndx_in_parent)
{
    m_spec.init(SubspecRef(SubspecRef::const_cast_tag(), shared_spec));

    // A table instatiated with a zero-ref is just an empty table
    // but it will have to create itself on first modification
    if (columns_ref != 0) {
        m_columns.init_from_ref(columns_ref);
        // Also initializes m_size
        create_column_accessors(); // Throws
    }
    m_columns.set_parent(parent, ndx_in_parent);
}


struct Table::InsertSubtableColumns: SubtableUpdater {
    InsertSubtableColumns(size_t i, DataType t):
        m_column_ndx(i), m_type(t)
    {
    }
    void update(const ColumnTable& subtables, size_t row_ndx, Array& subcolumns) TIGHTDB_OVERRIDE
    {
        size_t subtable_size = subtables.get_subtable_size(row_ndx);
        Allocator& alloc = subcolumns.get_alloc();
        ref_type column_ref = create_column(ColumnType(m_type), subtable_size, alloc); // Throws
        _impl::DeepArrayRefDestroyGuard dg(column_ref, alloc);
        subcolumns.insert(m_column_ndx, column_ref); // Throws
        dg.release();
    }
    void update_accessor(Table& table, size_t row_ndx) TIGHTDB_OVERRIDE
    {
        table.adj_insert_column(m_column_ndx); // Throws
        table.mark();
        table.refresh_accessor_tree(row_ndx);
    }
private:
    const size_t m_column_ndx;
    const DataType m_type;
};


struct Table::RemoveSubtableColumns: SubtableUpdater {
    RemoveSubtableColumns(size_t i):
        m_column_ndx(i)
    {
    }
    void update(const ColumnTable&, size_t, Array& subcolumns) TIGHTDB_OVERRIDE
    {
        ref_type column_ref = to_ref(subcolumns.get(m_column_ndx));
        subcolumns.erase(m_column_ndx); // Throws
        Array::destroy_deep(column_ref, subcolumns.get_alloc());
    }
    void update_accessor(Table& table, size_t row_ndx) TIGHTDB_OVERRIDE
    {
        table.adj_erase_column(m_column_ndx);
        table.mark();
        table.refresh_accessor_tree(row_ndx);
    }
private:
    const size_t m_column_ndx;
};


struct Table::RenameSubtableColumns: SubtableUpdater {
    void update(const ColumnTable&, size_t, Array&) TIGHTDB_OVERRIDE
    {
    }
    void update_accessor(Table& table, size_t row_ndx) TIGHTDB_OVERRIDE
    {
        table.mark();
        table.refresh_accessor_tree(row_ndx);
    }
};


void Table::do_insert_column(Descriptor& desc, size_t column_ndx, DataType type,
                             StringData name, Table* link_target_table)
{
    TIGHTDB_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::root_table(desc);
    TIGHTDB_ASSERT(!root_table.has_shared_type());

    if (desc.is_root()) {
        root_table.insert_root_column(column_ndx, ColumnType(type), name,
                                      link_target_table); // Throws
    }
    else {
        Spec* spec = df::get_spec(desc);
        spec->insert_column(column_ndx, ColumnType(type), name); // Throws
        if (!root_table.is_empty()) {
            InsertSubtableColumns updater(column_ndx, type);
            update_subtables(desc, &updater); // Throws
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = root_table.get_repl())
        repl->insert_column(desc, column_ndx, type, name, link_target_table); // Throws
#endif
}


void Table::do_remove_column(Descriptor& desc, size_t column_ndx)
{
    TIGHTDB_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::root_table(desc);
    TIGHTDB_ASSERT(!root_table.has_shared_type());
    TIGHTDB_ASSERT(column_ndx < desc.get_column_count());

    if (desc.is_root()) {
        root_table.remove_root_column(column_ndx); // Throws
    }
    else {
        Spec* spec = df::get_spec(desc);
        spec->remove_column(column_ndx); // Throws
        if (!root_table.is_empty()) {
            RemoveSubtableColumns updater(column_ndx);
            update_subtables(desc, &updater); // Throws
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = root_table.get_repl())
        repl->erase_column(desc, column_ndx); // Throws
#endif
}


void Table::do_rename_column(Descriptor& desc, size_t column_ndx, StringData name)
{
    TIGHTDB_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::root_table(desc);
    TIGHTDB_ASSERT(!root_table.has_shared_type());
    TIGHTDB_ASSERT(column_ndx < desc.get_column_count());

    Spec* spec = df::get_spec(desc);
    spec->rename_column(column_ndx, name); // Throws

    if (desc.is_root()) {
        root_table.bump_version();
    }
    else {
        if (!root_table.is_empty()) {
            RenameSubtableColumns updater;
            update_subtables(desc, &updater); // Throws
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = root_table.get_repl())
        repl->rename_column(desc, column_ndx, name); // Throws
#endif
}


void Table::insert_root_column(size_t col_ndx, ColumnType col_type, StringData name,
                               Table* link_target_table)
{
    // Back-link columns can only be appended, never inserted
    TIGHTDB_ASSERT(col_type != col_type_BackLink || col_ndx == m_columns.size());

    m_search_index = 0;
    bump_version();

    // Add the column to the spec
    m_spec.insert_column(col_ndx, col_type, name); // Throws

    Spec::ColumnInfo info;
    m_spec.get_column_info(col_ndx, info);
    size_t ndx_in_parent = info.m_column_ref_ndx;
    ref_type ref = create_column(col_type, m_size, m_columns.get_alloc()); // Throws
    m_columns.insert(ndx_in_parent, ref); // Throws
    ColumnBase* col = create_column_accessor(col_type, col_ndx, ndx_in_parent); // Throws
    {
        UniquePtr<ColumnBase> guard(col);
        m_cols.insert(m_cols.begin() + col_ndx, col);
        // FSA: m_cols.insert(col_ndx, intptr_t(col)); // Throws
        guard.release();
    }

    // Update cached column indexes for subsequent column accessors
    int ndx_in_parent_diff = 1;
    adjust_column_index(col_ndx+1, ndx_in_parent_diff);

    if (link_target_table) {
        size_t target_table_ndx = link_target_table->get_index_in_parent();
        m_spec.set_link_target_table(col_ndx, target_table_ndx); // Throws

        link_target_table->create_backlinks_column(*this, col_ndx); // Throws

        size_t origin_table_ndx = get_index_in_parent();
        ColumnBackLink& backlink_col =
            link_target_table->get_backlink_column(origin_table_ndx, col_ndx);

        // Origin column needs target table to create accessors
        ColumnLinkBase& origin_col = static_cast<ColumnLinkBase&>(*col);
        origin_col.set_target_table(*link_target_table);
        origin_col.set_backlink_column(backlink_col);
    }
}


void Table::remove_root_column(size_t column_ndx)
{
    m_search_index = 0;
    bump_version();

    Spec::ColumnInfo info;
    m_spec.get_column_info(column_ndx, info);

    // Remove the column from the spec
    m_spec.remove_column(column_ndx); // Throws

    // Remove and destroy the ref from m_columns
    ref_type column_ref = m_columns.get_as_ref(info.m_column_ref_ndx);
    Array::destroy_deep(column_ref, m_columns.get_alloc());
    m_columns.erase(info.m_column_ref_ndx);

    // If the column had an index we have to remove that as well
    if (info.m_has_index) {
        ref_type index_ref = m_columns.get_as_ref(info.m_column_ref_ndx);
        Array::destroy_deep(index_ref, m_columns.get_alloc());
        m_columns.erase(info.m_column_ref_ndx);
    }

    // Delete the column accessor
    ColumnBase* col = m_cols[column_ndx];
    col->detach_subtable_accessors();
    delete col;

    m_cols.erase(m_cols.begin() + column_ndx);

    // Update cached column indexes for subsequent column accessors
    int ndx_in_parent_diff = info.m_has_index ? -2 : -1;
    adjust_column_index(column_ndx, ndx_in_parent_diff);

    // If there are no columns left, mark the table as empty
    if (get_column_count() == 0) {
        discard_row_accessors();
        detach_views_except(0);
        m_size = 0;
    }
}


void Table::unregister_view(const TableViewBase* view) TIGHTDB_NOEXCEPT
{
    // Fixme: O(n) may be unacceptable - if so, put and maintain
    // iterator or index in TableViewBase.
    vector<const TableViewBase*>::iterator it;
    vector<const TableViewBase*>::iterator end = m_views.end();
    for (it = m_views.begin(); it != end; ++it) {
        if (*it == view) {
            *it = m_views.back();
            m_views.pop_back();
            break;
        }
    }
}


void Table::register_row_accessor(RowBase* row) const
{
    m_row_accessors.push_back(row); // Throws
}


void Table::unregister_row_accessor(RowBase* row) const TIGHTDB_NOEXCEPT
{
    typedef row_accessors::iterator iter;
    iter i = m_row_accessors.begin(), end = m_row_accessors.end();
    for (;;) {
        if (i == end)
            return;
        if (*i == row)
            break;
        ++i;
    }
    --end;
    // If the discarded accessor is not the last entry, we
    // need to move the last entry over
    if (i != end)
        *i = *end;
    m_row_accessors.pop_back();
}


void Table::discard_row_accessors() TIGHTDB_NOEXCEPT
{
    typedef row_accessors::const_iterator iter;
    iter end = m_row_accessors.end();
    for (iter i = m_row_accessors.begin(); i != end; ++i)
        (*i)->m_table.reset(); // Detach
    m_row_accessors.clear();
}


void Table::update_subtables(Descriptor& desc, SubtableUpdater* updater)
{
    size_t stat_buf[8];
    size_t size = sizeof stat_buf / sizeof *stat_buf;
    size_t* begin = stat_buf;
    size_t* end = begin + size;
    UniquePtr<size_t> dyn_buf;
    for (;;) {
        typedef _impl::DescriptorFriend df;
        begin = df::record_subdesc_path(desc, begin, end);
        if (TIGHTDB_LIKELY(begin)) {
            Table& root_table = df::root_table(desc);
            root_table.update_subtables(begin, end, updater); // Throws
            return;
        }
        if (int_multiply_with_overflow_detect(size, 2))
            throw runtime_error("Too many subdescriptor nesting levels");
        begin = new size_t[size]; // Throws
        end = begin + size;
        dyn_buf.reset(begin);
    }
}


void Table::update_subtables(const size_t* col_path_begin, const size_t* col_path_end,
                             SubtableUpdater* updater)
{
    size_t col_path_size = col_path_end - col_path_begin;
    TIGHTDB_ASSERT(col_path_size >= 1);

    size_t column_ndx = *col_path_begin;
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == col_type_Table);

    ColumnTable& subtables = get_column_table(column_ndx); // Throws
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
            subtable->m_spec.init_from_parent();
        }
        if (is_parent_of_modify_level) {
            // These subtables of the parent at this level are the ones that
            // need to be modified.
            if (!updater)
                continue;
            // If the table is degenerate, there is no underlying subtable to
            // modify, and since a table accessor attached to a degenerate
            // subtable has no cached columns, a preexisting subtable accessor
            // will not have to be refreshed either.
            ref_type subtable_ref = subtables.get_as_ref(row_ndx);
            if (subtable_ref == 0)
                continue;
            Allocator& alloc = m_columns.get_alloc();
            Array subcolumns(subtable_ref, &subtables, row_ndx, alloc);
            updater->update(subtables, row_ndx, subcolumns); // Throws
            if (subtable)
                updater->update_accessor(*subtable, row_ndx); // Throws
        }
        else {
            // These subtables of the parent at this level are ancestors of the
            // subtables that need to be modified, so we can safely instantiate
            // missing subtable accessors.
            if (subtables.get_as_ref(row_ndx) == 0)
                continue; // Degenerate subatble
            if (!subtable) {
                // If there is no updater, the we only need to refesh
                // preexisting accessors
                if (!updater)
                    continue;
                subtable.reset(subtables.get_subtable_ptr(row_ndx)); // Throws
            }
            subtable->update_subtables(col_path_begin+1, col_path_end, updater); // Throws
        }
    }
}


void Table::update_accessors(const size_t* col_path_begin, const size_t* col_path_end,
                             AccessorUpdater& updater)
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    TIGHTDB_ASSERT(is_attached());

    if (col_path_begin == col_path_end) {
        updater.update(*this); // Throws
        return;
    }
    updater.update_parent(*this); // Throws

    size_t col_ndx = col_path_begin[0];
    // If this table is not a degenerate subtable, then `col_ndx` must be a
    // valid index into `m_cols`.
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_cols.size());

    // Early-out if this accessor refers to a degenerate subtable
    if (m_cols.empty())
        return;

    if (ColumnBase* col = m_cols[col_ndx]) {
        TIGHTDB_ASSERT(dynamic_cast<ColumnTable*>(col));
        ColumnTable* col_2 = static_cast<ColumnTable*>(col);
        col_2->update_table_accessors(col_path_begin+1, col_path_end, updater); // Throws
    }
}


// Create columns as well as column accessors.
void Table::create_columns()
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || m_columns.is_empty()); // only on initial creation

    // Instantiate first if we have an empty table (from zero-ref)
    if (!m_columns.is_attached()) {
        m_columns.create(Array::type_HasRefs);
        m_columns.update_parent();
    }

    Allocator& alloc = m_columns.get_alloc();

    // Add the newly defined columns
    size_t n = m_spec.get_column_count();
    for (size_t i=0; i<n; ++i) {
        ColumnType type = m_spec.get_column_type(i);
        ColumnAttr attr = m_spec.get_column_attr(i);
        size_t ref_pos =  m_columns.size();
        ColumnBase* new_col = 0;

        switch (type) {
            case type_Int:
            case type_Bool:
            case type_DateTime: {
                Column* c = new Column(Array::type_Normal, alloc);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_Link: {
                ColumnLink* c = new ColumnLink(alloc);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_LinkList: {
                ColumnLinkList* c = new ColumnLinkList(alloc);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_Float: {
                ColumnFloat* c = new ColumnFloat(alloc);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_Double: {
                ColumnDouble* c = new ColumnDouble(alloc);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_String: {
                AdaptiveStringColumn* c = new AdaptiveStringColumn(alloc);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_Binary: {
                ColumnBinary* c = new ColumnBinary(alloc);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_Table: {
                size_t column_ndx = m_cols.size();
                ColumnTable* c = new ColumnTable(alloc, this, column_ndx);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }
            case type_Mixed: {
                size_t column_ndx = m_cols.size();
                ColumnMixed* c = new ColumnMixed(alloc, this, column_ndx);
                m_columns.add(c->get_ref());
                c->set_parent(&m_columns, ref_pos);
                new_col = c;
                break;
            }

            case col_type_StringEnum:
            case col_type_Reserved1:
            case col_type_Reserved4:
            case col_type_BackLink:
                TIGHTDB_ASSERT(false);
        }

        // Cache Columns
        m_cols.push_back(new_col);
        // FSA: m_cols.add(reinterpret_cast<intptr_t>(new_col)); // FIXME: intptr_t is not guaranteed to exists, not even in C++11

        // Atributes on columns may define that they come with an index
        if (attr != col_attr_None) {
            TIGHTDB_ASSERT(attr == col_attr_Indexed); // only supported attr so far
            size_t column_ndx = m_cols.size()-1;
            set_index(column_ndx, false);
        }
    }
}


void Table::detach() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->on_table_destroyed(this);
    m_spec.m_top.detach();
#endif

    discard_desc_accessor();

    // This prevents the destructor from deallocating the underlying
    // memory structure, and from attempting to notify the parent. It
    // also causes is_attached() to return false.
    m_columns.set_parent(0,0);

    discard_row_accessors();
    discard_subtable_accessors();

    destroy_column_accessors();
    m_cols.clear();
    // FSA: m_cols.destroy();
    detach_views_except(0);
}


// Note about exception safety:
// This function must be called with a view that is either 0, OR
// already present in the view registry, Otherwise it may throw.
void Table::detach_views_except(const TableViewBase* view) TIGHTDB_NOEXCEPT
{
    vector<const TableViewBase*>::iterator end = m_views.end();
    vector<const TableViewBase*>::iterator it = m_views.begin();
    while (it != end) {
        const TableViewBase* v = *it;
        if (v != view)
            v->detach();
        ++it;
    }
    m_views.clear();
    if (view) {
        // Can **not** throw, because if the view is not 0, it must
        // have been in the registry before AND since clear does not
        // release memory, no new memory is needed for push_back:
        m_views.push_back(view);
    }
}


void Table::discard_subtable_accessors() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    size_t n = m_cols.size();
    for (size_t i = 0; i < n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->detach_subtable_accessors();
    }
}


void Table::discard_desc_accessor() TIGHTDB_NOEXCEPT
{
    if (m_descriptor) {
        // Must hold a reliable reference count while detaching
        DescriptorRef desc(m_descriptor);
        typedef _impl::DescriptorFriend df;
        df::detach(*desc);
        m_descriptor = 0;
    }
}


void Table::instantiate_before_change()
{
    // Empty (zero-ref'ed) tables need to be instantiated before first modification
    if (!m_columns.is_attached())
        create_columns(); // Throws
}


ColumnBase* Table::create_column_accessor(ColumnType col_type, size_t col_ndx, size_t ndx_in_parent)
{
    ref_type ref = m_columns.get_as_ref(ndx_in_parent);
    Allocator& alloc = m_columns.get_alloc();
    switch (col_type) {
        case col_type_Int:
        case col_type_Bool:
        case col_type_DateTime:
            return new Column(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_Float:
            return new ColumnFloat(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_Double:
            return new ColumnDouble(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_String:
            return new AdaptiveStringColumn(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_Binary:
            return new ColumnBinary(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_StringEnum: {
            ArrayParent* keys_parent;
            size_t keys_ndx_in_parent;
            ref_type keys_ref =
                m_spec.get_enumkeys_ref(col_ndx, &keys_parent, &keys_ndx_in_parent);
            return new ColumnStringEnum(keys_ref, ref, &m_columns, ndx_in_parent,
                                        keys_parent, keys_ndx_in_parent, alloc); // Throws
        }
        case col_type_Table:
            return new ColumnTable(alloc, this, col_ndx, &m_columns, ndx_in_parent, ref); // Throws
        case col_type_Mixed:
            return new ColumnMixed(alloc, this, col_ndx, &m_columns, ndx_in_parent, ref); // Throws
        case col_type_Link:
            // Target table will be set by group after entire table has been created
            return new ColumnLink(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_LinkList:
            // Target table will be set by group after entire table has been created
            return new ColumnLinkList(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_BackLink:
            // Origin table will be set by group after entire table has been created
            return new ColumnBackLink(ref, &m_columns, ndx_in_parent, alloc); // Throws
        case col_type_Reserved1:
        case col_type_Reserved4:
            // These have no function yet and are therefore unexpected.
            break;
    }
    TIGHTDB_ASSERT(false);
    return 0;
}


void Table::create_column_accessors()
{
    TIGHTDB_ASSERT(m_cols.empty()); // only done on creation

    size_t ndx_in_parent = 0;
    size_t num_cols = m_spec.get_column_count();
    m_cols.resize(num_cols);
    for (size_t col_ndx = 0; col_ndx < num_cols; ++col_ndx) {
        ColumnType col_type = m_spec.get_column_type(col_ndx);
        ColumnBase* col = create_column_accessor(col_type, col_ndx, ndx_in_parent); // Throws
        m_cols[col_ndx] = col;

        // Attributes on columns may define that they come with a search index
        ColumnAttr attr = m_spec.get_column_attr(col_ndx);
        if (attr != col_attr_None) {
            TIGHTDB_ASSERT(attr == col_attr_Indexed); // only attribute supported for now
            TIGHTDB_ASSERT(col_type == col_type_String ||
                           col_type == col_type_StringEnum);  // index only for strings

            ref_type index_ref = m_columns.get_as_ref(ndx_in_parent + 1);
            col->set_index_ref(index_ref, &m_columns, ndx_in_parent + 1); // Throws

            // A search index occupies one slot in m_columns.
            ++ndx_in_parent;
        }

        ++ndx_in_parent;
    }

    // Set table size
    if (num_cols == 0) {
        m_size = 0;
    }
    else {
        ColumnBase* first_col = m_cols[0];
        m_size = first_col->size();
    }
}


void Table::destroy_column_accessors() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        ColumnBase* column = m_cols[i];
        delete column;
    }
    m_cols.clear();
}


Table::~Table() TIGHTDB_NOEXCEPT
{
    // Whenever this is not a free-standing table, the destructor must be able
    // to operate without assuming more than minimal accessor consistency This
    // means in particular that it cannot access the underlying structure of
    // array nodes. See AccessorConcistncyLevels.

    if (!is_attached()) {
        // This table has been detached.
        TIGHTDB_ASSERT(m_ref_count == 0);
        return;
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->on_table_destroyed(this);
    m_spec.m_top.detach();
#endif

    if (!m_top.is_attached()) {
        // This is a subtable with a shared spec, and its lifetime is managed by
        // reference counting, so we must let the parent know about the demise
        // of this subtable.
        ArrayParent* parent = m_columns.get_parent();
        TIGHTDB_ASSERT(parent);
        TIGHTDB_ASSERT(m_ref_count == 0);
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(parent));
        static_cast<Parent*>(parent)->child_accessor_destroyed(this);
        destroy_column_accessors();
        m_cols.clear();
        return;
    }

    // This is a table with an independent spec.
    if (ArrayParent* parent = m_top.get_parent()) {
        // This is a table whose lifetime is managed by reference
        // counting, so we must let our parent know about our demise.
        TIGHTDB_ASSERT(m_ref_count == 0);
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(parent));
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


bool Table::has_index(size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    const ColumnBase& col = get_column_base(column_ndx);
    return col.has_index();
}


void Table::set_index(size_t column_ndx, bool update_spec)
{
    TIGHTDB_ASSERT(!has_shared_type());
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    if (has_index(column_ndx))
        return;

    m_search_index = 0;
    // FIXME: can this ever change the outcome of a query?
    bump_version();

    ColumnType ct = get_real_column_type(column_ndx);
    Spec::ColumnInfo info;
    m_spec.get_column_info(column_ndx, info);
    size_t column_pos = info.m_column_ref_ndx;
    ref_type index_ref = 0;

    if (ct == col_type_String) {
        AdaptiveStringColumn& col = get_column_string(column_ndx);

        // Create the index
        StringIndex& index = col.create_index();
        index.set_parent(&m_columns, column_pos+1);
        index_ref = index.get_ref();
    }
    else if (ct == col_type_StringEnum) {
        ColumnStringEnum& col = get_column_string_enum(column_ndx);

        // Create the index
        StringIndex& index = col.create_index();
        index.set_parent(&m_columns, column_pos+1);
        index_ref = index.get_ref();
    }
    else {
        TIGHTDB_ASSERT(false);
        return;
    }

    // Insert ref into columns list after the owning column
    m_columns.insert(column_pos+1, index_ref);
    int ndx_in_parent_diff = 1;
    adjust_column_index(column_ndx+1, ndx_in_parent_diff);

    // Update spec
    if (update_spec)
        m_spec.set_column_attr(column_ndx, col_attr_Indexed);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->add_index_to_column(this, column_ndx); // Throws
#endif
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

const ColumnBase& Table::get_column_base(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_spec.get_column_count());
    TIGHTDB_ASSERT(m_cols.size() == m_spec.get_column_count());
    return *m_cols[ndx];
}


ColumnBase& Table::get_column_base(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_spec.get_column_count());
    instantiate_before_change();
    TIGHTDB_ASSERT(m_cols.size() == m_spec.get_column_count());
    return *m_cols[ndx];
}


const Column& Table::get_column(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<Column, col_type_Int>(ndx);
}

Column& Table::get_column(size_t ndx)
{
    return get_column<Column, col_type_Int>(ndx);
}

const AdaptiveStringColumn& Table::get_column_string(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<AdaptiveStringColumn, col_type_String>(ndx);
}

AdaptiveStringColumn& Table::get_column_string(size_t ndx)
{
    return get_column<AdaptiveStringColumn, col_type_String>(ndx);
}

const ColumnStringEnum& Table::get_column_string_enum(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnStringEnum, col_type_StringEnum>(ndx);
}

ColumnStringEnum& Table::get_column_string_enum(size_t ndx)
{
    return get_column<ColumnStringEnum, col_type_StringEnum>(ndx);
}

const ColumnFloat& Table::get_column_float(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnFloat, col_type_Float>(ndx);
}

ColumnFloat& Table::get_column_float(size_t ndx)
{
    return get_column<ColumnFloat, col_type_Float>(ndx);
}

const ColumnDouble& Table::get_column_double(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnDouble, col_type_Double>(ndx);
}

ColumnDouble& Table::get_column_double(size_t ndx)
{
    return get_column<ColumnDouble, col_type_Double>(ndx);
}

const ColumnBinary& Table::get_column_binary(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnBinary, col_type_Binary>(ndx);
}

ColumnBinary& Table::get_column_binary(size_t ndx)
{
    return get_column<ColumnBinary, col_type_Binary>(ndx);
}

const ColumnTable &Table::get_column_table(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnTable, col_type_Table>(ndx);
}

ColumnTable &Table::get_column_table(size_t ndx)
{
    return get_column<ColumnTable, col_type_Table>(ndx);
}

const ColumnMixed& Table::get_column_mixed(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnMixed, col_type_Mixed>(ndx);
}

ColumnMixed& Table::get_column_mixed(size_t ndx)
{
    return get_column<ColumnMixed, col_type_Mixed>(ndx);
}

const ColumnLinkBase& Table::get_column_link_base(size_t ndx) const TIGHTDB_NOEXCEPT
{
    const ColumnBase& col_base = get_column_base(ndx);
    TIGHTDB_ASSERT(m_spec.get_column_type(ndx) == col_type_Link ||
                   m_spec.get_column_type(ndx) == col_type_LinkList);
    const ColumnLinkBase& col_link_base = static_cast<const ColumnLinkBase&>(col_base);
    return col_link_base;
}

ColumnLinkBase& Table::get_column_link_base(size_t ndx)
{
    ColumnBase& col_base = get_column_base(ndx);
    TIGHTDB_ASSERT(m_spec.get_column_type(ndx) == col_type_Link ||
                   m_spec.get_column_type(ndx) == col_type_LinkList);
    ColumnLinkBase& col_link_base = static_cast<ColumnLinkBase&>(col_base);
    return col_link_base;
}

const ColumnLink& Table::get_column_link(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnLink, col_type_Link>(ndx);
}

ColumnLink& Table::get_column_link(size_t ndx)
{
    return get_column<ColumnLink, col_type_Link>(ndx);
}

const ColumnLinkList& Table::get_column_link_list(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnLinkList, col_type_LinkList>(ndx);
}

ColumnLinkList& Table::get_column_link_list(size_t ndx)
{
    return get_column<ColumnLinkList, col_type_LinkList>(ndx);
}

const ColumnBackLink& Table::get_column_back_link(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnBackLink, col_type_BackLink>(ndx);
}

ColumnBackLink& Table::get_column_back_link(size_t ndx)
{
    return get_column<ColumnBackLink, col_type_BackLink>(ndx);
}


void Table::validate_column_type(const ColumnBase& column, ColumnType col_type, size_t ndx) const
{
    ColumnType real_col_type = get_real_column_type(ndx);
    if (col_type == col_type_Int) {
        TIGHTDB_ASSERT(real_col_type == col_type_Int || real_col_type == col_type_Bool ||
                       real_col_type == col_type_DateTime);
    }
    else {
        TIGHTDB_ASSERT(col_type == real_col_type);
    }
    static_cast<void>(column);
    static_cast<void>(ndx);
    static_cast<void>(real_col_type);
}


size_t Table::get_size_from_ref(ref_type spec_ref, ref_type columns_ref,
                                Allocator& alloc) TIGHTDB_NOEXCEPT
{
    ColumnType first_col_type = ColumnType();
    if (!Spec::get_first_column_type_from_ref(spec_ref, alloc, first_col_type))
        return 0;
    const char* columns_header = alloc.translate(columns_ref);
    TIGHTDB_ASSERT(Array::get_size_from_header(columns_header) != 0);
    ref_type first_col_ref = to_ref(Array::get(columns_header, 0));
    size_t size = ColumnBase::get_size_from_type_and_ref(first_col_type, first_col_ref, alloc);
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
        dg_2.reset(mem.m_ref);
        int_fast64_t v(mem.m_ref); // FIXME: Dangerous case (unsigned -> signed)
        top.add(v); // Throws
        dg_2.release();
    }
    {
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_HasRefs, context_flag, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int_fast64_t v(mem.m_ref); // FIXME: Dangerous case (unsigned -> signed)
        top.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return top.get_ref();
}


ref_type Table::create_column(ColumnType col_type, size_t size, Allocator& alloc)
{
    switch (col_type) {
        case col_type_Int:
        case col_type_Bool:
        case col_type_DateTime: {
            int_fast64_t value = 0;
            return Column::create(Array::type_Normal, size, value, alloc); // Throws
        }
        case col_type_Float:
            return ColumnFloat::create(size, alloc); // Throws
        case col_type_Double:
            return ColumnDouble::create(size, alloc); // Throws
        case col_type_String:
            return AdaptiveStringColumn::create(size, alloc); // Throws
        case col_type_Binary:
            return ColumnBinary::create(size, alloc); // Throws
        case col_type_Table:
            return ColumnTable::create(size, alloc); // Throws
        case col_type_Mixed:
            return ColumnMixed::create(size, alloc); // Throws
        case col_type_Link:
            return ColumnLink::create(size, alloc); // Throws
        case col_type_LinkList:
            return ColumnLinkList::create(size, alloc); // Throws
        case col_type_BackLink:
            return ColumnBackLink::create(size, alloc); // Throws
        case col_type_StringEnum:
        case col_type_Reserved1:
        case col_type_Reserved4:
            break;
    }
    TIGHTDB_ASSERT(false);
    return 0;
}


ref_type Table::clone_columns(Allocator& alloc) const
{
    Array new_columns(Array::type_HasRefs, null_ptr, 0, alloc);
    size_t n = get_column_count();
    for (size_t i=0; i<n; ++i) {
        ref_type new_col_ref;
        const ColumnBase* col = &get_column_base(i);
        if (const ColumnStringEnum* enum_col = dynamic_cast<const ColumnStringEnum*>(col)) {
            AdaptiveStringColumn new_col(alloc);
            // FIXME: Should be optimized with something like
            // new_col.add(seq_tree_accessor.begin(),
            // seq_tree_accessor.end())
            size_t n2 = enum_col->size();
            for (size_t i2=0; i2<n2; ++i2)
                new_col.add(enum_col->get(i));
            new_col_ref = new_col.get_ref();
        }
        else {
            const Array& root = *col->get_root_array();
            MemRef mem = root.clone_deep(alloc); // Throws
            new_col_ref = mem.m_ref;
        }
        new_columns.add(new_col_ref);
    }
    return new_columns.get_ref();
}


ref_type Table::clone(Allocator& alloc) const
{
    if (m_top.is_attached()) {
        MemRef mem = m_top.clone_deep(alloc); // Throws
        return mem.m_ref;
    }

    Array new_top(alloc);
    _impl::DeepArrayDestroyGuard dg(&new_top);
    new_top.create(Array::type_HasRefs); // Throws
    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        MemRef mem = m_spec.m_top.clone_deep(alloc); // Throws
        dg_2.reset(mem.m_ref);
        int_fast64_t v(mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
        new_top.add(v); // Throws
        dg_2.release();
    }
    {
        MemRef mem = m_columns.clone_deep(alloc); // Throws
        dg_2.reset(mem.m_ref);
        int_fast64_t v(mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
        new_top.add(v); // Throws
        dg_2.release();
    }
    dg.release();
    return new_top.get_ref();
}


void Table::insert_empty_row(size_t row_ndx, size_t num_rows)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(row_ndx <= m_size);
    TIGHTDB_ASSERT(num_rows <= numeric_limits<size_t>::max() - row_ndx);
    bump_version();

    size_t num_cols = m_spec.get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& column = get_column_base(col_ndx);
        bool is_append = row_ndx == m_size;
        column.insert(row_ndx, num_rows, is_append); // Throws
    }
    adj_row_acc_insert_rows(row_ndx, num_rows);
    m_size += num_rows;

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_empty_rows(this, row_ndx, num_rows); // Throws
#endif
}


void Table::clear()
{
    TIGHTDB_ASSERT(is_attached());
    bump_version();

    size_t num_cols = m_spec.get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& column = get_column_base(col_ndx);
        column.clear(); // Throws
    }
    discard_row_accessors();
    m_size = 0;

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->clear_table(this); // Throws
#endif
}


void Table::do_remove(size_t row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(row_ndx < m_size);
    bump_version();

    bool is_last = row_ndx == m_size - 1;

    size_t num_cols = m_spec.get_column_count();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase& column = get_column_base(col_ndx);
        column.erase(row_ndx, is_last); // Throws
    }
    adj_row_acc_erase_row(row_ndx);
    --m_size;

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->erase_row(this, row_ndx); // Throws
#endif
}


void Table::move_last_over(size_t target_row_ndx)
{
    TIGHTDB_ASSERT(target_row_ndx < m_size);
    bump_version();

    // FIXME: PossibleLinkMergeConflict: `target_row_ndx` is now allowed to be equal to m_size-1. How does Group::TransactAdvancer deal with that?

    size_t last_row_ndx = m_size - 1;
    size_t num_cols = m_spec.get_column_count();
    if (target_row_ndx != last_row_ndx) {
        for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
            ColumnBase& column = get_column_base(col_ndx);
            column.move_last_over(target_row_ndx, last_row_ndx); // Throws
        }
    }
    else {
        for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
            ColumnBase& column = get_column_base(col_ndx);
            bool is_last = true;
            column.erase(target_row_ndx, is_last); // Throws
        }
    }
    adj_row_acc_move_last_over(target_row_ndx, last_row_ndx);
    --m_size;

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->move_last_over(this, target_row_ndx, last_row_ndx); // Throws
#endif
}


void Table::insert_subtable(size_t col_ndx, size_t row_ndx, const Table* table)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Table);
    TIGHTDB_ASSERT(row_ndx <= m_size);

    ColumnTable& subtables = get_column_table(col_ndx);
    subtables.insert(row_ndx, table);

    // FIXME: Replication is not yet able to handle copying insertion of non-empty tables.
#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_table(this, col_ndx, row_ndx); // Throws
#endif
}


void Table::set_subtable(size_t col_ndx, size_t row_ndx, const Table* table)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Table);
    TIGHTDB_ASSERT(row_ndx < m_size);
    bump_version();

    ColumnTable& subtables = get_column_table(col_ndx);
    subtables.set(row_ndx, table);

    // FIXME: Replication is not yet able to handle copying insertion of non-empty tables.
#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_table(this, col_ndx, row_ndx); // Throws
#endif
}


void Table::insert_mixed_subtable(size_t col_ndx, size_t row_ndx, const Table* t)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Mixed);
    TIGHTDB_ASSERT(row_ndx <= m_size);

    ColumnMixed& mixed_col = get_column_mixed(col_ndx);
    mixed_col.insert_subtable(row_ndx, t);

    // FIXME: Replication is not yet able to handle copuing insertion of non-empty tables.
#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_mixed(this, col_ndx, row_ndx, Mixed::subtable_tag()); // Throws
#endif
}


void Table::set_mixed_subtable(size_t col_ndx, size_t row_ndx, const Table* t)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Mixed);
    TIGHTDB_ASSERT(row_ndx < m_size);
    bump_version();

    ColumnMixed& mixed_col = get_column_mixed(col_ndx);
    mixed_col.set_subtable(row_ndx, t);

    // FIXME: Replication is not yet able to handle copying assignment of non-empty tables.
#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_mixed(this, col_ndx, row_ndx, Mixed::subtable_tag()); // Throws
#endif
}


Table* Table::get_subtable_accessor(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    // If this table is not a degenerate subtable, then `col_ndx` must be a
    // valid index into `m_cols`.
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_cols.size());
    // The column accessor may not exist yet, but in that case the subtable
    // accessor cannot exist either. Column accessors are missing only during
    // certian operations such as the the updating of the accessor tree when a
    // read transactions is advanced.
    if (m_columns.is_attached()) {
        if (ColumnBase* col = m_cols[col_ndx])
            return col->get_subtable_accessor(row_ndx);
    }
    return 0;
}


Table* Table::get_link_target_table_accessor(size_t col_ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    // So far, link columns can only exist in group-level tables, so this table
    // cannot be degenerate.
    TIGHTDB_ASSERT(m_columns.is_attached());
    TIGHTDB_ASSERT(col_ndx < m_cols.size());
    if (ColumnBase* col = m_cols[col_ndx]) {
        TIGHTDB_ASSERT(dynamic_cast<ColumnLinkBase*>(col));
        return static_cast<ColumnLinkBase*>(col)->get_target_table();
    }
    return 0;
}


void Table::discard_subtable_accessor(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    TIGHTDB_ASSERT(is_attached());
    // If this table is not a degenerate subtable, then `col_ndx` must be a
    // valid index into `m_cols`.
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_cols.size());
    if (ColumnBase* col = m_cols[col_ndx])
        col->discard_subtable_accessor(row_ndx);
}


Table* Table::get_subtable_ptr(size_t col_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(row_ndx < m_size);

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_Table) {
        ColumnTable& subtables = get_column_table(col_ndx);
        return subtables.get_subtable_ptr(row_ndx); // Throws
    }
    if (type == col_type_Mixed) {
        ColumnMixed& subtables = get_column_mixed(col_ndx);
        return subtables.get_subtable_ptr(row_ndx); // Throws
    }
    TIGHTDB_ASSERT(false);
    return 0;
}


size_t Table::get_subtable_size(size_t col_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(row_ndx < m_size);

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_Table) {
        const ColumnTable& subtables = get_column_table(col_ndx);
        return subtables.get_subtable_size(row_ndx);
    }
    if (type == col_type_Mixed) {
        const ColumnMixed& subtables = get_column_mixed(col_ndx);
        return subtables.get_subtable_size(row_ndx);
    }
    TIGHTDB_ASSERT(false);
    return 0;
}


void Table::clear_subtable(size_t col_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(row_ndx <= m_size);
    bump_version();

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_Table) {
        ColumnTable& subtables = get_column_table(col_ndx);
        subtables.set(row_ndx, 0);

#ifdef TIGHTDB_ENABLE_REPLICATION
        if (Replication* repl = get_repl())
            repl->set_table(this, col_ndx, row_ndx); // Throws
#endif
    }
    else if (type == col_type_Mixed) {
        ColumnMixed& subtables = get_column_mixed(col_ndx);
        subtables.set_subtable(row_ndx, 0);

#ifdef TIGHTDB_ENABLE_REPLICATION
        if (Replication* repl = get_repl())
            repl->set_mixed(this, col_ndx, row_ndx, Mixed::subtable_tag()); // Throws
#endif
    }
    else {
        TIGHTDB_ASSERT(false);
    }
}


Group* Table::get_parent_group() const TIGHTDB_NOEXCEPT
{
    if (!m_top.is_attached())
        return null_ptr;

    Parent* parent = static_cast<Parent*>(m_top.get_parent()); // ArrayParent guaranteed to be Table::Parent
    if (!parent || !parent->is_parent_group())
        return null_ptr;

    Group* group = static_cast<Group*>(parent);
    return group;
}


const Table* Table::get_parent_table_ptr(size_t* column_ndx_out) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    const Array& real_top = m_top.is_attached() ? m_top : m_columns;
    if (ArrayParent* array_parent = real_top.get_parent()) {
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(array_parent));
        Parent* table_parent = static_cast<Parent*>(array_parent);
        return table_parent->get_parent_table(column_ndx_out);
    }
    return 0;
}


size_t Table::get_index_in_parent() const TIGHTDB_NOEXCEPT
{
    const Array& real_top = m_top.is_attached() ? m_top : m_columns;
    ArrayParent* parent = real_top.get_parent();
    if (!parent)
        return npos;
    size_t index_in_parent = real_top.get_ndx_in_parent();
    return index_in_parent;
}


int64_t Table::get_int(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = get_column(column_ndx);
    return column.get(ndx);
}

void Table::set_int(size_t column_ndx, size_t ndx, int_fast64_t value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    Column& column = get_column(column_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_int(this, column_ndx, ndx, value); // Throws
#endif
}

void Table::add_int(size_t column_ndx, int64_t value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == col_type_Int);
    bump_version();
    get_column(column_ndx).adjust(value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->add_int_to_column(this, column_ndx, value); // Throws
#endif
}

bool Table::get_bool(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == col_type_Bool);
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = get_column(column_ndx);
    return column.get(ndx) != 0;
}

void Table::set_bool(size_t column_ndx, size_t ndx, bool value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == col_type_Bool);
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    Column& column = get_column(column_ndx);
    column.set(ndx, value ? 1 : 0);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_bool(this, column_ndx, ndx, value); // Throws
#endif
}

DateTime Table::get_datetime(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == col_type_DateTime);
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = get_column(column_ndx);
    return time_t(column.get(ndx));
}

void Table::set_datetime(size_t column_ndx, size_t ndx, DateTime value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == col_type_DateTime);
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    Column& column = get_column(column_ndx);
    column.set(ndx, int64_t(value.get_datetime()));

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_date_time(this, column_ndx, ndx, value); // Throws
#endif
}

void Table::insert_int(size_t column_ndx, size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    Column& column = get_column(column_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_int(this, column_ndx, ndx, value); // Throws
#endif
}


float Table::get_float(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnFloat& column = get_column_float(column_ndx);
    return column.get(ndx);
}

void Table::set_float(size_t column_ndx, size_t ndx, float value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnFloat& column = get_column_float(column_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_float(this, column_ndx, ndx, value); // Throws
#endif
}

void Table::insert_float(size_t column_ndx, size_t ndx, float value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnFloat& column = get_column_float(column_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_float(this, column_ndx, ndx, value); // Throws
#endif
}


double Table::get_double(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnDouble& column = get_column_double(column_ndx);
    return column.get(ndx);
}

void Table::set_double(size_t column_ndx, size_t ndx, double value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnDouble& column = get_column_double(column_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_double(this, column_ndx, ndx, value); // Throws
#endif
}

void Table::insert_double(size_t column_ndx, size_t ndx, double value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnDouble& column = get_column_double(column_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_double(this, column_ndx, ndx, value); // Throws
#endif
}


StringData Table::get_string(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    ColumnType type = get_real_column_type(column_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(column_ndx);
        return column.get(ndx);
    }

    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(column_ndx);
    return column.get(ndx);
}

void Table::set_string(size_t column_ndx, size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnType type = get_real_column_type(column_ndx);
    if (type == col_type_String) {
        AdaptiveStringColumn& column = get_column_string(column_ndx);
        column.set(ndx, value);
    }
    else {
        TIGHTDB_ASSERT(type == col_type_StringEnum);
        ColumnStringEnum& column = get_column_string_enum(column_ndx);
        column.set(ndx, value);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_string(this, column_ndx, ndx, value); // Throws
#endif
}

void Table::insert_string(size_t column_ndx, size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnType type = get_real_column_type(column_ndx);
    if (type == col_type_String) {
        AdaptiveStringColumn& column = get_column_string(column_ndx);
        column.insert(ndx, value);
    }
    else {
        TIGHTDB_ASSERT(type == col_type_StringEnum);
        ColumnStringEnum& column = get_column_string_enum(column_ndx);
        column.insert(ndx, value);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_string(this, column_ndx, ndx, value); // Throws
#endif
}


BinaryData Table::get_binary(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnBinary& column = get_column_binary(column_ndx);
    return column.get(ndx);
}

void Table::set_binary(size_t column_ndx, size_t ndx, BinaryData value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnBinary& column = get_column_binary(column_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_binary(this, column_ndx, ndx, value); // Throws
#endif
}

void Table::insert_binary(size_t column_ndx, size_t ndx, BinaryData value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnBinary& column = get_column_binary(column_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_binary(this, column_ndx, ndx, value); // Throws
#endif
}


Mixed Table::get_mixed(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnMixed& column = get_column_mixed(column_ndx);

    DataType type = column.get_type(ndx);
    switch (type) {
        case type_Int:
            return Mixed(column.get_int(ndx));
        case type_Bool:
            return Mixed(column.get_bool(ndx));
        case type_DateTime:
            return Mixed(DateTime(column.get_datetime(ndx)));
        case type_Float:
            return Mixed(column.get_float(ndx));
        case type_Double:
            return Mixed(column.get_double(ndx));
        case type_String:
            return Mixed(column.get_string(ndx)); // Throws
        case type_Binary:
            return Mixed(column.get_binary(ndx)); // Throws
        case type_Table:
            return Mixed::subtable_tag();
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            break;
    }
    TIGHTDB_ASSERT(false);
    return Mixed(int64_t(0));
}

DataType Table::get_mixed_type(size_t column_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnMixed& column = get_column_mixed(column_ndx);
    return column.get_type(ndx);
}

void Table::set_mixed(size_t column_ndx, size_t ndx, Mixed value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnMixed& column = get_column_mixed(column_ndx);
    DataType type = value.get_type();

    switch (type) {
        case type_Int:
            column.set_int(ndx, value.get_int());
            break;
        case type_Bool:
            column.set_bool(ndx, value.get_bool());
            break;
        case type_DateTime:
            column.set_datetime(ndx, value.get_datetime());
            break;
        case type_Float:
            column.set_float(ndx, value.get_float());
            break;
        case type_Double:
            column.set_double(ndx, value.get_double());
            break;
        case type_String:
            column.set_string(ndx, value.get_string());
            break;
        case type_Binary:
            column.set_binary(ndx, value.get_binary());
            break;
        case type_Table:
            column.set_subtable(ndx, 0);
            break;
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            TIGHTDB_ASSERT(false);
            break;
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_mixed(this, column_ndx, ndx, value); // Throws
#endif
}

void Table::insert_mixed(size_t column_ndx, size_t ndx, Mixed value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnMixed& column = get_column_mixed(column_ndx);
    DataType type = value.get_type();

    switch (type) {
        case type_Int:
            column.insert_int(ndx, value.get_int());
            break;
        case type_Bool:
            column.insert_bool(ndx, value.get_bool());
            break;
        case type_DateTime:
            column.insert_datetime(ndx, value.get_datetime());
            break;
        case type_Float:
            column.insert_float(ndx, value.get_float());
            break;
        case type_Double:
            column.insert_double(ndx, value.get_double());
            break;
        case type_String:
            column.insert_string(ndx, value.get_string());
            break;
        case type_Binary:
            column.insert_binary(ndx, value.get_binary());
            break;
        case type_Table:
            column.insert_subtable(ndx, 0);
            break;
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            TIGHTDB_ASSERT(false);
            break;
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_mixed(this, column_ndx, ndx, value); // Throws
#endif
}


size_t Table::get_link(size_t col_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(row_ndx < m_size);
    const ColumnLink& column = get_column_link(col_ndx);
    return column.get_link(row_ndx);
}

TableRef Table::get_link_target(size_t col_ndx) TIGHTDB_NOEXCEPT
{
    ColumnLinkBase& column = get_column_link_base(col_ndx);
    return column.get_target_table()->get_table_ref();
}

void Table::set_link(size_t col_ndx, size_t row_ndx, size_t target_row_ndx)
{
    TIGHTDB_ASSERT(row_ndx < m_size);
    bump_version();
    ColumnLink& column = get_column_link(col_ndx);
    column.set_link(row_ndx, target_row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl()) {
        size_t link = 1 + target_row_ndx;
        repl->set_link(this, col_ndx, row_ndx, link); // Throws
    }
#endif
}

void Table::insert_link(size_t col_ndx, size_t row_ndx, size_t target_row_ndx)
{
    TIGHTDB_ASSERT(row_ndx == m_size); // can only append to unorded tables
    ColumnLink& column = get_column_link(col_ndx);
    column.insert_link(row_ndx, target_row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl()) {
        size_t link = 1 + target_row_ndx;
        repl->insert_link(this, col_ndx, row_ndx, link); // Throws
    }
#endif
}

bool Table::is_null_link(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_size);
    const ColumnLink& column = get_column_link(col_ndx);
    return column.is_null_link(ndx);
}

void Table::nullify_link(size_t col_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT(row_ndx < m_size);
    bump_version();
    ColumnLink& column = get_column_link(col_ndx);
    column.nullify_link(row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl()) {
        size_t link = 0; // Null-link
        repl->set_link(this, col_ndx, row_ndx, link); // Throws
    }
#endif
}


void Table::insert_linklist(size_t col_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT(row_ndx == m_size); // can only append to unorded tables
    ColumnLinkList& column = get_column_link_list(col_ndx);
    column.insert(row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_link_list(this, col_ndx, row_ndx); // Throws
#endif
}

ConstLinkViewRef Table::get_linklist(size_t col_ndx, size_t row_ndx) const
{
    TIGHTDB_ASSERT(row_ndx < m_size);
    const ColumnLinkList& column = get_column_link_list(col_ndx);
    return column.get(row_ndx);
}

LinkViewRef Table::get_linklist(size_t col_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT(row_ndx < m_size);
    // FIXME: this looks wrong! It should instead be the modifying operations of
    // LinkView that bump the change count of the containing table.
    ColumnLinkList& column = get_column_link_list(col_ndx);
    return column.get(row_ndx);
}

bool Table::linklist_is_empty(size_t col_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(row_ndx < m_size);
    const ColumnLinkList& column = get_column_link_list(col_ndx);
    return !column.has_links(row_ndx);
}

size_t Table::get_link_count(size_t col_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(row_ndx < m_size);
    const ColumnLinkList& column = get_column_link_list(col_ndx);
    return column.get_link_count(row_ndx);
}


void Table::insert_done()
{
    bump_version();

    size_t row_ndx = m_size;
    size_t num_rows = 1;
    adj_row_acc_insert_rows(row_ndx, num_rows);

    ++m_size;

    // If the table has backlinks, the columns containing them will
    // not be exposed to the users. So we have to manually extend them
    // after inserts. Note that you can only have backlinks on unordered
    // tables, so inserts will only be used for appends.
    if (m_spec.has_backlinks()) {
        size_t backlinks_start = m_spec.get_public_column_count();
        size_t column_count = m_spec.get_column_count();

        for (size_t i = backlinks_start; i < column_count; ++i) {
            ColumnBackLink& column = get_column_back_link(i);
            column.add_row();
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->row_insert_complete(this); // Throws
#endif
}


// count ----------------------------------------------

size_t Table::count_int(size_t column_ndx, int64_t value) const
{
    if(!m_columns.is_attached())
        return 0;

    const Column& column = get_column<Column, col_type_Int>(column_ndx);
    return column.count(value);
}
size_t Table::count_float(size_t column_ndx, float value) const
{
    if(!m_columns.is_attached())
        return 0;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(column_ndx);
    return column.count(value);
}
size_t Table::count_double(size_t column_ndx, double value) const
{
    if(!m_columns.is_attached())
        return 0;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(column_ndx);
    return column.count(value);
}
size_t Table::count_string(size_t column_ndx, StringData value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < get_column_count());

    if(!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(column_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(column_ndx);
        return column.count(value);
    }
    else {
        TIGHTDB_ASSERT(type == col_type_StringEnum);
        const ColumnStringEnum& column = get_column_string_enum(column_ndx);
        return column.count(value);
    }
}

// sum ----------------------------------------------

int64_t Table::sum_int(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

    const Column& column = get_column<Column, col_type_Int>(column_ndx);
    return column.sum();
}
double Table::sum_float(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(column_ndx);
    return column.sum();
}
double Table::sum_double(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(column_ndx);
    return column.sum();
}

// average ----------------------------------------------

double Table::average_int(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

    const Column& column = get_column<Column, col_type_Int>(column_ndx);
    return column.average();
}
double Table::average_float(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(column_ndx);
    return column.average();
}
double Table::average_double(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(column_ndx);
    return column.average();
}

// minimum ----------------------------------------------

#define USE_COLUMN_AGGREGATE 1

int64_t Table::minimum_int(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

#if USE_COLUMN_AGGREGATE
    const Column& column = get_column<Column, col_type_Int>(column_ndx);
    return column.minimum();
#else
    if (is_empty())
        return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        int64_t v = get_int(column_ndx, i);
        if (v < mv) {
            mv = v;
        }
    }
    return mv;
#endif
}

float Table::minimum_float(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(column_ndx);
    return column.minimum();
}
double Table::minimum_double(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(column_ndx);
    return column.minimum();
}

// maximum ----------------------------------------------

int64_t Table::maximum_int(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

#if USE_COLUMN_AGGREGATE
    const Column& column = get_column<Column, col_type_Int>(column_ndx);
    return column.maximum();
#else
    if (is_empty())
        return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        int64_t v = get_int(column_ndx, i);
        if (v > mv) {
            mv = v;
        }
    }
    return mv;
#endif
}
float Table::maximum_float(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(column_ndx);
    return column.maximum();
}
double Table::maximum_double(size_t column_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(column_ndx);
    return column.maximum();
}



size_t Table::lookup(StringData value) const
{
    // Early-out if this is a degenerate subtable
    if(!m_columns.is_attached())
        return not_found;

    // First time we do a lookup we check if we can cache the index
    if (!m_search_index) {
        if (get_column_count() < 1)
            return not_found; // no column to lookup in

        ColumnType type = get_real_column_type(0);
        if (type == col_type_String) {
            const AdaptiveStringColumn& column = get_column_string(0);
            if (!column.has_index()) {
                return column.find_first(value);
            }
            else {
                m_search_index = &column.get_index();
            }
        }
        else if (type == col_type_StringEnum) {
            const ColumnStringEnum& column = get_column_string_enum(0);
            if (!column.has_index()) {
                return column.find_first(value);
            }
            else {
                m_search_index = &column.get_index();
            }
        }
        else {
            return not_found; // invalid column type
        }
    }

    // Do lookup directly on cached index
    return m_search_index->find_first(value);
}

template <class T> size_t Table::find_first(size_t column_ndx, T value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == ColumnTypeTraits3<T>::ct_id_real);

    if(!m_columns.is_attached())
        return not_found;

    typedef typename ColumnTypeTraits3<T>::column_type ColType;
    const ColType& column = get_column<ColType, ColumnTypeTraits3<T>::ct_id>(column_ndx);
    return column.find_first(value);
}

size_t Table::find_first_int(size_t column_ndx, int64_t value) const
{
    return find_first<int64_t>(column_ndx, value);
}

size_t Table::find_first_bool(size_t column_ndx, bool value) const
{
    return find_first<bool>(column_ndx, value);
}

size_t Table::find_first_datetime(size_t column_ndx, DateTime value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    TIGHTDB_ASSERT(get_real_column_type(column_ndx) == col_type_DateTime);

    if(!m_columns.is_attached())
        return not_found;

    const Column& column = get_column(column_ndx);

    return column.find_first(int64_t(value.get_datetime()));
}

size_t Table::find_first_float(size_t column_ndx, float value) const
{
    return find_first<float>(column_ndx, value);
}

size_t Table::find_first_double(size_t column_ndx, double value) const
{
    return find_first<double>(column_ndx, value);
}

size_t Table::find_first_string(size_t column_ndx, StringData value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    if(!m_columns.is_attached())
        return not_found;

    ColumnType type = get_real_column_type(column_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(column_ndx);
        return column.find_first(value);
    }
    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(column_ndx);
    return column.find_first(value);
}

size_t Table::find_first_binary(size_t, BinaryData) const
{
    // FIXME: Implement this!
    throw runtime_error("Not implemented");
}


template <class T> TableView Table::find_all(size_t column_ndx, T value)
{
    return where().equal(column_ndx, value).find_all();
}


TableView Table::find_all_int(size_t column_ndx, int64_t value)
{
    return find_all<int64_t>(column_ndx, value);
}

ConstTableView Table::find_all_int(size_t column_ndx, int64_t value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(column_ndx, value);
}

TableView Table::find_all_bool(size_t column_ndx, bool value)
{
    return find_all<bool>(column_ndx, value);
}

ConstTableView Table::find_all_bool(size_t column_ndx, bool value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(column_ndx, value);
}


TableView Table::find_all_float(size_t column_ndx, float value)
{
    return find_all<float>(column_ndx, value);
}

ConstTableView Table::find_all_float(size_t column_ndx, float value) const
{
    return const_cast<Table*>(this)->find_all<float>(column_ndx, value);
}

TableView Table::find_all_double(size_t column_ndx, double value)
{
    return find_all<double>(column_ndx, value);
}

ConstTableView Table::find_all_double(size_t column_ndx, double value) const
{
    return const_cast<Table*>(this)->find_all<double>(column_ndx, value);
}

TableView Table::find_all_datetime(size_t column_ndx, DateTime value)
{
    return find_all<int64_t>(column_ndx, int64_t(value.get_datetime()));
}

ConstTableView Table::find_all_datetime(size_t column_ndx, DateTime value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(column_ndx, int64_t(value.get_datetime()));
}

TableView Table::find_all_string(size_t column_ndx, StringData value)
{
    return where().equal(column_ndx, value).find_all();
}

ConstTableView Table::find_all_string(size_t column_ndx, StringData value) const
{
    return const_cast<Table*>(this)->find_all_string(column_ndx, value);
}

TableView Table::find_all_binary(size_t, BinaryData)
{
    // FIXME: Implement this!
    throw runtime_error("Not implemented");
}

ConstTableView Table::find_all_binary(size_t, BinaryData) const
{
    // FIXME: Implement this!
    throw runtime_error("Not implemented");
}

TableView Table::get_distinct_view(size_t column_ndx)
{
    // FIXME: lacks support for reactive updates
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    TIGHTDB_ASSERT(has_index(column_ndx));

    TableView tv(*this);
    Column& refs = tv.get_ref_column();

    if(m_columns.is_attached()) {
        ColumnType type = get_real_column_type(column_ndx);
        if (type == col_type_String) {
            const AdaptiveStringColumn& column = get_column_string(column_ndx);
            const StringIndex& index = column.get_index();
            index.distinct(refs);
        }
        else {
            TIGHTDB_ASSERT(type == col_type_StringEnum);
            const ColumnStringEnum& column = get_column_string_enum(column_ndx);
            const StringIndex& index = column.get_index();
            index.distinct(refs);
        }
    }
    return tv;
}

ConstTableView Table::get_distinct_view(size_t column_ndx) const
{
    return const_cast<Table*>(this)->get_distinct_view(column_ndx);
}

TableView Table::get_sorted_view(size_t column_ndx, bool ascending)
{
    TableView tv = where().find_all();
    tv.sort(column_ndx, ascending);
    return tv;
}

ConstTableView Table::get_sorted_view(size_t column_ndx, bool ascending) const
{
    return const_cast<Table*>(this)->get_sorted_view(column_ndx, ascending);
}


namespace {

struct AggrState {
    AggrState() : block(Array::no_prealloc_tag()), added_row(false) {}

    const Table* table;
    const StringIndex* dst_index;
    size_t group_by_column;

    const ColumnStringEnum* enums;
    vector<size_t> keys;
    Array block;
    size_t offset;
    size_t block_end;

    bool added_row;
};

typedef size_t (*get_group_fnc)(size_t, AggrState&, Table&);

size_t get_group_ndx(size_t i, AggrState& state, Table& result)
{
    StringData str = state.table->get_string(state.group_by_column, i);
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
        state.enums->Column::GetBlock(i, state.block, state.offset);
        state.block_end = state.offset + state.block.size();
    }

    // Since we know the exact number of distinct keys,
    // we can use that to avoid index lookups
    int64_t key = state.block.get(i - state.offset);
    size_t ndx = state.keys[key];

    // Stored position is offset by one, so zero can indicate
    // that no entry have been added yet.
    if (ndx == 0) {
        ndx = result.add_empty_row();
        result.set_string(0, ndx, state.enums->get(i));
        state.keys[key] = ndx+1;
        state.added_row = true;
    }
    else
        --ndx;
    return ndx;
}

} //namespace

// Simple pivot aggregate method. Experimental! Please do not document method publicly.
void Table::aggregate(size_t group_by_column, size_t aggr_column, AggrType op, Table& result,
                      const Column* viewrefs) const
{
    TIGHTDB_ASSERT(result.is_empty() && result.get_column_count() == 0);
    TIGHTDB_ASSERT(group_by_column < m_columns.size());
    TIGHTDB_ASSERT(aggr_column < m_columns.size());

    TIGHTDB_ASSERT(get_column_type(group_by_column) == type_String);
    TIGHTDB_ASSERT(op == aggr_count || get_column_type(aggr_column) == type_Int);

    // Add columns to result table
    result.add_column(type_String, get_column_name(group_by_column));

    if (op == aggr_count)
        result.add_column(type_Int, "COUNT()");
    else
        result.add_column(type_Int, get_column_name(aggr_column));

    // Cache columms
    const Column& src_column = get_column(aggr_column);
    Column& dst_column = result.get_column(1);

    AggrState state;
    get_group_fnc get_group_ndx_fnc = NULL;

    // When doing grouped aggregates, the column to group on is likely
    // to be auto-enumerated (without a lot of duplicates grouping does not
    // make much sense). So we can use this knowledge to optimize the process.
    ColumnType key_type = get_real_column_type(group_by_column);
    if (key_type == col_type_StringEnum) {
        const ColumnStringEnum& enums = get_column_string_enum(group_by_column);
        size_t key_count = enums.get_keys().size();

        state.enums = &enums;
        state.keys.assign(key_count, 0);

        enums.Column::GetBlock(0, state.block, state.offset);
        state.block_end = state.offset + state.block.size();
        get_group_ndx_fnc = &get_group_ndx_blocked;
    }
    else {
        // If the group_by column is not auto-enumerated, we have to do
        // (more expensive) direct lookups.
        result.set_index(0);
        const StringIndex& dst_index = result.get_column_string(0).get_index();

        state.table = this;
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
                    size_t i = viewrefs->get(r);
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // Count
                    dst_column.adjust(ndx, 1);
                }
                break;
            case aggr_sum:
                for (size_t r = 0; r < count; ++r) {
                    size_t i = viewrefs->get(r);
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // Sum
                    int64_t value = src_column.get(i);
                    dst_column.adjust(ndx, value);
                }
                break;
            case aggr_avg:
            {
                // Add temporary column for counts
                result.add_column(type_Int, "count");
                Column& cnt_column = result.get_column(2);

                for (size_t r = 0; r < count; ++r) {
                    size_t i = viewrefs->get(r);
                    size_t ndx = (*get_group_ndx_fnc)(i, state, result);

                    // SUM
                    int64_t value = src_column.get(i);
                    dst_column.adjust(ndx, value);

                    // Increment count
                    cnt_column.adjust(ndx, 1);
                }

                // Calculate averages
                result.add_column(type_Double, "average");
                ColumnDouble& mean_column = result.get_column_double(3);
                const size_t res_count = result.size();
                for (size_t i = 0; i < res_count; ++i) {
                    int64_t sum   = dst_column.get(i);
                    int64_t count = cnt_column.get(i);
                    double res   = double(sum) / double(count);
                    mean_column.set(i, res);
                }

                // Remove temp columns
                result.remove_column(1); // sums
                result.remove_column(1); // counts
                break;
            }
            case aggr_min:
                for (size_t r = 0; r < count; ++r) {
                    size_t i = viewrefs->get(r);

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
                    size_t i = viewrefs->get(r);

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
            case aggr_avg:
            {
                // Add temporary column for counts
                result.add_column(type_Int, "count");
                Column& cnt_column = result.get_column(2);

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
                ColumnDouble& mean_column = result.get_column_double(3);
                const size_t res_count = result.size();
                for (size_t i = 0; i < res_count; ++i) {
                    int64_t sum   = dst_column.get(i);
                    int64_t count = cnt_column.get(i);
                    double res    = double(sum) / double(count);
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
    TIGHTDB_ASSERT(!m_columns.is_attached() || end < size());

    TableView ctv(*this);
    if (m_columns.is_attached()) {
        Column& refs = ctv.get_ref_column();
        for (size_t i = begin; i < end; ++i)
            refs.add(i);
    }
    return ctv;
}

ConstTableView Table::get_range_view(size_t begin, size_t end) const
{
    return const_cast<Table*>(this)->get_range_view(begin, end);
}



size_t Table::lower_bound_int(size_t column_ndx, int64_t value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(column_ndx).lower_bound_int(value);
}

size_t Table::upper_bound_int(size_t column_ndx, int64_t value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(column_ndx).upper_bound_int(value);
}

size_t Table::lower_bound_bool(size_t column_ndx, bool value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(column_ndx).lower_bound_int(value);
}

size_t Table::upper_bound_bool(size_t column_ndx, bool value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(column_ndx).upper_bound_int(value);
}

size_t Table::lower_bound_float(size_t column_ndx, float value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_float(column_ndx).lower_bound(value);
}

size_t Table::upper_bound_float(size_t column_ndx, float value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_float(column_ndx).upper_bound(value);
}

size_t Table::lower_bound_double(size_t column_ndx, double value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_double(column_ndx).lower_bound(value);
}

size_t Table::upper_bound_double(size_t column_ndx, double value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_double(column_ndx).upper_bound(value);
}

size_t Table::lower_bound_string(size_t column_ndx, StringData value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    if(!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(column_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(column_ndx);
        return column.lower_bound_string(value);
    }
    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(column_ndx);
    return column.lower_bound_string(value);
}

size_t Table::upper_bound_string(size_t column_ndx, StringData value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || column_ndx < m_columns.size());
    if(!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(column_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(column_ndx);
        return column.upper_bound_string(value);
    }
    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(column_ndx);
    return column.upper_bound_string(value);
}

void Table::optimize()
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
        ColumnType type = get_real_column_type(i);
        if (type == col_type_String) {
            AdaptiveStringColumn* column = &get_column_string(i);

            ref_type keys_ref, values_ref;
            bool res = column->auto_enumerate(keys_ref, values_ref);
            if (!res)
                continue;

            Spec::ColumnInfo info;
            m_spec.get_column_info(i, info);
            ArrayParent* keys_parent;
            size_t keys_ndx;
            m_spec.upgrade_string_to_enum(i, keys_ref, keys_parent, keys_ndx);

            // Upgrading the column may have moved the
            // refs to keylists in other columns so we
            // have to update their parent info
            for (size_t c = i+1; c < m_cols.size(); ++c) {
                ColumnType type = get_real_column_type(c);
                if (type == col_type_StringEnum) {
                    ColumnStringEnum& column = get_column_string_enum(c);
                    column.adjust_keys_ndx_in_parent(1);
                }
            }

            // Indexes are also in m_columns, so we need adjusted pos
            size_t pos_in_mcolumns = m_spec.get_column_pos(i);

            // Replace column
            ColumnStringEnum* e =
                new ColumnStringEnum(keys_ref, values_ref, &m_columns,
                                     pos_in_mcolumns, keys_parent, keys_ndx, alloc);
            m_columns.set(pos_in_mcolumns, values_ref);
            m_cols[i] = e;

            // Inherit any existing index
            if (info.m_has_index) {
                StringIndex* index = column->release_index();
                e->install_index(index);
            }

            // Clean up the old column
            column->destroy();
            delete column;
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->optimize_table(this); // Throws
#endif
}


class Table::SliceWriter: public Group::TableWriter {
public:
    SliceWriter(const Table& table, StringData table_name,
                size_t offset, size_t size) TIGHTDB_NOEXCEPT:
        m_table(table),
        m_table_name(table_name),
        m_offset(offset),
        m_size(size)
    {
    }

    size_t write_names(_impl::OutputStream& out) TIGHTDB_OVERRIDE
    {
        Allocator& alloc = Allocator::get_default();
        ArrayString table_names(alloc);
        table_names.create(); // Throws
        _impl::DestroyGuard<ArrayString> dg(&table_names);
        table_names.add(m_table_name); // Throws
        size_t pos = table_names.write(out); // Throws
        return pos;
    }

    size_t write_tables(_impl::OutputStream& out) TIGHTDB_OVERRIDE
    {
        Allocator& alloc = Allocator::get_default();

        // Make a copy of the spec of this table, modify it, and then
        // write it to the output stream
        ref_type spec_ref;
        {
            MemRef mem = m_table.m_spec.m_top.clone_deep(alloc); // Throws
            Spec spec(alloc);
            spec.init(mem); // Throws
            _impl::DestroyGuard<Spec> dg(&spec);
            size_t n = spec.get_column_count();
            for (size_t i = 0; i != n; ++i) {
                ColumnAttr attr = spec.get_column_attr(i);
                // Remove any index specifying attributes
                attr = ColumnAttr(attr & ~col_attr_Indexed);
                spec.set_column_attr(i, attr); // Throws
            }
            size_t pos = spec.m_top.write(out); // Throws
            spec_ref = pos;
        }

        // Make a copy of the selected slice of each column
        ref_type columns_ref;
        {
            Array column_refs(alloc);
            column_refs.create(Array::type_HasRefs); // Throws
            _impl::ShallowArrayDestroyGuard dg(&column_refs);
            size_t table_size = m_table.size();
            size_t n = m_table.m_cols.size();
            for (size_t i = 0; i != n; ++i) {
                ColumnBase* column = m_table.m_cols[i];
                ref_type ref = column->write(m_offset, m_size, table_size, out); // Throws
                int_fast64_t ref_2(ref); // FIXME: Dangerous cast (unsigned -> signed)
                column_refs.add(ref_2); // Throws
            }
            bool recurse = false; // Shallow
            size_t pos = column_refs.write(out, recurse); // Throws
            columns_ref = pos;
        }

        // Create a new top array for the table
        ref_type table_top_ref;
        {
            Array table_top(alloc);
            table_top.create(Array::type_HasRefs); // Throws
            _impl::ShallowArrayDestroyGuard dg(&table_top);
            int_fast64_t spec_ref_2(spec_ref); // FIXME: Dangerous cast (unsigned -> signed)
            table_top.add(spec_ref_2); // Throws
            int_fast64_t columns_ref_2(columns_ref); // FIXME: Dangerous cast (unsigned -> signed)
            table_top.add(columns_ref_2); // Throws
            bool recurse = false; // Shallow
            size_t pos = table_top.write(out, recurse); // Throws
            table_top_ref = pos;
        }

        // Create the array of tables of size one
        Array tables(alloc);
        tables.create(Array::type_HasRefs); // Throws
        _impl::ShallowArrayDestroyGuard dg(&tables);
        int_fast64_t table_top_ref_2(table_top_ref); // FIXME: Dangerous cast (unsigned -> signed)
        tables.add(table_top_ref_2); // Throws
        bool recurse = false; // Shallow
        size_t pos = tables.write(out, recurse); // Throws
        return pos;
    }

private:
    const Table& m_table;
    const StringData m_table_name;
    const size_t m_offset, m_size;
};

void Table::write(ostream& out, size_t offset, size_t size, StringData override_table_name) const
{
    size_t table_size = this->size();
    if (offset > table_size)
        throw out_of_range("Offset is out of range");
    size_t remaining_size = table_size - offset;
    size_t size_2 = size;
    if (size_2 > remaining_size)
        size_2 = remaining_size;
    StringData table_name = override_table_name;
    if (!table_name)
        table_name = get_name();
    SliceWriter writer(*this, table_name, offset, size_2);
    Group::write(out, writer); // Throws
}


void Table::adjust_column_index(size_t column_ndx_begin, int ndx_in_parent_diff)
    TIGHTDB_NOEXCEPT
{
    size_t num_cols = m_cols.size();
    for (size_t col_ndx = column_ndx_begin; col_ndx != num_cols; ++col_ndx) {
        ColumnBase* column = m_cols[col_ndx];
        column->get_root_array()->adjust_ndx_in_parent(ndx_in_parent_diff);
        column->update_column_index(col_ndx, m_spec);

        // If we modified before link columns, we have to update their
        // backlinks to point to their new position
        ColumnType type = m_spec.get_column_type(col_ndx);
        if (type == col_type_Link || type == col_type_LinkList) {
            size_t table_ndx = get_index_in_parent();
            ColumnLinkBase* col = static_cast<ColumnLinkBase*>(column);
            TableRef target_table(col->get_target_table());
            target_table->update_backlink_column_ref(table_ndx, col_ndx-ndx_in_parent_diff,
                                                     col_ndx);
        }
    }
}

void Table::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());

    // There is no top for sub-tables sharing spec
    if (m_top.is_attached()) {
        if (!m_top.update_from_parent(old_baseline))
            return;
    }

    m_spec.update_from_parent(old_baseline);

    if (!m_columns.is_attached())
        return; // Degenerate subtable

    if (!m_columns.update_from_parent(old_baseline))
        return;

    // Update column accessors
    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        ColumnBase* column = m_cols[i];
        column->update_from_parent(old_baseline);
    }
}


// to JSON: ------------------------------------------

void Table::to_json(ostream& out) const
{
    // Represent table as list of objects
    out << "[";

    size_t row_count = size();
    for (size_t r = 0; r < row_count; ++r) {
        if (r > 0)
            out << ",";
        to_json_row(r, out);
    }

    out << "]";
}


namespace {

inline void out_datetime(ostream& out, DateTime value)
{
    time_t rawtime = value.get_datetime();
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

inline void out_binary(ostream& out, const BinaryData bin)
{
    const char* p = bin.data();
    for (size_t i = 0; i < bin.size(); ++i)
        out << setw(2) << setfill('0') << hex << static_cast<unsigned int>(p[i]) << dec;
}

template<class T> void out_floats(ostream& out, T value)
{
    streamsize old = out.precision();
    out.precision(numeric_limits<T>::digits10 + 1);
    out << scientific << value;
    out.precision(old);
}

} // anonymous namespace


void Table::to_json_row(size_t row_ndx, ostream& out) const
{
    out << "{";
    size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        if (i > 0)
            out << ",";

        StringData name = get_column_name(i);
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
            case type_DateTime:
                out << "\""; out_datetime(out, get_datetime(i, row_ndx)); out << "\"";
                break;
            case type_Binary:
                out << "\""; out_binary(out, get_binary(i, row_ndx)); out << "\"";
                break;
            case type_Table:
                get_subtable(i, row_ndx)->to_json(out);
                break;
            case type_Mixed:
            {
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
                        case type_DateTime:
                            out << "\""; out_datetime(out, m.get_datetime()); out << "\"";
                            break;
                        case type_Binary:
                            out << "\""; out_binary(out, m.get_binary()); out << "\"";
                            break;
                        case type_Table:
                        case type_Mixed:
                        case type_Link:
                        case type_LinkList:
                            TIGHTDB_ASSERT(false);
                            break;
                    }
                }
                break;
            }
            case type_Link:
                // FIXME: print entire linked row
                out << "\"Link to: " << get_link(i, row_ndx) << "\"";
                break;
            case type_LinkList:
                // FIXME: print entire list of linked row
                //out << "\"Link to: " << get_int(i, row_ndx) << "\"";
                break;
        }
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
    return count+1;
}

} // anonymous namespace


void Table::to_string(ostream& out, size_t limit) const
{
    // Print header (will also calculate widths)
    vector<size_t> widths;
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

void Table::row_to_string(size_t row_ndx, ostream& out) const
{
    TIGHTDB_ASSERT(row_ndx < size());

    // Print header (will also calculate widths)
    vector<size_t> widths;
    to_string_header(out, widths);

    // Print row contents
    to_string_row(row_ndx, out, widths);
}

void Table::to_string_header(ostream& out, vector<size_t>& widths) const
{
    size_t column_count = get_column_count();
    size_t row_count = size();
    size_t row_ndx_width = chars_in_int(row_count);
    widths.push_back(row_ndx_width);

    // Empty space over row numbers
    for (size_t i = 0; i < row_ndx_width+1; ++i)
        out << " ";

    // Write header
    for (size_t col = 0; col < column_count; ++col) {
        StringData name = get_column_name(col);
        DataType type = get_column_type(col);
        size_t width = 0;
        switch (type) {
            case type_Bool:
                width = 5;
                break;
            case type_DateTime:
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
                    width = max(width, len+2);
                }
                width += 2; // space for "[]"
                break;
            case type_Binary:
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = chars_in_int(get_binary(col, row).size()) + 2;
                    width = max(width, len);
                }
                width += 6; // space for " bytes"
                break;
            case type_String: {
                // Find max length of the strings
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = get_string(col, row).size();
                    width = max(width, len);
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
                        size_t len = chars_in_int( get_subtable_size(col, row) ) + 2;
                        width = max(width, len);
                        continue;
                    }
                    Mixed m = get_mixed(col, row);
                    switch (mtype) {
                        case type_Bool:
                            width = max(width, size_t(5));
                            break;
                        case type_DateTime:
                            width = max(width, size_t(19));
                            break;
                        case type_Int:
                            width = max(width, chars_in_int(m.get_int()));
                            break;
                        case type_Float:
                            width = max(width, size_t(14));
                            break;
                        case type_Double:
                            width = max(width, size_t(14));
                            break;
                        case type_Binary:
                            width = max(width, chars_in_int(m.get_binary().size()) + 6);
                            break;
                        case type_String: {
                            size_t len = m.get_string().size();
                            if (len > 20)
                                len = 23;
                            width = max(width, len);
                            break;
                        }
                        case type_Table:
                        case type_Mixed:
                        case type_Link:
                        case type_LinkList:
                            TIGHTDB_ASSERT(false);
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
        out << string(name);
    }
    out << "\n";
}


namespace {

inline void out_string(ostream& out, const string text, const size_t max_len)
{
    out.setf(ostream::left, ostream::adjustfield);
    if (text.size() > max_len)
        out << text.substr(0, max_len) + "...";
    else
        out << text;
    out.unsetf(ostream::adjustfield);
}

inline void out_table(ostream& out, const size_t len)
{
    streamsize width = out.width() - chars_in_int(len) - 1;
    out.width(width);
    out << "[" << len << "]";
}

} // anonymous namespace


void Table::to_string_row(size_t row_ndx, ostream& out, const vector<size_t>& widths) const
{
    size_t column_count  = get_column_count();
    size_t row_ndx_width = widths[0];

    out << scientific;          // for float/double
    out.width(row_ndx_width);
    out << row_ndx << ":";

    for (size_t col = 0; col < column_count; ++col) {
        out << "  "; // spacing
        out.width(widths[col+1]);

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
            case type_DateTime:
                out_datetime(out, get_datetime(col, row_ndx));
                break;
            case type_Table:
                out_table(out, get_subtable_size(col, row_ndx));
                break;
            case type_Binary:
                out.width(widths[col+1]-6); // adjust for " bytes" text
                out << get_binary(col, row_ndx).size() << " bytes";
                break;
            case type_Mixed:
            {
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
                        case type_DateTime:
                            out_datetime(out, m.get_datetime());
                            break;
                        case type_Binary:
                            out.width(widths[col+1]-6); // adjust for " bytes" text
                            out << m.get_binary().size() << " bytes";
                            break;
                        case type_Table:
                        case type_Mixed:
                        case type_Link:
                        case type_LinkList:
                            TIGHTDB_ASSERT(false);
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
    TIGHTDB_ASSERT(t.get_column_count() == n);
    for (size_t i = 0; i != n; ++i) {
        ColumnType type = get_real_column_type(i);
        TIGHTDB_ASSERT(type == col_type_String     ||
                       type == col_type_StringEnum ||
                       type == t.get_real_column_type(i));

        switch (type) {
            case col_type_Int:
            case col_type_Bool:
            case col_type_DateTime: {
                const Column& c1 = get_column(i);
                const Column& c2 = t.get_column(i);
                if (!c1.compare_int(c2))
                    return false;
                continue;
            }
            case col_type_Float: {
                const ColumnFloat& c1 = get_column_float(i);
                const ColumnFloat& c2 = t.get_column_float(i);
                if (!c1.compare(c2))
                    return false;
                continue;
            }
            case col_type_Double: {
                const ColumnDouble& c1 = get_column_double(i);
                const ColumnDouble& c2 = t.get_column_double(i);
                if (!c1.compare(c2))
                    return false;
                continue;
            }
            case col_type_String: {
                const AdaptiveStringColumn& c1 = get_column_string(i);
                ColumnType type2 = t.get_real_column_type(i);
                if (type2 == col_type_String) {
                    const AdaptiveStringColumn& c2 = t.get_column_string(i);
                    if (!c1.compare_string(c2))
                        return false;
                }
                else {
                    TIGHTDB_ASSERT(type2 == col_type_StringEnum);
                    const ColumnStringEnum& c2 = t.get_column_string_enum(i);
                    if (!c2.compare_string(c1))
                        return false;
                }
                continue;
            }
            case col_type_StringEnum: {
                const ColumnStringEnum& c1 = get_column_string_enum(i);
                ColumnType type2 = t.get_real_column_type(i);
                if (type2 == col_type_StringEnum) {
                    const ColumnStringEnum& c2 = t.get_column_string_enum(i);
                    if (!c1.compare_string(c2))
                        return false;
                }
                else {
                    TIGHTDB_ASSERT(type2 == col_type_String);
                    const AdaptiveStringColumn& c2 = t.get_column_string(i);
                    if (!c1.compare_string(c2))
                        return false;
                }
                continue;
            }
            case col_type_Binary: {
                const ColumnBinary& c1 = get_column_binary(i);
                const ColumnBinary& c2 = t.get_column_binary(i);
                if (!c1.compare_binary(c2))
                    return false;
                continue;
            }
            case col_type_Table: {
                const ColumnTable& c1 = get_column_table(i);
                const ColumnTable& c2 = t.get_column_table(i);
                if (!c1.compare_table(c2)) // Throws
                    return false;
                continue;
            }
            case col_type_Mixed: {
                const ColumnMixed& c1 = get_column_mixed(i);
                const ColumnMixed& c2 = t.get_column_mixed(i);
                if (!c1.compare_mixed(c2))
                    return false;
                continue;
            }
            case col_type_Link: {
                const ColumnLink& c1 = get_column_link(i);
                const ColumnLink& c2 = t.get_column_link(i);
                if (!c1.compare_int(c2))
                    return false;
                continue;
            }
            case col_type_LinkList: {
                const ColumnLinkList& c1 = get_column_link_list(i);
                const ColumnLinkList& c2 = t.get_column_link_list(i);
                if (!c1.compare_link_list(c2))
                    return false;
                continue;
            }
            case col_type_BackLink:
            case col_type_Reserved1:
            case col_type_Reserved4:
                break;
        }
        TIGHTDB_ASSERT(false);
    }
    return true;
}


const Array* Table::get_column_root(size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    return m_cols[col_ndx]->get_root_array();
}


pair<const Array*, const Array*> Table::get_string_column_roots(size_t col_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());

    const ColumnBase* col = m_cols[col_ndx];

    const Array* root = col->get_root_array();
    const Array* enum_root = 0;

    if (const ColumnStringEnum* c = dynamic_cast<const ColumnStringEnum*>(col)) {
        enum_root = c->get_enum_root_array();
    }
    else {
        TIGHTDB_ASSERT(dynamic_cast<const AdaptiveStringColumn*>(col));
    }

    return make_pair(root, enum_root);
}


StringData Table::Parent::get_child_name(size_t) const TIGHTDB_NOEXCEPT
{
    return StringData();
}


Table* Table::Parent::get_parent_table(size_t*) const TIGHTDB_NOEXCEPT
{
    return 0;
}


void Table::adj_accessors_insert_rows(size_t row_ndx, size_t num_rows) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    adj_row_acc_insert_rows(row_ndx, num_rows);

    // Adjust subtable accessors after insertion of new rows
    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->adj_accessors_insert_rows(row_ndx, num_rows);
    }
}


void Table::adj_accessors_erase_row(size_t row_ndx) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    adj_row_acc_erase_row(row_ndx);

    // Adjust subtable accessors after removal of a row
    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->adj_accessors_erase_row(row_ndx);
    }
}


void Table::adj_accessors_move_last_over(size_t target_row_ndx, size_t last_row_ndx)
    TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    adj_row_acc_move_last_over(target_row_ndx, last_row_ndx);

    // Adjust subtable accessors after 'move last over' removal of a row
    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->adj_accessors_move_last_over(target_row_ndx, last_row_ndx);
    }
}


void Table::adj_row_acc_insert_rows(size_t row_ndx, size_t num_rows) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    // Adjust row accessors after insertion of new rows
    typedef row_accessors::const_iterator iter;
    iter end = m_row_accessors.end();
    for (iter i = m_row_accessors.begin(); i != end; ++i) {
        RowBase* row = *i;
        if (row->m_row_ndx >= row_ndx)
            row->m_row_ndx += num_rows;
    }
}


void Table::adj_row_acc_erase_row(size_t row_ndx) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    // Adjust row accessors after removal of a row
    size_t i = 0, n = m_row_accessors.size();
    while (i != n) {
        RowBase*& row = m_row_accessors[i];
        if (row->m_row_ndx == row_ndx) {
            row->m_table.reset(); // Detach
            // Move last over
            row = m_row_accessors[--n];
            m_row_accessors.pop_back();
            continue;
        }
        if (row->m_row_ndx > row_ndx)
            --row->m_row_ndx;
        ++i;
    }
}


void Table::adj_row_acc_move_last_over(size_t target_row_ndx, size_t last_row_ndx)
    TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    // Adjust row accessors after 'move last over' removal of a row
    size_t i = 0, n = m_row_accessors.size();
    while (i != n) {
        RowBase*& row = m_row_accessors[i];
        if (row->m_row_ndx == target_row_ndx) {
            row->m_table.reset(); // Detach
            // Move last over in list of accessors
            row = m_row_accessors[--n];
            m_row_accessors.pop_back();
            continue;
        }
        if (row->m_row_ndx == last_row_ndx)
            row->m_row_ndx = target_row_ndx;
        ++i;
    }
}


void Table::adj_clear_nonroot() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    discard_row_accessors();
    discard_subtable_accessors();
    destroy_column_accessors();
    m_columns.detach();
}


void Table::adj_insert_column(size_t col_ndx)
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    TIGHTDB_ASSERT(is_attached());
    bool not_degenerate = m_columns.is_attached();
    if (not_degenerate) {
        TIGHTDB_ASSERT(col_ndx <= m_cols.size());
        m_cols.insert(m_cols.begin() + col_ndx, 0); // Throws
    }
}


void Table::adj_erase_column(size_t col_ndx) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    TIGHTDB_ASSERT(is_attached());
    bool not_degenerate = m_columns.is_attached();
    if (not_degenerate) {
        TIGHTDB_ASSERT(col_ndx < m_cols.size());
        if (ColumnBase* col = m_cols[col_ndx]) {
            col->detach_subtable_accessors();
            delete col;
        }
        m_cols.erase(m_cols.begin() + col_ndx);

        // If we removed the last column, we need to discard all row accessors
        if (m_cols.empty())
            discard_row_accessors();
    }
}


void Table::recursive_mark() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

    mark();

    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->recursive_mark();
    }
}


void Table::regressive_mark() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistncyLevels.

#ifdef TIGHTDB_ENABLE_REPLICATION
    Table* table = this;
    for (;;) {
        if (table->m_mark)
            break;
        table->m_mark = true;
        table = get_parent_table_ptr();
        if (!table)
            break;
    }
#endif
}


void Table::refresh_accessor_tree(size_t ndx_in_parent)
{
    TIGHTDB_ASSERT(is_attached());
    if (m_top.is_attached()) {
        // Root table (independent descriptor)
        m_top.set_ndx_in_parent(ndx_in_parent);
#ifdef TIGHTDB_ENABLE_REPLICATION
        if (!m_mark)
            return;
#endif
        m_top.init_from_parent();
        m_spec.init_from_parent();
        m_columns.init_from_parent();
    }
    else {
        // Subtable with shared descriptor
        m_columns.set_ndx_in_parent(ndx_in_parent);
#ifdef TIGHTDB_ENABLE_REPLICATION
        if (!m_mark)
            return;
#endif
        m_spec.init_from_parent();

        // If the underlying table was degenerate, then `m_cols` must still be
        // empty.
        TIGHTDB_ASSERT(m_columns.is_attached() || m_cols.empty());

        ref_type columns_ref = m_columns.get_ref_from_parent();
        if (columns_ref != 0) {
            if (!m_columns.is_attached()) {
                // The underlying table is no longer degenerate
                size_t num_cols = m_spec.get_column_count();
                m_cols.resize(num_cols); // throws
            }
            m_columns.init_from_ref(columns_ref);
        }
        else if (m_columns.is_attached()) {
            // The underlying table has become degenerate
            m_columns.detach();
            destroy_column_accessors();
        }
    }
    m_search_index = 0;

    size_t col_ndx_in_parent = 0; // Index in Table::m_columns
    size_t num_cols = m_cols.size();
    for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
        ColumnBase* col = m_cols[col_ndx];

        // If the current column accessor is AdaptiveStringColumn, but the
        // underlying column has been upgraded to an enumerated strings column,
        // then we need to replace the accessor with an instance of
        // ColumnStringEnum.
        if (col && col->is_string_col()) {
            ColumnType col_type = m_spec.get_column_type(col_ndx);
            if (col_type == col_type_StringEnum) {
                delete col;
                col = 0;
                // We need to store null in `m_cols` to avoid a crash during
                // destruction of the table accessor in case an error occurs
                // before the refresh operation is complete.
                m_cols[col_ndx] = 0;
            }
        }

        if (col) {
            // Refresh the column accessor
            col->get_root_array()->set_ndx_in_parent(col_ndx_in_parent);
            col->refresh_accessor_tree(col_ndx, m_spec); // Throws
        }
        else {
            ColumnType col_type = m_spec.get_column_type(col_ndx);
            col = create_column_accessor(col_type, col_ndx, col_ndx_in_parent); // Throws
            m_cols[col_ndx] = col;
        }

        // If the column was equipped column with search index, create the
        // search index accessor.
        ColumnAttr attr = m_spec.get_column_attr(col_ndx);
        bool has_search_index = attr & col_attr_Indexed;
        if (has_search_index && !col->has_index()) {
            ref_type ref = m_columns.get_as_ref(col_ndx_in_parent+1);
            col->set_index_ref(ref, &m_columns, col_ndx_in_parent+1); // Throws
        }

        col_ndx_in_parent += (has_search_index ? 2 : 1);
    }

    // Set table size
    if (num_cols == 0) {
        m_size = 0;
    }
    else {
        ColumnBase* first_col = m_cols[0];
        m_size = first_col->size();
    }


#ifdef TIGHTDB_ENABLE_REPLICATION
    m_mark = false;
#endif

    bump_version(false);
}


#ifdef TIGHTDB_DEBUG

void Table::Verify() const
{
    TIGHTDB_ASSERT(is_attached());
    if (!m_columns.is_attached())
        return; // Accessor for degenerate subtable

    if (m_top.is_attached())
        m_top.Verify();
    m_columns.Verify();
    m_spec.Verify();


    // Verify row accessors
    {
        typedef row_accessors::const_iterator iter;
        iter end = m_row_accessors.end();
        for (iter i = m_row_accessors.begin(); i != end; ++i) {
            RowBase* row = *i;
            // Check that each row accessor occurs only once
            TIGHTDB_ASSERT(find(i+1, end, row) == end);
            // Check that it is attached to this table
            TIGHTDB_ASSERT(row->m_table.get() == this);
            // Check that its row index is not out of bounds
            TIGHTDB_ASSERT(row->m_row_ndx < size());
        }
    }

    // Verify column accessors
    {
        size_t n = m_spec.get_column_count();
        TIGHTDB_ASSERT(n == m_cols.size());
        for (size_t i = 0; i != n; ++i) {
            const ColumnBase& column = get_column_base(i);
            column.Verify();
            TIGHTDB_ASSERT(column.size() == m_size);
        }
    }
}


void Table::to_dot(ostream& out, StringData title) const
{
    if (m_top.is_attached()) {
        out << "subgraph cluster_table_with_spec" << m_top.get_ref() << " {" << endl;
        out << " label = \"Table";
        if (0 < title.size())
            out << "\\n'" << title << "'";
        out << "\";" << endl;
        m_top.to_dot(out, "table_top");
        m_spec.to_dot(out);
    }
    else {
        out << "subgraph cluster_table_"  << m_columns.get_ref() <<  " {" << endl;
        out << " label = \"Table";
        if (0 < title.size())
            out << " " << title;
        out << "\";" << endl;
    }

    to_dot_internal(out);

    out << "}" << endl;
}


void Table::to_dot_internal(ostream& out) const
{
    m_columns.to_dot(out, "columns");

    // Columns
    size_t n = get_column_count();
    for (size_t i = 0; i != n; ++i) {
        const ColumnBase& column = get_column_base(i);
        StringData name = get_column_name(i);
        column.to_dot(out, name);
    }
}


void Table::print() const
{
    // Table header
    cout << "Table: len(" << m_size << ")\n    ";
    size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        StringData name = m_spec.get_column_name(i);
        cout << left << setw(10) << name << right << " ";
    }

    // Types
    cout << "\n    ";
    for (size_t i = 0; i < column_count; ++i) {
        ColumnType type = get_real_column_type(i);
        switch (type) {
            case type_Int:
                cout << "Int        "; break;
            case type_Float:
                cout << "Float      "; break;
            case type_Double:
                cout << "Double     "; break;
            case type_Bool:
                cout << "Bool       "; break;
            case type_String:
                cout << "String     "; break;
            case col_type_StringEnum:
                cout << "String     "; break;
            default:
                TIGHTDB_ASSERT(false);
        }
    }
    cout << "\n";

    // Columns
    for (size_t i = 0; i < m_size; ++i) {
        cout << setw(3) << i;
        for (size_t n = 0; n < column_count; ++n) {
            ColumnType type = get_real_column_type(n);
            switch (type) {
                case type_Int: {
                    const Column& column = get_column(n);
                    cout << setw(10) << column.get(i) << " ";
                    break;
                }
                case type_Float: {
                    const ColumnFloat& column = get_column_float(n);
                    cout << setw(10) << column.get(i) << " ";
                    break;
                }
                case type_Double: {
                    const ColumnDouble& column = get_column_double(n);
                    cout << setw(10) << column.get(i) << " ";
                    break;
                }
                case type_Bool: {
                    const Column& column = get_column(n);
                    cout << (column.get(i) == 0 ? "     false " : "      true ");
                    break;
                }
                case type_String: {
                    const AdaptiveStringColumn& column = get_column_string(n);
                    cout << setw(10) << column.get(i) << " ";
                    break;
                }
                case col_type_StringEnum: {
                    const ColumnStringEnum& column = get_column_string_enum(n);
                    cout << setw(10) << column.get(i) << " ";
                    break;
                }
                default:
                    TIGHTDB_ASSERT(false);
            }
        }
        cout << "\n";
    }
    cout << "\n";
}


MemStats Table::stats() const
{
    MemStats stats;
    m_top.stats(stats);
    return stats;
}


void Table::dump_node_structure() const
{
    dump_node_structure(cerr, 0);
}

void Table::dump_node_structure(ostream& out, int level) const
{
    int indent = level * 2;
    out << setw(indent) << "" << "Table (top_ref: "<<m_top.get_ref()<<")\n";
    size_t n = get_column_count();
    for (size_t i = 0; i != n; ++i) {
        const ColumnBase& column = get_column_base(i);
        column.dump_node_structure(out, level+1);
    }
}


#endif // TIGHTDB_DEBUG
