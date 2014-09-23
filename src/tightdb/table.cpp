#define _CRT_SECURE_NO_WARNINGS
#include <limits>
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>

#include <tightdb/util/features.h>
#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/exceptions.hpp>
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
/// This level of consistency is guaranteed, and it is also the **maximum** that
/// may be assumed by the application after a library function fails by throwing
/// an unexpected exception (such as std::bad_alloc). It is also the **minimum**
/// level of consistency that is required to be able to properly destroy the
/// accessor objects (manually, or as a result of stack unwinding).
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


void Table::insert_column_link(size_t col_ndx, DataType type, StringData name, Table& target)
{
    get_descriptor()->insert_column_link(col_ndx, type, name, target); // Throws
}


size_t Table::get_backlink_count(size_t row_ndx, const Table& origin,
                                 size_t origin_col_ndx) const TIGHTDB_NOEXCEPT
{
    size_t origin_table_ndx = origin.get_index_in_group();
    size_t backlink_col_ndx = m_spec.find_backlink_column(origin_table_ndx, origin_col_ndx);
    const ColumnBackLink& backlink_col = get_column_backlink(backlink_col_ndx);
    return backlink_col.get_backlink_count(row_ndx);
}


size_t Table::get_backlink(size_t row_ndx, const Table& origin, size_t origin_col_ndx,
                           size_t backlink_ndx) const TIGHTDB_NOEXCEPT
{
    size_t origin_table_ndx = origin.get_index_in_group();
    size_t backlink_col_ndx = m_spec.find_backlink_column(origin_table_ndx, origin_col_ndx);
    const ColumnBackLink& backlink_col = get_column_backlink(backlink_col_ndx);
    return backlink_col.get_backlink(row_ndx, backlink_ndx);
}


void Table::connect_opposite_link_columns(size_t link_col_ndx, Table& target_table,
                                          size_t backlink_col_ndx) TIGHTDB_NOEXCEPT
{
    ColumnLinkBase& link_col = get_column_link_base(link_col_ndx);
    ColumnBackLink& backlink_col = target_table.get_column_backlink(backlink_col_ndx);
    link_col.set_target_table(target_table);
    link_col.set_backlink_column(backlink_col);
    backlink_col.set_origin_table(*this);
    backlink_col.set_origin_column(link_col);
}


void Table::insert_column(size_t col_ndx, DataType type, StringData name,
                          DescriptorRef* subdesc)
{
    TIGHTDB_ASSERT(!has_shared_type());
    get_descriptor()->insert_column(col_ndx, type, name, subdesc); // Throws
}


void Table::remove_column(size_t col_ndx)
{
    TIGHTDB_ASSERT(!has_shared_type());
    get_descriptor()->remove_column(col_ndx); // Throws
}


void Table::rename_column(size_t col_ndx, StringData name)
{
    TIGHTDB_ASSERT(!has_shared_type());
    get_descriptor()->rename_column(col_ndx, name); // Throws
}


DescriptorRef Table::get_descriptor()
{
    TIGHTDB_ASSERT(is_attached());

    if (has_shared_type()) {
        ArrayParent* array_parent = m_columns.get_parent();
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(array_parent));
        Parent* table_parent = static_cast<Parent*>(array_parent);
        size_t col_ndx = 0;
        Table* parent = table_parent->get_parent_table(&col_ndx);
        TIGHTDB_ASSERT(parent);
        return parent->get_descriptor()->get_subdescriptor(col_ndx); // Throws
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
    size_t col_ndx = desc->get_column_count();
    desc->insert_column(col_ndx, type, name); // Throws
    return col_ndx;
}


void Table::insert_subcolumn(const path_vec& path, size_t col_ndx,
                             DataType type, StringData name)
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



void Table::init(ref_type top_ref, ArrayParent* parent, size_t ndx_in_parent,
                 bool skip_create_column_accessors)
{
    m_mark = false;

#ifdef TIGHTDB_ENABLE_REPLICATION
    m_version = 0;
#endif

    // Load from allocated memory
    m_top.set_parent(parent, ndx_in_parent);
    m_top.init_from_ref(top_ref);
    TIGHTDB_ASSERT(m_top.size() == 2);

    size_t spec_ndx_in_parent = 0;
    m_spec.set_parent(&m_top, spec_ndx_in_parent);
    m_spec.init_from_parent();
    size_t columns_ndx_in_parent = 1;
    m_columns.set_parent(&m_top, columns_ndx_in_parent);
    m_columns.init_from_parent();

    size_t num_cols = m_spec.get_column_count();
    m_cols.resize(num_cols); // Throws

    if (!skip_create_column_accessors) {
        // Create column accessors and initialize `m_size`
        refresh_column_accessors(); // Throws
    }
}


void Table::init(ConstSubspecRef shared_spec, ArrayParent* parent_column, size_t parent_row_ndx)
{
    m_mark = false;

#ifdef TIGHTDB_ENABLE_REPLICATION
    m_version = 0;
#endif

    m_spec.init(SubspecRef(SubspecRef::const_cast_tag(), shared_spec));
    m_columns.set_parent(parent_column, parent_row_ndx);

    // A degenerate subtable has no underlying columns array and no column
    // accessors yet. They will be created on first modification.
    ref_type columns_ref = m_columns.get_ref_from_parent();
    if (columns_ref != 0) {
        m_columns.init_from_ref(columns_ref);

        size_t num_cols = m_spec.get_column_count();
        m_cols.resize(num_cols); // Throws
    }

    // Create column accessors and initialize `m_size`
    refresh_column_accessors(); // Throws
}


struct Table::InsertSubtableColumns: SubtableUpdater {
    InsertSubtableColumns(size_t i, DataType t):
        m_column_ndx(i), m_type(t)
    {
    }
    void update(const ColumnTable& subtables, Array& subcolumns) TIGHTDB_OVERRIDE
    {
        size_t row_ndx = subcolumns.get_ndx_in_parent();
        size_t subtable_size = subtables.get_subtable_size(row_ndx);
        Allocator& alloc = subcolumns.get_alloc();
        ref_type column_ref = create_column(ColumnType(m_type), subtable_size, alloc); // Throws
        _impl::DeepArrayRefDestroyGuard dg(column_ref, alloc);
        subcolumns.insert(m_column_ndx, column_ref); // Throws
        dg.release();
    }
    void update_accessor(Table& table) TIGHTDB_OVERRIDE
    {
        table.adj_insert_column(m_column_ndx); // Throws
        table.refresh_column_accessors(m_column_ndx); // Throws
        bool bump_global = false;
        table.bump_version(bump_global);
    }
private:
    const size_t m_column_ndx;
    const DataType m_type;
};


struct Table::EraseSubtableColumns: SubtableUpdater {
    EraseSubtableColumns(size_t i):
        m_column_ndx(i)
    {
    }
    void update(const ColumnTable&, Array& subcolumns) TIGHTDB_OVERRIDE
    {
        ref_type column_ref = to_ref(subcolumns.get(m_column_ndx));
        subcolumns.erase(m_column_ndx); // Throws
        Array::destroy_deep(column_ref, subcolumns.get_alloc());
    }
    void update_accessor(Table& table) TIGHTDB_OVERRIDE
    {
        table.adj_erase_column(m_column_ndx);
        table.refresh_column_accessors(m_column_ndx);
        bool bump_global = false;
        table.bump_version(bump_global);
    }
private:
    const size_t m_column_ndx;
};


struct Table::RenameSubtableColumns: SubtableUpdater {
    void update(const ColumnTable&, Array&) TIGHTDB_OVERRIDE
    {
    }
    void update_accessor(Table& table) TIGHTDB_OVERRIDE
    {
        bool bump_global = false;
        table.bump_version(bump_global);
    }
};


void Table::do_insert_column(Descriptor& desc, size_t col_ndx, DataType type,
                             StringData name, Table* link_target_table)
{
    TIGHTDB_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::get_root_table(desc);
    TIGHTDB_ASSERT(!root_table.has_shared_type());

    if (desc.is_root()) {
        root_table.bump_version();
        root_table.insert_root_column(col_ndx, type, name, link_target_table); // Throws
    }
    else {
        Spec& spec = df::get_spec(desc);
        spec.insert_column(col_ndx, ColumnType(type), name); // Throws
        if (!root_table.is_empty()) {
            root_table.m_top.get_alloc().bump_global_version();
            InsertSubtableColumns updater(col_ndx, type);
            update_subtables(desc, &updater); // Throws
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = root_table.get_repl())
        repl->insert_column(desc, col_ndx, type, name, link_target_table); // Throws
#endif
}


void Table::do_erase_column(Descriptor& desc, size_t col_ndx)
{
    TIGHTDB_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::get_root_table(desc);
    TIGHTDB_ASSERT(!root_table.has_shared_type());
    TIGHTDB_ASSERT(col_ndx < desc.get_column_count());

    // For root tables, it is possible that the column to be removed is the last
    // column that is not a backlink column. If there are no backlink columns,
    // then the removal of the last column is enough to effectively truncate the
    // size (number of rows) to zero, since the number of rows is simply the
    // number of entries en each column. If, on the other hand, there are
    // additional backlink columns, we need to inject a clear operation before
    // the column removal to correctly reproduce the desired effect, namely that
    // the table appears truncated after the removal of the last non-hidden
    // column. This has the a regular replicated clear operation in order to get
    // the right behaviour in Group::advance_transact().
    if (desc.is_root()) {
        if (root_table.m_spec.get_public_column_count() == 1 && root_table.m_cols.size() > 1)
            root_table.clear(); // Throws
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = root_table.get_repl())
        repl->erase_column(desc, col_ndx); // Throws
#endif

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


void Table::do_rename_column(Descriptor& desc, size_t col_ndx, StringData name)
{
    TIGHTDB_ASSERT(desc.is_attached());

    typedef _impl::DescriptorFriend df;
    Table& root_table = df::get_root_table(desc);
    TIGHTDB_ASSERT(!root_table.has_shared_type());
    TIGHTDB_ASSERT(col_ndx < desc.get_column_count());

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

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = root_table.get_repl())
        repl->rename_column(desc, col_ndx, name); // Throws
#endif
}


void Table::insert_root_column(size_t col_ndx, DataType type, StringData name,
                               Table* link_target_table)
{
    TIGHTDB_ASSERT(col_ndx <= m_spec.get_public_column_count());

    do_insert_root_column(col_ndx, ColumnType(type), name); // Throws
    adj_insert_column(col_ndx); // Throws
    update_link_target_tables(col_ndx, col_ndx + 1); // Throws

    // When the inserted column is a link-type column, we must also add a
    // backlink column to the target table, however, since the origin column
    // accessor does not yet exist, the connection between the column accessors
    // (Table::connect_opposite_link_columns()) cannot be established yet. The
    // marking of the target table tells Table::refresh_column_accessors() that
    // it should not try to establish the connection yet. The connection will be
    // established by Table::refresh_column_accessors() when it is invoked for
    // the target table below.
    if (link_target_table) {
        size_t target_table_ndx = link_target_table->get_index_in_group();
        m_spec.set_opposite_link_table_ndx(col_ndx, target_table_ndx); // Throws
        link_target_table->mark();
    }

    refresh_column_accessors(col_ndx); // Throws

    if (link_target_table) {
        link_target_table->unmark();
        size_t origin_table_ndx = get_index_in_group();
        link_target_table->insert_backlink_column(origin_table_ndx, col_ndx); // Throws
    }
}


void Table::erase_root_column(size_t col_ndx)
{
    TIGHTDB_ASSERT(col_ndx < m_spec.get_public_column_count());

    // For link columns we need to erase the backlink column first in case the
    // target table is the same as the origin table (because the backlink column
    // occurs after regular columns.)
    ColumnType col_type = m_spec.get_column_type(col_ndx);
    if (is_link_type(col_type)) {
        Table* link_target_table = get_link_target_table_accessor(col_ndx);
        size_t origin_table_ndx = get_index_in_group();
        link_target_table->erase_backlink_column(origin_table_ndx, col_ndx); // Throws
    }

    do_erase_root_column(col_ndx); // Throws
    adj_erase_column(col_ndx);
    update_link_target_tables(col_ndx + 1, col_ndx); // Throws
    refresh_column_accessors(col_ndx);
}


void Table::do_insert_root_column(size_t ndx, ColumnType type, StringData name)
{
    m_spec.insert_column(ndx, type, name); // Throws

    Spec::ColumnInfo info;
    m_spec.get_column_info(ndx, info);
    size_t ndx_in_parent = info.m_column_ref_ndx;
    ref_type col_ref = create_column(type, m_size, m_columns.get_alloc()); // Throws
    m_columns.insert(ndx_in_parent, col_ref); // Throws
}


void Table::do_erase_root_column(size_t ndx)
{
    Spec::ColumnInfo info;
    m_spec.get_column_info(ndx, info);
    m_spec.erase_column(ndx); // Throws

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
}


void Table::insert_backlink_column(size_t origin_table_ndx, size_t origin_col_ndx)
{
    size_t backlink_col_ndx = m_cols.size();
    do_insert_root_column(backlink_col_ndx, col_type_BackLink, ""); // Throws
    adj_insert_column(backlink_col_ndx); // Throws
    m_spec.set_opposite_link_table_ndx(backlink_col_ndx, origin_table_ndx); // Throws
    m_spec.set_backlink_origin_column(backlink_col_ndx, origin_col_ndx); // Throws
    refresh_column_accessors(backlink_col_ndx); // Throws
}


void Table::erase_backlink_column(size_t origin_table_ndx, size_t origin_col_ndx)
{
    size_t backlink_col_ndx = m_spec.find_backlink_column(origin_table_ndx, origin_col_ndx);
    TIGHTDB_ASSERT(backlink_col_ndx != tightdb::not_found);
    do_erase_root_column(backlink_col_ndx); // Throws
    adj_erase_column(backlink_col_ndx);
    refresh_column_accessors(backlink_col_ndx); // Throws
}


void Table::update_link_target_tables(size_t old_col_ndx_begin, size_t new_col_ndx_begin)
{
    // Called when columns are inserted or removed.

    // If there are any subsequent link-type columns, the corresponding target
    // tables need to be updated such that their descriptors specify the right
    // origin table column indexes.

    size_t num_cols = m_cols.size();
    for (size_t new_col_ndx = new_col_ndx_begin; new_col_ndx < num_cols; ++new_col_ndx) {
        ColumnType type = m_spec.get_column_type(new_col_ndx);
        if (!is_link_type(type))
            continue;
        ColumnLinkBase* link_col = static_cast<ColumnLinkBase*>(m_cols[new_col_ndx]);
        Spec& target_spec = link_col->get_target_table().m_spec;
        size_t origin_table_ndx = get_index_in_group();
        size_t old_col_ndx = old_col_ndx_begin + (new_col_ndx - new_col_ndx_begin);
        size_t backlink_col_ndx = target_spec.find_backlink_column(origin_table_ndx, old_col_ndx);
        target_spec.set_backlink_origin_column(backlink_col_ndx, new_col_ndx); // Throws
    }
}


void Table::register_row_accessor(RowBase* row) const TIGHTDB_NOEXCEPT
{
    row->m_prev = 0;
    row->m_next = m_row_accessors;
    if (m_row_accessors)
        m_row_accessors->m_prev = row;
    m_row_accessors = row;
}


void Table::unregister_row_accessor(RowBase* row) const TIGHTDB_NOEXCEPT
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


void Table::discard_row_accessors() TIGHTDB_NOEXCEPT
{
    for (RowBase* row = m_row_accessors; row; row = row->m_next)
        row->m_table.reset(); // Detach
    m_row_accessors = 0;
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
            Table& root_table = df::get_root_table(desc);
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

    size_t col_ndx = *col_path_begin;
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Table);

    ColumnTable& subtables = get_column_table(col_ndx); // Throws
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
                updater->update_accessor(*subtable); // Throws
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
    // underlying node structure. See AccessorConsistencyLevels.

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


void Table::create_degen_subtab_columns()
{
    // Creates columns as well as column accessors for a degenerate
    // subtable. When done, that subtable is no longer degenerate.

    TIGHTDB_ASSERT(!m_columns.is_attached());

    m_columns.create(Array::type_HasRefs); // Throws
    m_columns.update_parent(); // Throws

    Allocator& alloc = m_columns.get_alloc();
    size_t num_cols = m_spec.get_column_count();
    for (size_t i = 0; i < num_cols; ++i) {
        ColumnType type = m_spec.get_column_type(i);
        size_t size = 0;
        ref_type ref = create_column(type, size, alloc); // Throws
        m_columns.add(int_fast64_t(ref)); // Throws

        // So far, only root tables can have search indexes, and this is not a
        // root table.
        TIGHTDB_ASSERT(m_spec.get_column_attr(i) == col_attr_None);
    }

    m_cols.resize(num_cols);
    refresh_column_accessors();
}


void Table::detach() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

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

    discard_child_accessors();
    destroy_column_accessors();
    m_cols.clear();
    // FSA: m_cols.destroy();
    discard_views();
}


void Table::unregister_view(const TableViewBase* view) TIGHTDB_NOEXCEPT
{
    // Fixme: O(n) may be unacceptable - if so, put and maintain
    // iterator or index in TableViewBase.
    typedef views::iterator iter;
    iter end = m_views.end();
    for (iter i = m_views.begin(); i != end; ++i) {
        if (*i == view) {
            *i = m_views.back();
            m_views.pop_back();
            break;
        }
    }
}


void Table::move_registered_view(const TableViewBase* old_addr,
                                 const TableViewBase* new_addr) TIGHTDB_NOEXCEPT
{
    typedef views::iterator iter;
    iter end = m_views.end();
    for (iter i = m_views.begin(); i != end; ++i) {
        if (*i == old_addr) {
            *i = new_addr;
            return;
        }
    }
    TIGHTDB_ASSERT(false);
}


void Table::discard_views() TIGHTDB_NOEXCEPT
{
    typedef views::const_iterator iter;
    iter end = m_views.end();
    for (iter i = m_views.begin(); i != end; ++i)
        (*i)->detach();
    m_views.clear();
}


void Table::discard_child_accessors() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    discard_row_accessors();

    size_t n = m_cols.size();
    for (size_t i = 0; i < n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->discard_child_accessors();
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
    // Degenerate subtables need to be instantiated before first modification
    if (!m_columns.is_attached())
        create_degen_subtab_columns(); // Throws
}


ColumnBase* Table::create_column_accessor(ColumnType col_type, size_t col_ndx, size_t ndx_in_parent)
{
    ColumnBase* col = 0;
    ref_type ref = m_columns.get_as_ref(ndx_in_parent);
    Allocator& alloc = m_columns.get_alloc();
    switch (col_type) {
        case col_type_Int:
        case col_type_Bool:
        case col_type_DateTime:
            col = new Column(alloc, ref); // Throws
            break;
        case col_type_Float:
            col = new ColumnFloat(alloc, ref); // Throws
            break;
        case col_type_Double:
            col = new ColumnDouble(alloc, ref); // Throws
            break;
        case col_type_String:
            col = new AdaptiveStringColumn(alloc, ref); // Throws
            break;
        case col_type_Binary:
            col = new ColumnBinary(alloc, ref); // Throws
            break;
        case col_type_StringEnum: {
            ArrayParent* keys_parent;
            size_t keys_ndx_in_parent;
            ref_type keys_ref =
                m_spec.get_enumkeys_ref(col_ndx, &keys_parent, &keys_ndx_in_parent);
            ColumnStringEnum* col_2 = new ColumnStringEnum(alloc, ref, keys_ref); // Throws
            col_2->get_keys().set_parent(keys_parent, keys_ndx_in_parent);
            col = col_2;
            break;
        }
        case col_type_Table:
            col = new ColumnTable(alloc, ref, this, col_ndx); // Throws
            break;
        case col_type_Mixed:
            col = new ColumnMixed(alloc, ref, this, col_ndx); // Throws
            break;
        case col_type_Link:
            // Target table will be set by group after entire table has been created
            col = new ColumnLink(alloc, ref); // Throws
            break;
        case col_type_LinkList:
            // Target table will be set by group after entire table has been created
            col = new ColumnLinkList(alloc, ref, this, col_ndx); // Throws
            break;
        case col_type_BackLink:
            // Origin table will be set by group after entire table has been created
            col = new ColumnBackLink(alloc, ref); // Throws
            break;
        case col_type_Reserved1:
        case col_type_Reserved4:
            // These have no function yet and are therefore unexpected.
            break;
    }
    TIGHTDB_ASSERT(col);
    col->set_parent(&m_columns, ndx_in_parent);
    return col;
}


void Table::destroy_column_accessors() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

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
    // array nodes. See AccessorConsistencyLevels.

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


bool Table::has_search_index(size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    // Utilize the guarantee that m_cols.size() == 0 for a detached table accessor.
    if (TIGHTDB_UNLIKELY(col_ndx >= m_cols.size()))
        return false;
    const ColumnBase& col = get_column_base(col_ndx);
    return col.has_search_index();
}


void Table::add_search_index(size_t col_ndx)
{
    if (TIGHTDB_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    if (TIGHTDB_UNLIKELY(has_shared_type()))
        throw LogicError(LogicError::wrong_kind_of_table);

    if (TIGHTDB_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);

    if (has_search_index(col_ndx))
        return;

    TIGHTDB_ASSERT(!m_primary_key);

    ColumnType type = get_real_column_type(col_ndx);
    Spec::ColumnInfo info;
    m_spec.get_column_info(col_ndx, info);
    size_t column_pos = info.m_column_ref_ndx;
    ref_type index_ref = 0;

    // Create the index
    if (type == col_type_String) {
        AdaptiveStringColumn& col = get_column_string(col_ndx);
        StringIndex& index = col.create_search_index(); // Throws
        index.set_parent(&m_columns, column_pos+1);
        index_ref = index.get_ref();
    }
    else if (type == col_type_StringEnum) {
        ColumnStringEnum& col = get_column_string_enum(col_ndx);
        StringIndex& index = col.create_search_index(); // Throws
        index.set_parent(&m_columns, column_pos+1);
        index_ref = index.get_ref();
    }
    else {
        throw LogicError(LogicError::illegal_combination);
    }

    // Insert ref into list of column refs after the owning column
    m_columns.insert(column_pos+1, index_ref); // Throws

    int attr = m_spec.get_column_attr(col_ndx);
    attr |= col_attr_Indexed;
    m_spec.set_column_attr(col_ndx, ColumnAttr(attr)); // Throws

    refresh_column_accessors(col_ndx+1); // Throws

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->add_search_index(this, col_ndx); // Throws
#endif
}


bool Table::has_primary_key() const TIGHTDB_NOEXCEPT
{
    // Utilize the guarantee that m_cols.size() == 0 for a detached table accessor.
    size_t n = m_cols.size();
    for (size_t i = 0; i < n; ++i) {
        ColumnAttr attr = m_spec.get_column_attr(i);
        if (attr & col_attr_PrimaryKey)
            return true;
    }
    return false;
}


bool Table::try_add_primary_key(size_t col_ndx)
{
    if (TIGHTDB_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    if (TIGHTDB_UNLIKELY(has_shared_type()))
        throw LogicError(LogicError::wrong_kind_of_table);

    if (TIGHTDB_UNLIKELY(has_primary_key()))
        throw LogicError(LogicError::has_primary_key);

    if (TIGHTDB_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);

    if (TIGHTDB_UNLIKELY(!has_search_index(col_ndx)))
        throw LogicError(LogicError::no_search_index);

    // FIXME: Also check that there are no null values
    // (NoNullConstraintViolation).
    ColumnType type = get_real_column_type(col_ndx);
    ColumnBase& col = get_column_base(col_ndx);
    if (type == col_type_String) {
        AdaptiveStringColumn& col_2 = static_cast<AdaptiveStringColumn&>(col);
        StringIndex& index = col_2.get_search_index();
        if (index.has_duplicate_values())
            return false;
        index.set_allow_duplicate_values(false);
    }
    else if (type == col_type_StringEnum) {
        ColumnStringEnum& col_2 = static_cast<ColumnStringEnum&>(col);
        StringIndex& index = col_2.get_search_index();
        if (index.has_duplicate_values())
            return false;
        index.set_allow_duplicate_values(false);
    }
    else {
        // Impossible case, because we know that a search index was already
        // added.
        TIGHTDB_ASSERT(false);
    }

    int attr = m_spec.get_column_attr(col_ndx);
    attr |= col_attr_PrimaryKey;
    m_spec.set_column_attr(col_ndx, ColumnAttr(attr)); // Throws

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->add_primary_key(this, col_ndx); // Throws
#endif

    return true;
}


void Table::remove_primary_key()
{
    if (TIGHTDB_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    if (TIGHTDB_UNLIKELY(has_shared_type()))
        throw LogicError(LogicError::wrong_kind_of_table);

    size_t num_cols = m_cols.size();
    for (size_t col_ndx = 0; col_ndx < num_cols; ++col_ndx) {
        int attr = m_spec.get_column_attr(col_ndx);
        if (attr & col_attr_PrimaryKey) {
            attr &= ~col_attr_PrimaryKey;
            m_spec.set_column_attr(col_ndx, ColumnAttr(attr)); // Throws
            m_primary_key = 0;

            ColumnType type = get_real_column_type(col_ndx);
            ColumnBase& col = get_column_base(col_ndx);
            if (type == col_type_String) {
                AdaptiveStringColumn& col_2 = static_cast<AdaptiveStringColumn&>(col);
                StringIndex& index = col_2.get_search_index();
                index.set_allow_duplicate_values(true);
            }
            else if (type == col_type_StringEnum) {
                ColumnStringEnum& col_2 = static_cast<ColumnStringEnum&>(col);
                StringIndex& index = col_2.get_search_index();
                index.set_allow_duplicate_values(true);
            }
            else {
                TIGHTDB_ASSERT(false);
            }

#ifdef TIGHTDB_ENABLE_REPLICATION
            if (Replication* repl = get_repl())
                repl->remove_primary_key(this); // Throws
#endif
            return;
        }
    }

    throw LogicError(LogicError::no_primary_key);
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

const ColumnBackLink& Table::get_column_backlink(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_column<ColumnBackLink, col_type_BackLink>(ndx);
}

ColumnBackLink& Table::get_column_backlink(size_t ndx)
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
        case col_type_DateTime:
            return Column::create(alloc, Array::type_Normal, size); // Throws
        case col_type_Float:
            return ColumnFloat::create(alloc, size); // Throws
        case col_type_Double:
            return ColumnDouble::create(alloc, size); // Throws
        case col_type_String:
            return AdaptiveStringColumn::create(alloc, size); // Throws
        case col_type_Binary:
            return ColumnBinary::create(alloc, size); // Throws
        case col_type_Table:
            return ColumnTable::create(alloc, size); // Throws
        case col_type_Mixed:
            return ColumnMixed::create(alloc, size); // Throws
        case col_type_Link:
            return ColumnLink::create(alloc, size); // Throws
        case col_type_LinkList:
            return ColumnLinkList::create(alloc, size); // Throws
        case col_type_BackLink:
            return ColumnBackLink::create(alloc, size); // Throws
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
    Array new_columns(alloc);
    new_columns.create(Array::type_HasRefs); // Throws
    size_t num_cols = get_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; ++col_ndx) {
        ref_type new_col_ref;
        const ColumnBase* col = &get_column_base(col_ndx);
        if (const ColumnStringEnum* enum_col = dynamic_cast<const ColumnStringEnum*>(col)) {
            ref_type ref = AdaptiveStringColumn::create(alloc); // Throws
            AdaptiveStringColumn new_col(alloc, ref); // Throws
            // FIXME: Should be optimized with something like
            // new_col.add(seq_tree_accessor.begin(),
            // seq_tree_accessor.end())
            size_t n = enum_col->size();
            for (size_t i = 0; i < n; ++i)
                new_col.add(enum_col->get(i)); // Throws
            new_col_ref = new_col.get_ref();
        }
        else {
            const Array& root = *col->get_root_array();
            MemRef mem = root.clone_deep(alloc); // Throws
            new_col_ref = mem.m_ref;
        }
        new_columns.add(int_fast64_t(new_col_ref)); // Throws
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
    if (row_ndx < m_size)
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


void Table::remove(size_t row_ndx)
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
        return &static_cast<ColumnLinkBase*>(col)->get_target_table();
    }
    return 0;
}


void Table::discard_subtable_accessor(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

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


size_t Table::get_parent_row_index() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    const Array& real_top = m_top.is_attached() ? m_top : m_columns;
    Parent* parent = static_cast<Parent*>(real_top.get_parent()); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return npos; // Free-standing table
    if (parent->get_parent_group())
        return tightdb::npos; // Group-level table
    size_t index_in_parent = real_top.get_ndx_in_parent();
    return index_in_parent;
}


Group* Table::get_parent_group() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    if (!m_top.is_attached())
        return 0; // Subtable with shared descriptor
    Parent* parent = static_cast<Parent*>(m_top.get_parent()); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return 0; // Free-standing table
    Group* group = parent->get_parent_group();
    if (!group)
        return 0; // Subtable with independent descriptor
    return group;
}


size_t Table::get_index_in_group() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    if (!m_top.is_attached())
        return tightdb::npos; // Subtable with shared descriptor
    Parent* parent = static_cast<Parent*>(m_top.get_parent()); // ArrayParent guaranteed to be Table::Parent
    if (!parent)
        return tightdb::npos; // Free-standing table
    if (!parent->get_parent_group())
        return tightdb::npos; // Subtable with independent descriptor
    size_t index_in_parent = m_top.get_ndx_in_parent();
    return index_in_parent;
}


int64_t Table::get_int(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = get_column(col_ndx);
    return column.get(ndx);
}

void Table::set_int(size_t col_ndx, size_t ndx, int_fast64_t value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    Column& column = get_column(col_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_int(this, col_ndx, ndx, value); // Throws
#endif
}

void Table::add_int(size_t col_ndx, int64_t value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Int);
    bump_version();
    get_column(col_ndx).adjust(value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->add_int_to_column(this, col_ndx, value); // Throws
#endif
}

bool Table::get_bool(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Bool);
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = get_column(col_ndx);
    return column.get(ndx) != 0;
}

void Table::set_bool(size_t col_ndx, size_t ndx, bool value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_Bool);
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    Column& column = get_column(col_ndx);
    column.set(ndx, value ? 1 : 0);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_bool(this, col_ndx, ndx, value); // Throws
#endif
}

DateTime Table::get_datetime(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_DateTime);
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = get_column(col_ndx);
    return time_t(column.get(ndx));
}

void Table::set_datetime(size_t col_ndx, size_t ndx, DateTime value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_DateTime);
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    Column& column = get_column(col_ndx);
    column.set(ndx, int64_t(value.get_datetime()));

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_date_time(this, col_ndx, ndx, value); // Throws
#endif
}

void Table::insert_int(size_t col_ndx, size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    Column& column = get_column(col_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_int(this, col_ndx, ndx, value); // Throws
#endif
}


float Table::get_float(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnFloat& column = get_column_float(col_ndx);
    return column.get(ndx);
}

void Table::set_float(size_t col_ndx, size_t ndx, float value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnFloat& column = get_column_float(col_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_float(this, col_ndx, ndx, value); // Throws
#endif
}

void Table::insert_float(size_t col_ndx, size_t ndx, float value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnFloat& column = get_column_float(col_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_float(this, col_ndx, ndx, value); // Throws
#endif
}


double Table::get_double(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnDouble& column = get_column_double(col_ndx);
    return column.get(ndx);
}

void Table::set_double(size_t col_ndx, size_t ndx, double value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnDouble& column = get_column_double(col_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_double(this, col_ndx, ndx, value); // Throws
#endif
}

void Table::insert_double(size_t col_ndx, size_t ndx, double value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnDouble& column = get_column_double(col_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_double(this, col_ndx, ndx, value); // Throws
#endif
}


StringData Table::get_string(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(col_ndx);
        return column.get(ndx);
    }

    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(col_ndx);
    return column.get(ndx);
}

void Table::set_string(size_t col_ndx, size_t ndx, StringData value)
{
    if (TIGHTDB_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (TIGHTDB_UNLIKELY(ndx >= m_size))
        throw LogicError(LogicError::row_index_out_of_range);
    // For a degenerate subtable, `m_cols.size()` is zero, even when it has a
    // column, however, the previous row index check guarantees that `m_size >
    // 0`, and since `m_size` is also zero for a degenerate subtable, the table
    // cannot be degenerate if we got this far.
    if (TIGHTDB_UNLIKELY(col_ndx >= m_cols.size()))
        throw LogicError(LogicError::column_index_out_of_range);

    bump_version();

    ColumnBase& col = get_column_base(col_ndx);
    col.set_string(ndx, value); // Throws

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_string(this, col_ndx, ndx, value); // Throws
#endif
}

void Table::insert_string(size_t col_ndx, size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        AdaptiveStringColumn& column = get_column_string(col_ndx);
        column.insert(ndx, value);
    }
    else {
        TIGHTDB_ASSERT(type == col_type_StringEnum);
        ColumnStringEnum& column = get_column_string_enum(col_ndx);
        column.insert(ndx, value);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_string(this, col_ndx, ndx, value); // Throws
#endif
}


BinaryData Table::get_binary(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnBinary& column = get_column_binary(col_ndx);
    return column.get(ndx);
}

void Table::set_binary(size_t col_ndx, size_t ndx, BinaryData value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnBinary& column = get_column_binary(col_ndx);
    column.set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->set_binary(this, col_ndx, ndx, value); // Throws
#endif
}

void Table::insert_binary(size_t col_ndx, size_t ndx, BinaryData value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnBinary& column = get_column_binary(col_ndx);
    column.insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->insert_binary(this, col_ndx, ndx, value); // Throws
#endif
}


Mixed Table::get_mixed(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnMixed& column = get_column_mixed(col_ndx);

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

DataType Table::get_mixed_type(size_t col_ndx, size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(col_ndx < m_columns.size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnMixed& column = get_column_mixed(col_ndx);
    return column.get_type(ndx);
}

void Table::set_mixed(size_t col_ndx, size_t ndx, Mixed value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);
    bump_version();

    ColumnMixed& column = get_column_mixed(col_ndx);
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
        repl->set_mixed(this, col_ndx, ndx, value); // Throws
#endif
}

void Table::insert_mixed(size_t col_ndx, size_t ndx, Mixed value)
{
    TIGHTDB_ASSERT(col_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnMixed& column = get_column_mixed(col_ndx);
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
        repl->insert_mixed(this, col_ndx, ndx, value); // Throws
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
    return column.get_target_table().get_table_ref();
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
            ColumnBackLink& column = get_column_backlink(i);
            column.add_row();
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->row_insert_complete(this); // Throws
#endif
}


// count ----------------------------------------------

size_t Table::count_int(size_t col_ndx, int64_t value) const
{
    if(!m_columns.is_attached())
        return 0;

    const Column& column = get_column<Column, col_type_Int>(col_ndx);
    return column.count(value);
}
size_t Table::count_float(size_t col_ndx, float value) const
{
    if(!m_columns.is_attached())
        return 0;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(col_ndx);
    return column.count(value);
}
size_t Table::count_double(size_t col_ndx, double value) const
{
    if(!m_columns.is_attached())
        return 0;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(col_ndx);
    return column.count(value);
}
size_t Table::count_string(size_t col_ndx, StringData value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < get_column_count());

    if(!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(col_ndx);
        return column.count(value);
    }
    else {
        TIGHTDB_ASSERT(type == col_type_StringEnum);
        const ColumnStringEnum& column = get_column_string_enum(col_ndx);
        return column.count(value);
    }
}

// sum ----------------------------------------------

int64_t Table::sum_int(size_t col_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

    const Column& column = get_column<Column, col_type_Int>(col_ndx);
    return column.sum();
}
double Table::sum_float(size_t col_ndx) const
{
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(col_ndx);
    return column.sum();
}
double Table::sum_double(size_t col_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(col_ndx);
    return column.sum();
}

// average ----------------------------------------------

double Table::average_int(size_t col_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

    const Column& column = get_column<Column, col_type_Int>(col_ndx);
    return column.average();
}
double Table::average_float(size_t col_ndx) const
{
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(col_ndx);
    return column.average();
}
double Table::average_double(size_t col_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(col_ndx);
    return column.average();
}

// minimum ----------------------------------------------

#define USE_COLUMN_AGGREGATE 1

int64_t Table::minimum_int(size_t col_ndx, size_t* return_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

#if USE_COLUMN_AGGREGATE
    const Column& column = get_column<Column, col_type_Int>(col_ndx);
    return column.minimum(0, npos, npos, return_ndx);
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
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(col_ndx);
    return column.minimum(0, npos, npos, return_ndx);
}

double Table::minimum_double(size_t col_ndx, size_t* return_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(col_ndx);
    return column.minimum(0, npos, npos, return_ndx);
}

// maximum ----------------------------------------------

int64_t Table::maximum_int(size_t col_ndx, size_t* return_ndx) const
{
    if(!m_columns.is_attached())
        return 0;

#if USE_COLUMN_AGGREGATE
    const Column& column = get_column<Column, col_type_Int>(col_ndx);
    return column.maximum(0, npos, npos, return_ndx);
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
    if(!m_columns.is_attached())
        return 0.f;

    const ColumnFloat& column = get_column<ColumnFloat, col_type_Float>(col_ndx);
    return column.maximum(0, npos, npos, return_ndx);
}

double Table::maximum_double(size_t col_ndx, size_t* return_ndx) const
{
    if(!m_columns.is_attached())
        return 0.;

    const ColumnDouble& column = get_column<ColumnDouble, col_type_Double>(col_ndx);
    return column.maximum(0, npos, npos, return_ndx);
}


void Table::reveal_primary_key() const
{
    size_t n = m_cols.size();
    for (size_t i = 0; i < n; ++i) {
        ColumnAttr attr = m_spec.get_column_attr(i);
        if (attr & col_attr_PrimaryKey) {
            ColumnType type = m_spec.get_column_type(i);
            const ColumnBase& col = get_column_base(i);
            if (type == col_type_String) {
                const AdaptiveStringColumn& col_2 = static_cast<const AdaptiveStringColumn&>(col);
                m_primary_key = &col_2.get_search_index();
                return;
            }
            if (type == col_type_StringEnum) {
                const ColumnStringEnum& col_2 = static_cast<const ColumnStringEnum&>(col);
                m_primary_key = &col_2.get_search_index();
                return;
            }
            TIGHTDB_ASSERT(false);
            return;
        }
    }
    throw LogicError(LogicError::no_primary_key);
}


size_t Table::do_find_pkey_int(int_fast64_t) const
{
    if (TIGHTDB_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    if (TIGHTDB_UNLIKELY(!m_primary_key))
        reveal_primary_key(); // Throws

    // FIXME: Implement this when integer indexes become available. For now, all
    // search indexes are of string type.
    throw LogicError(LogicError::type_mismatch);
}


size_t Table::do_find_pkey_string(StringData value) const
{
    if (TIGHTDB_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    if (TIGHTDB_UNLIKELY(!m_primary_key))
        reveal_primary_key(); // Throws

    // FIXME: In case of datatype mismatch throw LogicError::type_mismatch. For
    // now, all search indexes are of string type.

    size_t row_ndx = m_primary_key->find_first(value); // Throws
    return row_ndx;
}


template<class T> size_t Table::find_first(size_t col_ndx, T value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == ColumnTypeTraits3<T>::ct_id_real);

    if(!m_columns.is_attached())
        return not_found;

    typedef typename ColumnTypeTraits3<T>::column_type ColType;
    const ColType& column = get_column<ColType, ColumnTypeTraits3<T>::ct_id>(col_ndx);
    return column.find_first(value);
}

size_t Table::find_first_link(size_t target_row_index) const
{
    size_t ret = where().links_to(m_link_chain[0], target_row_index).find();
    m_link_chain.clear();
    return ret;
}

size_t Table::find_first_int(size_t col_ndx, int64_t value) const
{
    return find_first<int64_t>(col_ndx, value);
}

size_t Table::find_first_bool(size_t col_ndx, bool value) const
{
    return find_first<bool>(col_ndx, value);
}

size_t Table::find_first_datetime(size_t col_ndx, DateTime value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    TIGHTDB_ASSERT(get_real_column_type(col_ndx) == col_type_DateTime);

    if(!m_columns.is_attached())
        return not_found;

    const Column& column = get_column(col_ndx);

    return column.find_first(int64_t(value.get_datetime()));
}

size_t Table::find_first_float(size_t col_ndx, float value) const
{
    return find_first<float>(col_ndx, value);
}

size_t Table::find_first_double(size_t col_ndx, double value) const
{
    return find_first<double>(col_ndx, value);
}

size_t Table::find_first_string(size_t col_ndx, StringData value) const
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    if(!m_columns.is_attached())
        return not_found;

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(col_ndx);
        return column.find_first(value);
    }
    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(col_ndx);
    return column.find_first(value);
}

size_t Table::find_first_binary(size_t, BinaryData) const
{
    // FIXME: Implement this!
    throw runtime_error("Not implemented");
}


template <class T> TableView Table::find_all(size_t col_ndx, T value)
{
    return where().equal(col_ndx, value).find_all();
}

TableView Table::find_all_link(size_t target_row_index)
{
    TableView tv = where().links_to(m_link_chain[0], target_row_index).find_all();
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

TableView Table::find_all_datetime(size_t col_ndx, DateTime value)
{
    return find_all<int64_t>(col_ndx, int64_t(value.get_datetime()));
}

ConstTableView Table::find_all_datetime(size_t col_ndx, DateTime value) const
{
    return const_cast<Table*>(this)->find_all<int64_t>(col_ndx, int64_t(value.get_datetime()));
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
    throw runtime_error("Not implemented");
}

ConstTableView Table::find_all_binary(size_t, BinaryData) const
{
    // FIXME: Implement this!
    throw runtime_error("Not implemented");
}

TableView Table::get_distinct_view(size_t col_ndx)
{
    // FIXME: lacks support for reactive updates
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    TIGHTDB_ASSERT(has_search_index(col_ndx));

    TableView tv(*this);
    Column& refs = tv.m_row_indexes;

    if(m_columns.is_attached()) {
        ColumnType type = get_real_column_type(col_ndx);
        if (type == col_type_String) {
            const AdaptiveStringColumn& column = get_column_string(col_ndx);
            const StringIndex& index = column.get_search_index();
            index.distinct(refs);
        }
        else {
            TIGHTDB_ASSERT(type == col_type_StringEnum);
            const ColumnStringEnum& column = get_column_string_enum(col_ndx);
            const StringIndex& index = column.get_search_index();
            index.distinct(refs);
        }
    }
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
        result.add_search_index(0);
        const StringIndex& dst_index = result.get_column_string(0).get_search_index();

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
        Column& refs = ctv.m_row_indexes;
        for (size_t i = begin; i < end; ++i)
            refs.add(i);
    }
    return ctv;
}

ConstTableView Table::get_range_view(size_t begin, size_t end) const
{
    return const_cast<Table*>(this)->get_range_view(begin, end);
}



size_t Table::lower_bound_int(size_t col_ndx, int64_t value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).lower_bound_int(value);
}

size_t Table::upper_bound_int(size_t col_ndx, int64_t value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).upper_bound_int(value);
}

size_t Table::lower_bound_bool(size_t col_ndx, bool value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).lower_bound_int(value);
}

size_t Table::upper_bound_bool(size_t col_ndx, bool value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column(col_ndx).upper_bound_int(value);
}

size_t Table::lower_bound_float(size_t col_ndx, float value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_float(col_ndx).lower_bound(value);
}

size_t Table::upper_bound_float(size_t col_ndx, float value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_float(col_ndx).upper_bound(value);
}

size_t Table::lower_bound_double(size_t col_ndx, double value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_double(col_ndx).lower_bound(value);
}

size_t Table::upper_bound_double(size_t col_ndx, double value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    return !m_columns.is_attached() ? 0 : get_column_double(col_ndx).upper_bound(value);
}

size_t Table::lower_bound_string(size_t col_ndx, StringData value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    if(!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(col_ndx);
        return column.lower_bound_string(value);
    }
    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(col_ndx);
    return column.lower_bound_string(value);
}

size_t Table::upper_bound_string(size_t col_ndx, StringData value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx < m_columns.size());
    if(!m_columns.is_attached())
        return 0;

    ColumnType type = get_real_column_type(col_ndx);
    if (type == col_type_String) {
        const AdaptiveStringColumn& column = get_column_string(col_ndx);
        return column.upper_bound_string(value);
    }
    TIGHTDB_ASSERT(type == col_type_StringEnum);
    const ColumnStringEnum& column = get_column_string_enum(col_ndx);
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

            ref_type ref, keys_ref;
            bool res = column->auto_enumerate(keys_ref, ref);
            if (!res)
                continue;

            Spec::ColumnInfo info;
            m_spec.get_column_info(i, info);
            ArrayParent* keys_parent;
            size_t keys_ndx_in_parent;
            m_spec.upgrade_string_to_enum(i, keys_ref, keys_parent, keys_ndx_in_parent);

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
            size_t ndx_in_parent = m_spec.get_column_ndx_in_parent(i);

            // Replace column
            ColumnStringEnum* e = new ColumnStringEnum(alloc, ref, keys_ref); // Throws
            e->set_parent(&m_columns, ndx_in_parent);
            e->get_keys().set_parent(keys_parent, keys_ndx_in_parent);
            m_cols[i] = e;
            m_columns.set(ndx_in_parent, ref); // Throws

            // Inherit any existing index
            if (info.m_has_search_index) {
                StringIndex* index = column->release_search_index();
                e->install_search_index(index);
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
                int attr = spec.get_column_attr(i);
                // Remove any index specifying attributes
                attr &= ~(col_attr_Indexed | col_attr_PrimaryKey);
                spec.set_column_attr(i, ColumnAttr(attr)); // Throws
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
void Table::to_json_row(std::size_t row_ndx, std::ostream& out, size_t link_depth, std::map<std::string,
                        std::string>* renames) const
{
    std::map<std::string, std::string> renames2;
    renames = renames ? renames : &renames2;

    vector<ref_type> followed;
    to_json_row(row_ndx, out, link_depth, *renames, followed);
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
    // fixme, windows prints exponent as 3 digits while *nix prints 2. We use _set_output_format()
    // and restore it again with _set_output_format() because we're a library which must not permanently
    // set modes that effect the application. However, this method of set/get is not thread safe! Must
    // be fixed before releasing Windows versions of core.
#if _MSC_VER
    int oldformat = _get_output_format();
    _set_output_format(_TWO_DIGIT_EXPONENT);
#endif
    streamsize old = out.precision();
    out.precision(numeric_limits<T>::digits10 + 1);
    out << scientific << value;
    out.precision(old);

#if _MSC_VER
    _set_output_format(oldformat);
#endif

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

void Table::to_json_row(std::size_t row_ndx, std::ostream& out, size_t link_depth,
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
        {
            ColumnLinkBase& clb = const_cast<Table*>(this)->get_column_link_base(i);
            ColumnLink& cl = static_cast<ColumnLink&>(clb);
            Table& table = cl.get_target_table();

            if (!cl.is_null_link(row_ndx)) {
                ref_type lnk = clb.get_ref();
                if ((link_depth == 0) ||
                    (link_depth == not_found && std::find(followed.begin(), followed.end(), lnk) != followed.end())) {
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
        case type_LinkList:
        {
            ColumnLinkBase& clb = const_cast<Table*>(this)->get_column_link_base(i);
            ColumnLinkList& cll = static_cast<ColumnLinkList&>(clb);
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
                for (size_t link = 0; link < lv->size(); link++) {
                    if (link > 0)
                        out << ", ";
                    followed.push_back(lnk);
                    size_t new_depth = link_depth == not_found ? not_found : link_depth - 1;
                    table.to_json_row(lv->get(link).get_index(), out, new_depth, renames, followed);
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

    // fixme, windows prints exponents as 3 digits while *nix prints 2. We use _set_output_format()
    // and restore it again with _set_output_format() because we're a library which must not permanently
    // set modes that effect the application. However, this method of set/get is not thread safe! Must
    // be fixed before releasing Windows versions of core.
#if _MSC_VER
    int oldformat = _get_output_format();
    _set_output_format(_TWO_DIGIT_EXPONENT);
#endif

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

#if _MSC_VER
    _set_output_format(oldformat);
#endif

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
        enum_root = c->get_keys().get_root_array();
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


Group* Table::Parent::get_parent_group() TIGHTDB_NOEXCEPT
{
    return 0;
}


Table* Table::Parent::get_parent_table(size_t*) TIGHTDB_NOEXCEPT
{
    return 0;
}


void Table::adj_accessors_insert_rows(size_t row_ndx, size_t num_rows) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

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
    // underlying node structure. See AccessorConsistencyLevels.

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
    // underlying node structure. See AccessorConsistencyLevels.

    adj_row_acc_move_last_over(target_row_ndx, last_row_ndx);

    // Adjust subtable accessors after 'move last over' removal of a row
    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->adj_accessors_move_last_over(target_row_ndx, last_row_ndx);
    }
}


void Table::adj_acc_clear_root_table() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistencyLevels.

    discard_row_accessors();

    size_t n = m_cols.size();
    for (size_t i = 0; i < n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->adj_acc_clear_root_table();
    }
}


void Table::adj_acc_clear_nonroot_table() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConcistencyLevels.

    discard_child_accessors();
    destroy_column_accessors();
    m_columns.detach();
}


void Table::adj_row_acc_insert_rows(size_t row_ndx, size_t num_rows) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Adjust row accessors after insertion of new rows
    for (RowBase* row = m_row_accessors; row; row = row->m_next) {
        if (row->m_row_ndx >= row_ndx)
            row->m_row_ndx += num_rows;
    }
}


void Table::adj_row_acc_erase_row(size_t row_ndx) TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Adjust row accessors after removal of a row
    RowBase* row = m_row_accessors;
    while (row) {
        RowBase* next = row->m_next;
        if (row->m_row_ndx == row_ndx) {
            row->m_table.reset();
            unregister_row_accessor(row);
        }
        else if (row->m_row_ndx > row_ndx) {
            --row->m_row_ndx;
        }
        row = next;
    }
}


void Table::adj_row_acc_move_last_over(size_t target_row_ndx, size_t last_row_ndx)
    TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Adjust row accessors after 'move last over' removal of a row
    RowBase* row = m_row_accessors;
    while (row) {
        RowBase* next = row->m_next;
        if (row->m_row_ndx == target_row_ndx) {
            row->m_table.reset();
            unregister_row_accessor(row);
        }
        else if (row->m_row_ndx == last_row_ndx) {
            row->m_row_ndx = target_row_ndx;
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
    // underlying node structure. See AccessorConsistencyLevels.

    TIGHTDB_ASSERT(is_attached());
    bool not_degenerate = m_columns.is_attached();
    if (not_degenerate) {
        TIGHTDB_ASSERT(col_ndx < m_cols.size());
        if (ColumnBase* col = m_cols[col_ndx])
            delete col;
        m_cols.erase(m_cols.begin() + col_ndx);
    }
}


void Table::recursive_mark() TIGHTDB_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    mark();

    size_t n = m_cols.size();
    for (size_t i = 0; i != n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->mark(ColumnBase::mark_Recursive);
    }
}


void Table::mark_link_target_tables(size_t col_ndx_begin) TIGHTDB_NOEXCEPT
{
    // Beyond the constraints on the specified column index, this function must
    // assume no more than minimal consistency of the accessor hierarchy. This
    // means in particular that it cannot access the underlying node
    // structure. See AccessorConsistencyLevels.

    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(!m_columns.is_attached() || col_ndx_begin <= m_cols.size());
    size_t n = m_cols.size();
    for (size_t i = col_ndx_begin; i < n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->mark(ColumnBase::mark_LinkTargets);
    }
}


void Table::mark_opposite_link_tables() TIGHTDB_NOEXCEPT
{
    // Beyond the constraints on the specified column index, this function must
    // assume no more than minimal consistency of the accessor hierarchy. This
    // means in particular that it cannot access the underlying node
    // structure. See AccessorConsistencyLevels.

    TIGHTDB_ASSERT(is_attached());
    size_t n = m_cols.size();
    for (size_t i = 0; i < n; ++i) {
        if (ColumnBase* col = m_cols[i])
            col->mark(ColumnBase::mark_LinkOrigins | ColumnBase::mark_LinkTargets);
    }
}


void Table::refresh_accessor_tree()
{
    TIGHTDB_ASSERT(is_attached());

    if (m_top.is_attached()) {
        // Root table (free-standing table, group-level table, or subtable with
        // independent descriptor)
        m_top.init_from_parent();
        m_spec.init_from_parent();
        m_columns.init_from_parent();
    }
    else {
        // Subtable with shared descriptor
        m_spec.init_from_parent();

        // If the underlying table was degenerate, then `m_cols` must still be
        // empty.
        TIGHTDB_ASSERT(m_columns.is_attached() || m_cols.empty());

        ref_type columns_ref = m_columns.get_ref_from_parent();
        if (columns_ref != 0) {
            if (!m_columns.is_attached()) {
                // The underlying table is no longer degenerate
                size_t num_cols = m_spec.get_column_count();
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


void Table::refresh_column_accessors(size_t col_ndx_begin)
{
    m_primary_key = 0;

    // Index of column in Table::m_columns, which is not always equal to the
    // 'logical' column index.
    size_t ndx_in_parent = m_spec.get_column_ndx_in_parent(col_ndx_begin);

    size_t col_ndx_end = m_cols.size();
    for (size_t col_ndx = col_ndx_begin; col_ndx != col_ndx_end; ++col_ndx) {
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
            col->get_root_array()->set_ndx_in_parent(ndx_in_parent);
            col->refresh_accessor_tree(col_ndx, m_spec); // Throws
        }
        else {
            ColumnType col_type = m_spec.get_column_type(col_ndx);
            col = create_column_accessor(col_type, col_ndx, ndx_in_parent); // Throws
            m_cols[col_ndx] = col;
            // In the case of a link-type column, we must establish a connection
            // between it and the corresponding backlink column. This, however,
            // cannot be done until both the origin and the target table
            // accessor have been sufficiently refreshed. The solution is to
            // attempt the connection establishment when the link coumn is
            // created, and when the backlink column is created. In both cases,
            // if the opposite table accessor is still dirty, the establishment
            // of the connection is postponed.
            typedef _impl::GroupFriend gf;
            if (is_link_type(col_type)) {
                Group& group = *get_parent_group();
                size_t target_table_ndx = m_spec.get_opposite_link_table_ndx(col_ndx);
                Table& target_table = gf::get_table(group, target_table_ndx); // Throws
                if (!target_table.is_marked() && &target_table != this) {
                    size_t origin_ndx_in_group = m_top.get_ndx_in_parent();
                    size_t backlink_col_ndx =
                        target_table.m_spec.find_backlink_column(origin_ndx_in_group, col_ndx);
                    connect_opposite_link_columns(col_ndx, target_table, backlink_col_ndx);
                }
            }
            else if (col_type == col_type_BackLink) {
                Group& group = *get_parent_group();
                size_t origin_table_ndx = m_spec.get_opposite_link_table_ndx(col_ndx);
                Table& origin_table = gf::get_table(group, origin_table_ndx); // Throws
                if (!origin_table.is_marked() || &origin_table == this) {
                    size_t link_col_ndx = m_spec.get_origin_column_ndx(col_ndx);
                    origin_table.connect_opposite_link_columns(link_col_ndx, *this, col_ndx);
                }
            }
        }

        // If there is no search index accessor, but the column has been
        // equipped with a search index, create the accessor now.
        ColumnAttr attr = m_spec.get_column_attr(col_ndx);
        bool has_search_index = (attr & col_attr_Indexed)    != 0;
        bool is_primary_key   = (attr & col_attr_PrimaryKey) != 0;
        TIGHTDB_ASSERT(has_search_index || !is_primary_key);
        if (has_search_index) {
            bool allow_duplicate_values = !is_primary_key;
            if (col->has_search_index()) {
                col->set_search_index_allow_duplicate_values(allow_duplicate_values);
            }
            else {
                ref_type ref = m_columns.get_as_ref(ndx_in_parent+1);
                col->set_search_index_ref(ref, &m_columns, ndx_in_parent+1,
                                          allow_duplicate_values); // Throws
            }
        }
        ndx_in_parent += (has_search_index ? 2 : 1);
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
}


bool Table::is_cross_table_link_target() const TIGHTDB_NOEXCEPT
{
    size_t n = m_cols.size();
    for (size_t i = m_spec.get_public_column_count(); i < n; ++i) {
        TIGHTDB_ASSERT(dynamic_cast<ColumnBackLink*>(m_cols[i]));
        ColumnBackLink& backlink_col = static_cast<ColumnBackLink&>(*m_cols[i]);
        Table& origin = backlink_col.get_origin_table();
        if (&origin != this)
            return true;
    }
    return false;
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
        for (RowBase* row = m_row_accessors; row; row = row->m_next) {
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
            std::size_t ndx_in_parent = m_spec.get_column_ndx_in_parent(i);
            TIGHTDB_ASSERT(ndx_in_parent == column.get_root_array()->get_ndx_in_parent());
            column.Verify(*this, i);
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
        out << setw(indent) << "" << "  Column "<<(i+1)<<"\n";
        const ColumnBase& column = get_column_base(i);
        column.do_dump_node_structure(out, level+2);
    }
}


#endif // TIGHTDB_DEBUG
