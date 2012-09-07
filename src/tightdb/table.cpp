#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <iomanip>
#include <fstream>

#include <tightdb/table.hpp>
#include <tightdb/index.hpp>
#include <tightdb/alloc_slab.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_binary.hpp>
#include <tightdb/column_table.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/index_string.hpp>

using namespace std;

namespace tightdb {


struct FakeParent: Table::Parent {
    virtual void update_child_ref(size_t, size_t) {} // Ignore
    virtual void child_destroyed(size_t) {} // Ignore
    virtual size_t get_child_ref(size_t) const { return 0; }
};


// -- Table ---------------------------------------------------------------------------------

void Table::init_from_ref(size_t top_ref, ArrayParent* parent, size_t ndx_in_parent)
{
    // Load from allocated memory
    m_top.UpdateRef(top_ref);
    m_top.SetParent(parent, ndx_in_parent);
    TIGHTDB_ASSERT(m_top.Size() == 2);

    const size_t spec_ref    = m_top.GetAsRef(0);
    const size_t columns_ref = m_top.GetAsRef(1);

    init_from_ref(spec_ref, columns_ref, &m_top, 1);
    m_spec_set.set_parent(&m_top, 0);
}

void Table::init_from_ref(size_t spec_ref, size_t columns_ref,
                          ArrayParent* parent, size_t ndx_in_parent)
{
    m_spec_set.update_ref(spec_ref);

    // A table instatiated with a zero-ref is just an empty table
    // but it will have to create itself on first modification
    if (columns_ref != 0) {
        m_columns.UpdateRef(columns_ref);
        CacheColumns(); // Also initializes m_size
    }
    m_columns.SetParent(parent, ndx_in_parent);
}

void Table::CreateColumns()
{
    TIGHTDB_ASSERT(!m_columns.IsValid() || m_columns.is_empty()); // only on initial creation

    // Instantiate first if we have an empty table (from zero-ref)
    if (!m_columns.IsValid()) {
        m_columns.SetType(COLUMN_HASREFS);
    }

    size_t subtable_count = 0;
    ColumnType attr = COLUMN_ATTR_NONE;
    Allocator& alloc = m_columns.GetAllocator();
    const size_t count = m_spec_set.get_type_attr_count();

    // Add the newly defined columns
    for (size_t i=0; i<count; ++i) {
        const ColumnType type = m_spec_set.get_type_attr(i);
        const size_t ref_pos =  m_columns.Size();
        ColumnBase* new_col = 0;

        switch (type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOL:
        case COLUMN_TYPE_DATE:
            {
                Column* c = new Column(COLUMN_NORMAL, alloc);
                m_columns.add(c->GetRef());
                c->SetParent(&m_columns, ref_pos);
                new_col = c;
            }
            break;
        case COLUMN_TYPE_STRING:
            {
                AdaptiveStringColumn* c = new AdaptiveStringColumn(alloc);
                m_columns.add(c->GetRef());
                c->SetParent(&m_columns, ref_pos);
                new_col = c;
            }
            break;
        case COLUMN_TYPE_BINARY:
            {
                ColumnBinary* c = new ColumnBinary(alloc);
                m_columns.add(c->GetRef());
                c->SetParent(&m_columns, ref_pos);
                new_col = c;
            }
            break;
        case COLUMN_TYPE_TABLE:
            {
                const size_t column_ndx = m_cols.Size();
                const size_t subspec_ref = m_spec_set.get_subspec_ref(subtable_count);
                ColumnTable* c = new ColumnTable(alloc, this, column_ndx, subspec_ref);
                m_columns.add(c->GetRef());
                c->SetParent(&m_columns, ref_pos);
                new_col = c;
                ++subtable_count;
            }
            break;
        case COLUMN_TYPE_MIXED:
            {
                const size_t column_ndx = m_cols.Size();
                ColumnMixed* c = new ColumnMixed(alloc, this, column_ndx);
                m_columns.add(c->GetRef());
                c->SetParent(&m_columns, ref_pos);
                new_col = c;
            }
            break;

            // Attributes
        case COLUMN_ATTR_INDEXED:
        case COLUMN_ATTR_UNIQUE:
            attr = type;
            continue; // attr prefix column types)

        default:
            TIGHTDB_ASSERT(false);
        }

        // Cache Columns
        m_cols.add(reinterpret_cast<intptr_t>(new_col)); // FIXME: intptr_t is not guaranteed to exists, not even in C++11

        // Atributes on columns may define that they come with an index
        if (attr != COLUMN_ATTR_NONE) {
            const size_t column_ndx = m_cols.Size()-1;
            set_index(column_ndx, false);

            attr = COLUMN_ATTR_NONE;
        }
    }
}

Spec& Table::get_spec()
{
    TIGHTDB_ASSERT(m_top.IsValid()); // you can only change specs on top-level tables
    return m_spec_set;
}

const Spec& Table::get_spec() const
{
    return m_spec_set;
}


void Table::invalidate()
{
    // This prevents the destructor from deallocating the underlying
    // memory structure, and from attempting to notify the parent. It
    // also causes is_valid() to return false.
    m_columns.SetParent(0,0);

    // Invalidate all subtables
    const size_t n = m_cols.Size();
    for (size_t i=0; i<n; ++i) {
        ColumnBase* const c = reinterpret_cast<ColumnBase*>(m_cols.Get(i));
        if (ColumnTable* c2 = dynamic_cast<ColumnTable*>(c)) {
            c2->invalidate_subtables();
        }
        else if (ColumnMixed* c2 = dynamic_cast<ColumnMixed*>(c)) {
            c2->invalidate_subtables();
        }
    }

    ClearCachedColumns();
}


void Table::InstantiateBeforeChange()
{
    // Empty (zero-ref'ed) tables need to be instantiated before first modification
    if (!m_columns.IsValid()) CreateColumns();
}

void Table::CacheColumns()
{
    TIGHTDB_ASSERT(m_cols.is_empty()); // only done on creation

    Allocator& alloc = m_columns.GetAllocator();
    ColumnType attr = COLUMN_ATTR_NONE;
    size_t size = size_t(-1);
    size_t ndx_in_parent = 0;
    const size_t count = m_spec_set.get_type_attr_count();
    size_t subtable_count = 0;

    // Cache columns
    for (size_t i = 0; i < count; ++i) {
        const ColumnType type = m_spec_set.get_type_attr(i);
        const size_t ref = m_columns.GetAsRef(ndx_in_parent);

        ColumnBase* new_col = 0;
        size_t colsize = size_t(-1);
        switch (type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOL:
        case COLUMN_TYPE_DATE:
            {
                Column* c = new Column(ref, &m_columns, ndx_in_parent, alloc);
                colsize = c->Size();
                new_col = c;
            }
            break;
        case COLUMN_TYPE_STRING:
            {
                AdaptiveStringColumn* c =
                    new AdaptiveStringColumn(ref, &m_columns, ndx_in_parent, alloc);
                colsize = c->Size();
                new_col = c;
            }
            break;
        case COLUMN_TYPE_BINARY:
            {
                ColumnBinary* c = new ColumnBinary(ref, &m_columns, ndx_in_parent, alloc);
                colsize = c->Size();
                new_col = c;
            }
            break;
        case COLUMN_TYPE_STRING_ENUM:
            {
                const size_t values_ref = m_columns.GetAsRef(ndx_in_parent+1);
                ColumnStringEnum* c =
                    new ColumnStringEnum(ref, values_ref, &m_columns, ndx_in_parent, alloc);
                colsize = c->Size();
                new_col = c;
                ++ndx_in_parent; // advance one extra pos to account for keys/values pair
            }
            break;
        case COLUMN_TYPE_TABLE:
            {
                const size_t column_ndx = m_cols.Size();
                const size_t spec_ref = m_spec_set.get_subspec_ref(subtable_count);
                ColumnTable* c = new ColumnTable(alloc, this, column_ndx, &m_columns, ndx_in_parent,
                                                 spec_ref, ref);
                colsize = c->Size();
                new_col = c;
                ++subtable_count;
            }
            break;
        case COLUMN_TYPE_MIXED:
            {
                const size_t column_ndx = m_cols.Size();
                ColumnMixed* c =
                    new ColumnMixed(alloc, this, column_ndx, &m_columns, ndx_in_parent, ref);
                colsize = c->Size();
                new_col = c;
            }
            break;

            // Attributes (prefixing column types)
        case COLUMN_ATTR_INDEXED:
        case COLUMN_ATTR_UNIQUE:
            attr = type;
            continue;

        default:
            TIGHTDB_ASSERT(false);
        }

        m_cols.add(reinterpret_cast<intptr_t>(new_col)); // FIXME: intptr_t is not guaranteed to exists, even in C++11

        // Atributes on columns may define that they come with an index
        if (attr != COLUMN_ATTR_NONE) {
            TIGHTDB_ASSERT(attr == COLUMN_ATTR_INDEXED); // only attribute supported for now
            TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING ||
                           type == COLUMN_TYPE_STRING_ENUM);  // index only for strings

            const size_t pndx = ndx_in_parent+1;
            const size_t index_ref = m_columns.GetAsRef(pndx);
            new_col->SetIndexRef(index_ref, &m_columns, pndx);

            ++ndx_in_parent; // advance one extra pos to account for index
            attr = COLUMN_ATTR_NONE;
        }

        // Set table size
        // (and verify that all column are same length)
        if (size == size_t(-1)) size = colsize;
        else TIGHTDB_ASSERT(size == colsize);

        ++ndx_in_parent;
    }

    if (size != size_t(-1)) m_size = size;
}

void Table::ClearCachedColumns()
{
    TIGHTDB_ASSERT(m_cols.IsValid());

    const size_t count = m_cols.Size();
    for (size_t i = 0; i < count; ++i) {
        ColumnBase* const column = reinterpret_cast<ColumnBase*>(m_cols.Get(i));
        delete column;
    }
    m_cols.Destroy();
}

Table::~Table()
{
#ifdef TIGHTDB_ENABLE_REPLICATION
    get_local_transact_log().on_table_destroyed();
#endif

    if (!is_valid()) {
        // This table has been invalidated.
        TIGHTDB_ASSERT(m_ref_count == 0);
        return;
    }

    if (!m_top.IsValid()) {
        // This is a table with a shared spec, and its lifetime is
        // managed by reference counting, so we must let our parent
        // know about our demise.
        ArrayParent* parent = m_columns.GetParent();
        TIGHTDB_ASSERT(parent);
        TIGHTDB_ASSERT(m_ref_count == 0);
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(parent));
        static_cast<Parent*>(parent)->child_destroyed(m_columns.GetParentNdx());
        ClearCachedColumns();
        return;
    }

    // This is a table with an independent spec.
    if (ArrayParent* parent = m_top.GetParent()) {
        // This is a table whose lifetime is managed by reference
        // counting, so we must let our parent know about our demise.
        TIGHTDB_ASSERT(m_ref_count == 0);
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(parent));
        static_cast<Parent*>(parent)->child_destroyed(m_top.GetParentNdx());
        ClearCachedColumns();
        return;
    }

    // This is a freestanding table, so we are responsible for
    // deallocating the underlying memory structure. If the table was
    // created using the public table constructor (a stack allocated
    // table) then the reference count must be strictly positive at
    // this point. Otherwise the table has been created using
    // LangBindHelper::new_table(), and then the reference count must
    // be zero, because that is what has caused the destructor to be
    // called. In the latter case, there can be no subtables to
    // invalidate, because they would have kept the parent alive.
    if (0 < m_ref_count) invalidate();
    m_top.Destroy();
}

size_t Table::get_column_count() const
{
    return m_spec_set.get_column_count();
}

const char* Table::get_column_name(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return m_spec_set.get_column_name(ndx);
}

size_t Table::get_column_index(const char* name) const
{
    return m_spec_set.get_column_index(name);
}

ColumnType Table::GetRealColumnType(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return m_spec_set.get_real_column_type(ndx);
}

ColumnType Table::get_column_type(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());

    // Hides internal types like COLUM_STRING_ENUM
    return m_spec_set.get_column_type(ndx);
}

size_t Table::GetColumnRefPos(size_t column_ndx) const
{
    size_t pos = 0;
    size_t current_column = 0;
    const size_t count = m_spec_set.get_type_attr_count();

    for (size_t i = 0; i < count; ++i) {
        if (current_column == column_ndx)
            return pos;

        const ColumnType type = (ColumnType)m_spec_set.get_type_attr(i);
        if (type >= COLUMN_ATTR_INDEXED)
            continue; // ignore attributes
        if (type < COLUMN_TYPE_STRING_ENUM)
            ++pos;
        else
            pos += 2;

        ++current_column;
    }

    TIGHTDB_ASSERT(false);
    return (size_t)-1;
}

size_t Table::add_column(ColumnType type, const char* name)
{
    // Currently it's not possible to dynamically add columns to a table with content.
    TIGHTDB_ASSERT(size() == 0);
    if (size() != 0)
        return size_t(-1);

    m_spec_set.add_column(type, name); // FIXME: May fail

    const size_t column_ndx = m_cols.Size();

    ColumnBase* new_col = 0;
    Allocator& alloc = m_columns.GetAllocator();

    switch (type) {
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_BOOL:
    case COLUMN_TYPE_DATE:
        {
            Column* c = new Column(COLUMN_NORMAL, alloc);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
        }
        break;
    case COLUMN_TYPE_STRING:
        {
            AdaptiveStringColumn* c = new AdaptiveStringColumn(alloc);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
        }
        break;
    case COLUMN_TYPE_BINARY:
        {
            ColumnBinary* c = new ColumnBinary(alloc);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
        }
        break;

    case COLUMN_TYPE_TABLE:
        {
            const size_t subspec_ref = m_spec_set.get_subspec_ref(m_spec_set.get_num_subspecs()-1);
            ColumnTable* c = new ColumnTable(alloc, this, column_ndx, subspec_ref);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
        }
        break;

    case COLUMN_TYPE_MIXED:
        {
            ColumnMixed* c = new ColumnMixed(alloc, this, column_ndx);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
        }
        break;
    default:
        TIGHTDB_ASSERT(false);
    }

    m_cols.add(reinterpret_cast<intptr_t>(new_col)); // FIXME: intptr_t is not guaranteed to exists, even in C++11

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().add_column(type, name);
    if (err) throw_error(err);
#endif

    return column_ndx;
}

bool Table::has_index(size_t column_ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    const ColumnBase& col = GetColumnBase(column_ndx);
    return col.HasIndex();
}

void Table::set_index(size_t column_ndx, bool update_spec)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    if (has_index(column_ndx)) return;

    const ColumnType ct = GetRealColumnType(column_ndx);
    const size_t column_pos = GetColumnRefPos(column_ndx);
    size_t ndx_ref = -1;

    if (ct == COLUMN_TYPE_STRING) {
        AdaptiveStringColumn& col = GetColumnString(column_ndx);

        // Create the index
        StringIndex& ndx = col.CreateIndex();
        ndx.SetParent(&m_columns, column_pos+1);
        ndx_ref = ndx.GetRef();
    }
    else if (ct == COLUMN_TYPE_STRING_ENUM) {
        ColumnStringEnum& col = GetColumnStringEnum(column_ndx);

        // Create the index
        StringIndex& ndx = col.CreateIndex();
        ndx.SetParent(&m_columns, column_pos+1);
        ndx_ref = ndx.GetRef();
    }
    else {
        TIGHTDB_ASSERT(false);
        return;
    }

    // Insert ref into columns list after the owning column
    m_columns.Insert(column_pos+1, ndx_ref);
    UpdateColumnRefs(column_ndx+1, 1);

    // Update spec
    if (update_spec)
        m_spec_set.set_column_attr(column_ndx, COLUMN_ATTR_INDEXED);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().add_index_to_column(column_ndx);
    if (err) throw_error(err);
#endif
}

ColumnBase& Table::GetColumnBase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    InstantiateBeforeChange();
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    return *reinterpret_cast<ColumnBase*>(m_cols.Get(ndx));
}

const ColumnBase& Table::GetColumnBase(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    return *reinterpret_cast<ColumnBase*>(m_cols.Get(ndx));
}

Column& Table::GetColumn(size_t ndx)
{
    ColumnBase& column = GetColumnBase(ndx);
    TIGHTDB_ASSERT(column.IsIntColumn());
    return static_cast<Column&>(column);
}

const Column& Table::GetColumn(size_t ndx) const
{
    const ColumnBase& column = GetColumnBase(ndx);
    TIGHTDB_ASSERT(column.IsIntColumn());
    return static_cast<const Column&>(column);
}

AdaptiveStringColumn& Table::GetColumnString(size_t ndx)
{
    ColumnBase& column = GetColumnBase(ndx);
    TIGHTDB_ASSERT(column.IsStringColumn());
    return static_cast<AdaptiveStringColumn&>(column);
}

const AdaptiveStringColumn& Table::GetColumnString(size_t ndx) const
{
    const ColumnBase& column = GetColumnBase(ndx);
    TIGHTDB_ASSERT(column.IsStringColumn());
    return static_cast<const AdaptiveStringColumn&>(column);
}


ColumnStringEnum& Table::GetColumnStringEnum(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    InstantiateBeforeChange();
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    return *reinterpret_cast<ColumnStringEnum*>(m_cols.Get(ndx));
}

const ColumnStringEnum& Table::GetColumnStringEnum(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    return *reinterpret_cast<ColumnStringEnum*>(m_cols.Get(ndx));
}

ColumnBinary& Table::GetColumnBinary(size_t ndx)
{
    ColumnBase& column = GetColumnBase(ndx);
    TIGHTDB_ASSERT(column.IsBinaryColumn());
    return static_cast<ColumnBinary&>(column);
}

const ColumnBinary& Table::GetColumnBinary(size_t ndx) const
{
    const ColumnBase& column = GetColumnBase(ndx);
    TIGHTDB_ASSERT(column.IsBinaryColumn());
    return static_cast<const ColumnBinary&>(column);
}

ColumnTable &Table::GetColumnTable(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    InstantiateBeforeChange();
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    return *static_cast<ColumnTable*>(reinterpret_cast<ColumnBase*>(m_cols.Get(ndx)));
}

const ColumnTable &Table::GetColumnTable(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    return *static_cast<ColumnTable*>(reinterpret_cast<ColumnBase*>(m_cols.Get(ndx)));
}

ColumnMixed& Table::GetColumnMixed(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    InstantiateBeforeChange();
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    return *static_cast<ColumnMixed*>(reinterpret_cast<ColumnBase*>(m_cols.Get(ndx)));
}

const ColumnMixed& Table::GetColumnMixed(size_t ndx) const
{
    TIGHTDB_ASSERT(m_cols.Size() == get_column_count());
    TIGHTDB_ASSERT(ndx < get_column_count());
    return *static_cast<ColumnMixed*>(reinterpret_cast<ColumnBase*>(m_cols.Get(ndx)));
}

size_t Table::add_empty_row(size_t num_rows)
{
    const size_t n = get_column_count();
    for (size_t i=0; i<n; ++i) {
        ColumnBase& column = GetColumnBase(i);
        for (size_t j=0; j<num_rows; ++j) {
            column.add();
        }
    }

    // Return index of first new added row
    size_t new_ndx = m_size;
    m_size += num_rows;

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().insert_empty_rows(new_ndx, 1);
    if (err) throw_error(err);
#endif

    return new_ndx;
}

void Table::insert_empty_row(size_t ndx, size_t num_rows)
{
    const size_t ndx2 = ndx + num_rows; // FIXME: Should we check for overflow?
    const size_t n = get_column_count();
    for (size_t i=0; i<n; ++i) {
        ColumnBase& column = GetColumnBase(i);
        // FIXME: This could maybe be optimized by passing 'num_rows' to column.insert()
        for (size_t j=ndx; j<ndx2; ++j) {
            column.insert(j);
        }
    }

    m_size += num_rows;

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().insert_empty_rows(ndx, num_rows);
    if (err) throw_error(err);
#endif
}

void Table::clear()
{
    const size_t count = get_column_count();
    for (size_t i = 0; i < count; ++i) {
        ColumnBase& column = GetColumnBase(i);
        column.Clear();
    }
    m_size = 0;

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().clear_table();
    if (err) throw_error(err);
#endif
}

void Table::remove(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_size);

    const size_t count = get_column_count();
    for (size_t i = 0; i < count; ++i) {
        ColumnBase& column = GetColumnBase(i);
        column.Delete(ndx);
    }
    --m_size;

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().remove_row(ndx);
    if (err) throw_error(err);
#endif
}

void Table::insert_subtable(size_t column_ndx, size_t ndx)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_TABLE);
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnTable& subtables = GetColumnTable(column_ndx);
    subtables.invalidate_subtables();
    subtables.Insert(ndx); // FIXME: Consider calling virtual method insert(size_t) instead.

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err =
        get_local_transact_log().insert_value(column_ndx, ndx, Replication::subtable_tag());
    if (err) throw_error(err);
#endif
}

Table* Table::get_subtable_ptr(size_t col_idx, size_t row_idx)
{
    TIGHTDB_ASSERT(col_idx < get_column_count());
    TIGHTDB_ASSERT(row_idx < m_size);

    const ColumnType type = GetRealColumnType(col_idx);
    if (type == COLUMN_TYPE_TABLE) {
        ColumnTable& subtables = GetColumnTable(col_idx);
        return subtables.get_subtable_ptr(row_idx);
    }
    if (type == COLUMN_TYPE_MIXED) {
        ColumnMixed& subtables = GetColumnMixed(col_idx);
        return subtables.get_subtable_ptr(row_idx);
    }
    TIGHTDB_ASSERT(false);
    return 0;
}

const Table* Table::get_subtable_ptr(size_t col_idx, size_t row_idx) const
{
    TIGHTDB_ASSERT(col_idx < get_column_count());
    TIGHTDB_ASSERT(row_idx < m_size);

    const ColumnType type = GetRealColumnType(col_idx);
    if (type == COLUMN_TYPE_TABLE) {
        const ColumnTable& subtables = GetColumnTable(col_idx);
        return subtables.get_subtable_ptr(row_idx);
    }
    if (type == COLUMN_TYPE_MIXED) {
        const ColumnMixed& subtables = GetColumnMixed(col_idx);
        return subtables.get_subtable_ptr(row_idx);
    }
    TIGHTDB_ASSERT(false);
    return 0;
}

size_t Table::get_subtable_size(size_t col_idx, size_t row_idx) const
{
    TIGHTDB_ASSERT(col_idx < get_column_count());
    TIGHTDB_ASSERT(row_idx < m_size);

    const ColumnType type = GetRealColumnType(col_idx);
    if (type == COLUMN_TYPE_TABLE) {
        const ColumnTable& subtables = GetColumnTable(col_idx);
        return subtables.get_subtable_size(row_idx);
    }
    if (type == COLUMN_TYPE_MIXED) {
        const ColumnMixed& subtables = GetColumnMixed(col_idx);
        return subtables.get_subtable_size(row_idx);
    }
    TIGHTDB_ASSERT(false);
    return 0;
}

void Table::clear_subtable(size_t col_idx, size_t row_idx)
{
    TIGHTDB_ASSERT(col_idx < get_column_count());
    TIGHTDB_ASSERT(row_idx <= m_size);

    const ColumnType type = GetRealColumnType(col_idx);
    if (type == COLUMN_TYPE_TABLE) {
        ColumnTable& subtables = GetColumnTable(col_idx);
        subtables.Clear(row_idx);
        subtables.invalidate_subtables();

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err =
        get_local_transact_log().set_value(col_idx, row_idx, Replication::subtable_tag());
    if (err) throw_error(err);
#endif
    }
    else if (type == COLUMN_TYPE_MIXED) {
        ColumnMixed& subtables = GetColumnMixed(col_idx);
        subtables.set_subtable(row_idx);
        subtables.invalidate_subtables();

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err =
        get_local_transact_log().set_value(col_idx, row_idx, Mixed(Mixed::subtable_tag()));
    if (err) throw_error(err);
#endif
    }
    else {
        TIGHTDB_ASSERT(false);
    }
}

int64_t Table::get_int(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = GetColumn(column_ndx);
    return column.Get(ndx);
}

void Table::set_int(size_t column_ndx, size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    Column& column = GetColumn(column_ndx);
    column.Set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}

void Table::add_int(size_t column_ndx, int64_t value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_INT);
    GetColumn(column_ndx).Increment64(value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().add_int_to_column(column_ndx, value);
    if (err) throw_error(err);
#endif
}

bool Table::get_bool(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_BOOL);
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = GetColumn(column_ndx);
    return column.Get(ndx) != 0;
}

void Table::set_bool(size_t column_ndx, size_t ndx, bool value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_BOOL);
    TIGHTDB_ASSERT(ndx < m_size);

    Column& column = GetColumn(column_ndx);
    column.Set(ndx, value ? 1 : 0);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx, int(value));
    if (err) throw_error(err);
#endif
}

time_t Table::get_date(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_DATE);
    TIGHTDB_ASSERT(ndx < m_size);

    const Column& column = GetColumn(column_ndx);
    return (time_t)column.Get(ndx);
}

void Table::set_date(size_t column_ndx, size_t ndx, time_t value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_DATE);
    TIGHTDB_ASSERT(ndx < m_size);

    Column& column = GetColumn(column_ndx);
    column.Set(ndx, (int64_t)value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}

void Table::insert_int(size_t column_ndx, size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    Column& column = GetColumn(column_ndx);
    column.Insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().insert_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}

const char* Table::get_string(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnType type = GetRealColumnType(column_ndx);

    if (type == COLUMN_TYPE_STRING) {
        const AdaptiveStringColumn& column = GetColumnString(column_ndx);
        return column.Get(ndx);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        const ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        return column.Get(ndx);
    }
}

void Table::set_string(size_t column_ndx, size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnType type = GetRealColumnType(column_ndx);

    if (type == COLUMN_TYPE_STRING) {
        AdaptiveStringColumn& column = GetColumnString(column_ndx);
        column.Set(ndx, value);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        column.Set(ndx, value);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx,
                                                        BinaryData(value, std::strlen(value)));
    if (err) throw_error(err);
#endif
}

void Table::insert_string(size_t column_ndx, size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    const ColumnType type = GetRealColumnType(column_ndx);

    if (type == COLUMN_TYPE_STRING) {
        AdaptiveStringColumn& column = GetColumnString(column_ndx);
        column.Insert(ndx, value);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        column.Insert(ndx, value);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().insert_value(column_ndx, ndx,
                                                           BinaryData(value, std::strlen(value)));
    if (err) throw_error(err);
#endif
}

BinaryData Table::get_binary(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnBinary& column = GetColumnBinary(column_ndx);
    return column.Get(ndx);
}

void Table::set_binary(size_t column_ndx, size_t ndx, const char* data, size_t size)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    ColumnBinary& column = GetColumnBinary(column_ndx);
    column.Set(ndx, data, size);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx, BinaryData(data, size));
    if (err) throw_error(err);
#endif
}

void Table::insert_binary(size_t column_ndx, size_t ndx, const char* data, size_t size)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnBinary& column = GetColumnBinary(column_ndx);
    column.Insert(ndx, data, size);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().insert_value(column_ndx, ndx, BinaryData(data, size));
    if (err) throw_error(err);
#endif
}

Mixed Table::get_mixed(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnMixed& column = GetColumnMixed(column_ndx);
    const ColumnType   type   = column.GetType(ndx);

    switch (type) {
        case COLUMN_TYPE_INT:
            return Mixed(column.GetInt(ndx));
        case COLUMN_TYPE_BOOL:
            return Mixed(column.get_bool(ndx));
        case COLUMN_TYPE_DATE:
            return Mixed(Date(column.get_date(ndx)));
        case COLUMN_TYPE_STRING:
            return Mixed(column.get_string(ndx));
        case COLUMN_TYPE_BINARY:
            return Mixed(column.get_binary(ndx));
        case COLUMN_TYPE_TABLE:
            return Mixed::subtable_tag();
        default:
            TIGHTDB_ASSERT(false);
            return Mixed((int64_t)0);
    }
}

ColumnType Table::get_mixed_type(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnMixed& column = GetColumnMixed(column_ndx);
    return column.GetType(ndx);
}

void Table::set_mixed(size_t column_ndx, size_t ndx, Mixed value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    ColumnMixed& column = GetColumnMixed(column_ndx);
    const ColumnType type = value.get_type();

    switch (type) {
        case COLUMN_TYPE_INT:
            column.SetInt(ndx, value.get_int());
            break;
        case COLUMN_TYPE_BOOL:
            column.set_bool(ndx, value.get_bool());
            break;
        case COLUMN_TYPE_DATE:
            column.set_date(ndx, value.get_date());
            break;
        case COLUMN_TYPE_STRING:
            column.set_string(ndx, value.get_string());
            break;
        case COLUMN_TYPE_BINARY:
        {
            const BinaryData b = value.get_binary();
            column.set_binary(ndx, (const char*)b.pointer, b.len);
            break;
        }
        case COLUMN_TYPE_TABLE:
            column.set_subtable(ndx);
            break;
        default:
            TIGHTDB_ASSERT(false);
    }

    column.invalidate_subtables();

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}

void Table::insert_mixed(size_t column_ndx, size_t ndx, Mixed value) {
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnMixed& column = GetColumnMixed(column_ndx);
    const ColumnType type = value.get_type();

    switch (type) {
        case COLUMN_TYPE_INT:
            column.insert_int(ndx, value.get_int());
            break;
        case COLUMN_TYPE_BOOL:
            column.insert_bool(ndx, value.get_bool());
            break;
        case COLUMN_TYPE_DATE:
            column.insert_date(ndx, value.get_date());
            break;
        case COLUMN_TYPE_STRING:
            column.insert_string(ndx, value.get_string());
            break;
        case COLUMN_TYPE_BINARY:
        {
            const BinaryData b = value.get_binary();
            column.insert_binary(ndx, (const char*)b.pointer, b.len);
            break;
        }
        case COLUMN_TYPE_TABLE:
            column.insert_subtable(ndx);
            break;
        default:
            TIGHTDB_ASSERT(false);
    }

    column.invalidate_subtables();

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().insert_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}

void Table::insert_done()
{
    ++m_size;

#ifdef TIGHTDB_DEBUG
    Verify();
#endif // TIGHTDB_DEBUG

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().row_insert_complete();
    if (err) throw_error(err);
#endif
}

size_t Table::count(size_t column_ndx, int64_t target) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == COLUMN_TYPE_INT);

    const Column& column = GetColumn(column_ndx);
    return column.count(target);
}

size_t Table::count_string(size_t column_ndx, const char* value) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(value);
    
    const ColumnType type = GetRealColumnType(column_ndx);
    
    if (type == COLUMN_TYPE_STRING) {
        const AdaptiveStringColumn& column = GetColumnString(column_ndx);
        return column.count(value);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        const ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        return column.count(value);
    }
}

int64_t Table::sum(size_t column_ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == COLUMN_TYPE_INT);

    const Column& column = GetColumn(column_ndx);
    return column.sum();
}

int64_t Table::maximum(size_t column_ndx) const
{
    if (is_empty()) return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        const int64_t v = get_int(column_ndx, i);
        if (v > mv) {
            mv = v;
        }
    }
    return mv;
}

int64_t Table::minimum(size_t column_ndx) const
{
    if (is_empty()) return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        const int64_t v = get_int(column_ndx, i);
        if (v < mv) {
            mv = v;
        }
    }
    return mv;
}

size_t Table::find_first_int(size_t column_ndx, int64_t value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_INT);
    const Column& column = GetColumn(column_ndx);

    return column.find_first(value);
}

size_t Table::find_first_bool(size_t column_ndx, bool value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_BOOL);
    const Column& column = GetColumn(column_ndx);

    return column.find_first(value ? 1 : 0);
}

size_t Table::find_first_date(size_t column_ndx, time_t value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_DATE);
    const Column& column = GetColumn(column_ndx);

    return column.find_first((int64_t)value);
}

size_t Table::find_first_string(size_t column_ndx, const char* value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const ColumnType type = GetRealColumnType(column_ndx);

    if (type == COLUMN_TYPE_STRING) {
        const AdaptiveStringColumn& column = GetColumnString(column_ndx);
        return column.find_first(value);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        const ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        return column.find_first(value);
    }
}

size_t Table::find_pos_int(size_t column_ndx, int64_t value) const
{
    return GetColumn(column_ndx).find_pos(value);
}

TableView Table::find_all_int(size_t column_ndx, int64_t value)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    TableView tv(*this);
    column.find_all(tv.get_ref_column(), value);
    return move(tv);
}

ConstTableView Table::find_all_int(size_t column_ndx, int64_t value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    ConstTableView tv(*this);
    column.find_all(tv.get_ref_column(), value);
    return move(tv);
}

TableView Table::find_all_bool(size_t column_ndx, bool value)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    TableView tv(*this);
    column.find_all(tv.get_ref_column(), value ? 1 :0);
    return move(tv);
}

ConstTableView Table::find_all_bool(size_t column_ndx, bool value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    ConstTableView tv(*this);
    column.find_all(tv.get_ref_column(), value ? 1 :0);
    return move(tv);
}

TableView Table::find_all_date(size_t column_ndx, time_t value)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    TableView tv(*this);
    column.find_all(tv.get_ref_column(), int64_t(value));
    return move(tv);
}

ConstTableView Table::find_all_date(size_t column_ndx, time_t value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    ConstTableView tv(*this);
    column.find_all(tv.get_ref_column(), int64_t(value));
    return move(tv);
}

TableView Table::find_all_string(size_t column_ndx, const char *value)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const ColumnType type = GetRealColumnType(column_ndx);

    TableView tv(*this);
    if (type == COLUMN_TYPE_STRING) {
        const AdaptiveStringColumn& column = GetColumnString(column_ndx);
        column.find_all(tv.get_ref_column(), value);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        const ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        column.find_all(tv.get_ref_column(), value);
    }
    return move(tv);
}

ConstTableView Table::find_all_string(size_t column_ndx, const char *value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const ColumnType type = GetRealColumnType(column_ndx);

    ConstTableView tv(*this);
    if (type == COLUMN_TYPE_STRING) {
        const AdaptiveStringColumn& column = GetColumnString(column_ndx);
        column.find_all(tv.get_ref_column(), value);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        const ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        column.find_all(tv.get_ref_column(), value);
    }
    return move(tv);
}



TableView Table::find_all_hamming(size_t column_ndx, uint64_t value, size_t max)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    TableView tv(*this);
    column.find_all_hamming(tv.get_ref_column(), value, max);
    return move(tv);
}

ConstTableView Table::find_all_hamming(size_t column_ndx, uint64_t value, size_t max) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const Column& column = GetColumn(column_ndx);

    ConstTableView tv(*this);
    column.find_all_hamming(tv.get_ref_column(), value, max);
    return move(tv);
}

TableView Table::get_sorted_view(size_t column_ndx, bool ascending)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    TableView tv(*this);

    // Insert refs to all rows in table
    Array& refs = tv.get_ref_column();
    const size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        refs.add(i);
    }

    // Sort the refs based on the given column
    tv.sort(column_ndx, ascending);

    return move(tv);
}

ConstTableView Table::get_sorted_view(size_t column_ndx, bool ascending) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    ConstTableView tv(*this);

    // Insert refs to all rows in table
    Array& refs = tv.get_ref_column();
    const size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        refs.add(i);
    }

    // Sort the refs based on the given column
    tv.sort(column_ndx, ascending);

    return move(tv);
}

void Table::optimize()
{
    const size_t column_count = get_column_count();
    Allocator& alloc = m_columns.GetAllocator();

    for (size_t i = 0; i < column_count; ++i) {
        const ColumnType type = GetRealColumnType(i);

        if (type == COLUMN_TYPE_STRING) {
            AdaptiveStringColumn* column = &GetColumnString(i);

            size_t ref_keys;
            size_t ref_values;
            const bool res = column->AutoEnumerate(ref_keys, ref_values);
            if (!res) continue;

            // Add to spec and column refs
            m_spec_set.set_column_type(i, COLUMN_TYPE_STRING_ENUM);
            const size_t column_ndx = GetColumnRefPos(i);
            m_columns.Set(column_ndx, ref_keys);
            m_columns.Insert(column_ndx+1, ref_values);

            // There are still same number of columns, but since
            // the enum type takes up two posistions in m_columns
            // we have to move refs in all following columns
            UpdateColumnRefs(column_ndx+1, 1);

            // Replace cached column
            ColumnStringEnum* const e = new ColumnStringEnum(ref_keys, ref_values, &m_columns, column_ndx, alloc); // FIXME: We may have to use 'new (nothrow)' here. It depends on whether we choose to allow exceptions.
            m_cols.Set(i, (intptr_t)e);

            // Inherit any existing index
            if (column->HasIndex()) {
                StringIndex& ndx = column->PullIndex();
                e->ReuseIndex(ndx);
            }

            // Clean up the old column
            column->Destroy();
            delete column;
        }
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().optimize_table();
    if (err) throw_error(err);
#endif
}

void Table::UpdateColumnRefs(size_t column_ndx, int diff)
{
    for (size_t i = column_ndx; i < m_cols.Size(); ++i) {
        ColumnBase* const column = reinterpret_cast<ColumnBase*>(m_cols.Get(i));
        column->UpdateParentNdx(diff);
    }
}

void Table::UpdateFromParent() {
    // There is no top for sub-tables sharing spec
    if (m_top.IsValid()) {
        if (!m_top.UpdateFromParent()) return;
    }

    m_spec_set.update_from_parent();
    if (!m_columns.UpdateFromParent()) return;

    // Update cached columns
    const size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        ColumnBase* const column = reinterpret_cast<ColumnBase*>(m_cols.Get(i));
        column->UpdateFromParent();
    }

    // Size may have changed
    if (column_count == 0) {
        m_size = 0;
    }
    else {
        const ColumnBase* const column = reinterpret_cast<ColumnBase*>(m_cols.Get(0));
        m_size = column->Size();
    }
}


void Table::update_from_spec()
{
    TIGHTDB_ASSERT(m_columns.is_empty() && m_cols.is_empty()); // only on initial creation

    CreateColumns();
}


void Table::to_json(std::ostream& out)
{
    // Represent table as list of objects
    out << "[";

    const size_t row_count    = size();
    const size_t column_count = get_column_count();

    // We need a buffer for formatting dates (and binary to hex). Max
    // size is 21 bytes (incl quotes and zero byte) "YYYY-MM-DD HH:MM:SS"\0
    char buffer[30];

    for (size_t r = 0; r < row_count; ++r) {
        if (r) out << ",";
        out << "{";

        for (size_t i = 0; i < column_count; ++i) {
            if (i) out << ",";

            const char* const name = get_column_name(i);
            out << "\"" << name << "\":";

            const ColumnType type = get_column_type(i);
            switch (type) {
                case COLUMN_TYPE_INT:
                    out << get_int(i, r);
                    break;
                case COLUMN_TYPE_BOOL:
                    out << (get_bool(i, r) ? "true" : "false");
                    break;
                case COLUMN_TYPE_STRING:
                    out << "\"" << get_string(i, r) << "\"";
                    break;
                case COLUMN_TYPE_DATE:
                {
                    const time_t rawtime = get_date(i, r);
                    struct tm* const t = gmtime(&rawtime);
                    const size_t res = strftime(buffer, 30, "\"%Y-%m-%d %H:%M:%S\"", t);
                    if (!res) break;

                    out << buffer;
                    break;
                }
                case COLUMN_TYPE_BINARY:
                {
                    const BinaryData bin = get_binary(i, r);
                    const char* const p = (char*)bin.pointer;

                    out << "\"";
                    for (size_t i = 0; i < bin.len; ++i) {
                        sprintf(buffer, "%02x", (unsigned int)p[i]);
                        out << buffer;
                    }
                    out << "\"";
                    break;
                }
                case COLUMN_TYPE_TABLE:
                {
                    get_subtable(i, r)->to_json(out);
                    break;
                }
                case COLUMN_TYPE_MIXED:
                {
                    const ColumnType mtype = get_mixed_type(i, r);
                    if (mtype == COLUMN_TYPE_TABLE) {
                        get_subtable(i, r)->to_json(out);
                    }
                    else {
                        const Mixed m = get_mixed(i, r);
                        switch (mtype) {
                            case COLUMN_TYPE_INT:
                                out << m.get_int();
                                break;
                            case COLUMN_TYPE_BOOL:
                                out << (get_bool(i, r) ? "true" : "false");
                                break;
                            case COLUMN_TYPE_STRING:
                                out << "\"" << m.get_string() << "\"";
                                break;
                            case COLUMN_TYPE_DATE:
                            {
                                const time_t rawtime = m.get_date();
                                struct tm* const t = gmtime(&rawtime);
                                const size_t res = strftime(buffer, 30, "\"%Y-%m-%d %H:%M:%S\"", t);
                                if (!res) break;

                                out << buffer;
                                break;
                            }
                            case COLUMN_TYPE_BINARY:
                            {
                                const BinaryData bin = m.get_binary();
                                const char* const p = (char*)bin.pointer;

                                out << "\"";
                                for (size_t i = 0; i < bin.len; ++i) {
                                    sprintf(buffer, "%02x", (unsigned int)p[i]);
                                    out << buffer;
                                }
                                out << "\"";
                                break;
                            }
                            default:
                                TIGHTDB_ASSERT(false);
                        }

                    }
                    break;
                }

                default:
                    TIGHTDB_ASSERT(false);
            }
        }

        out << "}";
    }

    out << "]";
}

static size_t chars_in_int(int64_t v)
{
    size_t count = 0;
    while (v /= 10)
        ++count;
    return count+1;
}

void Table::to_string(std::ostream& out, size_t limit) const
{
    const size_t column_count = get_column_count();
    const size_t row_count = size();

    // Print header
    std::vector<size_t> widths;
    const size_t row_ndx_width = chars_in_int(row_count);
    widths.push_back(row_ndx_width);
    for (size_t i = 0; i < row_ndx_width; ++i)
        out << " ";
    for (size_t i = 0; i < column_count; ++i) {
        const char* const name = get_column_name(i);
        const ColumnType type = get_column_type(i);
        size_t width = strlen(name);
        switch (type) {
        case COLUMN_TYPE_BOOL:
            if (width < 5) width = 5;
            break;
        case COLUMN_TYPE_INT:
            {
                const size_t max = chars_in_int(maximum(i));
                if (width < max) width = max;
            }
            break;
        case COLUMN_TYPE_STRING:
        case COLUMN_TYPE_MIXED:
            // TODO: Calculate precise width needed
            if (width < 10) width = 10;
            break;
        case COLUMN_TYPE_DATE:
            if (width < 21) width = 21;
            break;
        case COLUMN_TYPE_TABLE:
            if (width < 3) width = 3;
            break;
        default:
            break;
        }
        widths.push_back(width);
        out << "  "; // spacing

        out.width(width);
        out << name;
    }
    out << "\n";

    // We need a buffer for formatting dates (and binary to hex). Max
    // size is 21 bytes (incl quotes and zero byte) "YYYY-MM-DD HH:MM:SS"\0
    char buffer[30];

    // Set limit=-1 to print all rows, otherwise only print to limit
    const size_t out_count = (limit == (size_t)-1) ? row_count
                                                   : (row_count < limit) ? row_count : limit;

    // Print rows
    for (size_t i = 0; i < out_count; ++i) {
        out.width(row_ndx_width);
        out << i;

        for (size_t n = 0; n < column_count; ++n) {
            out << "  "; // spacing
            out.width(widths[n+1]);

            const ColumnType type = get_column_type(n);
            switch (type) {
                case COLUMN_TYPE_BOOL:
                {
                    const char* const s = get_bool(n, i) ? "true" : "false";
                    out << s;
                }
                    break;
                case COLUMN_TYPE_INT:
                    out << get_int(n, i);
                    break;
                case COLUMN_TYPE_STRING:
                    out.setf(std::ostream::left, std::ostream::adjustfield);
                    out << get_string(n, i);
                    out.unsetf(std::ostream::adjustfield);
                    break;
                case COLUMN_TYPE_DATE:
                {
                    const time_t rawtime = get_date(n, i);
                    struct tm* const t = gmtime(&rawtime);
                    const size_t res = strftime(buffer, 30, "\"%Y-%m-%d %H:%M:%S\"", t);
                    if (!res) break;

                    out << buffer;
                    break;
                }
                case COLUMN_TYPE_TABLE:
                    out.width(widths[n+1]-2); // adjust for first char only
                    out << "[" << get_subtable_size(n, i) << "]";
                    break;
                case COLUMN_TYPE_MIXED:
                {
                    const ColumnType mtype = get_mixed_type(n, i);
                    if (mtype == COLUMN_TYPE_TABLE) {
                        out.width(widths[n+1]-2); // adjust for first char only
                        out << "[" << get_subtable_size(n, i) << "]";
                    }
                    else {
                        const Mixed m = get_mixed(n, i);
                        switch (mtype) {
                            case COLUMN_TYPE_INT:
                                out << m.get_int();
                                break;
                            case COLUMN_TYPE_BOOL:
                            {
                                const char* const s = m.get_bool() ? "true" : "false";
                                out << s;
                                break;
                            }
                            case COLUMN_TYPE_STRING:
                                out << m.get_string();
                                break;
                            case COLUMN_TYPE_DATE:
                            {
                                const time_t rawtime = m.get_date();
                                struct tm* const t = gmtime(&rawtime);
                                const size_t res = strftime(buffer, 30, "\"%Y-%m-%d %H:%M:%S\"", t);
                                if (!res) break;

                                out << buffer;
                                break;
                            }
                            case COLUMN_TYPE_BINARY:
                            {
                                const BinaryData bin = m.get_binary();
                                out << bin.len << "bytes";
                                break;
                            }
                            default:
                                TIGHTDB_ASSERT(false);
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }
        out << "\n";
    }

    if (out_count < row_count) {
        const size_t rest = row_count - out_count;
        out << "... and " << rest << " more rows (total " << row_count << ")";
    }
}

bool Table::compare_rows(const Table& t) const
{
    // A wrapper for an empty subtable with shared spec may be created
    // with m_data == 0. In this case there are no Column wrappers, so
    // the standard comparison scheme becomes impossible.
    if (m_size == 0) return t.m_size == 0;

    const size_t n = get_column_count();
    TIGHTDB_ASSERT(t.get_column_count() == n);
    for (size_t i=0; i<n; ++i) {
        const ColumnType type = GetRealColumnType(i);
        TIGHTDB_ASSERT(t.GetRealColumnType(i) == type);

        switch (type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOL:
        case COLUMN_TYPE_DATE:
            {
                const Column& c1 = GetColumn(i);
                const Column& c2 = t.GetColumn(i);
                if (!c1.Compare(c2)) return false;
            }
            break;
        case COLUMN_TYPE_STRING:
            {
                const AdaptiveStringColumn& c1 = GetColumnString(i);
                const AdaptiveStringColumn& c2 = t.GetColumnString(i);
                if (!c1.Compare(c2)) return false;
            }
            break;
        case COLUMN_TYPE_BINARY:
            {
                const ColumnBinary& c1 = GetColumnBinary(i);
                const ColumnBinary& c2 = t.GetColumnBinary(i);
                if (!c1.Compare(c2)) return false;
            }
            break;
        case COLUMN_TYPE_TABLE:
            {
                const ColumnTable& c1 = GetColumnTable(i);
                const ColumnTable& c2 = t.GetColumnTable(i);
                if (!c1.Compare(c2)) return false;
            }
            break;
        case COLUMN_TYPE_MIXED:
            {
                const ColumnMixed& c1 = GetColumnMixed(i);
                const ColumnMixed& c2 = t.GetColumnMixed(i);
                if (!c1.Compare(c2)) return false;
            }
            break;
        case COLUMN_TYPE_STRING_ENUM:
            {
                const ColumnStringEnum& c1 = GetColumnStringEnum(i);
                const ColumnStringEnum& c2 = t.GetColumnStringEnum(i);
                if (!c1.Compare(c2)) return false;
            }
            break;

        default:
            TIGHTDB_ASSERT(false);
        }
    }
    return true;
}


#ifdef TIGHTDB_DEBUG

void Table::Verify() const
{
    if (m_top.IsValid()) m_top.Verify();
    m_columns.Verify();
    if (m_columns.IsValid()) {
        const size_t column_count = get_column_count();
        TIGHTDB_ASSERT(column_count == m_cols.Size());

        for (size_t i = 0; i < column_count; ++i) {
            const ColumnType type = GetRealColumnType(i);
            switch (type) {
            case COLUMN_TYPE_INT:
            case COLUMN_TYPE_BOOL:
            case COLUMN_TYPE_DATE:
                {
                    const Column& column = GetColumn(i);
                    TIGHTDB_ASSERT(column.Size() == m_size);
                    column.Verify();
                }
                break;
            case COLUMN_TYPE_STRING:
                {
                    const AdaptiveStringColumn& column = GetColumnString(i);
                    TIGHTDB_ASSERT(column.Size() == m_size);
                    column.Verify();
                }
                break;
            case COLUMN_TYPE_STRING_ENUM:
                {
                    const ColumnStringEnum& column = GetColumnStringEnum(i);
                    TIGHTDB_ASSERT(column.Size() == m_size);
                    column.Verify();
                }
                break;
            case COLUMN_TYPE_BINARY:
                {
                    const ColumnBinary& column = GetColumnBinary(i);
                    TIGHTDB_ASSERT(column.Size() == m_size);
                    column.Verify();
                }
                break;
            case COLUMN_TYPE_TABLE:
                {
                    const ColumnTable& column = GetColumnTable(i);
                    TIGHTDB_ASSERT(column.Size() == m_size);
                    column.Verify();
                }
                break;
            case COLUMN_TYPE_MIXED:
                {
                    const ColumnMixed& column = GetColumnMixed(i);
                    TIGHTDB_ASSERT(column.Size() == m_size);
                    column.Verify();
                }
                break;
            default:
                TIGHTDB_ASSERT(false);
            }
        }
    }

    m_spec_set.Verify();

    Allocator& alloc = m_columns.GetAllocator();
    alloc.Verify();
}

void Table::to_dot(std::ostream& out, const char* title) const
{
    if (m_top.IsValid()) {
        out << "subgraph cluster_topleveltable" << m_top.GetRef() << " {" << endl;
        out << " label = \"TopLevelTable";
        if (title) out << "\\n'" << title << "'";
        out << "\";" << endl;
        m_top.ToDot(out, "table_top");
        const Spec& specset = get_spec();
        specset.to_dot(out);
    }
    else {
        out << "subgraph cluster_table_"  << m_columns.GetRef() <<  " {" << endl;
        out << " label = \"Table";
        if (title) out << " " << title;
        out << "\";" << endl;
    }

    ToDotInternal(out);

    out << "}" << endl;
}

void Table::ToDotInternal(std::ostream& out) const
{
    m_columns.ToDot(out, "columns");

    // Columns
    const size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        const ColumnBase& column = GetColumnBase(i);
        const char* const name = get_column_name(i);
        column.ToDot(out, name);
    }
}

void Table::print() const
{
    // Table header
    cout << "Table: len(" << m_size << ")\n    ";
    const size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        const char* name = m_spec_set.get_column_name(i);
        cout << left << setw(10) << name << right << " ";
    }

    // Types
    cout << "\n    ";
    for (size_t i = 0; i < column_count; ++i) {
        const ColumnType type = GetRealColumnType(i);
        switch (type) {
        case COLUMN_TYPE_INT:
            cout << "Int        "; break;
        case COLUMN_TYPE_BOOL:
            cout << "Bool       "; break;
        case COLUMN_TYPE_STRING:
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
            const ColumnType type = GetRealColumnType(n);
            switch (type) {
            case COLUMN_TYPE_INT:
                {
                    const Column& column = GetColumn(n);
                    cout << setw(10) << column.Get(i) << " ";
                }
                break;
            case COLUMN_TYPE_BOOL:
                {
                    const Column& column = GetColumn(n);
                    cout << (column.Get(i) == 0 ? "     false " : "      true ");
                }
                break;
            case COLUMN_TYPE_STRING:
                {
                    const AdaptiveStringColumn& column = GetColumnString(n);
                    cout << setw(10) << column.Get(i) << " ";
                }
                break;
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
    m_top.Stats(stats);

    return stats;
}

#endif // TIGHTDB_DEBUG


} // namespace tightdb
