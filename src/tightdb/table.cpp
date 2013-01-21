#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>


#include <tightdb/config.h>

#ifndef TIGHTDB_HAVE_RTTI
#  ifdef __GNUC__
#    warning RTTI appears to be disabled
#  else
#    pragma message("RTTI appears to be disabled")
#  endif
#endif

#ifndef TIGHTDB_HAVE_EXCEPTIONS
#  ifdef __GNUC__
#    warning Exceptions appear to be disabled
#  else
#    pragma message("Exceptions appear to be disabled")
#  endif
#endif

#include <tightdb/table.hpp>
#include <tightdb/index.hpp>
#include <tightdb/alloc_slab.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_float.hpp>
#include <tightdb/column_double.hpp>
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
        case COLUMN_TYPE_FLOAT:
            {
                ColumnFloat* c = new ColumnFloat(alloc);
                m_columns.add(c->GetRef());
                c->SetParent(&m_columns, ref_pos);
                new_col = c;
            }
            break;
        case COLUMN_TYPE_DOUBLE:
            {
                ColumnDouble* c = new ColumnDouble(alloc);
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
        c->invalidate_subtables_virtual();
    }

    ClearCachedColumns();
}


void Table::InstantiateBeforeChange()
{
    // Empty (zero-ref'ed) tables need to be instantiated before first modification
    if (!m_columns.IsValid()) 
        CreateColumns();
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
        case COLUMN_TYPE_FLOAT:
            {
                ColumnFloat* c = new ColumnFloat(ref, &m_columns, ndx_in_parent, alloc);
                colsize = c->Size();
                new_col = c;
            }
            break;
        case COLUMN_TYPE_DOUBLE:
            {
                ColumnDouble* c = new ColumnDouble(ref, &m_columns, ndx_in_parent, alloc);
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
                ++ndx_in_parent; // advance one matchcount pos to account for keys/values pair
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

            ++ndx_in_parent; // advance one matchcount pos to account for index
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
#ifdef TIGHTDB_HAVE_RTTI
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(parent));
#endif
        static_cast<Parent*>(parent)->child_destroyed(m_columns.GetParentNdx());
        ClearCachedColumns();
        return;
    }

    // This is a table with an independent spec.
    if (ArrayParent* parent = m_top.GetParent()) {
        // This is a table whose lifetime is managed by reference
        // counting, so we must let our parent know about our demise.
        TIGHTDB_ASSERT(m_ref_count == 0);
#ifdef TIGHTDB_HAVE_RTTI
        TIGHTDB_ASSERT(dynamic_cast<Parent*>(parent));
#endif
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
    // invalidate, because they would have kept their parent alive.
    if (0 < m_ref_count) invalidate();
    else ClearCachedColumns();
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

        ++pos;
        const ColumnType type = (ColumnType)m_spec_set.get_type_attr(i);
        if (type >= COLUMN_ATTR_INDEXED)
            continue; // ignore attributes
        if (type == COLUMN_TYPE_STRING_ENUM)
            ++pos; // string enums take up two places in m_columns

        ++current_column;
    }

    TIGHTDB_ASSERT(false);
    return (size_t)-1;
}

size_t Table::add_subcolumn(const vector<size_t>& column_path, ColumnType type, const char* name)
{
    TIGHTDB_ASSERT(!column_path.empty());

    // Update existing tables
    do_add_subcolumn(column_path, 0, type);

    // Update spec
    const size_t column_ndx = m_spec_set.add_subcolumn(column_path, type, name);

    //TODO: go back over any live instances and set subspec ref

#ifdef TIGHTDB_ENABLE_REPLICATION
    // TODO:
    //error_code err = get_local_transact_log().add_column(type, name);
    //if (err) throw_error(err);
#endif

    return column_ndx;
}

void Table::do_add_subcolumn(const vector<size_t>& column_path, size_t pos, ColumnType type)
{
    const size_t column_ndx = column_path[pos];
    const bool   is_last    = (pos == column_path.size()-1);

#ifdef TIGHTDB_DEBUG
    const ColumnType stype = GetRealColumnType(column_ndx);
    TIGHTDB_ASSERT(stype == COLUMN_TYPE_TABLE);
#endif // TIGHTDB_DEBUG

    const size_t row_count = size();
    ColumnTable& subtables = GetColumnTable(column_ndx);

    for (size_t i = 0; i < row_count; ++i) {
        if (!subtables.has_subtable(i)) continue;

        TableRef subtable = subtables.get_subtable_ptr(i)->get_table_ref();
        if (is_last)
            subtable->do_add_column(type);
        else
            subtable->do_add_subcolumn(column_path, pos+1, type);
    }
}

size_t Table::add_column(ColumnType type, const char* name)
{
    // Create column and add cached instance
    const size_t column_ndx = do_add_column(type);

    // Update spec
    m_spec_set.add_column(type, name);

    // Since subspec was not set at creation time we have to set it now
    if (type == COLUMN_TYPE_TABLE) {
        const size_t subspec_ref = m_spec_set.get_subspec_ref(m_spec_set.get_num_subspecs()-1);
        ColumnTable& c = GetColumnTable(column_ndx);
        c.set_specref(subspec_ref);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().add_column(type, name);
    if (err) throw_error(err);
#endif

    return column_ndx;
}

size_t Table::do_add_column(ColumnType type)
{
    const size_t count      = size();
    const size_t column_ndx = m_cols.Size();

    ColumnBase* new_col = NULL;
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
            c->fill(count);
        }
        break;
    case COLUMN_TYPE_FLOAT:
        {
            ColumnFloat* c = new ColumnFloat(alloc);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
            c->fill(count);
        }
        break;
    case COLUMN_TYPE_DOUBLE:
        {
            ColumnDouble* c = new ColumnDouble(alloc);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
            c->fill(count);
        }
        break;
    case COLUMN_TYPE_STRING:
        {
            AdaptiveStringColumn* c = new AdaptiveStringColumn(alloc);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
            c->fill(count);
        }
        break;
    case COLUMN_TYPE_BINARY:
        {
            ColumnBinary* c = new ColumnBinary(alloc);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
            c->fill(count);
        }
        break;

    case COLUMN_TYPE_TABLE:
        {
            ColumnTable* c = new ColumnTable(alloc, this, column_ndx, -1); // subspec ref will be filled in later
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
            c->fill(count);
        }
        break;

    case COLUMN_TYPE_MIXED:
        {
            ColumnMixed* c = new ColumnMixed(alloc, this, column_ndx);
            m_columns.add(c->GetRef());
            c->SetParent(&m_columns, m_columns.Size()-1);
            new_col = c;
            c->fill(count);
        }
        break;
    default:
        TIGHTDB_ASSERT(false);
    }

    m_cols.add(reinterpret_cast<intptr_t>(new_col)); // FIXME: intptr_t is not guaranteed to exists, even in C++11

    return column_ndx;
}

void Table::remove_column(size_t column_ndx)
{
    // Remove from data layer and free all cached instances
    do_remove_column(column_ndx);

    // Update Spec
    m_spec_set.remove_column(column_ndx);
}

void Table::remove_column(const vector<size_t>& column_path)
{
    // Remove from data layer and free all cached instances
    do_remove_column(column_path, 0);

    // Update Spec
    m_spec_set.remove_column(column_path);
}

void Table::do_remove_column(size_t column_ndx)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    // Delete the cached column
    ColumnBase* const column = reinterpret_cast<ColumnBase*>(m_cols.Get(column_ndx));
    const bool has_index = column->HasIndex();
    column->invalidate_subtables_virtual();
    column->Destroy();
    delete column;
    m_cols.Delete(column_ndx);

    // Remove from column list
    const size_t column_pos = GetColumnRefPos(column_ndx);
    m_columns.Delete(column_pos);
    int deleted = 1;

    // If the column had an index we have to remove that as well
    if (has_index) {
        m_columns.Delete(column_pos);
        ++deleted;
    }

    // Update parent refs in following columns
    UpdateColumnRefs(column_ndx, -deleted);

    // If there are no columns left, mark the table as empty
    if (get_column_count() == 1) // not deleted in spec yet
        m_size = 0;
}

void Table::do_remove_column(const vector<size_t>& column_path, size_t pos)
{
    const size_t sub_count  = column_path.size();
    const size_t column_ndx = column_path[pos];

    if (pos == sub_count-1) {
        do_remove_column(column_ndx);
    }
    else {
#ifdef TIGHTDB_DEBUG
        const ColumnType type = GetRealColumnType(column_ndx);
        TIGHTDB_ASSERT(type == COLUMN_TYPE_TABLE);
#endif // TIGHTDB_DEBUG

        const size_t row_count = size();
        ColumnTable& subtables = GetColumnTable(column_ndx);

        for (size_t i = 0; i < row_count; ++i) {
            if (!subtables.has_subtable(i)) continue;

            TableRef subtable = subtables.get_subtable_ptr(i)->get_table_ref();
            subtable->do_remove_column(column_path, pos+1);
        }
    }
}

void Table::rename_column(size_t column_ndx, const char* name) {
    m_spec_set.rename_column(column_ndx, name);
}

void Table::rename_column(const vector<size_t>& column_path, const char* name) {
    m_spec_set.rename_column(column_path, name);
}


bool Table::has_index(size_t column_ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    const ColumnBase& col = GetColumnBase(column_ndx);
    return col.HasIndex();
}

void Table::set_index(size_t column_ndx, bool update_spec)
{
    TIGHTDB_ASSERT(!has_shared_spec());
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


void Table::validate_column_type(const ColumnBase& column, ColumnType type, size_t ndx) const
{
    if (type == COLUMN_TYPE_INT || type == COLUMN_TYPE_DATE || type == COLUMN_TYPE_BOOL)
        TIGHTDB_ASSERT(column.IsIntColumn());
    else {
        ColumnType this_type = GetRealColumnType(ndx);
        TIGHTDB_ASSERT(type == this_type); 
    }
}

template <class C, ColumnType type>
C& Table::GetColumn(size_t ndx)
{
    ColumnBase& column = GetColumnBase(ndx);
#ifdef TIGHTDB_DEBUG
    validate_column_type(column, type, ndx);
#endif
    return static_cast<C&>(column);
}

template <class C, ColumnType type>
const C& Table::GetColumn(size_t ndx) const
{
    const ColumnBase& column = GetColumnBase(ndx);
#ifdef TIGHTDB_DEBUG
    validate_column_type(column, type, ndx);
#endif
    return static_cast<const C&>(column);
}

// TODO: get rid of the COLUMN_TYPE tempalte parameter

Column& Table::GetColumn(size_t ndx)             { return GetColumn<Column, COLUMN_TYPE_INT>(ndx); }
const Column& Table::GetColumn(size_t ndx) const { return GetColumn<Column, COLUMN_TYPE_INT>(ndx); }

AdaptiveStringColumn& Table::GetColumnString(size_t ndx)             { return GetColumn<AdaptiveStringColumn, COLUMN_TYPE_STRING>(ndx); }
const AdaptiveStringColumn& Table::GetColumnString(size_t ndx) const { return GetColumn<AdaptiveStringColumn, COLUMN_TYPE_STRING>(ndx); }

ColumnStringEnum& Table::GetColumnStringEnum(size_t ndx)             { return GetColumn<ColumnStringEnum, COLUMN_TYPE_STRING_ENUM>(ndx); }
const ColumnStringEnum& Table::GetColumnStringEnum(size_t ndx) const { return GetColumn<ColumnStringEnum, COLUMN_TYPE_STRING_ENUM>(ndx); }

ColumnFloat& Table::GetColumnFloat(size_t ndx)               { return GetColumn<ColumnFloat, COLUMN_TYPE_FLOAT>(ndx); }
const ColumnFloat& Table::GetColumnFloat(size_t ndx) const   { return GetColumn<ColumnFloat, COLUMN_TYPE_FLOAT>(ndx); }

ColumnDouble& Table::GetColumnDouble(size_t ndx)             { return GetColumn<ColumnDouble, COLUMN_TYPE_DOUBLE>(ndx); }
const ColumnDouble& Table::GetColumnDouble(size_t ndx) const { return GetColumn<ColumnDouble, COLUMN_TYPE_DOUBLE>(ndx); }

ColumnBinary& Table::GetColumnBinary(size_t ndx)             { return GetColumn<ColumnBinary, COLUMN_TYPE_BINARY>(ndx); }
const ColumnBinary& Table::GetColumnBinary(size_t ndx) const { return GetColumn<ColumnBinary, COLUMN_TYPE_BINARY>(ndx); }

ColumnTable &Table::GetColumnTable(size_t ndx)               { return GetColumn<ColumnTable, COLUMN_TYPE_TABLE>(ndx); }
const ColumnTable &Table::GetColumnTable(size_t ndx) const   { return GetColumn<ColumnTable, COLUMN_TYPE_TABLE>(ndx); }

ColumnMixed& Table::GetColumnMixed(size_t ndx)               { return GetColumn<ColumnMixed, COLUMN_TYPE_MIXED>(ndx); }
const ColumnMixed& Table::GetColumnMixed(size_t ndx) const   { return GetColumn<ColumnMixed, COLUMN_TYPE_MIXED>(ndx); }



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
        subtables.ClearTable(row_idx);
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


float Table::get_float(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnFloat& column = GetColumnFloat(column_ndx);
    return column.Get(ndx);
}

void Table::set_float(size_t column_ndx, size_t ndx, float value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    ColumnFloat& column = GetColumnFloat(column_ndx);
    column.Set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}

void Table::insert_float(size_t column_ndx, size_t ndx, float value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnFloat& column = GetColumnFloat(column_ndx);
    column.Insert(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().insert_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}


double Table::get_double(size_t column_ndx, size_t ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    const ColumnDouble& column = GetColumnDouble(column_ndx);
    return column.Get(ndx);
}

void Table::set_double(size_t column_ndx, size_t ndx, double value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx < m_size);

    ColumnDouble& column = GetColumnDouble(column_ndx);
    column.Set(ndx, value);

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().set_value(column_ndx, ndx, value);
    if (err) throw_error(err);
#endif
}

void Table::insert_double(size_t column_ndx, size_t ndx, double value)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(ndx <= m_size);

    ColumnDouble& column = GetColumnDouble(column_ndx);
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

size_t Table::get_string_length(size_t column_ndx, size_t ndx) const
{
    // TODO: Implement faster get_string_length()
    return strlen( get_string(column_ndx, ndx) );
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
        case COLUMN_TYPE_FLOAT:
            return Mixed(column.get_float(ndx));
        case COLUMN_TYPE_DOUBLE:
            return Mixed(column.get_double(ndx));
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
        case COLUMN_TYPE_FLOAT:
            column.set_float(ndx, value.get_float());
            break;
        case COLUMN_TYPE_DOUBLE:
            column.set_double(ndx, value.get_double());
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
        case COLUMN_TYPE_FLOAT:
            column.insert_float(ndx, value.get_float());
            break;
        case COLUMN_TYPE_DOUBLE:
            column.insert_double(ndx, value.get_double());
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
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
    error_code err = get_local_transact_log().row_insert_complete();
    if (err) throw_error(err);
#endif
}


// count ----------------------------------------------

template <class T>
size_t Table::count(size_t column_ndx, T target) const
{
    // asserts are done in GetColumn
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    const ColType& column = GetColumn<ColType, ColumnTypeTraits<T>::id>(column_ndx);
    return column.count(target);
}
size_t Table::count_int(size_t column_ndx, int64_t target) const
{
    return count<int64_t>(column_ndx, target);
}
size_t Table::count_float(size_t column_ndx, float target) const
{
    return count<float>(column_ndx, target);
}
size_t Table::count_double(size_t column_ndx, double target) const
{
    return count<double>(column_ndx, target);
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

// sum ----------------------------------------------

template <typename T, typename R>
R Table::sum(size_t column_ndx) const
{
    // asserts are done in GetColumn
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    const ColType& column = GetColumn<ColType, ColumnTypeTraits<T>::id>(column_ndx);
    return column.sum();
}
int64_t Table::sum(size_t column_ndx) const
{
    return sum<int64_t, int64_t>(column_ndx);
}
float Table::sum_float(size_t column_ndx) const
{
    return sum<float, float>(column_ndx);
}
double Table::sum_double(size_t column_ndx) const
{
    return sum<double, double>(column_ndx);
}

// average ----------------------------------------------

template <typename T>
double Table::average(size_t column_ndx) const
{
    // asserts are done in GetColumn
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    const ColType& column = GetColumn<ColType, ColumnTypeTraits<T>::id>(column_ndx);
    return column.average();
}
double Table::average(size_t column_ndx) const
{
    return average<int64_t>(column_ndx);
}
double Table::average_float(size_t column_ndx) const
{
    return average<float>(column_ndx);
}
double Table::average_double(size_t column_ndx) const
{
    return average<double>(column_ndx);
}

// minimum ----------------------------------------------

template <typename T>
T Table::minimum(size_t column_ndx) const
{
    // asserts are done in GetColumnT
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    const ColType& column = GetColumn<ColType, ColumnTypeTraits<T>::id>(column_ndx);
    return column.minimum();
}

#define USE_COLUMN_AGGREGATE 1

int64_t Table::minimum(size_t column_ndx) const
{
#if USE_COLUMN_AGGREGATE
    return minimum<int64_t>(column_ndx);
#else
    if (is_empty()) 
        return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        const int64_t v = get_int(column_ndx, i);
        if (v < mv) {
            mv = v;
        }
    }
    return mv;
#endif
}


float Table::minimum_float(size_t column_ndx) const
{
    return minimum<float>(column_ndx);
}
double Table::minimum_double(size_t column_ndx) const
{
    return minimum<double>(column_ndx);
}

// maximum ----------------------------------------------

template <typename T>
T Table::maximum(size_t column_ndx) const
{
    // asserts are done in GetColumnT
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    const ColType& column = GetColumn<ColType, ColumnTypeTraits<T>::id>(column_ndx);
    return column.maximum();
}

int64_t Table::maximum(size_t column_ndx) const
{
#if USE_COLUMN_AGGREGATE
    return maximum<int64_t>(column_ndx);
#else
    if (is_empty()) 
        return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < size(); ++i) {
        const int64_t v = get_int(column_ndx, i);
        if (v > mv) {
            mv = v;
        }
    }
    return mv;
#endif
}
float Table::maximum_float(size_t column_ndx) const
{
    return maximum<float>(column_ndx);
}
double Table::maximum_double(size_t column_ndx) const
{
    return maximum<double>(column_ndx);
}



size_t Table::lookup(const char* value) const
{
    // First time we do a lookup we check if we can cache the index
    if (!m_lookup_index) {
        if (get_column_count() < 1)
            return not_found; // no column to lookup in

        const ColumnType type = GetRealColumnType(0);

        if (type == COLUMN_TYPE_STRING) {
            const AdaptiveStringColumn& column = GetColumnString(0);
            if (!column.HasIndex())
                return column.find_first(value);
            else {
                m_lookup_index = &column.GetIndex();
            }
        }
        else if (type == COLUMN_TYPE_STRING_ENUM) {
            const ColumnStringEnum& column = GetColumnStringEnum(0);
            if (!column.HasIndex())
                return column.find_first(value);
            else {
                m_lookup_index = &column.GetIndex();
            }
        }
        else return not_found; // invalid column type
    }

    // Do lookup directly on cached index
    return m_lookup_index->find_first(value);
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

size_t Table::find_first_float(size_t column_ndx, float value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_FLOAT);
    const ColumnFloat& column = GetColumnFloat(column_ndx);

    return column.find_first(value);
}

size_t Table::find_first_double(size_t column_ndx, double value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(GetRealColumnType(column_ndx) == COLUMN_TYPE_DOUBLE);
    const ColumnDouble& column = GetColumnDouble(column_ndx);

    return column.find_first(value);
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

size_t Table::find_first_binary(size_t column_ndx, const char* value, size_t len) const
{
    // FIXME: Implement this!
    static_cast<void>(column_ndx);
    static_cast<void>(value);
    static_cast<void>(len);
    throw_error(ERROR_NOT_IMPLEMENTED);
    return 0;
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


TableView Table::find_all_float(size_t column_ndx, float value)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const ColumnFloat& column = GetColumnFloat(column_ndx);

    TableView tv(*this);
    column.find_all(tv.get_ref_column(), value);
    return move(tv);
}

ConstTableView Table::find_all_float(size_t column_ndx, float value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const ColumnFloat& column = GetColumnFloat(column_ndx);

    ConstTableView tv(*this);
    column.find_all(tv.get_ref_column(), value);
    return move(tv);
}

TableView Table::find_all_double(size_t column_ndx, double value)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const ColumnDouble& column = GetColumnDouble(column_ndx);

    TableView tv(*this);
    column.find_all(tv.get_ref_column(), value);
    return move(tv);
}

ConstTableView Table::find_all_double(size_t column_ndx, double value) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());

    const ColumnDouble& column = GetColumnDouble(column_ndx);

    ConstTableView tv(*this);
    column.find_all(tv.get_ref_column(), value);
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

TableView Table::find_all_binary(size_t column_ndx, const char* value, size_t len)
{
    // FIXME: Implement this!
    static_cast<void>(column_ndx);
    static_cast<void>(value);
    static_cast<void>(len);
    throw_error(ERROR_NOT_IMPLEMENTED);
    return TableView(*this);
}

ConstTableView Table::find_all_binary(size_t column_ndx, const char* value, size_t len) const
{
    // FIXME: Implement this!
    static_cast<void>(column_ndx);
    static_cast<void>(value);
    static_cast<void>(len);
    throw_error(ERROR_NOT_IMPLEMENTED);
    return ConstTableView(*this);
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


TableView Table::distinct(size_t column_ndx)
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(has_index(column_ndx));

    TableView tv(*this);
    Array& refs = tv.get_ref_column();

    const ColumnType type = GetRealColumnType(column_ndx);
    if (type == COLUMN_TYPE_STRING) {
        const AdaptiveStringColumn& column = GetColumnString(column_ndx);
        const StringIndex& ndx = column.GetIndex();
        ndx.distinct(refs);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        const ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        const StringIndex& ndx = column.GetIndex();
        ndx.distinct(refs);
    }
    return move(tv);
}

ConstTableView Table::distinct(size_t column_ndx) const
{
    TIGHTDB_ASSERT(column_ndx < m_columns.Size());
    TIGHTDB_ASSERT(has_index(column_ndx));

    ConstTableView tv(*this);
    Array& refs = tv.get_ref_column();

    const ColumnType type = GetRealColumnType(column_ndx);
    if (type == COLUMN_TYPE_STRING) {
        const AdaptiveStringColumn& column = GetColumnString(column_ndx);
        const StringIndex& ndx = column.GetIndex();
        ndx.distinct(refs);
    }
    else {
        TIGHTDB_ASSERT(type == COLUMN_TYPE_STRING_ENUM);
        const ColumnStringEnum& column = GetColumnStringEnum(column_ndx);
        const StringIndex& ndx = column.GetIndex();
        ndx.distinct(refs);
    }
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
            UpdateColumnRefs(i+1, 1);

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



static inline void out_date(std::ostream& out, const time_t rawtime) 
{
    struct tm* const t = gmtime(&rawtime);
    if (t) {
        // We need a buffer for formatting dates (and binary to hex). Max
        // size is 20 bytes (incl zero byte) "YYYY-MM-DD HH:MM:SS"\0
        char buffer[30];
        const size_t res = strftime(buffer, 30, "%Y-%m-%d %H:%M:%S", t);
        if (res)
            out << buffer;
    }
}

static inline void out_binary(std::ostream& out, const BinaryData bin) 
{
    const char* const p = (char*)bin.pointer;
    for (size_t i = 0; i < bin.len; ++i)
        out << setw(2) << setfill('0') << hex << (unsigned int)p[i] << dec;
}

void Table::to_json(std::ostream& out)
{
    // Represent table as list of objects
    out << "[";

    const size_t row_count = size();
    for (size_t r = 0; r < row_count; ++r) {
        if (r > 0) 
            out << ",";
        to_json_row(r, out);
    }

    out << "]";
}

void Table::to_json_row(size_t row_ndx, std::ostream& out)
{
    out << "{";
    const size_t column_count = get_column_count();
    for (size_t i = 0; i < column_count; ++i) {
        if (i > 0) 
            out << ",";

        const char* const name = get_column_name(i);
        out << "\"" << name << "\":";

        const ColumnType type = get_column_type(i);
        switch (type) {
            case COLUMN_TYPE_INT:
                out << get_int(i, row_ndx);
                break;
            case COLUMN_TYPE_BOOL:
                out << (get_bool(i, row_ndx) ? "true" : "false");
                break;
            case COLUMN_TYPE_STRING:
                out << "\"" << get_string(i, row_ndx) << "\"";
                break;
            case COLUMN_TYPE_DATE:
                out << "\""; out_date(out, get_date(i, row_ndx)); out << "\"";
                break;
            case COLUMN_TYPE_BINARY:
                out << "\""; out_binary(out, get_binary(i, row_ndx)); out << "\""; 
                break;
            case COLUMN_TYPE_TABLE:
                get_subtable(i, row_ndx)->to_json(out);
                break;
            case COLUMN_TYPE_MIXED:
            {
                const ColumnType mtype = get_mixed_type(i, row_ndx);
                if (mtype == COLUMN_TYPE_TABLE) {
                    get_subtable(i, row_ndx)->to_json(out);
                }
                else {
                    const Mixed m = get_mixed(i, row_ndx);
                    switch (mtype) {
                        case COLUMN_TYPE_INT:
                            out << m.get_int();
                            break;
                        case COLUMN_TYPE_BOOL:
                            out << (m.get_bool() ? "true" : "false");
                            break;
                        case COLUMN_TYPE_STRING:
                            out << "\"" << m.get_string() << "\"";
                            break;
                        case COLUMN_TYPE_DATE:
                            out << "\""; out_date(out, m.get_date()); out << "\"";
                            break;
                        case COLUMN_TYPE_BINARY:
                            out << "\""; out_binary(out, m.get_binary()); out << "\""; 
                            break;
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


static size_t chars_in_int(int64_t v)
{
    size_t count = 0;
    while (v /= 10)
        ++count;
    return count+1;
}

void Table::to_string(std::ostream& out, size_t limit) const
{
    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    to_string_header(out, widths);

    // Set limit=-1 to print all rows, otherwise only print to limit
    const size_t row_count = size();
    const size_t out_count = (limit == (size_t)-1) ? row_count
                                                   : (row_count < limit) ? row_count : limit;

    // Print rows
    for (size_t i = 0; i < out_count; ++i) {
        to_string_row(i, out, widths);
    }

    if (out_count < row_count) {
        const size_t rest = row_count - out_count;
        out << "... and " << rest << " more rows (total " << row_count << ")";
    }
}

void Table::row_to_string(size_t row_ndx, std::ostream& out) const
{
    TIGHTDB_ASSERT(row_ndx < size());

    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    to_string_header(out, widths);

    // Print row contents
    to_string_row(row_ndx, out, widths);
}

void Table::to_string_header(std::ostream& out, std::vector<size_t>& widths) const
{
    const size_t column_count = get_column_count();
    const size_t row_count = size();
    const size_t row_ndx_width = chars_in_int(row_count);
    widths.push_back(row_ndx_width);

    // Empty space over row numbers
    for (size_t i = 0; i < row_ndx_width+1; ++i)
        out << " ";

    // Write header
    for (size_t col = 0; col < column_count; ++col) {
        const char* const name = get_column_name(col);
        const ColumnType type = get_column_type(col);
        size_t width = 0;
        switch (type) {
            case COLUMN_TYPE_BOOL:
                width = 5;
                break;
            case COLUMN_TYPE_DATE:
                width = 19;
                break;
            case COLUMN_TYPE_INT:
                width = chars_in_int(maximum(col));
                break;
            case COLUMN_TYPE_FLOAT:
                width = 12;  // FIXME
                break;
            case COLUMN_TYPE_DOUBLE:
                width = 12;  // FIXME
                break;
            case COLUMN_TYPE_TABLE:
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = chars_in_int( get_subtable_size(col, row) );
                    width = max(width, len+2);
                }
                width += 2; // space for "[]"
                break;
            case COLUMN_TYPE_BINARY:
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = chars_in_int( get_binary(col, row).len ) + 2;
                    width = max(width, len);
                }
                width += 6; // space for " bytes"
                break;
            case COLUMN_TYPE_STRING:
            {   // Find max length of the strings
                for (size_t row = 0; row < row_count; ++row) {
                    size_t len = get_string_length(col, row);
                    width = max(width, len);
                }
                if (width > 20) width = 23; // cut strings longer than 20 chars
                break;
            }
            case COLUMN_TYPE_MIXED:
                // Find max length of the mixed values
                width = 0;
                for (size_t row = 0; row < row_count; ++row) {
                    const ColumnType mtype = get_mixed_type(col, row);
                    if (mtype == COLUMN_TYPE_TABLE) {
                        size_t len = chars_in_int( get_subtable_size(col, row) ) + 2;
                        width = max(width, len);
                    }
                    else {
                        const Mixed m = get_mixed(col, row);
                        switch (mtype) {
                            case COLUMN_TYPE_BOOL:
                                width = max(width, (size_t)5);
                                break;
                            case COLUMN_TYPE_DATE:
                                width = max(width, (size_t)19);
                                break;
                            case COLUMN_TYPE_INT:
                                width = max(width, chars_in_int(m.get_int()));
                                break;
                            case COLUMN_TYPE_FLOAT:
                                width = max(width, (size_t)12); // FIXME
                                break;
                            case COLUMN_TYPE_DOUBLE:
                                width = max(width, (size_t)12); // FIXME
                                break;
                            case COLUMN_TYPE_BINARY:
                                width = max(width, chars_in_int(m.get_binary().len) + 6);
                                break;
                            case COLUMN_TYPE_STRING:
                            {
                                size_t len = strlen(m.get_string());
                                if (len > 20) len = 23;
                                width = max(width, len);
                                break;
                            }
                            default:
                                TIGHTDB_ASSERT(false);
                        }
                    }
                }
                break;
            default:
                TIGHTDB_ASSERT(false);
        }
        // Set width to max of column name and the longest value
        const size_t name_len = strlen(name);
        if (name_len > width)
            width = name_len;
        
        widths.push_back(width);
        out << "  "; // spacing

        out.width(width);
        out << name;
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
    const size_t width = out.width() - chars_in_int(len) - 1;
    out.width(width);
    out << "[" << len << "]";
}

}

void Table::to_string_row(size_t row_ndx, std::ostream& out, const std::vector<size_t>& widths) const
{
    const size_t column_count  = get_column_count();
    const size_t row_ndx_width = widths[0];

    out.width(row_ndx_width);
    out << row_ndx << ":";

    for (size_t col = 0; col < column_count; ++col) {
        out << "  "; // spacing
        out.width(widths[col+1]);

        const ColumnType type = get_column_type(col);
        switch (type) {
            case COLUMN_TYPE_BOOL:
                out << (get_bool(col, row_ndx) ? "true" : "false");
                break;
            case COLUMN_TYPE_INT:
                out << get_int(col, row_ndx);
                break;
            case COLUMN_TYPE_FLOAT:
                out << get_float(col, row_ndx);
                break;
            case COLUMN_TYPE_DOUBLE:
                out << get_double(col, row_ndx);
                break;
            case COLUMN_TYPE_STRING:
                out_string(out, get_string(col, row_ndx), 20);
                break;
            case COLUMN_TYPE_DATE:
                out_date(out, get_date(col, row_ndx));
                break;
            case COLUMN_TYPE_TABLE:
                out_table(out, get_subtable_size(col, row_ndx));
                break;
            case COLUMN_TYPE_BINARY:
                out.width(widths[col+1]-6); // adjust for " bytes" text
                out << get_binary(col, row_ndx).len << " bytes";
                break;
            case COLUMN_TYPE_MIXED:
            {
                const ColumnType mtype = get_mixed_type(col, row_ndx);
                if (mtype == COLUMN_TYPE_TABLE) {
                    out_table(out, get_subtable_size(col, row_ndx));
                }
                else {
                    const Mixed m = get_mixed(col, row_ndx);
                    switch (mtype) {
                        case COLUMN_TYPE_BOOL:
                            out << (m.get_bool() ? "true" : "false");
                            break;
                        case COLUMN_TYPE_INT:
                            out << m.get_int();
                            break;
                        case COLUMN_TYPE_FLOAT:
                            out << m.get_float();
                            break;
                        case COLUMN_TYPE_DOUBLE:
                            out << m.get_double();
                            break;
                        case COLUMN_TYPE_STRING:
                            out_string(out, m.get_string(), 20);
                            break;
                        case COLUMN_TYPE_DATE:
                            out_date(out, m.get_date());
                            break;
                        case COLUMN_TYPE_BINARY:
                            out.width(widths[col+1]-6); // adjust for " bytes" text
                            out << m.get_binary().len << " bytes";
                            break;
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
        case COLUMN_TYPE_FLOAT:
            {
                const ColumnFloat& c1 = GetColumnFloat(i);
                const ColumnFloat& c2 = t.GetColumnFloat(i);
                if (!c1.Compare(c2)) return false;
            }
            break;
        case COLUMN_TYPE_DOUBLE:
            {
                const ColumnDouble& c1 = GetColumnDouble(i);
                const ColumnDouble& c2 = t.GetColumnDouble(i);
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
            case COLUMN_TYPE_FLOAT:
                {
                    const ColumnFloat& column = GetColumnFloat(i);
                    TIGHTDB_ASSERT(column.Size() == m_size);
                    column.Verify();
                }
                break;
            case COLUMN_TYPE_DOUBLE:
                {
                    const ColumnDouble& column = GetColumnDouble(i);
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
        case COLUMN_TYPE_FLOAT:
            cout << "Float      "; break;
        case COLUMN_TYPE_DOUBLE:
            cout << "Double     "; break;
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
            case COLUMN_TYPE_FLOAT:
                {
                    const ColumnFloat& column = GetColumnFloat(n);
                    cout << setw(10) << column.Get(i) << " ";
                }
                break;
            case COLUMN_TYPE_DOUBLE:
                {
                    const ColumnDouble& column = GetColumnDouble(n);
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
