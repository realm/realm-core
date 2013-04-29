#include <tightdb/column_mixed.hpp>

using namespace std;

namespace tightdb {

ColumnMixed::~ColumnMixed()
{
    delete m_types;
    delete m_refs;
    delete m_data;
    delete m_array;
}

void ColumnMixed::Destroy()
{
    if (m_array != NULL)
        m_array->Destroy();
}

void ColumnMixed::SetParent(ArrayParent *parent, size_t pndx)
{
    m_array->SetParent(parent, pndx);
}

void ColumnMixed::UpdateFromParent()
{
    if (!m_array->UpdateFromParent())
        return;

    m_types->UpdateFromParent();
    m_refs->UpdateFromParent();
    if (m_data)
        m_data->UpdateFromParent();
}


void ColumnMixed::Create(Allocator& alloc, const Table* table, size_t column_ndx)
{
    m_array = new Array(Array::coldef_HasRefs, NULL, 0, alloc);

    m_types = new Column(Array::coldef_Normal, alloc);
    m_refs  = new RefsColumn(alloc, table, column_ndx);

    m_array->add(m_types->GetRef());
    m_array->add(m_refs->GetRef());

    m_types->SetParent(m_array, 0);
    m_refs->SetParent(m_array, 1);
}

void ColumnMixed::Create(Allocator& alloc, const Table* table, size_t column_ndx,
                         ArrayParent* parent, size_t ndx_in_parent, size_t ref)
{
    m_array = new Array(ref, parent, ndx_in_parent, alloc);
    TIGHTDB_ASSERT(m_array->size() == 2 || m_array->size() == 3);

    const size_t types_ref = m_array->GetAsRef(0);
    const size_t refs_ref  = m_array->GetAsRef(1);

    m_types = new Column(types_ref, m_array, 0, alloc);
    m_refs  = new RefsColumn(alloc, table, column_ndx, m_array, 1, refs_ref);
    TIGHTDB_ASSERT(m_types->Size() == m_refs->Size());

    // Binary column with values that does not fit in refs
    // is only there if needed
    if (m_array->size() == 3) {
        const size_t data_ref = m_array->GetAsRef(2);
        m_data = new ColumnBinary(data_ref, m_array, 2, alloc);
    }
}

void ColumnMixed::InitDataColumn()
{
    if (m_data)
        return;

    TIGHTDB_ASSERT(m_array->size() == 2);

    // Create new data column for items that do not fit in refs
    m_data = new ColumnBinary(m_array->GetAllocator());
    const size_t ref = m_data->GetRef();

    m_array->add(ref);
    m_data->SetParent(m_array, 2);
}

void ColumnMixed::clear_value(size_t ndx, MixedColType newtype)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    MixedColType type = MixedColType(m_types->get(ndx));
    if (type != mixcol_Int) {
        switch (type) {
            case mixcol_IntNeg:
            case mixcol_Bool:
            case mixcol_Date:
            case mixcol_Float:
            case mixcol_Double:
            case mixcol_DoubleNeg:
                break;
            case mixcol_String:
            case mixcol_Binary: {
                // If item is in middle of the column, we just clear
                // it to avoid having to adjust refs to following items
                // TODO: this is a leak. We should adjust
                const size_t ref = m_refs->GetAsRef(ndx) >> 1;
                if (ref == m_data->Size()-1)
                    m_data->erase(ref);
                else
                    // FIXME: But this will lead to unbounded in-file
                    // leaking in for(;;) { insert_binary(i, ...);
                    // erase(i); }
                    m_data->set(ref, BinaryData());
                break;
            }
            case mixcol_Table: {
                // Delete entire table
                const size_t ref = m_refs->GetAsRef(ndx);
                Array top(ref, 0, 0, m_array->GetAllocator());
                top.Destroy();
                break;
            }
            default:
                TIGHTDB_ASSERT(false);
        }
    }
    if (type != newtype)
        m_types->set(ndx, newtype);
    m_refs->set(ndx, 0);
}

void ColumnMixed::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    // Remove refs or binary data
    clear_value(ndx, mixcol_Int);

    m_types->erase(ndx);
    m_refs->erase(ndx);

    invalidate_subtables();
}

void ColumnMixed::move_last_over(size_t ndx) {
    TIGHTDB_ASSERT(ndx+1 < Size());

    // Remove refs or binary data
    clear_value(ndx, mixcol_Int);

    m_types->move_last_over(ndx);
    m_refs->move_last_over(ndx);
}

void ColumnMixed::Clear()
{
    m_types->Clear();
    m_refs->Clear();
    if (m_data)
        m_data->Clear();
}

DataType ColumnMixed::get_type(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    MixedColType coltype = MixedColType(m_types->get(ndx));
    switch (coltype) {
        case mixcol_IntNeg:    return type_Int;
        case mixcol_DoubleNeg: return type_Double;
        default: return DataType(coltype);   // all others must be in sync with ColumnType
    }
}

void ColumnMixed::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        m_types->insert(i, mixcol_Int);
    }
    for (size_t i = 0; i < count; ++i) {
        m_refs->insert(i, 1); // 1 is zero shifted one and low bit set;
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


void ColumnMixed::set_string(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    InitDataColumn();

    const MixedColType type = MixedColType(m_types->get(ndx));

    // See if we can reuse data position
    if (type == mixcol_String) {
        size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->set_string(ref, value);
    }
    else if (type == mixcol_Binary) {
        size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->set_string(ref, value);
        m_types->set(ndx, mixcol_String);
    }
    else {
        // Remove refs or binary data
        clear_value(ndx, mixcol_String);

        // Add value to data column
        const size_t ref = m_data->Size();
        m_data->add_string(value);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        const int64_t v = (ref << 1) + 1;

        m_types->set(ndx, mixcol_String);
        m_refs->set(ndx, v);
    }
}

void ColumnMixed::set_binary(size_t ndx, BinaryData value)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    InitDataColumn();

    const MixedColType type = MixedColType(m_types->get(ndx));

    // See if we can reuse data position
    if (type == mixcol_String) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->set(ref, value);
        m_types->set(ndx, mixcol_Binary);
    }
    else if (type == mixcol_Binary) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->set(ref, value);
    }
    else {
        // Remove refs or binary data
        clear_value(ndx, mixcol_Binary);

        // Add value to data column
        const size_t ref = m_data->Size();
        m_data->add(value);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        const int64_t v = (ref << 1) + 1;

        m_types->set(ndx, mixcol_Binary);
        m_refs->set(ndx, v);
    }
}

bool ColumnMixed::compare(const ColumnMixed& c) const
{
    const size_t n = Size();
    if (c.Size() != n)
        return false;

    for (size_t i=0; i<n; ++i) {
        const DataType type = get_type(i);
        if (c.get_type(i) != type)
            return false;
        switch (type) {
        case type_Int:
            if (get_int(i) != c.get_int(i)) return false;
            break;
        case type_Bool:
            if (get_bool(i) != c.get_bool(i)) return false;
            break;
        case type_Date:
            if (get_date(i) != c.get_date(i)) return false;
            break;
        case type_Float:
            if (get_float(i) != c.get_float(i)) return false;
            break;
        case type_Double:
            if (get_double(i) != c.get_double(i)) return false;
            break;
        case type_String:
            if (get_string(i) != c.get_string(i)) return false;
            break;
        case type_Binary:
            if (get_binary(i) != c.get_binary(i)) return false;
            break;
        case type_Table: {
                ConstTableRef t1 = get_subtable_ptr(i)->get_table_ref();
                ConstTableRef t2 = c.get_subtable_ptr(i)->get_table_ref();
                if (*t1 != *t2) return false;
            }
            break;
        case type_Mixed:
            TIGHTDB_ASSERT(false);
            break;
        }
    }
    return true;
}


#ifdef TIGHTDB_DEBUG

void ColumnMixed::Verify() const
{
    m_array->Verify();
    m_types->Verify();
    m_refs->Verify();
    if (m_data) m_data->Verify();

    // types and refs should be in sync
    const size_t types_len = m_types->Size();
    const size_t refs_len  = m_refs->Size();
    TIGHTDB_ASSERT(types_len == refs_len);

    // Verify each sub-table
    const size_t count = Size();
    for (size_t i = 0; i < count; ++i) {
        const int64_t v = m_refs->get(i);
        if (v == 0 || v & 0x1) continue;
        ConstTableRef subtable = m_refs->get_subtable(i);
        subtable->Verify();
    }
}

void ColumnMixed::ToDot(ostream& out, StringData title) const
{
    const size_t ref = GetRef();

    out << "subgraph cluster_columnmixed" << ref << " {" << endl;
    out << " label = \"ColumnMixed";
    if (0 < title.size()) out << "\\n'" << title << "'";
    out << "\";" << endl;

    m_array->ToDot(out, "mixed_top");

    // Write sub-tables
    const size_t count = Size();
    for (size_t i = 0; i < count; ++i) {
        const MixedColType type = MixedColType(m_types->get(i));
        if (type != mixcol_Table) continue;
        ConstTableRef subtable = m_refs->get_subtable(i);
        subtable->to_dot(out);
    }

    m_types->ToDot(out, "types");
    m_refs->ToDot(out, "refs");

    if (m_array->size() > 2) {
        m_data->ToDot(out, "data");
    }

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

} // namespace tightdb
