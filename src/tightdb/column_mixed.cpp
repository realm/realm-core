#include <tightdb/column_mixed.hpp>

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
    m_array = new Array(coldef_HasRefs, NULL, 0, alloc);

    m_types = new Column(coldef_Normal, alloc);
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
    TIGHTDB_ASSERT(m_array->Size() == 2 || m_array->Size() == 3);

    const size_t types_ref = m_array->GetAsRef(0);
    const size_t refs_ref  = m_array->GetAsRef(1);

    m_types = new Column(types_ref, m_array, 0, alloc);
    m_refs  = new RefsColumn(alloc, table, column_ndx, m_array, 1, refs_ref);
    TIGHTDB_ASSERT(m_types->Size() == m_refs->Size());

    // Binary column with values that does not fit in refs
    // is only there if needed
    if (m_array->Size() == 3) {
        const size_t data_ref = m_array->GetAsRef(2);
        m_data = new ColumnBinary(data_ref, m_array, 2, alloc);
    }
}

void ColumnMixed::InitDataColumn()
{
    if (m_data)
        return;

    TIGHTDB_ASSERT(m_array->Size() == 2);

    // Create new data column for items that do not fit in refs
    m_data = new ColumnBinary(m_array->GetAllocator());
    const size_t ref = m_data->GetRef();

    m_array->add(ref);
    m_data->SetParent(m_array, 2);
}

void ColumnMixed::clear_value(size_t ndx, MixedColType newtype)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    const MixedColType type = (MixedColType)m_types->Get(ndx);
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
            case mixcol_Binary:
            {
                // If item is in middle of the column, we just clear
                // it to avoid having to adjust refs to following items
                const size_t ref = m_refs->GetAsRef(ndx) >> 1;
                if (ref == m_data->Size()-1)
                    m_data->Delete(ref);
                else
                    m_data->Set(ref, "", 0);
                break;
            }
            case mixcol_Table:
            {
                // Delete entire table
                const size_t ref = m_refs->GetAsRef(ndx);
                Array top(ref, (Array*)NULL, 0, m_array->GetAllocator());
                top.Destroy();
                break;
            }
            default:
                TIGHTDB_ASSERT(false);
        }
    }
    if (type != newtype)
        m_types->Set(ndx, newtype);
}

void ColumnMixed::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    // Remove refs or binary data
    clear_value(ndx, mixcol_Int);

    m_types->Delete(ndx);
    m_refs->Delete(ndx);

    invalidate_subtables();
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
    MixedColType coltype = static_cast<MixedColType>(m_types->Get(ndx));
    switch (coltype) {
        case mixcol_IntNeg:    return type_Int;
        case mixcol_DoubleNeg: return type_Double;
        default: return static_cast<DataType>(coltype);   // all others must be in sync with ColumnType
    }
}

void ColumnMixed::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        m_types->Insert(i, mixcol_Int);
    }
    for (size_t i = 0; i < count; ++i) {
        m_refs->Insert(i, 1); // 1 is zero shifted one and low bit set;
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


void ColumnMixed::set_string(size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    InitDataColumn();

    const MixedColType type = (MixedColType)m_types->Get(ndx);
    const size_t len = strlen(value)+1;

    // See if we can reuse data position
    if (type == mixcol_String) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
    }
    else if (type == mixcol_Binary) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
        m_types->Set(ndx, mixcol_String);
    }
    else {
        // Remove refs or binary data
        clear_value(ndx, mixcol_String);

        // Add value to data column
        const size_t ref = m_data->Size();
        m_data->add(value, len);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        const int64_t v = (ref << 1) + 1;

        m_types->Set(ndx, mixcol_String);
        m_refs->Set(ndx, v);
    }
}

void ColumnMixed::set_binary(size_t ndx, const char* value, size_t len)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    InitDataColumn();

    const MixedColType type = (MixedColType)m_types->Get(ndx);

    // See if we can reuse data position
    if (type == mixcol_String) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
        m_types->Set(ndx, mixcol_Binary);
    }
    else if (type == mixcol_Binary) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
    }
    else {
        // Remove refs or binary data
        clear_value(ndx, mixcol_Binary);

        // Add value to data column
        const size_t ref = m_data->Size();
        m_data->add(value, len);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        const int64_t v = (ref << 1) + 1;

        m_types->Set(ndx, mixcol_Binary);
        m_refs->Set(ndx, v);
    }
}

bool ColumnMixed::Compare(const ColumnMixed& c) const
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
            if (strcmp(get_string(i), c.get_string(i)) != 0) return false;
            break;
        case type_Binary:
            {
                const BinaryData d1 = get_binary(i);
                const BinaryData d2 = c.get_binary(i);
                if (d1.len != d2.len || !std::equal(d1.pointer, d1.pointer+d1.len, d2.pointer))
                    return false;
            }
            break;
        case type_Table:
            {
                ConstTableRef t1 = get_subtable_ptr(i)->get_table_ref();
                ConstTableRef t2 = c.get_subtable_ptr(i)->get_table_ref();
                if (*t1 != *t2)
                    return false;
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
        const size_t tref = m_refs->GetAsRef(i);
        if (tref == 0 || tref & 0x1) continue;
        ConstTableRef subtable = m_refs->get_subtable(i);
        subtable->Verify();
    }
}

void ColumnMixed::ToDot(std::ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    out << "subgraph cluster_columnmixed" << ref << " {" << std::endl;
    out << " label = \"ColumnMixed";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    m_array->ToDot(out, "mixed_top");

    // Write sub-tables
    const size_t count = Size();
    for (size_t i = 0; i < count; ++i) {
        const MixedColType type = (MixedColType)m_types->Get(i);
        if (type != mixcol_Table) continue;
        ConstTableRef subtable = m_refs->get_subtable(i);
        subtable->to_dot(out);
    }

    m_types->ToDot(out, "types");
    m_refs->ToDot(out, "refs");

    if (m_array->Size() > 2) {
        m_data->ToDot(out, "data");
    }

    out << "}" << std::endl;
}

#endif // TIGHTDB_DEBUG

} // namespace tightdb
