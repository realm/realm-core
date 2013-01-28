#include <limits>
#include <tightdb/column_mixed.hpp>
#include <tightdb/column_binary.hpp>

using namespace std;

#define BIT63 0x8000000000000000    

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
    m_array = new Array(COLUMN_HASREFS, NULL, 0, alloc);

    m_types = new Column(COLUMN_NORMAL, alloc);
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
    if (type != MIXED_COL_INT) {
        switch (type) {
            case MIXED_COL_INT_NEG:
            case MIXED_COL_BOOL:
            case MIXED_COL_DATE:
            case MIXED_COL_FLOAT:
            case MIXED_COL_DOUBLE:
            case MIXED_COL_DOUBLE_NEG:
                break;
            case MIXED_COL_STRING:
            case MIXED_COL_BINARY:
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
            case MIXED_COL_TABLE:
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

ColumnType ColumnMixed::GetType(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    MixedColType coltype = static_cast<MixedColType>(m_types->Get(ndx));
    switch (coltype) {
    case MIXED_COL_INT:         return COLUMN_TYPE_INT;
    case MIXED_COL_INT_NEG:     return COLUMN_TYPE_INT;
    case MIXED_COL_BOOL:        return COLUMN_TYPE_BOOL;
    case MIXED_COL_STRING:      return COLUMN_TYPE_STRING;
    case MIXED_COL_BINARY:      return COLUMN_TYPE_BINARY;
    case MIXED_COL_TABLE:       return COLUMN_TYPE_TABLE;
    case MIXED_COL_DATE:        return COLUMN_TYPE_DATE;
    case MIXED_COL_FLOAT:       return COLUMN_TYPE_FLOAT;
    case MIXED_COL_DOUBLE:      return COLUMN_TYPE_DOUBLE;
    case MIXED_COL_DOUBLE_NEG:  return COLUMN_TYPE_DOUBLE;
    default:
        TIGHTDB_ASSERT(false); 
        return (COLUMN_TYPE_INT);
    }
}


//
// Getters
//

int64_t ColumnMixed::get_value(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    // Shift the unsigned value right - ensuring 0 gets in from left.
    // Shifting signed integers right doesn't ensure 0's.
    const uint64_t value = static_cast<uint64_t>(m_refs->Get(ndx)) >> 1;
    return static_cast<int64_t>(value);
}

int64_t ColumnMixed::get_int(size_t ndx) const
{
    // Get first 63 bits of the integer value
    int64_t value = get_value(ndx);

    // restore 'sign'-bit from the column-type
    const MixedColType coltype = static_cast<MixedColType>(m_types->Get(ndx));
    if (coltype == MIXED_COL_INT_NEG)
        value |= BIT63; // set sign bit (63)
    else {
        TIGHTDB_ASSERT(coltype == MIXED_COL_INT);
    }
    return value;
}

bool ColumnMixed::get_bool(size_t ndx) const
{
    TIGHTDB_ASSERT(m_types->Get(ndx) == MIXED_COL_BOOL);

    return (get_value(ndx) != 0);
}

time_t ColumnMixed::get_date(size_t ndx) const
{
    TIGHTDB_ASSERT(m_types->Get(ndx) == MIXED_COL_DATE);

    return static_cast<time_t>(get_value(ndx));
}

float ColumnMixed::get_float(size_t ndx) const
{
    TIGHTDB_STATIC_ASSERT(numeric_limits<float>::is_iec559, "'float' is not IEEE");
    TIGHTDB_STATIC_ASSERT((sizeof(float) * CHAR_BIT == 32), "Assume 32 bit float.");
    TIGHTDB_ASSERT(m_types->Get(ndx) == MIXED_COL_FLOAT);

    const int64_t intval = get_value(ndx);
    const void* vptr = reinterpret_cast<const void*>(&intval);
    const float value = * reinterpret_cast<const float *>(vptr);
    return value;
}

double ColumnMixed::get_double(size_t ndx) const
{
    TIGHTDB_STATIC_ASSERT(numeric_limits<double>::is_iec559, "'double' is not IEEE");
    TIGHTDB_STATIC_ASSERT((sizeof(double) * CHAR_BIT == 64), "Assume 64 bit double.");

    int64_t intval = get_value(ndx);

    // restore 'sign'-bit from the column-type
    const MixedColType coltype = static_cast<MixedColType>(m_types->Get(ndx));
    if (coltype == MIXED_COL_DOUBLE_NEG)
        intval |= BIT63; // set sign bit (63)
    else {
        TIGHTDB_ASSERT(coltype == MIXED_COL_DOUBLE);
    }
    const void* vptr = reinterpret_cast<const void*>(&intval);
    const double value = * reinterpret_cast<const double *>(vptr);
    return value;
}

const char* ColumnMixed::get_string(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    TIGHTDB_ASSERT(m_types->Get(ndx) == MIXED_COL_STRING);
    TIGHTDB_ASSERT(m_data);

    const size_t ref = m_refs->GetAsRef(ndx) >> 1;
    const char* value = (const char*)m_data->GetData(ref);
    return value;
}

BinaryData ColumnMixed::get_binary(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    TIGHTDB_ASSERT(m_types->Get(ndx) == MIXED_COL_BINARY);
    TIGHTDB_ASSERT(m_data);

    const size_t ref = m_refs->GetAsRef(ndx) >> 1;
    return m_data->Get(ref);
}

//
// Inserts
//

// Insert a int64 value. 
// Store 63 bit of the value in m_refs. Store sign bit in m_types.

template<ColumnMixed::MixedColType pos_type, ColumnMixed::MixedColType neg_type, typename T>
void ColumnMixed::insert_int64(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= m_types->Size());

    void* vptr = reinterpret_cast<void*>(&value);
    int64_t val64 = * reinterpret_cast<int64_t*>(vptr);

    // 'store' the sign-bit in the integer-type
    if ((val64 & BIT63) == 0)
        m_types->Insert(ndx, pos_type);
    else
        m_types->Insert(ndx, neg_type);

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    val64 = (val64 << 1) + 1;
    m_refs->Insert(ndx, val64);
}

void ColumnMixed::insert_int(size_t ndx, int64_t value)
{
    insert_int64<MIXED_COL_INT, MIXED_COL_INT_NEG, int64_t>(ndx, value);
}

void ColumnMixed::insert_double(size_t ndx, double value)
{

    insert_int64<MIXED_COL_DOUBLE, MIXED_COL_DOUBLE_NEG, double>(ndx, value);
}

void ColumnMixed::insert_float(size_t ndx, float value)
{
    TIGHTDB_ASSERT(ndx <= m_types->Size());

    const void* vptr = reinterpret_cast<const void*>(&value);
    const int32_t val32 = * reinterpret_cast<const int32_t*>(vptr);
    // Shift value one bit and set lowest bit to indicate that this is not a ref
    const int64_t val64 = (static_cast<int64_t>(val32) << 1) + 1;
    m_refs->Insert(ndx, val64);
    m_types->Insert(ndx, MIXED_COL_FLOAT);
}

void ColumnMixed::insert_bool(size_t ndx, bool value)
{
    TIGHTDB_ASSERT(ndx <= m_types->Size());

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    const int64_t v = ((value ? 1 : 0) << 1) + 1;

    m_types->Insert(ndx, MIXED_COL_BOOL);
    m_refs->Insert(ndx, v);
}

void ColumnMixed::insert_date(size_t ndx, time_t value)
{
    TIGHTDB_ASSERT(ndx <= m_types->Size());

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    const int64_t v = (value << 1) + 1;

    m_types->Insert(ndx, MIXED_COL_DATE);
    m_refs->Insert(ndx, v);
}

void ColumnMixed::insert_string(size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(ndx <= m_types->Size());
    InitDataColumn();

    const size_t len = strlen(value)+1;
    const size_t ref = m_data->Size();
    m_data->add(value, len);

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    const int64_t v = (ref << 1) + 1;

    m_types->Insert(ndx, MIXED_COL_STRING);
    m_refs->Insert(ndx, v);
}

void ColumnMixed::insert_binary(size_t ndx, const char* value, size_t len)
{
    TIGHTDB_ASSERT(ndx <= m_types->Size());
    InitDataColumn();

    const size_t ref = m_data->Size();
    m_data->add(value, len);

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    const int64_t v = (ref << 1) + 1;

    m_types->Insert(ndx, MIXED_COL_BINARY);
    m_refs->Insert(ndx, v);
}

void ColumnMixed::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        m_types->Insert(i, MIXED_COL_INT);
    }
    for (size_t i = 0; i < count; ++i) {
        m_refs->Insert(i, 1); // 1 is zero shifted one and low bit set; 
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

//
// Setters
//

// Set a int64 value. 
// Store 63 bit of the value in m_refs. Store sign bit in m_types.

template<ColumnMixed::MixedColType pos_type, ColumnMixed::MixedColType neg_type, typename T>
void ColumnMixed::set_int64(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    void* vptr = reinterpret_cast<void*>(&value);
    int64_t val64 = *(reinterpret_cast<int64_t*>(vptr));
    
    // If sign-bit is set in value, 'store' it in the column-type
    const MixedColType coltype = ((val64 & BIT63) == 0) ? pos_type : neg_type;
    
    // Remove refs or binary data (sets type to double)
    clear_value(ndx, coltype);

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    val64 = (val64 << 1) + 1;
    m_refs->Set(ndx, val64);
}

void ColumnMixed::set_int(size_t ndx, int64_t value)
{
    set_int64<MIXED_COL_INT, MIXED_COL_INT_NEG, int64_t>(ndx, value);
}

void ColumnMixed::set_double(size_t ndx, double value)
{
    set_int64<MIXED_COL_DOUBLE, MIXED_COL_DOUBLE_NEG, double>(ndx, value);
}

template<ColumnMixed::MixedColType coltype>
void ColumnMixed::set_value(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    // Remove refs or binary data (sets type to float)
    clear_value(ndx, coltype);

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    const int64_t v = (value << 1) + 1;
    m_refs->Set(ndx, v);
}

void ColumnMixed::set_float(size_t ndx, float value)
{
    const void* vptr = reinterpret_cast<const void*>(&value);
    const int32_t val32 = * reinterpret_cast<const int32_t*>(vptr);
    set_value<MIXED_COL_FLOAT>(ndx, static_cast<const int64_t>(val32));
}

void ColumnMixed::set_bool(size_t ndx, bool value)
{
    set_value<MIXED_COL_BOOL>(ndx, (value ? 1 : 0));
}

void ColumnMixed::set_date(size_t ndx, time_t value)
{
    set_value<MIXED_COL_DATE>(ndx, static_cast<const int64_t>(value));
}

void ColumnMixed::set_string(size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    InitDataColumn();

    const MixedColType type = (MixedColType)m_types->Get(ndx);
    const size_t len = strlen(value)+1;

    // See if we can reuse data position
    if (type == MIXED_COL_STRING) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
    }
    else if (type == MIXED_COL_BINARY) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
        m_types->Set(ndx, MIXED_COL_STRING);
    }
    else {
        // Remove refs or binary data
        clear_value(ndx, MIXED_COL_STRING);

        // Add value to data column
        const size_t ref = m_data->Size();
        m_data->add(value, len);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        const int64_t v = (ref << 1) + 1;

        m_types->Set(ndx, MIXED_COL_STRING);
        m_refs->Set(ndx, v);
    }
}

void ColumnMixed::set_binary(size_t ndx, const char* value, size_t len)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    InitDataColumn();

    const MixedColType type = (MixedColType)m_types->Get(ndx);

    // See if we can reuse data position
    if (type == MIXED_COL_STRING) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
        m_types->Set(ndx, MIXED_COL_BINARY);
    }
    else if (type == MIXED_COL_BINARY) {
        const size_t ref = m_refs->GetAsRef(ndx) >> 1;
        m_data->Set(ref, value, len);
    }
    else {
        // Remove refs or binary data
        clear_value(ndx, MIXED_COL_BINARY);

        // Add value to data column
        const size_t ref = m_data->Size();
        m_data->add(value, len);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        const int64_t v = (ref << 1) + 1;

        m_types->Set(ndx, MIXED_COL_BINARY);
        m_refs->Set(ndx, v);
    }
}


// FIXME: Check that callers test the return value
bool ColumnMixed::insert_subtable(size_t ndx)
{
    TIGHTDB_ASSERT(ndx <= m_types->Size());
    const size_t ref = Table::create_empty_table(m_array->GetAllocator());
    if (!ref) return false;
    // FIXME: These inserts can also fail on allocation
    m_types->Insert(ndx, MIXED_COL_TABLE);
    m_refs->Insert(ndx, ref);
    return true;
}

// FIXME: Check that callers test the return value
bool ColumnMixed::set_subtable(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());
    const size_t ref = Table::create_empty_table(m_array->GetAllocator());
    if (!ref) 
        return false;
    // FIXME: Could the following operations also fail on allocation?
    clear_value(ndx, MIXED_COL_TABLE); // Remove any previous refs or binary data
    m_refs->Set(ndx, ref);
    return true;
}

void ColumnMixed::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_types->Size());

    // Remove refs or binary data
    clear_value(ndx, MIXED_COL_INT);

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

bool ColumnMixed::Compare(const ColumnMixed& c) const
{
    const size_t n = Size();
    if (c.Size() != n) 
        return false;
    
    for (size_t i=0; i<n; ++i) {
        const ColumnType type = GetType(i);
        if (c.GetType(i) != type)
            return false;
        switch (type) {
        case COLUMN_TYPE_INT:
            if (get_int(i) != c.get_int(i)) return false;
            break;
        case COLUMN_TYPE_BOOL:
            if (get_bool(i) != c.get_bool(i)) return false;
            break;
        case COLUMN_TYPE_DATE:
            if (get_date(i) != c.get_date(i)) return false;
            break;
        case COLUMN_TYPE_FLOAT:
            if (get_float(i) != c.get_float(i)) return false;
            break;
        case COLUMN_TYPE_DOUBLE:
            if (get_double(i) != c.get_double(i)) return false;
            break;
        case COLUMN_TYPE_STRING:
            if (strcmp(get_string(i), c.get_string(i)) != 0) return false;
            break;
        case COLUMN_TYPE_BINARY:
            {
                const BinaryData d1 = get_binary(i);
                const BinaryData d2 = c.get_binary(i);
                if (d1.len != d2.len ||
                    !equal(d1.pointer, d1.pointer+d1.len, d2.pointer)) return false;
            }
            break;
        case COLUMN_TYPE_TABLE:
            {
                ConstTableRef t1 = get_subtable_ptr(i)->get_table_ref();
                ConstTableRef t2 = c.get_subtable_ptr(i)->get_table_ref();
                if (*t1 != *t2) return false;
            }
            break;
        default:
            TIGHTDB_ASSERT(false);
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
        if (type != MIXED_COL_TABLE) continue;
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
