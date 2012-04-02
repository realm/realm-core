#include "ColumnMixed.h"
#include "ColumnBinary.h"

ColumnMixed::ColumnMixed(Allocator& alloc) : m_data(NULL){
	m_array = new Array(COLUMN_HASREFS, NULL, 0, alloc);
	
	m_types = new Column(COLUMN_NORMAL, alloc);
	m_refs  = new RefsColumn(alloc);
	
	m_array->Add(m_types->GetRef());
	m_array->Add(m_refs->GetRef());
	
	m_types->SetParent(m_array, 0);
	m_refs->SetParent(m_array, 1);
}

ColumnMixed::ColumnMixed(size_t ref, Array* parent, size_t pndx, Allocator& alloc) : m_data(NULL){
	Create(ref, parent, pndx, alloc);
}

ColumnMixed::~ColumnMixed() {
	delete m_types;
	delete m_refs;
	delete m_data;
}

void ColumnMixed::Destroy() {
	if(m_array != NULL)
		m_array->Destroy();
}

void ColumnMixed::SetParent(Array* parent, size_t pndx) {
	m_array->SetParent(parent, pndx);
}

void ColumnMixed::Create(size_t ref, Array* parent, size_t pndx, Allocator& alloc) {
	m_array = new Array(ref, parent, pndx, alloc);
	assert(m_array->Size() == 2 || m_array->Size() == 3);
	
	const size_t ref_types = m_array->GetAsRef(0);
	const size_t ref_refs  = m_array->GetAsRef(1);
	
	m_types = new Column(ref_types, m_array, 0, alloc);
	m_refs  = new RefsColumn(ref_refs,  m_array, 1, alloc);
	assert(m_types->Size() == m_refs->Size());
	
	// Binary column with values that does not fit in refs
	// is only there if needed
	if (m_array->Size() == 3) {
		const size_t ref_data = m_array->GetAsRef(2);
		m_data = new ColumnBinary(ref_data, m_array, 2, alloc);
	}
}

void ColumnMixed::InitDataColumn() {
	if (m_data) return;
	
	assert(m_array->Size() == 2);
	
	// Create new data column for items that do not fit in refs
	m_data = new ColumnBinary(m_array->GetAllocator());
	const size_t ref = m_data->GetRef();
	
	m_array->Add(ref);
	m_data->SetParent(m_array, 2);
}

void ColumnMixed::ClearValue(size_t ndx, ColumnType newtype) {
	assert(ndx < m_types->Size());
	
	const ColumnType type = (ColumnType)m_types->Get(ndx);
	if (type != COLUMN_TYPE_INT) {
		switch (type) {
			case COLUMN_TYPE_BOOL:
			case COLUMN_TYPE_DATE:
				break;
			case COLUMN_TYPE_STRING:
			case COLUMN_TYPE_BINARY:
			{
				// If item is in middle of the column, we just clear
				// it to avoid having to adjust refs to following items
				const size_t ref = m_refs->GetAsRef(ndx) >> 1;
				if (ref == m_data->Size()-1) m_data->Delete(ref);
				else m_data->Set(ref, "", 0);
				break;
			}
			case COLUMN_TYPE_TABLE:
			{
				// Delete entire table
				const size_t ref = m_refs->GetAsRef(ndx);
				Array top(ref, (Array*)NULL, 0, m_array->GetAllocator());
				top.Destroy();
				break;
			}
			default:
				assert(false);
		}
	}
	
	if (type != newtype) m_types->Set(ndx, newtype);
}

ColumnType ColumnMixed::GetType(size_t ndx) const {
	assert(ndx < m_types->Size());
	return (ColumnType)m_types->Get(ndx);
}

int64_t ColumnMixed::GetInt(size_t ndx) const {
	assert(ndx < m_types->Size());
	assert(m_types->Get(ndx) == COLUMN_TYPE_INT);
	
	const int64_t value = m_refs->Get(ndx) >> 1;
	return value;
}

bool ColumnMixed::GetBool(size_t ndx) const {
	assert(ndx < m_types->Size());
	assert(m_types->Get(ndx) == COLUMN_TYPE_BOOL);
	
	const bool value = (m_refs->Get(ndx) >> 1) == 1;
	return value;
}

time_t ColumnMixed::GetDate(size_t ndx) const {
	assert(ndx < m_types->Size());
	assert(m_types->Get(ndx) == COLUMN_TYPE_DATE);
	
	const time_t value = m_refs->Get(ndx) >> 1;
	return value;
}

const char* ColumnMixed::GetString(size_t ndx) const {
	assert(ndx < m_types->Size());
	assert(m_types->Get(ndx) == COLUMN_TYPE_STRING);
	assert(m_data);
	
	const size_t ref = m_refs->GetAsRef(ndx) >> 1;
	const char* value = (const char*)m_data->GetData(ref);
	
	return value;
}

BinaryData ColumnMixed::GetBinary(size_t ndx) const {
	assert(ndx < m_types->Size());
	assert(m_types->Get(ndx) == COLUMN_TYPE_BINARY);
	assert(m_data);
	
	const size_t ref = m_refs->GetAsRef(ndx) >> 1;
	
	return m_data->Get(ref);
}

void ColumnMixed::InsertInt(size_t ndx, int64_t value) {
	assert(ndx <= m_types->Size());
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = (value << 1) + 1;
	
	m_types->Insert(ndx, COLUMN_TYPE_INT);
	m_refs->Insert(ndx, v);
}

void ColumnMixed::InsertBool(size_t ndx, bool value) {
	assert(ndx <= m_types->Size());
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = ((value ? 1 : 0) << 1) + 1;
	
	m_types->Insert(ndx, COLUMN_TYPE_BOOL);
	m_refs->Insert(ndx, v);
}

void ColumnMixed::InsertDate(size_t ndx, time_t value) {
	assert(ndx <= m_types->Size());
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = (value << 1) + 1;
	
	m_types->Insert(ndx, COLUMN_TYPE_DATE);
	m_refs->Insert(ndx, v);
}

void ColumnMixed::InsertString(size_t ndx, const char* value) {
	assert(ndx <= m_types->Size());
	InitDataColumn();
	
	const size_t len = strlen(value)+1;
	const size_t ref = m_data->Size();
	m_data->Add(value, len);
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = (ref << 1) + 1;
	
	m_types->Insert(ndx, COLUMN_TYPE_STRING);
	m_refs->Insert(ndx, v);
}

void ColumnMixed::InsertBinary(size_t ndx, const char* value, size_t len) {
	assert(ndx <= m_types->Size());
	InitDataColumn();
	
	const size_t ref = m_data->Size();
	m_data->Add(value, len);
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = (ref << 1) + 1;
	
	m_types->Insert(ndx, COLUMN_TYPE_BINARY);
	m_refs->Insert(ndx, v);
}

void ColumnMixed::SetInt(size_t ndx, int64_t value) {
	assert(ndx < m_types->Size());
	
	// Remove refs or binary data (sets type to int)
	ClearValue(ndx, COLUMN_TYPE_INT);
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = (value << 1) + 1;
	
	m_refs->Set(ndx, v);
}

void ColumnMixed::SetBool(size_t ndx, bool value) {
	assert(ndx < m_types->Size());
	
	// Remove refs or binary data (sets type to int)
	ClearValue(ndx, COLUMN_TYPE_BOOL);
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = ((value ? 1 : 0) << 1) + 1;
	
	m_refs->Set(ndx, v);
}

void ColumnMixed::SetDate(size_t ndx, time_t value) {
	assert(ndx < m_types->Size());
	
	// Remove refs or binary data (sets type to int)
	ClearValue(ndx, COLUMN_TYPE_DATE);
	
	// Shift value one bit and set lowest bit to indicate
	// that this is not a ref
	const int64_t v = (value << 1) + 1;
	
	m_refs->Set(ndx, v);
}

void ColumnMixed::SetString(size_t ndx, const char* value) {
	assert(ndx < m_types->Size());
	InitDataColumn();
	
	const ColumnType type = (ColumnType)m_types->Get(ndx);
	const size_t len = strlen(value)+1;
	
	// See if we can reuse data position
	if (type == COLUMN_TYPE_STRING) {
		const size_t ref = m_refs->GetAsRef(ndx) >> 1;
		m_data->Set(ref, value, len);
	}
	else if (type == COLUMN_TYPE_BINARY) {
		const size_t ref = m_refs->GetAsRef(ndx) >> 1;
		m_data->Set(ref, value, len);
		m_types->Set(ndx, COLUMN_TYPE_STRING);
	}
	else {
		// Remove refs or binary data
		ClearValue(ndx, COLUMN_TYPE_STRING);
		
		// Add value to data column
		const size_t ref = m_data->Size();
		m_data->Add(value, len);
		
		// Shift value one bit and set lowest bit to indicate
		// that this is not a ref
		const int64_t v = (ref << 1) + 1;
		
		m_types->Set(ndx, COLUMN_TYPE_STRING);
		m_refs->Set(ndx, v);
	}
}

void ColumnMixed::SetBinary(size_t ndx, const char* value, size_t len) {
	assert(ndx < m_types->Size());
	InitDataColumn();
	
	const ColumnType type = (ColumnType)m_types->Get(ndx);
	
	// See if we can reuse data position
	if (type == COLUMN_TYPE_STRING) {
		const size_t ref = m_refs->GetAsRef(ndx) >> 1;
		m_data->Set(ref, value, len);
		m_types->Set(ndx, COLUMN_TYPE_BINARY);
	}
	else if (type == COLUMN_TYPE_BINARY) {
		const size_t ref = m_refs->GetAsRef(ndx) >> 1;
		m_data->Set(ref, value, len);
	}
	else {
		// Remove refs or binary data
		ClearValue(ndx, COLUMN_TYPE_BINARY);
		
		// Add value to data column
		const size_t ref = m_data->Size();
		m_data->Add(value, len);
		
		// Shift value one bit and set lowest bit to indicate
		// that this is not a ref
		const int64_t v = (ref << 1) + 1;
		
		m_types->Set(ndx, COLUMN_TYPE_BINARY);
		m_refs->Set(ndx, v);
	}
}

bool ColumnMixed::Add() {
	InsertInt(Size(), 0);
	return true;
}

void ColumnMixed::Delete(size_t ndx) {
	assert(ndx < m_types->Size());
	
	// Remove refs or binary data
	ClearValue(ndx, COLUMN_TYPE_INT);
	
	m_types->Delete(ndx);
	m_refs->Delete(ndx);
}

void ColumnMixed::Clear() {
	m_types->Clear();
	m_refs->Clear();
	if (m_data) m_data->Clear();
}

#ifdef _DEBUG

void ColumnMixed::Verify() const {
	m_array->Verify();
	m_types->Verify();
	m_refs->Verify();
	if (m_data) m_data->Verify();
	
	// types and refs should be in sync
	const size_t types_len = m_types->Size();
	const size_t refs_len  = m_refs->Size();
	assert(types_len == refs_len);
	
	// Verify each sub-table
	const size_t count = Size();
	for (size_t i = 0; i < count; ++i) {
		const size_t tref = m_refs->GetAsRef(i);
		if (tref == 0 || tref & 0x1) continue;
		
		const TopLevelTable t = ((ColumnMixed*)this)-> GetTable(i);
		t.Verify();
	}
}

void ColumnMixed::ToDot(std::ostream& out, const char* title) const {
	const size_t ref = GetRef();
	
	out << "subgraph cluster_columnmixed" << ref << " {" << std::endl;
	out << " label = \"ColumnMixed";
	if (title) out << "\\n'" << title << "'";
	out << "\";" << std::endl;
	
	m_array->ToDot(out, "mixed_top");
	
	// Write sub-tables
	const size_t count = Size();
	for (size_t i = 0; i < count; ++i) {
		const ColumnType type = (ColumnType)m_types->Get(i);
		if (type != COLUMN_TYPE_TABLE) continue;
		if (m_refs->GetAsRef(i) == 0) continue; // empty table
		
		const TopLevelTable t = ((ColumnMixed*)this)->GetTable(i);
		t.ToDot(out);
	}
	
	m_types->ToDot(out, "types");
	m_refs->ToDot(out, "refs");
	
	if (m_array->Size() > 2) {
		m_data->ToDot(out, "data");
	}
	
		
	out << "}" << std::endl;
}

#endif //_DEBUG


void ColumnMixed::RefsColumn::insert_table(size_t ndx) {
	// Create new table
	TopLevelTable table(m_array->GetAllocator());

	Insert(ndx, table.GetRef());

	table.SetParent(m_array, ndx);
}

void ColumnMixed::RefsColumn::set_table(size_t ndx) {
	// Create new table
	TopLevelTable table(m_array->GetAllocator());

	Set(ndx, table.GetRef());

	table.SetParent(m_array, ndx);
}

TopLevelTable ColumnMixed::RefsColumn::get_table(size_t ndx) {
	const size_t ref = GetAsRef(ndx);
	Allocator &alloc = m_array->GetAllocator();

	bool is_subtable = true;
	return TopLevelTable(alloc, ref, m_array, ndx, is_subtable);
}

TopLevelTable *ColumnMixed::RefsColumn::get_table_ptr(size_t ndx) {
	const size_t ref = GetAsRef(ndx);
	Allocator &alloc = m_array->GetAllocator();

	bool is_subtable = true;
	// Receiver will own pointer and has to delete it when done
	return new TopLevelTable(alloc, ref, m_array, ndx, is_subtable);
}
