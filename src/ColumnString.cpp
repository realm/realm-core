#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug

#include "ColumnString.h"
//#include "ArrayString.h"
#include "ArrayStringLong.h"

// Pre-declare local functions
ColumnDef GetTypeFromArray(size_t ref, Allocator& alloc);

ColumnDef GetTypeFromArray(size_t ref, Allocator& alloc) {
	const uint8_t* const header = (uint8_t*)alloc.Translate(ref);
	const bool isNode = (header[0] & 0x80) != 0;
	const bool hasRefs  = (header[0] & 0x40) != 0;

	if (isNode) return COLUMN_NODE;
	else if (hasRefs) return COLUMN_HASREFS;
	else return COLUMN_NORMAL;
}

AdaptiveStringColumn::AdaptiveStringColumn(Allocator& alloc) {
	m_array = new ArrayString(NULL, 0, alloc);
}

AdaptiveStringColumn::AdaptiveStringColumn(size_t ref, Array* parent, size_t pndx, Allocator& alloc) {
	const ColumnDef type = GetTypeFromArray(ref, alloc);
	if (type == COLUMN_NODE) {
		m_array = new Array(ref, parent, pndx, alloc);
	}
	else if (type == COLUMN_HASREFS) {
		m_array = new ArrayStringLong(ref, parent, pndx, alloc);
	}
	else {
		m_array = new ArrayString(ref, parent, pndx, alloc);
	}
}

AdaptiveStringColumn::AdaptiveStringColumn(size_t ref, const Array* parent, size_t pndx, Allocator& alloc) {
	const ColumnDef type = GetTypeFromArray(ref, alloc);
	if (type == COLUMN_NODE) {
		m_array = new Array(ref, parent, pndx, alloc);
	}
	else if (type == COLUMN_HASREFS) {
		m_array = new ArrayStringLong(ref, parent, pndx, alloc);
	}
	else {
		m_array = new ArrayString(ref, parent, pndx, alloc);
	}
}

AdaptiveStringColumn::~AdaptiveStringColumn() {
}

void AdaptiveStringColumn::Destroy() {
	if (IsNode()) m_array->Destroy();
	else if (IsLongStrings()) {
		((ArrayStringLong*)m_array)->Destroy();
	}
	else ((ArrayString*)m_array)->Destroy();
}


void AdaptiveStringColumn::UpdateRef(size_t ref) {
	assert(GetTypeFromArray(ref, m_array->GetAllocator()) == COLUMN_NODE); // Can only be called when creating node

	if (IsNode()) m_array->UpdateRef(ref);
	else {
		// Replace the string array with int array for node
		Array* array = new Array(ref, m_array->GetParent(), m_array->GetParentNdx(), m_array->GetAllocator());
		delete m_array;
		m_array = array;
	}
}

bool AdaptiveStringColumn::IsEmpty() const {
	if (IsNode()) {
		const Array offsets = NodeGetOffsets();
		return offsets.IsEmpty();
	}
	else if (IsLongStrings()) {
		return ((ArrayStringLong*)m_array)->IsEmpty();
	}
	else {
		return ((ArrayString*)m_array)->IsEmpty();
	}
}

size_t AdaptiveStringColumn::Size() const {
	if (IsNode())  {
		const Array offsets = NodeGetOffsets();
		const size_t size = offsets.IsEmpty() ? 0 : (size_t)offsets.Back();
		return size;
	}
	else if (IsLongStrings()) {
		return ((ArrayStringLong*)m_array)->Size();
	}
	else {
		return ((ArrayString*)m_array)->Size();
	}
}

void AdaptiveStringColumn::Clear() {
	if (m_array->IsNode()) {
		// Revert to string array
		m_array->Destroy();
		Array* array = new ArrayString(m_array->GetParent(), m_array->GetParentNdx(), m_array->GetAllocator());
		delete m_array;
		m_array = array;
	}
	else if (IsLongStrings()) {
		((ArrayStringLong*)m_array)->Clear();
	}
	else ((ArrayString*)m_array)->Clear();
}

const char* AdaptiveStringColumn::Get(size_t ndx) const {
	assert(ndx < Size());
	return TreeGet<const char*, AdaptiveStringColumn>(ndx);
}

bool AdaptiveStringColumn::Set(size_t ndx, const char* value) {
	assert(ndx < Size());
	return TreeSet<const char*, AdaptiveStringColumn>(ndx, value);
}

bool AdaptiveStringColumn::Add(const char* value) {
	return Insert(Size(), value);
}

bool AdaptiveStringColumn::Insert(size_t ndx, const char* value) {
	assert(ndx <= Size());
	return TreeInsert<const char*, AdaptiveStringColumn>(ndx, value);
}

void AdaptiveStringColumn::Delete(size_t ndx) {
	assert(ndx < Size());
	TreeDelete<const char*, AdaptiveStringColumn>(ndx);
}

size_t AdaptiveStringColumn::Find(const char* value, size_t, size_t) const {
	assert(value);
	return TreeFind<const char*, AdaptiveStringColumn>(value, 0, -1);
}

size_t AdaptiveStringColumn::Write(std::ostream& out, size_t& pos) const {
	return TreeWrite<const char*, AdaptiveStringColumn>(out, pos);
}

const char* AdaptiveStringColumn::LeafGet(size_t ndx) const {
	if (IsLongStrings()) {
		return ((ArrayStringLong*)m_array)->Get(ndx);
	}
	else {
		return ((ArrayString*)m_array)->Get(ndx);
	}
}

bool AdaptiveStringColumn::LeafSet(size_t ndx, const char* value) {
	// Easy to set if the strings fit
	const size_t len = strlen(value);
	if (IsLongStrings()) {
		((ArrayStringLong*)m_array)->Set(ndx, value, len);
		return true;
	}
	else if (len < 64) {
		return ((ArrayString*)m_array)->Set(ndx, value);
	}

	// Replace string array with long string array
	ArrayStringLong* const newarray = new ArrayStringLong((Array*)NULL, 0, m_array->GetAllocator());

	// Copy strings to new array
	ArrayString* const oldarray = (ArrayString*)m_array;
	for (size_t i = 0; i < oldarray->Size(); ++i) {
		newarray->Add(oldarray->Get(i));
	}
	newarray->Set(ndx, value, len);

	// Update parent to point to new array
	Array* const parent = oldarray->GetParent();
	if (parent) {
		const size_t pndx = oldarray->GetParentNdx();
		parent->Set(pndx, newarray->GetRef());
		newarray->SetParent(parent, pndx);
	}

	// Replace string array with long string array
	m_array = (Array*)newarray;
	oldarray->Destroy();
	delete oldarray;

	return true;}

bool AdaptiveStringColumn::LeafInsert(size_t ndx, const char* value) {
	// Easy to insert if the strings fit
	const size_t len = strlen(value);
	if (IsLongStrings()) {
		((ArrayStringLong*)m_array)->Insert(ndx, value, len);
		return true;
	}
	else if (len < 64) {
		return ((ArrayString*)m_array)->Insert(ndx, value);
	}

	// Replace string array with long string array
	ArrayStringLong* const newarray = new ArrayStringLong((Array*)NULL, 0, m_array->GetAllocator());

	// Copy strings to new array
	ArrayString* const oldarray = (ArrayString*)m_array;
	for (size_t i = 0; i < oldarray->Size(); ++i) {
		newarray->Add(oldarray->Get(i));
	}
	newarray->Insert(ndx, value, len);

	// Update parent to point to new array
	Array* const parent = oldarray->GetParent();
	if (parent) {
		const size_t pndx = oldarray->GetParentNdx();
		parent->Set(pndx, newarray->GetRef());
		newarray->SetParent(parent, pndx);
	}

	// Replace string array with long string array
	m_array = (Array*)newarray;
	oldarray->Destroy();
	delete oldarray;

	return true;
}

void AdaptiveStringColumn::LeafDelete(size_t ndx) {
	if (IsLongStrings()) {
		((ArrayStringLong*)m_array)->Delete(ndx);
	}
	else {
		((ArrayString*)m_array)->Delete(ndx);
	}
}

size_t AdaptiveStringColumn::LeafWrite(std::ostream& out, size_t& pos) const {
	if (IsLongStrings()) {
		return ((ArrayStringLong*)m_array)->Write(out, pos);
	}
	else {
		const size_t leaf_pos = pos;
		pos += ((ArrayString*)m_array)->Write(out);
		return leaf_pos;
	}
}

#ifdef _DEBUG
#include <cstring> // strcmp()

bool AdaptiveStringColumn::Compare(const AdaptiveStringColumn& c) const {
	if (c.Size() != Size()) return false;

	for (size_t i = 0; i < Size(); ++i) {
		const char* s1 = Get(i);
		const char* s2 = c.Get(i);
		if (strcmp(s1, s2) != 0) return false;
	}

	return true;
}

void AdaptiveStringColumn::ToDot(FILE* f, bool) const {
	m_array->ToDot(f);
}

#endif //_DEBUG
