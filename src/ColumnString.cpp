#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug

#include "Column.h"

// Pre-declare local functions
bool IsNodeRef(size_t ref, Allocator& alloc);

bool IsNodeRef(size_t ref, Allocator& alloc) {
	const uint8_t* const header = (uint8_t*)alloc.Translate(ref);
	const bool isNode = (header[0] & 0x80) != 0;
	return isNode;
}

AdaptiveStringColumn::AdaptiveStringColumn(Allocator& alloc) {
	m_array = new ArrayString(NULL, 0, alloc);
}

AdaptiveStringColumn::AdaptiveStringColumn(size_t ref, Array* parent, size_t pndx, Allocator& alloc) {
	if (IsNodeRef(ref, alloc)) {
		m_array = new Array(ref, parent, pndx, alloc);
	}
	else {
		m_array = new ArrayString(ref, parent, pndx, alloc);
	}
}

AdaptiveStringColumn::AdaptiveStringColumn(size_t ref, const Array* parent, size_t pndx, Allocator& alloc) {
	if (IsNodeRef(ref, alloc)) {
		m_array = new Array(ref, parent, pndx, alloc);
	}
	else {
		m_array = new ArrayString(ref, parent, pndx, alloc);
	}
}

AdaptiveStringColumn::~AdaptiveStringColumn() {
}

void AdaptiveStringColumn::Destroy() {
	if (IsNode()) m_array->Destroy();
	else ((ArrayString*)m_array)->Destroy();
}


void AdaptiveStringColumn::UpdateRef(size_t ref) {
	assert(IsNodeRef(ref, m_array->GetAllocator())); // Can only be called when creating node

	if (IsNode()) m_array->UpdateRef(ref);
	else {
		// Replace the string array with int array for node
		Array* array = new Array(ref, m_array->GetParent(), m_array->GetParentNdx(), m_array->GetAllocator());
		delete m_array;
		m_array = array;
	}
}

bool AdaptiveStringColumn::IsEmpty() const {
	if (!IsNode()) return ((ArrayString*)m_array)->IsEmpty();
	else {
		const Array offsets = NodeGetOffsets();
		return offsets.IsEmpty();
	}
}

size_t AdaptiveStringColumn::Size() const {
	if (!IsNode()) return ((ArrayString*)m_array)->Size();
	else {
		const Array offsets = NodeGetOffsets();
		return offsets.IsEmpty() ? 0 : (size_t)offsets.Back();
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
	else ((ArrayString*)m_array)->Clear();
}

const char* AdaptiveStringColumn::Get(size_t ndx) const {
	return TreeGet<const char*, AdaptiveStringColumn>(ndx);
}

bool AdaptiveStringColumn::Set(size_t ndx, const char* value) {
	return TreeSet<const char*, AdaptiveStringColumn>(ndx, value);
}

bool AdaptiveStringColumn::Add(const char* value) {
	return Insert(Size(), value);
}

bool AdaptiveStringColumn::Insert(size_t ndx, const char* value) {
	return TreeInsert<const char*, AdaptiveStringColumn>(ndx, value);
}

void AdaptiveStringColumn::Delete(size_t ndx) {
	TreeDelete<const char*, AdaptiveStringColumn>(ndx);
}

size_t AdaptiveStringColumn::Find(const char* value, size_t, size_t) const {
	assert(value);
	return TreeFind<const char*, AdaptiveStringColumn>(value, 0, -1);
}

size_t AdaptiveStringColumn::Write(std::ostream& out, size_t& pos) const {
	return TreeWrite<const char*, AdaptiveStringColumn>(out, pos);
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
