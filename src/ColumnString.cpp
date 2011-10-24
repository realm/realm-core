#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug

#include "Column.h"

AdaptiveStringColumn::AdaptiveStringColumn(Allocator& alloc) : m_array(NULL, 0, alloc) {
}

AdaptiveStringColumn::AdaptiveStringColumn(size_t ref, Array* parent, size_t pndx, Allocator& alloc)
: m_array(ref, parent, pndx, alloc) {
}

AdaptiveStringColumn::~AdaptiveStringColumn() {
}

const char* AdaptiveStringColumn::Get(size_t ndx) const {
	return m_array.Get(ndx);
}

bool AdaptiveStringColumn::Set(size_t ndx, const char* value) {
	return Set(ndx, value, strlen(value));
}

bool AdaptiveStringColumn::Set(size_t ndx, const char* value, size_t len) {
	return m_array.Set(ndx, value, len);
}

bool AdaptiveStringColumn::Add() {
	return m_array.Add();
}

bool AdaptiveStringColumn::Add(const char* value) {
	return m_array.Add(value);
}

bool AdaptiveStringColumn::Insert(size_t ndx, const char* value, size_t len) {
	return m_array.Insert(ndx, value, len);
}

void AdaptiveStringColumn::Delete(size_t ndx) {
	m_array.Delete(ndx);
}

size_t AdaptiveStringColumn::Find(const char* value) const {
	assert(value);
	return Find(value, strlen(value));
}

size_t AdaptiveStringColumn::Find(const char* value, size_t len) const {
	assert(value);
	return m_array.Find(value, len);
}


size_t AdaptiveStringColumn::Write(std::ostream& out, size_t& pos) const {
	const size_t arrayPos = pos;
	pos += m_array.Write(out);
	return arrayPos;
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
	m_array.ToDot(f);
}

#endif //_DEBUG
