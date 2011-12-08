#include "ColumnStringEnum.h"


ColumnStringEnum::ColumnStringEnum(size_t ref_keys, size_t ref_values, Array* parent, size_t pndx, Allocator& alloc)
: m_keys(ref_keys, parent, pndx, alloc), m_values(ref_values, parent, pndx+1, alloc) {}

ColumnStringEnum::ColumnStringEnum(size_t ref_keys, size_t ref_values, const Array* parent, size_t pndx, Allocator& alloc)
: m_keys(ref_keys, parent, pndx, alloc), m_values(ref_values, parent, pndx+1, alloc) {}

ColumnStringEnum::~ColumnStringEnum() {}

void ColumnStringEnum::Destroy() {
	m_keys.Destroy();
	m_values.Destroy();
}

void ColumnStringEnum::UpdateParentNdx(int diff) {
	m_keys.UpdateParentNdx(diff);
	m_values.UpdateParentNdx(diff);
}

size_t ColumnStringEnum::Size() const {
	return m_values.Size();
}

bool ColumnStringEnum::IsEmpty() const {
	return m_values.IsEmpty();
}

const char* ColumnStringEnum::Get(size_t ndx) const {
	assert(ndx < m_values.Size());
	const size_t key_ndx = m_values.Get(ndx);
	return m_keys.Get(key_ndx);
}

bool ColumnStringEnum::Add(const char* value) {
	return Insert(m_values.Size(), value);
}

bool ColumnStringEnum::Set(size_t ndx, const char* value) {
	assert(ndx < m_values.Size());
	assert(value);

	const size_t key_ndx = GetKeyNdx(value);
	return m_values.Set(ndx, key_ndx);
}

bool ColumnStringEnum::Insert(size_t ndx, const char* value) {
	assert(ndx <= m_values.Size());
	assert(value);

	const size_t key_ndx = GetKeyNdx(value);
	return m_values.Insert(ndx, key_ndx);
}

void ColumnStringEnum::Delete(size_t ndx) {
	assert(ndx < m_values.Size());
	m_values.Delete(ndx);
}

void ColumnStringEnum::Clear() {
	// Note that clearing a StringEnum does not remove keys
	m_values.Clear();
}

void ColumnStringEnum::FindAll(Column &res, const char* value, size_t start, size_t end) const {
	// todo, fixme, implement
	return;
}


size_t ColumnStringEnum::Find(const char* value, size_t start, size_t end) const {
	// Find key
	const size_t key_ndx = m_keys.Find(value);
	if (key_ndx == (size_t)-1) return -1;

	return m_values.Find(key_ndx, start, end);
}

size_t ColumnStringEnum::GetKeyNdx(const char* value) {
	const size_t res = m_keys.Find(value);
	if (res != (size_t)-1) return res;
	else {
		const size_t pos = m_keys.Size();
		m_keys.Add(value);
		return pos;
	}
}

#ifdef _DEBUG

bool ColumnStringEnum::Compare(const ColumnStringEnum& c) const {
	if (c.Size() != Size()) return false;

	for (size_t i = 0; i < Size(); ++i) {
		const char* s1 = Get(i);
		const char* s2 = c.Get(i);
		if (strcmp(s1, s2) != 0) return false;
	}

	return true;
}

void ColumnStringEnum::Verify() const {
	m_keys.Verify();
	m_values.Verify();
}

MemStats ColumnStringEnum::Stats() const {
	MemStats stats;
	stats.Add(m_keys.Stats());
	stats.Add(m_values.Stats());
	return stats;
}

#endif //_DEBUG
