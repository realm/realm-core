#include "ArrayStringLong.h"
#include "ArrayBlob.h"
#include <assert.h>
#include "win32/types.h" //ssize_t


ArrayStringLong::ArrayStringLong(Array* parent, size_t pndx, Allocator& alloc) : Array(COLUMN_HASREFS, parent, pndx, alloc), m_offsets(COLUMN_NORMAL, NULL, 0, m_alloc), m_blob(NULL, 0, m_alloc) {
	// Add subarrays for long string
	Array::Add(m_offsets.GetRef());
	Array::Add(m_blob.GetRef());
	m_offsets.SetParent((Array*)this, 0);
	m_blob.SetParent((Array*)this, 1);
}

ArrayStringLong::ArrayStringLong(size_t ref, const Array* parent, size_t pndx, Allocator& alloc) : Array(ref, parent, pndx, alloc), m_offsets(Array::Get(0), (Array*)NULL, 0, alloc), m_blob(Array::Get(1), (Array*)NULL, 0, alloc) {
	assert(HasRefs() && !IsNode()); // HasRefs indicates that this is a long string
	assert(Array::Size() == 2);
	assert(m_blob.Size() == m_offsets.IsEmpty() ? 0 : m_offsets.Back());

	m_offsets.SetParent((Array*)this, 0);
	m_blob.SetParent((Array*)this, 1);
}

// Creates new array (but invalid, call UpdateRef to init)
//ArrayStringLong::ArrayStringLong(Allocator& alloc) : Array(alloc) {}

ArrayStringLong::~ArrayStringLong() {
}

bool ArrayStringLong::IsEmpty() const {
	return m_offsets.IsEmpty();
}
size_t ArrayStringLong::Size() const {
	return m_offsets.Size();
}

const char* ArrayStringLong::Get(size_t ndx) const {
	assert(ndx < m_offsets.Size());

	const size_t offset = ndx ? m_offsets.Get(ndx-1) : 0;
	return (const char*)m_blob.Get(offset);
}

void ArrayStringLong::Add(const char* value) {
	Add(value, strlen(value));
}

void ArrayStringLong::Add(const char* value, size_t len) {
	assert(value);

	len += 1; // include trailing null byte
	m_blob.Add((void*)value, len);
	m_offsets.Add(m_offsets.IsEmpty() ? len : m_offsets.Back() + len);
}

void ArrayStringLong::Set(size_t ndx, const char* value) {
	Set(ndx, value, strlen(value));
}

void ArrayStringLong::Set(size_t ndx, const char* value, size_t len) {
	assert(ndx < m_offsets.Size());
	assert(value);

	const size_t start = ndx ? m_offsets.Get(ndx-1) : 0;
	const size_t current_end = m_offsets.Get(ndx);

	len += 1; // include trailing null byte
	const ssize_t diff =  (start + len) - current_end;

	m_blob.Replace(start, current_end, (void*)value, len);
	m_offsets.Adjust(ndx, diff);
}

void ArrayStringLong::Insert(size_t ndx, const char* value) {
	Insert(ndx, value, strlen(value));
}

void ArrayStringLong::Insert(size_t ndx, const char* value, size_t len) {
	assert(ndx <= m_offsets.Size());
	assert(value);

	const size_t pos = ndx ? m_offsets.Get(ndx-1) : 0;
	len += 1; // include trailing null byte

	m_blob.Insert(pos, (void*)value, len);
	m_offsets.Insert(ndx, pos + len);
	m_offsets.Adjust(ndx+1, len);
}

void ArrayStringLong::Delete(size_t ndx) {
	assert(ndx < m_offsets.Size());

	const size_t start = ndx ? m_offsets.Get(ndx-1) : 0;
	const size_t end = m_offsets.Get(ndx);

	m_blob.Delete(start, end);
	m_offsets.Delete(ndx);
	m_offsets.Adjust(ndx, start - end);
}

void ArrayStringLong::Clear() {
	m_blob.Clear();
	m_offsets.Clear();
}

size_t ArrayStringLong::Find(const char* value, size_t start, size_t end) const {
	assert(value);
	return FindWithLen(value, strlen(value), start, end);
}

size_t ArrayStringLong::FindWithLen(const char* value, size_t len, size_t start, size_t end) const {
	assert(value);

	len += 1; // include trailing null byte
	const size_t count = m_offsets.Size();
	size_t offset = 0;
	for (size_t i = 0; i < count; ++i) {
		const size_t end = m_offsets.Get(i);

		// Only compare strings if length matches
		if ((end - offset) == len) {
			const char* const v = (const char*)m_blob.Get(offset);
			if (value[0] == *v && strcmp(value, v) == 0)
				return i;
		}
		offset = end;
	}

	return (size_t)-1; // not found
}