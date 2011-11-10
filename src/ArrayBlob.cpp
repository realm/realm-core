#include "ArrayBlob.h"
#include <assert.h>

ArrayBlob::ArrayBlob(Array* parent, size_t pndx, Allocator& alloc) : Array(COLUMN_NORMAL, parent, pndx, alloc) {
}

ArrayBlob::ArrayBlob(size_t ref, const Array* parent, size_t pndx, Allocator& alloc) : Array(ref, parent, pndx, alloc) {
}

// Creates new array (but invalid, call UpdateRef to init)
ArrayBlob::ArrayBlob(Allocator& alloc) : Array(alloc) {
}

ArrayBlob::~ArrayBlob() {
}

const uint8_t* ArrayBlob::Get(size_t pos) const {
	return m_data + pos;
}

void ArrayBlob::Add(void* data, size_t len) {
	Replace(m_len, m_len, data, len);
}

void ArrayBlob::Insert(size_t pos, void* data, size_t len) {
	Replace(pos, pos, data, len);
}

void ArrayBlob::Replace(size_t start, size_t end, void* data, size_t len) {
	assert(start <= end);
	assert(end <= m_len);
	assert(len == 0 || data);

	CopyOnWrite();

	// Reallocate if needed
	const size_t gapsize = end - start;
	const size_t newsize = (m_len - gapsize) + len;
	Alloc(newsize, 1); // also updates header

	// Resize previous space to fit new data
	// (not needed if we append to end)
	if (start != m_len && gapsize != len) {
		const size_t dst = start + len;
		const size_t src_len = m_len - end;
		memmove(m_data + dst, m_data + end, src_len);
	}

	// Insert the data
	memcpy(m_data + start, data, len);

	m_len = newsize;
}

void ArrayBlob::Delete(size_t start, size_t end) {
	Replace(start, end, NULL, 0);
}

void ArrayBlob::Clear() {
	Replace(0, m_len, NULL, 0);
}

size_t ArrayBlob::CalcByteLen(size_t count, size_t) const {
	return 8 + count; // include room for header
}

size_t ArrayBlob::Write(std::ostream& out) const {
	// Calculate how many bytes the array takes up
	const size_t len = 8 + m_len;

	// Write header first
	// TODO: replace capacity with checksum
	out.write((const char*)m_data-8, 8);

	// Write array
	const size_t arrayByteLen = len - 8;
	if (arrayByteLen) out.write((const char*)m_data, arrayByteLen);

	// Pad so next block will be 64bit aligned
	const char pad[8] = {0,0,0,0,0,0,0,0};
	const size_t rest = (~len & 0x7)+1;

	if (rest < 8) {
		out.write(pad, rest);
		return len + rest;
	}
	else return len; // Return number of bytes written
}