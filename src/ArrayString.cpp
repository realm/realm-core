#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug

#include "ArrayString.h"

// MP: What about wide strings (Unicode)?

ArrayString::ArrayString(Allocator& alloc) : Array(COLUMN_NORMAL, NULL, 0, alloc) {
}

ArrayString::~ArrayString() {
}

const char* ArrayString::Get(size_t ndx) const {
	assert(ndx < m_len);

	if (m_width == 0) return "";
	else return (const char*)(m_data + (ndx * m_width));
}

bool ArrayString::Set(size_t ndx, const char* value) {
	assert(ndx < m_len);
	assert(value);

	return Set(ndx, value, strlen(value));
}

bool ArrayString::Set(size_t ndx, const char* value, size_t len) {
	assert(ndx < m_len);
	assert(value);
	assert(len < 64); // otherwise we have to use another column type

	// Special case for lists of zero-length strings
	if (len == 0 && m_width == 0) {
		m_len += 1;
		return true;
	}

	// Calc min column width (incl trailing zero-byte)
	size_t width = 0;
	if (len < 4) width = 4;
	else if (len < 8) width = 8;
	else if (len < 16) width = 16;
	else if (len < 32) width = 32;
	else if (len < 64) width = 64;
	else assert(false);

	// Make room for the new value
	if (width > m_width) {
		const size_t oldwidth = m_width;
		if (!Alloc(m_len, width)) return false;

		// Expand the old values
		int k = (int)m_len;
		while (--k >= 0) {
            const char* v = (const char*)m_data + (k * oldwidth);

            // Move the value
			char* data = (char*)m_data + (k * m_width);
			memmove(data, v, oldwidth);
            memset (data + oldwidth, '\0', (size_t)(m_width - oldwidth)); // pad with zeroes
		}
	}

	// Set the value
	char* data = (char*)m_data + (ndx * m_width);
	memmove(data, value, len);
    memset (data + len, '\0', (size_t)(m_width - len)); // pad with zeroes

	return true;
}

bool ArrayString::Add() {
	return Insert(m_len, "", 0);
}

bool ArrayString::Add(const char* value) {
	return Insert(m_len, value, strlen(value));
}

bool ArrayString::Insert(size_t ndx, const char* value, size_t len) {
	assert(ndx <= m_len);
	assert(value);
	assert(len < 64); // otherwise we have to use another column type

	// Special case for lists of zero-length strings
	if (len == 0 && m_width == 0) {
		m_len += 1;
        MEMREF_GET_HEADER(m_data)->length = m_len;
		return true;
	}

	// Calc min column width (incl trailing zero-byte)
	size_t width = 0;
	if (len < 4) width = 4;
	else if (len < 8) width = 8;
	else if (len < 16) width = 16;
	else if (len < 32) width = 32;
	else if (len < 64) width = 64;
	else assert(false);
	
	const bool doExpand = width > m_width;

	// Make room for the new value
	const size_t oldwidth = m_width;
	if (!Alloc(m_len+1, width)) return false;

	// Move values below insertion (may expand)
	if (doExpand) {
		// Expand the old values
		int k = (int)m_len;
		while (--k >= (int)ndx) {
			const char* v = (const char*)m_data + (k * oldwidth);

			// Move the value
			char* data = (char*)m_data + ((k+1) * m_width);
			memmove(data, v, oldwidth);
            memset (data + oldwidth, '\0', (size_t)(m_width - oldwidth)); // pad with zeroes
		}
	}
	else if (ndx != m_len) {
		// when no expansion, use memmove
		unsigned char* src = m_data + (ndx * m_width);
		unsigned char* dst = src + m_width;
		const size_t count = (m_len - ndx) * m_width;
		memmove(dst, src, count);
	}

	// Set the value
	char* data = (char*)m_data + (ndx * m_width);
	memmove(data, value, len);
    memset (data + len, '\0', (size_t)(m_width - len)); // pad with zeroes

	// Expand values above insertion
	if (doExpand) {
		int k = (int)ndx;
		while (--k >= 0) {
			const char* v = (const char*)m_data + (k * oldwidth);

			// Move the value
			char* data = (char*)m_data + (k * m_width);
			memmove(data, v, oldwidth);
            memset (data + oldwidth, '\0', (size_t)(m_width - oldwidth)); // pad with zeroes
		}
	}

	++m_len;
	return true;
}

void ArrayString::Delete(size_t ndx) {
	assert(ndx < m_len);

    // Update length (also in header)
    --m_len;
    MEMREF_GET_HEADER(m_data)->length = m_len;

	// move data under deletion up
	if (ndx < m_len) {
		char* src = (char*)m_data + ((ndx+1) * m_width);
		char* dst = (char*)m_data + (ndx * m_width);
		const size_t len = (m_len - ndx) * m_width;
		memmove(dst, src, len);
	}
}


bool ArrayString::Alloc(size_t count, size_t width) {
	assert(width <= 64);
	if (width < m_width) width = m_width; // width can only expand

	// Calculate size in bytes
	const size_t len = MEMREF_HEADER_SIZE + (count * width); // always need room for header
	
	if (len > m_capacity) {
		// Try to expand with 50% to avoid to many reallocs
		size_t new_capacity = m_capacity ? (m_capacity + (m_capacity / 2)) : 128;
		if (new_capacity < len) new_capacity = len; 

		// Allocate the space
		MemRef mref;
		if (m_data) mref = m_alloc.ReAlloc(MEMREF_GET_HEADER(m_data), new_capacity);
		else mref = m_alloc.Alloc(new_capacity);

		if (!mref.pointer) return false;

		m_ref = mref.ref;
		m_data = (unsigned char*)mref.pointer + MEMREF_HEADER_SIZE;
		m_capacity = new_capacity;

		// Update ref in parent
		if (m_parent) m_parent->Set(m_parentNdx, mref.ref);
	}

	// Pack width in 3 bits (log2)
	unsigned int w = 0;
	unsigned int b = (unsigned int)width;
	while (b) {++w; b >>= 1;}
	assert(0 <= w && w < 8);

	// Update 8-byte header
	// isNode 1 bit, hasRefs 1 bit, 3 bits unused, width 3 bits, len 3 bytes, capacity 3 bytes
    MemRef::Header* const header = MEMREF_GET_HEADER(m_data);
    header->isNode   = m_isNode;
    header->hasRefs  = m_hasRefs;
    header->width    = w;
    header->length   = count;
    header->capacity = m_capacity;

	m_width = width;
	return true;
}

size_t ArrayString::Find(const char* value) const {
	assert(value);
	return Find(value, strlen(value));
}

size_t ArrayString::Find(const char* value, size_t len) const {
	assert(value);

	if (m_len == 0) return (size_t)-1; // empty list
	if (len >= m_width) return (size_t)-1; // A string can never be wider than the column width

	if (m_width == 0) {
		return 0; 
	}
	else if (m_width == 4) {
		int32_t v = 0;
		memcpy(&v, value, len);

		const int32_t* const t = (int32_t*)m_data;
		for (size_t i = 0; i < m_len; ++i) {
			if (v == t[i]) return i;
		}
	}
	else if (m_width == 8) {
		int64_t v = 0;
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		for (size_t i = 0; i < m_len; ++i) {
			if (v == t[i]) return i;
		}
	}
	else if (m_width == 16) {
		int64_t v[2] = {0,0};
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		const size_t end = m_len * 2;
		for (size_t i = 0; i < end; i += 2) {
			if (v[0] == t[i] && v[1] == t[i+1]) return i/2;
		}
	}
	else if (m_width == 32) {
		int64_t v[4] = {0,0,0,0};
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		const size_t end = m_len * 4;
		for (size_t i = 0; i < end; i += 4) {
			if (v[0] == t[i] && v[1] == t[i+1] && v[2] == t[i+2] && v[3] == t[i+3]) return i/4;
		}
	}
	else if (m_width == 64) {
		int64_t v[8] = {0,0,0,0,0,0,0,0};
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		const size_t end = m_len * 8;
		for (size_t i = 0; i < end; i += 8) {
			if (v[0] == t[i] && v[1] == t[i+1] && v[2] == t[i+2] && v[3] == t[i+3] &&
			    v[4] == t[i+4] && v[5] == t[i+5] && v[6] == t[i+6] && v[7] == t[i+7]) return i/8;
		}
	}
	else assert(false);
		
	return (size_t)-1;
}

#ifdef _DEBUG
#include "stdio.h"

void ArrayString::Stats() const {
	size_t total = 0;
	size_t longest = 0;

	for (size_t i = 0; i < m_len; ++i) {
		const char* str = Get(i);
		const size_t len = strlen(str)+1;

		total += len;
		if (len > longest) longest = len;
	}

	const size_t size = m_len * m_width;
	const size_t zeroes = size - total;
	const size_t zavg = zeroes / m_len;

	printf("Count: %zu\n", m_len);
	printf("Width: %zu\n", m_width);
	printf("Total: %zu\n", size);
	printf("Capacity: %zu\n\n", m_capacity);
	printf("Bytes string: %zu\n", total);
	printf("     longest: %zu\n", longest);
	printf("Bytes zeroes: %zu\n", zeroes);
	printf("         avg: %zu\n", zavg);
}

void ArrayString::ToDot(FILE* f) const {
	const size_t ref = GetRef();

	fprintf(f, "n%zx [label=\"", ref);

	for (size_t i = 0; i < m_len; ++i) {
		if (i > 0) fprintf(f, " | ");

		fprintf(f, "%s", Get(i));
	}
	
	fprintf(f, "\"];\n");
}

#endif //_DEBUG