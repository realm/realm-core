#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug
#include "utilities.h"
#include "Column.h"
#include "ArrayString.h"

// Pre-declare local functions
size_t round_up(size_t len);

size_t round_up(size_t len) {
	size_t width = 0;
	if (len == 0)     width = 0;
	else if (len < 3) width = 4;
	else {
		width = len;
		width |= width >> 1;
		width |= width >> 2;
		width |= width >> 4;
		++width;
	}
	return width;
}

ArrayString::ArrayString(Array* parent, size_t pndx, Allocator& alloc) : Array(COLUMN_NORMAL, parent, pndx, alloc) {
}

ArrayString::ArrayString(size_t ref, const Array* parent, size_t pndx, Allocator& alloc) : Array(ref, parent, pndx, alloc) {
}

// Creates new array (but invalid, call UpdateRef to init)
ArrayString::ArrayString(Allocator& alloc) : Array(alloc) {
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

	// Check if we need to copy before modifying
	if (!CopyOnWrite()) return false;

	// Calc min column width (incl trailing zero-byte)
	size_t width = round_up(len);

	// Make room for the new value
	if (width > m_width) {
		const size_t oldwidth = m_width;
		m_width = width;
		if (!Alloc(m_len, m_width)) return false;

		// Expand the old values
		int k = (int)m_len;
		while (--k >= 0) {
			const char* v = (const char*)m_data + (k * oldwidth);

			// Move the value
			char* data = (char*)m_data + (k * m_width);
			char* const end = data + m_width;
			memmove(data, v, oldwidth);
			for (data += oldwidth; data < end; ++data) {
				*data = '\0'; // pad with zeroes
			}
		}
	}

	// Set the value
	char* data = (char*)m_data + (ndx * m_width);
	char* const end = data + m_width;
	memmove(data, value, len);
	for (data += len; data < end; ++data) {
		*data = '\0'; // pad with zeroes
	}

	return true;
}

bool ArrayString::Add() {
	return Insert(m_len, "", 0);
}

bool ArrayString::Add(const char* value) {
	return Insert(m_len, value, strlen(value));
}

bool ArrayString::Insert(size_t ndx, const char* value) {
	return Insert(ndx, value, strlen(value));
}



bool ArrayString::Insert(size_t ndx, const char* value, size_t len) {
	assert(ndx <= m_len);
	assert(value);
	assert(len < 64); // otherwise we have to use another column type

	// Check if we need to copy before modifying
	if (!CopyOnWrite()) return false;

	// Calc min column width (incl trailing zero-byte)
	size_t width = round_up(len);
	
	const bool doExpand = width > m_width;

	// Make room for the new value
	const size_t oldwidth = m_width;
	if (doExpand) m_width = width;
	if (!Alloc(m_len+1, m_width)) return false;

	// Move values below insertion (may expand)
	if (doExpand) {
		// Expand the old values
		int k = (int)m_len;
		while (--k >= (int)ndx) {
			const char* v = (const char*)m_data + (k * oldwidth);

			// Move the value
			char* data = (char*)m_data + ((k+1) * m_width);
			char* const end = data + m_width;
			memmove(data, v, oldwidth);
			for (data += oldwidth; data < end; ++data) {
				*data = '\0'; // pad with zeroes
			}
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
	char* const end = data + m_width;
	memmove(data, value, len);
	for (data += len; data < end; ++data) {
		*data = '\0'; // pad with zeroes
	}

	// Expand values above insertion
	if (doExpand) {
		int k = (int)ndx;
		while (--k >= 0) {
			const char* v = (const char*)m_data + (k * oldwidth);

			// Move the value
			char* data = (char*)m_data + (k * m_width);
			char* const end = data + m_width;
			memmove(data, v, oldwidth);
			for (data += oldwidth; data < end; ++data) {
				*data = '\0'; // pad with zeroes
			}
		}
	}

	++m_len;
	return true;
}

void ArrayString::Delete(size_t ndx) {
	assert(ndx < m_len);

	// Check if we need to copy before modifying
	CopyOnWrite();

	--m_len;

	// move data under deletion up
	if (ndx < m_len) {
		char* src = (char*)m_data + ((ndx+1) * m_width);
		char* dst = (char*)m_data + (ndx * m_width);
		const size_t len = (m_len - ndx) * m_width;
		memmove(dst, src, len);
	}

	// Update length in header
	set_header_len(m_len);
}

size_t ArrayString::CalcByteLen(size_t count, size_t width) const {
	return 8 + (count * width);
}

size_t ArrayString::Find(const char* value, size_t start, size_t end) const {
	assert(value);
	return FindWithLen(value, strlen(value), start, end);
}

void ArrayString::FindAll(Column& result, const char* value) {
	assert(value);
	FindAll(result, value, strlen(value));
}

void ArrayString::FindAll(Column& result, const char* value, size_t len) {
	assert(value);
	size_t first = (size_t)-1;
	do {
		first = FindWithLen(value, len, first + 1, m_len);
		if(first != (size_t)-1)
		result.Add(first);
	} while (first != (size_t)-1);

}

size_t ArrayString::FindWithLen(const char* value, size_t len, size_t start, size_t end) const {
	assert(value);

	if (end == (size_t)-1) end = m_len;
	if (start == end) return (size_t)-1;
	assert(start < m_len && end <= m_len && start < end);
	if (m_len == 0) return (size_t)-1; // empty list
	if (len >= m_width) return (size_t)-1; // A string can never be wider than the column width

#define OPTIMIZE_TEST

#ifdef OPTIMIZE_TEST

	/*
	// benchmark that you can place in testarraystring.cpp or whereever
	c.Add("ynnvnsdsg");
	c.Add("ujnnljd");
	c.Add("dfgfgffggg gngs");
	c.Add("hpgsdppp");
	c.Add("sufy n");
	c.Add("psdpppdfgg");
	printf("finding");
	for(uint64_t i = 0; i < 200*1000*1000; i++)
		volatile size_t t = c.Find("psdppp")
*/

/*
// performs + instead of * in address generation
	char v = *value;
	for (unsigned char *r = m_data + start * m_width; r < m_data + end * m_width; r += m_width) { 
		if (v == *r) { 
			if(strncmp(value, (const char *)r, len) == 0) {
				return (r - m_data) / m_width;
			}
		}
	}
	return (size_t)-1;
*/



// todo, ensure behaves as expected when m_width = 0
	// 50 - 80% faster in some cases (few short strings, such as column names), same speed in most others, never slower
	for (size_t i = start; i < end; ++i) {
		if (value[0] == m_data[i * m_width] && value[len] == m_data[i * m_width + len]) { 
			if(strncmp(value, (const char *)m_data + i * m_width, len) == 0)
				return i;
			}
		}
	return (size_t)-1;

#else
	if (m_width == 0) {
		return 0; 
	}
	else if (m_width == 4) {
		int32_t v = 0;
		memcpy(&v, value, len);

		const int32_t* const t = (int32_t*)m_data;
		for (size_t i = start; i < end; ++i) {
			if (v == t[i]) return i;
		}
	}
	else if (m_width == 8) {
		int64_t v = 0;
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		for (size_t i = start; i < end; ++i) {
			if (v == t[i]) return i;
		}
	}
	else if (m_width == 16) {
		int64_t v[2] = {0,0};
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		const size_t end2 = end * 2;
		for (size_t i = start; i < end2; i += 2) {
			if (v[0] == t[i] && v[1] == t[i+1]) return i/2;
		}
	}
	else if (m_width == 32) {
		int64_t v[4] = {0,0,0,0};
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		const size_t end2 = end * 4;
		for (size_t i = start; i < end2; i += 4) {
			if (v[0] == t[i] && v[1] == t[i+1] && v[2] == t[i+2] && v[3] == t[i+3]) return i/4;
		}
	}
	else if (m_width == 64) {
		int64_t v[8] = {0,0,0,0,0,0,0,0};
		memcpy(&v, value, len);

		const int64_t* const t = (int64_t*)m_data;
		const size_t end2 = end * 8;
		for (size_t i = start; i < end2; i += 8) {
			if (v[0] == t[i] && v[1] == t[i+1] && v[2] == t[i+2] && v[3] == t[i+3] &&
			    v[4] == t[i+4] && v[5] == t[i+5] && v[6] == t[i+6] && v[7] == t[i+7]) return i/8;
		}
	}
	else assert(false);
		
	return (size_t)-1;

#endif

}

size_t ArrayString::Write(std::ostream& out) const {
	// Calculate how many bytes the array takes up
	const size_t len = 8 + (m_len * m_width);

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

#ifdef _DEBUG
#include "stdio.h"

bool ArrayString::Compare(const ArrayString& c) const {
	if (c.Size() != Size()) return false;

	for (size_t i = 0; i < Size(); ++i) {
		if (strcmp(Get(i), c.Get(i)) != 0) return false;
	}

	return true;
}

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