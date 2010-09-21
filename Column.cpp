#include "Column.h"
#include <stdlib.h>
#include <assert.h>

Column::Column() : m_data(0), m_len(0), m_capacity(0), m_width(0) {
	SetWidth(0);
}

Column::~Column() {
	free(m_data);
}

static unsigned int fBitsNeeded(int64_t v) {
	if ((v >> 4) == 0) {
		static int8_t bits[] =  {0, 1, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4};
		return bits[(int8_t)v];
	}

	// first flip all bits if bit 31 is set
	if (v < 0) v = ~v;
	// ... bit 31 is now always zero

	// then check if bits 15-31 used (32b), 7-31 used (16b), else (8b)
	return v >> 31 ? 64 : v >> 15 ? 32 : v >> 7 ? 16 : 8;
}

void Column::Clear() {
	m_len = 0;
	SetWidth(0);
}

int64_t Column::Get64(size_t ndx) const {
	assert(ndx < m_len);

	return (this->*m_getter)(ndx);
}

bool Column::Set64(size_t ndx, int64_t value) {
	assert(ndx < m_len);

	// Make room for the new value
	const size_t width = fBitsNeeded(value);
	if (width > m_width) {
		Getter oldGetter = m_getter;
		if (!Alloc(m_len, width)) return false;
		SetWidth(width);

		// Expand the old values
		int k = m_len;
		while (--k >= 0) {
			const int64_t v = (this->*oldGetter)(k);
			(this->*m_setter)(k, v);
		}
	}

	// Set the value
	(this->*m_setter)(ndx, value);

	return true;
}

bool Column::Add64(int64_t value) {
	return Insert64(m_len, value);
}

bool Column::Insert64(size_t ndx, int64_t value) {
	assert(ndx <= m_len);

	Getter getter = m_getter;

	// Make room for the new value
	const size_t width = fBitsNeeded(value);
	const bool doExpand = (width > m_width);
	if (doExpand) {
		if (!Alloc(m_len+1, width)) return false;
		SetWidth(width);
	}
	else {
		if (!Alloc(m_len+1, m_width)) return false;
	}

	// Move values below insertion (may expand)
	// TODO: if byte sized and no expansion, use memmove
	if (doExpand || m_width < 8) {
		int k = m_len;
		while (--k >= (int)ndx) {
			const int64_t v = (this->*getter)(k);
			(this->*m_setter)(k+1, v);
		}
	}
	else if (ndx != m_len) {
		// when byte sized and no expansion, use memmove
		const size_t w = (m_width == 64) ? 8 : (m_width == 32) ? 4 : (m_width == 16) ? 2 : 1;
		unsigned char* src = m_data + (ndx * w);
		unsigned char* dst = src + w;
		const size_t count = (m_len - ndx) * w;
		memmove(dst, src, count);
	}

	// Insert the new value
	(this->*m_setter)(ndx, value);

	// Expand values above insertion
	if (doExpand) {
		int k = ndx;
		while (--k >= 0) {
			const int64_t v = (this->*getter)(k);
			(this->*m_setter)(k, v);
		}
	}

	++m_len;
	return true;
}

void Column::Delete(size_t ndx) {
	assert(ndx < m_len);

	// Move values below deletion up
	if (m_width < 8) {
		for (size_t i = ndx+1; i < m_len; ++i) {
			const int64_t v = (this->*m_getter)(i);
			(this->*m_setter)(i-1, v);
		}
	}
	else if (ndx < m_len-1) {
		// when byte sized, use memmove
		const size_t w = (m_width == 64) ? 8 : (m_width == 32) ? 4 : (m_width == 16) ? 2 : 1;
		unsigned char* dst = m_data + (ndx * w);
		unsigned char* src = dst + w;
		const size_t count = (m_len - ndx - 1) * w;
		memmove(dst, src, count);
	}

	--m_len;
}

size_t Column::Find(int64_t value, size_t start, size_t end) const {
	if (IsEmpty()) return -1;
	if (end == -1) end = m_len;
	if (start == end) return -1;

	assert(start < m_len && end <= m_len && start < end);

	// If the value is wider than the column
	// then we know it can't be there
	const size_t width = fBitsNeeded(value);
	if (width > m_width) return -1;

	// Do optimized search based on column width
	if (m_width == 0) {
		return start; // value can only be zero
	}
	else if (m_width == 8) {
		const int8_t v = (int8_t)value;
		const int8_t* p = (const int8_t*)m_data + start;
		const int8_t* const e = (const int8_t*)m_data + end;
		while (p < e) {
			if (*p == v) return p - (const int8_t*)m_data;
			++p;
		}
	}
	else if (m_width == 16) {
		const int16_t v = (int16_t)value;
		const int16_t* p = (const int16_t*)m_data + start;
		const int16_t* const e = (const int16_t*)m_data + end;
		while (p < e) {
			if (*p == v) return p - (const int16_t*)m_data;
			++p;
		}
	}
	else if (m_width == 32) {
		const int32_t v = (int32_t)value;
		const int32_t* p = (const int32_t*)m_data + start;
		const int32_t* const e = (const int32_t*)m_data + end;
		while (p < e) {
			if (*p == v) return p - (const int32_t*)m_data;
			++p;
		}
	}
	else if (m_width == 64) {
		const int64_t v = (int64_t)value;
		const int64_t* p = (const int64_t*)m_data + start;
		const int64_t* const e = (const int64_t*)m_data + end;
		while (p < e) {
			if (*p == v) return p - (const int64_t*)m_data;
			++p;
		}
	}
	else {
		// Naive search
		for (size_t i = start; i < end; ++i) {
			const int64_t v = (this->*m_getter)(i);
			if (v == value) return i;
		}
	}

	return -1; // not found
}

int64_t Column::Get_0b(size_t ndx) const {
	return 0;
}

int64_t Column::Get_1b(size_t ndx) const {
	const size_t offset = ndx >> 3;
	return (m_data[offset] >> (ndx & 7)) & 0x01;
}

int64_t Column::Get_2b(size_t ndx) const {
	const size_t offset = ndx >> 2;
	return (m_data[offset] >> ((ndx & 3) << 1)) & 0x03;
}

int64_t Column::Get_4b(size_t ndx) const {
	const size_t offset = ndx >> 1;
	return (m_data[offset] >> ((ndx & 1) << 2)) & 0x0F;
}

int64_t Column::Get_8b(size_t ndx) const {
	return *((const signed char*)(m_data + ndx));
}

int64_t Column::Get_16b(size_t ndx) const {
	const size_t offset = ndx * 2;
	return *(const int16_t*)(m_data + offset);
}

int64_t Column::Get_32b(size_t ndx) const {
	const size_t offset = ndx * 4;
	return *(const int32_t*)(m_data + offset);
}

int64_t Column::Get_64b(size_t ndx) const {
	const size_t offset = ndx * 8;
	return *(const int64_t*)(m_data + offset);
}

void Column::Set_0b(size_t, int64_t) {
}

void Column::Set_1b(size_t ndx, int64_t value) {
	const size_t offset = ndx >> 3;
	ndx &= 7;

	uint8_t* p = &m_data[offset];
	*p = (*p &~ (1 << ndx)) | (((uint8_t)value & 1) << ndx);
}

void Column::Set_2b(size_t ndx, int64_t value) {
	const size_t offset = ndx >> 2;
	const int n = (ndx & 3) << 1;

	uint8_t* p = &m_data[offset];
	*p = (*p &~ (0x03 << n)) | (((uint8_t)value & 0x03) << n);
}

void Column::Set_4b(size_t ndx, int64_t value) {
	const size_t offset = ndx >> 1;
	const int n = (ndx & 1) << 2;

	uint8_t* p = &m_data[offset];
	*p = (*p &~ (0x0F << n)) | (((uint8_t)value & 0x0F) << n);
}

void Column::Set_8b(size_t ndx, int64_t value) {
	*((char*)m_data + ndx) = (char)value;
}

void Column::Set_16b(size_t ndx, int64_t value) {
	const size_t offset = ndx * 2;
	*(int16_t*)(m_data + offset) = (int16_t)value;
}

void Column::Set_32b(size_t ndx, int64_t value) {
	const size_t offset = ndx * 4;
	*(int32_t*)(m_data + offset) = (int32_t)value;
}

void Column::Set_64b(size_t ndx, int64_t value) {
	const size_t offset = ndx * 8;
	*(int64_t*)(m_data + offset) = value;
}

bool Column::Reserve(size_t count, size_t width) {
	return Alloc(count, width);
}

bool Column::Alloc(size_t count, size_t width) {
	if (width == 0) return true;

	// Calculate size in bytes
	size_t len = 0;
	if (width == 1) {
		len = count >> 3;
		if (count & 0x07) ++len;
	}
	else if (width == 2) {
		len = count >> 2;
		if (count & 0x03) ++len;
	}
	else if (width == 4) {
		len = count >> 1;
		if (count & 0x01) ++len;
	}
	else {
		assert(width == 8 || width == 16 || width == 32 || width == 64);
		len = count * (width >> 3);
	}

	if (len > m_capacity) {
		// Allocate the space
		unsigned char* data = NULL;
		if (m_data) data = (unsigned char*)realloc(m_data, len);
		else data = (unsigned char*)malloc(len);

		if (!data) return false;

		m_data = data;
		m_capacity = len;
	}

	return true;
}

void Column::SetWidth(size_t width) {
	if (width == 0) {
		m_getter = &Column::Get_0b;
		m_setter = &Column::Set_0b;
	}
	else if (width == 1) {
		m_getter = &Column::Get_1b;
		m_setter = &Column::Set_1b;
	}
	else if (width == 2) {
		m_getter = &Column::Get_2b;
		m_setter = &Column::Set_2b;
	}
	else if (width == 4) {
		m_getter = &Column::Get_4b;
		m_setter = &Column::Set_4b;
	}
	else if (width == 8) {
		m_getter = &Column::Get_8b;
		m_setter = &Column::Set_8b;
	}
	else if (width == 16) {
		m_getter = &Column::Get_16b;
		m_setter = &Column::Set_16b;
	}
	else if (width == 32) {
		m_getter = &Column::Get_32b;
		m_setter = &Column::Set_32b;
	}
	else if (width == 64) {
		m_getter = &Column::Get_64b;
		m_setter = &Column::Set_64b;
	}
	else {
		assert(false);
	}

	m_width = width;
}