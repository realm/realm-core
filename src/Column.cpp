#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug output
#include <climits> // size_t

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <cstdint> // unint8_t etc
#endif

#include "Column.h"

#define MAX_LIST_SIZE 1000

Column::Column()
: m_data(NULL), m_len(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false), m_index(NULL), m_parent(NULL), m_parentNdx(0) {
	SetWidth(0);
}

Column::Column(ColumnDef type, Column* parent, size_t pndx)
: m_data(NULL), m_len(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false), m_index(NULL), m_parent(parent), m_parentNdx(pndx) {
	if (type == COLUMN_NODE) m_isNode = m_hasRefs = true;
	else if (type == COLUMN_HASREFS)    m_hasRefs = true;

	Alloc(0, 0);
	SetWidth(0);

	// Add subcolumns for nodes
	if (m_isNode) {
		const Column offsets(COLUMN_NORMAL);
		const Column refs(COLUMN_HASREFS);
		ListAdd((intptr_t)offsets.GetRef());
		ListAdd((intptr_t)refs.GetRef());
	}
}

Column::Column(void* ref)
: m_index(NULL), m_parent(NULL), m_parentNdx(0) {
	Create(ref);
}

Column::Column(void* ref, Column* parent, size_t pndx)
: m_index(NULL), m_parent(parent), m_parentNdx(pndx) {
	Create(ref);
}

Column::Column(void* ref, const Column* parent, size_t pndx)
: m_index(NULL), m_parent(const_cast<Column*>(parent)), m_parentNdx(pndx) {
	Create(ref);
}

Column::Column(const Column& column) : m_index(NULL), m_parent(column.m_parent), m_parentNdx(column.m_parentNdx) {
	Create(column.GetRef());
}

Column& Column::operator=(const Column& column) {
	m_parent = column.m_parent;
	m_parentNdx = column.m_parentNdx;
	Create(column.GetRef());
	return *this;
}

bool Column::operator==(const Column& column) const {
	if (m_data != column.m_data) return false;
	if (m_isNode != column.m_isNode) return false;
	if (m_hasRefs != column.m_hasRefs) return false;
	if (m_width != column.m_width) return false;
	if (m_len != column.m_len) return false;
	if (m_capacity != column.m_capacity) return false;

	return true;
}

Column::~Column() {
}

void Column::Create(void* ref) {
	assert(ref);
	uint8_t* const header = (uint8_t*)ref;

	// parse the 8byte header
	m_isNode   = (header[0] & 0x80) != 0;
	m_hasRefs  = (header[0] & 0x40) != 0;
	m_width    = (1 << (header[0] & 0x07)) >> 1; // 0, 1, 2, 4, 8, 16, 32, 64
	m_len      = (header[1] << 16) + (header[2] << 8) + header[3];
	m_capacity = (header[4] << 16) + (header[5] << 8) + header[6];

	m_data = header + 8;

	SetWidth(m_width);
}

bool Column::IsEmpty() const {
	if (!IsNode()) return m_len == 0;
	else {
		const Column offsets = GetSubColumn(0);
		return offsets.IsEmpty();
	}
}

size_t Column::Size() const {
	if (!IsNode()) return m_len;
	else {
		const Column offsets = GetSubColumn(0);
		return offsets.IsEmpty() ? 0 : (size_t)offsets.ListBack();
	}
}

void Column::SetParent(Column* parent, size_t pndx) {
	m_parent = parent;
	m_parentNdx = pndx;
}

Column Column::GetSubColumn(size_t ndx) {
	assert(ndx < m_len);
	assert(m_hasRefs);

	return Column((void*)ListGet(ndx), this, ndx);
}

const Column Column::GetSubColumn(size_t ndx) const {
	assert(ndx < m_len);
	assert(m_hasRefs);

	return Column((void*)ListGet(ndx), this, ndx);
}

void Column::Destroy() {
	if (m_hasRefs) {
		for (size_t i = 0; i < ListSize(); ++i) {
			Column sub((void*)ListGet(i), this, i);
			sub.Destroy();
		}
	}
	
	void* ref = m_data-8;
	free(ref);
	m_data = NULL;
}

void Column::Clear() {
	if (IsNode()) {
		Destroy();
		m_isNode = false;
		m_hasRefs = false;
		m_capacity = 0;
		Alloc(0,0);
	}
	
	m_len = 0;
	SetWidth(0);
}

/**
 * Takes a 64bit value and return the minimum number of bits needed to fit the value.
 * For alignment this is rounded up to nearest log2.
 * Posssible results {0, 1, 2, 4, 8, 16, 32, 64}
 */
static unsigned int BitWidth(int64_t v) {
	if ((v >> 4) == 0) {
		static const int8_t bits[] = {0, 1, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4};
		return bits[(int8_t)v];
	}

	// first flip all bits if bit 63 is set
	if (v < 0) v = ~v;
	// ... bit 63 is now always zero

	// then check if bits 15-31 used (32b), 7-31 used (16b), else (8b)
	return v >> 31 ? 64 : v >> 15 ? 32 : v >> 7 ? 16 : 8;
}

int64_t Column::Get64(size_t ndx) const {
	if (IsNode()) {
		// Get subnode table
		const Column offsets = GetSubColumn(0);
		const Column refs = GetSubColumn(1);

		// Find the subnode containing the item
		const size_t node_ndx = offsets.ListFindPos(ndx);

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.ListGet(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Get item
		const Column target = refs.GetSubColumn(node_ndx);
		return target.Get64(local_ndx);
	}
	else return ListGet(ndx);
}

int64_t Column::ListGet(size_t ndx) const {
	assert(ndx < m_len);
	return (this->*m_getter)(ndx);
}

int64_t Column::ListBack() const {
	assert(m_len);
	return (this->*m_getter)(m_len-1);
}

bool Column::Set64(size_t ndx, int64_t value) {
	if (IsNode()) {
		// Get subnode table
		const Column offsets = GetSubColumn(0);
		Column refs = GetSubColumn(1);

		// Find the subnode containing the item
		const size_t node_ndx = offsets.ListFindPos(ndx);

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.ListGet(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Set item
		Column target = refs.GetSubColumn(node_ndx);
		return target.Set64(local_ndx, value);
	}
	else return ListSet(ndx, value);
}

bool Column::ListSet(size_t ndx, int64_t value) {
	assert(ndx < m_len);

	// Make room for the new value
	const size_t width = BitWidth(value);
	if (width > m_width) {
		Getter oldGetter = m_getter;
		if (!Alloc(m_len, width)) return false;
		SetWidth(width);

		// Expand the old values
		int k = (int)m_len;
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
	return Insert64(Size(), value);
}

bool Column::Insert64(size_t ndx, int64_t value) {
	assert(ndx <= Size());

	const NodeChange nc = DoInsert(ndx, value);
	switch (nc.type) {
	case NodeChange::ERROR:
		return false; // allocation error
	case NodeChange::NONE:
		break;
	case NodeChange::INSERT_BEFORE:
		{
			Column newNode(COLUMN_NODE);
			newNode.NodeAdd(nc.ref1);
			newNode.NodeAdd(GetRef());
			UpdateRef(newNode.GetRef());
			break;
		}
	case NodeChange::INSERT_AFTER:
		{
			Column newNode(COLUMN_NODE);
			newNode.NodeAdd(GetRef());
			newNode.NodeAdd(nc.ref1);
			UpdateRef(newNode.GetRef());
			break;
		}
	case NodeChange::SPLIT:
		{
			Column newNode(COLUMN_NODE);
			newNode.NodeAdd(nc.ref1);
			newNode.NodeAdd(nc.ref2);
			UpdateRef(newNode.GetRef());
			break;
		}
	default:
		assert(false);
		return false;
	}

	Verify();
	return true;
}

Column::NodeChange Column::DoInsert(size_t ndx, int64_t value) {
	if (IsNode()) {
		// Get subnode table
		Column offsets = GetSubColumn(0);
		Column refs = GetSubColumn(1);

		// Find the subnode containing the item
		size_t node_ndx = offsets.ListFindPos(ndx);
		if (node_ndx == -1) {
			// node can never be empty, so try to fit in last item
			node_ndx = offsets.ListSize()-1;
		}

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.ListGet(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Get sublist
		Column target = refs.GetSubColumn(node_ndx);

		// Insert item
		const NodeChange nc = target.DoInsert(local_ndx, value);
		if (nc.type ==  NodeChange::ERROR) return NodeChange(NodeChange::ERROR); // allocation error
		else if (nc.type ==  NodeChange::NONE) {
			offsets.ListIncrement(1, node_ndx);  // update offsets
			return NodeChange(NodeChange::NONE); // no new nodes
		}

		if (nc.type == NodeChange::INSERT_AFTER) ++node_ndx;

		// If there is room, just update node directly
		if (offsets.ListSize() < MAX_LIST_SIZE) {
			if (nc.type == NodeChange::SPLIT) return NodeInsertSplit(node_ndx, nc.ref2);
			else return NodeInsert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
		}

		// Else create new node
		Column newNode(COLUMN_NODE);
		newNode.NodeAdd(nc.ref1);

		switch (node_ndx) {
		case 0:	            // insert before
			return NodeChange(NodeChange::INSERT_BEFORE, newNode.GetRef());
		case MAX_LIST_SIZE:	// insert below
			return NodeChange(NodeChange::INSERT_AFTER, newNode.GetRef());
		default:            // split
			// Move items below split to new node
			const size_t len = refs.ListSize();
			for (size_t i = node_ndx; i < len; ++i) {
				newNode.NodeAdd((void*)refs.ListGet(i));
			}
			offsets.ListResize(node_ndx);
			refs.ListResize(node_ndx);
			return NodeChange(NodeChange::SPLIT, GetRef(), newNode.GetRef());
		}
	}
	else {
		// Is there room in the list?
		// lists with refs are internal cannot be split
		if (m_hasRefs || m_len < MAX_LIST_SIZE) {
			return ListInsert(ndx, value);
		}

		// Create new list for item
		Column newList;
		if (!newList.ListAdd(value)) return NodeChange(NodeChange::ERROR);
		
		switch (ndx) {
		case 0:	            // insert before
			return NodeChange(NodeChange::INSERT_BEFORE, newList.GetRef());
		case MAX_LIST_SIZE:	// insert below
			return NodeChange(NodeChange::INSERT_AFTER, newList.GetRef());
		default:            // split
			// Move items below split to new list
			for (size_t i = ndx; i < m_len; ++i) {
				newList.ListAdd(ListGet(i));
			}
			ListResize(ndx);

			return NodeChange(NodeChange::SPLIT, GetRef(), newList.GetRef());
		}
	}
}

size_t Column::ListFindPos(int64_t value) const {
	int low = -1;
	int high = (int)m_len;

	// Binary search based on: http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
	while (high - low > 1) {
		const size_t probe = ((unsigned int)low + (unsigned int)high) >> 1;
		const int64_t v = (this->*m_getter)(probe);

		if (v > value) high = (int)probe;
		else           low = (int)probe;
	}
	if (high == (int)m_len) return (size_t)-1;
	else return high;
}

bool Column::ListInsert(size_t ndx, int64_t value) {
	assert(ndx <= m_len);

	Getter getter = m_getter;

	// Make room for the new value
	const size_t width = BitWidth(value);
	const bool doExpand = (width > m_width);
	if (doExpand) {
		if (!Alloc(m_len+1, width)) return false;
		SetWidth(width);
	}
	else {
		if (!Alloc(m_len+1, m_width)) return false;
	}

	// Move values below insertion (may expand)
	if (doExpand || m_width < 8) {
		int k = (int)m_len;
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
		int k = (int)ndx;
		while (--k >= 0) {
			const int64_t v = (this->*getter)(k);
			(this->*m_setter)(k, v);
		}
	}

	// Update length
	// (no need to do it in header as it has been done by Alloc)
	++m_len;

	return true;
}

void Column::UpdateRef(void* ref) {
	Create(ref);

	// Update ref in parent
	if (m_parent) m_parent->ListSet(m_parentNdx, (intptr_t)ref);
}

size_t GetRefSize(void* ref) {
	// parse the length part of 8byte header
	const uint8_t* const header = (uint8_t*)ref;
	return (header[1] << 16) + (header[2] << 8) + header[3];
}

void SetRefSize(void* ref, size_t len) {
	uint8_t* const header = (uint8_t*)(ref);
	header[1] = ((len >> 16) & 0x000000FF);
	header[2] = (len >> 8) & 0x000000FF;
	header[3] = len & 0x000000FF;
}

bool Column::NodeInsert(size_t ndx, void* ref) {
	assert(ref);
	assert(IsNode());
	
	Column offsets = GetSubColumn(0);
	Column refs = GetSubColumn(1);
	assert(ndx <= offsets.ListSize());

	const Column col(ref);
	const size_t refSize = col.Size();
	const int64_t newOffset = (ndx ? offsets.ListGet(ndx-1) : 0) + refSize;

	if (!offsets.ListInsert(ndx, newOffset)) return false;
	if (ndx+1 < offsets.ListSize()) {
		if (!offsets.ListIncrement(refSize, ndx+1)) return false;
	}
	return refs.ListInsert(ndx, (intptr_t)ref);
}

bool Column::NodeAdd(void* ref) {
	assert(ref);
	assert(IsNode());

	Column offsets = GetSubColumn(0);
	Column refs = GetSubColumn(1);
	const Column col(ref);

	const int64_t newOffset = (offsets.IsEmpty() ? 0 : offsets.ListBack()) + col.Size();
	if (!offsets.ListAdd(newOffset)) return false;
	return refs.ListAdd((intptr_t)ref);
}

bool Column::NodeUpdateOffsets(size_t ndx) {
	assert(IsNode());

	Column offsets = GetSubColumn(0);
	Column refs = GetSubColumn(1);
	assert(ndx < offsets.ListSize());

	const int64_t newSize = GetRefSize((void*)refs.ListGet(ndx));
	const int64_t oldSize = offsets.ListGet(ndx) - (ndx ? offsets.ListGet(ndx-1) : 0);
	const int64_t diff = newSize - oldSize;
	
	return offsets.ListIncrement(diff, ndx);
}

bool Column::NodeInsertSplit(size_t ndx, void* newRef) {
	assert(IsNode());
	assert(newRef);

	Column offsets = GetSubColumn(0);
	Column refs = GetSubColumn(1);
	assert(ndx < offsets.ListSize());

	// Update original size
	const int64_t offset = ndx ? offsets.ListGet(ndx-1) : 0;
	const int64_t newSize = GetRefSize((void*)refs.ListGet(ndx));
	const int64_t oldSize = offsets.ListGet(ndx) - offset;
	const int64_t diff = newSize - oldSize;
	const int64_t newOffset = offset + newSize;
	offsets.ListSet(ndx, newOffset);

	// Insert new ref
	const int64_t refSize = GetRefSize(newRef);
	offsets.ListInsert(ndx+1, newOffset + refSize);
	refs.ListInsert(ndx+1, (intptr_t)newRef);

	// Update lower offsets
	const int64_t newDiff = diff + refSize;
	return offsets.ListIncrement(newDiff, ndx+2);
}

bool Column::ListAdd(int64_t value) {
	return ListInsert(m_len, value);
}

void Column::ListResize(size_t count) {
	assert(count <= m_len);

	// Update length (also in header)
	m_len = count;
	SetRefSize(m_data-8, m_len);
}

void Column::Delete(size_t ndx) {
	assert(ndx < Size());

	if (!IsNode()) ListDelete(ndx);
	else {
		// Get subnode table
		Column offsets = GetSubColumn(0);
		Column refs = GetSubColumn(1);

		// Find the subnode containing the item
		const size_t node_ndx = offsets.ListFindPos(ndx);
		assert(node_ndx != -1);

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.ListGet(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Get sublist
		Column target = refs.GetSubColumn(node_ndx);
		target.Delete(local_ndx);

		// Remove ref in node
		if (target.IsEmpty()) {
			offsets.ListDelete(node_ndx);
			refs.ListDelete(node_ndx);
			target.Destroy();
		}

		// Update lower offsets
		if (node_ndx < offsets.Size()) offsets.ListIncrement(-1, node_ndx);
	}
}

void Column::ListDelete(size_t ndx) {
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

	// Update length (also in header)
	--m_len;
	SetRefSize(m_data-8, m_len);
}

bool Column::Increment64(int64_t value, size_t start, size_t end) {
	if (!IsNode()) return ListIncrement(value, start, end);
	else {
		//TODO: partial incr
		Column refs = GetSubColumn(1);
		for (size_t i = 0; i < refs.Size(); ++i) {
			Column col = refs.GetSubColumn(i);
			if (!col.Increment64(value)) return false;
		}
		return true;
	}
}

bool Column::ListIncrement(int64_t value, size_t start, size_t end) {
	if (end == -1) end = m_len;
	assert(start < m_len);
	assert(end >= start && end <= m_len);

	// Increment range
	for (size_t i = start; i < end; ++i) {
		ListSet(i, ListGet(i) + value);
	}
	return true;
}

size_t Column::Find(int64_t value, size_t start, size_t end) const {
	assert(start <= Size());
	assert(end == -1 || end <= Size());
	if (IsEmpty()) return (size_t)-1;

	// Use index if possible
	if (m_index && start == 0 && end == -1) {
		return FindWithIndex(value);
	}

	if (!IsNode()) return ListFind(value, start, end);
	else {
		// Get subnode table
		Column offsets = GetSubColumn(0);
		Column refs = GetSubColumn(1);
		const size_t count = refs.ListSize();

		if (start == 0 && end == -1) {
			for (size_t i = 0; i < count; ++i) {
				const Column col = refs.GetSubColumn(i);
				const size_t ndx = col.Find(value);
				if (ndx != -1) {
					const size_t offset = i ? (size_t)offsets.ListGet(i-1) : 0;
					return offset + ndx;
				}
			}
		}
		else {
			// partial search
			size_t i = offsets.ListFindPos(start);
			size_t offset = i ? (size_t)offsets.ListGet(i-1) : 0;
			size_t s = start - offset;
			size_t e = (end == -1 || (int)end >= offsets.ListGet(i)) ? -1 : end - offset;

			for (;;) {
				const Column col = refs.GetSubColumn(i);

				const size_t ndx = col.Find(value, s, e);
				if (ndx != -1) {
					const size_t offset = i ? (size_t)offsets.ListGet(i-1) : 0;
					return offset + ndx;
				}

				++i;
				if (i >= count) break;

				s = 0;
				if (end != -1) {
					if (end >= (size_t)offsets.ListGet(i)) e = (size_t)-1;
					else {
						offset = (size_t)offsets.ListGet(i-1);
						e = end - offset;
					}
				}
			}
		}

		return (size_t)-1; // not found
	}
}

size_t Column::ListFind(int64_t value, size_t start, size_t end) const {
	if (IsEmpty()) return (size_t)-1;
	if (end == -1) end = m_len;
	if (start == end) return (size_t)-1;

	assert(start < m_len && end <= m_len && start < end);

	// If the value is wider than the column
	// then we know it can't be there
	const size_t width = BitWidth(value);
	if (width > m_width) return (size_t)-1;

	// Do optimized search based on column width
	if (m_width == 0) {
		return start; // value can only be zero
	}
	else if (m_width == 2) {
		// Create a pattern to match 64bits at a time
		const int64_t v = ~0ULL/0x3 * value;

		const int64_t* p = (const int64_t*)m_data + start;
		const size_t end64 = m_len / 32;
		const int64_t* const e = (const int64_t*)m_data + end64;

		// Check 64bits at a time for match
		while (p < e) {
			const uint64_t v2 = *p ^ v; // zero matching bit segments
			const bool hasZeroByte = (v2 - 0x5555555555555555UL) & ~v2 
																	 & 0xAAAAAAAAAAAAAAAAUL;
			if (hasZeroByte) break;
			++p;
		}

		// Position of last chunk (may be partial)
		size_t i = (p - (const int64_t*)m_data) * 32;

		// Manually check the rest
		while (i < end) {
			const size_t offset = i >> 2;
			const int64_t v = (m_data[offset] >> ((i & 3) << 1)) & 0x03;
			if (v == value) return i;
			++i;
		}
	}
	else if (m_width == 4) {
		// Create a pattern to match 64bits at a time
		const int64_t v = ~0ULL/0xF * value;

		const int64_t* p = (const int64_t*)m_data + start;
		const size_t end64 = m_len / 16;
		const int64_t* const e = (const int64_t*)m_data + end64;

		// Check 64bits at a time for match
		while (p < e) {
			const uint64_t v2 = *p ^ v; // zero matching bit segments
			const bool hasZeroByte = (v2 - 0x1111111111111111UL) & ~v2 
																	 & 0x8888888888888888UL;
			if (hasZeroByte) break;
			++p;
		}

		// Position of last chunk (may be partial)
		size_t i = (p - (const int64_t*)m_data) * 16;

		// Manually check the rest
		while (i < end) {
			const size_t offset = i >> 1;
			const int64_t v = (m_data[offset] >> ((i & 1) << 2)) & 0xF;
			if (v == value) return i;
			++i;
		}
	}
  else if (m_width == 8) {
		// TODO: Handle partial searches

		// Create a pattern to match 64bits at a time
		const int64_t v = ~0ULL/0xFF * value;

		const int64_t* p = (const int64_t*)m_data + start;
		const size_t end64 = m_len / 8;
		const int64_t* const e = (const int64_t*)m_data + end64;

		// Check 64bits at a time for match
		while (p < e) {
			const uint64_t v2 = *p ^ v; // zero matching bit segments
			const uint64_t hasZeroByte = (v2 - 0x0101010101010101ULL) & ~v2
																			 & 0x8080808080808080ULL;
			if (hasZeroByte) break;
			++p;
		}

		// Position of last chunk (may be partial)
		size_t i = (p - (const int64_t*)m_data) * 8;
		const int8_t* d = (const int8_t*)m_data;

		// Manually check the rest
		while (i < end) {
			if (value == d[i]) return i;
			++i;
		}
	}
	else if (m_width == 16) {
		// Create a pattern to match 64bits at a time
		const int64_t v = ~0ULL/0xFFFF * value;

		const int64_t* p = (const int64_t*)m_data + start;
		const size_t end64 = m_len / 4;
		const int64_t* const e = (const int64_t*)m_data + end64;

		// Check 64bits at a time for match
		while (p < e) {
			const uint64_t v2 = *p ^ v; // zero matching bit segments
			const uint64_t hasZeroByte = (v2 - 0x0001000100010001UL) & ~v2
																			 & 0x8000800080008000UL;
			if (hasZeroByte) break;
			++p;
		}
		
		// Position of last chunk (may be partial)
		size_t i = (p - (const int64_t*)m_data) * 4;
		const int16_t* d = (const int16_t*)m_data;

		// Manually check the rest
		while (i < end) {
			if (value == d[i]) return i;
			++i;
		}
	}
	else if (m_width == 32) {
		// Create a pattern to match 64bits at a time
		const int64_t v = ~0ULL/0xFFFFFFFF * value;

		const int64_t* p = (const int64_t*)m_data + start;
		const size_t end64 = m_len / 2;
		const int64_t* const e = (const int64_t*)m_data + end64;

		// Check 64bits at a time for match
		while (p < e) {
			const uint64_t v2 = *p ^ v; // zero matching bit segments
			const uint64_t hasZeroByte = (v2 - 0x0000000100000001UL) & ~v2
																			 & 0x8000800080000000UL;
			if (hasZeroByte) break;
			++p;
		}
		
		// Position of last chunk (may be partial)
		size_t i = (p - (const int64_t*)m_data) * 2;
		const int32_t* d = (const int32_t*)m_data;

		// Manually check the rest
		while (i < end) {
			if (value == d[i]) return i;
			++i;
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

	return (size_t)-1; // not found
}

size_t Column::FindWithIndex(int64_t target) const {
	assert(m_index);
	assert(m_index->Size() == Size());
	assert(m_index_refs);

	int low = -1;
	int high = (int)Size();

	// Binary search through index
	// based on: http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
	while (high - low > 1) {
		const int probe = ((unsigned int)low + (unsigned int)high) >> 1;
		const int64_t v = m_index->Get64(probe);

		if (v > target)
			high = probe;
		else
			low = probe;
	}
	if (low == -1) return (size_t)-1;

	if (m_index->Get64(low) != target)
		return (size_t)-1;
	else
		return (size_t)m_index_refs->Get64(low);
}

void SortIndex(Column& index, const Column& target, size_t lo, size_t hi) {
	// Quicksort based on
	// http://www.inf.fh-flensburg.de/lang/algorithmen/sortieren/quick/quicken.htm
	int i = (int)lo;
	int j = (int)hi;

	// comparison element x
	const size_t ndx = (lo + hi)/2;
	const size_t ref = (size_t)index.Get64(ndx);
	const int64_t x = target.Get64(ref);

	// partition
	do {
		while (target.Get64((size_t)index.Get64(i)) < x) i++;
		while (target.Get64((size_t)index.Get64(j)) > x) j--;
		if (i <= j) {
			const int64_t h = index.Get64(i);
			index.Set64(i, index.Get64(j));
			index.Set64(j, h);
			i++; j--;
		}
	} while (i <= j);

	//  recursion
	if ((int)lo < j) SortIndex(index, target, lo, j);
	if (i < (int)hi) SortIndex(index, target, i, hi);
}

bool Column::HasIndex() const {
	return m_index != NULL;
}

Column& Column::GetIndex() {
	assert(m_index);
	return *m_index;
}

void Column::ClearIndex() {
	m_index = NULL;
	m_index_refs = NULL;
}

void Column::BuildIndex(Column& index_refs) {
	// Make sure the index has room for all the refs
	index_refs.Clear();
	const size_t len = Size();
	const size_t width = BitWidth(Size());
	index_refs.Reserve(len, width);

	// Fill it up with unsorted refs
	for (size_t i = 0; i < len; ++i) {
		index_refs.Add64(i);
	}

	// Sort the index
	SortIndex(index_refs, *this, 0, len-1);

	// Create the actual index
	Column* ndx = new Column();
	for (size_t i = 0; i < len; ++i) {
		ndx->Add64(Get64((size_t)index_refs.Get64(i)));
	}

	// Keep ref to index
	m_index = ndx;
	m_index_refs = &index_refs;
}

int64_t Column::Get_0b(size_t) const {
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
	// Calculate size in bytes
	size_t len = 8; // always need room for header
	switch (width) {
	case 0:
		break;
	case 1:
		len += count >> 3;
		if (count & 0x07) ++len;
		break;
	case 2:
		len += count >> 2;
		if (count & 0x03) ++len;
		break;
	case 4:
		len += count >> 1;
		if (count & 0x01) ++len;
		break;
	default:
		assert(width == 8 || width == 16 || width == 32 || width == 64);
		len += count * (width >> 3);
	}

	if (len > m_capacity) {
		// Try to expand with 50% to avoid to many reallocs
		size_t new_capacity = m_capacity ? m_capacity + m_capacity / 2 : 128;
		if (new_capacity < len) new_capacity = len; 

		// Allocate the space
		unsigned char* data = NULL;
		if (m_data) data = (unsigned char*)realloc(m_data-8, new_capacity);
		else data = (unsigned char*)malloc(new_capacity);

		if (!data) return false;

		m_data = data+8;
		m_capacity = new_capacity;

		// Update ref in parent
		if (m_parent) m_parent->ListSet(m_parentNdx, (uintptr_t)data);
	}

	// Pack width in 3 bits (log2)
	unsigned int w = 0;
	unsigned int b = (unsigned int)width;
	while (b) {++w; b >>= 1;}
	assert(0 <= w && w < 8);

	// Update 8-byte header
	// isNode 1 bit, hasRefs 1 bit, 3 bits unused, width 3 bits, len 3 bytes, capacity 3 bytes
	uint8_t* const header = (uint8_t*)(m_data-8);
	header[0] = m_isNode << 7;
	header[0] += m_hasRefs << 6;
	header[0] += (uint8_t)w;
	header[1] = (count >> 16) & 0x000000FF;
	header[2] = (count >> 8) & 0x000000FF;
	header[3] = count & 0x000000FF;
	header[4] = (m_capacity >> 16) & 0x000000FF;
	header[5] = (m_capacity >> 8) & 0x000000FF;
	header[6] = m_capacity & 0x000000FF;

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

#include "stdio.h"

void Column::Print() const {
	if (IsNode()) {
		printf("Node: %x\n", GetRef());
		
		const Column offsets = GetSubColumn(0);
		const Column refs = GetSubColumn(1);

		for (size_t i = 0; i < refs.ListSize(); ++i) {
			printf(" %d: %d %x\n", i, (int)offsets.ListGet(i), (int)refs.ListGet(i));
		}
		for (size_t i = 0; i < refs.ListSize(); ++i) {
			const Column col = refs.GetSubColumn(i);
			col.Print();
		}
	}
	else {
		printf("%x: (%d) ", GetRef(), ListSize());
		for (size_t i = 0; i < ListSize(); ++i) {
			if (i) printf(", ");
			printf("%d", (int)ListGet(i));
		}
		printf("\n");
	}
}

void Column::Verify() const {
#ifdef _DEBUG
	if (IsNode()) {
		assert(ListSize() == 2);
		assert(m_hasRefs);

		const Column offsets = GetSubColumn(0);
		const Column refs = GetSubColumn(1);

		size_t off = 0;
		for (size_t i = 0; i < refs.ListSize(); ++i) {
			const Column col = refs.GetSubColumn(i);
			col.Verify();

			off += col.Size();
			if (offsets.ListGet(i) != (int)off) {
				assert(false);
			}
		}
	}
	else {
		assert(m_width == 0 || m_width == 1 || m_width == 2 || m_width == 4 || m_width == 8 || m_width == 16 || m_width == 32 || m_width == 64);
	}
#endif //_DEBUG
}

StringColumn::StringColumn(Column& refs, Column& lengths) : m_refs(refs), m_lengths(lengths) {
}

StringColumn::~StringColumn() {
}

void* StringColumn::Alloc(const char* value, size_t len) {
	assert(len); // empty strings are not allocated, but marked with zero-ref

	char* const data = (char*)malloc(len+1); // room for trailing zero-byte
	if (!data) return NULL; // alloc failed

	memmove(data, value, len);
	data[len] = '\0';

	return data;
}

void StringColumn::Free(size_t ndx) {
	assert(ndx < m_refs.Size());

	void* data = (void*)m_refs.Get(ndx);
	if (!data) return;

	free(data);
}

const char* StringColumn::Get(size_t ndx) const {
	assert(ndx < m_refs.Size());

	return (const char*)m_refs.Get(ndx);
}

bool StringColumn::Set(size_t ndx, const char* value) {
	return Set(ndx, value, strlen(value));
}

bool StringColumn::Set(size_t ndx, const char* value, size_t len) {
	assert(ndx < m_refs.Size());

	// Empty strings are just marked with zero-ref
	if (len == 0) {
		Free(ndx);
		m_refs.Set(ndx, 0);
		m_lengths.Set(ndx, 0);
		return true;
	}

	void* ref = Alloc(value, len);
	if (!ref) return false;
	Free(ndx);

	m_refs.Set(ndx, (intptr_t)ref);
	m_lengths.Set(ndx, len);
	return true;
}

bool StringColumn::Add() {
	return Insert(Size(), "", 0);
}

bool StringColumn::Insert(size_t ndx, const char* value, size_t len) {
	assert(ndx <= m_refs.Size());

	// Empty strings are just marked with zero-ref
	if (len == 0) {
		m_refs.Insert(ndx, 0);
		m_lengths.Insert(ndx, 0);
		return true;
	}

	void* ref = Alloc(value, len);
	if (!ref) return false;

	m_refs.Insert(ndx, (intptr_t)ref);
	m_lengths.Insert(ndx, len);
	return true;
}

void StringColumn::Clear() {
	m_refs.Clear();
	m_lengths.Clear();
}

void StringColumn::Delete(size_t ndx) {
	assert(ndx < m_refs.Size());

	m_refs.Delete(ndx);
	m_lengths.Delete(ndx);
}

size_t StringColumn::Find(const char* value) const {
	return Find(value, strlen(value));
}

size_t StringColumn::Find(const char* value, size_t len) const {
	size_t pos = 0;

	// special case for zero-length strings
	if (len == 0) {
		return m_lengths.Find(0, pos);
	}
	
	const size_t count = m_refs.Size();
	while (pos < count) {
		// Find next string with matching length
		pos = m_lengths.Find(len, pos);
		if (pos == -1) return (size_t)-1;

		// We do a quick manual check of first byte before
		// calling expensive memcmp
		const char* const v = Get(pos);
		if (v[0] == value[0]) {
			if (memcmp(value, v, len) == 0) return pos;
		}
		++pos;
	}

	return (size_t)-1;
}
