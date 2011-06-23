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
#include "Index.h"

#define MAX_LIST_SIZE 1000

Column::Column() : m_index(NULL) {
}

Column::Column(ColumnDef type, Array* parent, size_t pndx) : m_array(type, parent, pndx),  m_index(NULL) {
	// Add subcolumns for nodes
	if (IsNode()) {
		const Array offsets(COLUMN_NORMAL);
		const Array refs(COLUMN_HASREFS);
		m_array.Add((intptr_t)offsets.GetRef());
		m_array.Add((intptr_t)refs.GetRef());
	}
}

Column::Column(void* ref) : m_array(ref), m_index(NULL) {
}

Column::Column(void* ref, Array* parent, size_t pndx) : m_array(ref, parent, pndx), m_index(NULL) {
}

Column::Column(void* ref, const Array* parent, size_t pndx): m_array(ref, parent, pndx), m_index(NULL) {
}

Column::Column(const Column& column) : m_array(column.m_array), m_index(NULL) {
}

Column& Column::operator=(const Column& column) {
	m_array = column.m_array;
	m_index = column.m_index;
	return *this;
}

bool Column::operator==(const Column& column) const {
	return m_array == column.m_array;
}

Column::~Column() {
}


bool Column::IsEmpty() const {
	if (!IsNode()) return m_array.IsEmpty();
	else {
		const Array offsets = m_array.GetSubArray(0);
		return offsets.IsEmpty();
	}
}

size_t Column::Size() const {
	if (!IsNode()) return m_array.Size();
	else {
		const Array offsets = m_array.GetSubArray(0);
		return offsets.IsEmpty() ? 0 : (size_t)offsets.Back();
	}
}

void Column::SetParent(Array* parent, size_t pndx) {
	m_array.SetParent(parent, pndx);
}

static Column GetColumnFromRef(Array& parent, size_t ndx) {
	assert(parent.HasRefs());
	assert(ndx < parent.Size());
	return Column((void*)parent.Get(ndx), &parent, ndx);
}

static const Column GetColumnFromRef(const Array& parent, size_t ndx) {
	assert(parent.HasRefs());
	assert(ndx < parent.Size());
	return Column((void*)parent.Get(ndx), &parent, ndx);
}

/*Column Column::GetSubColumn(size_t ndx) {
	assert(ndx < m_len);
	assert(m_hasRefs);

	return Column((void*)ListGet(ndx), this, ndx);
}

const Column Column::GetSubColumn(size_t ndx) const {
	assert(ndx < m_len);
	assert(m_hasRefs);

	return Column((void*)ListGet(ndx), this, ndx);
}*/

void Column::Clear() {
	m_array.Clear();
	if (m_array.IsNode()) m_array.SetType(COLUMN_NORMAL);
}

int64_t Column::Get64(size_t ndx) const {
	if (IsNode()) {
		// Get subnode table
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);

		// Find the subnode containing the item
		const size_t node_ndx = offsets.FindPos(ndx);

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.Get(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Get item
		const Column target = GetColumnFromRef(refs, node_ndx);
		return target.Get64(local_ndx);
	}
	else return m_array.Get(ndx);
}

bool Column::Set64(size_t ndx, int64_t value) {
	if (IsNode()) {
		// Get subnode table
		const Array offsets = m_array.GetSubArray(0);
		Array refs = m_array.GetSubArray(1);

		// Find the subnode containing the item
		const size_t node_ndx = offsets.FindPos(ndx);

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.Get(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Set item
		Column target = GetColumnFromRef(refs, node_ndx);
		if (!target.Set64(local_ndx, value)) return false;
	}
	else if (!m_array.Set(ndx, value)) return false;

#ifdef _DEBUG
	Verify();
#endif //DEBUG

	return true;
}

bool Column::Add64(int64_t value) {
	return Insert64(Size(), value);
}

bool Column::Insert64(size_t ndx, int64_t value) {
	assert(ndx <= Size());

	const NodeChange nc = DoInsert(ndx, value);
	switch (nc.type) {
	case NodeChange::CT_ERROR:
		return false; // allocation error
	case NodeChange::CT_NONE:
		break;
	case NodeChange::CT_INSERT_BEFORE:
		{
			Column newNode(COLUMN_NODE);
			newNode.NodeAdd(nc.ref1);
			newNode.NodeAdd(GetRef());
			m_array.UpdateRef(newNode.GetRef());
			break;
		}
	case NodeChange::CT_INSERT_AFTER:
		{
			Column newNode(COLUMN_NODE);
			newNode.NodeAdd(GetRef());
			newNode.NodeAdd(nc.ref1);
			m_array.UpdateRef(newNode.GetRef());
			break;
		}
	case NodeChange::CT_SPLIT:
		{
			Column newNode(COLUMN_NODE);
			newNode.NodeAdd(nc.ref1);
			newNode.NodeAdd(nc.ref2);
			m_array.UpdateRef(newNode.GetRef());
			break;
		}
	default:
		assert(false);
		return false;
	}

#ifdef _DEBUG
	Verify();
#endif //DEBUG

	return true;
}

Column::NodeChange Column::DoInsert(size_t ndx, int64_t value) {
	if (IsNode()) {
		// Get subnode table
		Array offsets = m_array.GetSubArray(0);
		Array refs = m_array.GetSubArray(1);

		// Find the subnode containing the item
		size_t node_ndx = offsets.FindPos(ndx);
		if (node_ndx == -1) {
			// node can never be empty, so try to fit in last item
			node_ndx = offsets.Size()-1;
		}

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.Get(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Get sublist
		Column target = GetColumnFromRef(refs, node_ndx);

		// Insert item
		const NodeChange nc = target.DoInsert(local_ndx, value);
		if (nc.type ==  NodeChange::CT_ERROR) return NodeChange(NodeChange::CT_ERROR); // allocation error
		else if (nc.type ==  NodeChange::CT_NONE) {
			offsets.Increment(1, node_ndx);  // update offsets
			return NodeChange(NodeChange::CT_NONE); // no new nodes
		}

		if (nc.type == NodeChange::CT_INSERT_AFTER) ++node_ndx;

		// If there is room, just update node directly
		if (offsets.Size() < MAX_LIST_SIZE) {
			if (nc.type == NodeChange::CT_SPLIT) return NodeInsertSplit(node_ndx, nc.ref2);
			else return NodeInsert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
		}

		// Else create new node
		Column newNode(COLUMN_NODE);
		newNode.NodeAdd(nc.ref1);

		switch (node_ndx) {
		case 0:	            // insert before
			return NodeChange(NodeChange::CT_INSERT_BEFORE, newNode.GetRef());
		case MAX_LIST_SIZE:	// insert below
			return NodeChange(NodeChange::CT_INSERT_AFTER, newNode.GetRef());
		default:            // split
			// Move items below split to new node
			const size_t len = refs.Size();
			for (size_t i = node_ndx; i < len; ++i) {
				newNode.NodeAdd((void*)refs.Get(i));
			}
			offsets.Resize(node_ndx);
			refs.Resize(node_ndx);
			return NodeChange(NodeChange::CT_SPLIT, GetRef(), newNode.GetRef());
		}
	}
	else {
		// Is there room in the list?
		if (m_array.Size() < MAX_LIST_SIZE) {
			return m_array.Insert(ndx, value);
		}

		// Create new list for item
		Array newList;
		if (!newList.Add(value)) return NodeChange(NodeChange::CT_ERROR);
		
		switch (ndx) {
		case 0:	            // insert before
			return NodeChange(NodeChange::CT_INSERT_BEFORE, newList.GetRef());
		case MAX_LIST_SIZE:	// insert below
			return NodeChange(NodeChange::CT_INSERT_AFTER, newList.GetRef());
		default:            // split
			// Move items below split to new list
			for (size_t i = ndx; i < m_array.Size(); ++i) {
				newList.Add(m_array.Get(i));
			}
			m_array.Resize(ndx);

			return NodeChange(NodeChange::CT_SPLIT, GetRef(), newList.GetRef());
		}
	}
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
	
	Array offsets = m_array.GetSubArray(0);
	Array refs = m_array.GetSubArray(1);
	assert(ndx <= offsets.Size());

	const Column col(ref);
	const size_t refSize = col.Size();
	const int64_t newOffset = (ndx ? offsets.Get(ndx-1) : 0) + refSize;

	if (!offsets.Insert(ndx, newOffset)) return false;
	if (ndx+1 < offsets.Size()) {
		if (!offsets.Increment(refSize, ndx+1)) return false;
	}
	return refs.Insert(ndx, (intptr_t)ref);
}

bool Column::NodeAdd(void* ref) {
	assert(ref);
	assert(IsNode());

	Array offsets = m_array.GetSubArray(0);
	Array refs = m_array.GetSubArray(1);
	const Column col(ref);

	const int64_t newOffset = (offsets.IsEmpty() ? 0 : offsets.Back()) + col.Size();
	if (!offsets.Add(newOffset)) return false;
	return refs.Add((intptr_t)ref);
}

bool Column::NodeUpdateOffsets(size_t ndx) {
	assert(IsNode());

	Array offsets = m_array.GetSubArray(0);
	Array refs = m_array.GetSubArray(1);
	assert(ndx < offsets.Size());

	const int64_t newSize = GetRefSize((void*)refs.Get(ndx));
	const int64_t oldSize = offsets.Get(ndx) - (ndx ? offsets.Get(ndx-1) : 0);
	const int64_t diff = newSize - oldSize;
	
	return offsets.Increment(diff, ndx);
}

bool Column::NodeInsertSplit(size_t ndx, void* newRef) {
	assert(IsNode());
	assert(newRef);

	Array offsets = m_array.GetSubArray(0);
	Array refs = m_array.GetSubArray(1);
	assert(ndx < offsets.Size());

	// Update original size
	const int64_t offset = ndx ? offsets.Get(ndx-1) : 0;
	const int64_t newSize = GetRefSize((void*)refs.Get(ndx));
	const int64_t oldSize = offsets.Get(ndx) - offset;
	const int64_t diff = newSize - oldSize;
	const int64_t newOffset = offset + newSize;
	offsets.Set(ndx, newOffset);

	// Insert new ref
	const int64_t refSize = GetRefSize(newRef);
	offsets.Insert(ndx+1, newOffset + refSize);
	refs.Insert(ndx+1, (intptr_t)newRef);

	// Update lower offsets
	const int64_t newDiff = diff + refSize;
	return offsets.Increment(newDiff, ndx+2);
}

void Column::Delete(size_t ndx) {
	assert(ndx < Size());

	if (!IsNode()) m_array.Delete(ndx);
	else {
		// Get subnode table
		Array offsets = m_array.GetSubArray(0);
		Array refs = m_array.GetSubArray(1);

		// Find the subnode containing the item
		const size_t node_ndx = offsets.FindPos(ndx);
		assert(node_ndx != -1);

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.Get(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Get sublist
		Column target = GetColumnFromRef(refs, node_ndx);
		target.Delete(local_ndx);

		// Remove ref in node
		if (target.IsEmpty()) {
			offsets.Delete(node_ndx);
			refs.Delete(node_ndx);
			target.Destroy();
		}

		// Update lower offsets
		if (node_ndx < offsets.Size()) offsets.Increment(-1, node_ndx);
	}
}

bool Column::Increment64(int64_t value, size_t start, size_t end) {
	if (!IsNode()) return m_array.Increment(value, start, end);
	else {
		//TODO: partial incr
		Array refs = m_array.GetSubArray(1);
		for (size_t i = 0; i < refs.Size(); ++i) {
			Column col = GetColumnFromRef(refs, i);
			if (!col.Increment64(value)) return false;
		}
		return true;
	}
}

size_t Column::Find(int64_t value, size_t start, size_t end) const {
	assert(start <= Size());
	assert(end == -1 || end <= Size());
	if (IsEmpty()) return (size_t)-1;

	// Use index if possible
	/*if (m_index && start == 0 && end == -1) {
		return FindWithIndex(value);
	}*/

	if (!IsNode()) return m_array.Find(value, start, end);
	else {
		// Get subnode table
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);
		const size_t count = refs.Size();

		if (start == 0 && end == -1) {
			for (size_t i = 0; i < count; ++i) {
				const Column col((void*)refs.Get(i));
				const size_t ndx = col.Find(value);
				if (ndx != -1) {
					const size_t offset = i ? (size_t)offsets.Get(i-1) : 0;
					return offset + ndx;
				}
			}
		}
		else {
			// partial search
			size_t i = offsets.FindPos(start);
			size_t offset = i ? (size_t)offsets.Get(i-1) : 0;
			size_t s = start - offset;
			size_t e = (end == -1 || (int)end >= offsets.Get(i)) ? -1 : end - offset;

			for (;;) {
				const Column col((void*)refs.Get(i));

				const size_t ndx = col.Find(value, s, e);
				if (ndx != -1) {
					const size_t offset = i ? (size_t)offsets.Get(i-1) : 0;
					return offset + ndx;
				}

				++i;
				if (i >= count) break;

				s = 0;
				if (end != -1) {
					if (end >= (size_t)offsets.Get(i)) e = (size_t)-1;
					else {
						offset = (size_t)offsets.Get(i-1);
						e = end - offset;
					}
				}
			}
		}

		return (size_t)-1; // not found
	}
}

void Column::FindAll(Column& result, int64_t value, size_t offset,
					 size_t start, size_t end) const {
	assert(start <= Size());
	assert(end == -1 || end <= Size());

	if (!IsNode()) return m_array.FindAll(result, value, offset, start, end);
	else {
		// Get subnode table
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);
		const size_t count = refs.Size();

		for (size_t i = 0; i < count; ++i) {
			const Column col((void*)refs.Get(i));
			const size_t localOffset = i ? (size_t)offsets.Get(i-1) : 0;
			col.FindAll(result, value, localOffset);
		}
	}
}


size_t Column::FindWithIndex(int64_t target) const {
	assert(m_index);
	assert(m_index->Size() == Size());
	
	return m_index->Find(target);
}

Index& Column::GetIndex() {
	assert(m_index);
	return *m_index;
}

void Column::ClearIndex() {
	m_index = NULL;
}

void Column::BuildIndex(Index& index) {
	index.BuildIndex(*this);
	m_index = &index; // Keep ref to index
}


#ifdef _DEBUG
#include "stdio.h"

void Column::Print() const {
	if (IsNode()) {
		printf("Node: %x\n", m_array.GetRef());
		
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);

		for (size_t i = 0; i < refs.Size(); ++i) {
			printf(" %d: %d %x\n", i, (int)offsets.Get(i), (int)refs.Get(i));
		}
		for (size_t i = 0; i < refs.Size(); ++i) {
			const Column col((void*)refs.Get(i));
			col.Print();
		}
	}
	else {
		m_array.Print();
	}
}

void Column::Verify() const {
	if (IsNode()) {
		assert(m_array.Size() == 2);
		//assert(m_hasRefs);

		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);
		offsets.Verify();
		refs.Verify();
		assert(refs.HasRefs());
		assert(offsets.Size() == refs.Size());

		size_t off = 0;
		for (size_t i = 0; i < refs.Size(); ++i) {
			void* ref = (void*)refs.Get(i);
			assert(ref);

			const Column col(ref);
			col.Verify();

			off += col.Size();
			if (offsets.Get(i) != (int)off) {
				assert(false);
			}
		}
	}
	else m_array.Verify();
}
#endif //_DEBUG
