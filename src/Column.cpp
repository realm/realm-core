#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug output
#include <climits> // size_t

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif

#include "Column.h"
#include "Index.h"

#ifndef MAX_LIST_SIZE
#define MAX_LIST_SIZE 1000
#endif

// Pre-declare local functions
void SetRefSize(void* ref, size_t len);

Column::Column(Allocator& alloc) : m_array(COLUMN_NORMAL, NULL, 0, alloc),  m_index(NULL) {
	Create();
}

Column::Column(ColumnDef type, Allocator& alloc) : m_array(type, NULL, 0, alloc),  m_index(NULL) {
	Create();
}

Column::Column(ColumnDef type, Array* parent, size_t pndx, Allocator& alloc) : m_array(type, parent, pndx, alloc),  m_index(NULL) {
	Create();
}

Column::Column(size_t ref, Array* parent, size_t pndx, Allocator& alloc) : m_array(ref, parent, pndx, alloc), m_index(NULL) {
}

Column::Column(size_t ref, const Array* parent, size_t pndx, Allocator& alloc): m_array(ref, parent, pndx, alloc), m_index(NULL) {
}

Column::Column(const Column& column) : m_array(column.m_array), m_index(NULL) {
}

void Column::Create() {
	// Add subcolumns for nodes
	if (IsNode()) {
		const Array offsets(COLUMN_NORMAL, NULL, 0, m_array.GetAllocator());
		const Array refs(COLUMN_HASREFS, NULL, 0, m_array.GetAllocator());
		m_array.Add((intptr_t)offsets.GetRef());
		m_array.Add((intptr_t)refs.GetRef());
	}
}

bool Column::operator==(const Column& column) const {
	return m_array == column.m_array;
}

Column::~Column() {
	delete m_index; // does not destroy index!
}

void Column::Destroy() {
	ClearIndex();
	m_array.Destroy();
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
	return Column((size_t)parent.Get(ndx), &parent, ndx, parent.GetAllocator());
}

static const Column GetColumnFromRef(const Array& parent, size_t ndx) {
	assert(parent.HasRefs());
	assert(ndx < parent.Size());
	return Column((size_t)parent.Get(ndx), &parent, ndx);
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
	const int64_t oldVal = m_index ? Get64(ndx) : 0; // cache oldval for index

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

	// Update index
	if (m_index) m_index->Set(ndx, oldVal, value);

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

	// Update index
	if (m_index) {
		const bool isLast = (ndx+1 == Size());
		m_index->Insert(ndx, value, isLast);
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
				newNode.NodeAdd((size_t)refs.Get(i));
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

size_t Column::GetRefSize(size_t ref) const {
	// parse the length part of 8byte header
	const uint8_t* const header = (uint8_t*)m_array.GetAllocator().Translate(ref);
	return (header[1] << 16) + (header[2] << 8) + header[3];
}

void SetRefSize(void* ref, size_t len) {
	uint8_t* const header = (uint8_t*)(ref);
	header[1] = ((len >> 16) & 0x000000FF);
	header[2] = (len >> 8) & 0x000000FF;
	header[3] = len & 0x000000FF;
}

bool Column::NodeInsert(size_t ndx, size_t ref) {
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

bool Column::NodeAdd(size_t ref) {
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

	const int64_t newSize = GetRefSize((size_t)refs.Get(ndx));
	const int64_t oldSize = offsets.Get(ndx) - (ndx ? offsets.Get(ndx-1) : 0);
	const int64_t diff = newSize - oldSize;
	
	return offsets.Increment(diff, ndx);
}

bool Column::NodeInsertSplit(size_t ndx, size_t newRef) {
	assert(IsNode());
	assert(newRef);

	Array offsets = m_array.GetSubArray(0);
	Array refs = m_array.GetSubArray(1);
	assert(ndx < offsets.Size());

	// Update original size
	const int64_t offset = ndx ? offsets.Get(ndx-1) : 0;
	const int64_t newSize = GetRefSize((size_t)refs.Get(ndx));
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

	const int64_t oldVal = m_index ? Get64(ndx) : 0; // cache oldval for index

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

	// Update index
	if (m_index) {
		const bool isLast = (ndx == Size());
		m_index->Delete(ndx, oldVal, isLast);
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
				const Column col((size_t)refs.Get(i));
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
				const Column col((size_t)refs.Get(i));

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
	if (IsEmpty()) return;

	if (!IsNode()) return m_array.FindAll(result, value, offset, start, end);
	else {
		// Get subnode table
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);
		const size_t count = refs.Size();

		for (size_t i = 0; i < count; ++i) {
			const Column col((size_t)refs.Get(i));
			const size_t localOffset = i ? (size_t)offsets.Get(i-1) : 0;
			col.FindAll(result, value, (localOffset+offset));
		}
	}
}

void Column::FindAllHamming(Column& result, uint64_t value, size_t maxdist, size_t offset) const {
	if (!IsNode()) {
		m_array.FindAllHamming(result, value, maxdist, offset);
	}
	else {
		// Get subnode table
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);
		const size_t count = refs.Size();

		for (size_t i = 0; i < count; ++i) {
			const Column col((size_t)refs.Get(i));
			col.FindAllHamming(result, value, maxdist, offset);
			offset += (size_t)offsets.Get(i);
		}
	}
}

size_t Column::FindPos(int64_t target) const {
	// NOTE: Binary search only works if the column is sorted

	if (!IsNode()) {
		return m_array.FindPos(target);
	}

	const int len = (int)Size();
	int low = -1;
	int high = len;

	// Binary search based on:
	// http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
	// Finds position of largest value SMALLER than the target
	while (high - low > 1) {
		const size_t probe = ((unsigned int)low + (unsigned int)high) >> 1;
		const int64_t v = Get(probe);

		if (v > target) high = (int)probe;
		else            low = (int)probe;
	}
	if (high == len) return (size_t)-1;
	else return high;
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
	if (m_index) {
		m_index->Destroy();
		delete m_index;
		m_index = NULL;
	}
}

void Column::BuildIndex(Index& index) {
	index.BuildIndex(*this);
	m_index = &index; // Keep ref to index
}

void Column::Sort() {
	DoSort(0, Size()-1);
}

void Column::DoSort(size_t lo, size_t hi) {
	//TODO: This is pretty slow. A better stategy will be to
	//      sort each leaf/array on it's own and then merge

	// Quicksort based on
	// http://www.inf.fh-flensburg.de/lang/algorithmen/sortieren/quick/quicken.htm
	int i = (int)lo;
	int j = (int)hi;

	// comparison element x
	const size_t ndx = (lo + hi)/2;
	const int64_t x = (size_t)Get64(ndx);

	// partition
	do {
		while (Get64(i) < x) i++;
		while (Get64(j) > x) j--;
		if (i <= j) {
			const int64_t h = Get64(i);
			Set64(i, Get64(j));
			Set64(j, h);
			i++; j--;
		}
	} while (i <= j);

	//  recursion
	if ((int)lo < j) DoSort(lo, j);
	if (i < (int)hi) DoSort(i, hi);
}

size_t Column::Write(std::ostream& out, size_t& pos) const {
	if (IsNode()) {
		// First write out all sub-arrays
		const Array refs = m_array.GetSubArray(1);
		Array newRefs;
		for (size_t i = 0; i < refs.Size(); ++i) {
			const Column col((size_t)refs.Get(i));
			const size_t sub_pos = col.Write(out, pos);
			newRefs.Add(sub_pos);
		}

		// Write (new) refs
		const size_t refs_pos = pos;
		pos += newRefs.Write(out);

		// Write offsets
		const size_t offsets_pos = pos;
		const Array offsets = m_array.GetSubArray(0);
		pos += offsets.Write(out);

		// Write new array with node info
		const size_t node_pos = pos;
		Array node(COLUMN_NODE);
		node.Add(offsets_pos);
		node.Add(refs_pos);
		pos += node.Write(out);

		return node_pos;
	}
	else {
		const size_t array_pos = pos;
		pos += m_array.Write(out);
		return array_pos;
	}
}


#ifdef _DEBUG
#include "stdio.h"

bool Column::Compare(const Column& c) const {
	if (c.Size() != Size()) return false;

	for (size_t i = 0; i < Size(); ++i) {
		if (Get64(i) != c.Get64(i)) return false;
	}

	return true;
}

void Column::Print() const {
	if (IsNode()) {
		printf("Node: %zx\n", m_array.GetRef());
		
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);

		for (size_t i = 0; i < refs.Size(); ++i) {
			printf(" %zu: %d %x\n", i, (int)offsets.Get(i), (int)refs.Get(i));
		}
		for (size_t i = 0; i < refs.Size(); ++i) {
			const Column col((size_t)refs.Get(i));
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
			const size_t ref = (size_t)refs.Get(i);
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

void Column::ToDot(FILE* f, bool isTop) const {
	const size_t ref = m_array.GetRef();
	if (isTop) fprintf(f, "subgraph cluster_%zu {\ncolor=black;\nstyle=dashed;\n", ref);

	if (m_array.IsNode()) {
		const Array offsets = m_array.GetSubArray(0);
		const Array refs = m_array.GetSubArray(1);

		fprintf(f, "n%zx [label=\"", ref);
		for (size_t i = 0; i < offsets.Size(); ++i) {
			if (i > 0) fprintf(f, " | ");
			fprintf(f, "{%lld", offsets.Get(i));
			fprintf(f, " | <%zu>}", i);
		}
		fprintf(f, "\"];\n");

		for (size_t i = 0; i < refs.Size(); ++i) {
			void* r = (void*)refs.Get(i);
			fprintf(f, "n%zx:%zu -> n%p\n", ref, i, r);
		}

		// Sub-columns
		for (size_t i = 0; i < refs.Size(); ++i) {
			const size_t r = (size_t)refs.Get(i);
			const Column col(r);
			col.ToDot(f, false);
		}
	}
	else m_array.ToDot(f, false);

	if (isTop) fprintf(f, "}\n\n");
}

#endif //_DEBUG
