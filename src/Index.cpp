#include "Index.h"
#include <cassert>

#define MAX_LIST_SIZE 1000

Index::Index() : Column(COLUMN_HASREFS) {
	// Add subcolumns for leafs
	const Array values(COLUMN_NORMAL);
	const Array refs(COLUMN_NORMAL); // we do not own these refs (to column positions), so no COLUMN_HASREF
	m_array.Add((intptr_t)values.GetRef());
	m_array.Add((intptr_t)refs.GetRef());
}

Index::Index(ColumnDef type, Array* parent, size_t pndx) : Column(type, parent, pndx) {
}

Index::Index(void* ref) : Column(ref) {
}

Index::Index(void* ref, Array* parent, size_t pndx) : Column(ref, parent, pndx) {
}


void Index::BuildIndex(const Column& src) {
	//assert(IsEmpty());

	// Brute-force build-up
	// TODO: sort and merge
	for (size_t i = 0; i < src.Size(); ++i) {
		Insert64(i, src.Get64(i));
	}

#ifdef _DEBUG
	Verify();
#endif //_DEBUG
}

bool Index::Insert64(size_t ndx, int64_t value) {
	const NodeChange nc = DoInsert(ndx, value);
	switch (nc.type) {
	case NodeChange::CT_ERROR:
		return false; // allocation error
	case NodeChange::CT_NONE:
		break;
	case NodeChange::CT_INSERT_BEFORE:
		{
			Index newNode(COLUMN_NODE);
			newNode.NodeAdd(nc.ref1);
			newNode.NodeAdd(GetRef());
			m_array.UpdateRef(newNode.GetRef());
			break;
		}
	case NodeChange::CT_INSERT_AFTER:
		{
			Index newNode(COLUMN_NODE);
			newNode.NodeAdd(GetRef());
			newNode.NodeAdd(nc.ref1);
			m_array.UpdateRef(newNode.GetRef());
			break;
		}
	case NodeChange::CT_SPLIT:
		{
			Index newNode(COLUMN_NODE);
			newNode.NodeAdd(nc.ref1);
			newNode.NodeAdd(nc.ref2);
			m_array.UpdateRef(newNode.GetRef());
			break;
		}
	default:
		assert(false);
		return false;
	}

	return true;
}

static Index GetIndexFromRef(Array& parent, size_t ndx) {
	assert(parent.HasRefs());
	assert(ndx < parent.Size());
	return Index((void*)parent.Get(ndx), &parent, ndx);
}

bool Index::LeafInsert(size_t ref, int64_t value) {
	assert(!IsNode());

	// Get subnode table
	Array values = m_array.GetSubArray(0);
	Array refs = m_array.GetSubArray(1);

	const size_t ins_pos = values.FindPos2(value);

	if (ins_pos == -1) {
		values.Add(value);
		refs.Add(ref);
	}
	else {
		values.Insert(ins_pos, value);
		refs.Insert(ins_pos, ref);
	}

	return true;
}

bool Index::NodeAdd(void* ref) {assert(ref);
	assert(IsNode());

	const Index col(ref);
	assert(!col.IsEmpty());
	const int64_t maxval = col.MaxValue();

	Array offsets = m_array.GetSubArray(0);
	Array refs = m_array.GetSubArray(1);

	const size_t ins_pos = offsets.FindPos2(maxval);

	if (ins_pos == -1) {
		offsets.Add(maxval);
		refs.Add((intptr_t)ref);
	}
	else {
		offsets.Insert(ins_pos, maxval);
		refs.Insert(ins_pos, (intptr_t)ref);
	}

	return true;
}

int64_t Index::MaxValue() const {
	const Array values = m_array.GetSubArray(0);
	return values.IsEmpty() ? 0 : values.Back();
}

Column::NodeChange Index::DoInsert(size_t ndx, int64_t value) {
	if (IsNode()) {
		// Get subnode table
		Array offsets = m_array.GetSubArray(0);
		Array refs = m_array.GetSubArray(1);

		// Find the subnode containing the item
		size_t node_ndx = offsets.FindPos2(ndx);
		if (node_ndx == -1) {
			// node can never be empty, so try to fit in last item
			node_ndx = offsets.Size()-1;
		}

		// Calc index in subnode
		const size_t offset = node_ndx ? (size_t)offsets.Get(node_ndx-1) : 0;
		const size_t local_ndx = ndx - offset;

		// Get sublist
		Index target = GetIndexFromRef(refs, node_ndx);

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
		Index newNode(COLUMN_NODE);
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
		if (Size() < MAX_LIST_SIZE) {
			return LeafInsert(ndx, value);
		}

		// Create new list for item
		Index newList;
		if (!newList.LeafInsert(ndx, value)) return NodeChange(NodeChange::CT_ERROR);
		
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

size_t Index::Find(int64_t value) {
	void* ref = GetRef();
	for (;;) {
		const Array node(ref);
		const Array values = node.GetSubArray(0);
		const Array refs = node.GetSubArray(1);

		const size_t pos = values.FindPos2(value);

		if (pos == -1) return (size_t)-1;
		else if (!m_array.IsNode()) return refs.Get(pos);
		
		ref = (void*)refs.Get(pos);
	}
}

/*
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

*/

#ifdef _DEBUG

void Index::Verify() const {
	assert(m_array.Size() == 2);
	assert(m_array.HasRefs());

	const Array offsets = m_array.GetSubArray(0);
	const Array refs = m_array.GetSubArray(1);
	offsets.Verify();
	refs.Verify();
	assert(offsets.Size() == refs.Size());
	
	if (m_array.IsNode()) {
		assert(refs.HasRefs());

		// Make sure that all offsets matches biggest value in ref
		for (size_t i = 0; i < refs.Size(); ++i) {
			void* ref = (void*)refs.Get(i);
			assert(ref);

			const Index col(ref);
			col.Verify();

			if (offsets.Get(i) != col.MaxValue()) {
				assert(false);
			}
		}
	}
	else {
		assert(!refs.HasRefs());
	}
}

#endif //_DEBUG