#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug output
#include <climits> // size_t
#include "query/QueryEngine.h"
#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif

#include "Column.h"
#include "Index.h"

// Pre-declare local functions
void SetRefSize(void* ref, size_t len);
bool callme_sum(Array *a, size_t start, size_t end, size_t caller_base, void *state);
bool callme_min(Array *a, size_t start, size_t end, size_t caller_offset, void *state);
bool callme_max(Array *a, size_t start, size_t end, size_t caller_offset, void *state);
bool callme_arrays(Array *a, size_t start, size_t end, size_t caller_offset, void *state);
void merge_core_references(Array *vals, Array *idx0, Array *idx1, Array *idxres);
void merge_core(Array *a0, Array *a1, Array *res);
Array* merge(Array *ArrayList);
void merge_references(Array *valuelist, Array *indexlists, Array **indexresult);

Column::Column(Allocator& alloc) : m_index(NULL) {
	m_array = new Array(COLUMN_NORMAL, NULL, 0, alloc);
	Create();
}

Column::Column(ColumnDef type, Allocator& alloc) : m_index(NULL) {
	m_array = new Array(type, NULL, 0, alloc);
	Create();
}

Column::Column(ColumnDef type, Array* parent, size_t pndx, Allocator& alloc) : m_index(NULL) {
	m_array = new Array(type, parent, pndx, alloc);
	Create();
}

Column::Column(size_t ref, Array* parent, size_t pndx, Allocator& alloc) : m_index(NULL) {
	m_array = new Array(ref, parent, pndx, alloc);
}

Column::Column(size_t ref, const Array* parent, size_t pndx, Allocator& alloc): m_index(NULL) {
	m_array = new Array(ref, parent, pndx, alloc);
}

Column::Column(const Column& column) : m_index(NULL) {
	m_array = column.m_array; // we now own array
	column.m_array = NULL;    // so invalidate source
}

void Column::Create() {
	// Add subcolumns for nodes
	if (IsNode()) {
		const Array offsets(COLUMN_NORMAL, NULL, 0, m_array->GetAllocator());
		const Array refs(COLUMN_HASREFS, NULL, 0, m_array->GetAllocator());
		m_array->Add(offsets.GetRef());
		m_array->Add(refs.GetRef());
	}
}

void Column::UpdateRef(size_t ref) {
	m_array->UpdateRef(ref);
}

bool Column::operator==(const Column& column) const {
	return *m_array == *(column.m_array);
}

Column::~Column() {
	delete m_array;
	delete m_index; // does not destroy index!
}

void Column::Destroy() {
	ClearIndex();
	if(m_array != NULL)
		m_array->Destroy();
}


bool Column::IsEmpty() const {
	if (!IsNode()) return m_array->IsEmpty();
	else {
		const Array offsets = NodeGetOffsets();
		return offsets.IsEmpty();
	}
}

size_t Column::Size() const {
	if (!IsNode()) return m_array->Size();
	else {
		const Array offsets = NodeGetOffsets();
		return offsets.IsEmpty() ? 0 : (size_t)offsets.Back();
	}
}

void Column::SetParent(Array* parent, size_t pndx) {
	m_array->SetParent(parent, pndx);
}

void Column::UpdateParentNdx(int diff) {
	m_array->UpdateParentNdx(diff);
}

// Used by column b-tree code to ensure all leaf having same type
void Column::SetHasRefs() {
	m_array->SetType(COLUMN_HASREFS);
}

void Column::GetParentInfo(size_t ndx, Array*& parent, size_t& pndx, size_t offset) const {
	if (IsNode()) {
		// Get subnode table
		const Array offsets = NodeGetOffsets();
		const Array refs    = NodeGetRefs();

		// Find the subnode containing the item
		const size_t node_ndx = offsets.FindPos(ndx);

		// Calc index in subnode
		const size_t local_offset = node_ndx ? (size_t)offsets.Get(node_ndx-1) : 0;
		const size_t local_ndx    = ndx - local_offset;

		// Get parent info
		const Column target = GetColumnFromRef<Column>(refs, node_ndx);
		target.GetParentInfo(local_ndx, parent, pndx, offset + local_offset);
	}
	else {
		parent = m_array;
		pndx   = ndx + offset;
	}
}

static Column GetColumnFromRef(Array& parent, size_t ndx) {
	assert(parent.HasRefs());
	assert(ndx < parent.Size());
	return Column((size_t)parent.Get(ndx), &parent, ndx, parent.GetAllocator());
}

/*
static const Column GetColumnFromRef(const Array& parent, size_t ndx) {
	assert(parent.HasRefs());
	assert(ndx < parent.Size());
	return Column((size_t)parent.Get(ndx), &parent, ndx);
}
*/


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
	m_array->Clear();
	if (m_array->IsNode()) m_array->SetType(COLUMN_NORMAL);
}

int64_t Column::Get(size_t ndx) const {
	return TreeGet<int64_t, Column>(ndx);
}

size_t Column::GetAsRef(size_t ndx) const {
	return TO_REF(TreeGet<int64_t, Column>(ndx));
}

bool Column::Set(size_t ndx, int64_t value) {
	const int64_t oldVal = m_index ? Get(ndx) : 0; // cache oldval for index

	const bool res = TreeSet<int64_t, Column>(ndx, value);
	if (!res) return false;

	// Update index
	if (m_index) m_index->Set(ndx, oldVal, value);

#ifdef _DEBUG
	Verify();
#endif //DEBUG

	return true;
}

bool Column::Add(int64_t value) {
	return Insert(Size(), value);
}

bool Column::Insert(size_t ndx, int64_t value) {
	assert(ndx <= Size());

	const bool res = TreeInsert<int64_t, Column>(ndx, value);
	if (!res) return false;

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

bool callme_sum(Array *a, size_t start, size_t end, size_t caller_base, void *state) {
	(void)caller_base; 
	int64_t s = a->Sum(start, end);
	*(int64_t *)state += s;
	return true;
}

int64_t Column::Sum(size_t start, size_t end) const {
	int64_t sum = 0;
	TreeVisitLeafs<Array, Column>(start, end, 0, callme_sum, (void *)&sum);
	return sum;
}

class AggregateState {
public:
	AggregateState() : isValid(false), result(0) {}
	bool    isValid;
	int64_t result;
};

bool callme_min(Array *a, size_t start, size_t end, size_t caller_offset, void *state) {
	(void)caller_offset;
	AggregateState* p = (AggregateState*)state;

	int64_t res;
	if (!a->Min(res, start, end)) return true;

	if (!p->isValid || (res < p->result)) {
		p->result  = res;
		p->isValid = true;
	}
	return true;
}

int64_t Column::Min(size_t start, size_t end) const {
	AggregateState state;
	TreeVisitLeafs<Array, Column>(start, end, 0, callme_min, (void *)&state);
	return state.result; // will return zero for empty ranges
}

bool callme_max(Array *a, size_t start, size_t end, size_t caller_offset, void *state) {
	(void)caller_offset;
	AggregateState* p = (AggregateState*)state;

	int64_t res;
	if (!a->Max(res, start, end)) return true;

	if (!p->isValid || (res > p->result)) {
		p->result  = res;
		p->isValid = true;
	}
	return true;
}

int64_t Column::Max(size_t start, size_t end) const {
	AggregateState state;
	TreeVisitLeafs<Array, Column>(start, end, 0, callme_max, (void *)&state);
	return state.result; // will return zero for empty ranges
}

// Input: 
//     vals:   An array of values 
//     idx0:   Array of indexes pointing into vals, sorted with respect to vals
//     idx1:   Array of indexes pointing into vals, sorted with respect to vals
//     idx0 and idx1 are allowed not to contain index pointers to *all* elements in vals
//     (idx0->Size() + idx1->Size() < vals.Size() is OK).
// Output:
//     idxres: Merged array of indexes sorted with respect to vals
void merge_core_references(Array *vals, Array *idx0, Array *idx1, Array *idxres) {

	int64_t v0, v1;
	size_t i0, i1;
	size_t p0 = 0, p1 = 0;
	size_t s0 = idx0->Size();
	size_t s1 = idx1->Size();

	i0 = idx0->GetAsRef(p0++);
	i1 = idx1->GetAsRef(p1++);
	v0 = vals->Get(i0);
	v1 = vals->Get(i1);

	for(;;) {
		if(v0 < v1) {
			idxres->Add(i0);
			// Only check p0 if it has been modified :)
			if(p0 == s0)
				break;
			i0 = idx0->GetAsRef(p0++);
			v0 = vals->Get(i0);
		}
		else {
			idxres->Add(i1);
			if(p1 == s1)
				break;
			i1 = idx1->GetAsRef(p1++);
			v1 = vals->Get(i1);
		}
	}

	if(p0 == s0)
		p0--;
	else
		p1--;

	while(p0 < s0) {
		i0 = idx0->GetAsRef(p0++);
		v0 = vals->Get(i0);
		idxres->Add(i0);
	}
	while(p1 < s1) {
		i1 = idx1->GetAsRef(p1++);
		v1 = vals->Get(i1);
		idxres->Add(i1);
	}

	assert(idxres->Size() == idx0->Size() + idx1->Size());
}


// Merge two sorted arrays into a single sorted array
void merge_core(Array *a0, Array *a1, Array *res) {
	int64_t v0, v1;
	size_t p0 = 0, p1 = 0;
	size_t s0 = a0->Size();
	size_t s1 = a1->Size();

	v0 = a0->Get(p0++);
	v1 = a1->Get(p1++);

	for(;;) {
		if(v0 < v1) {
			res->Add(v0);
			if(p0 == s0)
				break;
			v0 = a0->Get(p0++);
		}
		else {
			res->Add(v1);
			if(p1 == s1)
				break;
			v1 = a1->Get(p1++);
		}
	}

	if(p0 == s0)
		p0--;
	else
		p1--;

	while(p0 < s0) {
		v0 = a0->Get(p0++);
		res->Add(v0);
	}
	while(p1 < s1) {
		v1 = a1->Get(p1++);
		res->Add(v1);
	}

	assert(res->Size() == a0->Size() + a1->Size());
}


// Input: 
//     ArrayList: An array of references to non-instantiated Arrays of values. The values in each array must be in sorted order
// Return value:
//     Merge-sorted array of all values
Array *merge(Array *ArrayList) {
	if(ArrayList->Size() == 1) {
		size_t ref = ArrayList->GetAsRef(0);
		Array *a = new Array(ref, (Array *)&merge);
		return a;
	}
	
	Array Left, Right;
	size_t left = ArrayList->Size() / 2;
	for(size_t t = 0; t < left; t++)
		Left.Add(ArrayList->Get(t));
	for(size_t t = left; t < ArrayList->Size(); t++)
		Right.Add(ArrayList->Get(t));

	Array *l;
	Array *r;
	Array *res = new Array();

	// We merge left-half-first instead of bottom-up so that we access the same data in each call
	// so that it's in cache, at least for the first few iterations until lists get too long
	l = merge(&Left);
	r = merge(&Right);
	merge_core(l, r, res);
	return res;
}

// Input: 
//     valuelist:   One array of values
//     indexlists:  Array of pointers to non-instantiated Arrays of index numbers into valuelist
// Output:
//     indexresult: Array of indexes into valuelist, sorted with respect to values in valuelist
// TODO: Set owner of created arrays and Destroy/delete them if created by merge_references()
void merge_references(Array *valuelist, Array *indexlists, Array **indexresult) {
	if(indexlists->Size() == 1) {
//		size_t ref = valuelist->Get(0);
		*indexresult = (Array *)indexlists->Get(0);
		return;
	}
	
	Array LeftV, RightV;
	Array LeftI, RightI;
	size_t left = indexlists->Size() / 2;
	for(size_t t = 0; t < left; t++) {
		LeftV.Add(indexlists->Get(t));
		LeftI.Add(indexlists->Get(t));
	}
	for(size_t t = left; t < indexlists->Size(); t++) {
		RightV.Add(indexlists->Get(t));
		RightI.Add(indexlists->Get(t));
	}

	Array *li;
	Array *ri;

	Array *ResI = new Array();

	// We merge left-half-first instead of bottom-up so that we access the same data in each call
	// so that it's in cache, at least for the first few iterations until lists get too long
	merge_references(valuelist, &LeftI, &ri);
	merge_references(valuelist, &RightI, &li);
	merge_core_references(valuelist, li, ri, ResI);

	*indexresult = ResI;
}



bool callme_arrays(Array *a, size_t start, size_t end, size_t caller_offset, void *state) {
	(void)end;
	(void)start;
	(void)caller_offset;
	Array* p = (Array*)state;
	size_t ref = a->GetRef();
	p->Add((int64_t)ref); // todo, check cast
	return true;
}

void Column::Sort(size_t start, size_t end) {
	Array arr;
	TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&arr);
	for(size_t t = 0; t < arr.Size(); t++) {	
		size_t ref = arr.GetAsRef(t);
		Array a(ref);
		a.Sort();
	}

	Array *sorted = merge(&arr);
	Clear();

	// Todo, this is a bit slow. Add bulk insert or the like to Column
	for(size_t t = 0; t < sorted->Size(); t++) {
		Insert(t, sorted->Get(t));
	}
}


// TODO: Set owner of created arrays and Destroy/delete them if created by merge_references()
void Column::ReferenceSort(size_t start, size_t end, Column &ref) {
	Array values; // pointers to non-instantiated arrays of values
	Array indexes; // pointers to instantiated arrays of index pointers
	Array all_values;
	TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&values);

	size_t offset = 0;
	for(size_t t = 0; t < values.Size(); t++) {
		Array *i = new Array();
		size_t ref = values.GetAsRef(t);
		Array v(ref);
		for(size_t j = 0; j < v.Size(); j++)
			all_values.Add(v.Get(j));
		v.ReferenceSort(*i);
		for(size_t n = 0; n < v.Size(); n++)
			i->Set(n, i->Get(n) + offset);
		offset += v.Size();
		indexes.Add((int64_t)i);
	}

	Array *ResI;

	merge_references(&all_values, &indexes, &ResI);

	for(size_t t = 0; t < ResI->Size(); t++)
		ref.Add(ResI->Get(t));
}

size_t ColumnBase::GetRefSize(size_t ref) const {
	// parse the length part of 8byte header
	const uint8_t* const header = (uint8_t*)m_array->GetAllocator().Translate(ref);
	return (header[1] << 16) + (header[2] << 8) + header[3];
}

Array ColumnBase::NodeGetOffsets() {
	assert(IsNode());
	return m_array->GetSubArray(0);
}

const Array ColumnBase::NodeGetOffsets() const {
	assert(IsNode());
	return m_array->GetSubArray(0);
}

Array ColumnBase::NodeGetRefs() {
	assert(IsNode());
	return m_array->GetSubArray(1);
}

const Array ColumnBase::NodeGetRefs() const {
	assert(IsNode());
	return m_array->GetSubArray(1);
}

bool ColumnBase::NodeUpdateOffsets(size_t ndx) {
	assert(IsNode());

	Array offsets = NodeGetOffsets();
	Array refs = NodeGetRefs();
	assert(ndx < offsets.Size());

	const int64_t newSize = GetRefSize((size_t)refs.Get(ndx));
	const int64_t oldSize = offsets.Get(ndx) - (ndx ? offsets.Get(ndx-1) : 0);
	const int64_t diff = newSize - oldSize;
	
	return offsets.Increment(diff, ndx);
}

void Column::Delete(size_t ndx) {
	assert(ndx < Size());

	const int64_t oldVal = m_index ? Get(ndx) : 0; // cache oldval for index

	TreeDelete<int64_t, Column>(ndx);

	// Flatten tree if possible
	while (IsNode()) {
		Array refs = NodeGetRefs();
		if (refs.Size() != 1) break;

		const size_t ref = refs.GetAsRef(0);
		refs.Delete(0); // avoid destroying subtree
		m_array->Destroy();
		m_array->UpdateRef(ref);
	}

	// Update index
	if (m_index) {
		const bool isLast = (ndx == Size());
		m_index->Delete(ndx, oldVal, isLast);
	}
}

bool Column::Increment64(int64_t value, size_t start, size_t end) {
	if (!IsNode()) return m_array->Increment(value, start, end);
	else {
		//TODO: partial incr
		Array refs = NodeGetRefs();
		for (size_t i = 0; i < refs.Size(); ++i) {
			Column col = GetColumnFromRef(refs, i);
			if (!col.Increment64(value)) return false;
		}
		return true;
	}
}

size_t Column::Find(int64_t value, size_t start, size_t end) const {
	assert(start <= Size());
	assert(end == (size_t)-1 || end <= Size());
	if (IsEmpty()) return (size_t)-1;
	return TreeFind<int64_t, Column, EQUAL>(value, start, end);
}

void Column::FindAll(Array& result, int64_t value, size_t caller_offset, size_t start, size_t end) const {
	(void)caller_offset;
	assert(start <= Size());
	assert(end == (size_t)-1 || end <= Size());
	if (IsEmpty()) return;
	TreeFindAll<int64_t, Column>(result, value, 0, start, end);
}

void Column::LeafFindAll(Array &result, int64_t value, size_t add_offset, size_t start, size_t end) const {
	return m_array->FindAll(result, value, add_offset, start, end);
}

void Column::FindAllHamming(Array& result, uint64_t value, size_t maxdist, size_t offset) const {
	if (!IsNode()) {
		m_array->FindAllHamming(result, value, maxdist, offset);
	}
	else {
		// Get subnode table
		const Array offsets = NodeGetOffsets();
		const Array refs = NodeGetRefs();
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
		return m_array->FindPos(target);
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
	Sort(0, Size());
}


#ifdef _DEBUG
#include "stdio.h"

bool Column::Compare(const Column& c) const {
	if (c.Size() != Size()) return false;

	for (size_t i = 0; i < Size(); ++i) {
		if (Get(i) != c.Get(i)) return false;
	}

	return true;
}

void Column::Print() const {
	if (IsNode()) {
		printf("Node: %zx\n", m_array->GetRef());
		
		const Array offsets = NodeGetOffsets();
		const Array refs = NodeGetRefs();

		for (size_t i = 0; i < refs.Size(); ++i) {
			printf(" %zu: %d %x\n", i, (int)offsets.Get(i), (int)refs.Get(i));
		}
		for (size_t i = 0; i < refs.Size(); ++i) {
			const Column col((size_t)refs.Get(i));
			col.Print();
		}
	}
	else {
		m_array->Print();
	}
}

void Column::Verify() const {
	if (IsNode()) {
		assert(m_array->Size() == 2);
		//assert(m_hasRefs);

		const Array offsets = NodeGetOffsets();
		const Array refs = NodeGetRefs();
		offsets.Verify();
		refs.Verify();
		assert(refs.HasRefs());
		assert(offsets.Size() == refs.Size());

		size_t off = 0;
		for (size_t i = 0; i < refs.Size(); ++i) {
			const size_t ref = (size_t)refs.Get(i);
			assert(ref);

			const Column col(ref, (const Array*)NULL, 0, m_array->GetAllocator());
			col.Verify();

			off += col.Size();
			const size_t node_off = offsets.GetAsRef(i);
			if (node_off != off) {
				assert(false);
			}
		}
	}
	else m_array->Verify();
}

void ColumnBase::ToDot(std::ostream& out, const char* title) const {
	const size_t ref = GetRef();
	
	out << "subgraph cluster_column" << ref << " {" << std::endl;
	out << " label = \"Column";
	if (title) out << "\\n'" << title << "'";
	out << "\";" << std::endl;
	
	ArrayToDot(out, *m_array);
	
	out << "}" << std::endl;
}

void ColumnBase::ArrayToDot(std::ostream& out, const Array& array) const {
	if (array.IsNode()) {
		const Array offsets = array.GetSubArray(0);
		const Array refs    = array.GetSubArray(1);
		const size_t ref    = array.GetRef();
		
		out << "subgraph cluster_node" << ref << " {" << std::endl;
		out << " label = \"Node\";" << std::endl;
		
		array.ToDot(out);
		offsets.ToDot(out, "offsets");
		
		out << "}" << std::endl;
		
		refs.ToDot(out, "refs");
		
		const size_t count = refs.Size();
		for (size_t i = 0; i < count; ++i) {
			const Array r = refs.GetSubArray(i);
			ArrayToDot(out, r);
		}
	}
	else LeafToDot(out, array);
}

void ColumnBase::LeafToDot(std::ostream& out, const Array& array) const {
	array.ToDot(out);
}

MemStats Column::Stats() const {
	MemStats stats(m_array->Stats());

	if (m_array->IsNode()) {
		const Array refs = NodeGetRefs();

		for (size_t i = 0; i < refs.Size(); ++i) {
			const size_t r = (size_t)refs.Get(i);
			const Column col(r);

			const MemStats m = col.Stats();
			stats.Add(m);
		}
	}

	return stats;
}


#endif //_DEBUG



