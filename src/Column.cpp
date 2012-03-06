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

#include "UnitTest++.h"

// Pre-declare local functions
void SetRefSize(void* ref, size_t len);
bool callme_sum(Array &a, size_t start, size_t end, size_t caller_base, void *state);
bool callme_min(Array &a, size_t start, size_t end, size_t caller_offset, void *state);
bool callme_max(Array &a, size_t start, size_t end, size_t caller_offset, void *state);

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
	m_array->Clear();
	if (m_array->IsNode()) m_array->SetType(COLUMN_NORMAL);
}

int64_t Column::Get(size_t ndx) const {
	return TreeGet<int64_t, Column>(ndx);
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

	i0 = idx0->Get(p0++);
	i1 = idx1->Get(p1++);
	v0 = vals->Get(i0);
	v1 = vals->Get(i1);

	for(;;) {
		if(v0 < v1) {
			idxres->Add(i0);
			// Only check p0 if it has been modified :)
			if(p0 == s0)
				break;
			i0 = idx0->Get(p0++);
			v0 = vals->Get(i0);
		}
		else {
			idxres->Add(i1);
			if(p1 == s1)
				break;
			i1 = idx1->Get(p1++);
			v1 = vals->Get(i1);
		}
	}

	if(p0 == s0)
		p0--;
	else
		p1--;

	while(p0 < s0) {
		i0 = idx0->Get(p0++);
		v0 = vals->Get(i0);
		idxres->Add(i0);
	}
	while(p1 < s1) {
		i1 = idx1->Get(p1++);
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
		size_t ref = ArrayList->Get(0);
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
void merge_references(Array *valuelist, Array *indexlists, Array **indexresult) {
	if(indexlists->Size() == 1) {
		size_t ref = valuelist->Get(0);
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
	Array* p = (Array*)state;
	size_t ref = a->GetRef();
	p->Add((int64_t)ref); // todo, check cast
	return true;
}

void Column::Sort(size_t start, size_t end) {
	Array arr;
	TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&arr);
	for(size_t t = 0; t < arr.Size(); t++) {	
		size_t ref = arr.Get(t);
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


void Column::ReferenceSort(size_t start, size_t end, Column &ref) {
	Array values; // pointers to non-instantiated arrays of values
	Array indexes; // pointers to instantiated arrays of index pointers
	Array all_values;
	TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&values);

	size_t offset = 0;
	for(size_t t = 0; t < values.Size(); t++) {
		Array *i = new Array();
		size_t ref = values.Get(t);
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

		const size_t ref = refs.Get(0);
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
	Sort(0, Size()-1);
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
			const size_t node_off = offsets.Get(i);
			if (node_off != off) {
				assert(false);
			}
		}
	}
	else m_array->Verify();
}

void Column::ToDot(FILE* f, bool isTop) const {
	const size_t ref = m_array->GetRef();
	if (isTop) fprintf(f, "subgraph cluster_%zu {\ncolor=black;\nstyle=dashed;\n", ref);

	if (m_array->IsNode()) {
		const Array offsets = NodeGetOffsets();
		const Array refs = NodeGetRefs();

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
	else m_array->ToDot(f, false);

	if (isTop) fprintf(f, "}\n\n");
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






/*
//56 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	int64_t v0, v1;
	size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
	size_t s1 = a1->Size();
	size_t s2 = a2->Size();

	v0 = a1->Get<8>(p0);
	v1 = a2->Get<8>(p1);

	for(size_t i = 0; p0  < s1; i++) {


		if(v0 < v1) {
			wush += v0;
			p0++;
			v0 = a1->Get<8>(p0);

			if(v0 < v1) {
				wush += v0;
				p0++;
				v0 = a1->Get<8>(p0);
			}
			else {
				wush += v1;
				p1++;
				v1 = a2->Get<8>(p1);
			}



		}
		else {
			wush += v1;
			p1++;
			v1 = a2->Get<8>(p1);

			if(v0 < v1) {
				wush += v0;
				p0++;
				v0 = a1->Get<8>(p0);
			}
			else {
				wush += v1;
				p1++;
				v1 = a2->Get<8>(p1);
			}



		}



	}
	volatile size_t wush2 = wush;
}

*/


/*
// 37 ms
void merge_core(Array *a0, Array *a1, Array *res) {
	size_t wush = 0;
	tos = 0;
	int64_t v0, v1, vv0, vv1;
	size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
	size_t s0 = a0->Size();
	size_t s1 = a1->Size();

	v0 = a0->Get<8>(p0);
	v1 = a1->Get<8>(p1);
	
	for(size_t i = 0; p0 + 1 < s0 && p1 + 1 < s1; i++) {
		if(v0 < v1) {
			vv0 = a1->Get<8>(++p0);
			if(v0 < vv0) {
				list[tos++] = v0;
				list[tos++] = vv0;
				v0 = a0->Get<8>(++p0);
			}
			else {
				list[tos++] = vv0;
				list[tos++] = v0;
				v0 = a0->Get<8>(++p0);
				v1 = a1->Get<8>(++p1);
			}
		}
		else {
			vv1 = a1->Get<8>(++p1);
			if(v1 < vv1) {
				list[tos++] = v1;
				list[tos++] = vv1;
				v0 = a0->Get<8>(++p0);
				v1 = a1->Get<8>(++p1);
			}
			else {
				list[tos++] = vv1;
				list[tos++] = v1;
				v1 = a1->Get<8>(++p1);
			}
		}

	}
	

	volatile size_t wush2 = wush;
}
*/









/*
// 50 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	int64_t v0, v1;
	size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
	size_t s1 = a1->Size();
	size_t s2 = a2->Size();

	for(size_t i = 0; p0  < s1 && p1  < s2; i++) {

		v0 = a1->Get<32>(p0 >> 2);
		v1 = a2->Get<32>(p1 >> 2);
		
		if(v0 < v1) {
			// v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
			wush += v0 >> (64 - 8);
			p0++;
			v0 <<= 8;
		}
		else {
			wush += v1 >> (64 - 8);
			p1++;
			v1 <<= 8;
		}


		if(v0 < v1) {
			// v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
			wush += v0 >> (64 - 8);
			p0++;
			v0 <<= 8;
		}
		else {
			wush += v1 >> (64 - 8);
			p1++;
			v1 <<= 8;
		}


		if(v0 < v1) {
			// v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
			wush += v0 >> (64 - 8);
			p0++;
			v0 <<= 8;
		}
		else {
			wush += v1 >> (64 - 8);
			p1++;
			v1 <<= 8;
		}


		if(v0 < v1) {
			// v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
			wush += v0 >> (64 - 8);
			p0++;
			v0 <<= 8;
		}
		else {
			wush += v1 >> (64 - 8);
			p1++;
			v1 <<= 8;
		}
	}
	volatile size_t wush2 = wush;
}
*/


/*
// Branch free merge :)
// 130 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	Array *a[2] = {a1, a2};
	int64_t v = 0;
	int64_t p = 0;
	size_t s0 = a[0]->Size();
	size_t s1 = a[1]->Size();
	for(size_t i = 0; (p & 0xffff) < s0; i++) {

		v = a1->Get<8>(p & 0xffff);
		v = a2->Get<8>(p >> 16) << 16;
		int m = (v & 0xffff) < (v >> 16) ? 0 : 1; // cmovg
		wush += (v >> (m * 16) & 0xffff);
		p += 1 + (m * 0x10000);


	}

		volatile size_t wush2 = wush;
}
*/


/*
// Branch free merge :)
// 130 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	Array *a[2] = {a1, a2};
	int64_t v = 0;
	int64_t p = 0;
	size_t s0 = a[0]->Size();
	size_t s1 = a[1]->Size();
	for(size_t i = 0; (p & 0xffff) < s0; i++) {

		v = a1->Get<8>(p & 0xffff);
		v = a2->Get<8>(p >> 16);
		int m = (v & 0xffff) < (v >> 16) ? 0 : 1; // cmovg
		wush += (v >> (m * 16) & 0xffff);
		p += 1 + (m * 0x10000);
	}

		volatile size_t wush2 = wush;
}
*/


/*
// Branch free merge :)
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	Array *a[2] = {a1, a2};
	int64_t v0, v1;
	int32_t p[2] = {0, 0};
	size_t s0 = a[0]->Size();
	size_t s1 = a[1]->Size();
	for(size_t i = 0; p[0]  < s0; i++) {

		v0 = a[0]->Get<8>(p[0]);
		v1 = a[1]->Get<8>(p[1]);
		int m = v0 < v1 ? 0 : 1; // cmovg
		wush += a[m]->Get<8>(p[m]);
//		p[m]++;
		*((int64_t *)p) += 1 + 0x100000000 * m; //bug

	}
}
*/


/*
// Each time two input lists are merged into a new list, the old ones can be destroyed to save
// memory. Set delete_old to true to do that.
Array *merge(Array *ArrayList, bool delete_old) {
	if(ArrayList->Size() == 1) {
		size_t ref = ArrayList->Get(0);
		Array *a = new Array(ref);
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
	l = merge(&Left, true);
	r = merge(&Right, true);

	UnitTest::Timer timer;
	unsigned int g = -1;
	unsigned int ms;
	for(int j = 0; j < 20; j++) {
		timer.Start();

		for(int i = 0; i < 20000; i++)
			merge_core(l, r, res);
		ms = timer.GetTimeInMs();
		if (ms < g)
			g = ms;
		printf("%d ms\n", ms);
	}
	printf("\n%d ms\n", g);
	getchar();


	if(delete_old) {
//		l->Destroy();
//		r->Destroy();
//		delete(l);
//		delete(r);
	}

	return res;
}*/



/*
// Branch free merge :)
// 320 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	Array *a[2] = {a1, a2};
	int64_t v[2]; // todo, test non-subscripted v
	size_t p[2] = {0, 0};

	for(size_t i = 0; p[0] < a[0]->Size() && p[1] < a[1]->Size(); i++) {
		v[0] = a[0]->Get(p[0]);
		v[1] = a[1]->Get(p[1]);
		size_t m = v[0] < v[1] ? 0 : 1; // cmovg
		//res->Add(v[m]);
		wush += v[m];
		p[m]++;
	}

	for(size_t t = 0; t < 2; t++)
		for(; p[t] < a[t]->Size(); p[t]++)
			res->Add(a[t]->Get(p[t]));

	volatile size_t wush2 = wush;
}

*/

/*
// Branch free merge :)
// 396 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	Array *a[2] = {a1, a2};
	int64_t v0, v1;
	size_t p[2] = {0, 0};
	size_t s0 = a[0]->Size();
	size_t s1 = a[1]->Size();
	for(size_t i = 0; p[0]  < s0; i++) {

		v0 = a[0]->Get<8>(p[0]);
		v1 = a[1]->Get<8>(p[1]);
		int m = v0 < v1 ? 0 : 1; // cmovg
		wush += a[m]->Get<8>(p[m]);
		p[m]++;


	}



}
*/


/*
// 74 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	int64_t v0, v1;
	size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
	size_t s1 = a1->Size();
	size_t s2 = a2->Size();

	for(size_t i = 0; p0  < s1 && p1 < s2; i++) {

		v0 = a1->Get<8>(p0);
		v1 = a2->Get<8>(p1);
		if(v0 < v1) {
			wush += v0;
			p0++;
		}
		else {
			wush += v1;
			p1++;
		}
	}
	volatile size_t wush2 = wush;
}
*/

/*
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	int64_t v0, v1;
	size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
	size_t s1 = a1->Size();
	size_t s2 = a2->Size();

	v0 = a1->Get<64>(p0);
	v1 = a2->Get<64>(p1);

	for(size_t i = 0; p0 < s1 && p1 < s2; i++) {
		if(v0 < v1) {
			wush += v0;
			p0++;
			v0 = a1->Get<8>(p0);
		}
		else {
			wush += v1;
			p1++;
			v1 = a2->Get<8>(p1);
		}

	}
}

*/

/*
// 45 ms
void merge_core(Array *a1, Array *a2, Array *res) {
	size_t wush = 0;
	int64_t v0, v1;
	size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
	size_t s1 = a1->Size();
	size_t s2 = a2->Size();

	v0 = a1->Get<8>(p0);
	v1 = a2->Get<8>(p1);

	for(size_t i = 0; p0  < s1 && p1 < s2; i++) {


		if(v0 < v1) {
			wush += v0;
			p0++;
			v0 = a1->Get<8>(p0);
		}
		else {
			wush += v1;
			p1++;
			v1 = a2->Get<8>(p1);
		}


		if(v0 < v1) {
			wush += v0;
			p0++;
			v0 = a1->Get<8>(p0);
		}
		else {
			wush += v1;
			p1++;
			v1 = a2->Get<8>(p1);
		}


		if(v0 < v1) {
			wush += v0;
			p0++;
			v0 = a1->Get<8>(p0);
		}
		else {
			wush += v1;
			p1++;
			v1 = a2->Get<8>(p1);
		}


		if(v0 < v1) {
			wush += v0;
			p0++;
			v0 = a1->Get<8>(p0);
		}
		else {
			wush += v1;
			p1++;
			v1 = a2->Get<8>(p1);
		}

	}
	volatile size_t wush2 = wush;
}
*/

/*
// 61 ms,  8*unr = 46, 8*=61, 4*unrolled = 62
void merge_core(Array *a0, Array *a1, Array *res) {
	tos = 0;
	size_t wush = 0;
	uint64_t v0, v1;
	size_t p0 = 15, p1 = 0;
	size_t s0 = a0->Size();
	size_t s1 = a1->Size();

	for(size_t i = 0; p0 + 8 < s0 && p1 + 8 < s1; i++) {

		v0 = a0->Get<8>(p0 + 0) << 0*8 | 
			 a0->Get<8>(p0 + 1) << 1*8 | 
			 a0->Get<8>(p0 + 2) << 2*8 | 
			 a0->Get<8>(p0 + 3) << 3*8 |
			 a0->Get<8>(p0 + 4) << 4*8 | 
			 a0->Get<8>(p0 + 5) << 5*8 | 
			 a0->Get<8>(p0 + 6) << 6*8 | 
			 a0->Get<8>(p0 + 7) << 7*8; 

		v1 = a1->Get<8>(p1 + 0) << 0*8 | 
			 a1->Get<8>(p1 + 1) << 1*8 | 
			 a1->Get<8>(p1 + 2) << 2*8 | 
			 a1->Get<8>(p1 + 3) << 3*8 |
			 a1->Get<8>(p1 + 4) << 4*8 | 
			 a1->Get<8>(p1 + 5) << 5*8 | 
			 a1->Get<8>(p1 + 6) << 6*8 |
			 a1->Get<8>(p1 + 7) << 7*8;

		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;


		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;


		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;


		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;


		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;


		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;


		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;


		if(v0 < v1) {
			list[tos] = v0 >> 7*8;
			p0++;
			v1 <<= 8;
		}
		else {
			list[tos] = v1 >> 7*8;
			v0 <<= 8;
			p1++;
		}
		tos++;




	}
	volatile size_t wush2 = wush;
}
*/

