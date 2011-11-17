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


// Pre-declare local functions
void SetRefSize(void* ref, size_t len);

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
	m_array = column.m_array;
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

	return TreeFind<int64_t, Column>(value, start, end);
}

void Column::FindAll(Column& result, int64_t value, size_t offset,
					 size_t start, size_t end) const {
	assert(start <= Size());
	assert(end == (size_t)-1 || end <= Size());
	if (IsEmpty()) return;

	if (!IsNode()) return m_array->FindAll(result, value, offset, start, end);
	else {
		// Get subnode table
		const Array offsets = NodeGetOffsets();
		const Array refs = NodeGetRefs();
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
	const int64_t x = Get(ndx);

	// partition
	do {
		while (Get(i) < x) i++;
		while (Get(j) > x) j--;
		if (i <= j) {
			const int64_t h = Get(i);
			Set(i, Get(j));
			Set(j, h);
			i++; j--;
		}
	} while (i <= j);

	//  recursion
	if ((int)lo < j) DoSort(lo, j);
	if (i < (int)hi) DoSort(i, hi);
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

#endif //_DEBUG
