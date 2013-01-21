#include <cstdlib>
#include <cstring>
#include <cstdio> // debug output
#include <climits> // size_t
#include <iostream>
#include <iomanip>

#ifdef _MSC_VER
#include <win32/stdint.h>
#else
#include <stdint.h> // unint8_t etc
#endif

#include <tightdb/column.hpp>
#include <tightdb/index.hpp>
#include <tightdb/query_engine.hpp>

using namespace std;

namespace {

using namespace tightdb;

Column GetColumnFromRef(Array &parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.HasRefs());
    TIGHTDB_ASSERT(ndx < parent.Size());
    return Column((size_t)parent.Get(ndx), &parent, ndx, parent.GetAllocator());
}

/*
const Column GetColumnFromRef(const Array& parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.HasRefs());
    TIGHTDB_ASSERT(ndx < parent.Size());
    return Column((size_t)parent.Get(ndx), &parent, ndx);
}
*/

// Pre-declare local functions
bool callme_arrays(Array* a, size_t start, size_t end, size_t caller_offset, void* state);
void merge_core_references(Array* vals, Array* idx0, Array* idx1, Array* idxres);
void merge_core(const Array& a0, const Array& a1, Array& res);
Array* merge(const Array& ArrayList);
void merge_references(Array* valuelist, Array* indexlists, Array** indexresult);
    
// Input:
//     vals:   An array of values
//     idx0:   Array of indexes pointing into vals, sorted with respect to vals
//     idx1:   Array of indexes pointing into vals, sorted with respect to vals
//     idx0 and idx1 are allowed not to contain index pointers to *all* elements in vals
//     (idx0->Size() + idx1->Size() < vals.Size() is OK).
// Output:
//     idxres: Merged array of indexes sorted with respect to vals
void merge_core_references(Array* vals, Array* idx0, Array* idx1, Array* idxres)
{
    int64_t v0, v1;
    size_t i0, i1;
    size_t p0 = 0, p1 = 0;
    size_t s0 = idx0->Size();
    size_t s1 = idx1->Size();

    i0 = idx0->GetAsRef(p0++);
    i1 = idx1->GetAsRef(p1++);
    v0 = vals->Get(i0);
    v1 = vals->Get(i1);

    for (;;) {
        if (v0 < v1) {
            idxres->add(i0);
            // Only check p0 if it has been modified :)
            if (p0 == s0)
                break;
            i0 = idx0->GetAsRef(p0++);
            v0 = vals->Get(i0);
        }
        else {
            idxres->add(i1);
            if (p1 == s1)
                break;
            i1 = idx1->GetAsRef(p1++);
            v1 = vals->Get(i1);
        }
    }

    if (p0 == s0)
        p0--;
    else
        p1--;

    while (p0 < s0) {
        i0 = idx0->GetAsRef(p0++);
        idxres->add(i0);
    }
    while (p1 < s1) {
        i1 = idx1->GetAsRef(p1++);
        idxres->add(i1);
    }

    TIGHTDB_ASSERT(idxres->Size() == idx0->Size() + idx1->Size());
}

// Merge two sorted arrays into a single sorted array
void merge_core(const Array& a0, const Array& a1, Array& res)
{
    TIGHTDB_ASSERT(res.is_empty());

    size_t p0 = 0;
    size_t p1 = 0;
    const size_t s0 = a0.Size();
    const size_t s1 = a1.Size();

    int64_t v0 = a0.Get(p0++);
    int64_t v1 = a1.Get(p1++);

    for (;;) {
        if (v0 < v1) {
            res.add(v0);
            if (p0 == s0)
                break;
            v0 = a0.Get(p0++);
        }
        else {
            res.add(v1);
            if (p1 == s1)
                break;
            v1 = a1.Get(p1++);
        }
    }

    if (p0 == s0)
        --p0;
    else
        --p1;

    while (p0 < s0) {
        v0 = a0.Get(p0++);
        res.add(v0);
    }
    while (p1 < s1) {
        v1 = a1.Get(p1++);
        res.add(v1);
    }

    TIGHTDB_ASSERT(res.Size() == a0.Size() + a1.Size());
}

// Input:
//     ArrayList: An array of references to non-instantiated Arrays of values. The values in each array must be in sorted order
// Return value:
//     Merge-sorted array of all values
Array* merge(const Array& arrayList)
{
    const size_t size = arrayList.Size();

    if (size == 1) 
        return NULL; // already sorted

    Array leftHalf, rightHalf;
    const size_t leftSize = size / 2;
    for (size_t t = 0; t < leftSize; ++t)
        leftHalf.add(arrayList.Get(t));
    for (size_t t = leftSize; t < size; ++t)
        rightHalf.add(arrayList.Get(t));

    // We merge left-half-first instead of bottom-up so that we access the same data in each call
    // so that it's in cache, at least for the first few iterations until lists get too long
    Array* left = merge(leftHalf);
    Array* right = merge(rightHalf);
    Array* res = new Array();

    if (left && right)
        merge_core(*left, *right, *res);
    else if (left) {
        const size_t ref = rightHalf.GetAsRef(0);
        Array right0(ref, NULL);
        merge_core(*left, right0, *res);
    }
    else if (right) {
        const size_t ref = leftHalf.GetAsRef(0);
        Array left0(ref, NULL);
        merge_core(left0, *right, *res);
    }

    // Clean-up
    leftHalf.Destroy();
    rightHalf.Destroy();
    if (left) 
        left->Destroy();
    if (right) 
        right->Destroy();
    delete left;
    delete right;

    return res; // receiver now own the array, and has to delete it when done
}

// Input:
//     valuelist:   One array of values
//     indexlists:  Array of pointers to non-instantiated Arrays of index numbers into valuelist
// Output:
//     indexresult: Array of indexes into valuelist, sorted with respect to values in valuelist
// TODO: Set owner of created arrays and Destroy/delete them if created by merge_references()
void merge_references(Array* valuelist, Array* indexlists, Array** indexresult)
{
    if (indexlists->Size() == 1) {
//      size_t ref = valuelist->Get(0);
        *indexresult = (Array *)indexlists->Get(0);
        return;
    }

    Array leftV, rightV;
    Array leftI, rightI;
    size_t leftSize = indexlists->Size() / 2;
    for (size_t t = 0; t < leftSize; t++) {
        leftV.add(indexlists->Get(t));
        leftI.add(indexlists->Get(t));
    }
    for (size_t t = leftSize; t < indexlists->Size(); t++) {
        rightV.add(indexlists->Get(t));
        rightI.add(indexlists->Get(t));
    }

    Array *li;
    Array *ri;

    Array *resI = new Array();

    // We merge left-half-first instead of bottom-up so that we access the same data in each call
    // so that it's in cache, at least for the first few iterations until lists get too long
    merge_references(valuelist, &leftI, &ri);
    merge_references(valuelist, &rightI, &li);
    merge_core_references(valuelist, li, ri, resI);

    *indexresult = resI;
}

bool callme_arrays(Array* a, size_t start, size_t end, size_t caller_offset, void* state)
{
    (void)end;
    (void)start;
    (void)caller_offset;
    Array* p = (Array*)state;
    const size_t ref = a->GetRef();
    p->add((int64_t)ref); // todo, check cast
    return true;
}

}


namespace tightdb {

size_t ColumnBase::get_size_from_ref(size_t ref, Allocator& alloc)
{
    Array a(ref, 0, 0, alloc);
    if (!a.IsNode())
        return a.Size();
    Array offsets(a.Get(0), 0, 0, alloc);
    return offsets.is_empty() ? 0 : size_t(offsets.back());
}

Column::Column(Allocator& alloc): m_index(0)
{
    m_array = new Array(COLUMN_NORMAL, 0, 0, alloc);
    Create();
}

Column::Column(ColumnDef type, Allocator& alloc): m_index(0)
{
    m_array = new Array(type, 0, 0, alloc);
    Create();
}

Column::Column(ColumnDef type, ArrayParent* parent, size_t pndx, Allocator& alloc): m_index(0)
{
    m_array = new Array(type, parent, pndx, alloc);
    Create();
}

Column::Column(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc): m_index(0)
{
    m_array = new Array(ref, parent, pndx, alloc);
}

Column::Column(const Column& column): ColumnBase(), m_index(0)
{
    m_array = column.m_array; // we now own array
    column.m_array = 0;       // so invalidate source
}

void Column::Create()
{
    // Add subcolumns for nodes
    if (IsNode()) {
        const Array offsets(COLUMN_NORMAL, 0, 0, m_array->GetAllocator());
        const Array refs(COLUMN_HASREFS, 0, 0, m_array->GetAllocator());
        m_array->add(offsets.GetRef());
        m_array->add(refs.GetRef());
    }
}

void Column::UpdateRef(size_t ref)
{
    m_array->UpdateRef(ref);
}

bool Column::operator==(const Column& column) const
{
    return *m_array == *(column.m_array);
}

Column::~Column()
{
    delete m_array;
    delete m_index; // does not destroy index!
}

void Column::Destroy()
{
    ClearIndex();
    if (m_array)
        m_array->Destroy();
}


bool Column::is_empty() const
{
    if (!IsNode())
        return m_array->is_empty();
    const Array offsets = NodeGetOffsets();
    return offsets.is_empty();
}

size_t Column::Size() const
{
    if (!IsNode())
        return m_array->Size();
    const Array offsets = NodeGetOffsets();
    return offsets.is_empty() ? 0 : size_t(offsets.back());
}

void Column::UpdateParentNdx(int diff)
{
    m_array->UpdateParentNdx(diff);
    if (m_index)
        m_index->UpdateParentNdx(diff);
}

// Used by column b-tree code to ensure all leaf having same type
void Column::SetHasRefs()
{
    m_array->SetType(COLUMN_HASREFS);
}

/*
Column Column::GetSubColumn(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_len);
    TIGHTDB_ASSERT(m_hasRefs);

    return Column((void*)ListGet(ndx), this, ndx);
}

const Column Column::GetSubColumn(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < m_len);
    TIGHTDB_ASSERT(m_hasRefs);

    return Column((void*)ListGet(ndx), this, ndx);
}
*/

void Column::Clear()
{
    m_array->Clear();
    if (m_array->IsNode())
        m_array->SetType(COLUMN_NORMAL);
}

int64_t Column::Get(size_t ndx) const
{
    return m_array->ColumnGet(ndx);
    //return TreeGet<int64_t, Column>(ndx);
}

size_t Column::GetAsRef(size_t ndx) const
{
    return to_ref(TreeGet<int64_t, Column>(ndx));
}

bool Column::Set(size_t ndx, int64_t value)
{
    const int64_t oldVal = m_index ? Get(ndx) : 0; // cache oldval for index

    const bool res = TreeSet<int64_t, Column>(ndx, value);
    if (!res)
        return false;

    // Update index
    if (m_index)
        m_index->Set(ndx, oldVal, value);

    return true;
}

bool Column::add(int64_t value)
{
    return Insert(Size(), value);
}

bool Column::Insert(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx <= Size());

    const bool res = TreeInsert<int64_t, Column>(ndx, value);
    if (!res)
        return false;

    // Update index
    if (m_index) {
        const bool isLast = (ndx+1 == Size());
        m_index->Insert(ndx, value, isLast);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif

    return true;
}

void Column::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());
    TIGHTDB_ASSERT(!m_index);

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<int64_t, Column>(i, 0);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

template <ACTION action, class cond, class T>
T Column::aggregate(T target, size_t start, size_t end, size_t *matchcount) const
{ 
    if (end == size_t(-1)) 
        end = Size();

    // We must allocate 'node' on stack with malloca() because malloc is slow (makes aggregate on 1000 elements around 10 times
    // slower because of initial overhead).
        //    NODE<int64_t, Column, cond>* node = (NODE<int64_t, Column, cond>*)alloca(sizeof(NODE<int64_t, Column, cond>));     
        //    new (node) NODE<int64_t, Column, cond>(target, 0);
    NODE<T, Column, cond> node(target, 0);

    node.QuickInit((Column*)this, target); 
    state_state<T> st;
    st.init(action, NULL, size_t(-1));

    node.template aggregate_local<action, T>(&st, start, end, size_t(-1), NULL, matchcount);

    return st.state;
}

// int64_t specific:

size_t Column::count(int64_t target) const
{
    return size_t(aggregate<TDB_COUNT, EQUAL, int64_t>(target, 0, Size()));
}

int64_t Column::sum(size_t start, size_t end) const
{
    return aggregate<TDB_SUM, NONE, int64_t>(0, start, end);
}

double Column::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();
    size_t size = end - start;
    int64_t sum = aggregate<TDB_SUM, NONE, int64_t>(0, start, end);
    double avg = double( sum ) / double( size == 0 ? 1 : size );
    return avg;
}

int64_t Column::minimum(size_t start, size_t end) const
{
    return aggregate<TDB_MIN, NONE, int64_t>(0, start, end);
}

int64_t Column::maximum(size_t start, size_t end) const
{
    return aggregate<TDB_MAX, NONE, int64_t>(0, start, end);
}



void Column::sort(size_t start, size_t end)
{
    Array arr;
    TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&arr);
    for (size_t t = 0; t < arr.Size(); t++) {
        const size_t ref = to_ref(arr.Get(t));
        Array a(ref);
        a.sort();
    }

    Array* sorted = merge(arr);
    if (sorted) {
        // Todo, this is a bit slow. Add bulk insert or the like to Column
        const size_t count = sorted->Size();
        for (size_t t = 0; t < count; ++t) {
            Set(t, sorted->Get(t));
        }

        sorted->Destroy();
        delete sorted;
    }

    // Clean-up
    arr.Destroy();
}


// TODO: Set owner of created arrays and Destroy/delete them if created by merge_references()
void Column::ReferenceSort(size_t start, size_t end, Column& ref)
{
    Array values; // pointers to non-instantiated arrays of values
    Array indexes; // pointers to instantiated arrays of index pointers
    Array all_values;
    TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&values);

    size_t offset = 0;
    for (size_t t = 0; t < values.Size(); t++) {
        Array *i = new Array();
        size_t ref = values.GetAsRef(t);
        Array v(ref);
        for (size_t j = 0; j < v.Size(); j++)
            all_values.add(v.Get(j));
        v.ReferenceSort(*i);
        for (size_t n = 0; n < v.Size(); n++)
            i->Set(n, i->Get(n) + offset);
        offset += v.Size();
        indexes.add((int64_t)i);
    }

    Array *ResI;

    merge_references(&all_values, &indexes, &ResI);

    for (size_t t = 0; t < ResI->Size(); t++)
        ref.add(ResI->Get(t));
}

size_t ColumnBase::GetRefSize(size_t ref) const
{
    // parse the length part of 8byte header
    const uint8_t* const header = (uint8_t*)m_array->GetAllocator().Translate(ref);
    return (header[1] << 16) + (header[2] << 8) + header[3];
}

Array ColumnBase::NodeGetOffsets()
{
    TIGHTDB_ASSERT(IsNode());
    return m_array->GetSubArray(0);
}

const Array ColumnBase::NodeGetOffsets() const
{
    TIGHTDB_ASSERT(IsNode());
    return m_array->GetSubArray(0);
}

Array ColumnBase::NodeGetRefs()
{
    TIGHTDB_ASSERT(IsNode());
    return m_array->GetSubArray(1);
}

const Array ColumnBase::NodeGetRefs() const
{
    TIGHTDB_ASSERT(IsNode());
    return m_array->GetSubArray(1);
}

bool ColumnBase::NodeUpdateOffsets(size_t ndx)
{
    TIGHTDB_ASSERT(IsNode());

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();
    TIGHTDB_ASSERT(ndx < offsets.Size());

    const int64_t newSize = GetRefSize((size_t)refs.Get(ndx));
    const int64_t oldSize = offsets.Get(ndx) - (ndx ? offsets.Get(ndx-1) : 0);
    const int64_t diff = newSize - oldSize;

    return offsets.Increment(diff, ndx);
}

bool ColumnBase::NodeAddKey(size_t ref)
{
    TIGHTDB_ASSERT(ref);
    TIGHTDB_ASSERT(IsNode());

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();
    TIGHTDB_ASSERT(offsets.Size() < MAX_LIST_SIZE);

    const Array new_top(ref, NULL, 0,m_array->GetAllocator());
    const Array new_offsets(new_top.GetAsRef(0), NULL, 0,m_array->GetAllocator());
    TIGHTDB_ASSERT(!new_offsets.is_empty());

    const int64_t key = new_offsets.back();
    if (!offsets.add(key)) return false;
    return refs.add(ref);
}

void Column::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());

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

bool Column::Increment64(int64_t value, size_t start, size_t end)
{
    if (!IsNode()) return m_array->Increment(value, start, end);
    else {
        //TODO: partial incr
        Array refs = NodeGetRefs();
        const size_t count = refs.Size();
        for (size_t i = 0; i < count; ++i) {
            Column col = ::GetColumnFromRef(refs, i);
            if (!col.Increment64(value)) return false;
        }
        return true;
    }
}

void Column::IncrementIf(int64_t limit, int64_t value)
{
    if (!IsNode()) m_array->IncrementIf(limit, value);
    else {
        Array refs = NodeGetRefs();
        const size_t count = refs.Size();
        for (size_t i = 0; i < count; ++i) {
            Column col = ::GetColumnFromRef(refs, i);
            col.IncrementIf(limit, value);
        }
    }
}

size_t Column::find_first(int64_t value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(start <= Size());
    TIGHTDB_ASSERT(end == (size_t)-1 || end <= Size());

    if (start == 0 && end == (size_t)-1) {
        Array cache(m_array->GetAllocator());
        const size_t ref = m_array->GetRef();

        return m_array->ColumnFind(value, ref, cache);
    }
    else {
        return TreeFind<int64_t, Column, EQUAL>(value, start, end);
    }
}

void Column::find_all(Array& result, int64_t value, size_t caller_offset, size_t start, size_t end) const
{
    (void)caller_offset;
    TIGHTDB_ASSERT(start <= Size());
    TIGHTDB_ASSERT(end == (size_t)-1 || end <= Size());
    if (is_empty()) return;
    TreeFindAll<int64_t, Column>(result, value, 0, start, end);
}

void Column::LeafFindAll(Array &result, int64_t value, size_t add_offset, size_t start, size_t end) const
{
	m_array->find_all(result, value, add_offset, start, end);
}

void Column::find_all_hamming(Array& result, uint64_t value, size_t maxdist, size_t offset) const
{
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
            col.find_all_hamming(result, value, maxdist, offset);
            offset += (size_t)offsets.Get(i);
        }
    }
}

size_t Column::find_pos(int64_t target) const
{
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
    if (high == len) return not_found;
    else return high;
}

size_t Column::find_pos2(int64_t target) const
{
    // NOTE: Binary search only works if the column is sorted

    if (!IsNode()) {
        return m_array->FindPos2(target);
    }

    const int len = (int)Size();
    int low = -1;
    int high = len;

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of closest value BIGGER OR EQUAL to the target
    while (high - low > 1) {
        const size_t probe = ((unsigned int)low + (unsigned int)high) >> 1;
        const int64_t v = Get(probe);

        if (v < target) low  = (int)probe;
        else            high = (int)probe;
    }
    if (high == len) return not_found;
    else return high;
}


size_t Column::FindWithIndex(int64_t target) const
{
    TIGHTDB_ASSERT(m_index);
    TIGHTDB_ASSERT(m_index->Size() == Size());

    return m_index->find_first(target);
}

Index& Column::GetIndex()
{
    TIGHTDB_ASSERT(m_index);
    return *m_index;
}

void Column::ClearIndex()
{
    if (m_index) {
        m_index->Destroy();
        delete m_index;
        m_index = NULL;
    }
}

void Column::BuildIndex(Index& index)
{
    index.BuildIndex(*this);
    m_index = &index; // Keep ref to index
}

void Column::sort()
{
    sort(0, Size());
}

bool Column::Compare(const Column& c) const
{
    const size_t n = Size();
    if (c.Size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        if (Get(i) != c.Get(i)) return false;
    }
    return true;
}


#ifdef TIGHTDB_DEBUG

void Column::Print() const
{
    if (IsNode()) {
        cout << "Node: " << hex << m_array->GetRef() << dec << "\n";

        const Array offsets = NodeGetOffsets();
        const Array refs = NodeGetRefs();

        for (size_t i = 0; i < refs.Size(); ++i) {
            cout << " " << i << ": " << offsets.Get(i) << " " << hex << refs.Get(i) << dec <<"\n";
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

void Column::Verify() const
{
    if (IsNode()) {
        TIGHTDB_ASSERT(m_array->Size() == 2);
        //TIGHTDB_ASSERT(m_hasRefs);

        const Array offsets = NodeGetOffsets();
        const Array refs = NodeGetRefs();
        offsets.Verify();
        refs.Verify();
        TIGHTDB_ASSERT(refs.HasRefs());
        TIGHTDB_ASSERT(offsets.Size() == refs.Size());

        size_t off = 0;
        for (size_t i = 0; i < refs.Size(); ++i) {
            const size_t ref = size_t(refs.Get(i));
            TIGHTDB_ASSERT(ref);

            const Column col(ref, NULL, 0, m_array->GetAllocator());
            col.Verify();

            off += col.Size();
            const size_t node_off = (size_t)offsets.Get(i);
            if (node_off != off) {
                TIGHTDB_ASSERT(false);
            }
        }
    }
    else m_array->Verify();
}

void ColumnBase::ToDot(std::ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    out << "subgraph cluster_column" << ref << " {" << std::endl;
    out << " label = \"Column";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    ArrayToDot(out, *m_array);

    out << "}" << std::endl;
}

void ColumnBase::ArrayToDot(std::ostream& out, const Array& array) const
{
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

void ColumnBase::LeafToDot(std::ostream& out, const Array& array) const
{
    array.ToDot(out);
}

MemStats Column::Stats() const
{
    MemStats stats;
    m_array->Stats(stats);

    return stats;
}

#endif // TIGHTDB_DEBUG

}
