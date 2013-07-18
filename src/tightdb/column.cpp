#include <stdint.h> // unint8_t etc
#include <cstdlib>
#include <cstring>
#include <cstdio> // debug output
#include <climits> // size_t
#include <iostream>
#include <iomanip>

#include <tightdb/column.hpp>
#include <tightdb/index.hpp>
#include <tightdb/query_engine.hpp>

using namespace std;

namespace {

using namespace tightdb;

Column GetColumnFromRef(Array &parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.has_refs());
    TIGHTDB_ASSERT(ndx < parent.size());
    return Column(parent.get_as_ref(ndx), &parent, ndx, parent.get_alloc());
}

/*
const Column GetColumnFromRef(const Array& parent, size_t ndx)
{
    TIGHTDB_ASSERT(parent.has_refs());
    TIGHTDB_ASSERT(ndx < parent.size());
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
//     (idx0->size() + idx1->size() < vals.size() is OK).
// Output:
//     idxres: Merged array of indexes sorted with respect to vals
void merge_core_references(Array* vals, Array* idx0, Array* idx1, Array* idxres)
{
    int64_t v0, v1;
    size_t i0, i1;
    size_t p0 = 0, p1 = 0;
    size_t s0 = idx0->size();
    size_t s1 = idx1->size();

    i0 = idx0->get_as_ref(p0++);
    i1 = idx1->get_as_ref(p1++);
    v0 = vals->get(i0);
    v1 = vals->get(i1);

    for (;;) {
        if (v0 < v1) {
            idxres->add(i0);
            // Only check p0 if it has been modified :)
            if (p0 == s0)
                break;
            i0 = idx0->get_as_ref(p0++);
            v0 = vals->get(i0);
        }
        else {
            idxres->add(i1);
            if (p1 == s1)
                break;
            i1 = idx1->get_as_ref(p1++);
            v1 = vals->get(i1);
        }
    }

    if (p0 == s0)
        p0--;
    else
        p1--;

    while (p0 < s0) {
        i0 = idx0->get_as_ref(p0++);
        idxres->add(i0);
    }
    while (p1 < s1) {
        i1 = idx1->get_as_ref(p1++);
        idxres->add(i1);
    }

    TIGHTDB_ASSERT(idxres->size() == idx0->size() + idx1->size());
}

// Merge two sorted arrays into a single sorted array
void merge_core(const Array& a0, const Array& a1, Array& res)
{
    TIGHTDB_ASSERT(res.is_empty());

    size_t p0 = 0;
    size_t p1 = 0;
    const size_t s0 = a0.size();
    const size_t s1 = a1.size();

    int64_t v0 = a0.get(p0++);
    int64_t v1 = a1.get(p1++);

    for (;;) {
        if (v0 < v1) {
            res.add(v0);
            if (p0 == s0)
                break;
            v0 = a0.get(p0++);
        }
        else {
            res.add(v1);
            if (p1 == s1)
                break;
            v1 = a1.get(p1++);
        }
    }

    if (p0 == s0)
        --p0;
    else
        --p1;

    while (p0 < s0) {
        v0 = a0.get(p0++);
        res.add(v0);
    }
    while (p1 < s1) {
        v1 = a1.get(p1++);
        res.add(v1);
    }

    TIGHTDB_ASSERT(res.size() == a0.size() + a1.size());
}

// Input:
//     ArrayList: An array of references to non-instantiated Arrays of values. The values in each array must be in sorted order
// Return value:
//     Merge-sorted array of all values
Array* merge(const Array& arrayList)
{
    const size_t size = arrayList.size();

    if (size == 1)
        return NULL; // already sorted

    Array leftHalf, rightHalf;
    const size_t leftSize = size / 2;
    for (size_t t = 0; t < leftSize; ++t)
        leftHalf.add(arrayList.get(t));
    for (size_t t = leftSize; t < size; ++t)
        rightHalf.add(arrayList.get(t));

    // We merge left-half-first instead of bottom-up so that we access the same data in each call
    // so that it's in cache, at least for the first few iterations until lists get too long
    Array* left = merge(leftHalf);
    Array* right = merge(rightHalf);
    Array* res = new Array();

    if (left && right)
        merge_core(*left, *right, *res);
    else if (left) {
        const size_t ref = rightHalf.get_as_ref(0);
        Array right0(ref, NULL);
        merge_core(*left, right0, *res);
    }
    else if (right) {
        const size_t ref = leftHalf.get_as_ref(0);
        Array left0(ref, NULL);
        merge_core(left0, *right, *res);
    }

    // Clean-up
    leftHalf.destroy();
    rightHalf.destroy();
    if (left)
        left->destroy();
    if (right)
        right->destroy();
    delete left;
    delete right;

    return res; // receiver now own the array, and has to delete it when done
}

// Input:
//     valuelist:   One array of values
//     indexlists:  Array of pointers to non-instantiated Arrays of index numbers into valuelist
// Output:
//     indexresult: Array of indexes into valuelist, sorted with respect to values in valuelist
// TODO: Set owner of created arrays and destroy/delete them if created by merge_references()
void merge_references(Array* valuelist, Array* indexlists, Array** indexresult)
{
    if (indexlists->size() == 1) {
//      size_t ref = valuelist->get(0);
        *indexresult = reinterpret_cast<Array*>(indexlists->get(0));
        return;
    }

    Array leftV, rightV;
    Array leftI, rightI;
    size_t leftSize = indexlists->size() / 2;
    for (size_t t = 0; t < leftSize; t++) {
        leftV.add(indexlists->get(t));
        leftI.add(indexlists->get(t));
    }
    for (size_t t = leftSize; t < indexlists->size(); t++) {
        rightV.add(indexlists->get(t));
        rightI.add(indexlists->get(t));
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
    static_cast<void>(end);
    static_cast<void>(start);
    static_cast<void>(caller_offset);
    Array* p = static_cast<Array*>(state);
    const size_t ref = a->get_ref();
    p->add(int64_t(ref)); // todo, check cast
    return true;
}

}


namespace tightdb {

size_t ColumnBase::get_size_from_ref(ref_type ref, Allocator& alloc) TIGHTDB_NOEXCEPT
{
    Array a(ref, 0, 0, alloc);
    if (a.is_leaf())
        return a.size();
    Array offsets(a.get_as_ref(0), 0, 0, alloc);
    TIGHTDB_ASSERT(!offsets.is_empty());
    return size_t(offsets.back());
}

bool ColumnBase::is_node_from_ref(ref_type ref, Allocator& alloc) TIGHTDB_NOEXCEPT
{
    const uint8_t* header = reinterpret_cast<uint8_t*>(alloc.translate(ref));
    bool is_node = (header[0] & 0x80) != 0;
    return is_node;
}

Column::Column(Allocator& alloc): m_index(0)
{
    m_array = new Array(Array::type_Normal, 0, 0, alloc);
    Create();
}

Column::Column(Array::Type type, Allocator& alloc): m_index(0)
{
    m_array = new Array(type, 0, 0, alloc);
    Create();
}

Column::Column(Array::Type type, ArrayParent* parent, size_t pndx, Allocator& alloc): m_index(0)
{
    m_array = new Array(type, parent, pndx, alloc);
    Create();
}

Column::Column(ref_type ref, ArrayParent* parent, size_t pndx, Allocator& alloc): m_index(0)
{
    m_array = new Array(ref, parent, pndx, alloc);
}

Column::Column(const Column& column): ColumnBase(), m_index(0)
{
    m_array = column.m_array; // we now own array
    // FIXME: Unfortunate hidden constness violation here
    column.m_array = 0;       // so invalidate source
}

void Column::Create()
{
    // Add subcolumns for nodes
    if (!root_is_leaf()) {
        Array offsets(Array::type_Normal, 0, 0, m_array->get_alloc());
        Array refs(Array::type_HasRefs, 0, 0, m_array->get_alloc());
        m_array->add(offsets.get_ref());
        m_array->add(refs.get_ref());
    }
}

void Column::update_ref(ref_type ref)
{
    m_array->update_ref(ref);
}

bool Column::operator==(const Column& column) const
{
    return *m_array == *column.m_array;
}

Column::~Column()
{
    delete m_array;
    delete m_index; // does not destroy index!
}

void Column::destroy()
{
    ClearIndex();
    if (m_array)
        m_array->destroy();
}


bool Column::is_empty() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf())
        return m_array->is_empty();
    const Array offsets = NodeGetOffsets();
    return offsets.is_empty();
}

size_t Column::size() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf())
        return m_array->size();
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
    m_array->set_type(Array::type_HasRefs);
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

void Column::clear()
{
    m_array->clear();
    if (!m_array->is_leaf())
        m_array->set_type(Array::type_Normal);
}

void Column::set(size_t ndx, int64_t value)
{
    const int64_t oldVal = m_index ? get(ndx) : 0; // cache oldval for index

    TreeSet<int64_t, Column>(ndx, value);

    // Update index
    if (m_index)
        m_index->set(ndx, oldVal, value);
}

void Column::add(int64_t value)
{
    insert(size(), value);
}

void Column::insert(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx <= size());

    TreeInsert<int64_t, Column>(ndx, value);

    // Update index
    if (m_index) {
        const bool isLast = (ndx+1 == size());
        m_index->insert(ndx, value, isLast);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
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

// int64_t specific:

size_t Column::count(int64_t target) const
{
    return size_t(aggregate<int64_t, int64_t, act_Count, Equal>(target, 0, size(), NULL));
}

int64_t Column::sum(size_t start, size_t end) const
{
    return aggregate<int64_t, int64_t, act_Sum, None>(0, start, end, NULL);
}

double Column::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = size();
    size_t size = end - start;
    int64_t sum = aggregate<int64_t, int64_t, act_Sum, None>(0, start, end, NULL);
    double avg = double( sum ) / double( size == 0 ? 1 : size );
    return avg;
}

int64_t Column::minimum(size_t start, size_t end) const
{
    return aggregate<int64_t, int64_t, act_Min, None>(0, start, end, NULL);
}

int64_t Column::maximum(size_t start, size_t end) const
{
    return aggregate<int64_t, int64_t, act_Max, None>(0, start, end, NULL);
}



void Column::sort(size_t start, size_t end)
{
    Array arr;
    TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&arr);
    for (size_t t = 0; t < arr.size(); t++) {
        const size_t ref = to_ref(arr.get(t));
        Array a(ref);
        a.sort();
    }

    Array* sorted = merge(arr);
    if (sorted) {
        // Todo, this is a bit slow. Add bulk insert or the like to Column
        const size_t count = sorted->size();
        for (size_t t = 0; t < count; ++t) {
            set(t, sorted->get(t));
        }

        sorted->destroy();
        delete sorted;
    }

    // Clean-up
    arr.destroy();
}


// TODO: Set owner of created arrays and destroy/delete them if created by merge_references()
void Column::ReferenceSort(size_t start, size_t end, Column& ref)
{
    Array values; // pointers to non-instantiated arrays of values
    Array indexes; // pointers to instantiated arrays of index pointers
    Array all_values;
    TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, (void *)&values);

    size_t offset = 0;
    for (size_t t = 0; t < values.size(); t++) {
        Array *i = new Array();
        size_t ref = values.get_as_ref(t);
        Array v(ref);
        for (size_t j = 0; j < v.size(); j++)
            all_values.add(v.get(j));
        v.ReferenceSort(*i);
        for (size_t n = 0; n < v.size(); n++)
            i->set(n, i->get(n) + offset);
        offset += v.size();
        indexes.add(int64_t(i));
    }

    Array *ResI;

    merge_references(&all_values, &indexes, &ResI);

    for (size_t t = 0; t < ResI->size(); t++)
        ref.add(ResI->get(t));
}

size_t ColumnBase::GetRefSize(size_t ref) const
{
    // parse the length part of 8byte header
    const uint8_t* const header = (uint8_t*)m_array->get_alloc().translate(ref);
    return (header[1] << 16) + (header[2] << 8) + header[3];
}

Array ColumnBase::NodeGetOffsets() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!root_is_leaf());
    return m_array->GetSubArray(0); // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array genrally causes the parent to be modified too.
}

Array ColumnBase::NodeGetRefs() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!root_is_leaf());
    return m_array->GetSubArray(1); // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array genrally causes the parent to be modified too.
}

void ColumnBase::NodeUpdateOffsets(size_t ndx)
{
    TIGHTDB_ASSERT(!root_is_leaf());

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();
    TIGHTDB_ASSERT(ndx < offsets.size());

    const int64_t newSize = GetRefSize(refs.get_as_ref(ndx));
    const int64_t oldSize = offsets.get(ndx) - (ndx ? offsets.get(ndx-1) : 0);
    const int64_t diff = newSize - oldSize;

    offsets.Increment(diff, ndx);
}

void ColumnBase::NodeAddKey(size_t ref)
{
    TIGHTDB_ASSERT(ref);
    TIGHTDB_ASSERT(!root_is_leaf());

    Array offsets = NodeGetOffsets();
    Array refs = NodeGetRefs();
    TIGHTDB_ASSERT(offsets.size() < TIGHTDB_MAX_LIST_SIZE);

    const Array new_top(ref, NULL, 0,m_array->get_alloc());
    const Array new_offsets(new_top.get_as_ref(0), NULL, 0,m_array->get_alloc());
    TIGHTDB_ASSERT(!new_offsets.is_empty());

    const int64_t key = new_offsets.back();
    offsets.add(key);
    refs.add(ref);
}

void Column::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());

    const int64_t oldVal = m_index ? get(ndx) : 0; // cache oldval for index

    TreeDelete<int64_t, Column>(ndx);

    // Flatten tree if possible
    while (!root_is_leaf()) {
        Array refs = NodeGetRefs();
        if (refs.size() != 1)
            break;

        size_t ref = refs.get_as_ref(0);
        refs.erase(0); // avoid destroying subtree
        m_array->destroy();
        m_array->update_ref(ref);
    }

    // Update index
    if (m_index) {
        bool isLast = (ndx == size());
        m_index->erase(ndx, oldVal, isLast);
    }
}

void Column::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());

    size_t ndx_last = size()-1;
    int64_t v = get(ndx_last);

    set(ndx, v);
    erase(ndx_last);
}

void Column::Increment64(int64_t value, size_t start, size_t end)
{
    if (root_is_leaf()) {
        m_array->Increment(value, start, end);
        return;
    }

    //TODO: partial incr
    Array refs = NodeGetRefs();
    size_t count = refs.size();
    for (size_t i = 0; i < count; ++i) {
        Column col = ::GetColumnFromRef(refs, i);
        col.Increment64(value);
    }
}

void Column::IncrementIf(int64_t limit, int64_t value)
{
    if (root_is_leaf()) m_array->IncrementIf(limit, value);
    else {
        Array refs = NodeGetRefs();
        size_t count = refs.size();
        for (size_t i = 0; i < count; ++i) {
            Column col = ::GetColumnFromRef(refs, i);
            col.IncrementIf(limit, value);
        }
    }
}

size_t Column::find_first(int64_t value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(start <= size());
    TIGHTDB_ASSERT(end == size_t(-1) || end <= size());

    if (start == 0 && end == size_t(-1)) {
        Array cache(m_array->get_alloc());
        size_t ref = m_array->get_ref();
        return m_array->ColumnFind(value, ref, cache);
    }
    else {
        return TreeFind<int64_t, Column, Equal>(value, start, end);
    }
}

void Column::find_all(Array& result, int64_t value, size_t caller_offset, size_t start, size_t end) const
{
    (void)caller_offset;
    TIGHTDB_ASSERT(start <= size());
    TIGHTDB_ASSERT(end == size_t(-1) || end <= size());
    if (is_empty())
        return;
    TreeFindAll<int64_t, Column>(result, value, 0, start, end);
}

void Column::LeafFindAll(Array &result, int64_t value, size_t add_offset, size_t start, size_t end) const
{
    m_array->find_all(result, value, add_offset, start, end);
}

size_t Column::find_pos(int64_t target) const TIGHTDB_NOEXCEPT
{
    // NOTE: Binary search only works if the column is sorted

    if (root_is_leaf()) {
        return m_array->FindPos(target);
    }

    const int len = int(size());
    int low = -1;
    int high = len;

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of largest value SMALLER than the target
    while (high - low > 1) {
        size_t probe = (unsigned(low) + unsigned(high)) >> 1;
        int64_t v = get(probe);

        if (v > target)
            high = int(probe);
        else
            low  = int(probe);
    }
    if (high == len)
        return not_found;
    else
        return high;
}

size_t Column::find_pos2(int64_t target) const TIGHTDB_NOEXCEPT
{
    // NOTE: Binary search only works if the column is sorted

    if (root_is_leaf()) {
        return m_array->FindPos2(target);
    }

    const int len = int(size());
    int low = -1;
    int high = len;

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of closest value BIGGER OR EQUAL to the target
    while (high - low > 1) {
        size_t probe = ((unsigned int)low + (unsigned int)high) >> 1;
        int64_t v = get(probe);

        if (v < target)
            low  = int(probe);
        else
            high = int(probe);
    }
    if (high == len)
        return not_found;
    else
        return high;
}

bool Column::find_sorted(int64_t target, size_t& pos) const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        return m_array->FindPosSorted(target, pos);
    }

    const size_t len = size();
    size_t low = size_t(-1);
    size_t high = len;

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of closest value BIGGER OR EQUAL to the target
    while (high - low > 1) {
        size_t probe = (low + high) >> 1;
        int64_t v = get(probe);

        if (v < target)
            low  = probe;
        else
            high = probe;
    }

    pos = high;
    if (high == len)
        return false;
    else
        return get(high) == target;
}


size_t Column::FindWithIndex(int64_t target) const
{
    TIGHTDB_ASSERT(m_index);
    TIGHTDB_ASSERT(m_index->size() == size());

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
        m_index->destroy();
        delete m_index;
        m_index = 0;
    }
}

void Column::BuildIndex(Index& index)
{
    index.BuildIndex(*this);
    m_index = &index; // Keep ref to index
}

void Column::sort()
{
    sort(0, size());
}

bool Column::compare(const Column& c) const
{
    const size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


#ifdef TIGHTDB_DEBUG

void Column::Print() const
{
    if (!root_is_leaf()) {
        cout << "Node: " << hex << m_array->get_ref() << dec << "\n";

        const Array offsets = NodeGetOffsets();
        const Array refs = NodeGetRefs();

        for (size_t i = 0; i < refs.size(); ++i) {
            cout << " " << i << ": " << offsets.get(i) << " " << hex << refs.get(i) << dec <<"\n";
        }
        for (size_t i = 0; i < refs.size(); ++i) {
            const Column col(refs.get_as_ref(i));
            col.Print();
        }
    }
    else {
        m_array->Print();
    }
}

void Column::Verify() const
{
    if (!root_is_leaf()) {
        TIGHTDB_ASSERT(m_array->size() == 2);
        //TIGHTDB_ASSERT(m_hasRefs);

        const Array offsets = NodeGetOffsets();
        const Array refs = NodeGetRefs();
        offsets.Verify();
        refs.Verify();
        TIGHTDB_ASSERT(refs.has_refs());
        TIGHTDB_ASSERT(offsets.size() == refs.size());

        size_t off = 0;
        for (size_t i = 0; i < refs.size(); ++i) {
            const size_t ref = size_t(refs.get(i));
            TIGHTDB_ASSERT(ref);

            const Column col(ref, NULL, 0, m_array->get_alloc());
            col.Verify();

            off += col.size();
            const size_t node_off = size_t(offsets.get(i));
            if (node_off != off) {
                TIGHTDB_ASSERT(false);
            }
        }
    }
    else
        m_array->Verify();
}

void ColumnBase::ToDot(ostream& out, StringData title) const
{
    const size_t ref = get_ref();

    out << "subgraph cluster_column" << ref << " {" << endl;
    out << " label = \"Column";
    if (0 < title.size()) out << "\\n'" << title << "'";
    out << "\";" << endl;

    ArrayToDot(out, *m_array);

    out << "}" << endl;
}

void ColumnBase::ArrayToDot(ostream& out, const Array& array) const
{
    if (!array.is_leaf()) {
        const Array offsets = array.GetSubArray(0);
        const Array refs    = array.GetSubArray(1);
        const size_t ref    = array.get_ref();

        out << "subgraph cluster_node" << ref << " {" << endl;
        out << " label = \"Node\";" << endl;

        array.ToDot(out);
        offsets.ToDot(out, "offsets");

        out << "}" << endl;

        refs.ToDot(out, "refs");

        const size_t count = refs.size();
        for (size_t i = 0; i < count; ++i) {
            const Array r = refs.GetSubArray(i);
            ArrayToDot(out, r);
        }
    }
    else
        LeafToDot(out, array);
}

void ColumnBase::LeafToDot(ostream& out, const Array& array) const
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
