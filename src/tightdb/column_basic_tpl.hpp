#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#ifdef _MSC_VER
    #include <win32\types.h>
#endif

//#include <tightdb/query.hpp>
#include <tightdb/query_engine.hpp>

class tightdb::ParentNode;
template <class T, class F> class tightdb::BASICNODE;


namespace {

using namespace tightdb;

// TODO: This could be common 
bool IsNodeFromRef(size_t ref, Allocator& alloc)
{
    const uint8_t* const header = (uint8_t*)alloc.Translate(ref);
    const bool isNode = (header[0] & 0x80) != 0;

    return isNode;
}

}


namespace tightdb {

template<typename T>
ColumnBasic<T>::ColumnBasic(Allocator& alloc)
{
    m_array = new ArrayBasic<T>(NULL, 0, alloc);
}

template<typename T>
ColumnBasic<T>::ColumnBasic(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    const bool isNode = IsNodeFromRef(ref, alloc);
    if (isNode)
        m_array = new Array(ref, parent, pndx, alloc);
    else
        m_array = new ArrayBasic<T>(ref, parent, pndx, alloc);
}

template<typename T>
ColumnBasic<T>::~ColumnBasic()
{
    if (IsNode()) 
        delete m_array;
    else 
        delete (ArrayBasic<T>*)m_array;
}

template<typename T>
void ColumnBasic<T>::Destroy()
{
    if (IsNode()) 
        m_array->Destroy();
    else 
        ((ArrayBasic<T>*)m_array)->Destroy();
}


template<typename T>
void ColumnBasic<T>::UpdateRef(size_t ref)
{
    TIGHTDB_ASSERT(IsNodeFromRef(ref, m_array->GetAllocator())); // Can only be called when creating node

    if (IsNode()) 
        m_array->UpdateRef(ref);
    else {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Replace the generic array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->GetAllocator());
        delete m_array;
        m_array = array;

        // Update ref in parent
        if (parent) 
            parent->update_child_ref(pndx, ref);
    }
}

template<typename T>
bool ColumnBasic<T>::is_empty() const
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return ((ArrayBasic<T>*)m_array)->is_empty();
    }
}

template<typename T>
size_t ColumnBasic<T>::Size() const
{
    if (IsNode())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : (size_t)offsets.back();
        return size;
    }
    else {
        return ((ArrayBasic<T>*)m_array)->Size();
    }
}

template<typename T>
void ColumnBasic<T>::Clear()
{
    if (m_array->IsNode()) {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Revert to generic array
        ArrayBasic<T>* array = new ArrayBasic<T>(parent, pndx, m_array->GetAllocator());
        if (parent) 
            parent->update_child_ref(pndx, array->GetRef());

        // Remove original node
        m_array->Destroy();
        delete m_array;

        m_array = array;
    }
    else 
        ((ArrayBasic<T>*)m_array)->Clear();
}

template<typename T>
void ColumnBasic<T>::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < Size());
    ((ArrayBasic<T>*)m_array)->Resize(ndx);
}

template<typename T>
T ColumnBasic<T>::Get(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());
    // return m_array->Column???StringGet(ndx);
    // FIXME??? why different in stringColumn?
    return TreeGet<T, ColumnBasic<T> >(ndx);
}

template<typename T>
bool ColumnBasic<T>::Set(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < Size());
    return TreeSet<T,ColumnBasic<T> >(ndx, value);
}

template<typename T>
bool ColumnBasic<T>::add(T value)
{
    return Insert(Size(), value);
}

template<typename T>
bool ColumnBasic<T>::Insert(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= Size());
    return TreeInsert<T, ColumnBasic<T> >(ndx, value);
}

template<typename T>
void ColumnBasic<T>::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<T, ColumnBasic<T> >(i, 0);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

template<typename T>
bool ColumnBasic<T>::Compare(const ColumnBasic& c) const
{
    const size_t n = Size();
    if (c.Size() != n) 
        return false;
    for (size_t i=0; i<n; ++i) {
        const T v1 = Get(i);
        const T v2 = c.Get(i);
        if (v1 == v2) 
            return false;
    }
    return true;
}


template<typename T>
void ColumnBasic<T>::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeDelete<T, ColumnBasic<T> >(ndx);
}

template<typename T>
T ColumnBasic<T>::LeafGet(size_t ndx) const
{
    return ((ArrayBasic<T>*)m_array)->Get(ndx);
}

template<typename T>
bool ColumnBasic<T>::LeafSet(size_t ndx, T value)
{
    ((ArrayBasic<T>*)m_array)->Set(ndx, value);
    return true;
}

template<typename T>
bool ColumnBasic<T>::LeafInsert(size_t ndx, T value)
{
    ((ArrayBasic<T>*)m_array)->Insert(ndx, value);
    return true;
}

template<typename T>
void ColumnBasic<T>::LeafDelete(size_t ndx)
{
    ((ArrayBasic<T>*)m_array)->Delete(ndx);
}


#ifdef TIGHTDB_DEBUG

template<typename T>
void ColumnBasic<T>::LeafToDot(std::ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.GetRef();
    const ArrayBasic<T> newArray(ref, NULL, 0, array.GetAllocator());

    newArray.ToDot(out);
}

#endif // TIGHTDB_DEBUG


template<typename T> template<class F>
size_t ColumnBasic<T>::LeafFind(T value, size_t start, size_t end) const
{
    return ((ArrayBasic<T>*)m_array)->find_first(value, start, end);
}

template<typename T>
void ColumnBasic<T>::LeafFindAll(Array &result, T value, size_t add_offset, size_t start, size_t end) const
{
    return ((ArrayBasic<T>*)m_array)->find_all(result, value, add_offset, start, end);
}

template<typename T>
size_t ColumnBasic<T>::find_first(T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    return TreeFind<T, ColumnBasic<T>, EQUAL>(value, start, end);
}

template<typename T>
void ColumnBasic<T>::find_all(Array &result, T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    TreeFindAll<T, ColumnBasic<T> >(result, value, 0, start, end);
}


#if 1

// TODO: Move to ColumnBase?
template<typename T> template <typename R, ACTION action, class condition>
R ColumnBasic<T>::aggregate(T target, size_t start, size_t end, size_t *matchcount) const
{ 
    if (end == size_t(-1)) 
        end = Size();

    // We must allocate 'node' on stack with malloca() because malloc is slow (makes aggregate on 1000 elements around 10 times
    // slower because of initial over).
        //    NODE<int64_t, Column, cond>* node = (NODE<int64_t, Column, cond>*)alloca(sizeof(NODE<int64_t, Column, cond>));     
        //    new (node) NODE<int64_t, Column, cond>(target, 0);
    BASICNODE<T, condition> node(target, NULL);

    node.QuickInit((ColumnBasic<T>*)this, target); 
    state_state<R> st;
    st.init(action, NULL, size_t(-1));

    SequentialGetter<T> sg((ColumnTypeTraits<T>::column_type*)this); 
    node.template aggregate_local<action, R, T>(&st, start, end, size_t(-1), &sg, matchcount);

    return st.state;
}

template<typename T>
size_t ColumnBasic<T>::count(T target) const
{
    return size_t(aggregate<int64_t, TDB_COUNT, EQUAL>(target, 0, Size()));
}

template<typename T>
T ColumnBasic<T>::sum(size_t start, size_t end) const
{
    return aggregate<T, TDB_SUM, NONE>(0, start, end);
}

template<typename T>
double ColumnBasic<T>::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();
    size_t size = end - start;
    T sum1 = aggregate<T, TDB_SUM, NONE>(0, start, end);
    double avg = double(sum1) / double( size == 0 ? 1 : size ); 
    return avg;
}

template<typename T>
T ColumnBasic<T>::minimum(size_t start, size_t end) const
{
    return aggregate<T, TDB_MIN, NONE>(0, start, end);
}

template<typename T>
T ColumnBasic<T>::maximum(size_t start, size_t end) const
{
    return aggregate<T, TDB_MAX, NONE>(0, start, end);
}

/*
template<typename T>
void ColumnBasic<T>::sort(size_t start, size_t end)
{
    // TODO
    assert(0);
}
*/

#else

// TODO: test performance of column aggregates, remove slowest
template<typename T>
size_t ColumnBasic<T>::count(T target) const
{
    size_t count = 0;
    
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = 0; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const ColumnBasic<T> col(ref, NULL, 0, m_array->GetAllocator());
            
            count += col.count(target);
        }
    }
    else {
        count += ((ArrayBasic<T>*)m_array)->count(target);
    }
    return count;
}

template<typename T>
double ColumnBasic<T>::sum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();

    double sum = 0;
    
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = start; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const ColumnBasic<T> col(ref, NULL, 0, m_array->GetAllocator());
            
            sum += col.sum(start, end);
        }
    }
    else {
        sum += ((ArrayBasic<T>*)m_array)->sum(start, end);
    }
    return sum;
}

template<typename T>
double ColumnBasic<T>::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();
    size_t size = end - start;
    double sum2 = sum(start, end);
    double avg = sum2 / double( size == 0 ? 1 : size );
    return avg;
}

template<typename T>
T ColumnBasic<T>::minimum(size_t start, size_t end) const
{
    assert(0);
    return 0.0;
}

template<typename T>
T ColumnBasic<T>::maximum(size_t start, size_t end) const
{
    assert(0);
    return 0.0;
}

#endif

}
