#include <stdint.h> // unint8_t etc
#include <cstdlib>
#include <cstring>
#include <climits>
#include <sstream>
#include <iostream>
#include <iomanip>

#include <tightdb/column.hpp>
#include <tightdb/query_engine.hpp>

using namespace std;
using namespace tightdb;


namespace {

/*
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
Array* merge(const Array& array_list)
{
    size_t size = array_list.size();

    if (size == 1)
        return NULL; // already sorted

    Array left_half, right_half;
    const size_t leftSize = size / 2;
    for (size_t t = 0; t < leftSize; ++t)
        left_half.add(array_list.get(t));
    for (size_t t = leftSize; t < size; ++t)
        right_half.add(array_list.get(t));

    // We merge left-half-first instead of bottom-up so that we access the same data in each call
    // so that it's in cache, at least for the first few iterations until lists get too long
    Array* left = merge(left_half);
    Array* right = merge(right_half);
    Array* res = new Array();

    if (left && right)
        merge_core(*left, *right, *res);
    else if (left) {
        ref_type ref = right_half.get_as_ref(0);
        Array right0(ref, 0);
        merge_core(*left, right0, *res);
    }
    else if (right) {
        ref_type ref = left_half.get_as_ref(0);
        Array left0(ref, 0);
        merge_core(left0, *right, *res);
    }

    // Clean-up
    left_half.destroy();
    right_half.destroy();
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

    Array* li;
    Array* ri;

    Array* resI = new Array;

    // We merge left-half-first instead of bottom-up so that we access the same data in each call
    // so that it's in cache, at least for the first few iterations until lists get too long
    merge_references(valuelist, &leftI, &ri);
    merge_references(valuelist, &rightI, &li);
    merge_core_references(valuelist, li, ri, resI);

    *indexresult = resI;
}
*/

} // anonymous namespace


void ColumnBase::adjust_ndx_in_parent(int diff) TIGHTDB_NOEXCEPT
{
    m_array->adjust_ndx_in_parent(diff);
}

void ColumnBase::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    m_array->update_from_parent(old_baseline);
}


void ColumnBase::introduce_new_root(ref_type new_sibling_ref, Array::TreeInsertBase& state,
                                    bool is_append)
{
    // At this point the original root and its new sibling is either
    // both leafs, or both inner nodes on the same form, compact or
    // general. Due to invar:bptree-node-form, the new root may be on
    // the compact form if is_append is true and both are either leafs
    // or inner nodes on the compact form.

    Array* orig_root = m_array;
    Allocator& alloc = orig_root->get_alloc();
    ArrayParent* parent = orig_root->get_parent();
    size_t ndx_in_parent = orig_root->get_ndx_in_parent();
    UniquePtr<Array> new_root(new Array(Array::type_InnerColumnNode,
                                        parent, ndx_in_parent, alloc)); // Throws
    bool compact_form = is_append && (orig_root->is_leaf() || orig_root->get(0) % 2 == 1);
    // Something is wrong if we were not appending and the original
    // root is still on the compact form.
    TIGHTDB_ASSERT(!compact_form || is_append);
    if (compact_form) {
        // FIXME: Dangerous cast here (unsigned -> signed)
        int_fast64_t v = state.m_split_offset; // elems_per_child
        new_root->add(1 + 2*v); // Throws
    }
    else {
        Array new_offsets(alloc);
        new_offsets.create(Array::type_Normal); // Throws
        // FIXME: Dangerous cast here (unsigned -> signed)
        new_offsets.add(state.m_split_offset); // Throws
        // FIXME: Dangerous cast here (unsigned -> signed)
        new_root->add(new_offsets.get_ref()); // Throws
    }
    // FIXME: Dangerous cast here (unsigned -> signed)
    new_root->add(orig_root->get_ref()); // Throws
    // FIXME: Dangerous cast here (unsigned -> signed)
    new_root->add(new_sibling_ref); // Throws
    // FIXME: Dangerous cast here (unsigned -> signed)
    int_fast64_t v = state.m_split_size; // total_elems_in_tree
    new_root->add(1 + 2*v); // Throws
    delete orig_root;
    m_array = new_root.release();
}


void Column::clear()
{
    m_array->clear();
    if (!m_array->is_leaf())
        m_array->set_type(Array::type_Normal);
}


namespace {

struct SetLeafElem: Array::UpdateHandler {
    Array m_leaf;
    const int_fast64_t m_value;
    SetLeafElem(Allocator& alloc, int_fast64_t value) TIGHTDB_NOEXCEPT:
        m_leaf(alloc), m_value(value) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        m_leaf.init_from_mem(mem);
        m_leaf.set_parent(parent, ndx_in_parent);
        m_leaf.set(elem_ndx_in_leaf, m_value); // Throws
    }
};

} // anonymous namespace

void Column::set(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx < size());

    if (m_array->is_leaf()) {
        m_array->set(ndx, value); // Throws
        return;
    }

    SetLeafElem set_leaf_elem(m_array->get_alloc(), value);
    m_array->update_bptree_elem(ndx, set_leaf_elem); // Throws
}

void Column::fill(size_t count)
{
    TIGHTDB_ASSERT(size() == 0);

    // Fill column with default values
    //
    // FIXME: this is a very naive approach we could speedup by
    // creating full nodes directly
    for (size_t i = 0; i < count; ++i)
        add(0);
}

// int64_t specific:

size_t Column::count(int64_t target) const
{
    return size_t(aggregate<int64_t, int64_t, act_Count, Equal>(target, 0, size()));
}

int64_t Column::sum(size_t start, size_t end, size_t limit) const
{
    return aggregate<int64_t, int64_t, act_Sum, None>(0, start, end, limit);
}

double Column::average(size_t start, size_t end, size_t limit) const
{
    if (end == size_t(-1))
        end = size();
    size_t size = end - start;
    if(limit < size)
        size = limit;
    int64_t sum = aggregate<int64_t, int64_t, act_Sum, None>(0, start, end, limit);
    double avg = double(sum) / double(size == 0 ? 1 : size);
    return avg;
}

int64_t Column::minimum(size_t start, size_t end, size_t limit) const
{
    return aggregate<int64_t, int64_t, act_Min, None>(0, start, end, limit);
}

int64_t Column::maximum(size_t start, size_t end, size_t limit) const
{
    return aggregate<int64_t, int64_t, act_Max, None>(0, start, end, limit);
}


/*
// TODO: Set owner of created arrays and destroy/delete them if created by merge_references()
void Column::ReferenceSort(size_t start, size_t end, Column& ref)
{
    Array values; // pointers to non-instantiated arrays of values
    Array indexes; // pointers to instantiated arrays of index pointers
    Array all_values;
    TreeVisitLeafs<Array, Column>(start, end, 0, callme_arrays, &values);

    size_t offset = 0;
    for (size_t t = 0; t < values.size(); t++) {
        Array* i = new Array();
        ref_type ref = values.get_as_ref(t);
        Array v(ref);
        for (size_t j = 0; j < v.size(); j++)
            all_values.add(v.get(j));
        v.ReferenceSort(*i);
        for (size_t n = 0; n < v.size(); n++)
            i->set(n, i->get(n) + offset);
        offset += v.size();
        indexes.add(int64_t(i));
    }

    Array* ResI;

    merge_references(&all_values, &indexes, &ResI);

    for (size_t t = 0; t < ResI->size(); t++)
        ref.add(ResI->get(t));
}
*/


class Column::EraseLeafElem: public EraseHandlerBase {
public:
    Array m_leaf;
    bool m_leaves_have_refs;
    EraseLeafElem(Column& column) TIGHTDB_NOEXCEPT:
        EraseHandlerBase(column), m_leaf(get_alloc()),
        m_leaves_have_refs(false) {}
    bool erase_leaf_elem(MemRef leaf_mem, ArrayParent* parent,
                         size_t leaf_ndx_in_parent,
                         size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        m_leaf.init_from_mem(leaf_mem);
        TIGHTDB_ASSERT(m_leaf.size() >= 1);
        size_t last_ndx = m_leaf.size() - 1;
        if (last_ndx == 0) {
            m_leaves_have_refs = m_leaf.has_refs();
            return true;
        }
        m_leaf.set_parent(parent, leaf_ndx_in_parent);
        size_t ndx = elem_ndx_in_leaf;
        if (ndx == npos)
            ndx = last_ndx;
        m_leaf.erase(ndx); // Throws
        return false;
    }
    void destroy_leaf(MemRef leaf_mem) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        get_alloc().free_(leaf_mem);
    }
    void replace_root_by_leaf(MemRef leaf_mem) TIGHTDB_OVERRIDE
    {
        UniquePtr<Array> leaf(new Array(get_alloc())); // Throws
        leaf->init_from_mem(leaf_mem);
        replace_root(leaf); // Throws
    }
    void replace_root_by_empty_leaf() TIGHTDB_OVERRIDE
    {
        UniquePtr<Array> leaf(new Array(get_alloc())); // Throws
        leaf->create(m_leaves_have_refs ? Array::type_HasRefs :
                     Array::type_Normal); // Throws
        replace_root(leaf); // Throws
    }
};

void Column::erase(size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(ndx < size());
    TIGHTDB_ASSERT(is_last == (ndx == size()-1));

    if (m_array->is_leaf()) {
        m_array->erase(ndx); // Throws
        return;
    }

    size_t ndx_2 = is_last ? npos : ndx;
    EraseLeafElem handler(*this);
    Array::erase_bptree_elem(m_array, ndx_2, handler); // Throws
}


void Column::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());

    size_t last_ndx = size() - 1;
    int64_t v = get(last_ndx);

    set(ndx, v); // Throws

    bool is_last = true;
    erase(last_ndx, is_last); // Throws
}


namespace {

template<bool with_limit> struct AdjustHandler: Array::UpdateHandler {
    Array m_leaf;
    const int_fast64_t m_limit, m_diff;
    AdjustHandler(Allocator& alloc, int_fast64_t limit, int_fast64_t diff) TIGHTDB_NOEXCEPT:
        m_leaf(alloc), m_limit(limit), m_diff(diff) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent, size_t) TIGHTDB_OVERRIDE
    {
        m_leaf.init_from_mem(mem);
        m_leaf.set_parent(parent, ndx_in_parent);
        if (with_limit) {
            m_leaf.adjust_ge(m_limit, m_diff); // Throws
        }
        else {
            m_leaf.adjust(0, m_leaf.size(), m_diff); // Throws
        }
    }
};

} // anonymous namespace

void Column::adjust(int_fast64_t diff)
{
    if (m_array->is_leaf()) {
        m_array->adjust(0, m_array->size(), diff); // Throws
        return;
    }

    const bool with_limit = false;
    int_fast64_t dummy_limit = 0;
    AdjustHandler<with_limit> leaf_handler(m_array->get_alloc(), dummy_limit, diff);
    m_array->update_bptree_leaves(leaf_handler); // Throws
}



void Column::adjust_ge(int_fast64_t limit, int_fast64_t diff)
{
    if (m_array->is_leaf()) {
        m_array->adjust_ge(limit, diff); // Throws
        return;
    }

    const bool with_limit = true;
    AdjustHandler<with_limit> leaf_handler(m_array->get_alloc(), limit, diff);
    m_array->update_bptree_leaves(leaf_handler); // Throws
}



size_t Column::find_first(int64_t value, size_t begin, size_t end) const
{
    TIGHTDB_ASSERT(begin <= size());
    TIGHTDB_ASSERT(end == npos || (begin <= end && end <= size()));

    if (root_is_leaf())
        return m_array->find_first(value, begin, end); // Throws (maybe)

    // FIXME: It would be better to always require that 'end' is
    // specified explicitely, since Table has the size readily
    // available, and Array::get_bptree_size() is deprecated.
    if (end == npos)
        end = m_array->get_bptree_size();

    Array leaf(m_array->get_alloc());
    size_t ndx_in_tree = begin;
    while (ndx_in_tree < end) {
        pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_tree);
        leaf.init_from_mem(p.first);
        size_t ndx_in_leaf = p.second;
        size_t leaf_offset = ndx_in_tree - ndx_in_leaf;
        size_t end_in_leaf = min(leaf.size(), end - leaf_offset);
        size_t ndx = leaf.find_first(value, ndx_in_leaf, end_in_leaf); // Throws (maybe)
        if (ndx != not_found)
            return leaf_offset + ndx;
        ndx_in_tree = leaf_offset + end_in_leaf;
    }

    return not_found;
}

void Column::find_all(Array& result, int64_t value, size_t begin, size_t end) const
{
    TIGHTDB_ASSERT(begin <= size());
    TIGHTDB_ASSERT(end == npos || (begin <= end && end <= size()));

    if (root_is_leaf()) {
        size_t leaf_offset = 0;
        m_array->find_all(result, value, leaf_offset, begin, end); // Throws
        return;
    }

    // FIXME: It would be better to always require that 'end' is
    // specified explicitely, since Table has the size readily
    // available, and Array::get_bptree_size() is deprecated.
    if (end == npos)
        end = m_array->get_bptree_size();

    Array leaf(m_array->get_alloc());
    size_t ndx_in_tree = begin;
    while (ndx_in_tree < end) {
        pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_tree);
        leaf.init_from_mem(p.first);
        size_t ndx_in_leaf = p.second;
        size_t leaf_offset = ndx_in_tree - ndx_in_leaf;
        size_t end_in_leaf = min(leaf.size(), end - leaf_offset);
        leaf.find_all(result, value, leaf_offset, ndx_in_leaf, end_in_leaf); // Throws
        ndx_in_tree = leaf_offset + end_in_leaf;
    }
}


bool Column::compare_int(const Column& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


void Column::do_insert(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx == npos || ndx < size());
    ref_type new_sibling_ref;
    Array::TreeInsert<Column> state;
    if (root_is_leaf()) {
        TIGHTDB_ASSERT(ndx == npos || ndx < TIGHTDB_MAX_LIST_SIZE);
        new_sibling_ref = m_array->bptree_leaf_insert(ndx, value, state);
    }
    else {
        state.m_value = value;
        if (ndx == npos) {
            new_sibling_ref = m_array->bptree_append(state);
        }
        else {
            new_sibling_ref = m_array->bptree_insert(ndx, state);
        }
    }

    if (TIGHTDB_UNLIKELY(new_sibling_ref)) {
        bool is_append = ndx == npos;
        introduce_new_root(new_sibling_ref, state, is_append);
    }
}


#ifdef TIGHTDB_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    leaf.Verify();
    return leaf.size();
}

} // anonymous namespace

void Column::Verify() const
{
    if (root_is_leaf()) {
        m_array->Verify();
        return;
    }

    m_array->verify_bptree(&verify_leaf);
}

class ColumnBase::LeafToDot: public Array::ToDotHandler {
public:
    const ColumnBase& m_column;
    LeafToDot(const ColumnBase& column): m_column(column) {}
    void to_dot(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                ostream& out) TIGHTDB_OVERRIDE
    {
        m_column.leaf_to_dot(mem, parent, ndx_in_parent, out);
    }
};

void ColumnBase::tree_to_dot(ostream& out) const
{
    LeafToDot handler(*this);
    m_array->bptree_to_dot(out, handler);
}

void ColumnBase::dump_node_structure() const
{
    dump_node_structure(cerr, 0);
}


void Column::to_dot(ostream& out, StringData title) const
{
    ref_type ref = m_array->get_ref();
    out << "subgraph cluster_integer_column" << ref << " {" << endl;
    out << " label = \"Integer column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;
    tree_to_dot(out);
    out << "}" << endl;
}

void Column::leaf_to_dot(MemRef leaf_mem, ArrayParent* parent, size_t ndx_in_parent,
                         ostream& out) const
{
    Array leaf(m_array->get_alloc());
    leaf.init_from_mem(leaf_mem);
    leaf.set_parent(parent, ndx_in_parent);
    leaf.to_dot(out);
}

MemStats Column::stats() const
{
    MemStats stats;
    m_array->stats(stats);

    return stats;
}

namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, ostream& out, int level)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    int indent = level * 2;
    out << setw(indent) << "" << "Integer leaf (ref: "<<leaf.get_ref()<<", "
        "size: "<<leaf.size()<<")\n";
    ostringstream out_2;
    for (size_t i = 0; i != leaf.size(); ++i) {
        if (i != 0) {
            out_2 << ", ";
            if (out_2.tellp() > 70) {
                out_2 << "...";
                break;
            }
        }
        out_2 << leaf.get(i);
    }
    out << setw(indent) << "" << "  Elems: "<<out_2.str()<<"\n";
}

} // anonymous namespace

void Column::dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // TIGHTDB_DEBUG
