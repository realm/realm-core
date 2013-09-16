#include <algorithm>
#include <iostream>
#include <iomanip>

#include <tightdb/column_binary.hpp>

using namespace std;
using namespace tightdb;


ColumnBinary::ColumnBinary(Allocator& alloc)
{
    m_array = new ArrayBinary(0, 0, alloc);
}

ColumnBinary::ColumnBinary(ref_type ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    bool root_is_leaf = root_is_leaf_from_ref(ref, alloc);
    if (root_is_leaf) {
        m_array = new ArrayBinary(ref, parent, pndx, alloc);
    }
    else {
        m_array = new Array(ref, parent, pndx, alloc);
    }
}

ColumnBinary::~ColumnBinary() TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        delete static_cast<ArrayBinary*>(m_array);
    }
    else {
        delete m_array;
    }
}

void ColumnBinary::clear()
{
    if (m_array->is_leaf()) {
        static_cast<ArrayBinary*>(m_array)->clear();
        return;
    }

    ArrayParent* parent = m_array->get_parent();
    size_t pndx = m_array->get_ndx_in_parent();

    // Revert to binary array
    ArrayBinary* array = new ArrayBinary(parent, pndx, m_array->get_alloc());
    if (parent)
        parent->update_child_ref(pndx, array->get_ref());

    // Remove original node
    m_array->destroy();
    delete m_array;

    m_array = array;
}


namespace {

struct SetLeafElem: Array::UpdateHandler {
    Allocator& m_alloc;
    const BinaryData m_value;
    const bool m_add_zero_term;
    SetLeafElem(Allocator& alloc, BinaryData value, bool add_zero_term) TIGHTDB_NOEXCEPT:
        m_alloc(alloc), m_value(value), m_add_zero_term(add_zero_term) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        ArrayBinary leaf(mem, parent, ndx_in_parent, m_alloc);
        leaf.set(elem_ndx_in_leaf, m_value, m_add_zero_term); // Throws
    }
};

} // anonymous namespace

void ColumnBinary::set(size_t ndx, BinaryData value)
{
    TIGHTDB_ASSERT(ndx < size());

    if (m_array->is_leaf()) {
        static_cast<ArrayBinary*>(m_array)->set(ndx, value); // Throws
        return;
    }

    bool add_zero_term = false;
    SetLeafElem set_leaf_elem(m_array->get_alloc(), value, add_zero_term);
    m_array->update_bptree_elem(ndx, set_leaf_elem); // Throws
}

void ColumnBinary::set_string(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < size());

    BinaryData value_2(value.data(), value.size());
    bool add_zero_term = true;

    if (m_array->is_leaf()) {
        static_cast<ArrayBinary*>(m_array)->set(ndx, value_2, add_zero_term); // Throws
        return;
    }

    SetLeafElem set_leaf_elem(m_array->get_alloc(), value_2, add_zero_term);
    m_array->update_bptree_elem(ndx, set_leaf_elem); // Throws
}


void ColumnBinary::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        add(BinaryData());
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


class ColumnBinary::EraseLeafElem: public ColumnBase::EraseHandlerBase {
public:
    EraseLeafElem(ColumnBinary& column) TIGHTDB_NOEXCEPT:
        EraseHandlerBase(column) {}
    bool erase_leaf_elem(MemRef leaf_mem, ArrayParent* parent,
                         size_t leaf_ndx_in_parent,
                         size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        ArrayBinary leaf(leaf_mem, parent, leaf_ndx_in_parent, get_alloc());
        TIGHTDB_ASSERT(leaf.size() >= 1);
        size_t last_ndx = leaf.size() - 1;
        if (last_ndx == 0)
            return true;
        size_t ndx = elem_ndx_in_leaf;
        if (ndx == npos)
            ndx = last_ndx;
        leaf.erase(ndx); // Throws
        return false;
    }
    void destroy_leaf(MemRef leaf_mem) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        ArrayParent* parent = 0;
        size_t ndx_in_parent = 0;
        Array leaf(leaf_mem, parent, ndx_in_parent, get_alloc());
        leaf.destroy();
    }
    void replace_root_by_leaf(MemRef leaf_mem) TIGHTDB_OVERRIDE
    {
        ArrayParent* parent = 0;
        size_t ndx_in_parent = 0;
        UniquePtr<Array> leaf(new ArrayBinary(leaf_mem, parent, ndx_in_parent,
                                              get_alloc())); // Throws
        replace_root(leaf); // Throws
    }
    void replace_root_by_empty_leaf() TIGHTDB_OVERRIDE
    {
        ArrayParent* parent = 0;
        size_t ndx_in_parent = 0;
        UniquePtr<Array> leaf(new ArrayBinary(parent, ndx_in_parent,
                                              get_alloc())); // Throws
        replace_root(leaf); // Throws
    }
};

void ColumnBinary::erase(size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(ndx < size());
    TIGHTDB_ASSERT(is_last == (ndx == size()-1));

    if (m_array->is_leaf()) {
        static_cast<ArrayBinary*>(m_array)->erase(ndx); // Throws
        return;
    }

    size_t ndx_2 = is_last ? npos : ndx;
    EraseLeafElem erase_leaf_elem(*this);
    Array::erase_bptree_elem(m_array, ndx_2, erase_leaf_elem); // Throws
}

void ColumnBinary::resize(size_t ndx)
{
    TIGHTDB_ASSERT(root_is_leaf()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < size());
    static_cast<ArrayBinary*>(m_array)->resize(ndx);
}

void ColumnBinary::move_last_over(size_t ndx)
{
    // FIXME: ExceptionSafety: The current implementation of this
    // function is not exception-safe, and it is hard to see how to
    // repair it.

    // FIXME: Consider doing two nested calls to
    // update_bptree_elem(). If the two leafs are not the same, no
    // copying is needed. If they are the same, call
    // ArrayBinary::move_last_over() (does not yet
    // exist). ArrayBinary::move_last_over() could be implemented in a
    // way that avoids the intermediate copy. This approach is also
    // likely to be necesseray for exception safety.

    TIGHTDB_ASSERT(ndx+1 < size());

    size_t last_ndx = size() - 1;
    BinaryData value = get(last_ndx);

    // Copying binary data from a column to itself requires an
    // intermediate copy of the data (constr:bptree-copy-to-self).
    UniquePtr<char[]> buffer(new char[value.size()]);
    copy(value.data(), value.data()+value.size(), buffer.get());
    BinaryData copy_of_value(buffer.get(), value.size());

    set(ndx, copy_of_value);

    bool is_last = true;
    erase(last_ndx, is_last);
}

bool ColumnBinary::compare_binary(const ColumnBinary& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i != n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


void ColumnBinary::do_insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx == npos || ndx < size());
    ref_type new_sibling_ref;
    InsertState state;
    if (root_is_leaf()) {
        TIGHTDB_ASSERT(ndx == npos || ndx < TIGHTDB_MAX_LIST_SIZE);
        ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
        new_sibling_ref = leaf->bptree_leaf_insert(ndx, value, add_zero_term, state);
    }
    else {
        state.m_value = value;
        state.m_add_zero_term = add_zero_term;
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


ref_type ColumnBinary::leaf_insert(MemRef leaf_mem, ArrayParent& parent,
                                   size_t ndx_in_parent, Allocator& alloc,
                                   size_t insert_ndx,
                                   Array::TreeInsert<ColumnBinary>& state)
{
    ArrayBinary leaf(leaf_mem, &parent, ndx_in_parent, alloc);
    InsertState& state_2 = static_cast<InsertState&>(state);
    return leaf.bptree_leaf_insert(insert_ndx, state.m_value, state_2.m_add_zero_term, state);
}


#ifdef TIGHTDB_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    ArrayBinary leaf(mem, 0, 0, alloc);
    leaf.Verify();
    return leaf.size();
}

} // anonymous namespace

void ColumnBinary::Verify() const
{
    if (root_is_leaf()) {
        static_cast<ArrayBinary*>(m_array)->Verify();
        return;
    }

    m_array->verify_bptree(&verify_leaf);
}


void ColumnBinary::to_dot(ostream& out, StringData title) const
{
    ref_type ref = m_array->get_ref();
    out << "subgraph cluster_binary_column" << ref << " {" << endl;
    out << " label = \"Binary column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;
    tree_to_dot(out);
    out << "}" << endl;
}

void ColumnBinary::leaf_to_dot(MemRef leaf_mem, ArrayParent* parent, size_t ndx_in_parent,
                               ostream& out) const
{
    ArrayBinary leaf(leaf_mem.m_ref, parent, ndx_in_parent, m_array->get_alloc());
    leaf.to_dot(out);
}


namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, ostream& out, int level)
{
    ArrayBinary leaf(mem, 0, 0, alloc);
    int indent = level * 2;
    out << setw(indent) << "" << "Binary leaf (size: "<<leaf.size()<<")\n";
}

} // anonymous namespace

void ColumnBinary::dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // TIGHTDB_DEBUG
