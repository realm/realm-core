#include <stdint.h> // unint8_t etc
#include <cstdlib>
#include <cstring>
#include <climits>
#include <sstream>
#include <iostream>
#include <iomanip>

#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_table.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/query_engine.hpp>
#include <tightdb/exceptions.hpp>
#include <tightdb/table.hpp>
#include <tightdb/index_string.hpp>
#include <tightdb/array_integer.hpp>

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

void ColumnBase::set_string(size_t, StringData)
{
    throw LogicError(LogicError::type_mismatch);
}

void ColumnBase::update_from_parent(size_t old_baseline) REALM_NOEXCEPT
{
    m_array->update_from_parent(old_baseline);
}


void ColumnBase::cascade_break_backlinks_to(size_t, CascadeState&)
{
    // No-op by default
}


void ColumnBase::cascade_break_backlinks_to_all_rows(size_t, CascadeState&)
{
    // No-op by default
}


#ifdef REALM_DEBUG

void ColumnBase::Verify(const Table&, size_t) const
{
    Verify();
}

#endif // REALM_DEBUG


namespace {

struct GetSizeFromRef {
    const ref_type m_ref;
    Allocator& m_alloc;
    size_t m_size;
    GetSizeFromRef(ref_type r, Allocator& a): m_ref(r), m_alloc(a), m_size(0) {}
    template<class Col> void call() REALM_NOEXCEPT
    {
        m_size = Col::get_size_from_ref(m_ref, m_alloc);
    }
};

template<class Op> void col_type_deleg(Op& op, ColumnType type)
{
    switch (type) {
        case col_type_Int:
        case col_type_Bool:
        case col_type_DateTime:
        case col_type_Link:
            op.template call<Column>();
            return;
        case col_type_String:
            op.template call<AdaptiveStringColumn>();
            return;
        case col_type_StringEnum:
            op.template call<ColumnStringEnum>();
            return;
        case col_type_Binary:
            op.template call<ColumnBinary>();
            return;
        case col_type_Table:
            op.template call<ColumnTable>();
            return;
        case col_type_Mixed:
            op.template call<ColumnMixed>();
            return;
        case col_type_Float:
            op.template call<ColumnFloat>();
            return;
        case col_type_Double:
            op.template call<ColumnDouble>();
            return;
        case col_type_Reserved1:
        case col_type_Reserved4:
        case col_type_LinkList:
        case col_type_BackLink:
            break;
    }
    REALM_ASSERT_DEBUG(false);
}


class TreeWriter {
public:
    TreeWriter(_impl::OutputStream&) REALM_NOEXCEPT;
    ~TreeWriter() REALM_NOEXCEPT;

    void add_leaf_ref(ref_type child_ref, size_t elems_in_child, ref_type* is_last);

private:
    Allocator& m_alloc;
    _impl::OutputStream& m_out;
    class ParentLevel;
    UniquePtr<ParentLevel> m_last_parent_level;
};

class TreeWriter::ParentLevel {
public:
    ParentLevel(Allocator&, _impl::OutputStream&, size_t max_elems_per_child);
    ~ParentLevel() REALM_NOEXCEPT;

    void add_child_ref(ref_type child_ref, size_t elems_in_child,
                       bool leaf_or_compact, ref_type* is_last);

private:
    const size_t m_max_elems_per_child; // A power of `REALM_MAX_BPNODE_SIZE`
    size_t m_elems_in_parent; // Zero if reinitialization is needed
    bool m_is_on_general_form; // Defined only when m_elems_in_parent > 0
    Array m_main;
    ArrayInteger m_offsets;
    _impl::OutputStream& m_out;
    UniquePtr<ParentLevel> m_prev_parent_level;
};


inline TreeWriter::TreeWriter(_impl::OutputStream& out) REALM_NOEXCEPT:
    m_alloc(Allocator::get_default()),
    m_out(out)
{
}

inline TreeWriter::~TreeWriter() REALM_NOEXCEPT
{
}

void TreeWriter::add_leaf_ref(ref_type leaf_ref, size_t elems_in_leaf, ref_type* is_last)
{
    if (!m_last_parent_level) {
        if (is_last) {
            *is_last = leaf_ref;
            return;
        }
        m_last_parent_level.reset(new ParentLevel(m_alloc, m_out,
                                                  REALM_MAX_BPNODE_SIZE)); // Throws
    }
    bool leaf_or_compact = true;
    m_last_parent_level->add_child_ref(leaf_ref, elems_in_leaf,
                                       leaf_or_compact, is_last); // Throws
}


inline TreeWriter::ParentLevel::ParentLevel(Allocator& alloc, _impl::OutputStream& out,
                                            size_t max_elems_per_child):
    m_max_elems_per_child(max_elems_per_child),
    m_elems_in_parent(0),
    m_main(alloc),
    m_offsets(alloc),
    m_out(out)
{
    m_main.create(Array::type_InnerBptreeNode); // Throws
}

inline TreeWriter::ParentLevel::~ParentLevel() REALM_NOEXCEPT
{
    m_offsets.destroy(); // Shallow
    m_main.destroy(); // Shallow
}

void TreeWriter::ParentLevel::add_child_ref(ref_type child_ref, size_t elems_in_child,
                                            bool leaf_or_compact, ref_type* is_last)
{
    bool force_general_form = !leaf_or_compact ||
        (elems_in_child != m_max_elems_per_child &&
         m_main.size() != 1 + REALM_MAX_BPNODE_SIZE - 1 &&
         !is_last);

    // Add the incoming child to this inner node
    if (m_elems_in_parent > 0) { // This node contains children already
        if (!m_is_on_general_form && force_general_form) {
            if (!m_offsets.is_attached())
                m_offsets.create(Array::type_Normal); // Throws
            int_fast64_t v(m_max_elems_per_child); // FIXME: Dangerous cast (unsigned -> signed)
            size_t n = m_main.size();
            for (size_t i = 1; i != n; ++i)
                m_offsets.add(v); // Throws
            m_is_on_general_form = true;
        }
        {
            int_fast64_t v(child_ref); // FIXME: Dangerous cast (unsigned -> signed)
            m_main.add(v); // Throws
        }
        if (m_is_on_general_form) {
            int_fast64_t v(m_elems_in_parent); // FIXME: Dangerous cast (unsigned -> signed)
            m_offsets.add(v); // Throws
        }
        m_elems_in_parent += elems_in_child;
        if (!is_last && m_main.size() < 1 + REALM_MAX_BPNODE_SIZE)
          return;
    }
    else { // First child in this node
        m_main.add(0); // Placeholder for `elems_per_child` or `offsets_ref`
        int_fast64_t v(child_ref); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.add(v); // Throws
        m_elems_in_parent = elems_in_child;
        m_is_on_general_form = force_general_form; // `invar:bptree-node-form`
        if (m_is_on_general_form && !m_offsets.is_attached())
            m_offsets.create(Array::type_Normal); // Throws
        if (!is_last)
            return;
    }

    // No more children will be added to this node

    // Write this inner node to the output stream
    if (!m_is_on_general_form) {
        int_fast64_t v(m_max_elems_per_child); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.set(0, 1 + 2*v); // Throws
    }
    else {
        size_t pos = m_offsets.write(m_out); // Throws
        ref_type ref = pos;
        int_fast64_t v(ref); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.set(0, v); // Throws
    }
    {
        int_fast64_t v(m_elems_in_parent); // FIXME: Dangerous cast (unsigned -> signed)
        m_main.add(1 + 2*v); // Throws
    }
    bool recurse = false; // Shallow
    size_t pos = m_main.write(m_out, recurse); // Throws
    ref_type parent_ref = pos;

    // Whether the resulting ref must be added to the previous parent
    // level, or reported as the final ref (through `is_last`) depends
    // on whether more children are going to be added, and on whether
    // a previous parent level already exists
    if (!is_last) {
        if (!m_prev_parent_level) {
            Allocator& alloc = m_main.get_alloc();
            size_t next_level_elems_per_child = m_max_elems_per_child;
            if (int_multiply_with_overflow_detect(next_level_elems_per_child,
                                                  REALM_MAX_BPNODE_SIZE))
                throw runtime_error("Overflow in number of elements per child");
            m_prev_parent_level.reset(new ParentLevel(alloc, m_out,
                                                      next_level_elems_per_child)); // Throws
        }
    }
    else if (!m_prev_parent_level) {
        *is_last = parent_ref;
        return;
    }
    m_prev_parent_level->add_child_ref(parent_ref, m_elems_in_parent,
                                       !m_is_on_general_form, is_last); // Throws

    // Clear the arrays in preperation for the next child
    if (!is_last) {
        if (m_offsets.is_attached())
            m_offsets.clear(); // Shallow
        m_main.clear(); // Shallow
        m_elems_in_parent = 0;
    }
}

} // anonymous namespace



size_t ColumnBase::get_size_from_type_and_ref(ColumnType type, ref_type ref,
                                              Allocator& alloc) REALM_NOEXCEPT
{
    GetSizeFromRef op(ref, alloc);
    col_type_deleg(op, type);
    return op.m_size;
}


class ColumnBase::WriteSliceHandler: public Array::VisitHandler {
public:
    WriteSliceHandler(size_t offset, size_t size, Allocator& alloc,
                      ColumnBase::SliceHandler &slice_handler,
                      _impl::OutputStream& out) REALM_NOEXCEPT:
        m_begin(offset), m_end(offset + size),
        m_leaf_cache(alloc),
        m_slice_handler(slice_handler),
        m_out(out),
        m_tree_writer(out),
        m_top_ref(0)
    {
    }
    ~WriteSliceHandler() REALM_NOEXCEPT
    {
    }
    bool visit(const Array::NodeInfo& leaf_info) REALM_OVERRIDE
    {
        size_t size = leaf_info.m_size, pos;
        size_t leaf_begin = leaf_info.m_offset;
        size_t leaf_end   = leaf_begin + size;
        REALM_ASSERT_3(leaf_begin, <=, m_end);
        REALM_ASSERT_3(leaf_end, >=, m_begin);
        bool no_slicing = leaf_begin >= m_begin && leaf_end <= m_end;
        if (no_slicing) {
            m_leaf_cache.init_from_mem(leaf_info.m_mem);
            pos = m_leaf_cache.write(m_out); // Throws
        }
        else {
            // Slice the leaf
            Allocator& slice_alloc = Allocator::get_default();
            size_t begin = max(leaf_begin, m_begin);
            size_t end   = min(leaf_end,   m_end);
            size_t offset = begin - leaf_begin;
            size = end - begin;
            MemRef mem =
                m_slice_handler.slice_leaf(leaf_info.m_mem, offset, size, slice_alloc); // Throws
            Array slice(slice_alloc);
            _impl::DeepArrayDestroyGuard dg(&slice);
            slice.init_from_mem(mem);
            pos = slice.write(m_out); // Throws
        }
        ref_type ref = pos;
        ref_type* is_last = 0;
        if (leaf_end >= m_end)
            is_last = &m_top_ref;
        m_tree_writer.add_leaf_ref(ref, size, is_last); // Throws
        return !is_last;
    }
    ref_type get_top_ref() const REALM_NOEXCEPT
    {
        return m_top_ref;
    }
private:
    size_t m_begin, m_end;
    Array m_leaf_cache;
    ColumnBase::SliceHandler& m_slice_handler;
    _impl::OutputStream& m_out;
    TreeWriter m_tree_writer;
    ref_type m_top_ref;
};


ref_type ColumnBase::write(const Array* root, size_t slice_offset, size_t slice_size,
                           size_t table_size, SliceHandler& handler, _impl::OutputStream& out)
{
    REALM_ASSERT(root->is_inner_bptree_node());

    size_t offset = slice_offset;
    if (slice_size == 0)
        offset = 0;
    // At this point we know that `offset` refers to an element that
    // exists in the tree (this is required by
    // Array::visit_bptree_leaves()). There are two cases to consider:
    // First, if `slice_size` is non-zero, then `offset` must already
    // refer to an existsing element. If `slice_size` is zero, then
    // `offset` has been set to zero at this point. Zero is the index
    // of an existing element, because the tree cannot be empty at
    // this point. This follows from the fact that the root is an
    // inner node, and that an inner node must contain at least one
    // element (invar:bptree-nonempty-inner +
    // invar:bptree-nonempty-leaf).
    WriteSliceHandler handler_2(offset, slice_size, root->get_alloc(), handler, out);
    const_cast<Array*>(root)->visit_bptree_leaves(offset, table_size, handler_2); // Throws
    return handler_2.get_top_ref();
}


void ColumnBase::introduce_new_root(ref_type new_sibling_ref, Array::TreeInsertBase& state,
                                    bool is_append)
{
    // At this point the original root and its new sibling is either
    // both leaves, or both inner nodes on the same form, compact or
    // general. Due to invar:bptree-node-form, the new root is allowed
    // to be on the compact form if is_append is true and both
    // siblings are either leaves or inner nodes on the compact form.

    Array* orig_root = m_array;
    Allocator& alloc = orig_root->get_alloc();
    UniquePtr<Array> new_root(new Array(alloc)); // Throws
    new_root->create(Array::type_InnerBptreeNode); // Throws
    new_root->set_parent(orig_root->get_parent(), orig_root->get_ndx_in_parent());
    new_root->update_parent(); // Throws
    bool compact_form =
        is_append && (!orig_root->is_inner_bptree_node() || orig_root->get(0) % 2 != 0);
    // Something is wrong if we were not appending and the original
    // root is still on the compact form.
    REALM_ASSERT(!compact_form || is_append);
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


ref_type ColumnBase::build(size_t* rest_size_ptr, size_t fixed_height,
                           Allocator& alloc, CreateHandler& handler)
{
    size_t rest_size = *rest_size_ptr;
    size_t orig_rest_size = rest_size;
    size_t leaf_size = min(size_t(REALM_MAX_BPNODE_SIZE), rest_size);
    rest_size -= leaf_size;
    ref_type node = handler.create_leaf(leaf_size);
    size_t height = 1;
    try {
        for (;;) {
            if (fixed_height > 0 ? fixed_height == height : rest_size == 0) {
                *rest_size_ptr = rest_size;
                return node;
            }
            Array new_inner_node(alloc);
            new_inner_node.create(Array::type_InnerBptreeNode); // Throws
            try {
                int_fast64_t v = orig_rest_size - rest_size; // elems_per_child
                new_inner_node.add(1 + 2*v); // Throws
                v = node; // FIXME: Dangerous cast here (unsigned -> signed)
                new_inner_node.add(v); // Throws
                node = 0;
                size_t num_children = 1;
                while (rest_size > 0 && num_children != REALM_MAX_BPNODE_SIZE) {
                    ref_type child = build(&rest_size, height, alloc, handler); // Throws
                    try {
                        int_fast64_t v = child; // FIXME: Dangerous cast here (unsigned -> signed)
                        new_inner_node.add(v); // Throws
                    }
                    catch (...) {
                        Array::destroy_deep(child, alloc);
                        throw;
                    }
                }
                v = orig_rest_size - rest_size; // total_elems_in_tree
                new_inner_node.add(1 + 2*v); // Throws
            }
            catch (...) {
                new_inner_node.destroy_deep();
                throw;
            }
            node = new_inner_node.get_ref();
            ++height;
        }
    }
    catch (...) {
        if (node != 0)
            Array::destroy_deep(node, alloc);
        throw;
    }
}

void Column::destroy() REALM_NOEXCEPT
{
    if (m_search_index) {
        static_cast<StringIndex*>(m_search_index)->destroy();
    }

    if (m_array)
        m_array->destroy_deep();
}

Column::~Column() REALM_NOEXCEPT
{
    if (m_search_index) {
        //static_cast<StringIndex*>(m_search_index)->destroy();
        delete static_cast<StringIndex*>(m_search_index);
        m_search_index = null_ptr;
    }
    delete m_array;
}


void Column::move_assign(Column& col)
{
    destroy();
    delete m_array;
    m_array = col.m_array;
    col.m_array = 0;
    m_search_index = col.m_search_index;
    col.m_search_index = null_ptr;
}

void Column::update_from_parent(size_t old_baseline) REALM_NOEXCEPT
{
    m_array->update_from_parent(old_baseline);

    if (m_search_index)
        m_search_index->update_from_parent(old_baseline);
}

namespace {

struct SetLeafElem: Array::UpdateHandler {
    Array m_leaf;
    const int_fast64_t m_value;
    SetLeafElem(Allocator& alloc, int_fast64_t value) REALM_NOEXCEPT:
        m_leaf(alloc), m_value(value) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                size_t elem_ndx_in_leaf) REALM_OVERRIDE
    {
        m_leaf.init_from_mem(mem);
        m_leaf.set_parent(parent, ndx_in_parent);
        m_leaf.set(elem_ndx_in_leaf, m_value); // Throws
    }
};

struct AdjustLeafElem: Array::UpdateHandler {
    Array m_leaf;
    const int_fast64_t m_value;
    AdjustLeafElem(Allocator& alloc, int_fast64_t value) REALM_NOEXCEPT:
    m_leaf(alloc), m_value(value) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                size_t elem_ndx_in_leaf) REALM_OVERRIDE
    {
        m_leaf.init_from_mem(mem);
        m_leaf.set_parent(parent, ndx_in_parent);
        m_leaf.adjust(elem_ndx_in_leaf, m_value); // Throws
    }
};

} // anonymous namespace

void Column::set(size_t ndx, int64_t value)
{
    REALM_ASSERT_DEBUG(ndx < size());

    if (m_search_index) {
        static_cast<StringIndex*>(m_search_index)->set(ndx, value);
    }

    if (!m_array->is_inner_bptree_node()) {
        array()->set(ndx, value); // Throws
        return;
    }

    SetLeafElem set_leaf_elem(m_array->get_alloc(), value);
    array()->update_bptree_elem(ndx, set_leaf_elem); // Throws
}

// When a value of a signed type is converted to an unsigned type, the C++ standard guarantees that negative values 
// are converted from the native representation to 2's complement, but the opposite conversion is left as undefined. 
// tightdb::util::from_twos_compl() is used here to perform the correct opposite unsigned-to-signed conversion,
// which reduces to a no-op when 2's complement is the native representation of negative values.
void Column::set_uint(size_t ndx, uint64_t value)
{
    set(ndx, from_twos_compl<int_fast64_t>(value));
}

void Column::set_as_ref(size_t ndx, ref_type ref)
{
    set(ndx, from_ref(ref));
}

void Column::adjust(size_t ndx, int_fast64_t diff)
{
    REALM_ASSERT_3(ndx, <, size());

    if (!m_array->is_inner_bptree_node()) {
        array()->adjust(ndx, diff); // Throws
        return;
    }

    AdjustLeafElem set_leaf_elem(array()->get_alloc(), diff);
    array()->update_bptree_elem(ndx, set_leaf_elem); // Throws
}


// int64_t specific:

size_t Column::count(int64_t target) const
{
    if (m_search_index)
        return static_cast<StringIndex*>(m_search_index)->count(target);

    return size_t(aggregate<int64_t, int64_t, act_Count, Equal>(target, 0, size()));
}

int64_t Column::sum(size_t start, size_t end, size_t limit, size_t* return_ndx) const
{
    return aggregate<int64_t, int64_t, act_Sum, None>(0, start, end, limit, return_ndx);
}

double Column::average(size_t start, size_t end, size_t limit, size_t* return_ndx) const
{
    if (end == size_t(-1))
        end = size();
    size_t size = end - start;
    if(limit < size)
        size = limit;
    int64_t sum = aggregate<int64_t, int64_t, act_Sum, None>(0, start, end, limit, return_ndx);
    double avg = double(sum) / double(size == 0 ? 1 : size);
    return avg;
}

int64_t Column::minimum(size_t start, size_t end, size_t limit, size_t* return_ndx) const
{
    return aggregate<int64_t, int64_t, act_Min, None>(0, start, end, limit, return_ndx);
}

int64_t Column::maximum(size_t start, size_t end, size_t limit, size_t* return_ndx) const
{
    return aggregate<int64_t, int64_t, act_Max, None>(0, start, end, limit, return_ndx);
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


void Column::destroy_subtree(size_t ndx, bool clear_value)
{
    int_fast64_t value = get(ndx);

    // Null-refs indicate empty subtrees
    if (value == 0)
        return;

    // A ref is always 8-byte aligned, so the lowest bit
    // cannot be set. If it is, it means that it should not be
    // interpreted as a ref.
    if (value % 2 != 0)
        return;

    // Delete subtree
    ref_type ref = to_ref(value);
    Allocator& alloc = get_alloc();
    Array::destroy_deep(ref, alloc);

    if (clear_value)
        set(ndx, 0); // Throws
}


namespace {

template<bool with_limit> struct AdjustHandler: Array::UpdateHandler {
    ArrayInteger m_leaf;
    const int_fast64_t m_limit, m_diff;
    AdjustHandler(Allocator& alloc, int_fast64_t limit, int_fast64_t diff) REALM_NOEXCEPT:
        m_leaf(alloc), m_limit(limit), m_diff(diff) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent, size_t) REALM_OVERRIDE
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
    if (!array()->is_inner_bptree_node()) {
        array()->adjust(0, m_array->size(), diff); // Throws
        return;
    }

    const bool with_limit = false;
    int_fast64_t dummy_limit = 0;
    AdjustHandler<with_limit> leaf_handler(array()->get_alloc(), dummy_limit, diff);
    array()->update_bptree_leaves(leaf_handler); // Throws
}


void Column::adjust_ge(int_fast64_t limit, int_fast64_t diff)
{
    if (!m_array->is_inner_bptree_node()) {
        array()->adjust_ge(limit, diff); // Throws
        return;
    }

    const bool with_limit = true;
    AdjustHandler<with_limit> leaf_handler(array()->get_alloc(), limit, diff);
    array()->update_bptree_leaves(leaf_handler); // Throws
}

namespace {

    // Getter function for index. For integer index, the caller must supply a buffer that we can store the 
    // extracted value in (it may be bitpacked, so we cannot return a pointer in to the Array as we do with 
    // String index).
    StringData get_string(void* column, size_t ndx, char* buffer)
    {
        int64_t i = static_cast<Column*>(column)->get(ndx);
        *reinterpret_cast<int64_t*>(buffer) = i;
        StringData s = to_str(*reinterpret_cast<int64_t*>(buffer));
        return s;
    }

} // anonymous namespace

StringIndex* Column::create_search_index()
{
    REALM_ASSERT(!m_search_index);
    UniquePtr<StringIndex> index;
    StringIndex* si = new StringIndex(this, &get_string, m_array->get_alloc()); // Throws
    index.reset(si);

    // Populate the index
    size_t num_rows = size();
    for (size_t row_ndx = 0; row_ndx != num_rows; ++row_ndx) {
        int64_t value = get(row_ndx);
        size_t num_rows = 1;
        bool is_append = true;
        index.get()->insert(row_ndx, value, num_rows, is_append); // Throws
    }

    m_search_index = index.release();
    return m_search_index;
}

StringIndex* Column::get_search_index() REALM_NOEXCEPT
{
    return m_search_index;
}

const StringIndex* Column::get_search_index() const REALM_NOEXCEPT
{
    return m_search_index;
}

void Column::destroy_search_index() REALM_NOEXCEPT
{
    delete m_search_index;
    m_search_index = 0;
}

void Column::set_search_index_ref(ref_type ref, ArrayParent* parent,
    size_t ndx_in_parent, bool allow_duplicate_valaues)
{
    REALM_ASSERT(!m_search_index);
    m_search_index = new StringIndex(ref, parent, ndx_in_parent, this, &get_string,
        !allow_duplicate_valaues, m_array->get_alloc()); // Throws
}

size_t Column::find_first(int64_t value, size_t begin, size_t end) const
{
    REALM_ASSERT_3(begin, <=, size());
    REALM_ASSERT(end == npos || (begin <= end && end <= size()));

    if (m_search_index && begin == 0 && end == npos)
        return static_cast<StringIndex*>(m_search_index)->find_first(value);

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


void Column::find_all(Column& result, int64_t value, size_t begin, size_t end) const
{
    REALM_ASSERT_3(begin, <=, size());
    REALM_ASSERT(end == npos || (begin <= end && end <= size()));

    if (m_search_index && begin == 0 && end == size_t(-1))
        return static_cast<StringIndex*>(m_search_index)->find_all(result, value);

    if (root_is_leaf()) {
        size_t leaf_offset = 0;
        m_array->find_all(&result, value, leaf_offset, begin, end); // Throws
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
        leaf.find_all(&result, value, leaf_offset, ndx_in_leaf, end_in_leaf); // Throws
        ndx_in_tree = leaf_offset + end_in_leaf;
    }
}


bool Column::compare_int(const Column& c) const REALM_NOEXCEPT
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


void Column::do_insert(size_t row_ndx, int_fast64_t value, size_t num_rows)
{
    REALM_ASSERT_DEBUG(row_ndx == tightdb::npos || row_ndx < size());

    ref_type new_sibling_ref;
    Array::TreeInsert<Column> state;
    for (size_t i = 0; i != num_rows; ++i) {
        size_t row_ndx_2 = row_ndx == tightdb::npos ? tightdb::npos : row_ndx + i;
        if (root_is_leaf()) {
            REALM_ASSERT_DEBUG(row_ndx_2 == tightdb::npos || row_ndx_2 < REALM_MAX_BPNODE_SIZE);
            new_sibling_ref = m_array->bptree_leaf_insert(row_ndx_2, value, state); // Throws
        }
        else {
            state.m_value = value;
            if (row_ndx_2 == tightdb::npos) {
                new_sibling_ref = m_array->bptree_append(state); // Throws
            }
            else {
                new_sibling_ref = m_array->bptree_insert(row_ndx_2, state); // Throws
            }
        }
        if (REALM_UNLIKELY(new_sibling_ref)) {
            bool is_append = row_ndx_2 == tightdb::npos;
            introduce_new_root(new_sibling_ref, state, is_append); // Throws
        }
    }


    if (m_search_index) {
        bool is_append = row_ndx == tightdb::npos;
        size_t row_ndx_2 = is_append ? size() - num_rows : row_ndx;
        static_cast<StringIndex*>(m_search_index)->insert(row_ndx_2, value, num_rows, is_append); // Throws
    }

}


class Column::EraseLeafElem: public EraseHandlerBase {
public:
    Array m_leaf;
    bool m_leaves_have_refs;
    EraseLeafElem(Column& column) REALM_NOEXCEPT:
        EraseHandlerBase(column), m_leaf(get_alloc()),
        m_leaves_have_refs(false) {}
    bool erase_leaf_elem(MemRef leaf_mem, ArrayParent* parent,
                         size_t leaf_ndx_in_parent,
                         size_t elem_ndx_in_leaf) REALM_OVERRIDE
    {
        m_leaf.init_from_mem(leaf_mem);
        REALM_ASSERT_3(m_leaf.size(), >=, 1);
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
    void destroy_leaf(MemRef leaf_mem) REALM_NOEXCEPT REALM_OVERRIDE
    {
        // FIXME: Seems like this would cause file space leaks if
        // m_leaves_have_refs is true, but consider carefully how
        // m_leaves_have_refs get its value.
        get_alloc().free_(leaf_mem);
    }
    void replace_root_by_leaf(MemRef leaf_mem) REALM_OVERRIDE
    {
        Array* leaf = new Array(get_alloc()); // Throws
        leaf->init_from_mem(leaf_mem);
        replace_root(leaf); // Throws, but callee takes ownership of accessor
    }
    void replace_root_by_empty_leaf() REALM_OVERRIDE
    {
        UniquePtr<Array> leaf(new Array(get_alloc())); // Throws
        leaf->create(m_leaves_have_refs ? Array::type_HasRefs :
                     Array::type_Normal); // Throws
        replace_root(leaf.release()); // Throws, but callee takes ownership of accessor
    }
};


void Column::do_erase(size_t ndx, bool is_last)
{
    REALM_ASSERT_DEBUG(ndx < size());
    REALM_ASSERT_DEBUG(is_last == (ndx == size()-1));

    if (m_search_index)
        static_cast<StringIndex*>(m_search_index)->erase<StringData>(ndx, is_last);

    if (!m_array->is_inner_bptree_node()) {
        m_array->erase(ndx); // Throws
        return;
    }

    size_t ndx_2 = is_last ? npos : ndx;
    EraseLeafElem handler(*this);
    Array::erase_bptree_elem(m_array, ndx_2, handler); // Throws
}


void Column::do_move_last_over(size_t row_ndx, size_t last_row_ndx)
{
    REALM_ASSERT_3(row_ndx, <=, last_row_ndx);
    REALM_ASSERT_DEBUG(last_row_ndx + 1 == size());

    if (m_search_index) {
        // remove the value to be overwritten from index
        bool is_last = true; // This tells StringIndex::erase() to not adjust subsequent indexes
        static_cast<StringIndex*>(m_search_index)->erase<StringData>(row_ndx, is_last); // Throws

        // update index to point to new location
        if (row_ndx != last_row_ndx) {
            int_fast64_t moved_value = get(last_row_ndx);
            static_cast<StringIndex*>(m_search_index)->update_ref(moved_value, last_row_ndx, row_ndx); // Throws
        }
    }

    // Copy value from last row over
    int_fast64_t value = get(last_row_ndx);
    if (array()->is_inner_bptree_node()) {
        SetLeafElem set_leaf_elem(array()->get_alloc(), value);
        array()->update_bptree_elem(row_ndx, set_leaf_elem); // Throws
    }
    else {
        array()->set(row_ndx, value); // Throws
    }

    // Discard last row
    if (array()->is_inner_bptree_node()) {
        size_t row_ndx_2 = tightdb::npos;
        EraseLeafElem handler(*this);
        Array::erase_bptree_elem(array(), row_ndx_2, handler); // Throws
    }
    else {
        array()->erase(last_row_ndx); // Throws
    }
}


void Column::do_clear()
{
    if (m_search_index)
        static_cast<StringIndex*>(m_search_index)->clear();

    m_array->clear_and_destroy_children();
    if (m_array->is_inner_bptree_node())
        m_array->set_type(Array::type_Normal);
}


class Column::CreateHandler: public ColumnBase::CreateHandler {
public:
    CreateHandler(Array::Type leaf_type, int_fast64_t value, Allocator& alloc):
        m_leaf_type(leaf_type), m_value(value), m_alloc(alloc) {}
    ref_type create_leaf(size_t size) REALM_OVERRIDE
    {
        bool context_flag = false;
        MemRef mem = ArrayInteger::create_array(m_leaf_type, context_flag, size,
                                         m_value, m_alloc); // Throws
        return mem.m_ref;
    }
private:
    const Array::Type m_leaf_type;
    const int_fast64_t m_value;
    Allocator& m_alloc;
};

ref_type Column::create(Allocator& alloc, Array::Type leaf_type, size_t size, int_fast64_t value)
{
    CreateHandler handler(leaf_type, value, alloc);
    return ColumnBase::create(alloc, size, handler);
}


class Column::SliceHandler: public ColumnBase::SliceHandler {
public:
    SliceHandler(Allocator& alloc): m_leaf(alloc) {}
    MemRef slice_leaf(MemRef leaf_mem, size_t offset, size_t size,
                      Allocator& target_alloc) REALM_OVERRIDE
    {
        m_leaf.init_from_mem(leaf_mem);
        return m_leaf.slice_and_clone_children(offset, size, target_alloc); // Throws
    }
private:
    Array m_leaf;
};

ref_type Column::write(size_t slice_offset, size_t slice_size,
                       size_t table_size, _impl::OutputStream& out) const
{
    ref_type ref;
    if (root_is_leaf()) {
        Allocator& alloc = Allocator::get_default();
        MemRef mem = m_array->slice_and_clone_children(slice_offset, slice_size, alloc); // Throws
        Array slice(alloc);
        _impl::DeepArrayDestroyGuard dg(&slice);
        slice.init_from_mem(mem);
        bool recurse = true;
        size_t pos = slice.write(out, recurse); // Throws
        ref = pos;
    }
    else {
        SliceHandler handler(get_alloc());
        ref = ColumnBase::write(m_array, slice_offset, slice_size,
                                table_size, handler, out); // Throws
    }
    return ref;
}


void Column::refresh_accessor_tree(size_t, const Spec&)
{
    // With this type of column (Column), `m_array` is always an instance of
    // Array. This is true because all leafs are instances of Array, and when
    // the root is an inner B+-tree node, only the top array of the inner node
    // is cached. This means that we never have to change the type of the cached
    // root array.
    m_array->init_from_parent();
}


#ifdef REALM_DEBUG

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
                ostream& out) REALM_OVERRIDE
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
    do_dump_node_structure(cerr, 0);
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

void Column::do_dump_node_structure(ostream& out, int level) const
{
    dump_node_structure(*m_array, out, level);
}

void Column::dump_node_structure(const Array& root,  ostream& out, int level)
{
    root.dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // REALM_DEBUG
