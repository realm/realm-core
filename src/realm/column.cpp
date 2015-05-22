#include <stdint.h> // unint8_t etc
#include <cstdlib>
#include <cstring>
#include <climits>
#include <sstream>
#include <iostream>
#include <iomanip>

#include <realm/column.hpp>
#include <realm/column_table.hpp>
#include <realm/column_mixed.hpp>
#include <realm/query_engine.hpp>
#include <realm/exceptions.hpp>
#include <realm/table.hpp>
#include <realm/index_string.hpp>
#include <realm/array_integer.hpp>

using namespace std;
using namespace realm;
using namespace realm::util;

void ColumnBase::move_assign(ColumnBase&) REALM_NOEXCEPT
{
    destroy();
}

void ColumnBaseWithIndex::move_assign(ColumnBaseWithIndex& col) REALM_NOEXCEPT
{
    ColumnBase::move_assign(col);
    m_search_index = std::move(col.m_search_index);
}

void ColumnBase::set_string(size_t, StringData)
{
    throw LogicError(LogicError::type_mismatch);
}

void ColumnBaseWithIndex::update_from_parent(size_t old_baseline) REALM_NOEXCEPT
{
    if (m_search_index) {
        m_search_index->update_from_parent(old_baseline);
    }
}


void ColumnBase::cascade_break_backlinks_to(size_t, CascadeState&)
{
    // No-op by default
}


void ColumnBase::cascade_break_backlinks_to_all_rows(size_t, CascadeState&)
{
    // No-op by default
}

void ColumnBaseWithIndex::destroy() REALM_NOEXCEPT
{
    if (m_search_index) {
        m_search_index->destroy();
    }
}


#ifdef REALM_DEBUG

void ColumnBase::Verify(const Table&, size_t) const
{
    Verify();
}

#endif // REALM_DEBUG

void ColumnBaseSimple::replace_root_array(std::unique_ptr<Array> leaf)
{
    // FIXME: Duplicated from bptree.cpp.
    ArrayParent* parent = m_array->get_parent();
    std::size_t ndx_in_parent = m_array->get_ndx_in_parent();
    leaf->set_parent(parent, ndx_in_parent);
    leaf->update_parent(); // Throws
    m_array = std::move(leaf);
}


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
    std::unique_ptr<ParentLevel> m_last_parent_level;
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
    std::unique_ptr<ParentLevel> m_prev_parent_level;
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
    bool visit(const Array::NodeInfo& leaf_info) override
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


ref_type ColumnBaseSimple::write(const Array* root, size_t slice_offset, size_t slice_size,
                           size_t table_size, SliceHandler& handler, _impl::OutputStream& out)
{
    return BpTreeBase::write_subtree(*root, slice_offset, slice_size, table_size, handler, out);
}


void ColumnBaseSimple::introduce_new_root(ref_type new_sibling_ref, Array::TreeInsertBase& state,
                                    bool is_append)
{
    // At this point the original root and its new sibling is either
    // both leaves, or both inner nodes on the same form, compact or
    // general. Due to invar:bptree-node-form, the new root is allowed
    // to be on the compact form if is_append is true and both
    // siblings are either leaves or inner nodes on the compact form.

    Array* orig_root = get_root_array();
    Allocator& alloc = get_alloc();
    std::unique_ptr<Array> new_root(new Array(alloc)); // Throws
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
    replace_root_array(std::move(new_root));
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


void ColumnBaseWithIndex::destroy_search_index() REALM_NOEXCEPT
{
    m_search_index.reset();
}

void ColumnBaseWithIndex::set_search_index_ref(ref_type ref, ArrayParent* parent,
    size_t ndx_in_parent, bool allow_duplicate_valaues)
{
    REALM_ASSERT(!m_search_index);
    m_search_index.reset(new StringIndex(ref, parent, ndx_in_parent, this,
        !allow_duplicate_valaues, get_alloc())); // Throws
}


#ifdef REALM_DEBUG

class ColumnBase::LeafToDot: public Array::ToDotHandler {
public:
    const ColumnBase& m_column;
    LeafToDot(const ColumnBase& column): m_column(column) {}
    void to_dot(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                ostream& out) override
    {
        m_column.leaf_to_dot(mem, parent, ndx_in_parent, out);
    }
};

void ColumnBaseSimple::tree_to_dot(ostream& out) const
{
    ColumnBase::bptree_to_dot(get_root_array(), out);
}

void ColumnBase::bptree_to_dot(const Array* root, ostream& out) const
{
    LeafToDot handler(*this);
    root->bptree_to_dot(out, handler);
}

void ColumnBase::dump_node_structure() const
{
    do_dump_node_structure(cerr, 0);
}

namespace realm {
namespace _impl {

void leaf_dumper(MemRef mem, Allocator& alloc, std::ostream& out, int level)
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

} // namespace _impl
} // namespace realm

#endif // REALM_DEBUG
