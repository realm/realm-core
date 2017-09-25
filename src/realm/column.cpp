/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <cstdint> // unint8_t etc
#include <cstdlib>
#include <cstring>
#include <climits>

#ifdef REALM_DEBUG
#include <iostream>
#include <iomanip>
#include <sstream>
#endif

#include <realm/column.hpp>
#include <realm/column_table.hpp>
#include <realm/column_mixed.hpp>
#include <realm/query_engine.hpp>
#include <realm/exceptions.hpp>
#include <realm/table.hpp>
#include <realm/index_string.hpp>
#include <realm/array_integer.hpp>

using namespace realm;
using namespace realm::util;

TableRef ColumnBase::get_subtable_accessor(size_t) const noexcept
{
    return {};
}


bool ColumnBase::is_nullable() const noexcept
{
    return false;
}

bool ColumnBase::is_null(size_t) const noexcept
{
    return false;
}

void ColumnBase::set_null(size_t)
{
    throw LogicError{LogicError::column_not_nullable};
}

void ColumnBase::move_assign(ColumnBase&) noexcept
{
    destroy();
}

void ColumnBase::refresh_accessor_tree(size_t new_col_ndx, const realm::Spec&)
{
    m_column_ndx = new_col_ndx;
}

void ColumnBaseWithIndex::move_assign(ColumnBaseWithIndex& col) noexcept
{
    ColumnBase::move_assign(col);
    m_search_index = std::move(col.m_search_index);
}

void ColumnBase::set_string(size_t, StringData)
{
    throw LogicError(LogicError::type_mismatch);
}

void ColumnBaseWithIndex::set_ndx_in_parent(size_t ndx) noexcept
{
    if (m_search_index) {
        m_search_index->set_ndx_in_parent(ndx + 1);
    }
}

void ColumnBaseWithIndex::update_from_parent(size_t old_baseline) noexcept
{
    if (m_search_index) {
        m_search_index->update_from_parent(old_baseline);
    }
}

void ColumnBaseWithIndex::refresh_accessor_tree(size_t new_col_ndx, const realm::Spec& spec)
{
    ColumnBase::refresh_accessor_tree(new_col_ndx, spec);
    if (m_search_index) {
        m_search_index->refresh_accessor_tree(new_col_ndx, spec);
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

void ColumnBaseWithIndex::destroy() noexcept
{
    if (m_search_index) {
        m_search_index->destroy();
    }
}

void ColumnBase::verify(const Table&, size_t column_ndx) const
{
    verify();
    REALM_ASSERT_EX(column_ndx == m_column_ndx, column_ndx, m_column_ndx);
}

void ColumnBaseSimple::replace_root_array(std::unique_ptr<Array> leaf)
{
    // FIXME: Duplicated from bptree.cpp.
    ArrayParent* parent = m_array->get_parent();
    size_t ndx_in_parent = m_array->get_ndx_in_parent();
    leaf->set_parent(parent, ndx_in_parent);
    leaf->update_parent(); // Throws
    m_array = std::move(leaf);
}


namespace {

struct GetSizeFromRef {
    const ref_type m_ref;
    Allocator& m_alloc;
    size_t m_size;
    GetSizeFromRef(ref_type r, Allocator& a)
        : m_ref(r)
        , m_alloc(a)
        , m_size(0)
    {
    }

    template <class Col>
    void call() noexcept
    {
        m_size = Col::get_size_from_ref(m_ref, m_alloc);
    }
};

template <class Op>
void col_type_deleg(Op& op, ColumnType type, bool nullable)
{
    switch (type) {
        case col_type_Int:
        case col_type_Bool:
        case col_type_OldDateTime:
            if (nullable)
                op.template call<IntNullColumn>();
            else
                op.template call<IntegerColumn>();
            return;
        case col_type_Link:
            op.template call<IntegerColumn>();
            return;
        case col_type_Timestamp:
            op.template call<TimestampColumn>();
            return;
        case col_type_String:
            op.template call<StringColumn>();
            return;
        case col_type_StringEnum:
            op.template call<StringEnumColumn>();
            return;
        case col_type_Binary:
            op.template call<BinaryColumn>();
            return;
        case col_type_Table:
            op.template call<SubtableColumn>();
            return;
        case col_type_Mixed:
            op.template call<MixedColumn>();
            return;
        case col_type_Float:
            op.template call<FloatColumn>();
            return;
        case col_type_Double:
            op.template call<DoubleColumn>();
            return;
        case col_type_LinkList:
            op.template call<LinkListColumn>();
            return;
        case col_type_BackLink:
            op.template call<BacklinkColumn>();
            return;
        case col_type_Reserved4:
            break;
    }
    REALM_ASSERT_DEBUG(false);
}

} // anonymous namespace


size_t ColumnBase::get_size_from_type_and_ref(ColumnType type, ref_type ref, Allocator& alloc, bool nullable) noexcept
{
    GetSizeFromRef op(ref, alloc);
    col_type_deleg(op, type, nullable);
    return op.m_size;
}


ref_type ColumnBaseSimple::write(const Array* root, size_t slice_offset, size_t slice_size, size_t table_size,
                                 SliceHandler& handler, _impl::OutputStream& out)
{
    REALM_ASSERT(root->is_inner_bptree_node());
    return BpTreeBase::write_subtree(static_cast<const BpTreeNode&>(*root), slice_offset, slice_size, table_size,
                                     handler, out);
}


void ColumnBaseSimple::introduce_new_root(ref_type new_sibling_ref, TreeInsertBase& state, bool is_append)
{
    // At this point the original root and its new sibling is either
    // both leaves, or both inner nodes on the same form, compact or
    // general. Due to invar:bptree-node-form, the new root is allowed
    // to be on the compact form if is_append is true and both
    // siblings are either leaves or inner nodes on the compact form.

    Array* orig_root = get_root_array();
    Allocator& alloc = get_alloc();
    std::unique_ptr<Array> new_root(new BpTreeNode(alloc)); // Throws
    new_root->create(Array::type_InnerBptreeNode);          // Throws
    new_root->set_parent(orig_root->get_parent(), orig_root->get_ndx_in_parent());
    new_root->update_parent(); // Throws
    bool compact_form = is_append && (!orig_root->is_inner_bptree_node() || orig_root->get(0) % 2 != 0);
    // Something is wrong if we were not appending and the original
    // root is still on the compact form.
    REALM_ASSERT(!compact_form || is_append);
    if (compact_form) {
        int_fast64_t v = to_int64(state.m_split_offset); // elems_per_child
        new_root->add(1 + 2 * v);                        // Throws
    }
    else {
        Array new_offsets(alloc);
        new_offsets.create(Array::type_Normal);          // Throws
        new_offsets.add(to_int64(state.m_split_offset)); // Throws
        new_root->add(from_ref(new_offsets.get_ref()));  // Throws
    }
    new_root->add(from_ref(orig_root->get_ref())); // Throws
    new_root->add(from_ref(new_sibling_ref));      // Throws
    int_fast64_t v = to_int64(state.m_split_size); // total_elems_in_tree
    new_root->add(1 + 2 * v);                      // Throws
    replace_root_array(std::move(new_root));
}


ref_type ColumnBase::build(size_t* rest_size_ptr, size_t fixed_height, Allocator& alloc, CreateHandler& handler)
{
    size_t rest_size = *rest_size_ptr;
    size_t orig_rest_size = rest_size;
    size_t elems_per_child = REALM_MAX_BPNODE_SIZE;
    size_t leaf_size = std::min(elems_per_child, rest_size);
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
                int_fast64_t v = elems_per_child;
                new_inner_node.add(1 + 2 * v); // Throws
                v = from_ref(node);
                new_inner_node.add(v); // Throws
                node = 0;
                size_t num_children = 1;
                while (rest_size > 0 && num_children != REALM_MAX_BPNODE_SIZE) {
                    ref_type child = build(&rest_size, height, alloc, handler); // Throws
                    try {
                        int_fast64_t w = from_ref(child);
                        new_inner_node.add(w); // Throws
                    }
                    // LCOV_EXCL_START
                    catch (...) {
                        Array::destroy_deep(child, alloc);
                        throw;
                    }
                    // LCOV_EXCL_STOP
                    ++num_children;
                }
                v = orig_rest_size - rest_size; // total_elems_in_tree
                new_inner_node.add(1 + 2 * v);  // Throws
            }
            // LCOV_EXCL_START
            catch (...) {
                new_inner_node.destroy_deep();
                throw;
            }
            // LCOV_EXCL_STOP
            node = new_inner_node.get_ref();
            ++height;
            // Overflow is impossible here is all nodes will have elems_per_child <= orig_rest_size
            elems_per_child *= REALM_MAX_BPNODE_SIZE;
        }
    }
    // LCOV_EXCL_START
    catch (...) {
        if (node != 0)
            Array::destroy_deep(node, alloc);
        throw;
    }
    // LCOV_EXCL_STOP
}


/*
// TODO: Set owner of created arrays and destroy/delete them if created by merge_references()
void IntegerColumn::reference_sort(size_t start, size_t end, Column& ref)
{
    Array values; // pointers to non-instantiated arrays of values
    Array indexes; // pointers to instantiated arrays of index pointers
    Array all_values;
    TreeVisitLeafs<Array, IntegerColumn>(start, end, 0, callme_arrays, &values);

    size_t offset = 0;
    for (size_t t = 0; t < values.size(); t++) {
        Array* i = new Array();
        ref_type ref = values.get_as_ref(t);
        Array v(ref);
        for (size_t j = 0; j < v.size(); j++)
            all_values.add(v.get(j));
        v.reference_sort(*i);
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


void ColumnBaseWithIndex::destroy_search_index() noexcept
{
    m_search_index.reset();
}

void ColumnBaseWithIndex::set_search_index_ref(ref_type ref, ArrayParent* parent, size_t ndx_in_parent)
{
    REALM_ASSERT(!m_search_index);
    m_search_index.reset(new StringIndex(ref, parent, ndx_in_parent, this, get_alloc())); // Throws
}


#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug functions

class ColumnBase::LeafToDot : public Array::ToDotHandler {
public:
    const ColumnBase& m_column;
    LeafToDot(const ColumnBase& column)
        : m_column(column)
    {
    }
    void to_dot(MemRef mem, ArrayParent* parent, size_t ndx_in_parent, std::ostream& out) override
    {
        m_column.leaf_to_dot(mem, parent, ndx_in_parent, out);
    }
};

void ColumnBaseSimple::tree_to_dot(std::ostream& out) const
{
    ColumnBase::bptree_to_dot(get_root_array(), out);
}

void ColumnBase::bptree_to_dot(const Array* root, std::ostream& out) const
{
    LeafToDot handler(*this);
    root->bptree_to_dot(out, handler);
}

void ColumnBase::dump_node_structure() const
{
    do_dump_node_structure(std::cerr, 0);
}

namespace realm {
namespace _impl {

void leaf_dumper(MemRef mem, Allocator& alloc, std::ostream& out, int level)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    int indent = level * 2;
    out << std::setw(indent) << ""
        << "Integer leaf (ref: " << leaf.get_ref() << ", size: " << leaf.size() << ")\n";
    std::ostringstream out_2;
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
    out << std::setw(indent) << ""
        << "  Elems: " << out_2.str() << "\n";
}

} // namespace _impl
} // namespace realm

#endif // LCOV_EXCL_STOP ignore debug functions
