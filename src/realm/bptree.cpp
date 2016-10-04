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

#include <realm/array_direct.hpp>
#include <realm/bptree.hpp>
#include <realm/array_integer.hpp>

using namespace realm;

namespace {

class UpdateAdapter {
public:
    UpdateAdapter(BpTreeNode::UpdateHandler& handler) noexcept
        : m_handler(handler)
    {
    }
    void operator()(const BpTreeNode::NodeInfo& leaf_info)
    {
        size_t elem_ndx_in_leaf = 0;
        m_handler.update(leaf_info.m_mem, leaf_info.m_parent, leaf_info.m_ndx_in_parent, elem_ndx_in_leaf); // Throws
    }

private:
    BpTreeNode::UpdateHandler& m_handler;
};

class VisitAdapter {
public:
    VisitAdapter(BpTreeNode::VisitHandler& handler) noexcept
        : m_handler(handler)
    {
    }
    bool operator()(const BpTreeNode::NodeInfo& leaf_info)
    {
        return m_handler.visit(leaf_info); // Throws
    }

private:
    BpTreeNode::VisitHandler& m_handler;
};

class ArrayOffsets : public Array {
public:
    using Array::Array;
    using Array::copy_on_write;
};

} // anonymous namespace


namespace {

// Find the index of the child node that contains the specified
// element index. Element index zero corresponds to the first element
// of the first leaf node contained in the subtree corresponding with
// the specified 'offsets' array.
//
// Returns (child_ndx, ndx_in_child).
template <int width>
inline std::pair<size_t, size_t> find_child_from_offsets(const char* offsets_header, size_t elem_ndx) noexcept
{
    const char* offsets_data = Array::get_data_from_header(offsets_header);
    size_t offsets_size = Array::get_size_from_header(offsets_header);
    size_t child_ndx = upper_bound<width>(offsets_data, offsets_size, elem_ndx);
    size_t elem_ndx_offset = child_ndx == 0 ? 0 : to_size_t(get_direct<width>(offsets_data, child_ndx - 1));
    size_t ndx_in_child = elem_ndx - elem_ndx_offset;
    return std::make_pair(child_ndx, ndx_in_child);
}


// Returns (child_ndx, ndx_in_child)
inline std::pair<size_t, size_t> find_bptree_child(int_fast64_t first_value, size_t ndx,
                                                   const Allocator& alloc) noexcept
{
    size_t child_ndx;
    size_t ndx_in_child;
    if (first_value % 2 != 0) {
        // Case 1/2: No offsets array (compact form)
        size_t elems_per_child = to_size_t(first_value / 2);
        child_ndx = ndx / elems_per_child;
        ndx_in_child = ndx % elems_per_child;
        // FIXME: It may be worth considering not to store the total
        // number of elements in each compact node. This would also
        // speed up a tight sequence of append-to-column.
    }
    else {
        // Case 2/2: Offsets array (general form)
        ref_type offsets_ref = to_ref(first_value);
        char* offsets_header = alloc.translate(offsets_ref);
        uint_least8_t offsets_width = Array::get_width_from_header(offsets_header);
        std::pair<size_t, size_t> p;
        REALM_TEMPEX(p = find_child_from_offsets, offsets_width, (offsets_header, ndx));
        child_ndx = p.first;
        ndx_in_child = p.second;
    }
    return std::make_pair(child_ndx, ndx_in_child);
}


// Returns (child_ndx, ndx_in_child)
inline std::pair<size_t, size_t> find_bptree_child(Array& node, size_t ndx) noexcept
{
    int_fast64_t first_value = node.get(0);
    return find_bptree_child(first_value, ndx, node.get_alloc());
}


// Returns (child_ref, ndx_in_child)
template <int width>
inline std::pair<ref_type, size_t> find_bptree_child(const char* data, size_t ndx, const Allocator& alloc) noexcept
{
    int_fast64_t first_value = get_direct<width>(data, 0);
    std::pair<size_t, size_t> p = find_bptree_child(first_value, ndx, alloc);
    size_t child_ndx = p.first;
    size_t ndx_in_child = p.second;
    ref_type child_ref = to_ref(get_direct<width>(data, 1 + child_ndx));
    return std::make_pair(child_ref, ndx_in_child);
}


// Visit leaves of the B+-tree rooted at this inner node, starting
// with the leaf that contains the element at the specified global
// index start offset (`start_offset`), and ending when the handler
// returns false.
//
// The specified node must be an inner node, and the specified handler
// must have the follewing signature:
//
//     bool handler(const Array::NodeInfo& leaf_info)
//
// `node_offset` is the global index of the first element in this
// subtree, and `node_size` is the number of elements in it.
//
// This function returns true if, and only if the handler has returned
// true for all handled leafs.
//
// This function is designed work without the presence of the `N_t`
// field in the inner B+-tree node
// (a.k.a. `total_elems_in_subtree`). This was done in anticipation of
// the removal of the deprecated field in a future version of the
// Realm file format.
//
// This function is also designed in anticipation of a change in the
// way column accessors work. Some aspects of the implementation of
// this function are not yet as they are intended to be, due the fact
// that column accessors cache the root node rather than the last used
// leaf node. When the behaviour of the column accessors is changed,
// the signature of this function should be changed to
// foreach_bptree_leaf(const array::NodeInfo&, Handler, size_t
// start_offset). This will allow for a number of minor (but
// important) improvements.
template <class Handler>
bool foreach_bptree_leaf(Array& node, size_t node_offset, size_t node_size, Handler handler,
                         size_t start_offset) noexcept(noexcept(handler(BpTreeNode::NodeInfo())))
{
    REALM_ASSERT(node.is_inner_bptree_node());

    Allocator& alloc = node.get_alloc();
    Array offsets(alloc);
    size_t child_ndx = 0, child_offset = node_offset;
    size_t elems_per_child = 0;
    {
        REALM_ASSERT_3(node.size(), >=, 1);
        int_fast64_t first_value = node.get(0);
        bool is_compact = first_value % 2 != 0;
        if (is_compact) {
            // Compact form
            elems_per_child = to_size_t(first_value / 2);
            if (start_offset > node_offset) {
                size_t local_start_offset = start_offset - node_offset;
                child_ndx = local_start_offset / elems_per_child;
                child_offset += child_ndx * elems_per_child;
            }
        }
        else {
            // General form
            ref_type offsets_ref = to_ref(first_value);
            offsets.init_from_ref(offsets_ref);
            if (start_offset > node_offset) {
                size_t local_start_offset = start_offset - node_offset;
                child_ndx = offsets.upper_bound_int(local_start_offset);
                if (child_ndx > 0)
                    child_offset += to_size_t(offsets.get(child_ndx - 1));
            }
        }
    }
    REALM_ASSERT_3(node.size(), >=, 2);
    size_t num_children = node.size() - 2;
    REALM_ASSERT_3(num_children, >=, 1); // invar:bptree-nonempty-inner
    BpTreeNode::NodeInfo child_info;
    child_info.m_parent = &node;
    child_info.m_ndx_in_parent = 1 + child_ndx;
    child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
    child_info.m_offset = child_offset;
    bool children_are_leaves = !Array::get_is_inner_bptree_node_from_header(child_info.m_mem.get_addr());
    for (;;) {
        child_info.m_size = elems_per_child;
        bool is_last_child = child_ndx == num_children - 1;
        if (!is_last_child) {
            bool is_compact = elems_per_child != 0;
            if (!is_compact) {
                size_t next_child_offset = node_offset + to_size_t(offsets.get(child_ndx - 1 + 1));
                child_info.m_size = next_child_offset - child_info.m_offset;
            }
        }
        else {
            size_t next_child_offset = node_offset + node_size;
            child_info.m_size = next_child_offset - child_info.m_offset;
        }
        bool go_on;
        if (children_are_leaves) {
            const BpTreeNode::NodeInfo& const_child_info = child_info;
            go_on = handler(const_child_info);
        }
        else {
            Array child(alloc);
            child.init_from_mem(child_info.m_mem);
            child.set_parent(child_info.m_parent, child_info.m_ndx_in_parent);
            go_on = foreach_bptree_leaf(child, child_info.m_offset, child_info.m_size, handler, start_offset);
        }
        if (!go_on)
            return false;
        if (is_last_child)
            break;
        ++child_ndx;
        child_info.m_ndx_in_parent = 1 + child_ndx;
        child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
        child_info.m_offset += child_info.m_size;
    }
    return true;
}


// Same as foreach_bptree_leaf() except that this version is faster
// and has no support for slicing. That also means that the return
// value of the handler is ignored. Finally,
// `Array::NodeInfo::m_offset` and `Array::NodeInfo::m_size` are not
// calculated. With these simplification it is possible to avoid any
// access to the `offsets` array.
template <class Handler>
void simplified_foreach_bptree_leaf(Array& node, Handler handler) noexcept(noexcept(handler(BpTreeNode::NodeInfo())))
{
    REALM_ASSERT(node.is_inner_bptree_node());

    Allocator& alloc = node.get_alloc();
    size_t child_ndx = 0;
    REALM_ASSERT_3(node.size(), >=, 2);
    size_t num_children = node.size() - 2;
    REALM_ASSERT_3(num_children, >=, 1); // invar:bptree-nonempty-inner
    BpTreeNode::NodeInfo child_info;
    child_info.m_parent = &node;
    child_info.m_ndx_in_parent = 1 + child_ndx;
    child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
    child_info.m_offset = 0;
    child_info.m_size = 0;
    bool children_are_leaves = !Array::get_is_inner_bptree_node_from_header(child_info.m_mem.get_addr());
    for (;;) {
        if (children_are_leaves) {
            const BpTreeNode::NodeInfo& const_child_info = child_info;
            handler(const_child_info);
        }
        else {
            Array child(alloc);
            child.init_from_mem(child_info.m_mem);
            child.set_parent(child_info.m_parent, child_info.m_ndx_in_parent);
            simplified_foreach_bptree_leaf(child, handler);
        }
        bool is_last_child = child_ndx == num_children - 1;
        if (is_last_child)
            break;
        ++child_ndx;
        child_info.m_ndx_in_parent = 1 + child_ndx;
        child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
    }
}


inline void destroy_inner_bptree_node(MemRef mem, int_fast64_t first_value, Allocator& alloc) noexcept
{
    alloc.free_(mem);
    if (first_value % 2 == 0) {
        // Node has offsets array
        ref_type offsets_ref = to_ref(first_value);
        alloc.free_(offsets_ref, alloc.translate(offsets_ref));
    }
}

void destroy_singlet_bptree_branch(MemRef mem, Allocator& alloc, BpTreeNode::EraseHandler& handler) noexcept
{
    MemRef mem_2 = mem;
    for (;;) {
        const char* header = mem_2.get_addr();
        bool is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
        if (is_leaf) {
            handler.destroy_leaf(mem_2);
            return;
        }

        const char* data = Array::get_data_from_header(header);
        uint_least8_t width = Array::get_width_from_header(header);
        size_t ndx = 0;
        std::pair<int_fast64_t, int_fast64_t> p = get_two(data, width, ndx);
        int_fast64_t first_value = p.first;
        ref_type child_ref = to_ref(p.second);

        destroy_inner_bptree_node(mem_2, first_value, alloc);

        mem_2.set_ref(child_ref);
        mem_2.set_addr(alloc.translate(child_ref));
        // inform encryption layer on next loop iteration
    }
}

void elim_superfluous_bptree_root(Array* root, MemRef parent_mem, int_fast64_t parent_first_value, ref_type child_ref,
                                  BpTreeNode::EraseHandler& handler)
{
    Allocator& alloc = root->get_alloc();
    char* child_header = alloc.translate(child_ref);
    MemRef child_mem(child_header, child_ref, alloc);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        handler.replace_root_by_leaf(child_mem); // Throws
        // Since the tree has now been modified, the height reduction
        // operation cannot be aborted without leaking memory, so the
        // rest of the operation must proceed without throwing. This
        // includes retrocursive completion of earlier invocations of
        // this function.
        //
        // Note also that 'root' may be destroy at this point.
    }
    else {
        size_t child_size = Array::get_size_from_header(child_header);
        REALM_ASSERT_3(child_size, >=, 2);
        size_t num_grandchildren = child_size - 2;
        REALM_ASSERT_3(num_grandchildren, >=, 1); // invar:bptree-nonempty-inner
        if (num_grandchildren > 1) {
            // This child is an inner node, and is the closest one to
            // the root that has more than one child, so make it the
            // new root.
            root->init_from_ref(child_ref);
            root->update_parent(); // Throws
            // From this point on, the height reduction operation
            // cannot be aborted without leaking memory, so the rest
            // of the operation must proceed without throwing. This
            // includes retrocursive completion of earlier invocations
            // of this function.
        }
        else {
            // This child is an inner node, but has itself just one
            // child, so continue hight reduction.
            int_fast64_t child_first_value = Array::get(child_header, 0);
            ref_type grandchild_ref = to_ref(Array::get(child_header, 1));
            elim_superfluous_bptree_root(root, child_mem, child_first_value, grandchild_ref, handler); // Throws
        }
    }

    // At this point, a new root has been installed. The new root is
    // some descendant of the node referenced by 'parent_mem'. Array
    // nodes comprising eliminated B+-tree nodes must be freed. Our
    // job is to free those comprising that parent. It is crucial that
    // this part does not throw.
    alloc.free_(parent_mem);
    if (parent_first_value % 2 == 0) {
        // Parent has offsets array
        ref_type offsets_ref = to_ref(parent_first_value);
        alloc.free_(offsets_ref, alloc.translate(offsets_ref));
    }
}

} // anonymous namespace

std::pair<MemRef, size_t> BpTreeNode::get_bptree_leaf(size_t ndx) const noexcept
{
    REALM_ASSERT(is_inner_bptree_node());

    size_t ndx_2 = ndx;
    uint_least8_t width = m_width;
    const char* data = m_data;

    for (;;) {
        std::pair<ref_type, size_t> p;
        REALM_TEMPEX(p = find_bptree_child, width, (data, ndx_2, m_alloc));
        ref_type child_ref = p.first;
        size_t ndx_in_child = p.second;
        char* child_header = m_alloc.translate(child_ref);
        bool child_is_leaf = !get_is_inner_bptree_node_from_header(child_header);
        if (child_is_leaf) {
            MemRef mem(child_header, child_ref, m_alloc);
            return std::make_pair(mem, ndx_in_child);
        }
        ndx_2 = ndx_in_child;
        width = get_width_from_header(child_header);
        data = get_data_from_header(child_header);
    }
    return {};
}

ref_type BpTreeNode::insert_bptree_child(Array& offsets, size_t orig_child_ndx, ref_type new_sibling_ref,
                                         TreeInsertBase& state)
{
    // When a child is split, the new child must always be inserted
    // after the original
    size_t orig_child_ref_ndx = 1 + orig_child_ndx;
    size_t insert_ndx = orig_child_ref_ndx + 1;

    REALM_ASSERT_DEBUG(insert_ndx <= size() - 1);
    if (REALM_LIKELY(size() < 1 + REALM_MAX_BPNODE_SIZE + 1)) {
        // Case 1/2: This parent has space for the new child, so it
        // does not have to be split.
        insert(insert_ndx, new_sibling_ref); // Throws
        // +2 because stored value is 1 + 2*total_elems_in_subtree
        adjust(size() - 1, +2); // Throws
        if (offsets.is_attached()) {
            size_t elem_ndx_offset = orig_child_ndx > 0 ? to_size_t(offsets.get(orig_child_ndx - 1)) : 0;
            offsets.insert(orig_child_ndx, elem_ndx_offset + state.m_split_offset); // Throws
            offsets.adjust(orig_child_ndx + 1, offsets.size(), +1);                 // Throws
        }
        return 0; // Parent node was not split
    }

    // Case 2/2: This parent is full, so it needs to be plit.
    //
    // We first create a new sibling of the parent, and then we move
    // some of the children over. The caller must insert the new
    // sibling after the original.
    size_t elem_ndx_offset = 0;
    if (orig_child_ndx > 0) {
        if (offsets.is_attached()) {
            elem_ndx_offset = size_t(offsets.get(orig_child_ndx - 1));
        }
        else {
            int_fast64_t elems_per_child = get(0) / 2;
            elem_ndx_offset = size_t(orig_child_ndx * elems_per_child);
        }
    }

    Allocator& allocator = get_alloc();
    Array new_sibling(allocator), new_offsets(allocator);
    new_sibling.create(type_InnerBptreeNode); // Throws
    if (offsets.is_attached()) {
        new_offsets.set_parent(&new_sibling, 0);
        new_offsets.create(type_Normal);                  // Throws
        new_sibling.add(from_ref(new_offsets.get_ref())); // Throws
    }
    else {
        int_fast64_t v = get(0); // v = 1 + 2 * elems_per_child
        new_sibling.add(v);      // Throws
    }
    size_t new_split_offset, new_split_size;
    if (insert_ndx - 1 >= REALM_MAX_BPNODE_SIZE) {
        REALM_ASSERT_3(insert_ndx - 1, ==, REALM_MAX_BPNODE_SIZE);
        // Case 1/2: The split child was the last child of the parent
        // to be split. In this case the parent may or may not be on
        // the compact form.
        new_split_offset = elem_ndx_offset + state.m_split_offset;
        new_split_size = elem_ndx_offset + state.m_split_size;
        new_sibling.add(new_sibling_ref); // Throws
    }
    else {
        // Case 2/2: The split child was not the last child of the
        // parent to be split. Since this is not possible during
        // 'append', we can safely assume that the parent node is on
        // the general form.
        REALM_ASSERT(new_offsets.is_attached());
        new_split_offset = elem_ndx_offset + state.m_split_size;
        new_split_size = to_size_t(back() / 2) + 1;
        REALM_ASSERT_3(size(), >=, 2);
        size_t num_children = size() - 2;
        REALM_ASSERT_3(num_children, >=, 1); // invar:bptree-nonempty-inner
        // Move some refs over
        size_t child_refs_end = 1 + num_children;
        for (size_t i = insert_ndx; i != child_refs_end; ++i)
            new_sibling.add(get(i)); // Throws
        // Move some offsets over
        size_t offsets_end = num_children - 1;
        for (size_t i = orig_child_ndx + 1; i != offsets_end; ++i) {
            size_t offset = to_size_t(offsets.get(i));
            new_offsets.add(offset - (new_split_offset - 1)); // Throws
        }
        // Update original parent
        erase(insert_ndx + 1, child_refs_end);
        set(insert_ndx, from_ref(new_sibling_ref)); // Throws
        offsets.erase(orig_child_ndx + 1, offsets_end);
        offsets.set(orig_child_ndx, elem_ndx_offset + state.m_split_offset); // Throws
    }
    int_fast64_t v = new_split_offset;     // total_elems_in_subtree
    set(size() - 1, 1 + 2 * v);            // Throws
    v = new_split_size - new_split_offset; // total_elems_in_subtree
    new_sibling.add(1 + 2 * v);            // Throws
    state.m_split_offset = new_split_offset;
    state.m_split_size = new_split_size;
    return new_sibling.get_ref();
}

void BpTreeNode::create_bptree_offsets(Array& offsets, int_fast64_t first_value)
{
    offsets.create(type_Normal); // Throws
    int_fast64_t elems_per_child = first_value / 2;
    int_fast64_t accum_num_elems = 0;
    size_t num_children = size() - 2;
    for (size_t i = 0; i != num_children - 1; ++i) {
        accum_num_elems += elems_per_child;
        offsets.add(accum_num_elems); // Throws
    }
    set(0, offsets.get_ref()); // Throws
}


bool BpTreeNode::do_erase_bptree_elem(size_t elem_ndx, EraseHandler& handler)
{
    ArrayOffsets offsets(m_alloc);
    size_t child_ndx;
    size_t ndx_in_child;
    if (elem_ndx == npos) {
        size_t num_children = size() - 2;
        child_ndx = num_children - 1;
        ndx_in_child = npos;
    }
    else {
        // If this node is not already on the general form, convert it
        // now. Since this conversion will occur from root to leaf, it
        // will maintain invar:bptree-node-form.
        ensure_bptree_offsets(offsets); // Throws

        // Ensure that the offsets array is not in read-only memory. This
        // is necessary to guarantee that the adjustments of the element
        // counts below will succeed.
        offsets.copy_on_write(); // Throws

        // FIXME: Can we pass 'offsets' to find_bptree_child() to
        // speed it up?
        std::pair<size_t, size_t> p = find_bptree_child(*this, elem_ndx);
        child_ndx = p.first;
        ndx_in_child = p.second;
    }

    size_t child_ref_ndx = 1 + child_ndx;
    ref_type child_ref = get_as_ref(child_ref_ndx);
    char* child_header = m_alloc.translate(child_ref);
    MemRef child_mem(child_header, child_ref, m_alloc);
    bool child_is_leaf = !get_is_inner_bptree_node_from_header(child_header);
    bool destroy_child;
    if (child_is_leaf) {
        destroy_child = handler.erase_leaf_elem(child_mem, this, child_ref_ndx, ndx_in_child); // Throws
    }
    else {
        BpTreeNode child(m_alloc);
        child.init_from_mem(child_mem);
        child.set_parent(this, child_ref_ndx);
        destroy_child = child.do_erase_bptree_elem(ndx_in_child, handler); // Throws
    }
    size_t num_children = size() - 2;
    if (destroy_child) {
        if (num_children == 1)
            return true; // Destroy this node too
        REALM_ASSERT_3(num_children, >=, 2);
        child_ref = get_as_ref(child_ref_ndx);
        child_header = m_alloc.translate(child_ref);
        // destroy_singlet.... will take care of informing the encryption layer
        child_mem = MemRef(child_header, child_ref, m_alloc);
        erase(child_ref_ndx); // Throws
        destroy_singlet_bptree_branch(child_mem, m_alloc, handler);
        // If the erased element is the last one, we did not attach
        // the offsets array above, even if one was preset. Since we
        // are removing a child, we have to do that now.
        if (elem_ndx == npos) {
            int_fast64_t first_value = front();
            bool general_form = first_value % 2 == 0;
            if (general_form) {
                offsets.init_from_ref(to_ref(first_value));
                offsets.set_parent(this, 0);
            }
        }
    }
    if (offsets.is_attached()) {
        // These adjustments are guaranteed to succeed because of the
        // copy-on-write on the offets array above, and because of the
        // fact that we never increase or insert values.
        size_t offsets_adjust_begin = child_ndx;
        if (destroy_child) {
            if (offsets_adjust_begin == num_children - 1)
                --offsets_adjust_begin;
            offsets.erase(offsets_adjust_begin);
        }
        offsets.adjust(offsets_adjust_begin, offsets.size(), -1);
    }

    // The following adjustment is guaranteed to succeed because we
    // decrease the value, and because the subtree rooted at this node
    // has been modified, so this array cannot be in read-only memory
    // any longer.
    adjust(size() - 1, -2); // -2 because stored value is 1 + 2*total_elems_in_subtree

    return false; // Element erased and offsets adjusted
}


void BpTreeBase::replace_root(std::unique_ptr<Array> leaf)
{
    if (m_root) {
        // Maintain parent.
        ArrayParent* parent = m_root->get_parent();
        size_t ndx_in_parent = m_root->get_ndx_in_parent();
        leaf->set_parent(parent, ndx_in_parent);
        leaf->update_parent(); // Throws
    }
    m_root = std::move(leaf);
}

void BpTreeBase::introduce_new_root(ref_type new_sibling_ref, TreeInsertBase& state, bool is_append)
{
    // At this point the original root and its new sibling is either
    // both leaves, or both inner nodes on the same form, compact or
    // general. Due to invar:bptree-node-form, the new root is allowed
    // to be on the compact form if is_append is true and both
    // siblings are either leaves or inner nodes on the compact form.

    Array* orig_root = &root();
    Allocator& alloc = get_alloc();
    std::unique_ptr<BpTreeNode> new_root(new BpTreeNode(alloc)); // Throws
    new_root->create(Array::type_InnerBptreeNode);               // Throws
    new_root->set_parent(orig_root->get_parent(), orig_root->get_ndx_in_parent());
    new_root->update_parent(); // Throws
    bool compact_form = is_append && (!orig_root->is_inner_bptree_node() || orig_root->get(0) % 2 != 0);
    // Something is wrong if we were not appending and the original
    // root is still on the compact form.
    REALM_ASSERT(!compact_form || is_append);
    if (compact_form) {
        int_fast64_t v = state.m_split_offset; // elems_per_child
        new_root->add(1 + 2 * v);              // Throws
    }
    else {
        Array new_offsets(alloc);
        new_offsets.create(Array::type_Normal);         // Throws
        new_offsets.add(state.m_split_offset);          // Throws
        new_root->add(from_ref(new_offsets.get_ref())); // Throws
    }
    new_root->add(orig_root->get_ref()); // Throws
    new_root->add(new_sibling_ref);      // Throws
    int_fast64_t v = state.m_split_size; // total_elems_in_tree
    new_root->add(1 + 2 * v);            // Throws
    replace_root(std::move(new_root));
}


// Throws only if handler throws.
bool BpTreeNode::visit_bptree_leaves(size_t elem_ndx_offset, size_t elems_in_tree, VisitHandler& handler)
{
    REALM_ASSERT_3(elem_ndx_offset, <, elems_in_tree);
    size_t root_offset = 0, root_size = elems_in_tree;
    VisitAdapter adapter(handler);
    size_t start_offset = elem_ndx_offset;
    return foreach_bptree_leaf(*this, root_offset, root_size, adapter, start_offset); // Throws
}

void BpTreeNode::update_bptree_leaves(UpdateHandler& handler)
{
    UpdateAdapter adapter(handler);
    simplified_foreach_bptree_leaf(*this, adapter); // Throws
}

void BpTreeNode::update_bptree_elem(size_t elem_ndx, UpdateHandler& handler)
{
    REALM_ASSERT(is_inner_bptree_node());

    std::pair<size_t, size_t> p = find_bptree_child(*this, elem_ndx);
    size_t child_ndx = p.first;
    size_t ndx_in_child = p.second;
    size_t child_ref_ndx = 1 + child_ndx;
    ref_type child_ref = get_as_ref(child_ref_ndx);
    char* child_header = m_alloc.translate(child_ref);
    MemRef child_mem(child_header, child_ref, m_alloc);
    bool child_is_leaf = !get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        handler.update(child_mem, this, child_ref_ndx, ndx_in_child); // Throws
        return;
    }
    BpTreeNode child(m_alloc);
    child.init_from_mem(child_mem);
    child.set_parent(this, child_ref_ndx);
    child.update_bptree_elem(ndx_in_child, handler); // Throws
}


void BpTreeNode::erase_bptree_elem(BpTreeNode* root, size_t elem_ndx, EraseHandler& handler)
{
    REALM_ASSERT(root->is_inner_bptree_node());
    REALM_ASSERT_3(root->size(), >=, 1 + 1 + 1); // invar:bptree-nonempty-inner
    REALM_ASSERT_DEBUG(elem_ndx == npos || elem_ndx + 1 != root->get_bptree_size());

    // Note that this function is implemented in a way that makes it
    // fully exception safe. Please be sure to keep it that way.

    bool destroy_root = root->do_erase_bptree_elem(elem_ndx, handler); // Throws

    // do_erase_bptree_elem() returns true if erasing the element
    // would produce an empty tree. In this case, to maintain
    // invar:bptree-nonempty-inner, we must replace the root with an
    // empty leaf.
    //
    // FIXME: ExceptionSafety: While this maintains general exception
    // safety, it does not provide the extra guarantee that we would
    // like, namely that removal of an element is guaranteed to
    // succeed if that element was inserted during the current
    // transaction (noexcept:bptree-erase). This is why we want to be
    // able to have a column with no root node and a zero-ref in
    // Table::m_columns.
    if (destroy_root) {
        MemRef root_mem = root->get_mem();
        REALM_ASSERT_3(root->size(), >=, 2);
        int_fast64_t first_value = root->get(0);
        ref_type child_ref = root->get_as_ref(1);
        Allocator& alloc = root->get_alloc();
        handler.replace_root_by_empty_leaf(); // Throws
        // 'root' may be destroyed at this point
        destroy_inner_bptree_node(root_mem, first_value, alloc);
        char* child_header = alloc.translate(child_ref);
        // destroy_singlet.... will take care of informing the encryption layer
        MemRef child_mem(child_header, child_ref, alloc);
        destroy_singlet_bptree_branch(child_mem, alloc, handler);
        return;
    }

    // If at this point, the root has only a single child left, the
    // root has become superfluous, and can be replaced by its single
    // child. This applies recursivly.
    size_t num_children = root->size() - 2;
    if (num_children > 1)
        return;

    // ExceptionSafety: The recursive elimination of superfluous
    // singlet roots is desirable but optional according to the tree
    // invariants. Since we cannot allow an exception to be thrown
    // after having successfully modified the tree, and since the root
    // elimination process cannot be guaranteed to not throw, we have
    // to abort a failed attempt by catching and ignoring the thrown
    // exception. This is always safe due to the exception safety of
    // the root elimination process itself.
    try {
        MemRef root_mem = root->get_mem();
        REALM_ASSERT_3(root->size(), >=, 2);
        int_fast64_t first_value = root->get(0);
        ref_type child_ref = root->get_as_ref(1);
        elim_superfluous_bptree_root(root, root_mem, first_value, child_ref, handler); // Throws
    }
    catch (...) {
        // Abort optional step by ignoring excpetion
    }
}

namespace {

class TreeWriter {
public:
    TreeWriter(_impl::OutputStream&) noexcept;
    ~TreeWriter() noexcept;

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
    ~ParentLevel() noexcept;

    void add_child_ref(ref_type child_ref, size_t elems_in_child, bool leaf_or_compact, ref_type* is_last);

private:
    const size_t m_max_elems_per_child; // A power of `REALM_MAX_BPNODE_SIZE`
    size_t m_elems_in_parent;           // Zero if reinitialization is needed
    bool m_is_on_general_form;          // Defined only when m_elems_in_parent > 0
    Array m_main;
    ArrayInteger m_offsets;
    _impl::OutputStream& m_out;
    std::unique_ptr<ParentLevel> m_prev_parent_level;
};


inline TreeWriter::TreeWriter(_impl::OutputStream& out) noexcept
    : m_alloc(Allocator::get_default())
    , m_out(out)
{
}

inline TreeWriter::~TreeWriter() noexcept
{
}

void TreeWriter::add_leaf_ref(ref_type leaf_ref, size_t elems_in_leaf, ref_type* is_last)
{
    if (!m_last_parent_level) {
        if (is_last) {
            *is_last = leaf_ref;
            return;
        }
        m_last_parent_level.reset(new ParentLevel(m_alloc, m_out, REALM_MAX_BPNODE_SIZE)); // Throws
    }
    bool leaf_or_compact = true;
    m_last_parent_level->add_child_ref(leaf_ref, elems_in_leaf, leaf_or_compact, is_last); // Throws
}


inline TreeWriter::ParentLevel::ParentLevel(Allocator& alloc, _impl::OutputStream& out, size_t max_elems_per_child)
    : m_max_elems_per_child(max_elems_per_child)
    , m_elems_in_parent(0)
    , m_main(alloc)
    , m_offsets(alloc)
    , m_out(out)
{
    m_main.create(Array::type_InnerBptreeNode); // Throws
}

inline TreeWriter::ParentLevel::~ParentLevel() noexcept
{
    m_offsets.destroy(); // Shallow
    m_main.destroy();    // Shallow
}

void TreeWriter::ParentLevel::add_child_ref(ref_type child_ref, size_t elems_in_child, bool leaf_or_compact,
                                            ref_type* is_last)
{
    bool force_general_form = !leaf_or_compact || (elems_in_child != m_max_elems_per_child &&
                                                   m_main.size() != 1 + REALM_MAX_BPNODE_SIZE - 1 && !is_last);

    // Add the incoming child to this inner node
    if (m_elems_in_parent > 0) { // This node contains children already
        if (!m_is_on_general_form && force_general_form) {
            if (!m_offsets.is_attached())
                m_offsets.create(Array::type_Normal); // Throws
            int_fast64_t v(m_max_elems_per_child);
            size_t n = m_main.size();
            for (size_t i = 1; i != n; ++i)
                m_offsets.add(v); // Throws
            m_is_on_general_form = true;
        }
        {
            int_fast64_t v(from_ref(child_ref));
            m_main.add(v); // Throws
        }
        if (m_is_on_general_form) {
            int_fast64_t v(m_elems_in_parent);
            m_offsets.add(v); // Throws
        }
        m_elems_in_parent += elems_in_child;
        if (!is_last && m_main.size() < 1 + REALM_MAX_BPNODE_SIZE)
            return;
    }
    else {             // First child in this node
        m_main.add(0); // Placeholder for `elems_per_child` or `offsets_ref`
        int_fast64_t v(from_ref(child_ref));
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
        int_fast64_t v(m_max_elems_per_child);
        m_main.set(0, 1 + 2 * v); // Throws
    }
    else {
        bool deep = true;                                              // Deep
        bool only_if_modified = false;                                 // Always
        ref_type ref = m_offsets.write(m_out, deep, only_if_modified); // Throws
        int_fast64_t v(from_ref(ref));
        m_main.set(0, v); // Throws
    }
    {
        int_fast64_t v(m_elems_in_parent);
        m_main.add(1 + 2 * v); // Throws
    }
    bool deep = false;                                                 // Shallow
    bool only_if_modified = false;                                     // Always
    ref_type parent_ref = m_main.write(m_out, deep, only_if_modified); // Throws

    // Whether the resulting ref must be added to the previous parent
    // level, or reported as the final ref (through `is_last`) depends
    // on whether more children are going to be added, and on whether
    // a previous parent level already exists
    if (!is_last) {
        if (!m_prev_parent_level) {
            Allocator& alloc = m_main.get_alloc();
            size_t next_level_elems_per_child = m_max_elems_per_child;
            if (util::int_multiply_with_overflow_detect(next_level_elems_per_child, REALM_MAX_BPNODE_SIZE))
                throw std::runtime_error("Overflow in number of elements per child");
            m_prev_parent_level.reset(new ParentLevel(alloc, m_out, next_level_elems_per_child)); // Throws
        }
    }
    else if (!m_prev_parent_level) {
        *is_last = parent_ref;
        return;
    }
    m_prev_parent_level->add_child_ref(parent_ref, m_elems_in_parent, !m_is_on_general_form, is_last); // Throws

    // Clear the arrays in preperation for the next child
    if (!is_last) {
        if (m_offsets.is_attached())
            m_offsets.clear(); // Shallow
        m_main.clear();        // Shallow
        m_elems_in_parent = 0;
    }
}

} // anonymous namespace

struct BpTreeBase::WriteSliceHandler : public BpTreeNode::VisitHandler {
public:
    WriteSliceHandler(size_t offset, size_t size, Allocator& alloc, BpTreeBase::SliceHandler& slice_handler,
                      _impl::OutputStream& out) noexcept
        : m_begin(offset)
        , m_end(offset + size)
        , m_leaf_cache(alloc)
        , m_slice_handler(slice_handler)
        , m_out(out)
        , m_tree_writer(out)
        , m_top_ref(0)
    {
    }
    ~WriteSliceHandler() noexcept
    {
    }
    bool visit(const BpTreeNode::NodeInfo& leaf_info) override
    {
        ref_type ref;
        size_t size = leaf_info.m_size;
        size_t leaf_begin = leaf_info.m_offset;
        size_t leaf_end = leaf_begin + size;
        REALM_ASSERT_3(leaf_begin, <=, m_end);
        REALM_ASSERT_3(leaf_end, >=, m_begin);
        bool no_slicing = leaf_begin >= m_begin && leaf_end <= m_end;
        if (no_slicing) {
            // Warning: Initializing leaf as Array.
            m_leaf_cache.init_from_mem(leaf_info.m_mem);
            bool deep = true;                                        // Deep
            bool only_if_modified = false;                           // Always
            ref = m_leaf_cache.write(m_out, deep, only_if_modified); // Throws
        }
        else {
            // Slice the leaf
            Allocator& slice_alloc = Allocator::get_default();
            size_t begin = std::max(leaf_begin, m_begin);
            size_t end = std::min(leaf_end, m_end);
            size_t offset = begin - leaf_begin;
            size = end - begin;
            MemRef mem = m_slice_handler.slice_leaf(leaf_info.m_mem, offset, size, slice_alloc); // Throws
            Array slice(slice_alloc);
            _impl::DeepArrayDestroyGuard dg(&slice);
            // Warning: Initializing leaf as Array.
            slice.init_from_mem(mem);
            bool deep = true;                                 // Deep
            bool only_if_modified = false;                    // Always
            ref = slice.write(m_out, deep, only_if_modified); // Throws
        }
        ref_type* is_last = nullptr;
        if (leaf_end >= m_end)
            is_last = &m_top_ref;
        m_tree_writer.add_leaf_ref(ref, size, is_last); // Throws
        return !is_last;
    }
    ref_type get_top_ref() const noexcept
    {
        return m_top_ref;
    }

private:
    size_t m_begin, m_end;
    Array m_leaf_cache;
    BpTreeBase::SliceHandler& m_slice_handler;
    _impl::OutputStream& m_out;
    TreeWriter m_tree_writer;
    ref_type m_top_ref;
};

ref_type BpTreeBase::write_subtree(const BpTreeNode& root, size_t slice_offset, size_t slice_size, size_t table_size,
                                   SliceHandler& handler, _impl::OutputStream& out)
{
    REALM_ASSERT(root.is_inner_bptree_node());

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
    WriteSliceHandler handler_2(offset, slice_size, root.get_alloc(), handler, out);
    const_cast<BpTreeNode&>(root).visit_bptree_leaves(offset, table_size, handler_2); // Throws
    return handler_2.get_top_ref();
}
