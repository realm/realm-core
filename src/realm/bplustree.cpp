/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#include <realm/bplustree.hpp>

using namespace realm;

BPlusTreeNode::~BPlusTreeNode()
{
}

ref_type BPlusTreeLeaf::bptree_insert(size_t ndx, State& state, InsertFunc& func)
{
    size_t leaf_size = get_node_size();
    REALM_ASSERT_DEBUG(leaf_size <= REALM_MAX_BPNODE_SIZE);
    if (ndx > leaf_size)
        ndx = leaf_size;
    if (REALM_LIKELY(leaf_size < REALM_MAX_BPNODE_SIZE)) {
        func(this, ndx);
        return 0; // Leaf was not split
    }

    // Split leaf node
    auto new_leaf = m_tree->create_leaf_node();
    if (ndx == leaf_size) {
        func(new_leaf.get(), 0);
        state.split_offset = ndx;
    }
    else {
        move(new_leaf.get(), ndx, 0);
        func(this, ndx);
        state.split_offset = ndx + 1;
    }
    state.split_size = leaf_size + 1;

    return new_leaf->get_ref();
}

void BPlusTreeLeaf::bptree_access(size_t ndx, AccessFunc& func)
{
    func(this, ndx);
}

size_t BPlusTreeLeaf::bptree_erase(size_t ndx, EraseFunc& func)
{
    return func(this, ndx);
}

bool BPlusTreeLeaf::bptree_traverse(size_t offset, TraverseFunc& func)
{
    return func(this, offset);
}

BPlusTreeInner::BPlusTreeInner(BPlusTreeBase* tree)
    : BPlusTreeNode(tree)
    , Array(tree->get_alloc())
    , m_offsets(tree->get_alloc())
{
    m_offsets.set_parent(this, 0);
}

void BPlusTreeInner::create(size_t elems_per_child)
{
    // Born only with room for number of elements per child
    uint64_t tagged = (elems_per_child << 1) + 1;
    Array::create(Array::type_InnerBptreeNode, false, 1, tagged);
}

BPlusTreeInner::~BPlusTreeInner()
{
}

void BPlusTreeInner::init_from_mem(MemRef mem)
{
    Array::init_from_mem(mem);
    auto rot = Array::get(0);
    if ((rot & 1) == 0) {
        // rot is a ref
        m_offsets.init_from_ref(to_ref(rot));
    }
}

void BPlusTreeInner::bptree_access(size_t n, AccessFunc& func)
{
    size_t child_ndx;
    size_t child_offset;
    if (m_offsets.is_attached()) {
        child_ndx = m_offsets.upper_bound(n);
        child_offset = get_child_offset(child_ndx);
        REALM_ASSERT_3(child_ndx, <, get_node_size());
    }
    else {
        auto elems_per_child = get_elems_per_child();
        child_ndx = n / elems_per_child;
        child_offset = child_ndx * elems_per_child;
    }

    ref_type child_ref = _get_child_ref(child_ndx);
    char* child_header = m_alloc.translate(child_ref);
    MemRef mem(child_header, child_ref, m_alloc);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        auto leaf = cache_leaf(mem, child_ndx);
        func(leaf, n - child_offset);
    }
    else {
        BPlusTreeInner node(m_tree);
        node.set_parent(this, child_ndx);
        node.init_from_mem(mem);
        node.bptree_access(n - child_offset, func);
    }
}

ref_type BPlusTreeInner::bptree_insert(size_t ndx, State& state, InsertFunc& func)
{
    size_t child_ndx;
    if (ndx != npos) {
        ensure_offsets();
        child_ndx = m_offsets.upper_bound(ndx);
        auto child_offset = get_child_offset(child_ndx);
        ndx -= child_offset;
        REALM_ASSERT_3(child_ndx, <, get_node_size());
    }
    else {
        child_ndx = get_node_size() - 1;
    }

    ref_type child_ref = _get_child_ref(child_ndx);
    char* child_header = m_alloc.translate(child_ref);
    MemRef mem(child_header, child_ref, m_alloc);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    ref_type new_sibling_ref;
    if (child_is_leaf) {
        auto leaf = cache_leaf(mem, child_ndx);
        new_sibling_ref = leaf->bptree_insert(ndx, state, func);
    }
    else {
        BPlusTreeInner node(m_tree);
        node.set_parent(this, child_ndx);
        node.init_from_mem(mem);
        new_sibling_ref = node.bptree_insert(ndx, state, func);
    }

    if (!new_sibling_ref) {
        adjust(size() - 1, +2); // Throws
        if (m_offsets.is_attached()) {
            m_offsets.adjust(child_ndx, m_offsets.size(), 1);
        }
        return 0;
    }

    return insert_child(child_ndx, new_sibling_ref, state);
}

size_t BPlusTreeInner::bptree_erase(size_t n, EraseFunc& func)
{
    ensure_offsets();

    size_t child_ndx = m_offsets.upper_bound(n);
    size_t child_offset = get_child_offset(child_ndx);
    REALM_ASSERT_3(child_ndx, <, get_node_size());

    size_t new_size;
    ref_type child_ref = _get_child_ref(child_ndx);
    char* child_header = m_alloc.translate(child_ref);
    MemRef mem(child_header, child_ref, m_alloc);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        auto leaf = cache_leaf(mem, child_ndx);
        new_size = func(leaf, n - child_offset);
    }
    else {
        BPlusTreeInner node(m_tree);
        node.set_parent(this, child_ndx);
        node.init_from_mem(mem);
        new_size = node.bptree_erase(n - child_offset, func);
    }

    size_t num_children = get_node_size();
    if (new_size == 0) {
        if (num_children == 1)
            return 0; // Destroy this node too
        erase_and_destroy_child(child_ndx);
    }
    m_offsets.adjust(child_ndx, m_offsets.size(), -1);

    adjust(size() - 1, -2);

    return get_node_size();
}

bool BPlusTreeInner::bptree_traverse(size_t n, TraverseFunc& func)
{
    size_t sz = get_node_size();
    for (size_t i = 0; i < sz; i++) {
        size_t child_offset;
        if (m_offsets.is_attached()) {
            child_offset = get_child_offset(i);
        }
        else {
            auto elems_per_child = get_elems_per_child();
            child_offset = i * elems_per_child;
        }

        bool done;
        ref_type child_ref = _get_child_ref(i);
        char* child_header = m_alloc.translate(child_ref);
        MemRef mem(child_header, child_ref, m_alloc);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
        if (child_is_leaf) {
            auto leaf = cache_leaf(mem, i);
            done = func(leaf, child_offset + n);
        }
        else {
            BPlusTreeInner node(m_tree);
            node.set_parent(this, i);
            node.init_from_mem(mem);
            // std::cout << "BPlusTreeInner offset: " << child_offset << std::endl;
            done = node.bptree_traverse(child_offset + n, func);
        }
        if (done) {
            return true;
        }
    }
    return false;
}

void BPlusTreeInner::move(BPlusTreeNode* new_node, size_t ndx, int64_t adj)
{
    BPlusTreeInner* dst(static_cast<BPlusTreeInner*>(new_node));
    size_t sz = get_node_size();

    // Copy refs
    for (size_t i = ndx; i < sz; i++) {
        size_t offs = get_child_offset(i);
        dst->_add_child_ref(_get_child_ref(i), offs - adj);
    }
    truncate(ndx + 1);
    if (ndx > 0)
        m_offsets.truncate(ndx - 1);
}

void BPlusTreeInner::ensure_offsets()
{
    if (!m_offsets.is_attached()) {
        size_t elems_per_child = get_elems_per_child();
        size_t sz = size();
        size_t num_offsets = (sz > 2) ? sz - 3 : 0;
        size_t offs = 0;

        m_offsets.create(num_offsets, num_offsets * elems_per_child);
        for (size_t i = 0; i != num_offsets; ++i) {
            offs += elems_per_child;
            m_offsets.set(i, offs);
        }
        Array::set_as_ref(0, m_offsets.get_ref());
    }
}

inline BPlusTreeLeaf* BPlusTreeInner::cache_leaf(MemRef mem, size_t ndx)
{
    return m_tree->cache_leaf(mem, this, ndx + 1);
}

void BPlusTreeInner::erase_and_destroy_child(size_t ndx)
{
    ref_type ref = _get_child_ref(ndx);
    erase(ndx + 1);
    Array::destroy_deep(ref, m_tree->get_alloc());
    REALM_ASSERT_DEBUG(m_offsets.is_attached());
    size_t sz = m_offsets.size();
    if (sz) {
        // in this case there will always be an offset to erase
        if (ndx < sz) {
            m_offsets.erase(ndx);
        }
        else {
            m_offsets.erase(sz - 1);
        }
    }
    REALM_ASSERT_DEBUG(m_offsets.size() == get_node_size() - 1);
}

ref_type BPlusTreeInner::insert_child(size_t child_ndx, ref_type new_sibling_ref, State& state)
{
    size_t new_ref_ndx = child_ndx + 1;

    size_t sz = get_node_size();
    if (sz < REALM_MAX_BPNODE_SIZE) {
        // Room in current node for the new child
        adjust(size() - 1, +2); // Throws
        if (m_offsets.is_attached()) {
            size_t elem_ndx_offset = get_child_offset(child_ndx);
            m_offsets.insert(child_ndx, elem_ndx_offset + state.split_offset); // Throws
            m_offsets.adjust(child_ndx + 1, m_offsets.size(), +1);             // Throws
        }
        _insert_child_ref(new_ref_ndx, new_sibling_ref);
        return ref_type(0);
    }

    // This node has to be split
    BPlusTreeInner new_sibling(m_tree);

    size_t elem_ndx_offset = 0;
    if (m_offsets.is_attached()) {
        new_sibling.create(0);
        new_sibling.ensure_offsets();
        elem_ndx_offset = get_child_offset(child_ndx);
    }
    else {
        size_t elems_per_child = get_elems_per_child();
        elem_ndx_offset = child_ndx * elems_per_child;
        new_sibling.create(elems_per_child);
    }

    size_t new_split_offset;
    size_t new_split_size;
    if (new_ref_ndx == sz) {
        // Case 1/2: The split child was the last child of the parent
        // to be split. In this case the parent may or may not be on
        // the compact form.
        new_split_offset = size_t(elem_ndx_offset + state.split_offset);
        new_split_size = elem_ndx_offset + state.split_size;
        new_sibling._add_child_ref(new_sibling_ref); // Throws
        set_tree_size(new_split_offset);             // Throws
    }
    else {
        // Case 2/2: The split child was not the last child of the
        // parent to be split. Since this is not possible during
        // 'append', we can safely assume that the parent node is on
        // the general form.
        new_split_offset = size_t(elem_ndx_offset + state.split_size);
        new_split_size = get_tree_size() + 1;

        move(&new_sibling, new_ref_ndx, (new_split_offset - 1));               // Strips off tree size
        _add_child_ref(new_sibling_ref, elem_ndx_offset + state.split_offset); // Throws
        append_tree_size(new_split_offset);                                    // Throws
    }

    new_sibling.append_tree_size(new_split_size - new_split_offset); // Throws

    state.split_offset = new_split_offset;
    state.split_size = new_split_size;

    return new_sibling.get_ref();
}

BPlusTreeBase::~BPlusTreeBase()
{
}

void BPlusTreeBase::replace_root(std::unique_ptr<BPlusTreeNode> new_root)
{
    // Maintain parent.
    new_root->set_parent(m_parent, m_ndx_in_parent);
    new_root->update_parent(); // Throws

    m_root = std::move(new_root);
}

void BPlusTreeBase::bptree_insert(size_t n, BPlusTreeNode::InsertFunc& func)
{
    size_t bptree_size = m_root->get_tree_size();
    if (n == bptree_size) {
        n = npos;
    }
    BPlusTreeNode::State state;
    ref_type new_sibling_ref = m_root->bptree_insert(n, state, func);
    if (REALM_UNLIKELY(new_sibling_ref)) {
        bool compact_form = (n == npos) && m_root->is_compact();
        auto new_root = std::make_unique<BPlusTreeInner>(this);
        if (!compact_form) {
            new_root->create(0);
            new_root->ensure_offsets();
        }
        else {
            new_root->create(size_t(state.split_offset));
        }

        new_root->_add_child_ref(m_root->get_ref());                   // Throws
        new_root->_add_child_ref(new_sibling_ref, state.split_offset); // Throws
        new_root->append_tree_size(state.split_size);
        replace_root(std::move(new_root));
    }
}

void BPlusTreeBase::bptree_erase(size_t n, BPlusTreeNode::EraseFunc& func)
{
    size_t root_size = m_root->bptree_erase(n, func);
    while (!m_root->is_leaf() && root_size == 1) {
        BPlusTreeInner* node = static_cast<BPlusTreeInner*>(m_root.get());

        ref_type new_root_ref = node->_get_child_ref(0);
        node->_clear_child_ref(0);
        node->destroy_deep();

        auto new_root = create_root_from_ref(new_root_ref);

        replace_root(std::move(new_root));
        root_size = m_root->get_node_size();
    }
}

std::unique_ptr<BPlusTreeNode> BPlusTreeBase::create_root_from_ref(ref_type ref)
{
    char* header = m_alloc.translate(ref);
    bool is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
    bool reuse_root = m_root && m_root->is_leaf() == is_leaf;

    if (reuse_root) {
        m_root->init_from_ref(ref);
        return std::move(m_root);
    }

    if (is_leaf) {
        return init_leaf_node(ref);
    }
    else {
        std::unique_ptr<BPlusTreeNode> new_root = std::make_unique<BPlusTreeInner>(this);
        new_root->init_from_ref(ref);
        return new_root;
    }
}
