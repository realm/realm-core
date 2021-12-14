/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#include "realm/cluster_tree.hpp"
#include "realm/group.hpp"
#include "realm/replication.hpp"
#include "realm/array_backlink.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_fixed_bytes.hpp"

#include <iostream>

/*
 * Node-splitting is done in the way that if the new element comes after all the
 * current elements, then the new element is added to the new node as the only
 * element and the old node is untouched. Here split key is the key of the new
 * element.
 * Otherwise the node is split so that the new element can be added to the old node.
 * So all elements that should come after the new element are moved to the new node.
 * Split key is the key of the first element that is moved. (First key that comes
 * after the new element).
 * Merging is done when a node is less than half full and the combined size will be
 * less than 3/4 of the max size.
 */

namespace realm {
/*
 * The inner nodes are organized in the way that the main array has a ref to the
 * (optional) key array in position 0 and the subtree depth in position 1. After
 * that follows refs to the subordinate nodes.
 */
class ClusterNodeInner : public ClusterNode {
public:
    ClusterNodeInner(Allocator& allocator, const ClusterTree& tree_top);
    ~ClusterNodeInner() override;

    void create(int sub_tree_depth);
    void init(MemRef mem) override;
    void update_from_parent() noexcept override;
    MemRef ensure_writeable(ObjKey k) override;
    void update_ref_in_parent(ObjKey, ref_type) override;

    bool is_leaf() const override
    {
        return false;
    }

    int get_sub_tree_depth() const override
    {
        return m_sub_tree_depth;
    }

    bool traverse(ClusterTree::TraverseFunction func, int64_t) const;
    void update(ClusterTree::UpdateFunction func, int64_t);

    size_t node_size() const override
    {
        return Array::size() - s_first_node_index;
    }
    size_t get_tree_size() const override
    {
        return size_t(Array::get(s_sub_tree_size)) >> 1;
    }
    void set_tree_size(size_t sub_tree_size)
    {
        Array::set(s_sub_tree_size, sub_tree_size << 1 | 1);
    }
    size_t update_sub_tree_size();

    int64_t get_last_key_value() const override;

    void ensure_general_form() override;
    void insert_column(ColKey col) override;
    void remove_column(ColKey col) override;
    size_t nb_columns() const override;
    ref_type insert(ObjKey k, const FieldValues& init_values, State& state) override;
    bool try_get(ObjKey k, State& state) const noexcept override;
    ObjKey get(size_t ndx, State& state) const override;
    size_t get_ndx(ObjKey key, size_t ndx) const noexcept override;
    size_t erase(ObjKey k, CascadeState& state) override;
    void nullify_incoming_links(ObjKey key, CascadeState& state) override;
    void add(ref_type ref, int64_t key_value = 0);

    // Reset first (and only!) child ref and return node based on the previous value
    std::unique_ptr<ClusterNode> return_and_clear_first_child()
    {
        REALM_ASSERT(node_size() == 1);
        auto new_root = m_tree_top.get_node(this, s_first_node_index);
        Array::set(s_first_node_index, 0); // The node is no longer belonging to this
        return new_root;
    }

    int64_t get_first_key_value()
    {
        return m_keys.is_attached() ? m_keys.get(0) : 0;
    }

    bool get_leaf(ObjKey key, ClusterNode::IteratorState& state) const noexcept;

    void dump_objects(int64_t key_offset, std::string lead) const override;

private:
    static constexpr size_t s_key_ref_index = 0;
    static constexpr size_t s_sub_tree_depth_index = 1;
    static constexpr size_t s_sub_tree_size = 2;
    static constexpr size_t s_first_node_index = 3;

    int m_sub_tree_depth = 0;
    int m_shift_factor = 0;

    struct ChildInfo {
        size_t ndx;
        uint64_t offset;
        ObjKey key;
        MemRef mem;
    };
    bool find_child(ObjKey key, ChildInfo& ret) const noexcept
    {
        if (m_keys.is_attached()) {
            auto upper = m_keys.upper_bound(uint64_t(key.value));
            // The first entry in m_keys will always be smaller than or equal
            // to all keys in this subtree. If you get zero returned here, the
            // key is not in the tree
            if (upper == 0) {
                return false;
            }
            ret.ndx = upper - 1;
            ret.offset = m_keys.get(ret.ndx);
        }
        else {
            size_t sz = node_size();
            REALM_ASSERT_DEBUG(sz > 0);
            size_t max_ndx = sz - 1;
            ret.ndx = std::min(size_t(key.value) >> m_shift_factor, max_ndx);
            ret.offset = ret.ndx << m_shift_factor;
        }
        ret.key = ObjKey(key.value - ret.offset);
        ref_type child_ref = _get_child_ref(ret.ndx);
        char* child_header = m_alloc.translate(child_ref);
        ret.mem = MemRef(child_header, child_ref, m_alloc);
        return true;
    }

    ref_type _get_child_ref(size_t ndx) const noexcept
    {
        return Array::get_as_ref(ndx + s_first_node_index);
    }
    void _insert_child_ref(size_t ndx, ref_type ref)
    {
        Array::insert(ndx + s_first_node_index, from_ref(ref));
    }
    void _erase_child_ref(size_t ndx)
    {
        Array::erase(ndx + s_first_node_index);
    }
    void move(size_t ndx, ClusterNode* new_node, int64_t key_adj) override;

    template <class T, class F>
    T recurse(ObjKey key, F func);

    template <class T, class F>
    T recurse(ChildInfo& child_info, F func);
    // Adjust key offset values by adding offset
    void adjust_keys(int64_t offset);
    // Make sure the first key offset value is 0
    // This is done by adjusting the child node by current first offset
    // and setting it to 0 thereafter
    void adjust_keys_first_child(int64_t adj);
};

/***************************** ClusterNodeInner ******************************/

ClusterNodeInner::ClusterNodeInner(Allocator& allocator, const ClusterTree& tree_top)
    : ClusterNode(0, allocator, tree_top)
{
}

ClusterNodeInner::~ClusterNodeInner() {}

void ClusterNodeInner::create(int sub_tree_depth)
{
    Array::create(Array::type_InnerBptreeNode, false, s_first_node_index);

    Array::set(s_key_ref_index, 0);

    Array::set(s_sub_tree_depth_index, RefOrTagged::make_tagged(sub_tree_depth));
    Array::set(s_sub_tree_size, 1); // sub_tree_size = 0 (as tagged value)
    m_sub_tree_depth = sub_tree_depth;
    m_shift_factor = m_sub_tree_depth * node_shift_factor;
}

void ClusterNodeInner::init(MemRef mem)
{
    Array::init_from_mem(mem);
    m_keys.set_parent(this, s_key_ref_index);
    ref_type ref = Array::get_as_ref(s_key_ref_index);
    if (ref) {
        m_keys.init_from_ref(ref);
    }
    else {
        m_keys.detach();
    }
    m_sub_tree_depth = int(Array::get(s_sub_tree_depth_index)) >> 1;
    m_shift_factor = m_sub_tree_depth * node_shift_factor;
}

void ClusterNodeInner::update_from_parent() noexcept
{
    Array::update_from_parent();
    ref_type ref = Array::get_as_ref(s_key_ref_index);
    if (ref) {
        m_keys.update_from_parent();
    }
    m_sub_tree_depth = int(Array::get(s_sub_tree_depth_index)) >> 1;
}

template <class T, class F>
T ClusterNodeInner::recurse(ObjKey key, F func)
{
    ChildInfo child_info;
    if (!find_child(key, child_info)) {
        throw KeyNotFound("Child not found in recurse");
    }
    return recurse<T>(child_info, func);
}

template <class T, class F>
T ClusterNodeInner::recurse(ChildInfo& child_info, F func)
{
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_info.mem.get_addr());
    if (child_is_leaf) {
        Cluster leaf(child_info.offset + m_offset, m_alloc, m_tree_top);
        leaf.set_parent(this, child_info.ndx + s_first_node_index);
        leaf.init(child_info.mem);
        return func(&leaf, child_info);
    }
    else {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.set_parent(this, child_info.ndx + s_first_node_index);
        node.init(child_info.mem);
        node.set_offset(child_info.offset + m_offset);
        return func(&node, child_info);
    }
}

MemRef ClusterNodeInner::ensure_writeable(ObjKey key)
{
    return recurse<MemRef>(key, [](ClusterNode* node, ChildInfo& child_info) {
        return node->ensure_writeable(child_info.key);
    });
}

void ClusterNodeInner::update_ref_in_parent(ObjKey key, ref_type ref)
{
    ChildInfo child_info;
    if (!find_child(key, child_info)) {
        throw KeyNotFound("Child not found in update_ref_in_parent");
    }
    if (this->m_sub_tree_depth == 1) {
        set(child_info.ndx + s_first_node_index, ref);
    }
    else {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.set_parent(this, child_info.ndx + s_first_node_index);
        node.init(child_info.mem);
        node.set_offset(child_info.offset + m_offset);
        node.update_ref_in_parent(child_info.key, ref);
    }
}

ref_type ClusterNodeInner::insert(ObjKey key, const FieldValues& init_values, ClusterNode::State& state)
{
    return recurse<ref_type>(key, [this, &state, &init_values](ClusterNode* node, ChildInfo& child_info) {
        ref_type new_sibling_ref = node->insert(child_info.key, init_values, state);

        set_tree_size(get_tree_size() + 1);

        if (!new_sibling_ref) {
            return ref_type(0);
        }

        size_t new_ref_ndx = child_info.ndx + 1;

        int64_t split_key_value = state.split_key + child_info.offset;
        uint64_t sz = node_size();
        if (sz < cluster_node_size) {
            if (m_keys.is_attached()) {
                m_keys.insert(new_ref_ndx, split_key_value);
            }
            else {
                if (uint64_t(split_key_value) != sz << m_shift_factor) {
                    ensure_general_form();
                    m_keys.insert(new_ref_ndx, split_key_value);
                }
            }
            _insert_child_ref(new_ref_ndx, new_sibling_ref);
            return ref_type(0);
        }

        ClusterNodeInner child(m_alloc, m_tree_top);
        child.create(m_sub_tree_depth);
        if (new_ref_ndx == sz) {
            child.add(new_sibling_ref);
            state.split_key = split_key_value;
        }
        else {
            int64_t first_key_value = m_keys.get(new_ref_ndx);
            child.ensure_general_form();
            move(new_ref_ndx, &child, first_key_value);
            add(new_sibling_ref, split_key_value); // Throws
            state.split_key = first_key_value;
        }

        // Some objects has been moved out of this tree - find out how many
        size_t child_sub_tree_size = child.update_sub_tree_size();
        set_tree_size(get_tree_size() - child_sub_tree_size);

        return child.get_ref();
    });
}

bool ClusterNodeInner::try_get(ObjKey key, ClusterNode::State& state) const noexcept
{
    ChildInfo child_info;
    if (!find_child(key, child_info)) {
        return false;
    }
    return const_cast<ClusterNodeInner*>(this)->recurse<bool>(child_info,
                                                              [&state](const ClusterNode* node, ChildInfo& info) {
                                                                  return node->try_get(info.key, state);
                                                              });
}

ObjKey ClusterNodeInner::get(size_t ndx, ClusterNode::State& state) const
{
    size_t sz = node_size();
    size_t child_ndx = 0;
    while (child_ndx < sz) {
        int64_t key_offset = m_keys.is_attached() ? m_keys.get(child_ndx) : (child_ndx << m_shift_factor);

        ref_type child_ref = _get_child_ref(child_ndx);
        char* child_header = m_alloc.translate(child_ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
        size_t sub_tree_size;
        if (child_is_leaf) {
            sub_tree_size = Cluster::node_size_from_header(m_alloc, child_header);
            if (ndx < sub_tree_size) {
                Cluster leaf(key_offset + m_offset, m_alloc, m_tree_top);
                leaf.init(MemRef(child_header, child_ref, m_alloc));
                REALM_ASSERT(sub_tree_size == leaf.get_tree_size());
                return leaf.get(ndx, state);
            }
        }
        else {
            ClusterNodeInner node(m_alloc, m_tree_top);
            node.init(MemRef(child_header, child_ref, m_alloc));
            node.set_offset(key_offset + m_offset);
            sub_tree_size = node.get_tree_size();
            if (ndx < sub_tree_size) {
                return node.get(ndx, state);
            }
        }
        child_ndx++;
        ndx -= sub_tree_size;
    }
    return {};
}

size_t ClusterNodeInner::get_ndx(ObjKey key, size_t ndx) const noexcept
{
    ChildInfo child_info;
    if (!find_child(key, child_info)) {
        return realm::npos;
    }

    // First figure out how many objects there are in nodes before actual one
    // then descent in tree

    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_info.mem.get_addr());
    if (child_is_leaf) {
        for (unsigned i = 0; i < child_info.ndx; i++) {
            ref_type ref = _get_child_ref(i);
            char* header = m_alloc.translate(ref);
            ndx += Cluster::node_size_from_header(m_alloc, header);
        }
        Cluster leaf(child_info.offset + m_offset, m_alloc, m_tree_top);
        leaf.init(child_info.mem);
        return leaf.get_ndx(child_info.key, ndx);
    }
    else {
        for (unsigned i = 0; i < child_info.ndx; i++) {
            char* header = m_alloc.translate(_get_child_ref(i));
            ndx += size_t(Array::get(header, s_sub_tree_size)) >> 1;
        }
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.init(child_info.mem);
        node.set_offset(child_info.offset + m_offset);
        return node.get_ndx(child_info.key, ndx);
    }
}

void ClusterNodeInner::adjust_keys(int64_t adj)
{
    ensure_general_form();
    REALM_ASSERT(m_keys.get(0) == 0);
    m_keys.adjust(0, m_keys.size(), adj);

    // Now the first key offset value is 'adj' - it must be 0
    adjust_keys_first_child(adj);
}

void ClusterNodeInner::adjust_keys_first_child(int64_t adj)
{
    ref_type child_ref = _get_child_ref(0);
    char* child_header = m_alloc.translate(child_ref);
    auto mem = MemRef(child_header, child_ref, m_alloc);
    if (Array::get_is_inner_bptree_node_from_header(child_header)) {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.set_parent(this, s_first_node_index);
        node.init(mem);
        node.adjust_keys(adj);
    }
    else {
        Cluster node(0, m_alloc, m_tree_top);
        node.set_parent(this, s_first_node_index);
        node.init(mem);
        node.adjust_keys(adj);
    }
    m_keys.set(0, 0);
}

size_t ClusterNodeInner::erase(ObjKey key, CascadeState& state)
{
    return recurse<size_t>(key, [this, &state](ClusterNode* erase_node, ChildInfo& child_info) {
        size_t erase_node_size = erase_node->erase(child_info.key, state);
        bool is_leaf = erase_node->is_leaf();
        set_tree_size(get_tree_size() - 1);

        if (erase_node_size == 0) {
            erase_node->destroy_deep();

            ensure_general_form();
            _erase_child_ref(child_info.ndx);
            m_keys.erase(child_info.ndx);
            if (child_info.ndx == 0 && m_keys.size() > 0) {
                auto first_offset = m_keys.get(0);
                // Adjust all key values in new first node
                // We have to make sure that the first key offset value
                // in all inner nodes is 0
                adjust_keys_first_child(first_offset);
            }
        }
        else if (erase_node_size < cluster_node_size / 2 && child_info.ndx < (node_size() - 1)) {
            // Candidate for merge. First calculate if the combined size of current and
            // next sibling is small enough.
            size_t sibling_ndx = child_info.ndx + 1;
            Cluster l2(child_info.offset, m_alloc, m_tree_top);
            ClusterNodeInner n2(m_alloc, m_tree_top);
            ClusterNode* sibling_node = is_leaf ? static_cast<ClusterNode*>(&l2) : static_cast<ClusterNode*>(&n2);
            sibling_node->set_parent(this, sibling_ndx + s_first_node_index);
            sibling_node->init_from_parent();

            size_t combined_size = sibling_node->node_size() + erase_node_size;

            if (combined_size < cluster_node_size * 3 / 4) {
                // Calculate value that must be subtracted from the moved keys
                // (will be negative as the sibling has bigger keys)
                int64_t key_adj = m_keys.is_attached() ? (m_keys.get(child_info.ndx) - m_keys.get(sibling_ndx))
                                                       : 0 - (1 << m_shift_factor);
                // And then move all elements into current node
                sibling_node->ensure_general_form();
                erase_node->ensure_general_form();
                sibling_node->move(0, erase_node, key_adj);

                if (!erase_node->is_leaf()) {
                    static_cast<ClusterNodeInner*>(erase_node)->update_sub_tree_size();
                }

                // Destroy sibling
                sibling_node->destroy_deep();

                ensure_general_form();
                _erase_child_ref(sibling_ndx);
                m_keys.erase(sibling_ndx);
            }
        }

        return node_size();
    });
}

void ClusterNodeInner::nullify_incoming_links(ObjKey key, CascadeState& state)
{
    recurse<void>(key, [&state](ClusterNode* node, ChildInfo& child_info) {
        node->nullify_incoming_links(child_info.key, state);
    });
}

void ClusterNodeInner::ensure_general_form()
{
    if (!m_keys.is_attached()) {
        size_t current_size = node_size();
        m_keys.create(current_size, (current_size - 1) << m_shift_factor);
        m_keys.update_parent();
        for (size_t i = 0; i < current_size; i++) {
            m_keys.set(i, i << m_shift_factor);
        }
    }
}

void ClusterNodeInner::insert_column(ColKey col)
{
    size_t sz = node_size();
    for (size_t i = 0; i < sz; i++) {
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(this, i + s_first_node_index);
        node->insert_column(col);
    }
}

void ClusterNodeInner::remove_column(ColKey col)
{
    size_t sz = node_size();
    for (size_t i = 0; i < sz; i++) {
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(this, i + s_first_node_index);
        node->remove_column(col);
    }
}

size_t ClusterNodeInner::nb_columns() const
{
    ref_type ref = _get_child_ref(0);
    char* header = m_alloc.translate(ref);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
    MemRef mem(header, ref, m_alloc);
    if (child_is_leaf) {
        Cluster leaf(0, m_alloc, m_tree_top);
        leaf.init(mem);
        return leaf.nb_columns();
    }
    else {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.init(mem);
        return node.nb_columns();
    }
}

void ClusterNodeInner::add(ref_type ref, int64_t key_value)
{
    if (m_keys.is_attached()) {
        m_keys.add(key_value);
    }
    else {
        if (uint64_t(key_value) != (uint64_t(node_size()) << m_shift_factor)) {
            ensure_general_form();
            m_keys.add(key_value);
        }
    }
    Array::add(from_ref(ref));
}

// Find leaf that contains the object identified by key. If this does not exist return the
// leaf that contains the next object
bool ClusterNodeInner::get_leaf(ObjKey key, ClusterNode::IteratorState& state) const noexcept
{
    size_t child_ndx;
    if (m_keys.is_attached()) {
        child_ndx = m_keys.upper_bound(uint64_t(key.value));
        if (child_ndx > 0)
            child_ndx--;
    }
    else {
        REALM_ASSERT_DEBUG(node_size() > 0);
        size_t max_ndx = node_size() - 1;
        if (key.value < 0)
            child_ndx = 0;
        else
            child_ndx = std::min(size_t(key.value) >> m_shift_factor, max_ndx);
    }

    size_t sz = node_size();
    while (child_ndx < sz) {
        uint64_t key_offset = m_keys.is_attached() ? m_keys.get(child_ndx) : (child_ndx << m_shift_factor);
        ObjKey new_key(key_offset < uint64_t(key.value) ? key.value - key_offset : 0);
        state.m_key_offset += key_offset;

        ref_type child_ref = _get_child_ref(child_ndx);
        char* child_header = m_alloc.translate(child_ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
        if (child_is_leaf) {
            state.m_current_leaf.init(MemRef(child_header, child_ref, m_alloc));
            state.m_current_leaf.set_offset(state.m_key_offset);
            state.m_current_index = state.m_current_leaf.lower_bound_key(new_key);
            if (state.m_current_index < state.m_current_leaf.node_size())
                return true;
        }
        else {
            ClusterNodeInner node(m_alloc, m_tree_top);
            node.init(MemRef(child_header, child_ref, m_alloc));
            if (node.get_leaf(new_key, state))
                return true;
        }
        state.m_key_offset -= key_offset;
        child_ndx++;
    }
    return false;
}

// LCOV_EXCL_START
void ClusterNodeInner::dump_objects(int64_t key_offset, std::string lead) const
{
    std::cout << lead << "node" << std::endl;
    if (!m_keys.is_attached()) {
        std::cout << lead << "compact form" << std::endl;
    }
    size_t sz = node_size();
    for (unsigned i = 0; i < sz; i++) {
        int64_t key_value;
        if (m_keys.is_attached()) {
            key_value = m_keys.get(i) + key_offset;
        }
        else {
            key_value = (i << m_shift_factor) + key_offset;
        }
        std::cout << lead << std::hex << "split: " << key_value << std::dec << std::endl;
        m_tree_top.get_node(const_cast<ClusterNodeInner*>(this), i + s_first_node_index)
            ->dump_objects(key_value, lead + "   ");
    }
}
// LCOV_EXCL_STOP
void ClusterNodeInner::move(size_t ndx, ClusterNode* new_node, int64_t key_adj)
{
    auto new_cluster_node_inner = static_cast<ClusterNodeInner*>(new_node);
    for (size_t i = ndx; i < node_size(); i++) {
        new_cluster_node_inner->Array::add(_get_child_ref(i));
    }
    for (size_t i = ndx; i < m_keys.size(); i++) {
        new_cluster_node_inner->m_keys.add(m_keys.get(i) - key_adj);
    }
    truncate(ndx + s_first_node_index);
    if (m_keys.is_attached()) {
        m_keys.truncate(ndx);
    }
}

size_t ClusterNodeInner::update_sub_tree_size()
{
    size_t sub_tree_size = 0;
    auto sz = node_size();

    for (unsigned i = 0; i < sz; i++) {
        ref_type ref = _get_child_ref(i);
        char* header = m_alloc.translate(ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
        if (child_is_leaf) {
            sub_tree_size += Cluster::node_size_from_header(m_alloc, header);
        }
        else {
            sub_tree_size += size_t(Array::get(header, s_sub_tree_size)) >> 1;
        }
    }
    set_tree_size(sub_tree_size);
    return sub_tree_size;
}

bool ClusterNodeInner::traverse(ClusterTree::TraverseFunction func, int64_t key_offset) const
{
    auto sz = node_size();

    for (unsigned i = 0; i < sz; i++) {
        ref_type ref = _get_child_ref(i);
        char* header = m_alloc.translate(ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
        MemRef mem(header, ref, m_alloc);
        int64_t offs = (m_keys.is_attached() ? m_keys.get(i) : i << m_shift_factor) + key_offset;
        if (child_is_leaf) {
            Cluster leaf(offs, m_alloc, m_tree_top);
            leaf.init(mem);
            if (func(&leaf)) {
                return true;
            }
        }
        else {
            ClusterNodeInner node(m_alloc, m_tree_top);
            node.init(mem);
            if (node.traverse(func, offs)) {
                return true;
            }
        }
    }
    return false;
}

void ClusterNodeInner::update(ClusterTree::UpdateFunction func, int64_t key_offset)
{
    auto sz = node_size();

    for (unsigned i = 0; i < sz; i++) {
        ref_type ref = _get_child_ref(i);
        char* header = m_alloc.translate(ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
        MemRef mem(header, ref, m_alloc);
        int64_t offs = (m_keys.is_attached() ? m_keys.get(i) : i << m_shift_factor) + key_offset;
        if (child_is_leaf) {
            Cluster leaf(offs, m_alloc, m_tree_top);
            leaf.init(mem);
            leaf.set_parent(this, i + s_first_node_index);
            func(&leaf);
        }
        else {
            ClusterNodeInner node(m_alloc, m_tree_top);
            node.init(mem);
            node.set_parent(this, i + s_first_node_index);
            node.update(func, offs);
        }
    }
}

int64_t ClusterNodeInner::get_last_key_value() const
{
    auto last_ndx = node_size() - 1;

    ref_type ref = _get_child_ref(last_ndx);
    char* header = m_alloc.translate(ref);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
    MemRef mem(header, ref, m_alloc);
    int64_t offset = m_keys.is_attached() ? m_keys.get(last_ndx) : last_ndx << m_shift_factor;
    if (child_is_leaf) {
        Cluster leaf(offset, m_alloc, m_tree_top);
        leaf.init(mem);
        return offset + leaf.get_last_key_value();
    }
    else {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.init(mem);
        return offset + node.get_last_key_value();
    }
}

ClusterTree::ClusterTree(Allocator& alloc)
    : m_alloc(alloc)
{
}

ClusterTree::~ClusterTree() {}

size_t ClusterTree::size_from_ref(ref_type ref, Allocator& alloc)
{
    size_t ret = 0;
    if (ref) {
        Array arr(alloc);
        arr.init_from_ref(ref);
        if (arr.is_inner_bptree_node()) {
            ret = size_t(arr.get(2)) >> 1;
        }
        else {
            int64_t rot = arr.get(0);
            if (rot & 1) {
                ret = size_t(rot) >> 1;
            }
            else {
                ref_type key_ref = to_ref(rot);
                MemRef mem(key_ref, alloc);
                auto header = mem.get_addr();
                ret = Node::get_size_from_header(header);
            }
        }
    }
    return ret;
}

std::unique_ptr<ClusterNode> ClusterTree::create_root_from_parent(ArrayParent* parent, size_t ndx_in_parent)
{
    ref_type ref = parent->get_child_ref(ndx_in_parent);
    if (!ref)
        return nullptr;

    MemRef mem{m_alloc.translate(ref), ref, m_alloc};
    const char* header = mem.get_addr();
    bool is_leaf = !Array::get_is_inner_bptree_node_from_header(header);

    bool can_reuse_root_accessor = m_root && m_root->is_leaf() == is_leaf;
    if (can_reuse_root_accessor) {
        m_root->init(mem);
        return std::move(m_root); // Same root will be reinstalled.
    }

    // Not reusing root note, allocating a new one.
    std::unique_ptr<ClusterNode> new_root;
    if (is_leaf) {
        new_root = std::make_unique<Cluster>(0, m_alloc, *this);
    }
    else {
        new_root = std::make_unique<ClusterNodeInner>(m_alloc, *this);
    }
    new_root->init(mem);
    new_root->set_parent(parent, ndx_in_parent);

    return new_root;
}

std::unique_ptr<ClusterNode> ClusterTree::get_node(ArrayParent* parent, size_t ndx_in_parent) const
{
    ref_type ref = parent->get_child_ref(ndx_in_parent);

    std::unique_ptr<ClusterNode> node;

    char* child_header = static_cast<char*>(m_alloc.translate(ref));
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        node = std::make_unique<Cluster>(0, m_alloc, *this);
    }
    else {
        node = std::make_unique<ClusterNodeInner>(m_alloc, *this);
    }
    node->init(MemRef(child_header, ref, m_alloc));
    node->set_parent(parent, ndx_in_parent);

    return node;
}

void ClusterTree::clear()
{
    m_root->destroy_deep();

    auto leaf = std::make_unique<Cluster>(0, m_root->get_alloc(), *this);
    leaf->create();
    replace_root(std::move(leaf));

    bump_content_version();
    bump_storage_version();

    m_size = 0;
}

void ClusterTree::replace_root(std::unique_ptr<ClusterNode> new_root)
{
    if (new_root != m_root) {
        // Maintain parent.
        new_root->set_parent(m_root->get_parent(), m_root->get_ndx_in_parent());
        new_root->update_parent(); // Throws
        m_root = std::move(new_root);
    }
}

bool ClusterTree::init_from_parent()
{
    m_root = get_root_from_parent();
    if (m_root) {
        m_size = m_root->get_tree_size();
        return true;
    }
    m_size = 0;
    return false;
}

void ClusterTree::update_from_parent() noexcept
{
    m_root->update_from_parent();
    m_size = m_root->get_tree_size();
}

void ClusterTree::insert_fast(ObjKey k, const FieldValues& init_values, ClusterNode::State& state)
{
    ref_type new_sibling_ref = m_root->insert(k, init_values, state);
    if (REALM_UNLIKELY(new_sibling_ref)) {
        auto new_root = std::make_unique<ClusterNodeInner>(m_root->get_alloc(), *this);
        new_root->create(m_root->get_sub_tree_depth() + 1);

        new_root->add(m_root->get_ref());                // Throws
        new_root->add(new_sibling_ref, state.split_key); // Throws
        new_root->update_sub_tree_size();

        replace_root(std::move(new_root));
    }
    m_size++;
}

ClusterNode::State ClusterTree::insert(ObjKey k, const FieldValues& values)
{
    ClusterNode::State state;
    FieldValues init_values(values);

    // Sort ColKey according to index
    std::sort(init_values.begin(), init_values.end(), [](auto& a, auto& b) {
        return a.col_key.get_index().val < b.col_key.get_index().val;
    });

    insert_fast(k, init_values, state);
    update_indexes(k, init_values);

    bump_content_version();
    bump_storage_version();

    return state;
}

bool ClusterTree::is_valid(ObjKey k) const noexcept
{
    if (m_size == 0)
        return false;

    ClusterNode::State state;
    return m_root->try_get(k, state);
}

ClusterNode::State ClusterTree::get(ObjKey k) const
{
    ClusterNode::State state;
    m_root->get(k, state);
    return state;
}

ClusterNode::State ClusterTree::try_get(ObjKey k) const noexcept
{
    ClusterNode::State state;
    if (!(k && m_root->try_get(k, state)))
        state.index = realm::npos;
    return state;
}

ClusterNode::State ClusterTree::get(size_t ndx, ObjKey& k) const
{
    if (ndx >= m_size) {
        throw std::out_of_range("Object was deleted");
    }
    ClusterNode::State state;
    k = m_root->get(ndx, state);
    return state;
}

size_t ClusterTree::get_ndx(ObjKey k) const noexcept
{
    return m_root->get_ndx(k, 0);
}

void ClusterTree::erase(ObjKey k, CascadeState& state)
{
    cleanup_key(k);
    if (!k.is_unresolved()) {
        if (auto table = get_owning_table()) {
            if (Replication* repl = table->get_repl()) {
                repl->remove_object(table, k);
            }
        }
    }

    size_t root_size = m_root->erase(k, state);

    bump_content_version();
    bump_storage_version();
    m_size--;
    while (!m_root->is_leaf() && root_size == 1) {
        ClusterNodeInner* node = static_cast<ClusterNodeInner*>(m_root.get());

        REALM_ASSERT(node->get_first_key_value() == 0);
        auto new_root = node->return_and_clear_first_child();
        node->destroy_deep();

        replace_root(std::move(new_root));
        root_size = m_root->node_size();
    }
}

bool ClusterTree::get_leaf(ObjKey key, ClusterNode::IteratorState& state) const noexcept
{
    state.clear();

    if (m_root->is_leaf()) {
        Cluster* node = static_cast<Cluster*>(m_root.get());
        REALM_ASSERT_DEBUG(node->get_offset() == 0);
        state.m_key_offset = 0;
        state.m_current_leaf.init(node->get_mem());
        state.m_current_leaf.set_offset(state.m_key_offset);
        state.m_current_index = node->lower_bound_key(key);
        return state.m_current_index < state.m_current_leaf.node_size();
    }
    else {
        ClusterNodeInner* node = static_cast<ClusterNodeInner*>(m_root.get());
        return node->get_leaf(key, state);
    }
}

bool ClusterTree::traverse(TraverseFunction func) const
{
    if (m_root->is_leaf()) {
        return func(static_cast<Cluster*>(m_root.get()));
    }
    else {
        return static_cast<ClusterNodeInner*>(m_root.get())->traverse(func, 0);
    }
}

void ClusterTree::update(UpdateFunction func)
{
    if (m_root->is_leaf()) {
        func(static_cast<Cluster*>(m_root.get()));
    }
    else {
        static_cast<ClusterNodeInner*>(m_root.get())->update(func, 0);
    }
}

void ClusterTree::verify() const
{
#ifdef REALM_DEBUG
    traverse([](const Cluster* cluster) {
        cluster->verify();
        return false;
    });
#endif
}

void ClusterTree::nullify_links(ObjKey obj_key, CascadeState& state)
{
    REALM_ASSERT(state.m_group);
    m_root->nullify_incoming_links(obj_key, state);
}

/**************************  ClusterTree::Iterator  **************************/

ClusterTree::Iterator::Iterator(const ClusterTree& t, size_t ndx)
    : m_tree(t)
    , m_leaf(0, t.get_alloc(), t)
    , m_state(m_leaf)
    , m_instance_version(t.get_instance_version())
    , m_leaf_invalid(false)
    , m_position(ndx)
{
    auto sz = t.size();
    if (ndx >= sz) {
        // end
        m_position = sz;
        m_leaf_invalid = true;
    }
    else if (ndx == 0) {
        // begin
        m_key = load_leaf(ObjKey(0));
        m_leaf_start_pos = 0;
    }
    else {
        auto s = const_cast<ClusterTree&>(m_tree).get(ndx, m_key);
        m_state.init(s, m_key);
        m_leaf_start_pos = ndx - m_state.m_current_index;
    }
}

ClusterTree::Iterator::Iterator(const Iterator& other)
    : m_tree(other.m_tree)
    , m_leaf(0, m_tree.get_alloc(), m_tree)
    , m_state(m_leaf)
    , m_instance_version(m_tree.get_instance_version())
    , m_key(other.m_key)
    , m_leaf_invalid(other.m_leaf_invalid)
    , m_position(other.m_position)
{
    m_leaf_start_pos = m_position - m_state.m_current_index;
}

size_t ClusterTree::Iterator::get_position()
{
    auto ndx = m_tree.get_ndx(m_key);
    if (ndx == realm::npos) {
        throw std::logic_error("Outdated iterator");
    }
    return ndx;
}

ObjKey ClusterTree::Iterator::load_leaf(ObjKey key) const
{
    m_storage_version = m_tree.get_storage_version(m_instance_version);
    // 'key' may or may not exist. If it does not exist, state is updated
    // to point to the next object in line.
    if (m_tree.get_leaf(key, m_state)) {
        m_leaf_start_pos = m_position - m_state.m_current_index;
        // Get the actual key value
        return m_leaf.get_real_key(m_state.m_current_index);
    }
    else {
        // end of table
        return null_key;
    }
}

void ClusterTree::Iterator::go(size_t abs_pos)
{
    size_t sz = m_tree.size();
    if (abs_pos >= sz) {
        throw std::out_of_range("Index out of range");
    }

    m_position = abs_pos;

    // If the position is within the current leaf then just set the iterator to that position
    if (!m_leaf_invalid && m_storage_version == m_tree.get_storage_version(m_instance_version)) {
        if (abs_pos >= m_leaf_start_pos && abs_pos < (m_leaf_start_pos + m_leaf.node_size())) {
            m_state.m_current_index = abs_pos - m_leaf_start_pos;
            m_key = m_leaf.get_real_key(m_state.m_current_index);
            return;
        }
    }

    // Find cluster holding requested position
    auto s = m_tree.get(abs_pos, m_key);
    m_state.init(s, m_key);
    m_leaf_start_pos = abs_pos - s.index;
    m_leaf_invalid = false;
}

bool ClusterTree::Iterator::update() const
{
    if (m_leaf_invalid || m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        ObjKey k = load_leaf(m_key);
        m_leaf_invalid = !k || (k != m_key);
        if (m_leaf_invalid) {
            throw std::logic_error("Outdated iterator");
        }
        return true;
    }

    REALM_ASSERT(m_leaf.is_attached());
    return false;
}

ClusterTree::Iterator& ClusterTree::Iterator::operator++()
{
    if (m_leaf_invalid || m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        ObjKey k = load_leaf(m_key);
        if (k != m_key) {
            // Objects was deleted. k points to the next object
            m_key = k;
            m_leaf_invalid = !m_key;
            return *this;
        }
    }

    m_state.m_current_index++;
    m_position++;
    if (m_state.m_current_index == m_leaf.node_size()) {
        m_key = load_leaf(ObjKey(m_key.value + 1));
        m_leaf_invalid = !m_key;
    }
    else {
        m_key = m_leaf.get_real_key(m_state.m_current_index);
    }
    return *this;
}

ClusterTree::Iterator& ClusterTree::Iterator::operator+=(ptrdiff_t adj)
{
    // If you have to jump far away and thus have to load many leaves,
    // this function will be slow
    REALM_ASSERT(adj >= 0);
    if (adj == 0) {
        return *this;
    }

    size_t n = size_t(adj);
    if (m_leaf_invalid || m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        ObjKey k = load_leaf(m_key);
        if (k != m_key) {
            // Objects was deleted. k points to the next object
            m_key = k;
            m_position = m_key ? m_tree.get_ndx(m_key) : m_tree.size();
            n--;
        }
    }
    if (n > 0) {
        m_position += n;
        size_t left_in_leaf = m_leaf.node_size() - m_state.m_current_index;
        if (n < left_in_leaf) {
            m_state.m_current_index += n;
            m_key = m_leaf.get_real_key(m_state.m_current_index);
            n = 0;
        }
        else {
            if (m_position < m_tree.size()) {
                auto s = const_cast<ClusterTree&>(m_tree).get(m_position, m_key);
                m_state.init(s, m_key);
                m_leaf_start_pos = m_position - m_state.m_current_index;
            }
            else {
                m_key = ObjKey();
                m_position = m_tree.size();
            }
        }
    }
    m_leaf_invalid = !m_key;
    return *this;
}

} // namespace realm
