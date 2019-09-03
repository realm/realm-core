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

#include "realm/cluster_tree.hpp"
#include "realm/table.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_basic.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_key.hpp"
#include "realm/array_backlink.hpp"
#include "realm/index_string.hpp"
#include "realm/column_type_traits.hpp"
#include "realm/replication.hpp"
#include <iostream>
#include <cmath>

using namespace realm;

namespace {
#if REALM_MAX_BPNODE_SIZE > 256
constexpr int node_shift_factor = 8;
#else
constexpr int node_shift_factor = 2;
#endif

constexpr size_t cluster_node_size = 1 << node_shift_factor;
}

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
    bool update_from_parent(size_t old_baseline) noexcept override;
    MemRef ensure_writeable(ObjKey k) override;

    bool is_leaf() const override
    {
        return false;
    }

    int get_sub_tree_depth() const override
    {
        return m_sub_tree_depth;
    }

    bool traverse(ClusterTree::TraverseFunction& func, int64_t) const;
    void update(ClusterTree::UpdateFunction& func, int64_t);

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
    ref_type insert(ObjKey k, const FieldValues& init_values, State& state) override;
    bool try_get(ObjKey k, State& state) const override;
    ObjKey get(size_t ndx, State& state) const override;
    size_t get_ndx(ObjKey key, size_t ndx) const override;
    size_t erase(ObjKey k, CascadeState& state) override;
    void add(ref_type ref, int64_t key_value = 0);

    // Reset first (and only!) child ref and return the previous value
    ref_type clear_first_child_ref()
    {
        REALM_ASSERT(node_size() == 1);
        ref_type ret = Array::get_as_ref(s_first_node_index);
        Array::set(s_first_node_index, 0);
        return ret;
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
    bool find_child(ObjKey key, ChildInfo& ret) const
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

    ref_type _get_child_ref(size_t ndx) const
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
};
}

void ClusterNode::IteratorState::clear()
{
    m_current_leaf.detach();
    m_key_offset = 0;
    m_current_index = size_t(-1);
}

void ClusterNode::get(ObjKey k, ClusterNode::State& state) const
{
    if (!k || !try_get(k, state)) {
        throw InvalidKey("Key not found");
    }
}

/***************************** ClusterNodeInner ******************************/

ClusterNodeInner::ClusterNodeInner(Allocator& allocator, const ClusterTree& tree_top)
    : ClusterNode(0, allocator, tree_top)
{
}

ClusterNodeInner::~ClusterNodeInner()
{
}

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

bool ClusterNodeInner::update_from_parent(size_t old_baseline) noexcept
{
    if (Array::update_from_parent(old_baseline)) {
        ref_type ref = Array::get_as_ref(s_key_ref_index);
        if (ref) {
            m_keys.update_from_parent(old_baseline);
        }
        m_sub_tree_depth = int(Array::get(s_sub_tree_depth_index)) >> 1;
        return true;
    }
    return false;
}

template <class T, class F>
T ClusterNodeInner::recurse(ObjKey key, F func)
{
    ChildInfo child_info;
    if (!find_child(key, child_info)) {
        throw InvalidKey("Key not found");
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
    return recurse<MemRef>(
        key, [](ClusterNode* node, ChildInfo& child_info) { return node->ensure_writeable(child_info.key); });
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
        size_t sz = node_size();
        if (sz < cluster_node_size) {
            if (m_keys.is_attached()) {
                m_keys.insert(new_ref_ndx, split_key_value);
            }
            else {
                if (size_t(split_key_value) != sz << m_shift_factor) {
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

bool ClusterNodeInner::try_get(ObjKey key, ClusterNode::State& state) const
{
    ChildInfo child_info;
    if (!find_child(key, child_info)) {
        return false;
    }
    return const_cast<ClusterNodeInner*>(this)->recurse<bool>(
        child_info, [&state](const ClusterNode* node, ChildInfo& info) { return node->try_get(info.key, state); });
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
            Cluster leaf(key_offset + m_offset, m_alloc, m_tree_top);
            leaf.init(MemRef(child_header, child_ref, m_alloc));
            sub_tree_size = leaf.get_tree_size();
            if (ndx < sub_tree_size) {
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

size_t ClusterNodeInner::get_ndx(ObjKey key, size_t ndx) const
{
    ChildInfo child_info;
    if (!find_child(key, child_info)) {
        throw InvalidKey("Key not found");
    }

    // First figure out how many objects there are in nodes before actual one
    // then descent in tree

    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_info.mem.get_addr());
    if (child_is_leaf) {
        for (unsigned i = 0; i < child_info.ndx; i++) {
            ref_type ref = _get_child_ref(i);
            char* header = m_alloc.translate(ref);
            MemRef mem(header, ref, m_alloc);
            Cluster leaf(0, m_alloc, m_tree_top);
            leaf.init(mem);
            ndx += leaf.get_tree_size();
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

size_t ClusterNodeInner::erase(ObjKey key, CascadeState& state)
{
    return recurse<size_t>(key, [this, &state](ClusterNode* erase_node, ChildInfo& child_info) {
        size_t erase_node_size = erase_node->erase(child_info.key, state);

        set_tree_size(get_tree_size() - 1);

        if (erase_node_size == 0) {
            erase_node->destroy_deep();

            ensure_general_form();
            _erase_child_ref(child_info.ndx);
            m_keys.erase(child_info.ndx);
        }
        else if (erase_node_size < cluster_node_size / 2 && child_info.ndx < (node_size() - 1)) {
            // Candidate for merge. First calculate if the combined size of current and
            // next sibling is small enough.
            size_t sibling_ndx = child_info.ndx + 1;
            Cluster l2(child_info.offset, m_alloc, m_tree_top);
            ClusterNodeInner n2(m_alloc, m_tree_top);
            ClusterNode* sibling_node =
                erase_node->is_leaf() ? static_cast<ClusterNode*>(&l2) : static_cast<ClusterNode*>(&n2);
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
        ref_type child_ref = _get_child_ref(i);
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(child_ref);
        node->set_parent(this, i + s_first_node_index);
        node->insert_column(col);
    }
}

void ClusterNodeInner::remove_column(ColKey col)
{
    size_t sz = node_size();
    for (size_t i = 0; i < sz; i++) {
        ref_type child_ref = _get_child_ref(i);
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(child_ref);
        node->set_parent(this, i + s_first_node_index);
        node->remove_column(col);
    }
}

void ClusterNodeInner::add(ref_type ref, int64_t key_value)
{
    if (m_keys.is_attached()) {
        m_keys.add(key_value);
    }
    else {
        if (size_t(key_value) != (node_size() << m_shift_factor)) {
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
        int64_t key_offset = m_keys.is_attached() ? m_keys.get(child_ndx) : (child_ndx << m_shift_factor);
        ObjKey new_key(key_offset < key.value ? key.value - key_offset : 0);
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
        m_tree_top.get_node(_get_child_ref(i))->dump_objects(key_value, lead + "   ");
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
        MemRef mem(header, ref, m_alloc);
        if (child_is_leaf) {
            Cluster leaf(0, m_alloc, m_tree_top);
            leaf.init(mem);
            sub_tree_size += leaf.get_tree_size();
        }
        else {
            sub_tree_size += size_t(Array::get(header, s_sub_tree_size)) >> 1;
        }
    }
    set_tree_size(sub_tree_size);
    return sub_tree_size;
}

bool ClusterNodeInner::traverse(ClusterTree::TraverseFunction& func, int64_t key_offset) const
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

void ClusterNodeInner::update(ClusterTree::UpdateFunction& func, int64_t key_offset)
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

/********************************* Cluster ***********************************/

template <class T>
inline void Cluster::do_create(ColKey col)
{
    T arr(m_alloc);
    arr.create();
    auto col_ndx = col.get_index();
    arr.set_parent(this, col_ndx.val + s_first_col_index);
    arr.update_parent();
}

void Cluster::create(size_t nb_leaf_columns)
{
    // Create array with the required size
    Array::create(type_HasRefs, false, nb_leaf_columns + s_first_col_index);
    Array::set(0, RefOrTagged::make_tagged(0));
    auto table = m_tree_top.get_owner();
    auto column_initialize = [this](ColKey col_key) {
        auto col_ndx = col_key.get_index();
        auto type = col_key.get_type();
        auto attr = col_key.get_attrs();
        if (attr.test(col_attr_List)) {
            ArrayInteger arr(m_alloc);
            arr.create(type_HasRefs);
            arr.set_parent(this, col_ndx.val + s_first_col_index);
            arr.update_parent();
            return false;
        }
        switch (type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_create<ArrayIntNull>(col_key);
                }
                else {
                    do_create<ArrayInteger>(col_key);
                }
                break;
            case col_type_Bool:
                do_create<ArrayBoolNull>(col_key);
                break;
            case col_type_Float:
                do_create<ArrayFloatNull>(col_key);
                break;
            case col_type_Double:
                do_create<ArrayDoubleNull>(col_key);
                break;
            case col_type_String: {
                size_t spec_ndx = m_tree_top.get_owner()->leaf_ndx2spec_ndx(col_ndx);
                if (m_tree_top.get_spec().is_string_enum_type(spec_ndx)) {
                    do_create<ArrayInteger>(col_key);
                }
                else {
                    do_create<ArrayString>(col_key);
                }
                break;
            }
            case col_type_Binary:
                do_create<ArrayBinary>(col_key);
                break;
            case col_type_Timestamp:
                do_create<ArrayTimestamp>(col_key);
                break;
            case col_type_Link:
                do_create<ArrayKey>(col_key);
                break;
            case col_type_BackLink:
                do_create<ArrayBacklink>(col_key);
                break;
            default:
                throw LogicError(LogicError::illegal_type);
                break;
        }
        return false;
    };
    table->for_each_and_every_column(column_initialize);
}

void Cluster::init(MemRef mem)
{
    Array::init_from_mem(mem);
    auto rot = Array::get_as_ref_or_tagged(0);
    if (rot.is_tagged()) {
        m_keys.detach();
    }
    else {
        m_keys.init_from_ref(rot.get_as_ref());
    }
}

bool Cluster::update_from_parent(size_t old_baseline) noexcept
{
    if (Array::update_from_parent(old_baseline)) {
        auto rot = Array::get_as_ref_or_tagged(0);
        if (!rot.is_tagged()) {
            m_keys.update_from_parent(old_baseline);
        }
        return true;
    }
    return false;
}

MemRef Cluster::ensure_writeable(ObjKey)
{
    copy_on_write();
    return get_mem();
}

namespace realm {

template <class T>
inline void Cluster::set_spec(T&, ColKey::Idx)
{
}

template <>
inline void Cluster::set_spec(ArrayString& arr, ColKey::Idx col_ndx)
{
    auto spec_ndx = m_tree_top.get_owner()->leaf_ndx2spec_ndx(col_ndx);
    arr.set_spec(const_cast<Spec*>(&m_tree_top.get_spec()), spec_ndx);
}
} // namespace realm

template <class T>
inline void Cluster::do_insert_row(size_t ndx, ColKey col, Mixed init_val, bool nullable)
{
    using U = typename util::RemoveOptional<typename T::value_type>::type;

    T arr(m_alloc);
    auto col_ndx = col.get_index();
    arr.set_parent(this, col_ndx.val + s_first_col_index);
    set_spec<T>(arr, col_ndx);
    arr.init_from_parent();
    if (init_val.is_null()) {
        arr.insert(ndx, T::default_value(nullable));
    }
    else {
        arr.insert(ndx, init_val.get<U>());
    }
}

inline void Cluster::do_insert_key(size_t ndx, ColKey col_key, Mixed init_val, ObjKey origin_key)
{
    ObjKey key = init_val.is_null() ? ObjKey{} : init_val.get<ObjKey>();
    ArrayKey arr(m_alloc);
    auto col_ndx = col_key.get_index();
    arr.set_parent(this, col_ndx.val + s_first_col_index);
    arr.init_from_parent();
    arr.insert(ndx, key);

    // Insert backlink if link is not null
    if (key) {
        const Table* origin_table = m_tree_top.get_owner();
        TableRef opp_table = origin_table->get_opposite_table(col_key);
        ColKey opp_col = origin_table->get_opposite_column(col_key);
        Obj target_obj = opp_table->get_object(key);
        target_obj.add_backlink(opp_col, origin_key);
    }
}

void Cluster::insert_row(size_t ndx, ObjKey k, const FieldValues& init_values)
{
    if (m_keys.is_attached()) {
        m_keys.insert(ndx, k.value);
    }
    else {
        Array::set(s_key_ref_or_size_index, Array::get(s_key_ref_or_size_index) + 2); // Increments size by 1
    }

    auto val = init_values.begin();
    auto table = m_tree_top.get_owner();
    auto insert_in_column = [&](ColKey col_key) {
        auto col_ndx = col_key.get_index();
        auto attr = col_key.get_attrs();
        Mixed init_value;
        // init_values must be sorted in col_ndx order - this is ensured by ClustTree::insert()
        if (val != init_values.end() && val->col_key.get_index().val == col_ndx.val) {
            init_value = val->value;
            ++val;
        }

        if (attr.test(col_attr_List)) {
            REALM_ASSERT(init_value.is_null());
            ArrayInteger arr(m_alloc);
            arr.set_parent(this, col_ndx.val + s_first_col_index);
            arr.init_from_parent();
            arr.insert(ndx, 0);
            return false;
        }

        bool nullable = attr.test(col_attr_Nullable);
        auto type = col_key.get_type();
        switch (type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_insert_row<ArrayIntNull>(ndx, col_key, init_value, nullable);
                }
                else {
                    do_insert_row<ArrayInteger>(ndx, col_key, init_value, nullable);
                }
                break;
            case col_type_Bool:
                do_insert_row<ArrayBoolNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Float:
                do_insert_row<ArrayFloatNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Double:
                do_insert_row<ArrayDoubleNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_String:
                do_insert_row<ArrayString>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Binary:
                do_insert_row<ArrayBinary>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Timestamp:
                do_insert_row<ArrayTimestamp>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Link:
                do_insert_key(ndx, col_key, init_value, ObjKey(k.value + get_offset()));
                break;
            case col_type_BackLink: {
                ArrayBacklink arr(m_alloc);
                arr.set_parent(this, col_ndx.val + s_first_col_index);
                arr.init_from_parent();
                arr.insert(ndx, 0);
                break;
            }
            default:
                REALM_ASSERT(false);
                break;
        }
        return false;
    };
    table->for_each_and_every_column(insert_in_column);
}

template <class T>
inline void Cluster::do_move(size_t ndx, ColKey col_key, Cluster* to)
{
    auto col_ndx = col_key.get_index().val + s_first_col_index;
    T src(m_alloc);
    src.set_parent(this, col_ndx);
    src.init_from_parent();

    T dst(m_alloc);
    dst.set_parent(to, col_ndx);
    dst.init_from_parent();

    src.move(dst, ndx);
}

void Cluster::move(size_t ndx, ClusterNode* new_node, int64_t offset)
{
    auto new_leaf = static_cast<Cluster*>(new_node);

    auto move_from_column = [&](ColKey col_key) {
        auto attr = col_key.get_attrs();
        auto type = col_key.get_type();

        if (attr.test(col_attr_List)) {
            do_move<ArrayInteger>(ndx, col_key, new_leaf);
            return false;
        }

        switch (type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_move<ArrayIntNull>(ndx, col_key, new_leaf);
                }
                else {
                    do_move<ArrayInteger>(ndx, col_key, new_leaf);
                }
                break;
            case col_type_Bool:
                do_move<ArrayBoolNull>(ndx, col_key, new_leaf);
                break;
            case col_type_Float:
                do_move<ArrayFloat>(ndx, col_key, new_leaf);
                break;
            case col_type_Double:
                do_move<ArrayDouble>(ndx, col_key, new_leaf);
                break;
            case col_type_String: {
                const Spec& spec = m_tree_top.get_spec();
                size_t spec_ndx = m_tree_top.get_owner()->leaf_ndx2spec_ndx(col_key.get_index());
                if (spec.is_string_enum_type(spec_ndx))
                    do_move<ArrayInteger>(ndx, col_key, new_leaf);
                else
                    do_move<ArrayString>(ndx, col_key, new_leaf);
                break;
            }
            case col_type_Binary:
                do_move<ArrayBinary>(ndx, col_key, new_leaf);
                break;
            case col_type_Timestamp:
                do_move<ArrayTimestamp>(ndx, col_key, new_leaf);
                break;
            case col_type_Link:
                do_move<ArrayKey>(ndx, col_key, new_leaf);
                break;
            case col_type_BackLink:
                do_move<ArrayBacklink>(ndx, col_key, new_leaf);
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
        return false;
    };
    m_tree_top.get_owner()->for_each_and_every_column(move_from_column);
    for (size_t i = ndx; i < m_keys.size(); i++) {
        new_leaf->m_keys.add(m_keys.get(i) - offset);
    }
    m_keys.truncate(ndx);
}

Cluster::~Cluster()
{
}

void Cluster::ensure_general_form()
{
    if (!m_keys.is_attached()) {
        size_t current_size = get_size_in_compact_form();
        m_keys.create(current_size, 255);
        m_keys.update_parent();
        for (size_t i = 0; i < current_size; i++) {
            m_keys.set(i, i);
        }
    }
}

template <class T>
inline void Cluster::do_insert_column(ColKey col_key, bool nullable)
{
    size_t sz = node_size();

    T arr(m_alloc);
    arr.create();
    auto val = T::default_value(nullable);
    for (size_t i = 0; i < sz; i++) {
        arr.add(val);
    }
    auto col_ndx = col_key.get_index();
    unsigned ndx = col_ndx.val + s_first_col_index;
    if (ndx == size())
        Array::insert(ndx, from_ref(arr.get_ref()));
    else
        Array::set(ndx, from_ref(arr.get_ref()));
}

void Cluster::insert_column(ColKey col_key)
{
    auto attr = col_key.get_attrs();
    if (attr.test(col_attr_List)) {
        size_t sz = node_size();

        ArrayInteger arr(m_alloc);
        arr.Array::create(type_HasRefs, false, sz, 0);
        auto col_ndx = col_key.get_index();
        unsigned idx = col_ndx.val + s_first_col_index;
        if (idx == size())
            Array::insert(idx, from_ref(arr.get_ref()));
        else
            Array::set(idx, from_ref(arr.get_ref()));
        return;
    }
    bool nullable = attr.test(col_attr_Nullable);
    auto type = col_key.get_type();
    switch (type) {
        case col_type_Int:
            if (nullable) {
                do_insert_column<ArrayIntNull>(col_key, nullable);
            }
            else {
                do_insert_column<ArrayInteger>(col_key, nullable);
            }
            break;
        case col_type_Bool:
            do_insert_column<ArrayBoolNull>(col_key, nullable);
            break;
        case col_type_Float:
            do_insert_column<ArrayFloatNull>(col_key, nullable);
            break;
        case col_type_Double:
            do_insert_column<ArrayDoubleNull>(col_key, nullable);
            break;
        case col_type_String:
            do_insert_column<ArrayString>(col_key, nullable);
            break;
        case col_type_Binary:
            do_insert_column<ArrayBinary>(col_key, nullable);
            break;
        case col_type_Timestamp:
            do_insert_column<ArrayTimestamp>(col_key, nullable);
            break;
        case col_type_Link:
            do_insert_column<ArrayKey>(col_key, nullable);
            break;
        case col_type_BackLink:
            do_insert_column<ArrayBacklink>(col_key, nullable);
            break;
        default:
            throw LogicError(LogicError::illegal_type);
            break;
    }
}

void Cluster::remove_column(ColKey col_key)
{
    auto col_ndx = col_key.get_index();
    unsigned idx = col_ndx.val + s_first_col_index;
    ref_type ref = to_ref(Array::get(idx));
    if (ref != 0) {
        Array::destroy_deep(ref, m_alloc);
    }
    if (idx == size() - 1)
        Array::erase(idx);
    else
        Array::set(idx, 0);
}

ref_type Cluster::insert(ObjKey k, const FieldValues& init_values, ClusterNode::State& state)
{
    int64_t current_key_value = -1;
    size_t sz;
    size_t ndx;

    if (m_keys.is_attached()) {
        sz = m_keys.size();
        ndx = m_keys.lower_bound(uint64_t(k.value));
        if (ndx < sz) {
            current_key_value = m_keys.get(ndx);
            if (k.value == current_key_value) {
                throw InvalidKey("Key already used");
            }
        }
    }
    else {
        sz = size_t(Array::get(s_key_ref_or_size_index)) >> 1; // Size is stored as tagged integer
        if (k.value < int(sz)) {
            throw InvalidKey("Key already used");
        }
        // Key value is bigger than all other values, should be put last
        ndx = sz;
        if (k.value > int(sz)) {
            ensure_general_form();
        }
    }

    ref_type ret = 0;

    REALM_ASSERT_DEBUG(sz <= cluster_node_size);
    if (REALM_LIKELY(sz < cluster_node_size)) {
        insert_row(ndx, k, init_values); // Throws
        state.mem = get_mem();
        state.index = ndx;
    }
    else {
        // Split leaf node
        Cluster new_leaf(0, m_alloc, m_tree_top);
        new_leaf.create(size() - 1);
        if (ndx == sz) {
            new_leaf.insert_row(0, ObjKey(0), init_values); // Throws
            state.split_key = k.value;
            state.mem = new_leaf.get_mem();
            state.index = 0;
        }
        else {
            // Current cluster must be in general form to get here
            REALM_ASSERT_DEBUG(m_keys.is_attached());
            new_leaf.ensure_general_form();
            move(ndx, &new_leaf, current_key_value);
            insert_row(ndx, k, init_values); // Throws
            state.mem = get_mem();
            state.split_key = current_key_value;
            state.index = ndx;
        }
        ret = new_leaf.get_ref();
    }

    return ret;
}

bool Cluster::try_get(ObjKey k, ClusterNode::State& state) const
{
    state.mem = get_mem();
    if (m_keys.is_attached()) {
        state.index = m_keys.lower_bound(uint64_t(k.value));
        return state.index != m_keys.size() && m_keys.get(state.index) == uint64_t(k.value);
    }
    else {
        if (k.value < (Array::get(s_key_ref_or_size_index) >> 1)) {
            state.index = size_t(k.value);
            return true;
        }
    }
    return false;
}

ObjKey Cluster::get(size_t ndx, ClusterNode::State& state) const
{
    state.index = ndx;
    state.mem = get_mem();
    return get_real_key(ndx);
}

template <class T>
inline void Cluster::do_erase(size_t ndx, ColKey col_key)
{
    auto col_ndx = col_key.get_index();
    T values(m_alloc);
    values.set_parent(this, col_ndx.val + s_first_col_index);
    set_spec<T>(values, col_ndx);
    values.init_from_parent();
    values.erase(ndx);
}

inline void Cluster::do_erase_key(size_t ndx, ColKey col_key, CascadeState& state)
{
    ArrayKey values(m_alloc);
    auto col_ndx = col_key.get_index();
    values.set_parent(this, col_ndx.val + s_first_col_index);
    values.init_from_parent();

    ObjKey key = values.get(ndx);
    if (key != null_key) {
        remove_backlinks(get_real_key(ndx), col_key, {key}, state);
    }
    values.erase(ndx);
}

size_t Cluster::get_ndx(ObjKey k, size_t ndx) const
{
    size_t index;
    if (m_keys.is_attached()) {
        index = m_keys.lower_bound(uint64_t(k.value));
        if (index == m_keys.size() || m_keys.get(index) != uint64_t(k.value)) {
            throw InvalidKey("Key not found");
        }
    }
    else {
        if (k.value >= (Array::get(s_key_ref_or_size_index) >> 1)) {
            throw InvalidKey("Key not found");
        }
        index = size_t(k.value);
    }
    return index + ndx;
}

size_t Cluster::erase(ObjKey key, CascadeState& state)
{
    size_t ndx;
    if (m_keys.is_attached()) {
        ndx = m_keys.lower_bound(uint64_t(key.value));
        if (ndx == m_keys.size() || m_keys.get(ndx) != uint64_t(key.value)) {
            throw InvalidKey("Key not found");
        }
    }
    else {
        ndx = size_t(key.value);
        if (ndx >= get_as_ref_or_tagged(0).get_as_int()) {
            throw InvalidKey("Key not found");
        }
    }

    const Spec& spec = m_tree_top.get_spec();
    size_t num_cols = spec.get_column_count();
    size_t num_public_cols = spec.get_public_column_count();
    // We must start with backlink columns in case the corresponding link
    // columns are in the same table so that we can nullify links before
    // erasing rows in the link columns.
    //
    // This phase also generates replication instructions documenting the side-
    // effects of deleting the object (i.e. link nullifications). These instructions
    // must come before the actual deletion of the object, but at the same time
    // the Replication object may need a consistent view of the row (not including
    // link columns). Therefore we first nullify links to this object, then
    // generate the instruction, and then delete the row in the remaining columns.

    for (size_t col_ndx = num_public_cols; col_ndx < num_cols; col_ndx++) {
        ColKey col_key = m_tree_top.get_owner()->spec_ndx2colkey(col_ndx);
        ColKey::Idx leaf_ndx = col_key.get_index();
        auto type = col_key.get_type();
        REALM_ASSERT(type == col_type_BackLink);
        ArrayBacklink values(m_alloc);
        values.set_parent(this, leaf_ndx.val + s_first_col_index);
        values.init_from_parent();
        // Ensure that Cluster is writable and able to hold references to nodes in
        // the slab area before nullifying or deleting links. These operation may
        // both have the effect that other objects may be constructed and manipulated.
        // If those other object are in the same cluster that the object to be deleted
        // is in, then that will cause another accessor to this cluster to be created.
        // It would lead to an error if the cluster node was relocated without it being
        // reflected in the context here.
        values.copy_on_write();
        values.nullify_fwd_links(ndx, state);
    }

    ObjKey real_key = get_real_key(ndx);
    auto table = m_tree_top.get_owner();
    if (state.notification_handler()) {
        Group::CascadeNotification notifications;
        notifications.rows.emplace_back(table->get_key(), real_key);
        state.send_notifications(notifications);
    }

    const_cast<Table*>(table)->free_local_id_after_hash_collision(real_key);
    if (Replication* repl = table->get_repl()) {
        repl->remove_object(table, real_key);
    }

    auto erase_in_column = [&](ColKey col_key) {
        auto col_type = col_key.get_type();
        auto col_ndx = col_key.get_index();
        auto attr = col_key.get_attrs();
        if (attr.test(col_attr_List)) {
            ArrayInteger values(m_alloc);
            values.set_parent(this, col_ndx.val + s_first_col_index);
            values.init_from_parent();
            ref_type ref = values.get_as_ref(ndx);

            if (ref) {
                if (col_type == col_type_LinkList) {
                    BPlusTree<ObjKey> links(m_alloc);
                    links.init_from_ref(ref);
                    if (links.size() > 0) {
                        remove_backlinks(ObjKey(key.value + m_offset), col_key, links.get_all(), state);
                    }
                }
                Array::destroy_deep(ref, m_alloc);
            }

            values.erase(ndx);

            return false;
        }

        switch (col_type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_erase<ArrayIntNull>(ndx, col_key);
                }
                else {
                    do_erase<ArrayInteger>(ndx, col_key);
                }
                break;
            case col_type_Bool:
                do_erase<ArrayBoolNull>(ndx, col_key);
                break;
            case col_type_Float:
                do_erase<ArrayFloatNull>(ndx, col_key);
                break;
            case col_type_Double:
                do_erase<ArrayDoubleNull>(ndx, col_key);
                break;
            case col_type_String:
                do_erase<ArrayString>(ndx, col_key);
                break;
            case col_type_Binary:
                do_erase<ArrayBinary>(ndx, col_key);
                break;
            case col_type_Timestamp:
                do_erase<ArrayTimestamp>(ndx, col_key);
                break;
            case col_type_Link:
                do_erase_key(ndx, col_key, state);
                break;
            case col_type_BackLink:
                do_erase<ArrayBacklink>(ndx, col_key);
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
        return false;
    };
    m_tree_top.get_owner()->for_each_and_every_column(erase_in_column);

    if (m_keys.is_attached()) {
        m_keys.erase(ndx);
    }
    else {
        size_t current_size = get_size_in_compact_form();
        if (ndx == current_size - 1) {
            // When deleting last, we can still maintain compact form
            set(0, RefOrTagged::make_tagged(current_size - 1));
        }
        else {
            ensure_general_form();
            m_keys.erase(ndx);
        }
    }

    return node_size();
}

void Cluster::upgrade_string_to_enum(ColKey col_key, ArrayString& keys)
{
    auto col_ndx = col_key.get_index();
    ArrayInteger indexes(m_alloc);
    indexes.create(Array::type_Normal, false);
    ArrayString values(m_alloc);
    ref_type ref = Array::get_as_ref(col_ndx.val + s_first_col_index);
    values.init_from_ref(ref);
    size_t sz = values.size();
    for (size_t i = 0; i < sz; i++) {
        auto v = values.get(i);
        size_t pos = keys.lower_bound(v);
        REALM_ASSERT_3(pos, !=, keys.size());
        indexes.add(pos);
    }
    Array::set(col_ndx.val + s_first_col_index, indexes.get_ref());
    Array::destroy_deep(ref, m_alloc);
}

void Cluster::init_leaf(ColKey col_key, ArrayPayload* leaf) const
{
    auto col_ndx = col_key.get_index();
    // FIXME: Move this validation into callers.
    // Currently, the query subsystem may call with an unvalidated key.
    // once fixed, reintroduce the noexcept declaration :-D
    m_tree_top.get_owner()->report_invalid_key(col_key);
    ref_type ref = to_ref(Array::get(col_ndx.val + 1));
    if (leaf->need_spec()) {
        size_t spec_ndx = m_tree_top.get_owner()->leaf_ndx2spec_ndx(col_ndx);
        leaf->set_spec(const_cast<Spec*>(&m_tree_top.get_spec()), spec_ndx);
    }
    leaf->init_from_ref(ref);
    leaf->set_parent(const_cast<Cluster*>(this), col_ndx.val + 1);
}

void Cluster::add_leaf(ColKey col_key, ref_type ref)
{
    auto col_ndx = col_key.get_index();
    REALM_ASSERT((col_ndx.val + 1) == size());
    Array::insert(col_ndx.val + 1, from_ref(ref));
}

// LCOV_EXCL_START
void Cluster::dump_objects(int64_t key_offset, std::string lead) const
{
    std::cout << lead << "leaf - size: " << node_size() << std::endl;
    if (!m_keys.is_attached()) {
        std::cout << lead << "compact form" << std::endl;
    }
    for (unsigned i = 0; i < node_size(); i++) {
        int64_t key_value;
        if (m_keys.is_attached()) {
            key_value = m_keys.get(i);
        }
        else {
            key_value = int64_t(i);
        }
        std::cout << lead << "key: " << std::hex << key_value + key_offset << std::dec;
        for (size_t j = 1; j < size(); j++) {
            if (m_tree_top.get_spec().get_column_attr(j - 1).test(col_attr_List)) {
                std::cout << ", list";
            }

            switch (m_tree_top.get_spec().get_column_type(j - 1)) {
                case col_type_Int: {
                    bool nullable = m_tree_top.get_spec().get_column_attr(j - 1).test(col_attr_Nullable);
                    ref_type ref = Array::get_as_ref(j);
                    if (nullable) {
                        ArrayIntNull arr_int_null(m_alloc);
                        arr_int_null.init_from_ref(ref);
                        if (arr_int_null.is_null(i)) {
                            std::cout << ", null";
                        }
                        else {
                            std::cout << ", " << arr_int_null.get(i).value();
                        }
                    }
                    else {
                        Array arr(m_alloc);
                        arr.init_from_ref(ref);
                        std::cout << ", " << arr.get(i);
                    }
                    break;
                }
                case col_type_Bool: {
                    ArrayBoolNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    auto val = arr.get(i);
                    std::cout << ", " << (val ? (*val ? "true" : "false") : "null");
                    break;
                }
                case col_type_Float: {
                    ArrayFloatNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    auto val = arr.get(i);
                    if (val)
                        std::cout << ", " << *val;
                    else
                        std::cout << ", null";
                    break;
                }
                case col_type_Double: {
                    ArrayDoubleNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    auto val = arr.get(i);
                    if (val)
                        std::cout << ", " << *val;
                    else
                        std::cout << ", null";
                    break;
                    break;
                }
                case col_type_String: {
                    ArrayString arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_Binary: {
                    ArrayBinary arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_Timestamp: {
                    ArrayTimestamp arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    if (arr.is_null(i)) {
                        std::cout << ", " << "null";
                    }
                    else  {
                        std::cout << ", " << arr.get(i);
                    }
                    break;
                }
                case col_type_Link: {
                    ArrayKey arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_BackLink: {
                    break;
                }
                default:
                    std::cout << ", Error";
                    break;
            }
        }
        std::cout << std::endl;
    }
}
// LCOV_EXCL_STOP

void Cluster::remove_backlinks(ObjKey origin_key, ColKey origin_col_key, const std::vector<ObjKey>& keys,
                               CascadeState& state) const
{
    const Table* origin_table = m_tree_top.get_owner();
    TableRef target_table = origin_table->get_opposite_table(origin_col_key);
    ColKey backlink_col_key = origin_table->get_opposite_column(origin_col_key);

    CascadeState::Mode mode = state.m_mode;
    bool strong_links = (origin_table->get_link_type(origin_col_key) == link_Strong);
    bool only_strong_links = (mode == CascadeState::Mode::strong);

    for (auto key : keys) {
        if (key != null_key) {
            Obj target_obj = target_table->get_object(key);
            bool last_removed = target_obj.remove_one_backlink(backlink_col_key, origin_key); // Throws

            // Check if the object should be cascade deleted
            if (mode != CascadeState::none && (mode == CascadeState::all || (strong_links && last_removed))) {
                bool has_backlinks = target_obj.has_backlinks(only_strong_links);

                if (!has_backlinks) {
                    // Object has no more backlinks - add to list for deletion
                    state.m_to_be_deleted.emplace_back(target_table->get_key(), key);
                }
            }
        }
    }
}

/******************************** ClusterTree ********************************/

ClusterTree::ClusterTree(Table* owner, Allocator& alloc)
    : m_owner(owner)
    , m_alloc(alloc)
{
}

MemRef ClusterTree::create_empty_cluster(Allocator& alloc)
{
    Array arr(alloc);
    arr.create(Array::type_HasRefs); // Throws

    arr.add(RefOrTagged::make_tagged(0)); // Compact form
    return arr.get_mem();
}

std::unique_ptr<ClusterNode> ClusterTree::create_root_from_mem(Allocator& alloc, MemRef mem)
{
    const char* header = mem.get_addr();
    bool is_leaf = !Array::get_is_inner_bptree_node_from_header(header);

    bool can_reuse_root_accessor = m_root && &m_root->get_alloc() == &alloc && m_root->is_leaf() == is_leaf;
    if (can_reuse_root_accessor) {
        m_root->init(mem);
        return std::move(m_root); // Same root will be reinstalled.
    }

    // Not reusing root note, allocating a new one.
    std::unique_ptr<ClusterNode> new_root;
    if (is_leaf) {
        new_root = std::make_unique<Cluster>(0, alloc, *this);
    }
    else {
        new_root = std::make_unique<ClusterNodeInner>(alloc, *this);
    }
    new_root->init(mem);

    return new_root;
}

void ClusterTree::replace_root(std::unique_ptr<ClusterNode> new_root)
{
    if (new_root != m_root) {
        // Maintain parent.
        ArrayParent* parent = m_root->get_parent();
        size_t ndx_in_parent = m_root->get_ndx_in_parent();
        new_root->set_parent(parent, ndx_in_parent);
        new_root->update_parent(); // Throws
        m_root = std::move(new_root);
    }
}

void ClusterTree::init_from_ref(ref_type ref)
{
    auto new_root = create_root_from_ref(m_alloc, ref);
    if (m_root) {
        ArrayParent* parent = m_root->get_parent();
        size_t ndx_in_parent = m_root->get_ndx_in_parent();
        new_root->set_parent(parent, ndx_in_parent);
    }
    m_root = std::move(new_root);
    m_size = m_root->get_tree_size();
}

void ClusterTree::init_from_parent()
{
    ref_type ref = m_root->get_ref_from_parent();
    init_from_ref(ref);
}

bool ClusterTree::update_from_parent(size_t old_baseline) noexcept
{
    bool was_updated = m_root->update_from_parent(old_baseline);
    if (was_updated) {
        m_size = m_root->get_tree_size();
    }
    return was_updated;
}

void ClusterTree::clear()
{
    size_t num_cols = get_spec().get_public_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; col_ndx++) {
        auto col_key = m_owner->spec_ndx2colkey(col_ndx);
        if (StringIndex* index = m_owner->get_search_index(col_key)) {
            index->clear();
        }
    }

    remove_links(); // This will also delete objects loosing their last strong link

    m_root->destroy_deep();

    auto leaf = std::make_unique<Cluster>(0, m_root->get_alloc(), *this);
    leaf->create(m_owner->num_leaf_cols());
    replace_root(std::move(leaf));
    m_size = 0;
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

Obj ClusterTree::insert(ObjKey k, const FieldValues& values)
{
    ClusterNode::State state;
    FieldValues init_values(values);
    const Table* table = get_owner();

    // Sort ColKey according to index
    std::sort(init_values.begin(), init_values.end(),
              [](auto& a, auto& b) { return a.col_key.get_index().val < b.col_key.get_index().val; });

    insert_fast(k, init_values, state);

    // Update index
    auto value = init_values.begin();
    auto insert_in_column = [&](ColKey col_key) {
        // Check if initial value is provided
        Mixed init_value;
        if (value != init_values.end() && value->col_key.get_index().val == col_key.get_index().val) {
            init_value = value->value;
            ++value;
        }

        if (StringIndex* index = table->get_search_index(col_key)) {
            auto type = col_key.get_type();
            auto attr = col_key.get_attrs();
            bool nullable = attr.test(col_attr_Nullable);
            switch (type) {
                case col_type_Int:
                    if (init_value.is_null()) {
                        index->insert(k, ArrayIntNull::default_value(nullable));
                    }
                    else {
                        index->insert(k, init_value.get<int64_t>());
                    }
                    break;
                case col_type_Bool:
                    if (init_value.is_null()) {
                        index->insert(k, ArrayBoolNull::default_value(nullable));
                    }
                    else {
                        index->insert(k, init_value.get<bool>());
                    }
                    break;
                case col_type_String:
                    if (init_value.is_null()) {
                        index->insert(k, ArrayString::default_value(nullable));
                    }
                    else {
                        index->insert(k, init_value.get<String>());
                    }
                    break;
                case col_type_Timestamp:
                    if (init_value.is_null()) {
                        index->insert(k, ArrayTimestamp::default_value(nullable));
                    }
                    else {
                        index->insert(k, init_value.get<Timestamp>());
                    }
                    break;
                default:
                    break;
            }
        }
        return false;
    };
    get_owner()->for_each_public_column(insert_in_column);

    if (Replication* repl = table->get_repl()) {
        repl->create_object(table, k);
        for (const auto& v : values) {
            if (v.value.is_null()) {
                repl->set_null(table, v.col_key, k, _impl::instr_Set);
            }
            else {
                repl->set(table, v.col_key, k, v.value, _impl::instr_Set);
            }
        }
    }

    return Obj(this, state.mem, k, state.index);
}

bool ClusterTree::is_valid(ObjKey k) const
{
    ClusterNode::State state;
    return m_root->try_get(k, state);
}

ConstObj ClusterTree::get(ObjKey k) const
{
    ClusterNode::State state;
    m_root->get(k, state);
    return ConstObj(this, state.mem, k, state.index);
}

Obj ClusterTree::get(ObjKey k)
{
    ClusterNode::State state;
    m_root->get(k, state);
    return Obj(this, state.mem, k, state.index);
}

ConstObj ClusterTree::get(size_t ndx) const
{
    if (ndx >= m_size) {
        throw std::out_of_range("Object was deleted");
    }
    ClusterNode::State state;
    ObjKey k = m_root->get(ndx, state);
    return ConstObj(this, state.mem, k, state.index);
}

Obj ClusterTree::get(size_t ndx)
{
    if (ndx >= m_size) {
        throw std::out_of_range("Object was deleted");
    }
    ClusterNode::State state;
    ObjKey k = m_root->get(ndx, state);
    return Obj(this, state.mem, k, state.index);
}

size_t ClusterTree::get_ndx(ObjKey k) const
{
    return m_root->get_ndx(k, 0);
}

void ClusterTree::erase(ObjKey k, CascadeState& state)
{
    size_t num_cols = get_spec().get_public_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; col_ndx++) {
        auto col_key = m_owner->spec_ndx2colkey(col_ndx);
        if (StringIndex* index = m_owner->get_search_index(col_key)) {
            index->erase(k);
        }
    }

    size_t root_size = m_root->erase(k, state);

    bump_content_version();
    bump_storage_version();
    m_size--;
    while (!m_root->is_leaf() && root_size == 1) {
        ClusterNodeInner* node = static_cast<ClusterNodeInner*>(m_root.get());

        uint64_t offset = node->get_first_key_value();
        ref_type new_root_ref = node->clear_first_child_ref();
        node->destroy_deep();

        auto new_root = get_node(new_root_ref);
        new_root->adjust_keys(offset);

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

bool ClusterTree::traverse(TraverseFunction& func) const
{
    if (m_root->is_leaf()) {
        return func(static_cast<Cluster*>(m_root.get()));
    }
    else {
        return static_cast<ClusterNodeInner*>(m_root.get())->traverse(func, 0);
    }
}

void ClusterTree::update(UpdateFunction& func)
{
    if (m_root->is_leaf()) {
        func(static_cast<Cluster*>(m_root.get()));
    }
    else {
        static_cast<ClusterNodeInner*>(m_root.get())->update(func, 0);
    }
}

void ClusterTree::enumerate_string_column(ColKey col_key)
{
    Allocator& alloc = get_alloc();

    ArrayString keys(alloc);
    ArrayString leaf(alloc);
    keys.create();

    ClusterTree::TraverseFunction collect_strings = [col_key, &leaf, &keys](const Cluster* cluster) {
        cluster->init_leaf(col_key, &leaf);
        size_t sz = leaf.size();
        size_t key_size = keys.size();
        for (size_t i = 0; i < sz; i++) {
            auto v = leaf.get(i);
            size_t pos = keys.lower_bound(v);
            if (pos == key_size || keys.get(pos) != v) {
                keys.insert(pos, v); // Throws
                key_size++;
            }
        }

        return false; // Continue
    };

    ClusterTree::UpdateFunction upgrade = [col_key, &keys](Cluster* cluster) {
        cluster->upgrade_string_to_enum(col_key, keys);
    };

    // Populate 'keys' array
    traverse(collect_strings);

    // Store key strings in spec
    size_t spec_ndx = get_owner()->colkey2spec_ndx(col_key);
    const_cast<Spec*>(&get_spec())->upgrade_string_to_enum(spec_ndx, keys.get_ref());

    // Replace column in all clusters
    update(upgrade);
}

std::unique_ptr<ClusterNode> ClusterTree::get_node(ref_type ref) const
{
    std::unique_ptr<ClusterNode> node;
    Allocator& alloc = m_root->get_alloc();

    char* child_header = static_cast<char*>(alloc.translate(ref));
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        node = std::make_unique<Cluster>(0, alloc, *this);
    }
    else {
        node = std::make_unique<ClusterNodeInner>(alloc, *this);
    }
    node->init(MemRef(child_header, ref, alloc));

    return node;
}

size_t ClusterTree::get_column_index(StringData col_name) const
{
    return get_spec().get_column_index(col_name);
}

const Spec& ClusterTree::get_spec() const
{
    typedef _impl::TableFriend tf;
    return tf::get_spec(*m_owner);
}

void ClusterTree::remove_links()
{
    CascadeState state(CascadeState::Mode::strong);
    state.m_group = m_owner->get_parent_group();
    Allocator& alloc = get_alloc();
    // This function will add objects that should be deleted to 'state'
    ClusterTree::TraverseFunction func = [this, &state, &alloc](const Cluster* cluster) {
        auto remove_link_from_column = [&](ColKey col_key) {
            // Prevent making changes to table that is going to be removed anyway
            // Furthermore it is a prerequisite for using 'traverse' that the tree
            // is not modified
            if (m_owner->links_to_self(col_key)) {
                return false;
            }
            auto col_type = col_key.get_type();
            if (col_type == col_type_Link) {
                ArrayKey values(alloc);
                cluster->init_leaf(col_key, &values);
                size_t sz = values.size();
                for (size_t i = 0; i < sz; i++) {
                    if (ObjKey key = values.get(i)) {
                        cluster->remove_backlinks(cluster->get_real_key(i), col_key, {key}, state);
                    }
                }
            }
            else if (col_type == col_type_LinkList) {
                ArrayInteger values(alloc);
                cluster->init_leaf(col_key, &values);
                size_t sz = values.size();
                for (size_t i = 0; i < sz; i++) {
                    if (ref_type ref = values.get_as_ref(i)) {
                        BPlusTree<ObjKey> links(alloc);
                        links.init_from_ref(ref);
                        if (links.size() > 0) {
                            cluster->remove_backlinks(cluster->get_real_key(i), col_key, links.get_all(), state);
                        }
                    }
                }
            }
            else if (col_type == col_type_BackLink) {
                ArrayBacklink values(alloc);
                cluster->init_leaf(col_key, &values);
                values.set_parent(const_cast<Cluster*>(cluster),
                                  col_key.get_index().val + Cluster::s_first_col_index);
                size_t sz = values.size();
                for (size_t i = 0; i < sz; i++) {
                    values.nullify_fwd_links(i, state);
                }
            }
            return false;
        };
        get_owner()->for_each_and_every_column(remove_link_from_column);
        // Continue
        return false;
    };

    // Go through all clusters
    traverse(func);

    m_owner->remove_recursive(state);
}


ClusterTree::ConstIterator::ConstIterator(const ClusterTree& t, size_t ndx)
    : m_tree(t)
    , m_leaf(0, t.get_alloc(), t)
    , m_state(m_leaf)
    , m_instance_version(t.get_instance_version())
{
    if (ndx == 0) {
        // begin
        m_key = load_leaf(ObjKey(0));
    }
    else {
        // end
        m_key = null_key;
    }
}

ClusterTree::ConstIterator::ConstIterator(const ClusterTree& t, ObjKey key)
    : m_tree(t)
    , m_leaf(0, t.get_alloc(), t)
    , m_state(m_leaf)
    , m_instance_version(t.get_instance_version())
    , m_key(key)
{
}

ObjKey ClusterTree::ConstIterator::load_leaf(ObjKey key) const
{
    m_storage_version = m_tree.get_storage_version(m_instance_version);
    // 'key' may or may not exist. If it does not exist, state is updated
    // to point to the next object in line.
    if (m_tree.get_leaf(key, m_state)) {
        // Get the actual key value
        return m_leaf.get_real_key(m_state.m_current_index);
    }
    else {
        // end of table
        return null_key;
    }
}

ClusterTree::ConstIterator::pointer Table::ConstIterator::operator->() const
{
    if (m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        ObjKey k = load_leaf(m_key);
        if (k != m_key)
            throw std::out_of_range("Object was deleted");
    }

    REALM_ASSERT(m_leaf.is_attached());

    return new (&m_obj_cache_storage)
        Obj(const_cast<ClusterTree*>(&m_tree), m_leaf.get_mem(), m_key, m_state.m_current_index);
}

ClusterTree::ConstIterator& Table::ConstIterator::operator++()
{
    if (m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        ObjKey k = load_leaf(m_key);
        if (k != m_key) {
            // Objects was deleted. k points to the next object
            m_key = k;
            return *this;
        }
    }
    m_state.m_current_index++;
    if (m_state.m_current_index == m_leaf.node_size()) {
        m_key = load_leaf(ObjKey(m_key.value + 1));
    }
    else {
        m_key = m_leaf.get_real_key(m_state.m_current_index);
    }
    return *this;
}

ClusterTree::ConstIterator& Table::ConstIterator::operator+=(ptrdiff_t adj)
{
    // If you have to jump far away and thus have to load many leaves,
    // this function will be slow

    REALM_ASSERT(adj >= 0);
    size_t n = size_t(adj);
    if (m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        load_leaf(m_key);
    }
    while (n != 0 && m_key != null_key) {
        size_t left_in_leaf = m_leaf.node_size() - m_state.m_current_index;
        if (n < left_in_leaf) {
            m_state.m_current_index += n;
            m_key = m_leaf.get_real_key(m_state.m_current_index);
            n = 0;
        }
        else {
            // load next leaf
            n -= left_in_leaf;
            m_key = m_leaf.get_real_key(m_state.m_current_index + left_in_leaf - 1);
            m_key = load_leaf(ObjKey(m_key.value + 1));
        }
    }
    return *this;
}
