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
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_key.hpp"
#include "realm/array_backlink.hpp"
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

    void create(int sub_tree_depth) override;
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

    bool traverse(ClusterTree::TraverseFunction func, int64_t) const;

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
    void insert_column(size_t ndx) override;
    void remove_column(size_t ndx) override;
    ref_type insert(ObjKey k, State& state) override;
    void get(ObjKey k, State& state) const override;
    ObjKey get(size_t ndx, State& state) const override;
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
        int64_t offset;
        ObjKey key;
    };
    ChildInfo find_child(ObjKey key) const
    {
        ChildInfo ret;
        if (m_keys.is_attached()) {
            auto upper = m_keys.upper_bound(uint64_t(key.value));
            // The first entry in m_keys will always be smaller than or equal
            // to all keys in this subtree.
            REALM_ASSERT_DEBUG(upper > 0);
            ret.ndx = upper - 1;
            ret.offset = m_keys.get(ret.ndx);
            ret.key = ObjKey(key.value - ret.offset);
        }
        else {
            size_t sz = node_size();
            REALM_ASSERT_DEBUG(sz > 0);
            size_t max_ndx = sz - 1;
            ret.ndx = std::min(size_t(key.value) >> m_shift_factor, max_ndx);
            ret.offset = ret.ndx << m_shift_factor;
            ret.key = ObjKey(key.value - ret.offset);
        }
        return ret;
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
};
}

void ClusterNode::IteratorState::clear()
{
    m_current_leaf.detach();
    m_key_offset = 0;
    m_current_index = size_t(-1);
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
    ChildInfo child_info = find_child(key);

    ref_type child_ref = _get_child_ref(child_info.ndx);
    char* child_header = m_alloc.translate(child_ref);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        Cluster leaf(child_info.offset + m_offset, m_alloc, m_tree_top);
        leaf.set_parent(this, child_info.ndx + s_first_node_index);
        leaf.init(MemRef(child_header, child_ref, m_alloc));
        return func(&leaf, child_info);
    }
    else {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.set_parent(this, child_info.ndx + s_first_node_index);
        node.init(MemRef(child_header, child_ref, m_alloc));
        node.set_offset(child_info.offset + m_offset);
        return func(&node, child_info);
    }
}

MemRef ClusterNodeInner::ensure_writeable(ObjKey key)
{
    return recurse<MemRef>(
        key, [](ClusterNode* node, ChildInfo& child_info) { return node->ensure_writeable(child_info.key); });
}

ref_type ClusterNodeInner::insert(ObjKey key, ClusterNode::State& state)
{
    return recurse<ref_type>(key, [this, &state](ClusterNode* node, ChildInfo& child_info) {
        ref_type new_sibling_ref = node->insert(child_info.key, state);

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

void ClusterNodeInner::get(ObjKey key, ClusterNode::State& state) const
{
    return const_cast<ClusterNodeInner*>(this)->recurse<void>(
        key, [&state](const ClusterNode* node, ChildInfo& child_info) { return node->get(child_info.key, state); });
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

void ClusterNodeInner::insert_column(size_t ndx)
{
    size_t sz = node_size();
    for (size_t i = 0; i < sz; i++) {
        ref_type child_ref = _get_child_ref(i);
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(child_ref);
        node->set_parent(this, i + s_first_node_index);
        node->insert_column(ndx);
    }
}

void ClusterNodeInner::remove_column(size_t ndx)
{
    size_t sz = node_size();
    for (size_t i = 0; i < sz; i++) {
        ref_type child_ref = _get_child_ref(i);
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(child_ref);
        node->set_parent(this, i + s_first_node_index);
        node->remove_column(ndx);
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
inline void Cluster::do_create(size_t col_ndx)
{
    T arr(m_alloc);
    arr.create();
    arr.set_parent(this, col_ndx + s_first_col_index);
    arr.update_parent();
}

void Cluster::create(int)
{
    // Create array with the required size
    size_t nb_columns = m_tree_top.get_spec().get_column_count();
    Array::create(type_HasRefs, false, nb_columns + s_first_col_index);
    Array::set(0, RefOrTagged::make_tagged(0));
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        ColumnAttrMask attr = m_tree_top.get_spec().get_column_attr(col_ndx);

        if (attr.test(col_attr_List)) {
            ArrayInteger arr(m_alloc);
            arr.create(type_HasRefs);
            arr.set_parent(this, col_ndx + s_first_col_index);
            arr.update_parent();

            continue;
        }
        switch (m_tree_top.get_spec().get_column_type(col_ndx)) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_create<ArrayIntNull>(col_ndx);
                }
                else {
                    do_create<ArrayInteger>(col_ndx);
                }
                break;
            case col_type_Bool:
                do_create<ArrayBoolNull>(col_ndx);
                break;
            case col_type_Float:
                do_create<ArrayFloat>(col_ndx);
                break;
            case col_type_Double:
                do_create<ArrayDouble>(col_ndx);
                break;
            case col_type_String:
            case col_type_StringEnum:
                do_create<ArrayString>(col_ndx);
                break;
            case col_type_Binary:
                do_create<ArrayBinary>(col_ndx);
                break;
            case col_type_Timestamp:
                do_create<ArrayTimestamp>(col_ndx);
                break;
            case col_type_Link:
                do_create<ArrayKey>(col_ndx);
                break;
            case col_type_BackLink:
                do_create<ArrayBacklink>(col_ndx);
                break;
            default:
                throw LogicError(LogicError::illegal_type);
                break;
        }
    }
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

template <class T>
inline void Cluster::do_insert_row(size_t ndx, size_t col_ndx, ColumnAttrMask attr)
{
    if (attr.test(col_attr_List)) {
        ArrayInteger arr(m_alloc);
        arr.set_parent(this, col_ndx + s_first_col_index);
        arr.init_from_parent();
        arr.insert(ndx, 0);
    }
    else {
        T arr(m_alloc);
        arr.set_parent(this, col_ndx + s_first_col_index);
        arr.init_from_parent();
        arr.insert(ndx, T::default_value(attr.test(col_attr_Nullable)));
    }
}

void Cluster::insert_row(size_t ndx, ObjKey k)
{
    if (m_keys.is_attached()) {
        m_keys.insert(ndx, k.value);
    }
    else {
        Array::set(s_key_ref_or_size_index, Array::get(s_key_ref_or_size_index) + 2); // Increments size by 1
    }

    size_t nb_columns = size() - 1;
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        ColumnAttrMask attr = m_tree_top.get_spec().get_column_attr(col_ndx);

        if (attr.test(col_attr_List)) {
            do_insert_row<ArrayInteger>(ndx, col_ndx, attr);
            continue;
        }

        switch (m_tree_top.get_spec().get_column_type(col_ndx)) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_insert_row<ArrayIntNull>(ndx, col_ndx, attr);
                }
                else {
                    do_insert_row<ArrayInteger>(ndx, col_ndx, attr);
                }
                break;
            case col_type_Bool:
                do_insert_row<ArrayBoolNull>(ndx, col_ndx, attr);
                break;
            case col_type_Float:
                do_insert_row<ArrayFloat>(ndx, col_ndx, attr);
                break;
            case col_type_Double:
                do_insert_row<ArrayDouble>(ndx, col_ndx, attr);
                break;
            case col_type_String:
            case col_type_StringEnum:
                do_insert_row<ArrayString>(ndx, col_ndx, attr);
                break;
            case col_type_Binary:
                do_insert_row<ArrayBinary>(ndx, col_ndx, attr);
                break;
            case col_type_Timestamp:
                do_insert_row<ArrayTimestamp>(ndx, col_ndx, attr);
                break;
            case col_type_Link:
                do_insert_row<ArrayKey>(ndx, col_ndx, attr);
                break;
            case col_type_BackLink:
                do_insert_row<ArrayBacklink>(ndx, col_ndx, attr);
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
    }
}

template <class T>
inline void Cluster::do_move(size_t ndx, size_t col_ndx, Cluster* to)
{
    size_t end = node_size();
    T src(m_alloc);
    src.set_parent(this, col_ndx + s_first_col_index);
    src.init_from_parent();

    T dst(m_alloc);
    dst.set_parent(to, col_ndx + s_first_col_index);
    dst.init_from_parent();

    for (size_t j = ndx; j < end; j++) {
        dst.add(src.get(j));
    }
    src.truncate_and_destroy_children(ndx);
}

void Cluster::move(size_t ndx, ClusterNode* new_node, int64_t offset)
{
    auto new_leaf = static_cast<Cluster*>(new_node);

    size_t nb_columns = size() - 1;
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        ColumnAttrMask attr = m_tree_top.get_spec().get_column_attr(col_ndx);

        if (attr.test(col_attr_List)) {
            do_move<ArrayInteger>(ndx, col_ndx, new_leaf);
            continue;
        }

        switch (m_tree_top.get_spec().get_column_type(col_ndx)) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_move<ArrayIntNull>(ndx, col_ndx, new_leaf);
                }
                else {
                    do_move<ArrayInteger>(ndx, col_ndx, new_leaf);
                }
                break;
            case col_type_Bool:
                do_move<ArrayBoolNull>(ndx, col_ndx, new_leaf);
                break;
            case col_type_Float:
                do_move<ArrayFloat>(ndx, col_ndx, new_leaf);
                break;
            case col_type_Double:
                do_move<ArrayDouble>(ndx, col_ndx, new_leaf);
                break;
            case col_type_String:
            case col_type_StringEnum:
                do_move<ArrayString>(ndx, col_ndx, new_leaf);
                break;
            case col_type_Binary:
                do_move<ArrayBinary>(ndx, col_ndx, new_leaf);
                break;
            case col_type_Timestamp:
                do_move<ArrayTimestamp>(ndx, col_ndx, new_leaf);
                break;
            case col_type_Link:
                do_move<ArrayKey>(ndx, col_ndx, new_leaf);
                break;
            case col_type_BackLink:
                do_move<ArrayBacklink>(ndx, col_ndx, new_leaf);
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
    }

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
inline void Cluster::do_insert_column(size_t col_ndx, bool nullable)
{
    size_t sz = node_size();

    T arr(m_alloc);
    arr.create();
    auto val = T::default_value(nullable);
    for (size_t i = 0; i < sz; i++) {
        arr.add(val);
    }
    Array::insert(col_ndx + s_first_col_index, from_ref(arr.get_ref()));
}

void Cluster::insert_column(size_t col_ndx)
{
    ColumnAttrMask attr = m_tree_top.get_spec().get_column_attr(col_ndx);
    if (attr.test(col_attr_List)) {
        size_t sz = node_size();

        ArrayInteger arr(m_alloc);
        arr.Array::create(type_HasRefs, false, sz, 0);
        Array::insert(col_ndx + s_first_col_index, from_ref(arr.get_ref()));
        return;
    }
    bool nullable = attr.test(col_attr_Nullable);

    switch (m_tree_top.get_spec().get_column_type(col_ndx)) {
        case col_type_Int:
            if (nullable) {
                do_insert_column<ArrayIntNull>(col_ndx, nullable);
            }
            else {
                do_insert_column<ArrayInteger>(col_ndx, nullable);
            }
            break;
        case col_type_Bool:
            do_insert_column<ArrayBoolNull>(col_ndx, nullable);
            break;
        case col_type_Float:
            do_insert_column<ArrayFloat>(col_ndx, nullable);
            break;
        case col_type_Double:
            do_insert_column<ArrayDouble>(col_ndx, nullable);
            break;
        case col_type_String:
        case col_type_StringEnum:
            do_insert_column<ArrayString>(col_ndx, nullable);
            break;
        case col_type_Binary:
            do_insert_column<ArrayBinary>(col_ndx, nullable);
            break;
        case col_type_Timestamp:
            do_insert_column<ArrayTimestamp>(col_ndx, nullable);
            break;
        case col_type_Link:
            do_insert_column<ArrayKey>(col_ndx, nullable);
            break;
        case col_type_BackLink:
            do_insert_column<ArrayBacklink>(col_ndx, nullable);
            break;
        default:
            throw LogicError(LogicError::illegal_type);
            break;
    }
}

void Cluster::remove_column(size_t col_ndx)
{
    col_ndx++;
    ref_type ref = to_ref(Array::get(col_ndx));
    if (ref != 0) {
        Array::destroy_deep(ref, m_alloc);
    }
    Array::erase(col_ndx);
}

ref_type Cluster::insert(ObjKey k, ClusterNode::State& state)
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
        if (size_t(k.value) < sz) {
            throw InvalidKey("Key already used");
        }
        // Key value is bigger than all other values, should be put last
        ndx = sz;
        if (size_t(k.value) > sz) {
            ensure_general_form();
        }
    }

    ref_type ret = 0;

    REALM_ASSERT_DEBUG(sz <= cluster_node_size);
    if (REALM_LIKELY(sz < cluster_node_size)) {
        insert_row(ndx, k); // Throws
        state.ref = get_ref();
        state.index = ndx;
    }
    else {
        // Split leaf node
        Cluster new_leaf(0, m_alloc, m_tree_top);
        new_leaf.create();
        if (ndx == sz) {
            new_leaf.insert_row(0, ObjKey(0)); // Throws
            state.split_key = k.value;
            state.ref = new_leaf.get_ref();
            state.index = 0;
        }
        else {
            // Current cluster must be in general form to get here
            REALM_ASSERT_DEBUG(m_keys.is_attached());
            new_leaf.ensure_general_form();
            move(ndx, &new_leaf, current_key_value);
            insert_row(ndx, k); // Throws
            state.ref = get_ref();
            state.split_key = current_key_value;
            state.index = ndx;
        }
        ret = new_leaf.get_ref();
    }

    return ret;
}

void Cluster::get(ObjKey k, ClusterNode::State& state) const
{
    state.ref = get_ref();
    if (m_keys.is_attached()) {
        state.index = m_keys.lower_bound(uint64_t(k.value));
        if (state.index == m_keys.size() || m_keys.get(state.index) != uint64_t(k.value)) {
            throw InvalidKey("Key not found");
        }
    }
    else {
        state.index = size_t(k.value);
        if (state.index >= get_as_ref_or_tagged(0).get_as_int()) {
            throw InvalidKey("Key not found");
        }
    }
}

ObjKey Cluster::get(size_t ndx, ClusterNode::State& state) const
{
    state.index = ndx;
    state.ref = get_ref();
    return get_real_key(ndx);
}

template <class T>
inline void Cluster::do_erase(size_t ndx, size_t col_ndx)
{
    T values(m_alloc);
    values.set_parent(this, col_ndx + s_first_col_index);
    values.init_from_parent();
    values.erase(ndx);
}

inline void Cluster::do_erase_key(size_t ndx, size_t col_ndx, CascadeState& state)
{
    ArrayKey values(m_alloc);
    values.set_parent(this, col_ndx + s_first_col_index);
    values.init_from_parent();

    ObjKey key = values.get(ndx);
    if (key != null_key) {
        remove_backlinks(get_real_key(ndx), col_ndx, {key}, state);
    }
    values.erase(ndx);
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
    for (size_t col_ndx = num_cols - 1; col_ndx >= num_public_cols; --col_ndx) {
        REALM_ASSERT(spec.get_column_type(col_ndx) == col_type_BackLink);
        ArrayBacklink values(m_alloc);
        values.set_parent(this, col_ndx + s_first_col_index);
        values.init_from_parent();
        values.nullify_fwd_links(ndx, state);
    }

    for (size_t col_ndx = 0; col_ndx < num_cols; col_ndx++) {
        auto col_type = spec.get_column_type(col_ndx);
        ColumnAttrMask attr = spec.get_column_attr(col_ndx);
        if (attr.test(col_attr_List)) {
            ArrayInteger values(m_alloc);
            values.set_parent(this, col_ndx + s_first_col_index);
            values.init_from_parent();
            ref_type ref = values.get_as_ref(ndx);

            if (ref) {
                if (col_type == col_type_LinkList) {
                    ArrayKey links(m_alloc);
                    links.init_from_ref(ref);
                    if (links.size() > 0) {
                        remove_backlinks(ObjKey(key.value + m_offset), col_ndx, links.get_all(), state);
                    }
                }
                Array::destroy_deep(ref, m_alloc);
            }

            values.erase(ndx);

            continue;
        }

        switch (col_type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_erase<ArrayIntNull>(ndx, col_ndx);
                }
                else {
                    do_erase<ArrayInteger>(ndx, col_ndx);
                }
                break;
            case col_type_Bool:
                do_erase<ArrayBoolNull>(ndx, col_ndx);
                break;
            case col_type_Float:
                do_erase<ArrayFloat>(ndx, col_ndx);
                break;
            case col_type_Double:
                do_erase<ArrayDouble>(ndx, col_ndx);
                break;
            case col_type_String:
            case col_type_StringEnum:
                do_erase<ArrayString>(ndx, col_ndx);
                break;
            case col_type_Binary:
                do_erase<ArrayBinary>(ndx, col_ndx);
                break;
            case col_type_Timestamp:
                do_erase<ArrayTimestamp>(ndx, col_ndx);
                break;
            case col_type_Link:
                do_erase_key(ndx, col_ndx, state);
                break;
            case col_type_BackLink:
                do_erase<ArrayBacklink>(ndx, col_ndx);
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
    }

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
                    ArrayFloat arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_Double: {
                    ArrayDouble arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_StringEnum:
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
                    std::cout << ", " << arr.get(i);
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

void Cluster::remove_backlinks(ObjKey origin_key, size_t origin_col_ndx, const std::vector<ObjKey>& keys,
                               CascadeState& state) const
{
    const Table* origin_table = m_tree_top.get_owner();

    // Find target table
    ColKey origin_col_key = origin_table->ndx2colkey(origin_col_ndx);
    TableRef target_table = _impl::TableFriend::get_opposite_link_table(*origin_table, origin_col_key);
    TableKey target_table_key = target_table->get_key();

    // Find actual backlink column
    const Spec& target_table_spec = _impl::TableFriend::get_spec(*target_table);
    size_t backlink_col = target_table_spec.find_backlink_column(origin_table->get_key(), origin_col_key);
    ColKey backlink_col_key = target_table->ndx2colkey(backlink_col);

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
                    state.rows.emplace_back(target_table_key, key);
                }
            }
        }
    }
}

/******************************** ClusterTree ********************************/

ClusterTree::ClusterTree(Table* owner, Allocator& alloc)
    : m_owner(owner)
    , m_root(std::make_unique<Cluster>(0, alloc, *this))
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

void ClusterTree::init_from_parent()
{
    ref_type ref = m_root->get_ref_from_parent();
    if (ref) {
        ArrayParent* parent = m_root->get_parent();
        size_t ndx_in_parent = m_root->get_ndx_in_parent();
        auto new_root = create_root_from_ref(m_root->get_alloc(), ref);
        new_root->set_parent(parent, ndx_in_parent);
        m_root = std::move(new_root);
        m_size = m_root->get_tree_size();
    }
    else {
        m_root->detach();
        m_size = 0;
    }
}

bool ClusterTree::update_from_parent(size_t old_baseline) noexcept
{
    bool was_updated = m_root->update_from_parent(old_baseline);
    if (was_updated) {
        m_size = m_root->get_tree_size();
    }
    return was_updated;
}

// FIXME: These functions are time critical, we need more direct access
// without going through multiple indirections.
uint64_t ClusterTree::bump_content_version()
{
    m_owner->bump_content_version();
    return m_owner->get_content_version();
}

void ClusterTree::bump_storage_version()
{
    m_owner->bump_storage_version();
}

uint64_t ClusterTree::get_content_version() const
{
    return m_owner->get_content_version();
}

uint64_t ClusterTree::get_instance_version() const
{
    return m_owner->get_instance_version();
}

uint64_t ClusterTree::get_storage_version(uint64_t instance_version) const
{
    return m_owner->get_storage_version(instance_version);
}

void ClusterTree::clear()
{
    size_t num_cols = get_spec().get_public_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; col_ndx++) {
        auto col_key = m_owner->ndx2colkey(col_ndx);
        if (StringIndex* index = m_owner->get_search_index(col_key)) {
            index->clear();
        }
    }

    remove_links(); // This will also delete objects loosing their last strong link

    m_root->destroy_deep();

    auto leaf = std::make_unique<Cluster>(0, m_root->get_alloc(), *this);
    leaf->create();
    replace_root(std::move(leaf));
    m_size = 0;
}

Obj ClusterTree::insert(ObjKey k)
{
    ClusterNode::State state;
    ref_type new_sibling_ref = m_root->insert(k, state);
    if (REALM_UNLIKELY(new_sibling_ref)) {
        auto new_root = std::make_unique<ClusterNodeInner>(m_root->get_alloc(), *this);
        new_root->create(m_root->get_sub_tree_depth() + 1);

        new_root->add(m_root->get_ref());                // Throws
        new_root->add(new_sibling_ref, state.split_key); // Throws
        new_root->update_sub_tree_size();

        replace_root(std::move(new_root));
    }

    // Update index
    const Spec& spec = get_spec();
    size_t num_cols = spec.get_public_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; col_ndx++) {
        auto col_key = m_owner->ndx2colkey(col_ndx);
        if (StringIndex* index = m_owner->get_search_index(col_key)) {
            bool nullable = spec.get_column_attr(col_ndx).test(col_attr_Nullable);
            switch (spec.get_column_type(col_ndx)) {
                case col_type_Int:
                    index->insert(k, ArrayIntNull::default_value(nullable));
                    break;
                case col_type_Bool:
                    index->insert(k, ArrayBoolNull::default_value(nullable));
                    break;
                case col_type_String:
                case col_type_StringEnum:
                    index->insert(k, ArrayString::default_value(nullable));
                    break;
                case col_type_Timestamp:
                    index->insert(k, ArrayTimestamp::default_value(nullable));
                    break;
                default:
                    break;
            }
        }
    }

    if (Replication* repl = get_alloc().get_replication()) {
        repl->create_object(get_owner(), k);
    }
    m_size++;
    return Obj(this, state.ref, k, state.index);
}

bool ClusterTree::is_valid(ObjKey k) const
{
    try {
        ClusterNode::State state;
        m_root->get(k, state);
    }
    catch (const InvalidKey&) {
        return false;
    }
    return true;
}

ConstObj ClusterTree::get(ObjKey k) const
{
    ClusterNode::State state;
    m_root->get(k, state);
    return ConstObj(this, state.ref, k, state.index);
}

Obj ClusterTree::get(ObjKey k)
{
    ClusterNode::State state;
    m_root->get(k, state);
    return Obj(this, state.ref, k, state.index);
}

ConstObj ClusterTree::get(size_t ndx) const
{
    if (ndx >= m_size) {
        throw std::out_of_range("Object was deleted");
    }
    ClusterNode::State state;
    ObjKey k = m_root->get(ndx, state);
    return ConstObj(this, state.ref, k, state.index);
}

Obj ClusterTree::get(size_t ndx)
{
    if (ndx >= m_size) {
        throw std::out_of_range("Object was deleted");
    }
    ClusterNode::State state;
    ObjKey k = m_root->get(ndx, state);
    return Obj(this, state.ref, k, state.index);
}

void ClusterTree::erase(ObjKey k, CascadeState& state)
{
    size_t num_cols = get_spec().get_public_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; col_ndx++) {
        auto col_key = m_owner->ndx2colkey(col_ndx);
        if (StringIndex* index = m_owner->get_search_index(col_key)) {
            index->erase(k);
        }
    }

    size_t root_size = m_root->erase(k, state);

    if (Replication* repl = get_alloc().get_replication()) {
        repl->remove_object(get_owner(), k);
    }

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

bool ClusterTree::traverse(TraverseFunction func) const
{
    if (m_root->is_leaf()) {
        return func(static_cast<Cluster*>(m_root.get()));
    }
    else {
        return static_cast<ClusterNodeInner*>(m_root.get())->traverse(func, 0);
    }
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
    const Spec& spec = get_spec();
    CascadeState state(CascadeState::Mode::strong);
    state.track_link_nullifications = true;
    Allocator& alloc = get_alloc();
    // This function will add objects that should be deleted to 'state'
    auto func = [&spec, &state, &alloc](const Cluster* cluster) {
        size_t num_cols = spec.get_column_count();
        for (size_t col_ndx = 0; col_ndx != num_cols; ++col_ndx) {
            auto col_type = spec.get_column_type(col_ndx);
            if (col_type == col_type_Link) {
                ArrayKey values(alloc);
                cluster->init_leaf(col_ndx, &values);
                size_t sz = values.size();
                for (size_t i = 0; i < sz; i++) {
                    if (ObjKey key = values.get(i)) {
                        cluster->remove_backlinks(cluster->get_real_key(i), col_ndx, {key}, state);
                    }
                }
            }
            else if (col_type == col_type_LinkList) {
                ArrayInteger values(alloc);
                cluster->init_leaf(col_ndx, &values);
                size_t sz = values.size();
                for (size_t i = 0; i < sz; i++) {
                    if (ref_type ref = values.get_as_ref(i)) {
                        ArrayKey links(alloc);
                        links.init_from_ref(ref);
                        if (links.size() > 0) {
                            cluster->remove_backlinks(cluster->get_real_key(i), col_ndx, links.get_all(), state);
                        }
                    }
                }
            }
            else if (col_type == col_type_BackLink) {
                ArrayBacklink values(alloc);
                cluster->init_leaf(col_ndx, &values);
                values.set_parent(const_cast<Cluster*>(cluster), col_ndx + Cluster::s_first_col_index);
                size_t sz = values.size();
                for (size_t i = 0; i < sz; i++) {
                    values.nullify_fwd_links(i, state);
                }
            }
        }
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
        Obj(const_cast<ClusterTree*>(&m_tree), m_leaf.get_ref(), m_key, m_state.m_current_index);
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
