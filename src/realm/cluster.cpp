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

using namespace realm;

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

class ClusterNodeInner : public ClusterNode {
public:
    ClusterNodeInner(Allocator& allocator, const ClusterTree& tree_top);
    ~ClusterNodeInner() override;

    void create() override;
    void init(MemRef mem) override;
    bool update_from_parent(size_t old_baseline) noexcept override;
    MemRef ensure_writeable(Key k) override;

    bool is_leaf() const override
    {
        return false;
    }

    bool traverse(ClusterTree::TraverseFunction func, int64_t) const;

    size_t get_tree_size() const override;
    int64_t get_last_key() const override;

    void insert_column(size_t ndx) override;
    void remove_column(size_t ndx) override;
    ref_type insert(Key k, State& state) override;
    void get(Key k, State& state) const override;
    unsigned erase(Key k) override;
    void add(ref_type ref, int64_t key_value = 0);

    ref_type get_child(size_t ndx) const
    {
        return m_children.get_as_ref(ndx);
    }

    // Reset first (and only!) child ref and return the previous value
    ref_type clear_first_child_ref()
    {
        REALM_ASSERT(m_children.size() == 1);
        ref_type ret = m_children.get_as_ref(0);
        m_children.set(0, 0);
        return ret;
    }

    int64_t get_first_key_value()
    {
        return m_keys.get(0);
    }

    bool get_leaf(Key key, ClusterNode::IteratorState& state) const noexcept;

    void dump_objects(int64_t key_offset, std::string lead) const override;

private:
    Array m_children;

    struct ChildInfo {
        size_t ndx;
        int64_t offset;
        Key key;
    };
    ChildInfo find_child(Key key) const
    {
        ChildInfo ret;
        auto upper = m_keys.upper_bound_int(key.value);
        // The first entry in m_keys will always be smaller than or equal
        // to all keys in this subtree.
        REALM_ASSERT_DEBUG(upper > 0);
        ret.ndx = upper - 1;
        ret.offset = m_keys.get(ret.ndx);
        ret.key = Key(key.value - ret.offset);
        return ret;
    }

    void move(size_t ndx, ClusterNode* new_node, int64_t key_adj) override;

    template <class T, class F>
    T recurse(Key key, F func);
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
    : ClusterNode(allocator, tree_top)
    , m_children(allocator)
{
    m_children.set_parent(this, 1);
}

ClusterNodeInner::~ClusterNodeInner()
{
}

void ClusterNodeInner::create()
{
    Array::create(Array::type_InnerBptreeNode, false, 2);

    m_keys.create(Array::type_Normal);
    m_children.create(Array::type_HasRefs);
    m_keys.update_parent();
    m_children.update_parent();
}

void ClusterNodeInner::init(MemRef mem)
{
    Array::init_from_mem(mem);
    m_keys.set_parent(this, 0);
    m_keys.init_from_parent();
    m_children.set_parent(this, 1);
    m_children.init_from_parent();
}

bool ClusterNodeInner::update_from_parent(size_t old_baseline) noexcept
{
    if (Array::update_from_parent(old_baseline)) {
        m_keys.update_from_parent(old_baseline);
        m_children.update_from_parent(old_baseline);
        return true;
    }
    return false;
}

template <class T, class F>
T ClusterNodeInner::recurse(Key key, F func)
{
    ChildInfo child_info = find_child(key);

    ref_type child_ref = m_children.get_as_ref(child_info.ndx);
    char* child_header = m_alloc.translate(child_ref);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        Cluster leaf(m_alloc, m_tree_top);
        leaf.set_parent(&m_children, child_info.ndx);
        leaf.init(MemRef(child_header, child_ref, m_alloc));
        return func(&leaf, child_info);
    }
    else {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.set_parent(&m_children, child_info.ndx);
        node.init(MemRef(child_header, child_ref, m_alloc));
        return func(&node, child_info);
    }
}

MemRef ClusterNodeInner::ensure_writeable(Key key)
{
    return recurse<MemRef>(
        key, [](ClusterNode* node, ChildInfo& child_info) { return node->ensure_writeable(child_info.key); });
}

ref_type ClusterNodeInner::insert(Key key, ClusterNode::State& state)
{
    return recurse<ref_type>(key, [this, &state](ClusterNode* node, ChildInfo& child_info) {
        ref_type new_sibling_ref = node->insert(child_info.key, state);

        if (!new_sibling_ref) {
            return ref_type(0);
        }

        size_t new_ref_ndx = child_info.ndx + 1;

        int64_t split_key_value = state.split_key + child_info.offset;
        if (m_children.size() < REALM_MAX_BPNODE_SIZE) {
            m_children.insert(new_ref_ndx, new_sibling_ref);
            m_keys.insert(new_ref_ndx, split_key_value);
            return ref_type(0);
        }

        ClusterNodeInner child(m_alloc, m_tree_top);
        child.create();
        if (new_ref_ndx == m_keys.size()) {
            child.add(new_sibling_ref);
            state.split_key = split_key_value;
        }
        else {
            int64_t first_key_value = m_keys.get(new_ref_ndx);
            move(new_ref_ndx, &child, first_key_value);
            add(new_sibling_ref, split_key_value); // Throws
            state.split_key = first_key_value;
        }

        return child.get_ref();
    });
}

void ClusterNodeInner::get(Key key, ClusterNode::State& state) const
{
    return const_cast<ClusterNodeInner*>(this)->recurse<void>(
        key, [&state](const ClusterNode* node, ChildInfo& child_info) { return node->get(child_info.key, state); });
}

unsigned ClusterNodeInner::erase(Key key)
{
    return recurse<unsigned>(key, [this](ClusterNode* erase_node, ChildInfo& child_info) {
        unsigned erase_node_size = erase_node->erase(child_info.key);

        if (erase_node_size == 0) {
            erase_node->destroy_deep();
            m_children.erase(child_info.ndx);
            m_keys.erase(child_info.ndx);
        }
        else if (erase_node_size < REALM_MAX_BPNODE_SIZE / 2 && child_info.ndx < (node_size() - 1)) {
            // Candidate for merge. First calculate if the combined size of current and
            // next sibling is small enough.
            size_t sibling_ndx = child_info.ndx + 1;
            Cluster l2(m_alloc, m_tree_top);
            ClusterNodeInner n2(m_alloc, m_tree_top);
            ClusterNode* sibling_node =
                erase_node->is_leaf() ? static_cast<ClusterNode*>(&l2) : static_cast<ClusterNode*>(&n2);
            sibling_node->set_parent(&m_children, sibling_ndx);
            sibling_node->init_from_parent();

            unsigned combined_size = sibling_node->node_size() + erase_node_size;

            if (combined_size < REALM_MAX_BPNODE_SIZE * 3 / 4) {
                // Calculate value that must be subtracted from the moved keys
                // (will be negative as the sibling has bigger keys)
                int64_t key_adj = m_keys.get(child_info.ndx) - m_keys.get(sibling_ndx);
                // And then move all elements into current node
                sibling_node->move(0, erase_node, key_adj);

                // Destroy sibling
                sibling_node->destroy_deep();
                m_children.erase(sibling_ndx);
                m_keys.erase(sibling_ndx);
            }
        }

        return node_size();
    });
}

void ClusterNodeInner::insert_column(size_t ndx)
{
    for (size_t i = 0; i < m_children.size(); i++) {
        ref_type child_ref = m_children.get_as_ref(i);
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(child_ref);
        node->set_parent(&m_children, i);
        node->insert_column(ndx);
    }
}

void ClusterNodeInner::remove_column(size_t ndx)
{
    for (size_t i = 0; i < m_children.size(); i++) {
        ref_type child_ref = m_children.get_as_ref(i);
        std::shared_ptr<ClusterNode> node = m_tree_top.get_node(child_ref);
        node->set_parent(&m_children, i);
        node->remove_column(ndx);
    }
}

void ClusterNodeInner::add(ref_type ref, int64_t key_value)
{
    m_children.add(from_ref(ref));
    m_keys.add(key_value);
}

// Find leaf that contains the object identified by key. If this does not exist return the
// leaf that contains the next object
bool ClusterNodeInner::get_leaf(Key key, ClusterNode::IteratorState& state) const noexcept
{
    size_t child_ndx = m_keys.upper_bound_int(key.value);
    if (child_ndx > 0)
        child_ndx--;

    while (child_ndx < m_children.size()) {
        int64_t key_offset = m_keys.get(child_ndx);
        Key new_key = Key(key.value - key_offset);
        state.m_key_offset += key_offset;

        ref_type child_ref = m_children.get_as_ref(child_ndx);
        char* child_header = m_alloc.translate(child_ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
        if (child_is_leaf) {
            state.m_current_leaf.init(MemRef(child_header, child_ref, m_alloc));
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
    for (unsigned i = 0; i < m_keys.size(); i++) {
        int64_t key_value = m_keys.get(i) + key_offset;
        std::cout << lead << std::hex << "split: " << key_value << std::dec << std::endl;
        m_tree_top.get_node(m_children.get_as_ref(i))->dump_objects(key_value, lead + "   ");
    }
}

void ClusterNodeInner::move(size_t ndx, ClusterNode* new_node, int64_t key_adj)
{
    auto new_cluster_node_inner = static_cast<ClusterNodeInner*>(new_node);
    for (size_t i = ndx; i < m_children.size(); i++) {
        new_cluster_node_inner->m_children.add(m_children.get(i));
    }
    for (size_t i = ndx; i < m_keys.size(); i++) {
        new_cluster_node_inner->m_keys.add(m_keys.get(i) - key_adj);
    }
    m_children.truncate(ndx);
    m_keys.truncate(ndx);
}

size_t ClusterNodeInner::get_tree_size() const
{
    size_t tree_size = 0;
    traverse(
        [&tree_size](const Cluster* cluster, int64_t) {
            tree_size += cluster->node_size();
            return false;
        },
        0);
    return tree_size;
}

bool ClusterNodeInner::traverse(ClusterTree::TraverseFunction func, int64_t key_offset) const
{
    unsigned sz = node_size();

    for (unsigned i = 0; i < sz; i++) {
        ref_type ref = m_children.get_as_ref(i);
        char* header = m_alloc.translate(ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
        MemRef mem(header, ref, m_alloc);
        int64_t offs = key_offset + get_key(i);
        if (child_is_leaf) {
            Cluster leaf(m_alloc, m_tree_top);
            leaf.init(mem);
            if (func(&leaf, offs)) {
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

int64_t ClusterNodeInner::get_last_key() const
{
    unsigned last_ndx = node_size() - 1;

    ref_type ref = m_children.get_as_ref(last_ndx);
    char* header = m_alloc.translate(ref);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
    MemRef mem(header, ref, m_alloc);
    int64_t offset = m_keys.get(last_ndx);
    if (child_is_leaf) {
        Cluster leaf(m_alloc, m_tree_top);
        leaf.init(mem);
        return offset + leaf.get_last_key();
    }
    else {
        ClusterNodeInner node(m_alloc, m_tree_top);
        node.init(mem);
        return offset + node.get_last_key();
    }
}

/********************************* Cluster ***********************************/

template <class T>
inline void Cluster::do_create(size_t col_ndx)
{
    T arr(m_alloc);
    arr.create();
    arr.set_parent(this, col_ndx + 1);
    arr.update_parent();
}

void Cluster::create()
{
    // Create array with the required size
    size_t nb_columns = m_tree_top.get_spec().get_column_count();
    Array::create(type_HasRefs, false, nb_columns + 1);
    m_keys.create(Array::type_Normal);
    m_keys.update_parent();
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        int attr = m_tree_top.get_spec().get_column_attr(col_ndx);

        if (attr & col_attr_List) {
            ArrayInteger arr(m_alloc);
            arr.create(type_HasRefs);
            arr.set_parent(this, col_ndx + 1);
            arr.update_parent();

            continue;
        }
        switch (m_tree_top.get_spec().get_column_type(col_ndx)) {
            case col_type_Int:
                if (attr & col_attr_Nullable) {
                    do_create<ArrayIntNull>(col_ndx);
                }
                else {
                    do_create<ArrayInteger>(col_ndx);
                }
                break;
            case col_type_Bool:
                do_create<ArrayBool>(col_ndx);
                break;
            case col_type_Float:
                do_create<ArrayFloat>(col_ndx);
                break;
            case col_type_Double:
                do_create<ArrayDouble>(col_ndx);
                break;
            case col_type_String:
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
                // Silently ignore unsupported types
                // TODO: Eventually we should have an assert here
                break;
        }
    }
}

void Cluster::init(MemRef mem)
{
    Array::init_from_mem(mem);
    m_keys.init_from_ref(Array::get_as_ref(0));
}

bool Cluster::update_from_parent(size_t old_baseline) noexcept
{
    if (Array::update_from_parent(old_baseline)) {
        m_keys.update_from_parent(old_baseline);
        return true;
    }
    return false;
}

MemRef Cluster::ensure_writeable(Key)
{
    copy_on_write();
    return get_mem();
}

template <class T>
inline void Cluster::do_insert_row(size_t ndx, size_t col_ndx, int attr)
{
    if (attr & col_attr_List) {
        ArrayInteger arr(m_alloc);
        arr.set_parent(this, col_ndx + 1);
        arr.init_from_parent();
        arr.insert(ndx, 0);
    }
    else {
        T arr(m_alloc);
        arr.set_parent(this, col_ndx + 1);
        arr.init_from_parent();
        arr.insert(ndx, T::default_value(attr & col_attr_Nullable));
    }
}

void Cluster::insert_row(size_t ndx, Key k)
{
    m_keys.insert(ndx, k.value);
    size_t nb_columns = size() - 1;
    for (size_t col_ndx = 0; col_ndx < nb_columns; col_ndx++) {
        int attr = m_tree_top.get_spec().get_column_attr(col_ndx);

        if (attr & col_attr_List) {
            do_insert_row<ArrayInteger>(ndx, col_ndx, attr);
            continue;
        }

        switch (m_tree_top.get_spec().get_column_type(col_ndx)) {
            case col_type_Int:
                if (attr) {
                    do_insert_row<ArrayIntNull>(ndx, col_ndx, attr);
                }
                else {
                    do_insert_row<ArrayInteger>(ndx, col_ndx, attr);
                }
                break;
            case col_type_Bool:
                do_insert_row<ArrayBool>(ndx, col_ndx, attr);
                break;
            case col_type_Float:
                do_insert_row<ArrayFloat>(ndx, col_ndx, attr);
                break;
            case col_type_Double:
                do_insert_row<ArrayDouble>(ndx, col_ndx, attr);
                break;
            case col_type_String:
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
    src.set_parent(this, col_ndx + 1);
    src.init_from_parent();

    T dst(m_alloc);
    dst.set_parent(to, col_ndx + 1);
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
        int attr = m_tree_top.get_spec().get_column_attr(col_ndx);

        if (attr & col_attr_List) {
            do_move<ArrayInteger>(ndx, col_ndx, new_leaf);
            continue;
        }

        switch (m_tree_top.get_spec().get_column_type(col_ndx)) {
            case col_type_Int:
                if (attr & col_attr_Nullable) {
                    do_move<ArrayIntNull>(ndx, col_ndx, new_leaf);
                }
                else {
                    do_move<ArrayInteger>(ndx, col_ndx, new_leaf);
                }
                break;
            case col_type_Bool:
                do_move<ArrayBool>(ndx, col_ndx, new_leaf);
                break;
            case col_type_Float:
                do_move<ArrayFloat>(ndx, col_ndx, new_leaf);
                break;
            case col_type_Double:
                do_move<ArrayDouble>(ndx, col_ndx, new_leaf);
                break;
            case col_type_String:
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
    Array::insert(col_ndx + 1, from_ref(arr.get_ref()));
}

void Cluster::insert_column(size_t col_ndx)
{
    int attr = m_tree_top.get_spec().get_column_attr(col_ndx);
    if (attr & col_attr_List) {
        size_t sz = node_size();

        ArrayInteger arr(m_alloc);
        arr.create(type_HasRefs);
        for (size_t i = 0; i < sz; i++) {
            arr.add(0);
        }
        Array::insert(col_ndx + 1, from_ref(arr.get_ref()));
        return;
    }
    bool nullable = attr & col_attr_Nullable;

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
            do_insert_column<ArrayBool>(col_ndx, nullable);
            break;
        case col_type_Float:
            do_insert_column<ArrayFloat>(col_ndx, nullable);
            break;
        case col_type_Double:
            do_insert_column<ArrayDouble>(col_ndx, nullable);
            break;
        case col_type_String:
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
            // We need to insert something in spite we don't support the type yet
            // TODO: Eventually we should have an assert here
            Array::insert(col_ndx + 1, 0);
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

ref_type Cluster::insert(Key k, ClusterNode::State& state)
{
    int64_t current_key_value = -1;
    size_t ndx = m_keys.lower_bound_int(k.value);
    size_t sz = node_size();
    if (ndx < sz) {
        current_key_value = m_keys.get(ndx);
        if (k.value == current_key_value) {
            throw InvalidKey("Key already used");
        }
    }

    ref_type ret = 0;

    REALM_ASSERT_DEBUG(sz <= REALM_MAX_BPNODE_SIZE);
    if (REALM_LIKELY(sz < REALM_MAX_BPNODE_SIZE)) {
        insert_row(ndx, k); // Throws
        state.ref = get_ref();
        state.index = ndx;
    }
    else {
        // Split leaf node
        Cluster new_leaf(m_alloc, m_tree_top);
        new_leaf.create();
        if (ndx == sz) {
            new_leaf.insert_row(0, Key(0)); // Throws
            state.split_key = k.value;
            state.ref = new_leaf.get_ref();
            state.index = 0;
        }
        else {
            move(ndx, &new_leaf, current_key_value);
            insert_row(ndx, k); // Throws
            state.ref = get_ref();
            state.split_key = current_key_value;
            state.index = ndx;
        }
        ret = new_leaf.get_ref();
    }

    if (Replication* repl = m_alloc.get_replication()) {
        repl->create_object(m_tree_top.get_owner(), k);
    }

    return ret;
}

void Cluster::get(Key k, ClusterNode::State& state) const
{
    state.ref = get_ref();
    state.index = m_keys.lower_bound_int(k.value);
    if (state.index == m_keys.size() || m_keys.get(state.index) != k.value) {
        throw InvalidKey("Key not found");
    }
}

template <class T>
inline void Cluster::do_erase(size_t ndx, size_t col_ndx)
{
    T values(m_alloc);
    values.set_parent(this, col_ndx + 1);
    values.init_from_parent();
    values.erase(ndx);
}

namespace realm {
template <>
inline void Cluster::do_erase<ArrayKey>(size_t ndx, size_t col_ndx)
{
    ArrayKey values(m_alloc);
    values.set_parent(this, col_ndx + 1);
    values.init_from_parent();

    TableRef target_table = _impl::TableFriend::get_opposite_link_table(*m_tree_top.get_owner(), col_ndx);
    const Spec& target_table_spec = _impl::TableFriend::get_spec(*target_table);
    size_t backlink_col =
        target_table_spec.find_backlink_column(m_tree_top.get_owner()->get_index_in_group(), col_ndx);

    Key old_key = values.get(ndx);

    if (old_key != realm::null_key) {
        Obj target_obj = target_table->get_object(old_key);
        target_obj.remove_one_backlink(backlink_col, Key(get_key(ndx))); // Throws
    }


    values.erase(ndx);
}
}

unsigned Cluster::erase(Key k)
{
    size_t ndx = m_keys.lower_bound_int(k.value);
    if (ndx == m_keys.size() || m_keys.get(ndx) != k.value) {
        throw InvalidKey("Key not found");
    }

    const Spec& spec = m_tree_top.get_spec();
    size_t num_cols = spec.get_column_count();
    size_t num_public_cols = spec.get_public_column_count();
    // We must start with backlink columns in case the corresponding link
    // columns are in the same table so that the link columns are not updated
    // twice. Backlink columns will nullify the rows in connected link columns
    // first so by the time we get to the link column in this loop, the rows to
    // be removed have already been nullified.
    //
    // This phase also generates replication instructions documenting the side-
    // effects of deleting the object (i.e. link nullifications). These instructions
    // must come before the actual deletion of the object, but at the same time
    // the Replication object may need a consistent view of the row (not including
    // link columns). Therefore we first delete the row in backlink columns, then
    // generate the instruction, and then delete the row in the remaining columns.
    for (size_t col_ndx = num_cols - 1; col_ndx >= num_public_cols; --col_ndx) {
        REALM_ASSERT(spec.get_column_type(col_ndx) == col_type_BackLink);
        do_erase<ArrayBacklink>(ndx, col_ndx);
    }

    if (Replication* repl = m_alloc.get_replication()) {
        repl->remove_object(m_tree_top.get_owner(), k);
    }

    for (size_t col_ndx = 0; col_ndx < num_public_cols; col_ndx++) {
        int attr = spec.get_column_attr(col_ndx);
        if (attr & col_attr_List) {
            ArrayInteger values(m_alloc);
            values.set_parent(this, col_ndx + 1);
            values.init_from_parent();
            ref_type ref = values.get_as_ref(ndx);

            if (ref) {
                Array::destroy_deep(ref, m_alloc);
            }

            values.erase(ndx);

            continue;
        }
        switch (spec.get_column_type(col_ndx)) {
            case col_type_Int:
                if (attr & col_attr_Nullable) {
                    do_erase<ArrayIntNull>(ndx, col_ndx);
                }
                else {
                    do_erase<ArrayInteger>(ndx, col_ndx);
                }
                break;
            case col_type_Bool:
                do_erase<ArrayBool>(ndx, col_ndx);
                break;
            case col_type_Float:
                do_erase<ArrayFloat>(ndx, col_ndx);
                break;
            case col_type_Double:
                do_erase<ArrayDouble>(ndx, col_ndx);
                break;
            case col_type_String:
                do_erase<ArrayString>(ndx, col_ndx);
                break;
            case col_type_Binary:
                do_erase<ArrayBinary>(ndx, col_ndx);
                break;
            case col_type_Timestamp:
                do_erase<ArrayTimestamp>(ndx, col_ndx);
                break;
            case col_type_Link:
                do_erase<ArrayKey>(ndx, col_ndx);
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
    }

    m_keys.erase(ndx);

    return node_size();
}

void Cluster::dump_objects(int64_t key_offset, std::string lead) const
{
    std::cout << lead << "leaf - size: " << node_size() << std::endl;
    for (unsigned i = 0; i < node_size(); i++) {
        std::cout << lead << "key: " << std::hex << m_keys.get(i) + key_offset << std::dec;
        for (size_t j = 1; j < size(); j++) {
            switch (m_tree_top.get_spec().get_column_type(j - 1)) {
                case col_type_Int: {
                    bool nullable = m_tree_top.get_spec().get_column_attr(j - 1) & col_attr_Nullable;
                    Array arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    if (nullable) {
                        auto arr_int_null = reinterpret_cast<ArrayIntNull*>(&arr);
                        arr_int_null->init_from_ref(ref);
                        if (arr_int_null->is_null(i)) {
                            std::cout << ", null";
                        }
                        else {
                            std::cout << ", " << arr_int_null->get(i).value();
                        }
                    }
                    else {
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

/******************************** ClusterTree ********************************/

ClusterTree::ClusterTree(Table* owner, Allocator& alloc)
    : m_owner(owner)
    , m_root(std::make_unique<Cluster>(alloc, *this))
{
}

MemRef ClusterTree::create_empty_cluster(Allocator& alloc)
{
    Array arr(alloc);
    arr.create(Array::type_HasRefs); // Throws

    {
        MemRef mem = Array::create_empty_array(Array::type_Normal, false, alloc); // Throws
        arr.add(from_ref(mem.get_ref()));                                         // Throws
    }
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
        new_root = std::make_unique<Cluster>(alloc, *this);
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


uint64_t ClusterTree::bump_version()
{
    m_owner->bump_version();
    return m_owner->get_version_counter();
}

uint64_t ClusterTree::get_version_counter() const
{
    return m_owner->get_version_counter();
}

void ClusterTree::clear()
{
    m_root->destroy_deep();

    std::unique_ptr<ClusterNode> leaf = std::make_unique<Cluster>(m_root->get_alloc(), *this);
    leaf->create();
    replace_root(std::move(leaf));
    m_size = 0;
}

Obj ClusterTree::insert(Key k)
{
    ClusterNode::State state;
    ref_type new_sibling_ref = m_root->insert(k, state);
    if (REALM_UNLIKELY(new_sibling_ref)) {
        auto new_root = std::make_unique<ClusterNodeInner>(m_root->get_alloc(), *this);
        new_root->create();

        new_root->add(m_root->get_ref());                // Throws
        new_root->add(new_sibling_ref, state.split_key); // Throws

        replace_root(std::move(new_root));
    }
    m_size++;
    return Obj(this, state.ref, k, state.index);
}

ConstObj ClusterTree::get(Key k) const
{
    ClusterNode::State state;
    m_root->get(k, state);
    return ConstObj(this, state.ref, k, state.index);
}

Obj ClusterTree::get(Key k)
{
    ClusterNode::State state;
    m_root->get(k, state);
    return Obj(this, state.ref, k, state.index);
}

void ClusterTree::erase(Key k)
{
    unsigned root_size = m_root->erase(k);
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

bool ClusterTree::get_leaf(Key key, ClusterNode::IteratorState& state) const noexcept
{
    state.clear();

    if (m_root->is_leaf()) {
        Cluster* node = static_cast<Cluster*>(m_root.get());
        state.m_current_leaf.init(node->get_mem());
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
        return func(static_cast<Cluster*>(m_root.get()), 0);
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
        node = std::make_unique<Cluster>(alloc, *this);
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

ClusterTree::ConstIterator::ConstIterator(const ClusterTree& t, size_t ndx)
    : m_tree(t)
    , m_leaf(t.get_alloc(), t)
    , m_state(m_leaf)
{
    if (ndx == 0) {
        // begin
        m_key = load_leaf(Key(0));
    }
    else {
        // end
        m_key = null_key;
    }
}

ClusterTree::ConstIterator::ConstIterator(const ClusterTree& t, Key key)
    : m_tree(t)
    , m_leaf(t.get_alloc(), t)
    , m_state(m_leaf)
    , m_key(key)
{
}

Key ClusterTree::ConstIterator::load_leaf(Key key) const
{
    m_version = m_tree.get_version_counter();
    // 'key' may or may not exist. If it does not exist, state is updated
    // to point to the next object in line.
    if (m_tree.get_leaf(key, m_state)) {
        // Get the actual key value
        return Key(m_leaf.get_key(m_state.m_current_index) + m_state.m_key_offset);
    }
    else {
        // end of table
        return null_key;
    }
}

ClusterTree::ConstIterator::pointer Table::ConstIterator::operator->() const
{
    if (m_version != m_tree.get_version_counter()) {
        Key k = load_leaf(m_key);
        if (k != m_key)
            throw std::out_of_range("Object was deleted");
    }

    REALM_ASSERT(m_leaf.is_attached());

    return new (&m_obj_cache_storage)
        Obj(const_cast<ClusterTree*>(&m_tree), m_leaf.get_ref(), m_key, m_state.m_current_index);
}

ClusterTree::ConstIterator& Table::ConstIterator::operator++()
{
    if (m_version != m_tree.get_version_counter()) {
        Key k = load_leaf(m_key);
        if (k != m_key) {
            // Objects was deleted. k points to the next object
            m_key = k;
            return *this;
        }
    }
    m_state.m_current_index++;
    if (m_state.m_current_index == m_leaf.node_size()) {
        m_key = load_leaf(Key(m_key.value + 1));
    }
    else {
        m_key = Key(m_leaf.get_key(m_state.m_current_index) + m_state.m_key_offset);
    }
    return *this;
}
