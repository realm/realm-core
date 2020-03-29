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
#include "realm/table.hpp"

namespace realm {

TableClusterTree::TableClusterTree(Table* owner, Allocator& alloc, size_t top_position_for_cluster_tree)
    : ClusterTree(alloc)
    , m_owner(owner)
    , m_top_position_for_cluster_tree(top_position_for_cluster_tree)
{
}

TableClusterTree::~TableClusterTree() {}

void TableClusterTree::enumerate_string_column(ColKey col_key)
{
    Allocator& alloc = get_alloc();

    ArrayString keys(alloc);
    ArrayString leaf(alloc);
    keys.create();

    auto collect_strings = [col_key, &leaf, &keys](const Cluster* cluster) {
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

    auto upgrade = [col_key, &keys](Cluster* cluster) { cluster->upgrade_string_to_enum(col_key, keys); };

    // Populate 'keys' array
    traverse(collect_strings);

    // Store key strings in spec
    size_t spec_ndx = m_owner->colkey2spec_ndx(col_key);
    const_cast<Spec*>(&m_owner->m_spec)->upgrade_string_to_enum(spec_ndx, keys.get_ref());

    // Replace column in all clusters
    update(upgrade);
}

TableRef TableClusterTree::get_table_ref() const
{
    REALM_ASSERT(m_owner != nullptr);
    // as safe as storing the TableRef locally in the ClusterTree,
    // because the cluster tree and the table is basically one object :-O
    return m_owner->m_own_ref;
}

void TableClusterTree::for_each_and_every_column(ColIterateFunction func) const
{
    m_owner->for_each_and_every_column(func);
}

void TableClusterTree::set_spec(ArrayPayload& arr, ColKey::Idx col_ndx) const
{
    auto spec_ndx = m_owner->leaf_ndx2spec_ndx(col_ndx);
    arr.set_spec(&m_owner->m_spec, spec_ndx);
}

bool TableClusterTree::is_string_enum_type(ColKey::Idx col_ndx) const
{
    size_t spec_ndx = m_owner->leaf_ndx2spec_ndx(col_ndx);
    return m_owner->m_spec.is_string_enum_type(spec_ndx);
}

ArrayParent* TableClusterTree::get_parent() const
{
    return &m_owner->m_top;
}

size_t TableClusterTree::num_leaf_cols() const
{
    return m_owner->num_leaf_cols();
}

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
    if (m_key) {
        auto k = load_leaf(m_key);
        if (k != m_key)
            throw std::runtime_error("ConstIterator copy failed");
    }
    m_leaf_start_pos = m_position - m_state.m_current_index;
}

size_t ClusterTree::Iterator::get_position()
{
    try {
        return m_tree.get_ndx(m_key);
    }
    catch (...) {
        throw std::runtime_error("Outdated iterator");
    }
    return 0; // dummy
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

ObjKey ClusterTree::Iterator::go(size_t n)
{
    if (m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        // reload
        m_position = get_position(); // Will throw if base object is deleted
        load_leaf(m_key);
    }

    auto abs_pos = n + m_position;

    auto leaf_node_size = m_leaf.node_size();
    ObjKey k;
    if (abs_pos < m_leaf_start_pos || abs_pos >= (m_leaf_start_pos + leaf_node_size)) {
        if (abs_pos >= m_tree.size()) {
            throw std::out_of_range("Index out of range");
        }
        // Find cluster holding requested position

        auto s = m_tree.get(abs_pos, k);
        m_state.init(s, k);
        m_leaf_start_pos = abs_pos - s.index;
    }
    else {
        m_state.m_current_index = (abs_pos - m_leaf_start_pos);
        k = m_leaf.get_real_key(m_state.m_current_index);
    }
    // The state no longer corresponds to m_key
    m_leaf_invalid = true;
    return k;
}

bool ClusterTree::Iterator::update() const
{
    if (m_leaf_invalid || m_storage_version != m_tree.get_storage_version(m_instance_version)) {
        ObjKey k = load_leaf(m_key);
        m_leaf_invalid = (k != m_key);
        if (m_leaf_invalid) {
            throw std::runtime_error("Outdated iterator");
        }
        return true;
    }

    REALM_ASSERT(m_leaf.is_attached());
    return false;
}

auto TableClusterTree::ConstIterator::operator[](size_t n) -> reference
{
    auto k = go(n);
    if (m_obj.get_key() != k) {
        new (&m_obj) Obj(m_table, m_leaf.get_mem(), k, m_state.m_current_index);
    }
    return m_obj;
}

TableClusterTree::ConstIterator::pointer TableClusterTree::ConstIterator::operator->() const
{
    if (update() || m_key != m_obj.get_key()) {
        new (&m_obj) Obj(m_table, m_leaf.get_mem(), m_key, m_state.m_current_index);
    }

    return &m_obj;
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
