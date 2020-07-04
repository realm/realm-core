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

#include "realm/table_cluster_tree.hpp"
#include "realm/table.hpp"
#include "realm/replication.hpp"
#include "realm/array_key.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_backlink.hpp"
#include "realm/array_string.hpp"
#include "realm/group.hpp"

namespace realm {

TableClusterTree::TableClusterTree(Table* owner, Allocator& alloc, size_t top_position_for_cluster_tree)
    : ClusterTree(alloc)
    , m_owner(owner)
    , m_top_position_for_cluster_tree(top_position_for_cluster_tree)
{
}

TableClusterTree::~TableClusterTree() {}

void TableClusterTree::clear(CascadeState& state)
{
    m_owner->clear_indexes();

    if (state.m_group) {
        remove_all_links(state); // This will also delete objects loosing their last strong link
    }

    // We no longer have "clear table" instruction, so we have to report the removal of each
    // object individually
    if (Replication* repl = m_owner->get_repl()) {
        // Go through all clusters
        traverse([repl, this](const Cluster* cluster) {
            auto sz = cluster->node_size();
            for (size_t i = 0; i < sz; i++) {
                repl->remove_object(m_owner, cluster->get_real_key(i));
            }
            // Continue
            return false;
        });
    }

    ClusterTree::clear();
}


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

    auto upgrade = [col_key, &keys](Cluster* cluster) {
        cluster->upgrade_string_to_enum(col_key, keys);
    };

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

void TableClusterTree::cleanup_key(ObjKey k)
{
    m_owner->free_local_id_after_hash_collision(k);
    m_owner->erase_from_search_indexes(k);
}

void TableClusterTree::update_indexes(ObjKey k, const FieldValues& init_values)
{
    m_owner->update_indexes(k, init_values);
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

std::unique_ptr<ClusterNode> TableClusterTree::get_root_from_parent()
{
    return create_root_from_parent(&m_owner->m_top, m_top_position_for_cluster_tree);
}

void TableClusterTree::remove_all_links(CascadeState& state)
{
    Allocator& alloc = get_alloc();
    // This function will add objects that should be deleted to 'state'
    auto func = [this, &state, &alloc](const Cluster* cluster) {
        auto remove_link_from_column = [&](ColKey col_key) {
            // Prevent making changes to table that is going to be removed anyway
            // Furthermore it is a prerequisite for using 'traverse' that the tree
            // is not modified
            if (get_owning_table()->links_to_self(col_key)) {
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
                BPlusTree<ObjKey> links(alloc);
                for (size_t i = 0; i < sz; i++) {
                    if (ref_type ref = values.get_as_ref(i)) {
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
        m_owner->for_each_and_every_column(remove_link_from_column);
        // Continue
        return false;
    };

    // Go through all clusters
    traverse(func);

    const_cast<Table*>(get_owning_table())->remove_recursive(state);
}

/***********************  TableClusterTree::Iterator  ************************/

auto TableClusterTree::Iterator::operator[](size_t n) -> reference
{
    auto k = go(n);
    if (m_obj.get_key() != k) {
        new (&m_obj) Obj(m_table, m_leaf.get_mem(), k, m_state.m_current_index);
    }
    return m_obj;
}

TableClusterTree::Iterator::pointer TableClusterTree::Iterator::operator->() const
{
    if (update() || m_key != m_obj.get_key()) {
        new (&m_obj) Obj(m_table, m_leaf.get_mem(), m_key, m_state.m_current_index);
    }

    return &m_obj;
}

} // namespace realm
