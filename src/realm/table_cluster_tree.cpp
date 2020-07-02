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
#include "realm/index_string.hpp"
#include "realm/replication.hpp"
#include "realm/array_key.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_backlink.hpp"

namespace realm {

TableClusterTree::TableClusterTree(Table* owner, Allocator& alloc, size_t top_position_for_cluster_tree)
    : ClusterTree(owner, alloc, top_position_for_cluster_tree)
{
}

TableClusterTree::~TableClusterTree() {}

void TableClusterTree::clear(CascadeState& state)
{
    size_t num_cols = get_spec().get_public_column_count();
    for (size_t col_ndx = 0; col_ndx < num_cols; col_ndx++) {
        auto col_key = m_owner->spec_ndx2colkey(col_ndx);
        if (StringIndex* index = m_owner->get_search_index(col_key)) {
            index->clear();
        }
    }

    if (state.m_group) {
        remove_all_links(state); // This will also delete objects loosing their last strong link
    }

    // We no longer have "clear table" instruction, so we have to report the removal of each
    // object individually
    auto table = m_owner;
    if (Replication* repl = table->get_repl()) {
        // Go through all clusters
        traverse([repl, table](const Cluster* cluster) {
            auto sz = cluster->node_size();
            for (size_t i = 0; i < sz; i++) {
                repl->remove_object(table, cluster->get_real_key(i));
            }
            // Continue
            return false;
        });
    }

    m_root->destroy_deep();

    auto leaf = std::make_unique<Cluster>(0, m_root->get_alloc(), *this);
    leaf->create(m_owner->num_leaf_cols());
    replace_root(std::move(leaf));

    bump_content_version();
    bump_storage_version();

    m_size = 0;
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
        get_owner()->for_each_and_every_column(remove_link_from_column);
        // Continue
        return false;
    };

    // Go through all clusters
    traverse(func);

    m_owner->remove_recursive(state);
}

TableClusterTree::Iterator::pointer TableClusterTree::Iterator::operator->() const
{
    if (update() || m_key != m_obj.get_key()) {
        new (&m_obj) Obj(m_table, m_leaf.get_mem(), m_key, m_state.m_current_index);
    }

    return &m_obj;
}

auto TableClusterTree::Iterator::operator[](size_t n) -> reference
{
    auto k = go(n);
    if (m_obj.get_key() != k) {
        new (&m_obj) Obj(m_table, m_leaf.get_mem(), k, m_state.m_current_index);
    }
    return m_obj;
}

} // namespace realm
