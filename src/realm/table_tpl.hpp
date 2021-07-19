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

#ifndef REALM_TABLE_TPL_HPP
#define REALM_TABLE_TPL_HPP

#include <realm/table.hpp>
#include <realm/query_conditions_tpl.hpp>

namespace realm {

template <typename T>
void Table::aggregate(QueryStateBase& st, ColKey column_key) const
{
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    LeafType leaf(get_alloc());

    auto f = [&leaf, column_key, &st](const Cluster* cluster) {
        // direct aggregate on the leaf
        cluster->init_leaf(column_key, &leaf);
        st.m_key_offset = cluster->get_offset();
        st.m_key_values = cluster->get_key_array();

        bool cont = true;
        size_t sz = leaf.size();
        for (size_t local_index = 0; cont && local_index < sz; local_index++) {
            auto v = leaf.get(local_index);
            cont = st.match(local_index, v);
        }
        // We should continue
        return false;
    };

    traverse_clusters(f);
}

template <typename T>
double Table::average(ColKey col_key, size_t* resultcount) const
{
    QueryStateSum<typename util::RemoveOptional<T>::type> st;
    aggregate<T>(st, col_key);
    auto sum = st.result_sum();
    double avg = 0;
    size_t items_counted = st.result_count();
    if (items_counted != 0)
        avg = double(sum) / items_counted;
    if (resultcount)
        *resultcount = items_counted;
    return avg;
}
}


#endif /* REALM_TABLE_TPL_HPP */
