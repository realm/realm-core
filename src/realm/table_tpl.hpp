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
#include <realm/aggregate.hpp>

namespace realm {
template <Action action, typename T, typename R>
R Table::aggregate(ColKey column_key, T value, size_t* resultcount, Key* return_ndx) const
{
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    using ResultType = typename AggregateResultType<T, action>::result_type;
    bool nullable = is_nullable(column_key);
    QueryState<ResultType> st(action);
    LeafType leaf(get_alloc());
    size_t column_ndx = colkey2ndx(column_key);

    traverse_clusters([value, &leaf, column_ndx, &st, nullable](const Cluster* cluster) {
        // direct aggregate on the leaf
        cluster->init_leaf(column_ndx, &leaf);
        Aggregate<action, T> aggr(leaf, nullable);
        st.m_key_offset = cluster->get_offset();
        st.m_key_values = cluster->get_key_array();

        aggr(st, value);
        // We should continue
        return false;
    });

    if (resultcount) {
        *resultcount = st.m_match_count;
    }

    if (return_ndx) {
        *return_ndx = st.m_minmax_index;
    }

    return st.m_match_count ? st.m_state : R{};
}

template <typename T>
double Table::average(ColKey col_key, size_t* resultcount) const
{
    using ResultType = typename AggregateResultType<T, act_Sum>::result_type;
    size_t count;
    auto sum = aggregate<act_Sum, T, ResultType>(col_key, T{}, &count, nullptr);
    double avg = 0;
    if (count != 0)
        avg = double(sum) / count;
    if (resultcount)
        *resultcount = count;
    return avg;
}
}


#endif /* REALM_TABLE_TPL_HPP */
