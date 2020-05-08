/*************************************************************************
 *
 * Copyright 2017 Realm Inc.
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

#ifndef REALM_AGGREGATE_HPP
#define REALM_AGGREGATE_HPP

#include <realm/column_type_traits.hpp>
#include <realm/array.hpp>
#include <realm/query_conditions.hpp>

namespace realm {

namespace _aggr {

template <typename T>
inline bool is_null(const T& v)
{
    return v.is_null();
}

template <>
inline bool is_null<float>(const float& v)
{
    return null::is_null_float(v);
}

template <>
inline bool is_null<double>(const double& v)
{
    return null::is_null_float(v);
}

template <class LeafType>
struct FindInLeaf {

    template <Action action, class Condition, class T, class R>
    static bool find(const LeafType& leaf, T target, QueryState<R>& state)
    {
        Condition cond;
        bool cont = true;
        bool null_target = is_null(target);
        size_t sz = leaf.size();
        for (size_t local_index = 0; cont && local_index < sz; local_index++) {
            auto v = leaf.get(local_index);
            if (cond(v, target, is_null(v), null_target)) {
                cont = state.template match<action, false>(local_index, 0, v);
            }
        }
        return cont;
    }
};

template <>
struct FindInLeaf<ArrayInteger> {

    template <Action action, class Condition, class T, class R>
    static bool find(const ArrayInteger& leaf, T target, QueryState<R>& state)
    {
        const int c = Condition::condition;
        return leaf.find(c, action, target, 0, leaf.size(), 0, &state);
    }
};

template <>
struct FindInLeaf<ArrayIntNull> {

    template <Action action, class Condition, class T, class R>
    static bool find(const ArrayIntNull& leaf, T target, QueryState<R>& state)
    {
        constexpr int cond = Condition::condition;
        return leaf.find(cond, action, target, 0, leaf.size(), 0, &state);
    }
};

} // namespace _aggr

template <Action action, typename T>
class Aggregate {
public:
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    using ResultType = typename AggregateResultType<T, action>::result_type;

    Aggregate(const LeafType& leaf, bool nullable)
        : m_leaf(leaf)
        , m_nullable(nullable)
    {
    }
    bool operator()(QueryState<ResultType>& st, T value) const
    {
        if (action == act_Sum) {
            if (m_nullable)
                return _aggr::FindInLeaf<LeafType>::template find<act_Sum, NotNull, T, ResultType>(m_leaf, value, st);
            else
                return _aggr::FindInLeaf<LeafType>::template find<act_Sum, None, T, ResultType>(m_leaf, value, st);
        }
        else if (action == act_Count) {
            return _aggr::FindInLeaf<LeafType>::template find<action, Equal, T, ResultType>(m_leaf, value, st);
        }
        else {
            return _aggr::FindInLeaf<LeafType>::template find<action, NotNull, T, ResultType>(m_leaf, value, st);
        }
    }

private:
    const LeafType& m_leaf;
    bool m_nullable;
};

} // namespace realm

#endif // REALM_AGGREGATE_HPP
