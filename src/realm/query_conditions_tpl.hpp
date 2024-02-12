/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

#ifndef REALM_QUERY_CONDITIONS_TPL_HPP
#define REALM_QUERY_CONDITIONS_TPL_HPP

#include <realm/aggregate_ops.hpp>
#include <realm/query_conditions.hpp>
#include <realm/column_type_traits.hpp>

#include <cmath>

namespace realm {

template <class T>
class QueryStateSum : public QueryStateBase {
public:
    using Type = T;
    using ResultType = typename aggregate_operations::Sum<T>::ResultType;
    using QueryStateBase::QueryStateBase;
    bool match(size_t index, Mixed value) noexcept final
    {
        if (m_source_column) {
            REALM_ASSERT_DEBUG(value.is_null());
            value = m_source_column->get_any(index);
        }
        if (!value.is_null()) {
            auto v = value.get<T>();
            if (!m_state.accumulate(v))
                return true; // no match, continue searching
            ++m_match_count;
        }
        return (m_limit > m_match_count);
    }
    bool match(size_t index) noexcept final
    {
        REALM_ASSERT(m_source_column);
        Mixed value{m_source_column->get_any(index)};
        if (!value.is_null()) {
            auto v = value.get<T>();
            if (!m_state.accumulate(v))
                return true; // no match, continue searching
            ++m_match_count;
        }
        return (m_limit > m_match_count);
    }
    ResultType result_sum() const
    {
        return m_state.result();
    }
    size_t result_count() const
    {
        return m_state.items_counted();
    }

private:
    aggregate_operations::Sum<typename util::RemoveOptional<T>::type> m_state;
};

template <class R, template <class> class State>
class QueryStateMinMax : public QueryStateBase {
public:
    using QueryStateBase::QueryStateBase;
    bool match(size_t index, Mixed value) noexcept final
    {
        if (m_source_column) {
            REALM_ASSERT_DEBUG(value.is_null());
            value = m_source_column->get_any(index);
        }
        if (!value.is_null()) {
            auto v = value.get<R>();
            if (!m_state.accumulate(v)) {
                return true; // no match, continue searching
            }
            ++m_match_count;
            m_minmax_key = (m_key_values ? m_key_values->get(index) : index) + m_key_offset;
        }
        return m_limit > m_match_count;
    }
    bool match(size_t index) noexcept final
    {
        REALM_ASSERT(m_source_column);
        Mixed value{m_source_column->get_any(index)};
        if (!value.is_null()) {
            auto v = value.get<R>();
            if (!m_state.accumulate(v)) {
                return true; // no match, continue searching
            }
            ++m_match_count;
            m_minmax_key = (m_key_values ? m_key_values->get(index) : index) + m_key_offset;
        }
        return m_limit > m_match_count;
    }
    Mixed get_result() const
    {
        return m_state.is_null() ? Mixed() : m_state.result();
    }

private:
    State<typename util::RemoveOptional<R>::type> m_state;
};

template <class R>
class QueryStateMin : public QueryStateMinMax<R, aggregate_operations::Minimum> {
public:
    using QueryStateMinMax<R, aggregate_operations::Minimum>::QueryStateMinMax;
};

template <class R>
class QueryStateMax : public QueryStateMinMax<R, aggregate_operations::Maximum> {
public:
    using QueryStateMinMax<R, aggregate_operations::Maximum>::QueryStateMinMax;
};

template <class Target>
class AggregateHelper {
public:
    static std::optional<Mixed> sum(const Table& table, const Target& target, ColKey col_key)
    {
        table.check_column(col_key);
        if (col_key.is_collection())
            return std::nullopt;
        switch (table.get_column_type(col_key)) {
            case type_Int:
                if (table.is_nullable(col_key)) {
                    return sum<std::optional<int64_t>>(target, col_key);
                }
                else {
                    return sum<int64_t>(target, col_key);
                }
            case type_Float:
                return sum<float>(target, col_key);
            case type_Double:
                return sum<double>(target, col_key);
            case type_Decimal:
                return sum<Decimal128>(target, col_key);
            case type_Mixed:
                return sum<Mixed>(target, col_key);
            default:
                return std::nullopt;
        }
    }

    static std::optional<Mixed> avg(const Table& table, const Target& target, ColKey col_key, size_t* value_count)
    {
        table.check_column(col_key);
        if (col_key.is_collection())
            return std::nullopt;
        switch (table.get_column_type(col_key)) {
            case type_Int:
                if (table.is_nullable(col_key)) {
                    return average<std::optional<int64_t>>(target, col_key, value_count);
                }
                else {
                    return average<int64_t>(target, col_key, value_count);
                }
            case type_Float:
                return average<float>(target, col_key, value_count);
            case type_Double:
                return average<double>(target, col_key, value_count);
            case type_Decimal:
                return average<Decimal128>(target, col_key, value_count);
            case type_Mixed:
                return average<Mixed>(target, col_key, value_count);
            default:
                return std::nullopt;
        }
    }

    static std::optional<Mixed> min(const Table& table, const Target& target, ColKey col_key, ObjKey* return_ndx)
    {
        return minmax<QueryStateMin>(table, target, col_key, return_ndx);
    }

    static std::optional<Mixed> max(const Table& table, const Target& target, ColKey col_key, ObjKey* return_ndx)
    {
        return minmax<QueryStateMax>(table, target, col_key, return_ndx);
    }

private:
    template <typename T>
    static Mixed average(const Target& target, ColKey col_key, size_t* value_count)
    {
        QueryStateSum<typename util::RemoveOptional<T>::type> st;
        target.template aggregate<T>(st, col_key);
        if (value_count)
            *value_count = st.result_count();
        if (st.result_count() == 0)
            return Mixed();

        using Result = typename aggregate_operations::Average<typename util::RemoveOptional<T>::type>::ResultType;
        return Result(st.result_sum()) / st.result_count();
    }

    template <typename T>
    static Mixed sum(const Target& target, ColKey col_key)
    {
        if (col_key.is_collection())
            return std::nullopt;
        QueryStateSum<typename util::RemoveOptional<T>::type> st;
        target.template aggregate<T>(st, col_key);
        return st.result_sum();
    }

    template <template <typename> typename QueryState>
    static std::optional<Mixed> minmax(const Table& table, const Target& target, ColKey col_key, ObjKey* return_ndx)
    {
        table.check_column(col_key);
        if (col_key.is_collection())
            return std::nullopt;
        switch (table.get_column_type(col_key)) {
            case type_Int:
                if (table.is_nullable(col_key)) {
                    return minmax<QueryState, std::optional<int64_t>>(target, col_key, return_ndx);
                }
                else {
                    return minmax<QueryState, int64_t>(target, col_key, return_ndx);
                }
            case type_Float:
                return minmax<QueryState, float>(target, col_key, return_ndx);
            case type_Double:
                return minmax<QueryState, double>(target, col_key, return_ndx);
            case type_Decimal:
                return minmax<QueryState, Decimal128>(target, col_key, return_ndx);
            case type_Timestamp:
                return minmax<QueryState, Timestamp>(target, col_key, return_ndx);
            case type_Mixed:
                return minmax<QueryState, Mixed>(target, col_key, return_ndx);
            default:
                return std::nullopt;
        }
    }

    template <template <typename> typename QueryState, typename T>
    static Mixed minmax(const Target& target, ColKey col_key, ObjKey* return_ndx)
    {
        QueryState<typename util::RemoveOptional<T>::type> st;
        target.template aggregate<T>(st, col_key);
        if (return_ndx)
            *return_ndx = ObjKey(st.m_minmax_key);
        return st.get_result();
    }
};

} // namespace realm

#endif /* REALM_QUERY_CONDITIONS_TPL_HPP */
