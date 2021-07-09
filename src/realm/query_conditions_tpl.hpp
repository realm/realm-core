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
    using ResultType = typename aggregate_operations::Sum<T>::ResultType;
    explicit QueryStateSum(size_t limit = -1)
        : QueryStateBase(limit)
    {
    }
    bool match(size_t, Mixed value) noexcept final
    {
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

template <class R>
class QueryStateMin : public QueryStateBase {
public:
    explicit QueryStateMin(size_t limit = -1)
        : QueryStateBase(limit)
    {
    }
    bool match(size_t index, Mixed value) noexcept final
    {
        if (!value.is_null()) {
            auto v = value.get<R>();
            if (!m_state.accumulate(v)) {
                return true; // no match, continue searching
            }
            ++m_match_count;
            m_minmax_key = (m_key_values ? m_key_values->get(index) : 0) + m_key_offset;
        }
        return (m_limit > m_match_count);
    }
    R get_min() const
    {
        return m_state.is_null() ? R{} : m_state.result();
    }

private:
    aggregate_operations::Minimum<typename util::RemoveOptional<R>::type> m_state;
};

template <class R>
class QueryStateMax : public QueryStateBase {
public:
    explicit QueryStateMax(size_t limit = -1)
        : QueryStateBase(limit)
    {
    }
    bool match(size_t index, Mixed value) noexcept final
    {
        if (!value.is_null()) {
            auto v = value.get<R>();
            if (!m_state.accumulate(v)) {
                return true; // no match, continue search
            }
            ++m_match_count;
            m_minmax_key = (m_key_values ? m_key_values->get(index) : 0) + m_key_offset;
        }
        return (m_limit > m_match_count);
    }
    R get_max() const
    {
        return m_state.is_null() ? R{} : m_state.result();
    }

private:
    aggregate_operations::Maximum<typename util::RemoveOptional<R>::type> m_state;
};

} // namespace realm

#endif /* REALM_QUERY_CONDITIONS_TPL_HPP */
