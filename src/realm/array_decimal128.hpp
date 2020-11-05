/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#ifndef REALM_ARRAY_DECIMAL128_HPP
#define REALM_ARRAY_DECIMAL128_HPP

#include <realm/array.hpp>
#include <realm/decimal128.hpp>

namespace realm {

class ArrayDecimal128 : public ArrayPayload, private Array {
public:
    using value_type = Decimal128;

    using Array::Array;
    using Array::destroy;
    using Array::get_ref;
    using Array::init_from_mem;
    using Array::init_from_parent;
    using Array::size;
    using Array::truncate;
    using Array::update_parent;
    using Array::verify;

    static Decimal128 default_value(bool nullable)
    {
        return nullable ? Decimal128(realm::null()) : Decimal128(0);
    }

    void create()
    {
        auto mem = Array::create(type_Normal, false, wtype_Multiply, 0, 0, m_alloc); // Throws
        Array::init_from_mem(mem);
    }

    void init_from_ref(ref_type ref) noexcept override
    {
        Array::init_from_ref(ref);
    }

    void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept override
    {
        Array::set_parent(parent, ndx_in_parent);
    }

    bool is_null(size_t ndx) const
    {
        return this->get_width() == 0 || get(ndx).is_null();
    }

    Decimal128 get(size_t ndx) const
    {
        REALM_ASSERT(ndx < m_size);
        auto values = reinterpret_cast<Decimal128*>(this->m_data);
        return values[ndx];
    }

    Mixed get_any(size_t ndx) const override
    {
        return Mixed(get(ndx));
    }

    void add(Decimal128 value)
    {
        insert(size(), value);
    }

    void set(size_t ndx, Decimal128 value);
    void set_null(size_t ndx)
    {
        set(ndx, Decimal128(realm::null()));
    }

    void insert(size_t ndx, Decimal128 value);
    void erase(size_t ndx);
    void move(ArrayDecimal128& dst, size_t ndx);
    void clear()
    {
        truncate(0);
    }

    size_t find_first(Decimal128 value, size_t begin = 0, size_t end = npos) const noexcept;

protected:
    size_t calc_byte_len(size_t num_items, size_t) const override
    {
        return num_items * sizeof(Decimal128) + header_size;
    }
};

template <>
class QueryState<Decimal128> : public QueryStateBase {
public:
    Decimal128 m_state;

    template <Action action>
    bool uses_val()
    {
        return (action == act_Max || action == act_Min || action == act_Sum);
    }

    QueryState(Action action, Array* = nullptr, size_t limit = -1)
        : QueryStateBase(limit)
    {
        if (action == act_Max)
            m_state = Decimal128("-inf");
        else if (action == act_Min)
            m_state = Decimal128("+inf");
    }

    template <Action action, bool>
    inline bool match(size_t index, uint64_t /*indexpattern*/, Decimal128 value)
    {
        static_assert(action == act_Sum || action == act_Max || action == act_Min || action == act_Count,
                      "Search action not supported");

        if (action == act_Count) {
            ++m_match_count;
        }
        else if (!value.is_null()) {
            ++m_match_count;
            if (action == act_Max) {
                if (value > m_state) {
                    m_state = value;
                    if (m_key_values) {
                        m_minmax_index = m_key_values->get(index) + m_key_offset;
                    }
                    else {
                        m_minmax_index = int64_t(index);
                    }
                }
            }
            else if (action == act_Min) {
                if (value < m_state) {
                    m_state = value;
                    if (m_key_values) {
                        m_minmax_index = m_key_values->get(index) + m_key_offset;
                    }
                    else {
                        m_minmax_index = int64_t(index);
                    }
                }
            }
            else if (action == act_Sum) {
                m_state += value;
            }
        }

        return (m_limit > m_match_count);
    }
};

} // namespace realm

#endif /* REALM_ARRAY_DECIMAL128_HPP */
