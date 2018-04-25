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

#include <realm/array_timestamp.hpp>

using namespace realm;

ArrayTimestamp::ArrayTimestamp(Allocator& a)
    : Array(a)
    , m_seconds(a)
    , m_nanoseconds(a)
{
    m_seconds.set_parent(this, 0);
    m_nanoseconds.set_parent(this, 1);
}

void ArrayTimestamp::create()
{
    Array::create(Array::type_HasRefs, false /* context_flag */, 2);

    MemRef seconds = ArrayIntNull::create_array(Array::type_Normal, false, 0, util::none, m_alloc);
    Array::set_as_ref(0, seconds.get_ref());
    MemRef nanoseconds = ArrayInteger::create_empty_array(Array::type_Normal, false, m_alloc);
    Array::set_as_ref(1, nanoseconds.get_ref());

    m_seconds.init_from_parent();
    m_nanoseconds.init_from_parent();
}

void ArrayTimestamp::init_from_mem(MemRef mem) noexcept
{
    Array::init_from_mem(mem);
    m_seconds.init_from_parent();
    m_nanoseconds.init_from_parent();
}

void ArrayTimestamp::set(size_t ndx, Timestamp value)
{
    if (value.is_null()) {
        return set_null(ndx);
    }

    util::Optional<int64_t> seconds = util::make_optional(value.get_seconds());
    int32_t nanoseconds = value.get_nanoseconds();

    m_seconds.set(ndx, seconds);
    m_nanoseconds.set(ndx, nanoseconds); // Throws
}

void ArrayTimestamp::insert(size_t ndx, Timestamp value)
{
    if (value.is_null()) {
        m_seconds.insert(ndx, util::none);
        m_nanoseconds.insert(ndx, 0); // Throws
    }
    else {
        util::Optional<int64_t> seconds = util::make_optional(value.get_seconds());
        int32_t nanoseconds = value.get_nanoseconds();

        m_seconds.insert(ndx, seconds);
        m_nanoseconds.insert(ndx, nanoseconds); // Throws
    }
}

size_t ArrayTimestamp::find_first(Timestamp value, size_t begin, size_t end) const noexcept
{
    if (value.is_null()) {
        return m_seconds.find_first(realm::util::none, begin, end);
    }
    while (begin < end) {
        auto res = m_seconds.find_first(value.get_seconds(), begin, end);
        if (res == npos)
            return not_found;
        if (m_nanoseconds.get(res) == value.get_nanoseconds())
            return res;
        begin = res + 1;
    }
    return not_found;
}
