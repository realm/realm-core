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

#ifndef REALM_ARRAY_TIMESTAMP_HPP
#define REALM_ARRAY_TIMESTAMP_HPP

#include <realm/array_integer.hpp>
#include <realm/timestamp.hpp>

namespace realm {

class ArrayTimestamp : private Array {
public:
    explicit ArrayTimestamp(Allocator&);

    using Array::set_parent;
    using Array::update_parent;
    using Array::get_ref;

    static Timestamp default_value(bool nullable)
    {
        return nullable ? Timestamp{} : Timestamp{0, 0};
    }

    void create();

    void init_from_ref(ref_type ref);
    void init_from_parent();

    size_t size() const
    {
        return m_seconds.size();
    }

    void add(Timestamp value)
    {
        insert(m_seconds.size(), value);
    }
    void set(size_t ndx, Timestamp value);
    void set_null(size_t ndx)
    {
        // Value in m_nanoseconds is irrelevant if m_seconds is null
        m_seconds.set_null(ndx); // Throws
    }
    void insert(size_t ndx, Timestamp value);
    Timestamp get(size_t ndx) const
    {
        util::Optional<int64_t> seconds = m_seconds.get(ndx);
        return seconds ? Timestamp(*seconds, int32_t(m_nanoseconds.get(ndx))) : Timestamp{};
    }
    bool is_null(size_t ndx) const
    {
        return m_seconds.is_null(ndx);
    }
    void erase(size_t ndx)
    {
        m_seconds.erase(ndx);
        m_nanoseconds.erase(ndx);
    }
    void truncate_and_destroy_children(size_t ndx)
    {
        m_seconds.truncate(ndx);
        m_nanoseconds.truncate(ndx);
    }

    size_t find_first(Timestamp value, size_t begin, size_t end) const noexcept
    {
        for (size_t t = begin; t < end; t++) {
            if (get(t) == value)
                return t;
        }
        return not_found;
    }

private:
    ArrayIntNull m_seconds;
    ArrayInteger m_nanoseconds;
};
}

#endif /* SRC_REALM_ARRAY_BINARY_HPP_ */
