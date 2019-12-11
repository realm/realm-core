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

#ifndef REALM_ARRAY_OBJECT_ID_HPP
#define REALM_ARRAY_OBJECT_ID_HPP

#include <realm/array.hpp>
#include <realm/object_id.hpp>

namespace realm {

class ArrayObjectId : public ArrayPayload, private Array {
public:
    using value_type = ObjectId;

    using Array::Array;
    using Array::destroy;
    using Array::get_ref;
    using Array::init_from_mem;
    using Array::init_from_parent;
    using Array::update_parent;
    using Array::verify;

    static ObjectId default_value(bool)
    {
        return ObjectId();
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

    size_t size() const
    {
        REALM_ASSERT(m_size < s_width * 10000);
        // return m_size / 12
        // Multiply by 0x10000 / 3. Divide by 0x10000 * 4
        // Error is shifted away.
        // Ensured to work for number of elements < 10000. Also on 32 bit
        return (m_size * 0x5556) >> 18;
    }

    bool is_null(size_t ndx) const
    {
        return this->get_width() == 0 || get(ndx).is_null();
    }

    ObjectId get(size_t ndx) const
    {
        REALM_ASSERT(s_width * ndx < m_size);
        auto values = reinterpret_cast<ObjectId*>(this->m_data);
        return values[ndx];
    }

    void add(ObjectId value)
    {
        insert(size(), value);
    }

    void set(size_t ndx, ObjectId value);
    void insert(size_t ndx, ObjectId value);
    void erase(size_t ndx);
    void move(ArrayObjectId& dst, size_t ndx);
    void clear()
    {
        truncate(0);
    }
    void truncate(size_t ndx)
    {
        Array::truncate(s_width * ndx);
    }

    size_t find_first(ObjectId value, size_t begin = 0, size_t end = npos) const noexcept;

protected:
    static constexpr size_t s_width = sizeof(ObjectId); // Size of each element
    static_assert(s_width == 12, "Size of ObjectId must be 12");

    size_t calc_byte_len(size_t num_items, size_t) const override
    {
        return num_items;
    }
};

} // namespace realm

#endif /* REALM_ARRAY_OBJECT_ID_HPP */
