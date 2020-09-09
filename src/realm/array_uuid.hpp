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

#ifndef REALM_ARRAY_UUID_HPP
#define REALM_ARRAY_UUID_HPP

#include <realm/array.hpp>
#include <realm/uuid.hpp>

namespace realm {

class ArrayUUID : public ArrayPayload, protected Array {
public:
    using value_type = UUID;

    using Array::Array;
    using Array::destroy;
    using Array::get_ref;
    using Array::init_from_mem;
    using Array::init_from_parent;
    using Array::update_parent;
    using Array::verify;

    static UUID default_value(bool nullable)
    {
        if (nullable) {
            return UUID();
        }
        // FIXME: verify this and optimize it
        return UUID("00000000-0000-0000-0000-000000000001");
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
        return m_size;
    }

    bool is_null(size_t ndx) const
    {
        return this->get_width() == 0 || get(ndx).is_null();
    }

    void set_null(size_t ndx)
    {
        set(ndx, UUID());
    }

    UUID get(size_t ndx) const
    {
        REALM_ASSERT(is_valid_ndx(ndx));
        return reinterpret_cast<const UUID*>(this->m_data)[ndx];
    }

    void add(const UUID& value)
    {
        insert(size(), value);
    }

    void set(size_t ndx, const UUID& value);
    void insert(size_t ndx, const UUID& value);
    void erase(size_t ndx);
    void move(ArrayUUID& dst, size_t ndx);
    void clear()
    {
        truncate(0);
    }
    void truncate(size_t ndx)
    {
        Array::truncate(ndx);
    }

    size_t find_first(const UUID& value, size_t begin = 0, size_t end = npos) const noexcept;

protected:
    static constexpr size_t s_width = sizeof(UUID); // Size of each element
    static_assert(s_width == 16, "Size of UUID must be 16");

    size_t calc_byte_len(size_t num_items, size_t width) const override
    {
        return header_size + (num_items * width);
    }

    inline bool is_valid_ndx(size_t ndx) const
    {
        return ndx < m_size;
    }
};

} // namespace realm

#endif /* REALM_ARRAY_UUID_HPP */
