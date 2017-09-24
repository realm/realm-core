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

#ifndef SRC_REALM_ARRAY_STRING_HPP_
#define SRC_REALM_ARRAY_STRING_HPP_

#include <realm/array_string_short.hpp>
#include <realm/array_blobs_small.hpp>
#include <realm/array_blobs_big.hpp>

namespace realm {

class ArrayString {
public:
    explicit ArrayString(Allocator&);

    static StringData default_value(bool nullable)
    {
        return nullable ? StringData{} : StringData{""};
    }

    void create();

    ref_type get_ref() const
    {
        return m_arr->get_ref();
    }

    void set_parent(ArrayParent* parent, size_t ndx_in_parent)
    {
        m_arr->set_parent(parent, ndx_in_parent);
    }

    void update_parent()
    {
        m_arr->update_parent();
    }

    void init_from_ref(ref_type ref);
    void init_from_parent();

    size_t size() const;

    void add(StringData value);
    void set(size_t ndx, StringData value);
    void set_null(size_t ndx)
    {
        set(ndx, StringData{});
    }
    void insert(size_t ndx, StringData value);
    StringData get(size_t ndx) const;
    bool is_null(size_t ndx) const;
    void erase(size_t ndx);
    void truncate_and_destroy_children(size_t ndx);

    size_t find_first(StringData value, size_t begin, size_t end) const noexcept
    {
        for (size_t t = begin; t < end; t++) {
            if (get(t) == value)
                return t;
        }
        return not_found;
    }

private:
    static constexpr size_t small_string_max_size = 15;  // ArrayStringShort
    static constexpr size_t medium_string_max_size = 63; // ArrayStringLong
    union Storage {
        std::aligned_storage<sizeof(ArrayStringShort), alignof(ArrayStringShort)>::type m_string_short;
        std::aligned_storage<sizeof(ArraySmallBlobs), alignof(ArraySmallBlobs)>::type m_string_long;
        std::aligned_storage<sizeof(ArrayBigBlobs), alignof(ArrayBigBlobs)>::type m_big_blobs;
    };
    enum class Type {
        small,
        medium,
        big,
    };

    Type m_type = Type::small;

    Allocator& m_alloc;
    Storage m_storage;
    Array* m_arr;

    Type upgrade_leaf(size_t value_size);
};
}

#endif /* SRC_REALM_ARRAY_STRING_HPP_ */
