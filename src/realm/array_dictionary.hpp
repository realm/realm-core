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

#ifndef REALM_ARRAY_DICTIONARY_HPP
#define REALM_ARRAY_DICTIONARY_HPP

#include <realm/dictionary.hpp>

namespace realm {

class ArrayDictionary : public ArrayPayload, private Array {
public:
    using value_type = ConstDictionary;

    using Array::get_ref;
    using Array::set_parent;
    using Array::update_parent;
    using Array::init_from_mem;
    using Array::init_from_ref;
    using Array::init_from_parent;
    using Array::truncate_and_destroy_children;

    explicit ArrayDictionary(Allocator& alloc)
        : Array(alloc)
    {
    }

    static Dictionary default_value(bool nullable = false)
    {
        Dictionary dict;
        if (!nullable)
            dict.create();
        return dict;
    }

    void init_from_ref(ref_type ref) noexcept override
    {
        Array::init_from_ref(ref);
    }
    void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept override
    {
        Array::set_parent(parent, ndx_in_parent);
    }
    void create()
    {
        Array::create(type_HasRefs);
    }
    void destroy()
    {
        Array::destroy_deep();
    }

    size_t size() const
    {
        return Array::size() >> 1;
    }

    bool is_null(size_t ndx) const
    {
        return Array::get(ndx << 1) == 0;
    }

    void add(const ConstDictionary& value);
    void set(size_t ndx, const ConstDictionary& value);
    void set_null(size_t ndx);
    void insert(size_t ndx, const ConstDictionary& value);

    ConstDictionary get(size_t ndx) const;

    void update(size_t ndx, Mixed key, Mixed value);
    Mixed get(size_t ndx, Mixed key) const;

    void erase(size_t ndx);

    size_t find_first(const ConstDictionary& value, size_t begin, size_t end) const noexcept;
};
}

#endif /* REALM_ARRAY_DICTIONARY_HPP */
