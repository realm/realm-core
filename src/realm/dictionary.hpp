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

#ifndef REALM_DICTIONARY_HPP
#define REALM_DICTIONARY_HPP

#include <realm/array.hpp>
#include <realm/array_mixed.hpp>

namespace realm {

class ConstDictionary {
public:
    class Iterator;

    ConstDictionary(Allocator& alloc = Allocator::get_default())
        : m_alloc(alloc)
        , m_keys(alloc)
        , m_values(alloc)
    {
    }

    ConstDictionary(ConstDictionary&& other)
        : ConstDictionary(other.m_alloc)
    {
        *this = std::move(other);
    }

    ConstDictionary& operator=(ConstDictionary&& other);

    bool operator==(const ConstDictionary& other) const;
    bool operator!=(const ConstDictionary& other) const
    {
        return !(*this == other);
    }

    bool is_null() const
    {
        return !m_keys.is_attached();
    }

    size_t size() const
    {
        REALM_ASSERT(!is_null());
        return m_keys.size();
    }

    // throws std::out_of_range if key is not found
    Mixed get(Mixed key) const;

    Iterator begin() const;
    Iterator end() const;

protected:
    friend class ArrayDictionary;

    Allocator& m_alloc;
    ArrayMixed m_keys;
    ArrayMixed m_values;

    ConstDictionary(const ConstDictionary& other)
        : ConstDictionary(other.m_alloc)
    {
        *this = other;
    }
    ConstDictionary& operator=(const ConstDictionary& other);

    void _destroy()
    {
        Array::destroy_deep(m_keys.get_ref(), m_alloc);
        Array::destroy_deep(m_values.get_ref(), m_alloc);
        m_keys.detach();
        m_values.detach();
    }
    void init_from_refs(ref_type first, ref_type second)
    {
        m_keys.init_from_ref(first);
        m_values.init_from_ref(second);
        update_parent();
    }
    std::pair<ref_type, ref_type> get_refs() const
    {
        if (is_null()) {
            return {0, 0};
        }
        else {
            return std::make_pair(m_keys.get_ref(), m_values.get_ref());
        }
    }
    void set_parent(ArrayParent* parent, size_t ndx)
    {
        m_keys.set_parent(parent, ndx);
        m_values.set_parent(parent, ndx + 1);
    }
    void init_from_parent()
    {
        m_keys.init_from_parent();
        m_values.init_from_parent();
    }
    void update_parent()
    {
        m_keys.update_parent();
        m_values.update_parent();
    }
};

class Dictionary : public ConstDictionary {
public:
    Dictionary(Allocator& alloc = Allocator::get_default())
        : ConstDictionary(alloc)
    {
    }
    ~Dictionary();

    Dictionary& operator=(const ConstDictionary& other)
    {
        ConstDictionary::operator=(other);
        return *this;
    }

    void create();
    void destroy();

    // returns true if the element was inserted
    bool insert(Mixed key, Mixed value);
    // insert or update element
    void update(Mixed key, Mixed value);

    void clear();
};

class ConstDictionary::Iterator {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef const std::pair<Mixed, Mixed> value_type;
    typedef ptrdiff_t difference_type;
    typedef const value_type* pointer;
    typedef const value_type& reference;

    pointer operator->();

    reference operator*()
    {
        return *operator->();
    }

    Iterator& operator++()
    {
        m_pos++;
        return *this;
    }

    Iterator operator++(int)
    {
        Iterator tmp(*this);
        operator++();
        return tmp;
    }

    bool operator!=(const Iterator& rhs)
    {
        return m_pos != rhs.m_pos;
    }

    bool operator==(const Iterator& rhs)
    {
        return m_pos == rhs.m_pos;
    }

private:
    friend class ConstDictionary;

    const ArrayMixed& m_keys;
    const ArrayMixed& m_values;
    size_t m_pos;
    std::pair<Mixed, Mixed> m_val;

    Iterator(const ConstDictionary* dict, size_t pos)
        : m_keys(dict->m_keys)
        , m_values(dict->m_values)
        , m_pos(pos)
    {
    }
};

}

#endif /* SRC_REALM_DICTIONARY_HPP_ */
