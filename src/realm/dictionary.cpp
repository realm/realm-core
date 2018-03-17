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

#include <realm/dictionary.hpp>

using namespace realm;

/****************************** ConstDictionary ******************************/

ConstDictionary& ConstDictionary::operator=(const ConstDictionary& other)
{
    if (this != &other) {
        if (!is_null()) {
            _destroy();
        }
        if (!other.is_null()) {
            {
                MemRef mem(other.m_keys.get_ref(), other.m_alloc);
                MemRef copy_mem = Array::clone(mem, other.m_alloc, m_alloc); // Throws
                m_keys.init_from_mem(copy_mem);
            }
            {
                MemRef mem(other.m_values.get_ref(), other.m_alloc);
                MemRef copy_mem = Array::clone(mem, other.m_alloc, m_alloc); // Throws
                m_values.init_from_mem(copy_mem);
            }
            update_parent();
        }
    }
    return *this;
}

ConstDictionary& ConstDictionary::operator=(ConstDictionary&& other)
{
    if (&other != this) {
        // Allocator must be the same.
        if (!other.is_null()) {
            REALM_ASSERT(&m_alloc == &other.m_alloc);
            auto refs = other.get_refs();
            init_from_refs(refs.first, refs.second);
            other.m_keys.detach();
        }
    }

    return *this;
}

bool ConstDictionary::operator==(const ConstDictionary& other) const
{
    if (size() != other.size()) {
        return false;
    }
    auto i1 = begin();
    auto i2 = other.begin();
    auto e = end();
    while (i1 != e) {
        if (i1->first != i2->first) {
            return false;
        }
        if (i1->second != i2->second) {
            return false;
        }
        ++i1;
        ++i2;
    }

    return true;
}

Mixed ConstDictionary::get(Mixed key) const
{
    size_t m = m_keys.find_first(key, 0, realm::npos);
    if (m == realm::npos) {
        throw std::out_of_range("Key not found");
    }
    return m_values.get(m);
}

ConstDictionary::Iterator ConstDictionary::begin() const
{
    return Iterator(this, 0);
}

ConstDictionary::Iterator ConstDictionary::end() const
{
    return Iterator(this, size());
}

/******************************** Dictionary *********************************/

Dictionary::~Dictionary()
{
    // If destroyed as a standalone dictionary, destroy all memory allocated
    if (m_keys.get_parent() == nullptr) {
        destroy();
    }
}

void Dictionary::create()
{
    if (!m_keys.is_attached()) {
        m_keys.create();
        m_values.create();
        update_parent();
    }
}

void Dictionary::destroy()
{
    if (m_keys.is_attached()) {
        _destroy();
        update_parent();
    }
}

bool Dictionary::insert(Mixed key, Mixed value)
{
    size_t m = m_keys.find_first(key, 0, realm::npos);

    if (m != realm::npos) {
        return false;
    }

    m_keys.add(key);
    m_values.add(value);

    return true;
}

void Dictionary::update(Mixed key, Mixed value)
{
    size_t m = m_keys.find_first(key, 0, realm::npos);

    if (m != realm::npos) {
        m_values.set(m, value);
    }
    else {
        m_keys.add(key);
        m_values.add(value);
    }
}

void Dictionary::clear()
{
    if (size() > 0) {
        m_keys.truncate_and_destroy_children(0);
        m_values.truncate_and_destroy_children(0);
    }
}

/************************* ConstDictionary::Iterator *************************/

ConstDictionary::Iterator::pointer ConstDictionary::Iterator::operator->()
{
    REALM_ASSERT(m_pos < m_keys.size());
    m_val = std::make_pair(m_keys.get(m_pos), m_values.get(m_pos));
    return &m_val;
}
