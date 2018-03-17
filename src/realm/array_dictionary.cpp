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

#include <realm/array_dictionary.hpp>

using namespace realm;

void ArrayDictionary::add(const ConstDictionary& value)
{
    // Make room
    size_t ndx = size();
    Array::add(0);
    Array::add(0);

    Dictionary dict(m_alloc);
    dict.set_parent(this, ndx << 1);
    dict = value;
}

void ArrayDictionary::set(size_t ndx, const ConstDictionary& value)
{
    Dictionary dict(m_alloc);
    dict.set_parent(this, ndx << 1);
    if (!is_null(ndx)) {
        dict.init_from_parent();
    }
    dict = value;
}

void ArrayDictionary::insert(size_t ndx, const ConstDictionary& value)
{
    // Make room
    Array::insert(ndx << 1, 0);
    Array::insert((ndx << 1) + 1, 0);

    Dictionary dict(m_alloc);
    dict.set_parent(this, ndx << 1);
    dict = value;
}

ConstDictionary ArrayDictionary::get(size_t ndx) const
{
    ConstDictionary value(m_alloc);
    value.set_parent(const_cast<ArrayDictionary*>(this), ndx << 1);
    value.init_from_parent();

    return value;
}

void ArrayDictionary::update(size_t ndx, Mixed key, Mixed value)
{
    Dictionary dict(m_alloc);
    dict.set_parent(this, ndx << 1);
    if (!is_null(ndx)) {
        dict.init_from_parent();
    }
    else {
        dict.create();
    }
    dict.update(key, value);
}

Mixed ArrayDictionary::get(size_t ndx, Mixed key) const
{
    ConstDictionary dict = get(ndx);
    return dict.get(key);
}

void ArrayDictionary::erase(size_t ndx)
{
    Array::destroy_deep(Array::get_as_ref(ndx << 1), m_alloc);
    Array::destroy_deep(Array::get_as_ref((ndx << 1) + 1), m_alloc);

    Array::erase(ndx << 1);
    Array::erase((ndx << 1) + 1);
}
