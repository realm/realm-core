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

#include <realm/obj_list.hpp>
#include <realm/table.hpp>
#include <realm/sort_descriptor.hpp>

using namespace realm;

size_t ObjList::size() const
{
    if (m_key_values)
        return m_key_values->size();
    if (m_key_vector)
        return m_key_vector->size();
    REALM_ASSERT_RELEASE(false && "ObjList with no values");
}

// Get key for object this view is "looking" at.
ObjKey ObjList::get_key(size_t ndx) const
{
    if (m_key_values)
        return ObjKey(m_key_values->get(ndx));
    if (m_key_vector)
        return (*m_key_vector)[ndx];
    return ObjKey();
}


ObjList::ObjList(KeyColumn* key_values)
    : m_key_values(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ObjList::ObjList(std::vector<ObjKey>* key_values)
    : m_key_vector(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ObjList::ObjList(KeyColumn* key_values, const Table* parent)
    : m_table(parent)
    , m_key_values(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ObjList::ObjList(std::vector<ObjKey>* key_values, const Table* parent)
    : m_table(parent)
    , m_key_vector(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ConstObj ObjList::get_object(size_t row_ndx) const
{
    REALM_ASSERT(m_table);
    if (m_key_values) {
        REALM_ASSERT(row_ndx < m_key_values->size());
        ObjKey key(m_key_values->get(row_ndx));
        REALM_ASSERT(key != realm::null_key);
        return m_table->get_object(key);
    }
    if (m_key_vector) {
        REALM_ASSERT(row_ndx < m_key_vector->size());
        ObjKey key((*m_key_vector)[row_ndx]);
        REALM_ASSERT(key != realm::null_key);
        return m_table->get_object(key);
    }
    REALM_ASSERT(false && "no object keys");
    return ConstObj();
}

ConstObj ObjList::try_get_object(size_t row_ndx) const
{
    REALM_ASSERT(m_table);
    if (m_key_values) {
        REALM_ASSERT(row_ndx < m_key_values->size());
        ObjKey key(m_key_values->get(row_ndx));
        REALM_ASSERT(key != realm::null_key);
        return m_table->is_valid(key) ? m_table->get_object(key) : ConstObj();
    }
    if (m_key_vector) {
        REALM_ASSERT(row_ndx < m_key_vector->size());
        ObjKey key((*m_key_vector)[row_ndx]);
        REALM_ASSERT(key != realm::null_key);
        return m_table->is_valid(key) ? m_table->get_object(key) : ConstObj();
    }
    REALM_ASSERT(false && "no object keys");
    return ConstObj();
}

void ObjList::assign(KeyColumn* key_values, const Table* parent)
{
    m_key_values = key_values;
    m_key_vector = nullptr;
    m_table = ConstTableRef(parent);
}
