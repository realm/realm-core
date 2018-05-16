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
    return m_key_values->size();
}

// Get key for object this view is "looking" at.
ObjKey ObjList::get_key(size_t ndx) const
{
    return ObjKey(m_key_values->get(ndx));
}

void ObjList::do_sort(const DescriptorOrdering& ordering)
{
    if (ordering.is_empty())
        return;
    size_t sz = size();
    if (sz == 0)
        return;

    // Gather the current rows into a container we can use std algorithms on
    size_t detached_ref_count = 0;
    CommonDescriptor::IndexPairs index_pairs;
    index_pairs.reserve(sz);
    // always put any detached refs at the end of the sort
    // FIXME: reconsider if this is the right thing to do
    // FIXME: consider specialized implementations in derived classes
    // (handling detached refs is not required in linkviews)
    for (size_t t = 0; t < sz; t++) {
        ObjKey key = get_key(t);
        if (m_table->is_valid(key)) {
            index_pairs.emplace_back(key, t);
        }
        else
            ++detached_ref_count;
    }

    const int num_descriptors = int(ordering.size());
    for (int desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
        const CommonDescriptor* common_descr = ordering[desc_ndx];
        const CommonDescriptor* next = ((desc_ndx + 1) < num_descriptors) ? ordering[desc_ndx + 1] : nullptr;
        SortDescriptor::Sorter predicate = common_descr->sorter(*m_key_values);

        // Sorting can be specified by multiple columns, so that if two entries in the first column are
        // identical, then the rows are ordered according to the second column, and so forth. For the
        // first column, we cache all the payload of fields of the view in a std::vector<Mixed>
        predicate.cache_first_column(index_pairs);

        common_descr->execute(index_pairs, predicate, next);
    }
    // Apply the results
    m_key_values->clear();
    for (auto& pair : index_pairs) {
        m_key_values->add(pair.key_for_object);
    }
    for (size_t t = 0; t < detached_ref_count; ++t)
        m_key_values->add(null_key);
}

ObjList::ObjList(KeyColumn* key_values)
    : m_key_values(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ObjList::ObjList(KeyColumn* key_values, const Table* parent)
    : m_table(parent->get_table_ref())
    , m_key_values(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ConstObj ObjList::get_object(size_t row_ndx) const
{
    REALM_ASSERT(m_table);
    REALM_ASSERT(row_ndx < m_key_values->size());
    ObjKey key(m_key_values->get(row_ndx));
    REALM_ASSERT(key != realm::null_key);
    return m_table->get_object(key);
}

void ObjList::assign(KeyColumn* key_values, const Table* parent)
{
    m_key_values = key_values;
    m_table = parent ? parent->get_table_ref() : TableRef();
}
