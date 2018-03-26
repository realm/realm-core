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
    return m_key_values.size();
}

// Get key for object this view is "looking" at.
ObjKey ObjList::get_key(size_t ndx) const
{
    return ObjKey(m_key_values.get(ndx));
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
    std::vector<CommonDescriptor::IndexPair> v;
    v.reserve(sz);
    // always put any detached refs at the end of the sort
    // FIXME: reconsider if this is the right thing to do
    // FIXME: consider specialized implementations in derived classes
    // (handling detached refs is not required in linkviews)
    for (size_t t = 0; t < sz; t++) {
        ObjKey key = get_key(t);
        if (m_table->is_valid(key)) {
            v.push_back({key, t});
        }
        else
            ++detached_ref_count;
    }

    const int num_descriptors = int(ordering.size());
    for (int desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
        const CommonDescriptor* common_descr = ordering[desc_ndx];

        if (const auto* sort_descr = dynamic_cast<const SortDescriptor*>(common_descr)) {

            SortDescriptor::Sorter sort_predicate = sort_descr->sorter(m_key_values);            
            auto& col = sort_predicate.m_columns[0];
            ColKey ck = col.col_key;

            for (size_t i = 0; i < v.size(); i++) {
                ObjKey key = v[i].key_for_object;

                if (!col.translated_keys.empty()) {
                    key = col.translated_keys[v[i].index_in_view];
                }

                // Sorting can be specified by multiple columns, so that if two entries in the first column are
                // identical, then the rows are ordered according to the second column, and so forth. For the
                // first column, we cache all the payload of fields of the view in a std::vector<Mixed>
                ConstObj obj = col.table->get_object(key);
                DataType dt = sort_predicate.m_columns[0].table->get_column_type(ck);
                auto& vec = sort_predicate.m_columns[0].payload;
                if (dt == type_Int) {
                    vec.emplace_back(obj.get<Int>(ck));
                }
                else if (dt == type_String) {
                    vec.emplace_back(obj.get<String>(ck));
                }
                else if (dt == type_Float) {
                    vec.emplace_back(obj.get<Float>(ck));
                }
                else if (dt == type_Double) {
                    vec.emplace_back(obj.get<Double>(ck));
                }
                else if (dt == type_Bool) {
                    vec.emplace_back(obj.get<Bool>(ck));
                }
                else if (dt == type_Timestamp) {
                    vec.emplace_back(obj.get<Timestamp>(ck));
                }
            }

            std::sort(v.begin(), v.end(), std::ref(sort_predicate));

            bool is_last_ordering = desc_ndx == num_descriptors - 1;
            // not doing this on the last step is an optimisation
            if (!is_last_ordering) {
                const size_t v_size = v.size();
                // Distinct must choose the winning unique elements by sorted
                // order not by the previous tableview order, the lowest
                // "index_in_view" wins.
                for (size_t i = 0; i < v_size; ++i) {
                    v[i].index_in_view = i;
                }
            }
        }
        else { // distinct descriptor
            auto distinct_predicate = common_descr->sorter(m_key_values);

            // Remove all rows which have a null link along the way to the distinct columns
            if (distinct_predicate.has_links()) {
                v.erase(std::remove_if(v.begin(), v.end(),
                                       [&](auto&& index) { return distinct_predicate.any_is_null(index); }),
                        v.end());
            }

            // Sort by the columns to distinct on
            std::sort(v.begin(), v.end(), std::ref(distinct_predicate));

            // Remove all duplicates
            v.erase(std::unique(v.begin(), v.end(),
                                [&](auto&& a, auto&& b) {
                                    // "not less than" is "equal" since they're sorted
                                    return !distinct_predicate(a, b, false);
                                }),
                    v.end());
            bool will_be_sorted_next = desc_ndx < num_descriptors - 1 && ordering.descriptor_is_sort(desc_ndx + 1);
            if (!will_be_sorted_next) {
                // Restore the original order, this is either the original
                // tableview order or the order of the previous sort
                std::sort(v.begin(), v.end(), [](auto a, auto b) { return a.index_in_view < b.index_in_view; });
            }
        }
    }
    // Apply the results
    m_key_values.clear();
    for (auto& pair : v) {
        m_key_values.add(pair.key_for_object);
    }
    for (size_t t = 0; t < detached_ref_count; ++t)
        m_key_values.add(null_key);
}

ObjList::ObjList(KeyColumn& key_values)
    : m_key_values(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ObjList::ObjList(KeyColumn& key_values, const Table* parent)
    : m_table(parent->get_table_ref())
    , m_key_values(key_values)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

ConstObj ObjList::get(size_t row_ndx) const
{
    REALM_ASSERT(m_table);
    REALM_ASSERT(row_ndx < m_key_values.size());
    ObjKey key(m_key_values.get(row_ndx));
    REALM_ASSERT(key != realm::null_key);
    return m_table->get_object(key);
}
