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
        Key key = get_key(t);
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
        m_key_values.add(pair.key_for_object.value);
    }
    for (size_t t = 0; t < detached_ref_count; ++t)
        m_key_values.add(null_key.value);
}
inline void ObjList::allocate_key_values()
{
    // FIXME: This code is unreasonably complicated because it uses `IntegerColumn` as
    // a free-standing container, and beause `IntegerColumn` does not conform to the
    // RAII idiom (nor should it).
    Allocator& alloc = m_key_values.get_alloc();
    _impl::DeepArrayRefDestroyGuard ref_guard(alloc);
    ref_guard.reset(IntegerColumn::create(alloc)); // Throws
    m_key_values.init_from_ref(alloc, ref_guard.release());
}


ObjList::ObjList()
    : m_key_values(IntegerColumn::unattached_root_tag(), Allocator::get_default())
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
    ref_type ref = IntegerColumn::create(m_key_values.get_alloc()); // Throws
    m_key_values.get_root_array()->init_from_ref(ref);
}

ObjList::ObjList(const ObjList& source)
    : m_table(source.m_table)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(source.m_debug_cookie)
#endif
{
    Allocator& alloc = m_key_values.get_alloc();
    MemRef mem = source.m_key_values.get_root_array()->clone_deep(alloc); // Throws
    m_key_values.init_from_mem(alloc, mem);
}

ObjList::ObjList(ObjList&& source)
    : m_table(std::move(source.m_table))
    , m_key_values(std::move(source.m_key_values))
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(source.m_debug_cookie)
#endif
{
}


ObjList::ObjList(Table* parent)
    : m_table(parent->get_table_ref())
    , m_key_values(IntegerColumn::unattached_root_tag(), Allocator::get_default())
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
    allocate_key_values();
}

ObjList::ObjList(IntegerColumn&& col)
    : m_key_values(std::move(col))
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

// FIXME: this only works (and is only used) for row indexes with memory
// managed by the default allocator, e.q. for TableViews.
ObjList::ObjList(const ObjList& source, ConstSourcePayload mode)
#ifdef REALM_COOKIE_CHECK
    : m_debug_cookie(source.m_debug_cookie)
#endif
{
    REALM_ASSERT(&source.m_key_values.get_alloc() == &Allocator::get_default());

    if (mode == ConstSourcePayload::Copy && source.m_key_values.is_attached()) {
        MemRef mem = source.m_key_values.clone_deep(Allocator::get_default());
        m_key_values.init_from_mem(Allocator::get_default(), mem);
    }
}

ObjList::ObjList(ObjList& source, MutableSourcePayload)
#ifdef REALM_COOKIE_CHECK
    : m_debug_cookie(source.m_debug_cookie)
#endif
{
    REALM_ASSERT(&source.m_key_values.get_alloc() == &Allocator::get_default());

    // move the data payload, but make sure to leave the source array intact or
    // attempts to reuse it for a query rerun will crash (or assert, if lucky)
    // There really *has* to be a way where we don't need to first create an empty
    // array, and then destroy it
    if (source.m_key_values.is_attached()) {
        m_key_values.detach();
        m_key_values.init_from_mem(Allocator::get_default(), source.m_key_values.get_mem());
        source.m_key_values.init_from_ref(Allocator::get_default(), IntegerColumn::create(Allocator::get_default()));
    }
}

ConstObj ObjList::get(size_t row_ndx) const noexcept
{
    REALM_ASSERT(m_table);
    REALM_ASSERT(row_ndx < m_key_values.size());
    Key key(m_key_values.get(row_ndx));
    REALM_ASSERT(key != realm::null_key);
    return m_table->get_object(key);
}
