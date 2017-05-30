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

#include <realm/views.hpp>

#include <realm/column_link.hpp>
#include <realm/table.hpp>

using namespace realm;

namespace {
struct IndexPair {
    size_t index_in_column;
    size_t index_in_view;
};
} // anonymous namespace

SortDescriptor::SortDescriptor(Table const& table, std::vector<std::vector<size_t>> column_indices,
                               std::vector<bool> ascending)
    : m_ascending(std::move(ascending))
{
    REALM_ASSERT(!column_indices.empty());
    REALM_ASSERT_EX(m_ascending.empty() || m_ascending.size() == column_indices.size(), m_ascending.size(),
                    column_indices.size());
    if (m_ascending.empty())
        m_ascending.resize(column_indices.size(), true);
    if (table.is_degenerate())
        return;

    using tf = _impl::TableFriend;
    m_columns.resize(column_indices.size());
    for (size_t i = 0; i < m_columns.size(); ++i) {
        auto& columns = m_columns[i];
        auto& indices = column_indices[i];
        REALM_ASSERT(!column_indices.empty());

        columns.reserve(indices.size());
        const Table* cur_table = &table;
        for (auto index : indices) {
            auto& col = tf::get_column(*cur_table, index);
            columns.push_back(&col);
            if (auto link_col = dynamic_cast<const LinkColumn*>(&col)) {
                cur_table = &link_col->get_target_table();
            }
            else if (columns.size() != indices.size()) {
                // Only last column in link chain is allowed to be non-link
                throw LogicError(LogicError::type_mismatch);
            }
        }
    }
}

void SortDescriptor::generate_patch(SortDescriptor const& desc, HandoverPatch& patch)
{
    if (desc) {
        std::vector<std::vector<size_t>> column_indices;
        column_indices.reserve(desc.m_columns.size());
        for (auto& cols : desc.m_columns) {
            std::vector<size_t> indices;
            indices.reserve(cols.size());
            for (const ColumnBase* col : cols)
                indices.push_back(col->get_column_index());
            column_indices.push_back(std::move(indices));
        }

        patch.reset(new SortDescriptorHandoverPatch{std::move(column_indices), desc.m_ascending});
    }
}

SortDescriptor SortDescriptor::create_from_and_consume_patch(HandoverPatch& patch, Table const& table)
{
    SortDescriptor ret;
    if (patch) {
        ret = SortDescriptor(table, std::move(patch->columns), std::move(patch->ascending));
        patch.reset();
    }
    return ret;
}

class SortDescriptor::Sorter {
public:
    Sorter(std::vector<std::vector<const ColumnBase*>> const& columns, std::vector<bool> const& ascending,
           IntegerColumn const& row_indexes);
    Sorter() {}

    bool operator()(IndexPair i, IndexPair j, bool total_ordering = true) const;

    bool has_links() const
    {
        return std::any_of(m_columns.begin(), m_columns.end(),
                           [](auto&& col) { return !col.translated_row.empty(); });
    }

    bool any_is_null(IndexPair i) const
    {
        return std::any_of(m_columns.begin(), m_columns.end(),
                           [=](auto&& col) { return col.is_null[i.index_in_view]; });
    }

private:
    struct SortColumn {
        std::vector<bool> is_null;
        std::vector<size_t> translated_row;
        const ColumnBase* column;
        bool ascending;
    };
    std::vector<SortColumn> m_columns;
};

SortDescriptor::Sorter::Sorter(std::vector<std::vector<const ColumnBase*>> const& columns,
                               std::vector<bool> const& ascending, IntegerColumn const& row_indexes)
{
    REALM_ASSERT(!columns.empty());
    size_t num_rows = row_indexes.size();

    m_columns.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        m_columns.push_back({{}, {}, columns[i].back(), ascending[i]});
        REALM_ASSERT_EX(!columns[i].empty(), i);
        if (columns[i].size() == 1) { // no link chain
            continue;
        }

        auto& translated_rows = m_columns.back().translated_row;
        auto& is_null = m_columns.back().is_null;
        translated_rows.resize(num_rows);
        is_null.resize(num_rows);

        for (size_t row_ndx = 0; row_ndx < num_rows; row_ndx++) {
            size_t translated_index = to_size_t(row_indexes.get(row_ndx));
            for (size_t j = 0; j + 1 < columns[i].size(); ++j) {
                // type was checked when creating the SortDescriptor
                auto link_col = static_cast<const LinkColumn*>(columns[i][j]);
                if (link_col->is_null(translated_index)) {
                    is_null[row_ndx] = true;
                    break;
                }
                translated_index = link_col->get_link(translated_index);
            }
            translated_rows[row_ndx] = translated_index;
        }
    }
}

SortDescriptor::Sorter SortDescriptor::sorter(IntegerColumn const& row_indexes) const
{
    return Sorter(m_columns, m_ascending, row_indexes);
}

bool SortDescriptor::Sorter::operator()(IndexPair i, IndexPair j, bool total_ordering) const
{
    for (size_t t = 0; t < m_columns.size(); t++) {
        size_t index_i = i.index_in_column;
        size_t index_j = j.index_in_column;

        if (!m_columns[t].translated_row.empty()) {
            bool null_i = m_columns[t].is_null[i.index_in_view];
            bool null_j = m_columns[t].is_null[j.index_in_view];

            if (null_i && null_j) {
                continue;
            }
            if (null_i || null_j) {
                // Sort null links at the end if m_ascending[t], else at beginning.
                return m_columns[t].ascending != null_i;
            }

            index_i = m_columns[t].translated_row[i.index_in_view];
            index_j = m_columns[t].translated_row[j.index_in_view];
        }

        if (int c = m_columns[t].column->compare_values(index_i, index_j))
            return m_columns[t].ascending ? c > 0 : c < 0;
    }
    // make sort stable by using original index as final comparison
    return total_ordering ? i.index_in_view < j.index_in_view : 0;
}

void RowIndexes::do_sort(const SortDescriptor& order, const SortDescriptor& distinct, bool sort_before_distinct)
{
    if (!order && !distinct)
        return;
    size_t sz = size();
    if (sz == 0)
        return;

    // Gather the current rows into a container we can use std algorithms on
    size_t detached_ref_count = 0;
    std::vector<IndexPair> v;
    v.reserve(sz);
    // always put any detached refs at the end of the sort
    // FIXME: reconsider if this is the right thing to do
    // FIXME: consider specialized implementations in derived classes
    // (handling detached refs is not required in linkviews)
    for (size_t t = 0; t < sz; t++) {
        int64_t ndx = m_row_indexes.get(t);
        if (ndx != detached_ref) {
            v.push_back(IndexPair{static_cast<size_t>(ndx), t});
        }
        else
            ++detached_ref_count;
    }

    SortDescriptor::Sorter sort_predicate;
    if (order) {
        sort_predicate = order.sorter(m_row_indexes);
    }

    if (order && sort_before_distinct) {
        std::sort(v.begin(), v.end(), std::ref(sort_predicate));
        if (distinct) {
            const size_t v_size = v.size();
            // Distinct must choose the winning unique elements by sorted order not
            // by the previous tableview order, the lowest "index_in_view" wins.
            for (size_t i = 0; i < v_size; ++i) {
                v[i].index_in_view = i;
            }
        }
    }

    if (distinct) {
        // Setting an order on the distinct descriptor is now incorrect.
        // Distinct uses the existing order of the view, if the ordering matters
        // then do a sort beforehand to change the results of distinct.
        if (distinct.has_custom_order()) {
            throw LogicError(LogicError::unsupported_order_on_distinct);
        }

        auto distinct_predicate = distinct.sorter(m_row_indexes);

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

        // Restore the original order unless we're just going to sort it again anyway
        if (order && sort_before_distinct) { // restore sorted order
            std::sort(v.begin(), v.end(), std::ref(sort_predicate));
        } else if (!order) { // restore original order
            std::sort(v.begin(), v.end(), [](auto a, auto b) { return a.index_in_view < b.index_in_view; });
        }
    }

    if (order && !sort_before_distinct) {
        std::sort(v.begin(), v.end(), std::ref(sort_predicate));
    }

    // Apply the results
    m_row_indexes.clear();
    for (auto& pair : v)
        m_row_indexes.add(pair.index_in_column);
    for (size_t t = 0; t < detached_ref_count; ++t)
        m_row_indexes.add(-1);
}

RowIndexes::RowIndexes(IntegerColumn::unattached_root_tag urt, realm::Allocator& alloc)
    : m_row_indexes(urt, alloc)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

RowIndexes::RowIndexes(IntegerColumn&& col)
    : m_row_indexes(std::move(col))
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}


// FIXME: this only works (and is only used) for row indexes with memory
// managed by the default allocator, e.q. for TableViews.
RowIndexes::RowIndexes(const RowIndexes& source, ConstSourcePayload mode)
#ifdef REALM_COOKIE_CHECK
    : m_debug_cookie(source.m_debug_cookie)
#endif
{
    REALM_ASSERT(&source.m_row_indexes.get_alloc() == &Allocator::get_default());

    if (mode == ConstSourcePayload::Copy && source.m_row_indexes.is_attached()) {
        MemRef mem = source.m_row_indexes.clone_deep(Allocator::get_default());
        m_row_indexes.init_from_mem(Allocator::get_default(), mem);
    }
}

RowIndexes::RowIndexes(RowIndexes& source, MutableSourcePayload)
#ifdef REALM_COOKIE_CHECK
    : m_debug_cookie(source.m_debug_cookie)
#endif
{
    REALM_ASSERT(&source.m_row_indexes.get_alloc() == &Allocator::get_default());

    // move the data payload, but make sure to leave the source array intact or
    // attempts to reuse it for a query rerun will crash (or assert, if lucky)
    // There really *has* to be a way where we don't need to first create an empty
    // array, and then destroy it
    if (source.m_row_indexes.is_attached()) {
        m_row_indexes.detach();
        m_row_indexes.init_from_mem(Allocator::get_default(), source.m_row_indexes.get_mem());
        source.m_row_indexes.init_from_ref(Allocator::get_default(), IntegerColumn::create(Allocator::get_default()));
    }
}
