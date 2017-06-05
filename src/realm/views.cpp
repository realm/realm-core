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
    REALM_ASSERT_EX(m_ascending.empty() || m_ascending.size() == column_indices.size(), m_ascending.size(),
                    column_indices.size());
    if (m_ascending.empty())
        m_ascending.resize(column_indices.size(), true);
    if (table.is_degenerate()) {
        // We need access to the column acessors and that's not available in a
        // degenerate table, it would crash if we continued to try and access it
        throw LogicError(LogicError::descriptor_on_degenerate_table);
    }
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

void SortDescriptor::merge_with(SortDescriptor&& other)
{
    m_columns.insert(m_columns.end(),
                     std::make_move_iterator(other.m_columns.begin()),
                     std::make_move_iterator(other.m_columns.end()));
    // Do not use a move iterator on a vector of bools!
    // It will form a reference to a temporary and return incorrect results.
    m_ascending.insert(m_ascending.end(),
                       other.m_ascending.begin(),
                       other.m_ascending.end());
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

std::vector<std::vector<size_t>> SortDescriptor::export_column_indices() const
{
    std::vector<std::vector<size_t>> column_indices;
    column_indices.reserve(m_columns.size());
    for (auto& cols : m_columns) {
        std::vector<size_t> indices;
        indices.reserve(cols.size());
        for (const ColumnBase* col : cols)
            indices.push_back(col->get_column_index());
        column_indices.push_back(indices);
    }
    return column_indices;
}

std::vector<bool> SortDescriptor::export_order() const
{
    return m_ascending;
}


SortDescriptor::Sorter SortDescriptor::sorter(IntegerColumn const& row_indexes) const
{
    REALM_ASSERT(!m_columns.empty());
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

void DescriptorOrdering::emplace_sort(SortDescriptor&& sort)
{
    size_t orig_length = m_descriptors.size();
    if (orig_length == 0) {
        m_descriptors.emplace_back(std::move(sort));
    }
    else if (descriptor_is_sort(orig_length - 1)) {
        // merge them together by appending internally
        m_descriptors[orig_length - 1].merge_with(std::move(sort));
    }
    else { // last descriptor is distinct, just add it
        m_descriptors.emplace_back(std::move(sort));
    }
}

void DescriptorOrdering::emplace_distinct(DistinctDescriptor&& distinct)
{
    size_t orig_length = m_descriptors.size();
    if (orig_length == 0) {
        // add an empty sort descriptor first to keep ordering
        m_descriptors.emplace_back(SortDescriptor());
        m_descriptors.emplace_back(std::move(distinct));
    }
    else if (descriptor_is_distinct(orig_length - 1)) {
        // add the next distinct operation, separated by a placeholder sort
        m_descriptors.emplace_back(SortDescriptor());
        m_descriptors.emplace_back(std::move(distinct));
    }
    else { // last descriptor is sort, just add it
        m_descriptors.emplace_back(std::move(distinct));
    }
}

bool DescriptorOrdering::descriptor_is_sort(size_t index) const
{
    // The list is kept in alternating order of sort/distinct/sort/distinct...
    return index % 2 == 0;
}

bool DescriptorOrdering::descriptor_is_distinct(size_t index) const
{
    return !descriptor_is_sort(index);
}

const SortDescriptor& DescriptorOrdering::operator[](size_t ndx) const
{
    if (REALM_UNLIKELY(ndx >= m_descriptors.size())) {
        throw LogicError(LogicError::descriptor_out_of_range);
    }
    return m_descriptors[ndx];
}

bool DescriptorOrdering::will_apply_sort() const
{
    size_t num_descriptors = m_descriptors.size();
    for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
        if (descriptor_is_sort(desc_ndx) && m_descriptors[desc_ndx]) {
            return true;
        }
    }
    return false;
}

bool DescriptorOrdering::will_apply_distinct() const
{
    size_t num_descriptors = m_descriptors.size();
    for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
        if (descriptor_is_distinct(desc_ndx) && m_descriptors[desc_ndx]) {
            return true;
        }
    }
    return false;
}

void DescriptorOrdering::generate_patch(DescriptorOrdering const& descriptors, HandoverPatch& patch)
{
    if (!descriptors.is_empty()) {
        const size_t num_descriptors = descriptors.size();
        std::vector<std::vector<std::vector<size_t>>> column_indices;
        std::vector<std::vector<bool>> column_orders;
        column_indices.reserve(num_descriptors);
        column_orders.reserve(num_descriptors);
        for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
            const SortDescriptor& desc = descriptors[desc_ndx];
            column_indices.push_back(desc.export_column_indices());
            column_orders.push_back(desc.export_order());
        }
        patch.reset(new DescriptorOrderingHandoverPatch{std::move(column_indices), std::move(column_orders)});
    }
}

DescriptorOrdering DescriptorOrdering::create_from_and_consume_patch(HandoverPatch& patch, Table const& table)
{
    DescriptorOrdering ordering;
    if (patch) {
        const size_t num_descriptors = patch->columns.size();
        REALM_ASSERT_EX(num_descriptors == patch->ascending.size(),
                        num_descriptors, patch->ascending.size());
        for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
            SortDescriptor desc(table, std::move(patch->columns[desc_ndx]),
                             std::move(patch->ascending[desc_ndx]));
            if (ordering.descriptor_is_sort(desc_ndx)) {
                ordering.emplace_sort(std::move(desc));
            }
            else {
                ordering.emplace_distinct(std::move(desc));
            }
        }
        patch.reset();
    }
    return ordering;
}

void RowIndexes::do_sort(const DescriptorOrdering& ordering) {
    if (ordering.is_empty())
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

    const size_t num_descriptors = ordering.size();
    for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
        if (ordering.descriptor_is_sort(desc_ndx)) {
            // an empty (placeholder) sort descriptor is a no-op
            if (!ordering[desc_ndx]) continue;

            SortDescriptor::Sorter sort_predicate = ordering[desc_ndx].sorter(m_row_indexes);

            std::sort(v.begin(), v.end(), std::ref(sort_predicate));

            bool is_last_ordering = desc_ndx >= num_descriptors - 1;
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
            REALM_ASSERT_DEBUG(ordering.descriptor_is_distinct(desc_ndx));
            REALM_ASSERT_DEBUG(ordering[desc_ndx]); // distinct should always be valid

            // Setting an order on the distinct descriptor is now incorrect.
            // Distinct uses the order of the view. If the order matters,
            // then do a sort beforehand to change the results of distinct.
            if (ordering[desc_ndx].has_custom_order()) {
                throw LogicError(LogicError::unsupported_order_on_distinct);
            }
            auto distinct_predicate = ordering[desc_ndx].sorter(m_row_indexes);

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

            bool will_be_sorted_next = desc_ndx < num_descriptors - 1 && ordering[desc_ndx + 1];
            if (!will_be_sorted_next) {
                // Restore the original order, this is either the original
                // tableview order or the order of the previous sort
                std::sort(v.begin(), v.end(), [](auto a, auto b) { return a.index_in_view < b.index_in_view; });
            }
        }
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
