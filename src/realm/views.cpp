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

#include <realm/util/optional.hpp>

using namespace realm;

namespace {
struct IndexPair {
    size_t index_in_column;
    size_t index_in_view;
};
} // anonymous namespace

ColumnsDescriptor::ColumnsDescriptor(Table const& table, std::vector<std::vector<size_t>> column_indices)
{
    if (table.is_degenerate()) {
        // We need access to the column acessors and that's not available in a
        // degenerate table. Since sorting an empty table is a noop just return.
        return;
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

std::unique_ptr<BaseDescriptor> ColumnsDescriptor::clone() const
{
    return std::unique_ptr<BaseDescriptor>(new ColumnsDescriptor(*this));
}

SortDescriptor::SortDescriptor(Table const& table, std::vector<std::vector<size_t>> column_indices,
                               std::vector<bool> ascending)
    : ColumnsDescriptor(table, column_indices)
    , m_ascending(std::move(ascending))
{
    REALM_ASSERT_EX(m_ascending.empty() || m_ascending.size() == column_indices.size(), m_ascending.size(),
                    column_indices.size());
    if (m_ascending.empty())
        m_ascending.resize(column_indices.size(), true);
    if (table.is_degenerate()) {
        m_ascending.clear(); // keep consistency with empty m_columns
    }
}

std::unique_ptr<BaseDescriptor> SortDescriptor::clone() const
{
    return std::unique_ptr<ColumnsDescriptor>(new SortDescriptor(*this));
}

void SortDescriptor::merge_with(SortDescriptor&& other)
{
    m_columns.insert(m_columns.begin(),
                     std::make_move_iterator(other.m_columns.begin()),
                     std::make_move_iterator(other.m_columns.end()));
    // Do not use a move iterator on a vector of bools!
    // It will form a reference to a temporary and return incorrect results.
    m_ascending.insert(m_ascending.begin(),
                       other.m_ascending.begin(),
                       other.m_ascending.end());
}

class ColumnsDescriptor::Sorter {
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
                           [=](auto&& col) { return col.is_null.empty() ? false : col.is_null[i.index_in_view]; });
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

ColumnsDescriptor::Sorter::Sorter(std::vector<std::vector<const ColumnBase*>> const& columns,
                                 std::vector<bool> const& ascending, IntegerColumn const& row_indexes)
{
    REALM_ASSERT(!columns.empty());
    REALM_ASSERT_EX(columns.size() == ascending.size(), columns.size(), ascending.size());
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
                // type was checked when creating the ColumnsDescriptor
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

DescriptorExport ColumnsDescriptor::export_for_handover() const
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
    DescriptorExport out;
    out.type = DescriptorType::Distinct;
    out.columns = column_indices;
    return out;
}

DescriptorExport SortDescriptor::export_for_handover() const
{
    DescriptorExport out = ColumnsDescriptor::export_for_handover();
    out.type = DescriptorType::Sort;
    out.ordering = m_ascending;
    return out;
}

std::string SortDescriptor::get_description(TableRef attached_table) const
{
    std::string description = "SORT(";
    for (size_t i = 0; i < m_columns.size(); ++i) {
        const size_t chain_size = m_columns[i].size();
        TableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            size_t col_ndx = m_columns[i][j]->get_column_index();
            REALM_ASSERT_DEBUG(col_ndx < cur_link_table->get_column_count());
            StringData col_name = cur_link_table->get_column_name(col_ndx);
            description += std::string(col_name);
            if (j < chain_size - 1) {
                description += ".";
                cur_link_table = cur_link_table->get_link_target(col_ndx);
            }
        }
        description += " ";
        if (i < m_ascending.size()) {
            if (m_ascending[i]) {
                description += "ASC";
            } else {
                description += "DESC";
            }
        }
        if (i < m_columns.size() - 1) {
            description += ", ";
        }
    }
    description += ")";
    return description;
}

std::string ColumnsDescriptor::get_description(TableRef attached_table) const
{
    std::string description = "DISTINCT(";
    for (size_t i = 0; i < m_columns.size(); ++i) {
        const size_t chain_size = m_columns[i].size();
        TableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            size_t col_ndx = m_columns[i][j]->get_column_index();
            REALM_ASSERT_DEBUG(col_ndx < cur_link_table->get_column_count());
            StringData col_name = cur_link_table->get_column_name(col_ndx);
            description += std::string(col_name);
            if (j < chain_size - 1) {
                description += ".";
                cur_link_table = cur_link_table->get_link_target(col_ndx);
            }
        }
        if (i < m_columns.size() - 1) {
            description += ", ";
        }
    }
    description += ")";
    return description;
}

ColumnsDescriptor::Sorter ColumnsDescriptor::sorter(IntegerColumn const& row_indexes) const
{
    REALM_ASSERT(!m_columns.empty());
    std::vector<bool> ascending(m_columns.size(), true);
    return Sorter(m_columns, ascending, row_indexes);
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

LimitDescriptor::LimitDescriptor(size_t limit)
    : m_limit(limit)
{
}

std::string LimitDescriptor::get_description(TableRef) const
{
    return "LIMIT(" + serializer::print_value(m_limit) + ")";
}

std::unique_ptr<BaseDescriptor> LimitDescriptor::clone() const
{
    return std::unique_ptr<BaseDescriptor>(new LimitDescriptor(*this));
}

DescriptorExport LimitDescriptor::export_for_handover() const
{
    DescriptorExport out;
    out.type = DescriptorType::Limit;
    out.limit = m_limit;
    return out;
}

DescriptorOrdering::DescriptorOrdering(const DescriptorOrdering& other)
{
    for (const auto& d : other.m_descriptors) {
        m_descriptors.emplace_back(d->clone());
    }
}

DescriptorOrdering& DescriptorOrdering::operator=(const DescriptorOrdering& rhs)
{
    if (&rhs != this) {
        m_descriptors.clear();
        for (const auto& d : rhs.m_descriptors) {
            m_descriptors.emplace_back(d->clone());
        }
    }
    return *this;
}

void DescriptorOrdering::append_sort(SortDescriptor sort)
{
    if (!sort.is_valid()) {
        return;
    }
    if (!m_descriptors.empty()) {
        if (SortDescriptor* previous_sort = dynamic_cast<SortDescriptor*>(m_descriptors.back().get())) {
            previous_sort->merge_with(std::move(sort));
            return;
        }
    }
    m_descriptors.emplace_back(new SortDescriptor(std::move(sort)));
}

void DescriptorOrdering::append_distinct(DistinctDescriptor distinct)
{
    if (distinct.is_valid()) {
        m_descriptors.emplace_back(new DistinctDescriptor(std::move(distinct)));
    }
}

void DescriptorOrdering::append_limit(LimitDescriptor limit)
{
    REALM_ASSERT(limit.is_valid());
    m_descriptors.emplace_back(new LimitDescriptor(std::move(limit)));
}

bool DescriptorOrdering::descriptor_is_sort(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    SortDescriptor* sort_descr = dynamic_cast<SortDescriptor*>(m_descriptors[index].get());
    return (sort_descr != nullptr);
}

bool DescriptorOrdering::descriptor_is_distinct(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    SortDescriptor* sort_descr = dynamic_cast<SortDescriptor*>(m_descriptors[index].get());
    LimitDescriptor* limit_descr = dynamic_cast<LimitDescriptor*>(m_descriptors[index].get());
    return (!sort_descr && !limit_descr); // dynamic cast of a sort descriptor to ColumnsDescriptor will succeed
}

bool DescriptorOrdering::descriptor_is_limit(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    LimitDescriptor* desc = dynamic_cast<LimitDescriptor*>(m_descriptors[index].get());
    return (desc != nullptr);
}

const BaseDescriptor* DescriptorOrdering::operator[](size_t ndx) const
{
    return m_descriptors.at(ndx).get(); // may throw std::out_of_range
}

bool DescriptorOrdering::will_apply_sort() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return dynamic_cast<SortDescriptor*>(desc.get()) != nullptr;
    });
}

bool DescriptorOrdering::will_apply_distinct() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return dynamic_cast<SortDescriptor*>(desc.get()) == nullptr && dynamic_cast<LimitDescriptor*>(desc.get()) == nullptr;
    });
}

bool DescriptorOrdering::will_apply_limit() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return dynamic_cast<LimitDescriptor*>(desc.get()) != nullptr;
    });
}

std::string DescriptorOrdering::get_description(TableRef target_table) const
{
    std::string description = "";
    for (auto it = m_descriptors.begin(); it != m_descriptors.end(); ++it) {
        REALM_ASSERT_DEBUG(bool(*it));
        description += (*it)->get_description(target_table);
        if (it != m_descriptors.end() - 1) {
            description += " ";
        }
    }
    return description;
}

void DescriptorOrdering::generate_patch(DescriptorOrdering const& descriptors, HandoverPatch& patch)
{
    if (!descriptors.is_empty()) {
        const size_t num_descriptors = descriptors.size();
        std::vector<DescriptorExport> out;
        out.reserve(num_descriptors);
        for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
            const BaseDescriptor* desc = descriptors[desc_ndx];
            out.push_back(desc->export_for_handover());
        }
        patch.reset(new DescriptorOrderingHandoverPatch{std::move(out)});
    }
}

DescriptorOrdering DescriptorOrdering::create_from_and_consume_patch(HandoverPatch& patch, Table const& table)
{
    DescriptorOrdering ordering;
    if (patch) {
        for (size_t desc_ndx = 0; desc_ndx < patch->descriptors.size(); ++desc_ndx) {
            DescriptorExport single = patch->descriptors[desc_ndx];
            switch (single.type) {
                case DescriptorType::Sort:
                    ordering.append_sort(SortDescriptor(table, std::move(single.columns), std::move(single.ordering)));
                    break;
                case DescriptorType::Distinct:
                    ordering.append_distinct(DistinctDescriptor(table, std::move(single.columns)));
                    break;
                case DescriptorType::Limit:
                    ordering.append_limit(LimitDescriptor(single.limit));
                    break;
            }
        }
        patch.reset();
    }
    return ordering;
}

void RowIndexes::do_sort(const DescriptorOrdering& ordering) {
    m_limit_count = 0;
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

    const int num_descriptors = int(ordering.size());
    for (int desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
        const BaseDescriptor* common_descr = ordering[desc_ndx];

        if (const auto* sort_descr = dynamic_cast<const SortDescriptor*>(common_descr)) {

            SortDescriptor::Sorter sort_predicate = sort_descr->sorter(m_row_indexes);

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
        else if(const auto* distinct_descr = dynamic_cast<const DistinctDescriptor*>(common_descr)){ // distinct descriptor
            auto distinct_predicate = distinct_descr->sorter(m_row_indexes);

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
        } else if (const auto* limit_descr = dynamic_cast<const LimitDescriptor*>(common_descr)) {
            const size_t limit = limit_descr->get_limit();
            if (v.size() > limit) {
                m_limit_count += v.size() - limit;
                v.erase(v.begin() + limit, v.end());
            }
        } else {
            REALM_UNREACHABLE();
        }
    }
    // Apply the results
    m_row_indexes.clear();
    for (auto& pair : v) {
        m_row_indexes.add(pair.index_in_column);
    }
    for (size_t t = 0; t < detached_ref_count; ++t) {
        m_row_indexes.add(-1);
    }
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
    : m_limit_count(source.m_limit_count)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(source.m_debug_cookie)
#endif
{
    REALM_ASSERT(&source.m_row_indexes.get_alloc() == &Allocator::get_default());

    if (mode == ConstSourcePayload::Copy && source.m_row_indexes.is_attached()) {
        MemRef mem = source.m_row_indexes.clone_deep(Allocator::get_default());
        m_row_indexes.init_from_mem(Allocator::get_default(), mem);
    }
}

RowIndexes::RowIndexes(RowIndexes& source, MutableSourcePayload)
    : m_limit_count(source.m_limit_count)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(source.m_debug_cookie)
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
