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
#include <realm/exceptions.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/table_view.hpp>

#include <typeinfo>

using namespace realm;

namespace {

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
           std::vector<IndexPair> const& rows);
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
                                 std::vector<bool> const& ascending, std::vector<IndexPair> const& rows)
{
    REALM_ASSERT(!columns.empty());
    REALM_ASSERT_EX(columns.size() == ascending.size(), columns.size(), ascending.size());

    m_columns.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        m_columns.push_back({{}, {}, columns[i].back(), ascending[i]});
        REALM_ASSERT_EX(!columns[i].empty(), i);
        if (columns[i].size() == 1) { // no link chain
            continue;
        }

        auto& translated_rows = m_columns.back().translated_row;
        auto& is_null = m_columns.back().is_null;
        size_t max_index = std::max_element(rows.begin(), rows.end(),
                                            [](auto&& a, auto&& b) { return a.index_in_view < b.index_in_view; })->index_in_view;
        translated_rows.resize(max_index + 1);
        is_null.resize(max_index + 1);

        for (size_t row_ndx = 0; row_ndx < rows.size(); row_ndx++) {
            size_t index_in_view = rows[row_ndx].index_in_view;
            size_t translated_index = rows[row_ndx].index_in_column;
            for (size_t j = 0; j + 1 < columns[i].size(); ++j) {
                // type was checked when creating the ColumnsDescriptor
                auto link_col = static_cast<const LinkColumn*>(columns[i][j]);
                if (link_col->is_null(translated_index)) {
                    is_null[index_in_view] = true;
                    break;
                }
                translated_index = link_col->get_link(translated_index);
            }
            translated_rows[index_in_view] = translated_index;
        }
    }
}

DescriptorExport ColumnsDescriptor::export_for_handover() const
{
    std::vector<std::vector<DescriptorLinkPath>> column_indices;
    column_indices.reserve(m_columns.size());
    for (auto& cols : m_columns) {
        std::vector<DescriptorLinkPath> indices;
        indices.reserve(cols.size());
        for (const ColumnBase* col : cols)
            indices.push_back(DescriptorLinkPath{col->get_column_index(), realm::npos, false});
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

std::string SortDescriptor::get_description(ConstTableRef attached_table) const
{
    std::string description = "SORT(";
    for (size_t i = 0; i < m_columns.size(); ++i) {
        const size_t chain_size = m_columns[i].size();
        ConstTableRef cur_link_table = attached_table;
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

std::string ColumnsDescriptor::description_for_prefix(std::string prefix, ConstTableRef attached_table) const
{
    std::string description = prefix + "(";
    for (size_t i = 0; i < m_columns.size(); ++i) {
        const size_t chain_size = m_columns[i].size();
        ConstTableRef cur_link_table = attached_table;
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


std::string ColumnsDescriptor::get_description(ConstTableRef attached_table) const
{
    return description_for_prefix("DISTINCT", attached_table);
}

ColumnsDescriptor::Sorter ColumnsDescriptor::sorter(std::vector<IndexPair> const& rows) const
{
    REALM_ASSERT(!m_columns.empty());
    std::vector<bool> ascending(m_columns.size(), true);
    return Sorter(m_columns, ascending, rows);
}


SortDescriptor::Sorter SortDescriptor::sorter(std::vector<IndexPair> const& rows) const
{
    REALM_ASSERT(!m_columns.empty());
    return Sorter(m_columns, m_ascending, rows);
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

std::string LimitDescriptor::get_description(ConstTableRef) const
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

IncludeDescriptor::IncludeDescriptor(const Table& table, const std::vector<std::vector<LinkPathPart>>& column_links)
    : ColumnsDescriptor()
{
    if (table.is_degenerate()) {
        // We need access to the column acessors and that's not available in a
        // degenerate table. Since inclusion on an empty table is a noop just return.
        return;
    }
    using tf = _impl::TableFriend;
    m_columns.resize(column_links.size());
    m_backlink_sources.resize(column_links.size());
    for (size_t i = 0; i < m_columns.size(); ++i) {
        auto& columns = m_columns[i];
        auto& links = column_links[i];
        auto& backlink_source = m_backlink_sources[i];
        REALM_ASSERT(!column_links.empty());

        columns.reserve(links.size());
        backlink_source.reserve(links.size());
        const Table* cur_table = &table;
        size_t link_ndx = 0;
        for (auto link : links) {
            if (bool(link.from)) { // backlink
                REALM_ASSERT(cur_table == link.from->get_link_target(link.column_ndx));
                auto& col = tf::get_column(*link.from, link.column_ndx);
                columns.push_back(&col);
                backlink_source.push_back(link.from);
                if (auto link_col = dynamic_cast<const LinkColumnBase*>(&col)) { // LinkColumn and ListColumn
                    if (link_col->get_target_table() != *cur_table) {
                        // the link does not point to the last table in the chain
                        throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: this link does not "
                                                            "connect to the previous table ('%3').",
                                                            i, link_ndx, cur_table->get_name()));
                    }
                    cur_table = link.from.get();
                }
                else {
                    throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: a backlink was denoted "
                                                        "but this column ('%3') is not a link.",
                                                        i, link_ndx, cur_table->get_column_name(link.column_ndx)));
                }
            }
            else { // forward link/list
                auto& col = tf::get_column(*cur_table, link.column_ndx);
                columns.push_back(&col);
                backlink_source.push_back(TableRef());
                if (auto link_col = dynamic_cast<const LinkColumnBase*>(&col)) { // LinkColumn and ListColumn
                    if (columns.size() == links.size()) {
                        // An inclusion must end with a backlink column, link/list columns are included automatically
                        throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: the last part of an "
                                                            "included path must be a backlink column.",
                                                            i, link_ndx));
                    }
                    cur_table = &link_col->get_target_table();
                }
                else {
                    // An inclusion chain must consist entirely of link/list/backlink columns
                    throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: all columns in the path "
                                                        "must be a link/list/backlink type but this column ('%3') "
                                                        "is a different type.",
                                                        i, link_ndx, cur_table->get_column_name(link.column_ndx)));
                }
            }
            ++link_ndx;
        }
    }
}

std::string IncludeDescriptor::get_description(ConstTableRef attached_table) const
{
    realm::util::serializer::SerialisationState basic_serialiser;
    std::string description = "INCLUDE(";
    for (size_t i = 0; i < m_columns.size(); ++i) {
        auto chain = m_columns[i];
        const size_t chain_size = chain.size();
        ConstTableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            if (j != 0) {
                description += realm::util::serializer::value_separator;
            }

            size_t col_ndx = chain[j]->get_column_index();
            if (ConstTableRef from_table = m_backlink_sources[i][j]) { // backlink
                REALM_ASSERT_DEBUG(col_ndx < from_table->get_column_count());
                REALM_ASSERT_DEBUG(from_table->get_link_target(col_ndx)->get_name() == cur_link_table->get_name());
                description += basic_serialiser.get_backlink_column_name(from_table, col_ndx);
                cur_link_table = from_table;
            }
            else {
                REALM_ASSERT_DEBUG(col_ndx < cur_link_table->get_column_count());
                description += basic_serialiser.get_column_name(cur_link_table, col_ndx);
                if (j < chain_size - 1) {
                    cur_link_table = cur_link_table->get_link_target(col_ndx);
                }
            }
        }
        if (i < m_columns.size() - 1) {
            description += ", ";
        }
    }
    description += ")";
    return description;
}

std::unique_ptr<BaseDescriptor> IncludeDescriptor::clone() const
{
    return std::unique_ptr<BaseDescriptor>(new IncludeDescriptor(*this));
}

DescriptorExport IncludeDescriptor::export_for_handover() const
{
    std::vector<std::vector<DescriptorLinkPath>> column_indices;
    column_indices.reserve(m_columns.size());
    REALM_ASSERT_EX(m_backlink_sources.size() == m_columns.size(), m_backlink_sources.size(), m_columns.size());
    for (size_t i = 0; i < m_columns.size(); ++i) {
        std::vector<DescriptorLinkPath> indices;
        const size_t chain_size = m_columns[i].size();
        indices.reserve(chain_size);
        REALM_ASSERT_EX(m_backlink_sources[i].size() == chain_size, m_backlink_sources[i].size(), chain_size);
        for (size_t j = 0; j < chain_size; ++j) {
            auto col_ndx = m_columns[i][j]->get_column_index();
            if (ConstTableRef from_table = m_backlink_sources[i][j]) {
                indices.push_back(DescriptorLinkPath{col_ndx, from_table->get_index_in_group(), true});
            }
            else {
                indices.push_back(DescriptorLinkPath{col_ndx, realm::npos, false});
            }
        }
        column_indices.push_back(indices);
    }
    DescriptorExport out;
    out.columns = column_indices;
    out.type = DescriptorType::Include;
    return out;
}

void IncludeDescriptor::append(const IncludeDescriptor& other)
{
    REALM_ASSERT_DEBUG(other.m_backlink_sources.size() == other.m_columns.size());
    for (size_t i = 0; i < other.m_columns.size(); ++i) {
        this->m_columns.push_back(other.m_columns[i]);
        this->m_backlink_sources.push_back(other.m_backlink_sources[i]);
    }
}

void IncludeDescriptor::report_included_backlinks(
    const Table* origin, size_t row_ndx,
    std::function<void(const Table*, const std::unordered_set<size_t>&)> reporter) const
{
    REALM_ASSERT_DEBUG(origin);
    REALM_ASSERT_DEBUG(row_ndx < origin->size());

    for (size_t i = 0; i < m_columns.size(); ++i) {
        const Table* table = origin;
        std::unordered_set<size_t> rows_to_explore;
        rows_to_explore.insert(row_ndx);
        for (size_t j = 0; j < m_columns[i].size(); ++j) {
            std::unordered_set<size_t> results_of_next_table;
            if (bool(m_backlink_sources[i][j])) { // backlink
                const Table& from_table = *m_backlink_sources[i][j].get();
                size_t from_col_ndx = m_columns[i][j]->get_column_index();
                for (auto row_to_explore : rows_to_explore) {
                    size_t num_backlinks = table->get_backlink_count(row_to_explore, from_table, from_col_ndx);
                    for (size_t backlink_ndx = 0; backlink_ndx < num_backlinks; ++backlink_ndx) {
                        results_of_next_table.insert(
                            table->get_backlink(row_to_explore, from_table, from_col_ndx, backlink_ndx));
                    }
                }
                reporter(&from_table, results_of_next_table); // only report backlinks
                table = &from_table;
            }
            else {
                size_t col_ndx = m_columns[i][j]->get_column_index();
                DataType col_type = table->get_column_type(col_ndx);
                if (col_type == type_Link) {
                    for (auto row_to_explore : rows_to_explore) {
                        size_t link_translation = table->get_link(col_ndx, row_to_explore);
                        if (link_translation != realm::npos) { // null links terminate a chain
                            results_of_next_table.insert(link_translation);
                        }
                    }
                }
                else if (col_type == type_LinkList) {
                    for (auto row_to_explore : rows_to_explore) {
                        ConstLinkViewRef links = table->get_linklist(col_ndx, row_to_explore);
                        const size_t num_links = links->size();
                        for (size_t link_ndx = 0; link_ndx < num_links; ++link_ndx) {
                            results_of_next_table.insert(links->get(link_ndx).get_index());
                        }
                    }
                }
                else {
                    // unexpected column type, type checking already happened
                    // in the IncludeDescriptor constructor so this should never happen
                    REALM_UNREACHABLE();
                }
                ConstTableRef linked_table = table->get_link_target(col_ndx);
                table = linked_table.get();
            }
            rows_to_explore = results_of_next_table;
        }
    }
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
    if (!m_descriptors.empty() && m_descriptors.back()->get_type() == DescriptorType::Sort) {
        SortDescriptor* previous_sort = static_cast<SortDescriptor*>(m_descriptors.back().get());
        previous_sort->merge_with(std::move(sort));
        return;
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

void DescriptorOrdering::append_include(IncludeDescriptor include)
{
    if (include.is_valid()) {
        m_descriptors.emplace_back(new IncludeDescriptor(std::move(include)));
    }
}

util::Optional<size_t> DescriptorOrdering::remove_all_limits()
{
    size_t min_limit = size_t(-1);
    for (auto it = m_descriptors.begin(); it != m_descriptors.end();) {
        if ((*it)->get_type() == DescriptorType::Limit) {
            const LimitDescriptor* limit = static_cast<const LimitDescriptor*>(it->get());
            if (limit->get_limit() < min_limit) {
                min_limit = limit->get_limit();
            }
            it = m_descriptors.erase(it);
        } else {
            ++it;
        }
    }
    return min_limit == size_t(-1) ? util::none : util::some<size_t>(min_limit);
}

bool DescriptorOrdering::descriptor_is_sort(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    return m_descriptors[index]->get_type() == DescriptorType::Sort;
}

bool DescriptorOrdering::descriptor_is_distinct(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    return m_descriptors[index]->get_type() == DescriptorType::Distinct;
}

bool DescriptorOrdering::descriptor_is_limit(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    return m_descriptors[index]->get_type() == DescriptorType::Limit;
}

bool DescriptorOrdering::descriptor_is_include(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    return m_descriptors[index]->get_type() == DescriptorType::Include;
}

DescriptorType DescriptorOrdering::get_type(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    return m_descriptors[index]->get_type();
}

const BaseDescriptor* DescriptorOrdering::operator[](size_t ndx) const
{
    return m_descriptors.at(ndx).get(); // may throw std::out_of_range
}

bool DescriptorOrdering::will_apply_sort() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return desc->get_type() == DescriptorType::Sort;
    });
}

bool DescriptorOrdering::will_apply_distinct() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return desc->get_type() == DescriptorType::Distinct;
    });
}

bool DescriptorOrdering::will_apply_limit() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return desc->get_type() == DescriptorType::Limit;
    });
}

bool DescriptorOrdering::will_apply_include() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return desc->get_type() == DescriptorType::Include;
    });
}

realm::util::Optional<size_t> DescriptorOrdering::get_min_limit() const
{
    realm::util::Optional<size_t> min_limit;
    for (size_t i = 0; i < m_descriptors.size(); ++i) {
        if (m_descriptors[i]->get_type() == DescriptorType::Limit) {
            const LimitDescriptor* limit = static_cast<const LimitDescriptor*>(m_descriptors[i].get());
            REALM_ASSERT(limit);
            min_limit = bool(min_limit) ? std::min(*min_limit, limit->get_limit()) : limit->get_limit();
        }
    }
    return min_limit;
}

bool DescriptorOrdering::will_limit_to_zero() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return (desc->get_type() == DescriptorType::Limit &&
                static_cast<LimitDescriptor*>(desc.get())->get_limit() == 0);
    });
}

IncludeDescriptor DescriptorOrdering::compile_included_backlinks() const
{
    IncludeDescriptor includes;
    for (auto it = m_descriptors.begin(); it != m_descriptors.end(); ++it) {
        REALM_ASSERT_DEBUG(bool(*it));
        if ((*it)->get_type() == DescriptorType::Include) {
            includes.append(*static_cast<const IncludeDescriptor*>(it->get()));
        }
    }
    return includes; // this might be empty: see is_valid()
}

std::string DescriptorOrdering::get_description(ConstTableRef target_table) const
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

std::vector<std::vector<size_t>> generate_forward_paths(const DescriptorExport& single)
{
    std::vector<std::vector<size_t>> paths;
    for (size_t i = 0; i < single.columns.size(); ++i) {
        std::vector<size_t> path;
        for (size_t j = 0; j < single.columns[i].size(); ++j) {
            path.push_back(single.columns[i][j].col_ndx);
        }
        paths.emplace_back(std::move(path));
    }
    return paths;
}

std::vector<std::vector<LinkPathPart>> generate_bidirectional_paths(const DescriptorExport& single,
                                                                    Table const& start)
{
    REALM_ASSERT(start.is_group_level());
    using tf = _impl::TableFriend;

    Group* g = tf::get_parent_group(start);
    REALM_ASSERT(g);
    std::vector<std::vector<LinkPathPart>> paths;
    for (size_t i = 0; i < single.columns.size(); ++i) {
        std::vector<LinkPathPart> path;
        for (size_t j = 0; j < single.columns[i].size(); ++j) {
            if (single.columns[i][j].is_backlink) {
                REALM_ASSERT_EX(g->size() > single.columns[i][j].table_ndx, g->size(), single.columns[i][j].table_ndx,
                                i, j);
                TableRef from_table = g->get_table(single.columns[i][j].table_ndx);
                path.push_back(LinkPathPart(single.columns[i][j].col_ndx, from_table));
            }
            else {
                path.push_back(LinkPathPart(single.columns[i][j].col_ndx));
            }
        }
        paths.emplace_back(std::move(path));
    }
    return paths;
}

DescriptorOrdering DescriptorOrdering::create_from_and_consume_patch(HandoverPatch& patch, Table const& table)
{
    DescriptorOrdering ordering;
    if (patch) {
        for (size_t desc_ndx = 0; desc_ndx < patch->descriptors.size(); ++desc_ndx) {
            DescriptorExport single = patch->descriptors[desc_ndx];

            switch (single.type) {
                case DescriptorType::Sort:
                    ordering.append_sort(
                        SortDescriptor(table, generate_forward_paths(single), std::move(single.ordering)));
                    break;
                case DescriptorType::Distinct:
                    ordering.append_distinct(DistinctDescriptor(table, generate_forward_paths(single)));
                    break;
                case DescriptorType::Limit:
                    ordering.append_limit(LimitDescriptor(single.limit));
                    break;
                case DescriptorType::Include:
                    ordering.append_include(IncludeDescriptor(table, generate_bidirectional_paths(single, table)));
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
    std::vector<ColumnsDescriptor::IndexPair> v;
    v.reserve(sz);
    // always put any detached refs at the end of the sort
    // FIXME: reconsider if this is the right thing to do
    // FIXME: consider specialized implementations in derived classes
    // (handling detached refs is not required in linkviews)
    for (size_t t = 0; t < sz; t++) {
        int64_t ndx = m_row_indexes.get(t);
        if (ndx != detached_ref) {
            v.push_back({static_cast<size_t>(ndx), t});
        }
        else
            ++detached_ref_count;
    }

    const int num_descriptors = int(ordering.size());
    for (int desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {

        DescriptorType type = ordering.get_type(desc_ndx);
        switch (type) {
            case DescriptorType::Sort:
            {
                const auto* sort_descr = static_cast<const SortDescriptor*>(ordering[desc_ndx]);
                SortDescriptor::Sorter sort_predicate = sort_descr->sorter(v);

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
                break;
            }
            case DescriptorType::Distinct:
            {
                const auto* distinct_descr = static_cast<const DistinctDescriptor*>(ordering[desc_ndx]);
                auto distinct_predicate = distinct_descr->sorter(v);

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

                break;
            }
            case DescriptorType::Limit:
            {
                const auto* limit_descr = static_cast<const LimitDescriptor*>(ordering[desc_ndx]);
                const size_t limit = limit_descr->get_limit();
                if (v.size() > limit) {
                    m_limit_count += v.size() - limit;
                    v.erase(v.begin() + limit, v.end());
                }
                break;
            }
            case DescriptorType::Include: {
                // Include descriptors have no effect on core queries; they only operate at the sync level
                break;
            }
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
