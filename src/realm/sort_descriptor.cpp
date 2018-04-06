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

#include <realm/sort_descriptor.hpp>
#include <realm/table.hpp>
#include <realm/util/assert.hpp>

using namespace realm;

CommonDescriptor::CommonDescriptor(Table const& table, std::vector<std::vector<ColKey>> column_indices)
{
    m_column_ids.resize(column_indices.size());
    for (size_t i = 0; i < m_column_ids.size(); ++i) {
        auto& column_ids = m_column_ids[i];
        auto& indices = column_indices[i];
        REALM_ASSERT(!column_indices.empty());

        column_ids.reserve(indices.size());
        const Table* cur_table = &table;
        for (auto index : indices) {
            column_ids.push_back(ColumnId{cur_table, index});
            DataType col_type = cur_table->get_column_type(index);
            if (col_type == type_Link) {
                cur_table = cur_table->get_link_target(index);
            }
            else if (column_ids.size() != indices.size()) {
                // Only last column in link chain is allowed to be non-link
                throw LogicError(LogicError::type_mismatch);
            }
        }
    }
}

std::unique_ptr<CommonDescriptor> CommonDescriptor::clone() const
{
    return std::unique_ptr<CommonDescriptor>(new CommonDescriptor(*this));
}

std::string CommonDescriptor::get_description(ConstTableRef attached_table) const
{
    std::string description = "DISTINCT(";
    for (size_t i = 0; i < m_column_ids.size(); ++i) {
        const size_t chain_size = m_column_ids[i].size();
        ConstTableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            ColKey col_key = m_column_ids[i][j].col_key;
            StringData col_name = cur_link_table->get_column_name(col_key);
            description += std::string(col_name);
            if (j < chain_size - 1) {
                description += ".";
                cur_link_table = cur_link_table->get_link_target(col_key);
            }
        }
        if (i < m_column_ids.size() - 1) {
            description += ", ";
        }
    }
    description += ")";
    return description;
}

std::string SortDescriptor::get_description(ConstTableRef attached_table) const
{
    std::string description = "SORT(";
    for (size_t i = 0; i < m_column_ids.size(); ++i) {
        const size_t chain_size = m_column_ids[i].size();
        ConstTableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            ColKey col_key = m_column_ids[i][j].col_key;
            StringData col_name = cur_link_table->get_column_name(col_key);
            description += std::string(col_name);
            if (j < chain_size - 1) {
                description += ".";
                cur_link_table = cur_link_table->get_link_target(col_key);
            }
        }
        description += " ";
        if (i < m_ascending.size()) {
            if (m_ascending[i]) {
                description += "ASC";
            }
            else {
                description += "DESC";
            }
        }
        if (i < m_column_ids.size() - 1) {
            description += ", ";
        }
    }
    description += ")";
    return description;
}

SortDescriptor::SortDescriptor(Table const& table, std::vector<std::vector<ColKey>> column_indices,
                               std::vector<bool> ascending)
    : CommonDescriptor(table, column_indices)
    , m_ascending(std::move(ascending))
{
    REALM_ASSERT_EX(m_ascending.empty() || m_ascending.size() == column_indices.size(), m_ascending.size(),
                    column_indices.size());
    if (m_ascending.empty())
        m_ascending.resize(column_indices.size(), true);
}

std::unique_ptr<CommonDescriptor> SortDescriptor::clone() const
{
    return std::unique_ptr<CommonDescriptor>(new SortDescriptor(*this));
}

void SortDescriptor::merge_with(SortDescriptor&& other)
{
    m_column_ids.insert(m_column_ids.begin(), std::make_move_iterator(other.m_column_ids.begin()),
                        std::make_move_iterator(other.m_column_ids.end()));
    // Do not use a move iterator on a vector of bools!
    // It will form a reference to a temporary and return incorrect results.
    m_ascending.insert(m_ascending.begin(), other.m_ascending.begin(), other.m_ascending.end());
}


CommonDescriptor::Sorter::Sorter(std::vector<std::vector<ColumnId>> const& columns,
                                 std::vector<bool> const& ascending, KeyColumn const& key_values)
{
    REALM_ASSERT(!columns.empty());
    REALM_ASSERT_EX(columns.size() == ascending.size(), columns.size(), ascending.size());
    size_t num_objs = key_values.size();

    m_columns.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        m_columns.emplace_back(columns[i].back().table, columns[i].back().col_key, ascending[i]);
        REALM_ASSERT_EX(!columns[i].empty(), i);
        if (columns[i].size() == 1) { // no link chain
            continue;
        }

        auto& translated_keys = m_columns.back().translated_keys;
        auto& is_null = m_columns.back().is_null;
        translated_keys.resize(num_objs);
        is_null.resize(num_objs);

        for (size_t row_ndx = 0; row_ndx < num_objs; row_ndx++) {
            ObjKey translated_key = ObjKey(key_values.get(row_ndx));
            for (size_t j = 0; j + 1 < columns[i].size(); ++j) {
                ConstObj obj = columns[i][j].table->get_object(translated_key);
                // type was checked when creating the CommonDescriptor
                if (obj.is_null(columns[i][j].col_key)) {
                    is_null[row_ndx] = true;
                    break;
                }
                translated_key = obj.get<ObjKey>(columns[i][j].col_key);
            }
            translated_keys[row_ndx] = translated_key;
        }
    }
}

std::vector<std::vector<ColKey>> CommonDescriptor::export_column_indices() const
{
    std::vector<std::vector<ColKey>> column_indices;
    column_indices.reserve(m_column_ids.size());
    for (auto& cols : m_column_ids) {
        std::vector<ColKey> indices;
        indices.reserve(cols.size());
        for (auto col_id : cols)
            indices.push_back(col_id.col_key);
        column_indices.push_back(indices);
    }
    return column_indices;
}

std::vector<bool> SortDescriptor::export_order() const
{
    return m_ascending;
}

CommonDescriptor::Sorter CommonDescriptor::sorter(KeyColumn const& row_indexes) const
{
    REALM_ASSERT(!m_column_ids.empty());
    std::vector<bool> ascending(m_column_ids.size(), true);
    return Sorter(m_column_ids, ascending, row_indexes);
}

void CommonDescriptor::execute(IndexPairs& v, const Sorter& predicate, const CommonDescriptor* next) const
{
    using IP = CommonDescriptor::IndexPair;
    // Remove all rows which have a null link along the way to the distinct columns
    if (predicate.has_links()) {
        auto nulls =
            std::remove_if(v.begin(), v.end(), [&](const IP& index) { return predicate.any_is_null(index); });
        v.erase(nulls, v.end());
    }

    // Sort by the columns to distinct on
    std::sort(v.begin(), v.end(), std::ref(predicate));

    // Move duplicates to the back - "not less than" is "equal" since they're sorted
    auto duplicates =
        std::unique(v.begin(), v.end(), [&](const IP& a, const IP& b) { return !predicate(a, b, false); });
    // Erase the duplicates
    v.erase(duplicates, v.end());
    bool will_be_sorted_next = next && next->is_sort();
    if (!will_be_sorted_next) {
        // Restore the original order, this is either the original
        // tableview order or the order of the previous sort
        std::sort(v.begin(), v.end(), [](const IP& a, const IP& b) { return a.index_in_view < b.index_in_view; });
    }
}

SortDescriptor::Sorter SortDescriptor::sorter(KeyColumn const& row_indexes) const
{
    REALM_ASSERT(!m_column_ids.empty());
    return Sorter(m_column_ids, m_ascending, row_indexes);
}

void SortDescriptor::execute(IndexPairs& v, const Sorter& predicate, const CommonDescriptor* next) const
{
    std::sort(v.begin(), v.end(), std::ref(predicate));

    // not doing this on the last step is an optimisation
    if (next) {
        const size_t v_size = v.size();
        // Distinct must choose the winning unique elements by sorted
        // order not by the previous tableview order, the lowest
        // "index_in_view" wins.
        for (size_t i = 0; i < v_size; ++i) {
            v[i].index_in_view = i;
        }
    }
}

// This function must conform to 'is less' predicate - that is:
// return true if i is strictly smaller than j
bool SortDescriptor::Sorter::operator()(IndexPair i, IndexPair j, bool total_ordering) const
{
    // Sorting can be specified by multiple columns, so that if two entries in the first column are
    // identical, then the rows are ordered according to the second column, and so forth. For the
    // first column, all the payload of the View is cached in IndexPair::cached_value.
    for (size_t t = 0; t < m_columns.size(); t++) {
        if (!m_columns[t].translated_keys.empty()) {
            bool null_i = m_columns[t].is_null[i.index_in_view];
            bool null_j = m_columns[t].is_null[j.index_in_view];

            if (null_i && null_j) {
                continue;
            }
            if (null_i || null_j) {
                // Sort null links at the end if m_ascending[t], else at beginning.
                return m_columns[t].ascending != null_i;
            }
        }

        int c;

        if (t == 0) {
            c = i.cached_value.compare(j.cached_value);
        }
        else {
            ObjKey key_i = i.key_for_object;
            ObjKey key_j = j.key_for_object;

            if (!m_columns[t].translated_keys.empty()) {
                key_i = m_columns[t].translated_keys[i.index_in_view];
                key_j = m_columns[t].translated_keys[j.index_in_view];
            }
            ConstObj obj_i = m_columns[t].table->get_object(key_i);
            ConstObj obj_j = m_columns[t].table->get_object(key_j);

            c = obj_i.cmp(obj_j, m_columns[t].col_key);
        }
        // if c is negative i comes before j
        if (c) {
            return m_columns[t].ascending ? c < 0 : c > 0;
        }

    }
    // make sort stable by using original index as final comparison
    return total_ordering ? i.index_in_view < j.index_in_view : 0;
}

void SortDescriptor::Sorter::cache_first_column(IndexPairs& v)
{
    auto& col = m_columns[0];
    ColKey ck = col.col_key;
    DataType dt = m_columns[0].table->get_column_type(ck);
    bool is_nullable = col.table->is_nullable(ck);

    for (size_t i = 0; i < v.size(); i++) {
        IndexPair& index = v[i];
        ObjKey key = index.key_for_object;

        if (!col.translated_keys.empty()) {
            if (col.is_null[i]) {
                index.cached_value = Mixed();
                continue;
            }
            else {
                key = col.translated_keys[v[i].index_in_view];
            }
        }

        ConstObj obj = col.table->get_object(key);
        switch (dt) {
            case type_Int:
                if (is_nullable) {
                    auto val = obj.get<util::Optional<int64_t>>(ck);
                    if (val) {
                        index.cached_value = val.value();
                    }
                    else {
                        index.cached_value = Mixed();
                    }
                }
                else {
                    index.cached_value = obj.get<Int>(ck);
                }
                break;
            case type_Timestamp:
                index.cached_value = obj.get<Timestamp>(ck);
                break;
            case type_String:
                index.cached_value = obj.get<String>(ck);
                break;
            case type_Float:
                index.cached_value = obj.get<Float>(ck);
                break;
            case type_Double:
                index.cached_value = obj.get<Double>(ck);
                break;
            case type_Bool:
                index.cached_value = obj.get<Bool>(ck);
                break;
            case type_Link:
                index.cached_value = obj.get<ObjKey>(ck);
                break;
            default:
                REALM_UNREACHABLE();
                break;
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

const CommonDescriptor* DescriptorOrdering::operator[](size_t ndx) const
{
    return m_descriptors.at(ndx).get(); // may throw std::out_of_range
}

bool DescriptorOrdering::will_apply_sort() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<CommonDescriptor>& desc) {
        REALM_ASSERT(desc->is_valid());
        return desc->is_sort();
    });
}

bool DescriptorOrdering::will_apply_distinct() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<CommonDescriptor>& desc) {
        REALM_ASSERT(desc->is_valid());
        return !desc->is_sort();
    });
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
        std::vector<std::vector<std::vector<ColKey>>> column_indices;
        std::vector<std::vector<bool>> column_orders;
        column_indices.reserve(num_descriptors);
        column_orders.reserve(num_descriptors);
        for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
            const CommonDescriptor* desc = descriptors[desc_ndx];
            column_indices.push_back(desc->export_column_indices());
            column_orders.push_back(desc->export_order());
        }
        patch.reset(new DescriptorOrderingHandoverPatch{std::move(column_indices), std::move(column_orders)});
    }
}

DescriptorOrdering DescriptorOrdering::create_from_and_consume_patch(HandoverPatch& patch, Table const& table)
{
    DescriptorOrdering ordering;
    if (patch) {
        const size_t num_descriptors = patch->columns.size();
        REALM_ASSERT_EX(num_descriptors == patch->ascending.size(), num_descriptors, patch->ascending.size());
        for (size_t desc_ndx = 0; desc_ndx < num_descriptors; ++desc_ndx) {
            if (patch->columns[desc_ndx].size() != patch->ascending[desc_ndx].size()) {
                // If size differs, it must be a distinct
                ordering.append_distinct(DistinctDescriptor(table, std::move(patch->columns[desc_ndx])));
            }
            else {
                ordering.append_sort(SortDescriptor(table, std::move(patch->columns[desc_ndx]),
                                                    std::move(patch->ascending[desc_ndx])));
            }
        }
        patch.reset();
    }
    return ordering;
}
