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
            if (col_type == type_Link || col_type == type_LinkList) {
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
        m_columns.push_back({{}, {}, columns[i].back().table, columns[i].back().col_key, ascending[i]});
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


SortDescriptor::Sorter SortDescriptor::sorter(KeyColumn const& row_indexes) const
{
    REALM_ASSERT(!m_column_ids.empty());
    return Sorter(m_column_ids, m_ascending, row_indexes);
}

bool SortDescriptor::Sorter::operator()(IndexPair i, IndexPair j, bool total_ordering) const
{
    for (size_t t = 0; t < m_columns.size(); t++) {
        ObjKey key_i = i.key_for_object;
        ObjKey key_j = j.key_for_object;

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

            key_i = m_columns[t].translated_keys[i.index_in_view];
            key_j = m_columns[t].translated_keys[j.index_in_view];
        }

        ConstObj obj_i = m_columns[t].table->get_object(key_i);
        ConstObj obj_j = m_columns[t].table->get_object(key_j);
        if (int c = obj_i.cmp(obj_j, m_columns[t].col_key))
            return m_columns[t].ascending ? c > 0 : c < 0;
    }
    // make sort stable by using original index as final comparison
    return total_ordering ? i.index_in_view < j.index_in_view : 0;
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

bool DescriptorOrdering::descriptor_is_sort(size_t index) const
{
    REALM_ASSERT(index < m_descriptors.size());
    SortDescriptor* sort_descr = dynamic_cast<SortDescriptor*>(m_descriptors[index].get());
    return (sort_descr != nullptr);
}

bool DescriptorOrdering::descriptor_is_distinct(size_t index) const
{
    return !descriptor_is_sort(index);
}

const CommonDescriptor* DescriptorOrdering::operator[](size_t ndx) const
{
    return m_descriptors.at(ndx).get(); // may throw std::out_of_range
}

bool DescriptorOrdering::will_apply_sort() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<CommonDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return dynamic_cast<SortDescriptor*>(desc.get()) != nullptr;
    });
}

bool DescriptorOrdering::will_apply_distinct() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<CommonDescriptor>& desc) {
        REALM_ASSERT(desc.get()->is_valid());
        return dynamic_cast<SortDescriptor*>(desc.get()) == nullptr;
    });
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
