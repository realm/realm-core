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
#include <realm/db.hpp>
#include <realm/util/assert.hpp>

using namespace realm;

ColumnsDescriptor::ColumnsDescriptor(std::vector<std::vector<ColKey>> column_keys)
    : m_column_keys(std::move(column_keys))
{
}

std::unique_ptr<BaseDescriptor> DistinctDescriptor::clone() const
{
    return std::unique_ptr<DistinctDescriptor>(new DistinctDescriptor(*this));
}

void ColumnsDescriptor::collect_dependencies(const Table* table, std::vector<TableKey>& table_keys) const
{
    for (auto& columns : m_column_keys) {
        auto sz = columns.size();
        // If size is 0 or 1 there is no link chain and hence no additional tables to check
        if (sz > 1) {
            const Table* t = table;
            for (size_t i = 0; i < sz - 1; i++) {
                ColKey col = columns[i];
                ConstTableRef target_table;
                if (t->get_column_type(col) == type_Link) {
                    target_table = t->get_link_target(col);
                }
                if (!target_table)
                    return;
                table_keys.push_back(target_table->get_key());
                t = target_table;
            }
        }
    }
}

std::string DistinctDescriptor::get_description(ConstTableRef attached_table) const
{
    std::string description = "DISTINCT(";
    for (size_t i = 0; i < m_column_keys.size(); ++i) {
        const size_t chain_size = m_column_keys[i].size();
        ConstTableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            ColKey col_key = m_column_keys[i][j];
            StringData col_name = cur_link_table->get_column_name(col_key);
            description += std::string(col_name);
            if (j < chain_size - 1) {
                description += ".";
                cur_link_table = cur_link_table->get_link_target(col_key);
            }
        }
        if (i < m_column_keys.size() - 1) {
            description += ", ";
        }
    }
    description += ")";
    return description;
}

std::string SortDescriptor::get_description(ConstTableRef attached_table) const
{
    std::string description = "SORT(";
    for (size_t i = 0; i < m_column_keys.size(); ++i) {
        const size_t chain_size = m_column_keys[i].size();
        ConstTableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            ColKey col_key = m_column_keys[i][j];
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
        if (i < m_column_keys.size() - 1) {
            description += ", ";
        }
    }
    description += ")";
    return description;
}

SortDescriptor::SortDescriptor(std::vector<std::vector<ColKey>> column_keys, std::vector<bool> ascending)
    : ColumnsDescriptor(std::move(column_keys))
    , m_ascending(std::move(ascending))
{
    REALM_ASSERT_EX(m_ascending.empty() || m_ascending.size() == m_column_keys.size(), m_ascending.size(),
                    m_column_keys.size());
    if (m_ascending.empty())
        m_ascending.resize(m_column_keys.size(), true);
}

std::unique_ptr<BaseDescriptor> SortDescriptor::clone() const
{
    return std::unique_ptr<ColumnsDescriptor>(new SortDescriptor(*this));
}

void SortDescriptor::merge_with(SortDescriptor&& other)
{
    m_column_keys.insert(m_column_keys.begin(), other.m_column_keys.begin(), other.m_column_keys.end());
    // Do not use a move iterator on a vector of bools!
    // It will form a reference to a temporary and return incorrect results.
    m_ascending.insert(m_ascending.begin(), other.m_ascending.begin(), other.m_ascending.end());
}


BaseDescriptor::Sorter::Sorter(std::vector<std::vector<ColKey>> const& column_lists,
                               std::vector<bool> const& ascending, Table const& root_table,
                               KeyColumn const& key_values)
{
    REALM_ASSERT(!column_lists.empty());
    REALM_ASSERT_EX(column_lists.size() == ascending.size(), column_lists.size(), ascending.size());
    size_t num_objs = key_values.size();

    m_columns.reserve(column_lists.size());
    for (size_t i = 0; i < column_lists.size(); ++i) {
        auto& columns = column_lists[i];
        auto sz = columns.size();
        REALM_ASSERT_EX(!columns.empty(), i);

        if (sz == 1) { // no link chain
            m_columns.emplace_back(&root_table, columns[0], ascending[i]);
            continue;
        }

        std::vector<const Table*> tables = {&root_table};
        tables.resize(sz);
        for (size_t j = 0; j + 1 < sz; ++j) {
            if (tables[j]->get_column_type(columns[j]) != type_Link) {
                // Only last column in link chain is allowed to be non-link
                throw LogicError(LogicError::type_mismatch);
            }
            tables[j + 1] = tables[j]->get_link_target(columns[j]);
        }

        m_columns.emplace_back(tables.back(), columns.back(), ascending[i]);

        auto& translated_keys = m_columns.back().translated_keys;
        auto& is_null = m_columns.back().is_null;
        translated_keys.resize(num_objs);
        is_null.resize(num_objs);

        for (size_t row_ndx = 0; row_ndx < num_objs; row_ndx++) {
            ObjKey translated_key = ObjKey(key_values.get(row_ndx));
            for (size_t j = 0; j + 1 < sz; ++j) {
                ConstObj obj = tables[j]->get_object(translated_key);
                // type was checked when creating the ColumnsDescriptor
                if (obj.is_null(columns[j])) {
                    is_null[row_ndx] = true;
                    break;
                }
                translated_key = obj.get<ObjKey>(columns[j]);
            }
            translated_keys[row_ndx] = translated_key;
        }
    }
}

BaseDescriptor::Sorter DistinctDescriptor::sorter(Table const& table, KeyColumn const& row_indexes) const
{
    REALM_ASSERT(!m_column_keys.empty());
    std::vector<bool> ascending(m_column_keys.size(), true);
    return Sorter(m_column_keys, ascending, table, row_indexes);
}

void DistinctDescriptor::execute(IndexPairs& v, const Sorter& predicate, const BaseDescriptor* next) const
{
    using IP = ColumnsDescriptor::IndexPair;
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
    bool will_be_sorted_next = next && next->get_type() == DescriptorType::Sort;
    if (!will_be_sorted_next) {
        // Restore the original order, this is either the original
        // tableview order or the order of the previous sort
        std::sort(v.begin(), v.end(), [](const IP& a, const IP& b) { return a.index_in_view < b.index_in_view; });
    }
}

BaseDescriptor::Sorter SortDescriptor::sorter(Table const& table, KeyColumn const& row_indexes) const
{
    REALM_ASSERT(!m_column_keys.empty());
    return Sorter(m_column_keys, m_ascending, table, row_indexes);
}

void SortDescriptor::execute(IndexPairs& v, const Sorter& predicate, const BaseDescriptor* next) const
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

std::string LimitDescriptor::get_description(ConstTableRef) const
{
    return "LIMIT(" + serializer::print_value(m_limit) + ")";
}

std::unique_ptr<BaseDescriptor> LimitDescriptor::clone() const
{
    return std::unique_ptr<BaseDescriptor>(new LimitDescriptor(*this));
}

void LimitDescriptor::execute(IndexPairs& v, const Sorter&, const BaseDescriptor*) const
{
    if (v.size() > m_limit) {
        v.m_removed_by_limit += v.size() - m_limit;
        v.erase(v.begin() + m_limit, v.end());
    }
}


// This function must conform to 'is less' predicate - that is:
// return true if i is strictly smaller than j
bool BaseDescriptor::Sorter::operator()(IndexPair i, IndexPair j, bool total_ordering) const
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

void BaseDescriptor::Sorter::cache_first_column(IndexPairs& v)
{
    if (m_columns.empty())
        return;

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
                if (is_nullable && obj.is_null(ck)) {
                    index.cached_value = Mixed();
                }
                else {
                    index.cached_value = obj.get<Float>(ck);
                }
                break;
            case type_Double:
                if (is_nullable && obj.is_null(ck)) {
                    index.cached_value = Mixed();
                }
                else {
                    index.cached_value = obj.get<Double>(ck);
                }
                break;
            case type_Bool:
                if (is_nullable && obj.is_null(ck)) {
                    index.cached_value = Mixed();
                }
                else {
                    index.cached_value = obj.get<Bool>(ck);
                }
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

void DescriptorOrdering::append_limit(LimitDescriptor limit)
{
    if (limit.is_valid()) {
        m_descriptors.emplace_back(new LimitDescriptor(std::move(limit)));
    }
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
        REALM_ASSERT(desc->is_valid());
        return desc->get_type() == DescriptorType::Sort;
    });
}

bool DescriptorOrdering::will_apply_distinct() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc->is_valid());
        return desc->get_type() == DescriptorType::Distinct;
    });
}

bool DescriptorOrdering::will_apply_limit() const
{
    return std::any_of(m_descriptors.begin(), m_descriptors.end(), [](const std::unique_ptr<BaseDescriptor>& desc) {
        REALM_ASSERT(desc->is_valid());
        return desc->get_type() == DescriptorType::Limit;
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

void DescriptorOrdering::collect_dependencies(const Table* table)
{
    m_dependencies.clear();
    for (auto& descr : m_descriptors) {
        descr->collect_dependencies(table, m_dependencies);
    }
}

void DescriptorOrdering::get_versions(const Group* group, TableVersions& versions) const
{
    for (auto table_key : m_dependencies) {
        REALM_ASSERT_DEBUG(group);
        versions.emplace_back(table_key, group->get_table(table_key)->get_content_version());
    }
}
