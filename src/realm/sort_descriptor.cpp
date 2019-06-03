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

LinkPathPart::LinkPathPart(ColKey col_key, ConstTableRef source)
    : column_key(col_key)
    , from(source->get_key())
{
}


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
                               std::vector<bool> const& ascending, Table const& root_table, const IndexPairs& indexes)
{
    REALM_ASSERT(!column_lists.empty());
    REALM_ASSERT_EX(column_lists.size() == ascending.size(), column_lists.size(), ascending.size());
    size_t translated_size = std::max_element(indexes.begin(), indexes.end())->index_in_view + 1;

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
            tables[j]->report_invalid_key(columns[j]);
            if (columns[j].get_type() != col_type_Link) {
                // Only last column in link chain is allowed to be non-link
                throw LogicError(LogicError::type_mismatch);
            }
            tables[j + 1] = tables[j]->get_link_target(columns[j]);
        }

        m_columns.emplace_back(tables.back(), columns.back(), ascending[i]);

        auto& translated_keys = m_columns.back().translated_keys;
        auto& is_null = m_columns.back().is_null;
        translated_keys.resize(translated_size);
        is_null.resize(translated_size);

        for (const auto& index : indexes) {
            size_t index_in_view = index.index_in_view;
            ObjKey translated_key = index.key_for_object;
            for (size_t j = 0; j + 1 < sz; ++j) {
                ConstObj obj = tables[j]->get_object(translated_key);
                // type was checked when creating the ColumnsDescriptor
                if (obj.is_null(columns[j])) {
                    is_null[index_in_view] = true;
                    break;
                }
                translated_key = obj.get<ObjKey>(columns[j]);
            }
            translated_keys[index_in_view] = translated_key;
        }
    }
}

BaseDescriptor::Sorter DistinctDescriptor::sorter(Table const& table, const IndexPairs& indexes) const
{
    REALM_ASSERT(!m_column_keys.empty());
    std::vector<bool> ascending(m_column_keys.size(), true);
    return Sorter(m_column_keys, ascending, table, indexes);
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

void IncludeDescriptor::execute(IndexPairs&, const Sorter&, const BaseDescriptor*) const
{
    // does nothing.
}

BaseDescriptor::Sorter SortDescriptor::sorter(Table const& table, const IndexPairs& indexes) const
{
    REALM_ASSERT(!m_column_keys.empty());
    return Sorter(m_column_keys, m_ascending, table, indexes);
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

IncludeDescriptor::IncludeDescriptor(const Table& table, const std::vector<std::vector<LinkPathPart>>& column_links)
    : ColumnsDescriptor()
{
    m_column_keys.resize(column_links.size());
    m_backlink_sources.resize(column_links.size());
    using tf = _impl::TableFriend;
    Group* group(tf::get_parent_group(table));
    for (size_t i = 0; i < m_column_keys.size(); ++i) { // for each path:
        auto& columns = m_column_keys[i];
        auto& links = column_links[i];
        auto& backlink_source = m_backlink_sources[i];
        REALM_ASSERT(!column_links.empty());

        columns.reserve(links.size());
        backlink_source.reserve(links.size());
        const Table* cur_table = &table;
        size_t link_ndx = 0;
        for (auto link : links) {  // follow path, one link at a time:
            if (bool(link.from)) { // backlink
                // must point back to cur_table:
                Table* from_table = group->get_table(link.from);
                REALM_ASSERT(cur_table == from_table->get_opposite_table(link.column_key));
                columns.push_back(link.column_key);
                backlink_source.push_back(link.from);
                auto type = DataType(link.column_key.get_type());
                if (type == type_Link || type == type_LinkList) {
                    // FIXME: How can this ever be true - ref assert above...
                    if (from_table->get_opposite_table_key(link.column_key) != cur_table->get_key()) {
                        // the link does not point to the last table in the chain
                        throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: this link does not "
                                                            "connect to the previous table ('%3').",
                                                            i, link_ndx, cur_table->get_name()));
                    }
                    cur_table = from_table;
                }
                else {
                    throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: a backlink was denoted "
                                                        "but this column ('%3') is not a link.",
                                                        i, link_ndx, cur_table->get_column_name(link.column_key)));
                }
            }
            else { // forward link/list
                columns.push_back(link.column_key);
                backlink_source.push_back(TableKey());
                auto type = DataType(link.column_key.get_type());
                if (type == type_Link || type == type_LinkList) {
                    if (columns.size() == links.size()) {
                        // An inclusion must end with a backlink column, link/list columns are included automatically
                        throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: the last part of an "
                                                            "included path must be a backlink column.",
                                                            i, link_ndx));
                    }
                    cur_table = cur_table->get_opposite_table(link.column_key);
                }
                else {
                    // An inclusion chain must consist entirely of link/list/backlink columns
                    throw InvalidPathError(util::format("Invalid INCLUDE path at [%1, %2]: all columns in the path "
                                                        "must be a link/list/backlink type but this column ('%3') "
                                                        "is a different type.",
                                                        i, link_ndx, cur_table->get_column_name(link.column_key)));
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
    using tf = _impl::TableFriend;
    Group* group(tf::get_parent_group(*attached_table));
    for (size_t i = 0; i < m_column_keys.size(); ++i) {
        auto& chain = m_column_keys[i];
        const size_t chain_size = chain.size();
        ConstTableRef cur_link_table = attached_table;
        for (size_t j = 0; j < chain_size; ++j) {
            if (j != 0) {
                description += realm::util::serializer::value_separator;
            }

            auto col_key = chain[j];
            if (auto from_table_key = m_backlink_sources[i][j]) { // backlink
                ConstTableRef from_table = group->get_table(from_table_key);
                REALM_ASSERT_DEBUG(from_table->valid_column(col_key));
                REALM_ASSERT_DEBUG(from_table->get_link_target(col_key) == cur_link_table);
                description += basic_serialiser.get_backlink_column_name(from_table, col_key);
                cur_link_table = from_table;
            }
            else {
                REALM_ASSERT_DEBUG(cur_link_table->valid_column(col_key));
                description += basic_serialiser.get_column_name(cur_link_table, col_key);
                if (j < chain_size - 1) {
                    cur_link_table = cur_link_table->get_link_target(col_key);
                }
            }
        }
        if (i < m_column_keys.size() - 1) {
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

void IncludeDescriptor::append(const IncludeDescriptor& other)
{
    REALM_ASSERT_DEBUG(other.m_backlink_sources.size() == other.m_column_keys.size());
    for (size_t i = 0; i < other.m_column_keys.size(); ++i) {
        this->m_column_keys.push_back(other.m_column_keys[i]);
        this->m_backlink_sources.push_back(other.m_backlink_sources[i]);
    }
}

void IncludeDescriptor::report_included_backlinks(
    const Table* origin, ObjKey obj,
    std::function<void(const Table*, const std::unordered_set<ObjKey>&)> reporter) const
{
    REALM_ASSERT_DEBUG(origin);
    REALM_ASSERT_DEBUG(obj);
    using tf = _impl::TableFriend;
    Group* group(tf::get_parent_group(*origin));

    for (size_t i = 0; i < m_column_keys.size(); ++i) {
        const Table* table = origin;
        std::unordered_set<ObjKey> objkeys_to_explore;
        objkeys_to_explore.insert(obj);



        for (size_t j = 0; j < m_column_keys[i].size(); ++j) {
            std::unordered_set<ObjKey> results_of_next_table;
            if (bool(m_backlink_sources[i][j])) { // backlink - collect objects linking into "objs_to_explore"
                // get table and column holding fwd links:
                const Table& from_table = *group->get_table(m_backlink_sources[i][j]);
                ColKey from_col = m_column_keys[i][j];
                for (auto objkey_to_explore : objkeys_to_explore) {
                    // collect backlinks for this object
                    auto target_obj = table->get_object(objkey_to_explore);
                    size_t num_backlinks = target_obj.get_backlink_count(from_table, from_col);
                    for (size_t backlink_ndx = 0; backlink_ndx < num_backlinks; ++backlink_ndx) {
                        results_of_next_table.insert(target_obj.get_backlink(from_table, from_col, backlink_ndx));
                    }
                }
                reporter(&from_table, results_of_next_table); // only report backlinks
                table = &from_table;
            }
            else {
                ColKey col_key = m_column_keys[i][j];
                DataType col_type = DataType(col_key.get_type());
                if (col_type == type_Link) {
                    for (auto objkey_to_explore : objkeys_to_explore) {
                        auto src_obj = table->get_object(objkey_to_explore);
                        ObjKey link_translation = src_obj.get<ObjKey>(col_key);
                        if (link_translation) { // null links terminate a chain
                            results_of_next_table.insert(link_translation);
                        }
                    }
                }
                else if (col_type == type_LinkList) {
                    for (auto objkey_to_explore : objkeys_to_explore) {
                        auto src_obj = table->get_object(objkey_to_explore);
                        auto links = src_obj.get_linklist(col_key);
                        const size_t num_links = links.size();
                        for (size_t link_ndx = 0; link_ndx < num_links; ++link_ndx) {
                            results_of_next_table.insert(links[link_ndx].get_key());
                        }
                    }
                }
                else {
                    // unexpected column type, type checking already happened
                    // in the IncludeDescriptor constructor so this should never happen
                    REALM_UNREACHABLE();
                }
                ConstTableRef linked_table = table->get_link_target(col_key);
                table = linked_table;
            }
            objkeys_to_explore = results_of_next_table;
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

void DescriptorOrdering::append_include(IncludeDescriptor include)
{
    if (include.is_valid()) {
        m_descriptors.emplace_back(new IncludeDescriptor(std::move(include)));
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
    for (auto it = m_descriptors.begin(); it != m_descriptors.end(); it++) {
        if ((*it)->get_type() == DescriptorType::Limit) {
            const LimitDescriptor* limit = static_cast<const LimitDescriptor*>(it->get());
            REALM_ASSERT(limit);
            min_limit = bool(min_limit) ? std::min(*min_limit, limit->get_limit()) : limit->get_limit();
        }
    }
    return min_limit;
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
