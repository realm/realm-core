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

#include <realm/query_expression.hpp>
#include <realm/group.hpp>
#include <realm/dictionary.hpp>

namespace realm {

void LinkMap::set_base_table(ConstTableRef table)
{
    if (table == get_base_table())
        return;

    m_tables.clear();
    m_tables.push_back(table);
    m_link_types.clear();
    m_only_unary_links = true;

    for (size_t i = 0; i < m_link_column_keys.size(); i++) {
        ColKey link_column_key = m_link_column_keys[i];
        // Link column can be either LinkList or single Link
        ColumnType type = link_column_key.get_type();
        REALM_ASSERT(Table::is_link_type(type) || type == col_type_BackLink);
        if (type == col_type_LinkList || type == col_type_BackLink ||
            (type == col_type_Link && link_column_key.is_collection())) {
            m_only_unary_links = false;
        }

        m_link_types.push_back(type);
        REALM_ASSERT(table->valid_column(link_column_key));
        table = table.unchecked_ptr()->get_opposite_table(link_column_key);
        m_tables.push_back(table);
    }
}

void LinkMap::collect_dependencies(std::vector<TableKey>& tables) const
{
    for (auto& t : m_tables) {
        TableKey k = t->get_key();
        if (find(tables.begin(), tables.end(), k) == tables.end()) {
            tables.push_back(k);
        }
    }
}

std::string LinkMap::description(util::serializer::SerialisationState& state) const
{
    std::string s;
    for (size_t i = 0; i < m_link_column_keys.size(); ++i) {
        if (i < m_tables.size() && m_tables[i]) {
            s += state.get_column_name(m_tables[i], m_link_column_keys[i]);
            if (i != m_link_column_keys.size() - 1) {
                s += util::serializer::value_separator;
            }
        }
    }
    return s;
}

void LinkMap::map_links(size_t column, ObjKey key, LinkMapFunction& lm) const
{
    bool last = (column + 1 == m_link_column_keys.size());
    ColumnType type = m_link_types[column];
    ColKey column_key = m_link_column_keys[column];
    const Obj obj = m_tables[column]->get_object(key);
    if (column_key.is_collection()) {
        auto coll = obj.get_linkcollection_ptr(column_key);
        size_t sz = coll->size();
        for (size_t t = 0; t < sz; t++) {
            if (ObjKey k = coll->get_key(t)) {
                // Unresolved links are filtered out
                if (last) {
                    lm.consume(k);
                }
                else
                    map_links(column + 1, k, lm);
            }
        }
    }
    else if (type == col_type_Link) {
        if (ObjKey k = obj.get<ObjKey>(column_key)) {
            if (!k.is_unresolved()) {
                if (last)
                    lm.consume(k);
                else
                    map_links(column + 1, k, lm);
            }
        }
    }
    else if (type == col_type_BackLink) {
        auto backlinks = obj.get_all_backlinks(column_key);
        for (auto k : backlinks) {
            if (last) {
                lm.consume(k);
            }
            else
                map_links(column + 1, k, lm);
        }
    }
    else {
        REALM_ASSERT(false);
    }
}

void LinkMap::map_links(size_t column, size_t row, LinkMapFunction& lm) const
{
    REALM_ASSERT(m_leaf_ptr != nullptr);

    bool last = (column + 1 == m_link_column_keys.size());
    ColumnType type = m_link_types[column];
    ColKey column_key = m_link_column_keys[column];
    if (type == col_type_Link && !column_key.is_set()) {
        if (column_key.is_dictionary()) {
            auto leaf = static_cast<const ArrayInteger*>(m_leaf_ptr);
            if (leaf->get(row)) {
                auto key_type = m_tables[column]->get_dictionary_key_type(column_key);
                DictionaryClusterTree dict_cluster(const_cast<ArrayInteger*>(leaf), key_type,
                                                   get_base_table()->get_alloc(), row);
                dict_cluster.init_from_parent();

                // Iterate through cluster and insert all link values
                ArrayMixed leaf(get_base_table()->get_alloc());
                dict_cluster.traverse([&](const Cluster* cluster) {
                    size_t e = cluster->node_size();
                    cluster->init_leaf(DictionaryClusterTree::s_values_col, &leaf);
                    for (size_t i = 0; i < e; i++) {
                        auto m = leaf.get(i);
                        if (m.is_type(type_TypedLink)) {
                            auto link = m.get_link();
                            REALM_ASSERT(link.get_table_key() == this->m_tables[column + 1]->get_key());
                            auto k = link.get_obj_key();
                            if (!k.is_unresolved()) {
                                if (last)
                                    lm.consume(k);
                                else
                                    map_links(column + 1, k, lm);
                            }
                        }
                    }
                    // Continue
                    return false;
                });
            }
        }
        else {
            REALM_ASSERT(!column_key.is_collection());
            if (ObjKey k = static_cast<const ArrayKey*>(m_leaf_ptr)->get(row)) {
                if (!k.is_unresolved()) {
                    if (last)
                        lm.consume(k);
                    else
                        map_links(column + 1, k, lm);
                }
            }
        }
    }
    // Note: Link lists and link sets have compatible storage.
    else if (type == col_type_LinkList || (type == col_type_Link && column_key.is_set())) {
        if (ref_type ref = static_cast<const ArrayList*>(m_leaf_ptr)->get(row)) {
            BPlusTree<ObjKey> links(get_base_table()->get_alloc());
            links.init_from_ref(ref);
            size_t sz = links.size();
            for (size_t t = 0; t < sz; t++) {
                ObjKey k = links.get(t);
                if (!k.is_unresolved()) {
                    if (last) {
                        if (!lm.consume(k)) {
                            return;
                        }
                    }
                    else
                        map_links(column + 1, k, lm);
                }
            }
        }
    }
    else if (type == col_type_BackLink) {
        auto back_links = static_cast<const ArrayBacklink*>(m_leaf_ptr);
        size_t sz = back_links->get_backlink_count(row);
        for (size_t t = 0; t < sz; t++) {
            ObjKey k = back_links->get_backlink(row, t);
            if (last) {
                bool continue2 = lm.consume(k);
                if (!continue2)
                    return;
            }
            else
                map_links(column + 1, k, lm);
        }
    }
    else {
        REALM_ASSERT(false);
    }
}

std::vector<ObjKey> LinkMap::get_origin_ndxs(ObjKey key, size_t column) const
{
    if (column == m_link_types.size()) {
        return {key};
    }
    std::vector<ObjKey> keys = get_origin_ndxs(key, column + 1);
    std::vector<ObjKey> ret;
    auto origin_col = m_link_column_keys[column];
    auto origin = m_tables[column];
    auto link_type = m_link_types[column];
    if (link_type == col_type_BackLink) {
        auto link_table = origin->get_opposite_table(origin_col);
        ColKey link_col_key = origin->get_opposite_column(origin_col);

        for (auto k : keys) {
            const Obj o = link_table.unchecked_ptr()->get_object(k);
            if (link_col_key.is_collection()) {
                auto coll = o.get_linkcollection_ptr(link_col_key);
                auto sz = coll->size();
                for (size_t i = 0; i < sz; i++) {
                    if (ObjKey x = coll->get_key(i))
                        ret.push_back(x);
                }
            }
            else if (link_col_key.get_type() == col_type_Link) {
                ret.push_back(o.get<ObjKey>(link_col_key));
            }
        }
    }
    else {
        auto target = m_tables[column + 1];
        for (auto k : keys) {
            const Obj o = target->get_object(k);
            auto cnt = o.get_backlink_count(*origin, origin_col);
            for (size_t i = 0; i < cnt; i++) {
                ret.push_back(o.get_backlink(*origin, origin_col, i));
            }
        }
    }
    return ret;
}

ColumnDictionaryKey Columns<Dictionary>::key(const Mixed& key_value)
{
    if (m_key_type != type_Mixed && key_value.get_type() != m_key_type) {
        throw LogicError(LogicError::collection_type_mismatch);
    }

    return ColumnDictionaryKey(key_value, *this);
}

ColumnDictionaryKeys Columns<Dictionary>::keys()
{
    return ColumnDictionaryKeys(*this);
}

void ColumnDictionaryKey::init_key(Mixed key_value)
{
    REALM_ASSERT(!key_value.is_null());

    m_key = key_value;
    if (!key_value.is_null()) {
        if (m_key.get_type() == type_String) {
            m_buffer = std::string(m_key.get_string());
            m_key = Mixed(m_buffer);
        }
        m_objkey = Dictionary::get_internal_obj_key(m_key);
    }
}

void ColumnDictionaryKeys::set_cluster(const Cluster* cluster)
{
    m_leaf_ptr = nullptr;
    m_array_ptr = nullptr;
    if (m_link_map.has_links()) {
        m_link_map.set_cluster(cluster);
    }
    else {
        // Create new Leaf
        m_array_ptr = LeafPtr(new (&m_leaf_cache_storage) ArrayInteger(m_link_map.get_base_table()->get_alloc()));
        cluster->init_leaf(m_column_key, m_array_ptr.get());
        m_leaf_ptr = m_array_ptr.get();
    }
}


void ColumnDictionaryKeys::evaluate(size_t index, ValueBase& destination)
{
    if (m_link_map.has_links()) {
        REALM_ASSERT(m_leaf_ptr == nullptr);
        std::vector<ObjKey> links = m_link_map.get_links(index);
        auto sz = links.size();

        // Here we don't really know how many values to expect
        std::vector<Mixed> values;
        for (size_t t = 0; t < sz; t++) {
            const Obj obj = m_link_map.get_target_table()->get_object(links[t]);
            auto dict = obj.get_dictionary(m_column_key);
            // Insert all values
            dict.for_all_keys<StringData>([&values](const Mixed& value) {
                values.emplace_back(value);
            });
        }

        // Copy values over
        destination.init(true, values.size());
        destination.set(values.begin(), values.end());
    }
    else {
        // Not a link column
        Allocator& alloc = get_base_table()->get_alloc();

        REALM_ASSERT(m_leaf_ptr != nullptr);
        if (m_leaf_ptr->get(index)) {
            DictionaryClusterTree dict_cluster(static_cast<Array*>(m_leaf_ptr), m_key_type, alloc, index);
            dict_cluster.init_from_parent();
            auto col = dict_cluster.get_keys_column_key();

            destination.init(true, dict_cluster.size());
            ArrayString leaf(alloc);
            size_t n = 0;
            // Iterate through cluster and insert all keys
            dict_cluster.traverse([&leaf, &destination, &n, col](const Cluster* cluster) {
                size_t e = cluster->node_size();
                cluster->init_leaf(col, &leaf);
                for (size_t i = 0; i < e; i++) {
                    destination.set(n, leaf.get(i));
                    n++;
                }
                // Continue
                return false;
            });
        }
    }
}

void ColumnDictionaryKey::evaluate(size_t index, ValueBase& destination)
{
    if (links_exist()) {
        REALM_ASSERT(m_leaf_ptr == nullptr);
        std::vector<ObjKey> links = m_link_map.get_links(index);
        auto sz = links.size();

        destination.init_for_links(m_link_map.only_unary_links(), sz);
        for (size_t t = 0; t < sz; t++) {
            const Obj obj = m_link_map.get_target_table()->get_object(links[t]);
            auto dict = obj.get_dictionary(m_column_key);
            Mixed val;
            if (auto opt_val = dict.try_get(m_key)) {
                val = *opt_val;
                if (m_prop_list.size()) {
                    if (val.is_type(type_TypedLink)) {
                        auto obj = get_base_table()->get_parent_group()->get_object(val.get<ObjLink>());
                        val = obj.get_any(m_prop_list.begin(), m_prop_list.end());
                    }
                    else {
                        val = {};
                    }
                }
            }
            destination.set(t, val);
        }
    }
    else {
        // Not a link column
        Allocator& alloc = get_base_table()->get_alloc();

        REALM_ASSERT(m_leaf_ptr != nullptr);
        if (m_leaf_ptr->get(index)) {
            DictionaryClusterTree dict_cluster(static_cast<Array*>(m_leaf_ptr), m_key_type, alloc, index);
            dict_cluster.init_from_parent();

            Mixed val;
            auto state = dict_cluster.try_get_with_key(m_objkey, m_key);
            if (state.index != realm::npos) {
                ArrayMixed values(alloc);
                ref_type ref = to_ref(Array::get(state.mem.get_addr(), 2));
                values.init_from_ref(ref);
                val = values.get(state.index);
                if (m_prop_list.size()) {
                    if (val.is_type(type_TypedLink)) {
                        auto obj = get_base_table()->get_parent_group()->get_object(val.get<ObjLink>());
                        val = obj.get_any(m_prop_list.begin(), m_prop_list.end());
                    }
                    else {
                        val = {};
                    }
                }
            }
            destination.set(0, val);
        }
    }
}

class DictionarySize : public Columns<Dictionary> {
public:
    DictionarySize(const Columns<Dictionary>& other)
        : Columns<Dictionary>(other)
    {
    }
    void evaluate(size_t index, ValueBase& destination) override
    {
        Allocator& alloc = this->m_link_map.get_target_table()->get_alloc();
        Value<int64_t> list_refs;
        this->get_lists(index, list_refs, 1);
        destination.init(list_refs.m_from_link_list, list_refs.size());
        for (size_t i = 0; i < list_refs.size(); i++) {
            ref_type ref = to_ref(list_refs[i].get_int());
            size_t s = ClusterTree::size_from_ref(ref, alloc);
            destination.set(i, int64_t(s));
        }
    }

    std::unique_ptr<Subexpr> clone() const override
    {
        return std::unique_ptr<Subexpr>(new DictionarySize(*this));
    }
};

SizeOperator<int64_t> Columns<Dictionary>::size()
{
    std::unique_ptr<Subexpr> ptr(new DictionarySize(*this));
    return SizeOperator<int64_t>(std::move(ptr));
}

void Columns<Dictionary>::evaluate(size_t index, ValueBase& destination)
{
    if (links_exist()) {
        REALM_ASSERT(m_leaf_ptr == nullptr);
        std::vector<ObjKey> links = m_link_map.get_links(index);
        auto sz = links.size();

        // Here we don't really know how many values to expect
        std::vector<Mixed> values;
        for (size_t t = 0; t < sz; t++) {
            const Obj obj = m_link_map.get_target_table()->get_object(links[t]);
            auto dict = obj.get_dictionary(m_column_key);
            // Insert all values
            dict.for_all_values([&values](const Mixed& value) {
                values.emplace_back(value);
            });
        }

        // Copy values over
        destination.init(true, values.size());
        destination.set(values.begin(), values.end());
    }
    else {
        // Not a link column
        Allocator& alloc = get_base_table()->get_alloc();

        REALM_ASSERT(m_leaf_ptr != nullptr);
        if (m_leaf_ptr->get(index)) {
            DictionaryClusterTree dict_cluster(static_cast<Array*>(m_leaf_ptr), m_key_type, alloc, index);
            dict_cluster.init_from_parent();

            destination.init(true, dict_cluster.size());
            ArrayMixed leaf(alloc);
            size_t n = 0;
            // Iterate through cluster and insert all values
            dict_cluster.traverse([&leaf, &destination, &n](const Cluster* cluster) {
                size_t e = cluster->node_size();
                cluster->init_leaf(DictionaryClusterTree::s_values_col, &leaf);
                for (size_t i = 0; i < e; i++) {
                    destination.set(n, leaf.get(i));
                    n++;
                }
                // Continue
                return false;
            });
        }
    }
}


void Columns<Link>::evaluate(size_t index, ValueBase& destination)
{
    // Destination must be of Key type. It only makes sense to
    // compare keys with keys
    std::vector<ObjKey> links = m_link_map.get_links(index);

    if (m_link_map.only_unary_links()) {
        ObjKey key;
        if (!links.empty()) {
            key = links[0];
        }
        destination.init(false, 1);
        destination.set(0, key);
    }
    else {
        destination.init(true, links.size());
        destination.set(links.begin(), links.end());
    }
}

void ColumnListBase::set_cluster(const Cluster* cluster)
{
    m_leaf_ptr = nullptr;
    m_array_ptr = nullptr;
    if (m_link_map.has_links()) {
        m_link_map.set_cluster(cluster);
    }
    else {
        // Create new Leaf
        m_array_ptr = LeafPtr(new (&m_leaf_cache_storage) ArrayInteger(m_link_map.get_base_table()->get_alloc()));
        cluster->init_leaf(m_column_key, m_array_ptr.get());
        m_leaf_ptr = m_array_ptr.get();
    }
}

void ColumnListBase::get_lists(size_t index, Value<int64_t>& destination, size_t nb_elements)
{
    if (m_link_map.has_links()) {
        std::vector<ObjKey> links = m_link_map.get_links(index);
        auto sz = links.size();

        if (m_link_map.only_unary_links()) {
            int64_t val = 0;
            if (sz == 1) {
                const Obj obj = m_link_map.get_target_table()->get_object(links[0]);
                val = obj._get<int64_t>(m_column_key.get_index());
            }
            destination.init(false, 1);
            destination.set(0, val);
        }
        else {
            destination.init(true, sz);
            for (size_t t = 0; t < sz; t++) {
                const Obj obj = m_link_map.get_target_table()->get_object(links[t]);
                int64_t val = obj._get<int64_t>(m_column_key.get_index());
                destination.set(t, val);
            }
        }
    }
    else {
        size_t rows = std::min(m_leaf_ptr->size() - index, nb_elements);

        destination.init(false, rows);

        for (size_t t = 0; t < rows; t++) {
            destination.set(t, m_leaf_ptr->get(index + t));
        }
    }
}

Query Subexpr2<StringData>::equal(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, Equal, EqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::equal(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<Equal, EqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::not_equal(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, NotEqual, NotEqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::not_equal(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<NotEqual, NotEqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::begins_with(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, BeginsWith, BeginsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::begins_with(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<BeginsWith, BeginsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::ends_with(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, EndsWith, EndsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::ends_with(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<EndsWith, EndsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::contains(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, Contains, ContainsIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::contains(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<Contains, ContainsIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::like(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, Like, LikeIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::like(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<Like, LikeIns>(*this, col, case_sensitive);
}


// BinaryData

Query Subexpr2<BinaryData>::equal(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, Equal, EqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::equal(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<Equal, EqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::not_equal(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, NotEqual, NotEqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::not_equal(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<NotEqual, NotEqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::begins_with(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, BeginsWith, BeginsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::begins_with(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<BeginsWith, BeginsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::ends_with(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, EndsWith, EndsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::ends_with(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<EndsWith, EndsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::contains(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, Contains, ContainsIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::contains(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<Contains, ContainsIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::like(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, Like, LikeIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::like(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<Like, LikeIns>(*this, col, case_sensitive);
}

// Mixed

Query Subexpr2<Mixed>::equal(Mixed sd, bool case_sensitive)
{
    return mixed_compare<Mixed, Equal, EqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<Mixed>::equal(const Subexpr2<Mixed>& col, bool case_sensitive)
{
    return mixed_compare<Equal, EqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<Mixed>::not_equal(Mixed sd, bool case_sensitive)
{
    return mixed_compare<Mixed, NotEqual, NotEqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<Mixed>::not_equal(const Subexpr2<Mixed>& col, bool case_sensitive)
{
    return mixed_compare<NotEqual, NotEqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<Mixed>::begins_with(Mixed sd, bool case_sensitive)
{
    return mixed_compare<Mixed, BeginsWith, BeginsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<Mixed>::begins_with(const Subexpr2<Mixed>& col, bool case_sensitive)
{
    return mixed_compare<BeginsWith, BeginsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<Mixed>::ends_with(Mixed sd, bool case_sensitive)
{
    return mixed_compare<Mixed, EndsWith, EndsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<Mixed>::ends_with(const Subexpr2<Mixed>& col, bool case_sensitive)
{
    return mixed_compare<EndsWith, EndsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<Mixed>::contains(Mixed sd, bool case_sensitive)
{
    return mixed_compare<Mixed, Contains, ContainsIns>(*this, sd, case_sensitive);
}

Query Subexpr2<Mixed>::contains(const Subexpr2<Mixed>& col, bool case_sensitive)
{
    return mixed_compare<Contains, ContainsIns>(*this, col, case_sensitive);
}

Query Subexpr2<Mixed>::like(Mixed sd, bool case_sensitive)
{
    return mixed_compare<Mixed, Like, LikeIns>(*this, sd, case_sensitive);
}

Query Subexpr2<Mixed>::like(const Subexpr2<Mixed>& col, bool case_sensitive)
{
    return mixed_compare<Like, LikeIns>(*this, col, case_sensitive);
}

} // namespace realm
