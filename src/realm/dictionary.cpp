/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#include <realm/dictionary.hpp>
#include <realm/dictionary_cluster_tree.hpp>
#include <realm/array_mixed.hpp>
#include <realm/group.hpp>
#include <realm/replication.hpp>
#include <algorithm>

namespace realm {

// Dummy cluster to be used if dictionary has no cluster created and an iterator is requested
static DictionaryClusterTree dummy_cluster(nullptr, type_Int, Allocator::get_default(), 0);

/************************** DictionaryClusterTree ****************************/

DictionaryClusterTree::DictionaryClusterTree(ArrayParent* owner, DataType key_type, Allocator& alloc, size_t ndx)
    : ClusterTree(alloc)
    , m_owner(owner)
    , m_ndx_in_cluster(ndx)
    , m_keys_col(ColKey(ColKey::Idx{0}, ColumnType(key_type), ColumnAttrMask(), 0))
{
}

DictionaryClusterTree::~DictionaryClusterTree() {}

/******************************** Dictionary *********************************/

Dictionary::Dictionary(const Obj& obj, ColKey col_key)
    : Base(obj, col_key)
    , m_key_type(m_obj.get_table()->get_dictionary_key_type(m_col_key))
{
    if (!col_key.is_dictionary()) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
    init_from_parent();
}

Dictionary::~Dictionary()
{
    delete m_clusters;
}

Dictionary& Dictionary::operator=(const Dictionary& other)
{
    Base::operator=(static_cast<const Base&>(other));

    if (this != &other) {
        init_from_parent();
    }
    return *this;
}
size_t Dictionary::size() const
{
    if (!is_attached())
        return 0;
    update_if_needed();
    if (!m_clusters)
        return 0;

    return m_clusters->size();
}

DataType Dictionary::get_key_data_type() const
{
    return m_key_type;
}

DataType Dictionary::get_value_data_type() const
{
    return DataType(m_col_key.get_type());
}

bool Dictionary::is_null(size_t ndx) const
{
    return get_any(ndx).is_null();
}

Mixed Dictionary::get_any(size_t ndx) const
{
    if (ndx >= size()) {
        throw std::out_of_range("ndx out of range");
    }
    ObjKey k;
    return do_get(m_clusters->get(ndx, k));
}

size_t Dictionary::find_any(Mixed value) const
{
    size_t ret = realm::not_found;
    ArrayMixed leaf(m_obj.get_alloc());
    size_t start_ndx = 0;

    m_clusters->traverse([&](const Cluster* cluster) {
        size_t e = cluster->node_size();
        cluster->init_leaf(DictionaryClusterTree::s_values_col, &leaf);
        for (size_t i = 0; i < e; i++) {
            if (leaf.get(i) == value) {
                ret = start_ndx + i;
                return true;
            }
        }
        start_ndx += e;
        // Continue
        return false;
    });

    return ret;
}

Mixed Dictionary::min(size_t* return_ndx) const
{
    Mixed m;
    size_t ndx = realm::npos;
    size_t i = realm::npos;

    for (auto&& elem : *this) {
        i++;
        if (elem.second.is_null())
            continue;

        if (m.is_null() || elem.second < m) {
            m = elem.second;
            ndx = i;
        }
    }

    if (return_ndx)
        *return_ndx = ndx;

    return m;
}

Mixed Dictionary::max(size_t* return_ndx) const
{
    Mixed m;
    size_t ndx = realm::npos;
    size_t i = realm::npos;

    for (auto&& elem : *this) {
        i++;
        if (elem.second.is_null())
            continue;

        if (m.is_null() || elem.second > m) {
            m = elem.second;
            ndx = i;
        }
    }

    if (return_ndx)
        *return_ndx = ndx;

    return m;
}

Mixed Dictionary::sum(size_t* return_cnt) const
{
    Mixed s;
    size_t cnt = 0;
    for (auto&& elem : *this) {
        if (elem.second.is_null())
            continue;

        cnt++;

        if (s.is_null()) {
            s = elem.second;
        }
        else {
            if (s.get_type() != elem.second.get_type()) {
                throw std::runtime_error("Sum not supported");
            }
            switch (s.get_type()) {
                case type_Int:
                    s = Mixed(s.get_int() + elem.second.get_int());
                    break;
                case type_Float:
                    s = Mixed(s.get_float() + elem.second.get_float());
                    break;
                case type_Double:
                    s = Mixed(s.get_double() + elem.second.get_double());
                    break;
                case type_Decimal:
                    s = Mixed(s.get<Decimal128>() + elem.second.get<Decimal128>());
                    break;
                default:
                    throw std::runtime_error("Average not supported");
                    break;
            }
        }
    }

    if (return_cnt)
        *return_cnt = cnt;

    return s;
}

Mixed Dictionary::avg(size_t* return_cnt) const
{
    auto s = sum(return_cnt);
    if (*return_cnt && !s.is_null()) {
        switch (s.get_type()) {
            case type_Int:
                return Mixed(double(s.get_int()) / *return_cnt);
            case type_Float:
                return Mixed(double(s.get_float()) / *return_cnt);
            case type_Double:
                return Mixed(s.get_double() / *return_cnt);
            case type_Decimal:
                return Mixed(s.get<Decimal128>() / *return_cnt);
            default:
                throw std::runtime_error("Average not supported");
                break;
        }
    }
    return {};
}

void Dictionary::sort(std::vector<size_t>& indices, bool ascending) const
{
    auto sz = size();
    auto sz2 = indices.size();

    indices.reserve(sz);
    if (sz < sz2) {
        // If list size has decreased, we have to start all over
        indices.clear();
        sz2 = 0;
    }
    for (size_t i = sz2; i < sz; i++) {
        // If list size has increased, just add the missing indices
        indices.push_back(i);
    }
    auto b = indices.begin();
    auto e = indices.end();
    if (ascending) {
        std::sort(b, e, [this](size_t i1, size_t i2) {
            return get_any(i1) < get_any(i2);
        });
    }
    else {
        std::sort(b, e, [this](size_t i1, size_t i2) {
            return get_any(i1) > get_any(i2);
        });
    }
}
void Dictionary::distinct(std::vector<size_t>&, util::Optional<bool>) const {}

Mixed Dictionary::get(Mixed key) const
{
    update_if_needed();
    if (m_clusters) {
        auto hash = key.hash();
        ObjKey k(int64_t(hash & 0x7FFFFFFFFFFFFFFF));
        return do_get(m_clusters->get(k));
    }
    throw realm::KeyNotFound("Dictionary::get");
    return {};
}

util::Optional<Mixed> Dictionary::try_get(Mixed key) const noexcept
{
    update_if_needed();
    if (m_clusters) {
        auto hash = key.hash();
        ObjKey k(int64_t(hash & 0x7FFFFFFFFFFFFFFF));
        auto state = m_clusters->try_get(k);
        if (state.index != realm::npos)
            return do_get(m_clusters->get(k));
    }
    return {};
}

Dictionary::Iterator Dictionary::begin() const
{
    return Iterator(this, 0);
}

Dictionary::Iterator Dictionary::end() const
{
    return Iterator(this, size());
}

void Dictionary::create()
{
    if (!m_clusters && m_obj.is_valid()) {
        MemRef mem = Cluster::create_empty_cluster(m_obj.get_alloc());
        update_child_ref(0, mem.get_ref());
        m_clusters = new DictionaryClusterTree(this, m_key_type, m_obj.get_alloc(), m_obj.get_row_ndx());
        m_clusters->init_from_parent();
        m_clusters->add_columns();
    }
}

std::pair<Dictionary::Iterator, bool> Dictionary::insert(Mixed key, Mixed value)
{
    if (m_key_type != type_Mixed && key.get_type() != m_key_type) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
    if (m_col_key.get_type() == col_type_Link && value.get_type() == type_TypedLink) {
        if (m_obj.get_table()->get_opposite_table_key(m_col_key) != value.get<ObjLink>().get_table_key()) {
            throw std::runtime_error("Dictionary::insert: Wrong object type");
        }
    }
    else if (m_col_key.get_type() != col_type_Mixed && value.get_type() != DataType(m_col_key.get_type())) {
        throw LogicError(LogicError::collection_type_mismatch);
    }

    update_if_needed();

    ObjLink new_link;
    if (value.get_type() == type_TypedLink) {
        new_link = value.get<ObjLink>();
        m_obj.get_table()->get_parent_group()->validate(new_link);
    }
    else if (value.get_type() == type_Link) {
        auto target_table = m_obj.get_table()->get_opposite_table(m_col_key);
        auto key = value.get<ObjKey>();
        if (!target_table->is_valid(key)) {
            throw LogicError(LogicError::target_row_index_out_of_range);
        }

        new_link = ObjLink(target_table->get_key(), key);
        value = Mixed(new_link);
    }

    create();
    auto hash = key.hash();
    ObjKey k(int64_t(hash & 0x7FFFFFFFFFFFFFFF));

    if (Replication* repl = this->m_obj.get_replication()) {
        repl->dictionary_insert(*this, key, value);
    }

    bump_content_version();
    try {
        ClusterNode::State state = m_clusters->insert(k, key, value);

        if (new_link) {
            CascadeState cascade_state;
            m_obj.replace_backlink(m_col_key, ObjLink(), new_link, cascade_state);
        }
        return {Iterator(this, state.index), true};
    }
    catch (const KeyAlreadyUsed&) {
        auto state = m_clusters->get(k);
        Array fallback(m_obj.get_alloc());
        Array& fields = m_clusters->get_fields_accessor(fallback, state.mem);
        ArrayMixed values(m_obj.get_alloc());
        values.set_parent(&fields, 2);
        values.init_from_parent();

        Mixed old_value = values.get(state.index);
        ObjLink old_link;
        if (!old_value.is_null() && old_value.get_type() == type_TypedLink) {
            old_link = old_value.get<ObjLink>();
        }

        if (new_link != old_link) {
            CascadeState cascade_state;
            m_obj.replace_backlink(m_col_key, old_link, new_link, cascade_state);
        }

        values.set(state.index, value);
        return {Iterator(this, state.index), false};
    }
}

const Mixed Dictionary::operator[](Mixed key)
{
    Mixed ret;

    try {
        ret = get(key);
    }
    catch (const KeyNotFound&) {
        create();
        auto hash = key.hash();
        ObjKey k(int64_t(hash & 0x7FFFFFFFFFFFFFFF));
        bump_content_version();
        m_clusters->insert(k, key, Mixed{});
    }

    return ret;
}

Dictionary::Iterator Dictionary::find(Mixed key)
{
    if (m_clusters) {
        auto hash = key.hash();
        ObjKey k(int64_t(hash & 0x7FFFFFFFFFFFFFFF));
        try {
            return Iterator(this, m_clusters->get_ndx(k));
        }
        catch (...) {
        }
    }
    return end();
}

void Dictionary::erase(Mixed key)
{
    update_if_needed();

    if (m_clusters) {
        auto hash = key.hash();
        ObjKey k(int64_t(hash & 0x7FFFFFFFFFFFFFFF));
        CascadeState state;
        if (Replication* repl = this->m_obj.get_replication()) {
            repl->dictionary_erase(*this, key);
        }
        m_clusters->erase(k, state);
        bump_content_version();
    }
}

void Dictionary::erase(Iterator it)
{
    erase((*it).first);
}

void Dictionary::nullify(Mixed key)
{
    auto hash = key.hash();
    ObjKey k(int64_t(hash & 0x7FFFFFFFFFFFFFFF));

    bump_content_version();

    auto state = m_clusters->get(k);
    ArrayMixed values(m_obj.get_alloc());
    ref_type ref = to_ref(Array::get(state.mem.get_addr(), 2));
    values.init_from_ref(ref);
    values.set(state.index, Mixed());
}

void Dictionary::clear()
{
    if (size() > 0) {
        // TODO: Should we have a "dictionary_clear" instruction?
        if (Replication* repl = this->m_obj.get_replication()) {
            for (auto&& elem : *this) {
                repl->dictionary_erase(*this, elem.first);
            }
        }
        // Just destroy the whole cluster
        m_clusters->destroy();
        delete m_clusters;
        m_clusters = nullptr;

        update_child_ref(0, 0);
    }
}

bool Dictionary::init_from_parent() const
{
    bool valid = false;
    auto ref = to_ref(m_obj._get<int64_t>(m_col_key.get_index()));

    if (ref) {
        if (!m_clusters)
            m_clusters = new DictionaryClusterTree(const_cast<Dictionary*>(this), m_key_type, m_obj.get_alloc(),
                                                   m_obj.get_row_ndx());

        m_clusters->init_from_parent();
        valid = true;
    }
    else {
        delete m_clusters;
        m_clusters = nullptr;
    }

    update_content_version();

    return valid;
}

Mixed Dictionary::do_get(ClusterNode::State&& s) const
{
    ArrayMixed values(m_obj.get_alloc());
    ref_type ref = to_ref(Array::get(s.mem.get_addr(), 2));
    values.init_from_ref(ref);
    Mixed val = values.get(s.index);

    // Filter out potential unresolved links
    if (!val.is_null() && val.get_type() == type_TypedLink) {
        auto link = val.get<ObjLink>();
        auto key = link.get_obj_key();
        if (key.is_unresolved()) {
            return {};
        }
        if (m_col_key.get_type() == col_type_Link) {
            return key;
        }
    }
    return val;
}

/************************* Dictionary::Iterator *************************/

CollectionIterator<Dictionary>::CollectionIterator(const Dictionary* dict, size_t pos)
    : ClusterTree::Iterator(dict->m_clusters ? *dict->m_clusters : dummy_cluster, pos)
    , m_key_type(dict->get_key_data_type())
{
}

auto Dictionary::Iterator::operator*() const -> value_type
{
    update();
    Mixed key;
    switch (m_key_type) {
        case type_String: {
            ArrayString keys(m_tree.get_alloc());
            ref_type ref = to_ref(Array::get(m_leaf.get_mem().get_addr(), 1));
            keys.init_from_ref(ref);
            key = Mixed(keys.get(m_state.m_current_index));
            break;
        }
        case type_Int: {
            ArrayInteger keys(m_tree.get_alloc());
            ref_type ref = to_ref(Array::get(m_leaf.get_mem().get_addr(), 1));
            keys.init_from_ref(ref);
            key = Mixed(keys.get(m_state.m_current_index));
            break;
        }
        default:
            throw std::runtime_error("Not implemented");
            break;
    }

    ArrayMixed values(m_tree.get_alloc());
    ref_type ref = to_ref(Array::get(m_leaf.get_mem().get_addr(), 2));
    values.init_from_ref(ref);

    return std::make_pair(key, values.get(m_state.m_current_index));
}

} // namespace realm
