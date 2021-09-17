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
#include <realm/aggregate_ops.hpp>
#include <realm/array_mixed.hpp>
#include <realm/array_ref.hpp>
#include <realm/group.hpp>
#include <realm/replication.hpp>

#include <algorithm>

namespace realm {

namespace {
void validate_key_value(const Mixed& key)
{
    if (key.is_type(type_String)) {
        auto str = key.get_string();
        if (str.size()) {
            if (str[0] == '$')
                throw std::runtime_error("Dictionary::insert: key must not start with '$'");
            if (memchr(str.data(), '.', str.size()))
                throw std::runtime_error("Dictionary::insert: key must not contain '.'");
        }
    }
}
} // namespace

// Dummy cluster to be used if dictionary has no cluster created and an iterator is requested
static DictionaryClusterTree dummy_cluster(nullptr, type_Int, Allocator::get_default(), 0);

#ifdef REALM_DEBUG
// This will leave the upper bit 0. Reserves space for alternative keys in case of collision
uint64_t Dictionary::s_hash_mask = 0x7FFFFFFFFFFFFFFFULL;
#endif

/************************** DictionaryClusterTree ****************************/

DictionaryClusterTree::DictionaryClusterTree(ArrayParent* owner, DataType key_type, Allocator& alloc, size_t ndx)
    : ClusterTree(alloc)
    , m_owner(owner)
    , m_ndx_in_cluster(ndx)
    , m_keys_col(ColKey(ColKey::Idx{0}, ColumnType(key_type), ColumnAttrMask(), 0))
{
}

DictionaryClusterTree::~DictionaryClusterTree() {}

bool DictionaryClusterTree::init_from_parent()
{
    if (ClusterTree::init_from_parent()) {
        m_has_collision_column = (nb_columns() == 3);
        return true;
    }
    return false;
}

Mixed DictionaryClusterTree::get_key(const ClusterNode::State& s) const
{
    Mixed key;
    switch (m_keys_col.get_type()) {
        case col_type_String: {
            ArrayString keys(m_alloc);
            ref_type ref = to_ref(Array::get(s.mem.get_addr(), 1));
            keys.init_from_ref(ref);
            key = Mixed(keys.get(s.index));
            break;
        }
        case col_type_Int: {
            ArrayInteger keys(m_alloc);
            ref_type ref = to_ref(Array::get(s.mem.get_addr(), 1));
            keys.init_from_ref(ref);
            key = Mixed(keys.get(s.index));
            break;
        }
        default:
            throw std::runtime_error("Dictionary keys can only be strings or integers");
            break;
    }
    return key;
}

// Called when a given state represent an entry with no matching key
// Check its potential siblings for a match. 'state' is only updated if match is found.
ObjKey DictionaryClusterTree::find_sibling(ClusterNode::State& state, Mixed key) const noexcept
{
    // Find alternatives
    Array fallback(m_alloc);
    Array& fields = get_fields_accessor(fallback, state.mem);
    ArrayRef refs(m_alloc);
    refs.set_parent(&fields, 3);
    refs.init_from_parent();

    if (auto ref = refs.get(state.index)) {
        Array links(m_alloc);
        links.set_parent(&refs, state.index);
        links.init_from_ref(ref);

        auto sz = links.size();
        for (size_t i = 0; i < sz; i++) {
            ObjKey sibling(links.get(i));
            auto state2 = ClusterTree::try_get(sibling);
            if (state2 && get_key(state2).compare_signed(key) == 0) {
                state = state2;
                return sibling;
            }
        }
    }

    return {};
}

ClusterNode::State DictionaryClusterTree::try_get_with_key(ObjKey k, Mixed key) const noexcept
{
    auto state = ClusterTree::try_get(k);
    if (state) {
        // Some entry found - Check if key matches.
        if (get_key(state).compare_signed(key) != 0) {
            // Key does not match - try looking for a sibling
            if (!(m_has_collision_column && find_sibling(state, key))) {
                // Not found
                state.index = realm::npos;
            }
        }
    }

    return state;
}

size_t DictionaryClusterTree::get_ndx_with_key(ObjKey k, Mixed key) const noexcept
{
    size_t pos = ClusterTree::get_ndx(k);
    if (pos == realm::npos) {
        return pos;
    }
    auto state = get(pos, k);
    if (get_key(state).compare_signed(key) == 0) {
        return pos;
    }
    if (m_has_collision_column) {
        // Find alternatives
        Array fallback(m_alloc);
        Array& fields = get_fields_accessor(fallback, state.mem);
        ArrayRef refs(m_alloc);
        refs.set_parent(&fields, 3);
        refs.init_from_parent();

        if (auto ref = refs.get(state.index)) {
            Array links(m_alloc);
            links.init_from_ref(ref);

            auto sz = links.size();
            for (size_t i = 0; i < sz; i++) {
                ObjKey sibling(links.get(i));
                state = ClusterTree::get(sibling);
                if (get_key(state).compare_signed(key) == 0) {
                    return ClusterTree::get_ndx(sibling);
                }
            }
        }
    }
    return realm::npos;
}


template <typename AggregateType>
void DictionaryClusterTree::do_accumulate(size_t* return_ndx, AggregateType& agg) const
{
    ArrayMixed leaf(m_alloc);
    size_t start_ndx = 0;
    size_t ndx = realm::npos;

    traverse([&](const Cluster* cluster) {
        size_t e = cluster->node_size();
        cluster->init_leaf(s_values_col, &leaf);
        for (size_t i = 0; i < e; i++) {
            auto val = leaf.get(i);
            if (agg.accumulate(val)) {
                ndx = i + start_ndx;
            }
        }
        start_ndx += e;
        // Continue
        return false;
    });

    if (return_ndx)
        *return_ndx = ndx;
}

Mixed DictionaryClusterTree::min(size_t* return_ndx) const
{
    aggregate_operations::Minimum<Mixed> agg;
    do_accumulate(return_ndx, agg);
    return agg.is_null() ? Mixed{} : agg.result();
}

Mixed DictionaryClusterTree::max(size_t* return_ndx) const
{
    aggregate_operations::Maximum<Mixed> agg;
    do_accumulate(return_ndx, agg);
    return agg.is_null() ? Mixed{} : agg.result();
}

Mixed DictionaryClusterTree::sum(size_t* return_cnt, DataType type) const
{
    if (type == type_Int) {
        aggregate_operations::Sum<Int> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.result();
    }
    else if (type == type_Double) {
        aggregate_operations::Sum<Double> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.result();
    }
    else if (type == type_Float) {
        aggregate_operations::Sum<Float> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.result();
    }
    else {
        aggregate_operations::Sum<Mixed> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.result();
    }
}

Mixed DictionaryClusterTree::avg(size_t* return_cnt, DataType type) const
{
    if (type == type_Int) {
        aggregate_operations::Average<Int> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.is_null() ? Mixed{} : agg.result();
    }
    else if (type == type_Double) {
        aggregate_operations::Average<Double> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.is_null() ? Mixed{} : agg.result();
    }
    else if (type == type_Float) {
        aggregate_operations::Average<Float> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.is_null() ? Mixed{} : agg.result();
    }
    else { // Decimal128 is covered with mixed as well.
        aggregate_operations::Average<Mixed> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return agg.is_null() ? Mixed{} : agg.result();
    }
}

/******************************** Dictionary *********************************/

Dictionary::Dictionary(const Obj& obj, ColKey col_key)
    : Base(obj, col_key)
    , m_key_type(m_obj.get_table()->get_dictionary_key_type(m_col_key))
{
    if (!col_key.is_dictionary()) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
}

Dictionary::~Dictionary() = default;

Dictionary& Dictionary::operator=(const Dictionary& other)
{
    Base::operator=(static_cast<const Base&>(other));

    if (this != &other) {
        // Back to scratch
        m_clusters.reset();
        reset_content_version();
    }

    return *this;
}

size_t Dictionary::size() const
{
    if (!update())
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
    // Note: `size()` calls `update_if_needed()`.
    if (ndx >= size()) {
        throw std::out_of_range("ndx out of range");
    }
    ObjKey k;
    return do_get(m_clusters->get(ndx, k));
}

std::pair<Mixed, Mixed> Dictionary::get_pair(size_t ndx) const
{
    // Note: `size()` calls `update_if_needed()`.
    if (ndx >= size()) {
        throw std::out_of_range("ndx out of range");
    }
    ObjKey k;
    return do_get_pair(m_clusters->get(ndx, k));
}

Mixed Dictionary::get_key(size_t ndx) const
{
    // Note: `size()` calls `update_if_needed()`.
    if (ndx >= size()) {
        throw std::out_of_range("ndx out of range");
    }
    ObjKey k;
    return do_get_key(m_clusters->get(ndx, k));
}

size_t Dictionary::find_any(Mixed value) const
{
    size_t ret = realm::not_found;
    if (size()) {
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
    }

    return ret;
}

size_t Dictionary::find_any_key(Mixed key) const noexcept
{
    if (size() == 0) {
        return realm::not_found;
    }
    return m_clusters->get_ndx_with_key(get_internal_obj_key(key), key);
}

util::Optional<Mixed> Dictionary::min(size_t* return_ndx) const
{
    if (update()) {
        return m_clusters->min(return_ndx);
    }
    if (return_ndx)
        *return_ndx = realm::npos;
    return Mixed{};
}

util::Optional<Mixed> Dictionary::max(size_t* return_ndx) const
{
    if (update()) {
        return m_clusters->max(return_ndx);
    }
    if (return_ndx)
        *return_ndx = realm::npos;
    return Mixed{};
}

util::Optional<Mixed> Dictionary::sum(size_t* return_cnt) const
{
    if (update()) {
        return m_clusters->sum(return_cnt, get_value_data_type());
    }
    if (return_cnt)
        *return_cnt = 0;
    return Mixed{0};
}

util::Optional<Mixed> Dictionary::avg(size_t* return_cnt) const
{
    if (update()) {
        return m_clusters->avg(return_cnt, get_value_data_type());
    }
    if (return_cnt)
        *return_cnt = 0;
    return Mixed{};
}

void Dictionary::align_indices(std::vector<size_t>& indices) const
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
}

void Dictionary::sort(std::vector<size_t>& indices, bool ascending) const
{
    align_indices(indices);
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

void Dictionary::distinct(std::vector<size_t>& indices, util::Optional<bool> ascending) const
{
    align_indices(indices);

    bool sort_ascending = ascending ? *ascending : true;
    sort(indices, sort_ascending);
    indices.erase(std::unique(indices.begin(), indices.end(),
                              [this](size_t i1, size_t i2) {
                                  return get_any(i1) == get_any(i2);
                              }),
                  indices.end());

    if (!ascending) {
        // need to return indices in original ordering
        std::sort(indices.begin(), indices.end(), std::less<size_t>());
    }
}

void Dictionary::sort_keys(std::vector<size_t>& indices, bool ascending) const
{
    align_indices(indices);
    auto b = indices.begin();
    auto e = indices.end();
    if (ascending) {
        std::sort(b, e, [this](size_t i1, size_t i2) {
            return get_key(i1) < get_key(i2);
        });
    }
    else {
        std::sort(b, e, [this](size_t i1, size_t i2) {
            return get_key(i1) > get_key(i2);
        });
    }
}

void Dictionary::distinct_keys(std::vector<size_t>& indices, util::Optional<bool>) const
{
    // we rely on the design of dictionary to assume that the keys are unique
    align_indices(indices);
}


Obj Dictionary::create_and_insert_linked_object(Mixed key)
{
    Table& t = *get_target_table();
    auto o = t.is_embedded() ? t.create_linked_object() : t.create_object();
    insert(key, o.get_key());
    return o;
}

Mixed Dictionary::get(Mixed key) const
{
    if (auto opt_val = try_get(key)) {
        return *opt_val;
    }
    throw realm::KeyNotFound("Dictionary::get");
}

util::Optional<Mixed> Dictionary::try_get(Mixed key) const noexcept
{
    if (size()) {
        if (auto state = m_clusters->try_get_with_key(get_internal_obj_key(key), key))
            return do_get(state);
    }
    return {};
}

Dictionary::Iterator Dictionary::begin() const
{
    // Need an update because the `Dictionary::Iterator` constructor relies on
    // `m_clusters` to determine if it was already at end.
    update();
    return Iterator(this, 0);
}

Dictionary::Iterator Dictionary::end() const
{
    return Iterator(this, size());
}

std::pair<Dictionary::Iterator, bool> Dictionary::insert(Mixed key, Mixed value)
{
    if (m_key_type != type_Mixed && key.get_type() != m_key_type) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
    if (value.is_null()) {
        if (!m_col_key.is_nullable()) {
            throw LogicError(LogicError::type_mismatch);
        }
    }
    else {
        if (m_col_key.get_type() == col_type_Link && value.get_type() == type_TypedLink) {
            if (m_obj.get_table()->get_opposite_table_key(m_col_key) != value.get<ObjLink>().get_table_key()) {
                throw std::runtime_error("Dictionary::insert: Wrong object type");
            }
        }
        else if (m_col_key.get_type() != col_type_Mixed && value.get_type() != DataType(m_col_key.get_type())) {
            throw LogicError(LogicError::type_mismatch);
        }
    }

    validate_key_value(key);
    ensure_created();

    ObjLink new_link;
    if (value.is_type(type_TypedLink)) {
        new_link = value.get<ObjLink>();
        if (!new_link.is_unresolved())
            m_obj.get_table()->get_parent_group()->validate(new_link);
    }
    else if (value.is_type(type_Link)) {
        auto target_table = m_obj.get_table()->get_opposite_table(m_col_key);
        auto key = value.get<ObjKey>();
        if (!key.is_unresolved() && !target_table->is_valid(key)) {
            throw LogicError(LogicError::target_row_index_out_of_range);
        }
        new_link = ObjLink(target_table->get_key(), key);
        value = Mixed(new_link);
    }

    if (!m_clusters) {
        throw LogicError(LogicError::detached_accessor);
    }

    ObjKey k = get_internal_obj_key(key);

    bool old_entry = false;
    ClusterNode::State state = m_clusters->try_get(k);
    if (!state) {
        // key does not already exist
        state = m_clusters->insert(k, key, value);
    }
    else {
        // Check if key matches
        if (do_get_key(state).compare_signed(key) == 0) {
            old_entry = true;
        }
        else {
            // We have a collision
            if (!m_clusters->has_collisions()) {
                m_clusters->create_collision_column();
                // cluster tree has changed - find entry again
                state = m_clusters->try_get(k);
            }

            if (auto sibling = m_clusters->find_sibling(state, key)) {
                old_entry = true;
                k = sibling;
            }
            else {
                // Create a key with upper bit set and link it
                auto hash = size_t(k.value);
                auto start = hash;
                ObjKey k2(-2 - hash);
                while (m_clusters->is_valid(k2)) {
                    hash = (hash + 1) & s_hash_mask;
                    REALM_ASSERT(hash != start);
                    k2 = ObjKey(-2 - hash);
                }

                Array fallback(m_obj.get_alloc());
                Array& fields = m_clusters->get_fields_accessor(fallback, state.mem);
                ArrayRef refs(m_obj.get_alloc());
                refs.set_parent(&fields, 3);
                refs.init_from_parent();

                auto old_ref = refs.get(state.index);
                if (!old_ref) {
                    MemRef mem = Array::create_empty_array(NodeHeader::type_Normal, false, m_obj.get_alloc());
                    refs.set(state.index, mem.get_ref());
                }
                Array links(m_obj.get_alloc());
                links.set_parent(&refs, state.index);
                links.init_from_parent();
                links.add(k2.value);

                if (fields.has_missing_parent_update()) {
                    m_clusters->update_ref_in_parent(k, fields.get_ref());
                }

                state = m_clusters->insert(k2, key, value);
                k = k2;
            }
        }
    }

    if (Replication* repl = this->m_obj.get_replication()) {
        auto ndx = m_clusters->get_ndx(k);
        if (old_entry) {
            repl->dictionary_set(*this, ndx, key, value);
        }
        else {
            repl->dictionary_insert(*this, ndx, key, value);
        }
    }

    bump_content_version();

    ObjLink old_link;
    if (old_entry) {
        Array fallback(m_obj.get_alloc());
        Array& fields = m_clusters->get_fields_accessor(fallback, state.mem);
        ArrayMixed values(m_obj.get_alloc());
        values.set_parent(&fields, 2);
        values.init_from_parent();

        Mixed old_value = values.get(state.index);
        if (old_value.is_type(type_TypedLink)) {
            old_link = old_value.get<ObjLink>();
        }
        values.set(state.index, value);
        if (fields.has_missing_parent_update()) {
            m_clusters->update_ref_in_parent(k, fields.get_ref());
        }
    }

    if (new_link != old_link) {
        CascadeState cascade_state(CascadeState::Mode::Strong);
        bool recurse = m_obj.replace_backlink(m_col_key, old_link, new_link, cascade_state);
        if (recurse)
            _impl::TableFriend::remove_recursive(*m_obj.get_table(), cascade_state); // Throws
    }

    return {Iterator(this, state.index), !old_entry};
}

const Mixed Dictionary::operator[](Mixed key)
{
    auto ret = try_get(key);
    if (!ret) {
        ret = Mixed{};
        insert(key, Mixed{});
    }

    return *ret;
}

bool Dictionary::contains(Mixed key) const noexcept
{
    if (size() == 0) {
        return false;
    }
    return m_clusters->try_get_with_key(get_internal_obj_key(key), key);
}

Dictionary::Iterator Dictionary::find(Mixed key) const noexcept
{
    auto ndx = find_any_key(key);
    if (ndx != realm::npos) {
        return Iterator(this, ndx);
    }
    return end();
}

ObjKey Dictionary::handle_collision_in_erase(const Mixed& key, ObjKey k, ClusterNode::State& state)
{
    Array fallback(m_obj.get_alloc());
    Array& fields = m_clusters->get_fields_accessor(fallback, state.mem);
    ArrayRef refs(m_obj.get_alloc());
    refs.set_parent(&fields, 3);
    refs.init_from_parent();

    if (!refs.get(state.index)) {
        // No collisions on this hash
        REALM_ASSERT(do_get_key(state).compare_signed(key) == 0);
        return k;
    }

    ObjKey k2;
    Array links(m_obj.get_alloc());
    links.set_parent(&refs, state.index);
    links.init_from_parent();

    auto sz = links.size();
    REALM_ASSERT(sz > 0);
    if (do_get_key(state).compare_signed(key) == 0) {
        // We are erasing main entry, swap last sibling over
        k2 = ObjKey(links.get(sz - 1));
        if (sz == 1) {
            refs.set(links.get_ndx_in_parent(), 0);
            links.destroy();
        }
        else {
            links.erase(sz - 1);
        }
        if (fields.has_missing_parent_update()) {
            auto new_ref = fields.get_ref();
            m_clusters->update_ref_in_parent(k, new_ref);
        }

        auto index1 = state.index;
        state = m_clusters->get(k2);
        Array fallback2(m_obj.get_alloc());
        Array& fields2 = m_clusters->get_fields_accessor(fallback2, state.mem);

        swap_content(fields, fields2, index1, state.index);

        if (fields2.has_missing_parent_update()) {
            auto new_ref = fields2.get_ref();
            m_clusters->update_ref_in_parent(k2, new_ref);
            // The content of the sibling has now been updated with the
            // content of the master entry, so 'state' must point to this
            // new writable cluster
            state.mem = MemRef(new_ref, m_obj.get_alloc());
        }
    }
    else {
        // We are erasing a sibling
        // Find the right one and erase from list in main entry
        size_t i = 0;
        for (;;) {
            k2 = ObjKey(links.get(i));
            state = m_clusters->get(k2);
            if (do_get_key(state).compare_signed(key) == 0) {
                // Erase this entry in the link array
                if (links.size() == 1) {
                    refs.set(links.get_ndx_in_parent(), 0);
                    links.destroy();
                }
                else {
                    links.erase(i);
                }
                break;
            }
            i++;
            REALM_ASSERT_RELEASE(i < sz);
        }
        if (fields.has_missing_parent_update()) {
            auto new_ref = fields.get_ref();
            // This may COW the cluster 'state' points to, but it is still
            // ok to use it for reading, so no need to update
            m_clusters->update_ref_in_parent(k, new_ref);
        }
    }
    return k2; // This will ensure that sibling is erased
}

UpdateStatus Dictionary::update_if_needed() const
{
    auto status = Base::update_if_needed();
    switch (status) {
        case UpdateStatus::Detached: {
            m_clusters.reset();
            return UpdateStatus::Detached;
        }
        case UpdateStatus::NoChange: {
            if (m_clusters && m_clusters->is_attached()) {
                return UpdateStatus::NoChange;
            }
            // The tree has not been initialized yet for this accessor, so
            // perform lazy initialization by treating it as an update.
            [[fallthrough]];
        }
        case UpdateStatus::Updated: {
            bool attached = init_from_parent(false);
            return attached ? UpdateStatus::Updated : UpdateStatus::Detached;
        }
    }
    REALM_UNREACHABLE();
}

UpdateStatus Dictionary::ensure_created()
{
    auto status = Base::ensure_created();
    switch (status) {
        case UpdateStatus::Detached:
            break; // Not possible (would have thrown earlier).
        case UpdateStatus::NoChange: {
            if (m_clusters && m_clusters->is_attached()) {
                return UpdateStatus::NoChange;
            }
            // The tree has not been initialized yet for this accessor, so
            // perform lazy initialization by treating it as an update.
            [[fallthrough]];
        }
        case UpdateStatus::Updated: {
            bool attached = init_from_parent(true);
            REALM_ASSERT(attached);
            return attached ? UpdateStatus::Updated : UpdateStatus::Detached;
        }
    }

    REALM_UNREACHABLE();
}

bool Dictionary::try_erase(Mixed key)
{
    validate_key_value(key);
    ClusterNode::State state;
    ObjKey k = get_internal_obj_key(key);
    if (size()) {
        state = m_clusters->try_get(k);
    }
    if (!state) {
        return false;
    }

    if (m_clusters->has_collisions()) {
        // State will be updated to refer to element that is going to be erased
        k = handle_collision_in_erase(key, k, state);
    }

    ArrayMixed values(m_obj.get_alloc());
    ref_type ref = to_ref(Array::get(state.mem.get_addr(), 2));
    values.init_from_ref(ref);
    auto old_value = values.get(state.index);

    CascadeState cascade_state(CascadeState::Mode::Strong);
    bool recurse = clear_backlink(old_value, cascade_state);
    if (recurse)
        _impl::TableFriend::remove_recursive(*m_obj.get_table(), cascade_state); // Throws

    if (Replication* repl = this->m_obj.get_replication()) {
        auto ndx = m_clusters->get_ndx(k);
        repl->dictionary_erase(*this, ndx, key);
    }

    CascadeState dummy;
    m_clusters->erase(k, dummy);
    bump_content_version();

    return true;
}


void Dictionary::erase(Mixed key)
{
    if (!try_erase(key)) {
        throw KeyNotFound("Dictionary::erase");
    }
}

void Dictionary::erase(Iterator it)
{
    erase((*it).first);
}

void Dictionary::nullify(Mixed key)
{
    REALM_ASSERT(m_clusters);
    ObjKey k = get_internal_obj_key(key);
    auto state = m_clusters->try_get_with_key(k, key);
    REALM_ASSERT(state.index != realm::npos);

    if (Replication* repl = this->m_obj.get_replication()) {
        auto ndx = m_clusters->get_ndx(k);
        repl->dictionary_set(*this, ndx, key, Mixed());
    }

    Array fallback(m_obj.get_alloc());
    Array& fields = m_clusters->get_fields_accessor(fallback, state.mem);
    ArrayMixed values(m_obj.get_alloc());
    values.set_parent(&fields, 2);
    values.init_from_parent();

    values.set(state.index, Mixed());

    if (fields.has_missing_parent_update()) {
        m_clusters->update_ref_in_parent(k, fields.get_ref());
    }
}

void Dictionary::remove_backlinks(CascadeState& state) const
{
    for (auto&& elem : *this) {
        clear_backlink(elem.second, state);
    }
}

void Dictionary::clear()
{
    if (size() > 0) {
        // TODO: Should we have a "dictionary_clear" instruction?
        Replication* repl = m_obj.get_replication();
        bool recurse = false;
        CascadeState cascade_state(CascadeState::Mode::Strong);
        for (auto&& elem : *this) {
            if (clear_backlink(elem.second, cascade_state))
                recurse = true;
            if (repl) {
                // Logically we always erase the first element
                repl->dictionary_erase(*this, 0, elem.first);
            }
        }

        // Just destroy the whole cluster
        m_clusters->destroy();
        m_clusters.reset();

        update_child_ref(0, 0);

        if (recurse)
            _impl::TableFriend::remove_recursive(*m_obj.get_table(), cascade_state); // Throws
    }
}

bool Dictionary::init_from_parent(bool allow_create) const
{
    if (!m_clusters) {
        m_clusters.reset(new DictionaryClusterTree(const_cast<Dictionary*>(this), m_key_type, m_obj.get_alloc(),
                                                   m_obj.get_row_ndx()));
    }

    if (m_clusters->init_from_parent()) {
        // All is well
        return true;
    }


    if (!allow_create) {
        return false;
    }

    MemRef mem = Cluster::create_empty_cluster(m_obj.get_alloc());
    const_cast<Dictionary*>(this)->update_child_ref(0, mem.get_ref());
    bool attached = m_clusters->init_from_parent();
    REALM_ASSERT(attached);
    m_clusters->add_columns();

    return true;
}

Mixed Dictionary::do_get(const ClusterNode::State& s) const
{
    ArrayMixed values(m_obj.get_alloc());
    ref_type ref = to_ref(Array::get(s.mem.get_addr(), 2));
    values.init_from_ref(ref);
    Mixed val = values.get(s.index);

    // Filter out potential unresolved links
    if (val.is_type(type_TypedLink)) {
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

Mixed Dictionary::do_get_key(const ClusterNode::State& s) const
{
    return m_clusters->get_key(s);
}

std::pair<Mixed, Mixed> Dictionary::do_get_pair(const ClusterNode::State& s) const
{
    return {do_get_key(s), do_get(s)};
}

bool Dictionary::clear_backlink(Mixed value, CascadeState& state) const
{
    if (value.is_type(type_TypedLink)) {
        return m_obj.remove_backlink(m_col_key, value.get_link(), state);
    }
    return false;
}

void Dictionary::swap_content(Array& fields1, Array& fields2, size_t index1, size_t index2)
{
    std::string buf1, buf2;

    // Swap keys
    REALM_ASSERT(m_key_type == type_String);
    ArrayString keys(m_obj.get_alloc());
    keys.set_parent(&fields1, 1);
    keys.init_from_parent();
    buf1 = keys.get(index1);

    keys.set_parent(&fields2, 1);
    keys.init_from_parent();
    buf2 = keys.get(index2);
    keys.set(index2, buf1);

    keys.set_parent(&fields1, 1);
    keys.init_from_parent();
    keys.set(index1, buf2);

    // Swap values
    ArrayMixed values(m_obj.get_alloc());
    values.set_parent(&fields1, 2);
    values.init_from_parent();
    Mixed val1 = values.get(index1);
    val1.use_buffer(buf1);

    values.set_parent(&fields2, 2);
    values.init_from_parent();
    Mixed val2 = values.get(index2);
    val2.use_buffer(buf2);
    values.set(index2, val1);

    values.set_parent(&fields1, 2);
    values.init_from_parent();
    values.set(index1, val2);
}

/************************* Dictionary::Iterator *************************/

Dictionary::Iterator::Iterator(const Dictionary* dict, size_t pos)
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


/************************* DictionaryLinkValues *************************/

DictionaryLinkValues::DictionaryLinkValues(const Obj& obj, ColKey col_key)
    : m_source(obj, col_key)
{
    REALM_ASSERT_EX(col_key.get_type() == col_type_Link, col_key.get_type());
}

DictionaryLinkValues::DictionaryLinkValues(const Dictionary& source)
    : m_source(source)
{
    REALM_ASSERT_EX(source.get_value_data_type() == type_Link, source.get_value_data_type());
}

ObjKey DictionaryLinkValues::get_key(size_t ndx) const
{
    Mixed val = m_source.get_any(ndx);
    if (val.is_type(type_Link)) {
        return val.get<ObjKey>();
    }
    return {};
}

// In contrast to a link list and a link set, a dictionary can contain null links.
// This is because the corresponding key may contain useful information by itself.
bool DictionaryLinkValues::is_obj_valid(size_t ndx) const noexcept
{
    Mixed val = m_source.get_any(ndx);
    return val.is_type(type_Link);
}

Obj DictionaryLinkValues::get_object(size_t row_ndx) const
{
    Mixed val = m_source.get_any(row_ndx);
    if (val.is_type(type_Link)) {
        return get_target_table()->get_object(val.get<ObjKey>());
    }
    return {};
}

} // namespace realm
