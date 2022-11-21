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

template <class T>
class SortedKeys {
public:
    SortedKeys(T* keys)
        : m_list(keys)
    {
    }
    CollectionIterator<T> begin() const
    {
        return CollectionIterator<T>(m_list, 0);
    }
    CollectionIterator<T> end() const
    {
        return CollectionIterator<T>(m_list, m_list->size());
    }

private:
    T* m_list;
};
} // namespace


/******************************** Dictionary *********************************/

Dictionary::Dictionary(const Obj& obj, ColKey col_key)
    : Base(obj, col_key)
    , m_key_type(m_obj.get_table()->get_dictionary_key_type(m_col_key))
{
    if (!col_key.is_dictionary()) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
}

Dictionary::Dictionary(Allocator& alloc, ColKey col_key, ref_type ref)
    : Base(Obj{}, col_key)
    , m_key_type(type_String)
{
    REALM_ASSERT(ref);
    m_dictionary_top.reset(new Array(alloc));
    m_dictionary_top->init_from_ref(ref);
    m_keys.reset(new BPlusTree<StringData>(alloc));
    m_values.reset(new BPlusTree<Mixed>(alloc));
    m_keys->set_parent(m_dictionary_top.get(), 0);
    m_values->set_parent(m_dictionary_top.get(), 1);
    m_keys->init_from_parent();
    m_values->init_from_parent();
}

Dictionary::~Dictionary() = default;

Dictionary& Dictionary::operator=(const Dictionary& other)
{
    Base::operator=(static_cast<const Base&>(other));

    if (this != &other) {
        // Back to scratch
        m_dictionary_top.reset();
        reset_content_version();
    }

    return *this;
}

size_t Dictionary::size() const
{
    if (!update())
        return 0;

    return m_values->size();
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
    return m_values->get(ndx);
}

std::pair<Mixed, Mixed> Dictionary::get_pair(size_t ndx) const
{
    // Note: `size()` calls `update_if_needed()`.
    if (ndx >= size()) {
        throw std::out_of_range("ndx out of range");
    }
    return do_get_pair(ndx);
}

Mixed Dictionary::get_key(size_t ndx) const
{
    // Note: `size()` calls `update_if_needed()`.
    if (ndx >= size()) {
        throw std::out_of_range("ndx out of range");
    }
    return do_get_key(ndx);
}

size_t Dictionary::find_any(Mixed value) const
{
    return size() ? m_values->find_first(value) : realm::not_found;
}

size_t Dictionary::find_any_key(Mixed key) const noexcept
{
    if (update()) {
        return do_find_key(key);
    }

    return realm::npos;
}

template <typename AggregateType>
void Dictionary::do_accumulate(size_t* return_ndx, AggregateType& agg) const
{
    size_t ndx = realm::npos;

    m_values->traverse([&](BPlusTreeNode* node, size_t offset) {
        auto leaf = static_cast<BPlusTree<Mixed>::LeafNode*>(node);
        size_t e = leaf->size();
        for (size_t i = 0; i < e; i++) {
            auto val = leaf->get(i);
            if (agg.accumulate(val)) {
                ndx = i + offset;
            }
        }
        // Continue
        return IteratorControl::AdvanceToNext;
    });

    if (return_ndx)
        *return_ndx = ndx;
}

util::Optional<Mixed> Dictionary::do_min(size_t* return_ndx) const
{
    aggregate_operations::Minimum<Mixed> agg;
    do_accumulate(return_ndx, agg);
    return agg.is_null() ? Mixed{} : agg.result();
}

util::Optional<Mixed> Dictionary::do_max(size_t* return_ndx) const
{
    aggregate_operations::Maximum<Mixed> agg;
    do_accumulate(return_ndx, agg);
    return agg.is_null() ? Mixed{} : agg.result();
}

util::Optional<Mixed> Dictionary::do_sum(size_t* return_cnt) const
{
    auto type = get_value_data_type();
    if (type == type_Int) {
        aggregate_operations::Sum<Int> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return Mixed{agg.result()};
    }
    else if (type == type_Double) {
        aggregate_operations::Sum<Double> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return Mixed{agg.result()};
    }
    else if (type == type_Float) {
        aggregate_operations::Sum<Float> agg;
        do_accumulate(nullptr, agg);
        if (return_cnt)
            *return_cnt = agg.items_counted();
        return Mixed{agg.result()};
    }

    aggregate_operations::Sum<Mixed> agg;
    do_accumulate(nullptr, agg);
    if (return_cnt)
        *return_cnt = agg.items_counted();
    return Mixed{agg.result()};
}

util::Optional<Mixed> Dictionary::do_avg(size_t* return_cnt) const
{
    auto type = get_value_data_type();
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
    // Decimal128 is covered with mixed as well.
    aggregate_operations::Average<Mixed> agg;
    do_accumulate(nullptr, agg);
    if (return_cnt)
        *return_cnt = agg.items_counted();
    return agg.is_null() ? Mixed{} : agg.result();
}

namespace {
bool can_minmax(DataType type)
{
    switch (type) {
        case type_Int:
        case type_Float:
        case type_Double:
        case type_Decimal:
        case type_Mixed:
        case type_Timestamp:
            return true;
        default:
            return false;
    }
}
bool can_sum(DataType type)
{
    switch (type) {
        case type_Int:
        case type_Float:
        case type_Double:
        case type_Decimal:
        case type_Mixed:
            return true;
        default:
            return false;
    }
}
} // anonymous namespace

util::Optional<Mixed> Dictionary::min(size_t* return_ndx) const
{
    if (!can_minmax(get_value_data_type())) {
        return std::nullopt;
    }
    if (update()) {
        return do_min(return_ndx);
    }
    if (return_ndx)
        *return_ndx = realm::not_found;
    return Mixed{};
}

util::Optional<Mixed> Dictionary::max(size_t* return_ndx) const
{
    if (!can_minmax(get_value_data_type())) {
        return std::nullopt;
    }
    if (update()) {
        return do_max(return_ndx);
    }
    if (return_ndx)
        *return_ndx = realm::not_found;
    return Mixed{};
}

util::Optional<Mixed> Dictionary::sum(size_t* return_cnt) const
{
    if (!can_sum(get_value_data_type())) {
        return std::nullopt;
    }
    if (update()) {
        return do_sum(return_cnt);
    }
    if (return_cnt)
        *return_cnt = 0;
    return Mixed{0};
}

util::Optional<Mixed> Dictionary::avg(size_t* return_cnt) const
{
    if (!can_sum(get_value_data_type())) {
        return std::nullopt;
    }
    if (update()) {
        return do_avg(return_cnt);
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

namespace {
template <class T>
void do_sort(std::vector<size_t>& indices, bool ascending, const std::vector<T>& values)
{
    auto b = indices.begin();
    auto e = indices.end();
    std::sort(b, e, [ascending, &values](size_t i1, size_t i2) {
        return ascending ? values[i1] < values[i2] : values[i2] < values[i1];
    });
}
} // anonymous namespace

void Dictionary::sort(std::vector<size_t>& indices, bool ascending) const
{
    align_indices(indices);
    do_sort(indices, ascending, m_values->get_all());
}

void Dictionary::distinct(std::vector<size_t>& indices, util::Optional<bool> ascending) const
{
    align_indices(indices);
    auto values = m_values->get_all();
    do_sort(indices, ascending.value_or(true), values);
    indices.erase(std::unique(indices.begin(), indices.end(),
                              [&values](size_t i1, size_t i2) {
                                  return values[i1] == values[i2];
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
#ifdef REALM_DEBUG
    if (indices.size() > 1) {
        // We rely in the design that the keys are already sorted
        switch (m_key_type) {
            case type_String: {
                SortedKeys help(static_cast<BPlusTree<StringData>*>(m_keys.get()));
                auto is_sorted = std::is_sorted(help.begin(), help.end());
                REALM_ASSERT(is_sorted);
                break;
            }
            case type_Int: {
                SortedKeys help(static_cast<BPlusTree<Int>*>(m_keys.get()));
                auto is_sorted = std::is_sorted(help.begin(), help.end());
                REALM_ASSERT(is_sorted);
                break;
            }
            default:
                break;
        }
    }
#endif
    if (ascending) {
        std::sort(indices.begin(), indices.end());
    }
    else {
        std::sort(indices.begin(), indices.end(), std::greater<size_t>());
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
    if (update()) {
        auto ndx = do_find_key(key);
        if (ndx != realm::npos) {
            return do_get(ndx);
        }
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
    if (key.get_type() != m_key_type) {
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

    if (!m_dictionary_top) {
        throw LogicError(LogicError::detached_accessor);
    }

    bool old_entry = false;
    auto [ndx, actual_key] = find_impl(key);
    if (actual_key != key) {
        // key does not already exist
        switch (m_key_type) {
            case type_String:
                static_cast<BPlusTree<StringData>*>(m_keys.get())->insert(ndx, key.get_string());
                break;
            case type_Int:
                static_cast<BPlusTree<Int>*>(m_keys.get())->insert(ndx, key.get_int());
                break;
            default:
                throw std::runtime_error("Not implemented");
                break;
        }
        m_values->insert(ndx, value);
    }
    else {
        old_entry = true;
    }

    if (Replication* repl = this->m_obj.get_replication()) {
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
        Mixed old_value = m_values->get(ndx);
        if (old_value.is_type(type_TypedLink)) {
            old_link = old_value.get<ObjLink>();
        }
        m_values->set(ndx, value);
    }

    if (new_link != old_link) {
        CascadeState cascade_state(CascadeState::Mode::Strong);
        bool recurse = m_obj.replace_backlink(m_col_key, old_link, new_link, cascade_state);
        if (recurse)
            _impl::TableFriend::remove_recursive(*m_obj.get_table(), cascade_state); // Throws
    }

    return {Iterator(this, ndx), !old_entry};
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

Obj Dictionary::get_object(StringData key)
{
    if (auto val = try_get(key)) {
        if ((*val).is_type(type_TypedLink)) {
            return get_table()->get_parent_group()->get_object((*val).get_link());
        }
    }
    return {};
}

bool Dictionary::contains(Mixed key) const noexcept
{
    return find_any_key(key) != realm::npos;
}

Dictionary::Iterator Dictionary::find(Mixed key) const noexcept
{
    auto ndx = find_any_key(key);
    if (ndx != realm::npos) {
        return Iterator(this, ndx);
    }
    return end();
}

UpdateStatus Dictionary::update_if_needed() const
{
    auto status = Base::update_if_needed();
    switch (status) {
        case UpdateStatus::Detached: {
            m_dictionary_top.reset();
            return UpdateStatus::Detached;
        }
        case UpdateStatus::NoChange: {
            if (m_dictionary_top && m_dictionary_top->is_attached()) {
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
            if (m_dictionary_top && m_dictionary_top->is_attached()) {
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
    if (!update())
        return false;

    auto ndx = do_find_key(key);
    if (ndx == realm::npos) {
        return false;
    }

    do_erase(ndx, key);

    return true;
}


void Dictionary::erase(Mixed key)
{
    if (!try_erase(key)) {
        throw KeyNotFound("Dictionary::erase");
    }
}

auto Dictionary::erase(Iterator it) -> Iterator
{
    auto pos = it.m_ndx;
    if (pos >= size()) {
        throw std::out_of_range("ndx out of range");
    }

    do_erase(pos, do_get_key(pos));
    if (pos < size())
        pos++;
    return {this, pos};
}

void Dictionary::nullify(Mixed key)
{
    REALM_ASSERT(m_dictionary_top);
    auto ndx = do_find_key(key);
    REALM_ASSERT(ndx != realm::npos);

    if (Replication* repl = this->m_obj.get_replication()) {
        repl->dictionary_set(*this, ndx, key, Mixed());
    }

    m_values->set(ndx, Mixed());
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
        m_dictionary_top->destroy_deep();
        m_dictionary_top.reset();

        update_child_ref(0, 0);

        if (recurse)
            _impl::TableFriend::remove_recursive(*m_obj.get_table(), cascade_state); // Throws
    }
}

bool Dictionary::init_from_parent(bool allow_create) const
{
    auto ref = m_obj._get<int64_t>(m_col_key.get_index());

    if ((ref || allow_create) && !m_dictionary_top) {
        m_dictionary_top.reset(new Array(m_obj.get_alloc()));
        m_dictionary_top->set_parent(const_cast<Dictionary*>(this), m_obj.get_row_ndx());
        switch (m_key_type) {
            case type_String: {
                m_keys.reset(new BPlusTree<StringData>(m_obj.get_alloc()));
                break;
            }
            case type_Int: {
                m_keys.reset(new BPlusTree<Int>(m_obj.get_alloc()));
                break;
            }
            default:
                throw std::runtime_error("Not implemented");
                break;
        }
        m_keys->set_parent(m_dictionary_top.get(), 0);
        m_values.reset(new BPlusTree<Mixed>(m_obj.get_alloc()));
        m_values->set_parent(m_dictionary_top.get(), 1);
    }

    if (ref) {
        m_dictionary_top->init_from_parent();
        m_keys->init_from_parent();
        m_values->init_from_parent();
    }
    else {
        // dictionary detached
        if (!allow_create) {
            m_dictionary_top.reset();
            return false;
        }

        // Create dictionary
        m_dictionary_top->create(Array::type_HasRefs, false, 2, 0);
        m_values->create();
        m_keys->create();
        m_dictionary_top->update_parent();
    }

    return true;
}

size_t Dictionary::do_find_key(Mixed key) const noexcept
{
    auto [ndx, actual_key] = find_impl(key);
    if (actual_key == key) {
        return ndx;
    }
    return realm::npos;
}

std::pair<size_t, Mixed> Dictionary::find_impl(Mixed key) const noexcept
{
    auto sz = m_keys->size();
    Mixed actual;
    if (sz && key.is_type(m_key_type)) {
        switch (m_key_type) {
            case type_String: {
                auto keys = static_cast<BPlusTree<StringData>*>(m_keys.get());
                StringData val = key.get<StringData>();
                SortedKeys help(keys);
                auto it = std::lower_bound(help.begin(), help.end(), val);
                if (it.index() < sz) {
                    actual = *it;
                }
                return {it.index(), actual};
                break;
            }
            case type_Int: {
                auto keys = static_cast<BPlusTree<Int>*>(m_keys.get());
                Int val = key.get<Int>();
                SortedKeys help(keys);
                auto it = std::lower_bound(help.begin(), help.end(), val);
                if (it.index() < sz) {
                    actual = *it;
                }
                return {it.index(), actual};
                break;
            }
            default:
                break;
        }
    }

    return {sz, actual};
}

Mixed Dictionary::do_get(size_t ndx) const
{
    Mixed val = m_values->get(ndx);

    // Filter out potential unresolved links
    if (val.is_type(type_TypedLink)) {
        auto link = val.get<ObjLink>();
        auto key = link.get_obj_key();
        if (key.is_unresolved()) {
            return {};
        }
    }
    return val;
}

void Dictionary::do_erase(size_t ndx, Mixed key)
{
    auto old_value = m_values->get(ndx);

    CascadeState cascade_state(CascadeState::Mode::Strong);
    bool recurse = clear_backlink(old_value, cascade_state);
    if (recurse)
        _impl::TableFriend::remove_recursive(*m_obj.get_table(), cascade_state); // Throws

    if (Replication* repl = this->m_obj.get_replication()) {
        repl->dictionary_erase(*this, ndx, key);
    }

    m_keys->erase(ndx);
    m_values->erase(ndx);

    bump_content_version();
}

Mixed Dictionary::do_get_key(size_t ndx) const
{
    switch (m_key_type) {
        case type_String: {
            return static_cast<BPlusTree<StringData>*>(m_keys.get())->get(ndx);
        }
        case type_Int: {
            return static_cast<BPlusTree<Int>*>(m_keys.get())->get(ndx);
        }
        default:
            throw std::runtime_error("Not implemented");
            break;
    }

    return {};
}

std::pair<Mixed, Mixed> Dictionary::do_get_pair(size_t ndx) const
{
    return {do_get_key(ndx), do_get(ndx)};
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

Mixed Dictionary::find_value(Mixed value) const noexcept
{
    size_t ndx = update() ? m_values->find_first(value) : realm::npos;
    return (ndx == realm::npos) ? Mixed{} : do_get_key(ndx);
}

void Dictionary::verify() const
{
    m_keys->verify();
    m_values->verify();
    REALM_ASSERT(m_keys->size() == m_values->size());
}

void Dictionary::migrate()
{
    // Dummy implementation of legacy dictionary cluster tree
    class DictionaryClusterTree : public ClusterTree {
    public:
        DictionaryClusterTree(ArrayParent* owner, Allocator& alloc, size_t ndx)
            : ClusterTree(nullptr, alloc, ndx)
            , m_owner(owner)
        {
        }

        std::unique_ptr<ClusterNode> get_root_from_parent() final
        {
            return create_root_from_parent(m_owner, m_top_position_for_cluster_tree);
        }

    private:
        ArrayParent* m_owner;
    };

    if (auto dict_ref = m_obj._get<int64_t>(get_col_key().get_index())) {
        DictionaryClusterTree cluster_tree(this, m_obj.get_alloc(), m_obj.get_row_ndx());
        if (cluster_tree.init_from_parent()) {
            // Create an empty dictionary in the old ones place
            m_obj.set_int(get_col_key(), 0);
            ensure_created();

            ArrayString keys(m_obj.get_alloc()); // We only support string type keys.
            ArrayMixed values(m_obj.get_alloc());
            constexpr ColKey key_col(ColKey::Idx{0}, col_type_String, ColumnAttrMask(), 0);
            constexpr ColKey value_col(ColKey::Idx{1}, col_type_Mixed, ColumnAttrMask(), 0);
            size_t nb_elements = cluster_tree.size();
            cluster_tree.traverse([&](const Cluster* cluster) {
                cluster->init_leaf(key_col, &keys);
                cluster->init_leaf(value_col, &values);
                auto sz = cluster->node_size();
                for (size_t i = 0; i < sz; i++) {
                    // Just use low level functions to insert elements. All keys must be legal and
                    // unique and all values must match expected type. Links should just be preserved
                    // so no need to worry about backlinks.
                    StringData key = keys.get(i);
                    auto [ndx, actual_key] = find_impl(key);
                    REALM_ASSERT(actual_key != key);
                    static_cast<BPlusTree<StringData>*>(m_keys.get())->insert(ndx, key);
                    m_values->insert(ndx, values.get(i));
                }
                return IteratorControl::AdvanceToNext;
            });
            REALM_ASSERT(size() == nb_elements);
            Array::destroy_deep(to_ref(dict_ref), m_obj.get_alloc());
        }
        else {
            REALM_UNREACHABLE();
        }
    }
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
    if (val.is_type(type_Link, type_TypedLink)) {
        return val.get<ObjKey>();
    }
    return {};
}

Obj DictionaryLinkValues::get_object(size_t row_ndx) const
{
    Mixed val = m_source.get_any(row_ndx);
    if (val.is_type(type_TypedLink)) {
        return get_table()->get_parent_group()->get_object(val.get_link());
    }
    return {};
}

} // namespace realm
