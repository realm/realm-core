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
#include <realm/list.hpp>
#include <realm/set.hpp>
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
                throw Exception(ErrorCodes::InvalidDictionaryKey, "Dictionary::insert: key must not start with '$'");
            if (memchr(str.data(), '.', str.size()))
                throw Exception(ErrorCodes::InvalidDictionaryKey, "Dictionary::insert: key must not contain '.'");
        }
    }
}

} // namespace


/******************************** Dictionary *********************************/

Dictionary::Dictionary(ColKey col_key, uint8_t level)
    : Base(col_key)
    , CollectionParent(level)
{
    if (!(col_key.is_dictionary() || col_key.get_type() == col_type_Mixed)) {
        throw InvalidArgument(ErrorCodes::TypeMismatch, "Property not a dictionary");
    }
}

Dictionary::Dictionary(Allocator& alloc, ColKey col_key, ref_type ref)
    : Base(Obj{}, col_key)
    , m_key_type(type_String)
{
    set_alloc(alloc);
    REALM_ASSERT(ref);
    m_dictionary_top.reset(new Array(alloc));
    m_dictionary_top->init_from_ref(ref);
    m_keys.reset(new BPlusTree<StringData>(alloc));
    m_values.reset(new BPlusTreeMixed(alloc));
    m_keys->set_parent(m_dictionary_top.get(), 0);
    m_values->set_parent(m_dictionary_top.get(), 1);
    m_keys->init_from_parent();
    m_values->init_from_parent();
}

Dictionary::~Dictionary() = default;

Dictionary& Dictionary::operator=(const Dictionary& other)
{
    if (this != &other) {
        Base::operator=(other);
        CollectionParent::operator=(other);

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
    auto current_size = size();
    CollectionBase::validate_index("get_any()", ndx, current_size);
    return do_get(ndx);
}

std::pair<Mixed, Mixed> Dictionary::get_pair(size_t ndx) const
{
    // Note: `size()` calls `update_if_needed()`.
    auto current_size = size();
    CollectionBase::validate_index("get_pair()", ndx, current_size);
    return do_get_pair(ndx);
}

Mixed Dictionary::get_key(size_t ndx) const
{
    // Note: `size()` calls `update_if_needed()`.
    CollectionBase::validate_index("get_key()", ndx, size());
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
                IteratorAdapter help(static_cast<BPlusTree<StringData>*>(m_keys.get()));
                auto is_sorted = std::is_sorted(help.begin(), help.end());
                REALM_ASSERT(is_sorted);
                break;
            }
            case type_Int: {
                IteratorAdapter help(static_cast<BPlusTree<Int>*>(m_keys.get()));
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

void Dictionary::insert_collection(const PathElement& path_elem, CollectionType dict_or_list)
{
    if (dict_or_list == CollectionType::Set) {
        throw IllegalOperation("Set nested in Dictionary is not supported");
    }
    check_level();
    insert(path_elem.get_key(), Mixed(0, dict_or_list));
}

template <class T>
inline std::shared_ptr<T> Dictionary::do_get_collection(const PathElement& path_elem)
{
    update();
    auto get_shared = [&]() -> std::shared_ptr<CollectionParent> {
        auto weak = weak_from_this();

        if (weak.expired()) {
            REALM_ASSERT_DEBUG(m_level == 1);
            return std::make_shared<Dictionary>(*this);
        }

        return weak.lock();
    };

    auto shared = get_shared();
    auto ret = std::make_shared<T>(m_col_key, get_level() + 1);
    ret->set_owner(shared, build_index(path_elem.get_key()));
    return ret;
}

DictionaryPtr Dictionary::get_dictionary(const PathElement& path_elem) const
{
    return const_cast<Dictionary*>(this)->do_get_collection<Dictionary>(path_elem);
}

std::shared_ptr<Lst<Mixed>> Dictionary::get_list(const PathElement& path_elem) const
{
    return const_cast<Dictionary*>(this)->do_get_collection<Lst<Mixed>>(path_elem);
}

Mixed Dictionary::get(Mixed key) const
{
    if (auto opt_val = try_get(key)) {
        return *opt_val;
    }
    throw KeyNotFound("Dictionary::get");
}

util::Optional<Mixed> Dictionary::try_get(Mixed key) const
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
    auto my_table = get_table_unchecked();
    if (key.get_type() != m_key_type) {
        throw InvalidArgument(ErrorCodes::InvalidDictionaryKey, "Dictionary::insert: Invalid key type");
    }
    if (m_col_key) {
        if (value.is_null()) {
            if (!m_col_key.is_nullable()) {
                throw InvalidArgument(ErrorCodes::InvalidDictionaryValue, "Dictionary::insert: Value cannot be null");
            }
        }
        else {
            if (m_col_key.get_type() == col_type_Link && value.get_type() == type_TypedLink) {
                if (my_table->get_opposite_table_key(m_col_key) != value.get<ObjLink>().get_table_key()) {
                    throw InvalidArgument(ErrorCodes::InvalidDictionaryValue,
                                          "Dictionary::insert: Wrong object type");
                }
            }
            else if (m_col_key.get_type() != col_type_Mixed && value.get_type() != DataType(m_col_key.get_type())) {
                throw InvalidArgument(ErrorCodes::InvalidDictionaryValue, "Dictionary::insert: Wrong value type");
            }
            else if (value.is_type(type_Link) && m_col_key.get_type() != col_type_Link) {
                throw InvalidArgument(ErrorCodes::InvalidDictionaryValue,
                                      "Dictionary::insert: No target table for link");
            }
        }
    }

    validate_key_value(key);
    ensure_created();

    ObjLink new_link;
    if (value.is_type(type_TypedLink)) {
        new_link = value.get<ObjLink>();
        if (!new_link.is_unresolved())
            my_table->get_parent_group()->validate(new_link);
    }
    else if (value.is_type(type_Link)) {
        auto target_table = my_table->get_opposite_table(m_col_key);
        auto key = value.get<ObjKey>();
        if (!key.is_unresolved() && !target_table->is_valid(key)) {
            throw InvalidArgument(ErrorCodes::KeyNotFound, "Target object not found");
        }
        new_link = ObjLink(target_table->get_key(), key);
        value = Mixed(new_link);
    }

    if (!m_dictionary_top) {
        throw StaleAccessor("Stale dictionary");
    }

    bool set_nested_collection_key = value.is_type(type_Dictionary, type_List);
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
                break;
        }
        m_values->insert(ndx, value);
    }
    else {
        old_entry = true;
    }

    if (Replication* repl = get_replication()) {
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
        if (!value.is_same_type(old_value) || value != old_value) {
            if (old_value.is_type(type_TypedLink)) {
                old_link = old_value.get<ObjLink>();
            }
            m_values->set(ndx, value);
        }
        else {
            set_nested_collection_key = false;
        }
    }

    if (set_nested_collection_key) {
        m_values->ensure_keys();
        set_key(*m_values, ndx);
    }

    if (new_link != old_link) {
        CascadeState cascade_state(CascadeState::Mode::Strong);
        bool recurse = Base::replace_backlink(m_col_key, old_link, new_link, cascade_state);
        if (recurse)
            _impl::TableFriend::remove_recursive(*my_table, cascade_state); // Throws
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

void Dictionary::add_index(Path& path, const Index& index) const
{
    auto ndx = m_values->find_key(index.get_salt());
    auto keys = static_cast<BPlusTree<StringData>*>(m_keys.get());
    path.emplace_back(keys->get(ndx));
}

size_t Dictionary::find_index(const Index& index) const
{
    update();
    return m_values->find_key(index.get_salt());
}

UpdateStatus Dictionary::do_update_if_needed(bool allow_create) const
{
    switch (get_update_status()) {
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
        case UpdateStatus::Updated:
            return init_from_parent(allow_create);
    }
    REALM_UNREACHABLE();
}

UpdateStatus Dictionary::update_if_needed() const
{
    constexpr bool allow_create = false;
    return do_update_if_needed(allow_create);
}

void Dictionary::ensure_created()
{
    constexpr bool allow_create = true;
    if (do_update_if_needed(allow_create) == UpdateStatus::Detached) {
        throw StaleAccessor("Dictionary no longer exists");
    }
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
        throw KeyNotFound(util::format("Cannot remove key %1 from dictionary: key not found", key));
    }
}

auto Dictionary::erase(Iterator it) -> Iterator
{
    auto pos = it.m_ndx;
    CollectionBase::validate_index("erase()", pos, size());

    do_erase(pos, do_get_key(pos));
    if (pos < size())
        pos++;
    return {this, pos};
}

void Dictionary::nullify(size_t ndx)
{
    REALM_ASSERT(m_dictionary_top);
    REALM_ASSERT(ndx != realm::npos);

    if (Replication* repl = get_replication()) {
        auto key = do_get_key(ndx);
        repl->dictionary_set(*this, ndx, key, Mixed());
    }

    m_values->set(ndx, Mixed());
}

bool Dictionary::nullify(ObjLink target_link)
{
    size_t ndx = find_first(target_link);
    if (ndx != realm::not_found) {
        nullify(ndx);
        return true;
    }
    else {
        // There must be a link in a nested collection
        size_t sz = size();
        for (size_t ndx = 0; ndx < sz; ndx++) {
            auto val = m_values->get(ndx);
            auto key = do_get_key(ndx);
            if (val.is_type(type_Dictionary)) {
                auto dict = get_dictionary(key.get_string());
                if (dict->nullify(target_link)) {
                    return true;
                }
            }
            if (val.is_type(type_List)) {
                auto list = get_list(key.get_string());
                if (list->nullify(target_link)) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool Dictionary::replace_link(ObjLink old_link, ObjLink replace_link)
{
    size_t ndx = find_first(old_link);
    if (ndx != realm::not_found) {
        auto key = do_get_key(ndx);
        insert(key, replace_link);
        return true;
    }
    else {
        // There must be a link in a nested collection
        size_t sz = size();
        for (size_t ndx = 0; ndx < sz; ndx++) {
            auto val = m_values->get(ndx);
            auto key = do_get_key(ndx);
            if (val.is_type(type_Dictionary)) {
                auto dict = get_dictionary(key.get_string());
                if (dict->replace_link(old_link, replace_link)) {
                    return true;
                }
            }
            if (val.is_type(type_List)) {
                auto list = get_list(key.get_string());
                if (list->replace_link(old_link, replace_link)) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool Dictionary::remove_backlinks(CascadeState& state) const
{
    size_t sz = size();
    bool recurse = false;
    for (size_t ndx = 0; ndx < sz; ndx++) {
        if (clear_backlink(ndx, state)) {
            recurse = true;
        }
    }
    return recurse;
}

size_t Dictionary::find_first(Mixed value) const
{
    return update() ? m_values->find_first(value) : realm::not_found;
}

void Dictionary::clear()
{
    if (size() > 0) {
        if (Replication* repl = get_replication()) {
            repl->dictionary_clear(*this);
        }
        CascadeState cascade_state(CascadeState::Mode::Strong);
        bool recurse = remove_backlinks(cascade_state);

        // Just destroy the whole cluster
        m_dictionary_top->destroy_deep();
        m_dictionary_top.reset();

        update_child_ref(0, 0);

        if (recurse)
            _impl::TableFriend::remove_recursive(*get_table_unchecked(), cascade_state); // Throws
    }
}

UpdateStatus Dictionary::init_from_parent(bool allow_create) const
{
    Base::update_content_version();
    try {
        auto ref = Base::get_collection_ref();
        if ((ref || allow_create) && !m_dictionary_top) {
            Allocator& alloc = get_alloc();
            m_dictionary_top.reset(new Array(alloc));
            m_dictionary_top->set_parent(const_cast<Dictionary*>(this), 0);
            switch (m_key_type) {
                case type_String: {
                    m_keys.reset(new BPlusTree<StringData>(alloc));
                    break;
                }
                case type_Int: {
                    m_keys.reset(new BPlusTree<Int>(alloc));
                    break;
                }
                default:
                    break;
            }
            m_keys->set_parent(m_dictionary_top.get(), 0);
            m_values.reset(new BPlusTreeMixed(alloc));
            m_values->set_parent(m_dictionary_top.get(), 1);
        }

        if (ref) {
            m_dictionary_top->init_from_ref(ref);
            m_keys->init_from_parent();
            m_values->init_from_parent();
        }
        else {
            // dictionary detached
            if (!allow_create) {
                m_dictionary_top.reset();
                return UpdateStatus::Detached;
            }

            // Create dictionary
            m_dictionary_top->create(Array::type_HasRefs, false, 2, 0);
            m_values->create();
            m_keys->create();
            m_dictionary_top->update_parent();
        }

        return UpdateStatus::Updated;
    }
    catch (...) {
        m_dictionary_top.reset();
        throw;
    }
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
                IteratorAdapter help(keys);
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
                IteratorAdapter help(keys);
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
    if (val.is_type(type_TypedLink) && val.get<ObjKey>().is_unresolved()) {
        return {};
    }
    return val;
}

void Dictionary::do_erase(size_t ndx, Mixed key)
{
    CascadeState cascade_state(CascadeState::Mode::Strong);
    bool recurse = clear_backlink(ndx, cascade_state);

    if (recurse)
        _impl::TableFriend::remove_recursive(*get_table_unchecked(), cascade_state); // Throws

    if (Replication* repl = get_replication()) {
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
            break;
    }

    return {};
}

std::pair<Mixed, Mixed> Dictionary::do_get_pair(size_t ndx) const
{
    return {do_get_key(ndx), do_get(ndx)};
}

bool Dictionary::clear_backlink(size_t ndx, CascadeState& state) const
{
    auto value = m_values->get(ndx);
    if (value.is_type(type_TypedLink)) {
        return Base::remove_backlink(m_col_key, value.get_link(), state);
    }
    if (value.is_type(type_Dictionary)) {
        Dictionary dict{*const_cast<Dictionary*>(this), m_values->get_key(ndx)};
        return dict.remove_backlinks(state);
    }
    if (value.is_type(type_List)) {
        Lst<Mixed> list{*const_cast<Dictionary*>(this), m_values->get_key(ndx)};
        return list.remove_backlinks(state);
    }
    return false;
}

void Dictionary::swap_content(Array& fields1, Array& fields2, size_t index1, size_t index2)
{
    std::string buf1, buf2;

    // Swap keys
    REALM_ASSERT(m_key_type == type_String);
    ArrayString keys(get_alloc());
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
    ArrayMixed values(get_alloc());
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

StableIndex Dictionary::build_index(Mixed key) const
{
    auto it = find(key);
    int64_t index = (it != end()) ? m_values->get_key(it.index()) : 0;
    return {index};
}


void Dictionary::verify() const
{
    m_keys->verify();
    m_values->verify();
    REALM_ASSERT(m_keys->size() == m_values->size());
}

void Dictionary::get_key_type()
{
    m_key_type = get_table()->get_dictionary_key_type(m_col_key);
    if (!(m_key_type == type_String || m_key_type == type_Int))
        throw Exception(ErrorCodes::InvalidDictionaryKey, "Dictionary keys can only be strings or integers");
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

    if (auto dict_ref = Base::get_collection_ref()) {
        Allocator& alloc = get_alloc();
        DictionaryClusterTree cluster_tree(this, alloc, 0);
        if (cluster_tree.init_from_parent()) {
            // Create an empty dictionary in the old ones place
            Base::set_collection_ref(0);
            ensure_created();

            ArrayString keys(alloc); // We only support string type keys.
            ArrayMixed values(alloc);
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
            Array::destroy_deep(to_ref(dict_ref), alloc);
        }
        else {
            REALM_UNREACHABLE();
        }
    }
}

template <>
void CollectionBaseImpl<DictionaryBase>::to_json(std::ostream&, JSONOutputMode,
                                                 util::FunctionRef<void(const Mixed&)>) const
{
}

void Dictionary::to_json(std::ostream& out, JSONOutputMode output_mode,
                         util::FunctionRef<void(const Mixed&)> fn) const
{
    if (output_mode == output_mode_xjson_plus) {
        out << "{ \"$dictionary\": ";
    }
    out << "{";

    auto sz = size();
    for (size_t i = 0; i < sz; i++) {
        if (i > 0)
            out << ",";
        out << do_get_key(i) << ":";
        Mixed val = do_get(i);
        if (val.is_type(type_TypedLink)) {
            fn(val);
        }
        else if (val.is_type(type_Dictionary)) {
            DummyParent parent(this->get_table(), val.get_ref());
            Dictionary dict(parent, 0);
            dict.to_json(out, output_mode, fn);
        }
        else if (val.is_type(type_List)) {
            DummyParent parent(this->get_table(), val.get_ref());
            Lst<Mixed> list(parent, 0);
            list.to_json(out, output_mode, fn);
        }
        else {
            val.to_json(out, output_mode);
        }
    }

    out << "}";
    if (output_mode == output_mode_xjson_plus) {
        out << "}";
    }
}

ref_type Dictionary::get_collection_ref(Index index, CollectionType type) const
{
    auto ndx = m_values->find_key(index.get_salt());
    if (ndx != realm::not_found) {
        auto val = m_values->get(ndx);
        if (val.is_type(DataType(int(type)))) {
            return val.get_ref();
        }
        throw realm::IllegalOperation(util::format("Not a %1", type));
    }
    throw StaleAccessor("This collection is no more");
}

bool Dictionary::check_collection_ref(Index index, CollectionType type) const noexcept
{
    auto ndx = m_values->find_key(index.get_salt());
    if (ndx != realm::not_found) {
        return m_values->get(ndx).is_type(DataType(int(type)));
    }
    return false;
}

void Dictionary::set_collection_ref(Index index, ref_type ref, CollectionType type)
{
    auto ndx = m_values->find_key(index.get_salt());
    if (ndx == realm::not_found) {
        throw StaleAccessor("Collection has been deleted");
    }
    m_values->set(ndx, Mixed(ref, type));
}

LinkCollectionPtr Dictionary::clone_as_obj_list() const
{
    if (get_value_data_type() == type_Link) {
        return std::make_unique<DictionaryLinkValues>(*this);
    }
    return nullptr;
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
