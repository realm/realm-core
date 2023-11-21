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


#include "realm/list.hpp"
#include "realm/cluster_tree.hpp"
#include "realm/array_basic.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_decimal128.hpp"
#include "realm/array_fixed_bytes.hpp"
#include "realm/array_typed_link.hpp"
#include "realm/array_mixed.hpp"
#include "realm/column_type_traits.hpp"
#include "realm/object_id.hpp"
#include "realm/table.hpp"
#include "realm/table_view.hpp"
#include "realm/group.hpp"
#include "realm/replication.hpp"
#include "realm/dictionary.hpp"
#include "realm/index_string.hpp"
#include "realm/util/bson/bson.hpp"

namespace realm {

/****************************** Lst aggregates *******************************/

namespace {
void do_sort(std::vector<size_t>& indices, size_t size, util::FunctionRef<bool(size_t, size_t)> comp)
{
    auto old_size = indices.size();
    indices.reserve(size);
    if (size < old_size) {
        // If list size has decreased, we have to start all over
        indices.clear();
        old_size = 0;
    }
    for (size_t i = old_size; i < size; i++) {
        // If list size has increased, just add the missing indices
        indices.push_back(i);
    }

    auto b = indices.begin();
    auto e = indices.end();
    std::sort(b, e, comp);
}
} // anonymous namespace

template <class T>
void Lst<T>::sort(std::vector<size_t>& indices, bool ascending) const
{
    update();

    auto tree = m_tree.get();
    if (ascending) {
        do_sort(indices, size(), [tree](size_t i1, size_t i2) {
            return tree->get(i1) < tree->get(i2);
        });
    }
    else {
        do_sort(indices, size(), [tree](size_t i1, size_t i2) {
            return tree->get(i1) > tree->get(i2);
        });
    }
}

// std::unique, but leaving the minimum value rather than the first found value
// for runs of duplicates. This makes distinct stable without relying on a
// stable sort, which makes it easier to write tests and avoids surprising results
// where distinct appears to change the order of elements
template <class Iterator, class Predicate>
static Iterator min_unique(Iterator first, Iterator last, Predicate pred)
{
    if (first == last) {
        return first;
    }

    Iterator result = first;
    while (++first != last) {
        bool equal = pred(*result, *first);
        if ((equal && *result > *first) || (!equal && ++result != first))
            *result = *first;
    }
    return ++result;
}

template <class T>
void Lst<T>::distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order) const
{
    indices.clear();
    sort(indices, sort_order.value_or(true));
    if (indices.empty()) {
        return;
    }

    auto tree = m_tree.get();
    auto duplicates = min_unique(indices.begin(), indices.end(), [tree](size_t i1, size_t i2) noexcept {
        return tree->get(i1) == tree->get(i2);
    });

    // Erase the duplicates
    indices.erase(duplicates, indices.end());

    if (!sort_order) {
        // Restore original order
        std::sort(indices.begin(), indices.end());
    }
}

/********************************** LstBase *********************************/

template <>
void CollectionBaseImpl<LstBase>::to_json(std::ostream& out, JSONOutputMode output_mode,
                                          util::FunctionRef<void(const Mixed&)> fn) const
{
    auto sz = size();
    out << "[";
    for (size_t i = 0; i < sz; i++) {
        if (i > 0)
            out << ",";
        Mixed val = get_any(i);
        if (val.is_type(type_TypedLink)) {
            fn(val);
        }
        else {
            val.to_json(out, output_mode);
        }
    }
    out << "]";
}

void LstBase::set(const bson::BsonArray& arr)
{
    auto this_type = get_data_type();
    clear();
    auto ndx = 0;
    for (auto& value : arr) {
        if (value.type() == bson::Bson::Type::Document) {
            REALM_ASSERT(this_type == type_Mixed);
            const bson::BsonDocument& document = static_cast<const bson::BsonDocument&>(value);
            if (auto obj_link = is_link(document)) {
                insert_any(ndx, obj_link);
            }
            else {
                insert_collection(ndx, CollectionType::Dictionary);
                get_dictionary(ndx)->set(document);
            }
        }
        else if (value.type() == bson::Bson::Type::Array) {
            REALM_ASSERT(this_type == type_Mixed);
            insert_collection(ndx, CollectionType::List);
            get_list(ndx)->LstBase::set(static_cast<const bson::BsonArray&>(value));
        }
        else {
            Mixed val(value);
            REALM_ASSERT(this_type == type_Mixed || val.is_type(this_type));
            insert_any(ndx, val);
        }
        ++ndx;
    }
}

void LstBase::to_bson(bson::BsonArray& arr) const
{
    auto sz = size();
    for (size_t ndx = 0; ndx < sz; ++ndx) {
        Mixed value = get_any(ndx);
        if (get_data_type() == type_Link) {
            arr.append(link_to_bson(value.get<ObjKey>()));
        }
        else {
            arr.append(mixed_to_bson(value));
        }
    }
}

/***************************** Lst<Stringdata> ******************************/

template <>
void Lst<StringData>::do_insert(size_t ndx, StringData value)
{
    if (auto index = get_table_unchecked()->get_string_index(m_col_key)) {
        // Inserting a value already present is idempotent
        index->insert(get_owner_key(), value);
    }
    m_tree->insert(ndx, value);
}

template <>
void Lst<StringData>::do_set(size_t ndx, StringData value)
{
    if (auto index = get_table_unchecked()->get_string_index(m_col_key)) {
        auto old_value = m_tree->get(ndx);
        size_t nb_old = 0;
        m_tree->for_all([&](StringData val) {
            if (val == old_value) {
                nb_old++;
            }
            return !(nb_old > 1);
        });

        if (nb_old == 1) {
            // Remove last one
            index->erase_string(get_owner_key(), old_value);
        }
        // Inserting a value already present is idempotent
        index->insert(get_owner_key(), value);
    }
    m_tree->set(ndx, value);
}

template <>
inline void Lst<StringData>::do_remove(size_t ndx)
{
    if (auto index = get_table_unchecked()->get_string_index(m_col_key)) {
        auto old_value = m_tree->get(ndx);
        size_t nb_old = 0;
        m_tree->for_all([&](StringData val) {
            if (val == old_value) {
                nb_old++;
            }
            return !(nb_old > 1);
        });

        if (nb_old == 1) {
            index->erase_string(get_owner_key(), old_value);
        }
    }
    m_tree->erase(ndx);
}

template <>
inline void Lst<StringData>::do_clear()
{
    if (auto index = get_table_unchecked()->get_string_index(m_col_key)) {
        index->erase_list(get_owner_key(), *this);
    }
    m_tree->clear();
}

/********************************* Lst<Key> *********************************/

template <>
void Lst<ObjKey>::do_set(size_t ndx, ObjKey target_key)
{
    auto origin_table = get_table_unchecked();
    auto target_table_key = origin_table->get_opposite_table_key(m_col_key);
    ObjKey old_key = this->get(ndx);
    CascadeState state(CascadeState::Mode::Strong);
    bool recurse = replace_backlink(m_col_key, {target_table_key, old_key}, {target_table_key, target_key}, state);

    m_tree->set(ndx, target_key);

    if (recurse) {
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
    if (target_key.is_unresolved()) {
        if (!old_key.is_unresolved())
            m_tree->set_context_flag(true);
    }
    else if (old_key.is_unresolved()) {
        // We might have removed the last unresolved link - check it
        _impl::check_for_last_unresolved(m_tree.get());
    }
}

template <>
void Lst<ObjKey>::do_insert(size_t ndx, ObjKey target_key)
{
    auto origin_table = get_table_unchecked();
    auto target_table_key = origin_table->get_opposite_table_key(m_col_key);
    set_backlink(m_col_key, {target_table_key, target_key});
    m_tree->insert(ndx, target_key);
    if (target_key.is_unresolved()) {
        m_tree->set_context_flag(true);
    }
}

template <>
void Lst<ObjKey>::do_remove(size_t ndx)
{
    auto origin_table = get_table_unchecked();
    auto target_table_key = origin_table->get_opposite_table_key(m_col_key);
    ObjKey old_key = get(ndx);
    CascadeState state(old_key.is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);

    bool recurse = remove_backlink(m_col_key, {target_table_key, old_key}, state);

    m_tree->erase(ndx);

    if (recurse) {
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
    if (old_key.is_unresolved()) {
        // We might have removed the last unresolved link - check it
        _impl::check_for_last_unresolved(m_tree.get());
    }
}

template <>
void Lst<ObjKey>::do_clear()
{
    auto origin_table = get_table_unchecked();
    TableRef target_table = get_obj().get_target_table(m_col_key);

    size_t sz = size();
    if (!target_table->is_embedded()) {
        size_t ndx = sz;
        while (ndx--) {
            do_set(ndx, null_key);
            m_tree->erase(ndx);
        }
        m_tree->set_context_flag(false);
        return;
    }

    TableKey target_table_key = target_table->get_key();
    ColKey backlink_col = origin_table->get_opposite_column(m_col_key);

    CascadeState state;

    typedef _impl::TableFriend tf;
    for (size_t ndx = 0; ndx < sz; ++ndx) {
        ObjKey target_key = m_tree->get(ndx);
        Obj target_obj = target_table->get_object(target_key);
        target_obj.remove_one_backlink(backlink_col, get_obj().get_key()); // Throws
        // embedded objects should only have one incoming link
        REALM_ASSERT_EX(target_obj.get_backlink_count() == 0, target_obj.get_backlink_count());
        state.m_to_be_deleted.emplace_back(target_table_key, target_key);
    }

    m_tree->clear();
    m_tree->set_context_flag(false);

    tf::remove_recursive(*origin_table, state); // Throws
}

template <>
void Lst<ObjLink>::do_set(size_t ndx, ObjLink target_link)
{
    ObjLink old_link = get(ndx);
    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);
    bool recurse = replace_backlink(m_col_key, old_link, target_link, state);

    m_tree->set(ndx, target_link);

    if (recurse) {
        auto origin_table = get_table_unchecked();
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
}

template <>
void Lst<ObjLink>::do_insert(size_t ndx, ObjLink target_link)
{
    set_backlink(m_col_key, target_link);
    m_tree->insert(ndx, target_link);
}

template <>
void Lst<ObjLink>::do_remove(size_t ndx)
{
    ObjLink old_link = get(ndx);
    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);

    bool recurse = remove_backlink(m_col_key, old_link, state);

    m_tree->erase(ndx);

    if (recurse) {
        auto table = get_table_unchecked();
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

/******************************** Lst<Mixed> *********************************/

bool Lst<Mixed>::init_from_parent(bool allow_create) const
{
    if (!m_tree) {
        m_tree.reset(new BPlusTreeMixed(get_alloc()));
        const ArrayParent* parent = this;
        m_tree->set_parent(const_cast<ArrayParent*>(parent), 0);
    }
    try {
        auto ref = Base::get_collection_ref();
        if (ref) {
            m_tree->init_from_ref(ref);
        }
        else {
            if (!allow_create) {
                m_tree->detach();
                return false;
            }

            // The ref in the column was NULL, create the tree in place.
            m_tree->create();
            REALM_ASSERT(m_tree->is_attached());
        }
    }
    catch (...) {
        m_tree->detach();
        throw;
    }

    return true;
}

UpdateStatus Lst<Mixed>::update_if_needed_with_status() const
{
    auto status = Base::get_update_status();
    switch (status) {
        case UpdateStatus::Detached: {
            m_tree.reset();
            return UpdateStatus::Detached;
        }
        case UpdateStatus::NoChange:
            if (m_tree && m_tree->is_attached()) {
                return UpdateStatus::NoChange;
            }
            // The tree has not been initialized yet for this accessor, so
            // perform lazy initialization by treating it as an update.
            [[fallthrough]];
        case UpdateStatus::Updated: {
            bool attached = init_from_parent(false);
            Base::update_content_version();
            CollectionParent::m_parent_version++;
            return attached ? UpdateStatus::Updated : UpdateStatus::Detached;
        }
    }
    REALM_UNREACHABLE();
}

size_t Lst<Mixed>::find_first(const Mixed& value) const
{
    if (!update())
        return not_found;

    if (value.is_null()) {
        auto ndx = m_tree->find_first(value);
        auto size = ndx == not_found ? m_tree->size() : ndx;
        for (size_t i = 0; i < size; ++i) {
            if (m_tree->get(i).is_unresolved_link())
                return i;
        }
        return ndx;
    }
    return m_tree->find_first(value);
}

Mixed Lst<Mixed>::set(size_t ndx, Mixed value)
{
    // get will check for ndx out of bounds
    Mixed old = do_get(ndx, "set()");
    if (Replication* repl = Base::get_replication()) {
        repl->list_set(*this, ndx, value);
    }
    if (!(old.is_same_type(value) && old == value)) {
        do_set(ndx, value);
        bump_content_version();
    }
    return old;
}

void Lst<Mixed>::insert(size_t ndx, Mixed value)
{
    ensure_created();
    auto sz = size();
    CollectionBase::validate_index("insert()", ndx, sz + 1);
    if (value.is_type(type_TypedLink)) {
        get_table()->get_parent_group()->validate(value.get_link());
    }
    if (Replication* repl = Base::get_replication()) {
        repl->list_insert(*this, ndx, value, sz);
    }
    do_insert(ndx, value);
    bump_content_version();
}

void Lst<Mixed>::resize(size_t new_size)
{
    size_t current_size = size();
    if (new_size != current_size) {
        while (new_size > current_size) {
            insert_null(current_size++);
        }
        remove(new_size, current_size);
        Base::bump_both_versions();
    }
}

Mixed Lst<Mixed>::remove(size_t ndx)
{
    // get will check for ndx out of bounds
    Mixed old = do_get(ndx, "remove()");
    if (Replication* repl = Base::get_replication()) {
        repl->list_erase(*this, ndx);
    }

    do_remove(ndx);
    bump_content_version();
    return old;
}

void Lst<Mixed>::remove(size_t from, size_t to)
{
    while (from < to) {
        remove(--to);
    }
}

void Lst<Mixed>::clear()
{
    if (size() > 0) {
        if (Replication* repl = Base::get_replication()) {
            repl->list_clear(*this);
        }
        CascadeState state;
        bool recurse = remove_backlinks(state);

        m_tree->clear();

        if (recurse) {
            auto table = get_table_unchecked();
            _impl::TableFriend::remove_recursive(*table, state); // Throws
        }
        bump_content_version();
    }
}

void Lst<Mixed>::move(size_t from, size_t to)
{
    auto sz = size();
    CollectionBase::validate_index("move()", from, sz);
    CollectionBase::validate_index("move()", to, sz);

    if (from != to) {
        if (Replication* repl = Base::get_replication()) {
            repl->list_move(*this, from, to);
        }
        if (to > from) {
            to++;
        }
        else {
            from++;
        }
        // We use swap here as it handles the special case for StringData where
        // 'to' and 'from' points into the same array. In this case you cannot
        // set an entry with the result of a get from another entry in the same
        // leaf.
        m_tree->insert(to, Mixed());
        m_tree->swap(from, to);
        m_tree->erase(from);

        bump_content_version();
    }
}

void Lst<Mixed>::swap(size_t ndx1, size_t ndx2)
{
    auto sz = size();
    CollectionBase::validate_index("swap()", ndx1, sz);
    CollectionBase::validate_index("swap()", ndx2, sz);

    if (ndx1 != ndx2) {
        if (Replication* repl = Base::get_replication()) {
            LstBase::swap_repl(repl, ndx1, ndx2);
        }
        m_tree->swap(ndx1, ndx2);
        bump_content_version();
    }
}

void Lst<Mixed>::insert_collection(const PathElement& path_elem, CollectionType dict_or_list)
{
    if (dict_or_list == CollectionType::Set) {
        throw IllegalOperation("Set nested in List<Mixed> is not supported");
    }

    ensure_created();
    check_level();
    m_tree->ensure_keys();
    insert(path_elem.get_ndx(), Mixed(0, dict_or_list));
    int64_t key = generate_key(size());
    while (m_tree->find_key(key) != realm::not_found) {
        key++;
    }
    m_tree->set_key(path_elem.get_ndx(), key);
    bump_content_version();
}

void Lst<Mixed>::set_collection(const PathElement& path_elem, CollectionType dict_or_list)
{
    if (dict_or_list == CollectionType::Set) {
        throw IllegalOperation("Set nested in List<Mixed> is not supported");
    }

    auto ndx = path_elem.get_ndx();
    // get will check for ndx out of bounds
    Mixed old_val = do_get(ndx, "set_collection()");
    Mixed new_val(0, dict_or_list);

    check_level();

    if (old_val != new_val) {
        m_tree->ensure_keys();
        set(ndx, new_val);
        int64_t key = m_tree->get_key(ndx);
        if (key == 0) {
            key = generate_key(size());
            while (m_tree->find_key(key) != realm::not_found) {
                key++;
            }
            m_tree->set_key(ndx, key);
        }
        bump_content_version();
    }
}

DictionaryPtr Lst<Mixed>::get_dictionary(const PathElement& path_elem) const
{
    update();
    auto weak = const_cast<Lst<Mixed>*>(this)->weak_from_this();
    auto shared = weak.expired() ? std::make_shared<Lst<Mixed>>(*this) : weak.lock();
    DictionaryPtr ret = std::make_shared<Dictionary>(m_col_key, get_level() + 1);
    ret->set_owner(shared, m_tree->get_key(path_elem.get_ndx()));
    return ret;
}

std::shared_ptr<Lst<Mixed>> Lst<Mixed>::get_list(const PathElement& path_elem) const
{
    update();
    auto weak = const_cast<Lst<Mixed>*>(this)->weak_from_this();
    auto shared = weak.expired() ? std::make_shared<Lst<Mixed>>(*this) : weak.lock();
    std::shared_ptr<Lst<Mixed>> ret = std::make_shared<Lst<Mixed>>(m_col_key, get_level() + 1);
    ret->set_owner(shared, m_tree->get_key(path_elem.get_ndx()));
    return ret;
}

void Lst<Mixed>::do_set(size_t ndx, Mixed value)
{
    ObjLink old_link;
    ObjLink target_link;
    Mixed old_value = m_tree->get(ndx);

    if (old_value.is_type(type_TypedLink)) {
        old_link = old_value.get<ObjLink>();
    }
    if (value.is_type(type_TypedLink)) {
        target_link = value.get<ObjLink>();
        get_table_unchecked()->get_parent_group()->validate(target_link);
    }

    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);
    bool recurse = Base::replace_backlink(m_col_key, old_link, target_link, state);

    m_tree->set(ndx, value);

    if (recurse) {
        auto origin_table = get_table_unchecked();
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
}

void Lst<Mixed>::do_insert(size_t ndx, Mixed value)
{
    if (value.is_type(type_TypedLink)) {
        Base::set_backlink(m_col_key, value.get<ObjLink>());
    }

    m_tree->insert(ndx, value);
}

void Lst<Mixed>::do_remove(size_t ndx)
{
    CascadeState state;
    bool recurse = clear_backlink(ndx, state);

    m_tree->erase(ndx);

    if (recurse) {
        auto table = get_table_unchecked();
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

void Lst<Mixed>::sort(std::vector<size_t>& indices, bool ascending) const
{
    update();

    auto tree = m_tree.get();
    if (ascending) {
        do_sort(indices, size(), [tree](size_t i1, size_t i2) {
            return unresolved_to_null(tree->get(i1)) < unresolved_to_null(tree->get(i2));
        });
    }
    else {
        do_sort(indices, size(), [tree](size_t i1, size_t i2) {
            return unresolved_to_null(tree->get(i1)) > unresolved_to_null(tree->get(i2));
        });
    }
}

void Lst<Mixed>::distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order) const
{
    indices.clear();
    sort(indices, sort_order.value_or(true));
    if (indices.empty()) {
        return;
    }

    auto tree = m_tree.get();
    auto duplicates = min_unique(indices.begin(), indices.end(), [tree](size_t i1, size_t i2) noexcept {
        return unresolved_to_null(tree->get(i1)) == unresolved_to_null(tree->get(i2));
    });

    // Erase the duplicates
    indices.erase(duplicates, indices.end());

    if (!sort_order) {
        // Restore original order
        std::sort(indices.begin(), indices.end());
    }
}

util::Optional<Mixed> Lst<Mixed>::min(size_t* return_ndx) const
{
    if (update()) {
        return MinHelper<Mixed>::eval(*m_tree, return_ndx);
    }
    return MinHelper<Mixed>::not_found(return_ndx);
}

util::Optional<Mixed> Lst<Mixed>::max(size_t* return_ndx) const
{
    if (update()) {
        return MaxHelper<Mixed>::eval(*m_tree, return_ndx);
    }
    return MaxHelper<Mixed>::not_found(return_ndx);
}

util::Optional<Mixed> Lst<Mixed>::sum(size_t* return_cnt) const
{
    if (update()) {
        return SumHelper<Mixed>::eval(*m_tree, return_cnt);
    }
    return SumHelper<Mixed>::not_found(return_cnt);
}

util::Optional<Mixed> Lst<Mixed>::avg(size_t* return_cnt) const
{
    if (update()) {
        return AverageHelper<Mixed>::eval(*m_tree, return_cnt);
    }
    return AverageHelper<Mixed>::not_found(return_cnt);
}

void Lst<Mixed>::to_json(std::ostream& out, JSONOutputMode output_mode,
                         util::FunctionRef<void(const Mixed&)> fn) const
{
    out << "[";

    auto sz = size();
    for (size_t i = 0; i < sz; i++) {
        if (i > 0)
            out << ",";
        Mixed val = m_tree->get(i);
        if (val.is_type(type_TypedLink)) {
            fn(val);
        }
        else if (val.is_type(type_Dictionary)) {
            DummyParent parent(this->get_table(), val.get_ref());
            Dictionary dict(parent, i);
            dict.to_json(out, output_mode, fn);
        }
        else if (val.is_type(type_List)) {
            DummyParent parent(this->get_table(), val.get_ref());
            Lst<Mixed> list(parent, i);
            list.to_json(out, output_mode, fn);
        }
        else {
            val.to_json(out, output_mode);
        }
    }

    out << "]";
}

ref_type Lst<Mixed>::get_collection_ref(Index index, CollectionType type) const
{
    auto ndx = m_tree->find_key(index.get_salt());
    if (ndx != realm::not_found) {
        auto val = get(ndx);
        if (val.is_type(DataType(int(type)))) {
            return val.get_ref();
        }
        throw realm::IllegalOperation(util::format("Not a %1", type));
    }
    throw StaleAccessor("This collection is no more");
    return 0;
}

bool Lst<Mixed>::check_collection_ref(Index index, CollectionType type) const noexcept
{
    auto ndx = m_tree->find_key(index.get_salt());
    if (ndx != realm::not_found) {
        return get(ndx).is_type(DataType(int(type)));
    }
    return false;
}

void Lst<Mixed>::set_collection_ref(Index index, ref_type ref, CollectionType type)
{
    auto ndx = m_tree->find_key(index.get_salt());
    if (ndx == realm::not_found) {
        throw StaleAccessor("Collection has been deleted");
    }
    m_tree->set(ndx, Mixed(ref, type));
}

void Lst<Mixed>::add_index(Path& path, const Index& index) const
{
    auto ndx = m_tree->find_key(index.get_salt());
    REALM_ASSERT(ndx != realm::not_found);
    path.emplace_back(ndx);
}

size_t Lst<Mixed>::find_index(const Index& index) const
{
    update();
    return m_tree->find_key(index.get_salt());
}

bool Lst<Mixed>::nullify(ObjLink link)
{
    size_t ndx = find_first(link);
    if (ndx != realm::not_found) {
        if (Replication* repl = Base::get_replication()) {
            repl->list_erase(*this, ndx); // Throws
        }

        m_tree->erase(ndx);
        return true;
    }
    else {
        // There must be a link in a nested collection
        size_t sz = size();
        for (size_t ndx = 0; ndx < sz; ndx++) {
            Mixed val = m_tree->get(ndx);
            if (val.is_type(type_Dictionary)) {
                auto dict = get_dictionary(ndx);
                if (dict->nullify(link)) {
                    return true;
                }
            }
            if (val.is_type(type_List)) {
                auto list = get_list(ndx);
                if (list->nullify(link)) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool Lst<Mixed>::replace_link(ObjLink old_link, ObjLink replace_link)
{
    size_t ndx = find_first(old_link);
    if (ndx != realm::not_found) {
        set(ndx, replace_link);
        return true;
    }
    else {
        // There must be a link in a nested collection
        size_t sz = size();
        for (size_t ndx = 0; ndx < sz; ndx++) {
            Mixed val = m_tree->get(ndx);
            if (val.is_type(type_Dictionary)) {
                auto dict = get_dictionary(ndx);
                if (dict->replace_link(old_link, replace_link)) {
                    return true;
                }
            }
            if (val.is_type(type_List)) {
                auto list = get_list(ndx);
                if (list->replace_link(old_link, replace_link)) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool Lst<Mixed>::clear_backlink(size_t ndx, CascadeState& state) const
{
    Mixed value = m_tree->get(ndx);
    if (value.is_type(type_TypedLink, type_Dictionary, type_List)) {
        if (value.is_type(type_TypedLink)) {
            auto link = value.get<ObjLink>();
            if (link.get_obj_key().is_unresolved()) {
                state.m_mode = CascadeState::Mode::All;
            }
            return Base::remove_backlink(m_col_key, link, state);
        }
        else if (value.is_type(type_List)) {
            return get_list(ndx)->remove_backlinks(state);
        }
        else if (value.is_type(type_Dictionary)) {
            return get_dictionary(ndx)->remove_backlinks(state);
        }
    }
    return false;
}

bool Lst<Mixed>::remove_backlinks(CascadeState& state) const
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

bool Lst<Mixed>::update_if_needed() const
{
    auto status = update_if_needed_with_status();
    if (status == UpdateStatus::Detached) {
        throw StaleAccessor("CollectionList no longer exists");
    }
    return status == UpdateStatus::Updated;
}

/********************************** LnkLst ***********************************/

Obj LnkLst::create_and_insert_linked_object(size_t ndx)
{
    Table& t = *get_target_table();
    auto o = t.is_embedded() ? t.create_linked_object() : t.create_object();
    m_list.insert(ndx, o.get_key());
    return o;
}

Obj LnkLst::create_and_set_linked_object(size_t ndx)
{
    Table& t = *get_target_table();
    auto o = t.is_embedded() ? t.create_linked_object() : t.create_object();
    m_list.set(ndx, o.get_key());
    return o;
}

TableView LnkLst::get_sorted_view(SortDescriptor order) const
{
    TableView tv(clone_linklist());
    tv.do_sync();
    tv.sort(std::move(order));
    return tv;
}

TableView LnkLst::get_sorted_view(ColKey column_key, bool ascending) const
{
    TableView v = get_sorted_view(SortDescriptor({{column_key}}, {ascending}));
    return v;
}

void LnkLst::remove_target_row(size_t link_ndx)
{
    // Deleting the object will automatically remove all links
    // to it. So we do not have to manually remove the deleted link
    ObjKey k = get(link_ndx);
    get_target_table()->remove_object(k);
}

void LnkLst::remove_all_target_rows()
{
    if (is_attached()) {
        update_if_needed();
        _impl::TableFriend::batch_erase_rows(*get_target_table(), *m_list.m_tree);
    }
}

void LnkLst::set(const bson::BsonArray& arr)
{
    clear();
    auto ndx = 0;
    for (auto& value : arr) {
        auto target_table = get_target_table();
        ObjKey obj_key;
        if (target_table->is_embedded()) {
            Obj obj = create_and_insert_linked_object(ndx);
            if (value.type() != bson::Bson::Type::Null)
                obj.set(static_cast<const bson::BsonDocument&>(value));
        }
        else {
            m_list.insert(ndx, target_table->get_objkey_from_primary_key(Mixed(value)));
        }
        ++ndx;
    }
    update_unresolved(UpdateStatus::Updated);
}

void LnkLst::to_json(std::ostream& out, JSONOutputMode, util::FunctionRef<void(const Mixed&)> fn) const
{
    out << "[";

    auto sz = m_list.size();
    for (size_t i = 0; i < sz; i++) {
        if (i > 0)
            out << ",";
        Mixed val(m_list.get(i));
        fn(val);
    }

    out << "]";
}

void LnkLst::replace_link(ObjKey old_val, ObjKey new_val)
{
    update_if_needed();
    auto tree = m_list.m_tree.get();
    auto n = tree->find_first(old_val);
    REALM_ASSERT(n != realm::npos);
    if (Replication* repl = get_obj().get_replication()) {
        repl->list_set(m_list, n, new_val);
    }
    tree->set(n, new_val);
    m_list.bump_content_version();
    if (new_val.is_unresolved()) {
        if (!old_val.is_unresolved()) {
            tree->set_context_flag(true);
        }
    }
    else {
        _impl::check_for_last_unresolved(tree);
    }
}

// Force instantiation:
template class Lst<ObjKey>;
template class Lst<ObjLink>;
template class Lst<int64_t>;
template class Lst<bool>;
template class Lst<StringData>;
template class Lst<BinaryData>;
template class Lst<Timestamp>;
template class Lst<float>;
template class Lst<double>;
template class Lst<Decimal128>;
template class Lst<ObjectId>;
template class Lst<UUID>;
template class Lst<util::Optional<int64_t>>;
template class Lst<util::Optional<bool>>;
template class Lst<util::Optional<float>>;
template class Lst<util::Optional<double>>;
template class Lst<util::Optional<ObjectId>>;
template class Lst<util::Optional<UUID>>;

} // namespace realm
