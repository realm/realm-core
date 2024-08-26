/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#include "realm/set.hpp"

#include "realm/array_basic.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_decimal128.hpp"
#include "realm/array_fixed_bytes.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_mixed.hpp"
#include "realm/array_string.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_typed_link.hpp"
#include "realm/replication.hpp"

#include <numeric> // std::iota

namespace realm {


/********************************** SetBase *********************************/

void SetBase::insert_repl(Replication* repl, size_t index, Mixed value) const
{
    repl->set_insert(*this, index, value);
}

void SetBase::erase_repl(Replication* repl, size_t index, Mixed value) const
{
    repl->set_erase(*this, index, value);
}

void SetBase::clear_repl(Replication* repl) const
{
    repl->set_clear(*this);
}

static std::vector<Mixed> convert_to_set(const CollectionBase& rhs)
{
    std::vector<Mixed> mixed(rhs.begin(), rhs.end());
    std::sort(mixed.begin(), mixed.end());
    mixed.erase(std::unique(mixed.begin(), mixed.end()), mixed.end());
    return mixed;
}

bool SetBase::is_subset_of(const CollectionBase& rhs) const
{
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return is_subset_of(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return is_subset_of(other_set.begin(), other_set.end());
}

template <class It1, class It2>
bool SetBase::is_subset_of(It1 first, It2 last) const
{
    return std::includes(first, last, begin(), end());
}

bool SetBase::is_strict_subset_of(const CollectionBase& rhs) const
{
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return size() != rhs.size() && is_subset_of(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return size() != other_set.size() && is_subset_of(other_set.begin(), other_set.end());
}

bool SetBase::is_superset_of(const CollectionBase& rhs) const
{
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return is_superset_of(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return is_superset_of(other_set.begin(), other_set.end());
}

template <class It1, class It2>
bool SetBase::is_superset_of(It1 first, It2 last) const
{
    return std::includes(begin(), end(), first, last);
}

bool SetBase::is_strict_superset_of(const CollectionBase& rhs) const
{
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return size() != rhs.size() && is_superset_of(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return size() != other_set.size() && is_superset_of(other_set.begin(), other_set.end());
}

bool SetBase::intersects(const CollectionBase& rhs) const
{
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return intersects(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return intersects(other_set.begin(), other_set.end());
}

template <class It1, class It2>
bool SetBase::intersects(It1 first, It2 last) const
{
    auto it = begin();
    while (it != end() && first != last) {
        if (*it < *first) {
            ++it;
        }
        else if (*first < *it) {
            ++first;
        }
        else {
            return true;
        }
    }
    return false;
}

bool SetBase::set_equals(const CollectionBase& rhs) const
{
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return size() == rhs.size() && is_subset_of(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return size() == other_set.size() && is_subset_of(other_set.begin(), other_set.end());
}

void SetBase::assign_union(const CollectionBase& rhs)
{
    if (*this == rhs) {
        return;
    }
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return assign_union(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return assign_union(other_set.begin(), other_set.end());
}

template <class It1, class It2>
void SetBase::assign_union(It1 first, It2 last)
{
    std::vector<Mixed> the_diff;
    std::set_difference(first, last, begin(), end(), std::back_inserter(the_diff));
    // 'the_diff' now contains all the elements that are in foreign set, but not in 'this'
    // Now insert those elements
    for (auto&& value : the_diff) {
        insert_any(value);
    }
}

void SetBase::assign_intersection(const CollectionBase& rhs)
{
    if (*this == rhs) {
        return;
    }
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return assign_intersection(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return assign_intersection(other_set.begin(), other_set.end());
}

template <class It1, class It2>
void SetBase::assign_intersection(It1 first, It2 last)
{
    std::vector<Mixed> intersection;
    std::set_intersection(first, last, begin(), end(), std::back_inserter(intersection));
    clear();
    // Elements in intersection comes from foreign set, so ok to use here
    for (auto&& value : intersection) {
        insert_any(value);
    }
}

void SetBase::assign_difference(const CollectionBase& rhs)
{
    if (*this == rhs) {
        clear();
        return;
    }
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return assign_difference(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return assign_difference(other_set.begin(), other_set.end());
}

template <class It1, class It2>
void SetBase::assign_difference(It1 first, It2 last)
{
    std::vector<Mixed> intersection;
    std::set_intersection(first, last, begin(), end(), std::back_inserter(intersection));
    // 'intersection' now contains all the elements that are in both foreign set and 'this'.
    // Remove those elements. The elements comes from the foreign set, so ok to refer to.
    for (auto&& value : intersection) {
        erase_any(value);
    }
}

void SetBase::assign_symmetric_difference(const CollectionBase& rhs)
{
    if (*this == rhs) {
        clear();
        return;
    }
    if (auto other_set = dynamic_cast<const SetBase*>(&rhs)) {
        return assign_symmetric_difference(other_set->begin(), other_set->end());
    }
    auto other_set = convert_to_set(rhs);
    return assign_symmetric_difference(other_set.begin(), other_set.end());
}

template <class It1, class It2>
void SetBase::assign_symmetric_difference(It1 first, It2 last)
{
    std::vector<Mixed> difference;
    std::set_difference(first, last, begin(), end(), std::back_inserter(difference));
    std::vector<Mixed> intersection;
    std::set_intersection(first, last, begin(), end(), std::back_inserter(intersection));
    // Now remove the common elements and add the differences
    for (auto&& value : intersection) {
        erase_any(value);
    }
    for (auto&& value : difference) {
        insert_any(value);
    }
}

template <>
void CollectionBaseImpl<SetBase>::to_json(std::ostream& out, JSONOutputMode output_mode,
                                          util::FunctionRef<void(const Mixed&)> fn) const
{
    if (output_mode == output_mode_xjson_plus) {
        out << "{ \"$set\": ";
    }

    out << "[";
    auto sz = size();
    for (size_t i = 0; i < sz; i++) {
        if (i > 0)
            out << ",";
        Mixed val = get_any(i);
        if (val.is_type(type_Link, type_TypedLink)) {
            fn(val);
        }
        else {
            val.to_json(out, output_mode);
        }
    }
    out << "]";
    if (output_mode == output_mode_xjson_plus) {
        out << "}";
    }
}

void SetBase::resort_range(size_t start, size_t end)
{
    if (end > size()) {
        end = size();
    }
    if (start >= end) {
        return;
    }
    std::vector<size_t> indices;
    indices.resize(end - start);
    std::iota(indices.begin(), indices.end(), 0);
    std::sort(indices.begin(), indices.end(), [&](auto a, auto b) {
        return get_any(a + start) < get_any(b + start);
    });
    for (size_t i = 0; i < indices.size(); ++i) {
        if (indices[i] != i) {
            m_tree->swap(i + start, start + indices[i]);
            auto it = std::find(indices.begin() + i, indices.end(), i);
            REALM_ASSERT(it != indices.end());
            *it = indices[i];
            indices[i] = i;
        }
    }
}

/********************************* Set<Key> *********************************/

template <>
void Set<ObjKey>::do_insert(size_t ndx, ObjKey target_key)
{
    auto origin_table = get_table_unchecked();
    auto target_table_key = origin_table->get_opposite_table_key(m_col_key);
    set_backlink(m_col_key, {target_table_key, target_key});
    tree().insert(ndx, target_key);
    if (target_key.is_unresolved()) {
        tree().set_context_flag(true);
    }
}

template <>
void Set<ObjKey>::do_erase(size_t ndx)
{
    auto origin_table = get_table_unchecked();
    auto target_table_key = origin_table->get_opposite_table_key(m_col_key);
    ObjKey old_key = get(ndx);
    CascadeState state(old_key.is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);

    bool recurse = remove_backlink(m_col_key, {target_table_key, old_key}, state);

    tree().erase(ndx);

    if (recurse) {
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
    if (old_key.is_unresolved()) {
        // We might have removed the last unresolved link - check it

        // FIXME: Exploit the fact that the values are sorted and unresolved
        // keys have a negative value.
        _impl::check_for_last_unresolved(&tree());
    }
}

template <>
void Set<ObjKey>::do_clear()
{
    size_t ndx = size();
    while (ndx--) {
        do_erase(ndx);
    }

    tree().set_context_flag(false);
}

template <>
void Set<ObjKey>::migrate()
{
}

template class Set<ObjKey>;

template <>
void Set<ObjLink>::do_insert(size_t ndx, ObjLink target_link)
{
    set_backlink(m_col_key, target_link);
    tree().insert(ndx, target_link);
}

template <>
void Set<ObjLink>::do_erase(size_t ndx)
{
    ObjLink old_link = get(ndx);
    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);

    bool recurse = remove_backlink(m_col_key, old_link, state);

    tree().erase(ndx);

    if (recurse) {
        auto table = get_table_unchecked();
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

template <>
void Set<Mixed>::do_insert(size_t ndx, Mixed value)
{
    REALM_ASSERT(!value.is_type(type_Link));
    if (value.is_type(type_TypedLink)) {
        auto target_link = value.get<ObjLink>();
        get_table_unchecked()->get_parent_group()->validate(target_link);
        set_backlink(m_col_key, target_link);
    }
    tree().insert(ndx, value);
}

template <>
void Set<Mixed>::do_erase(size_t ndx)
{
    if (Mixed old_value = get(ndx); old_value.is_type(type_TypedLink)) {
        auto old_link = old_value.get<ObjLink>();

        CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All
                                                                  : CascadeState::Mode::Strong);
        bool recurse = remove_backlink(m_col_key, old_link, state);

        tree().erase(ndx);

        if (recurse) {
            auto table = get_table_unchecked();
            _impl::TableFriend::remove_recursive(*table, state); // Throws
        }
    }
    else {
        tree().erase(ndx);
    }
}

template <>
void Set<Mixed>::do_clear()
{
    size_t ndx = size();
    while (ndx--) {
        do_erase(ndx);
    }
}

template <>
void Set<Mixed>::migrate()
{
    // We should just move all string values to be before the binary values
    size_t first_binary = size();
    BPlusTree<Mixed>& my_tree(tree());
    for (size_t n = 0; n < size(); n++) {
        if (my_tree.get(n).is_type(type_Binary)) {
            first_binary = n;
            break;
        }
    }

    for (size_t n = first_binary; n < size(); n++) {
        if (my_tree.get(n).is_type(type_String)) {
            my_tree.insert(first_binary, Mixed());
            my_tree.swap(n + 1, first_binary);
            my_tree.erase(n + 1);
            first_binary++;
        }
    }
}

template <>
void Set<Mixed>::migration_resort()
{
    // sort order of strings and binaries changed
    auto first_string = std::lower_bound(begin(), end(), StringData(""));
    auto last_binary = std::partition_point(first_string, end(), [](const Mixed& item) {
        return item.is_type(type_String, type_Binary);
    });
    resort_range(first_string.index(), last_binary.index());
}

template <>
void Set<StringData>::migration_resort()
{
    // sort order of strings changed
    resort_range(0, size());
}

template <>
void Set<BinaryData>::migration_resort()
{
    // sort order of binaries changed
    resort_range(0, size());
}

void LnkSet::remove_target_row(size_t link_ndx)
{
    // Deleting the object will automatically remove all links
    // to it. So we do not have to manually remove the deleted link
    ObjKey k = get(link_ndx);
    get_target_table()->remove_object(k);
}

void LnkSet::remove_all_target_rows()
{
    if (m_set.update()) {
        _impl::TableFriend::batch_erase_rows(*get_target_table(), m_set.tree());
    }
}

void LnkSet::to_json(std::ostream& out, JSONOutputMode mode, util::FunctionRef<void(const Mixed&)> fn) const
{
    m_set.to_json(out, mode, fn);
}

void set_sorted_indices(size_t sz, std::vector<size_t>& indices, bool ascending)
{
    indices.resize(sz);
    if (ascending) {
        std::iota(indices.begin(), indices.end(), 0);
    }
    else {
        std::iota(indices.rbegin(), indices.rend(), 0);
    }
}
} // namespace realm
