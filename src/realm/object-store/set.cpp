////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/set.hpp>

#include <realm/object-store/impl/list_notifier.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/shared_realm.hpp>

namespace realm::object_store {

Set::Set(const Set&) = default;
Set::Set(Set&&) = default;
Set& Set::operator=(const Set&) = default;
Set& Set::operator=(Set&&) = default;

Query Set::get_query() const
{
    return get_table()->where(as<Obj>());
}

ConstTableRef Set::get_table() const
{
    verify_attached();
    if (m_type == PropertyType::Object)
        return set_base().get_target_table();
    throw std::runtime_error("not implemented");
}

template <class T>
size_t Set::find(const T& value) const
{
    verify_attached();
    return as<T>().find(value);
}

size_t Set::find(Query&& q) const
{
    verify_attached();
    if (m_type == PropertyType::Object) {
        ObjKey key = get_query().and_query(std::move(q)).find();
        return key ? as<Obj>().find_first(key) : not_found;
    }
    throw std::runtime_error("not implemented");
}

template <typename T>
T Set::get(size_t row_ndx) const
{
    verify_valid_row(row_ndx);
    return as<T>().get(row_ndx);
}

template <class T>
std::pair<size_t, bool> Set::insert(T value)
{
    verify_in_transaction();
    return as<T>().insert(value);
}

template <class T>
std::pair<size_t, bool> Set::remove(const T& value)
{
    verify_in_transaction();
    return as<T>().erase(value);
}

util::Optional<Mixed> Set::max(ColKey col) const
{
    if (get_type() == PropertyType::Object)
        return as_results().max(col);
    size_t out_ndx = not_found;
    auto result = set_base().max(&out_ndx);
    if (!result) {
        throw Results::UnsupportedColumnTypeException(set_base().get_col_key(), set_base().get_table(), "max");
    }
    return out_ndx == not_found ? none : result;
}

util::Optional<Mixed> Set::min(ColKey col) const
{
    if (get_type() == PropertyType::Object)
        return as_results().min(col);

    size_t out_ndx = not_found;
    auto result = set_base().min(&out_ndx);
    if (!result) {
        throw Results::UnsupportedColumnTypeException(set_base().get_col_key(), set_base().get_table(), "min");
    }
    return out_ndx == not_found ? none : result;
}

Mixed Set::sum(ColKey col) const
{
    if (get_type() == PropertyType::Object)
        return *as_results().sum(col);

    auto result = set_base().sum();
    if (!result) {
        throw Results::UnsupportedColumnTypeException(set_base().get_col_key(), set_base().get_table(), "sum");
    }
    return *result;
}

util::Optional<Mixed> Set::average(ColKey col) const
{
    if (get_type() == PropertyType::Object)
        return as_results().average(col);
    size_t count = 0;
    auto result = set_base().avg(&count);
    if (!result) {
        throw Results::UnsupportedColumnTypeException(set_base().get_col_key(), set_base().get_table(), "average");
    }
    return count == 0 ? none : result;
}

bool Set::operator==(const Set& rgt) const noexcept
{
    return set_base().get_table() == rgt.set_base().get_table() &&
           set_base().get_owner_key() == rgt.set_base().get_owner_key() &&
           set_base().get_col_key() == rgt.set_base().get_col_key();
}

Results Set::filter(Query q) const
{
    verify_attached();
    return Results(m_realm, std::dynamic_pointer_cast<LnkSet>(m_coll_base), get_query().and_query(std::move(q)));
}

Set Set::freeze(const std::shared_ptr<Realm>& frozen_realm) const
{
    auto frozen_set(frozen_realm->import_copy_of(*m_coll_base));
    if (frozen_set) {
        return Set(frozen_realm, std::move(frozen_set));
    }
    else {
        return Set{};
    }
}

#define REALM_PRIMITIVE_SET_TYPE(T)                                                                                  \
    template T Set::get<T>(size_t) const;                                                                            \
    template size_t Set::find<T>(const T&) const;                                                                    \
    template std::pair<size_t, bool> Set::remove<T>(T const&);                                                       \
    template std::pair<size_t, bool> Set::insert<T>(T);

REALM_PRIMITIVE_SET_TYPE(bool)
REALM_PRIMITIVE_SET_TYPE(int64_t)
REALM_PRIMITIVE_SET_TYPE(float)
REALM_PRIMITIVE_SET_TYPE(double)
REALM_PRIMITIVE_SET_TYPE(StringData)
REALM_PRIMITIVE_SET_TYPE(BinaryData)
REALM_PRIMITIVE_SET_TYPE(Timestamp)
REALM_PRIMITIVE_SET_TYPE(ObjKey)
REALM_PRIMITIVE_SET_TYPE(ObjectId)
REALM_PRIMITIVE_SET_TYPE(Decimal)
REALM_PRIMITIVE_SET_TYPE(UUID)
REALM_PRIMITIVE_SET_TYPE(util::Optional<bool>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<int64_t>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<float>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<double>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<ObjectId>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<UUID>)

#undef REALM_PRIMITIVE_SET_TYPE

template size_t Set::find<Mixed>(const Mixed&) const;
template std::pair<size_t, bool> Set::remove<Mixed>(Mixed const&);
template std::pair<size_t, bool> Set::insert<Mixed>(Mixed);

template <>
std::pair<size_t, bool> Set::insert<int>(int value)
{
    return insert(int64_t(value));
}

template <>
std::pair<size_t, bool> Set::remove<int>(const int& value)
{
    return remove(int64_t(value));
}

std::pair<size_t, bool> Set::insert_any(Mixed value)
{
    verify_in_transaction();
    return set_base().insert_any(value);
}

Mixed Set::get_any(size_t ndx) const
{
    verify_valid_row(ndx);
    auto value = set_base().get_any(ndx);
    record_audit_read(value);
    return value;
}

std::pair<size_t, bool> Set::remove_any(Mixed value)
{
    verify_in_transaction();
    return set_base().erase_any(value);
}

size_t Set::find_any(Mixed value) const
{
    return set_base().find_any(value);
}

void Set::delete_all()
{
    verify_in_transaction();
    if (m_type == PropertyType::Object)
        as<Obj>().remove_all_target_rows();
    else
        set_base().clear();
}

void Set::remove_all()
{
    verify_in_transaction();
    set_base().clear();
}

template <>
size_t Set::find<int>(const int& value) const
{
    return find(int64_t(value));
}

template <>
Mixed Set::get<Mixed>(size_t row_ndx) const
{
    verify_valid_row(row_ndx);
    auto& set = as<Mixed>();
    auto value = set.get(row_ndx);
    record_audit_read(value);
    return value;
}

template <>
Obj Set::get<Obj>(size_t row_ndx) const
{
    verify_valid_row(row_ndx);
    auto& set = as<Obj>();
    auto obj = set.get_object(row_ndx);
    record_audit_read(obj);
    return obj;
}

template <>
size_t Set::find<Obj>(const Obj& obj) const
{
    verify_attached();
    validate(obj);
    // FIXME: Handle Mixed / ObjLink
    return as<ObjKey>().find(obj.get_key());
}

template <>
std::pair<size_t, bool> Set::remove<Obj>(const Obj& obj)
{
    verify_in_transaction();
    validate(obj);
    // FIXME: Handle Mixed / ObjLink
    return as<ObjKey>().erase(obj.get_key());
}

template <>
std::pair<size_t, bool> Set::insert<Obj>(Obj obj)
{
    verify_in_transaction();
    validate(obj);
    // FIXME: Handle Mixed / ObjLink
    return as<ObjKey>().insert(obj.get_key());
}

bool Set::is_subset_of(const Collection& rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().is_subset_of(rhs.get_impl());
    });
}

bool Set::is_strict_subset_of(const Collection& rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().is_strict_subset_of(rhs.get_impl());
    });
}

bool Set::is_superset_of(const Collection& rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().is_superset_of(rhs.get_impl());
    });
}

bool Set::is_strict_superset_of(const Collection& rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().is_strict_superset_of(rhs.get_impl());
    });
}

bool Set::intersects(const Collection& rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().intersects(rhs.get_impl());
    });
}

bool Set::set_equals(const Collection& rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().set_equals(rhs.get_impl());
    });
}

void Set::assign_intersection(const Collection& rhs)
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().assign_intersection(rhs.get_impl());
    });
}

void Set::assign_union(const Collection& rhs)
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().assign_union(rhs.get_impl());
    });
}

void Set::assign_difference(const Collection& rhs)
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().assign_difference(rhs.get_impl());
    });
}

void Set::assign_symmetric_difference(const Collection& rhs)
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().assign_symmetric_difference(rhs.get_impl());
    });
}

} // namespace realm::object_store

namespace std {
size_t hash<realm::object_store::Set>::operator()(realm::object_store::Set const& set) const
{
    return set.hash();
}
} // namespace std
