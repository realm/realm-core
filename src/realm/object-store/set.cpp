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
#include <realm/object-store/impl/set_notifier.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/shared_realm.hpp>

namespace realm::object_store {
using namespace _impl;

Set::Set() noexcept = default;
Set::~Set() = default;
Set::Set(const Set&) = default;
Set::Set(Set&&) = default;
Set& Set::operator=(const Set&) = default;
Set& Set::operator=(Set&&) = default;

Set::Set(std::shared_ptr<Realm> r, const Obj& parent_obj, ColKey col)
    : Collection(std::move(r), parent_obj, col)
    , m_set_base(std::dynamic_pointer_cast<SetBase>(m_coll_base))
{
}

Set::Set(std::shared_ptr<Realm> r, const SetBase& set)
    : Collection(std::move(r), set)
    , m_set_base(std::dynamic_pointer_cast<SetBase>(m_coll_base))
{
}

Query Set::get_query() const
{
    verify_attached();
    if (m_type == PropertyType::Object) {
        return static_cast<LnkSet&>(*m_set_base).get_target_table()->where(as<Obj>());
    }
    throw std::runtime_error("not implemented");
}

ConstTableRef Set::get_target_table() const
{
    auto table = m_set_base->get_table();
    auto col = m_set_base->get_col_key();
    if (col.get_type() != col_type_Link)
        return nullptr;
    return table->get_link_target(col);
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
    auto result = m_set_base->max(&out_ndx);
    if (result.is_null()) {
        throw realm::Results::UnsupportedColumnTypeException(m_set_base->get_col_key(), m_set_base->get_table(),
                                                             "max");
    }
    return out_ndx == not_found ? none : util::make_optional(result);
}

util::Optional<Mixed> Set::min(ColKey col) const
{
    if (get_type() == PropertyType::Object)
        return as_results().min(col);

    size_t out_ndx = not_found;
    auto result = m_set_base->min(&out_ndx);
    if (result.is_null()) {
        throw realm::Results::UnsupportedColumnTypeException(m_set_base->get_col_key(), m_set_base->get_table(),
                                                             "min");
    }
    return out_ndx == not_found ? none : util::make_optional(result);
}

Mixed Set::sum(ColKey col) const
{
    if (get_type() == PropertyType::Object)
        return *as_results().sum(col);

    auto result = m_set_base->sum();
    if (result.is_null()) {
        throw realm::Results::UnsupportedColumnTypeException(m_set_base->get_col_key(), m_set_base->get_table(),
                                                             "sum");
    }
    return result;
}

util::Optional<Mixed> Set::average(ColKey col) const
{
    if (get_type() == PropertyType::Object)
        return as_results().average(col);
    size_t count = 0;
    auto result = m_set_base->avg(&count);
    if (result.is_null()) {
        throw realm::Results::UnsupportedColumnTypeException(m_set_base->get_col_key(), m_set_base->get_table(),
                                                             "average");
    }
    return count == 0 ? none : util::make_optional(result);
}

bool Set::operator==(const Set& rgt) const noexcept
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>() == rgt.as<std::decay_t<decltype(*t)>>();
    });
}

Results Set::snapshot() const
{
    return as_results().snapshot();
}

Results Set::sort(SortDescriptor order) const
{
    verify_attached();
    if ((m_type == PropertyType::Object)) {
        return Results(m_realm, std::dynamic_pointer_cast<LnkSet>(m_set_base), util::none, std::move(order));
    }
    else {
        DescriptorOrdering o;
        o.append_sort(order);
        return Results(m_realm, m_set_base, std::move(o));
    }
}

Results Set::sort(const std::vector<std::pair<std::string, bool>>& keypaths) const
{
    return as_results().sort(keypaths);
}

Results Set::filter(Query q) const
{
    verify_attached();
    return Results(m_realm, std::dynamic_pointer_cast<LnkSet>(m_set_base), get_query().and_query(std::move(q)));
}

Set Set::freeze(const std::shared_ptr<Realm>& frozen_realm) const
{
    return Set(frozen_realm, *frozen_realm->import_copy_of(*m_set_base));
}

NotificationToken Set::add_notification_callback(CollectionChangeCallback cb) &
{
    if (m_notifier && !m_notifier->have_callbacks())
        m_notifier.reset();
    if (!m_notifier) {
        m_notifier = std::make_shared<SetNotifier>(m_realm, *m_set_base, m_type);
        RealmCoordinator::register_notifier(m_notifier);
    }
    return {m_notifier, m_notifier->add_callback(std::move(cb))};
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
REALM_PRIMITIVE_SET_TYPE(Mixed)
REALM_PRIMITIVE_SET_TYPE(util::Optional<bool>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<int64_t>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<float>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<double>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<ObjectId>)
REALM_PRIMITIVE_SET_TYPE(util::Optional<UUID>)

#undef REALM_PRIMITIVE_SET_TYPE

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

void Set::remove_all()
{
    verify_in_transaction();
    m_set_base->clear();
}

template <>
size_t Set::find<int>(const int& value) const
{
    return find(int64_t(value));
}

template <>
Obj Set::get<Obj>(size_t row_ndx) const
{
    verify_valid_row(row_ndx);
    auto& set = as<Obj>();
    return set.get_object(row_ndx);
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

bool Set::is_subset_of(const Set &rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().is_subset_of(rhs.as<std::decay_t<decltype(*t)>>());
    });
}

bool Set::is_superset_of(const Set &rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().is_superset_of(rhs.as<std::decay_t<decltype(*t)>>());
    });
}

bool Set::intersects(const Set &rhs) const
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().intersects(rhs.as<std::decay_t<decltype(*t)>>());
    });
}

void Set::assign_intersection(const Set &rhs)
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().assign_intersection(rhs.as<std::decay_t<decltype(*t)>>());
    });
}

void Set::assign_union(const Set &rhs)
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().assign_union(rhs.as<std::decay_t<decltype(*t)>>());
    });
}

void Set::assign_difference(const Set& rhs)
{
    return dispatch([&](auto t) {
        return this->as<std::decay_t<decltype(*t)>>().assign_difference(rhs.as<std::decay_t<decltype(*t)>>());
    });
}

} // namespace realm::object_store

namespace {
size_t hash_combine()
{
    return 0;
}
template <typename T, typename... Rest>
size_t hash_combine(const T& v, Rest... rest)
{
    size_t h = hash_combine(rest...);
    h ^= std::hash<T>()(v) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
}
} // namespace

namespace std {
size_t hash<realm::object_store::Set>::operator()(realm::object_store::Set const& set) const
{
    auto& impl = *set.m_set_base;
    return hash_combine(impl.get_key().value, impl.get_table()->get_key().value, impl.get_col_key().value);
}
} // namespace std
