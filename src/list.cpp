////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#include "list.hpp"

#include "impl/list_notifier.hpp"
#include "impl/primitive_list_notifier.hpp"
#include "impl/realm_coordinator.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "shared_realm.hpp"

namespace {
template<typename T>
struct ListType {
    using type = realm::Lst<T>;
};

template<>
struct ListType<realm::Obj> {
    using type = realm::LnkLst;
};
}

namespace realm {
using namespace realm::_impl;

List::List() noexcept = default;
List::~List() = default;

//List::List(const List&) = default;
//List& List::operator=(const List&) = default;
List::List(List&&) = default;
List& List::operator=(List&&) = default;

List::List(std::shared_ptr<Realm> r, Obj& parent_obj, ColKey col)
: m_realm(std::move(r))
, m_type(ObjectSchema::from_core_type(*parent_obj.get_table(), col) & ~PropertyType::Array)
{
    switch_on_type(m_type, [&](auto t) {
        using T = std::decay_t<decltype(*t)>;
        m_list_base = std::make_unique<typename ListType<T>::type>(parent_obj, col);
    });
}

static StringData object_name(Table const& table)
{
    return ObjectStore::object_type_for_table_name(table.get_name());
}

ObjectSchema const& List::get_object_schema() const
{
    verify_attached();
    REALM_ASSERT(get_type() == PropertyType::Object);

    if (!m_object_schema) {
        auto object_type = object_name(static_cast<LnkLst&>(*m_list_base).get_target_table());
        auto it = m_realm->schema().find(object_type);
        REALM_ASSERT(it != m_realm->schema().end());
        m_object_schema = &*it;
    }
    return *m_object_schema;
}

Query List::get_query() const
{
    verify_attached();
    return Query();
//    return m_link_view ? m_table->where(m_link_view) : m_table->where();
}

ObjKey List::get_parent_object_key() const
{
    verify_attached();
    return m_list_base->get_key();
}

void List::verify_valid_row(size_t row_ndx, bool insertion) const
{
    size_t s = size();
    if (row_ndx > s || (!insertion && row_ndx == s)) {
        throw OutOfBoundsIndexException{row_ndx, s + insertion};
    }
}

void List::validate(Obj obj) const
{
    if (!obj.is_valid())
        throw std::invalid_argument("Object has been deleted or invalidated");
    auto& target = static_cast<LnkLst&>(*m_list_base).get_target_table();
    if (obj.get_table() != &target)
        throw std::invalid_argument(util::format("Object of type (%1) does not match List type (%2)",
                                                 object_name(*obj.get_table()),
                                                 object_name(target)));
}

bool List::is_valid() const
{
    if (!m_realm)
        return false;
    m_realm->verify_thread();
    if (!m_realm->is_in_read_transaction())
        return false;
    return m_list_base->is_attached();
}

void List::verify_attached() const
{
    if (!is_valid()) {
        throw InvalidatedException();
    }
}

void List::verify_in_transaction() const
{
    verify_attached();
    m_realm->verify_in_write();
}

size_t List::size() const
{
    verify_attached();
    return m_list_base->size();
}

template<typename T>
T List::get(size_t row_ndx) const
{
    verify_valid_row(row_ndx);
    return as<T>().get(row_ndx);
}

template<>
Obj List::get(size_t row_ndx) const
{
    verify_valid_row(row_ndx);
    auto& list = as<Obj>();
    return list.get_target_table().get_object(list.get(row_ndx));
}

template<typename T>
size_t List::find(T const& value) const
{
    verify_attached();
    return as<T>().find_first(value);
}

template<>
size_t List::find(Obj const& o) const
{
    verify_attached();
    if (!o.is_valid())
        return not_found;
    validate(o);

    return as<Obj>().ConstLstIf<ObjKey>::find_first(o.get_key());
}

size_t List::find(Query&& q) const
{
    verify_attached();
    #if 0
    if (m_link_view) {
        size_t index = get_query().and_query(std::move(q)).find();
        return index == not_found ? index : m_link_view->find(index);
    }
    return q.find();
    #endif
    return 0;
}

template<typename T>
void List::add(T value)
{
    verify_in_transaction();
    as<T>().add(value);
}

template<typename T>
void List::insert(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx, true);
    as<T>().insert(row_ndx, value);
}

void List::move(size_t source_ndx, size_t dest_ndx)
{
    verify_in_transaction();
    verify_valid_row(source_ndx);
    verify_valid_row(dest_ndx); // Can't be one past end due to removing one earlier
    if (source_ndx == dest_ndx)
        return;

    m_list_base->move(source_ndx, dest_ndx);
}

void List::remove(size_t row_ndx)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    m_list_base->remove(row_ndx, row_ndx + 1);
}

void List::remove_all()
{
    verify_in_transaction();
    m_list_base->clear();
}

template<typename T>
void List::set(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
//    validate(row);
    as<T>().set(row_ndx, value);
}

void List::swap(size_t ndx1, size_t ndx2)
{
    verify_in_transaction();
    verify_valid_row(ndx1);
    verify_valid_row(ndx2);
    m_list_base->swap(ndx1, ndx2);
}

void List::delete_at(size_t row_ndx)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    if (m_type == PropertyType::Object)
        as<Obj>().remove_target_row(row_ndx);
    else
        m_list_base->remove(row_ndx, row_ndx + 1);
}

void List::delete_all()
{
    verify_in_transaction();
    if (m_type == PropertyType::Object)
        as<Obj>().remove_all_target_rows();
    else
        m_list_base->clear();
}

Results List::sort(SortDescriptor order) const
{
    verify_attached();
    #if 0
    if (m_link_view)
        return Results(m_realm, m_link_view, util::none, std::move(order));

    DescriptorOrdering new_order;
    new_order.append_sort(std::move(order));
    return Results(m_realm, get_query(), std::move(new_order));
    #endif
    return Results();
}

Results List::sort(std::vector<std::pair<std::string, bool>> const& keypaths) const
{
    return as_results().sort(keypaths);
}

Results List::filter(Query q) const
{
    verify_attached();
    #if 0
    if (m_link_view)
        return Results(m_realm, m_link_view, get_query().and_query(std::move(q)));
    return Results(m_realm, get_query().and_query(std::move(q)));
    #endif
    return Results();
}

Results List::as_results() const
{
    verify_attached();
    return Results(m_realm, *m_list_base);
}

Results List::snapshot() const
{
    return as_results().snapshot();
}

util::Optional<Mixed> List::max(size_t column)
{
    return as_results().max(column);
}

util::Optional<Mixed> List::min(size_t column)
{
    return as_results().min(column);
}

Mixed List::sum(size_t column)
{
    // Results::sum() returns none only for Mode::Empty Results, so we can
    // safely ignore that possibility here
    return *as_results().sum(column);
}

util::Optional<double> List::average(size_t column)
{
    return as_results().average(column);
}

bool List::operator==(List const& rgt) const noexcept
{
    return m_list_base->get_key() == rgt.m_list_base->get_key()
        && m_list_base->get_col_key() == rgt.m_list_base->get_col_key();
    return false;
}

NotificationToken List::add_notification_callback(CollectionChangeCallback cb) &
{
    verify_attached();
    // Adding a new callback to a notifier which had all of its callbacks
    // removed does not properly reinitialize the notifier. Work around this by
    // recreating it instead.
    // FIXME: The notifier lifecycle here is dumb (when all callbacks are removed
    // from a notifier a zombie is left sitting around uselessly) and should be
    // cleaned up.
    if (m_notifier && !m_notifier->have_callbacks())
        m_notifier.reset();
    #if 0
    if (!m_notifier) {
        if (get_type() == PropertyType::Object)
            m_notifier = std::static_pointer_cast<_impl::CollectionNotifier>(std::make_shared<ListNotifier>(m_link_view, m_realm));
        else
            m_notifier = std::static_pointer_cast<_impl::CollectionNotifier>(std::make_shared<PrimitiveListNotifier>(m_table, m_realm));
        RealmCoordinator::register_notifier(m_notifier);
    }
    return {m_notifier, m_notifier->add_callback(std::move(cb))};
    #endif
    return {nullptr, 0};
}

List::OutOfBoundsIndexException::OutOfBoundsIndexException(size_t r, size_t c)
: std::out_of_range(util::format("Requested index %1 greater than max %2", r, c - 1))
, requested(r), valid_count(c) {}

#define REALM_PRIMITIVE_LIST_TYPE(T) \
    template T List::get<T>(size_t) const; \
    template size_t List::find<T>(T const&) const; \
    template void List::add<T>(T); \
    template void List::insert<T>(size_t, T); \
    template void List::set<T>(size_t, T);

REALM_PRIMITIVE_LIST_TYPE(bool)
REALM_PRIMITIVE_LIST_TYPE(int64_t)
REALM_PRIMITIVE_LIST_TYPE(float)
REALM_PRIMITIVE_LIST_TYPE(double)
REALM_PRIMITIVE_LIST_TYPE(StringData)
REALM_PRIMITIVE_LIST_TYPE(BinaryData)
REALM_PRIMITIVE_LIST_TYPE(Timestamp)
//REALM_PRIMITIVE_LIST_TYPE(util::Optional<bool>)
REALM_PRIMITIVE_LIST_TYPE(util::Optional<int64_t>)
//REALM_PRIMITIVE_LIST_TYPE(util::Optional<float>)
//REALM_PRIMITIVE_LIST_TYPE(util::Optional<double>)

#undef REALM_PRIMITIVE_LIST_TYPE
} // namespace realm

namespace std {
size_t hash<realm::List>::operator()(realm::List const& list) const
{
//    return std::hash<void*>()(list.m_link_view ? list.m_link_view.get() : (void*)list.m_table.get());
    return 0;
}
}
