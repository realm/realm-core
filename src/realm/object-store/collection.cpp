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

#include <realm/object-store/collection.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/impl/list_notifier.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>

namespace realm::object_store {

Collection::OutOfBoundsIndexException::OutOfBoundsIndexException(size_t r, size_t c)
    : std::out_of_range(util::format("Requested index %1 greater than max %2", r, c - 1))
    , requested(r)
    , valid_count(c)
{
}

Collection::Collection(PropertyType type) noexcept
    : m_type(type)
{
}

Collection::Collection(const Object& parent_obj, const Property* prop)
    : m_realm(parent_obj.get_realm())
    , m_type(prop->type)
    , m_coll_base(parent_obj.obj().get_collection_ptr(prop->column_key))
    , m_is_embedded(m_type == PropertyType::Object && m_coll_base->get_target_table()->is_embedded())
{
}

Collection::Collection(std::shared_ptr<Realm> r, const Obj& parent_obj, ColKey col)
    : m_realm(std::move(r))
    , m_type(ObjectSchema::from_core_type(col) & ~PropertyType::Collection)
    , m_coll_base(parent_obj.get_collection_ptr(col))
    , m_is_embedded(m_type == PropertyType::Object && m_coll_base->get_target_table()->is_embedded())
{
}

Collection::Collection(std::shared_ptr<Realm> r, const CollectionBase& coll)
    : m_realm(std::move(r))
    , m_type(ObjectSchema::from_core_type(coll.get_col_key()) & ~PropertyType::Collection)
    , m_coll_base(coll.clone_collection())
    , m_is_embedded(m_type == PropertyType::Object && m_coll_base->get_target_table()->is_embedded())
{
}

Collection::Collection(std::shared_ptr<Realm> r, CollectionBasePtr coll)
    : m_realm(std::move(r))
    , m_type(ObjectSchema::from_core_type(coll->get_col_key()) & ~PropertyType::Collection)
    , m_coll_base(std::move(coll))
    , m_is_embedded(m_type == PropertyType::Object && m_coll_base->get_target_table()->is_embedded())
{
}

Collection::~Collection() {}

Collection::Collection(const Collection&) = default;
Collection& Collection::operator=(const Collection&) = default;
Collection::Collection(Collection&&) = default;
Collection& Collection::operator=(Collection&&) = default;

bool Collection::is_valid() const
{
    if (!m_realm)
        return false;
    m_realm->verify_thread();
    if (!m_realm->is_in_read_transaction())
        return false;
    return m_coll_base->is_attached();
}

ObjKey Collection::get_parent_object_key() const
{
    verify_attached();
    return m_coll_base->get_owner_key();
}

ColKey Collection::get_parent_column_key() const
{
    verify_attached();
    return m_coll_base->get_col_key();
}

TableKey Collection::get_parent_table_key() const
{
    verify_attached();
    return m_coll_base->get_table()->get_key();
}

static StringData object_name(Table const& table)
{
    return ObjectStore::object_type_for_table_name(table.get_name());
}

void Collection::validate(const Obj& obj) const
{
    if (!obj.is_valid())
        throw std::invalid_argument("Object has been deleted or invalidated");
    // FIXME: This does not work for TypedLink.
    auto target = m_coll_base->get_target_table();
    if (obj.get_table() != target)
        throw std::invalid_argument(util::format("Object of type (%1) does not match List type (%2)",
                                                 object_name(*obj.get_table()), object_name(*target)));
}

void Collection::verify_attached() const
{
    if (!is_valid()) {
        std::string coll_type = "Collection";
        if (is_array(m_type))
            coll_type = "List";
        else if (is_dictionary(m_type))
            coll_type = "Dictionary";
        else if (is_set(m_type))
            coll_type = "Set";
        throw InvalidatedException(util::format("Access to invalidated %1 object", coll_type));
    }
}

void Collection::verify_in_transaction() const
{
    verify_attached();
    m_realm->verify_in_write();
}

size_t Collection::size() const
{
    verify_attached();
    return m_coll_base->size();
}

void Collection::verify_valid_row(size_t row_ndx, bool insertion) const
{
    size_t s = size();
    if (row_ndx > s || (!insertion && row_ndx == s)) {
        throw OutOfBoundsIndexException{row_ndx, s + insertion};
    }
}

const ObjectSchema& Collection::get_object_schema() const
{
    verify_attached();

    REALM_ASSERT(get_type() == PropertyType::Object);
    auto object_schema = m_object_schema.load();
    if (!object_schema) {
        auto object_type = object_name(*m_coll_base->get_target_table());
        auto it = m_realm->schema().find(object_type);
        REALM_ASSERT(it != m_realm->schema().end());
        m_object_schema = object_schema = &*it;
    }
    return *object_schema;
}

bool Collection::is_frozen() const noexcept
{
    return m_realm->is_frozen();
}

Results Collection::as_results() const
{
    verify_attached();
    return Results(m_realm, m_coll_base);
}

Results Collection::sort(SortDescriptor order) const
{
    verify_attached();
    return Results(m_realm, m_coll_base, util::none, std::move(order));
}

Results Collection::sort(std::vector<std::pair<std::string, bool>> const& keypaths) const
{
    return as_results().sort(keypaths);
}

Results Collection::snapshot() const
{
    return as_results().snapshot();
}


NotificationToken Collection::add_notification_callback(CollectionChangeCallback callback,
                                                        KeyPathArray key_path_array) &
{
    verify_attached();
    m_realm->verify_notifications_available();
    // Adding a new callback to a notifier which had all of its callbacks
    // removed does not properly reinitialize the notifier. Work around this by
    // recreating it instead.
    // FIXME: The notifier lifecycle here is dumb (when all callbacks are removed
    // from a notifier a zombie is left sitting around uselessly) and should be
    // cleaned up.
    if (m_notifier && !m_notifier->have_callbacks())
        m_notifier.reset();
    if (!m_notifier) {
        m_notifier = std::make_shared<_impl::ListNotifier>(m_realm, *m_coll_base, m_type);
        _impl::RealmCoordinator::register_notifier(m_notifier);
    }
    return {m_notifier, m_notifier->add_callback(std::move(callback), std::move(key_path_array))};
}

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

size_t Collection::hash() const noexcept
{
    auto& impl = *m_coll_base;
    return hash_combine(impl.get_owner_key().value, impl.get_table()->get_key().value, impl.get_col_key().value);
}

} // namespace realm::object_store
