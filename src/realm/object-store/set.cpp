#include <realm/object-store/set.hpp>
#include <realm/object-store/impl/list_notifier.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/shared_realm.hpp>

#include <realm/set.hpp>

namespace realm::object_store {

Set::Set() noexcept = default;
Set::~Set() = default;
Set::Set(const Set&) = default;
Set::Set(Set&&) = default;
Set& Set::operator=(const Set&) = default;
Set& Set::operator=(Set&&) = default;

Set::Set(std::shared_ptr<Realm> r, const Obj& parent_obj, ColKey col)
    : m_realm(std::move(r))
    , m_type(ObjectSchema::from_core_type(col) & ~PropertyType::Set)
    , m_set_base(parent_obj.get_setbase_ptr(col))
{
}

realm::ObjKey Set::get_parent_object_key() const noexcept
{
    return m_set_base->get_key();
}

realm::ColKey Set::get_parent_column_key() const noexcept
{
    return m_set_base->get_col_key();
}

realm::TableKey Set::get_parent_table_key() const noexcept
{
    return m_set_base->get_table()->get_key();
}

bool Set::is_valid() const
{
    if (!m_realm)
        return false;
    m_realm->verify_thread();
    if (!m_realm->is_in_read_transaction())
        return false;
    return m_set_base->is_attached();
}

void Set::verify_attached() const
{
    if (!is_valid()) {
        throw InvalidatedException();
    }
}

void Set::verify_in_transaction() const
{
    verify_attached();
    m_realm->verify_in_write();
}

size_t Set::size() const
{
    verify_attached();
    return m_set_base->size();
}

template <class T>
size_t Set::find(const T& value) const
{
    verify_attached();
    return as<T>().find(value);
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

util::Optional<Mixed> Set::max(ColKey column) const
{
    return util::none;
}
util::Optional<Mixed> Set::min(ColKey column) const
{
    return util::none;
}
util::Optional<Mixed> Set::average(ColKey column) const
{
    return util::none;
}

bool Set::operator==(const Set& rhs) const noexcept
{
    return true;
}

Results Set::as_results() const
{
    return {};
}
Results Set::snapshot() const
{
    return {};
}

Results Set::sort(SortDescriptor order) const
{
    return {};
}
Results Set::sort(const std::vector<std::pair<std::string, bool>>& keypaths) const
{
    return {};
}
Results Set::filter(Query q) const
{
    return {};
}

Set Set::freeze(const std::shared_ptr<Realm>& realm) const
{
    return *this;
}
bool Set::is_frozen() const noexcept
{
    return true;
}

NotificationToken Set::add_notification_callback(CollectionChangeCallback cb) & {}

#define REALM_PRIMITIVE_SET_TYPE(T)                                                                                  \
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

template <>
size_t Set::find<int>(const int& value) const
{
    return find(int64_t(value));
}

} // namespace realm::object_store
