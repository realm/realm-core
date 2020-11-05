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

realm::ObjKey realm::object_store::Set::get_parent_object_key() const noexcept
{
    return m_set_base->get_key();
}

realm::ColKey realm::object_store::Set::get_parent_column_key() const noexcept
{
    return m_set_base->get_col_key();
}

realm::TableKey realm::object_store::Set::get_parent_table_key() const noexcept
{
    return m_set_base->get_table()->get_key();
}

bool realm::object_store::Set::is_valid() const noexcept
{
    if (!m_realm)
        return false;
    m_realm->verify_thread();
    if (!m_realm->is_in_read_transaction())
        return false;
    return m_set_base->is_attached();
}

void realm::object_store::Set::verify_attached() const
{
    if (!is_valid()) {
        throw InvalidatedException();
    }
}

void realm::object_store::Set::verify_in_transaction() const
{
    verify_attached();
    m_realm->verify_in_write();
}

size_t realm::object_store::Set::size() const
{
    verify_attached();
    return m_set_base->size();
}

template <class T>
bool realm::object_store::Set::remove(T const&) { return true; }


template <class T>
std::pair<size_t, bool> realm::object_store::Set::insert(T value) {
    verify_in_transaction();
    return as<T>().insert(value);
}

//template <class T, class Context>
//bool realm::object_store::Set::remove(Context&, const T&) { return true; }

util::Optional<Mixed> realm::object_store::Set::max(ColKey column) const { return util::none; }
util::Optional<Mixed> realm::object_store::Set::min(ColKey column) const { return util::none; }
util::Optional<Mixed> realm::object_store::Set::average(ColKey column) const { return util::none; }

bool realm::object_store::Set::operator==(const Set& rhs) const noexcept { return true; }

Results realm::object_store::Set::as_results() const { return {}; }
Results realm::object_store::Set::snapshot() const  { return {}; }

Results realm::object_store::Set::sort(SortDescriptor order) const { return {}; }
Results realm::object_store::Set::sort(const std::vector<std::pair<std::string, bool>>& keypaths) const { return {}; }
Results realm::object_store::Set::filter(Query q) const { return {}; }

Set realm::object_store::Set::freeze(const std::shared_ptr<Realm>& realm) const { return *this; }
bool realm::object_store::Set::is_frozen() const noexcept { return true;}

NotificationToken realm::object_store::Set::add_notification_callback(CollectionChangeCallback cb) & { }

#define REALM_PRIMITIVE_SET_TYPE(T)                                                                                 \
    template bool Set::remove<T>(T const&);                                                                   \
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
} // namespace realm::object_store
