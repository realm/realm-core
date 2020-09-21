#include "set.hpp"

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

} // namespace realm::object_store