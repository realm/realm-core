////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#ifndef REALM_OS_OBJECT_ACCESSOR_IMPL_HPP
#define REALM_OS_OBJECT_ACCESSOR_IMPL_HPP

#include "object_accessor.hpp"

#include "util/any.hpp"

namespace realm {
using AnyDict = std::map<std::string, util::Any>;
using AnyVector = std::vector<util::Any>;

struct CppContext {
    std::shared_ptr<Realm> realm;
    const ObjectSchema* object_schema;

    CppContext() = default;
    CppContext(std::shared_ptr<Realm> realm) : realm(std::move(realm)) { }
    CppContext(CppContext& c, realm::Property const& prop)
    : realm(c.realm)
    , object_schema(&*realm->schema().find(prop.object_type))
    { }

    util::Optional<util::Any> value_for_property(util::Any& dict,
                                                 const std::string &prop_name, size_t) const
    {
        auto const& v = any_cast<AnyDict&>(dict);
        auto it = v.find(prop_name);
        return it == v.end() ? util::none : util::make_optional(it->second);
    }

    template<typename Func>
    void enumerate_list(util::Any& value, Func&& fn) {
        for (auto&& v : any_cast<AnyVector&>(value))
            fn(v);
    }

    util::Optional<util::Any>
    default_value_for_property(Realm*, ObjectSchema const&, std::string const&) const
    {
        return util::none;
    }

    template<typename T>
    T unbox(util::Any& v, bool = false, bool = false) const { return any_cast<T>(v); }

    util::Any box(BinaryData v) { return std::string(v); }
    util::Any box(bool v) { return v; }
    util::Any box(double v) { return v; }
    util::Any box(float v) { return v; }
    util::Any box(long long v) { return v; }
    util::Any box(StringData v) { return std::string(v); }
    util::Any box(Timestamp v) { return v; }
    util::Any box(List v) { return v; }
    util::Any box(TableRef v) { return v; }
    util::Any box(Results v) { return v; }
    util::Any box(Object v) { return v; }
    util::Any box(Mixed) { REALM_TERMINATE("not supported"); }

    bool is_null(util::Any const& v) const noexcept { return !v.has_value(); }
    util::Any null_value() const noexcept { return {}; }

    void will_change(Object const&, Property const&) {}
    void did_change() {}
    std::string print(util::Any const&) const { return "not implemented"; }
    bool allow_missing(util::Any const&) const { return false; }
};

template<>
inline StringData CppContext::unbox(util::Any& v, bool, bool) const
{
    if (!v.has_value())
        return StringData();
    auto value = any_cast<std::string&>(v);
    return StringData(value.c_str(), value.size());
}

template<>
inline BinaryData CppContext::unbox(util::Any& v, bool, bool) const
{
    if (!v.has_value())
        return BinaryData();
    auto value = any_cast<std::string&>(v);
    return BinaryData(value.c_str(), value.size());
}

template<>
inline RowExpr CppContext::unbox(util::Any& v, bool create, bool update) const
{
    if (auto object = any_cast<Object>(&v))
        return object->row();
    if (auto row = any_cast<RowExpr>(&v))
        return *row;
    if (!create)
        return RowExpr();

    return Object::create(const_cast<CppContext&>(*this), realm, *object_schema, v, update).row();
}

template<>
inline util::Optional<bool> CppContext::unbox(util::Any& v, bool, bool) const
{
    return v.has_value() ? util::make_optional(unbox<bool>(v)) : util::none;
}

template<>
inline util::Optional<int64_t> CppContext::unbox(util::Any& v, bool, bool) const
{
    return v.has_value() ? util::make_optional(unbox<int64_t>(v)) : util::none;
}

template<>
inline util::Optional<double> CppContext::unbox(util::Any& v, bool, bool) const
{
    return v.has_value() ? util::make_optional(unbox<double>(v)) : util::none;
}

template<>
inline util::Optional<float> CppContext::unbox(util::Any& v, bool, bool) const
{
    return v.has_value() ? util::make_optional(unbox<float>(v)) : util::none;
}

template<>
inline Mixed CppContext::unbox(util::Any&, bool, bool) const
{
    throw std::logic_error("'Any' type is unsupported");
}
}

#endif /* REALM_OS_OBJECT_ACCESSOR_IMPL_HPP */
