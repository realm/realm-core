////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#ifndef REALM_TEST_COLLECTION_FIXTURES_HPP
#define REALM_TEST_COLLECTION_FIXTURES_HPP

#include <catch2/catch.hpp>

#include <realm/object-store/list.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>

#include <realm/db.hpp>
#include <realm/util/any.hpp>

#include <type_traits>

namespace realm::collection_fixtures {

struct less {
    template <class T, class U>
    auto operator()(T&& a, U&& b) const noexcept
    {
        return Mixed(a).compare(Mixed(b)) < 0;
    }
};
struct greater {
    template <class T, class U>
    auto operator()(T&& a, U&& b) const noexcept
    {
        return Mixed(a).compare(Mixed(b)) > 0;
    }
};

template <PropertyType prop_type, typename T>
struct Base {
    using Type = T;
    using Wrapped = T;
    using Boxed = T;
    using AvgType = double;
    enum { is_optional = false };

    static PropertyType property_type()
    {
        return prop_type;
    }
    static util::Any to_any(T value)
    {
        return value;
    }

    template <typename Fn>
    static auto unwrap(T value, Fn&& fn)
    {
        return fn(value);
    }

    static T min()
    {
        abort();
    }
    static T max()
    {
        abort();
    }
    static T sum()
    {
        abort();
    }
    static AvgType average()
    {
        abort();
    }
    static T empty_sum_value()
    {
        return T{};
    }
    static bool can_sum()
    {
        return std::is_arithmetic<T>::value;
    }
    static bool can_average()
    {
        return std::is_arithmetic<T>::value;
    }
    static bool can_minmax()
    {
        return std::is_arithmetic<T>::value;
    }
};

struct Int : Base<PropertyType::Int, int64_t> {
    static std::vector<int64_t> values()
    {
        return {3, 1, 2};
    }
    static int64_t min()
    {
        return 1;
    }
    static int64_t max()
    {
        return 3;
    }
    static int64_t sum()
    {
        return 6;
    }
    static double average()
    {
        return 2.0;
    }
};

struct Bool : Base<PropertyType::Bool, bool> {
    static std::vector<bool> values()
    {
        return {true, false};
    }
    static bool can_sum()
    {
        return false;
    }
    static bool can_average()
    {
        return false;
    }
    static bool can_minmax()
    {
        return false;
    }
};

struct Float : Base<PropertyType::Float, float> {
    static std::vector<float> values()
    {
        return {3.3f, 1.1f, 2.2f};
    }
    static float min()
    {
        return 1.1f;
    }
    static float max()
    {
        return 3.3f;
    }
    static auto sum()
    {
        return Approx(6.6f);
    }
    static auto average()
    {
        return Approx(2.2f);
    }
};

struct Double : Base<PropertyType::Double, double> {
    static std::vector<double> values()
    {
        return {3.3, 1.1, 2.2};
    }
    static double min()
    {
        return 1.1;
    }
    static double max()
    {
        return 3.3;
    }
    static auto sum()
    {
        return Approx(6.6);
    }
    static auto average()
    {
        return Approx(2.2);
    }
};

struct String : Base<PropertyType::String, StringData> {
    using Boxed = std::string;
    static std::vector<StringData> values()
    {
        return {"c", "a", "b"};
    }
    static util::Any to_any(StringData value)
    {
        return value ? std::string(value) : util::Any();
    }
};

struct Binary : Base<PropertyType::Data, BinaryData> {
    using Boxed = std::string;
    static util::Any to_any(BinaryData value)
    {
        return value ? std::string(value) : util::Any();
    }
    static std::vector<BinaryData> values()
    {
        return {BinaryData("c", 1), BinaryData("a", 1), BinaryData("b", 1)};
    }
};

struct Date : Base<PropertyType::Date, Timestamp> {
    static std::vector<Timestamp> values()
    {
        return {Timestamp(3, 3), Timestamp(1, 1), Timestamp(2, 2)};
    }
    static bool can_minmax()
    {
        return true;
    }
    static Timestamp min()
    {
        return Timestamp(1, 1);
    }
    static Timestamp max()
    {
        return Timestamp(3, 3);
    }
};

struct MixedVal : Base<PropertyType::Mixed, realm::Mixed> {
    using AvgType = Decimal128;
    enum { is_optional = true };
    static std::vector<realm::Mixed> values()
    {
        return {Mixed{realm::UUID()}, Mixed{int64_t(1)},      Mixed{},
                Mixed{"hello world"}, Mixed{Timestamp(1, 1)}, Mixed{Decimal128("300")},
                Mixed{double(2.2)},   Mixed{float(3.3)}};
    }
    static PropertyType property_type()
    {
        return PropertyType::Mixed | PropertyType::Nullable;
    }
    static Mixed min()
    {
        return Mixed{int64_t(1)};
    }
    static Mixed max()
    {
        return Mixed{UUID()};
    }
    static Decimal128 sum()
    {
        return Decimal128("300") + Decimal128(int64_t(1)) + Decimal128(double(2.2)) + Decimal128(float(3.3));
    }
    static Decimal128 average()
    {
        return (sum() / Decimal128(4));
    }
    static Mixed empty_sum_value()
    {
        return Mixed{0};
    }
    static bool can_sum()
    {
        return true;
    }
    static bool can_average()
    {
        return true;
    }
    static bool can_minmax()
    {
        return true;
    }
};

struct OID : Base<PropertyType::ObjectId, ObjectId> {
    static std::vector<ObjectId> values()
    {
        return {ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa")};
    }
};

struct UUID : Base<PropertyType::UUID, realm::UUID> {
    static std::vector<realm::UUID> values()
    {
        return {realm::UUID("3b241101-e2bb-4255-8caf-4136c566a962"),
                realm::UUID("3b241101-a2b3-4255-8caf-4136c566a999")};
    }
};

struct Decimal : Base<PropertyType::Decimal, Decimal128> {
    using AvgType = Decimal128;
    static std::vector<Decimal128> values()
    {
        return {Decimal128("876.54e32"), Decimal128("123.45e6")};
    }
    static Decimal128 min()
    {
        return Decimal128("123.45e6");
    }
    static Decimal128 max()
    {
        return Decimal128("876.54e32");
    }
    static Decimal128 sum()
    {
        return Decimal128("123.45e6") + Decimal128("876.54e32");
    }
    static Decimal128 average()
    {
        return ((Decimal128("123.45e6") + Decimal128("876.54e32")) / Decimal128(2));
    }
    static bool can_sum()
    {
        return true;
    }
    static bool can_average()
    {
        return true;
    }
    static bool can_minmax()
    {
        return true;
    }
};

template <typename BaseT>
struct BoxedOptional : BaseT {
    using Type = util::Optional<typename BaseT::Type>;
    using Boxed = Type;
    enum { is_optional = true };
    static PropertyType property_type()
    {
        return BaseT::property_type() | PropertyType::Nullable;
    }
    static std::vector<Type> values()
    {
        std::vector<Type> ret;
        for (auto v : BaseT::values())
            ret.push_back(Type(v));
        ret.push_back(util::none);
        return ret;
    }
    static auto unwrap(Type value)
    {
        return *value;
    }
    static util::Any to_any(Type value)
    {
        return value ? util::Any(*value) : util::Any();
    }

    template <typename Fn>
    static auto unwrap(Type value, Fn&& fn)
    {
        return value ? fn(*value) : fn(null());
    }
};

template <typename BaseT>
struct UnboxedOptional : BaseT {
    enum { is_optional = true };
    static PropertyType property_type()
    {
        return BaseT::property_type() | PropertyType::Nullable;
    }
    static auto values() -> decltype(BaseT::values())
    {
        auto ret = BaseT::values();
        if constexpr (std::is_same_v<BaseT, collection_fixtures::Decimal>) {
            // The default Decimal128 ctr is 0, but we want a null value
            ret.push_back(Decimal128(realm::null()));
        }
        else {
            ret.push_back(typename BaseT::Type());
        }
        return ret;
    }
};

} // namespace realm::collection_fixtures

namespace realm {
template <typename T>
bool operator==(List const& list, std::vector<T> const& values)
{
    if (list.size() != values.size())
        return false;
    for (size_t i = 0; i < values.size(); ++i) {
        if (list.get<T>(i) != values[i])
            return false;
    }
    return true;
}

template <typename T>
bool operator==(Results const& results, std::vector<T> const& values)
{
    // FIXME: this is only necessary because Results::size() and ::get() are not const
    Results copy{results};
    if (copy.size() != values.size())
        return false;
    for (size_t i = 0; i < values.size(); ++i) {
        if (copy.get<T>(i) != values[i])
            return false;
    }
    return true;
}

} // namespace realm

#endif // REALM_TEST_COLLECTION_FIXTURES_HPP
