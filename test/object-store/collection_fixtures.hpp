#include "property.hpp"
#include <realm/util/any.hpp>

#include <type_traits>

namespace realm::primitive_fixtures {

template <PropertyType prop_type, typename T>
struct Base {
    using Type = T;
    using Wrapped = T;
    using Boxed = T;
    using AvgType = double;

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
    static std::vector<BinaryData> values()
    {
        return {BinaryData("a", 1)};
    }
    static util::Any to_any(BinaryData value)
    {
        return value ? std::string(value) : util::Any();
    }
};

struct Date : Base<PropertyType::Date, Timestamp> {
    static std::vector<Timestamp> values()
    {
        return {Timestamp(1, 1)};
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
        return Timestamp(1, 1);
    }
};

struct OID : Base<PropertyType::ObjectId, ObjectId> {
    static std::vector<ObjectId> values()
    {
        return {ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb")};
    }
};

struct Decimal : Base<PropertyType::Decimal, Decimal128> {
    using AvgType = Decimal128;
    static std::vector<Decimal128> values()
    {
        return {Decimal128("123.45e6"), Decimal128("876.54e32")};
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
    static PropertyType property_type()
    {
        return BaseT::property_type() | PropertyType::Nullable;
    }
    static auto values() -> decltype(BaseT::values())
    {
        auto ret = BaseT::values();
        ret.push_back(typename BaseT::Type());
        return ret;
    }
};
} // namespace realm::primitive_fixtures