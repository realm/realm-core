/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/mixed.hpp>
#include <realm/decimal128.hpp>
#include <realm/unicode.hpp>
#include <realm/column_type_traits.hpp>
#include <realm/obj.hpp>
#include <realm/table.hpp>
#include <realm/query_value.hpp>
#include <realm/util/serializer.hpp>

namespace realm {
namespace {
static const int sorting_rank[19] = {
    // Observe! Changing these values breaks the file format for Set<Mixed>

    -1, // null
    1,  // type_Int = 0,
    0,  // type_Bool = 1,
    2,  // type_String = 2,
    -1,
    2,  // type_Binary = 4,
    -1, // type_OldTable = 5,
    -1, // type_Mixed = 6,
    -1, // type_OldDateTime = 7,
    3,  // type_Timestamp = 8,
    1,  // type_Float = 9,
    1,  // type_Double = 10,
    1,  // type_Decimal = 11,
    7,  // type_Link = 12,
    -1, // type_LinkList = 13,
    -1,
    4, // type_ObjectId = 15,
    6, // type_TypedLink = 16
    5, // type_UUID = 17

    // Observe! Changing these values breaks the file format for Set<Mixed>
};

int compare_string(StringData a, StringData b)
{
    if (a == b)
        return 0;
    return utf8_compare(a, b) ? -1 : 1;
}

int compare_binary(BinaryData a, BinaryData b)
{
    size_t asz = a.size();
    size_t bsz = b.size();
    size_t min_sz = std::min(asz, bsz);
    int ret = memcmp(a.data(), b.data(), min_sz);
    if (ret == 0) {
        if (asz > bsz)
            ret = 1;
        else if (asz < bsz)
            ret = -1;
    }
    return ret;
}

template <int>
struct IntTypeForSize;
template <>
struct IntTypeForSize<1> {
    using type = uint8_t;
};
template <>
struct IntTypeForSize<2> {
    using type = uint16_t;
};
template <>
struct IntTypeForSize<4> {
    using type = uint32_t;
};
template <>
struct IntTypeForSize<8> {
    using type = uint64_t;
};

template <typename Float>
int compare_float(Float a_raw, Float b_raw)
{
    bool a_nan = std::isnan(a_raw);
    bool b_nan = std::isnan(b_raw);
    if (!a_nan && !b_nan) {
        // Just compare as IEEE floats
        return a_raw == b_raw ? 0 : a_raw < b_raw ? -1 : 1;
    }
    if (a_nan && b_nan) {
        // Compare the nan values as unsigned
        using IntType = typename IntTypeForSize<sizeof(Float)>::type;
        IntType a = 0, b = 0;
        memcpy(&a, &a_raw, sizeof(Float));
        memcpy(&b, &b_raw, sizeof(Float));
        return a == b ? 0 : a < b ? -1 : 1;
    }
    // One is nan, the other is not
    // nans are treated as being less than all non-nan values
    return a_nan ? -1 : 1;
}

template <typename T>
int compare_generic(T lhs, T rhs)
{
    return lhs == rhs ? 0 : lhs < rhs ? -1 : 1;
}

// This is the tricky one. Needs to support the following cases:
// * Doubles with a fractional component.
// * Longs that can't be precisely represented as a double.
// * Doubles outside of the range of Longs (including +/- Inf).
// * NaN (defined by us as less than all Longs)
// * Return value is always -1, 0, or 1 to ensure it is safe to negate.
int compare_long_to_double(int64_t lhs, double rhs)
{
    // All Longs are > NaN
    if (std::isnan(rhs))
        return 1;

    // Ints with magnitude <= 2**53 can be precisely represented as doubles.
    // Additionally, doubles outside of this range can't have a fractional component.
    static const int64_t kEndOfPreciseDoubles = 1ll << 53;
    if (lhs <= kEndOfPreciseDoubles && lhs >= -kEndOfPreciseDoubles) {
        return compare_float(double(lhs), rhs);
    }

    // Large magnitude doubles (including +/- Inf) are strictly > or < all Longs.
    static const double kBoundOfLongRange = -static_cast<double>(LLONG_MIN); // positive 2**63
    if (rhs >= kBoundOfLongRange)
        return -1; // Can't be represented in a Long.
    if (rhs < -kBoundOfLongRange)
        return 1; // Can be represented in a Long.

    // Remaining Doubles can have their integer component precisely represented as long longs.
    // If they have a fractional component, they must be strictly > or < lhs even after
    // truncation of the fractional component since low-magnitude lhs were handled above.
    return compare_generic(lhs, int64_t(rhs));
}
} // anonymous namespace

Mixed::Mixed(const Obj& obj) noexcept
    : Mixed(ObjLink(obj.get_table()->get_key(), obj.get_key()))
{
}

bool Mixed::types_are_comparable(const Mixed& lhs, const Mixed& rhs)
{
    if (lhs.m_type == rhs.m_type)
        return lhs.m_type != 0;

    if (lhs.is_null() || rhs.is_null())
        return false;

    DataType l_type = lhs.get_type();
    DataType r_type = rhs.get_type();
    return data_types_are_comparable(l_type, r_type);
}

bool Mixed::data_types_are_comparable(DataType l_type, DataType r_type)
{
    if (l_type == r_type)
        return true;

    if (is_numeric(l_type, r_type)) {
        return true;
    }
    if ((l_type == type_String && r_type == type_Binary) || (r_type == type_String && l_type == type_Binary)) {
        return true;
    }
    if (l_type == type_Mixed || r_type == type_Mixed) {
        return true; // Mixed is comparable with any type
    }
    return false;
}

bool Mixed::accumulate_numeric_to(Decimal128& destination) const
{
    bool did_accumulate = false;
    if (!is_null()) {
        switch (get_type()) {
            case type_Int:
                destination += Decimal128(get_int());
                did_accumulate = true;
                break;
            case type_Double:
                destination += Decimal128(get_double());
                did_accumulate = true;
                break;
            case type_Float:
                destination += Decimal128(get_float());
                did_accumulate = true;
                break;
            case type_Decimal: {
                auto val = get_decimal();
                if (!val.is_nan()) {
                    destination += val;
                    did_accumulate = true;
                }
                break;
            }
            default:
                break;
        }
    }
    return did_accumulate;
}

int Mixed::compare(const Mixed& b) const
{
    // Observe! Changing this function breaks the file format for Set<Mixed>

    if (is_null()) {
        return b.is_null() ? 0 : -1;
    }
    if (b.is_null())
        return 1;

    // None is null
    auto type = get_type();
    switch (type) {
        case type_Bool: {
            if (b.get_type() == type_Bool) {
                return compare_generic(bool_val, b.bool_val);
            }
            break;
        }
        case type_Int:
            switch (b.get_type()) {
                case type_Int:
                    return compare_generic(int_val, b.int_val);
                case type_Float:
                    return compare_long_to_double(int_val, b.float_val);
                case type_Double:
                    return compare_long_to_double(int_val, b.double_val);
                case type_Decimal:
                    return Decimal128(int_val).compare(b.decimal_val);
                default:
                    break;
            }
            break;
        case type_String:
            if (b.get_type() == type_String)
                return compare_string(get<StringData>(), b.get<StringData>());
            [[fallthrough]];
        case type_Binary:
            if (b.get_type() == type_String || b.get_type() == type_Binary)
                return compare_binary(get<BinaryData>(), b.get<BinaryData>());
            break;
        case type_Float:
            switch (b.get_type()) {
                case type_Int:
                    return -compare_long_to_double(b.int_val, float_val);
                case type_Float:
                    return compare_float(float_val, b.float_val);
                case type_Double:
                    return compare_float(double(float_val), b.double_val);
                case type_Decimal:
                    return Decimal128(float_val).compare(b.decimal_val);
                default:
                    break;
            }
            break;
        case type_Double:
            switch (b.get_type()) {
                case type_Int:
                    return -compare_long_to_double(b.int_val, double_val);
                case type_Float:
                    return compare_float(double_val, double(b.float_val));
                case type_Double:
                    return compare_float(double_val, b.double_val);
                case type_Decimal:
                    return Decimal128(double_val).compare(b.decimal_val);
                default:
                    break;
            }
            break;
        case type_Timestamp:
            if (b.get_type() == type_Timestamp) {
                return compare_generic(date_val, b.date_val);
            }
            break;
        case type_ObjectId:
            if (b.get_type() == type_ObjectId) {
                return compare_generic(id_val, b.id_val);
            }
            break;
        case type_Decimal:
            switch (b.get_type()) {
                case type_Int:
                    return decimal_val.compare(Decimal128(b.int_val));
                case type_Float:
                    return decimal_val.compare(Decimal128(b.float_val));
                case type_Double:
                    return decimal_val.compare(Decimal128(b.double_val));
                case type_Decimal:
                    return decimal_val.compare(b.decimal_val);
                default:
                    break;
            }
            break;
        case type_Link:
            if (b.get_type() == type_Link) {
                return compare_generic(int_val, b.int_val);
            }
            break;
        case type_TypedLink:
            if (b.is_type(type_TypedLink)) {
                return compare_generic(link_val, b.link_val);
            }
            break;
        case type_UUID:
            if (b.get_type() == type_UUID) {
                return compare_generic(uuid_val, b.uuid_val);
            }
            break;
        default:
            if (type == type_TypeOfValue && b.get_type() == type_TypeOfValue) {
                return TypeOfValue(int_val).matches(TypeOfValue(b.int_val)) ? 0 : compare_generic(int_val, b.int_val);
            }
            REALM_ASSERT_RELEASE(false && "Compare not supported for this column type");
            break;
    }

    // Comparing rank of types as a fallback makes it possible to sort of a list of Mixed
    REALM_ASSERT(sorting_rank[m_type] != sorting_rank[b.m_type]);
    // Using rank table will ensure that all numeric values are kept together
    return (sorting_rank[m_type] > sorting_rank[b.m_type]) ? 1 : -1;

    // Observe! Changing this function breaks the file format for Set<Mixed>
}

int Mixed::compare_signed(const Mixed& b) const
{
    if (is_type(type_String) && b.is_type(type_String)) {
        auto a_val = get_string();
        auto b_val = b.get_string();
        return a_val == b_val ? 0 : a_val < b_val ? -1 : 1;
    }
    return compare(b);
}

template <>
int64_t Mixed::export_to_type() const noexcept
{
    // If the common type is Int, then both values must be Int
    REALM_ASSERT(get_type() == type_Int);
    return int_val;
}

template <>
float Mixed::export_to_type() const noexcept
{
    // If the common type is Float, then values must be either Int or Float
    REALM_ASSERT(m_type);
    switch (get_type()) {
        case type_Int:
            return float(int_val);
        case type_Float:
            return float_val;
        default:
            REALM_ASSERT(false);
            break;
    }
    return 0.;
}

template <>
double Mixed::export_to_type() const noexcept
{
    // If the common type is Double, then values must be either Int, Float or Double
    REALM_ASSERT(m_type);
    switch (get_type()) {
        case type_Int:
            return double(int_val);
        case type_Float:
            return double(float_val);
        case type_Double:
            return double_val;
        default:
            REALM_ASSERT(false);
            break;
    }
    return 0.;
}

template <>
Decimal128 Mixed::export_to_type() const noexcept
{
    REALM_ASSERT(m_type);
    switch (get_type()) {
        case type_Int:
            return Decimal128(int_val);
        case type_Float:
            return Decimal128(float_val);
        case type_Double:
            return Decimal128(double_val);
        case type_Decimal:
            return decimal_val;
        default:
            REALM_ASSERT(false);
            break;
    }
    return {};
}

template <>
util::Optional<int64_t> Mixed::get<util::Optional<int64_t>>() const noexcept
{
    if (is_null()) {
        return {};
    }
    return get<int64_t>();
}

template <>
util::Optional<bool> Mixed::get<util::Optional<bool>>() const noexcept
{
    if (is_null()) {
        return {};
    }
    return get<bool>();
}

template <>
util::Optional<float> Mixed::get<util::Optional<float>>() const noexcept
{
    if (is_null()) {
        return {};
    }
    return get<float>();
}

template <>
util::Optional<double> Mixed::get<util::Optional<double>>() const noexcept
{
    if (is_null()) {
        return {};
    }
    return get<double>();
}

template <>
util::Optional<ObjectId> Mixed::get<util::Optional<ObjectId>>() const noexcept
{
    if (is_null()) {
        return {};
    }
    return get<ObjectId>();
}

template <>
util::Optional<UUID> Mixed::get<util::Optional<UUID>>() const noexcept
{
    if (is_null()) {
        return {};
    }
    return get<UUID>();
}

static DataType get_common_type(DataType t1, DataType t2)
{
    // It might be by accident that this works, but it finds the most advanced type
    DataType common = std::max(t1, t2);
    return common;
}

Mixed Mixed::operator+(const Mixed& rhs) const
{
    if (!is_null() && !rhs.is_null()) {
        auto common_type = get_common_type(get_type(), rhs.get_type());
        switch (common_type) {
            case type_Int:
                return export_to_type<Int>() + rhs.export_to_type<Int>();
            case type_Float:
                return export_to_type<float>() + rhs.export_to_type<float>();
            case type_Double:
                return export_to_type<double>() + rhs.export_to_type<double>();
            case type_Decimal:
                return export_to_type<Decimal128>() + rhs.export_to_type<Decimal128>();
            default:
                break;
        }
    }
    return {};
}

Mixed Mixed::operator-(const Mixed& rhs) const
{
    if (!is_null() && !rhs.is_null()) {
        auto common_type = get_common_type(get_type(), rhs.get_type());
        switch (common_type) {
            case type_Int:
                return export_to_type<Int>() - rhs.export_to_type<Int>();
            case type_Float:
                return export_to_type<float>() - rhs.export_to_type<float>();
            case type_Double:
                return export_to_type<double>() - rhs.export_to_type<double>();
            case type_Decimal:
                return export_to_type<Decimal128>() - rhs.export_to_type<Decimal128>();
            default:
                break;
        }
    }
    return {};
}

Mixed Mixed::operator*(const Mixed& rhs) const
{
    if (!is_null() && !rhs.is_null()) {
        auto common_type = get_common_type(get_type(), rhs.get_type());
        switch (common_type) {
            case type_Int:
                return export_to_type<Int>() * rhs.export_to_type<Int>();
            case type_Float:
                return export_to_type<float>() * rhs.export_to_type<float>();
            case type_Double:
                return export_to_type<double>() * rhs.export_to_type<double>();
            case type_Decimal:
                return export_to_type<Decimal128>() * rhs.export_to_type<Decimal128>();
            default:
                break;
        }
    }
    return {};
}

Mixed Mixed::operator/(const Mixed& rhs) const
{
    if (!is_null() && !rhs.is_null()) {
        auto common_type = get_common_type(get_type(), rhs.get_type());
        switch (common_type) {
            case type_Int: {
                auto dividend = export_to_type<Int>();
                auto divisor = rhs.export_to_type<Int>();
                // We don't want to throw here. This is usually used as part of a query
                // and in this case we would just expect a no match
                if (divisor == 0)
                    return dividend < 0 ? std::numeric_limits<int64_t>::min() : std::numeric_limits<int64_t>::max();
                return dividend / divisor;
            }
            case type_Float:
                static_assert(std::numeric_limits<float>::is_iec559); // Infinity is supported
                return export_to_type<float>() / rhs.export_to_type<float>();
            case type_Double:
                static_assert(std::numeric_limits<double>::is_iec559); // Infinity is supported
                return export_to_type<double>() / rhs.export_to_type<double>();
            case type_Decimal:
                return export_to_type<Decimal128>() / rhs.export_to_type<Decimal128>();
            default:
                break;
        }
    }
    return {};
}

size_t Mixed::hash() const
{
    if (is_null())
        return 0;

    size_t hash = 0;
    switch (get_type()) {
        case type_Int:
            hash = size_t(int_val);
            break;
        case type_Bool:
            hash = bool_val ? 0xdeadbeef : 0xcafebabe;
            break;
        case type_Float: {
            auto unsigned_data = reinterpret_cast<const unsigned char*>(&float_val);
            hash = murmur2_or_cityhash(unsigned_data, sizeof(float));
            break;
        }
        case type_Double: {
            auto unsigned_data = reinterpret_cast<const unsigned char*>(&double_val);
            hash = murmur2_or_cityhash(unsigned_data, sizeof(double));
            break;
        }
        case type_String:
            hash = get<StringData>().hash();
            break;
        case type_Binary: {
            auto bin = get<BinaryData>();
            StringData str(bin.data(), bin.size());
            hash = str.hash();
            break;
        }
        case type_Timestamp:
            hash = get<Timestamp>().hash();
            break;
        case type_ObjectId:
            hash = get<ObjectId>().hash();
            break;
        case type_UUID: {
            hash = get<UUID>().hash();
            break;
        }
        case type_TypedLink: {
            auto unsigned_data = reinterpret_cast<const unsigned char*>(&link_val);
            hash = murmur2_or_cityhash(unsigned_data, 12);
            break;
        }
        case type_Decimal:
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            REALM_ASSERT_RELEASE(false && "Hash not supported for this column type");
            break;
    }

    return hash;
}

StringData Mixed::get_index_data(std::array<char, 16>& buffer) const
{
    if (is_null()) {
        return {};
    }
    switch (get_type()) {
        case type_Int: {
            int64_t i = get_int();
            const char* c = reinterpret_cast<const char*>(&i);
            realm::safe_copy_n(c, sizeof(int64_t), buffer.data());
            return StringData{buffer.data(), sizeof(int64_t)};
        }
        case type_Bool: {
            int64_t i = get_bool() ? 1 : 0;
            return Mixed(i).get_index_data(buffer);
        }
        case type_Float: {
            auto v2 = get_float();
            int i = int(v2);
            if (i == v2) {
                return Mixed(i).get_index_data(buffer);
            }
            const char* src = reinterpret_cast<const char*>(&v2);
            realm::safe_copy_n(src, sizeof(float), buffer.data());
            return StringData{buffer.data(), sizeof(float)};
        }
        case type_Double: {
            auto v2 = get_double();
            int i = int(v2);
            if (i == v2) {
                return Mixed(i).get_index_data(buffer);
            }
            const char* src = reinterpret_cast<const char*>(&v2);
            realm::safe_copy_n(src, sizeof(double), buffer.data());
            return StringData{buffer.data(), sizeof(double)};
        }
        case type_String:
            return get_string();
        case type_Binary: {
            auto bin = get_binary();
            return {bin.data(), bin.size()};
        }
        case type_Timestamp: {
            auto dt = get<Timestamp>();
            int64_t s = dt.get_seconds();
            int32_t ns = dt.get_nanoseconds();
            constexpr size_t index_size = sizeof(s) + sizeof(ns);
            const char* s_buf = reinterpret_cast<const char*>(&s);
            const char* ns_buf = reinterpret_cast<const char*>(&ns);
            realm::safe_copy_n(s_buf, sizeof(s), buffer.data());
            realm::safe_copy_n(ns_buf, sizeof(ns), buffer.data() + sizeof(s));
            return StringData{buffer.data(), index_size};
        }
        case type_ObjectId: {
            auto id = get<ObjectId>();
            memcpy(&buffer, &id, sizeof(ObjectId));
            return StringData{buffer.data(), sizeof(ObjectId)};
        }
        case type_Decimal: {
            auto v2 = this->get_decimal();
            int64_t i;
            if (v2.to_int(i)) {
                return Mixed(i).get_index_data(buffer);
            }
            const char* src = reinterpret_cast<const char*>(&v2);
            realm::safe_copy_n(src, sizeof(v2), buffer.data());
            return StringData{buffer.data(), sizeof(v2)};
        }
        case type_UUID: {
            auto id = get<UUID>();
            const auto bytes = id.to_bytes();
            std::copy_n(bytes.data(), bytes.size(), buffer.begin());
            return StringData{buffer.data(), bytes.size()};
        }
        case type_TypedLink: {
            auto link = get<ObjLink>();
            uint32_t k1 = link.get_table_key().value;
            int64_t k2 = link.get_obj_key().value;
            const char* src = reinterpret_cast<const char*>(&k1);
            realm::safe_copy_n(src, sizeof(k1), buffer.data());
            src = reinterpret_cast<const char*>(&k2);
            realm::safe_copy_n(src, sizeof(k2), buffer.data() + sizeof(k1));
            return StringData{buffer.data(), sizeof(k1) + sizeof(k2)};
        }
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            break;
    }
    REALM_ASSERT_RELEASE(false && "Index not supported for this column type");
    return {};
}

void Mixed::use_buffer(std::string& buf)
{
    if (is_null()) {
        return;
    }
    switch (get_type()) {
        case type_String:
            buf = std::string(string_val);
            string_val = StringData(buf);
            break;
        case type_Binary:
            buf = std::string(binary_val);
            binary_val = BinaryData(buf);
            break;
        default:
            break;
    }
}

// LCOV_EXCL_START
std::ostream& operator<<(std::ostream& out, const Mixed& m)
{
    if (m.is_null()) {
        out << "null";
    }
    else {
        switch (m.get_type()) {
            case type_Int:
                out << m.int_val;
                break;
            case type_Bool:
                out << (m.bool_val ? "true" : "false");
                break;
            case type_Float:
                out << m.float_val;
                break;
            case type_Double:
                out << m.double_val;
                break;
            case type_String:
                out << util::serializer::print_value(m.string_val);
                break;
            case type_Binary:
                out << util::serializer::print_value(m.binary_val);
                break;
            case type_Timestamp:
                out << util::serializer::print_value(m.date_val);
                break;
            case type_Decimal:
                out << m.decimal_val;
                break;
            case type_ObjectId:
                out << util::serializer::print_value(m.id_val);
                break;
            case type_Link:
                out << util::serializer::print_value(ObjKey(m.int_val));
                break;
            case type_TypedLink:
                out << util::serializer::print_value(m.link_val);
                break;
            case type_UUID:
                out << util::serializer::print_value(m.uuid_val);
                break;
            case type_Mixed:
            case type_LinkList:
                REALM_ASSERT(false);
        }
    }
    return out;
}
// LCOV_EXCL_STOP


} // namespace realm
