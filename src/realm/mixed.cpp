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

namespace _impl {

static const int sorting_rank[19] = {
    0, // null
    0, // type_Int = 0,
    0, // type_Bool = 1,
    1, // type_String = 2,
    -1,
    1,  // type_Binary = 4,
    -1, // type_OldTable = 5,
    -1, // type_Mixed = 6,
    -1, // type_OldDateTime = 7,
    2,  // type_Timestamp = 8,
    0,  // type_Float = 9,
    0,  // type_Double = 10,
    0,  // type_Decimal = 11,
    3,  // type_Link = 12,
    4,  // type_LinkList = 13,
    -1,
    5, // type_ObjectId = 15,
    6, // type_TypedLink = 16
    7, // type_UUID = 17
};

inline int compare_string(StringData a, StringData b)
{
    if (a == b)
        return 0;
    return utf8_compare(a, b) ? -1 : 1;
}

inline int compare_binary(BinaryData a, BinaryData b)
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
inline int compare_float(Float a_raw, Float b_raw)
{
    bool a_nan = std::isnan(a_raw);
    bool b_nan = std::isnan(b_raw);
    if (!a_nan && !b_nan) {
        // Just compare as IEEE floats
        return a_raw == b_raw ? 0 : a_raw < b_raw ? -1 : 1;
    }
    if (a_nan && b_nan) {
        // Compare the nan values as unsigned
        using IntType = typename _impl::IntTypeForSize<sizeof(Float)>::type;
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
inline int compare_generic(T lhs, T rhs)
{
    return lhs == rhs ? 0 : lhs < rhs ? -1 : 1;
}

inline int compare_decimals(Decimal128 lhs, Decimal128 rhs)
{
    return lhs.compare(rhs);
}

inline int compare_decimal_to_double(Decimal128 lhs, double rhs)
{
    return lhs.compare(Decimal128(rhs)); // FIXME: slow and not accurate in all cases
}

// This is the tricky one. Needs to support the following cases:
// * Doubles with a fractional component.
// * Longs that can't be precisely represented as a double.
// * Doubles outside of the range of Longs (including +/- Inf).
// * NaN (defined by us as less than all Longs)
// * Return value is always -1, 0, or 1 to ensure it is safe to negate.
inline int compare_long_to_double(int64_t lhs, double rhs)
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
} // namespace _impl

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

    bool l_is_numeric = l_type == type_Int || l_type == type_Bool || l_type == type_Float || l_type == type_Double ||
                        l_type == type_Decimal;
    bool r_is_numeric = r_type == type_Int || r_type == type_Bool || r_type == type_Float || r_type == type_Double ||
                        r_type == type_Decimal;
    if (l_is_numeric && r_is_numeric) {
        return true;
    }
    if ((l_type == type_String && r_type == type_Binary) || (r_type == type_String && l_type == type_Binary)) {
        return true;
    }
    if ((l_type == type_ObjectId && r_type == type_Timestamp) ||
        (r_type == type_ObjectId && l_type == type_Timestamp)) {
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
    if (is_null()) {
        return b.is_null() ? 0 : -1;
    }
    if (b.is_null())
        return 1;

    // None is null
    auto type = get_type();
    switch (type) {
        case type_Bool: {
            int64_t i_val = bool_val ? 1 : 0;
            switch (b.get_type()) {
                case type_Int:
                    return _impl::compare_generic(i_val, b.int_val);
                case type_Bool:
                    return _impl::compare_generic(bool_val, b.bool_val);
                case type_Float:
                    return _impl::compare_long_to_double(i_val, b.float_val);
                case type_Double:
                    return _impl::compare_long_to_double(i_val, b.double_val);
                case type_Decimal:
                    return _impl::compare_decimals(Decimal128(i_val), b.decimal_val);
                default:
                    break;
            }
            break;
        }
        case type_Int:
            switch (b.get_type()) {
                case type_Int:
                    return _impl::compare_generic(int_val, b.int_val);
                case type_Bool:
                    return _impl::compare_generic(int_val, int64_t(b.bool_val ? 1 : 0));
                case type_Float:
                    return _impl::compare_long_to_double(int_val, b.float_val);
                case type_Double:
                    return _impl::compare_long_to_double(int_val, b.double_val);
                case type_Decimal:
                    return _impl::compare_decimals(Decimal128(int_val), b.decimal_val);
                default:
                    break;
            }
            break;
        case type_String:
            if (b.get_type() == type_String)
                return _impl::compare_string(get<StringData>(), b.get<StringData>());
            [[fallthrough]];
        case type_Binary:
            if (b.get_type() == type_String || b.get_type() == type_Binary)
                return _impl::compare_binary(get<BinaryData>(), b.get<BinaryData>());
            break;
        case type_Float:
            switch (b.get_type()) {
                case type_Int:
                    return -_impl::compare_long_to_double(b.int_val, float_val);
                case type_Bool:
                    return -_impl::compare_long_to_double(b.bool_val ? 1 : 0, float_val);
                case type_Float:
                    return _impl::compare_float(float_val, b.float_val);
                case type_Double:
                    return _impl::compare_float(double(float_val), b.double_val);
                case type_Decimal:
                    return -_impl::compare_decimal_to_double(b.decimal_val, double(float_val));
                default:
                    break;
            }
            break;
        case type_Double:
            switch (b.get_type()) {
                case type_Int:
                    return -_impl::compare_long_to_double(b.int_val, double_val);
                case type_Bool:
                    return -_impl::compare_long_to_double(b.bool_val ? 1 : 0, double_val);
                case type_Float:
                    return _impl::compare_float(double_val, double(b.float_val));
                case type_Double:
                    return _impl::compare_float(double_val, b.double_val);
                case type_Decimal:
                    return -_impl::compare_decimal_to_double(b.decimal_val, double_val);
                default:
                    break;
            }
            break;
        case type_Timestamp:
            if (b.get_type() == type_Timestamp) {
                return _impl::compare_generic(date_val, b.date_val);
            }
            else if (b.get_type() == type_ObjectId) {
                return _impl::compare_generic(date_val, b.id_val.get_timestamp());
            }
            break;
        case type_ObjectId:
            if (b.get_type() == type_ObjectId) {
                return _impl::compare_generic(id_val, b.id_val);
            }
            else if (b.get_type() == type_Timestamp) {
                return _impl::compare_generic(id_val.get_timestamp(), b.date_val);
            }
            break;
        case type_Decimal:
            switch (b.get_type()) {
                case type_Int:
                    return _impl::compare_decimals(decimal_val, Decimal128(b.int_val));
                case type_Bool:
                    return _impl::compare_decimals(decimal_val, Decimal128(b.bool_val ? 1 : 0));
                case type_Float:
                    return _impl::compare_decimal_to_double(decimal_val, double(b.float_val));
                case type_Double:
                    return _impl::compare_decimal_to_double(decimal_val, b.double_val);
                case type_Decimal:
                    return _impl::compare_decimals(decimal_val, b.decimal_val);
                default:
                    break;
            }
            break;
        case type_Link:
            if (b.get_type() == type_Link) {
                return _impl::compare_generic(int_val, b.int_val);
            }
            break;
        case type_TypedLink:
            if (b.is_type(type_TypedLink)) {
                return _impl::compare_generic(link_val, b.link_val);
            }
            break;
        case type_UUID:
            if (b.get_type() == type_UUID) {
                return _impl::compare_generic(uuid_val, b.uuid_val);
            }
            break;
        default:
            if (type == type_TypeOfValue && b.get_type() == type_TypeOfValue) {
                return TypeOfValue(int_val).matches(TypeOfValue(b.int_val))
                           ? 0
                           : _impl::compare_generic(int_val, b.int_val);
            }
            REALM_ASSERT_RELEASE(false && "Compare not supported for this column type");
            break;
    }

    // Comparing types as a fallback option makes it possible to make a sort of a list of Mixed
    // This will also handle the case where null values are considered lower than all other values
    REALM_ASSERT(_impl::sorting_rank[m_type] != _impl::sorting_rank[b.m_type]);
    // Using rank table will ensure that all numeric values comes first
    return (_impl::sorting_rank[m_type] > _impl::sorting_rank[b.m_type]) ? 1 : -1;
}

template <class T>
T Mixed::export_to_type() const noexcept
{
    REALM_ASSERT(m_type);
    switch (get_type()) {
        case type_Int:
            return T(int_val);
        case type_Float:
            return T(float_val);
        case type_Double:
            return T(double_val);
        default:
            REALM_ASSERT(false);
            break;
    }
    return T();
}

template int64_t Mixed::export_to_type<int64_t>() const noexcept;
template float Mixed::export_to_type<float>() const noexcept;
template double Mixed::export_to_type<double>() const noexcept;

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
        case type_Decimal: {
            std::hash<realm::Decimal128> h;
            hash = h(decimal_val);
            break;
        }
        case type_UUID: {
            hash = get<UUID>().hash();
            break;
        }
        case type_TypedLink: {
            auto unsigned_data = reinterpret_cast<const unsigned char*>(&link_val);
            hash = murmur2_or_cityhash(unsigned_data, 12);
            break;
        }
        case type_Mixed:
        case type_Link:
        case type_LinkList:
            REALM_ASSERT_RELEASE(false && "Hash not supported for this column type");
            break;
    }

    return hash;
}

void Mixed::use_buffer(std::string& buf)
{
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
                out << ObjKey(m.int_val);
                break;
            case type_TypedLink:
                out << m.link_val;
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
