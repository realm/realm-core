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

#ifndef REALM_MIXED_HPP
#define REALM_MIXED_HPP

#include <cstdint> // int64_t - not part of C++03, not even required by C++11 (see C++11 section 18.4.1)

#include <cstddef> // size_t
#include <cstring>

#include <realm/keys.hpp>
#include <realm/binary_data.hpp>
#include <realm/data_type.hpp>
#include <realm/string_data.hpp>
#include <realm/timestamp.hpp>
#include <realm/util/assert.hpp>
#include <realm/utilities.hpp>

namespace realm {


/// This class represents a polymorphic Realm value.
///
/// At any particular moment an instance of this class stores a
/// definite value of a definite type. If, for instance, that is an
/// integer value, you may call get<int64_t>() to extract that value. You
/// may call get_type() to discover what type of value is currently
/// stored. Calling get<int64_t>() on an instance that does not store an
/// integer, has undefined behavior, and likewise for all the other
/// types that can be stored.
///
/// It is crucial to understand that the act of extracting a value of
/// a particular type requires definite knowledge about the stored
/// type. Calling a getter method for any particular type, that is not
/// the same type as the stored value, has undefined behavior.
///
/// While values of numeric types are contained directly in a Mixed
/// instance, character and binary data are merely referenced. A Mixed
/// instance never owns the referenced data, nor does it in any other
/// way attempt to manage its lifetime.
///
/// For compatibility with C style strings, when a string (character
/// data) is stored in a Realm database, it is always followed by a
/// terminating null character. This is also true when strings are
/// stored in a mixed type column. This means that in the following
/// code, if the 'mixed' value of the 8th row stores a string, then \c
/// c_str will always point to a null-terminated string:
///
/// \code{.cpp}
///
///   const char* c_str = my_table[7].mixed.data(); // Always null-terminated
///
/// \endcode
///
/// Note that this assumption does not hold in general for strings in
/// instances of Mixed. Indeed there is nothing stopping you from
/// constructing a new Mixed instance that refers to a string without
/// a terminating null character.
///
/// At the present time no soultion has been found that would allow
/// for a Mixed instance to directly store a reference to a table. The
/// problem is roughly as follows: From most points of view, the
/// desirable thing to do, would be to store the table reference in a
/// Mixed instance as a plain pointer without any ownership
/// semantics. This would have no negative impact on the performance
/// of copying and destroying Mixed instances, and it would serve just
/// fine for passing a table as argument when setting the value of an
/// entry in a mixed column. In that case a copy of the referenced
/// table would be inserted into the mixed column.
///
/// On the other hand, when retrieving a table reference from a mixed
/// column, storing it as a plain pointer in a Mixed instance is no
/// longer an acceptable option. The complex rules for managing the
/// lifetime of a Table instance, that represents a subtable,
/// necessitates the use of a "smart pointer" such as
/// TableRef. Enhancing the Mixed class to be able to act as a
/// TableRef would be possible, but would also lead to several new
/// problems. One problem is the risk of a Mixed instance outliving a
/// stack allocated Table instance that it references. This would be a
/// fatal error. Another problem is the impact that the nontrivial
/// table reference has on the performance of copying and destroying
/// Mixed instances.
///
/// \sa StringData
class Mixed {
public:
    Mixed() noexcept
        : m_type(DataType(null_type))
    {
    }

    Mixed(int i) noexcept
        : Mixed(int64_t(i))
    {
    }
    Mixed(int64_t) noexcept;
    Mixed(bool) noexcept;
    Mixed(float) noexcept;
    Mixed(double) noexcept;
    Mixed(StringData) noexcept;
    Mixed(BinaryData) noexcept;
    Mixed(Timestamp) noexcept;
    Mixed(ObjKey) noexcept;

    // These are shortcuts for Mixed(StringData(c_str)), and are
    // needed to avoid unwanted implicit conversion of char* to bool.
    Mixed(char* c_str) noexcept
        : Mixed(StringData(c_str))
    {
    }
    Mixed(const char* c_str) noexcept
        : Mixed(StringData(c_str))
    {
    }
    Mixed(const std::string& s) noexcept
        : Mixed(StringData(s))
    {
    }

    ~Mixed() noexcept
    {
    }

    DataType get_type() const noexcept
    {
        REALM_ASSERT(m_type != null_type);
        return DataType(m_type);
    }

    template <class T>
    T get() const noexcept;

    // These functions are kept to be backwards compatible
    int64_t get_int() const;
    bool get_bool() const;
    float get_float() const;
    double get_double() const;
    StringData get_string() const;
    Timestamp get_timestamp() const;

    bool is_null() const;
    int compare(const Mixed& b) const;
    bool operator==(const Mixed& other) const
    {
        return compare(other) == 0;
    }
    bool operator!=(const Mixed& other) const
    {
        return compare(other) != 0;
    }

private:
    template <class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>&, const Mixed&);

    static constexpr int null_type = 0x0f;
    uint8_t m_type;

    union {
        int64_t int_val;
        bool bool_val;
        float float_val;
        double double_val;
        StringData string_val;
        Timestamp date_val;
    };
};

// Implementation:

inline Mixed::Mixed(int64_t v) noexcept
{
    m_type = type_Int;
    int_val = v;
}

inline Mixed::Mixed(bool v) noexcept
{
    m_type = type_Bool;
    bool_val = v;
}

inline Mixed::Mixed(float v) noexcept
{
    m_type = type_Float;
    float_val = v;
}

inline Mixed::Mixed(double v) noexcept
{
    m_type = type_Double;
    double_val = v;
}

inline Mixed::Mixed(StringData v) noexcept
{
    m_type = type_String;
    string_val = v;
}

inline Mixed::Mixed(BinaryData v) noexcept
{
    m_type = type_Binary;
    string_val = StringData(v.data(), v.size());
}

inline Mixed::Mixed(Timestamp v) noexcept
{
    m_type = type_Timestamp;
    date_val = v;
}

inline Mixed::Mixed(ObjKey v) noexcept
{
    m_type = type_Link;
    int_val = v.value;
}

template <>
inline int64_t Mixed::get<int64_t>() const noexcept
{
    REALM_ASSERT(m_type == type_Int);
    return int_val;
}

inline int64_t Mixed::get_int() const
{
    return get<int64_t>();
}

template <>
inline bool Mixed::get<bool>() const noexcept
{
    REALM_ASSERT(m_type == type_Bool);
    return bool_val;
}

inline bool Mixed::get_bool() const
{
    return get<bool>();
}

template <>
inline float Mixed::get<float>() const noexcept
{
    REALM_ASSERT(m_type == type_Float);
    return float_val;
}

inline float Mixed::get_float() const
{
    return get<float>();
}

template <>
inline double Mixed::get<double>() const noexcept
{
    REALM_ASSERT(m_type == type_Double);
    return double_val;
}

inline double Mixed::get_double() const
{
    return get<double>();
}

template <>
inline StringData Mixed::get<StringData>() const noexcept
{
    REALM_ASSERT(m_type == type_String);
    return string_val;
}

inline StringData Mixed::get_string() const
{
    return get<StringData>();
}

template <>
inline BinaryData Mixed::get<BinaryData>() const noexcept
{
    REALM_ASSERT(m_type == type_Binary);
    return BinaryData(string_val.data(), string_val.size());
}

template <>
inline Timestamp Mixed::get<Timestamp>() const noexcept
{
    REALM_ASSERT(m_type == type_Timestamp);
    return date_val;
}

inline Timestamp Mixed::get_timestamp() const
{
    return get<Timestamp>();
}

template <>
inline ObjKey Mixed::get<ObjKey>() const noexcept
{
    REALM_ASSERT(m_type == type_Link);
    return ObjKey(int_val);
}

inline bool Mixed::is_null() const
{
    if (m_type == null_type)
        return true;

    switch (get_type()) {
        case type_String:
            return get<StringData>().is_null();
        case type_Timestamp:
            return get<Timestamp>().is_null();
        case type_Link:
            return get<ObjKey>() == null_key;
        default:
            break;
    }
    return false;
}

inline int Mixed::compare(const Mixed& b) const
{
    if (is_null()) {
        return b.is_null() ? 0 : -1;
    }
    if (b.is_null())
        return 1;

    switch (get_type()) {
        case type_Int:
            if (get<int64_t>() > b.get<int64_t>())
                return 1;
            else if (get<int64_t>() < b.get<int64_t>())
                return -1;
            break;
        case type_String:
            if (get<StringData>() > b.get<StringData>())
                return 1;
            else if (get<StringData>() < b.get<StringData>())
                return -1;
            break;
        case type_Float:
            if (get<float>() > b.get<float>())
                return 1;
            else if (get<float>() < b.get<float>())
                return -1;
            break;
        case type_Double:
            if (get<double>() > b.get<double>())
                return 1;
            else if (get<double>() < b.get<double>())
                return -1;
            break;
        case type_Bool:
            if (get<bool>() > b.get<bool>())
                return 1;
            else if (get<bool>() < b.get<bool>())
                return -1;
            break;
        case type_Timestamp:
            if (get<Timestamp>() > b.get<Timestamp>())
                return 1;
            else if (get<Timestamp>() < b.get<Timestamp>())
                return -1;
            break;
        case type_Link:
            if (get<ObjKey>() > b.get<ObjKey>())
                return 1;
            else if (get<ObjKey>() < b.get<ObjKey>())
                return -1;
            break;
        default:
            REALM_ASSERT_RELEASE(false && "Compare not supported for this column type");
            break;
    }

    return 0;
}

// LCOV_EXCL_START
template <class Ch, class Tr>
inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Mixed& m)
{
    out << "Mixed(";
    switch (m.m_type) {
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
            out << m.string_val;
            break;
        case type_Binary:
            out << BinaryData(m.string_val.data(), m.string_val.size());
            break;
        case type_Timestamp:
            out << m.date_val;
            break;
        case type_Link:
            out << ObjKey(m.int_val);
            break;
        case type_OldDateTime:
        case type_OldTable:
        case type_OldMixed:
        case type_LinkList:
            REALM_ASSERT(false);
    }
    out << ")";
    return out;
}
// LCOV_EXCL_STOP

} // namespace realm

#endif // REALM_MIXED_HPP
