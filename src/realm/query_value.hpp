/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

#ifndef REALM_QUERY_VALUE_HPP
#define REALM_QUERY_VALUE_HPP

#include <string>

#include <realm/data_type.hpp>
#include <realm/mixed.hpp>

namespace realm {

class TypeOfValue {
public:
    enum Attribute {
        Null = 0x0001,
        Int = 0x0002,
        Double = 0x0004,
        Float = 0x0008,
        Bool = 0x0010,
        Timestamp = 0x0020,
        String = 0x0040,
        Binary = 0x0080,
        UUID = 0x0100,
        ObjectId = 0x0200,
        Decimal128 = 0x0400,
        ObjectLink = 0x0800,
        Object = 0x1000,
        Array = 0x2000,
        Numeric = Int + Double + Float + Decimal128,
        Collection = Array + Object
    };
    explicit TypeOfValue(int64_t attributes);
    explicit TypeOfValue(std::string_view attribute_tags);
    explicit TypeOfValue(const class Mixed& value);
    explicit TypeOfValue(const ColKey& col_key);
    explicit TypeOfValue(const DataType& data_type);
    bool matches(const class Mixed& value) const;
    bool matches(const TypeOfValue& other) const
    {
        return (m_attributes & other.m_attributes) != 0;
    }
    int64_t get_attributes() const
    {
        return m_attributes;
    }
    std::string to_string() const;

private:
    int64_t m_attributes;
};

class QueryValue : public Mixed {
public:
    using Mixed::Mixed;

    QueryValue(const Mixed& other)
        : Mixed(other)
    {
    }

    QueryValue(TypeOfValue v) noexcept
    {
        m_type = int(type_TypeOfValue) + 1;
        int_val = v.get_attributes();
    }

    TypeOfValue get_type_of_value() const noexcept
    {
        REALM_ASSERT(get_type() == type_TypeOfValue);
        return TypeOfValue(int_val);
    }
};

} // namespace realm

#endif // REALM_QUERY_VALUE_HPP
