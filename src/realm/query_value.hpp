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
    enum Attribute : uint64_t {
        None = 0,
        Null = 1,
        Int = 2,
        Double = 4,
        Float = 8,
        Bool = 16,
        Timestamp = 32,
        String = 64,
        Binary = 128,
        UUID = 256,
        ObjectId = 512,
        Decimal128 = 1024,
        ObjectLink = 2048,
        Mixed = 4096,
        Numeric = Int + Double + Float + Decimal128,
    };
    TypeOfValue(uint64_t attributes)
        : m_attributes(attributes)
    {
    }
    TypeOfValue(const std::string& attribute_tags);
    TypeOfValue(const class Mixed& value);
    bool matches(const class Mixed& value) const;
    bool matches(const TypeOfValue& other) const
    {
        return (m_attributes & other.m_attributes) != 0;
    }
    uint64_t get_attributes() const
    {
        return m_attributes;
    }
    std::string to_string() const;

private:
    uint64_t m_attributes;
};

struct SubexprType {
    enum QueryTypeExtension { raw_data_type, type_of_query };
    constexpr SubexprType(DataType::Type type, QueryTypeExtension expression_type = raw_data_type) noexcept
        : m_data_type(type)
        , m_expression_type(expression_type)
    {
    }
    constexpr SubexprType(DataType type, QueryTypeExtension expression_type = raw_data_type) noexcept
        : m_data_type(type)
        , m_expression_type(expression_type)
    {
    }
    QueryTypeExtension get_expression_type() const
    {
        return m_expression_type;
    }
    constexpr operator int64_t() const noexcept
    {
        if (m_expression_type == type_of_query) {
            return 0xff;
        }
        else {
            return int64_t(m_data_type);
        }
    }
    constexpr bool operator==(const DataType& rhs) const noexcept
    {
        return m_expression_type == raw_data_type && m_data_type == rhs;
    }
    constexpr bool operator!=(const DataType& rhs) const noexcept
    {
        return !(*this == rhs);
    }
    constexpr bool operator==(const SubexprType& rhs) const noexcept
    {
        return m_expression_type == rhs.m_expression_type &&
               (m_expression_type == raw_data_type ? (m_data_type == rhs.m_data_type) : true);
    }
    constexpr bool operator!=(const SubexprType& rhs) const noexcept
    {
        return !(*this == rhs);
    }
    bool is_valid()
    {
        return m_expression_type == raw_data_type && m_data_type.is_valid();
    }
    DataType get_data_type()
    {
        REALM_ASSERT_EX(m_expression_type == raw_data_type, m_expression_type);
        return m_data_type;
    }

private:
    DataType m_data_type;
    QueryTypeExtension m_expression_type;
};

static constexpr SubexprType exp_QueryExpressionType =
    SubexprType{DataType::Type::Int, SubexprType::QueryTypeExtension::type_of_query};
static constexpr SubexprType exp_Int = SubexprType{DataType::Type::Int};
static constexpr SubexprType exp_Bool = SubexprType{DataType::Type::Bool};
static constexpr SubexprType exp_String = SubexprType{DataType::Type::String};
static constexpr SubexprType exp_Binary = SubexprType{DataType::Type::Binary};
static constexpr SubexprType exp_Mixed = SubexprType{DataType::Type::Mixed};
static constexpr SubexprType exp_Timestamp = SubexprType{DataType::Type::Timestamp};
static constexpr SubexprType exp_Float = SubexprType{DataType::Type::Float};
static constexpr SubexprType exp_Double = SubexprType{DataType::Type::Double};
static constexpr SubexprType exp_Decimal = SubexprType{DataType::Type::Decimal};
static constexpr SubexprType exp_Link = SubexprType{DataType::Type::Link};
static constexpr SubexprType exp_LinkList = SubexprType{DataType::Type::LinkList};
static constexpr SubexprType exp_ObjectId = SubexprType{DataType::Type::ObjectId};
static constexpr SubexprType exp_TypedLink = SubexprType{DataType::Type::TypedLink};
static constexpr SubexprType exp_UUID = SubexprType{DataType::Type::UUID};

struct QueryValue {
    QueryValue()
        : m_type(exp_Int)
        , m_mixed()
    {
    }
    QueryValue(const Mixed& mixed)
        : m_type(mixed.is_null() ? exp_Int : mixed.get_type())
        , m_mixed(mixed)
    {
    }
    QueryValue(const TypeOfValue& value)
        : m_type(exp_QueryExpressionType)
        , m_type_of_value(value)
    {
    }
    ~QueryValue() {}

    Mixed as_mixed() const
    {
        REALM_ASSERT(m_type != exp_QueryExpressionType);
        return m_mixed;
    }
    TypeOfValue as_type_of_value() const
    {
        REALM_ASSERT_EX(m_type == exp_QueryExpressionType, m_type);
        return m_type_of_value;
    }

    inline SubexprType get_type() const
    {
        return m_type;
    }

    SubexprType::QueryTypeExtension get_expression_type() const
    {
        return m_type.get_expression_type();
    }

    bool is_null() const
    {
        return m_type.get_expression_type() == SubexprType::QueryTypeExtension::raw_data_type && m_mixed.is_null();
    }

    static bool types_are_comparable(const QueryValue& v1, const QueryValue& v2)
    {
        if (v1.m_type == exp_QueryExpressionType || v2.m_type == exp_QueryExpressionType) {
            return v1.m_type == v2.m_type;
        }
        return Mixed::types_are_comparable(v1.m_mixed, v2.m_mixed);
    }

    bool operator==(const QueryValue& v2) const
    {
        if (m_type == exp_QueryExpressionType || v2.m_type == exp_QueryExpressionType) {
            return m_type == v2.m_type && m_type_of_value.matches(v2.m_type_of_value);
        }
        else {
            return m_mixed == v2.m_mixed;
        }
    }

private:
    SubexprType m_type;
    union {
        Mixed m_mixed;
        TypeOfValue m_type_of_value;
    };
};

} // namespace realm

#endif // REALM_QUERY_VALUE_HPP
