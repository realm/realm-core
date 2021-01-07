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

#include <numeric>
#include <unordered_map>

#include <realm/query_value.hpp>

using namespace realm;

// These keys must be stored as lowercase. Some naming comes from MongoDB's conventions
// see https://docs.mongodb.com/manual/reference/operator/query/type/
static std::unordered_map<std::string, TypeOfValue::Attribute> attribute_map = {
    {"null", TypeOfValue::Null},          {"int", TypeOfValue::Int},         {"integer", TypeOfValue::Int},
    {"bool", TypeOfValue::Bool},          {"boolean", TypeOfValue::Bool},    {"string", TypeOfValue::String},
    {"binary", TypeOfValue::Binary},      {"mixed", TypeOfValue::Mixed},     {"timestamp", TypeOfValue::Timestamp},
    {"float", TypeOfValue::Float},        {"double", TypeOfValue::Double},   {"decimal128", TypeOfValue::Decimal128},
    {"decimal", TypeOfValue::Decimal128}, {"link", TypeOfValue::ObjectLink}, {"object", TypeOfValue::ObjectLink},
    {"objectid", TypeOfValue::ObjectId},  {"uuid", TypeOfValue::UUID},       {"numeric", TypeOfValue::Numeric},
    {"date", TypeOfValue::Timestamp},     {"bindata", TypeOfValue::Binary}};
constexpr char attribute_separator = '|';

TypeOfValue::Attribute get_single_from(std::string str)
{
    std::transform(str.begin(), str.end(), str.begin(), toLowerAscii);
    auto it = attribute_map.find(str);
    if (it == attribute_map.end()) {
        std::string all_keys = std::accumulate(std::next(attribute_map.begin()), attribute_map.end(),
                                               attribute_map.begin()->first, [](std::string s, auto it) {
                                                   return std::move(s) + ", " + it.first;
                                               });
        throw std::runtime_error(util::format(
            "Unable to parse the type attribute string '%1', supported case insensitive values are: [%2]", str,
            all_keys));
    }
    return it->second;
}


TypeOfValue::Attribute attribute_from(DataType type)
{
    switch (type) {
        case DataType::Type::Int:
            return TypeOfValue::Attribute::Int;
        case DataType::Type::Bool:
            return TypeOfValue::Attribute::Bool;
        case DataType::Type::String:
            return TypeOfValue::Attribute::String;
        case DataType::Type::Binary:
            return TypeOfValue::Attribute::Binary;
        case DataType::Type::Mixed:
            return TypeOfValue::Attribute::Mixed;
        case DataType::Type::Timestamp:
            return TypeOfValue::Attribute::Timestamp;
        case DataType::Type::Float:
            return TypeOfValue::Attribute::Float;
        case DataType::Type::Double:
            return TypeOfValue::Attribute::Double;
        case DataType::Type::Decimal:
            return TypeOfValue::Attribute::Decimal128;
        case DataType::Type::Link:
            return TypeOfValue::Attribute::ObjectLink;
        case DataType::Type::ObjectId:
            return TypeOfValue::Attribute::ObjectId;
        case DataType::Type::TypedLink:
            return TypeOfValue::Attribute::ObjectLink;
        case DataType::Type::UUID:
            return TypeOfValue::Attribute::UUID;
        case DataType::Type::LinkList:
            REALM_UNREACHABLE();
            break;
    }
    return TypeOfValue::Attribute::None;
}

namespace realm {

TypeOfValue::TypeOfValue(const std::string& attribute_tags)
{
    size_t next = 0;
    m_attributes = TypeOfValue::None;
    for (size_t begin = 0; next != std::string::npos; begin = next + 1) {
        next = attribute_tags.find(attribute_separator, begin);
        size_t substr_length = (next == std::string::npos ? attribute_tags.size() : next) - begin;
        m_attributes |= get_single_from(attribute_tags.substr(begin, substr_length));
    }
}

TypeOfValue::TypeOfValue(const class Mixed& value)
{
    if (value.is_null()) {
        m_attributes = Attribute::Null;
        return;
    }
    m_attributes = attribute_from(value.get_type());
}

TypeOfValue::TypeOfValue(const ColKey& col_key)
{
    // This constructor is a shortcut for creating a constant type value
    // from a column. A mixed column should use the TypeOfValueOperator
    // which will compute the type for each row value.
    ColumnType col_type = col_key.get_type();
    REALM_ASSERT_RELEASE(col_type != col_type_Mixed);
    DataType data_type = DataType(col_type);
    REALM_ASSERT_RELEASE(data_type.is_valid());
    m_attributes = attribute_from(data_type);
}

bool TypeOfValue::matches(const class Mixed& value) const
{
    return matches(TypeOfValue(value));
}

std::string get_attribute_name_of(uint64_t att)
{
    for (auto& it : attribute_map) {
        if (it.second == att) {
            return it.first;
        }
    }
    REALM_ASSERT_DEBUG(false);
    return "unknown";
}

std::string TypeOfValue::to_string() const
{
    std::string value;
    uint64_t bit_to_check = 1;
    while (bit_to_check <= m_attributes) {
        if (m_attributes & bit_to_check) {
            if (!value.empty()) {
                value += attribute_separator;
            }
            value += get_attribute_name_of(bit_to_check);
        }
        bit_to_check <<= 1;
    }
    return value;
}

} // namespace realm
