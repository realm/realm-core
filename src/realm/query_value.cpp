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

namespace realm {

// These keys must be stored as lowercase. Some naming comes from MongoDB's conventions
// see https://docs.mongodb.com/manual/reference/operator/query/type/
static std::unordered_map<std::string, TypeOfValue::Attribute> attribute_map = {
    {"null", TypeOfValue::Null},          {"int", TypeOfValue::Int},         {"integer", TypeOfValue::Int},
    {"bool", TypeOfValue::Bool},          {"boolean", TypeOfValue::Bool},    {"string", TypeOfValue::String},
    {"binary", TypeOfValue::Binary},      {"mixed", TypeOfValue::Mixed},     {"timestamp", TypeOfValue::Timestamp},
    {"float", TypeOfValue::Float},        {"double", TypeOfValue::Double},   {"decimal128", TypeOfValue::Decimal128},
    {"decimal", TypeOfValue::Decimal128}, {"link", TypeOfValue::ObjectLink}, {"object", TypeOfValue::ObjectLink},
    {"objectid", TypeOfValue::ObjectId},  {"uuid", TypeOfValue::UUID},       {"numeric", TypeOfValue::Numeric},
    {"long", TypeOfValue::Int},           {"date", TypeOfValue::Timestamp},  {"bindata", TypeOfValue::Binary}};
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
    switch (value.get_type()) {
        case DataType::Type::Int:
            m_attributes = Attribute::Int;
            break;
        case DataType::Type::Bool:
            m_attributes = Attribute::Bool;
            break;
        case DataType::Type::String:
            m_attributes = Attribute::String;
            break;
        case DataType::Type::Binary:
            m_attributes = Attribute::Binary;
            break;
        case DataType::Type::Mixed:
            m_attributes = Attribute::Mixed;
            break;
        case DataType::Type::Timestamp:
            m_attributes = Attribute::Timestamp;
            break;
        case DataType::Type::Float:
            m_attributes = Attribute::Float;
            break;
        case DataType::Type::Double:
            m_attributes = Attribute::Double;
            break;
        case DataType::Type::Decimal:
            m_attributes = Attribute::Decimal128;
            break;
        case DataType::Type::Link:
            m_attributes = Attribute::ObjectLink;
            break;
        case DataType::Type::ObjectId:
            m_attributes = Attribute::ObjectId;
            break;
        case DataType::Type::TypedLink:
            m_attributes = Attribute::ObjectLink;
            break;
        case DataType::Type::UUID:
            m_attributes = Attribute::UUID;
            break;
        case DataType::Type::LinkList:
            REALM_UNREACHABLE();
            return;
    }
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
