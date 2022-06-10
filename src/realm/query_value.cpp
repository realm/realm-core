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

#include <realm/query_value.hpp>

#include <realm/mixed.hpp>

#include <algorithm>
#include <numeric>
#include <unordered_map>

using namespace realm;

namespace {

// These keys must be stored as lowercase. Some naming comes from MongoDB's conventions
// see https://docs.mongodb.com/manual/reference/operator/query/type/
static const std::vector<std::pair<std::string, TypeOfValue::Attribute>> attribute_map = {
    {"null", TypeOfValue::Null},
    {"int", TypeOfValue::Int},
    {"integer", TypeOfValue::Int},
    {"int16", TypeOfValue::Int},
    {"int32", TypeOfValue::Int},
    {"int64", TypeOfValue::Int},
    {"short", TypeOfValue::Int},
    {"long", TypeOfValue::Int},
    {"byte", TypeOfValue::Int},
    {"char", TypeOfValue::Int},
    {"bool", TypeOfValue::Bool},
    {"boolean", TypeOfValue::Bool},
    {"string", TypeOfValue::String},
    {"binary", TypeOfValue::Binary},
    {"data", TypeOfValue::Binary},
    {"bytearray", TypeOfValue::Binary},
    {"byte[]", TypeOfValue::Binary},
    {"date", TypeOfValue::Timestamp},
    {"datetimeoffset", TypeOfValue::Timestamp},
    {"timestamp", TypeOfValue::Timestamp},
    {"float", TypeOfValue::Float},
    {"double", TypeOfValue::Double},
    {"decimal128", TypeOfValue::Decimal128},
    {"decimal", TypeOfValue::Decimal128},
    {"object", TypeOfValue::ObjectLink},
    {"link", TypeOfValue::ObjectLink},
    {"objectid", TypeOfValue::ObjectId},
    {"uuid", TypeOfValue::UUID},
    {"guid", TypeOfValue::UUID},
    {"numeric", TypeOfValue::Numeric},
    {"bindata", TypeOfValue::Binary}};

TypeOfValue::Attribute get_single_from(std::string str)
{
    std::transform(str.begin(), str.end(), str.begin(), toLowerAscii);
    auto it = std::find_if(attribute_map.begin(), attribute_map.end(), [&](const auto& attr_pair) {
        return attr_pair.first == str;
    });
    if (it == attribute_map.end()) {
        std::string all_keys =
            std::accumulate(std::next(attribute_map.begin()), attribute_map.end(), attribute_map.begin()->first,
                            [](const std::string& s, const auto& it) {
                                return s + ", " + it.first;
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
            throw std::runtime_error("Cannot construct a strongly typed 'TypeOfValue' from ambiguous 'mixed'");
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
            break;
    }
    throw std::runtime_error(util::format("Invalid value '%1' cannot be converted to 'TypeOfValue'", type));
}
} // anonymous namespace

namespace realm {

TypeOfValue::TypeOfValue(int64_t attributes)
    : m_attributes(attributes)
{
    if (m_attributes == 0) {
        throw std::runtime_error("Invalid value 0 found when converting to TypeOfValue; a type must be specified");
    }
}

TypeOfValue::TypeOfValue(const std::string& attribute_tags)
{
    m_attributes = get_single_from(attribute_tags);
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

TypeOfValue::TypeOfValue(const DataType& data_type)
{
    REALM_ASSERT_RELEASE(data_type.is_valid());
    m_attributes = attribute_from(data_type);
}

bool TypeOfValue::matches(const class Mixed& value) const
{
    return matches(TypeOfValue(value));
}

static const std::string* get_attribute_name_of(int64_t att)
{
    for (const auto& it : attribute_map) {
        if (it.second == att) {
            return &it.first;
        }
    }
    return nullptr;
}

std::string TypeOfValue::to_string() const
{
    if (auto value = get_attribute_name_of(m_attributes)) {
        return *value;
    }

    std::vector<std::string> values;
    int64_t bit_to_check = 1;
    while (bit_to_check <= m_attributes) {
        if (m_attributes & bit_to_check) {
            auto val = get_attribute_name_of(bit_to_check);
            REALM_ASSERT_RELEASE_EX(val, bit_to_check);
            values.push_back(*val);
        }
        bit_to_check <<= 1;
    }
    REALM_ASSERT_RELEASE(values.size() > 0);
    if (values.size() == 1) {
        return values[0];
    }
    else {
        return util::format("{%1}", std::accumulate(std::next(values.begin()), values.end(), values[0],
                                                    [](std::string previous, std::string current) {
                                                        return std::move(previous) + ", " + current;
                                                    }));
    }
}

} // namespace realm
