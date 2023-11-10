/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#include <realm/util/bson/bson.hpp>
#include <realm/util/bson/indexed_map.hpp>
#include <realm/object-store/results.hpp>
#include "query_ast.hpp"

namespace realm {

using namespace query_parser;

namespace {

std::map<std::string, CompareType> equal_operators = {
    {"$eq", CompareType::EQUAL},
    {"$in", CompareType::IN},
    {"$ne", CompareType::NOT_EQUAL},
};

std::map<std::string, CompareType> relational_operators = {
    {"$gt", CompareType::GREATER},
    {"$gte", CompareType::GREATER_EQUAL},
    {"$lt", CompareType::LESS},
    {"$lte", CompareType::LESS_EQUAL},
};

enum class QueryLogicalOperators { $and, $not, $nor, $or };

std::map<std::string, QueryLogicalOperators> logical_operators = {
    {"$and", QueryLogicalOperators::$and},
    {"$or", QueryLogicalOperators::$or},
    {"$nor", QueryLogicalOperators::$nor},
    {"$not", QueryLogicalOperators::$not},
};

class BsonConstant : public ConstantNode {
public:
    BsonConstant(bson::Bson b)
        : value(b)
    {
    }

    std::unique_ptr<Subexpr> visit(ParserDriver*, DataType) override
    {
        switch (value.type()) {
            case bson::Bson::Type::Int32:
                return std::make_unique<Value<int64_t>>(int32_t(value));
            case bson::Bson::Type::Int64:
                return std::make_unique<Value<int64_t>>(int64_t(value));
            case bson::Bson::Type::Bool:
                return std::make_unique<Value<Bool>>(bool(value));
            case bson::Bson::Type::Double:
                return std::make_unique<Value<double>>(double(value));
            case bson::Bson::Type::String:
                return std::make_unique<ConstantStringValue>(std::string(value));
            case bson::Bson::Type::Binary: {
                auto data = static_cast<std::vector<char>>(value);
                return std::make_unique<ConstantBinaryValue>(BinaryData(data.data(), data.size()));
            }
            case bson::Bson::Type::Timestamp:
            case bson::Bson::Type::Datetime:
                return std::make_unique<Value<Timestamp>>(Timestamp(value));
            case bson::Bson::Type::ObjectId:
                return std::make_unique<Value<ObjectId>>(ObjectId(value));
            case bson::Bson::Type::Decimal128:
                return std::make_unique<Value<Decimal128>>(Decimal128(value));
            case bson::Bson::Type::Uuid:
                return std::make_unique<Value<UUID>>(UUID(value));
            case bson::Bson::Type::Null:
                return std::make_unique<Value<null>>(realm::null());
            default:
                throw Exception(ErrorCodes::Error::MalformedJson, "Unsupported Bson Type");
        }
        return {};
    }

private:
    bson::Bson value;
};

static inline const char* find_chr(const char* p, char c)
{
    while (*p && *p != c) {
        ++p;
    }
    return p;
}

static std::vector<std::string> split(const char* path)
{
    std::vector<std::string> ret;
    do {
        auto p = find_chr(path, '.');
        ret.emplace_back(path, p);
        path = p;
    } while (*path++ == '.');
    return ret;
}

} // namespace

std::vector<QueryNode*> ParserDriver::get_query_nodes(const bson::BsonArray& bson_array)
{
    std::vector<QueryNode*> ret;
    for (const auto& document : bson_array) {
        ret.emplace_back(get_query_node(static_cast<bson::BsonDocument>(document)));
    }
    return ret;
}

void ParserDriver::parse(const bson::BsonDocument& document)
{
    result = get_query_node(document);
}

QueryNode* ParserDriver::get_query_node(const bson::BsonDocument& document)
{
    QueryNode* ret = nullptr;
    for (const auto& [key, value] : document) {
        QueryNode* node = nullptr;
        // top level document will contain either keys to compare values against,
        // or logical operators like $and or $or that will contain an array of query ops
        if (logical_operators.count(key)) {
            switch (logical_operators[key]) {
                case QueryLogicalOperators::$and: {
                    REALM_ASSERT(value.type() == bson::Bson::Type::Array);
                    std::vector<QueryNode*> nodes = get_query_nodes(static_cast<bson::BsonArray>(value));
                    REALM_ASSERT(nodes.size() >= 2);
                    node = m_parse_nodes.create<AndNode>(nodes[0], nodes[1]);
                    break;
                }
                case QueryLogicalOperators::$not:
                    node = m_parse_nodes.create<NotNode>(get_query_node(static_cast<bson::BsonDocument>(value)));
                    break;
                case QueryLogicalOperators::$nor:
                    break;
                case QueryLogicalOperators::$or: {
                    REALM_ASSERT(value.type() == bson::Bson::Type::Array);
                    std::vector<QueryNode*> nodes = get_query_nodes(static_cast<bson::BsonArray>(value));
                    REALM_ASSERT(nodes.size() >= 2);
                    node = m_parse_nodes.create<OrNode>(nodes[0], nodes[1]);
                    break;
                }
            }
        }
        else {
            auto path = m_parse_nodes.create<PathNode>();
            auto path_elements = split(key.c_str());
            for (auto& elem : path_elements) {
                path->add_element(elem);
            }
            auto prop = m_parse_nodes.create<PropertyNode>(path);
            // if the value type is a document, we expect that it is a
            // "query document". if it's not, we assume they are doing a value comparison
            if (value.type() == bson::Bson::Type::Document) {
                const auto& doc = static_cast<bson::BsonDocument>(value);
                auto [key, val] = doc[0];
                ValueNode* right;
                if (val.type() == bson::Bson::Type::Array) {
                    auto list = m_parse_nodes.create<ListNode>();
                    for (const auto& document : static_cast<bson::BsonArray>(val)) {
                        list->add_element(m_parse_nodes.create<BsonConstant>(document));
                    }
                    right = list;
                }
                else {
                    right = m_parse_nodes.create<BsonConstant>(val);
                }
                if (equal_operators.count(key)) {
                    node = m_parse_nodes.create<EqualityNode>(prop, equal_operators[key], right);
                }
                else {
                    node = m_parse_nodes.create<RelationalNode>(prop, relational_operators[key], right);
                }
            }
            else {
                node = m_parse_nodes.create<EqualityNode>(prop, CompareType::EQUAL,
                                                          m_parse_nodes.create<BsonConstant>(value));
            }
        }
        if (ret) {
            ret = m_parse_nodes.create<AndNode>(ret, node);
        }
        else {
            ret = node;
        }
    }
    return ret;
}

Query Table::query(const bson::BsonDocument& document) const
{
    NoArguments args;
    ParserDriver driver(m_own_ref, args, {});
    driver.parse(document);
    driver.result->canonicalize();
    return driver.result->visit(&driver);
}

Results Results::find(const bson::BsonDocument& document) const
{
    return filter(m_table->query(document));
}

Results Results::find(const std::string& document) const
{
    const char* p = document.c_str();
    while (isspace(*p))
        ++p;
    if (*p == '{') {
        // This seems to be MQL
        return find(static_cast<bson::BsonDocument>(bson::parse(document)));
    }
    else {
        // Try good old RQL
        return filter(m_table->query(document));
    }
}

} // namespace realm
