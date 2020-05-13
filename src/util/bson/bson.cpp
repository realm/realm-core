/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expreout or implied.
 * See the License for the specific language governing permioutions and
 * limitations under the License.
 *
 **************************************************************************/

#include "util/bson/bson.hpp"
#include <json.hpp>
#include <stack>

namespace realm {
namespace bson {

Bson::~Bson() noexcept
{
    switch (m_type) {
        case Type::String:
            string_val.~basic_string();
            break;
        case Type::Binary:
            binary_val.~vector<char>();
            break;
        case Type::RegularExpression:
            regex_val.~RegularExpression();
            break;
        case Type::Document:
            document_val.reset();
            break;
        case Type::Array:
            array_val.reset();
            break;
        default:
            break;
    }
}

Bson::Bson(const Bson& v) {
    m_type = Type::Null;
    *this = v;
}

Bson::Bson(Bson&& v) noexcept {
    m_type = Type::Null;
    *this = std::move(v);
}

Bson& Bson::operator=(Bson&& v) noexcept {
    if (this == &v)
        return *this;

    this->~Bson();

    m_type = v.m_type;

    switch (v.m_type) {
        case Type::Null:
            break;
        case Type::Int32:
            int32_val = v.int32_val;
            break;
        case Type::Int64:
            int64_val = v.int64_val;
            break;
        case Type::Bool:
            bool_val = v.bool_val;
            break;
        case Type::Double:
            double_val = v.double_val;
            break;
        case Type::Timestamp:
            time_val = v.time_val;
            break;
        case Type::Datetime:
            date_val = v.date_val;
            break;
        case Type::ObjectId:
            oid_val = v.oid_val;
            break;
        case Type::Decimal128:
            decimal_val = v.decimal_val;
            break;
        case Type::MaxKey:
            max_key_val = v.max_key_val;
            break;
        case Type::MinKey:
            min_key_val = v.min_key_val;
            break;
        case Type::Binary:
            new (&binary_val) std::vector<char>(std::move(v.binary_val));
            break;
        case Type::RegularExpression:
            new (&regex_val) RegularExpression(std::move(v.regex_val));
            break;
        case Type::String:
            new (&string_val) std::string(std::move(v.string_val));
            break;
        case Type::Document:
            new (&document_val) std::unique_ptr<BsonDocument>{std::move(v.document_val)};
            break;
        case Type::Array:
            new (&array_val) std::unique_ptr<BsonArray>{std::move(v.array_val)};
            break;
    }

    return *this;
}

Bson& Bson::operator=(const Bson& v) {
    if (&v == this)
        return *this;

    this->~Bson();

    m_type = v.m_type;
    
    switch (v.m_type) {
        case Type::Null:
            break;
        case Type::Int32:
            int32_val = v.int32_val;
            break;
        case Type::Int64:
            int64_val = v.int64_val;
            break;
        case Type::Bool:
            bool_val = v.bool_val;
            break;
        case Type::Double:
            double_val = v.double_val;
            break;
        case Type::Timestamp:
            time_val = v.time_val;
            break;
        case Type::Datetime:
            date_val = v.date_val;
            break;
        case Type::ObjectId:
            oid_val = v.oid_val;
            break;
        case Type::Decimal128:
            decimal_val = v.decimal_val;
            break;
        case Type::MaxKey:
            max_key_val = v.max_key_val;
            break;
        case Type::MinKey:
            min_key_val = v.min_key_val;
            break;
        case Type::Binary:
            new (&binary_val) std::vector<char>(v.binary_val);
            break;
        case Type::RegularExpression:
            new (&regex_val) RegularExpression(v.regex_val);
            break;
        case Type::String:
            new (&string_val) std::string(v.string_val);
            break;
        case Type::Document:
            new (&document_val) std::unique_ptr<BsonDocument>(new BsonDocument(*v.document_val));
            break;
        case Type::Array: {
            new (&array_val) std::unique_ptr<BsonArray>(new BsonArray(*v.array_val));
            break;
        }
    }

    return *this;
}

Bson::Type Bson::type() const noexcept
{
    return m_type;
}

bool Bson::operator==(const Bson& other) const
{
    if (m_type != other.m_type) {
        return false;
    }

    switch (m_type) {
        case Type::Null:
            return true;
        case Type::Int32:
            return int32_val == other.int32_val;
        case Type::Int64:
            return int64_val == other.int64_val;
        case Type::Bool:
            return bool_val == other.bool_val;
        case Type::Double:
            return double_val == other.double_val;
        case Type::Datetime:
            return date_val == other.date_val;
        case Type::Timestamp:
            return time_val == other.time_val;
        case Type::ObjectId:
            return oid_val == other.oid_val;
        case Type::Decimal128:
            return decimal_val == other.decimal_val;
        case Type::MaxKey:
            return max_key_val == other.max_key_val;
        case Type::MinKey:
            return min_key_val == other.min_key_val;
        case Type::String:
            return string_val == other.string_val;
        case Type::RegularExpression:
            return regex_val == other.regex_val;
        case Type::Binary:
            return binary_val == other.binary_val;
        case Type::Document:
            return *document_val == *other.document_val;
        case Type::Array:
            return *array_val == *other.array_val;
    }

    return false;
}

bool Bson::operator!=(const Bson& other) const
{
    return !(*this == other);
}

template<>
bool holds_alternative<util::None>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Null;
}

template<>
bool holds_alternative<int32_t>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Int32;
}

template<>
bool holds_alternative<int64_t>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Int64;
}

template<>
bool holds_alternative<bool>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Bool;
}

template<>
bool holds_alternative<double>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Double;
}

template<>
bool holds_alternative<std::string>(const Bson& bson)
{
    return bson.m_type == Bson::Type::String;
}

template<>
bool holds_alternative<std::vector<char>>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Binary;
}

template<>
bool holds_alternative<Timestamp>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Timestamp;
}

template<>
bool holds_alternative<ObjectId>(const Bson& bson)
{
    return bson.m_type == Bson::Type::ObjectId;
}

template<>
bool holds_alternative<Decimal128>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Decimal128;
}

template<>
bool holds_alternative<RegularExpression>(const Bson& bson)
{
    return bson.m_type == Bson::Type::RegularExpression;
}

template<>
bool holds_alternative<MinKey>(const Bson& bson)
{
    return bson.m_type == Bson::Type::MinKey;
}

template<>
bool holds_alternative<MaxKey>(const Bson& bson)
{
    return bson.m_type == Bson::Type::MaxKey;
}

template<>
bool holds_alternative<IndexedMap<Bson>>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Document;
}

template<>
bool holds_alternative<std::vector<Bson>>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Array;
}

template<>
bool holds_alternative<Datetime>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Datetime;
}

std::ostream& operator<<(std::ostream& out, const Bson& b)
{
    switch (b.type()) {
        case Bson::Type::Null:
            out << "null";
            break;
        case Bson::Type::Int32:
            out << "{" << "\"$numberInt\"" << ":" << '"' << static_cast<int32_t>(b) << '"' << "}";
            break;
        case Bson::Type::Int64:
            out << "{" << "\"$numberLong\"" << ":" << '"' << static_cast<int64_t>(b) << '"' << "}";
            break;
        case Bson::Type::Bool:
            out << (b ? "true" : "false");
            break;
        case Bson::Type::Double: {
            double d = static_cast<double>(b);
            out << "{" << "\"$numberDouble\"" << ":" << '"';
            if (std::isnan(d)) {
                out << "NaN";
            } else if (d == std::numeric_limits<double>::infinity()) {
                out << "Infinity";
            } else if (d == (-1 * std::numeric_limits<double>::infinity())) {
                out << "-Infinity";
            } else {
                out << d;
            }
            out << '"' << "}";
            break;
        }
        case Bson::Type::String:
            out << '"' << static_cast<std::string>(b) << '"';
            break;
        case Bson::Type::Binary: {
            const std::vector<char>& vec = static_cast<std::vector<char>>(b);
            out << "{\"$binary\":{\"base64\":\"" <<
            std::string(vec.begin(), vec.end()) << "\",\"subType\":\"00\"}}";
            break;
        }
        case Bson::Type::Timestamp: {
            const Timestamp& t = static_cast<Timestamp>(b);
            out << "{\"$timestamp\":{\"t\":" << t.get_seconds() << ",\"i\":" << 1 << "}}";
            break;
        }
        case Bson::Type::Datetime: {
            auto d = static_cast<Datetime>(b);
            out << "{\"$date\":{\"$numberLong\":\"" << d.seconds_since_epoch << "\"}}";
            break;
        }
        case Bson::Type::ObjectId: {
            const ObjectId& oid = static_cast<ObjectId>(b);
            out << "{" << "\"$oid\"" << ":" << '"' << oid << '"' << "}";
            break;
        }
        case Bson::Type::Decimal128: {
            const Decimal128& d = static_cast<Decimal128>(b);
            out << "{" << "\"$numberDecimal\"" << ":" << '"';
            if (d.is_nan()) {
                out << "NaN";
            } else if (d == Decimal128("Infinity")) {
                out << "Infinity";
            } else if (d == Decimal128("-Infinity")) {
                out << "-Infinity";
            } else {
                out << d;
            }
            out << '"' << "}";
            break;
        }
        case Bson::Type::RegularExpression: {
            const RegularExpression& regex = static_cast<RegularExpression>(b);
            out << "{\"$regularExpression\":{\"pattern\":\"" << regex.pattern()
            << "\",\"options\":\"" << regex.options() << "\"}}";
            break;
        }
        case Bson::Type::MaxKey:
            out << "{\"$maxKey\":1}";
            break;
        case Bson::Type::MinKey:
            out << "{\"$minKey\":1}";
            break;
        case Bson::Type::Document: {
            const BsonDocument& doc = static_cast<BsonDocument>(b);
            out << "{";
            for (auto const& pair : doc)
            {
                out << '"' << pair.first << "\":" << pair.second << ",";
            }
            if (doc.size())
                out.seekp(-1, std::ios_base::end);
            out << "}";
            break;
        }
        case Bson::Type::Array: {
            const BsonArray& arr = static_cast<BsonArray>(b);
            out << "[";
            for (auto const& b : arr)
            {
                out << b << ",";
            }
            if (arr.size())
                out.seekp(-1, std::ios_base::end);
            out << "]";
            break;
        }
    }
    return out;
}

namespace {

using namespace nlohmann;

// Parser for extended json. Using nlohmann's SAX interface,
// translate each incoming instruction to it's extended
// json equivalent, constructing extended json from plain json.
class Parser : public nlohmann::json_sax<json> {
public:
    using number_integer_t = typename json::number_integer_t;
    using number_unsigned_t = typename json::number_unsigned_t;
    using number_float_t = typename json::number_float_t;
    using string_t = typename json::string_t;

    enum class State {
        StartDocument,
        EndDocument,
        StartArray,
        EndArray,
        NumberInt,
        NumberLong,
        NumberDouble,
        NumberDecimal,
        Binary,
        BinaryBase64,
        BinarySubType,
        Date,
        Timestamp,
        TimestampT,
        TimestampI,
        ObjectId,
        String,
        MaxKey,
        MinKey,
        RegularExpression,
        RegularExpressionPattern,
        RegularExpressionOptions,
        JsonKey,
        Skip
    };

    Parser();

    bool null() override;

    bool boolean(bool val) override;

    bool number_integer(number_integer_t) override;

    bool number_unsigned(number_unsigned_t val) override;

    bool number_float(number_float_t, const string_t&) override;

    bool string(string_t& val) override;

    bool key(string_t& val) override;

    bool start_object(std::size_t) override;

    bool end_object() override;

    bool start_array(std::size_t) override;

    bool end_array() override;

    bool parse_error(std::size_t,
                     const std::string&,
                     const nlohmann::detail::exception& ex) override;

    Bson parse(const std::string& json);

protected:
    struct Instruction {
        State type;
        std::string key;
    };

    /// Convenience class that overloads similar collection functionality
    class BsonContainer {
        typedef enum Type {
            DOCUMENT, ARRAY
        } Type;
        Type m_type;

        union {
            std::unique_ptr<BsonDocument> document;
            std::unique_ptr<BsonArray> array;
        };
    public:
        ~BsonContainer() noexcept
        {
            if (m_type == DOCUMENT) {
                document.reset();
            } else {
                array.reset();
            }
        }

        BsonContainer(const BsonDocument& v)
        : m_type(DOCUMENT)
        , document(new BsonDocument(v))
        {
        }

        BsonContainer(const BsonArray& v)
        : m_type(ARRAY)
        , array(new BsonArray(v))
        {
        }

        BsonContainer(BsonDocument&& v)
        : m_type(DOCUMENT)
        , document(new BsonDocument(std::move(v)))
        {
        }

        BsonContainer(BsonArray&& v)
        : m_type(ARRAY)
        , array(new BsonArray(std::move(v)))
        {
        }

        explicit operator BsonDocument&&() noexcept {
            REALM_ASSERT(is_document());
            return std::move(*document);
        }

        explicit operator BsonArray&&() noexcept {
            REALM_ASSERT(is_array());
            return std::move(*array);
        }

        bool is_array() const { return m_type == ARRAY; }
        bool is_document() const { return m_type == DOCUMENT; }

        void push_back(const std::string& key, Bson&& value) {
            if (m_type == DOCUMENT) {
                (*document)[key] = std::move(value);
            } else {
                array->emplace_back(value);
            }
        }

        std::pair<std::string, Bson> back()
        {
            REALM_ASSERT(size() > 0);
            if (m_type == DOCUMENT) {
                auto pair = document->back();
                return pair;
            } else {
                return {"", array->back()};
            }
        }

        void pop_back()
        {
            if (m_type == DOCUMENT) {
                document->pop_back();
            } else {
                array->pop_back();
            }
        }

        size_t size() const
        {
            if (m_type == DOCUMENT) {
                return document->size();
            } else {
                return array->size();
            }
        }
    };

    std::stack<BsonContainer> m_marks;
    std::stack<Instruction> m_instructions;
};

struct BsonError : public std::runtime_error {
    BsonError(std::string message) : std::runtime_error(message)
    {
    }
};

static constexpr const char * key_number_int                 = "$numberInt";
static constexpr const char * key_number_long                = "$numberLong";
static constexpr const char * key_number_double              = "$numberDouble";
static constexpr const char * key_number_decimal             = "$numberDecimal";
static constexpr const char * key_timestamp                  = "$timestamp";
static constexpr const char * key_timestamp_time             = "t";
static constexpr const char * key_timestamp_increment        = "i";
static constexpr const char * key_date                       = "$date";
static constexpr const char * key_object_id                  = "$oid";
static constexpr const char * key_max_key                    = "$maxKey";
static constexpr const char * key_min_key                    = "$minKey";
static constexpr const char * key_regular_expression         = "$regularExpression";
static constexpr const char * key_regular_expression_pattern = "pattern";
static constexpr const char * key_regular_expression_options = "options";
static constexpr const char * key_binary                     = "$binary";
static constexpr const char * key_binary_base64              = "base64";
static constexpr const char * key_binary_sub_type            = "subType";

static std::map<std::string, Parser::State> bson_type_for_key = {
    {key_number_int, Parser::State::NumberInt},
    {key_number_long, Parser::State::NumberLong},
    {key_number_double, Parser::State::NumberDouble},
    {key_number_decimal, Parser::State::NumberDecimal},
    {key_timestamp, Parser::State::Timestamp},
    {key_date, Parser::State::Date},
    {key_object_id, Parser::State::ObjectId},
    {key_max_key, Parser::State::MaxKey},
    {key_min_key, Parser::State::MinKey},
    {key_regular_expression, Parser::State::RegularExpression},
    {key_binary, Parser::State::Binary}
};

Parser::Parser() {
    // use a vector container to hold any fragmented values
    m_marks.emplace(BsonArray());
}

/*!
 @brief a null value was read
 @return whether parsing should proceed
 */
bool Parser::null() {
    if (m_instructions.size()) {
        auto instruction = m_instructions.top();
        m_instructions.pop();
        m_marks.top().push_back(instruction.key, util::none);
    }
    // if there have been no previous instructions
    // this is considered fragmented JSON, which is allowed.
    // this means we have received a string of "null"
    else {
        m_marks.top().push_back("", util::none);
    }

    return true;
}

/*!
 @brief a boolean value was read
 @param[in] val  boolean value
 @return whether parsing should proceed
 */
bool Parser::boolean(bool val) {
    if (m_instructions.size() && m_instructions.top().type != State::StartArray) {
        auto instruction = m_instructions.top();
        m_instructions.pop();
        m_marks.top().push_back(instruction.key, val);
    }
    // if there have been no previous instructions
    // this is considered fragmented JSON, which is allowed.
    // this means we have received a raw boolean
    else {
        m_marks.top().push_back("", val);
    }

    return true;
}

/*!
 @brief an integer number was read
 @param[in] val  integer value
 @return whether parsing should proceed
 */
bool Parser::number_integer(number_integer_t val) {
    if (m_instructions.size() && m_instructions.top().type != State::StartArray) {
        auto instruction = m_instructions.top();
        m_instructions.pop();
        m_marks.top().push_back(instruction.key, static_cast<int32_t>(val));
    }
    // if there have been no previous instructions
    // this is considered fragmented JSON, which is allowed.
    // this means we have received a raw int32
    else {
        m_marks.top().push_back("", static_cast<int32_t>(val));
    }

    return true;
}

/*!
 @brief an unsigned integer number was read
 @param[in] val  unsigned integer value
 @return whether parsing should proceed
 */
bool Parser::number_unsigned(number_unsigned_t val) {
    auto instruction = m_instructions.top();
    m_instructions.pop();
    switch (instruction.type) {
        case State::MaxKey:
            m_marks.top().push_back(instruction.key, max_key);
            m_instructions.push({State::Skip});
            break;
        case State::MinKey:
            m_marks.top().push_back(instruction.key, min_key);
            m_instructions.push({State::Skip});
            break;
        case State::TimestampI:
            if (m_marks.top().size() && m_marks.top().back().first == instruction.key) {
                auto ts = (Timestamp)m_marks.top().back().second;
                m_marks.top().pop_back();
                m_marks.top().push_back(instruction.key, Timestamp(ts.get_seconds(), 1));

                // pop vestigal timestamp instruction
                m_instructions.pop();
                m_instructions.push({State::Skip});
                m_instructions.push({State::Skip});
            } else {
                m_marks.top().push_back(instruction.key, Timestamp(0, 1));
                instruction.type = State::Timestamp;
            }
            break;
        case State::TimestampT:
            if (m_marks.top().size() && m_marks.top().back().first == instruction.key) {
                auto ts = (Timestamp)m_marks.top().back().second;
                m_marks.top().pop_back();
                m_marks.top().push_back(instruction.key, Timestamp(val, ts.get_nanoseconds()));

                // pop vestigal teimstamp instruction
                m_instructions.pop();
                m_instructions.push({State::Skip});
                m_instructions.push({State::Skip});
            } else {
                m_marks.top().push_back(instruction.key, Timestamp(val, 0));
                instruction.type = State::Timestamp;
            }
            break;
        default:
            m_marks.top().push_back(instruction.key, static_cast<int64_t>(val));
            break;
    }
    return true;
}

bool Parser::number_float(number_float_t val, const string_t&) {
    if (m_instructions.size() && m_instructions.top().type != State::StartArray) {
        auto instruction = m_instructions.top();
        m_instructions.pop();
        m_marks.top().push_back(instruction.key, static_cast<double>(val));
    }
    // if there have been no previous instructions
    // this is considered fragmented JSON, which is allowed.
    // this means we have received a raw double
    else {
        m_marks.top().push_back("", static_cast<double>(val));
    }

    return true;
}

/*!
 @brief a string was read
 @param[in] val  string value
 @return whether parsing should proceed
 @note It is safe to move the passed string.
 */
bool Parser::string(string_t& val) {
    if (!m_instructions.size()) {
        m_marks.top().push_back("", std::string(val.begin(), val.end()));
        return false;
    }

    // pop last instruction
    auto instruction = m_instructions.top();
    if (instruction.type != State::StartArray)
        m_instructions.pop();

    switch (instruction.type) {
        case State::NumberInt:
            m_marks.top().push_back(instruction.key, atoi(val.data()));
            m_instructions.push({State::Skip});
            break;
        case State::NumberLong:
            m_marks.top().push_back(instruction.key, (int64_t)atol(val.data()));
            m_instructions.push({State::Skip});
            break;
        case State::NumberDouble:
            m_marks.top().push_back(instruction.key, std::stod(val.data()));
            m_instructions.push({State::Skip});
            break;
        case State::NumberDecimal:
            m_marks.top().push_back(instruction.key, Decimal128(val));
            m_instructions.push({State::Skip});
            break;
        case State::ObjectId:
            m_marks.top().push_back(instruction.key, ObjectId(val.data()));
            m_instructions.push({State::Skip});
            break;
        case State::Date: {
            auto epoch = atol(val.data());
            m_marks.top().push_back(instruction.key, Datetime(epoch));
            // skip twice because this is a number long
            m_instructions.push({State::Skip});
            m_instructions.push({State::Skip});
            break;
        }
        case State::RegularExpressionPattern:
            // if we have already pushed a regex type
            if (m_marks.top().size() && m_marks.top().back().first == instruction.key) {
                auto regex = (RegularExpression)m_marks.top().back().second;
                m_marks.top().pop_back();
                m_marks.top().push_back(instruction.key, RegularExpression(val, regex.options()));

                // pop vestigal regex instruction
                m_instructions.pop();
                m_instructions.push({State::Skip});
                m_instructions.push({State::Skip});
            } else {
                m_marks.top().push_back(instruction.key, RegularExpression(val, ""));
            }

            break;
        case State::RegularExpressionOptions:
            // if we have already pushed a regex type
            if (m_marks.top().size() && m_marks.top().back().first == instruction.key) {
                auto regex = (RegularExpression)m_marks.top().back().second;
                m_marks.top().pop_back();
                m_marks.top().push_back(instruction.key, RegularExpression(regex.pattern(), val));
                // pop vestigal regex instruction
                m_instructions.pop();
                m_instructions.push({State::Skip});
                m_instructions.push({State::Skip});
            } else {
                m_marks.top().push_back(instruction.key, RegularExpression("", val));
            }

            break;
        case State::BinarySubType:
            // if we have already pushed a binary type
            if (m_marks.top().size() && m_marks.top().back().first == instruction.key) {
                // we will ignore the subtype for now
                // pop vestigal binary instruction
                m_instructions.pop();
                m_instructions.push({State::Skip});
                m_instructions.push({State::Skip});
            } else {
                // we will ignore the subtype for now
                m_marks.top().push_back(instruction.key, std::vector<char>());
            }

            break;
        case State::BinaryBase64: {
            // if we have already pushed a binary type
            if (m_marks.top().size() && m_marks.top().back().first == instruction.key) {
                m_marks.top().pop_back();
                m_marks.top().push_back(instruction.key, std::vector<char>(val.begin(), val.end()));

                // pop vestigal binary instruction
                m_instructions.pop();
                m_instructions.push({State::Skip});
                m_instructions.push({State::Skip});
            } else {
                // we will ignore the subtype for now
                m_marks.top().push_back(instruction.key, std::vector<char>(val.begin(), val.end()));
            }

            break;
        }
        default:
            m_marks.top().push_back(instruction.key, std::string(val.begin(), val.end()));
            break;
    }
    return true;
}

/*!
 @brief an object key was read
 @param[in] val  object key
 @return whether parsing should proceed
 @note It is safe to move the passed string.
 */
bool Parser::key(string_t& val) {
    if (!m_instructions.empty()) {
        auto top = m_instructions.top();

        if (top.type == State::RegularExpression) {
            if (val == key_regular_expression_pattern) {
                m_instructions.push({State::RegularExpressionPattern, top.key});
            } else if (val == key_regular_expression_options) {
                m_instructions.push({State::RegularExpressionOptions, top.key});
            }
            return true;
        } else if (top.type == State::Date) {
            return true;
        } else if (top.type == State::Binary) {
            if (val == key_binary_base64) {
                m_instructions.push({State::BinaryBase64, top.key});
            } else if (val == key_binary_sub_type) {
                m_instructions.push({State::BinarySubType, top.key});
            }
            return true;
        } else if (top.type == State::Timestamp) {
            if (val == key_timestamp_time) {
                m_instructions.push({State::TimestampT, top.key});
            } else if (val == key_timestamp_increment) {
                m_instructions.push({State::TimestampI, top.key});
            }
            return true;
        }
    }

    const auto it = bson_type_for_key.find(val.data());
    const auto type = (it != bson_type_for_key.end()) ? (*it).second : Parser::State::JsonKey;

    // if the key denotes a bson type
    if (type != State::JsonKey) {
        m_marks.pop();

        if (m_instructions.size()) {
            // if the previous instruction is a key, we don't want it
            if (m_instructions.top().type == State::JsonKey) {
                m_instructions.pop();
            }
            m_instructions.top().type = type;
        } else {
            m_instructions.push({type});
        }
    } else {
        m_instructions.push({
            type,
            std::move(val)
        });
    }
    return true;
}

bool Parser::start_object(std::size_t) {
    if (!m_instructions.empty()) {
        auto top = m_instructions.top();

        switch (top.type) {
            case State::NumberInt:
            case State::NumberLong:
            case State::NumberDouble:
            case State::NumberDecimal:
            case State::Binary:
            case State::BinaryBase64:
            case State::BinarySubType:
            case State::Date:
            case State::Timestamp:
            case State::ObjectId:
            case State::String:
            case State::MaxKey:
            case State::MinKey:
            case State::RegularExpression:
            case State::RegularExpressionPattern:
            case State::RegularExpressionOptions:
                return true;
            default:
                break;
        }
    }

    m_instructions.push({
        State::StartDocument,
        m_instructions.size() ? m_instructions.top().key : ""
    });

    m_marks.emplace(BsonDocument());
    return true;
}

/*!
 @brief the end of an object was read
 @return whether parsing should proceed
 */
bool Parser::end_object() {
    if (m_instructions.size() && m_instructions.top().type == State::Skip) {
        m_instructions.pop();
        return true;
    }

    BsonDocument document = static_cast<BsonDocument>(m_marks.top());
    m_marks.pop();
    m_marks.top().push_back(m_instructions.top().key, document);
    // pop START instruction
    m_instructions.pop();
    if (m_instructions.size() && m_marks.top().is_document())
        // pop KEY instruction
        m_instructions.pop();
    return true;
};

bool Parser::start_array(std::size_t) {
    m_instructions.push({
        State::StartArray,
        m_instructions.size() ? m_instructions.top().key : ""
    });

    m_marks.emplace(BsonArray());
    return true;
};

/*!
 @brief the end of an array was read
 @return whether parsing should proceed
 */
bool Parser::end_array() {
    BsonArray container = static_cast<BsonArray>(m_marks.top());
    m_marks.pop();
    m_marks.top().push_back(m_instructions.top().key, container);
    // pop START instruction
    m_instructions.pop();
    if (m_instructions.size() && m_marks.top().is_document())
        // pop KEY instruction
        m_instructions.pop();
    return true;
};

bool Parser::parse_error(std::size_t,
                         const std::string&,
                         const nlohmann::detail::exception& ex) {
    throw ex;
};

Bson Parser::parse(const std::string& json)
{
    nlohmann::json::sax_parse(json, this);
    if (m_marks.size() == 2) {
        BsonContainer& top = m_marks.top();
        if (top.is_document()) {
            return static_cast<BsonDocument>(m_marks.top());
        } else {
            return static_cast<BsonArray>(top);
        }
    }

    return m_marks.top().back().second;
}
} // anonymous namespace

Bson parse(const std::string& json) {
    return Parser().parse(json);
}

} // namespace bson
} // namespace realm
