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
#include <sstream>
#include <stack>
#include <algorithm>
#include <sstream>

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

std::string Bson::to_string() const
{
    std::stringstream ss;
    ss << *this;
    return ss.str();
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
    return bson.m_type == Bson::Type::Datetime;
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
bool holds_alternative<MongoTimestamp>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Timestamp;
}

struct PrecisionGuard {
    PrecisionGuard(std::ostream& stream, std::streamsize new_precision)
        : stream(stream)
        , old_precision(stream.precision(new_precision))
        {}

    ~PrecisionGuard() {
        stream.precision(old_precision);
    }

    std::ostream& stream;
    std::streamsize old_precision;
};

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
                PrecisionGuard precision_guard(out, std::numeric_limits<double>::max_digits10);
                out << d;
            }
            out << '"' << "}";
            break;
        }
        case Bson::Type::String:
            out << nlohmann::json(b.operator const std::string&()).dump();
            break;
        case Bson::Type::Binary: {
            const std::vector<char>& vec = static_cast<std::vector<char>>(b);
            out << "{\"$binary\":{\"base64\":\"" <<
            std::string(vec.begin(), vec.end()) << "\",\"subType\":\"00\"}}";
            break;
        }
        case Bson::Type::Timestamp: {
            const MongoTimestamp& t = static_cast<MongoTimestamp>(b);
            out << "{\"$timestamp\":{\"t\":" << t.seconds << ",\"i\":" << t.increment << "}}";
            break;
        }
        case Bson::Type::Datetime: {
            auto d = static_cast<realm::Timestamp>(b);

            out << "{\"$date\":{\"$numberLong\":\""
                << ((d.get_seconds() * 1000) + d.get_nanoseconds()/1000000)
                << "\"}}";
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
            bool first = true;
            for (auto const& pair : doc) {
                if (!first)
                    out << ',';
                first = false;
                out << nlohmann::json(pair.first).dump() << ':' << pair.second;
            }
            out << "}";
            break;
        }
        case Bson::Type::Array: {
            const BsonArray& arr = static_cast<BsonArray>(b);
            out << "[";
            bool first = true;
            for (auto const& b : arr) {
                if (!first)
                    out << ',';
                first = false;
                out << b;
            }
            out << "]";
            break;
        }
    }
    return out;
}

std::string Bson::toJson() const {
    std::stringstream s;
    s << *this;
    return s.str();
}

namespace {

struct BsonError : public std::runtime_error {
    BsonError(std::string message) : std::runtime_error(std::move(message))
    {
    }
};

namespace {
// This implements just enough of the map API to support nlohmann's DOM apis that we use.
template <typename K, typename V, typename ...Ignored>
struct LinearMap {
    using key_type = K;
    using mapped_type = V;
    using value_type = std::pair<const K, V>;
    using storage_type = std::vector<value_type>;
    using iterator = typename storage_type::iterator;
    using const_iterator = typename storage_type::const_iterator;

    auto begin() { return _elems.begin(); }
    auto begin() const { return _elems.begin(); }
    auto end() { return _elems.end(); }
    auto end() const { return _elems.end(); }
    auto size() const { return _elems.size(); }
    auto max_size() const { return _elems.max_size(); }
    auto clear() { return _elems.clear(); }
    V& operator[](const K& k) {
        // assume this is only used for adding a new element.
        return _elems.emplace_back(k, V()).second;
    }

    template <typename... Args>
    std::pair<iterator, bool> emplace(Args&&... args) {
        // assume this is only used for adding a new element.
        _elems.emplace_back(std::forward<Args>(args)...);
        return {--_elems.end(), true};
    }

    iterator erase(iterator) {
        // This is only used when mutating the DOM which we don't do.
        REALM_TERMINATE("LinearMap::erase() should never be called");
    }

    storage_type _elems;
};

using Json = nlohmann::basic_json<LinearMap>;

Bson dom_obj_to_bson(const Json& json);

Bson dom_elem_to_bson(const Json& json) {
    switch (json.type()) {
        case Json::value_t::null:
            return Bson();
        case Json::value_t::string:
            return Bson(json.get<std::string>());
        case Json::value_t::boolean:
            return Bson(json.get<bool>());
        case Json::value_t::number_integer:
            return Bson(json.get<int64_t>());
        case Json::value_t::number_unsigned: {
            uint64_t val = json.get<uint64_t>();
            if (val <= uint64_t(std::numeric_limits<int64_t>::max()))
                return Bson(int64_t(val));
            return Bson(double(val));
        }
        case Json::value_t::number_float:
            return Bson(json.get<double>());
        case Json::value_t::object:
            return dom_obj_to_bson(json);
        case Json::value_t::array: {
            std::vector<Bson> out;
            for (auto&& elem : json) {
                out.push_back(dom_elem_to_bson(elem));
            }
            return Bson(std::move(out));
        }
        case Json::value_t::discarded:
            REALM_TERMINATE("should never see discarded");
    }
    REALM_TERMINATE("unknown json value type");
}

// This works around the deleted rvalue constructor in StringData
inline StringData tosd(const std::string& s) { return s; }

// Keep these sorted by key. This is checked so you can't forget.
using FancyParser = Bson(*)(const Json& json);
static constexpr std::pair<std::string_view, FancyParser> bson_fancy_parsers[] = {
    {"$binary", +[](const Json& json) {
        util::Optional<std::vector<char>> base64;
        util::Optional<uint8_t> subType;
        if (json.size() != 2)
            throw BsonError("invalid extended json $binary");
        for (auto&& [k, v] : json.items()) {
            if (k == "base64") {
                const std::string& str = v.get<std::string>();
                base64.emplace(str.begin(), str.end());
            } else if (k == "subType") {
                subType = std::stoul(v.get<std::string>(), nullptr, 16);
            }
        }
        if (!base64 || !subType)
            throw BsonError("invalid extended json $binary");
        return Bson(std::move(*base64)); // TODO don't throw away the subType.
    }},
    {"$date", +[](const Json& json) {
        int64_t millis_since_epoch = dom_elem_to_bson(json).operator int64_t();
        return Bson(realm::Timestamp(millis_since_epoch / 1000,
                                     (millis_since_epoch % 1000) * 1'000'000)); // ms -> ns
    }},
    {"$maxKey", +[](const Json&) { return Bson(MaxKey()); }},
    {"$minKey", +[](const Json&) { return Bson(MinKey()); }},
    {"$numberDecimal", +[](const Json& json) { return Bson(Decimal128(tosd(json.get<std::string>()))); }},
    {"$numberDouble", +[](const Json& json) { return Bson(std::stod(json.get<std::string>())); }},
    {"$numberInt", +[](const Json& json) { return Bson(int32_t(std::stoi(json.get<std::string>()))); }},
    {"$numberLong", +[](const Json& json) { return Bson(int64_t(std::stoll(json.get<std::string>()))); }},
    {"$oid", +[](const Json& json) { return Bson(ObjectId(json.get<std::string>().c_str())); }},
    {"$regularExpression", +[](const Json& json) {
        util::Optional<std::string> pattern;
        util::Optional<std::string> options;
        if (json.size() != 2)
            throw BsonError("invalid extended json $binary");
        for (auto&& [k, v] : json.items()) {
            if (k == "pattern") {
                pattern = v.get<std::string>();
            } else if (k == "options") {
                options = v.get<std::string>();
            }
        }
        if (!pattern || !options)
            throw BsonError("invalid extended json $binary");
        return Bson(RegularExpression(std::move(*pattern), std::move(*options)));
    }},
    {"$timestamp", +[](const Json& json) {
        util::Optional<uint32_t> t;
        util::Optional<uint32_t> i;
        if (json.size() != 2)
            throw BsonError("invalid extended json $timestamp");
        for (auto&& [k, v] : json.items()) {
            if (k == "t") {
                t = v.get<uint32_t>();
            } else if (k == "i") {
                i = v.get<uint32_t>();
            }
        }
        if (!t || !i)
            throw BsonError("invalid extended json $timestamp");
        return Bson(MongoTimestamp(*t, *i));
    }},
};

constexpr auto parser_comp = [](const std::pair<std::string_view, FancyParser>& lhs,
                                const std::pair<std::string_view, FancyParser>& rhs) {
    return lhs.first < rhs.first;
};

// TODO do this instead in C++20
// static_assert(std::ranges::is_sorted(bson_fancy_parsers, parser_comp));
#if REALM_DEBUG
[[maybe_unused]] bool check_sort_on_startup = [] {
    REALM_ASSERT(std::is_sorted(std::begin(bson_fancy_parsers), std::end(bson_fancy_parsers), parser_comp));
    return false;
}();
#endif

Bson dom_obj_to_bson(const Json& json) {
    if (json.size() == 1) {
        const auto& [key, value] = json.items().begin();
        if (key[0] == '$') {
            auto it = std::lower_bound(std::begin(bson_fancy_parsers),
                                       std::end(bson_fancy_parsers),
                                       std::pair<std::string_view, FancyParser>(key, nullptr),
                                       parser_comp);
            if (it != std::end(bson_fancy_parsers) && it->first == key) {
                return it->second(value);
            }
        }
    }

    BsonDocument out;
    for (auto&& [k, v] : json.items()) {
        out[k] = dom_elem_to_bson(v);
    }
    return out;
}
}

} // anonymous namespace

Bson parse(const std::string_view& json) {
    return dom_elem_to_bson(Json::parse(json));
}

} // namespace bson
} // namespace realm
