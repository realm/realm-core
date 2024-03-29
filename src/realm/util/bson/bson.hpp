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

#ifndef REALM_BSON_HPP
#define REALM_BSON_HPP

#include <realm/util/bson/indexed_map.hpp>
#include <realm/util/bson/regular_expression.hpp>
#include <realm/util/bson/min_key.hpp>
#include <realm/util/bson/max_key.hpp>
#include <realm/util/bson/mongo_timestamp.hpp>
#include <realm/util/span.hpp>

#include <realm/binary_data.hpp>
#include <realm/timestamp.hpp>
#include <realm/decimal128.hpp>
#include <realm/object_id.hpp>
#include <realm/uuid.hpp>
#include <ostream>

namespace realm {
namespace bson {

class BsonDocument;
class BsonArray;

class Bson {
public:
    enum class Type {
        Null,
        Int32,
        Int64,
        Bool,
        Double,
        String,
        Binary,
        Timestamp,
        Datetime,
        ObjectId,
        Decimal128,
        RegularExpression,
        MaxKey,
        MinKey,
        Document,
        Array,
        Uuid
    };

    Bson() noexcept
        : m_type(Type::Null)
    {
    }

    Bson(util::None) noexcept
        : Bson()
    {
    }

    Bson(int32_t) noexcept;
    Bson(int64_t) noexcept;
    Bson(bool) noexcept;
    Bson(double) noexcept;
    Bson(MinKey) noexcept;
    Bson(MaxKey) noexcept;
    Bson(MongoTimestamp) noexcept;
    Bson(realm::Timestamp) noexcept;
    Bson(Decimal128) noexcept;
    Bson(ObjectId) noexcept;
    Bson(realm::UUID) noexcept;

    Bson(const RegularExpression&) noexcept;
    Bson(const std::vector<char>&) noexcept;
    Bson(const std::string&) noexcept;
    Bson(const BsonDocument&) noexcept;
    Bson(const BsonArray&) noexcept;
    Bson(std::string&&) noexcept;
    Bson(BsonDocument&&) noexcept;
    Bson(BsonArray&&) noexcept;

    // These are shortcuts for Bson(StringData(c_str)), and are
    // needed to avoid unwanted implicit conversion of char* to bool.
    Bson(char* c_str) noexcept
        : Bson(StringData(c_str))
    {
    }
    Bson(const char* c_str) noexcept
        : Bson(StringData(c_str))
    {
    }

    ~Bson() noexcept;

    Bson(Bson&& v) noexcept;
    Bson(const Bson& v);
    Bson& operator=(Bson&& v) noexcept;
    Bson& operator=(const Bson& v);

    explicit operator util::None() const
    {
        REALM_ASSERT(m_type == Type::Null);
        return util::none;
    }

    explicit operator int32_t() const
    {
        REALM_ASSERT(m_type == Bson::Type::Int32);
        return int32_val;
    }

    explicit operator int64_t() const
    {
        REALM_ASSERT(m_type == Bson::Type::Int64);
        return int64_val;
    }

    explicit operator bool() const
    {
        REALM_ASSERT(m_type == Bson::Type::Bool);
        return bool_val;
    }

    explicit operator double() const
    {
        REALM_ASSERT(m_type == Bson::Type::Double);
        return double_val;
    }

    explicit operator std::string&()
    {
        REALM_ASSERT(m_type == Bson::Type::String);
        return string_val;
    }

    explicit operator const std::string&() const
    {
        REALM_ASSERT(m_type == Bson::Type::String);
        return string_val;
    }

    explicit operator std::vector<char>&()
    {
        REALM_ASSERT(m_type == Bson::Type::Binary);
        return binary_val;
    }

    explicit operator const std::vector<char>&() const
    {
        REALM_ASSERT(m_type == Bson::Type::Binary);
        return binary_val;
    }

    explicit operator MongoTimestamp() const
    {
        REALM_ASSERT(m_type == Bson::Type::Timestamp);
        return time_val;
    }

    explicit operator realm::Timestamp() const
    {
        REALM_ASSERT(m_type == Bson::Type::Datetime);
        return date_val;
    }

    explicit operator ObjectId() const
    {
        REALM_ASSERT(m_type == Bson::Type::ObjectId);
        return oid_val;
    }

    explicit operator Decimal128() const
    {
        REALM_ASSERT(m_type == Bson::Type::Decimal128);
        return decimal_val;
    }

    explicit operator const RegularExpression&() const
    {
        REALM_ASSERT(m_type == Bson::Type::RegularExpression);
        return regex_val;
    }

    explicit operator MinKey() const
    {
        REALM_ASSERT(m_type == Bson::Type::MinKey);
        return min_key_val;
    }

    explicit operator MaxKey() const
    {
        REALM_ASSERT(m_type == Bson::Type::MaxKey);
        return max_key_val;
    }

    explicit operator BsonDocument&() noexcept
    {
        REALM_ASSERT(m_type == Bson::Type::Document);
        return *document_val;
    }

    explicit operator const BsonDocument&() const noexcept
    {
        REALM_ASSERT(m_type == Bson::Type::Document);
        return *document_val;
    }

    explicit operator BsonArray&() noexcept
    {
        REALM_ASSERT(m_type == Bson::Type::Array);
        return *array_val;
    }

    explicit operator const BsonArray&() const noexcept
    {
        REALM_ASSERT(m_type == Bson::Type::Array);
        return *array_val;
    }

    explicit operator realm::UUID() const
    {
        REALM_ASSERT(m_type == Bson::Type::Uuid);
        return uuid_val;
    }

    Type type() const noexcept;
    std::string to_string() const;

    std::string toJson() const;

    bool operator==(const Bson& other) const;
    bool operator!=(const Bson& other) const;

private:
    friend std::ostream& operator<<(std::ostream& out, const Bson& m);
    template <typename T>
    friend bool holds_alternative(const Bson& bson);

    Type m_type;

    union {
        int32_t int32_val;
        int64_t int64_val;
        bool bool_val;
        double double_val;
        MongoTimestamp time_val;
        ObjectId oid_val;
        Decimal128 decimal_val;
        MaxKey max_key_val;
        MinKey min_key_val;
        realm::Timestamp date_val;
        realm::UUID uuid_val;
        // ref types
        RegularExpression regex_val;
        std::string string_val;
        std::vector<char> binary_val;
        std::unique_ptr<BsonDocument> document_val;
        std::unique_ptr<BsonArray> array_val;
    };
};

class BsonDocument : private IndexedMap<Bson> {
public:
    using iterator = IndexedMap<Bson>::iterator;
    using IndexedMap<Bson>::begin;
    using IndexedMap<Bson>::end;

    BsonDocument() {}
    BsonDocument(BsonDocument&& other)
        : IndexedMap<Bson>(std::move(other))
    {
    }
    BsonDocument(const BsonDocument& other)
        : IndexedMap<Bson>(other)
    {
    }
    BsonDocument(std::initializer_list<entry> entries)
        : IndexedMap<Bson>(entries)
    {
    }

    BsonDocument& operator=(const BsonDocument& rhs)
    {
        this->IndexedMap<Bson>::operator=(rhs);
        return *this;
    }

    uint32_t size() const
    {
        return uint32_t(IndexedMap<Bson>::size());
    }

    bool empty() const
    {
        return size() == 0;
    }

    Bson& operator[](const std::string& k)
    {
        return IndexedMap<Bson>::operator[](k);
    }

    Bson& at(const std::string& k)
    {
        return IndexedMap<Bson>::at(k);
    }

    const Bson& at(const std::string& k) const
    {
        return IndexedMap<Bson>::at(k);
    }

    Bson* find(const std::string& key)
    {
        auto& raw = entries();
        if (auto it = raw.find(key); it != raw.end()) {
            return &it->second;
        }
        return nullptr;
    }

    const Bson* find(const std::string& key) const
    {
        auto& raw = entries();
        if (auto it = raw.find(key); it != raw.end()) {
            return &it->second;
        }
        return nullptr;
    }

    bool operator==(const BsonDocument& other) const noexcept
    {
        return static_cast<const IndexedMap<Bson>*>(this)->entries() == other.entries();
    }
};

class BsonArray : private std::vector<Bson> {
public:
    using entry = Bson;
    using vector<Bson>::vector;
    using vector<Bson>::begin;
    using vector<Bson>::end;
    using vector<Bson>::size;
    using vector<Bson>::empty;
    using vector<Bson>::push_back;
    using vector<Bson>::operator[];

    bool operator==(const BsonArray& other) const
    {
        return *static_cast<const std::vector<Bson>*>(this) == other;
    }
};

inline Bson::Bson(int32_t v) noexcept
{
    m_type = Bson::Type::Int32;
    int32_val = v;
}

inline Bson::Bson(int64_t v) noexcept
{
    m_type = Bson::Type::Int64;
    int64_val = v;
}

inline Bson::Bson(bool v) noexcept
{
    m_type = Bson::Type::Bool;
    bool_val = v;
}

inline Bson::Bson(double v) noexcept
{
    m_type = Bson::Type::Double;
    double_val = v;
}

inline Bson::Bson(MinKey v) noexcept
{
    m_type = Bson::Type::MinKey;
    min_key_val = v;
}

inline Bson::Bson(MaxKey v) noexcept
{
    m_type = Bson::Type::MaxKey;
    max_key_val = v;
}
inline Bson::Bson(const RegularExpression& v) noexcept
{
    m_type = Bson::Type::RegularExpression;
    new (&regex_val) RegularExpression(v);
}

inline Bson::Bson(const std::vector<char>& v) noexcept
{
    m_type = Bson::Type::Binary;
    new (&binary_val) std::vector<char>(v);
}

inline Bson::Bson(const std::string& v) noexcept
{
    m_type = Bson::Type::String;
    new (&string_val) std::string(v);
}

inline Bson::Bson(std::string&& v) noexcept
{
    m_type = Bson::Type::String;
    new (&string_val) std::string(std::move(v));
}

inline Bson::Bson(MongoTimestamp v) noexcept
{
    m_type = Bson::Type::Timestamp;
    time_val = v;
}

inline Bson::Bson(realm::Timestamp v) noexcept
{
    m_type = Bson::Type::Datetime;
    date_val = v;
}

inline Bson::Bson(Decimal128 v) noexcept
{
    m_type = Bson::Type::Decimal128;
    decimal_val = v;
}

inline Bson::Bson(ObjectId v) noexcept
{
    m_type = Bson::Type::ObjectId;
    oid_val = v;
}

inline Bson::Bson(const BsonDocument& v) noexcept
    : m_type(Bson::Type::Document)
    , document_val(new BsonDocument(v))
{
}

inline Bson::Bson(const BsonArray& v) noexcept
    : m_type(Bson::Type::Array)
    , array_val(new BsonArray(v))
{
}

inline Bson::Bson(BsonDocument&& v) noexcept
    : m_type(Bson::Type::Document)
    , document_val(new BsonDocument(std::move(v)))
{
}

inline Bson::Bson(BsonArray&& v) noexcept
    : m_type(Bson::Type::Array)
    , array_val(new BsonArray(std::move(v)))
{
}

inline Bson::Bson(realm::UUID v) noexcept
{
    m_type = Bson::Type::Uuid;
    uuid_val = v;
}

template <typename T>
bool holds_alternative(const Bson& bson);

std::ostream& operator<<(std::ostream& out, const Bson& b);

Bson parse(util::Span<const char> json);
bool accept(util::Span<const char> json) noexcept;

} // namespace bson
} // namespace realm

#endif // REALM_BSON_HPP
