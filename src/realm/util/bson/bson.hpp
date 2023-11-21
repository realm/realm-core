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
    enum class Type : uint8_t {
        Null = 0xa,
        Int32 = 0x10,
        Int64 = 0x12,
        Bool = 0x08,
        Double = 0x01,
        String = 0x02,
        Binary = 0x05,
        Timestamp = 0x11,
        Datetime = 0x09,
        ObjectId = 0x07,
        Decimal128 = 0x13,
        RegularExpression = 0x0b,
        MaxKey = 0xff,
        MinKey = 0x7f,
        Document = 0x03,
        Array = 0x04,
        Uuid = 0x14
    };

    Bson() noexcept
        : m_type(Type::Null)
    {
    }

    Bson(Bson&& v) noexcept;
    Bson(const Bson& v);

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
    Bson(std::vector<char>&&) noexcept;
    Bson(const char* begin, const char* end) noexcept;
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

    Bson& operator=(Bson&& v) noexcept;
    Bson& operator=(const Bson& v);

    uint32_t size() const;
    void append_to(uint8_t*) const;

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
        if (m_type == Bson::Type::Int64)
            return int64_val;
        return int32_t(*this);
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

    explicit operator BinaryData() const
    {
        REALM_ASSERT(m_type == Bson::Type::Binary);
        return {binary_val.data(), binary_val.size()};
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

class BsonDocument {
public:
    using entry = std::pair<std::string_view, Bson>;

    struct iterator {
        iterator(const BsonDocument* p, uint32_t n)
            : m_doc(p)
            , m_ndx(n)
        {
        }
        bool operator==(const iterator& other)
        {
            return m_ndx == other.m_ndx;
        }
        bool operator!=(const iterator& other)
        {
            return m_ndx != other.m_ndx;
        }
        const entry& operator*()
        {
            return *operator->();
        }

        const entry* operator->();
        iterator& operator++();

    private:
        const BsonDocument* m_doc;
        uint32_t m_ndx;
        entry m_value; /* Internal value for various state. */
    };

    BsonDocument()
    {
        init();
    }
    BsonDocument(BsonDocument&&);
    BsonDocument(const BsonDocument&);
    BsonDocument(std::initializer_list<entry> entries);
    BsonDocument(const uint8_t* buf);
    ~BsonDocument();

    BsonDocument& operator=(const BsonDocument&);

    uint32_t length() const
    {
        return len;
    }
    uint32_t size() const;
    bool empty() const
    {
        return size() == 0;
    }
    BinaryData serialize() const
    {
        return {reinterpret_cast<const char*>(get_data()), len};
    }

    void append(std::string_view, const Bson&);
    BsonDocument append_array(std::string_view key)
    {
        return append_bson(key, Bson::Type::Array);
    }
    BsonDocument append_document(std::string_view key)
    {
        return append_bson(key, Bson::Type::Document);
    }
    void append_to(uint8_t* p)
    {
        memcpy(p, get_data(), len);
    }

    Bson operator[](std::string_view k) const
    {
        return at(k);
    }
    Bson at(std::string_view k) const;
    std::optional<Bson> find(std::string_view key) const;

    bool operator==(const BsonDocument&) const;

    iterator begin() const
    {
        return {this, 0};
    }
    iterator end() const
    {
        return {this, size()};
    }

    std::string dump() const;

private:
    friend class BsonArray;
    static constexpr size_t BSON_INLINE_DATA_SIZE = 120;
    enum Flags : uint32_t {
        BSON_FLAG_NONE = 0,
        BSON_FLAG_INLINE = (1 << 0),
        BSON_FLAG_STATIC = (1 << 1),
        BSON_FLAG_RDONLY = (1 << 2),
        BSON_FLAG_CHILD = (1 << 3),
        BSON_FLAG_IN_CHILD = (1 << 4),
        BSON_FLAG_NO_FREE = (1 << 5),
    };

    uint32_t flags = BSON_FLAG_INLINE;
    uint32_t len = 0;
    struct ImplAlloc {
        BsonDocument* parent; // parent bson if a child
        uint32_t depth;       // Subdocument depth.
        uint8_t** buf;        // pointer to buffer pointer
        size_t* buflen;       // pointer to buffer length
        size_t offset;        // our offset inside *buf
        uint8_t* alloc;       // buffer that we own.
        size_t alloclen;      // length of buffer that we own.
    };
    union {
        uint8_t data[BSON_INLINE_DATA_SIZE];
        ImplAlloc impl_alloc;
    };
    std::vector<uint32_t> entries;

    const uint8_t* get_data() const
    {
        return const_cast<BsonDocument*>(this)->get_data();
    }

    uint8_t* get_data()
    {
        if ((flags & BSON_FLAG_INLINE)) {
            return data;
        }
        else {
            return *impl_alloc.buf + impl_alloc.offset;
        }
    }
    void grow(uint32_t size)
    {
        REALM_ASSERT((flags & BSON_FLAG_RDONLY) == 0);
        if (flags & BSON_FLAG_INLINE) {
            inline_grow(size);
        }
        else {
            alloc_grow(size);
        }
    }
    void init();
    void inline_grow(uint32_t size);
    void alloc_grow(uint32_t size);
    BsonDocument append_bson(std::string_view, Bson::Type);
    void encode_length()
    {
        *reinterpret_cast<uint32_t*>(get_data()) = len;
    }
    const uint8_t* next(const uint8_t* data, uint32_t len);
    Bson get_value(Bson::Type type, const uint8_t* data) const;
};

class BsonArray {
public:
    using entry = Bson;
    struct iterator {
        iterator(BsonDocument::iterator it)
            : m_it(it)
        {
        }
        bool operator!=(const iterator& other)
        {
            return m_it != other.m_it;
        }
        const entry& operator*()
        {
            return m_it->second;
        }
        iterator& operator++()
        {
            ++m_it;
            return *this;
        }

    private:
        BsonDocument::iterator m_it;
    };

    BsonArray() {}
    BsonArray(BsonArray&& arr)
        : m_doc(std::move(arr.m_doc))
    {
    }
    BsonArray(BsonDocument&& doc)
        : m_doc(std::move(doc))
    {
    }
    BsonArray(const BsonArray& arr)
        : m_doc(arr.m_doc)
    {
    }
    BsonArray(std::initializer_list<entry> entries)
    {
        for (auto& e : entries) {
            append(e);
        }
    }
    BsonArray(const uint8_t* buf)
        : m_doc(buf)
    {
    }

    uint32_t length() const
    {
        return m_doc.length();
    }
    size_t size() const
    {
        return m_doc.size();
    }

    bool empty() const
    {
        return size() == 0;
    }

    BinaryData serialize() const
    {
        return m_doc.serialize();
    }

    Bson operator[](size_t) const;

    void append(const Bson&);
    BsonDocument append_document();
    void append_to(uint8_t* p)
    {
        m_doc.append_to(p);
    }

    bool operator==(const BsonArray&) const;
    iterator begin() const
    {
        return iterator(m_doc.begin());
    }
    iterator end() const
    {
        return iterator(m_doc.end());
    }

private:
    friend class BsonDocument;
    BsonDocument m_doc;
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

inline Bson::Bson(std::vector<char>&& v) noexcept
{
    m_type = Bson::Type::Binary;
    new (&binary_val) std::vector<char>(std::move(v));
}

inline Bson::Bson(const char* begin, const char* end) noexcept
{
    m_type = Bson::Type::Binary;
    new (&binary_val) std::vector<char>(begin, end);
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
