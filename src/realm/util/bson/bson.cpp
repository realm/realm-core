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

#include <realm/mixed.hpp>
#include <realm/util/bson/bson.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/json_parser.hpp>
#include <realm/mixed.hpp>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <charconv>
#include <stack>
#include <array>

namespace realm {
namespace bson {

/*********************************** Bson ************************************/

Bson::Bson(const Bson& v)
{
    m_type = Type::Null;
    *this = v;
}

Bson::Bson(Bson&& v) noexcept
{
    m_type = Type::Null;
    *this = std::move(v);
}

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

Bson& Bson::operator=(Bson&& v) noexcept
{
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
        case Type::Uuid:
            uuid_val = v.uuid_val;
            break;
    }

    return *this;
}

Bson& Bson::operator=(const Bson& v)
{
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
        case Type::Uuid:
            uuid_val = v.uuid_val;
            break;
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
        case Type::Uuid:
            return uuid_val == other.uuid_val;
    }

    return false;
}

bool Bson::operator!=(const Bson& other) const
{
    return !(*this == other);
}

uint32_t Bson::size() const
{
    switch (type()) {
        case Bson::Type::Null:
            return 0;
        case Bson::Type::Int32:
            return sizeof(int32_t);
        case Bson::Type::Int64:
            return sizeof(int64_t);
        case Bson::Type::Bool:
            return 1;
        case Bson::Type::Double:
            return sizeof(double);
        case Bson::Type::String:
            return uint32_t(string_val.size() + 4 + 1);
        case Bson::Type::Binary:
            return uint32_t(binary_val.size() + 4 + 1);
        case Bson::Type::Datetime:
        case Bson::Type::Timestamp:
            return sizeof(uint64_t);
        case Bson::Type::ObjectId:
            return sizeof(ObjectId);
        case Bson::Type::Decimal128:
            return sizeof(Decimal128);
        case Bson::Type::RegularExpression: {
            auto opt_str = this->regex_val.options_str();
            return uint32_t(regex_val.pattern().size() + 1 + opt_str.size() + 1);
        }
        case Bson::Type::MinKey:
        case Bson::Type::MaxKey:
            return 0;
        case Bson::Type::Document:
            return document_val->length();
        case Bson::Type::Array:
            return array_val->length();
        case Bson::Type::Uuid:
            return sizeof(uint32_t) + 1 + sizeof(UUID);
    }
    return 0;
}

namespace {

template <class T>
inline void _append(uint8_t* p, const T& val)
{
    memcpy(p, &val, sizeof(T));
}

template <class T>
T _extract(const uint8_t* p)
{
    T val;
    memcpy(&val, p, sizeof(T));
    return val;
}

} // namespace

void Bson::append_to(uint8_t* p) const
{
    switch (type()) {
        case Bson::Type::Null:
            break;
        case Bson::Type::Int32:
            _append(p, int32_val);
            break;
        case Bson::Type::Int64:
            _append(p, int64_val);
            break;
        case Bson::Type::Bool:
            *p = bool_val ? 1 : 0;
            break;
        case Bson::Type::Double:
            _append(p, double_val);
            break;
        case Bson::Type::String: {
            auto sz = string_val.size();
            _append(p, uint32_t(sz + 1));
            p += sizeof(uint32_t);
            memcpy(reinterpret_cast<char*>(p), string_val.data(), sz);
            p += sz;
            *p = 0;
            break;
        }
        case Bson::Type::Binary:
            _append(p, uint32_t(uint32_t(binary_val.size())));
            p += sizeof(uint32_t);
            *p++ = 0; // Subtype
            if (!binary_val.empty()) {
                memcpy(p, binary_val.data(), binary_val.size());
            }
            break;
        case Bson::Type::Datetime: {
            int64_t millisecs = date_val.get_seconds() * 1000 + date_val.get_nanoseconds() / 1000000;
            _append(p, millisecs);
            break;
        }
        case Bson::Type::Timestamp:
            _append(p, time_val);
            break;
        case Bson::Type::ObjectId:
            _append(p, oid_val);
            break;
        case Bson::Type::Decimal128:
            _append(p, decimal_val);
            break;
        case Bson::Type::RegularExpression: {
            auto pattern_sz = regex_val.pattern().size();
            auto opt_str = regex_val.options_str();
            strcpy(reinterpret_cast<char*>(p), regex_val.pattern().c_str());
            strcpy(reinterpret_cast<char*>(p + pattern_sz + 1), opt_str.c_str());
            break;
        }
        case Bson::Type::MinKey:
            break;
        case Bson::Type::MaxKey:
            break;
        case Bson::Type::Document:
            document_val->append_to(p);
            break;
        case Bson::Type::Array:
            array_val->append_to(p);
            break;
        case Bson::Type::Uuid:
            _append(p, uint32_t(UUID::num_bytes));
            p += sizeof(uint32_t);
            *p++ = 4; // Subtype
            _append(p, uuid_val);
            break;
    }
}

std::string Bson::toJson() const
{
    std::stringstream s;
    s << *this;
    return s.str();
}

/******************************* BsonDocument ********************************/

BsonDocument::BsonDocument(std::initializer_list<entry> entries)
{
    init();
    for (auto& e : entries) {
        append(e.first, e.second);
    }
}

BsonDocument::BsonDocument(const uint8_t* from)
{
    auto from_len = _extract<uint32_t>(from);
    REALM_ASSERT(from[from_len - 1] == '\0');
    len = from_len;
    flags = BSON_FLAG_NO_FREE | BSON_FLAG_RDONLY;
    impl_alloc.parent = NULL;
    impl_alloc.depth = 0;
    impl_alloc.buf = &impl_alloc.alloc;
    impl_alloc.buflen = &impl_alloc.alloclen;
    impl_alloc.offset = 0;
    impl_alloc.alloc = const_cast<uint8_t*>(from);
    impl_alloc.alloclen = from_len;

    auto buf = get_data();
    const uint8_t* p = buf + 4;
    while (*p) {
        unsigned offs = unsigned(p - buf);
        entries.push_back(offs);
        p = next(p, len - offs);
        if (!p)
            throw std::runtime_error("Error");
    }
}

BsonDocument::BsonDocument(const BsonDocument& other)
{
    len = 0;
    flags = BSON_FLAG_INLINE;
    *this = other;
}

BsonDocument::BsonDocument(BsonDocument&& from)
{
    len = from.len;
    flags = from.flags;
    if ((from.flags & BSON_FLAG_INLINE)) {
        memcpy(data, from.data, len);
    }
    else {
        impl_alloc.parent = from.impl_alloc.parent;
        impl_alloc.depth = from.impl_alloc.depth;
        impl_alloc.offset = from.impl_alloc.offset;
        if (flags & BSON_FLAG_CHILD) {
            impl_alloc.buf = from.impl_alloc.buf;
            impl_alloc.buflen = from.impl_alloc.buflen;
            impl_alloc.alloc = nullptr;
            impl_alloc.alloclen = 0;
            from.flags &= ~BSON_FLAG_CHILD;
        }
        else {
            impl_alloc.buf = &impl_alloc.alloc;
            impl_alloc.buflen = &impl_alloc.alloclen;
            impl_alloc.alloc = from.impl_alloc.alloc;
            impl_alloc.alloclen = from.impl_alloc.alloclen;
            from.flags |= BSON_FLAG_NO_FREE;
        }
    }
    entries = std::move(from.entries);
}

BsonDocument::~BsonDocument()
{
    if (flags & BSON_FLAG_CHILD) {
        auto parent = impl_alloc.parent;
        REALM_ASSERT(parent != nullptr);
        REALM_ASSERT((parent->flags & BSON_FLAG_IN_CHILD));
        REALM_ASSERT(!(flags & BSON_FLAG_IN_CHILD));
        parent->flags &= ~BSON_FLAG_IN_CHILD;
        parent->len += len - 5;
        parent->get_data()[parent->len - 1] = '\0';
        parent->encode_length();
    }
    else if (!(flags & (BSON_FLAG_RDONLY | BSON_FLAG_INLINE | BSON_FLAG_NO_FREE))) {
        free(*impl_alloc.buf);
    }
}

BsonDocument& BsonDocument::operator=(const BsonDocument& other)
{
    entries = other.entries;
    grow(other.len);
    len = other.len;
    memcpy(get_data(), other.get_data(), len);
    return *this;
}

uint32_t BsonDocument::size() const
{
    return uint32_t(entries.size());
}

void BsonDocument::append(std::string_view key, const Bson& b)
{
    REALM_ASSERT(!(flags & BSON_FLAG_IN_CHILD));
    REALM_ASSERT(!(flags & BSON_FLAG_RDONLY));

    auto value_size = b.size();
    uint32_t n_bytes = uint32_t(1 + key.size() + 1 + value_size);

    grow(n_bytes);

    auto buf = get_data();
    auto p = buf + len - 1;

    entries.push_back(unsigned(p - get_data())); // store offset of type field

    // Add type
    auto type = b.type();
    *p++ = static_cast<uint8_t>((type == Bson::Type::Uuid) ? Bson::Type::Binary : type);

    // Add key
    for (auto c : key)
        *p++ = c;
    *p++ = '\0';

    // Add value
    b.append_to(p);
    p += value_size;

    // Add terminating zero
    *p++ = '\0';

    len += n_bytes;
    encode_length();
}

BsonDocument BsonDocument::append_bson(std::string_view key, Bson::Type type)
{
    REALM_ASSERT(!(flags & BSON_FLAG_IN_CHILD));
    REALM_ASSERT(!(flags & BSON_FLAG_RDONLY));

    // If this is an inline BsonDocument, then we need to convert
    // it to a heap allocated buffer. This makes extending buffers
    // of child bson documents much simpler logic, as they can just
    // realloc the *buf pointer.
    if (flags & BSON_FLAG_INLINE) {
        REALM_ASSERT(len <= 120);
        inline_grow(128 - len);
        REALM_ASSERT(!(flags & BSON_FLAG_INLINE));
    }

    uint32_t n_bytes = uint32_t(1 + key.size() + 1 + 5);
    grow(n_bytes);

    uint8_t* buf = get_data();
    uint8_t* p = buf + len - 1;

    entries.push_back(unsigned(p - get_data())); // store offset of type field

    // Add type
    *p++ = static_cast<uint8_t>((type == Bson::Type::Uuid) ? Bson::Type::Binary : type);

    // Add key
    for (auto c : key)
        *p++ = c;
    *p++ = '\0';

    // Initialize  value
    _append(p, uint32_t(5));
    p += sizeof(uint32_t);
    *p++ = '\0';

    len += n_bytes;

    // Mark the document as working on a child document so that no
    // further modifications can happen until the child document
    // is deleted.
    flags |= BSON_FLAG_IN_CHILD;

    // Initialize the child bson_t structure and point it at the parents
    // buffers. This allows us to realloc directly from the child without
    // walking up to the parent bson_t.
    BsonDocument doc;
    doc.flags = (BSON_FLAG_CHILD | BSON_FLAG_NO_FREE | BSON_FLAG_STATIC);

    if (flags & BSON_FLAG_CHILD) {
        doc.impl_alloc.depth = impl_alloc.depth + 1;
    }
    else {
        doc.impl_alloc.depth = 1;
    }

    doc.impl_alloc.parent = this;
    doc.impl_alloc.buf = impl_alloc.buf;
    doc.impl_alloc.buflen = impl_alloc.buflen;
    doc.impl_alloc.offset = impl_alloc.offset + len - 1 - 5;
    doc.len = 5;
    doc.impl_alloc.alloc = nullptr;
    doc.impl_alloc.alloclen = 0;

    return doc;
}

void BsonDocument::init()
{
    flags = BSON_FLAG_INLINE | BSON_FLAG_STATIC;
    len = 5;
    data[0] = 5;
    data[1] = 0;
    data[2] = 0;
    data[3] = 0;
    data[4] = 0;
}

namespace {
size_t next_power_of_two(size_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    if constexpr (sizeof(v) == 8) {
        v |= v >> 32;
    }
    v++;

    return v;
}
} // namespace
void BsonDocument::inline_grow(uint32_t sz)
{
    size_t req = size_t(len) + sz;
    if (req > BSON_INLINE_DATA_SIZE) {
        req = next_power_of_two(req);

        if (req > std::numeric_limits<uint32_t>::max()) {
            throw RuntimeError(ErrorCodes::LimitExceeded, "Bson document too large");
        }

        uint8_t* new_data = reinterpret_cast<uint8_t*>(malloc(req));
        memcpy(new_data, data, len);

        flags &= ~BSON_FLAG_INLINE;
        impl_alloc.parent = NULL;
        impl_alloc.depth = 0;
        impl_alloc.buf = &impl_alloc.alloc;
        impl_alloc.buflen = &impl_alloc.alloclen;
        impl_alloc.offset = 0;
        impl_alloc.alloc = new_data;
        impl_alloc.alloclen = req;
    }
}

void BsonDocument::alloc_grow(uint32_t sz)
{
    // Determine how many bytes we need for this document in the buffer
    // including necessary trailing bytes for parent documents.
    size_t req = impl_alloc.offset + len + sz + impl_alloc.depth;

    if (req > *impl_alloc.buflen) {
        req = next_power_of_two(req);

        if (req > std::numeric_limits<uint32_t>::max()) {
            throw RuntimeError(ErrorCodes::LimitExceeded, "Bson document too large");
        }

        *impl_alloc.buf = reinterpret_cast<uint8_t*>(realloc(*impl_alloc.buf, req));
        *impl_alloc.buflen = req;
    }
}

const uint8_t* BsonDocument::next(const uint8_t* data, uint32_t len)
{
    REALM_ASSERT(data);
    uint32_t o = 1;
    while (o < len) {
        if (!data[o++])
            break;
    }
    if (o == len)
        return nullptr;

    Bson::Type type = Bson::Type(*data);
    uint32_t next_off = o;
    switch (type) {
        case Bson::Type::Datetime:
        case Bson::Type::Double:
        case Bson::Type::Int64:
        case Bson::Type::Timestamp:
            next_off = o + 8;
            break;
        case Bson::Type::String: {
            if ((o + 4) >= len) {
                return nullptr;
            }

            uint32_t l = _extract<uint32_t>(data + o);
            o += 4;

            if (l > (len - o)) {
                return nullptr;
            }
            next_off = o + l;

            // Make sure the string length includes the NUL byte.
            if (REALM_UNLIKELY((l == 0) || (next_off >= len))) {
                return nullptr;
            }

            // Make sure the last byte is a NUL byte.
            if (REALM_UNLIKELY(data[next_off - 1] != '\0')) {
                return nullptr;
            }
            break;
        }
        case Bson::Type::Uuid:
        case Bson::Type::Binary: {
            if (o >= (len - 4)) {
                return nullptr;
            }

            uint32_t l = _extract<uint32_t>(data + o);

            if (l >= (len - o - 4)) {
                return nullptr;
            }

            next_off = o + 5 + l;
            break;
        }
        case Bson::Type::Array:
        case Bson::Type::Document: {
            if (o >= (len - 4)) {
                return nullptr;
            }

            uint32_t l = _extract<uint32_t>(data + o);

            if ((l > len) || (l > (len - o))) {
                return nullptr;
            }

            next_off = o + l;
            break;
        }
        case Bson::Type::ObjectId:
            next_off = o + 12;
            break;
        case Bson::Type::Bool: {
            auto val = data[o];
            if (val != 0x00 && val != 0x01) {
                return nullptr;
            }

            next_off = o + 1;
            break;
        }
        case Bson::Type::RegularExpression: {
            bool eor = false;
            bool eoo = false;

            for (; o < len; o++) {
                if (!data[o]) {
                    eor = true;
                    break;
                }
            }

            if (!eor) {
                return nullptr;
            }

            for (; o < len; o++) {
                if (!data[o]) {
                    eoo = true;
                    break;
                }
            }

            if (!eoo) {
                return nullptr;
            }

            next_off = o + 1;
            break;
        }
        case Bson::Type::Int32:
            next_off = o + 4;
            break;
        case Bson::Type::Decimal128:
            next_off = o + 16;
            break;
        case Bson::Type::MaxKey:
        case Bson::Type::MinKey:
        case Bson::Type::Null:
            // next_off = o;
            break;
    }

    /*
     * Check to see if any of the field locations would overflow the
     * current BSON buffer. If so, set the error location to the offset
     * of where the field starts.
     */
    if (next_off >= len) {
        return nullptr;
    }

    return data + next_off;
}
Bson BsonDocument::at(std::string_view key) const
{
    auto val = find(key);
    if (!val) {
        throw KeyNotFound("BsonDocument::at");
    }
    return *val;
}

std::optional<Bson> BsonDocument::find(std::string_view k) const
{
    uint32_t sz = size();
    auto buf = get_data();
    for (uint32_t n = 0; n < sz; n++) {
        const uint8_t* p = buf + entries[n];
        Bson::Type type = Bson::Type(*p++);
        const char* key = reinterpret_cast<const char*>(p);
        if (k == std::string_view(key)) {
            p += (k.size() + 1);
            return get_value(type, p);
        }
    }
    return {};
}

bool BsonDocument::operator==(const BsonDocument& other) const
{
    if (size() != other.size())
        return false;
    for (auto it = begin(); it != end(); ++it) {
        auto other_val = other.find(it->first);
        if (!other_val)
            return false;
        if (it->second != *other_val)
            return false;
    }
    return true;
}

std::string BsonDocument::dump() const
{
    std::ostringstream ss;
    ss << *this;
    return ss.str();
}

/************************** BsonDocument::iterator ***************************/

const BsonDocument::entry* BsonDocument::iterator::operator->()
{
    const uint8_t* p = m_doc->get_data() + m_doc->entries[m_ndx];
    Bson::Type type = Bson::Type(*p++);
    const char* key_str = reinterpret_cast<const char*>(p);
    auto key_length = strlen(key_str);
    m_value.first = std::string_view{key_str, key_length};
    p += (key_length + 1);
    m_value.second = m_doc->get_value(type, p);
    return &m_value;
}

Bson BsonDocument::get_value(Bson::Type type, const uint8_t* p) const
{
    switch (type) {
        case Bson::Type::Null:
            return {};
        case Bson::Type::Int32:
            return _extract<int32_t>(p);
            break;
        case Bson::Type::Int64:
            return _extract<int64_t>(p);
            break;
        case Bson::Type::Bool:
            return (*p == 1);
            break;
        case Bson::Type::Double:
            return _extract<double>(p);
            break;
        case Bson::Type::String: {
            auto len = _extract<uint32_t>(p) - 1;
            return std::string(reinterpret_cast<const char*>(p + sizeof(uint32_t)), len);
            break;
        }
        case Bson::Type::Binary: {
            auto len = _extract<uint32_t>(p);
            auto subtype = p[4];
            p += 5;
            if (subtype == 4) {
                REALM_ASSERT(len == UUID::num_bytes);
                return _extract<UUID>(p);
            }
            else {
                return Bson(reinterpret_cast<const char*>(p), reinterpret_cast<const char*>(p + len));
            }
            break;
        }
        case Bson::Type::Datetime: {
            int64_t millisecs = _extract<int64_t>(p);
            int64_t seconds = millisecs / 1000;
            int32_t nanoseconds = int32_t(millisecs % 1000 * 1000000);
            return Timestamp(seconds, nanoseconds);
            break;
        }
        case Bson::Type::Timestamp:
            return _extract<MongoTimestamp>(p);
            break;
        case Bson::Type::ObjectId:
            return _extract<ObjectId>(p);
            break;
        case Bson::Type::Decimal128:
            return _extract<Decimal128>(p);
            break;
        case Bson::Type::RegularExpression: {
            const char* pattern = reinterpret_cast<const char*>(p);
            const char* options = reinterpret_cast<const char*>(p + strlen(pattern) + 1);
            return RegularExpression(pattern, options);
            break;
        }
        case Bson::Type::MinKey:
            return Bson(MinKey());
            break;
        case Bson::Type::MaxKey:
            return Bson(MaxKey());
            break;
        case Bson::Type::Document:
            return BsonDocument(p);
            break;
        case Bson::Type::Array:
            return BsonArray(p);
            break;
        case Bson::Type::Uuid:
            break;
    }
    return {};
}


BsonDocument::iterator& BsonDocument::iterator::operator++()
{
    m_ndx++;
    return *this;
}

/********************************* BsonArray *********************************/

void BsonArray::append(const Bson& b)
{
    auto n = m_doc.size();
    char buffer[10];
    auto len = sprintf(buffer, "%u", n);
    m_doc.append(std::string_view(buffer, len), b);
}

BsonDocument BsonArray::append_document()
{
    auto n = m_doc.size();
    char buffer[10];
    auto len = sprintf(buffer, "%u", n);
    return m_doc.append_document(std::string_view(buffer, len));
}

Bson BsonArray::operator[](size_t ndx) const
{
    if (ndx >= size()) {
        throw OutOfBounds("BsonArray::operator[]", ndx, size());
    }
    BsonDocument::iterator it(&m_doc, uint32_t(ndx));
    return it->second;
}

bool BsonArray::operator==(const BsonArray& other) const
{
    if (size() != other.size())
        return false;
    auto other_it = other.begin();
    for (auto it = begin(); it != end(); ++it, ++other_it) {
        if (*it != *other_it)
            return false;
    }
    return true;
}

template <>
bool holds_alternative<util::None>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Null;
}

template <>
bool holds_alternative<int32_t>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Int32;
}

template <>
bool holds_alternative<int64_t>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Int64;
}

template <>
bool holds_alternative<bool>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Bool;
}

template <>
bool holds_alternative<double>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Double;
}

template <>
bool holds_alternative<std::string>(const Bson& bson)
{
    return bson.m_type == Bson::Type::String;
}

template <>
bool holds_alternative<std::vector<char>>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Binary;
}

template <>
bool holds_alternative<Timestamp>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Datetime;
}

template <>
bool holds_alternative<ObjectId>(const Bson& bson)
{
    return bson.m_type == Bson::Type::ObjectId;
}

template <>
bool holds_alternative<Decimal128>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Decimal128;
}

template <>
bool holds_alternative<RegularExpression>(const Bson& bson)
{
    return bson.m_type == Bson::Type::RegularExpression;
}

template <>
bool holds_alternative<MinKey>(const Bson& bson)
{
    return bson.m_type == Bson::Type::MinKey;
}

template <>
bool holds_alternative<MaxKey>(const Bson& bson)
{
    return bson.m_type == Bson::Type::MaxKey;
}

template <>
bool holds_alternative<BsonDocument>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Document;
}

template <>
bool holds_alternative<BsonArray>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Array;
}

template <>
bool holds_alternative<MongoTimestamp>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Timestamp;
}

template <>
bool holds_alternative<UUID>(const Bson& bson)
{
    return bson.m_type == Bson::Type::Uuid;
}

namespace {
struct PrecisionGuard {
    PrecisionGuard(std::ostream& stream, std::streamsize new_precision)
        : stream(stream)
        , old_precision(stream.precision(new_precision))
    {
    }

    ~PrecisionGuard()
    {
        stream.precision(old_precision);
    }

    std::ostream& stream;
    std::streamsize old_precision;
};
} // namespace

std::ostream& operator<<(std::ostream& out, const Bson& b)
{
    switch (b.type()) {
        case Bson::Type::Null:
            out << "null";
            break;
        case Bson::Type::Int32:
            out << "{"
                << "\"$numberInt\""
                << ":" << '"' << static_cast<int32_t>(b) << '"' << "}";
            break;
        case Bson::Type::Int64:
            out << "{"
                << "\"$numberLong\""
                << ":" << '"' << static_cast<int64_t>(b) << '"' << "}";
            break;
        case Bson::Type::Bool:
            out << (b ? "true" : "false");
            break;
        case Bson::Type::Double: {
            double d = static_cast<double>(b);
            out << "{"
                << "\"$numberDouble\""
                << ":" << '"';
            if (std::isnan(d)) {
                out << "NaN";
            }
            else if (d == std::numeric_limits<double>::infinity()) {
                out << "Infinity";
            }
            else if (d == (-1 * std::numeric_limits<double>::infinity())) {
                out << "-Infinity";
            }
            else {
                PrecisionGuard precision_guard(out, std::numeric_limits<double>::max_digits10);
                out << d;
            }
            out << '"' << "}";
            break;
        }
        case Bson::Type::String:
            Mixed(b).to_json(out, JSONOutputMode::output_mode_xjson);
            break;
        case Bson::Type::Binary: {
            BinaryData bin_data = static_cast<BinaryData>(b);
            size_t sz = bin_data.size();
            std::string encode_buffer;
            encode_buffer.resize(util::base64_encoded_size(sz));
            util::base64_encode(bin_data, encode_buffer);
            out << "{\"$binary\":{\"base64\":\"" << encode_buffer << "\",\"subType\":\"00\"}}";
            break;
        }
        case Bson::Type::Timestamp: {
            const MongoTimestamp& t = static_cast<MongoTimestamp>(b);
            out << "{\"$timestamp\":{\"t\":" << t.seconds << ",\"i\":" << t.increment << "}}";
            break;
        }
        case Bson::Type::Datetime: {
            auto d = static_cast<realm::Timestamp>(b);

            out << "{\"$date\":{\"$numberLong\":\"" << ((d.get_seconds() * 1000) + d.get_nanoseconds() / 1000000)
                << "\"}}";
            break;
        }
        case Bson::Type::ObjectId: {
            const ObjectId& oid = static_cast<ObjectId>(b);
            out << "{"
                << "\"$oid\""
                << ":" << '"' << oid << '"' << "}";
            break;
        }
        case Bson::Type::Decimal128: {
            const Decimal128& d = static_cast<Decimal128>(b);
            out << "{"
                << "\"$numberDecimal\""
                << ":" << '"';
            if (d.is_nan()) {
                out << "NaN";
            }
            else if (d == Decimal128("Infinity")) {
                out << "Infinity";
            }
            else if (d == Decimal128("-Infinity")) {
                out << "-Infinity";
            }
            else {
                out << d;
            }
            out << '"' << "}";
            break;
        }
        case Bson::Type::RegularExpression: {
            const RegularExpression& regex = static_cast<RegularExpression>(b);
            out << "{\"$regularExpression\":{\"pattern\":\"" << regex.pattern() << "\",\"options\":\""
                << regex.options_str() << "\"}}";
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
            for (auto& [key, value] : doc) {
                if (!first)
                    out << ',';
                first = false;
                Mixed(key).to_json(out, JSONOutputMode::output_mode_xjson);
                out << ':' << value;
            }
            out << "}";
            break;
        }
        case Bson::Type::Array: {
            const BsonArray& arr = static_cast<BsonArray>(b);
            out << "[";
            bool first = true;
            for (auto& b : arr) {
                if (!first)
                    out << ',';
                first = false;
                out << b;
            }
            out << "]";
            break;
        }
        case Bson::Type::Uuid: {
            const UUID& u = static_cast<UUID>(b);
            out << "{\"$binary\":{\"base64\":\"";
            out << u.to_base64();
            out << "\",\"subType\":\"04\"}}";
            break;
        }
    }
    return out;
}

namespace {

struct BsonError : public std::runtime_error {
    BsonError(std::string message)
        : std::runtime_error(std::move(message))
    {
    }
};

class Parser {
public:
    Parser()
        : idle(this)
        , array_insert(this)
        , accept_key(this)
        , accept_value(this)
        , state(&idle)
    {
    }
    Bson parse(util::Span<const char> json);

private:
    using FancyParser = Bson (*)(const Bson& bson);
    static std::array<std::pair<std::string_view, FancyParser>, 12> fancy_parsers;

    struct State {
        State(Parser* p)
            : parser(p)
        {
        }
        virtual ~State() {}
        virtual State* value(Bson)
        {
            return nullptr;
        }
        virtual State* array_begin();
        virtual State* object_begin();
        virtual State* end()
        {
            return nullptr;
        }
        Parser* parser;
    };

    struct Idle : public State {
        using State::State;
        State* value(Bson val) override;
    };

    struct ArrayInsert : public State {
        using State::State;
        State* value(Bson val) override;
        State* end() override;
    };
    struct AcceptKey : public State {
        using State::State;
        State* value(Bson val) override;
        State* end() override;
    };
    struct AcceptValue : public State {
        using State::State;
        State* value(Bson val) override;
    };

    State* reduce(Bson&& b);

    static constexpr auto parser_comp = [](const std::pair<std::string_view, FancyParser>& lhs,
                                           const std::pair<std::string_view, FancyParser>& rhs) {
        return lhs.first < rhs.first;
    };
    // TODO do this instead in C++20
    // static_assert(std::ranges::is_sorted(bson_fancy_parsers, parser_comp));
#if REALM_DEBUG
    bool check_sort_on_startup = [] {
        REALM_ASSERT(std::is_sorted(fancy_parsers.begin(), fancy_parsers.end(), parser_comp));
        return false;
    }();
#endif

    std::vector<std::string> keys;
    std::stack<bson::Bson> work;

    Idle idle;
    ArrayInsert array_insert;
    AcceptKey accept_key;
    AcceptValue accept_value;
    State* state;
};

std::array<std::pair<std::string_view, Parser::FancyParser>, 12> Parser::fancy_parsers{{
    {"$binary",
     +[](const Bson& bson) {
         auto& document = static_cast<const bson::BsonDocument&>(bson);
         util::Optional<std::vector<char>> base64;
         util::Optional<uint8_t> subType;
         if (document.size() != 2)
             throw BsonError("invalid extended json $binary");
         for (const auto& [k, v] : document) {
             if (k == "base64") {
                 const std::string& str = static_cast<const std::string&>(v);
                 base64.emplace(str.begin(), str.end());
             }
             else if (k == "subType") {
                 subType = uint8_t(std::stoul(static_cast<const std::string&>(v), nullptr, 16));
             }
         }
         if (!base64 || !subType)
             throw BsonError("invalid extended json $binary");
         auto stringData = StringData(reinterpret_cast<const char*>(base64->data()), base64->size());
         util::Optional<std::vector<char>> decoded_chars = util::base64_decode_to_vector(stringData);
         if (!decoded_chars)
             throw BsonError("Invalid base64 in $binary");

         if (subType == 0x04) { // UUID
             UUID::UUIDBytes bytes{};
             std::copy_n(decoded_chars->data(), bytes.size(), bytes.begin());
             return Bson(UUID(bytes));
         }
         else {
             return Bson(std::move(*decoded_chars)); // TODO don't throw away the subType.
         }
     }},
    {"$date",
     +[](const Bson& bson) {
         int64_t millis_since_epoch = static_cast<int64_t>(bson);
         return Bson(realm::Timestamp(millis_since_epoch / 1000,
                                      (millis_since_epoch % 1000) * 1'000'000)); // ms -> ns
     }},
    {"$maxKey",
     +[](const Bson&) {
         return Bson(MaxKey());
     }},
    {"$minKey",
     +[](const Bson&) {
         return Bson(MinKey());
     }},
    {"$numberDecimal",
     +[](const Bson& bson) {
         return Bson(Decimal128(static_cast<const std::string&>(bson)));
     }},
    {"$numberDouble",
     +[](const Bson& bson) {
         return Bson(std::stod(static_cast<const std::string&>(bson)));
     }},
    {"$numberInt",
     +[](const Bson& bson) {
         return Bson(int32_t(std::stoi(static_cast<const std::string&>(bson))));
     }},
    {"$numberLong",
     +[](const Bson& bson) {
         return Bson(int64_t(std::stoll(static_cast<const std::string&>(bson))));
     }},
    {"$oid",
     +[](const Bson& bson) {
         return Bson(ObjectId(static_cast<const std::string&>(bson).c_str()));
     }},
    {"$regularExpression",
     +[](const Bson& bson) {
         auto& document = static_cast<const bson::BsonDocument&>(bson);
         util::Optional<std::string> pattern;
         util::Optional<std::string> options;
         if (document.size() != 2)
             throw BsonError("invalid extended json $binary");
         for (const auto& [k, v] : document) {
             if (k == "pattern") {
                 pattern = static_cast<const std::string&>(v);
             }
             else if (k == "options") {
                 options = static_cast<const std::string&>(v);
             }
         }
         if (!pattern || !options)
             throw BsonError("invalid extended json $binary");
         return Bson(RegularExpression(std::move(*pattern), std::move(*options)));
     }},
    {"$timestamp",
     +[](const Bson& bson) {
         auto& document = static_cast<const bson::BsonDocument&>(bson);
         util::Optional<uint32_t> t;
         util::Optional<uint32_t> i;
         if (document.size() != 2)
             throw BsonError("invalid extended json $timestamp");
         for (const auto& [k, v] : document) {
             if (k == "t") {
                 t = uint32_t(static_cast<int64_t>(v));
             }
             else if (k == "i") {
                 i = uint32_t(static_cast<int64_t>(v));
             }
         }
         if (!t || !i)
             throw BsonError("invalid extended json $timestamp");
         return Bson(MongoTimestamp(*t, *i));
     }},
    {"$uuid",
     +[](const Bson& bson) {
         std::string uuid = static_cast<const std::string&>(bson);
         return Bson(UUID(uuid));
     }},
}};

Parser::State* Parser::State::array_begin()
{
    parser->work.emplace(bson::BsonArray());
    return &parser->array_insert;
}

Parser::State* Parser::State::object_begin()
{
    parser->work.push(bson::BsonDocument());
    return &parser->accept_key;
}

Parser::State* Parser::Idle::value(Bson val)
{
    parser->work.emplace(std::move(val));
    return this;
}

Parser::State* Parser::ArrayInsert::value(Bson val)
{
    static_cast<bson::BsonArray&>(parser->work.top()).append(std::move(val));
    return this;
}

Parser::State* Parser::ArrayInsert::end()
{
    bson::Bson b = std::move(parser->work.top());
    parser->work.pop();
    return parser->reduce(std::move(b));
}

Parser::State* Parser::AcceptKey::value(Bson val)
{
    parser->keys.push_back(static_cast<std::string&>(val));
    return &parser->accept_value;
}

Parser::State* Parser::AcceptKey::end()
{
    // Document done
    bson::Bson b = std::move(parser->work.top());
    parser->work.pop();
    REALM_ASSERT(b.type() == bson::Bson::Type::Document);
    auto& document = static_cast<bson::BsonDocument&>(b);
    if (document.size() == 1) {
        auto first_pair = document.begin();
        auto key = first_pair->first;
        if (key[0] == '$') {
            auto it = std::lower_bound(fancy_parsers.begin(), fancy_parsers.end(),
                                       std::pair<std::string_view, FancyParser>(key, nullptr), parser_comp);
            if (it != parser->fancy_parsers.end() && it->first == key) {
                b = it->second(first_pair->second);
            }
        }
    }
    return parser->reduce(std::move(b));
}

Parser::State* Parser::AcceptValue::value(Bson val)
{
    bson::Bson& top = parser->work.top();
    static_cast<bson::BsonDocument&>(top).append(parser->keys.back(), std::move(val));
    parser->keys.pop_back();
    return &parser->accept_key;
}

Bson Parser::parse(util::Span<const char> json)
{
    auto ec = util::JSONParser([this](auto&& event) -> std::error_condition {
                  using EventType = util::JSONParser::EventType;
                  State* next_state = nullptr;

                  switch (event.type) {
                      case EventType::number_integer: {
                          auto i = event.integer;
                          auto i32 = int32_t(i);
                          if (i == i32) {
                              next_state = state->value(i32);
                          }
                          else {
                              next_state = state->value(i);
                          }
                          break;
                      }
                      case EventType::number_float:
                          next_state = state->value(event.number);
                          break;
                      case EventType::string: {
                          auto buffer = event.unescape_string();
                          next_state = state->value(std::string(buffer.data(), buffer.size()));
                          break;
                      }
                      case EventType::boolean:
                          next_state = state->value(event.boolean);
                          break;
                      case EventType::array_begin:
                          next_state = state->array_begin();
                          break;
                      case EventType::array_end:
                          next_state = state->end();
                          break;
                      case EventType::object_begin:
                          next_state = state->object_begin();
                          break;
                      case EventType::object_end:
                          next_state = state->end();
                          break;
                      case EventType::null:
                          next_state = state->value(Bson());
                          break;
                  }
                  if (!next_state) {
                      return util::JSONParser::Error::unexpected_token;
                  }
                  state = next_state;
                  return std::error_condition{};
              }).parse(json);

    if (ec) {
        throw std::logic_error(ec.message());
    }
    REALM_ASSERT(work.size() == 1);
    return std::move(work.top());
}

Parser::State* Parser::reduce(Bson&& b)
{
    if (work.empty()) {
        // we must be done
        work.emplace(std::move(b));
        return &idle;
    }
    bson::Bson& top = work.top();
    if (top.type() == bson::Bson::Type::Array) {
        static_cast<bson::BsonArray&>(top).append(std::move(b));
        return &array_insert;
    }
    if (top.type() == bson::Bson::Type::Document) {
        static_cast<bson::BsonDocument&>(top).append(keys.back(), std::move(b));
        keys.pop_back();
        return &accept_key;
    }
    return nullptr;
}

} // namespace

Bson parse(util::Span<const char> json)
{
    Parser p;
    return p.parse(json);
}

bool accept(util::Span<const char> json) noexcept
{
    using Parser = util::JSONParser;

    Parser string_parser{[&](auto&&) -> std::error_condition {
        return std::error_condition{};
    }};
    auto ec = string_parser.parse(json);

    return !bool(ec);
}

} // namespace bson
} // namespace realm
