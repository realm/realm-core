
#include <realm/sync/changeset_parser.hpp>

#include <realm/global_key.hpp>
#include <realm/mixed.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/sync/instructions.hpp>
#include <realm/sync/noinst/integer_codec.hpp>
#include <realm/table.hpp>
#include <realm/util/base64.hpp>

#include <set>

using namespace realm;
using namespace realm::sync;

namespace {

struct State {
    util::NoCopyInputStream& m_input;
    InstructionHandler& m_handler;

    explicit State(util::NoCopyInputStream& input, InstructionHandler& handler)
        : m_input(input)
        , m_handler(handler)
    {
    }

    // pointer into transaction log, each instruction is parsed from m_input_begin and onwards.
    // Each instruction are assumed to be contiguous in memory.
    const char* m_input_begin = nullptr;
    // pointer to one past current instruction log chunk. If m_input_begin reaches m_input_end,
    // a call to next_input_buffer will move m_input_begin and m_input_end to a new chunk of
    // memory. Setting m_input_end to 0 disables this check, and is used if it is already known
    // that all of the instructions are in memory.
    const char* m_input_end = nullptr;

    std::string m_buffer;
    std::set<uint32_t> m_valid_interned_strings;
    // Cannot use StringData as key type since m_input_begin may start pointing
    // to a new chunk of memory.
    std::set<std::string, std::less<>> m_intern_strings;


    void parse_one(); // Throws
    bool has_next() noexcept;

    // Advance m_input_begin and m_input_end to reflect the next block of
    // instructions.
    // Returns false if no more input was available
    bool next_input_buffer() noexcept;

    template <class T = int64_t>
    T read_int(); // Throws

    util::Optional<Instruction::Payload::Type> read_optional_payload_type();
    Instruction::Payload::Type read_payload_type();
    Instruction::AddColumn::CollectionType read_collection_type();
    Instruction::Payload read_payload();
    Instruction::Payload::Link read_link();
    Instruction::PrimaryKey read_object_key();
    Instruction::Path read_path();
    bool read_char(char& c) noexcept;
    void read_bytes(char* data, size_t size); // Throws
    bool read_bool();                         // Throws
    float read_float();                       // Throws
    double read_double();                     // Throws
    InternString read_intern_string();        // Throws
    GlobalKey read_global_key();              // Throws
    Timestamp read_timestamp();               // Throws
    ObjectId read_object_id();                // Throws
    Decimal128 read_decimal();                // Throws
    UUID read_uuid();                         // Throws

    void read_path_instr(Instruction::PathInstruction& instr);

    // Reads a string value from the stream. The returned value is only valid
    // until the next call to `read_string()` or `read_binary()`.
    StringData read_string(); // Throws

    // Reads a binary blob value from the stream. The returned value is only
    // valid until the next call to `read_string()` or `read_binary()`.
    BinaryData read_binary(); // Throws

    BinaryData read_buffer(size_t size);

    REALM_NORETURN void parser_error(const char* complaint); // Throws
    REALM_NORETURN void parser_error()
    {
        parser_error("Bad input");
    } // Throws
};

struct UnreachableInstructionHandler : public InstructionHandler {
    void set_intern_string(uint32_t, StringBufferRange) override
    {
        REALM_UNREACHABLE();
    }

    StringBufferRange add_string_range(StringData) override
    {
        REALM_UNREACHABLE();
    }

    void operator()(const Instruction&) override
    {
        REALM_UNREACHABLE();
    }
};

struct InstructionBuilder : InstructionHandler {
    explicit InstructionBuilder(Changeset& log)
        : m_log(log)
    {
    }
    Changeset& m_log;

    void operator()(const Instruction& instr) final
    {
        m_log.push_back(instr);
    }

    StringBufferRange add_string_range(StringData string) final
    {
        return m_log.append_string(string);
    }

    void set_intern_string(uint32_t index, StringBufferRange range) final
    {
        InternStrings& strings = m_log.interned_strings();
        if (strings.size() <= index) {
            strings.resize(index + 1, StringBufferRange{0, 0});
        }
        strings[index] = range;
    }
};

Instruction::Payload::Type State::read_payload_type()
{
    using Type = Instruction::Payload::Type;
    auto type = Instruction::Payload::Type(read_int());
    // Validate the type.
    switch (type) {
        case Type::GlobalKey:
            [[fallthrough]];
        case Type::Erased:
            [[fallthrough]];
        case Type::Dictionary:
            [[fallthrough]];
        case Type::ObjectValue:
            [[fallthrough]];
        case Type::Null:
            [[fallthrough]];
        case Type::Int:
            [[fallthrough]];
        case Type::Bool:
            [[fallthrough]];
        case Type::String:
            [[fallthrough]];
        case Type::Binary:
            [[fallthrough]];
        case Type::Timestamp:
            [[fallthrough]];
        case Type::Float:
            [[fallthrough]];
        case Type::Double:
            [[fallthrough]];
        case Type::Decimal:
            [[fallthrough]];
        case Type::Link:
            [[fallthrough]];
        case Type::ObjectId:
            [[fallthrough]];
        case Type::UUID:
            return type;
    }
    parser_error("Unsupported data type");
}

Instruction::AddColumn::CollectionType State::read_collection_type()
{
    using CollectionType = Instruction::AddColumn::CollectionType;
    auto type = Instruction::AddColumn::CollectionType(read_int<uint8_t>());
    // Validate the type.
    switch (type) {
        case CollectionType::Single:
            [[fallthrough]];
        case CollectionType::List:
            [[fallthrough]];
        case CollectionType::Dictionary:
            [[fallthrough]];
        case CollectionType::Set:
            return type;
    }
    parser_error("Unsupported collection type");
}

Instruction::Payload State::read_payload()
{
    using Type = Instruction::Payload::Type;

    Instruction::Payload payload;
    payload.type = read_payload_type();
    auto& data = payload.data;
    switch (payload.type) {
        case Type::GlobalKey: {
            parser_error("Unsupported payload data type");
        }
        case Type::Int: {
            data.integer = read_int();
            return payload;
        }
        case Type::Bool: {
            data.boolean = read_bool();
            return payload;
        }
        case Type::Float: {
            data.fnum = read_float();
            return payload;
        }
        case Type::Double: {
            data.dnum = read_double();
            return payload;
        }
        case Type::String: {
            StringData value = read_string();
            data.str = m_handler.add_string_range(value);
            return payload;
        }
        case Type::Binary: {
            BinaryData value = read_binary();
            data.binary = m_handler.add_string_range(StringData{value.data(), value.size()});
            return payload;
        }
        case Type::Timestamp: {
            data.timestamp = read_timestamp();
            return payload;
        }
        case Type::ObjectId: {
            data.object_id = read_object_id();
            return payload;
        }
        case Type::Decimal: {
            data.decimal = read_decimal();
            return payload;
        }
        case Type::UUID: {
            data.uuid = read_uuid();
            return payload;
        }
        case Type::Link: {
            data.link = read_link();
            return payload;
        }

        case Type::Null:
            [[fallthrough]];
        case Type::Dictionary:
            [[fallthrough]];
        case Type::Erased:
            [[fallthrough]];
        case Type::ObjectValue:
            return payload;
    }

    parser_error("Unsupported payload type");
}

Instruction::PrimaryKey State::read_object_key()
{
    using Type = Instruction::Payload::Type;
    Type type = read_payload_type();
    switch (type) {
        case Type::Null:
            return mpark::monostate{};
        case Type::Int:
            return read_int();
        case Type::String:
            return read_intern_string();
        case Type::GlobalKey:
            return read_global_key();
        case Type::ObjectId:
            return read_object_id();
        case Type::UUID:
            return read_uuid();
        default:
            break;
    }
    parser_error("Unsupported object key type");
}

Instruction::Payload::Link State::read_link()
{
    auto target_class = read_intern_string();
    auto key = read_object_key();
    return Instruction::Payload::Link{target_class, key};
}

Instruction::Path State::read_path()
{
    Instruction::Path path;
    size_t path_len = read_int<uint32_t>();

    // Note: Not reserving `path_len`, because a corrupt changeset could cause std::bad_alloc to be thrown.
    if (path_len != 0)
        path.m_path.reserve(16);

    for (size_t i = 0; i < path_len; ++i) {
        int64_t element = read_int();
        if (element >= 0) {
            // Integer path element
            path.m_path.emplace_back(uint32_t(element));
        }
        else {
            // String path element
            path.m_path.emplace_back(read_intern_string());
        }
    }

    return path;
}

void State::read_path_instr(Instruction::PathInstruction& instr)
{
    instr.table = read_intern_string();
    instr.object = read_object_key();
    instr.field = read_intern_string();
    instr.path = read_path();
}

void State::parse_one()
{
    uint64_t t = read_int<uint64_t>();

    if (t == InstrTypeInternString) {
        uint32_t index = read_int<uint32_t>();
        StringData str = read_string();
        if (auto it = m_intern_strings.find(static_cast<std::string_view>(str)); it != m_intern_strings.end()) {
            parser_error("Unexpected intern string");
        }
        if (auto it = m_valid_interned_strings.find(index); it != m_valid_interned_strings.end()) {
            parser_error("Unexpected intern index");
        }
        StringBufferRange range = m_handler.add_string_range(str);
        m_handler.set_intern_string(index, range);
        m_valid_interned_strings.emplace(index);
        m_intern_strings.emplace(std::string{str});
        return;
    }

    switch (Instruction::Type(t)) {
        case Instruction::Type::AddTable: {
            Instruction::AddTable instr;
            instr.table = read_intern_string();
            auto table_type = Table::Type(read_int<uint8_t>());
            switch (table_type) {
                case Table::Type::TopLevel:
                case Table::Type::TopLevelAsymmetric: {
                    Instruction::AddTable::TopLevelTable spec;
                    spec.pk_field = read_intern_string();
                    spec.pk_type = read_payload_type();
                    if (!is_valid_key_type(spec.pk_type)) {
                        parser_error("Invalid primary key type in AddTable");
                    }
                    spec.pk_nullable = read_bool();
                    spec.is_asymmetric = (table_type == Table::Type::TopLevelAsymmetric);
                    instr.type = spec;
                    break;
                }
                case Table::Type::Embedded: {
                    instr.type = Instruction::AddTable::EmbeddedTable{};
                    break;
                }
                default:
                    parser_error("AddTable: unknown table type");
            }
            m_handler(instr);
            return;
        }
        case Instruction::Type::EraseTable: {
            Instruction::EraseTable instr;
            instr.table = read_intern_string();
            m_handler(instr);
            return;
        }
        case Instruction::Type::CreateObject: {
            Instruction::CreateObject instr;
            instr.table = read_intern_string();
            instr.object = read_object_key();
            m_handler(instr);
            return;
        }
        case Instruction::Type::EraseObject: {
            Instruction::EraseObject instr;
            instr.table = read_intern_string();
            instr.object = read_object_key();
            m_handler(instr);
            return;
        }
        case Instruction::Type::Update: {
            Instruction::Update instr;
            read_path_instr(instr);
            instr.value = read_payload();

            // If the last path element is a string, we are setting a field. Otherwise, we are setting an array
            // element.
            if (!instr.is_array_update()) {
                instr.is_default = read_bool();
            }
            else {
                instr.prior_size = read_int<uint32_t>();
            }
            m_handler(instr);
            return;
        }
        case Instruction::Type::AddInteger: {
            Instruction::AddInteger instr;
            read_path_instr(instr);
            instr.value = read_int();
            m_handler(instr);
            return;
        }
        case Instruction::Type::AddColumn: {
            Instruction::AddColumn instr;
            instr.table = read_intern_string();
            instr.field = read_intern_string();
            instr.type = read_payload_type();
            instr.nullable = read_bool();
            instr.collection_type = read_collection_type();
            if (instr.type == Instruction::Payload::Type::Link) {
                instr.link_target_table = read_intern_string();
            }
            if (instr.collection_type == Instruction::AddColumn::CollectionType::Dictionary) {
                instr.key_type = read_payload_type();
            }
            else {
                instr.key_type = Instruction::Payload::Type::Null;
            }
            m_handler(instr);
            return;
        }
        case Instruction::Type::EraseColumn: {
            Instruction::EraseColumn instr;
            instr.table = read_intern_string();
            instr.field = read_intern_string();
            m_handler(instr);
            return;
        }
        case Instruction::Type::ArrayInsert: {
            Instruction::ArrayInsert instr;
            read_path_instr(instr);
            if (!instr.path.is_array_index()) {
                parser_error("ArrayInsert without an index");
            }
            instr.value = read_payload();
            instr.prior_size = read_int<uint32_t>();
            m_handler(instr);
            return;
        }
        case Instruction::Type::ArrayMove: {
            Instruction::ArrayMove instr;
            read_path_instr(instr);
            if (!instr.path.is_array_index()) {
                parser_error("ArrayMove without an index");
            }
            instr.ndx_2 = read_int<uint32_t>();
            instr.prior_size = read_int<uint32_t>();
            m_handler(instr);
            return;
        }
        case Instruction::Type::ArrayErase: {
            Instruction::ArrayErase instr;
            read_path_instr(instr);
            if (!instr.path.is_array_index()) {
                parser_error("ArrayErase without an index");
            }
            instr.prior_size = read_int<uint32_t>();
            m_handler(instr);
            return;
        }
        case Instruction::Type::Clear: {
            Instruction::Clear instr;
            read_path_instr(instr);
            uint32_t prior_size = read_int<uint32_t>();
            static_cast<void>(prior_size); // Ignored
            m_handler(instr);
            return;
        }
        case Instruction::Type::SetInsert: {
            Instruction::SetInsert instr;
            read_path_instr(instr);
            instr.value = read_payload();
            m_handler(instr);
            return;
        }
        case Instruction::Type::SetErase: {
            Instruction::SetErase instr;
            read_path_instr(instr);
            instr.value = read_payload();
            m_handler(instr);
            return;
        }
    }

    parser_error("unknown instruction");
}


bool State::has_next() noexcept
{
    return m_input_begin != m_input_end || next_input_buffer();
}

bool State::next_input_buffer() noexcept
{
    auto next = m_input.next_block();
    m_input_begin = next.begin();
    m_input_end = next.end();
    return m_input_begin != m_input_end;
}

template <class T>
T State::read_int()
{
    T value = 0;
    if (REALM_LIKELY(_impl::decode_int(*this, value)))
        return value;
    parser_error("bad changeset - integer decoding failure");
}

bool State::read_char(char& c) noexcept
{
    if (m_input_begin == m_input_end && !next_input_buffer())
        return false;
    c = *m_input_begin++;
    return true;
}

void State::read_bytes(char* data, size_t size)
{
    for (;;) {
        const size_t avail = m_input_end - m_input_begin;
        if (size <= avail)
            break;
        std::copy_n(m_input_begin, avail, data);
        if (!next_input_buffer())
            parser_error("truncated input");
        data += avail;
        size -= avail;
    }
    const char* to = m_input_begin + size;
    std::copy_n(m_input_begin, size, data);
    m_input_begin = to;
}

bool State::read_bool()
{
    return read_int<uint8_t>(); // Throws
}

float State::read_float()
{
    static_assert(std::numeric_limits<float>::is_iec559 &&
                      sizeof(float) * std::numeric_limits<unsigned char>::digits == 32,
                  "Unsupported 'float' representation");
    float value;
    read_bytes(reinterpret_cast<char*>(&value), sizeof value); // Throws
    return value;
}

double State::read_double()
{
    static_assert(std::numeric_limits<double>::is_iec559 &&
                      sizeof(double) * std::numeric_limits<unsigned char>::digits == 64,
                  "Unsupported 'double' representation");
    double value;
    read_bytes(reinterpret_cast<char*>(&value), sizeof value); // Throws
    return value;
}

InternString State::read_intern_string()
{
    uint32_t index = read_int<uint32_t>(); // Throws
    if (m_valid_interned_strings.find(index) == m_valid_interned_strings.end())
        parser_error("Invalid interned string");
    return InternString{index};
}

GlobalKey State::read_global_key()
{
    uint64_t hi = read_int<uint64_t>(); // Throws
    uint64_t lo = read_int<uint64_t>(); // Throws
    return GlobalKey{hi, lo};
}

Timestamp State::read_timestamp()
{
    int64_t seconds = read_int<int64_t>();     // Throws
    int64_t nanoseconds = read_int<int64_t>(); // Throws
    if (nanoseconds > std::numeric_limits<int32_t>::max())
        parser_error("timestamp out of range");
    return Timestamp{seconds, int32_t(nanoseconds)};
}

ObjectId State::read_object_id()
{
    // FIXME: This is completely wrong and unsafe.
    ObjectId id;
    read_bytes(reinterpret_cast<char*>(&id), sizeof(id));
    return id;
}

UUID State::read_uuid()
{
    UUID::UUIDBytes bytes{};
    read_bytes(reinterpret_cast<char*>(bytes.data()), bytes.size());
    return UUID(bytes);
}

Decimal128 State::read_decimal()
{
    _impl::Bid128 cx;
    if (!_impl::decode_int(*this, cx))
        parser_error("bad changeset - decimal decoding failure");

    int exp = read_int<int>();
    bool sign = read_int<int>() != 0;
    Decimal128::Bid128 tmp;
    memcpy(&tmp, &cx, sizeof(Decimal128::Bid128));
    return Decimal128(tmp, exp, sign);
}

StringData State::read_string()
{
    uint64_t size = read_int<uint64_t>(); // Throws

    if (size > realm::Table::max_string_size)
        parser_error("string too long"); // Throws
    if (size > std::numeric_limits<size_t>::max())
        parser_error("invalid length"); // Throws

    BinaryData buffer = read_buffer(size_t(size));
    return StringData{buffer.data(), size_t(size)};
}

BinaryData State::read_binary()
{
    uint64_t size = read_int<uint64_t>(); // Throws

    if (size > std::numeric_limits<size_t>::max())
        parser_error("invalid binary length"); // Throws

    return read_buffer(size_t(size));
}

BinaryData State::read_buffer(size_t size)
{
    const size_t avail = m_input_end - m_input_begin;
    if (avail >= size) {
        m_input_begin += size;
        return BinaryData(m_input_begin - size, size);
    }

    m_buffer.clear();
    m_buffer.resize(size); // Throws
    read_bytes(m_buffer.data(), size);
    return BinaryData(m_buffer.data(), size);
}

void State::parser_error(const char* complaints)
{
    throw BadChangesetError{complaints};
}

} // anonymous namespace

namespace realm {
namespace sync {

void parse_changeset(util::InputStream& input, Changeset& out_log)
{
    util::Buffer<char> input_buffer{1024};
    util::NoCopyInputStreamAdaptor in_2{input, input_buffer};
    return parse_changeset(in_2, out_log);
}

void parse_changeset(util::NoCopyInputStream& input, Changeset& out_log)
{
    InstructionBuilder builder{out_log};
    State state{input, builder};

    while (state.has_next())
        state.parse_one();
}

OwnedMixed parse_base64_encoded_primary_key(std::string_view str)
{
    auto bin_encoded = util::base64_decode_to_vector(str);
    if (!bin_encoded) {
        throw BadChangesetError("invalid base64 in base64-encoded primary key");
    }
    util::SimpleNoCopyInputStream stream(*bin_encoded);
    UnreachableInstructionHandler fake_encoder;
    State state{stream, fake_encoder};
    using Type = Instruction::Payload::Type;
    Type type = state.read_payload_type();
    switch (type) {
        case Type::Null:
            return OwnedMixed{};
        case Type::Int:
            return OwnedMixed{state.read_int()};
        case Type::String: {
            auto str = state.read_string();
            return OwnedMixed{std::string{str.data(), str.size()}};
        }
        case Type::GlobalKey:
            // GlobalKey's are not actually used as primary keys in sync. We currently have wire protocol support
            // for them, but we've never sent them to the sync server.
            REALM_UNREACHABLE();
        case Type::ObjectId:
            return OwnedMixed{state.read_object_id()};
        case Type::UUID:
            return OwnedMixed{state.read_uuid()};
        default:
            throw BadChangesetError(util::format("invalid primary key type %1", static_cast<int>(type)));
    }
}

} // namespace sync
} // namespace realm
