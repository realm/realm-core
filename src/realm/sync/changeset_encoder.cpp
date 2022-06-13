#include <realm/sync/noinst/integer_codec.hpp>
#include <realm/sync/changeset_encoder.hpp>

using namespace realm;
using namespace realm::sync;

void ChangesetEncoder::operator()(const Instruction::AddTable& instr)
{
    auto spec = mpark::get_if<Instruction::AddTable::TopLevelTable>(&instr.type);
    const bool is_embedded = (spec == nullptr);
    Table::Type table_type;
    if (!is_embedded) {
        if (spec->is_asymmetric) {
            table_type = Table::Type::TopLevelAsymmetric;
        }
        else {
            table_type = Table::Type::TopLevel;
        }
    }
    else {
        table_type = Table::Type::Embedded;
    }
    auto table_type_int = static_cast<uint8_t>(table_type);
    append(Instruction::Type::AddTable, instr.table, table_type_int);
    if (!is_embedded) {
        append_value(spec->pk_field);
        append_value(spec->pk_type);
        append_value(spec->pk_nullable);
    }
}

void ChangesetEncoder::operator()(const Instruction::EraseTable& instr)
{
    append(Instruction::Type::EraseTable, instr.table);
}

void ChangesetEncoder::operator()(const Instruction::CreateObject& instr)
{
    append(Instruction::Type::CreateObject, instr.table, instr.object);
}

void ChangesetEncoder::operator()(const Instruction::EraseObject& instr)
{
    append(Instruction::Type::EraseObject, instr.table, instr.object);
}

void ChangesetEncoder::operator()(const Instruction::Update& instr)
{
    if (instr.is_array_update()) {
        append_path_instr(Instruction::Type::Update, instr, instr.value, instr.prior_size);
    }
    else {
        append_path_instr(Instruction::Type::Update, instr, instr.value, instr.is_default);
    }
}

// Appends sequence [value-type, dumb-value]
void ChangesetEncoder::append_value(const Instruction::Payload& payload)
{
    using Type = Instruction::Payload::Type;

    append_value(payload.type);
    const auto& data = payload.data;

    switch (payload.type) {
        case Type::GlobalKey: {
            return append_value(data.key);
        }
        case Type::Int: {
            return append_value(data.integer);
        }
        case Type::Bool: {
            return append_value(data.boolean);
        }
        case Type::String: {
            return append_string(data.str);
        }
        case Type::Binary: {
            return append_string(data.binary);
        }
        case Type::Timestamp: {
            return append_value(data.timestamp);
        }
        case Type::Float: {
            return append_value(data.fnum);
        }
        case Type::Double: {
            return append_value(data.dnum);
        }
        case Type::Decimal: {
            return append_value(data.decimal);
        }
        case Type::ObjectId: {
            return append_value(data.object_id);
        }
        case Type::UUID: {
            return append_value(data.uuid);
        }
        case Type::Link: {
            return append_value(data.link);
        }
        case Type::Erased:
            [[fallthrough]];
        case Type::Dictionary:
            [[fallthrough]];
        case Type::ObjectValue:
            [[fallthrough]];
        case Type::Null:
            // The payload type does not carry additional data.
            return;
    }
    REALM_TERMINATE("Invalid payload type.");
}

void ChangesetEncoder::append_value(Instruction::Payload::Type type)
{
    append_value(int64_t(type));
}

void ChangesetEncoder::append_value(Instruction::AddColumn::CollectionType type)
{
    append_value(uint8_t(type));
}

void ChangesetEncoder::append_value(const Instruction::Payload::Link& link)
{
    append_value(link.target_table);
    append_value(link.target);
}

void ChangesetEncoder::append_value(const Instruction::PrimaryKey& pk)
{
    using Type = Instruction::Payload::Type;
    auto append = util::overload{
        [&](mpark::monostate) {
            append_value(Type::Null);
        },
        [&](int64_t value) {
            append_value(Type::Int);
            append_value(value);
        },
        [&](InternString str) {
            // Note: Contextual difference. In payloads, Type::String denotes a
            // StringBufferRange, but here it denotes to an InternString.
            append_value(Type::String);
            append_value(str);
        },
        [&](GlobalKey key) {
            append_value(Type::GlobalKey);
            append_value(key);
        },
        [&](ObjectId id) {
            append_value(Type::ObjectId);
            append_value(id);
        },
        [&](UUID uuid) {
            append_value(Type::UUID);
            append_value(uuid);
        },
    };
    mpark::visit(std::move(append), pk);
}

void ChangesetEncoder::append_value(const Instruction::Path& path)
{
    append_value(uint32_t(path.m_path.size()));
    for (auto& element : path.m_path) {
        // Integer path elements are encoded as their integer values.
        // String path elements are encoded as [-1, intern_string_id].
        if (auto index = mpark::get_if<uint32_t>(&element)) {
            append_value(int64_t(*index));
        }
        else if (auto name = mpark::get_if<InternString>(&element)) {
            // Since indices cannot be negative, use -1 to indicate that the path element is a
            // string.
            append_value(int64_t(-1));
            append_value(*name);
        }
    }
}

void ChangesetEncoder::operator()(const Instruction::AddInteger& instr)
{
    append_path_instr(Instruction::Type::AddInteger, instr, instr.value);
}

void ChangesetEncoder::operator()(const Instruction::AddColumn& instr)
{
    bool is_dictionary = (instr.collection_type == Instruction::AddColumn::CollectionType::Dictionary);
    // Mixed columns are always nullable.
    REALM_ASSERT(instr.type != Instruction::Payload::Type::Null || instr.nullable || is_dictionary);
    append(Instruction::Type::AddColumn, instr.table, instr.field, instr.type, instr.nullable, instr.collection_type);

    if (instr.type == Instruction::Payload::Type::Link) {
        append_value(instr.link_target_table);
    }
    if (is_dictionary) {
        append_value(instr.key_type);
    }
}

void ChangesetEncoder::operator()(const Instruction::EraseColumn& instr)
{
    append(Instruction::Type::EraseColumn, instr.table, instr.field);
}

void ChangesetEncoder::operator()(const Instruction::ArrayInsert& instr)
{
    append_path_instr(Instruction::Type::ArrayInsert, instr, instr.value, instr.prior_size);
}

void ChangesetEncoder::operator()(const Instruction::ArrayMove& instr)
{
    append_path_instr(Instruction::Type::ArrayMove, instr, instr.ndx_2, instr.prior_size);
}

void ChangesetEncoder::operator()(const Instruction::ArrayErase& instr)
{
    append_path_instr(Instruction::Type::ArrayErase, instr, instr.prior_size);
}

void ChangesetEncoder::operator()(const Instruction::Clear& instr)
{
    uint32_t prior_size = 0; // Ignored
    append_path_instr(Instruction::Type::Clear, instr, prior_size);
}

void ChangesetEncoder::operator()(const Instruction::SetInsert& instr)
{
    append_path_instr(Instruction::Type::SetInsert, instr, instr.value);
}

void ChangesetEncoder::operator()(const Instruction::SetErase& instr)
{
    append_path_instr(Instruction::Type::SetErase, instr, instr.value);
}

InternString ChangesetEncoder::intern_string(StringData str)
{
    auto it = m_intern_strings_rev.find(static_cast<std::string_view>(str));
    if (it == m_intern_strings_rev.end()) {
        size_t index = m_intern_strings_rev.size();
        // FIXME: Assert might be able to be removed after refactoring of changeset_parser types?
        REALM_ASSERT_RELEASE_EX(index <= std::numeric_limits<uint32_t>::max(), index);
        bool inserted;
        std::tie(it, inserted) = m_intern_strings_rev.insert({std::string{str}, uint32_t(index)});
        REALM_ASSERT_RELEASE_EX(inserted, str);

        StringBufferRange range = add_string_range(str);
        set_intern_string(uint32_t(index), range);
    }

    return InternString{it->second};
}

void ChangesetEncoder::set_intern_string(uint32_t index, StringBufferRange range)
{
    // Emit InternString metainstruction:
    append_int(uint64_t(InstrTypeInternString));
    append_int(index);
    append_string(range);
}

StringBufferRange ChangesetEncoder::add_string_range(StringData data)
{
    m_string_range = static_cast<std::string_view>(data);
    REALM_ASSERT(data.size() <= std::numeric_limits<uint32_t>::max());
    return StringBufferRange{0, uint32_t(data.size())};
}

void ChangesetEncoder::append_bytes(const void* bytes, size_t size)
{
    // FIXME: It would be better to move ownership of `m_buffer` to the caller,
    // potentially reducing the number of allocations to zero (amortized).
    m_buffer.reserve(1024); // lower the amount of reallocations
    m_buffer.append(static_cast<const char*>(bytes), size);
}

void ChangesetEncoder::append_string(StringBufferRange str)
{
    REALM_ASSERT(str.offset + str.size <= m_string_range.size());
    append_value(uint64_t(str.size));
    append_bytes(m_string_range.data() + str.offset, str.size);
}

template <class... Args>
void ChangesetEncoder::append(Instruction::Type t, Args&&... args)
{
    append_value(uint8_t(t));
    int unpack[] = {0, (append_value(args), 0)...};
    static_cast<void>(unpack);
}

template <class... Args>
void ChangesetEncoder::append_path_instr(Instruction::Type t, const Instruction::PathInstruction& instr,
                                         Args&&... args)
{
    append_value(uint8_t(t));
    append_value(instr.table);
    append_value(instr.object);
    append_value(instr.field);
    append_value(instr.path);
    (append_value(std::forward<Args>(args)), ...);
}

template <class T>
void ChangesetEncoder::append_int(T integer)
{
    // One sign bit plus number of value bits
    const int num_bits = 1 + std::numeric_limits<T>::digits;
    // Only the first 7 bits are available per byte. Had it not been
    // for the fact that maximum guaranteed bit width of a char is 8,
    // this value could have been increased to 15 (one less than the
    // number of value bits in 'unsigned').
    const int bits_per_byte = 7;
    const int max_bytes = (num_bits + (bits_per_byte - 1)) / bits_per_byte;

    char buffer[max_bytes];
    std::size_t n = _impl::encode_int(buffer, integer);
    append_bytes(buffer, n);
}

void ChangesetEncoder::append_value(DataType type)
{
    append_value(uint64_t(type));
}

void ChangesetEncoder::append_value(bool v)
{
    // Reduce template instantiations of append_int
    append_value(uint8_t(v));
}

void ChangesetEncoder::append_value(uint8_t integer)
{
    // Reduce template instantiations of append_int
    append_value(uint64_t(integer));
}

void ChangesetEncoder::append_value(uint32_t integer)
{
    // Reduce template instantiations of append_int
    append_value(uint64_t(integer));
}

void ChangesetEncoder::append_value(uint64_t integer)
{
    append_int(integer);
}

void ChangesetEncoder::append_value(int64_t integer)
{
    append_int(integer);
}

void ChangesetEncoder::append_value(float number)
{
    append_bytes(&number, sizeof(number));
}

void ChangesetEncoder::append_value(double number)
{
    append_bytes(&number, sizeof(number));
}

void ChangesetEncoder::append_value(InternString str)
{
    REALM_ASSERT(str != InternString::npos);
    append_value(str.value);
}

void ChangesetEncoder::append_value(GlobalKey oid)
{
    append_value(oid.hi());
    append_value(oid.lo());
}

void ChangesetEncoder::append_value(Timestamp timestamp)
{
    append_value(timestamp.get_seconds());
    append_value(int64_t(timestamp.get_nanoseconds()));
}

void ChangesetEncoder::append_value(ObjectId id)
{
    append_bytes(&id, sizeof(id));
}

void ChangesetEncoder::append_value(UUID id)
{
    const auto bytes = id.to_bytes();
    append_bytes(bytes.data(), bytes.size());
}

void ChangesetEncoder::append_value(Decimal128 id)
{
    Decimal128::Bid128 cx;
    int exp;
    bool sign;
    id.unpack(cx, exp, sign);
    char buffer[16];
    _impl::Bid128 tmp;
    memcpy(&tmp, &cx, sizeof(Decimal128::Bid128));
    auto n = _impl::encode_int(buffer, tmp);
    append_bytes(buffer, n);
    append_value(int64_t(exp));
    append_value(sign);
}

auto ChangesetEncoder::release() noexcept -> Buffer
{
    m_intern_strings_rev.clear();
    return std::move(m_buffer);
}

void ChangesetEncoder::reset() noexcept
{
    m_intern_strings_rev.clear();
    m_buffer.clear();
}

void ChangesetEncoder::encode_single(const Changeset& log)
{
    // Checking if the log is empty avoids serialized interned strings in a
    // changeset where all meaningful instructions have been discarded due to
    // merge or compaction.
    if (!log.empty()) {
        add_string_range(log.string_data());
        const auto& strings = log.interned_strings();
        for (size_t i = 0; i < strings.size(); ++i) {
            set_intern_string(uint32_t(i), strings[i]); // Throws
        }
        for (auto instr : log) {
            if (!instr)
                continue;
            (*this)(*instr); // Throws
        }
    }
}
