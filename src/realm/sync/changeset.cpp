#include <realm/sync/changeset.hpp>

#if REALM_DEBUG
#include <iostream>
#include <iomanip>
#include <sstream>
#endif // REALM_DEBUG

using namespace realm;
using namespace realm::sync;
using namespace realm::util;

Changeset::Changeset()
{
    m_strings = std::make_shared<InternStrings>();
    m_string_buffer = std::make_shared<StringBuffer>();
}

Changeset::Changeset(const Changeset& other, share_buffers_tag)
    : version(other.version)
    , last_integrated_remote_version(other.last_integrated_remote_version)
    , origin_timestamp(other.origin_timestamp)
    , origin_file_ident(other.origin_file_ident)
{
    m_strings = other.m_strings;
    m_string_buffer = other.m_string_buffer;
}

InternString Changeset::intern_string(StringData str)
{
    if (InternString interned = find_string(str))
        return interned;

    REALM_ASSERT(m_string_buffer->size() < std::numeric_limits<uint32_t>::max());
    REALM_ASSERT(m_strings->size() < std::numeric_limits<uint32_t>::max());
    REALM_ASSERT(str.size() < std::numeric_limits<uint32_t>::max());

    // FIXME: Very slow.
    uint32_t size = uint32_t(str.size());
    uint32_t offset = uint32_t(m_string_buffer->size());
    m_string_buffer->append(str.data(), size);
    uint32_t index = uint32_t(m_strings->size());
    m_strings->push_back(StringBufferRange{offset, size});
    return InternString{index};
}


InternString Changeset::find_string(StringData string) const noexcept
{
    // FIXME: Linear search can be very expensive as changesets can be very big
    std::size_t n = m_strings->size();
    for (std::size_t i = 0; i < n; ++i) {
        const auto& range = (*m_strings)[i];
        StringData string_2{m_string_buffer->data() + range.offset, range.size};
        if (string_2 == string)
            return InternString{std::uint_least32_t(i)};
    }
    return InternString{};
}

PrimaryKey Changeset::get_key(const Instruction::PrimaryKey& key) const noexcept
{
    // we do not use the expected `mpark::visit(overload...` because in MSVC 2019 this
    // code produces a segfault for something that works on other compilers.
    // See https://github.com/realm/realm-core/issues/4624
    if (const auto int64_ptr = mpark::get_if<int64_t>(&key)) {
        return *int64_ptr;
    }
    else if (const auto intern_string_ptr = mpark::get_if<InternString>(&key)) {
        return this->get_string(*intern_string_ptr);
    }
    else if (const auto monostate_ptr = mpark::get_if<mpark::monostate>(&key)) {
        return *monostate_ptr;
    }
    else if (const auto global_key_ptr = mpark::get_if<GlobalKey>(&key)) {
        return *global_key_ptr;
    }
    else if (const auto oid_ptr = mpark::get_if<ObjectId>(&key)) {
        return *oid_ptr;
    }
    else if (const auto uuid_ptr = mpark::get_if<UUID>(&key)) {
        return *uuid_ptr;
    }
    else {
        REALM_UNREACHABLE(); // unhandled primary key type
    }
}

bool Changeset::operator==(const Changeset& that) const noexcept
{
    if (m_instructions == that.m_instructions) {
        return *m_strings == *that.m_strings;
    }
    return false;
}

std::ostream& Changeset::print_value(std::ostream& os, const Instruction::Payload& value) const noexcept
{
    using Type = Instruction::Payload::Type;

    os << get_type_name(value.type) << "(";
    auto& data = value.data;
    switch (value.type) {
        case Type::ObjectValue:
            break;
        case Type::GlobalKey:
            os << data.key;
            break;
        case Type::Erased:
            break;
        case Type::Dictionary:
            break;
        case Type::Null:
            break;
        case Type::Int:
            os << data.integer;
            break;
        case Type::Bool:
            os << data.boolean;
            break;
        case Type::String:
            os << "\"" << get_string(data.str) << "\"";
            break;
        case Type::Binary:
            os << "...";
            break;
        case Type::Timestamp:
            os << data.timestamp;
            break;
        case Type::Float:
            os << data.fnum;
            break;
        case Type::Double:
            os << data.dnum;
            break;
        case Type::Decimal:
            os << data.decimal;
            break;
        case Type::UUID:
            os << data.uuid;
            break;
        case Type::Link: {
            os << "target_table = " << get_string(data.link.target_table) << ", "
               << "target = " << format_pk(get_key(data.link.target));
            break;
        };
        case Type::ObjectId:
            os << data.object_id;
            break;
    }
    return os << ")";
}

std::ostream& Changeset::print_path(std::ostream& os, const Instruction::Path& path) const noexcept
{
    bool first = true;
    for (auto& element : path.m_path) {
        if (!first) {
            os << '.';
        }
        first = false;
        auto print = overload{
            [&](uint32_t index) {
                os << index;
            },
            [&](InternString str) {
                os << get_string(str);
            },
        };
        mpark::visit(print, element);
    }
    return os;
}

std::ostream& Changeset::print_path(std::ostream& os, InternString table, const Instruction::PrimaryKey& pk,
                                    util::Optional<InternString> field, const Instruction::Path* path) const
{
    os << get_string(table) << "[" << format_pk(get_key(pk)) << "]";
    if (field) {
        os << "." << get_string(*field);
    }
    if (path) {
        for (auto& element : *path) {
            if (auto subfield = mpark::get_if<InternString>(&element)) {
                os << "." << get_string(*subfield);
            }
            else if (auto index = mpark::get_if<uint32_t>(&element)) {
                os << "[" << *index << "]";
            }
            else {
                REALM_TERMINATE("Invalid path");
            }
        }
    }
    return os;
}

std::ostream& realm::sync::operator<<(std::ostream& os, const Changeset& changeset)
{
#if REALM_DEBUG // LCOV_EXCL_START
    changeset.print(os);
    return os;
#else
    return os << "[changeset with " << changeset.size() << " instructions]";
#endif
}


#if REALM_DEBUG // LCOV_EXCL_START
void Changeset::print(std::ostream& os) const
{
    Changeset::Printer printer{os};
    Changeset::Reflector reflector{printer, *this};
    os << std::left << std::setw(16) << "InternStrings";
    for (size_t i = 0; i < m_strings->size(); ++i) {
        os << i << "=\"" << get_string(m_strings->at(i)) << '"';
        if (i + 1 != m_strings->size())
            os << ", ";
    }
    os << "\n";

    reflector.visit_all();
}

void Changeset::print() const
{
    print(std::cerr);
}


void Changeset::verify() const
{
    for (size_t i = 0; i < m_strings->size(); ++i) {
        auto& range = m_strings->at(i);
        REALM_ASSERT(range.offset <= m_string_buffer->size());
        REALM_ASSERT(range.offset + range.size <= m_string_buffer->size());
    }

    auto verify_string_range = [&](StringBufferRange range) {
        REALM_ASSERT(range.offset <= m_string_buffer->size());
        REALM_ASSERT(range.offset + range.size <= m_string_buffer->size());
    };

    auto verify_intern_string = [&](InternString str) {
        auto range = get_intern_string(str);
        verify_string_range(range);
    };

    auto verify_key = [&](const Instruction::PrimaryKey& key) {
        mpark::visit(util::overload{[&](InternString str) {
                                        verify_intern_string(str);
                                    },
                                    [](auto&&) {}},
                     key);
    };

    auto verify_payload = [&](const Instruction::Payload& payload) {
        using Type = Instruction::Payload::Type;
        switch (payload.type) {
            case Type::String: {
                return verify_string_range(payload.data.str);
            }
            case Type::Binary: {
                return verify_string_range(payload.data.binary);
            }
            case Type::Link: {
                verify_intern_string(payload.data.link.target_table);
                return verify_key(payload.data.link.target);
            }
            default:
                return;
        }
    };

    auto verify_path = [&](const Instruction::Path& path) {
        for (auto& element : path.m_path) {
            mpark::visit(util::overload{[&](InternString str) {
                                            verify_intern_string(str);
                                        },
                                        [](auto&&) {}},
                         element);
        }
    };

    for (auto instr : *this) {
        if (!instr)
            continue;

        if (auto table_instr = instr->get_if<Instruction::TableInstruction>()) {
            verify_intern_string(table_instr->table);
            if (auto object_instr = instr->get_if<Instruction::ObjectInstruction>()) {
                verify_key(object_instr->object);

                if (auto path_instr = instr->get_if<Instruction::PathInstruction>()) {
                    verify_path(path_instr->path);
                }

                if (auto set_instr = instr->get_if<Instruction::Update>()) {
                    verify_payload(set_instr->value);
                }
                else if (auto insert_instr = instr->get_if<Instruction::ArrayInsert>()) {
                    verify_payload(insert_instr->value);
                }
            }
            else if (auto add_table_instr = instr->get_if<Instruction::AddTable>()) {
                mpark::visit(util::overload{
                                 [&](const Instruction::AddTable::PrimaryKeySpec& spec) {
                                     REALM_ASSERT(is_valid_key_type(spec.type));
                                     verify_intern_string(spec.field);
                                 },
                                 [](const Instruction::AddTable::EmbeddedTable&) {},
                             },
                             add_table_instr->type);
            }
            else if (auto add_column_instr = instr->get_if<Instruction::AddColumn>()) {
                verify_intern_string(add_column_instr->field);
                if (add_column_instr->type == Instruction::Payload::Type::Link) {
                    verify_intern_string(add_column_instr->link_target_table);
                }
            }
            else if (auto erase_column_instr = instr->get_if<Instruction::EraseColumn>()) {
                verify_intern_string(erase_column_instr->field);
            }
        }
        else {
            REALM_TERMINATE("Corrupt instruction type");
        }
    }
}

void Changeset::Reflector::operator()(const Instruction::AddTable& p) const
{
    m_tracer.name("AddTable");
    table_instr(p);
    auto trace = util::overload{
        [&](const Instruction::AddTable::PrimaryKeySpec& spec) {
            m_tracer.field("pk_field", spec.field);
            m_tracer.field("pk_type", spec.type);
            m_tracer.field("pk_nullable", spec.nullable);
        },
        [&](const Instruction::AddTable::EmbeddedTable&) {
            m_tracer.field("embedded", true);
        },
    };
    mpark::visit(trace, p.type);
}

void Changeset::Reflector::operator()(const Instruction::EraseTable& p) const
{
    m_tracer.name("EraseTable");
    table_instr(p);
}

void Changeset::Reflector::operator()(const Instruction::Update& p) const
{
    m_tracer.name("Update");
    path_instr(p);
    m_tracer.field("value", p.value);
    if (p.is_array_update()) {
        m_tracer.field("prior_size", p.prior_size);
    }
    else {
        m_tracer.field("default", p.is_default);
    }
}

void Changeset::Reflector::operator()(const Instruction::AddInteger& p) const
{
    m_tracer.name("AddInteger");
    path_instr(p);
    m_tracer.field("value", Instruction::Payload{p.value});
}

void Changeset::Reflector::operator()(const Instruction::CreateObject& p) const
{
    m_tracer.name("CreateObject");
    object_instr(p);
}

void Changeset::Reflector::operator()(const Instruction::EraseObject& p) const
{
    m_tracer.name("EraseObject");
    object_instr(p);
}

void Changeset::Reflector::operator()(const Instruction::ArrayInsert& p) const
{
    m_tracer.name("ArrayInsert");
    path_instr(p);
    m_tracer.field("value", p.value);
    m_tracer.field("prior_size", p.prior_size);
}

void Changeset::Reflector::operator()(const Instruction::ArrayMove& p) const
{
    m_tracer.name("ArrayMove");
    path_instr(p);
    m_tracer.field("ndx_2", p.ndx_2);
    m_tracer.field("prior_size", p.prior_size);
}

void Changeset::Reflector::operator()(const Instruction::ArrayErase& p) const
{
    m_tracer.name("ArrayErase");
    path_instr(p);
    m_tracer.field("prior_size", p.prior_size);
}

void Changeset::Reflector::operator()(const Instruction::Clear& p) const
{
    m_tracer.name("Clear");
    path_instr(p);
}

void Changeset::Reflector::operator()(const Instruction::SetInsert& p) const
{
    m_tracer.name("SetInsert");
    path_instr(p);
    m_tracer.field("value", p.value);
}

void Changeset::Reflector::operator()(const Instruction::SetErase& p) const
{
    m_tracer.name("SetErase");
    path_instr(p);
    m_tracer.field("value", p.value);
}

void Changeset::Reflector::operator()(const Instruction::AddColumn& p) const
{
    m_tracer.name("AddColumn");
    m_tracer.field("table", p.table);
    m_tracer.field("field", p.field);
    if (p.type != Instruction::Payload::Type::Null) {
        m_tracer.field("type", p.type);
    }
    else {
        m_tracer.field("type", Instruction::Payload::Type::Null);
    }
    m_tracer.field("nullable", p.nullable);
    m_tracer.field("collection_type", p.collection_type);
    if (p.type == Instruction::Payload::Type::Link) {
        m_tracer.field("target_table", p.link_target_table);
    }
    if (p.collection_type == Instruction::AddColumn::CollectionType::Dictionary) {
        m_tracer.field("key_type", p.key_type);
    }
}

void Changeset::Reflector::operator()(const Instruction::EraseColumn& p) const
{
    m_tracer.name("EraseColumn");
    m_tracer.field("table", p.table);
    m_tracer.field("field", p.field);
}

void Changeset::Reflector::table_instr(const Instruction::TableInstruction& p) const
{
    m_tracer.field("path", p.table);
}

void Changeset::Reflector::object_instr(const Instruction::ObjectInstruction& p) const
{
    m_tracer.path("path", p.table, p.object, util::none, nullptr);
}

void Changeset::Reflector::path_instr(const Instruction::PathInstruction& p) const
{
    m_tracer.path("path", p.table, p.object, p.field, &p.path);
}

void Changeset::Reflector::visit_all() const
{
    m_tracer.set_changeset(&m_changeset);
    for (auto instr : m_changeset) {
        if (!instr)
            continue;
        m_tracer.before_each();
        instr->visit(*this);
        m_tracer.after_each();
    }
    m_tracer.set_changeset(nullptr);
}

void Changeset::Printer::name(StringData n)
{
    pad_or_ellipsis(n, 16);
}

void Changeset::Printer::print_field(StringData name, std::string value)
{
    if (!m_first) {
        m_out << ", ";
    }
    m_first = false;
    m_out << name << "=" << value;
}

void Changeset::Printer::path(StringData name, InternString table, const Instruction::PrimaryKey& pk,
                              util::Optional<InternString> field, const Instruction::Path* path)
{
    std::stringstream ss;
    m_changeset->print_path(ss, table, pk, field, path);
    print_field(name, ss.str());
}

void Changeset::Printer::field(StringData n, InternString value)
{
    std::stringstream ss;
    ss << "\"" << m_changeset->get_string(value) << "\"";
    print_field(n, ss.str());
}

void Changeset::Printer::field(StringData n, Instruction::Payload::Type type)
{
    print_field(n, get_type_name(type));
}

void Changeset::Printer::field(StringData n, Instruction::AddColumn::CollectionType type)
{
    print_field(n, get_collection_type(type));
}

std::string Changeset::Printer::primary_key_to_string(const Instruction::PrimaryKey& key)
{
    auto convert = overload{
        [&](const mpark::monostate&) {
            return std::string("NULL");
        },
        [&](int64_t value) {
            std::stringstream ss;
            ss << value;
            return ss.str();
        },
        [&](InternString str) {
            std::stringstream ss;
            ss << "\"" << m_changeset->get_string(str) << "\"";
            return ss.str();
        },
        [&](GlobalKey key) {
            std::stringstream ss;
            ss << key;
            return ss.str();
        },
        [&](ObjectId id) {
            std::stringstream ss;
            ss << id;
            return ss.str();
        },
        [&](UUID uuid) {
            return uuid.to_string();
        },
    };
    return mpark::visit(convert, key);
}

void Changeset::Printer::field(StringData n, const Instruction::PrimaryKey& key)
{
    std::stringstream ss;
    ss << format_pk(m_changeset->get_key(key));
    print_field(n, ss.str());
}

void Changeset::Printer::field(StringData n, const Instruction::Payload& value)
{
    std::stringstream ss;
    m_changeset->print_value(ss, value);
    print_field(n, ss.str());
}

void Changeset::Printer::field(StringData n, const Instruction::Path& path)
{
    std::stringstream ss;
    ss << "[";
    bool first = true;
    for (auto& element : path.m_path) {
        if (!first) {
            ss << ".";
        }
        first = false;

        auto print = util::overload{
            [&](InternString field) {
                ss << m_changeset->get_string(field);
            },
            [&](uint32_t index) {
                ss << index;
            },
        };
        mpark::visit(print, element);
    }
    ss << "]";
    print_field(n, ss.str());
}

void Changeset::Printer::field(StringData n, uint32_t value)
{
    std::stringstream ss;
    ss << value;
    print_field(n, ss.str());
}

void Changeset::Printer::after_each()
{
    m_out << "\n";
    m_first = true;
}

void Changeset::Printer::pad_or_ellipsis(StringData s, int width) const
{
    // FIXME: Does not work with UTF-8.
    std::string str = s; // FIXME: StringData doesn't work with iomanip because it calls ios_base::write() directly
    if (str.size() > size_t(width)) {
        m_out << str.substr(0, width - 1) << "~";
    }
    else {
        m_out << std::left << std::setw(width) << str;
    }
}

#endif // REALM_DEBUG LCOV_EXCL_STOP
