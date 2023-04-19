#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <type_traits>
#include <limits>
#include <algorithm>
#include <memory>
#include <string>
#include <set>
#include <sstream>
#include <iostream>

#include <realm/array_integer.hpp>
#include <realm/util/features.h>
#include <realm/util/optional.hpp>
#include <realm/util/enum.hpp>
#include <realm/util/hex_dump.hpp>
#include <realm/util/timestamp_formatter.hpp>
#include <realm/util/load_file.hpp>
#include <realm/group.hpp>
#include <realm/version.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/sync/protocol.hpp>

using namespace realm;
using util::TimestampFormatter;
using IntegerBpTree = BPlusTree<std::int64_t>; // FIXME: Avoid use of optional type `std::int64_t`
using sync::file_ident_type;
using sync::salt_type;
using sync::SaltedFileIdent;
using sync::timestamp_type;
using sync::UploadCursor;
using sync::version_type;
using ClientType = _impl::ServerHistory::ClientType;

// FIXME: Ideas for additional forms of filtering:
// - Filter to class (only changesets that refer to an object of this class or
//   to the class itself).
// - Filter to origin time range (only changesets that originate in the
//   specified time range).


namespace {

enum class Format { auto_, nothing, version, info, annotate, changeset, hexdump, raw };
enum class Summary { auto_, off, brief, full };

struct FormatSpec {
    static util::EnumAssoc map[];
};
util::EnumAssoc FormatSpec::map[] = {
    {int(Format::auto_), "auto"},      {int(Format::nothing), "nothing"},   {int(Format::version), "version"},
    {int(Format::info), "info"},       {int(Format::annotate), "annotate"}, {int(Format::changeset), "changeset"},
    {int(Format::hexdump), "hexdump"}, {int(Format::raw), "raw"},           {0, nullptr}};
using FormatEnum = util::Enum<Format, FormatSpec>;

struct SummarySpec {
    static util::EnumAssoc map[];
};
util::EnumAssoc SummarySpec::map[] = {{int(Summary::auto_), "auto"},
                                      {int(Summary::off), "off"},
                                      {int(Summary::brief), "brief"},
                                      {int(Summary::full), "full"},
                                      {0, nullptr}};
using SummaryEnum = util::Enum<Summary, SummarySpec>;

struct InstructionTypeSpec {
    static util::EnumAssoc map[];
};

util::EnumAssoc InstructionTypeSpec::map[] = {{int(sync::Instruction::Type::AddTable), "AddTable"},
                                              {int(sync::Instruction::Type::EraseTable), "EraseTable"},
                                              {int(sync::Instruction::Type::CreateObject), "CreateObject"},
                                              {int(sync::Instruction::Type::EraseObject), "EraseObject"},
                                              {int(sync::Instruction::Type::Update), "Update"},
                                              {int(sync::Instruction::Type::AddInteger), "AddInteger"},
                                              {int(sync::Instruction::Type::AddColumn), "AddColumn"},
                                              {int(sync::Instruction::Type::EraseColumn), "EraseColumn"},
                                              {int(sync::Instruction::Type::ArrayInsert), "ArrayInsert"},
                                              {int(sync::Instruction::Type::ArrayMove), "ArrayMove"},
                                              {int(sync::Instruction::Type::ArrayErase), "ArrayErase"},
                                              {int(sync::Instruction::Type::Clear), "Clear"},
                                              {int(sync::Instruction::Type::SetInsert), "SetInsert"},
                                              {int(sync::Instruction::Type::SetErase), "SetErase"},
                                              {0, nullptr}};
using InstructionTypeEnum = util::Enum<sync::Instruction::Type, InstructionTypeSpec>;


template <class T>
std::string format_num_something(T num, const char* singular_form, const char* plural_form,
                                 std::locale loc = std::locale{})
{
    using lim = std::numeric_limits<T>;
    bool need_singular = (num == T(1) || (lim::is_signed && num == T(-1)));
    const char* form = (need_singular ? singular_form : plural_form);
    std::ostringstream out;
    out.imbue(loc);
    out << num << " " << form;
    return std::move(out).str();
}

std::string format_num_history_entries(std::size_t num)
{
    return format_num_something(num, "history entry", "history entries");
}


bool get_changeset_size(const BinaryColumn& col, std::size_t row_ndx, std::size_t& size) noexcept
{
    std::size_t size_2 = 0;
    std::size_t pos = 0;
    BinaryData chunk = col.get_at(row_ndx, pos);
    if (!chunk.is_null()) {
        for (;;) {
            size_2 += chunk.size();
            if (pos == 0) {
                size = size_2;
                return true;
            }
            chunk = col.get_at(row_ndx, pos);
        }
    }
    return false;
}


bool get_changeset(const BinaryColumn& col, std::size_t row_ndx, util::AppendBuffer<char>& buffer)
{
    std::size_t pos = 0;
    BinaryData chunk = col.get_at(row_ndx, pos);
    if (!chunk.is_null()) {
        for (;;) {
            buffer.append(chunk.data(), chunk.size()); // Throws
            if (pos == 0)
                return true;
            chunk = col.get_at(row_ndx, pos);
        }
    }
    return false;
}


enum class LogicalClientType {
    special,
    upstream,
    self,
    indirect,
    regular,
    subserver,
    legacy,
};


void all_client_files(std::set<LogicalClientType>& types)
{
    types = {LogicalClientType::special,  LogicalClientType::upstream, LogicalClientType::self,
             LogicalClientType::indirect, LogicalClientType::regular,  LogicalClientType::subserver,
             LogicalClientType::legacy};
}


bool parse_client_types(std::string_view string, std::set<LogicalClientType>& types)
{
    std::set<LogicalClientType> types_2;
    for (char ch : string) {
        switch (ch) {
            case 'r':
                types_2.insert(LogicalClientType::regular);
                continue;
            case 's':
                types_2.insert(LogicalClientType::subserver);
                continue;
            case 'l':
                types_2.insert(LogicalClientType::legacy);
                continue;
            case 'i':
                types_2.insert(LogicalClientType::indirect);
                continue;
            case 'u':
                types_2.insert(LogicalClientType::upstream);
                continue;
            case 'S':
                types_2.insert(LogicalClientType::self);
                continue;
            case 'U':
                types_2.insert(LogicalClientType::special);
                continue;
        }
        return false;
    }
    types = types_2;
    return true;
}


class Expr {
public:
    using InternString = sync::InternString;
    using Instruction = sync::Instruction;
    using Payload = Instruction::Payload;
    using Changeset = sync::Changeset;

    struct InstrInfo {
        Instruction::Type type;
        InternString class_name;
        sync::instr::PrimaryKey object_id;
        InternString property;
        const Payload* payload;

        bool is_modification() const noexcept
        {
            return true;
        }
    };

    virtual void reset(const Changeset&) noexcept {}

    virtual bool eval(const InstrInfo&) const noexcept = 0;

    virtual ~Expr() noexcept {}
};


class InstructionTypeExpr : public Expr {
public:
    InstructionTypeExpr(Instruction::Type type) noexcept
        : m_type{type}
    {
    }

    bool eval(const InstrInfo& instr) const noexcept override final
    {
        return (instr.type == m_type);
    }

private:
    const Instruction::Type m_type;
};


class ModifiesClassExpr : public Expr {
public:
    ModifiesClassExpr(std::string class_name) noexcept
        : m_class_name{std::move(class_name)}
    {
    }

    void reset(const Changeset& changeset) noexcept override
    {
        m_interned_class_name = changeset.find_string(m_class_name);
    }

    bool eval(const InstrInfo& instr) const noexcept override
    {
        return (instr.class_name && instr.class_name == m_interned_class_name && instr.is_modification());
    }

private:
    const std::string m_class_name;
    InternString m_interned_class_name;
};


class ModifiesObjectExpr : public ModifiesClassExpr {
public:
    ModifiesObjectExpr(std::string class_name, sync::instr::PrimaryKey object_id) noexcept
        : ModifiesClassExpr{std::move(class_name)}
        , m_object_id{std::move(object_id)}
    {
    }

    bool eval(const InstrInfo& instr) const noexcept override
    {
        return (ModifiesClassExpr::eval(instr) && (instr.object_id != sync::instr::PrimaryKey{mpark::monostate{}}) &&
                instr.object_id == m_object_id);
    }

private:
    const sync::instr::PrimaryKey m_object_id;
};


class ModifiesPropertyExpr : public ModifiesObjectExpr {
public:
    ModifiesPropertyExpr(std::string class_name, sync::instr::PrimaryKey object_id, std::string property) noexcept
        : ModifiesObjectExpr{std::move(class_name), std::move(object_id)}
        , m_property{std::move(property)}
    {
    }

    void reset(const Changeset& changeset) noexcept override final
    {
        ModifiesObjectExpr::reset(changeset);
        m_interned_property = changeset.find_string(m_property);
    }

    bool eval(const InstrInfo& instr) const noexcept override final
    {
        return (ModifiesObjectExpr::eval(instr) && instr.property && instr.property == m_interned_property);
    }

private:
    const std::string m_property;
    InternString m_interned_property;
};


class LinksToObjectExpr : public Expr {
public:
    LinksToObjectExpr(std::string class_name, sync::instr::PrimaryKey object_id) noexcept
        : m_class_name{std::move(class_name)}
        , m_object_id{std::move(object_id)}
    {
    }

    void reset(const Changeset& changeset) noexcept override
    {
        m_interned_class_name = changeset.find_string(m_class_name);
    }

    bool eval(const InstrInfo& instr) const noexcept override
    {
        return (instr.payload && instr.payload->type == Payload::Type::Link &&
                instr.payload->data.link.target_table == m_interned_class_name &&
                instr.payload->data.link.target == m_object_id);
    }

private:
    const std::string m_class_name;
    const sync::instr::PrimaryKey m_object_id;
    InternString m_interned_class_name;
};


class AndExpr : public Expr {
public:
    AndExpr(std::unique_ptr<Expr> left, std::unique_ptr<Expr> right) noexcept
        : m_left{std::move(left)}
        , m_right{std::move(right)}
    {
    }

    void reset(const Changeset& changeset) noexcept override
    {
        m_left->reset(changeset);
        m_right->reset(changeset);
    }

    bool eval(const InstrInfo& instr) const noexcept override
    {
        return (m_left->eval(instr) && m_right->eval(instr));
    }

private:
    const std::unique_ptr<Expr> m_left, m_right;
};


class InstructionMatcher {
public:
    using InternString = sync::InternString;
    using Instruction = sync::Instruction;
    using Payload = Instruction::Payload;

    explicit InstructionMatcher(const Expr& expression) noexcept
        : m_expression{expression}
    {
    }

    bool operator()(Instruction::AddTable& instr) noexcept
    {
        m_selected_class_name = instr.table;
        return modify_class(Instruction::Type::AddTable);
    }

    bool operator()(Instruction::EraseTable& instr) noexcept
    {
        m_selected_class_name = instr.table;
        return modify_class(Instruction::Type::EraseTable);
    }

    bool operator()(Instruction::AddColumn&) noexcept
    {
        return modify_class(Instruction::Type::AddColumn);
    }

    bool operator()(Instruction::EraseColumn&) noexcept
    {
        return modify_class(Instruction::Type::EraseColumn);
    }

    bool operator()(Instruction::CreateObject& instr) noexcept
    {
        return modify_object(Instruction::Type::CreateObject, instr.object);
    }

    bool operator()(Instruction::EraseObject& instr) noexcept
    {
        return modify_object(Instruction::Type::EraseObject, instr.object);
    }

    bool operator()(Instruction::Update& instr) noexcept
    {
        return modify_object(Instruction::Type::Update, instr.object);
    }

    bool operator()(Instruction::AddInteger&) noexcept
    {
        return modify_property(Instruction::Type::AddInteger);
    }

    bool operator()(Instruction::ArrayInsert& instr) noexcept
    {
        return modify_object(Instruction::Type::ArrayInsert, instr.object);
    }

    bool operator()(Instruction::ArrayMove&) noexcept
    {
        return modify_property(Instruction::Type::ArrayMove);
    }

    bool operator()(Instruction::ArrayErase&) noexcept
    {
        return modify_property(Instruction::Type::ArrayErase);
    }

    bool operator()(Instruction::Clear&) noexcept
    {
        return modify_property(Instruction::Type::Clear);
    }

    bool operator()(Instruction::SetInsert&) noexcept
    {
        return modify_property(Instruction::Type::SetInsert);
    }

    bool operator()(Instruction::SetErase&) noexcept
    {
        return modify_property(Instruction::Type::SetErase);
    }

private:
    const Expr& m_expression;
    InternString m_selected_class_name;
    sync::instr::PrimaryKey m_selected_object_id;
    InternString m_selected_property;

    bool modify_class(Instruction::Type instruction_type) const noexcept
    {
        GlobalKey object_id;
        return modify_object(instruction_type, object_id);
    }

    bool modify_object(Instruction::Type instruction_type, sync::instr::PrimaryKey object_id,
                       const Payload* payload = nullptr) const noexcept
    {
        InternString property;
        Expr::InstrInfo info = {instruction_type, m_selected_class_name, object_id, property, payload};
        return m_expression.eval(info);
    }

    bool modify_property(Instruction::Type instruction_type, const Payload* payload = nullptr) const noexcept
    {
        Expr::InstrInfo info = {instruction_type, m_selected_class_name, m_selected_object_id, m_selected_property,
                                payload};
        return m_expression.eval(info);
    }
};


class SyncHistoryCursor {
public:
    virtual ~SyncHistoryCursor() = default;
    virtual bool next() = 0;
    virtual version_type get_version() const = 0;
    virtual file_ident_type get_origin_file() const = 0;
    virtual timestamp_type get_origin_timestamp() const = 0;
    virtual void print_info(std::ostream&) const = 0;
    virtual void print_annotated_info(std::ostream&, TimestampFormatter&) const = 0;
    virtual void get_changeset(util::AppendBuffer<char>&) const = 0;
};


class ClientFilesCursor {
public:
    virtual ~ClientFilesCursor() = default;
    virtual bool next() = 0;
    virtual LogicalClientType get_logical_client_type() const = 0;
    virtual ClientType get_client_type() const = 0;
    virtual std::time_t get_last_seen_timestamp() const = 0;
    virtual version_type get_locked_version() const = 0;
    virtual void print_annotated_info(std::ostream&, TimestampFormatter&) const = 0;
};


class CursorFactory {
public:
    using HistoryCursorPtr = std::unique_ptr<SyncHistoryCursor>;
    using ClientFilesCursorPtr = std::unique_ptr<ClientFilesCursor>;

    virtual ~CursorFactory() = default;

    virtual HistoryCursorPtr create_history_cursor(util::Optional<file_ident_type> reciprocal) = 0;
    virtual HistoryCursorPtr create_history_cursor(util::Optional<file_ident_type> reciprocal,
                                                   version_type version) = 0;
    virtual HistoryCursorPtr create_history_cursor(util::Optional<file_ident_type> reciprocal,
                                                   version_type begin_version, version_type end_version) = 0;

    virtual ClientFilesCursorPtr create_client_files_cursor() = 0;
    virtual ClientFilesCursorPtr create_client_files_cursor(file_ident_type client_file) = 0;
};


class RegularSyncHistoryCursor : public SyncHistoryCursor {
public:
    virtual bool reciprocal(file_ident_type recip_file_ident) = 0;

    void init()
    {
        m_begin_version = m_base_version;
        m_end_version = m_last_version;
        m_curr_version = m_begin_version;
    }

    bool init(version_type version)
    {
        if (version <= m_base_version || version > m_last_version) {
            std::cerr << "ERROR: Specified version is out of range\n"; // Throws
            return false;
        }
        m_begin_version = version_type(version - 1);
        m_end_version = version;
        m_curr_version = m_begin_version;
        return true;
    }

    bool init(version_type begin_version, version_type end_version)
    {
        if (begin_version < m_base_version || begin_version > m_last_version) {
            std::cerr << "ERROR: Specified begin version is out of range\n"; // Throws
            return false;
        }
        if (end_version < begin_version || end_version > m_last_version) {
            std::cerr << "ERROR: Specified end version is out of range\n"; // Throws
            return false;
        }
        m_begin_version = begin_version;
        m_end_version = end_version;
        m_curr_version = m_begin_version;
        return true;
    }

    bool next() override final
    {
        if (m_curr_version < m_end_version) {
            ++m_curr_version;
            return true;
        }
        return false;
    }

    version_type get_version() const override final
    {
        get_history_entry_index(); // Throws
        return m_curr_version;
    }

protected:
    version_type m_base_version = 0;
    version_type m_last_version = 0;

    std::size_t get_history_entry_index() const
    {
        if (m_curr_version > m_begin_version)
            return std::size_t(m_curr_version - m_base_version - 1);
        throw std::runtime_error("Bad cursor state");
    }

    version_type get_current_version() const noexcept
    {
        return m_curr_version;
    }

private:
    version_type m_begin_version = 0;
    version_type m_end_version = 0;
    version_type m_curr_version = 0;
};


class RegularClientFilesCursor : public ClientFilesCursor {
public:
    void init()
    {
        m_begin = 0;
        m_end = m_size;
        m_next = 0;
    }

    bool init(file_ident_type client_file)
    {
        std::size_t client_file_index = std::size_t(client_file);
        if (client_file_index >= m_size) {
            std::cerr << "ERROR: Specified client file identifier is out of range\n"; // Throws
            return false;
        }
        m_begin = client_file_index;
        m_end = std::size_t(client_file_index + 1);
        m_next = client_file_index;
        return true;
    }

    bool next() override final
    {
        if (m_next < m_end) {
            ++m_next;
            return true;
        }
        return false;
    }

protected:
    std::size_t m_size = 0;

    std::size_t get_client_file_index() const
    {
        if (m_next > m_begin)
            return std::size_t(m_next - 1);
        throw std::runtime_error("Bad cursor state");
    }

private:
    std::size_t m_begin = 0;
    std::size_t m_end = 0;
    std::size_t m_next = 0;
};


class RegularCursorFactory : public CursorFactory {
public:
    HistoryCursorPtr create_history_cursor(util::Optional<file_ident_type> reciprocal) override
    {
        std::unique_ptr<RegularSyncHistoryCursor> cursor = do_create_history_cursor(); // Throws
        if (REALM_UNLIKELY(!cursor))
            return nullptr;
        if (reciprocal) {
            if (REALM_UNLIKELY(!cursor->reciprocal(*reciprocal))) // Throws
                return nullptr;
        }
        cursor->init(); // Throws
        return cursor;
    }

    HistoryCursorPtr create_history_cursor(util::Optional<file_ident_type> reciprocal, version_type version) override
    {
        std::unique_ptr<RegularSyncHistoryCursor> cursor = do_create_history_cursor(); // Throws
        if (REALM_UNLIKELY(!cursor))
            return nullptr;
        if (reciprocal) {
            if (REALM_UNLIKELY(!cursor->reciprocal(*reciprocal))) // Throws
                return nullptr;
        }
        if (REALM_UNLIKELY(!cursor->init(version))) // Throws
            return nullptr;
        return cursor;
    }

    HistoryCursorPtr create_history_cursor(util::Optional<file_ident_type> reciprocal, version_type begin_version,
                                           version_type end_version) override
    {
        std::unique_ptr<RegularSyncHistoryCursor> cursor = do_create_history_cursor(); // Throws
        if (REALM_UNLIKELY(!cursor))
            return nullptr;
        if (reciprocal) {
            if (REALM_UNLIKELY(!cursor->reciprocal(*reciprocal))) // Throws
                return nullptr;
        }
        if (REALM_UNLIKELY(!cursor->init(begin_version, end_version))) // Throws
            return nullptr;
        return cursor;
    }

    ClientFilesCursorPtr create_client_files_cursor() override
    {
        std::unique_ptr<RegularClientFilesCursor> cursor = do_create_client_files_cursor(); // Throws
        if (REALM_UNLIKELY(!cursor))
            return nullptr;
        cursor->init(); // Throws
        return cursor;
    }

    ClientFilesCursorPtr create_client_files_cursor(file_ident_type client_file) override
    {
        std::unique_ptr<RegularClientFilesCursor> cursor = do_create_client_files_cursor(); // Throws
        if (REALM_UNLIKELY(!cursor))
            return nullptr;
        if (!cursor->init(client_file)) // Throws
            return nullptr;
        return cursor;
    }

protected:
    virtual std::unique_ptr<RegularSyncHistoryCursor> do_create_history_cursor() = 0;
    virtual std::unique_ptr<RegularClientFilesCursor> do_create_client_files_cursor() = 0;
};


class NullSyncHistoryCursor : public RegularSyncHistoryCursor {
public:
    bool reciprocal(file_ident_type) override final
    {
        return false;
    }

    file_ident_type get_origin_file() const override final
    {
        get_history_entry_index(); // Throws
        return 0;
    }

    timestamp_type get_origin_timestamp() const override final
    {
        get_history_entry_index(); // Throws
        return 0;
    }

    void print_info(std::ostream&) const override final
    {
        get_history_entry_index(); // Throws
    }

    void print_annotated_info(std::ostream&, TimestampFormatter&) const override final
    {
        get_history_entry_index(); // Throws
    }

    void get_changeset(util::AppendBuffer<char>&) const override final
    {
        get_history_entry_index(); // Throws
    }
};


class NullClientFilesCursor : public RegularClientFilesCursor {
public:
    LogicalClientType get_logical_client_type() const override final
    {
        get_client_file_index(); // Throws
        return {};
    }

    ClientType get_client_type() const override final
    {
        get_client_file_index(); // Throws
        return {};
    }

    std::time_t get_last_seen_timestamp() const override final
    {
        get_client_file_index(); // Throws
        return 0;
    }

    version_type get_locked_version() const override final
    {
        get_client_file_index(); // Throws
        return 0;
    }

    void print_annotated_info(std::ostream&, TimestampFormatter&) const override final
    {
        get_client_file_index(); // Throws
    }
};


class NullCursorFactory : public RegularCursorFactory {
protected:
    std::unique_ptr<RegularSyncHistoryCursor> do_create_history_cursor() override
    {
        return std::make_unique<NullSyncHistoryCursor>(); // Throws
    }

    std::unique_ptr<RegularClientFilesCursor> do_create_client_files_cursor() override
    {
        return std::make_unique<NullClientFilesCursor>(); // Throws
    }
};


class ClientHistoryCursor_1_to_2 : public RegularSyncHistoryCursor {
public:
    ClientHistoryCursor_1_to_2(Allocator& alloc, ref_type root_ref, int schema_version,
                               version_type current_snapshot_version)
    {
        REALM_ASSERT(schema_version >= 1 && schema_version <= 2);

        if (root_ref == 0)
            return;

        // Size of fixed-size arrays
        std::size_t root_size = 21;
        if (schema_version < 2)
            root_size = 23;

        // Slots in root array of history compartment
        std::size_t changesets_iip = 13;
        std::size_t reciprocal_transforms_iip = 14;
        std::size_t remote_versions_iip = 15;
        std::size_t origin_file_idents_iip = 16;
        std::size_t origin_timestamps_iip = 17;
        if (schema_version < 2) {
            changesets_iip = 0;
            reciprocal_transforms_iip = 1;
            remote_versions_iip = 2;
            origin_file_idents_iip = 3;
            origin_timestamps_iip = 4;
        }

        Array root{alloc};
        root.init_from_ref(root_ref);
        if (root.size() != root_size)
            throw std::runtime_error("Unexpected size of root array of history compartment");
        {
            ref_type ref = root.get_as_ref(changesets_iip);
            m_changesets.reset(new BinaryColumn(alloc)); // Throws
            m_changesets->init_from_ref(ref);
        }
        {
            ref_type ref = root.get_as_ref(reciprocal_transforms_iip);
            m_reciprocal_transforms.reset(new BinaryColumn(alloc)); // Throws
            m_reciprocal_transforms->init_from_ref(ref);
        }
        {
            m_remote_versions.reset(new IntegerBpTree(alloc));         // Throws
            m_remote_versions->set_parent(&root, remote_versions_iip); // Throws
            m_remote_versions->create();
        }
        {
            m_origin_file_idents.reset(new IntegerBpTree(alloc));            // Throws
            m_origin_file_idents->set_parent(&root, origin_file_idents_iip); // Throws
            m_origin_file_idents->create();
        }
        {
            m_origin_timestamps.reset(new IntegerBpTree(alloc));           // Throws
            m_origin_timestamps->set_parent(&root, origin_timestamps_iip); // Throws
            m_origin_timestamps->create();
        }
        std::size_t history_size = m_changesets->size();
        m_base_version = version_type(current_snapshot_version - history_size);
        m_last_version = current_snapshot_version;
    }

    bool reciprocal(file_ident_type recip_file_ident) override final
    {
        if (recip_file_ident != 0) {
            std::cerr << "ERROR: Bad reciprocal file identifier (must be zero)\n"; // Throws
            return false;
        }
        m_reciprocal = true;
        return true;
    }

    file_ident_type get_origin_file() const override final
    {
        std::size_t index = get_history_entry_index(); // Throws
        return file_ident_type(m_origin_file_idents->get(index));
    }

    timestamp_type get_origin_timestamp() const override final
    {
        std::size_t index = get_history_entry_index(); // Throws
        return timestamp_type(m_origin_timestamps->get(index));
    }

    void print_info(std::ostream& out) const override final
    {
        std::size_t index = get_history_entry_index(); // Throws
        version_type client_version = get_current_version();
        file_ident_type origin_file = file_ident_type(m_origin_file_idents->get(index));
        timestamp_type origin_timestamp = timestamp_type(m_origin_timestamps->get(index));
        version_type server_version = version_type(m_remote_versions->get(index));
        std::size_t changeset_size = get_changeset_size(index);
        out << client_version << " " << origin_file << " " << origin_timestamp
            << " "
               ""
            << server_version << " " << changeset_size << "\n"; // Throws
    }

    void print_annotated_info(std::ostream& out, TimestampFormatter& timestamp_formatter) const override final
    {
        std::size_t index = get_history_entry_index(); // Throws
        version_type client_version = get_current_version();
        file_ident_type origin_file = file_ident_type(m_origin_file_idents->get(index));
        const char* origin = (origin_file == 0 ? "local" : "remote");
        timestamp_type origin_timestamp = timestamp_type(m_origin_timestamps->get(index));
        std::time_t time = 0;
        long nanoseconds = 0;
        sync::map_changeset_timestamp(origin_timestamp, time, nanoseconds);
        version_type server_version = version_type(m_remote_versions->get(index));
        std::size_t changeset_size = get_changeset_size(index);
        out << "Produced client version: " << client_version
            << "\n"
               "Identifier of origin file: "
            << origin_file << " (" << origin
            << " origin)\n"
               "Origin timestamp: "
            << origin_timestamp
            << " "
               "("
            << timestamp_formatter.format(time, nanoseconds)
            << ")\n"
               "Last integrated server version: "
            << server_version
            << "\n"
               "Changeset size: "
            << changeset_size << "\n"; // Throws
    }

    void get_changeset(util::AppendBuffer<char>& buffer) const override final
    {
        std::size_t index = get_history_entry_index(); // Throws
        if (m_reciprocal) {
            if (::get_changeset(*m_reciprocal_transforms, index, buffer)) // Throws
                return;
        }
        ::get_changeset(*m_changesets, index, buffer); // Throws
    }

private:
    std::unique_ptr<BinaryColumn> m_changesets;
    std::unique_ptr<BinaryColumn> m_reciprocal_transforms;
    std::unique_ptr<IntegerBpTree> m_remote_versions;
    std::unique_ptr<IntegerBpTree> m_origin_file_idents;
    std::unique_ptr<IntegerBpTree> m_origin_timestamps;
    bool m_reciprocal = false;

    std::size_t get_changeset_size(std::size_t index) const noexcept
    {
        std::size_t size = 0;
        if (m_reciprocal) {
            if (::get_changeset_size(*m_reciprocal_transforms, index, size))
                return size;
        }
        ::get_changeset_size(*m_changesets, index, size);
        return size;
    }
};


class ClientCursorFactory_1_to_2 : public RegularCursorFactory {
public:
    ClientCursorFactory_1_to_2(Allocator& alloc, ref_type root_ref, int schema_version,
                               version_type current_snapshot_version) noexcept
        : m_alloc{alloc}
        , m_root_ref{root_ref}
        , m_schema_version{schema_version}
        , m_current_snapshot_version{current_snapshot_version}
    {
    }

protected:
    std::unique_ptr<RegularSyncHistoryCursor> do_create_history_cursor() override
    {
        return std::make_unique<ClientHistoryCursor_1_to_2>(m_alloc, m_root_ref, m_schema_version,
                                                            m_current_snapshot_version); // Throws
    }

    std::unique_ptr<RegularClientFilesCursor> do_create_client_files_cursor() override
    {
        return std::make_unique<NullClientFilesCursor>(); // Throws
    }

private:
    Allocator& m_alloc;
    const ref_type m_root_ref;
    const int m_schema_version;
    const version_type m_current_snapshot_version;
};


class ServerHistoryCursor_6_to_10 : public RegularSyncHistoryCursor {
public:
    ServerHistoryCursor_6_to_10(Allocator& alloc, ref_type root_ref, int schema_version)
        : m_schema_version{schema_version}
        , m_root{alloc}
    {
        REALM_ASSERT(schema_version >= 6 && schema_version <= 10);

        if (root_ref == 0)
            return;

        // Size of fixed-size arrays
        std::size_t root_size = 11;
        std::size_t sync_history_size = 6;
        if (schema_version < 8)
            root_size = 10;

        // Slots in root array of history compartment
        std::size_t history_base_version_iip = 1;
        std::size_t sync_history_iip = 3;

        // Slots in root array of `sync_history` table
        std::size_t sh_version_salts_iip = 0;
        std::size_t sh_origin_files_iip = 1;
        std::size_t sh_client_versions_iip = 2;
        std::size_t sh_timestamps_iip = 3;
        std::size_t sh_changesets_iip = 4;

        m_root.init_from_ref(root_ref);
        if (m_root.size() != root_size)
            throw std::runtime_error("Unexpected size of root array of history compartment");
        Array sync_history{alloc};
        sync_history.init_from_ref(m_root.get_as_ref_or_tagged(sync_history_iip).get_as_ref());
        if (sync_history.size() != sync_history_size)
            throw std::runtime_error("Unexpected size of root array of `sync_history` table");
        {
            m_version_salts.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = sync_history.get_as_ref(sh_version_salts_iip);
            m_version_salts->init_from_ref(ref); // Throws
        }
        {
            m_origin_files.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = sync_history.get_as_ref(sh_origin_files_iip);
            m_origin_files->init_from_ref(ref); // Throws
        }
        {
            m_client_versions.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = sync_history.get_as_ref(sh_client_versions_iip);
            m_client_versions->init_from_ref(ref); // Throws
        }
        {
            m_timestamps.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = sync_history.get_as_ref(sh_timestamps_iip);
            m_timestamps->init_from_ref(ref); // Throws
        }
        {
            ref_type ref = sync_history.get_as_ref(sh_changesets_iip);
            m_changesets.reset(new BinaryColumn(alloc)); // Throws
            m_changesets->init_from_ref(ref);
        }
        std::size_t history_size = m_version_salts->size();
        REALM_ASSERT(m_origin_files->size() == history_size);
        REALM_ASSERT(m_client_versions->size() == history_size);
        REALM_ASSERT(m_timestamps->size() == history_size);
        REALM_ASSERT(m_changesets->size() == history_size);
        m_base_version = version_type(m_root.get_as_ref_or_tagged(history_base_version_iip).get_as_int());
        m_last_version = version_type(m_base_version + history_size);
    }

    bool reciprocal(file_ident_type recip_file_ident) override final
    {
        // Size of fixed-size arrays
        std::size_t client_files_size = 8;
        if (m_schema_version < 8) {
            client_files_size = 6;
        }
        else if (m_schema_version < 10) {
            client_files_size = 7;
        }

        // Slots in root array of history compartment
        std::size_t client_files_iip = 0;

        // Slots in root array of `client_files` table
        std::size_t cf_rh_base_versions_iip = 2;
        std::size_t cf_recip_hist_refs_iip = 3;

        Allocator& alloc = m_root.get_alloc();
        Array client_files{alloc};
        client_files.init_from_ref(m_root.get_as_ref_or_tagged(client_files_iip).get_as_ref());
        if (client_files.size() != client_files_size)
            throw std::runtime_error("Unexpected size of root array of `client_files` table");

        IntegerBpTree cf_rh_base_versions{alloc};
        {
            ref_type ref = client_files.get_as_ref(cf_rh_base_versions_iip);
            cf_rh_base_versions.init_from_ref(ref); // Throws
        }
        IntegerBpTree cf_recip_hist_refs{alloc};
        {
            ref_type ref = client_files.get_as_ref(cf_recip_hist_refs_iip);
            cf_recip_hist_refs.init_from_ref(ref); // Throws
        }

        std::size_t num_client_files = cf_rh_base_versions.size();
        std::size_t client_file_index = std::size_t(recip_file_ident);
        bool good_recip_file_ident = (recip_file_ident >= 1 && client_file_index < num_client_files);
        if (!good_recip_file_ident) {
            std::cerr << "ERROR: Bad reciprocal file identifier\n"; // Throws
            return false;
        }

        std::size_t recip_hist_size = 0;
        {
            ref_type ref = ref_type(cf_recip_hist_refs.get(client_file_index));
            if (ref != 0) {
                m_recip_hist.reset(new BinaryColumn(alloc)); // Throws
                recip_hist_size = m_recip_hist->size();
                m_recip_hist->init_from_ref(ref);
            }
        }

        version_type recip_hist_base_version = version_type(cf_rh_base_versions.get(client_file_index));
        std::size_t recip_hist_offset = std::size_t(recip_hist_base_version - m_base_version);

        m_base_version = recip_hist_base_version;
        m_recip_hist_offset = recip_hist_offset;
        m_recip_hist_size = recip_hist_size;
        m_reciprocal = true;
        return true;
    }

    file_ident_type get_origin_file() const override final
    {
        std::size_t index_1 = get_history_entry_index(); // Throws
        std::size_t index_2 = get_real_history_index(index_1);
        return file_ident_type(m_origin_files->get(index_2));
    }

    timestamp_type get_origin_timestamp() const override final
    {
        std::size_t index_1 = get_history_entry_index(); // Throws
        std::size_t index_2 = get_real_history_index(index_1);
        return timestamp_type(m_timestamps->get(index_2));
    }

    void print_info(std::ostream& out) const override final
    {
        std::size_t index_1 = get_history_entry_index(); // Throws
        std::size_t index_2 = get_real_history_index(index_1);
        version_type server_version = get_current_version();
        salt_type version_salt = salt_type(m_version_salts->get(index_2));
        file_ident_type origin_file = file_ident_type(m_origin_files->get(index_2));
        timestamp_type origin_timestamp = timestamp_type(m_timestamps->get(index_2));
        version_type client_version = version_type(m_client_versions->get(index_2));
        std::size_t changeset_size = get_changeset_size(index_1);
        out << server_version << " " << version_salt << " " << origin_file
            << " "
               ""
            << origin_timestamp << " " << client_version
            << " "
               ""
            << changeset_size << "\n"; // Throws
    }

    void print_annotated_info(std::ostream& out, TimestampFormatter& timestamp_formatter) const override final
    {
        std::size_t index_1 = get_history_entry_index(); // Throws
        std::size_t index_2 = get_real_history_index(index_1);
        version_type server_version = get_current_version();
        salt_type version_salt = salt_type(m_version_salts->get(index_2));
        file_ident_type origin_file = file_ident_type(m_origin_files->get(index_2));
        const char* origin = (origin_file == 0 ? "local" : "remote");
        timestamp_type origin_timestamp = timestamp_type(m_timestamps->get(index_2));
        std::time_t time = 0;
        long nanoseconds = 0;
        sync::map_changeset_timestamp(origin_timestamp, time, nanoseconds);
        version_type client_version = version_type(m_client_versions->get(index_2));
        std::size_t changeset_size = get_changeset_size(index_1);
        out << "Produced server version: " << server_version
            << "\n"
               "Server version salt: "
            << version_salt
            << "\n"
               "Identifier of origin file: "
            << origin_file << " (" << origin
            << " origin)\n"
               "Origin timestamp: "
            << origin_timestamp
            << " "
               "("
            << timestamp_formatter.format(time, nanoseconds)
            << ")\n"
               "Last integrated client version: "
            << client_version
            << "\n"
               "Changeset size: "
            << changeset_size << "\n"; // Throws
    }

    void get_changeset(util::AppendBuffer<char>& buffer) const override final
    {
        std::size_t index = get_history_entry_index(); // Throws
        if (m_reciprocal) {
            bool coverage = (index < m_recip_hist_size);
            if (coverage && ::get_changeset(*m_recip_hist, index, buffer)) // Throws
                return;
            index = m_recip_hist_offset + index;
        }
        ::get_changeset(*m_changesets, index, buffer); // Throws
    }

private:
    const int m_schema_version;
    Array m_root;
    std::unique_ptr<IntegerBpTree> m_version_salts;
    std::unique_ptr<IntegerBpTree> m_origin_files;
    std::unique_ptr<IntegerBpTree> m_client_versions;
    std::unique_ptr<IntegerBpTree> m_timestamps;
    std::unique_ptr<BinaryColumn> m_changesets;
    std::unique_ptr<BinaryColumn> m_recip_hist;
    std::size_t m_recip_hist_offset = 0;
    std::size_t m_recip_hist_size = 0;
    bool m_reciprocal = false;

    std::size_t get_real_history_index(std::size_t index) const noexcept
    {
        return (m_reciprocal ? m_recip_hist_offset + index : index);
    }

    std::size_t get_changeset_size(std::size_t index) const noexcept
    {
        std::size_t size = 0;
        if (m_reciprocal) {
            bool coverage = (index < m_recip_hist_size);
            if (coverage && ::get_changeset_size(*m_recip_hist, index, size))
                return size;
            index = m_recip_hist_offset + index;
        }
        ::get_changeset_size(*m_changesets, index, size);
        return size;
    }
};


class ServerClientFilesCursor_6_to_10 : public RegularClientFilesCursor {
public:
    ServerClientFilesCursor_6_to_10(Allocator& alloc, ref_type root_ref, int schema_version)
        : m_root{alloc}
    {
        REALM_ASSERT(schema_version >= 6 && schema_version <= 10);

        if (root_ref == 0)
            return;

        // Size of fixed-size arrays
        std::size_t root_size = 11;
        std::size_t client_files_size = 8;
        std::size_t sync_history_size = 6;
        if (schema_version < 8) {
            root_size = 10;
            client_files_size = 6;
        }
        else if (schema_version < 10) {
            client_files_size = 7;
        }

        // Slots in root array of history compartment
        std::size_t client_files_iip = 0;
        std::size_t history_base_version_iip = 1;
        std::size_t sync_history_iip = 3;
        std::size_t upstream_status_iip = 6;
        std::size_t partial_sync_iip = 7;

        // Slots in root array of `client_files` table
        std::size_t cf_ident_salts_iip = 0;
        std::size_t cf_client_versions_iip = 1;
        std::size_t cf_rh_base_versions_iip = 2;
        std::size_t cf_proxy_files_iip = 4;
        std::size_t cf_client_types_iip = 5;
        std::size_t cf_last_seen_timestamps_iip = 6;
        std::size_t cf_locked_server_versions_iip = 7;
        if (schema_version < 10) {
            cf_client_types_iip = std::size_t(-1);
            cf_last_seen_timestamps_iip = 5;
            cf_locked_server_versions_iip = 6;
        }

        // Slots in root array of `sync_history` table
        std::size_t sh_version_salts_iip = 0;

        // Slots in root array of `upstream_status`
        std::size_t us_client_file_ident_iip = 0;

        // Slots in root array of `partial_sync`
        std::size_t ps_partial_file_ident_iip = 0;

        m_root.init_from_ref(root_ref);
        if (m_root.size() != root_size)
            throw std::runtime_error("Unexpected size of root array of history compartment");
        Array client_files{alloc};
        client_files.init_from_ref(m_root.get_as_ref_or_tagged(client_files_iip).get_as_ref());
        if (client_files.size() != client_files_size)
            throw std::runtime_error("Unexpected size of root array of `client_files` table");
        {
            m_ident_salts.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = client_files.get_as_ref(cf_ident_salts_iip);
            m_ident_salts->init_from_ref(ref); // Throws
        }
        {
            m_client_versions.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = client_files.get_as_ref(cf_client_versions_iip);
            m_client_versions->init_from_ref(ref); // Throws
        }
        {
            m_rh_base_versions.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = client_files.get_as_ref(cf_rh_base_versions_iip);
            m_rh_base_versions->init_from_ref(ref); // Throws
        }
        {
            m_proxy_files.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = client_files.get_as_ref(cf_proxy_files_iip);
            m_proxy_files->init_from_ref(ref); // Throws
        }
        if (schema_version >= 10) {
            m_client_types.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = client_files.get_as_ref(cf_client_types_iip);
            m_client_types->init_from_ref(ref); // Throws
        }
        {
            m_last_seen_timestamps.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = client_files.get_as_ref(cf_last_seen_timestamps_iip);
            m_last_seen_timestamps->init_from_ref(ref); // Throws
        }
        if (schema_version >= 8) {
            m_locked_server_versions.reset(new IntegerBpTree(alloc)); // Throws
            ref_type ref = client_files.get_as_ref(cf_locked_server_versions_iip);
            m_locked_server_versions->init_from_ref(ref); // Throws
        }
        m_size = m_ident_salts->size();
        REALM_ASSERT(m_client_versions->size() == m_size);
        REALM_ASSERT(m_rh_base_versions->size() == m_size);
        REALM_ASSERT(m_proxy_files->size() == m_size);
        REALM_ASSERT(!m_client_types || m_client_types->size() == m_size);
        REALM_ASSERT(m_last_seen_timestamps->size() == m_size);
        REALM_ASSERT(!m_locked_server_versions || m_locked_server_versions->size() == m_size);

        {
            Array sync_history{alloc};
            sync_history.init_from_ref(m_root.get_as_ref_or_tagged(sync_history_iip).get_as_ref());
            if (sync_history.size() != sync_history_size)
                throw std::runtime_error("Unexpected size of root array of `sync_history` table");
            IntegerBpTree version_salts{alloc}; // Throws
            ref_type ref = sync_history.get_as_ref(sh_version_salts_iip);
            version_salts.init_from_ref(ref); // Throws
            std::size_t history_size = version_salts.size();
            version_type base_version =
                version_type(m_root.get_as_ref_or_tagged(history_base_version_iip).get_as_int());
            m_last_version = version_type(base_version + history_size);
        }

        // Find the client file entry that corresponds to ourselves
        {
            ref_type upstream_status_ref = m_root.get_as_ref(upstream_status_iip);
            ref_type partial_sync_ref = m_root.get_as_ref(partial_sync_iip);
            REALM_ASSERT_RELEASE(upstream_status_ref == 0 || partial_sync_ref == 0);
            if (upstream_status_ref != 0) {
                Array us{alloc};
                us.init_from_ref(upstream_status_ref);
                if (file_ident_type file_ident = file_ident_type(us.get(us_client_file_ident_iip)))
                    m_self = file_ident;
            }
            else if (partial_sync_ref != 0) {
                Array ps{alloc};
                ps.init_from_ref(partial_sync_ref);
                m_self = file_ident_type(ps.get(ps_partial_file_ident_iip));
            }
        }
    }

    LogicalClientType get_logical_client_type() const override final
    {
        std::size_t index = get_client_file_index(); // Throws
        if (REALM_UNLIKELY(index == 0))
            return LogicalClientType::special;
        if (REALM_UNLIKELY(index == 1)) {
            file_ident_type client_file_ident = file_ident_type(index);
            return (client_file_ident == m_self ? LogicalClientType::self : LogicalClientType::upstream);
        }
        switch (get_client_type(index)) {
            case ClientType::upstream:
                return LogicalClientType::upstream;
            case ClientType::self:
                return LogicalClientType::self;
            case ClientType::indirect:
                return LogicalClientType::indirect;
            case ClientType::legacy:
                return LogicalClientType::legacy;
            case ClientType::regular:
                return LogicalClientType::regular;
            case ClientType::subserver:
                return LogicalClientType::subserver;
        }
        REALM_ASSERT(false);
        return {};
    }

    ClientType get_client_type() const override final
    {
        std::size_t index = get_client_file_index(); // Throws
        return get_client_type(index);
    }

    std::time_t get_last_seen_timestamp() const override final
    {
        std::size_t index = get_client_file_index(); // Throws
        std::int_fast64_t value = m_last_seen_timestamps->get(index);
        return std::time_t(value);
    }

    version_type get_locked_version() const override final
    {
        std::size_t index = get_client_file_index(); // Throws
        std::int_fast64_t value_1 = m_rh_base_versions->get(index);
        std::int_fast64_t value_2 = m_locked_server_versions->get(index);
        return std::min(version_type(value_1), version_type(value_2));
    }

    void print_annotated_info(std::ostream& out, TimestampFormatter& timestamp_formatter) const override final
    {
        std::size_t client_file_index = get_client_file_index();          // Throws
        SaltedFileIdent client_file_ident = get_client_file_ident();      // Throws
        UploadCursor upload_progress = get_upload_progress();             // Throws
        version_type locked_server_version = get_locked_server_version(); // Throws
        file_ident_type proxy_file = get_proxy_file();                    // Throws
        ClientType client_type = get_client_type(client_file_index);
        std::time_t last_seen_timestamp = get_last_seen_timestamp();                                        // Throws
        std::string client_description = describe_client(client_file_ident.ident, client_type, proxy_file); // Throws
        out << "Client file identifier: " << client_file_ident.ident
            << "\n"
               "File identifier salt: "
            << client_file_ident.salt
            << "\n"
               "Last integrated client version: "
            << upload_progress.client_version
            << "\n"
               "Reciprocal history base version: "
               ""
            << upload_progress.last_integrated_server_version
            << "\n"
               "Locked server version: "
            << locked_server_version
            << "\n"
               "Identifier of proxy file: "
            << proxy_file
            << "\n"
               "Client type: "
            << int(client_type) << " (" << client_description
            << ")\n"
               "Last seen timestamp: "
            << last_seen_timestamp; // Throws
        if (_impl::ServerHistory::is_direct_client(client_type)) {
            out << " ";
            bool is_expired = (last_seen_timestamp == 0);
            if (is_expired) {
                out << "(expired)"; // Throws
            }
            else {
                out << "(" << timestamp_formatter.format(last_seen_timestamp, 0) << ")"; // Throws
            }
        }
        out << "\n"; // Throws
    }

private:
    Array m_root;
    std::unique_ptr<IntegerBpTree> m_ident_salts;
    std::unique_ptr<IntegerBpTree> m_client_versions;
    std::unique_ptr<IntegerBpTree> m_rh_base_versions;
    std::unique_ptr<IntegerBpTree> m_proxy_files;
    std::unique_ptr<IntegerBpTree> m_client_types;
    std::unique_ptr<IntegerBpTree> m_last_seen_timestamps;
    std::unique_ptr<IntegerBpTree> m_locked_server_versions;
    version_type m_last_version = 0;
    file_ident_type m_self = 1;

    SaltedFileIdent get_client_file_ident() const
    {
        std::size_t index = get_client_file_index(); // Throws
        file_ident_type file_ident = file_ident_type(index);
        salt_type ident_salt = salt_type(m_ident_salts->get(index));
        return {file_ident, ident_salt};
    }

    UploadCursor get_upload_progress() const
    {
        std::size_t index = get_client_file_index(); // Throws
        version_type client_version = version_type(m_client_versions->get(index));
        version_type rh_base_version = version_type(m_rh_base_versions->get(index));
        return {client_version, rh_base_version};
    }

    version_type get_locked_server_version() const
    {
        std::size_t index = get_client_file_index(); // Throws
        if (m_locked_server_versions)
            return version_type(m_locked_server_versions->get(index));
        return m_last_version;
    }

    file_ident_type get_proxy_file() const
    {
        std::size_t index = get_client_file_index(); // Throws
        return file_ident_type(m_proxy_files->get(index));
    }

    ClientType get_client_type(std::size_t client_file_index) const noexcept
    {
        if (m_client_types)
            return ClientType(m_client_types->get(client_file_index));
        if (client_file_index < 2)
            return ClientType(0);
        if (file_ident_type(client_file_index) == m_self)
            return ClientType::self;
        auto ident_salt = salt_type(m_ident_salts->get(client_file_index));
        if (ident_salt != 0)
            return ClientType::legacy;
        auto proxy_file = file_ident_type(m_proxy_files->get(client_file_index));
        if (proxy_file != 0)
            return ClientType::indirect;
        return ClientType::upstream;
    }

    std::string describe_client(file_ident_type client_file_ident, ClientType client_type,
                                file_ident_type proxy_file) const
    {
        if (client_file_ident == 0) {
            REALM_ASSERT(client_type == ClientType(0));
            return "special"; // Throws
        }
        if (client_file_ident == 1) {
            REALM_ASSERT(client_type == ClientType(0));
            if (client_file_ident == m_self)
                return "self";                             // Throws
            return "root of star topology server cluster"; // Throws
        }
        switch (client_type) {
            case ClientType::upstream:
                break;
            case ClientType::self:
                return "self"; // Throws
            case ClientType::indirect: {
                REALM_ASSERT(proxy_file != 0);
                auto proxy_file_index = std::size_t(proxy_file);
                auto proxy_file_type = get_client_type(proxy_file_index);
                return "client of " + describe_client(proxy_file, proxy_file_type, 0); // Throws
            }
            case ClientType::legacy:
                return "legacy entry"; // Throws
            case ClientType::regular:
                return "regular client"; // Throws
            case ClientType::subserver:
                return "subserver"; // Throws
        }
        return "reachable via upstream server"; // Throws
    }
};


class ServerCursorFactory_6_to_10 : public RegularCursorFactory {
public:
    ServerCursorFactory_6_to_10(Allocator& alloc, ref_type root_ref, int schema_version)
        : m_alloc{alloc}
        , m_root_ref{root_ref}
        , m_schema_version{schema_version}
    {
    }

protected:
    std::unique_ptr<RegularSyncHistoryCursor> do_create_history_cursor() override
    {
        return std::make_unique<ServerHistoryCursor_6_to_10>(m_alloc, m_root_ref,
                                                             m_schema_version); // Throws
    }

    std::unique_ptr<RegularClientFilesCursor> do_create_client_files_cursor() override
    {
        return std::make_unique<ServerClientFilesCursor_6_to_10>(m_alloc, m_root_ref,
                                                                 m_schema_version); // Throws
    }

private:
    Allocator& m_alloc;
    const ref_type m_root_ref;
    const int m_schema_version;
};


void inspect_history(SyncHistoryCursor& cursor, util::Optional<file_ident_type> origin_file, Expr* expression,
                     Format format, Summary summary, bool with_versions, std::ostream& out)
{

    TimestampFormatter::Config timestamp_config;
    timestamp_config.precision = TimestampFormatter::Precision::milliseconds;
    TimestampFormatter timestamp_formatter{timestamp_config};
    util::AppendBuffer<char> buffer;
    std::size_t num_history_entries = 0;
    version_type min_version = std::numeric_limits<version_type>::max();
    version_type max_version = std::numeric_limits<version_type>::min();
    timestamp_type min_timestamp = std::numeric_limits<timestamp_type>::max();
    timestamp_type max_timestamp = std::numeric_limits<timestamp_type>::min();
    while (cursor.next()) {
        version_type version = cursor.get_version(); // Throws
        if (REALM_UNLIKELY(origin_file)) {
            if (REALM_LIKELY(cursor.get_origin_file() != *origin_file))
                continue;
        }
        if (REALM_UNLIKELY(expression)) {
            buffer.clear();
            cursor.get_changeset(buffer); // Throws
            util::SimpleInputStream in{buffer};
            sync::Changeset changeset;
            sync::parse_changeset(in, changeset); // Throws
            expression->reset(changeset);
            InstructionMatcher matcher{*expression};
            bool instr_was_found = false;
            for (auto instr : changeset) {
                REALM_ASSERT(instr);
                bool did_match = instr->visit(matcher);
                if (REALM_LIKELY(!did_match))
                    continue;
                instr_was_found = true;
                break;
            }
            if (REALM_LIKELY(!instr_was_found))
                continue;
        }
        switch (format) {
            case Format::auto_: {
                REALM_ASSERT(false);
                break;
            }
            case Format::nothing: {
                break;
            }
            case Format::version: {
                out << version << "\n"; // Throws
                break;
            }
            case Format::info: {
                cursor.print_info(out); // Throws
                break;
            }
            case Format::annotate: {
                if (num_history_entries > 0)
                    out << "\n";
                cursor.print_annotated_info(out, timestamp_formatter); // Throws
                break;
            }
            case Format::changeset: {
                if (with_versions)
                    out << "# Version " << version << "\n"; // Throws
                buffer.clear();
                cursor.get_changeset(buffer); // Throws
                util::SimpleInputStream in{buffer};
                sync::Changeset changeset;
                sync::parse_changeset(in, changeset); // Throws
#if REALM_DEBUG
                changeset.print(out); // Throws
#else
                REALM_ASSERT(false);
#endif
                break;
            }
            case Format::hexdump: {
                if (with_versions)
                    out << version << " "; // Throws
                buffer.clear();
                cursor.get_changeset(buffer);                                // Throws
                out << util::hex_dump(buffer.data(), buffer.size()) << "\n"; // Throws
                break;
            }
            case Format::raw: {
                buffer.clear();
                cursor.get_changeset(buffer);            // Throws
                out.write(buffer.data(), buffer.size()); // Throws
                break;
            }
        }
        ++num_history_entries;
        if (REALM_UNLIKELY(version < min_version))
            min_version = version;
        if (REALM_UNLIKELY(version > max_version))
            max_version = version;
        if (summary == Summary::full) {
            timestamp_type timestamp = cursor.get_origin_timestamp(); // Throws
            if (REALM_UNLIKELY(timestamp < min_timestamp))
                min_timestamp = timestamp;
            if (REALM_UNLIKELY(timestamp > max_timestamp))
                max_timestamp = timestamp;
        }
    }

    if (format == Format::annotate && summary != Summary::off && num_history_entries > 0)
        out << "\n";

    switch (summary) {
        case Summary::auto_:
            REALM_ASSERT(false);
            break;
        case Summary::off:
            break;
        case Summary::brief:
            out << format_num_history_entries(num_history_entries);
            if (num_history_entries > 0) {
                out << " (version " << (min_version - 1)
                    << " -> "
                       ""
                    << max_version << ")"; // Throws
            }
            out << "\n"; // Throws
            break;
        case Summary::full:
            out << "Number of selected history entries: " << num_history_entries << "\n";
            if (num_history_entries > 0) {
                std::time_t min_time = 0, max_time = 0;
                long min_nanoseconds = 0, max_nanoseconds = 0;
                sync::map_changeset_timestamp(min_timestamp, min_time, min_nanoseconds);
                sync::map_changeset_timestamp(max_timestamp, max_time, max_nanoseconds);
                out << "Version range: " << (min_version - 1)
                    << " -> "
                       ""
                    << max_version
                    << "\n"
                       "Time range: "
                       ""
                    << timestamp_formatter.format(min_time, min_nanoseconds)
                    << " -> "
                       ""
                    << timestamp_formatter.format(max_time, max_nanoseconds)
                    << " "
                       "(unreliable)\n"; // Throws
            }
            break;
    }
}


void inspect_client_files(ClientFilesCursor& cursor, std::ostream& out,
                          const std::set<LogicalClientType>& client_file_types, bool unexpired_client_files,
                          bool expired_client_files, std::time_t min_last_seen_timestamp,
                          std::time_t max_last_seen_timestamp, version_type max_locked_version)
{
    TimestampFormatter timestamp_formatter;
    std::size_t num_client_files = 0;
    std::time_t min_timestamp = std::numeric_limits<std::time_t>::max();
    std::time_t max_timestamp = 0;
    while (cursor.next()) {
        LogicalClientType logical_client_type = cursor.get_logical_client_type(); // Throws
        if (client_file_types.count(logical_client_type) == 0)
            continue;
        std::time_t last_seen_timestamp = cursor.get_last_seen_timestamp(); // Throws
        bool is_unexpired = (last_seen_timestamp > 0);
        ClientType client_type = cursor.get_client_type(); // Throws
        if (_impl::ServerHistory::is_direct_client(client_type)) {
            if (is_unexpired) {
                if (!unexpired_client_files)
                    continue;
                if (last_seen_timestamp < min_last_seen_timestamp)
                    continue;
                if (last_seen_timestamp > max_last_seen_timestamp)
                    continue;
                if (max_locked_version < std::numeric_limits<version_type>::max()) {
                    version_type locked_version = cursor.get_locked_version();
                    if (locked_version > max_locked_version)
                        continue;
                }
            }
            else {
                if (!expired_client_files)
                    continue;
            }
        }
        if (num_client_files > 0)
            out << "\n";
        cursor.print_annotated_info(out, timestamp_formatter); // Throws
        ++num_client_files;
        if (is_unexpired) {
            if (REALM_UNLIKELY(last_seen_timestamp < min_timestamp))
                min_timestamp = last_seen_timestamp;
            if (REALM_UNLIKELY(last_seen_timestamp > max_timestamp))
                max_timestamp = last_seen_timestamp;
        }
    }
    if (num_client_files > 0)
        out << "\n";
    out << "Number of selected client files: " << num_client_files << "\n";
    bool have_timestamp_range = (max_timestamp > 0);
    if (have_timestamp_range) {
        out << "Range of last seen timestamps: "
               ""
            << min_timestamp << " (" << timestamp_formatter.format(min_timestamp, 0)
            << ") -> "
               ""
            << max_timestamp << " (" << timestamp_formatter.format(max_timestamp, 0) << ")\n"; // Throws
    }
}

} // unnamed namespace


int main(int argc, char* argv[])
{
    bool client_files = false;
    int commandline_form = 0;
    std::string realm_path;
    version_type begin_version = 0, end_version = 0;
    FormatEnum format = Format::auto_;
    SummaryEnum summary = Summary::auto_;
    bool with_versions = false;
    util::Optional<file_ident_type> reciprocal;
    util::Optional<file_ident_type> origin_file;
    std::string class_name;
    GlobalKey object_id;
    std::string property;
    std::unique_ptr<Expr> expression;
    file_ident_type client_file = 0;
    std::set<LogicalClientType> client_file_types = {LogicalClientType::regular, LogicalClientType::subserver,
                                                     LogicalClientType::legacy};
    bool unexpired_client_files = true;
    bool expired_client_files = false;
    std::time_t min_last_seen_timestamp = std::numeric_limits<std::time_t>::min();
    std::time_t max_last_seen_timestamp = std::numeric_limits<std::time_t>::max();
    version_type max_locked_version = std::numeric_limits<version_type>::max();
    ;
    std::string encryption_key;

    // Process command-line
    {
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool error = false;
        bool help = false;
        bool version = false;
        int argc_2 = 0;
        int i = 0;
        char* arg = nullptr;
        auto get_string_value = [&](std::string& var) {
            if (i < argc) {
                var = argv[i++];
                return true;
            }
            return false;
        };
        auto get_parsed_value_with_check = [&](auto& var, auto check_val) {
            std::string str_val;
            if (get_string_value(str_val)) {
                std::istringstream in(str_val);
                in.imbue(std::locale::classic());
                in.unsetf(std::ios_base::skipws);
                using value_type = typename std::remove_reference<decltype(var)>::type;
                value_type val = value_type{};
                in >> val;
                using traits = std::istringstream::traits_type;
                if (in && in.peek() == traits::eof() && check_val(val)) {
                    var = val;
                    return true;
                }
            }
            return false;
        };
        auto get_parsed_value = [&](auto& var) {
            return get_parsed_value_with_check(var, [](auto) {
                return true;
            });
        };
        auto add_expr = [&](std::unique_ptr<Expr> e) {
            if (expression)
                e = std::make_unique<AndExpr>(std::move(expression), std::move(e));
            expression = std::move(e);
        };
        while (i < argc) {
            arg = argv[i++];
            if (arg[0] != '-') {
                argv[argc_2++] = arg;
                continue;
            }
            if (std::strcmp(arg, "-c") == 0 || std::strcmp(arg, "--client-files") == 0) {
                if (argc_2 == 1) {
                    client_files = true;
                }
                else {
                    std::cerr << "ERROR: Unexpected command-line argument: " << arg << "\n";
                    error = true;
                }
                continue;
            }
            else if (std::strcmp(arg, "-h") == 0 || std::strcmp(arg, "--help") == 0) {
                help = true;
                continue;
            }
            else if (std::strcmp(arg, "-f") == 0 || std::strcmp(arg, "--format") == 0) {
                if (get_parsed_value(format))
                    continue;
            }
            else if (std::strcmp(arg, "-s") == 0 || std::strcmp(arg, "--summary") == 0) {
                if (get_parsed_value(summary))
                    continue;
            }
            else if (std::strcmp(arg, "-V") == 0 || std::strcmp(arg, "--with-versions") == 0) {
                with_versions = true;
                continue;
            }
            else if (std::strcmp(arg, "-r") == 0 || std::strcmp(arg, "--reciprocal") == 0) {
                file_ident_type value;
                if (get_parsed_value(value)) {
                    reciprocal = value;
                    continue;
                }
            }
            else if (std::strcmp(arg, "-a") == 0 || std::strcmp(arg, "--origin-file") == 0) {
                file_ident_type value;
                if (get_parsed_value(value)) {
                    origin_file = value;
                    continue;
                }
            }
            else if (std::strcmp(arg, "-I") == 0 || std::strcmp(arg, "--instruction-type") == 0) {
                InstructionTypeEnum value;
                if (get_parsed_value(value)) {
                    auto expr = std::make_unique<InstructionTypeExpr>(value);
                    add_expr(std::move(expr));
                    continue;
                }
            }
            else if (std::strcmp(arg, "-C") == 0 || std::strcmp(arg, "--class") == 0) {
                std::string value;
                if (get_string_value(value)) {
                    class_name = std::move(value);
                    continue;
                }
            }
            else if (std::strcmp(arg, "-O") == 0 || std::strcmp(arg, "--object") == 0) {
                GlobalKey value;
                if (get_parsed_value(value)) {
                    object_id = std::move(value);
                    continue;
                }
            }
            else if (std::strcmp(arg, "-P") == 0 || std::strcmp(arg, "--property") == 0) {
                std::string value;
                if (get_string_value(value)) {
                    property = std::move(value);
                    continue;
                }
            }
            else if (std::strcmp(arg, "-m") == 0 || std::strcmp(arg, "--modifies-object") == 0) {
                auto expr = std::make_unique<ModifiesObjectExpr>(class_name, object_id);
                add_expr(std::move(expr));
                continue;
            }
            else if (std::strcmp(arg, "-p") == 0 || std::strcmp(arg, "--modifies-property") == 0) {
                auto expr = std::make_unique<ModifiesPropertyExpr>(class_name, object_id, property);
                add_expr(std::move(expr));
                continue;
            }
            else if (std::strcmp(arg, "-l") == 0 || std::strcmp(arg, "--links-to-object") == 0) {
                auto expr = std::make_unique<LinksToObjectExpr>(class_name, object_id);
                add_expr(std::move(expr));
                continue;
            }
            else if (std::strcmp(arg, "-A") == 0 || std::strcmp(arg, "--all-client-files") == 0) {
                all_client_files(client_file_types);
                unexpired_client_files = true;
                expired_client_files = true;
                continue;
            }
            else if (std::strcmp(arg, "-T") == 0 || std::strcmp(arg, "--client-file-types") == 0) {
                std::string value;
                if (get_string_value(value)) {
                    if (parse_client_types(value, client_file_types))
                        continue;
                }
            }
            else if (std::strcmp(arg, "-E") == 0 || std::strcmp(arg, "--also-expired-client-files") == 0) {
                unexpired_client_files = true;
                expired_client_files = true;
                continue;
            }
            else if (std::strcmp(arg, "-F") == 0 || std::strcmp(arg, "--only-expired-client-files") == 0) {
                unexpired_client_files = false;
                expired_client_files = true;
                continue;
            }
            else if (std::strcmp(arg, "-U") == 0 || std::strcmp(arg, "--only-unexpired-client-files") == 0) {
                unexpired_client_files = true;
                expired_client_files = false;
                continue;
            }
            else if (std::strcmp(arg, "-M") == 0 || std::strcmp(arg, "--min-last-seen-timestamp") == 0) {
                std::time_t value;
                if (get_parsed_value(value)) {
                    min_last_seen_timestamp = value;
                    continue;
                }
            }
            else if (std::strcmp(arg, "-N") == 0 || std::strcmp(arg, "--max-last-seen-timestamp") == 0) {
                std::time_t value;
                if (get_parsed_value(value)) {
                    max_last_seen_timestamp = value;
                    continue;
                }
            }
            else if (std::strcmp(arg, "-L") == 0 || std::strcmp(arg, "--max-locked-version") == 0) {
                version_type value;
                if (get_parsed_value(value)) {
                    max_locked_version = value;
                    continue;
                }
            }
            else if (std::strcmp(arg, "-e") == 0 || std::strcmp(arg, "--encryption-key") == 0) {
                if (get_string_value(encryption_key))
                    continue;
            }
            else if (std::strcmp(arg, "-v") == 0 || std::strcmp(arg, "--version") == 0) {
                version = true;
                continue;
            }
            std::cerr << "ERROR: Bad or missing value for command-line option: " << arg << "\n";
            error = true;
        }
        argc = argc_2;

        i = 0;
        if (!get_string_value(realm_path)) {
            error = true;
        }
        else if (i + 0 == argc) {
            commandline_form = 1;
        }
        else if (i + 1 == argc) {
            commandline_form = 2;
            if (!client_files) {
                if (!get_parsed_value(end_version))
                    error = true;
            }
            else {
                if (!get_parsed_value(client_file))
                    error = true;
            }
        }
        else if (i + 2 == argc && !client_files) {
            commandline_form = 3;
            if (!get_parsed_value(begin_version))
                error = true;
            if (!error && !get_parsed_value(end_version))
                error = true;
        }
        else {
            std::cerr << "ERROR: Too many command-line arguments\n";
            error = true;
        }

        bool bad_combination = (reciprocal && client_files);
        if (bad_combination)
            error = true;

        if (help) {
            // clang-format off
            std::cerr <<
                "Synopsis: "<<prog<<" <realm file>\n"
                "          "<<prog<<" <realm file> <version>\n"
                "          "<<prog<<" <realm file> <begin version> <end version>\n"
                "          "<<prog<<" <realm file> (-c | --client-files)\n"
                "          "<<prog<<" <realm file> (-c | --client-files) <file ident>\n"
                "\n"
                "The first three forms are for inspecting a specific range of the\n"
                "synchronization history of the specified Realm file. In the first form, the\n"
                "range is the entire history. In the second form, the range is the one history\n"
                "entry whose changeset produced the specifed synchronization version. In the\n"
                "third form, the range is as specified.\n"
                "\n"
                "The last two forms are for inspecting the client files registry of a server-\n"
                "side file. In the first of these two forms, information about all registered\n"
                "client files is shown (subject to `--all-client-files`). In the last form,\n"
                "information is shown only for the client file identified by the specified\n"
                "client file identifier.\n"
                "\n"
                "Options:\n"
                "  -h, --help           Display command-line synopsis followed by the list of\n"
                "                       available options.\n"
                "  -f, --format <what>  What to output for each selected history entry. The\n"
                "                       value can be `auto` (default), `nothing`, `version`,\n"
                "                       `info`, `annotate`, `changeset`, `hexdump`, or `raw`.\n"
                "                       When the value is `auto`, the effective value is\n"
                "                       `nothing` in the 1st and 3rd command-line forms, and\n"
                "                       `annotate` in the 2nd command-line form. `annotate`\n"
                "                       shows information that is stored in each history entry,\n"
                "                       but not the changeset itself. `info` shows the same\n"
                "                       information, and in the same order as `annotate`, but\n"
                "                       using only a single line per history entry, and without\n"
                "                       annotations. `version` shows only the synchronization\n"
                "                       version produced by the changeset of each of the\n"
                "                       selected history entries. `hexdump` shows a hex dump of\n"
                "                       the changeset (one line per history entry). `changeset`\n"
                "                       shows the changeset in a human-readable form (only\n"
                "                       available when tool is built in debug mode).\n"
                "  -s, --summary <what>  What to output as a final summary. The value can be\n"
                "                       `auto` (default), `off`, `brief`, or `full`. When the\n"
                "                       value is `auto`, the effective value is `brief` if\n"
                "                       `--format` is effectively `nothing`, `annotate`, or\n"
                "                       `changeset`. Otherwise it is `off`.\n"
                "  -V, --with-versions  When `--format` is `changeset` or `hexdump`, also show\n"
                "                       which version is produced by each of the selected\n"
                "                       changesets.\n"
                "  -r, --reciprocal <file ident>\n"
                "                       Instead of inspecting the main history, inspect instead\n"
                "                       the reciprocal history for the reciprocal file\n"
                "                       identified by <file ident>. With client-side files, this\n"
                "                       must be zero, and the implied reciprocal file is the\n"
                "                       server-side file.\n"
                "  -a, --origin-file <file ident>\n"
                "                       Only include history entries whose changeset originated\n"
                "                       from the file identified by <file ident>.\n"
                "  -I, --instruction-type <type>\n"
                "                       Only include history entries whose changeset contains an\n"
                "                       instruction of the specified type. See header file\n"
                "                       `<realm/sync/instructions.hpp>` for the list of\n"
                "                       instruction types. This acts as an additional\n"
                "                       instruction condition. See `--modifies-object` for more\n"
                "                       on instruction conditions.\n"
                "  -C, --class <name>   The class name that applies when specifying various\n"
                "                       instruction conditions, such as `--modifies-object`.\n"
                "  -O, --object <object ident>\n"
                "                       The object identifier that applies when specifying\n"
                "                       various instruction conditions, such as\n"
                "                       `--modifies-object`. An object identifier is a pair of\n"
                "                       integers in hexadecimal form separated by a hyphen (`-`)\n"
                "                       and enclosed in curly braces. It could be `{5-17A}`, for\n"
                "                       example.\n"
                "  -P, --property <name>  The property name that applies when specifying various\n"
                "                       instruction conditions, such as `--modifies-property`.\n"
                "  -m, --modifies-object  Only include history entries that contain an\n"
                "                       instruction that modifies the object specified by\n"
                "                       `--class` and `--object`. This acts as an additional\n"
                "                       instruction condition. When at least one instruction\n"
                "                       condition is specified (`--instruction-type`,\n"
                "                       `--modifies-object`, `--modifies-property`, or\n"
                "                       `--links-to-object`), a changeset is included only if an\n"
                "                       instruction can be found in that changeset, that\n"
                "                       satisfies all the specified instruction conditions.\n"
                "  -p, --modifies-property\n"
                "                       Only include history entries that contain an instruction\n"
                "                       that modifies the property specified by `--class`,\n"
                "                       `--object`, and `--property`. This acts as an additional\n"
                "                       instruction condition. See `--modifies-object` for more\n"
                "                       on instruction conditions.\n"
                "  -l, --links-to-object  Only include history entries that contain an\n"
                "                       instruction that establishes a link to the object\n"
                "                       specified by `--class` and `--object`. This acts as an\n"
                "                       additional instruction condition. See\n"
                "                       `--modifies-object` for more on instruction conditions.\n"
                "  -A, --all-client-files  Include all types of client file entries. Equivalent\n"
                "                       to passing `rspliuSU` to `--client-file-types` and also\n"
                "                       specifying `--also-expired-client-files`.\n"
                "  -T, --client-file-types <types>\n"
                "                       Specify which types of client file entries to include\n"
                "                       when using the `--client-files` form of this command.\n"
                "                       The argument is a string in which each letter specifies\n"
                "                       that a particular type of client file entries must be\n"
                "                       included. The valid letters are as follows: `r` for\n"
                "                       regular direct clients, `s` for files on direct\n"
                "                       subservers, `p` for direct partial views, `l` for legacy\n"
                "                       entries, `i` for indirect clients (clients of subservers\n"
                "                       or of partial views), `u` for entries reachable via the\n"
                "                       upstream server or via the reference file, `S` for the\n"
                "                       entry representing the file itself, and `U` for the\n"
                "                       special entry used to represent the upstream server,\n"
                "                       when one exists. The default value is `rspl`. This\n"
                "                       option has no effect when a specific client file is\n"
                "                       specified after `--client-files`, i.e., in the 5th form\n"
                "                       shown above. See also `--also-expired-client-files`.\n"
                "  -E, --also-expired-client-files\n"
                "                       Include both expired and unexpired client file entries\n"
                "                       when using the `--client-files` form of this command. By\n"
                "                       default, only unexpired entries are included. The\n"
                "                       expired / unexpired distinction only applies to types of\n"
                "                       entries associated with direct clients (i.e., `r`, `s`,\n"
                "                       `p`, and `l`). See also `--only-expired-client-files`,\n"
                "                       `--only-unexpired-client-files`, and\n"
                "                       `--client-file-types`.\n"
                "  -F, --only-expired-client-files\n"
                "                       Include only expired client file entries when using the\n"
                "                       `--client-files` form of this command. See also\n"
                "                       `--also-expired-client-files`.\n"
                "  -U, --only-unexpired-client-files\n"
                "                       Include only unexpired client file entries when using\n"
                "                       the `--client-files` form of this command. See also\n"
                "                       `--also-expired-client-files`.\n"
                "  -M, --min-last-seen-timestamp <timestamp>\n"
                "                       Only include entries for direct clients whose\n"
                "                       'last seen' timestamp is at least <timestamp> (seconds\n"
                "                       since beginning of UNIX epoch). This applies only to\n"
                "                       unexpired entries associated with direct clients (i.e.,\n"
                "                       `r`, `s`, `p`, and `l`). See also `--client-file-types`.\n"
                "  -N, --max-last-seen-timestamp <timestamp>\n"
                "                       Only include entries for direct clients wose 'last seen'\n"
                "                       timestamp is at most <timestamp>. See also\n"
                "                       `--min-last-seen-timestamp`.\n"
                "  -L, --max-locked-version <version>\n"
                "                       Only include entries for direct clients where either\n"
                "                       `rh_base_version` or `locked_server_version` is less\n"
                "                       than, or equal to `<version>`. Here, `rh_base_version`\n"
                "                       is the base version of the base version of the\n"
                "                       reciprocal history, and `locked_server_version` is as\n"
                "                       explained in the specification of the UPLOAD message.\n"
                "                       This applies only to unexpired entries associated with\n"
                "                       direct clients (i.e., `r`, `s`, `p`, and `l`). To select\n"
                "                       client file entries which are blocking in-place history\n"
                "                       compaction beyond <version> (until <version> + 1) given\n"
                "                       a particular <time to live>, use\n"
                "                       `--min-last-seen-timestamp <timestamp>\n"
                "                       --max-locked-version <version>`, where `<timestamp>` is\n"
                "                       now minus `<time to live>`.\n"
                "  -e, --encryption-key <path>\n"
                "                       Access the Realm file using an encryption key. The\n"
                "                       64-byte encryption key is assumed to be stored in the\n"
                "                       file system at the specified path.\n"
                "  -v, --version        Show the version of the Realm Sync release that this\n"
                "                       command belongs to.\n";
            // clang-format on
            return EXIT_SUCCESS;
        }

        if (version) {
            const char* build_mode;
#if REALM_DEBUG
            build_mode = "Debug";
#else
            build_mode = "Release";
#endif
            std::cerr << "RealmSync/" REALM_VERSION_STRING " (build_mode=" << build_mode << ")\n";
            return EXIT_SUCCESS;
        }

        if (error) {
            std::cerr << "ERROR: Bad command line\n"
                         "Try `"
                      << prog << " --help`\n";
            return EXIT_FAILURE;
        }
    }

    std::unique_ptr<CursorFactory> factory = std::make_unique<NullCursorFactory>(); // Throws

    std::string encryption_key_2;
    const char* encryption_key_3 = nullptr;
    if (!encryption_key.empty()) {
        encryption_key_2 = util::load_file(encryption_key); // Throws
        encryption_key_3 = encryption_key_2.data();
    }
    Group group{realm_path, encryption_key_3}; // Throws
    using gf = _impl::GroupFriend;
    int file_format_version = gf::get_file_format_version(group);
    if (file_format_version != 9) {
        std::cerr << "ERROR: Unexpected file format version "
                     ""
                  << file_format_version << "\n"; // Throws
        return EXIT_FAILURE;
    }
    Allocator& alloc = gf::get_alloc(group);
    ref_type top_ref = gf::get_top_ref(group);
    if (top_ref != 0) {
        Array top{alloc};
        top.init_from_ref(top_ref);
        ref_type history_ref = 0;
        if (top.size() > 7) {
            REALM_ASSERT(top.size() >= 9);
            history_ref = top.get_as_ref(8);
        }
        version_type version;
        int history_type;
        int history_schema_version;
        gf::get_version_and_history_info(alloc, top_ref, version, history_type, history_schema_version);
        if (history_type == Replication::hist_SyncClient) {
            if (history_schema_version >= 1 && history_schema_version <= 2) {
                factory = std::make_unique<ClientCursorFactory_1_to_2>(alloc, history_ref, history_schema_version,
                                                                       version); // Throws
            }
            else {
                std::cerr << "ERROR: Unsupported schema version "
                             "("
                          << history_schema_version
                          << ") in client-side history "
                             "compartment\n"; // Throws
                return EXIT_FAILURE;
            }
        }
        else if (history_type == Replication::hist_SyncServer) {
            if (history_schema_version >= 6 && history_schema_version <= 10) {
                factory = std::make_unique<ServerCursorFactory_6_to_10>(alloc, history_ref,
                                                                        history_schema_version); // Throws
            }
            else {
                std::cerr << "ERROR: Unsupported schema version "
                             "("
                          << history_schema_version
                          << ") in server-side history "
                             "compartment\n"; // Throws
                return EXIT_FAILURE;
            }
        }
        else if (history_type != Replication::hist_None) {
            std::cerr << "ERROR: Unsupported schema type (" << history_type
                      << ") in history "
                         "compartment\n"; // Throws
            return EXIT_FAILURE;
        }
    }

    if (!client_files) {
        if (format == Format::auto_) {
            switch (commandline_form) {
                case 1:
                case 3:
                    format = Format::nothing;
                    break;
                case 2:
                    format = Format::annotate;
                    break;
                default:
                    REALM_ASSERT(false);
            }
        }

        if (summary == Summary::auto_) {
            switch (format) {
                case Format::auto_:
                    REALM_ASSERT(false);
                    break;
                case Format::nothing:
                case Format::annotate:
                case Format::changeset:
                    summary = Summary::brief;
                    break;
                case Format::version:
                case Format::info:
                case Format::hexdump:
                case Format::raw:
                    summary = Summary::off;
                    break;
            }
        }

#if !REALM_DEBUG
        if (format == Format::changeset) {
            std::cerr << "ERROR: Changesets can only be rendered in human-readable form when "
                         "this tool is built in debug mode\n"; // Throws
            return EXIT_FAILURE;
        }
#endif

        std::unique_ptr<SyncHistoryCursor> cursor;
        switch (commandline_form) {
            case 1:
                cursor = factory->create_history_cursor(reciprocal); // Throws
                break;
            case 2:
                cursor = factory->create_history_cursor(reciprocal, end_version); // Throws
                break;
            case 3:
                cursor = factory->create_history_cursor(reciprocal, begin_version,
                                                        end_version); // Throws
                break;
            default:
                REALM_ASSERT(false);
        }
        if (!cursor)
            return EXIT_FAILURE;

        inspect_history(*cursor, origin_file, expression.get(), format, summary, with_versions,
                        std::cout); // Throws
    }
    else {
        std::unique_ptr<ClientFilesCursor> cursor;
        switch (commandline_form) {
            case 1:
                cursor = factory->create_client_files_cursor(); // Throws
                break;
            case 2:
                cursor = factory->create_client_files_cursor(client_file); // Throws
                all_client_files(client_file_types);
                unexpired_client_files = true;
                expired_client_files = true;
                break;
            default:
                REALM_ASSERT(false);
        }
        if (!cursor)
            return EXIT_FAILURE;

        inspect_client_files(*cursor, std::cout, client_file_types, unexpired_client_files, expired_client_files,
                             min_last_seen_timestamp, max_last_seen_timestamp, max_locked_version); // Throws
    }
}
