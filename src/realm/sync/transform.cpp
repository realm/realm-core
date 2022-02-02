#include <algorithm>
#include <functional>
#include <utility>
#include <vector>
#include <map>
#include <sstream>
#include <fstream>

#if REALM_DEBUG
#include <iostream> // std::cerr used for debug tracing
#include <mutex>    // std::unique_lock used for debug tracing
#endif              // REALM_DEBUG

#include <realm/util/buffer.hpp>
#include <realm/string_data.hpp>
#include <realm/data_type.hpp>
#include <realm/mixed.hpp>
#include <realm/column_fwd.hpp>
#include <realm/db.hpp>
#include <realm/impl/transact_log.hpp>
#include <realm/replication.hpp>
#include <realm/sync/instructions.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/transform.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/changeset_encoder.hpp>
#include <realm/sync/noinst/changeset_index.hpp>
#include <realm/sync/noinst/protocol_codec.hpp>
#include <realm/util/allocation_metrics.hpp>
#include <realm/util/metered/vector.hpp>
#include <realm/util/metered/map.hpp>
#include <realm/util/logger.hpp>

namespace realm {

namespace {

#if REALM_DEBUG
#if defined(_MSC_VER)
#define TERM_RED ""
#define TERM_YELLOW ""
#define TERM_CYAN ""
#define TERM_MAGENTA ""
#define TERM_GREEN ""
#define TERM_BOLD ""
#define TERM_RESET ""
#else
#define TERM_RED "\x1b[31;22m"
#define TERM_YELLOW "\x1b[33;22m"
#define TERM_CYAN "\x1b[36;22m"
#define TERM_MAGENTA "\x1b[35;22m"
#define TERM_GREEN "\x1b[32;22m"
#define TERM_BOLD "\x1b[1m"
#define TERM_RESET "\x1b[39;49;22m"
#endif
#endif

#define REALM_MERGE_ASSERT(condition)                                                                                \
    (REALM_LIKELY(condition) ? static_cast<void>(0) : throw sync::TransformError{"Assertion failed: " #condition})


const util::AllocationMetricName g_transform_metric_scope{"transform"};

} // unnamed namespace

using namespace realm;
using namespace realm::sync;
using namespace realm::util;

namespace _impl {

struct TransformerImpl::Discriminant {
    timestamp_type timestamp;
    file_ident_type client_file_ident;
    Discriminant(timestamp_type t, file_ident_type p)
        : timestamp(t)
        , client_file_ident(p)
    {
    }

    Discriminant(const Discriminant&) = default;
    Discriminant& operator=(const Discriminant&) = default;

    bool operator<(const Discriminant& other) const
    {
        return timestamp == other.timestamp ? (client_file_ident < other.client_file_ident)
                                            : timestamp < other.timestamp;
    }

    bool operator==(const Discriminant& other) const
    {
        return timestamp == other.timestamp && client_file_ident == other.client_file_ident;
    }
    bool operator!=(const Discriminant& other) const
    {
        return !((*this) == other);
    }
};

struct TransformerImpl::Side {
    Transformer& m_transformer;
    Changeset* m_changeset = nullptr;
    Discriminant m_discriminant;

    bool was_discarded = false;
    bool was_replaced = false;
    size_t m_path_len = 0;

    Side(Transformer& transformer)
        : m_transformer(transformer)
        , m_discriminant(0, 0)
    {
    }

    virtual void skip_tombstones() noexcept = 0;
    virtual void next_instruction() noexcept = 0;
    virtual Instruction& get() noexcept = 0;

    void substitute(const Instruction& instr)
    {
        was_replaced = true;
        get() = instr;
    }

    StringData get_string(StringBufferRange range) const
    {
        // Relying on the transaction parser to only provide valid StringBufferRanges.
        return m_changeset->get_string(range);
    }

    StringData get_string(InternString intern_string) const
    {
        // Rely on the parser having checked the consistency of the interned strings
        return m_changeset->get_string(intern_string);
    }

    InternString intern_string(StringData data) const
    {
        return m_changeset->intern_string(data);
    }

    const Discriminant& timestamp() const
    {
        return m_discriminant;
    }

    InternString adopt_string(const Side& other_side, InternString other_string)
    {
        // FIXME: This needs to change if we choose to compare strings through a
        // mapping of InternStrings.
        StringData string = other_side.get_string(other_string);
        return intern_string(string);
    }

    Instruction::PrimaryKey adopt_key(const Side& other_side, const Instruction::PrimaryKey& other_key)
    {
        if (auto str = mpark::get_if<InternString>(&other_key)) {
            return adopt_string(other_side, *str);
        }
        else {
            // Non-string keys do not need to be adopted.
            return other_key;
        }
    }

    void adopt_path(Instruction::PathInstruction& instr, const Side& other_side,
                    const Instruction::PathInstruction& other)
    {
        instr.table = adopt_string(other_side, other.table);
        instr.object = adopt_key(other_side, other.object);
        instr.field = adopt_string(other_side, other.field);
        instr.path.m_path.clear();
        instr.path.m_path.reserve(other.path.size());
        for (auto& element : other.path.m_path) {
            auto push = util::overload{
                [&](uint32_t index) {
                    instr.path.m_path.push_back(index);
                },
                [&](InternString str) {
                    instr.path.m_path.push_back(adopt_string(other_side, str));
                },
            };
            mpark::visit(push, element);
        }
    }

protected:
    void init_with_instruction(const Instruction& instr) noexcept
    {
        was_discarded = false;
        was_replaced = false;
        m_path_len = instr.path_length();
    }
};

struct TransformerImpl::MajorSide : TransformerImpl::Side {
    MajorSide(Transformer& transformer)
        : Side(transformer)
    {
    }

    void set_next_changeset(Changeset* changeset) noexcept;
    void discard();
    void prepend(Instruction operation);
    template <class InputIterator>
    void prepend(InputIterator begin, InputIterator end);

    void init_with_instruction(Changeset::iterator position) noexcept
    {
        REALM_ASSERT(position >= m_changeset->begin());
        REALM_ASSERT(position != m_changeset->end());
        m_position = position;
        skip_tombstones();
        REALM_ASSERT(position != m_changeset->end());

        m_discriminant = Discriminant{m_changeset->origin_timestamp, m_changeset->origin_file_ident};

        Side::init_with_instruction(get());
    }

    void skip_tombstones() noexcept final
    {
        while (m_position != m_changeset->end() && !*m_position) {
            ++m_position;
        }
    }

    void next_instruction() noexcept final
    {
        REALM_ASSERT(m_position != m_changeset->end());
        do {
            ++m_position;
        } while (m_position != m_changeset->end() && !*m_position);
    }

    Instruction& get() noexcept final
    {
        return **m_position;
    }

    size_t get_object_ids_in_current_instruction(_impl::ChangesetIndex::GlobalID* ids, size_t max_ids)
    {
        return _impl::get_object_ids_in_instruction(*m_changeset, get(), ids, max_ids);
    }

    Changeset::iterator m_position;
};

struct TransformerImpl::MinorSide : TransformerImpl::Side {
    using Position = _impl::ChangesetIndex::RangeIterator;

    MinorSide(Transformer& transformer)
        : Side(transformer)
    {
    }

    void discard();
    void prepend(Instruction operation);
    template <class InputIterator>
    void prepend(InputIterator begin, InputIterator end);

    void substitute(const Instruction& instr)
    {
        was_replaced = true;
        get() = instr;
    }

    Position begin() noexcept
    {
        return Position{m_conflict_ranges};
    }

    Position end() noexcept
    {
        return Position{m_conflict_ranges, Position::end_tag{}};
    }

    void update_changeset_pointer() noexcept
    {
        if (REALM_LIKELY(m_position != end())) {
            m_changeset = m_position.m_outer->first;
        }
        else {
            m_changeset = nullptr;
        }
    }

    void skip_tombstones() noexcept final
    {
        if (m_position != end() && *m_position)
            return;
        skip_tombstones_slow();
    }

    REALM_NOINLINE void skip_tombstones_slow() noexcept
    {
        while (m_position != end() && !*m_position) {
            ++m_position;
        }
        update_changeset_pointer();
    }

    void next_instruction() noexcept final
    {
        REALM_ASSERT(m_position != end());
        ++m_position;
        update_changeset_pointer();
        skip_tombstones();
    }

    Instruction& get() noexcept final
    {
        Instruction* instr = *m_position;
        REALM_ASSERT(instr != nullptr);
        return *instr;
    }

    void init_with_instruction(Position position)
    {
        // REALM_ASSERT(position >= Position(m_conflict_ranges));
        REALM_ASSERT(position != end());
        m_position = position;
        update_changeset_pointer();
        skip_tombstones();
        REALM_ASSERT(position != end());

        m_discriminant = Discriminant{m_changeset->origin_timestamp, m_changeset->origin_file_ident};

        Side::init_with_instruction(get());
    }

    _impl::ChangesetIndex::RangeIterator m_position;
    _impl::ChangesetIndex* m_changeset_index = nullptr;
    _impl::ChangesetIndex::Ranges* m_conflict_ranges = nullptr;
};

#if defined(REALM_DEBUG) // LCOV_EXCL_START Debug utilities

struct TransformerImpl::MergeTracer {
public:
    Side& m_minor;
    Side& m_major;
    const Changeset& m_minor_log;
    const Changeset& m_major_log;
    Instruction minor_before;
    Instruction major_before;

    // field => pair(original_value, change)
    struct Diff {
        std::map<std::string, std::pair<int64_t, int64_t>> numbers;
        std::map<std::string, std::pair<std::string, std::string>> strings;

        bool empty() const noexcept
        {
            return numbers.empty() && strings.empty();
        }
    };

    explicit MergeTracer(Side& minor, Side& major)
        : m_minor(minor)
        , m_major(major)
        , m_minor_log(*minor.m_changeset)
        , m_major_log(*major.m_changeset)
        , minor_before(minor.get())
        , major_before(major.get())
    {
    }

    struct FieldTracer : sync::Changeset::Reflector::Tracer {
        std::string m_name;
        std::map<std::string, std::string, std::less<>> m_fields;

        const Changeset* m_changeset = nullptr;

        void set_changeset(const Changeset* changeset) override
        {
            m_changeset = changeset;
        }

        StringData get_string(InternString str)
        {
            return m_changeset->get_string(str);
        }

        void name(StringData n) override
        {
            m_name = n;
        }

        void path(StringData n, InternString table, const Instruction::PrimaryKey& pk,
                  util::Optional<InternString> field, const Instruction::Path* path) override
        {
            std::stringstream ss;
            m_changeset->print_path(ss, table, pk, field, path);
            m_fields.emplace(n, ss.str());
        }

        void field(StringData n, InternString str) override
        {
            m_fields.emplace(n, get_string(str));
        }

        void field(StringData n, Instruction::Payload::Type type) override
        {
            m_fields.emplace(n, get_type_name(type));
        }

        void field(StringData n, Instruction::AddColumn::CollectionType type) override
        {
            m_fields.emplace(n, get_collection_type(type));
        }

        void field(StringData n, const Instruction::PrimaryKey& key) override
        {
            auto real_key = m_changeset->get_key(key);
            std::stringstream ss;
            ss << format_pk(real_key);
            m_fields.emplace(n, ss.str());
        }

        void field(StringData n, const Instruction::Payload& value) override
        {
            std::stringstream ss;
            m_changeset->print_value(ss, value);
            m_fields.emplace(n, ss.str());
        }

        void field(StringData n, const Instruction::Path& value) override
        {
            std::stringstream ss;
            m_changeset->print_path(ss, value);
            m_fields.emplace(n, ss.str());
        }

        void field(StringData n, uint32_t value) override
        {
            std::stringstream ss;
            ss << value;
            m_fields.emplace(n, ss.str());
        }
    };

    struct PrintDiffTracer : sync::Changeset::Reflector::Tracer {
        std::ostream& m_os;
        const FieldTracer& m_before;
        bool m_first = true;
        const Changeset* m_changeset = nullptr;

        PrintDiffTracer(std::ostream& os, const FieldTracer& before)
            : m_os(os)
            , m_before(before)
        {
        }

        void set_changeset(const Changeset* changeset) override
        {
            m_changeset = changeset;
        }

        StringData get_string(InternString str) const noexcept
        {
            return m_changeset->get_string(str);
        }

        void name(StringData n) override
        {
            m_os << std::left << std::setw(16) << std::string(n);
        }

        void path(StringData n, InternString table, const Instruction::PrimaryKey& pk,
                  util::Optional<InternString> field, const Instruction::Path* path) override
        {
            std::stringstream ss;
            m_changeset->print_path(ss, table, pk, field, path);
            diff_field(n, ss.str());
        }

        void field(StringData n, InternString str) override
        {
            diff_field(n, get_string(str));
        }

        void field(StringData n, Instruction::Payload::Type type) override
        {
            diff_field(n, get_type_name(type));
        }

        void field(StringData n, Instruction::AddColumn::CollectionType type) override
        {
            diff_field(n, get_collection_type(type));
        }

        void field(StringData n, const Instruction::PrimaryKey& value) override
        {
            std::stringstream ss;
            ss << format_pk(m_changeset->get_key(value));
            diff_field(n, ss.str());
        }

        void field(StringData n, const Instruction::Payload& value) override
        {
            std::stringstream ss;
            m_changeset->print_value(ss, value);
            diff_field(n, ss.str());
        }

        void field(StringData n, const Instruction::Path& value) override
        {
            std::stringstream ss;
            m_changeset->print_path(ss, value);
            diff_field(n, ss.str());
        }

        void field(StringData n, uint32_t value) override
        {
            std::stringstream ss;
            ss << value;
            diff_field(n, ss.str());
        }

        void diff_field(StringData name, std::string value)
        {
            std::stringstream ss;
            ss << name << "=";
            auto it = m_before.m_fields.find(name);
            if (it == m_before.m_fields.end() || it->second == value) {
                ss << value;
            }
            else {
                ss << it->second << "->" << value;
            }
            if (!m_first) {
                m_os << ", ";
            }
            m_os << ss.str();
            m_first = false;
        }
    };

    static void print_instr(std::ostream& os, const Instruction& instr, const Changeset& changeset)
    {
        Changeset::Printer printer{os};
        Changeset::Reflector reflector{printer, changeset};
        instr.visit(reflector);
    }

    bool print_diff(std::ostream& os, bool print_unmodified, const Instruction& before, const Changeset& before_log,
                    Side& side) const
    {
        if (side.was_discarded) {
            print_instr(os, before, before_log);
            os << " (DISCARDED)";
        }
        else if (side.was_replaced) {
            print_instr(os, before, before_log);
            os << " (REPLACED)";
        }
        else {
            Instruction after = side.get();
            if (print_unmodified || (before != after)) {
                FieldTracer before_tracer;
                before_tracer.set_changeset(&before_log);
                PrintDiffTracer after_tracer{os, before_tracer};
                Changeset::Reflector before_reflector{before_tracer, *side.m_changeset};
                Changeset::Reflector after_reflector{after_tracer, *side.m_changeset};
                before.visit(before_reflector);
                after.visit(after_reflector); // This prints the diff'ed instruction
            }
            else {
                os << "(=)";
            }
        }
        return true;
    }

    void print_diff(std::ostream& os, bool print_unmodified) const
    {
        bool must_print_minor = m_minor.was_discarded || m_minor.was_replaced;
        if (!must_print_minor) {
            Instruction minor_after = m_minor.get();
            must_print_minor = (minor_before != minor_after);
        }
        bool must_print_major = m_major.was_discarded || m_major.was_replaced;
        if (!must_print_major) {
            Instruction major_after = m_major.get();
            must_print_major = (major_before != major_after);
        }
        bool must_print = (print_unmodified || must_print_minor || must_print_major);
        if (must_print) {
            std::stringstream ss_minor;
            std::stringstream ss_major;

            print_diff(ss_minor, true, minor_before, m_minor_log, m_minor);
            print_diff(ss_major, print_unmodified, major_before, m_major_log, m_major);

            os << std::left << std::setw(80) << ss_minor.str();
            os << ss_major.str() << "\n";
        }
    }

    void pad_or_ellipsis(std::ostream& os, const std::string& str, int width) const
    {
        // FIXME: Does not work with UTF-8.
        if (str.size() > size_t(width)) {
            os << str.substr(0, width - 1) << "~";
        }
        else {
            os << std::left << std::setw(width) << str;
        }
    }
};
#endif // LCOV_EXCL_STOP REALM_DEBUG


struct TransformerImpl::Transformer {
    MajorSide m_major_side;
    MinorSide m_minor_side;
    MinorSide::Position m_minor_end;
    bool m_trace;
    Reporter* const m_reporter;
    long m_num_merges = 0;

    Transformer(bool trace, Reporter* reporter)
        : m_major_side{*this}
        , m_minor_side{*this}
        , m_trace{trace}
        , m_reporter{reporter}
    {
    }

    void report_merge(bool force)
    {
        ++m_num_merges;
        long report_every = 1000000;
        if (REALM_LIKELY(!force && m_num_merges < report_every)) {
            return;
        }
        if (REALM_UNLIKELY(!m_reporter)) {
            return;
        }
        m_reporter->on_changesets_merged(m_num_merges); // Throws
        m_num_merges = 0;
    }

    void transform()
    {
        m_major_side.m_position = m_major_side.m_changeset->begin();
        m_major_side.skip_tombstones();

        while (m_major_side.m_position != m_major_side.m_changeset->end()) {
            m_major_side.init_with_instruction(m_major_side.m_position);

            set_conflict_ranges();
            m_minor_end = m_minor_side.end();
            m_minor_side.m_position = m_minor_side.begin();
            transform_major();

            if (!m_major_side.was_discarded)
                // Discarding the instruction moves to the next one.
                m_major_side.next_instruction();
            m_major_side.skip_tombstones();
        }

        report_merge(true); // Throws
    }

    _impl::ChangesetIndex::Ranges* get_conflict_ranges_for_instruction(const Instruction& instr)
    {
        _impl::ChangesetIndex& index = *m_minor_side.m_changeset_index;

        if (_impl::is_schema_change(instr)) {
            ///
            /// CONFLICT GROUP: Everything touching that class
            ///
            auto ranges = index.get_everything();
            if (!ranges->empty()) {
#if REALM_DEBUG // LCOV_EXCL_START
                if (m_trace) {
                    std::cerr << TERM_RED << "Conflict group: Everything (due to schema change)\n" << TERM_RESET;
                }
#endif // REALM_DEBUG LCOV_EXCL_STOP
            }
            return ranges;
        }
        else {
            ///
            /// CONFLICT GROUP: Everything touching the involved objects,
            /// including schema changes.
            ///
            _impl::ChangesetIndex::GlobalID major_ids[2];
            size_t num_major_ids = m_major_side.get_object_ids_in_current_instruction(major_ids, 2);
            REALM_ASSERT(num_major_ids <= 2);
            REALM_ASSERT(num_major_ids >= 1);
#if REALM_DEBUG // LCOV_EXCL_START
            if (m_trace) {
                std::cerr << TERM_RED << "Conflict group: ";
                if (num_major_ids == 0) {
                    std::cerr << "(nothing - no object references)";
                }
                for (size_t i = 0; i < num_major_ids; ++i) {
                    std::cerr << major_ids[i].table_name << "[" << format_pk(major_ids[i].object_id) << "]";
                    if (i + 1 != num_major_ids)
                        std::cerr << ", ";
                }
                std::cerr << "\n" << TERM_RESET;
            }
#endif // REALM_DEBUG LCOV_EXCL_STOP
            auto ranges = index.get_modifications_for_object(major_ids[0]);
            if (num_major_ids == 2) {
                // Check that the index has correctly joined the ranges for the
                // two object IDs.
                REALM_ASSERT(ranges == index.get_modifications_for_object(major_ids[1]));
            }
            return ranges;
        }
    }

    void set_conflict_ranges()
    {
        const auto& major_instr = m_major_side.get();
        m_minor_side.m_conflict_ranges = get_conflict_ranges_for_instruction(major_instr);
        /* m_minor_side.m_changeset_index->verify(); */
    }

    void set_next_major_changeset(Changeset* changeset) noexcept
    {
        m_major_side.m_changeset = changeset;
        m_major_side.m_position = changeset->begin();
        m_major_side.skip_tombstones();
    }

    void discard_major()
    {
        m_major_side.m_position = m_major_side.m_changeset->erase_stable(m_major_side.m_position);
        m_major_side.was_discarded = true; // This terminates the loop in transform_major();
        m_major_side.m_changeset->set_dirty(true);
    }

    void discard_minor()
    {
        m_minor_side.was_discarded = true;
        m_minor_side.m_position = m_minor_side.m_changeset_index->erase_instruction(m_minor_side.m_position);
        m_minor_side.m_changeset->set_dirty(true);
        m_minor_side.update_changeset_pointer();
    }

    template <class InputIterator>
    void prepend_major(InputIterator instr_begin, InputIterator instr_end)
    {
        REALM_ASSERT(*m_major_side.m_position); // cannot prepend a tombstone
        auto insert_position = m_major_side.m_position;
        m_major_side.m_position = m_major_side.m_changeset->insert_stable(insert_position, instr_begin, instr_end);
        m_major_side.m_changeset->set_dirty(true);
        size_t num_prepended = instr_end - instr_begin;
        transform_prepended_major(num_prepended);
    }

    void prepend_major(Instruction instr)
    {
        const Instruction* begin = &instr;
        const Instruction* end = begin + 1;
        prepend_major(begin, end);
    }

    template <class InputIterator>
    void prepend_minor(InputIterator instr_begin, InputIterator instr_end)
    {
        REALM_ASSERT(*m_minor_side.m_position); // cannot prepend a tombstone
        auto insert_position = m_minor_side.m_position.m_pos;
        m_minor_side.m_position.m_pos =
            m_minor_side.m_changeset->insert_stable(insert_position, instr_begin, instr_end);
        m_minor_side.m_changeset->set_dirty(true);
        size_t num_prepended = instr_end - instr_begin;
        // Go back to the instruction that initiated this prepend
        for (size_t i = 0; i < num_prepended; ++i) {
            ++m_minor_side.m_position;
        }
        REALM_ASSERT(m_minor_end == m_minor_side.end());
    }

    void prepend_minor(Instruction instr)
    {
        const Instruction* begin = &instr;
        const Instruction* end = begin + 1;
        prepend_minor(begin, end);
    }

    void transform_prepended_major(size_t num_prepended)
    {
        auto orig_major_was_discarded = m_major_side.was_discarded;
        auto orig_major_path_len = m_major_side.m_path_len;

        // Reset 'was_discarded', as it should refer to the prepended
        // instructions in the below, not the instruction that instigated the
        // prepend.
        m_major_side.was_discarded = false;
        REALM_ASSERT(m_major_side.m_position != m_major_side.m_changeset->end());

#if defined(REALM_DEBUG) // LCOV_EXCL_START
        if (m_trace) {
            std::cerr << std::setw(80) << " ";
            MergeTracer::print_instr(std::cerr, m_major_side.get(), *m_major_side.m_changeset);
            std::cerr << " (PREPENDED " << num_prepended << ")\n";
        }
#endif // REALM_DEBUG LCOV_EXCL_STOP

        for (size_t i = 0; i < num_prepended; ++i) {
            auto orig_minor_index = m_minor_side.m_position;
            auto orig_minor_was_discarded = m_minor_side.was_discarded;
            auto orig_minor_was_replaced = m_minor_side.was_replaced;
            auto orig_minor_path_len = m_minor_side.m_path_len;

            // Skip the instruction that initiated this prepend.
            if (!m_minor_side.was_discarded) {
                // Discarding an instruction moves to the next.
                m_minor_side.next_instruction();
            }

            REALM_ASSERT(m_major_side.m_position != m_major_side.m_changeset->end());
            m_major_side.init_with_instruction(m_major_side.m_position);
            REALM_ASSERT(!m_major_side.was_discarded);
            REALM_ASSERT(m_major_side.m_position != m_major_side.m_changeset->end());
            transform_major();
            if (!m_major_side.was_discarded) {
                // Discarding an instruction moves to the next.
                m_major_side.next_instruction();
            }
            REALM_ASSERT(m_major_side.m_position != m_major_side.m_changeset->end());

            m_minor_side.m_position = orig_minor_index;
            m_minor_side.was_discarded = orig_minor_was_discarded;
            m_minor_side.was_replaced = orig_minor_was_replaced;
            m_minor_side.m_path_len = orig_minor_path_len;
            m_minor_side.update_changeset_pointer();
        }

#if defined(REALM_DEBUG) // LCOV_EXCL_START
        if (m_trace) {
            std::cerr << TERM_CYAN << "(end transform of prepended major)\n" << TERM_RESET;
        }
#endif // REALM_DEBUG LCOV_EXCL_STOP

        m_major_side.was_discarded = orig_major_was_discarded;
        m_major_side.m_path_len = orig_major_path_len;
    }

    void transform_major()
    {
        m_minor_side.skip_tombstones();

#if defined(REALM_DEBUG) // LCOV_EXCL_START Debug tracing
        const bool print_noop_merges = false;
        bool new_major = true; // print an instruction every time we go to the next major regardless
#endif                         // LCOV_EXCL_STOP REALM_DEBUG

        while (m_minor_side.m_position != m_minor_end) {
            m_minor_side.init_with_instruction(m_minor_side.m_position);

#if defined(REALM_DEBUG) // LCOV_EXCL_START Debug tracing
            if (m_trace) {
                MergeTracer tracer{m_minor_side, m_major_side};
                merge_instructions(m_major_side, m_minor_side);
                if (new_major)
                    std::cerr << TERM_CYAN << "\n(new major round)\n" << TERM_RESET;
                tracer.print_diff(std::cerr, new_major || print_noop_merges);
                new_major = false;
            }
            else {
#endif // LCOV_EXCL_STOP REALM_DEBUG
                merge_instructions(m_major_side, m_minor_side);
#if defined(REALM_DEBUG) // LCOV_EXCL_START
            }
#endif // LCOV_EXCL_STOP REALM_DEBUG

            if (m_major_side.was_discarded)
                break;
            if (!m_minor_side.was_discarded)
                // Discarding an instruction moves to the next one.
                m_minor_side.next_instruction();
            m_minor_side.skip_tombstones();
        }
    }

    void merge_instructions(MajorSide& left, MinorSide& right);
    template <class OuterSide, class InnerSide>
    void merge_nested(OuterSide& outer, InnerSide& inner);
};

void TransformerImpl::MajorSide::set_next_changeset(Changeset* changeset) noexcept
{
    m_transformer.set_next_major_changeset(changeset);
}
void TransformerImpl::MajorSide::discard()
{
    m_transformer.discard_major();
}
void TransformerImpl::MajorSide::prepend(Instruction operation)
{
    m_transformer.prepend_major(std::move(operation));
}
template <class InputIterator>
void TransformerImpl::MajorSide::prepend(InputIterator begin, InputIterator end)
{
    m_transformer.prepend_major(std::move(begin), std::move(end));
}
void TransformerImpl::MinorSide::discard()
{
    m_transformer.discard_minor();
}
void TransformerImpl::MinorSide::prepend(Instruction operation)
{
    m_transformer.prepend_minor(std::move(operation));
}
template <class InputIterator>
void TransformerImpl::MinorSide::prepend(InputIterator begin, InputIterator end)
{
    m_transformer.prepend_minor(std::move(begin), std::move(end));
}
} // namespace _impl

namespace {

template <class LeftInstruction, class RightInstruction, class Enable = void>
struct Merge;
template <class Outer>
struct MergeNested;

struct MergeUtils {
    using TransformerImpl = _impl::TransformerImpl;
    MergeUtils(TransformerImpl::Side& left_side, TransformerImpl::Side& right_side)
        : m_left_side(left_side)
        , m_right_side(right_side)
    {
    }

    // CAUTION: All of these utility functions rely implicitly on the "left" and
    // "right" arguments corresponding to the left and right sides. If the
    // arguments are switched, mayhem ensues.

    bool same_string(InternString left, InternString right) const noexcept
    {
        // FIXME: Optimize string comparison by building a map of InternString values up front.
        return m_left_side.m_changeset->get_string(left) == m_right_side.m_changeset->get_string(right);
    }

    bool same_key(const Instruction::PrimaryKey& left, const Instruction::PrimaryKey& right) const noexcept
    {
        // FIXME: Once we can compare string by InternString map lookups,
        // compare the string components of the keys using that.
        PrimaryKey left_key = m_left_side.m_changeset->get_key(left);
        PrimaryKey right_key = m_right_side.m_changeset->get_key(right);
        return left_key == right_key;
    }

    bool same_payload(const Instruction::Payload& left, const Instruction::Payload& right)
    {
        using Type = Instruction::Payload::Type;

        if (left.type != right.type)
            return false;

        switch (left.type) {
            case Type::Null:
                return true;
            case Type::Erased:
                return true;
            case Type::Dictionary:
                return true;
            case Type::ObjectValue:
                return true;
            case Type::GlobalKey:
                return left.data.key == right.data.key;
            case Type::Int:
                return left.data.integer == right.data.integer;
            case Type::Bool:
                return left.data.boolean == right.data.boolean;
            case Type::String:
                return m_left_side.get_string(left.data.str) == m_right_side.get_string(right.data.str);
            case Type::Binary:
                return m_left_side.get_string(left.data.binary) == m_right_side.get_string(right.data.binary);
            case Type::Timestamp:
                return left.data.timestamp == right.data.timestamp;
            case Type::Float:
                return left.data.fnum == right.data.fnum;
            case Type::Double:
                return left.data.dnum == right.data.dnum;
            case Type::Decimal:
                return left.data.decimal == right.data.decimal;
            case Type::Link: {
                if (!same_key(left.data.link.target, right.data.link.target)) {
                    return false;
                }
                auto left_target = m_left_side.get_string(left.data.link.target_table);
                auto right_target = m_right_side.get_string(right.data.link.target_table);
                return left_target == right_target;
            }
            case Type::ObjectId:
                return left.data.object_id == right.data.object_id;
            case Type::UUID:
                return left.data.uuid == right.data.uuid;
        }

        REALM_MERGE_ASSERT(false && "Invalid payload type in instruction");
    }

    bool same_path_element(const Instruction::Path::Element& left,
                           const Instruction::Path::Element& right) const noexcept
    {
        const auto& pred = util::overload{
            [&](uint32_t lhs, uint32_t rhs) {
                return lhs == rhs;
            },
            [&](InternString lhs, InternString rhs) {
                return same_string(lhs, rhs);
            },
            [&](const auto&, const auto&) {
                // FIXME: Paths contain incompatible element types. Should we raise an
                // error here?
                return false;
            },
        };
        return mpark::visit(pred, left, right);
    }

    bool same_path(const Instruction::Path& left, const Instruction::Path& right) const noexcept
    {
        if (left.size() == right.size()) {
            for (size_t i = 0; i < left.size(); ++i) {
                if (!same_path_element(left[i], right[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    bool same_table(const Instruction::TableInstruction& left,
                    const Instruction::TableInstruction& right) const noexcept
    {
        return same_string(left.table, right.table);
    }

    bool same_object(const Instruction::ObjectInstruction& left,
                     const Instruction::ObjectInstruction& right) const noexcept
    {
        return same_table(left, right) && same_key(left.object, right.object);
    }

    template <class Left, class Right>
    bool same_column(const Left& left, const Right& right) const noexcept
    {
        if constexpr (std::is_convertible_v<const Right&, const Instruction::PathInstruction&>) {
            const Instruction::PathInstruction& rhs = right;
            return same_table(left, right) && same_string(left.field, rhs.field);
        }
        else if constexpr (std::is_convertible_v<const Right&, const Instruction::ObjectInstruction&>) {
            // CreateObject/EraseObject do not have a column built in.
            return false;
        }
        else {
            return same_table(left, right) && same_string(left.field, right.field);
        }
    }

    bool same_field(const Instruction::PathInstruction& left,
                    const Instruction::PathInstruction& right) const noexcept
    {
        return same_object(left, right) && same_string(left.field, right.field);
    }

    bool same_path(const Instruction::PathInstruction& left, const Instruction::PathInstruction& right) const noexcept
    {
        return same_field(left, right) && same_path(left.path, right.path);
    }

    bool same_container(const Instruction::Path& left, const Instruction::Path& right) const noexcept
    {
        // The instructions refer to the same container if the paths have the
        // same length, and elements [0..n-1] are equal (so the last element is
        // disregarded). If the path length is 1, this counts as referring to
        // the same container.
        if (left.size() == right.size()) {
            if (left.size() == 0)
                return true;

            for (size_t i = 0; i < left.size() - 1; ++i) {
                if (!same_path_element(left[i], right[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    bool same_container(const Instruction::PathInstruction& left,
                        const Instruction::PathInstruction& right) const noexcept
    {
        return same_field(left, right) && same_container(left.path, right.path);
    }

    // NOTE: `is_prefix_of()` should only return true if the left is a strictly
    // shorter path than the right, and the entire left path is the initial
    // sequence of the right.

    bool is_prefix_of(const Instruction::AddTable& left, const Instruction::TableInstruction& right) const noexcept
    {
        return same_table(left, right);
    }

    bool is_prefix_of(const Instruction::EraseTable& left, const Instruction::TableInstruction& right) const noexcept
    {
        return same_table(left, right);
    }

    bool is_prefix_of(const Instruction::AddColumn&, const Instruction::TableInstruction&) const noexcept
    {
        // Right side is a schema instruction.
        return false;
    }

    bool is_prefix_of(const Instruction::EraseColumn&, const Instruction::TableInstruction&) const noexcept
    {
        // Right side is a schema instruction.
        return false;
    }

    bool is_prefix_of(const Instruction::AddColumn& left, const Instruction::ObjectInstruction& right) const noexcept
    {
        return same_column(left, right);
    }

    bool is_prefix_of(const Instruction::EraseColumn& left,
                      const Instruction::ObjectInstruction& right) const noexcept
    {
        return same_column(left, right);
    }

    bool is_prefix_of(const Instruction::ObjectInstruction&, const Instruction::TableInstruction&) const noexcept
    {
        // Right side is a schema instruction.
        return false;
    }

    bool is_prefix_of(const Instruction::ObjectInstruction& left,
                      const Instruction::PathInstruction& right) const noexcept
    {
        return same_object(left, right);
    }

    bool is_prefix_of(const Instruction::PathInstruction&, const Instruction::TableInstruction&) const noexcept
    {
        // Path instructions can never be full prefixes of table-level instructions. Note that this also covers
        // ObjectInstructions.
        return false;
    }

    bool is_prefix_of(const Instruction::PathInstruction& left,
                      const Instruction::PathInstruction& right) const noexcept
    {
        if (left.path.size() < right.path.size() && same_field(left, right)) {
            for (size_t i = 0; i < left.path.size(); ++i) {
                if (!same_path_element(left.path[i], right.path[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    // True if the left side is an instruction that touches a container within
    // right's path. Equivalent to is_prefix_of, except the last element (the
    // index) is not considered.
    bool is_container_prefix_of(const Instruction::PathInstruction& left,
                                const Instruction::PathInstruction& right) const
    {
        if (left.path.size() != 0 && left.path.size() < right.path.size() && same_field(left, right)) {
            for (size_t i = 0; i < left.path.size() - 1; ++i) {
                if (!same_path_element(left.path[i], right.path[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    bool is_container_prefix_of(const Instruction::PathInstruction&, const Instruction::TableInstruction&) const
    {
        return false;
    }

    bool value_targets_table(const Instruction::Payload& value,
                             const Instruction::TableInstruction& right) const noexcept
    {
        if (value.type == Instruction::Payload::Type::Link) {
            StringData target_table = m_left_side.get_string(value.data.link.target_table);
            StringData right_table = m_right_side.get_string(right.table);
            return target_table == right_table;
        }
        return false;
    }

    bool value_targets_object(const Instruction::Payload& value,
                              const Instruction::ObjectInstruction& right) const noexcept
    {
        if (value_targets_table(value, right)) {
            return same_key(value.data.link.target, right.object);
        }
        return false;
    }

    bool value_targets_object(const Instruction::Update& left, const Instruction::ObjectInstruction& right) const
    {
        return value_targets_object(left.value, right);
    }

    bool value_targets_object(const Instruction::ArrayInsert& left, const Instruction::ObjectInstruction& right) const
    {
        return value_targets_object(left.value, right);
    }

    // When the left side has a shorter path, get the path component on the
    // right that corresponds to the last component on the left.
    //
    // Note that this will only be used in the context of array indices, because
    // those are the only path components that are modified during OT.
    uint32_t& corresponding_index_in_path(const Instruction::PathInstruction& left,
                                          Instruction::PathInstruction& right) const
    {
        REALM_ASSERT(left.path.size() != 0);
        REALM_ASSERT(left.path.size() < right.path.size());
        REALM_ASSERT(mpark::holds_alternative<uint32_t>(left.path.back()));
        size_t index = left.path.size() - 1;
        if (!mpark::holds_alternative<uint32_t>(right.path[index])) {
            throw TransformError{"Inconsistent paths"};
        }
        return mpark::get<uint32_t>(right.path[index]);
    }

    uint32_t& corresponding_index_in_path(const Instruction::PathInstruction&,
                                          const Instruction::TableInstruction&) const
    {
        // A path instruction can never have a shorter path than something that
        // isn't a PathInstruction.
        REALM_UNREACHABLE();
    }

    void merge_get_vs_move(uint32_t& get_ndx, const uint32_t& move_from_ndx,
                           const uint32_t& move_to_ndx) const noexcept
    {
        if (get_ndx == move_from_ndx) {
            // CONFLICT: Update of a moved element.
            //
            // RESOLUTION: On the left side, use the MOVE operation to transform the
            // UPDATE operation received from the right side.
            get_ndx = move_to_ndx; // --->
        }
        else {
            // Right update vs left remove
            if (get_ndx > move_from_ndx) {
                get_ndx -= 1; // --->
            }
            // Right update vs left insert
            if (get_ndx >= move_to_ndx) {
                get_ndx += 1; // --->
            }
        }
    }

protected:
    TransformerImpl::Side& m_left_side;
    TransformerImpl::Side& m_right_side;
};

template <class LeftInstruction, class RightInstruction>
struct MergeBase : MergeUtils {
    static const Instruction::Type A = Instruction::GetInstructionType<LeftInstruction>::value;
    static const Instruction::Type B = Instruction::GetInstructionType<RightInstruction>::value;
    static_assert(A >= B, "A < B. Please reverse the order of instruction types. :-)");

    MergeBase(TransformerImpl::Side& left_side, TransformerImpl::Side& right_side)
        : MergeUtils(left_side, right_side)
    {
    }
    MergeBase(MergeBase&&) = delete;
};

#define DEFINE_MERGE(A, B)                                                                                           \
    template <>                                                                                                      \
    struct Merge<A, B> {                                                                                             \
        template <class LeftSide, class RightSide>                                                                   \
        struct DoMerge : MergeBase<A, B> {                                                                           \
            A& left;                                                                                                 \
            B& right;                                                                                                \
            LeftSide& left_side;                                                                                     \
            RightSide& right_side;                                                                                   \
            DoMerge(A& left, B& right, LeftSide& left_side, RightSide& right_side)                                   \
                : MergeBase<A, B>(left_side, right_side)                                                             \
                , left(left)                                                                                         \
                , right(right)                                                                                       \
                , left_side(left_side)                                                                               \
                , right_side(right_side)                                                                             \
            {                                                                                                        \
            }                                                                                                        \
            void do_merge();                                                                                         \
        };                                                                                                           \
        template <class LeftSide, class RightSide>                                                                   \
        static inline void merge(A& left, B& right, LeftSide& left_side, RightSide& right_side)                      \
        {                                                                                                            \
            DoMerge<LeftSide, RightSide> do_merge{left, right, left_side, right_side};                               \
            do_merge.do_merge();                                                                                     \
        }                                                                                                            \
    };                                                                                                               \
    template <class LeftSide, class RightSide>                                                                       \
    void Merge<A, B>::DoMerge<LeftSide, RightSide>::do_merge()

#define DEFINE_MERGE_NOOP(A, B)                                                                                      \
    template <>                                                                                                      \
    struct Merge<A, B> {                                                                                             \
        static const Instruction::Type left_type = Instruction::GetInstructionType<A>::value;                        \
        static const Instruction::Type right_type = Instruction::GetInstructionType<B>::value;                       \
        static_assert(left_type >= right_type,                                                                       \
                      "left_type < right_type. Please reverse the order of instruction types. :-)");                 \
        template <class LeftSide, class RightSide>                                                                   \
        static inline void merge(A&, B&, LeftSide&, RightSide&)                                                      \
        { /* Do nothing */                                                                                           \
        }                                                                                                            \
    }


#define DEFINE_NESTED_MERGE(A)                                                                                       \
    template <>                                                                                                      \
    struct MergeNested<A> {                                                                                          \
        template <class B, class OuterSide, class InnerSide>                                                         \
        struct DoMerge : MergeUtils {                                                                                \
            A& outer;                                                                                                \
            B& inner;                                                                                                \
            OuterSide& outer_side;                                                                                   \
            InnerSide& inner_side;                                                                                   \
            DoMerge(A& outer, B& inner, OuterSide& outer_side, InnerSide& inner_side)                                \
                : MergeUtils(outer_side, inner_side)                                                                 \
                , outer(outer)                                                                                       \
                , inner(inner)                                                                                       \
                , outer_side(outer_side)                                                                             \
                , inner_side(inner_side)                                                                             \
            {                                                                                                        \
            }                                                                                                        \
            void do_merge();                                                                                         \
        };                                                                                                           \
        template <class B, class OuterSide, class InnerSide>                                                         \
        static inline void merge(A& outer, B& inner, OuterSide& outer_side, InnerSide& inner_side)                   \
        {                                                                                                            \
            DoMerge<B, OuterSide, InnerSide> do_merge{outer, inner, outer_side, inner_side};                         \
            do_merge.do_merge();                                                                                     \
        }                                                                                                            \
    };                                                                                                               \
    template <class B, class OuterSide, class InnerSide>                                                             \
    void MergeNested<A>::DoMerge<B, OuterSide, InnerSide>::do_merge()

#define DEFINE_NESTED_MERGE_NOOP(A)                                                                                  \
    template <>                                                                                                      \
    struct MergeNested<A> {                                                                                          \
        template <class B, class OuterSide, class InnerSide>                                                         \
        static inline void merge(const A&, const B&, const OuterSide&, const InnerSide&)                             \
        { /* Do nothing */                                                                                           \
        }                                                                                                            \
    }

// Implementation that reverses order.
template <class A, class B>
struct Merge<A, B,
             typename std::enable_if<(Instruction::GetInstructionType<A>::value <
                                      Instruction::GetInstructionType<B>::value)>::type> {
    template <class LeftSide, class RightSide>
    static void merge(A& left, B& right, LeftSide& left_side, RightSide& right_side)
    {
        Merge<B, A>::merge(right, left, right_side, left_side);
    }
};

///
///  GET READY!
///
///  Realm supports 12 instructions at the time of this writing. Each
///  instruction type needs one rule for each other instruction type. We only
///  define one rule to handle each combination (A vs B and B vs A are handle by
///  a single rule).
///
///  This gives (19 * (19 + 1)) / 2 = 78 merge rules below.
///
///  Merge rules are ordered such that the second instruction type is always of
///  a lower enum value than the first.
///
///  Nested merge rules apply when one instruction has a strictly longer path
///  than another. All instructions that have a path of the same length will
///  meet each other through regular merge rules, regardless of whether they
///  share a prefix.
///


/// AddTable rules

DEFINE_NESTED_MERGE_NOOP(Instruction::AddTable);

DEFINE_MERGE(Instruction::AddTable, Instruction::AddTable)
{
    if (same_table(left, right)) {
        StringData left_name = left_side.get_string(left.table);
        if (auto left_spec = mpark::get_if<Instruction::AddTable::PrimaryKeySpec>(&left.type)) {
            if (auto right_spec = mpark::get_if<Instruction::AddTable::PrimaryKeySpec>(&right.type)) {
                StringData left_pk_name = left_side.get_string(left_spec->field);
                StringData right_pk_name = right_side.get_string(right_spec->field);
                if (left_pk_name != right_pk_name) {
                    std::stringstream ss;
                    ss << "Schema mismatch: '" << left_name << "' has primary key '" << left_pk_name
                       << "' on one side,"
                          "but primary key '"
                       << right_pk_name << "' on the other.";
                    throw SchemaMismatchError(ss.str());
                }

                if (left_spec->type != right_spec->type) {
                    std::stringstream ss;
                    ss << "Schema mismatch: '" << left_name << "' has primary key '" << left_pk_name
                       << "', which is of type " << get_type_name(left_spec->type) << " on one side and type "
                       << get_type_name(right_spec->type) << " on the other.";
                    throw SchemaMismatchError(ss.str());
                }

                if (left_spec->nullable != right_spec->nullable) {
                    std::stringstream ss;
                    ss << "Schema mismatch: '" << left_name << "' has primary key '" << left_pk_name
                       << "', which is nullable on one side, but not the other";
                    throw SchemaMismatchError(ss.str());
                }
            }
            else {
                std::stringstream ss;
                ss << "Schema mismatch: '" << left_name << "' has a primary key on one side, but not on the other.";
                throw SchemaMismatchError(ss.str());
            }
        }
        else if (mpark::get_if<Instruction::AddTable::EmbeddedTable>(&left.type)) {
            if (!mpark::get_if<Instruction::AddTable::EmbeddedTable>(&right.type)) {
                std::stringstream ss;
                ss << "Schema mismatch: '" << left_name << "' is an embedded table on one side, but not the other";
                throw SchemaMismatchError(ss.str());
            }
        }

        // Names are the same, PK presence is the same, and if there is a primary
        // key, its name, type, and nullability are the same. Discard both sides.
        left_side.discard();
        right_side.discard();
        return;
    }
}

DEFINE_MERGE(Instruction::EraseTable, Instruction::AddTable)
{
    if (same_table(left, right)) {
        right_side.discard();
    }
}

DEFINE_MERGE_NOOP(Instruction::CreateObject, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::EraseObject, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::Update, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::AddInteger, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::AddColumn, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::EraseColumn, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::ArrayInsert, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::ArrayMove, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::ArrayErase, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::AddTable);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::AddTable);

/// EraseTable rules

DEFINE_NESTED_MERGE(Instruction::EraseTable)
{
    if (is_prefix_of(outer, inner)) {
        inner_side.discard();
    }
}

DEFINE_MERGE(Instruction::EraseTable, Instruction::EraseTable)
{
    if (same_table(left, right)) {
        left_side.discard();
        right_side.discard();
    }
}

// Handled by nesting rule.
DEFINE_MERGE_NOOP(Instruction::CreateObject, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::EraseObject, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::Update, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::AddInteger, Instruction::EraseTable);

DEFINE_MERGE(Instruction::AddColumn, Instruction::EraseTable)
{
    // AddColumn on an erased table handled by nesting.

    if (left.type == Instruction::Payload::Type::Link && same_string(left.link_target_table, right.table)) {
        // Erase of a table where the left side adds a link column targeting it.
        Instruction::EraseColumn erase_column;
        erase_column.table = right_side.adopt_string(left_side, left.table);
        erase_column.field = right_side.adopt_string(left_side, left.field);
        right_side.prepend(erase_column);
        left_side.discard();
    }
}

// Handled by nested rule.
DEFINE_MERGE_NOOP(Instruction::EraseColumn, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::ArrayInsert, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::ArrayMove, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::ArrayErase, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::EraseTable);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::EraseTable);


/// CreateObject rules

// CreateObject cannot interfere with instructions that have a longer path.
DEFINE_NESTED_MERGE_NOOP(Instruction::CreateObject);

// CreateObject is idempotent.
DEFINE_MERGE_NOOP(Instruction::CreateObject, Instruction::CreateObject);

DEFINE_MERGE(Instruction::EraseObject, Instruction::CreateObject)
{
    if (same_object(left, right)) {
        // CONFLICT: Create and Erase of the same object.
        //
        // RESOLUTION: Erase always wins.
        right_side.discard();
    }
}

DEFINE_MERGE_NOOP(Instruction::Update, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::AddInteger, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::AddColumn, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::EraseColumn, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::ArrayInsert, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::ArrayMove, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::ArrayErase, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::CreateObject);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::CreateObject);


/// Erase rules

DEFINE_NESTED_MERGE(Instruction::EraseObject)
{
    if (is_prefix_of(outer, inner)) {
        // Erase always wins.
        inner_side.discard();
    }
}

DEFINE_MERGE(Instruction::EraseObject, Instruction::EraseObject)
{
    if (same_object(left, right)) {
        // We keep the most recent erase. This prevents the situation where a
        // high number of EraseObject instructions in the past trumps a
        // Erase-Create pair in the future.
        if (right_side.timestamp() < left_side.timestamp()) {
            right_side.discard();
        }
        else {
            left_side.discard();
        }
    }
}

// Handled by nested merge.
DEFINE_MERGE_NOOP(Instruction::Update, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::AddInteger, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::AddColumn, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::EraseColumn, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::ArrayInsert, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::ArrayMove, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::ArrayErase, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::EraseObject);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::EraseObject);


/// Set rules

DEFINE_NESTED_MERGE(Instruction::Update)
{
    using Type = Instruction::Payload::Type;

    if (outer.value.type == Type::ObjectValue || outer.value.type == Type::Dictionary) {
        // Creating an embedded object or a dictionary is an idempotent
        // operation, and should not eliminate updates to the subtree.
        return;
    }

    // Setting a value higher up in the hierarchy overwrites any modification to
    // the inner value, regardless of when this happened.
    if (is_prefix_of(outer, inner)) {
        inner_side.discard();
    }
}

DEFINE_MERGE(Instruction::Update, Instruction::Update)
{
    // The two instructions are at the same level of nesting.

    using Type = Instruction::Payload::Type;

    if (same_path(left, right)) {
        bool left_is_default = false;
        bool right_is_default = false;
        REALM_MERGE_ASSERT(left.is_array_update() == right.is_array_update());

        if (!left.is_array_update()) {
            REALM_MERGE_ASSERT(!right.is_array_update());
            left_is_default = left.is_default;
            right_is_default = right.is_default;
        }
        else {
            REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        }

        if (left.value.type != right.value.type) {
            // Embedded object / dictionary creation should always lose to an
            // Update(value), because these structures are nested, and we need to
            // discard any update inside the structure.
            if (left.value.type == Type::Dictionary || left.value.type == Type::ObjectValue) {
                left_side.discard();
                return;
            }
            else if (right.value.type == Type::Dictionary || right.value.type == Type::ObjectValue) {
                right_side.discard();
                return;
            }
        }

        // CONFLICT: Two updates of the same element.
        //
        // RESOLUTION: Suppress the effect of the UPDATE operation with the lower
        // timestamp. Note that the timestamps can never be equal. This is
        // achieved on both sides by discarding the received UPDATE operation if
        // it has a lower timestamp than the previously applied UPDATE operation.
        if (left_is_default == right_is_default) {
            if (left_side.timestamp() < right_side.timestamp()) {
                left_side.discard(); // --->
            }
            else {
                right_side.discard(); // <---
            }
        }
        else {
            if (left_is_default) {
                left_side.discard();
            }
            else {
                right_side.discard();
            }
        }
    }
}

DEFINE_MERGE(Instruction::AddInteger, Instruction::Update)
{
    // The two instructions are at the same level of nesting.

    if (same_path(left, right)) {
        // CONFLICT: Add vs Set of the same element.
        //
        // RESOLUTION: If the Add was later than the Set, add its value to
        // the payload of the Set instruction. Otherwise, discard it.

        REALM_MERGE_ASSERT(right.value.type == Instruction::Payload::Type::Int || right.value.is_null());

        bool right_is_default = !right.is_array_update() && right.is_default;

        // Note: AddInteger survives SetDefault, regardless of timestamp.
        if (right_side.timestamp() < left_side.timestamp() || right_is_default) {
            if (right.value.is_null()) {
                // The AddInteger happened "after" the Set(null). This becomes a
                // no-op, but if the server later integrates a Set(int) that
                // came-before the AddInteger, it will be taken into account again.
                return;
            }

            // Wrapping add
            uint64_t ua = uint64_t(right.value.data.integer);
            uint64_t ub = uint64_t(left.value);
            right.value.data.integer = int64_t(ua + ub);
        }
        else {
            left_side.discard();
        }
    }
}

DEFINE_MERGE_NOOP(Instruction::AddColumn, Instruction::Update);

DEFINE_MERGE(Instruction::EraseColumn, Instruction::Update)
{
    if (same_column(left, right)) {
        right_side.discard();
    }
}

DEFINE_MERGE(Instruction::ArrayInsert, Instruction::Update)
{
    if (same_container(left, right)) {
        REALM_ASSERT(right.is_array_update());
        REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        REALM_MERGE_ASSERT(left.index() <= left.prior_size);
        REALM_MERGE_ASSERT(right.index() < right.prior_size);
        right.prior_size += 1;
        if (right.index() >= left.index()) {
            right.index() += 1; // --->
        }
    }
}

DEFINE_MERGE(Instruction::ArrayMove, Instruction::Update)
{
    if (same_container(left, right)) {
        REALM_ASSERT(right.is_array_update());

        REALM_MERGE_ASSERT(left.index() < left.prior_size);
        REALM_MERGE_ASSERT(right.index() < right.prior_size);

        // FIXME: This marks both sides as dirty, even when they are unmodified.
        merge_get_vs_move(right.index(), left.index(), left.ndx_2);
    }
}

DEFINE_MERGE(Instruction::ArrayErase, Instruction::Update)
{
    if (same_container(left, right)) {
        REALM_ASSERT(right.is_array_update());
        REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        REALM_MERGE_ASSERT(left.index() < left.prior_size);
        REALM_MERGE_ASSERT(right.index() < right.prior_size);

        // CONFLICT: Update of a removed element.
        //
        // RESOLUTION: Discard the UPDATE operation received on the right side.
        right.prior_size -= 1;

        if (left.index() == right.index()) {
            // CONFLICT: Update of a removed element.
            //
            // RESOLUTION: Discard the UPDATE operation received on the right side.
            right_side.discard();
        }
        else if (right.index() > left.index()) {
            right.index() -= 1;
        }
    }
}

// Handled by nested rule
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::Update);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::Update);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::Update);


/// AddInteger rules

DEFINE_NESTED_MERGE_NOOP(Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::AddInteger, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::AddColumn, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::EraseColumn, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::ArrayInsert, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::ArrayMove, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::ArrayErase, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::AddInteger);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::AddInteger);


/// AddColumn rules

DEFINE_NESTED_MERGE_NOOP(Instruction::AddColumn);

DEFINE_MERGE(Instruction::AddColumn, Instruction::AddColumn)
{
    if (same_column(left, right)) {
        StringData left_name = left_side.get_string(left.field);
        if (left.type != right.type) {
            std::stringstream ss;
            ss << "Schema mismatch: Property '" << left_name << "' in class '" << left_side.get_string(left.table)
               << "' is of type " << get_type_name(left.type) << " on one side and type " << get_type_name(right.type)
               << " on the other.";
            throw SchemaMismatchError(ss.str());
        }

        if (left.nullable != right.nullable) {
            std::stringstream ss;
            ss << "Schema mismatch: Property '" << left_name << "' in class '" << left_side.get_string(left.table)
               << "' is nullable on one side and not on the other.";
            throw SchemaMismatchError(ss.str());
        }

        if (left.collection_type != right.collection_type) {
            auto collection_type_name = [](Instruction::AddColumn::CollectionType type) -> const char* {
                switch (type) {
                    case Instruction::AddColumn::CollectionType::Single:
                        return "single value";
                    case Instruction::AddColumn::CollectionType::List:
                        return "list";
                    case Instruction::AddColumn::CollectionType::Dictionary:
                        return "dictionary";
                    case Instruction::AddColumn::CollectionType::Set:
                        return "set";
                }
                REALM_TERMINATE("");
            };

            std::stringstream ss;
            const char* left_type = collection_type_name(left.collection_type);
            const char* right_type = collection_type_name(right.collection_type);
            ss << "Schema mismatch: Property '" << left_name << "' in class '" << left_side.get_string(left.table)
               << "' is a " << left_type << " on one side, and a " << right_type << " on the other.";
            throw SchemaMismatchError(ss.str());
        }

        if (left.type == Instruction::Payload::Type::Link) {
            StringData left_target = left_side.get_string(left.link_target_table);
            StringData right_target = right_side.get_string(right.link_target_table);
            if (left_target != right_target) {
                std::stringstream ss;
                ss << "Schema mismatch: Link property '" << left_name << "' in class '"
                   << left_side.get_string(left.table) << "' points to class '" << left_target
                   << "' on one side and to '" << right_target << "' on the other.";
                throw SchemaMismatchError(ss.str());
            }
        }

        // Name, type, nullability and link targets match -- discard both
        // sides and proceed.
        left_side.discard();
        right_side.discard();
    }
}

DEFINE_MERGE(Instruction::EraseColumn, Instruction::AddColumn)
{
    if (same_column(left, right)) {
        right_side.discard();
    }
}

DEFINE_MERGE_NOOP(Instruction::ArrayInsert, Instruction::AddColumn);
DEFINE_MERGE_NOOP(Instruction::ArrayMove, Instruction::AddColumn);
DEFINE_MERGE_NOOP(Instruction::ArrayErase, Instruction::AddColumn);
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::AddColumn);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::AddColumn);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::AddColumn);


/// EraseColumn rules

DEFINE_NESTED_MERGE_NOOP(Instruction::EraseColumn);

DEFINE_MERGE(Instruction::EraseColumn, Instruction::EraseColumn)
{
    if (same_column(left, right)) {
        left_side.discard();
        right_side.discard();
    }
}

DEFINE_MERGE_NOOP(Instruction::ArrayInsert, Instruction::EraseColumn);
DEFINE_MERGE_NOOP(Instruction::ArrayMove, Instruction::EraseColumn);
DEFINE_MERGE_NOOP(Instruction::ArrayErase, Instruction::EraseColumn);
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::EraseColumn);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::EraseColumn);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::EraseColumn);

/// ArrayInsert rules

DEFINE_NESTED_MERGE(Instruction::ArrayInsert)
{
    if (is_container_prefix_of(outer, inner)) {
        auto& index = corresponding_index_in_path(outer, inner);
        if (index >= outer.index()) {
            index += 1;
        }
    }
}

DEFINE_MERGE(Instruction::ArrayInsert, Instruction::ArrayInsert)
{
    if (same_container(left, right)) {
        REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        left.prior_size++;
        right.prior_size++;

        if (left.index() > right.index()) {
            left.index() += 1; // --->
        }
        else if (left.index() < right.index()) {
            right.index() += 1; // <---
        }
        else { // left index == right index
            // CONFLICT: Two element insertions at the same position.
            //
            // Resolution: Place the inserted elements in order of increasing
            // timestamp. Note that the timestamps can never be equal.
            if (left_side.timestamp() < right_side.timestamp()) {
                right.index() += 1;
            }
            else {
                left.index() += 1;
            }
        }
    }
}

DEFINE_MERGE(Instruction::ArrayMove, Instruction::ArrayInsert)
{
    if (same_container(left, right)) {
        left.prior_size += 1;

        // Left insertion vs right removal
        if (right.index() > left.index()) {
            right.index() -= 1; // --->
        }
        else {
            left.index() += 1; // <---
        }

        // Left insertion vs left insertion
        if (right.index() < left.ndx_2) {
            left.ndx_2 += 1; // <---
        }
        else if (right.index() > left.ndx_2) {
            right.index() += 1; // --->
        }
        else { // right.index() == left.ndx_2
            // CONFLICT: Insertion and movement to same position.
            //
            // RESOLUTION: Place the two elements in order of increasing
            // timestamp. Note that the timestamps can never be equal.
            if (left_side.timestamp() < right_side.timestamp()) {
                left.ndx_2 += 1; // <---
            }
            else {
                right.index() += 1; // --->
            }
        }
    }
}

DEFINE_MERGE(Instruction::ArrayErase, Instruction::ArrayInsert)
{
    if (same_container(left, right)) {
        REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        REALM_MERGE_ASSERT(left.index() < left.prior_size);
        REALM_MERGE_ASSERT(right.index() <= right.prior_size);

        left.prior_size++;
        right.prior_size--;
        if (right.index() <= left.index()) {
            left.index() += 1; // --->
        }
        else {
            right.index() -= 1; // <---
        }
    }
}

// Handled by nested rules
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::ArrayInsert);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::ArrayInsert);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::ArrayInsert);


/// ArrayMove rules

DEFINE_NESTED_MERGE(Instruction::ArrayMove)
{
    if (is_container_prefix_of(outer, inner)) {
        auto& index = corresponding_index_in_path(outer, inner);
        merge_get_vs_move(outer.index(), index, outer.ndx_2);
    }
}

DEFINE_MERGE(Instruction::ArrayMove, Instruction::ArrayMove)
{
    if (same_container(left, right)) {
        REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        REALM_MERGE_ASSERT(left.index() < left.prior_size);
        REALM_MERGE_ASSERT(right.index() < right.prior_size);
        REALM_MERGE_ASSERT(left.ndx_2 < left.prior_size);
        REALM_MERGE_ASSERT(right.ndx_2 < right.prior_size);

        if (left.index() < right.index()) {
            right.index() -= 1; // <---
        }
        else if (left.index() > right.index()) {
            left.index() -= 1; // --->
        }
        else {
            // CONFLICT: Two movements of same element.
            //
            // RESOLUTION: Respect the MOVE operation associated with the higher
            // timestamp. If the previously applied MOVE operation has the higher
            // timestamp, discard the received MOVE operation, otherwise use the
            // previously applied MOVE operation to transform the received MOVE
            // operation. Note that the timestamps are never equal.
            if (left_side.timestamp() < right_side.timestamp()) {
                right.index() = left.ndx_2; // <---
                left_side.discard();        // --->
                if (right.index() == right.ndx_2) {
                    right_side.discard(); // <---
                }
            }
            else {
                left.index() = right.ndx_2; // --->
                if (left.index() == left.ndx_2) {
                    left_side.discard(); // --->
                }
                right_side.discard(); // <---
            }
            return;
        }

        // Left insertion vs right removal
        if (left.ndx_2 > right.index()) {
            left.ndx_2 -= 1; // --->
        }
        else {
            right.index() += 1; // <---
        }

        // Left removal vs right insertion
        if (left.index() < right.ndx_2) {
            right.ndx_2 -= 1; // <---
        }
        else {
            left.index() += 1; // --->
        }

        // Left insertion vs right insertion
        if (left.ndx_2 < right.ndx_2) {
            right.ndx_2 += 1; // <---
        }
        else if (left.ndx_2 > right.ndx_2) {
            left.ndx_2 += 1; // --->
        }
        else { // left.ndx_2 == right.ndx_2
            // CONFLICT: Two elements moved to the same position
            //
            // RESOLUTION: Place the moved elements in order of increasing
            // timestamp. Note that the timestamps can never be equal.
            if (left_side.timestamp() < right_side.timestamp()) {
                right.ndx_2 += 1; // <---
            }
            else {
                left.ndx_2 += 1; // --->
            }
        }

        if (left.index() == left.ndx_2) {
            left_side.discard(); // --->
        }
        if (right.index() == right.ndx_2) {
            right_side.discard(); // <---
        }
    }
}

DEFINE_MERGE(Instruction::ArrayErase, Instruction::ArrayMove)
{
    if (same_container(left, right)) {
        REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        REALM_MERGE_ASSERT(left.index() < left.prior_size);
        REALM_MERGE_ASSERT(right.index() < right.prior_size);

        right.prior_size -= 1;

        if (left.index() == right.index()) {
            // CONFLICT: Removal of a moved element.
            //
            // RESOLUTION: Discard the received MOVE operation on the left side, and
            // use the previously applied MOVE operation to transform the received
            // REMOVE operation on the right side.
            left.index() = right.ndx_2; // --->
            right_side.discard();       // <---
        }
        else {
            // Left removal vs right removal
            if (left.index() > right.index()) {
                left.index() -= 1; // --->
            }
            else {                  // left.index() < right.index()
                right.index() -= 1; // <---
            }
            // Left removal vs right insertion
            if (left.index() >= right.ndx_2) {
                left.index() += 1; // --->
            }
            else {
                right.ndx_2 -= 1; // <---
            }

            if (right.index() == right.ndx_2) {
                right_side.discard(); // <---
            }
        }
    }
}

// Handled by nested rule.
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::ArrayMove);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::ArrayMove);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::ArrayMove);


/// ArrayErase rules

DEFINE_NESTED_MERGE(Instruction::ArrayErase)
{
    if (is_prefix_of(outer, inner)) {
        // Erase of subtree - inner instruction touches the subtree.
        inner_side.discard();
    }
    else if (is_container_prefix_of(outer, inner)) {
        // Erase of a sibling element in the container - adjust the path.
        auto& index = corresponding_index_in_path(outer, inner);
        if (outer.index() < index) {
            index -= 1;
        }
        else {
            REALM_ASSERT(index != outer.index());
        }
    }
}

DEFINE_MERGE(Instruction::ArrayErase, Instruction::ArrayErase)
{
    if (same_container(left, right)) {
        REALM_MERGE_ASSERT(left.prior_size == right.prior_size);
        REALM_MERGE_ASSERT(left.index() < left.prior_size);
        REALM_MERGE_ASSERT(right.index() < right.prior_size);

        left.prior_size -= 1;
        right.prior_size -= 1;

        if (left.index() > right.index()) {
            left.index() -= 1; // --->
        }
        else if (left.index() < right.index()) {
            right.index() -= 1; // <---
        }
        else { // left.index() == right.index()
            // CONFLICT: Two removals of the same element.
            //
            // RESOLUTION: On each side, discard the received REMOVE operation.
            left_side.discard();  // --->
            right_side.discard(); // <---
        }
    }
}

// Handled by nested rules.
DEFINE_MERGE_NOOP(Instruction::Clear, Instruction::ArrayErase);
DEFINE_MERGE_NOOP(Instruction::SetInsert, Instruction::ArrayErase);
DEFINE_MERGE_NOOP(Instruction::SetErase, Instruction::ArrayErase);


/// Clear rules

DEFINE_NESTED_MERGE(Instruction::Clear)
{
    // Note: Clear instructions do not have an index in their path.
    if (is_prefix_of(outer, inner)) {
        inner_side.discard();
    }
}

DEFINE_MERGE(Instruction::Clear, Instruction::Clear)
{
    if (same_path(left, right)) {
        // CONFLICT: Two clears of the same container.
        //
        // RESOLUTION: Discard the clear with the lower timestamp. This has the
        // effect of preserving insertions that came after the clear from the
        // side that has the higher timestamp.
        if (left_side.timestamp() < right_side.timestamp()) {
            left_side.discard();
        }
        else {
            right_side.discard();
        }
    }
}

DEFINE_MERGE(Instruction::SetInsert, Instruction::Clear)
{
    if (same_path(left, right)) {
        left_side.discard();
    }
}

DEFINE_MERGE(Instruction::SetErase, Instruction::Clear)
{
    if (same_path(left, right)) {
        left_side.discard();
    }
}


/// SetInsert rules

DEFINE_NESTED_MERGE_NOOP(Instruction::SetInsert);

DEFINE_MERGE(Instruction::SetInsert, Instruction::SetInsert)
{
    if (same_path(left, right)) {
        // CONFLICT: Two inserts into the same set.
        //
        // RESOLUTION: If the values are the same, discard the insertion with the lower timestamp. Otherwise,
        // do nothing.
        //
        // NOTE: Set insertion is idempotent. Keeping the instruction with the higher timestamp is necessary
        // because we want to maintain associativity in the case where intermittent erases (as ordered by
        // timestamp) arrive at a later point in time.
        if (same_payload(left.value, right.value)) {
            if (left_side.timestamp() < right_side.timestamp()) {
                left_side.discard();
            }
            else {
                right_side.discard();
            }
        }
    }
}

DEFINE_MERGE(Instruction::SetErase, Instruction::SetInsert)
{
    if (same_path(left, right)) {
        // CONFLICT: Insertion and erase in the same set.
        //
        // RESOLUTION: If the inserted/erased values are the same, discard the instruction with the lower
        // timestamp. Otherwise, do nothing.
        //
        // Note: Set insertion and erase are both idempotent. Keeping the instruction with the higher
        // timestamp is necessary because we want to maintain associativity.
        if (same_payload(left.value, right.value)) {
            if (left_side.timestamp() < right_side.timestamp()) {
                left_side.discard();
            }
            else {
                right_side.discard();
            }
        }
    }
}


/// SetErase rules.

DEFINE_NESTED_MERGE_NOOP(Instruction::SetErase);

DEFINE_MERGE(Instruction::SetErase, Instruction::SetErase)
{
    if (same_path(left, right)) {
        // CONFLICT: Two erases in the same set.
        //
        // RESOLUTION: If the values are the same, discard the instruction with the lower timestamp.
        // Otherwise, do nothing.
        if (left.value == right.value) {
            if (left_side.timestamp() < right_side.timestamp()) {
                left_side.discard();
            }
            else {
                right_side.discard();
            }
        }
    }
}


///
/// END OF MERGE RULES!
///

} // namespace

namespace _impl {
template <class Left, class Right>
void merge_instructions_2(Left& left, Right& right, TransformerImpl::MajorSide& left_side,
                          TransformerImpl::MinorSide& right_side)
{
    Merge<Left, Right>::merge(left, right, left_side, right_side);
}

template <class Outer, class Inner, class OuterSide, class InnerSide>
void merge_nested_2(Outer& outer, Inner& inner, OuterSide& outer_side, InnerSide& inner_side)
{
    MergeNested<Outer>::merge(outer, inner, outer_side, inner_side);
}

void TransformerImpl::Transformer::merge_instructions(MajorSide& their_side, MinorSide& our_side)
{
    report_merge(false); // Throws

    // FIXME: Find a way to avoid heap-copies of the path.
    Instruction their_before = their_side.get();
    Instruction our_before = our_side.get();

    if (their_side.get().get_if<Instruction::Update>()) {
        REALM_ASSERT(their_side.m_path_len > 2);
    }
    if (our_side.get().get_if<Instruction::Update>()) {
        REALM_ASSERT(our_side.m_path_len > 2);
    }
    if (their_side.get().get_if<Instruction::EraseObject>()) {
        REALM_ASSERT(their_side.m_path_len == 2);
    }
    if (our_side.get().get_if<Instruction::EraseObject>()) {
        REALM_ASSERT(our_side.m_path_len == 2);
    }

    // Update selections on the major side (outer loop) according to events on
    // the minor side (inner loop). The selection may only be impacted if the
    // instruction level is lower (i.e. at a higher point in the hierarchy).
    if (our_side.m_path_len < their_side.m_path_len) {
        merge_nested(our_side, their_side);
        if (their_side.was_discarded)
            return;
    }
    else if (our_side.m_path_len > their_side.m_path_len) {
        merge_nested(their_side, our_side);
        if (our_side.was_discarded)
            return;
    }

    if (!their_side.was_discarded && !our_side.was_discarded) {
        // Even if the instructions were nested, we must still perform a regular
        // merge, because link-related instructions contain information from higher
        // levels (both rows, columns, and tables).
        //
        // FIXME: This condition goes away when dangling links are implemented.
        their_side.get().visit([&](auto& their_instruction) {
            our_side.get().visit([&](auto& our_instruction) {
                merge_instructions_2(their_instruction, our_instruction, their_side, our_side);
            });
        });
    }

    // Note: `left` and/or `right` may be dangling at this point due to
    // discard/prepend. However, if they were not discarded, their iterators are
    // required to point to an instruction of the same type.
    if (!their_side.was_discarded && !their_side.was_replaced) {
        const auto& their_after = their_side.get();
        if (!(their_after == their_before)) {
            their_side.m_changeset->set_dirty(true);
        }
    }

    if (!our_side.was_discarded && !our_side.was_replaced) {
        const auto& our_after = our_side.get();
        if (!(our_after == our_before)) {
            our_side.m_changeset->set_dirty(true);
        }
    }
}


template <class OuterSide, class InnerSide>
void TransformerImpl::Transformer::merge_nested(OuterSide& outer_side, InnerSide& inner_side)
{
    outer_side.get().visit([&](auto& outer) {
        inner_side.get().visit([&](auto& inner) {
            merge_nested_2(outer, inner, outer_side, inner_side);
        });
    });
}


TransformerImpl::TransformerImpl()
    : m_changeset_parser() // Throws
{
}

void TransformerImpl::merge_changesets(file_ident_type local_file_ident, Changeset* their_changesets,
                                       size_t their_size, Changeset** our_changesets, size_t our_size,
                                       Reporter* reporter, util::Logger* logger)
{
    REALM_ASSERT(their_size != 0);
    REALM_ASSERT(our_size != 0);
    bool trace = false;
#if REALM_DEBUG && !REALM_UWP
    // FIXME: Not thread-safe (use config parameter instead and confine enviromnent reading to test/test_all.cpp)
    const char* trace_p = ::getenv("UNITTEST_TRACE_TRANSFORM");
    trace = (trace_p && StringData{trace_p} != "no");
    static std::mutex trace_mutex;
    util::Optional<std::unique_lock<std::mutex>> l;
    if (trace) {
        l = std::unique_lock<std::mutex>{trace_mutex};
    }
#endif
    Transformer transformer{trace, reporter};

    _impl::ChangesetIndex their_index;
    size_t their_num_instructions = 0;
    size_t our_num_instructions = 0;

    // Loop through all instructions on both sides and build conflict groups.
    // This causes the index to merge ranges that are connected by instructions
    // on the left side, but which aren't connected on the right side.
    // FIXME: The conflict groups can be persisted as part of the changeset to
    // skip this step in the future.
    for (size_t i = 0; i < their_size; ++i) {
        size_t num_instructions = their_changesets[i].size();
        their_num_instructions += num_instructions;
        if (logger) {
            logger->trace("Scanning incoming changeset [%1/%2] (%3 instructions)", i + 1, their_size,
                          num_instructions);
        }

        their_index.scan_changeset(their_changesets[i]);
    }
    for (size_t i = 0; i < our_size; ++i) {
        Changeset& our_changeset = *our_changesets[i];
        size_t num_instructions = our_changeset.size();
        our_num_instructions += num_instructions;
        if (logger) {
            logger->trace("Scanning local changeset [%1/%2] (%3 instructions)", i + 1, our_size, num_instructions);
        }

        their_index.scan_changeset(our_changeset);
    }

    // Build the index.
    for (size_t i = 0; i < their_size; ++i) {
        if (logger) {
            logger->trace("Indexing incoming changeset [%1/%2] (%3 instructions)", i + 1, their_size,
                          their_changesets[i].size());
        }
        their_index.add_changeset(their_changesets[i]);
    }

    if (logger) {
        logger->debug("Finished changeset indexing (incoming: %1 changeset(s) / %2 instructions, local: %3 "
                      "changeset(s) / %4 instructions, conflict group(s): %5)",
                      their_size, their_num_instructions, our_size, our_num_instructions,
                      their_index.get_num_conflict_groups());
    }

#if REALM_DEBUG // LCOV_EXCL_START
    if (trace) {
        std::cerr << TERM_YELLOW << "\n=> PEER " << std::hex << local_file_ident
                  << " merging "
                     "changeset(s)/from peer(s):\n";
        for (size_t i = 0; i < their_size; ++i) {
            std::cerr << "Changeset version " << std::dec << their_changesets[i].version << " from peer "
                      << their_changesets[i].origin_file_ident << " at timestamp "
                      << their_changesets[i].origin_timestamp << "\n";
        }
        std::cerr << "Transforming through local changeset(s):\n";
        for (size_t i = 0; i < our_size; ++i) {
            std::cerr << "Changeset version " << our_changesets[i]->version << " from peer "
                      << our_changesets[i]->origin_file_ident << " at timestamp "
                      << our_changesets[i]->origin_timestamp << "\n";
        }

        for (size_t i = 0; i < our_size; ++i) {
            std::cerr << TERM_RED << "\nLOCAL (RECIPROCAL) CHANGESET BEFORE MERGE:\n" << TERM_RESET;
            our_changesets[i]->print(std::cerr);
        }

        for (size_t i = 0; i < their_size; ++i) {
            std::cerr << TERM_RED << "\nINCOMING CHANGESET BEFORE MERGE:\n" << TERM_RESET;
            their_changesets[i].print(std::cerr);
        }

        std::cerr << TERM_MAGENTA << "\nINCOMING CHANGESET INDEX:\n" << TERM_RESET;
        their_index.print(std::cerr);
        std::cerr << '\n';
        their_index.verify();

        std::cerr << TERM_YELLOW << std::setw(80) << std::left << "MERGE TRACE (incoming):"
                  << "MERGE TRACE (local):\n"
                  << TERM_RESET;
    }
#else
    static_cast<void>(local_file_ident);
#endif // REALM_DEBUG LCOV_EXCL_STOP

    for (size_t i = 0; i < our_size; ++i) {
        if (logger) {
            logger->trace(
                "Transforming local changeset [%1/%2] through %3 incoming changeset(s) with %4 conflict group(s)",
                i + 1, our_size, their_size, their_index.get_num_conflict_groups());
        }
        Changeset* our_changeset = our_changesets[i];

        transformer.m_major_side.set_next_changeset(our_changeset);
        // MinorSide uses the index to find the Changeset.
        transformer.m_minor_side.m_changeset_index = &their_index;
        transformer.transform(); // Throws
    }

    if (logger) {
        logger->debug("Finished transforming %1 local changesets through %2 incoming changesets (%3 vs %4 "
                      "instructions, in %5 conflict groups)",
                      our_size, their_size, our_num_instructions, their_num_instructions,
                      their_index.get_num_conflict_groups());
    }

#if REALM_DEBUG // LCOV_EXCL_START
    // Check that the index is still valid after transformation.
    their_index.verify();
#endif // REALM_DEBUG LCOV_EXCL_STOP

#if REALM_DEBUG // LCOV_EXCL_START
    if (trace) {
        for (size_t i = 0; i < our_size; ++i) {
            std::cerr << TERM_CYAN << "\nRECIPROCAL CHANGESET AFTER MERGE:\n" << TERM_RESET;
            our_changesets[i]->print(std::cerr);
            std::cerr << '\n';
        }
        for (size_t i = 0; i < their_size; ++i) {
            std::cerr << TERM_CYAN << "INCOMING CHANGESET AFTER MERGE:\n" << TERM_RESET;
            their_changesets[i].print(std::cerr);
            std::cerr << '\n';
        }
    }
#endif // LCOV_EXCL_STOP REALM_DEBUG
}

void TransformerImpl::transform_remote_changesets(TransformHistory& history, file_ident_type local_file_ident,
                                                  version_type current_local_version, Changeset* parsed_changesets,
                                                  std::size_t num_changesets, Reporter* reporter,
                                                  util::Logger* logger)
{
    REALM_ASSERT(local_file_ident != 0);

    AllocationMetricNameScope scope{g_transform_metric_scope};

    metered::vector<Changeset*> our_changesets;

    try {
        // p points to the beginning of a range of changesets that share the same
        // "base", i.e. are based on the same local version.
        auto p = parsed_changesets;
        auto parsed_changesets_end = parsed_changesets + num_changesets;
        while (p != parsed_changesets_end) {
            // Find the range of incoming changesets that share the same
            // last_integrated_local_version, which means we can merge them in one go.
            auto same_base_range_end = std::find_if(p + 1, parsed_changesets_end, [&](auto& changeset) {
                return p->last_integrated_remote_version != changeset.last_integrated_remote_version;
            });

            version_type begin_version = p->last_integrated_remote_version;
            version_type end_version = current_local_version;
            for (;;) {
                HistoryEntry history_entry;
                version_type version = history.find_history_entry(begin_version, end_version, history_entry);
                if (version == 0)
                    break; // No more local changesets

                Changeset& our_changeset = get_reciprocal_transform(history, local_file_ident, version,
                                                                    history_entry); // Throws
                our_changesets.push_back(&our_changeset);

                begin_version = version;
            }

            if (!our_changesets.empty()) {
                merge_changesets(local_file_ident, &*p, same_base_range_end - p, our_changesets.data(),
                                 our_changesets.size(), reporter, logger); // Throws
            }

            p = same_base_range_end;
            our_changesets.clear(); // deliberately not releasing memory
        }
    }
    catch (...) {
        // If an exception was thrown while merging, the transform cache will
        // be polluted. This is a problem since the same cache object is reused
        // by multiple invocations to transform_remote_changesets(), so we must
        // clear the cache before rethrowing.
        //
        // Note that some valid changesets can still cause exceptions to be
        // thrown by the merge algorithm, namely incompatible schema changes.
        m_reciprocal_transform_cache.clear();
        throw;
    }

    // NOTE: Any exception thrown during flushing *MUST* lead to rollback of
    // the current transaction.
    flush_reciprocal_transform_cache(history); // Throws
}


Changeset& TransformerImpl::get_reciprocal_transform(TransformHistory& history, file_ident_type local_file_ident,
                                                     version_type version, const HistoryEntry& history_entry)
{
    auto p = m_reciprocal_transform_cache.emplace(version, nullptr); // Throws
    auto i = p.first;
    if (p.second) {
        i->second = std::make_unique<Changeset>(); // Throws
        ChunkedBinaryData data = history.get_reciprocal_transform(version);
        ChunkedBinaryInputStream in{data};
        Changeset& changeset = *i->second;
        sync::parse_changeset(in, changeset); // Throws

        changeset.version = version;
        changeset.last_integrated_remote_version = history_entry.remote_version;
        changeset.origin_timestamp = history_entry.origin_timestamp;
        file_ident_type origin_file_ident = history_entry.origin_file_ident;
        if (origin_file_ident == 0)
            origin_file_ident = local_file_ident;
        changeset.origin_file_ident = origin_file_ident;
    }
    return *i->second;
}


void TransformerImpl::flush_reciprocal_transform_cache(TransformHistory& history)
{
    try {
        ChangesetEncoder::Buffer output_buffer;
        for (const auto& entry : m_reciprocal_transform_cache) {
            if (entry.second->is_dirty()) {
                encode_changeset(*entry.second, output_buffer); // Throws
                version_type version = entry.first;
                BinaryData data{output_buffer.data(), output_buffer.size()};
                history.set_reciprocal_transform(version, data); // Throws
                output_buffer.clear();
            }
        }
        m_reciprocal_transform_cache.clear();
    }
    catch (...) {
        m_reciprocal_transform_cache.clear();
        throw;
    }
}

} // namespace _impl

namespace sync {
std::unique_ptr<Transformer> make_transformer()
{
    return std::make_unique<_impl::TransformerImpl>(); // Throws
}


void parse_remote_changeset(const Transformer::RemoteChangeset& remote_changeset, Changeset& parsed_changeset)
{
    // origin_file_ident = 0 is currently used to indicate an entry of local
    // origin.
    REALM_ASSERT(remote_changeset.origin_file_ident != 0);
    REALM_ASSERT(remote_changeset.remote_version != 0);

    ChunkedBinaryInputStream remote_in{remote_changeset.data};
    try {
        parse_changeset(remote_in, parsed_changeset); // Throws
    }
    catch (sync::BadChangesetError& e) {
        throw TransformError(e.what());
    }
    parsed_changeset.version = remote_changeset.remote_version;
    parsed_changeset.last_integrated_remote_version = remote_changeset.last_integrated_local_version;
    parsed_changeset.origin_timestamp = remote_changeset.origin_timestamp;
    parsed_changeset.origin_file_ident = remote_changeset.origin_file_ident;
}

} // namespace sync
} // namespace realm
