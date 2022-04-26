/*************************************************************************
 *
 * Copyright 2017 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_SYNC_IMPL_INSTRUCTION_APPLIER_HPP
#define REALM_SYNC_IMPL_INSTRUCTION_APPLIER_HPP

#include <realm/sync/instructions.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/util/logger.hpp>
#include <realm/list.hpp>
#include <realm/dictionary.hpp>

#include <tuple>

namespace realm {
namespace sync {

struct Changeset;

struct InstructionApplier {
    explicit InstructionApplier(Transaction&) noexcept;

    /// Throws BadChangesetError if application fails due to a problem with the
    /// changeset.
    ///
    /// FIXME: Consider using std::error_code instead of throwing
    /// BadChangesetError.
    void apply(const Changeset&, util::Logger*);

    void begin_apply(const Changeset&, util::Logger*) noexcept;
    void end_apply() noexcept;

protected:
    util::Optional<Obj> get_top_object(const Instruction::ObjectInstruction&,
                                       const std::string_view& instr = "(unspecified)");
    static std::unique_ptr<LstBase> get_list_from_path(Obj& obj, ColKey col);
    StringData get_string(InternString) const;
    StringData get_string(StringBufferRange) const;
    BinaryData get_binary(StringBufferRange) const;
#define REALM_DECLARE_INSTRUCTION_HANDLER(X) virtual void operator()(const Instruction::X&);
    REALM_FOR_EACH_INSTRUCTION_TYPE(REALM_DECLARE_INSTRUCTION_HANDLER)
#undef REALM_DECLARE_INSTRUCTION_HANDLER
    friend struct Instruction; // to allow visitor

    template <class A>
    static void apply(A& applier, const Changeset&, util::Logger*);

    // Allows for in-place modification of changeset while applying it
    template <class A>
    static void apply(A& applier, Changeset&, util::Logger*);

    TableRef table_for_class_name(StringData) const; // Throws

    Transaction& m_transaction;

    template <class... Args>
    void log(const char* fmt, Args&&... args)
    {
        if (m_logger) {
            m_logger->trace(fmt, std::forward<Args>(args)...); // Throws
        }
    }

    using ListCallback = util::UniqueFunction<void(LstBase&, size_t)>;
    void resolve_list(const Instruction::PathInstruction& instr, const char* instr_name, ListCallback&& callback);
    bool check_links_exist(const Instruction::Payload& payload);
    bool allows_null_links(const Instruction::PathInstruction& instr, const std::string_view& instr_name);
    std::string to_string(const Instruction::PathInstruction& instr) const;

    struct PathResolver {
        enum class Status { Pending, Success, DidNotResolve };
        using PropCallbackType = util::UniqueFunction<void(PathResolver*, Obj&, ColKey)>;
        using ListCallbackType = util::UniqueFunction<void(PathResolver*, LstBase&)>;
        using ListIndexCallbackType = util::UniqueFunction<void(PathResolver*, LstBase&, uint32_t)>;
        using DictionaryCallbackType = util::UniqueFunction<void(PathResolver*, Dictionary&)>;
        using DictionaryKeyCallbackType = util::UniqueFunction<void(PathResolver*, Dictionary&, Mixed)>;
        using SetCallbackType = util::UniqueFunction<void(PathResolver*, SetBase&)>;
        using ErrorCallbackType = util::UniqueFunction<void(const std::string&)>;
        using ColAdvanceType = util::UniqueFunction<void(PathResolver*, ColKey)>;
        using DictKeyAdvanceType = util::UniqueFunction<void(PathResolver*, StringData)>;
        using ListIndexAdvanceType = util::UniqueFunction<void(PathResolver*, uint32_t)>;
        using NullLinkAdvanceType = util::UniqueFunction<void(PathResolver*, StringData, StringData)>;
        using FinishCallbackType = util::UniqueFunction<void()>;
        using StringGetterType = util::UniqueFunction<StringData(InternString)>;

        PathResolver(Obj top_obj, const Instruction::PathInstruction& instr, const std::string_view& instr_name,
                     ErrorCallbackType on_error, StringGetterType string_getter);
        virtual ~PathResolver();
        virtual Status resolve();

        PathResolver* on_property(PropCallbackType);
        PathResolver* on_list(ListCallbackType);
        PathResolver* on_list_index(ListIndexCallbackType);
        PathResolver* on_dictionary(DictionaryCallbackType);
        PathResolver* on_dictionary_key(DictionaryKeyCallbackType);
        PathResolver* on_set(SetCallbackType);
        PathResolver* on_error(ErrorCallbackType);
        PathResolver* on_column_advance(ColAdvanceType);
        PathResolver* on_dict_key_advance(DictKeyAdvanceType);
        PathResolver* on_list_index_advance(ListIndexAdvanceType);
        PathResolver* on_null_link_advance(NullLinkAdvanceType);
        PathResolver* on_finish(FinishCallbackType);
        void do_not_resolve();
        void preempt_success();
        const std::string_view& instruction_name() const noexcept
        {
            return m_instr_name;
        }

    protected:
        void resolve_field(Obj& obj, InternString field);
        void resolve_list_element(LstBase& list, uint32_t index);
        void resolve_dictionary_element(Dictionary& dict, InternString key);

        Obj m_top_obj;
        const Instruction::PathInstruction& m_instr;
        std::string_view m_instr_name;
        Status m_status;
        Instruction::Path::const_iterator m_it_begin;
        Instruction::Path::const_iterator m_it_end;

        PropCallbackType m_on_prop;
        ListCallbackType m_on_list;
        ListIndexCallbackType m_on_list_ndx;
        DictionaryCallbackType m_on_dictionary;
        DictionaryKeyCallbackType m_on_dictionary_key;
        SetCallbackType m_on_set;
        ErrorCallbackType m_on_error;
        ColAdvanceType m_on_col_advance;
        DictKeyAdvanceType m_on_dict_key_advance;
        ListIndexAdvanceType m_on_list_advance;
        NullLinkAdvanceType m_on_null_link;
        FinishCallbackType m_on_finish;
        StringGetterType m_string_getter;
    };

private:
    const Changeset* m_log = nullptr;
    util::Logger* m_logger = nullptr;

    Group::TableNameBuffer m_table_name_buffer;
    InternString m_last_table_name;
    InternString m_last_field_name;
    TableRef m_last_table;
    ColKey m_last_field;
    util::Optional<Instruction::PrimaryKey> m_last_object_key;
    util::Optional<Instruction::Path> m_current_path;
    util::Optional<Obj> m_last_object;
    std::unique_ptr<LstBase> m_last_list;

    StringData get_table_name(const Instruction::TableInstruction&, const std::string_view& instr = "(unspecified)");
    TableRef get_table(const Instruction::TableInstruction&, const std::string_view& instr = "(unspecified)");

    // Note: This may return a non-invalid ObjKey if the key is dangling.
    ObjKey get_object_key(Table& table, const Instruction::PrimaryKey&,
                          const std::string_view& instr = "(unspecified)") const;
    std::unique_ptr<PathResolver> make_resolver(const Instruction::PathInstruction& instr,
                                                const std::string_view& instr_name);

    template <class F>
    void visit_payload(const Instruction::Payload&, F&& visitor);

    REALM_NORETURN void bad_transaction_log(const std::string& msg) const;
    template <class... Params>
    REALM_NORETURN void bad_transaction_log(const char* msg, Params&&... params) const;
};


// Implementation

inline InstructionApplier::InstructionApplier(Transaction& group) noexcept
    : m_transaction(group)
{
}

inline void InstructionApplier::begin_apply(const Changeset& log, util::Logger* logger) noexcept
{
    m_log = &log;
    m_logger = logger;
}

inline void InstructionApplier::end_apply() noexcept
{
    m_log = nullptr;
    m_logger = nullptr;
    m_last_table_name = InternString{};
    m_last_field_name = InternString{};
    m_last_table = TableRef{};
    m_last_field = ColKey{};
    m_last_object.reset();
    m_last_object_key.reset();
    m_last_list.reset();
}

template <class A>
inline void InstructionApplier::apply(A& applier, const Changeset& changeset, util::Logger* logger)
{
    applier.begin_apply(changeset, logger);
    for (auto instr : changeset) {
        if (!instr)
            continue;
        instr->visit(applier); // Throws
    }
    applier.end_apply();
}

template <class A>
inline void InstructionApplier::apply(A& applier, Changeset& changeset, util::Logger* logger)
{
    applier.begin_apply(changeset, logger);
    for (auto instr : changeset) {
        if (!instr)
            continue;
        instr->visit(applier); // Throws
#if REALM_DEBUG
        applier.m_table_info_cache.verify();
#endif
    }
    applier.end_apply();
}

inline void InstructionApplier::apply(const Changeset& log, util::Logger* logger)
{
    apply(*this, log, logger); // Throws
}

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_IMPL_INSTRUCTION_APPLIER_HPP
