#include <realm/sync/instruction_applier.hpp>
#include <realm/set.hpp>
#include <realm/util/scope_exit.hpp>

#include <realm/transaction.hpp>

namespace realm::sync {
namespace {

REALM_NORETURN void throw_bad_transaction_log(std::string msg)
{
    throw BadChangesetError{std::move(msg)};
}

} // namespace

REALM_NORETURN void InstructionApplier::bad_transaction_log(const std::string& msg) const
{
    if (m_last_object_key) {
        // If the last_object_key is valid then we should have a changeset and a current table
        REALM_ASSERT(m_log);
        REALM_ASSERT(m_last_table_name);
        std::stringstream ss;
        util::Optional<InternString> field_name;
        if (m_last_field_name) {
            field_name = m_last_field_name;
        }
        const instr::Path* cur_path = m_current_path ? &(*m_current_path) : nullptr;
        m_log->print_path(ss, m_last_table_name, *m_last_object_key, field_name, cur_path);
        throw_bad_transaction_log(
            util::format("%1 (instruction target: %2, version: %3, last_integrated_remote_version: %4, "
                         "origin_file_ident: %5, timestamp: %6)",
                         msg, ss.str(), m_log->version, m_log->last_integrated_remote_version,
                         m_log->origin_file_ident, m_log->origin_timestamp));
    }
    else if (m_last_table_name) {
        // We should have a changeset if we have a table name defined.
        REALM_ASSERT(m_log);
        throw_bad_transaction_log(
            util::format("%1 (instruction table: %2, version: %3, last_integrated_remote_version: %4, "
                         "origin_file_ident: %5, timestamp: %6)",
                         msg, m_log->get_string(m_last_table_name), m_log->version,
                         m_log->last_integrated_remote_version, m_log->origin_file_ident, m_log->origin_timestamp));
    }
    else if (m_log) {
        // If all we have is a changeset, then we should log whatever we can about it.
        throw_bad_transaction_log(util::format("%1 (version: %2, last_integrated_remote_version: %3, "
                                               "origin_file_ident: %4, timestamp: %5)",
                                               msg, m_log->version, m_log->last_integrated_remote_version,
                                               m_log->origin_file_ident, m_log->origin_timestamp));
    }
    throw_bad_transaction_log(std::move(msg));
}

template <class... Params>
REALM_NORETURN void InstructionApplier::bad_transaction_log(const char* msg, Params&&... params) const
{
    // FIXME: Avoid throwing in normal program flow (since changesets can come
    // in over the network, defective changesets are part of normal program
    // flow).
    bad_transaction_log(util::format(msg, std::forward<Params>(params)...));
}

StringData InstructionApplier::get_string(InternString str) const
{
    auto string = m_log->try_get_intern_string(str);
    if (REALM_UNLIKELY(!string))
        bad_transaction_log("string read fails");
    return m_log->get_string(*string);
}

StringData InstructionApplier::get_string(StringBufferRange range) const
{
    auto string = m_log->try_get_string(range);
    if (!string)
        bad_transaction_log("string read error");
    return *string;
}

BinaryData InstructionApplier::get_binary(StringBufferRange range) const
{
    auto string = m_log->try_get_string(range);
    if (!string)
        bad_transaction_log("binary read error");
    return BinaryData{string->data(), string->size()};
}

TableRef InstructionApplier::table_for_class_name(StringData class_name) const
{
    if (class_name.size() >= Group::max_table_name_length - 6)
        bad_transaction_log("class name too long");
    Group::TableNameBuffer buffer;
    return m_transaction.get_table(Group::class_name_to_table_name(class_name, buffer));
}

template <typename T>
struct TemporarySwapOut {
    explicit TemporarySwapOut(T& target)
        : target(target)
        , backup()
    {
        std::swap(target, backup);
    }

    ~TemporarySwapOut()
    {
        std::swap(backup, target);
    }

    T& target;
    T backup;
};

void InstructionApplier::operator()(const Instruction::AddTable& instr)
{
    auto table_name = get_table_name(instr);

    // Temporarily swap out the last object key so it doesn't get included in error messages
    TemporarySwapOut<decltype(m_last_object_key)> last_object_key_guard(m_last_object_key);

    auto add_table = util::overload{
        [&](const Instruction::AddTable::TopLevelTable& spec) {
            auto table_type = (spec.is_asymmetric ? Table::Type::TopLevelAsymmetric : Table::Type::TopLevel);
            if (spec.pk_type == Instruction::Payload::Type::GlobalKey) {
                log("sync::create_table(group, \"%1\", %2);", table_name, table_type);
                m_transaction.get_or_add_table(table_name, table_type);
            }
            else {
                if (!is_valid_key_type(spec.pk_type)) {
                    bad_transaction_log("Invalid primary key type '%1' while adding table '%2'", int8_t(spec.pk_type),
                                        table_name);
                }
                DataType pk_type = get_data_type(spec.pk_type);
                StringData pk_field = get_string(spec.pk_field);
                bool nullable = spec.pk_nullable;

                log("group.get_or_add_table_with_primary_key(group, \"%1\", %2, \"%3\", %4, %5);", table_name,
                    pk_type, pk_field, nullable, table_type);
                m_transaction.get_or_add_table_with_primary_key(table_name, pk_type, pk_field, nullable, table_type);
            }
        },
        [&](const Instruction::AddTable::EmbeddedTable&) {
            if (TableRef table = m_transaction.get_table(table_name)) {
                if (!table->is_embedded()) {
                    bad_transaction_log("AddTable: The existing table '%1' is not embedded", table_name);
                }
            }
            else {
                log("group.add_embedded_table(\"%1\");", table_name);
                m_transaction.add_table(table_name, Table::Type::Embedded);
            }
        },
    };

    mpark::visit(std::move(add_table), instr.type);
}

void InstructionApplier::operator()(const Instruction::EraseTable& instr)
{
    auto table_name = get_table_name(instr);
    // Temporarily swap out the last object key so it doesn't get included in error messages
    TemporarySwapOut<decltype(m_last_object_key)> last_object_key_guard(m_last_object_key);

    if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_transaction.has_table(table_name)))) {
        // FIXME: Should EraseTable be considered idempotent?
        bad_transaction_log("table does not exist");
    }

    log("sync::erase_table(m_group, \"%1\")", table_name);
    m_transaction.remove_table(table_name);
}

void InstructionApplier::operator()(const Instruction::CreateObject& instr)
{
    auto table = get_table(instr);
    ColKey pk_col = table->get_primary_key_column();
    m_last_object_key = instr.object;

    mpark::visit(
        util::overload{
            [&](mpark::monostate) {
                if (!pk_col) {
                    bad_transaction_log("CreateObject(NULL) on table without a primary key");
                }
                if (!table->is_nullable(pk_col)) {
                    bad_transaction_log("CreateObject(NULL) on a table with a non-nullable primary key");
                }
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), realm::util::none);",
                    table->get_name());
                m_last_object = table->create_object_with_primary_key(util::none);
            },
            [&](int64_t pk) {
                if (!pk_col) {
                    bad_transaction_log("CreateObject(Int) on table without a primary key");
                }
                if (table->get_column_type(pk_col) != type_Int) {
                    bad_transaction_log("CreateObject(Int) on a table with primary key type %1",
                                        table->get_column_type(pk_col));
                }
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), %2);", table->get_name(), pk);
                m_last_object = table->create_object_with_primary_key(pk);
            },
            [&](InternString pk) {
                if (!pk_col) {
                    bad_transaction_log("CreateObject(String) on table without a primary key");
                }
                if (table->get_column_type(pk_col) != type_String) {
                    bad_transaction_log("CreateObject(String) on a table with primary key type %1",
                                        table->get_column_type(pk_col));
                }
                StringData str = get_string(pk);
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), \"%2\");", table->get_name(),
                    str);
                m_last_object = table->create_object_with_primary_key(str);
            },
            [&](const ObjectId& id) {
                if (!pk_col) {
                    bad_transaction_log("CreateObject(ObjectId) on table without a primary key");
                }
                if (table->get_column_type(pk_col) != type_ObjectId) {
                    bad_transaction_log("CreateObject(ObjectId) on a table with primary key type %1",
                                        table->get_column_type(pk_col));
                }
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), %2);", table->get_name(), id);
                m_last_object = table->create_object_with_primary_key(id);
            },
            [&](const UUID& id) {
                if (!pk_col) {
                    bad_transaction_log("CreateObject(UUID) on table without a primary key");
                }
                if (table->get_column_type(pk_col) != type_UUID) {
                    bad_transaction_log("CreateObject(UUID) on a table with primary key type %1",
                                        table->get_column_type(pk_col));
                }
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), %2);", table->get_name(), id);
                m_last_object = table->create_object_with_primary_key(id);
            },
            [&](GlobalKey key) {
                if (pk_col) {
                    bad_transaction_log("CreateObject(GlobalKey) on table with a primary key");
                }
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), GlobalKey{%2, %3});",
                    table->get_name(), key.hi(), key.lo());
                m_last_object = table->create_object(key);
            },
        },
        instr.object);
}

void InstructionApplier::operator()(const Instruction::EraseObject& instr)
{
    // FIXME: Log actions.
    // Note: EraseObject is idempotent.
    if (auto obj = get_top_object(instr, "EraseObject")) {
        // This call will prevent incoming links to be nullified/deleted
        obj->invalidate();
    }
    m_last_object.reset();
}

template <class F>
void InstructionApplier::visit_payload(const Instruction::Payload& payload, F&& visitor)
{
    using Type = Instruction::Payload::Type;

    const auto& data = payload.data;
    switch (payload.type) {
        case Type::ObjectValue:
            return visitor(Instruction::Payload::ObjectValue{});
        case Type::Dictionary:
            return bad_transaction_log("Nested dictionaries not supported yet");
        case Type::Erased:
            return visitor(Instruction::Payload::Erased{});
        case Type::GlobalKey:
            return visitor(realm::util::none); // FIXME: Not sure about this
        case Type::Null:
            return visitor(realm::util::none);
        case Type::Int:
            return visitor(data.integer);
        case Type::Bool:
            return visitor(data.boolean);
        case Type::String: {
            StringData value = get_string(data.str);
            return visitor(value);
        }
        case Type::Binary: {
            BinaryData value = get_binary(data.binary);
            return visitor(value);
        }
        case Type::Timestamp:
            return visitor(data.timestamp);
        case Type::Float:
            return visitor(data.fnum);
        case Type::Double:
            return visitor(data.dnum);
        case Type::Decimal:
            return visitor(data.decimal);
        case Type::Link: {
            StringData class_name = get_string(data.link.target_table);
            Group::TableNameBuffer buffer;
            StringData target_table_name = Group::class_name_to_table_name(class_name, buffer);
            TableRef target_table = m_transaction.get_table(target_table_name);
            if (!target_table) {
                bad_transaction_log("Link with invalid target table '%1'", target_table_name);
            }
            if (target_table->is_embedded()) {
                bad_transaction_log("Link to embedded table '%1'", target_table_name);
            }
            ObjKey target = get_object_key(*target_table, data.link.target);
            ObjLink link = ObjLink{target_table->get_key(), target};
            return visitor(link);
        }
        case Type::ObjectId:
            return visitor(data.object_id);
        case Type::UUID:
            return visitor(data.uuid);
    }
}

void InstructionApplier::operator()(const Instruction::Update& instr)
{
    struct UpdateResolver : public PathResolver {
        UpdateResolver(InstructionApplier* applier, const Instruction::Update& instr)
            : PathResolver(applier, instr, "Update")
            , m_instr(instr)
        {
        }
        void on_property(Obj& obj, ColKey col) override
        {
            // Update of object field.

            auto table = obj.get_table();
            auto table_name = table->get_name();
            auto field_name = table->get_column_name(col);
            auto data_type = DataType(col.get_type());

            auto visitor = [&](const mpark::variant<ObjLink, Mixed, Instruction::Payload::ObjectValue,
                                                    Instruction::Payload::Erased>& arg) {
                if (const auto link_ptr = mpark::get_if<ObjLink>(&arg)) {
                    if (data_type == type_Mixed || data_type == type_TypedLink) {
                        obj.set_any(col, *link_ptr, m_instr.is_default);
                    }
                    else if (data_type == type_Link) {
                        // Validate target table.
                        auto target_table = table->get_link_target(col);
                        if (target_table->get_key() != link_ptr->get_table_key()) {
                            m_applier->bad_transaction_log(
                                "Update: Target table mismatch (expected %1, got %2)", target_table->get_name(),
                                m_applier->m_transaction.get_table(link_ptr->get_table_key())->get_name());
                        }
                        obj.set<ObjKey>(col, link_ptr->get_obj_key(), m_instr.is_default);
                    }
                    else {
                        m_applier->bad_transaction_log("Update: Type mismatch in '%2.%1' (expected %3, got %4)",
                                                       field_name, table_name, col.get_type(), type_Link);
                    }
                }
                else if (const auto mixed_ptr = mpark::get_if<Mixed>(&arg)) {
                    if (mixed_ptr->is_null()) {
                        if (col.is_nullable()) {
                            obj.set_null(col, m_instr.is_default);
                        }
                        else {
                            m_applier->bad_transaction_log("Update: NULL in non-nullable field '%2.%1'", field_name,
                                                           table_name);
                        }
                    }
                    else if (data_type == type_Mixed || mixed_ptr->get_type() == data_type) {
                        obj.set_any(col, *mixed_ptr, m_instr.is_default);
                    }
                    else {
                        m_applier->bad_transaction_log("Update: Type mismatch in '%2.%1' (expected %3, got %4)",
                                                       field_name, table_name, col.get_type(), mixed_ptr->get_type());
                    }
                }
                else if (const auto obj_val_ptr = mpark::get_if<Instruction::Payload::ObjectValue>(&arg)) {
                    if (obj.is_null(col)) {
                        obj.create_and_set_linked_object(col);
                    }
                }
                else if (const auto erase_ptr = mpark::get_if<Instruction::Payload::Erased>(&arg)) {
                    m_applier->bad_transaction_log("Update: Dictionary erase at object field");
                }
            };

            m_applier->visit_payload(m_instr.value, visitor);
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            // Update of list element.

            auto col = list.get_col_key();
            auto data_type = DataType(col.get_type());
            auto table = list.get_table();
            auto table_name = table->get_name();
            auto field_name = table->get_column_name(col);

            auto visitor = util::overload{
                [&](const ObjLink& link) {
                    if (data_type == type_TypedLink) {
                        REALM_ASSERT(dynamic_cast<Lst<ObjLink>*>(&list));
                        auto& link_list = static_cast<Lst<ObjLink>&>(list);
                        link_list.set(index, link);
                    }
                    else if (data_type == type_Mixed) {
                        REALM_ASSERT(dynamic_cast<Lst<Mixed>*>(&list));
                        auto& mixed_list = static_cast<Lst<Mixed>&>(list);
                        mixed_list.set(index, link);
                    }
                    else if (data_type == type_LinkList || data_type == type_Link) {
                        REALM_ASSERT(dynamic_cast<Lst<ObjKey>*>(&list));
                        auto& link_list = static_cast<Lst<ObjKey>&>(list);
                        // Validate the target.
                        auto target_table = table->get_link_target(col);
                        if (target_table->get_key() != link.get_table_key()) {
                            m_applier->bad_transaction_log(
                                "Update: Target table mismatch (expected '%1', got '%2')", target_table->get_name(),
                                m_applier->m_transaction.get_table(link.get_table_key())->get_name());
                        }
                        link_list.set(index, link.get_obj_key());
                    }
                    else {
                        m_applier->bad_transaction_log(
                            "Update: Type mismatch in list at '%2.%1' (expected link type, was %3)", field_name,
                            table_name, data_type);
                    }
                },
                [&](Mixed value) {
                    if (value.is_null()) {
                        if (col.is_nullable()) {
                            list.set_null(index);
                        }
                        else {
                            m_applier->bad_transaction_log("Update: NULL in non-nullable list '%2.%1'", field_name,
                                                           table_name);
                        }
                    }
                    else {
                        if (data_type == type_Mixed || value.get_type() == data_type) {
                            list.set_any(index, value);
                        }
                        else {
                            m_applier->bad_transaction_log(
                                "Update: Type mismatch in list at '%2.%1' (expected %3, got %4)", field_name,
                                table_name, data_type, value.get_type());
                        }
                    }
                },
                [&](const Instruction::Payload::ObjectValue&) {
                    // Embedded object creation is idempotent, and link lists cannot
                    // contain nulls, so this is a no-op.
                },
                [&](const Instruction::Payload::Erased&) {
                    m_applier->bad_transaction_log("Update: Dictionary erase of list element");
                },
            };

            m_applier->visit_payload(m_instr.value, visitor);
            return Status::Pending;
        }
        Status on_dictionary_key(Dictionary& dict, Mixed key) override
        {
            // Update (insert) of dictionary element.

            auto visitor = util::overload{
                [&](Mixed value) {
                    if (value.is_null()) {
                        // FIXME: Separate handling of NULL is needed because
                        // `Mixed::get_type()` asserts on NULL.
                        dict.insert(key, value);
                    }
                    else if (value.get_type() == type_Link) {
                        m_applier->bad_transaction_log("Update: Untyped links are not supported in dictionaries.");
                    }
                    else {
                        dict.insert(key, value);
                    }
                },
                [&](const Instruction::Payload::Erased&) {
                    dict.erase(key);
                },
                [&](const Instruction::Payload::ObjectValue&) {
                    dict.create_and_insert_linked_object(key);
                },
            };

            m_applier->visit_payload(m_instr.value, visitor);
            return Status::Pending;
        }

    private:
        const Instruction::Update& m_instr;
    };
    UpdateResolver resolver(this, instr);
    resolver.resolve();
}

void InstructionApplier::operator()(const Instruction::AddInteger& instr)
{
    // FIXME: Implement increments of array elements, dictionary values.
    struct AddIntegerResolver : public PathResolver {
        AddIntegerResolver(InstructionApplier* applier, const Instruction::AddInteger& instr)
            : PathResolver(applier, instr, "AddInteger")
            , m_instr(instr)
        {
        }
        void on_property(Obj& obj, ColKey col)
        {
            // Increment of object field.
            if (!obj.is_null(col)) {
                try {
                    obj.add_int(col, m_instr.value);
                }
                catch (const LogicError&) {
                    auto table = obj.get_table();
                    m_applier->bad_transaction_log("AddInteger: Not an integer field '%2.%1'",
                                                   table->get_column_name(col), table->get_name());
                }
            }
        }

    private:
        const Instruction::AddInteger& m_instr;
    };
    AddIntegerResolver resolver(this, instr);
    resolver.resolve();
}

void InstructionApplier::operator()(const Instruction::AddColumn& instr)
{
    using Type = Instruction::Payload::Type;
    using CollectionType = Instruction::AddColumn::CollectionType;

    // Temporarily swap out the last object key so it doesn't get included in error messages
    TemporarySwapOut<decltype(m_last_object_key)> last_object_key_guard(m_last_object_key);

    auto table = get_table(instr, "AddColumn");
    auto col_name = get_string(instr.field);

    if (ColKey existing_key = table->get_column_key(col_name)) {
        DataType new_type = get_data_type(instr.type);
        ColumnType existing_type = existing_key.get_type();
        if (existing_type == col_type_LinkList) {
            existing_type = col_type_Link;
        }
        if (existing_type != ColumnType(new_type)) {
            bad_transaction_log("AddColumn: Schema mismatch for existing column in '%1.%2' (expected %3, got %4)",
                                table->get_name(), col_name, existing_type, new_type);
        }
        bool existing_is_list = existing_key.is_list();
        if ((instr.collection_type == CollectionType::List) != existing_is_list) {
            bad_transaction_log(
                "AddColumn: Schema mismatch for existing column in '%1.%2' (existing is%3 a list, the other is%4)",
                table->get_name(), col_name, existing_is_list ? "" : " not", existing_is_list ? " not" : "");
        }
        bool existing_is_set = existing_key.is_set();
        if ((instr.collection_type == CollectionType::Set) != existing_is_set) {
            bad_transaction_log(
                "AddColumn: Schema mismatch for existing column in '%1.%2' (existing is%3 a set, the other is%4)",
                table->get_name(), col_name, existing_is_set ? "" : " not", existing_is_set ? " not" : "");
        }
        bool existing_is_dict = existing_key.is_dictionary();
        if ((instr.collection_type == CollectionType::Dictionary) != existing_is_dict) {
            bad_transaction_log("AddColumn: Schema mismatch for existing column in '%1.%2' (existing is%3 a "
                                "dictionary, the other is%4)",
                                table->get_name(), col_name, existing_is_dict ? "" : " not",
                                existing_is_dict ? " not" : "");
        }
        if (new_type == type_Link) {
            Group::TableNameBuffer buffer;
            auto target_table_name = Group::class_name_to_table_name(get_string(instr.link_target_table), buffer);
            if (target_table_name != table->get_link_target(existing_key)->get_name()) {
                bad_transaction_log("AddColumn: Schema mismatch for existing column in '%1.%2' (link targets differ)",
                                    table->get_name(), col_name);
            }
        }
        return;
    }

    if (instr.collection_type == CollectionType::Dictionary && instr.key_type != Type::String) {
        bad_transaction_log("AddColumn '%1.%3' adding dictionary column with non-string keys", table->get_name(),
                            col_name);
    }

    if (instr.type != Type::Link) {
        DataType type = get_data_type(instr.type);
        switch (instr.collection_type) {
            case CollectionType::Single: {
                table->add_column(type, col_name, instr.nullable);
                break;
            }
            case CollectionType::List: {
                table->add_column_list(type, col_name, instr.nullable);
                break;
            }
            case CollectionType::Dictionary: {
                DataType key_type = get_data_type(instr.key_type);
                table->add_column_dictionary(type, col_name, instr.nullable, key_type);
                break;
            }
            case CollectionType::Set: {
                table->add_column_set(type, col_name, instr.nullable);
                break;
            }
        }
    }
    else {
        Group::TableNameBuffer buffer;
        auto target_table_name = get_string(instr.link_target_table);
        if (target_table_name.size() != 0) {
            TableRef target = m_transaction.get_table(Group::class_name_to_table_name(target_table_name, buffer));
            if (!target) {
                bad_transaction_log("AddColumn(Link) '%1.%2' to table '%3' which doesn't exist", table->get_name(),
                                    col_name, target_table_name);
            }
            if (instr.collection_type == CollectionType::List) {
                table->add_column_list(*target, col_name);
            }
            else if (instr.collection_type == CollectionType::Set) {
                table->add_column_set(*target, col_name);
            }
            else if (instr.collection_type == CollectionType::Dictionary) {
                table->add_column_dictionary(*target, col_name);
            }
            else {
                REALM_ASSERT(instr.collection_type == CollectionType::Single);
                table->add_column(*target, col_name);
            }
        }
        else {
            if (instr.collection_type == CollectionType::List) {
                table->add_column_list(type_TypedLink, col_name);
            }
            else {
                REALM_ASSERT(instr.collection_type == CollectionType::Single);
                table->add_column(type_TypedLink, col_name);
            }
        }
    }
}

void InstructionApplier::operator()(const Instruction::EraseColumn& instr)
{
    // Temporarily swap out the last object key so it doesn't get included in error messages
    TemporarySwapOut<decltype(m_last_object_key)> last_object_key_guard(m_last_object_key);

    auto table = get_table(instr, "EraseColumn");
    auto col_name = get_string(instr.field);

    ColKey col = table->get_column_key(col_name);
    if (!col) {
        bad_transaction_log("EraseColumn '%1.%2' which doesn't exist", table->get_name(), col_name);
    }

    table->remove_column(col);
}

void InstructionApplier::operator()(const Instruction::ArrayInsert& instr)
{
    struct ArrayInsertResolver : public PathResolver {
        ArrayInsertResolver(InstructionApplier* applier, const Instruction::ArrayInsert& instr)
            : PathResolver(applier, instr, "ArrayInsert")
            , m_instr(instr)
        {
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            auto col = list.get_col_key();
            auto data_type = DataType(col.get_type());
            auto table = list.get_table();
            auto table_name = table->get_name();
            auto field_name = table->get_column_name(col);

            if (index > m_instr.prior_size) {
                m_applier->bad_transaction_log("ArrayInsert: Invalid insertion index (index = %1, prior_size = %2)",
                                               index, m_instr.prior_size);
            }

            if (index > list.size()) {
                m_applier->bad_transaction_log("ArrayInsert: Index out of bounds (%1 > %2)", index, list.size());
            }

            if (m_instr.prior_size != list.size()) {
                m_applier->bad_transaction_log("ArrayInsert: Invalid prior_size (list size = %1, prior_size = %2)",
                                               list.size(), m_instr.prior_size);
            }

            auto inserter = util::overload{
                [&](const ObjLink& link) {
                    if (data_type == type_TypedLink) {
                        REALM_ASSERT(dynamic_cast<Lst<ObjLink>*>(&list));
                        auto& link_list = static_cast<Lst<ObjLink>&>(list);
                        link_list.insert(index, link);
                    }
                    else if (data_type == type_Mixed) {
                        REALM_ASSERT(dynamic_cast<Lst<Mixed>*>(&list));
                        auto& mixed_list = static_cast<Lst<Mixed>&>(list);
                        mixed_list.insert(index, link);
                    }
                    else if (data_type == type_LinkList || data_type == type_Link) {
                        REALM_ASSERT(dynamic_cast<Lst<ObjKey>*>(&list));
                        auto& link_list = static_cast<Lst<ObjKey>&>(list);
                        // Validate the target.
                        auto target_table = table->get_link_target(col);
                        if (target_table->get_key() != link.get_table_key()) {
                            m_applier->bad_transaction_log(
                                "ArrayInsert: Target table mismatch (expected '%1', got '%2')",
                                target_table->get_name(),
                                m_applier->m_transaction.get_table(link.get_table_key())->get_name());
                        }
                        link_list.insert(index, link.get_obj_key());
                    }
                    else {
                        m_applier->bad_transaction_log(
                            "ArrayInsert: Type mismatch in list at '%2.%1' (expected link type, was %3)", field_name,
                            table_name, data_type);
                    }
                },
                [&](Mixed value) {
                    if (value.is_null()) {
                        if (col.is_nullable()) {
                            list.insert_null(index);
                        }
                        else {
                            m_applier->bad_transaction_log("ArrayInsert: NULL in non-nullable list '%2.%1'",
                                                           field_name, table_name);
                        }
                    }
                    else {
                        if (data_type == type_Mixed || value.get_type() == data_type) {
                            list.insert_any(index, value);
                        }
                        else {
                            m_applier->bad_transaction_log(
                                "ArrayInsert: Type mismatch in list at '%2.%1' (expected %3, got %4)", field_name,
                                table_name, data_type, value.get_type());
                        }
                    }
                },
                [&](const Instruction::Payload::ObjectValue&) {
                    if (col.get_type() == col_type_LinkList || col.get_type() == col_type_Link) {
                        auto target_table = list.get_table()->get_link_target(col);
                        if (!target_table->is_embedded()) {
                            m_applier->bad_transaction_log(
                                "ArrayInsert: Creation of embedded object of type '%1', which is not "
                                "an embedded table",
                                target_table->get_name());
                        }

                        REALM_ASSERT(dynamic_cast<LnkLst*>(&list));
                        auto& link_list = static_cast<LnkLst&>(list);
                        link_list.create_and_insert_linked_object(index);
                    }
                    else {
                        m_applier->bad_transaction_log(
                            "ArrayInsert: Creation of embedded object in non-link list field '%2.%1'", field_name,
                            table_name);
                    }
                },
                [&](const Instruction::Payload::Dictionary&) {
                    m_applier->bad_transaction_log("Dictionary payload for ArrayInsert");
                },
                [&](const Instruction::Payload::Erased&) {
                    m_applier->bad_transaction_log("Dictionary erase payload for ArrayInsert");
                },
            };

            m_applier->visit_payload(m_instr.value, inserter);
            return Status::Pending;
        }

    private:
        const Instruction::ArrayInsert& m_instr;
    };
    ArrayInsertResolver(this, instr).resolve();
}

void InstructionApplier::operator()(const Instruction::ArrayMove& instr)
{
    struct ArrayMoveResolver : public PathResolver {
        ArrayMoveResolver(InstructionApplier* applier, const Instruction::ArrayMove& instr)
            : PathResolver(applier, instr, "ArrayMove")
            , m_instr(instr)
        {
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            if (index >= list.size()) {
                m_applier->bad_transaction_log("ArrayMove from out of bounds (%1 >= %2)", m_instr.index(),
                                               list.size());
            }
            if (m_instr.ndx_2 >= list.size()) {
                m_applier->bad_transaction_log("ArrayMove to out of bounds (%1 >= %2)", m_instr.ndx_2, list.size());
            }
            if (index == m_instr.ndx_2) {
                // FIXME: Does this really need to be an error?
                m_applier->bad_transaction_log("ArrayMove to same location (%1)", m_instr.index());
            }
            if (m_instr.prior_size != list.size()) {
                m_applier->bad_transaction_log("ArrayMove: Invalid prior_size (list size = %1, prior_size = %2)",
                                               list.size(), m_instr.prior_size);
            }
            list.move(index, m_instr.ndx_2);
            return Status::Pending;
        }

    private:
        const Instruction::ArrayMove& m_instr;
    };
    ArrayMoveResolver(this, instr).resolve();
}

void InstructionApplier::operator()(const Instruction::ArrayErase& instr)
{
    struct ArrayEraseResolver : public PathResolver {
        ArrayEraseResolver(InstructionApplier* applier, const Instruction::ArrayErase& instr)
            : PathResolver(applier, instr, "ArrayErase")
            , m_instr(instr)
        {
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            if (index >= m_instr.prior_size) {
                m_applier->bad_transaction_log("ArrayErase: Invalid index (index = %1, prior_size = %2)", index,
                                               m_instr.prior_size);
            }
            if (index >= list.size()) {
                m_applier->bad_transaction_log("ArrayErase: Index out of bounds (%1 >= %2)", index, list.size());
            }
            if (m_instr.prior_size != list.size()) {
                m_applier->bad_transaction_log("ArrayErase: Invalid prior_size (list size = %1, prior_size = %2)",
                                               list.size(), m_instr.prior_size);
            }
            list.remove(index, index + 1);
            return Status::Pending;
        }

    private:
        const Instruction::ArrayErase& m_instr;
    };
    ArrayEraseResolver(this, instr).resolve();
}

void InstructionApplier::operator()(const Instruction::Clear& instr)
{
    struct ClearResolver : public PathResolver {
        ClearResolver(InstructionApplier* applier, const Instruction::Clear& instr)
            : PathResolver(applier, instr, "Clear")
        {
        }
        void on_list(LstBase& list) override
        {
            list.clear();
        }
        void on_dictionary(Dictionary& dict) override
        {
            dict.clear();
        }
        void on_set(SetBase& set) override
        {
            set.clear();
        }
    };
    ClearResolver(this, instr).resolve();
}

bool InstructionApplier::allows_null_links(const Instruction::PathInstruction& instr,
                                           const std::string_view& instr_name)
{
    struct AllowsNullsResolver : public PathResolver {
        AllowsNullsResolver(InstructionApplier* applier, const Instruction::PathInstruction& instr,
                            const std::string_view& instr_name)
            : PathResolver(applier, instr, instr_name)
            , m_allows_nulls(false)
        {
        }
        Status on_list_index(LstBase&, uint32_t) override
        {
            return Status::Pending;
        }
        void on_list(LstBase&) override {}
        void on_set(SetBase&) override {}
        void on_dictionary(Dictionary&) override
        {
            m_allows_nulls = true;
        }
        Status on_dictionary_key(Dictionary&, Mixed) override
        {
            m_allows_nulls = true;
            return Status::Pending;
        }
        void on_property(Obj&, ColKey) override
        {
            m_allows_nulls = true;
        }
        bool allows_nulls()
        {
            resolve();
            return m_allows_nulls;
        }

    private:
        bool m_allows_nulls;
    };
    return AllowsNullsResolver(this, instr, instr_name).allows_nulls();
}

std::string InstructionApplier::to_string(const Instruction::PathInstruction& instr) const
{
    REALM_ASSERT(m_log);
    std::stringstream ss;
    m_log->print_path(ss, instr.table, instr.object, instr.field, &instr.path);
    return ss.str();
}

bool InstructionApplier::check_links_exist(const Instruction::Payload& payload)
{
    bool valid_payload = true;
    using Type = Instruction::Payload::Type;
    if (payload.type == Type::Link) {
        StringData class_name = get_string(payload.data.link.target_table);
        Group::TableNameBuffer buffer;
        StringData target_table_name = Group::class_name_to_table_name(class_name, buffer);
        TableRef target_table = m_transaction.get_table(target_table_name);
        if (!target_table) {
            bad_transaction_log("Link with invalid target table '%1'", target_table_name);
        }
        if (target_table->is_embedded()) {
            bad_transaction_log("Link to embedded table '%1'", target_table_name);
        }
        Mixed linked_pk =
            mpark::visit(util::overload{[&](mpark::monostate) {
                                            return Mixed{}; // the link exists and the pk is null
                                        },
                                        [&](int64_t pk) {
                                            return Mixed{pk};
                                        },
                                        [&](InternString interned_pk) {
                                            return Mixed{get_string(interned_pk)};
                                        },
                                        [&](GlobalKey) {
                                            bad_transaction_log(
                                                "Unexpected link to embedded object while validating a primary key");
                                            return Mixed{}; // appease the compiler; visitors must have a single
                                                            // return type
                                        },
                                        [&](ObjectId pk) {
                                            return Mixed{pk};
                                        },
                                        [&](UUID pk) {
                                            return Mixed{pk};
                                        }},
                         payload.data.link.target);

        if (!target_table->find_primary_key(linked_pk)) {
            valid_payload = false;
        }
    }
    return valid_payload;
}

void InstructionApplier::operator()(const Instruction::SetInsert& instr)
{
    struct SetInsertResolver : public PathResolver {
        SetInsertResolver(InstructionApplier* applier, const Instruction::SetInsert& instr)
            : PathResolver(applier, instr, "SetInsert")
            , m_instr(instr)
        {
        }
        void on_set(SetBase& set) override
        {
            auto col = set.get_col_key();
            auto data_type = DataType(col.get_type());
            auto table = set.get_table();
            auto table_name = table->get_name();
            auto field_name = table->get_column_name(col);

            auto inserter = util::overload{
                [&](const ObjLink& link) {
                    if (data_type == type_TypedLink) {
                        REALM_ASSERT(dynamic_cast<Set<ObjLink>*>(&set));
                        auto& link_set = static_cast<Set<ObjLink>&>(set);
                        link_set.insert(link);
                    }
                    else if (data_type == type_Mixed) {
                        REALM_ASSERT(dynamic_cast<Set<Mixed>*>(&set));
                        auto& mixed_set = static_cast<Set<Mixed>&>(set);
                        mixed_set.insert(link);
                    }
                    else if (data_type == type_Link) {
                        REALM_ASSERT(dynamic_cast<Set<ObjKey>*>(&set));
                        auto& link_set = static_cast<Set<ObjKey>&>(set);
                        // Validate the target.
                        auto target_table = table->get_link_target(col);
                        if (target_table->get_key() != link.get_table_key()) {
                            m_applier->bad_transaction_log(
                                "SetInsert: Target table mismatch (expected '%1', got '%2')",
                                target_table->get_name(), table_name);
                        }
                        link_set.insert(link.get_obj_key());
                    }
                    else {
                        m_applier->bad_transaction_log(
                            "SetInsert: Type mismatch in set at '%2.%1' (expected link type, was %3)", field_name,
                            table_name, data_type);
                    }
                },
                [&](Mixed value) {
                    if (value.is_null() && !col.is_nullable()) {
                        m_applier->bad_transaction_log("SetInsert: NULL in non-nullable set '%2.%1'", field_name,
                                                       table_name);
                    }

                    if (data_type == type_Mixed || value.is_null() || value.get_type() == data_type) {
                        set.insert_any(value);
                    }
                    else {
                        m_applier->bad_transaction_log(
                            "SetInsert: Type mismatch in set at '%2.%1' (expected %3, got %4)", field_name,
                            table_name, data_type, value.get_type());
                    }
                },
                [&](const Instruction::Payload::ObjectValue&) {
                    m_applier->bad_transaction_log("SetInsert: Sets of embedded objects are not supported.");
                },
                [&](const Instruction::Payload::Dictionary&) {
                    m_applier->bad_transaction_log("SetInsert: Sets of dictionaries are not supported.");
                },
                [&](const Instruction::Payload::Erased&) {
                    m_applier->bad_transaction_log("SetInsert: Dictionary erase payload in SetInsert");
                },
            };

            m_applier->visit_payload(m_instr.value, inserter);
        }

    private:
        const Instruction::SetInsert& m_instr;
    };
    SetInsertResolver(this, instr).resolve();
}

void InstructionApplier::operator()(const Instruction::SetErase& instr)
{
    struct SetEraseResolver : public PathResolver {
        SetEraseResolver(InstructionApplier* applier, const Instruction::SetErase& instr)
            : PathResolver(applier, instr, "SetErase")
            , m_instr(instr)
        {
        }
        void on_set(SetBase& set) override
        {
            auto col = set.get_col_key();
            auto data_type = DataType(col.get_type());
            auto table = set.get_table();
            auto table_name = table->get_name();
            auto field_name = table->get_column_name(col);

            auto inserter = util::overload{
                [&](const ObjLink& link) {
                    if (data_type == type_TypedLink) {
                        REALM_ASSERT(dynamic_cast<Set<ObjLink>*>(&set));
                        auto& link_set = static_cast<Set<ObjLink>&>(set);
                        link_set.erase(link);
                    }
                    else if (data_type == type_Mixed) {
                        REALM_ASSERT(dynamic_cast<Set<Mixed>*>(&set));
                        auto& mixed_set = static_cast<Set<Mixed>&>(set);
                        mixed_set.erase(link);
                    }
                    else if (data_type == type_Link) {
                        REALM_ASSERT(dynamic_cast<Set<ObjKey>*>(&set));
                        auto& link_set = static_cast<Set<ObjKey>&>(set);
                        // Validate the target.
                        auto target_table = table->get_link_target(col);
                        if (target_table->get_key() != link.get_table_key()) {
                            m_applier->bad_transaction_log(
                                "SetErase: Target table mismatch (expected '%1', got '%2')", target_table->get_name(),
                                table_name);
                        }
                        link_set.erase(link.get_obj_key());
                    }
                    else {
                        m_applier->bad_transaction_log(
                            "SetErase: Type mismatch in set at '%2.%1' (expected link type, was %3)", field_name,
                            table_name, data_type);
                    }
                },
                [&](Mixed value) {
                    if (value.is_null() && !col.is_nullable()) {
                        m_applier->bad_transaction_log("SetErase: NULL in non-nullable set '%2.%1'", field_name,
                                                       table_name);
                    }

                    if (data_type == type_Mixed || value.get_type() == data_type) {
                        set.erase_any(value);
                    }
                    else {
                        m_applier->bad_transaction_log(
                            "SetErase: Type mismatch in set at '%2.%1' (expected %3, got %4)", field_name, table_name,
                            data_type, value.get_type());
                    }
                },
                [&](const Instruction::Payload::ObjectValue&) {
                    m_applier->bad_transaction_log("SetErase: Sets of embedded objects are not supported.");
                },
                [&](const Instruction::Payload::Dictionary&) {
                    m_applier->bad_transaction_log("SetErase: Sets of dictionaries are not supported.");
                },
                [&](const Instruction::Payload::Erased&) {
                    m_applier->bad_transaction_log("SetErase: Dictionary erase payload in SetErase");
                },
            };

            m_applier->visit_payload(m_instr.value, inserter);
        }

    private:
        const Instruction::SetErase& m_instr;
    };
    SetEraseResolver(this, instr).resolve();
}

StringData InstructionApplier::get_table_name(const Instruction::TableInstruction& instr,
                                              const std::string_view& name)
{
    if (auto class_name = m_log->try_get_string(instr.table)) {
        return Group::class_name_to_table_name(*class_name, m_table_name_buffer);
    }
    else {
        bad_transaction_log("Corrupt table name in %1 instruction", name);
    }
}

TableRef InstructionApplier::get_table(const Instruction::TableInstruction& instr, const std::string_view& name)
{
    if (instr.table == m_last_table_name) {
        return m_last_table;
    }
    else {
        auto table_name = get_table_name(instr, name);
        TableRef table = m_transaction.get_table(table_name);
        if (!table) {
            bad_transaction_log("%1: Table '%2' does not exist", name, table_name);
        }
        m_last_table = table;
        m_last_table_name = instr.table;
        m_last_object_key.reset();
        m_last_object.reset();
        m_last_field_name = InternString{};
        m_last_field = ColKey{};
        return table;
    }
}

util::Optional<Obj> InstructionApplier::get_top_object(const Instruction::ObjectInstruction& instr,
                                                       const std::string_view& name)
{
    if (m_last_table_name == instr.table && m_last_object_key && m_last_object &&
        *m_last_object_key == instr.object) {
        // We have already found the object, reuse it.
        return *m_last_object;
    }
    else {
        TableRef table = get_table(instr, name);
        ObjKey key = get_object_key(*table, instr.object, name);
        if (!key) {
            return util::none;
        }
        if (!table->is_valid(key)) {
            // Check if the object is deleted or is a tombstone.
            return util::none;
        }

        Obj obj = table->get_object(key);
        m_last_object_key = instr.object;
        m_last_object = obj;
        return obj;
    }
}

std::unique_ptr<LstBase> InstructionApplier::get_list_from_path(Obj& obj, ColKey col)
{
    // For link columns, `Obj::get_listbase_ptr()` always returns an instance whose concrete type is
    // `LnkLst`, which uses condensed indexes. However, we are interested in using non-condensed
    // indexes, so we need to manually construct a `Lst<ObjKey>` instead for lists of non-embedded
    // links.
    REALM_ASSERT(col.is_list());
    std::unique_ptr<LstBase> list;
    if (col.get_type() == col_type_Link || col.get_type() == col_type_LinkList) {
        auto table = obj.get_table();
        if (!table->get_link_target(col)->is_embedded()) {
            list = obj.get_list_ptr<ObjKey>(col);
        }
        else {
            list = obj.get_listbase_ptr(col);
        }
    }
    else {
        list = obj.get_listbase_ptr(col);
    }
    return list;
}

InstructionApplier::PathResolver::PathResolver(InstructionApplier* applier, const Instruction::PathInstruction& instr,
                                               const std::string_view& instr_name)
    : m_applier(applier)
    , m_path_instr(instr)
    , m_instr_name(instr_name)
{
}

InstructionApplier::PathResolver::~PathResolver()
{
    on_finish();
}

void InstructionApplier::PathResolver::on_property(Obj&, ColKey)
{
    m_applier->bad_transaction_log(util::format("Invalid path for %1 (object, column)", m_instr_name));
}

void InstructionApplier::PathResolver::on_list(LstBase&)
{
    m_applier->bad_transaction_log(util::format("Invalid path for %1 (list)", m_instr_name));
}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::on_list_index(LstBase&, uint32_t)
{
    m_applier->bad_transaction_log(util::format("Invalid path for %1 (list, index)", m_instr_name));
    return Status::DidNotResolve;
}

void InstructionApplier::PathResolver::on_dictionary(Dictionary&)
{
    m_applier->bad_transaction_log(util::format("Invalid path for %1 (dictionary, key)", m_instr_name));
}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::on_dictionary_key(Dictionary&, Mixed)
{
    m_applier->bad_transaction_log(util::format("Invalid path for %1 (dictionary, key)", m_instr_name));
    return Status::DidNotResolve;
}

void InstructionApplier::PathResolver::on_set(SetBase&)
{
    m_applier->bad_transaction_log(util::format("Invalid path for %1 (set)", m_instr_name));
}

void InstructionApplier::PathResolver::on_error(const std::string& err_msg)
{
    m_applier->bad_transaction_log(err_msg);
}

void InstructionApplier::PathResolver::on_column_advance(ColKey col)
{
    m_applier->m_last_field = col;
}

void InstructionApplier::PathResolver::on_dict_key_advance(StringData) {}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::on_list_index_advance(uint32_t)
{
    return Status::Pending;
}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::on_null_link_advance(StringData,
                                                                                                StringData)
{
    return Status::Pending;
}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::on_begin(const util::Optional<Obj>&)
{
    m_applier->m_current_path = m_path_instr.path;
    m_applier->m_last_field_name = m_path_instr.field;
    return Status::Pending;
}

void InstructionApplier::PathResolver::on_finish()
{
    m_applier->m_current_path.reset();
    m_applier->m_last_field_name = InternString{};
    m_applier->m_last_field = ColKey{};
}

StringData InstructionApplier::PathResolver::get_string(InternString interned)
{
    return m_applier->get_string(interned);
}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::resolve()
{
    util::Optional<Obj> obj = m_applier->get_top_object(m_path_instr, m_instr_name);
    Status begin_status = on_begin(obj);
    if (begin_status != Status::Pending) {
        return begin_status;
    }
    if (!obj) {
        m_applier->bad_transaction_log("%1: No such object: %3 in class '%2'", m_instr_name,
                                       format_pk(m_applier->m_log->get_key(m_path_instr.object)),
                                       get_string(m_path_instr.table));
    }

    m_it_begin = m_path_instr.path.begin();
    m_it_end = m_path_instr.path.end();
    Status status = resolve_field(*obj, m_path_instr.field);
    return status == Status::Pending ? Status::Success : status;
}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::resolve_field(Obj& obj, InternString field)
{
    auto field_name = get_string(field);
    ColKey col = obj.get_table()->get_column_key(field_name);
    if (!col) {
        on_error(util::format("%1: No such field: '%2' in class '%3'", m_instr_name, field_name,
                              obj.get_table()->get_name()));
        return Status::DidNotResolve;
    }
    on_column_advance(col);

    if (m_it_begin == m_it_end) {
        if (col.is_list()) {
            auto list = obj.get_listbase_ptr(col);
            on_list(*list);
        }
        else if (col.is_dictionary()) {
            auto dict = obj.get_dictionary(col);
            on_dictionary(dict);
        }
        else if (col.is_set()) {
            SetBasePtr set;
            if (col.get_type() == col_type_Link) {
                // We are interested in using non-condensed indexes - as for Lists below
                set = obj.get_set_ptr<ObjKey>(col);
            }
            else {
                set = obj.get_setbase_ptr(col);
            }
            on_set(*set);
        }
        else {
            on_property(obj, col);
        }
        return Status::Pending;
    }

    if (col.is_list()) {
        if (auto pindex = mpark::get_if<uint32_t>(&*m_it_begin)) {
            std::unique_ptr<LstBase> list = InstructionApplier::get_list_from_path(obj, col);
            ++m_it_begin;
            return resolve_list_element(*list, *pindex);
        }
        on_error(util::format("%1: List index is not an integer on field '%2' in class '%3'", m_instr_name,
                              field_name, obj.get_table()->get_name()));
    }
    else if (col.is_dictionary()) {
        if (auto pkey = mpark::get_if<InternString>(&*m_it_begin)) {
            auto dict = obj.get_dictionary(col);
            ++m_it_begin;
            return resolve_dictionary_element(dict, *pkey);
        }
        on_error(util::format("%1: Dictionary key is not a string on field '%2' in class '%3'", m_instr_name,
                              field_name, obj.get_table()->get_name()));
    }
    else if (col.get_type() == col_type_Link) {
        auto target = obj.get_table()->get_link_target(col);
        if (!target->is_embedded()) {
            on_error(util::format("%1: Reference through non-embedded link in field '%2' in class '%3'", m_instr_name,
                                  field_name, obj.get_table()->get_name()));
        }
        else if (obj.is_null(col)) {
            Status null_status =
                on_null_link_advance(obj.get_table()->get_name(), obj.get_table()->get_column_name(col));
            if (null_status != Status::Pending) {
                return null_status;
            }
            on_error(util::format("%1: Reference through NULL embedded link in field '%2' in class '%3'",
                                  m_instr_name, field_name, obj.get_table()->get_name()));
        }
        else if (auto pfield = mpark::get_if<InternString>(&*m_it_begin)) {
            auto embedded_object = obj.get_linked_object(col);
            ++m_it_begin;
            return resolve_field(embedded_object, *pfield);
        }
        else {
            on_error(util::format("%1: Embedded object field reference is not a string", m_instr_name));
        }
    }
    else {
        on_error(util::format("%1: Resolving path through unstructured field '%3.%2' of type %4", m_instr_name,
                              field_name, obj.get_table()->get_name(), col.get_type()));
    }
    return Status::DidNotResolve;
}

InstructionApplier::PathResolver::Status InstructionApplier::PathResolver::resolve_list_element(LstBase& list,
                                                                                                uint32_t index)
{
    if (m_it_begin == m_it_end) {
        return on_list_index(list, index);
    }

    auto col = list.get_col_key();
    auto field_name = list.get_table()->get_column_name(col);

    if (col.get_type() == col_type_LinkList) {
        auto target = list.get_table()->get_link_target(col);
        if (!target->is_embedded()) {
            on_error(util::format("%1: Reference through non-embedded link at '%3.%2[%4]'", m_instr_name, field_name,
                                  list.get_table()->get_name(), index));
            return Status::DidNotResolve;
        }

        Status list_status = on_list_index_advance(index);
        if (list_status != Status::Pending) {
            return list_status;
        }

        REALM_ASSERT(dynamic_cast<LnkLst*>(&list));
        auto& link_list = static_cast<LnkLst&>(list);
        if (index >= link_list.size()) {
            on_error(util::format("%1: Out-of-bounds index through list at '%3.%2[%4]'", m_instr_name, field_name,
                                  list.get_table()->get_name(), index));
        }
        else if (auto pfield = mpark::get_if<InternString>(&*m_it_begin)) {
            auto embedded_object = link_list.get_object(index);
            ++m_it_begin;
            return resolve_field(embedded_object, *pfield);
        }
        on_error(util::format("%1: Embedded object field reference is not a string", m_instr_name));
    }
    else {
        on_error(util::format(
            "%1: Resolving path through unstructured list element on '%3.%2', which is a list of type '%4'",
            m_instr_name, field_name, list.get_table()->get_name(), col.get_type()));
    }
    return Status::DidNotResolve;
}

InstructionApplier::PathResolver::Status
InstructionApplier::PathResolver::resolve_dictionary_element(Dictionary& dict, InternString key)
{
    StringData string_key = get_string(key);
    if (m_it_begin == m_it_end) {
        return on_dictionary_key(dict, Mixed{string_key});
    }

    on_dict_key_advance(string_key);

    auto col = dict.get_col_key();
    auto table = dict.get_table();
    auto field_name = table->get_column_name(col);

    if (col.get_type() == col_type_Link) {
        auto target = dict.get_target_table();
        if (!target->is_embedded()) {
            on_error(util::format("%1: Reference through non-embedded link at '%3.%2[%4]'", m_instr_name, field_name,
                                  table->get_name(), string_key));
            return Status::DidNotResolve;
        }

        auto embedded_object = dict.get_object(string_key);
        if (!embedded_object) {
            Status null_link_status = on_null_link_advance(table->get_name(), string_key);
            if (null_link_status != Status::Pending) {
                return null_link_status;
            }
            on_error(util::format("%1: Unmatched key through dictionary at '%3.%2[%4]'", m_instr_name, field_name,
                                  table->get_name(), string_key));
        }
        else if (auto pfield = mpark::get_if<InternString>(&*m_it_begin)) {
            ++m_it_begin;
            return resolve_field(embedded_object, *pfield);
        }
        else {
            on_error(util::format("%1: Embedded object field reference is not a string", m_instr_name));
        }
    }
    else {
        on_error(
            util::format("%1: Resolving path through non link element on '%3.%2', which is a dictionary of type '%4'",
                         m_instr_name, field_name, table->get_name(), col.get_type()));
    }
    return Status::DidNotResolve;
}


ObjKey InstructionApplier::get_object_key(Table& table, const Instruction::PrimaryKey& primary_key,
                                          const std::string_view& name) const
{
    StringData table_name = table.get_name();
    ColKey pk_col = table.get_primary_key_column();
    StringData pk_name = "";
    DataType pk_type;
    if (pk_col) {
        pk_name = table.get_column_name(pk_col);
        pk_type = table.get_column_type(pk_col);
    }
    return mpark::visit(
        util::overload{
            [&](mpark::monostate) {
                if (!pk_col) {
                    bad_transaction_log(
                        "%1 instruction with NULL primary key, but table '%2' does not have a primary key column",
                        name, table_name);
                }
                if (!table.is_nullable(pk_col)) {
                    bad_transaction_log("%1 instruction with NULL primary key, but column '%2.%3' is not nullable",
                                        name, table_name, pk_name);
                }

                ObjKey key = table.get_objkey_from_primary_key(realm::util::none);
                return key;
            },
            [&](int64_t pk) {
                if (!pk_col) {
                    bad_transaction_log("%1 instruction with integer primary key (%2), but table '%3' does not have "
                                        "a primary key column",
                                        name, pk, table_name);
                }
                if (pk_type != type_Int) {
                    bad_transaction_log(
                        "%1 instruction with integer primary key (%2), but '%3.%4' has primary keys of type '%5'",
                        name, pk, table_name, pk_name, pk_type);
                }
                ObjKey key = table.get_objkey_from_primary_key(pk);
                return key;
            },
            [&](InternString interned_pk) {
                auto pk = get_string(interned_pk);
                if (!pk_col) {
                    bad_transaction_log("%1 instruction with string primary key (\"%2\"), but table '%3' does not "
                                        "have a primary key column",
                                        name, pk, table_name);
                }
                if (pk_type != type_String) {
                    bad_transaction_log(
                        "%1 instruction with string primary key (\"%2\"), but '%3.%4' has primary keys of type '%5'",
                        name, pk, table_name, pk_name, pk_type);
                }
                ObjKey key = table.get_objkey_from_primary_key(pk);
                return key;
            },
            [&](GlobalKey id) {
                if (pk_col) {
                    bad_transaction_log(
                        "%1 instruction without primary key, but table '%2' has a primary key column of type %3",
                        name, table_name, pk_type);
                }
                ObjKey key = table.get_objkey_from_global_key(id);
                return key;
            },
            [&](ObjectId pk) {
                if (!pk_col) {
                    bad_transaction_log("%1 instruction with ObjectId primary key (\"%2\"), but table '%3' does not "
                                        "have a primary key column",
                                        name, pk, table_name);
                }
                if (pk_type != type_ObjectId) {
                    bad_transaction_log(
                        "%1 instruction with ObjectId primary key (%2), but '%3.%4' has primary keys of type '%5'",
                        name, pk, table_name, pk_name, pk_type);
                }
                ObjKey key = table.get_objkey_from_primary_key(pk);
                return key;
            },
            [&](UUID pk) {
                if (!pk_col) {
                    bad_transaction_log("%1 instruction with UUID primary key (\"%2\"), but table '%3' does not "
                                        "have a primary key column",
                                        name, pk, table_name);
                }
                if (pk_type != type_UUID) {
                    bad_transaction_log(
                        "%1 instruction with UUID primary key (%2), but '%3.%4' has primary keys of type '%5'", name,
                        pk, table_name, pk_name, pk_type);
                }
                ObjKey key = table.get_objkey_from_primary_key(pk);
                return key;
            }},
        primary_key);
}


} // namespace realm::sync
