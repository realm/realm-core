#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/object.hpp>

#include <realm/group.hpp>

namespace realm::sync {

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
    TableNameBuffer buffer;
    return m_transaction.get_table(class_name_to_table_name(class_name, buffer));
}

void InstructionApplier::operator()(const Instruction::AddTable& instr)
{
    auto table_name = get_table_name(instr);

    auto add_table = util::overloaded{
        [&](const Instruction::AddTable::PrimaryKeySpec& spec) {
            if (spec.type == Instruction::Payload::Type::GlobalKey) {
                log("sync::create_table(group, \"%1\");", table_name);
                sync::create_table(m_transaction, table_name);
            }
            else {
                if (!is_valid_key_type(spec.type)) {
                    bad_transaction_log("Invalid primary key type");
                }
                DataType pk_type = get_data_type(spec.type);
                StringData pk_field = get_string(spec.field);
                bool nullable = spec.nullable;
                log("sync::create_table_with_primary_key(group, \"%1\", %2, \"%3\", %4);", table_name, pk_type,
                    pk_field, nullable);
                sync::create_table_with_primary_key(m_transaction, table_name, pk_type, pk_field, nullable);
            }
        },
        [&](const Instruction::AddTable::EmbeddedTable&) {
            log("group.add_embedded_table(\"%1\");", table_name);
            m_transaction.add_embedded_table(table_name);
        },
    };

    mpark::visit(std::move(add_table), instr.type);

    m_table_info_cache.clear();
}

void InstructionApplier::operator()(const Instruction::EraseTable& instr)
{
    auto table_name = get_table_name(instr);

    if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_transaction.has_table(table_name)))) {
        // FIXME: Should EraseTable be considered idempotent?
        bad_transaction_log("table does not exist");
    }

    log("sync::erase_table(m_group, \"%1\")", table_name);
    sync::erase_table(m_transaction, m_table_info_cache, table_name);
    m_table_info_cache.clear();
}

void InstructionApplier::operator()(const Instruction::CreateObject& instr)
{
    auto table = get_table(instr);
    ColKey pk_col = table->get_primary_key_column();

    mpark::visit(
        util::overloaded{
            [&](mpark::monostate) {
                if (!pk_col) {
                    bad_transaction_log("CreateObject(NULL) on table without a primary key");
                }
                if (!table->is_nullable(pk_col)) {
                    bad_transaction_log("CreateObject(NULL) on a table with a non-nullable primary key");
                }
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), realm::util::none);",
                    table->get_name());
                sync::create_object_with_primary_key(m_table_info_cache, *table, util::none);
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
                sync::create_object_with_primary_key(m_table_info_cache, *table, pk);
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
                sync::create_object_with_primary_key(m_table_info_cache, *table, str);
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
                table->create_object_with_primary_key(id);
            },
            [&](GlobalKey key) {
                if (pk_col) {
                    bad_transaction_log("CreateObject(GlobalKey) on table with a primary key");
                }
                log("sync::create_object_with_primary_key(group, get_table(\"%1\"), GlobalKey{%2, %3});",
                    table->get_name(), key, key.hi(), key.lo());
                sync::create_object(m_table_info_cache, *table, key);
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
    m_table_info_cache.clear_last_object(*m_last_table);
}

template <class F>
void InstructionApplier::set_value(const SetTargetInfo& info, const Instruction::Payload& value, F&& setter,
                                   const char* name)
{
    using Type = Instruction::Payload::Type;

    const auto& data = value.data;
    switch (value.type) {
        case Type::ObjectValue: {
            if (!info.is_embedded_link) {
                bad_transaction_log("%1(Object) on a property that does not contain embedded objects");
            }
            setter(Instruction::Payload::ObjectValue{});
            return;
        }
        case Type::GlobalKey: {
            bad_transaction_log("%1(GlobalKey) is not allowed", name);
        }
        case Type::Null: {
            if (!info.nullable) {
                bad_transaction_log("%1(NULL) on '%2.%3', which is not nullable", name, info.table_name,
                                    info.col_name);
            }
            setter(realm::util::none);
            return;
        }
        case Type::Int: {
            if (info.type != type_Int) {
                bad_transaction_log("%1(Int) on '%2.%3' of type %4", name, info.table_name, info.col_name, info.type);
            }
            setter(data.integer);
            return;
        }
        case Type::Bool: {
            if (info.type != type_Bool) {
                bad_transaction_log("%1(Bool) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            setter(data.boolean);
            return;
        }
        case Type::String: {
            if (info.type != type_String) {
                bad_transaction_log("%1(String) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            StringData str = get_string(data.str);
            setter(str);
            return;
        }
        case Type::Binary: {
            if (info.type != type_Binary) {
                bad_transaction_log("%1(Binary) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            BinaryData bin = get_binary(data.binary);
            setter(bin);
            return;
        }
        case Type::Timestamp: {
            if (info.type != type_Timestamp) {
                bad_transaction_log("%1(Timestamp) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            setter(data.timestamp);
            return;
        }
        case Type::Float: {
            if (info.type != type_Float) {
                bad_transaction_log("%1(Float) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            setter(data.fnum);
            return;
        }
        case Type::Double: {
            if (info.type != type_Double) {
                bad_transaction_log("%1(Double) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            setter(data.dnum);
            return;
        }
        case Type::Decimal: {
            if (info.type != type_Decimal) {
                bad_transaction_log("%1(Decimal) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            setter(data.decimal);
            return;
        }
        case Type::Link: {
            if (info.type != type_Link) {
                bad_transaction_log("Set(Link) on '%2.%3' of type %4", name, info.table_name, info.col_name,
                                    info.type);
            }
            TableNameBuffer buffer;
            StringData class_name = get_string(data.link.target_table);
            StringData target_table_name = class_name_to_table_name(class_name, buffer);
            TableRef target_table = m_transaction.get_table(target_table_name);
            if (!target_table) {
                bad_transaction_log("Set(Link) with invalid target table '%1'", target_table_name);
            }
            // FIXME: This needs adjustment for embedded objects.
            ColKey source_col = m_last_table->get_column_key(info.col_name);
            TableRef expected_target_table = m_last_table->get_link_target(source_col);
            if (expected_target_table != target_table) {
                bad_transaction_log("Set(Link) with unexpected target table '%1' (expected '%2')", target_table_name,
                                    expected_target_table->get_name());
            }
            ObjKey target = get_object_key(*target_table, data.link.target, "Set(Link)");
            setter(target);
            return;
        }
        case Type::TypedLink: {
            // FIXME
            return;
        }
        case Type::Mixed: {
            // FIXME
            return;
        }
        case Type::ObjectId: {
            if (info.type != type_ObjectId) {
                bad_transaction_log("Set(ObjectId) on column '%1.%2' of type %3", info.table_name, info.col_name,
                                    info.type);
            }
            setter(data.object_id);
            return;
        }
    }
}


void InstructionApplier::operator()(const Instruction::Set& instr)
{
    if (!instr.is_array_set()) {
        auto path = get_field(instr, "Set");
        // FIXME: Would use structured bindings, but they cannot be captured by lamdas.
        auto obj = std::move(std::get<0>(path));
        auto col = std::move(std::get<1>(path));

        auto table = obj.get_table();
        SetTargetInfo info;
        info.table_name = table->get_name();
        info.col_name = table->get_column_name(col);
        info.type = table->get_column_type(col);
        info.nullable = table->is_nullable(col);
        info.is_embedded_link = (info.type == type_Link && table->get_link_target(col)->is_embedded());

        auto setter = util::overloaded{
            [&](const util::None&) {
                obj.set_null(col, instr.is_default);
            },
            [&](const Instruction::Payload::ObjectValue&) {
                // FIXME: Embedded object creation is not idempotent in Core.
                if (obj.is_null(col)) {
                    obj.create_and_set_linked_object(col);
                }
            },
            [&](const auto& val) {
                obj.set(col, val, instr.is_default);
            },
        };

        set_value(info, instr.value, std::move(setter), "Set");
    }
    else {
        auto& list = get_list(instr, "ArraySet");
        auto table = list.get_table();
        ColKey col = list.get_col_key();
        SetTargetInfo info;
        info.table_name = table->get_name();
        info.col_name = table->get_column_name(col);
        info.type = table->get_column_type(col);
        info.nullable = table->is_nullable(col);

        if (info.type == type_LinkList) {
            info.type = type_Link;
        }

        info.is_embedded_link = (info.type == type_Link && table->get_link_target(col)->is_embedded());

        size_t ndx = instr.index();

        if (ndx >= list.size()) {
            bad_transaction_log("Set out of bounds on list '%1.%2' (%3 >= %4)", info.table_name, info.col_name, ndx,
                                list.size());
        }

        auto setter = util::overloaded{
            [&](const util::None&) {
                list.set_null(ndx);
            },
            [&](const Instruction::Payload::ObjectValue&) {
                // Embedded object creation is idempotent, and link lists cannot
                // contain nulls, so this is a no-op.
            },
            [&](const auto& val) {
                using type = std::remove_cv_t<std::remove_reference_t<decltype(val)>>;
                auto& lst = static_cast<Lst<type>&>(list);
                lst.set(ndx, val);
            },
        };

        set_value(info, instr.value, std::move(setter), "ArraySet");
    }
}

void InstructionApplier::operator()(const Instruction::AddInteger& instr)
{
    auto [obj, col] = get_field(instr, "AddInteger");

    TableRef table = obj.get_table();
    auto type = table->get_column_type(col);
    if (type != type_Int) {
        bad_transaction_log("AddInteger on column '%1.%2' of type %3", table->get_name(), table->get_column_name(col),
                            type);
    }

    if (obj.is_null(col)) {
        // AddInteger on NULL is a no-op.
        return;
    }
    obj.add_int(col, instr.value);
}

void InstructionApplier::operator()(const Instruction::AddColumn& instr)
{
    using Type = Instruction::Payload::Type;

    auto table = get_table(instr, "AddColumn");
    auto col_name = get_string(instr.field);

    if (table->get_column_key(col_name)) {
        bad_transaction_log("AddColumn '%1.%3' which already exists", table->get_name(), col_name);
    }

    if (instr.type != Type::Link) {
        DataType type = get_data_type(instr.type);
        if (instr.list) {
            table->add_column_list(type, col_name, instr.nullable);
        }
        else {
            table->add_column(type, col_name, instr.nullable);
        }
    }
    else {
        TableNameBuffer buffer;
        auto target_table_name = class_name_to_table_name(get_string(instr.link_target_table), buffer);
        TableRef target = m_transaction.get_table(target_table_name);
        if (!target) {
            bad_transaction_log("AddColumn(Link) '%1.%2' to table '%3' which doesn't exist", table->get_name(),
                                col_name, target_table_name);
        }
        DataType type = instr.list ? type_LinkList : type_Link;
        table->add_column_link(type, col_name, *target);
    }
}

void InstructionApplier::operator()(const Instruction::EraseColumn& instr)
{
    auto table = get_table(instr, "EraseColumn");
    auto col_name = get_string(instr.field);

    ColKey col = table->get_column_key(col_name);
    if (!col) {
        bad_transaction_log("EraseColumn '%1.%2' which doesn't exist");
    }

    table->remove_column(col);
}

void InstructionApplier::operator()(const Instruction::ArrayInsert& instr)
{
    auto& list = get_list(instr, "ArrayInsert");

    if (instr.index() > list.size()) {
        bad_transaction_log("ArrayInsert out of bounds");
    }

    auto table = list.get_table();
    ColKey col = list.get_col_key();
    SetTargetInfo info;
    info.table_name = table->get_name();
    info.col_name = table->get_column_name(col);
    info.type = table->get_column_type(col);
    info.nullable = table->is_nullable(col);

    if (info.type == type_LinkList) {
        info.type = type_Link;
    }

    info.is_embedded_link = (info.type == type_Link && table->get_link_target(col)->is_embedded());

    auto setter = util::overloaded{
        [&](const util::None&) {
            list.insert_null(instr.index());
        },
        [&](const Instruction::Payload::ObjectValue&) {
            auto& lst = static_cast<LnkLst&>(list);
            lst.create_and_insert_linked_object(instr.index());
        },
        [&](const auto& value) {
            using type = std::remove_cv_t<std::remove_reference_t<decltype(value)>>;
            auto& lst = static_cast<Lst<type>&>(list);
            lst.insert(instr.index(), value);
        },
    };
    set_value(info, instr.value, std::move(setter), "ArrayInsert");
}

void InstructionApplier::operator()(const Instruction::ArrayMove& instr)
{
    auto& list = get_list(instr, "ArrayMove");

    if (instr.index() >= list.size()) {
        bad_transaction_log("ArrayMove from out of bounds (%1 >= %2)", instr.index(), list.size());
    }
    if (instr.ndx_2 >= list.size()) {
        bad_transaction_log("ArrayMove to out of bounds (%1 >= %2)", instr.ndx_2, list.size());
    }
    if (instr.index() == instr.ndx_2) {
        // FIXME: Does this really need to be an error?
        bad_transaction_log("ArrayMove to same location (%1)", instr.index());
    }

    list.move(instr.index(), instr.ndx_2);
}

void InstructionApplier::operator()(const Instruction::ArrayErase& instr)
{
    auto& list = get_list(instr, "ArrayErase");

    if (instr.index() >= list.size()) {
        bad_transaction_log("ArrayErase out of bounds (%1 >= %2)", instr.index(), list.size());
    }

    list.remove(instr.index(), instr.index() + 1);
}

void InstructionApplier::operator()(const Instruction::ArrayClear& instr)
{
    auto& list = get_list(instr, "ArrayClear");

    list.clear();
}

StringData InstructionApplier::get_table_name(const Instruction::TableInstruction& instr, const char* name)
{
    if (auto class_name = m_log->try_get_string(instr.table)) {
        return class_name_to_table_name(*class_name, m_table_name_buffer);
    }
    else {
        bad_transaction_log("Corrupt table name in %1 instruction", name);
    }
}

TableRef InstructionApplier::get_table(const Instruction::TableInstruction& instr, const char* name)
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

util::Optional<Obj> InstructionApplier::get_top_object(const Instruction::ObjectInstruction& instr, const char* name)
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

std::tuple<Obj, ColKey> InstructionApplier::get_field(const Instruction::PathInstruction& instr, const char* name)
{
    // First, get the top-level object.
    Obj obj;
    if (auto o = get_top_object(instr, name)) {
        obj = std::move(*o);
    }
    else {
        bad_transaction_log("%1: No such object: %3 in class '%2'", name, get_string(instr.table),
                            format_pk(m_log->get_key(instr.object)));
    }

    // Find the column corresponding to the field in the path instruction.
    ColKey col;
    StringData field_name;
    if (instr.table == m_last_table_name) {
        if (instr.field == m_last_field_name) {
            col = m_last_field;
        }
        else {
            // The last table is unchanged, so we can safely update the m_last_field cache.
            field_name = get_string(instr.field);
            col = obj.get_table()->get_column_key(field_name);
            m_last_field_name = instr.field;
            m_last_field = col;
        }
    }
    else {
        // Last table does not match, so just get the column without updating any caches.
        field_name = get_string(instr.field);
        col = obj.get_table()->get_column_key(field_name);
    }

    if (!col) {
        bad_transaction_log("%1: No such field '%2.%3'", name, obj.get_table()->get_name(), field_name);
    }

    // At this point, 'obj' is the top-level object. Now descend through the path.
    //
    // FIXME: This should be refactored into something less horrible.
    for (size_t i = 0; i < instr.path.size(); ++i) {
        // If the next path element is a string, expect the current field to be
        // an embedded link.
        if (auto pfield = mpark::get_if<InternString>(&instr.path[i])) {
            if (col.get_type() == col_type_Link) {
                if (obj.is_null(col)) {
                    // FIXME: We can consider whether addressing through a NULL
                    // embedded object should implicitly create it. This is the
                    // behavior of MongoDB dotted paths.
                    bad_transaction_log("%1: Invalid path (embedded object is NULL)", name);
                }
                obj = obj.get_linked_object(col);
                TableRef tbl = obj.get_table();
                if (!tbl->is_embedded()) {
                    bad_transaction_log("%1: Invalid path (link is not embedded '%2.%3')", name,
                                        obj.get_table()->get_name(), obj.get_table()->get_column_name(col));
                }
                StringData col_name = get_string(*pfield);
                col = tbl->get_column_key(col_name);
                if (!col) {
                    bad_transaction_log("%1: Invalid path (no such property on embedded table '%2.%3')", name,
                                        tbl->get_name(), col_name);
                }
            }
            else {
                bad_transaction_log("%1: Invalid path (not an embedded object '%2.%3')", name,
                                    obj.get_table()->get_name(), obj.get_table()->get_column_name(col));
            }
        }
        else {
            size_t remaining = instr.path.size() - i - 1;
            if (remaining == 0) {
                // This was the last element of the path, and it was an
                // index. Just return the list field.
                break;
            }

            // The next path element is an index, and there are more elements,
            // so we expect the current field to be a list of embedded objects.
            uint32_t index = mpark::get<uint32_t>(instr.path[i]);
            if (col.get_type() == col_type_LinkList) {
                // We have an index, and further path elements — descend into the list.
                LnkLst lst = obj.get_linklist(col);
                if (index >= lst.size()) {
                    bad_transaction_log("%1: Invalid path (index out of bounds on '%2.%3')", name,
                                        obj.get_table()->get_name(), obj.get_table()->get_column_name(col));
                }
                obj = lst.get_object(index);
                TableRef tbl = obj.get_table();
                if (!tbl->is_embedded()) {
                    bad_transaction_log("%1: Invalid path (link list is not embedded '%2.%3')", name,
                                        obj.get_table()->get_name(), obj.get_table()->get_column_name(col));
                }
                if (auto psubfield = mpark::get_if<InternString>(&instr.path[i + 1])) {
                    ++i; // Skip over the index.
                    StringData col_name = get_string(*psubfield);
                    col = tbl->get_column_key(col_name);
                    if (!col) {
                        bad_transaction_log("%1: Invalid path (no such property on embedded table '%2.%3')", name,
                                            tbl->get_name(), col_name);
                    }
                }
                else {
                    bad_transaction_log("%1: Invalid path (arrays of arrays are not supported)", name);
                }
            }
            else {
                bad_transaction_log("%1: Invalid path (index into '%2.%3', which is not a list of embedded objects)",
                                    name, obj.get_table()->get_name(), obj.get_table()->get_column_name(col));
            }
        }
    }

    // This assertion checks that the col key belongs to the table in a
    // roundabout way.
    REALM_ASSERT(!obj.get_table()->get_column_name(col).is_null());

    return std::make_tuple(std::move(obj), col);
}

LstBase& InstructionApplier::get_list(const Instruction::PathInstruction& instr, const char* name)
{
    // Note: get_field() returns the last object field in the path, which is to
    // say that if the last element of the path is an index, it will not be
    // traversed.
    auto [obj, col] = get_field(instr, name);

    // Note: Since `get_field()` may return an embedded object (for which we
    // don't set m_last_table), we have to find the table through the object.
    //
    // FIXME: Cache list pointers.
    auto table = obj.get_table();
    if (!table->is_list(col)) {
        bad_transaction_log("%1: '%2.%3' is not a list", name, table->get_name(), table->get_column_name(col));
    }

    // FIXME: All of the `get_list` methods on `Obj` return a pointer to a
    // `LnkLst`, and never a `Lst<ObjKey>`, which means we get condensed indices
    // rather than uncondensed. For embedded objects, however, we actually want
    // the condensed interface.
    if (col.get_type() == col_type_LinkList && !table->get_link_target(col)->is_embedded()) {
        m_last_list = std::make_unique<Lst<ObjKey>>(obj, col);
    }
    else {
        m_last_list = obj.get_listbase_ptr(col);
    }

    return *m_last_list;
}

ObjKey InstructionApplier::get_object_key(Table& table, const Instruction::PrimaryKey& primary_key,
                                          const char* name) const
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
        util::overloaded{
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
            }},
        primary_key);
}


} // namespace realm::sync
