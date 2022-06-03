#include <realm/sync/instruction_replication.hpp>
#include <realm/transaction.hpp>
#include <realm/sync/transform.hpp> // TransformError
#include <realm/list.hpp>

namespace realm {
namespace sync {

void SyncReplication::reset()
{
    m_encoder.reset();

    m_last_table = nullptr;
    m_last_object = ObjKey();
    m_last_field = ColKey();
    m_last_class_name = InternString::npos;
    m_last_primary_key = Instruction::PrimaryKey();
    m_last_field_name = InternString::npos;
}

void SyncReplication::do_initiate_transact(Group& group, version_type current_version, bool history_updated)
{
    Replication::do_initiate_transact(group, current_version, history_updated);
    m_transaction = dynamic_cast<Transaction*>(&group); // FIXME: Is this safe?
    m_write_validator = make_write_validator(*m_transaction);
    reset();
}

Instruction::Payload SyncReplication::as_payload(Mixed value)
{
    if (value.is_null()) {
        return Instruction::Payload{};
    }

    switch (value.get_type()) {
        case type_Int: {
            return Instruction::Payload{value.get<int64_t>()};
        }
        case type_Bool: {
            return Instruction::Payload{value.get<bool>()};
        }
        case type_Float: {
            return Instruction::Payload{value.get<float>()};
        }
        case type_Double: {
            return Instruction::Payload{value.get<double>()};
        }
        case type_String: {
            auto str = value.get<StringData>();
            auto range = m_encoder.add_string_range(str);
            return Instruction::Payload{range};
        }
        case type_Binary: {
            auto binary = value.get<BinaryData>();
            auto range = m_encoder.add_string_range(StringData{binary.data(), binary.size()});
            const bool is_binary = true;
            return Instruction::Payload{range, is_binary};
        }
        case type_Timestamp: {
            return Instruction::Payload{value.get<Timestamp>()};
        }
        case type_Decimal: {
            return Instruction::Payload{value.get<Decimal128>()};
        }
        case type_ObjectId: {
            return Instruction::Payload{value.get<ObjectId>()};
        }
        case type_UUID: {
            return Instruction::Payload{value.get<UUID>()};
        }
        case type_TypedLink:
            [[fallthrough]];
        case type_Link: {
            REALM_TERMINATE("as_payload() needs table/collection for links");
            break;
        }
        case type_Mixed:
            [[fallthrough]];
        case type_LinkList: {
            REALM_TERMINATE("Invalid payload type");
            break;
        }
    }
    return Instruction::Payload{};
}

Instruction::Payload SyncReplication::as_payload(const CollectionBase& collection, Mixed value)
{
    return as_payload(*collection.get_table(), collection.get_col_key(), value);
}

Instruction::Payload SyncReplication::as_payload(const Table& table, ColKey col_key, Mixed value)
{
    if (value.is_null()) {
        // FIXME: `Mixed::get_type()` asserts on null.
        return Instruction::Payload{};
    }

    if (value.is_type(type_Link)) {
        ConstTableRef target_table = table.get_link_target(col_key);
        if (target_table->is_embedded()) {
            // FIXME: Include target table name to support Mixed of Embedded Objects.
            return Instruction::Payload::ObjectValue{};
        }

        Instruction::Payload::Link link;
        link.target_table = emit_class_name(*target_table);
        link.target = primary_key_for_object(*target_table, value.get<ObjKey>());
        return Instruction::Payload{link};
    }
    else if (value.is_type(type_TypedLink)) {
        auto obj_link = value.get<ObjLink>();
        ConstTableRef target_table = m_transaction->get_table(obj_link.get_table_key());
        REALM_ASSERT(target_table);

        if (target_table->is_embedded()) {
            ConstTableRef static_target_table = table.get_link_target(col_key);

            if (static_target_table != target_table)
                REALM_TERMINATE("Dynamically typed embedded objects not supported yet.");
            return Instruction::Payload::ObjectValue{};
        }

        Instruction::Payload::Link link;
        link.target_table = emit_class_name(*target_table);
        link.target = primary_key_for_object(*target_table, obj_link.get_obj_key());
        return Instruction::Payload{link};
    }
    else {
        return as_payload(value);
    }
}

InternString SyncReplication::emit_class_name(StringData table_name)
{
    return m_encoder.intern_string(Group::table_name_to_class_name(table_name));
}

InternString SyncReplication::emit_class_name(const Table& table)
{
    return emit_class_name(table.get_name());
}

Instruction::Payload::Type SyncReplication::get_payload_type(DataType type) const
{
    using Type = Instruction::Payload::Type;
    switch (type) {
        case type_Int:
            return Type::Int;
        case type_Bool:
            return Type::Bool;
        case type_String:
            return Type::String;
        case type_Binary:
            return Type::Binary;
        case type_Timestamp:
            return Type::Timestamp;
        case type_Float:
            return Type::Float;
        case type_Double:
            return Type::Double;
        case type_Decimal:
            return Type::Decimal;
        case type_Link:
            return Type::Link;
        case type_LinkList:
            return Type::Link;
        case type_TypedLink:
            return Type::Link;
        case type_ObjectId:
            return Type::ObjectId;
        case type_UUID:
            return Type::UUID;
        case type_Mixed:
            return Type::Null;
    }
    unsupported_instruction();
    return Type::Int; // Make compiler happy
}

void SyncReplication::add_class(TableKey tk, StringData name, Table::Type table_type)
{
    Replication::add_class(tk, name, table_type);

    bool is_class = m_transaction->table_is_public(tk);

    if (is_class && !m_short_circuit) {
        Instruction::AddTable instr;
        instr.table = emit_class_name(name);
        if (table_type == Table::Type::Embedded) {
            instr.type = Instruction::AddTable::EmbeddedTable{};
        }
        else {
            auto field = m_encoder.intern_string(""); // FIXME: Should this be "_id"?
            const bool is_nullable = false;
            bool is_asymmetric = (table_type == Table::Type::TopLevelAsymmetric);
            instr.type = Instruction::AddTable::TopLevelTable{
                field,
                Instruction::Payload::Type::GlobalKey,
                is_nullable,
                is_asymmetric,
            };
        }
        emit(instr);
    }
}

void SyncReplication::add_class_with_primary_key(TableKey tk, StringData name, DataType pk_type, StringData pk_field,
                                                 bool nullable, Table::Type table_type)
{
    Replication::add_class_with_primary_key(tk, name, pk_type, pk_field, nullable, table_type);

    bool is_class = m_transaction->table_is_public(tk);

    if (is_class && !m_short_circuit) {
        Instruction::AddTable instr;
        instr.table = emit_class_name(name);
        auto field = m_encoder.intern_string(pk_field);
        auto is_asymmetric = (table_type == Table::Type::TopLevelAsymmetric);
        auto spec = Instruction::AddTable::TopLevelTable{field, get_payload_type(pk_type), nullable, is_asymmetric};
        if (!is_valid_key_type(spec.pk_type)) {
            unsupported_instruction();
        }
        instr.type = std::move(spec);
        emit(instr);
    }
}

void SyncReplication::create_object(const Table* table, GlobalKey oid)
{
    if (table->is_embedded()) {
        unsupported_instruction(); // FIXME: TODO
    }

    Replication::create_object(table, oid);
    if (select_table(*table)) {
        if (table->get_primary_key_column()) {
            // Trying to create object without a primary key in a table that
            // has a primary key column.
            unsupported_instruction();
        }
        Instruction::CreateObject instr;
        instr.table = m_last_class_name;
        instr.object = oid;
        emit(instr);
    }
}

Instruction::PrimaryKey SyncReplication::as_primary_key(Mixed value)
{
    if (value.is_null()) {
        return mpark::monostate{};
    }
    else if (value.get_type() == type_Int) {
        return value.get<int64_t>();
    }
    else if (value.get_type() == type_String) {
        return m_encoder.intern_string(value.get<StringData>());
    }
    else if (value.get_type() == type_ObjectId) {
        return value.get<ObjectId>();
    }
    else if (value.get_type() == type_UUID) {
        return value.get<UUID>();
    }
    else {
        // Unsupported primary key type.
        unsupported_instruction();
    }
}

void SyncReplication::create_object_with_primary_key(const Table* table, ObjKey oid, Mixed value)
{
    if (table->is_embedded()) {
        // Trying to create an object with a primary key in an embedded table.
        unsupported_instruction();
    }

    Replication::create_object_with_primary_key(table, oid, value);
    if (select_table(*table)) {
        if (m_write_validator) {
            m_write_validator(*table);
        }

        auto col = table->get_primary_key_column();
        if (col && ((value.is_null() && col.is_nullable()) || DataType(col.get_type()) == value.get_type())) {
            Instruction::CreateObject instr;
            instr.table = m_last_class_name;
            instr.object = as_primary_key(value);
            emit(instr);
        }
        else {
            // Trying to create object with primary key in table without a
            // primary key column, or with wrong primary key type.
            unsupported_instruction();
        }
    }
}


void SyncReplication::prepare_erase_class(TableKey table_key)
{
    REALM_ASSERT(!m_table_being_erased);
    m_table_being_erased = table_key;
}

void SyncReplication::erase_class(TableKey table_key, size_t num_tables)
{
    Replication::erase_class(table_key, num_tables);

    StringData table_name = m_transaction->get_table_name(table_key);

    bool is_class = m_transaction->table_is_public(table_key);

    if (is_class) {
        REALM_ASSERT(table_key == m_table_being_erased);
        m_table_being_erased = TableKey();

        if (!m_short_circuit) {
            Instruction::EraseTable instr;
            instr.table = emit_class_name(table_name);
            emit(instr);
        }
    }

    m_last_table = nullptr;
}

void SyncReplication::rename_class(TableKey, StringData)
{
    unsupported_instruction();
}

void SyncReplication::insert_column(const Table* table, ColKey col_key, DataType type, StringData name,
                                    Table* target_table)
{
    Replication::insert_column(table, col_key, type, name, target_table);
    using CollectionType = Instruction::AddColumn::CollectionType;

    if (select_table(*table)) {
        Instruction::AddColumn instr;
        instr.table = m_last_class_name;
        instr.field = m_encoder.intern_string(name);
        instr.nullable = col_key.is_nullable();
        instr.type = get_payload_type(type);

        if (col_key.is_list()) {
            instr.collection_type = CollectionType::List;
        }
        else if (col_key.is_dictionary()) {
            instr.collection_type = CollectionType::Dictionary;
            auto key_type = table->get_dictionary_key_type(col_key);
            REALM_ASSERT(key_type == type_String);
            instr.key_type = get_payload_type(key_type);
        }
        else if (col_key.is_set()) {
            instr.collection_type = CollectionType::Set;
            auto value_type = table->get_column_type(col_key);
            REALM_ASSERT(value_type != type_LinkList);
            instr.type = get_payload_type(value_type);
            instr.key_type = Instruction::Payload::Type::Null;
        }
        else {
            REALM_ASSERT(!col_key.is_collection());
            instr.collection_type = CollectionType::Single;
            instr.key_type = Instruction::Payload::Type::Null;
        }

        // Mixed columns are always nullable.
        REALM_ASSERT(instr.type != Instruction::Payload::Type::Null || instr.nullable ||
                     instr.collection_type == CollectionType::Dictionary);

        if (instr.type == Instruction::Payload::Type::Link && target_table) {
            instr.link_target_table = emit_class_name(*target_table);
        }
        else {
            instr.link_target_table = m_encoder.intern_string("");
        }
        emit(instr);
    }
}

void SyncReplication::erase_column(const Table* table, ColKey col_ndx)
{
    Replication::erase_column(table, col_ndx);

    if (select_table(*table)) {
        if (table->get_key() == m_table_being_erased) {
            // Ignore any EraseColumn instructions generated by Core as part of
            // EraseTable.
            return;
        }
        // Not allowed to remove PK/OID columns!
        REALM_ASSERT(col_ndx != table->get_primary_key_column());
        Instruction::EraseColumn instr;
        instr.table = m_last_class_name;
        instr.field = m_encoder.intern_string(table->get_column_name(col_ndx));
        emit(instr);
    }
}

void SyncReplication::rename_column(const Table*, ColKey, StringData)
{
    unsupported_instruction();
}

void SyncReplication::list_set(const CollectionBase& list, size_t ndx, Mixed value)
{
    Mixed prior_value = list.get_any(ndx);
    bool prior_is_unresolved =
        prior_value.is_type(type_Link, type_TypedLink) && prior_value.get<ObjKey>().is_unresolved();

    // If link is unresolved, it should not be communicated.
    if (value.is_type(type_Link, type_TypedLink) && value.get<ObjKey>().is_unresolved()) {
        // ... but reported internally as a deletion if prior value was not unresolved
        if (!prior_is_unresolved)
            Replication::list_erase(list, ndx);
    }
    else {
        if (prior_is_unresolved) {
            Replication::list_insert(list, ndx, value, 0 /* prior size not used */);
        }
        else {
            Replication::list_set(list, ndx, value);
        }
    }

    if (select_collection(list)) {
        // If this is an embedded object then we need to emit and erase/insert instruction so that the old
        // object gets cleared, otherwise you'll only see the Update ObjectValue instruction, which is idempotent,
        // and that will lead to corrupted prior size for array operations inside the embedded object during
        // changeset application.
        auto needs_insert_erase_sequence = [&] {
            if (value.is_type(type_Link)) {
                return list.get_target_table()->is_embedded();
            }
            else if (value.is_type(type_TypedLink)) {
                return m_transaction->get_table(value.get_link().get_table_key())->is_embedded();
            }
            return false;
        };
        if (needs_insert_erase_sequence()) {
            REALM_ASSERT(!list.is_null(ndx));
            Instruction::ArrayErase erase_instr;
            populate_path_instr(erase_instr, list, static_cast<uint32_t>(ndx));
            erase_instr.prior_size = uint32_t(list.size());
            emit(erase_instr);

            Instruction::ArrayInsert insert_instr;
            populate_path_instr(insert_instr, list, static_cast<uint32_t>(ndx));
            insert_instr.prior_size = erase_instr.prior_size - 1;
            insert_instr.value = as_payload(list, value);
            emit(insert_instr);
        }
        else {
            Instruction::Update instr;
            populate_path_instr(instr, list, uint32_t(ndx));
            REALM_ASSERT(instr.is_array_update());
            instr.value = as_payload(list, value);
            instr.prior_size = uint32_t(list.size());
            emit(instr);
        }
    }
}

void SyncReplication::list_insert(const CollectionBase& list, size_t ndx, Mixed value, size_t prior_size)
{
    // If link is unresolved, it should not be communicated.
    if (!(value.is_type(type_Link, type_TypedLink) && value.get<ObjKey>().is_unresolved())) {
        Replication::list_insert(list, ndx, value, prior_size);
    }

    if (select_collection(list)) {
        Instruction::ArrayInsert instr;
        populate_path_instr(instr, list, uint32_t(ndx));
        instr.value = as_payload(list, value);
        instr.prior_size = uint32_t(prior_size);
        emit(instr);
    }
}

void SyncReplication::add_int(const Table* table, ColKey col, ObjKey ndx, int_fast64_t value)
{
    Replication::add_int(table, col, ndx, value);

    if (select_table(*table)) {
        REALM_ASSERT(col != table->get_primary_key_column());

        Instruction::AddInteger instr;
        populate_path_instr(instr, *table, ndx, col);
        instr.value = value;
        emit(instr);
    }
}

void SyncReplication::set(const Table* table, ColKey col, ObjKey key, Mixed value, _impl::Instruction variant)
{
    Replication::set(table, col, key, value, variant);

    if (key.is_unresolved()) {
        return;
    }

    if (col == table->get_primary_key_column()) {
        return;
    }

    // If link is unresolved, it should not be communicated.
    if (value.is_type(type_Link, type_TypedLink) && value.get<ObjKey>().is_unresolved()) {
        return;
    }

    if (select_table(*table)) {
        // Omit of Update(NULL, default=true) for embedded object / dictionary
        // columns if the value is already NULL. This is a workaround for the
        // fact that erase always wins for nested structures, but we don't want
        // default values to win over later embedded object creation.
        if (variant == _impl::instr_SetDefault && value.is_null()) {
            if (col.get_type() == col_type_Link && table->get_object(key).is_null(col)) {
                return;
            }
            if (col.is_dictionary() && table->get_object(key).is_null(col)) {
                // Dictionary columns cannot currently be NULL, but this is
                // likely to change.
                return;
            }
        }

        Instruction::Update instr;
        populate_path_instr(instr, *table, key, col);
        instr.value = as_payload(*table, col, value);
        instr.is_default = (variant == _impl::instr_SetDefault);
        emit(instr);
    }
}


void SyncReplication::remove_object(const Table* table, ObjKey row_ndx)
{
    Replication::remove_object(table, row_ndx);
    if (table->is_embedded())
        return;
    if (table->is_asymmetric())
        return;
    REALM_ASSERT(!row_ndx.is_unresolved());

    if (select_table(*table)) {
        Instruction::EraseObject instr;
        instr.table = m_last_class_name;
        instr.object = primary_key_for_object(*table, row_ndx);
        emit(instr);
    }
}


void SyncReplication::list_move(const CollectionBase& view, size_t from_ndx, size_t to_ndx)
{
    Replication::list_move(view, from_ndx, to_ndx);
    if (select_collection(view)) {
        Instruction::ArrayMove instr;
        populate_path_instr(instr, view, uint32_t(from_ndx));
        instr.ndx_2 = uint32_t(to_ndx);
        instr.prior_size = uint32_t(view.size());
        emit(instr);
    }
}

void SyncReplication::list_erase(const CollectionBase& list, size_t ndx)
{
    Mixed prior_value = list.get_any(ndx);
    // If link is unresolved, it should not be communicated.
    if (!(prior_value.is_type(type_Link, type_TypedLink) && prior_value.get<ObjKey>().is_unresolved())) {
        Replication::list_erase(list, ndx);
    }

    size_t prior_size = list.size();
    if (select_collection(list)) {
        Instruction::ArrayErase instr;
        populate_path_instr(instr, list, uint32_t(ndx));
        instr.prior_size = uint32_t(prior_size);
        emit(instr);
    }
}

void SyncReplication::list_clear(const CollectionBase& view)
{
    Replication::list_clear(view);
    if (select_collection(view)) {
        Instruction::Clear instr;
        populate_path_instr(instr, view);
        emit(instr);
    }
}

void SyncReplication::set_insert(const CollectionBase& set, size_t set_ndx, Mixed value)
{
    Replication::set_insert(set, set_ndx, value);

    if (select_collection(set)) {
        Instruction::SetInsert instr;
        populate_path_instr(instr, set);
        instr.value = as_payload(set, value);
        emit(instr);
    }
}

void SyncReplication::set_erase(const CollectionBase& set, size_t set_ndx, Mixed value)
{
    Replication::set_erase(set, set_ndx, value);

    if (select_collection(set)) {
        Instruction::SetErase instr;
        populate_path_instr(instr, set);
        instr.value = as_payload(set, value);
        emit(instr);
    }
}

void SyncReplication::set_clear(const CollectionBase& set)
{
    Replication::set_clear(set);

    if (select_collection(set)) {
        Instruction::Clear instr;
        populate_path_instr(instr, set);
        emit(instr);
    }
}

void SyncReplication::dictionary_update(const CollectionBase& dict, const Mixed& key, const Mixed& value)
{
    // If link is unresolved, it should not be communicated.
    if (value.is_type(type_Link, type_TypedLink) && value.get<ObjKey>().is_unresolved()) {
        return;
    }

    if (select_collection(dict)) {
        Instruction::Update instr;
        REALM_ASSERT(key.get_type() == type_String);
        populate_path_instr(instr, dict);
        StringData key_value = key.get_string();
        instr.path.push_back(m_encoder.intern_string(key_value));
        instr.value = as_payload(dict, value);
        instr.is_default = false;
        emit(instr);
    }
}

void SyncReplication::dictionary_insert(const CollectionBase& dict, size_t ndx, Mixed key, Mixed value)
{
    Replication::dictionary_insert(dict, ndx, key, value);
    dictionary_update(dict, key, value);
}

void SyncReplication::dictionary_set(const CollectionBase& dict, size_t ndx, Mixed key, Mixed value)
{
    Replication::dictionary_set(dict, ndx, key, value);
    dictionary_update(dict, key, value);
}

void SyncReplication::dictionary_erase(const CollectionBase& dict, size_t ndx, Mixed key)
{
    Replication::dictionary_erase(dict, ndx, key);

    if (select_collection(dict)) {
        Instruction::Update instr;
        REALM_ASSERT(key.get_type() == type_String);
        populate_path_instr(instr, dict);
        StringData key_value = key.get_string();
        instr.path.push_back(m_encoder.intern_string(key_value));
        instr.value = Instruction::Payload::Erased{};
        instr.is_default = false;
        emit(instr);
    }
}

void SyncReplication::nullify_link(const Table* table, ColKey col_ndx, ObjKey ndx)
{
    Replication::nullify_link(table, col_ndx, ndx);

    if (select_table(*table)) {
        Instruction::Update instr;
        populate_path_instr(instr, *table, ndx, col_ndx);
        REALM_ASSERT(!instr.is_array_update());
        instr.value = Instruction::Payload{realm::util::none};
        instr.is_default = false;
        emit(instr);
    }
}

void SyncReplication::link_list_nullify(const Lst<ObjKey>& view, size_t ndx)
{
    size_t prior_size = view.size();
    Replication::link_list_nullify(view, ndx);
    if (select_collection(view)) {
        Instruction::ArrayErase instr;
        populate_path_instr(instr, view, uint32_t(ndx));
        instr.prior_size = uint32_t(prior_size);
        emit(instr);
    }
}

void SyncReplication::unsupported_instruction() const
{
    throw realm::sync::TransformError{"Unsupported instruction"};
}

bool SyncReplication::select_table(const Table& table)
{
    if (is_short_circuited()) {
        return false;
    }

    if (&table == m_last_table) {
        return true;
    }

    if (!m_transaction->table_is_public(table.get_key())) {
        return false;
    }

    m_last_class_name = emit_class_name(table);
    m_last_table = &table;
    m_last_field = ColKey{};
    m_last_object = ObjKey{};
    m_last_primary_key.reset();
    return true;
}

bool SyncReplication::select_collection(const CollectionBase& view)
{
    if (view.get_owner_key().is_unresolved()) {
        return false;
    }

    return select_table(*view.get_table());
}

Instruction::PrimaryKey SyncReplication::primary_key_for_object(const Table& table, ObjKey key)
{
    bool should_emit = select_table(table);
    REALM_ASSERT(should_emit);

    if (table.get_primary_key_column()) {
        return as_primary_key(table.get_primary_key(key));
    }

    GlobalKey global_key = table.get_object_id(key);
    return global_key;
}

void SyncReplication::populate_path_instr(Instruction::PathInstruction& instr, const Table& table, ObjKey key,
                                          ColKey field)
{
    REALM_ASSERT(key);
    REALM_ASSERT(field);

    if (table.is_embedded()) {
        // For embedded objects, Obj::traverse_path() yields the top object
        // first, then objects in the path in order.
        auto obj = table.get_object(key);
        auto path_sizer = [&](size_t size) {
            REALM_ASSERT(size != 0);
            // Reserve 2 elements per path component, because link list entries
            // have both a field and an index.
            instr.path.m_path.reserve(size * 2);
        };

        auto visitor = [&](const Obj& path_obj, ColKey next_field, Mixed index) {
            auto element_table = path_obj.get_table();
            if (element_table->is_embedded()) {
                StringData field_name = element_table->get_column_name(next_field);
                InternString interned_field_name = m_encoder.intern_string(field_name);
                instr.path.push_back(interned_field_name);
            }
            else {
                // This is the top object, populate it the normal way.
                populate_path_instr(instr, *element_table, path_obj.get_key(), next_field);
            }

            if (next_field.is_list()) {
                instr.path.push_back(uint32_t(index.get_int()));
            }
            else if (next_field.is_dictionary()) {
                InternString interned_field_name = m_encoder.intern_string(index.get_string());
                instr.path.push_back(interned_field_name);
            }
        };

        obj.traverse_path(visitor, path_sizer);

        // The field in the embedded object is the last path component.
        StringData field_in_embedded = table.get_column_name(field);
        InternString interned_field_in_embedded = m_encoder.intern_string(field_in_embedded);
        instr.path.push_back(interned_field_in_embedded);
        return;
    }

    bool should_emit = select_table(table);
    REALM_ASSERT(should_emit);

    instr.table = m_last_class_name;

    if (m_last_object == key) {
        instr.object = *m_last_primary_key;
    }
    else {
        instr.object = primary_key_for_object(table, key);
        m_last_object = key;
        m_last_primary_key = instr.object;
    }

    if (m_last_field == field) {
        instr.field = m_last_field_name;
    }
    else {
        instr.field = m_encoder.intern_string(table.get_column_name(field));
        m_last_field = field;
        m_last_field_name = instr.field;
    }
}

void SyncReplication::populate_path_instr(Instruction::PathInstruction& instr, const CollectionBase& list)
{
    ConstTableRef source_table = list.get_table();
    ObjKey source_obj = list.get_owner_key();
    ColKey source_field = list.get_col_key();
    populate_path_instr(instr, *source_table, source_obj, source_field);
}

void SyncReplication::populate_path_instr(Instruction::PathInstruction& instr, const CollectionBase& list,
                                          uint32_t ndx)
{
    populate_path_instr(instr, list);
    instr.path.m_path.push_back(ndx);
}

} // namespace sync
} // namespace realm
