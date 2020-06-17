#include <realm/sync/instruction_replication.hpp>
#include <realm/db.hpp>
#include <realm/sync/transform.hpp> // TransformError
#include <realm/sync/object.hpp>

namespace realm {
namespace sync {

SyncReplication::SyncReplication(const std::string& realm_path)
    : TrivialReplication(realm_path)
{
}

void SyncReplication::initialize(DB& sg)
{
    REALM_ASSERT(!m_sg);
    m_sg = &sg;
}

void SyncReplication::reset()
{
    m_encoder.reset();

    m_last_table = nullptr;
    m_last_object = ObjKey();
    m_last_field = ColKey();
    m_last_class_name = InternString::npos;
    m_last_primary_key = Instruction::PrimaryKey();
    m_last_field_name = InternString::npos;

    m_cache->clear();
}

void SyncReplication::do_initiate_transact(Group& group, version_type current_version, bool history_updated)
{
    TrivialReplication::do_initiate_transact(group, current_version, history_updated);
    Transaction& transaction = dynamic_cast<Transaction&>(group); // FIXME: Is this safe?
    m_cache.reset(new TableInfoCache{transaction});
    reset();
}

template <class T>
auto SyncReplication::as_payload(T value)
{
    return Instruction::Payload{value};
}

template <>
auto SyncReplication::as_payload(StringData value)
{
    auto range = m_encoder.add_string_range(value);
    return Instruction::Payload{range};
}

template <>
auto SyncReplication::as_payload(BinaryData value)
{
    auto range = m_encoder.add_string_range(StringData{value.data(), value.size()});
    const bool is_binary = true;
    return Instruction::Payload{range, is_binary};
}

InternString SyncReplication::emit_class_name(StringData table_name)
{
    return m_encoder.intern_string(table_name_to_class_name(table_name));
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

        case type_Mixed:
            [[fallthrough]];
        case type_OldTable:
            [[fallthrough]];
        case type_OldDateTime:
            unsupported_instruction();
    }
    return Type::Int; // Make compiler happy
}

void SyncReplication::add_class(TableKey tk, StringData name, bool is_embedded)
{
    TrivialReplication::add_class(tk, name, is_embedded);

    bool is_class = name.begins_with("class_");

    if (is_class && !m_short_circuit) {
        Instruction::AddTable instr;
        instr.table = emit_class_name(name);
        if (is_embedded) {
            instr.type = Instruction::AddTable::EmbeddedTable{};
        }
        else {
            auto field = m_encoder.intern_string(""); // FIXME: Should this be "_id"?
            const bool is_nullable = false;
            instr.type = Instruction::AddTable::PrimaryKeySpec{
                field,
                Instruction::Payload::Type::GlobalKey,
                is_nullable,
            };
        }
        emit(instr);
    }
}

void SyncReplication::add_class_with_primary_key(TableKey tk, StringData name, DataType pk_type, StringData pk_field,
                                                 bool nullable)
{
    TrivialReplication::add_class_with_primary_key(tk, name, pk_type, pk_field, nullable);

    bool is_class = name.begins_with("class_");

    if (is_class && !m_short_circuit) {
        Instruction::AddTable instr;
        instr.table = emit_class_name(name);
        auto field = m_encoder.intern_string(pk_field);
        auto spec = Instruction::AddTable::PrimaryKeySpec{field, get_payload_type(pk_type), nullable};
        if (!is_valid_key_type(spec.type)) {
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

    TrivialReplication::create_object(table, oid);
    if (select_table(*table)) {
        const auto& info = m_cache->get_table_info(*table);
        if (info.primary_key_col) {
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
    else {
        // Unsupported primary key type.
        unsupported_instruction();
    }
}

void SyncReplication::create_object_with_primary_key(const Table* table, GlobalKey oid, Mixed value)
{
    if (table->is_embedded()) {
        // Trying to create an object with a primary key in an embedded table.
        unsupported_instruction();
    }

    TrivialReplication::create_object_with_primary_key(table, oid, value);
    if (select_table(*table)) {
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


void SyncReplication::prepare_erase_table(StringData table_name)
{
    REALM_ASSERT(table_name.begins_with("class_"));
    REALM_ASSERT(m_table_being_erased.empty());
    m_table_being_erased = std::string(table_name);
}

void SyncReplication::erase_group_level_table(TableKey table_key, size_t num_tables)
{
    TrivialReplication::erase_group_level_table(table_key, num_tables);

    StringData table_name = m_cache->m_transaction.get_table_name(table_key);

    bool is_class = table_name.begins_with("class_");

    if (is_class) {
        REALM_ASSERT(table_name == m_table_being_erased);
        m_table_being_erased.clear();
        m_cache->clear(); // FIXME: Harsh, but rare.

        if (!m_short_circuit) {
            Instruction::EraseTable instr;
            instr.table = emit_class_name(table_name);
            emit(instr);
        }
    }

    m_last_table = nullptr;
}

void SyncReplication::rename_group_level_table(TableKey, StringData)
{
    unsupported_instruction();
}

void SyncReplication::insert_column(const Table* table, ColKey col_ndx, DataType type, StringData name,
                                    Table* target_table)
{
    TrivialReplication::insert_column(table, col_ndx, type, name, target_table);

    if (select_table(*table)) {
        Instruction::AddColumn instr;
        instr.table = m_last_class_name;
        instr.field = m_encoder.intern_string(name);
        instr.nullable = col_ndx.is_nullable();

        if (type != type_Mixed) {
            instr.type = get_payload_type(type);
        }

        instr.list = col_ndx.is_list();

        // Mixed columns are always nullable.
        REALM_ASSERT(instr.type || instr.nullable);

        if (instr.type == Instruction::Payload::Type::Link && target_table) {
            instr.link_target_table = emit_class_name(*target_table);
        }
        else {
            instr.link_target_table = m_encoder.intern_string("");
        }
        emit(instr);

        // Invalidate cache
        m_cache->m_table_info.erase(table->get_key());
    }
}

void SyncReplication::erase_column(const Table* table, ColKey col_ndx)
{
    TrivialReplication::erase_column(table, col_ndx);

    if (select_table(*table)) {
        if (table->get_name() == m_table_being_erased) {
            // Ignore any EraseColumn instructions generated by Core as part of
            // EraseTable.
            return;
        }
        auto& info = m_cache->get_table_info(*table);
        // Not allowed to remove PK/OID columns!
        REALM_ASSERT(col_ndx != info.primary_key_col);
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

template <class T>
void SyncReplication::set(const Table* table, ColKey col, ObjKey key, T value, _impl::Instruction variant)
{
    if (select_table(*table)) {
        Instruction::Set instr;
        populate_path_instr(instr, *table, key, col);
        instr.value = as_payload(value);
        instr.is_default = (variant == _impl::instr_SetDefault);
        emit(instr);
    }
}

template <class T>
void SyncReplication::list_set(const ConstLstBase& list, size_t ndx, T value)
{
    if (select_list(list)) {
        Instruction::Set instr;
        populate_path_instr(instr, list, uint32_t(ndx));
        REALM_ASSERT(instr.is_array_set());
        instr.value = as_payload(value);
        instr.prior_size = uint32_t(list.size());
        emit(instr);
    }
}

template <class T>
void SyncReplication::list_insert(const ConstLstBase& list, size_t ndx, T value)
{
    if (select_list(list)) {
        auto sz = uint32_t(list.size());
        Instruction::ArrayInsert instr;
        populate_path_instr(instr, list, uint32_t(ndx));
        instr.value = as_payload(value);
        instr.prior_size = sz;
        emit(instr);
    }
}

void SyncReplication::set_int(const Table* table, ColKey col, ObjKey ndx, int_fast64_t value,
                              _impl::Instruction variant)
{
    TrivialReplication::set_int(table, col, ndx, value, variant);
    set(table, col, ndx, value, variant);
}

void SyncReplication::add_int(const Table* table, ColKey col, ObjKey ndx, int_fast64_t value)
{
    TrivialReplication::add_int(table, col, ndx, value);

    if (select_table(*table)) {
        REALM_ASSERT(col != m_cache->get_table_info(*table).primary_key_col);

        Instruction::AddInteger instr;
        populate_path_instr(instr, *table, ndx, col);
        instr.value = value;
        emit(instr);
    }
}

void SyncReplication::set_bool(const Table* table, ColKey col, ObjKey ndx, bool value, _impl::Instruction variant)
{
    TrivialReplication::set_bool(table, col, ndx, value, variant);
    set(table, col, ndx, value, variant);
}

void SyncReplication::set_float(const Table* table, ColKey col, ObjKey ndx, float value, _impl::Instruction variant)
{
    TrivialReplication::set_float(table, col, ndx, value, variant);
    set(table, col, ndx, value, variant);
}

void SyncReplication::set_double(const Table* table, ColKey col, ObjKey ndx, double value, _impl::Instruction variant)
{
    TrivialReplication::set_double(table, col, ndx, value, variant);
    set(table, col, ndx, value, variant);
}

void SyncReplication::set_string(const Table* table, ColKey col, ObjKey ndx, StringData value,
                                 _impl::Instruction variant)
{
    TrivialReplication::set_string(table, col, ndx, value, variant);

    if (value.is_null()) {
        set(table, col, ndx, realm::util::none, variant);
    }
    else {
        set(table, col, ndx, value, variant);
    }
}

void SyncReplication::set_binary(const Table* table, ColKey col, ObjKey ndx, BinaryData value,
                                 _impl::Instruction variant)
{
    TrivialReplication::set_binary(table, col, ndx, value, variant);

    if (value.is_null()) {
        set(table, col, ndx, realm::util::none, variant);
    }
    else {
        set(table, col, ndx, value, variant);
    }
}

void SyncReplication::set_timestamp(const Table* table, ColKey col, ObjKey ndx, Timestamp value,
                                    _impl::Instruction variant)
{
    TrivialReplication::set_timestamp(table, col, ndx, value, variant);
    set(table, col, ndx, value, variant);
}

void SyncReplication::set_object_id(const Table* table, ColKey col, ObjKey ndx, ObjectId value,
                                    _impl::Instruction variant)
{
    TrivialReplication::set_object_id(table, col, ndx, value, variant);
    set(table, col, ndx, value, variant);
}

void SyncReplication::set_decimal(const Table* table, ColKey col, ObjKey ndx, Decimal128 value,
                                  _impl::Instruction variant)
{
    TrivialReplication::set_decimal(table, col, ndx, value, variant);
    set(table, col, ndx, value, variant);
}

void SyncReplication::set_link(const Table* table, ColKey col, ObjKey ndx, ObjKey value, _impl::Instruction variant)
{
    TrivialReplication::set_link(table, col, ndx, value, variant);

    if (value.is_unresolved()) {
        // If link is unresolved, it should not be communicated
        return;
    }
    if (select_table(*table)) {
        if (value) {
            Instruction::Payload::Link link;
            ConstTableRef link_target_table = table->get_link_target(col);
            if (link_target_table->is_embedded()) {
                using Payload = Instruction::Payload;

                Instruction::Set instr;
                populate_path_instr(instr, *table, ndx, col);
                if (value) {
                    instr.value = Payload::ObjectValue{};
                }
                else {
                    instr.value = Payload{util::none};
                }
                emit(instr);
            }
            else {
                link.target_table = emit_class_name(*link_target_table);
                link.target = primary_key_for_object(*link_target_table, value);
                set(table, col, ndx, link, variant);
            }
        }
        else {
            set(table, col, ndx, realm::util::none, variant);
        }
    }
}

void SyncReplication::set_typed_link(const Table* table, ColKey col, ObjKey ndx, ObjLink value,
                                     _impl::Instruction variant)
{
    TrivialReplication::set_typed_link(table, col, ndx, value, variant);

    if (select_table(*table)) {
        if (value) {
            Instruction::Payload::Link link;
            ConstTableRef link_target_table = m_cache->m_transaction.get_table(value.get_table_key());
            REALM_ASSERT(link_target_table);

            if (link_target_table->is_embedded()) {
                REALM_TERMINATE("Mixed with embedded objects not supported yet.");
            }

            link.target_table = emit_class_name(*link_target_table);
            link.target = primary_key_for_object(*link_target_table, value.get_obj_key());
            set(table, col, ndx, link, variant);
        }
        else {
            set(table, col, ndx, realm::util::none, variant);
        }
    }
}

void SyncReplication::set_null(const Table* table, ColKey col, ObjKey ndx, _impl::Instruction variant)
{
    TrivialReplication::set_null(table, col, ndx, variant);
    set(table, col, ndx, realm::util::none, variant);
}

void SyncReplication::insert_substring(const Table*, ColKey, ObjKey, size_t, StringData)
{
    unsupported_instruction();
}

void SyncReplication::erase_substring(const Table*, ColKey, ObjKey, size_t, size_t)
{
    unsupported_instruction();
}

void SyncReplication::list_set_null(const ConstLstBase& list, size_t ndx)
{
    TrivialReplication::list_set_null(list, ndx);
    list_set(list, ndx, util::none);
}

void SyncReplication::list_set_int(const ConstLstBase& list, size_t ndx, int64_t value)
{
    TrivialReplication::list_set_int(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_bool(const ConstLstBase& list, size_t ndx, bool value)
{
    TrivialReplication::list_set_bool(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_float(const ConstLstBase& list, size_t ndx, float value)
{
    TrivialReplication::list_set_float(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_double(const ConstLstBase& list, size_t ndx, double value)
{
    TrivialReplication::list_set_double(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_string(const ConstLstBase& list, size_t ndx, StringData value)
{
    TrivialReplication::list_set_string(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_binary(const ConstLstBase& list, size_t ndx, BinaryData value)
{
    TrivialReplication::list_set_binary(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_timestamp(const ConstLstBase& list, size_t ndx, Timestamp value)
{
    TrivialReplication::list_set_timestamp(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_object_id(const ConstLstBase& list, size_t ndx, ObjectId value)
{
    TrivialReplication::list_set_object_id(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_decimal(const ConstLstBase& list, size_t ndx, Decimal128 value)
{
    TrivialReplication::list_set_decimal(list, ndx, value);
    list_set(list, ndx, value);
}

void SyncReplication::list_set_typed_link(const ConstLstBase& list, size_t ndx, ObjLink value)
{
    TrivialReplication::list_set_typed_link(list, ndx, value);

    if (select_list(list)) {
        Instruction::Set instr;
        populate_path_instr(instr, list, uint32_t(ndx));
        REALM_ASSERT(instr.is_array_set());

        ConstTableRef target_table = m_cache->m_transaction.get_table(value.get_table_key());
        REALM_ASSERT(target_table);

        if (target_table->is_embedded()) {
            REALM_TERMINATE("Mixed with embedded objects not supported yet.");
        }
        else {
            Instruction::Payload::Link link;
            link.target_table = emit_class_name(*target_table);
            link.target = primary_key_for_object(*target_table, value.get_obj_key());
            instr.value = Instruction::Payload{link};
        }
        instr.prior_size = uint32_t(list.size());
        emit(instr);
    }
}

void SyncReplication::list_set_link(const Lst<ObjKey>& list, size_t ndx, ObjKey value)
{
    TrivialReplication::list_set_link(list, ndx, value);

    if (value.is_unresolved()) {
        // If link is unresolved, it should not be communicated
        return;
    }
    if (select_list(list)) {
        Instruction::Set instr;
        populate_path_instr(instr, list, uint32_t(ndx));
        REALM_ASSERT(instr.is_array_set());

        ConstTableRef target_table = list.get_table()->get_link_target(list.get_col_key());
        if (target_table->is_embedded()) {
            if (value) {
                instr.value = Instruction::Payload::ObjectValue{};
            }
            else {
                // ArraySet(null) is not yet supported by Core, so this will
                // never happen in practice. The only way to erase an embedded
                // object in a list is to remove the entry.
                instr.value = Instruction::Payload{};
            }
        }
        else {
            Instruction::Payload::Link link;
            link.target_table = emit_class_name(*target_table);
            link.target = primary_key_for_object(*target_table, value);
            instr.value = Instruction::Payload{link};
        }
        instr.prior_size = uint32_t(list.size());
        emit(instr);
    }
}

void SyncReplication::list_insert_null(const ConstLstBase& list, size_t ndx)
{
    TrivialReplication::list_insert_null(list, ndx);
    list_insert(list, ndx, util::none);
}

void SyncReplication::list_insert_int(const ConstLstBase& list, size_t ndx, int64_t value)
{
    TrivialReplication::list_insert_int(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_bool(const ConstLstBase& list, size_t ndx, bool value)
{
    TrivialReplication::list_insert_bool(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_float(const ConstLstBase& list, size_t ndx, float value)
{
    TrivialReplication::list_insert_float(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_double(const ConstLstBase& list, size_t ndx, double value)
{
    TrivialReplication::list_insert_double(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_string(const ConstLstBase& list, size_t ndx, StringData value)
{
    TrivialReplication::list_insert_string(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_binary(const ConstLstBase& list, size_t ndx, BinaryData value)
{
    TrivialReplication::list_insert_binary(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_timestamp(const ConstLstBase& list, size_t ndx, Timestamp value)
{
    TrivialReplication::list_insert_timestamp(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_object_id(const ConstLstBase& list, size_t ndx, ObjectId value)
{
    TrivialReplication::list_insert_object_id(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_decimal(const ConstLstBase& list, size_t ndx, Decimal128 value)
{
    TrivialReplication::list_insert_decimal(list, ndx, value);
    list_insert(list, ndx, value);
}

void SyncReplication::list_insert_typed_link(const ConstLstBase& list, size_t ndx, ObjLink value)
{
    TrivialReplication::list_insert_typed_link(list, ndx, value);

    if (select_list(list)) {
        Instruction::ArrayInsert instr;
        populate_path_instr(instr, list, uint32_t(ndx));

        ConstTableRef target_table = m_cache->m_transaction.get_table(value.get_table_key());
        REALM_ASSERT(target_table);

        if (target_table->is_embedded()) {
            REALM_TERMINATE("Mixed with embedded objects not supported yet.");
        }
        else {
            Instruction::Payload::Link link;
            link.target_table = emit_class_name(*target_table);
            link.target = primary_key_for_object(*target_table, value.get_obj_key());
            instr.value = Instruction::Payload{link};
        }
        instr.prior_size = uint32_t(list.size());
        emit(instr);
    }
}

void SyncReplication::list_insert_link(const Lst<ObjKey>& list, size_t ndx, ObjKey value)
{
    TrivialReplication::list_insert_link(list, ndx, value);

    if (select_list(list)) {
        Instruction::ArrayInsert instr;
        populate_path_instr(instr, list, uint32_t(ndx));
        ConstTableRef target_table = list.get_table()->get_link_target(list.get_col_key());
        if (target_table->is_embedded()) {
            instr.value = Instruction::Payload::ObjectValue{};
        }
        else {
            auto link = Instruction::Payload::Link{emit_class_name(*target_table),
                                                   primary_key_for_object(*target_table, value)};
            instr.value = Instruction::Payload{link};
        }
        instr.prior_size = uint32_t(list.size());

        emit(instr);
    }
}

void SyncReplication::remove_object(const Table* table, ObjKey row_ndx)
{
    TrivialReplication::remove_object(table, row_ndx);
    if (table->is_embedded())
        return;
    REALM_ASSERT(!row_ndx.is_unresolved());

    // FIXME: This probably belongs in a function similar to sync::create_object().
    if (table->get_name().begins_with("class_")) {
        if (is_short_circuited())
            m_cache->clear_last_object(*table);
    }

    if (select_table(*table)) {
        Instruction::EraseObject instr;
        instr.table = m_last_class_name;
        instr.object = primary_key_for_object(*table, row_ndx);
        emit(instr);
        m_cache->clear_last_object(*table);
    }
}


void SyncReplication::list_move(const ConstLstBase& view, size_t from_ndx, size_t to_ndx)
{
    TrivialReplication::list_move(view, from_ndx, to_ndx);
    if (select_list(view)) {
        Instruction::ArrayMove instr;
        populate_path_instr(instr, view, uint32_t(from_ndx));
        instr.ndx_2 = uint32_t(to_ndx);
        emit(instr);
    }
}

void SyncReplication::list_erase(const ConstLstBase& view, size_t ndx)
{
    size_t prior_size = view.size();
    TrivialReplication::list_erase(view, ndx);
    if (select_list(view)) {
        Instruction::ArrayErase instr;
        populate_path_instr(instr, view, uint32_t(ndx));
        instr.prior_size = uint32_t(prior_size);
        emit(instr);
    }
}

void SyncReplication::list_clear(const ConstLstBase& view)
{
    size_t prior_size = view.size();
    TrivialReplication::list_clear(view);
    if (select_list(view)) {
        Instruction::ArrayClear instr;
        populate_path_instr(instr, view);
        instr.prior_size = uint32_t(prior_size);
        emit(instr);
    }
}

void SyncReplication::nullify_link(const Table* table, ColKey col_ndx, ObjKey ndx)
{
    TrivialReplication::nullify_link(table, col_ndx, ndx);

    if (select_table(*table)) {
        Instruction::Set instr;
        populate_path_instr(instr, *table, ndx, col_ndx);
        REALM_ASSERT(!instr.is_array_set());
        instr.value = Instruction::Payload{realm::util::none};
        instr.is_default = false;
        emit(instr);
    }
}

void SyncReplication::link_list_nullify(const Lst<ObjKey>& view, size_t ndx)
{
    size_t prior_size = view.size();
    TrivialReplication::link_list_nullify(view, ndx);
    if (select_list(view)) {
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
    else {
        StringData name = table.get_name();
        if (name.begins_with("class_")) {
            m_last_class_name = emit_class_name(table);
            m_last_table = &table;
            m_last_field = ColKey{};
            m_last_object = ObjKey{};
            m_last_primary_key.reset();
            return true;
        }
        return false;
    }
}

bool SyncReplication::select_list(const ConstLstBase& view)
{
    return select_table(*view.get_table());
}

Instruction::PrimaryKey SyncReplication::primary_key_for_object(const Table& table, ObjKey key)
{
    bool should_emit = select_table(table);
    REALM_ASSERT(should_emit);

    ColKey pk_col = table.get_primary_key_column();
    const Obj obj = table.get_object(key);
    if (pk_col) {
        DataType pk_type = table.get_column_type(pk_col);
        if (obj.is_null(pk_col)) {
            return mpark::monostate{};
        }

        if (pk_type == type_Int) {
            return obj.get<int64_t>(pk_col);
        }

        if (pk_type == type_String) {
            StringData str = obj.get<StringData>(pk_col);
            auto interned = m_encoder.intern_string(str);
            return interned;
        }

        if (pk_type == type_ObjectId) {
            ObjectId id = obj.get<ObjectId>(pk_col);
            return id;
        }

        unsupported_instruction(); // Unsupported PK type
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

        auto visitor = [&](const Obj& path_obj, ColKey next_field, size_t index) {
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
                instr.path.push_back(uint32_t(index));
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

void SyncReplication::populate_path_instr(Instruction::PathInstruction& instr, const ConstLstBase& list)
{
    ConstTableRef source_table = list.get_table();
    ObjKey source_obj = list.get_key();
    ColKey source_field = list.get_col_key();
    populate_path_instr(instr, *source_table, source_obj, source_field);
}

void SyncReplication::populate_path_instr(Instruction::PathInstruction& instr, const ConstLstBase& list, uint32_t ndx)
{
    populate_path_instr(instr, list);
    instr.path.m_path.push_back(ndx);
}

} // namespace sync
} // namespace realm
