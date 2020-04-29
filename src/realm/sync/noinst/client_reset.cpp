#include <set>
#include <vector>

#include <realm/db.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>

using namespace realm;
using namespace _impl;
using namespace sync;

namespace {

// The recovery fails if there seems to be conflict between the
// instructions and state.
//
// After failure the processing stops and the client reset will
// drop all local changes.
//
// Failure is triggered by:
// 1. Destructive schema changes.
// 2. Creation of an already existing table with another type.
// 3. Creation of an already existing column with another type.
struct ClientResetFailed {
};

// Takes two lists, src and dst, and makes dst equal src. src is unchanged.
template <class T>
bool _copy_list(ConstLst<T>& src, Lst<T>& dst)
{
    // The two arrays are compared by finding the longest common prefix and
    // suffix.  The middle section differs between them and is made equal by
    // updating the middle section of dst.
    //
    // Example:
    // src = abcdefghi
    // dst = abcxyhi
    // The common prefix is abc. The common suffix is hi. xy is replaced by defg.

    bool updated = false;
    size_t len_src = src.size();
    size_t len_dst = dst.size();
    size_t len_min = std::min(len_src, len_dst);

    size_t ndx = 0;
    size_t suffix_len = 0;

    while (ndx < len_min && src.get(ndx) == dst.get(ndx)) {
        ndx++;
    }

    size_t suffix_len_max = len_min - ndx;
    while (suffix_len < suffix_len_max && src.get(len_src - 1 - suffix_len) == dst.get(len_dst - 1 - suffix_len)) {
        suffix_len++;
    }

    len_min -= (ndx + suffix_len);

    for (size_t i = 0; i < len_min; i++) {
        auto val = src.get(ndx);
        if (dst.get(ndx) != val) {
            dst.set(ndx, val);
        }
        ndx++;
    }

    // New elements must be inserted in dst.
    while (len_dst < len_src) {
        dst.insert(ndx, src.get(ndx));
        len_dst++;
        ndx++;
        updated = true;
    }
    // Excess elements must be removed from ll_dst.
    while (len_dst > len_src) {
        len_dst--;
        dst.remove(len_dst - suffix_len);
        updated = true;
    }

    REALM_ASSERT(dst.size() == len_src);
    return updated;
}

template <class T>
bool _copy_list(const ConstObj& src_obj, ColKey src_col, Obj& dst_obj, ColKey dst_col)
{
    auto src = src_obj.get_list<T>(src_col);
    auto dst = dst_obj.get_list<T>(dst_col);
    return _copy_list(src, dst);
}

bool copy_list(const ConstObj& src_obj, ColKey src_col, Obj& dst_obj, ColKey dst_col)
{
    switch (src_col.get_type()) {
        case col_type_Int:
            if (src_col.get_attrs().test(col_attr_Nullable)) {
                return _copy_list<util::Optional<Int>>(src_obj, src_col, dst_obj, dst_col);
            }
            else {
                return _copy_list<Int>(src_obj, src_col, dst_obj, dst_col);
            }
        case col_type_Bool:
            return _copy_list<util::Optional<Bool>>(src_obj, src_col, dst_obj, dst_col);
        case col_type_Float:
            return _copy_list<util::Optional<float>>(src_obj, src_col, dst_obj, dst_col);
        case col_type_Double:
            return _copy_list<util::Optional<double>>(src_obj, src_col, dst_obj, dst_col);
        case col_type_String:
            return _copy_list<String>(src_obj, src_col, dst_obj, dst_col);
        case col_type_Binary:
            return _copy_list<Binary>(src_obj, src_col, dst_obj, dst_col);
        case col_type_Timestamp:
            return _copy_list<Timestamp>(src_obj, src_col, dst_obj, dst_col);
        default:
            break;
    }
    REALM_ASSERT(false);
    return false;
}

bool copy_linklist(ConstLnkLst& ll_src, LnkLst& ll_dst, std::function<ObjKey(ObjKey)> convert_ndx)
{
    // This function ensures that the link list in ll_dst is equal to the
    // link list in ll_src with equality defined by the conversion function
    // convert_ndx.
    //
    // The function uses the same principle as copy_subtable() above.

    bool updated = false;
    size_t len_src = ll_src.size();
    size_t len_dst = ll_dst.size();

    size_t prefix_len, suffix_len;

    for (prefix_len = 0; prefix_len < len_src && prefix_len < len_dst; ++prefix_len) {
        auto ndx_src = ll_src.get(prefix_len);
        auto ndx_dst = ll_dst.get(prefix_len);
        auto ndx_converted = convert_ndx(ndx_src);
        if (ndx_converted != ndx_dst)
            break;
    }

    for (suffix_len = 0; prefix_len + suffix_len < len_src && prefix_len + suffix_len < len_dst; ++suffix_len) {
        auto ndx_src = ll_src.get(len_src - 1 - suffix_len);
        auto ndx_dst = ll_dst.get(len_dst - 1 - suffix_len);
        auto ndx_converted = convert_ndx(ndx_src);
        if (ndx_converted != ndx_dst)
            break;
    }

    if (len_src > len_dst) {
        // New elements must be inserted in ll_dst.
        for (size_t i = prefix_len; i < prefix_len + (len_src - len_dst); ++i) {
            auto ndx_src = ll_src.get(i);
            auto ndx_converted = convert_ndx(ndx_src);
            ll_dst.insert(i, ndx_converted);
            updated = true;
        }
    }
    else if (len_dst > len_src) {
        // Elements must be removed from ll_dst.
        for (size_t i = len_dst - suffix_len; i > len_src - suffix_len; --i)
            ll_dst.remove(i - 1);
        updated = true;
    }
    REALM_ASSERT(ll_dst.size() == len_src);

    // Copy elements from ll_src to ll_dst.
    for (size_t i = prefix_len; i < len_src - suffix_len; ++i) {
        auto ndx_src = ll_src.get(i);
        auto ndx_converted = convert_ndx(ndx_src);
        ll_dst.set(i, ndx_converted);
    }
    return updated;
}

// struct RecoverLocalChangesetsHandler {

//     using Instruction = sync::Instruction;

//     util::Logger& logger;
//     Transaction& transaction;
//     sync::TableInfoCache& table_info_cache;
//     const sync::Changeset* log;
//     TableRef selected_table;
//     std::unique_ptr<LstBase> selected_list;
//     TableRef link_target_table;

//     // Map from table name to map from GlobalKey to GlobalKey.
//     std::map<std::string, std::map<GlobalKey, GlobalKey>> object_id_conversion;

//     RecoverLocalChangesetsHandler(util::Logger& logger,
//                                   Transaction& tr,
//                                   sync::TableInfoCache& table_info_cache)
//         : logger{logger}
//         , transaction{tr}
//         , table_info_cache{table_info_cache}
//     {
//     }

//     void reset()
//     {
//         selected_table = TableRef{};
//         selected_list.reset();
//     }

//     GlobalKey convert_oid(StringData table_name, GlobalKey oid)
//     {
//         auto search = object_id_conversion.find(table_name);
//         if (search == object_id_conversion.end())
//             return oid;
//         auto search_2 = search->second.find(oid);
//         if (search_2 == search->second.end())
//             return oid;
//         return search_2->second;
//     }

//     bool process_changeset(ChunkedBinaryData& chunked_changeset)
//     {
//         if (chunked_changeset.size() == 0)
//             return true;

//         ChunkedBinaryInputStream in{chunked_changeset};
//         sync::Changeset parsed_changeset;
//         sync::parse_changeset(in, parsed_changeset); // Throws

//         log = &parsed_changeset;
//         try {
//             for (auto instr: parsed_changeset) {
//                 if (!instr)
//                     continue;
//                 instr->visit(*this); // Throws
//             }
//         }
//         catch (const ClientResetFailed&) {
//             reset();
//             return false;
//         }
//         reset();
//         return true;
//     }

//     StringData get_string(sync::InternString intern_string) const
//     {
//         auto string_buffer_range = log->try_get_intern_string(intern_string);
//         REALM_ASSERT(string_buffer_range);
//         return log->get_string(*string_buffer_range);
//     }

//     StringData get_string(sync::StringBufferRange range) const
//     {
//         auto string = log->try_get_string(range);
//         REALM_ASSERT(string);
//         return *string;
//     }

//     std::string table_name_for_class(sync::InternString class_name_intern)
//     {
//         StringData class_name = get_string(class_name_intern);
//         std::stringstream ss;
//         ss << "class_" << class_name;
//         return ss.str();
//     }

//     template <class T>
//     std::string table_name_for_instruction(const T& instr) const {
//         TableNameBuffer buffer;
//         StringData class_name = get_string(instr.table);
//         StringData table_name = class_name_to_table_name(class_name, buffer);
//         return std::string{table_name};
//     }

//     template <class T>
//     void select_table(const T& instr) {
//         TableNameBuffer buffer;
//         StringData class_name = get_string(instr.table);
//         StringData table_name = class_name_to_table_name(class_name, buffer);
//         if (!selected_table || selected_table->get_name() != table_name) {
//             selected_table = transaction.get_table(table_name);
//         }
//     }

//     Obj get_object(const Instruction::Payload& id) const {
//         ColKey pk_col = selected_table->get_primary_key_column();
//         ObjKey key;
//         if (pk_col) {
//             if (id.is_null()) {
//                 if (selected_table->is_nullable(pk_col)) {
//                     key = selected_table->find_first_null(pk_col);
//                 }
//             } else if (id.type == Instruction::Payload::Type::Int) {
//                 if (selected_table->get_column_type(pk_col) == type_Int) {
//                     key = selected_table->find_first_int(pk_col, id.data.integer);
//                 }
//             } else if (id.type == Instruction::Payload::Type::String) {
//                 if (selected_table->get_column_type(pk_col) == type_String) {
//                     StringData str = get_string(id.data.str);
//                     key = selected_table->find_first_string(pk_col, str);
//                 }
//             }
//         }
//         if (!key) {
//             throw ClientResetFailed{};
//         }
//         return selected_table->get_object(key);
//     }

//     template <class T>
//     Obj select_object(const T& instr) {
//         select_table(instr);
//         return get_object(instr.object);
//     }

//     template <class T>
//     void select_field(const T& instr) {
//         Obj obj = select_object(instr);
//         if (instr.path.size() != 1) {
//             // FIXME: Add support for longer paths (embedded objects)
//             throw ClientResetFailed{};
//         }

//         if (auto col_name = std::get_if<InternString>(instr.path[0])) {
//             auto col_name_string = get_string(*col_name);
//             auto col_key = selected_table->get_column_key(col_name_string);
//             selected_list = obj.get_listbase_ptr(col_key);
//         } else {
//             throw ClientResetFailed{};
//         }
//     }

//     void operator()(const Instruction::AddTable& instr)
//     {
//         std::string table_name = table_name_for_instruction(instr);
//         TableRef table = transaction.get_table(table_name);
//         if (table) {
//             sync::TableInfoCache::TableInfo table_info
//                 = table_info_cache.get_table_info(*table);
//             if (auto spec = std::get_if<Instruction::AddTable::PrimaryKeySpec>(&instr.type)) {
//                 if (spec->type == Instruction::Payload::Type::GlobalKey && table_info.primary_key_col) {
//                     // Table has primary key, but AddTable does not.
//                     logger.warn("Table %1 has different types on client and server", table_name);
//                     throw ClientResetFailed{};
//                 }
//                 if (spec->nullable != table_info.primary_key_nullable) {
//                     logger.warn("Table %1 has different types on client and server", table_name);
//                     throw ClientResetFailed{};
//                 }
//                 if (spec->type == Instruction::Payload::Type::Int && table_info.primary_key_type != type_Int) {
//                     logger.warn("Table %1 has different types on client and server", table_name);
//                     throw ClientResetFailed{};
//                 }
//                 else if (spec->type == Instruction::Payload::Type::String && table_info.primary_key_type !=
//                 type_String) {
//                     logger.warn("Table %1 has different types on client and server", table_name);
//                     throw ClientResetFailed{};
//                 }
//             } else {
//                 if (!table->is_embedded()) {
//                     logger.warn("Table %1 has different types on client and server", table_name);
//                     throw ClientResetFailed{};
//                 }
//             }
//             return;
//         }

//         if (auto spec = std::get_if<Instruction::AddTable::PrimaryKeySpec>(&instr.type)) {
//             switch (spec->type) {
//                 case Instruction::Payload::Type::GlobalKey: {
//                     sync::create_table(transaction, table_name);
//                     break;
//                 }
//                 case Instruction::Payload::Type::Int: {
//                     StringData pk_col_name = get_string(spec->field);
//                     sync::create_table_with_primary_key(transaction, table_name, type_Int, pk_col_name,
//                     spec->nullable); break;
//                 }
//                 case Instruction::Payload::Type::String: {
//                     StringData pk_col_name = get_string(spec->field);
//                     sync::create_table_with_primary_key(transaction, table_name, type_String, pk_col_name,
//                     spec->nullable); break;
//                 }
//                 default: {
//                     throw ClientResetFailed{};
//                 }
//             }
//         } else {
//             transaction.add_embedded_table(table_name);
//         }

//         table_info_cache.clear();
//     }

//     void operator()(const Instruction::EraseTable& instr)
//     {
//         // Destructive schema changes are not allowed by the resetting client.
//         static_cast<void>(instr);
//         logger.warn("Tables cannot be erased during client reset");
//         throw ClientResetFailed{};
//     }

//     void operator()(const Instruction::CreateObject& instr)
//     {
//         if (selected_table) {
//             const TableInfoCache::TableInfo& table_info =
//                 table_info_cache.get_table_info(*selected_table);
//             if (table_info.primary_key_col) {
//                 if (!instr.has_primary_key) {
//                     failed = true;
//                     return;
//                 }
//                 ObjKey obj_key = sync::row_for_object_id(table_info_cache, *selected_table, instr.object);
//                 if (!obj_key) {
//                     if (instr.payload.type == type_Int) {
//                         if (table_info.primary_key_type != type_Int) {
//                             failed = true;
//                             return;
//                         }
//                         int_fast64_t pk = instr.payload.data.integer;
//                         create_object_with_primary_key(table_info_cache, *selected_table, pk);
//                     }
//                     if (instr.payload.type == type_String) {
//                         if (table_info.primary_key_type != type_String) {
//                             failed = true;
//                             return;
//                         }
//                         StringData pk = get_string(instr.payload.data.str);
//                         create_object_with_primary_key(table_info_cache, *selected_table, pk);
//                     }
//                     else if (instr.payload.is_null()) {
//                         create_object_with_primary_key(table_info_cache, *selected_table, none);
//                     }
//                 }
//             }
//             else {
//                 // Non primary key table.
//                 // The object ids must have the new client file ident, so we must keep
//                 // track of conversions between object ids.
//                 Obj obj = sync::create_object(table_info_cache, *selected_table);
//                 GlobalKey oid = sync::object_id_for_row(table_info_cache, obj);
//                 object_id_conversion[selected_table->get_name()][instr.object] = oid;
//             }
//         }
//     }

//     void operator()(const Instruction::EraseObject& instr)
//     {
//         logger.debug("EraseObject, %1", instr.object);
//         if (selected_table) {
//             GlobalKey oid = convert_oid(selected_table->get_name(), instr.object);
//             ObjKey obj_key = row_for_object_id(table_info_cache, *selected_table, oid);
//             logger.debug("obj_key = %1", obj_key.value);
//             if (obj_key) {
//                 selected_table->remove_object(obj_key);
//                 table_info_cache.clear_last_object(*selected_table);
//             }
//         }
//     }

//     void operator()(const Instruction::Set& instr)
//     {
//         if (REALM_COVER_NEVER(!selected_table))
//             return;

//         ColKey col_key = selected_table->get_column_key(get_string(instr.field));
//         if (!col_key)
//             return;

//         GlobalKey oid = convert_oid(selected_table->get_name(), instr.object);
//         Obj obj = obj_for_object_id(table_info_cache, *selected_table, oid);
//         if (!obj)
//             return;

//         bool is_default = instr.is_default;
//         auto col_type = col_key.get_type();

//         if (instr.payload.is_null()) {
//             if (instr.payload.type == -1) {
//                 if (col_type == col_type_Link) {
//                     obj.set(col_key, ObjKey());
//                 }
//                 else {
//                     if (selected_table->is_nullable(col_key)) {
//                         obj.set_null(col_key, is_default);
//                     }
//                 }
//             }
//             else if (instr.payload.type == -2) {
//                 // -2 means implicit nullify
//                 if (col_type == col_type_Link) {
//                     obj.set_null(col_key, is_default);
//                 }
//             }
//             return;
//         }

//         if (col_type != instr.payload.type)
//             return;

//         switch (instr.payload.type) {
//             case type_Int: {
//                 obj.set(col_key, instr.payload.data.integer, is_default);
//                 return;
//             }
//             case type_Bool: {
//                 obj.set(col_key, instr.payload.data.boolean, is_default);
//                 return;
//             }
//             case type_Float: {
//                 obj.set(col_key, instr.payload.data.fnum, is_default);
//                 return;
//             }
//             case type_Double: {
//                 obj.set(col_key, instr.payload.data.dnum, is_default);
//                 return;
//             }
//             case type_String: {
//                 StringData string = get_string(instr.payload.data.str);
//                 obj.set(col_key, string, is_default);
//                 return;
//             }
//             case type_Binary: {
//                 StringData value = get_string(instr.payload.data.str);
//                 obj.set(col_key, BinaryData{value.data(), value.size()}, is_default);
//                 return;
//             }
//             case type_Timestamp: {
//                 obj.set(col_key, instr.payload.data.timestamp, is_default);
//                 return;
//             }
//             case type_Link: {
//                 ConstTableRef table_target = selected_table->get_link_target(col_key);
//                 std::string table_name_target = table_name_for_class(instr.payload.data.link.target_table);
//                 if (table_target->get_name() != table_name_target)
//                     return;

//                 GlobalKey oid_target = convert_oid(table_target->get_name(), instr.payload.data.link.target);
//                 auto target_obj_key = row_for_object_id(table_info_cache, *table_target, oid_target);
//                 if (!target_obj_key)
//                     return;
//                 obj.set(col_key, target_obj_key, is_default);
//                 return;
//             }
//             default:
//                 return;
//         }
//     }

//     void operator()(const Instruction::AddInteger& instr)
//     {
//         if (selected_table) {
//             if (ColKey col_key = selected_table->get_column_key(get_string(instr.field))) {
//                 if (col_key.get_type() != col_type_Int) {
//                     failed = true;
//                     return;
//                 }
//                 GlobalKey oid = convert_oid(selected_table->get_name(), instr.object);
//                 if (Obj obj = obj_for_object_id(table_info_cache, *selected_table, oid)) {
//                     if (obj.is_null(col_key))
//                         return;
//                     obj.add_int(col_key, instr.value);
//                 }
//             }
//         }
//     }

//     void operator()(const Instruction::AddColumn& instr)
//     {
//         if (REALM_COVER_NEVER(!selected_table))
//             return;
//         StringData column_name = get_string(instr.field); // Throws
//         ColKey col_key = selected_table->get_column_key(column_name);
//         if (col_key) {
//             if (instr.type == type_Link || instr.type == type_LinkList) {
//                 if (DataType(col_key.get_type()) != instr.type) {
//                     logger.warn("The column %1 in table %2 has incompatible "
//                                 "type between client and server",
//                                 column_name, selected_table->get_name());
//                     failed = true;
//                     return;
//                 }
//                 TableRef table_target = selected_table->get_link_target(col_key);
//                 std::string target_name = table_name_for_class(instr.link_target_table);
//                 if (table_target->get_name() != target_name) {
//                     logger.debug("Failed here, target_name = %1, name = %2", target_name,
//                     table_target->get_name()); logger.warn("The column %1 in table %2 has incompatible "
//                                 "type between client and server",
//                                 column_name, selected_table->get_name());
//                     failed = true;
//                     return;
//                 }
//             }
//             else {
//                 if (instr.container_type == sync::ContainerType::None) {
//                     if (DataType(col_key.get_type()) != instr.type) {
//                         logger.warn("The column %1 in table %2 has incompatible "
//                                     "type between client and server",
//                                     column_name, selected_table->get_name());
//                         failed = true;
//                         return;
//                     }
//                     if (selected_table->is_nullable(col_key) != instr.nullable) {
//                         logger.warn("The column %1 in table %2 has incompatible "
//                                     "type between client and server",
//                                     column_name, selected_table->get_name());
//                         failed = true;
//                         return;
//                     }
//                 }
//                 else {
//                     if (!col_key.get_attrs().test(col_attr_List)) {
//                         logger.warn("The column %1 in table %2 has incompatible "
//                                     "type between client and server",
//                                     column_name, selected_table->get_name());
//                         failed = true;
//                         return;
//                     }
//                     if (DataType(col_key.get_type()) != instr.type) {
//                         logger.warn("The column %1 in table %2 has incompatible "
//                                     "type between client and server",
//                                     column_name, selected_table->get_name());
//                         failed = true;
//                         return;
//                     }
//                     if (col_key.get_attrs().test(col_attr_Nullable) != instr.nullable) {
//                         logger.warn("The column %1 in table %2 has incompatible "
//                                     "type between client and server",
//                                     column_name, selected_table->get_name());
//                         failed = true;
//                         return;
//                     }
//                 }
//             }
//             return;
//         }
//         REALM_ASSERT(!col_key);

//         // Add the column if possible.
//         if (instr.type == type_Link || instr.type == type_LinkList) {
//             std::string target_name = table_name_for_class(instr.link_target_table);
//             logger.debug("target_name = %1", target_name);
//             TableRef table_target = transaction.get_table(target_name);
//             if (!table_target)
//                 return;
//             logger.debug("table_target = %1", table_target->get_column_count());
//             selected_table->add_column_link(instr.type, column_name, *table_target);
//         }
//         else if (instr.container_type == sync::ContainerType::None) {
//             selected_table->add_column(instr.type, column_name, instr.nullable);
//         }
//         else {
//             selected_table->add_column_list(instr.type, column_name, instr.nullable);
//         }
//     }

//     void operator()(const Instruction::EraseColumn& instr)
//     {
//         // Destructive schema changes are not allowed by the resetting client.
//         static_cast<void>(instr);
//         failed = true;
//         return;
//     }

//     void operator()(const Instruction::ArraySet& instr)
//     {
//         logger.debug("ArraySet\n");
//         if (selected_list) {

//             if (instr.ndx >= selected_list->size())
//                 return;

//             if (instr.payload.is_null()) {
//                 selected_list->set_null(instr.ndx);
//                 return;
//             }

//             LstBase* list = selected_list.get();
//             switch (instr.payload.type) {
//                 case type_Int: {
//                     REALM_ASSERT(dynamic_cast<Lst<Int>*>(list));
//                     static_cast<Lst<Int>*>(list)->set(instr.ndx, instr.payload.data.integer);
//                     return;
//                 }
//                 case type_Bool: {
//                     REALM_ASSERT(dynamic_cast<Lst<Bool>*>(list));
//                     static_cast<Lst<Bool>*>(list)->set(instr.ndx, instr.payload.data.boolean);
//                     return;
//                 }
//                 case type_Float: {
//                     REALM_ASSERT(dynamic_cast<Lst<Float>*>(list));
//                     static_cast<Lst<Float>*>(list)->set(instr.ndx, instr.payload.data.fnum);
//                     return;
//                 }
//                 case type_Double: {
//                     REALM_ASSERT(dynamic_cast<Lst<Double>*>(list));
//                     static_cast<Lst<Double>*>(list)->set(instr.ndx, instr.payload.data.dnum);
//                     return;
//                 }
//                 case type_String: {
//                     StringData string = get_string(instr.payload.data.str);
//                     REALM_ASSERT(dynamic_cast<Lst<String>*>(list));
//                     static_cast<Lst<String>*>(list)->set(instr.ndx, string);
//                     return;
//                 }
//                 case type_Binary: {
//                     StringData value = get_string(instr.payload.data.str);
//                     REALM_ASSERT(dynamic_cast<Lst<Binary>*>(list));
//                     static_cast<Lst<Binary>*>(list)->set(instr.ndx, BinaryData{value.data(), value.size()});
//                     return;
//                 }
//                 case type_Timestamp: {
//                     REALM_ASSERT(dynamic_cast<Lst<Timestamp>*>(list));
//                     static_cast<Lst<Timestamp>*>(list)->set(instr.ndx, instr.payload.data.timestamp);
//                     return;
//                 }
//                 case type_Link: {
//                     REALM_ASSERT(link_target_table);
//                     GlobalKey oid_target = convert_oid(link_target_table->get_name(),
//                     instr.payload.data.link.target); ObjKey target_obj_key = row_for_object_id(table_info_cache,
//                     *link_target_table, oid_target); if (!target_obj_key)
//                         return;
//                     REALM_ASSERT(dynamic_cast<Lst<ObjKey>*>(list));
//                     static_cast<Lst<ObjKey>*>(list)->set(instr.ndx, target_obj_key);
//                     return;
//                 }
//                 case type_OldDateTime:
//                 case type_OldTable:
//                 case type_OldMixed:
//                 case type_LinkList:
//                     return;
//             }
//         }
//     }

//     void operator()(const Instruction::ArrayInsert& instr)
//     {
//         logger.debug("ArrayInsert");
//         if (selected_list) {
//             logger.debug("ArrayInsert, array");
//             if (instr.ndx > selected_list->size())
//                 return;

//             if (instr.payload.is_null()) {
//                 selected_list->insert_null(instr.ndx);
//                 return;
//             }

//             LstBase* list = selected_list.get();
//             switch (instr.payload.type) {
//                 case type_Int: {
//                     REALM_ASSERT(dynamic_cast<Lst<Int>*>(list));
//                     static_cast<Lst<Int>*>(list)->insert(instr.ndx, instr.payload.data.integer);
//                     break;
//                 }
//                 case type_Bool: {
//                     REALM_ASSERT(dynamic_cast<Lst<Bool>*>(list));
//                     static_cast<Lst<Bool>*>(list)->insert(instr.ndx, instr.payload.data.boolean);
//                     break;
//                 }
//                 case type_Float: {
//                     REALM_ASSERT(dynamic_cast<Lst<Float>*>(list));
//                     static_cast<Lst<Float>*>(list)->insert(instr.ndx, instr.payload.data.fnum);
//                     break;
//                 }
//                 case type_Double: {
//                     REALM_ASSERT(dynamic_cast<Lst<Double>*>(list));
//                     static_cast<Lst<Double>*>(list)->insert(instr.ndx, instr.payload.data.dnum);
//                     break;
//                 }
//                 case type_String: {
//                     StringData string = get_string(instr.payload.data.str);
//                     REALM_ASSERT(dynamic_cast<Lst<String>*>(list));
//                     static_cast<Lst<String>*>(list)->insert(instr.ndx, string);
//                     break;
//                 }
//                 case type_Binary: {
//                     StringData value = get_string(instr.payload.data.str);
//                     REALM_ASSERT(dynamic_cast<Lst<Binary>*>(list));
//                     static_cast<Lst<Binary>*>(list)->insert(instr.ndx, BinaryData{value.data(), value.size()});
//                     break;
//                 }
//                 case type_Timestamp: {
//                     REALM_ASSERT(dynamic_cast<Lst<Timestamp>*>(list));
//                     static_cast<Lst<Timestamp>*>(list)->insert(instr.ndx, instr.payload.data.timestamp);
//                     break;
//                 }
//                 case type_Link: {
//                     GlobalKey oid_target = convert_oid(link_target_table->get_name(),
//                     instr.payload.data.link.target); ObjKey target_obj_key = row_for_object_id(table_info_cache,
//                     *link_target_table, oid_target); if (!target_obj_key)
//                         return;

//                     REALM_ASSERT(dynamic_cast<Lst<ObjKey>*>(list));
//                     static_cast<Lst<ObjKey>*>(list)->insert(instr.ndx, target_obj_key);
//                     break;
//                 }
//                 case type_OldDateTime:
//                 case type_OldTable:
//                 case type_OldMixed:
//                 case type_LinkList:
//                     break;
//             }
//         }
//     }

//     void operator()(const Instruction::ArrayMove& instr)
//     {
//         uint32_t ndx_max = std::max(instr.ndx_1, instr.ndx_2);

//         if (selected_list) {
//             if (ndx_max < selected_list->size()) {
//                 selected_list->move(instr.ndx_1, instr.ndx_2);
//             }
//         }
//     }

//     void operator()(const Instruction::ArrayErase& instr)
//     {
//         if (selected_list) {
//             if (instr.ndx < selected_list->size()) {
//                 selected_list->remove(instr.ndx, instr.ndx + 1);
//             }
//         }
//     }

//     void operator()(const Instruction::ArrayClear& instr)
//     {
//         static_cast<void>(instr);
//         if (selected_list) {
//             selected_list->clear();
//         }
//     }
// };

// bool recover_local_changesets(sync::version_type current_version_local, ClientHistoryImpl& history_local,
//                               sync::version_type client_version, Transaction& tr_remote,
//                               sync::TableInfoCache& table_info_cache_remote, util::Logger& logger)
// {
//     RecoverLocalChangesetsHandler handler{logger, tr_remote, table_info_cache_remote};

//     sync::version_type begin_version = client_version + 1;
//     while(true) {
//         Optional<_impl::ClientHistoryImpl::LocalChangeset> local_changeset =
//             history_local.get_next_local_changeset(current_version_local, begin_version);
//         if (!local_changeset)
//             return true;
//         logger.debug("Local changeset version = %1, size = %2", local_changeset->version,
//                      local_changeset->changeset.size());
//         bool success = handler.process_changeset(local_changeset->changeset);
//         if (!success)
//             return false;
//         begin_version = local_changeset->version + 1;
//     }
// }


} // namespace


void client_reset::transfer_group(const Transaction& group_src, const sync::TableInfoCache& table_info_cache_src,
                                  Transaction& group_dst, sync::TableInfoCache& table_info_cache_dst,
                                  util::Logger& logger)
{
    logger.debug("copy_group, src size = %1, dst size = %2", group_src.size(), group_dst.size());

    // Find all tables in dst that should be removed.
    std::set<std::string> tables_to_remove;
    for (auto table_key : group_dst.get_table_keys()) {
        StringData table_name = group_dst.get_table_name(table_key);
        if (!table_name.begins_with("class"))
            continue;
        logger.debug("key = %1, table_name = %2", table_key.value, table_name);
        ConstTableRef table_src = group_src.get_table(table_name);
        if (!table_src) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        // Check whether the table type is the same.
        TableRef table_dst = group_dst.get_table(table_key);
        bool has_pk_src = sync::table_has_primary_key(table_info_cache_src, *table_src);
        bool has_pk_dst = sync::table_has_primary_key(table_info_cache_dst, *table_dst);
        if (has_pk_src != has_pk_dst) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        if (!has_pk_src)
            continue;

        // Now the tables both have primary keys.
        const sync::TableInfoCache::TableInfo& table_info_src = table_info_cache_src.get_table_info(*table_src);
        const sync::TableInfoCache::TableInfo& table_info_dst = table_info_cache_dst.get_table_info(*table_dst);
        if (table_info_src.primary_key_type != table_info_dst.primary_key_type ||
            table_info_src.primary_key_nullable != table_info_dst.primary_key_nullable) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        StringData pk_col_name_src = table_src->get_column_name(table_info_src.primary_key_col);
        StringData pk_col_name_dst = table_dst->get_column_name(table_info_dst.primary_key_col);
        if (pk_col_name_src != pk_col_name_dst) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        // The table survives.
        logger.debug("Table '%1' will remain", table_name);
    }
    table_info_cache_dst.clear();

    // Remove all columns that link to one of the tables to be removed.
    for (auto table_key : group_dst.get_table_keys()) {
        TableRef table_dst = group_dst.get_table(table_key);
        StringData table_name = table_dst->get_name();
        if (!table_name.begins_with("class"))
            continue;
        std::vector<std::string> columns_to_remove;
        for (ColKey col_key : table_dst->get_column_keys()) {
            DataType column_type = table_dst->get_column_type(col_key);
            if (column_type == type_Link || column_type == type_LinkList) {
                TableRef table_target = table_dst->get_link_target(col_key);
                StringData table_target_name = table_target->get_name();
                if (tables_to_remove.find(table_target_name) != tables_to_remove.end()) {
                    StringData col_name = table_dst->get_column_name(col_key);
                    columns_to_remove.push_back(col_name);
                }
            }
        }
        for (const std::string& col_name : columns_to_remove) {
            logger.debug("Column '%1' in table '%2' is removed", col_name, table_dst->get_name());
            ColKey col_key = table_dst->get_column_key(col_name);
            table_dst->remove_column(col_key);
        }
    }

    // Remove the tables to be removed.
    for (const std::string& table_name : tables_to_remove)
        sync::erase_table(group_dst, table_info_cache_dst, table_name);

    table_info_cache_dst.clear();

    // Create new tables in dst if needed.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        bool has_pk = sync::table_has_primary_key(table_info_cache_src, *table_src);
        TableRef table_dst = group_dst.get_table(table_name);
        if (!table_dst) {
            // Create the table.
            if (!has_pk) {
                sync::create_table(group_dst, table_name);
            }
            else {
                const sync::TableInfoCache::TableInfo& table_info_src =
                    table_info_cache_src.get_table_info(*table_src);
                DataType pk_type = table_info_src.primary_key_type;
                StringData pk_col_name = table_src->get_column_name(table_info_src.primary_key_col);
                bool nullable = table_info_src.primary_key_nullable;
                group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, nullable);
            }
        }
    }

    // Now the class tables are identical.
    size_t num_tables;
    {
        size_t num_tables_src = 0;
        for (auto table_key : group_src.get_table_keys()) {
            if (group_src.get_table_name(table_key).begins_with("class"))
                ++num_tables_src;
        }
        size_t num_tables_dst = 0;
        for (auto table_key : group_dst.get_table_keys()) {
            if (group_dst.get_table_name(table_key).begins_with("class"))
                ++num_tables_dst;
        }
        REALM_ASSERT(num_tables_src == num_tables_dst);
        num_tables = num_tables_src;
    }
    logger.debug("The number of tables is %1", num_tables);

    // Remove columns in dst if they are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        std::vector<std::string> columns_to_remove;
        for (ColKey col_key : table_dst->get_column_keys()) {
            auto col_type = col_key.get_type();
            StringData col_name = table_dst->get_column_name(col_key);
            ColKey col_key_src = table_src->get_column_key(col_name);
            if (!col_key_src) {
                columns_to_remove.push_back(col_name);
                continue;
            }
            if (col_key_src.get_type() != col_type) {
                columns_to_remove.push_back(col_name);
                continue;
            }
            if (!(col_key.get_attrs() == col_key.get_attrs())) {
                columns_to_remove.push_back(col_name);
                continue;
            }
            if (Table::is_link_type(col_type)) {
                ConstTableRef target_src = table_src->get_link_target(col_key_src);
                TableRef target_dst = table_dst->get_link_target(col_key);
                if (target_src->get_name() != target_dst->get_name()) {
                    columns_to_remove.push_back(col_name);
                    continue;
                }
            }
        }
        for (const std::string& col_name : columns_to_remove) {
            logger.debug("Column '%1' in table '%2' is removed", col_name, table_name);
            ColKey col_key = table_dst->get_column_key(col_name);
            table_dst->remove_column(col_key);
        }
    }

    // Add columns in dst if present in src and absent in dst.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with(
                "class")) // FIXME: This is an imprecise check. A more correct version would check for `class_`, but
                          // this should be done by a shared function somewhere. Maybe one exists already.
            continue;
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        for (ColKey col_key : table_src->get_column_keys()) {
            StringData col_name = table_src->get_column_name(col_key);
            ColKey col_key_dst = table_dst->get_column_key(col_name);
            if (!col_key_dst) {
                DataType type = table_src->get_column_type(col_key);
                bool nullable = table_src->is_nullable(col_key);
                bool has_search_index = table_src->has_search_index(col_key);
                logger.trace("Create column, table = %1, column name = %2, "
                             " type = %3, nullable = %4, has_search_index = %5",
                             table_name, col_name, type, nullable, has_search_index);
                ColKey col_key_dst;
                if (Table::is_link_type(ColumnType(type))) {
                    ConstTableRef target_src = table_src->get_link_target(col_key);
                    TableRef target_dst = group_dst.get_table(target_src->get_name());
                    col_key_dst = table_dst->add_column_link(type, col_name, *target_dst);
                }
                else if (col_key.get_attrs().test(col_attr_List)) {
                    col_key_dst = table_dst->add_column_list(type, col_name, nullable);
                }
                else {
                    col_key_dst = table_dst->add_column(type, col_name, nullable);
                }

                if (has_search_index)
                    table_dst->add_search_index(col_key_dst);
            }
        }
    }

    // Now the schemas are identical.

    // Remove objects in dst that are absent in src.
    // We will also have to remove all objects created locally as they should have
    // new keys because the client file id is changed.
    auto new_file_id = group_dst.get_sync_file_id();
    for (auto table_key : group_src.get_table_keys()) {
        auto table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        logger.debug("Removing objects in '%1'", table_name);
        auto table_dst = group_dst.get_table(table_name);
        std::vector<std::pair<GlobalKey, ObjKey>> objects_to_remove;
        for (auto obj : *table_dst) {
            auto oid = table_dst->get_object_id(obj.get_key());
            auto key_src = table_src->get_objkey(oid);
            if (oid.hi() == new_file_id || !key_src || !table_src->is_valid(key_src))
                objects_to_remove.emplace_back(oid, obj.get_key());
        }
        for (auto& pair : objects_to_remove) {
            logger.debug("  removing '%1'", pair.first);
            table_dst->remove_object(pair.second);
        }
    }

    // Add objects that are present in src but absent in dst.
    for (auto table_key : group_src.get_table_keys()) {
        auto table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        logger.debug("Adding objects in '%1'", table_name);
        auto table_dst = group_dst.get_table(table_name);
        auto pk_col = table_src->get_primary_key_column();

        for (auto& obj : *table_src) {
            auto oid = table_src->get_object_id(obj.get_key());
            auto key_dst = table_dst->get_objkey(oid);
            if (!key_dst || !table_dst->is_valid(key_dst)) {
                logger.debug("  adding '%1'", oid);
                if (pk_col) {
                    auto pk = obj.get_any(pk_col);
                    table_dst->create_object_with_primary_key(pk);
                }
                else {
                    table_dst->create_object(oid);
                }
            }
        }
    }

    // Now src and dst have identical schemas and objects. The values might
    // still differ.

    // Diff all the values and update if needed.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_src->size() == table_dst->size());
        REALM_ASSERT(table_src->get_column_count() == table_dst->get_column_count());
        const sync::TableInfoCache::TableInfo& table_info_src = table_info_cache_src.get_table_info(*table_src);
        if (table_src->get_primary_key_column()) {
            logger.debug("Updating values for table '%1', number of rows = %2, "
                         "number of columns = %3, primary_key_col = %4, "
                         "primary_key_type = %5",
                         table_name, table_src->size(), table_src->get_column_count(),
                         table_info_src.primary_key_col.get_index().val, table_info_src.primary_key_type);
        }
        else {
            logger.debug("Updating values for table '%1', number of rows = %2, number of columns = %3", table_name,
                         table_src->size(), table_src->get_column_count());
        }

        for (const ConstObj& src : *table_src) {
            auto oid = sync::object_id_for_row(table_info_cache_src, src);
            auto dst = obj_for_object_id(table_info_cache_dst, *table_dst, oid);
            REALM_ASSERT(dst);
            bool updated = false;

            for (ColKey col_key_src : table_src->get_column_keys()) {
                if (col_key_src == table_info_src.primary_key_col)
                    continue;
                StringData col_name = table_src->get_column_name(col_key_src);
                ColKey col_key_dst = table_dst->get_column_key(col_name);
                REALM_ASSERT(col_key_dst);
                DataType col_type = table_src->get_column_type(col_key_src);
                if (col_type == type_Link) {
                    ConstTableRef table_target_src = table_src->get_link_target(col_key_src);
                    TableRef table_target_dst = table_dst->get_link_target(col_key_dst);
                    REALM_ASSERT(table_target_src->get_name() == table_target_dst->get_name());

                    if (src.is_null(col_key_src)) {
                        if (!dst.is_null(col_key_dst)) {
                            dst.set_null(col_key_dst);
                            updated = true;
                        }
                    }
                    else {
                        ObjKey target_obj_key_src = src.get<ObjKey>(col_key_src);
                        GlobalKey target_oid =
                            sync::object_id_for_row(table_info_cache_src, *table_target_src, target_obj_key_src);
                        ObjKey target_obj_key_dst =
                            sync::row_for_object_id(table_info_cache_dst, *table_target_dst, target_oid);
                        if (dst.get<ObjKey>(col_key_dst) != target_obj_key_dst) {
                            dst.set(col_key_dst, target_obj_key_dst);
                            updated = true;
                        }
                    }
                }
                else if (col_type == type_LinkList) {
                    ConstTableRef table_target_src = table_src->get_link_target(col_key_src);
                    TableRef table_target_dst = table_dst->get_link_target(col_key_dst);
                    REALM_ASSERT(table_target_src->get_name() == table_target_dst->get_name());
                    // convert_ndx converts the row index in table_target_src
                    // to the row index in table_target_dst such that the
                    // object ids are the same.
                    auto convert_ndx = [&](ObjKey key_src) {
                        auto oid = sync::object_id_for_row(table_info_cache_src, *table_target_src, key_src);
                        ObjKey key_dst = sync::row_for_object_id(table_info_cache_dst, *table_target_dst, oid);
                        REALM_ASSERT(key_dst);
                        return key_dst;
                    };
                    auto ll_src = src.get_linklist(col_key_src);
                    auto ll_dst = dst.get_linklist(col_key_dst);
                    if (copy_linklist(ll_src, ll_dst, convert_ndx)) {
                        updated = true;
                    }
                }
                else if (col_key_src.get_attrs().test(col_attr_List)) {
                    if (copy_list(src, col_key_src, dst, col_key_dst)) {
                        updated = true;
                    }
                }
                else {
                    auto val_src = src.get_any(col_key_src);
                    auto val_dst = dst.get_any(col_key_dst);
                    if (val_src != val_dst) {
                        dst.set(col_key_dst, val_src);
                        updated = true;
                    }
                }
            }
            if (updated) {
                logger.debug("  updating %1", oid);
            }
        }
    }
}


void client_reset::recover_schema(const Transaction& group_src, const sync::TableInfoCache& table_info_cache_src,
                                  Transaction& group_dst, util::Logger& logger)
{
    // First the missing tables are created. Columns must be created later due
    // to links.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        TableRef table_dst = group_dst.get_table(table_name);
        if (table_dst) {
            // Disagreement of table type is ignored.
            // That problem is rare and cannot be resolved here.
            continue;
        }
        bool has_pk = sync::table_has_primary_key(table_info_cache_src, *table_src);
        // Create the table.
        logger.trace("Recover the table %1", table_name);
        if (!has_pk) {
            sync::create_table(group_dst, table_name);
        }
        else {
            const sync::TableInfoCache::TableInfo& table_info_src = table_info_cache_src.get_table_info(*table_src);
            DataType pk_type = table_info_src.primary_key_type;
            StringData pk_col_name = table_src->get_column_name(table_info_src.primary_key_col);
            bool nullable = table_info_src.primary_key_nullable;
            group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, nullable);
        }
    }

    // Create the missing columns.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        for (ColKey col_key : table_src->get_column_keys()) {
            StringData col_name = table_src->get_column_name(col_key);
            ColKey col_key_dst = table_dst->get_column_key(col_name);
            if (!col_key_dst) {
                DataType type = table_src->get_column_type(col_key);
                bool nullable = table_src->is_nullable(col_key);
                logger.trace("Recover column, table = %1, column name = %2, "
                             " type = %3, nullable = %4",
                             table_name, col_name, type, nullable);
                if (type == type_Link || type == type_LinkList) {
                    ConstTableRef target_src = table_src->get_link_target(col_key);
                    TableRef target_dst = group_dst.get_table(target_src->get_name());
                    table_dst->add_column_link(type, col_name, *target_dst);
                }
                else if (col_key.get_attrs().test(col_attr_List)) {
                    table_dst->add_column_list(type, col_name, nullable);
                }
                else {
                    table_dst->add_column(type, col_name, nullable);
                }
            }
        }
    }
}

client_reset::LocalVersionIDs
client_reset::perform_client_reset_diff(const std::string& path_remote, const std::string& path_local,
                                        const util::Optional<std::array<char, 64>>& encryption_key,
                                        sync::SaltedFileIdent client_file_ident, sync::SaltedVersion server_version,
                                        uint_fast64_t downloaded_bytes, sync::version_type client_version,
                                        bool recover_local_changes, util::Logger& logger, bool should_commit_remote)
{
    logger.info("Client reset, path_remote = %1, path_local = %2, "
                "encryption = %3, client_file_ident.ident = %4, "
                "client_file_ident.salt = %5, server_version.version = %6, "
                "server_version.salt = %7, downloaded_bytes = %8, "
                "client_version = %9, recover_local_changes = %10, "
                "should_commit_remote = %11.",
                path_remote, path_local, (encryption_key ? "on" : "off"), client_file_ident.ident,
                client_file_ident.salt, server_version.version, server_version.salt, downloaded_bytes, client_version,
                (recover_local_changes ? "true" : "false"), (should_commit_remote ? "true" : "false"));

    DBOptions shared_group_options(encryption_key ? encryption_key->data() : nullptr);
    ClientHistoryImpl history_local{path_local};
    DBRef sg_local = DB::create(history_local, shared_group_options);

    auto group_local = sg_local->start_write();
    VersionID old_version_local = group_local->get_version_of_current_transaction();
    sync::version_type current_version_local = old_version_local.version;
    group_local->get_history()->ensure_updated(current_version_local);
    sync::TableInfoCache table_info_cache_local{*group_local};

    std::unique_ptr<ClientHistoryImpl> history_remote = std::make_unique<ClientHistoryImpl>(path_remote);
    DBRef sg_remote = DB::create(*history_remote, shared_group_options);
    auto wt_remote = sg_remote->start_write();
    sync::version_type current_version_remote = wt_remote->get_version();
    history_local.set_client_file_ident_in_wt(current_version_local, client_file_ident);
    history_remote->set_client_file_ident_in_wt(current_version_remote, client_file_ident);

    if (recover_local_changes) {
        // Set the client file ident in the remote Realm to prepare it for creation of
        // local changes.
        sync::TableInfoCache table_info_cache_remote{*wt_remote};

        // Copy tables and columns from local into remote to avoid destructive schema changes.
        // The instructions that create tables and columns present in local but not
        // remote will then be uploaded to the server.
        // This needs to be done before the local changes are recovered otherwise we might loose
        // modifications on a table, which creation has been integrated on the server but not yet
        // present in the state file
        recover_schema(*group_local, table_info_cache_local, *wt_remote, logger);

        // Recover the local changesets with client versions above 'client_version'.
        // The recovered changeset will be in history_remote's instruction encoder
        // when the function returns.

        // FIXME: Re-enable
        // bool recovered_local_changes = recover_local_changesets(current_version_local, history_local,
        // client_version,
        //                                                         *wt_remote, table_info_cache_remote, logger);
        bool recovered_local_changes = false;

        if (!recovered_local_changes) {
            const char* msg = "The local data in the client Realm could not be recovered "
                              // "due to a schema mismatch"; // FIXME
                              "due to recovery not being supported";
            logger.warn(msg);
            // Reset the transaction.
            wt_remote.reset(); // Rollback
            sg_remote.reset(); // Close file

            // Reopen
            history_remote = std::make_unique<ClientHistoryImpl>(path_remote);
            sg_remote = DB::create(*history_remote, shared_group_options);
            wt_remote = sg_remote->start_write();
            current_version_remote = wt_remote->get_version();
            history_remote->set_client_file_ident_in_wt(current_version_remote, client_file_ident);
            // Recover schema again. The previous changes were never comitted
            recover_schema(*group_local, table_info_cache_local, *wt_remote, logger);
        }
    }

    // Diff the content from remote into local.
    {
        sync::TableInfoCache table_info_cache_remote{*wt_remote};
        // Copy, by diffing, all group content from the remote to the local.
        transfer_group(*wt_remote, table_info_cache_remote, *group_local, table_info_cache_local, logger);
    }

    // Extract the changeset produced in the remote Realm during recovery.
    sync::ChangesetEncoder& instruction_encoder = history_remote->get_instruction_encoder();
    const sync::ChangesetEncoder::Buffer& buffer = instruction_encoder.buffer();
    BinaryData recovered_changeset{buffer.data(), buffer.size()};

    //    {
    //        // Debug.
    //        ChunkedBinaryInputStream in{recovered_changeset};
    //        sync::Changeset log;
    //        sync::parse_changeset(in, log); // Throws
    //        log.print();
    //    }

    history_local.set_client_reset_adjustments(current_version_local, client_file_ident, server_version,
                                               downloaded_bytes, recovered_changeset);
    if (should_commit_remote)
        wt_remote->commit();

    // Finally, the local Realm is committed.
    group_local->commit_and_continue_as_read();
    VersionID new_version_local = group_local->get_version_of_current_transaction();
    logger.debug("perform_client_reset_diff is done, old_version.version = %1, "
                 "old_version.index = %2, new_version.version = %3, "
                 "new_version.index = %4",
                 old_version_local.version, old_version_local.index, new_version_local.version,
                 new_version_local.index);

    return LocalVersionIDs{old_version_local, new_version_local};
}
