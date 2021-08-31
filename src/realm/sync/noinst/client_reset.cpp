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
bool _copy_list(Lst<T>& src, Lst<T>& dst)
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
bool _copy_list(const Obj& src_obj, ColKey src_col, Obj& dst_obj, ColKey dst_col)
{
    auto src = src_obj.get_list<T>(src_col);
    auto dst = dst_obj.get_list<T>(dst_col);
    return _copy_list(src, dst);
}

bool copy_list(const Obj& src_obj, ColKey src_col, Obj& dst_obj, ColKey dst_col)
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

bool copy_linklist(LnkLst& ll_src, LnkLst& ll_dst, std::function<ObjKey(ObjKey)> convert_ndx)
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

} // namespace

void client_reset::remove_all_tables(Transaction& tr_dst, util::Logger& logger)
{
    logger.debug("remove_all_tables, dst size = %1", tr_dst.size());
    // Remove the tables to be removed.
    for (auto table_key : tr_dst.get_table_keys()) {
        TableRef table = tr_dst.get_table(table_key);
        if (table->get_name().begins_with("class_")) {
            sync::erase_table(tr_dst, table);
        }
    }
}

void client_reset::transfer_group(const Transaction& group_src, Transaction& group_dst, util::Logger& logger)
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
        auto pk_col_src = table_src->get_primary_key_column();
        auto pk_col_dst = table_dst->get_primary_key_column();
        bool has_pk_src = bool(pk_col_src);
        bool has_pk_dst = bool(pk_col_dst);
        if (has_pk_src != has_pk_dst) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        if (!has_pk_src)
            continue;

        // Now the tables both have primary keys.
        if (pk_col_src.get_type() != pk_col_dst.get_type() || pk_col_src.is_nullable() != pk_col_dst.is_nullable()) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        StringData pk_col_name_src = table_src->get_column_name(pk_col_src);
        StringData pk_col_name_dst = table_dst->get_column_name(pk_col_dst);
        if (pk_col_name_src != pk_col_name_dst) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        // The table survives.
        logger.debug("Table '%1' will remain", table_name);
    }

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
        sync::erase_table(group_dst, table_name);

    // Create new tables in dst if needed.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        bool has_pk = sync::table_has_primary_key(*table_src);
        TableRef table_dst = group_dst.get_table(table_name);
        if (!table_dst) {
            // Create the table.
            if (!has_pk) {
                sync::create_table(group_dst, table_name);
            }
            else {
                auto pk_col_src = table_src->get_primary_key_column();
                DataType pk_type = DataType(pk_col_src.get_type());
                StringData pk_col_name = table_src->get_column_name(pk_col_src);
                group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, pk_col_src.is_nullable());
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
                    if (type == type_LinkList) {
                        col_key_dst = table_dst->add_column_list(*target_dst, col_name);
                    }
                    else {
                        col_key_dst = table_dst->add_column(*target_dst, col_name);
                    }
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
        if (auto pk_col = table_dst->get_primary_key_column()) {
            std::vector<std::pair<Mixed, ObjKey>> objects_to_remove;
            for (auto obj : *table_dst) {
                auto pk = obj.get_any(pk_col);
                if (!table_src->find_primary_key(pk)) {
                    objects_to_remove.emplace_back(pk, obj.get_key());
                }
            }
            for (auto& pair : objects_to_remove) {
                logger.debug("  removing '%1'", pair.first);
                table_dst->remove_object(pair.second);
            }
        }
        else {
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

        if (auto pk_col = table_src->get_primary_key_column()) {
            logger.debug("Updating values for table '%1', number of rows = %2, "
                         "number of columns = %3, primary_key_col = %4, "
                         "primary_key_type = %5",
                         table_name, table_src->size(), table_src->get_column_count(), pk_col.get_index().val,
                         pk_col.get_type());
        }
        else {
            logger.debug("Updating values for table '%1', number of rows = %2, number of columns = %3", table_name,
                         table_src->size(), table_src->get_column_count());
        }

        for (const Obj& src : *table_src) {
            auto oid = src.get_object_id();
            auto dst = obj_for_object_id(*table_dst, oid);
            REALM_ASSERT(dst);
            bool updated = false;

            for (ColKey col_key_src : table_src->get_column_keys()) {
                if (col_key_src == table_src->get_primary_key_column())
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
                        GlobalKey target_oid = table_target_src->get_object_id(target_obj_key_src);
                        ObjKey target_obj_key_dst = sync::row_for_object_id(*table_target_dst, target_oid);
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
                        auto oid = table_target_src->get_object_id(key_src);
                        ObjKey key_dst = sync::row_for_object_id(*table_target_dst, oid);
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


void client_reset::recover_schema(const Transaction& group_src, Transaction& group_dst, util::Logger& logger)
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
        // Create the table.
        logger.trace("Recover the table %1", table_name);
        if (auto pk_col = table_src->get_primary_key_column()) {
            DataType pk_type = DataType(pk_col.get_type());
            StringData pk_col_name = table_src->get_column_name(pk_col);
            group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, pk_col.is_nullable());
        }
        else {
            sync::create_table(group_dst, table_name);
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
                if (col_key.is_list()) {
                    if (type == type_LinkList) {
                        ConstTableRef target_src = table_src->get_link_target(col_key);
                        TableRef target_dst = group_dst.get_table(target_src->get_name());
                        table_dst->add_column_list(*target_dst, col_name);
                    }
                    else {
                        table_dst->add_column_list(type, col_name, nullable);
                    }
                }
                else {
                    if (type == type_Link) {
                        ConstTableRef target_src = table_src->get_link_target(col_key);
                        TableRef target_dst = group_dst.get_table(target_src->get_name());
                        table_dst->add_column(*target_dst, col_name);
                    }
                    else {
                        table_dst->add_column(type, col_name, nullable);
                    }
                }
            }
        }
    }
}

client_reset::LocalVersionIDs client_reset::perform_client_reset_diff(DB& db, sync::SaltedFileIdent client_file_ident,
                                                                      sync::SaltedVersion server_version,
                                                                      util::Logger& logger)
{
    logger.info("Client reset: path_local = %1, client_file_ident.ident = %2, "
                "client_file_ident.salt = %3, server_version.version = %4, "
                "server_version.salt = %5",
                db.get_path(), client_file_ident.ident, client_file_ident.salt, server_version.version,
                server_version.salt);

    auto& history = dynamic_cast<ClientHistoryImpl&>(*db.get_replication());
    auto group_local = db.start_write();
    VersionID old_version_local = group_local->get_version_of_current_transaction();
    sync::version_type current_version_local = old_version_local.version;
    group_local->get_history()->ensure_updated(current_version_local);

    // make breaking changes in the local copy which cannot be advanced
    remove_all_tables(*group_local, logger);

    // Extract the changeset produced in the remote Realm during recovery.
    // Since recovery mode has been unsupported this is always empty.
    BinaryData recovered_changeset;
    uint_fast64_t downloaded_bytes = 0;
    history.set_client_reset_adjustments(current_version_local, client_file_ident, server_version, downloaded_bytes,
                                         recovered_changeset);

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
