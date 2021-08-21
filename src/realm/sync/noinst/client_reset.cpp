#include <set>
#include <vector>

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/set.hpp>

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
bool _copy_list(LstBasePtr src, LstBasePtr dst)
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
    size_t len_src = src->size();
    size_t len_dst = dst->size();
    size_t len_min = std::min(len_src, len_dst);

    size_t ndx = 0;
    size_t suffix_len = 0;

    while (ndx < len_min && src->get_any(ndx) == dst->get_any(ndx)) {
        ndx++;
    }

    size_t suffix_len_max = len_min - ndx;
    while (suffix_len < suffix_len_max &&
           src->get_any(len_src - 1 - suffix_len) == dst->get_any(len_dst - 1 - suffix_len)) {
        suffix_len++;
    }

    len_min -= (ndx + suffix_len);

    for (size_t i = 0; i < len_min; i++) {
        auto val = src->get_any(ndx);
        if (dst->get_any(ndx) != val) {
            dst->set_any(ndx, val);
        }
        ndx++;
    }

    // New elements must be inserted in dst.
    while (len_dst < len_src) {
        dst->insert_any(ndx, src->get_any(ndx));
        len_dst++;
        ndx++;
        updated = true;
    }
    // Excess elements must be removed from ll_dst.
    if (len_dst > len_src) {
        dst->remove(len_src - suffix_len, len_dst - suffix_len);
        updated = true;
    }

    REALM_ASSERT(dst->size() == len_src);
    return updated;
}

bool copy_list(const Obj& src_obj, ColKey src_col, Obj& dst_obj, ColKey dst_col)
{
    LstBasePtr src = src_obj.get_listbase_ptr(src_col);
    LstBasePtr dst = dst_obj.get_listbase_ptr(dst_col);
    return _copy_list(std::move(src), std::move(dst));
}

bool copy_set(const Obj& src_obj, ColKey src_col, Obj& dst_obj, ColKey dst_col)
{
    SetBasePtr src = src_obj.get_setbase_ptr(src_col);
    SetBasePtr dst = dst_obj.get_setbase_ptr(dst_col);

    std::vector<size_t> sorted_src, sorted_dst, to_insert, to_delete;
    constexpr bool ascending = true;
    // the implementation could be storing elements in sorted order, but
    // we don't assume that here.
    src->sort(sorted_src, ascending);
    dst->sort(sorted_dst, ascending);

    size_t dst_ndx = 0;
    for (size_t src_ndx = 0; src_ndx < sorted_src.size(); ++src_ndx) {
        size_t ndx_in_src = sorted_src[src_ndx];
        Mixed src_val = src->get_any(ndx_in_src);
        while (dst_ndx < sorted_dst.size()) {
            size_t ndx_in_dst = sorted_dst[dst_ndx];
            Mixed dst_val = dst->get_any(ndx_in_dst);
            int cmp = src_val.compare(dst_val); // FIXME: need to compare pks of objects not ObjKeys
            if (cmp == 0) {
                // equal: advance both src and dst
                ++dst_ndx;
                break;
            }
            else if (cmp < 0) {
                // src < dst: insert src, advance src only
                to_insert.push_back(ndx_in_src);
                break;
            }
            else {
                // src > dst: delete dst, advance only dst
                to_delete.push_back(ndx_in_dst);
                ++dst_ndx;
                continue;
            }
        }
        if (dst_ndx >= sorted_dst.size()) {
            to_insert.push_back(ndx_in_src);
        }
    }
    while (dst_ndx < sorted_dst.size()) {
        to_delete.push_back(sorted_dst[dst_ndx++]);
    }

    std::sort(to_delete.begin(), to_delete.end());
    for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
        dst->erase_any(dst->get_any(*it));
    }
    for (auto ndx : to_insert) {
        dst->insert_any(src->get_any(ndx));
    }
    return to_delete.size() || to_insert.size();
}

bool copy_dictionary(const Obj& src_obj, ColKey src_col, Obj& dst_obj, ColKey dst_col)
{
    Dictionary src = src_obj.get_dictionary(src_col);
    Dictionary dst = dst_obj.get_dictionary(dst_col);

    std::vector<size_t> sorted_src, sorted_dst, to_insert, to_delete;
    constexpr bool ascending = true;
    src.sort_keys(sorted_src, ascending);
    dst.sort_keys(sorted_dst, ascending);

    size_t dst_ndx = 0;
    for (size_t src_ndx = 0; src_ndx < sorted_src.size(); ++src_ndx) {
        auto src_val = src.get_pair(sorted_src[src_ndx]);

        while (dst_ndx < sorted_dst.size()) {
            auto dst_val = dst.get_pair(sorted_dst[dst_ndx]);
            int cmp = src_val.first.compare(dst_val.first);
            if (cmp == 0) {
                // FIXME: need to compare pks of objects not ObjKeys
                cmp = src_val.second.compare(dst_val.second);
            }
            if (cmp == 0) {
                // equal: advance both src and dst
                ++dst_ndx;
                break;
            }
            else if (cmp < 0) {
                // src < dst: insert src, advance src only
                to_insert.push_back(sorted_src[src_ndx]);
                break;
            }
            else {
                // src > dst: delete dst, advance only dst
                to_delete.push_back(sorted_dst[dst_ndx]);
                ++dst_ndx;
                continue;
            }
        }
        if (dst_ndx >= sorted_dst.size()) {
            to_insert.push_back(sorted_src[src_ndx]);
        }
    }
    while (dst_ndx < sorted_dst.size()) {
        to_delete.push_back(sorted_dst[dst_ndx++]);
    }

    std::sort(to_delete.begin(), to_delete.end());
    for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
        dst.erase(dst.begin() + *it);
    }
    for (auto ndx : to_insert) {
        auto pair = src.get_pair(ndx);
        dst.insert(pair.first, pair.second);
    }
    return to_delete.size() || to_insert.size();
}

bool copy_linklist(LnkLst& ll_src, LnkLst& ll_dst, std::function<ObjKey(ObjKey)> convert_object_keys)
{
    // This function ensures that the link list in ll_dst is equal to the
    // link list in ll_src with equality defined by the conversion function
    // convert_object_keys.

    bool updated = false;
    size_t len_src = ll_src.size();
    size_t len_dst = ll_dst.size();

    size_t prefix_len, suffix_len;

    for (prefix_len = 0; prefix_len < len_src && prefix_len < len_dst; ++prefix_len) {
        auto key_src = ll_src.get(prefix_len);
        auto key_dst = ll_dst.get(prefix_len);
        auto key_converted = convert_object_keys(key_src);
        if (key_converted != key_dst)
            break;
    }

    for (suffix_len = 0; prefix_len + suffix_len < len_src && prefix_len + suffix_len < len_dst; ++suffix_len) {
        auto key_src = ll_src.get(len_src - 1 - suffix_len);
        auto key_dst = ll_dst.get(len_dst - 1 - suffix_len);
        auto key_converted = convert_object_keys(key_src);
        if (key_converted != key_dst)
            break;
    }

    if (len_src > len_dst) {
        // New elements must be inserted in ll_dst.
        for (size_t i = prefix_len; i < prefix_len + (len_src - len_dst); ++i) {
            auto key_src = ll_src.get(i);
            auto key_converted = convert_object_keys(key_src);
            ll_dst.insert(i, key_converted);
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
        auto key_src = ll_src.get(i);
        auto key_converted = convert_object_keys(key_src);
        ll_dst.set(i, key_converted); // FIXME: updated should be set if this differs
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
            REALM_ASSERT(has_pk); // a sync table will have a pk
            auto pk_col_src = table_src->get_primary_key_column();
            DataType pk_type = DataType(pk_col_src.get_type());
            StringData pk_col_name = table_src->get_column_name(pk_col_src);
            group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, pk_col_src.is_nullable());
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
    // FIXME: make sure that the pk columns are the same?

    // Now the schemas are identical.

    // Remove objects in dst that are absent in src.
    // We will also have to remove all objects created locally as they should have
    // new keys because the client file id is changed.
    for (auto table_key : group_src.get_table_keys()) {
        auto table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        logger.debug("Removing objects in '%1'", table_name);
        auto table_dst = group_dst.get_table(table_name);

        auto pk_col = table_dst->get_primary_key_column();
        REALM_ASSERT(pk_col); // sync realms always have a pk
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

    // Add objects that are present in src but absent in dst.
    for (auto table_key : group_src.get_table_keys()) {
        auto table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        if (!table_name.begins_with("class"))
            continue;
        logger.debug("Adding objects in '%1'", table_name);
        auto table_dst = group_dst.get_table(table_name);

        // the following code relies on both tables having a pk
        // which is true of a sync'd Realm
        REALM_ASSERT(table_src->get_primary_key_column());
        REALM_ASSERT(table_dst->get_primary_key_column());

        for (auto& obj : *table_src) {
            auto src_pk = obj.get_primary_key();
            auto key_dst = table_dst->find_primary_key(src_pk);
            if (!key_dst || !table_dst->is_valid(key_dst)) {
                logger.debug("  adding '%1'", src_pk);
                table_dst->create_object_with_primary_key(src_pk);
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
        auto pk_col = table_src->get_primary_key_column();
        REALM_ASSERT(table_src->get_primary_key_column());
        logger.debug("Updating values for table '%1', number of rows = %2, "
                     "number of columns = %3, primary_key_col = %4, "
                     "primary_key_type = %5",
                     table_name, table_src->size(), table_src->get_column_count(), pk_col.get_index().val,
                     pk_col.get_type());

        for (const Obj& src : *table_src) {
            auto src_pk = src.get_primary_key();
            auto dst = table_dst->get_object_with_primary_key(src_pk);
            REALM_ASSERT(dst);
            bool updated = false;

            for (ColKey col_key_src : table_src->get_column_keys()) {
                if (col_key_src == pk_col)
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
                        auto target_pk_src = table_target_src->get_primary_key(target_obj_key_src);
                        auto dst_obj_key = dst.get<ObjKey>(col_key_dst);
                        if (!dst_obj_key ||
                            target_pk_src != table_target_dst->get_primary_key(dst.get<ObjKey>(col_key_dst))) {
                            ObjKey target_obj_key_dst = table_target_dst->get_objkey_from_primary_key(target_pk_src);
                            dst.set(col_key_dst, target_obj_key_dst);
                            updated = true;
                        }
                    }
                }
                else if (col_type == type_LinkList) {
                    ConstTableRef table_target_src = table_src->get_link_target(col_key_src);
                    TableRef table_target_dst = table_dst->get_link_target(col_key_dst);
                    REALM_ASSERT(table_target_src->get_name() == table_target_dst->get_name());
                    // convert_keys converts the object key in table_target_src
                    // to the object key in table_target_dst such that the
                    // primary keys are the same.
                    auto convert_keys = [&](ObjKey key_src) {
                        auto src_pk = table_target_src->get_primary_key(key_src);
                        auto key_dst = table_target_dst->find_primary_key(src_pk);
                        REALM_ASSERT(key_dst);
                        return key_dst;
                    };
                    auto ll_src = src.get_linklist(col_key_src);
                    auto ll_dst = dst.get_linklist(col_key_dst);
                    if (copy_linklist(ll_src, ll_dst, convert_keys)) {
                        updated = true;
                    }
                }
                else if (col_key_src.is_list()) {
                    if (copy_list(src, col_key_src, dst, col_key_dst)) {
                        updated = true;
                    }
                }
                else if (col_key_src.is_dictionary()) {
                    if (copy_dictionary(src, col_key_src, dst, col_key_dst)) {
                        updated = true;
                    }
                }
                else if (col_key_src.is_set()) {
                    if (copy_set(src, col_key_src, dst, col_key_dst)) {
                        updated = true;
                    }
                }
                else {
                    REALM_ASSERT(!col_key_src.is_collection());
                    auto val_src = src.get_any(col_key_src);
                    auto val_dst = dst.get_any(col_key_dst);
                    if (val_src != val_dst) {
                        dst.set_any(col_key_dst, val_src);
                        updated = true;
                    }
                }
            }
            if (updated) {
                logger.debug("  updating %1", src_pk);
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

client_reset::LocalVersionIDs client_reset::perform_client_reset_diff(
    const std::string& path_local, const util::Optional<std::string> path_fresh,
    const util::Optional<std::array<char, 64>>& encryption_key,
    std::function<void(TransactionRef local, TransactionRef remote)> notify_before,
    std::function<void(TransactionRef local)> notify_after, sync::SaltedFileIdent client_file_ident,
    util::Logger& logger)
{
    logger.info("Client reset, path_local = %1, "
                "encryption = %2, client_file_ident.ident = %3, "
                "client_file_ident.salt = %4,",
                path_local, (encryption_key ? "on" : "off"), client_file_ident.ident, client_file_ident.salt);

    DBOptions shared_group_options(encryption_key ? encryption_key->data() : nullptr);
    ClientHistoryImpl history_local{path_local};
    DBRef sg_local = DB::create(history_local, shared_group_options);
    DBRef sg_remote;
    std::unique_ptr<ClientHistoryImpl> history_remote;

    if (path_fresh) {
        history_remote = std::make_unique<ClientHistoryImpl>(*path_fresh);
        sg_remote = DB::create(*history_remote, shared_group_options);
    }
    if (notify_before) {
        TransactionRef local_frozen, remote_frozen;
        if (sg_local) {
            local_frozen = sg_local->start_frozen();
        }
        if (sg_remote) {
            remote_frozen = sg_remote->start_frozen();
        }
        notify_before(local_frozen, remote_frozen);
    }
    auto group_local = sg_local->start_write();
    VersionID old_version_local = group_local->get_version_of_current_transaction();
    sync::version_type current_version_local = old_version_local.version;
    group_local->get_history()->ensure_updated(current_version_local);
    BinaryData recovered_changeset;
    sync::SaltedVersion fresh_server_version = {0, 0};

    // changes made here are reflected in the notifier logs
    if (path_fresh) { // seamless_loss mode
        REALM_ASSERT(sg_remote);
        REALM_ASSERT(history_remote);
        auto wt_remote = sg_remote->start_write();
        sync::version_type current_version_remote = wt_remote->get_version();
        history_local.set_client_file_ident_in_wt(current_version_local, client_file_ident);
        history_remote->set_client_file_ident_in_wt(current_version_remote, client_file_ident);

        sync::version_type remote_version;
        SaltedFileIdent remote_ident;
        SyncProgress remote_progress;
        history_remote->get_status(remote_version, remote_ident, remote_progress);
        fresh_server_version = remote_progress.latest_server_version;

        transfer_group(*wt_remote, *group_local, logger);

        // Extract the changeset produced in the remote Realm during recovery.
        sync::ChangesetEncoder& instruction_encoder = history_remote->get_instruction_encoder();
        const sync::ChangesetEncoder::Buffer& buffer = instruction_encoder.buffer();
        recovered_changeset = {buffer.data(), buffer.size()};
        //        {
        //            // Debug.
        //            ChunkedBinaryInputStream in{recovered_changeset};
        //            sync::Changeset log;
        //            sync::parse_changeset(in, log); // Throws
        //            log.print();
        //        }
    }
    else { // manual discard mode
        remove_all_tables(*group_local, logger);
    }
    uint_fast64_t downloaded_bytes = 0; // FIXME: check this
    history_local.set_client_reset_adjustments(current_version_local, client_file_ident, fresh_server_version,
                                               downloaded_bytes, recovered_changeset);

    // Finally, the local Realm is committed.
    group_local->commit_and_continue_as_read();
    VersionID new_version_local = group_local->get_version_of_current_transaction();
    logger.debug("perform_client_reset_diff is done, old_version.version = %1, "
                 "old_version.index = %2, new_version.version = %3, "
                 "new_version.index = %4",
                 old_version_local.version, old_version_local.index, new_version_local.version,
                 new_version_local.index);
    if (notify_after) {
        TransactionRef local_frozen;
        if (sg_local) {
            local_frozen = sg_local->start_frozen();
        }
        notify_after(local_frozen);
    }

    return LocalVersionIDs{old_version_local, new_version_local};
}
