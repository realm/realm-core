#include <realm/sync/object.hpp>
#include <realm/sync/object_id.hpp>
#include <realm/sync/instruction_replication.hpp>

#include <realm/table.hpp>
#include <realm/group.hpp>
#include <realm/db.hpp>

#include <set>
#include <map>

namespace realm {
namespace sync {

ConstTableRef TableInfoCache::TableInfo::get_table(const Transaction& t) const
{
    return t.get_table(key);
}

TableInfoCache::TableInfoCache(const Transaction& t)
    : m_transaction(t)
{
}

void TableInfoCache::clear()
{
    m_table_info.clear();
}

void TableInfoCache::clear_last_object(const Table& table)
{
    auto it = m_table_info.find(table.get_key()); // Throws

    if (it != m_table_info.end()) {
        it->second.clear_last_object();
    }
}

bool has_object_ids(const Table& t)
{
    const Group& group = *_impl::TableFriend::get_parent_group(t);
    const Replication* r = group.get_replication();
    return dynamic_cast<const SyncReplication*>(r);
}

namespace {

SyncReplication* sync_replication_for_group(Group& g)
{
    Replication* r = g.get_replication();
    return dynamic_cast<SyncReplication*>(r);
}

} // unnamed namespace

bool is_object_id_stability_achieved(const DB&, const Transaction& transaction)
{
    return transaction.get_sync_file_id() != 0;
}


void erase_table(Transaction& g, TableInfoCache& table_info_cache, TableRef table)
{
    SyncReplication* repl = sync_replication_for_group(g);

    if (repl) {
        repl->prepare_erase_table(table->get_name());
    }

    TableKey table_index = table->get_key();

    g.remove_table(table_index);
    table_info_cache.clear();
}

void erase_table(Transaction& g, TableInfoCache& table_info_cache, StringData name)
{
    erase_table(g, table_info_cache, g.get_table(name));
}

ColKey add_array_column(Table& table, DataType element_type, StringData column_name, bool is_nullable)
{
    return table.add_column_list(element_type, column_name, is_nullable);
}

const TableInfoCache::TableInfo& TableInfoCache::get_table_info(TableKey table_key) const
{
    auto it = m_table_info.find(table_key); // Throws

    if (it == m_table_info.end()) {
        ConstTableRef table = m_transaction.get_table(table_key);
        TableInfo info;
        info.key = table_key;
        info.name = table->get_name();
        info.primary_key_col = table->get_primary_key_column();
        if (info.primary_key_col) {
            info.primary_key_type = table->get_column_type(info.primary_key_col);
            info.primary_key_nullable = table->is_nullable(info.primary_key_col);
        }
        it = m_table_info.insert({table_key, std::move(info)}).first;
    }

    return it->second;
}

const TableInfoCache::TableInfo& TableInfoCache::get_table_info(const Table& table) const
{
    return get_table_info(table.get_key());
}

void TableInfoCache::verify()
{
    REALM_ASSERT(m_table_info.size() <= m_transaction.size());
    for (auto it : m_table_info) {
        TableInfo& table_info = it.second;
        ConstTableRef table = m_transaction.get_table(it.first);
        REALM_ASSERT(table_info.name == table->get_name());
        if (table_info.last_obj_key) {
            REALM_ASSERT(table->is_valid(table_info.last_obj_key));
            GlobalKey real_object_id = table->get_object_id(table_info.last_obj_key);
            REALM_ASSERT_DEBUG(table_info.last_object_id == real_object_id);
        }
    }
}

bool table_has_primary_key(const TableInfoCache& cache, const Table& table)
{
    return bool(cache.get_table_info(table).primary_key_col);
}


GlobalKey object_id_for_row(const TableInfoCache& cache, const Table& table, ObjKey key)
{
    const auto& info = cache.get_table_info(table);
    if (info.last_obj_key != key) {
        info.last_obj_key = key;
        info.last_object_id = table.get_object_id(key);
    }
    REALM_ASSERT_DEBUG(info.last_object_id == table.get_object_id(key));
    return info.last_object_id;
}

GlobalKey object_id_for_row(const TableInfoCache& cache, const ConstObj& obj)
{
    return object_id_for_row(cache, *obj.get_table(), obj.get_key());
}

PrimaryKey primary_key_for_row(const Table& table, ObjKey key)
{
    auto obj = table.get_object(key);
    return primary_key_for_row(obj);
}

PrimaryKey primary_key_for_row(const ConstObj& obj)
{
    auto table = obj.get_table();
    ColKey pk_col = table->get_primary_key_column();
    if (pk_col) {
        ColumnType pk_type = pk_col.get_type();
        if (obj.is_null(pk_col)) {
            return mpark::monostate{};
        }

        if (pk_type == col_type_Int) {
            return obj.get<int64_t>(pk_col);
        }

        if (pk_type == col_type_String) {
            return obj.get<StringData>(pk_col);
        }

        if (pk_type == col_type_ObjectId) {
            return obj.get<ObjectId>(pk_col);
        }

        REALM_TERMINATE("Missing primary key type support");
    }

    GlobalKey global_key = obj.get_object_id();
    return global_key;
}

ObjKey row_for_object_id(const TableInfoCache& cache, const Table& table, GlobalKey id)
{
    const auto& info = cache.get_table_info(table);
    if (info.last_object_id != id) {
        if (auto key = table.get_objkey(id)) {
            info.last_object_id = id;
            info.last_obj_key = key;
        }
        else {
            return realm::null_key;
        }
    }
    REALM_ASSERT_DEBUG(info.last_obj_key == table.get_objkey(id));
    return info.last_obj_key;
}

Obj obj_for_object_id(const TableInfoCache& cache, const Table& table, GlobalKey id)
{
    const auto& info = cache.get_table_info(table);
    ObjKey key = (info.last_object_id == id) ? info.last_obj_key : table.get_objkey(id);

    try {
        auto obj = table.get_object(key);
        info.last_object_id = id;
        info.last_obj_key = key;
        REALM_ASSERT_DEBUG(info.last_obj_key == table.get_objkey(id));
        return obj;
    }
    catch (const KeyNotFound&) {
        info.last_obj_key = ObjKey();
        info.last_object_id = {};
    }
    return {};
}

ObjKey row_for_primary_key(const Table& table, PrimaryKey key)
{
    ColKey pk_col = table.get_primary_key_column();
    if (pk_col) {
        ColumnType pk_type = pk_col.get_type();

        if (auto pk = mpark::get_if<mpark::monostate>(&key)) {
            static_cast<void>(pk);
            if (!pk_col.is_nullable()) {
                REALM_TERMINATE("row_for_primary_key with null on non-nullable primary key column");
            }
            return table.find_primary_key({});
        }

        if (pk_type == col_type_Int) {
            if (auto pk = mpark::get_if<int64_t>(&key)) {
                return table.find_primary_key(*pk);
            }
            else {
                REALM_TERMINATE("row_for_primary_key mismatching primary key type (expected int)");
            }
        }

        if (pk_type == col_type_String) {
            if (auto pk = mpark::get_if<StringData>(&key)) {
                return table.find_primary_key(*pk);
            }
            else {
                REALM_TERMINATE("row_for_primary_key mismatching primary key type (expected string)");
            }
        }

        if (pk_type == col_type_ObjectId) {
            if (auto pk = mpark::get_if<ObjectId>(&key)) {
                return table.find_primary_key(*pk);
            }
            else {
                REALM_TERMINATE("row_for_primary_key mismatching primary key type (expected ObjectId)");
            }
        }

        REALM_TERMINATE("row_for_primary_key missing primary key type support");
    }

    if (auto global_key = mpark::get_if<GlobalKey>(&key)) {
        return table.get_objkey(*global_key);
    }
    else {
        REALM_TERMINATE("row_for_primary_key() with primary key, expected GlobalKey");
    }
}

ConstObj obj_for_primary_key(const Table& table, PrimaryKey key)
{
    ObjKey obj_key = row_for_primary_key(table, key);
    REALM_ASSERT(obj_key);
    return table.get_object(obj_key);
}

Obj obj_for_primary_key(Table& table, PrimaryKey key)
{
    ObjKey obj_key = row_for_primary_key(table, key);
    REALM_ASSERT(obj_key);
    return table.get_object(obj_key);
}

Obj create_object_with_primary_key(const TableInfoCache& cache, Table& t, util::Optional<int64_t> primary_key)
{
    REALM_ASSERT(table_has_primary_key(cache, t)); // FIXME: Exception
    return t.create_object_with_primary_key(primary_key ? Mixed(*primary_key) : Mixed());
}

Obj create_object_with_primary_key(const TableInfoCache& cache, Table& t, StringData primary_key)
{
    REALM_ASSERT(table_has_primary_key(cache, t)); // FIXME: Exception

    return t.create_object_with_primary_key(primary_key);
}

Obj create_object_with_primary_key(const TableInfoCache& cache, Table& t, int64_t primary_key)
{
    return create_object_with_primary_key(cache, t, util::some<int64_t>(primary_key));
}

} // namespace sync
} // namespace realm


using namespace realm;

namespace {

class MigrationError : public std::exception {
public:
    MigrationError(const char* message)
        : m_message{message}
    {
    }
    const char* what() const noexcept override
    {
        return m_message;
    }

private:
    const char* m_message;
};

} // unnamed namespace


void sync::import_from_legacy_format(const Group& old_group, Group& new_group, util::Logger& logger)
{
    static_cast<void>(old_group);
    static_cast<void>(new_group);
    static_cast<void>(logger);
    // FIXME: TODO
}
