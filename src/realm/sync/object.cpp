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


void erase_table(Transaction& g, TableRef table)
{
    SyncReplication* repl = sync_replication_for_group(g);

    if (repl) {
        repl->prepare_erase_table(table->get_name());
    }

    TableKey table_index = table->get_key();

    g.remove_table(table_index);
}

void erase_table(Transaction& g, StringData name)
{
    erase_table(g, g.get_table(name));
}

ColKey add_array_column(Table& table, DataType element_type, StringData column_name, bool is_nullable)
{
    return table.add_column_list(element_type, column_name, is_nullable);
}

bool table_has_primary_key(const Table& table)
{
    return table.get_primary_key_column().operator bool();
}

PrimaryKey primary_key_for_row(const Table& table, ObjKey key)
{
    auto obj = table.get_object(key);
    return primary_key_for_row(obj);
}

PrimaryKey primary_key_for_row(const Obj& obj)
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

ObjKey row_for_object_id(const Table& table, GlobalKey id)
{
    return table.get_objkey(id);
}

Obj obj_for_object_id(const Table& table, GlobalKey id)
{
    ObjKey key = table.get_objkey(id);

    try {
        auto obj = table.get_object(key);
        return obj;
    }
    catch (const KeyNotFound&) {
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
    return {};
}

Obj obj_for_primary_key(const Table& table, PrimaryKey key)
{
    ObjKey obj_key = row_for_primary_key(table, key);
    REALM_ASSERT(obj_key);
    return table.get_object(obj_key);
}

Obj create_object_with_primary_key(Table& t, util::Optional<int64_t> primary_key)
{
    return t.create_object_with_primary_key(primary_key ? Mixed(*primary_key) : Mixed());
}

Obj create_object_with_primary_key(Table& t, StringData primary_key)
{
    return t.create_object_with_primary_key(primary_key);
}

Obj create_object_with_primary_key(Table& t, int64_t primary_key)
{
    return create_object_with_primary_key(t, util::some<int64_t>(primary_key));
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
