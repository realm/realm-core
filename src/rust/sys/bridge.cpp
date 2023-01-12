#include "bridge.hpp"
#include "realm/db_options.hpp"
#include "realm/version_id.hpp"
#include "realm-sys.hpp"

namespace realm::rust {

namespace {
inline realm::MemRef bridge(MemRef)
{
    REALM_TERMINATE("todo");
}

inline MemRef bridge(realm::MemRef memref)
{
    return MemRef{memref.get_addr(), memref.get_ref()};
}

inline realm::DBOptions bridge(DBOptions options)
{
    // TODO
    return realm::DBOptions{};
}

constexpr inline realm::VersionID bridge(VersionID version) noexcept
{
    return realm::VersionID(version.version, version.index);
}

inline realm::StringData bridge(::rust::Str str) noexcept
{
    return realm::StringData(str.data(), str.length());
}

inline ::rust::Str bridge(StringData str) noexcept
{
    return ::rust::Str{str.data(), str.size()};
}

inline realm::TableKey bridge(realm::rust::TableKey key) noexcept
{
    return realm::TableKey{key.value};
}
inline realm::rust::TableKey bridge(realm::TableKey key) noexcept
{
    return realm::rust::TableKey{key.value};
}
inline realm::ColKey bridge(realm::rust::ColKey key) noexcept
{
    return realm::ColKey{key.value};
}
inline realm::rust::ColKey bridge(realm::ColKey key) noexcept
{
    return realm::rust::ColKey{key.value};
}
inline realm::ObjKey bridge(realm::rust::ObjKey key) noexcept
{
    return realm::ObjKey{key.value};
}
inline realm::rust::ObjKey bridge(realm::ObjKey key) noexcept
{
    return realm::rust::ObjKey{key.value};
}

inline realm::rust::TableRef bridge(realm::ConstTableRef table_ref) noexcept
{
    return realm::rust::TableRef{table_ref.unchecked_ptr(), table_ref.instance_version()};
}

static_assert(sizeof(realm::TableKey) == sizeof(realm::rust::TableKey));
static_assert(sizeof(realm::ColKey) == sizeof(realm::rust::ColKey));
static_assert(sizeof(realm::ObjKey) == sizeof(realm::rust::ObjKey));
} // namespace

Allocator* get_default_allocator() noexcept
{
    return &Allocator::get_default();
}

MemRef allocator_alloc(const Allocator& alloc, size_t size)
{
    return bridge(const_cast<Allocator&>(alloc).alloc(size));
}

void allocator_free(const Allocator& alloc, MemRef mem) noexcept
{
    const_cast<Allocator&>(alloc).free_(bridge(mem));
}

std::shared_ptr<DB> db_create(::rust::Slice<const uint8_t> path, bool no_create, const DBOptions& options)
{
    return DB::create(std::string(reinterpret_cast<const char*>(path.data()), path.size()), no_create,
                      bridge(options));
}

std::shared_ptr<DB> db_create_with_replication(std::unique_ptr<Replication> repl, ::rust::Slice<const uint8_t> path,
                                               const DBOptions& options)
{
    return DB::create(std::move(repl), std::string(reinterpret_cast<const char*>(path.data()), path.size()),
                      bridge(options));
}

void db_delete_files(::rust::Slice<const uint8_t> path, bool& did_delete)
{
    auto path_str = std::string{reinterpret_cast<const char*>(path.data()), path.size()};
    DB::delete_files(path_str, &did_delete, false);
}

void db_delete_files_and_lockfile(::rust::Slice<const uint8_t> path, bool& did_delete, bool delete_lockfile)
{
    auto path_str = std::string{reinterpret_cast<const char*>(path.data()), path.size()};
    DB::delete_files(path_str, &did_delete, delete_lockfile);
}

realm::TransactionRef db_start_read(const DB& db, VersionID version)
{
    return const_cast<DB&>(db).start_read(bridge(version));
}

realm::TransactionRef db_start_write(const DB& db, bool nonblocking)
{
    return const_cast<DB&>(db).start_write(nonblocking);
}

realm::TransactionRef db_start_frozen(const DB& db, VersionID version)
{
    return const_cast<DB&>(db).start_frozen(bridge(version));
}

const Allocator& txn_get_alloc(const Transaction& txn) noexcept
{
    return _impl::GroupFriend::get_alloc(txn);
}

ref_type txn_get_top_ref(const Transaction& txn) noexcept
{
    return _impl::GroupFriend::get_top_ref(txn);
}

uint64_t txn_commit(const Transaction& txn)
{
    return const_cast<Transaction&>(txn).commit();
}

void txn_commit_and_continue_as_read(const Transaction& txn)
{
    const_cast<Transaction&>(txn).commit_and_continue_as_read();
}

void txn_commit_and_continue_writing(const Transaction& txn)
{
    const_cast<Transaction&>(txn).commit_and_continue_writing();
}

void txn_rollback(const Transaction& txn)
{
    const_cast<Transaction&>(txn).rollback();
}

void txn_rollback_and_continue_as_read(const Transaction& txn)
{
    const_cast<Transaction&>(txn).rollback_and_continue_as_read();
}

void txn_advance_read(const Transaction& txn, VersionID target_version)
{
    const_cast<Transaction&>(txn).advance_read(bridge(target_version));
}

bool txn_promote_to_write(const Transaction& txn, bool nonblocking)
{
    return const_cast<Transaction&>(txn).promote_to_write(nonblocking);
}

realm::TransactionRef txn_freeze(const Transaction& txn)
{
    return const_cast<Transaction&>(txn).freeze();
}

bool txn_has_table(const Transaction& txn, ::rust::Str name) noexcept
{
    return txn.has_table(bridge(name));
}

TableKey txn_find_table(const Transaction& txn, ::rust::Str name) noexcept
{
    return bridge(txn.find_table(bridge(name)));
}

::rust::Str txn_get_table_name(const Transaction& txn, TableKey key)
{
    return bridge(txn.get_table_name(bridge(key)));
}

bool txn_table_is_public(const Transaction& txn, TableKey key)
{
    return txn.table_is_public(bridge(key));
}

realm::rust::TableRef txn_get_table(const Transaction& txn, TableKey key)
{
    auto t = const_cast<Transaction&>(txn).get_table(bridge(key));
    return bridge(t);
}

realm::rust::TableRef txn_get_table_by_name(const Transaction& txn, ::rust::Str name)
{
    StringData name_sd{bridge(name)};
    auto t = const_cast<Transaction&>(txn).get_table(name_sd);
    return bridge(t);
}

realm::rust::TableRef txn_add_table(const Transaction& txn, ::rust::Str name, TableType type)
{
    auto t = const_cast<Transaction&>(txn).add_table(bridge(name), type);
    return bridge(t);
}

realm::rust::TableRef txn_add_table_with_primary_key(const Transaction& txn, ::rust::Str name, DataType pk_type,
                                                     ::rust::Str pk_name, bool nullable, TableType type)
{
    auto t = const_cast<Transaction&>(txn).add_table_with_primary_key(bridge(name), pk_type, bridge(pk_name),
                                                                      nullable, type);
    return bridge(t);
}

realm::rust::TableRef txn_get_or_add_table(const Transaction& txn, ::rust::Str name, TableType type)
{
    auto t = const_cast<Transaction&>(txn).get_or_add_table(bridge(name), type);
    return bridge(t);
}

realm::rust::TableRef txn_get_or_add_table_with_primary_key(const Transaction& txn, ::rust::Str name,
                                                            DataType pk_type, ::rust::Str pk_name, bool nullable,
                                                            TableType type)
{
    auto t = const_cast<Transaction&>(txn).get_or_add_table_with_primary_key(bridge(name), pk_type, bridge(pk_name),
                                                                             nullable, type);
    return bridge(t);
}

void txn_remove_table(const Transaction& txn, TableKey key)
{
    REALM_TERMINATE("TODO");
}

void txn_remove_table_by_name(const Transaction& txn, ::rust::Str name)
{
    REALM_TERMINATE("TODO");
}

::rust::Str table_get_name(const Table& table) noexcept
{
    return bridge(table.get_name());
}

std::unique_ptr<Obj> table_get_object(const Table&)
{
    REALM_TERMINATE("TODO");
}

std::unique_ptr<Obj> table_create_object(const Table& table)
{
    return std::make_unique<Obj>(const_cast<Table&>(table).create_object());
}

TableKey table_get_key(const Table& table) noexcept
{
    return bridge(table.get_key());
}

const Spec& table_get_spec(const Table& table)
{
    return realm::_impl::TableFriend::get_spec(table);
}

ColKey table_add_column(const Table& table, DataType ty, ::rust::Str name, bool nullable)
{
    return bridge(const_cast<Table&>(table).add_column(ty, bridge(name), nullable));
}

bool table_traverse_clusters(const Table& table, char* userdata,
                             ::rust::Fn<IteratorControl(const Cluster&, char*)> function)
{
    return table.traverse_clusters([&](const Cluster* cluster) noexcept -> IteratorControl {
        REALM_ASSERT(cluster);
        return function(*cluster, userdata);
    });
}

ColKey spec_get_key(const Spec& spec, size_t column_ndx)
{
    REALM_TERMINATE("TODO");
}

ColumnType spec_get_column_type(const Spec& spec, size_t column_ndx) noexcept
{
    return spec.get_column_type(column_ndx);
}

::rust::Str spec_get_column_name(const Spec& spec, size_t column_ndx) noexcept
{
    return bridge(spec.get_column_name(column_ndx));
}

size_t spec_get_column_index(const Spec& spec, ::rust::Str name) noexcept
{
    REALM_TERMINATE("TODO");
}

::rust::Str obj_get_string(const Obj& obj, ColKey col_key)
{
    return bridge(obj.get<StringData>(bridge(col_key)));
}

int64_t obj_get_int(const Obj& obj, ColKey col_key)
{
    return obj.get<int64_t>(bridge(col_key));
}

void obj_set_string(const Obj& obj, ColKey col_key, ::rust::Str value)
{
    const_cast<Obj&>(obj).set<StringData>(bridge(col_key), bridge(value));
}

void obj_set_int(const Obj& obj, ColKey col_key, int64_t value)
{
    const_cast<Obj&>(obj).set(bridge(col_key), value);
}

ref_type cluster_get_keys_ref(const Cluster& cluster) noexcept
{
    return to_ref(cluster.Array::get_as_ref(0));
}

ref_type cluster_get_column_ref(const Cluster& cluster, size_t column_ndx) noexcept
{
    return to_ref(cluster.Array::get_as_ref(column_ndx + 1));
}

::rust::Str table_name_to_class_name(::rust::Str) noexcept
{
    REALM_TERMINATE("TODO");
}

::rust::String class_name_to_table_name(::rust::Str) noexcept
{
    REALM_TERMINATE("TODO");
}

NodeHeaderWidthType array_get_width_type(const Array& array) noexcept
{
    char* header = array.get_mem().get_addr();
    return Array::get_wtype_from_header(header);
}

} // namespace realm::rust