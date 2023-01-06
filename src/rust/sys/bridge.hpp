#include "realm/db_options.hpp"
#include "realm/history.hpp"
#include <realm/alloc.hpp>
#include <realm/db.hpp>
#include <realm/transaction.hpp>
#include <rust/cxx.h>

namespace realm::rust {
using Allocator = realm::Allocator;
using DB = realm::DB;
using Transaction = realm::Transaction;
using DBDurability = realm::DBOptions::Durability;
using Table = realm::Table;
using Spec = realm::Spec;
using DataType = realm::DataType::Type;
using ColumnType = realm::ColumnType::Type;
using ColumnAttr = realm::ColumnAttr;
using TableType = realm::Table::Type;
using Obj = realm::Obj;
using TransactStage = realm::DB::TransactStage;
using IteratorControl = realm::IteratorControl;
using Cluster = realm::Cluster;

using Replication = realm::Replication;
using realm::make_in_realm_history;

using ref_type = realm::ref_type;

// Shared types (not imported from `realm::`, defined in the bridge, require
// manual translation).
struct MemRef;
struct DBOptions;
struct VersionID;
struct TableKey;
struct ColKey;
struct ObjKey;
struct TableRef;


Allocator* get_default_allocator() noexcept;
MemRef allocator_alloc(const Allocator& alloc, size_t size);
void allocator_free(const Allocator& alloc, MemRef mem) noexcept;
std::shared_ptr<DB> db_create(::rust::Slice<const uint8_t> file, bool no_create, const DBOptions&);
std::shared_ptr<DB> db_create_with_replication(std::unique_ptr<Replication>, ::rust::Slice<const uint8_t> file,
                                               const DBOptions&);
realm::TransactionRef db_start_read(const DB&, VersionID);
realm::TransactionRef db_start_frozen(const DB&, VersionID);
realm::TransactionRef db_start_write(const DB&, bool nonblocking);
void db_delete_files(::rust::Slice<const uint8_t> path, bool& did_delete);
void db_delete_files_and_lockfile(::rust::Slice<const uint8_t> path, bool& did_delete, bool delete_lockfile);

uint64_t txn_commit(const Transaction&);
void txn_commit_and_continue_as_read(const Transaction&);
void txn_commit_and_continue_writing(const Transaction&);
void txn_rollback(const Transaction&);
void txn_rollback_and_continue_as_read(const Transaction&);
void txn_advance_read(const Transaction&, VersionID target_version);
bool txn_promote_to_write(const Transaction&, bool nonblocking);
realm::TransactionRef txn_freeze(const Transaction&);
bool txn_has_table(const Transaction&, ::rust::Str name) noexcept;
TableKey txn_find_table(const Transaction&, ::rust::Str name) noexcept;
::rust::Str txn_get_table_name(const Transaction&, TableKey key);
bool txn_table_is_public(const Transaction&, TableKey key);
TableRef txn_get_table(const Transaction&, TableKey key);
TableRef txn_get_table_by_name(const Transaction&, ::rust::Str name);
TableRef txn_add_table(const Transaction&, ::rust::Str name, TableType type);
TableRef txn_add_table_with_primary_key(const Transaction&, ::rust::Str name, DataType pk_type, ::rust::Str pk_name,
                                        bool nullable, TableType type);
TableRef txn_get_or_add_table(const Transaction&, ::rust::Str name, TableType type);
TableRef txn_get_or_add_table_with_primary_key(const Transaction&, ::rust::Str name, DataType pk_type,
                                               ::rust::Str pk_name, bool nullable, TableType type);
void txn_remove_table(const Transaction&, TableKey key);
void txn_remove_table_by_name(const Transaction&, ::rust::Str name);


::rust::Str table_get_name(const Table&) noexcept;
TableKey table_get_key(const Table&) noexcept;
std::unique_ptr<Obj> table_get_object(const Table&);
std::unique_ptr<Obj> table_create_object(const Table&);
const Spec& table_get_spec(const Table&);
bool table_traverse_clusters(const Table&, char* userdata, ::rust::Fn<IteratorControl(const Cluster&, char*)>);
ColKey table_add_column(const Table&, DataType type, ::rust::Str name, bool nullable);

ColKey spec_get_key(const Spec& spec, size_t column_ndx);
ColumnType spec_get_column_type(const Spec& spec, size_t column_ndx) noexcept;
::rust::Str spec_get_column_name(const Spec& spec, size_t column_ndx) noexcept;
size_t spec_get_column_index(const Spec& spec, ::rust::Str name) noexcept;

::rust::Str table_name_to_class_name(::rust::Str) noexcept;
::rust::String class_name_to_table_name(::rust::Str) noexcept;

::rust::Str obj_get_string(const Obj& obj, ColKey col_key);
int64_t obj_get_int(const Obj& obj, ColKey col_key);

void obj_set_string(const Obj& obj, ColKey col_key, ::rust::Str value);
void obj_set_int(const Obj& obj, ColKey col_key, int64_t value);

ref_type cluster_get_keys_ref(const Cluster& cluster) noexcept;
ref_type cluster_get_column_ref(const Cluster& cluster, size_t column_ndx) noexcept;

} // namespace realm::rust