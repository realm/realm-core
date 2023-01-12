/*!
 * Raw C++ bindings to Realm Core.
 *
 * ### Const versus non-const versus borrow versus mutable borrow
 *
 * The way const/mutable C++ references are used in Core does not map directly
 * to Rust borrow/mutable borrow. On the C++ side, it is perfectly valid to have
 * multiple aliasing pointers to the same things (`Table`, `Transaction`, etc.),
 * as evidenced by the fact that they are often behind shared ptrs while still
 * expected to be used through non-const references.
 *
 * For example, the same `Table` pointer can be contained in multiple `TableRef`s
 * at the same time, and it's fine for all those `TableRef`s to be in use
 * simultaneously. But it means that we cannot form a `&mut Table` even from a
 * `&mut TableRef`, as that would be UB from Rust's perspective - even though
 * `Table` is opaque from the Rust side. The same thing goes for `Transaction`
 * and `TransactionRef`.
 *
 * However, the `cxx` crate *does* assume a one-to-one mapping between
 * const/non-const references and borrow/mutable borrow. To work around this
 * discrepancy, we treat all potentially aliasing references as non-mutable borrows
 * on the Rust side, and then `const_cast` them on the C++ side. This is valid
 * in C++ because the constness is a "temporary" situation that only exists on
 * the bridge - at no point is something considered `const` in C++ cast to a
 * non-const reference. In short, `T&` is cast to `const T&` and then back to `T&`
 * when crossing the bridge.
 *
 * ### Methods versus free functions
 *
 * Some methods in C++ cannot be represented as methods in the Rust bridge, and
 * so are wrapped in a free function (defined in bridge.cpp):
 *
 * 1. When the above const-versus-borrow applies. The bridge has to do the
 *    `const_cast`.
 * 2. When a type conversion occurs to/from any type that is transparent on the
 *    bridge (because C++ is the source of truth, but the bridge can't see those
 *    type definitions).
 *
 * Whenever possible, the bindings use methods to reduce boilerplate.
 */

#[cxx::bridge(namespace = "realm::rust")]
mod ffi {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MemRef {
        m_addr: *mut c_char,
        m_ref: usize,
    }

    #[derive(Debug, Clone, Copy)]
    #[repr(u16)]
    pub enum DBDurability {
        Full,
        MemOnly,
        Unsafe,
    }

    #[derive(Debug, Clone, Copy)]
    #[repr(i32)]
    pub enum DataType {
        Int = 0,
        Bool = 1,
        String = 2,
        Binary = 4,
        Mixed = 6,
        Timestamp = 8,
        Float = 9,
        Double = 10,
        Decimal = 11,
        Link = 12,
        LinkList = 13,
        ObjectId = 15,
        TypedLink = 16,
        UUID = 17,
    }

    #[derive(Debug, Clone, Copy)]
    #[repr(i32)]
    pub enum ColumnType {
        // Column types
        Int = 0,
        Bool = 1,
        String = 2,
        Binary = 4,
        Mixed = 6,
        Timestamp = 8,
        Float = 9,
        Double = 10,
        Decimal = 11,
        Link = 12,
        LinkList = 13,
        BackLink = 14,
        ObjectId = 15,
        TypedLink = 16,
        UUID = 17,
    }

    #[derive(Debug, Clone, Copy)]
    #[repr(i32)]
    pub enum ColumnAttr {
        col_attr_None = 0,
        col_attr_Indexed = 1,

        /// Specifies that this column forms a unique constraint. It requires
        /// `col_attr_Indexed`.
        col_attr_Unique = 2,

        /// Reserved for future use.
        col_attr_Reserved = 4,

        /// Specifies that the links of this column are strong, not weak. Applies
        /// only to link columns (`type_Link` and `type_LinkList`).
        col_attr_StrongLinks = 8,

        /// Specifies that elements in the column can be null.
        col_attr_Nullable = 16,

        /// Each element is a list of values
        col_attr_List = 32,

        /// Each element is a dictionary
        col_attr_Dictionary = 64,

        /// Each element is a set of values
        col_attr_Set = 128,

        /// Specifies that elements in the column are full-text indexed
        col_attr_FullText_Indexed = 256,

        /// Either list, dictionary, or set
        col_attr_Collection = 224,
    }

    #[derive(Debug, Clone, Copy)]
    #[repr(u8)]
    pub enum TableType {
        TopLevel = 0,
        Embedded = 1,
        TopLevelAsymmetric = 2,
    }

    #[derive(Debug, Clone, Copy)]
    pub struct VersionID {
        pub version: u64,
        pub index: u32,
    }

    #[derive(Debug, Default)]
    pub struct DBOptions {
        pub durability: DBDurability,
        pub encryption_key: String,
        pub allow_file_format_upgrade: bool,
        pub is_immutable: bool,
        pub backup_at_file_format_change: bool,
        pub enable_async_writes: bool,
    }

    #[derive(Debug, Clone, Copy)]
    #[repr(i32)]
    pub enum TransactStage {
        transact_Ready,
        transact_Reading,
        transact_Writing,
        transact_Frozen,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Hash)]
    pub struct TableKey {
        value: u32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Hash)]
    pub struct ColKey {
        value: i64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Hash)]
    pub struct ObjKey {
        value: i64,
    }

    pub struct TableRef {
        pub ptr: *const Table,
        pub instance_version: u64,
    }

    /// Corresponding to `realm::ClusterNode::State`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct ClusterNodeState {
        pub split_key: i64,
        pub mem: MemRef,
        pub index: usize,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(i32)]
    pub enum IteratorControl {
        AdvanceToNext,
        Stop,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(i32)]
    pub enum NodeHeaderType {
        type_Normal,
        type_InnerBptreeNode,
        type_HasRefs,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(i32)]
    pub enum NodeHeaderWidthType {
        wtype_Bits = 0,
        wtype_Multiply = 1,
        wtype_Ignore = 2,
    }

    unsafe extern "C++" {
        include!("bridge.hpp");
        type DBDurability;
        type DataType;
        type ColumnType;
        type ColumnAttr;
        type IteratorControl;
    }

    unsafe extern "C++" {
        pub type Allocator;
        unsafe fn translate(self: &Allocator, ref_: usize) -> *mut c_char;
        fn is_read_only(self: &Allocator, ref_: usize) -> bool;
        fn verify(self: &Allocator) -> Result<()>;

        fn get_default_allocator() -> *mut Allocator;

        fn allocator_alloc(allocator: &Allocator, size: usize) -> Result<MemRef>;
        unsafe fn allocator_free(allocator: &Allocator, mem: MemRef);
    }

    unsafe extern "C++" {
        pub type DB;
        fn get_alloc(self: Pin<&mut DB>) -> Result<Pin<&mut Allocator>>;
        fn get_number_of_versions(self: Pin<&mut DB>) -> Result<u64>;
        fn get_path(self: &DB) -> &CxxString;
        fn get_allocated_size(self: &DB) -> usize;
        fn db_create(path: &[u8], no_create: bool, options: &DBOptions) -> Result<SharedPtr<DB>>;
        fn db_create_with_replication(
            repl: UniquePtr<Replication>,
            path: &[u8],
            options: &DBOptions,
        ) -> Result<SharedPtr<DB>>;
        fn db_start_read(db: &DB, version: VersionID) -> Result<SharedPtr<Transaction>>;
        fn db_start_frozen(db: &DB, version: VersionID) -> Result<SharedPtr<Transaction>>;
        fn db_start_write(db: &DB, nonblocking: bool) -> Result<SharedPtr<Transaction>>;

        fn db_delete_files(path: &[u8], did_delete: Pin<&mut bool>) -> Result<()>;
        unsafe fn db_delete_files_and_lockfile(
            path: &[u8],
            did_delete: Pin<&mut bool>,
            delete_lockfile: bool,
        ) -> Result<()>;
    }

    unsafe extern "C++" {
        pub type Replication;

        fn make_in_realm_history() -> Result<UniquePtr<Replication>>;
    }

    unsafe extern "C++" {
        pub type Transaction;
        pub type TransactStage;
        fn get_version(self: &Transaction) -> u64;
        fn get_commit_size(self: &Transaction) -> Result<usize>;
        fn get_transact_stage(self: &Transaction) -> TransactStage;
        fn is_frozen(self: &Transaction) -> bool;

        fn txn_get_alloc(txn: &Transaction) -> &Allocator;
        fn txn_get_top_ref(txn: &Transaction) -> usize; // ref_type
        fn txn_commit(txn: &Transaction) -> Result<u64>;
        fn txn_commit_and_continue_as_read(txn: &Transaction) -> Result<()>;
        fn txn_commit_and_continue_writing(txn: &Transaction) -> Result<()>;
        fn txn_rollback(txn: &Transaction) -> Result<()>;
        fn txn_rollback_and_continue_as_read(txn: &Transaction) -> Result<()>;
        // TODO: Support observers.
        fn txn_advance_read(txn: &Transaction, target_version: VersionID) -> Result<()>;
        // TODO: Support observers.
        fn txn_promote_to_write(txn: &Transaction, nonblocking: bool) -> Result<bool>;
        fn txn_freeze(txn: &Transaction) -> Result<SharedPtr<Transaction>>;

        // Group interface:

        /// True if the number of tables in the group is zero.
        fn is_empty(self: &Transaction) -> bool;

        /// Number of tables.
        fn size(self: &Transaction) -> usize;

        fn txn_has_table(txn: &Transaction, name: &str) -> bool;
        fn txn_find_table(txn: &Transaction, name: &str) -> TableKey;
        fn txn_get_table_name(txn: &Transaction, key: TableKey) -> Result<&str>;
        fn txn_table_is_public(txn: &Transaction, key: TableKey) -> Result<bool>;
        fn txn_get_table(txn: &Transaction, key: TableKey) -> Result<TableRef>;
        fn txn_get_table_by_name(txn: &Transaction, name: &str) -> Result<TableRef>;
        fn txn_add_table(txn: &Transaction, name: &str, table_type: TableType) -> Result<TableRef>;
        fn txn_add_table_with_primary_key(
            txn: &Transaction,
            name: &str,
            pk_type: DataType,
            pk_name: &str,
            nullable: bool,
            table_type: TableType,
        ) -> Result<TableRef>;
        fn txn_get_or_add_table(
            txn: &Transaction,
            name: &str,
            table_type: TableType,
        ) -> Result<TableRef>;
        fn txn_get_or_add_table_with_primary_key(
            txn: &Transaction,
            name: &str,
            pk_type: DataType,
            pk_name: &str,
            nullable: bool,
            table_type: TableType,
        ) -> Result<TableRef>;
        fn txn_remove_table(txn: &Transaction, key: TableKey) -> Result<()>;
        fn txn_remove_table_by_name(txn: &Transaction, name: &str) -> Result<()>;
    }

    unsafe extern "C++" {
        pub type Table;
        pub type TableType;

        fn is_embedded(self: &Table) -> bool;
        fn is_asymmetric(self: &Table) -> bool;
        fn size(self: &Table) -> usize;
        fn get_table_type(self: &Table) -> TableType;
        fn get_column_count(self: &Table) -> usize;
        fn get_instance_version(self: &Table) -> u64;
        fn get_state(self: &Table) -> *const c_char;

        fn table_get_key(table: &Table) -> TableKey;
        fn table_get_name(table: &Table) -> &str;
        fn table_get_object(table: &Table) -> Result<UniquePtr<Obj>>;
        fn table_create_object(table: &Table) -> Result<UniquePtr<Obj>>;
        fn table_get_spec(table: &Table) -> &Spec;
        fn table_add_column(
            table: &Table,
            ty: DataType,
            name: &str,
            nullable: bool,
        ) -> Result<ColKey>;

        unsafe fn table_traverse_clusters(
            table: &Table,
            userdata: *mut c_char,
            traverse_fn: unsafe fn(&Cluster, *mut c_char) -> IteratorControl,
        ) -> Result<bool>;
    }

    unsafe extern "C++" {
        pub type Cluster;

        fn is_leaf(self: &Cluster) -> bool;
        fn get_sub_tree_depth(self: &Cluster) -> i32;
        fn is_writeable(self: &Cluster) -> Result<bool>;
        fn node_size(self: &Cluster) -> Result<usize>;
        fn get_tree_size(self: &Cluster) -> Result<usize>;
        fn get_last_key_value(self: &Cluster) -> Result<i64>;
        fn nb_columns(self: &Cluster) -> usize;

        /// Get the ref to the leaf column. Return type is `ref_type`.
        fn cluster_get_keys_ref(cluster: &Cluster) -> usize;
        /// Get the ref to the leaf column. Return type is `ref_type`.
        unsafe fn cluster_get_column_ref(cluster: &Cluster, column_ndx: usize) -> usize;
    }

    unsafe extern "C++" {
        pub type Spec;
        fn get_column_count(self: &Spec) -> usize;
        fn get_public_column_count(self: &Spec) -> usize;

        fn spec_get_key(spec: &Spec, column_ndx: usize) -> Result<ColKey>;
        fn spec_get_column_type(spec: &Spec, column_ndx: usize) -> ColumnType;
        fn spec_get_column_name(spec: &Spec, column_ndx: usize) -> &str;
        fn spec_get_column_index(spec: &Spec, name: &str) -> usize;
    }

    unsafe extern "C++" {
        pub type Obj;

        fn obj_get_int(obj: &Obj, col_key: ColKey) -> Result<i64>;

        /// Get string.
        ///
        /// SAFETY: This function is unsafe because the returned string may be
        /// invalidated during a write transaction if another write occurs to
        /// the same leaf column.
        unsafe fn obj_get_string(obj: &Obj, col_key: ColKey) -> Result<&str>;

        fn obj_set_int(obj: &Obj, col_key: ColKey, value: i64) -> Result<()>;
        fn obj_set_string(obj: &Obj, col_key: ColKey, value: &str) -> Result<()>;
    }

    unsafe extern "C++" {
        pub type Array<'alloc>;
        pub type NodeHeaderType;
        pub type NodeHeaderWidthType;

        /// Array constructor.
        fn array_new_unattached<'a>(alloc: &'a Allocator) -> Result<UniquePtr<Array<'a>>>;
        fn array_get_width_type(array: &Array) -> NodeHeaderWidthType;

        /// Unsafe because the ref must be valid for the allocator that the
        /// array was initialized with.
        unsafe fn init_from_ref(self: Pin<&mut Array>, ref_: usize);
        fn is_attached(self: &Array) -> bool;
        fn is_read_only(self: &Array) -> bool;
        fn size(self: &Array) -> usize;
        fn is_empty(self: &Array) -> bool;
        fn get_ref(self: &Array) -> usize; // ref_type
        fn get_header(self: &Array) -> *mut c_char;
        fn has_parent(self: &Array) -> bool;
        fn get_ndx_in_parent(self: &Array) -> usize;
        fn get_type(self: &Array) -> NodeHeaderType;
        fn get_width(self: &Array) -> usize;
        fn has_refs(self: &Array) -> bool;
        fn is_inner_bptree_node(self: &Array) -> bool;
        fn get_byte_size(self: &Array) -> usize;

        /// Unsafe because no bounds check.
        unsafe fn get(self: &Array, ndx: usize) -> i64;
    }

    // Free utility functions.
    unsafe extern "C++" {
        fn table_name_to_class_name(table_name: &str) -> &str;
    }
}

use std::ffi::c_char;

pub use ffi::*;

#[doc(no_inline)]
pub use cxx;

#[allow(non_camel_case_types)]
pub type ref_type = usize;

unsafe impl Send for DB {}
unsafe impl Sync for DB {}

impl Default for DBDurability {
    fn default() -> Self {
        Self::Full
    }
}

impl Default for VersionID {
    fn default() -> Self {
        Self {
            version: u64::MAX,
            index: 0,
        }
    }
}

impl TableKey {
    pub const INVALID: TableKey = TableKey {
        value: u32::MAX >> 1,
    };
}

impl ColKey {
    pub const INVALID: ColKey = ColKey { value: i64::MAX };

    pub fn is_nullable(&self) -> bool {
        todo!()
    }

    pub fn is_list(&self) -> bool {
        todo!()
    }

    pub fn is_set(&self) -> bool {
        todo!()
    }

    pub fn is_dictionary(&self) -> bool {
        todo!()
    }

    pub fn is_collection(&self) -> bool {
        todo!()
    }

    pub fn get_index(&self) -> usize {
        todo!()
    }

    pub fn get_type(&self) -> ColumnType {
        todo!()
    }

    pub fn get_attrs(&self) -> ColumnAttr {
        todo!()
    }

    pub fn get_tag(&self) -> u32 {
        todo!()
    }
}

impl ObjKey {
    pub const INVALID: ObjKey = ObjKey { value: -1 };

    pub const fn is_unresolved(&self) -> bool {
        self.value <= -2
    }
}

impl Transaction {
    pub fn alloc(&self) -> &Allocator {
        txn_get_alloc(self)
    }

    pub fn get_top_ref(&self) -> ref_type {
        txn_get_top_ref(self)
    }

    pub fn freeze(&self) -> Result<cxx::SharedPtr<Transaction>, cxx::Exception> {
        txn_freeze(self)
    }
}

impl Table {
    pub fn create_object(&self) -> Result<cxx::UniquePtr<Obj>, cxx::Exception> {
        table_create_object(self)
    }

    pub fn traverse_clusters<F>(&self, func: F) -> Result<bool, cxx::Exception>
    where
        F: FnMut(&Cluster) -> IteratorControl,
    {
        struct Callback<F> {
            func: F,
        }

        impl<F: FnMut(&Cluster) -> IteratorControl> Callback<F> {
            // NOTE: This function should be declared `unsafe`, but for some
            // reason `cxx` doesn't respect the unsafe-ness of the function
            // pointer in the signature of `table_traverse_clusters`.
            fn call(cluster: &Cluster, this: *mut c_char) -> IteratorControl {
                let this: *mut Self = this as _;
                let this = unsafe { &mut *this };
                (this.func)(cluster)
            }
        }

        let mut userdata = Callback { func };
        unsafe { table_traverse_clusters(self, &mut userdata as *mut _ as _, Callback::<F>::call) }
    }
}

impl Spec {
    pub fn get_key(&self, column_ndx: usize) -> Result<ColKey, cxx::Exception> {
        spec_get_key(self, column_ndx)
    }

    pub fn get_column_type(&self, column_ndx: usize) -> ColumnType {
        spec_get_column_type(self, column_ndx)
    }

    pub fn get_column_name(&self, column_ndx: usize) -> &str {
        spec_get_column_name(self, column_ndx)
    }

    pub fn get_column_index(&self, name: &str) -> usize {
        spec_get_column_index(self, name)
    }
}

impl Obj {
    pub fn get_int(&self, col_key: ColKey) -> Result<i64, cxx::Exception> {
        obj_get_int(self, col_key)
    }

    pub unsafe fn get_string(&self, col_key: ColKey) -> Result<&str, cxx::Exception> {
        obj_get_string(self, col_key)
    }

    pub fn set_int(&self, col_key: ColKey, value: i64) -> Result<(), cxx::Exception> {
        obj_set_int(self, col_key, value)
    }

    pub fn set_string(&self, col_key: ColKey, value: &str) -> Result<(), cxx::Exception> {
        obj_set_string(self, col_key, value)
    }
}

impl Cluster {
    pub fn get_keys_ref(&self) -> ref_type {
        cluster_get_keys_ref(self)
    }

    pub fn get_column_ref(&self, column_ndx: usize) -> ref_type {
        if column_ndx >= self.nb_columns() {
            panic!("column index out of bounds");
        } else {
            unsafe { self.get_column_ref_unchecked(column_ndx) }
        }
    }

    pub unsafe fn get_column_ref_unchecked(&self, column_ndx: usize) -> ref_type {
        cluster_get_column_ref(self, column_ndx)
    }
}

impl<'a> Array<'a> {
    pub fn new(alloc: &'a Allocator) -> Result<cxx::UniquePtr<Array<'a>>, cxx::Exception> {
        array_new_unattached(alloc)
    }

    pub fn width_type(&self) -> NodeHeaderWidthType {
        array_get_width_type(self)
    }

    pub fn width_per_element_in_bits(&self) -> usize {
        let width = self.get_width();
        match self.width_type() {
            NodeHeaderWidthType::wtype_Ignore => 8,
            NodeHeaderWidthType::wtype_Multiply => width * 8,
            _ => width,
        }
    }
}
