use crate::{Error, Writer};

use super::sys;
use std::{fmt::Debug, marker::PhantomData, ops::Deref, ptr::NonNull};

pub use sys::DataType;
use sys::{ColKey, Spec, TableKey};

pub enum ColumnType {
    NonNullable(sys::DataType),
    Nullable(sys::DataType),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableRef<'txn> {
    // The table pointer is owned by the transaction.
    //
    // Note: This cannot be a normal reference, because that implies that the
    // `Table` is never modified by anyone else. But that's exactly what Realm
    // Core does when tables are deleted (and this `TableRef` becomes
    // "dangling"). In other words, it is only ever safe to dereference the
    // `TableRef` when `instance_version` has been checked.
    //
    // Note also: `Table` pointers never become truly dangling (as in, pointing
    // to an invalid memory location), which is what makes it safe to call
    // `get_instance_version()` to determine if the table is what we think it
    // is.
    //
    // TODO: Since `Table` is opaque, it is unclear whether it is UB in Rust to
    // keep the borrow when C++ destroys and reinitializes a `Table` slot in
    // `Group`. Doing it this way is agnostic to the opaqueness of `Table`, and
    // it may be possible to simplify it.
    table: NonNull<sys::Table>,

    // The pointer is guaranteed to be valid as long as its instance version
    // matches this number.
    instance_version: u64,

    _marker: PhantomData<&'txn ()>,
}

impl<'txn> TableRef<'txn> {
    /// Panic if the table reference has become dangling or invalid.
    ///
    /// This is implicitly called by `Deref`.
    ///
    /// (Corresponds to `ConstTableRef::check()` in Realm Core.)
    pub fn panic_if_dangling(&self) {
        let table = unsafe { self.table.as_ref() };
        let instance_version = table.get_instance_version();
        if instance_version != self.instance_version {
            let state = table.get_state();
            let state = unsafe { std::ffi::CStr::from_ptr(state) };
            panic!("Invalid table ref ({:?})", state);
        }
    }

    /// Create a `TableRef` from a raw FFI-enabled struct.
    ///
    /// Returns `None` if the table ptr is null.
    ///
    /// This is safe to call if:
    ///
    /// 1. The table pointer is a valid pointer to a `sys::Table`
    ///    (`realm::Table`) object.
    /// 2. The `Table` is owned by the transaction with lifetime 'txn.
    pub(crate) unsafe fn from_sys(table_ref: sys::TableRef) -> Option<Self> {
        let ptr = NonNull::new(table_ref.ptr as *mut _)?;
        Some(TableRef {
            table: ptr,
            instance_version: table_ref.instance_version,
            _marker: PhantomData,
        })
    }

    pub(crate) fn to_sys(&self) -> sys::TableRef {
        sys::TableRef {
            ptr: self.table.as_ptr() as _,
            instance_version: self.instance_version,
        }
    }

    pub fn key(&self) -> TableKey {
        sys::table_get_key(&*self)
    }

    pub fn name(&self) -> &str {
        sys::table_get_name(&*self)
    }

    pub fn size(&self) -> usize {
        sys::Table::size(&*self)
    }

    pub fn spec(&self) -> &Spec {
        sys::table_get_spec(&*self)
    }

    pub fn add_column(
        &self,
        writer: &mut Writer<'txn>,
        ty: DataType,
        name: &str,
        nullable: bool,
    ) -> Result<ColKey, Error> {
        let _ = writer;
        Ok(sys::table_add_column(&*self, ty, name, nullable)?)
    }
}

impl<'txn> Deref for TableRef<'txn> {
    type Target = sys::Table;

    fn deref(&self) -> &Self::Target {
        self.panic_if_dangling();
        unsafe { self.table.as_ref() }
    }
}

impl<'txn> Debug for TableRef<'txn> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableRef({})", self.name())
    }
}
