use std::ops::Deref;

use sys::VersionID;

use crate::{ColumnType, Error};

use super::{cxx, sys, TableRef, DB};

/// Continuous transaction.
///
/// The transaction starts out in the "reading" state. It can be transitioned
/// into the "writing" state with `promote_to_write()`, which returns a
/// [`Writer`] that must be either committed or rolled back.
///
/// Note that `Transaction` is `Send`, but not `Sync`. If multiple threads want
/// to use the Realm file simultaneously, they must each maintain their own
/// `Transaction`. However, it is perfectly fine to send transactions between
/// threads and let them use it in turn, or it may be wrapped in a mutex.
pub struct Transaction {
    txn: cxx::SharedPtr<sys::Transaction>,
}

unsafe impl Send for Transaction {}

impl Transaction {
    pub fn new(db: &DB) -> Result<Self, Error> {
        db.start_read(None)
    }

    pub fn from_inner(txn: cxx::SharedPtr<sys::Transaction>) -> Self {
        Transaction { txn }
    }

    pub fn into_inner(self) -> cxx::SharedPtr<sys::Transaction> {
        self.txn
    }

    pub fn as_inner(&self) -> &cxx::SharedPtr<sys::Transaction> {
        &self.txn
    }

    pub fn stage(&self) -> sys::TransactStage {
        self.txn.get_transact_stage()
    }

    pub fn promote_to_write(&self) -> Result<Writer, Error> {
        let success = sys::txn_promote_to_write(&self.txn, false)?;
        if !success {
            panic!("non-blocking promote_to_write returned false");
        }
        Ok(Writer { txn: self })
    }

    pub fn advance_read(&self, target_version: Option<VersionID>) -> Result<(), Error> {
        Ok(sys::txn_advance_read(
            &self.txn,
            target_version.unwrap_or_default(),
        )?)
    }

    pub fn is_frozen(&self) -> bool {
        self.txn.is_frozen()
    }

    pub fn get_table(&self, key: sys::TableKey) -> Result<TableRef, Error> {
        let table = sys::txn_get_table(&self.txn, key)?;
        Ok(unsafe { TableRef::from_sys(table).expect("invalid table key") })
    }

    pub fn get_table_by_name(&self, name: &str) -> Result<Option<TableRef>, Error> {
        let table = sys::txn_get_table_by_name(&self.txn, name)?;
        Ok(unsafe { TableRef::from_sys(table) })
    }

    pub fn freeze(&self) -> Result<FrozenTransaction, Error> {
        Ok(FrozenTransaction {
            txn: Transaction {
                txn: self.txn.freeze()?,
            },
        })
    }
}

/// Continuous write transaction.
///
/// `Writer` represents that a `Transaction` is currently in the "writing"
/// state. No writes to the Realm file can be made without going through
/// `Writer`.
///
/// When finished writing, call either `commit()` or `rollback()` to put the
/// transaction back in the "reading" state (corresponding to
/// `commit_and_continue_as_read()` and `rollback_and_continue_as_read()`,
/// respectively).
///
/// If the writer is dropped without having `commit()` or `rollback()` called,
/// the `Drop` implementation panicks, unless the current thread is already
/// panicking. If the writer is dropped while panicking, it silently tries to
/// roll back the write.
///
/// Note: If rollback during panic fails, this may leave the `Transaction`
/// object in an unrecoverable state. This should only happen in extreme
/// scenarios (like system heap allocation failure), so not many other things
/// will be working anyway.
pub struct Writer<'txn> {
    txn: &'txn Transaction,
}

impl<'txn> Writer<'txn> {
    pub fn commit_and_continue(&mut self) -> Result<(), Error> {
        sys::txn_commit_and_continue_writing(&self.txn.txn)?;
        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        Ok(sys::txn_commit_and_continue_as_read(&self.txn.txn)?)
    }

    pub fn rollback(self) -> Result<(), Error> {
        Ok(sys::txn_rollback_and_continue_as_read(&self.txn.txn)?)
    }

    pub fn add_table(
        &mut self,
        name: &str,
        table_type: sys::TableType,
    ) -> Result<TableRef<'txn>, Error> {
        let table = sys::txn_add_table(&self.txn.txn, name, table_type)?;
        Ok(unsafe { TableRef::from_sys(table) }.expect("add_table returned NULL"))
    }

    pub fn add_table_with_primary_key(
        &mut self,
        name: &str,
        pk_type: ColumnType,
        pk_name: &str,
    ) -> Result<TableRef<'txn>, Error> {
        let (nullable, ty) = match pk_type {
            ColumnType::NonNullable(ty) => (false, ty),
            ColumnType::Nullable(ty) => (true, ty),
        };
        let table = sys::txn_add_table_with_primary_key(
            &self.txn.txn,
            name,
            ty,
            pk_name,
            nullable,
            sys::TableType::TopLevel,
        )?;
        Ok(unsafe { TableRef::from_sys(table) }.expect("add_table_with_primary_key returned NULL"))
    }
}

impl<'txn> Deref for Writer<'txn> {
    // Returning a double reference here to get the correct deduced lifetime in
    // return types of methods on `Transaction`. If deref just returned plain
    // `Transaction`, the returned `TableRef`s would be bound to the lifetime of
    // the `Writer` rather than the `Transaction`.
    type Target = &'txn Transaction;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl<'txn> Drop for Writer<'txn> {
    fn drop(&mut self) {
        let stage = self.txn.txn.get_transact_stage();

        if stage == sys::TransactStage::transact_Writing {
            if std::thread::panicking() {
                let _ = sys::txn_rollback_and_continue_as_read(&self.txn.txn);
                // TODO: Record if rollback failed during panic.
            } else {
                panic!("Missing call to `Writer::commit()` or `Writer::rollback()`");
            }
        } else if stage == sys::TransactStage::transact_Reading {
            // Do nothing.
        } else if stage == sys::TransactStage::transact_Ready {
            unreachable!("transaction writer did not return the transaction to the reading state");
        } else if stage == sys::TransactStage::transact_Frozen {
            unreachable!("transaction writer existed for frozen transaction");
        } else {
            panic!("invalid transaction stage");
        }
    }
}

pub struct FrozenTransaction {
    txn: Transaction,
}

unsafe impl Send for FrozenTransaction {}
unsafe impl Sync for FrozenTransaction {}

impl Deref for FrozenTransaction {
    type Target = Transaction;

    fn deref(&self) -> &Self::Target {
        debug_assert_eq!(self.txn.stage(), sys::TransactStage::transact_Frozen);
        &self.txn
    }
}
