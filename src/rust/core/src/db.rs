use std::{ffi::OsStr, path::Path, pin::Pin};

use sys::VersionID;

use crate::{Error, Transaction};

use super::{cxx, sys};

/// An open Realm file.
///
/// This is internally a handle, so it may be cloned cheaply and passed to
/// multiple threads etc.
#[derive(Clone)]
pub struct DB {
    db: cxx::SharedPtr<sys::DB>,
}

impl DB {
    pub fn open(path: impl AsRef<Path>, options: &sys::DBOptions) -> Result<DB, Error> {
        use os_str_bytes::OsStrBytes;
        let path = path.as_ref();
        let path_bytes = path.to_raw_bytes();
        Ok(DB {
            db: sys::db_create_with_replication(
                // TODO: Support more history types.
                sys::make_in_realm_history()?,
                &path_bytes,
                options,
            )?,
        })
    }

    pub fn path(&self) -> std::borrow::Cow<Path> {
        use os_str_bytes::OsStrBytes;
        use std::borrow::Cow;
        match OsStr::assert_from_raw_bytes(self.db.get_path().as_bytes()) {
            Cow::Borrowed(path) => Cow::Borrowed(path.as_ref()),
            Cow::Owned(path) => Cow::Owned(path.into()),
        }
    }

    pub fn start_read(&self, version: Option<sys::VersionID>) -> Result<Transaction, Error> {
        Ok(Transaction::from_inner(sys::db_start_read(
            &self.db,
            version.unwrap_or_default(),
        )?))
    }

    pub fn start_frozen(&self, version: Option<VersionID>) -> Result<Transaction, Error> {
        Ok(Transaction::from_inner(sys::db_start_frozen(
            &self.db,
            version.unwrap_or_default(),
        )?))
    }

    pub fn delete_files(self) -> Result<bool, Error> {
        let path = self.db.get_path().as_bytes();
        let mut did_delete = false;
        sys::db_delete_files(path, Pin::new(&mut did_delete))?;
        Ok(did_delete)
    }

    pub unsafe fn delete_files_and_lockfile(self, delete_lockfile: bool) -> Result<bool, Error> {
        let path = self.db.get_path().as_bytes();
        let mut did_delete = false;
        sys::db_delete_files_and_lockfile(path, Pin::new(&mut did_delete), delete_lockfile)?;
        Ok(did_delete)
    }
}
