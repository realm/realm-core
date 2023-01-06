pub use realm_sys as sys;
pub use sys::cxx;

pub use sys::{TableKey, VersionID};

mod alloc;
mod db;
mod error;
mod obj;
mod table;
mod transaction;

pub use alloc::*;
pub use db::*;
pub use error::*;
pub use obj::*;
pub use table::*;
pub use transaction::*;

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;

    #[test]
    fn alloc_sanity() {
        let alloc = DefaultAllocator::new();
        let m = alloc.alloc(1024).unwrap();
        println!("Hello, World! {:x}", m.m_ref);
    }

    struct DBDeleteGuard(Option<DB>);

    impl DBDeleteGuard {
        pub fn new(db: DB) -> Self {
            Self(Some(db))
        }
    }

    impl Deref for DBDeleteGuard {
        type Target = DB;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl Drop for DBDeleteGuard {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().delete_files();
        }
    }

    #[test]
    fn db_create() {
        let db = DBDeleteGuard::new(DB::open("test.realm", &Default::default()).unwrap());
        let txn = Transaction::new(&db).unwrap();

        let mut writer = txn.promote_to_write().unwrap();
        let table = writer
            .add_table("class_TestTable", sys::TableType::TopLevel)
            .unwrap();
        let table2 = writer
            .get_table_by_name("class_TestTable")
            .unwrap()
            .unwrap();
        writer.commit().unwrap();
        assert_eq!(table, table2);
        assert_eq!(table.name(), "class_TestTable");
        assert_eq!(table.key(), table2.key());
        assert_eq!(table.size(), 0);

        let nonexisting = txn.get_table_by_name("class_NonExisting").unwrap();
        assert_eq!(nonexisting, None);
    }

    #[test]
    fn traverse_clusters() {
        let db = DBDeleteGuard::new(DB::open("test2.realm", &Default::default()).unwrap());
        let txn = Transaction::new(&db).unwrap();

        let mut writer = txn.promote_to_write().unwrap();
        let table = writer
            .add_table("class_TestTable", sys::TableType::TopLevel)
            .unwrap();

        let int_col = table
            .add_column(&mut writer, DataType::Int, "int", false)
            .unwrap();
        let str_col = table
            .add_column(&mut writer, DataType::String, "string", true)
            .unwrap();
        assert_ne!(int_col, sys::ColKey::INVALID);
        assert_ne!(str_col, sys::ColKey::INVALID);
        assert_ne!(int_col, str_col);

        for i in 0..100_000 {
            let obj = table.create_object().unwrap();
            obj.set_int(int_col, i).unwrap();
            obj.set_string(str_col, &format!("{i}")).unwrap();
        }

        writer.commit().unwrap();

        // Check the spec
        {
            let spec = table.spec();
            assert_eq!(spec.get_column_count(), 2);
            assert_eq!(spec.get_column_name(0), "int");
            assert_eq!(spec.get_column_name(1), "string");
            assert_eq!(spec.get_column_type(0), sys::ColumnType::Int);
            assert_eq!(spec.get_column_type(1), sys::ColumnType::String);
        }

        assert_eq!(table.size(), 100_000);

        let mut num_clusters = 0;
        let mut num_leaves = 0;
        let mut num_inner = 0;
        let mut int_refs = vec![];
        let mut str_refs = vec![];
        let mut node_size_sum = 0;
        table
            .traverse_clusters(|cluster| {
                num_clusters += 1;
                if cluster.is_leaf() {
                    num_leaves += 1;
                } else {
                    num_inner += 1;
                }

                int_refs.push(cluster.get_column_ref(0));
                str_refs.push(cluster.get_column_ref(1));
                node_size_sum += cluster.node_size().unwrap();

                sys::IteratorControl::AdvanceToNext
            })
            .unwrap();
        assert_eq!(node_size_sum, 100_000);
        assert_eq!(num_inner, 0);
        assert_eq!(num_leaves, num_clusters);
        // just a sanity check, the real value depends on the branching factor.
        assert!(num_clusters > 200);

        assert_eq!(int_refs.len(), num_clusters);
        assert_eq!(str_refs.len(), num_clusters);

        assert!(int_refs.iter().all(|r| *r != 0));
        assert!(str_refs.iter().all(|r| *r != 0));
    }
}
