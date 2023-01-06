use std::marker::PhantomData;

use super::{cxx, sys};

pub struct Obj<'txn> {
    obj: cxx::UniquePtr<sys::Obj>,
    _marker: PhantomData<&'txn ()>,
}
