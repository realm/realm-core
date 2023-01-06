use std::fmt::{Debug, Display};

use super::cxx;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("C++ exception: {0}")]
    Exception(#[from] CxxException),
}

pub struct CxxException(pub cxx::Exception);

impl Display for CxxException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for CxxException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for CxxException {}

impl From<cxx::Exception> for Error {
    fn from(value: cxx::Exception) -> Self {
        Error::Exception(CxxException(value))
    }
}
