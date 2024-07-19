use std::{error::Error, fmt};

use encoding::value::label::Label;

use crate::error::ConceptReadError;

pub mod operation_time_validation;

#[derive(Debug, Clone)]
pub enum DataValidationError {
    ConceptRead(ConceptReadError),
    CannotCreateInstanceOfAbstractType(Label<'static>),
}

impl fmt::Display for DataValidationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for DataValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptRead(source) => Some(source),
            Self::CannotCreateInstanceOfAbstractType(_) => None,
        }
    }
}
