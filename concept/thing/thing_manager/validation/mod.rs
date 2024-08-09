/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use encoding::value::label::Label;

use crate::error::ConceptReadError;

pub mod operation_time_validation;

#[derive(Debug, Clone)]
pub enum DataValidationError {
    ConceptRead(ConceptReadError),
    CannotCreateInstanceOfAbstractType(Label<'static>),
    CannotAddOwnerInstanceForNotOwnedAttributeType(Label<'static>, Label<'static>),
    CannotAddPlayerInstanceForNotPlayedRoleType(Label<'static>, Label<'static>),
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
            Self::CannotAddOwnerInstanceForNotOwnedAttributeType(_, _) => None,
            Self::CannotAddPlayerInstanceForNotPlayedRoleType(_, _) => None,
        }
    }
}
