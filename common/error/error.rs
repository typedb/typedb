/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt::{Debug, Display};

pub trait ErrorSource {
    fn source(&self) -> Option<&dyn Error>;
}


pub trait TypeDBError: ErrorSource + Debug + Display{
    fn code(&self) -> &str;

    fn source_typedb_error(&self) -> Option<&dyn TypeDBError>;

    fn root_source_typedb_error(&self) -> &dyn TypeDBError where Self: Sized {
        let mut error: &dyn TypeDBError = self;
        while let Some(source) = error.source_typedb_error() {
            error = source;
        }
        error
    }
}
