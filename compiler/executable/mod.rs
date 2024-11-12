/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use error::typedb_error;

use crate::executable::{fetch::executable::FetchCompilationError, insert::WriteCompilationError};

pub mod delete;
pub mod fetch;
pub mod function;
pub mod insert;
pub mod match_;
pub mod modifiers;
pub mod pipeline;
pub mod reduce;

typedb_error!(
    pub ExecutableCompilationError(component = "Query executable", prefix = "QEE") {
        InsertExecutableCompilation(1, "Error compiling insert clause into executable.", (source : Box<WriteCompilationError>)),
        DeleteExecutableCompilation(2, "Error compiling delete clause into executable.", (source : Box<WriteCompilationError>)),
        FetchCompliation(3, "Error compiling fetch clause into executable.", (typedb_source : FetchCompilationError)),
    }
);
