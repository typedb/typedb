/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use error::typedb_error;
use crate::executable::insert::WriteCompilationError;

pub mod delete;
pub mod fetch;
pub mod insert;
pub mod match_;
pub mod modifiers;
pub mod reduce;
pub mod pipeline;
pub mod function;


typedb_error!(
    pub ExecutableError(component = "Query executable", prefix = "QEE") {
        InsertExecutableCompilation(1, "Error compiling insert stage into executable.", (source : WriteCompilationError)),
        DeleteExecutableCompilation(2, "Error compiling delete stage into executable.", (source : WriteCompilationError)),
    }
);
