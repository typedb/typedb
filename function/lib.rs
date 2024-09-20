/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;

use compiler::match_::inference::FunctionTypeInferenceError;
use encoding::error::EncodingError;
use error::typedb_error;
use ir::program::{FunctionReadError, FunctionRepresentationError};

pub mod function;
pub mod function_cache;
pub mod function_manager;

typedb_error!(
    pub FunctionError(component = "Function", prefix = "FUN") {
        FunctionNotFound(1, "Function was not found"),
        AllFunctionsTypeCheckFailure(2, "Type checking all functions currently defined failed.", ( typedb_source: FunctionTypeInferenceError )),
        CommittedFunctionsTypeCheck(3, "Type checking stored functions failed.", ( typedb_source: FunctionTypeInferenceError )),
        FunctionTranslation(4, "Failed to translate TypeQL function into internal representation", ( typedb_source: FunctionRepresentationError )),
        FunctionAlreadyExists(5, "A function with name '{name}' already exists", name: String),
        CreateFunctionEncoding(6, "Encoding error while trying to create function.", ( source: EncodingError )),
        FunctionRetrieval(7, "Error retrieving function.", (  source: FunctionReadError )),
        CommittedFunctionParseError(8, "Error while parsing committed.", ( typedb_source: typeql::Error )),
    }
);
