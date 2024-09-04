/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use compiler::{
    expression::ExpressionCompileError, insert::WriteCompilationError, match_::inference::TypeInferenceError,
};
use error::typedb_error;
use executor::pipeline::PipelineExecutionError;
use function::FunctionError;
use ir::{program::FunctionDefinitionError, PatternDefinitionError};

use crate::{define::DefineError, redefine::RedefineError, undefine::UndefineError};

typedb_error!(
    pub QueryError(domain = "Query", prefix = "QRY") {
        // TODO: decide if we want to include whole query
        ParseError(1, "Failed to parse TypeQL query.", query: String, ( typedb_source: typeql::Error )),
        // TODO: move these to typedb_source once all errors are implemented
        Define(2, "Failed to execute define query.", ( source: DefineError )),
        Redefine(3, "Failed to execute redefine query.", ( source: RedefineError )),
        Undefine(4, "Failed to execute undefine query.", ( source: UndefineError )),
        FunctionDefinition(5, "Error in provided function. ", ( source: FunctionDefinitionError )),
        FunctionRetrieval(6, "Failed to retrieve function. ",  ( source: FunctionError )),
        PatternDefinition(7, "Error in provided pattern. ", ( source: PatternDefinitionError )),
        TypeInference(8, "Error during type inference. ", ( source: TypeInferenceError )),
        WriteCompilation(9, "Error while compiling write query.", ( source: WriteCompilationError )),
        ExpressionCompilation(10, "Error while compiling expression.", ( source: ExpressionCompileError )),
        WritePipelineExecutionError(11, "Error while execution write pipeline.", ( typedb_source: PipelineExecutionError )),
        ReadPipelineExecutionError(12, "Error while executing read pipeline.", ( typedb_source: PipelineExecutionError )),
        QueryExecutionClosedEarly(13, "Query execution was closed before it finished, possibly due to transaction close, rollback, or commit."),
    }
);
