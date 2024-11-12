/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::{
    annotation::{AnnotationError, expression::ExpressionCompileError},
    executable::{ExecutableCompilationError, insert::WriteCompilationError},
};
use error::typedb_error;
use executor::pipeline::PipelineExecutionError;
use function::FunctionError;
use ir::{pipeline::FunctionRepresentationError, RepresentationError};

use crate::{define::DefineError, redefine::RedefineError, undefine::UndefineError};

typedb_error!(
    pub QueryError(component = "Query execution", prefix = "QEX") {
        // TODO: decide if we want to include whole query
        ParseError(1, "Failed to parse TypeQL query.", query: String, ( typedb_source: typeql::Error )),
        Define(2, "Failed to execute define query.", ( typedb_source: DefineError )),
        Redefine(3, "Failed to execute redefine query.", ( typedb_source: RedefineError )),
        Undefine(4, "Failed to execute undefine query.", ( typedb_source: UndefineError )),
        FunctionDefinition(5, "Error in provided function. ", ( typedb_source: FunctionRepresentationError )),
        FunctionRetrieval(6, "Failed to retrieve function. ",  ( typedb_source: FunctionError )),
        Representation(7, "Error in provided query. ", ( typedb_source: RepresentationError )),
        Annotation(8, "Error analysing query. ", ( typedb_source: AnnotationError )),
        ExecutableCompilation(9, "Error compiling query. ", ( typedb_source: ExecutableCompilationError )),
        WriteCompilation(10, "Error while compiling write query.", ( source: WriteCompilationError )),
        ExpressionCompilation(11, "Error while compiling expression.", ( source: ExpressionCompileError )),
        WritePipelineExecutionError(12, "Error while execution write pipeline.", ( typedb_source: Box<PipelineExecutionError> )),
        ReadPipelineExecutionError(13, "Error while executing read pipeline.", ( typedb_source: Box<PipelineExecutionError> )),
        QueryExecutionClosedEarly(14, "Query execution was closed before it finished, possibly due to transaction close, rollback, commit, or a server-side error (these should be visible in the server logs)."),
    }
);
