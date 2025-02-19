/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::{
    annotation::{expression::ExpressionCompileError, AnnotationError},
    executable::{ExecutableCompilationError, WriteCompilationError},
    transformation::StaticOptimiserError,
};
use error::typedb_error;
use executor::pipeline::{pipeline::PipelineError, PipelineExecutionError};
use function::FunctionError;
use ir::RepresentationError;

use crate::{define::DefineError, redefine::RedefineError, undefine::UndefineError};

typedb_error! {
    pub QueryError(component = "Query execution", prefix = "QEX") {
        ParseError(1, "Failed to parse TypeQL query.", source_query: String, typedb_source: typeql::Error),
        Define(2, "Failed to execute define query.",   source_query: String, typedb_source: DefineError),
        Redefine(3, "Failed to execute redefine query.",  source_query: String, typedb_source: RedefineError),
        Undefine(4, "Failed to execute undefine query.",  source_query: String, typedb_source: UndefineError),
        FunctionDefinition(5, "Error defining function.",  source_query: String, typedb_source: FunctionError),
        Representation(7, "Error in provided query.",  source_query: String, typedb_source: Box<RepresentationError>),
        Annotation(8, "Error analysing query.",  source_query: String, typedb_source: AnnotationError),
        Transformation(9, "Error applying query transformation.",  source_query: String, typedb_source: StaticOptimiserError),
        ExecutableCompilation(10, "Error compiling query.",  source_query: String, typedb_source: ExecutableCompilationError),
        WriteCompilation(11, "Error while compiling write query.",  source_query: String, typedb_source: WriteCompilationError),
        ExpressionCompilation(12, "Error while compiling expression.",  source_query: String, typedb_source: ExpressionCompileError),
        Pipeline(13, "Pipeline error.",  source_query: String, typedb_source: Box<PipelineError>),
        WritePipelineExecution(14, "Error while execution write pipeline.", source_query: String, typedb_source: Box<PipelineExecutionError>),
        ReadPipelineExecution(15, "Error while executing read pipeline.",  source_query: String, typedb_source: Box<PipelineExecutionError>),
        QueryExecutionClosedEarly(16, "Query execution was closed before it finished, possibly due to transaction close, rollback, commit, or a server-side error (these should be visible in the server logs)."),
    }
}
