/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use concept::thing::statistics::Statistics;
use concept::type_::type_manager::TypeManager;
use ir::program::program::Program;
use storage::snapshot::ReadableSnapshot;
use crate::CompileError;
use crate::expression::block_compiler::compile_expressions;
use crate::inference::annotated_functions::{IndexedAnnotatedFunctions};
use crate::inference::annotated_program::AnnotatedProgram;
use crate::inference::type_inference::infer_types;
use crate::planner::program_plan::ProgramPlan;

pub fn compile(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_functions: Arc<IndexedAnnotatedFunctions>, // TODO: these should include fully compiled plans already
    statistics: &Statistics,
    program: Program, // TODO: i think we can  delete this abstraction and pass in the entry block and premable functions only
) -> Result<ProgramPlan, CompileError> {
    let (entry_block, premable_functions) = program.into_parts();
    // note: we could precompile the preamble functions first ahead of time, to be consistent with how schema functions are handled
    //       instead, we currently recompile the functions in every clause they are used in
    let (entry_annotations, annotated_preamble_functions) = infer_types(
        &entry_block, premable_functions, snapshot, &type_manager, schema_functions.as_ref(),
    ).map_err(|err| CompileError::ProgramTypeInference { source: err })?;
    let entry_expressions = compile_expressions(snapshot, type_manager, &entry_block, &entry_annotations)
        .map_err(|err| CompileError::ExpressionCompile { source: err })?;

    // TODO: at this point, we should finish setting all the variable categories in the entry BlockContext!
    //       notably, we have determined all expression output types
    //       we can also fully determine the variable

    let annotated_program = AnnotatedProgram::new(
        entry_block,
        entry_annotations,
        entry_expressions,
        annotated_preamble_functions,
        schema_functions,
    );

    // TODO: this is where we would introduce some static optimisation passes, such as replacing 2x roleplayer IR with Index IR

    debug_assert!(!annotated_program.contains_functions(), "Function planning and execution is not implemented.");
    let program_plan = ProgramPlan::from_program(annotated_program, &statistics);
    Ok(program_plan)
}
