/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;

use answer::variable::Variable;
use compiler::{
    delete::program::DeleteProgram,
    insert::program::InsertProgram,
    match_::{inference::annotated_functions::AnnotatedUnindexedFunctions, planner::pattern_plan::MatchProgram},
    VariablePosition,
};
use concept::thing::statistics::Statistics;
use ir::program::{block::VariableRegistry, function::Function};

use crate::{annotation::AnnotatedStage, error::QueryError};

pub struct CompiledPipeline {
    pub(super) compiled_functions: Vec<CompiledFunction>,
    pub(super) compiled_stages: Vec<CompiledStage>,
}

pub struct CompiledFunction {
    plan: MatchProgram,
    returns: HashMap<Variable, VariablePosition>,
}

pub enum CompiledStage {
    Match(MatchProgram),
    Insert(InsertProgram),
    Delete(DeleteProgram),
}

impl CompiledStage {
    fn output_row_mapping(&self) -> HashMap<Variable, VariablePosition> {
        match self {
            CompiledStage::Match(program) => program.variable_positions().to_owned(),
            CompiledStage::Insert(program) => program
                .output_row_schema
                .iter()
                .enumerate()
                .map(|(i, (v, _))| (*v, VariablePosition::new(i as u32)))
                .collect(),
            CompiledStage::Delete(program) => program
                .output_row_schema
                .iter()
                .enumerate()
                .map(|(i, (v, _))| (*v, VariablePosition::new(i as u32)))
                .collect(),
        }
    }
}

pub(super) fn compile_pipeline(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    annotated_functions: AnnotatedUnindexedFunctions,
    annotated_stages: Vec<AnnotatedStage>,
) -> Result<CompiledPipeline, QueryError> {
    let compiled_functions = annotated_functions
        .iter_functions()
        .map(|function| compile_function(statistics, variable_registry, function))
        .collect::<Result<Vec<_>, _>>()?;

    let mut compiled_stages = Vec::with_capacity(annotated_stages.len());
    for stage in annotated_stages {
        let input_variable_positions =
            compiled_stages.last().map(|stage: &CompiledStage| stage.output_row_mapping()).unwrap_or(HashMap::new());

        let compiled_stage = compile_stage(statistics, variable_registry, &input_variable_positions, stage)?;
        compiled_stages.push(compiled_stage);
    }
    Ok(CompiledPipeline { compiled_functions, compiled_stages })
}

fn compile_function(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    function: &Function,
) -> Result<CompiledFunction, QueryError> {
    todo!()
}

fn compile_stage(
    statistics: &Statistics,
    variable_registry: &VariableRegistry,
    input_variables: &HashMap<Variable, VariablePosition>,
    annotated_stage: AnnotatedStage,
) -> Result<CompiledStage, QueryError> {
    match &annotated_stage {
        AnnotatedStage::Match { block, block_annotations, compiled_expressions, variable_value_types } => {
            let plan =
                MatchProgram::from_block(block, block_annotations, variable_registry, compiled_expressions, statistics);
            Ok(CompiledStage::Match(plan))
        }
        AnnotatedStage::Insert { block, annotations } => {
            let plan =
                compiler::insert::program::compile(block.conjunction().constraints(), input_variables, annotations)
                    .map_err(|source| QueryError::WriteCompilation { source })?;
            Ok(CompiledStage::Insert(plan))
        }
        AnnotatedStage::Delete { block, deleted_variables, annotations } => {
            let plan = compiler::delete::program::compile(
                input_variables,
                annotations,
                block.conjunction().constraints(),
                deleted_variables,
            )
            .map_err(|source| QueryError::WriteCompilation { source })?;
            Ok(CompiledStage::Delete(plan))
        }
        _ => todo!(),
        // AnnotatedStage::Filter(_) => {}
        // AnnotatedStage::Sort(_) => {}
        // AnnotatedStage::Offset(_) => {}
        // AnnotatedStage::Limit(_) => {}
    }
}
