/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use compiler::{
    executable::{fetch::executable::ExecutableFetch, pipeline::ExecutableStage},
    VariablePosition,
};
use concept::thing::thing_manager::ThingManager;
use ir::pipeline::{ParameterRegistry, VariableRegistry};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    pipeline::{
        delete::DeleteStageExecutor,
        fetch::FetchStageExecutor,
        initial::InitialStage,
        insert::InsertStageExecutor,
        match_::MatchStageExecutor,
        modifiers::{
            LimitStageExecutor, OffsetStageExecutor, RequireStageExecutor, SelectStageExecutor, SortStageExecutor,
        },
        reduce::ReduceStageExecutor,
        stage::{ExecutionContext, ReadPipelineStage, StageAPI, WritePipelineStage},
        PipelineExecutionError,
    },
    ExecutionInterrupt,
};

pub enum Pipeline<Snapshot: ReadableSnapshot, Nonterminals: StageAPI<Snapshot>> {
    Unfetched(Nonterminals, HashMap<String, VariablePosition>),
    Fetched(Nonterminals, FetchStageExecutor<Snapshot>),
}

impl<Snapshot: ReadableSnapshot + 'static, Nonterminals: StageAPI<Snapshot>> Pipeline<Snapshot, Nonterminals> {
    fn build_with_fetch(
        variable_registry: &VariableRegistry,
        last_stage: Nonterminals,
        last_stage_output_positions: HashMap<Variable, VariablePosition>,
        executable_fetch: Option<ExecutableFetch>,
    ) -> Self {
        let named_outputs = last_stage_output_positions
            .iter()
            .filter_map(|(variable, &position)| {
                variable_registry.variable_names().get(variable).map(|name| (name.clone(), position))
            })
            .collect::<HashMap<_, _>>();

        match executable_fetch {
            None => Pipeline::Unfetched(last_stage, named_outputs),
            Some(executable) => {
                let fetch = FetchStageExecutor::new(executable);
                Pipeline::Fetched(last_stage, fetch)
            }
        }
    }

    pub fn rows_positions(&self) -> Option<&HashMap<String, VariablePosition>> {
        match self {
            Self::Unfetched(_, positions) => Some(positions),
            Self::Fetched(_, _) => None,
        }
    }

    pub fn into_rows_iterator(
        self,
        execution_interrupt: ExecutionInterrupt,
    ) -> Result<
        (Nonterminals::OutputIterator, ExecutionContext<Snapshot>),
        (PipelineExecutionError, ExecutionContext<Snapshot>),
    > {
        match self {
            Self::Unfetched(nonterminals, _) => nonterminals.into_iterator(execution_interrupt),
            Self::Fetched(nonterminals, _) => {
                let (_, context) = nonterminals.into_iterator(execution_interrupt)?;
                Err((PipelineExecutionError::FetchUsedAsRows {}, context))
            }
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> Pipeline<Snapshot, ReadPipelineStage<Snapshot>> {
    pub fn build_read_pipeline(
        snapshot: Arc<Snapshot>,
        variable_registry: &VariableRegistry,
        thing_manager: Arc<ThingManager>,
        executable_stages: Vec<ExecutableStage>,
        executable_fetch: Option<ExecutableFetch>,
        parameters: ParameterRegistry,
    ) -> Self {
        let output_variable_positions = executable_stages.last().unwrap().output_row_mapping();
        let context = ExecutionContext::new(snapshot, thing_manager, Arc::new(parameters));
        let mut last_stage = ReadPipelineStage::Initial(InitialStage::new(context));
        for executable_stage in executable_stages {
            match executable_stage {
                ExecutableStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let match_stage = MatchStageExecutor::new(match_executable, last_stage);
                    last_stage = ReadPipelineStage::Match(Box::new(match_stage));
                }
                ExecutableStage::Insert(_) => {
                    unreachable!("Insert clause cannot exist in a read pipeline.")
                }
                ExecutableStage::Delete(_) => {
                    unreachable!("Delete clause cannot exist in a read pipeline.")
                }
                ExecutableStage::Select(select_executable) => {
                    let select_stage = SelectStageExecutor::new(select_executable, last_stage);
                    last_stage = ReadPipelineStage::Select(Box::new(select_stage));
                }
                ExecutableStage::Sort(sort_executable) => {
                    let sort_stage = SortStageExecutor::new(sort_executable, last_stage);
                    last_stage = ReadPipelineStage::Sort(Box::new(sort_stage));
                }
                ExecutableStage::Offset(offset_executable) => {
                    let offset_stage = OffsetStageExecutor::new(offset_executable, last_stage);
                    last_stage = ReadPipelineStage::Offset(Box::new(offset_stage));
                }
                ExecutableStage::Limit(limit_executable) => {
                    let limit_stage = LimitStageExecutor::new(limit_executable, last_stage);
                    last_stage = ReadPipelineStage::Limit(Box::new(limit_stage));
                }
                ExecutableStage::Require(require_executable) => {
                    let require_stage = RequireStageExecutor::new(require_executable, last_stage);
                    last_stage = ReadPipelineStage::Require(Box::new(require_stage));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable, last_stage);
                    last_stage = ReadPipelineStage::Reduce(Box::new(reduce_stage));
                }
            }
        }
        Pipeline::build_with_fetch(variable_registry, last_stage, output_variable_positions, executable_fetch)
    }
}

impl<Snapshot: WritableSnapshot + 'static> Pipeline<Snapshot, WritePipelineStage<Snapshot>> {
    pub fn build_write_pipeline(
        snapshot: Snapshot,
        variable_registry: &VariableRegistry,
        thing_manager: Arc<ThingManager>,
        executable_stages: Vec<ExecutableStage>,
        executable_fetch: Option<ExecutableFetch>,
        parameters: ParameterRegistry,
    ) -> Self {
        let output_variable_positions = executable_stages.last().unwrap().output_row_mapping();
        let context = ExecutionContext::new(Arc::new(snapshot), thing_manager, Arc::new(parameters));
        let mut last_stage = WritePipelineStage::Initial(InitialStage::new(context));
        for executable_stage in executable_stages {
            match executable_stage {
                ExecutableStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let match_stage = MatchStageExecutor::new(match_executable, last_stage);
                    last_stage = WritePipelineStage::Match(Box::new(match_stage));
                }
                ExecutableStage::Insert(insert_executable) => {
                    let insert_stage = InsertStageExecutor::new(insert_executable, last_stage);
                    last_stage = WritePipelineStage::Insert(Box::new(insert_stage));
                }
                ExecutableStage::Delete(delete_executable) => {
                    let delete_stage = DeleteStageExecutor::new(delete_executable, last_stage);
                    last_stage = WritePipelineStage::Delete(Box::new(delete_stage));
                }
                ExecutableStage::Select(select_executable) => {
                    let select_stage = SelectStageExecutor::new(select_executable, last_stage);
                    last_stage = WritePipelineStage::Select(Box::new(select_stage));
                }
                ExecutableStage::Sort(sort_executable) => {
                    let sort_stage = SortStageExecutor::new(sort_executable, last_stage);
                    last_stage = WritePipelineStage::Sort(Box::new(sort_stage));
                }
                ExecutableStage::Offset(offset_executable) => {
                    let offset_stage = OffsetStageExecutor::new(offset_executable, last_stage);
                    last_stage = WritePipelineStage::Offset(Box::new(offset_stage));
                }
                ExecutableStage::Limit(limit_executable) => {
                    let limit_stage = LimitStageExecutor::new(limit_executable, last_stage);
                    last_stage = WritePipelineStage::Limit(Box::new(limit_stage));
                }
                ExecutableStage::Require(require_executable) => {
                    let require_stage = RequireStageExecutor::new(require_executable, last_stage);
                    last_stage = WritePipelineStage::Require(Box::new(require_stage));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable, last_stage);
                    last_stage = WritePipelineStage::Reduce(Box::new(reduce_stage));
                }
            }
        }
        Pipeline::build_with_fetch(variable_registry, last_stage, output_variable_positions, executable_fetch)
    }
}

// fn new_unfetched(nonterminals: Nonterminals, variable_positions: HashMap<String, VariablePosition>) -> Self {
//     Self::Unfetched(nonterminals, variable_positions)
// }
//
// fn new_fetched(nonterminals: Nonterminals, terminal: FetchStageExecutor<Snapshot>) -> Self {
//     Self::Fetched(nonterminals, terminal)
// }
//
// pub fn rows_positions(&self) -> Option<&HashMap<String, VariablePosition>> {
//     match self {
//         Self::Unfetched(_, positions) => Some(positions),
//         Self::Fetched(_, _) => None,
//     }
// }
//
// pub fn into_rows_iterator(
//     self,
//     execution_interrupt: ExecutionInterrupt,
// ) -> Result<
//     (Nonterminals::OutputIterator, ExecutionContext<Snapshot>),
//     (PipelineExecutionError, ExecutionContext<Snapshot>),
// > {
//     match self {
//         Self::Unfetched(nonterminals, _) => nonterminals.into_iterator(execution_interrupt),
//         Self::Fetched(nonterminals, _) => {
//             let (_, context) = nonterminals.into_iterator(execution_interrupt)?;
//             Err((PipelineExecutionError::FetchUsedAsRows {}, context))
//         }
//     }
// }
//
// pub fn into_maps_iterator(
//     self,
//     execution_interrupt: ExecutionInterrupt,
// ) -> Result<((), ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)> {
//     match self {
//         Self::Unfetched(nonterminals, _) => {
//             let (_, context) = nonterminals.into_iterator(execution_interrupt)?;
//             Err((PipelineExecutionError::FetchUsedAsRows {}, context))
//         }
//         Self::Fetched(nonterminals, terminal) => {
//             let (rows_iterator, context) = nonterminals.into_iterator(execution_interrupt)?;
//             todo!()
//         }
//     }
// }
