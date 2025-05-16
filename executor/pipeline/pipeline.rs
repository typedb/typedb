/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use compiler::{
    executable::{fetch::executable::ExecutableFetch, function::ExecutableFunctionRegistry, pipeline::ExecutableStage},
    query_structure::{ParametrisedQueryStructure, QueryStructure},
    VariablePosition,
};
use concept::thing::thing_manager::ThingManager;
use error::typedb_error;
use ir::pipeline::ParameterRegistry;
use resource::profile::QueryProfile;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    document::ConceptDocument,
    pipeline::{
        delete::DeleteStageExecutor,
        fetch::FetchStageExecutor,
        initial::InitialStage,
        insert::InsertStageExecutor,
        match_::MatchStageExecutor,
        modifiers::{
            DistinctStageExecutor, LimitStageExecutor, OffsetStageExecutor, RequireStageExecutor, SelectStageExecutor,
            SortStageExecutor,
        },
        put::PutStageExecutor,
        reduce::ReduceStageExecutor,
        stage::{ExecutionContext, ReadPipelineStage, StageAPI, WritePipelineStage},
        update::UpdateStageExecutor,
        PipelineExecutionError,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct Pipeline<Snapshot: ReadableSnapshot, Nonterminals: StageAPI<Snapshot>> {
    last_stage: Nonterminals,
    named_outputs: HashMap<String, VariablePosition>,
    query_structure: Option<QueryStructure>,
    fetch: Option<FetchStageExecutor<Snapshot>>,
}

impl<Snapshot: ReadableSnapshot + 'static, Nonterminals: StageAPI<Snapshot>> Pipeline<Snapshot, Nonterminals> {
    fn build_with_fetch(
        variable_names: &HashMap<Variable, String>,
        query_structure: Option<QueryStructure>,
        executable_functions: Arc<ExecutableFunctionRegistry>,
        last_stage: Nonterminals,
        last_stage_output_positions: HashMap<Variable, VariablePosition>,
        executable_fetch: Option<Arc<ExecutableFetch>>,
    ) -> Self {
        let named_outputs = last_stage_output_positions
            .iter()
            .filter_map(|(variable, &position)| variable_names.get(variable).map(|name| (name.clone(), position)))
            .collect::<HashMap<_, _>>();
        let fetch = executable_fetch.map(|executable| FetchStageExecutor::new(executable, executable_functions));
        Self { named_outputs, last_stage, fetch, query_structure }
    }

    pub fn has_fetch(&self) -> bool {
        self.fetch.is_some()
    }

    pub fn rows_positions(&self) -> Option<&HashMap<String, VariablePosition>> {
        match self.fetch {
            None => Some(&self.named_outputs),
            Some(_) => None,
        }
    }

    pub fn query_structure(&self) -> Option<&QueryStructure> {
        self.query_structure.as_ref()
    }

    pub fn into_rows_iterator(
        self,
        execution_interrupt: ExecutionInterrupt,
    ) -> Result<
        (Nonterminals::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        match self.fetch {
            None => self.last_stage.into_iterator(execution_interrupt),
            Some(_) => {
                let (_, context) = self.last_stage.into_iterator(execution_interrupt)?;
                Err((Box::new(PipelineExecutionError::FetchUsedAsRows {}), context))
            }
        }
    }

    pub fn into_documents_iterator(
        self,
        execution_interrupt: ExecutionInterrupt,
    ) -> Result<
        (impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        match self.fetch {
            None => {
                let (_, context) = self.last_stage.into_iterator(execution_interrupt)?;
                Err((Box::new(PipelineExecutionError::RowsUsedAsFetch {}), context))
            }
            Some(fetch_executor) => {
                let (rows_iterator, context) = self.last_stage.into_iterator(execution_interrupt.clone())?;
                Ok(fetch_executor.into_iterator::<Nonterminals>(rows_iterator, context, execution_interrupt))
            }
        }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> Pipeline<Snapshot, ReadPipelineStage<Snapshot>> {
    pub fn build_read_pipeline(
        snapshot: Arc<Snapshot>,
        thing_manager: Arc<ThingManager>,
        variable_names: &HashMap<Variable, String>,
        query_structure: Option<Arc<ParametrisedQueryStructure>>,
        executable_functions: Arc<ExecutableFunctionRegistry>,
        executable_stages: &[ExecutableStage],
        executable_fetch: Option<Arc<ExecutableFetch>>,
        parameters: Arc<ParameterRegistry>,
        input: Option<MaybeOwnedRow<'_>>,
        query_profile: Arc<QueryProfile>,
    ) -> Result<Self, Box<PipelineError>> {
        let output_variable_positions = executable_stages.last().unwrap().output_row_mapping();
        let context = ExecutionContext::new_with_profile(snapshot, thing_manager, parameters.clone(), query_profile);
        let mut last_stage = ReadPipelineStage::Initial(Box::new(
            input
                .map(|row| InitialStage::new_with(context.clone(), row))
                .unwrap_or_else(|| InitialStage::new_empty(context)),
        ));
        for executable_stage in executable_stages {
            match executable_stage {
                ExecutableStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let match_stage =
                        MatchStageExecutor::new(match_executable.clone(), last_stage, executable_functions.clone());
                    last_stage = ReadPipelineStage::Match(Box::new(match_stage));
                }
                ExecutableStage::Insert(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Insert".to_string() }))
                }
                ExecutableStage::Update(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Update".to_string() }))
                }
                ExecutableStage::Put(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Put".to_string() }))
                }
                ExecutableStage::Delete(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Delete".to_string() }))
                }
                ExecutableStage::Select(select_executable) => {
                    let select_stage = SelectStageExecutor::new(select_executable.clone(), last_stage);
                    last_stage = ReadPipelineStage::Select(Box::new(select_stage));
                }
                ExecutableStage::Sort(sort_executable) => {
                    let sort_stage = SortStageExecutor::new(sort_executable.clone(), last_stage);
                    last_stage = ReadPipelineStage::Sort(Box::new(sort_stage));
                }
                ExecutableStage::Distinct(distinct_executable) => {
                    let distinct_stage = DistinctStageExecutor::new(distinct_executable.clone(), last_stage);
                    last_stage = ReadPipelineStage::Distinct(Box::new(distinct_stage));
                }
                ExecutableStage::Offset(offset_executable) => {
                    let offset_stage = OffsetStageExecutor::new(offset_executable.clone(), last_stage);
                    last_stage = ReadPipelineStage::Offset(Box::new(offset_stage));
                }
                ExecutableStage::Limit(limit_executable) => {
                    let limit_stage = LimitStageExecutor::new(limit_executable.clone(), last_stage);
                    last_stage = ReadPipelineStage::Limit(Box::new(limit_stage));
                }
                ExecutableStage::Require(require_executable) => {
                    let require_stage = RequireStageExecutor::new(require_executable.clone(), last_stage);
                    last_stage = ReadPipelineStage::Require(Box::new(require_stage));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable.clone(), last_stage);
                    last_stage = ReadPipelineStage::Reduce(Box::new(reduce_stage));
                }
            }
        }
        Ok(Pipeline::build_with_fetch(
            variable_names,
            query_structure
                .map(|qs| qs.with_parameters(parameters, variable_names.clone(), &output_variable_positions)),
            executable_functions.clone(),
            last_stage,
            output_variable_positions,
            executable_fetch,
        ))
    }
}

impl<Snapshot: WritableSnapshot + 'static> Pipeline<Snapshot, WritePipelineStage<Snapshot>> {
    pub fn build_write_pipeline(
        snapshot: Snapshot,
        variable_names: &HashMap<Variable, String>,
        query_structure: Option<Arc<ParametrisedQueryStructure>>,
        thing_manager: Arc<ThingManager>,
        executable_functions: Arc<ExecutableFunctionRegistry>,
        executable_stages: Vec<ExecutableStage>,
        executable_fetch: Option<Arc<ExecutableFetch>>,
        parameters: Arc<ParameterRegistry>,
        query_profile: Arc<QueryProfile>,
    ) -> Self {
        let output_variable_positions = executable_stages.last().unwrap().output_row_mapping();
        let context =
            ExecutionContext::new_with_profile(Arc::new(snapshot), thing_manager, parameters.clone(), query_profile);
        let mut last_stage = WritePipelineStage::Initial(Box::new(InitialStage::new_empty(context)));
        for executable_stage in executable_stages {
            match executable_stage {
                ExecutableStage::Match(match_executable) => {
                    // TODO: Pass expressions & functions
                    let match_stage =
                        MatchStageExecutor::new(match_executable, last_stage, executable_functions.clone());
                    last_stage = WritePipelineStage::Match(Box::new(match_stage));
                }
                ExecutableStage::Insert(insert_executable) => {
                    let insert_stage = InsertStageExecutor::new(insert_executable, last_stage);
                    last_stage = WritePipelineStage::Insert(Box::new(insert_stage));
                }
                ExecutableStage::Update(update_executable) => {
                    let update_stage = UpdateStageExecutor::new(update_executable, last_stage);
                    last_stage = WritePipelineStage::Update(Box::new(update_stage));
                }
                ExecutableStage::Put(put_executable) => {
                    let put_stage = PutStageExecutor::new(put_executable, last_stage, executable_functions.clone());
                    last_stage = WritePipelineStage::Put(Box::new(put_stage));
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
                ExecutableStage::Distinct(distinct_executable) => {
                    let distinct_stage = DistinctStageExecutor::new(distinct_executable, last_stage);
                    last_stage = WritePipelineStage::Distinct(Box::new(distinct_stage));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable, last_stage);
                    last_stage = WritePipelineStage::Reduce(Box::new(reduce_stage));
                }
            }
        }
        Pipeline::build_with_fetch(
            variable_names,
            query_structure
                .map(|qs| qs.with_parameters(parameters, variable_names.clone(), &output_variable_positions)),
            executable_functions.clone(),
            last_stage,
            output_variable_positions,
            executable_fetch,
        )
    }
}

typedb_error! {
    pub PipelineError(component = "Pipeline", prefix = "PIP") {
        InvalidReadPipelineStage(1, "{stage} clause cannot exist in a read pipeline.", stage: String ),
    }
}
