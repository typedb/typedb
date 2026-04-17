/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use compiler::{
    VariablePosition,
    executable::{fetch::executable::ExecutableFetch, function::ExecutableFunctionRegistry, pipeline::ExecutableStage},
    query_structure::{ParametrisedPipelineStructure, PipelineStructure},
};
use concept::thing::thing_manager::ThingManager;
use error::typedb_error;
use ir::pipeline::ParameterRegistry;
use resource::profile::QueryProfile;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    ExecutionInterrupt,
    document::ConceptDocument,
    pipeline::{
        PipelineExecutionError,
        delete::DeleteStageExecutor,
        fetch::FetchStageExecutor,
        initial::{InitialIterator, InitialStage},
        insert::InsertStageExecutor,
        match_::MatchStageExecutor,
        modifiers::{
            DistinctStageExecutor, LimitStageExecutor, OffsetStageExecutor, RequireStageExecutor, SelectStageExecutor,
            SortStageExecutor,
        },
        put::PutStageExecutor,
        reduce::ReduceStageExecutor,
        stage::{
            ExecutionContext, ReadPipelineStage, ReadStageIterator, StageAPI, WritePipelineStage, WriteStageIterator,
        },
        update::UpdateStageExecutor,
    },
    row::MaybeOwnedRow,
};

pub struct Pipeline<Snapshot: ReadableSnapshot, Nonterminals: StageAPI<Snapshot>> {
    initial_iterator: Nonterminals::InputIterator,
    stages: Vec<Nonterminals>,
    named_outputs: HashMap<String, VariablePosition>,
    pipeline_structure: Option<PipelineStructure>,
    fetch: Option<FetchStageExecutor<Snapshot>>,
    context: ExecutionContext<Snapshot>,
}

impl<Snapshot: ReadableSnapshot + 'static, Nonterminals> Pipeline<Snapshot, Nonterminals>
where
    Nonterminals: StageAPI<Snapshot>,
{
    fn build_with_fetch(
        variable_names: &HashMap<Variable, String>,
        pipeline_structure: Option<PipelineStructure>,
        executable_functions: Arc<ExecutableFunctionRegistry>,
        initial_iterator: Nonterminals::InputIterator,
        stages: Vec<Nonterminals>,
        last_stage_output_positions: HashMap<Variable, VariablePosition>,
        executable_fetch: Option<Arc<ExecutableFetch>>,
        context: ExecutionContext<Snapshot>,
    ) -> Self {
        let named_outputs = last_stage_output_positions
            .iter()
            .filter_map(|(variable, &position)| variable_names.get(variable).map(|name| (name.clone(), position)))
            .collect::<HashMap<_, _>>();
        let fetch = executable_fetch.map(|executable| FetchStageExecutor::new(executable, executable_functions));
        Self { initial_iterator, named_outputs, stages, fetch, pipeline_structure, context }
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

    pub fn pipeline_structure(&self) -> Option<&PipelineStructure> {
        self.pipeline_structure.as_ref()
    }
}

impl<Snapshot: ReadableSnapshot + 'static> Pipeline<Snapshot, ReadPipelineStage<Snapshot>> {
    pub fn build_read_pipeline(
        snapshot: Arc<Snapshot>,
        thing_manager: Arc<ThingManager>,
        variable_names: &HashMap<Variable, String>,
        pipeline_structure: Option<Arc<ParametrisedPipelineStructure>>,
        executable_functions: Arc<ExecutableFunctionRegistry>,
        executable_stages: &[ExecutableStage],
        executable_fetch: Option<Arc<ExecutableFetch>>,
        parameters: Arc<ParameterRegistry>,
        input: Option<MaybeOwnedRow<'_>>,
        query_profile: Arc<QueryProfile>,
    ) -> Result<Self, Box<PipelineError>> {
        let output_variable_positions = executable_stages.last().unwrap().output_row_mapping();
        let context = ExecutionContext::new_with_profile(snapshot, thing_manager, parameters.clone(), query_profile);

        let initial_iterator =
            input.map(|row| InitialStage::new_with(row)).unwrap_or_else(|| InitialStage::new_empty());
        let initial_iterator = ReadStageIterator::Initial(Box::new(initial_iterator.into_iterator()));

        let mut stages: Vec<ReadPipelineStage<Snapshot>> = Vec::with_capacity(executable_stages.len());

        for executable_stage in executable_stages {
            match executable_stage {
                ExecutableStage::Match(conjunction_executable) => {
                    let match_stage =
                        MatchStageExecutor::new(conjunction_executable.clone(), executable_functions.clone());
                    stages.push(ReadPipelineStage::Match(Box::new(match_stage)));
                }
                ExecutableStage::Insert(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Insert".to_string() }));
                }
                ExecutableStage::Update(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Update".to_string() }));
                }
                ExecutableStage::Put(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Put".to_string() }));
                }
                ExecutableStage::Delete(_) => {
                    return Err(Box::new(PipelineError::InvalidReadPipelineStage { stage: "Delete".to_string() }));
                }
                ExecutableStage::Select(select_executable) => {
                    let select_stage = SelectStageExecutor::new(select_executable.clone());
                    stages.push(ReadPipelineStage::Select(Box::new(select_stage)));
                }
                ExecutableStage::Sort(sort_executable) => {
                    let sort_stage = SortStageExecutor::new(sort_executable.clone());
                    stages.push(ReadPipelineStage::Sort(Box::new(sort_stage)));
                }
                ExecutableStage::Distinct(distinct_executable) => {
                    let distinct_stage = DistinctStageExecutor::new(distinct_executable.clone());
                    stages.push(ReadPipelineStage::Distinct(Box::new(distinct_stage)));
                }
                ExecutableStage::Offset(offset_executable) => {
                    let offset_stage = OffsetStageExecutor::new(offset_executable.clone());
                    stages.push(ReadPipelineStage::Offset(Box::new(offset_stage)));
                }
                ExecutableStage::Limit(limit_executable) => {
                    let limit_stage = LimitStageExecutor::new(limit_executable.clone());
                    stages.push(ReadPipelineStage::Limit(Box::new(limit_stage)));
                }
                ExecutableStage::Require(require_executable) => {
                    let require_stage = RequireStageExecutor::new(require_executable.clone());
                    stages.push(ReadPipelineStage::Require(Box::new(require_stage)));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable.clone());
                    stages.push(ReadPipelineStage::Reduce(Box::new(reduce_stage)));
                }
            }
        }
        Ok(Pipeline::build_with_fetch(
            variable_names,
            pipeline_structure.map(|qs| qs.with_parameters(parameters, &variable_names)),
            executable_functions.clone(),
            initial_iterator,
            stages,
            output_variable_positions,
            executable_fetch,
            context,
        ))
    }

    pub fn into_rows_iterator(
        self,
        execution_interrupt: ExecutionInterrupt,
    ) -> Result<
        (ReadStageIterator<Snapshot>, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        match self.fetch {
            None => Self::run_stages(self.initial_iterator, self.stages, self.context, execution_interrupt),
            Some(_) => Err((Box::new(PipelineExecutionError::FetchUsedAsRows {}), self.context)),
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
            None => Err((Box::new(PipelineExecutionError::RowsUsedAsFetch {}), self.context)),
            Some(fetch_executor) => {
                let (rows_iterator, context) =
                    Self::run_stages(self.initial_iterator, self.stages, self.context, execution_interrupt.clone())?;
                Ok(fetch_executor.into_iterator::<ReadPipelineStage<Snapshot>>(
                    rows_iterator,
                    context,
                    execution_interrupt,
                ))
            }
        }
    }

    fn run_stages(
        initial_iterator: ReadStageIterator<Snapshot>,
        stages: Vec<ReadPipelineStage<Snapshot>>,
        context: ExecutionContext<Snapshot>,
        execution_interrupt: ExecutionInterrupt,
    ) -> Result<
        (ReadStageIterator<Snapshot>, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let mut current_iterator = initial_iterator;
        let mut context = context;
        for stage in stages {
            let (next_iterator, next_context) =
                stage.into_iterator(current_iterator, context, execution_interrupt.clone())?;
            current_iterator = next_iterator;
            context = next_context;
        }
        Ok((current_iterator, context))
    }
}

impl<Snapshot: WritableSnapshot + 'static> Pipeline<Snapshot, WritePipelineStage<Snapshot>> {
    pub fn build_write_pipeline(
        snapshot: Snapshot,
        variable_names: &HashMap<Variable, String>,
        pipeline_structure: Option<Arc<ParametrisedPipelineStructure>>,
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

        let initial_iterator =
            WriteStageIterator::Initial(Box::new(InitialIterator::new(crate::batch::FixedBatch::SINGLE_EMPTY_ROW)));

        let mut stages = Vec::with_capacity(executable_stages.len());
        for executable_stage in executable_stages {
            match executable_stage {
                ExecutableStage::Match(conjunction_executable) => {
                    let match_stage = MatchStageExecutor::new(conjunction_executable, executable_functions.clone());
                    stages.push(WritePipelineStage::Match(Box::new(match_stage)));
                }
                ExecutableStage::Insert(insert_executable) => {
                    let insert_stage = InsertStageExecutor::new(insert_executable);
                    stages.push(WritePipelineStage::Insert(Box::new(insert_stage)));
                }
                ExecutableStage::Update(update_executable) => {
                    let update_stage = UpdateStageExecutor::new(update_executable);
                    stages.push(WritePipelineStage::Update(Box::new(update_stage)));
                }
                ExecutableStage::Put(put_executable) => {
                    let put_stage = PutStageExecutor::new(put_executable, executable_functions.clone());
                    stages.push(WritePipelineStage::Put(Box::new(put_stage)));
                }
                ExecutableStage::Delete(delete_executable) => {
                    let delete_stage = DeleteStageExecutor::new(delete_executable);
                    stages.push(WritePipelineStage::Delete(Box::new(delete_stage)));
                }
                ExecutableStage::Select(select_executable) => {
                    let select_stage = SelectStageExecutor::new(select_executable);
                    stages.push(WritePipelineStage::Select(Box::new(select_stage)));
                }
                ExecutableStage::Sort(sort_executable) => {
                    let sort_stage = SortStageExecutor::new(sort_executable);
                    stages.push(WritePipelineStage::Sort(Box::new(sort_stage)));
                }
                ExecutableStage::Offset(offset_executable) => {
                    let offset_stage = OffsetStageExecutor::new(offset_executable);
                    stages.push(WritePipelineStage::Offset(Box::new(offset_stage)));
                }
                ExecutableStage::Limit(limit_executable) => {
                    let limit_stage = LimitStageExecutor::new(limit_executable);
                    stages.push(WritePipelineStage::Limit(Box::new(limit_stage)));
                }
                ExecutableStage::Require(require_executable) => {
                    let require_stage = RequireStageExecutor::new(require_executable);
                    stages.push(WritePipelineStage::Require(Box::new(require_stage)));
                }
                ExecutableStage::Distinct(distinct_executable) => {
                    let distinct_stage = DistinctStageExecutor::new(distinct_executable);
                    stages.push(WritePipelineStage::Distinct(Box::new(distinct_stage)));
                }
                ExecutableStage::Reduce(reduce_executable) => {
                    let reduce_stage = ReduceStageExecutor::new(reduce_executable);
                    stages.push(WritePipelineStage::Reduce(Box::new(reduce_stage)));
                }
            }
        }
        Pipeline::build_with_fetch(
            variable_names,
            pipeline_structure.map(|qs| qs.with_parameters(parameters, &variable_names)),
            executable_functions.clone(),
            initial_iterator,
            stages,
            output_variable_positions,
            executable_fetch,
            context,
        )
    }

    fn run_stages(
        initial_iterator: WriteStageIterator<Snapshot>,
        stages: Vec<WritePipelineStage<Snapshot>>,
        context: ExecutionContext<Snapshot>,
        execution_interrupt: ExecutionInterrupt,
    ) -> Result<
        (WriteStageIterator<Snapshot>, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        let mut current_iterator = initial_iterator;
        let mut context = context;
        for stage in stages {
            let (next_iterator, next_context) =
                stage.into_iterator(current_iterator, context, execution_interrupt.clone())?;
            current_iterator = next_iterator;
            context = next_context;
        }
        Ok((current_iterator, context))
    }

    pub fn into_rows_iterator(
        self,
        execution_interrupt: ExecutionInterrupt,
    ) -> Result<
        (WriteStageIterator<Snapshot>, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        match self.fetch {
            None => Self::run_stages(self.initial_iterator, self.stages, self.context, execution_interrupt),
            Some(_) => Err((Box::new(PipelineExecutionError::FetchUsedAsRows {}), self.context)),
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
            None => Err((Box::new(PipelineExecutionError::RowsUsedAsFetch {}), self.context)),
            Some(fetch_executor) => {
                let (rows_iterator, context) =
                    Self::run_stages(self.initial_iterator, self.stages, self.context, execution_interrupt.clone())?;
                Ok(fetch_executor.into_iterator::<WritePipelineStage<Snapshot>>(
                    rows_iterator,
                    context,
                    execution_interrupt,
                ))
            }
        }
    }
}

typedb_error! {
    pub PipelineError(component = "Pipeline", prefix = "PIP") {
        InvalidReadPipelineStage(1, "{stage} clause cannot exist in a read pipeline.", stage: String ),
    }
}
