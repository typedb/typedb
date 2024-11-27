/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use answer::{variable::Variable, variable_value::VariableValue, Concept, Thing};
use compiler::{
    executable::{
        fetch::executable::{
            ExecutableFetch, ExecutableFetchListSubFetch, FetchObjectInstruction, FetchSomeInstruction,
        },
        function::ExecutableFunction,
        match_::planner::function_plan::ExecutableFunctionRegistry,
        next_executable_id,
    },
    VariablePosition,
};
use concept::{
    error::ConceptReadError,
    thing::{
        object::{HasIterator, ObjectAPI},
        thing_manager::ThingManager,
    },
    type_::{attribute_type::AttributeType, OwnerAPI, TypeAPI},
};
use encoding::value::label::Label;
use error::typedb_error;
use ir::{pattern::ParameterID, pipeline::ParameterRegistry};
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    document::{ConceptDocument, DocumentLeaf, DocumentList, DocumentMap, DocumentNode},
    error::ReadExecutionError,
    pipeline::{
        pipeline::{Pipeline, PipelineError},
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError,
    },
    profile::{QueryProfile, StageProfile},
    read::{
        pattern_executor::PatternExecutor, step_executor::create_executors_for_function,
        tabled_functions::TabledFunctions, QueryPatternSuspensions,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

macro_rules! exactly_one_or_return_err {
    ($call:expr, $error:expr) => {{
        let first_result = $call;
        if $call.is_some() {
            return Err($error);
        }
        first_result
    }};
}

pub struct FetchStageExecutor<Snapshot: ReadableSnapshot> {
    executable: Arc<ExecutableFetch>,
    functions: Arc<ExecutableFunctionRegistry>,
    _phantom: PhantomData<Snapshot>,
}

impl<Snapshot: ReadableSnapshot + 'static> FetchStageExecutor<Snapshot> {
    pub(crate) fn new(executable: Arc<ExecutableFetch>, functions: Arc<ExecutableFunctionRegistry>) -> Self {
        Self { executable, functions, _phantom: PhantomData }
    }

    pub(crate) fn into_iterator<PreviousStage: StageAPI<Snapshot>>(
        self,
        previous_iterator: PreviousStage::OutputIterator,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> (impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>, ExecutionContext<Snapshot>) {
        let ExecutionContext { snapshot, thing_manager, parameters, profile } = context.clone();
        let executable = self.executable;
        let functions = self.functions;
        let stage_profile = profile.profile_stage(|| String::from("Fetch"), executable.executable_id);
        let documents_iterator = previous_iterator
            .map_static(move |row_result| match row_result {
                Ok(row) => execute_fetch(
                    &executable,
                    snapshot.clone(),
                    thing_manager.clone(),
                    parameters.clone(),
                    functions.clone(),
                    profile.clone(),
                    stage_profile.clone(),
                    row.as_reference(),
                    interrupt.clone(),
                )
                .map_err(|err| Box::new(PipelineExecutionError::FetchError { typedb_source: err })),
                Err(err) => Err(err.clone()),
            })
            .into_iter();
        (documents_iterator, context)
    }
}

fn execute_fetch(
    fetch: &ExecutableFetch,
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    stage_profile: Arc<StageProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<ConceptDocument, FetchExecutionError> {
    let step = stage_profile.extend_or_get(0, || String::from("Root fetch"));
    let measurement = step.start_measurement();
    let node = execute_object(
        &fetch.object_instruction,
        snapshot,
        thing_manager,
        parameters,
        functions,
        query_profile,
        row,
        interrupt,
    )?;
    measurement.end(&step, 1, 1);
    Ok(ConceptDocument { root: node })
}

fn execute_fetch_some(
    fetch_some: &FetchSomeInstruction,
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentNode, FetchExecutionError> {
    match fetch_some {
        FetchSomeInstruction::SingleVar(position) => variable_value_to_document(row.get(*position).as_reference()),
        FetchSomeInstruction::SingleAttribute(position, attribute_type) => {
            execute_single_attribute(snapshot, thing_manager, row, position, attribute_type)
        }
        FetchSomeInstruction::SingleFunction(function, variable_positions) => execute_single_function(
            snapshot,
            thing_manager,
            parameters,
            functions_registry,
            query_profile,
            row,
            interrupt,
            variable_positions,
            function,
        ),
        FetchSomeInstruction::Object(object) => execute_object(
            object,
            snapshot,
            thing_manager,
            parameters,
            functions_registry,
            query_profile.clone(),
            row,
            interrupt,
        ),
        FetchSomeInstruction::ListFunction(function, variable_positions) => execute_list_function(
            snapshot,
            thing_manager,
            parameters,
            functions_registry,
            query_profile,
            row,
            interrupt,
            variable_positions,
            function,
        ),
        FetchSomeInstruction::ListSubFetch(subfetch) => execute_list_subfetch(
            snapshot,
            thing_manager,
            parameters,
            functions_registry,
            query_profile,
            row,
            interrupt,
            subfetch,
        ),
        FetchSomeInstruction::ListAttributesAsList(position, attribute_type) => {
            execute_list_attributes_as_list(snapshot, thing_manager, row, position, attribute_type)
        }
        FetchSomeInstruction::ListAttributesFromList(_, _) => {
            Err(FetchExecutionError::Unimplemented { description: "List attributes are not available yet." })
        }
    }
}

fn execute_single_attribute(
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    row: MaybeOwnedRow<'_>,
    position: &VariablePosition,
    attribute_type: &AttributeType,
) -> Result<DocumentNode, FetchExecutionError> {
    let variable_value = row.get(*position).as_reference();
    match variable_value {
        VariableValue::Empty => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        VariableValue::Thing(Thing::Entity(entity)) => {
            execute_attribute_single(entity, attribute_type.clone(), snapshot, thing_manager).map(DocumentNode::Leaf)
        }
        VariableValue::Thing(Thing::Relation(relation)) => {
            execute_attribute_single(relation, attribute_type.clone(), snapshot, thing_manager).map(DocumentNode::Leaf)
        }
        VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
        VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
        VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
    }
}

fn execute_single_function(
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    mut interrupt: ExecutionInterrupt,
    variable_positions: &HashMap<Variable, VariablePosition>,
    function: &ExecutableFunction,
) -> Result<DocumentNode, FetchExecutionError> {
    let (mut pattern_executor, execution_context) = prepare_single_function_execution(
        snapshot,
        thing_manager,
        parameters,
        functions_registry.clone(),
        query_profile,
        variable_positions,
        row,
        function,
    )?;
    let mut tabled_functions = TabledFunctions::new(functions_registry.clone());
    let mut suspend_points = QueryPatternSuspensions::new();

    let batch = exactly_one_or_return_err!(
        pattern_executor
            .compute_next_batch(&execution_context, &mut interrupt, &mut tabled_functions, &mut suspend_points)
            .map_err(|err| FetchExecutionError::ReadExecution { typedb_source: Box::new(err) })?,
        FetchExecutionError::FetchSingleFunctionNotSingle { func_name: "func".to_string() } // TODO: Can we get function name here?
    );

    let batch = match batch {
        Some(batch) => batch,
        None => return Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
    };

    // TODO: We could create an iterator over rows in a single call here instead
    let mut row_iter = batch.into_iter();
    let document = match exactly_one_or_return_err!(
        row_iter.next(),
        FetchExecutionError::FetchSingleFunctionNotSingle { func_name: "func".to_string() }
    ) {
        Some(row) => {
            let mut value_iter = row.row().iter();
            let result = exactly_one_or_return_err!(
                value_iter.next(),
                FetchExecutionError::FetchSingleFunctionNotScalar { func_name: "func".to_string() }
            );
            match result {
                Some(value) => variable_value_to_document(value.clone())?,
                None => DocumentNode::Leaf(DocumentLeaf::Empty),
            }
        }
        None => DocumentNode::Leaf(DocumentLeaf::Empty),
    };

    Ok(document)
}

fn execute_object(
    fetch_object: &FetchObjectInstruction,
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentNode, FetchExecutionError> {
    match fetch_object {
        FetchObjectInstruction::Entries(entries) => {
            let object = execute_object_entries(
                entries,
                snapshot,
                thing_manager,
                parameters,
                functions,
                query_profile,
                row,
                interrupt,
            )?;
            Ok(DocumentNode::Map(object))
        }
        FetchObjectInstruction::Attributes(position) => {
            execute_object_attributes(*position, snapshot, thing_manager, row)
        }
    }
}

fn execute_list_function(
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    mut interrupt: ExecutionInterrupt,
    variable_positions: &HashMap<Variable, VariablePosition>,
    function: &ExecutableFunction,
) -> Result<DocumentNode, FetchExecutionError> {
    let (mut pattern_executor, execution_context) = prepare_single_function_execution(
        snapshot,
        thing_manager,
        parameters,
        functions_registry.clone(),
        query_profile,
        variable_positions,
        row,
        function,
    )?;
    let mut tabled_functions = TabledFunctions::new(functions_registry.clone());
    let mut suspend_points = QueryPatternSuspensions::new();

    let mut nodes = Vec::new();
    // TODO: We could create an iterator over rows in a single call here instead
    while let Some(batch) = pattern_executor
        .compute_next_batch(&execution_context, &mut interrupt, &mut tabled_functions, &mut suspend_points)
        .map_err(|err| FetchExecutionError::ReadExecution { typedb_source: Box::new(err) })?
    {
        for row in batch {
            for value in row {
                nodes.push(variable_value_to_document(value.clone())?);
            }
        }
    }

    Ok(DocumentNode::List(DocumentList::new_from(nodes)))
}

fn execute_list_subfetch(
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
    executable_subfetch: &ExecutableFetchListSubFetch,
) -> Result<DocumentNode, FetchExecutionError> {
    let ExecutableFetchListSubFetch { input_position_mapping, variable_registry, stages, fetch } = executable_subfetch;

    let pipeline = if input_position_mapping.is_empty() {
        Pipeline::build_read_pipeline(
            snapshot,
            thing_manager,
            variable_registry.variable_names(),
            functions_registry,
            stages,
            Some(fetch.clone()),
            parameters,
            None,
        )
    } else {
        let max_position = input_position_mapping.values().max().map(|pos| pos.as_usize()).unwrap();
        let mut initial_row = vec![VariableValue::Empty; max_position + 1];
        input_position_mapping.iter().for_each(|(parent_row_position, local_row_position)| {
            initial_row[local_row_position.as_usize()] =
                row[parent_row_position.as_usize()].as_reference().into_owned();
        });
        let initial_row = MaybeOwnedRow::new_owned(initial_row, row.multiplicity());
        Pipeline::build_read_pipeline(
            snapshot,
            thing_manager,
            variable_registry.variable_names(),
            functions_registry,
            stages,
            Some(fetch.clone()),
            parameters,
            Some(initial_row),
        )
    }
    .map_err(|typedb_source| FetchExecutionError::Pipeline { typedb_source })?;

    let (iterator, _context) = pipeline
        .into_documents_iterator(interrupt)
        .map_err(|(err, _context)| FetchExecutionError::SubFetch { typedb_source: err })?;

    let mut nodes = Vec::new();
    for result in iterator {
        nodes.push(result.map_err(|err| FetchExecutionError::SubFetch { typedb_source: err })?.root);
    }
    Ok(DocumentNode::List(DocumentList::new_from(nodes)))
}

fn execute_list_attributes_as_list(
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    row: MaybeOwnedRow<'_>,
    position: &VariablePosition,
    attribute_type: &AttributeType,
) -> Result<DocumentNode, FetchExecutionError> {
    let variable_value = row.get(*position).as_reference();
    match variable_value {
        VariableValue::Empty => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        VariableValue::Thing(Thing::Entity(entity)) => {
            execute_attributes_list(entity, attribute_type.clone(), snapshot, thing_manager).map(DocumentNode::List)
        }
        VariableValue::Thing(Thing::Relation(relation)) => {
            execute_attributes_list(relation, attribute_type.clone(), snapshot, thing_manager).map(DocumentNode::List)
        }
        VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
        VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
        VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
    }
}

fn execute_object_attributes(
    variable_position: VariablePosition,
    snapshot: Arc<impl ReadableSnapshot>,
    thing_manager: Arc<ThingManager>,
    row: MaybeOwnedRow<'_>,
) -> Result<DocumentNode, FetchExecutionError> {
    let concept = row.get(variable_position);
    match concept {
        VariableValue::Empty => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        VariableValue::Thing(Thing::Entity(entity)) => {
            execute_attributes_all(entity.as_reference(), snapshot, thing_manager)
        }
        VariableValue::Thing(Thing::Relation(relation)) => {
            execute_attributes_all(relation.as_reference(), snapshot, thing_manager)
        }
        VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
        VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
        VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
    }
}

fn execute_attributes_all<'a>(
    object: impl ObjectAPI<'a>,
    snapshot: Arc<impl ReadableSnapshot>,
    thing_manager: Arc<ThingManager>,
) -> Result<DocumentNode, FetchExecutionError> {
    let mut iter = object.get_has_unordered(snapshot.as_ref(), &thing_manager);
    let mut map: HashMap<Arc<Label<'static>>, DocumentNode> = HashMap::new();
    while let Some(result) = iter.next() {
        let (attribute, count) = result.map_err(|err| FetchExecutionError::ConceptRead { source: err })?;
        let attribute_type = attribute.type_();
        let label = attribute_type
            .get_label_arc(snapshot.as_ref(), thing_manager.type_manager())
            .map_err(|err| FetchExecutionError::ConceptRead { source: err })?;
        let leaf = DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(attribute.into_owned()))));

        let is_bounded_to_one = object
            .type_()
            .is_owned_attribute_type_bounded_to_one(snapshot.as_ref(), thing_manager.type_manager(), attribute_type)
            .map_err(|err| FetchExecutionError::ConceptRead { source: err })?;
        if is_bounded_to_one {
            map.insert(label, leaf);
        } else {
            let entry = map.entry(label).or_insert_with(|| DocumentNode::List(DocumentList::new()));
            for _ in 0..count {
                entry.as_list_mut().list.push(leaf.clone())
            }
        }
    }
    Ok(DocumentNode::Map(DocumentMap::GeneratedKeys(map)))
}

fn execute_attribute_single<'a>(
    object: impl ObjectAPI<'a>,
    attribute_type: AttributeType,
    snapshot: Arc<impl ReadableSnapshot>,
    thing_manager: Arc<ThingManager>,
) -> Result<DocumentLeaf, FetchExecutionError> {
    let mut iter = prepare_attribute_type_has_iterator(object, attribute_type.clone(), &snapshot, &thing_manager)?;

    while let Some(result) = iter.next() {
        let (has, count) = result.map_err(|source| FetchExecutionError::ConceptRead { source })?;
        let attribute = has.attribute();
        let suitable = attribute
            .type_()
            .is_subtype_transitive_of_or_same(snapshot.as_ref(), thing_manager.type_manager(), attribute_type.clone())
            .map_err(|source| FetchExecutionError::ConceptRead { source })?;
        if suitable {
            debug_assert!(count <= 1);
            return Ok(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(has.attribute().into_owned()))));
        }
    }
    Ok(DocumentLeaf::Empty)
}

fn execute_attributes_list<'a>(
    object: impl ObjectAPI<'a>,
    attribute_type: AttributeType,
    snapshot: Arc<impl ReadableSnapshot>,
    thing_manager: Arc<ThingManager>,
) -> Result<DocumentList, FetchExecutionError> {
    let mut list = DocumentList::new();
    let mut iter = prepare_attribute_type_has_iterator(object, attribute_type.clone(), &snapshot, &thing_manager)?;

    while let Some(result) = iter.next() {
        let (has, count) = result.map_err(|source| FetchExecutionError::ConceptRead { source })?;
        let attribute = has.attribute();
        let suitable = attribute
            .type_()
            .is_subtype_transitive_of_or_same(snapshot.as_ref(), thing_manager.type_manager(), attribute_type.clone())
            .map_err(|source| FetchExecutionError::ConceptRead { source })?;
        if suitable {
            for _ in 0..count {
                list.list.push(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(
                    has.attribute().into_owned(),
                )))));
            }
        }
    }
    Ok(list)
}

fn prepare_attribute_type_has_iterator<'a>(
    object: impl ObjectAPI<'a>,
    attribute_type: AttributeType,
    snapshot: &Arc<impl ReadableSnapshot>,
    thing_manager: &Arc<ThingManager>,
) -> Result<HasIterator, FetchExecutionError> {
    let subtypes = attribute_type
        .get_subtypes_transitive(snapshot.as_ref(), thing_manager.type_manager())
        .map_err(|source| FetchExecutionError::ConceptRead { source })?;
    let attribute_types = TypeAPI::chain_types(attribute_type.clone(), subtypes.into_iter().cloned());

    object
        .get_has_types_range_unordered(snapshot.as_ref(), thing_manager.as_ref(), attribute_types)
        .map_err(|source| FetchExecutionError::ConceptRead { source })
}

fn prepare_single_function_execution<Snapshot: ReadableSnapshot + 'static>(
    snapshot: Arc<Snapshot>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    variable_positions: &HashMap<Variable, VariablePosition>,
    row: MaybeOwnedRow<'_>,
    function: &ExecutableFunction,
) -> Result<(PatternExecutor, Arc<ExecutionContext<Snapshot>>), FetchExecutionError> {
    let mut args = vec![VariableValue::Empty; function.argument_positions.len()];
    for (var, write_pos) in &function.argument_positions {
        debug_assert!(write_pos.as_usize() < args.len());
        args[write_pos.as_usize()] = row.get(*variable_positions.get(var).unwrap()).clone().into_owned();
    }
    let args = MaybeOwnedRow::new_owned(args, row.multiplicity());

    let step_executors =
        create_executors_for_function(&snapshot, &thing_manager, &functions_registry, &query_profile, function)
            .map_err(|err| FetchExecutionError::ConceptRead { source: err })?;
    let mut pattern_executor = PatternExecutor::new(next_executable_id(), step_executors);
    pattern_executor.prepare(FixedBatch::from(args));
    Ok((pattern_executor, Arc::new(ExecutionContext::new(snapshot, thing_manager, parameters))))
}

fn execute_object_entries(
    entries: &HashMap<ParameterID, FetchSomeInstruction>,
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    functions: Arc<ExecutableFunctionRegistry>,
    query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentMap, FetchExecutionError> {
    let mut map = HashMap::with_capacity(entries.len());
    for (id, fetch_some) in entries {
        map.insert(
            *id,
            execute_fetch_some(
                fetch_some,
                snapshot.clone(),
                thing_manager.clone(),
                parameters.clone(),
                functions.clone(),
                query_profile.clone(),
                row.as_reference(),
                interrupt.clone(),
            )?,
        );
    }
    Ok(DocumentMap::UserKeys(map))
}

fn variable_value_to_document(variable_value: VariableValue<'_>) -> Result<DocumentNode, FetchExecutionError> {
    match variable_value.into_owned() {
        VariableValue::Empty => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        VariableValue::Type(type_) => Ok(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Type(type_)))),
        VariableValue::Thing(thing) => match thing {
            Thing::Entity(_) => Err(FetchExecutionError::FetchEntities {}),
            Thing::Relation(_) => Err(FetchExecutionError::FetchRelations {}),
            Thing::Attribute(_) => Ok(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(thing)))),
        },
        VariableValue::Value(value) => Ok(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Value(value)))),
        VariableValue::ThingList(_) => {
            todo!()
        }
        VariableValue::ValueList(_) => {
            todo!()
        }
    }
}

typedb_error!(
    pub FetchExecutionError(component = "Fetch execution", prefix = "FEX") {
        Unimplemented(0, "Unimplemented: {description}.", description: &'static str),

        FetchAttributesOfType(1, "Received unexpected Type instead of Entity or Relation while fetching owned attributes."),
        FetchAttributesOfAttribute(2, "Received unexpected Attribute instead of Entity or Relation while fetching owned attributes."),
        FetchAttributesOfValue(3, "Received unexpected Value instead of Entity or Relation while fetching owned attributes."),
        FetchAttributesOfList(4, "Received unexpected List instead of Entity or Relation while fetching owned attributes."),

        FetchEntities(5, "Fetching entities is not supported, use '$var.*' or '$var.<attribute type>' to fetch attributes instead."),
        FetchRelations(6, "Fetching relations is not supported, use '$var.*' or '$var.<attribute type>' to fetch attributes instead."),

        FetchSingleFunctionNotScalar(7, "Fetching results of a function call '{func_name}()' expected a scalar return, got a tuple instead.", func_name: String),
        FetchSingleFunctionNotSingle(8, "Fetching results of a function call '{func_name}()' expected a single return, got a stream instead. It must be wrapped in `[]` to collect into a list.", func_name: String),

        SubFetch(10, "Error executing sub fetch.", ( typedb_source : Box<PipelineExecutionError>)),

        ConceptRead(30, "Unexpected failed to read concept.", ( source: Box<ConceptReadError>)),
        ReadExecution(31, "Unexpected failed to execute read.", ( typedb_source: Box<ReadExecutionError> )),
        Pipeline(32, "Pipeline error.", ( typedb_source: Box<PipelineError> )),
    }
);
