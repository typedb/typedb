/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use typeql::parser::Rule::query;
use answer::{Concept, Thing, variable::Variable, variable_value::VariableValue};
use compiler::{
    VariablePosition,
    executable::{
        fetch::executable::{
            ExecutableFetch, ExecutableFetchListSubFetch, FetchObjectInstruction, FetchSomeInstruction,
        },
        function::{ExecutableFunctionRegistry, executable::ExecutableFunction},
        next_executable_id,
    },
};
use concept::{
    error::ConceptReadError,
    thing::{has::Has, object::ObjectAPI, thing_manager::ThingManager},
    type_::{OwnerAPI, TypeAPI, attribute_type::AttributeType},
};
use encoding::value::label::Label;
use error::{typedb_error, unimplemented_feature};
use function::function_manager::FunctionManager;
use ir::{pattern::ParameterID, pipeline::ParameterRegistry};
use ir::pipeline::QueryContext;
use lending_iterator::LendingIterator;
use resource::profile::{QueryProfile, StepProfile, StorageCounters};
use storage::snapshot::ReadableSnapshot;

use crate::{
    ExecutionInterrupt, Provenance,
    batch::{Batch, FixedBatch},
    document::{ConceptDocument, DocumentLeaf, DocumentList, DocumentMap, DocumentNode},
    error::ReadExecutionError,
    pipeline::{
        PipelineExecutionError,
        pipeline::{Pipeline, PipelineError},
        stage::{ExecutionContext, StageAPI},
    },
    read::{
        pattern_executor::PatternExecutor, step_executor::create_executors_for_function,
        tabled_functions::TabledFunctions,
    },
    row::MaybeOwnedRow,
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
        execution_context: ExecutionContext<Snapshot>,
        query_context: Arc<QueryContext>,
        interrupt: ExecutionInterrupt,
    ) -> (impl Iterator<Item=Result<ConceptDocument, Box<PipelineExecutionError>>>, ExecutionContext<Snapshot>) {
        // let ExecutionContext { snapshot, thing_manager, function_manager,
        //     // parameters, profile
        // } = context.clone();
        let executable = self.executable;
        let functions = self.functions;
        let stage_profile = query_context.profile.profile_stage(|| String::from("Fetch"), executable.executable_id);
        let pattern_profile = stage_profile.create_or_get_pattern(|| String::from("Fetch pattern"));
        let step = pattern_profile.extend_or_get_step(0, || String::from("Root fetch"));
        let documents_iterator = previous_iterator
            .map_static({
                let cloned_context = execution_context.clone();
                move |row_result| match row_result {
                    Ok(row) => execute_fetch(
                        &cloned_context,
                        query_context.clone(),
                        &executable,
                        // snapshot.clone(),
                        // thing_manager.clone(),
                        // function_manager.clone(),
                        // parameters.clone(),
                        functions.clone(),
                        // profile.clone(),
                        &step,
                        row.as_reference(),
                        interrupt.clone(),
                    )
                        .map_err(|err| Box::new(PipelineExecutionError::FetchError { typedb_source: err })),
                    Err(err) => Err(err.clone()),
                }
            })
            .into_iter();
        (documents_iterator, execution_context)
    }
}

fn execute_fetch(
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    query_context: Arc<QueryContext>,
    fetch: &ExecutableFetch,
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    // parameters: Arc<ParameterRegistry>,
    functions: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    step: &StepProfile,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<ConceptDocument, FetchExecutionError> {
    let measurement = step.start_measurement();
    let node = execute_object(
        execution_context,
        query_context,
        &fetch.object_instruction,
        // snapshot,
        // thing_manager,
        // function_manager,
        // parameters,
        functions,
        // query_profile,
        row,
        interrupt,
    )?;
    measurement.end(step, 1, 1);
    Ok(ConceptDocument { root: node })
}

fn execute_fetch_some(
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    query_context: Arc<QueryContext>,
    fetch_some: &FetchSomeInstruction,
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    // parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentNode, FetchExecutionError> {
    match fetch_some {
        FetchSomeInstruction::SingleVar(position) => variable_value_to_document(row.get(*position).as_reference()),
        FetchSomeInstruction::SingleAttribute(position, attribute_type) => {
            execute_single_attribute(execution_context, row, position, attribute_type)
        }
        FetchSomeInstruction::SingleFunction(function, variable_positions) => execute_single_function(
            execution_context,
            &query_context,
            // snapshot,
            // thing_manager,
            // function_manager,
            // parameters,
            functions_registry,
            // query_profile,
            row,
            interrupt,
            variable_positions,
            function,
        ),
        FetchSomeInstruction::Label(ty) => Ok(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Type(*ty)))),
        FetchSomeInstruction::Object(object) => execute_object(
            execution_context,
            query_context,
            object,
            // snapshot,
            // thing_manager,
            // function_manager,
            // parameters,
            functions_registry,
            // query_profile,
            row,
            interrupt,
        ),
        FetchSomeInstruction::ListFunction(function, variable_positions) => execute_list_function(
            execution_context,
            &query_context,
            // snapshot,
            // thing_manager,
            // function_manager,
            // parameters,
            functions_registry,
            // query_profile,
            row,
            interrupt,
            variable_positions,
            function,
        ),
        FetchSomeInstruction::ListSubFetch(subfetch) => execute_list_subfetch(
            execution_context,
            query_context.clone(),
            // snapshot,
            // thing_manager,
            // function_manager,
            // parameters,
            functions_registry,
            // query_profile,
            row,
            interrupt,
            subfetch,
        ),
        FetchSomeInstruction::ListAttributesAsList(position, attribute_type) => {
            execute_list_attributes_as_list(execution_context, row, position, attribute_type)
        }
        FetchSomeInstruction::ListAttributesFromList(_, _) => {
            Err(FetchExecutionError::Unimplemented { description: "List attributes are not available yet." })
        }
    }
}

fn execute_single_attribute(
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: MaybeOwnedRow<'_>,
    position: &VariablePosition,
    attribute_type: &AttributeType,
) -> Result<DocumentNode, FetchExecutionError> {
    let variable_value = row.get(*position).as_reference();
    match variable_value {
        VariableValue::None => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        VariableValue::Thing(Thing::Entity(entity)) => {
            execute_attribute_single(execution_context, entity, *attribute_type).map(DocumentNode::Leaf)
        }
        VariableValue::Thing(Thing::Relation(relation)) => {
            execute_attribute_single(execution_context, relation, *attribute_type).map(DocumentNode::Leaf)
        }
        VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
        VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
        VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
    }
}

fn execute_single_function(
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    query_context: &QueryContext,
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    // parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    mut interrupt: ExecutionInterrupt,
    variable_positions: &HashMap<Variable, VariablePosition>,
    function: &ExecutableFunction,
) -> Result<DocumentNode, FetchExecutionError> {
    let mut pattern_executor = prepare_single_function_execution(
        execution_context,
        query_context,
        // snapshot,
        // thing_manager,
        // function_manager,
        // parameters,
        functions_registry.clone(),
        // query_profile,
        variable_positions,
        row,
        function,
    )?;
    let mut tabled_functions = TabledFunctions::new(functions_registry.clone(), query_context.profile.clone());

    let batch = exactly_one_or_return_err!(
        pattern_executor
            .compute_next_batch(execution_context, &query_context.parameters, &mut interrupt, &mut tabled_functions)
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
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    query_context: Arc<QueryContext>,
    fetch_object: &FetchObjectInstruction,
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    // parameters: Arc<ParameterRegistry>,
    functions: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentNode, FetchExecutionError> {
    match fetch_object {
        FetchObjectInstruction::Entries(entries) => {
            let object = execute_object_entries(
                execution_context,
                query_context,
                entries,
                // snapshot,
                // thing_manager,
                // function_manager,
                // parameters,
                functions,
                // query_profile,
                row,
                interrupt,
            )?;
            Ok(DocumentNode::Map(object))
        }
        FetchObjectInstruction::Attributes(position) => {
            execute_object_attributes(*position, execution_context.snapshot.as_ref(), &execution_context.thing_manager, row)
        }
    }
}

fn execute_list_function(
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    query_context: &QueryContext,
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    // parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    mut interrupt: ExecutionInterrupt,
    variable_positions: &HashMap<Variable, VariablePosition>,
    function: &ExecutableFunction,
) -> Result<DocumentNode, FetchExecutionError> {
    let mut pattern_executor = prepare_single_function_execution(
        execution_context,
        query_context,
        // snapshot,
        // thing_manager,
        // function_manager,
        // parameters,
        functions_registry.clone(),
        // query_profile,
        variable_positions,
        row,
        function,
    )?;
    let mut tabled_functions = TabledFunctions::new(functions_registry.clone(), query_context.profile.clone());

    let mut nodes = Vec::new();
    // TODO: We could create an iterator over rows in a single call here instead
    while let Some(batch) = pattern_executor
        .compute_next_batch(execution_context, &query_context.parameters, &mut interrupt, &mut tabled_functions)
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
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    query_context: Arc<QueryContext>,
    // parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
    executable_subfetch: &ExecutableFetchListSubFetch,
) -> Result<DocumentNode, FetchExecutionError> {
    let ExecutableFetchListSubFetch { input_position_mapping, variable_registry, stages, fetch } = executable_subfetch;
    let width = input_position_mapping.values().max().map(|pos| pos.as_usize() as u32 + 1).unwrap_or(0);
    let mut initial_batch = Batch::new(width, 1);
    initial_batch.append(|mut write_to| {
        input_position_mapping.iter().for_each(|(parent_row_position, local_row_position)| {
            write_to.set(*local_row_position, row[parent_row_position.as_usize()].as_reference().into_owned());
        });
    });
    let pipeline = Pipeline::build_read_pipeline(
        execution_context.clone(),
        query_context,
        variable_registry.variable_names(),
        None,
        functions_registry,
        None,
        stages,
        Some(fetch.clone()),
        // parameters,
        initial_batch,
        // query_profile,
    )
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
    execution_context: &ExecutionContext<impl ReadableSnapshot>,
    row: MaybeOwnedRow<'_>,
    position: &VariablePosition,
    attribute_type: &AttributeType,
) -> Result<DocumentNode, FetchExecutionError> {
    let variable_value = row.get(*position).as_reference();
    match variable_value {
        VariableValue::None => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        VariableValue::Thing(Thing::Entity(entity)) => {
            execute_attributes_list(entity, *attribute_type, execution_context).map(DocumentNode::List)
        }
        VariableValue::Thing(Thing::Relation(relation)) => {
            execute_attributes_list(relation, *attribute_type, execution_context).map(DocumentNode::List)
        }
        VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
        VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
        VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
    }
}

fn execute_object_attributes(
    variable_position: VariablePosition,
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    row: MaybeOwnedRow<'_>,
) -> Result<DocumentNode, FetchExecutionError> {
    let concept = row.get(variable_position);
    match concept {
        VariableValue::None => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        &VariableValue::Thing(Thing::Entity(entity)) => execute_attributes_all(entity, snapshot, thing_manager),
        &VariableValue::Thing(Thing::Relation(relation)) => execute_attributes_all(relation, snapshot, thing_manager),
        VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
        VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
        VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
    }
}

fn execute_attributes_all(
    object: impl ObjectAPI,
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
) -> Result<DocumentNode, FetchExecutionError> {
    let iter = object
        .get_has_unordered(snapshot, &thing_manager, StorageCounters::DISABLED)
        .map_err(|err| FetchExecutionError::ConceptRead { typedb_source: err })?;
    let mut map: HashMap<Arc<Label>, DocumentNode> = HashMap::new();
    for result in iter {
        let (has, count) = result.map_err(|err| FetchExecutionError::ConceptRead { typedb_source: err })?;
        let attribute = has.attribute();
        let attribute_type = attribute.type_();
        let label = attribute_type
            .get_label_arc(snapshot, thing_manager.type_manager())
            .map_err(|err| FetchExecutionError::ConceptRead { typedb_source: err })?;
        let leaf = DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(attribute.clone()))));

        let is_bounded_to_one = object
            .type_()
            .is_owned_attribute_type_bounded_to_one(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|err| FetchExecutionError::ConceptRead { typedb_source: err })?;
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

fn execute_attribute_single(
    execution_context: &ExecutionContext<impl ReadableSnapshot>,
    object: impl ObjectAPI,
    attribute_type: AttributeType,
    // snapshot: &impl ReadableSnapshot,
    // thing_manager: &ThingManager,
) -> Result<DocumentLeaf, FetchExecutionError> {
    let iter = prepare_attribute_type_has_iterator(object, attribute_type, execution_context.snapshot.as_ref(), execution_context.thing_manager.as_ref())?;

    for result in iter {
        let (has, count) = result.map_err(|source| FetchExecutionError::ConceptRead { typedb_source: source })?;
        let attribute = has.attribute();
        let suitable = attribute
            .type_()
            .is_subtype_transitive_of_or_same(execution_context.snapshot.as_ref(), execution_context.thing_manager.type_manager(), attribute_type)
            .map_err(|source| FetchExecutionError::ConceptRead { typedb_source: source })?;
        if suitable {
            debug_assert!(count <= 1);
            return Ok(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(has.attribute().clone()))));
        }
    }
    Ok(DocumentLeaf::Empty)
}

fn execute_attributes_list<Snapshot: ReadableSnapshot>(
    object: impl ObjectAPI,
    attribute_type: AttributeType,
    execution_context: &ExecutionContext<Snapshot>,
    // snapshot: &Snapshot,
    // thing_manager: &ThingManager,
) -> Result<DocumentList, FetchExecutionError> {
    let mut list = DocumentList::new();
    let iter = prepare_attribute_type_has_iterator(object, attribute_type, execution_context.snapshot.as_ref(), execution_context.thing_manager.as_ref())?;

    for result in iter {
        let (has, count) = result.map_err(|source| FetchExecutionError::ConceptRead { typedb_source: source })?;
        let attribute = has.attribute();
        let suitable = attribute
            .type_()
            .is_subtype_transitive_of_or_same(execution_context.snapshot.as_ref(), execution_context.thing_manager.type_manager(), attribute_type)
            .map_err(|source| FetchExecutionError::ConceptRead { typedb_source: source })?;
        if suitable {
            for _ in 0..count {
                list.list.push(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(
                    has.attribute().clone(),
                )))));
            }
        }
    }
    Ok(list)
}

fn prepare_attribute_type_has_iterator<'a, Snapshot: ReadableSnapshot>(
    object: impl ObjectAPI,
    attribute_type: AttributeType,
    snapshot: &'a Snapshot,
    thing_manager: &'a ThingManager,
) -> Result<impl Iterator<Item=Result<(Has, u64), Box<ConceptReadError>>> + 'a, FetchExecutionError> {
    let subtypes = attribute_type
        .get_subtypes_transitive(snapshot, thing_manager.type_manager())
        .map_err(|source| FetchExecutionError::ConceptRead { typedb_source: source })?;
    let iter = Iterator::filter(
        object
            .get_has_types_range_unordered(snapshot, thing_manager, StorageCounters::DISABLED)
            .map_err(|err| FetchExecutionError::ConceptRead { typedb_source: err })?,
        move |result| {
            result.as_ref().is_ok_and(|(has, _count)| {
                has.attribute().type_() == attribute_type || subtypes.contains(&has.attribute().type_())
            })
        },
    );
    Ok(iter)
}

fn prepare_single_function_execution<Snapshot: ReadableSnapshot + 'static>(
    // snapshot: Arc<Snapshot>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    execution_context: &ExecutionContext<Snapshot>,
    query_context: &QueryContext,
    // parameters: Arc<ParameterRegistry>,
    functions_registry: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    variable_positions: &HashMap<Variable, VariablePosition>,
    row: MaybeOwnedRow<'_>,
    function: &ExecutableFunction,
    // TODO: verify that this doesn't need to actually return a new ExecutionContext (eg. reuses parameters?)
) -> Result<PatternExecutor, FetchExecutionError> {
    let mut args = vec![VariableValue::None; function.argument_positions.len()];
    for (var, write_pos) in &function.argument_positions {
        debug_assert!(write_pos.as_usize() < args.len());
        args[write_pos.as_usize()] = row.get(*variable_positions.get(var).unwrap()).clone().into_owned();
    }
    let args = MaybeOwnedRow::new_owned(args, row.multiplicity(), Provenance::INITIAL);

    let step_executors =
        create_executors_for_function(execution_context, &functions_registry, query_context.profile.clone(), function)
            .map_err(|err| FetchExecutionError::ConceptRead { typedb_source: err })?;
    let mut pattern_executor = PatternExecutor::new(next_executable_id(), step_executors);
    pattern_executor.prepare(FixedBatch::from(args));
    Ok(pattern_executor)
}

fn execute_object_entries(
    execution_context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    query_context: Arc<QueryContext>,
    entries: &HashMap<ParameterID, FetchSomeInstruction>,
    // snapshot: Arc<impl ReadableSnapshot + 'static>,
    // thing_manager: &ThingManager,
    // function_manager: Arc<FunctionManager>,
    // parameters: Arc<ParameterRegistry>,
    functions: Arc<ExecutableFunctionRegistry>,
    // query_profile: Arc<QueryProfile>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentMap, FetchExecutionError> {
    let mut map = HashMap::with_capacity(entries.len());
    for (id, fetch_some) in entries {
        map.insert(
            id.clone(),
            execute_fetch_some(
                execution_context,
                query_context.clone(),
                fetch_some,
                // snapshot.clone(),
                // thing_manager.clone(),
                // function_manager.clone(),
                // parameters.clone(),
                functions.clone(),
                // query_profile.clone(),
                row.as_reference(),
                interrupt.clone(),
            )?,
        );
    }
    Ok(DocumentMap::UserKeys(map))
}

fn variable_value_to_document(variable_value: VariableValue<'_>) -> Result<DocumentNode, FetchExecutionError> {
    match variable_value.into_owned() {
        VariableValue::None => Ok(DocumentNode::Leaf(DocumentLeaf::Empty)),
        VariableValue::Type(type_) => Ok(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Type(type_)))),
        VariableValue::Thing(thing) => match thing {
            Thing::Entity(_) => Err(FetchExecutionError::FetchEntities {}),
            Thing::Relation(_) => Err(FetchExecutionError::FetchRelations {}),
            Thing::Attribute(_) => Ok(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(thing)))),
        },
        VariableValue::Value(value) => Ok(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Value(value)))),
        VariableValue::ThingList(_) => {
            unimplemented_feature!(Lists)
        }
        VariableValue::ValueList(_) => {
            unimplemented_feature!(Lists)
        }
    }
}

typedb_error! {
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

        SubFetch(10, "Error executing sub fetch.", typedb_source: Box<PipelineExecutionError>),

        ConceptRead(30, "Unexpected failed to read concept.", typedb_source: Box<ConceptReadError>),
        ReadExecution(31, "Unexpected failed to execute read.", typedb_source: Box<ReadExecutionError>),
        Pipeline(32, "Pipeline error.", typedb_source: Box<PipelineError>),
    }
}
