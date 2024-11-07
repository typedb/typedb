/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use answer::{variable_value::VariableValue, Concept, Thing};
use compiler::{
    executable::{
        fetch::executable::{
            ExecutableFetch, ExecutableFetchListSubFetch, FetchObjectInstruction, FetchSomeInstruction,
        },
        match_::planner::function_plan::ExecutableFunctionRegistry,
    },
    VariablePosition,
};
use concept::{
    error::ConceptReadError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{attribute_type::AttributeType, Capability, OwnerAPI, TypeAPI},
};
use encoding::value::label::Label;
use error::typedb_error;
use ir::{pattern::ParameterID, pipeline::ParameterRegistry};
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    document::{ConceptDocument, DocumentLeaf, DocumentList, DocumentMap, DocumentNode},
    pipeline::{
        pipeline::Pipeline,
        stage::{ExecutionContext, StageAPI},
        PipelineExecutionError,
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub struct FetchStageExecutor<Snapshot: ReadableSnapshot> {
    executable: Arc<ExecutableFetch>,
    _phantom: PhantomData<Snapshot>,
}

impl<Snapshot: ReadableSnapshot + 'static> FetchStageExecutor<Snapshot> {
    pub(crate) fn new(executable: Arc<ExecutableFetch>) -> Self {
        Self { executable, _phantom: PhantomData::default() }
    }

    pub(crate) fn into_iterator<PreviousStage: StageAPI<Snapshot>>(
        self,
        previous_iterator: PreviousStage::OutputIterator,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> (impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>, ExecutionContext<Snapshot>) {
        let ExecutionContext { snapshot, thing_manager, parameters } = context.clone();
        let executable = self.executable;
        let documents_iterator = previous_iterator
            .map_static(move |row_result| match row_result {
                Ok(row) => execute_fetch(
                    &executable,
                    snapshot.clone(),
                    thing_manager.clone(),
                    parameters.clone(),
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
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<ConceptDocument, FetchExecutionError> {
    let node = execute_object(&fetch.object_instruction, snapshot, thing_manager, parameters, row, interrupt)?;
    Ok(ConceptDocument { root: node })
}

fn execute_fetch_some(
    fetch_some: &FetchSomeInstruction,
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentNode, FetchExecutionError> {
    match fetch_some {
        FetchSomeInstruction::SingleVar(position) => {
            variable_value_to_document(row.get(*position).as_reference())
        }
        FetchSomeInstruction::SingleAttribute(position, attribute_type) => {
            let variable_value = row.get(*position).as_reference();
            match variable_value {
                VariableValue::Empty => {
                    Ok(DocumentNode::Leaf(DocumentLeaf::Empty))
                },
                VariableValue::Thing(Thing::Entity(entity)) => {
                    execute_attribute_single(entity, attribute_type.clone(), snapshot, thing_manager)
                        .map(|leaf| DocumentNode::Leaf(leaf))
                }
                VariableValue::Thing(Thing::Relation(relation)) => {
                    execute_attribute_single(relation, attribute_type.clone(), snapshot, thing_manager)
                        .map(|leaf| DocumentNode::Leaf(leaf))
                }
                VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
                VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
                VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
                VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
            }
        }
        FetchSomeInstruction::SingleFunction(_function) => {
            Err(FetchExecutionError::Unimplemented {
                description: "Fetch expressions and match-return subqueries are not available yet (try a match-fetch subquery instead?).".to_owned()
            })
        }
        FetchSomeInstruction::Object(object) => {
            execute_object(object, snapshot, thing_manager, parameters, row, interrupt)
        }
        FetchSomeInstruction::ListFunction(_function) => {
            Err(FetchExecutionError::Unimplemented {
                description: "Fetch expressions and match-return subqueries are not available yet (try a match-fetch subquery instead?).".to_owned()
            })
        }
        FetchSomeInstruction::ListSubFetch(ExecutableFetchListSubFetch { input_position_mapping, variable_registry, stages, fetch }) => {
            let pipeline = if input_position_mapping.is_empty() {
                Pipeline::build_read_pipeline(
                    snapshot,
                    thing_manager,
                    &variable_registry,
                    Arc::new(ExecutableFunctionRegistry::TODO__empty()), // TODO
                    &**stages,
                    Some(fetch.clone()),
                    parameters,
                    None
                )
            } else {
                let max_position = input_position_mapping.values().max().map(|pos| pos.as_usize()).unwrap();
                let mut initial_row = vec![VariableValue::Empty; max_position + 1];
                input_position_mapping.iter().for_each(|(parent_row_position, local_row_position)| {
                    initial_row[local_row_position.as_usize()] = row[parent_row_position.as_usize()].as_reference().into_owned();
                });
                let initial_row = MaybeOwnedRow::new_owned(initial_row, row.multiplicity());
                Pipeline::build_read_pipeline(
                    snapshot,
                    thing_manager,
                    &variable_registry,
                    Arc::new(ExecutableFunctionRegistry::TODO__empty()),
                    &**stages,
                    Some(fetch.clone()),
                    parameters,
                    Some(initial_row)
                )
            };

            let (iterator, _context) = pipeline.into_documents_iterator(interrupt)
                .map_err(|(err, _context)| FetchExecutionError::SubFetch { typedb_source: err })?;

            let mut nodes = Vec::new();
            for result in iterator {
                nodes.push(
                    result.map_err(|err| FetchExecutionError::SubFetch { typedb_source: err })?.root
                );
            }
            Ok(DocumentNode::List(DocumentList::new_from(nodes)))
        }
        FetchSomeInstruction::ListAttributesAsList(position, attribute_type) => {
            let variable_value = row.get(*position).as_reference();
            match variable_value {
                VariableValue::Empty => {
                    Ok(DocumentNode::Leaf(DocumentLeaf::Empty))
                },
                VariableValue::Thing(Thing::Entity(entity)) => {
                    execute_attributes_list(entity, attribute_type.clone(), snapshot, thing_manager)
                        .map(|list| DocumentNode::List(list))
                }
                VariableValue::Thing(Thing::Relation(relation)) => {
                    execute_attributes_list(relation, attribute_type.clone(), snapshot, thing_manager)
                        .map(|list| DocumentNode::List(list))
                }
                VariableValue::Thing(Thing::Attribute(_)) => Err(FetchExecutionError::FetchAttributesOfAttribute {}),
                VariableValue::Type(_) => Err(FetchExecutionError::FetchAttributesOfType {}),
                VariableValue::Value(_) => Err(FetchExecutionError::FetchAttributesOfValue {}),
                VariableValue::ThingList(_) | VariableValue::ValueList(_) => Err(FetchExecutionError::FetchAttributesOfList {}),
            }
        }
        FetchSomeInstruction::ListAttributesFromList(_, _) => {
            Err(FetchExecutionError::Unimplemented { description: "List attributes are not available yet.".to_string() })
        }
    }
}

fn execute_object(
    fetch_object: &FetchObjectInstruction,
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
    row: MaybeOwnedRow<'_>,
    interrupt: ExecutionInterrupt,
) -> Result<DocumentNode, FetchExecutionError> {
    match fetch_object {
        FetchObjectInstruction::Entries(entries) => {
            let object = execute_object_entries(entries, snapshot, thing_manager, parameters, row, interrupt)?;
            Ok(DocumentNode::Map(object))
        }
        FetchObjectInstruction::Attributes(position) => {
            execute_object_attributes(*position, snapshot, thing_manager, row)
        }
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

        let is_scalar = object
            .type_()
            .is_owned_attribute_type_scalar(snapshot.as_ref(), thing_manager.type_manager(), attribute_type)
            .map_err(|err| FetchExecutionError::ConceptRead { source: err })?;
        if is_scalar {
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
    attribute_type: AttributeType<'static>,
    snapshot: Arc<impl ReadableSnapshot>,
    thing_manager: Arc<ThingManager>,
) -> Result<DocumentLeaf, FetchExecutionError> {
    let mut iter = object.get_has_type_unordered(snapshot.as_ref(), thing_manager.as_ref(), attribute_type);
    let attribute = iter.next();
    match attribute {
        None => Ok(DocumentLeaf::Empty),
        Some(Err(err)) => Err(FetchExecutionError::ConceptRead { source: err }),
        Some(Ok((attribute, count))) => {
            debug_assert!(count <= 1);
            Ok(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(attribute.into_owned()))))
        }
    }
}

fn execute_attributes_list<'a>(
    object: impl ObjectAPI<'a>,
    attribute_type: AttributeType<'static>,
    snapshot: Arc<impl ReadableSnapshot>,
    thing_manager: Arc<ThingManager>,
) -> Result<DocumentList, FetchExecutionError> {
    let mut list = DocumentList::new();
    let mut iter = object.get_has_type_unordered(snapshot.as_ref(), thing_manager.as_ref(), attribute_type);
    while let Some(result) = iter.next() {
        match result {
            Ok((attribute, count)) => {
                for _ in 0..count {
                    list.list.push(DocumentNode::Leaf(DocumentLeaf::Concept(Concept::Thing(Thing::Attribute(
                        attribute.as_reference().into_owned(),
                    )))));
                }
            }
            Err(err) => return Err(FetchExecutionError::ConceptRead { source: err }),
        }
    }
    Ok(list)
}

fn execute_object_entries(
    entries: &HashMap<ParameterID, FetchSomeInstruction>,
    snapshot: Arc<impl ReadableSnapshot + 'static>,
    thing_manager: Arc<ThingManager>,
    parameters: Arc<ParameterRegistry>,
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
        Unimplemented(0, "Unimplemented: {description}.", description: String),

        FetchAttributesOfType(1, "Received unexpected Type instead of Entity or Relation while fetching owned attributes."),
        FetchAttributesOfAttribute(2, "Received unexpected Attribute instead of Entity or Relation while fetching owned attributes."),
        FetchAttributesOfValue(3, "Received unexpected Value instead of Entity or Relation while fetching owned attributes."),
        FetchAttributesOfList(4, "Received unexpected List instead of Entity or Relation while fetching owned attributes."),

        FetchEntities(5, "Fetching entities is not supported, use '$var.*' or '$var.<attribute type>' to fetch attributes instead."),
        FetchRelations(6, "Fetching relations is not supported, use '$var.*' or '$var.<attribute type>' to fetch attributes instead."),

        SubFetch(10, "Error executing sub fetch.", ( typedb_source : Box<PipelineExecutionError>)),

        ConceptRead(30, "Unexpected failed to read concept.", ( source: Box<ConceptReadError>)),
    }
);
