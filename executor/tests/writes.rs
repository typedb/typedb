/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    vec,
};

use answer::variable_value::VariableValue;
use compiler::{
    self,
    annotation::{
        function::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        match_inference::infer_types,
    },
    VariablePosition,
};
use concept::{
    thing::{object::ObjectAPI, relation::Relation, thing_manager::ThingManager},
    type_::{object_type::ObjectType, type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    pipeline::{
        delete::DeleteStageExecutor,
        insert::InsertStageExecutor,
        stage::{ExecutionContext, StageAPI, StageIterator},
        PipelineExecutionError,
    },
    row::MaybeOwnedRow,
    write::WriteError,
    ExecutionInterrupt,
};
use ir::{pipeline::function_signature::HashMapFunctionSignatureIndex, translation::TranslationContext};
use lending_iterator::{AsHkt, AsNarrowingIterator, LendingIterator};
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, WritableSnapshot, WriteSnapshot},
    MVCCStorage,
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const GROUP_LABEL: Label = Label::new_static("group");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");
const MEMBERSHIP_GROUP_LABEL: Label = Label::new_static_scoped("group", "membership", "membership:group");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

fn setup_schema(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let group_type = type_manager.create_entity_type(&mut snapshot, &GROUP_LABEL).unwrap();

    let membership_type = type_manager.create_relation_type(&mut snapshot, &MEMBERSHIP_LABEL).unwrap();
    let relates_member = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_MEMBER_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    let membership_member_type = relates_member.role();
    let relates_group = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_GROUP_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_member_type.clone()).unwrap();
    group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_group_type.clone()).unwrap();

    snapshot.commit().unwrap();
}

struct ShimStage<Snapshot> {
    rows: Vec<Result<MaybeOwnedRow<'static>, PipelineExecutionError>>,
    context: ExecutionContext<Snapshot>,
}

impl<Snapshot> ShimStage<Snapshot> {
    fn new(rows: Vec<Vec<VariableValue<'static>>>, context: ExecutionContext<Snapshot>) -> Self {
        let rows = rows.into_iter().map(|values| Ok(MaybeOwnedRow::new_owned(values, 1))).collect();
        Self { rows, context }
    }
}

struct ShimIterator(
    AsNarrowingIterator<
        vec::IntoIter<Result<MaybeOwnedRow<'static>, PipelineExecutionError>>,
        Result<AsHkt![MaybeOwnedRow<'_>], PipelineExecutionError>,
    >,
);

impl StageIterator for ShimIterator {}

impl LendingIterator for ShimIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.0.next()
    }
}

impl<Snapshot> StageAPI<Snapshot> for ShimStage<Snapshot> {
    type OutputIterator = ShimIterator;

    fn into_iterator(
        self,
        _: ExecutionInterrupt,
    ) -> Result<(Self::OutputIterator, ExecutionContext<Snapshot>), (PipelineExecutionError, ExecutionContext<Snapshot>)>
    {
        Ok((ShimIterator(AsNarrowingIterator::new(self.rows)), self.context))
    }
}

fn execute_insert<Snapshot: WritableSnapshot + 'static>(
    snapshot: Snapshot,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    query_str: &str,
    input_row_var_names: &[&str],
    input_rows: Vec<Vec<VariableValue<'static>>>,
) -> Result<(Vec<MaybeOwnedRow<'static>>, Snapshot), WriteError> {
    let mut translation_context = TranslationContext::new();
    let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
    let block = ir::translation::writes::translate_insert(&mut translation_context, &typeql_insert).unwrap();
    let input_row_format = input_row_var_names
        .iter()
        .enumerate()
        .map(|(i, v)| (translation_context.get_variable(*v).unwrap(), VariablePosition::new(i as u32)))
        .collect::<HashMap<_, _>>();

    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
    let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
    let entry_annotations = infer_types(
        &snapshot,
        &block,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )
    .unwrap();

    let variable_registry = Arc::new(translation_context.variable_registry);

    let insert_plan = compiler::executable::insert::executable::compile(
        variable_registry,
        block.conjunction().constraints(),
        &input_row_format,
        &entry_annotations,
    )
    .unwrap();

    println!("Insert Vertex:\n{:?}", &insert_plan.concept_instructions);
    println!("Insert Edges:\n{:?}", &insert_plan.connection_instructions);

    println!("Insert output row schema: {:?}", &insert_plan.output_row_schema);

    let snapshot = Arc::new(snapshot);
    let initial = ShimStage::new(
        input_rows,
        ExecutionContext { snapshot, thing_manager, parameters: Arc::new(translation_context.parameters) },
    );
    let insert_executor = InsertStageExecutor::new(Arc::new(insert_plan), initial);
    let (output_iter, context) =
        insert_executor.into_iterator(ExecutionInterrupt::new_uninterruptible()).map_err(|(err, _)| match err {
            PipelineExecutionError::WriteError { typedb_source } => typedb_source,
            _ => unreachable!(),
        })?;
    let output_rows = output_iter
        .map_static(|res| res.map(|row| row.into_owned()))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| match err {
            PipelineExecutionError::WriteError { typedb_source } => typedb_source,
            _ => unreachable!(),
        })?;
    Ok((output_rows, Arc::into_inner(context.snapshot).unwrap()))
}

fn execute_delete<Snapshot: WritableSnapshot + 'static>(
    snapshot: Snapshot,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    mock_match_string_for_annotations: &str,
    delete_str: &str,
    input_row_var_names: &[&str],
    input_rows: Vec<Vec<VariableValue<'static>>>,
) -> Result<(Vec<MaybeOwnedRow<'static>>, Snapshot), WriteError> {
    let mut translation_context = TranslationContext::new();
    let entry_annotations = {
        let typeql_match = typeql::parse_query(mock_match_string_for_annotations)
            .unwrap()
            .into_pipeline()
            .stages
            .pop()
            .unwrap()
            .into_match();
        let block = ir::translation::match_::translate_match(
            &mut translation_context,
            &HashMapFunctionSignatureIndex::empty(),
            &typeql_match,
        )
        .unwrap()
        .finish()
        .unwrap();
        let variable_registry = &translation_context.variable_registry;
        let previous_stage_variable_annotations = &BTreeMap::new();
        let annotated_schema_functions = &IndexedAnnotatedFunctions::empty();
        let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
        infer_types(
            &snapshot,
            &block,
            variable_registry,
            &type_manager,
            previous_stage_variable_annotations,
            annotated_schema_functions,
            Some(annotated_preamble_functions),
        )
        .unwrap()
    };

    let typeql_delete = typeql::parse_query(delete_str).unwrap().into_pipeline().stages.pop().unwrap().into_delete();
    let (block, deleted_concepts) =
        ir::translation::writes::translate_delete(&mut translation_context, &typeql_delete).unwrap();
    let input_row_format = input_row_var_names
        .iter()
        .enumerate()
        .map(|(i, v)| (translation_context.get_variable(*v).unwrap(), VariablePosition::new(i as u32)))
        .collect::<HashMap<_, _>>();

    let delete_plan = compiler::executable::delete::executable::compile(
        &input_row_format,
        &entry_annotations,
        block.conjunction().constraints(),
        &deleted_concepts,
    )
    .unwrap();

    let snapshot = Arc::new(snapshot);
    let initial = ShimStage::new(
        input_rows,
        ExecutionContext { snapshot, thing_manager, parameters: Arc::new(translation_context.parameters) },
    );
    let delete_executor = DeleteStageExecutor::new(Arc::new(delete_plan), initial);
    let (output_iter, context) =
        delete_executor.into_iterator(ExecutionInterrupt::new_uninterruptible()).map_err(|(err, _)| match err {
            PipelineExecutionError::WriteError { typedb_source } => typedb_source,
            _ => unreachable!(),
        })?;
    let output_rows = output_iter
        .map_static(|res| res.map(|row| row.into_owned()))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| match err {
            PipelineExecutionError::WriteError { typedb_source } => typedb_source,
            _ => unreachable!(),
        })?;
    Ok((output_rows, Arc::into_inner(context.snapshot).unwrap()))
}

#[test]
fn has() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    setup_schema(storage.clone());
    let snapshot = storage.clone().open_snapshot_write();
    let (_, snapshot) = execute_insert(
        snapshot,
        type_manager.clone(),
        thing_manager.clone(),
        "insert $p isa person, has age 10;",
        &[],
        vec![vec![]],
    )
    .unwrap();
    snapshot.commit().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let age_type = type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
    let attr_age_10 = thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
    assert_eq!(1, attr_age_10.get_owners(&snapshot, &thing_manager).count());
    snapshot.close_resources()
}

#[test]
fn test() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    setup_schema(storage.clone());

    let snapshot = storage.clone().open_snapshot_write();
    let query_str = "
        insert
         $p isa person; $g isa group;
         (member: $p, group: $g) isa membership;
    ";
    let (_, snapshot) = execute_insert(snapshot, type_manager, thing_manager, query_str, &[], vec![vec![]]).unwrap();
    snapshot.commit().unwrap();
}

#[test]
fn relation() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    setup_schema(storage.clone());

    let snapshot = storage.clone().open_snapshot_write();
    let query_str = "
        insert
         $p isa person; $g isa group;
         (member: $p, group: $g) isa membership;
    ";
    let (_, snapshot) =
        execute_insert(snapshot, type_manager.clone(), thing_manager.clone(), query_str, &[], vec![vec![]]).unwrap();
    snapshot.commit().unwrap();

    let snapshot = storage.open_snapshot_read();
    let person_type = type_manager.get_entity_type(&snapshot, &PERSON_LABEL).unwrap().unwrap();
    let group_type = type_manager.get_entity_type(&snapshot, &GROUP_LABEL).unwrap().unwrap();
    let membership_type = type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
    let member_role = membership_type
        .get_relates_role_name(&snapshot, &type_manager, MEMBERSHIP_MEMBER_LABEL.name.as_str())
        .unwrap()
        .unwrap()
        .role();
    let group_role = membership_type
        .get_relates_role_name(&snapshot, &type_manager, MEMBERSHIP_GROUP_LABEL.name.as_str())
        .unwrap()
        .unwrap()
        .role();
    let relations: Vec<Relation<'_>> = thing_manager
        .get_relations_in(&snapshot, membership_type.clone())
        .map_static(|item| item.map(|relation| relation.clone().into_owned()))
        .try_collect()
        .unwrap();
    assert_eq!(1, relations.len());
    let role_players = relations[0]
        .get_players(&snapshot, &thing_manager)
        .map_static(|item| {
            item.map(|(roleplayer, _)| (roleplayer.player().into_owned(), roleplayer.role_type().into_owned()))
        })
        .try_collect::<Vec<_>, _>()
        .unwrap();
    assert!(role_players.iter().any(|(player, role)| {
        (player.type_().clone(), role.clone()) == (ObjectType::Entity(person_type.clone()), member_role.clone())
    }));
    assert!(role_players.iter().any(|(player, role)| {
        (player.type_().clone(), role.clone()) == (ObjectType::Entity(group_type.clone()), group_role.clone())
    }));
    snapshot.close_resources();
}

#[test]
fn relation_with_inferred_roles() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    setup_schema(storage.clone());

    let snapshot = storage.clone().open_snapshot_write();
    let query_str = "
        insert
         $p isa person; $g isa group;
         ($p, $g) isa membership;
    ";
    let (_, snapshot) =
        execute_insert(snapshot, type_manager.clone(), thing_manager.clone(), query_str, &[], vec![vec![]]).unwrap();
    snapshot.commit().unwrap();

    let snapshot = storage.open_snapshot_read();
    let person_type = type_manager.get_entity_type(&snapshot, &PERSON_LABEL).unwrap().unwrap();
    let group_type = type_manager.get_entity_type(&snapshot, &GROUP_LABEL).unwrap().unwrap();
    let membership_type = type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
    let member_role = membership_type
        .get_relates_role_name(&snapshot, &type_manager, MEMBERSHIP_MEMBER_LABEL.name.as_str())
        .unwrap()
        .unwrap()
        .role();
    let group_role = membership_type
        .get_relates_role_name(&snapshot, &type_manager, MEMBERSHIP_GROUP_LABEL.name.as_str())
        .unwrap()
        .unwrap()
        .role();
    let relations: Vec<Relation<'_>> = thing_manager
        .get_relations_in(&snapshot, membership_type.clone())
        .map_static(|item| item.map(|relation| relation.clone().into_owned()))
        .try_collect()
        .unwrap();
    assert_eq!(1, relations.len());
    let role_players = relations[0]
        .get_players(&snapshot, &thing_manager)
        .map_static(|item| {
            item.map(|(roleplayer, _)| (roleplayer.player().into_owned(), roleplayer.role_type().into_owned()))
        })
        .try_collect::<Vec<_>, _>()
        .unwrap();
    assert!(role_players.iter().any(|(player, role)| {
        (player.type_().clone(), role.clone()) == (ObjectType::Entity(person_type.clone()), member_role.clone())
    }));
    assert!(role_players.iter().any(|(player, role)| {
        (player.type_().clone(), role.clone()) == (ObjectType::Entity(group_type.clone()), group_role.clone())
    }));
    snapshot.close_resources();
}

#[test]
fn test_has_with_input_rows() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    setup_schema(storage.clone());
    let snapshot = storage.clone().open_snapshot_write();
    let (inserted_rows, snapshot) = execute_insert(
        snapshot,
        type_manager.clone(),
        thing_manager.clone(),
        "insert $p isa person;",
        &[],
        vec![vec![]],
    )
    .unwrap();
    let p10 = inserted_rows[0][0].clone();
    let (inserted_rows, snapshot) = execute_insert(
        snapshot,
        type_manager.clone(),
        thing_manager.clone(),
        "insert $p has age 10;",
        &["p"],
        vec![vec![p10.clone()]],
    )
    .unwrap();
    let a10 = inserted_rows[0][1].clone();
    snapshot.commit().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let age_type = type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
    let age_of_p10 = p10
        .as_thing()
        .as_object()
        .get_has_type_unordered(&snapshot, &thing_manager, age_type.clone())
        .unwrap()
        .map_static(|result| result.unwrap().0.clone().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(a10.as_thing().as_attribute(), age_of_p10[0]);
    let owner_of_a10 = a10
        .as_thing()
        .as_attribute()
        .get_owners(&snapshot, &thing_manager)
        .map_static(|result| result.unwrap().0.clone().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(p10.as_thing().as_object(), owner_of_a10[0]);
    snapshot.close_resources();
}

#[test]
fn delete_has() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    setup_schema(storage.clone());
    let snapshot = storage.clone().open_snapshot_write();
    let (inserted_rows, snapshot) = execute_insert(
        snapshot,
        type_manager.clone(),
        thing_manager.clone(),
        "insert $p isa person;",
        &[],
        vec![vec![]],
    )
    .unwrap();
    let p10 = inserted_rows[0][0].clone();
    let (inserted_rows, snapshot) = execute_insert(
        snapshot,
        type_manager.clone(),
        thing_manager.clone(),
        "insert $p has age 10;",
        &["p"],
        vec![vec![p10.clone()]],
    )
    .unwrap();
    let a10 = inserted_rows[0][1].clone().into_owned();
    snapshot.commit().unwrap();

    let snapshot = storage.clone().open_snapshot_write();
    assert_eq!(1, p10.as_thing().as_object().get_has_unordered(&snapshot, &thing_manager).count());
    let (_, snapshot) = execute_delete(
        snapshot,
        type_manager.clone(),
        thing_manager.clone(),
        "match $p isa person; $a isa age;",
        "delete has $a of $p;",
        &["p", "a"],
        vec![vec![p10.clone(), a10.clone()]],
    )
    .unwrap();
    snapshot.commit().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    assert_eq!(0, p10.as_thing().as_object().get_has_unordered(&snapshot, &thing_manager).count());
    snapshot.close_resources()
}
