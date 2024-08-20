/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use std::{collections::HashMap, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::{
    match_::inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
    VariablePosition,
};
use concept::{
    thing::{object::ObjectAPI, relation::Relation, thing_manager::ThingManager},
    type_::{object_type::ObjectType, type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    batch::Row,
    pipeline::PipelineStageAPI,
    write::{delete::DeleteExecutor, insert::InsertExecutor, WriteError},
};
use ir::{program::function_signature::HashMapFunctionSignatureIndex, translation::TranslationContext};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, WritableSnapshot, WriteSnapshot},
    MVCCStorage,
};

use crate::common::{load_managers, setup_storage};

mod common;

const PERSON_LABEL: Label = Label::new_static("person");
const GROUP_LABEL: Label = Label::new_static("group");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");
const MEMBERSHIP_GROUP_LABEL: Label = Label::new_static_scoped("group", "membership", "membership:group");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

fn setup_schema(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

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
            None,
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
            None,
        )
        .unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone()).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone()).unwrap();
    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_member_type.clone()).unwrap();
    group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_group_type.clone()).unwrap();

    snapshot.commit().unwrap();
}

fn execute_insert(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    query_str: &str,
    input_row_var_names: &[&str],
    input_rows: Vec<Vec<VariableValue<'static>>>,
) -> Result<Vec<Vec<VariableValue<'static>>>, WriteError> {
    let mut translation_context = TranslationContext::new();
    let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
    let block = ir::translation::writes::translate_insert(&mut translation_context, &typeql_insert).unwrap();
    let input_row_format = input_row_var_names
        .iter()
        .enumerate()
        .map(|(i, v)| (translation_context.visible_variables.get(*v).unwrap().clone(), VariablePosition::new(i as u32)))
        .collect::<HashMap<_, _>>();

    let (entry_annotations, _) = infer_types(
        &block,
        vec![],
        snapshot,
        type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let mut insert_plan =
        compiler::insert::program::compile(block.conjunction().constraints(), &input_row_format, &entry_annotations)
            .unwrap();

    println!("Insert Vertex:\n{:?}", &insert_plan.concepts);
    println!("Insert Edges:\n{:?}", &insert_plan.connections);
    insert_plan.debug_info.iter().for_each(|(k, v)| {
        println!("{:?} -> {:?}", k, translation_context.variable_registry.get_variables_named().get(v))
    });

    // TODO: Replace with accumulator
    let mut output_rows = Vec::with_capacity(input_rows.len());
    println!("Insert output row schema: {:?}", &insert_plan.output_row_schema);
    let output_width = insert_plan.output_row_schema.len();
    let mut insert_executor = InsertExecutor::new(insert_plan);
    for mut input_row in input_rows {
        let mut output_multiplicity = 1;
        output_rows.push(
            (0..output_width)
                .map(|i| input_row.get(i).map_or_else(|| VariableValue::Empty, |existing| existing.clone()))
                .collect::<Vec<_>>(),
        );
        // Copy input row in
        insert_executor.execute_insert(
            snapshot,
            &thing_manager,
            &mut Row::new(output_rows.last_mut().unwrap(), &mut output_multiplicity),
        )?;
    }
    Ok(output_rows)
}

fn execute_delete(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    mock_match_string_for_annotations: &str,
    delete_str: &str,
    input_row_var_names: &[&str],
    input_rows: Vec<Vec<VariableValue<'static>>>,
) -> Result<Vec<Vec<VariableValue<'static>>>, WriteError> {
    let mut translation_context = TranslationContext::new();
    let (entry_annotations, _) = {
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
        .finish();
        compiler::match_::inference::type_inference::infer_types(
            &block,
            vec![],
            snapshot,
            &type_manager,
            &IndexedAnnotatedFunctions::empty(),
            &translation_context.variable_registry,
        )
        .unwrap()
    };

    let typeql_delete = typeql::parse_query(delete_str).unwrap().into_pipeline().stages.pop().unwrap().into_delete();
    let (block, deleted_concepts) =
        ir::translation::writes::translate_delete(&mut translation_context, &typeql_delete).unwrap();
    let input_row_format = input_row_var_names
        .iter()
        .enumerate()
        .map(|(i, v)| (translation_context.visible_variables.get(*v).unwrap().clone(), VariablePosition::new(i as u32)))
        .collect::<HashMap<_, _>>();

    let delete_plan = compiler::delete::program::compile(
        &input_row_format,
        &entry_annotations,
        block.conjunction().constraints(),
        &deleted_concepts,
    )
    .unwrap();
    let delete_executor = DeleteExecutor::new(delete_plan);
    let mut output_rows = Vec::with_capacity(input_rows.len());
    for mut input_row in input_rows {
        let mut output_vec = Vec::with_capacity(input_row.len());
        output_vec.extend_from_slice(input_row.as_mut_slice()); // copy from input
        let mut output_multiplicity = 1;
        let mut output = Row::new(&mut output_vec, &mut output_multiplicity);
        delete_executor.execute_delete(snapshot, thing_manager, &mut output);
        output_rows.push(output_vec);
    }
    println!("{:?}", &delete_executor.plan().output_row_schema);
    Ok(output_rows)
}

#[test]
fn has() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());
    let mut snapshot = storage.clone().open_snapshot_write();
    execute_insert(
        &mut snapshot,
        &type_manager,
        &thing_manager,
        "insert $p isa person, has age 10;",
        &vec![],
        vec![vec![]],
    )
    .unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = storage.clone().open_snapshot_read();
        let age_type = type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &thing_manager).count());
        snapshot.close_resources()
    }
}

#[test]
fn test() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());

    let mut snapshot = storage.clone().open_snapshot_write();
    let query_str = "
        insert
         $p isa person; $g isa group;
         (member: $p, group: $g) isa membership;
    ";
    execute_insert(&mut snapshot, &type_manager, &thing_manager, query_str, &vec![], vec![vec![]]).unwrap();
    snapshot.commit().unwrap();
}

#[test]
fn relation() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());

    let mut snapshot = storage.clone().open_snapshot_write();
    let query_str = "
        insert
         $p isa person; $g isa group;
         (member: $p, group: $g) isa membership;
    ";
    execute_insert(&mut snapshot, &type_manager, &thing_manager, query_str, &vec![], vec![vec![]]).unwrap();
    snapshot.commit().unwrap();
    // read back
    {
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
}

#[test]
fn relation_with_inferred_roles() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());

    let mut snapshot = storage.clone().open_snapshot_write();
    let query_str = "
        insert
         $p isa person; $g isa group;
         ($p, $g) isa membership;
    ";
    execute_insert(&mut snapshot, &type_manager, &thing_manager, query_str, &vec![], vec![vec![]]).unwrap();
    snapshot.commit().unwrap();
    // read back
    {
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
}

#[test]
fn test_has_with_input_rows() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());
    let mut snapshot = storage.clone().open_snapshot_write();
    let inserted_rows =
        execute_insert(&mut snapshot, &type_manager, &thing_manager, "insert $p isa person;", &vec![], vec![vec![]])
            .unwrap();
    let p10 = inserted_rows[0][0].clone().into_owned();
    let inserted_rows = execute_insert(
        &mut snapshot,
        &type_manager,
        &thing_manager,
        "insert $p has age 10;",
        &vec!["p"],
        vec![vec![p10.clone()]],
    )
    .unwrap();
    let a10 = inserted_rows[0][1].clone();
    snapshot.commit().unwrap();

    {
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
    };
}

#[test]
fn delete_has() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());
    let mut snapshot = storage.clone().open_snapshot_write();
    let inserted_rows =
        execute_insert(&mut snapshot, &type_manager, &thing_manager, "insert $p isa person;", &vec![], vec![vec![]])
            .unwrap();
    let p10 = inserted_rows[0][0].clone().into_owned();
    let inserted_rows = execute_insert(
        &mut snapshot,
        &type_manager,
        &thing_manager,
        "insert $p has age 10;",
        &vec!["p"],
        vec![vec![p10.clone()]],
    )
    .unwrap();
    let a10 = inserted_rows[0][1].clone().into_owned();
    snapshot.commit().unwrap();

    let mut snapshot = storage.clone().open_snapshot_write();
    assert_eq!(1, p10.as_thing().as_object().get_has_unordered(&snapshot, &thing_manager).count());
    execute_delete(
        &mut snapshot,
        &type_manager,
        &thing_manager,
        "match $p isa person; $a isa age;",
        "delete has $a of $p;",
        &vec!["p", "a"],
        vec![vec![p10.clone(), a10.clone()]],
    )
    .unwrap();
    snapshot.commit().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    assert_eq!(0, p10.as_thing().as_object().get_has_unordered(&snapshot, &thing_manager).count());
    snapshot.close_resources()
}
