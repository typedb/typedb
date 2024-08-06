/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::inference::annotated_functions::AnnotatedCommittedFunctions;
use concept::{
    error::ConceptReadError,
    thing::{object::Object, relation::Relation, thing_manager::ThingManager},
    type_::{object_type::ObjectType, role_type::RoleType, type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    batch::Row,
    insert_executor::{InsertError, InsertExecutor},
};
use ir::program::{function_signature::HashMapFunctionIndex, program::Program};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadableSnapshot, WritableSnapshot, WriteSnapshot},
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
        .create_relates(&mut snapshot, &type_manager, MEMBERSHIP_MEMBER_LABEL.name().as_str(), Ordering::Unordered)
        .unwrap();
    let membership_member_type = relates_member.role();
    let relates_group = membership_type
        .create_relates(&mut snapshot, &type_manager, MEMBERSHIP_GROUP_LABEL.name().as_str(), Ordering::Unordered)
        .unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();

    person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_plays(&mut snapshot, &type_manager, membership_member_type.clone()).unwrap();
    group_type.set_plays(&mut snapshot, &type_manager, membership_group_type.clone()).unwrap();

    snapshot.commit().unwrap();
}

fn execute_insert(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    query_str: &str,
) -> Result<Vec<VariableValue<'static>>, InsertError> {
    let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
    let block =
        ir::translation::insert::translate_insert(&HashMapFunctionIndex::empty(), &typeql_insert).unwrap().finish();
    let annotated_program = compiler::inference::type_inference::infer_types(
        Program::new(block, vec![]),
        snapshot,
        &type_manager,
        Arc::new(AnnotatedCommittedFunctions::new(vec![].into_boxed_slice(), vec![].into_boxed_slice())),
    )
    .unwrap();
    let insert_plan = compiler::planner::insert_planner::build_insert_plan(
        &HashMap::new(),
        annotated_program.get_entry_annotations(),
        annotated_program.get_entry().conjunction().constraints(),
    )
    .unwrap();

    println!("{:?}", &insert_plan.instructions);
    insert_plan.debug_info.iter().for_each(|(k, v)| {
        println!("{:?} -> {:?}", k, annotated_program.get_entry().context().get_variables_named().get(v))
    });
    let mut output_vec = (0..insert_plan.n_created_concepts).map(|_| VariableValue::Empty).collect::<Vec<_>>();
    let mut output_multiplicity = 0;
    let output = Row::new(&mut output_vec, &mut output_multiplicity);
    let mut executor = InsertExecutor::new(insert_plan);
    executor::insert_executor::execute(
        snapshot,
        &thing_manager,
        &mut executor,
        &Row::new(vec![].as_mut_slice(), &mut 1),
        output,
    )?;
    Ok(output_vec)
}

#[test]
fn has() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());
    let mut snapshot = storage.clone().open_snapshot_write();
    execute_insert(&mut snapshot, &type_manager, &thing_manager, "insert $p isa person, has age 10;").unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = storage.clone().open_snapshot_read();
        let person_type = type_manager.get_entity_type(&snapshot, &PERSON_LABEL).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_name_john =
            thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_name_john.get_owners(&snapshot, &thing_manager).count());
        snapshot.close_resources()
    }
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
    execute_insert(&mut snapshot, &type_manager, &thing_manager, query_str).unwrap();
    snapshot.commit().unwrap();
    // read back
    {
        let snapshot = storage.open_snapshot_read();
        let person_type = type_manager.get_entity_type(&snapshot, &PERSON_LABEL).unwrap().unwrap();
        let group_type = type_manager.get_entity_type(&snapshot, &GROUP_LABEL).unwrap().unwrap();
        let membership_type = type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
        let member_role = membership_type
            .get_relates_of_role(&snapshot, &type_manager, MEMBERSHIP_MEMBER_LABEL.name.as_str())
            .unwrap()
            .unwrap()
            .role();
        let group_role = membership_type
            .get_relates_of_role(&snapshot, &type_manager, MEMBERSHIP_GROUP_LABEL.name.as_str())
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
    execute_insert(&mut snapshot, &type_manager, &thing_manager, query_str).unwrap();
    snapshot.commit().unwrap();
    // read back
    {
        let snapshot = storage.open_snapshot_read();
        let person_type = type_manager.get_entity_type(&snapshot, &PERSON_LABEL).unwrap().unwrap();
        let group_type = type_manager.get_entity_type(&snapshot, &GROUP_LABEL).unwrap().unwrap();
        let membership_type = type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
        let member_role = membership_type
            .get_relates_of_role(&snapshot, &type_manager, MEMBERSHIP_MEMBER_LABEL.name.as_str())
            .unwrap()
            .unwrap()
            .role();
        let group_role = membership_type
            .get_relates_of_role(&snapshot, &type_manager, MEMBERSHIP_GROUP_LABEL.name.as_str())
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
