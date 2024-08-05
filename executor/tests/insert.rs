/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use compiler::inference::annotated_functions::AnnotatedCommittedFunctions;
use concept::type_::{Ordering, OwnerAPI, PlayerAPI};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{batch::Row, insert_executor::InsertExecutor};
use ir::program::{function_signature::HashMapFunctionIndex, program::Program};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, WriteSnapshot},
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

#[test]
fn basic_has() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());
    let typeql_insert = typeql::parse_query(
        "
        insert $p isa person, has age 10;
    ",
    )
    .unwrap()
    .into_pipeline()
    .stages
    .pop()
    .unwrap()
    .into_insert();
    let block =
        ir::translation::insert::translate_insert(&HashMapFunctionIndex::empty(), &typeql_insert).unwrap().finish();
    let mut snapshot = storage.clone().open_snapshot_write();
    let annotated_program = compiler::inference::type_inference::infer_types(
        Program::new(block, vec![]),
        &snapshot,
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
    let mut executor = InsertExecutor::new(insert_plan);
    executor::insert_executor::execute(
        &mut snapshot,
        &thing_manager,
        &mut executor,
        &Row::new(vec![].as_mut_slice(), &mut 1),
    )
    .unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = storage.clone().open_snapshot_read();
        let person_type = type_manager.get_entity_type(&snapshot, &PERSON_LABEL).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_name_john =
            thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_name_john.get_owners(&snapshot, &thing_manager).count());
    }
}

#[test]
fn basic_role_player() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    setup_schema(storage.clone());
    let typeql_insert = typeql::parse_query(
        "
        insert
         $p isa person; $g isa group;
         ($p, $g) isa membership;
         (membership:member: $p, membership:group: $g) isa membership;
    ",
    )
    .unwrap()
    .into_pipeline()
    .stages
    .pop()
    .unwrap()
    .into_insert();
    let block =
        ir::translation::insert::translate_insert(&HashMapFunctionIndex::empty(), &typeql_insert).unwrap().finish();
    let mut snapshot = storage.clone().open_snapshot_write();
    let annotated_program = compiler::inference::type_inference::infer_types(
        Program::new(block, vec![]),
        &snapshot,
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
    let mut executor = InsertExecutor::new(insert_plan);
    executor::insert_executor::execute(
        &mut snapshot,
        &thing_manager,
        &mut executor,
        &Row::new(vec![].as_mut_slice(), &mut 1),
    )
    .unwrap();
    snapshot.commit().unwrap();
}
