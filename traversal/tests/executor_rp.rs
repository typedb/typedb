/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use concept::{
    error::ConceptReadError,
    thing::object::ObjectAPI,
    type_::{Ordering, OwnerAPI, PlayerAPI},
};
use encoding::{
    graph::type_::Kind,
    value::{label::Label, value::Value, value_type::ValueType},
};
use ir::{
    inference::type_inference::infer_types,
    pattern::constraint::IsaKind,
    program::{
        block::FunctionalBlock,
        program::{CompiledSchemaFunctions, Program},
    },
};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
    MVCCStorage,
};
use traversal::{
    executor::{batch::ImmutableRow, program_executor::ProgramExecutor},
    planner::{
        pattern_plan::{Instruction, IntersectionStep, IterateBounds, PatternPlan, Step},
        program_plan::ProgramPlan,
    },
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

fn setup_database(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL, false).unwrap();
    let group_type = type_manager.create_entity_type(&mut snapshot, &GROUP_LABEL, false).unwrap();

    let membership_type = type_manager.create_relation_type(&mut snapshot, &MEMBERSHIP_LABEL, false).unwrap();
    let relates_member = membership_type
        .create_relates(&mut snapshot, &type_manager, MEMBERSHIP_MEMBER_LABEL.name().as_str(), Ordering::Unordered)
        .unwrap();
    let membership_member_type = relates_member.role();
    let relates_group = membership_type
        .create_relates(&mut snapshot, &type_manager, MEMBERSHIP_GROUP_LABEL.name().as_str(), Ordering::Unordered)
        .unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL, false).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL, false).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();

    person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_plays(&mut snapshot, &type_manager, membership_member_type.clone()).unwrap();
    group_type.set_plays(&mut snapshot, &type_manager, membership_group_type.clone()).unwrap();

    let _person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let _person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let _person_3 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();

    let _group_1 = thing_manager.create_entity(&mut snapshot, group_type.clone()).unwrap();
    let _group_2 = thing_manager.create_entity(&mut snapshot, group_type.clone()).unwrap();

    let _membership_1 = thing_manager.create_relation(&mut snapshot, membership_type.clone()).unwrap();
    let _membership_2 = thing_manager.create_relation(&mut snapshot, membership_type.clone()).unwrap();

    let mut _age_1 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap();
    let mut _age_2 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap();

    let mut _name_1 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Abby".to_string())))
        .unwrap();
    let mut _name_2 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Bobby".to_string())))
        .unwrap();

    _membership_1
        .add_player(
            &mut snapshot,
            &thing_manager,
            membership_member_type.clone(),
            _person_1.clone().into_owned_object(),
        )
        .unwrap();
    _membership_1
        .add_player(&mut snapshot, &thing_manager, membership_group_type.clone(), _group_1.clone().into_owned_object())
        .unwrap();
    _membership_2
        .add_player(
            &mut snapshot,
            &thing_manager,
            membership_member_type.clone(),
            _person_3.clone().into_owned_object(),
        )
        .unwrap();
    _membership_2
        .add_player(&mut snapshot, &thing_manager, membership_member_type.clone(), _group_2.clone().into_owned_object())
        .unwrap();

    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_1.as_reference()).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_2.as_reference()).unwrap();

    _person_2.set_has_unordered(&mut snapshot, &thing_manager, _age_1.as_reference()).unwrap();

    _person_3.set_has_unordered(&mut snapshot, &thing_manager, _name_1.as_reference()).unwrap();
    _person_3.set_has_unordered(&mut snapshot, &thing_manager, _name_2.as_reference()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
}

#[test]
fn traverse_rp_unbounded_sorted_from() {
    let (_tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $membership links (group: $group, member: $person), isa membership;
    //

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable(&"person_type").unwrap();
    let var_group_type = conjunction.get_or_declare_variable(&"group_type").unwrap();
    let var_membership_type = conjunction.get_or_declare_variable(&"membership_type").unwrap();
    let var_membership_member_type = conjunction.get_or_declare_variable(&"membership_member_type").unwrap();
    let var_membership_group_type = conjunction.get_or_declare_variable(&"membership_group_type").unwrap();

    let var_person = conjunction.get_or_declare_variable(&"person").unwrap();
    let var_group = conjunction.get_or_declare_variable(&"group").unwrap();
    let var_membership = conjunction.get_or_declare_variable(&"membership").unwrap();

    let rp_membership_person = conjunction
        .constraints_mut()
        .add_role_player(var_membership, var_person, Some(var_membership_member_type))
        .unwrap()
        .clone();
    let rp_membership_group = conjunction
        .constraints_mut()
        .add_role_player(var_membership, var_group, Some(var_membership_group_type))
        .unwrap()
        .clone();

    // add all constraints to make type inference return correct types, though we only plan Has's
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_group, var_group_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_membership, var_membership_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_group_type, GROUP_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_membership_type, MEMBERSHIP_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_member_type, MEMBERSHIP_MEMBER_LABEL.scoped_name().as_str())
        .unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_membership_group_type, MEMBERSHIP_GROUP_LABEL.scoped_name().as_str())
        .unwrap();

    let program = Program::new(block.finish(), Vec::new());

    let annotated_program = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(program, &snapshot, &type_manager, Arc::new(CompiledSchemaFunctions::empty())).unwrap()
    };

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_membership,
        vec![
            Instruction::RolePlayer(rp_membership_person.clone(), IterateBounds::None([])),
            Instruction::RolePlayer(rp_membership_group.clone(), IterateBounds::None([])),
        ],
        &vec![var_membership, var_group, var_person],
    ))];

    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan =
        ProgramPlan::new(pattern_plan, annotated_program.get_entry_annotations().clone(), HashMap::new());

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone());
        ProgramExecutor::new(program_plan, &snapshot, &thing_manager).unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> = iterator
            .map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone()))
            .collect();
        assert_eq!(rows.len(), 7);

        for row in rows {
            let r = row.unwrap();
            print!("{}", r);
            println!()
        }
    }
}

#[test]
fn traverse_has_unbounded_sorted_to_merged() {
    let (_tmp_dir, storage) = setup_storage();

    setup_database(storage.clone());

    // query:
    //   match
    //    $person has attribute $attribute;

    // IR
    let mut block = FunctionalBlock::builder();
    let mut conjunction = block.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable(&"person_type").unwrap();
    let var_attribute_type = conjunction.get_or_declare_variable(&"attr_type").unwrap();
    let var_person = conjunction.get_or_declare_variable(&"person").unwrap();
    let var_attribute = conjunction.get_or_declare_variable(&"attr").unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction
        .constraints_mut()
        .add_label(var_attribute_type, Kind::Attribute.root_label().scoped_name().as_str())
        .unwrap();
    let program = Program::new(block.finish(), Vec::new());

    let annotated_program = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(program, &snapshot, &type_manager, Arc::new(CompiledSchemaFunctions::empty())).unwrap()
    };

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_attribute,
        vec![Instruction::Has(has_attribute.clone(), IterateBounds::None([]))],
        &vec![var_person, var_attribute],
    ))];
    let pattern_plan = PatternPlan::new(steps, annotated_program.get_entry().context().clone());
    let program_plan =
        ProgramPlan::new(pattern_plan, annotated_program.get_entry_annotations().clone(), HashMap::new());

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone());
        ProgramExecutor::new(program_plan, &snapshot, &thing_manager).unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let variable_positions = executor.entry_variable_positions().clone();

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
            iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();

        // person 1 - has age 1, has age 2, has age 3, has name 1, has name 2 => 5 answers
        // person 2 - has age 1, has age 4, has age 5 => 3 answers
        // person 3 - has age 4, has name 3 => 2 answers

        assert_eq!(rows.len(), 10);

        for row in &rows {
            let r = row.as_ref().unwrap();
            print!("{}", r);
        }

        let attribute_position = variable_positions.get(&var_attribute).unwrap();
        let mut last_attribute = rows[0].as_ref().unwrap().get(*attribute_position);
        for row in &rows {
            let r = row.as_ref().unwrap();
            let attribute = r.get(*attribute_position);
            assert!(last_attribute <= attribute, "{} <= {} failed", &last_attribute, &attribute);
            last_attribute = attribute;
        }
    }
}
