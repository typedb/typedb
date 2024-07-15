/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use std::{borrow::Cow,
          collections::HashMap, sync::Arc};

use concept::{
    thing::{object::ObjectAPI, statistics::Statistics},
    type_::{Ordering, OwnerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use ir::{
    inference::type_inference::infer_types,
    program::{
        block::FunctionalBlock,
        function_signature::{FunctionSignatureIndex, HashMapFunctionIndex},
        program::Program,
    },
    translator::block_builder::TypeQLBuilder,
};
use storage::{
    durability_client::WALClient
    ,
    snapshot::{CommittableSnapshot, WriteSnapshot},
};
use storage::sequence_number::SequenceNumber;
use storage::snapshot::ReadSnapshot;
use traversal::executor::program_executor::ProgramExecutor;
use traversal::planner::pattern_plan::PatternPlan;
use traversal::planner::program_plan::ProgramPlan;
use crate::common::{load_managers, setup_storage};

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

#[test]
fn test_planning_traversal() {
    let (tmp_dir, storage) = setup_storage();
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL, false).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL, false).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL, false).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();

    let person = [
            thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
     thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
     thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap(),
        ];

    let age = [
            thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap(),
     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap(),
     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap(),
     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap(),
     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap(),
        ];

    let name = [ thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("John".to_string())))
        .unwrap(),
     thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Alice".to_string())))
        .unwrap(),
     thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Leila".to_string())))
        .unwrap(),
        ];

    person[0].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, age[1].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, age[2].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, name[0].clone()).unwrap();
    person[0].set_has_unordered(&mut snapshot, &thing_manager, name[1].clone()).unwrap();

    person[1].set_has_unordered(&mut snapshot, &thing_manager, age[4].clone()).unwrap();
    person[1].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    person[1].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();

    person[2].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    person[2].set_has_unordered(&mut snapshot, &thing_manager, name[2].clone()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();

    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(&storage).unwrap();

    let query = "match $person isa person, has name $name, has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();

    // IR
    let empty_function_index = FunctionSignatureIndex::new(&HashMapFunctionIndex {}, HashMap::new());
    let mut builder = TypeQLBuilder::build_match_but_dont_finish(&empty_function_index, &match_).unwrap();
    builder.conjunction_mut().and_typeql_patterns(&match_.patterns).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["person", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let program = Program::new(block, HashMap::new());
    let type_annotations = infer_types(&program, &snapshot, &type_manager).unwrap();
    let pattern_plan = PatternPlan::from_block(program.entry, &type_annotations, &statistics);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new());
    let executor = ProgramExecutor::new(program_plan, &type_annotations, &snapshot, &thing_manager).unwrap();

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);
    }
}