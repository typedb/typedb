/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use compiler::{
    inference::{annotated_functions::AnnotatedCommittedFunctions, type_inference::infer_types},
    planner::{pattern_plan::PatternPlan, program_plan::ProgramPlan},
};
use concept::{
    thing::{object::ObjectAPI, statistics::Statistics},
    type_::{annotation::AnnotationCardinality, owns::OwnsAnnotation, Ordering, OwnerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::program_executor::ProgramExecutor;
use ir::{
    program::{function_signature::HashMapFunctionIndex, program::Program},
    translation::match_::translate_match,
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::{sequence_number::SequenceNumber, snapshot::CommittableSnapshot};

use crate::common::{load_managers, setup_storage};

mod common;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

#[test]
fn test_planning_traversal() {
    let (_tmp_dir, storage) = setup_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    const CARDINALITY_ANY: OwnsAnnotation = OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None));

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();

    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();

    let person_owns_age =
        person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_owns_age.set_annotation(&mut snapshot, &type_manager, CARDINALITY_ANY).unwrap();

    let person_owns_name =
        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_owns_name.set_annotation(&mut snapshot, &type_manager, CARDINALITY_ANY).unwrap();

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

    let name = [
        thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("John"))).unwrap(),
        thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Alice")))
            .unwrap(),
        thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Leila")))
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
    let empty_function_index = HashMapFunctionIndex::empty();
    let builder = translate_match(&empty_function_index, &match_).unwrap();
    // builder.add_limit(3);
    // builder.add_filter(vec!["person", "age"]).unwrap();
    let block = builder.finish();

    // Executor
    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let program = Program::new(block, Vec::new());
    let annotated_program =
        infer_types(program, &snapshot, &type_manager, Arc::new(AnnotatedCommittedFunctions::empty())).unwrap();
    let pattern_plan = PatternPlan::from_block(&annotated_program, &statistics);
    let program_plan = ProgramPlan::new(pattern_plan, annotated_program.entry_annotations().clone(), HashMap::new());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    {
        let snapshot = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows = iterator
            .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
            .into_iter()
            .try_collect::<_, Vec<_>, _>()
            .unwrap();

        assert_eq!(rows.len(), 7);

        for row in rows {
            for value in row {
                print!("{}, ", value);
            }
            println!()
        }
    }
}
