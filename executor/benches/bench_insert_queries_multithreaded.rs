/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{array, collections::HashMap, sync::{Arc, OnceLock}, thread};
use std::sync::RwLock;
use std::thread::{JoinHandle, sleep};
use std::time::{Duration, Instant};

use answer::variable_value::VariableValue;
use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use concept::{
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI},
};
use encoding::value::{label::Label, value_type::ValueType};

use compiler::match_::inference::type_inference::infer_types;
use compiler::VariablePosition;
use executor::row::Row;
use executor::write::insert::InsertExecutor;
use executor::write::WriteError;
use ir::translation::TranslationContext;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, WritableSnapshot},
    MVCCStorage,
};
use test_utils::init_logging;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

static PERSON_LABEL: OnceLock<Label> = OnceLock::new();
static GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_MEMBER_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static AGE_LABEL: OnceLock<Label> = OnceLock::new();
static NAME_LABEL: OnceLock<Label> = OnceLock::new();
fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();
    let person_type = type_manager.create_entity_type(&mut snapshot, PERSON_LABEL.get().unwrap()).unwrap();
    let group_type = type_manager.create_entity_type(&mut snapshot, GROUP_LABEL.get().unwrap()).unwrap();

    let membership_type = type_manager.create_relation_type(&mut snapshot, MEMBERSHIP_LABEL.get().unwrap()).unwrap();
    let relates_member = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_MEMBER_LABEL.get().unwrap().name().as_str(),
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
            MEMBERSHIP_GROUP_LABEL.get().unwrap().name().as_str(),
            Ordering::Unordered,
            None,
        )
        .unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, AGE_LABEL.get().unwrap()).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, NAME_LABEL.get().unwrap()).unwrap();
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
    input_row_var_names: &Vec<&str>,
    input_rows: Vec<Vec<VariableValue<'static>>>,
) -> Result<Vec<Vec<VariableValue<'static>>>, WriteError> {
    let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
    let mut translation_context = TranslationContext::new();
    let block = ir::translation::writes::translate_insert(&mut translation_context, &typeql_insert).unwrap();
    let input_row_format = input_row_var_names
        .iter()
        .enumerate()
        .map(|(i, v)| (*translation_context.visible_variables.get(*v).unwrap(), VariablePosition::new(i as u32)))
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

    let insert_plan =
        compiler::insert::program::compile(Arc::new(translation_context.variable_registry), block.conjunction().constraints(), &input_row_format, &entry_annotations)
            .unwrap();

    let mut output_rows = Vec::with_capacity(input_rows.len());
    let output_width = insert_plan.output_row_schema.len();
    let insert_executor = InsertExecutor::new(insert_plan);
    for input_row in input_rows {
        let mut output_multiplicity = 1;
        output_rows.push(
            (0..output_width)
                .map(|i| input_row.get(i).map_or_else(|| VariableValue::Empty, |existing| existing.clone()))
                .collect::<Vec<_>>(),
        );
        insert_executor.execute_insert(
            snapshot,
            thing_manager,
            &mut Row::new(output_rows.last_mut().unwrap(), &mut output_multiplicity),
        )?;
    }
    Ok(output_rows)
}

fn main() {
    PERSON_LABEL.set(Label::new_static("person")).unwrap();
    GROUP_LABEL.set(Label::new_static("group")).unwrap();
    MEMBERSHIP_LABEL.set(Label::new_static("membership")).unwrap();
    MEMBERSHIP_MEMBER_LABEL.set(Label::new_static_scoped("member", "membership", "membership:member")).unwrap();
    MEMBERSHIP_GROUP_LABEL.set(Label::new_static_scoped("group", "membership", "membership:group")).unwrap();
    AGE_LABEL.set(Label::new_static("age")).unwrap();
    NAME_LABEL.set(Label::new_static("name")).unwrap();
    init_logging();

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), Some(storage.read_watermark()));
    const NUM_THREADS: usize = 32;
    const INTERNAL_ITERS: u64 = 100;
    let start_signal_rw_lock = Arc::new(RwLock::new(()));
    let write_guard = start_signal_rw_lock.write().unwrap();
    let join_handles: [JoinHandle<()>; NUM_THREADS] = array::from_fn(|_| {
        let storage_cloned = storage.clone();
        let type_manager_cloned = type_manager.clone();
        let thing_manager_cloned = thing_manager.clone();
        let rw_lock_cloned = start_signal_rw_lock.clone();
        thread::spawn(move || {
            drop(rw_lock_cloned.read().unwrap());
            for _ in 0..INTERNAL_ITERS {
                let mut snapshot = storage_cloned.clone().open_snapshot_write();
                let age: u32 = rand::random();
                let _row = execute_insert(
                    &mut snapshot,
                    &type_manager_cloned,
                    &thing_manager_cloned,
                    &format!("insert $p isa person, has age {age};"),
                    &vec![],
                    vec![vec![]],
                )
                    .unwrap();
                snapshot.commit().unwrap();
            }
        })
    });
    println!("Sleeping 1s before starting threads");
    sleep(Duration::from_secs(1));
    println!("Start!");
    let start = Instant::now();
    drop(write_guard); // Start
    for join_handle in join_handles {
        join_handle.join().unwrap()
    }
    let time_taken_ms = start.elapsed().as_millis();
    println!("{NUM_THREADS} threads * {INTERNAL_ITERS} iters took: {time_taken_ms} ms");
}
