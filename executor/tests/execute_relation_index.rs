/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use compiler::{
    annotation::{function::EmptyAnnotatedFunctionSignatures, match_inference::infer_types},
    executable::{
        match_::{
            instructions::{
                ConstraintInstruction, Inputs,
            },
            planner::{
                function_plan::ExecutableFunctionRegistry,
                match_executable::{ExecutionStep, IntersectionStep, MatchExecutable},
            },
        },
        next_executable_id,
    },
    ExecutorVariable, VariablePosition,
};
use compiler::executable::match_::instructions::{CheckInstruction, CheckVertex};
use compiler::executable::match_::instructions::thing::{HasReverseInstruction, IndexedRelationInstruction};
use compiler::executable::match_::instructions::type_::TypeListInstruction;
use compiler::executable::match_::planner::match_executable::CheckStep;
use concept::{
    thing::object::ObjectAPI,
    type_::{
        annotation::AnnotationCardinality, Ordering, OwnerAPI, PlayerAPI,
        relates::RelatesAnnotation,
    },
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    error::ReadExecutionError, ExecutionInterrupt, match_executor::MatchExecutor, pipeline::stage::ExecutionContext,
    profile::QueryProfile, row::MaybeOwnedRow,
};
use ir::{
    pattern::constraint::IsaKind,
    pipeline::{block::Block, ParameterRegistry},
    translation::TranslationContext,
};
use ir::pattern::constraint::Comparator;
use ir::pattern::Vertex;
use lending_iterator::LendingIterator;
use storage::{durability_client::WALClient, MVCCStorage, snapshot::CommittableSnapshot};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const MOVIE_LABEL: Label = Label::new_static("group");
const CHARACTER_LABEL: Label = Label::new_static("character");
const AGE_LABEL: Label = Label::new_static("age");
const ID_LABEL: Label = Label::new_static("id");
const CASTING_LABEL: Label = Label::new_static("casting");
const CASTING_MOVIE_LABEL: Label = Label::new_static_scoped("movie", "casting", "casting:movie");
const CASTING_ACTOR_LABEL: Label = Label::new_static_scoped("actor", "casting", "casting:actor");
const CASTING_CHARACTER_LABEL: Label = Label::new_static_scoped("character", "casting", "casting:character");

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let movie_type = type_manager.create_entity_type(&mut snapshot, &MOVIE_LABEL).unwrap();
    let character_type = type_manager.create_entity_type(&mut snapshot, &CHARACTER_LABEL).unwrap();

    let casting_type = type_manager.create_relation_type(&mut snapshot, &CASTING_LABEL).unwrap();

    let relates_movie = casting_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            CASTING_MOVIE_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    let casting_movie_type = relates_movie.role();

    let relates_actor = casting_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            CASTING_ACTOR_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    let casting_actor_type = relates_actor.role();
    
    let relates_character = casting_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            CASTING_CHARACTER_LABEL.name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    relates_character.set_annotation(
        &mut snapshot,
        &type_manager,
        &thing_manager,
        RelatesAnnotation::Cardinality(AnnotationCardinality::new(0, Some(2)))
    ).unwrap();
    let casting_character_type = relates_character.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let id_type = type_manager.create_attribute_type(&mut snapshot, &ID_LABEL).unwrap();
    id_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();

    let _person_owns_age =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered).unwrap();
    let _movie_owns_id =
        movie_type.set_owns(&mut snapshot, &type_manager, &thing_manager, id_type, Ordering::Unordered).unwrap();
    let _character_owns_id =
        character_type.set_owns(&mut snapshot, &type_manager, &thing_manager, id_type, Ordering::Unordered).unwrap();

    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, casting_actor_type).unwrap();
    movie_type.set_plays(&mut snapshot, &type_manager, &thing_manager, casting_movie_type).unwrap();
    character_type.set_plays(&mut snapshot, &type_manager, &thing_manager, casting_character_type).unwrap();

    /*
    insert
         $person_1 isa person, has age 10;
         $person_2 isa person, has age 11;

         $movie_1 isa movie, has id 0;
         $movie_2 isa movie, has id 1;
         $movie_3 isa movie, has id 2;

         $character_1 isa character, has id 0;
         $character_2 isa character, has id 1;
         $character_3 isa character, has id 2;

         $casting_binary isa casting, links (movie: $movie_1, actor: $person_1);
         $casting_ternary isa casting, links (movie: $movie_2, actor: $person_1, character: $character_1);
         $casting_quaternary_multi_role_player isa casting,
            links (movie: $movie_3, actor: $person_2, character: $character_2, character: $character_3);
    */
    let age_10 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Long(10)).unwrap();
    let age_11 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Long(11)).unwrap();

    let id_0 = thing_manager.create_attribute(&mut snapshot, id_type, Value::Long(0)).unwrap();
    let id_1 = thing_manager.create_attribute(&mut snapshot, id_type, Value::Long(1)).unwrap();
    let id_2 = thing_manager.create_attribute(&mut snapshot, id_type, Value::Long(2)).unwrap();

    let person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    let person_2 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_10).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &age_11).unwrap();

    let movie_1 = thing_manager.create_entity(&mut snapshot, movie_type).unwrap();
    let movie_2 = thing_manager.create_entity(&mut snapshot, movie_type).unwrap();
    let movie_3 = thing_manager.create_entity(&mut snapshot, movie_type).unwrap();
    movie_1.set_has_unordered(&mut snapshot, &thing_manager, &id_0).unwrap();
    movie_2.set_has_unordered(&mut snapshot, &thing_manager, &id_1).unwrap();
    movie_3.set_has_unordered(&mut snapshot, &thing_manager, &id_2).unwrap();

    let character_1 = thing_manager.create_entity(&mut snapshot, character_type).unwrap();
    let character_2 = thing_manager.create_entity(&mut snapshot, character_type).unwrap();
    let character_3 = thing_manager.create_entity(&mut snapshot, character_type).unwrap();
    character_1.set_has_unordered(&mut snapshot, &thing_manager, &id_0).unwrap();
    character_2.set_has_unordered(&mut snapshot, &thing_manager, &id_1).unwrap();
    character_3.set_has_unordered(&mut snapshot, &thing_manager, &id_2).unwrap();

    let casting_binary = thing_manager.create_relation(&mut snapshot, casting_type).unwrap();
    let casting_ternary = thing_manager.create_relation(&mut snapshot, casting_type).unwrap();
    let casting_quaternary_multi_role_player = thing_manager.create_relation(&mut snapshot, casting_type).unwrap();

    casting_binary.add_player(&mut snapshot, &thing_manager, casting_movie_type, movie_1.into_object()).unwrap();
    casting_binary.add_player(&mut snapshot, &thing_manager, casting_actor_type, person_1.into_object()).unwrap();

    casting_ternary.add_player(&mut snapshot, &thing_manager, casting_movie_type, movie_2.into_object()).unwrap();
    casting_ternary.add_player(&mut snapshot, &thing_manager, casting_actor_type, person_1.into_object()).unwrap();
    casting_ternary.add_player(&mut snapshot, &thing_manager, casting_character_type, character_1.into_object()).unwrap();

    casting_quaternary_multi_role_player.add_player(&mut snapshot, &thing_manager, casting_movie_type, movie_3.into_object()).unwrap();
    casting_quaternary_multi_role_player.add_player(&mut snapshot, &thing_manager, casting_actor_type, person_2.into_object()).unwrap();
    casting_quaternary_multi_role_player.add_player(&mut snapshot, &thing_manager, casting_character_type, character_2.into_object()).unwrap();
    casting_quaternary_multi_role_player.add_player(&mut snapshot, &thing_manager, casting_character_type, character_3.into_object()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok(), "{:?}", finalise_result.unwrap_err());
    snapshot.commit().unwrap();
}

#[test]
fn traverse_index_from_unbound() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $casting links (movie: $movie, character: $character), isa casting;

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_movie_type = conjunction.get_or_declare_variable("movie_type").unwrap();
    let var_casting_type = conjunction.get_or_declare_variable("casting_type").unwrap();
    let var_casting_movie_type = conjunction.get_or_declare_variable("casting_movie_type").unwrap();
    let var_casting_character_type = conjunction.get_or_declare_variable("casting_character_type").unwrap();

    let var_movie = conjunction.get_or_declare_variable("movie").unwrap();
    let var_character = conjunction.get_or_declare_variable("character").unwrap();
    let var_casting = conjunction.get_or_declare_variable("casting").unwrap();

    let links_casting_character = conjunction.constraints_mut()
        .add_links(var_casting, var_character, var_casting_character_type).unwrap().clone();
    let links_casting_movie = conjunction.constraints_mut()
        .add_links(var_casting, var_movie, var_casting_movie_type).unwrap().clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_movie, var_movie_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_casting, var_casting_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_movie_type, MOVIE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_type, CASTING_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_movie_type, CASTING_MOVIE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_character_type, CASTING_CHARACTER_LABEL.clone()).unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
        .unwrap();

    let row_vars = vec![var_movie, var_character, var_casting];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [
            var_movie_type,
            var_casting_type,
            var_casting_movie_type,
            var_casting_character_type,
            var_movie,
            var_character,
            var_casting,
        ]
            .map(|var| {
                if row_vars.contains(&var) {
                    (var, ExecutorVariable::RowPosition(variable_positions[&var]))
                } else {
                    (var, ExecutorVariable::Internal(var))
                }
            }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan with unbound movie as the start -- should produce:

    // movie 2 -> character 1
    // movie 3 -> character 2
    // movie 3 -> character 3
    let steps = vec![
        // unbound movie ----> person via indexed relation
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_movie],
            vec![ConstraintInstruction::IndexedRelation(
                IndexedRelationInstruction::new(
                    var_movie, var_character, var_casting, var_casting_movie_type, var_casting_character_type,
                    Inputs::None([]),
                    entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    &entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .player_to_relation(),
                    &entry_annotations.constraint_annotations_of(links_casting_character.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_character.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                ).map(&mapping)
            )],
            vec![variable_positions[&var_movie], variable_positions[&var_character], variable_positions[&var_casting]],
            &named_variables,
            3,
        )),
    ];

    let executable = MatchExecutable::new(next_executable_id(), steps, variable_positions.clone(), row_vars.clone());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
        .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager.clone(), value_parameters.clone());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();
    for row in &rows {
        let r = row.as_ref().unwrap();
        print!("{}", r);
        println!()
    }
    assert_eq!(rows.len(), 3);


    // Plan with unbound character as the start -- should produce:

    // character 1 -> movie 2
    // character 2 -> movie 3
    // character 3 -> movie 3
    let steps = vec![
        // unbound movie ----> person via indexed relation
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_movie],
            vec![ConstraintInstruction::IndexedRelation(
                IndexedRelationInstruction::new(
                    var_character, var_movie, var_casting, var_casting_character_type, var_casting_movie_type,
                    Inputs::None([]),
                    entry_annotations.constraint_annotations_of(links_casting_character.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    &entry_annotations.constraint_annotations_of(links_casting_character.clone().into()).unwrap()
                        .as_links()
                        .player_to_relation(),
                    &entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_character.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                ).map(&mapping)
            )],
            vec![variable_positions[&var_movie], variable_positions[&var_character], variable_positions[&var_casting]],
            &named_variables,
            3,
        )),
    ];

    let executable = MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
        .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, value_parameters);
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();
    for row in &rows {
        let r = row.as_ref().unwrap();
        print!("{}", r);
        println!()
    }
    assert_eq!(rows.len(), 3);
}

#[test]
fn traverse_index_from_bound() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $movie isa movie, has id 0;
    //    $casting links (movie: $movie, actor: $person), isa casting;

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let id_0_parameter = value_parameters.register_value(Value::Long(0));
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_movie_type = conjunction.get_or_declare_variable("movie_type").unwrap();
    let var_casting_type = conjunction.get_or_declare_variable("casting_type").unwrap();
    let var_id_type = conjunction.get_or_declare_variable("id_type").unwrap();
    let var_casting_movie_type = conjunction.get_or_declare_variable("casting_movie_type").unwrap();
    let var_casting_actor_type = conjunction.get_or_declare_variable("casting_actor_type").unwrap();

    let var_movie = conjunction.get_or_declare_variable("movie").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_casting = conjunction.get_or_declare_variable("casting").unwrap();
    let var_id = conjunction.get_or_declare_variable("id").unwrap();

    let links_casting_actor = conjunction.constraints_mut()
        .add_links(var_casting, var_person, var_casting_actor_type).unwrap().clone();
    let links_casting_movie = conjunction.constraints_mut()
        .add_links(var_casting, var_movie, var_casting_movie_type).unwrap().clone();
    let movie_has_id = conjunction.constraints_mut()
        .add_has(var_movie, var_id).unwrap().clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_movie, var_movie_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_casting, var_casting_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_id, var_id_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_movie_type, MOVIE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_type, CASTING_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_id_type, ID_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_movie_type, CASTING_MOVIE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_actor_type, CASTING_ACTOR_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_comparison(Vertex::Variable(var_id), Vertex::Parameter(id_0_parameter), Comparator::Equal).unwrap();

    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
        .unwrap();

    let row_vars = vec![var_id, var_movie, var_person, var_casting];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [
            var_movie_type,
            var_casting_type,
            var_id_type,
            var_casting_movie_type,
            var_casting_actor_type,

            var_movie,
            var_person,
            var_casting,
            var_id,
        ]
            .map(|var| {
                if row_vars.contains(&var) {
                    (var, ExecutorVariable::RowPosition(variable_positions[&var]))
                } else {
                    (var, ExecutorVariable::Internal(var))
                }
            }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan with bound movie
    let steps = vec![
        // movie has id;
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_movie],
            vec![
                ConstraintInstruction::HasReverse(
                    HasReverseInstruction::new(movie_has_id, Inputs::None([]), &entry_annotations).map(&mapping),
                ),
            ],
            vec![variable_positions[&var_movie], variable_positions[&var_id]],
            &named_variables,
            2,
        )),
        // id == 0
        ExecutionStep::Check(CheckStep::new(
            vec![CheckInstruction::Comparison {
                lhs: CheckVertex::Variable(mapping.get(&var_id).unwrap().clone()),
                rhs: CheckVertex::Parameter(id_0_parameter),
                comparator: Comparator::Equal
            }],
            vec![variable_positions[&var_movie], variable_positions[&var_id]],
            2,
        )),
        // bound Movie ----> person via indexed relation
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person],
            vec![ConstraintInstruction::IndexedRelation(
                IndexedRelationInstruction::new(
                    var_movie, var_person, var_casting, var_casting_movie_type, var_casting_actor_type,
                    Inputs::Single([var_movie]),
                    entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    &entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .player_to_relation(),
                    &entry_annotations.constraint_annotations_of(links_casting_actor.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_actor.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                ).map(&mapping)
            )],
            vec![variable_positions[&var_movie], variable_positions[&var_id], variable_positions[&var_person]],
            &named_variables,
            3,
        ))
    ];

    let executable = MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
        .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::new(value_parameters));
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();
    assert_eq!(rows.len(), 1);

    for row in rows {
        let r = row.unwrap();
        print!("{}", r);
        println!()
    }
}

#[test]
fn traverse_index_bound_role_type_filtered_correctly() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query testing only exactly 1 role bound:
    //   match
    //    $casting links (movie: $movie, $other), isa casting;

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_movie_type = conjunction.get_or_declare_variable("movie_type").unwrap();
    let var_casting_type = conjunction.get_or_declare_variable("casting_type").unwrap();
    let var_casting_movie_type = conjunction.get_or_declare_variable("casting_movie_type").unwrap();
    let var_casting_other_type = conjunction.get_or_declare_variable("casting_other_type").unwrap();

    let var_movie = conjunction.get_or_declare_variable("movie").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_casting = conjunction.get_or_declare_variable("casting").unwrap();

    let links_casting_other = conjunction.constraints_mut()
        .add_links(var_casting, var_person, var_casting_other_type).unwrap().clone();
    let links_casting_movie = conjunction.constraints_mut()
        .add_links(var_casting, var_movie, var_casting_movie_type).unwrap().clone();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_movie, var_movie_type.into()).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_casting, var_casting_type.into()).unwrap();
    conjunction.constraints_mut().add_label(var_movie_type, MOVIE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_type, CASTING_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_casting_movie_type, CASTING_MOVIE_LABEL.clone()).unwrap();

    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
        .unwrap();

    let row_vars = vec![var_casting_movie_type, var_casting_other_type, var_movie, var_person, var_casting];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from(
        [
            var_movie_type,
            var_casting_type,
            var_casting_movie_type,
            var_casting_other_type,

            var_movie,
            var_person,
            var_casting,
        ]
            .map(|var| {
                if row_vars.contains(&var) {
                    (var, ExecutorVariable::RowPosition(variable_positions[&var]))
                } else {
                    (var, ExecutorVariable::Internal(var))
                }
            }),
    );
    let named_variables = mapping.values().copied().collect();

    // Plan with a single bound role, should produce:
    
    // casting (movie: movie 1, actor: person 1)
    // casting (movie: movie 2, actor: person 1)
    // casting (movie: movie 2, character: character 1)
    // casting (movie: movie 3, actor: person 2)
    // casting (movie: movie 3, actor: character 2)
    // casting (movie: movie 3, actor: character 3)
    let steps = vec![
        // $0 type casting:movie
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_casting_movie_type],
            vec![
                ConstraintInstruction::TypeList(
                    TypeListInstruction::new(
                        var_casting_movie_type,
                        entry_annotations.vertex_annotations_of(&Vertex::Variable(var_casting_movie_type)).unwrap().clone()
                    )
                ).map(&mapping),
            ],
            vec![variable_positions[&var_casting_movie_type]],
            &named_variables,
            1,
        )),
        // unbound movie ----> person via indexed relation, with one bound role type
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_movie],
            vec![ConstraintInstruction::IndexedRelation(
                IndexedRelationInstruction::new(
                    var_person, var_movie, var_casting, var_casting_other_type, var_casting_movie_type,
                    Inputs::Single([var_casting_movie_type]),
                    entry_annotations.constraint_annotations_of(links_casting_other.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    &entry_annotations.constraint_annotations_of(links_casting_other.clone().into()).unwrap()
                        .as_links()
                        .player_to_relation(),
                    &entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .relation_to_player(),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_other.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                    Arc::new(entry_annotations.constraint_annotations_of(links_casting_movie.clone().into()).unwrap()
                        .as_links()
                        .player_to_role()
                        .values().flat_map(|set| set.iter().map(|type_| type_.as_role_type())).collect()),
                ).map(&mapping)
            )],
            vec![
                variable_positions[&var_casting_movie_type],
                variable_positions[&var_casting_other_type],
                variable_positions[&var_movie],
                variable_positions[&var_person],
                variable_positions[&var_casting],
            ],
            &named_variables,
            5,
        ))
    ];

    let executable = MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
        .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::new(value_parameters));
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> = iterator
        .map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect();

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        print!("{}", r);
        println!()
    }

    assert_eq!(rows.len(), 6);
}

