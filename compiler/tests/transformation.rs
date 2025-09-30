/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, sync::Arc};

use compiler::{
    annotation::{
        function::EmptyAnnotatedFunctionSignatures, match_inference::infer_types, type_annotations::BlockAnnotations,
    },
    transformation::{
        redundant_constraints::optimize_away_statically_unsatisfiable_conjunctions,
        relation_index::relation_index_transformation,
    },
};
use concept::type_::{type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI};
use encoding::value::label::Label;
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, Vertex},
    pipeline::{function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
    translation::{match_::translate_match, PipelineTranslationContext},
};
use itertools::Itertools;
use resource::profile::{CommitProfile, StorageCounters};
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadableSnapshot},
    MVCCStorage,
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const DOG_LABEL: Label = Label::new_static("dog");
const DOG_OWNERSHIP_LABEL: Label = Label::new_static("dog-ownership");
const DOG_OWNERSHIP_DOG: Label = Label::new_static_scoped("dog", "dog-ownership", "dog-ownership:dog");
const DOG_OWNERSHIP_OWNER: Label = Label::new_static_scoped("owner", "dog-ownership", "dog-ownership:owner");
const START_TIME_LABEL: Label = Label::new_static("start-time");

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None, false);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let dog_type = type_manager.create_entity_type(&mut snapshot, &DOG_LABEL).unwrap();
    let dog_ownership_type = type_manager.create_relation_type(&mut snapshot, &DOG_OWNERSHIP_LABEL).unwrap();
    let relates_dog = dog_ownership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            DOG_OWNERSHIP_DOG.name.as_str(),
            Ordering::Unordered,
            StorageCounters::DISABLED,
        )
        .unwrap();
    let relates_owner = dog_ownership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            DOG_OWNERSHIP_OWNER.name.as_str(),
            Ordering::Unordered,
            StorageCounters::DISABLED,
        )
        .unwrap();
    person_type
        .set_plays(&mut snapshot, &type_manager, &thing_manager, relates_owner.role(), StorageCounters::DISABLED)
        .unwrap();
    dog_type
        .set_plays(&mut snapshot, &type_manager, &thing_manager, relates_dog.role(), StorageCounters::DISABLED)
        .unwrap();

    let start_time_type = type_manager.create_attribute_type(&mut snapshot, &START_TIME_LABEL).unwrap();
    dog_ownership_type
        .set_owns(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            start_time_type,
            Ordering::Unordered,
            StorageCounters::DISABLED,
        )
        .unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
    assert!(finalise_result.is_ok());
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

fn translate_and_annotate(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query: &str,
) -> (Conjunction, BlockAnnotations) {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline().stages.remove(0).into_match();
    let mut context = PipelineTranslationContext::new();
    let mut parameters = ParameterRegistry::new();
    let translated =
        translate_match(&mut context, &mut parameters, &HashMapFunctionSignatureIndex::empty(), &parsed).unwrap();

    let block = translated.finish().unwrap();
    let type_annotations = infer_types(
        snapshot,
        &block,
        &context.variable_registry,
        type_manager,
        &BTreeMap::new(),
        &EmptyAnnotatedFunctionSignatures,
        false,
    )
    .unwrap();
    let conjunction = block.into_conjunction();
    (conjunction, type_annotations)
}

fn run_test_relation_index_transformation_single<const STATIC_SCHEMA_GUARANTEE: bool>() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None, STATIC_SCHEMA_GUARANTEE);
    let snapshot = storage.clone().open_snapshot_read();

    let dog_ownership = type_manager.get_relation_type(&snapshot, &DOG_OWNERSHIP_LABEL).unwrap().unwrap();
    assert!(dog_ownership.qualifies_for_relation_index(&snapshot, &type_manager).unwrap());

    let query = "match $r links ($role_x: $x, $role_y: $y);";
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline().stages.remove(0).into_match();
    let mut context = PipelineTranslationContext::new();
    let mut parameters = ParameterRegistry::new();
    let translated =
        translate_match(&mut context, &mut parameters, &HashMapFunctionSignatureIndex::empty(), &parsed).unwrap();

    let block = translated.finish().unwrap();
    let mut type_annotations = infer_types(
        &snapshot,
        &block,
        &context.variable_registry,
        &type_manager,
        &BTreeMap::new(),
        &EmptyAnnotatedFunctionSignatures,
        false,
    )
    .unwrap();

    let mut conjunction = block.into_conjunction();

    relation_index_transformation(&mut conjunction, &mut type_annotations, &thing_manager, &snapshot).unwrap();

    let first_indexed_relation =
        conjunction.constraints().iter().filter_map(|constraint| constraint.as_indexed_relation()).next();
    if STATIC_SCHEMA_GUARANTEE {
        let indexed_relation = first_indexed_relation.expect("Expected indexed relation");
        let var_r = Vertex::Variable(context.get_variable("r").unwrap());
        let var_x = Vertex::Variable(context.get_variable("x").unwrap());
        let var_y = Vertex::Variable(context.get_variable("y").unwrap());
        let var_role_x = Vertex::Variable(context.get_variable("role_x").unwrap());
        let var_role_y = Vertex::Variable(context.get_variable("role_y").unwrap());

        assert_eq!(indexed_relation.relation(), &(var_r));
        assert!(indexed_relation.player_1() == &(var_x) || indexed_relation.player_2() == &(var_x));
        assert!(indexed_relation.player_1() == &(var_y) || indexed_relation.player_2() == &(var_y));
        assert!(indexed_relation.role_type_1() == &(var_role_x) || indexed_relation.role_type_2() == &(var_role_x));
        assert!(indexed_relation.role_type_1() == &(var_role_y) || indexed_relation.role_type_2() == &(var_role_y));
    } else {
        assert!(first_indexed_relation.is_none(), "Expected no indexed relation without relation guarantees");
    }
}

#[test]
fn test_relation_index_transformation_single() {
    run_test_relation_index_transformation_single::<false>()
}

#[test]
fn test_relation_index_transformation_single_static_schema_guarantee() {
    run_test_relation_index_transformation_single::<true>()
}

fn run_test_relation_index_transformation_dual<const STATIC_SCHEMA_GUARANTEE: bool>() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None, STATIC_SCHEMA_GUARANTEE);
    let snapshot = storage.clone().open_snapshot_read();

    let dog_ownership = type_manager.get_relation_type(&snapshot, &DOG_OWNERSHIP_LABEL).unwrap().unwrap();
    assert!(dog_ownership.qualifies_for_relation_index(&snapshot, &type_manager).unwrap());

    let query = "match $r links ($x, $y); $q links ($a, $b);";
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline().stages.remove(0).into_match();
    let mut context = PipelineTranslationContext::new();
    let mut parameters = ParameterRegistry::new();
    let translated =
        translate_match(&mut context, &mut parameters, &HashMapFunctionSignatureIndex::empty(), &parsed).unwrap();

    let block = translated.finish().unwrap();
    let mut type_annotations = infer_types(
        &snapshot,
        &block,
        &context.variable_registry,
        &type_manager,
        &BTreeMap::new(),
        &EmptyAnnotatedFunctionSignatures,
        false,
    )
    .unwrap();

    let mut conjunction = block.into_conjunction();

    relation_index_transformation(&mut conjunction, &mut type_annotations, &thing_manager, &snapshot).unwrap();

    let var_r = Vertex::Variable(context.get_variable("r").unwrap());
    let var_x = Vertex::Variable(context.get_variable("x").unwrap());
    let var_y = Vertex::Variable(context.get_variable("y").unwrap());

    let var_q = Vertex::Variable(context.get_variable("q").unwrap());
    let var_a = Vertex::Variable(context.get_variable("a").unwrap());
    let var_b = Vertex::Variable(context.get_variable("b").unwrap());

    let mut indexed_relations =
        conjunction.constraints().iter().filter_map(|constraint| constraint.as_indexed_relation());
    if STATIC_SCHEMA_GUARANTEE {
        let first_indexed_relation = indexed_relations.next().expect("Expected first indexed relation");
        let second_indexed_relation = indexed_relations.next().expect("Expected second indexed relation");

        if first_indexed_relation.relation() == &var_r {
            assert!(first_indexed_relation.player_1() == &var_x || first_indexed_relation.player_2() == &var_x);
            assert!(first_indexed_relation.player_1() == &var_y || first_indexed_relation.player_2() == &var_y);

            assert!(second_indexed_relation.relation() == &var_q || second_indexed_relation.player_2() == &var_q);
            assert!(second_indexed_relation.player_1() == &var_a || second_indexed_relation.player_2() == &var_a);
            assert!(second_indexed_relation.player_1() == &var_b || second_indexed_relation.player_2() == &var_b);
        } else {
            assert!(first_indexed_relation.relation() == &var_q || first_indexed_relation.player_2() == &var_q);
            assert!(first_indexed_relation.player_1() == &var_a || first_indexed_relation.player_2() == &var_a);
            assert!(first_indexed_relation.player_1() == &var_b || first_indexed_relation.player_2() == &var_b);

            assert!(second_indexed_relation.player_1() == &var_x || second_indexed_relation.player_2() == &var_x);
            assert!(second_indexed_relation.player_1() == &var_y || second_indexed_relation.player_2() == &var_y);
        }
    } else {
        assert!(indexed_relations.next().is_none(), "Expected no indexed relation without relation guarantees");
    }
}

#[test]
fn test_relation_index_transformation_dual() {
    run_test_relation_index_transformation_dual::<false>()
}

#[test]
fn test_relation_index_transformation_dual_static_schema_guarantee() {
    run_test_relation_index_transformation_dual::<true>()
}

fn run_test_relation_index_transformation_not_applied_ternary<const STATIC_SCHEMA_GUARANTEE: bool>() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None, STATIC_SCHEMA_GUARANTEE);
    let snapshot = storage.clone().open_snapshot_read();

    let dog_ownership = type_manager.get_relation_type(&snapshot, &DOG_OWNERSHIP_LABEL).unwrap().unwrap();
    assert!(dog_ownership.qualifies_for_relation_index(&snapshot, &type_manager).unwrap());

    let query = "match $r links ($x, $y, $z);";
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline().stages.remove(0).into_match();
    let mut context = PipelineTranslationContext::new();
    let mut parameters = ParameterRegistry::new();
    let translated =
        translate_match(&mut context, &mut parameters, &HashMapFunctionSignatureIndex::empty(), &parsed).unwrap();

    let block = translated.finish().unwrap();
    let mut type_annotations = infer_types(
        &snapshot,
        &block,
        &context.variable_registry,
        &type_manager,
        &BTreeMap::new(),
        &EmptyAnnotatedFunctionSignatures,
        false,
    )
    .unwrap();

    let mut conjunction = block.into_conjunction();

    relation_index_transformation(&mut conjunction, &mut type_annotations, &thing_manager, &snapshot).unwrap();

    let mut indexed_relations =
        conjunction.constraints().iter().filter_map(|constraint| constraint.as_indexed_relation());
    assert!(!indexed_relations.next().is_some());
}

#[test]
fn test_relation_index_transformation_not_applied_ternary() {
    run_test_relation_index_transformation_not_applied_ternary::<false>()
}

#[test]
fn test_relation_index_transformation_not_applied_ternary_static_schema_guarantee() {
    run_test_relation_index_transformation_not_applied_ternary::<true>()
}

//  TODO: we just want to add with an exclusitivity constraint
//
// fn run_test_relation_index_transformation_not_applied_attribute<const STATIC_SCHEMA_GUARANTEE: bool>() {
//     let (_tmp_dir, mut storage) = create_core_storage();
//     setup_database(&mut storage);
//     let (type_manager, thing_manager) = load_managers(storage.clone(), None, STATIC_SCHEMA_GUARANTEE);
//     let snapshot = storage.clone().open_snapshot_read();
//
//     let dog_ownership = type_manager.get_relation_type(&snapshot, &DOG_OWNERSHIP_LABEL).unwrap().unwrap();
//     assert!(type_manager.relation_index_available(&snapshot, dog_ownership).unwrap());
//
//     let query = "match $r links ($x, $y), has start-time $a; $a == 10;";
//     let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline().stages.remove(0).into_match();
//     let mut context = TranslationContext::new();
//     let mut parameters = ParameterRegistry::new();
//     let translated = translate_match(
//         &mut context,
//         &mut parameters,
//         &HashMapFunctionSignatureIndex::empty(),
//         &parsed,
//     ).unwrap();
//
//     let block = translated.finish().unwrap();
//     let type_annotations = infer_types(
//         &snapshot,
//         &block,
//         &context.variable_registry,
//         &type_manager,
//         &BTreeMap::new(),
//         &EmptyAnnotatedFunctionSignatures,
//     ).unwrap();
//
//     let mut conjunction = block.into_conjunction();
//
//     println!("before transform:\n{}", &conjunction);
//     relation_index_transformation(
//         &mut conjunction,
//         &type_annotations,
//         &thing_manager,
//         &snapshot
//     ).unwrap();
//
//     println!("{}", &conjunction);
// }
//
// #[test]
// fn test_relation_index_transformation_not_applied_attribute() {
//     run_test_relation_index_transformation_not_applied_attribute::<false>()
// }
//
// #[test]
// fn test_relation_index_transformation_not_applied_attribute_static_schema_guarantee() {
//     run_test_relation_index_transformation_not_applied_attribute::<true>()
// }

fn run_test_optimise_away<const STATIC_SCHEMA_GUARANTEE: bool>() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, _thing_manager) = load_managers(storage.clone(), None, STATIC_SCHEMA_GUARANTEE);
    let snapshot = storage.clone().open_snapshot_read();
    {
        let query = "match $p sub person, plays dog-ownership:owner;";
        let (mut conjunction, type_annotations) = translate_and_annotate(&snapshot, &type_manager, query);
        optimize_away_statically_unsatisfiable_conjunctions(&mut conjunction, &type_annotations);
        assert!(
            conjunction.constraints().len() == 2
                && conjunction.constraints().iter().any(|c| matches!(c, Constraint::Plays(_)))
                && conjunction.constraints().iter().any(|c| matches!(c, Constraint::Sub(_)))
        );
    }
    {
        let query = "match $p sub person, plays dog-ownership:dog;";
        let (mut conjunction, type_annotations) = translate_and_annotate(&snapshot, &type_manager, query);
        optimize_away_statically_unsatisfiable_conjunctions(&mut conjunction, &type_annotations);
        assert!(matches!(conjunction.constraints().iter().exactly_one().unwrap(), Constraint::Unsatisfiable(_)));
    }

    {
        let query = "match $p sub person; { $p plays dog-ownership:dog; } or { $p plays dog-ownership:owner; };";
        let (mut conjunction, type_annotations) = translate_and_annotate(&snapshot, &type_manager, query);
        optimize_away_statically_unsatisfiable_conjunctions(&mut conjunction, &type_annotations);
        assert!(matches!(conjunction.constraints().iter().exactly_one().unwrap(), Constraint::Sub(_)));
        let must_be_plays = conjunction
            .nested_patterns()
            .iter()
            .exactly_one()
            .unwrap()
            .as_disjunction()
            .unwrap()
            .conjunctions()
            .iter()
            .exactly_one()
            .unwrap()
            .constraints()
            .iter()
            .exactly_one()
            .unwrap();
        assert!(matches!(must_be_plays, Constraint::Plays(_)))
    }

    {
        let query = "match $p sub person; not { $p plays dog-ownership:dog; };";
        let (mut conjunction, type_annotations) = translate_and_annotate(&snapshot, &type_manager, query);
        optimize_away_statically_unsatisfiable_conjunctions(&mut conjunction, &type_annotations);
        assert!(matches!(conjunction.constraints().iter().exactly_one().unwrap(), Constraint::Sub(_)));
        let must_be_optimised_to_unsatisfiable = conjunction
            .nested_patterns()
            .iter()
            .exactly_one()
            .unwrap()
            .as_negation()
            .unwrap()
            .conjunction()
            .constraints()
            .iter()
            .exactly_one()
            .unwrap();
        assert!(matches!(must_be_optimised_to_unsatisfiable, Constraint::Unsatisfiable(_)))
    }
}

#[test]
fn test_optimise_away() {
    run_test_optimise_away::<false>()
}

#[test]
fn test_optimise_away_static_schema_guarantee() {
    run_test_optimise_away::<true>()
}
