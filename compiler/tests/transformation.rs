/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::sync::Arc;
use typeql::Pattern;
use compiler::annotation::function::EmptyAnnotatedFunctionSignatures;
use compiler::annotation::match_inference::infer_types;
use compiler::annotation::pipeline::annotate_preamble_and_pipeline;
use compiler::transformation::relation_index::relation_index_transformation;
use concept::thing::statistics::Statistics;
use concept::thing::thing_manager::ThingManager;
use concept::type_::{Ordering, PlayerAPI};
use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use encoding::value::label::Label;
use ir::pipeline::function_signature::HashMapFunctionSignatureIndex;
use ir::pipeline::ParameterRegistry;
use ir::translation::match_::translate_match;
use ir::translation::TranslationContext;
use storage::durability_client::WALClient;
use storage::MVCCStorage;
use storage::sequence_number::SequenceNumber;
use storage::snapshot::CommittableSnapshot;
use test_utils::assert_matches;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;


const PERSON_LABEL: Label = Label::new_static("person");
const DOG_LABEL: Label = Label::new_static("dog");
const DOG_OWNERSHIP_LABEL: Label = Label::new_static("dog-ownership");
const DOG_OWNERSHIP_DOG: Label = Label::new_static_scoped("dog", "dog-ownership", "dog-ownership:dog");
const DOG_OWNERSHIP_OWNER: Label = Label::new_static_scoped("owner", "dog-ownership", "dog-ownership:owner");

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let dog_type = type_manager.create_entity_type(&mut snapshot, &DOG_LABEL).unwrap();
    let dog_ownership_type = type_manager.create_relation_type(&mut snapshot, &DOG_OWNERSHIP_LABEL).unwrap();
    let relates_dog = dog_ownership_type.create_relates(&mut snapshot, &type_manager, &thing_manager, DOG_OWNERSHIP_DOG.name.as_str(), Ordering::Unordered).unwrap();
    let relates_owner = dog_ownership_type.create_relates(&mut snapshot, &type_manager, &thing_manager, DOG_OWNERSHIP_OWNER.name.as_str(), Ordering::Unordered).unwrap();
    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, relates_owner.role()).unwrap();
    dog_type.set_plays(&mut snapshot, &type_manager, &thing_manager, relates_dog.role()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
}

#[test]
fn test_relation_index_transformation() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let snapshot = storage.clone().open_snapshot_read();

    let dog_ownership = type_manager.get_relation_type(&snapshot, &DOG_OWNERSHIP_LABEL).unwrap().unwrap();
    assert!(type_manager.relation_index_available(&snapshot, dog_ownership).unwrap());

    let query = "match $r links ($x, $y);";
    let parsed = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();
    let mut context = TranslationContext::new();
    let mut parameters = ParameterRegistry::new();
    let translated = translate_match(
        &mut context,
        &mut parameters,
        &HashMapFunctionSignatureIndex::empty(),
        &parsed,
    ).unwrap();

    let block = translated.finish().unwrap();
    let type_annotations = infer_types(
        &snapshot,
        &block,
        &context.variable_registry,
        &type_manager,
        &BTreeMap::new(),
        &EmptyAnnotatedFunctionSignatures,
    ).unwrap();

    let mut conjunction = block.into_conjunction();

    println!("before transform:\n{}", &conjunction);
    relation_index_transformation(
        &mut conjunction,
        &type_annotations,
        &type_manager,
        &snapshot
    ).unwrap();

    println!("{}", &conjunction);
}