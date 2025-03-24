/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    ops::Bound,
    sync::Arc,
};

use answer::variable::Variable;
use compiler::{
    annotation::{
        function::EmptyAnnotatedFunctionSignatures, match_inference::infer_types, type_annotations::TypeAnnotations,
    },
    executable::{
        function::ExecutableFunctionRegistry,
        match_::{
            instructions::{
                thing::{HasInstruction, HasReverseInstruction, IsaReverseInstruction},
                type_::TypeListInstruction,
                CheckInstruction, CheckVertex, ConstraintInstruction, Inputs,
            },
            planner::{
                match_executable::{ExecutionStep, IntersectionStep, MatchExecutable},
                plan::PlannerStatistics,
            },
        },
        next_executable_id,
    },
    ExecutorVariable, VariablePosition,
};
use concept::{
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{
        annotation::AnnotationCardinality, object_type::ObjectType, owns::OwnsAnnotation, relates::RelatesAnnotation,
        type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI,
    },
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    error::ReadExecutionError, match_executor::MatchExecutor, pipeline::stage::ExecutionContext, row::MaybeOwnedRow,
    ExecutionInterrupt,
};
use ir::{
    pattern::{
        constraint::{Comparator, IsaKind},
        Vertex,
    },
    pipeline::{block::Block, ParameterRegistry},
    translation::TranslationContext,
};
use lending_iterator::LendingIterator;
use resource::profile::{CommitProfile, QueryProfile, StorageCounters};
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadableSnapshot},
    MVCCStorage,
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;
use typeql::common::Span;

const PERSON_LABEL: Label = Label::new_static("person");
const MOVIE_LABEL: Label = Label::new_static("group");
const CHARACTER_LABEL: Label = Label::new_static("character");
const NAME_LABEL: Label = Label::new_static("name");
const AGE_LABEL: Label = Label::new_static("age");
const GOV_ID_LABEL: Label = Label::new_static("gov_id");
const ID_LABEL: Label = Label::new_static("id");
const CASTING_LABEL: Label = Label::new_static("casting");
const CASTING_MOVIE_LABEL: Label = Label::new_static_scoped("movie", "casting", "casting:movie");
const CASTING_ACTOR_LABEL: Label = Label::new_static_scoped("actor", "casting", "casting:actor");
const CASTING_CHARACTER_LABEL: Label = Label::new_static_scoped("character", "casting", "casting:character");

const VALUE_STRING_LONG_UNINLINEABLE: &str = "longy-mc-long-face-uninlineable";
const VALUE_STRING_ABBY: &str = "abby";
const VALUE_STRING_BOLTON: &str = "bolton";
const VALUE_STRING_WILLOW: &str = "willow";

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
    relates_character
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            RelatesAnnotation::Cardinality(AnnotationCardinality::new(0, Some(2))),
        )
        .unwrap();
    let casting_character_type = relates_character.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let id_type = type_manager.create_attribute_type(&mut snapshot, &ID_LABEL).unwrap();
    id_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let gov_id_type = type_manager.create_attribute_type(&mut snapshot, &GOV_ID_LABEL).unwrap();
    gov_id_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    let _person_owns_age =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered).unwrap();
    let person_owns_gov_id =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, gov_id_type, Ordering::Unordered).unwrap();
    person_owns_gov_id
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
        )
        .unwrap();
    let person_owns_name =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Unordered).unwrap();
    person_owns_name
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
        )
        .unwrap();
    let _movie_owns_id =
        movie_type.set_owns(&mut snapshot, &type_manager, &thing_manager, id_type, Ordering::Unordered).unwrap();
    let _character_owns_id =
        character_type.set_owns(&mut snapshot, &type_manager, &thing_manager, id_type, Ordering::Unordered).unwrap();

    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, casting_actor_type).unwrap();
    movie_type.set_plays(&mut snapshot, &type_manager, &thing_manager, casting_movie_type).unwrap();
    character_type.set_plays(&mut snapshot, &type_manager, &thing_manager, casting_character_type).unwrap();

    /*
    insert
         $person_1 isa person,
            has age 10,
            has gov_id 0,
            has gov_id 1,
            has gov_id 2,
            has gov_id 3;
         $person_2 isa person,
           has age 11,
           has name "abby",
           has name "bolton",
           has name "longy-mc-long-face-uninlineable"
           has name "willa";

         $person_3 isa person,
           has age 10,
           has gov_id 4;

         $person_4 isa person,
           has age 10;

         $person_5 isa person,
           has age 10,
           has gov_id 5;

         $person_6 isa person,
           has gov_id 6;

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
    let age_10 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(10)).unwrap();
    let age_11 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(11)).unwrap();

    let id_0 = thing_manager.create_attribute(&mut snapshot, id_type, Value::Integer(0)).unwrap();
    let id_1 = thing_manager.create_attribute(&mut snapshot, id_type, Value::Integer(1)).unwrap();
    let id_2 = thing_manager.create_attribute(&mut snapshot, id_type, Value::Integer(2)).unwrap();

    let gov_id_0 = thing_manager.create_attribute(&mut snapshot, gov_id_type, Value::Integer(0)).unwrap();
    let gov_id_1 = thing_manager.create_attribute(&mut snapshot, gov_id_type, Value::Integer(1)).unwrap();
    let gov_id_2 = thing_manager.create_attribute(&mut snapshot, gov_id_type, Value::Integer(2)).unwrap();
    let gov_id_3 = thing_manager.create_attribute(&mut snapshot, gov_id_type, Value::Integer(3)).unwrap();
    let gov_id_4 = thing_manager.create_attribute(&mut snapshot, gov_id_type, Value::Integer(4)).unwrap();
    let gov_id_5 = thing_manager.create_attribute(&mut snapshot, gov_id_type, Value::Integer(5)).unwrap();
    let gov_id_6 = thing_manager.create_attribute(&mut snapshot, gov_id_type, Value::Integer(6)).unwrap();

    let name_abby = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(VALUE_STRING_ABBY)))
        .unwrap();
    let name_bolton = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(VALUE_STRING_BOLTON)))
        .unwrap();
    let name_uninlineable = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(VALUE_STRING_LONG_UNINLINEABLE)))
        .unwrap();
    let name_willa = thing_manager
        .create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(VALUE_STRING_WILLOW)))
        .unwrap();

    let person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_10, StorageCounters::DISABLED).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &gov_id_0, StorageCounters::DISABLED).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &gov_id_1, StorageCounters::DISABLED).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &gov_id_2, StorageCounters::DISABLED).unwrap();
    person_1.set_has_unordered(&mut snapshot, &thing_manager, &gov_id_3, StorageCounters::DISABLED).unwrap();

    let person_2 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &age_11, StorageCounters::DISABLED).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &name_abby, StorageCounters::DISABLED).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &name_bolton, StorageCounters::DISABLED).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &name_uninlineable, StorageCounters::DISABLED).unwrap();
    person_2.set_has_unordered(&mut snapshot, &thing_manager, &name_willa, StorageCounters::DISABLED).unwrap();

    let person_3 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    person_3.set_has_unordered(&mut snapshot, &thing_manager, &age_10, StorageCounters::DISABLED).unwrap();
    person_3.set_has_unordered(&mut snapshot, &thing_manager, &gov_id_4, StorageCounters::DISABLED).unwrap();

    let person_4 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    person_4.set_has_unordered(&mut snapshot, &thing_manager, &age_10, StorageCounters::DISABLED).unwrap();

    let person_5 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    person_5.set_has_unordered(&mut snapshot, &thing_manager, &age_10, StorageCounters::DISABLED).unwrap();
    person_5.set_has_unordered(&mut snapshot, &thing_manager, &gov_id_5, StorageCounters::DISABLED).unwrap();

    let person_6 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
    person_6.set_has_unordered(&mut snapshot, &thing_manager, &gov_id_6, StorageCounters::DISABLED).unwrap();

    let movie_1 = thing_manager.create_entity(&mut snapshot, movie_type).unwrap();
    let movie_2 = thing_manager.create_entity(&mut snapshot, movie_type).unwrap();
    let movie_3 = thing_manager.create_entity(&mut snapshot, movie_type).unwrap();
    movie_1.set_has_unordered(&mut snapshot, &thing_manager, &id_0, StorageCounters::DISABLED).unwrap();
    movie_2.set_has_unordered(&mut snapshot, &thing_manager, &id_1, StorageCounters::DISABLED).unwrap();
    movie_3.set_has_unordered(&mut snapshot, &thing_manager, &id_2, StorageCounters::DISABLED).unwrap();

    let character_1 = thing_manager.create_entity(&mut snapshot, character_type).unwrap();
    let character_2 = thing_manager.create_entity(&mut snapshot, character_type).unwrap();
    let character_3 = thing_manager.create_entity(&mut snapshot, character_type).unwrap();
    character_1.set_has_unordered(&mut snapshot, &thing_manager, &id_0, StorageCounters::DISABLED).unwrap();
    character_2.set_has_unordered(&mut snapshot, &thing_manager, &id_1, StorageCounters::DISABLED).unwrap();
    character_3.set_has_unordered(&mut snapshot, &thing_manager, &id_2, StorageCounters::DISABLED).unwrap();

    let casting_binary = thing_manager.create_relation(&mut snapshot, casting_type).unwrap();
    let casting_ternary = thing_manager.create_relation(&mut snapshot, casting_type).unwrap();
    let casting_quaternary_multi_role_player = thing_manager.create_relation(&mut snapshot, casting_type).unwrap();

    casting_binary
        .add_player(&mut snapshot, &thing_manager, casting_movie_type, movie_1.into_object(), StorageCounters::DISABLED)
        .unwrap();
    casting_binary
        .add_player(
            &mut snapshot,
            &thing_manager,
            casting_actor_type,
            person_1.into_object(),
            StorageCounters::DISABLED,
        )
        .unwrap();

    casting_ternary
        .add_player(&mut snapshot, &thing_manager, casting_movie_type, movie_2.into_object(), StorageCounters::DISABLED)
        .unwrap();
    casting_ternary
        .add_player(
            &mut snapshot,
            &thing_manager,
            casting_actor_type,
            person_1.into_object(),
            StorageCounters::DISABLED,
        )
        .unwrap();
    casting_ternary
        .add_player(
            &mut snapshot,
            &thing_manager,
            casting_character_type,
            character_1.into_object(),
            StorageCounters::DISABLED,
        )
        .unwrap();

    casting_quaternary_multi_role_player
        .add_player(&mut snapshot, &thing_manager, casting_movie_type, movie_3.into_object(), StorageCounters::DISABLED)
        .unwrap();
    casting_quaternary_multi_role_player
        .add_player(
            &mut snapshot,
            &thing_manager,
            casting_actor_type,
            person_2.into_object(),
            StorageCounters::DISABLED,
        )
        .unwrap();
    casting_quaternary_multi_role_player
        .add_player(
            &mut snapshot,
            &thing_manager,
            casting_character_type,
            character_2.into_object(),
            StorageCounters::DISABLED,
        )
        .unwrap();
    casting_quaternary_multi_role_player
        .add_player(
            &mut snapshot,
            &thing_manager,
            casting_character_type,
            character_3.into_object(),
            StorageCounters::DISABLED,
        )
        .unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
    assert!(finalise_result.is_ok(), "{:?}", finalise_result.unwrap_err());
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

fn position_mapping<const N: usize, const M: usize>(
    row_vars: [Variable; N],
    internal_vars: [Variable; M],
) -> (
    HashMap<ExecutorVariable, Variable>,
    HashMap<Variable, VariablePosition>,
    HashMap<Variable, ExecutorVariable>,
    HashSet<ExecutorVariable>,
) {
    let position_to_var: HashMap<_, _> =
        row_vars.into_iter().enumerate().map(|(i, v)| (ExecutorVariable::new_position(i as _), v)).collect();
    let variable_positions =
        HashMap::from_iter(position_to_var.iter().map(|(i, var)| (*var, i.as_position().unwrap())));
    let mapping: HashMap<_, _> = row_vars
        .into_iter()
        .map(|var| (var, ExecutorVariable::RowPosition(variable_positions[&var])))
        .chain(internal_vars.into_iter().map(|var| (var, ExecutorVariable::Internal(var))))
        .collect();
    let named_variables = mapping.values().copied().collect();
    (position_to_var, variable_positions, mapping, named_variables)
}

fn get_type_annotations(
    translation_context: &TranslationContext,
    entry: &Block,
    snapshot: &impl ReadableSnapshot,
    type_manager: &Arc<TypeManager>,
) -> TypeAnnotations {
    let previous_stage_variable_annotations = &BTreeMap::new();
    infer_types(
        snapshot,
        &entry,
        &translation_context.variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
        false,
    )
    .unwrap()
}

fn execute_steps(
    steps: Vec<ExecutionStep>,
    variable_positions: HashMap<Variable, VariablePosition>,
    row_vars: HashMap<ExecutorVariable, Variable>,
    storage: Arc<MVCCStorage<WALClient>>,
    thing_manager: Arc<ThingManager>,
    value_parameters: Arc<ParameterRegistry>,
    profile: &QueryProfile,
) -> Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> {
    let executable = MatchExecutable::new(
        next_executable_id(),
        steps,
        variable_positions.clone(),
        row_vars.clone(),
        PlannerStatistics::new(),
    );

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        profile,
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager.clone(), value_parameters.clone());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    iterator
        .map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| Box::new(err.clone())))
        .collect()
}

#[test]
fn value_int_equality_isa_reads() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $attr isa id; $attr == 2; # middle of the range

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_int_2_id = value_parameters.register_value(Value::Integer(2), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_id_type = conjunction.constraints_mut().get_or_declare_variable("var_id_type", None).unwrap();
    let var_attr = conjunction.constraints_mut().get_or_declare_variable("attr", None).unwrap();

    let isa =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attr, var_id_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_id_type, ID_LABEL.clone()).unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_attr), Vertex::Parameter(value_int_2_id), Comparator::Equal, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_id_type, var_attr], []);

    // Plan
    //    1. Intersection($id_type label ID;)
    //    2. Intersection($attr isa $id_type; (VALUE constraints = Eq(value_int_2_id)))
    //
    // Should output:
    //  $attr -> id(2)

    let value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_attr),
        rhs: CheckVertex::Parameter(value_int_2_id),
        comparator: Comparator::Equal,
    }
    .map(&mapping);
    let mut isa_reverse_instruction =
        IsaReverseInstruction::new(isa, Inputs::Single([var_id_type]), &type_annotations).map(&mapping);
    isa_reverse_instruction.add_check(value_check);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_id_type],
            vec![ConstraintInstruction::TypeList(
                TypeListInstruction::new(
                    var_id_type,
                    type_annotations.vertex_annotations().get(&Vertex::Variable(var_id_type)).unwrap().clone(),
                )
                .map(&mapping),
            )],
            vec![variable_positions[&var_id_type]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_attr],
            vec![ConstraintInstruction::IsaReverse(isa_reverse_instruction)],
            vec![variable_positions[&var_id_type], variable_positions[&var_attr]],
            &named_variables,
            2,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);

    assert_eq!(rows.len(), 1);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();
    // 2 seeks: one for the attribute instance iterator, and one for the attribute-has-owner check
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 2);
    // 1 advance: attribute iterator needs to step forward and finish: the initial key range has been left
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 1);
}

#[test]
fn value_int_equality_has_reverse_reads() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person isa person, has gov_id 1; # middle value

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_int_1_id = value_parameters.register_value(Value::Integer(1), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_gov_id = conjunction.constraints_mut().get_or_declare_variable("var_gov_id", None).unwrap();
    let var_gov_id_type = conjunction.constraints_mut().get_or_declare_variable("var_gov_id_type", None).unwrap();

    let has = conjunction.constraints_mut().add_has(var_person, var_gov_id, None).unwrap().clone();
    let _isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_gov_id = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_gov_id, var_gov_id_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_gov_id_type, GOV_ID_LABEL.clone()).unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_gov_id), Vertex::Parameter(value_int_1_id), Comparator::Equal, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) = position_mapping([var_person, var_gov_id], []);

    // plan (requires correct type annotations)
    //       HasReverse($person, $gov_id) with $gov_id = 1
    //
    // Should output:
    //  (person 1, gov_id 1)

    let value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_gov_id),
        rhs: CheckVertex::Parameter(value_int_1_id),
        comparator: Comparator::Equal,
    }
    .map(&mapping);
    let mut has_reverse_instruction =
        HasReverseInstruction::new(has, Inputs::None([]), &type_annotations).map(&mapping);
    has_reverse_instruction.add_check(value_check);

    let steps = vec![ExecutionStep::Intersection(IntersectionStep::new(
        mapping[&var_gov_id],
        vec![ConstraintInstruction::HasReverse(has_reverse_instruction)],
        vec![variable_positions[&var_person], variable_positions[&var_gov_id]],
        &named_variables,
        2,
    ))];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);

    assert_eq!(rows.len(), 1);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(0, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();
    // 1 seek: skip directly to the correct attribute value, and find the only owner
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 1);
    // 1 advance: iterator needs to step forward and finish
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 1);
}

#[test]
fn value_int_equality_has_bound_owner() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person isa person, has gov_id 1; # middle value

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_int_1_id = value_parameters.register_value(Value::Integer(1), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_gov_id = conjunction.constraints_mut().get_or_declare_variable("var_gov_id", None).unwrap();
    let var_gov_id_type = conjunction.constraints_mut().get_or_declare_variable("var_gov_id_type", None).unwrap();

    let has = conjunction.constraints_mut().add_has(var_person, var_gov_id, None).unwrap().clone();
    let isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_gov_id = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_gov_id, var_gov_id_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_gov_id_type, GOV_ID_LABEL.clone()).unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_gov_id), Vertex::Parameter(value_int_1_id), Comparator::Equal, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_person, var_person_type, var_gov_id], []);

    // plan (requires correct type annotations)
    //      IsaReverse($person_type, $person)
    //      Has($person, $gov_id) with $gov_id = 1
    //
    // Should output:
    //  (person 1, gov_id 1)

    let value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_gov_id),
        rhs: CheckVertex::Parameter(value_int_1_id),
        comparator: Comparator::Equal,
    }
    .map(&mapping);
    let mut has_instruction = HasInstruction::new(has, Inputs::Single([var_person]), &type_annotations).map(&mapping);
    has_instruction.add_check(value_check);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person_type],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &type_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person], variable_positions[&var_person_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_gov_id],
            vec![ConstraintInstruction::Has(has_instruction)],
            vec![variable_positions[&var_person], variable_positions[&var_gov_id]],
            &named_variables,
            3,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);

    assert_eq!(rows.len(), 1);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();
    // 6 seeks: for each person, we should skip directly to the person + owned ID
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 6);
    // 1 advance: the iterator matching the only person + id needs to step forward and finish
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 1);
}

#[test]
fn value_int_inequality_has_bound_owner() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person isa person, has gov_id $gov_id; $gov_id >= 1; $gov_id < 3; # middle range

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_int_1_id = value_parameters.register_value(Value::Integer(1), Span { begin_offset: 0, end_offset: 0 });
    let value_int_3_id = value_parameters.register_value(Value::Integer(3), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_gov_id = conjunction.constraints_mut().get_or_declare_variable("var_gov_id", None).unwrap();
    let var_gov_id_type = conjunction.constraints_mut().get_or_declare_variable("var_gov_id_type", None).unwrap();

    let has = conjunction.constraints_mut().add_has(var_person, var_gov_id, None).unwrap().clone();
    let isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_gov_id = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_gov_id, var_gov_id_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_gov_id_type, GOV_ID_LABEL.clone()).unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(
            Vertex::Variable(var_gov_id),
            Vertex::Parameter(value_int_1_id),
            Comparator::GreaterOrEqual,
            None,
        )
        .unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_gov_id), Vertex::Parameter(value_int_3_id), Comparator::Less, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&mut translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_person, var_person_type, var_gov_id], []);

    // plan (requires correct type annotations)
    // plan: Isa($person, person)
    //       Has($person, $_gov_id) with >= 1 and < 3
    //
    // Should output:
    //  (person 1, gov_id 1)
    //  (person 1, gov_id 2)

    let greater_value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_gov_id),
        rhs: CheckVertex::Parameter(value_int_1_id),
        comparator: Comparator::GreaterOrEqual,
    }
    .map(&mapping);
    let lesser_value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_gov_id),
        rhs: CheckVertex::Parameter(value_int_3_id),
        comparator: Comparator::Less,
    }
    .map(&mapping);
    let mut has_instruction = HasInstruction::new(has, Inputs::Single([var_person]), &type_annotations).map(&mapping);
    has_instruction.add_check(greater_value_check);
    has_instruction.add_check(lesser_value_check);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person_type],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &type_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person], variable_positions[&var_person_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_gov_id],
            vec![ConstraintInstruction::Has(has_instruction)],
            vec![variable_positions[&var_person], variable_positions[&var_gov_id]],
            &named_variables,
            3,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);
    assert_eq!(rows.len(), 2);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();
    // 6 seeks: for each person, we should skip directly to the person + owned ID
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 6);
    // 2 advance: the iterator matching person 1 will advances twice (once to find the second ID, then to fail)
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 2)
}

#[test]
fn value_inline_string_equality_has_bound_owner() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person isa person, has name "abby";

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_string_abby = value_parameters
        .register_value(Value::String(Cow::Borrowed(VALUE_STRING_ABBY)), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_name = conjunction.constraints_mut().get_or_declare_variable("var_name", None).unwrap();
    let var_name_type = conjunction.constraints_mut().get_or_declare_variable("var_name_type", None).unwrap();

    let has = conjunction.constraints_mut().add_has(var_person, var_name, None).unwrap().clone();
    let isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_name =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_name), Vertex::Parameter(value_string_abby), Comparator::Equal, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let entry_annotations = get_type_annotations(&mut translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_person, var_person_type, var_name], []);

    // plan (requires correct type annotations)
    //      IsaReverse($person_type, $person)
    //      Has($person, $name) with $name = "abby"
    //
    // Should output:
    //  (person 2, name "abby")

    let value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_name),
        rhs: CheckVertex::Parameter(value_string_abby),
        comparator: Comparator::Equal,
    }
    .map(&mapping);
    let mut has_instruction = HasInstruction::new(has, Inputs::Single([var_person]), &entry_annotations).map(&mapping);
    has_instruction.add_check(value_check);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person_type],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &entry_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person], variable_positions[&var_person_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_name],
            vec![ConstraintInstruction::Has(has_instruction)],
            vec![variable_positions[&var_person], variable_positions[&var_name]],
            &named_variables,
            3,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);

    assert_eq!(rows.len(), 1);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();
    // 6 seeks: for each person, we should skip directly to the person + owned name
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 6);
    // 1 advance: the iterator matching the only person + name needs to step forward and finish
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 1);
}

#[test]
fn value_hashed_string_equality_has_bound_owner() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person isa person, has name "long....etc";

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_string_hashed = value_parameters.register_value(
        Value::String(Cow::Borrowed(VALUE_STRING_LONG_UNINLINEABLE)),
        Span { begin_offset: 0, end_offset: 0 },
    );
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_name = conjunction.constraints_mut().get_or_declare_variable("var_name", None).unwrap();
    let var_name_type = conjunction.constraints_mut().get_or_declare_variable("var_name_type", None).unwrap();

    let has = conjunction.constraints_mut().add_has(var_person, var_name, None).unwrap().clone();
    let isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_name =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_name), Vertex::Parameter(value_string_hashed), Comparator::Equal, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&mut translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_person, var_person_type, var_name], []);

    // plan (requires correct type annotations)
    //      IsaReverse($person_type, $person)
    //      Has($person, $name) with $name = "long..."
    //
    // Should output:
    //  (person 2, name "long...")

    let value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_name),
        rhs: CheckVertex::Parameter(value_string_hashed),
        comparator: Comparator::Equal,
    }
    .map(&mapping);
    let mut has_instruction = HasInstruction::new(has, Inputs::Single([var_person]), &type_annotations).map(&mapping);
    has_instruction.add_check(value_check);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person_type],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &type_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person], variable_positions[&var_person_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_name],
            vec![ConstraintInstruction::Has(has_instruction)],
            vec![variable_positions[&var_person], variable_positions[&var_name]],
            &named_variables,
            3,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);

    assert_eq!(rows.len(), 1);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();
    // 6 seeks: for each person, we should skip directly to the person + owned name
    // 1 seek: for the unlined name, we have to get the value in order to filter it out
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 7);
    // 1 advance: the iterator matching the only person + name needs to step forward and finish
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 1);
}

#[test]
fn value_string_inequality_reduces_has_reads_bound_owner() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person isa person, has name $name; $name >= "bolton"; $name < "willow";

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_string_bolton = value_parameters
        .register_value(Value::String(Cow::Borrowed(VALUE_STRING_BOLTON)), Span { begin_offset: 0, end_offset: 0 });
    let value_string_willow = value_parameters
        .register_value(Value::String(Cow::Borrowed(VALUE_STRING_WILLOW)), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_name = conjunction.constraints_mut().get_or_declare_variable("var_name", None).unwrap();
    let var_name_type = conjunction.constraints_mut().get_or_declare_variable("var_name_type", None).unwrap();

    let has = conjunction.constraints_mut().add_has(var_person, var_name, None).unwrap().clone();
    let isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_name =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.clone()).unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(
            Vertex::Variable(var_name),
            Vertex::Parameter(value_string_bolton),
            Comparator::GreaterOrEqual,
            None,
        )
        .unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_name), Vertex::Parameter(value_string_willow), Comparator::Less, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&mut translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_person, var_person_type, var_name], []);

    // plan (requires correct type annotations)
    // plan: Isa($person, person)
    //       Has($person, $name) with >= and <
    // Should output:
    //  (person 2, bolton)
    //  (person 2, long...)

    let greater_value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_name),
        rhs: CheckVertex::Parameter(value_string_bolton),
        comparator: Comparator::GreaterOrEqual,
    }
    .map(&mapping);
    let lesser_value_check = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_name),
        rhs: CheckVertex::Parameter(value_string_willow),
        comparator: Comparator::Less,
    }
    .map(&mapping);
    let mut has_instruction = HasInstruction::new(has, Inputs::Single([var_person]), &type_annotations).map(&mapping);
    has_instruction.add_check(greater_value_check);
    has_instruction.add_check(lesser_value_check);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person_type],
            vec![ConstraintInstruction::IsaReverse(
                IsaReverseInstruction::new(isa_person, Inputs::None([]), &type_annotations).map(&mapping),
            )],
            vec![variable_positions[&var_person], variable_positions[&var_person_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_name],
            vec![ConstraintInstruction::Has(has_instruction)],
            vec![variable_positions[&var_person], variable_positions[&var_name]],
            &named_variables,
            3,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);
    assert_eq!(rows.len(), 2);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();
    // 6 seeks: for each person, we should skip directly to the person + owned name.
    // 2 seeks: each of the 2 comparison check filters get the value of the Attribute repeatedly...
    //      TODO: if we embed the Value cache into the AttributeVertex, this could be avoided? Note: only expensive for un-inlined values!
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 8);
    // 2 advance: the iterator matching person 1 will advance twice (once to find the second name, then to fail)
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 2)
}

#[test]
fn intersection_seeks() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $age isa age 10;
    //    $person isa person, has $age;
    //    $person has gov_id $gov_id;

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_int_10 = value_parameters.register_value(Value::Integer(10), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_gov_id = conjunction.constraints_mut().get_or_declare_variable("var_gov_id", None).unwrap();
    let var_gov_id_type = conjunction.constraints_mut().get_or_declare_variable("var_gov_id_type", None).unwrap();
    let var_age = conjunction.constraints_mut().get_or_declare_variable("var_age", None).unwrap();
    let var_age_type = conjunction.constraints_mut().get_or_declare_variable("var_age_type", None).unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age, None).unwrap().clone();
    let has_gov_id = conjunction.constraints_mut().add_has(var_person, var_gov_id, None).unwrap().clone();
    let _isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_gov_id = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_gov_id, var_gov_id_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_gov_id_type, GOV_ID_LABEL.clone()).unwrap();
    let isa_age =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();

    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_age), Vertex::Parameter(value_int_10), Comparator::Equal, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&mut translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_age, var_age_type, var_person, var_gov_id], []);

    // plan (requires correct type annotations)
    // plan:
    // 1. Isa($age, age) value == 10
    // 2. Intersect:
    //       ReverseHas($person, $age) ==> independently produces many people
    //       has($person, $gov_id) ==> unbound this produces many people
    // ---> should output:
    //  (person 1, age 10, gov_id 0)
    //  (person 1, age 10, gov_id 1)
    //  (person 1, age 10, gov_id 2)
    //  (person 1, age 10, gov_id 3)
    //  (person 3, age 10, gov_id 4)
    //  (person 6, age 10, gov_id 5)

    let age_equal_10 = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_age),
        rhs: CheckVertex::Parameter(value_int_10),
        comparator: Comparator::Equal,
    }
    .map(&mapping);
    let mut isa_age = IsaReverseInstruction::new(isa_age, Inputs::None([]), &type_annotations).map(&mapping);
    isa_age.add_check(age_equal_10);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_age_type],
            vec![ConstraintInstruction::IsaReverse(isa_age)],
            vec![variable_positions[&var_age], variable_positions[&var_age_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person],
            vec![
                ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                    has_age,
                    Inputs::Single([var_age]),
                    &type_annotations,
                ))
                .map(&mapping),
                ConstraintInstruction::Has(HasInstruction::new(has_gov_id, Inputs::None([]), &type_annotations))
                    .map(&mapping),
            ],
            vec![
                variable_positions[&var_person],
                variable_positions[&var_gov_id],
                variable_positions[&var_age],
                variable_positions[&var_age_type],
            ],
            &named_variables,
            4,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);
    for row in rows.iter() {
        println!("Row: {}", row.as_ref().unwrap())
    }
    assert_eq!(rows.len(), 6);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();

    // expected evaluation
    //  open initial iterators: 2 seeks... HasReverse[age 10] finds Person 1. Has[unbound] finds Person 1 and attributes.
    //      Has[unbound] Person1 advances 1 past age 10 (first attribute type) to skip to GovID attributes
    //      Now have match!
    //  => advance each iterator: 2 advances... HasReverse is at Person 2. Has is on Person 1 + GovId 1
    //  => Cartesian sub-iterator opened for Has iterator: 1 seek. Has is now back to Person 1, age 10... 1 advance... finds GovID 0
    //     => TODO: there's room for an optimisation here: we don't have to re-open a new iterator when only have 1 cartesian iterator!
    //              we can just advance it linearly through the answers!
    //     Question: will Cartesian re-emit GovID 0?
    //      => Cartesian iterator then gets 3 more GovIds (GovID 1, 2, 3) in Person 1 intersection: 3 advances, plus 1 advance to go past & fail
    //      => TODO: since we simply reopen the cartesian Has[unbound] iterator with no further control
    //               we end up iterating over all Has until we hit Person2.GovId (this hasUnbound filters internally!)
    //               this induces another 7 advances!! (see CartesianIterator::reopen_iterator)
    //  => Has[unbound] seeks to HasReverse's value of Person2: 1 seek (does 1 peek = 1 advances first)... ends up at Person 3 (Person 2 has no gov id)
    //      Has[unbound] at Person 3 will first find age 10, which is skipped with 1 advance. Now at GovId 4.
    //  => HasReverse seeks Has's value of Person3: [1 seek] which actually reduces to 1 advance as it checks the iterator. match!
    //  => advance each iterator: 2 advances...
    //      HasReverse is at Person 4.
    //      Has[unbound] is on Person 4's first attribute, age 10, which is skipped with 1 advance.
    //          Now at Person 5's first attribute, age 10, which is skipped, with 1 advance.. Now at Person 5.GovId
    //  => HasReverse seeks to Has's value Person 5: [1 seek], which actually reduces to 1 advance as it checks the iterator. match!
    //  => advance both iterators: 2 advances... run out of answers in HasReverse. Finished!

    // total seek: 4
    // total advance: 25 (24 ? off by one...)
    // for each person, we should skip directly to the person + owned name
    assert_eq!(storage_counters.get_raw_seek().unwrap(), 4);
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 24);
}

#[test]
fn intersections_seeks_with_extra_values() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $age isa age 12;
    //    $person has $age;
    //    $person has gov_id $gov_id;
    //    $gov_id > 2;

    // add `match $person_3 isa person, has gov_id 4; insert $person_3 has age 12;`
    // this reveals the use of the Value during an intersection seek optimisation
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();
    let person_type = type_manager.get_entity_type(&mut snapshot, &PERSON_LABEL).unwrap().unwrap();
    let age_type = type_manager.get_attribute_type(&mut snapshot, &AGE_LABEL).unwrap().unwrap();
    let gov_id_type = type_manager.get_attribute_type(&mut snapshot, &GOV_ID_LABEL).unwrap().unwrap();
    let gov_id_4 = thing_manager
        .get_attribute_with_value(&snapshot, gov_id_type, Value::Integer(4), StorageCounters::DISABLED)
        .unwrap()
        .unwrap();
    let person_4 = Iterator::next(&mut thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
        &snapshot,
        &gov_id_4,
        &(Bound::Included(ObjectType::Entity(person_type)), Bound::Included(ObjectType::Entity(person_type))),
        StorageCounters::DISABLED,
    ))
    .unwrap()
    .unwrap()
    .0
    .owner();
    let age_12 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(12)).unwrap();
    person_4.set_has_unordered(&mut snapshot, &thing_manager, &age_12, StorageCounters::DISABLED).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    // IR to compute type annotations
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let value_int_12 = value_parameters.register_value(Value::Integer(12), Span { begin_offset: 0, end_offset: 0 });
    let value_int_2 = value_parameters.register_value(Value::Integer(2), Span { begin_offset: 0, end_offset: 0 });
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("var_person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("var_person_type", None).unwrap();
    let var_gov_id = conjunction.constraints_mut().get_or_declare_variable("var_gov_id", None).unwrap();
    let var_gov_id_type = conjunction.constraints_mut().get_or_declare_variable("var_gov_id_type", None).unwrap();
    let var_age = conjunction.constraints_mut().get_or_declare_variable("var_age", None).unwrap();
    let var_age_type = conjunction.constraints_mut().get_or_declare_variable("var_age_type", None).unwrap();

    let has_age = conjunction.constraints_mut().add_has(var_person, var_age, None).unwrap().clone();
    let has_gov_id = conjunction.constraints_mut().add_has(var_person, var_gov_id, None).unwrap().clone();
    let _isa_person = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.clone()).unwrap();
    let _isa_gov_id = conjunction
        .constraints_mut()
        .add_isa(IsaKind::Subtype, var_gov_id, var_gov_id_type.into(), None)
        .unwrap()
        .clone();
    conjunction.constraints_mut().add_label(var_gov_id_type, GOV_ID_LABEL.clone()).unwrap();
    let isa_age =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into(), None).unwrap().clone();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.clone()).unwrap();

    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_age), Vertex::Parameter(value_int_12), Comparator::Equal, None)
        .unwrap();
    conjunction
        .constraints_mut()
        .add_comparison(Vertex::Variable(var_gov_id), Vertex::Parameter(value_int_2), Comparator::Greater, None)
        .unwrap();

    let entry = builder.finish().unwrap();
    let value_parameters = Arc::new(value_parameters);

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let type_annotations = get_type_annotations(&mut translation_context, &entry, &snapshot, &type_manager);

    let (row_vars, variable_positions, mapping, named_variables) =
        position_mapping([var_age, var_age_type, var_person, var_gov_id], []);

    // plan (requires correct type annotations)
    // plan:
    // 1. Isa($age, age) value == 12
    // 2. Intersect:
    //       ReverseHas($person, $age) ==> independently produces many people
    //       Has($person, $gov_id) $gov_id > 2 ==> unbound this produces many people
    //  Note that the interesting case here is that the first iterator would produce Persons, which are used in intersection with the second Has iterator
    //    however, seeking through that iterator to search for a specific person with the required Has should also leverage the value range restriction!
    // ---> should output:
    //  (person 3, age 12, gov_id 4)

    let age_equal_12 = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_age),
        rhs: CheckVertex::Parameter(value_int_12),
        comparator: Comparator::Equal,
    }
    .map(&mapping);
    let gov_id_gt_2 = CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_gov_id),
        rhs: CheckVertex::Parameter(value_int_2),
        comparator: Comparator::Greater,
    }
    .map(&mapping);
    let mut isa_age = IsaReverseInstruction::new(isa_age, Inputs::None([]), &type_annotations).map(&mapping);
    isa_age.add_check(age_equal_12);
    let mut has_gov_id = HasInstruction::new(has_gov_id, Inputs::None([]), &type_annotations).map(&mapping);
    has_gov_id.add_check(gov_id_gt_2);

    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_age_type],
            vec![ConstraintInstruction::IsaReverse(isa_age)],
            vec![variable_positions[&var_age], variable_positions[&var_age_type]],
            &named_variables,
            2,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_person],
            vec![
                ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                    has_age,
                    Inputs::Single([var_age]),
                    &type_annotations,
                ))
                .map(&mapping),
                ConstraintInstruction::Has(has_gov_id),
            ],
            vec![
                variable_positions[&var_person],
                variable_positions[&var_gov_id],
                variable_positions[&var_age],
                variable_positions[&var_age_type],
            ],
            &named_variables,
            4,
        )),
    ];

    let query_profile = QueryProfile::new(true);
    let rows =
        execute_steps(steps, variable_positions, row_vars, storage, thing_manager, value_parameters, &query_profile);
    for row in rows.iter() {
        println!("Row: {}", row.as_ref().unwrap())
    }
    assert_eq!(rows.len(), 1);

    let stage_profiles = query_profile.stage_profiles().read().unwrap();
    let (_, match_profile) = stage_profiles.iter().next().unwrap();
    let intersection_step_profile = match_profile.extend_or_get(1, || String::new());
    let storage_counters = intersection_step_profile.storage_counters();

    // expected evaluation
    //  open initial iterators: 2 seeks... HasReverse[age 12] finds Person 3. Has[unbound] finds Person 1 and attributes
    //      Has[unbound] Person1 advances 4 past age 10, gov id 0, gov id 1, gov id 2, lands on GovId3
    //  Has[unbound] does 1 seek (induces 1 advance) to Person3+GovID2... Now at Person3.GovID4. match!
    //  HasReverse does 1 advance to fail.
    //      Has[unbound] will do 4 advance through Person4+Age10, Person5+Age10|GovID6, Person6+GovID6, plus 1 advance to fail.
    //      ==> TODO: this should be optimisable with a short-circuit, but it is currently impossible due to iterators skipping values internally!

    assert_eq!(storage_counters.get_raw_seek().unwrap(), 3);
    assert_eq!(storage_counters.get_raw_advance().unwrap(), 10)
}
