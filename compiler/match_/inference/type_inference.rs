/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::ValueType;
use ir::{
    pattern::Vertex,
    program::{
        block::{FunctionalBlock, VariableRegistry},
        function::Function,
    },
};
use storage::snapshot::ReadableSnapshot;

use crate::match_::inference::{
    annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
    pattern_type_inference::infer_types_for_block,
    type_annotations::{FunctionAnnotations, TypeAnnotations},
    FunctionTypeInferenceError, TypeInferenceError,
};

pub fn infer_types_for_functions(
    functions: Vec<Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
) -> Result<AnnotatedUnindexedFunctions, FunctionTypeInferenceError> {
    // In the preliminary annotations, functions are annotated based only on the variable categories of the called function.
    let preliminary_annotations_res: Result<Vec<FunctionAnnotations>, FunctionTypeInferenceError> = functions
        .iter()
        .map(|function| infer_types_for_function(function, snapshot, type_manager, indexed_annotated_functions, None))
        .collect();
    let preliminary_annotations =
        AnnotatedUnindexedFunctions::new(functions.into_boxed_slice(), preliminary_annotations_res?.into_boxed_slice());

    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let annotations_res = preliminary_annotations
        .iter_functions()
        .map(|function| {
            infer_types_for_function(
                function,
                snapshot,
                type_manager,
                indexed_annotated_functions,
                Some(&preliminary_annotations),
            )
        })
        .collect::<Result<Vec<FunctionAnnotations>, FunctionTypeInferenceError>>()?;

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the tigs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    let (ir, _) = preliminary_annotations.into_parts();
    let annotated = AnnotatedUnindexedFunctions::new(ir, annotations_res.into_boxed_slice());
    Ok(annotated)
}

pub fn infer_types_for_function(
    function: &Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<FunctionAnnotations, FunctionTypeInferenceError> {
    let root_tig = infer_types_for_block(
        snapshot,
        function.block(),
        function.variable_registry(),
        type_manager,
        &BTreeMap::new(),
        indexed_annotated_functions,
        local_functions,
    )
    .map_err(|err| FunctionTypeInferenceError::TypeInference {
        name: function.name().to_string(),
        typedb_source: err,
    })?;
    let body_annotations = TypeAnnotations::build(root_tig);
    let return_annotations = function.return_operation().output_annotations(body_annotations.vertex_annotations());
    Ok(FunctionAnnotations { return_annotations, block_annotations: body_annotations })
}

pub fn infer_types_for_match_block(
    match_block: &FunctionalBlock,
    variable_registry: &VariableRegistry,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    previous_stage_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<answer::Type>>>,
    annotated_schema_functions: &IndexedAnnotatedFunctions,
    annotated_preamble_functions: &AnnotatedUnindexedFunctions,
) -> Result<TypeAnnotations, TypeInferenceError> {
    let root_tig = infer_types_for_block(
        snapshot,
        match_block,
        variable_registry,
        type_manager,
        previous_stage_variable_annotations,
        annotated_schema_functions,
        Some(annotated_preamble_functions),
    )?;
    let type_annotations = TypeAnnotations::build(root_tig);
    debug_assert!(match_block
        .scope_context()
        .referenced_variables() // FIXME vertices?
        .all(|var| type_annotations.vertex_annotations_of(&Vertex::Variable(var)).is_some()));
    Ok(type_annotations)
}

pub fn resolve_value_types(
    types: &BTreeSet<answer::Type>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<HashSet<ValueType>, TypeInferenceError> {
    types
        .iter()
        .map(|type_| match type_ {
            Type::Attribute(attribute_type) => {
                match attribute_type.get_value_type_without_source(snapshot, type_manager) {
                    Ok(None) => {
                        let label = match type_.get_label(snapshot, type_manager) {
                            Ok(label) => label.scoped_name().as_str().to_owned(),
                            Err(_) => format!("could_not_resolve__{type_}"),
                        };
                        Err(TypeInferenceError::AttemptedToResolveValueTypeOfAttributeWithoutOne { label })
                    }
                    Ok(Some(value_type)) => Ok(value_type),
                    Err(source) => Err(TypeInferenceError::ConceptRead { source }),
                }
            }
            _ => {
                let label = match type_.get_label(snapshot, type_manager) {
                    Ok(label) => label.scoped_name().as_str().to_owned(),
                    Err(_) => format!("could_not_resolve__{type_}"),
                };
                Err(TypeInferenceError::AttemptedToResolveValueTypeOfNonAttributeType { label })
            }
        })
        .collect::<Result<HashSet<_>, TypeInferenceError>>()
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        sync::Arc,
    };

    use answer::{variable::Variable, Type};
    use concept::type_::{entity_type::EntityType, relation_type::RelationType, role_type::RoleType};
    use encoding::{
        graph::{
            definition::definition_key::{DefinitionID, DefinitionKey},
            type_::vertex::{PrefixedTypeVertexEncoding, TypeID},
        },
        layout::prefix::Prefix,
    };
    use ir::{
        pattern::{
            constraint::{Constraint, IsaKind, Links},
            variable_category::{VariableCategory, VariableOptionality},
            Vertex,
        },
        program::{
            block::{FunctionalBlock, VariableRegistry},
            function::{Function, ReturnOperation},
            function_signature::{FunctionID, FunctionSignature},
        },
        translation::TranslationContext,
    };
    use itertools::Itertools;

    use crate::match_::inference::{
        annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        pattern_type_inference::{
            infer_types_for_block, tests::expected_edge, NestedTypeInferenceGraphDisjunction, TypeInferenceGraph,
            VertexAnnotations,
        },
        tests::{
            managers,
            schema_consts::{setup_types, LABEL_CAT},
            setup_storage,
        },
        type_annotations::{ConstraintTypeAnnotations, LeftRightFilteredAnnotations},
        type_inference::{infer_types_for_function, infer_types_for_match_block, TypeAnnotations},
    };

    #[test]
    fn test_translation() {
        let (var_relation, var_role_type, var_player) = (0..3).map(Variable::new).collect_tuple().unwrap();
        let type_rel_0 = Type::Relation(RelationType::build_from_type_id(TypeID::build(0)));
        let type_rel_1 = Type::Relation(RelationType::build_from_type_id(TypeID::build(1)));
        let type_role_0 = Type::RoleType(RoleType::build_from_type_id(TypeID::build(0)));
        let type_role_1 = Type::RoleType(RoleType::build_from_type_id(TypeID::build(1)));
        let type_player_0 = Type::Entity(EntityType::build_from_type_id(TypeID::build(0)));
        let type_player_1 = Type::Relation(RelationType::build_from_type_id(TypeID::build(2)));

        let mut translation_context = TranslationContext::new();
        let dummy = FunctionalBlock::builder(translation_context.next_block_context()).finish();
        let constraint1 = Constraint::Links(Links::new(var_relation, var_player, var_role_type));
        let constraint2 = Constraint::Links(Links::new(var_relation, var_player, var_role_type));
        let nested1 = TypeInferenceGraph {
            conjunction: dummy.conjunction(),
            vertices: VertexAnnotations::from([
                (var_relation.into(), BTreeSet::from([type_rel_0.clone()])),
                (var_role_type.into(), BTreeSet::from([type_role_0.clone()])),
                (var_player.into(), BTreeSet::from([type_player_0.clone()])),
            ]),
            edges: vec![
                expected_edge(
                    &constraint1,
                    var_relation.into(),
                    var_role_type.into(),
                    vec![(type_rel_0.clone(), type_role_0.clone())],
                ),
                expected_edge(
                    &constraint1,
                    var_player.into(),
                    var_role_type.into(),
                    vec![(type_player_0.clone(), type_role_0.clone())],
                ),
            ],
            nested_disjunctions: vec![],
            nested_negations: vec![],
            nested_optionals: vec![],
        };
        let vertex_annotations = VertexAnnotations::from([
            (var_relation.into(), BTreeSet::from([type_rel_1.clone()])),
            (var_role_type.into(), BTreeSet::from([type_role_1.clone()])),
            (var_player.into(), BTreeSet::from([type_player_1.clone()])),
        ]);
        let shared_variables = vertex_annotations.keys().filter_map(Vertex::as_variable).collect();
        let nested2 = TypeInferenceGraph {
            conjunction: dummy.conjunction(),
            vertices: vertex_annotations.clone(),
            edges: vec![
                expected_edge(
                    &constraint1,
                    var_relation.into(),
                    var_role_type.into(),
                    vec![(type_rel_1.clone(), type_role_1.clone())],
                ),
                expected_edge(
                    &constraint1,
                    var_player.into(),
                    var_role_type.into(),
                    vec![(type_player_1.clone(), type_role_1.clone())],
                ),
            ],
            nested_disjunctions: vec![],
            nested_negations: vec![],
            nested_optionals: vec![],
        };
        let tig = TypeInferenceGraph {
            conjunction: dummy.conjunction(),
            vertices: VertexAnnotations::from([
                (var_relation.into(), BTreeSet::from([type_rel_0.clone(), type_rel_1.clone()])),
                (var_role_type.into(), BTreeSet::from([type_role_0.clone(), type_role_1.clone()])),
                (var_player.into(), BTreeSet::from([type_player_0.clone(), type_player_1.clone()])),
            ]),
            edges: vec![],
            nested_disjunctions: vec![NestedTypeInferenceGraphDisjunction {
                disjunction: vec![nested1, nested2],
                shared_variables,
                shared_vertex_annotations: vertex_annotations,
            }],
            nested_negations: vec![],
            nested_optionals: vec![],
        };
        let type_annotations = TypeAnnotations::build(tig);

        let lra1 = LeftRightFilteredAnnotations {
            left_to_right: Arc::new(BTreeMap::from([(type_rel_0.clone(), vec![type_player_0.clone()])])),
            filters_on_right: Arc::new(BTreeMap::from([(
                type_player_0.clone(),
                BTreeSet::from([type_role_0.clone()]),
            )])),
            right_to_left: Arc::new(BTreeMap::from([(type_player_0.clone(), vec![type_rel_0.clone()])])),
            filters_on_left: Arc::new(BTreeMap::from([(type_rel_0.clone(), BTreeSet::from([type_role_0.clone()]))])),
        };
        let lra2 = LeftRightFilteredAnnotations {
            left_to_right: Arc::new(BTreeMap::from([(type_rel_1.clone(), vec![type_player_1.clone()])])),
            filters_on_right: Arc::new(BTreeMap::from([(
                type_player_1.clone(),
                BTreeSet::from([type_role_1.clone()]),
            )])),
            right_to_left: Arc::new(BTreeMap::from([(type_player_1.clone(), vec![type_rel_1.clone()])])),
            filters_on_left: Arc::new(BTreeMap::from([(type_rel_1.clone(), BTreeSet::from([type_role_1.clone()]))])),
        };
        let expected_annotations = TypeAnnotations::new(
            BTreeMap::from([
                (var_relation.into(), Arc::new(BTreeSet::from([type_rel_0.clone(), type_rel_1.clone()]))),
                (var_role_type.into(), Arc::new(BTreeSet::from([type_role_0.clone(), type_role_1.clone()]))),
                (var_player.into(), Arc::new(BTreeSet::from([type_player_0.clone(), type_player_1.clone()]))),
            ]),
            HashMap::from([
                (constraint1, ConstraintTypeAnnotations::LeftRightFiltered(lra1)),
                (constraint2, ConstraintTypeAnnotations::LeftRightFiltered(lra2)),
            ]),
        );
        assert_eq!(expected_annotations.vertex_annotations(), type_annotations.vertex_annotations());
        assert_eq!(expected_annotations.constraint_annotations(), type_annotations.constraint_annotations());
    }

    #[test]
    fn test_functions() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, _), (type_fears, _, _)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);
        let object_types = [type_animal.clone(), type_cat.clone(), type_dog.clone(), type_fears.clone()];

        let (with_no_cache, with_local_cache, with_schema_cache) = [
            FunctionID::Preamble(0),
            FunctionID::Preamble(0),
            FunctionID::Schema(DefinitionKey::build(Prefix::DefinitionFunction, DefinitionID::build(0))),
        ]
        .iter()
        .map(|function_id| {
            let mut function_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(function_context.next_block_context());
            let mut f_conjunction = builder.conjunction_mut();
            let f_var_animal = f_conjunction.get_or_declare_variable("called_animal").unwrap();
            let f_var_animal_type = f_conjunction.get_or_declare_variable("called_animal_type").unwrap();
            let f_var_name = f_conjunction.get_or_declare_variable("called_name").unwrap();
            f_conjunction.constraints_mut().add_label(f_var_animal_type, LABEL_CAT.scoped_name().as_str()).unwrap();
            f_conjunction.constraints_mut().add_isa(IsaKind::Subtype, f_var_animal, f_var_animal_type.into()).unwrap();
            f_conjunction.constraints_mut().add_has(f_var_animal, f_var_name).unwrap();
            let f_ir = Function::new(
                "fn_test",
                builder.finish(),
                function_context.variable_registry,
                vec![],
                ReturnOperation::Stream(vec![f_var_animal]),
            );

            let mut entry_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(entry_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let var_animal = conjunction.get_or_declare_variable("animal").unwrap();

            let callee_signature = FunctionSignature::new(
                function_id.clone(),
                vec![],
                vec![(VariableCategory::Object, VariableOptionality::Required)],
                true,
            );
            conjunction
                .constraints_mut()
                .add_function_binding(vec![var_animal], &callee_signature, vec![], "test_fn")
                .unwrap();
            let entry = builder.finish();
            (entry, entry_context, f_ir)
        })
        .collect_tuple()
        .unwrap();

        let snapshot = storage.open_snapshot_read();

        {
            // Local inference only
            let (entry, entry_context, _) = with_no_cache;
            let var_animal = var_from_registry(&entry_context.variable_registry, "animal").unwrap();
            let annotations_without_schema_cache = TypeAnnotations::build(
                infer_types_for_block(
                    &snapshot,
                    &entry,
                    &entry_context.variable_registry,
                    &type_manager,
                    &BTreeMap::new(),
                    &IndexedAnnotatedFunctions::empty(),
                    None,
                )
                .unwrap(),
            );
            assert_eq!(
                *annotations_without_schema_cache.vertex_annotations(),
                BTreeMap::from([(var_animal.into(), Arc::new(BTreeSet::from(object_types.clone())))])
            );
        }

        {
            // With schema cache
            let (entry, entry_context, f_ir) = with_local_cache;

            let f_annotations =
                infer_types_for_function(&f_ir, &snapshot, &type_manager, &IndexedAnnotatedFunctions::empty(), None)
                    .unwrap();
            let f_var_animal = var_from_registry(f_ir.variable_registry(), "called_animal").unwrap();
            let f_var_animal_type = var_from_registry(f_ir.variable_registry(), "called_animal_type").unwrap();
            let f_var_name = var_from_registry(f_ir.variable_registry(), "called_name").unwrap();

            assert_eq!(
                *f_annotations.block_annotations.vertex_annotations(),
                BTreeMap::from([
                    (f_var_animal.into(), Arc::new(BTreeSet::from([type_cat.clone()]))),
                    (f_var_animal_type.into(), Arc::new(BTreeSet::from([type_cat.clone()]))),
                    (f_var_name.into(), Arc::new(BTreeSet::from([type_catname.clone(), type_name.clone()])))
                ])
            );

            let var_animal = var_from_registry(&entry_context.variable_registry, "animal").unwrap();
            let local_cache = AnnotatedUnindexedFunctions::new(Box::new([f_ir]), Box::new([f_annotations]));
            let entry_annotations = infer_types_for_match_block(
                &entry,
                &entry_context.variable_registry,
                &snapshot,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                &local_cache,
            )
            .unwrap();
            assert_eq!(
                entry_annotations.vertex_annotations(),
                &BTreeMap::from([(var_animal.into(), Arc::new(BTreeSet::from([type_cat.clone()])))]),
            );
        }

        {
            // With schema cache
            let (entry, entry_context, f_ir) = with_schema_cache;

            let f_annotations =
                infer_types_for_function(&f_ir, &snapshot, &type_manager, &IndexedAnnotatedFunctions::empty(), None)
                    .unwrap();
            let f_var_animal = var_from_registry(f_ir.variable_registry(), "called_animal").unwrap();
            let f_var_animal_type = var_from_registry(f_ir.variable_registry(), "called_animal_type").unwrap();
            let f_var_name = var_from_registry(f_ir.variable_registry(), "called_name").unwrap();

            assert_eq!(
                *f_annotations.block_annotations.vertex_annotations(),
                BTreeMap::from([
                    (f_var_animal.into(), Arc::new(BTreeSet::from([type_cat.clone()]))),
                    (f_var_animal_type.into(), Arc::new(BTreeSet::from([type_cat.clone()]))),
                    (f_var_name.into(), Arc::new(BTreeSet::from([type_catname.clone(), type_name.clone()])))
                ])
            );

            let var_animal = var_from_registry(&entry_context.variable_registry, "animal").unwrap();
            let schema_cache = IndexedAnnotatedFunctions::new(Box::new([Some(f_ir)]), Box::new([Some(f_annotations)]));
            let entry_annotations = infer_types_for_match_block(
                &entry,
                &entry_context.variable_registry,
                &snapshot,
                &type_manager,
                &BTreeMap::new(),
                &schema_cache,
                &AnnotatedUnindexedFunctions::empty(),
            )
            .unwrap();
            assert_eq!(
                *entry_annotations.vertex_annotations(),
                BTreeMap::from([(var_animal.into(), Arc::new(BTreeSet::from([type_cat.clone()])))]),
            );
        }

        fn var_from_registry(registry: &VariableRegistry, name: &str) -> Option<Variable> {
            registry.variable_names().iter().find(|(_, n)| n.as_str() == name).map(|(v, _)| *v)
        }
    }
}
