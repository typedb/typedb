/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use ir::program::{function::Function, program::Program};
use storage::snapshot::ReadableSnapshot;

use super::pattern_type_inference::infer_types_for_block;
use crate::inference::{
    annotated_functions::{AnnotatedCommittedFunctions, AnnotatedUncommittedFunctions},
    annotated_program::AnnotatedProgram,
    type_annotations::{FunctionAnnotations, TypeAnnotations},
    TypeInferenceError,
};

pub(crate) type VertexAnnotations = BTreeMap<Variable, BTreeSet<Type>>;

pub fn infer_types(
    program: Program,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_functions: Arc<AnnotatedCommittedFunctions>,
) -> Result<AnnotatedProgram, TypeInferenceError> {
    let (entry, functions) = program.into_parts();
    let preamble_functions = infer_types_for_functions(functions, snapshot, type_manager, &schema_functions)?;
    let root_tig = infer_types_for_block(snapshot, &entry, type_manager, &schema_functions, Some(&preamble_functions))?;
    let entry_annotations = TypeAnnotations::build(root_tig);
    Ok(AnnotatedProgram::new(entry, entry_annotations, preamble_functions, schema_functions))
}

pub fn infer_types_for_functions(
    functions: Vec<Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_functions: &AnnotatedCommittedFunctions,
) -> Result<AnnotatedUncommittedFunctions, TypeInferenceError> {
    // In the preliminary annotations, functions are annotated based only on the variable categories of the called function.
    let preliminary_annotations_res: Result<Vec<FunctionAnnotations>, TypeInferenceError> = functions
        .iter()
        .map(|function| infer_types_for_function(function, snapshot, type_manager, schema_functions, None))
        .collect();
    let preliminary_annotations = AnnotatedUncommittedFunctions::new(
        functions.into_boxed_slice(),
        preliminary_annotations_res?.into_boxed_slice(),
    );

    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let annotations_res: Result<Vec<FunctionAnnotations>, TypeInferenceError> = preliminary_annotations
        .iter_functions()
        .map(|function| {
            infer_types_for_function(function, snapshot, type_manager, schema_functions, Some(&preliminary_annotations))
        })
        .collect();

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the tigs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    let (ir, annotation) = preliminary_annotations.into_parts();
    let annotated = AnnotatedUncommittedFunctions::new(ir, annotations_res?.into_boxed_slice());

    Ok(annotated)
}

pub fn infer_types_for_function(
    function: &Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_functions: &AnnotatedCommittedFunctions,
    local_functions: Option<&AnnotatedUncommittedFunctions>,
) -> Result<FunctionAnnotations, TypeInferenceError> {
    let root_tig = infer_types_for_block(snapshot, function.block(), type_manager, schema_functions, local_functions)?;
    let body_annotations = TypeAnnotations::build(root_tig);
    let return_annotations = function.return_operation().output_annotations(body_annotations.variable_annotations());
    Ok(FunctionAnnotations { return_annotations, block_annotations: body_annotations })
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet},
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
        },
        program::{
            block::{BlockContext, FunctionalBlock},
            function::{Function, ReturnOperation},
            function_signature::{FunctionID, FunctionSignature},
            program::Program,
        },
    };
    use itertools::Itertools;

    use crate::inference::{
        annotated_functions::AnnotatedCommittedFunctions,
        pattern_type_inference::{
            infer_types_for_block, tests::expected_edge, NestedTypeInferenceGraphDisjunction, TypeInferenceGraph,
        },
        tests::{
            managers,
            schema_consts::{setup_types, LABEL_CAT},
            setup_storage,
        },
        type_annotations::{ConstraintTypeAnnotations, LeftRightAnnotations, LeftRightFilteredAnnotations},
        type_inference::{infer_types, infer_types_for_function, TypeAnnotations},
    };

    pub(crate) fn expected_left_right_annotation(
        constraint: &Constraint<Variable>,
        left: Variable,
        right: Variable,
        types: Vec<(Type, Type)>,
    ) -> ConstraintTypeAnnotations {
        let mut left_to_right = BTreeMap::new();
        let mut right_to_left = BTreeMap::new();
        for (l, r) in &types {
            left_to_right.insert(l.clone(), Vec::new());
            right_to_left.insert(r.clone(), Vec::new());
        }
        for (l, r) in &types {
            left_to_right.get_mut(l).unwrap().push(r.clone());
            right_to_left.get_mut(r).unwrap().push(l.clone());
        }
        ConstraintTypeAnnotations::LeftRight(LeftRightAnnotations::new(left_to_right, right_to_left))
    }

    #[test]
    fn test_translation() {
        let (var_relation, var_role_type, var_player) = (0..3).map(Variable::new).collect_tuple().unwrap();
        let type_rel_0 = Type::Relation(RelationType::build_from_type_id(TypeID::build(0)));
        let type_rel_1 = Type::Relation(RelationType::build_from_type_id(TypeID::build(1)));
        let type_role_0 = Type::RoleType(RoleType::build_from_type_id(TypeID::build(0)));
        let type_role_1 = Type::RoleType(RoleType::build_from_type_id(TypeID::build(1)));
        let type_player_0 = Type::Entity(EntityType::build_from_type_id(TypeID::build(0)));
        let type_player_1 = Type::Relation(RelationType::build_from_type_id(TypeID::build(2)));

        let dummy = FunctionalBlock::builder().finish();
        let constraint1 = Constraint::Links(Links::new(var_relation, var_player, var_role_type));
        let constraint2 = Constraint::Links(Links::new(var_relation, var_player, var_role_type));
        let nested1 = TypeInferenceGraph {
            conjunction: dummy.conjunction(),
            vertices: BTreeMap::from([
                (var_relation, BTreeSet::from([type_rel_0.clone()])),
                (var_role_type, BTreeSet::from([type_role_0.clone()])),
                (var_player, BTreeSet::from([type_player_0.clone()])),
            ]),
            edges: vec![
                expected_edge(
                    &constraint1,
                    var_relation,
                    var_role_type,
                    vec![(type_rel_0.clone(), type_role_0.clone())],
                ),
                expected_edge(
                    &constraint1,
                    var_player,
                    var_role_type,
                    vec![(type_player_0.clone(), type_role_0.clone())],
                ),
            ],
            nested_disjunctions: vec![],
            nested_negations: vec![],
            nested_optionals: vec![],
        };
        let vertex_annotations = BTreeMap::from([
            (var_relation, BTreeSet::from([type_rel_1.clone()])),
            (var_role_type, BTreeSet::from([type_role_1.clone()])),
            (var_player, BTreeSet::from([type_player_1.clone()])),
        ]);
        let shared_variables: BTreeSet<Variable> = vertex_annotations.keys().copied().collect();
        let nested2 = TypeInferenceGraph {
            conjunction: dummy.conjunction(),
            vertices: vertex_annotations.clone(),
            edges: vec![
                expected_edge(
                    &constraint1,
                    var_relation,
                    var_role_type,
                    vec![(type_rel_1.clone(), type_role_1.clone())],
                ),
                expected_edge(
                    &constraint1,
                    var_player,
                    var_role_type,
                    vec![(type_player_1.clone(), type_role_1.clone())],
                ),
            ],
            nested_disjunctions: vec![],
            nested_negations: vec![],
            nested_optionals: vec![],
        };
        let tig = TypeInferenceGraph {
            conjunction: dummy.conjunction(),
            vertices: BTreeMap::from([
                (var_relation, BTreeSet::from([type_rel_0.clone(), type_rel_1.clone()])),
                (var_role_type, BTreeSet::from([type_role_0.clone(), type_role_1.clone()])),
                (var_player, BTreeSet::from([type_player_0.clone(), type_player_1.clone()])),
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
            filters_on_right: Arc::new(BTreeMap::from([(type_player_0.clone(), HashSet::from([type_role_0.clone()]))])),
            right_to_left: Arc::new(BTreeMap::from([(type_player_0.clone(), vec![type_rel_0.clone()])])),
            filters_on_left: Arc::new(BTreeMap::from([(type_rel_0.clone(), HashSet::from([type_role_0.clone()]))])),
        };
        let lra2 = LeftRightFilteredAnnotations {
            left_to_right: Arc::new(BTreeMap::from([(type_rel_1.clone(), vec![type_player_1.clone()])])),
            filters_on_right: Arc::new(BTreeMap::from([(type_player_1.clone(), HashSet::from([type_role_1.clone()]))])),
            right_to_left: Arc::new(BTreeMap::from([(type_player_1.clone(), vec![type_rel_1.clone()])])),
            filters_on_left: Arc::new(BTreeMap::from([(type_rel_1.clone(), HashSet::from([type_role_1.clone()]))])),
        };
        let expected_annotations = TypeAnnotations::new(
            HashMap::from([
                (var_relation, Arc::new(HashSet::from([type_rel_0.clone(), type_rel_1.clone()]))),
                (var_role_type, Arc::new(HashSet::from([type_role_0.clone(), type_role_1.clone()]))),
                (var_player, Arc::new(HashSet::from([type_player_0.clone(), type_player_1.clone()]))),
            ]),
            HashMap::from([
                (constraint1, ConstraintTypeAnnotations::LeftRightFiltered(lra1)),
                (constraint2, ConstraintTypeAnnotations::LeftRightFiltered(lra2)),
            ]),
        );
        assert_eq!(expected_annotations.variable_annotations(), type_annotations.variable_annotations());
        assert_eq!(expected_annotations.constraint_annotations(), type_annotations.constraint_annotations());
    }

    #[test]
    fn test_functions() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), (type_fears, _, _)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);
        let animal_types = [type_animal.clone(), type_cat.clone(), type_dog.clone()];
        let object_types = [type_animal.clone(), type_cat.clone(), type_dog.clone(), type_fears.clone()];

        let (with_no_cache, with_local_cache, with_schema_cache) = [
            FunctionID::Preamble(0),
            FunctionID::Preamble(0),
            FunctionID::Schema(DefinitionKey::build(Prefix::DefinitionFunction, DefinitionID::build(0))),
        ]
        .iter()
        .map(|function_id| {
            let mut builder = FunctionalBlock::builder();
            let mut f_conjunction = builder.conjunction_mut();
            let f_var_animal = f_conjunction.get_or_declare_variable("called_animal").unwrap();
            let f_var_animal_type = f_conjunction.get_or_declare_variable("called_animal_type").unwrap();
            let f_var_name = f_conjunction.get_or_declare_variable("called_name").unwrap();
            f_conjunction.constraints_mut().add_label(f_var_animal_type, LABEL_CAT).unwrap();
            f_conjunction.constraints_mut().add_isa(IsaKind::Subtype, f_var_animal, f_var_animal_type).unwrap();
            f_conjunction.constraints_mut().add_has(f_var_animal, f_var_name).unwrap();
            let f_ir = Function::new(builder.finish(), vec![], ReturnOperation::Stream(vec![f_var_animal]));

            let mut builder = FunctionalBlock::builder();
            let mut conjunction = builder.conjunction_mut();
            let context = BlockContext::new();
            let var_animal = conjunction.get_or_declare_variable("animal").unwrap();

            let callee_signature = FunctionSignature::new(
                function_id.clone(),
                vec![],
                vec![(VariableCategory::Object, VariableOptionality::Required)],
                true,
            );
            conjunction.constraints_mut().add_function_binding(vec![var_animal], &callee_signature, vec![]).unwrap();
            let entry = builder.finish();
            (entry, f_ir)
        })
        .collect_tuple()
        .unwrap();

        let snapshot = storage.open_snapshot_read();
        let f_annotations = {
            let (_, f_ir) = &with_no_cache;
            let f_annotations =
                infer_types_for_function(f_ir, &snapshot, &type_manager, &AnnotatedCommittedFunctions::empty(), None)
                    .unwrap();
            let isa = f_ir.block().conjunction().constraints()[0].clone();
            let f_var_animal = f_ir.block().context().get_variable("called_animal").unwrap();
            let f_var_animal_type = f_ir.block().context().get_variable("called_animal_type").unwrap();
            let f_var_name = f_ir.block().context().get_variable("called_name").unwrap();
            assert_eq!(
                *f_annotations.block_annotations.variable_annotations(),
                HashMap::from([
                    (f_var_animal, Arc::new(HashSet::from([type_cat.clone()]))),
                    (f_var_animal_type, Arc::new(HashSet::from([type_cat.clone()]))),
                    (f_var_name, Arc::new(HashSet::from([type_catname.clone()])))
                ])
            );
            f_annotations
        };

        {
            // Local inference only
            let (entry, _) = with_no_cache;
            let var_animal = entry.context().get_variable("animal").unwrap();
            let annotations_without_schema_cache = TypeAnnotations::build(
                infer_types_for_block(&snapshot, &entry, &type_manager, &AnnotatedCommittedFunctions::empty(), None)
                    .unwrap(),
            );
            assert_eq!(
                *annotations_without_schema_cache.variable_annotations(),
                HashMap::from([(var_animal, Arc::new(HashSet::from(object_types.clone()))),])
            );
        }
        {
            // With schema cache
            let (entry, f_ir) = with_local_cache;
            let var_animal = entry.context().get_variable("animal").unwrap();
            let annotations_with_local_cache = infer_types(
                Program::new(entry, vec![f_ir]),
                &snapshot,
                &type_manager,
                Arc::new(AnnotatedCommittedFunctions::empty()),
            )
            .unwrap();
            assert_eq!(
                *annotations_with_local_cache.entry_annotations.variable_annotations(),
                HashMap::from([(var_animal, Arc::new(HashSet::from([type_cat.clone()])))]),
            );
        }

        {
            // With schema cache
            let (entry, f_ir) = with_schema_cache;
            let var_animal = entry.context().get_variable("animal").unwrap();
            let f_id = FunctionID::Schema(DefinitionKey::build(Prefix::DefinitionFunction, DefinitionID::build(0)));
            let schema_cache =
                AnnotatedCommittedFunctions::new(Box::new([Some(f_ir)]), Box::new([Some(f_annotations)]));
            let annotations_with_schema_cache =
                infer_types(Program::new(entry, vec![]), &snapshot, &type_manager, Arc::new(schema_cache)).unwrap();
            assert_eq!(
                *annotations_with_schema_cache.entry_annotations.variable_annotations(),
                HashMap::from([(var_animal, Arc::new(HashSet::from([type_cat.clone()])))]),
            );
        }
    }
}
