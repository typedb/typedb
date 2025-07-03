/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeSet, HashSet};

use answer::Type;
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::ValueType;
use storage::snapshot::ReadableSnapshot;

use crate::annotation::TypeInferenceError;

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
                        Err(TypeInferenceError::InternalAttributeTypeWithoutValueType { label })
                    }
                    Ok(Some(value_type)) => Ok(value_type),
                    Err(source) => Err(TypeInferenceError::ConceptRead { typedb_source: source }),
                }
            }
            _ => {
                let label = match type_.get_label(snapshot, type_manager) {
                    Ok(label) => label.scoped_name().as_str().to_owned(),
                    Err(_) => format!("could_not_resolve__{type_}"),
                };
                Err(TypeInferenceError::InternalValueTypeOfNonAttributeType { label })
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
    use assert as assert_true;
    use encoding::{
        graph::definition::definition_key::{DefinitionID, DefinitionKey},
        layout::prefix::Prefix,
        value::label::Label,
    };
    use ir::{
        pattern::{
            constraint::{Constraint, IsaKind, SubKind},
            variable_category::{VariableCategory, VariableOptionality},
            Vertex,
        },
        pipeline::{
            block::Block,
            function::{Function, FunctionBody, ReturnOperation},
            function_signature::{FunctionID, FunctionSignature},
            ParameterRegistry, VariableRegistry,
        },
        translation::{pipeline::TranslatedStage, PipelineTranslationContext},
    };
    use itertools::Itertools;

    use crate::annotation::{
        function::{
            annotate_named_function, AnnotatedFunctionSignature, AnnotatedFunctionSignaturesImpl,
            EmptyAnnotatedFunctionSignatures,
        },
        match_inference::{
            compute_type_inference_graph, infer_types, prune_types, NestedTypeInferenceGraphDisjunction,
            TypeInferenceEdge, TypeInferenceGraph, VertexAnnotations,
        },
        pipeline::AnnotatedStage,
        tests::{
            managers,
            schema_consts::{
                setup_types, LABEL_ANIMAL, LABEL_CAT, LABEL_CATNAME, LABEL_DOG, LABEL_DOGNAME, LABEL_FEARS,
                LABEL_HAS_FEAR, LABEL_IS_FEARED, LABEL_NAME,
            },
            setup_storage,
        },
        type_seeder::TypeGraphSeedingContext,
        TypeInferenceError,
    };

    #[test]
    fn test_functions() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, type_dog), (_, type_catname, type_dogname), (type_fears, _, _)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);
        // let concrete_object_types = [type_cat, type_dog, type_fears];
        let all_concrete_instance_types = [type_cat, type_dog, type_fears, type_catname, type_dogname];

        let (with_no_cache, with_local_cache, _with_schema_cache) = [
            FunctionID::Preamble(0),
            FunctionID::Preamble(0),
            FunctionID::Schema(DefinitionKey::build(Prefix::DefinitionFunction, DefinitionID::build(0))),
        ]
        .iter()
        .map(|function_id| {
            // with fun fn_test() -> animal: match $called_animal isa cat, has $called_name; return { $called_animal };
            // match $animal = fn_test();
            let mut function_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(function_context.new_block_builder_context(&mut value_parameters));
            let mut f_conjunction = builder.conjunction_mut();
            let f_var_animal = f_conjunction.constraints_mut().get_or_declare_variable("called_animal", None).unwrap();
            let f_var_animal_type =
                f_conjunction.constraints_mut().get_or_declare_variable("called_animal_type", None).unwrap();
            let f_var_name = f_conjunction.constraints_mut().get_or_declare_variable("called_name", None).unwrap();
            f_conjunction.constraints_mut().add_label(f_var_animal_type, LABEL_CAT.clone()).unwrap();
            f_conjunction
                .constraints_mut()
                .add_isa(IsaKind::Subtype, f_var_animal, f_var_animal_type.into(), None)
                .unwrap();
            f_conjunction.constraints_mut().add_has(f_var_animal, f_var_name, None).unwrap();
            let function_block = builder.finish().unwrap();
            let f_ir = Function::new(
                "fn_test",
                function_context,
                value_parameters,
                vec![],
                Some(vec![]),
                None,
                FunctionBody::new(
                    vec![TranslatedStage::Match { block: function_block, source_span: None }],
                    ReturnOperation::Stream(vec![f_var_animal], None),
                ),
            );

            let mut entry_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(entry_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let var_animal = conjunction.constraints_mut().get_or_declare_variable("animal", None).unwrap();

            let callee_signature = FunctionSignature::new(
                function_id.clone(),
                vec![],
                vec![(VariableCategory::Object, VariableOptionality::Required)],
                true,
            );
            conjunction
                .constraints_mut()
                .add_function_binding(vec![var_animal], &callee_signature, vec![], "test_fn", None)
                .unwrap();
            let entry = builder.finish().unwrap();
            (entry, entry_context, f_ir)
        })
        .collect_tuple()
        .unwrap();

        let snapshot = storage.open_snapshot_read();

        {
            // Local inference only
            // match $animal = fn_test();
            let (entry, entry_context, _) = with_no_cache;
            let var_animal = var_from_registry(&entry_context.variable_registry, "animal").unwrap();
            let annotations_without_schema_cache = infer_types(
                &snapshot,
                &entry,
                &entry_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap()
            .into_parts()
            .into_iter()
            .exactly_one()
            .unwrap()
            .1;
            assert_eq!(
                *annotations_without_schema_cache.vertex_annotations(),
                BTreeMap::from([(var_animal.into(), Arc::new(BTreeSet::from(all_concrete_instance_types)))])
            );
        }

        {
            // Inference for preamble function
            // with fun fn_test() -> animal: match $called_animal isa cat, has $called_name; return { $called_animal };
            let (entry, entry_context, mut f_ir) = with_local_cache;

            let f_annotations =
                annotate_named_function(&mut f_ir, &snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures)
                    .unwrap();
            let f_var_animal =
                var_from_registry(&f_ir.translation_context().variable_registry, "called_animal").unwrap();
            let f_var_animal_type =
                var_from_registry(&f_ir.translation_context().variable_registry, "called_animal_type").unwrap();
            let f_var_name = var_from_registry(&f_ir.translation_context().variable_registry, "called_name").unwrap();
            let AnnotatedStage::Match { block, block_annotations, .. } = &f_annotations.stages[0] else {
                unreachable!()
            };
            assert_eq!(
                block_annotations.type_annotations_of(block.conjunction()).unwrap().vertex_annotations(),
                &BTreeMap::from([
                    (f_var_animal.into(), Arc::new(BTreeSet::from([type_cat]))),
                    (f_var_animal_type.into(), Arc::new(BTreeSet::from([type_cat]))),
                    (f_var_name.into(), Arc::new(BTreeSet::from([type_catname]))),
                    (Vertex::Label(LABEL_CAT), Arc::new(BTreeSet::from([type_cat]))),
                ])
            );

            // Inference with inference of function cached
            // with fun fn_test() -> animal: match $called_animal isa cat, has $called_name; return { $called_animal };
            // match $animal = fn_test();
            let var_animal = var_from_registry(&entry_context.variable_registry, "animal").unwrap();
            let variable_registry = &entry_context.variable_registry;
            let previous_stage_variable_annotations = &BTreeMap::new();
            let empty_schema_functions = HashMap::<DefinitionKey, AnnotatedFunctionSignature>::new();
            let preamble_functions = vec![f_annotations];
            let function_annotations =
                AnnotatedFunctionSignaturesImpl::new(&empty_schema_functions, &preamble_functions);
            let entry_annotations = infer_types(
                &snapshot,
                &entry,
                variable_registry,
                &type_manager,
                previous_stage_variable_annotations,
                &function_annotations,
                false,
            )
            .unwrap();
            assert_eq!(
                entry_annotations.type_annotations_of(entry.conjunction()).unwrap().vertex_annotations(),
                &BTreeMap::from([(var_animal.into(), Arc::new(BTreeSet::from([type_cat])))]),
            );
        }

        // { // TODO: We changed the function cache so the ID has to be DefinitionKey
        //     // With schema cache
        //     let (entry, entry_context, mut f_ir) = with_schema_cache;
        //
        //     let f_annotations = annotate_function(
        //         &mut f_ir,
        //         &snapshot,
        //         &type_manager,
        //         Some(&IndexedAnnotatedFunctions::empty()),
        //         None,
        //         None,
        //         None,
        //     )
        //     .unwrap();
        //     let f_var_animal =
        //         var_from_registry(&f_ir.translation_context().variable_registry, "called_animal").unwrap();
        //     let f_var_animal_type =
        //         var_from_registry(&f_ir.translation_context().variable_registry, "called_animal_type").unwrap();
        //     let f_var_name = var_from_registry(&f_ir.translation_context().variable_registry, "called_name").unwrap();
        //
        //     let AnnotatedStage::Match { block_annotations, .. } = &f_annotations.stages[0] else { unreachable!() };
        //     assert_eq!(
        //         block_annotations.vertex_annotations(),
        //         &BTreeMap::from([
        //             (f_var_animal.into(), Arc::new(BTreeSet::from([type_cat.clone()]))),
        //             (f_var_animal_type.into(), Arc::new(BTreeSet::from([type_cat.clone()]))),
        //             (f_var_name.into(), Arc::new(BTreeSet::from([type_catname.clone(), type_name.clone()])))
        //         ])
        //     );
        //
        //     let var_animal = var_from_registry(&entry_context.variable_registry, "animal").unwrap();
        //     let schema_cache = IndexedAnnotatedFunctions::new(vec![f_annotations]);
        //     let variable_registry = &entry_context.variable_registry;
        //     let previous_stage_variable_annotations = &BTreeMap::new();
        //     let annotated_preamble_functions = &AnnotatedUnindexedFunctions::empty();
        //     let entry_annotations = infer_types(
        //         &snapshot,
        //         &entry,
        //         variable_registry,
        //         &type_manager,
        //         previous_stage_variable_annotations,
        //         &schema_cache,
        //         Some(annotated_preamble_functions),
        //     )
        //     .unwrap();
        //     assert_eq!(
        //         *entry_annotations.vertex_annotations(),
        //         BTreeMap::from([(var_animal.into(), Arc::new(BTreeSet::from([type_cat.clone()])))]),
        //     );
        // }

        fn var_from_registry(registry: &VariableRegistry, name: &str) -> Option<Variable> {
            registry.variable_names().iter().find(|(_, n)| n.as_str() == name).map(|(v, _)| *v)
        }
    }

    pub(crate) fn expected_edge(
        constraint: &Constraint<Variable>,
        left: Vertex<Variable>,
        right: Vertex<Variable>,
        left_right_type_pairs: Vec<(Type, Type)>,
    ) -> TypeInferenceEdge<'_> {
        let mut left_to_right = BTreeMap::new();
        let mut right_to_left = BTreeMap::new();
        for (l, r) in left_right_type_pairs {
            left_to_right.entry(l).or_insert_with(BTreeSet::new);
            left_to_right.get_mut(&l).unwrap().insert(r);
            right_to_left.entry(r).or_insert_with(BTreeSet::new);
            right_to_left.get_mut(&r).unwrap().insert(l);
        }
        TypeInferenceEdge { constraint, left, right, left_to_right, right_to_left }
    }

    #[test]
    fn basic_binary_edges() {
        // Some version of `$a isa animal, has name $n;`
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let all_concrete_animals = BTreeSet::from([type_cat, type_dog]);
        let all_concrete_names = BTreeSet::from([type_catname, type_dogname]);

        {
            // Case 1: $a isa cat, has name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.clone()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_NAME.clone()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();
            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat])),
                    (var_name.into(), BTreeSet::from([type_catname])),
                    (var_animal_type.into(), BTreeSet::from([type_cat])),
                    (var_name_type.into(), BTreeSet::from([type_name])),
                    (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat])),
                    (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat, type_cat)],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname, type_name)],
                    ),
                    expected_edge(&constraints[4], var_animal.into(), var_name.into(), vec![(type_cat, type_catname)]),
                ],
                nested_disjunctions: Vec::new(),
            };

            assert_eq!(expected_graph.vertices, graph.vertices);
            assert_eq!(expected_graph.edges, graph.edges);
            assert_eq!(expected_graph, graph);
        }

        {
            // Case 2: $a isa animal, has cat-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_ANIMAL.clone()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_CATNAME.clone()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();

            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat])),
                    (var_name.into(), BTreeSet::from([type_catname])),
                    (var_animal_type.into(), BTreeSet::from([type_animal])),
                    (var_name_type.into(), BTreeSet::from([type_catname])),
                    (Vertex::Label(LABEL_ANIMAL), BTreeSet::from([type_animal])),
                    (Vertex::Label(LABEL_CATNAME), BTreeSet::from([type_catname])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat, type_animal)],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname, type_catname)],
                    ),
                    expected_edge(&constraints[4], var_animal.into(), var_name.into(), vec![(type_cat, type_catname)]),
                ],
                nested_disjunctions: Vec::new(),
            };
            assert_eq!(expected_graph, graph);
        }

        {
            // Case 3: $a isa cat, has dog-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.clone()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_DOGNAME.clone()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();
            let err = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap_err();

            assert!(match err {
                TypeInferenceError::DetectedUnsatisfiableEdge { left_variable, right_variable, .. } => {
                    left_variable == "animal" && right_variable == "name"
                }
                _ => false,
            });
        }

        {
            // Case 4: $a isa animal, has name $n; // Just to be sure
            let types_a = all_concrete_animals.clone();
            let types_n = all_concrete_names.clone();
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_ANIMAL.clone()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_NAME.clone()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();
            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), types_a),
                    (var_name.into(), types_n),
                    (var_animal_type.into(), BTreeSet::from([type_animal])),
                    (var_name_type.into(), BTreeSet::from([type_name])),
                    (Vertex::Label(LABEL_ANIMAL), BTreeSet::from([type_animal])),
                    (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat, type_animal), (type_dog, type_animal)],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname, type_name), (type_dogname, type_name)],
                    ),
                    expected_edge(
                        &constraints[4],
                        var_animal.into(),
                        var_name.into(),
                        vec![(type_cat, type_catname), (type_dog, type_dogname)],
                    ),
                ],
                nested_disjunctions: Vec::new(),
            };
            assert_eq!(expected_graph.edges, graph.edges);
            assert_eq!(expected_graph, graph);
        }
    }

    #[test]
    fn basic_nested_graphs() {
        // Some version of `$a isa animal, has name $n;`
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let mut translation_context = PipelineTranslationContext::new();
        let mut value_parameters = ParameterRegistry::new();
        let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
        let mut conjunction = builder.conjunction_mut();
        let (var_animal, var_name, var_name_type) = ["animal", "name", "name_type"]
            .into_iter()
            .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
            .collect_tuple()
            .unwrap();

        // Case 1: {$a isa cat;} or {$a isa dog;}; $a has name $n;
        conjunction.constraints_mut().add_label(var_name_type, Label::build("name", None)).unwrap();
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
        conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

        let mut disj = conjunction.add_disjunction();

        let mut branch1 = disj.add_conjunction();
        let b1_var_animal_type = branch1.constraints_mut().get_or_declare_variable("b1_animal_type", None).unwrap();
        branch1.constraints_mut().add_isa(IsaKind::Subtype, var_animal, b1_var_animal_type.into(), None).unwrap();
        branch1.constraints_mut().add_label(b1_var_animal_type, LABEL_CAT.clone()).unwrap();

        let mut branch2 = disj.add_conjunction();
        let b2_var_animal_type = branch2.constraints_mut().get_or_declare_variable("b2_animal_type", None).unwrap();
        branch2.constraints_mut().add_isa(IsaKind::Subtype, var_animal, b2_var_animal_type.into(), None).unwrap();
        branch2.constraints_mut().add_label(b2_var_animal_type, LABEL_DOG.clone()).unwrap();

        let (b1_var_animal_type, b2_var_animal_type) = (b1_var_animal_type, b2_var_animal_type);

        let block = builder.finish().unwrap();

        let snapshot = storage.clone().open_snapshot_write();
        let graph = compute_type_inference_graph(
            &snapshot,
            block.block_context(),
            block.conjunction(),
            &translation_context.variable_registry,
            &type_manager,
            &BTreeMap::new(),
            &EmptyAnnotatedFunctionSignatures,
            false,
        )
        .unwrap();

        let conjunction = block.conjunction();
        let disj = conjunction.nested_patterns()[0].as_disjunction().unwrap();
        let [b1, b2] = disj.conjunctions() else { unreachable!() };
        let b1_isa = &b1.constraints()[0];
        let b2_isa = &b2.constraints()[0];
        let expected_nested_graphs = vec![
            TypeInferenceGraph {
                conjunction: b1,
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat])),
                    (b1_var_animal_type.into(), BTreeSet::from([type_cat])),
                    (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat])),
                ]),
                edges: vec![expected_edge(
                    b1_isa,
                    var_animal.into(),
                    b1_var_animal_type.into(),
                    vec![(type_cat, type_cat)],
                )],
                nested_disjunctions: Vec::new(),
            },
            TypeInferenceGraph {
                conjunction: b2,
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_dog])),
                    (b2_var_animal_type.into(), BTreeSet::from([type_dog])),
                    (Vertex::Label(LABEL_DOG), BTreeSet::from([type_dog])),
                ]),
                edges: vec![expected_edge(
                    b2_isa,
                    var_animal.into(),
                    b2_var_animal_type.into(),
                    vec![(type_dog, type_dog)],
                )],
                nested_disjunctions: Vec::new(),
            },
        ];

        let expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_animal.into(), BTreeSet::from([type_cat, type_dog])),
                (var_name.into(), BTreeSet::from([type_catname, type_dogname])),
                (var_name_type.into(), BTreeSet::from([type_name])),
                (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name])),
            ]),
            edges: vec![
                expected_edge(
                    &conjunction.constraints()[1],
                    var_name.into(),
                    var_name_type.into(),
                    vec![(type_catname, type_name), (type_dogname, type_name)],
                ),
                expected_edge(
                    &conjunction.constraints()[2],
                    var_animal.into(),
                    var_name.into(),
                    vec![(type_cat, type_catname), (type_dog, type_dogname)],
                ),
            ],
            nested_disjunctions: vec![NestedTypeInferenceGraphDisjunction {
                disjunction: expected_nested_graphs,
                shared_variables: BTreeSet::new(),
                shared_vertex_annotations: VertexAnnotations::default(),
            }],
        };

        assert_eq!(expected_graph, graph);
    }

    #[test]
    fn no_type_constraints() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, type_dog), (_, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        // Case 1: $a has $n;
        let snapshot = storage.clone().open_snapshot_write();
        let mut translation_context = PipelineTranslationContext::new();
        let mut value_parameters = ParameterRegistry::new();
        let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
        let mut conjunction = builder.conjunction_mut();
        let (var_animal, var_name) = ["animal", "name"]
            .into_iter()
            .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
            .collect_tuple()
            .unwrap();

        conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

        let block = builder.finish().unwrap();
        let conjunction = block.conjunction();
        let constraints = conjunction.constraints();
        let graph = compute_type_inference_graph(
            &snapshot,
            block.block_context(),
            block.conjunction(),
            &translation_context.variable_registry,
            &type_manager,
            &BTreeMap::new(),
            &EmptyAnnotatedFunctionSignatures,
            false,
        )
        .unwrap();

        let expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_animal.into(), BTreeSet::from([type_cat, type_dog])),
                (var_name.into(), BTreeSet::from([type_catname, type_dogname])),
            ]),
            edges: vec![expected_edge(
                &constraints[0],
                var_animal.into(),
                var_name.into(),
                vec![(type_cat, type_catname), (type_dog, type_dogname)],
            )],
            nested_disjunctions: Vec::new(),
        };

        assert_eq!(expected_graph, graph);
    }

    #[test]
    fn role_players() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, type_dog), _, (type_fears, type_has_fear, type_is_feared)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        // With roles specified
        let snapshot = storage.clone().open_snapshot_write();
        let mut translation_context = PipelineTranslationContext::new();
        let mut value_parameters = ParameterRegistry::new();
        let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
        let mut conjunction = builder.conjunction_mut();
        let (
            var_has_fear,
            var_is_feared,
            var_fears_type,
            var_fears,
            var_role_has_fear,
            var_role_is_feared,
            var_role_has_fear_type,
            var_role_is_feared_type,
        ) = [
            "has_fear",
            "is_feared",
            "fears_type",
            "fears",
            "role_has_fear",
            "role_is_fear",
            "role_has_fear_type",
            "role_is_feared_type",
        ]
        .into_iter()
        .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
        .collect_tuple()
        .unwrap();

        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_fears, var_fears_type.into(), None).unwrap();
        conjunction.constraints_mut().add_label(var_fears_type, LABEL_FEARS.clone()).unwrap();
        conjunction.constraints_mut().add_links(var_fears, var_has_fear, var_role_has_fear, None).unwrap();
        conjunction.constraints_mut().add_links(var_fears, var_is_feared, var_role_is_feared, None).unwrap();

        conjunction
            .constraints_mut()
            .add_sub(SubKind::Subtype, var_role_has_fear.into(), var_role_has_fear_type.into(), None)
            .unwrap();
        conjunction.constraints_mut().add_label(var_role_has_fear_type, LABEL_HAS_FEAR.clone()).unwrap();
        conjunction
            .constraints_mut()
            .add_sub(SubKind::Subtype, var_role_is_feared.into(), var_role_is_feared_type.into(), None)
            .unwrap();
        conjunction.constraints_mut().add_label(var_role_is_feared_type, LABEL_IS_FEARED.clone()).unwrap();

        let block = builder.finish().unwrap();

        let conjunction = block.conjunction();

        let graph = compute_type_inference_graph(
            &snapshot,
            block.block_context(),
            block.conjunction(),
            &translation_context.variable_registry,
            &type_manager,
            &BTreeMap::new(),
            &EmptyAnnotatedFunctionSignatures,
            false,
        )
        .unwrap();

        let expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_has_fear.into(), BTreeSet::from([type_cat])),
                (var_is_feared.into(), BTreeSet::from([type_dog])),
                (var_fears_type.into(), BTreeSet::from([type_fears])),
                (var_fears.into(), BTreeSet::from([type_fears])),
                (var_role_has_fear.into(), BTreeSet::from([type_has_fear])),
                (var_role_is_feared.into(), BTreeSet::from([type_is_feared])),
                (var_role_has_fear_type.into(), BTreeSet::from([type_has_fear])),
                (var_role_is_feared_type.into(), BTreeSet::from([type_is_feared])),
                (Vertex::Label(LABEL_FEARS), BTreeSet::from([type_fears])),
                (Vertex::Label(LABEL_HAS_FEAR), BTreeSet::from([type_has_fear])),
                (Vertex::Label(LABEL_IS_FEARED), BTreeSet::from([type_is_feared])),
            ]),
            edges: vec![
                // isa
                expected_edge(
                    &conjunction.constraints()[0],
                    var_fears.into(),
                    var_fears_type.into(),
                    vec![(type_fears, type_fears)],
                ),
                // has-fear edge
                expected_edge(
                    &conjunction.constraints()[2],
                    var_fears.into(),
                    var_role_has_fear.into(),
                    vec![(type_fears, type_has_fear)],
                ),
                expected_edge(
                    &conjunction.constraints()[2],
                    var_has_fear.into(),
                    var_role_has_fear.into(),
                    vec![(type_cat, type_has_fear)],
                ),
                // is-feared edge
                expected_edge(
                    &conjunction.constraints()[3],
                    var_fears.into(),
                    var_role_is_feared.into(),
                    vec![(type_fears, type_is_feared)],
                ),
                expected_edge(
                    &conjunction.constraints()[3],
                    var_is_feared.into(),
                    var_role_is_feared.into(),
                    vec![(type_dog, type_is_feared)],
                ),
                expected_edge(
                    &conjunction.constraints()[4],
                    var_role_has_fear.into(),
                    var_role_has_fear_type.into(),
                    vec![(type_has_fear, type_has_fear)],
                ),
                expected_edge(
                    &conjunction.constraints()[6],
                    var_role_is_feared.into(),
                    var_role_is_feared_type.into(),
                    vec![(type_is_feared, type_is_feared)],
                ),
            ],
            nested_disjunctions: Vec::new(),
        };

        assert_eq!(expected_graph, graph);
    }

    #[test]
    fn type_constraints() {
        // Some version of `$a isa animal, has name $n;`
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, type_dog), (_, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let all_concrete_animals = BTreeSet::from([type_cat, type_dog]);
        let all_concrete_names = BTreeSet::from([type_catname, type_dogname]);

        {
            // Case 1: $a isa $at; $at label cat; $n isa! $nt; $at owns $nt;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_owned_type) =
                ["animal", "name", "animal_type", "name_type"]
                    .into_iter()
                    .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                    .collect_tuple()
                    .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.clone()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Exact, var_name, var_owned_type.into(), None).unwrap();
            conjunction.constraints_mut().add_owns(var_animal_type.into(), var_owned_type.into(), None).unwrap();

            let block = builder.finish().unwrap();
            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat])),
                    (var_name.into(), BTreeSet::from([type_catname])),
                    (var_animal_type.into(), BTreeSet::from([type_cat])),
                    (var_owned_type.into(), BTreeSet::from([type_catname])),
                    (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat, type_cat)],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_owned_type.into(),
                        vec![(type_catname, type_catname)],
                    ),
                    expected_edge(
                        &constraints[3],
                        var_animal_type.into(),
                        var_owned_type.into(),
                        vec![(type_cat, type_catname)],
                    ),
                ],
                nested_disjunctions: Vec::new(),
            };

            assert_eq!(expected_graph, graph);
        }

        {
            // Case 2: $a isa $at; $n isa $nt; $nt type catname; $at owns $nt;
            let snapshot = storage.clone().open_snapshot_write();

            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_owner_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_owner_type.into(), None).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_CATNAME.clone()).unwrap();
            conjunction.constraints_mut().add_owns(var_owner_type.into(), var_name_type.into(), None).unwrap();

            let block = builder.finish().unwrap();

            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat])),
                    (var_name.into(), BTreeSet::from([type_catname])),
                    (var_owner_type.into(), BTreeSet::from([type_cat])),
                    (var_name_type.into(), BTreeSet::from([type_catname])),
                    (Vertex::Label(LABEL_CATNAME), BTreeSet::from([type_catname])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_owner_type.into(),
                        vec![(type_cat, type_cat)],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname, type_catname)],
                    ),
                    expected_edge(
                        &constraints[3],
                        var_owner_type.into(),
                        var_name_type.into(),
                        vec![(type_cat, type_catname)],
                    ),
                ],
                nested_disjunctions: Vec::new(),
            };
            assert_eq!(expected_graph, graph);
        }

        {
            // Case 3: $a isa $at; $at type cat; $n isa $nt; $nt type dogname; $at owns $nt;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.clone()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_DOGNAME.clone()).unwrap();
            conjunction.constraints_mut().add_owns(var_animal_type.into(), var_name_type.into(), None).unwrap();

            let block = builder.finish().unwrap();
            let constraints = block.conjunction().constraints();
            // We manually compute the graph so we can confirm it decays to empty annotations everywhere
            let mut graph = TypeGraphSeedingContext::new(
                &snapshot,
                &type_manager,
                &EmptyAnnotatedFunctionSignatures,
                &translation_context.variable_registry,
                false,
            )
            .create_graph(block.block_context(), &BTreeMap::new(), block.conjunction())
            .unwrap();
            crate::annotation::match_inference::prune_types(&mut graph);

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::new()),
                    (var_name.into(), BTreeSet::new()),
                    (var_animal_type.into(), BTreeSet::new()),
                    (var_name_type.into(), BTreeSet::new()),
                    (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat])),
                    (Vertex::Label(LABEL_DOGNAME), BTreeSet::from([type_dogname])),
                ]),
                edges: vec![
                    expected_edge(&constraints[0], var_animal.into(), var_animal_type.into(), Vec::new()),
                    expected_edge(&constraints[2], var_name.into(), var_name_type.into(), Vec::new()),
                    expected_edge(&constraints[4], var_animal_type.into(), var_name_type.into(), Vec::new()),
                ],
                nested_disjunctions: Vec::new(),
            };
            assert_eq!(expected_graph, graph);
        }

        {
            // Case 4: $a isa! $at; $n isa! $nt; $at owns $nt;
            let types_a = all_concrete_animals.clone();
            let types_n = all_concrete_names.clone();
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Exact, var_animal, var_animal_type.into(), None).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Exact, var_name, var_name_type.into(), None).unwrap();
            conjunction.constraints_mut().add_owns(var_animal_type.into(), var_name_type.into(), None).unwrap();

            let block = builder.finish().unwrap();
            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), types_a.clone()),
                    (var_name.into(), types_n.clone()),
                    (var_animal_type.into(), types_a.clone()),
                    (var_name_type.into(), types_n.clone()),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat, type_cat), (type_dog, type_dog)],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname, type_catname), (type_dogname, type_dogname)],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_animal_type.into(),
                        var_name_type.into(),
                        vec![(type_cat, type_catname), (type_dog, type_dogname)],
                    ),
                ],
                nested_disjunctions: Vec::new(),
            };

            assert_eq!(expected_graph, graph);
        }
    }

    #[test]
    fn basic_binary_edges_fixed_labels() {
        // Some version of `$a isa animal, has name $n;`
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let all_concrete_animals = BTreeSet::from([type_cat, type_dog]);
        let all_concrete_names = BTreeSet::from([type_catname, type_dogname]);

        {
            // Case 1: $a isa cat, has name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] = ["animal", "name"]
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap());

            conjunction
                .constraints_mut()
                .add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_CAT), None)
                .unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_NAME), None).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();
            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat])),
                    (var_name.into(), BTreeSet::from([type_catname])),
                    (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat])),
                    (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        Vertex::Label(LABEL_CAT),
                        vec![(type_cat, type_cat)],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        Vertex::Label(LABEL_NAME),
                        vec![(type_catname, type_name)],
                    ),
                    expected_edge(&constraints[2], var_animal.into(), var_name.into(), vec![(type_cat, type_catname)]),
                ],
                nested_disjunctions: Vec::new(),
            };

            assert_eq!(expected_graph.vertices, graph.vertices);
            assert_eq!(expected_graph.edges, graph.edges);
            assert_eq!(expected_graph, graph);
        }

        {
            // Case 2: $a isa animal, has cat-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] = ["animal", "name"]
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap());

            conjunction
                .constraints_mut()
                .add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_ANIMAL), None)
                .unwrap();
            conjunction
                .constraints_mut()
                .add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_CATNAME), None)
                .unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();

            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat])),
                    (var_name.into(), BTreeSet::from([type_catname])),
                    (Vertex::Label(LABEL_ANIMAL), BTreeSet::from([type_animal])),
                    (Vertex::Label(LABEL_CATNAME), BTreeSet::from([type_catname])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        Vertex::Label(LABEL_ANIMAL),
                        vec![(type_cat, type_animal)],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        Vertex::Label(LABEL_CATNAME),
                        vec![(type_catname, type_catname)],
                    ),
                    expected_edge(&constraints[2], var_animal.into(), var_name.into(), vec![(type_cat, type_catname)]),
                ],
                nested_disjunctions: Vec::new(),
            };
            assert_eq!(expected_graph, graph);
        }

        {
            // Case 3: $a isa cat, has dog-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] = ["animal", "name"]
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap());

            conjunction
                .constraints_mut()
                .add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_CAT), None)
                .unwrap();
            conjunction
                .constraints_mut()
                .add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_DOGNAME), None)
                .unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();
            let err = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap_err();
            assert!(match err {
                TypeInferenceError::DetectedUnsatisfiableEdge { left_variable, right_variable, .. } => {
                    left_variable == "animal" && right_variable == "name"
                }
                _ => false,
            });
        }

        {
            // Case 4: $a isa animal, has name $n; // Just to be sure
            let types_a = all_concrete_animals.clone();
            let types_n = all_concrete_names.clone();
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] = ["animal", "name"]
                .map(|name| conjunction.constraints_mut().get_or_declare_variable(name, None).unwrap());

            conjunction
                .constraints_mut()
                .add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_ANIMAL), None)
                .unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_NAME), None).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

            let block = builder.finish().unwrap();
            let constraints = block.conjunction().constraints();
            let graph = compute_type_inference_graph(
                &snapshot,
                block.block_context(),
                block.conjunction(),
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &EmptyAnnotatedFunctionSignatures,
                false,
            )
            .unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), types_a),
                    (var_name.into(), types_n),
                    (Vertex::Label(LABEL_ANIMAL), BTreeSet::from([type_animal])),
                    (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        Vertex::Label(LABEL_ANIMAL),
                        vec![(type_cat, type_animal), (type_dog, type_animal)],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        Vertex::Label(LABEL_NAME),
                        vec![(type_catname, type_name), (type_dogname, type_name)],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_animal.into(),
                        var_name.into(),
                        vec![(type_cat, type_catname), (type_dog, type_dogname)],
                    ),
                ],
                nested_disjunctions: Vec::new(),
            };
            assert_eq!(expected_graph.vertices, graph.vertices);
            assert_eq!(expected_graph.edges, graph.edges);
        }
    }

    #[test]
    fn no_labels() {
        // dog sub animal, owns dog-name; cat sub animal owns cat-name;
        // cat-name sub animal-name; dog-name sub animal-name;
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_type_animal, type_cat, type_dog), (_type_name, type_catname, type_dogname), (type_fears, _, _)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        // Case 1: $a has $n;
        let mut translation_context = PipelineTranslationContext::new();
        let mut value_parameters = ParameterRegistry::new();
        let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
        let mut conjunction = builder.conjunction_mut();
        let var_animal = conjunction.constraints_mut().get_or_declare_variable("animal", None).unwrap();
        let var_name = conjunction.constraints_mut().get_or_declare_variable("name", None).unwrap();

        // Try seeding
        conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

        let block = builder.finish().unwrap();
        let conjunction = block.conjunction();

        let constraints = conjunction.constraints();
        let mut expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_animal.into(), BTreeSet::from([type_cat, type_dog])),
                (var_name.into(), BTreeSet::from([type_catname, type_dogname])),
            ]),
            edges: vec![expected_edge(
                &constraints[0],
                var_animal.into(),
                var_name.into(),
                vec![(type_cat, type_catname), (type_dog, type_dogname)],
            )],
            nested_disjunctions: vec![],
        };

        let snapshot = storage.clone().open_snapshot_write();
        let empty_function_cache = EmptyAnnotatedFunctionSignatures;
        let seeder = TypeGraphSeedingContext::new(
            &snapshot,
            &type_manager,
            &empty_function_cache,
            &translation_context.variable_registry,
            false,
        );
        let mut graph = seeder.create_graph(block.block_context(), &BTreeMap::new(), conjunction).unwrap();
        prune_types(&mut graph);
        if expected_graph != graph {
            // We need this because of non-determinism
            expected_graph.vertices.get_mut(&var_animal.into()).unwrap().insert(type_fears);
            assert_eq!(expected_graph, graph)
        }
    }
}
