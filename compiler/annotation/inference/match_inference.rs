/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::{Type as TypeAnnotation, variable::Variable};
use ir::{
    pattern::{
        Pattern, Scope, ScopeId, Vertex, conjunction::Conjunction, constraint::Constraint, disjunction::Disjunction,
        nested_pattern::NestedPattern,
    },
    pipeline::{
        VariableRegistry,
        block::{Block, BlockContext},
    },
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    PipelineAnnotationContext, TypeInferenceError,
    inference::{RetainAndContainExt, VertexAnnotations, type_seeder::TypeGraphSeedingContext},
    pipeline::RunningVariableAnnotations,
    type_annotations::{
        BlockAnnotations, ConstraintTypeAnnotations, LeftRightAnnotations, LinksAnnotations, TypeAnnotations,
    },
    type_inference::TypeInferenceMode,
};

pub fn infer_types_for_block(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    previous_stage_annotations: &RunningVariableAnnotations,
    block: &Block,
    type_inference_mode: TypeInferenceMode,
) -> Result<BlockAnnotations, TypeInferenceError> {
    let input_annotations = previous_stage_annotations
        .concepts
        .iter()
        .map(|(var, annotations)| (Vertex::Variable(*var), (**annotations).clone()))
        .collect();
    let input_annotations = VertexAnnotations { annotations: input_annotations };
    let root_graph = infer_types_impl(ctx, block.conjunction(), &input_annotations, type_inference_mode)?;
    let mut type_annotations_by_scope = HashMap::new();
    root_graph.into_type_annotations_by_scope(&mut type_annotations_by_scope);
    Ok(BlockAnnotations::new(type_annotations_by_scope))
}

fn infer_types_impl<'conj>(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    conjunction: &'conj Conjunction,
    input_annotations: &VertexAnnotations,
    type_inference_mode: TypeInferenceMode,
) -> Result<FullTypeInferenceGraph<'conj>, TypeInferenceError> {
    let graph = compute_type_inference_graph(ctx, conjunction, input_annotations, type_inference_mode)?;
    let full_graph = infer_types_in_negations_and_optionals_and_complete(ctx, graph, type_inference_mode)?;
    Ok(full_graph)
}

fn infer_types_in_negations_and_optionals_and_complete<'conj>(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    graph: TypeInferenceGraph<'conj>,
    type_inference_mode: TypeInferenceMode,
) -> Result<FullTypeInferenceGraph<'conj>, TypeInferenceError> {
    // Add the negations & optionals to TypeInferenceGraph to get FullTypeInferenceGraph.
    // Copy over the optional variable annotations from optionals & disjunctions.
    let TypeInferenceGraph { conjunction, mut vertices, edges, nested_disjunctions } = graph;
    let mut negations = Vec::new();
    let mut optionals = Vec::new();
    for nested in conjunction.nested_patterns() {
        match nested {
            NestedPattern::Disjunction(_) => {} // Done above
            NestedPattern::Negation(negation) => {
                // No variables can escape a negation => Nothing to update
                negations.push(infer_types_impl(ctx, negation.conjunction(), &vertices, type_inference_mode)?);
            }
            NestedPattern::Optional(optional) => {
                let optional_graph = infer_types_impl(ctx, optional.conjunction(), &vertices, type_inference_mode)?;
                optional.optionally_bound_by_pattern().for_each(|optional_var| {
                    let optional_vertex = Vertex::Variable(optional_var);
                    let annotations = optional_graph.vertices[&optional_vertex].clone();
                    vertices.insert(optional_vertex, annotations);
                });
                optionals.push(optional_graph)
            }
        }
    }
    let mut disjunctions = Vec::new();
    for nested_disjunction in nested_disjunctions {
        let branches = nested_disjunction
            .disjunction
            .into_iter()
            .map(|d| infer_types_in_negations_and_optionals_and_complete(ctx, d, type_inference_mode))
            .collect::<Result<Vec<_>, _>>()?;
        nested_disjunction.disjunction_pattern.optionally_bound_by_pattern().for_each(|optional_var| {
            let optional_vertex = Vertex::Variable(optional_var);
            let annotations = branches.iter().flat_map(|b| b.vertices[&optional_vertex].iter().copied()).collect();
            vertices.insert(optional_vertex, annotations);
        });
        disjunctions.push(branches);
    }
    Ok(FullTypeInferenceGraph { conjunction, vertices, edges, disjunctions, optionals, negations })
}

fn all_vertex_annotations_available(
    context: &BlockContext,
    variable_registry: &VariableRegistry,
    conjunction: &Conjunction,
    by_scope: &HashMap<ScopeId, TypeAnnotations>,
) -> bool {
    let conjunction_annotations = by_scope.get(&conjunction.scope_id()).unwrap();
    (conjunction
        .visible_referenced_variables()
        .filter(|var| {
            let category = variable_registry.get_variable_category(*var).unwrap();
            category.is_category_type() || category.is_category_thing()
        })
        .all(|v| conjunction_annotations.vertex_annotations_of(&Vertex::Variable(v)).is_some()))
        && (conjunction.nested_patterns().iter().all(|nested| match nested {
            NestedPattern::Disjunction(disj) => disj
                .conjunctions()
                .iter()
                .all(|inner| all_vertex_annotations_available(context, variable_registry, inner, by_scope)),
            NestedPattern::Negation(inner) => {
                all_vertex_annotations_available(context, variable_registry, inner.conjunction(), by_scope)
            }
            NestedPattern::Optional(inner) => {
                all_vertex_annotations_available(context, variable_registry, inner.conjunction(), by_scope)
            }
        }))
}

pub(crate) fn compute_type_inference_graph<'graph>(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    conjunction: &'graph Conjunction,
    input_annotations: &VertexAnnotations,
    type_inference_mode: TypeInferenceMode,
) -> Result<TypeInferenceGraph<'graph>, TypeInferenceError> {
    let mut graph = TypeGraphSeedingContext::new(
        ctx.snapshot,
        ctx.type_manager,
        ctx.annotated_function_signatures,
        ctx.variable_registry,
        type_inference_mode,
    )
    .create_graph(input_annotations, conjunction)?;
    pre_check_edges_for_trivial_unsatisfiability(&graph)
        .map_err(|(graph, edge)| construct_error_message_for_unsatisfiable_edge(ctx, graph, edge))?;

    prune_types(&mut graph);
    // TODO: Throw error when any set becomes empty happens, rather than waiting for the it to propagate
    graph.check_thing_constraints_satisfiable(ctx.variable_registry)?;
    Ok(graph)
}

fn construct_error_message_for_unsatisfiable_edge(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    graph: &TypeInferenceGraph<'_>,
    edge: &TypeInferenceEdge<'_>,
) -> TypeInferenceError {
    let resolve_vertex = |vertex: &Vertex<Variable>| match vertex {
        Vertex::Variable(v) => ctx.variable_registry.get_variable_name_or_unnamed(*v).to_owned(),
        Vertex::Label(label) => label.scoped_name().as_str().to_string(),
        Vertex::Parameter(_) => unreachable!("Parameters can't be involved in TypeInferenceEdges"),
    };
    let resolve_type_label = |type_: &answer::Type| {
        type_
            .get_label(ctx.snapshot, ctx.type_manager)
            .map(|label| label.scoped_name().to_string())
            .unwrap_or_else(|_| "(Error while resolving label)".to_owned())
    };
    let left_variable = resolve_vertex(&edge.left);
    let right_variable = resolve_vertex(&edge.right);
    let left_types = graph.vertices.annotations.get(&edge.left).unwrap().iter().map(resolve_type_label).join(", ");
    let right_types = graph.vertices.annotations.get(&edge.right).unwrap().iter().map(resolve_type_label).join(", ");
    TypeInferenceError::DetectedUnsatisfiableEdge {
        left_variable,
        right_variable,
        left_types,
        right_types,
        constraint_type: edge.constraint.name().to_owned(),
        source_span: edge.constraint.source_span(),
    }
}

fn pre_check_edges_for_trivial_unsatisfiability<'a>(
    graph: &'a TypeInferenceGraph<'a>,
) -> Result<(), (&'a TypeInferenceGraph<'a>, &'a TypeInferenceEdge<'a>)> {
    let mut thing_edges = graph.edges.iter().filter(|edge| match &edge.constraint {
        Constraint::Links(_) | Constraint::Has(_) | Constraint::Isa(_) => true,
        _ => false,
    });
    if let Some(edge) = thing_edges.find(|edge| edge.left_to_right.is_empty() || edge.right_to_left.is_empty()) {
        return Err((graph, edge));
    }
    graph
        .nested_disjunctions
        .iter()
        .flat_map(|d| d.disjunction.iter())
        .try_for_each(|nested| pre_check_edges_for_trivial_unsatisfiability(nested))?;
    Ok(())
}

pub(crate) fn prune_types(graph: &mut TypeInferenceGraph<'_>) {
    while graph.prune_vertices_from_constraints() {
        graph.prune_constraints_from_vertices();
    }
}

#[derive(Debug)]
pub(crate) struct TypeInferenceGraph<'this> {
    pub(crate) conjunction: &'this Conjunction,
    pub(crate) vertices: VertexAnnotations,
    pub(crate) edges: Vec<TypeInferenceEdge<'this>>,
    pub(crate) nested_disjunctions: Vec<NestedTypeInferenceGraphDisjunction<'this>>,
}

impl<'this> TypeInferenceGraph<'this> {
    fn prune_constraints_from_vertices(&mut self) {
        for edge in &mut self.edges {
            edge.prune_self_from_vertices(&self.vertices)
        }
        for nested_graph in &mut self.nested_disjunctions {
            nested_graph.prune_self_from_vertices(&self.vertices)
        }
    }

    fn prune_vertices_from_constraints(&mut self) -> bool {
        let mut is_modified = false;
        for edge in &mut self.edges {
            is_modified |= edge.prune_vertices_from_self(&mut self.vertices);
        }
        for nested_graph in &mut self.nested_disjunctions {
            is_modified |= nested_graph.prune_vertices_from_self(&mut self.vertices);
        }
        is_modified
    }

    fn check_thing_constraints_satisfiable(
        &self,
        variable_registry: &VariableRegistry,
    ) -> Result<(), TypeInferenceError> {
        let thing_variable_present = self
            .vertices
            .annotations
            .iter()
            .filter_map(|(var, _)| var.as_variable())
            .any(|var| variable_registry.get_variable_category(var).unwrap().is_category_thing());

        let any_vertex_empty = self.vertices.annotations.iter().any(|(_, types)| types.is_empty());
        if any_vertex_empty && thing_variable_present {
            return Err(TypeInferenceError::DetectedUnsatisfiablePattern {});
        }
        self.nested_disjunctions
            .iter()
            .flat_map(|d| d.disjunction.iter())
            .try_for_each(|graph| graph.check_thing_constraints_satisfiable(variable_registry))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct TypeInferenceEdge<'this> {
    pub(crate) constraint: &'this Constraint<Variable>,
    pub(crate) left: Vertex<Variable>,
    pub(crate) right: Vertex<Variable>,
    pub(crate) left_to_right: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
    pub(crate) right_to_left: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
}

impl<'this> TypeInferenceEdge<'this> {
    // Construction

    pub(crate) fn build(
        constraint: &'this Constraint<Variable>,
        left: Vertex<Variable>,
        right: Vertex<Variable>,
        initial_left_to_right: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
        initial_right_to_left: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
    ) -> TypeInferenceEdge<'this> {
        // The left_to_right & right_to_left sets must be consistent with each other. i.e.
        //   They must contain the same set of edges.
        // This is a pre-condition to the type-inference loop.
        // This is currently true by construction.
        debug_assert!(
            initial_left_to_right
                .iter()
                .all(|(u, vs)| vs.iter().all(|v| initial_right_to_left.get(v).unwrap().contains(u)))
        );
        debug_assert!(
            initial_right_to_left
                .iter()
                .all(|(u, vs)| vs.iter().all(|v| initial_left_to_right.get(v).unwrap().contains(u)))
        );
        TypeInferenceEdge {
            constraint,
            left,
            right,
            left_to_right: initial_left_to_right,
            right_to_left: initial_right_to_left,
        }
    }

    fn remove_type_from_values_of(
        type_: &TypeAnnotation,
        keys_to_look_under: &BTreeSet<TypeAnnotation>,
        remove_from_values_of: &mut BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
    ) {
        for other_type in keys_to_look_under {
            let value_set_to_remove_from = remove_from_values_of.get_mut(other_type).unwrap();
            value_set_to_remove_from.remove(type_);
            let remaining_size = value_set_to_remove_from.len();
            if 0 == remaining_size {
                remove_from_values_of.remove(other_type);
            }
        }
    }

    fn prune_vertices_from_self(&self, vertices: &mut VertexAnnotations) -> bool {
        let mut is_modified = false;
        {
            let left_vertex_annotations = vertices.get_mut(&self.left).unwrap();
            let size_before = left_vertex_annotations.len();
            left_vertex_annotations.retain_intersection(&self.left_to_right);
            is_modified = is_modified || size_before != left_vertex_annotations.len();
        };
        {
            let right_vertex_annotations = vertices.get_mut(&self.right).unwrap();
            let size_before = right_vertex_annotations.len();
            right_vertex_annotations.retain_intersection(&self.right_to_left);
            is_modified = is_modified || size_before != right_vertex_annotations.len();
        };
        is_modified
    }

    fn prune_self_from_vertices(&mut self, vertices: &VertexAnnotations) {
        let TypeInferenceEdge { left_to_right, right_to_left, .. } = self;
        {
            let left_vertex_annotations = vertices.get(&self.left).unwrap();
            left_to_right.iter().filter(|(left_type, _)| !left_vertex_annotations.contains(*left_type)).for_each(
                |(left_type, right_keys)| Self::remove_type_from_values_of(left_type, right_keys, right_to_left),
            );
            left_to_right.retain_intersection(left_vertex_annotations);
        };
        {
            let right_vertex_annotations = vertices.get(&self.right).unwrap();
            right_to_left.iter().filter(|(right_type, _)| !right_vertex_annotations.contains(*right_type)).for_each(
                |(right_type, left_keys)| Self::remove_type_from_values_of(right_type, left_keys, left_to_right),
            );
            right_to_left.retain_intersection(right_vertex_annotations);
        };
    }
}

#[derive(Debug)]
pub(crate) struct NestedTypeInferenceGraphDisjunction<'this> {
    pub(crate) disjunction_pattern: &'this Disjunction,
    pub(crate) disjunction: Vec<TypeInferenceGraph<'this>>,
}

impl NestedTypeInferenceGraphDisjunction<'_> {
    fn prune_self_from_vertices(&mut self, parent_vertices: &VertexAnnotations) {
        let Self { disjunction, disjunction_pattern } = self;
        for nested_graph in disjunction {
            let TypeInferenceGraph { conjunction, vertices: nested_vertices, .. } = nested_graph;
            for vertex in Self::branch_variables_affected_by_parent(disjunction_pattern, conjunction) {
                debug_assert!(parent_vertices.contains_key(&vertex) && nested_vertices.contains_key(&vertex));
                nested_vertices.get_mut(&vertex).unwrap().retain_intersection(parent_vertices[&vertex])
            }
            nested_graph.prune_constraints_from_vertices();
        }
    }

    fn prune_vertices_from_self(&mut self, parent_vertices: &mut VertexAnnotations) -> bool {
        debug_assert!(Self::variables_affecting_parent(self.disjunction_pattern).all(|vertex| {
            self.disjunction.iter().all(|branch| branch.vertices.contains_key(&vertex))
                && parent_vertices.contains_key(&vertex)
        }));

        let mut is_modified = false;
        for nested_graph in &mut self.disjunction {
            is_modified |= nested_graph.prune_vertices_from_constraints();
        }

        for vertex in Self::variables_affecting_parent(self.disjunction_pattern) {
            let parent_vertex_types = parent_vertices.get_mut(&vertex).unwrap();
            let size_before = parent_vertex_types.len();
            parent_vertex_types.retain(|type_| {
                self.disjunction.iter().any(|nested_graph| {
                    nested_graph.vertices.get(&vertex).map(|nested_types| nested_types.contains(type_)).unwrap_or(true)
                })
            });
            is_modified |= size_before != parent_vertex_types.len();
        }
        is_modified
    }

    pub(crate) fn variables_affecting_parent(disjunction: &Disjunction) -> impl Iterator<Item = Vertex<Variable>> {
        // Are those in every branch. But simpler:
        disjunction.always_bound_by_pattern().map(Vertex::Variable)
    }

    pub(crate) fn branch_variables_affected_by_parent(
        disjunction: &Disjunction,
        branch: &Conjunction,
    ) -> impl Iterator<Item = Vertex<Variable>> {
        disjunction
            .visible_referenced_variables()
            .filter(|variable| branch.is_variable_visible_referenced(variable))
            .map(Vertex::Variable)
    }
}

#[derive(Debug)]
struct FullTypeInferenceGraph<'this> {
    conjunction: &'this Conjunction,
    vertices: VertexAnnotations,
    edges: Vec<TypeInferenceEdge<'this>>,
    disjunctions: Vec<Vec<FullTypeInferenceGraph<'this>>>,
    optionals: Vec<FullTypeInferenceGraph<'this>>,
    negations: Vec<FullTypeInferenceGraph<'this>>,
}

impl FullTypeInferenceGraph<'_> {
    fn into_type_annotations_by_scope(self, by_scope: &mut HashMap<ScopeId, TypeAnnotations>) {
        let Self { conjunction, vertices, edges, disjunctions, negations, optionals } = self;
        let type_annotations = Self::build_type_annotations(vertices, edges);
        by_scope.insert(conjunction.scope_id(), type_annotations);
        let all_nested = disjunctions
            .into_iter()
            .flat_map(|disjunction| disjunction.into_iter())
            .chain(negations.into_iter())
            .chain(optionals.into_iter());
        all_nested.for_each(|nested| nested.into_type_annotations_by_scope(by_scope));
    }

    fn build_type_annotations(vertices: VertexAnnotations, edges: Vec<TypeInferenceEdge<'_>>) -> TypeAnnotations {
        let mut constraint_annotations = HashMap::new();
        let mut combine_links_edges = HashMap::new();
        edges.into_iter().for_each(|edge| {
            let TypeInferenceEdge { constraint, left_to_right, right_to_left, .. } = edge;
            if let Constraint::Links(links) = edge.constraint {
                if let Some((other_left_right, other_right_left)) = combine_links_edges.remove(&edge.right) {
                    let lrf_annotation = {
                        if &edge.left == links.relation() {
                            LinksAnnotations::build(left_to_right, right_to_left, other_left_right, other_right_left)
                        } else {
                            LinksAnnotations::build(other_left_right, other_right_left, left_to_right, right_to_left)
                        }
                    };
                    constraint_annotations.insert(constraint.clone(), ConstraintTypeAnnotations::Links(lrf_annotation));
                } else {
                    combine_links_edges.insert(edge.right, (left_to_right, right_to_left));
                }
            } else {
                let lr_annotations = LeftRightAnnotations::build(left_to_right, right_to_left);
                constraint_annotations.insert(constraint.clone(), ConstraintTypeAnnotations::LeftRight(lr_annotations));
            }
        });

        let vertex_annotations = vertices
            .into_iter()
            .map(|(variable, types)| (variable.into(), Arc::new(types)))
            .collect::<BTreeMap<_, _>>();
        TypeAnnotations::new(vertex_annotations, constraint_annotations)
    }
}

#[cfg(debug_assertions)]
pub fn infer_types_for_test_only(
    ctx: &mut PipelineAnnotationContext<'_, impl ReadableSnapshot>,
    block: &Block,
    is_write_stage: bool,
) -> Result<BlockAnnotations, TypeInferenceError> {
    let stage_type =
        if is_write_stage { TypeInferenceMode::ExactAndExplicit } else { TypeInferenceMode::ConcreteSubtypesOnly };
    let empty_annotations = RunningVariableAnnotations::empty();
    infer_types_for_block(ctx, &empty_annotations, block, stage_type)
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        sync::Arc,
    };

    use answer::{Type, variable::Variable};
    use assert as assert_true;
    use encoding::{
        graph::definition::definition_key::{DefinitionID, DefinitionKey},
        layout::prefix::Prefix,
        value::label::Label,
    };
    use ir::{
        pattern::{
            AssignedVariable, Vertex,
            constraint::{Constraint, IsaKind, SubKind},
            variable_category::{VariableCategory, VariableOptionality},
        },
        pipeline::{
            ParameterRegistry, VariableRegistry,
            block::Block,
            function::{Function, FunctionBody, ReturnOperation},
            function_signature::{FunctionID, FunctionSignature},
        },
        translation::{PipelineTranslationContext, pipeline::TranslatedStage},
    };
    use itertools::Itertools;

    use crate::{
        PipelineOrigin,
        annotation::{
            AnnotationContext, TypeInferenceError,
            function::{
                AnnotatedFunctionSignature, AnnotatedFunctionSignaturesImpl, EmptyAnnotatedFunctionSignatures,
                annotate_named_function,
            },
            inference::{
                match_inference::{
                    NestedTypeInferenceGraphDisjunction, TypeInferenceEdge, TypeInferenceGraph, VertexAnnotations,
                    compute_type_inference_graph, infer_types_for_block, prune_types,
                },
                type_seeder::TypeGraphSeedingContext,
            },
            pipeline::{AnnotatedStage, RunningVariableAnnotations},
            tests::{
                managers,
                schema_consts::{
                    LABEL_ANIMAL, LABEL_CAT, LABEL_CATNAME, LABEL_DOG, LABEL_DOGNAME, LABEL_FEARS, LABEL_HAS_FEAR,
                    LABEL_IS_FEARED, LABEL_NAME, setup_types,
                },
                setup_storage,
            },
            type_inference::TypeInferenceMode,
        },
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
            FunctionID::Schema(DefinitionKey::new(Prefix::DefinitionFunction, DefinitionID::new(0))),
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
                vec![],
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
                .add_function_binding(
                    vec![AssignedVariable::new_required(var_animal)],
                    &callee_signature,
                    vec![],
                    "test_fn",
                    None,
                )
                .unwrap();
            let entry = builder.finish().unwrap();
            (entry, entry_context, f_ir)
        })
        .collect_tuple()
        .unwrap();

        let snapshot = storage.open_snapshot_read();
        let empty_annotation_context =
            AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
        {
            // Local inference only
            // match $animal = fn_test();
            let (entry, mut entry_context, _) = with_no_cache;
            let var_animal = var_from_registry(&entry_context.variable_registry, "animal").unwrap();

            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                empty_annotation_context.for_pipeline(&mut entry_context.variable_registry, &parameters);
            let annotations_without_schema_cache = infer_types_for_block(
                &mut pipeline_annotation_context,
                &RunningVariableAnnotations::empty(),
                &entry,
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            let (entry, mut entry_context, mut f_ir) = with_local_cache;

            let f_annotations =
                annotate_named_function(&mut f_ir, &empty_annotation_context, PipelineOrigin::Query).unwrap();
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
            let previous_stage_variable_annotations = &RunningVariableAnnotations::empty();
            let empty_schema_functions = HashMap::<DefinitionKey, AnnotatedFunctionSignature>::new();
            let preamble_functions = vec![f_annotations];
            let function_annotations =
                AnnotatedFunctionSignaturesImpl::new(&empty_schema_functions, &preamble_functions);

            let annotation_context = AnnotationContext::new(&snapshot, &type_manager, &function_annotations);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut entry_context.variable_registry, &parameters);

            let entry_annotations = infer_types_for_block(
                &mut pipeline_annotation_context,
                previous_stage_variable_annotations,
                &entry,
                TypeInferenceMode::ConcreteSubtypesOnly,
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

            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let err = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
            )
            .unwrap_err();

            assert_true!(match err {
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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

        let mut disj = conjunction.add_disjunction(None);

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
        let annotation_context = AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
        let parameters = ParameterRegistry::new();
        let mut pipeline_annotation_context =
            annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
        let graph = compute_type_inference_graph(
            &mut pipeline_annotation_context,
            block.conjunction(),
            &VertexAnnotations::new(),
            TypeInferenceMode::ConcreteSubtypesOnly,
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
                disjunction_pattern: &disj,
                disjunction: expected_nested_graphs,
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
        let parameters = ParameterRegistry::new();
        let annotation_context = AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
        let mut pipeline_annotation_context =
            annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
        let graph = compute_type_inference_graph(
            &mut pipeline_annotation_context,
            block.conjunction(),
            &VertexAnnotations::new(),
            TypeInferenceMode::ConcreteSubtypesOnly,
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
        let parameters = ParameterRegistry::new();
        let annotation_context = AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
        let mut pipeline_annotation_context =
            annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
        let graph = compute_type_inference_graph(
            &mut pipeline_annotation_context,
            block.conjunction(),
            &VertexAnnotations::new(),
            TypeInferenceMode::ConcreteSubtypesOnly,
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
                TypeInferenceMode::ConcreteSubtypesOnly,
            )
            .create_graph(&VertexAnnotations::new(), block.conjunction())
            .unwrap();
            prune_types(&mut graph);

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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let err = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
            )
            .unwrap_err();
            assert_true!(match err {
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
            let annotation_context =
                AnnotationContext::new(&snapshot, &type_manager, &EmptyAnnotatedFunctionSignatures);
            let parameters = ParameterRegistry::new();
            let mut pipeline_annotation_context =
                annotation_context.for_pipeline(&mut translation_context.variable_registry, &parameters);
            let graph = compute_type_inference_graph(
                &mut pipeline_annotation_context,
                block.conjunction(),
                &VertexAnnotations::new(),
                TypeInferenceMode::ConcreteSubtypesOnly,
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
            TypeInferenceMode::ConcreteSubtypesOnly,
        );
        let mut graph = seeder.create_graph(&VertexAnnotations::new(), conjunction).unwrap();
        prune_types(&mut graph);
        if expected_graph != graph {
            // We need this because of non-determinism
            expected_graph.vertices.get_mut(&var_animal.into()).unwrap().insert(type_fears);
            assert_eq!(expected_graph, graph)
        }
    }
}
