/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::{Deref, DerefMut},
    sync::Arc,
};

use answer::{variable::Variable, Type as TypeAnnotation};
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::{conjunction::Conjunction, constraint::Constraint, variable_category::VariableCategory, Vertex},
    pipeline::{block::Block, VariableRegistry},
};
use itertools::chain;
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    function::AnnotatedFunctionSignatures,
    type_annotations::{ConstraintTypeAnnotations, LeftRightAnnotations, LinksAnnotations, TypeAnnotations},
    type_seeder::TypeGraphSeedingContext,
    TypeInferenceError,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct VertexAnnotations {
    annotations: BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>>,
}

impl VertexAnnotations {
    pub(crate) fn add_or_intersect(
        &mut self,
        vertex: &Vertex<Variable>,
        new_annotations: Cow<'_, BTreeSet<TypeAnnotation>>,
    ) -> bool {
        if let Some(existing_annotations) = self.get_mut(vertex) {
            let size_before = existing_annotations.len();
            existing_annotations.retain(|x| new_annotations.contains(x));
            existing_annotations.len() == size_before
        } else {
            self.insert(vertex.clone(), new_annotations.into_owned());
            true
        }
    }
}

impl Deref for VertexAnnotations {
    type Target = BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>>;

    fn deref(&self) -> &Self::Target {
        &self.annotations
    }
}

impl DerefMut for VertexAnnotations {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.annotations
    }
}

impl IntoIterator for VertexAnnotations {
    type Item = <BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.into_iter()
    }
}

impl<'a> IntoIterator for &'a VertexAnnotations {
    type Item = <&'a BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <&'a BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter()
    }
}

impl<'a> IntoIterator for &'a mut VertexAnnotations {
    type Item = <&'a mut BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <&'a mut BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter_mut()
    }
}

impl<T> From<T> for VertexAnnotations
where
    BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>>: From<T>,
{
    fn from(t: T) -> Self {
        Self { annotations: t.into() }
    }
}

pub fn infer_types(
    snapshot: &impl ReadableSnapshot,
    block: &Block,
    variable_registry: &VariableRegistry,
    type_manager: &TypeManager,
    previous_stage_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<TypeAnnotation>>>,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    is_write_stage: bool,
) -> Result<TypeAnnotations, TypeInferenceError> {
    let graph = compute_type_inference_graph(
        snapshot,
        block,
        variable_registry,
        type_manager,
        previous_stage_variable_annotations,
        annotated_function_signatures,
        is_write_stage,
    )?;
    let mut vertex_annotations = BTreeMap::new();
    let mut constraint_annotations = HashMap::new();
    graph.collect_type_annotations(&mut vertex_annotations, &mut constraint_annotations);
    let type_annotations = TypeAnnotations::new(vertex_annotations, constraint_annotations);
    debug_assert!(block.block_context().referenced_variables().all(|var| {
        match variable_registry.get_variable_category(var) {
            None => {
                unreachable!("Safe to ignore. But can we know the ValueTypeCategory of an assignment at translation?")
            }
            Some(VariableCategory::Value) | Some(VariableCategory::ValueList) => true,
            Some(_) => type_annotations.vertex_annotations_of(&Vertex::Variable(var)).is_some(),
        }
    }));
    Ok(type_annotations)
}

pub(crate) fn compute_type_inference_graph<'graph>(
    snapshot: &impl ReadableSnapshot,
    block: &'graph Block,
    variable_registry: &VariableRegistry,
    type_manager: &TypeManager,
    previous_stage_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<TypeAnnotation>>>,
    annotated_function_signatures: &dyn AnnotatedFunctionSignatures,
    is_write_stage: bool,
) -> Result<TypeInferenceGraph<'graph>, TypeInferenceError> {
    let mut graph = TypeGraphSeedingContext::new(
        snapshot,
        type_manager,
        annotated_function_signatures,
        variable_registry,
        is_write_stage,
    )
    .create_graph(block.block_context(), previous_stage_variable_annotations, block.conjunction())?;
    prune_types(&mut graph);
    // TODO: Throw error when any set becomes empty happens, rather than waiting for the it to propagate
    graph.check_thing_constraints_satisfiable(variable_registry)?;
    Ok(graph)
}

pub(crate) fn prune_types(graph: &mut TypeInferenceGraph<'_>) {
    while graph.prune_vertices_from_constraints() {
        graph.prune_constraints_from_vertices();
    }

    // Then do it for the nested negations & optionals
    // TODO: This is too permissive. We should have seeded these with the pruned types from the parent.
    prune_types_for_nested_negations_and_optionals(graph);
}

fn prune_types_for_nested_negations_and_optionals(graph: &mut TypeInferenceGraph<'_>) {
    graph
        .nested_disjunctions
        .iter_mut()
        .flat_map(|disjunction| disjunction.disjunction.iter_mut())
        .for_each(|nested| prune_types_for_nested_negations_and_optionals(nested));
    chain(graph.nested_negations.iter_mut(), graph.nested_optionals.iter_mut()).for_each(|nested| {
        for (vertex, parent_annotations) in &graph.vertices.annotations {
            if let Some(nested_annotations) = nested.vertices.annotations.get_mut(vertex) {
                nested_annotations.retain(|t| parent_annotations.contains(t));
            }
        }
        prune_types(nested)
    });
}

#[derive(Debug)]
pub(crate) struct TypeInferenceGraph<'this> {
    pub(crate) conjunction: &'this Conjunction,
    pub(crate) vertices: VertexAnnotations,
    pub(crate) edges: Vec<TypeInferenceEdge<'this>>,
    pub(crate) nested_disjunctions: Vec<NestedTypeInferenceGraphDisjunction<'this>>,
    pub(crate) nested_negations: Vec<TypeInferenceGraph<'this>>,
    pub(crate) nested_optionals: Vec<TypeInferenceGraph<'this>>,
}

impl TypeInferenceGraph<'_> {
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

    pub(crate) fn collect_type_annotations(
        self,
        vertex_annotations: &mut BTreeMap<Vertex<Variable>, Arc<BTreeSet<TypeAnnotation>>>,
        constraint_annotations: &mut HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    ) {
        let TypeInferenceGraph { vertices, edges, nested_disjunctions, nested_negations, nested_optionals, .. } = self;

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

        vertices.into_iter().for_each(|(variable, types)| {
            vertex_annotations.entry(variable).or_insert_with(|| Arc::new(types));
        });

        chain(
            chain(nested_negations, nested_optionals),
            nested_disjunctions.into_iter().flat_map(|disjunction| disjunction.disjunction),
        )
        .for_each(|nested| nested.collect_type_annotations(vertex_annotations, constraint_annotations));
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
            .chain(self.nested_optionals.iter())
            .chain(self.nested_negations.iter())
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
        debug_assert!(initial_left_to_right
            .iter()
            .all(|(u, vs)| vs.iter().all(|v| initial_right_to_left.get(v).unwrap().contains(u))));
        debug_assert!(initial_right_to_left
            .iter()
            .all(|(u, vs)| vs.iter().all(|v| initial_left_to_right.get(v).unwrap().contains(u))));
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
            left_vertex_annotations.retain(|k| self.left_to_right.contains_key(k));
            is_modified = is_modified || size_before != left_vertex_annotations.len();
        };
        {
            let right_vertex_annotations = vertices.get_mut(&self.right).unwrap();
            let size_before = right_vertex_annotations.len();
            right_vertex_annotations.retain(|k| self.right_to_left.contains_key(k));
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
            left_to_right.retain(|left_type, _| left_vertex_annotations.contains(left_type));
        };
        {
            let right_vertex_annotations = vertices.get(&self.right).unwrap();
            right_to_left.iter().filter(|(right_type, _)| !right_vertex_annotations.contains(*right_type)).for_each(
                |(right_type, left_keys)| Self::remove_type_from_values_of(right_type, left_keys, left_to_right),
            );
            right_to_left.retain(|left_type, _| right_vertex_annotations.contains(left_type));
        };
    }
}

#[derive(Debug)]
pub(crate) struct NestedTypeInferenceGraphDisjunction<'this> {
    pub(crate) disjunction: Vec<TypeInferenceGraph<'this>>,
    pub(crate) shared_variables: BTreeSet<Variable>,
    pub(crate) shared_vertex_annotations: VertexAnnotations,
}

impl NestedTypeInferenceGraphDisjunction<'_> {
    fn prune_self_from_vertices(&mut self, parent_vertices: &VertexAnnotations) {
        for nested_graph in &mut self.disjunction {
            for (vertex, vertex_types) in &mut nested_graph.vertices {
                if let Some(parent_vertex_types) = parent_vertices.get(vertex) {
                    vertex_types.retain(|type_| parent_vertex_types.contains(type_))
                }
            }
            nested_graph.prune_constraints_from_vertices();
        }
    }

    fn prune_vertices_from_self(&mut self, parent_vertices: &mut VertexAnnotations) -> bool {
        let mut is_modified = false;
        for nested_graph in &mut self.disjunction {
            is_modified |= nested_graph.prune_vertices_from_constraints();
        }

        for (parent_vertex, parent_vertex_types) in parent_vertices {
            let size_before = parent_vertex_types.len();
            parent_vertex_types.retain(|type_| {
                self.disjunction.iter().any(|nested_graph| {
                    nested_graph
                        .vertices
                        .get(parent_vertex)
                        .map(|nested_types| nested_types.contains(type_))
                        .unwrap_or(true)
                })
            });
            is_modified |= size_before != parent_vertex_types.len();
        }
        is_modified
    }
}

#[cfg(test)]
pub mod tests {}
