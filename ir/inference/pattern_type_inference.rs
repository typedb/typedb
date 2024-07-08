/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type as TypeAnnotation, Type};
use concept::type_::type_manager::TypeManager;
use itertools::chain;
use storage::snapshot::ReadableSnapshot;

use crate::{
    inference::{
        seed_types::TypeSeeder,
        type_inference::{
            ConstraintTypeAnnotations, LeftRightAnnotations, LeftRightFilteredAnnotations, VertexAnnotations,
        },
        TypeInferenceError,
    },
    pattern::{conjunction::Conjunction, constraint::Constraint},
    program::block::BlockContext,
};

/*
The type inference algorithm involves 2 phases:
1. The seed phase annotates each vertex with a super-set of possible types.
2. The refinement phase then iteratively intersects the type annotations on vertices and edges, and vice-versa.

Seed phase:
    0. We first infer types for vertices from labels and function return/argument types.
    1. We recursively propagate annotations over constraints for any unannotated variable is attached to an annotated one.
    2. If any vertex remains unannotated, we pick one of them, assign it all possible types from its category, and goto 1.
    3. For each edge, we create a mapping from a type on the left (resp. right) to a set of possible types on the right (resp. left).
    4. We make the two maps "consistent"  i.e. left_to_right.keys = right_to_left.values and vice-versa.
    5. Any keys with an empty-set as value is removed.

Refinement phase:
    Repeat till the vertex annotations remain unchanged:
    0. Refine the annotations for each vertex by intersecting them with the annotations on each edge they're involved in.
    1. Refine the annotations on each edge by intersecting them with the annotations of the vertex.

The current implementation is naive in that it re-evaluates every vertex and every edge in every iteration.
It should be easy to optimise by keeping track of which vertices have changed and only using the attached constraints to refine/seed.
It should also be possible to interleave the two phases, as in the original design:
    Add all variables to frontier.
    Repeat till the frontier is empty:
        Pick a variable from the frontier.
        For each constraint attached, addOrIntersect types possible for the neighbor variable, add neighbor-var to frontier if it's annotations have changed
*/

pub(crate) fn infer_types_for_conjunction<'graph>(
    snapshot: &impl ReadableSnapshot,
    context: &BlockContext,
    type_manager: &TypeManager,
    conjunction: &'graph Conjunction,
) -> Result<TypeInferenceGraph<'graph>, TypeInferenceError> {
    let mut tig = TypeSeeder::new(snapshot, type_manager).seed_types(context, conjunction)?;
    run_type_inference(&mut tig);
    Ok(tig)
}

fn run_type_inference(tig: &mut TypeInferenceGraph<'_>) {
    let mut is_modified = tig.prune_vertices_from_constraints();
    while is_modified {
        tig.prune_constraints_from_vertices();
        is_modified = tig.prune_vertices_from_constraints();
    }

    // Then do it for the nested negations & optionals
    tig.nested_negations.iter_mut().for_each(|nested| run_type_inference(nested));
    tig.nested_optionals.iter_mut().for_each(|nested| run_type_inference(nested));
}

#[derive(Debug)]
pub struct TypeInferenceGraph<'this> {
    pub(crate) conjunction: &'this Conjunction,
    pub(crate) vertices: VertexAnnotations,
    pub(crate) edges: Vec<TypeInferenceEdge<'this>>,
    pub(crate) nested_disjunctions: Vec<NestedTypeInferenceGraphDisjunction<'this>>,
    pub(crate) nested_negations: Vec<TypeInferenceGraph<'this>>,
    pub(crate) nested_optionals: Vec<TypeInferenceGraph<'this>>,
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
            is_modified = is_modified || edge.prune_vertices_from_self(&mut self.vertices);
        }
        for nested_graph in &mut self.nested_disjunctions {
            is_modified = is_modified || nested_graph.prune_vertices_from_self(&mut self.vertices);
        }
        is_modified
    }

    pub(crate) fn collect_type_annotations(
        self,
        variable_annotations: &mut HashMap<Variable, Arc<HashSet<Type>>>,
        constraint_annotations: &mut HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    ) {
        let TypeInferenceGraph { vertices, edges, nested_disjunctions, nested_negations, nested_optionals, .. } = self;

        let mut combine_role_player_edges = HashMap::new();
        edges.into_iter().for_each(|edge| {
            let TypeInferenceEdge { constraint, left_to_right, right_to_left, .. } = edge;
            if let Constraint::RolePlayer(rp) = edge.constraint {
                if let Some((other_left_right, other_right_left)) = combine_role_player_edges.remove(&edge.right) {
                    let lrf_annotation = {
                        if edge.left == rp.relation {
                            LeftRightFilteredAnnotations::build(
                                left_to_right,
                                right_to_left,
                                other_left_right,
                                other_right_left,
                            )
                        } else {
                            LeftRightFilteredAnnotations::build(
                                other_left_right,
                                other_right_left,
                                left_to_right,
                                right_to_left,
                            )
                        }
                    };
                    constraint_annotations
                        .insert(constraint.clone(), ConstraintTypeAnnotations::LeftRightFiltered(lrf_annotation));
                } else {
                    combine_role_player_edges.insert(edge.right, (left_to_right, right_to_left));
                }
            } else {
                let lr_annotations = LeftRightAnnotations::build(left_to_right, right_to_left);
                constraint_annotations.insert(constraint.clone(), ConstraintTypeAnnotations::LeftRight(lr_annotations));
            }
        });

        vertices.into_iter().for_each(|(variable, types)| {
            if !variable_annotations.contains_key(&variable) {
                variable_annotations.insert(variable, Arc::new(types.into_iter().collect::<HashSet<TypeAnnotation>>()));
            }
        });

        chain(
            chain(nested_negations, nested_optionals),
            nested_disjunctions.into_iter().flat_map(|disjunction| disjunction.disjunction),
        )
        .for_each(|nested| nested.collect_type_annotations(variable_annotations, constraint_annotations));
    }
}

#[derive(Debug)]

pub struct TypeInferenceEdge<'this> {
    pub(crate) constraint: &'this Constraint<Variable>,
    pub(crate) left: Variable,
    pub(crate) right: Variable,
    pub(crate) left_to_right: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
    pub(crate) right_to_left: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
}

impl<'this> TypeInferenceEdge<'this> {
    // Construction

    pub(crate) fn build(
        constraint: &'this Constraint<Variable>,
        left: Variable,
        right: Variable,
        initial_left_to_right: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
        initial_right_to_left: BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
    ) -> TypeInferenceEdge<'this> {
        // The final left_to_right & right_to_left sets must be consistent with each other. i.e.
        //      left_to_right.keys() == union(right_to_left.values()) AND
        //      right_to_left.keys() == union(left_to_right.values())
        // This is a pre-condition to the type-inference loop.
        let mut left_to_right = initial_left_to_right;
        let mut right_to_left = initial_right_to_left;
        let left_types = Self::intersect_first_keys_with_union_of_second_values(&left_to_right, &right_to_left);
        let right_types = Self::intersect_first_keys_with_union_of_second_values(&right_to_left, &left_to_right);
        Self::prune_keys_not_in_first_and_values_not_in_second(&mut left_to_right, &left_types, &right_types);
        Self::prune_keys_not_in_first_and_values_not_in_second(&mut right_to_left, &right_types, &left_types);
        TypeInferenceEdge { constraint, left, right, left_to_right, right_to_left }
    }

    fn intersect_first_keys_with_union_of_second_values(
        keys_from: &BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
        values_from: &BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
    ) -> BTreeSet<TypeAnnotation> {
        values_from.values().flatten().filter(|v| keys_from.contains_key(v)).cloned().collect()
    }

    fn prune_keys_not_in_first_and_values_not_in_second(
        prune_from: &mut BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>,
        allowed_keys: &BTreeSet<TypeAnnotation>,
        allowed_values: &BTreeSet<TypeAnnotation>,
    ) {
        prune_from.retain(|type_, _| allowed_keys.contains(type_));
        for v in prune_from.values_mut() {
            v.retain(|type_| allowed_values.contains(type_));
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

    // prune_vertices
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
    pub(crate) shared_vertex_annotations: BTreeMap<Variable, BTreeSet<TypeAnnotation>>,
}

impl<'this> NestedTypeInferenceGraphDisjunction<'this> {
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
            nested_graph.prune_vertices_from_constraints();
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
pub mod tests {

    use std::collections::{BTreeMap, BTreeSet};

    use answer::{variable::Variable, Type as TypeAnnotation};
    use itertools::Itertools;

    use crate::{
        inference::{
            pattern_type_inference::{
                infer_types_for_conjunction, NestedTypeInferenceGraphDisjunction, TypeInferenceEdge, TypeInferenceGraph,
            },
            tests::{
                managers,
                schema_consts::{
                    setup_types, LABEL_ANIMAL, LABEL_CAT, LABEL_CATNAME, LABEL_DOG, LABEL_DOGNAME, LABEL_FEARS,
                    LABEL_NAME,
                },
                setup_storage,
            },
        },
        pattern::{
            conjunction::Conjunction,
            constraint::{Constraint, IsaKind},
            ScopeId,
        },
        program::block::BlockContext,
    };

    pub(crate) fn expected_edge(
        constraint: &Constraint<Variable>,
        left: Variable,
        right: Variable,
        left_right_type_pairs: Vec<(TypeAnnotation, TypeAnnotation)>,
    ) -> TypeInferenceEdge<'_> {
        let mut left_to_right = BTreeMap::new();
        let mut right_to_left = BTreeMap::new();
        for (l, r) in left_right_type_pairs {
            if !left_to_right.contains_key(&l) {
                left_to_right.insert(l.clone(), BTreeSet::new());
            }
            left_to_right.get_mut(&l).unwrap().insert(r.clone());
            if !right_to_left.contains_key(&r) {
                right_to_left.insert(r.clone(), BTreeSet::new());
            }
            right_to_left.get_mut(&r).unwrap().insert(l.clone());
        }
        TypeInferenceEdge { constraint, left, right, left_to_right, right_to_left }
    }

    #[test]
    fn basic_binary_edges() {
        // Some version of `$a isa animal, has name $n;`
        let storage = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);

        let all_animals = BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]);
        let all_names = BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()]);

        {
            // Case 1: $a isa cat, has animal-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut context = BlockContext::new();
            let mut conjunction = Conjunction::new(ScopeId::ROOT);
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(&mut context, name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_animal, var_animal_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_animal_type, LABEL_CAT).unwrap();
            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_name, var_name_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_name_type, LABEL_NAME).unwrap();
            conjunction.constraints_mut().add_has(&mut context, var_animal, var_name).unwrap();
            let constraints = conjunction.constraints().constraints();

            let tig = infer_types_for_conjunction(&snapshot, &context, &type_manager, &conjunction).unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: &conjunction,
                vertices: BTreeMap::from([
                    (var_animal, BTreeSet::from([type_cat.clone()])),
                    (var_name, BTreeSet::from([type_catname.clone()])),
                    (var_animal_type, BTreeSet::from([type_cat.clone()])),
                    (var_name_type, BTreeSet::from([type_name.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal,
                        var_animal_type,
                        vec![(type_cat.clone(), type_cat.clone())],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name,
                        var_name_type,
                        vec![(type_catname.clone(), type_name.clone())],
                    ),
                    expected_edge(
                        &constraints[4],
                        var_animal,
                        var_name,
                        vec![(type_cat.clone(), type_catname.clone())],
                    ),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };

            assert_eq!(expected_tig, tig);
        }

        {
            // Case 2: $a isa animal, has cat-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut context = BlockContext::new();
            let mut conjunction = Conjunction::new(ScopeId::ROOT);
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(&mut context, name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_animal, var_animal_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_animal_type, LABEL_ANIMAL).unwrap();
            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_name, var_name_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_name_type, LABEL_CATNAME).unwrap();
            conjunction.constraints_mut().add_has(&mut context, var_animal, var_name).unwrap();
            let constraints = conjunction.constraints().constraints();
            let tig = infer_types_for_conjunction(&snapshot, &context, &type_manager, &conjunction).unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: &conjunction,
                vertices: BTreeMap::from([
                    (var_animal, BTreeSet::from([type_cat.clone()])),
                    (var_name, BTreeSet::from([type_catname.clone()])),
                    (var_animal_type, BTreeSet::from([type_animal.clone()])),
                    (var_name_type, BTreeSet::from([type_catname.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal,
                        var_animal_type,
                        vec![(type_cat.clone(), type_animal.clone())],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name,
                        var_name_type,
                        vec![(type_catname.clone(), type_catname.clone())],
                    ),
                    expected_edge(
                        &constraints[4],
                        var_animal,
                        var_name,
                        vec![(type_cat.clone(), type_catname.clone())],
                    ),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };
            assert_eq!(expected_tig, tig);
        }

        {
            // Case 3: $a isa cat, has dog-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut context = BlockContext::new();
            let mut conjunction = Conjunction::new(ScopeId::ROOT);
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(&mut context, name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_animal, var_animal_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_animal_type, LABEL_CAT).unwrap();
            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_name, var_name_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_name_type, LABEL_DOGNAME).unwrap();
            conjunction.constraints_mut().add_has(&mut context, var_animal, var_name).unwrap();

            let constraints = conjunction.constraints().constraints();
            let tig = infer_types_for_conjunction(&snapshot, &context, &type_manager, &conjunction).unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: &conjunction,
                vertices: BTreeMap::from([
                    (var_animal, BTreeSet::new()),
                    (var_name, BTreeSet::new()),
                    (var_animal_type, BTreeSet::new()),
                    (var_name_type, BTreeSet::new()),
                ]),
                edges: vec![
                    expected_edge(&constraints[0], var_animal, var_animal_type, Vec::new()),
                    expected_edge(&constraints[2], var_name, var_name_type, Vec::new()),
                    expected_edge(&constraints[4], var_animal, var_name, Vec::new()),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };
            assert_eq!(expected_tig, tig);
        }

        {
            // Case 4: $a isa animal, has name $n; // Just to be sure
            let types_a = all_animals.clone();
            let types_n = all_names.clone();
            let snapshot = storage.clone().open_snapshot_write();
            let mut context = BlockContext::new();
            let mut conjunction = Conjunction::new(ScopeId::ROOT);
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(&mut context, name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_animal, var_animal_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_animal_type, LABEL_ANIMAL).unwrap();
            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_name, var_name_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_name_type, LABEL_NAME).unwrap();
            conjunction.constraints_mut().add_has(&mut context, var_animal, var_name).unwrap();
            let constraints = conjunction.constraints().constraints();
            let tig = infer_types_for_conjunction(&snapshot, &context, &type_manager, &conjunction).unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: &conjunction,
                vertices: BTreeMap::from([
                    (var_animal, types_a),
                    (var_name, types_n),
                    (var_animal_type, BTreeSet::from([type_animal.clone()])),
                    (var_name_type, BTreeSet::from([type_name.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal,
                        var_animal_type,
                        vec![
                            (type_cat.clone(), type_animal.clone()),
                            (type_dog.clone(), type_animal.clone()),
                            (type_animal.clone(), type_animal.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name,
                        var_name_type,
                        vec![
                            (type_catname.clone(), type_name.clone()),
                            (type_dogname.clone(), type_name.clone()),
                            (type_name.clone(), type_name.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[4],
                        var_animal,
                        var_name,
                        vec![
                            (type_cat.clone(), type_catname.clone()),
                            (type_dog.clone(), type_dogname.clone()),
                            (type_animal.clone(), type_name.clone()),
                        ],
                    ),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };
            assert_eq!(expected_tig, tig);
        }
    }

    #[test]
    fn basic_nested_graphs() {
        // Some version of `$a isa animal, has name $n;`
        let storage = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);

        let all_animals = BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]);
        let all_names = BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()]);

        let mut context = BlockContext::new();
        let mut conjunction = Conjunction::new(ScopeId::ROOT);
        let (var_animal, var_name, var_name_type) = ["animal", "name", "name_type"]
            .into_iter()
            .map(|name| conjunction.get_or_declare_variable(&mut context, name).unwrap())
            .collect_tuple()
            .unwrap();
        {
            // Case 1: {$a isa cat;} or {$a isa dog;} $a has animal-name $n;

            let (b1_var_animal_type, b2_var_animal_type) = {
                let disj = conjunction.add_disjunction(&mut context);

                let branch1 = disj.add_conjunction(&mut context);
                let b1_var_animal_type = branch1.get_or_declare_variable(&mut context, "b1_animal_type").unwrap();
                branch1
                    .constraints_mut()
                    .add_isa(&mut context, IsaKind::Subtype, var_animal, b1_var_animal_type)
                    .unwrap();
                branch1.constraints_mut().add_label(&mut context, b1_var_animal_type, LABEL_CAT).unwrap();

                let branch2 = disj.add_conjunction(&mut context);
                let b2_var_animal_type = branch2.get_or_declare_variable(&mut context, "b2_animal_type").unwrap();
                branch2
                    .constraints_mut()
                    .add_isa(&mut context, IsaKind::Subtype, var_animal, b2_var_animal_type)
                    .unwrap();
                branch2.constraints_mut().add_label(&mut context, b2_var_animal_type, LABEL_DOG).unwrap();

                (b1_var_animal_type, b2_var_animal_type)
            };
            conjunction.constraints_mut().add_label(&mut context, var_name_type, "name").unwrap();
            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_name, var_name_type).unwrap();
            conjunction.constraints_mut().add_has(&mut context, var_animal, var_name).unwrap();

            let snapshot = storage.clone().open_snapshot_write();
            let tig = infer_types_for_conjunction(&snapshot, &context, &type_manager, &conjunction).unwrap();

            let disj = conjunction.nested_patterns().first().unwrap().as_disjunction().unwrap();
            let [b1, b2] = &disj.conjunctions()[..] else { unreachable!() };
            let b1_isa = &b1.constraints().constraints()[0];
            let b2_isa = &b2.constraints().constraints()[0];
            let expected_nested_graphs = vec![
                TypeInferenceGraph {
                    conjunction: b1,
                    vertices: BTreeMap::from([
                        (var_animal, BTreeSet::from([type_cat.clone()])),
                        (b1_var_animal_type, BTreeSet::from([type_cat.clone()])),
                    ]),
                    edges: vec![expected_edge(
                        b1_isa,
                        var_animal,
                        b1_var_animal_type,
                        vec![(type_cat.clone(), type_cat.clone())],
                    )],
                    nested_disjunctions: Vec::new(),
                    nested_negations: Vec::new(),
                    nested_optionals: Vec::new(),
                },
                TypeInferenceGraph {
                    conjunction: b2,
                    vertices: BTreeMap::from([
                        (var_animal, BTreeSet::from([type_dog.clone()])),
                        (b2_var_animal_type, BTreeSet::from([type_dog.clone()])),
                    ]),
                    edges: vec![expected_edge(
                        b2_isa,
                        var_animal,
                        b2_var_animal_type,
                        vec![(type_dog.clone(), type_dog.clone())],
                    )],
                    nested_disjunctions: Vec::new(),
                    nested_negations: Vec::new(),
                    nested_optionals: Vec::new(),
                },
            ];
            let expected_graph = TypeInferenceGraph {
                conjunction: &conjunction,
                vertices: BTreeMap::from([
                    (var_animal, BTreeSet::from([type_cat.clone(), type_dog.clone()])),
                    (var_name, BTreeSet::from([type_catname.clone(), type_dogname.clone()])),
                    (var_name_type, BTreeSet::from([type_name.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &conjunction.constraints().constraints()[1],
                        var_name,
                        var_name_type,
                        vec![(type_catname.clone(), type_name.clone()), (type_dogname.clone(), type_name.clone())],
                    ),
                    expected_edge(
                        &conjunction.constraints().constraints()[2],
                        var_animal,
                        var_name,
                        vec![(type_cat.clone(), type_catname.clone()), (type_dog.clone(), type_dogname.clone())],
                    ),
                ],
                nested_disjunctions: vec![NestedTypeInferenceGraphDisjunction {
                    disjunction: expected_nested_graphs,
                    shared_variables: BTreeSet::new(),
                    shared_vertex_annotations: BTreeMap::new(),
                }],
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };

            assert_eq!(expected_graph, tig);
        }
    }

    #[test]
    fn no_type_constraints() {
        let storage = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);

        let all_animals = BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]);
        let all_names = BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()]);

        {
            // Case 1: $a has $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut context = BlockContext::new();
            let mut conjunction = Conjunction::new(ScopeId::ROOT);
            let (var_animal, var_name) = ["animal", "name"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(&mut context, name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_has(&mut context, var_animal, var_name).unwrap();

            let constraints = conjunction.constraints().constraints();
            let tig = infer_types_for_conjunction(&snapshot, &context, &type_manager, &conjunction).unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: &conjunction,
                vertices: BTreeMap::from([
                    (var_animal, BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()])),
                    (var_name, BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()])),
                ]),
                edges: vec![expected_edge(
                    &constraints[0],
                    var_animal,
                    var_name,
                    vec![
                        (type_animal.clone(), type_name.clone()),
                        (type_cat, type_catname.clone()),
                        (type_dog, type_dogname.clone()),
                    ],
                )],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };

            assert_eq!(expected_tig, tig);
        }
    }

    #[test]
    fn role_players() {
        let storage = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), _, (type_fears, type_has_fear, type_is_feared)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);

        {
            // With roles specified
            let snapshot = storage.clone().open_snapshot_write();
            let mut context = BlockContext::new();
            let mut conjunction = Conjunction::new(ScopeId::ROOT);
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
            .map(|name| conjunction.get_or_declare_variable(&mut context, name).unwrap())
            .collect_tuple()
            .unwrap();

            conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_fears, var_fears_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_fears_type, LABEL_FEARS).unwrap();
            conjunction
                .constraints_mut()
                .add_role_player(&mut context, var_fears, var_has_fear, Some(var_role_has_fear))
                .unwrap();
            conjunction
                .constraints_mut()
                .add_role_player(&mut context, var_fears, var_is_feared, Some(var_role_is_feared))
                .unwrap();

            conjunction.constraints_mut().add_sub(&mut context, var_role_has_fear, var_role_has_fear_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_role_has_fear_type, "fears:has-fear").unwrap();
            conjunction.constraints_mut().add_sub(&mut context, var_role_is_feared, var_role_is_feared_type).unwrap();
            conjunction.constraints_mut().add_label(&mut context, var_role_is_feared_type, "fears:is-feared").unwrap();

            let constraints = conjunction.constraints().constraints();

            let tig = infer_types_for_conjunction(&snapshot, &context, &type_manager, &conjunction).unwrap();

            let expected_graph = TypeInferenceGraph {
                conjunction: &conjunction,
                vertices: BTreeMap::from([
                    (var_has_fear, BTreeSet::from([type_cat.clone()])),
                    (var_is_feared, BTreeSet::from([type_dog.clone()])),
                    (var_fears_type, BTreeSet::from([type_fears.clone()])),
                    (var_fears, BTreeSet::from([type_fears.clone()])),
                    (var_role_has_fear, BTreeSet::from([type_has_fear.clone()])),
                    (var_role_is_feared, BTreeSet::from([type_is_feared.clone()])),
                    (var_role_has_fear_type, BTreeSet::from([type_has_fear.clone()])),
                    (var_role_is_feared_type, BTreeSet::from([type_is_feared.clone()])),
                ]),
                edges: vec![
                    // isa
                    expected_edge(
                        &conjunction.constraints().constraints()[0],
                        var_fears,
                        var_fears_type,
                        vec![(type_fears.clone(), type_fears.clone())],
                    ),
                    // has-fear edge
                    expected_edge(
                        &conjunction.constraints().constraints()[2],
                        var_fears,
                        var_role_has_fear,
                        vec![(type_fears.clone(), type_has_fear.clone())],
                    ),
                    expected_edge(
                        &conjunction.constraints().constraints()[2],
                        var_has_fear,
                        var_role_has_fear,
                        vec![(type_cat.clone(), type_has_fear.clone())],
                    ),
                    // is-feared edge
                    expected_edge(
                        &conjunction.constraints().constraints()[3],
                        var_fears,
                        var_role_is_feared,
                        vec![(type_fears.clone(), type_is_feared.clone())],
                    ),
                    expected_edge(
                        &conjunction.constraints().constraints()[3],
                        var_is_feared,
                        var_role_is_feared,
                        vec![(type_dog.clone(), type_is_feared.clone())],
                    ),
                    expected_edge(
                        &conjunction.constraints().constraints()[4],
                        var_role_has_fear,
                        var_role_has_fear_type,
                        vec![(type_has_fear.clone(), type_has_fear.clone())],
                    ),
                    expected_edge(
                        &conjunction.constraints().constraints()[6],
                        var_role_is_feared,
                        var_role_is_feared_type,
                        vec![(type_is_feared.clone(), type_is_feared.clone())],
                    ),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };

            assert_eq!(expected_graph, tig);
        }
    }
}
