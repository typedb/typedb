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
    pattern::{conjunction::Conjunction, constraint::Constraint, Vertex},
    program::block::{FunctionalBlock, VariableRegistry},
};
use itertools::chain;
use storage::snapshot::ReadableSnapshot;

use crate::match_::inference::{
    annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
    type_annotations::{ConstraintTypeAnnotations, LeftRightAnnotations, LeftRightFilteredAnnotations},
    type_seeder::TypeSeeder,
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

pub(crate) fn infer_types_for_block<'graph>(
    snapshot: &impl ReadableSnapshot,
    block: &'graph FunctionalBlock,
    variable_registry: &VariableRegistry,
    type_manager: &TypeManager,
    previous_stage_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<TypeAnnotation>>>,
    schema_functions: &IndexedAnnotatedFunctions,
    local_function_cache: Option<&AnnotatedUnindexedFunctions>,
) -> Result<TypeInferenceGraph<'graph>, TypeInferenceError> {
    let mut tig = TypeSeeder::new(snapshot, type_manager, schema_functions, local_function_cache, variable_registry)
        .seed_types(block.scope_context(), previous_stage_variable_annotations, block.conjunction())?;
    run_type_inference(&mut tig);
    // TODO: Throw error when any set becomes empty happens, rather than waiting for the it to propagate
    if tig.vertices.iter().any(|(var, types)| types.is_empty()) {
        Err(TypeInferenceError::DetectedUnsatisfiablePattern {})
    } else {
        Ok(tig)
    }
}

fn run_type_inference(tig: &mut TypeInferenceGraph<'_>) {
    while tig.prune_vertices_from_constraints() {
        tig.prune_constraints_from_vertices();
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
                    combine_links_edges.insert(edge.right, (left_to_right, right_to_left));
                }
            } else {
                let lr_annotations = LeftRightAnnotations::build(left_to_right, right_to_left);
                constraint_annotations.insert(constraint.clone(), ConstraintTypeAnnotations::LeftRight(lr_annotations));
            }
        });

        vertices.into_iter().for_each(|(variable, types)| {
            vertex_annotations.entry(variable).or_insert_with(|| Arc::new(types.into_iter().collect()));
        });

        chain(
            chain(nested_negations, nested_optionals),
            nested_disjunctions.into_iter().flat_map(|disjunction| disjunction.disjunction),
        )
        .for_each(|nested| nested.collect_type_annotations(vertex_annotations, constraint_annotations));
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
    use ir::{
        pattern::{
            constraint::{Constraint, IsaKind, SubKind},
            Vertex,
        },
        program::{block::FunctionalBlock, function_signature::HashMapFunctionSignatureIndex},
        translation::TranslationContext,
    };
    use itertools::Itertools;
    use test_utils::assert_matches;

    use crate::match_::inference::{
        annotated_functions::IndexedAnnotatedFunctions,
        pattern_type_inference::{
            infer_types_for_block, NestedTypeInferenceGraphDisjunction, TypeInferenceEdge, TypeInferenceGraph,
            VertexAnnotations,
        },
        tests::{
            managers,
            schema_consts::{
                setup_types, LABEL_ANIMAL, LABEL_CAT, LABEL_CATNAME, LABEL_DOG, LABEL_DOGNAME, LABEL_FEARS, LABEL_NAME,
            },
            setup_storage,
        },
        TypeInferenceError,
    };

    pub(crate) fn expected_edge(
        constraint: &Constraint<Variable>,
        left: Vertex<Variable>,
        right: Vertex<Variable>,
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
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let all_animals = BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]);
        let all_names = BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()]);

        {
            // Case 1: $a isa cat, has name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_NAME.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();
            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name.into(), BTreeSet::from([type_catname.clone(), type_name.clone()])),
                    (var_animal_type.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name_type.into(), BTreeSet::from([type_name.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat.clone(), type_cat.clone())],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname.clone(), type_name.clone()), (type_name.clone(), type_name.clone())],
                    ),
                    expected_edge(
                        &constraints[4],
                        var_animal.into(),
                        var_name.into(),
                        vec![(type_cat.clone(), type_name.clone()), (type_cat.clone(), type_catname.clone())],
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
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_ANIMAL.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_CATNAME.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();

            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name.into(), BTreeSet::from([type_catname.clone()])),
                    (var_animal_type.into(), BTreeSet::from([type_animal.clone()])),
                    (var_name_type.into(), BTreeSet::from([type_catname.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat.clone(), type_animal.clone())],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname.clone(), type_catname.clone())],
                    ),
                    expected_edge(
                        &constraints[4],
                        var_animal.into(),
                        var_name.into(),
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
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_DOGNAME.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();
            let err = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap_err();

            assert_matches!(err, TypeInferenceError::DetectedUnsatisfiablePattern {})
        }

        {
            // Case 4: $a isa animal, has name $n; // Just to be sure
            let types_a = all_animals.clone();
            let types_n = all_names.clone();
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_ANIMAL.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_NAME.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();
            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), types_a),
                    (var_name.into(), types_n),
                    (var_animal_type.into(), BTreeSet::from([type_animal.clone()])),
                    (var_name_type.into(), BTreeSet::from([type_name.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![
                            (type_cat.clone(), type_animal.clone()),
                            (type_dog.clone(), type_animal.clone()),
                            (type_animal.clone(), type_animal.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_name_type.into(),
                        vec![
                            (type_catname.clone(), type_name.clone()),
                            (type_dogname.clone(), type_name.clone()),
                            (type_name.clone(), type_name.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[4],
                        var_animal.into(),
                        var_name.into(),
                        vec![
                            (type_cat.clone(), type_catname.clone()),
                            (type_cat.clone(), type_name.clone()),
                            (type_dog.clone(), type_dogname.clone()),
                            (type_dog.clone(), type_name.clone()),
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
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let mut translation_context = TranslationContext::new();
        let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
        let mut conjunction = builder.conjunction_mut();
        let (var_animal, var_name, var_name_type) = ["animal", "name", "name_type"]
            .into_iter()
            .map(|name| conjunction.get_or_declare_variable(name).unwrap())
            .collect_tuple()
            .unwrap();

        // Case 1: {$a isa cat;} or {$a isa dog;} $a has animal-name $n;
        conjunction.constraints_mut().add_label(var_name_type, "name").unwrap();
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
        conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

        let mut disj = conjunction.add_disjunction();

        let mut branch1 = disj.add_conjunction();
        let b1_var_animal_type = branch1.get_or_declare_variable("b1_animal_type").unwrap();
        branch1.constraints_mut().add_isa(IsaKind::Subtype, var_animal, b1_var_animal_type.into()).unwrap();
        branch1.constraints_mut().add_label(b1_var_animal_type, LABEL_CAT.scoped_name().as_str()).unwrap();

        let mut branch2 = disj.add_conjunction();
        let b2_var_animal_type = branch2.get_or_declare_variable("b2_animal_type").unwrap();
        branch2.constraints_mut().add_isa(IsaKind::Subtype, var_animal, b2_var_animal_type.into()).unwrap();
        branch2.constraints_mut().add_label(b2_var_animal_type, LABEL_DOG.scoped_name().as_str()).unwrap();

        let (b1_var_animal_type, b2_var_animal_type) = (b1_var_animal_type, b2_var_animal_type);

        let block = builder.finish();

        let snapshot = storage.clone().open_snapshot_write();
        let tig = infer_types_for_block(
            &snapshot,
            &block,
            &translation_context.variable_registry,
            &type_manager,
            &BTreeMap::new(),
            &IndexedAnnotatedFunctions::empty(),
            None,
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
                    (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                    (b1_var_animal_type.into(), BTreeSet::from([type_cat.clone()])),
                ]),
                edges: vec![expected_edge(
                    b1_isa,
                    var_animal.into(),
                    b1_var_animal_type.into(),
                    vec![(type_cat.clone(), type_cat.clone())],
                )],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            },
            TypeInferenceGraph {
                conjunction: b2,
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_dog.clone()])),
                    (b2_var_animal_type.into(), BTreeSet::from([type_dog.clone()])),
                ]),
                edges: vec![expected_edge(
                    b2_isa,
                    var_animal.into(),
                    b2_var_animal_type.into(),
                    vec![(type_dog.clone(), type_dog.clone())],
                )],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            },
        ];

        let expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_animal.into(), BTreeSet::from([type_cat.clone(), type_dog.clone()])),
                (var_name.into(), BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()])),
                (var_name_type.into(), BTreeSet::from([type_name.clone()])),
            ]),
            edges: vec![
                expected_edge(
                    &conjunction.constraints()[1],
                    var_name.into(),
                    var_name_type.into(),
                    vec![
                        (type_name.clone(), type_name.clone()),
                        (type_catname.clone(), type_name.clone()),
                        (type_dogname.clone(), type_name.clone()),
                    ],
                ),
                expected_edge(
                    &conjunction.constraints()[2],
                    var_animal.into(),
                    var_name.into(),
                    vec![
                        (type_cat.clone(), type_catname.clone()),
                        (type_cat.clone(), type_name.clone()),
                        (type_dog.clone(), type_dogname.clone()),
                        (type_dog.clone(), type_name.clone()),
                    ],
                ),
            ],
            nested_disjunctions: vec![NestedTypeInferenceGraphDisjunction {
                disjunction: expected_nested_graphs,
                shared_variables: BTreeSet::new(),
                shared_vertex_annotations: VertexAnnotations::default(),
            }],
            nested_negations: Vec::new(),
            nested_optionals: Vec::new(),
        };

        assert_eq!(expected_graph, tig);
    }

    #[test]
    fn no_type_constraints() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        // Case 1: $a has $n;
        let snapshot = storage.clone().open_snapshot_write();
        let mut translation_context = TranslationContext::new();
        let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
        let mut conjunction = builder.conjunction_mut();
        let (var_animal, var_name) = ["animal", "name"]
            .into_iter()
            .map(|name| conjunction.get_or_declare_variable(name).unwrap())
            .collect_tuple()
            .unwrap();

        conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

        let block = builder.finish();
        let conjunction = block.conjunction();
        let constraints = conjunction.constraints();
        let tig = infer_types_for_block(
            &snapshot,
            &block,
            &translation_context.variable_registry,
            &type_manager,
            &BTreeMap::new(),
            &IndexedAnnotatedFunctions::empty(),
            None,
        )
        .unwrap();

        let expected_tig = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_animal.into(), BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()])),
                (var_name.into(), BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()])),
            ]),
            edges: vec![expected_edge(
                &constraints[0],
                var_animal.into(),
                var_name.into(),
                vec![
                    (type_animal.clone(), type_name.clone()),
                    (type_cat.clone(), type_catname.clone()),
                    (type_cat.clone(), type_name.clone()),
                    (type_dog.clone(), type_dogname.clone()),
                    (type_dog.clone(), type_name.clone()),
                ],
            )],
            nested_disjunctions: Vec::new(),
            nested_negations: Vec::new(),
            nested_optionals: Vec::new(),
        };

        assert_eq!(expected_tig, tig);
    }

    #[test]
    fn role_players() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, type_dog), _, (type_fears, type_has_fear, type_is_feared)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        // With roles specified
        let snapshot = storage.clone().open_snapshot_write();
        let mut translation_context = TranslationContext::new();
        let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
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
        .map(|name| conjunction.get_or_declare_variable(name).unwrap())
        .collect_tuple()
        .unwrap();

        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_fears, var_fears_type.into()).unwrap();
        conjunction.constraints_mut().add_label(var_fears_type, LABEL_FEARS.scoped_name().as_str()).unwrap();
        conjunction.constraints_mut().add_links(var_fears, var_has_fear, var_role_has_fear).unwrap();
        conjunction.constraints_mut().add_links(var_fears, var_is_feared, var_role_is_feared).unwrap();

        conjunction
            .constraints_mut()
            .add_sub(SubKind::Subtype, var_role_has_fear.into(), var_role_has_fear_type.into())
            .unwrap();
        conjunction.constraints_mut().add_label(var_role_has_fear_type, "fears:has-fear").unwrap();
        conjunction
            .constraints_mut()
            .add_sub(SubKind::Subtype, var_role_is_feared.into(), var_role_is_feared_type.into())
            .unwrap();
        conjunction.constraints_mut().add_label(var_role_is_feared_type, "fears:is-feared").unwrap();

        let block = builder.finish();

        let conjunction = block.conjunction();

        let tig = infer_types_for_block(
            &snapshot,
            &block,
            &translation_context.variable_registry,
            &type_manager,
            &BTreeMap::new(),
            &IndexedAnnotatedFunctions::empty(),
            None,
        )
        .unwrap();

        let expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_has_fear.into(), BTreeSet::from([type_cat.clone()])),
                (var_is_feared.into(), BTreeSet::from([type_dog.clone()])),
                (var_fears_type.into(), BTreeSet::from([type_fears.clone()])),
                (var_fears.into(), BTreeSet::from([type_fears.clone()])),
                (var_role_has_fear.into(), BTreeSet::from([type_has_fear.clone()])),
                (var_role_is_feared.into(), BTreeSet::from([type_is_feared.clone()])),
                (var_role_has_fear_type.into(), BTreeSet::from([type_has_fear.clone()])),
                (var_role_is_feared_type.into(), BTreeSet::from([type_is_feared.clone()])),
            ]),
            edges: vec![
                // isa
                expected_edge(
                    &conjunction.constraints()[0],
                    var_fears.into(),
                    var_fears_type.into(),
                    vec![(type_fears.clone(), type_fears.clone())],
                ),
                // has-fear edge
                expected_edge(
                    &conjunction.constraints()[2],
                    var_fears.into(),
                    var_role_has_fear.into(),
                    vec![(type_fears.clone(), type_has_fear.clone())],
                ),
                expected_edge(
                    &conjunction.constraints()[2],
                    var_has_fear.into(),
                    var_role_has_fear.into(),
                    vec![(type_cat.clone(), type_has_fear.clone())],
                ),
                // is-feared edge
                expected_edge(
                    &conjunction.constraints()[3],
                    var_fears.into(),
                    var_role_is_feared.into(),
                    vec![(type_fears.clone(), type_is_feared.clone())],
                ),
                expected_edge(
                    &conjunction.constraints()[3],
                    var_is_feared.into(),
                    var_role_is_feared.into(),
                    vec![(type_dog.clone(), type_is_feared.clone())],
                ),
                expected_edge(
                    &conjunction.constraints()[4],
                    var_role_has_fear.into(),
                    var_role_has_fear_type.into(),
                    vec![(type_has_fear.clone(), type_has_fear.clone())],
                ),
                expected_edge(
                    &conjunction.constraints()[6],
                    var_role_is_feared.into(),
                    var_role_is_feared_type.into(),
                    vec![(type_is_feared.clone(), type_is_feared.clone())],
                ),
            ],
            nested_disjunctions: Vec::new(),
            nested_negations: Vec::new(),
            nested_optionals: Vec::new(),
        };

        assert_eq!(expected_graph, tig);
    }

    #[test]
    fn type_constraints() {
        // Some version of `$a isa animal, has name $n;`
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let all_animals = BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]);
        let all_names = BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()]);

        {
            // Case 1: $a isa $at; $at label cat; $n isa! $nt; $at owns $nt;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_owned_type) =
                ["animal", "name", "animal_type", "name_type"]
                    .into_iter()
                    .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                    .collect_tuple()
                    .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Exact, var_name, var_owned_type.into()).unwrap();
            conjunction.constraints_mut().add_owns(var_animal_type.into(), var_owned_type.into()).unwrap();

            let block = builder.finish();
            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name.into(), BTreeSet::from([type_name.clone(), type_catname.clone()])),
                    (var_animal_type.into(), BTreeSet::from([type_cat.clone()])),
                    (var_owned_type.into(), BTreeSet::from([type_name.clone(), type_catname.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_animal_type.into(),
                        vec![(type_cat.clone(), type_cat.clone())],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_name.into(),
                        var_owned_type.into(),
                        vec![(type_catname.clone(), type_catname.clone()), (type_name.clone(), type_name.clone())],
                    ),
                    expected_edge(
                        &constraints[3],
                        var_animal_type.into(),
                        var_owned_type.into(),
                        vec![(type_cat.clone(), type_catname.clone()), (type_cat.clone(), type_name.clone())],
                    ),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };

            assert_eq!(expected_tig, tig);
        }

        {
            // Case 2: $a isa $at; $n isa $nt; $nt type catname; $at owns $nt;
            let snapshot = storage.clone().open_snapshot_write();

            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_owner_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_owner_type.into()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_CATNAME.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_owns(var_owner_type.into(), var_name_type.into()).unwrap();

            let block = builder.finish();

            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name.into(), BTreeSet::from([type_catname.clone()])),
                    (var_owner_type.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name_type.into(), BTreeSet::from([type_catname.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        var_owner_type.into(),
                        vec![(type_cat.clone(), type_cat.clone())],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        var_name_type.into(),
                        vec![(type_catname.clone(), type_catname.clone())],
                    ),
                    expected_edge(
                        &constraints[3],
                        var_owner_type.into(),
                        var_name_type.into(),
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
            // Case 3: $a isa $at; $at type cat; $n isa $nt; $nt type dogname; $at owns $nt;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
            conjunction.constraints_mut().add_label(var_name_type, LABEL_DOGNAME.scoped_name().as_str()).unwrap();
            conjunction.constraints_mut().add_owns(var_animal_type.into(), var_name_type.into()).unwrap();

            let block = builder.finish();
            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::new()),
                    (var_name.into(), BTreeSet::new()),
                    (var_animal_type.into(), BTreeSet::new()),
                    (var_name_type.into(), BTreeSet::new()),
                ]),
                edges: vec![
                    expected_edge(&constraints[0], var_animal.into(), var_animal_type.into(), Vec::new()),
                    expected_edge(&constraints[2], var_name.into(), var_name_type.into(), Vec::new()),
                    expected_edge(&constraints[4], var_animal_type.into(), var_name_type.into(), Vec::new()),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };
            assert_eq!(expected_tig, tig);
        }

        {
            // Case 4: $a isa! $at; $n isa! $nt; $at owns $nt;
            let types_a = all_animals.clone();
            let types_n = all_names.clone();
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let (var_animal, var_name, var_animal_type, var_name_type) = ["animal", "name", "animal_type", "name_type"]
                .into_iter()
                .map(|name| conjunction.get_or_declare_variable(name).unwrap())
                .collect_tuple()
                .unwrap();

            conjunction.constraints_mut().add_isa(IsaKind::Exact, var_animal, var_animal_type.into()).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Exact, var_name, var_name_type.into()).unwrap();
            conjunction.constraints_mut().add_owns(var_animal_type.into(), var_name_type.into()).unwrap();

            let block = builder.finish();
            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
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
                        vec![
                            (type_cat.clone(), type_cat.clone()),
                            (type_dog.clone(), type_dog.clone()),
                            (type_animal.clone(), type_animal.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        var_name_type.into(),
                        vec![
                            (type_catname.clone(), type_catname.clone()),
                            (type_dogname.clone(), type_dogname.clone()),
                            (type_name.clone(), type_name.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_animal_type.into(),
                        var_name_type.into(),
                        vec![
                            (type_cat.clone(), type_name.clone()),
                            (type_cat.clone(), type_catname.clone()),
                            (type_dog.clone(), type_name.clone()),
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
    fn basic_binary_edges_fixed_labels() {
        // Some version of `$a isa animal, has name $n;`
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let all_animals = BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]);
        let all_names = BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()]);

        {
            // Case 1: $a isa cat, has name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] =
                ["animal", "name"].map(|name| conjunction.get_or_declare_variable(name).unwrap());

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_CAT)).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_NAME)).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();
            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name.into(), BTreeSet::from([type_catname.clone(), type_name.clone()])),
                    (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat.clone()])),
                    (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        Vertex::Label(LABEL_CAT),
                        vec![(type_cat.clone(), type_cat.clone())],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        Vertex::Label(LABEL_NAME),
                        vec![(type_catname.clone(), type_name.clone()), (type_name.clone(), type_name.clone())],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_animal.into(),
                        var_name.into(),
                        vec![(type_cat.clone(), type_name.clone()), (type_cat.clone(), type_catname.clone())],
                    ),
                ],
                nested_disjunctions: Vec::new(),
                nested_negations: Vec::new(),
                nested_optionals: Vec::new(),
            };

            assert_eq!(expected_tig.vertices, tig.vertices);
            assert_eq!(expected_tig.edges, tig.edges);
            assert_eq!(expected_tig, tig);
        }

        {
            // Case 2: $a isa animal, has cat-name $n;
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] =
                ["animal", "name"].map(|name| conjunction.get_or_declare_variable(name).unwrap());

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_ANIMAL)).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_CATNAME)).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();

            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                    (var_name.into(), BTreeSet::from([type_catname.clone()])),
                    (Vertex::Label(LABEL_ANIMAL), BTreeSet::from([type_animal.clone()])),
                    (Vertex::Label(LABEL_CATNAME), BTreeSet::from([type_catname.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        Vertex::Label(LABEL_ANIMAL),
                        vec![(type_cat.clone(), type_animal.clone())],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        Vertex::Label(LABEL_CATNAME),
                        vec![(type_catname.clone(), type_catname.clone())],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_animal.into(),
                        var_name.into(),
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
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] =
                ["animal", "name"].map(|name| conjunction.get_or_declare_variable(name).unwrap());

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_CAT)).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_DOGNAME)).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();
            let err = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap_err();
            assert_matches!(err, TypeInferenceError::DetectedUnsatisfiablePattern {});
        }

        {
            // Case 4: $a isa animal, has name $n; // Just to be sure
            let types_a = all_animals.clone();
            let types_n = all_names.clone();
            let snapshot = storage.clone().open_snapshot_write();
            let mut translation_context = TranslationContext::new();
            let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
            let mut conjunction = builder.conjunction_mut();
            let [var_animal, var_name] =
                ["animal", "name"].map(|name| conjunction.get_or_declare_variable(name).unwrap());

            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, Vertex::Label(LABEL_ANIMAL)).unwrap();
            conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, Vertex::Label(LABEL_NAME)).unwrap();
            conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

            let block = builder.finish();
            let constraints = block.conjunction().constraints();
            let tig = infer_types_for_block(
                &snapshot,
                &block,
                &translation_context.variable_registry,
                &type_manager,
                &BTreeMap::new(),
                &IndexedAnnotatedFunctions::empty(),
                None,
            )
            .unwrap();

            let expected_tig = TypeInferenceGraph {
                conjunction: block.conjunction(),
                vertices: VertexAnnotations::from([
                    (var_animal.into(), types_a),
                    (var_name.into(), types_n),
                    (Vertex::Label(LABEL_ANIMAL), BTreeSet::from([type_animal.clone()])),
                    (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name.clone()])),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_animal.into(),
                        Vertex::Label(LABEL_ANIMAL),
                        vec![
                            (type_cat.clone(), type_animal.clone()),
                            (type_dog.clone(), type_animal.clone()),
                            (type_animal.clone(), type_animal.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_name.into(),
                        Vertex::Label(LABEL_NAME),
                        vec![
                            (type_catname.clone(), type_name.clone()),
                            (type_dogname.clone(), type_name.clone()),
                            (type_name.clone(), type_name.clone()),
                        ],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_animal.into(),
                        var_name.into(),
                        vec![
                            (type_cat.clone(), type_catname.clone()),
                            (type_cat.clone(), type_name.clone()),
                            (type_dog.clone(), type_dogname.clone()),
                            (type_dog.clone(), type_name.clone()),
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
}
