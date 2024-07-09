/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use storage::snapshot::ReadableSnapshot;

use super::pattern_type_inference::infer_types_for_block;
use crate::{
    inference::pattern_type_inference::TypeInferenceGraph, pattern::constraint::Constraint, program::program::Program,
};

/*
Design:
1. Assign a static, deterministic ordering over the functions in the Program.
2. Assign a static, deterministic ordering over the connected variables in each functional block's Pattern.
3. Set the possible types for each variable to all types of its category initially (note: function input and output variables can be restricted to subtypes of labelled types initially!)
4. For each variable in the ordering, go over each constraint and intersect types

Output data structure:
TypeAnnotations per FunctionalBlock

Note: On function call boundaries, can assume the current set of schema types per input and output variable.
      However, we should then recurse into the sub-function IRs and tighten their input/output types based on their type inference.

 */

pub(crate) type VertexAnnotations = BTreeMap<Variable, BTreeSet<Type>>;

pub fn infer_types(program: &Program, snapshot: &impl ReadableSnapshot, type_manager: &TypeManager) -> TypeAnnotations {
    // let mut entry_type_annotations = TypeAnnotations::new(HashMap::new(), HashMap::new());
    // let mut function_type_annotations: HashMap<DefinitionKey<'static>, TypeAnnotations> = HashMap::new();
    // todo!()
    // TODO: Extend to functions when we implement them
    let root_tig = infer_types_for_block(snapshot, program.entry(), type_manager).unwrap();
    TypeAnnotations::build(root_tig)
}

pub struct TypeAnnotations {
    variables: HashMap<Variable, Arc<HashSet<Type>>>,
    constraints: HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
}

impl TypeAnnotations {
    pub fn new(
        variables: HashMap<Variable, Arc<HashSet<Type>>>,
        constraints: HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    ) -> Self {
        TypeAnnotations { variables, constraints }
    }

    pub(crate) fn build(root_type_inference_graph: TypeInferenceGraph<'_>) -> Self {
        let mut vertex_annotations = HashMap::new();
        let mut constraint_annotations = HashMap::new();
        root_type_inference_graph.collect_type_annotations(&mut vertex_annotations, &mut constraint_annotations);
        Self::new(vertex_annotations, constraint_annotations)
    }

    pub fn variable_annotations(&self, variable: Variable) -> Option<Arc<HashSet<Type>>> {
        self.variables.get(&variable).cloned()
    }

    pub fn constraint_annotations(&self, constraint: Constraint<Variable>) -> Option<&ConstraintTypeAnnotations> {
        self.constraints.get(&constraint)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConstraintTypeAnnotations {
    LeftRight(LeftRightAnnotations),
    LeftRightFiltered(LeftRightFilteredAnnotations), // note: function calls, comparators, and value assignments are not stored here, since they do not actually co-constrain Schema types possible.
                                                     //       in other words, they are always right to left or deal only in value types.
}

impl ConstraintTypeAnnotations {
    pub fn get_left_right(&self) -> &LeftRightAnnotations {
        match self {
            ConstraintTypeAnnotations::LeftRight(annotations) => annotations,
            ConstraintTypeAnnotations::LeftRightFiltered(_) => panic!("Unexpected type."),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct LeftRightAnnotations {
    left_to_right: Arc<BTreeMap<Type, Vec<Type>>>,
    right_to_left: Arc<BTreeMap<Type, Vec<Type>>>,
}

impl LeftRightAnnotations {
    pub fn new(left_to_right: BTreeMap<Type, Vec<Type>>, right_to_left: BTreeMap<Type, Vec<Type>>) -> Self {
        Self { left_to_right: Arc::new(left_to_right), right_to_left: Arc::new(right_to_left) }
    }

    pub(crate) fn build(
        left_to_right_set: BTreeMap<Type, BTreeSet<Type>>,
        right_to_left_set: BTreeMap<Type, BTreeSet<Type>>,
    ) -> Self {
        let mut left_to_right = BTreeMap::new();
        for (left, right_set) in left_to_right_set {
            left_to_right.insert(left, right_set.into_iter().collect());
        }
        let mut right_to_left = BTreeMap::new();
        for (right, left_set) in right_to_left_set {
            right_to_left.insert(right, left_set.into_iter().collect());
        }
        Self::new(left_to_right, right_to_left)
    }

    pub fn left_to_right(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.left_to_right.clone()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct LeftRightFilteredAnnotations {
    // pub(crate) left_to_right: BTreeMap<Type, (BTreeSet<Type>, HashSet<Type>)>,
    // pub(crate) right_to_left: BTreeMap<Type, (BTreeSet<Type>, HashSet<Type>)>,
    // TODO: I think we'll need to be able to traverse from the Filter variable to the left and right. example: `match $role sub friendship:friend; $r ($role: $x);`
    // filter_to_left
    // filter_to_right

    // TODO: krishnan: ^ I don't know if I miss something, but this makes more sense to me:
    // Filtered edges are encoded as  (left,right,filter) and (right,left,filter).
    pub(crate) left_to_right: BTreeMap<Type, Vec<Type>>,
    pub(crate) filters_on_right: BTreeMap<Type, HashSet<Type>>, // The key is the type of the right variable

    pub(crate) right_to_left: BTreeMap<Type, Vec<Type>>,
    pub(crate) filters_on_left: BTreeMap<Type, HashSet<Type>>, // The key is the type of the left variable
}

impl LeftRightFilteredAnnotations {
    pub(crate) fn build(
        relation_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_relation: BTreeMap<Type, BTreeSet<Type>>,
        player_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_player: BTreeMap<Type, BTreeSet<Type>>,
    ) -> Self {
        let mut role_to_player = role_to_player;
        let mut role_to_relation = role_to_relation;
        let mut left_to_right = BTreeMap::new();
        let mut right_to_left = BTreeMap::new();
        let mut filters_on_right = BTreeMap::new();
        let mut filters_on_left = BTreeMap::new();
        for (relation, role_set) in relation_to_role {
            for role in &role_set {
                left_to_right.insert(relation.clone(), role_to_player.remove(role).unwrap().into_iter().collect());
            }
            filters_on_left.insert(relation, role_set.into_iter().collect());
        }

        for (player, role_set) in player_to_role {
            for role in &role_set {
                right_to_left.insert(player.clone(), role_to_relation.remove(role).unwrap().into_iter().collect());
            }
            filters_on_right.insert(player, role_set.into_iter().collect());
        }
        Self { left_to_right, filters_on_right, right_to_left, filters_on_left }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet},
        sync::Arc,
    };

    use answer::{variable::Variable, Type};
    use concept::type_::{entity_type::EntityType, relation_type::RelationType, role_type::RoleType};
    use encoding::graph::type_::vertex::{PrefixedTypeVertexEncoding, TypeID};
    use itertools::Itertools;

    use crate::{
        inference::{
            pattern_type_inference::{tests::expected_edge, NestedTypeInferenceGraphDisjunction, TypeInferenceGraph},
            type_inference::{ConstraintTypeAnnotations, LeftRightFilteredAnnotations, TypeAnnotations},
        },
        pattern::constraint::{Constraint, RolePlayer},
        program::block::FunctionalBlock,
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

        let dummy = FunctionalBlock::builder().finish();
        let constraint1 = Constraint::RolePlayer(RolePlayer::new(var_relation, var_player, Some(var_role_type)));
        let constraint2 = Constraint::RolePlayer(RolePlayer::new(var_relation, var_player, Some(var_role_type)));
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
            left_to_right: BTreeMap::from([(type_rel_0.clone(), vec![type_player_0.clone()])]),
            filters_on_right: BTreeMap::from([(type_player_0.clone(), HashSet::from([type_role_0.clone()]))]),
            right_to_left: BTreeMap::from([(type_player_0.clone(), vec![type_rel_0.clone()])]),
            filters_on_left: BTreeMap::from([(type_rel_0.clone(), HashSet::from([type_role_0.clone()]))]),
        };
        let lra2 = LeftRightFilteredAnnotations {
            left_to_right: BTreeMap::from([(type_rel_1.clone(), vec![type_player_1.clone()])]),
            filters_on_right: BTreeMap::from([(type_player_1.clone(), HashSet::from([type_role_1.clone()]))]),
            right_to_left: BTreeMap::from([(type_player_1.clone(), vec![type_rel_1.clone()])]),
            filters_on_left: BTreeMap::from([(type_rel_1.clone(), HashSet::from([type_role_1.clone()]))]),
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
        assert_eq!(expected_annotations.variables, type_annotations.variables);
        assert_eq!(expected_annotations.constraints, type_annotations.constraints);
    }
}
