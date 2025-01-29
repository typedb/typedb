/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use ir::pattern::{constraint::Constraint, Vertex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeAnnotations {
    vertex: BTreeMap<Vertex<Variable>, Arc<BTreeSet<Type>>>,
    constraints: HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
}

impl TypeAnnotations {
    pub fn new(
        variables: BTreeMap<Vertex<Variable>, Arc<BTreeSet<Type>>>,
        constraints: HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    ) -> Self {
        TypeAnnotations { vertex: variables, constraints }
    }

    pub fn vertex_annotations(&self) -> &BTreeMap<Vertex<Variable>, Arc<BTreeSet<Type>>> {
        &self.vertex
    }

    pub fn vertex_annotations_of(&self, vertex: &Vertex<Variable>) -> Option<&Arc<BTreeSet<Type>>> {
        self.vertex.get(vertex)
    }

    pub fn constraint_annotations(&self) -> &HashMap<Constraint<Variable>, ConstraintTypeAnnotations> {
        &self.constraints
    }

    pub fn constraint_annotations_mut(&mut self) -> &mut HashMap<Constraint<Variable>, ConstraintTypeAnnotations> {
        &mut self.constraints
    }

    // TOOD: Just accept a reference.
    pub fn constraint_annotations_of(&self, constraint: Constraint<Variable>) -> Option<&ConstraintTypeAnnotations> {
        self.constraints.get(&constraint)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintTypeAnnotations {
    LeftRight(LeftRightAnnotations),
    Links(LinksAnnotations),
    IndexedRelation(IndexedRelationAnnotations),
}

impl ConstraintTypeAnnotations {
    pub fn as_left_right(&self) -> &LeftRightAnnotations {
        match self {
            ConstraintTypeAnnotations::LeftRight(annotations) => annotations,
            ConstraintTypeAnnotations::Links(_) | ConstraintTypeAnnotations::IndexedRelation(_) => {
                panic!("Unexpected type.")
            }
        }
    }

    pub fn as_links(&self) -> &LinksAnnotations {
        match self {
            ConstraintTypeAnnotations::Links(annotations) => annotations,
            ConstraintTypeAnnotations::LeftRight(_) | ConstraintTypeAnnotations::IndexedRelation(_) => {
                panic!("Unexpected type.")
            }
        }
    }

    pub fn as_indexed_relation(&self) -> &IndexedRelationAnnotations {
        match self {
            ConstraintTypeAnnotations::IndexedRelation(annotations) => annotations,
            ConstraintTypeAnnotations::LeftRight(_) | ConstraintTypeAnnotations::Links(_) => panic!("Unexpected type"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn right_to_left(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.right_to_left.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinksAnnotations {
    pub(crate) relation_to_player: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) relation_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,

    pub(crate) player_to_relation: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) player_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
}

impl LinksAnnotations {
    pub(crate) fn build(
        relation_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_relation: BTreeMap<Type, BTreeSet<Type>>,
        player_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_player: BTreeMap<Type, BTreeSet<Type>>,
    ) -> Self {
        let relation_to_player = relation_to_role
            .iter()
            .map(|(relation, role_set)| {
                (*relation, role_set.iter().flat_map(|role| role_to_player[role].clone()).collect())
            })
            .collect();
        let relation_to_role_vec =
            relation_to_role.into_iter().map(|(rel, role_set)| (rel, role_set.into_iter().collect())).collect();

        let player_to_relation = player_to_role
            .iter()
            .map(|(player, role_set)| {
                (*player, role_set.iter().flat_map(|role| role_to_relation[role].clone()).collect())
            })
            .collect();
        let player_to_role_vec =
            player_to_role.into_iter().map(|(player, role_set)| (player, role_set.into_iter().collect())).collect();

        Self {
            relation_to_player: Arc::new(relation_to_player),
            relation_to_role: Arc::new(relation_to_role_vec),
            player_to_relation: Arc::new(player_to_relation),
            player_to_role: Arc::new(player_to_role_vec),
        }
    }

    pub fn relation_to_player(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.relation_to_player.clone()
    }

    pub fn player_to_role(&self) -> Arc<BTreeMap<Type, BTreeSet<Type>>> {
        self.player_to_role.clone()
    }

    pub fn player_to_relation(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.player_to_relation.clone()
    }

    pub fn relation_to_role(&self) -> Arc<BTreeMap<Type, BTreeSet<Type>>> {
        self.relation_to_role.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexedRelationAnnotations {
    // player 1 to relation to player 2
    pub(crate) player_1_to_relation: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) relation_to_player_2: Arc<BTreeMap<Type, Vec<Type>>>,

    // player 2 to relation to player 1
    pub(crate) player_2_to_relation: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) relation_to_player_1: Arc<BTreeMap<Type, Vec<Type>>>,

    pub(crate) player_1_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    pub(crate) player_2_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    pub(crate) relation_to_player_1_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    pub(crate) relation_to_player_2_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
}

impl IndexedRelationAnnotations {
    pub(crate) fn new(
        player_1_to_relation: Arc<BTreeMap<Type, Vec<Type>>>,
        relation_to_player_2: Arc<BTreeMap<Type, Vec<Type>>>,
        player_2_to_relation: Arc<BTreeMap<Type, Vec<Type>>>,
        relation_to_player_1: Arc<BTreeMap<Type, Vec<Type>>>,
        player_1_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
        player_2_to_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
        relation_to_player_1_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
        relation_to_player_2_role: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    ) -> Self {
        Self {
            player_1_to_relation,
            relation_to_player_2,
            player_2_to_relation,
            relation_to_player_1,
            player_1_to_role,
            player_2_to_role,
            relation_to_player_1_role,
            relation_to_player_2_role,
        }
    }
}
