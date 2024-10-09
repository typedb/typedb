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

    pub fn constraint_annotations_of(&self, constraint: Constraint<Variable>) -> Option<&ConstraintTypeAnnotations> {
        self.constraints.get(&constraint)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintTypeAnnotations {
    LeftRight(LeftRightAnnotations),
    LeftRightFiltered(LeftRightFilteredAnnotations), // note: function calls, comparators, and value assignments are not stored here,
                                                     //       since they do not actually co-constrain Schema types possible.
                                                     //       in other words, they are always right to left or deal only in value types.
}

impl ConstraintTypeAnnotations {
    pub fn as_left_right(&self) -> &LeftRightAnnotations {
        match self {
            ConstraintTypeAnnotations::LeftRight(annotations) => annotations,
            ConstraintTypeAnnotations::LeftRightFiltered(_) => panic!("Unexpected type."),
        }
    }

    pub fn as_left_right_filtered(&self) -> &LeftRightFilteredAnnotations {
        match self {
            ConstraintTypeAnnotations::LeftRightFiltered(annotations) => annotations,
            ConstraintTypeAnnotations::LeftRight(_) => panic!("Unexpected type."),
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
pub struct LeftRightFilteredAnnotations {
    // Filtered edges are encoded as  (left,right,filter) and (right,left,filter).
    pub(crate) left_to_right: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) filters_on_right: Arc<BTreeMap<Type, BTreeSet<Type>>>, // The key is the type of the right variable

    pub(crate) right_to_left: Arc<BTreeMap<Type, Vec<Type>>>,
    pub(crate) filters_on_left: Arc<BTreeMap<Type, BTreeSet<Type>>>, // The key is the type of the left variable
}

impl LeftRightFilteredAnnotations {
    pub(crate) fn build(
        relation_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_relation: BTreeMap<Type, BTreeSet<Type>>,
        player_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_player: BTreeMap<Type, BTreeSet<Type>>,
    ) -> Self {
        let left_to_right = relation_to_role
            .iter()
            .map(|(relation, role_set)| {
                (relation.clone(), role_set.iter().flat_map(|role| role_to_player[role].clone()).collect())
            })
            .collect();
        let filters_on_left =
            relation_to_role.into_iter().map(|(rel, role_set)| (rel, role_set.into_iter().collect())).collect();

        let right_to_left = player_to_role
            .iter()
            .map(|(player, role_set)| {
                (player.clone(), role_set.iter().flat_map(|role| role_to_relation[role].clone()).collect())
            })
            .collect();
        let filters_on_right =
            player_to_role.into_iter().map(|(player, role_set)| (player, role_set.into_iter().collect())).collect();

        Self {
            left_to_right: Arc::new(left_to_right),
            filters_on_right: Arc::new(filters_on_right),
            right_to_left: Arc::new(right_to_left),
            filters_on_left: Arc::new(filters_on_left),
        }
    }

    pub fn left_to_right(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.left_to_right.clone()
    }

    pub fn filters_on_right(&self) -> Arc<BTreeMap<Type, BTreeSet<Type>>> {
        self.filters_on_right.clone()
    }

    pub fn right_to_left(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.right_to_left.clone()
    }

    pub fn filters_on_left(&self) -> Arc<BTreeMap<Type, BTreeSet<Type>>> {
        self.filters_on_left.clone()
    }
}
