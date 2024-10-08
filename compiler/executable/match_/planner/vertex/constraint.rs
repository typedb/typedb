/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pattern::constraint::{Has, Links, SubKind};
use itertools::Itertools;

use crate::match_::{
    inference::type_annotations::TypeAnnotations,
    planner::vertex::{
        Costed, Direction, ElementCost, Input, PlannerVertex, ADVANCE_ITERATOR_RELATIVE_COST,
        OPEN_ITERATOR_RELATIVE_COST,
    },
};

#[derive(Debug)]
pub(crate) enum ConstraintVertex {
    Isa(IsaPlanner),
    Has(HasPlanner),
    Links(LinksPlanner),

    Sub(SubPlanner),
    Owns(OwnsPlanner),
    Relates(RelatesPlanner),
    Plays(PlaysPlanner),
}

impl ConstraintVertex {
    pub(crate) fn is_valid(&self, _: usize, _: &[usize], _: &HashMap<usize, HashSet<usize>>) -> bool {
        true // always valid: iterators
    }

    pub(crate) fn unbound_direction(&self) -> Direction {
        match self {
            Self::Isa(inner) => inner.unbound_direction,
            Self::Has(inner) => inner.unbound_direction,
            Self::Links(inner) => inner.unbound_direction,
            Self::Sub(inner) => inner.unbound_direction,
            Self::Owns(inner) => inner.unbound_direction,
            Self::Relates(inner) => inner.unbound_direction,
            Self::Plays(inner) => inner.unbound_direction,
        }
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            Self::Isa(inner) => Box::new(inner.variables()),
            Self::Has(inner) => Box::new(inner.variables()),
            Self::Links(inner) => Box::new(inner.variables()),

            Self::Sub(inner) => Box::new(inner.variables()),
            Self::Owns(inner) => Box::new(inner.variables()),
            Self::Relates(inner) => Box::new(inner.variables()),
            Self::Plays(inner) => Box::new(inner.variables()),
        }
    }
}

impl Costed for ConstraintVertex {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
        match self {
            Self::Isa(inner) => inner.cost(inputs, elements),
            Self::Has(inner) => inner.cost(inputs, elements),
            Self::Links(inner) => inner.cost(inputs, elements),

            Self::Sub(inner) => inner.cost(inputs, elements),
            Self::Owns(inner) => inner.cost(inputs, elements),
            Self::Relates(inner) => inner.cost(inputs, elements),
            Self::Plays(inner) => inner.cost(inputs, elements),
        }
    }
}

#[derive(Debug)]
pub(crate) struct IsaPlanner {
    thing: usize,
    type_: Input,
    unbound_direction: Direction,
}

impl IsaPlanner {
    pub(crate) fn from_constraint(
        isa: &ir::pattern::constraint::Isa<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        let thing = variable_index[&isa.thing().as_variable().unwrap()];
        let type_ = Input::from_vertex(isa.type_(), variable_index);
        Self { thing, type_, unbound_direction: Direction::Reverse }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [Some(self.thing), self.type_.as_variable()].into_iter().flatten()
    }
}

impl Costed for IsaPlanner {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
        elements[self.thing].cost(inputs, elements)
    }
}

#[derive(Debug)]
pub(crate) struct HasPlanner {
    pub owner: usize,
    pub attribute: usize,
    expected_size: f64,
    expected_unbound_size: f64,
    unbound_direction: Direction,
}

impl HasPlanner {
    pub(crate) fn from_constraint(
        constraint: &Has<Variable>,
        variable_index: &HashMap<Variable, usize>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let owner = constraint.owner();
        let attribute = constraint.attribute();

        let owner_types = &**type_annotations.vertex_annotations_of(owner).unwrap();
        let attribute_types = &**type_annotations.vertex_annotations_of(attribute).unwrap();

        let expected_size = itertools::iproduct!(owner_types, attribute_types)
            .filter_map(|(owner, attribute)| {
                statistics.has_attribute_counts.get(&owner.as_object_type())?.get(&attribute.as_attribute_type())
            })
            .sum::<u64>() as f64;

        let unbound_forward_size = owner_types
            .iter()
            .filter_map(|owner| statistics.has_attribute_counts.get(&owner.as_object_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        let unbound_backward_size = attribute_types
            .iter()
            .filter_map(|attribute| statistics.attribute_owner_counts.get(&attribute.as_attribute_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        let expected_unbound_size = f64::min(unbound_forward_size, unbound_backward_size);
        let unbound_direction =
            if unbound_forward_size <= unbound_backward_size { Direction::Canonical } else { Direction::Reverse };

        Self {
            owner: variable_index[&owner.as_variable().unwrap()],
            attribute: variable_index[&attribute.as_variable().unwrap()],
            expected_size,
            expected_unbound_size,
            unbound_direction,
        }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.owner, self.attribute].into_iter()
    }
}

impl Costed for HasPlanner {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
        let is_owner_bound = inputs.contains(&self.owner);
        let is_attribute_bound = inputs.contains(&self.attribute);

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_owner_bound, is_attribute_bound) {
            (true, true) => 0.0,
            (false, false) => ADVANCE_ITERATOR_RELATIVE_COST * self.expected_unbound_size / self.expected_size,
            (true, false) | (false, true) => ADVANCE_ITERATOR_RELATIVE_COST,
        };

        let owner_size = elements[self.owner].as_variable().unwrap().expected_size();
        let attribute_size = elements[self.attribute].as_variable().unwrap().expected_size();

        let branching_factor = match (is_owner_bound, is_attribute_bound) {
            (true, true) => self.expected_size / owner_size / attribute_size,
            (true, false) => self.expected_size / owner_size,
            (false, true) => self.expected_size / attribute_size,
            (false, false) => self.expected_size,
        };

        ElementCost { per_input, per_output, branching_factor }
    }
}

#[derive(Debug)]
pub(crate) struct LinksPlanner {
    pub relation: usize,
    pub player: usize,
    pub role: usize,
    expected_size: f64,
    expected_unbound_size: f64,
    unbound_direction: Direction,
}

impl LinksPlanner {
    pub(crate) fn from_constraint(
        links: &Links<Variable>,
        variable_index: &HashMap<Variable, usize>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let relation = links.relation();
        let player = links.player();
        let role = links.role_type();

        let relation_types = &**type_annotations.vertex_annotations_of(relation).unwrap();
        let player_types = &**type_annotations.vertex_annotations_of(player).unwrap();

        let constraint_types =
            type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_left_right_filtered();

        let expected_size = constraint_types
            .filters_on_left()
            .iter()
            .flat_map(|(relation, roles)| {
                roles.iter().cartesian_product(player_types).flat_map(|(role, player)| {
                    statistics
                        .relation_role_player_counts
                        .get(&relation.as_relation_type())?
                        .get(&role.as_role_type())?
                        .get(&player.as_object_type())
                })
            })
            .sum::<u64>() as f64;

        let unbound_forward_size = relation_types
            .iter()
            .filter_map(|relation| {
                Some(statistics.relation_role_player_counts.get(&relation.as_relation_type())?.values().flat_map(
                    |player_to_count| {
                        player_types.iter().filter_map(|player| player_to_count.get(&player.as_object_type()))
                    },
                ))
            })
            .flatten()
            .sum::<u64>() as f64;

        let unbound_backward_size = player_types
            .iter()
            .filter_map(|player| {
                Some(statistics.player_role_relation_counts.get(&player.as_object_type())?.values().flat_map(
                    |relation_to_count| {
                        relation_types.iter().filter_map(|relation| relation_to_count.get(&relation.as_relation_type()))
                    },
                ))
            })
            .flatten()
            .sum::<u64>() as f64;

        let expected_unbound_size = f64::min(unbound_forward_size, unbound_backward_size);
        let unbound_direction =
            if unbound_forward_size <= unbound_backward_size { Direction::Canonical } else { Direction::Reverse };

        let relation = relation.as_variable().unwrap();
        let player = player.as_variable().unwrap();
        let role = role.as_variable().unwrap();

        Self {
            relation: variable_index[&relation],
            player: variable_index[&player],
            role: variable_index[&role],
            expected_size,
            expected_unbound_size,
            unbound_direction,
        }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.relation, self.player, self.role].into_iter()
    }
}

impl Costed for LinksPlanner {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
        let is_relation_bound = inputs.contains(&self.relation);
        let is_player_bound = inputs.contains(&self.player);

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_relation_bound, is_player_bound) {
            (true, true) => 0.0,
            (false, false) => ADVANCE_ITERATOR_RELATIVE_COST * self.expected_unbound_size / self.expected_size,
            (true, false) | (false, true) => ADVANCE_ITERATOR_RELATIVE_COST,
        };

        let relation_size = elements[self.relation].as_variable().unwrap().expected_size();
        let player_size = elements[self.player].as_variable().unwrap().expected_size();

        let branching_factor = match (is_relation_bound, is_player_bound) {
            (true, true) => self.expected_size / relation_size / player_size,
            (true, false) => self.expected_size / relation_size,
            (false, true) => self.expected_size / player_size,
            (false, false) => self.expected_size,
        };

        ElementCost { per_input, per_output, branching_factor }
    }
}

#[derive(Debug)]
pub(crate) struct SubPlanner {
    type_: Input,
    supertype: Input,
    kind: SubKind,
    unbound_direction: Direction,
}

impl SubPlanner {
    pub(crate) fn from_constraint(
        sub: &ir::pattern::constraint::Sub<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
    ) -> Self {
        Self {
            type_: Input::from_vertex(sub.subtype(), variable_index),
            supertype: Input::from_vertex(sub.supertype(), variable_index),
            kind: sub.sub_kind(),
            unbound_direction: Direction::Reverse,
        }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.type_.as_variable(), self.supertype.as_variable()].into_iter().flatten()
    }
}

impl Costed for SubPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

#[derive(Debug)]
pub(crate) struct OwnsPlanner {
    owner: Input,
    attribute: Input,
    unbound_direction: Direction,
}

impl OwnsPlanner {
    pub(crate) fn from_constraint(
        owns: &ir::pattern::constraint::Owns<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> OwnsPlanner {
        let owner = Input::from_vertex(owns.owner(), variable_index);
        let attribute = Input::from_vertex(owns.attribute(), variable_index);
        Self { owner, attribute, unbound_direction: Direction::Canonical }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.owner.as_variable(), self.attribute.as_variable()].into_iter().flatten()
    }
}

impl Costed for OwnsPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

#[derive(Debug)]
pub(crate) struct RelatesPlanner {
    relation: Input,
    role_type: Input,
    unbound_direction: Direction,
}

impl RelatesPlanner {
    pub(crate) fn from_constraint(
        relates: &ir::pattern::constraint::Relates<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> RelatesPlanner {
        let relation = Input::from_vertex(relates.relation(), variable_index);
        let role_type = Input::from_vertex(relates.role_type(), variable_index);
        Self { relation, role_type, unbound_direction: Direction::Canonical }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.relation.as_variable(), self.role_type.as_variable()].into_iter().flatten()
    }
}

impl Costed for RelatesPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

#[derive(Debug)]
pub(crate) struct PlaysPlanner {
    player: Input,
    role_type: Input,
    unbound_direction: Direction,
}

impl PlaysPlanner {
    pub(crate) fn from_constraint(
        plays: &ir::pattern::constraint::Plays<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> PlaysPlanner {
        let player = Input::from_vertex(plays.player(), variable_index);
        let role_type = Input::from_vertex(plays.role_type(), variable_index);
        Self { player, role_type, unbound_direction: Direction::Canonical }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.player.as_variable(), self.role_type.as_variable()].into_iter().flatten()
    }
}

impl Costed for PlaysPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}
