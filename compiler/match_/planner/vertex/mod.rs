/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    iter,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pattern::{
    constraint::{Comparison, Has, Links, SubKind},
    Vertex,
};
use itertools::{chain, Itertools};

use crate::match_::inference::type_annotations::TypeAnnotations;

const OPEN_ITERATOR_RELATIVE_COST: f64 = 5.0;
const ADVANCE_ITERATOR_RELATIVE_COST: f64 = 1.0;

const REGEX_EXPECTED_CHECKS_PER_MATCH: f64 = 2.0;
const CONTAINS_EXPECTED_CHECKS_PER_MATCH: f64 = 2.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Canonical,
    Reverse,
}

// FIXME name
#[derive(Debug)]
pub(super) enum PlannerVertex {
    Constant,
    Label(LabelPlanner),

    Input(InputPlanner),

    Type(TypePlanner),
    Thing(ThingPlanner),
    Value(ValuePlanner),

    Isa(IsaPlanner),
    Has(HasPlanner),
    Links(LinksPlanner),
    Expression(()),
    Comparison(ComparisonPlanner),

    Sub(SubPlanner),
    Owns(OwnsPlanner),
    Relates(RelatesPlanner),
    Plays(PlaysPlanner),

    NestedPattern(NestedPatternPlanner),
}

impl PlannerVertex {
    pub(super) fn is_valid(&self, index: usize, ordered: &[usize], adjacency: &HashMap<usize, HashSet<usize>>) -> bool {
        match self {
            Self::Constant | Self::Label(_) => true, // always valid: comes from query

            Self::Input(_) => true, // always valid: comes from the enclosing scope

            Self::Type(_) | Self::Thing(_) | Self::Value(_) => {
                let adjacent = &adjacency[&index];
                ordered.iter().any(|x| adjacent.contains(x))
            } // may be invalid: must be produced

            Self::Expression(_) => todo!("validate expression"), // may be invalid: inputs must be bound
            Self::Comparison(ComparisonPlanner { lhs, rhs }) => {
                if let Input::Variable(lhs) = lhs {
                    if !ordered.contains(lhs) {
                        return false;
                    }
                }
                if let Input::Variable(rhs) = rhs {
                    if !ordered.contains(rhs) {
                        return false;
                    }
                }
                true
            }

            | Self::Isa(_)
            | Self::Has(_)
            | Self::Links(_)
            | Self::Sub(_)
            | Self::Owns(_)
            | Self::Relates(_)
            | Self::Plays(_) => true, // always valid: iterators

            Self::NestedPattern(inner) => inner.is_valid(index, ordered, adjacency),
        }
    }

    fn as_thing(&self) -> Option<&ThingPlanner> {
        match self {
            Self::Thing(v) => Some(v),
            _ => None,
        }
    }

    pub(super) fn unbound_direction(&self) -> Direction {
        match self {
            Self::Constant => todo!(),
            Self::Label(_) => todo!(),
            Self::Input(_) => unreachable!(),
            Self::Type(_) => unreachable!(),
            Self::Thing(_) => unreachable!(),
            Self::Value(_) => unreachable!(),
            Self::Isa(inner) => inner.unbound_direction,
            Self::Has(inner) => inner.unbound_direction,
            Self::Links(inner) => inner.unbound_direction,
            Self::Expression(_) => todo!(),
            Self::Comparison(_) => Direction::Canonical,
            Self::Sub(inner) => inner.unbound_direction,
            Self::Owns(inner) => inner.unbound_direction,
            Self::Relates(inner) => inner.unbound_direction,
            Self::Plays(inner) => inner.unbound_direction,
            Self::NestedPattern(_) => unreachable!(),
        }
    }

    pub(super) fn variables(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            Self::Constant => Box::new(iter::empty()),
            Self::Label(inner) => Box::new(inner.variables()),

            Self::Input(_inner) => Box::new(iter::empty()),
            Self::Type(_) | Self::Thing(_) | Self::Value(_) => Box::new(iter::empty()),

            Self::Isa(inner) => Box::new(inner.variables()),
            Self::Has(inner) => Box::new(inner.variables()),
            Self::Links(inner) => Box::new(inner.variables()),
            Self::Expression(_inner) => todo!(),
            Self::Comparison(inner) => Box::new(inner.variables()),

            Self::Sub(inner) => Box::new(inner.variables()),
            Self::Owns(inner) => Box::new(inner.variables()),
            Self::Relates(inner) => Box::new(inner.variables()),
            Self::Plays(inner) => Box::new(inner.variables()),

            Self::NestedPattern(inner) => Box::new(inner.variables()),
        }
    }

    pub(super) fn add_is(&mut self, other: usize) {
        match self {
            Self::Constant => todo!(),
            Self::Label(_) => todo!(),

            Self::Input(_inner) => todo!(),
            Self::Type(_inner) => todo!(),
            Self::Thing(inner) => inner.add_is(other),
            Self::Value(_inner) => todo!(),

            Self::Isa(_inner) => todo!(),
            Self::Has(_inner) => todo!(),
            Self::Links(_inner) => todo!(),
            Self::Expression(_inner) => todo!(),
            Self::Comparison(_inner) => todo!(),

            Self::Sub(_inner) => todo!(),
            Self::Owns(_inner) => todo!(),
            Self::Relates(_inner) => todo!(),
            Self::Plays(_inner) => todo!(),

            Self::NestedPattern(_inner) => unreachable!(),
        }
    }

    pub(super) fn add_equal(&mut self, other: Input) {
        match self {
            Self::Constant | Self::Input(_) => (),
            Self::Value(_) => todo!(),
            Self::Thing(inner) => inner.add_equal(other),
            _ => todo!(),
        }
    }

    pub(super) fn add_lower_bound(&mut self, other: Input) {
        match self {
            Self::Constant | Self::Input(_) => (),
            Self::Value(_inner) => todo!(),
            Self::Thing(inner) => inner.add_lower_bound(other),
            _ => todo!(),
        }
    }

    pub(super) fn add_upper_bound(&mut self, other: Input) {
        match self {
            Self::Constant | Self::Input(_) => (),
            Self::Value(_inner) => todo!(),
            Self::Thing(inner) => inner.add_upper_bound(other),
            _ => todo!(),
        }
    }

    pub(super) fn is_variable(&self) -> bool {
        matches!(self, Self::Thing(_) | Self::Type(_) | Self::Value(_))
    }

    /// Returns `true` if the planner vertex is [`Value`].
    ///
    /// [`Value`]: PlannerVertex::Value
    #[must_use]
    pub(super) fn is_value(&self) -> bool {
        matches!(self, Self::Value(..))
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ElementCost {
    pub per_input: f64,
    pub per_output: f64,
    pub branching_factor: f64,
}

impl ElementCost {
    fn free_with_branching(branching_factor: f64) -> Self {
        Self { per_input: 0.0, per_output: 0.0, branching_factor }
    }

    pub(super) fn chain(self, other: Self) -> Self {
        Self {
            per_input: self.per_input + other.per_input * self.branching_factor,
            per_output: self.per_output / other.branching_factor + other.per_output,
            branching_factor: self.branching_factor * other.branching_factor,
        }
    }
}

impl Default for ElementCost {
    fn default() -> Self {
        Self { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

pub(super) trait Costed {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> ElementCost;
}

impl Costed for PlannerVertex {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> ElementCost {
        match self {
            Self::Constant => ElementCost::default(),
            Self::Label(_) => ElementCost::default(),
            Self::Input(inner) => inner.cost(inputs, elements),

            Self::Type(inner) => inner.cost(inputs, elements),
            Self::Thing(inner) => inner.cost(inputs, elements),
            Self::Value(inner) => inner.cost(inputs, elements),

            Self::Isa(inner) => inner.cost(inputs, elements),
            Self::Has(inner) => inner.cost(inputs, elements),
            Self::Links(inner) => inner.cost(inputs, elements),
            Self::Expression(_) => todo!("expression cost"),
            Self::Comparison(inner) => inner.cost(inputs, elements),

            Self::Sub(inner) => inner.cost(inputs, elements),
            Self::Owns(inner) => inner.cost(inputs, elements),
            Self::Relates(inner) => inner.cost(inputs, elements),
            Self::Plays(inner) => inner.cost(inputs, elements),

            Self::NestedPattern(inner) => inner.cost(inputs, elements),
        }
    }
}

#[derive(Debug)]
pub(super) struct LabelPlanner {
    var: usize,
}

impl LabelPlanner {
    fn variables(&self) -> impl Iterator<Item = usize> {
        iter::once(self.var)
    }

    pub(crate) fn from_label_constraint(
        label: &ir::pattern::constraint::Label<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
    ) -> LabelPlanner {
        Self { var: variable_index[&label.left().as_variable().unwrap()] }
    }

    pub(crate) fn from_role_name_constraint(
        role_name: &ir::pattern::constraint::RoleName<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
    ) -> LabelPlanner {
        Self { var: variable_index[&role_name.left().as_variable().unwrap()] }
    }

    pub(crate) fn from_kind_constraint(
        kind: &ir::pattern::constraint::Kind<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
    ) -> LabelPlanner {
        Self { var: variable_index[&kind.type_().as_variable().unwrap()] }
    }
}

#[derive(Debug)]
pub(super) struct InputPlanner;

impl InputPlanner {
    pub(crate) fn from_variable(_: Variable, _: &TypeAnnotations) -> Self {
        Self
    }
}

impl Costed for InputPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        ElementCost::default()
    }
}

#[derive(Debug)]
pub(super) struct TypePlanner {
    branching_factor: f64,
}

impl TypePlanner {
    pub(crate) fn from_variable(variable: Variable, type_annotations: &TypeAnnotations) -> Self {
        let num_types = type_annotations.vertex_annotations_of(&Vertex::Variable(variable)).unwrap().len();
        Self { branching_factor: num_types as f64 }
    }
}

impl Costed for TypePlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        ElementCost::free_with_branching(self.branching_factor)
    }
}

#[derive(Debug, Default)]
pub(super) struct ThingPlanner {
    expected_size: f64,

    bound_exact: HashSet<usize>, // IID or exact Type + Value

    bound_value_equal: HashSet<Input>,
    bound_value_below: HashSet<Input>,
    bound_value_above: HashSet<Input>,
}

impl ThingPlanner {
    pub(super) fn from_variable(
        variable: Variable,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let expected_size = type_annotations
            .vertex_annotations_of(&Vertex::Variable(variable))
            .expect("expected thing variable to have been annotated with types")
            .iter()
            .filter_map(|type_| match type_ {
                answer::Type::Entity(type_) => statistics.entity_counts.get(type_),
                answer::Type::Relation(type_) => statistics.relation_counts.get(type_),
                answer::Type::Attribute(type_) => statistics.attribute_counts.get(type_),
                answer::Type::RoleType(type_) => {
                    panic!("Found a Thing variable `{variable}` with a Role Type annotation: {type_}")
                }
            })
            .sum::<u64>() as f64;
        Self { expected_size, ..Default::default() }
    }

    pub(super) fn add_is(&mut self, other: usize) {
        self.bound_exact.insert(other);
    }

    pub(super) fn add_equal(&mut self, other: Input) {
        self.bound_value_equal.insert(other);
    }

    pub(super) fn add_lower_bound(&mut self, other: Input) {
        self.bound_value_below.insert(other);
    }

    pub(super) fn add_upper_bound(&mut self, other: Input) {
        self.bound_value_above.insert(other);
    }
}

impl Costed for ThingPlanner {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> ElementCost {
        let bounds = chain!(&self.bound_value_equal, &self.bound_value_above, &self.bound_value_below).collect_vec();
        for &i in inputs {
            if !bounds.contains(&&Input::Variable(i)) {
                return ElementCost::default();
            }
        }

        if self.bound_exact.iter().any(|bound| inputs.contains(bound)) {
            return ElementCost::default();
        }

        let per_input = OPEN_ITERATOR_RELATIVE_COST;
        let per_output = ADVANCE_ITERATOR_RELATIVE_COST;
        let mut branching_factor = self.expected_size;

        for bound in &self.bound_value_equal {
            if bound == &Input::Fixed {
                branching_factor /= self.expected_size;
            } else if matches!(bound, Input::Variable(var) if inputs.contains(var)) {
                let b = match &elements[bound.as_variable().unwrap()] {
                    PlannerVertex::Constant => 1.0,
                    PlannerVertex::Value(value) => 1.0 / value.expected_size(inputs),
                    PlannerVertex::Thing(thing) => 1.0 / thing.expected_size,
                    _ => unreachable!("equality with an edge"),
                };
                branching_factor = f64::min(branching_factor, b);
            }
        }

        /* TODO
        let mut is_bounded_below = false;
        let mut is_bounded_above = false;

        for &input in inputs {
            if self.bound_exact.contains(&input) {
                if matches!(elements[input], PlannerVertex::Constant) {
                } else {
                    // comes from a previous step, must be in the DB?
                    // TODO verify this assumption
                    per_input = 0.0;
                    per_output = 0.0;
                    branching_factor /= self.expected_size;
                }
            }

            if self.bound_value_equal.contains(&input) {
                let b = match &elements[input] {
                    PlannerVertex::Constant => 1.0,
                    PlannerVertex::Value(value) => 1.0 / value.expected_size(inputs),
                    PlannerVertex::Thing(thing) => 1.0 / thing.expected_size,
                    _ => unreachable!("equality with an edge"),
                };
                branching_factor = f64::min(branching_factor, b);
            }

            if self.bound_value_below.contains(&input) {
                is_bounded_below = true;
            }

            if self.bound_value_above.contains(&input) {
                is_bounded_above = true;
            }
        }

        if is_bounded_below ^ is_bounded_above {
            branching_factor /= 2.0
        } else if is_bounded_below && is_bounded_above {
            branching_factor /= 3.0
        }
        */

        ElementCost { per_input, per_output, branching_factor }
    }
}

#[derive(Debug)]
pub(super) struct ValuePlanner;

impl ValuePlanner {
    pub(crate) fn from_variable(_: Variable) -> Self {
        Self
    }

    fn expected_size(&self, _inputs: &[usize]) -> f64 {
        todo!("value planner expected size")
    }
}

impl Costed for ValuePlanner {
    fn cost(&self, inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        if inputs.is_empty() {
            ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
        } else {
            ElementCost { per_input: f64::INFINITY, per_output: 0.0, branching_factor: f64::INFINITY }
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(super) enum Input {
    Fixed,
    Variable(usize),
}

impl Input {
    pub(super) fn from_vertex(vertex: &Vertex<Variable>, variable_index: &HashMap<Variable, usize>) -> Self {
        match vertex {
            Vertex::Variable(var) => Input::Variable(variable_index[var]),
            Vertex::Label(_) | Vertex::Parameter(_) => Input::Fixed,
        }
    }

    pub(super) fn as_variable(self) -> Option<usize> {
        if let Self::Variable(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(super) struct IsaPlanner {
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
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> ElementCost {
        elements[self.thing].cost(inputs, elements)
    }
}

#[derive(Debug)]
pub(super) struct HasPlanner {
    pub owner: usize,
    pub attribute: usize,
    expected_size: f64,
    expected_unbound_size: f64,
    unbound_direction: Direction,
}

impl HasPlanner {
    pub(super) fn from_constraint(
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
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> ElementCost {
        let is_owner_bound = inputs.contains(&self.owner);
        let is_attribute_bound = inputs.contains(&self.attribute);

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_owner_bound, is_attribute_bound) {
            (true, true) => 0.0,
            (false, false) => ADVANCE_ITERATOR_RELATIVE_COST * self.expected_unbound_size / self.expected_size,
            (true, false) | (false, true) => ADVANCE_ITERATOR_RELATIVE_COST,
        };

        let owner_size = if let Some(owner) = elements[self.owner].as_thing() { owner.expected_size } else { 1.0 };
        let attribute_size =
            if let Some(attribute) = elements[self.attribute].as_thing() { attribute.expected_size } else { 1.0 };

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
pub(super) struct LinksPlanner {
    pub relation: usize,
    pub player: usize,
    pub role: usize,
    expected_size: f64,
    expected_unbound_size: f64,
    unbound_direction: Direction,
}

impl LinksPlanner {
    pub(super) fn from_constraint(
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
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> ElementCost {
        let is_relation_bound = inputs.contains(&self.relation);
        let is_player_bound = inputs.contains(&self.player);

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_relation_bound, is_player_bound) {
            (true, true) => 0.0,
            (false, false) => ADVANCE_ITERATOR_RELATIVE_COST * self.expected_unbound_size / self.expected_size,
            (true, false) | (false, true) => ADVANCE_ITERATOR_RELATIVE_COST,
        };

        let relation = elements[self.relation].as_thing().unwrap();
        let player = elements[self.player].as_thing().unwrap();

        let branching_factor = match (is_relation_bound, is_player_bound) {
            (true, true) => self.expected_size / relation.expected_size / player.expected_size,
            (true, false) => self.expected_size / relation.expected_size,
            (false, true) => self.expected_size / player.expected_size,
            (false, false) => self.expected_size,
        };

        ElementCost { per_input, per_output, branching_factor }
    }
}

#[derive(Debug)]
pub(super) struct ComparisonPlanner {
    pub lhs: Input,
    pub rhs: Input,
}

impl ComparisonPlanner {
    pub(super) fn from_constraint(
        constraint: &Comparison<Variable>,
        variable_index: &HashMap<Variable, usize>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        Self {
            lhs: Input::from_vertex(constraint.lhs(), variable_index),
            rhs: Input::from_vertex(constraint.rhs(), variable_index),
        }
    }

    fn variables(&self) -> impl Iterator<Item = usize> {
        [self.lhs.as_variable(), self.rhs.as_variable()].into_iter().flatten()
    }
}

impl Costed for ComparisonPlanner {
    fn cost(&self, _: &[usize], _: &[PlannerVertex]) -> ElementCost {
        ElementCost::default()
    }
}

#[derive(Debug)]
pub(super) struct SubPlanner {
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
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

#[derive(Debug)]
pub(super) struct OwnsPlanner {
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
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

#[derive(Debug)]
pub(super) struct RelatesPlanner {
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
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

#[derive(Debug)]
pub(super) struct PlaysPlanner {
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
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
    }
}

#[derive(Debug)]
pub(super) struct NestedPatternPlanner {
    variables: Vec<usize>,
    cost: ElementCost,
}

impl NestedPatternPlanner {
    pub(super) fn new(variables: Vec<usize>, cost: ElementCost) -> Self {
        Self { variables, cost }
    }

    fn is_valid(&self, _index: usize, ordered: &[usize], _adjacency: &HashMap<usize, HashSet<usize>>) -> bool {
        self.variables().all(|var| ordered.contains(&var))
    }

    fn variables(&self) -> impl Iterator<Item = usize> + '_ {
        self.variables.iter().copied()
    }
}

impl Costed for NestedPatternPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex]) -> ElementCost {
        // TODO cost can be adjusted for disjunctions
        self.cost
    }
}
