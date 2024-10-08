/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pattern::Vertex;
use itertools::{chain, Itertools};

use crate::match_::{
    inference::type_annotations::TypeAnnotations,
    planner::vertex::{
        Costed, ElementCost, Input, PlannerVertex, ADVANCE_ITERATOR_RELATIVE_COST, OPEN_ITERATOR_RELATIVE_COST,
    },
};

#[derive(Debug)]
pub(crate) enum VariableVertex {
    Input(InputPlanner),
    Shared(SharedPlanner),

    Type(TypePlanner),
    Thing(ThingPlanner),
    Value(ValuePlanner),
}

impl VariableVertex {
    pub(super) fn is_valid(&self, index: usize, ordered: &[usize], adjacency: &HashMap<usize, HashSet<usize>>) -> bool {
        match self {
            Self::Input(_) => true, // always valid: comes from the enclosing scope
            Self::Shared(_) => todo!(),

            Self::Type(_) | Self::Thing(_) | Self::Value(_) => {
                let adjacent = &adjacency[&index];
                ordered.iter().any(|x| adjacent.contains(x))
            } // may be invalid: must be produced
        }
    }

    pub(crate) fn expected_size(&self) -> f64 {
        match self {
            Self::Input(_) => 1.0,
            Self::Shared(_) => todo!(),
            Self::Type(inner) => inner.expected_size,
            Self::Thing(inner) => inner.expected_size,
            Self::Value(_) => 1.0,
        }
    }

    pub(crate) fn add_is(&mut self, other: usize) {
        match self {
            Self::Input(_inner) => todo!(),
            Self::Shared(_) => todo!(),
            Self::Type(_inner) => todo!(),
            Self::Thing(inner) => inner.add_is(other),
            Self::Value(_inner) => todo!(),
        }
    }

    pub(crate) fn add_equal(&mut self, other: Input) {
        match self {
            Self::Input(_) => (),
            Self::Shared(_) => todo!(),
            Self::Value(_) => todo!(),
            Self::Thing(inner) => inner.add_equal(other),
            Self::Type(_) => unreachable!(),
        }
    }

    pub(crate) fn add_lower_bound(&mut self, other: Input) {
        match self {
            Self::Input(_) => (),
            Self::Shared(_) => todo!(),
            Self::Value(_inner) => todo!(),
            Self::Thing(inner) => inner.add_lower_bound(other),
            Self::Type(_) => unreachable!(),
        }
    }

    pub(crate) fn add_upper_bound(&mut self, other: Input) {
        match self {
            Self::Input(_) => (),
            Self::Shared(_) => todo!(),
            Self::Value(_inner) => todo!(),
            Self::Thing(inner) => inner.add_upper_bound(other),
            Self::Type(_) => unreachable!(),
        }
    }

    /// Returns `true` if the variable vertex is [`Input`].
    ///
    /// [`Input`]: VariableVertex::Input
    #[must_use]
    pub(crate) fn is_input(&self) -> bool {
        matches!(self, Self::Input(..))
    }

    /// Returns `true` if the variable vertex is [`Value`].
    ///
    /// [`Value`]: VariableVertex::Value
    #[must_use]
    pub(crate) fn is_value(&self) -> bool {
        matches!(self, Self::Value(..))
    }
}

impl Costed for VariableVertex {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
        match self {
            Self::Input(inner) => inner.cost(inputs, elements),
            Self::Shared(inner) => inner.cost(inputs, elements),

            Self::Type(inner) => inner.cost(inputs, elements),
            Self::Thing(inner) => inner.cost(inputs, elements),
            Self::Value(inner) => inner.cost(inputs, elements),
        }
    }
}

#[derive(Debug)]
pub(crate) struct InputPlanner;

impl InputPlanner {
    pub(crate) fn from_variable(_: Variable, _: &TypeAnnotations) -> Self {
        Self
    }
}

impl Costed for InputPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost::default()
    }
}

#[derive(Debug)]
pub(crate) struct SharedPlanner;

impl SharedPlanner {
    pub(crate) fn from_variable(_: Variable, _: &TypeAnnotations) -> Self {
        todo!()
    }
}

impl Costed for SharedPlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost::default()
    }
}

#[derive(Debug)]
pub(crate) struct TypePlanner {
    expected_size: f64,
}

impl TypePlanner {
    pub(crate) fn from_variable(variable: Variable, type_annotations: &TypeAnnotations) -> Self {
        let num_types = type_annotations.vertex_annotations_of(&Vertex::Variable(variable)).unwrap().len();
        Self { expected_size: num_types as f64 }
    }
}

impl Costed for TypePlanner {
    fn cost(&self, _inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        ElementCost::free_with_branching(self.expected_size)
    }
}

#[derive(Default)]
pub(crate) struct ThingPlanner {
    expected_size: f64,

    bound_exact: HashSet<usize>, // IID or exact Type + Value

    bound_value_equal: HashSet<Input>,
    bound_value_below: HashSet<Input>,
    bound_value_above: HashSet<Input>,
}

impl fmt::Debug for ThingPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThingPlanner").finish()
    }
}

impl ThingPlanner {
    pub(crate) fn from_variable(
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

    pub(crate) fn add_is(&mut self, other: usize) {
        self.bound_exact.insert(other);
    }

    pub(crate) fn add_equal(&mut self, other: Input) {
        self.bound_value_equal.insert(other);
    }

    pub(crate) fn add_lower_bound(&mut self, other: Input) {
        self.bound_value_below.insert(other);
    }

    pub(crate) fn add_upper_bound(&mut self, other: Input) {
        self.bound_value_above.insert(other);
    }
}

impl Costed for ThingPlanner {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex<'_>]) -> ElementCost {
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
                    PlannerVertex::Fixed(_) => 1.0,
                    PlannerVertex::Variable(VariableVertex::Value(value)) => 1.0 / value.expected_size(inputs),
                    PlannerVertex::Variable(VariableVertex::Thing(thing)) => 1.0 / thing.expected_size,
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
pub(crate) struct ValuePlanner;

impl ValuePlanner {
    pub(crate) fn from_variable(_: Variable) -> Self {
        Self
    }

    fn expected_size(&self, _inputs: &[usize]) -> f64 {
        todo!("value planner expected size")
    }
}

impl Costed for ValuePlanner {
    fn cost(&self, inputs: &[usize], _elements: &[PlannerVertex<'_>]) -> ElementCost {
        if inputs.is_empty() {
            ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
        } else {
            ElementCost { per_input: f64::INFINITY, per_output: 0.0, branching_factor: f64::INFINITY }
        }
    }
}
