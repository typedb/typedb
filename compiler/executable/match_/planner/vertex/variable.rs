/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, fmt};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pattern::Vertex;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::match_::planner::{
        plan::{Graph, PatternVertexId, VariableVertexId, VertexId},
        vertex::{Costed, ElementCost, Input},
    },
};

#[derive(Clone, Debug)]
pub(crate) enum VariableVertex {
    Input(InputPlanner),

    Type(TypePlanner),
    Thing(ThingPlanner),
    Value(ValuePlanner),
}

impl VariableVertex {
    const RESTRICTION_NONE: f64 = 1.0;

    pub(super) fn is_valid(&self, index: VertexId, ordered: &[VertexId], graph: &Graph<'_>) -> bool {
        let VertexId::Variable(index) = index else { unreachable!("variable with incompatible index: {index:?}") };
        match self {
            Self::Input(_) => true, // always valid: comes from the enclosing scope

            Self::Type(inner) => inner.is_valid(index, ordered, graph),
            Self::Thing(inner) => inner.is_valid(index, ordered, graph),
            Self::Value(inner) => inner.is_valid(index, ordered, graph),
        }
    }

    pub(crate) fn expected_output_size(&self, inputs: &[VertexId]) -> f64 {
        let unrestricted_size = match self {
            Self::Input(_) => 1.0,
            Self::Type(inner) => inner.unrestricted_expected_size,
            Self::Thing(inner) => inner.unrestricted_expected_size,
            Self::Value(_) => 1.0,
        };
        unrestricted_size * self.selectivity(inputs)
    }

    pub(crate) fn selectivity(&self, inputs: &[VertexId]) -> f64 {
        // the fraction of possible actual outputs (based on type information) when restricted (for example, by comparators)
        match self {
            VariableVertex::Input(_) => Self::RESTRICTION_NONE,
            VariableVertex::Type(inner) => inner.selectivity(inputs),
            VariableVertex::Thing(inner) => inner.selectivity(inputs),
            VariableVertex::Value(inner) => inner.selectivity(inputs),
        }
    }

    pub(crate) fn set_binding(&mut self, binding_pattern: PatternVertexId) {
        match self {
            Self::Input(_) => unreachable!("attempting to assign to input variable"),

            Self::Type(inner) => inner.set_binding(binding_pattern),
            Self::Thing(inner) => inner.set_binding(binding_pattern),
            Self::Value(inner) => inner.set_binding(binding_pattern),
        }
    }

    pub(crate) fn add_is(&mut self, other: VariableVertexId) {
        match self {
            Self::Input(_inner) => todo!(),
            Self::Type(_inner) => todo!(),
            Self::Thing(inner) => inner.add_is(other),
            Self::Value(_inner) => unreachable!(),
        }
    }

    pub(crate) fn add_equal(&mut self, other: Input) {
        match self {
            Self::Input(_) => (),
            Self::Type(_) => unreachable!(),
            Self::Thing(inner) => inner.add_equal(other),
            Self::Value(inner) => inner.add_equal(other),
        }
    }

    pub(crate) fn add_lower_bound(&mut self, other: Input) {
        match self {
            Self::Input(_) => (),
            Self::Type(_) => unreachable!(),
            Self::Thing(inner) => inner.add_lower_bound(other),
            Self::Value(inner) => inner.add_lower_bound(other),
        }
    }

    pub(crate) fn add_upper_bound(&mut self, other: Input) {
        match self {
            Self::Input(_) => (),
            Self::Type(_) => unreachable!(),
            Self::Thing(inner) => inner.add_upper_bound(other),
            Self::Value(inner) => inner.add_upper_bound(other),
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

    pub(crate) fn variable(&self) -> Variable {
        match self {
            VariableVertex::Input(var) => var.variable,
            VariableVertex::Type(var) => var.variable,
            VariableVertex::Thing(var) => var.variable,
            VariableVertex::Value(var) => var.variable,
        }
    }
}

impl Costed for VariableVertex {
    fn cost(&self, inputs: &[VertexId], intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        match self {
            Self::Input(inner) => inner.cost(inputs, intersection, graph),
            Self::Type(inner) => inner.cost(inputs, intersection, graph),
            Self::Thing(inner) => inner.cost(inputs, intersection, graph),
            Self::Value(inner) => inner.cost(inputs, intersection, graph),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InputPlanner {
    variable: Variable,
}

impl fmt::Debug for InputPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InputPlanner").field("variable", &self.variable).finish()
    }
}

impl InputPlanner {
    pub(crate) fn from_variable(variable: Variable) -> Self {
        Self { variable }
    }
}

impl Costed for InputPlanner {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::FREE_BRANCH_1
    }
}

#[derive(Clone)]
pub(crate) struct TypePlanner {
    variable: Variable,
    binding: Option<PatternVertexId>,
    unrestricted_expected_size: f64,
}

impl fmt::Debug for TypePlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypePlanner").field("variable", &self.variable).field("binding", &self.binding).finish()
    }
}

impl TypePlanner {
    pub(crate) fn from_variable(variable: Variable, type_annotations: &TypeAnnotations) -> Self {
        let num_types = type_annotations.vertex_annotations_of(&Vertex::Variable(variable)).unwrap().len();
        Self { variable, binding: None, unrestricted_expected_size: num_types as f64 }
    }

    pub(crate) fn set_binding(&mut self, binding_pattern: PatternVertexId) {
        self.binding = Some(binding_pattern);
    }

    fn is_valid(&self, index: VariableVertexId, ordered: &[VertexId], graph: &Graph<'_>) -> bool {
        if let Some(binding) = self.binding {
            ordered.contains(&VertexId::Pattern(binding))
        } else {
            let adjacent = graph.variable_to_pattern().get(&index).unwrap();
            ordered.iter().filter_map(VertexId::as_pattern_id).any(|id| adjacent.contains(&id))
        }
    }

    fn selectivity(&self, _inputs: &[VertexId]) -> f64 {
        // TODO: if we incorporate, say, annotations, we could add some selectivity here
        VariableVertex::RESTRICTION_NONE
    }
}

impl Costed for TypePlanner {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::free_with_branching(self.unrestricted_expected_size)
    }
}

#[derive(Clone)]
pub(crate) struct ThingPlanner {
    variable: Variable,
    binding: Option<PatternVertexId>,
    unrestricted_expected_size: f64,
    unrestricted_expected_attribute_types: usize,

    restriction_exact: HashSet<VariableVertexId>, // IID or exact Type + Value

    restriction_equal: HashSet<Input>,
    restriction_from_below: HashSet<Input>,
    restriction_from_above: HashSet<Input>,
}

impl fmt::Debug for ThingPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThingPlanner").field("variable", &self.variable).field("binding", &self.binding).finish()
    }
}

impl ThingPlanner {
    const RESTRICTION_BELOW_SELECTIVITY: f64 = 0.5;
    const RESTRICTION_ABOVE_SELECTIVITY: f64 = 0.5;

    pub(crate) fn from_variable(
        variable: Variable,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let mut unrestricted_expected_size: f64 = 0.0;
        let mut unrestricted_expected_attribute_types: usize = 0;
        type_annotations
            .vertex_annotations_of(&Vertex::Variable(variable))
            .expect("expected thing variable to have been annotated with types")
            .iter()
            .for_each(|type_| match type_ {
                answer::Type::Entity(type_) => {
                    statistics.entity_counts.get(type_).map(|count| {
                        unrestricted_expected_size += *count as f64;
                    });
                }
                answer::Type::Relation(type_) => {
                    statistics.relation_counts.get(type_).map(|count| {
                        unrestricted_expected_size += *count as f64;
                    });
                }
                answer::Type::Attribute(type_) => {
                    statistics.attribute_counts.get(type_).map(|count| {
                        unrestricted_expected_size += *count as f64;
                        unrestricted_expected_attribute_types += 1;
                    });
                }
                answer::Type::RoleType(type_) => {
                    panic!("Found a Thing variable `{variable}` with a Role Type annotation: {type_}")
                }
            });
        Self {
            variable,
            binding: None,
            unrestricted_expected_size,
            unrestricted_expected_attribute_types,
            restriction_exact: HashSet::new(),
            restriction_equal: HashSet::new(),
            restriction_from_below: HashSet::new(),
            restriction_from_above: HashSet::new(),
        }
    }

    pub(crate) fn add_is(&mut self, other: VariableVertexId) {
        self.restriction_exact.insert(other);
    }

    pub(crate) fn add_equal(&mut self, other: Input) {
        self.restriction_equal.insert(other);
    }

    pub(crate) fn add_lower_bound(&mut self, other: Input) {
        self.restriction_from_below.insert(other);
    }

    pub(crate) fn add_upper_bound(&mut self, other: Input) {
        self.restriction_from_above.insert(other);
    }

    fn set_binding(&mut self, binding_pattern: PatternVertexId) {
        self.binding = Some(binding_pattern);
    }

    fn is_valid(&self, index: VariableVertexId, ordered: &[VertexId], graph: &Graph<'_>) -> bool {
        if let Some(binding) = self.binding {
            ordered.contains(&VertexId::Pattern(binding))
        } else {
            let adjacent = graph.variable_to_pattern().get(&index).unwrap();
            ordered.iter().filter_map(VertexId::as_pattern_id).any(|id| adjacent.contains(&id))
        }
    }

    fn selectivity(&self, inputs: &[VertexId]) -> f64 {
        if !self.restriction_exact.is_empty() {
            // exactly 1 of the full set is selected
            return 1.0 / self.unrestricted_expected_size;
        } else {
            // all are selected
            let mut selected = self.unrestricted_expected_size;
            if self.restriction_equal.iter().any(|restriction| is_input_available(restriction, inputs)) {
                // equality by value leads to one possible per attribute type
                selected = self.unrestricted_expected_attribute_types as f64;
            }
            if self.restriction_from_below.iter().any(|restriction| is_input_available(restriction, inputs)) {
                // some fraction of the selected will pass the strictest below filter
                selected *= Self::RESTRICTION_BELOW_SELECTIVITY;
            }
            if self.restriction_from_above.iter().any(|restriction| is_input_available(restriction, inputs)) {
                // some fraction of the selected will pass the strictest above filter
                selected *= Self::RESTRICTION_ABOVE_SELECTIVITY;
            }
            // normalise again by all possible (with no restrictions, we get selectivity of 1.0)
            selected / self.unrestricted_expected_size
        }
    }
}

fn branching_for_intersections(intersection_count: usize) -> f64 {
    // Note:
    //   this is a linearly improving cost function.
    //   In theory, it's min(input sizes), and in practice it's much better than that!
    //   The n-th root or some factor similarly might also work quite well
    1.0 / (intersection_count as f64)
}

impl Costed for ThingPlanner {
    fn cost(&self, inputs: &[VertexId], intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        match intersection {
            None => ElementCost::FREE_BRANCH_1,
            Some(variable_id) => {
                let mut intersection_count = 0;
                for input in inputs {
                    let input_element = &graph.elements()[input];
                    if input_element.variables().any(|var| var == variable_id) {
                        intersection_count += 1;
                    }
                }
                ElementCost {
                    per_input: 0.0,
                    per_output: 0.0,
                    branching_factor: branching_for_intersections(intersection_count),
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct ValuePlanner {
    variable: Variable,
    binding: Option<PatternVertexId>,

    restriction_value_equal: HashSet<Input>,
    restriction_value_below: HashSet<Input>,
    restriction_value_above: HashSet<Input>,
}

impl fmt::Debug for ValuePlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValuePlanner").field("variable", &self.variable).field("binding", &self.binding).finish()
    }
}

impl ValuePlanner {
    const RESTRICTION_EQUAL_SELECTIVITY: f64 = 0.1;
    const RESTRICTION_BELOW_SELECTIVITY: f64 = 0.5;
    const RESTRICTION_ABOVE_SELECTIVITY: f64 = 0.5;

    pub(crate) fn from_variable(variable: Variable) -> Self {
        Self {
            variable,
            binding: None,
            restriction_value_equal: HashSet::new(),
            restriction_value_below: HashSet::new(),
            restriction_value_above: HashSet::new(),
        }
    }

    fn set_binding(&mut self, binding_pattern: PatternVertexId) {
        self.binding = Some(binding_pattern);
    }

    fn is_valid(&self, index: VariableVertexId, ordered: &[VertexId], graph: &Graph<'_>) -> bool {
        if let Some(binding) = self.binding {
            ordered.contains(&VertexId::Pattern(binding))
        } else {
            let adjacent = graph.variable_to_pattern().get(&index).unwrap();
            ordered.iter().filter_map(VertexId::as_pattern_id).any(|id| adjacent.contains(&id))
        }
    }

    pub(crate) fn add_equal(&mut self, other: Input) {
        self.restriction_value_equal.insert(other);
    }

    pub(crate) fn add_lower_bound(&mut self, other: Input) {
        self.restriction_value_below.insert(other);
    }

    pub(crate) fn add_upper_bound(&mut self, other: Input) {
        self.restriction_value_above.insert(other);
    }

    fn selectivity(&self, inputs: &[VertexId]) -> f64 {
        // since there's no "expected size" of a value variable (we will always assign exactly 1 value)
        // we arbitrarily set some thresholds for selectivity of predicates
        let mut selectivity = VariableVertex::RESTRICTION_NONE;
        if self.restriction_value_equal.iter().any(|restriction| is_input_available(restriction, inputs)) {
            selectivity *= Self::RESTRICTION_EQUAL_SELECTIVITY;
        }
        if self.restriction_value_below.iter().any(|restriction| is_input_available(restriction, inputs)) {
            selectivity *= Self::RESTRICTION_BELOW_SELECTIVITY
        }
        if self.restriction_value_above.iter().any(|restriction| is_input_available(restriction, inputs)) {
            selectivity *= Self::RESTRICTION_ABOVE_SELECTIVITY
        }
        selectivity
    }
}

impl Costed for ValuePlanner {
    fn cost(&self, inputs: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        if inputs.is_empty() {
            ElementCost { per_input: 0.0, per_output: 0.0, branching_factor: 1.0 }
        } else {
            ElementCost { per_input: f64::INFINITY, per_output: 0.0, branching_factor: f64::INFINITY }
        }
    }
}

fn is_input_available(input: &Input, available_inputs: &[VertexId]) -> bool {
    match input {
        Input::Fixed => true,
        Input::Variable(variable_id) => available_inputs
            .iter()
            .any(|available| available.as_variable_id().is_some_and(|avail| avail == *variable_id)),
    }
}
