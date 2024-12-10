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
        plan::{PatternVertexId, VariableVertexId, VertexId},
        vertex::Input,
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
    const SELECTIVITY_MIN: f64 = 0.000001;
    pub(crate) const OUTPUT_SIZE_MIN: f64 = 1.0; // TODO: investigate

    pub(crate) fn expected_output_size(&self, inputs: &[VertexId]) -> f64 {
        let unrestricted_size = match self {
            Self::Input(_) => 1.0,
            Self::Type(inner) => inner.unrestricted_expected_size,
            Self::Thing(inner) => inner.unrestricted_expected_size,
            Self::Value(_) => 1.0,
        };
        f64::max(unrestricted_size * self.restriction_based_selectivity(inputs), Self::OUTPUT_SIZE_MIN)
    }

    pub(crate) fn unrestricted_expected_output_size(&self) -> f64 {
        let unrestricted_size = match self {
            Self::Input(_) => 1.0,
            Self::Type(inner) => inner.unrestricted_expected_size,
            Self::Thing(inner) => inner.unrestricted_expected_size,
            Self::Value(_) => 1.0,
        };
        f64::max(unrestricted_size, Self::OUTPUT_SIZE_MIN)
    }

    pub(crate) fn restriction_based_selectivity(&self, inputs: &[VertexId]) -> f64 {
        // the fraction of possible actual outputs (based on type information) when restricted (for example, by comparators)
        match self {
            VariableVertex::Input(_) => Self::RESTRICTION_NONE,
            VariableVertex::Type(inner) => inner.restriction_based_selectivity(inputs),
            VariableVertex::Thing(inner) => inner.restriction_based_selectivity(inputs),
            VariableVertex::Value(inner) => inner.restriction_based_selectivity(inputs),
        }
    }

    pub(crate) fn binding(&self) -> Option<PatternVertexId> {
        match self {
            Self::Input(_) => None,
            Self::Type(inner) => inner.binding,
            Self::Thing(inner) => inner.binding,
            Self::Value(inner) => inner.binding,
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

    pub(crate) fn variable(&self) -> Variable {
        match self {
            VariableVertex::Input(var) => var.variable,
            VariableVertex::Type(var) => var.variable,
            VariableVertex::Thing(var) => var.variable,
            VariableVertex::Value(var) => var.variable,
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

    fn restriction_based_selectivity(&self, _inputs: &[VertexId]) -> f64 {
        // TODO: if we incorporate, say, annotations, we could add some selectivity here
        VariableVertex::RESTRICTION_NONE
    }
}

#[derive(Clone)]
pub(crate) struct ThingPlanner {
    variable: Variable,
    binding: Option<PatternVertexId>,
    pub unrestricted_expected_size: f64,
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
        let mut unrestricted_expected_size: f64 = 1.0;
        let mut unrestricted_expected_attribute_types: usize = 0;
        for type_ in type_annotations
            .vertex_annotations_of(&Vertex::Variable(variable))
            .expect("expected thing variable to have been annotated with types")
            .iter()
        {
            match type_ {
                answer::Type::Entity(type_) => {
                    if let Some(count) = statistics.entity_counts.get(type_) {
                        unrestricted_expected_size += *count as f64;
                    }
                }
                answer::Type::Relation(type_) => {
                    if let Some(count) = statistics.relation_counts.get(type_) {
                        unrestricted_expected_size += *count as f64;
                    }
                }
                answer::Type::Attribute(type_) => {
                    if let Some(count) = statistics.attribute_counts.get(type_) {
                        unrestricted_expected_size += *count as f64;
                        unrestricted_expected_attribute_types += 1;
                    }
                }
                answer::Type::RoleType(type_) => {
                    panic!("Found a Thing variable `{variable}` with a Role Type annotation: {type_}")
                }
            }
        }

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

    fn restriction_based_selectivity(&self, inputs: &[VertexId]) -> f64 {
        // decrease selectivity whenever we have any matching restrictions
        let bias: f64 = 2.0;
        let selectivity = if self
            .restriction_exact
            .iter()
            .any(|restriction| is_input_available(&Input::Variable(*restriction), inputs))
        {
            // exactly 1 of the full set is selected
            1.0 / (self.unrestricted_expected_size * bias)
        } else {
            // all are selected
            let mut selected = self.unrestricted_expected_size;
            let mut any_restrictions = false;
            if self.restriction_equal.iter().any(|restriction| is_input_available(restriction, inputs)) {
                // equality by value leads to one possible per attribute type
                selected = self.unrestricted_expected_attribute_types as f64;
                any_restrictions = true;
            }
            if self.restriction_from_below.iter().any(|restriction| is_input_available(restriction, inputs)) {
                // some fraction of the selected will pass the strictest below filter
                selected *= Self::RESTRICTION_BELOW_SELECTIVITY;
                any_restrictions = true;
            }
            if self.restriction_from_above.iter().any(|restriction| is_input_available(restriction, inputs)) {
                // some fraction of the selected will pass the strictest above filter
                selected *= Self::RESTRICTION_ABOVE_SELECTIVITY;
                any_restrictions = true;
            }
            // normalise again by all possible (with no restrictions, we get selectivity of 1.0)
            if any_restrictions {
                selected / (self.unrestricted_expected_size * bias)
            } else {
                selected / self.unrestricted_expected_size
            }
        };
        f64::max(selectivity, VariableVertex::SELECTIVITY_MIN)
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

    pub(crate) fn add_equal(&mut self, other: Input) {
        self.restriction_value_equal.insert(other);
    }

    pub(crate) fn add_lower_bound(&mut self, other: Input) {
        self.restriction_value_below.insert(other);
    }

    pub(crate) fn add_upper_bound(&mut self, other: Input) {
        self.restriction_value_above.insert(other);
    }

    fn restriction_based_selectivity(&self, inputs: &[VertexId]) -> f64 {
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
        f64::max(selectivity, VariableVertex::SELECTIVITY_MIN)
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
