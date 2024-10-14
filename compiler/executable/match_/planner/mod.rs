/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, HashMap, HashSet},
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::ScopeId,
    pipeline::{block::Block, VariableRegistry},
};
use itertools::Itertools;

use crate::{
    annotation::{expression::compiled_expression::ExecutableExpression, type_annotations::TypeAnnotations},
    executable::match_::{
        instructions::{CheckInstruction, ConstraintInstruction},
        planner::{
            match_executable::{
                CheckStep, DisjunctionStep, ExecutionStep, IntersectionStep, MatchExecutable, NegationStep,
            },
            plan::plan_conjunction,
        },
    },
    VariablePosition,
};

pub mod function_plan;
pub mod match_executable;
mod plan;
mod vertex;

pub fn compile(
    block: &Block,
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: Arc<VariableRegistry>,
    expressions: &HashMap<Variable, ExecutableExpression>,
    statistics: &Statistics,
) -> MatchExecutable {
    let conjunction = block.conjunction();
    let block_context = block.block_context();
    debug_assert!(conjunction.captured_variables(block_context).all(|var| input_variables.contains_key(&var)));
    plan_conjunction(
        conjunction,
        block_context,
        input_variables,
        type_annotations,
        &variable_registry,
        expressions,
        statistics,
    )
    .lower(
        &block.scope_context().get_variable_scopes().map(|(var, _)| var).collect_vec(),
        input_variables,
        variable_registry,
    )
}

#[derive(Debug)]
struct IntersectionBuilder {
    sort_variable: Option<Variable>,
    instructions: Vec<ConstraintInstruction<VariablePosition>>,
    selected_variables: Vec<Variable>,
    output_width: Option<u32>,
}

impl IntersectionBuilder {
    fn new(selected_variables: Vec<Variable>) -> Self {
        Self { sort_variable: None, instructions: Vec::new(), selected_variables, output_width: None }
    }
}

#[derive(Debug, Default)]
struct CheckBuilder {
    instructions: Vec<CheckInstruction<VariablePosition>>,
    output_width: Option<u32>,
}

#[derive(Debug)]
struct NegationBuilder {
    negation: MatchExecutable,
    output_width: Option<u32>,
}

impl NegationBuilder {
    fn new(negation: MatchExecutable) -> Self {
        Self { negation, output_width: None }
    }
}

#[derive(Debug)]
struct DisjunctionBuilder {
    branches: Vec<MatchExecutable>,
    selected_variables: Vec<Variable>,
    output_width: Option<u32>,
}

impl DisjunctionBuilder {
    fn new(branches: Vec<MatchExecutable>, selected_variables: Vec<Variable>) -> Self {
        Self { branches, selected_variables, output_width: None }
    }
}

#[derive(Debug)]
enum StepBuilder {
    Intersection(IntersectionBuilder),
    Check(CheckBuilder),
    Negation(NegationBuilder),
    Disjunction(DisjunctionBuilder),
}

impl StepBuilder {
    fn finish(
        self,
        index: &HashMap<Variable, VariablePosition>,
        named_variables: &HashSet<VariablePosition>,
    ) -> ExecutionStep {
        match self {
            Self::Intersection(IntersectionBuilder {
                sort_variable,
                selected_variables,
                instructions,
                output_width,
            }) => {
                let sort_variable = sort_variable.map(|var| index[&var]).unwrap();
                ExecutionStep::Intersection(IntersectionStep::new(
                    sort_variable,
                    instructions,
                    &selected_variables.into_iter().map(|var| index[&var]).collect_vec(),
                    named_variables,
                    output_width.unwrap(),
                ))
            }
            Self::Check(CheckBuilder { instructions, output_width }) => {
                ExecutionStep::Check(CheckStep::new(instructions, output_width.unwrap()))
            }
            Self::Negation(NegationBuilder { negation, output_width }) => {
                ExecutionStep::Negation(NegationStep::new(negation, output_width.unwrap()))
            }
            Self::Disjunction(DisjunctionBuilder { branches, selected_variables, output_width }) => {
                ExecutionStep::Disjunction(DisjunctionStep {
                    branches,
                    selected_variables: selected_variables.into_iter().map(|var| index[&var]).collect(),
                    output_width: output_width.unwrap(),
                })
            }
        }
    }

    fn as_intersection(&self) -> Option<&IntersectionBuilder> {
        match self {
            Self::Intersection(v) => Some(v),
            _ => None,
        }
    }

    fn as_intersection_mut(&mut self) -> Option<&mut IntersectionBuilder> {
        match self {
            Self::Intersection(v) => Some(v),
            _ => None,
        }
    }

    fn as_check_mut(&mut self) -> Option<&mut CheckBuilder> {
        match self {
            Self::Check(v) => Some(v),
            _ => None,
        }
    }

    /// Returns `true` if the step builder is [`Intersection`].
    ///
    /// [`Intersection`]: StepBuilder::Intersection
    #[must_use]
    fn is_intersection(&self) -> bool {
        matches!(self, Self::Intersection(..))
    }

    /// Returns `true` if the step builder is [`Check`].
    ///
    /// [`Check`]: StepBuilder::Check
    #[must_use]
    fn is_check(&self) -> bool {
        matches!(self, Self::Check(..))
    }

    fn set_output_width(&mut self, position: u32) {
        match self {
            | StepBuilder::Intersection(IntersectionBuilder { output_width, .. })
            | StepBuilder::Disjunction(DisjunctionBuilder { output_width, .. })
            | StepBuilder::Check(CheckBuilder { output_width, .. })
            | StepBuilder::Negation(NegationBuilder { output_width, .. }) => *output_width = Some(position),
        }
    }
}

struct MatchExecutableBuilder {
    steps: Vec<StepBuilder>,
    current: Option<StepBuilder>,
    outputs: HashMap<VariablePosition, Variable>,
    producers: HashMap<Variable, (usize, usize)>,
    selected_variables: Vec<Variable>,
    index: HashMap<Variable, VariablePosition>,
    next_output: VariablePosition,
}

impl MatchExecutableBuilder {
    fn new(input_variables: &HashMap<Variable, VariablePosition>, selected_variables: Vec<Variable>) -> Self {
        let index = input_variables.clone();
        let outputs = index.iter().map(|(&var, &pos)| (pos, var)).collect();
        let next_position = input_variables.values().max().map(|&pos| pos.position + 1).unwrap_or_default();
        let next_output = VariablePosition::new(next_position);
        Self {
            steps: Vec::new(),
            current: None,
            outputs,
            producers: HashMap::new(),
            selected_variables,
            index,
            next_output,
        }
    }

    fn get_step_mut(&mut self, step: usize) -> Option<&mut StepBuilder> {
        self.steps.get_mut(step).or(self.current.as_mut())
    }

    fn push_instruction(
        &mut self,
        sort_variable: Variable,
        instruction: ConstraintInstruction<Variable>,
        outputs: impl IntoIterator<Item = Variable>,
    ) {
        if let Some(StepBuilder::Intersection(intersection_builder)) = &self.current {
            if let Some(current_sort) = intersection_builder.sort_variable {
                if current_sort != sort_variable || instruction.is_input_variable(current_sort) {
                    self.finish_one();
                }
            }
        }
        if self.current.as_ref().is_some_and(|builder| !builder.is_intersection()) {
            self.finish_one();
        }

        if self.current.is_none() {
            self.current = Some(StepBuilder::Intersection(IntersectionBuilder::new(self.selected_variables.clone())))
        }

        let current = self.current.as_ref().unwrap().as_intersection().unwrap();

        let producer_index = (self.steps.len(), current.instructions.len());
        for var in outputs {
            self.register_output(var);
            self.producers.entry(var).or_insert(producer_index);
        }

        let current = self.current.as_mut().unwrap().as_intersection_mut().unwrap();
        current.sort_variable = Some(sort_variable);
        current.instructions.push(instruction.map(&self.index));
    }

    fn push_check(&mut self, variables: &[Variable], check: CheckInstruction<VariablePosition>) {
        let producer = variables.iter().map(|var| self.producers.get(var)).max().unwrap_or(None);
        if let Some(&(program, instruction)) = producer {
            let Some(intersection) = self.get_step_mut(program).and_then(|step| step.as_intersection_mut()) else {
                todo!("expected an intersection to be the producer")
            };
            intersection.instructions[instruction].add_check(check);
        } else {
            // all variables are inputs
            if self.current.as_ref().is_some_and(|builder| !builder.is_check()) {
                self.finish_one();
            }
            if self.current.is_none() {
                self.current = Some(StepBuilder::Check(CheckBuilder::default()))
            }
            let current = self.current.as_mut().unwrap().as_check_mut().unwrap();
            current.instructions.push(check);
        };
    }

    fn push_step(&mut self, variable_positions: &HashMap<Variable, VariablePosition>, mut step: StepBuilder) {
        if self.current.is_some() {
            self.finish_one();
        }
        for (&var, &pos) in variable_positions {
            if !self.position_mapping().contains_key(&var) {
                self.index.insert(var, pos);
                self.outputs.insert(pos, var);
                self.next_output.position += 1;
            }
        }
        step.set_output_width(self.next_output.position);
        self.steps.push(step);
    }

    fn position_mapping(&self) -> &HashMap<Variable, VariablePosition> {
        &self.index
    }

    fn position(&self, var: Variable) -> VariablePosition {
        self.index[&var]
    }

    fn register_output(&mut self, var: Variable) {
        if let hash_map::Entry::Vacant(entry) = self.index.entry(var) {
            entry.insert(self.next_output);
            self.outputs.insert(self.next_output, var);
            self.next_output.position += 1;
        }
    }

    fn finish_one(&mut self) {
        if let Some(mut current) = self.current.take() {
            current.set_output_width(self.next_output.position);
            self.steps.push(current);
        }
    }

    fn finish(mut self, variable_registry: Arc<VariableRegistry>) -> MatchExecutable {
        self.finish_one();
        let named_variables = self
            .index
            .iter()
            .filter_map(|(var, &pos)| variable_registry.variable_names().get(var).and(Some(pos)))
            .collect();
        let steps = self.steps.into_iter().map(|builder| builder.finish(&self.index, &named_variables)).collect();
        let variable_positions_index =
            self.outputs.iter().sorted_by_key(|(k, _)| k.as_usize()).map(|(_, &v)| v).collect();
        MatchExecutable::new(steps, self.index.clone(), variable_positions_index)
    }
}
