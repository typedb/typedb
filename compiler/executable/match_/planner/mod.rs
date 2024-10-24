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
use ir::pipeline::{block::Block, function_signature::FunctionID, VariableRegistry};
use itertools::Itertools;

use crate::{
    annotation::{expression::compiled_expression::ExecutableExpression, type_annotations::TypeAnnotations},
    executable::match_::{
        instructions::{CheckInstruction, ConstraintInstruction},
        planner::{
            match_executable::{
                AssignmentStep, CheckStep, DisjunctionStep, ExecutionStep, FunctionCallStep, IntersectionStep,
                MatchExecutable, NegationStep,
            },
            plan::plan_conjunction,
        },
    },
    ExecutorVariable, VariablePosition,
};
use crate::executable::match_::planner::function_plan::ExecutableFunctionRegistry;

pub mod function_plan;
pub mod match_executable;
mod plan;
mod vertex;

pub fn compile(
    block: &Block,
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: Arc<VariableRegistry>,
    function_registry: &ExecutableFunctionRegistry, // TODO: When we have actual function costs
    expressions: &HashMap<Variable, ExecutableExpression<Variable>>,
    statistics: &Statistics,
) -> MatchExecutable {
    let conjunction = block.conjunction();
    let block_context = block.block_context();
    debug_assert!(conjunction.captured_variables(block_context).all(|var| input_variables.contains_key(&var)));

    let assigned_identities =
        input_variables.iter().map(|(&var, &position)| (var, ExecutorVariable::RowPosition(position))).collect();

    plan_conjunction(
        conjunction,
        block_context,
        input_variables,
        type_annotations,
        &variable_registry,
        expressions,
        statistics,
    )
    .lower(variable_registry.variable_names().keys().copied(), &assigned_identities, &variable_registry)
    .finish(variable_registry)
}

#[derive(Debug)]
struct IntersectionBuilder {
    sort_variable: Option<Variable>,
    instructions: Vec<ConstraintInstruction<ExecutorVariable>>,
}

impl IntersectionBuilder {
    fn new() -> Self {
        Self { sort_variable: None, instructions: Vec::new() }
    }
}

#[derive(Debug)]
struct ExpressionBuilder {
    executable_expression: ExecutableExpression<VariablePosition>,
    output: ExecutorVariable,
}

#[derive(Debug, Default)]
struct CheckBuilder {
    instructions: Vec<CheckInstruction<ExecutorVariable>>,
}

#[derive(Debug)]
struct NegationBuilder {
    negation: MatchExecutableBuilder,
}

impl NegationBuilder {
    fn new(negation: MatchExecutableBuilder) -> Self {
        Self { negation }
    }
}

#[derive(Debug)]
struct DisjunctionBuilder {
    branches: Vec<MatchExecutableBuilder>,
}

impl DisjunctionBuilder {
    fn new(branches: Vec<MatchExecutableBuilder>) -> Self {
        Self { branches }
    }
}

#[derive(Debug)]
struct FunctionCallBuilder {
    function_id: FunctionID,
    arguments: Vec<VariablePosition>,
    assigned: Vec<VariablePosition>,
    output_width: u32,
}

#[derive(Debug)]
// TODO rename
enum StepInstructionsBuilder {
    Intersection(IntersectionBuilder),
    Check(CheckBuilder),
    Negation(NegationBuilder),
    Disjunction(DisjunctionBuilder),
    Expression(ExpressionBuilder),
    FunctionCall(FunctionCallBuilder),
}

impl StepInstructionsBuilder {
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
}

impl From<StepInstructionsBuilder> for StepBuilder {
    fn from(instructions_builder: StepInstructionsBuilder) -> Self {
        StepBuilder { selected_variables: Vec::new(), builder: instructions_builder }
    }
}

#[derive(Debug)]
struct StepBuilder {
    selected_variables: Vec<Variable>,
    builder: StepInstructionsBuilder,
}

impl StepBuilder {
    fn finish(
        self,
        index: &HashMap<Variable, ExecutorVariable>,
        named_variables: &HashSet<ExecutorVariable>,
        variable_registry: Arc<VariableRegistry>,
    ) -> ExecutionStep {
        let selected_variables = self
            .selected_variables
            .into_iter()
            .filter_map(|var| index.get(&var).and_then(ExecutorVariable::as_position))
            .collect_vec();
        let output_width = selected_variables.iter().map(|position| position.as_usize() as u32 + 1).max().unwrap_or(0);

        match self.builder {
            StepInstructionsBuilder::Intersection(IntersectionBuilder { sort_variable, instructions }) => {
                let sort_variable = index[&sort_variable.unwrap()];
                ExecutionStep::Intersection(IntersectionStep::new(
                    sort_variable,
                    instructions,
                    selected_variables,
                    named_variables,
                    output_width,
                ))
            }
            StepInstructionsBuilder::Check(CheckBuilder { instructions }) => {
                ExecutionStep::Check(CheckStep::new(instructions, selected_variables, output_width))
            }
            StepInstructionsBuilder::Expression(ExpressionBuilder { executable_expression, output }) => {
                let input_positions = executable_expression.variables.iter().copied().unique().collect_vec();
                ExecutionStep::Assignment(AssignmentStep::new(
                    executable_expression,
                    input_positions,
                    output,
                    selected_variables,
                    output_width,
                ))
            }
            StepInstructionsBuilder::Negation(NegationBuilder { negation }) => ExecutionStep::Negation(
                NegationStep::new(negation.finish(variable_registry), selected_variables, output_width),
            ),
            StepInstructionsBuilder::Disjunction(DisjunctionBuilder { branches }) => {
                ExecutionStep::Disjunction(DisjunctionStep::new(
                    branches.into_iter().map(|builder| builder.finish(variable_registry.clone())).collect(),
                    selected_variables,
                    output_width,
                ))
            }
            StepInstructionsBuilder::FunctionCall(FunctionCallBuilder {
                function_id,
                arguments,
                assigned,
                output_width,
                ..
            }) => ExecutionStep::FunctionCall(FunctionCallStep { function_id, arguments, assigned, output_width }),
        }
    }
}

#[derive(Debug)]
struct MatchExecutableBuilder {
    selected_variables: Vec<Variable>,
    current_outputs: Vec<Variable>,
    produced_so_far: HashSet<Variable>,

    steps: Vec<StepBuilder>,
    current: Option<Box<StepBuilder>>,

    reverse_index: HashMap<ExecutorVariable, Variable>,
    index: HashMap<Variable, ExecutorVariable>,
    next_output: VariablePosition,
}

impl MatchExecutableBuilder {
    fn new(assigned_positions: &HashMap<Variable, ExecutorVariable>, selected_variables: Vec<Variable>) -> Self {
        let index = assigned_positions.clone();
        let current_outputs = index.keys().copied().collect();
        let reverse_index = index.iter().map(|(&var, &pos)| (pos, var)).collect();
        let next_position = assigned_positions
            .values()
            .filter_map(ExecutorVariable::as_position)
            .max()
            .map(|pos| pos.position + 1)
            .unwrap_or(0);
        let next_output = VariablePosition::new(next_position);
        Self {
            selected_variables,
            current_outputs,
            produced_so_far: HashSet::new(),
            steps: Vec::new(),
            current: None,
            reverse_index,
            index,
            next_output,
        }
    }

    fn push_instruction(&mut self, sort_variable: Variable, instruction: ConstraintInstruction<Variable>) {
        if let Some(StepBuilder { builder: StepInstructionsBuilder::Intersection(intersection_builder), .. }) =
            self.current.as_deref()
        {
            if let Some(current_sort) = intersection_builder.sort_variable {
                if current_sort != sort_variable || instruction.is_input_variable(current_sort) {
                    self.finish_one();
                }
            }
        }

        if self.current.as_ref().is_some_and(|builder| !builder.builder.is_intersection()) {
            self.finish_one();
        }

        if self.current.is_none() {
            self.current = Some(Box::new(StepBuilder {
                selected_variables: self.current_outputs.clone(),
                builder: StepInstructionsBuilder::Intersection(IntersectionBuilder::new()),
            }));
        }

        instruction.new_variables_foreach(|variable| {
            self.produced_so_far.insert(variable);
        });

        let current = self.current.as_mut().unwrap().builder.as_intersection_mut().unwrap();
        current.sort_variable = Some(sort_variable);
        current.instructions.push(instruction.map(&self.index));
    }

    fn push_check(&mut self, variables: &[Variable], check: CheckInstruction<ExecutorVariable>) {
        if let Some(intersection) = self.current.as_mut().and_then(|b| b.builder.as_intersection_mut()) {
            for instruction in intersection.instructions.iter_mut().rev() {
                let mut is_producer = false;
                instruction.new_variables_foreach(|var| is_producer |= variables.contains(&self.reverse_index[&var]));
                if is_producer {
                    instruction.add_check(check);
                    self.current.as_mut().unwrap().selected_variables = self.current_outputs.clone();
                    return;
                }
            }
        }
        // all variables are inputs
        if self.current.as_ref().is_some_and(|builder| !builder.builder.is_check()) {
            self.finish_one();
        }
        if self.current.is_none() {
            self.current = Some(Box::new(StepBuilder {
                selected_variables: self.current_outputs.clone(),
                builder: StepInstructionsBuilder::Check(CheckBuilder::default()),
            }))
        }
        let current = self.current.as_mut().unwrap().builder.as_check_mut().unwrap();
        current.instructions.push(check);
    }

    fn push_step(&mut self, variable_positions: &HashMap<Variable, ExecutorVariable>, mut step: StepBuilder) {
        self.finish_one();
        for (&var, &pos) in variable_positions {
            if !self.position_mapping().contains_key(&var) {
                self.index.insert(var, pos);
                self.reverse_index.insert(pos, var);
                self.next_output.position += 1;
            }
        }
        self.produced_so_far.extend(self.current_outputs.iter().copied());
        step.selected_variables = self.current_outputs.clone();

        self.steps.push(step);
    }

    fn position_mapping(&self) -> &HashMap<Variable, ExecutorVariable> {
        &self.index
    }

    fn position(&self, var: Variable) -> ExecutorVariable {
        self.index[&var]
    }

    fn register_output(&mut self, var: Variable) {
        if let hash_map::Entry::Vacant(entry) = self.index.entry(var) {
            entry.insert(ExecutorVariable::RowPosition(self.next_output));
            self.reverse_index.insert(ExecutorVariable::RowPosition(self.next_output), var);
            self.current_outputs.push(var);
            self.next_output.position += 1;
        }
    }

    fn register_internal(&mut self, var: Variable) {
        if let hash_map::Entry::Vacant(entry) = self.index.entry(var) {
            entry.insert(ExecutorVariable::Internal(var));
            self.reverse_index.insert(ExecutorVariable::Internal(var), var);
        }
    }

    fn remove_output(&mut self, var: Variable) {
        if !self.selected_variables.contains(&var) {
            self.current_outputs.retain(|v| v != &var);
        }
    }

    fn finish_one(&mut self) {
        if let Some(mut current) = self.current.take() {
            current.selected_variables = self.current_outputs.clone();
            self.steps.push(*current);
        }
    }

    fn finish(mut self, variable_registry: Arc<VariableRegistry>) -> MatchExecutable {
        self.finish_one();
        let named_variables = self
            .index
            .iter()
            .filter_map(|(var, &pos)| variable_registry.variable_names().get(var).and(Some(pos)))
            .collect();
        let steps = self
            .steps
            .into_iter()
            .map(|builder| builder.finish(&self.index, &named_variables, variable_registry.clone()))
            .collect();
        let variable_positions_index = self.reverse_index.iter().sorted_by_key(|(&k, _)| k).map(|(_, &v)| v).collect();
        MatchExecutable::new(
            steps,
            self.index.into_iter().filter_map(|(var, id)| Some((var, id.as_position()?))).collect(),
            variable_positions_index,
        )
    }
}
