/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    slice,
};

use answer::variable::Variable;
use ir::{
    pattern::{constraint::Constraint, IrID},
    pipeline::function_signature::FunctionID,
};

use crate::{
    annotation::expression::compiled_expression::ExecutableExpression,
    executable::match_::instructions::{CheckInstruction, ConstraintInstruction, VariableModes},
    ExecutorVariable, VariablePosition,
};

#[derive(Clone, Debug)]
pub struct MatchExecutable {
    pub(crate) steps: Vec<ExecutionStep>,
    variable_positions: HashMap<Variable, VariablePosition>,
    variable_positions_index: Vec<Variable>,
}

impl MatchExecutable {
    pub fn new(
        steps: Vec<ExecutionStep>,
        variable_positions: HashMap<Variable, VariablePosition>,
        variable_positions_index: Vec<Variable>,
    ) -> Self {
        Self { steps, variable_positions, variable_positions_index }
    }

    pub fn steps(&self) -> &[ExecutionStep] {
        &self.steps
    }

    pub fn outputs(&self) -> &[VariablePosition] {
        self.steps.last().unwrap().selected_variables()
    }

    pub fn variable_positions(&self) -> &HashMap<Variable, VariablePosition> {
        &self.variable_positions
    }

    pub fn variable_positions_index(&self) -> &[Variable] {
        &self.variable_positions_index
    }
}

impl Display for MatchExecutable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Match executable plan:")?;
        for (i, step) in self.steps().iter().enumerate() {
            write!(f, "\n  {i}: {step}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum ExecutionStep {
    Intersection(IntersectionStep),
    UnsortedJoin(UnsortedJoinStep),
    Assignment(AssignmentStep),
    Check(CheckStep),
    Disjunction(DisjunctionStep),
    Negation(NegationStep),
    Optional(OptionalStep),
    FunctionCall(FunctionCallStep),
}

impl ExecutionStep {
    pub fn selected_variables(&self) -> &[VariablePosition] {
        match self {
            ExecutionStep::Intersection(step) => &step.selected_variables,
            ExecutionStep::UnsortedJoin(step) => &step.selected_variables,
            ExecutionStep::Assignment(_) => todo!(),
            ExecutionStep::Check(step) => &step.selected_variables,
            ExecutionStep::Disjunction(step) => &step.selected_variables,
            ExecutionStep::Negation(step) => &step.selected_variables,
            ExecutionStep::Optional(_) => todo!(),
            ExecutionStep::FunctionCall(function_call) => function_call.assigned.as_slice(),
        }
    }

    pub fn new_variables(&self) -> &[VariablePosition] {
        match self {
            ExecutionStep::Intersection(step) => step.new_variables(),
            ExecutionStep::UnsortedJoin(step) => step.new_variables(),
            ExecutionStep::Assignment(step) => step.new_variables(),
            ExecutionStep::Check(_) => &[],
            ExecutionStep::Disjunction(_) => todo!(),
            ExecutionStep::Negation(_) => &[],
            ExecutionStep::Optional(_) => todo!(),
            ExecutionStep::FunctionCall(function_call) => function_call.assigned.as_slice(),
        }
    }

    pub fn output_width(&self) -> u32 {
        match self {
            ExecutionStep::Intersection(step) => step.output_width(),
            ExecutionStep::UnsortedJoin(step) => step.output_width(),
            ExecutionStep::Assignment(step) => step.output_width(),
            ExecutionStep::Check(step) => step.output_width(),
            ExecutionStep::Disjunction(step) => step.output_width(),
            ExecutionStep::Negation(step) => step.output_width(),
            ExecutionStep::Optional(_) => todo!(),
            ExecutionStep::FunctionCall(step) => step.output_width(),
        }
    }
}

impl Display for ExecutionStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStep::Intersection(step) => write!(f, "Sorted, {step}"),
            ExecutionStep::UnsortedJoin(step) => write!(f, "Unsorted, {step}"),
            ExecutionStep::Assignment(step) => write!(f, "Assign, {step}"),
            ExecutionStep::Check(step) => write!(f, "Check, {step}"),
            ExecutionStep::Disjunction(step) => write!(f, "Disjunction, {step}"),
            ExecutionStep::Negation(step) => write!(f, "Negation, {step}"),
            ExecutionStep::Optional(step) => write!(f, "Optional, {step}"),
            ExecutionStep::FunctionCall(step) => write!(f, "FunctionCall, {step}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IntersectionStep {
    pub sort_variable: ExecutorVariable,
    pub instructions: Vec<(ConstraintInstruction<ExecutorVariable>, VariableModes)>,
    new_variables: Vec<VariablePosition>,
    pub output_width: u32,
    input_variables: Vec<VariablePosition>,
    pub selected_variables: Vec<VariablePosition>,
}

impl IntersectionStep {
    pub fn new(
        sort_variable: ExecutorVariable,
        instructions: Vec<ConstraintInstruction<ExecutorVariable>>,
        selected_variables: Vec<VariablePosition>,
        named_variables: &HashSet<ExecutorVariable>,
        output_width: u32,
    ) -> Self {
        let mut input_variables = Vec::with_capacity(instructions.len() * 2);
        let mut new_variables = Vec::with_capacity(instructions.len() * 2);
        instructions.iter().for_each(|instruction| {
            instruction.new_variables_foreach(|var| {
                if let Some(var) = var.as_position() {
                    if !new_variables.contains(&var) {
                        new_variables.push(var)
                    }
                }
            });
            instruction.input_variables_foreach(|var| {
                let var = var.as_position().unwrap();
                if !input_variables.contains(&var) {
                    input_variables.push(var)
                }
            });
        });

        let instructions = instructions
            .into_iter()
            .map(|instruction| {
                let variable_modes = VariableModes::new_for(&instruction, &selected_variables, named_variables);
                (instruction, variable_modes)
            })
            .collect();
        Self { sort_variable, instructions, new_variables, output_width, input_variables, selected_variables }
    }

    fn new_variables(&self) -> &[VariablePosition] {
        &self.new_variables
    }

    fn output_width(&self) -> u32 {
        self.output_width
    }
}

impl Display for IntersectionStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "inputs={:?}, output_size={}, sort_by={}",
            &self.input_variables, self.output_width, self.sort_variable
        )?;
        for (instruction, modes) in &self.instructions {
            write!(f, "\n      {instruction} with ({modes})")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct UnsortedJoinStep {
    pub iterate_instruction: ConstraintInstruction<ExecutorVariable>,
    pub check_instructions: Vec<ConstraintInstruction<ExecutorVariable>>,
    new_variables: Vec<VariablePosition>,
    input_variables: Vec<VariablePosition>,
    selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
}

impl UnsortedJoinStep {
    pub fn new(
        iterate_instruction: ConstraintInstruction<ExecutorVariable>,
        check_instructions: Vec<ConstraintInstruction<ExecutorVariable>>,
        selected_variables: &[VariablePosition],
        output_width: u32,
    ) -> Self {
        let mut input_variables = Vec::with_capacity(check_instructions.len() * 2);
        let mut new_variables = Vec::with_capacity(5);
        iterate_instruction.new_variables_foreach(|var| {
            if let Some(var) = var.as_position() {
                if !new_variables.contains(&var) {
                    new_variables.push(var)
                }
            }
        });
        iterate_instruction.input_variables_foreach(|var| {
            let var = var.as_position().unwrap();
            if !input_variables.contains(&var) {
                input_variables.push(var)
            }
        });
        check_instructions.iter().for_each(|instruction| {
            instruction.input_variables_foreach(|var| {
                let var = var.as_position().unwrap();
                if !input_variables.contains(&var) {
                    input_variables.push(var)
                }
            })
        });
        Self {
            iterate_instruction,
            check_instructions,
            new_variables,
            input_variables,
            selected_variables: selected_variables.to_owned(),
            output_width,
        }
    }

    fn new_variables(&self) -> &[VariablePosition] {
        &self.new_variables
    }

    fn output_width(&self) -> u32 {
        todo!()
    }
}

impl Display for UnsortedJoinStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "inputs={:?}, output_size={:?}", &self.input_variables, self.output_width)?;
        write!(f, "\n      {}", &self.iterate_instruction)?;
        // TODO: do we need these at all?
        write!(f, "\n      {:?}", &self.check_instructions)
    }
}

#[derive(Clone, Debug)]
pub struct AssignmentStep {
    pub expression: ExecutableExpression<VariablePosition>,
    pub input_positions: Vec<VariablePosition>,
    pub unbound: ExecutorVariable,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
}

impl AssignmentStep {
    pub fn new(
        expression: ExecutableExpression<VariablePosition>,
        input_positions: Vec<VariablePosition>,
        unbound: ExecutorVariable,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        Self { expression, input_positions, unbound, selected_variables, output_width }
    }

    fn new_variables(&self) -> &[VariablePosition] {
        match &self.unbound {
            ExecutorVariable::RowPosition(pos) => slice::from_ref(pos),
            ExecutorVariable::Internal(_) => &[],
        }
    }

    fn output_width(&self) -> u32 {
        self.output_width
    }
}

impl Display for AssignmentStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "inputs={:?}, output_size={}", &self.input_positions, self.output_width)?;
        // TODO: Display expression
        write!(f, "\n      {:?}", &self.expression)
    }
}

#[derive(Clone, Debug)]
pub struct CheckStep {
    pub check_instructions: Vec<CheckInstruction<ExecutorVariable>>,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
}

impl CheckStep {
    pub fn new(
        check_instructions: Vec<CheckInstruction<ExecutorVariable>>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        Self { check_instructions, selected_variables, output_width }
    }

    pub fn output_width(&self) -> u32 {
        self.output_width
    }
}

impl Display for CheckStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for check in &self.check_instructions {
            write!(f, "\n      {}", check)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DisjunctionStep {
    pub branches: Vec<MatchExecutable>,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
}

impl DisjunctionStep {
    pub fn new(branches: Vec<MatchExecutable>, selected_variables: Vec<VariablePosition>, output_width: u32) -> Self {
        Self { branches, selected_variables, output_width }
    }

    pub fn output_width(&self) -> u32 {
        self.output_width
    }
}

impl Display for DisjunctionStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "output_size={}", self.output_width)?;
        for branch in &self.branches {
            write!(f, "\n      --- Start branch ---")?;
            write!(f, "{}", branch)?;
            write!(f, "\n      --- End branch ---")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct NegationStep {
    pub negation: MatchExecutable,
    pub selected_variables: Vec<VariablePosition>,
    pub output_width: u32,
}

impl NegationStep {
    pub fn new(negation: MatchExecutable, selected_variables: Vec<VariablePosition>, output_width: u32) -> Self {
        Self { negation, selected_variables, output_width }
    }

    pub fn output_width(&self) -> u32 {
        self.output_width
    }
}

impl Display for NegationStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "\n      --- Start negation ---")?;
        write!(f, "\n {}", &self.negation)?;
        write!(f, "\n      --- End negation ---")
    }
}

#[derive(Clone, Debug)]
pub struct OptionalStep {
    pub optional: MatchExecutable,
}

impl Display for OptionalStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "\n      --- Start negation ---")?;
        write!(f, "\n {}", &self.optional)?;
        write!(f, "\n      --- End negation ---")
    }
}

#[derive(Clone, Debug)]
pub struct FunctionCallStep {
    // TODO: Deduplication, selection counting etc.
    pub function_id: FunctionID,
    pub assigned: Vec<VariablePosition>,
    pub arguments: Vec<VariablePosition>,
    pub output_width: u32,
}

impl FunctionCallStep {
    pub(crate) fn output_width(&self) -> u32 {
        self.output_width
    }
}

impl Display for FunctionCallStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fn_id={}, assigned={:?}, arguments={:?}, output_size={}\n",
            self.function_id, &self.assigned, &self.arguments, self.output_width
        )
    }
}

pub trait InstructionAPI<ID: IrID> {
    fn constraint(&self) -> Constraint<ID>;
}
