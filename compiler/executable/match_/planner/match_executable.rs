/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use answer::variable::Variable;
use ir::{
    pattern::{
        constraint::{Constraint, ExpressionBinding},
        IrID,
    },
    pipeline::VariableRegistry,
};

use crate::{
    executable::match_::instructions::{CheckInstruction, ConstraintInstruction, VariableModes},
    VariablePosition,
};

#[derive(Clone, Debug)]
pub struct MatchExecutable {
    pub(crate) steps: Vec<ExecutionStep>,
    pub(crate) variable_registry: Arc<VariableRegistry>, // TODO: Maybe we never need this?

    variable_positions: HashMap<Variable, VariablePosition>,
    variable_positions_index: Vec<Variable>,
}

impl MatchExecutable {
    pub fn new(
        steps: Vec<ExecutionStep>,
        variable_registry: Arc<VariableRegistry>,
        variable_positions: HashMap<Variable, VariablePosition>,
        variable_positions_index: Vec<Variable>,
    ) -> Self {
        Self { steps, variable_registry, variable_positions, variable_positions_index }
    }

    pub fn steps(&self) -> &[ExecutionStep] {
        &self.steps
    }

    pub fn outputs(&self) -> &[VariablePosition] {
        self.steps.last().unwrap().selected_variables()
    }

    pub fn variable_registry(&self) -> &VariableRegistry {
        &self.variable_registry
    }

    pub fn variable_positions(&self) -> &HashMap<Variable, VariablePosition> {
        &self.variable_positions
    }

    pub fn variable_positions_index(&self) -> &[Variable] {
        &self.variable_positions_index
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
}

impl ExecutionStep {
    pub fn selected_variables(&self) -> &[VariablePosition] {
        match self {
            ExecutionStep::Intersection(step) => &step.selected_variables,
            ExecutionStep::UnsortedJoin(step) => &step.selected_variables,
            ExecutionStep::Assignment(_) => todo!(),
            ExecutionStep::Check(_) => &[],
            ExecutionStep::Disjunction(_) => todo!(),
            ExecutionStep::Negation(_) => &[],
            ExecutionStep::Optional(_) => todo!(),
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
        }
    }

    pub fn output_width(&self) -> u32 {
        match self {
            ExecutionStep::Intersection(step) => step.output_width(),
            ExecutionStep::UnsortedJoin(step) => step.output_width(),
            ExecutionStep::Assignment(step) => step.output_width(),
            ExecutionStep::Check(_) => 0, // FIXME is this correct?
            ExecutionStep::Disjunction(_) => todo!(),
            ExecutionStep::Negation(_) => 0,
            ExecutionStep::Optional(_) => todo!(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IntersectionStep {
    pub sort_variable: VariablePosition,
    pub instructions: Vec<(ConstraintInstruction<VariablePosition>, VariableModes)>,
    new_variables: Vec<VariablePosition>,
    output_width: u32,
    input_variables: Vec<VariablePosition>,
    pub selected_variables: Vec<VariablePosition>,
}

impl IntersectionStep {
    pub fn new(
        sort_variable: VariablePosition,
        instructions: Vec<ConstraintInstruction<VariablePosition>>,
        selected_variables: &[VariablePosition],
        named_variables: &HashSet<VariablePosition>,
        output_width: u32,
    ) -> Self {
        let mut input_variables = Vec::with_capacity(instructions.len() * 2);
        let mut new_variables = Vec::with_capacity(instructions.len() * 2);
        instructions.iter().for_each(|instruction| {
            instruction.new_variables_foreach(|var| {
                if !new_variables.contains(&var) {
                    new_variables.push(var)
                }
            });
            instruction.input_variables_foreach(|var| {
                if !input_variables.contains(&var) {
                    input_variables.push(var)
                }
            });
        });

        let instructions = instructions
            .into_iter()
            .map(|instruction| {
                let variable_modes = VariableModes::new_for(&instruction, selected_variables, named_variables);
                (instruction, variable_modes)
            })
            .collect();
        let selected_variables = selected_variables.to_owned();
        Self { sort_variable, instructions, new_variables, output_width, input_variables, selected_variables }
    }

    fn new_variables(&self) -> &[VariablePosition] {
        &self.new_variables
    }

    fn output_width(&self) -> u32 {
        self.output_width
    }
}

#[derive(Clone, Debug)]
pub struct UnsortedJoinStep {
    pub iterate_instruction: ConstraintInstruction<VariablePosition>,
    pub check_instructions: Vec<ConstraintInstruction<VariablePosition>>,
    new_variables: Vec<VariablePosition>,
    input_variables: Vec<VariablePosition>,
    selected_variables: Vec<VariablePosition>,
}

impl UnsortedJoinStep {
    pub fn new(
        iterate_instruction: ConstraintInstruction<VariablePosition>,
        check_instructions: Vec<ConstraintInstruction<VariablePosition>>,
        selected_variables: &[VariablePosition],
    ) -> Self {
        let mut input_variables = Vec::with_capacity(check_instructions.len() * 2);
        let mut new_variables = Vec::with_capacity(5);
        iterate_instruction.new_variables_foreach(|var| {
            if !new_variables.contains(&var) {
                new_variables.push(var)
            }
        });
        iterate_instruction.input_variables_foreach(|var| {
            if !input_variables.contains(&var) {
                input_variables.push(var)
            }
        });
        check_instructions.iter().for_each(|instruction| {
            instruction.input_variables_foreach(|var| {
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
        }
    }

    fn new_variables(&self) -> &[VariablePosition] {
        &self.new_variables
    }

    fn output_width(&self) -> u32 {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct AssignmentStep {
    assign_instruction: ExpressionBinding<VariablePosition>,
    check_instructions: Vec<ConstraintInstruction<VariablePosition>>,
    unbound: [VariablePosition; 1],
}

impl AssignmentStep {
    fn new_variables(&self) -> &[VariablePosition] {
        &self.unbound
    }

    fn output_width(&self) -> u32 {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct CheckStep {
    pub check_instructions: Vec<CheckInstruction<VariablePosition>>,
}

impl CheckStep {
    pub fn new(check_instructions: Vec<CheckInstruction<VariablePosition>>) -> Self {
        Self { check_instructions }
    }
}

#[derive(Clone, Debug)]
pub struct DisjunctionStep {
    pub disjunction: Vec<MatchExecutable>,
}

#[derive(Clone, Debug)]
pub struct NegationStep {
    pub negation: MatchExecutable,
}

#[derive(Clone, Debug)]
pub struct OptionalStep {
    pub optional: MatchExecutable,
}

pub trait InstructionAPI<ID: IrID> {
    fn constraint(&self) -> Constraint<ID>;
}
