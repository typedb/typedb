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
    program::VariableRegistry,
};

use crate::{
    match_::instructions::{CheckInstruction, ConstraintInstruction, VariableModes},
    VariablePosition,
};

#[derive(Clone, Debug)]
pub struct MatchProgram {
    pub(crate) programs: Vec<Program>,
    pub(crate) variable_registry: Arc<VariableRegistry>, // TODO: Maybe we never need this?

    variable_positions: HashMap<Variable, VariablePosition>,
    variable_positions_index: Vec<Variable>,
}

impl MatchProgram {
    pub fn new(
        programs: Vec<Program>,
        variable_registry: Arc<VariableRegistry>,
        variable_positions: HashMap<Variable, VariablePosition>,
        variable_positions_index: Vec<Variable>,
    ) -> Self {
        Self { programs, variable_registry, variable_positions, variable_positions_index }
    }

    pub fn programs(&self) -> &[Program] {
        &self.programs
    }

    pub fn outputs(&self) -> &[VariablePosition] {
        self.programs.last().unwrap().selected_variables()
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
pub enum Program {
    Intersection(IntersectionProgram),
    UnsortedJoin(UnsortedJoinProgram),
    Assignment(AssignmentProgram),
    Check(CheckProgram),
    Disjunction(DisjunctionProgram),
    Negation(NegationProgram),
    Optional(OptionalProgram),
}

impl Program {
    pub fn selected_variables(&self) -> &[VariablePosition] {
        match self {
            Program::Intersection(program) => &program.selected_variables,
            Program::UnsortedJoin(program) => &program.selected_variables,
            Program::Assignment(_) => todo!(),
            Program::Check(_) => &[],
            Program::Disjunction(_) => todo!(),
            Program::Negation(_) => &[],
            Program::Optional(_) => todo!(),
        }
    }

    pub fn new_variables(&self) -> &[VariablePosition] {
        match self {
            Program::Intersection(program) => program.new_variables(),
            Program::UnsortedJoin(program) => program.new_variables(),
            Program::Assignment(program) => program.new_variables(),
            Program::Check(_) => &[],
            Program::Disjunction(_) => todo!(),
            Program::Negation(_) => &[],
            Program::Optional(_) => todo!(),
        }
    }

    pub fn output_width(&self) -> u32 {
        match self {
            Program::Intersection(program) => program.output_width(),
            Program::UnsortedJoin(program) => program.output_width(),
            Program::Assignment(program) => program.output_width(),
            Program::Check(_) => 0, // FIXME is this correct?
            Program::Disjunction(_) => todo!(),
            Program::Negation(_) => 0,
            Program::Optional(_) => todo!(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IntersectionProgram {
    pub sort_variable: VariablePosition,
    pub instructions: Vec<(ConstraintInstruction<VariablePosition>, VariableModes)>,
    new_variables: Vec<VariablePosition>,
    output_width: u32,
    input_variables: Vec<VariablePosition>,
    pub selected_variables: Vec<VariablePosition>,
}

impl IntersectionProgram {
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
pub struct UnsortedJoinProgram {
    pub iterate_instruction: ConstraintInstruction<VariablePosition>,
    pub check_instructions: Vec<ConstraintInstruction<VariablePosition>>,
    new_variables: Vec<VariablePosition>,
    input_variables: Vec<VariablePosition>,
    selected_variables: Vec<VariablePosition>,
}

impl UnsortedJoinProgram {
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
pub struct AssignmentProgram {
    assign_instruction: ExpressionBinding<VariablePosition>,
    check_instructions: Vec<ConstraintInstruction<VariablePosition>>,
    unbound: [VariablePosition; 1],
}

impl AssignmentProgram {
    fn new_variables(&self) -> &[VariablePosition] {
        &self.unbound
    }

    fn output_width(&self) -> u32 {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct CheckProgram {
    pub check_instructions: Vec<CheckInstruction<VariablePosition>>,
}

impl CheckProgram {
    pub fn new(check_instructions: Vec<CheckInstruction<VariablePosition>>) -> Self {
        Self { check_instructions }
    }
}

#[derive(Clone, Debug)]
pub struct DisjunctionProgram {
    pub disjunction: Vec<MatchProgram>,
}

#[derive(Clone, Debug)]
pub struct NegationProgram {
    pub negation: MatchProgram,
}

#[derive(Clone, Debug)]
pub struct OptionalProgram {
    pub optional: MatchProgram,
}

pub trait InstructionAPI<ID: IrID> {
    fn constraint(&self) -> Constraint<ID>;
}
