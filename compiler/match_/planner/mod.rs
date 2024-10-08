/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * The planner should take a Program, and produce a Plan.
 *
 * A Plan should have an order over the Variables in a Pattern's constraint, for each Functional Block.
 *
 * We may need to be able to indicate which constraints are 'Seekable (+ ordered)' and therefore can be utilised in an intersection.
 * For example, function stream outputs are probably not seekable since we won't have traversals be seekable (at least to start!).
 */

pub mod function_plan;
pub mod pattern_plan;
mod plan;
pub mod program_plan;
mod vertex;

use std::{
    collections::{hash_map, HashMap},
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::program::{block::Block, VariableRegistry};
use itertools::Itertools;

use crate::{
    expression::compiled_expression::CompiledExpression,
    annotation::type_annotations::TypeAnnotations,
    match_::{
        instructions::{CheckInstruction, ConstraintInstruction},
        planner::{
            pattern_plan::{CheckProgram, IntersectionProgram, MatchProgram, NegationProgram, Program},
            plan::plan_conjunction,
        },
    },
    VariablePosition,
};

pub fn compile(
    block: &Block,
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: Arc<VariableRegistry>,
    expressions: &HashMap<Variable, CompiledExpression>,
    statistics: &Statistics,
) -> MatchProgram {
    let conjunction = block.conjunction();
    let scope_context = block.scope_context();
    debug_assert!(conjunction.captured_variables(scope_context).all(|var| input_variables.contains_key(&var)));

    plan_conjunction(
        conjunction,
        scope_context,
        input_variables,
        type_annotations,
        &variable_registry,
        expressions,
        statistics,
    )
    .lower(input_variables, variable_registry)
}

#[derive(Debug, Default)]
struct IntersectionBuilder {
    sort_variable: Option<Variable>,
    instructions: Vec<ConstraintInstruction<VariablePosition>>,
    output_width: Option<u32>,
}

#[derive(Debug, Default)]
struct CheckBuilder {
    instructions: Vec<CheckInstruction<VariablePosition>>,
}

#[derive(Debug)]
struct NegationBuilder {
    negation: MatchProgram,
}

#[derive(Debug)]
enum ProgramBuilder {
    Intersection(IntersectionBuilder),
    Check(CheckBuilder),
    Negation(NegationBuilder),
}

impl ProgramBuilder {
    fn finish(self, outputs: &HashMap<VariablePosition, Variable>) -> Program {
        match self {
            Self::Intersection(IntersectionBuilder { sort_variable, instructions, output_width }) => {
                let sort_variable = *outputs.iter().find(|(_, &item)| Some(item) == sort_variable).unwrap().0;
                Program::Intersection(IntersectionProgram::new(
                    sort_variable,
                    instructions,
                    &(0..output_width.unwrap()).map(VariablePosition::new).collect_vec(),
                    output_width.unwrap(),
                ))
            }
            Self::Check(CheckBuilder { instructions }) => Program::Check(CheckProgram::new(instructions)),
            Self::Negation(NegationBuilder { negation }) => {
                Program::Negation(pattern_plan::NegationProgram { negation })
            }
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

    /// Returns `true` if the program builder is [`Intersection`].
    ///
    /// [`Intersection`]: ProgramBuilder::Intersection
    #[must_use]
    fn is_intersection(&self) -> bool {
        matches!(self, Self::Intersection(..))
    }

    /// Returns `true` if the program builder is [`Check`].
    ///
    /// [`Check`]: ProgramBuilder::Check
    #[must_use]
    fn is_check(&self) -> bool {
        matches!(self, Self::Check(..))
    }

    fn set_output_width(&mut self, position: u32) {
        match self {
            ProgramBuilder::Intersection(IntersectionBuilder { output_width, .. }) => *output_width = Some(position),
            ProgramBuilder::Check(_) => (),
            ProgramBuilder::Negation(_) => (),
        }
    }
}

struct MatchProgramBuilder {
    programs: Vec<ProgramBuilder>,
    current: Option<ProgramBuilder>,
    outputs: HashMap<VariablePosition, Variable>,
    index: HashMap<Variable, VariablePosition>,
    next_output: VariablePosition,
}

impl MatchProgramBuilder {
    fn with_inputs(input_variables: &HashMap<Variable, VariablePosition>) -> Self {
        let index = input_variables.clone();
        let outputs = index.iter().map(|(&var, &pos)| (pos, var)).collect();
        let next_position = input_variables.values().max().map(|&pos| pos.position + 1).unwrap_or_default();
        let next_output = VariablePosition::new(next_position);
        Self { programs: Vec::new(), current: None, outputs, index, next_output }
    }

    fn get_program_mut(&mut self, program: usize) -> Option<&mut ProgramBuilder> {
        self.programs.get_mut(program).or(self.current.as_mut())
    }

    fn push_instruction(
        &mut self,
        sort_variable: Variable,
        instruction: ConstraintInstruction<Variable>,
        outputs: impl IntoIterator<Item = Variable>,
    ) -> (usize, usize) {
        if let Some(ProgramBuilder::Intersection(intersection_builder)) = &self.current {
            if let Some(current_sort) = intersection_builder.sort_variable {
                if current_sort != sort_variable || instruction.is_input_variable(current_sort) {
                    self.finish_one();
                }
            }
        }
        if self.current.as_ref().is_some_and(|builder| !builder.is_intersection()) {
            self.finish_one();
        }
        for var in outputs {
            self.register_output(var);
        }
        if self.current.is_none() {
            self.current = Some(ProgramBuilder::Intersection(IntersectionBuilder::default()))
        }
        let current = self.current.as_mut().unwrap().as_intersection_mut().unwrap();
        current.sort_variable = Some(sort_variable);
        current.instructions.push(instruction.map(&self.index));
        (self.programs.len(), current.instructions.len() - 1)
    }

    fn push_check_instruction(&mut self, instruction: CheckInstruction<VariablePosition>) {
        if self.current.as_ref().is_some_and(|builder| !builder.is_check()) {
            self.finish_one();
        }
        if self.current.is_none() {
            self.current = Some(ProgramBuilder::Check(CheckBuilder::default()))
        }
        let current = self.current.as_mut().unwrap().as_check_mut().unwrap();
        current.instructions.push(instruction);
    }

    fn push_program(&mut self, variable_positions: &HashMap<Variable, VariablePosition>, program: ProgramBuilder) {
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
        self.programs.push(program);
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
            self.programs.push(current);
        }
    }

    fn finish(mut self, variable_registry: Arc<VariableRegistry>) -> MatchProgram {
        self.finish_one();
        let programs = self.programs.into_iter().map(|builder| builder.finish(&self.outputs)).collect();
        let variable_positions_index =
            self.outputs.iter().sorted_by_key(|(k, _)| k.as_usize()).map(|(_, &v)| v).collect();
        MatchProgram::new(programs, variable_registry, self.index.clone(), variable_positions_index)
    }
}
