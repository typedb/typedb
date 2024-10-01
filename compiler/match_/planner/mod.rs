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
    mem,
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::program::{block::FunctionalBlock, VariableRegistry};
use itertools::Itertools;

use crate::{
    expression::compiled_expression::CompiledExpression,
    match_::{
        inference::type_annotations::TypeAnnotations,
        instructions::ConstraintInstruction,
        planner::{
            pattern_plan::{IntersectionProgram, MatchProgram, Program},
            plan::plan_conjunction,
        },
    },
    VariablePosition,
};

pub fn compile(
    block: &FunctionalBlock,
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: Arc<VariableRegistry>,
    expressions: &HashMap<Variable, CompiledExpression>,
    statistics: &Statistics,
) -> MatchProgram {
    assert!(block.modifiers().is_empty(), "TODO: modifiers in a FunctionalBlock");
    let conjunction = block.conjunction();
    let scope_context = block.scope_context();
    debug_assert!(conjunction.captured_variables(scope_context).all(|var| input_variables.contains_key(&var)));

    plan_conjunction(
        conjunction,
        scope_context,
        input_variables,
        type_annotations,
        variable_registry,
        expressions,
        statistics,
    )
    .lower()
}

// *** //

#[derive(Debug, Default)]
struct ProgramBuilder {
    sort_variable: Option<Variable>,
    instructions: Vec<ConstraintInstruction<VariablePosition>>,
    output_width: Option<u32>,
}

impl ProgramBuilder {
    fn finish(self, outputs: &HashMap<VariablePosition, Variable>) -> Program {
        let sort_variable = *outputs.iter().find(|(_, &item)| Some(item) == self.sort_variable).unwrap().0;
        Program::Intersection(IntersectionProgram::new(
            sort_variable,
            self.instructions,
            &(0..self.output_width.unwrap()).map(VariablePosition::new).collect_vec(),
            self.output_width.unwrap(),
        ))
    }
}

struct MatchProgramBuilder {
    programs: Vec<ProgramBuilder>,
    current: ProgramBuilder,
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
        Self { programs: Vec::new(), current: ProgramBuilder::default(), outputs, index, next_output }
    }

    fn get_program_mut(&mut self, program: usize) -> &mut ProgramBuilder {
        self.programs.get_mut(program).unwrap_or(&mut self.current)
    }

    fn push_instruction(
        &mut self,
        sort_variable: Variable,
        instruction: ConstraintInstruction<Variable>,
        outputs: impl IntoIterator<Item = Variable>,
    ) -> (usize, usize) {
        if self.current.sort_variable != Some(sort_variable) {
            self.finish_one();
        }
        for var in outputs {
            self.register_output(var);
        }
        self.current.sort_variable = Some(sort_variable);
        self.current.instructions.push(instruction.map(&self.index));
        (self.programs.len(), self.current.instructions.len() - 1)
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
        if !self.current.instructions.is_empty() {
            self.current.output_width = Some(self.next_output.position);
            self.programs.push(mem::take(&mut self.current));
        }
    }

    fn finish(mut self) -> Vec<Program> {
        self.finish_one();
        self.programs.into_iter().map(|builder| builder.finish(&self.outputs)).collect()
    }
}
