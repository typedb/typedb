/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use error::typedb_error;
use ir::{
    pattern::{constraint::ExpressionBinding, BranchID, Vertex},
    pipeline::{block::Block, function_signature::FunctionID, VariableRegistry},
};
use itertools::Itertools;
use tracing::{debug, trace};
use ir::pattern::variable_category::VariableOptionality;
use crate::{
    annotation::{expression::compiled_expression::ExecutableExpression, type_annotations::BlockAnnotations},
    executable::{
        function::FunctionCallCostProvider,
        match_::{
            instructions::{CheckInstruction, ConstraintInstruction},
            planner::{
                conjunction_executable::{
                    AssignmentStep, CheckStep, DisjunctionStep, ExecutionStep, FunctionCallStep, IntersectionStep,
                    ConjunctionExecutable, NegationStep,
                },
                plan::{plan_conjunction, PlannerStatistics, QueryPlanningError},
            },
        },
        next_executable_id,
    },
    ExecutorVariable, VariablePosition,
};
use crate::executable::match_::planner::conjunction_executable::OptionalStep;

pub mod conjunction_executable;
pub mod plan;
pub(crate) mod vertex;

typedb_error! {
    pub MatchCompilationError(component = "Match compiler", prefix = "MCP") {
        PlanningError(1, "Error during planning of match stage.", typedb_source: QueryPlanningError),
    }
}

pub fn compile(
    block: &Block,
    stage_input_annotations: &BTreeMap<Vertex<Variable>, Arc<BTreeSet<answer::Type>>>,
    stage_input_positions: &HashMap<Variable, VariablePosition>,
    selected_variables: HashSet<Variable>,
    type_annotations: &BlockAnnotations,
    variable_registry: &VariableRegistry,
    expressions: &HashMap<ExpressionBinding<Variable>, ExecutableExpression<Variable>>,
    statistics: &Statistics,
    call_cost_provider: &impl FunctionCallCostProvider,
) -> Result<ConjunctionExecutable, MatchCompilationError> {
    let conjunction = block.conjunction();
    let block_context = block.block_context();

    debug!("Planning conjunction:\n{conjunction}");

    let assigned_identities =
        stage_input_positions.iter().map(|(&var, &position)| (var, ExecutorVariable::RowPosition(position))).collect();

    let plan = plan_conjunction(
        conjunction,
        block_context,
        stage_input_positions,
        type_annotations,
        variable_registry,
        expressions,
        statistics,
        call_cost_provider,
    )
    .map_err(|source| MatchCompilationError::PlanningError { typedb_source: source })?
    .lower(
        stage_input_annotations,
        stage_input_positions.keys().copied(),
        selected_variables,
        &assigned_identities,
        variable_registry,
        None,
    )
    .map_err(|source| MatchCompilationError::PlanningError { typedb_source: source })?
    .finish(variable_registry);

    trace!("Finished planning conjunction:\n{conjunction}");
    debug!("Lowered plan:\n{plan}");

    Ok(plan)
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
    negation: ConjunctionExecutableBuilder,
}

impl NegationBuilder {
    fn new(negation: ConjunctionExecutableBuilder) -> Self {
        Self { negation }
    }
}

#[derive(Debug)]
struct OptionalBuilder {
    optional: ConjunctionExecutableBuilder,
}

impl OptionalBuilder {
    fn new(optional: ConjunctionExecutableBuilder) -> Self {
        Self { optional }
    }

    fn branch_id(&self) -> BranchID {
        self.optional.branch_id.expect("Optionals must be assigned a branch ID")
    }
}

#[derive(Debug)]
struct DisjunctionBuilder {
    branch_ids: Vec<BranchID>,
    branches: Vec<ConjunctionExecutableBuilder>,
}

impl DisjunctionBuilder {
    fn new(branch_ids: Vec<BranchID>, branches: Vec<ConjunctionExecutableBuilder>) -> Self {
        Self { branch_ids, branches }
    }
}

#[derive(Debug)]
struct FunctionCallBuilder {
    function_id: FunctionID,
    arguments: Vec<VariablePosition>,
    assigned: Vec<Option<VariablePosition>>,
    output_width: u32,
}

#[derive(Debug)]
// TODO rename
enum StepInstructionsBuilder {
    Intersection(IntersectionBuilder),
    Check(CheckBuilder),
    Negation(NegationBuilder),
    Disjunction(DisjunctionBuilder),
    Optional(OptionalBuilder),
    Expression(ExpressionBuilder),
    FunctionCall(FunctionCallBuilder),
}

impl StepInstructionsBuilder {
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
        variable_registry: &VariableRegistry,
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
            StepInstructionsBuilder::Optional(builder) => {
                let branch_id = builder.branch_id();
                let OptionalBuilder { optional } = builder;
                ExecutionStep::Optional(
                    OptionalStep::new(optional.finish(variable_registry), selected_variables, output_width, branch_id)
                )
            }
            StepInstructionsBuilder::Disjunction(DisjunctionBuilder { branch_ids, branches }) => {
                ExecutionStep::Disjunction(DisjunctionStep::new(
                    branch_ids,
                    branches.into_iter().map(|builder| builder.finish(variable_registry)).collect(),
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
            }) => ExecutionStep::FunctionCall(FunctionCallStep {
                function_id,
                arguments,
                assigned,
                selected_variables,
                output_width,
            }),
        }
    }
}

#[derive(Debug)]
struct ConjunctionExecutableBuilder {
    selected_variables: HashSet<Variable>,
    input_variables: Vec<Variable>,
    constraint_variables: HashSet<Variable>,

    current_outputs: HashSet<Variable>,
    produced_so_far: HashSet<Variable>,

    steps: Vec<StepBuilder>,
    current: Option<Box<StepBuilder>>,

    reverse_index: HashMap<ExecutorVariable, Variable>,
    index: HashMap<Variable, ExecutorVariable>,
    next_output: VariablePosition,

    planner_statistics: PlannerStatistics,
    branch_id: Option<BranchID>,
}

impl ConjunctionExecutableBuilder {
    fn new(
        branch_id: Option<BranchID>,
        assigned_positions: &HashMap<Variable, ExecutorVariable>,
        selected_variables: HashSet<Variable>,
        input_variables: Vec<Variable>,
        constraint_variables: HashSet<Variable>,
        planner_statistics: PlannerStatistics,
    ) -> Self {
        let index = assigned_positions.clone();
        let produced_so_far = HashSet::from_iter(input_variables.iter().copied());
        let current_outputs = produced_so_far.clone();
        let reverse_index = index.iter().map(|(&var, &pos)| (pos, var)).collect();
        let next_position = assigned_positions
            .values()
            .filter_map(ExecutorVariable::as_position)
            .max()
            .map(|pos| pos.position + 1)
            .unwrap_or(0);
        let next_output = VariablePosition::new(next_position);
        Self {
            branch_id,
            selected_variables,
            input_variables,
            constraint_variables,
            current_outputs,
            produced_so_far,
            steps: Vec::new(),
            current: None,
            reverse_index,
            index,
            next_output,
            planner_statistics,
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
                selected_variables: Vec::from_iter(self.current_outputs.iter().copied()),
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
        // if it is a comparison or IID (TODO) we can inline the check into previous instructions
        if self.inline_as_optimisation(variables, &check) {
            return;
        }

        if self.current.as_ref().is_some_and(|builder| !builder.builder.is_check()) {
            self.finish_one();
        }

        if self.current.is_none() {
            self.current = Some(Box::new(StepBuilder {
                selected_variables: Vec::from_iter(self.current_outputs.iter().copied()),
                builder: StepInstructionsBuilder::Check(CheckBuilder::default()),
            }))
        }
        let current = self.current.as_mut().unwrap().builder.as_check_mut().unwrap();
        current.instructions.push(check);
    }

    /// inject the check as an optimisation into previously built steps
    fn inline_as_optimisation(&mut self, variables: &[Variable], check: &CheckInstruction<ExecutorVariable>) -> bool {
        if !matches!(check, CheckInstruction::Comparison { .. } | CheckInstruction::Iid { .. }) {
            // TODO: inject IID check as well
            return false;
        }

        let mut inlined = false;
        let mut added_to_current = false;
        let steps_count = self.steps.len();
        for (i, step) in self.steps.iter_mut().chain(self.current.as_mut().map(|box_| box_.as_mut())).enumerate() {
            // TODO: we may be able to inject into non-intersection steps as well? For now, we know intersection steps are always sorted
            if let StepInstructionsBuilder::Intersection(intersection) = &mut step.builder {
                let mut is_added = false;
                for instruction in intersection.instructions.iter_mut() {
                    // if any check variable is produced and all other variables are available
                    let any_produced = variables.iter().any(|var| instruction.is_new_variable(self.index[var]));
                    let all_available = variables.iter().all(|var| {
                        instruction.is_new_variable(self.index[var]) || instruction.is_input_variable(self.index[var])
                    });
                    if any_produced && all_available {
                        instruction.add_check(check.clone());
                        is_added = true;
                    }
                }
                inlined |= is_added;
                if is_added && i == steps_count {
                    added_to_current = true;
                }
            }
        }
        if added_to_current {
            self.current.as_mut().unwrap().selected_variables = Vec::from_iter(self.current_outputs.iter().copied());
        }
        inlined
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
        step.selected_variables = Vec::from_iter(self.current_outputs.iter().copied());

        self.steps.push(step);
    }

    fn row_variables(&self) -> &[Variable] {
        if let Some(current) = &self.current {
            &current.selected_variables
        } else if let Some(last) = self.steps.last() {
            &last.selected_variables
        } else {
            &self.input_variables
        }
    }

    fn position_mapping(&self) -> &HashMap<Variable, ExecutorVariable> {
        &self.index
    }

    fn position(&self, var: Variable) -> ExecutorVariable {
        self.index[&var]
    }

    fn register_output(&mut self, var: Variable) {
        self.current_outputs.insert(var);
        if let hash_map::Entry::Vacant(entry) = self.index.entry(var) {
            entry.insert(ExecutorVariable::RowPosition(self.next_output));
            self.reverse_index.insert(ExecutorVariable::RowPosition(self.next_output), var);
            self.next_output.position += 1;
        }
    }

    fn register_internal(&mut self, var: Variable) {
        if let hash_map::Entry::Vacant(entry) = self.index.entry(var) {
            entry.insert(ExecutorVariable::Internal(var));
            self.reverse_index.insert(ExecutorVariable::new_internal(var), var);
        }
    }

    fn remove_output(&mut self, var: Variable) {
        if !self.selected_variables.contains(&var) {
            self.current_outputs.remove(&var);
        }
    }

    fn finish_one(&mut self) {
        if let Some(mut current) = self.current.take() {
            current.selected_variables = Vec::from_iter(self.current_outputs.iter().copied());
            self.steps.push(*current);
        }
    }

    fn finish(mut self, variable_registry: &VariableRegistry) -> ConjunctionExecutable {
        self.finish_one();
        let named_variables = self
            .index
            .iter()
            .filter_map(|(var, &pos)| variable_registry.variable_names().get(var).and(Some(pos)))
            .collect();
        let mut steps = Vec::with_capacity(self.steps.len() + 1);

        let optional_inputs_in_constraints = self.optional_inputs_in_constraints(variable_registry);
        if !optional_inputs_in_constraints.is_empty() {
            let mut builder = StepBuilder {
                selected_variables: Vec::from_iter(self.current_outputs.iter().copied()),
                builder: StepInstructionsBuilder::Check(CheckBuilder::default()),
            };
            builder.builder.as_check_mut().unwrap().instructions.push(
                CheckInstruction::NotNone { variables: optional_inputs_in_constraints },
            );
            steps.push(builder.finish(&self.index, &named_variables, variable_registry));
        }

        steps.extend(self
            .steps
            .into_iter()
            .map(|builder| builder.finish(&self.index, &named_variables, variable_registry))
        );
        ConjunctionExecutable::new(
            next_executable_id(),
            steps,
            self.index.into_iter().filter_map(|(var, id)| Some((var, id.as_position()?))).collect(),
            self.reverse_index,
            self.planner_statistics,
        )
    }

    fn optional_inputs_in_constraints(&self, variable_registry: &VariableRegistry) -> Vec<ExecutorVariable> {
        self.input_variables
            .iter()
            .filter(|&var| variable_registry
                .get_variable_optionality(*var)
                .is_some_and(|optionality| {
                    matches!(optionality, VariableOptionality::Optional) &&
                        self.constraint_variables.contains(var)
                }))
            .map(|optional_var| self.index[optional_var])
            .collect_vec()
    }
}
