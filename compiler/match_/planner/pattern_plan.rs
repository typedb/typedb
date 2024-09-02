/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, HashMap, HashSet},
    mem,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{
        constraint::{Comparator, Constraint, ExpressionBinding, Isa},
        variable_category::VariableCategory,
    },
    program::block::{FunctionalBlock, VariableRegistry},
};
use itertools::Itertools;

use crate::{
    expression::compiled_expression::CompiledExpression,
    match_::{
        inference::type_annotations::TypeAnnotations,
        instructions::{
            CheckInstruction, ConstraintInstruction, HasInstruction, HasReverseInstruction, Inputs,
            IsaReverseInstruction,
        },
        planner::vertex::{Costed, HasPlanner, LinksPlanner, PlannerVertex, ThingPlanner, ValuePlanner, VertexCost},
    },
};

#[derive(Debug)]
pub struct MatchProgram {
    pub(crate) programs: Vec<Program>,
    pub(crate) variable_registry: VariableRegistry,
}

/*
1. Named variables that are not returned or reused beyond a program can simply be counted, and not outputted
2. Anonymous variables that are not reused beyond a program can just be checked for a single answer

Planner outputs an ordering over variables, with directions over which edges should be traversed.
If we know this we can:
  1. group edges intersecting into the same variable as one Program.
  2. if the ordering implies it, we may need to perform Storage/Comparison checks, if the variables are visited disconnected and then joined
  3. some checks are fully bound, while others are not... when do we decide? What is a Check versus an Iterate instructions? Do we need to differentiate?
 */

#[derive(Debug)]
struct PlanBuilder {
    elements: Vec<PlannerVertex>,
    variable_index: HashMap<Variable, usize>,
    index_to_constraint: HashMap<usize, Constraint<Variable>>,
    adjacency: HashMap<usize, HashSet<usize>>,
    variable_isa: HashMap<Variable, Isa<Variable>>,
}

impl PlanBuilder {
    fn init(variable_registry: &VariableRegistry, type_annotations: &TypeAnnotations, statistics: &Statistics) -> Self {
        let mut elements = Vec::new();
        let variable_index = variable_registry
            .variable_categories()
            .filter_map(|(variable, category)| {
                match category {
                    VariableCategory::Type | VariableCategory::ThingType => None, // ignore for now
                    VariableCategory::RoleType => Some((variable, elements.len())),
                    VariableCategory::Thing | VariableCategory::Object | VariableCategory::Attribute => {
                        let planner = ThingPlanner::from_variable(variable, type_annotations, statistics);
                        let index = elements.len();
                        elements.push(PlannerVertex::Thing(planner));
                        Some((variable, index))
                    }
                    VariableCategory::Value => {
                        let planner = ValuePlanner::from_variable(variable);
                        let index = elements.len();
                        elements.push(PlannerVertex::Value(planner));
                        Some((variable, index))
                    }
                    | VariableCategory::ObjectList
                    | VariableCategory::ThingList
                    | VariableCategory::AttributeList
                    | VariableCategory::ValueList => todo!("list variable planning"),
                }
            })
            .collect();
        Self {
            elements,
            variable_index,
            index_to_constraint: HashMap::new(),
            adjacency: HashMap::new(),
            variable_isa: HashMap::new(),
        }
    }

    fn register_constraints(
        &mut self,
        conjunction: &ir::pattern::conjunction::Conjunction,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) {
        for constraint in conjunction.constraints() {
            let planner = match constraint {
                Constraint::RoleName(_) | Constraint::Label(_) | Constraint::Sub(_) => None, // ignore for now
                Constraint::Owns(_) => todo!("owns"),
                Constraint::Relates(_) => todo!("relates"),
                Constraint::Plays(_) => todo!("plays"),

                Constraint::Isa(isa) => {
                    self.variable_isa.insert(isa.thing(), isa.clone());
                    None
                }
                Constraint::Links(links) => {
                    let planner =
                        LinksPlanner::from_constraint(links, &self.variable_index, type_annotations, statistics);
                    self.elements.push(PlannerVertex::Links(planner));
                    self.elements.last()
                }
                Constraint::Has(has) => {
                    let planner = HasPlanner::from_constraint(has, &self.variable_index, type_annotations, statistics);
                    self.elements.push(PlannerVertex::Has(planner));
                    self.elements.last()
                }

                Constraint::ExpressionBinding(expression) => {
                    let lhs = self.variable_index[&expression.left()];
                    if expression.expression().is_constant() {
                        if matches!(self.elements[lhs], PlannerVertex::Value(_)) {
                            self.elements[lhs] = PlannerVertex::Constant
                        } else {
                            todo!("non-value var assignment?")
                        }
                        None
                    } else {
                        let planner_index = self.elements.len();
                        self.adjacency.entry(lhs).or_default().insert(planner_index);
                        self.elements.push(PlannerVertex::Expression(todo!("expression = {expression:?}")));
                        self.elements.last()
                    }
                }

                Constraint::FunctionCallBinding(_) => todo!("function call"),

                Constraint::Comparison(comparison) => {
                    let lhs = self.variable_index[&comparison.lhs()];
                    let rhs = self.variable_index[&comparison.rhs()];
                    self.adjacency.entry(lhs).or_default().insert(rhs);
                    self.adjacency.entry(rhs).or_default().insert(lhs);
                    match comparison.comparator() {
                        Comparator::Equal => {
                            self.elements[lhs].add_equal(rhs);
                            self.elements[rhs].add_equal(lhs);
                        }
                        Comparator::Less | Comparator::LessOrEqual => {
                            self.elements[lhs].add_upper_bound(rhs);
                            self.elements[rhs].add_lower_bound(lhs);
                        }
                        Comparator::Greater | Comparator::GreaterOrEqual => {
                            self.elements[lhs].add_lower_bound(rhs);
                            self.elements[rhs].add_upper_bound(lhs);
                        }
                        Comparator::Like => todo!("like operator"),
                        Comparator::Cointains => todo!("contains operator"),
                    }
                    None
                }
            };

            if let Some(planner) = planner {
                let planner_index = self.elements.len() - 1;
                self.index_to_constraint.insert(planner_index, constraint.clone());
                self.adjacency.entry(planner_index).or_default().extend(planner.variables());
                for v in planner.variables() {
                    self.adjacency.entry(v).or_default().insert(planner_index);
                }
            }
        }
    }

    fn initialise_greedy(&self) -> Vec<usize> {
        let mut open_set: HashSet<usize> = (0..self.elements.len()).collect();
        let mut ordering = Vec::with_capacity(self.elements.len());
        while !open_set.is_empty() {
            let (next, _cost) = open_set
                .iter()
                .filter(|&&elem| self.elements[elem].is_valid(&ordering))
                .map(|&elem| (elem, self.calculate_marginal_cost(&ordering, elem)))
                .min_by(|(_, lhs_cost), (_, rhs_cost)| lhs_cost.total_cmp(rhs_cost))
                .unwrap();
            ordering.push(next);
            open_set.remove(&next);
        }
        ordering
    }

    fn calculate_marginal_cost(&self, prefix: &[usize], next: usize) -> f64 {
        assert!(!prefix.contains(&next));
        let adjacent = self.adjacency.get(&next);
        let preceding = adjacent.into_iter().flatten().filter(|adj| prefix.contains(adj)).copied().collect_vec();
        let planner_vertex = &self.elements[next];
        let VertexCost { per_input, per_output, branching_factor } = planner_vertex.cost(&preceding, &self.elements);
        per_input + branching_factor * per_output
    }
}

impl MatchProgram {
    pub fn new(programs: Vec<Program>, context: VariableRegistry) -> Self {
        Self { programs, variable_registry: context }
    }

    pub fn from_block(
        block: &FunctionalBlock,
        type_annotations: &TypeAnnotations,
        variable_registry: &VariableRegistry,
        _expressions: &HashMap<Variable, CompiledExpression>,
        statistics: &Statistics,
    ) -> Self {
        assert!(block.modifiers().is_empty(), "TODO: modifiers in a FunctionalBlock");
        let conjunction = block.conjunction();
        assert!(conjunction.nested_patterns().is_empty(), "TODO: nested patterns in root conjunction");

        let mut plan_builder = PlanBuilder::init(variable_registry, type_annotations, statistics);
        plan_builder.register_constraints(conjunction, type_annotations, statistics);
        let ordering = plan_builder.initialise_greedy();

        let programs = lower_plan(&plan_builder, ordering, type_annotations);
        Self { programs, variable_registry: variable_registry.clone() }
    }

    pub fn programs(&self) -> &[Program] {
        &self.programs
    }

    pub fn outputs(&self) -> &[Variable] {
        self.programs.last().unwrap().selected_variables()
    }

    pub fn variable_registry(&self) -> &VariableRegistry {
        &self.variable_registry
    }
}

#[derive(Debug, Default)]
struct ProgramBuilder {
    sort_variable: Option<Variable>,
    instructions: Vec<ConstraintInstruction>,
    last_output: Option<usize>,
}

impl ProgramBuilder {
    fn finish(self, outputs: &[Variable]) -> Program {
        Program::Intersection(IntersectionProgram::new(
            self.sort_variable.unwrap(),
            self.instructions,
            &outputs[..self.last_output.unwrap()],
        ))
    }
}

#[derive(Default)]
struct MatchProgramBuilder {
    programs: Vec<ProgramBuilder>,
    current: ProgramBuilder,
    outputs: Vec<Variable>,
}

impl MatchProgramBuilder {
    fn get_program_mut(&mut self, program: usize) -> &mut ProgramBuilder {
        self.programs.get_mut(program).unwrap_or(&mut self.current)
    }

    fn push_instruction(&mut self, sort_variable: Variable, instruction: ConstraintInstruction) -> (usize, usize) {
        if self.current.sort_variable != Some(sort_variable) {
            self.finish_one();
        }
        self.current.sort_variable = Some(sort_variable);
        self.current.instructions.push(instruction);
        (self.programs.len(), self.current.instructions.len() - 1)
    }

    fn finish_one(&mut self) {
        if !self.current.instructions.is_empty() {
            self.current.last_output = Some(self.outputs.len());
            self.programs.push(mem::take(&mut self.current));
        }
    }

    fn finish(mut self) -> Vec<Program> {
        self.finish_one();
        self.programs.into_iter().map(|builder| builder.finish(&self.outputs)).collect()
    }
}

fn lower_plan(plan_builder: &PlanBuilder, ordering: Vec<usize>, type_annotations: &TypeAnnotations) -> Vec<Program> {
    let index_to_variable: HashMap<_, _> =
        plan_builder.variable_index.iter().map(|(&variable, &index)| (index, variable)).collect();

    let mut match_builder = MatchProgramBuilder::default();

    let mut producers = HashMap::with_capacity(plan_builder.variable_index.len());

    for (i, &index) in ordering.iter().enumerate() {
        let adjacent = match plan_builder.adjacency.get(&index) {
            Some(adj) => adj,
            None => &HashSet::new(),
        };
        if let Some(&var) = index_to_variable.get(&index) {
            if let PlannerVertex::Thing(_) = &plan_builder.elements[index] {
                if let hash_map::Entry::Vacant(entry) = producers.entry(var) {
                    let isa = &plan_builder.variable_isa[&var];
                    let instruction = ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
                        isa.clone(),
                        Inputs::None([]),
                        type_annotations,
                    ));
                    let producer_index = match_builder.push_instruction(var, instruction);
                    match_builder.outputs.push(var);
                    entry.insert(producer_index);
                }
            }
        } else {
            let inputs = adjacent
                .iter()
                .filter(|&adj| ordering[..i].contains(adj))
                .map(|adj| index_to_variable[adj])
                .collect::<HashSet<_>>();

            let constraint = &plan_builder.index_to_constraint[&index];
            if let Some(var) = &match_builder.current.sort_variable {
                if !constraint.ids().contains(var) {
                    match_builder.finish_one();
                }
            }

            match constraint {
                Constraint::RoleName(_) | Constraint::Label(_) | Constraint::Sub(_) | Constraint::Isa(_) => {
                    todo!("type constraint")
                }

                Constraint::Links(_links) => {
                    todo!()
                }

                Constraint::Has(has) => {
                    let owner = has.owner();
                    let attribute = has.attribute();

                    if inputs.len() == 2 {
                        let owner_producer = producers.get(&owner).expect("bound owner must have been produced");
                        let attribute_producer =
                            producers.get(&attribute).expect("bound attribute must have been produced");
                        let (program, instruction) = std::cmp::Ord::max(*owner_producer, *attribute_producer);
                        match_builder.get_program_mut(program).instructions[instruction]
                            .add_check(CheckInstruction::Has { owner, attribute });
                        continue;
                    }

                    let planner = plan_builder.elements[index].as_has().unwrap();
                    let sort_variable =
                        if inputs.is_empty() && planner.unbound_is_forward || inputs.contains(&attribute) {
                            owner
                        } else {
                            attribute
                        };

                    let has = has.clone();
                    let instruction = if inputs.contains(&owner) {
                        ConstraintInstruction::Has(HasInstruction::new(has, Inputs::Single([owner]), type_annotations))
                    } else if inputs.contains(&attribute) {
                        ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                            has,
                            Inputs::Single([attribute]),
                            type_annotations,
                        ))
                    } else if planner.unbound_is_forward {
                        ConstraintInstruction::Has(HasInstruction::new(has, Inputs::None([]), type_annotations))
                    } else {
                        ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                            has,
                            Inputs::None([]),
                            type_annotations,
                        ))
                    };

                    let producer_index = match_builder.push_instruction(sort_variable, instruction);
                    for &var in &[owner, attribute] {
                        if !inputs.contains(&var) {
                            match_builder.outputs.push(var);
                            producers.insert(var, producer_index);
                        }
                    }
                }

                Constraint::ExpressionBinding(_) => todo!("expression binding"),
                Constraint::FunctionCallBinding(_) => todo!("function call binding"),
                Constraint::Comparison(_) => todo!("comparison"),
                Constraint::Owns(_) => todo!("owns"),
                Constraint::Relates(_) => todo!("relates"),
                Constraint::Plays(_) => todo!("plays"),
            }
        }
    }
    match_builder.finish()
}

#[derive(Debug)]
pub enum Program {
    Intersection(IntersectionProgram),
    UnsortedJoin(UnsortedJoinProgram),
    Assignment(AssignmentProgram),
    Disjunction(DisjunctionProgram),
    Negation(NegationProgram),
    Optional(OptionalProgram),
}

impl Program {
    pub fn selected_variables(&self) -> &[Variable] {
        match self {
            Program::Intersection(program) => &program.selected_variables,
            Program::UnsortedJoin(program) => &program.selected_variables,
            Program::Assignment(_) => todo!(),
            Program::Disjunction(_) => todo!(),
            Program::Negation(_) => todo!(),
            Program::Optional(_) => todo!(),
        }
    }

    pub fn new_variables(&self) -> &[Variable] {
        match self {
            Program::Intersection(program) => program.new_variables(),
            Program::UnsortedJoin(program) => program.new_variables(),
            Program::Assignment(program) => program.new_variables(),
            Program::Disjunction(_) => todo!(),
            Program::Negation(_) => &[],
            Program::Optional(_) => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct IntersectionProgram {
    pub sort_variable: Variable,
    pub instructions: Vec<ConstraintInstruction>,
    new_variables: Vec<Variable>,
    input_variables: Vec<Variable>,
    pub selected_variables: Vec<Variable>,
}

impl IntersectionProgram {
    pub fn new(
        sort_variable: Variable,
        instructions: Vec<ConstraintInstruction>,
        selected_variables: &[Variable],
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
        Self {
            sort_variable,
            instructions,
            new_variables,
            input_variables,
            selected_variables: selected_variables.to_owned(),
        }
    }

    fn new_variables(&self) -> &[Variable] {
        &self.new_variables
    }
}

#[derive(Debug)]
pub struct UnsortedJoinProgram {
    pub iterate_instruction: ConstraintInstruction,
    pub check_instructions: Vec<ConstraintInstruction>,
    new_variables: Vec<Variable>,
    input_variables: Vec<Variable>,
    selected_variables: Vec<Variable>,
}

impl UnsortedJoinProgram {
    pub fn new(
        iterate_instruction: ConstraintInstruction,
        check_instructions: Vec<ConstraintInstruction>,
        selected_variables: &[Variable],
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

    fn new_variables(&self) -> &[Variable] {
        &self.new_variables
    }
}

#[derive(Debug)]
pub struct AssignmentProgram {
    assign_instruction: ExpressionBinding<Variable>,
    check_instructions: Vec<ConstraintInstruction>,
    unbound: [Variable; 1],
}

impl AssignmentProgram {
    fn new_variables(&self) -> &[Variable] {
        &self.unbound
    }
}

#[derive(Debug)]
pub struct DisjunctionProgram {
    pub disjunction: Vec<MatchProgram>,
}

#[derive(Debug)]
pub struct NegationProgram {
    pub negation: MatchProgram,
}

#[derive(Debug)]
pub struct OptionalProgram {
    pub optional: MatchProgram,
}

pub trait InstructionAPI {
    fn constraint(&self) -> Constraint<Variable>;
}
