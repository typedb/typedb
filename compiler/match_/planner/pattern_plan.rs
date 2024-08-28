/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    mem,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{
        constraint::{Comparator, Constraint, ExpressionBinding},
        variable_category::VariableCategory,
    },
    program::block::{BlockContext, FunctionalBlock},
};
use itertools::Itertools;

use crate::{
    expression::compiled_expression::CompiledExpression,
    match_::{
        inference::type_annotations::TypeAnnotations,
        instructions::{
            CheckInstruction, ConstraintInstruction, HasInstruction, HasReverseInstruction, Inputs,
            IsaReverseInstruction, LinksInstruction, LinksReverseInstruction,
        },
        planner::vertex::{Costed, HasPlanner, LinksPlanner, PlannerVertex, ThingPlanner, ValuePlanner, VertexCost},
    },
};

pub struct PatternPlan {
    pub(crate) programs: Vec<Program>,
    pub(crate) context: BlockContext,
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

impl PatternPlan {
    pub fn new(programs: Vec<Program>, context: BlockContext) -> Self {
        Self { programs, context }
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
        let context = block.context();
        let mut elements = Vec::new();
        let variable_index = register_variables(context, &mut elements, type_annotations, statistics);
        let (index_to_constraint, variable_isa, adjacency) =
            register_constraints(conjunction, &variable_index, &mut elements, type_annotations, statistics);

        let ordering = initialise_plan_greedy(&elements, &adjacency);

        let index_to_variable: HashMap<_, _> =
            variable_index.iter().map(|(&variable, &index)| (index, variable)).collect();

        let mut programs = Vec::with_capacity(index_to_constraint.len());
        let mut producers = HashMap::with_capacity(variable_index.len());
        let mut outputs = Vec::with_capacity(variable_index.len());

        let mut program_instructions = Vec::new();
        let mut sort_variable = None;

        for (i, &index) in ordering.iter().enumerate() {
            let adjacent = match adjacency.get(&index) {
                Some(adj) => adj,
                None => &HashSet::new(),
            };
            if let Some(&var) = index_to_variable.get(&index) {
                if let PlannerVertex::Thing(_) = &elements[index] {
                    let needs_isa = !adjacent
                        .iter()
                        .filter(|&&adj| elements[adj].is_iterator())
                        .any(|adj| ordering[..i].contains(adj));

                    if needs_isa {
                        if !program_instructions.is_empty() {
                            programs.push(Program::Intersection(IntersectionProgram::new(
                                sort_variable.take().unwrap(),
                                mem::take(&mut program_instructions),
                                &outputs,
                            )));
                        }

                        let isa = &variable_isa[&var];
                        outputs.push(var);
                        sort_variable = Some(var);
                        producers.insert(var, program_instructions.len());
                        program_instructions.push(ConstraintInstruction::IsaReverse(IsaReverseInstruction::new(
                            isa.clone(),
                            Inputs::None([]),
                            type_annotations,
                            Vec::new(),
                        )));
                    }
                }
            } else {
                let inputs = adjacent
                    .iter()
                    .filter(|&adj| ordering[..i].contains(adj))
                    .map(|adj| index_to_variable[adj])
                    .collect::<HashSet<_>>();

                let constraint = index_to_constraint[&index];
                match constraint {
                    Constraint::RoleName(_) | Constraint::Label(_) | Constraint::Sub(_) | Constraint::Isa(_) => {
                        todo!("type constraint")
                    }

                    Constraint::Links(links) => {
                        let relation = links.relation();
                        let player = links.player();
                        let role_type = links.role_type();

                        if inputs.len() >= 2 {
                            todo!("fully bound links");
                            continue;
                        }

                        if !inputs.is_empty() {
                            producers.clear();
                            programs.push(Program::Intersection(IntersectionProgram::new(
                                sort_variable.take().unwrap(),
                                mem::take(&mut program_instructions),
                                &outputs,
                            )));
                        }

                        for var in &[relation, player, role_type] {
                            if !inputs.contains(var) {
                                outputs.push(*var)
                            }
                        }

                        let planner = elements[index].as_links().unwrap();

                        let instruction = if inputs.contains(&relation) {
                            ConstraintInstruction::Links(LinksInstruction::new(
                                links.clone(),
                                Inputs::Single([relation]),
                                type_annotations,
                            ))
                        } else if inputs.contains(&player) {
                            ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                                links.clone(),
                                Inputs::Single([player]),
                                type_annotations,
                            ))
                        } else if planner.unbound_is_forward {
                            ConstraintInstruction::Links(LinksInstruction::new(
                                links.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        } else {
                            ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                                links.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        };
                        program_instructions.push(instruction);

                        if sort_variable.is_none() {
                            sort_variable =
                                if inputs.is_empty() && planner.unbound_is_forward || inputs.contains(&player) {
                                    Some(relation)
                                } else {
                                    Some(player)
                                };
                        }
                    }

                    Constraint::Has(has) => {
                        let owner = has.owner();
                        let attribute = has.attribute();

                        if inputs.len() == 2 {
                            let owner_producer = producers.get(&owner).expect("bound owner must have been produced");
                            let attribute_producer =
                                producers.get(&attribute).expect("bound attribute must have been produced");
                            let latest = usize::max(*owner_producer, *attribute_producer);
                            program_instructions[latest].add_check(CheckInstruction::Has { owner, attribute });
                            continue;
                        }

                        if !inputs.is_empty() {
                            producers.clear();
                            programs.push(Program::Intersection(IntersectionProgram::new(
                                sort_variable.take().unwrap(),
                                mem::take(&mut program_instructions),
                                &outputs,
                            )));
                        }

                        let planner = elements[index].as_has().unwrap();
                        for var in &[owner, attribute] {
                            if !inputs.contains(var) {
                                outputs.push(*var)
                            }
                        }

                        let instruction = if inputs.contains(&owner) {
                            producers.insert(attribute, program_instructions.len());
                            ConstraintInstruction::Has(HasInstruction::new(
                                has.clone(),
                                Inputs::Single([owner]),
                                type_annotations,
                            ))
                        } else if inputs.contains(&attribute) {
                            producers.insert(owner, program_instructions.len());
                            ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                                has.clone(),
                                Inputs::Single([attribute]),
                                type_annotations,
                            ))
                        } else if planner.unbound_is_forward {
                            producers.insert(owner, program_instructions.len());
                            producers.insert(attribute, program_instructions.len());
                            ConstraintInstruction::Has(HasInstruction::new(
                                has.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        } else {
                            producers.insert(owner, program_instructions.len());
                            producers.insert(attribute, program_instructions.len());
                            ConstraintInstruction::HasReverse(HasReverseInstruction::new(
                                has.clone(),
                                Inputs::None([]),
                                type_annotations,
                            ))
                        };
                        program_instructions.push(instruction);

                        if sort_variable.is_none() {
                            sort_variable =
                                if inputs.is_empty() && planner.unbound_is_forward || inputs.contains(&attribute) {
                                    Some(owner)
                                } else {
                                    Some(attribute)
                                };
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
        programs.push(Program::Intersection(IntersectionProgram::new(
            sort_variable.unwrap(),
            program_instructions,
            &outputs,
        )));
        Self { programs, context: context.clone() }
    }

    pub fn programs(&self) -> &[Program] {
        &self.programs
    }

    pub(crate) fn into_programs(self) -> impl Iterator<Item = Program> {
        self.programs.into_iter()
    }

    pub fn outputs(&self) -> &[Variable] {
        self.programs.last().unwrap().selected_variables()
    }

    pub(crate) fn into_steps(self) -> impl Iterator<Item = Step> {
        self.steps.into_iter()
    }

    pub fn outputs(&self) -> &[Variable] {
        self.steps.last().unwrap().selected_variables()
    }

=======
    pub fn context(&self) -> &BlockContext {
        &self.context
    }
}

fn register_variables(
    context: &BlockContext,
    elements: &mut Vec<PlannerVertex>,
    type_annotations: &TypeAnnotations,
    statistics: &Statistics,
) -> HashMap<Variable, usize> {
    context
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
        .collect()
}

fn register_constraints<'a>(
    conjunction: &'a ir::pattern::conjunction::Conjunction,
    variable_index: &HashMap<Variable, usize>,
    elements: &mut Vec<PlannerVertex>,
    type_annotations: &TypeAnnotations,
    statistics: &Statistics,
) -> (
    HashMap<usize, &'a Constraint<Variable>>,
    HashMap<Variable, ir::pattern::constraint::Isa<Variable>>,
    HashMap<usize, HashSet<usize>>,
) {
    let mut index_to_constraint = HashMap::new();
    let mut variable_isa = HashMap::new();
    let mut adjacency: HashMap<usize, HashSet<usize>> = HashMap::new();

    for constraint in conjunction.constraints() {
        match constraint {
            Constraint::RoleName(_) | Constraint::Label(_) | Constraint::Sub(_) => (), // ignore for now

            Constraint::Owns(_) => todo!("owns"),
            Constraint::Relates(_) => todo!("relates"),
            Constraint::Plays(_) => todo!("plays"),

            Constraint::Isa(isa) => {
                variable_isa.insert(isa.thing(), isa.clone());
            }

            Constraint::Links(links) => {
                let planner = LinksPlanner::from_constraint(links, variable_index, type_annotations, statistics);

                let planner_index = elements.len();

                index_to_constraint.insert(planner_index, constraint);

                adjacency.entry(planner_index).or_default().extend([planner.relation, planner.player, planner.role]);

                adjacency.entry(planner.relation).or_default().insert(planner_index);
                adjacency.entry(planner.player).or_default().insert(planner_index);
                adjacency.entry(planner.role).or_default().insert(planner_index);

                elements.push(PlannerVertex::Links(planner));
            }

            Constraint::Has(has) => {
                let planner = HasPlanner::from_constraint(has, variable_index, type_annotations, statistics);

                let planner_index = elements.len();

                index_to_constraint.insert(planner_index, constraint);

                adjacency.entry(planner_index).or_default().extend([planner.owner, planner.attribute]);

                adjacency.entry(planner.owner).or_default().insert(planner_index);
                adjacency.entry(planner.attribute).or_default().insert(planner_index);

                elements.push(PlannerVertex::Has(planner));
            }

            Constraint::ExpressionBinding(expression) => {
                let lhs = variable_index[&expression.left()];
                if expression.expression().is_constant() {
                    if matches!(elements[lhs], PlannerVertex::Value(_)) {
                        elements[lhs] = PlannerVertex::Constant
                    } else {
                        todo!("non-value var assignment?")
                    }
                } else {
                    let planner_index = elements.len();
                    adjacency.entry(lhs).or_default().insert(planner_index);
                    elements.push(PlannerVertex::Expression(todo!("expression = {expression:?}")));
                }
            }

            Constraint::FunctionCallBinding(_) => todo!("function call"),

            Constraint::Comparison(comparison) => {
                let lhs = variable_index[&comparison.lhs()];
                let rhs = variable_index[&comparison.rhs()];
                adjacency.entry(lhs).or_default().insert(rhs);
                adjacency.entry(rhs).or_default().insert(lhs);
                match comparison.comparator() {
                    Comparator::Equal => {
                        elements[lhs].add_equal(rhs);
                        elements[rhs].add_equal(lhs);
                    }
                    Comparator::Less | Comparator::LessOrEqual => {
                        elements[lhs].add_upper_bound(rhs);
                        elements[rhs].add_lower_bound(lhs);
                    }
                    Comparator::Greater | Comparator::GreaterOrEqual => {
                        elements[lhs].add_lower_bound(rhs);
                        elements[rhs].add_upper_bound(lhs);
                    }
                    Comparator::Like => todo!("like operator"),
                    Comparator::Cointains => todo!("contains operator"),
                }
            }
        }
    }
    (index_to_constraint, variable_isa, adjacency)
}

fn initialise_plan_greedy(elements: &[PlannerVertex], adjacency: &HashMap<usize, HashSet<usize>>) -> Vec<usize> {
    let mut open_set: HashSet<usize> = (0..elements.len()).collect();
    let mut ordering = Vec::with_capacity(elements.len());
    while !open_set.is_empty() {
        let (next, _cost) = open_set
            .iter()
            .filter(|&&elem| elements[elem].is_valid(&ordering))
            .map(|&elem| (elem, calculate_marginal_cost(elements, adjacency, &ordering, elem)))
            .min_by(|(_, lhs_cost), (_, rhs_cost)| lhs_cost.total_cmp(rhs_cost))
            .unwrap();
        ordering.push(next);
        open_set.remove(&next);
    }
    ordering
}

fn calculate_marginal_cost(
    elements: &[PlannerVertex],
    adjacency: &HashMap<usize, HashSet<usize>>,
    prefix: &[usize],
    next: usize,
) -> f64 {
    assert!(!prefix.contains(&next));
    let adjacent = adjacency.get(&next);
    let preceding = adjacent.into_iter().flatten().filter(|adj| prefix.contains(adj)).copied().collect_vec();
    let planner_vertex = &elements[next];
    let VertexCost { per_input, per_output, branching_factor } = planner_vertex.cost(&preceding, elements);
    per_input + branching_factor * per_output
}

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

pub struct DisjunctionProgram {
    pub disjunction: Vec<PatternPlan>,
}

pub struct NegationProgram {
    pub negation: PatternPlan,
}

pub struct OptionalProgram {
    pub optional: PatternPlan,
}

pub trait InstructionAPI {
    fn constraint(&self) -> Constraint<Variable>;
}
