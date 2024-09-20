/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ord,
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::{
    pattern::{
        constraint::{Comparator, Constraint, ExpressionBinding},
        variable_category::VariableCategory,
    },
    program::block::{FunctionalBlock, VariableRegistry},
};
use itertools::Itertools;
use ir::pattern::IrID;

use crate::{
    expression::compiled_expression::CompiledExpression,
    match_::{
        inference::type_annotations::TypeAnnotations,
        instructions::{
            thing::{
                HasInstruction, HasReverseInstruction, IsaInstruction, IsaReverseInstruction, LinksInstruction,
                LinksReverseInstruction,
            },
            type_::{
                OwnsInstruction, OwnsReverseInstruction, PlaysInstruction, PlaysReverseInstruction, RelatesInstruction,
                RelatesReverseInstruction, SubInstruction, SubReverseInstruction, TypeListInstruction,
            },
            CheckInstruction, ConstraintInstruction, Inputs,
        },
        planner::vertex::{
            ComparisonPlanner, Costed, Direction, HasPlanner, Input, IsaPlanner, LabelPlanner, LinksPlanner,
            OwnsPlanner, PlannerVertex, PlaysPlanner, RelatesPlanner, SubPlanner, ThingPlanner, TypePlanner,
            ValuePlanner, VertexCost,
        },
    },
    VariablePosition,
};

#[derive(Debug)]
pub struct MatchProgram {
    pub(crate) programs: Vec<Program>,
    pub(crate) variable_registry: Arc<VariableRegistry>,

    variable_positions: HashMap<Variable, VariablePosition>,
    variable_positions_index: Vec<Variable>,
}

impl MatchProgram {
    pub fn new(
        programs: Vec<Program>,
        variable_registry: VariableRegistry,
        variable_positions: HashMap<Variable, VariablePosition>,
        variable_positions_index: Vec<Variable>,
    ) -> Self {
        Self { programs, variable_registry: Arc::new(variable_registry), variable_positions, variable_positions_index }
    }

    pub fn compile(
        block: &FunctionalBlock,
        type_annotations: &TypeAnnotations,
        variable_registry: Arc<VariableRegistry>,
        _expressions: &HashMap<Variable, CompiledExpression>,
        statistics: &Statistics,
    ) -> Self {
        assert!(block.modifiers().is_empty(), "TODO: modifiers in a FunctionalBlock");
        let conjunction = block.conjunction();
        assert!(conjunction.nested_patterns().is_empty(), "TODO: nested patterns in root conjunction");

        let mut plan_builder = PlanBuilder::new(
            block.scope_context().referenced_variables(),
            &variable_registry,
            type_annotations,
            statistics,
        );
        plan_builder.register_constraints(conjunction);
        let ordering = plan_builder.initialise_greedy();

        let (variable_positions, index, programs) = lower_plan(&plan_builder, ordering, type_annotations);
        let variable_positions_index = index.into_iter().sorted_by_key(|(k, _)| k.as_usize()).map(|(_, v)| v).collect();
        Self { programs, variable_registry: variable_registry.clone(), variable_positions, variable_positions_index }
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
struct PlanBuilder<'a> {
    elements: Vec<PlannerVertex>,
    variable_index: HashMap<Variable, usize>,
    index_to_constraint: HashMap<usize, &'a Constraint<Variable>>,
    adjacency: HashMap<usize, HashSet<usize>>,
    type_annotations: &'a TypeAnnotations,
    statistics: &'a Statistics,
}

impl<'a> PlanBuilder<'a> {
    fn new(
        variables: impl Iterator<Item = Variable>,
        variable_registry: &VariableRegistry,
        type_annotations: &'a TypeAnnotations,
        statistics: &'a Statistics,
    ) -> Self {
        let mut elements = Vec::new();
        let variable_index = variables
            .map(|variable| {
                let category = variable_registry.get_variable_category(variable).unwrap();
                match category {
                    VariableCategory::Type | VariableCategory::ThingType | VariableCategory::RoleType => {
                        let planner = TypePlanner::from_variable(variable, type_annotations);
                        let index = elements.len();
                        elements.push(PlannerVertex::Type(planner));
                        (variable, index)
                    }
                    VariableCategory::Thing | VariableCategory::Object | VariableCategory::Attribute => {
                        let planner = ThingPlanner::from_variable(variable, type_annotations, statistics);
                        let index = elements.len();
                        elements.push(PlannerVertex::Thing(planner));
                        (variable, index)
                    }
                    VariableCategory::Value => {
                        let planner = ValuePlanner::from_variable(variable);
                        let index = elements.len();
                        elements.push(PlannerVertex::Value(planner));
                        (variable, index)
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
            type_annotations,
            statistics,
        }
    }

    fn register_constraints(&mut self, conjunction: &'a ir::pattern::conjunction::Conjunction) {
        let type_annotations = self.type_annotations;
        let statistics = self.statistics;

        for constraint in conjunction.constraints() {
            let planner = match constraint {
                Constraint::Kind(kind) => {
                    let planner = PlannerVertex::Label(LabelPlanner::from_kind_constraint(
                        kind,
                        &self.variable_index,
                        type_annotations,
                    ));
                    self.elements.push(planner);
                    self.elements.last()
                }
                Constraint::RoleName(role_name) => {
                    let planner = PlannerVertex::Label(LabelPlanner::from_role_name_constraint(
                        role_name,
                        &self.variable_index,
                        type_annotations,
                    ));
                    self.elements.push(planner);
                    self.elements.last()
                }
                Constraint::Label(label) => {
                    let planner = PlannerVertex::Label(LabelPlanner::from_label_constraint(
                        label,
                        &self.variable_index,
                        type_annotations,
                    ));
                    self.elements.push(planner);
                    self.elements.last()
                }
                Constraint::Sub(sub) => {
                    let planner = SubPlanner::from_constraint(sub, &self.variable_index, type_annotations);
                    self.elements.push(PlannerVertex::Sub(planner));
                    self.elements.last()
                }
                Constraint::Owns(owns) => {
                    let planner =
                        OwnsPlanner::from_constraint(owns, &self.variable_index, type_annotations, statistics);
                    self.elements.push(PlannerVertex::Owns(planner));
                    self.elements.last()
                }
                Constraint::Relates(relates) => {
                    let planner =
                        RelatesPlanner::from_constraint(relates, &self.variable_index, type_annotations, statistics);
                    self.elements.push(PlannerVertex::Relates(planner));
                    self.elements.last()
                }
                Constraint::Plays(plays) => {
                    let planner =
                        PlaysPlanner::from_constraint(plays, &self.variable_index, type_annotations, statistics);
                    self.elements.push(PlannerVertex::Plays(planner));
                    self.elements.last()
                }

                Constraint::Isa(isa) => {
                    let planner = IsaPlanner::from_constraint(isa, &self.variable_index, type_annotations, statistics);
                    self.elements.push(PlannerVertex::Isa(planner));
                    self.elements.last()
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
                    let lhs = self.variable_index[&expression.left().as_variable().unwrap()];
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
                    let lhs = Input::from_vertex(comparison.lhs(), &self.variable_index);
                    let rhs = Input::from_vertex(comparison.rhs(), &self.variable_index);
                    if let Input::Variable(lhs) = lhs {
                        match comparison.comparator() {
                            Comparator::Equal => {
                                self.elements[lhs].add_equal(rhs);
                            }
                            Comparator::Less | Comparator::LessOrEqual => {
                                self.elements[lhs].add_upper_bound(rhs);
                            }
                            Comparator::Greater | Comparator::GreaterOrEqual => {
                                self.elements[lhs].add_lower_bound(rhs);
                            }
                            Comparator::Like => todo!("like operator"),
                            Comparator::Contains => todo!("contains operator"),
                        }
                    }
                    if let Input::Variable(rhs) = rhs {
                        match comparison.comparator() {
                            Comparator::Equal => {
                                self.elements[rhs].add_equal(lhs);
                            }
                            Comparator::Less | Comparator::LessOrEqual => {
                                self.elements[rhs].add_upper_bound(lhs);
                            }
                            Comparator::Greater | Comparator::GreaterOrEqual => {
                                self.elements[rhs].add_lower_bound(lhs);
                            }
                            Comparator::Like => todo!("like operator"),
                            Comparator::Contains => todo!("contains operator"),
                        }
                    }
                    self.elements.push(PlannerVertex::Comparison(ComparisonPlanner::from_constraint(
                        comparison,
                        &self.variable_index,
                        type_annotations,
                        statistics,
                    )));
                    self.elements.last()
                }
            };

            if let Some(planner) = planner {
                let planner_index = self.elements.len() - 1;
                self.index_to_constraint.insert(planner_index, constraint);
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
                .filter(|&&elem| self.elements[elem].is_valid(elem, &ordering, &self.adjacency))
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

#[derive(Debug, Default)]
struct ProgramBuilder {
    sort_variable: Option<Variable>,
    instructions: Vec<ConstraintInstruction<VariablePosition>>,
    last_output: Option<u32>,
}

impl ProgramBuilder {
    fn finish(self, outputs: &HashMap<VariablePosition, Variable>) -> Program {
        let sort_variable = *outputs.iter().find(|(_, &item)| Some(item) == self.sort_variable).unwrap().0;
        Program::Intersection(IntersectionProgram::new(
            sort_variable,
            self.instructions,
            &(0..self.last_output.unwrap()).map(VariablePosition::new).collect_vec(),
            self.last_output.unwrap(),
        ))
    }
}

#[derive(Default)]
struct MatchProgramBuilder {
    programs: Vec<ProgramBuilder>,
    current: ProgramBuilder,
    outputs: HashMap<VariablePosition, Variable>,
    index: HashMap<Variable, VariablePosition>,
}

impl MatchProgramBuilder {
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
        if !self.index.contains_key(&var) {
            self.index.insert(var, VariablePosition::new(self.index.len() as u32));
            self.outputs.insert(VariablePosition::new(self.outputs.len() as u32), var);
        }
    }

    fn finish_one(&mut self) {
        if !self.current.instructions.is_empty() {
            self.current.last_output = Some(self.outputs.len() as u32);
            self.programs.push(mem::take(&mut self.current));
        }
    }

    fn finish(mut self) -> Vec<Program> {
        self.finish_one();
        self.programs.into_iter().map(|builder| builder.finish(&self.outputs)).collect()
    }
}

fn lower_plan(
    plan_builder: &PlanBuilder,
    ordering: Vec<usize>,
    type_annotations: &TypeAnnotations,
) -> (HashMap<Variable, VariablePosition>, HashMap<VariablePosition, Variable>, Vec<Program>) {
    let index_to_variable: HashMap<_, _> =
        plan_builder.variable_index.iter().map(|(&variable, &index)| (index, variable)).collect();

    let mut match_builder = MatchProgramBuilder::default();

    let mut producers: HashMap<Variable, _> = HashMap::with_capacity(plan_builder.variable_index.len());

    for (i, &index) in ordering.iter().enumerate() {
        if index_to_variable.contains_key(&index) {
            continue;
        }

        let adjacent = match plan_builder.adjacency.get(&index) {
            Some(adj) => adj,
            None => &HashSet::new(),
        };

        let inputs = adjacent
            .iter()
            .filter(|&adj| ordering[..i].contains(adj) && producers.contains_key(&index_to_variable[adj]))
            .map(|adj| index_to_variable[adj])
            .collect::<HashSet<_>>();

        let constraint = &plan_builder.index_to_constraint[&index];
        if let Some(var) = &match_builder.current.sort_variable {
            if !constraint.ids().contains(var) {
                match_builder.finish_one();
            }
        }

        let planner = &plan_builder.elements[index];

        macro_rules! binary {
            ($((with $with:ident))? $lhs:ident $con:ident $rhs:ident, $fw:ident($fwi:ident), $bw:ident($bwi:ident)) => {{
                let lhs = $con.$lhs();
                let rhs = $con.$rhs();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();

                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();

                assert!(num_input_variables > 0);

                if inputs.len() == num_input_variables {
                    let lhs_producer =
                        lhs_var.map(|lhs| producers.get(&lhs).expect("bound lhs must have been produced"));
                    let rhs_producer =
                        rhs_var.map(|rhs| producers.get(&rhs).expect("bound rhs must have been produced"));
                    let Some(&(program, instruction)) = Ord::max(lhs_producer, rhs_producer) else {
                        unreachable!("num_input_variables > 0")
                    };
                    let lhs_pos = lhs.clone().map(match_builder.position_mapping());
                    let rhs_pos = rhs.clone().map(match_builder.position_mapping());
                    match_builder.get_program_mut(program).instructions[instruction]
                        .add_check(CheckInstruction::$fw { $lhs: lhs_pos, $rhs: rhs_pos, $($with: $con.$with())? });
                    continue;
                }

                let sort_variable = match (lhs_var, rhs_var) {
                    (Some(lhs), Some(rhs)) => {
                        if inputs.contains(&rhs) {
                            lhs
                        } else if inputs.contains(&lhs) {
                            rhs
                        } else if planner.unbound_direction() == Direction::Canonical {
                            lhs
                        } else {
                            rhs
                        }
                    }
                    (Some(lhs), None) => lhs,
                    (None, Some(rhs)) => rhs,
                    (None, None) => unreachable!("no variables in constraint?"),
                };

                let con = $con.clone();
                let instruction = if lhs_var.is_some_and(|lhs| inputs.contains(&lhs)) {
                    ConstraintInstruction::$fw($fwi::new(con, Inputs::Single([lhs_var.unwrap()]), type_annotations))
                } else if rhs_var.is_some_and(|rhs| inputs.contains(&rhs)) {
                    ConstraintInstruction::$bw($bwi::new(con, Inputs::Single([rhs_var.unwrap()]), type_annotations))
                } else if planner.unbound_direction() == Direction::Canonical {
                    ConstraintInstruction::$fw($fwi::new(con, Inputs::None([]), type_annotations))
                } else {
                    ConstraintInstruction::$bw($bwi::new(con, Inputs::None([]), type_annotations))
                };
                let producer_index = match_builder.push_instruction(
                    sort_variable,
                    instruction,
                    [lhs_var, rhs_var].into_iter().flatten(),
                );

                for var in [lhs_var, rhs_var].into_iter().flatten() {
                    if !inputs.contains(&var) {
                        producers.insert(var, producer_index);
                    }
                }
            }};
        }

        match constraint {
            Constraint::Kind(kind) => {
                let var = kind.type_().as_variable().unwrap();
                let instruction = ConstraintInstruction::TypeList(TypeListInstruction::new(var, type_annotations));
                let producer_index = match_builder.push_instruction(var, instruction, [var]);
                producers.insert(var, producer_index);
            }
            Constraint::RoleName(name) => {
                let var = name.left().as_variable().unwrap();
                let instruction = ConstraintInstruction::TypeList(TypeListInstruction::new(var, type_annotations));
                let producer_index = match_builder.push_instruction(var, instruction, [var]);
                producers.insert(var, producer_index);
            }
            Constraint::Label(label) => {
                let var = label.left().as_variable().unwrap();
                let instruction = ConstraintInstruction::TypeList(TypeListInstruction::new(var, type_annotations));
                let producer_index = match_builder.push_instruction(var, instruction, [var]);
                producers.insert(var, producer_index);
            }

            Constraint::Isa(isa) => {
                binary!((with isa_kind) thing isa type_, Isa(IsaInstruction), IsaReverse(IsaReverseInstruction))
            }
            Constraint::Sub(sub) => {
                binary!((with sub_kind) subtype sub supertype, Sub(SubInstruction), SubReverse(SubReverseInstruction))
            }
            Constraint::Owns(owns) => {
                binary!(owner owns attribute, Owns(OwnsInstruction), OwnsReverse(OwnsReverseInstruction))
            }
            Constraint::Relates(relates) => {
                binary!(relation relates role_type, Relates(RelatesInstruction), RelatesReverse(RelatesReverseInstruction))
            }
            Constraint::Plays(plays) => {
                binary!(player plays role_type, Plays(PlaysInstruction), PlaysReverse(PlaysReverseInstruction))
            }

            Constraint::Has(has) => {
                binary!(owner has attribute, Has(HasInstruction), HasReverse(HasReverseInstruction))
            }

            Constraint::Links(links) => {
                let relation = links.relation().as_variable().unwrap();
                let player = links.player().as_variable().unwrap();
                let role = links.role_type().as_variable().unwrap();

                if inputs.len() == 3 {
                    let relation_producer = producers.get(&relation).expect("bound relation must have been produced");
                    let player_producer = producers.get(&player).expect("bound player must have been produced");
                    let (program, instruction) = std::cmp::Ord::max(*relation_producer, *player_producer);

                    let relation_pos = match_builder.position(relation).into();
                    let player_pos = match_builder.position(player).into();
                    let role_pos = match_builder.position(role).into();
                    match_builder.get_program_mut(program).instructions[instruction].add_check(
                        CheckInstruction::Links { relation: relation_pos, player: player_pos, role: role_pos },
                    );
                    continue;
                }

                let sort_variable = if inputs.is_empty() && planner.unbound_direction() == Direction::Canonical
                    || inputs.contains(&player)
                {
                    relation
                } else {
                    player
                };

                let links = links.clone();
                let instruction = if inputs.contains(&relation) {
                    ConstraintInstruction::Links(LinksInstruction::new(
                        links,
                        Inputs::Single([relation]),
                        type_annotations,
                    ))
                } else if inputs.contains(&player) {
                    ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                        links,
                        Inputs::Single([player]),
                        type_annotations,
                    ))
                } else if planner.unbound_direction() == Direction::Canonical {
                    ConstraintInstruction::Links(LinksInstruction::new(links, Inputs::None([]), type_annotations))
                } else {
                    ConstraintInstruction::LinksReverse(LinksReverseInstruction::new(
                        links,
                        Inputs::None([]),
                        type_annotations,
                    ))
                };
                let producer_index =
                    match_builder.push_instruction(sort_variable, instruction, [relation, player, role]);

                for &var in &[relation, player, role] {
                    if !inputs.contains(&var) {
                        producers.insert(var, producer_index);
                    }
                }
            }

            Constraint::ExpressionBinding(_) => todo!("expression binding"),
            Constraint::FunctionCallBinding(_) => todo!("function call binding"),
            Constraint::Comparison(compare) => {
                let lhs = compare.lhs();
                let rhs = compare.rhs();
                let comparator = compare.comparator();

                let lhs_var = lhs.as_variable();
                let rhs_var = rhs.as_variable();
                let num_input_variables = [lhs_var, rhs_var].into_iter().filter(|x| x.is_some()).count();
                assert!(num_input_variables > 0);
                if inputs.len() == num_input_variables {
                    let lhs_producer =
                        lhs_var.map(|lhs| producers.get(&lhs).expect("bound lhs must have been produced"));
                    let rhs_producer =
                        rhs_var.map(|rhs| producers.get(&rhs).expect("bound rhs must have been produced"));
                    let Some(&(program, instruction)) = Ord::max(lhs_producer, rhs_producer) else {
                        unreachable!("num_input_variables > 0")
                    };
                    let lhs_pos = lhs.clone().map(match_builder.position_mapping());
                    let rhs_pos = rhs.clone().map(match_builder.position_mapping());
                    match_builder.get_program_mut(program).instructions[instruction]
                        .add_check(CheckInstruction::Comparison { lhs: lhs_pos, rhs: rhs_pos, comparator });
                    continue;
                }
                todo!()
            }
        }
    }
    (match_builder.index.clone(), match_builder.outputs.clone(), match_builder.finish())
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
    pub fn selected_variables(&self) -> &[VariablePosition] {
        match self {
            Program::Intersection(program) => &program.selected_variables,
            Program::UnsortedJoin(program) => &program.selected_variables,
            Program::Assignment(_) => todo!(),
            Program::Disjunction(_) => todo!(),
            Program::Negation(_) => todo!(),
            Program::Optional(_) => todo!(),
        }
    }

    pub fn new_variables(&self) -> &[VariablePosition] {
        match self {
            Program::Intersection(program) => program.new_variables(),
            Program::UnsortedJoin(program) => program.new_variables(),
            Program::Assignment(program) => program.new_variables(),
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
            Program::Disjunction(_) => todo!(),
            Program::Negation(_) => todo!(),
            Program::Optional(_) => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct IntersectionProgram {
    pub sort_variable: VariablePosition,
    pub instructions: Vec<ConstraintInstruction<VariablePosition>>,
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

        Self {
            sort_variable,
            instructions,
            new_variables,
            output_width,
            input_variables,
            selected_variables: selected_variables.to_owned(),
        }
    }

    fn new_variables(&self) -> &[VariablePosition] {
        &self.new_variables
    }

    fn output_width(&self) -> u32 {
        self.output_width
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

pub trait InstructionAPI<ID: IrID> {
    fn constraint(&self) -> Constraint<ID>;
}
