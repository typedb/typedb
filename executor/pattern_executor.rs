/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use answer::{variable::Variable, variable_value::VariableValue};
use compiler::match_::{
    instructions::ConstraintInstruction,
    planner::pattern_plan::{
        AssignmentProgram, DisjunctionProgram, IntersectionProgram, MatchProgram, NegationProgram, OptionalProgram,
        Program, UnsortedJoinProgram,
    },
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::program::block::VariableRegistry;
use itertools::Itertools;
use lending_iterator::{AsLendingIterator, LendingIterator, Peekable};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::{Batch, BatchRowIterator, ImmutableRow, Row},
    instruction::{iterator::TupleIterator, InstructionExecutor},
    pipeline::{PipelineContext, PipelineError},
    SelectedPositions, VariablePosition,
};

pub(crate) struct PatternExecutor {
    variable_positions: HashMap<Variable, VariablePosition>,
    variable_positions_index: Vec<Variable>,

    program_executors: Vec<StepExecutor>,
    // modifiers: Modifier,
    initialised: bool,
    output: Option<Batch>,
}

impl PatternExecutor {
    pub(crate) fn new(
        plan: &MatchProgram,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let (programs, context) = (plan.programs(), plan.variable_registry());
        let mut variable_positions = HashMap::new();
        let mut program_executors = Vec::with_capacity(programs.len());
        for program in programs {
            for variable in program.new_variables() {
                let previous =
                    variable_positions.insert(*variable, VariablePosition::new(variable_positions.len() as u32));
                debug_assert_eq!(previous, Option::None);
            }
            let executor = StepExecutor::new(program, context, &variable_positions, snapshot, thing_manager)?;
            program_executors.push(executor)
        }

        let mut variable_positions_index = vec![Variable::new(0); variable_positions.len()];
        for (variable, position) in &variable_positions {
            variable_positions_index[position.as_usize()] = *variable
        }

        Ok(PatternExecutor {
            variable_positions,
            variable_positions_index,
            program_executors,
            // modifiers:
            initialised: false,
            output: None,
        })
    }

    pub(crate) fn variable_positions(&self) -> &HashMap<Variable, VariablePosition> {
        &self.variable_positions
    }

    pub(crate) fn variable_positions_index(&self) -> &Vec<Variable> {
        &self.variable_positions_index
    }

    pub fn into_iterator(
        self,
        snapshot: Arc<impl ReadableSnapshot + 'static>,
        thing_manager: Arc<ThingManager>,
    ) -> impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>> {
        AsLendingIterator::new(BatchIterator::new(self, snapshot, thing_manager)).flat_map(BatchRowIterator::new)
    }

    fn compute_next_batch(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        let programs_len = self.program_executors.len();

        let (mut current_program, mut last_program_batch, mut direction) = if self.initialised {
            (programs_len - 1, None, Direction::Backward)
        } else {
            self.initialised = true;
            (0, Some(Batch::SINGLE_EMPTY_ROW), Direction::Forward)
        };

        loop {
            match direction {
                Direction::Forward => {
                    if current_program >= programs_len {
                        return Ok(last_program_batch);
                    } else {
                        let batch = self.program_executors[current_program].batch_from(
                            last_program_batch.take().unwrap(),
                            snapshot,
                            thing_manager,
                        )?;
                        match batch {
                            None => {
                                direction = Direction::Backward;
                                if current_program == 0 {
                                    return Ok(None);
                                } else {
                                    current_program -= 1;
                                }
                            }
                            Some(batch) => {
                                last_program_batch = Some(batch);
                                current_program += 1;
                            }
                        }
                    }
                }
                Direction::Backward => {
                    let batch = self.program_executors[current_program].batch_continue(snapshot, thing_manager)?;
                    match batch {
                        None => {
                            if current_program == 0 {
                                return Ok(None);
                            } else {
                                current_program -= 1;
                            }
                        }
                        Some(batch) => {
                            direction = Direction::Forward;
                            last_program_batch = Some(batch);
                            current_program += 1;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Direction {
    Forward,
    Backward,
}

pub(crate) struct BatchIterator<Snapshot: ReadableSnapshot> {
    executor: PatternExecutor,
    context: PipelineContext<Snapshot>,
}

impl<Snapshot: ReadableSnapshot> BatchIterator<Snapshot> {
    pub(crate) fn new(executor: PatternExecutor, snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>) -> Self {
        Self::new_from_context(executor, PipelineContext::Shared(snapshot, thing_manager))
    }

    fn new_from_context(executor: PatternExecutor, context: PipelineContext<Snapshot>) -> Self {
        Self { executor, context }
    }

    pub(crate) fn into_parts(self) -> (PatternExecutor, PipelineContext<Snapshot>) {
        (self.executor, self.context)
    }

    pub(crate) fn try_get_shared_context(&mut self) -> Result<PipelineContext<Snapshot>, PipelineError> {
        self.context.try_get_shared()
    }
}

impl<Snapshot: ReadableSnapshot> Iterator for BatchIterator<Snapshot> {
    type Item = Result<Batch, ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Self { executor, context } = self;
        let (snapshot, thing_manager) = self.context.borrow_parts();
        let batch = self.executor.compute_next_batch(snapshot, thing_manager);
        batch.transpose()
    }
}

enum StepExecutor {
    SortedJoin(IntersectionExecutor),
    UnsortedJoin(UnsortedJoinExecutor),
    Assignment(AssignExecutor),

    Disjunction(DisjunctionExecutor),
    Negation(NegationExecutor),
    Optional(OptionalExecutor),
}

impl StepExecutor {
    fn new(
        program: &Program,
        variable_registry: &VariableRegistry,
        variable_positions: &HashMap<Variable, VariablePosition>,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let row_width = variable_positions.len() as u32;
        match program {
            Program::Intersection(IntersectionProgram { sort_variable, instructions, selected_variables, .. }) => {
                let executor = IntersectionExecutor::new(
                    *sort_variable,
                    instructions.clone(),
                    row_width,
                    selected_variables.clone(),
                    variable_registry.get_variables_named(),
                    variable_positions,
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::SortedJoin(executor))
            }
            Program::UnsortedJoin(UnsortedJoinProgram { iterate_instruction, check_instructions, .. }) => {
                let executor = UnsortedJoinExecutor::new(
                    iterate_instruction.clone(),
                    check_instructions.clone(),
                    row_width,
                    variable_positions,
                );
                Ok(Self::UnsortedJoin(executor))
            }
            Program::Assignment(AssignmentProgram { .. }) => {
                todo!()
            }
            Program::Disjunction(DisjunctionProgram { disjunction: disjunction_plans, .. }) => {
                // let executors = plans.into_iter().map(|pattern_plan| PatternExecutor::new(pattern_plan, )).collect();
                // Self::Disjunction(DisjunctionExecutor::new(executors, variable_positions))
                todo!()
            }
            Program::Negation(NegationProgram { negation: negation_plan, .. }) => {
                let executor = PatternExecutor::new(negation_plan, snapshot, thing_manager)?;
                // // TODO: add limit 1, filters if they aren't there already?
                Ok(Self::Negation(NegationExecutor::new(executor, variable_positions)))
            }
            Program::Optional(OptionalProgram { optional: optional_plan, .. }) => {
                let pattern_executor = PatternExecutor::new(optional_plan, snapshot, thing_manager)?;
                Ok(Self::Optional(OptionalExecutor::new(pattern_executor)))
            }
        }
    }

    fn batch_from(
        &mut self,
        input_batch: Batch,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        match self {
            StepExecutor::SortedJoin(sorted) => sorted.batch_from(input_batch, snapshot, thing_manager),
            StepExecutor::UnsortedJoin(unsorted) => unsorted.batch_from(input_batch),
            StepExecutor::Assignment(single) => single.batch_from(input_batch),
            StepExecutor::Disjunction(disjunction) => disjunction.batch_from(input_batch),
            StepExecutor::Negation(negation) => negation.batch_from(input_batch),
            StepExecutor::Optional(optional) => optional.batch_from(input_batch),
        }
    }

    fn batch_continue(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        match self {
            StepExecutor::SortedJoin(sorted) => sorted.batch_continue(snapshot, thing_manager),
            StepExecutor::UnsortedJoin(unsorted) => todo!(), // unsorted.batch_continue(snapshot, thing_manager),
            StepExecutor::Disjunction(disjunction) => todo!(),
            StepExecutor::Optional(optional) => todo!(),
            StepExecutor::Assignment(_) | StepExecutor::Negation(_) => Ok(None),
        }
    }
}

/// Performs an n-way intersection/join using sorted iterators.
/// To avoid missing cartesian outputs when multiple variables are unbound, the executor can leverage a
/// Cartesian sub-program, which generates all cartesian answers within one intersection, if there are any.
struct IntersectionExecutor {
    instruction_executors: Vec<InstructionExecutor>,
    sort_variable_position: VariablePosition,
    output_width: u32,
    outputs_selected: SelectedPositions,

    iterators: Vec<TupleIterator>,
    cartesian_iterator: CartesianIterator,
    input: Option<Peekable<BatchRowIterator>>,
    intersection_row: Vec<VariableValue<'static>>,
    intersection_multiplicity: u64,
    output: Option<Batch>,
}

impl IntersectionExecutor {
    fn new(
        sort_variable: Variable,
        instructions: Vec<ConstraintInstruction>,
        output_width: u32,
        select_variables: Vec<Variable>,
        named_variables: &HashMap<Variable, String>,
        variable_positions: &HashMap<Variable, VariablePosition>,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let sort_variable_position = *variable_positions.get(&sort_variable).unwrap();
        let instruction_count = instructions.len();
        let executors: Vec<InstructionExecutor> = instructions
            .into_iter()
            .map(|instruction| {
                InstructionExecutor::new(
                    instruction,
                    &select_variables,
                    named_variables,
                    variable_positions,
                    snapshot,
                    thing_manager,
                    Some(sort_variable),
                )
            })
            .try_collect()?;

        Ok(Self {
            instruction_executors: executors,
            sort_variable_position,
            output_width,
            outputs_selected: SelectedPositions::new(&select_variables, variable_positions),
            iterators: Vec::with_capacity(instruction_count),
            cartesian_iterator: CartesianIterator::new(
                output_width as usize,
                instruction_count,
                sort_variable_position,
            ),
            input: None,
            intersection_row: (0..output_width).map(|_| VariableValue::Empty).collect_vec(),
            intersection_multiplicity: 1,
            output: None,
        })
    }

    fn batch_from(
        &mut self,
        input_batch: Batch,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        debug_assert!(self.output.is_none() && (self.input.is_none() || self.input.as_mut().unwrap().peek().is_none()));
        self.input = Some(Peekable::new(BatchRowIterator::new(Ok(input_batch))));
        debug_assert!(self.input.as_mut().unwrap().peek().is_some());
        self.may_create_intersection_iterators(snapshot, thing_manager)?;
        self.may_compute_next_batch(snapshot, thing_manager)?;
        Ok(self.output.take())
    }

    fn batch_continue(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        debug_assert!(self.output.is_none());
        // TODO: this may not have to reopen iterators
        self.may_create_intersection_iterators(snapshot, thing_manager)?;
        self.may_compute_next_batch(snapshot, thing_manager)?;
        Ok(self.output.take())
    }

    fn may_compute_next_batch(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptReadError> {
        if self.compute_next_row(snapshot, thing_manager)? {
            // don't allocate batch until 1 answer is confirmed
            let mut batch = Batch::new(self.output_width);
            batch.append(|mut row| self.write_next_row_into(&mut row));
            while !batch.is_full() && self.compute_next_row(snapshot, thing_manager)? {
                batch.append(|mut row| self.write_next_row_into(&mut row));
            }
            self.output = Some(batch);
        }
        Ok(())
    }

    fn write_next_row_into(&mut self, row: &mut Row<'_>) {
        if self.cartesian_iterator.is_active() {
            self.cartesian_iterator.write_into(row)
        } else {
            row.copy_from(&self.intersection_row, self.intersection_multiplicity);
        }
    }

    fn compute_next_row(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<bool, ConceptReadError> {
        if self.cartesian_iterator.is_active() {
            let found = self.cartesian_iterator.find_next(snapshot, thing_manager, &self.instruction_executors)?;
            if found {
                Ok(true)
            } else {
                // advance the first iterator past the intersection point to move to the next intersection
                let intersection = &self.intersection_row[self.sort_variable_position.as_usize()];
                let iter = &mut self.iterators[0];
                while iter.peek_first_unbound_value().transpose()?.is_some_and(|value| value == intersection) {
                    iter.advance_single()?;
                }
                self.compute_next_row(snapshot, thing_manager)
            }
        } else {
            while !self.iterators.is_empty() {
                let found = self.find_intersection()?;
                if found {
                    self.record_intersection()?;
                    self.advance_intersection_iterators_with_multiplicity()?;
                    self.may_activate_cartesian(snapshot, thing_manager)?;
                    return Ok(true);
                } else {
                    self.iterators.clear();
                    let _ = self.input.as_mut().unwrap().next().unwrap().map_err(|err| err.clone())?;
                    if self.input.as_mut().unwrap().peek().is_some() {
                        self.may_create_intersection_iterators(snapshot, thing_manager)?;
                    }
                }
            }
            Ok(false)
        }
    }

    fn find_intersection(&mut self) -> Result<bool, ConceptReadError> {
        debug_assert!(!self.iterators.is_empty());
        if self.iterators.len() == 1 {
            // if there's only 1 iterator, we can just use it without any intersection
            return Ok(self.iterators[0].peek().is_some());
        } else if self.iterators[0].peek().is_none() {
            // short circuit if the first iterator doesn't have any more outputs
            self.clear_intersection_iterators();
            return Ok(false);
        }

        let mut current_max_index = 0;
        loop {
            let mut failed = false;
            let mut retry = false;
            for i in 0..self.iterators.len() {
                if i == current_max_index {
                    continue;
                }

                let (containing_i, containing_max, i_index, max_index) = if current_max_index > i {
                    let (containing_i, containing_max) = self.iterators.split_at_mut(current_max_index);
                    (containing_i, containing_max, i, 0)
                } else {
                    let (containing_max, containing_i) = self.iterators.split_at_mut(i);
                    (containing_i, containing_max, 0, current_max_index)
                };
                let current_max = containing_max[max_index].peek_first_unbound_value().unwrap().unwrap();
                let max_cmp_peek = match containing_i[i_index].peek_first_unbound_value() {
                    None => {
                        failed = true;
                        break;
                    }
                    Some(Ok(value)) => current_max.partial_cmp(value).unwrap(),
                    Some(Err(err)) => return Err(err.clone()),
                };

                match max_cmp_peek {
                    Ordering::Less => {
                        current_max_index = i;
                        retry = true;
                    }
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        let iter_i = &mut containing_i[i_index];
                        let next_value_cmp =
                            iter_i.advance_until_index_is(iter_i.first_unbound_index(), current_max)?;
                        match next_value_cmp {
                            None => {
                                failed = true;
                                break;
                            }
                            Some(Ordering::Less) => {
                                unreachable!("Skip to should always be empty or equal/greater than the target")
                            }
                            Some(Ordering::Equal) => {}
                            Some(Ordering::Greater) => {
                                current_max_index = i;
                                retry = true;
                            }
                        }
                    }
                }
            }
            if failed {
                self.clear_intersection_iterators();
                return Ok(false);
            } else if !retry {
                debug_assert!(self.all_iterators_intersect());
                return Ok(true);
            }
        }
    }

    fn may_create_intersection_iterators(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptReadError> {
        debug_assert!(self.iterators.is_empty());
        let peek = self.input.as_mut().unwrap().peek();
        if let Some(input) = peek {
            let next_row: &ImmutableRow<'_> = input.as_ref().map_err(|err| (*err).clone())?;
            for executor in &self.instruction_executors {
                self.iterators.push(executor.get_iterator(snapshot, thing_manager, next_row.as_reference())?);
            }
        }
        Ok(())
    }

    fn advance_intersection_iterators_with_multiplicity(&mut self) -> Result<(), ConceptReadError> {
        let mut multiplicity: u64 = 1;
        for iter in &mut self.iterators {
            multiplicity *= iter.advance_past()? as u64;
        }
        self.intersection_multiplicity = multiplicity;
        Ok(())
    }

    fn clear_intersection_iterators(&mut self) {
        self.iterators.clear()
    }

    fn all_iterators_intersect(&mut self) -> bool {
        let (first, rest) = self.iterators.split_at_mut(1);
        let peek_0 = first[0].peek_first_unbound_value().unwrap().unwrap();
        rest.iter_mut().all(|iter| iter.peek_first_unbound_value().unwrap().unwrap() == peek_0)
    }

    fn record_intersection(&mut self) -> Result<(), ConceptReadError> {
        self.intersection_row.fill(VariableValue::Empty);
        let mut row = Row::new(&mut self.intersection_row, &mut self.intersection_multiplicity);
        for iter in &mut self.iterators {
            iter.write_values(&mut row)
        }

        let input_row = self.input.as_mut().unwrap().peek().unwrap().as_ref().map_err(|err| (*err).clone())?;
        for position in self.outputs_selected.iter_selected() {
            if position.as_usize() < input_row.width() {
                row.set(position, input_row.get(position).clone().into_owned())
            }
        }
        self.intersection_multiplicity = 1;
        Ok(())
    }

    fn may_activate_cartesian(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptReadError> {
        if self.iterators.len() == 1 {
            // don't delegate to cartesian iterator and incur new iterator costs if there cannot be a cartesian product
            return Ok(());
        }
        let mut cartesian = false;
        for iter in &mut self.iterators {
            if iter
                .peek_first_unbound_value()
                .transpose()?
                .is_some_and(|value| value == &self.intersection_row[self.sort_variable_position.as_usize()])
            {
                cartesian = true;
                break;
            }
        }
        if cartesian {
            self.cartesian_iterator.activate(
                snapshot,
                thing_manager,
                &self.instruction_executors,
                &self.intersection_row,
                self.intersection_multiplicity,
                &mut self.iterators,
            )?
        }
        Ok(())
    }
}

struct CartesianIterator {
    sort_variable_position: VariablePosition,
    is_active: bool,
    intersection_source: Vec<VariableValue<'static>>,
    intersection_multiplicity: u64,
    cartesian_executor_indices: Vec<usize>,
    iterators: Vec<Option<TupleIterator>>,
}

impl CartesianIterator {
    fn new(width: usize, iterator_executor_count: usize, sort_variable_position: VariablePosition) -> Self {
        CartesianIterator {
            sort_variable_position,
            is_active: false,
            intersection_source: vec![VariableValue::Empty; width],
            intersection_multiplicity: 1,
            cartesian_executor_indices: Vec::with_capacity(iterator_executor_count),
            iterators: (0..iterator_executor_count).into_iter().map(|_| Option::None).collect_vec(),
        }
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn activate(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        iterator_executors: &Vec<InstructionExecutor>,
        source_intersection: &Vec<VariableValue<'static>>,
        source_multiplicity: u64,
        intersection_iterators: &mut Vec<TupleIterator>,
    ) -> Result<(), ConceptReadError> {
        debug_assert!(source_intersection.len() == self.intersection_source.len());
        self.is_active = true;
        self.intersection_source.clone_from_slice(source_intersection);
        self.intersection_multiplicity = source_multiplicity;

        // we are able to re-use existing iterators since they should only move forward. We only reset the indices
        self.cartesian_executor_indices.clear();

        let intersection = &source_intersection[self.sort_variable_position.as_usize()];
        for (index, iter) in intersection_iterators.iter_mut().enumerate() {
            if iter.peek_first_unbound_value().transpose()?.is_some_and(|value| value == intersection) {
                self.cartesian_executor_indices.push(index);

                // reopen/move existing cartesian iterators forward to the intersection point
                let preexisting_iterator = self.iterators[index].take();
                let iterator = match preexisting_iterator {
                    None => self.reopen_iterator(snapshot, thing_manager, &iterator_executors[index])?,
                    Some(mut iter) => {
                        // TODO: use seek()
                        let next_value_cmp = iter.advance_until_index_is(iter.first_unbound_index(), intersection)?;
                        debug_assert!(next_value_cmp.is_some() && next_value_cmp.unwrap().is_eq());
                        iter
                    }
                };
                self.iterators[index] = Some(iterator);
            }
        }
        Ok(())
    }

    fn find_next(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        executors: &Vec<InstructionExecutor>,
    ) -> Result<bool, ConceptReadError> {
        debug_assert!(self.is_active);
        // precondition: all required iterators are open to the intersection point

        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        let mut executor_index = self.cartesian_executor_indices.len() - 1;
        loop {
            let iterator_index = self.cartesian_executor_indices[executor_index];
            let iter = (&mut self.iterators)[iterator_index].as_mut().unwrap();
            iter.advance_single()?;
            if !iter.peek_first_unbound_value().transpose()?.is_some_and(|value| value == intersection) {
                if executor_index == 0 {
                    self.is_active = false;
                    return Ok(false);
                } else {
                    let reopened = self.reopen_iterator(snapshot, thing_manager, &executors[executor_index])?;
                    self.iterators[iterator_index] = Some(reopened);
                    executor_index -= 1;
                }
            } else {
                return Ok(true);
            }
        }
    }

    fn reopen_iterator(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        executor: &InstructionExecutor,
    ) -> Result<TupleIterator, ConceptReadError> {
        let mut reopened = executor.get_iterator(
            snapshot,
            thing_manager,
            ImmutableRow::new(&self.intersection_source, self.intersection_multiplicity),
        )?;
        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        // TODO: use seek()
        reopened.advance_until_index_is(reopened.first_unbound_index(), intersection)?;
        Ok(reopened)
    }

    fn write_into(&mut self, row: &mut Row<'_>) {
        for &executor_index in &self.cartesian_executor_indices {
            let iterator = self.iterators[executor_index].as_mut().unwrap();
            iterator.write_values(row)
        }
        for (index, value) in self.intersection_source.iter().enumerate() {
            if *row.get(VariablePosition::new(index as u32)) == VariableValue::Empty {
                row.set(VariablePosition::new(index as u32), value.clone());
            }
        }
        row.set_multiplicity(self.intersection_multiplicity);
    }
}

struct UnsortedJoinExecutor {
    iterate: ConstraintInstruction,
    checks: Vec<ConstraintInstruction>,

    output_width: u32,
    output: Option<Batch>,
}

impl UnsortedJoinExecutor {
    fn new(
        iterate: ConstraintInstruction,
        checks: Vec<ConstraintInstruction>,
        total_vars: u32,
        variable_positions: &HashMap<Variable, VariablePosition>,
    ) -> Self {
        Self { iterate, checks, output_width: total_vars, output: None }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Option<Batch> {
        todo!()
    }
}

struct AssignExecutor {
    // executor: AssignInstruction,
    // checks: Vec<CheckInstruction>,
}

impl AssignExecutor {
    fn batch_from(&mut self, input_batch: Batch) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }
}

struct DisjunctionExecutor {
    executors: Vec<PatternExecutor>,
}

impl DisjunctionExecutor {
    fn new(
        executors: Vec<PatternExecutor>,
        variable_positions: &HashMap<Variable, VariablePosition>,
    ) -> DisjunctionExecutor {
        Self { executors }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }
}

struct NegationExecutor {
    executor: PatternExecutor,
}

impl NegationExecutor {
    fn new(executor: PatternExecutor, variable_positions: &HashMap<Variable, VariablePosition>) -> NegationExecutor {
        Self { executor }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }
}

struct OptionalExecutor {
    executor: PatternExecutor,
}

impl OptionalExecutor {
    fn new(executor: PatternExecutor) -> OptionalExecutor {
        Self { executor }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }
}
