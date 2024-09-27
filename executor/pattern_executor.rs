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
use ir::program::VariableRegistry;
use itertools::Itertools;
use lending_iterator::{adaptors::FlatMap, AsLendingIterator, LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    instruction::{iterator::TupleIterator, InstructionExecutor},
    pipeline::stage::ExecutionContext,
    row::{MaybeOwnedRow, Row},
    ExecutionInterrupt, SelectedPositions, VariablePosition,
};

pub(crate) struct PatternExecutor {
    variable_positions: HashMap<Variable, VariablePosition>,
    variable_positions_index: Vec<Variable>,

    program_executors: Vec<ProgramExecutor>,
    // modifiers: Modifier,
    initialised: bool,
    output: Option<FixedBatch>,
}

impl PatternExecutor {
    pub(crate) fn new(
        program: &MatchProgram,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let programs = program.programs();
        let context = program.variable_registry();
        let variable_positions = program.variable_positions();

        let program_executors = programs
            .iter()
            .map(|program| ProgramExecutor::new(program, context, variable_positions, snapshot, thing_manager))
            .try_collect()?;

        Ok(PatternExecutor {
            variable_positions: program.variable_positions().clone(),
            variable_positions_index: program.variable_positions_index().to_owned(),
            program_executors,
            // modifiers:
            initialised: false,
            output: None,
        })
    }

    pub(crate) fn variable_positions(&self) -> &HashMap<Variable, VariablePosition> {
        &self.variable_positions
    }

    pub(crate) fn variable_positions_index(&self) -> &[Variable] {
        &self.variable_positions_index
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> PatternIterator<Snapshot> {
        PatternIterator::new(
            AsLendingIterator::new(BatchIterator::new(self, context, interrupt)).flat_map(FixedBatchRowIterator::new),
        )
    }

    fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let programs_len = self.program_executors.len();

        let (mut current_program, mut last_program_batch, mut direction) = if self.initialised {
            (programs_len - 1, None, Direction::Backward)
        } else {
            self.initialised = true;
            (0, Some(FixedBatch::SINGLE_EMPTY_ROW), Direction::Forward)
        };

        loop {
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }

            match direction {
                Direction::Forward => {
                    if current_program >= programs_len {
                        return Ok(last_program_batch);
                    } else {
                        let batch = self.program_executors[current_program]
                            .batch_from(last_program_batch.take().unwrap(), context)?;
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
                    let batch = self.program_executors[current_program].batch_continue(context)?;
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

type PatternRowIterator<Snapshot> = FlatMap<
    AsLendingIterator<BatchIterator<Snapshot>>,
    FixedBatchRowIterator,
    fn(Result<FixedBatch, ReadExecutionError>) -> FixedBatchRowIterator,
>;

pub(crate) struct PatternIterator<Snapshot: ReadableSnapshot + 'static> {
    iterator: PatternRowIterator<Snapshot>,
}

impl<Snapshot: ReadableSnapshot> PatternIterator<Snapshot> {
    fn new(iterator: PatternRowIterator<Snapshot>) -> Self {
        Self { iterator }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for PatternIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, &'a ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}

pub(crate) struct BatchIterator<Snapshot> {
    executor: PatternExecutor,
    context: ExecutionContext<Snapshot>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot> BatchIterator<Snapshot> {
    pub(crate) fn new(
        executor: PatternExecutor,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { executor, context, interrupt }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> Iterator for BatchIterator<Snapshot> {
    type Item = Result<FixedBatch, ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.executor.compute_next_batch(&self.context, &mut self.interrupt);
        batch.transpose()
    }
}

enum ProgramExecutor {
    SortedJoin(IntersectionExecutor),
    UnsortedJoin(UnsortedJoinExecutor),
    Assignment(AssignExecutor),

    Disjunction(DisjunctionExecutor),
    Negation(NegationExecutor),
    Optional(OptionalExecutor),
}

impl ProgramExecutor {
    fn new(
        program: &Program,
        variable_registry: &VariableRegistry,
        variable_positions: &HashMap<Variable, VariablePosition>,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let row_width = program.output_width();
        match program {
            Program::Intersection(IntersectionProgram { sort_variable, instructions, selected_variables, .. }) => {
                let executor = IntersectionExecutor::new(
                    *sort_variable,
                    instructions.clone(),
                    row_width,
                    selected_variables.clone(),
                    &variable_positions
                        .iter()
                        .filter_map(|(var, &pos)| {
                            variable_registry.variable_names().get(var).map(|name| (pos, name.to_owned()))
                        })
                        .collect(),
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::SortedJoin(executor))
            }
            Program::UnsortedJoin(UnsortedJoinProgram { iterate_instruction, check_instructions, .. }) => {
                let executor =
                    UnsortedJoinExecutor::new(iterate_instruction.clone(), check_instructions.clone(), row_width);
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
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            ProgramExecutor::SortedJoin(sorted) => sorted.batch_from(input_batch, context),
            ProgramExecutor::UnsortedJoin(unsorted) => unsorted.batch_from(input_batch),
            ProgramExecutor::Assignment(single) => single.batch_from(input_batch),
            ProgramExecutor::Disjunction(disjunction) => disjunction.batch_from(input_batch),
            ProgramExecutor::Negation(negation) => negation.batch_from(input_batch),
            ProgramExecutor::Optional(optional) => optional.batch_from(input_batch),
        }
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            ProgramExecutor::SortedJoin(sorted) => sorted.batch_continue(context),
            ProgramExecutor::UnsortedJoin(unsorted) => todo!(), // unsorted.batch_continue(snapshot, thing_manager),
            ProgramExecutor::Disjunction(disjunction) => todo!(),
            ProgramExecutor::Optional(optional) => todo!(),
            ProgramExecutor::Assignment(_) | ProgramExecutor::Negation(_) => Ok(None),
        }
    }
}

/// Performs an n-way intersection/join using sorted iterators.
/// To avoid missing cartesian outputs when multiple variables are unbound, the executor can leverage a
/// Cartesian sub-program, which generates all cartesian answers within one intersection, if there are any.
struct IntersectionExecutor {
    instruction_executors: Vec<InstructionExecutor>,
    sort_variable: VariablePosition,
    output_width: u32,
    outputs_selected: SelectedPositions,

    iterators: Vec<TupleIterator>,
    cartesian_iterator: CartesianIterator,
    input: Option<Peekable<FixedBatchRowIterator>>,
    intersection_row: Vec<VariableValue<'static>>,
    intersection_multiplicity: u64,
    output: Option<FixedBatch>,
}

impl IntersectionExecutor {
    fn new(
        sort_variable: VariablePosition,
        instructions: Vec<ConstraintInstruction<VariablePosition>>,
        output_width: u32,
        select_variables: Vec<VariablePosition>,
        named_variables: &HashMap<VariablePosition, String>,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let instruction_count = instructions.len();
        let executors: Vec<InstructionExecutor> = instructions
            .into_iter()
            .map(|instruction| {
                InstructionExecutor::new(
                    instruction,
                    &select_variables,
                    named_variables,
                    &**snapshot,
                    thing_manager,
                    Some(sort_variable),
                )
            })
            .try_collect()?;

        Ok(Self {
            instruction_executors: executors,
            sort_variable,
            output_width,
            outputs_selected: SelectedPositions::new(select_variables),
            iterators: Vec::with_capacity(instruction_count),
            cartesian_iterator: CartesianIterator::new(output_width as usize, instruction_count, sort_variable),
            input: None,
            intersection_row: (0..output_width).map(|_| VariableValue::Empty).collect_vec(),
            intersection_multiplicity: 1,
            output: None,
        })
    }

    fn batch_from(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        debug_assert!(self.output.is_none() && (self.input.is_none() || self.input.as_mut().unwrap().peek().is_none()));
        self.input = Some(Peekable::new(FixedBatchRowIterator::new(Ok(input_batch))));
        debug_assert!(self.input.as_mut().unwrap().peek().is_some());
        self.may_create_intersection_iterators(context)?;
        self.may_compute_next_batch(context)?;
        Ok(self.output.take())
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        debug_assert!(self.output.is_none());
        self.may_compute_next_batch(context)?;
        Ok(self.output.take())
    }

    fn may_compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        if self.compute_next_row(context)? {
            // don't allocate batch until 1 answer is confirmed
            let mut batch = FixedBatch::new(self.output_width);
            batch.append(|mut row| self.write_next_row_into(&mut row));
            while !batch.is_full() && self.compute_next_row(context)? {
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
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<bool, ReadExecutionError> {
        if self.cartesian_iterator.is_active() {
            let found = self.cartesian_iterator.find_next(context, &self.instruction_executors)?;
            if found {
                Ok(true)
            } else {
                // advance the first iterator past the intersection point to move to the next intersection
                let intersection = &self.intersection_row[self.sort_variable.as_usize()];
                let iter = &mut self.iterators[0];
                while iter
                    .peek_first_unbound_value()
                    .transpose()
                    .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                    .is_some_and(|value| value == intersection)
                {
                    iter.advance_single().map_err(|err| ReadExecutionError::ConceptRead { source: err })?;
                }
                self.compute_next_row(context)
            }
        } else {
            while !self.iterators.is_empty() {
                let found = self.find_intersection()?;
                if found {
                    self.record_intersection()?;
                    self.advance_intersection_iterators_with_multiplicity()?;
                    self.may_activate_cartesian(context)?;
                    return Ok(true);
                } else {
                    self.iterators.clear();
                    let _ = self.input.as_mut().unwrap().next().unwrap().map_err(|err| err.clone());
                    if self.input.as_mut().unwrap().peek().is_some() {
                        self.may_create_intersection_iterators(context)?;
                    }
                }
            }
            Ok(false)
        }
    }

    fn find_intersection(&mut self) -> Result<bool, ReadExecutionError> {
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
                    Some(Err(err)) => return Err(ReadExecutionError::ConceptRead { source: err.clone() }),
                };

                match max_cmp_peek {
                    Ordering::Less => {
                        current_max_index = i;
                        retry = true;
                    }
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        let iter_i = &mut containing_i[i_index];
                        let next_value_cmp = iter_i
                            .advance_until_index_is(iter_i.first_unbound_index(), current_max)
                            .map_err(|err| ReadExecutionError::ConceptRead { source: err })?;
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
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        debug_assert!(self.iterators.is_empty());
        let peek = self.input.as_mut().unwrap().peek();
        if let Some(input) = peek {
            let next_row: &MaybeOwnedRow<'_> = input.as_ref().map_err(|err| (*err).clone())?;
            for executor in &self.instruction_executors {
                self.iterators.push(executor.get_iterator(context, next_row.as_reference()).map_err(|err| {
                    ReadExecutionError::CreatingIterator { instruction_name: executor.name().to_string(), source: err }
                })?);
            }
        }
        Ok(())
    }

    fn advance_intersection_iterators_with_multiplicity(&mut self) -> Result<(), ReadExecutionError> {
        let mut multiplicity: u64 = 1;
        for iter in &mut self.iterators {
            multiplicity *= iter.advance_past().map_err(|err| ReadExecutionError::ConceptRead { source: err })? as u64;
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

    fn record_intersection(&mut self) -> Result<(), ReadExecutionError> {
        self.intersection_row.fill(VariableValue::Empty);
        let mut row = Row::new(&mut self.intersection_row, &mut self.intersection_multiplicity);
        for iter in &mut self.iterators {
            iter.write_values(&mut row)
        }

        let input_row = self.input.as_mut().unwrap().peek().unwrap().as_ref().map_err(|err| (*err).clone())?;
        for &position in &self.outputs_selected {
            if position.as_usize() < input_row.len() {
                row.set(position, input_row.get(position).clone().into_owned())
            }
        }
        self.intersection_multiplicity = 1;
        Ok(())
    }

    fn may_activate_cartesian(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        if self.iterators.len() == 1 {
            // don't delegate to cartesian iterator and incur new iterator costs if there cannot be a cartesian product
            return Ok(());
        }
        let mut cartesian = false;
        for iter in &mut self.iterators {
            if iter
                .peek_first_unbound_value()
                .transpose()
                .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                .is_some_and(|value| value == &self.intersection_row[self.sort_variable.as_usize()])
            {
                cartesian = true;
                break;
            }
        }
        if cartesian {
            self.cartesian_iterator.activate(
                context,
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
            iterators: (0..iterator_executor_count).map(|_| Option::None).collect_vec(),
        }
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn activate(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        iterator_executors: &[InstructionExecutor],
        source_intersection: &[VariableValue<'static>],
        source_multiplicity: u64,
        intersection_iterators: &mut Vec<TupleIterator>,
    ) -> Result<(), ReadExecutionError> {
        debug_assert!(source_intersection.len() == self.intersection_source.len());
        self.is_active = true;
        self.intersection_source.clone_from_slice(source_intersection);
        self.intersection_multiplicity = source_multiplicity;

        // we are able to re-use existing iterators since they should only move forward. We only reset the indices
        self.cartesian_executor_indices.clear();

        let intersection = &source_intersection[self.sort_variable_position.as_usize()];
        for (index, iter) in intersection_iterators.iter_mut().enumerate() {
            if iter
                .peek_first_unbound_value()
                .transpose()
                .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                .is_some_and(|value| value == intersection)
            {
                self.cartesian_executor_indices.push(index);

                // reopen/move existing cartesian iterators forward to the intersection point
                let preexisting_iterator = self.iterators[index].take();
                let iterator = match preexisting_iterator {
                    None => self.reopen_iterator(context, &iterator_executors[index])?,
                    Some(mut iter) => {
                        // TODO: use seek()
                        let next_value_cmp = iter
                            .advance_until_index_is(iter.first_unbound_index(), intersection)
                            .map_err(|err| ReadExecutionError::ConceptRead { source: err })?;
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
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        executors: &[InstructionExecutor],
    ) -> Result<bool, ReadExecutionError> {
        debug_assert!(self.is_active);
        // precondition: all required iterators are open to the intersection point

        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        let mut executor_index = self.cartesian_executor_indices.len() - 1;
        loop {
            let iterator_index = self.cartesian_executor_indices[executor_index];
            let iter = (&mut self.iterators)[iterator_index].as_mut().unwrap();
            iter.advance_single().map_err(|err| ReadExecutionError::ConceptRead { source: err })?;
            if !iter
                .peek_first_unbound_value()
                .transpose()
                .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                .is_some_and(|value| value == intersection)
            {
                if executor_index == 0 {
                    self.is_active = false;
                    return Ok(false);
                } else {
                    let reopened = self.reopen_iterator(context, &executors[executor_index])?;
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
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        executor: &InstructionExecutor,
    ) -> Result<TupleIterator, ReadExecutionError> {
        let mut reopened = executor
            .get_iterator(
                context,
                MaybeOwnedRow::new_borrowed(&self.intersection_source, &self.intersection_multiplicity),
            )
            .map_err(|err| ReadExecutionError::ConceptRead { source: err })?;
        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        // TODO: use seek()
        reopened
            .advance_until_index_is(reopened.first_unbound_index(), intersection)
            .map_err(|err| ReadExecutionError::AdvancingIteratorTo { source: err })?;
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
    iterate: ConstraintInstruction<VariablePosition>,
    checks: Vec<ConstraintInstruction<VariablePosition>>,

    output_width: u32,
    output: Option<FixedBatch>,
}

impl UnsortedJoinExecutor {
    fn new(
        iterate: ConstraintInstruction<VariablePosition>,
        checks: Vec<ConstraintInstruction<VariablePosition>>,
        total_vars: u32,
    ) -> Self {
        Self { iterate, checks, output_width: total_vars, output: None }
    }

    fn batch_from(&mut self, input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Option<FixedBatch> {
        todo!()
    }
}

struct AssignExecutor {
    // executor: AssignInstruction,
    // checks: Vec<CheckInstruction>,
}

impl AssignExecutor {
    fn batch_from(&mut self, input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
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

    fn batch_from(&mut self, input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
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

    fn batch_from(&mut self, input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
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

    fn batch_from(&mut self, input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }
}
