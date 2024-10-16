/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use answer::{variable::Variable, variable_value::VariableValue};
use compiler::{
    executable::match_::{
        instructions::{CheckInstruction, ConstraintInstruction, VariableModes},
        planner::match_executable::{
            AssignmentStep, CheckStep, DisjunctionStep, ExecutionStep, IntersectionStep, MatchExecutable, NegationStep,
            OptionalStep, UnsortedJoinStep,
        },
    },
    VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use itertools::Itertools;
use lending_iterator::{LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    instruction::{iterator::TupleIterator, Checker, InstructionExecutor},
    pattern_executor::MatchExecutor,
    pipeline::stage::ExecutionContext,
    row::{MaybeOwnedRow, Row},
    ExecutionInterrupt, SelectedPositions,
};

pub(crate) enum StepExecutor {
    SortedJoin(IntersectionExecutor),
    UnsortedJoin(UnsortedJoinExecutor),
    Assignment(AssignExecutor),
    Check(CheckExecutor),

    Negation(NegationExecutor),
}

impl StepExecutor {
    pub(crate) fn new(
        step: &ExecutionStep,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let row_width = step.output_width();
        match step {
            ExecutionStep::Intersection(IntersectionStep {
                sort_variable, instructions, selected_variables, ..
            }) => {
                let executor = IntersectionExecutor::new(
                    *sort_variable,
                    instructions.clone(),
                    row_width,
                    selected_variables.clone(),
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::SortedJoin(executor))
            }
            ExecutionStep::UnsortedJoin(UnsortedJoinStep { iterate_instruction, check_instructions, .. }) => {
                let executor =
                    UnsortedJoinExecutor::new(iterate_instruction.clone(), check_instructions.clone(), row_width);
                Ok(Self::UnsortedJoin(executor))
            }
            ExecutionStep::Assignment(AssignmentStep { .. }) => {
                todo!()
            }
            ExecutionStep::Check(CheckStep { check_instructions }) => {
                Ok(Self::Check(CheckExecutor::new(check_instructions.clone())))
            }

            ExecutionStep::Negation(_) | ExecutionStep::Disjunction(_) | ExecutionStep::Optional(_) => {
                todo!("deprecate")
                // let pattern_executor = PatternExecutor::new(optional_plan, snapshot, thing_manager)?;
                // Ok(Self::Optional(OptionalExecutor::new(pattern_executor)))
            }
        }
    }

    pub(crate) fn prepare(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        match self {
            StepExecutor::SortedJoin(sorted) => sorted.prepare(input_batch, context),
            StepExecutor::UnsortedJoin(unsorted) => unsorted.prepare(input_batch, context),
            StepExecutor::Assignment(assignment) => assignment.prepare(input_batch, context),
            StepExecutor::Check(check) => check.prepare(input_batch, context),
            _ => todo!("Deprecate the others"),
        }
    }

    pub(crate) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            StepExecutor::SortedJoin(sorted) => sorted.batch_continue(context, interrupt),
            StepExecutor::UnsortedJoin(_unsorted) => todo!(), // unsorted.batch_continue(context, interrupt),
            StepExecutor::Assignment(assignment) => assignment.batch_continue(context, interrupt),
            StepExecutor::Check(check) => check.batch_continue(context, interrupt),
            _ => todo!("deprecate"),
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
        instructions: Vec<(ConstraintInstruction<VariablePosition>, VariableModes)>,
        output_width: u32,
        select_variables: Vec<VariablePosition>,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let instruction_count = instructions.len();
        let executors: Vec<InstructionExecutor> = instructions
            .into_iter()
            .map(|(instruction, variable_modes)| {
                InstructionExecutor::new(instruction, variable_modes, &**snapshot, thing_manager, Some(sort_variable))
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

    fn prepare(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        debug_assert!(self.output.is_none() && (self.input.is_none() || self.input.as_mut().unwrap().peek().is_none()));
        self.input = Some(Peekable::new(FixedBatchRowIterator::new(Ok(input_batch))));
        debug_assert!(self.input.as_mut().unwrap().peek().is_some());
        self.may_create_intersection_iterators(context)?;
        Ok(())
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
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

    fn prepare(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<(), ReadExecutionError> {
        todo!()
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }
}

struct AssignExecutor {
    // input: FixedBatch,
    // executor: AssignInstruction,
    // checks: Vec<CheckInstruction>,
}

impl AssignExecutor {
    fn prepare(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<(), ReadExecutionError> {
        todo!()
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }
}

struct CheckExecutor {
    checker: Checker<()>,
    input: Option<FixedBatch>,
}

impl CheckExecutor {
    fn new(checks: Vec<CheckInstruction<VariablePosition>>) -> Self {
        let checker = Checker::new(checks, HashMap::new());
        Self { checker, input: None }
    }

    fn prepare(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        self.input = Some(input_batch);
        Ok(())
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        _interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let Some(input_batch) = self.input.take() else {
            return Ok(None);
        };

        let width = input_batch.width();
        let mut input = Peekable::new(FixedBatchRowIterator::new(Ok(input_batch)));
        debug_assert!(input.peek().is_some());

        let mut output = FixedBatch::new(width);

        while let Some(row) = input.next() {
            let input_row = row.map_err(|err| err.clone())?;
            if (self.checker.filter_for_row(context, &input_row))(&Ok(()))
                .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
            {
                output.append(|mut row| row.copy_from(input_row.row(), input_row.multiplicity()))
            }
        }

        Ok(Some(output))
    }
}

struct NegationExecutor {
    executable: MatchExecutable,
}

impl NegationExecutor {
    fn new(negation_plan: MatchExecutable) -> Self {
        Self { executable: negation_plan }
    }

    fn batch_from(
        &self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let width = input_batch.width();
        let mut input = Peekable::new(FixedBatchRowIterator::new(Ok(input_batch)));
        debug_assert!(input.peek().is_some());

        let mut output = FixedBatch::new(width);

        while let Some(row) = input.next() {
            let input_row = row.map_err(|err| err.clone())?;
            let executor =
                MatchExecutor::new(&self.executable, context.snapshot(), context.thing_manager(), input_row.clone())
                    .map_err(|error| ReadExecutionError::ConceptRead { source: error })?;
            let mut iterator = executor.into_iterator(context.clone(), interrupt.clone());
            if iterator.next().transpose().map_err(|err| err.clone())?.is_none() {
                output.append(|mut row| row.copy_from(input_row.row(), input_row.multiplicity()))
            }
        }

        Ok(Some(output))
    }
}
