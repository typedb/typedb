/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{hash_set, HashMap, HashSet},
    sync::Arc,
};

use answer::variable_value::VariableValue;
use compiler::{
    executable::match_::{
        instructions::{CheckInstruction, ConstraintInstruction, VariableModes},
        planner::match_executable::{
            AssignmentStep, CheckStep, DisjunctionStep, ExecutionStep, IntersectionStep, MatchExecutable, NegationStep,
            OptionalStep, UnsortedJoinStep,
        },
    },
    ExecutorVariable, VariablePosition,
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

pub(super) enum StepExecutor {
    SortedJoin(IntersectionExecutor),
    UnsortedJoin(UnsortedJoinExecutor),
    Assignment(AssignExecutor),
    Check(CheckExecutor),

    Disjunction(DisjunctionExecutor),
    Negation(NegationExecutor),
    Optional(OptionalExecutor),
}

impl StepExecutor {
    pub(super) fn new(
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
            ExecutionStep::Check(CheckStep { check_instructions, selected_variables, output_width }) => Ok(
                Self::Check(CheckExecutor::new(check_instructions.clone(), selected_variables.clone(), *output_width)),
            ),
            ExecutionStep::Disjunction(DisjunctionStep { branches, .. }) => {
                Ok(Self::Disjunction(DisjunctionExecutor::new(branches.clone(), row_width)))
            }
            ExecutionStep::Negation(NegationStep { negation: negation_plan, selected_variables, output_width }) => {
                // TODO: add limit 1, filters if they aren't there already?
                Ok(Self::Negation(NegationExecutor::new(
                    negation_plan.clone(),
                    selected_variables.clone(),
                    *output_width,
                )))
            }
            ExecutionStep::Optional(OptionalStep { optional: _optional_plan, .. }) => {
                todo!()
                // let pattern_executor = PatternExecutor::new(optional_plan, snapshot, thing_manager)?;
                // Ok(Self::Optional(OptionalExecutor::new(pattern_executor)))
            }
        }
    }

    pub(super) fn batch_from(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            StepExecutor::SortedJoin(sorted) => sorted.batch_from(input_batch, context),
            StepExecutor::UnsortedJoin(unsorted) => unsorted.batch_from(input_batch),
            StepExecutor::Assignment(single) => single.batch_from(input_batch),
            StepExecutor::Check(check) => check.batch_from(input_batch, context, interrupt),
            StepExecutor::Disjunction(disjunction) => disjunction.batch_from(input_batch, context, interrupt),
            StepExecutor::Negation(negation) => negation.batch_from(input_batch, context, interrupt),
            StepExecutor::Optional(optional) => optional.batch_from(input_batch),
        }
    }

    pub(super) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            StepExecutor::SortedJoin(sorted) => sorted.batch_continue(context),
            StepExecutor::UnsortedJoin(_unsorted) => todo!(), // unsorted.batch_continue(snapshot, thing_manager),
            StepExecutor::Disjunction(disjunction) => disjunction.batch_continue(context, interrupt),
            StepExecutor::Optional(_optional) => todo!(),
            StepExecutor::Assignment(_) | StepExecutor::Check(_) | StepExecutor::Negation(_) => Ok(None),
        }
    }
}

/// Performs an n-way intersection/join using sorted iterators.
/// To avoid missing cartesian outputs when multiple variables are unbound, the executor can leverage a
/// Cartesian sub-program, which generates all cartesian answers within one intersection, if there are any.
pub(super) struct IntersectionExecutor {
    instruction_executors: Vec<InstructionExecutor>,
    output_width: u32,
    outputs_selected: SelectedPositions,

    iterators: Vec<TupleIterator>,
    cartesian_iterator: CartesianIterator,
    input: Option<Peekable<FixedBatchRowIterator>>,

    intersection_value: VariableValue<'static>,
    intersection_row: Vec<VariableValue<'static>>,
    intersection_multiplicity: u64,

    output: Option<FixedBatch>,
}

impl IntersectionExecutor {
    fn new(
        sort_variable: ExecutorVariable,
        instructions: Vec<(ConstraintInstruction<ExecutorVariable>, VariableModes)>,
        output_width: u32,
        select_variables: Vec<VariablePosition>,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<Self, ConceptReadError> {
        let instruction_count = instructions.len();
        let executors: Vec<InstructionExecutor> = instructions
            .into_iter()
            .map(|(instruction, variable_modes)| {
                InstructionExecutor::new(instruction, variable_modes, &**snapshot, thing_manager, sort_variable)
            })
            .try_collect()?;

        Ok(Self {
            instruction_executors: executors,
            output_width,
            outputs_selected: SelectedPositions::new(select_variables),
            iterators: Vec::with_capacity(instruction_count),
            cartesian_iterator: CartesianIterator::new(output_width as usize, instruction_count),
            input: None,
            intersection_value: VariableValue::Empty,
            intersection_row: vec![VariableValue::Empty; output_width as usize],
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
                let iter = &mut self.iterators[0];
                while iter
                    .peek_first_unbound_value()
                    .transpose()
                    .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                    .is_some_and(|value| value == &self.intersection_value)
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
                    Ordering::Equal => (),
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
                            Some(Ordering::Equal) => (),
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
        self.intersection_value = VariableValue::Empty;
        self.intersection_row.fill(VariableValue::Empty);
        let mut row = Row::new(&mut self.intersection_row, &mut self.intersection_multiplicity);
        for iter in &mut self.iterators {
            if !self.intersection_value.is_empty() {
                iter.peek_first_unbound_value()
                    .transpose()
                    .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                    .inspect(|&value| assert_eq!(value, &self.intersection_value));
            } else {
                let value = iter
                    .peek_first_unbound_value()
                    .transpose()
                    .map_err(|err| ReadExecutionError::ConceptRead { source: err })?;
                if let Some(value) = value {
                    self.intersection_value = value.to_owned();
                }
            }
            iter.write_values(&mut row)
        }
        assert!(!self.intersection_value.is_empty());

        let input_row = self.input.as_mut().unwrap().peek().unwrap().as_ref().map_err(|&err| err.clone())?;
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
                .is_some_and(|value| value == &self.intersection_value)
            {
                cartesian = true;
                break;
            }
        }
        if cartesian {
            self.cartesian_iterator.activate(
                context,
                &self.instruction_executors,
                &self.intersection_value,
                &self.intersection_row,
                self.intersection_multiplicity,
                &mut self.iterators,
            )?
        }
        Ok(())
    }
}

struct CartesianIterator {
    is_active: bool,
    intersection_value: VariableValue<'static>,
    intersection_source: Vec<VariableValue<'static>>,
    intersection_multiplicity: u64,
    cartesian_executor_indices: Vec<usize>,
    iterators: Vec<Option<TupleIterator>>,
}

impl CartesianIterator {
    fn new(width: usize, iterator_executor_count: usize) -> Self {
        CartesianIterator {
            is_active: false,
            intersection_value: VariableValue::Empty,
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
        source_intersection_value: &VariableValue<'static>,
        source_intersection: &[VariableValue<'static>],
        source_multiplicity: u64,
        intersection_iterators: &mut [TupleIterator],
    ) -> Result<(), ReadExecutionError> {
        debug_assert!(source_intersection.len() == self.intersection_source.len());
        self.is_active = true;
        self.intersection_source.clone_from_slice(source_intersection);
        self.intersection_value = source_intersection_value.clone();
        self.intersection_multiplicity = source_multiplicity;

        // we are able to re-use existing iterators since they should only move forward. We only reset the indices
        self.cartesian_executor_indices.clear();

        for (index, iter) in intersection_iterators.iter_mut().enumerate() {
            if iter
                .peek_first_unbound_value()
                .transpose()
                .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                .is_some_and(|value| value == source_intersection_value)
            {
                self.cartesian_executor_indices.push(index);

                // reopen/move existing cartesian iterators forward to the intersection point
                let preexisting_iterator = self.iterators[index].take();
                let iterator = match preexisting_iterator {
                    None => self.reopen_iterator(context, &iterator_executors[index])?,
                    Some(mut iter) => {
                        // TODO: use seek()
                        let next_value_cmp = iter
                            .advance_until_index_is(iter.first_unbound_index(), source_intersection_value)
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

        let mut executor_index = self.cartesian_executor_indices.len() - 1;
        loop {
            let iterator_index = self.cartesian_executor_indices[executor_index];
            let iter = self.iterators[iterator_index].as_mut().unwrap();
            iter.advance_single().map_err(|err| ReadExecutionError::ConceptRead { source: err })?;
            if !iter
                .peek_first_unbound_value()
                .transpose()
                .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
                .is_some_and(|value| value == &self.intersection_value)
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
        // TODO: use seek()
        reopened
            .advance_until_index_is(reopened.first_unbound_index(), &self.intersection_value)
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

pub(super) struct UnsortedJoinExecutor {
    iterate: ConstraintInstruction<ExecutorVariable>,
    checks: Vec<ConstraintInstruction<ExecutorVariable>>,

    output_width: u32,
    output: Option<FixedBatch>,
}

impl UnsortedJoinExecutor {
    fn new(
        iterate: ConstraintInstruction<ExecutorVariable>,
        checks: Vec<ConstraintInstruction<ExecutorVariable>>,
        total_vars: u32,
    ) -> Self {
        Self { iterate, checks, output_width: total_vars, output: None }
    }

    fn batch_from(&mut self, _input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Option<FixedBatch> {
        todo!()
    }
}

pub(super) struct AssignExecutor {
    // executor: AssignInstruction,
    // checks: Vec<CheckInstruction>,
}

impl AssignExecutor {
    fn batch_from(&mut self, _input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }
}

pub(super) struct CheckExecutor {
    checker: Checker<()>,
    selected_variables: Vec<VariablePosition>,
    output_width: u32,
}

impl CheckExecutor {
    fn new(
        checks: Vec<CheckInstruction<ExecutorVariable>>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
    ) -> Self {
        let checker = Checker::new(checks, HashMap::new());
        Self { checker, selected_variables, output_width }
    }

    fn batch_from(
        &self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        _interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let mut input = Peekable::new(FixedBatchRowIterator::new(Ok(input_batch)));
        debug_assert!(input.peek().is_some());

        let mut output = FixedBatch::new(self.output_width);

        while let Some(row) = input.next() {
            let input_row = row.map_err(|err| err.clone())?;
            if (self.checker.filter_for_row(context, &input_row))(&Ok(()))
                .map_err(|err| ReadExecutionError::ConceptRead { source: err })?
            {
                output.append(|mut row| {
                    row.set_multiplicity(input_row.multiplicity());
                    for &position in &self.selected_variables {
                        row.set(position, input_row.get(position).clone().into_owned())
                    }
                })
            }
        }

        Ok(Some(output))
    }
}

pub(super) struct DisjunctionExecutor {
    branches: Vec<MatchExecutable>,

    current_iterator: Option<hash_set::IntoIter<MaybeOwnedRow<'static>>>,

    input: Option<Peekable<FixedBatchRowIterator>>,
    output: Option<FixedBatch>,

    output_width: u32, // we should at least make sure all branches have the same batch width
}

impl DisjunctionExecutor {
    fn new(branches: Vec<MatchExecutable>, output_width: u32) -> DisjunctionExecutor {
        assert!(branches.iter().all(|executable| executable.outputs().len() == output_width as usize));
        Self { branches, current_iterator: None, input: None, output: None, output_width }
    }

    fn batch_from(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        debug_assert!(
            self.output.is_none()
                && self.current_iterator.is_none()
                && !self.input.as_mut().is_some_and(|it| it.peek().is_some())
        );
        self.input = Some(Peekable::new(FixedBatchRowIterator::new(Ok(input_batch))));
        debug_assert!(self.input.as_mut().unwrap().peek().is_some());
        self.batch_continue(context, interrupt)
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        debug_assert!(self.output.is_none());
        self.compute_next_batch(context, interrupt)?;
        Ok(self.output.take())
    }

    fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<(), ReadExecutionError> {
        let mut batch = FixedBatch::new(self.output_width);
        while !batch.is_full() {
            if let Some(iterator) = &mut self.current_iterator {
                let next = iterator.next();
                match next {
                    Some(output_row) => {
                        batch.append(|mut row| row.copy_from(output_row.row(), output_row.multiplicity()))
                    }
                    None => self.current_iterator = None,
                }
            } else {
                self.current_iterator = self.initialize_executor_for_next_input_row(context, interrupt)?;
                if self.current_iterator.is_none() {
                    break;
                }
            }
        }
        if !batch.is_empty() {
            self.output = Some(batch);
        }
        Ok(())
    }

    fn initialize_executor_for_next_input_row(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<hash_set::IntoIter<MaybeOwnedRow<'static>>>, ReadExecutionError> {
        let Some(input_row) = self.input.as_mut().unwrap().next().transpose().map_err(|err| err.clone())? else {
            return Ok(None);
        };

        let branch_iters: Vec<_> = self
            .branches
            .iter()
            .map(|branch| {
                MatchExecutor::new(branch, context.snapshot(), context.thing_manager(), input_row.clone())
                    .map_err(|err| ReadExecutionError::ConceptRead { source: err })
            })
            .try_collect()?;

        #[allow(
            clippy::mutable_key_type,
            reason = "VariableValue may contain Attribute, which has an internally mutable value cache"
        )]
        let rows: HashSet<_> = branch_iters
            .into_iter()
            .flat_map(|branch_iter| {
                branch_iter
                    .into_iterator(context.clone(), interrupt.clone())
                    .map_static(|row| match row {
                        Ok(row) => Ok(row.into_owned()),
                        Err(err) => Err(err.clone()),
                    })
                    .into_iter()
            })
            .try_collect()?;

        Ok(Some(rows.into_iter()))
    }
}

pub(super) struct NegationExecutor {
    executable: MatchExecutable,
    selected_variables: Vec<VariablePosition>,
    output_width: u32,
}

impl NegationExecutor {
    fn new(negation_plan: MatchExecutable, selected_variables: Vec<VariablePosition>, output_width: u32) -> Self {
        Self { executable: negation_plan, selected_variables, output_width }
    }

    fn batch_from(
        &self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let mut input = Peekable::new(FixedBatchRowIterator::new(Ok(input_batch)));
        debug_assert!(input.peek().is_some());

        let mut output = FixedBatch::new(self.output_width);

        while let Some(row) = input.next() {
            let input_row = row.map_err(|err| err.clone())?;
            let executor =
                MatchExecutor::new(&self.executable, context.snapshot(), context.thing_manager(), input_row.clone())
                    .map_err(|error| ReadExecutionError::ConceptRead { source: error })?;
            let mut iterator = executor.into_iterator(context.clone(), interrupt.clone());
            if iterator.next().transpose().map_err(|err| err.clone())?.is_none() {
                output.append(|mut row| {
                    row.set_multiplicity(input_row.multiplicity());
                    for &position in &self.selected_variables {
                        row.set(position, input_row.get(position).clone().into_owned())
                    }
                })
            }
        }

        Ok(Some(output))
    }
}

pub(super) struct OptionalExecutor {
    executor: MatchExecutor,
}

impl OptionalExecutor {
    fn new(executor: MatchExecutor) -> OptionalExecutor {
        Self { executor }
    }

    fn batch_from(&mut self, input_batch: FixedBatch) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }

    fn batch_continue(&mut self) -> Result<Option<FixedBatch>, ReadExecutionError> {
        todo!()
    }
}
