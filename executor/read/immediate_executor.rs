/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap, fmt, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::{
    annotation::expression::compiled_expression::ExecutableExpression,
    executable::match_::{
        instructions::{CheckInstruction, ConstraintInstruction, VariableModes},
        planner::match_executable::{AssignmentStep, CheckStep, IntersectionStep, UnsortedJoinStep},
    },
    ExecutorVariable, VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use error::{unimplemented_feature, UnimplementedFeature};
use itertools::Itertools;
use lending_iterator::{LendingIterator, Peekable};
use resource::profile::StepProfile;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::{FixedBatch, FixedBatchRowIterator},
    error::ReadExecutionError,
    instruction::{iterator::TupleIterator, Checker, InstructionExecutor},
    pipeline::stage::ExecutionContext,
    read::{
        expression_executor::{evaluate_expression, ExpressionValue},
        step_executor::StepExecutors,
    },
    row::{MaybeOwnedRow, Row},
    ExecutionInterrupt, Provenance, SelectedPositions,
};

#[derive(Debug)]
pub(crate) enum ImmediateExecutor {
    SortedJoin(IntersectionExecutor),
    UnsortedJoin(UnsortedJoinExecutor),
    Check(CheckExecutor),
    Assignment(AssignExecutor),
}

impl From<ImmediateExecutor> for StepExecutors {
    fn from(val: ImmediateExecutor) -> Self {
        StepExecutors::Immediate(val)
    }
}

impl ImmediateExecutor {
    pub(crate) fn new_intersection(
        step: &IntersectionStep,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        profile: Arc<StepProfile>,
    ) -> Result<Self, Box<ConceptReadError>> {
        let IntersectionStep { sort_variable, instructions, selected_variables, output_width, .. } = step;

        let executor = IntersectionExecutor::new(
            *sort_variable,
            instructions.clone(),
            *output_width,
            selected_variables.clone(),
            snapshot,
            thing_manager,
            profile,
        )?;
        Ok(Self::SortedJoin(executor))
    }

    pub(crate) fn new_unsorted_join(
        step: &UnsortedJoinStep,
        step_profile: Arc<StepProfile>,
    ) -> Result<Self, Box<ConceptReadError>> {
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: UnimplementedFeature::UnsortedJoin,
        }));
        let UnsortedJoinStep { iterate_instruction, check_instructions, output_width, .. } = step;
        let executor = UnsortedJoinExecutor::new(
            iterate_instruction.clone(),
            check_instructions.clone(),
            *output_width,
            step_profile,
        );
        Ok(Self::UnsortedJoin(executor))
    }

    pub(crate) fn new_assignment(
        step: &AssignmentStep,
        step_profile: Arc<StepProfile>,
    ) -> Result<Self, Box<ConceptReadError>> {
        let AssignmentStep { expression, input_positions, unbound, selected_variables, output_width } = step;
        Ok(Self::Assignment(AssignExecutor::new(
            expression.clone(),
            input_positions.clone(),
            *unbound,
            selected_variables.clone(),
            *output_width,
            step_profile,
        )))
    }

    pub(crate) fn new_check(step: &CheckStep, step_profile: Arc<StepProfile>) -> Result<Self, Box<ConceptReadError>> {
        let CheckStep { check_instructions, selected_variables, output_width } = step;
        Ok(Self::Check(CheckExecutor::new(
            check_instructions.clone(),
            selected_variables.clone(),
            *output_width,
            step_profile,
        )))
    }

    pub(crate) fn reset(&mut self) {
        match self {
            ImmediateExecutor::SortedJoin(sorted) => sorted.reset(),
            ImmediateExecutor::UnsortedJoin(unsorted) => unsorted.reset(),
            ImmediateExecutor::Assignment(assignment) => assignment.reset(),
            ImmediateExecutor::Check(check) => check.reset(),
        }
    }

    pub(crate) fn prepare(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        match self {
            ImmediateExecutor::SortedJoin(sorted) => sorted.prepare(input_batch, context),
            ImmediateExecutor::UnsortedJoin(unsorted) => unsorted.prepare(input_batch, context),
            ImmediateExecutor::Assignment(assignment) => assignment.prepare(input_batch, context),
            ImmediateExecutor::Check(check) => check.prepare(input_batch, context),
        }
    }

    pub(crate) fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        match self {
            ImmediateExecutor::SortedJoin(sorted) => sorted.batch_continue(context, interrupt),
            ImmediateExecutor::UnsortedJoin(unsorted) => unsorted.batch_continue(context, interrupt),
            ImmediateExecutor::Assignment(assignment) => assignment.batch_continue(context, interrupt),
            ImmediateExecutor::Check(check) => check.batch_continue(context, interrupt),
        }
    }
}

/// Performs an n-way intersection/join using sorted iterators.
/// To avoid missing cartesian outputs when multiple variables are unbound, the executor can leverage a
/// Cartesian sub-program, which generates all cartesian answers within one intersection, if there are any.
pub(crate) struct IntersectionExecutor {
    instruction_executors: Vec<InstructionExecutor>,
    output_width: u32,
    outputs_selected: SelectedPositions,

    iterators: Vec<TupleIterator>,
    cartesian_iterator: CartesianIterator,
    input: Option<Peekable<FixedBatchRowIterator>>,

    intersection_value: VariableValue<'static>,
    intersection_row: Vec<VariableValue<'static>>,
    intersection_multiplicity: u64,
    intersection_provenance: Provenance,

    profile: Arc<StepProfile>,
}

impl fmt::Debug for IntersectionExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntersectionExecutor (instruction = {:?})", self.instruction_executors)
    }
}

impl IntersectionExecutor {
    fn new(
        sort_variable: ExecutorVariable,
        instructions: Vec<(ConstraintInstruction<ExecutorVariable>, VariableModes)>,
        output_width: u32,
        select_variables: Vec<VariablePosition>,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        profile: Arc<StepProfile>,
    ) -> Result<Self, Box<ConceptReadError>> {
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
            cartesian_iterator: CartesianIterator::new(output_width as usize, instruction_count, profile.clone()),
            input: None,
            intersection_value: VariableValue::Empty,
            intersection_row: vec![VariableValue::Empty; output_width as usize],
            intersection_multiplicity: 1,
            intersection_provenance: Provenance::INITIAL,
            profile,
        })
    }

    fn reset(&mut self) {
        self.input = None;
        self.iterators.clear();
    }

    fn prepare(
        &mut self,
        input_batch: FixedBatch,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        let measurement = self.profile.start_measurement();
        debug_assert!(self.input.is_none() || self.input.as_mut().unwrap().peek().is_none());
        self.input = Some(Peekable::new(FixedBatchRowIterator::new(Ok(input_batch))));
        debug_assert!(self.input.as_mut().unwrap().peek().is_some());
        self.may_create_intersection_iterators(context)?;
        measurement.end(&self.profile, 0, 0);
        Ok(())
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        _interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        self.may_compute_next_batch(context)
    }

    fn may_compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let measurement = self.profile.start_measurement();
        let output = if self.compute_next_row(context)? {
            // don't allocate batch until 1 answer is confirmed
            let mut batch = FixedBatch::new(self.output_width);
            batch.append(|mut row| self.write_next_row_into(&mut row));
            while !batch.is_full() && self.compute_next_row(context)? {
                batch.append(|mut row| self.write_next_row_into(&mut row));
            }
            Some(batch)
        } else {
            None
        };
        measurement.end(&self.profile, 1, output.as_ref().map(|batch| batch.len()).unwrap_or(0) as u64);
        Ok(output)
    }

    fn write_next_row_into(&mut self, row: &mut Row<'_>) {
        if self.cartesian_iterator.is_active() {
            self.cartesian_iterator.write_into(row, &self.outputs_selected);
        } else {
            row.set_multiplicity(self.intersection_multiplicity);
            for &position in &self.outputs_selected.selected {
                let value = self.intersection_row[position.as_usize()].clone();
                row.set(position, value);
            }
        }
        row.set_provenance(self.intersection_provenance);
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
                    .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?
                    .is_some_and(|value| value == &self.intersection_value)
                {
                    iter.advance_single().map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?;
                }
                self.compute_next_row(context)
            }
        } else {
            while self.input.as_mut().unwrap().peek().is_some() {
                let found = self.find_intersection()?;
                if found {
                    self.record_intersection()?;
                    self.advance_intersection_iterators_with_multiplicity()?;
                    self.may_activate_cartesian(context)?;
                    return Ok(true);
                } else {
                    self.iterators.clear();
                    self.cartesian_iterator.clear();
                    while self.iterators.is_empty() {
                        let _ = self.input.as_mut().unwrap().next().unwrap().map_err(|err| err.clone());
                        if self.input.as_mut().unwrap().peek().is_some() {
                            self.may_create_intersection_iterators(context)?;
                        } else {
                            break;
                        }
                    }
                }
            }
            Ok(false)
        }
    }

    fn find_intersection(&mut self) -> Result<bool, ReadExecutionError> {
        if self.iterators.is_empty() {
            return Ok(false);
        } else if self.iterators.len() == 1 {
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
                let iterator = &mut containing_max[max_index];
                let current_max = iterator.peek_first_unbound_value().unwrap().unwrap();
                let max_cmp_peek = match containing_i[i_index].peek_first_unbound_value() {
                    None => {
                        failed = true;
                        break;
                    }
                    Some(Ok(value)) => current_max.partial_cmp(value).unwrap(),
                    Some(Err(err)) => return Err(ReadExecutionError::ConceptRead { typedb_source: err.clone() }),
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
                            .advance_until_first_unbound_is(current_max)
                            .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?;
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
            self.intersection_provenance = next_row.provenance();
            for executor in &self.instruction_executors {
                let mut iterator = executor
                    .get_iterator(context, next_row.as_reference(), self.profile.storage_counters())
                    .map_err(|err| ReadExecutionError::CreatingIterator {
                        instruction_name: executor.name().to_string(),
                        typedb_source: err,
                    })?;
                if iterator.peek().is_none() {
                    self.iterators.clear();
                    return Ok(());
                }
                self.iterators.push(iterator);
            }
        }
        Ok(())
    }

    fn advance_intersection_iterators_with_multiplicity(&mut self) -> Result<(), ReadExecutionError> {
        // TODO: there's room for optimisation here:
        //       since we use iterators that hide their filtering/skipping conditions, it's possible we
        //       end up iterating internally over far too many keys! For example Has[$owner, $attr] where $attr is of type Age
        //       However, after we find the last Owner1+Age pair, every other Has is of Owner1,2,3...+Name! We will iterate over ALL
        //       Possible Has's until we run out. In reality, we just need the raw iterator to advance past the current Owner1
        //       --> This can then be utilised to short circuit when advancing multiple intersection iterators:
        //       If 1 iterator has no more answers after Owner1, then the other also just has to finish the Owner1 count
        //       and we can short-circuit evaluating this set of iterators based on the current input!
        let mut multiplicity: u64 = 1;
        for iter in &mut self.iterators {
            multiplicity *=
                iter.advance_past().map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })? as u64;
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
        let mut provenance = Provenance::INITIAL;
        let mut row = Row::new(&mut self.intersection_row, &mut self.intersection_multiplicity, &mut provenance);
        for iter in &mut self.iterators {
            if !self.intersection_value.is_empty() {
                iter.peek_first_unbound_value()
                    .transpose()
                    .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?
                    .inspect(|&value| assert_eq!(value, &self.intersection_value));
            } else {
                let value = iter
                    .peek_first_unbound_value()
                    .transpose()
                    .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?;
                if let Some(value) = value {
                    self.intersection_value = value.to_owned();
                }
            }
            iter.write_values(&mut row)
        }
        assert!(!self.intersection_value.is_empty());

        let input_row = self.input.as_mut().unwrap().peek().unwrap().as_ref().map_err(|&err| err.clone())?;
        for &position in &self.outputs_selected {
            // note: some input variable positions are re-used across stages, so we should only copy
            //       inputs into the output row if it is not already populated by the intersection
            if position.as_usize() < input_row.len()
                && !input_row.get(position).is_empty()
                && row.get(position).is_empty()
            {
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
                .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?
                .is_some_and(|value| value == &self.intersection_value)
            {
                cartesian = true;
                break;
            }
        }
        let Some(Ok(input_row)) = self.input.as_mut().unwrap().peek() else {
            unreachable!("We had to get the input row to get to this point")
        };
        if cartesian {
            self.cartesian_iterator.activate(
                context,
                &self.instruction_executors,
                &self.intersection_value,
                input_row,
                &self.intersection_row,
                self.intersection_multiplicity,
                &mut self.iterators,
            )?
        }
        Ok(())
    }
}

// TODO: prefetch all data involved in the cartesian instead of pinging Rocks
struct CartesianIterator {
    is_active: bool,
    intersection_value: VariableValue<'static>,
    input_row: Vec<VariableValue<'static>>,
    intersection_source: Vec<VariableValue<'static>>,
    intersection_multiplicity: u64,
    cartesian_executor_indices: Vec<usize>,
    iterators: Vec<Option<TupleIterator>>,
    profile: Arc<StepProfile>,
}

impl CartesianIterator {
    fn new(width: usize, iterator_executor_count: usize, profile: Arc<StepProfile>) -> Self {
        CartesianIterator {
            is_active: false,
            intersection_value: VariableValue::Empty,
            input_row: vec![VariableValue::Empty; width],
            intersection_source: vec![VariableValue::Empty; width],
            intersection_multiplicity: 1,
            cartesian_executor_indices: Vec::with_capacity(iterator_executor_count),
            iterators: (0..iterator_executor_count).map(|_| Option::None).collect_vec(),
            profile,
        }
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn clear(&mut self) {
        self.iterators.iter_mut().for_each(|iter| drop(iter.take()));
    }

    fn activate(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        iterator_executors: &[InstructionExecutor],
        source_intersection_value: &VariableValue<'static>,
        input_row: &[VariableValue<'static>],
        source_intersection: &[VariableValue<'static>],
        source_multiplicity: u64,
        intersection_iterators: &mut [TupleIterator],
    ) -> Result<(), ReadExecutionError> {
        // TODO: there's room for an optimisation here: we don't have to re-open a new iterator when only have 1 cartesian iterator!
        //       we can just advance it linearly through the answers, and not cost another lookup
        debug_assert!(source_intersection.len() == self.intersection_source.len());
        self.is_active = true;
        self.input_row[..input_row.len()].clone_from_slice(input_row);
        self.intersection_source.clone_from_slice(source_intersection);
        self.intersection_value = source_intersection_value.clone();
        self.intersection_multiplicity = source_multiplicity;

        // we are able to re-use existing iterators since they should only move forward. We only reset the indices
        self.cartesian_executor_indices.clear();

        for (index, iter) in intersection_iterators.iter_mut().enumerate() {
            if iter
                .peek_first_unbound_value()
                .transpose()
                .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?
                .is_some_and(|value| value == source_intersection_value)
            {
                self.cartesian_executor_indices.push(index);

                // reopen/move existing cartesian iterators forward to the intersection point if we can
                let preexisting_iterator = self.iterators[index].take();
                let iterator = match preexisting_iterator {
                    None => self.reopen_iterator(context, &iterator_executors[index])?,
                    Some(mut iter) => match iter.peek_first_unbound_value() {
                        None => self.reopen_iterator(context, &iterator_executors[index])?,
                        Some(Ok(value)) => {
                            if value < source_intersection_value {
                                iter.advance_until_first_unbound_is(source_intersection_value)
                                    .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?;
                                debug_assert_eq!(
                                    iter.peek_first_unbound_value().unwrap().unwrap(),
                                    source_intersection_value
                                );
                                iter
                            } else if value == source_intersection_value {
                                iter
                            } else {
                                self.reopen_iterator(context, &iterator_executors[index])?
                            }
                        }
                        Some(Err(err)) => {
                            return Err(ReadExecutionError::ConceptRead { typedb_source: err });
                        }
                    },
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
            iter.advance_single().map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?;
            if !iter
                .peek_first_unbound_value()
                .transpose()
                .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?
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
        /*
        TODO: this re-opens an iterator to contribute towards a cartesian product.
              However, we only need values within the intersection. This 'bound' is not information we pass into the reopened iterator!

              Example: Has[person $x, age $a], normally called in the Unbound mode.
              However, now we know we are within the intersection for Person 1.

              Say data looks like this:

              Person 0, age 10;
              Person 0, name 'a';
              Person 1, age 11;
              Person 1, age 12;
              Person 1, name 'b';
              Person 2, name 'c';
              Person 3, name 'd';
              Person ...

              The iterator will be opened in Unbound mode, and we will seek it to person 1 in this method.
              However, it also has built-in filtering to ensure that we only see Person-Age combinations!

              As a result, we will correctly find Person1-Age11 and Person1-Age12 for the Cartesian output.
              However, after that, the iterator may not return anything if it doesn't encounter any more Age owners!!
              In this case, we will advance through Person2-NameC, Person3-NameD, ... either until the end of all Has-Person prefixes
              or we find another Person with an Age!

              Ideally, we could use the bound Person1 as input to the getIterator to make sure we stick in the right range.
         */
        let mut reopened = executor
            .get_iterator(
                context,
                MaybeOwnedRow::new_borrowed(&self.input_row, &1, &Provenance::INITIAL),
                self.profile.storage_counters(),
            )
            .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?;
        // TODO: use seek()
        reopened
            .advance_until_first_unbound_is(&self.intersection_value)
            .map_err(|err| ReadExecutionError::AdvancingIteratorTo { typedb_source: err })?;
        Ok(reopened)
    }

    fn write_into(&mut self, row: &mut Row<'_>, outputs_selected: &SelectedPositions) {
        for &executor_index in &self.cartesian_executor_indices {
            let iterator = self.iterators[executor_index].as_mut().unwrap();
            iterator.write_values(row);
        }
        for pos in (0..self.intersection_source.len() as u32)
            .map(VariablePosition::new)
            .filter(|i| !outputs_selected.selected.contains(i))
        {
            row.unset(pos);
        }
        for (index, value) in self.intersection_source.iter().enumerate() {
            if *row.get(VariablePosition::new(index as u32)) == VariableValue::Empty {
                row.set(VariablePosition::new(index as u32), value.clone());
            }
        }
        row.set_multiplicity(self.intersection_multiplicity);
    }
}

#[derive(Debug)]
pub(crate) struct UnsortedJoinExecutor {
    iterate: ConstraintInstruction<ExecutorVariable>,
    checks: Vec<ConstraintInstruction<ExecutorVariable>>,

    output_width: u32,
    output: Option<FixedBatch>,
    profile: Arc<StepProfile>,
}

impl UnsortedJoinExecutor {
    fn new(
        iterate: ConstraintInstruction<ExecutorVariable>,
        checks: Vec<ConstraintInstruction<ExecutorVariable>>,
        total_vars: u32,
        profile: Arc<StepProfile>,
    ) -> Self {
        Self { iterate, checks, output_width: total_vars, output: None, profile }
    }

    fn reset(&mut self) {
        unimplemented_feature!(UnsortedJoin)
    }

    fn prepare(
        &mut self,
        _input_batch: FixedBatch,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<(), ReadExecutionError> {
        unimplemented_feature!(UnsortedJoin)
    }

    fn batch_continue(
        &mut self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        _interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        unimplemented_feature!(UnsortedJoin)
    }
}

#[derive(Debug)]
pub(crate) struct AssignExecutor {
    expression: ExecutableExpression<VariablePosition>,
    inputs: Vec<VariablePosition>,
    output: ExecutorVariable,
    selected_variables: Vec<VariablePosition>,
    output_width: u32,
    profile: Arc<StepProfile>,

    prepared_input: Option<FixedBatch>,
}

impl AssignExecutor {
    fn new(
        expression: ExecutableExpression<VariablePosition>,
        inputs: Vec<VariablePosition>,
        output: ExecutorVariable,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
        profile: Arc<StepProfile>,
    ) -> Self {
        Self { expression, inputs, output, selected_variables, output_width, profile, prepared_input: None }
    }

    fn reset(&mut self) {
        self.prepared_input = None;
    }

    fn prepare(
        &mut self,
        input_batch: FixedBatch,
        _context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        self.prepared_input = Some(input_batch);
        Ok(())
    }

    fn batch_continue(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        _interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        if self.prepared_input.is_none() {
            return Ok(None);
        }
        let measurement = self.profile.start_measurement();
        let mut input = Peekable::new(FixedBatchRowIterator::new(Ok(self.prepared_input.take().unwrap())));
        debug_assert!(input.peek().is_some());
        let mut output = FixedBatch::new(self.output_width);

        while !output.is_full() {
            let Some(row) = input.next() else { break };
            let input_row = row.map_err(|err| err.clone())?;
            let input_variables = self
                .inputs
                .iter()
                .map(|&pos| {
                    let value = input_row.get(pos).to_owned();
                    let expression_value =
                        ExpressionValue::try_from_value(value, context, self.profile.storage_counters())
                            .map_err(|typedb_source| ReadExecutionError::ExpressionEvaluate { typedb_source })?;
                    Ok((pos, expression_value))
                })
                .try_collect()?;
            let output_value = evaluate_expression(&self.expression, input_variables, &context.parameters)
                .map_err(|typedb_source| ReadExecutionError::ExpressionEvaluate { typedb_source })?;
            output.append(|mut row| {
                row.set_multiplicity(input_row.multiplicity());
                for &position in &self.selected_variables {
                    if position.as_usize() < input_row.len() {
                        row.set(position, input_row.get(position).clone().into_owned());
                    }
                }
                if let Some(position) = self.output.as_position() {
                    row.set(position, output_value.into());
                }
            })
        }
        measurement.end(&self.profile, 1, output.len() as u64);

        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
}

pub(crate) struct CheckExecutor {
    checker: Checker<()>,
    selected_variables: Vec<VariablePosition>,
    output_width: u32,
    input: Option<FixedBatch>,
    profile: Arc<StepProfile>,
}

impl fmt::Debug for CheckExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CheckExecutor (with checks {:?})", self.checker.checks)
    }
}

impl CheckExecutor {
    fn new(
        checks: Vec<CheckInstruction<ExecutorVariable>>,
        selected_variables: Vec<VariablePosition>,
        output_width: u32,
        profile: Arc<StepProfile>,
    ) -> Self {
        let checker = Checker::new(checks, HashMap::new());
        Self { checker, selected_variables, output_width, input: None, profile }
    }

    fn reset(&mut self) {
        self.input = None;
    }

    fn prepare(
        &mut self,
        input_batch: FixedBatch,
        _context: &ExecutionContext<impl ReadableSnapshot + 'static>,
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
        let measurement = self.profile.start_measurement();
        let mut input = Peekable::new(FixedBatchRowIterator::new(Ok(input_batch)));
        debug_assert!(input.peek().is_some());

        let mut output = FixedBatch::new(self.output_width);

        while let Some(row) = input.next() {
            let input_row = row.map_err(|err| err.clone())?;
            if self.checker.filter_for_row(context, &input_row, self.profile.storage_counters())(&Ok(()))
                .map_err(|err| ReadExecutionError::ConceptRead { typedb_source: err })?
            {
                output.append(|mut row| {
                    row.copy_mapped(input_row, self.selected_variables.iter().map(|pos| (*pos, *pos)));
                })
            }
        }
        measurement.end(&self.profile, 1, output.len() as u64);
        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
}
