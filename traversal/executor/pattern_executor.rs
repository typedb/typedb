/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::{collections::HashMap, io::Read, sync::Arc};

use itertools::Itertools;

use answer::{variable::Variable, variable_value::VariableValue};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::inference::type_inference::TypeAnnotations;
use lending_iterator::{AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{
        iterator::{ConstraintIterator, ConstraintIteratorProvider},
        Position,
    },
    planner::pattern_plan::{Check, Execution, Iterate, PatternPlan, Single, Step},
};

pub(crate) struct PatternExecutor {
    variable_positions: HashMap<Variable, Position>,
    variable_positions_index: Vec<Variable>,

    steps: Vec<StepExecutor>,
    // modifiers: Modifier,
    initialised: bool,
    outputs: Option<Batch>,
    output_index: usize,
}

impl PatternExecutor {
    pub(crate) fn new(
        plan: PatternPlan,
        type_annotations: &TypeAnnotations,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        // 1. assign positions based on the output variables of each step
        // 2. create step executors that have an output Batch corresponding to the total size of the variables we care about

        let mut variable_positions = HashMap::new();
        let mut steps = Vec::with_capacity(plan.steps().len());
        for step in plan.into_steps() {
            for variable in step.generated_variables() {
                let previous = variable_positions.insert(*variable, Position::new(variable_positions.len() as u32));
                debug_assert_eq!(previous, Option::None);
            }
            let executor = StepExecutor::new(step, &variable_positions, type_annotations, snapshot, thing_manager)?;
            steps.push(executor)
        }
        let mut variable_positions_index = vec![Variable::new(0); variable_positions.len()];
        for (variable, position) in &variable_positions {
            variable_positions_index[position.as_usize()] = *variable
        }

        Ok(PatternExecutor {
            variable_positions,
            variable_positions_index,
            steps,
            // modifiers:
            initialised: false,
            outputs: None,
            output_index: 0,
        })
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        snapshot: Arc<Snapshot>,
        thing_manager: Arc<ThingManager>,
    ) -> impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>> {
        AsLendingIterator::new(BatchIterator::new(self, snapshot, thing_manager))
            .flat_map(|batch| BatchRowIterator::new(batch))
    }

    fn compute_next_batch(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        let steps_len = self.steps.len();

        let (mut current_step, mut last_batch, mut direction) = if self.initialised {
            (steps_len - 1, None, Direction::Backward)
        } else {
            self.initialised = true;
            (0, Some(Batch::EMPTY_SINGLE_ROW), Direction::Forward)
        };

        loop {
            match direction {
                Direction::Forward => {
                    if current_step >= steps_len {
                        return Ok(last_batch);
                    } else {
                        let batch = (&mut self.steps[current_step]).batch_from(last_batch.take().unwrap(), snapshot, thing_manager)?;
                        match batch {
                            None => {
                                direction = Direction::Backward;
                                if current_step == 0 {
                                    return Ok(None);
                                } else {
                                    current_step -= 1;
                                }
                            }
                            Some(batch) => {
                                last_batch = Some(batch);
                                current_step += 1;
                            }
                        }
                    }
                }
                Direction::Backward => {
                    let batch = (&mut self.steps[current_step]).batch_continue(snapshot, thing_manager)?;
                    match batch {
                        None => {
                            if current_step == 0 {
                                return Ok(None);
                            } else {
                                current_step -= 1;
                            }
                        }
                        Some(batch) => {
                            direction = Direction::Forward;
                            last_batch = Some(batch);
                            current_step += 1;
                        }
                    }
                }
            }
        }
    }
}

enum Direction {
    Forward,
    Backward,
}

struct BatchIterator<Snapshot> {
    executor: PatternExecutor,
    snapshot: Arc<Snapshot>,
    thing_manager: Arc<ThingManager>,
}

impl<Snapshot: ReadableSnapshot> BatchIterator<Snapshot> {
    fn new(executor: PatternExecutor, snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>) -> Self {
        Self { executor, snapshot, thing_manager }
    }
}

impl<Snapshot: ReadableSnapshot> Iterator for BatchIterator<Snapshot> {
    type Item = Result<Batch, ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.executor.compute_next_batch(self.snapshot.as_ref(), self.thing_manager.as_ref());
        batch.transpose()
    }
}

enum StepExecutor {
    Sorted(SortedExecutor),
    Unsorted(UnsortedExecutor),
    Single(SingleExecutor),

    Disjunction(DisjunctionExecutor),
    Negation(NegationExecutor),
    Optional(OptionalExecutor),
}

impl StepExecutor {
    fn new(
        step: Step,
        variable_positions: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let vars_count = variable_positions.len() as u32;
        let Step { execution: execution, .. } = step;
        match execution {
            Execution::SortedIterators(iterates) => {
                let executor = SortedExecutor::new(
                    iterates,
                    vars_count,
                    variable_positions,
                    type_annotations,
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::Sorted(executor))
            }
            Execution::UnsortedIterator(iterate, checks) => {
                Ok(Self::Unsorted(UnsortedExecutor::new(iterate, checks, vars_count, variable_positions)))
            }
            Execution::Single(single, checks) => {
                Ok(Self::Single(SingleExecutor::new(single, checks, variable_positions)))
            }
            Execution::Disjunction(plans) => {
                todo!()
                // let executors = plans.into_iter().map(|pattern_plan| PatternExecutor::new(pattern_plan, )).collect();
                // Self::Disjunction(DisjunctionExecutor::new(executors, variable_positions))
            }
            Execution::Negation(plan) => {
                todo!()
                // let executor = PatternExecutor::new(plan, );
                // // TODO: add limit 1, filters if they aren't there already?
                // Self::Negation(NegationExecutor::new(executor, variable_positions))
            }
            Execution::Optional(plan) => {
                let pattern_executor = PatternExecutor::new(plan, type_annotations, snapshot, thing_manager)?;
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
            StepExecutor::Sorted(sorted) => sorted.batch_from(input_batch, snapshot, thing_manager),
            StepExecutor::Unsorted(unsorted) => unsorted.batch_from(input_batch),
            StepExecutor::Single(single) => single.batch_from(input_batch),
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
            StepExecutor::Sorted(sorted) => sorted.batch_continue(snapshot, thing_manager),
            StepExecutor::Unsorted(unsorted) => todo!(), // unsorted.batch_continue(snapshot, thing_manager),
            StepExecutor::Disjunction(disjunction) => todo!(),
            StepExecutor::Optional(optional) => todo!(),
            StepExecutor::Single(_) | StepExecutor::Negation(_) => Ok(None),
        }
    }
}

struct SortedExecutor {
    iterator_providers: Vec<ConstraintIteratorProvider>,
    intersection_iterators: Vec<ConstraintIterator>,
    cartesian_iterator: CartesianIterator,
    sort_variable_position: Position,
    output_width: u32,

    input: Option<BatchRowIterator>,
    intersection_row: Vec<VariableValue<'static>>,
    output: Option<Batch>,
}

impl SortedExecutor {
    fn new(
        iterates: Vec<Iterate>,
        vars_count: u32,
        variable_positions: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let sort_variable = iterates[0].sort_variable().unwrap();
        let sort_variable_position = *variable_positions.get(&sort_variable).unwrap();
        let providers: Vec<ConstraintIteratorProvider> = iterates
            .into_iter()
            .map(|iterate| {
                ConstraintIteratorProvider::new(iterate, variable_positions, type_annotations, snapshot, thing_manager)
            })
            .collect::<Result<Vec<_>, ConceptReadError>>()?;

        Ok(Self {
            intersection_iterators: Vec::with_capacity(providers.len()),
            cartesian_iterator: CartesianIterator::new(vars_count as usize, providers.len(), sort_variable_position),
            iterator_providers: providers,
            sort_variable_position,
            output_width: vars_count,
            input: None,
            intersection_row: (0..vars_count).into_iter().map(|_| VariableValue::Empty).collect_vec(),
            output: None,
        })
    }

    fn batch_from(
        &mut self,
        input_batch: Batch,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        debug_assert!(self.output.is_none() && (self.input.is_none() || !self.input.as_ref().unwrap().has_next()));
        self.input = Some(BatchRowIterator::new(Ok(input_batch)));
        debug_assert!(self.input.as_ref().unwrap().has_next());
        self.may_compute_next_batch(snapshot, thing_manager)?;
        Ok(self.output.take())
    }

    fn batch_continue(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        debug_assert!(self.output.is_none());
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
            batch.append(|mut row| self.write_next_row_into(&mut row))?;
            while !batch.is_full() && self.compute_next_row(snapshot, thing_manager)? {
                batch.append(|mut row| self.write_next_row_into(&mut row))?;
            }
            self.output = Some(batch);
        }
        Ok(())
    }

    fn write_next_row_into(&mut self, row: &mut Row<'_>) -> Result<(), ConceptReadError> {
        if self.cartesian_iterator.is_active() {
            self.cartesian_iterator.write_into(row)
        } else {
            row.copy_from(&self.intersection_row);
            Ok(())
        }
    }

    fn compute_next_row(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<bool, ConceptReadError> {
        if self.cartesian_iterator.is_active() {
            let found = self.cartesian_iterator.find_next(snapshot, thing_manager, &self.iterator_providers)?;
            if found {
                return Ok(true);
            } else {
                // advance the first iterator past the intersection point to move to the next intersection
                let intersection = &self.intersection_row[self.sort_variable_position.as_usize()];
                let iterator = &mut self.intersection_iterators[0];
                while iterator.has_value() && iterator.peek_sorted_value_equals(intersection)? {
                    iterator.advance()?;
                }
                self.compute_next_row(snapshot, thing_manager)
            }
        } else {
            let found = self.find_intersection(snapshot, thing_manager)?;
            if found {
                self.record_intersection()?;
                self.advance_intersection_iterators()?;
                self.may_activate_cartesian(snapshot, thing_manager)?;
                return Ok(true);
            } else {
                return Ok(false);
            }
        }
    }

    fn find_intersection(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<bool, ConceptReadError> {
        if self.intersection_iterators.is_empty() && !self.create_intersection_iterators(snapshot, thing_manager)? {
            return Ok(false);
        }

        debug_assert!(self.intersection_iterators.len() > 0);
        if self.intersection_iterators.len() == 1 {
            // if there's only 1 iterator, we can just use it without any intersection
            return Ok(self.intersection_iterators[0].has_value());
        } else if self.intersection_iterators[0].peek_sorted_value().transpose().map_err(|err| err.clone())?.is_none() {
            // short circuit if the first iterator doesn't have any more outputs
            self.clear_intersection_iterators();
            return Ok(false);
        }

        let mut current_max_index = 0;
        loop {
            let mut failed = false;
            let mut retry = false;
            for i in 0..self.intersection_iterators.len() {
                if i == current_max_index {
                    continue;
                }

                let (containing_i, containing_max, i_index, max_index) = if current_max_index > i {
                    let (containing_i, containing_max) = self.intersection_iterators.split_at_mut(current_max_index);
                    (containing_i, containing_max, i, 0)
                } else {
                    let (containing_max, containing_i) = self.intersection_iterators.split_at_mut(i);
                    (containing_i, containing_max, 0, current_max_index)
                };
                let max_cmp_peek = match containing_i[i_index].peek_sorted_value() {
                    None => {
                        failed = true;
                        break;
                    }
                    Some(Ok(value)) => {
                        let max_peek = containing_max[max_index].peek_sorted_value().unwrap().unwrap();
                        max_peek.partial_cmp(&value).unwrap()
                    }
                    Some(Err(err)) => {
                        return Err(err.clone());
                    }
                };

                match max_cmp_peek {
                    Ordering::Less => {
                        current_max_index = i;
                        retry = true;
                    }
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        let current_max = &mut containing_max[max_index].peek_sorted_value().unwrap().unwrap();
                        let iter_i = &mut containing_i[i_index];
                        let iterator_status = iter_i.skip_to_sorted_value(current_max)?;
                        match iterator_status {
                            None => {
                                failed = true;
                                break;
                            }
                            Some(Ordering::Less) => unreachable!("Skip to should always be empty or equal/greater than the target"),
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

    fn create_intersection_iterators(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<bool, ConceptReadError> {
        debug_assert!(self.intersection_iterators.is_empty());
        let next_row = self.input.as_mut().unwrap().next().transpose().map_err(|err| err.clone())?;
        match next_row {
            None => Ok(false),
            Some(row) => {
                for provider in &self.iterator_providers {
                    self.intersection_iterators.push(provider.get_iterator(snapshot, thing_manager, row)?);
                }
                Ok(true)
            }
        }
    }

    fn advance_intersection_iterators(&mut self) -> Result<(), ConceptReadError> {
        for iter in &mut self.intersection_iterators {
            let _ = iter.advance()?;
        }
        Ok(())
    }

    fn clear_intersection_iterators(&mut self) {
        self.intersection_iterators.clear()
    }

    fn all_iterators_intersect(&mut self) -> bool {
        let (first, rest) = self.intersection_iterators.split_at_mut(1);
        let peek_0 = first[0].peek_sorted_value().unwrap().unwrap();
        for iter in rest {
            if iter.peek_sorted_value().unwrap().unwrap() != peek_0 {
                return false;
            }
        }
        return true;
    }

    fn record_intersection(&mut self) -> Result<(), ConceptReadError> {
        for value in &mut self.intersection_row {
            *value = VariableValue::Empty
        }
        let mut row = Row::new(&mut self.intersection_row);
        for iter in &mut self.intersection_iterators {
            iter.write_values(&mut row).map_err(|err| err.clone())?
        }
        Ok(())
    }

    fn may_activate_cartesian(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptReadError> {
        let mut cartesian = false;
        for iter in &mut self.intersection_iterators {
            if iter.peek_sorted_value_equals(&self.intersection_row[self.sort_variable_position.as_usize()])? {
                cartesian = true;
                break;
            }
        };
        if cartesian {
            self.cartesian_iterator.activate(
                snapshot,
                thing_manager,
                &self.iterator_providers,
                &self.intersection_row,
                &mut self.intersection_iterators,
            )?
        }
        Ok(())
    }
}

struct CartesianIterator {
    sort_variable_position: Position,
    is_active: bool,
    intersection_source: Vec<VariableValue<'static>>,
    cartesian_provider_indices: Vec<usize>,
    iterators: Vec<Option<ConstraintIterator>>,
}

impl CartesianIterator {
    fn new(width: usize, iterator_provider_count: usize, sort_variable_position: Position) -> Self {
        CartesianIterator {
            sort_variable_position,
            is_active: false,
            intersection_source: vec![VariableValue::Empty; width],
            cartesian_provider_indices: Vec::with_capacity(iterator_provider_count),
            iterators: (0..iterator_provider_count).into_iter().map(|_| Option::None).collect_vec(),
        }
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn activate(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        iterator_providers: &Vec<ConstraintIteratorProvider>,
        source_intersection: &Vec<VariableValue<'static>>,
        intersection_iterators: &mut Vec<ConstraintIterator>,
    ) -> Result<(), ConceptReadError> {
        debug_assert!(source_intersection.len() == self.intersection_source.len());
        self.is_active = true;
        self.intersection_source.clone_from_slice(source_intersection);

        // we should be able to re-use existing iterators since they should only move forward
        self.cartesian_provider_indices.clear();

        let intersection = &source_intersection[self.sort_variable_position.as_usize()];
        for (index, iter) in intersection_iterators.iter_mut().enumerate() {
            if iter.peek_sorted_value_equals(intersection)? {
                self.cartesian_provider_indices.push(index);

                // reopen/move existing cartesian iterators forward to the intersection point
                let iterator = self.iterators[index].take();
                match iterator {
                    None => {
                        let reopened = self.reopen_iterator(snapshot, thing_manager, &iterator_providers[index])?;
                        self.iterators[index] = Some(reopened);
                    }
                    Some(mut iter) => {
                        let ordering = iter.skip_to_sorted_value(intersection)?;
                        debug_assert!(ordering.is_some() && ordering.unwrap().is_eq());
                    }
                }
            }
        }
        Ok(())
    }

    fn find_next(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        providers: &Vec<ConstraintIteratorProvider>,
    ) -> Result<bool, ConceptReadError> {
        debug_assert!(self.is_active);
        // precondition: all required iterators are open to the intersection point

        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        let mut provider_index = self.cartesian_provider_indices.len() - 1;
        loop {
            let iterator_index = self.cartesian_provider_indices[provider_index];
            let iter = (&mut self.iterators)[iterator_index].as_mut().unwrap();
            iter.advance()?;
            if !iter.peek_sorted_value_equals(intersection)? {
                if provider_index == 0 {
                    self.is_active = false;
                    return Ok(false);
                } else {
                    let reopened = self.reopen_iterator(snapshot, thing_manager, &providers[provider_index])?;
                    self.iterators[iterator_index] = Some(reopened);
                    provider_index -= 1;
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
        provider: &ConstraintIteratorProvider,
    ) -> Result<ConstraintIterator, ConceptReadError> {
        let mut reopened = provider.get_iterator(
            snapshot, thing_manager, ImmutableRow::new(&self.intersection_source),
        )?;
        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        reopened.skip_to_sorted_value(intersection)?;
        Ok(reopened)
    }

    fn write_into(&mut self, row: &mut Row) -> Result<(), ConceptReadError> {
        for &provider_index in &self.cartesian_provider_indices {
            let iterator = self.iterators[provider_index].as_mut().unwrap();
            iterator.write_values(row).map_err(|err| err.clone())?
        }
        for (index, value) in self.intersection_source.iter().enumerate() {
            if *row.get(Position::new(index as u32)) == VariableValue::Empty {
                row.set(Position::new(index as u32), value.clone());
            }
        }
        Ok(())
    }
}

struct UnsortedExecutor {
    iterate: Iterate,
    checks: Vec<Check>,

    output_width: u32,
    output: Option<Batch>,
}

impl UnsortedExecutor {
    fn new(
        iterate: Iterate,
        checks: Vec<Check>,
        total_vars: u32,
        variable_positions: &HashMap<Variable, Position>,
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

struct SingleExecutor {
    provider: Single,
    checks: Vec<Check>,
}

impl SingleExecutor {
    fn new(provider: Single, checks: Vec<Check>, variable_positions: &HashMap<Variable, Position>) -> SingleExecutor {
        Self { provider, checks }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Result<Option<Batch>, ConceptReadError> {
        todo!()
    }
}

struct DisjunctionExecutor {
    executors: Vec<PatternExecutor>,
}

impl DisjunctionExecutor {
    fn new(executors: Vec<PatternExecutor>, variable_positions: &HashMap<Variable, Position>) -> DisjunctionExecutor {
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
    fn new(executor: PatternExecutor, variable_positions: &HashMap<Variable, Position>) -> NegationExecutor {
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

const BATCH_ROWS_MAX: u32 = 64;

struct Batch {
    width: u32,
    entries: u32,
    data: Vec<VariableValue<'static>>,
}

impl Batch {
    const EMPTY_SINGLE_ROW: Batch = Batch { width: 0, entries: 1, data: Vec::new() };

    fn new(width: u32) -> Self {
        let size = width * BATCH_ROWS_MAX;
        Batch { width: width, data: vec![VariableValue::Empty; size as usize], entries: 0 }
    }

    fn rows_count(&self) -> u32 {
        self.entries
    }

    fn is_full(&self) -> bool {
        (self.entries * self.width) as usize == self.data.len()
    }

    fn get_row(&self, index: u32) -> ImmutableRow<'_> {
        debug_assert!(index <= self.entries);
        let start = (index * self.width) as usize;
        let end = ((index + 1) * self.width) as usize;
        let slice = &self.data[start..end];
        ImmutableRow::new(slice)
    }

    fn get_row_mut(&mut self, index: u32) -> Row<'_> {
        debug_assert!(index <= self.entries);
        self.row_internal_mut(index)
    }

    fn append<T>(&mut self, mut writer: impl FnMut(Row<'_>) -> T) -> T {
        debug_assert!(!self.is_full());
        let row = self.row_internal_mut(self.entries);
        let result = writer(row);
        self.entries += 1;
        result
    }

    fn row_internal_mut(&mut self, index: u32) -> Row<'_> {
        let start = (index * self.width) as usize;
        let end = ((index + 1) * self.width) as usize;
        let slice = &mut self.data[start..end];
        Row::new(slice)
    }
}

struct BatchRowIterator {
    batch: Result<Batch, ConceptReadError>,
    index: u32,
}

impl BatchRowIterator {
    fn new(batch: Result<Batch, ConceptReadError>) -> Self {
        Self { batch, index: 0 }
    }

    fn has_next(&self) -> bool {
        self.batch.as_ref().is_ok_and(|batch| self.index < batch.rows_count())
    }
}

impl LendingIterator for BatchRowIterator {
    type Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.batch.as_mut() {
            Ok(batch) => {
                if self.index >= batch.rows_count() {
                    None
                } else {
                    let row = batch.get_row(self.index);
                    self.index += 1;
                    Some(Ok(row))
                }
            }
            Err(err) => Some(Err(err)),
        }
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    row: &'a mut [VariableValue<'static>],
}

impl<'a> Row<'a> {
    fn new(row: &'a mut [VariableValue<'static>]) -> Self {
        Self { row }
    }

    pub(crate) fn len(&self) -> usize {
        self.row.len()
    }

    pub(crate) fn get(&self, position: Position) -> &VariableValue {
        &self.row[position.as_usize()]
    }

    pub(crate) fn set(&mut self, position: Position, value: VariableValue<'static>) {
        debug_assert!(*self.get(position) == VariableValue::Empty || *self.get(position) == value);
        self.row[position.as_usize()] = value;
    }

    fn copy_from(&mut self, row: &[VariableValue<'static>]) {
        debug_assert!(self.len() == row.len());
        self.row.clone_from_slice(row)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ImmutableRow<'a> {
    row: &'a [VariableValue<'static>],
}

impl<'a> ImmutableRow<'a> {
    fn new(row: &'a [VariableValue<'static>]) -> Self {
        Self { row }
    }

    pub(crate) fn len(&self) -> usize {
        self.row.len()
    }

    pub(crate) fn get(&self, position: Position) -> &VariableValue {
        &self.row[position.as_usize()]
    }

    pub fn to_vec(&self) -> Vec<VariableValue<'static>> {
        self.row.to_vec()
    }
}
