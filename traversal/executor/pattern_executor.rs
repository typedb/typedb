/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::{collections::HashMap, io::Read, sync::Arc};

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
    output_width: u32,

    input: Option<BatchRowIterator>,
    last_intersection: Vec<VariableValue<'static>>,
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
        let providers: Vec<ConstraintIteratorProvider> = iterates
            .into_iter()
            .map(|iterate| {
                ConstraintIteratorProvider::new(iterate, variable_positions, type_annotations, snapshot, thing_manager)
            })
            .collect::<Result<Vec<_>, ConceptReadError>>()?;

        Ok(Self {
            intersection_iterators: Vec::with_capacity(providers.len()),
            iterator_providers: providers,
            cartesian_iterator: CartesianIterator::new(),
            output_width: vars_count,
            input: None,
            last_intersection: Vec::with_capacity(vars_count as usize),
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
            batch.append(|mut row| self.write_next_row_into(&mut row));
            while !batch.is_full() && self.compute_next_row(snapshot, thing_manager)? {
                batch.append(|mut row| self.write_next_row_into(&mut row));
            }
            self.output = Some(batch);
        }
        Ok(())
    }

    fn compute_next_row(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<bool, ConceptReadError> {
        if self.cartesian_iterator.is_active() {
            self.cartesian_iterator.compute_next(snapshot, thing_manager)
        } else {
            let found = self.find_intersection(snapshot, thing_manager)?;
            if found {
                self.write_intersection_into(&mut Row { row: &mut self.last_intersection });
                self.advance_intersection_iterators()?;
                if self.cartesian_rows_exist() {
                    self.cartesian_iterator.activate(&self.last_intersection)
                }
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
            return self.intersection_iterators[0].advance();
        } else if self.intersection_iterators[0].peek_sorted_value().transpose().map_err(|err| err.clone())?.is_none() {
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

    fn cartesian_rows_exist(&mut self) -> bool {
        todo!()
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

    fn write_next_row_into(&self, row: &mut Row<'_>) {
        if self.cartesian_iterator.is_active() {
            self.cartesian_iterator.write_into(row)
        } else {
            self.write_intersection_into(row)
        }
    }

    fn write_intersection_into(&self, row: &mut Row<'_>) {
        row.copy_from(&self.last_intersection)
    }
}

struct CartesianIterator {}

impl CartesianIterator {
    fn new() -> Self {
        CartesianIterator {}
    }

    fn is_active(&self) -> bool {
        todo!()
    }

    fn activate(&mut self, source_intersection: &Vec<VariableValue<'static>>) {
        todo!()
    }

    fn deactive(&mut self) {}

    fn compute_next(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<bool, ConceptReadError> {
        todo!()
    }

    fn write_into(&self, p0: &mut Row) {
        todo!()
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
        ImmutableRow { row: slice }
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
        Row { row: slice }
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
    pub fn len(&self) -> usize {
        self.row.len()
    }

    pub fn get(&self, position: Position) -> &VariableValue {
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
    pub fn len(&self) -> usize {
        self.row.len()
    }

    pub fn get(&self, position: Position) -> &VariableValue {
        &self.row[position.as_usize()]
    }

    pub fn to_vec(&self) -> Vec<VariableValue<'static>> {
        self.row.to_vec()
    }
}
