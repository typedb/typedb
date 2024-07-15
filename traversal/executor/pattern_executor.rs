/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap, sync::Arc};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use answer::{variable::Variable, variable_value::VariableValue};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::inference::type_inference::TypeAnnotations;
use ir::program::block::BlockContext;
use lending_iterator::{AsLendingIterator, LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{
        instruction::{InstructionIterator, IteratorExecutor},
        Position,
    },
    planner::pattern_plan::{
        AssignmentStep, DisjunctionStep, Instruction, NegationStep,
        OptionalStep, PatternPlan, SortedJoinStep, Step, UnsortedJoinStep,
    },
};
use crate::executor::SelectedPositions;

pub(crate) struct PatternExecutor {
    variable_positions: HashMap<Variable, Position>,
    variable_positions_index: Vec<Variable>,

    step_executors: Vec<StepExecutor>,
    // modifiers: Modifier,
    initialised: bool,
    outputs: Option<Batch>,
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

        let PatternPlan { steps, context } = plan;

        let mut variable_positions = HashMap::new();
        let mut step_executors = Vec::with_capacity(steps.len());
        for step in steps {
            for variable in step.unbound_variables() {
                let previous = variable_positions.insert(*variable, Position::new(variable_positions.len() as u32));
                debug_assert_eq!(previous, Option::None);
            }
            let executor = StepExecutor::new(
                step, &context, &variable_positions, type_annotations, snapshot, thing_manager,
            )?;
            step_executors.push(executor)
        }
        let mut variable_positions_index = vec![Variable::new(0); variable_positions.len()];
        for (variable, position) in &variable_positions {
            variable_positions_index[position.as_usize()] = *variable
        }

        Ok(PatternExecutor {
            variable_positions,
            variable_positions_index,
            step_executors,
            // modifiers:
            initialised: false,
            outputs: None,
        })
    }

    pub(crate) fn variable_positions(&self) -> &HashMap<Variable, Position> {
        &self.variable_positions
    }

    pub(crate) fn variable_positions_index(&self) -> &Vec<Variable> {
        &self.variable_positions_index
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        snapshot: Arc<Snapshot>,
        thing_manager: Arc<ThingManager>,
    ) -> impl for<'a> LendingIterator<Item<'a>=Result<ImmutableRow<'a>, &'a ConceptReadError>> {
        AsLendingIterator::new(BatchIterator::new(self, snapshot, thing_manager))
            .flat_map(|batch| BatchRowIterator::new(batch))
    }

    fn compute_next_batch(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        let steps_len = self.step_executors.len();

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
                        let batch = self.step_executors[current_step]
                            .batch_from(last_batch.take().unwrap(), snapshot, thing_manager)?;
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
                    let batch = self.step_executors[current_step]
                        .batch_continue(snapshot, thing_manager)?;
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

#[derive(Clone, Copy, Debug)]
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
    SortedJoin(SortedJoinExecutor),
    UnsortedJoin(UnsortedJoinExecutor),
    Assignment(AssignExecutor),

    Disjunction(DisjunctionExecutor),
    Negation(NegationExecutor),
    Optional(OptionalExecutor),
}

impl StepExecutor {
    fn new(
        step: Step,
        block_context: &BlockContext,
        variable_positions: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let row_width = variable_positions.len() as u32;
        match step {
            Step::SortedJoin(SortedJoinStep { sort_variable, instructions, selected_variables, .. }) => {
                let executor = SortedJoinExecutor::new(
                    sort_variable,
                    instructions,
                    row_width,
                    selected_variables,
                    block_context.get_variables_named(),
                    variable_positions,
                    type_annotations,
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::SortedJoin(executor))
            }
            Step::UnsortedJoin(UnsortedJoinStep { iterate_instruction, check_instructions, .. }) => {
                let executor =
                    UnsortedJoinExecutor::new(iterate_instruction, check_instructions, row_width, variable_positions);
                Ok(Self::UnsortedJoin(executor))
            }
            Step::Assignment(AssignmentStep { .. }) => {
                todo!()
            }
            Step::Disjunction(DisjunctionStep { disjunction: disjunction_plans, .. }) => {
                // let executors = plans.into_iter().map(|pattern_plan| PatternExecutor::new(pattern_plan, )).collect();
                // Self::Disjunction(DisjunctionExecutor::new(executors, variable_positions))
                todo!()
            }
            Step::Negation(NegationStep { negation: negation_plan, .. }) => {
                let executor = PatternExecutor::new(negation_plan, type_annotations, snapshot, thing_manager)?;
                // // TODO: add limit 1, filters if they aren't there already?
                Ok(Self::Negation(NegationExecutor::new(executor, variable_positions)))
            }
            Step::Optional(OptionalStep { optional: optional_plan, .. }) => {
                let pattern_executor = PatternExecutor::new(optional_plan, type_annotations, snapshot, thing_manager)?;
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

struct SortedJoinExecutor {
    instruction_executors: Vec<IteratorExecutor>,
    sort_variable_position: Position,
    output_width: u32,
    outputs_selected: SelectedPositions,

    iterators: Vec<InstructionIterator>,
    cartesian_iterator: CartesianIterator,
    input: Option<Peekable<BatchRowIterator>>,
    intersection_row: Vec<VariableValue<'static>>,
    output: Option<Batch>,
}

impl SortedJoinExecutor {
    fn new(
        sort_variable: Variable,
        instructions: Vec<Instruction>,
        output_width: u32,
        select_variables: Vec<Variable>,
        named_variables: &HashMap<Variable, String>,
        variable_positions: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let sort_variable_position = *variable_positions.get(&sort_variable).unwrap();
        let instruction_count = instructions.len();
        let executors: Vec<IteratorExecutor> = instructions
            .into_iter()
            .map(|instruction| {
                IteratorExecutor::new(
                    instruction,
                    &select_variables,
                    named_variables,
                    variable_positions,
                    type_annotations,
                    snapshot,
                    thing_manager,
                    Some(sort_variable),
                )
            })
            .collect::<Result<Vec<_>, ConceptReadError>>()?;

        Ok(Self {
            instruction_executors: executors,
            sort_variable_position,
            output_width,
            outputs_selected: SelectedPositions::new(&select_variables, variable_positions),
            iterators: Vec::with_capacity(instruction_count),
            cartesian_iterator: CartesianIterator::new(output_width as usize, instruction_count, sort_variable_position),
            input: None,
            intersection_row: (0..output_width).map(|_| VariableValue::Empty).collect_vec(),
            output: None,
        })
    }

    fn batch_from(
        &mut self,
        input_batch: Batch,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Option<Batch>, ConceptReadError> {
        debug_assert!(self.output.is_none() && (self.input.is_none() || !self.input.as_mut().unwrap().peek().is_some()));
        self.input = Some(Peekable::new(BatchRowIterator::new(Ok(input_batch))));
        debug_assert!(self.input.as_mut().unwrap().peek().is_some());
        self.create_intersection_iterators(snapshot, thing_manager)?;
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
            let found = self.cartesian_iterator.find_next(snapshot, thing_manager, &self.instruction_executors)?;
            if found {
                Ok(true)
            } else {
                // advance the first iterator past the intersection point to move to the next intersection
                let intersection = &self.intersection_row[self.sort_variable_position.as_usize()];
                let iterator = &mut self.iterators[0];
                while iterator.has_value() && iterator.peek_sorted_value_equals(intersection)? {
                    iterator.advance()?;
                }
                self.compute_next_row(snapshot, thing_manager)
            }
        } else {
            loop {
                let found = self.find_intersection()?;
                if found {
                    self.record_intersection()?;
                    self.advance_intersection_iterators()?;
                    self.may_activate_cartesian(snapshot, thing_manager)?;
                    return Ok(true);
                } else {
                    self.iterators.clear();
                    let _ = self.input.as_mut().unwrap().next().unwrap().map_err(|err| err.clone())?;
                    if self.input.as_mut().unwrap().peek().is_some() {
                        self.create_intersection_iterators(snapshot, thing_manager)?;
                    }
                }
            }
        }
    }

    fn find_intersection(&mut self) -> Result<bool, ConceptReadError> {
        debug_assert!(!self.iterators.is_empty());
        if self.iterators.len() == 1 {
            // if there's only 1 iterator, we can just use it without any intersection
            return Ok(self.iterators[0].has_value());
        } else if self.iterators[0].peek_sorted_value().transpose().map_err(|err| err.clone())?.is_none() {
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

    fn create_intersection_iterators(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptReadError> {
        debug_assert!(self.iterators.is_empty());
        let peek = self.input.as_mut().unwrap().peek().unwrap();
        let next_row: &ImmutableRow<'_> = peek.as_ref().map_err(|err| (*err).clone())?;
        for executor in &self.instruction_executors {
            self.iterators.push(executor.get_iterator(snapshot, thing_manager, next_row.as_reference())?);
        }
        Ok(())
    }

    fn advance_intersection_iterators(&mut self) -> Result<(), ConceptReadError> {
        for iter in &mut self.iterators {
            let _ = iter.advance()?;
        }
        Ok(())
    }

    fn clear_intersection_iterators(&mut self) {
        self.iterators.clear()
    }

    fn all_iterators_intersect(&mut self) -> bool {
        let (first, rest) = self.iterators.split_at_mut(1);
        let peek_0 = first[0].peek_sorted_value().unwrap().unwrap();
        for iter in rest {
            if iter.peek_sorted_value().unwrap().unwrap() != peek_0 {
                return false;
            }
        }
        return true;
    }

    fn record_intersection(&mut self) -> Result<(), ConceptReadError> {
        self.intersection_row.fill(VariableValue::Empty);
        let mut row = Row::new(&mut self.intersection_row);
        for iter in &mut self.iterators {
            // iter.write_values_and_advance_optimised(&mut row).map_err(|err| err.clone())?
            iter.write_values(&mut row)?
        }

        let input_row = self.input.as_mut().unwrap().peek().unwrap().as_ref().map_err(|err| (*err).clone())?;
        for position in self.outputs_selected.iter_selected() {
            row.set(position, input_row.get(position).clone().into_owned())
        }
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
            if iter.peek_sorted_value_equals(&self.intersection_row[self.sort_variable_position.as_usize()])? {
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
                &mut self.iterators,
            )?
        }
        Ok(())
    }
}

struct CartesianIterator {
    sort_variable_position: Position,
    is_active: bool,
    intersection_source: Vec<VariableValue<'static>>,
    cartesian_executor_indices: Vec<usize>,
    iterators: Vec<Option<InstructionIterator>>,
}

impl CartesianIterator {
    fn new(width: usize, iterator_executor_count: usize, sort_variable_position: Position) -> Self {
        CartesianIterator {
            sort_variable_position,
            is_active: false,
            intersection_source: vec![VariableValue::Empty; width],
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
        iterator_executors: &Vec<IteratorExecutor>,
        source_intersection: &Vec<VariableValue<'static>>,
        intersection_iterators: &mut Vec<InstructionIterator>,
    ) -> Result<(), ConceptReadError> {
        debug_assert!(source_intersection.len() == self.intersection_source.len());
        self.is_active = true;
        self.intersection_source.clone_from_slice(source_intersection);

        // we are able to re-use existing iterators since they should only move forward. We only reset the indices
        self.cartesian_executor_indices.clear();

        let intersection = &source_intersection[self.sort_variable_position.as_usize()];
        for (index, iter) in intersection_iterators.iter_mut().enumerate() {
            if iter.peek_sorted_value_equals(intersection)? {
                self.cartesian_executor_indices.push(index);

                // reopen/move existing cartesian iterators forward to the intersection point
                let preexisting_iterator = self.iterators[index].take();
                let iterator = match preexisting_iterator {
                    None => self.reopen_iterator(snapshot, thing_manager, &iterator_executors[index])?,
                    Some(mut iter) => {
                        let ordering = iter.skip_to_sorted_value(intersection)?;
                        debug_assert!(ordering.is_some() && ordering.unwrap().is_eq());
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
        executors: &Vec<IteratorExecutor>,
    ) -> Result<bool, ConceptReadError> {
        debug_assert!(self.is_active);
        // precondition: all required iterators are open to the intersection point

        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        let mut executor_index = self.cartesian_executor_indices.len() - 1;
        loop {
            let iterator_index = self.cartesian_executor_indices[executor_index];
            let iter = (&mut self.iterators)[iterator_index].as_mut().unwrap();
            iter.advance()?;
            if !iter.peek_sorted_value_equals(intersection)? {
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
        executor: &IteratorExecutor,
    ) -> Result<InstructionIterator, ConceptReadError> {
        let mut reopened =
            executor.get_iterator(snapshot, thing_manager, ImmutableRow::new(&self.intersection_source))?;
        let intersection = &self.intersection_source[self.sort_variable_position.as_usize()];
        reopened.skip_to_sorted_value(intersection)?;
        Ok(reopened)
    }

    fn write_into(&mut self, row: &mut Row) -> Result<(), ConceptReadError> {
        for &executor_index in &self.cartesian_executor_indices {
            let iterator = self.iterators[executor_index].as_mut().unwrap();
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

struct UnsortedJoinExecutor {
    iterate: Instruction,
    checks: Vec<Instruction>,

    output_width: u32,
    output: Option<Batch>,
}

impl UnsortedJoinExecutor {
    fn new(
        iterate: Instruction,
        checks: Vec<Instruction>,
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

#[derive(Debug)]
struct Batch {
    width: u32,
    entries: u32,
    data: Vec<VariableValue<'static>>,
}

impl Batch {
    const EMPTY_SINGLE_ROW: Batch = Batch { width: 0, entries: 1, data: Vec::new() };

    fn new(width: u32) -> Self {
        let size = width * BATCH_ROWS_MAX;
        Batch { width, data: vec![VariableValue::Empty; size as usize], entries: 0 }
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

#[derive(Debug, Clone)]
pub struct ImmutableRow<'a> {
    row: Cow<'a, [VariableValue<'static>]>,
}

impl<'a> ImmutableRow<'a> {
    fn new(row: &'a [VariableValue<'static>]) -> Self {
        Self { row: Cow::Borrowed(row) }
    }

    pub fn width(&self) -> usize {
        self.row.len()
    }

    pub fn get(&self, position: Position) -> &VariableValue {
        &self.row[position.as_usize()]
    }

    pub fn into_owned(self) -> ImmutableRow<'static> {
        let cloned: Vec<VariableValue<'static>> = self.row.into_iter().map(|value| value.clone().into_owned()).collect();
        ImmutableRow { row: Cow::Owned(cloned) }
    }

    pub fn as_reference(&self) -> ImmutableRow<'_> {
        ImmutableRow { row: Cow::Borrowed(self.row.as_ref()) }
    }
}

impl ImmutableRow<'static> {
    pub fn into_iter(self) -> impl Iterator<Item=VariableValue<'static>> {
        self.row.into_owned().into_iter()
    }
}

impl<'a> Display for ImmutableRow<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for value in self.row.as_ref() {
            write!(f, "{value},\t")?
        }
        write!(f, "\n")
    }
}