/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use itertools::Itertools;

use answer::variable::Variable;
use answer::variable_value::VariableValue;
use concept::error::ConceptReadError;
use concept::thing::thing_manager::ThingManager;
use ir::inference::type_inference::TypeAnnotations;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::executor::iterator::{ConstraintIterator, ConstraintIteratorProvider};
use crate::executor::Position;
use crate::planner::pattern_plan::{Check, Execution, Iterate, PatternPlan, Single, IterateMode, Step};

pub(crate) struct PatternExecutor {
    variable_positions: HashMap<Variable, Position>,
    variable_positions_index: Vec<Variable>,

    steps: Vec<StepExecutor>,
    // modifiers: Modifier,
    outputs: Option<Batch>,
    output_index: usize,
}

impl PatternExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        plan: PatternPlan,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
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
            outputs: None,
            output_index: 0,
        })
    }

    pub fn into_rows(self) {
        // TODO: we could use a lending iterator here to avoid a malloc row/answer
        // self.flat_map(|batch| batch.into_rows_cloned())
        todo!()
    }

    fn compute_next_batch<Snapshot: ReadableSnapshot>(&mut self, snapshot: &Snapshot, thing_manager: &ThingManager<Snapshot>) -> Result<Option<Batch>, ConceptReadError> {
        let steps_len = self.steps.len();
        let mut step_index = steps_len;
        let mut last_batch = None;
        let mut direction = Direction::Backward;
        while true {
            let step = &mut self.steps[step_index];
            match direction {
                Direction::Forward => {
                    if step_index > steps_len {
                        return Ok(last_batch);
                    } else {
                        let batch = step.batch_from(last_batch.take().unwrap(), snapshot, thing_manager)?;
                        match batch {
                            None => {
                                direction = Direction::Backward;
                                step_index -= 1;
                            }
                            Some(batch) => {
                                last_batch = Some(batch);
                                step_index += 1;
                            }
                        }
                    }
                }
                Direction::Backward => {
                    if step_index < 0 {
                        return Ok(None);
                    } else {
                        let batch = step.batch_continue(snapshot, thing_manager)?;
                        match batch {
                            None => {
                                direction = Direction::Backward;
                                step_index -= 1;
                            }
                            Some(batch) => {
                                last_batch = Some(batch);
                                step_index += 1;
                            }
                        }
                    }
                }
            }
        }
        unreachable!("Computation must return from loop")
    }
}

enum Direction {
    Forward,
    Backward,
}

impl Iterator for PatternExecutor {
    type Item = Batch;

    fn next(&mut self) -> Option<Self::Item> {
        // if self.outputs.is_none() {
        //     self.outputs = self.compute_next_batch();
        // }
        // self.outputs.take()
        todo!()
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
    fn new<Snapshot: ReadableSnapshot>(
        step: Step,
        variable_positions: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Self, ConceptReadError> {
        let vars_count = step.total_variables_count();
        let Step { execution: execution, .. } = step;
        match execution {
            Execution::SortedIterators(iterates) => {
                let executor = SortedExecutor::new(iterates, vars_count, variable_positions, type_annotations, snapshot, thing_manager)?;
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

    fn batch_from<Snapshot: ReadableSnapshot>(
        &mut self,
        input_batch: Batch,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
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

    fn batch_continue<Snapshot: ReadableSnapshot>(
        &mut self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Option<Batch>, ConceptReadError> {
        match self {
            StepExecutor::Sorted(sorted) => sorted.batch_continue(snapshot, thing_manager),
            StepExecutor::Unsorted(unsorted) => todo!(),// unsorted.batch_continue(snapshot, thing_manager),
            StepExecutor::Disjunction(disjunction) => todo!(),
            StepExecutor::Optional(optional) => todo!(),
            StepExecutor::Single(_) | StepExecutor::Negation(_) => Ok(None),
        }
    }
}

struct SortedExecutor {
    iterator_providers: Vec<ConstraintIteratorProvider>,
    iterators: Vec<ConstraintIterator>,
    output_width: u32,

    input: Option<Batch>,
    next_input_row: usize,
    output: Option<Batch>,
}

impl SortedExecutor {
    fn new<Snapshot: ReadableSnapshot>(
        iterates: Vec<Iterate>,
        vars_count: u32,
        variable_positions: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Self, ConceptReadError> {
        let providers: Vec<ConstraintIteratorProvider> = iterates
            .into_iter()
            .map(|iterate| ConstraintIteratorProvider::new(iterate, variable_positions, type_annotations, snapshot, thing_manager))
            .collect::<Result<Vec<_>, ConceptReadError>>()?;

        Ok(Self {
            iterators: Vec::with_capacity(providers.len()),
            iterator_providers: providers,
            output_width: vars_count,
            input: None,
            next_input_row: 0,
            output: None,
        })
    }

    fn batch_from<Snapshot: ReadableSnapshot>(
        &mut self,
        input_batch: Batch,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Option<Batch>, ConceptReadError> {
        debug_assert!(
            self.output.is_none()
                && (self.input.is_none() || self.next_input_row == self.input.as_ref().unwrap().rows_count())
        );
        self.input = Some(input_batch);
        self.next_input_row = 0;
        self.compute_next_batch(snapshot, thing_manager)?;
        Ok(self.output.take())
    }

    fn batch_continue<Snapshot: ReadableSnapshot>(
        &mut self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Option<Batch>, ConceptReadError> {
        debug_assert!(
            self.output.is_none()
                && (self.input.is_some() && self.next_input_row < self.input.as_ref().unwrap().rows_count())
        );
        self.compute_next_batch(snapshot, thing_manager)?;
        Ok(self.output.take())
    }

    fn compute_next_batch<Snapshot: ReadableSnapshot>(
        &mut self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<(), ConceptReadError> {
        // don't allocate batch until 1 answer is confirmed
        if self.iterators.is_empty() {
            self.create_iterators(snapshot, thing_manager)?;
        }
        self.next_iterators_intersection();
        Ok(())
    }

    fn create_iterators<Snapshot: ReadableSnapshot>(
        &mut self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<(), ConceptReadError> {
        debug_assert!(self.iterators.is_empty());
        let next_row = self.input.as_mut().unwrap().get_row(self.next_input_row);
        // for provider in &self.iterator_providers {
        //     self.iterators.push(provider.get_iterator(snapshot, thing_manager))
        // }
        todo!()
    }

    fn next_iterators_intersection(&mut self) {
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

pub struct Batch {
    width: usize,
    data: Vec<VariableValue<'static>>,
}

impl Batch {
    fn new(width: u32) -> Self {
        let size = width * BATCH_ROWS_MAX;
        Batch { width: width as usize, data: vec![VariableValue::Empty; size as usize] }
    }

    fn rows_count(&self) -> usize {
        // TODO adjust if batch is not full
        BATCH_ROWS_MAX as usize
    }

    pub(crate) fn get_row(&mut self, index: usize) -> Row<'_> {
        let slice = &mut self.data[index * self.width..(index + 1) * self.width];
        Row { row: slice }
    }

    fn into_rows_cloned(self) -> RowsIterator {
        RowsIterator { batch: self, index: 0 }
    }
}

struct RowsIterator {
    batch: Batch,
    index: usize,
}

impl LendingIterator for RowsIterator {
    type Item<'a> = Row<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.index > self.batch.rows_count() {
            None
        } else {
            let slice = &mut self.batch.data[self.index * self.batch.width..(self.index + 1) * self.batch.width];
            self.index += 1;
            Some(Row { row: slice })
        }
    }
}

pub(crate) struct Row<'a> {
    row: &'a mut [VariableValue<'static>],
}

impl<'a> Row<'a> {
    pub(crate) fn len(&self) -> usize {
        self.row.len()
    }

    pub(crate) fn get(&self, position: Position) -> &VariableValue {
        &self.row[position.as_usize()]
    }

    pub(crate) fn set(&mut self, position: Position, value: VariableValue<'static>) {
        debug_assert!(*self.get(position) == VariableValue::Empty);
        self.row[position.as_usize()] = value;
    }
}
