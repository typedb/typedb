/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use itertools::Itertools;
use answer::variable::Variable;

use answer::variable_value::VariableValue;
use ir::pattern::constraint::{Comparison, FunctionCallBinding, Has, RolePlayer};
use crate::executor::constraint_iterator::ConstraintIteratorProvider;
use crate::executor::Position;

use crate::planner::pattern_plan::{Check, Execution, Iterate, PatternPlan, Single, SortedIterateMode, Step};

pub(crate) struct PatternExecutor {
    variable_positions: HashMap<Variable, Position>,
    variable_positions_index: Vec<Variable>,

    steps: Vec<StepExecutor>,
    // modifiers: Modifier,

    outputs: Option<Batch>,
    output_index: usize,
}

impl PatternExecutor {
    pub(crate) fn new(plan: PatternPlan, variable_positions: &HashMap<Variable, Position>) -> Self {
        // 1. assign positions based on the output variables of each step
        // 2. create step executors that have an output Batch corresponding to the total size of the variables we care about

        let mut variable_positions = HashMap::new();
        let mut steps = Vec::new();
        for step in plan.into_steps() {
            for variable in step.generated_variables() {
                let previous = variable_positions.insert(*variable, Position::new(variable_positions.len() as u32));
                debug_assert_eq!(previous, Option::None);
            }
            steps.push(StepExecutor::new(step, &variable_positions))
        }
        let mut variable_positions_index = vec![Variable::new(0); variable_positions.len()];
        for (variable, position) in &variable_positions {
            variable_positions_index[position.as_usize()] = *variable
        }

        PatternExecutor {
            variable_positions,
            variable_positions_index,
            steps,
            // modifiers:
            outputs: None,
            output_index: 0,
        }
    }

    pub fn into_rows(self) -> impl Iterator<Item=Row> {
        // TODO: we could use a lending iterator here to avoid a malloc row/answer
        self.flat_map(|batch| batch.into_rows_cloned())
    }

    fn compute_next_batch(&mut self) -> Option<Batch> {
        let steps_len = self.steps.len();
        let mut step_index = steps_len;
        let mut last_batch = None;
        let mut direction = Direction::Backward;
        while true {
            let step = &mut self.steps[step_index];
            match direction {
                Direction::Forward => {
                    if step_index > steps_len {
                        return last_batch;
                    } else {
                        let batch = step.batch_from(last_batch.take().unwrap());
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
                        return None;
                    } else {
                        let batch = step.batch_continue();
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
        if self.outputs.is_none() {
            self.outputs = self.compute_next_batch();
        }
        self.outputs.take()
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
    fn new(step: Step, variable_positions: &HashMap<Variable, Position>) -> Self {
        let Step { execution: execution, total_variables_count: vars_count, .. } = step;

        match execution {
            Execution::SortedIterators(iterates) => {
                Self::Sorted(SortedExecutor::new(iterates, vars_count, variable_positions))
            }
            Execution::UnsortedIterator(iterate, checks) => {
                Self::Unsorted(UnsortedExecutor::new(iterate, checks, vars_count, variable_positions))
            }
            Execution::Single(single, checks) => {
                Self::Single(SingleExecutor::new(single, checks, variable_positions))
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
                Self::Optional(OptionalExecutor::new(PatternExecutor::new(plan, variable_positions)))
            }
        }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Option<Batch> {
        match self {
            StepExecutor::Sorted(sorted) => sorted.batch_from(input_batch),
            StepExecutor::Unsorted(unsorted) => unsorted.batch_from(input_batch),
            StepExecutor::Single(single) => single.batch_from(input_batch),
            StepExecutor::Disjunction(disjunction) => disjunction.batch_from(input_batch),
            StepExecutor::Negation(negation) => negation.batch_from(input_batch),
            StepExecutor::Optional(optional) => optional.batch_from(input_batch),
        }
    }

    fn batch_continue(&mut self) -> Option<Batch> {
        match self {
            StepExecutor::Sorted(sorted) => sorted.batch_continue(),
            StepExecutor::Unsorted(unsorted) => unsorted.batch_continue(),
            StepExecutor::Disjunction(disjunction) => disjunction.batch_continue(),
            StepExecutor::Optional(optional) => optional.batch_continue(),
            StepExecutor::Single(_) | StepExecutor::Negation(_) => None,
        }
    }
}

struct SortedExecutor {
    iterator_providers: Vec<ConstraintIteratorProvider>,
    // iterator:

    output_width: u32,
    output: Option<Batch>,
}

impl SortedExecutor {
    fn new(iterates: Vec<Iterate>, vars_count: u32, variable_positions: &HashMap<Variable, Position>) -> Self {
        let providers = iterates
            .into_iter()
            .map(|iterate| ConstraintIteratorProvider::new(iterate, variable_positions))
            .collect_vec();

        Self {
            iterator_providers: providers,
            output_width: vars_count,
            output: None,
        }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Option<Batch> {
        // TODO: avoid malloc
        // input_batch.into_rows_cloned()
        //     .flat_map(|row| {
        //         // TODO: can we avoid this malloc?
        //         let iterators = Vec::with_capacity(self.iterates.len());
        //
        //         for iter in &self.iterates {}
        //     })
        todo!()
    }

    fn batch_continue(&mut self) -> Option<Batch> {
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
    fn new(iterate: Iterate, checks: Vec<Check>, total_vars: u32, variable_positions: &HashMap<Variable, Position>) -> Self {
        Self {
            iterate,
            checks,
            output_width: total_vars,
            output: None,
        }
    }

    fn batch_from(&mut self, input_batch: Batch) -> Option<Batch> {
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

    fn batch_from(&mut self, input_batch: Batch) -> Option<Batch> {
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

    fn batch_from(&mut self, input_batch: Batch) -> Option<Batch> {
        todo!()
    }

    fn batch_continue(&mut self) -> Option<Batch> {
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

    fn batch_from(&mut self, input_batch: Batch) -> Option<Batch> {
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

    fn batch_from(&mut self, input_batch: Batch) -> Option<Batch> {
        todo!()
    }

    fn batch_continue(&mut self) -> Option<Batch> {
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
        Batch {
            width: width as usize,
            data: vec![VariableValue::Empty; size as usize],
        }
    }

    fn rows(&self) -> usize {
        // TODO adjust if batch is not full
        BATCH_ROWS_MAX as usize
    }

    fn into_rows_cloned(self) -> RowsIterator {
        RowsIterator { batch: self, index: 0 }
    }
}

struct RowsIterator {
    batch: Batch,
    index: usize,
}

impl Iterator for RowsIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index > self.batch.rows() {
            None
        } else {
            let slice = &self.batch.data[self.index * self.batch.width..(self.index + 1) * self.batch.width];
            self.index += 1;
            Some(Row { row: slice.to_vec() })
        }
    }
}

pub(crate) struct Row {
    row: Vec<VariableValue<'static>>,
}
