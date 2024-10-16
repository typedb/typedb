/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::match_::planner::match_executable::NegationStep;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch, error::ReadExecutionError, pipeline::stage::ExecutionContext,
    read::pattern_executor::PatternExecutor, row::MaybeOwnedRow,
};

pub(super) enum SubPatternExecutor {
    Disjunction(Vec<BaseSubPatternExecutor<DisjunctionController>>),
    Negation(BaseSubPatternExecutor<NegationController>),
}

impl SubPatternExecutor {
    pub(crate) fn prepare_all_branches(
        &mut self,
        input: FixedBatch,
        _context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        // TODO: Better handling of the input. I can likely just keep the batch index into it.
        let as_rows = (0..input.len()).map(|i| input.get_row(i).into_owned()).collect();
        match self {
            SubPatternExecutor::Negation(inner) => {
                inner.prepare(as_rows);
            }
            SubPatternExecutor::Disjunction(branches) => {
                for branch in branches {
                    branch.prepare(as_rows.clone());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn branch_count(&self) -> usize {
        match self {
            SubPatternExecutor::Negation(_) => 1,
            SubPatternExecutor::Disjunction(branches) => branches.len(),
        }
    }
}

impl SubPatternExecutor {
    pub(crate) fn new_negation(
        plan: &NegationStep,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
    ) -> Result<SubPatternExecutor, ConceptReadError> {
        let inner = PatternExecutor::build(&plan.negation, snapshot, thing_manager)?;
        Ok(Self::Negation(BaseSubPatternExecutor::new(inner, NegationController::new())))
    }
}

pub(super) struct BaseSubPatternExecutor<Controller: SubPatternController> {
    inputs: Vec<MaybeOwnedRow<'static>>,
    pattern_executor: PatternExecutor,
    controller: Controller,
}

impl<Controller: SubPatternController> BaseSubPatternExecutor<Controller> {
    fn new(pattern_executor: PatternExecutor, controller: Controller) -> BaseSubPatternExecutor<Controller> {
        Self { inputs: Vec::new(), pattern_executor, controller }
    }
}

impl<Controller: SubPatternController> BaseSubPatternExecutor<Controller> {
    fn prepare(&mut self, inputs: Vec<MaybeOwnedRow<'static>>) {
        self.pattern_executor.reset();
        self.controller.reset();
        self.inputs = inputs;
    }

    pub(super) fn get_or_next_executing_pattern(&mut self) -> Option<&mut PatternExecutor> {
        if self.controller.is_active() && self.pattern_executor.stack_top().is_some() {
            Some(&mut self.pattern_executor)
        } else if let Some(row) = self.inputs.pop() {
            self.pattern_executor.prepare(FixedBatch::from(row.clone()));
            self.controller.prepare(row.clone());
            Some(&mut self.pattern_executor)
        } else {
            None
        }
    }

    pub(super) fn process_result(&mut self, result: Option<FixedBatch>) -> Option<FixedBatch> {
        match self.controller.process_result(result) {
            SubQueryControllerResult::Regular(processed) => processed,
            SubQueryControllerResult::ShortCircuit(processed) => {
                self.pattern_executor.reset(); // That should work?
                self.controller.reset();
                processed
            }
        }
    }
}

// Controllers
pub(super) trait SubPatternController {
    fn reset(&mut self);
    fn is_active(&self) -> bool;
    fn prepare(&mut self, row: MaybeOwnedRow<'static>);
    // None will cause the pattern_executor to backtrack. You can also choose to call short-circuit on the pattern_executor
    fn process_result(&mut self, result: Option<FixedBatch>) -> SubQueryControllerResult;
}

enum SubQueryControllerResult {
    Regular(Option<FixedBatch>),
    ShortCircuit(Option<FixedBatch>),
}

pub(super) struct NegationController {
    input: Option<MaybeOwnedRow<'static>>,
}

impl NegationController {
    fn new() -> Self {
        Self { input: None }
    }
}

impl SubPatternController for NegationController {
    fn reset(&mut self) {
        self.input = None;
    }

    fn is_active(&self) -> bool {
        self.input.is_some()
    }

    fn prepare(&mut self, row: MaybeOwnedRow<'static>) {
        self.input = Some(row);
    }

    fn process_result(&mut self, result: Option<FixedBatch>) -> SubQueryControllerResult {
        if result.is_some() {
            SubQueryControllerResult::ShortCircuit(None)
        } else {
            debug_assert!(self.input.is_some());
            // Doesn't need to short-circuit because the subpattern is exhausted.
            SubQueryControllerResult::Regular(Some(FixedBatch::from(self.input.take().unwrap())))
        }
    }
}
pub(super) struct DisjunctionController {
    input: Option<MaybeOwnedRow<'static>>,
}

impl DisjunctionController {
    fn new() -> Self {
        Self { input: None }
    }
}

impl SubPatternController for DisjunctionController {
    fn reset(&mut self) {
        self.input = None;
    }

    fn is_active(&self) -> bool {
        self.input.is_some()
    }

    fn prepare(&mut self, row: MaybeOwnedRow<'static>) {
        self.input = Some(row);
    }

    fn process_result(&mut self, result: Option<FixedBatch>) -> SubQueryControllerResult {
        SubQueryControllerResult::Regular(result)
    }
}
