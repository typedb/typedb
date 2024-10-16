/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::{
    executable::{
        match_::planner::{
            function_plan::ExecutableFunctionRegistry,
            match_executable::{FunctionCallStep, NegationStep},
        },
        pipeline::ExecutableStage,
    },
    VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::pattern_executor::PatternExecutor,
    row::{MaybeOwnedRow, Row},
};

pub(super) enum NestedPatternExecutor {
    Disjunction(Vec<BaseNestedPatternExecutor<DisjunctionController>>),
    Negation(BaseNestedPatternExecutor<NegationController>),
    InlinedFunction(BaseNestedPatternExecutor<InlinedFunctionController>),
}

impl NestedPatternExecutor {
    pub(crate) fn prepare_all_branches(
        &mut self,
        input: FixedBatch,
        _context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> Result<(), ReadExecutionError> {
        // TODO: Better handling of the input. I can likely just keep the batch index into it.
        let as_rows = (0..input.len()).map(|i| input.get_row(i).into_owned()).collect();
        match self {
            NestedPatternExecutor::Negation(inner) => {
                inner.prepare(as_rows);
            }
            NestedPatternExecutor::Disjunction(branches) => {
                for branch in branches {
                    branch.prepare(as_rows.clone());
                }
            }
            NestedPatternExecutor::InlinedFunction(inner) => {
                inner.prepare(as_rows);
            }
        }
        Ok(())
    }

    pub(crate) fn branch_count(&self) -> usize {
        match self {
            NestedPatternExecutor::Negation(_) => 1,
            NestedPatternExecutor::Disjunction(branches) => branches.len(),
            NestedPatternExecutor::InlinedFunction(_) => 1,
        }
    }
}

impl NestedPatternExecutor {
    pub(crate) fn new_negation(
        plan: &NegationStep,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        function_registry: &ExecutableFunctionRegistry,
    ) -> Result<NestedPatternExecutor, ConceptReadError> {
        let inner = PatternExecutor::build(&plan.negation, snapshot, thing_manager, function_registry)?;
        Ok(Self::Negation(BaseNestedPatternExecutor::new(inner, NegationController::new())))
    }

    pub(crate) fn new_inline_function(
        plan: &FunctionCallStep,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        function_registry: &ExecutableFunctionRegistry,
    ) -> Result<NestedPatternExecutor, ConceptReadError> {
        let function = function_registry.get(plan.function_id.clone());
        debug_assert!(!function.is_tabled);
        let ExecutableStage::Match(match_) = &function.executable_stages[0] else { todo!("Pipelines in functions") };
        let inner = PatternExecutor::build(match_, snapshot, thing_manager, function_registry)?;
        Ok(Self::InlinedFunction(BaseNestedPatternExecutor::new(
            inner,
            InlinedFunctionController::new(plan.arguments.clone(), plan.assigned.clone(), plan.output_width),
        )))
    }
}

pub(super) struct BaseNestedPatternExecutor<Controller: NestedPatternController> {
    inputs: Vec<MaybeOwnedRow<'static>>,
    pattern_executor: PatternExecutor,
    controller: Controller,
}

impl<Controller: NestedPatternController> BaseNestedPatternExecutor<Controller> {
    fn new(pattern_executor: PatternExecutor, controller: Controller) -> BaseNestedPatternExecutor<Controller> {
        Self { inputs: Vec::new(), pattern_executor, controller }
    }
}

impl<Controller: NestedPatternController> BaseNestedPatternExecutor<Controller> {
    fn prepare(&mut self, inputs: Vec<MaybeOwnedRow<'static>>) {
        self.pattern_executor.reset();
        self.controller.reset();
        self.inputs = inputs;
    }

    pub(super) fn get_or_next_executing_pattern(&mut self) -> Option<&mut PatternExecutor> {
        if self.controller.is_active() && self.pattern_executor.stack_top().is_some() {
            Some(&mut self.pattern_executor)
        } else if let Some(row) = self.inputs.pop() {
            self.controller.prepare_and_get_subpattern_input(row.clone());
            self.pattern_executor.prepare(FixedBatch::from(row.clone()));
            Some(&mut self.pattern_executor)
        } else {
            None
        }
    }

    pub(super) fn process_result(&mut self, result: Option<FixedBatch>) -> Option<FixedBatch> {
        match self.controller.process_result(result) {
            NestedPatternControllerResult::Regular(processed) => processed,
            NestedPatternControllerResult::ShortCircuit(processed) => {
                self.pattern_executor.reset(); // That should work?
                self.controller.reset();
                processed
            }
        }
    }
}

// Controllers
pub(super) trait NestedPatternController {
    fn reset(&mut self);
    fn is_active(&self) -> bool;
    fn prepare_and_get_subpattern_input(&mut self, row: MaybeOwnedRow<'static>) -> MaybeOwnedRow<'static>;
    // None will cause the pattern_executor to backtrack. You can also choose to call short-circuit on the pattern_executor
    fn process_result(&mut self, result: Option<FixedBatch>) -> NestedPatternControllerResult;
}

enum NestedPatternControllerResult {
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

impl NestedPatternController for NegationController {
    fn reset(&mut self) {
        self.input = None;
    }

    fn is_active(&self) -> bool {
        self.input.is_some()
    }

    fn prepare_and_get_subpattern_input(&mut self, row: MaybeOwnedRow<'static>) -> MaybeOwnedRow<'static> {
        self.input = Some(row.clone());
        row
    }

    fn process_result(&mut self, result: Option<FixedBatch>) -> NestedPatternControllerResult {
        if result.is_some() {
            NestedPatternControllerResult::ShortCircuit(None)
        } else {
            debug_assert!(self.input.is_some());
            // Doesn't need to short-circuit because the nested pattern is exhausted.
            NestedPatternControllerResult::Regular(Some(FixedBatch::from(self.input.take().unwrap())))
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

impl NestedPatternController for DisjunctionController {
    fn reset(&mut self) {
        self.input = None;
    }

    fn is_active(&self) -> bool {
        self.input.is_some()
    }

    fn prepare_and_get_subpattern_input(&mut self, row: MaybeOwnedRow<'static>) -> MaybeOwnedRow<'static> {
        self.input = Some(row.clone());
        row
    }

    fn process_result(&mut self, result: Option<FixedBatch>) -> NestedPatternControllerResult {
        NestedPatternControllerResult::Regular(result)
    }
}

pub(super) struct InlinedFunctionController {
    input: Option<MaybeOwnedRow<'static>>,
    arguments: Vec<VariablePosition>, // caller input -> callee input
    assigned: Vec<VariablePosition>,  // callee return -> caller output
    output_width: u32,
}

impl InlinedFunctionController {
    fn new(arguments: Vec<VariablePosition>, assigned: Vec<VariablePosition>, output_width: u32) -> Self {
        Self { input: None, arguments, assigned, output_width }
    }
}

impl NestedPatternController for InlinedFunctionController {
    fn reset(&mut self) {
        self.input = None;
    }

    fn is_active(&self) -> bool {
        self.input.is_some()
    }

    fn prepare_and_get_subpattern_input(&mut self, row: MaybeOwnedRow<'static>) -> MaybeOwnedRow<'static> {
        let args = self.arguments.iter().map(|pos| row.get(pos.clone()).clone().into_owned()).collect();
        let callee_input = MaybeOwnedRow::new_owned(args, row.multiplicity());
        self.input = Some(row);
        callee_input
    }

    fn process_result(&mut self, result: Option<FixedBatch>) -> NestedPatternControllerResult {
        let Self { input, assigned, output_width, .. } = self;
        debug_assert!(input.is_some());
        match result {
            None => NestedPatternControllerResult::Regular(None),
            Some(returned_batch) => {
                let input = input.as_ref().unwrap();
                let mut output_batch = FixedBatch::new(*output_width);
                for return_index in 0..returned_batch.len() {
                    // TODO: Deduplicate?
                    let returned_row = returned_batch.get_row(return_index);
                    output_batch.append(|mut output_row| {
                        for (i, element) in input.iter().enumerate() {
                            output_row.set(VariablePosition::new(i as u32), element.clone());
                        }
                        for (returned_index, output_position) in assigned.iter().enumerate() {
                            output_row.set(output_position.clone(), returned_row[returned_index].clone().into_owned());
                        }
                    });
                }
                NestedPatternControllerResult::Regular(Some(output_batch))
            }
        }
    }
}
