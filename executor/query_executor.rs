// /*
//  * This Source Code Form is subject to the terms of the Mozilla Public
//  * License, v. 2.0. If a copy of the MPL was not distributed with this
//  * file, You can obtain one at https://mozilla.org/MPL/2.0/.
//  */
//
// use std::collections::HashMap;
// use answer::variable_value::VariableValue;
// use ir::program::function_signature::FunctionID;
// use crate::row::MaybeOwnedRow;
//
// enum FunctionCallID {
//     Root,
//     TabledFunction(FunctionID, Vec<VariableValue<'static>>), // Untabled ones are owned by the step.
// }
//
// struct QueryExecutor {
//     function_execution_states: HashMap<FunctionCallID, FunctionExecutionState>,
//     function_execution_stack: Vec<FunctionCallID>
// }
//
// pub struct SuspensionPoint {
//     function_call_id: FunctionCallID,
//     step_index: usize,
//     input_row: Vec<VariableValue<'static>> // Definitely owned row.
// }
//
// pub struct FunctionExecutionState {
//     active_executor: Option<PatternExecutor>,
//     table: (), // Future use
//     suspension_points: Vec<SuspensionPoint>
// }
//
//
