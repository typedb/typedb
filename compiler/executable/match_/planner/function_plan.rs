/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, sync::Arc};

use encoding::graph::definition::definition_key::DefinitionKey;
use ir::pipeline::function_signature::FunctionID;

use crate::executable::{
    function::{ExecutableFunction, FunctionCallCostProvider},
    match_::planner::vertex::Cost,
};

#[derive(Clone)]
pub struct ExecutableFunctionRegistry {
    // Keep this abstraction in case we introduce function plan caching.
    schema_functions: Arc<HashMap<DefinitionKey, ExecutableFunction>>,
    preamble_functions: HashMap<usize, ExecutableFunction>,
}

impl fmt::Debug for ExecutableFunctionRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExecutableFunctionRegistry { omitted }")
    }
}

impl ExecutableFunctionRegistry {
    pub(crate) fn new(
        schema_functions: Arc<HashMap<DefinitionKey, ExecutableFunction>>,
        preamble_functions: HashMap<usize, ExecutableFunction>,
    ) -> Self {
        Self { schema_functions, preamble_functions }
    }

    pub fn empty() -> Self {
        Self::new(Arc::new(HashMap::new()), HashMap::new())
    }

    pub fn get(&self, function_id: &FunctionID) -> Option<&ExecutableFunction> {
        match function_id {
            FunctionID::Schema(id) => self.schema_functions.get(id),
            FunctionID::Preamble(id) => self.preamble_functions.get(id),
        }
    }

    pub(crate) fn schema_functions(&self) -> Arc<HashMap<DefinitionKey, ExecutableFunction>> {
        self.schema_functions.clone()
    }
}

impl FunctionCallCostProvider for ExecutableFunctionRegistry {
    fn get_call_cost(&self, function_id: &FunctionID) -> Cost {
        self.get(function_id).unwrap().single_call_cost
    }
}
