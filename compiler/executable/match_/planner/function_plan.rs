/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use encoding::graph::definition::definition_key::DefinitionKey;
use ir::pipeline::function_signature::FunctionID;

use crate::executable::function::ExecutableFunction;

pub struct ExecutableFunctionRegistry {
    // Keep this abstraction in case we introduce function plan caching.
    schema_functions: Arc<HashMap<DefinitionKey<'static>, ExecutableFunction>>,
    preamble_functions: HashMap<usize, ExecutableFunction>,
}

impl Debug for ExecutableFunctionRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExecutableFunctionRegistry { omitted }")
    }
}

impl ExecutableFunctionRegistry {
    pub(crate) fn new(
        schema_functions: Arc<HashMap<DefinitionKey<'static>, ExecutableFunction>>,
        preamble_functions: HashMap<usize, ExecutableFunction>,
    ) -> Self {
        Self { schema_functions, preamble_functions }
    }

    pub fn empty() -> Self {
        Self::new(Arc::new(HashMap::new()), HashMap::new())
    }

    pub fn get(&self, function_id: FunctionID) -> &ExecutableFunction {
        match &function_id {
            FunctionID::Schema(id) => self.schema_functions.get(id).unwrap(),
            FunctionID::Preamble(id) => self.preamble_functions.get(id).unwrap(),
        }
    }

    // TODO: Find all references, update them and remove this.
    pub fn TODO__empty() -> Self {
        Self::empty()
    }
}
