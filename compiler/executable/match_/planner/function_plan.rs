/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::{
    executable::{function::ExecutableFunction},
};

pub struct ExecutableFunctionRegistry {
    // Keep this abstraction in case we introduce function plan caching.
    schema_functions: Arc<HashMap<DefinitionKey<'static>, ExecutableFunction>>,
    preamble_functions: HashMap<usize, ExecutableFunction>,
}

impl ExecutableFunctionRegistry {
    pub(crate) fn new(
        schema_functions: Arc<HashMap<DefinitionKey<'static>, ExecutableFunction>>,
        preamble_functions: HashMap<usize, ExecutableFunction>,
    ) -> Self {
        Self { schema_functions, preamble_functions }
    }

    pub(crate) fn empty() -> Self {
        Self::new(Arc::new(HashMap::new()), HashMap::new())
    }
}
