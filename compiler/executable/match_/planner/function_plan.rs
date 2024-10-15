/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use typeql::Variable;
use ir::pipeline::function_signature::FunctionID;
use crate::executable::function::ExecutableFunction;
use crate::executable::match_::planner::plan::ConjunctionPlan;
use crate::VariablePosition;

pub struct FunctionPlanRegistry {
    // Keep this abstraction in case we introduce function plan caching.
    all_plans: HashMap<FunctionID, FunctionPlan>,
}

impl FunctionPlanRegistry {

    pub(crate) fn new(all_plans: HashMap<FunctionID, FunctionPlan>) -> Self {
        Self { all_plans }
    }

    pub(crate) fn empty() -> Self {
        Self::new(HashMap::new())
    }
}

pub struct PipelinePlan {
    executable: ExecutableFunction,
    cost: f64,
}

pub struct FunctionPlan {
    plan: PipelinePlan,
    is_tabled: bool,
}
