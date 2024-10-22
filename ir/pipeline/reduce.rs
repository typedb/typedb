/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;

#[derive(Debug, Clone)]
pub struct Reduce {
    pub assigned_reductions: Vec<(Variable, Reducer)>,
    pub within_group: Vec<Variable>,
}

impl Reduce {
    pub(crate) fn new(assigned_reductions: Vec<(Variable, Reducer)>, within_group: Vec<Variable>) -> Self {
        Self { assigned_reductions, within_group }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Reducer {
    Count,
    CountVar(Variable),
    Sum(Variable),
    Max(Variable),
    Mean(Variable),
    Median(Variable),
    Min(Variable),
    Std(Variable),
    // First, Any etc.
}

impl Reducer {
    pub fn name(&self) -> String {
        match self {
            Self::Count => typeql::token::ReduceOperator::Count.to_string(),
            Self::CountVar(_) => typeql::token::ReduceOperator::Count.to_string(),
            Self::Sum(_) => typeql::token::ReduceOperator::Sum.to_string(),
            Self::Max(_) => typeql::token::ReduceOperator::Max.to_string(),
            Self::Mean(_) => typeql::token::ReduceOperator::Mean.to_string(),
            Self::Median(_) => typeql::token::ReduceOperator::Median.to_string(),
            Self::Min(_) => typeql::token::ReduceOperator::Min.to_string(),
            Self::Std(_) => typeql::token::ReduceOperator::Std.to_string(),
        }
    }

    pub fn variable(&self) -> Option<Variable> {
        match self {
            Self::Count => None,
            Self::CountVar(var)
            | Self::Sum(var)
            | Self::Max(var)
            | Self::Mean(var)
            | Self::Median(var)
            | Self::Min(var)
            | Self::Std(var) => Some(*var),
        }
    }
}
