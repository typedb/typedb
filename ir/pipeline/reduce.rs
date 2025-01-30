/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    hash::{DefaultHasher, Hasher},
    mem,
};

use answer::variable::Variable;
use structural_equality::StructuralEquality;
use typeql::common::Span;

#[derive(Debug, Clone)]
pub struct Reduce {
    pub assigned_reductions: Vec<AssignedReduction>,
    pub groupby: Vec<Variable>,
    source_span: Option<Span>,
}

impl Reduce {
    pub(crate) fn new(
        assigned_reductions: Vec<AssignedReduction>,
        groupby: Vec<Variable>,
        source_span: Option<Span>,
    ) -> Self {
        Self { assigned_reductions, groupby, source_span }
    }

    pub fn variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.assigned_reductions
            .iter()
            .map(|assign_reduction| &assign_reduction.assigned)
            .cloned()
            .chain(self.groupby.iter().cloned())
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl StructuralEquality for Reduce {
    fn hash(&self) -> u64 {
        // TODO: these really could be order-free
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.assigned_reductions.hash());
        hasher.write_u64(self.groupby.hash());
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        // TODO: these really could be order-free
        self.assigned_reductions.equals(&other.assigned_reductions) && self.groupby.equals(&other.groupby)
    }
}

#[derive(Debug, Clone)]
pub struct AssignedReduction {
    pub assigned: Variable,
    pub reduction: Reducer,
}

impl AssignedReduction {
    pub(crate) fn new(assigned: Variable, reduction: Reducer) -> Self {
        Self { assigned, reduction }
    }
}

impl StructuralEquality for AssignedReduction {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.assigned.hash());
        hasher.write_u64(self.reduction.hash());
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.assigned.equals(&other.assigned) && self.reduction.equals(&other.reduction)
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

impl StructuralEquality for Reducer {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash() ^ self.variable().hash()
    }

    fn equals(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other) && self.variable().equals(&other.variable())
    }
}
