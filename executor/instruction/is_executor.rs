/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, iter};

use answer::variable_value::VariableValue;
use compiler::{
    executable::match_::instructions::{Inputs, IsInstruction},
    ExecutorVariable, VariablePosition,
};
use concept::error::ConceptReadError;
use ir::pattern::constraint::Is;
use lending_iterator::AsLendingIterator;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{NaiiveSeekable, SortedTupleIterator, TupleIterator},
        tuple::{Tuple, TuplePositions, TupleResult},
        Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct IsExecutor {
    is: Is<ExecutorVariable>,
    input: VariablePosition,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    checker: Checker<VariableValue<'static>>,
}

pub(crate) type IsToTupleFn = fn(Result<VariableValue<'static>, Box<ConceptReadError>>) -> TupleResult<'static>;

pub(super) type IsTupleIterator<I> =
    NaiiveSeekable<AsLendingIterator<iter::Map<iter::FilterMap<I, Box<IsFilterMapFn>>, IsToTupleFn>>>;

pub(super) type IsFilterFn = FilterFn<VariableValue<'static>>;
pub(super) type IsFilterMapFn = FilterMapUnchangedFn<VariableValue<'static>>;

pub(crate) type IsIterator = IsTupleIterator<iter::Once<Result<VariableValue<'static>, Box<ConceptReadError>>>>;

type IsVariableValueExtractor = for<'a, 'b> fn(&'a VariableValue<'b>) -> VariableValue<'a>;

pub(super) const EXTRACT_LHS: IsVariableValueExtractor = |lhs| lhs.as_reference();
pub(super) const EXTRACT_RHS: IsVariableValueExtractor = |rhs| rhs.as_reference();

fn is_to_tuple(result: Result<VariableValue<'static>, Box<ConceptReadError>>) -> TupleResult<'static> {
    match result {
        Ok(value) => Ok(Tuple::Pair([value.clone(), value])),
        Err(err) => Err(err),
    }
}

impl IsExecutor {
    pub(crate) fn new(
        is: IsInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        _sort_by: ExecutorVariable,
    ) -> Self {
        let IsInstruction { is, inputs, checks } = is;

        let lhs = is.lhs().as_variable().unwrap();
        let rhs = is.rhs().as_variable().unwrap();

        let output_tuple_positions = TuplePositions::Pair([Some(lhs), Some(rhs)]);

        let checker =
            Checker::<VariableValue<'_>>::new(checks, HashMap::from_iter([(lhs, EXTRACT_LHS), (rhs, EXTRACT_RHS)]));

        let Inputs::Single([ExecutorVariable::RowPosition(input)]) = inputs else {
            unreachable!("expected single input for IsExecutor ({inputs:?})")
        };

        Self { is, input, variable_modes, tuple_positions: output_tuple_positions, checker }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let check = self.checker.filter_for_row(context, &row, storage_counters);
        let filter_for_row: Box<IsFilterMapFn> = Box::new(move |item| match check(&item) {
            Ok(true) | Err(_) => Some(item),
            Ok(false) => None,
        });

        let input: VariableValue<'static> = row.get(self.input).clone().into_owned();
        let as_tuples = iter::once(Ok(input)).filter_map(filter_for_row).map(is_to_tuple as _);
        let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
        Ok(TupleIterator::Is(SortedTupleIterator::new(
            lending_tuples,
            self.tuple_positions.clone(),
            &self.variable_modes,
        )))
    }
}

impl fmt::Display for IsExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}, input: {}", &self.is, &self.input)
    }
}
