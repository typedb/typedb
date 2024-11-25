/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable_value::VariableValue;
use compiler::{
    executable::match_::instructions::{Inputs, IsInstruction},
    ExecutorVariable, VariablePosition,
};
use concept::error::ConceptReadError;
use ir::pattern::constraint::Is;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    AsHkt, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use super::tuple::Tuple;
use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{TuplePositions, TupleResult},
        Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct IsExecutor {
    is: Is<ExecutorVariable>,
    input: VariablePosition,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    checker: Checker<AsHkt![VariableValue<'_>]>,
}

pub(crate) type IsToTupleFn = for<'a> fn(Result<VariableValue<'a>, Box<ConceptReadError>>) -> TupleResult<'a>;

pub(super) type IsTupleIterator<I> = Map<
    TryFilter<I, Box<IsFilterFn>, AsHkt![VariableValue<'_>], Box<ConceptReadError>>,
    IsToTupleFn,
    AsHkt![TupleResult<'_>],
>;

pub(super) type IsFilterFn = FilterFn<AsHkt![VariableValue<'_>]>;

pub(crate) type IsIterator =
    IsTupleIterator<lending_iterator::Once<Result<AsHkt![VariableValue<'_>], Box<ConceptReadError>>>>;

type IsVariableValueExtractor = for<'a, 'b> fn(&'a VariableValue<'b>) -> VariableValue<'a>;

pub(super) const EXTRACT_LHS: IsVariableValueExtractor = |lhs| lhs.as_reference();
pub(super) const EXTRACT_RHS: IsVariableValueExtractor = |rhs| rhs.as_reference();

fn is_to_tuple(result: Result<VariableValue<'_>, Box<ConceptReadError>>) -> TupleResult<'_> {
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
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter_for_row: Box<IsFilterFn> = self.checker.filter_for_row(context, &row);
        let input: VariableValue<'static> = row.get(self.input).clone().into_owned();

        fn as_tuples<T: for<'a> LendingIterator<Item<'a> = Result<VariableValue<'a>, Box<ConceptReadError>>>>(
            it: T,
        ) -> Map<T, IsToTupleFn, AsHkt![TupleResult<'_>]> {
            it.map::<TupleResult<'_>, IsToTupleFn>(is_to_tuple)
        }

        let iterator = lending_iterator::once(Ok(input));
        let as_tuples = as_tuples(iterator.try_filter::<_, IsFilterFn, VariableValue<'_>, _>(filter_for_row));

        Ok(TupleIterator::Is(SortedTupleIterator::new(as_tuples, self.tuple_positions.clone(), &self.variable_modes)))
    }
}

impl fmt::Display for IsExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}, input: {}", &self.is, &self.input)
    }
}
