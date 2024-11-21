/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    iter, vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::TypeListInstruction, ExecutorVariable};
use concept::error::ConceptReadError;
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    higher_order::AdHocHkt,
    AsLendingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        sub_executor::NarrowingTupleIterator,
        tuple::{type_to_tuple, TuplePositions, TupleResult, TypeToTupleFn},
        Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct TypeListExecutor {
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    types: Vec<Type>,
    checker: Checker<Type>,
}

pub(super) type TypeFilterFn = FilterFn<Type>;

pub(super) type TypeTupleIterator<I> = NarrowingTupleIterator<
    Map<TryFilter<I, Box<TypeFilterFn>, Type, Box<ConceptReadError>>, TypeToTupleFn, AdHocHkt<TupleResult<'static>>>,
>;

pub(crate) type TypeIterator = TypeTupleIterator<
    AsLendingIterator<iter::Map<vec::IntoIter<Type>, fn(Type) -> Result<Type, Box<ConceptReadError>>>>,
>;

type TypeVariableValueExtractor = for<'a> fn(&'a Type) -> VariableValue<'a>;
pub(super) const EXTRACT_TYPE: TypeVariableValueExtractor = |ty| VariableValue::Type(ty.clone());

impl TypeListExecutor {
    pub(crate) fn new(
        type_: TypeListInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        _sort_by: ExecutorVariable,
    ) -> Self {
        debug_assert!(!variable_modes.all_inputs());
        let types = type_.types().iter().cloned().sorted().collect_vec();
        debug_assert!(!types.is_empty());
        let TypeListInstruction { type_var, checks, .. } = type_;
        debug_assert_eq!(type_var, _sort_by);
        let tuple_positions = TuplePositions::Single([Some(type_var)]);

        let type_ = type_.type_var;

        let checker = Checker::<Type>::new(checks, HashMap::from([(type_, EXTRACT_TYPE)]));

        Self { variable_modes, tuple_positions, types, checker }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter_for_row: Box<TypeFilterFn> = self.checker.filter_for_row(context, &row);
        let iterator = self.types.clone().into_iter().map(Ok as _);
        let as_tuples: TypeIterator = NarrowingTupleIterator(
            AsLendingIterator::new(iterator).try_filter::<_, TypeFilterFn, Type, _>(filter_for_row).map(type_to_tuple),
        );
        Ok(TupleIterator::Type(SortedTupleIterator::new(as_tuples, self.tuple_positions.clone(), &self.variable_modes)))
    }
}

impl Display for TypeListExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Type [")?;
        for type_ in &self.types {
            write!(f, "{}, ", type_)?;
        }
        writeln!(f, "], variable modes={:?}", &self.variable_modes)
    }
}
