/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, iter, vec};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::TypeListInstruction, ExecutorVariable};
use concept::error::ConceptReadError;
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{type_to_tuple, TuplePositions, TypeToTupleFn},
        Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct TypeListExecutor {
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    types: Vec<Type>,
    checker: Checker<Type>,
}

pub(super) type TypeFilterFn = FilterFn<Type>;
pub(super) type TypeFilterMapFn = FilterMapUnchangedFn<Type>;

pub(super) type TypeTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<TypeFilterMapFn>>, TypeToTupleFn>;

pub(crate) type TypeIterator =
    TypeTupleIterator<iter::Map<vec::IntoIter<Type>, fn(Type) -> Result<Type, Box<ConceptReadError>>>>;

type TypeVariableValueExtractor = for<'a> fn(&'a Type) -> VariableValue<'a>;
pub(super) const EXTRACT_TYPE: TypeVariableValueExtractor = |ty| VariableValue::Type(*ty);

impl TypeListExecutor {
    pub(crate) fn new(
        type_: TypeListInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        _sort_by: ExecutorVariable,
    ) -> Self {
        debug_assert!(!variable_modes.all_inputs());
        let types = type_.types().iter().cloned().sorted().collect_vec();
        // This instruction that needs to supports empty type-lists
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
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<TypeFilterMapFn> = Box::new(move |item| match check(&item) {
            Ok(true) | Err(_) => Some(item),
            Ok(false) => None,
        });
        let iterator = self.types.clone().into_iter().map(Ok as _);
        let as_tuples: TypeIterator = iterator.filter_map(filter_for_row).map(type_to_tuple);
        Ok(TupleIterator::Type(SortedTupleIterator::new(as_tuples, self.tuple_positions.clone(), &self.variable_modes)))
    }
}

impl fmt::Display for TypeListExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Type [")?;
        for type_ in &self.types {
            write!(f, "{}, ", type_)?;
        }
        writeln!(f, "], variable modes={:?}", &self.variable_modes)
    }
}
