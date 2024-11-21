/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter},
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::SubInstruction, ExecutorVariable};
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
        tuple::{sub_to_tuple_sub_super, sub_to_tuple_super_sub, SubToTupleFn, TuplePositions, TupleResult},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct SubExecutor {
    sub: ir::pattern::constraint::Sub<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>,
    supertypes: Arc<BTreeSet<Type>>,
    filter_fn: Arc<SubFilterFn>,
    checker: Checker<AdHocHkt<(Type, Type)>>,
}

pub(super) type SubTupleIterator<I> = NarrowingTupleIterator<
    Map<
        TryFilter<I, Box<SubFilterFn>, (Type, Type), Box<ConceptReadError>>,
        SubToTupleFn,
        AdHocHkt<TupleResult<'static>>,
    >,
>;

pub(super) type SubUnboundedSortedSub =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>>;
pub(super) type SubBoundedSortedSuper =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>>;

pub(super) type SubFilterFn = FilterFn<(Type, Type)>;

type SubVariableValueExtractor = fn(&(Type, Type)) -> VariableValue<'_>;
pub(super) const EXTRACT_SUB: SubVariableValueExtractor = |(sub, _)| VariableValue::Type(sub.clone());
pub(super) const EXTRACT_SUPER: SubVariableValueExtractor = |(_, sup)| VariableValue::Type(sup.clone());

pub(crate) struct NarrowingTupleIterator<I>(pub I);

impl<I> LendingIterator for NarrowingTupleIterator<I>
where
    I: for<'a> LendingIterator<Item<'a> = TupleResult<'static>>,
{
    type Item<'a> = TupleResult<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.0.next()
    }
}

impl SubExecutor {
    pub(crate) fn new(
        sub: SubInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let supertypes = sub.supertypes().clone();
        let sub_to_supertypes = sub.sub_to_supertypes().clone();
        debug_assert!(supertypes.len() > 0);

        let SubInstruction { sub, checks, .. } = sub;

        let iterate_mode = BinaryIterateMode::new(sub.subtype(), sub.supertype(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_sub_filter_sub_super(sub_to_supertypes.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_sub_filter_super(supertypes.clone())
            }
        };

        let subtype = sub.subtype().as_variable();
        let supertype = sub.supertype().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([subtype, supertype]),
            _ => TuplePositions::Pair([supertype, subtype]),
        };

        let checker = Checker::<AdHocHkt<(Type, Type)>>::new(
            checks,
            [(subtype, EXTRACT_SUB), (supertype, EXTRACT_SUPER)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            sub,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            sub_to_supertypes,
            supertypes,
            filter_fn,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<SubFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let sub_with_super = self
                    .sub_to_supertypes
                    .iter()
                    .flat_map(|(sub, supers)| supers.iter().map(|sup| Ok((sub.clone(), sup.clone()))))
                    .collect_vec();
                let as_tuples: SubUnboundedSortedSub = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_sub_super),
                );
                Ok(TupleIterator::SubUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                let subtype = type_from_row_or_annotations(self.sub.subtype(), row, self.sub_to_supertypes.keys());
                let supertypes = self.sub_to_supertypes.get(&subtype).unwrap_or(const { &Vec::new() });
                let sub_with_super = supertypes.iter().map(|sup| Ok((subtype.clone(), sup.clone()))).collect_vec(); // TODO cache this

                let as_tuples: SubBoundedSortedSuper = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_super_sub),
                );
                Ok(TupleIterator::SubBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl Display for SubExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "[{}], mode={}", &self.sub, &self.iterate_mode)
    }
}

fn create_sub_filter_sub_super(sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, sup)) => match sub_to_supertypes.get(sub) {
            Some(supertypes) => Ok(supertypes.contains(sup)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_super(supertypes: Arc<BTreeSet<Type>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, sup)) => Ok(supertypes.contains(sup)),
        Err(err) => Err(err.clone()),
    })
}
