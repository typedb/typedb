/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{
    executable::match_::instructions::type_::{AsInstruction, SubInstruction},
    ExecutorVariable,
};
use concept::{
    error::ConceptReadError,
    type_::{type_manager::TypeManager, TypeAPI},
};
use ir::pattern::constraint::SubKind;
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
        tuple::{
            as_to_tuple_specialised_specialising, as_to_tuple_specialising_specialised, sub_to_tuple_sub_super,
            sub_to_tuple_super_sub, AsToTupleFn, SubToTupleFn, TuplePositions, TupleResult,
        },
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct AsExecutor {
    as_: ir::pattern::constraint::As<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    specialising_to_specialised: Arc<BTreeMap<Type, Vec<Type>>>,
    specialised: Arc<BTreeSet<Type>>,
    filter_fn: Arc<AsFilterFn>,
    checker: Checker<AdHocHkt<(Type, Type)>>,
}

pub(super) type AsTupleIterator<I> = NarrowingTupleIterator<
    Map<TryFilter<I, Box<AsFilterFn>, (Type, Type), ConceptReadError>, AsToTupleFn, AdHocHkt<TupleResult<'static>>>,
>;

pub(super) type AsUnboundedSortedSpecialising =
    AsTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;
pub(super) type AsBoundedSortedSpecialised =
    AsTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;

pub(super) type AsFilterFn = FilterFn<(Type, Type)>;

type AsVariableValueExtractor = fn(&(Type, Type)) -> VariableValue<'_>;
pub(super) const EXTRACT_SPECIALISING: AsVariableValueExtractor =
    |(specialising, _)| VariableValue::Type(specialising.clone());
pub(super) const EXTRACT_SPECIALISED: AsVariableValueExtractor =
    |(_, specialised)| VariableValue::Type(specialised.clone());

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

impl AsExecutor {
    pub(crate) fn new(
        as_: AsInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let specialised = as_.specialised().clone();
        let specialising_to_specialised = as_.specialising_to_specialised().clone();
        debug_assert!(specialised.len() > 0);

        let AsInstruction { as_, checks, .. } = as_;

        let iterate_mode = BinaryIterateMode::new(as_.specialising(), as_.specialised(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => {
                create_as_filter_specialising_specialised(specialising_to_specialised.clone())
            }
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_as_filter_specialised(specialised.clone())
            }
        };

        let as_specialising = as_.specialising().as_variable();
        let as_specialised = as_.specialised().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([as_specialising, as_specialised]),
            _ => TuplePositions::Pair([as_specialised, as_specialising]),
        };

        let checker = Checker::<AdHocHkt<(Type, Type)>>::new(
            checks,
            [(as_specialising, EXTRACT_SPECIALISING), (as_specialised, EXTRACT_SPECIALISED)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            as_,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            specialising_to_specialised,
            specialised,
            filter_fn,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<AsFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let specialising_with_specialised = self
                    .specialising_to_specialised
                    .iter()
                    .flat_map(|(specialising, specialised)| {
                        assert_eq!(specialised.len(), 1, "As constraint is not inherited");
                        specialised.iter().map(|specialised| Ok((specialising.clone(), specialised.clone())))
                    })
                    .collect_vec();
                let as_tuples: AsUnboundedSortedSpecialising = NarrowingTupleIterator(
                    AsLendingIterator::new(specialising_with_specialised)
                        .try_filter::<_, AsFilterFn, (Type, Type), _>(filter_for_row)
                        .map(as_to_tuple_specialising_specialised),
                );
                Ok(TupleIterator::AsUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                let specialising =
                    type_from_row_or_annotations(self.as_.specialising(), row, self.specialising_to_specialised.keys());
                let type_manager = context.type_manager();
                let specialised = get_specialised(snapshot, type_manager, &specialising)?;
                let specialising_with_specialised = match specialised.map(|specialised| Ok((specialising, specialised)))
                {
                    Some(result) => vec![result],
                    None => vec![],
                }; // TODO cache this

                let as_tuples: AsBoundedSortedSpecialised = NarrowingTupleIterator(
                    AsLendingIterator::new(specialising_with_specialised)
                        .try_filter::<_, AsFilterFn, (Type, Type), _>(filter_for_row)
                        .map(as_to_tuple_specialised_specialising),
                );
                Ok(TupleIterator::AsBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

pub(super) fn get_specialised(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    specialising: &Type,
) -> Result<Option<Type>, ConceptReadError> {
    let supertype = match specialising {
        Type::Entity(_) | Type::Relation(_) | Type::Attribute(_) => unreachable!("Only RoleType can be specialised"),
        Type::RoleType(type_) => type_.get_supertype(snapshot, type_manager)?.map(Type::RoleType),
    };
    Ok(supertype)
}

fn create_as_filter_specialising_specialised(
    specialising_to_specialised: Arc<BTreeMap<Type, Vec<Type>>>,
) -> Arc<AsFilterFn> {
    Arc::new(move |result| match result {
        Ok((specialising, specialised)) => match specialising_to_specialised.get(specialising) {
            Some(all_specialised) => Ok(all_specialised.contains(specialised)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_as_filter_specialised(all_specialised: Arc<BTreeSet<Type>>) -> Arc<AsFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, specialised)) => Ok(all_specialised.contains(specialised)),
        Err(err) => Err(err.clone()),
    })
}
