/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    vec,
};

use answer::Type;
use compiler::{
    executable::match_::instructions::type_::{AsReverseInstruction, SubReverseInstruction},
    ExecutorVariable,
};
use concept::{
    error::ConceptReadError,
    type_::{type_manager::TypeManager, TypeAPI},
};
use ir::pattern::constraint::SubKind;
use itertools::Itertools;
use lending_iterator::{higher_order::AdHocHkt, AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        as_executor::{AsTupleIterator, NarrowingTupleIterator, EXTRACT_SPECIALISED, EXTRACT_SPECIALISING},
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{
            as_to_tuple_specialised_specialising, as_to_tuple_specialising_specialised, sub_to_tuple_sub_super,
            sub_to_tuple_super_sub, TuplePositions,
        },
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct AsReverseExecutor {
    as_: ir::pattern::constraint::As<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    specialised_to_specialising: Arc<BTreeMap<Type, Vec<Type>>>,
    specialising: Arc<BTreeSet<Type>>,
    filter_fn: Arc<AsFilterFn>,
    checker: Checker<AdHocHkt<(Type, Type)>>,
}

pub(super) type AsReverseUnboundedSortedSpecialised =
    AsTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;
pub(super) type AsReverseBoundedSortedSpecialising =
    AsTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;

pub(super) type AsFilterFn = FilterFn<(Type, Type)>;

impl AsReverseExecutor {
    pub(crate) fn new(
        as_: AsReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let specialising = as_.specialising().clone();
        let specialised_to_specialising = as_.specialised_to_specialising().clone();
        debug_assert!(specialising.len() > 0);

        let AsReverseInstruction { as_, checks, .. } = as_;

        let iterate_mode = BinaryIterateMode::new(as_.specialised(), as_.specialising(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => {
                create_as_filter_specialised_specialising(specialised_to_specialising.clone())
            }
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_sub_filter_sub(specialising.clone())
            }
        };

        let as_specialising = as_.specialising().as_variable();
        let as_specialised = as_.specialised().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([as_specialised, as_specialising]),
            _ => TuplePositions::Pair([as_specialising, as_specialised]),
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
            specialised_to_specialising,
            specialising,
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

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let specialising_with_specialised = self
                    .specialised_to_specialising
                    .iter()
                    .flat_map(|(specialised, all_specialising)| {
                        all_specialising.iter().map(|specialising| Ok((specialising.clone(), specialised.clone())))
                    })
                    .collect_vec();
                let as_tuples: AsReverseUnboundedSortedSpecialised = NarrowingTupleIterator(
                    AsLendingIterator::new(specialising_with_specialised)
                        .try_filter::<_, AsFilterFn, (Type, Type), _>(filter_for_row)
                        .map(as_to_tuple_specialised_specialising),
                );

                Ok(TupleIterator::AsReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                let specialised =
                    type_from_row_or_annotations(self.as_.specialised(), row, self.specialised_to_specialising.keys());
                let type_manager = context.type_manager();
                let specialising = get_specialising(&**context.snapshot(), type_manager, &specialised)?;
                let specialising_with_specialised =
                    specialising.into_iter().map(|sub| Ok((sub, specialised.clone()))).collect_vec(); // TODO cache this
                let as_tuples: AsReverseBoundedSortedSpecialising = NarrowingTupleIterator(
                    AsLendingIterator::new(specialising_with_specialised)
                        .try_filter::<_, AsFilterFn, (Type, Type), _>(filter_for_row)
                        .map(as_to_tuple_specialising_specialised),
                );
                Ok(TupleIterator::AsReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

pub(super) fn get_specialising(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    specialised: &Type,
) -> Result<Vec<Type>, ConceptReadError> {
    let specialising = match specialised {
        Type::Entity(_) | Type::Relation(_) | Type::Attribute(_) => unreachable!("Only RoleType can be specialised"),
        Type::RoleType(type_) => {
            let specialising = type_.get_subtypes(snapshot, type_manager)?;
            specialising.iter().cloned().map(Type::RoleType).sorted().collect_vec()
        }
    };
    Ok(specialising)
}

fn create_as_filter_specialised_specialising(
    specialised_to_specialising: Arc<BTreeMap<Type, Vec<Type>>>,
) -> Arc<AsFilterFn> {
    Arc::new(move |result| match result {
        Ok((specialising, specialised)) => match specialised_to_specialising.get(specialised) {
            Some(all_specialising) => Ok(all_specialising.contains(specialising)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_sub(all_specialising: Arc<BTreeSet<Type>>) -> Arc<AsFilterFn> {
    Arc::new(move |result| match result {
        Ok((specialising, _)) => Ok(all_specialising.contains(specialising)),
        Err(err) => Err(err.clone()),
    })
}
