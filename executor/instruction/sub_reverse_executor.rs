/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    vec,
};

use answer::Type;
use compiler::{executable::match_::instructions::type_::SubReverseInstruction, ExecutorVariable};
use concept::error::ConceptReadError;
use itertools::Itertools;
use lending_iterator::AsLendingIterator;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{NaiiveSeekable, SortedTupleIterator, TupleIterator},
        sub_executor::{SubFilterFn, SubFilterMapFn, SubTupleIterator, EXTRACT_SUB, EXTRACT_SUPER},
        tuple::{sub_to_tuple_sub_super, sub_to_tuple_super_sub, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct SubReverseExecutor {
    sub: ir::pattern::constraint::Sub<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    super_to_subtypes: Arc<BTreeMap<Type, Vec<Type>>>,
    subtypes: Arc<BTreeSet<Type>>,
    filter_fn: Arc<SubFilterFn>,
    checker: Checker<(Type, Type)>,
}

impl fmt::Debug for SubReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SubReverseExecutor")
    }
}

pub(super) type SubReverseUnboundedSortedSuper =
    SubTupleIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>;
pub(super) type SubReverseBoundedSortedSub =
    SubTupleIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>;

impl SubReverseExecutor {
    pub(crate) fn new(
        sub: SubReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let subtypes = sub.subtypes().clone();
        let super_to_subtypes = sub.super_to_subtypes().clone();
        debug_assert!(subtypes.len() > 0);

        let SubReverseInstruction { sub, checks, .. } = sub;

        let iterate_mode = BinaryIterateMode::new(sub.supertype(), sub.subtype(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_sub_filter_super_sub(super_to_subtypes.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_sub_filter_sub(subtypes.clone())
            }
        };

        let subtype = sub.subtype().as_variable();
        let supertype = sub.supertype().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([supertype, subtype]),
            _ => TuplePositions::Pair([subtype, supertype]),
        };

        let checker = Checker::<(Type, Type)>::new(
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
            super_to_subtypes,
            subtypes,
            filter_fn,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row, storage_counters);
        let filter_for_row: Box<SubFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let sub_with_super = self
                    .super_to_subtypes
                    .iter()
                    .flat_map(|(sup, subs)| subs.iter().map(|sub| Ok((*sub, *sup))))
                    .collect_vec();
                let as_tuples = sub_with_super.into_iter().filter_map(filter_for_row).map(sub_to_tuple_super_sub as _);
                let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
                Ok(TupleIterator::SubReverseUnbounded(SortedTupleIterator::new(
                    lending_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                // is this ever relevant?
                return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
                    functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
                }));
            }

            BinaryIterateMode::BoundFrom => {
                let supertype = type_from_row_or_annotations(self.sub.supertype(), row, self.super_to_subtypes.keys());
                let subtypes = self.super_to_subtypes.get(&supertype).unwrap_or(const { &Vec::new() });
                let sub_with_super = subtypes.iter().map(|sub| Ok((*sub, supertype))).collect_vec(); // TODO cache this
                let as_tuples = sub_with_super.into_iter().filter_map(filter_for_row).map(sub_to_tuple_sub_super as _);
                let lending_tuples = NaiiveSeekable::new(AsLendingIterator::new(as_tuples));
                Ok(TupleIterator::SubReverseBounded(SortedTupleIterator::new(
                    lending_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

impl fmt::Display for SubReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.sub, &self.iterate_mode)
    }
}

fn create_sub_filter_super_sub(super_to_subtypes: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, sup)) => match super_to_subtypes.get(sup) {
            Some(subtypes) => Ok(subtypes.contains(sub)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_sub(subtypes: Arc<BTreeSet<Type>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, _)) => Ok(subtypes.contains(sub)),
        Err(err) => Err(err.clone()),
    })
}
