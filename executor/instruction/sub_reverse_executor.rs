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
use compiler::{executable::match_::instructions::type_::SubReverseInstruction, ExecutorVariable};
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
        iterator::{SortedTupleIterator, TupleIterator},
        sub_executor::{NarrowingTupleIterator, SubTupleIterator, EXTRACT_SUB, EXTRACT_SUPER},
        tuple::{sub_to_tuple_sub_super, sub_to_tuple_super_sub, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, VariableModes,
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
    checker: Checker<AdHocHkt<(Type, Type)>>,
}

pub(super) type SubReverseUnboundedSortedSuper =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;
pub(super) type SubReverseBoundedSortedSub =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;

pub(super) type SubFilterFn = FilterFn<(Type, Type)>;

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
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<SubFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let sub_with_super = self
                    .super_to_subtypes
                    .iter()
                    .flat_map(|(sup, subs)| subs.iter().map(|sub| Ok((sub.clone(), sup.clone()))))
                    .collect_vec();
                let as_tuples: SubReverseUnboundedSortedSuper = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_super_sub),
                );
                Ok(TupleIterator::SubReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                let supertype = type_from_row_or_annotations(self.sub.supertype(), row, self.super_to_subtypes.keys());
                let type_manager = context.type_manager();
                let subtypes = get_subtypes(&**context.snapshot(), type_manager, &supertype, self.sub.sub_kind())?;
                let sub_with_super = subtypes.into_iter().map(|sub| Ok((sub, supertype.clone()))).collect_vec(); // TODO cache this
                let as_tuples: SubReverseBoundedSortedSub = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_sub_super),
                );
                Ok(TupleIterator::SubReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

pub(super) fn get_subtypes(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    supertype: &Type,
    sub_kind: SubKind,
) -> Result<Vec<Type>, ConceptReadError> {
    let mut subtypes = match sub_kind {
        SubKind::Exact => match supertype {
            Type::Entity(type_) => {
                let subtypes = type_.get_subtypes(snapshot, type_manager)?;
                subtypes.iter().cloned().map(Type::Entity).collect_vec()
            }
            Type::Relation(type_) => {
                let subtypes = type_.get_subtypes(snapshot, type_manager)?;
                subtypes.iter().cloned().map(Type::Relation).collect_vec()
            }
            Type::Attribute(type_) => {
                let subtypes = type_.get_subtypes(snapshot, type_manager)?;
                subtypes.iter().cloned().map(Type::Attribute).collect_vec()
            }
            Type::RoleType(type_) => {
                let subtypes = type_.get_subtypes(snapshot, type_manager)?;
                subtypes.iter().cloned().map(Type::RoleType).collect_vec()
            }
        },
        SubKind::Subtype => {
            let mut subtypes = match supertype {
                Type::Entity(type_) => {
                    let subtypes = type_.get_subtypes_transitive(snapshot, type_manager)?;
                    subtypes.iter().cloned().map(Type::Entity).collect_vec()
                }
                Type::Relation(type_) => {
                    let subtypes = type_.get_subtypes_transitive(snapshot, type_manager)?;
                    subtypes.iter().cloned().map(Type::Relation).collect_vec()
                }
                Type::Attribute(type_) => {
                    let subtypes = type_.get_subtypes_transitive(snapshot, type_manager)?;
                    subtypes.iter().cloned().map(Type::Attribute).collect_vec()
                }
                Type::RoleType(type_) => {
                    let subtypes = type_.get_subtypes_transitive(snapshot, type_manager)?;
                    subtypes.iter().cloned().map(Type::RoleType).collect_vec()
                }
            };
            subtypes.push(supertype.clone());
            subtypes
        }
    };
    subtypes.sort();
    Ok(subtypes)
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
