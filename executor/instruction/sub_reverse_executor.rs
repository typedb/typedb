/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::match_::instructions::type_::SubReverseInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pattern::constraint::SubKind;
use itertools::Itertools;
use lending_iterator::{higher_order::AdHocHkt, AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        sub_executor::{NarrowingTupleIterator, SubTupleIterator, EXTRACT_SUB, EXTRACT_SUPER},
        tuple::{sub_to_tuple_sub_super, sub_to_tuple_super_sub, TuplePositions},
        BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct SubReverseExecutor {
    sub: ir::pattern::constraint::Sub<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    super_to_subtypes: Arc<BTreeMap<Type, Vec<Type>>>,
    subtypes: Arc<HashSet<Type>>,
    filter_fn: Arc<SubFilterFn>,
    checker: Checker<AdHocHkt<(Type, Type)>>,
}

pub(super) type SubReverseUnboundedSortedSub =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;
pub(super) type SubReverseBoundedSortedSuper =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;

pub(super) type SubFilterFn = FilterFn<(Type, Type)>;

impl SubReverseExecutor {
    pub(crate) fn new(
        sub: SubReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let subtypes = sub.subtypes().clone();
        let super_to_subtypes = sub.super_to_subtypes().clone();
        debug_assert!(subtypes.len() > 0);

        let SubReverseInstruction { sub, checks, .. } = sub;

        let iterate_mode = BinaryIterateMode::new(sub.subtype(), sub.supertype(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_sub_filter_super_sub(super_to_subtypes.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_sub_filter_sub(subtypes.clone())
            }
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([sub.subtype(), sub.supertype()])
        } else {
            TuplePositions::Pair([sub.supertype(), sub.subtype()])
        };

        let checker = Checker::<AdHocHkt<(Type, Type)>> {
            checks,
            extractors: HashMap::from([(sub.subtype(), EXTRACT_SUB), (sub.supertype(), EXTRACT_SUPER)]),
            _phantom_data: PhantomData,
        };

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
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        debug_assert_eq!(self.sub.sub_kind(), SubKind::Subtype);

        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(snapshot, thing_manager, &row);
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
                let as_tuples: SubReverseUnboundedSortedSub = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_sub_super),
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
                debug_assert!(row.len() > self.sub.supertype().as_usize());
                let VariableValue::Type(sup) = row.get(self.sub.supertype()).to_owned() else {
                    unreachable!("Subtype must be a type")
                };

                let type_manager = thing_manager.type_manager();
                let subtypes = match sup.clone() {
                    Type::Entity(type_) => {
                        let mut subtypes =
                            type_manager.get_entity_type_subtypes(&**snapshot, type_.clone())?.to_owned();
                        subtypes.push(type_);
                        subtypes.sort();
                        subtypes.into_iter().map(Type::Entity).collect_vec()
                    }
                    Type::Relation(type_) => {
                        let mut subtypes =
                            type_manager.get_relation_type_subtypes(&**snapshot, type_.clone())?.to_owned();
                        subtypes.push(type_);
                        subtypes.sort();
                        subtypes.into_iter().map(Type::Relation).collect_vec()
                    }
                    Type::Attribute(type_) => {
                        let mut subtypes =
                            type_manager.get_attribute_type_subtypes(&**snapshot, type_.clone())?.to_owned();
                        subtypes.push(type_);
                        subtypes.sort();
                        subtypes.into_iter().map(Type::Attribute).collect_vec()
                    }
                    Type::RoleType(type_) => {
                        let mut subtypes = type_manager.get_role_type_subtypes(&**snapshot, type_.clone())?.to_owned();
                        subtypes.push(type_);
                        subtypes.sort();
                        subtypes.into_iter().map(Type::RoleType).collect_vec()
                    }
                };

                let sub_with_super = subtypes.into_iter().map(|sub| Ok((sub, sup.clone()))).collect_vec(); // TODO cache this
                let as_tuples: SubReverseBoundedSortedSuper = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_super_sub),
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

fn create_sub_filter_super_sub(super_to_subtypes: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, sup)) => match super_to_subtypes.get(sup) {
            Some(subtypes) => Ok(subtypes.contains(sub)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_sub(subtypes: Arc<HashSet<Type>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, _)) => Ok(subtypes.contains(sub)),
        Err(err) => Err(err.clone()),
    })
}
