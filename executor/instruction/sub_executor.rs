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
use compiler::match_::instructions::type_::SubInstruction;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::TypeAPI};
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
        tuple::{sub_to_tuple_sub_super, SubToTupleFn, TuplePositions, TupleResult},
        BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct SubExecutor {
    sub: ir::pattern::constraint::Sub<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>,
    supertypes: Arc<HashSet<Type>>,
    filter_fn: Arc<SubFilterFn>,
    checker: Checker<AdHocHkt<(Type, Type)>>,
}

pub(super) type SubTupleIterator<I> = NarrowingTupleIterator<
    Map<TryFilter<I, Box<SubFilterFn>, (Type, Type), ConceptReadError>, SubToTupleFn, AdHocHkt<TupleResult<'static>>>,
>;

pub(super) type SubUnboundedSortedSub =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;
pub(super) type SubBoundedSortedSuper =
    SubTupleIterator<AsLendingIterator<vec::IntoIter<Result<(Type, Type), ConceptReadError>>>>;

pub(super) type SubFilterFn = FilterFn<(Type, Type)>;

type SubVariableValueExtractor = fn(&(Type, Type)) -> VariableValue<'static>;
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
        sub: SubInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
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
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([sub.supertype(), sub.subtype()])
        } else {
            TuplePositions::Pair([sub.subtype(), sub.supertype()])
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
            sub_to_supertypes,
            supertypes,
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
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(snapshot, thing_manager, &row);
        let filter_for_row: Box<SubFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        match (self.sub.sub_kind(), self.iterate_mode) {
            (SubKind::Subtype, BinaryIterateMode::Unbound) => {
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

            (SubKind::Exact, BinaryIterateMode::Unbound) => {
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

            (_, BinaryIterateMode::UnboundInverted) => {
                todo!() // is this ever relevant?
            }

            (SubKind::Subtype, BinaryIterateMode::BoundFrom) => {
                debug_assert!(row.len() > self.sub.subtype().as_usize());
                let VariableValue::Type(sub) = row.get(self.sub.subtype()).to_owned() else {
                    unreachable!("Subtype must be a type")
                };

                let type_manager = thing_manager.type_manager();
                let mut supertypes = match &sub {
                    Type::Entity(type_) => {
                        let supertypes = type_.get_supertypes_transitive(&**snapshot, type_manager)?;
                        supertypes.iter().cloned().map(Type::Entity).collect_vec()
                    }
                    Type::Relation(type_) => {
                        let supertypes = type_.get_supertypes_transitive(&**snapshot, type_manager)?;
                        supertypes.iter().cloned().map(Type::Relation).collect_vec()
                    }
                    Type::Attribute(type_) => {
                        let supertypes = type_.get_supertypes_transitive(&**snapshot, type_manager)?;
                        supertypes.iter().cloned().map(Type::Attribute).collect_vec()
                    }
                    Type::RoleType(type_) => {
                        let supertypes = type_.get_supertypes_transitive(&**snapshot, type_manager)?;
                        supertypes.iter().cloned().map(Type::RoleType).collect_vec()
                    }
                };
                supertypes.push(sub.clone());
                supertypes.sort();

                let sub_with_super = supertypes.into_iter().map(|sup| Ok((sub.clone(), sup))).collect_vec(); // TODO cache this

                let as_tuples: SubBoundedSortedSuper = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_sub_super),
                );
                Ok(TupleIterator::SubBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            (SubKind::Exact, BinaryIterateMode::BoundFrom) => {
                debug_assert!(row.len() > self.sub.subtype().as_usize());
                let VariableValue::Type(sub) = row.get(self.sub.subtype()).to_owned() else {
                    unreachable!("Subtype must be a type")
                };

                let type_manager = thing_manager.type_manager();
                let sup = match &sub {
                    Type::Entity(type_) => type_.get_supertype(&**snapshot, type_manager)?.map(Type::Entity),
                    Type::Relation(type_) => type_.get_supertype(&**snapshot, type_manager)?.map(Type::Relation),
                    Type::Attribute(type_) => type_.get_supertype(&**snapshot, type_manager)?.map(Type::Attribute),
                    Type::RoleType(type_) => type_.get_supertype(&**snapshot, type_manager)?.map(Type::RoleType),
                };

                let sub_with_super = sup.map(|sup| Ok((sub, sup))).as_slice().to_vec(); // TODO cache this

                let as_tuples: SubBoundedSortedSuper = NarrowingTupleIterator(
                    AsLendingIterator::new(sub_with_super)
                        .try_filter::<_, SubFilterFn, (Type, Type), _>(filter_for_row)
                        .map(sub_to_tuple_sub_super),
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

fn create_sub_filter_sub_super(sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, sup)) => match sub_to_supertypes.get(sub) {
            Some(supertypes) => Ok(supertypes.contains(sup)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_super(supertypes: Arc<HashSet<Type>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, sup)) => Ok(supertypes.contains(sup)),
        Err(err) => Err(err.clone()),
    })
}
