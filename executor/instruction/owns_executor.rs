/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter,
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::match_::instructions::type_::OwnsInstruction;
use concept::{
    error::ConceptReadError,
    thing::thing_manager::ThingManager,
    type_::{owns::Owns, OwnerAPI},
};
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
        tuple::{owns_to_tuple_owner_attribute, OwnsToTupleFn, TuplePositions, TupleResult},
        BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct OwnsExecutor {
    owns: ir::pattern::constraint::Owns<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<HashSet<Type>>,
    filter_fn: Arc<OwnsFilterFn>,
    checker: Checker<AdHocHkt<Owns<'static>>>,
}

pub(super) type OwnsTupleIterator<I> = NarrowingTupleIterator<
    Map<
        TryFilter<I, Box<OwnsFilterFn>, AdHocHkt<Owns<'static>>, ConceptReadError>,
        OwnsToTupleFn,
        AdHocHkt<TupleResult<'static>>,
    >,
>;

pub(super) type OwnsUnboundedSortedOwner = OwnsTupleIterator<
    AsLendingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<Owns<'static>>>>,
            fn(Owns<'static>) -> Result<Owns<'static>, ConceptReadError>,
        >,
    >,
>;
pub(super) type OwnsBoundedSortedAttribute = OwnsTupleIterator<
    AsLendingIterator<
        iter::Map<vec::IntoIter<Owns<'static>>, fn(Owns<'static>) -> Result<Owns<'static>, ConceptReadError>>,
    >,
>;

pub(super) type OwnsFilterFn = FilterFn<AdHocHkt<Owns<'static>>>;

type OwnsVariableValueExtractor = fn(&Owns<'static>) -> VariableValue<'static>;
pub(super) const EXTRACT_OWNER: OwnsVariableValueExtractor =
    |owns| VariableValue::Type(Type::from(owns.owner().into_owned()));
pub(super) const EXTRACT_ATTRIBUTE: OwnsVariableValueExtractor =
    |owns| VariableValue::Type(Type::Attribute(owns.attribute().into_owned()));

impl OwnsExecutor {
    pub(crate) fn new(
        owns: OwnsInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let attribute_types = owns.attribute_types().clone();
        let owner_attribute_types = owns.owner_attribute_types().clone();
        debug_assert!(attribute_types.len() > 0);

        let OwnsInstruction { owns, checks, .. } = owns;

        let iterate_mode = BinaryIterateMode::new(owns.owner(), owns.attribute(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_owns_filter_owner_attribute(owner_attribute_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_owns_filter_attribute(attribute_types.clone())
            }
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([owns.attribute(), owns.owner()])
        } else {
            TuplePositions::Pair([owns.owner(), owns.attribute()])
        };

        let checker = Checker::<AdHocHkt<Owns<'static>>> {
            checks,
            extractors: HashMap::from([(owns.owner(), EXTRACT_OWNER), (owns.attribute(), EXTRACT_ATTRIBUTE)]),
            _phantom_data: PhantomData,
        };

        Self {
            owns,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            owner_attribute_types,
            attribute_types,
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
        let filter_for_row: Box<OwnsFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = thing_manager.type_manager();
                let owns: Vec<_> = self
                    .owner_attribute_types
                    .keys()
                    .map(|owner| match owner {
                        Type::Entity(entity) => entity.get_owns(&**snapshot, type_manager),
                        Type::Relation(relation) => relation.get_owns(&**snapshot, type_manager),
                        _ => unreachable!("owner types must be relation or entity types"),
                    })
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = owns.into_iter().flatten().map(Ok as _);
                let as_tuples: OwnsUnboundedSortedOwner = NarrowingTupleIterator(
                    AsLendingIterator::new(iterator)
                        .try_filter::<_, OwnsFilterFn, AdHocHkt<Owns<'_>>, _>(filter_for_row)
                        .map(owns_to_tuple_owner_attribute),
                );
                Ok(TupleIterator::OwnsUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.len() > self.owns.owner().as_usize());
                let VariableValue::Type(owner) = row.get(self.owns.owner()).to_owned() else {
                    unreachable!("Owner in `owns` must be a type")
                };

                let type_manager = thing_manager.type_manager();
                let owns = match owner {
                    Type::Entity(entity) => entity.get_owns(&**snapshot, type_manager)?,
                    Type::Relation(relation) => relation.get_owns(&**snapshot, type_manager)?,
                    _ => unreachable!("owner types must be relation or entity types"),
                };

                let iterator =
                    owns.to_owned().into_iter().sorted_by_key(|owns| (owns.attribute(), owns.owner())).map(Ok as _);
                let as_tuples: OwnsBoundedSortedAttribute = NarrowingTupleIterator(
                    AsLendingIterator::new(iterator)
                        .try_filter::<_, OwnsFilterFn, AdHocHkt<Owns<'_>>, _>(filter_for_row)
                        .map(owns_to_tuple_owner_attribute),
                );
                Ok(TupleIterator::OwnsBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_owns_filter_owner_attribute(owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok(owns) => match owner_attribute_types.get(&Type::from(owns.owner())) {
            Some(attribute_types) => Ok(attribute_types.contains(&Type::Attribute(owns.attribute()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_owns_filter_attribute(attribute_types: Arc<HashSet<Type>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok(owns) => Ok(attribute_types.contains(&Type::Attribute(owns.attribute()))),
        Err(err) => Err(err.clone()),
    })
}
