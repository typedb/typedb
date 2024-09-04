/*
 * This Source Code Form is ownsject to the terms of the Mozilla Public
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
use compiler::match_::instructions::type_::OwnsReverseInstruction;
use concept::{
    error::ConceptReadError,
    thing::thing_manager::ThingManager,
    type_::{object_type::ObjectType, owns::Owns},
};
use itertools::Itertools;
use lending_iterator::{higher_order::AdHocHkt, AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use super::owns_executor::{OwnsFilterFn, OwnsTupleIterator};
use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        owns_executor::{EXTRACT_ATTRIBUTE, EXTRACT_OWNER},
        sub_executor::NarrowingTupleIterator,
        tuple::{owns_to_tuple_attribute_owner, owns_to_tuple_owner_attribute, TuplePositions},
        BinaryIterateMode, Checker, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct OwnsReverseExecutor {
    owns: ir::pattern::constraint::Owns<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<HashSet<Type>>,
    filter_fn: Arc<OwnsFilterFn>,
    checker: Checker<AdHocHkt<Owns<'static>>>,
}

pub(super) type OwnsReverseUnboundedSortedAttribute = OwnsTupleIterator<
    AsLendingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashMap<ObjectType<'static>, Owns<'static>>>>,
            fn((ObjectType<'static>, Owns<'static>)) -> Result<Owns<'static>, ConceptReadError>,
        >,
    >,
>;
pub(super) type OwnsReverseBoundedSortedOwner = OwnsTupleIterator<
    AsLendingIterator<
        iter::Map<vec::IntoIter<Owns<'static>>, fn(Owns<'static>) -> Result<Owns<'static>, ConceptReadError>>,
    >,
>;

impl OwnsReverseExecutor {
    pub(crate) fn new(
        owns: OwnsReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let owner_types = owns.owner_types().clone();
        let attribute_owner_types = owns.attribute_owner_types().clone();
        debug_assert!(owner_types.len() > 0);

        let OwnsReverseInstruction { owns, checks, .. } = owns;

        let iterate_mode = BinaryIterateMode::new(owns.owner(), owns.attribute(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_owns_filter_owner_attribute(attribute_owner_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_owns_filter_attribute(owner_types.clone())
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
            attribute_owner_types,
            owner_types,
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
                    .attribute_owner_types
                    .keys()
                    .map(|attribute| attribute.as_attribute_type().get_owns(&**snapshot, type_manager))
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = owns.into_iter().flatten().map((|(_, owns)| Ok(owns)) as _);
                let as_tuples: OwnsReverseUnboundedSortedAttribute = NarrowingTupleIterator(
                    AsLendingIterator::new(iterator)
                        .try_filter::<_, OwnsFilterFn, AdHocHkt<Owns<'_>>, _>(filter_for_row)
                        .map(owns_to_tuple_attribute_owner),
                );
                Ok(TupleIterator::OwnsReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }

            BinaryIterateMode::UnboundInverted => {
                todo!() // is this ever relevant?
            }

            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.len() > self.owns.attribute().as_usize());
                let VariableValue::Type(Type::Attribute(attribute)) = row.get(self.owns.attribute()).to_owned() else {
                    unreachable!("Attribute in `owns` must be an attribute type")
                };

                let type_manager = thing_manager.type_manager();
                let owns = attribute.get_owns(&**snapshot, type_manager)?.values().cloned().collect_vec();

                let iterator = owns.into_iter().sorted_by_key(|owns| (owns.owner(), owns.attribute())).map(Ok as _);
                let as_tuples: OwnsReverseBoundedSortedOwner = NarrowingTupleIterator(
                    AsLendingIterator::new(iterator)
                        .try_filter::<_, OwnsFilterFn, AdHocHkt<Owns<'_>>, _>(filter_for_row)
                        .map(owns_to_tuple_owner_attribute),
                );
                Ok(TupleIterator::OwnsReverseBounded(SortedTupleIterator::new(
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
