/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    iter,
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::match_::instructions::type_::OwnsReverseInstruction;
use concept::{error::ConceptReadError, type_::owns::Owns};
use itertools::Itertools;
use lending_iterator::{AsHkt, AsNarrowingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use super::type_from_row_or_annotations;
use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        owns_executor::{OwnsFilterFn, OwnsTupleIterator, EXTRACT_ATTRIBUTE, EXTRACT_OWNER},
        tuple::{owns_to_tuple_attribute_owner, TuplePositions},
        BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct OwnsReverseExecutor {
    owns: ir::pattern::constraint::Owns<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<OwnsFilterFn>,
    checker: Checker<AsHkt![Owns<'_>]>,
}

pub(super) type OwnsReverseUnboundedSortedAttribute = OwnsTupleIterator<
    AsNarrowingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<Owns<'static>>>>,
            fn(Owns<'static>) -> Result<Owns<'static>, ConceptReadError>,
        >,
        Result<AsHkt![Owns<'_>], ConceptReadError>,
    >,
>;
pub(super) type OwnsReverseBoundedSortedOwner = OwnsTupleIterator<
    AsNarrowingIterator<
        iter::Map<vec::IntoIter<Owns<'static>>, fn(Owns<'static>) -> Result<Owns<'static>, ConceptReadError>>,
        Result<AsHkt![Owns<'_>], ConceptReadError>,
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

        let iterate_mode = BinaryIterateMode::new(owns.attribute(), owns.owner(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_owns_filter_owner_attribute(attribute_owner_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_owns_filter_attribute(owner_types.clone())
            }
        };

        let owner = owns.owner().as_variable();
        let attribute = owns.attribute().as_variable();

        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([owner, attribute])
        } else {
            TuplePositions::Pair([attribute, owner])
        };

        let checker = Checker::<AsHkt![Owns<'_>]> {
            checks,
            extractors: [(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
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
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<OwnsFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let type_manager = context.type_manager();
                let owns: Vec<_> = self
                    .attribute_owner_types
                    .keys()
                    .map(|attribute| attribute.as_attribute_type().get_owns(snapshot, type_manager))
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = owns.into_iter().flatten().map(Ok as _);
                let as_tuples: OwnsReverseUnboundedSortedAttribute =
                    AsNarrowingIterator::<_, Result<Owns<'_>, _>>::new(iterator)
                        .try_filter::<_, OwnsFilterFn, Owns<'_>, _>(filter_for_row)
                        .map(owns_to_tuple_attribute_owner);
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
                let attribute =
                    type_from_row_or_annotations(self.owns.attribute(), row, self.attribute_owner_types.keys());
                let Type::Attribute(attribute) = attribute else {
                    unreachable!("Attribute in `owns` must be an attribute type")
                };

                let type_manager = context.type_manager();
                let owns = attribute.get_owns(snapshot, type_manager)?.to_owned();

                let iterator = owns.into_iter().sorted_by_key(|owns| owns.owner()).map(Ok as _);
                let as_tuples: OwnsReverseBoundedSortedOwner =
                    AsNarrowingIterator::<_, Result<Owns<'_>, _>>::new(iterator)
                        .try_filter::<_, OwnsFilterFn, AsHkt![Owns<'_>], _>(filter_for_row)
                        .map(owns_to_tuple_attribute_owner);

                Ok(TupleIterator::OwnsReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn create_owns_filter_owner_attribute(attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok(owns) => match attribute_owner_types.get(&Type::Attribute(owns.attribute())) {
            Some(owner_types) => Ok(owner_types.contains(&Type::from(owns.owner().into_owned()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_owns_filter_attribute(owner_types: Arc<BTreeSet<Type>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok(owns) => Ok(owner_types.contains(&Type::from(owns.owner().into_owned()))),
        Err(err) => Err(err.clone()),
    })
}
