/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    iter,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::OwnsInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{owns::Owns, OwnerAPI},
};
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Map, TryFilter},
    AsHkt, AsNarrowingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{
            owns_to_tuple_attribute_owner, owns_to_tuple_owner_attribute, OwnsToTupleFn, TuplePositions, TupleResult,
        },
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct OwnsExecutor {
    owns: ir::pattern::constraint::Owns<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<OwnsFilterFn>,
    checker: Checker<AsHkt![Owns<'_>]>,
}

pub(super) type OwnsTupleIterator<I> =
    Map<TryFilter<I, Box<OwnsFilterFn>, AsHkt![Owns<'_>], ConceptReadError>, OwnsToTupleFn, AsHkt![TupleResult<'_>]>;

pub(super) type OwnsUnboundedSortedOwner = OwnsTupleIterator<
    AsNarrowingIterator<
        iter::Map<
            iter::Flatten<vec::IntoIter<HashSet<Owns<'static>>>>,
            fn(Owns<'static>) -> Result<Owns<'static>, ConceptReadError>,
        >,
        Result<AsHkt![Owns<'_>], ConceptReadError>,
    >,
>;
pub(super) type OwnsBoundedSortedAttribute = OwnsTupleIterator<
    AsNarrowingIterator<
        iter::Map<vec::IntoIter<Owns<'static>>, fn(Owns<'static>) -> Result<Owns<'static>, ConceptReadError>>,
        Result<AsHkt![Owns<'_>], ConceptReadError>,
    >,
>;

pub(super) type OwnsFilterFn = FilterFn<AsHkt![Owns<'_>]>;

type OwnsVariableValueExtractor = for<'a> fn(&'a Owns<'_>) -> VariableValue<'a>;
pub(super) const EXTRACT_OWNER: OwnsVariableValueExtractor =
    |owns| VariableValue::Type(Type::from(owns.owner().into_owned()));
pub(super) const EXTRACT_ATTRIBUTE: OwnsVariableValueExtractor =
    |owns| VariableValue::Type(Type::Attribute(owns.attribute()));

impl OwnsExecutor {
    pub(crate) fn new(
        owns: OwnsInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
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

        let owner = owns.owner().as_variable();
        let attribute = owns.attribute().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([owner, attribute]),
            _ => TuplePositions::Pair([attribute, owner]),
        };

        let checker = Checker::<AsHkt![Owns<'_>]>::new(
            checks,
            [(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

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
                    .owner_attribute_types
                    .keys()
                    .map(|owner| match owner {
                        Type::Entity(entity) => entity.get_owns(snapshot, type_manager),
                        Type::Relation(relation) => relation.get_owns(snapshot, type_manager),
                        _ => unreachable!("owner types must be relation or entity types"),
                    })
                    .map_ok(|set| set.to_owned())
                    .try_collect()?;
                let iterator = owns.into_iter().flatten().map(Ok as _);
                let as_tuples: OwnsUnboundedSortedOwner = AsNarrowingIterator::<_, Result<Owns<'_>, _>>::new(iterator)
                    .try_filter::<_, OwnsFilterFn, Owns<'_>, _>(filter_for_row)
                    .map(owns_to_tuple_owner_attribute);
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
                let owner = type_from_row_or_annotations(self.owns.owner(), row, self.owner_attribute_types.keys());
                let type_manager = context.type_manager();
                let owns = match owner {
                    Type::Entity(entity) => entity.get_owns(snapshot, type_manager)?,
                    Type::Relation(relation) => relation.get_owns(snapshot, type_manager)?,
                    _ => unreachable!("owner types must be relation or entity types"),
                };

                let iterator = owns.iter().cloned().sorted_by_key(|owns| (owns.attribute(), owns.owner())).map(Ok as _);
                let as_tuples: OwnsBoundedSortedAttribute =
                    AsNarrowingIterator::<_, Result<Owns<'_>, _>>::new(iterator)
                        .try_filter::<_, OwnsFilterFn, Owns<'_>, _>(filter_for_row)
                        .map(owns_to_tuple_attribute_owner);
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
        Ok(owns) => match owner_attribute_types.get(&Type::from(owns.owner().into_owned())) {
            Some(attribute_types) => Ok(attribute_types.contains(&Type::Attribute(owns.attribute()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_owns_filter_attribute(attribute_types: Arc<BTreeSet<Type>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok(owns) => Ok(attribute_types.contains(&Type::Attribute(owns.attribute()))),
        Err(err) => Err(err.clone()),
    })
}
