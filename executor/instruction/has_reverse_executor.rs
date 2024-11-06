/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, OnceLock},
};

use answer::Type;
use compiler::{executable::match_::instructions::thing::HasReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, has::Has, object::HasReverseIterator, thing_manager::ThingManager},
};
use itertools::{Itertools, MinMaxResult};
use lending_iterator::{kmerge::KMergeBy, AsHkt, LendingIterator, Peekable};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    snapshot::ReadableSnapshot,
};

use crate::{
    instruction::{
        has_executor::{HasFilterFn, HasOrderingFn, HasTupleIterator, EXTRACT_ATTRIBUTE, EXTRACT_OWNER},
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, Tuple, TuplePositions},
        BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct HasReverseExecutor {
    has: ir::pattern::constraint::Has<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<HasFilterFn>,
    attribute_cache: OnceLock<Vec<Attribute<'static>>>,
    checker: Checker<(AsHkt![Has<'_>], u64)>,
}

pub(crate) type HasReverseUnboundedSortedAttribute = HasTupleIterator<HasReverseIterator>;
pub(crate) type HasReverseUnboundedSortedOwnerMerged = HasTupleIterator<KMergeBy<HasReverseIterator, HasOrderingFn>>;
pub(crate) type HasReverseUnboundedSortedOwnerSingle = HasTupleIterator<HasReverseIterator>;
pub(crate) type HasReverseBoundedSortedOwner = HasTupleIterator<HasReverseIterator>;

impl HasReverseExecutor {
    pub(crate) fn new(
        has_reverse: HasReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!variable_modes.all_inputs());
        let attribute_owner_types = has_reverse.attribute_to_owner_types().clone();
        debug_assert!(!attribute_owner_types.is_empty());
        let owner_types = has_reverse.owner_types().clone();
        let HasReverseInstruction { has, checks, .. } = has_reverse;
        let iterate_mode = BinaryIterateMode::new(has.attribute(), has.owner(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_has_filter_attributes_owners(attribute_owner_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_has_filter_owners(owner_types.clone())
            }
        };

        let owner = has.owner().as_variable().unwrap();
        let attribute = has.attribute().as_variable().unwrap();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([Some(attribute), Some(owner)]),
            _ => TuplePositions::Pair([Some(owner), Some(attribute)]),
        };

        let checker = Checker::<(Has<'_>, _)>::new(
            checks,
            HashMap::from([(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]),
        );

        Ok(Self {
            has,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            attribute_owner_types,
            owner_types,
            filter_fn,
            attribute_cache: OnceLock::new(),
            checker,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        if self.iterate_mode.is_inverted() && self.attribute_cache.get().is_none() {
            // TODO: this will only work if the range is actually not dependent on the Row... needs special API

            // one-off initialisation of the cache of constants
            let value_range =
                self.checker.value_range_for(context, None, self.has.attribute().as_variable().unwrap())?;
            let mut cache = Vec::new();
            for type_ in self.attribute_owner_types.keys() {
                let instances: Vec<Attribute<'static>> = context
                    .thing_manager
                    .get_attributes_in_range(context.snapshot.as_ref(), type_.as_attribute_type(), &value_range)?
                    .map_static(|result| Ok(result?.clone().into_owned()))
                    .try_collect()?;
                cache.extend(instances);
            }
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            self.attribute_cache.get_or_init(|| cache);
        }

        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<HasFilterFn> = Box::new(move |item| match filter(item) {
            Ok(true) => check(item),
            fail => fail,
        });

        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();

        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let attribute_types_in_range = self.attribute_owner_types.keys().map(|type_| type_.as_attribute_type());
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let as_tuples: HasReverseUnboundedSortedAttribute = thing_manager
                    .get_has_from_attribute_type_range(snapshot, attribute_types_in_range)?
                    .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                    .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                Ok(TupleIterator::HasReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.attribute_cache.get().is_some());
                let (min_owner_type, max_owner_type) = min_max_types(&*self.owner_types);
                let owner_type_range = KeyRange::new_variable_width(
                    RangeStart::Inclusive(min_owner_type.as_object_type()),
                    RangeEnd::EndPrefixInclusive(max_owner_type.as_object_type()),
                );

                if self.attribute_cache.get().unwrap().len() == 1 {
                    let attr = &self.attribute_cache.get().unwrap()[0];
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = thing_manager
                        .get_has_reverse_by_attribute_and_owner_type_range(
                            snapshot,
                            attr.as_reference(),
                            owner_type_range,
                        )
                        .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row);
                    let as_tuples: HasReverseUnboundedSortedOwnerSingle =
                        iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
                    let attributes = self.attribute_cache.get().unwrap().iter();
                    let iterators = attributes.map(|attribute| {
                        Peekable::new(thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                            snapshot,
                            attribute.as_reference(),
                            owner_type_range.clone(),
                        ))
                    });

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<HasReverseIterator, HasOrderingFn> =
                        KMergeBy::new(iterators, compare_has_by_owner_then_attribute);
                    let as_tuples: HasReverseUnboundedSortedOwnerMerged = merged
                        .try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row)
                        .map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                let attribute = self.has.attribute().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > attribute.as_usize());
                let attribute = row.get(attribute);
                let (min_owner_type, max_owner_type) = min_max_types(&*self.owner_types);
                let type_range = KeyRange::new_variable_width(
                    RangeStart::Inclusive(min_owner_type.as_object_type()),
                    RangeEnd::EndPrefixInclusive(max_owner_type.as_object_type()),
                );
                let iterator = thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                    snapshot,
                    attribute.as_thing().as_attribute(),
                    type_range,
                );
                let filtered = iterator.try_filter::<_, HasFilterFn, (Has<'_>, _), _>(filter_for_row);
                let as_tuples: HasReverseBoundedSortedOwner =
                    filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }
}

fn min_max_types<'a>(types: impl IntoIterator<Item = &'a Type>) -> (Type, Type) {
    match types.into_iter().minmax() {
        MinMaxResult::NoElements => unreachable!("Empty type iterator"),
        MinMaxResult::OneElement(item) => (item.clone(), item.clone()),
        MinMaxResult::MinMax(min, max) => (min.clone(), max.clone()),
    }
}

fn create_has_filter_attributes_owners(attributes_owner_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<HasFilterFn> {
    Arc::new(move |result| match result {
        Ok((has, _)) => match attributes_owner_types.get(&Type::from(has.attribute().type_())) {
            Some(owner_types) => Ok(owner_types.contains(&Type::from(has.owner().type_()))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_has_filter_owners(owner_types: Arc<BTreeSet<Type>>) -> Arc<HasFilterFn> {
    Arc::new(move |result| match result {
        Ok((has, _)) => Ok(owner_types.contains(&Type::from(has.owner().type_()))),
        Err(err) => Err(err.clone()),
    })
}

fn compare_has_by_owner_then_attribute(
    pair: (&Result<(Has<'_>, u64), ConceptReadError>, &Result<(Has<'_>, u64), ConceptReadError>),
) -> Ordering {
    if let (Ok((has_1, _)), Ok((has_2, _))) = pair {
        (has_2.owner(), has_1.attribute()).cmp(&(has_2.owner(), has_2.attribute()))
    } else {
        Ordering::Equal
    }
}
