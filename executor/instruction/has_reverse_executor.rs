/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use answer::Type;
use compiler::match_::instructions::HasReverseInstruction;
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, has::Has, object::HasReverseIterator, thing_manager::ThingManager},
};
use itertools::{Itertools, MinMaxResult};
use lending_iterator::{
    adaptors::{Filter, Map},
    kmerge::KMergeBy,
    AsHkt, LendingIterator, Peekable,
};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    batch::ImmutableRow,
    instruction::{
        has_executor::{HasFilterFn, HasOrderingFn},
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{
            has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, HasToTupleFn, Tuple, TuplePositions,
            TupleResult,
        },
        BinaryIterateMode, VariableModes,
    },
    VariablePosition,
};

pub(crate) struct HasReverseExecutor {
    has: ir::pattern::constraint::Has<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<HashSet<Type>>,
    filter_fn: Arc<HasFilterFn>,
    attribute_cache: Option<Vec<Attribute<'static>>>,
}

pub(crate) type HasReverseUnboundedSortedAttribute =
    Map<Filter<HasReverseIterator, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasReverseUnboundedSortedOwnerMerged =
    Map<Filter<KMergeBy<HasReverseIterator, HasOrderingFn>, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasReverseUnboundedSortedOwnerSingle =
    Map<Filter<HasReverseIterator, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasReverseBoundedSortedOwner =
    Map<Filter<HasReverseIterator, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;

impl HasReverseExecutor {
    pub(crate) fn new(
        has_reverse: HasReverseInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!variable_modes.all_inputs());
        let attribute_owner_types = has_reverse.attribute_to_owner_types().clone();
        debug_assert!(!attribute_owner_types.is_empty());
        let owner_types = has_reverse.owner_types().clone();
        let has = has_reverse.has;
        let iterate_mode = BinaryIterateMode::new(has.attribute(), has.owner(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => Self::create_has_filter_attributes_owners(attribute_owner_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                Self::create_has_filter_owners(owner_types.clone())
            }
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([has.owner(), has.attribute()])
        } else {
            TuplePositions::Pair([has.attribute(), has.owner()])
        };

        let attribute_cache = if iterate_mode == BinaryIterateMode::UnboundInverted {
            let mut cache = Vec::new();
            for type_ in attribute_owner_types.keys() {
                let instances: Vec<Attribute<'static>> = thing_manager
                    .get_attributes_in(snapshot, type_.as_attribute_type())?
                    .map_static(|result| Ok(result?.clone().into_owned()))
                    .try_collect()?;
                cache.extend(instances);
            }
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            Some(cache)
        } else {
            None
        };

        Ok(Self {
            has,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            attribute_owner_types,
            owner_types,
            filter_fn,
            attribute_cache,
        })
    }

    pub(crate) fn get_iterator(
        &self,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let attribute_types_in_range = self.attribute_owner_types.keys().map(|type_| type_.as_attribute_type());
                let filter_fn = self.filter_fn.clone();
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator: Filter<HasReverseIterator, Arc<HasFilterFn>> = thing_manager
                    .get_has_from_attribute_type_range(&**snapshot, attribute_types_in_range)?
                    .filter::<_, HasFilterFn>(filter_fn);
                let as_tuples: Map<Filter<HasReverseIterator, Arc<HasFilterFn>>, HasToTupleFn, TupleResult<'_>> =
                    iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                Ok(TupleIterator::HasReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.attribute_cache.is_some());
                let (min_owner_type, max_owner_type) = Self::min_max_types(&*self.owner_types);
                let owner_type_range =
                    KeyRange::new_inclusive(min_owner_type.as_object_type(), max_owner_type.as_object_type());

                if let Some([attr]) = self.attribute_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = thing_manager
                        .get_has_reverse_by_attribute_and_owner_type_range(
                            &**snapshot,
                            attr.as_reference(),
                            owner_type_range,
                        )
                        .filter::<_, HasFilterFn>(self.filter_fn.clone());
                    let as_tuples: HasReverseUnboundedSortedOwnerSingle =
                        iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
                    let attributes = self.attribute_cache.as_ref().unwrap().iter();
                    let iterators = attributes.map(|attribute| {
                        Peekable::new(thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                            &**snapshot,
                            attribute.as_reference(),
                            owner_type_range.clone(),
                        ))
                    });

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<HasReverseIterator, HasOrderingFn> =
                        KMergeBy::new(iterators, Self::compare_has_by_owner_then_attribute);
                    let filtered: Filter<KMergeBy<HasReverseIterator, HasOrderingFn>, Arc<HasFilterFn>> =
                        merged.filter::<_, HasFilterFn>(self.filter_fn.clone());
                    let as_tuples: HasReverseUnboundedSortedOwnerMerged =
                        filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                    Ok(TupleIterator::HasReverseUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.width() > self.has.attribute().as_usize());
                let attribute = row.get(self.has.attribute());
                let (min_owner_type, max_owner_type) = Self::min_max_types(&*self.owner_types);
                let type_range =
                    KeyRange::new_inclusive(min_owner_type.as_object_type(), max_owner_type.as_object_type());
                let iterator = thing_manager.get_has_reverse_by_attribute_and_owner_type_range(
                    &**snapshot,
                    attribute.as_thing().as_attribute(),
                    type_range,
                );
                let filtered = iterator.filter::<_, HasFilterFn>(self.filter_fn.clone());
                let as_tuples: HasReverseBoundedSortedOwner =
                    filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                Ok(TupleIterator::HasReverseBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
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
                Some(owner_types) => owner_types.contains(&Type::from(has.owner().type_())),
                None => false,
            },
            Err(_) => true,
        })
    }

    fn create_has_filter_owners(owner_types: Arc<HashSet<Type>>) -> Arc<HasFilterFn> {
        Arc::new(move |result| match result {
            Ok((has, _)) => owner_types.contains(&Type::from(has.owner().type_())),
            Err(_) => true,
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
}
