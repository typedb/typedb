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
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, has::Has, object::HasReverseIterator, thing_manager::ThingManager},
};
use lending_iterator::{
    adaptors::{Filter, Map},
    kmerge::KMergeBy,
    AsHkt, LendingIterator, Peekable,
};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::executor::{
    batch::ImmutableRow,
    instruction::{
        has_executor::{HasFilterFn, HasOrderingFn},
        iterator::{inverted_instances_cache, SortedTupleIterator, TupleIterator},
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
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: ir::pattern::constraint::Has<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
        owner_types: Arc<HashSet<Type>>,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(attribute_owner_types.len() > 0);
        debug_assert!(!variable_modes.fully_bound());
        let iterate_mode = BinaryIterateMode::new(has.clone(), true, &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => {
                Self::create_has_filter_attributes_owners(attribute_owner_types.clone(), owner_types.clone())
            }
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                Self::create_has_filter_owners(owner_types.clone())
            }
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([has.owner(), has.attribute()])
        } else {
            TuplePositions::Pair([has.attribute(), has.owner()])
        };

        let attribute_cache = if matches!(iterate_mode, BinaryIterateMode::UnboundInverted) {
            Some(inverted_instances_cache(
                attribute_owner_types.keys().map(|type_| type_.as_attribute_type()),
                snapshot,
                thing_manager,
            )?)
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

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let attribute_types_in_range = self.attribute_owner_types.keys().map(|type_| type_.as_attribute_type());
                let filter_fn = self.filter_fn.clone();
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator: Filter<HasReverseIterator, Arc<HasFilterFn>> = thing_manager
                    .get_has_from_attribute_type_range(snapshot, attribute_types_in_range)?
                    .filter::<_, HasFilterFn>(filter_fn);
                let as_tuples: Map<Filter<HasReverseIterator, Arc<HasFilterFn>>, HasToTupleFn, TupleResult> =
                    iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                Ok(TupleIterator::HasReverseUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.attribute_cache.is_some());
                if let Some([attr]) = self.attribute_cache.as_deref() {
                    let (min_owner_type, max_owner_type) = Self::min_max_types(self.owner_types.iter());
                    let type_range =
                        KeyRange::new_inclusive(min_owner_type.as_object_type(), max_owner_type.as_object_type());
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = attr
                        .get_owners_by_type_range(snapshot, thing_manager, type_range)
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
                    let mut iterators: Vec<Peekable<HasReverseIterator>> =
                        Vec::with_capacity(self.attribute_cache.as_ref().unwrap().len());
                    let (min_owner_type, max_owner_type) = Self::min_max_types(self.owner_types.iter());
                    let type_range =
                        KeyRange::new_inclusive(min_owner_type.as_object_type(), max_owner_type.as_object_type());
                    for iter in self.attribute_cache.as_ref().unwrap().iter().map(|attribute| {
                        attribute.get_owners_by_type_range(snapshot, thing_manager, type_range.clone())
                    }) {
                        iterators.push(Peekable::new(iter))
                    }

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
                let (min_owner_type, max_owner_type) = Self::min_max_types(self.owner_types.iter());
                let type_range =
                    KeyRange::new_inclusive(min_owner_type.as_object_type(), max_owner_type.as_object_type());
                let iterator =
                    attribute.as_thing().as_attribute().get_owners_by_type_range(snapshot, thing_manager, type_range);
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

    fn min_max_types<'a>(types: impl Iterator<Item = &'a Type>) -> (Type, Type) {
        let mut min = None;
        let mut max = None;
        for type_ in types {
            if min.is_none() || min.as_ref().is_some_and(|min_type| min_type > type_) {
                min = Some(type_.clone())
            };
            if max.is_none() || max.as_ref().is_some_and(|max_type| max_type < type_) {
                max = Some(type_.clone())
            };
        }
        debug_assert!(min.is_some() && max.is_some());
        (min.unwrap(), max.unwrap())
    }

    fn create_has_filter_attributes_owners(
        attributes_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
        owner_types: Arc<HashSet<Type>>,
    ) -> Arc<HasFilterFn> {
        Arc::new({
            move |result: &Result<(Has<'_>, u64), ConceptReadError>| match result {
                Ok((has, _)) => {
                    attributes_owner_types.contains_key(&Type::from(has.attribute().type_()))
                        && owner_types.contains(&Type::from(has.owner().type_()))
                }
                Err(_) => true,
            }
        }) as Arc<HasFilterFn>
    }

    fn create_has_filter_owners(owner_types: Arc<HashSet<Type>>) -> Arc<HasFilterFn> {
        Arc::new({
            move |result: &Result<(Has<'_>, u64), ConceptReadError>| match result {
                Ok((has, _)) => owner_types.contains(&Type::from(has.owner().type_())),
                Err(_) => true,
            }
        }) as Arc<HasFilterFn>
    }

    fn compare_has_by_owner_then_attribute<'a, 'b>(
        pair: (&'a Result<(Has<'a>, u64), ConceptReadError>, &'b Result<(Has<'b>, u64), ConceptReadError>),
    ) -> Ordering {
        let (result_1, result_2) = pair;
        match (result_1, result_2) {
            (Ok((has_1, _)), Ok((has_2, _))) => {
                has_1.owner().cmp(&has_2.owner()).then(has_1.attribute().cmp(&has_2.attribute()))
            }
            _ => Ordering::Equal,
        }
    }
}
