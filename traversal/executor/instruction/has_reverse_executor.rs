/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use answer::Type;
use concept::error::ConceptReadError;
use concept::thing::attribute::Attribute;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use crate::executor::batch::ImmutableRow;
use crate::executor::instruction::{BinaryIterateMode, VariableModes};
use crate::executor::instruction::has_executor::HasFilterFn;
use crate::executor::instruction::iterator::TupleIterator;
use crate::executor::instruction::tuple::TuplePositions;
use crate::executor::VariablePosition;

pub(crate) struct HasReverseIteratorExecutor {
    has: ir::pattern::constraint::Has<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<HashSet<Type>>,
    filter_fn: Arc<HasFilterFn>,
    attribute_cache: Option<Vec<Attribute<'static>>>,
}

impl HasReverseIteratorExecutor {
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
            BinaryIterateMode::Unbound => Arc::new({
                let att_owner_types = attribute_owner_types.clone();
                let owner_types = owner_types.clone();
                move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
                    Ok((has, _)) => {
                        att_owner_types.contains_key(&Type::Attribute(has.attribute().type_()))
                            && owner_types.contains(&Type::from(has.owner().type_()))
                    }
                    Err(_) => true,
                }
            }) as Arc<HasFilterFn>,
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => Arc::new({
                let owner_types = owner_types.clone();
                move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
                    Ok((has, _)) => owner_types.contains(&Type::from(has.owner().type_())),
                    Err(_) => true,
                }
            }) as Arc<HasFilterFn>,
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([has.owner(), has.attribute()])
        } else {
            TuplePositions::Pair([has.attribute(), has.owner()])
        };

        let owner_cache = if matches!(iterate_mode, BinaryIterateMode::UnboundInverted) {
            let mut cache = Vec::new();
            for attr_type in attribute_owner_types.keys() {
                for attr in thing_manager
                    .get_attributes_in(snapshot, attr_type.as_attribute_type())?
                    .map_static(|result| result.map(|attr| attr.clone().into_owned()))
                    .into_iter() {
                    cache.push(attr?);
                }
            }
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            Some(cache)
        } else {
            None
        };
        todo!()
        //
        // Ok(Self {
        //     has,
        //     iterate_mode,
        //     variable_modes,
        //     tuple_positions: output_tuple_positions,
        //     owner_attribute_types,
        //     attribute_types,
        //     filter_fn,
        //     owner_cache,
        // })
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        // match self.iterate_mode {
        // BinaryIterateMode::Unbound => {
        //     let first_from_type = self.owner_attribute_types.first_key_value().unwrap().0;
        //     let last_key_from_type = self.owner_attribute_types.last_key_value().unwrap().0;
        //     let key_range =
        //         KeyRange::new_inclusive(first_from_type.as_object_type(), last_key_from_type.as_object_type());
        //     let filter_fn = self.filter_fn.has_both_filter();
        //     // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
        //     let iterator: Filter<HasIterator, Arc<crate::executor::instruction::has_executor::HasFilterBothFn>> = thing_manager
        //         .get_has_from_type_range_unordered(snapshot, key_range)
        //         .filter::<_, crate::executor::instruction::has_executor::HasFilterBothFn>(filter_fn);
        //     let as_tuples: Map<Filter<HasIterator, Arc<crate::executor::instruction::has_executor::HasFilterBothFn>>, crate::executor::instruction::has_executor::HasToTupleFn, TupleResult> =
        //         iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
        //     Ok(TupleIterator::HasUnbounded(SortedTupleIterator::new(
        //         as_tuples,
        //         self.tuple_positions.clone(),
        //         &self.variable_modes,
        //     )))
        // }
        // BinaryIterateMode::UnboundInverted => {
        //     debug_assert!(self.owner_cache.is_some());
        //     if let Some([iter]) = self.owner_cache.as_deref() {
        //         // no heap allocs needed if there is only 1 iterator
        //         let iterator = iter
        //             .get_has_types_range_unordered(
        //                 snapshot,
        //                 thing_manager,
        //                 // TODO: this should be just the types owned by the one instance's type in the cache!
        //                 self.attribute_types.iter().map(|t| t.as_attribute_type()),
        //             )?
        //             .filter::<_, crate::executor::instruction::has_executor::HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
        //         let as_tuples: HasUnboundedSortedAttributeSingle =
        //             iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
        //         Ok(TupleIterator::HasUnboundedInvertedSingle(SortedTupleIterator::new(
        //             as_tuples,
        //             self.tuple_positions.clone(),
        //             &self.variable_modes,
        //         )))
        //     } else {
        //         // // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
        //         let mut iterators: Vec<Peekable<HasIterator>> =
        //             Vec::with_capacity(self.owner_cache.as_ref().unwrap().len());
        //         for iter in self.owner_cache.as_ref().unwrap().iter().map(|object| {
        //             object.get_has_types_range_unordered(
        //                 snapshot,
        //                 thing_manager,
        //                 self.attribute_types.iter().map(|t| t.as_attribute_type()),
        //             )
        //         }) {
        //             iterators.push(Peekable::new(iter?))
        //         }
        //
        //         // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
        //         let merged: KMergeBy<HasIterator, crate::executor::instruction::has_executor::HasOrderByAttributeFn> =
        //             KMergeBy::new(iterators, Self::compare_has_by_attribute_then_owner);
        //         let filtered: Filter<KMergeBy<HasIterator, crate::executor::instruction::has_executor::HasOrderByAttributeFn>, Arc<crate::executor::instruction::has_executor::HasFilterAttributeFn>> =
        //             merged.filter::<_, crate::executor::instruction::has_executor::HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
        //         let as_tuples: HasUnboundedSortedAttributeMerged =
        //             filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
        //         Ok(TupleIterator::HasUnboundedInvertedMerged(SortedTupleIterator::new(
        //             as_tuples,
        //             self.tuple_positions.clone(),
        //             &self.variable_modes,
        //         )))
        //     }
        // }
        // BinaryIterateMode::BoundFrom => {
        //     debug_assert!(row.width() > self.has.owner().as_usize());
        //     let owner = row.get(self.has.owner());
        //     let iterator = match owner {
        //         VariableValue::Thing(Thing::Entity(entity)) => entity.get_has_types_range_unordered(
        //             snapshot,
        //             thing_manager,
        //             self.attribute_types.iter().map(|t| t.as_attribute_type()),
        //         )?,
        //         VariableValue::Thing(Thing::Relation(relation)) => relation.get_has_types_range_unordered(
        //             snapshot,
        //             thing_manager,
        //             self.attribute_types.iter().map(|t| t.as_attribute_type()),
        //         )?,
        //         _ => unreachable!("Has owner must be an entity or relation."),
        //     };
        //     let filtered = iterator.filter::<_, crate::executor::instruction::has_executor::HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
        //     let as_tuples: HasBoundedSortedAttribute =
        //         filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
        //     Ok(TupleIterator::HasBounded(SortedTupleIterator::new(
        //         as_tuples,
        //         self.tuple_positions.clone(),
        //         &self.variable_modes,
        //     )))
        // }
        todo!()
    }

    fn compare_has_by_attribute_then_owner<'a, 'b>(
        pair: (&'a Result<(concept::thing::has::Has<'a>, u64), ConceptReadError>, &'b Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>),
    ) -> Ordering {
        let (result_1, result_2) = pair;
        match (result_1, result_2) {
            (Ok((has_1, _)), Ok((has_2, _))) => {
                has_1.attribute().cmp(&has_2.attribute()).then(has_1.owner().cmp(&has_2.owner()))
            }
            _ => Ordering::Equal,
        }
    }
}

