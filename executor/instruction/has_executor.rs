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

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::instruction::constraint::instructions::HasInstruction;
use concept::{
    error::ConceptReadError,
    thing::{
        has::Has,
        object::{HasIterator, Object, ObjectAPI},
        thing_manager::ThingManager,
    },
};
use lending_iterator::{
    adaptors::{Filter, Map},
    higher_order::FnHktHelper,
    kmerge::KMergeBy,
    AsHkt, LendingIterator, Peekable,
};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    batch::ImmutableRow,
    instruction::{
        iterator::{inverted_instances_cache, SortedTupleIterator, TupleIterator},
        tuple::{
            has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, HasToTupleFn, Tuple, TuplePositions,
            TupleResult,
        },
        BinaryIterateMode, VariableModes,
    },
    VariablePosition,
};

pub(crate) struct HasExecutor {
    has: ir::pattern::constraint::Has<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<HashSet<Type>>,
    filter_fn: Arc<HasFilterFn>,
    owner_cache: Option<Vec<Object<'static>>>,
}

pub(crate) type HasUnboundedSortedOwner =
    Map<Filter<HasIterator, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasUnboundedSortedAttributeMerged =
    Map<Filter<KMergeBy<HasIterator, HasOrderingFn>, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasUnboundedSortedAttributeSingle =
    Map<Filter<HasIterator, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasBoundedSortedAttribute =
    Map<Filter<HasIterator, Arc<HasFilterFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;

pub(crate) type HasFilterFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;

pub(crate) type HasOrderingFn = for<'a, 'b> fn(
    (&'a Result<(Has<'a>, u64), ConceptReadError>, &'b Result<(Has<'b>, u64), ConceptReadError>),
) -> Ordering;

impl HasExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: HasInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(!variable_modes.all_inputs());
        let owner_attribute_types = has.edge_types();
        debug_assert!(owner_attribute_types.len() > 0);
        let attribute_types = has.end_types();
        let has = has.constraint;
        let iterate_mode = BinaryIterateMode::new(has.owner(), has.attribute(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => Self::create_has_filter_owners_attributes(owner_attribute_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                Self::create_has_filter_attributes(attribute_types.clone())
            }
        };
        let output_tuple_positions = if iterate_mode.is_inverted() {
            TuplePositions::Pair([has.attribute(), has.owner()])
        } else {
            TuplePositions::Pair([has.owner(), has.attribute()])
        };

        let owner_cache = if iterate_mode == BinaryIterateMode::UnboundInverted {
            Some(inverted_instances_cache(
                owner_attribute_types.keys().map(|t| t.as_object_type()),
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
            owner_attribute_types,
            attribute_types,
            filter_fn,
            owner_cache,
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
                let first_from_type = self.owner_attribute_types.first_key_value().unwrap().0;
                let last_key_from_type = self.owner_attribute_types.last_key_value().unwrap().0;
                let key_range =
                    KeyRange::new_inclusive(first_from_type.as_object_type(), last_key_from_type.as_object_type());
                let filter_fn = self.filter_fn.clone();
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator: Filter<HasIterator, Arc<HasFilterFn>> = thing_manager
                    .get_has_from_owner_type_range_unordered(snapshot, key_range)
                    .filter::<_, HasFilterFn>(filter_fn);
                let as_tuples: Map<Filter<HasIterator, Arc<HasFilterFn>>, HasToTupleFn, TupleResult<'_>> =
                    iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                debug_assert!(self.owner_cache.is_some());

                if let Some([owner]) = self.owner_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = owner
                        .get_has_types_range_unordered(
                            snapshot,
                            thing_manager,
                            // TODO: this should be just the types owned by the one instance's type in the cache!
                            self.attribute_types.iter().map(|t| t.as_attribute_type()),
                        )?
                        .filter::<_, HasFilterFn>(self.filter_fn.clone());
                    let as_tuples: HasUnboundedSortedAttributeSingle =
                        iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                    Ok(TupleIterator::HasUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators
                    //       so we don't have allocate again before the merging iterator
                    let owners = self.owner_cache.as_ref().unwrap().iter();
                    let iterators = owners
                        .map(|object| {
                            Ok(Peekable::new(object.get_has_types_range_unordered(
                                snapshot,
                                thing_manager,
                                self.attribute_types.iter().map(|ty| ty.as_attribute_type()),
                            )?))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<HasIterator, HasOrderingFn> =
                        KMergeBy::new(iterators, Self::compare_has_by_attribute_then_owner);
                    let filtered: Filter<KMergeBy<HasIterator, HasOrderingFn>, Arc<HasFilterFn>> =
                        merged.filter::<_, HasFilterFn>(self.filter_fn.clone());
                    let as_tuples: HasUnboundedSortedAttributeMerged =
                        filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                    Ok(TupleIterator::HasUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        self.tuple_positions.clone(),
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.width() > self.has.owner().as_usize());
                let iterator = match row.get(self.has.owner()) {
                    VariableValue::Thing(Thing::Entity(entity)) => entity.get_has_types_range_unordered(
                        snapshot,
                        thing_manager,
                        self.attribute_types.iter().map(|t| t.as_attribute_type()),
                    )?,
                    VariableValue::Thing(Thing::Relation(relation)) => relation.get_has_types_range_unordered(
                        snapshot,
                        thing_manager,
                        self.attribute_types.iter().map(|t| t.as_attribute_type()),
                    )?,
                    _ => unreachable!("Has owner must be an entity or relation."),
                };
                let filtered = iterator.filter::<_, HasFilterFn>(self.filter_fn.clone());
                let as_tuples: HasBoundedSortedAttribute =
                    filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn create_has_filter_owners_attributes(owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<HasFilterFn> {
        Arc::new(move |result| match result {
            Ok((has, _)) => match owner_attribute_types.get(&Type::from(has.owner().type_())) {
                Some(attribute_types) => attribute_types.contains(&Type::Attribute(has.attribute().type_())),
                None => false,
            },
            Err(_) => true,
        })
    }

    fn create_has_filter_attributes(attribute_types: Arc<HashSet<Type>>) -> Arc<HasFilterFn> {
        Arc::new(move |result| match result {
            Ok((has, _)) => attribute_types.contains(&Type::Attribute(has.attribute().type_())),
            Err(_) => true,
        })
    }

    fn compare_has_by_attribute_then_owner(
        pair: (&Result<(Has<'_>, u64), ConceptReadError>, &Result<(Has<'_>, u64), ConceptReadError>),
    ) -> Ordering {
        if let (Ok((has_1, _)), Ok((has_2, _))) = pair {
            (has_1.attribute(), has_2.owner()).cmp(&(has_2.attribute(), has_2.owner()))
        } else {
            Ordering::Equal
        }
    }
}
//
// struct HasCheckExecutor {
//     has: ir::pattern::constraint::Has<VariablePosition>,
// }
//
// impl HasCheckExecutor {
//     pub(crate) fn new(ir::pattern::constraint::Has<VariablePosition>,) -> Self {
//         Self { has }
//     }
//
//     pub(crate) fn check<Snapshot: ReadableSnapshot>(
//         &self,
//         snapshot: &Snapshot,
//         thing_manager: &ThingManager,
//         row: ImmutableRow<'_>,
//     ) -> Result<bool, ConceptReadError> {
//         debug_assert!(
//             *row.get(self.has.owner()) != VariableValue::Empty
//                 && *row.get(self.has.attribute()) != VariableValue::Empty
//         );
//         let owner = row.get(self.has.owner());
//         let attribute = row.get(self.has.attribute()).as_thing().as_attribute();
//         match owner {
//             VariableValue::Thing(Thing::Entity(entity)) => entity.has_attribute(snapshot, thing_manager, attribute),
//             VariableValue::Thing(Thing::Relation(relation)) => {
//                 relation.has_attribute(snapshot, thing_manager, attribute)
//             }
//             _ => unreachable!("Has owner must be an entity or relation."),
//         }
//     }
// }
