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
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
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
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::executor::{
    batch::ImmutableRow,
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{has_to_tuple_attribute_owner, has_to_tuple_owner_attribute, Tuple, TuplePositions, TupleResult},
        VariableModes,
    },
    VariablePosition,
};

pub(crate) struct HasExecutor {
    has: ir::pattern::constraint::Has<VariablePosition>,
    iterate_mode: IterateMode,
    variable_modes: VariableModes,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<HashSet<Type>>,
    filter_fn: HasExecutorFilter,

    owner_cache: Option<Vec<Object<'static>>>,
}

#[derive(Debug, Copy, Clone)]
enum IterateMode {
    UnboundSortedFrom,
    UnboundSortedTo,
    BoundFromSortedTo,
}

impl IterateMode {
    fn new(
        has: &ir::pattern::constraint::Has<VariablePosition>,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> IterateMode {
        debug_assert!(!var_modes.fully_bound());
        if var_modes.fully_unbound() {
            match sort_by {
                None => {
                    // arbitrarily pick from sorted
                    IterateMode::UnboundSortedFrom
                }
                Some(variable) => {
                    if has.owner() == variable {
                        IterateMode::UnboundSortedFrom
                    } else {
                        IterateMode::UnboundSortedTo
                    }
                }
            }
        } else {
            IterateMode::BoundFromSortedTo
        }
    }
}

enum HasExecutorFilter {
    HasFilterBoth(Arc<HasFilterBothFn>),
    HasFilterAttribute(Arc<HasFilterAttributeFn>),
    AttributeFilter(Arc<AttributeFilterFn>),
}

pub(crate) type HasUnboundedSortedOwner =
    Map<Filter<HasIterator, Arc<HasFilterBothFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasUnboundedSortedAttributeMerged = Map<
    Filter<KMergeBy<HasIterator, HasOrderByAttributeFn>, Arc<HasFilterAttributeFn>>,
    HasToTupleFn,
    AsHkt![TupleResult<'_>],
>;
pub(crate) type HasUnboundedSortedAttributeSingle =
    Map<Filter<HasIterator, Arc<HasFilterAttributeFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasBoundedSortedAttribute =
    Map<Filter<HasIterator, Arc<HasFilterAttributeFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;

type HasFilterBothFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type AttributeFilterFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(Attribute<'b>, u64), ConceptReadError>, bool>;
type HasFilterAttributeFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type HasOrderByAttributeFn = for<'a, 'b> fn(
    (&'a Result<(Has<'a>, u64), ConceptReadError>, &'b Result<(Has<'b>, u64), ConceptReadError>),
) -> Ordering;

type HasToTupleFn = for<'a> fn(Result<(Has<'a>, u64), ConceptReadError>) -> TupleResult<'a>;

impl HasExecutorFilter {
    fn has_both_filter(&self) -> Arc<HasFilterBothFn> {
        match self {
            HasExecutorFilter::HasFilterBoth(filter) => filter.clone(),
            HasExecutorFilter::HasFilterAttribute(_) => unreachable!("Has attribute filter is not a Has both filter."),
            HasExecutorFilter::AttributeFilter(_) => unreachable!("Attribute filter is not Has both filter."),
        }
    }

    fn has_attribute_filter(&self) -> Arc<HasFilterAttributeFn> {
        match self {
            HasExecutorFilter::HasFilterBoth(_) => unreachable!("Has both filter is not a Has Attribute filter."),
            HasExecutorFilter::HasFilterAttribute(filter) => filter.clone(),
            HasExecutorFilter::AttributeFilter(_) => unreachable!("Attribute filter is not Has Attribute filter."),
        }
    }
}

impl HasExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: ir::pattern::constraint::Has<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
        attribute_types: Arc<HashSet<Type>>,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(owner_attribute_types.len() > 0);
        debug_assert!(!variable_modes.fully_bound());
        let iterate_mode = IterateMode::new(&has, &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            IterateMode::UnboundSortedFrom => HasExecutorFilter::HasFilterBoth(Arc::new({
                let owner_att_types = owner_attribute_types.clone();
                let att_types = attribute_types.clone();
                move |result: &Result<(Has<'_>, u64), ConceptReadError>| match result {
                    Ok((has, _)) => {
                        owner_att_types.contains_key(&Type::from(has.owner().type_()))
                            && att_types.contains(&Type::Attribute(has.attribute().type_()))
                    }
                    Err(_) => true,
                }
            })),
            IterateMode::UnboundSortedTo | IterateMode::BoundFromSortedTo => {
                HasExecutorFilter::HasFilterAttribute(Arc::new({
                    let att_types = attribute_types.clone();
                    move |result: &Result<(Has<'_>, u64), ConceptReadError>| match result {
                        Ok((has, _)) => att_types.contains(&Type::Attribute(has.attribute().type_())),
                        Err(_) => true,
                    }
                }))
            }
        };

        let owner_cache = if matches!(iterate_mode, IterateMode::UnboundSortedTo) {
            let mut cache = Vec::new();
            for owner_type in owner_attribute_types.keys() {
                for result in thing_manager
                    .get_objects_in(snapshot, owner_type.as_object_type())
                    .map_static(|result| result.map(|object| object.clone().into_owned()))
                    .into_iter()
                {
                    match result {
                        Ok(object) => cache.push(object),
                        Err(err) => return Err(err),
                    }
                }
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
            owner_attribute_types: owner_attribute_types,
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
            IterateMode::UnboundSortedFrom => {
                let first_from_type = self.owner_attribute_types.first_key_value().unwrap().0;
                let last_key_from_type = self.owner_attribute_types.last_key_value().unwrap().0;
                let key_range =
                    KeyRange::new_inclusive(first_from_type.as_object_type(), last_key_from_type.as_object_type());
                let filter_fn = self.filter_fn.has_both_filter();
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator: Filter<HasIterator, Arc<HasFilterBothFn>> = thing_manager
                    .get_has_from_type_range_unordered(snapshot, key_range)
                    .filter::<_, HasFilterBothFn>(filter_fn);
                let as_tuples: Map<Filter<HasIterator, Arc<HasFilterBothFn>>, HasToTupleFn, TupleResult> =
                    iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    TuplePositions::Pair([self.has.owner(), self.has.attribute()]),
                    &self.variable_modes,
                )))
            }
            IterateMode::UnboundSortedTo => {
                debug_assert!(self.owner_cache.is_some());
                let positions = TuplePositions::Pair([self.has.attribute(), self.has.owner()]);
                if let Some([iter]) = self.owner_cache.as_deref() {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator = iter
                        .get_has_types_range_unordered(
                            snapshot,
                            thing_manager,
                            // TODO: this should be just the types owned by the one instance's type in the cache!
                            self.attribute_types.iter().map(|t| t.as_attribute_type()),
                        )?
                        .filter::<_, HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
                    let as_tuples: HasUnboundedSortedAttributeSingle =
                        iterator.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                    Ok(TupleIterator::HasUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    // // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
                    let mut iterators: Vec<Peekable<HasIterator>> =
                        Vec::with_capacity(self.owner_cache.as_ref().unwrap().len());
                    for iter in self.owner_cache.as_ref().unwrap().iter().map(|object| {
                        object.get_has_types_range_unordered(
                            snapshot,
                            thing_manager,
                            self.attribute_types.iter().map(|t| t.as_attribute_type()),
                        )
                    }) {
                        iterators.push(Peekable::new(iter?))
                    }

                    // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    let merged: KMergeBy<HasIterator, HasOrderByAttributeFn> =
                        KMergeBy::new(iterators, Self::compare_has_by_attribute_then_owner);
                    let filtered: Filter<KMergeBy<HasIterator, HasOrderByAttributeFn>, Arc<HasFilterAttributeFn>> =
                        merged.filter::<_, HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
                    let as_tuples: HasUnboundedSortedAttributeMerged =
                        filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_attribute_owner);
                    Ok(TupleIterator::HasUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                }
            }
            IterateMode::BoundFromSortedTo => {
                debug_assert!(row.width() > self.has.owner().as_usize());
                let owner = row.get(self.has.owner());
                let iterator = match owner {
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
                let filtered = iterator.filter::<_, HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
                let as_tuples: HasBoundedSortedAttribute =
                    filtered.map::<Result<Tuple<'_>, _>, _>(has_to_tuple_owner_attribute);
                Ok(TupleIterator::HasBounded(SortedTupleIterator::new(
                    as_tuples,
                    TuplePositions::Pair([self.has.owner(), self.has.attribute()]),
                    &self.variable_modes,
                )))
            }
        }
    }

    fn compare_has_by_attribute_then_owner<'a, 'b>(
        pair: (&'a Result<(Has<'a>, u64), ConceptReadError>, &'b Result<(Has<'b>, u64), ConceptReadError>),
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

// struct HasCheckExecutor {
//     has: Has<Position>,
// }
//
// impl HasCheckExecutor {
//     pub(crate) fn new(has: Has<Position>) -> Self {
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
