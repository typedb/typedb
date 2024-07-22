/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use std::ops::Range;

use answer::{Thing, Type, variable::Variable, variable_value::VariableValue};
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        object::{HasIterator, Object, ObjectAPI},
        thing_manager::ThingManager,
    },
};
use concept::thing::has::Has;
use lending_iterator::{adaptors::Filter, AsHkt, higher_order::FnHktHelper, kmerge::KMergeBy, LendingIterator, Peekable};
use lending_iterator::adaptors::Map;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    executor::{
        batch::ImmutableRow,
        instruction::VariableMode,
        Position,
    },
    planner::pattern_plan::IterateBounds,
};
use crate::executor::instruction::iterator::{InstructionTuplesIterator, SortedTupleIterator};
use crate::executor::instruction::tuple::{Tuple, TupleIndex, TuplePositions, TupleResult};
use crate::executor::instruction::VariableModes;

pub(crate) struct HasExecutor {
    has: ir::pattern::constraint::Has<Position>,
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
    fn new(has: &ir::pattern::constraint::Has<Variable>, variable_modes: &VariableModes, sort_by: Option<Variable>) -> IterateMode {
        debug_assert!(!variable_modes.is_fully_bound());
        if variable_modes.is_fully_unbound() {
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

pub(crate) type HasUnboundedSortedOwnerIterator = Map<Filter<HasIterator, Arc<HasFilterBothFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasUnboundedSortedAttributeMergedIterator =
Map<Filter<KMergeBy<HasIterator, HasOrderByAttributeFn>, Arc<HasFilterAttributeFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasUnboundedSortedAttributeSingleIterator = Map<Filter<HasIterator, Arc<HasFilterAttributeFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type HasBoundedSortedAttributeIterator = Map<Filter<HasIterator, Arc<HasFilterAttributeFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>;

type HasFilterBothFn =
dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type AttributeFilterFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(Attribute<'b>, u64), ConceptReadError>, bool>;
type HasFilterAttributeFn =
dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type HasOrderByAttributeFn = for<'a, 'b> fn(
    (
        &'a Result<(Has<'a>, u64), ConceptReadError>,
        &'b Result<(Has<'b>, u64), ConceptReadError>,
    ),
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

// TODO: basically, we can't type anything that explicitly returns something with a lfietime bound?
// --> We could create a TupleIterator that implements LendingIterator, with a generic over the input? This is
//     how we achieve HasIterator which returns Has<'a> !
//     Alternatively, we could copy into 'static values.
enum Test {
    Has(Map<Filter<HasIterator, Arc<HasFilterBothFn>>, HasToTupleFn, AsHkt![TupleResult<'_>]>),
}

impl HasExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: ir::pattern::constraint::Has<Variable>,
        bounds: IterateBounds<Variable>,
        selected_variables: &Vec<Variable>,
        named_variables: &HashMap<Variable, String>,
        variable_positions: &HashMap<Variable, Position>,
        sort_by: Option<Variable>,
        owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
        attribute_types: Arc<HashSet<Type>>,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(owner_attribute_types.len() > 0);

        let variable_modes = Self::variable_modes(&has, variable_positions, &bounds, selected_variables, named_variables);
        debug_assert!(!variable_modes.is_fully_bound());
        let iterate_mode = IterateMode::new(&has, &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            IterateMode::UnboundSortedFrom => HasExecutorFilter::HasFilterBoth(Arc::new({
                let owner_att_types = owner_attribute_types.clone();
                let att_types = attribute_types.clone();
                move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
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
                    move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
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
            has: has.into_ids(variable_positions),
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
    ) -> Result<InstructionTuplesIterator, ConceptReadError> {
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
                let as_tuples: Map<Filter<HasIterator, Arc<HasFilterBothFn>>, HasToTupleFn, TupleResult> = iterator
                    .map::<Result<Tuple<'_>, _>, _>(Self::has_to_tuple_owner_attribute);
                let positions = TuplePositions::Pair([self.has.owner(), self.has.attribute()]);
                let enumerated = Self::enumerated_range(&self.variable_modes, &positions);
                let enumerated_or_counted = Self::enumerated_or_counted_range(&self.variable_modes, &positions);
                Ok(InstructionTuplesIterator::HasUnbounded(SortedTupleIterator::new(
                    as_tuples,
                    positions,
                    0,
                    enumerated,
                    enumerated_or_counted,
                )))
            }
            IterateMode::UnboundSortedTo => {
                debug_assert!(self.owner_cache.is_some());
                let positions = TuplePositions::Pair([self.has.attribute(), self.has.owner()]);
                let enumerated = Self::enumerated_range(&self.variable_modes, &positions);
                let enumerated_or_counted = Self::enumerated_or_counted_range(&self.variable_modes, &positions);
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
                    let as_tuples: HasUnboundedSortedAttributeSingleIterator = iterator
                        .map::<Result<Tuple<'_>, _>, _>(Self::has_to_tuple_attribute_owner);
                    Ok(InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        0,
                        enumerated,
                        enumerated_or_counted
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
                    let as_tuples: HasUnboundedSortedAttributeMergedIterator = filtered
                        .map::<Result<Tuple<'_>, _>, _>(Self::has_to_tuple_attribute_owner);

                    Ok(InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        0,
                        enumerated,
                        enumerated_or_counted
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
                let as_tuples: HasBoundedSortedAttributeIterator = filtered
                    .map::<Result<Tuple<'_>, _>, _>(Self::has_to_tuple_owner_attribute);
                let positions = TuplePositions::Pair([self.has.owner(), self.has.attribute()]);
                let enumerated = Self::enumerated_range(&self.variable_modes, &positions);
                let enumerated_or_counted = Self::enumerated_or_counted_range(&self.variable_modes, &positions);
                Ok(InstructionTuplesIterator::HasBounded(SortedTupleIterator::new(
                    as_tuples,
                    positions,
                    1,
                    enumerated,
                    enumerated_or_counted,
                )))
            }
        }
    }

    fn compare_has_by_attribute_then_owner<'a, 'b>(
        pair: (
            &'a Result<(Has<'a>, u64), ConceptReadError>,
            &'b Result<(Has<'b>, u64), ConceptReadError>,
        ),
    ) -> Ordering {
        let (result_1, result_2) = pair;
        match (result_1, result_2) {
            (Ok((has_1, _)), Ok((has_2, _))) => {
                has_1.attribute().cmp(&has_2.attribute())
                    .then(has_1.owner().cmp(&has_2.owner()))
            }
            _ => Ordering::Equal,
        }
    }

    fn has_to_tuple_owner_attribute<'a>(result: Result<(Has<'a>, u64), ConceptReadError>) -> Result<Tuple<'a>, ConceptReadError> {
        match result {
            Ok((has, count)) => {
                let (owner, attribute) = has.into_owner_attribute();
                Ok(Tuple::Pair([
                    VariableValue::Thing(owner.into()),
                    VariableValue::Thing(attribute.into()),
                ]))
            }
            Err(err) => Err(err)
        }
    }

    fn has_to_tuple_attribute_owner<'a>(result: Result<(Has<'a>, u64), ConceptReadError>) -> Result<Tuple<'a>, ConceptReadError> {
        match result {
            Ok((has, count)) => {
                let (owner, attribute) = has.into_owner_attribute();
                Ok(Tuple::Pair([
                    VariableValue::Thing(attribute.into()),
                    VariableValue::Thing(owner.into()),
                ]))
            }
            Err(err) => Err(err)
        }
    }

    fn enumerated_range(
        variable_modes: &VariableModes,
        positions: &TuplePositions,
    ) -> Range<TupleIndex> {
        let mut last_enumerated = None;
        for (i, position) in positions.positions().iter().enumerate() {
            match variable_modes.get(*position).unwrap() {
                VariableMode::BoundSelect | VariableMode::UnboundSelect => {
                    last_enumerated = Some(i as TupleIndex);
                }
                VariableMode::UnboundCount => {}
                VariableMode::UnboundCheck => {}
            }
        }
        last_enumerated.map_or(0..0, |last| 0..last + 1)
    }

    fn enumerated_or_counted_range(
        variable_modes: &VariableModes,
        positions: &TuplePositions,
    ) -> Range<TupleIndex> {
        let mut last_enumerated_or_counted = None;
        for (i, position) in positions.positions().iter().enumerate() {
            match variable_modes.get(*position).unwrap() {
                VariableMode::BoundSelect | VariableMode::UnboundSelect | VariableMode::UnboundCount => {
                    last_enumerated_or_counted = Some(i as TupleIndex)
                }
                VariableMode::UnboundCheck => {}
            }
        }
        last_enumerated_or_counted.map_or(0..0, |last| 0..last + 1)
    }

    fn variable_modes(
        has: &ir::pattern::constraint::Has<Variable>,
        variable_positions: &HashMap<Variable, Position>,
        bounds: &IterateBounds<Variable>,
        selected: &Vec<Variable>,
        named: &HashMap<Variable, String>
    ) -> VariableModes {
        let (owner, attribute) = (has.owner(), has.attribute());
        let mut variable_modes = VariableModes::new();
        variable_modes.insert(
            *variable_positions.get(&owner).unwrap(),
            VariableMode::new(bounds.contains(owner), selected.contains(&owner), named.contains_key(&owner))
        );
        variable_modes.insert(
            *variable_positions.get(&attribute).unwrap(),
            VariableMode::new(bounds.contains(attribute), selected.contains(&attribute), named.contains_key(&attribute))
        );
        variable_modes
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
