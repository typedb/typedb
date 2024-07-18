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

use answer::{variable::Variable, variable_value::VariableValue, Thing, Type};
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        object::{HasIterator, Object, ObjectAPI},
        thing_manager::ThingManager,
    },
};
use ir::pattern::constraint::Has;
use lending_iterator::{adaptors::Filter, higher_order::FnHktHelper, kmerge::KMergeBy, LendingIterator, Peekable};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    executor::{
        batch::ImmutableRow,
        instruction::{
            iterator::{HasSortedAttributeIterator, HasSortedOwnerIterator, InstructionIterator},
            VariableMode,
        },
        Position,
    },
    planner::pattern_plan::IterateBounds,
};

pub(crate) struct HasIteratorExecutor {
    has: Has<Position>,
    iterate_mode: IterateMode,
    variable_modes: HasVariableModes,
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
    fn new(has: &Has<Variable>, variable_modes: HasVariableModes, sort_by: Option<Variable>) -> IterateMode {
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
            debug_assert!(variable_modes.owner.is_bound());
            IterateMode::BoundFromSortedTo
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct HasVariableModes {
    owner: VariableMode,
    attribute: VariableMode,
}

impl HasVariableModes {
    fn new(
        has: &Has<Variable>,
        bounds: &IterateBounds<Variable>,
        selected: &Vec<Variable>,
        named: &HashMap<Variable, String>,
    ) -> Self {
        let (owner, attribute) = (has.owner(), has.attribute());
        Self {
            owner: VariableMode::new(bounds.contains(owner), selected.contains(&owner), named.contains_key(&owner)),
            attribute: VariableMode::new(
                bounds.contains(attribute),
                selected.contains(&attribute),
                named.contains_key(&attribute),
            ),
        }
    }

    pub(crate) fn owner(&self) -> VariableMode {
        self.owner
    }

    pub(crate) fn attribute(&self) -> VariableMode {
        self.attribute
    }

    fn is_fully_bound(&self) -> bool {
        self.owner.is_bound() && self.attribute.is_bound()
    }

    fn is_fully_unbound(&self) -> bool {
        self.owner.is_unbound() && self.attribute.is_unbound()
    }
}

enum HasExecutorFilter {
    HasFilterBoth(Arc<HasFilterBothFn>),
    HasFilterAttribute(Arc<HasFilterAttributeFn>),
    AttributeFilter(Arc<AttributeFilterFn>),
}

enum HasIteratorSortedAttribute {
    UnboundedMerged,
}

pub(crate) type HasUnboundedSortedOwnerIterator = Filter<HasIterator, Arc<HasFilterBothFn>>;
pub(crate) type HasUnboundedSortedAttributeMergedIterator =
    Filter<KMergeBy<HasIterator, HasOrderByAttributeFn>, Arc<HasFilterAttributeFn>>;
pub(crate) type HasUnboundedSortedAttributeSingleIterator = Filter<HasIterator, Arc<HasFilterAttributeFn>>;
pub(crate) type HasBoundedSortedAttributeIterator = Filter<HasIterator, Arc<HasFilterAttributeFn>>;

type HasFilterBothFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type AttributeFilterFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(Attribute<'b>, u64), ConceptReadError>, bool>;
type HasFilterAttributeFn =
    dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type HasOrderByAttributeFn = for<'a, 'b> fn(
    (
        &'a Result<(concept::thing::has::Has<'a>, u64), ConceptReadError>,
        &'b Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>,
    ),
) -> Ordering;

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

impl HasIteratorExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: Has<Variable>,
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

        let variable_modes = HasVariableModes::new(&has, &bounds, selected_variables, named_variables);
        debug_assert!(!variable_modes.is_fully_bound());
        let iterate_mode = IterateMode::new(&has, variable_modes, sort_by);
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
    ) -> Result<InstructionIterator, ConceptReadError> {
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
                let peekable = InstructionIterator::HasSortedOwner(
                    HasSortedOwnerIterator::Unbounded(Peekable::new(iterator)),
                    self.has.clone(),
                    self.variable_modes,
                    None,
                );
                Ok(peekable)
            }
            IterateMode::UnboundSortedTo => {
                debug_assert!(self.owner_cache.is_some());
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
                    let peekable = InstructionIterator::HasSortedAttribute(
                        HasSortedAttributeIterator::UnboundedSingle(Peekable::new(iterator)),
                        self.has.clone(),
                        self.variable_modes,
                        None,
                    );
                    Ok(peekable)
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
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
                        KMergeBy::new(iterators, Self::compare_has_by_attribute);
                    let filtered: Filter<KMergeBy<HasIterator, HasOrderByAttributeFn>, Arc<HasFilterAttributeFn>> =
                        merged.filter::<_, HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
                    let peekable = InstructionIterator::HasSortedAttribute(
                        HasSortedAttributeIterator::UnboundedMerged(Peekable::new(filtered)),
                        self.has.clone(),
                        self.variable_modes,
                        None,
                    );
                    Ok(peekable)
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
                let peekable = InstructionIterator::HasSortedAttribute(
                    HasSortedAttributeIterator::Bounded(Peekable::new(filtered)),
                    self.has.clone(),
                    self.variable_modes,
                    None,
                );
                Ok(peekable)
            }
        }
    }

    fn compare_has_by_attribute<'a, 'b>(
        // result_1: &'a Result<(concept::thing::has::Has<'a>, u64), ConceptReadError>,
        // result_2: &'b Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>,
        pair: (
            &'a Result<(concept::thing::has::Has<'a>, u64), ConceptReadError>,
            &'b Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>,
        ),
    ) -> Ordering {
        let (result_1, result_2) = pair;
        match (result_1, result_2) {
            (Ok((has_1, _)), Ok((has_2, _))) => has_1.attribute().cmp(&has_2.attribute()),
            _ => Ordering::Equal,
        }
    }
}

struct HasCheckExecutor {
    has: Has<Position>,
}

impl HasCheckExecutor {
    pub(crate) fn new(has: Has<Position>) -> Self {
        Self { has }
    }

    pub(crate) fn check<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<bool, ConceptReadError> {
        debug_assert!(
            *row.get(self.has.owner()) != VariableValue::Empty
                && *row.get(self.has.attribute()) != VariableValue::Empty
        );
        let owner = row.get(self.has.owner());
        let attribute = row.get(self.has.attribute()).as_thing().as_attribute();
        match owner {
            VariableValue::Thing(Thing::Entity(entity)) => entity.has_attribute(snapshot, thing_manager, attribute),
            VariableValue::Thing(Thing::Relation(relation)) => {
                relation.has_attribute(snapshot, thing_manager, attribute)
            }
            _ => unreachable!("Has owner must be an entity or relation."),
        }
    }
}
