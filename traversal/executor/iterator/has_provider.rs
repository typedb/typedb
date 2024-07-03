/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use std::cmp::Ordering;

use itertools::Itertools;

use answer::{Thing, Type, variable::Variable};
use answer::variable_value::VariableValue;
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        object::{HasIterator, Object, ObjectAPI},
        thing_manager::ThingManager,
    },
    type_::attribute_type::AttributeType,
};
use ir::pattern::constraint::{Comparison, FunctionCallBinding, Has, RolePlayer};
use lending_iterator::{adaptors::Filter, higher_order::FnHktHelper, LendingIterator, Peekable};
use lending_iterator::kmerge::KMergeBy;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    executor::{pattern_executor::Row, Position},
    planner::pattern_plan::{Iterate, IterateMode},
};
use crate::executor::iterator::ConstraintIterator;
use crate::executor::pattern_executor::ImmutableRow;

pub(crate) struct HasProvider {
    has: Has<Position>,
    iterate_mode: IterateMode,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<HashSet<Type>>,
    filter_fn: HasProviderFilter,

    owner_cache: Option<Vec<Object<'static>>>,
}

enum HasProviderFilter {
    HasFilterBoth(Arc<HasFilterBothFn>),
    HasFilterAttribute(Arc<HasFilterAttributeFn>),
    AttributeFilter(Arc<AttributeFilterFn>),
}

pub(crate) type HasUnboundedSortedFromIterator = Peekable<Filter<HasIterator, Arc<HasFilterBothFn>>>;
// pub(crate) type HasUnboundedSortedToMultiIterator = Peekable<Filter<KMergeBy<HasIterator, HasOrderByAttributeFn>, Arc<HasFilterAttributeFn>>>;
pub(crate) type HasUnboundedSortedToSingleIterator = Peekable<Filter<HasIterator, Arc<HasFilterAttributeFn>>>;
pub(crate) type HasBoundedSortedToIterator = Peekable<Filter<HasIterator, Arc<HasFilterAttributeFn>>>;

type HasFilterBothFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type AttributeFilterFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(Attribute<'b>, u64), ConceptReadError>, bool>;
type HasFilterAttributeFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type HasMergeByAttributeFnHkt = dyn for <'a, 'b> FnHktHelper<(&'a Result<(concept::thing::has::Has<'a>, u64), ConceptReadError>, &'b Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>), Ordering>;
type HasOrderByAttributeFn = for<'a, 'b> fn(&'a Result<(concept::thing::has::Has<'a>, u64), ConceptReadError>, &'b Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>) -> Ordering;

impl HasProviderFilter {
    fn has_both_filter(&self) -> Arc<HasFilterBothFn> {
        match self {
            HasProviderFilter::HasFilterBoth(filter) => filter.clone(),
            HasProviderFilter::HasFilterAttribute(_) => unreachable!("Has attribute filter is not a Has both filter."),
            HasProviderFilter::AttributeFilter(_) => unreachable!("Attribute filter is not Has both filter."),
        }
    }

    fn has_attribute_filter(&self) -> Arc<HasFilterAttributeFn> {
        match self {
            HasProviderFilter::HasFilterBoth(_) => unreachable!("Has both filter is not a Has Attribute filter."),
            HasProviderFilter::HasFilterAttribute(filter) => filter.clone(),
            HasProviderFilter::AttributeFilter(_) => unreachable!("Attribute filter is not Has both filter."),
        }
    }

    fn attribute_filter(&self) -> Arc<AttributeFilterFn> {
        match self {
            HasProviderFilter::HasFilterBoth(_) => unreachable!("Has both filter is not Attribute filter."),
            HasProviderFilter::HasFilterAttribute(_) => unreachable!("Has Attribute filter is not Attribute filter."),
            HasProviderFilter::AttributeFilter(filter) => filter.clone(),
        }
    }
}

impl HasProvider {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: Has<Position>,
        iterate_mode: IterateMode,
        owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
        attribute_types: Arc<HashSet<Type>>,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(owner_attribute_types.len() > 0);
        let filter_fn = match &iterate_mode {
            IterateMode::UnboundSortedFrom => {
                HasProviderFilter::HasFilterBoth(Arc::new({
                    let owner_att_types = owner_attribute_types.clone();
                    let att_types = attribute_types.clone();
                    move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
                        Ok((has, _)) => {
                            owner_att_types.contains_key(&Type::from(has.owner().type_()))
                                && att_types.contains(&Type::Attribute(has.attribute().type_()))
                        }
                        Err(_) => true,
                    }
                }))
            }
            IterateMode::UnboundSortedTo => {
                HasProviderFilter::HasFilterAttribute(Arc::new({
                    let att_types = attribute_types.clone();
                    move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
                        Ok((has, _)) => att_types.contains(&Type::Attribute(has.attribute().type_())),
                        Err(_) => true,
                    }
                }))
            }
            IterateMode::BoundFromSortedTo => {
                HasProviderFilter::AttributeFilter(Arc::new({
                    let att_types = attribute_types.clone();
                    move |result: &Result<(Attribute<'_>, u64), ConceptReadError>| match result {
                        Ok((attribute, _)) => att_types.contains(&Type::Attribute(attribute.type_())),
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
    ) -> Result<ConstraintIterator, ConceptReadError> {
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
                Ok(ConstraintIterator::HasUnboundedSortedOwner(Peekable::new(iterator), self.has.clone()))
            }
            IterateMode::UnboundSortedTo => {
                debug_assert!(self.owner_cache.is_some());
                if self.owner_cache.as_ref().unwrap().len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let iterator: Filter<HasIterator, Arc<HasFilterAttributeFn>> = self.owner_cache.as_ref().unwrap()
                        .get(0).unwrap()
                        .get_has_types_range_unordered(snapshot, thing_manager, self.attribute_types.iter().map(|t| t.as_attribute_type()))?
                        .filter::<_, HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
                    Ok(ConstraintIterator::HasUnboundedSortedAttributeSingle(Peekable::new(iterator), self.has.clone()))
                } else {
                    // TODO: we could create a reusable space for these temporarily held iterators so we don't have allocate again before the merging iterator
                    let mut iterators: Vec<Peekable<HasIterator>> = Vec::with_capacity(self.owner_cache.as_ref().unwrap().len());
                    for iter in self.owner_cache.as_ref().unwrap().iter().map(|object|
                        object.get_has_types_range_unordered(snapshot, thing_manager, self.attribute_types.iter().map(|t| t.as_attribute_type()))
                    ) {
                        iterators.push(Peekable::new(iter?))
                    }

                    // // note: this will always have to heap alloc, if we use don't have a re-usable/small-vec'ed priority queue somewhere
                    // let merged: KMergeBy<HasIterator, HasOrderByAttributeFn> = KMergeBy::new(
                    //     iterators,
                    //    Self::compare_has_by_attribute
                    // );
                    // let filtered: Filter<KMergeBy<HasIterator, HasOrderByAttributeFn>, HasFilterAttributeFn> = merged.filter(self.filter_fn.has_attribute_filter());
                    // ConstraintIterator::HasUnboundedSortedToMulti(merged.filter(self.filter_fn.has_attribute_filter()))
                    todo!()
                }
            }
            IterateMode::BoundFromSortedTo => {
                debug_assert!(row.len() > self.has.owner().as_usize());
                let owner = row.get(self.has.owner());
                let iterator = match owner {
                    VariableValue::Thing(Thing::Entity(entity)) => {
                        entity.get_has_types_range_unordered(snapshot, thing_manager, self.attribute_types.iter().map(|t| t.as_attribute_type()))?
                    }
                    VariableValue::Thing(Thing::Relation(relation)) => {
                        relation.get_has_types_range_unordered(snapshot, thing_manager, self.attribute_types.iter().map(|t| t.as_attribute_type()))?
                    }
                    _ => unreachable!("Has owner must be an entity or relation.")
                };
                let filtered = iterator.filter::<_, HasFilterAttributeFn>(self.filter_fn.has_attribute_filter());
                Ok(ConstraintIterator::HasBoundedSortedAttribute(Peekable::new(filtered), self.has.clone()))
            }
        }
    }

    fn compare_has_by_attribute<'a, 'b>(
        result_1: &'a Result<(concept::thing::has::Has<'a>, u64), ConceptReadError>,
        result_2: &'b Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>
    ) -> Ordering {
        match (result_1, result_2) {
            (Ok((has_1, _)), Ok((has_2, _))) => has_1.attribute().cmp(&has_2.attribute()),
            _ => Ordering::Equal
        }
    }
}
