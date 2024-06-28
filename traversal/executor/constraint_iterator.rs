/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        object::{HasAttributeIterator, HasIterator, Object, ObjectAPI},
        thing_manager::ThingManager,
    },
    type_::attribute_type::AttributeType,
};
use ir::pattern::constraint::{Comparison, FunctionCallBinding, Has, RolePlayer};
use itertools::{kmerge_by, merge_join_by, Itertools, KMergeBy};
use lending_iterator::{
    adaptors::{Filter, Map},
    higher_order::{AdHocHkt, FnHktHelper, FnMutHktHelper},
    LendingIterator,
};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    executor::{pattern_executor::Row, Position},
    planner::pattern_plan::{Iterate, SortedIterateMode},
};

pub(crate) enum ConstraintIteratorProvider {
    Has(HasProvider),
    HasReverse(HasReverseProvider),

    RolePlayer(RolePlayerProvider),
    RolePlayerReverse(RolePlayerReverseProvider),

    // RelationIndex(RelationIndexProvider)
    // RelationIndexReverse(RelationIndexReverseProvider)
    FunctionCallBinding(FunctionCallBindingProvider),

    Comparison(ComparisonProvider),
    ComparisonReverse(ComparisonReverseProvider),
}

impl ConstraintIteratorProvider {
    pub(crate) fn new(iterate: Iterate, variable_mapping: &HashMap<Variable, Position>) -> Self {
        match iterate {
            Iterate::Has(has, mode) => {
                todo!()
                // Self::Has(HasProvider { has: has.into_ids(variable_mapping), iterate_mode: mode })
            }
            Iterate::HasReverse(has, mode) => {
                Self::HasReverse(HasReverseProvider { has: has.into_ids(variable_mapping), iterate_mode: mode })
            }
            Iterate::RolePlayer(rp, mode) => {
                Self::RolePlayer(RolePlayerProvider { role_player: rp.into_ids(variable_mapping), iterate_mode: mode })
            }
            Iterate::RolePlayerReverse(rp, mode) => Self::RolePlayerReverse(RolePlayerReverseProvider {
                role_player: rp.into_ids(variable_mapping),
                iterate_mode: mode,
            }),
            Iterate::FunctionCallBinding(function_call) => {
                todo!()
            }
            Iterate::Comparison(comparison) => {
                todo!()
            }
            Iterate::ComparisonReverse(comparison) => {
                todo!()
            }
        }
    }

    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        // match self {
        //     ConstraintIteratorProvider::Has(provider) => provider.get_iterator(row),
        //     ConstraintIteratorProvider::HasReverse(provider) => provider.get_iterator(row),
        //     ConstraintIteratorProvider::RolePlayer(provider) => provider.get_iterator(row),
        //     ConstraintIteratorProvider::RolePlayerReverse(provider) => provider.get_iterator(row),
        //     ConstraintIteratorProvider::FunctionCallBinding(provider) => todo!(),
        //     ConstraintIteratorProvider::Comparison(provider) => todo!(),
        //     ConstraintIteratorProvider::ComparisonReverse(provider) => todo!(),
        // }
        todo!()
    }
}

struct HasProvider {
    has: Has<Position>,
    iterate_mode: SortedIterateMode,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<HashSet<AttributeType<'static>>>,
    filter_fn: HasProviderFilter,

    owner_cache: Option<Vec<Object<'static>>>,
}

enum HasProviderFilter {
    HasFilter(Arc<HasFilterFn>),
    AttributeFilter(Arc<AttributeFilterFn>),
}

type HasFilterFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(concept::thing::has::Has<'b>, u64), ConceptReadError>, bool>;
type AttributeFilterFn = dyn for<'a, 'b> FnHktHelper<&'a Result<(Attribute<'b>, u64), ConceptReadError>, bool>;

impl HasProviderFilter {
    fn has_filter(&self) -> Arc<HasFilterFn> {
        match self {
            HasProviderFilter::HasFilter(filter) => filter.clone(),
            HasProviderFilter::AttributeFilter(_) => unreachable!("Attribute filter is not has filter."),
        }
    }

    fn attribute_filter(&self) -> Arc<AttributeFilterFn> {
        match self {
            HasProviderFilter::HasFilter(_) => unreachable!("Has filter is not attribute filter."),
            HasProviderFilter::AttributeFilter(filter) => filter.clone(),
        }
    }
}

impl HasProvider {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        has: Has<Position>,
        iterate_mode: SortedIterateMode,
        owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>, // vecs are in sorted order
        attribute_types: Arc<HashSet<AttributeType<'static>>>,
        snapshot: &Snapshot,
        thing_manager: ThingManager<Snapshot>,
    ) -> Result<Self, ConceptReadError> {
        debug_assert!(owner_attribute_types.len() > 0);
        let filter_fn = if iterate_mode.is_unbounded() {
            HasProviderFilter::HasFilter(Arc::new({
                let owner_att_types = owner_attribute_types.clone();
                let att_types = attribute_types.clone();
                move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
                    Ok((has, _)) => {
                        owner_att_types.contains_key(&Type::from(has.owner().type_()))
                            && att_types.contains(&has.attribute().type_())
                    }
                    Err(_) => true,
                }
            }))
        } else {
            HasProviderFilter::AttributeFilter(Arc::new({
                let att_types = attribute_types.clone();
                move |result: &Result<(Attribute<'_>, u64), ConceptReadError>| match result {
                    Ok((attribute, _)) => att_types.contains(&attribute.type_()),
                    Err(_) => true,
                }
            }))
        };

        let owner_cache = if matches!(iterate_mode, SortedIterateMode::UnboundSortedTo) {
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
        thing_manager: &ThingManager<Snapshot>,
        row: &Row,
    ) -> Result<ConstraintIterator, ConceptReadError> {
        match self.iterate_mode {
            SortedIterateMode::UnboundSortedFrom => {
                let first_from_type = self.owner_attribute_types.first_key_value().unwrap().0;
                let last_key_from_type = self.owner_attribute_types.last_key_value().unwrap().0;
                let key_range =
                    KeyRange::new_inclusive(first_from_type.as_object_type(), last_key_from_type.as_object_type());
                let filter_fn = self.filter_fn.has_filter();
                // TODO: we could cache the range byte arrays computed inside the thing_manager, for this case
                let iterator: Filter<HasIterator, Arc<HasFilterFn>> = thing_manager
                    .get_has_from_type_range_unordered(snapshot, key_range)
                    .filter::<_, HasFilterFn>(filter_fn);
                Ok(ConstraintIterator::HasUnboundedSortedFrom(iterator))
            }
            SortedIterateMode::UnboundSortedTo => {
                // debug_assert!(self.owner_cache.is_some());
                // let mut iterators = Vec::new();
                // for iter in self.owner_cache.as_ref().unwrap().iter().map(|object|
                //     object.get_has_types_range_unordered(snapshot, thing_manager, self.attribute_types.iter().cloned())
                // ) {
                //     let iter: HasAttributeIterator = iter?;
                //
                // }
                //
                // let merged = kmerge_by(iterators, |(attr_1, _), (attr_2, _)| attr_1 < attr_2);
                // let unique = merged.dedup_by(|(attr_1, _)| attr_1);
                // let single_counts = unique.map(|(attr, count)| (attr, 1));
                //
                // // ConstraintIterator::HasUnboundSortedTo(single_counts)
                todo!()
            }
            SortedIterateMode::BoundFromSortedTo => {
                todo!()
            }
        }
    }
}

struct HasReverseProvider {
    has: Has<Position>,
    iterate_mode: SortedIterateMode,
}

impl HasReverseProvider {
    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        todo!()
    }
}

struct RolePlayerProvider {
    role_player: RolePlayer<Position>,
    iterate_mode: SortedIterateMode,
}

impl RolePlayerProvider {
    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        todo!()
    }
}

struct RolePlayerReverseProvider {
    role_player: RolePlayer<Position>,
    iterate_mode: SortedIterateMode,
}

impl RolePlayerReverseProvider {
    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        todo!()
    }
}

struct FunctionCallBindingProvider {
    function_call_binding: FunctionCallBinding<Position>,
}

struct ComparisonProvider {
    comparison: Comparison<Position>,
}

struct ComparisonReverseProvider {
    comparision: Comparison<Position>,
}

pub(crate) enum ConstraintIterator {
    HasUnboundedSortedFrom(Filter<HasIterator, Arc<HasFilterFn>>),
}
