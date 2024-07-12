/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use answer::Type;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pattern::constraint::Isa;
use itertools::Itertools;
use lending_iterator::Peekable;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{iterator::ConstraintIterator, pattern_executor::ImmutableRow, Position},
    planner::pattern_plan::IterateMode,
};

pub(crate) struct IsaProvider {
    isa: Isa<Position>,
    iterate_mode: IterateMode,
    type_instance_types: Arc<BTreeMap<Type, Vec<Type>>>,
    thing_types: Arc<HashSet<Type>>,
    // filter_fn: crate::executor::iterator::has_provider::HasProviderFilter,
    type_cache: Option<Vec<Type>>,
}

impl IsaProvider {
    pub(crate) fn new(
        isa: Isa<Position>,
        iterate_mode: IterateMode,
        type_instance_types: Arc<BTreeMap<Type, Vec<Type>>>,
        thing_types: Arc<HashSet<Type>>,
    ) -> Self {
        debug_assert!(type_instance_types.len() > 0);
        // let filter_fn = match &iterate_mode {
        //     IterateMode::UnboundSortedFrom => crate::executor::iterator::has_provider::HasProviderFilter::HasFilterBoth(Arc::new({
        //         let owner_att_types = owner_attribute_types.clone();
        //         let att_types = attribute_types.clone();
        //         move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
        //             Ok((has, _)) => {
        //                 owner_att_types.contains_key(&Type::from(has.owner().type_()))
        //                     && att_types.contains(&Type::Attribute(has.attribute().type_()))
        //             }
        //             Err(_) => true,
        //         }
        //     })),
        //     IterateMode::UnboundSortedTo => crate::executor::iterator::has_provider::HasProviderFilter::HasFilterAttribute(Arc::new({
        //         let att_types = attribute_types.clone();
        //         move |result: &Result<(concept::thing::has::Has<'_>, u64), ConceptReadError>| match result {
        //             Ok((has, _)) => att_types.contains(&Type::Attribute(has.attribute().type_())),
        //             Err(_) => true,
        //         }
        //     })),
        //     IterateMode::BoundFromSortedTo => crate::executor::iterator::has_provider::HasProviderFilter::AttributeFilter(Arc::new({
        //         let att_types = attribute_types.clone();
        //         move |result: &Result<(Attribute<'_>, u64), ConceptReadError>| match result {
        //             Ok((attribute, _)) => att_types.contains(&Type::Attribute(attribute.type_())),
        //             Err(_) => true,
        //         }
        //     })),
        // };

        let type_cache = if matches!(iterate_mode, IterateMode::UnboundSortedTo) {
            let mut cache = type_instance_types.keys().cloned().collect_vec();
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            Some(cache)
        } else {
            None
        };

        Self { isa, iterate_mode, type_instance_types, thing_types, type_cache }
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<ConstraintIterator, ConceptReadError> {
        match self.iterate_mode {
            IterateMode::UnboundSortedFrom => {
                todo!()
            }
            IterateMode::UnboundSortedTo => {
                debug_assert!(self.type_cache.is_some());
                if self.type_cache.as_ref().unwrap().len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    // TODO: move this into the 'if'
                    let thing_type = &self.type_instance_types.get(&self.type_cache.as_ref().unwrap()[0]).unwrap()[0];
                    match thing_type {
                        Type::Entity(entity_type) => {
                            let iterator = ConstraintIterator::IsaEntitySortedThing(
                                Peekable::new(thing_manager.get_entities_in(snapshot, entity_type.clone())),
                                self.isa.clone(),
                            );
                            Ok(iterator)
                        }
                        Type::Relation(relation_type) => {
                            let iterator = ConstraintIterator::IsaRelationSortedThing(
                                Peekable::new(thing_manager.get_relations_in(snapshot, relation_type.clone())),
                                self.isa.clone(),
                            );
                            Ok(iterator)
                        }
                        Type::Attribute(attribute_type) => {
                            todo!()
                            // let attribute_iterator = thing_manager.get_attributes_in(snapshot, attribute_type.clone())?;
                            // Ok(ConstraintIterator::IsaAttributeSortedThing(attribute_iterator))
                        }
                        Type::RoleType(_) => unreachable!("Cannot get instances of role types."),
                    }
                } else {
                    todo!()
                }
            }
            IterateMode::BoundFromSortedTo => {
                todo!()
            }
        }
    }
}
