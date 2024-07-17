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
        attribute::AttributeIterator, entity::EntityIterator, relation::RelationIterator, thing_manager::ThingManager,
    },
};
use ir::pattern::constraint::Isa;
use itertools::Itertools;
use lending_iterator::Peekable;
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{
        instruction::{iterator::InstructionIterator, VariableMode},
        pattern_executor::ImmutableRow,
        Position,
    },
    planner::pattern_plan::IterateBounds,
};

pub(crate) struct IsaExecutor {
    isa: Isa<Position>,
    iterate_mode: IterateMode,
    variable_modes: IsaVariableModes,
    // TODO: if we ever want to implement transitivity directly in Executor, we could leverage type instances
    type_instance_types: Arc<BTreeMap<Type, Vec<Type>>>,
    thing_types: Arc<HashSet<Type>>,
    type_cache: Option<Arc<HashSet<Type>>>,
}

enum IterateMode {
    UnboundSortedFrom,
    UnboundSortedTo,
    BoundFromSortedTo,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct IsaVariableModes {
    thing: VariableMode,
    type_: VariableMode,
}

pub(crate) type IsaUnboundedSortedThingEntity = Peekable<EntityIterator>;
pub(crate) type IsaUnboundedSortedThingRelation = Peekable<RelationIterator>;
pub(crate) type IsaUnboundedSortedThingAttribute = Peekable<AttributeIterator>;

impl IsaVariableModes {
    fn new(
        isa: &Isa<Variable>,
        bounds: &IterateBounds<Variable>,
        selected: &Vec<Variable>,
        named: &HashMap<Variable, String>,
    ) -> Self {
        let (thing, type_) = (isa.thing(), isa.type_());
        Self {
            thing: VariableMode::new(bounds.contains(thing), selected.contains(&thing), named.contains_key(&thing)),
            type_: VariableMode::new(bounds.contains(type_), selected.contains(&type_), named.contains_key(&type_)),
        }
    }

    fn is_fully_bound(&self) -> bool {
        self.thing.is_bound() && self.type_.is_bound()
    }

    fn is_fully_unbound(&self) -> bool {
        self.thing.is_unbound() && self.type_.is_unbound()
    }

    pub(crate) fn thing(&self) -> VariableMode {
        self.thing
    }

    pub(crate) fn type_(&self) -> VariableMode {
        self.type_
    }
}

impl IterateMode {
    fn new(isa: &Isa<Variable>, variable_modes: IsaVariableModes, sort_by: Option<Variable>) -> IterateMode {
        debug_assert!(!variable_modes.is_fully_bound());
        if variable_modes.is_fully_unbound() {
            match sort_by {
                None => {
                    // arbitrarily pick from sorted
                    IterateMode::UnboundSortedFrom
                }
                Some(variable) => {
                    if isa.type_() == variable {
                        IterateMode::UnboundSortedFrom
                    } else {
                        IterateMode::UnboundSortedTo
                    }
                }
            }
        } else {
            debug_assert!(variable_modes.type_.is_bound());
            IterateMode::BoundFromSortedTo
        }
    }
}

impl IsaExecutor {
    pub(crate) fn new(
        isa: Isa<Variable>,
        iterate_bounds: IterateBounds<Variable>,
        selected_variables: &Vec<Variable>,
        variable_names: &HashMap<Variable, String>,
        variable_positions: &HashMap<Variable, Position>,
        sort_by: Option<Variable>,
        constraint_types: Arc<BTreeMap<Type, Vec<Type>>>,
        thing_types: Arc<HashSet<Type>>,
    ) -> Self {
        debug_assert!(thing_types.len() > 0);
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
        let variable_modes = IsaVariableModes::new(&isa, &iterate_bounds, selected_variables, variable_names);
        let iterate_mode = IterateMode::new(&isa, variable_modes, sort_by);
        let type_cache = if matches!(iterate_mode, IterateMode::UnboundSortedTo) {
            let mut cache = thing_types.clone();
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            Some(cache)
        } else {
            None
        };

        Self {
            isa: isa.into_ids(variable_positions),
            iterate_mode,
            variable_modes,
            type_instance_types: constraint_types,
            thing_types,
            type_cache,
        }
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<InstructionIterator, ConceptReadError> {
        match self.iterate_mode {
            IterateMode::UnboundSortedFrom => {
                todo!()
            }
            IterateMode::UnboundSortedTo => {
                debug_assert!(self.type_cache.is_some());
                if self.type_cache.as_ref().unwrap().len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    match &self.type_cache.iter().flat_map(|types| types.iter()).next().unwrap() {
                        Type::Entity(entity_type) => {
                            let iterator = InstructionIterator::IsaEntitySortedThing(
                                Peekable::new(thing_manager.get_entities_in(snapshot, entity_type.clone())),
                                self.isa.clone(),
                                self.variable_modes,
                                None,
                            );
                            Ok(iterator)
                        }
                        Type::Relation(relation_type) => {
                            let iterator = InstructionIterator::IsaRelationSortedThing(
                                Peekable::new(thing_manager.get_relations_in(snapshot, relation_type.clone())),
                                self.isa.clone(),
                                self.variable_modes,
                                None,
                            );
                            Ok(iterator)
                        }
                        Type::Attribute(attribute_type) => {
                            let iterator = InstructionIterator::IsaAttributeSortedThing(
                                Peekable::new(thing_manager.get_attributes_in(snapshot, attribute_type.clone())?),
                                self.isa.clone(),
                                self.variable_modes,
                                None,
                            );
                            Ok(iterator)
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
