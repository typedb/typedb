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
        attribute::{Attribute, AttributeIterator},
        entity::{Entity, EntityIterator},
        relation::{Relation, RelationIterator},
        thing_manager::ThingManager,
    },
};
use ir::pattern::constraint::Isa;
use itertools::Position;
use lending_iterator::{adaptors::Map, AsHkt, LendingIterator};
use resource::constants::traversal::CONSTANT_CONCEPT_LIMIT;
use storage::snapshot::ReadableSnapshot;

use crate::executor::{
    batch::ImmutableRow,
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{
            enumerated_or_counted_range, enumerated_range, isa_attribute_to_tuple_thing_type,
            isa_entity_to_tuple_thing_type, isa_relation_to_tuple_thing_type, TuplePositions, TupleResult,
        },
        VariableModes,
    },
    VariablePosition,
};

pub(crate) struct IsaExecutor {
    isa: Isa<VariablePosition>,
    iterate_mode: IterateMode,
    variable_modes: VariableModes,
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

pub(crate) type IsaUnboundedSortedThingEntitySingle = Map<EntityIterator, EntityToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type IsaUnboundedSortedThingRelationSingle =
    Map<RelationIterator, RelationToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type IsaUnboundedSortedThingAttributeSingle =
    Map<AttributeIterator, AttributeToTupleFn, AsHkt![TupleResult<'_>]>;

type EntityToTupleFn = for<'a> fn(Result<Entity<'a>, ConceptReadError>) -> TupleResult<'a>;
type RelationToTupleFn = for<'a> fn(Result<Relation<'a>, ConceptReadError>) -> TupleResult<'a>;
type AttributeToTupleFn = for<'a> fn(Result<Attribute<'a>, ConceptReadError>) -> TupleResult<'a>;

impl IterateMode {
    fn new(isa: &Isa<VariablePosition>, var_modes: &VariableModes, sort_by: Option<VariablePosition>) -> IterateMode {
        debug_assert!(!var_modes.fully_bound());
        if var_modes.fully_unbound() {
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
            IterateMode::BoundFromSortedTo
        }
    }
}

impl IsaExecutor {
    pub(crate) fn new(
        isa: Isa<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
        constraint_types: Arc<BTreeMap<Type, Vec<Type>>>,
        thing_types: Arc<HashSet<Type>>,
    ) -> Self {
        debug_assert!(thing_types.len() > 0);
        let iterate_mode = IterateMode::new(&isa, &variable_modes, sort_by);
        let type_cache = if matches!(iterate_mode, IterateMode::UnboundSortedTo) {
            let mut cache = thing_types.clone();
            debug_assert!(cache.len() < CONSTANT_CONCEPT_LIMIT);
            Some(cache)
        } else {
            None
        };

        Self { isa, iterate_mode, variable_modes, type_instance_types: constraint_types, thing_types, type_cache }
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self.iterate_mode {
            IterateMode::UnboundSortedFrom => {
                todo!()
            }
            IterateMode::UnboundSortedTo => {
                debug_assert!(self.type_cache.is_some());
                let positions = TuplePositions::Pair([self.isa.thing(), self.isa.type_()]);
                let enumerated = enumerated_range(&self.variable_modes, &positions);
                let enumerated_or_counted = enumerated_or_counted_range(&self.variable_modes, &positions);
                if self.type_cache.as_ref().unwrap().len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    match &self.type_cache.iter().flat_map(|types| types.iter()).next().unwrap() {
                        Type::Entity(entity_type) => {
                            let iterator = thing_manager.get_entities_in(snapshot, entity_type.clone());
                            let as_tuples: IsaUnboundedSortedThingEntitySingle =
                                iterator.map(isa_entity_to_tuple_thing_type);
                            Ok(TupleIterator::IsaEntityInvertedSingle(SortedTupleIterator::new(
                                as_tuples,
                                positions,
                                0,
                                enumerated,
                                enumerated_or_counted,
                            )))
                        }
                        Type::Relation(relation_type) => {
                            let iterator = thing_manager.get_relations_in(snapshot, relation_type.clone());
                            let as_tuples: IsaUnboundedSortedThingRelationSingle =
                                iterator.map(isa_relation_to_tuple_thing_type);
                            Ok(TupleIterator::IsaRelationInvertedSingle(SortedTupleIterator::new(
                                as_tuples,
                                positions,
                                0,
                                enumerated,
                                enumerated_or_counted,
                            )))
                        }
                        Type::Attribute(attribute_type) => {
                            let iterator = thing_manager.get_attributes_in(snapshot, attribute_type.clone())?;
                            let as_tuples: IsaUnboundedSortedThingAttributeSingle =
                                iterator.map(isa_attribute_to_tuple_thing_type);
                            Ok(TupleIterator::IsaAttributeInvertedSingle(SortedTupleIterator::new(
                                as_tuples,
                                positions,
                                0,
                                enumerated,
                                enumerated_or_counted,
                            )))
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
