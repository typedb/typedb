/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc, vec};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::instruction::constraint::instructions::IsaInstruction;
use concept::{
    error::ConceptReadError,
    iterator::InstanceIterator,
    thing::{
        attribute::{Attribute, AttributeIterator},
        entity::Entity,
        object::Object,
        relation::Relation,
        thing_manager::ThingManager,
    },
};
use ir::pattern::constraint::Isa;
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Chain, Flatten, Map},
    AsHkt, AsLendingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::ImmutableRow,
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, Tuple, TuplePositions, TupleResult},
        BinaryIterateMode, VariableModes,
    },
    VariablePosition,
};

pub(crate) struct IsaExecutor {
    isa: Isa<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    types: Arc<HashSet<Type>>,
}

pub(crate) enum IsaIterator {
    Entity(Map<InstanceIterator<AsHkt![Entity<'_>]>, EntityEraseFn, Result<AsHkt![Thing<'_>], ConceptReadError>>),
    Relation(Map<InstanceIterator<AsHkt![Relation<'_>]>, RelationEraseFn, Result<AsHkt![Thing<'_>], ConceptReadError>>),
    Attribute(
        Map<
            AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>,
            AttributeEraseFn,
            Result<AsHkt![Thing<'_>], ConceptReadError>,
        >,
    ),
}

impl LendingIterator for IsaIterator {
    type Item<'a> = Result<Thing<'a>, ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            IsaIterator::Entity(inner) => inner.next(),
            IsaIterator::Relation(inner) => inner.next(),
            IsaIterator::Attribute(inner) => inner.next(),
        }
    }
}

type MultipleTypeObjectIterator = Flatten<AsLendingIterator<vec::IntoIter<InstanceIterator<Object<'static>>>>>;
type MultipleTypeAttributeIterator =
    Flatten<AsLendingIterator<vec::IntoIter<AttributeIterator<InstanceIterator<Attribute<'static>>>>>>;
pub(crate) type MultipleTypeThingIterator = Chain<
    Map<MultipleTypeObjectIterator, ObjectEraseFn, Result<Thing<'static>, ConceptReadError>>,
    Map<MultipleTypeAttributeIterator, AttributeEraseFn, Result<Thing<'static>, ConceptReadError>>,
>;

pub(crate) type IsaUnboundedSortedTypeSingle = Map<IsaIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type IsaUnboundedSortedTypeMerged = Map<MultipleTypeThingIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;

pub(crate) type IsaUnboundedSortedThingSingle = Map<IsaIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;
pub(crate) type IsaUnboundedSortedThingMerged = Map<MultipleTypeThingIterator, ThingToTupleFn, AsHkt![TupleResult<'_>]>;

type ObjectEraseFn = for<'a> fn(Result<Object<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type EntityEraseFn = for<'a> fn(Result<Entity<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type RelationEraseFn = for<'a> fn(Result<Relation<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type AttributeEraseFn = for<'a> fn(Result<Attribute<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;

type ThingToTupleFn = for<'a> fn(Result<Thing<'a>, ConceptReadError>) -> TupleResult<'a>;

pub(crate) type IsaBoundedSortedType = lending_iterator::Once<AsHkt![TupleResult<'_>]>;

impl IsaExecutor {
    pub(crate) fn new(
        isa: IsaInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let types = isa.types().clone();
        debug_assert!(types.len() > 0);
        let isa = isa.isa;
        let iterate_mode = BinaryIterateMode::new(isa.thing(), isa.type_(), &variable_modes, sort_by);

        Self { isa, iterate_mode, variable_modes, types }
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let positions = TuplePositions::Pair([self.isa.thing(), self.isa.type_()]);
                if self.types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.types.iter().next().unwrap();
                    let iterator = instances_of_single_type(type_, thing_manager, snapshot)?;
                    let as_tuples: IsaUnboundedSortedThingSingle = iterator.map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaUnboundedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.types, thing_manager, snapshot)?;
                    let as_tuples: IsaUnboundedSortedThingMerged = thing_iter.map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaUnboundedMerged(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::UnboundInverted => {
                let positions = TuplePositions::Pair([self.isa.type_(), self.isa.thing()]);
                if self.types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.types.iter().next().unwrap();
                    let iterator = instances_of_single_type(type_, thing_manager, snapshot)?;
                    let as_tuples: IsaUnboundedSortedTypeSingle = iterator.map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.types, thing_manager, snapshot)?;
                    let as_tuples: IsaUnboundedSortedTypeMerged = thing_iter.map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                }
            }
            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.width() > self.isa.thing().as_usize());
                let positions = TuplePositions::Pair([self.isa.type_(), self.isa.thing()]);
                let thing = row.get(self.isa.thing()).to_owned();
                let type_ = match &thing {
                    VariableValue::Thing(Thing::Entity(entity)) => Type::from(entity.type_()),
                    VariableValue::Thing(Thing::Relation(relation)) => Type::from(relation.type_()),
                    VariableValue::Thing(Thing::Attribute(attribute)) => Type::from(attribute.type_()),
                    _ => unreachable!("Has thing must be an entity or relation."),
                };
                let as_tuples: IsaBoundedSortedType =
                    lending_iterator::once(Ok(Tuple::Pair([VariableValue::Type(type_), thing])));
                Ok(TupleIterator::IsaBounded(SortedTupleIterator::new(as_tuples, positions, &self.variable_modes)))
            }
        }
    }
}

pub(super) fn instances_of_all_types_chained(
    thing_types: &HashSet<Type>,
    thing_manager: &ThingManager,
    snapshot: &impl ReadableSnapshot,
) -> Result<MultipleTypeThingIterator, ConceptReadError> {
    let (attribute_types, object_types) =
        thing_types.iter().cloned().partition::<Vec<_>, _>(|type_| matches!(type_, Type::Attribute(_)));
    let object_iters = object_types
        .into_iter()
        .map(|type_| thing_manager.get_objects_in(snapshot, type_.as_object_type()))
        .collect_vec();
    let object_iter: Map<MultipleTypeObjectIterator, ObjectEraseFn, Result<Thing<'_>, ConceptReadError>> =
        AsLendingIterator::new(object_iters).flatten().map(|res| res.map(Thing::from));
    let attribute_iters: Vec<AttributeIterator<InstanceIterator<Attribute<'_>>>> = attribute_types
        .into_iter()
        .map(|type_| thing_manager.get_attributes_in(snapshot, type_.as_attribute_type()))
        .try_collect()?;
    let attribute_iter: Map<MultipleTypeAttributeIterator, AttributeEraseFn, Result<Thing<'static>, ConceptReadError>> =
        AsLendingIterator::new(attribute_iters).flatten().map(|res| res.map(Thing::Attribute));
    let thing_iter: MultipleTypeThingIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}

pub(super) fn instances_of_single_type(
    type_: &Type,
    thing_manager: &ThingManager,
    snapshot: &impl ReadableSnapshot,
) -> Result<IsaIterator, ConceptReadError> {
    match type_ {
        Type::Entity(entity_type) => Ok(IsaIterator::Entity(
            thing_manager.get_entities_in(snapshot, entity_type.clone()).map(|res| res.map(Thing::Entity)),
        )),
        Type::Relation(relation_type) => Ok(IsaIterator::Relation(
            thing_manager.get_relations_in(snapshot, relation_type.clone()).map(|res| res.map(Thing::Relation)),
        )),
        Type::Attribute(attribute_type) => Ok(IsaIterator::Attribute(
            thing_manager.get_attributes_in(snapshot, attribute_type.clone())?.map(|res| res.map(Thing::Attribute)),
        )),
        Type::RoleType(_) => unreachable!("Cannot get instances of role types."),
    }
}
