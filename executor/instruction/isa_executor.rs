/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::{Thing, Type, variable_value::VariableValue};
use compiler::match_::instructions::IsaInstruction;
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
    adaptors::{Chain, Flatten, Map, TryFilter},
    AsHkt, AsLendingIterator, LendingIterator,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        BinaryIterateMode,
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, Tuple, TuplePositions, TupleResult}, VariableModes,
    },
    VariablePosition,
};
use crate::instruction::{Checker, FilterFn};
use crate::row::MaybeOwnedRow;

pub(crate) struct IsaExecutor {
    isa: Isa<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    types: Arc<HashSet<Type>>,
    checker: Checker<AsHkt![Thing<'_>]>,
}

type MapToThing<I, F> = Map<I, F, Result<AsHkt![Thing<'_>], ConceptReadError>>;

#[allow(clippy::large_enum_variant)]
pub(crate) enum SingleTypeIsaIterator {
    Entity(MapToThing<InstanceIterator<AsHkt![Entity<'_>]>, EntityEraseFn>),
    Relation(MapToThing<InstanceIterator<AsHkt![Relation<'_>]>, RelationEraseFn>),
    Attribute(MapToThing<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, AttributeEraseFn>),
}

impl LendingIterator for SingleTypeIsaIterator {
    type Item<'a> = Result<Thing<'a>, ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            SingleTypeIsaIterator::Entity(inner) => inner.next(),
            SingleTypeIsaIterator::Relation(inner) => inner.next(),
            SingleTypeIsaIterator::Attribute(inner) => inner.next(),
        }
    }
}

type MultipleTypeIsaObjectIterator = Flatten<AsLendingIterator<vec::IntoIter<InstanceIterator<Object<'static>>>>>;
type MultipleTypeIsaAttributeIterator =
    Flatten<AsLendingIterator<vec::IntoIter<AttributeIterator<InstanceIterator<Attribute<'static>>>>>>;

pub(super) type MultipleTypeIsaIterator = Chain<
    MapToThing<MultipleTypeIsaObjectIterator, ObjectEraseFn>,
    MapToThing<MultipleTypeIsaAttributeIterator, AttributeEraseFn>,
>;

pub(super) type IsaTupleIterator<I> =
    Map<TryFilter<I, Box<IsaFilterFn>, AsHkt![Thing<'_>], ConceptReadError>, ThingToTupleFn, AsHkt![TupleResult<'_>]>;

pub(super) type IsaUnboundedSortedTypeSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(super) type IsaUnboundedSortedTypeMerged = IsaTupleIterator<MultipleTypeIsaIterator>;

pub(super) type IsaUnboundedSortedThingSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(super) type IsaUnboundedSortedThingMerged = IsaTupleIterator<MultipleTypeIsaIterator>;

pub(super) type IsaBoundedSortedType =
    IsaTupleIterator<lending_iterator::Once<Result<AsHkt![Thing<'_>], ConceptReadError>>>;

type ObjectEraseFn = for<'a> fn(Result<Object<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type EntityEraseFn = for<'a> fn(Result<Entity<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type RelationEraseFn = for<'a> fn(Result<Relation<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type AttributeEraseFn = for<'a> fn(Result<Attribute<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;

type ThingToTupleFn = for<'a> fn(Result<Thing<'a>, ConceptReadError>) -> TupleResult<'a>;

pub(super) type IsaFilterFn = FilterFn<AsHkt![Thing<'_>]>;

type IsaVariableValueExtractor = for<'a, 'b> fn(&'a Thing<'b>) -> VariableValue<'a>;

pub(super) const EXTRACT_THING: IsaVariableValueExtractor = |thing| VariableValue::Thing(thing.as_reference());
pub(super) const EXTRACT_TYPE: IsaVariableValueExtractor = |thing| VariableValue::Type(thing.type_());

impl IsaExecutor {
    pub(crate) fn new(
        isa: IsaInstruction<VariablePosition>,
        variable_modes: VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> Self {
        let types = isa.types().clone();
        debug_assert!(types.len() > 0);

        let IsaInstruction { isa, checks, .. } = isa;

        let iterate_mode = BinaryIterateMode::new(isa.thing(), isa.type_(), &variable_modes, sort_by);
        let checker = Checker::<Thing<'_>> {
            checks,
            extractors: HashMap::from([(isa.thing(), EXTRACT_THING), (isa.type_(), EXTRACT_TYPE)]),
            _phantom_data: PhantomData,
        };

        Self { isa, iterate_mode, variable_modes, types, checker }
    }

    pub(crate) fn get_iterator(
        &self,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter_for_row = self.checker.filter_for_row(snapshot, thing_manager, &row);
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let positions = TuplePositions::Pair([self.isa.thing(), self.isa.type_()]);
                if self.types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.types.iter().next().unwrap();
                    let iterator = instances_of_single_type(type_, thing_manager, &**snapshot)?;
                    let as_tuples: IsaUnboundedSortedThingSingle = iterator
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaUnboundedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.types, thing_manager, &**snapshot)?;
                    let as_tuples: IsaUnboundedSortedThingMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_thing_type);
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
                    let iterator = instances_of_single_type(type_, thing_manager, &**snapshot)?;
                    let as_tuples: IsaUnboundedSortedTypeSingle = iterator
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter = instances_of_all_types_chained(&self.types, thing_manager, &**snapshot)?;
                    let as_tuples: IsaUnboundedSortedTypeMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                        .map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaUnboundedInvertedMerged(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                }
            }

            BinaryIterateMode::BoundFrom => {
                debug_assert!(row.len() > self.isa.thing().as_usize());
                let positions = TuplePositions::Pair([self.isa.type_(), self.isa.thing()]);
                let VariableValue::Thing(thing) = row.get(self.isa.thing()).to_owned() else {
                    unreachable!("Has thing must be an entity or relation.")
                };
                let as_tuples: IsaBoundedSortedType = lending_iterator::once::<Result<Thing<'_>, _>>(Ok(thing))
                    .try_filter::<_, IsaFilterFn, Thing<'_>, _>(filter_for_row)
                    .map(isa_to_tuple_thing_type);
                Ok(TupleIterator::IsaBounded(SortedTupleIterator::new(as_tuples, positions, &self.variable_modes)))
            }
        }
    }
}

pub(super) fn instances_of_all_types_chained(
    thing_types: &HashSet<Type>,
    thing_manager: &ThingManager,
    snapshot: &impl ReadableSnapshot,
) -> Result<MultipleTypeIsaIterator, ConceptReadError> {
    let (attribute_types, object_types) =
        thing_types.iter().cloned().partition::<Vec<_>, _>(|type_| matches!(type_, Type::Attribute(_)));
    let object_iters = object_types
        .into_iter()
        .map(|type_| thing_manager.get_objects_in(snapshot, type_.as_object_type()))
        .collect_vec();
    let object_iter: Map<MultipleTypeIsaObjectIterator, ObjectEraseFn, Result<Thing<'_>, ConceptReadError>> =
        AsLendingIterator::new(object_iters).flatten().map(|res| res.map(Thing::from));
    let attribute_iters: Vec<AttributeIterator<InstanceIterator<Attribute<'_>>>> = attribute_types
        .into_iter()
        .map(|type_| thing_manager.get_attributes_in(snapshot, type_.as_attribute_type()))
        .try_collect()?;
    let attribute_iter: Map<
        MultipleTypeIsaAttributeIterator,
        AttributeEraseFn,
        Result<Thing<'static>, ConceptReadError>,
    > = AsLendingIterator::new(attribute_iters).flatten().map(|res| res.map(Thing::Attribute));
    let thing_iter: MultipleTypeIsaIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}

pub(super) fn instances_of_single_type(
    type_: &Type,
    thing_manager: &ThingManager,
    snapshot: &impl ReadableSnapshot,
) -> Result<SingleTypeIsaIterator, ConceptReadError> {
    match type_ {
        Type::Entity(entity_type) => Ok(SingleTypeIsaIterator::Entity(
            thing_manager.get_entities_in(snapshot, entity_type.clone()).map(|res| res.map(Thing::Entity)),
        )),
        Type::Relation(relation_type) => Ok(SingleTypeIsaIterator::Relation(
            thing_manager.get_relations_in(snapshot, relation_type.clone()).map(|res| res.map(Thing::Relation)),
        )),
        Type::Attribute(attribute_type) => Ok(SingleTypeIsaIterator::Attribute(
            thing_manager.get_attributes_in(snapshot, attribute_type.clone())?.map(|res| res.map(Thing::Attribute)),
        )),
        Type::RoleType(_) => unreachable!("Cannot get instances of role types."),
    }
}
