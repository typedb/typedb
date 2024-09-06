/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    iter,
    marker::PhantomData,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::match_::instructions::thing::IsaInstruction;
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
use ir::pattern::constraint::{Isa, IsaKind, SubKind};
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Chain, Flatten, Map, RepeatEach, TryFilter, Zip},
    AsHkt, AsLendingIterator, LendingIterator, Once,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        sub_executor::get_supertypes,
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, IsaToTupleFn, TuplePositions, TupleResult},
        BinaryIterateMode, Checker, FilterFn, VariableModes,
    },
    row::MaybeOwnedRow,
    VariablePosition,
};

pub(crate) struct IsaExecutor {
    isa: Isa<VariablePosition>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    types: Arc<HashSet<Type>>,
    checker: Checker<(AsHkt![Thing<'_>], Type)>,
}

type MapToThing<I, F> = Map<I, F, Result<AsHkt![Thing<'_>], ConceptReadError>>;

#[allow(clippy::large_enum_variant)]
pub(crate) enum SingleTypeIsaIterator {
    Entity(ThingWithTypes<MapToThing<InstanceIterator<AsHkt![Entity<'_>]>, EntityEraseFn>>),
    Relation(ThingWithTypes<MapToThing<InstanceIterator<AsHkt![Relation<'_>]>, RelationEraseFn>>),
    Attribute(ThingWithTypes<MapToThing<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, AttributeEraseFn>>),
}

impl LendingIterator for SingleTypeIsaIterator {
    type Item<'a> = Result<(Thing<'a>, Type), ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            SingleTypeIsaIterator::Entity(inner) => inner.next(),
            SingleTypeIsaIterator::Relation(inner) => inner.next(),
            SingleTypeIsaIterator::Attribute(inner) => inner.next(),
        }
    }
}

type MultipleTypeIsaObjectIterator = Flatten<
    AsLendingIterator<vec::IntoIter<ThingWithTypes<MapToThing<InstanceIterator<AsHkt![Object<'_>]>, ObjectEraseFn>>>>,
>;
type MultipleTypeIsaAttributeIterator = Flatten<
    AsLendingIterator<
        vec::IntoIter<
            ThingWithTypes<MapToThing<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, AttributeEraseFn>>,
        >,
    >,
>;

pub(super) type MultipleTypeIsaIterator = Chain<MultipleTypeIsaObjectIterator, MultipleTypeIsaAttributeIterator>;

pub(super) type IsaTupleIterator<I> = Map<
    TryFilter<I, Box<IsaFilterFn>, (AsHkt![Thing<'_>], Type), ConceptReadError>,
    IsaToTupleFn,
    AsHkt![TupleResult<'_>],
>;

type RezipThingTypeFn =
    for<'a, 'b> fn((&'a Result<Thing<'b>, ConceptReadError>, Type)) -> Result<(Thing<'a>, Type), ConceptReadError>;

type ThingWithTypes<I> = Map<
    Zip<RepeatEach<I>, AsLendingIterator<iter::Cycle<vec::IntoIter<Type>>>>,
    RezipThingTypeFn,
    Result<(AsHkt![Thing<'_>], Type), ConceptReadError>,
>;

pub(super) type IsaUnboundedSortedTypeSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(super) type IsaUnboundedSortedTypeMerged = IsaTupleIterator<MultipleTypeIsaIterator>;

pub(super) type IsaUnboundedSortedThingSingle = IsaTupleIterator<SingleTypeIsaIterator>;
pub(super) type IsaUnboundedSortedThingMerged = IsaTupleIterator<MultipleTypeIsaIterator>;

pub(super) type IsaBoundedSortedType =
    IsaTupleIterator<ThingWithTypes<Once<Result<AsHkt![Thing<'_>], ConceptReadError>>>>;

type ObjectEraseFn = for<'a> fn(Result<Object<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type EntityEraseFn = for<'a> fn(Result<Entity<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type RelationEraseFn = for<'a> fn(Result<Relation<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
type AttributeEraseFn = for<'a> fn(Result<Attribute<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;

pub(super) type IsaFilterFn = FilterFn<(AsHkt![Thing<'_>], Type)>;

type IsaVariableValueExtractor = for<'a, 'b> fn(&'a (Thing<'b>, Type)) -> VariableValue<'a>;

pub(super) const EXTRACT_THING: IsaVariableValueExtractor = |(thing, _)| VariableValue::Thing(thing.as_reference());
pub(super) const EXTRACT_TYPE: IsaVariableValueExtractor = |(_, type_)| VariableValue::Type(type_.clone());

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
        let checker = Checker::<(Thing<'_>, Type)> {
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
        let filter_for_row: Box<IsaFilterFn> = self.checker.filter_for_row(snapshot, thing_manager, &row);
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let positions = TuplePositions::Pair([self.isa.thing(), self.isa.type_()]);
                if self.types.len() == 1 {
                    // no heap allocs needed if there is only 1 iterator
                    let type_ = self.types.iter().next().unwrap();
                    let iterator = instances_of_single_type(&**snapshot, thing_manager, type_, self.isa.isa_kind())?;
                    let as_tuples: IsaUnboundedSortedThingSingle = iterator
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                        .map(isa_to_tuple_thing_type);
                    Ok(TupleIterator::IsaUnboundedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter =
                        instances_of_all_types_chained(&**snapshot, thing_manager, &*self.types, self.isa.isa_kind())?;
                    let as_tuples: IsaUnboundedSortedThingMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
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
                    let iterator = instances_of_single_type(&**snapshot, thing_manager, type_, self.isa.isa_kind())?;
                    let as_tuples: IsaUnboundedSortedTypeSingle = iterator
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                        .map(isa_to_tuple_type_thing);
                    Ok(TupleIterator::IsaUnboundedInvertedSingle(SortedTupleIterator::new(
                        as_tuples,
                        positions,
                        &self.variable_modes,
                    )))
                } else {
                    let thing_iter =
                        instances_of_all_types_chained(&**snapshot, thing_manager, &*self.types, self.isa.isa_kind())?;
                    let as_tuples: IsaUnboundedSortedTypeMerged = thing_iter
                        .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
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
                let type_ = thing.type_();
                let type_manager = thing_manager.type_manager();
                let types = match self.isa.isa_kind() {
                    IsaKind::Exact => vec![type_.clone()],
                    IsaKind::Subtype => get_supertypes(&**snapshot, type_manager, &type_, SubKind::Subtype)?,
                };
                let as_tuples: IsaBoundedSortedType = with_types(lending_iterator::once(Ok(thing)), types)
                    .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                    .map(isa_to_tuple_thing_type);
                Ok(TupleIterator::IsaBounded(SortedTupleIterator::new(as_tuples, positions, &self.variable_modes)))
            }
        }
    }
}

fn with_types<I: for<'a> LendingIterator<Item<'a> = Result<Thing<'a>, ConceptReadError>>>(
    iter: I,
    types: Vec<Type>,
) -> ThingWithTypes<I> {
    iter.repeat_each(types.len()).zip(AsLendingIterator::new(types.into_iter().cycle())).map(|(thing_res, ty)| {
        match thing_res {
            Ok(thing) => Ok((thing.as_reference(), ty)),
            Err(err) => Err(err.clone()),
        }
    })
}

pub(super) fn instances_of_all_types_chained<'a>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    thing_types: impl IntoIterator<Item = &'a Type>,
    isa_kind: IsaKind,
) -> Result<MultipleTypeIsaIterator, ConceptReadError> {
    let type_manager = thing_manager.type_manager();
    let thing_types: Vec<_> = thing_types
        .into_iter()
        .cloned()
        .map(|type_| {
            let types = match isa_kind {
                IsaKind::Exact => vec![type_.clone()],
                IsaKind::Subtype => get_supertypes(snapshot, type_manager, &type_, SubKind::Subtype)?,
            };
            Ok((type_, types))
        })
        .try_collect()?;

    let (attribute_types, object_types) =
        thing_types.into_iter().partition::<Vec<_>, _>(|(type_, _)| matches!(type_, Type::Attribute(_)));

    let object_iters: Vec<_> = object_types
        .into_iter()
        .map(|(type_, types)| {
            Ok(with_types(
                thing_manager
                    .get_objects_in(snapshot, type_.as_object_type())
                    .map((|res| res.map(Thing::from)) as ObjectEraseFn),
                types,
            ))
        })
        .try_collect()?;
    let object_iter: MultipleTypeIsaObjectIterator = AsLendingIterator::new(object_iters).flatten();

    let attribute_iters: Vec<_> = attribute_types
        .into_iter()
        .map(|(type_, types)| {
            Ok(with_types(
                thing_manager
                    .get_attributes_in(snapshot, type_.as_attribute_type())?
                    .map((|res| res.map(Thing::Attribute)) as AttributeEraseFn),
                types,
            ))
        })
        .try_collect()?;
    let attribute_iter: MultipleTypeIsaAttributeIterator = AsLendingIterator::new(attribute_iters).flatten();

    let thing_iter: MultipleTypeIsaIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}

pub(super) fn instances_of_single_type(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    type_: &Type,
    isa_kind: IsaKind,
) -> Result<SingleTypeIsaIterator, ConceptReadError> {
    let type_manager = thing_manager.type_manager();
    let types = match isa_kind {
        IsaKind::Exact => vec![type_.clone()],
        IsaKind::Subtype => get_supertypes(snapshot, type_manager, type_, SubKind::Subtype)?,
    };

    match type_ {
        Type::Entity(entity_type) => Ok(SingleTypeIsaIterator::Entity(with_types(
            thing_manager.get_entities_in(snapshot, entity_type.clone()).map(|res| res.map(Thing::Entity)),
            types,
        ))),
        Type::Relation(relation_type) => Ok(SingleTypeIsaIterator::Relation(with_types(
            thing_manager.get_relations_in(snapshot, relation_type.clone()).map(|res| res.map(Thing::Relation)),
            types,
        ))),
        Type::Attribute(attribute_type) => Ok(SingleTypeIsaIterator::Attribute(with_types(
            thing_manager.get_attributes_in(snapshot, attribute_type.clone())?.map(|res| res.map(Thing::Attribute)),
            types,
        ))),
        Type::RoleType(_) => unreachable!("Cannot get instances of role types."),
    }
}
