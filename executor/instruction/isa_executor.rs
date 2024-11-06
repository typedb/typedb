/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, iter, ops::Bound, sync::Arc, vec};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{executable::match_::instructions::thing::IsaInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    iterator::InstanceIterator,
    thing::{
        attribute::{Attribute, AttributeIterator},
        entity::Entity,
        object::Object,
        relation::Relation,
        thing_manager::ThingManager,
        ThingAPI,
    },
};
use encoding::{
    graph::{
        thing::{vertex_attribute::AttributeVertex, ThingVertex},
        type_::vertex::TypeVertexEncoding,
        Typed,
    },
    value::value::Value,
};
use ir::pattern::{
    constraint::{Isa, IsaKind},
    Vertex,
};
use itertools::Itertools;
use lending_iterator::{
    adaptors::{Chain, Flatten, Map, RepeatEach, TryFilter, Zip},
    AsHkt, AsLendingIterator, LendingIterator, Once,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, IsaToTupleFn, TuplePositions, TupleResult},
        BinaryIterateMode, Checker, FilterFn, VariableModes, TYPES_EMPTY,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct IsaExecutor {
    isa: Isa<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    instance_type_to_types: Arc<BTreeMap<Type, Vec<Type>>>,
    checker: Checker<(AsHkt![Thing<'_>], Type)>,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum SingleTypeIsaIterator {
    Entity(MapToThingType<InstanceIterator<AsHkt![Entity<'_>]>, EntityToThingTypeFn>),
    Relation(MapToThingType<InstanceIterator<AsHkt![Relation<'_>]>, RelationToThingTypeFn>),
    Attribute(MapToThingType<AttributeIterator<InstanceIterator<AsHkt![Attribute<'_>]>>, AttributeToThingTypeFn>),
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

type MapToThingType<I, F> = Map<I, F, Result<(AsHkt![Thing<'_>], Type), ConceptReadError>>;
type EntityToThingTypeFn =
    for<'a> fn(Result<Entity<'a>, ConceptReadError>) -> Result<(Thing<'a>, Type), ConceptReadError>;
type RelationToThingTypeFn =
    for<'a> fn(Result<Relation<'a>, ConceptReadError>) -> Result<(Thing<'a>, Type), ConceptReadError>;
type AttributeToThingTypeFn =
    for<'a> fn(Result<Attribute<'a>, ConceptReadError>) -> Result<(Thing<'a>, Type), ConceptReadError>;

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

pub(super) type MapToThing<I, F> = Map<I, F, Result<AsHkt![Thing<'_>], ConceptReadError>>;
pub(super) type ObjectEraseFn = for<'a> fn(Result<Object<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;
pub(super) type AttributeEraseFn =
    for<'a> fn(Result<Attribute<'a>, ConceptReadError>) -> Result<Thing<'a>, ConceptReadError>;

pub(super) type IsaFilterFn = FilterFn<(AsHkt![Thing<'_>], Type)>;

type IsaVariableValueExtractor = for<'a, 'b> fn(&'a (Thing<'b>, Type)) -> VariableValue<'a>;

pub(super) const EXTRACT_THING: IsaVariableValueExtractor = |(thing, _)| VariableValue::Thing(thing.as_reference());
pub(super) const EXTRACT_TYPE: IsaVariableValueExtractor = |(_, type_)| VariableValue::Type(type_.clone());

impl IsaExecutor {
    pub(crate) fn new(
        isa: IsaInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let IsaInstruction { isa, checks, instance_type_to_types, .. } = isa;
        debug_assert!(instance_type_to_types.len() > 0);
        let iterate_mode = BinaryIterateMode::new(isa.thing(), isa.type_(), &variable_modes, sort_by);

        let thing = isa.thing().as_variable();
        let type_ = isa.type_().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([thing, type_]),
            _ => TuplePositions::Pair([type_, thing]),
        };

        let checker = Checker::<(Thing<'_>, Type)>::new(
            checks,
            [(thing, EXTRACT_THING), (type_, EXTRACT_TYPE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            isa,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            instance_type_to_types,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        let filter_for_row: Box<IsaFilterFn> = self.checker.filter_for_row(context, &row);
        let snapshot = &**context.snapshot();
        let thing_manager = context.thing_manager();
        match self.iterate_mode {
            BinaryIterateMode::Unbound => {
                let instances_range = if let Vertex::Variable(thing_variable) = self.isa.thing() {
                    self.checker.value_range_for(context, Some(row), *thing_variable)?
                } else {
                    (Bound::Unbounded, Bound::Unbounded)
                };
                let thing_iter = instances_of_all_types_chained(
                    snapshot,
                    thing_manager,
                    self.instance_type_to_types.as_ref(),
                    self.isa.isa_kind(),
                    instances_range,
                )?;
                let as_tuples: IsaUnboundedSortedThingMerged = thing_iter
                    .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                    .map(isa_to_tuple_thing_type);
                Ok(TupleIterator::IsaUnboundedMerged(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
            }
            BinaryIterateMode::UnboundInverted => {
                unreachable!()
            }
            BinaryIterateMode::BoundFrom => {
                let thing = self.isa.thing().as_variable().unwrap().as_position().unwrap();
                debug_assert!(row.len() > thing.as_usize());
                let VariableValue::Thing(thing) = row.get(thing).to_owned() else {
                    unreachable!("Has thing must be an entity or relation.")
                };
                let type_ = thing.type_();
                let supertypes = self.instance_type_to_types.get(&type_).cloned().unwrap_or(TYPES_EMPTY.clone());
                let as_tuples: IsaBoundedSortedType = with_types(lending_iterator::once(Ok(thing)), supertypes)
                    .try_filter::<_, IsaFilterFn, (Thing<'_>, Type), _>(filter_for_row)
                    .map(isa_to_tuple_type_thing);
                Ok(TupleIterator::IsaBounded(SortedTupleIterator::new(
                    as_tuples,
                    self.tuple_positions.clone(),
                    &self.variable_modes,
                )))
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
    instance_types_to_types: &BTreeMap<Type, Vec<Type>>,
    isa_kind: IsaKind,
    instance_values_range: (Bound<Value<'_>>, Bound<Value<'_>>),
) -> Result<MultipleTypeIsaIterator, ConceptReadError> {
    // TODO: this method contains a lot of heap allocations - we clone the Vec<Type> each time!

    // object types and attribute types will continue to be sorted, based on their source in the BTreeMap
    let (attribute_types, object_types) =
        instance_types_to_types.into_iter().partition::<Vec<_>, _>(|(type_, _)| matches!(type_, Type::Attribute(_)));

    let object_iters: Vec<_> = object_types
        .into_iter()
        .map(|(type_, types)| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) { types.clone() } else { vec![type_.clone()] };
            Ok(with_types(
                thing_manager
                    .get_objects_in(snapshot, type_.as_object_type())
                    .map((|res| res.map(Thing::from)) as ObjectEraseFn),
                returned_types,
            ))
        })
        .try_collect()?;
    // Since the object types are sorted, and instance ordering follows matches type ordering, we have instance-sorting here
    let object_iter: MultipleTypeIsaObjectIterator = AsLendingIterator::new(object_iters).flatten();

    // TODO: don't unwrap inside the operators
    let type_manager = thing_manager.type_manager();
    let attribute_iters: Vec<_> = attribute_types
        .into_iter()
        .filter(|(type_, _)| type_.as_attribute_type().get_value_type(snapshot, type_manager).unwrap().is_some())
        .sorted_by_key(|(type_, _)| {
            // we manually have to sort for now, since the instance-sorting does not equal the type ordering
            AttributeVertex::build_prefix_type(
                AttributeVertex::value_type_category_to_prefix_type(
                    type_.as_attribute_type().get_value_type(snapshot, type_manager).unwrap().unwrap().0.category(),
                ),
                type_.as_attribute_type().vertex().type_id_(),
            )
        })
        .map(|(type_, types)| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) { types.clone() } else { vec![type_.clone()] };
            Ok(with_types(
                thing_manager
                    .get_attributes_in_range(snapshot, type_.as_attribute_type(), &instance_values_range)?
                    .map((|res| res.map(Thing::Attribute)) as AttributeEraseFn),
                returned_types,
            ))
        })
        .try_collect()?;
    let attribute_iter: MultipleTypeIsaAttributeIterator = AsLendingIterator::new(attribute_iters).flatten();

    let thing_iter: MultipleTypeIsaIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}
