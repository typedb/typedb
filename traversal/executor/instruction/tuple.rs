/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::ops::Range;
use answer::variable_value::VariableValue;
use concept::error::ConceptReadError;
use concept::thing::attribute::Attribute;
use concept::thing::entity::Entity;
use concept::thing::has::Has;
use concept::thing::relation::Relation;
use lending_iterator::higher_order::Hkt;
use crate::executor::instruction::{VariableMode, VariableModes};
use crate::executor::Position;

#[derive(Debug, Clone)]
pub(crate) enum Tuple<'a> {
    Single([VariableValue<'a>; 1]),
    Pair([VariableValue<'a>; 2]),
    Triple([VariableValue<'a>; 3]),
    Quintuple([VariableValue<'a>; 5]),
    Arbitrary(), // TODO: unknown sized tuples, for functions
}

impl<'a> Tuple<'a> {
    pub(crate) fn values(&self) -> &[VariableValue<'a>] {
        match self {
            Tuple::Single(values) => values,
            Tuple::Pair(values) => values,
            Tuple::Triple(values) => values,
            Tuple::Quintuple(values) => values,
            Tuple::Arbitrary() => {
                todo!()
            }
        }
    }

    pub(crate) fn into_owned(self) -> Tuple<'static> {
        match self {
            Tuple::Single([value]) => Tuple::Single([value.into_owned()]),
            Tuple::Pair([value_1, value_2]) => {
                Tuple::Pair([value_1.into_owned(), value_2.into_owned()])
            }
            Tuple::Triple([value_1, value_2, value_3]) => {
                Tuple::Triple([value_1.into_owned(), value_2.into_owned(), value_3.into_owned()])
            }
            Tuple::Quintuple([value_1, value_2, value_3, value_4, value_5]) => {
                Tuple::Quintuple([
                    value_1.into_owned(),
                    value_2.into_owned(),
                    value_3.into_owned(),
                    value_4.into_owned(),
                    value_5.into_owned()
                ])
            }
            Tuple::Arbitrary() => {
                todo!()
            }
        }
    }
}

impl Hkt for Tuple<'static> {
    type HktSelf<'a> = Tuple<'a>;
}

pub(crate) enum TuplePositions {
    Single([Position; 1]),
    Pair([Position; 2]),
    Triple([Position; 3]),
    Quintuple([Position; 5]),
    Arbitrary(), // TODO: unknown sized tuples, for functions
}

impl TuplePositions {
    pub(crate) fn as_single(&self) -> &[Position; 1] {
        match self {
            Self::Single(positions) => positions,
            _ => unreachable!("Cannot read tuple as Single."),
        }
    }

    pub(crate) fn as_pair(&self) -> &[Position; 2] {
        match self {
            Self::Pair(positions) => positions,
            _ => unreachable!("Cannot read tuple as Pair."),
        }
    }

    pub(crate) fn as_triple(&self) -> &[Position; 3] {
        match self {
            Self::Triple(positions) => positions,
            _ => unreachable!("Cannot read tuple as Single."),
        }
    }

    pub(crate) fn as_quintuple(&self) -> &[Position; 5] {
        match self {
            Self::Quintuple(positions) => positions,
            _ => unreachable!("Cannot read tuple as Single."),
        }
    }

    pub(crate) fn as_arbitrary(&self) {
        todo!()
    }

    pub(crate) fn positions(&self) -> &[Position] {
        match self {
            TuplePositions::Single(positions) => positions,
            TuplePositions::Pair(positions) => positions,
            TuplePositions::Triple(positions) => positions,
            TuplePositions::Quintuple(positions) => positions,
            TuplePositions::Arbitrary() => {
                todo!()
            }
        }
    }
}

pub(crate) type TupleIndex = u16;

pub(crate) type TupleResult<'a> = Result<Tuple<'a>, ConceptReadError>;

pub(crate) fn enumerated_range(
    variable_modes: &VariableModes,
    positions: &TuplePositions,
) -> Range<TupleIndex> {
    let mut last_enumerated = None;
    for (i, position) in positions.positions().iter().enumerate() {
        match variable_modes.get(*position).unwrap() {
            VariableMode::BoundSelect | VariableMode::UnboundSelect => {
                last_enumerated = Some(i as TupleIndex);
            }
            VariableMode::UnboundCount => {}
            VariableMode::UnboundCheck => {}
        }
    }
    last_enumerated.map_or(0..0, |last| 0..last + 1)
}

pub(crate) fn enumerated_or_counted_range(
    variable_modes: &VariableModes,
    positions: &TuplePositions,
) -> Range<TupleIndex> {
    let mut last_enumerated_or_counted = None;
    for (i, position) in positions.positions().iter().enumerate() {
        match variable_modes.get(*position).unwrap() {
            VariableMode::BoundSelect | VariableMode::UnboundSelect | VariableMode::UnboundCount => {
                last_enumerated_or_counted = Some(i as TupleIndex)
            }
            VariableMode::UnboundCheck => {}
        }
    }
    last_enumerated_or_counted.map_or(0..0, |last| 0..last + 1)
}


pub(crate) fn isa_entity_to_tuple_thing_type<'a>(result: Result<Entity<'a>, ConceptReadError>) -> TupleResult<'a> {
    match result {
        Ok(entity) => {
            let type_ = entity.type_();
            Ok(Tuple::Pair([VariableValue::Thing(entity.into()), VariableValue::Type(type_.into())]))
        },
        Err(err) => Err(err)
    }
}
pub(crate) fn isa_relation_to_tuple_thing_type<'a>(result: Result<Relation<'a>, ConceptReadError>) -> TupleResult<'a> {
    match result {
        Ok(relation) => {
            let type_ = relation.type_();
            Ok(Tuple::Pair([VariableValue::Thing(relation.into()), VariableValue::Type(type_.into())]))
        },
        Err(err) => Err(err)
    }
}

pub(crate) fn isa_attribute_to_tuple_thing_type<'a>(result: Result<Attribute<'a>, ConceptReadError>) -> TupleResult<'a> {
    match result {
        Ok(attribute) =>{
            let type_ = attribute.type_();
            Ok(Tuple::Pair([VariableValue::Thing(attribute.into()), VariableValue::Type(type_.into())]))
        }
        Err(err) => Err(err)
    }
}

pub(crate) fn has_to_tuple_owner_attribute<'a>(
    result: Result<(Has<'a>, u64), ConceptReadError>
) -> TupleResult<'a> {
    match result {
        Ok((has, count)) => {
            let (owner, attribute) = has.into_owner_attribute();
            Ok(Tuple::Pair([
                VariableValue::Thing(owner.into()),
                VariableValue::Thing(attribute.into()),
            ]))
        }
        Err(err) => Err(err)
    }
}

pub(crate) fn has_to_tuple_attribute_owner<'a>(
    result: Result<(Has<'a>, u64), ConceptReadError>
) -> TupleResult<'a> {
    match result {
        Ok((has, count)) => {
            let (owner, attribute) = has.into_owner_attribute();
            Ok(Tuple::Pair([
                VariableValue::Thing(attribute.into()),
                VariableValue::Thing(owner.into()),
            ]))
        }
        Err(err) => Err(err)
    }
}
