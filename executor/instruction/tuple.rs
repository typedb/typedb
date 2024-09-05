/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::{variable_value::VariableValue, Thing, Type};
use concept::{
    error::ConceptReadError,
    thing::{
        has::Has,
        relation::{Relation, RolePlayer},
    },
    type_::{owns::Owns, plays::Plays, relates::Relates},
};
use lending_iterator::higher_order::Hkt;

use crate::VariablePosition;

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
            Tuple::Arbitrary() => todo!(),
        }
    }

    pub(crate) fn into_owned(self) -> Tuple<'static> {
        match self {
            Tuple::Single(values) => Tuple::Single(values.map(VariableValue::into_owned)),
            Tuple::Pair(values) => Tuple::Pair(values.map(VariableValue::into_owned)),
            Tuple::Triple(values) => Tuple::Triple(values.map(VariableValue::into_owned)),
            Tuple::Quintuple(values) => Tuple::Quintuple(values.map(VariableValue::into_owned)),
            Tuple::Arbitrary() => todo!(),
        }
    }
}

impl Hkt for Tuple<'static> {
    type HktSelf<'a> = Tuple<'a>;
}

#[derive(Debug, Clone)]
pub(crate) enum TuplePositions {
    Single([VariablePosition; 1]),
    Pair([VariablePosition; 2]),
    Triple([VariablePosition; 3]),
    Quintuple([VariablePosition; 5]),
    Arbitrary(), // TODO: unknown sized tuples, for functions
}

impl TuplePositions {
    pub(crate) fn as_single(&self) -> &[VariablePosition; 1] {
        match self {
            Self::Single(positions) => positions,
            _ => unreachable!("Cannot read tuple as Single."),
        }
    }

    pub(crate) fn as_pair(&self) -> &[VariablePosition; 2] {
        match self {
            Self::Pair(positions) => positions,
            _ => unreachable!("Cannot read tuple as Pair."),
        }
    }

    pub(crate) fn as_triple(&self) -> &[VariablePosition; 3] {
        match self {
            Self::Triple(positions) => positions,
            _ => unreachable!("Cannot read tuple as Triple."),
        }
    }

    pub(crate) fn as_quintuple(&self) -> &[VariablePosition; 5] {
        match self {
            Self::Quintuple(positions) => positions,
            _ => unreachable!("Cannot read tuple as Quintuple."),
        }
    }

    pub(crate) fn as_arbitrary(&self) {
        todo!()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &VariablePosition> {
        self.positions().iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.positions().len()
    }

    pub(crate) fn positions(&self) -> &[VariablePosition] {
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

pub(crate) type TypeToTupleFn = fn(Type) -> TupleResult<'static>;

pub(crate) fn type_to_tuple(type_: Type) -> TupleResult<'static> {
    Ok(Tuple::Single([VariableValue::Type(type_)]))
}

pub(crate) type SubToTupleFn = fn(Result<(Type, Type), ConceptReadError>) -> TupleResult<'static>;

pub(crate) fn sub_to_tuple_sub_super(result: Result<(Type, Type), ConceptReadError>) -> TupleResult<'static> {
    match result {
        Ok((sub, sup)) => Ok(Tuple::Pair([VariableValue::Type(sub), VariableValue::Type(sup)])),
        Err(err) => Err(err),
    }
}

pub(crate) fn sub_to_tuple_super_sub(result: Result<(Type, Type), ConceptReadError>) -> TupleResult<'static> {
    match result {
        Ok((sub, sup)) => Ok(Tuple::Pair([VariableValue::Type(sup), VariableValue::Type(sub)])),
        Err(err) => Err(err),
    }
}

pub(crate) type OwnsToTupleFn = fn(Result<Owns<'static>, ConceptReadError>) -> TupleResult<'static>;

pub(crate) fn owns_to_tuple_owner_attribute(result: Result<Owns<'static>, ConceptReadError>) -> TupleResult<'static> {
    match result {
        Ok(owns) => Ok(Tuple::Pair(
            [Type::from(owns.owner().into_owned()), Type::Attribute(owns.attribute().to_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) fn owns_to_tuple_attribute_owner(result: Result<Owns<'static>, ConceptReadError>) -> TupleResult<'static> {
    match result {
        Ok(owns) => Ok(Tuple::Pair(
            [Type::Attribute(owns.attribute().to_owned()), Type::from(owns.owner().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) type RelatesToTupleFn = fn(Result<Relates<'static>, ConceptReadError>) -> TupleResult<'static>;

pub(crate) fn relates_to_tuple_relation_role(
    result: Result<Relates<'static>, ConceptReadError>,
) -> TupleResult<'static> {
    match result {
        Ok(relates) => Ok(Tuple::Pair(
            [Type::Relation(relates.relation().into_owned()), Type::RoleType(relates.role().to_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) fn relates_to_tuple_role_relation(
    result: Result<Relates<'static>, ConceptReadError>,
) -> TupleResult<'static> {
    match result {
        Ok(relates) => Ok(Tuple::Pair(
            [Type::RoleType(relates.role().to_owned()), Type::Relation(relates.relation().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) type PlaysToTupleFn = fn(Result<Plays<'static>, ConceptReadError>) -> TupleResult<'static>;

pub(crate) fn plays_to_tuple_player_role(result: Result<Plays<'static>, ConceptReadError>) -> TupleResult<'static> {
    match result {
        Ok(plays) => Ok(Tuple::Pair(
            [Type::from(plays.player().into_owned()), Type::RoleType(plays.role().to_owned())].map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) fn plays_to_tuple_role_player(result: Result<Plays<'static>, ConceptReadError>) -> TupleResult<'static> {
    match result {
        Ok(plays) => Ok(Tuple::Pair(
            [Type::RoleType(plays.role().to_owned()), Type::from(plays.player().into_owned())].map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) type ThingToTupleFn = for<'a> fn(Result<Thing<'a>, ConceptReadError>) -> TupleResult<'a>;

pub(crate) fn isa_to_tuple_thing_type(result: Result<Thing<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(thing) => {
            let type_ = thing.type_();
            Ok(Tuple::Pair([VariableValue::Thing(thing), VariableValue::Type(type_)]))
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn isa_to_tuple_type_thing(result: Result<Thing<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(thing) => Ok(Tuple::Pair([VariableValue::Type(thing.type_()), VariableValue::Thing(thing)])),
        Err(err) => Err(err),
    }
}

pub(crate) type HasToTupleFn = for<'a> fn(Result<(Has<'a>, u64), ConceptReadError>) -> TupleResult<'a>;

pub(crate) fn has_to_tuple_owner_attribute(result: Result<(Has<'_>, u64), ConceptReadError>) -> TupleResult<'_> {
    let (has, _count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(owner.into()), VariableValue::Thing(attribute.into())]))
}

pub(crate) fn has_to_tuple_attribute_owner(result: Result<(Has<'_>, u64), ConceptReadError>) -> TupleResult<'_> {
    let (has, _count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(attribute.into()), VariableValue::Thing(owner.into())]))
}

pub(crate) type LinksToTupleFn =
    for<'a> fn(Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>) -> TupleResult<'a>;

pub(crate) fn links_to_tuple_relation_player_role<'a>(
    result: Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>,
) -> TupleResult<'a> {
    let (rel, rp, _count) = result?;
    let role_type = rp.role_type();
    Ok(Tuple::Triple([
        VariableValue::Thing(rel.into()),
        VariableValue::Thing(rp.into_player().into()),
        VariableValue::Type(role_type.into()),
    ]))
}

pub(crate) fn links_to_tuple_player_relation_role<'a>(
    result: Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>,
) -> TupleResult<'a> {
    let (rel, rp, _count) = result?;
    let role_type = rp.role_type();
    Ok(Tuple::Triple([
        VariableValue::Thing(rp.into_player().into()),
        VariableValue::Thing(rel.into()),
        VariableValue::Type(role_type.into()),
    ]))
}
