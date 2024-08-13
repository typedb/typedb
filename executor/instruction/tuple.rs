/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable_value::VariableValue;
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        entity::Entity,
        has::Has,
        relation::{Relation, RolePlayer},
    },
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

pub(crate) fn isa_entity_to_tuple_thing_type(result: Result<Entity<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(entity) => {
            let type_ = entity.type_();
            Ok(Tuple::Pair([VariableValue::Thing(entity.into()), VariableValue::Type(type_.into())]))
        }
        Err(err) => Err(err),
    }
}
pub(crate) fn isa_relation_to_tuple_thing_type(result: Result<Relation<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(relation) => {
            let type_ = relation.type_();
            Ok(Tuple::Pair([VariableValue::Thing(relation.into()), VariableValue::Type(type_.into())]))
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn isa_attribute_to_tuple_thing_type(result: Result<Attribute<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(attribute) => {
            let type_ = attribute.type_();
            Ok(Tuple::Pair([VariableValue::Thing(attribute.into()), VariableValue::Type(type_.into())]))
        }
        Err(err) => Err(err),
    }
}
pub(crate) type HasToTupleFn = for<'a> fn(Result<(Has<'a>, u64), ConceptReadError>) -> TupleResult<'a>;

pub(crate) fn has_to_tuple_owner_attribute(result: Result<(Has<'_>, u64), ConceptReadError>) -> TupleResult<'_> {
    let (has, count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(owner.into()), VariableValue::Thing(attribute.into())]))
}

pub(crate) fn has_to_tuple_attribute_owner(result: Result<(Has<'_>, u64), ConceptReadError>) -> TupleResult<'_> {
    let (has, count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(attribute.into()), VariableValue::Thing(owner.into())]))
}

pub(crate) type LinksToTupleFn =
    for<'a> fn(Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>) -> TupleResult<'a>;

pub(crate) fn links_to_tuple_relation_player_role<'a>(
    result: Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>,
) -> TupleResult<'a> {
    let (rel, rp, count) = result?;
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
    let (rel, rp, count) = result?;
    let role_type = rp.role_type();
    Ok(Tuple::Triple([
        VariableValue::Thing(rp.into_player().into()),
        VariableValue::Thing(rel.into()),
        VariableValue::Type(role_type.into()),
    ]))
}
