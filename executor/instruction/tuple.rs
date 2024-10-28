/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::ExecutorVariable;
use concept::{
    error::ConceptReadError,
    thing::{
        has::Has,
        relation::{Relation, RolePlayer},
    },
    type_::{owns::Owns, plays::Plays, relates::Relates},
};
use concept::type_::attribute_type::AttributeType;
use concept::type_::object_type::ObjectType;
use lending_iterator::higher_order::Hkt;

#[derive(Debug, Clone)]
pub(crate) enum Tuple<'a> {
    Single([VariableValue<'a>; 1]),
    Pair([VariableValue<'a>; 2]),
    Triple([VariableValue<'a>; 3]),
    Quintuple([VariableValue<'a>; 5]),
    Arbitrary(), // TODO: unknown sized tuples, for functions
}

impl<'a, 'b> PartialEq<Tuple<'b>> for Tuple<'a> {
    fn eq(&self, other: &Tuple<'b>) -> bool {
        match (self, other) {
            (Tuple::Single(this), Tuple::Single(that)) => this.eq(that),
            (Tuple::Pair(this), Tuple::Pair(that)) => this.eq(that),
            (Tuple::Triple(this), Tuple::Triple(that)) => this.eq(that),
            (Tuple::Quintuple(this), Tuple::Quintuple(that)) => this.eq(that),
            _ => false,
        }
    }
}

impl<'a, 'b> PartialOrd<Tuple<'b>> for Tuple<'a> {
    fn partial_cmp(&self, other: &Tuple<'b>) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Tuple::Single(this), Tuple::Single(that)) => this.partial_cmp(that),
            (Tuple::Pair(this), Tuple::Pair(that)) => this.partial_cmp(that),
            (Tuple::Triple(this), Tuple::Triple(that)) => this.partial_cmp(that),
            (Tuple::Quintuple(this), Tuple::Quintuple(that)) => this.partial_cmp(that),
            _ => None,
        }
    }
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
    Single([Option<ExecutorVariable>; 1]),
    Pair([Option<ExecutorVariable>; 2]),
    Triple([Option<ExecutorVariable>; 3]),
    Quintuple([Option<ExecutorVariable>; 5]),
    Arbitrary(), // TODO: unknown sized tuples, for functions
}

impl TuplePositions {
    pub(crate) fn as_single(&self) -> &[Option<ExecutorVariable>; 1] {
        match self {
            Self::Single(positions) => positions,
            _ => unreachable!("Cannot read tuple as Single."),
        }
    }

    pub(crate) fn as_pair(&self) -> &[Option<ExecutorVariable>; 2] {
        match self {
            Self::Pair(positions) => positions,
            _ => unreachable!("Cannot read tuple as Pair."),
        }
    }

    pub(crate) fn as_triple(&self) -> &[Option<ExecutorVariable>; 3] {
        match self {
            Self::Triple(positions) => positions,
            _ => unreachable!("Cannot read tuple as Triple."),
        }
    }

    pub(crate) fn as_quintuple(&self) -> &[Option<ExecutorVariable>; 5] {
        match self {
            Self::Quintuple(positions) => positions,
            _ => unreachable!("Cannot read tuple as Quintuple."),
        }
    }

    pub(crate) fn as_arbitrary(&self) {
        todo!()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = Option<ExecutorVariable>> + '_ {
        self.positions().iter().copied()
    }

    pub(crate) fn len(&self) -> usize {
        self.positions().len()
    }

    pub(crate) fn positions(&self) -> &[Option<ExecutorVariable>] {
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

pub(crate) type TypeToTupleFn = fn(Result<Type, ConceptReadError>) -> TupleResult<'static>;

pub(crate) fn type_to_tuple(type_: Result<Type, ConceptReadError>) -> TupleResult<'static> {
    Ok(Tuple::Single([VariableValue::Type(type_?)]))
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

pub(crate) type OwnsToTupleFn = for<'a> fn(Result<(ObjectType<'a>, AttributeType<'a>), ConceptReadError>) -> TupleResult<'a>;

pub(crate) fn owns_to_tuple_owner_attribute<'a>(result: Result<(ObjectType<'a>, AttributeType<'a>), ConceptReadError>) -> TupleResult<'a> {
    match result {
        Ok((owner, attribute)) => Ok(Tuple::Pair(
            [Type::from(owner.clone().into_owned()), Type::Attribute(attribute.clone().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) fn owns_to_tuple_attribute_owner<'a>(result: Result<(ObjectType<'a>, AttributeType<'a>), ConceptReadError>) -> TupleResult<'a> {
    match result {
        Ok((owner, attribute)) => Ok(Tuple::Pair(
            [Type::Attribute(attribute.clone().into_owned()), Type::from(owner.clone().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) type RelatesToTupleFn = fn(Result<Relates<'_>, ConceptReadError>) -> TupleResult<'_>;

pub(crate) fn relates_to_tuple_relation_role(result: Result<Relates<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(relates) => Ok(Tuple::Pair(
            [Type::Relation(relates.relation().into_owned()), Type::RoleType(relates.role().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) fn relates_to_tuple_role_relation(result: Result<Relates<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(relates) => Ok(Tuple::Pair(
            [Type::RoleType(relates.role().into_owned()), Type::Relation(relates.relation().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) type PlaysToTupleFn = fn(Result<Plays<'_>, ConceptReadError>) -> TupleResult<'_>;

pub(crate) fn plays_to_tuple_player_role(result: Result<Plays<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(plays) => Ok(Tuple::Pair(
            [Type::from(plays.player().into_owned()), Type::RoleType(plays.role().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) fn plays_to_tuple_role_player(result: Result<Plays<'_>, ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok(plays) => Ok(Tuple::Pair(
            [Type::RoleType(plays.role().into_owned()), Type::from(plays.player().into_owned())]
                .map(VariableValue::Type),
        )),
        Err(err) => Err(err),
    }
}

pub(crate) type IsaToTupleFn = for<'a> fn(Result<(Thing<'a>, Type), ConceptReadError>) -> TupleResult<'a>;

pub(crate) fn isa_to_tuple_thing_type(result: Result<(Thing<'_>, Type), ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok((thing, type_)) => Ok(Tuple::Pair([VariableValue::Thing(thing), VariableValue::Type(type_)])),
        Err(err) => Err(err),
    }
}

pub(crate) fn isa_to_tuple_type_thing(result: Result<(Thing<'_>, Type), ConceptReadError>) -> TupleResult<'_> {
    match result {
        Ok((thing, type_)) => Ok(Tuple::Pair([VariableValue::Type(type_), VariableValue::Thing(thing)])),
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

pub(crate) fn links_to_tuple_role_relation_player<'a>(
    result: Result<(Relation<'a>, RolePlayer<'a>, u64), ConceptReadError>,
) -> TupleResult<'a> {
    let (rel, rp, _count) = result?;
    let role_type = rp.role_type();
    Ok(Tuple::Triple([
        VariableValue::Type(role_type.into()),
        VariableValue::Thing(rel.into()),
        VariableValue::Thing(rp.into_player().into()),
    ]))
}
