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
        relation::{IndexedRelationPlayers, Relation, RolePlayer},
    },
    type_::{attribute_type::AttributeType, object_type::ObjectType, relation_type::RelationType, role_type::RoleType},
};
use lending_iterator::higher_order::Hkt;

#[derive(Debug, Clone)]
pub(crate) enum Tuple<'a> {
    Single([VariableValue<'a>; 1]),
    Pair([VariableValue<'a>; 2]),
    Triple([VariableValue<'a>; 3]),
    Quintuple([VariableValue<'a>; 5]),
    Arbitrary(), // TODO: unknown sized tuples, for functions
}

impl PartialEq<Tuple<'_>> for Tuple<'_> {
    fn eq(&self, other: &Tuple<'_>) -> bool {
        match (self, other) {
            (Tuple::Single(this), Tuple::Single(that)) => this.eq(that),
            (Tuple::Pair(this), Tuple::Pair(that)) => this.eq(that),
            (Tuple::Triple(this), Tuple::Triple(that)) => this.eq(that),
            (Tuple::Quintuple(this), Tuple::Quintuple(that)) => this.eq(that),
            _ => false,
        }
    }
}

impl PartialOrd<Tuple<'_>> for Tuple<'_> {
    fn partial_cmp(&self, other: &Tuple<'_>) -> Option<std::cmp::Ordering> {
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

pub(crate) type TupleResult<'a> = Result<Tuple<'a>, Box<ConceptReadError>>;

pub(crate) type TypeToTupleFn = fn(Result<Type, Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn type_to_tuple(type_: Result<Type, Box<ConceptReadError>>) -> TupleResult<'static> {
    Ok(Tuple::Single([VariableValue::Type(type_?)]))
}

pub(crate) type SubToTupleFn = fn(Result<(Type, Type), Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn sub_to_tuple_sub_super(result: Result<(Type, Type), Box<ConceptReadError>>) -> TupleResult<'static> {
    match result {
        Ok((sub, sup)) => Ok(Tuple::Pair([VariableValue::Type(sub), VariableValue::Type(sup)])),
        Err(err) => Err(err),
    }
}

pub(crate) fn sub_to_tuple_super_sub(result: Result<(Type, Type), Box<ConceptReadError>>) -> TupleResult<'static> {
    match result {
        Ok((sub, sup)) => Ok(Tuple::Pair([VariableValue::Type(sup), VariableValue::Type(sub)])),
        Err(err) => Err(err),
    }
}

pub(crate) type OwnsToTupleFn = fn(Result<(ObjectType, AttributeType), Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn owns_to_tuple_owner_attribute<'a>(
    result: Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
) -> TupleResult<'a> {
    match result {
        Ok((owner, attribute)) => {
            Ok(Tuple::Pair([Type::from(owner), Type::Attribute(attribute)].map(VariableValue::Type)))
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn owns_to_tuple_attribute_owner<'a>(
    result: Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
) -> TupleResult<'a> {
    match result {
        Ok((owner, attribute)) => {
            Ok(Tuple::Pair([Type::Attribute(attribute), Type::from(owner)].map(VariableValue::Type)))
        }
        Err(err) => Err(err),
    }
}

pub(crate) type RelatesToTupleFn = fn(Result<(RelationType, RoleType), Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn relates_to_tuple_relation_role<'a>(
    result: Result<(RelationType, RoleType), Box<ConceptReadError>>,
) -> TupleResult<'a> {
    match result {
        Ok((relation, role)) => {
            Ok(Tuple::Pair([Type::Relation(relation), Type::RoleType(role)].map(VariableValue::Type)))
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn relates_to_tuple_role_relation<'a>(
    result: Result<(RelationType, RoleType), Box<ConceptReadError>>,
) -> TupleResult<'a> {
    match result {
        Ok((relation, role)) => {
            Ok(Tuple::Pair([Type::RoleType(role), Type::Relation(relation)].map(VariableValue::Type)))
        }
        Err(err) => Err(err),
    }
}

pub(crate) type PlaysToTupleFn = fn(Result<(ObjectType, RoleType), Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn plays_to_tuple_player_role<'a>(
    result: Result<(ObjectType, RoleType), Box<ConceptReadError>>,
) -> TupleResult<'a> {
    match result {
        Ok((player, role)) => Ok(Tuple::Pair([Type::from(player), Type::RoleType(role)].map(VariableValue::Type))),
        Err(err) => Err(err),
    }
}

pub(crate) fn plays_to_tuple_role_player<'a>(
    result: Result<(ObjectType, RoleType), Box<ConceptReadError>>,
) -> TupleResult<'a> {
    match result {
        Ok((player, role)) => Ok(Tuple::Pair([Type::RoleType(role), Type::from(player)].map(VariableValue::Type))),
        Err(err) => Err(err),
    }
}

pub(crate) type IsaToTupleFn = fn(Result<(Thing, Type), Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn isa_to_tuple_thing_type(result: Result<(Thing, Type), Box<ConceptReadError>>) -> TupleResult<'static> {
    match result {
        Ok((thing, type_)) => Ok(Tuple::Pair([VariableValue::Thing(thing), VariableValue::Type(type_)])),
        Err(err) => Err(err),
    }
}

pub(crate) fn isa_to_tuple_type_thing(result: Result<(Thing, Type), Box<ConceptReadError>>) -> TupleResult<'static> {
    match result {
        Ok((thing, type_)) => Ok(Tuple::Pair([VariableValue::Type(type_), VariableValue::Thing(thing)])),
        Err(err) => Err(err),
    }
}

pub(crate) type HasToTupleFn = fn(Result<(Has, u64), Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn has_to_tuple_owner_attribute(result: Result<(Has, u64), Box<ConceptReadError>>) -> TupleResult<'static> {
    let (has, _count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(owner.into()), VariableValue::Thing(attribute.into())]))
}

pub(crate) fn has_to_tuple_attribute_owner(result: Result<(Has, u64), Box<ConceptReadError>>) -> TupleResult<'static> {
    let (has, _count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(attribute.into()), VariableValue::Thing(owner.into())]))
}

pub(crate) type LinksToTupleFn = fn(Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>) -> TupleResult<'static>;

pub(crate) fn links_to_tuple_relation_player_role(
    result: Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let (rel, rp, _count) = result?;
    let role_type = rp.role_type();
    Ok(Tuple::Triple([
        VariableValue::Thing(rel.into()),
        VariableValue::Thing(rp.player().into()),
        VariableValue::Type(role_type.into()),
    ]))
}

pub(crate) fn links_to_tuple_player_relation_role(
    result: Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let (rel, rp, _count) = result?;
    let role_type = rp.role_type();
    Ok(Tuple::Triple([
        VariableValue::Thing(rp.player().into()),
        VariableValue::Thing(rel.into()),
        VariableValue::Type(role_type.into()),
    ]))
}

pub(crate) fn links_to_tuple_role_relation_player(
    result: Result<(Relation, RolePlayer, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let (rel, rp, _count) = result?;
    let role_type = rp.role_type();
    Ok(Tuple::Triple([
        VariableValue::Type(role_type.into()),
        VariableValue::Thing(rel.into()),
        VariableValue::Thing(rp.player().into()),
    ]))
}

pub(crate) type IndexedRelationToTupleFn =
    dyn Fn(Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>) -> TupleResult<'static>;

// corresponds to Unbound mode
pub(crate) fn indexed_relation_to_tuple_start_end_relation_startrole_endrole(
    result: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let ((player_start, player_end, relation, role_start, role_end), _count) = result?;
    Ok(Tuple::Quintuple([
        VariableValue::Thing(player_start.into()),
        VariableValue::Thing(player_end.into()),
        VariableValue::Thing(relation.into()),
        VariableValue::Type(role_start.into()),
        VariableValue::Type(role_end.into()),
    ]))
}

// corresponds to Unbound Inverted or BoundStart modes
pub(crate) fn indexed_relation_to_tuple_end_start_relation_startrole_endrole(
    result: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let ((player_start, player_end, relation, role_start, role_end), _count) = result?;
    Ok(Tuple::Quintuple([
        VariableValue::Thing(player_end.into()),
        VariableValue::Thing(player_start.into()),
        VariableValue::Thing(relation.into()),
        VariableValue::Type(role_start.into()),
        VariableValue::Type(role_end.into()),
    ]))
}

// corresponds to BoundStartBoundEnd mode
pub(crate) fn indexed_relation_to_tuple_relation_start_end_startrole_endrole(
    result: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let ((player_start, player_end, relation, role_start, role_end), _count) = result?;
    Ok(Tuple::Quintuple([
        VariableValue::Thing(relation.into()),
        VariableValue::Thing(player_start.into()),
        VariableValue::Thing(player_end.into()),
        VariableValue::Type(role_start.into()),
        VariableValue::Type(role_end.into()),
    ]))
}

// corresponds to BoundStartBoundEndBoundRelation mode
pub(crate) fn indexed_relation_to_tuple_startrole_start_end_relation_endrole(
    result: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let ((player_start, player_end, relation, role_start, role_end), _count) = result?;
    Ok(Tuple::Quintuple([
        VariableValue::Type(role_start.into()),
        VariableValue::Thing(player_start.into()),
        VariableValue::Thing(player_end.into()),
        VariableValue::Thing(relation.into()),
        VariableValue::Type(role_end.into()),
    ]))
}

pub(crate) fn indexed_relation_to_tuple_endrole_start_end_relation_relation_startrole(
    result: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let ((player_start, player_end, relation, role_start, role_end), _count) = result?;
    Ok(Tuple::Quintuple([
        VariableValue::Type(role_end.into()),
        VariableValue::Thing(player_start.into()),
        VariableValue::Thing(player_end.into()),
        VariableValue::Thing(relation.into()),
        VariableValue::Type(role_start.into()),
    ]))
}
