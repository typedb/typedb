/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, cmp::Ordering, collections::Bound};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::ExecutorVariable;
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        has::Has,
        object::Object,
        relation::{IndexedRelationPlayers, Links, Relation},
        ThingAPI,
    },
    type_::{
        attribute_type::AttributeType, object_type::ObjectType, relation_type::RelationType, role_type::RoleType,
        TypeAPI,
    },
};
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeLinks},
            vertex_attribute::{AttributeID, AttributeVertex},
            vertex_object::ObjectVertex,
        },
        type_::vertex::TypeVertexEncoding,
        Typed,
    },
    value::value::Value,
};
use lending_iterator::higher_order::Hkt;

use crate::instruction::{has_executor::FixedHasBounds, links_executor::FixedLinksBounds};

pub(crate) type TupleOrderingFn = for<'a, 'b> fn((&'a TupleResult<'static>, &'b TupleResult<'static>)) -> Ordering;

pub(crate) fn unsafe_compare_result_tuple<'a, 'b>(
    (first, second): (&'a TupleResult<'static>, &'b TupleResult<'static>),
) -> Ordering {
    let first_tuple = match first {
        Ok(first) => first,
        Err(_) => {
            // arbitrary
            return Ordering::Equal;
        }
    };
    let second_tuple = match second {
        Ok(second) => second,
        Err(_) => {
            // arbitrary
            return Ordering::Equal;
        }
    };
    first_tuple.partial_cmp(second_tuple).unwrap()
}

#[derive(Debug, Clone)]
pub(crate) enum Tuple<'a> {
    Single([VariableValue<'a>; 1]),
    Pair([VariableValue<'a>; 2]),
    Triple([VariableValue<'a>; 3]),
    Quintuple([VariableValue<'a>; 5]),
    Arbitrary(Vec<VariableValue<'a>>), // TODO: unknown sized tuples, for functions
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
        self.values().partial_cmp(other.values())
    }
}

impl<'a> Tuple<'a> {
    pub(crate) fn values(&self) -> &[VariableValue<'a>] {
        match self {
            Tuple::Single(values) => values,
            Tuple::Pair(values) => values,
            Tuple::Triple(values) => values,
            Tuple::Quintuple(values) => values,
            Tuple::Arbitrary(values) => values,
        }
    }

    pub(crate) fn values_mut(&mut self) -> &mut [VariableValue<'a>] {
        match self {
            Tuple::Single(values) => values,
            Tuple::Pair(values) => values,
            Tuple::Triple(values) => values,
            Tuple::Quintuple(values) => values,
            Tuple::Arbitrary(values) => values,
        }
    }

    pub(crate) fn into_owned(self) -> Tuple<'static> {
        match self {
            Tuple::Single(values) => Tuple::Single(values.map(VariableValue::into_owned)),
            Tuple::Pair(values) => Tuple::Pair(values.map(VariableValue::into_owned)),
            Tuple::Triple(values) => Tuple::Triple(values.map(VariableValue::into_owned)),
            Tuple::Quintuple(values) => Tuple::Quintuple(values.map(VariableValue::into_owned)),
            Tuple::Arbitrary(values) => Tuple::Arbitrary(values.into_iter().map(VariableValue::into_owned).collect()),
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
pub(crate) type TupleToHasFn = fn(&Tuple<'_>, &FixedHasBounds) -> Has;

pub(crate) fn has_to_tuple_owner_attribute(result: Result<(Has, u64), Box<ConceptReadError>>) -> TupleResult<'static> {
    let (has, _count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(owner.into()), VariableValue::Thing(attribute.into())]))
}

pub(crate) fn tuple_owner_attribute_to_has_canonical(tuple: &Tuple<'_>, fixed_has_bounds: &FixedHasBounds) -> Has {
    let (tuple_owner, tuple_attribute) = tuple_owner_attribute_to_owner_attribute(tuple);
    let (owner, attribute) = match fixed_has_bounds {
        FixedHasBounds::NoneWithLowerBounds(attribute_type_lower_bound, value_bound) => match value_bound {
            Bound::Included(lower_bound) | Bound::Excluded(lower_bound) => {
                if AttributeID::is_inlineable(lower_bound.as_reference()) {
                    let composed_attribute = Attribute::new(AttributeVertex::new(
                        attribute_type_lower_bound.vertex().type_id_(),
                        AttributeID::build_inline(lower_bound.as_reference()),
                    ));
                    debug_assert!(composed_attribute >= *tuple_attribute);
                    (tuple_owner, Cow::Owned(composed_attribute))
                } else {
                    (tuple_owner, Cow::Borrowed(tuple_attribute))
                }
            }
            Bound::Unbounded => (tuple_owner, Cow::Borrowed(tuple_attribute)),
        },
        FixedHasBounds::Owner(fixed_owner) => (*fixed_owner, Cow::Borrowed(tuple_attribute)),
        FixedHasBounds::Attribute(fixed_attribute) => (tuple_owner, Cow::Borrowed(fixed_attribute)),
    };
    Has::Edge(ThingEdgeHas::new(owner.vertex(), attribute.vertex()))
}

pub(crate) fn tuple_owner_attribute_to_has_reverse(tuple: &Tuple<'_>, fixed_has_bounds: &FixedHasBounds) -> Has {
    let (tuple_owner, tuple_attribute) = tuple_owner_attribute_to_owner_attribute(tuple);
    let (owner, attribute) = match fixed_has_bounds {
        FixedHasBounds::NoneWithLowerBounds(_, _) => (tuple_owner, tuple_attribute),
        FixedHasBounds::Owner(fixed_owner) => (*fixed_owner, tuple_attribute),
        FixedHasBounds::Attribute(fixed_attribute) => (tuple_owner, fixed_attribute),
    };
    Has::EdgeReverse(ThingEdgeHasReverse::new(attribute.vertex(), owner.vertex()))
}

fn tuple_owner_attribute_to_owner_attribute<'a>(tuple: &'a Tuple<'a>) -> (Object, &'a Attribute) {
    let owner = tuple
        .values()
        .get(0)
        .expect("Reverse tuple mapping missing owner")
        .get_thing()
        .map(|thing| thing.as_object())
        .unwrap_or_else(|| Object::MIN);
    let attribute = tuple
        .values()
        .get(1)
        .expect("Reverse tuple mapping missing attribute")
        .get_thing()
        .map(|thing| thing.as_attribute())
        .unwrap_or_else(|| &concept::thing::attribute::MIN_STATIC);
    (owner, attribute)
}

pub(crate) fn has_to_tuple_attribute_owner(result: Result<(Has, u64), Box<ConceptReadError>>) -> TupleResult<'static> {
    let (has, _count) = result?;
    let (owner, attribute) = has.into_owner_attribute();
    Ok(Tuple::Pair([VariableValue::Thing(attribute.into()), VariableValue::Thing(owner.into())]))
}

pub(crate) fn tuple_attribute_owner_to_has_canonical(tuple: &Tuple<'_>, fixed_has_bounds: &FixedHasBounds) -> Has {
    let (tuple_attribute, tuple_owner) = tuple_attribute_owner_to_attribute_owner(tuple);
    let (attribute, owner) = match fixed_has_bounds {
        // note: this means attribute is given in tuple, so we can ignore constants
        FixedHasBounds::NoneWithLowerBounds(_, _) => (tuple_attribute, &tuple_owner),
        FixedHasBounds::Owner(fixed_owner) => (tuple_attribute, fixed_owner),
        FixedHasBounds::Attribute(fixed_attribute) => (fixed_attribute, &tuple_owner),
    };
    Has::Edge(ThingEdgeHas::new(owner.vertex(), attribute.vertex()))
}

pub(crate) fn tuple_attribute_owner_to_has_reverse(tuple: &Tuple<'_>, fixed_has_bounds: &FixedHasBounds) -> Has {
    let (tuple_attribute, tuple_owner) = tuple_attribute_owner_to_attribute_owner(tuple);
    let (attribute, owner) = match fixed_has_bounds {
        // note: this means attribute is given in tuple, so we can ignore constants
        FixedHasBounds::NoneWithLowerBounds(_, _) => (tuple_attribute, tuple_owner),
        FixedHasBounds::Owner(fixed_owner) => (tuple_attribute, *fixed_owner),
        FixedHasBounds::Attribute(fixed_attribute) => (fixed_attribute, tuple_owner),
    };
    Has::EdgeReverse(ThingEdgeHasReverse::new(attribute.vertex(), owner.vertex()))
}

fn tuple_attribute_owner_to_attribute_owner<'a>(tuple: &'a Tuple<'_>) -> (&'a Attribute, Object) {
    let attribute = tuple
        .values()
        .get(0)
        .expect("Reverse tuple mapping missing attribute")
        .get_thing()
        .map(|thing| thing.as_attribute())
        .unwrap_or_else(|| &concept::thing::attribute::MIN_STATIC);
    let owner = tuple
        .values()
        .get(1)
        .expect("Reverse tuple mapping missing owner")
        .get_thing()
        .map(|thing| thing.as_object())
        .unwrap_or_else(|| Object::MIN);
    (attribute, owner)
}

pub(crate) type LinksToTupleFn = fn(Result<(Links, u64), Box<ConceptReadError>>) -> TupleResult<'static>;
pub(crate) type TupleToLinksFn = fn(&Tuple<'_>, &FixedLinksBounds) -> Links;

pub(crate) fn links_to_tuple_relation_player_role(
    result: Result<(Links, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let (links, _count) = result?;
    Ok(Tuple::Triple([
        VariableValue::Thing(links.relation().into()),
        VariableValue::Thing(links.player().into()),
        VariableValue::Type(links.role_type().into()),
    ]))
}

pub(crate) fn tuple_relation_player_role_to_links_canonical(
    tuple: &Tuple<'_>,
    fixed_bounds: &FixedLinksBounds,
) -> Links {
    let (tuple_relation, tuple_player, role) = tuple_relation_player_role_to_relation_player_role(tuple);
    let (relation, player) = match fixed_bounds {
        FixedLinksBounds::None => (tuple_relation, tuple_player),
        FixedLinksBounds::Relation(fixed_relation) => (*fixed_relation, tuple_player),
        FixedLinksBounds::Player(fixed_player) => (tuple_relation, *fixed_player),
        FixedLinksBounds::RelationAndPlayer(fixed_relation, fixed_player) => (*fixed_relation, *fixed_player),
    };
    Links::Edge(ThingEdgeLinks::new(relation.vertex(), player.vertex(), role.vertex()))
}

pub(crate) fn tuple_relation_player_role_to_links_reverse(tuple: &Tuple<'_>, fixed_bounds: &FixedLinksBounds) -> Links {
    let (tuple_relation, tuple_player, role) = tuple_relation_player_role_to_relation_player_role(tuple);
    let (relation, player) = match fixed_bounds {
        FixedLinksBounds::None => (tuple_relation, tuple_player),
        FixedLinksBounds::Relation(fixed_relation) => (*fixed_relation, tuple_player),
        FixedLinksBounds::Player(fixed_player) => (tuple_relation, *fixed_player),
        FixedLinksBounds::RelationAndPlayer(fixed_relation, fixed_player) => (*fixed_relation, *fixed_player),
    };
    Links::EdgeReverse(ThingEdgeLinks::new_reverse(player.vertex(), relation.vertex(), role.vertex()))
}

fn tuple_relation_player_role_to_relation_player_role(tuple: &Tuple<'_>) -> (Relation, Object, RoleType) {
    let relation = tuple
        .values()
        .get(0)
        .expect("Reverse tuple mapping missing relation")
        .get_thing()
        .map(|thing| thing.as_relation())
        .unwrap_or_else(|| Relation::MIN);
    let player = tuple
        .values()
        .get(1)
        .expect("Reverse tuple mapping missing player")
        .get_thing()
        .map(|thing| thing.as_object())
        .unwrap_or_else(|| Object::MIN);
    let role = tuple
        .values()
        .get(2)
        .expect("Reverse tuple mapping missing role type")
        .get_type()
        .map(|type_| type_.as_role_type())
        .unwrap_or_else(|| RoleType::MIN);
    (relation, player, role)
}

pub(crate) fn links_to_tuple_player_relation_role(
    result: Result<(Links, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let (links, _count) = result?;
    Ok(Tuple::Triple([
        VariableValue::Thing(links.player().into()),
        VariableValue::Thing(links.relation().into()),
        VariableValue::Type(links.role_type().into()),
    ]))
}

pub(crate) fn tuple_player_relation_role_to_links_canonical(
    tuple: &Tuple<'_>,
    fixed_bounds: &FixedLinksBounds,
) -> Links {
    let (tuple_player, tuple_relation, role) = tuple_player_relation_role_to_player_relation_role(tuple);
    let (player, relation) = match fixed_bounds {
        FixedLinksBounds::None => (tuple_player, tuple_relation),
        FixedLinksBounds::Relation(fixed_relation) => (tuple_player, *fixed_relation),
        FixedLinksBounds::Player(fixed_player) => (*fixed_player, tuple_relation),
        FixedLinksBounds::RelationAndPlayer(fixed_relation, fixed_player) => (*fixed_player, *fixed_relation),
    };
    Links::Edge(ThingEdgeLinks::new(relation.vertex(), player.vertex(), role.vertex()))
}

pub(crate) fn tuple_player_relation_role_to_links_reverse(tuple: &Tuple<'_>, fixed_bounds: &FixedLinksBounds) -> Links {
    let (tuple_player, tuple_relation, role) = tuple_player_relation_role_to_player_relation_role(tuple);
    let (player, relation) = match fixed_bounds {
        FixedLinksBounds::None => (tuple_player, tuple_relation),
        FixedLinksBounds::Relation(fixed_relation) => (tuple_player, *fixed_relation),
        FixedLinksBounds::Player(fixed_player) => (*fixed_player, tuple_relation),
        FixedLinksBounds::RelationAndPlayer(fixed_relation, fixed_player) => (*fixed_player, *fixed_relation),
    };
    Links::EdgeReverse(ThingEdgeLinks::new_reverse(player.vertex(), relation.vertex(), role.vertex()))
}

fn tuple_player_relation_role_to_player_relation_role(tuple: &Tuple<'_>) -> (Object, Relation, RoleType) {
    let player = tuple
        .values()
        .get(0)
        .expect("Reverse tuple mapping missing player")
        .get_thing()
        .map(|thing| thing.as_object())
        .unwrap_or_else(|| Object::MIN);
    let relation = tuple
        .values()
        .get(1)
        .expect("Reverse tuple mapping missing relation")
        .get_thing()
        .map(|thing| thing.as_relation())
        .unwrap_or_else(|| Relation::MIN);
    let role = tuple
        .values()
        .get(2)
        .expect("Reverse tuple mapping missing role type")
        .get_type()
        .map(|type_| type_.as_role_type())
        .unwrap_or_else(|| RoleType::MIN);
    (player, relation, role)
}

pub(crate) fn links_to_tuple_role_relation_player(
    result: Result<(Links, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let (links, _count) = result?;
    Ok(Tuple::Triple([
        VariableValue::Type(links.role_type().into()),
        VariableValue::Thing(links.relation().into()),
        VariableValue::Thing(links.player().into()),
    ]))
}

pub(crate) fn tuple_role_relation_player_to_links_canonical(
    tuple: &Tuple<'_>,
    fixed_bounds: &FixedLinksBounds,
) -> Links {
    let (role, tuple_relation, tuple_player) = tuple_role_relation_player_to_role_relation_player(tuple);
    let (player, relation) = match fixed_bounds {
        FixedLinksBounds::None => (tuple_player, tuple_relation),
        FixedLinksBounds::Relation(fixed_relation) => (tuple_player, *fixed_relation),
        FixedLinksBounds::Player(fixed_player) => (*fixed_player, tuple_relation),
        FixedLinksBounds::RelationAndPlayer(fixed_relation, fixed_player) => (*fixed_player, *fixed_relation),
    };
    Links::Edge(ThingEdgeLinks::new(relation.vertex(), player.vertex(), role.vertex()))
}

pub(crate) fn tuple_role_relation_player_to_links_reverse(tuple: &Tuple<'_>, fixed_bounds: &FixedLinksBounds) -> Links {
    let (role, tuple_relation, tuple_player) = tuple_role_relation_player_to_role_relation_player(tuple);
    let (player, relation) = match fixed_bounds {
        FixedLinksBounds::None => (tuple_player, tuple_relation),
        FixedLinksBounds::Relation(fixed_relation) => (tuple_player, *fixed_relation),
        FixedLinksBounds::Player(fixed_player) => (*fixed_player, tuple_relation),
        FixedLinksBounds::RelationAndPlayer(fixed_relation, fixed_player) => (*fixed_player, *fixed_relation),
    };
    Links::EdgeReverse(ThingEdgeLinks::new_reverse(player.vertex(), relation.vertex(), role.vertex()))
}

fn tuple_role_relation_player_to_role_relation_player(tuple: &Tuple<'_>) -> (RoleType, Relation, Object) {
    let role = tuple
        .values()
        .get(0)
        .expect("Reverse tuple mapping missing role type")
        .get_type()
        .map(|type_| type_.as_role_type())
        .unwrap_or_else(|| RoleType::MIN);
    let relation = tuple
        .values()
        .get(1)
        .expect("Reverse tuple mapping missing relation")
        .get_thing()
        .map(|thing| thing.as_relation())
        .unwrap_or_else(|| Relation::MIN);
    let player = tuple
        .values()
        .get(2)
        .expect("Reverse tuple mapping missing player")
        .get_thing()
        .map(|thing| thing.as_object())
        .unwrap_or_else(|| Object::MIN);
    (role, relation, player)
}

pub(crate) type IndexedRelationToTupleFn =
    dyn Fn(Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>) -> TupleResult<'static>;

// corresponds to Unbound mode
pub(crate) fn indexed_relation_to_tuple_start_end_relation_startrole_endrole(
    result: Result<(IndexedRelationPlayers, u64), Box<ConceptReadError>>,
) -> TupleResult<'static> {
    let ((player_start, player_end, relation_type_id, relation_id, role_start, role_end), _count) = result?;
    let relation = Relation::new(ObjectVertex::build_relation(relation_type_id, relation_id));
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
    let ((player_start, player_end, relation_type_id, relation_id, role_start, role_end), _count) = result?;
    let relation = Relation::new(ObjectVertex::build_relation(relation_type_id, relation_id));
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
    let ((player_start, player_end, relation_type_id, relation_id, role_start, role_end), _count) = result?;
    let relation = Relation::new(ObjectVertex::build_relation(relation_type_id, relation_id));
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
    let ((player_start, player_end, relation_type_id, relation_id, role_start, role_end), _count) = result?;
    let relation = Relation::new(ObjectVertex::build_relation(relation_type_id, relation_id));
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
    let ((player_start, player_end, relation_type_id, relation_id, role_start, role_end), _count) = result?;
    let relation = Relation::new(ObjectVertex::build_relation(relation_type_id, relation_id));
    Ok(Tuple::Quintuple([
        VariableValue::Type(role_end.into()),
        VariableValue::Thing(player_start.into()),
        VariableValue::Thing(player_end.into()),
        VariableValue::Thing(relation.into()),
        VariableValue::Type(role_start.into()),
    ]))
}
