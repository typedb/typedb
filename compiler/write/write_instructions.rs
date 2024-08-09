/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value::Value;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum VariableSource {
    TypeSource(TypeSource),
    ValueSource(ValueSource),
    ThingSource(ThingSource),
}

type VariablePosition = u32;
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum TypeSource {
    InputVariable(VariablePosition),
    TypeConstant(answer::Type),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ValueSource {
    InputVariable(VariablePosition),
    ValueConstant(Value<'static>),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ThingSource {
    InputVariable(VariablePosition),
    InsertedVariable(usize),
}

#[derive(Debug)]
pub struct PutEntity {
    pub type_: TypeSource,
}

#[derive(Debug)]
pub struct PutAttribute {
    pub type_: TypeSource,
    pub value: ValueSource,
}

#[derive(Debug)]
pub struct PutRelation {
    pub type_: TypeSource,
}

#[derive(Debug)]
pub struct DeleteEntity {
    pub entity: ThingSource,
}

#[derive(Debug)]
pub struct DeleteAttribute {
    pub attribute: ThingSource,
}

#[derive(Debug)]
pub struct Relation {
    pub relation: ThingSource,
}

#[derive(Debug)]
pub struct Has {
    pub owner: ThingSource,
    pub attribute: ThingSource,
}

#[derive(Debug)]
pub struct RolePlayer {
    pub relation: ThingSource,
    pub player: ThingSource,
    pub role: TypeSource,
}
