/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::planner::insert_planner::VariablePosition;

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum VariableSource {
    TypeSource(TypeSource),
    ValueSource(ValueSource),
    ThingSource(ThingSource),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum TypeSource {
    Input(VariablePosition),
    TypeConstant(usize),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum ValueSource {
    Input(VariablePosition),
    ValueConstant(usize),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum ThingSource {
    Input(VariablePosition),
    Inserted(usize),
}

#[derive(Debug)]
pub struct IsaEntity {
    pub type_: TypeSource,
}

#[derive(Debug)]
pub struct IsaAttribute {
    pub type_: TypeSource,
    pub value: ValueSource,
}

#[derive(Debug)]
pub struct IsaRelation {
    pub type_: TypeSource,
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
