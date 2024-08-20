/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value::Value;

use super::{ThingSource, TypeSource, ValueSource};

#[derive(Debug)]
pub enum InsertVertexInstruction {
    PutObject(PutObject),
    PutAttribute(PutAttribute),
}

#[derive(Debug)]
pub enum InsertEdgeInstruction {
    Has(Has),               // TODO: Ordering
    RolePlayer(RolePlayer), // TODO: Ordering
}

// TODO: Move to storing the inserted thing directly into the output row
#[derive(Debug)]
pub struct PutObject {
    pub type_: TypeSource,
    pub write_to: ThingSource,
}

#[derive(Debug)]
pub struct PutAttribute {
    pub type_: TypeSource,
    pub value: ValueSource,
    pub write_to: ThingSource,
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
