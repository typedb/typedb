/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::insert::{ThingSource, TypeSource};

#[derive(Debug)]
pub enum DeleteInstruction {
    // TODO: Just replace this with regular `Constraint`s and use a mapped-row?
    Thing(DeleteThing),
    Has(Has),               // TODO: Ordering
    RolePlayer(RolePlayer), // TODO: Ordering
}

#[derive(Debug)]
pub struct DeleteThing {
    pub thing: ThingSource,
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
