/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value::Value;

use crate::write::{ThingSource, TypeSource, ValueSource};

#[derive(Debug)]
pub struct PutObject {
    pub type_: TypeSource,
}

#[derive(Debug)]
pub struct PutAttribute {
    pub type_: TypeSource,
    pub value: ValueSource,
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
