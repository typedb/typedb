/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};
use crate::executable::insert::{ThingSource, TypeSource};

#[derive(Debug)]
pub enum ConnectionInstruction {
    Has(Has),               // TODO: Ordering
    Links(Links),           // TODO: Ordering
}

impl Display for ConnectionInstruction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionInstruction::Has(_) => write!(f, "has"),
            ConnectionInstruction::Links(_) => write!(f, "links"),
        }
    }
}

#[derive(Debug)]
pub struct ThingInstruction {
    pub thing: ThingSource,
}

impl Display for ThingInstruction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "instance")
    }
}

#[derive(Debug)]
pub struct Has {
    pub owner: ThingSource,
    pub attribute: ThingSource,
}

#[derive(Debug)]
pub struct Links {
    pub relation: ThingSource,
    pub player: ThingSource,
    pub role: TypeSource,
}
