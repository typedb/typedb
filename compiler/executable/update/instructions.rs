/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use crate::executable::insert::{ThingPosition, TypeSource};

#[derive(Debug)]
pub enum ConnectionInstruction {
    Has(Has),
    HasOrdered(HasOrdered),
    Links(Links),
    LinksOrdered(LinksOrdered),
}

impl fmt::Display for ConnectionInstruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionInstruction::Has(_) => write!(f, "has"),
            ConnectionInstruction::HasOrdered(_) => write!(f, "ordered has"),
            ConnectionInstruction::Links(_) => write!(f, "links"),
            ConnectionInstruction::LinksOrdered(_) => write!(f, "ordered links"),
        }
    }
}

#[derive(Debug)]
pub struct Has {
    pub owner: ThingPosition,
    pub attribute: ThingPosition,
}

#[derive(Debug)]
pub struct HasOrdered {
    pub owner: ThingPosition,
    pub attribute_type: TypeSource,
    pub attributes: Vec<ThingPosition>,
}

#[derive(Debug)]
pub struct Links {
    pub relation: ThingPosition,
    pub player: ThingPosition,
    pub role: TypeSource,
}

#[derive(Debug)]
pub struct LinksOrdered {
    pub relation: ThingPosition,
    pub role: TypeSource,
    pub players: Vec<ThingPosition>,
}
