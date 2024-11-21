/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use super::{ThingSource, TypeSource, ValueSource};

#[derive(Debug)]
pub enum ConceptInstruction {
    PutObject(PutObject),
    PutAttribute(PutAttribute),
}

impl Display for ConceptInstruction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConceptInstruction::PutObject(_) => write!(f, "Put object"),
            ConceptInstruction::PutAttribute(_) => write!(f, "Put attribute"),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionInstruction {
    Has(Has),     // TODO: Ordering
    Links(Links), // TODO: Ordering
}

impl Display for ConnectionInstruction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Has(_) => write!(f, "Put has"),
            Self::Links(_) => write!(f, "Put links"),
        }
    }
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
pub struct Links {
    pub relation: ThingSource,
    pub player: ThingSource,
    pub role: TypeSource,
}
