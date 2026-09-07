/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use crate::executable::insert::{ThingPosition, TypeSource, ValueSource};

#[derive(Debug)]
pub enum InsertInstruction {
    Concept(ConceptInstruction),
    Connection(ConnectionInstruction),
}

impl fmt::Display for InsertInstruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InsertInstruction::Concept(inner) => fmt::Display::fmt(inner, f),
            InsertInstruction::Connection(inner) => fmt::Display::fmt(inner, f),
        }
    }
}

#[derive(Debug)]
pub enum ConceptInstruction {
    PutObject(PutObject),
    PutAttribute(PutAttribute),
}

impl ConceptInstruction {
    pub(crate) fn inserted_type(&self) -> &TypeSource {
        match self {
            ConceptInstruction::PutObject(inner) => &inner.type_,
            ConceptInstruction::PutAttribute(inner) => &inner.type_,
        }
    }

    pub(crate) fn inserted_position(&self) -> ThingPosition {
        match self {
            ConceptInstruction::PutObject(inner) => inner.write_to,
            ConceptInstruction::PutAttribute(inner) => inner.write_to,
        }
    }
}

impl fmt::Display for ConceptInstruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConceptInstruction::PutObject(_) => write!(f, "Put object"),
            ConceptInstruction::PutAttribute(_) => write!(f, "Put attribute"),
        }
    }
}

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
            Self::Has(_) => write!(f, "Put has"),
            Self::HasOrdered(_) => write!(f, "Put ordered has"),
            Self::Links(_) => write!(f, "Put links"),
            Self::LinksOrdered(_) => write!(f, "Put ordered links"),
        }
    }
}

// TODO: Move to storing the inserted thing directly into the output row
#[derive(Debug)]
pub struct PutObject {
    pub type_: TypeSource,
    pub write_to: ThingPosition,
}

#[derive(Debug)]
pub struct PutAttribute {
    pub type_: TypeSource,
    pub value: ValueSource,
    pub write_to: ThingPosition,
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
