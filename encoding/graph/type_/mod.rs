/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter, Result};

pub mod edge;
pub mod index;
pub mod property;
pub mod vertex;
pub mod vertex_generator;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Kind {
    Entity,
    Attribute,
    Relation,
    Role,
}

impl Kind {
    pub const fn name(&self) -> &'static str {
        match self {
            Kind::Entity => "entity",
            Kind::Attribute => "attribute",
            Kind::Relation => "relation",
            Kind::Role => "relation:role",
        }
    }
}

impl std::fmt::Debug for Kind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kind[{}]", self.name())
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Copy, Clone)]
pub enum CapabilityKind {
    Relates,
    Plays,
    Owns,
}

impl CapabilityKind {
    pub fn name(&self) -> &'static str {
        match self {
            CapabilityKind::Relates => "relates",
            CapabilityKind::Plays => "plays",
            CapabilityKind::Owns => "owns",
        }
    }
}

impl Display for CapabilityKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.name())
    }
}

impl std::fmt::Debug for CapabilityKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "CapabilityKind[{}]", self)
    }
}
