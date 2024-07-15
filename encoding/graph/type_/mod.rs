/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter, Result};

use crate::value::label::Label;

pub mod edge;
pub mod index;
pub mod property;
pub mod vertex;
pub mod vertex_generator;

#[derive(Copy, Clone)]
pub enum Kind {
    Entity,
    Attribute,
    Relation,
    Role,
}

impl Kind {
    const fn all_kinds() -> [Kind; 4] {
        [Kind::Entity, Kind::Attribute, Kind::Relation, Kind::Role]
    }

    pub const fn root_label(&self) -> Label {
        match self {
            Kind::Entity => Label::new_static("entity"),
            Kind::Attribute => Label::new_static("attribute"),
            Kind::Relation => Label::new_static("relation"),
            Kind::Role => Label::new_static_scoped("role", "relation", "relation:role"),
        }
    }

    pub fn is_root_label(label: &Label) -> bool {
        Kind::all_kinds().iter().any(|kind| kind.root_label() == *label)
    }
}

impl std::fmt::Debug for Kind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kind[{}]", self.root_label().name)
    }
}

#[derive(Copy, Clone)]
pub enum CapabilityKind {
    Relates,
    Plays,
    Owns,
}

impl CapabilityKind {
    const fn all_kinds() -> [CapabilityKind; 3] {
        [CapabilityKind::Relates, CapabilityKind::Plays, CapabilityKind::Owns]
    }
}

impl Display for CapabilityKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let str = match self {
            CapabilityKind::Relates => "relates",
            CapabilityKind::Plays => "plays",
            CapabilityKind::Owns => "owns",
        };
        write!(f, "{}", str)
    }
}

impl std::fmt::Debug for CapabilityKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "CapabilityKind[{}]", self.to_string())
    }
}
