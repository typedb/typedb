/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::Formatter;

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
    pub const fn root_label(&self) -> Label {
        match self {
            Kind::Entity => Label::new_static("entity"),
            Kind::Attribute => Label::new_static("attribute"),
            Kind::Relation => Label::new_static("relation"),
            Kind::Role => Label::new_static_scoped("role", "relation", "relation:role"),
        }
    }
}

impl std::fmt::Debug for Kind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kind[{}]", self.root_label().name)
    }
}
