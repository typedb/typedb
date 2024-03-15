/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use crate::value::label::Label;

pub mod edge;
pub mod index;
pub mod property;
pub mod vertex;
pub mod vertex_generator;

pub enum Root {
    Entity,
    Attribute,
    Relation,
    Role,
}

impl Root {
    pub const fn label(&self) -> Label {
        match self {
            Root::Entity => Label::new_static("entity"),
            Root::Attribute => Label::new_static("attribute"),
            Root::Relation => Label::new_static("relation"),
            Root::Role => Label::new_static_scoped("role", "relation", "relation:role"),
        }
    }
}
