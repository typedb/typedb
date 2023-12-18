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
 *
 */

use storage::key::{Keyable};

pub mod thing;
pub mod type_;

pub const PREFIX_SIZE: usize = 1;
pub const INFIX_SIZE: usize = 1;

pub enum Prefix {
    EntityType,
    AttributeType,

    Entity,
    Attribute,
}

impl Prefix {
    pub fn as_bytes(&self) -> &[u8; PREFIX_SIZE] {
        match self {
            Prefix::EntityType => &[0],
            Prefix::AttributeType => &[1],
            Prefix::Entity => &[100],
            Prefix::Attribute => &[101],
        }
    }
}

pub enum Infix {
    HasForward,
    HasBackward,
}

impl Infix {
    pub(crate) fn as_bytes(&self) -> &[u8; INFIX_SIZE] {
        match self {
            Infix::HasForward => &[0],
            Infix::HasBackward => &[1],
        }
    }
}
