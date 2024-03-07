/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use encoding::AsBytes;
use encoding::graph::thing::vertex::{AttributeVertex, ObjectVertex};

use crate::{ConceptAPI, IIDAPI};

pub mod attribute;
pub mod entity;
pub mod thing_manager;

trait ThingAPI<'a>: ConceptAPI<'a> {}

pub trait ObjectAPI<'a>: ThingAPI<'a> {
    fn vertex(&'a self) -> &ObjectVertex<'a>;
}

pub trait AttributeAPI<'a>: ThingAPI<'a> {
    fn vertex(&'a self) -> &AttributeVertex<'a>;
}

