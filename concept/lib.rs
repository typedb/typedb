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

use bytes::byte_reference::ByteReference;
use encoding::AsBytes;
use thing::{AttributeAPI, ObjectAPI};
use type_::TypeAPI;

pub mod type_;
pub mod thing;
pub mod error;
pub mod iterator;

trait ConceptAPI<'a>: Eq + PartialEq {}

// ---- IID implementations ---

trait IIDAPI<'a> {
    fn iid(&'a self) -> ByteReference<'a>;
}


// --- Annotations ---

trait Annotatable<'a>: IIDAPI<'a> {

    fn set_annotation(&self) {
        // set annotation on this structure
        todo!()
    }

    fn get_annotations(&self) {
        // get "effective" annotations
        todo!()
    }

    fn get_declared_annotations(&self) {
        // get annotations declared
        todo!()
    }

    fn get_inherited_annotations(&self) {
        // get annotations inherited
        todo!()
    }
}