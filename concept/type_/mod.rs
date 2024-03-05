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

use crate::type_::attribute_type::AttributeType;

pub mod attribute_type;
pub mod entity_type;
pub mod type_manager;


struct Owns {}

trait Owner {
    fn set_owns(&self, attribute_type: &AttributeType) -> Owns {
        // encode ownership of the type
        todo!()
    }

    fn get_owns(&self) {
        // fetch iterator of Owns
        todo!()
    }
}

trait Owned {

    fn get_owners(&self) {
        // return owners of this type
        todo!()
    }
}

// Anything that is IID can be made annotateable
// --> We can make all Edges and Vertices implement IID
// --> All Concepts (eg. anything that has a Vertex) can be IID
// --> LAYOUT: we can make scans simpler by making annotations be a different prefix? This means edges and vertices will be treated the same...
trait Annotatable {

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