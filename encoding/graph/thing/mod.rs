/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod edge;
pub mod vertex_attribute;
pub mod vertex_generator;
pub mod vertex_object;
mod property;


trait VertexID {
    const LENGTH: usize;
}
