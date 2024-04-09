/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use bytes::byte_reference::ByteReference;

pub mod error;
pub mod iterator;
pub mod thing;
pub mod type_;
mod messages;

pub trait ConceptAPI<'a>: Eq + PartialEq {}

