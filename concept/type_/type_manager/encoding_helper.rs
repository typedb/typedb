/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;
use encoding::graph::type_::edge::EncodableParametrisedTypeEdge;
use encoding::layout::prefix::Prefix;
use crate::type_::type_manager::KindAPI;

//
// pub struct EdgeSub<'a, T: KindAPI<'a>> {
//     t: PhantomData<T>,
// }
// impl<'a, T: KindAPI<'static>> EncodableParametrisedTypeEdge<'a> for EdgeSub<T> {
//     const CANONICAL_PREFIX: Prefix = Prefix::EdgeSub;
//     const REVERSE_PREFIX: Prefix = Prefix::EdgeSubReverse;
//     type From = T;
//     type To = T;
//
//     fn from_vertices(from: T, to: T) -> Self {
//         todo!()
//     }
//
//     fn from(&self) -> Self::From {
//         todo!()
//     }
//
//     fn to(&self) -> Self::To {
//         todo!()
//     }
// }
