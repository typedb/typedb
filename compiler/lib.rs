/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(elided_lifetimes_in_paths)]
#![allow(clippy::result_large_err)]

use std::fmt::{Display, Formatter};

use ir::pattern::IrID;

pub mod annotation;
mod optimisation;
pub mod executable;

macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}
pub(crate) use filter_variants;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct VariablePosition {
    position: u32,
}

impl VariablePosition {
    pub fn new(position: u32) -> Self {
        VariablePosition { position }
    }

    pub fn as_usize(&self) -> usize {
        self.position as usize
    }
}

impl Display for VariablePosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "P_{}", self.position)
    }
}

impl IrID for VariablePosition {}
