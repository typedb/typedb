/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt::{Display, Formatter},
    iter::empty,
    marker::PhantomData,
};

use crate::pattern::IrID;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Expression<ID: IrID> {
    phantom: PhantomData<ID>,
}

impl<ID: IrID> Expression<ID> {
    pub fn ids(&self) -> impl Iterator<Item = ID> {
        todo!();
        empty()
    }
}

impl<ID: IrID> Display for Expression<ID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
