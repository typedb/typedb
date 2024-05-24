/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct Variable {
    id: VariableId,
}

impl Variable {
    pub fn new(id: u16) -> Self {
        Self { id: VariableId { id } }
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub(crate) struct VariableId {
    id: u16,
    // TODO: retain line/character from original query at which point this Variable was declared
}

impl VariableId {
    const MAX: usize = u16::MAX as usize;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum VariableType {
    Type,
    Object,
    Attribute,
    RoleImpl,
    Value,
    ObjectList,
    AttributeList,
    ValueList,
    RoleImplList,
}
