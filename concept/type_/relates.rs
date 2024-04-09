/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Relates<'a> {
    relation: RelationType<'a>,
    role: RoleType<'a>
}

impl<'a> Relates<'a> {
    pub(crate) fn new(relation: RelationType<'a>, role: RoleType<'a>) -> Self {
        Relates { relation: relation, role: role }
    }

    pub fn relation(&self) -> RelationType<'a> {
        self.relation.clone()
    }

    pub fn role(&self) -> RoleType<'a> {
        self.role.clone()
    }
}
