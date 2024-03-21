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

use crate::type_::relation_type::RelationType;
use crate::type_::{RelationTypeAPI, RoleTypeAPI};
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
