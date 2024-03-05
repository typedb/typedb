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

use std::borrow::Cow;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Label<'a> {
    pub name: Cow<'a, str>,
    pub scope: Option<Cow<'a, str>>,
}

impl<'a> Label<'a> {

    pub fn new(name: &'a str) -> Self {
        Label {
            name: Cow::Borrowed(name),
            scope: None,
        }
    }

    pub fn new_scoped(name: &'a str, scope: &'a str) -> Self {
        Label {
            name: Cow::Borrowed(name),
            scope: Some(Cow::Borrowed(scope)),
        }
    }

    pub fn name(&'a self) -> &'a str {
        &self.name
    }

    // TODO; can this just return an &Option<str> ?
    pub fn scope(&'a self) -> &Option<Cow<'a, str>> {
        &self.scope
    }
}
