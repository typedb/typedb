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

use crate::error::ConceptWriteError;
use crate::thing::attribute::Attribute;
use crate::thing::entity::Entity;
use crate::thing::relation::Relation;
use crate::thing::thing_manager::ThingManager;

enum Object<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
}

impl<'a> Object<'a> {
    fn set_has<D>(&self, thing_manager: &ThingManager<'_, '_, D>, attribute: &Attribute<'_>)
                  -> Result<(), ConceptWriteError> {
        match self {
            Object::Entity(entity) => entity.set_has(thing_manager, attribute),
            Object::Relation(relation) => relation.set_has(thing_manager, attribute),
        }
    }

    fn into_owned(self) -> Object<'static> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.into_owned()),
            Object::Relation(relation) => Object::Relation(relation.into_owned()),
        }
    }
}