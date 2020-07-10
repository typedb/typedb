/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.concept.type;

import grakn.concept.thing.Entity;

import java.util.stream.Stream;

public interface EntityType extends ThingType {

    @Override
    default EntityType asEntityType() { return this; }

    @Override
    EntityType sup();

    @Override
    Stream<? extends EntityType> sups();

    @Override
    Stream<? extends EntityType> subs();

    @Override
    Stream<? extends Entity> instances();

    void sup(EntityType superType);

    Entity create();

    Entity create(boolean isInferred);
}
