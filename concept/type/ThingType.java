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

package grakn.core.concept.type;

import grakn.core.concept.thing.Thing;

import java.util.stream.Stream;

public interface ThingType extends Type {

    @Override
    default ThingType asThingType() { return this; }

    @Override
    ThingType sup();

    @Override
    Stream<? extends ThingType> sups();

    @Override
    Stream<? extends ThingType> subs();

    Stream<? extends Thing> instances();

    void isAbstract(boolean isAbstract);

    void has(AttributeType attributeType);

    void has(AttributeType attributeType, boolean isKey);

    void has(AttributeType attributeType, AttributeType overriddenType);

    void has(AttributeType attributeType, AttributeType overriddenType, boolean isKey);

    void unhas(AttributeType attributeType);

    Stream<? extends AttributeType> attributes();

    Stream<? extends AttributeType> attributes(boolean isKeyOnly);

    Stream<? extends AttributeType> attributes(Class<?> valueType);

    Stream<? extends AttributeType> attributes(Class<?> valueType, boolean isKeyOnly);

    void plays(RoleType roleType);

    void plays(RoleType roleType, RoleType overriddenType);

    void unplay(RoleType roleType);

    Stream<? extends RoleType> playing();
}
