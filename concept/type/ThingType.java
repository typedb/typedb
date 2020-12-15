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
    ThingType getSupertype();

    @Override
    Stream<? extends ThingType> getSupertypes();

    @Override
    Stream<? extends ThingType> getSubtypes();

    Stream<? extends Thing> getInstances();

    long getInstancesCount();

    void setAbstract();

    void unsetAbstract();

    void setOwns(AttributeType attributeType);

    void setOwns(AttributeType attributeType, boolean isKey);

    void setOwns(AttributeType attributeType, AttributeType overriddenType);

    void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey);

    void unsetOwns(AttributeType attributeType);

    Stream<? extends AttributeType> getOwns();

    Stream<? extends AttributeType> getOwns(boolean onlyKey);

    Stream<? extends AttributeType> getOwns(AttributeType.ValueType valueType);

    Stream<? extends AttributeType> getOwns(AttributeType.ValueType valueType, boolean onlyKey);

    void setPlays(RoleType roleType);

    void setPlays(RoleType roleType, RoleType overriddenType);

    void unsetPlays(RoleType roleType);

    Stream<? extends RoleType> getPlays();
}
