/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.thing.Thing;

public interface ThingType extends Type {

    @Override
    ThingType getSupertype();

    @Override
    FunctionalIterator<? extends ThingType> getSupertypes();

    @Override
    FunctionalIterator<? extends ThingType> getSubtypes();

    @Override
    FunctionalIterator<? extends ThingType> getSubtypesExplicit();

    FunctionalIterator<? extends Thing> getInstances();

    FunctionalIterator<? extends Thing> getInstancesExplicit();

    void setAbstract();

    void unsetAbstract();

    void setOwns(AttributeType attributeType);

    void setOwns(AttributeType attributeType, boolean isKey);

    void setOwns(AttributeType attributeType, AttributeType overriddenType);

    void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey);

    void unsetOwns(AttributeType attributeType);

    FunctionalIterator<? extends AttributeType> getOwns();

    FunctionalIterator<? extends AttributeType> getOwnsExplicit();

    FunctionalIterator<? extends AttributeType> getOwns(boolean onlyKey);

    FunctionalIterator<? extends AttributeType> getOwnsExplicit(boolean onlyKey);

    FunctionalIterator<? extends AttributeType> getOwns(AttributeType.ValueType valueType);

    FunctionalIterator<? extends AttributeType> getOwnsExplicit(AttributeType.ValueType valueType);

    FunctionalIterator<? extends AttributeType> getOwns(AttributeType.ValueType valueType, boolean onlyKey);

    FunctionalIterator<? extends AttributeType> getOwnsExplicit(AttributeType.ValueType valueType, boolean onlyKey);

    AttributeType getOwnsOverridden(AttributeType attributeType);

    void setPlays(RoleType roleType);

    void setPlays(RoleType roleType, RoleType overriddenType);

    void unsetPlays(RoleType roleType);

    FunctionalIterator<? extends RoleType> getPlays();

    FunctionalIterator<? extends RoleType> getPlaysExplicit();

    RoleType getPlaysOverridden(RoleType roleType);
}
