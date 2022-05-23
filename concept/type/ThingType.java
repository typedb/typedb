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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.concept.thing.Thing;

public interface ThingType extends Type {

    @Override
    ThingType getSupertype();

    @Override
    FunctionalIterator<? extends ThingType> getSupertypes();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSubtypes();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSubtypesExplicit();

    Forwardable<? extends Thing, Order.Asc> getInstances();

    Forwardable<? extends Thing, Order.Asc> getInstancesExplicit();

    void setAbstract();

    void unsetAbstract();

    void setOwns(AttributeType attributeType);

    void setOwns(AttributeType attributeType, boolean isKey);

    void setOwns(AttributeType attributeType, AttributeType overriddenType);

    void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey);

    void unsetOwns(AttributeType attributeType);

    Forwardable<AttributeType, Order.Asc> getOwns();

    Forwardable<AttributeType, Order.Asc> getOwnsExplicit();

    Forwardable<AttributeType, Order.Asc> getOwns(boolean onlyKey);

    Forwardable<AttributeType, Order.Asc> getOwnsExplicit(boolean onlyKey);

    Forwardable<AttributeType, Order.Asc> getOwns(AttributeType.ValueType valueType);

    Forwardable<AttributeType, Order.Asc> getOwnsExplicit(AttributeType.ValueType valueType);

    Forwardable<AttributeType, Order.Asc> getOwns(AttributeType.ValueType valueType, boolean onlyKey);

    Forwardable<AttributeType, Order.Asc> getOwnsExplicit(AttributeType.ValueType valueType, boolean onlyKey);

    AttributeType getOwnsOverridden(AttributeType attributeType);

    void setPlays(RoleType roleType);

    void setPlays(RoleType roleType, RoleType overriddenType);

    void unsetPlays(RoleType roleType);

    Forwardable<RoleType, Order.Asc> getPlays();

    Forwardable<RoleType, Order.Asc> getPlaysExplicit();

    RoleType getPlaysOverridden(RoleType roleType);

    java.lang.String getSyntax();

    void getSyntax(StringBuilder builder);
}
