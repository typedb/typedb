/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;

public interface ThingType extends Type {

    @Override
    ThingType getSupertype();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSupertypes();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSubtypes();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSubtypesExplicit();

    Forwardable<? extends Thing, Order.Asc> getInstances();

    Forwardable<? extends Thing, Order.Asc> getInstancesExplicit();

    void setAbstract();

    void unsetAbstract();

    void setOwns(AttributeType attributeType);

    void setOwns(AttributeType attributeType, Set<TypeQLToken.Annotation> annotations);

    void setOwns(AttributeType attributeType, AttributeType overriddenType);

    void setOwns(AttributeType attributeType, AttributeType overriddenType, Set<TypeQLToken.Annotation> annotations);

    void unsetOwns(AttributeType attributeType);

    NavigableSet<Owns> getOwns();

    Optional<Owns> getOwns(AttributeType attributeType);

    Forwardable<Owns, Order.Asc> getOwns(Set<TypeQLToken.Annotation> annotations);

    Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType);

    Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType, Set<TypeQLToken.Annotation> annotations);

    NavigableSet<Owns> getOwnsExplicit();

    Optional<Owns> getOwnsExplicit(AttributeType attributeType);

    Forwardable<Owns, Order.Asc> getOwnsExplicit(Set<TypeQLToken.Annotation> annotations);

    Forwardable<Owns, Order.Asc> getOwnsExplicit(AttributeType.ValueType valueType);

    Forwardable<Owns, Order.Asc> getOwnsExplicit(AttributeType.ValueType valueType, Set<TypeQLToken.Annotation> annotations);

    AttributeType getOwnsOverridden(AttributeType attributeType);

    void setPlays(RoleType roleType);

    void setPlays(RoleType roleType, RoleType overriddenType);

    void unsetPlays(RoleType roleType);

    Forwardable<RoleType, Order.Asc> getPlays();

    Forwardable<RoleType, Order.Asc> getPlaysExplicit();

    RoleType getPlaysOverridden(RoleType roleType);

    java.lang.String getSyntax();

    void getSyntax(StringBuilder builder);

    void getSyntaxRecursive(StringBuilder builder);

    interface Owns extends Comparable<Owns> {

        AttributeType attributeType();

        Set<TypeQLToken.Annotation> effectiveAnnotations();

        Optional<AttributeType> overridden();

        void delete();

        void getSyntax(StringBuilder builder);
    }
}
