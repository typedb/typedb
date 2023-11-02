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
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;

public interface ThingType extends Type {

    @Override
    ThingType getSupertype();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSupertypes();

    Forwardable<? extends ThingTypeImpl, Order.Asc> getSupertypesWithThing();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSubtypes();

    @Override
    Forwardable<? extends ThingType, Order.Asc> getSubtypes(Transitivity transitivity);

    Forwardable<? extends Thing, Order.Asc> getInstances();

    Forwardable<? extends Thing, Order.Asc> getInstances(Transitivity transitivity);

    void setAbstract();

    void unsetAbstract();

    void setOwns(AttributeType attributeType);

    void setOwns(AttributeType attributeType, Set<Annotation> annotations);

    void setOwns(AttributeType attributeType, AttributeType overriddenType);

    void setOwns(AttributeType attributeType, AttributeType overriddenType, Set<Annotation> annotations);

    void unsetOwns(AttributeType attributeType);

    NavigableSet<Owns> getOwns();

    Optional<Owns> getOwns(AttributeType attributeType);

    Forwardable<Owns, Order.Asc> getOwns(Set<Annotation> annotations);

    Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType);

    Forwardable<Owns, Order.Asc> getOwns(AttributeType.ValueType valueType, Set<Annotation> annotations);

    NavigableSet<Owns> getOwns(Transitivity transitivity);

    Optional<Owns> getOwns(Transitivity transitivity, AttributeType attributeType);

    Forwardable<Owns, Order.Asc> getOwns(Transitivity transitivity, Set<Annotation> annotations);

    Forwardable<Owns, Order.Asc> getOwns(Transitivity transitivity, AttributeType.ValueType valueType);

    Forwardable<Owns, Order.Asc> getOwns(Transitivity transitivity, AttributeType.ValueType valueType, Set<Annotation> annotations);

    Set<AttributeType> getOwnedAttributes(Transitivity transitivity);

    AttributeType getOwnsOverridden(AttributeType attributeType);

    void setPlays(RoleType roleType);

    void setPlays(RoleType roleType, RoleType overriddenType);

    void unsetPlays(RoleType roleType);

    boolean plays(RoleType roleType);

    Forwardable<RoleType, Order.Asc> getPlays();

    Forwardable<RoleType, Order.Asc> getPlays(Transitivity transitivity);

    RoleType getPlaysOverridden(RoleType roleType);

    java.lang.String getSyntax();

    void getSyntax(StringBuilder builder);

    void getSyntaxRecursive(StringBuilder builder);

    interface Owns extends Comparable<Owns> {

        AttributeType attributeType();

        Set<Annotation> effectiveAnnotations();

        Optional<AttributeType> overridden();

        void delete();

        void getSyntax(StringBuilder builder);
    }
}
