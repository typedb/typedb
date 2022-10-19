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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;

public interface RoleType extends Type {

    @Override
    RoleType getSupertype();

    @Override
    Forwardable<? extends RoleType, Order.Asc> getSupertypes();

    @Override
    Forwardable<? extends RoleType, Order.Asc> getSubtypes();

    @Override
    Forwardable<? extends RoleType, Order.Asc> getSubtypesExplicit();

    RelationType getRelationType();

    Forwardable<? extends RelationType, Order.Asc> getRelationTypes();

    Forwardable<? extends ThingType, Order.Asc> getPlayerTypes();

    FunctionalIterator<? extends Relation> getRelationInstances();

    FunctionalIterator<? extends Relation> getRelationInstancesExplicit();

    FunctionalIterator<? extends Thing> getPlayerInstances();

    FunctionalIterator<? extends Thing> getPlayerInstancesExplicit();
}
