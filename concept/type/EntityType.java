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

import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.concept.thing.Entity;

public interface EntityType extends ThingType {

    @Override
    Seekable<? extends EntityType, Order.Asc> getSubtypes();

    @Override
    Seekable<? extends EntityType, Order.Asc> getSubtypesExplicit();

    @Override
    Seekable<? extends Entity, Order.Asc> getInstances();

    @Override
    Seekable<? extends Entity, Order.Asc> getInstancesExplicit();

    void setSupertype(EntityType superType);

    Entity create();

    Entity create(boolean isInferred);
}
