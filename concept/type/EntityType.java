/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.common.parameters.Concept.Existence;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.concept.thing.Entity;

public interface EntityType extends ThingType {

    @Override
    EntityType getSupertype();

    @Override
    Forwardable<? extends EntityType, Order.Asc> getSupertypes();

    @Override
    Forwardable<? extends EntityType, Order.Asc> getSubtypes();

    @Override
    Forwardable<? extends EntityType, Order.Asc> getSubtypes(Transitivity transitivity);

    @Override
    Forwardable<? extends Entity, Order.Asc> getInstances();

    @Override
    Forwardable<? extends Entity, Order.Asc> getInstances(Transitivity transitivity);

    void setSupertype(EntityType superType);

    Entity create();

    Entity create(Existence existence);
}
