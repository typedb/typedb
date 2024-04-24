/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
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
    Forwardable<? extends RoleType, Order.Asc> getSubtypes(Transitivity transitivity);

    RelationType getRelationType();

    Forwardable<? extends RelationType, Order.Asc> getRelationTypes();

    Forwardable<? extends ThingType, Order.Asc> getPlayerTypes();

    Forwardable<? extends ThingType, Order.Asc> getPlayerTypes(Transitivity transitivity);

    FunctionalIterator<? extends Relation> getRelationInstances();

    FunctionalIterator<? extends Relation> getRelationInstances(Transitivity transitivity);

    FunctionalIterator<? extends Thing> getPlayerInstances();

    FunctionalIterator<? extends Thing> getPlayerInstances(Transitivity transitivity);
}
