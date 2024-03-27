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
import com.vaticle.typedb.core.concept.thing.Relation;


public interface RelationType extends ThingType {

    @Override
    RelationType getSupertype();

    @Override
    Forwardable<? extends RelationType, Order.Asc> getSupertypes();

    @Override
    Forwardable<? extends RelationType, Order.Asc> getSubtypes();

    @Override
    Forwardable<? extends RelationType, Order.Asc> getSubtypes(Transitivity transitivity);

    @Override
    Forwardable<? extends Relation, Order.Asc> getInstances();

    @Override
    Forwardable<? extends Relation, Order.Asc> getInstances(Transitivity transitivity);

    void setSupertype(RelationType superType);

    void setRelates(String roleLabel);

    void setRelates(String roleLabel, String overriddenLabel);

    void unsetRelates(String roleLabel);

    Forwardable<? extends RoleType, Order.Asc> getRelates();

    Forwardable<? extends RoleType, Order.Asc> getRelates(Transitivity transitivity);

    RoleType getRelates(String roleLabel);

    RoleType getRelates(Transitivity transitivity, String roleLabel);

    RoleType getRelatesOverridden(String roleLabel);

    Relation create();

    Relation create(Existence existence);
}
