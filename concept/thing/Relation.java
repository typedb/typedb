/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.thing;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.common.parameters.Concept.Existence;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;

import java.util.List;
import java.util.Map;

public interface Relation extends Thing {

    @Override
    RelationType getType();

    void addPlayer(RoleType roleType, Thing player);

    void addPlayer(RoleType roleType, Thing player, Existence existence);

    void removePlayer(RoleType roleType, Thing player);

    FunctionalIterator<Thing> getPlayers(String... roleTypes);

    Forwardable<Thing, Order.Asc> getPlayers(RoleType roleType, RoleType... roleTypes);

    // TODO: This method should just return FunctionalIterator<Pair<RoleType, Thing>>
    Map<? extends RoleType, List<Thing>> getPlayersByRoleType();

    FunctionalIterator<RoleType> getRelating();
}
