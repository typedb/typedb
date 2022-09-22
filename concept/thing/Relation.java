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

package com.vaticle.typedb.core.concept.thing;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;

import java.util.List;
import java.util.Map;

public interface Relation extends Thing {

    @Override
    RelationType getType();

    void addPlayer(RoleType roleType, Thing player);

    void addPlayer(RoleType roleType, Thing player, boolean isInferred);

    void removePlayer(RoleType roleType, Thing player);

    FunctionalIterator<Thing> getPlayers(String... roleTypes);

    Forwardable<Thing, Order.Asc> getPlayers(RoleType roleType, RoleType... roleTypes);

    // TODO: This method should just return FunctionalIterator<Pair<RoleType, Thing>>
    Map<? extends RoleType, List<Thing>> getPlayersByRoleType();

    FunctionalIterator<? extends RoleType> getRelating();
}
