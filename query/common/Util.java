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

package com.vaticle.typedb.core.query.common;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;

import java.util.Set;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;

public class Util {

    private static final String TRACE_PREFIX = "util.";

    public static RoleType getRoleType(Relation relation, Thing player, RelationConstraint.RolePlayer rolePlayer) {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_role_type")) {
            RoleType roleType;
            Set<? extends RoleType> inferred;
            if (rolePlayer.roleType().isPresent()) {
                RelationType relationType = relation.getType();
                TypeVariable var = rolePlayer.roleType().get();
                if ((roleType = relationType.getRelates(var.label().get().label())) == null) {
                    throw TypeDBException.of(TYPE_NOT_FOUND, Label.of(var.label().get().label(), relationType.getLabel().name()));
                }
            } else if ((inferred = player.getType().getPlays()
                    .filter(rt -> rt.getRelationType().equals(relation.getType()))
                    .toSet()).size() == 1) {
                roleType = inferred.iterator().next();
            } else if (inferred.size() > 1) {
                throw TypeDBException.of(ROLE_TYPE_AMBIGUOUS, rolePlayer.player().reference());
            } else {
                throw TypeDBException.of(ROLE_TYPE_MISSING, rolePlayer.player().reference());
            }
            return roleType;
        }
    }
}
