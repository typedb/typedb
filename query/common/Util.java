/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.query.common;

import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.variable.TypeVariable;

import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static java.util.stream.Collectors.toSet;

public class Util {

    private static final String TRACE_PREFIX = "util.";

    public static RoleType getRoleType(Relation relation, Thing player, RelationConstraint.RolePlayer rolePlayer) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_role_type")) {
            RoleType roleType;
            Set<RoleType> inferred;
            if (rolePlayer.roleType().isPresent()) {
                RelationType relationType = relation.getType();
                TypeVariable var = rolePlayer.roleType().get();
                if ((roleType = relationType.getRelates(var.label().get().label())) == null) {
                    throw GraknException.of(TYPE_NOT_FOUND, Label.of(var.label().get().label(), relationType.getLabel().name()));
                }
            } else if ((inferred = player.getType().getPlays()
                    .filter(rt -> rt.getRelationType().equals(relation.getType()))
                    .collect(toSet())).size() == 1) {
                roleType = inferred.iterator().next();
            } else if (inferred.size() > 1) {
                throw GraknException.of(ROLE_TYPE_AMBIGUOUS, rolePlayer.player().reference());
            } else {
                throw GraknException.of(ROLE_TYPE_MISSING, rolePlayer.player().reference());
            }
            return roleType;
        }
    }
}
