/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.rpc.ConceptMethod;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class RemoteRelationship extends RemoteThing<Relationship, RelationshipType> implements Relationship {

    public static RemoteRelationship create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteRelationship(tx, id);
    }

    @Override
    public final Map<Role, Set<Thing>> allRolePlayers() {
        return runMethod(ConceptMethod.GET_ROLE_PLAYERS)
                .collect(groupingBy(RolePlayer::role, mapping(RolePlayer::player, toSet())));
    }

    @Override
    public final Stream<Thing> rolePlayers(Role... roles) {
        if (roles.length == 0) {
            return runMethod(ConceptMethod.GET_ROLE_PLAYERS).map(rolePlayer -> rolePlayer.player());
        } else {
            return runMethod(ConceptMethod.getRolePlayersByRoles(roles)).map(Concept::asThing);
        }
    }

    @Override
    public final Relationship addRolePlayer(Role role, Thing thing) {
        return runVoidMethod(ConceptMethod.setRolePlayer(RolePlayer.create(role, thing)));
    }

    @Override
    public final void removeRolePlayer(Role role, Thing thing) {
        runVoidMethod(ConceptMethod.removeRolePlayer(RolePlayer.create(role, thing)));
    }

    @Override
    final RelationshipType asMyType(Concept concept) {
        return concept.asRelationshipType();
    }

    @Override
    final Relationship asSelf(Concept concept) {
        return concept.asRelationship();
    }
}
