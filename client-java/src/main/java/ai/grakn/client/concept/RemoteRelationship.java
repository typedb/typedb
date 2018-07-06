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

package ai.grakn.client.concept;

import ai.grakn.client.Grakn;
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.IteratorProto;
import com.google.auto.value.AutoValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Client implementation of {@link ai.grakn.concept.Relationship}
 */
@AutoValue
public abstract class RemoteRelationship extends RemoteThing<Relationship, RelationshipType> implements Relationship {

    public static RemoteRelationship create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRelationship(tx, id);
    }

    @Override // TODO: Weird. Why is this not a stream, while other collections are returned as stream
    public final Map<Role, Set<Thing>> rolePlayersMap() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetRolePlayers(ConceptProto.GetRolePlayers.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetRolePlayers().getIteratorId();
        Iterable<ConceptProto.RolePlayer> rolePlayers = () -> new Grakn.Transaction.Iterator<>(
                tx(), iteratorId, res -> res.getRolePlayer()
        );

        Map<Role, Set<Thing>> rolePlayerMap = new HashMap<>();
        for (ConceptProto.RolePlayer rolePlayer : rolePlayers) {
            Role role = ConceptBuilder.concept(rolePlayer.getRole(), tx()).asRole();
            Thing player = ConceptBuilder.concept(rolePlayer.getPlayer(), tx()).asThing();
            if (rolePlayerMap.containsKey(role)) {
                rolePlayerMap.get(role).add(player);
            } else {
                rolePlayerMap.put(role, Collections.singleton(player));
            }
        }

        return rolePlayerMap;
    }

    @Override
    public final Stream<Thing> rolePlayers(Role... roles) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetRolePlayersByRoles(ConceptProto.GetRolePlayersByRoles.Req.newBuilder()
                        .addAllConcepts(ConceptBuilder.concepts(Arrays.asList(roles)))).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetRolePlayersByRoles().getIteratorId();
        Iterable<Thing> rolePlayers = () -> new Grakn.Transaction.Iterator<>(
                tx(), iteratorId, res -> ConceptBuilder.concept(res.getConcept(), tx()).asThing()
        );

        return StreamSupport.stream(rolePlayers.spliterator(), false);
    }

    @Override
    public final Relationship assign(Role role, Thing player) {
        ConceptProto.RolePlayer rolePlayer = ConceptProto.RolePlayer.newBuilder()
                .setRole(ConceptBuilder.concept(role))
                .setPlayer(ConceptBuilder.concept(player))
                .build();
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSetRolePlayer(ConceptProto.SetRolePlayer.Req.newBuilder()
                        .setRolePlayer(rolePlayer)).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final void unassign(Role role, Thing player) {
        ConceptProto.RolePlayer rolePlayer = ConceptProto.RolePlayer.newBuilder()
                .setRole(ConceptBuilder.concept(role))
                .setPlayer(ConceptBuilder.concept(player))
                .build();
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setUnsetRolePlayer(ConceptProto.UnsetRolePlayer.Req.newBuilder()
                        .setRolePlayer(rolePlayer)).build();

        runMethod(method);
    }

    @Override
    final RelationshipType asCurrentType(Concept concept) {
        return concept.asRelationshipType();
    }

    @Override
    final Relationship asCurrentBaseType(Concept other) {
        return other.asRelationship();
    }
}
