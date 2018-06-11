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
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.remote.rpc.RemoteIterator;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.rpc.util.ConceptBuilder;
import com.google.auto.value.AutoValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class RemoteRelationship extends RemoteThing<Relationship, RelationshipType> implements Relationship {

    public static RemoteRelationship create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteRelationship(tx, id);
    }

    @Override // TODO: Weird. Why is this not a stream, while other collections are returned as stream
    public final Map<Role, Set<Thing>> allRolePlayers() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetRolePlayers(GrpcConcept.Unit.getDefaultInstance());
        GrpcGrakn.TxResponse response = runMethod(method.build());

        GrpcIterator.IteratorId iteratorId = response.getConceptResponse().getIteratorId();
        Iterable<RolePlayer> rolePlayers = () -> new RemoteIterator<>(
                tx(), iteratorId, res -> tx().conceptReader().rolePlayer(res.getRolePlayer())
        );

        Map<Role, Set<Thing>> rolePlayerMap = new HashMap<>();
        for (RolePlayer rolePlayer : rolePlayers) {
            if (rolePlayerMap.containsKey(rolePlayer.role())) {
                rolePlayerMap.get(rolePlayer.role()).add(rolePlayer.player());
            } else {
                rolePlayerMap.put(rolePlayer.role(), Collections.singleton(rolePlayer.player()));
            }
        }

        return rolePlayerMap;
    }

    @Override // TODO: remove (roles.length==0){...} behavior as it is semantically the same as allRolePlayers() above
    public final Stream<Thing> rolePlayers(Role... roles) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        if (roles.length == 0) {
            method.setGetRolePlayers(GrpcConcept.Unit.getDefaultInstance());
        } else {
            method.setGetRolePlayersByRoles(ConceptBuilder.concepts(Stream.of(roles)));
        }

        GrpcGrakn.TxResponse response = runMethod(method.build());
        GrpcIterator.IteratorId iteratorId = response.getConceptResponse().getIteratorId();
        Iterable<RolePlayer> rolePlayers = () -> new RemoteIterator<>(
                tx(), iteratorId, res -> tx().conceptReader().rolePlayer(res.getRolePlayer())
        );

        return StreamSupport.stream(rolePlayers.spliterator(), false).map(RolePlayer::player);
    }

    @Override
    public final Relationship addRolePlayer(Role role, Thing thing) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setSetRolePlayer(ConceptBuilder.rolePlayer(RolePlayer.create(role, thing)));
        runMethod(method.build());

        return asSelf(this);
    }

    @Override
    public final void removeRolePlayer(Role role, Thing thing) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setUnsetRolePlayer(ConceptBuilder.rolePlayer(RolePlayer.create(role, thing)));
        runMethod(method.build());
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
