/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package grakn.core.client.concept;

import grakn.core.client.Grakn;
import grakn.core.client.rpc.RequestBuilder;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Relationship;
import grakn.core.concept.RelationshipType;
import grakn.core.concept.Role;
import grakn.core.concept.Thing;
import grakn.core.rpc.proto.ConceptProto;
import com.google.auto.value.AutoValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Client implementation of {@link grakn.core.concept.Relationship}
 */
@AutoValue
public abstract class RemoteRelationship extends RemoteThing<Relationship, RelationshipType> implements Relationship {

    static RemoteRelationship construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRelationship(tx, id);
    }

    @Override // TODO: Weird. Why is this not a stream, while other collections are returned as stream
    public final Map<Role, Set<Thing>> rolePlayersMap() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationRolePlayersMapReq(ConceptProto.Relation.RolePlayersMap.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getRelationRolePlayersMapIter().getId();
        Iterable<ConceptProto.Relation.RolePlayersMap.Iter.Res> rolePlayers = () -> new Grakn.Transaction.Iterator<>(
                tx(), iteratorId, res -> res.getConceptMethodIterRes().getRelationRolePlayersMapIterRes()
        );

        Map<Role, Set<Thing>> rolePlayerMap = new HashMap<>();
        for (ConceptProto.Relation.RolePlayersMap.Iter.Res rolePlayer : rolePlayers) {
            Role role = RemoteConcept.of(rolePlayer.getRole(), tx()).asRole();
            Thing player = RemoteConcept.of(rolePlayer.getPlayer(), tx()).asThing();
            if (rolePlayerMap.containsKey(role)) {
                rolePlayerMap.get(role).add(player);
            } else {
                rolePlayerMap.put(role, new HashSet<>(Collections.singletonList(player)));
            }
        }

        return rolePlayerMap;
    }

    @Override
    public final Stream<Thing> rolePlayers(Role... roles) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationRolePlayersReq(ConceptProto.Relation.RolePlayers.Req.newBuilder()
                        .addAllRoles(RequestBuilder.Concept.concepts(Arrays.asList(roles)))).build();

        int iteratorId = runMethod(method).getRelationRolePlayersIter().getId();
        return conceptStream(iteratorId, res -> res.getRelationRolePlayersIterRes().getThing()).map(Concept::asThing);
    }

    @Override
    public final Relationship assign(Role role, Thing player) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationAssignReq(ConceptProto.Relation.Assign.Req.newBuilder()
                        .setRole(RequestBuilder.Concept.concept(role))
                        .setPlayer(RequestBuilder.Concept.concept(player))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final void unassign(Role role, Thing player) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationUnassignReq(ConceptProto.Relation.Unassign.Req.newBuilder()
                        .setRole(RequestBuilder.Concept.concept(role))
                        .setPlayer(RequestBuilder.Concept.concept(player))).build();

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
