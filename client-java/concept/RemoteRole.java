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

import grakn.core.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.RelationType;
import grakn.core.concept.Role;
import grakn.core.concept.Type;
import grakn.core.protocol.ConceptProto;

import java.util.stream.Stream;

/**
 * Client implementation of Role
 */
public class RemoteRole extends RemoteSchemaConcept<Role> implements Role {

    RemoteRole(GraknClient.Transaction tx, ConceptId id) {
        super(tx, id);
    }

    static RemoteRole construct(GraknClient.Transaction tx, ConceptId id) {
        return new RemoteRole(tx, id);
    }

    @Override
    public final Stream<RelationType> relations() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRoleRelationsReq(ConceptProto.Role.Relations.Req.getDefaultInstance()).build();
        int iteratorId = runMethod(method).getRoleRelationsIter().getId();
        return conceptStream(iteratorId, res -> res.getRoleRelationsIterRes().getRelationType()).map(Concept::asRelationType);
    }

    @Override
    public final Stream<Type> players() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRolePlayersReq(ConceptProto.Role.Players.Req.getDefaultInstance()).build();
        int iteratorId = runMethod(method).getRolePlayersIter().getId();
        return conceptStream(iteratorId, res -> res.getRolePlayersIterRes().getType()).map(Concept::asType);
    }

    @Override
    final Role asCurrentBaseType(Concept other) {
        return other.asRole();
    }

    @Override
    final boolean equalsCurrentBaseType(Concept other) {
        return other.isRole();
    }
}
