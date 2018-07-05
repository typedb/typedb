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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.MethodProto;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.Role}
 */
@AutoValue
public abstract class RemoteRole extends RemoteSchemaConcept<Role> implements Role {

    public static RemoteRole create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRole(tx, id);
    }

    @Override
    public final Stream<RelationshipType> relationshipTypes() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetRelationshipTypesThatRelateRole(MethodProto.GetRelationshipTypesThatRelateRole.Req.getDefaultInstance()).build();
        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetRelationshipTypesThatRelateRole().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asRelationshipType);
    }

    @Override
    public final Stream<Type> playedByTypes() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetTypesThatPlayRole(MethodProto.GetTypesThatPlayRole.Req.getDefaultInstance()).build();
        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetTypesThatPlayRole().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asType);
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
