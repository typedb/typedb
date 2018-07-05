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
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.MethodProto;
import ai.grakn.rpc.proto.SessionProto;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.RelationshipType}
 */
@AutoValue
public abstract class RemoteRelationshipType extends RemoteType<RelationshipType, Relationship> implements RelationshipType {

    public static RemoteRelationshipType create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRelationshipType(tx, id);
    }

    @Override
    public final Relationship addRelationship() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setAddRelationship(MethodProto.AddRelationship.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        Concept concept = ConceptBuilder.concept(response.getConceptMethod().getResponse().getAddRelationship().getConcept(), tx());

        return asInstance(concept);
    }

    @Override
    public final Stream<Role> relates() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetRelatedRoles(MethodProto.GetRelatedRoles.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetRelatedRoles().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asRole);
    }

    @Override
    public final RelationshipType relates(Role role) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetRelatedRole(MethodProto.SetRelatedRole.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final RelationshipType deleteRelates(Role role) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setUnsetRelatedRole(MethodProto.UnsetRelatedRole.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    final RelationshipType asCurrentBaseType(Concept other) {
        return other.asRelationshipType();
    }

    @Override
    final boolean equalsCurrentBaseType(Concept other) {
        return other.isRelationshipType();
    }

    @Override
    protected final Relationship asInstance(Concept concept) {
        return concept.asRelationship();
    }
}
