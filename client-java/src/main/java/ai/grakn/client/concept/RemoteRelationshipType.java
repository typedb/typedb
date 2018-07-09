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
import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.rpc.proto.ConceptProto;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.RelationshipType}
 */
@AutoValue
public abstract class RemoteRelationshipType extends RemoteType<RelationshipType, Relationship> implements RelationshipType {

    static RemoteRelationshipType construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRelationshipType(tx, id);
    }

    @Override
    public final Relationship create() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationTypeCreate(ConceptProto.RelationType.Create.Req.getDefaultInstance()).build();

        Concept concept = ConceptReader.concept(runMethod(method).getRelationTypeCreate().getConcept(), tx());

        return asInstance(concept);
    }

    @Override
    public final Stream<Role> roles() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationTypeRoles(ConceptProto.RelationType.Roles.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getRelationTypeRoles().getId();
        return conceptStream(iteratorId, res -> res.getRelationTypeRoles().getConcept()).map(Concept::asRole);
    }

    @Override
    public final RelationshipType relates(Role role) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationTypeRelates(ConceptProto.RelationType.Relates.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final RelationshipType unrelate(Role role) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRelationTypeUnrelate(ConceptProto.RelationType.Unrelate.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(role))).build();

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
