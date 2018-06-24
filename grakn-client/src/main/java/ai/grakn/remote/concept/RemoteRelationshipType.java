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
import ai.grakn.remote.Transaction;
import ai.grakn.remote.rpc.ConceptBuilder;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteRelationshipType extends RemoteType<RelationshipType, Relationship> implements RelationshipType {

    public static RemoteRelationshipType create(Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRelationshipType(tx, id);
    }

    @Override
    public final Relationship addRelationship() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setAddRelationship(GrpcConcept.Unit.getDefaultInstance());
        GrpcGrakn.TxResponse response = runMethod(method.build());
        Concept concept = ConceptBuilder.concept(response.getConceptResponse().getConcept(), tx());

        return asInstance(concept);
    }

    @Override
    public final Stream<Role> relates() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetRelatedRoles(GrpcConcept.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asRole);
    }

    @Override
    public final RelationshipType relates(Role role) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setSetRelatedRole(ConceptBuilder.concept(role));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final RelationshipType deleteRelates(Role role) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setUnsetRelatedRole(ConceptBuilder.concept(role));
        runMethod(method.build());

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
