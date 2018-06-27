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

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.client.Grakn;
import ai.grakn.rpc.proto.ConceptProto;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteRole extends RemoteSchemaConcept<Role> implements Role {

    public static RemoteRole create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRole(tx, id);
    }

    @Override
    public final Stream<RelationshipType> relationshipTypes() {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setGetRelationshipTypesThatRelateRole(ConceptProto.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asRelationshipType);
    }

    @Override
    public final Stream<Type> playedByTypes() {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setGetTypesThatPlayRole(ConceptProto.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asType);
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
