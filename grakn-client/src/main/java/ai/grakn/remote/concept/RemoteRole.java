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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.grpc.ConceptMethods;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class RemoteRole extends RemoteSchemaConcept<Role> implements Role {

    public static RemoteRole create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteRole(tx, id);
    }

    @Override
    public final Stream<RelationshipType> relationshipTypes() {
        return runMethod(ConceptMethods.GET_RELATIONSHIP_TYPES_THAT_RELATE_ROLE).map(Concept::asRelationshipType);
    }

    @Override
    public final Stream<Type> playedByTypes() {
        return runMethod(ConceptMethods.GET_TYPES_THAT_PLAY_ROLE).map(Concept::asType);
    }

    @Override
    final Role asSelf(Concept concept) {
        return concept.asRole();
    }

    @Override
    final boolean isSelf(Concept concept) {
        return concept.isRole();
    }
}
