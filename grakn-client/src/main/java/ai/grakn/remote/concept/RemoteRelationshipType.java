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
import ai.grakn.rpc.ConceptMethods;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class RemoteRelationshipType extends RemoteType<RelationshipType, Relationship> implements RelationshipType {

    public static RemoteRelationshipType create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteRelationshipType(tx, id);
    }

    @Override
    public final Relationship addRelationship() {
        return asInstance(runMethod(ConceptMethods.ADD_RELATIONSHIP));
    }

    @Override
    public final Stream<Role> relates() {
        return runMethod(ConceptMethods.GET_RELATED_ROLES).map(Concept::asRole);
    }

    @Override
    public final RelationshipType relates(Role role) {
        return runVoidMethod(ConceptMethods.setRelatedRole(role));
    }

    @Override
    public final RelationshipType deleteRelates(Role role) {
        return runVoidMethod(ConceptMethods.unsetRelatedRole(role));
    }

    @Override
    final RelationshipType asSelf(Concept concept) {
        return concept.asRelationshipType();
    }

    @Override
    final boolean isSelf(Concept concept) {
        return concept.isRelationshipType();
    }

    @Override
    protected final Relationship asInstance(Concept concept) {
        return concept.asRelationship();
    }
}
