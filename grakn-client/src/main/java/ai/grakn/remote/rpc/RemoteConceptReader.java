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

package ai.grakn.remote.rpc;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.remote.concept.RemoteConcepts;
import ai.grakn.rpc.generated.GrpcConcept;

/**
 * Concept Reader for a Grakn Client
 */
public class RemoteConceptReader {

    private final RemoteGraknTx tx;

    public RemoteConceptReader(RemoteGraknTx tx){
        this.tx = tx;
    }

    public Concept concept(GrpcConcept.Concept concept) {
        ConceptId id = ConceptId.of(concept.getId());

        switch (concept.getBaseType()) {
            case ENTITY:
                return RemoteConcepts.createEntity(tx, id);
            case RELATIONSHIP:
                return RemoteConcepts.createRelationship(tx, id);
            case ATTRIBUTE:
                return RemoteConcepts.createAttribute(tx, id);
            case ENTITY_TYPE:
                return RemoteConcepts.createEntityType(tx, id);
            case RELATIONSHIP_TYPE:
                return RemoteConcepts.createRelationshipType(tx, id);
            case ATTRIBUTE_TYPE:
                return RemoteConcepts.createAttributeType(tx, id);
            case ROLE:
                return RemoteConcepts.createRole(tx, id);
            case RULE:
                return RemoteConcepts.createRule(tx, id);
            case META_TYPE:
                return RemoteConcepts.createMetaType(tx, id);
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }
}
