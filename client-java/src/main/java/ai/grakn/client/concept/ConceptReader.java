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
import ai.grakn.rpc.proto.ConceptProto;

/**
 * A utility class for a Grakn Client to take a {@link ConceptProto.Concept} and convert it into a {@link RemoteConcept}
 * by reading from the database.
 */
public class ConceptReader {

    public static Concept concept(ConceptProto.Concept concept, Grakn.Transaction tx) {
        ConceptId id = ConceptId.of(concept.getId());

        switch (concept.getBaseType()) {
            case ENTITY:
                return RemoteEntity.construct(tx, id);
            case RELATION:
                return RemoteRelationship.construct(tx, id);
            case ATTRIBUTE:
                return RemoteAttribute.construct(tx, id);
            case ENTITY_TYPE:
                return RemoteEntityType.construct(tx, id);
            case RELATION_TYPE:
                return RemoteRelationshipType.construct(tx, id);
            case ATTRIBUTE_TYPE:
                return RemoteAttributeType.construct(tx, id);
            case ROLE:
                return RemoteRole.construct(tx, id);
            case RULE:
                return RemoteRule.construct(tx, id);
            case META_TYPE:
                return RemoteMetaType.construct(tx, id);
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }

}
