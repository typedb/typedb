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
import ai.grakn.util.CommonUtil;

/**
 * Concept Reader for a Grakn Client
 */
public class ConceptConverter {

    public static Concept RPCToGraknConcept(RemoteGraknTx tx, GrpcConcept.Concept concept) {
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

    public static GrpcConcept.Concept GraknToRPCConcept(Concept concept) {
        return GrpcConcept.Concept.newBuilder()
                .setId(concept.getId().getValue())
                .setBaseType(getBaseType(concept))
                .build();
    }

    private static GrpcConcept.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return GrpcConcept.BaseType.ENTITY_TYPE;
        } else if (concept.isRelationshipType()) {
            return GrpcConcept.BaseType.RELATIONSHIP_TYPE;
        } else if (concept.isAttributeType()) {
            return GrpcConcept.BaseType.ATTRIBUTE_TYPE;
        } else if (concept.isEntity()) {
            return GrpcConcept.BaseType.ENTITY;
        } else if (concept.isRelationship()) {
            return GrpcConcept.BaseType.RELATIONSHIP;
        } else if (concept.isAttribute()) {
            return GrpcConcept.BaseType.ATTRIBUTE;
        } else if (concept.isRole()) {
            return GrpcConcept.BaseType.ROLE;
        } else if (concept.isRule()) {
            return GrpcConcept.BaseType.RULE;
        } else if (concept.isType()) {
            return GrpcConcept.BaseType.META_TYPE;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }
}
