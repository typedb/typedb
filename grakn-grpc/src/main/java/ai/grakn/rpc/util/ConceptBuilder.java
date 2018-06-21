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

package ai.grakn.rpc.util;

import ai.grakn.concept.Concept;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.util.CommonUtil;

import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A utility class to build RPC Concepts by converting them from Grakn Concepts.
 *
 * @author Grakn Warriors
 */
public class ConceptBuilder {
    public static GrpcConcept.Concept concept(Concept concept) {
        return GrpcConcept.Concept.newBuilder()
                .setId(concept.getId().getValue())
                .setBaseType(getBaseType(concept))
                .build();
    }

    public static GrpcConcept.BaseType getBaseType(Concept concept) {
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
    public static GrpcConcept.Concepts concepts(Stream<? extends Concept> concepts) {
        GrpcConcept.Concepts.Builder grpcConcepts = GrpcConcept.Concepts.newBuilder();
        grpcConcepts.addAllConcepts(concepts.map(ConceptBuilder::concept).collect(toList()));
        return grpcConcepts.build();
    }


}
