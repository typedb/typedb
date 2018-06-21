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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.remote.concept.RemoteConcepts;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.util.CommonUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Concept Reader for a Grakn Client
 */
public class ConceptConverter {

    public static Concept concept(RemoteGraknTx tx, GrpcConcept.Concept concept) {
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

    public static GrpcConcept.Concept concept(Concept concept) {
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

    public static GrpcConcept.Concepts concepts(Stream<? extends Concept> concepts) {
        GrpcConcept.Concepts.Builder grpcConcepts = GrpcConcept.Concepts.newBuilder();
        grpcConcepts.addAllConcepts(concepts.map(ConceptConverter::concept).collect(toList()));
        return grpcConcepts.build();
    }

    public static GrpcConcept.AttributeValue attributeValue(Object value) {
        GrpcConcept.AttributeValue.Builder builder = GrpcConcept.AttributeValue.newBuilder();
        if (value instanceof String) {
            builder.setString((String) value);
        } else if (value instanceof Boolean) {
            builder.setBoolean((boolean) value);
        } else if (value instanceof Integer) {
            builder.setInteger((int) value);
        } else if (value instanceof Long) {
            builder.setLong((long) value);
        } else if (value instanceof Float) {
            builder.setFloat((float) value);
        } else if (value instanceof Double) {
            builder.setDouble((double) value);
        } else if (value instanceof LocalDateTime) {
            builder.setDate(((LocalDateTime) value).atZone(ZoneId.of("Z")).toInstant().toEpochMilli());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + value);
        }

        return builder.build();
    }


    public static AttributeType.DataType<?> dataType(GrpcConcept.DataType dataType) {
        switch (dataType) {
            case String:
                return AttributeType.DataType.STRING;
            case Boolean:
                return AttributeType.DataType.BOOLEAN;
            case Integer:
                return AttributeType.DataType.INTEGER;
            case Long:
                return AttributeType.DataType.LONG;
            case Float:
                return AttributeType.DataType.FLOAT;
            case Double:
                return AttributeType.DataType.DOUBLE;
            case Date:
                return AttributeType.DataType.DATE;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + dataType);
        }
    }

    static GrpcConcept.DataType dataType(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.STRING)) {
            return GrpcConcept.DataType.String;
        } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return GrpcConcept.DataType.Boolean;
        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return GrpcConcept.DataType.Integer;
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return GrpcConcept.DataType.Long;
        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return GrpcConcept.DataType.Float;
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return GrpcConcept.DataType.Double;
        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return GrpcConcept.DataType.Date;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
        }
    }
}
