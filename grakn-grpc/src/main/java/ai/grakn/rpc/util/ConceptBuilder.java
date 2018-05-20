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

import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.util.CommonUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A utility class to build RPC Concepts by converting them from Grakn Concepts.
 *
 * @author Haikal Pribadi
 */
public class ConceptBuilder {
    public static GrpcConcept.Concept concept(Concept concept) {
        return GrpcConcept.Concept.newBuilder()
                .setId(GrpcConcept.ConceptId.newBuilder().setValue(concept.getId().getValue()).build())
                .setBaseType(getBaseType(concept))
                .build();
    }

    public static GrpcConcept.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return GrpcConcept.BaseType.EntityType;
        } else if (concept.isRelationshipType()) {
            return GrpcConcept.BaseType.RelationshipType;
        } else if (concept.isAttributeType()) {
            return GrpcConcept.BaseType.AttributeType;
        } else if (concept.isEntity()) {
            return GrpcConcept.BaseType.Entity;
        } else if (concept.isRelationship()) {
            return GrpcConcept.BaseType.Relationship;
        } else if (concept.isAttribute()) {
            return GrpcConcept.BaseType.Attribute;
        } else if (concept.isRole()) {
            return GrpcConcept.BaseType.Role;
        } else if (concept.isRule()) {
            return GrpcConcept.BaseType.Rule;
        } else if (concept.isType()) {
            return GrpcConcept.BaseType.MetaType;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }

    public static GrpcConcept.OptionalConcept optionalConcept(Optional<Concept> concept) {
        GrpcConcept.OptionalConcept.Builder builder = GrpcConcept.OptionalConcept.newBuilder();
        return concept
                .map(ConceptBuilder::concept)
                .map(builder::setPresent)
                .orElseGet(() -> builder.setAbsent(GrpcConcept.Unit.getDefaultInstance()))
                .build();
    }

    public static GrpcConcept.RolePlayer rolePlayer(RolePlayer rolePlayer) {
        return GrpcConcept.RolePlayer.newBuilder()
                .setRole(ConceptBuilder.concept(rolePlayer.role()))
                .setPlayer(ConceptBuilder.concept(rolePlayer.player()))
                .build();
    }

    public static GrpcConcept.Concepts concepts(Stream<? extends Concept> concepts) {
        GrpcConcept.Concepts.Builder grpcConcepts = GrpcConcept.Concepts.newBuilder();
        grpcConcepts.addAllConcept(concepts.map(ConceptBuilder::concept).collect(toList()));
        return grpcConcepts.build();
    }

    public static GrpcConcept.ConceptId conceptId(ConceptId id) {
        return GrpcConcept.ConceptId.newBuilder().setValue(id.getValue()).build();
    }

    public static GrpcConcept.Label label(Label label) {
        return GrpcConcept.Label.newBuilder().setValue(label.getValue()).build();
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

    public static GrpcConcept.DataType dataType(AttributeType.DataType<?> dataType) {
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

    public static GrpcConcept.OptionalRegex optionalRegex(Optional<String> regex) {
        GrpcConcept.OptionalRegex.Builder builder = GrpcConcept.OptionalRegex.newBuilder();
        return regex.map(builder::setPresent)
                .orElseGet(() -> builder.setAbsent(GrpcConcept.Unit.getDefaultInstance()))
                .build();
    }

    public static GrpcConcept.OptionalPattern optionalPattern(Optional<Pattern> pattern) {
        GrpcConcept.OptionalPattern.Builder builder = GrpcConcept.OptionalPattern.newBuilder();
        return pattern.map(ConceptBuilder::pattern)
                .map(builder::setPresent)
                .orElseGet(() -> builder.setAbsent(GrpcConcept.Unit.getDefaultInstance()))
                .build();
    }

    public static GrpcConcept.Pattern pattern(Pattern pattern) {
        return GrpcConcept.Pattern.newBuilder().setValue(pattern.toString()).build();
    }

    public static GrpcGrakn.TxType txType(GraknTxType txType) {
        switch (txType) {
            case READ:
                return GrpcGrakn.TxType.Read;
            case WRITE:
                return GrpcGrakn.TxType.Write;
            case BATCH:
                return GrpcGrakn.TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + txType);
        }
    }
}
