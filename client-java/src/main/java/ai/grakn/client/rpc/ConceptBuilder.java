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

package ai.grakn.client.rpc;

import ai.grakn.client.Grakn;
import ai.grakn.client.concept.RemoteAttribute;
import ai.grakn.client.concept.RemoteAttributeType;
import ai.grakn.client.concept.RemoteEntity;
import ai.grakn.client.concept.RemoteEntityType;
import ai.grakn.client.concept.RemoteMetaType;
import ai.grakn.client.concept.RemoteRelationship;
import ai.grakn.client.concept.RemoteRelationshipType;
import ai.grakn.client.concept.RemoteRole;
import ai.grakn.client.concept.RemoteRule;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.ComputeQueryImpl;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableMap;
import mjson.Json;

import java.text.NumberFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Concept Reader for a Grakn Client
 */
public class ConceptBuilder {

    public static Concept concept(ConceptProto.Concept concept, Grakn.Transaction tx) {
        ConceptId id = ConceptId.of(concept.getId());

        switch (concept.getBaseType()) {
            case ENTITY:
                return RemoteEntity.create(tx, id);
            case RELATIONSHIP:
                return RemoteRelationship.create(tx, id);
            case ATTRIBUTE:
                return RemoteAttribute.create(tx, id);
            case ENTITY_TYPE:
                return RemoteEntityType.create(tx, id);
            case RELATIONSHIP_TYPE:
                return RemoteRelationshipType.create(tx, id);
            case ATTRIBUTE_TYPE:
                return RemoteAttributeType.create(tx, id);
            case ROLE:
                return RemoteRole.create(tx, id);
            case RULE:
                return RemoteRule.create(tx, id);
            case META_TYPE:
                return RemoteMetaType.create(tx, id);
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }

    public static ConceptProto.Concept concept(Concept concept) {
        return ConceptProto.Concept.newBuilder()
                .setId(concept.getId().getValue())
                .setBaseType(getBaseType(concept))
                .build();
    }

    private static ConceptProto.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return ConceptProto.BaseType.ENTITY_TYPE;
        } else if (concept.isRelationshipType()) {
            return ConceptProto.BaseType.RELATIONSHIP_TYPE;
        } else if (concept.isAttributeType()) {
            return ConceptProto.BaseType.ATTRIBUTE_TYPE;
        } else if (concept.isEntity()) {
            return ConceptProto.BaseType.ENTITY;
        } else if (concept.isRelationship()) {
            return ConceptProto.BaseType.RELATIONSHIP;
        } else if (concept.isAttribute()) {
            return ConceptProto.BaseType.ATTRIBUTE;
        } else if (concept.isRole()) {
            return ConceptProto.BaseType.ROLE;
        } else if (concept.isRule()) {
            return ConceptProto.BaseType.RULE;
        } else if (concept.isType()) {
            return ConceptProto.BaseType.META_TYPE;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }

    public static ConceptProto.Concepts concepts(Collection<Concept> concepts) {
        ConceptProto.Concepts.Builder ConceptProtos = ConceptProto.Concepts.newBuilder();
        ConceptProtos.addAllConcepts(concepts.stream().map(ConceptBuilder::concept).collect(toList()));
        return ConceptProtos.build();
    }

    public static ConceptProto.AttributeValue attributeValue(Object value) {
        ConceptProto.AttributeValue.Builder builder = ConceptProto.AttributeValue.newBuilder();
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

    public static AttributeType.DataType<?> dataType(ConceptProto.DataType dataType) {
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

    static ConceptProto.DataType dataType(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.STRING)) {
            return ConceptProto.DataType.String;
        } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return ConceptProto.DataType.Boolean;
        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return ConceptProto.DataType.Integer;
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return ConceptProto.DataType.Long;
        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return ConceptProto.DataType.Float;
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return ConceptProto.DataType.Double;
        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return ConceptProto.DataType.Date;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
        }
    }

    public static Object answer(ConceptProto.Answer answer, Grakn.Transaction tx) {
        switch (answer.getAnswerCase()) {
            case QUERYANSWER:
                return queryAnswer(answer.getQueryAnswer(), tx);
            case COMPUTEANSWER:
                return computeAnswer(answer.getComputeAnswer());
            case OTHERRESULT:
                return Json.read(answer.getOtherResult()).getValue();
            default:
            case ANSWER_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + answer);
        }
    }

    public static Answer queryAnswer(ConceptProto.QueryAnswer queryAnswer, Grakn.Transaction tx) {
        ImmutableMap.Builder<Var, Concept> map = ImmutableMap.builder();

        queryAnswer.getQueryAnswerMap().forEach((grpcVar, ConceptProto) -> {
            map.put(Graql.var(grpcVar), concept(ConceptProto, tx));
        });

        return new QueryAnswer(map.build());
    }

    public static ComputeQuery.Answer computeAnswer(ConceptProto.ComputeAnswer computeAnswerRPC) {
        switch (computeAnswerRPC.getComputeAnswerCase()) {
            case NUMBER:
                try {
                    Number result = NumberFormat.getInstance().parse(computeAnswerRPC.getNumber());
                    return new ComputeQueryImpl.AnswerImpl().setNumber(result);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            case PATHS:
                return new ComputeQueryImpl.AnswerImpl().setPaths(paths(computeAnswerRPC.getPaths()));
            case CENTRALITY:
                return new ComputeQueryImpl.AnswerImpl().setCentrality(centrality(computeAnswerRPC.getCentrality()));
            case CLUSTERS:
                return new ComputeQueryImpl.AnswerImpl().setClusters(clusters(computeAnswerRPC.getClusters()));
            case CLUSTERSIZES:
                return new ComputeQueryImpl.AnswerImpl().setClusterSizes(clusterSizes(computeAnswerRPC.getClusterSizes()));
            default:
            case COMPUTEANSWER_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + computeAnswerRPC);
        }
    }

    public static List<List<ConceptId>> paths(ConceptProto.Paths pathsRPC) {
        List<List<ConceptId>> paths = new ArrayList<>(pathsRPC.getPathsList().size());

        for (ConceptProto.ConceptIds conceptIds : pathsRPC.getPathsList()) {
            paths.add(
                    conceptIds.getIdsList().stream()
                            .map(ConceptId::of)
                            .collect(toList())
            );
        }

        return paths;
    }

    public static Map<Long, Set<ConceptId>> centrality(ConceptProto.Centrality centralityRPC) {
        Map<Long, Set<ConceptId>> centrality = new HashMap<>();

        for (Map.Entry<Long, ConceptProto.ConceptIds> entry : centralityRPC.getCentralityMap().entrySet()) {
            centrality.put(
                    entry.getKey(),
                    entry.getValue().getIdsList().stream()
                            .map(ConceptId::of)
                            .collect(Collectors.toSet())
            );
        }

        return centrality;
    }

    public static Set<Set<ConceptId>> clusters(ConceptProto.Clusters clustersRPC) {
        Set<Set<ConceptId>> clusters = new HashSet<>();

        for (ConceptProto.ConceptIds conceptIds : clustersRPC.getClustersList()) {
            clusters.add(
                    conceptIds.getIdsList().stream()
                            .map(ConceptId::of)
                            .collect(Collectors.toSet())
            );
        }

        return clusters;
    }

    public static Set<Long> clusterSizes(ConceptProto.ClusterSizes clusterSizesRPC) {
        return new HashSet<>(clusterSizesRPC.getClusterSizesList());
    }
}
