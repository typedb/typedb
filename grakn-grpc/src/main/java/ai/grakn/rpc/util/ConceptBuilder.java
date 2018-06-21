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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.util.CommonUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    static GrpcConcept.AttributeValue attributeValue(Object value) {
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

    static GrpcGrakn.Answer answer(Object object) {
        GrpcGrakn.Answer answer;

        if (object instanceof Answer) {
            answer = GrpcGrakn.Answer.newBuilder().setQueryAnswer(ConceptBuilder.queryAnswer((Answer) object)).build();
        } else if (object instanceof ComputeQuery.Answer) {
            answer = GrpcGrakn.Answer.newBuilder().setComputeAnswer(ConceptBuilder.computeAnswer((ComputeQuery.Answer) object)).build();
        } else {
            // If not an QueryAnswer or ComputeAnswer, convert to JSON
            answer = GrpcGrakn.Answer.newBuilder().setOtherResult(Printer.jsonPrinter().toString(object)).build();
        }

        return answer;
    }

    static GrpcGrakn.QueryAnswer queryAnswer(Answer answer) {
        GrpcGrakn.QueryAnswer.Builder queryAnswerRPC = GrpcGrakn.QueryAnswer.newBuilder();
        answer.forEach((var, concept) -> {
            GrpcConcept.Concept conceptRps = concept(concept);
            queryAnswerRPC.putQueryAnswer(var.getValue(), conceptRps);
        });

        return queryAnswerRPC.build();
    }

    static GrpcGrakn.ComputeAnswer computeAnswer(ComputeQuery.Answer computeAnswer) {
        GrpcGrakn.ComputeAnswer.Builder computeAnswerRPC = GrpcGrakn.ComputeAnswer.newBuilder();

        if (computeAnswer.getNumber().isPresent()) {
            computeAnswerRPC.setNumber(computeAnswer.getNumber().get().toString());
        }
        else if (computeAnswer.getPaths().isPresent()) {
             computeAnswerRPC.setPaths(paths(computeAnswer.getPaths().get()));
        }
        else if (computeAnswer.getCentrality().isPresent()) {
            computeAnswerRPC.setCentrality(centralityCounts(computeAnswer.getCentrality().get()));
        }
        else if (computeAnswer.getClusters().isPresent()) {
            computeAnswerRPC.setClusters(clusters(computeAnswer.getClusters().get()));
        }
        else if (computeAnswer.getClusterSizes().isPresent()) {
            computeAnswerRPC.setClusterSizes(clusterSizes(computeAnswer.getClusterSizes().get()));
        }

        return computeAnswerRPC.build();
    }

    private static GrpcGrakn.Paths paths(List<List<ConceptId>> paths) {
        GrpcGrakn.Paths.Builder pathsRPC = GrpcGrakn.Paths.newBuilder();
        for (List<ConceptId> path : paths) pathsRPC.addPaths(conceptIds(path));

        return pathsRPC.build();
    }

    private static GrpcGrakn.Centrality centralityCounts(Map<Long, Set<ConceptId>> centralityCounts) {
        GrpcGrakn.Centrality.Builder centralityCountsRPC = GrpcGrakn.Centrality.newBuilder();

        for (Map.Entry<Long, Set<ConceptId>> centralityCount : centralityCounts.entrySet()) {
            centralityCountsRPC.putCentrality(centralityCount.getKey(), conceptIds(centralityCount.getValue()));
        }

        return centralityCountsRPC.build();
    }

    private static GrpcGrakn.ClusterSizes clusterSizes(Collection<Long> clusterSizes) {
        GrpcGrakn.ClusterSizes.Builder clusterSizesRPC = GrpcGrakn.ClusterSizes.newBuilder();
        clusterSizesRPC.addAllClusterSizes(clusterSizes);

        return clusterSizesRPC.build();
    }

    private static GrpcGrakn.Clusters clusters(Collection<? extends Collection<ConceptId>> clusters) {
        GrpcGrakn.Clusters.Builder clustersRPC = GrpcGrakn.Clusters.newBuilder();
        for(Collection<ConceptId> cluster : clusters) clustersRPC.addClusters(conceptIds(cluster));

        return clustersRPC.build();
    }

    private static GrpcConcept.ConceptIds conceptIds(Collection<ConceptId> conceptIds) {
        GrpcConcept.ConceptIds.Builder conceptIdsRPC = GrpcConcept.ConceptIds.newBuilder();
        conceptIdsRPC.addAllIds(conceptIds.stream()
                .map(id -> id.getValue())
                .collect(Collectors.toList()));

        return conceptIdsRPC.build();
    }
}
