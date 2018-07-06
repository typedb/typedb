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

package ai.grakn.engine.rpc;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.AnswerProto;
import ai.grakn.rpc.proto.ConceptProto;
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
 */
public class ConceptBuilder {

    public static Concept concept(ConceptProto.Concept AnswerProto, EmbeddedGraknTx tx) {
        return tx.getConcept(ConceptId.of(AnswerProto.getId()));
    }

    public static ConceptProto.Concept concept(Concept concept) {
        return ConceptProto.Concept.newBuilder()
                .setId(concept.id().getValue())
                .setBaseType(getBaseType(concept))
                .build();
    }

    private static ConceptProto.Concept.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return ConceptProto.Concept.BaseType.ENTITY_TYPE;
        } else if (concept.isRelationshipType()) {
            return ConceptProto.Concept.BaseType.RELATIONSHIP_TYPE;
        } else if (concept.isAttributeType()) {
            return ConceptProto.Concept.BaseType.ATTRIBUTE_TYPE;
        } else if (concept.isEntity()) {
            return ConceptProto.Concept.BaseType.ENTITY;
        } else if (concept.isRelationship()) {
            return ConceptProto.Concept.BaseType.RELATIONSHIP;
        } else if (concept.isAttribute()) {
            return ConceptProto.Concept.BaseType.ATTRIBUTE;
        } else if (concept.isRole()) {
            return ConceptProto.Concept.BaseType.ROLE;
        } else if (concept.isRule()) {
            return ConceptProto.Concept.BaseType.RULE;
        } else if (concept.isType()) {
            return ConceptProto.Concept.BaseType.META_TYPE;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
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

    static ConceptProto.AttributeValue attributeValue(Object value) {
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

    static AnswerProto.Answer answer(Object object) {
        AnswerProto.Answer answer;

        if (object instanceof Answer) {
            answer = AnswerProto.Answer.newBuilder().setQueryAnswer(ConceptBuilder.queryAnswer((Answer) object)).build();
        } else if (object instanceof ComputeQuery.Answer) {
            answer = AnswerProto.Answer.newBuilder().setComputeAnswer(ConceptBuilder.computeAnswer((ComputeQuery.Answer) object)).build();
        } else {
            // If not an QueryAnswer or ComputeAnswer, convert to JSON
            answer = AnswerProto.Answer.newBuilder().setOtherResult(Printer.jsonPrinter().toString(object)).build();
        }

        return answer;
    }

    static AnswerProto.QueryAnswer queryAnswer(Answer answer) {
        AnswerProto.QueryAnswer.Builder queryAnswerRPC = AnswerProto.QueryAnswer.newBuilder();
        answer.forEach((var, concept) -> {
            ConceptProto.Concept conceptRps = concept(concept);
            queryAnswerRPC.putQueryAnswer(var.getValue(), conceptRps);
        });

        return queryAnswerRPC.build();
    }

    static AnswerProto.ComputeAnswer computeAnswer(ComputeQuery.Answer computeAnswer) {
        AnswerProto.ComputeAnswer.Builder computeAnswerRPC = AnswerProto.ComputeAnswer.newBuilder();

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

    private static AnswerProto.Paths paths(List<List<ConceptId>> paths) {
        AnswerProto.Paths.Builder pathsRPC = AnswerProto.Paths.newBuilder();
        for (List<ConceptId> path : paths) pathsRPC.addPaths(conceptIds(path));

        return pathsRPC.build();
    }

    private static AnswerProto.Centrality centralityCounts(Map<Long, Set<ConceptId>> centralityCounts) {
        AnswerProto.Centrality.Builder centralityCountsRPC = AnswerProto.Centrality.newBuilder();

        for (Map.Entry<Long, Set<ConceptId>> centralityCount : centralityCounts.entrySet()) {
            centralityCountsRPC.putCentrality(centralityCount.getKey(), conceptIds(centralityCount.getValue()));
        }

        return centralityCountsRPC.build();
    }

    private static AnswerProto.ClusterSizes clusterSizes(Collection<Long> clusterSizes) {
        AnswerProto.ClusterSizes.Builder clusterSizesRPC = AnswerProto.ClusterSizes.newBuilder();
        clusterSizesRPC.addAllClusterSizes(clusterSizes);

        return clusterSizesRPC.build();
    }

    private static AnswerProto.Clusters clusters(Collection<? extends Collection<ConceptId>> clusters) {
        AnswerProto.Clusters.Builder clustersRPC = AnswerProto.Clusters.newBuilder();
        for(Collection<ConceptId> cluster : clusters) clustersRPC.addClusters(conceptIds(cluster));

        return clustersRPC.build();
    }

    private static AnswerProto.ConceptIds conceptIds(Collection<ConceptId> conceptIds) {
        AnswerProto.ConceptIds.Builder conceptIdsRPC = AnswerProto.ConceptIds.newBuilder();
        conceptIdsRPC.addAllIds(conceptIds.stream()
                .map(id -> id.getValue())
                .collect(Collectors.toList()));

        return conceptIdsRPC.build();
    }
}
