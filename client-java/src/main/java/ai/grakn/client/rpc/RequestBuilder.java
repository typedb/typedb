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

import ai.grakn.GraknTxType;
import ai.grakn.client.Grakn;
import ai.grakn.client.concept.RemoteConcept;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ConceptMap;
import ai.grakn.graql.internal.query.ComputeQueryImpl;
import ai.grakn.graql.internal.query.ConceptMapImpl;
import ai.grakn.rpc.proto.AnswerProto;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.KeyspaceProto;
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
 * A utility class to build RPC Requests from a provided set of Grakn concepts.
 */
public class RequestBuilder {

    /**
     * An RPC Request Builder class for Transaction Service
     */
    public static class Transaction {

        public static SessionProto.Transaction.Req open(ai.grakn.Keyspace keyspace, GraknTxType txType) {
            SessionProto.Transaction.Open.Req openRequest = SessionProto.Transaction.Open.Req.newBuilder()
                    .setKeyspace(keyspace.getValue())
                    .setType(SessionProto.Transaction.Type.valueOf(txType.getId()))
                    .build();

            return SessionProto.Transaction.Req.newBuilder().setOpenReq(openRequest).build();
        }

        public static SessionProto.Transaction.Req commit() {
            return SessionProto.Transaction.Req.newBuilder()
                    .setCommitReq(SessionProto.Transaction.Commit.Req.getDefaultInstance())
                    .build();
        }

        public static SessionProto.Transaction.Req query(Query<?> query) {
            return query(query.toString(), query.inferring());
        }

        public static SessionProto.Transaction.Req query(String queryString, boolean infer) {
            SessionProto.Transaction.Query.Req request = SessionProto.Transaction.Query.Req.newBuilder()
                    .setQuery(queryString)
                    .setInfer(infer)
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setQueryReq(request).build();
        }

        public static SessionProto.Transaction.Req getSchemaConcept(Label label) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setGetSchemaConceptReq(SessionProto.Transaction.GetSchemaConcept.Req.newBuilder().setLabel(label.getValue()))
                    .build();
        }

        public static SessionProto.Transaction.Req getConcept(ConceptId id) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setGetConceptReq(SessionProto.Transaction.GetConcept.Req.newBuilder().setId(id.getValue()))
                    .build();
        }


        public static SessionProto.Transaction.Req getAttributes(Object value) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setGetAttributesReq(SessionProto.Transaction.GetAttributes.Req.newBuilder()
                            .setValue(Concept.attributeValue(value))
                    ).build();
        }

        public static SessionProto.Transaction.Req putEntityType(Label label) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setPutEntityTypeReq(SessionProto.Transaction.PutEntityType.Req.newBuilder().setLabel(label.getValue()))
                    .build();
        }

        public static SessionProto.Transaction.Req putAttributeType(Label label, AttributeType.DataType<?> dataType) {
            SessionProto.Transaction.PutAttributeType.Req request = SessionProto.Transaction.PutAttributeType.Req.newBuilder()
                    .setLabel(label.getValue())
                    .setDataType(Concept.dataType(dataType))
                    .build();

            return SessionProto.Transaction.Req.newBuilder().setPutAttributeTypeReq(request).build();
        }

        public static SessionProto.Transaction.Req putRelationshipType(Label label) {
            SessionProto.Transaction.PutRelationType.Req request = SessionProto.Transaction.PutRelationType.Req.newBuilder()
                    .setLabel(label.getValue())
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setPutRelationTypeReq(request).build();
        }

        public static SessionProto.Transaction.Req putRole(Label label) {
            SessionProto.Transaction.PutRole.Req request = SessionProto.Transaction.PutRole.Req.newBuilder()
                    .setLabel(label.getValue())
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setPutRoleReq(request).build();
        }

        public static SessionProto.Transaction.Req putRule(Label label, Pattern when, Pattern then) {
            SessionProto.Transaction.PutRule.Req request = SessionProto.Transaction.PutRule.Req.newBuilder()
                    .setLabel(label.getValue())
                    .setWhen(when.toString())
                    .setThen(then.toString())
                    .build();
            return SessionProto.Transaction.Req.newBuilder().setPutRuleReq(request).build();
        }

        public static SessionProto.Transaction.Req iterate(int iteratorId) {
            return SessionProto.Transaction.Req.newBuilder()
                    .setIterateReq(SessionProto.Transaction.Iter.Req.newBuilder()
                            .setId(iteratorId)).build();
        }
    }

    /**
     * An RPC Request Builder class for Concept messages
     */
    public static class Concept {

        public static ConceptProto.Concept concept(ai.grakn.concept.Concept concept) {
            return ConceptProto.Concept.newBuilder()
                    .setId(concept.id().getValue())
                    .setBaseType(getBaseType(concept))
                    .build();
        }

        private static ConceptProto.Concept.BASE_TYPE getBaseType(ai.grakn.concept.Concept concept) {
            if (concept.isEntityType()) {
                return ConceptProto.Concept.BASE_TYPE.ENTITY_TYPE;
            } else if (concept.isRelationshipType()) {
                return ConceptProto.Concept.BASE_TYPE.RELATION_TYPE;
            } else if (concept.isAttributeType()) {
                return ConceptProto.Concept.BASE_TYPE.ATTRIBUTE_TYPE;
            } else if (concept.isEntity()) {
                return ConceptProto.Concept.BASE_TYPE.ENTITY;
            } else if (concept.isRelationship()) {
                return ConceptProto.Concept.BASE_TYPE.RELATION;
            } else if (concept.isAttribute()) {
                return ConceptProto.Concept.BASE_TYPE.ATTRIBUTE;
            } else if (concept.isRole()) {
                return ConceptProto.Concept.BASE_TYPE.ROLE;
            } else if (concept.isRule()) {
                return ConceptProto.Concept.BASE_TYPE.RULE;
            } else if (concept.isType()) {
                return ConceptProto.Concept.BASE_TYPE.META_TYPE;
            } else {
                throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
            }
        }

        public static Collection<ConceptProto.Concept> concepts(Collection<ai.grakn.concept.Concept> concepts) {
            return concepts.stream().map(Concept::concept).collect(toList());
        }

        public static ConceptProto.ValueObject attributeValue(Object value) {
            ConceptProto.ValueObject.Builder builder = ConceptProto.ValueObject.newBuilder();
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

        public static AttributeType.DataType<?> dataType(ConceptProto.AttributeType.DATA_TYPE dataType) {
            switch (dataType) {
                case STRING:
                    return AttributeType.DataType.STRING;
                case BOOLEAN:
                    return AttributeType.DataType.BOOLEAN;
                case INTEGER:
                    return AttributeType.DataType.INTEGER;
                case LONG:
                    return AttributeType.DataType.LONG;
                case FLOAT:
                    return AttributeType.DataType.FLOAT;
                case DOUBLE:
                    return AttributeType.DataType.DOUBLE;
                case DATE:
                    return AttributeType.DataType.DATE;
                default:
                case UNRECOGNIZED:
                    throw new IllegalArgumentException("Unrecognised " + dataType);
            }
        }

        static ConceptProto.AttributeType.DATA_TYPE dataType(AttributeType.DataType<?> dataType) {
            if (dataType.equals(AttributeType.DataType.STRING)) {
                return ConceptProto.AttributeType.DATA_TYPE.STRING;
            } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
                return ConceptProto.AttributeType.DATA_TYPE.BOOLEAN;
            } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
                return ConceptProto.AttributeType.DATA_TYPE.INTEGER;
            } else if (dataType.equals(AttributeType.DataType.LONG)) {
                return ConceptProto.AttributeType.DATA_TYPE.LONG;
            } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
                return ConceptProto.AttributeType.DATA_TYPE.FLOAT;
            } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
                return ConceptProto.AttributeType.DATA_TYPE.DOUBLE;
            } else if (dataType.equals(AttributeType.DataType.DATE)) {
                return ConceptProto.AttributeType.DATA_TYPE.DATE;
            } else {
                throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
            }
        }
    }

    /**
     * An RPC Request Builder class for Answer messages
     */
    public static class Answer {

        public static Object answer(AnswerProto.Answer answer, Grakn.Transaction tx) {
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

        public static ConceptMap queryAnswer(AnswerProto.QueryAnswer queryAnswer, Grakn.Transaction tx) {
            ImmutableMap.Builder<Var, ai.grakn.concept.Concept> map = ImmutableMap.builder();

            queryAnswer.getQueryAnswerMap().forEach((grpcVar, AnswerProto) -> {
                map.put(Graql.var(grpcVar), RemoteConcept.of(AnswerProto, tx));
            });

            return new ConceptMapImpl(map.build());
        }

        public static ComputeQuery.Answer computeAnswer(AnswerProto.ComputeAnswer computeAnswerRPC) {
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

        public static List<List<ConceptId>> paths(AnswerProto.Paths pathsRPC) {
            List<List<ConceptId>> paths = new ArrayList<>(pathsRPC.getPathsList().size());

            for (AnswerProto.ConceptIds conceptIds : pathsRPC.getPathsList()) {
                paths.add(
                        conceptIds.getIdsList().stream()
                                .map(ConceptId::of)
                                .collect(toList())
                );
            }

            return paths;
        }

        public static Map<Long, Set<ConceptId>> centrality(AnswerProto.Centrality centralityRPC) {
            Map<Long, Set<ConceptId>> centrality = new HashMap<>();

            for (Map.Entry<Long, AnswerProto.ConceptIds> entry : centralityRPC.getCentralityMap().entrySet()) {
                centrality.put(
                        entry.getKey(),
                        entry.getValue().getIdsList().stream()
                                .map(ConceptId::of)
                                .collect(Collectors.toSet())
                );
            }

            return centrality;
        }

        public static Set<Set<ConceptId>> clusters(AnswerProto.Clusters clustersRPC) {
            Set<Set<ConceptId>> clusters = new HashSet<>();

            for (AnswerProto.ConceptIds conceptIds : clustersRPC.getClustersList()) {
                clusters.add(
                        conceptIds.getIdsList().stream()
                                .map(ConceptId::of)
                                .collect(Collectors.toSet())
                );
            }

            return clusters;
        }

        public static Set<Long> clusterSizes(AnswerProto.ClusterSizes clusterSizesRPC) {
            return new HashSet<>(clusterSizesRPC.getClusterSizesList());
        }
    }

    /**
     * An RPC Request Builder class for Keyspace Service
     */
    public static class Keyspace {

        public static KeyspaceProto.Keyspace.Delete.Req delete(String name) {
            return KeyspaceProto.Keyspace.Delete.Req.newBuilder().setName(name).build();
        }
    }
}
