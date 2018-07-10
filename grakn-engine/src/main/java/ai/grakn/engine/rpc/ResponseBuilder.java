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
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.rpc.proto.AnswerProto;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.util.CommonUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 */
public class ResponseBuilder {

    /**
     * An RPC Response Builder class for Transaction responses
     */
    public static class Transaction {

        static SessionProto.Transaction.Res open() {
            return SessionProto.Transaction.Res.newBuilder()
                    .setOpenRes(SessionProto.Transaction.Open.Res.getDefaultInstance())
                    .build();
        }

        static SessionProto.Transaction.Res commit() {
            return SessionProto.Transaction.Res.newBuilder()
                    .setCommitRes(SessionProto.Transaction.Commit.Res.getDefaultInstance())
                    .build();
        }

        static SessionProto.Transaction.Res queryIterator(@Nullable int iteratorId) {
            SessionProto.Transaction.Query.Iter.Builder iterator = SessionProto.Transaction.Query.Iter.newBuilder();
            if (iteratorId == -1) {
                iterator.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                iterator.setId(iteratorId);
            }
            return SessionProto.Transaction.Res.newBuilder().setQueryIter(iterator).build();
        }

        static SessionProto.Transaction.Res getSchemaConcept(@Nullable ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.GetSchemaConcept.Res.Builder res = SessionProto.Transaction.GetSchemaConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ResponseBuilder.Concept.concept(concept));
            }
            return SessionProto.Transaction.Res.newBuilder().setGetSchemaConceptRes(res).build();
        }

        static SessionProto.Transaction.Res getConcept(@Nullable ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.GetConcept.Res.Builder res = SessionProto.Transaction.GetConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ResponseBuilder.Concept.concept(concept));
            }
            return SessionProto.Transaction.Res.newBuilder().setGetConceptRes(res).build();
        }

        static SessionProto.Transaction.Res getAttributesIterator(int iteratorId) {
            SessionProto.Transaction.GetAttributes.Iter.Builder res = SessionProto.Transaction.GetAttributes.Iter.newBuilder()
                    .setId(iteratorId);
            return SessionProto.Transaction.Res.newBuilder().setGetAttributesIter(res).build();
        }

        static SessionProto.Transaction.Res putEntityType(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutEntityType.Res.Builder res = SessionProto.Transaction.PutEntityType.Res.newBuilder()
                    .setConcept(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutEntityTypeRes(res).build();
        }

        static SessionProto.Transaction.Res putAttributeType(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutAttributeType.Res.Builder res = SessionProto.Transaction.PutAttributeType.Res.newBuilder()
                    .setConcept(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutAttributeTypeRes(res).build();
        }

        static SessionProto.Transaction.Res putRelationshipType(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutRelationshipType.Res.Builder res = SessionProto.Transaction.PutRelationshipType.Res.newBuilder()
                    .setConcept(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRelationshipTypeRes(res).build();
        }

        static SessionProto.Transaction.Res putRole(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutRole.Res.Builder res = SessionProto.Transaction.PutRole.Res.newBuilder()
                    .setConcept(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRoleRes(res).build();
        }

        static SessionProto.Transaction.Res putRule(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutRule.Res.Builder res = SessionProto.Transaction.PutRule.Res.newBuilder()
                    .setConcept(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRuleRes(res).build();
        }

        /**
         * An RPC Response Builder class for Transaction iterator responses
         */
        static class Iter {

            static SessionProto.Transaction.Res query(Object object) {
                return SessionProto.Transaction.Res.newBuilder()
                        .setIterateRes(SessionProto.Transaction.Iter.Res.newBuilder()
                                .setQueryIterRes(SessionProto.Transaction.Query.Iter.Res.newBuilder()
                                        .setAnswer(Answer.answer(object)))).build();
            }

            static SessionProto.Transaction.Res getAttributes(ai.grakn.concept.Concept concept) {
                return SessionProto.Transaction.Res.newBuilder()
                        .setIterateRes(SessionProto.Transaction.Iter.Res.newBuilder()
                                .setGetAttributesIterRes(SessionProto.Transaction.GetAttributes.Iter.Res.newBuilder()
                                        .setConcept(Concept.concept(concept)))).build();
            }

            static SessionProto.Transaction.Res conceptMethod(ConceptProto.Method.Iter.Res methodResponse) {
                return SessionProto.Transaction.Res.newBuilder()
                        .setIterateRes(SessionProto.Transaction.Iter.Res.newBuilder()
                                .setConceptMethodIterRes(methodResponse)).build();
            }
        }
    }

    /**
     * An RPC Response Builder class for Concept responses
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
                return ConceptProto.Concept.BASE_TYPE.RELATIONSHIP_TYPE;
            } else if (concept.isAttributeType()) {
                return ConceptProto.Concept.BASE_TYPE.ATTRIBUTE_TYPE;
            } else if (concept.isEntity()) {
                return ConceptProto.Concept.BASE_TYPE.ENTITY;
            } else if (concept.isRelationship()) {
                return ConceptProto.Concept.BASE_TYPE.RELATIONSHIP;
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

        static ConceptProto.AttributeType.DATA_TYPE DATA_TYPE(AttributeType.DataType<?> dataType) {
            if (dataType.equals(AttributeType.DataType.STRING)) {
                return ConceptProto.AttributeType.DATA_TYPE.String;
            } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
                return ConceptProto.AttributeType.DATA_TYPE.Boolean;
            } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
                return ConceptProto.AttributeType.DATA_TYPE.Integer;
            } else if (dataType.equals(AttributeType.DataType.LONG)) {
                return ConceptProto.AttributeType.DATA_TYPE.Long;
            } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
                return ConceptProto.AttributeType.DATA_TYPE.Float;
            } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
                return ConceptProto.AttributeType.DATA_TYPE.Double;
            } else if (dataType.equals(AttributeType.DataType.DATE)) {
                return ConceptProto.AttributeType.DATA_TYPE.Date;
            } else {
                throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
            }
        }

        public static AttributeType.DataType<?> DATA_TYPE(ConceptProto.AttributeType.DATA_TYPE dataType) {
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

        static ConceptProto.ValueObject attributeValue(Object value) {
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
    }

    /**
     * An RPC Response Builder class for Answer responses
     */
    public static class Answer {

        public static AnswerProto.Answer answer(Object object) {
            AnswerProto.Answer answer;

            if (object instanceof ai.grakn.graql.admin.Answer) {
                answer = AnswerProto.Answer.newBuilder().setQueryAnswer(queryAnswer((ai.grakn.graql.admin.Answer) object)).build();
            } else if (object instanceof ComputeQuery.Answer) {
                answer = AnswerProto.Answer.newBuilder().setComputeAnswer(computeAnswer((ComputeQuery.Answer) object)).build();
            } else {
                // If not an QueryAnswer or ComputeAnswer, convert to JSON
                answer = AnswerProto.Answer.newBuilder().setOtherResult(Printer.jsonPrinter().toString(object)).build();
            }

            return answer;
        }

        static AnswerProto.QueryAnswer queryAnswer(ai.grakn.graql.admin.Answer answer) {
            AnswerProto.QueryAnswer.Builder queryAnswerProto = AnswerProto.QueryAnswer.newBuilder();
            answer.forEach((var, concept) -> {
                ConceptProto.Concept conceptRps = ResponseBuilder.Concept.concept(concept);
                queryAnswerProto.putQueryAnswer(var.getValue(), conceptRps);
            });

            // TODO: answer.getExplanation should return null, rather than an instance where .getQuery() returns null
            if (answer.getExplanation() != null && answer.getExplanation().getQuery() != null) {
                queryAnswerProto.setExplanation(explanation(answer.getExplanation()));
            }

            return queryAnswerProto.build();
        }

        static AnswerProto.Explanation explanation(ai.grakn.graql.admin.AnswerExplanation explanation) {
            return AnswerProto.Explanation.newBuilder()
                    .setQueryPattern(explanation.getQuery().getPattern().toString())
                    .addAllQueryAnswer(explanation.getAnswers().stream()
                            .map(Answer::queryAnswer)
                            .collect(Collectors.toList()))
                    .build();
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

    public static StatusRuntimeException exception(RuntimeException e) {

        if (e instanceof GraknException) {
            GraknException ge = (GraknException) e;
            String message = ge.getName() + "-" + ge.getMessage();
            if (e instanceof TemporaryWriteException) {
                return exception(Status.RESOURCE_EXHAUSTED, message);
            } else if (e instanceof GraknBackendException) {
                return exception(Status.INTERNAL, message);
            } else if (e instanceof PropertyNotUniqueException) {
                return exception(Status.ALREADY_EXISTS, message);
            } else if (e instanceof GraknTxOperationException | e instanceof GraqlQueryException |
                    e instanceof GraqlSyntaxException | e instanceof InvalidKBException) {
                return exception(Status.INVALID_ARGUMENT, message);
            }
        } else if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        }

        return exception(Status.UNKNOWN, e.getMessage());
    }

    private static StatusRuntimeException exception(Status status, String message) {
        return exception(status.withDescription(message + ". Please check server logs for the stack trace."));
    }

    public static StatusRuntimeException exception(Status status) {
        return new StatusRuntimeException(status);
    }
}
