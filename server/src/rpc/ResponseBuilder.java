/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.core.server.rpc;

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
import ai.grakn.graql.admin.Explanation;
import ai.grakn.graql.answer.AnswerGroup;
import ai.grakn.graql.answer.ConceptList;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.ConceptSet;
import ai.grakn.graql.answer.ConceptSetMeasure;
import ai.grakn.graql.answer.Value;
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

        static SessionProto.Transaction.Res queryIterator(int iteratorId) {
            return SessionProto.Transaction.Res.newBuilder()
                    .setQueryIter(SessionProto.Transaction.Query.Iter.newBuilder().setId(iteratorId))
                    .build();
        }

        static SessionProto.Transaction.Res getSchemaConcept(@Nullable ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.GetSchemaConcept.Res.Builder res = SessionProto.Transaction.GetSchemaConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setSchemaConcept(ResponseBuilder.Concept.concept(concept));
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
                    .setEntityType(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutEntityTypeRes(res).build();
        }

        static SessionProto.Transaction.Res putAttributeType(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutAttributeType.Res.Builder res = SessionProto.Transaction.PutAttributeType.Res.newBuilder()
                    .setAttributeType(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutAttributeTypeRes(res).build();
        }

        static SessionProto.Transaction.Res putRelationshipType(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutRelationType.Res.Builder res = SessionProto.Transaction.PutRelationType.Res.newBuilder()
                    .setRelationType(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRelationTypeRes(res).build();
        }

        static SessionProto.Transaction.Res putRole(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutRole.Res.Builder res = SessionProto.Transaction.PutRole.Res.newBuilder()
                    .setRole(ResponseBuilder.Concept.concept(concept));
            return SessionProto.Transaction.Res.newBuilder().setPutRoleRes(res).build();
        }

        static SessionProto.Transaction.Res putRule(ai.grakn.concept.Concept concept) {
            SessionProto.Transaction.PutRule.Res.Builder res = SessionProto.Transaction.PutRule.Res.newBuilder()
                    .setRule(ResponseBuilder.Concept.concept(concept));
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
                                        .setAttribute(Concept.concept(concept)))).build();
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

        static ConceptProto.AttributeType.DATA_TYPE DATA_TYPE(AttributeType.DataType<?> dataType) {
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

        public static AttributeType.DataType<?> DATA_TYPE(ConceptProto.AttributeType.DATA_TYPE dataType) {
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
            AnswerProto.Answer.Builder answer = AnswerProto.Answer.newBuilder();

            if (object instanceof AnswerGroup) {
                answer.setAnswerGroup(answerGroup((AnswerGroup) object));
            } else if (object instanceof ConceptMap) {
                answer.setConceptMap(conceptMap((ConceptMap) object));
            } else if (object instanceof ConceptList) {
                answer.setConceptList(conceptList((ConceptList) object));
            } else if (object instanceof ConceptSetMeasure) {
                answer.setConceptSetMeasure(conceptSetMeasure((ConceptSetMeasure) object));
            } else if (object instanceof ConceptSet) {
                answer.setConceptSet(conceptSet((ConceptSet) object));
            } else if (object instanceof Value) {
                answer.setValue(value((Value) object));
            }

            return answer.build();
        }

        static AnswerProto.Explanation explanation(Explanation explanation) {
            AnswerProto.Explanation.Builder builder = AnswerProto.Explanation.newBuilder()
                    .addAllAnswers(explanation.getAnswers().stream().map(Answer::conceptMap)
                            .collect(Collectors.toList()));
            if (explanation.getQuery() != null) builder.setPattern(explanation.getQuery().getPattern().toString());
            return builder.build();
        }

        static AnswerProto.AnswerGroup answerGroup(AnswerGroup<?> answer) {
            AnswerProto.AnswerGroup.Builder answerGroupProto = AnswerProto.AnswerGroup.newBuilder()
                    .setOwner(ResponseBuilder.Concept.concept(answer.owner()))
                    .addAllAnswers(answer.answers().stream().map(Answer::answer).collect(Collectors.toList()));

            // TODO: answer.explanation should return null, rather than an instance where .getQuery() returns null
            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
                answerGroupProto.setExplanation(explanation(answer.explanation()));
            }
            return answerGroupProto.build();
        }

        static AnswerProto.ConceptMap conceptMap(ConceptMap answer) {
            AnswerProto.ConceptMap.Builder conceptMapProto = AnswerProto.ConceptMap.newBuilder();
            answer.map().forEach((var, concept) -> {
                ConceptProto.Concept conceptProto = ResponseBuilder.Concept.concept(concept);
                conceptMapProto.putMap(var.getValue(), conceptProto);
            });

            // TODO: answer.explanation should return null, rather than an instance where .getQuery() returns null
            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
                conceptMapProto.setExplanation(explanation(answer.explanation()));
            }
            return conceptMapProto.build();
        }

        static AnswerProto.ConceptList conceptList(ConceptList answer) {
            AnswerProto.ConceptList.Builder conceptListProto = AnswerProto.ConceptList.newBuilder();
            conceptListProto.setList(conceptIds(answer.list()));

            // TODO: answer.explanation should return null, rather than an instance where .getQuery() returns null
            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
                conceptListProto.setExplanation(explanation(answer.explanation()));
            }
            return conceptListProto.build();
        }

        static AnswerProto.ConceptSet conceptSet(ConceptSet answer) {
            AnswerProto.ConceptSet.Builder conceptSetProto = AnswerProto.ConceptSet.newBuilder();
            conceptSetProto.setSet(conceptIds(answer.set()));

            // TODO: answer.explanation should return null, rather than an instance where .getQuery() returns null
            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
                conceptSetProto.setExplanation(explanation(answer.explanation()));
            }
            return conceptSetProto.build();
        }

        static AnswerProto.ConceptSetMeasure conceptSetMeasure(ConceptSetMeasure answer) {
            AnswerProto.ConceptSetMeasure.Builder conceptSetMeasureProto = AnswerProto.ConceptSetMeasure.newBuilder();
            conceptSetMeasureProto.setSet(conceptIds(answer.set()));
            conceptSetMeasureProto.setMeasurement(number(answer.measurement()));
            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
                conceptSetMeasureProto.setExplanation(explanation(answer.explanation()));
            }
            return conceptSetMeasureProto.build();
        }

        static AnswerProto.Value value(Value answer) {
            AnswerProto.Value.Builder valueProto = AnswerProto.Value.newBuilder();
            valueProto.setNumber(number(answer.number()));

            // TODO: answer.explanation should return null, rather than an instance where .getQuery() returns null
            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
                valueProto.setExplanation(explanation(answer.explanation()));
            }
            return valueProto.build();
        }

        static AnswerProto.Number number(Number number) {
            return AnswerProto.Number.newBuilder().setValue(number.toString()).build();
        }

        private static AnswerProto.ConceptIds conceptIds(Collection<ConceptId> conceptIds) {
            AnswerProto.ConceptIds.Builder conceptIdsRPC = AnswerProto.ConceptIds.newBuilder();
            conceptIdsRPC.addAllIds(conceptIds.stream()
                    .map(id -> id.getValue())
                    .collect(Collectors.toList()));

            return conceptIdsRPC.build();
        }
    }

    public static StatusRuntimeException exception(Throwable e) {

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
