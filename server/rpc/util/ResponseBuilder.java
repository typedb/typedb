/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.rpc.util;

import com.google.protobuf.ByteString;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.Type;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.time.ZoneOffset;

import static java.lang.String.format;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 */
public class ResponseBuilder {

    public static StatusRuntimeException exception(Throwable e) {
        if (e instanceof GraknException) {
            return exception(Status.INTERNAL, e.getMessage());
        } else if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        } else {
            return exception(Status.UNKNOWN, e.getMessage());
        }
    }

    private static StatusRuntimeException exception(Status status, String message) {
        return status.withDescription(message + " Please check server logs for the stack trace.").asRuntimeException();
    }

    /**
     * An RPC Response Builder class for Transaction responses
     */
    public static class Transaction {

        public static TransactionProto.Transaction.Res open() {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setOpenRes(TransactionProto.Transaction.Open.Res.getDefaultInstance())
                    .build();
        }

        public static TransactionProto.Transaction.Res commit() {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setCommitRes(TransactionProto.Transaction.Commit.Res.getDefaultInstance())
                    .build();
        }

        public static TransactionProto.Transaction.Res getType(@Nullable grakn.core.concept.Concept concept) {
            TransactionProto.Transaction.GetType.Res.Builder res = TransactionProto.Transaction.GetType.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setType(ResponseBuilder.Concept.concept(concept));
            }
            return TransactionProto.Transaction.Res.newBuilder().setGetTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res getConcept(@Nullable grakn.core.concept.Concept concept) {
            TransactionProto.Transaction.GetConcept.Res.Builder res = TransactionProto.Transaction.GetConcept.Res.newBuilder();
            if (concept == null) {
                res.setNull(ConceptProto.Null.getDefaultInstance());
            } else {
                res.setConcept(ResponseBuilder.Concept.concept(concept));
            }
            return TransactionProto.Transaction.Res.newBuilder().setGetConceptRes(res).build();
        }

        public static TransactionProto.Transaction.Res putEntityType(grakn.core.concept.Concept concept) {
            TransactionProto.Transaction.PutEntityType.Res.Builder res = TransactionProto.Transaction.PutEntityType.Res.newBuilder()
                    .setEntityType(ResponseBuilder.Concept.concept(concept));
            return TransactionProto.Transaction.Res.newBuilder().setPutEntityTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putAttributeType(grakn.core.concept.Concept concept) {
            TransactionProto.Transaction.PutAttributeType.Res.Builder res = TransactionProto.Transaction.PutAttributeType.Res.newBuilder()
                    .setAttributeType(ResponseBuilder.Concept.concept(concept));
            return TransactionProto.Transaction.Res.newBuilder().setPutAttributeTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putRelationType(grakn.core.concept.Concept concept) {
            TransactionProto.Transaction.PutRelationType.Res.Builder res = TransactionProto.Transaction.PutRelationType.Res.newBuilder()
                    .setRelationType(ResponseBuilder.Concept.concept(concept));
            return TransactionProto.Transaction.Res.newBuilder().setPutRelationTypeRes(res).build();
        }

//        static TransactionProto.Transaction.Res putRule(grakn.core.concept.Concept concept) {
//            TransactionProto.Transaction.PutRule.Res.Builder res = TransactionProto.Transaction.PutRule.Res.newBuilder()
//                    .setRule(ResponseBuilder.Concept.concept(concept));
//            return TransactionProto.Transaction.Res.newBuilder().setPutRuleRes(res).build();
//        }
//
//        /**
//         * @param explanation
//         * @return
//         */
//        static TransactionProto.Transaction.Res explanation(Explanation explanation) {
//            TransactionProto.Transaction.Res.Builder res = TransactionProto.Transaction.Res.newBuilder();
//            AnswerProto.Explanation.Res.Builder explanationBuilder = AnswerProto.Explanation.Res.newBuilder()
//                    .addAllExplanation(explanation.getAnswers().stream().map(Answer::conceptMap)
//                                               .collect(Collectors.toList()));
//
//            if (explanation.isRuleExplanation()) {
//                Rule rule = ((RuleExplanation) explanation).getRule();
//                ConceptProto.Concept ruleProto = Concept.concept(rule);
//                explanationBuilder.setRule(ruleProto);
//            }
//
//            res.setExplanationRes(explanationBuilder.build());
//            return res.build();
//        }

        /**
         * An RPC Response Builder class for Transaction iterator responses
         */
        public static class Iter {

            public static TransactionProto.Transaction.Res done() {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setDone(true)).build();
            }

            public static TransactionProto.Transaction.Res id(int id) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setIteratorID(id)).build();
            }

            public static TransactionProto.Transaction.Res query(Object object) {
                throw new UnsupportedOperationException();
//                return TransactionProto.Transaction.Res.newBuilder()
//                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
//                                            .setQueryIterRes(TransactionProto.Transaction.Query.Iter.Res.newBuilder()
//                                                                     .setAnswer(Answer.answer(object)))).build();
            }

            public static TransactionProto.Transaction.Res conceptMethod(ConceptProto.Method.Iter.Res methodResponse) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setConceptMethodIterRes(TransactionProto.Transaction.ConceptMethod.Iter.Res.newBuilder()
                                                                             .setResponse(methodResponse))).build();
            }
        }
    }

    /**
     * An RPC Response Builder class for Concept responses
     */
    public static class Concept {

        public static ConceptProto.Concept concept(grakn.core.concept.Concept concept) {
            return ConceptProto.Concept.newBuilder()
                    .setIid(ByteString.copyFrom(concept.iid()))
                    .setBaseType(getBaseType(concept))
                    .build();
        }

        public static ConceptProto.Concept conceptPrefilled(grakn.core.concept.Concept concept) {
            ConceptProto.Concept.Builder builder = ConceptProto.Concept.newBuilder()
                    .setIid(ByteString.copyFrom(concept.iid()))
                    .setBaseType(getBaseType(concept));

            if (concept instanceof Type) {
                builder.setLabelRes(ConceptProto.Type.GetLabel.Res.newBuilder()
                                            .setLabel(concept.asType().label()));
            } else if (concept instanceof Thing) {
                builder.setTypeRes(ConceptProto.Thing.Type.Res.newBuilder()
                                           .setThingType(conceptPrefilled(concept.asThing().type())));
                builder.setInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                               .setInferred(concept.asThing().isInferred()));

                if (concept instanceof Attribute) {
                    builder.setValueRes(ConceptProto.Attribute.Value.Res.newBuilder()
                                                .setValue(attributeValue((Attribute) concept)));
                    builder.setValueTypeRes(ConceptProto.AttributeType.ValueType.Res.newBuilder()
                                                    .setValueType(valueType((Attribute) concept)));
                }
            }

            return builder.build();
        }

        private static ConceptProto.Concept.SCHEMA getBaseType(grakn.core.concept.Concept concept) {
            if (concept instanceof EntityType) {
                if (((EntityType) concept).isRoot()) {
                    return ConceptProto.Concept.SCHEMA.META_TYPE;
                } else {
                    return ConceptProto.Concept.SCHEMA.ENTITY_TYPE;
                }
            } else if (concept instanceof RelationType) {
                if (((RelationType) concept).isRoot()) {
                    return ConceptProto.Concept.SCHEMA.META_TYPE;
                } else {
                    return ConceptProto.Concept.SCHEMA.RELATION_TYPE;
                }
            } else if (concept instanceof AttributeType) {
                if (((AttributeType) concept).isRoot()) {
                    return ConceptProto.Concept.SCHEMA.META_TYPE;
                } else {
                    return ConceptProto.Concept.SCHEMA.ATTRIBUTE_TYPE;
                }
            } else if (concept instanceof Entity) {
                return ConceptProto.Concept.SCHEMA.ENTITY;
            } else if (concept instanceof Relation) {
                return ConceptProto.Concept.SCHEMA.RELATION;
            } else if (concept instanceof Attribute) {
                return ConceptProto.Concept.SCHEMA.ATTRIBUTE;
            } else if (concept instanceof RoleType) {
                return ConceptProto.Concept.SCHEMA.ROLE;
//            } else if (concept.isRule()) {
//                return ConceptProto.Concept.SCHEMA.RULE;
            } else {
                throw new GraknException(ErrorMessage.Internal.ILLEGAL_STATE);
            }
        }

        static ConceptProto.ValueObject attributeValue(Attribute attribute) {
            ConceptProto.ValueObject.Builder builder = ConceptProto.ValueObject.newBuilder();
            if (attribute instanceof Attribute.String) {
                builder.setString(((Attribute.String) attribute).value());
            } else if (attribute instanceof Attribute.Boolean) {
                builder.setBoolean(((Attribute.Boolean) attribute).value());
            } else if (attribute instanceof Attribute.Long) {
                builder.setLong(((Attribute.Long) attribute).value());
            } else if (attribute instanceof Attribute.Double) {
                builder.setDouble(((Attribute.Double) attribute).value());
            } else if (attribute instanceof Attribute.DateTime) {
                builder.setDatetime(
                        ((Attribute.DateTime) attribute).value().atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
            } else {
                throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
            }

            return builder.build();
        }

        public static ConceptProto.AttributeType.VALUE_TYPE VALUE_TYPE(AttributeType.ValueType valueType) {
            switch (valueType) {
                case STRING:
                    return ConceptProto.AttributeType.VALUE_TYPE.STRING;
                case BOOLEAN:
                    return ConceptProto.AttributeType.VALUE_TYPE.BOOLEAN;
                case LONG:
                    return ConceptProto.AttributeType.VALUE_TYPE.LONG;
                case DOUBLE:
                    return ConceptProto.AttributeType.VALUE_TYPE.DOUBLE;
                case DATETIME:
                    return ConceptProto.AttributeType.VALUE_TYPE.DATETIME;
                case OBJECT:
                    return ConceptProto.AttributeType.VALUE_TYPE.UNRECOGNIZED;
                default:
                    throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
            }
        }

        public static AttributeType.ValueType VALUE_TYPE(ConceptProto.AttributeType.VALUE_TYPE valueType) {
            switch (valueType) {
                case STRING:
                    return AttributeType.ValueType.STRING;
                case BOOLEAN:
                    return AttributeType.ValueType.BOOLEAN;
                case LONG:
                    return AttributeType.ValueType.LONG;
                case DOUBLE:
                    return AttributeType.ValueType.DOUBLE;
                case DATETIME:
                    return AttributeType.ValueType.DATETIME;
                default:
                case UNRECOGNIZED:
                    throw Status.UNIMPLEMENTED.withDescription(format("Unsupported value type '%s'", valueType)).asRuntimeException();
            }
        }

        public static ConceptProto.AttributeType.VALUE_TYPE valueType(Attribute attribute) {
            if (attribute instanceof Attribute.String) {
                return ConceptProto.AttributeType.VALUE_TYPE.STRING;
            } else if (attribute instanceof Attribute.Boolean) {
                return ConceptProto.AttributeType.VALUE_TYPE.BOOLEAN;
            } else if (attribute instanceof Attribute.Long) {
                return ConceptProto.AttributeType.VALUE_TYPE.LONG;
            } else if (attribute instanceof Attribute.Double) {
                return ConceptProto.AttributeType.VALUE_TYPE.DOUBLE;
            } else if (attribute instanceof Attribute.DateTime) {
                return ConceptProto.AttributeType.VALUE_TYPE.DATETIME;
            } else {
                throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
            }
        }
    }
//
//    /**
//     * An RPC Response Builder class for Answer responses
//     */
//    public static class Answer {
//
//        public static AnswerProto.Answer answer(Object object) {
//            AnswerProto.Answer.Builder answer = AnswerProto.Answer.newBuilder();
//
//            if (object instanceof AnswerGroup) {
//                answer.setAnswerGroup(answerGroup((AnswerGroup) object));
//            } else if (object instanceof ConceptMap) {
//                answer.setConceptMap(conceptMap((ConceptMap) object));
//            } else if (object instanceof ConceptList) {
//                answer.setConceptList(conceptList((ConceptList) object));
//            } else if (object instanceof ConceptSetMeasure) {
//                answer.setConceptSetMeasure(conceptSetMeasure((ConceptSetMeasure) object));
//            } else if (object instanceof ConceptSet) {
//                answer.setConceptSet(conceptSet((ConceptSet) object));
//            } else if (object instanceof Numeric) {
//                answer.setValue(value((Numeric) object));
//            } else if (object instanceof Void) {
//                answer.setVoid(voidMessage((Void) object));
//            }
//
//            return answer.build();
//        }
//
//
//        static AnswerProto.AnswerGroup answerGroup(AnswerGroup<?> answer) {
//            AnswerProto.AnswerGroup.Builder answerGroupProto = AnswerProto.AnswerGroup.newBuilder()
//                    .setOwner(ResponseBuilder.Concept.concept(answer.owner()))
//                    .addAllAnswers(answer.answers().stream().map(Answer::answer).collect(Collectors.toList()));
//
//            return answerGroupProto.build();
//        }
//
//        static AnswerProto.ConceptMap conceptMap(ConceptMap answer) {
//            AnswerProto.ConceptMap.Builder conceptMapProto = AnswerProto.ConceptMap.newBuilder();
//            answer.map().forEach((var, concept) -> {
//                ConceptProto.Concept conceptProto = ResponseBuilder.Concept.conceptPrefilled(concept);
//                conceptMapProto.putMap(var.name(), conceptProto);
//            });
//
//            if (answer.getPattern() != null) {
//                conceptMapProto.setPattern(answer.getPattern().toString());
//            }
//
//            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
//                conceptMapProto.setHasExplanation(true);
//            } else {
//                conceptMapProto.setHasExplanation(false);
//            }
//            return conceptMapProto.build();
//        }
//
//        static AnswerProto.ConceptList conceptList(ConceptList answer) {
//            AnswerProto.ConceptList.Builder conceptListProto = AnswerProto.ConceptList.newBuilder();
//            conceptListProto.setList(conceptIds(answer.list()));
//
//            return conceptListProto.build();
//        }
//
//        static AnswerProto.ConceptSet conceptSet(ConceptSet answer) {
//            AnswerProto.ConceptSet.Builder conceptSetProto = AnswerProto.ConceptSet.newBuilder();
//            conceptSetProto.setSet(conceptIds(answer.set()));
//
//            return conceptSetProto.build();
//        }
//
//        static AnswerProto.ConceptSetMeasure conceptSetMeasure(ConceptSetMeasure answer) {
//            AnswerProto.ConceptSetMeasure.Builder conceptSetMeasureProto = AnswerProto.ConceptSetMeasure.newBuilder();
//            conceptSetMeasureProto.setSet(conceptIds(answer.set()));
//            conceptSetMeasureProto.setMeasurement(number(answer.measurement()));
//            return conceptSetMeasureProto.build();
//        }
//
//        static AnswerProto.Value value(Numeric answer) {
//            AnswerProto.Value.Builder valueProto = AnswerProto.Value.newBuilder();
//            valueProto.setNumber(number(answer.number()));
//            return valueProto.build();
//        }
//
//        static AnswerProto.Number number(Number number) {
//            return AnswerProto.Number.newBuilder().setValue(number.toString()).build();
//        }
//
//        static AnswerProto.Void voidMessage(Void voidAnswer) {
//            return AnswerProto.Void.newBuilder().setMessage(voidAnswer.message()).build();
//        }
//
//        private static AnswerProto.ConceptIds conceptIds(Collection<ConceptId> conceptIds) {
//            AnswerProto.ConceptIds.Builder conceptIdsRPC = AnswerProto.ConceptIds.newBuilder();
//            conceptIdsRPC.addAllIds(conceptIds.stream()
//                                            .map(id -> id.getValue())
//                                            .collect(Collectors.toList()));
//
//            return conceptIdsRPC.build();
//        }
//    }
}
