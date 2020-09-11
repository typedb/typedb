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
import grakn.core.concept.data.Attribute;
import grakn.core.concept.data.Entity;
import grakn.core.concept.data.Relation;
import grakn.core.concept.data.Thing;
import grakn.core.concept.schema.AttributeType;
import grakn.core.concept.schema.EntityType;
import grakn.core.concept.schema.RelationType;
import grakn.core.concept.schema.RoleType;
import grakn.core.concept.schema.ThingType;
import grakn.core.concept.schema.Type;
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

        public static TransactionProto.Transaction.Res getThing(@Nullable Thing thing) {
            TransactionProto.Transaction.GetThing.Res.Builder res = TransactionProto.Transaction.GetThing.Res.newBuilder();
            if (thing != null) {
                res.setThing(ResponseBuilder.Concept.thing(thing));
            }
            return TransactionProto.Transaction.Res.newBuilder().setGetThingRes(res).build();
        }

        public static TransactionProto.Transaction.Res getType(@Nullable Type type) {
            TransactionProto.Transaction.GetType.Res.Builder res = TransactionProto.Transaction.GetType.Res.newBuilder();
            if (type != null) {
                res.setType(ResponseBuilder.Concept.type(type));
            }
            return TransactionProto.Transaction.Res.newBuilder().setGetTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putEntityType(EntityType entityType) {
            TransactionProto.Transaction.PutEntityType.Res.Builder res = TransactionProto.Transaction.PutEntityType.Res.newBuilder()
                    .setEntityType(ResponseBuilder.Concept.type(entityType));
            return TransactionProto.Transaction.Res.newBuilder().setPutEntityTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putAttributeType(AttributeType attributeType) {
            TransactionProto.Transaction.PutAttributeType.Res.Builder res = TransactionProto.Transaction.PutAttributeType.Res.newBuilder()
                    .setAttributeType(ResponseBuilder.Concept.type(attributeType));
            return TransactionProto.Transaction.Res.newBuilder().setPutAttributeTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putRelationType(RelationType relationType) {
            TransactionProto.Transaction.PutRelationType.Res.Builder res = TransactionProto.Transaction.PutRelationType.Res.newBuilder()
                    .setRelationType(ResponseBuilder.Concept.type(relationType));
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

            public static TransactionProto.Transaction.Res conceptMethod(ConceptProto.ThingMethod.Iter.Res methodResponse) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setConceptMethodThingIterRes(TransactionProto.Transaction.ConceptMethod.Thing.Iter.Res.newBuilder()
                                                                                  .setResponse(methodResponse))).build();
            }

            public static TransactionProto.Transaction.Res conceptMethod(ConceptProto.TypeMethod.Iter.Res methodResponse) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setConceptMethodTypeIterRes(TransactionProto.Transaction.ConceptMethod.Type.Iter.Res.newBuilder()
                                                                                 .setResponse(methodResponse))).build();
            }
        }
    }

    /**
     * An RPC Response Builder class for Concept responses
     */
    public static class Concept {

        public static ConceptProto.Thing thing(Thing thing) {
            final ConceptProto.Thing.Builder builder = ConceptProto.Thing.newBuilder()
                    .setIid(ByteString.copyFrom(thing.getIID()))
                    .setEncoding(getEncoding(thing));

            if (thing instanceof Attribute) {
                final Attribute attribute = thing.asAttribute();
                builder.setValueType(valueType(attribute))
                        .setValue(attributeValue(attribute));
            }

            return builder.build();
        }

        public static ConceptProto.Type type(Type type) {
            final ConceptProto.Type.Builder builder = ConceptProto.Type.newBuilder()
                    .setLabel(type.getLabel())
                    .setEncoding(getEncoding(type));
            if (type instanceof AttributeType) builder.setValueType(valueType(type.asAttributeType()));
            if (type instanceof RoleType) builder.setScope(type.asRoleType().getScope());
            if (type.isRoot()) builder.setRoot(true);
            return builder.build();
        }

        /* public static ConceptProto.Concept conceptPrefilled(grakn.core.concept.Concept concept) {
            ConceptProto.Concept.Builder builder = ConceptProto.Concept.newBuilder()
                    .setIid(ByteString.copyFrom(concept.getIID()))
                    .setBaseType(getSchema(concept));

            if (concept instanceof Type) {
                builder.setLabelRes(ConceptProto.Type.GetLabel.Res.newBuilder()
                                            .setLabel(concept.asType().getLabel()));
            } else if (concept instanceof Thing) {
                builder.setTypeRes(ConceptProto.Thing.GetType.Res.newBuilder()
                                           .setThingType(conceptPrefilled(concept.asThing().getType())));
                builder.setInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                               .setInferred(concept.asThing().isInferred()));

                if (concept instanceof Attribute) {
                    builder.setValueRes(ConceptProto.Attribute.GetValue.Res.newBuilder()
                                                .setValue(attributeValue((Attribute) concept)));
                    builder.setValueTypeRes(ConceptProto.AttributeType.GetValueType.Res.newBuilder()
                                                    .setValueType(valueType((Attribute) concept)));
                }
            }

            return builder.build();
        } */

        private static ConceptProto.Thing.ENCODING getEncoding(Thing thing) {
            if (thing instanceof Entity) {
                return ConceptProto.Thing.ENCODING.ENTITY;
            } else if (thing instanceof Relation) {
                return ConceptProto.Thing.ENCODING.RELATION;
            } else if (thing instanceof Attribute) {
                return ConceptProto.Thing.ENCODING.ATTRIBUTE;
            } else {
                throw new GraknException(ErrorMessage.Internal.ILLEGAL_STATE);
            }
        }

        private static ConceptProto.Type.ENCODING getEncoding(Type type) {
            if (type instanceof EntityType) {
                return ConceptProto.Type.ENCODING.ENTITY_TYPE;
            } else if (type instanceof RelationType) {
                return ConceptProto.Type.ENCODING.RELATION_TYPE;
            } else if (type instanceof AttributeType) {
                return ConceptProto.Type.ENCODING.ATTRIBUTE_TYPE;
            } else if (type instanceof ThingType) {
                return ConceptProto.Type.ENCODING.THING_TYPE;
            } else if (type instanceof RoleType) {
                return ConceptProto.Type.ENCODING.ROLE_TYPE;
//            } else if (concept.isRule()) {
//                return ConceptProto.Concept.ENCODING.RULE;
            } else {
                throw new GraknException(ErrorMessage.Internal.ILLEGAL_STATE);
            }
        }

        public static ConceptProto.Attribute.Value attributeValue(Attribute attribute) {
            final ConceptProto.Attribute.Value.Builder builder = ConceptProto.Attribute.Value.newBuilder();

            if (attribute instanceof Attribute.String) {
                builder.setString(attribute.asString().getValue());
            } else if (attribute instanceof Attribute.Long) {
                builder.setLong(attribute.asLong().getValue());
            } else if (attribute instanceof Attribute.Boolean) {
                builder.setBoolean(attribute.asBoolean().getValue());
            } else if (attribute instanceof Attribute.DateTime) {
                builder.setDatetime(attribute.asDateTime().getValue().toInstant(ZoneOffset.UTC).toEpochMilli());
            } else if (attribute instanceof Attribute.Double) {
                builder.setDouble(attribute.asDouble().getValue());
            } else {
                throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
            }

            return builder.build();
        }

        public static AttributeType.ValueType valueType(ConceptProto.AttributeType.VALUE_TYPE valueType) {
            switch (valueType) {
                case OBJECT:
                    return AttributeType.ValueType.OBJECT;
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
            return valueType(attribute.getType());
        }

        public static ConceptProto.AttributeType.VALUE_TYPE valueType(AttributeType attributeType) {
            if (attributeType instanceof AttributeType.String) {
                return ConceptProto.AttributeType.VALUE_TYPE.STRING;
            } else if (attributeType instanceof AttributeType.Boolean) {
                return ConceptProto.AttributeType.VALUE_TYPE.BOOLEAN;
            } else if (attributeType instanceof AttributeType.Long) {
                return ConceptProto.AttributeType.VALUE_TYPE.LONG;
            } else if (attributeType instanceof AttributeType.Double) {
                return ConceptProto.AttributeType.VALUE_TYPE.DOUBLE;
            } else if (attributeType instanceof AttributeType.DateTime) {
                return ConceptProto.AttributeType.VALUE_TYPE.DATETIME;
            } else if (attributeType.isRoot()) {
                return ConceptProto.AttributeType.VALUE_TYPE.OBJECT;
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
