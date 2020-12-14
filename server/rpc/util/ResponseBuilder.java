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
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import grakn.core.logic.Rule;
import grakn.protocol.AnswerProto;
import grakn.protocol.ConceptProto;
import grakn.protocol.LogicProto;
import grakn.protocol.TransactionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.ZoneOffset;
import java.util.stream.Collectors;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_ANSWER_TYPE;

public class ResponseBuilder {

    public static StatusRuntimeException exception(Throwable e) {
        if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        } else {
            return exception(Status.INTERNAL, e.getMessage());
        }
    }

    private static StatusRuntimeException exception(Status status, String message) {
        return status.withDescription(message + " Please check server logs for the stack trace.").asRuntimeException();
    }

    public static class Transaction {

        public static TransactionProto.Transaction.Res done(String id) {
            return TransactionProto.Transaction.Res.newBuilder().setId(id).setDone(true).build();
        }

        public static TransactionProto.Transaction.Res continueRes(String id) {
            return TransactionProto.Transaction.Res.newBuilder().setId(id).setContinue(true).build();
        }
    }

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

    public static class Concept {

        public static ConceptProto.Concept concept(grakn.core.concept.Concept concept) {
            if (concept == null) return null;
            if (concept instanceof Thing) {
                return ConceptProto.Concept.newBuilder().setThing(thing(concept.asThing())).build();
            } else {
                return ConceptProto.Concept.newBuilder().setType(type(concept.asType())).build();
            }
        }

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
                    .setLabel(type.getLabel().name())
                    .setEncoding(getEncoding(type));
            if (type instanceof AttributeType) builder.setValueType(valueType(type.asAttributeType()));
            if (type instanceof RoleType) builder.setScope(type.asRoleType().getLabel().scope().get());
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

        private static ConceptProto.Thing.Encoding getEncoding(Thing thing) {
            if (thing instanceof Entity) {
                return ConceptProto.Thing.Encoding.ENTITY;
            } else if (thing instanceof Relation) {
                return ConceptProto.Thing.Encoding.RELATION;
            } else if (thing instanceof Attribute) {
                return ConceptProto.Thing.Encoding.ATTRIBUTE;
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        private static ConceptProto.Type.Encoding getEncoding(Type type) {
            if (type instanceof EntityType) {
                return ConceptProto.Type.Encoding.ENTITY_TYPE;
            } else if (type instanceof RelationType) {
                return ConceptProto.Type.Encoding.RELATION_TYPE;
            } else if (type instanceof AttributeType) {
                return ConceptProto.Type.Encoding.ATTRIBUTE_TYPE;
            } else if (type instanceof ThingType) {
                return ConceptProto.Type.Encoding.THING_TYPE;
            } else if (type instanceof RoleType) {
                return ConceptProto.Type.Encoding.ROLE_TYPE;
            } else {
                throw GraknException.of(ILLEGAL_STATE);
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
                builder.setDateTime(attribute.asDateTime().getValue().toInstant(ZoneOffset.UTC).toEpochMilli());
            } else if (attribute instanceof Attribute.Double) {
                builder.setDouble(attribute.asDouble().getValue());
            } else {
                throw GraknException.of(ErrorMessage.Server.BAD_VALUE_TYPE);
            }

            return builder.build();
        }

        public static AttributeType.ValueType valueType(ConceptProto.AttributeType.ValueType valueType) {
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
                case UNRECOGNIZED:
                default:
                    throw GraknException.of(BAD_VALUE_TYPE, valueType);
            }
        }

        public static ConceptProto.AttributeType.ValueType valueType(Attribute attribute) {
            return valueType(attribute.getType());
        }

        public static ConceptProto.AttributeType.ValueType valueType(AttributeType attributeType) {
            if (attributeType instanceof AttributeType.String) {
                return ConceptProto.AttributeType.ValueType.STRING;
            } else if (attributeType instanceof AttributeType.Boolean) {
                return ConceptProto.AttributeType.ValueType.BOOLEAN;
            } else if (attributeType instanceof AttributeType.Long) {
                return ConceptProto.AttributeType.ValueType.LONG;
            } else if (attributeType instanceof AttributeType.Double) {
                return ConceptProto.AttributeType.ValueType.DOUBLE;
            } else if (attributeType instanceof AttributeType.DateTime) {
                return ConceptProto.AttributeType.ValueType.DATETIME;
            } else if (attributeType.isRoot()) {
                return ConceptProto.AttributeType.ValueType.OBJECT;
            } else {
                throw GraknException.of(ErrorMessage.Server.BAD_VALUE_TYPE);
            }
        }
    }

    public static class Logic {

        public static LogicProto.Rule rule(Rule rule) {
            final LogicProto.Rule.Builder builder = LogicProto.Rule.newBuilder()
                    .setLabel(rule.getLabel())
                    .setWhen(rule.getWhenPreNormalised().toString())
                    .setThen(rule.getThenPreNormalised().toString());
            return builder.build();
        }

    }

    /**
     * An RPC Response Builder class for Answer responses
     */
    public static class Answer {

        public static AnswerProto.Answer answer(Object object) {
            final AnswerProto.Answer.Builder answer = AnswerProto.Answer.newBuilder();

            if (object instanceof AnswerGroup) {
                answer.setAnswerGroup(answerGroup((AnswerGroup<?>) object));
            } else if (object instanceof ConceptMap) {
                answer.setConceptMap(conceptMap((ConceptMap) object));
            } else if (object instanceof Number) {
                answer.setNumber(number((Number) object));
            } else {
                throw GraknException.of(UNKNOWN_ANSWER_TYPE, className(object.getClass()));
            }

            return answer.build();
        }

        public static AnswerProto.AnswerGroup answerGroup(AnswerGroup<?> answer) {
            final AnswerProto.AnswerGroup.Builder answerGroupProto = AnswerProto.AnswerGroup.newBuilder()
                    .setOwner(ResponseBuilder.Concept.concept(answer.owner()))
                    .addAllAnswers(answer.answers().stream().map(Answer::answer).collect(Collectors.toList()));

            return answerGroupProto.build();
        }

        public static AnswerProto.ConceptMap conceptMap(ConceptMap answer) {
            final AnswerProto.ConceptMap.Builder conceptMapProto = AnswerProto.ConceptMap.newBuilder();
            // TODO: needs testing
            answer.concepts().forEach((ref, concept) -> {
                final ConceptProto.Concept conceptProto = ResponseBuilder.Concept.concept(concept);
                conceptMapProto.putMap(ref.toString(), conceptProto);
            });

            // TODO
//            if (answer.getPattern() != null) {
//                conceptMapProto.setPattern(answer.getPattern().toString());
//            }
//
//            if (answer.explanation() != null && !answer.explanation().isEmpty()) {
//                conceptMapProto.setHasExplanation(true);
//            } else {
//                conceptMapProto.setHasExplanation(false);
//            }
            return conceptMapProto.build();
        }

        public static AnswerProto.Number number(Number number) {
            return AnswerProto.Number.newBuilder().setValue(number.toString()).build();
        }
    }
}
