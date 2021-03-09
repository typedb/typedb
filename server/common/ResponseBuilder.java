/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.server.common;

import com.google.protobuf.ByteString;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
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

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static java.util.stream.Collectors.toList;

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

        public static TransactionProto.Transaction.Res iterate(String id, boolean hasNext) {
            return TransactionProto.Transaction.Res.newBuilder().setId(id).setIterateRes(
                    TransactionProto.Transaction.Iterate.Res.newBuilder().setHasNext(hasNext)
            ).build();
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
            if (concept.isThing()) {
                return ConceptProto.Concept.newBuilder().setThing(thing(concept.asThing())).build();
            } else {
                return ConceptProto.Concept.newBuilder().setType(type(concept.asType())).build();
            }
        }

        public static ConceptProto.Thing thing(Thing thing) {
            ConceptProto.Thing.Builder builder = ConceptProto.Thing.newBuilder()
                    .setIid(ByteString.copyFrom(thing.getIID()))
                    .setType(type(thing.getType()));
            if (thing.isAttribute()) builder.setValue(attributeValue(thing.asAttribute()));
            return builder.build();
        }

        public static ConceptProto.Type type(Type type) {
            ConceptProto.Type.Builder builder = ConceptProto.Type.newBuilder()
                    .setLabel(type.getLabel().name())
                    .setEncoding(getEncoding(type));
            if (type.isAttributeType()) builder.setValueType(valueType(type.asAttributeType()));
            if (type.isRoleType()) builder.setScope(type.asRoleType().getLabel().scope().get());
            if (type.isRoot()) builder.setRoot(true);
            return builder.build();
        }

        private static ConceptProto.Type.Encoding getEncoding(Type type) {
            if (type.isEntityType()) {
                return ConceptProto.Type.Encoding.ENTITY_TYPE;
            } else if (type.isRelationType()) {
                return ConceptProto.Type.Encoding.RELATION_TYPE;
            } else if (type.isAttributeType()) {
                return ConceptProto.Type.Encoding.ATTRIBUTE_TYPE;
            } else if (type.isThingType()) {
                return ConceptProto.Type.Encoding.THING_TYPE;
            } else if (type.isRoleType()) {
                return ConceptProto.Type.Encoding.ROLE_TYPE;
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        public static ConceptProto.Attribute.Value attributeValue(Attribute attribute) {
            ConceptProto.Attribute.Value.Builder builder = ConceptProto.Attribute.Value.newBuilder();

            if (attribute.isString()) {
                builder.setString(attribute.asString().getValue());
            } else if (attribute.isLong()) {
                builder.setLong(attribute.asLong().getValue());
            } else if (attribute.isBoolean()) {
                builder.setBoolean(attribute.asBoolean().getValue());
            } else if (attribute.isDateTime()) {
                builder.setDateTime(attribute.asDateTime().getValue().toInstant(ZoneOffset.UTC).toEpochMilli());
            } else if (attribute.isDouble()) {
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
            if (attributeType.isString()) {
                return ConceptProto.AttributeType.ValueType.STRING;
            } else if (attributeType.isBoolean()) {
                return ConceptProto.AttributeType.ValueType.BOOLEAN;
            } else if (attributeType.isLong()) {
                return ConceptProto.AttributeType.ValueType.LONG;
            } else if (attributeType.isDouble()) {
                return ConceptProto.AttributeType.ValueType.DOUBLE;
            } else if (attributeType.isDateTime()) {
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
            LogicProto.Rule.Builder builder = LogicProto.Rule.newBuilder()
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

        public static AnswerProto.ConceptMap conceptMap(ConceptMap answer) {
            AnswerProto.ConceptMap.Builder conceptMapProto = AnswerProto.ConceptMap.newBuilder();
            // TODO: needs testing
            answer.concepts().forEach((id, concept) -> {
                if (id.isName()) {
                    ConceptProto.Concept conceptProto = ResponseBuilder.Concept.concept(concept);
                    conceptMapProto.putMap(id.asVariable().asName().reference().name(), conceptProto);
                }
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

        public static AnswerProto.ConceptMapGroup conceptMapGroup(ConceptMapGroup answer) {
            return AnswerProto.ConceptMapGroup.newBuilder()
                    .setOwner(ResponseBuilder.Concept.concept(answer.owner()))
                    .addAllConceptMaps(answer.conceptMaps().stream().map(ResponseBuilder.Answer::conceptMap).collect(toList()))
                    .build();
        }

        public static AnswerProto.Numeric numeric(Numeric answer) {
            AnswerProto.Numeric.Builder builder = AnswerProto.Numeric.newBuilder();
            if (answer.isLong()) {
                builder.setLongValue(answer.asLong());
            } else if (answer.isDouble()) {
                builder.setDoubleValue(answer.asDouble());
            } else if (answer.isNaN()) {
                builder.setNan(true);
            }
            return builder.build();
        }

        public static AnswerProto.NumericGroup numericGroup(NumericGroup answer) {
            return AnswerProto.NumericGroup.newBuilder()
                    .setOwner(Concept.concept(answer.owner()))
                    .setNumber(numeric(answer.numeric()))
                    .build();
        }
    }
}
