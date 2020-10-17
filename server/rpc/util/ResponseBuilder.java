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
import grakn.core.concept.schema.Rule;
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
import grakn.protocol.AnswerProto;
import grakn.protocol.ConceptProto;
import grakn.protocol.QueryProto;
import grakn.protocol.TransactionProto;
import grakn.protocol.TransactionProto.Transaction.GetRule;
import grakn.protocol.TransactionProto.Transaction.GetThing;
import grakn.protocol.TransactionProto.Transaction.GetType;
import grakn.protocol.TransactionProto.Transaction.PutAttributeType;
import grakn.protocol.TransactionProto.Transaction.PutEntityType;
import grakn.protocol.TransactionProto.Transaction.PutRelationType;
import grakn.protocol.TransactionProto.Transaction.PutRule;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.time.ZoneOffset;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_ANSWER_TYPE;
import static java.lang.String.format;

public class ResponseBuilder {

    public static StatusRuntimeException exception(final Throwable e) {
        if (e instanceof GraknException) {
            return exception(Status.INTERNAL, e.getMessage());
        } else if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        } else {
            return exception(Status.UNKNOWN, e.getMessage());
        }
    }

    private static StatusRuntimeException exception(final Status status, final String message) {
        return status.withDescription(message + " Please check server logs for the stack trace.").asRuntimeException();
    }

    public static class Transaction {

        public static TransactionProto.Transaction.Res open() {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setOpenRes(TransactionProto.Transaction.Open.Res.getDefaultInstance()).build();
        }

        public static TransactionProto.Transaction.Res commit() {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setCommitRes(TransactionProto.Transaction.Commit.Res.getDefaultInstance()).build();
        }

        public static TransactionProto.Transaction.Res rollback() {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setRollbackRes(TransactionProto.Transaction.Rollback.Res.getDefaultInstance()).build();
        }

        public static TransactionProto.Transaction.Res getThing(@Nullable final Thing thing) {
            final GetThing.Res.Builder res = GetThing.Res.newBuilder();
            if (thing != null) res.setThing(Concept.thing(thing));
            return TransactionProto.Transaction.Res.newBuilder().setGetThingRes(res).build();
        }

        public static TransactionProto.Transaction.Res getType(@Nullable final Type type) {
            final GetType.Res.Builder res = GetType.Res.newBuilder();
            if (type != null) res.setType(Concept.type(type));
            return TransactionProto.Transaction.Res.newBuilder().setGetTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res getRule(@Nullable final Rule rule) {
            final GetRule.Res.Builder res = GetRule.Res.newBuilder();
            if (rule != null) res.setRule(Concept.rule(rule));
            return TransactionProto.Transaction.Res.newBuilder().setGetRuleRes(res).build();
        }

        public static TransactionProto.Transaction.Res putEntityType(final EntityType entityType) {
            final PutEntityType.Res.Builder res = PutEntityType.Res.newBuilder()
                    .setEntityType(ResponseBuilder.Concept.type(entityType));
            return TransactionProto.Transaction.Res.newBuilder().setPutEntityTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putAttributeType(final AttributeType attributeType) {
            final PutAttributeType.Res.Builder res = PutAttributeType.Res.newBuilder()
                    .setAttributeType(ResponseBuilder.Concept.type(attributeType));
            return TransactionProto.Transaction.Res.newBuilder().setPutAttributeTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putRelationType(final RelationType relationType) {
            final PutRelationType.Res.Builder res = PutRelationType.Res.newBuilder()
                    .setRelationType(ResponseBuilder.Concept.type(relationType));
            return TransactionProto.Transaction.Res.newBuilder().setPutRelationTypeRes(res).build();
        }

        public static TransactionProto.Transaction.Res putRule(final Rule rule) {
            final PutRule.Res.Builder res = PutRule.Res.newBuilder().setRule(ResponseBuilder.Concept.rule(rule));
            return TransactionProto.Transaction.Res.newBuilder().setPutRuleRes(res).build();
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

        public static class Iter {

            public static TransactionProto.Transaction.Res done() {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setDone(true)).build();
            }

            public static TransactionProto.Transaction.Res id(final int id) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setIteratorID(id)).build();
            }

            public static TransactionProto.Transaction.Res query(final QueryProto.Query.Iter.Res res) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setQueryIterRes(res)).build();
            }

            public static TransactionProto.Transaction.Res thingMethod(final ConceptProto.ThingMethod.Iter.Res res) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setConceptMethodThingIterRes(res)).build();
            }

            public static TransactionProto.Transaction.Res typeMethod(final ConceptProto.TypeMethod.Iter.Res res) {
                return TransactionProto.Transaction.Res.newBuilder()
                        .setIterRes(TransactionProto.Transaction.Iter.Res.newBuilder()
                                            .setConceptMethodTypeIterRes(res)).build();
            }
        }
    }

    public static class Concept {

        public static ConceptProto.Concept concept(final grakn.core.concept.Concept concept) {
            if (concept instanceof Thing) {
                return ConceptProto.Concept.newBuilder().setThing(thing(concept.asThing())).build();
            } else {
                return ConceptProto.Concept.newBuilder().setType(type(concept.asType())).build();
            }
        }

        public static ConceptProto.Thing thing(final Thing thing) {
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

        public static ConceptProto.Type type(final Type type) {
            final ConceptProto.Type.Builder builder = ConceptProto.Type.newBuilder()
                    .setLabel(type.getLabel())
                    .setEncoding(getEncoding(type));
            if (type instanceof AttributeType) builder.setValueType(valueType(type.asAttributeType()));
            if (type instanceof RoleType) builder.setScope(type.asRoleType().getScope());
            if (type.isRoot()) builder.setRoot(true);
            return builder.build();
        }

        public static ConceptProto.Rule rule(final Rule rule) {
            final ConceptProto.Rule.Builder builder = ConceptProto.Rule.newBuilder()
                    .setLabel(rule.getLabel())
                    .setWhen(rule.getWhenPreNormalised().toString())
                    .setThen(rule.getThenPreNormalised().toString());
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

        private static ConceptProto.Thing.ENCODING getEncoding(final Thing thing) {
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

        private static ConceptProto.Type.ENCODING getEncoding(final Type type) {
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
            } else {
                throw new GraknException(ErrorMessage.Internal.ILLEGAL_STATE);
            }
        }

        public static ConceptProto.Attribute.Value attributeValue(final Attribute attribute) {
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

        public static AttributeType.ValueType valueType(final ConceptProto.AttributeType.VALUE_TYPE valueType) {
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

        public static ConceptProto.AttributeType.VALUE_TYPE valueType(final Attribute attribute) {
            return valueType(attribute.getType());
        }

        public static ConceptProto.AttributeType.VALUE_TYPE valueType(final AttributeType attributeType) {
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

    /**
     * An RPC Response Builder class for Answer responses
     */
    public static class Answer {

        public static AnswerProto.Answer answer(final Object object) {
            final AnswerProto.Answer.Builder answer = AnswerProto.Answer.newBuilder();

            if (object instanceof AnswerGroup) {
                answer.setAnswerGroup(answerGroup((AnswerGroup<?>) object));
            } else if (object instanceof ConceptMap) {
                answer.setConceptMap(conceptMap((ConceptMap) object));
            } else if (object instanceof Number) {
                answer.setNumber(number((Number) object));
            } else {
                throw new GraknException(UNKNOWN_ANSWER_TYPE);
            }

            return answer.build();
        }

        public static AnswerProto.AnswerGroup answerGroup(final AnswerGroup<?> answer) {
            final AnswerProto.AnswerGroup.Builder answerGroupProto = AnswerProto.AnswerGroup.newBuilder()
                    .setOwner(ResponseBuilder.Concept.concept(answer.owner()))
                    .addAllAnswers(answer.answers().stream().map(Answer::answer).collect(Collectors.toList()));

            return answerGroupProto.build();
        }

        public static AnswerProto.ConceptMap conceptMap(final ConceptMap answer) {
            final AnswerProto.ConceptMap.Builder conceptMapProto = AnswerProto.ConceptMap.newBuilder();
            // TODO: needs testing
            answer.concepts().forEach((ref, concept) -> {
                final ConceptProto.Concept conceptProto = ResponseBuilder.Concept.concept(concept);
                conceptMapProto.putMap(ref.identifier(), conceptProto);
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

        public static AnswerProto.Number number(final Number number) {
            return AnswerProto.Number.newBuilder().setValue(number.toString()).build();
        }
    }
}
