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
import grakn.common.collection.Pair;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.server.SessionService;
import grakn.protocol.AnswerProto;
import grakn.protocol.ConceptProto;
import grakn.protocol.DatabaseProto;
import grakn.protocol.LogicProto;
import grakn.protocol.QueryProto;
import grakn.protocol.SessionProto;
import grakn.protocol.TransactionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.time.ZoneOffset;
import java.util.List;
import java.util.regex.Pattern;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.server.common.ResponseBuilder.Answer.numeric;
import static grakn.core.server.common.ResponseBuilder.Concept.protoThing;
import static grakn.core.server.common.ResponseBuilder.Concept.protoType;
import static grakn.core.server.common.ResponseBuilder.Rule.protoRule;
import static java.util.stream.Collectors.toList;

public class ResponseBuilder {

    public static StatusRuntimeException exception(Throwable e) {
        if (e instanceof StatusRuntimeException) {
            return (StatusRuntimeException) e;
        } else {
            return Status.INTERNAL.withDescription(
                    e.getMessage() + " Please check server logs for the stack trace."
            ).asRuntimeException();
        }
    }

    public static class Database {

        public static DatabaseProto.Database.Contains.Res contains(boolean contains) {
            return DatabaseProto.Database.Contains.Res.newBuilder().setContains(contains).build();
        }

        public static DatabaseProto.Database.Create.Res create() {
            return DatabaseProto.Database.Create.Res.getDefaultInstance();
        }

        public static DatabaseProto.Database.All.Res all(List<String> names) {
            return DatabaseProto.Database.All.Res.newBuilder().addAllNames(names).build();
        }

        public static DatabaseProto.Database.Delete.Res delete() {
            return DatabaseProto.Database.Delete.Res.getDefaultInstance();
        }
    }

    public static class Session {

        public static SessionProto.Session.Open.Res open(SessionService sessionSrv, int durationMillis) {
            return SessionProto.Session.Open.Res.newBuilder().setSessionId(sessionSrv.UUIDAsByteString())
                    .setServerDurationMillis(durationMillis).build();
        }

        public static SessionProto.Session.Pulse.Res pulse(boolean isAlive) {
            return SessionProto.Session.Pulse.Res.newBuilder().setAlive(isAlive).build();
        }

        public static SessionProto.Session.Close.Res close() {
            return SessionProto.Session.Close.Res.newBuilder().build();
        }
    }

    public static class Transaction {

        public static TransactionProto.Transaction.Server serverMsg(TransactionProto.Transaction.Res res) {
            return TransactionProto.Transaction.Server.newBuilder().setRes(res).build();
        }

        public static TransactionProto.Transaction.Server serverMsg(TransactionProto.Transaction.ResPart resPart) {
            return TransactionProto.Transaction.Server.newBuilder().setResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res open(String requestID) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(requestID).setOpenRes(
                    TransactionProto.Transaction.Open.Res.getDefaultInstance()
            ).build();
        }

        public static TransactionProto.Transaction.ResPart stream(
                String requestID, TransactionProto.Transaction.Stream.State state) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(requestID).setStreamResPart(
                    TransactionProto.Transaction.Stream.ResPart.newBuilder().setState(state)
            ).build();
        }

        public static TransactionProto.Transaction.Res commit(String requestID) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(requestID).setCommitRes(
                    TransactionProto.Transaction.Commit.Res.getDefaultInstance()
            ).build();
        }

        public static TransactionProto.Transaction.Res rollback(String requestID) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(requestID).setRollbackRes(
                    TransactionProto.Transaction.Rollback.Res.getDefaultInstance()
            ).build();
        }
    }

    public static class QueryManager {

        private static TransactionProto.Transaction.Res queryMgrRes(String reqID, QueryProto.QueryManager.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(reqID).setQueryManagerRes(res).build();
        }

        private static TransactionProto.Transaction.ResPart queryMgrResPart(
                String reqID, QueryProto.QueryManager.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(reqID).setQueryManagerResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res defineRes(String reqID) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setDefineRes(
                    QueryProto.QueryManager.Define.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res undefineRes(String reqID) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setUndefineRes(
                    QueryProto.QueryManager.Undefine.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart matchResPart(String reqID, List<ConceptMap> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setMatchResPart(
                    QueryProto.QueryManager.Match.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMap).toList()
                    )));
        }

        public static TransactionProto.Transaction.Res matchAggregateRes(String reqID, Numeric answer) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setMatchAggregateRes(
                    QueryProto.QueryManager.MatchAggregate.Res.newBuilder().setAnswer(numeric(answer))
            ));
        }

        public static TransactionProto.Transaction.ResPart matchGroupResPart(String reqID, List<ConceptMapGroup> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setMatchGroupResPart(
                    QueryProto.QueryManager.MatchGroup.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMapGroup).toList())
            ));
        }

        public static TransactionProto.Transaction.ResPart matchGroupAggregateResPart(String reqID, List<NumericGroup> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setMatchGroupAggregateResPart(
                    QueryProto.QueryManager.MatchGroupAggregate.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::numericGroup).toList()))
            );
        }

        public static TransactionProto.Transaction.ResPart insertResPart(String reqID, List<ConceptMap> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setInsertResPart(
                    QueryProto.QueryManager.Insert.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMap).toList()))
            );
        }

        public static TransactionProto.Transaction.Res deleteRes(String reqID) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setDeleteRes(
                    QueryProto.QueryManager.Delete.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart updateResPart(String reqID, List<ConceptMap> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setUpdateResPart(
                    QueryProto.QueryManager.Update.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMap).toList()))
            );
        }
    }

    public static class ConceptManager {

        public static TransactionProto.Transaction.Res conceptMgrRes(String reqID, ConceptProto.ConceptManager.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(reqID).setConceptManagerRes(res).build();
        }

        public static TransactionProto.Transaction.Res putEntityTypeRes(String reqID, EntityType entityType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutEntityTypeRes(
                    ConceptProto.ConceptManager.PutEntityType.Res.newBuilder().setEntityType(protoType(entityType))
            ));
        }

        public static TransactionProto.Transaction.Res putRelationTypeRes(String reqID, RelationType relationType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutRelationTypeRes(
                    ConceptProto.ConceptManager.PutRelationType.Res.newBuilder().setRelationType(protoType(relationType))
            ));
        }

        public static TransactionProto.Transaction.Res putAttributeTypeRes(String reqID, AttributeType attributeType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutAttributeTypeRes(
                    ConceptProto.ConceptManager.PutAttributeType.Res.newBuilder().setAttributeType(protoType(attributeType))
            ));
        }

        public static TransactionProto.Transaction.Res getThingTypeRes(String reqID, ThingType thingType) {
            ConceptProto.ConceptManager.GetThingType.Res.Builder getThingTypeRes = ConceptProto.ConceptManager.GetThingType.Res.newBuilder();
            if (thingType != null) getThingTypeRes.setThingType(protoType(thingType));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetThingTypeRes(getThingTypeRes));
        }

        public static TransactionProto.Transaction.Res getThingRes(String reqID, @Nullable grakn.core.concept.thing.Thing thing) {
            ConceptProto.ConceptManager.GetThing.Res.Builder getThingRes = ConceptProto.ConceptManager.GetThing.Res.newBuilder();
            if (thing != null) getThingRes.setThing(protoThing(thing));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetThingRes(getThingRes));
        }
    }

    public static class LogicManager {

        public static TransactionProto.Transaction.Res logicMgrRes(String reqID, LogicProto.LogicManager.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(reqID).setLogicManagerRes(res).build();
        }

        private static TransactionProto.Transaction.ResPart logicMgrResPart(
                String reqID, LogicProto.LogicManager.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(reqID).setLogicManagerResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res putRuleRes(String reqID, grakn.core.logic.Rule rule) {
            return logicMgrRes(reqID, LogicProto.LogicManager.Res.newBuilder().setPutRuleRes(
                    LogicProto.LogicManager.PutRule.Res.newBuilder().setRule(protoRule(rule))
            ));
        }

        public static TransactionProto.Transaction.Res getRuleRes(String reqID, grakn.core.logic.Rule rule) {
            LogicProto.LogicManager.GetRule.Res.Builder getRuleRes = LogicProto.LogicManager.GetRule.Res.newBuilder();
            if (rule != null) getRuleRes.setRule(protoRule(rule));
            return logicMgrRes(reqID, LogicProto.LogicManager.Res.newBuilder().setGetRuleRes(getRuleRes));
        }

        public static TransactionProto.Transaction.ResPart getRulesResPart(String reqID, List<grakn.core.logic.Rule> rules) {
            return logicMgrResPart(reqID, LogicProto.LogicManager.ResPart.newBuilder().setGetRulesResPart(
                    LogicProto.LogicManager.GetRules.ResPart.newBuilder().addAllRules(
                            rules.stream().map(Rule::protoRule).collect(toList()))
            ));
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

        public static ConceptProto.Concept protoConcept(grakn.core.concept.Concept concept) {
            if (concept == null) return null;
            if (concept.isThing()) {
                return ConceptProto.Concept.newBuilder().setThing(protoThing(concept.asThing())).build();
            } else {
                return ConceptProto.Concept.newBuilder().setType(protoType(concept.asType())).build();
            }
        }

        public static ConceptProto.Thing protoThing(grakn.core.concept.thing.Thing thing) {
            ConceptProto.Thing.Builder protoThing = ConceptProto.Thing.newBuilder()
                    .setIid(ByteString.copyFrom(thing.getIID()))
                    .setType(protoType(thing.getType()));
            if (thing.isAttribute()) protoThing.setValue(attributeValue(thing.asAttribute()));
            return protoThing.build();
        }

        public static ConceptProto.Type protoType(grakn.core.concept.type.Type type) {
            ConceptProto.Type.Builder protoType = ConceptProto.Type.newBuilder()
                    .setLabel(type.getLabel().name()).setEncoding(getEncoding(type));
            if (type.isAttributeType()) protoType.setValueType(valueType(type.asAttributeType()));
            if (type.isRoleType()) protoType.setScope(type.asRoleType().getLabel().scope().get());
            if (type.isRoot()) protoType.setRoot(true);
            return protoType.build();
        }

        private static ConceptProto.Type.Encoding getEncoding(grakn.core.concept.type.Type type) {
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

    public static class Type {

        private static TransactionProto.Transaction.Res typeRes(String reqID, ConceptProto.Type.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(reqID).setTypeRes(res).build();
        }

        private static TransactionProto.Transaction.ResPart typeResPart(String reqID, ConceptProto.Type.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(reqID).setTypeResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res setLabelRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeSetLabelRes(
                    ConceptProto.Type.SetLabel.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res isAbstractRes(String reqID, boolean isAbstract) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeIsAbstractRes(
                    ConceptProto.Type.IsAbstract.Res.newBuilder().setAbstract(isAbstract)
            ));
        }

        public static TransactionProto.Transaction.Res setAbstractRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeSetAbstractRes(
                    ConceptProto.ThingType.SetAbstract.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res unsetAbstractRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetAbstractRes(
                    ConceptProto.ThingType.UnsetAbstract.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res getSupertypeRes(
                String reqID, @Nullable grakn.core.concept.type.Type supertype) {
            ConceptProto.Type.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.Type.GetSupertype.Res.newBuilder();
            if (supertype != null) getSupertypeRes.setType(protoType(supertype));
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeGetSupertypeRes(getSupertypeRes));
        }

        public static TransactionProto.Transaction.Res setSupertypeRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeSetSupertypeRes(
                    ConceptProto.Type.SetSupertype.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getSupertypesResPart(
                String reqID, List<? extends grakn.core.concept.type.Type> types) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setTypeGetSupertypesResPart(
                    ConceptProto.Type.GetSupertypes.ResPart.newBuilder().addAllTypes(
                            types.stream().map(Concept::protoType).collect(toList()))));
        }

        public static TransactionProto.Transaction.ResPart getSubtypesResPart(
                String reqID, List<? extends grakn.core.concept.type.Type> types) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setTypeGetSubtypesResPart(
                    ConceptProto.Type.GetSubtypes.ResPart.newBuilder().addAllTypes(
                            types.stream().map(Concept::protoType).collect(toList()))));
        }

        public static TransactionProto.Transaction.ResPart getInstancesResPart(
                String reqID, List<? extends grakn.core.concept.thing.Thing> things) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setThingTypeGetInstancesResPart(
                    ConceptProto.ThingType.GetInstances.ResPart.newBuilder().addAllThings(
                            things.stream().map(Concept::protoThing).collect(toList()))));
        }

        public static TransactionProto.Transaction.ResPart getOwnsResPart(
                String reqID, List<? extends AttributeType> attributeTypes) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setThingTypeGetOwnsResPart(
                    ConceptProto.ThingType.GetOwns.ResPart.newBuilder().addAllAttributeTypes(
                            attributeTypes.stream().map(Concept::protoType).collect(toList()))));
        }

        public static TransactionProto.Transaction.ResPart getPlaysResPart(
                String reqID, List<? extends RoleType> roleTypes) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setThingTypeGetPlaysResPart(
                    ConceptProto.ThingType.GetPlays.ResPart.newBuilder().addAllRoles(
                            roleTypes.stream().map(Concept::protoType).collect(toList()))));
        }

        public static TransactionProto.Transaction.Res setOwnsRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeSetOwnsRes(
                    ConceptProto.ThingType.SetOwns.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res setPlaysRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeSetPlaysRes(
                    ConceptProto.ThingType.SetPlays.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res unsetOwnsRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetOwnsRes(
                    ConceptProto.ThingType.UnsetOwns.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res createRes(String reqID, Entity entity) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setEntityTypeCreateRes(
                    ConceptProto.EntityType.Create.Res.newBuilder().setEntity(protoThing(entity))
            ));
        }

        public static TransactionProto.Transaction.Res createRes(String reqID, Relation relation) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeCreateRes(
                    ConceptProto.RelationType.Create.Res.newBuilder().setRelation(protoThing(relation))
            ));
        }

        public static TransactionProto.Transaction.Res unsetPlaysRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetPlaysRes(
                    ConceptProto.ThingType.UnsetPlays.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getOwnersResPart(
                String reqID, List<? extends ThingType> owners) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setAttributeTypeGetOwnersResPart(
                    ConceptProto.AttributeType.GetOwners.ResPart.newBuilder().addAllOwners(
                            owners.stream().map(Concept::protoType).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.Res putRes(String reqID, Attribute attribute) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypePutRes(
                    ConceptProto.AttributeType.Put.Res.newBuilder().setAttribute(protoThing(attribute))
            ));
        }

        public static TransactionProto.Transaction.Res getRes(String reqID, @Nullable Attribute attribute) {
            ConceptProto.AttributeType.Get.Res.Builder getAttributeTypeRes = ConceptProto.AttributeType.Get.Res.newBuilder();
            if (attribute != null) getAttributeTypeRes.setAttribute(protoThing(attribute));
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRes(getAttributeTypeRes));
        }

        public static TransactionProto.Transaction.Res getRegexRes(String reqID, Pattern regex) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRegexRes(
                    ConceptProto.AttributeType.GetRegex.Res.newBuilder().setRegex((regex != null) ? regex.pattern() : "")
            ));
        }

        public static TransactionProto.Transaction.Res setRegexRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypeSetRegexRes(
                    ConceptProto.AttributeType.SetRegex.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getRelatesResPart(
                String reqID, List<? extends RoleType> roleTypes) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setRelationTypeGetRelatesResPart(
                    ConceptProto.RelationType.GetRelates.ResPart.newBuilder().addAllRoles(
                            roleTypes.stream().map(Concept::protoType).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.Res getRelatesForRoleLabelRes(String reqID, @Nullable RoleType roleType) {
            ConceptProto.RelationType.GetRelatesForRoleLabel.Res.Builder getRelatesRes =
                    ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder();
            if (roleType != null) getRelatesRes.setRoleType(protoType(roleType));
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeGetRelatesForRoleLabelRes(getRelatesRes));
        }

        public static TransactionProto.Transaction.Res setRelatesRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeSetRelatesRes(
                    ConceptProto.RelationType.SetRelates.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res unsetRelatesRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeUnsetRelatesRes(
                    ConceptProto.RelationType.UnsetRelates.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getRelationTypesResPart(String reqID, List<? extends RelationType> relationTypes) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setRoleTypeGetRelationTypesResPart(
                    ConceptProto.RoleType.GetRelationTypes.ResPart.newBuilder().addAllRelationTypes(
                            relationTypes.stream().map(Concept::protoType).collect(toList()))));
        }

        public static TransactionProto.Transaction.ResPart getPlayersResPart(String reqID, List<? extends ThingType> players) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setRoleTypeGetPlayersResPart(
                    ConceptProto.RoleType.GetPlayers.ResPart.newBuilder().addAllThingTypes(
                            players.stream().map(Concept::protoType).collect(toList()))));
        }

        public static TransactionProto.Transaction.Res deleteRes(String reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeDeleteRes(
                    ConceptProto.Type.Delete.Res.getDefaultInstance()
            ));
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
    }

    public static class Thing {

        public static TransactionProto.Transaction.Res thingRes(String reqID, ConceptProto.Thing.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(reqID).setThingRes(res).build();
        }

        public static TransactionProto.Transaction.ResPart thingResPart(
                String reqID, ConceptProto.Thing.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(reqID).setThingResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res isInferredRes(String reqID, boolean isInferred) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingIsInferredRes(
                    ConceptProto.Thing.IsInferred.Res.newBuilder().setInferred(isInferred)
            ));
        }

        public static TransactionProto.Transaction.Res getTypeRes(String reqID, ThingType thingType) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingGetTypeRes(
                    ConceptProto.Thing.GetType.Res.newBuilder().setThingType(protoType(thingType))
            ));
        }

        public static TransactionProto.Transaction.ResPart getHasResPart(String reqID, List<? extends Attribute> attributes) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetHasResPart(
                    ConceptProto.Thing.GetHas.ResPart.newBuilder().addAllAttributes(
                            attributes.stream().map(Concept::protoThing).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.ResPart getRelationsResPart(String reqID, List<? extends Relation> relations) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetRelationsResPart(
                    ConceptProto.Thing.GetRelations.ResPart.newBuilder().addAllRelations(
                            relations.stream().map(Concept::protoThing).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.ResPart getPlaysResPart(String reqID, List<? extends RoleType> roleTypes) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetPlaysResPart(
                    ConceptProto.Thing.GetPlays.ResPart.newBuilder().addAllRoleTypes(
                            roleTypes.stream().map(Concept::protoType).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.Res deleteRes(String reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingDeleteRes(
                    ConceptProto.Thing.Delete.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getPlayersByRoleTypeResPart(
                String reqID, List<Pair<RoleType, grakn.core.concept.thing.Thing>> rolePlayers) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetPlayersByRoleTypeResPart(
                    ConceptProto.Relation.GetPlayersByRoleType.ResPart.newBuilder().addAllRoleTypesWithPlayers(
                            rolePlayers.stream().map(rp -> ConceptProto.Relation.GetPlayersByRoleType.RoleTypeWithPlayer.newBuilder()
                                    .setRoleType(protoType(rp.first())).setPlayer(protoThing(rp.second())).build()).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.ResPart getPlayersResPart(
                String reqID, List<? extends grakn.core.concept.thing.Thing> players) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetPlayersResPart(
                    ConceptProto.Relation.GetPlayers.ResPart.newBuilder().addAllThings(
                            players.stream().map(Concept::protoThing).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.Res addPlayerRes(String reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setRelationAddPlayerRes(
                    ConceptProto.Relation.AddPlayer.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res removePlayerRes(String reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setRelationRemovePlayerRes(
                    ConceptProto.Relation.RemovePlayer.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getOwnersResPart(String reqID, List<? extends grakn.core.concept.thing.Thing> owners) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setAttributeGetOwnersResPart(
                    ConceptProto.Attribute.GetOwners.ResPart.newBuilder().addAllThings(
                            owners.stream().map(Concept::protoThing).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.Res setHasRes(String reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingSetHasRes(
                    ConceptProto.Thing.SetHas.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res unsetHasRes(String reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingUnsetHasRes(
                    ConceptProto.Thing.UnsetHas.Res.getDefaultInstance()
            ));
        }
    }

    public static class Rule {

        public static LogicProto.Rule protoRule(grakn.core.logic.Rule rule) {
            return LogicProto.Rule.newBuilder().setLabel(rule.getLabel())
                    .setWhen(rule.getWhenPreNormalised().toString())
                    .setThen(rule.getThenPreNormalised().toString()).build();
        }

        public static TransactionProto.Transaction.Res ruleRes(String reqID, LogicProto.Rule.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(reqID).setRuleRes(res).build();
        }

        public static TransactionProto.Transaction.Res setLabelRes(String reqID) {
            return ruleRes(reqID, LogicProto.Rule.Res.newBuilder().setRuleSetLabelRes(
                    LogicProto.Rule.SetLabel.Res.getDefaultInstance())
            );
        }

        public static TransactionProto.Transaction.Res deleteRes(String reqID) {
            return ruleRes(reqID, LogicProto.Rule.Res.newBuilder().setRuleDeleteRes(
                    LogicProto.Rule.Delete.Res.getDefaultInstance())
            );
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
                    ConceptProto.Concept conceptProto = ResponseBuilder.Concept.protoConcept(concept);
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
                    .setOwner(ResponseBuilder.Concept.protoConcept(answer.owner()))
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
                    .setOwner(Concept.protoConcept(answer.owner()))
                    .setNumber(numeric(answer.numeric()))
                    .build();
        }
    }
}
