/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server.common;

import com.google.protobuf.ByteString;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.Numeric;
import com.vaticle.typedb.core.concept.answer.NumericGroup;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.protocol.AnswerProto;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.CoreDatabaseProto.CoreDatabase;
import com.vaticle.typedb.protocol.CoreDatabaseProto.CoreDatabaseManager;
import com.vaticle.typedb.protocol.LogicProto;
import com.vaticle.typedb.protocol.QueryProto;
import com.vaticle.typedb.protocol.SessionProto;
import com.vaticle.typedb.protocol.TransactionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.google.protobuf.ByteString.copyFrom;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeUUID;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Answer.conceptMap;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Answer.numeric;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Concept.protoThing;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Logic.Rule.protoRule;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.protoType;
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

    public static ByteString UUIDAsByteString(UUID uuid) {
        return copyFrom(encodeUUID(uuid).getArray());
    }

    public static class DatabaseManager {

        public static CoreDatabaseManager.Contains.Res containsRes(boolean contains) {
            return CoreDatabaseManager.Contains.Res.newBuilder().setContains(contains).build();
        }

        public static CoreDatabaseManager.Create.Res createRes() {
            return CoreDatabaseManager.Create.Res.getDefaultInstance();
        }

        public static CoreDatabaseManager.All.Res allRes(List<String> names) {
            return CoreDatabaseManager.All.Res.newBuilder().addAllNames(names).build();
        }
    }

    public static class Database {

        public static CoreDatabase.Schema.Res schemaRes(String schema) {
            return CoreDatabase.Schema.Res.newBuilder().setSchema(schema).build();
        }

        public static CoreDatabase.Delete.Res deleteRes() {
            return CoreDatabase.Delete.Res.getDefaultInstance();
        }
    }

    public static class Session {

        public static SessionProto.Session.Open.Res openRes(UUID sessionID, int durationMillis) {
            return SessionProto.Session.Open.Res.newBuilder().setSessionId(UUIDAsByteString(sessionID))
                    .setServerDurationMillis(durationMillis).build();
        }

        public static SessionProto.Session.Pulse.Res pulseRes(boolean isAlive) {
            return SessionProto.Session.Pulse.Res.newBuilder().setAlive(isAlive).build();
        }

        public static SessionProto.Session.Close.Res closeRes() {
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

        public static TransactionProto.Transaction.Res open(UUID requestID) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(requestID)).setOpenRes(
                    TransactionProto.Transaction.Open.Res.getDefaultInstance()
            ).build();
        }

        public static TransactionProto.Transaction.ResPart stream(
                UUID requestID, TransactionProto.Transaction.Stream.State state) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(UUIDAsByteString(requestID)).setStreamResPart(
                    TransactionProto.Transaction.Stream.ResPart.newBuilder().setState(state)
            ).build();
        }

        public static TransactionProto.Transaction.Res commit(UUID requestID) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(requestID)).setCommitRes(
                    TransactionProto.Transaction.Commit.Res.getDefaultInstance()
            ).build();
        }

        public static TransactionProto.Transaction.Res rollback(UUID requestID) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(requestID)).setRollbackRes(
                    TransactionProto.Transaction.Rollback.Res.getDefaultInstance()
            ).build();
        }
    }

    public static class QueryManager {

        private static TransactionProto.Transaction.Res queryMgrRes(UUID reqID, QueryProto.QueryManager.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setQueryManagerRes(res).build();
        }

        private static TransactionProto.Transaction.ResPart queryMgrResPart(
                UUID reqID, QueryProto.QueryManager.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(UUIDAsByteString(reqID)).setQueryManagerResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res defineRes(UUID reqID) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setDefineRes(
                    QueryProto.QueryManager.Define.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res undefineRes(UUID reqID) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setUndefineRes(
                    QueryProto.QueryManager.Undefine.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart matchResPart(UUID reqID, List<ConceptMap> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setMatchResPart(
                    QueryProto.QueryManager.Match.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMap).toList()
                    )));
        }

        public static TransactionProto.Transaction.Res matchAggregateRes(UUID reqID, Numeric answer) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setMatchAggregateRes(
                    QueryProto.QueryManager.MatchAggregate.Res.newBuilder().setAnswer(numeric(answer))
            ));
        }

        public static TransactionProto.Transaction.ResPart matchGroupResPart(UUID reqID, List<ConceptMapGroup> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setMatchGroupResPart(
                    QueryProto.QueryManager.MatchGroup.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMapGroup).toList())
            ));
        }

        public static TransactionProto.Transaction.ResPart matchGroupAggregateResPart(UUID reqID, List<NumericGroup> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setMatchGroupAggregateResPart(
                    QueryProto.QueryManager.MatchGroupAggregate.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::numericGroup).toList()))
            );
        }

        public static TransactionProto.Transaction.ResPart insertResPart(UUID reqID, List<ConceptMap> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setInsertResPart(
                    QueryProto.QueryManager.Insert.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMap).toList()))
            );
        }

        public static TransactionProto.Transaction.Res deleteRes(UUID reqID) {
            return queryMgrRes(reqID, QueryProto.QueryManager.Res.newBuilder().setDeleteRes(
                    QueryProto.QueryManager.Delete.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart updateResPart(UUID reqID, List<ConceptMap> answers) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setUpdateResPart(
                    QueryProto.QueryManager.Update.ResPart.newBuilder().addAllAnswers(
                            iterate(answers).map(Answer::conceptMap).toList()))
            );
        }

        public static TransactionProto.Transaction.ResPart explainResPart(UUID reqID, List<Explanation> explanations) {
            return queryMgrResPart(reqID, QueryProto.QueryManager.ResPart.newBuilder().setExplainResPart(
                    QueryProto.QueryManager.Explain.ResPart.newBuilder().addAllExplanations(
                            iterate(explanations).map(Logic::explanation).toList()
                    )));
        }

    }

    public static class ConceptManager {

        public static TransactionProto.Transaction.Res conceptMgrRes(UUID reqID, ConceptProto.ConceptManager.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setConceptManagerRes(res).build();
        }

        public static TransactionProto.Transaction.Res putEntityTypeRes(UUID reqID, EntityType entityType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutEntityTypeRes(
                    ConceptProto.ConceptManager.PutEntityType.Res.newBuilder().setEntityType(protoType(entityType))
            ));
        }

        public static TransactionProto.Transaction.Res putRelationTypeRes(UUID reqID, RelationType relationType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutRelationTypeRes(
                    ConceptProto.ConceptManager.PutRelationType.Res.newBuilder().setRelationType(protoType(relationType))
            ));
        }

        public static TransactionProto.Transaction.Res putAttributeTypeRes(UUID reqID, AttributeType attributeType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutAttributeTypeRes(
                    ConceptProto.ConceptManager.PutAttributeType.Res.newBuilder().setAttributeType(protoType(attributeType))
            ));
        }

        public static TransactionProto.Transaction.Res getThingTypeRes(UUID reqID, ThingType thingType) {
            ConceptProto.ConceptManager.GetThingType.Res.Builder getThingTypeRes = ConceptProto.ConceptManager.GetThingType.Res.newBuilder();
            if (thingType != null) getThingTypeRes.setThingType(protoType(thingType));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetThingTypeRes(getThingTypeRes));
        }

        public static TransactionProto.Transaction.Res getThingRes(UUID reqID, @Nullable com.vaticle.typedb.core.concept.thing.Thing thing) {
            ConceptProto.ConceptManager.GetThing.Res.Builder getThingRes = ConceptProto.ConceptManager.GetThing.Res.newBuilder();
            if (thing != null) getThingRes.setThing(protoThing(thing));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetThingRes(getThingRes));
        }
    }

    public static class LogicManager {

        public static TransactionProto.Transaction.Res logicMgrRes(UUID reqID, LogicProto.LogicManager.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setLogicManagerRes(res).build();
        }

        private static TransactionProto.Transaction.ResPart logicMgrResPart(
                UUID reqID, LogicProto.LogicManager.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(UUIDAsByteString(reqID)).setLogicManagerResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res putRuleRes(UUID reqID, com.vaticle.typedb.core.logic.Rule rule) {
            return logicMgrRes(reqID, LogicProto.LogicManager.Res.newBuilder().setPutRuleRes(
                    LogicProto.LogicManager.PutRule.Res.newBuilder().setRule(protoRule(rule))
            ));
        }

        public static TransactionProto.Transaction.Res getRuleRes(UUID reqID, com.vaticle.typedb.core.logic.Rule rule) {
            LogicProto.LogicManager.GetRule.Res.Builder getRuleRes = LogicProto.LogicManager.GetRule.Res.newBuilder();
            if (rule != null) getRuleRes.setRule(protoRule(rule));
            return logicMgrRes(reqID, LogicProto.LogicManager.Res.newBuilder().setGetRuleRes(getRuleRes));
        }

        public static TransactionProto.Transaction.ResPart getRulesResPart(UUID reqID, List<com.vaticle.typedb.core.logic.Rule> rules) {
            return logicMgrResPart(reqID, LogicProto.LogicManager.ResPart.newBuilder().setGetRulesResPart(
                    LogicProto.LogicManager.GetRules.ResPart.newBuilder().addAllRules(
                            rules.stream().map(Logic.Rule::protoRule).collect(toList()))
            ));
        }
    }


    public static class Concept {

        public static ConceptProto.Concept protoConcept(com.vaticle.typedb.core.concept.Concept concept) {
            if (concept == null) return null;
            if (concept.isThing()) {
                return ConceptProto.Concept.newBuilder().setThing(protoThing(concept.asThing())).build();
            } else {
                return ConceptProto.Concept.newBuilder().setType(protoType(concept.asType())).build();
            }
        }

        public static ConceptProto.Thing protoThing(com.vaticle.typedb.core.concept.thing.Thing thing) {
            ConceptProto.Thing.Builder protoThing = ConceptProto.Thing.newBuilder()
                    .setIid(ByteString.copyFrom(thing.getIID().getArray()))
                    .setType(protoType(thing.getType()))
                    .setInferred(thing.isInferred());
            if (thing.isAttribute()) protoThing.setValue(attributeValue(thing.asAttribute()));
            return protoThing.build();
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
                throw TypeDBException.of(ErrorMessage.Server.BAD_VALUE_TYPE);
            }

            return builder.build();
        }

    }

    public static class Type {

        private static ConceptProto.Type.Encoding protoEncoding(com.vaticle.typedb.core.concept.type.Type type) {
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
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        public static ConceptProto.Type protoType(com.vaticle.typedb.core.concept.type.Type type) {
            ConceptProto.Type.Builder protoType = ConceptProto.Type.newBuilder()
                    .setLabel(type.getLabel().name()).setEncoding(protoEncoding(type));
            if (type.isAttributeType()) protoType.setValueType(AttributeType.protoValueType(type.asAttributeType()));
            if (type.isRoleType()) protoType.setScope(type.asRoleType().getLabel().scope().get());
            if (type.isRoot()) protoType.setRoot(true);
            return protoType.build();
        }

        private static TransactionProto.Transaction.Res typeRes(UUID reqID, ConceptProto.Type.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setTypeRes(res).build();
        }

        private static TransactionProto.Transaction.ResPart typeResPart(UUID reqID, ConceptProto.Type.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(UUIDAsByteString(reqID)).setTypeResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res deleteRes(UUID reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeDeleteRes(
                    ConceptProto.Type.Delete.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res setLabelRes(UUID reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeSetLabelRes(
                    ConceptProto.Type.SetLabel.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res isAbstractRes(UUID reqID, boolean isAbstract) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeIsAbstractRes(
                    ConceptProto.Type.IsAbstract.Res.newBuilder().setAbstract(isAbstract)
            ));
        }

        public static TransactionProto.Transaction.Res getSupertypeRes(
                UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.Type supertype) {
            ConceptProto.Type.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.Type.GetSupertype.Res.newBuilder();
            if (supertype != null) getSupertypeRes.setType(protoType(supertype));
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeGetSupertypeRes(getSupertypeRes));
        }

        public static TransactionProto.Transaction.Res setSupertypeRes(UUID reqID) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setTypeSetSupertypeRes(
                    ConceptProto.Type.SetSupertype.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getSupertypesResPart(
                UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.Type> types) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setTypeGetSupertypesResPart(
                    ConceptProto.Type.GetSupertypes.ResPart.newBuilder().addAllTypes(
                            types.stream().map(Type::protoType).collect(toList()))));
        }

        public static TransactionProto.Transaction.ResPart getSubtypesResPart(
                UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.Type> types) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setTypeGetSubtypesResPart(
                    ConceptProto.Type.GetSubtypes.ResPart.newBuilder().addAllTypes(
                            types.stream().map(Type::protoType).collect(toList()))));
        }

        public static class RoleType {

            public static TransactionProto.Transaction.ResPart getRelationTypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RelationType> relationTypes) {
                return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setRoleTypeGetRelationTypesResPart(
                        ConceptProto.RoleType.GetRelationTypes.ResPart.newBuilder().addAllRelationTypes(
                                relationTypes.stream().map(Type::protoType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getPlayersResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.ThingType> players) {
                return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setRoleTypeGetPlayersResPart(
                        ConceptProto.RoleType.GetPlayers.ResPart.newBuilder().addAllThingTypes(
                                players.stream().map(Type::protoType).collect(toList()))));
            }
        }

        public static class ThingType {

            public static TransactionProto.Transaction.ResPart getInstancesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Thing> things) {
                return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setThingTypeGetInstancesResPart(
                        ConceptProto.ThingType.GetInstances.ResPart.newBuilder().addAllThings(
                                things.stream().map(Concept::protoThing).collect(toList()))));
            }

            public static TransactionProto.Transaction.Res setAbstractRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeSetAbstractRes(
                        ConceptProto.ThingType.SetAbstract.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetAbstractRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetAbstractRes(
                        ConceptProto.ThingType.UnsetAbstract.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getOwnsResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.AttributeType> attributeTypes) {
                return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setThingTypeGetOwnsResPart(
                        ConceptProto.ThingType.GetOwns.ResPart.newBuilder().addAllAttributeTypes(
                                attributeTypes.stream().map(Type::protoType).collect(toList()))));
            }

            public static TransactionProto.Transaction.Res setOwnsRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeSetOwnsRes(
                        ConceptProto.ThingType.SetOwns.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetOwnsRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetOwnsRes(
                        ConceptProto.ThingType.UnsetOwns.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getPlaysResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RoleType> roleTypes) {
                return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setThingTypeGetPlaysResPart(
                        ConceptProto.ThingType.GetPlays.ResPart.newBuilder().addAllRoles(
                                roleTypes.stream().map(Type::protoType).collect(toList()))));
            }

            public static TransactionProto.Transaction.Res setPlaysRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeSetPlaysRes(
                        ConceptProto.ThingType.SetPlays.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetPlaysRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetPlaysRes(
                        ConceptProto.ThingType.UnsetPlays.Res.getDefaultInstance()
                ));
            }
        }

        public static class EntityType {

            public static TransactionProto.Transaction.Res createRes(UUID reqID, Entity entity) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setEntityTypeCreateRes(
                        ConceptProto.EntityType.Create.Res.newBuilder().setEntity(protoThing(entity))
                ));
            }
        }

        public static class RelationType {

            public static TransactionProto.Transaction.Res createRes(UUID reqID, Relation relation) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeCreateRes(
                        ConceptProto.RelationType.Create.Res.newBuilder().setRelation(protoThing(relation))
                ));
            }

            public static TransactionProto.Transaction.Res getRelatesForRoleLabelRes(
                    UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.RoleType roleType) {
                ConceptProto.RelationType.GetRelatesForRoleLabel.Res.Builder getRelatesRes =
                        ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder();
                if (roleType != null) getRelatesRes.setRoleType(protoType(roleType));
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeGetRelatesForRoleLabelRes(getRelatesRes));
            }

            public static TransactionProto.Transaction.ResPart getRelatesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RoleType> roleTypes) {
                return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setRelationTypeGetRelatesResPart(
                        ConceptProto.RelationType.GetRelates.ResPart.newBuilder().addAllRoles(
                                roleTypes.stream().map(Type::protoType).collect(toList()))
                ));
            }

            public static TransactionProto.Transaction.Res setRelatesRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeSetRelatesRes(
                        ConceptProto.RelationType.SetRelates.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetRelatesRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRelationTypeUnsetRelatesRes(
                        ConceptProto.RelationType.UnsetRelates.Res.getDefaultInstance()
                ));
            }
        }

        public static class AttributeType {

            public static ConceptProto.AttributeType.ValueType protoValueType(
                    com.vaticle.typedb.core.concept.type.AttributeType attributeType) {
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
                    throw TypeDBException.of(ErrorMessage.Server.BAD_VALUE_TYPE);
                }
            }

            public static TransactionProto.Transaction.Res putRes(UUID reqID, Attribute attribute) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypePutRes(
                        ConceptProto.AttributeType.Put.Res.newBuilder().setAttribute(protoThing(attribute))
                ));
            }

            public static TransactionProto.Transaction.Res getRes(UUID reqID, @Nullable Attribute attribute) {
                ConceptProto.AttributeType.Get.Res.Builder getAttributeTypeRes = ConceptProto.AttributeType.Get.Res.newBuilder();
                if (attribute != null) getAttributeTypeRes.setAttribute(protoThing(attribute));
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRes(getAttributeTypeRes));
            }

            public static TransactionProto.Transaction.Res getRegexRes(UUID reqID, Pattern regex) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRegexRes(
                        ConceptProto.AttributeType.GetRegex.Res.newBuilder().setRegex((regex != null) ? regex.pattern() : "")
                ));
            }

            public static TransactionProto.Transaction.Res setRegexRes(UUID reqID) {
                return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setAttributeTypeSetRegexRes(
                        ConceptProto.AttributeType.SetRegex.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getOwnersResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.ThingType> owners) {
                return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setAttributeTypeGetOwnersResPart(
                        ConceptProto.AttributeType.GetOwners.ResPart.newBuilder().addAllOwners(
                                owners.stream().map(Type::protoType).collect(toList()))
                ));
            }
        }
    }

    public static class Thing {

        public static TransactionProto.Transaction.Res thingRes(UUID reqID, ConceptProto.Thing.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setThingRes(res).build();
        }

        public static TransactionProto.Transaction.ResPart thingResPart(
                UUID reqID, ConceptProto.Thing.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(UUIDAsByteString(reqID)).setThingResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res deleteRes(UUID reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingDeleteRes(
                    ConceptProto.Thing.Delete.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res getTypeRes(UUID reqID, ThingType thingType) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingGetTypeRes(
                    ConceptProto.Thing.GetType.Res.newBuilder().setThingType(protoType(thingType))
            ));
        }

        public static TransactionProto.Transaction.ResPart getHasResPart(
                UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Attribute> attributes) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetHasResPart(
                    ConceptProto.Thing.GetHas.ResPart.newBuilder().addAllAttributes(
                            attributes.stream().map(Concept::protoThing).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.Res setHasRes(UUID reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingSetHasRes(
                    ConceptProto.Thing.SetHas.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.Res unsetHasRes(UUID reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingUnsetHasRes(
                    ConceptProto.Thing.UnsetHas.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getRelationsResPart(
                UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Relation> relations) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetRelationsResPart(
                    ConceptProto.Thing.GetRelations.ResPart.newBuilder().addAllRelations(
                            relations.stream().map(Concept::protoThing).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.ResPart getPlayingResPart(
                UUID reqID, List<? extends RoleType> roleTypes) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetPlayingResPart(
                    ConceptProto.Thing.GetPlaying.ResPart.newBuilder().addAllRoleTypes(
                            roleTypes.stream().map(Type::protoType).collect(toList()))
            ));
        }

        public static class Relation {

            public static TransactionProto.Transaction.Res addPlayerRes(UUID reqID) {
                return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setRelationAddPlayerRes(
                        ConceptProto.Relation.AddPlayer.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res removePlayerRes(UUID reqID) {
                return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setRelationRemovePlayerRes(
                        ConceptProto.Relation.RemovePlayer.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getPlayersResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Thing> players) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetPlayersResPart(
                        ConceptProto.Relation.GetPlayers.ResPart.newBuilder().addAllThings(
                                players.stream().map(Concept::protoThing).collect(toList()))
                ));
            }

            public static TransactionProto.Transaction.ResPart getPlayersByRoleTypeResPart(
                    UUID reqID, List<Pair<RoleType, com.vaticle.typedb.core.concept.thing.Thing>> rolePlayers) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetPlayersByRoleTypeResPart(
                        ConceptProto.Relation.GetPlayersByRoleType.ResPart.newBuilder().addAllRoleTypesWithPlayers(
                                rolePlayers.stream().map(rp -> ConceptProto.Relation.GetPlayersByRoleType.RoleTypeWithPlayer.newBuilder()
                                        .setRoleType(protoType(rp.first())).setPlayer(protoThing(rp.second())).build()).collect(toList()))
                ));
            }

            public static TransactionProto.Transaction.ResPart getRelatingResPart(UUID reqID, List<? extends RoleType> roleTypes) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetRelatingResPart(
                        ConceptProto.Relation.GetRelating.ResPart.newBuilder().addAllRoleTypes(
                                roleTypes.stream().map(Type::protoType).collect(toList()))
                ));
            }
        }

        public static class Attribute {

            public static TransactionProto.Transaction.ResPart getOwnersResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Thing> owners) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setAttributeGetOwnersResPart(
                        ConceptProto.Attribute.GetOwners.ResPart.newBuilder().addAllThings(
                                owners.stream().map(Concept::protoThing).collect(toList()))
                ));
            }
        }
    }

    public static class Logic {

        public static class Rule {

            public static LogicProto.Rule protoRule(com.vaticle.typedb.core.logic.Rule rule) {
                return LogicProto.Rule.newBuilder().setLabel(rule.getLabel())
                        .setWhen(rule.getWhenPreNormalised().toString())
                        .setThen(rule.getThenPreNormalised().toString()).build();
            }

            public static TransactionProto.Transaction.Res ruleRes(UUID reqID, LogicProto.Rule.Res.Builder res) {
                return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setRuleRes(res).build();
            }

            public static TransactionProto.Transaction.Res setLabelRes(UUID reqID) {
                return ruleRes(reqID, LogicProto.Rule.Res.newBuilder().setRuleSetLabelRes(
                        LogicProto.Rule.SetLabel.Res.getDefaultInstance())
                );
            }

            public static TransactionProto.Transaction.Res deleteRes(UUID reqID) {
                return ruleRes(reqID, LogicProto.Rule.Res.newBuilder().setRuleDeleteRes(
                        LogicProto.Rule.Delete.Res.getDefaultInstance())
                );
            }
        }

        public static LogicProto.Explanation explanation(Explanation explanation) {
            LogicProto.Explanation.Builder builder = LogicProto.Explanation.newBuilder();
            builder.setRule(protoRule(explanation.rule()));
            explanation.variableMapping().forEach((from, tos) -> {
                LogicProto.Explanation.VarList.Builder listBuilder = LogicProto.Explanation.VarList.newBuilder();
                tos.forEach(var -> listBuilder.addVars(var.name()));
                builder.putVarMapping(from.name(), listBuilder.build());
            });
            builder.setConclusion(conceptMap(explanation.conclusionAnswer()));
            builder.setCondition(conceptMap(explanation.conditionAnswer()));
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
                ConceptProto.Concept conceptProto = ResponseBuilder.Concept.protoConcept(concept);
                conceptMapProto.putMap(id.name(), conceptProto);
            });
            conceptMapProto.setExplainables(explainables(answer.explainables()));
            return conceptMapProto.build();
        }

        private static AnswerProto.Explainables explainables(ConceptMap.Explainables explainables) {
            AnswerProto.Explainables.Builder builder = AnswerProto.Explainables.newBuilder();
            explainables.relations().forEach(
                    (var, explainable) -> builder.putRelations(var.name(), explainable(explainable))
            );
            explainables.attributes().forEach(
                    (var, explainable) -> builder.putAttributes(var.name(), explainable(explainable))
            );
            Map<String, Map<String, ConceptMap.Explainable>> ownedExtracted = new HashMap<>();
            explainables.ownerships().forEach((ownership, explainable) -> {
                Map<String, ConceptMap.Explainable> owned = ownedExtracted.computeIfAbsent(ownership.first().name(), (val) -> new HashMap<String, ConceptMap.Explainable>());
                owned.put(ownership.second().name(), explainable);
            });
            ownedExtracted.forEach((owner, owned) -> {
                AnswerProto.Explainables.Owned.Builder ownedBuilder = AnswerProto.Explainables.Owned.newBuilder();
                owned.forEach((attribute, explainable) -> ownedBuilder.putOwned(attribute, explainable(explainable)));
                builder.putOwnerships(owner, ownedBuilder.build());
            });

            return builder.build();
        }

        private static AnswerProto.Explainable explainable(ConceptMap.Explainable explainable) {
            AnswerProto.Explainable.Builder builder = AnswerProto.Explainable.newBuilder();
            builder.setConjunction(explainable.conjunction().toString());
            builder.setId(explainable.id());
            return builder.build();
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
