/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.answer.PartialExplanation.ConclusionAnswer;
import com.vaticle.typedb.protocol.AnswerProto;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.ConnectionProto;
import com.vaticle.typedb.protocol.DatabaseProto;
import com.vaticle.typedb.protocol.LogicProto;
import com.vaticle.typedb.protocol.QueryProto;
import com.vaticle.typedb.protocol.ServerProto;
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
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Concept.Existence.INFERRED;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Answer.conceptMap;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Answer.numeric;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Logic.Rule.protoRule;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.protoAttribute;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.protoEntity;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.protoRelation;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.protoThing;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.protoAttributeType;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.protoEntityType;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.protoRelationType;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.protoRoleType;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.protoThingTypeRoot;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.protoType;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Value.protoValue;
import static java.util.stream.Collectors.toList;

public class ResponseBuilder {

    public static StatusRuntimeException exception(Throwable e) {
        if (e instanceof StatusRuntimeException) return (StatusRuntimeException) e;
        else return Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
    }

    public static ByteString UUIDAsByteString(UUID uuid) {
        return copyFrom(encodeUUID(uuid).getBytes());
    }

    public static class ServerManager {
        public static ServerProto.ServerManager.All.Res allRes(String serverAddress) {
            return ServerProto.ServerManager.All.Res.newBuilder()
                    .addServers(ServerProto.Server.newBuilder().setAddress(serverAddress)).build();
        }
    }

    public static class DatabaseManager {

        public static DatabaseProto.DatabaseManager.Contains.Res containsRes(boolean contains) {
            return DatabaseProto.DatabaseManager.Contains.Res.newBuilder().setContains(contains).build();
        }

        public static DatabaseProto.DatabaseManager.Create.Res createRes() {
            return DatabaseProto.DatabaseManager.Create.Res.getDefaultInstance();
        }

        public static DatabaseProto.DatabaseManager.Get.Res getRes(String serverAddress, String name) {
            return DatabaseProto.DatabaseManager.Get.Res.newBuilder().setDatabase(
                DatabaseProto.DatabaseReplicas.newBuilder().setName(name).addReplicas(
                        DatabaseProto.DatabaseReplicas.Replica.newBuilder().setAddress(serverAddress).setPrimary(true).setPreferred(true).setTerm(0).build()
                )
            ).build();
        }

        public static DatabaseProto.DatabaseManager.All.Res allRes(String serverAddress, List<String> names) {
            return DatabaseProto.DatabaseManager.All.Res.newBuilder().addAllDatabases(
                iterate(names).map(name -> DatabaseProto.DatabaseReplicas.newBuilder().setName(name).addReplicas(
                    DatabaseProto.DatabaseReplicas.Replica.newBuilder().setAddress(serverAddress).setPrimary(true).setPreferred(true).setTerm(0).build()
                ).build()).toList()
            ).build();
        }
    }

    public static class Database {

        public static DatabaseProto.Database.Schema.Res schemaRes(String schema) {
            return DatabaseProto.Database.Schema.Res.newBuilder().setSchema(schema).build();
        }

        public static DatabaseProto.Database.TypeSchema.Res typeSchemaRes(String schema) {
            return DatabaseProto.Database.TypeSchema.Res.newBuilder().setSchema(schema).build();
        }

        public static DatabaseProto.Database.RuleSchema.Res ruleSchemaRes(String schema) {
            return DatabaseProto.Database.RuleSchema.Res.newBuilder().setSchema(schema).build();
        }

        public static DatabaseProto.Database.Delete.Res deleteRes() {
            return DatabaseProto.Database.Delete.Res.getDefaultInstance();
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

        public static TransactionProto.Transaction.ResPart matchResPart(UUID reqID, List<? extends ConceptMap> answers) {
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

        public static TransactionProto.Transaction.Res getEntityTypeRes(UUID reqID, EntityType thingType) {
            ConceptProto.ConceptManager.GetEntityType.Res.Builder getEntityTypeRes =
                    ConceptProto.ConceptManager.GetEntityType.Res.newBuilder();
            if (thingType != null) getEntityTypeRes.setEntityType(protoEntityType(thingType));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetEntityTypeRes(getEntityTypeRes));
        }

        public static TransactionProto.Transaction.Res getRelationTypeRes(UUID reqID, RelationType thingType) {
            ConceptProto.ConceptManager.GetRelationType.Res.Builder getRelationTypeRes =
                    ConceptProto.ConceptManager.GetRelationType.Res.newBuilder();
            if (thingType != null) getRelationTypeRes.setRelationType(protoRelationType(thingType));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetRelationTypeRes(getRelationTypeRes));
        }

        public static TransactionProto.Transaction.Res getAttributeTypeRes(UUID reqID, AttributeType thingType) {
            ConceptProto.ConceptManager.GetAttributeType.Res.Builder getAttributeTypeRes =
                    ConceptProto.ConceptManager.GetAttributeType.Res.newBuilder();
            if (thingType != null) getAttributeTypeRes.setAttributeType(protoAttributeType(thingType));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetAttributeTypeRes(getAttributeTypeRes));
        }

        public static TransactionProto.Transaction.Res getEntityRes(
                UUID reqID, @Nullable com.vaticle.typedb.core.concept.thing.Entity thing
        ) {
            ConceptProto.ConceptManager.GetEntity.Res.Builder getEntityRes =
                    ConceptProto.ConceptManager.GetEntity.Res.newBuilder();
            if (thing != null) getEntityRes.setEntity(protoEntity(thing));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetEntityRes(getEntityRes));
        }

        public static TransactionProto.Transaction.Res getRelationRes(
                UUID reqID, @Nullable com.vaticle.typedb.core.concept.thing.Relation thing
        ) {
            ConceptProto.ConceptManager.GetRelation.Res.Builder getRelationRes =
                    ConceptProto.ConceptManager.GetRelation.Res.newBuilder();
            if (thing != null) getRelationRes.setRelation(protoRelation(thing));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetRelationRes(getRelationRes));
        }

        public static TransactionProto.Transaction.Res getAttributeRes(
                UUID reqID, @Nullable com.vaticle.typedb.core.concept.thing.Attribute thing
        ) {
            ConceptProto.ConceptManager.GetAttribute.Res.Builder getAttributeRes =
                    ConceptProto.ConceptManager.GetAttribute.Res.newBuilder();
            if (thing != null) getAttributeRes.setAttribute(protoAttribute(thing));
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetAttributeRes(getAttributeRes));
        }

        public static TransactionProto.Transaction.Res putEntityTypeRes(UUID reqID, EntityType entityType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutEntityTypeRes(
                    ConceptProto.ConceptManager.PutEntityType.Res.newBuilder().setEntityType(protoEntityType(entityType))
            ));
        }

        public static TransactionProto.Transaction.Res putRelationTypeRes(UUID reqID, RelationType relationType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutRelationTypeRes(
                    ConceptProto.ConceptManager.PutRelationType.Res.newBuilder().setRelationType(protoRelationType(relationType))
            ));
        }

        public static TransactionProto.Transaction.Res putAttributeTypeRes(UUID reqID, AttributeType attributeType) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setPutAttributeTypeRes(
                    ConceptProto.ConceptManager.PutAttributeType.Res.newBuilder().setAttributeType(protoAttributeType(attributeType))
            ));
        }

        public static TransactionProto.Transaction.Res getSchemaExceptionsRes(UUID reqID, List<TypeDBException> exceptions) {
            return conceptMgrRes(reqID, ConceptProto.ConceptManager.Res.newBuilder().setGetSchemaExceptionsRes(
                    ConceptProto.ConceptManager.GetSchemaExceptions.Res.newBuilder().addAllExceptions(
                            exceptions.stream().map(
                                    // TODO: We need a new TypeDB Exception API that is consistent,
                                    //       that ensures every exception has a code and a message.
                                    //       For this specific API we know that getSchemaExceptions() always does.
                                    e -> ConceptProto.Exception.newBuilder()
                                            .setCode(e.code().get()).setMessage(e.getMessage()).build()
                            ).collect(toList())
                    ).build()
            ));
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
            if (concept.isEntityType()) {
                return ConceptProto.Concept.newBuilder().setEntityType(protoEntityType(concept.asEntityType())).build();
            } else if (concept.isRelationType()) {
                return ConceptProto.Concept.newBuilder().setRelationType(protoRelationType(concept.asRelationType())).build();
            } else if (concept.isAttributeType()) {
                return ConceptProto.Concept.newBuilder().setAttributeType(protoAttributeType(concept.asAttributeType())).build();
            } else if (concept.isThingType()) {
                return ConceptProto.Concept.newBuilder().setThingTypeRoot(protoThingTypeRoot()).build();
            } else if (concept.isRoleType()) {
                return ConceptProto.Concept.newBuilder().setRoleType(protoRoleType(concept.asRoleType())).build();
            } else if (concept.isEntity()) {
                return ConceptProto.Concept.newBuilder().setEntity(protoEntity(concept.asEntity())).build();
            } else if (concept.isRelation()) {
                return ConceptProto.Concept.newBuilder().setRelation(protoRelation(concept.asRelation())).build();
            } else if (concept.isAttribute()) {
                return ConceptProto.Concept.newBuilder().setAttribute(protoAttribute(concept.asAttribute())).build();
            }
            throw TypeDBException.of(ErrorMessage.Internal.ILLEGAL_STATE);
        }
    }

    public static class Type {

        public static ConceptProto.ThingType protoThingType(com.vaticle.typedb.core.concept.type.ThingType type) {
            var builder = ConceptProto.ThingType.newBuilder();
            if (type.isEntityType()) builder.setEntityType(protoEntityType(type.asEntityType()));
            else if (type.isRelationType()) builder.setRelationType(protoRelationType(type.asRelationType()));
            else if (type.isAttributeType()) builder.setAttributeType(protoAttributeType(type.asAttributeType()));
            return builder.build();
        }

        public static ConceptProto.EntityType protoEntityType(com.vaticle.typedb.core.concept.type.EntityType type) {
            ConceptProto.EntityType.Builder protoEntityType = ConceptProto.EntityType.newBuilder().setLabel(type.getLabel().name());
            if (type.isRoot()) protoEntityType.setIsRoot(true);
            if (type.isAbstract()) protoEntityType.setIsAbstract(true);
            return protoEntityType.build();
        }

        public static ConceptProto.RelationType protoRelationType(com.vaticle.typedb.core.concept.type.RelationType type) {
            ConceptProto.RelationType.Builder protoRelationType = ConceptProto.RelationType.newBuilder().setLabel(type.getLabel().name());
            if (type.isRoot()) protoRelationType.setIsRoot(true);
            if (type.isAbstract()) protoRelationType.setIsAbstract(true);
            return protoRelationType.build();
        }

        public static ConceptProto.AttributeType protoAttributeType(com.vaticle.typedb.core.concept.type.AttributeType type) {
            ConceptProto.AttributeType.Builder protoAttributeType = ConceptProto.AttributeType.newBuilder()
                    .setLabel(type.getLabel().name()).setValueType(AttributeType.protoValueType(type.asAttributeType()));
            if (type.isRoot()) protoAttributeType.setIsRoot(true);
            if (type.isAbstract()) protoAttributeType.setIsAbstract(true);
            return protoAttributeType.build();
        }

        public static ConceptProto.ThingType.Root protoThingTypeRoot() {
            return ConceptProto.ThingType.Root.getDefaultInstance();
        }

        public static ConceptProto.RoleType protoRoleType(com.vaticle.typedb.core.concept.type.RoleType type) {
            assert type.asRoleType().getLabel().scope().isPresent();
            ConceptProto.RoleType.Builder protoRoleType = ConceptProto.RoleType.newBuilder()
                    .setLabel(type.getLabel().name()).setScope(type.asRoleType().getLabel().scope().get());
            if (type.isRoot()) protoRoleType.setIsRoot(true);
            if (type.isAbstract()) protoRoleType.setIsAbstract(true);
            return protoRoleType.build();
        }

        private static TransactionProto.Transaction.Res typeRes(UUID reqID, ConceptProto.Type.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setTypeRes(res).build();
        }

        private static TransactionProto.Transaction.ResPart typeResPart(UUID reqID, ConceptProto.Type.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(UUIDAsByteString(reqID)).setTypeResPart(resPart).build();
        }

        private static TransactionProto.Transaction.Res roleTypeRes(UUID reqID, ConceptProto.RoleType.Res.Builder res) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setRoleTypeRes(res));
        }

        private static TransactionProto.Transaction.ResPart roleTypeResPart(UUID reqID, ConceptProto.RoleType.ResPart.Builder res) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setRoleTypeResPart(res));
        }

        private static TransactionProto.Transaction.Res thingTypeRes(UUID reqID, ConceptProto.ThingType.Res.Builder res) {
            return typeRes(reqID, ConceptProto.Type.Res.newBuilder().setThingTypeRes(res));
        }

        private static TransactionProto.Transaction.ResPart thingTypeResPart(UUID reqID, ConceptProto.ThingType.ResPart.Builder res) {
            return typeResPart(reqID, ConceptProto.Type.ResPart.newBuilder().setThingTypeResPart(res));
        }

        public static class RoleType {
            public static TransactionProto.Transaction.Res deleteRes(UUID reqID) {
                return roleTypeRes(reqID, ConceptProto.RoleType.Res.newBuilder().setRoleTypeDeleteRes(
                        ConceptProto.RoleType.Delete.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res setLabelRes(UUID reqID) {
                return roleTypeRes(reqID, ConceptProto.RoleType.Res.newBuilder().setRoleTypeSetLabelRes(
                        ConceptProto.RoleType.SetLabel.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res getSupertypeRes(
                    UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.RoleType supertype) {
                ConceptProto.RoleType.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.RoleType.GetSupertype.Res.newBuilder();
                if (supertype != null) getSupertypeRes.setRoleType(protoRoleType(supertype));
                return roleTypeRes(reqID, ConceptProto.RoleType.Res.newBuilder().setRoleTypeGetSupertypeRes(getSupertypeRes));
            }

            public static TransactionProto.Transaction.ResPart getSupertypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RoleType> types) {
                return roleTypeResPart(reqID, ConceptProto.RoleType.ResPart.newBuilder().setRoleTypeGetSupertypesResPart(
                        ConceptProto.RoleType.GetSupertypes.ResPart.newBuilder().addAllRoleTypes(
                                types.stream().map(Type::protoRoleType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getSubtypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RoleType> types) {
                return roleTypeResPart(reqID, ConceptProto.RoleType.ResPart.newBuilder().setRoleTypeGetSubtypesResPart(
                        ConceptProto.RoleType.GetSubtypes.ResPart.newBuilder().addAllRoleTypes(
                                types.stream().map(Type::protoRoleType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getRelationTypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RelationType> relationTypes) {
                return roleTypeResPart(reqID, ConceptProto.RoleType.ResPart.newBuilder().setRoleTypeGetRelationTypesResPart(
                        ConceptProto.RoleType.GetRelationTypes.ResPart.newBuilder().addAllRelationTypes(
                                relationTypes.stream().map(Type::protoRelationType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getPlayerTypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.ThingType> players) {
                return roleTypeResPart(reqID, ConceptProto.RoleType.ResPart.newBuilder().setRoleTypeGetPlayerTypesResPart(
                        ConceptProto.RoleType.GetPlayerTypes.ResPart.newBuilder().addAllThingTypes(
                                players.stream().map(Type::protoThingType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getRelationInstancesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Relation> relations) {
                return roleTypeResPart(reqID, ConceptProto.RoleType.ResPart.newBuilder().setRoleTypeGetRelationInstancesResPart(
                        ConceptProto.RoleType.GetRelationInstances.ResPart.newBuilder().addAllRelations(
                                relations.stream().map(Thing::protoRelation).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getPlayerInstancesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Thing> players) {
                return roleTypeResPart(reqID, ConceptProto.RoleType.ResPart.newBuilder().setRoleTypeGetPlayerInstancesResPart(
                        ConceptProto.RoleType.GetPlayerInstances.ResPart.newBuilder().addAllThings(
                                players.stream().map(Thing::protoThing).collect(toList()))));
            }
        }

        public static class ThingType {
            public static TransactionProto.Transaction.Res deleteRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeDeleteRes(
                        ConceptProto.ThingType.Delete.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res setLabelRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeSetLabelRes(
                        ConceptProto.ThingType.SetLabel.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res setAbstractRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeSetAbstractRes(
                        ConceptProto.ThingType.SetAbstract.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetAbstractRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeUnsetAbstractRes(
                        ConceptProto.ThingType.UnsetAbstract.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getOwnsResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.AttributeType> attributeTypes) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setThingTypeGetOwnsResPart(
                        ConceptProto.ThingType.GetOwns.ResPart.newBuilder().addAllAttributeTypes(
                                attributeTypes.stream().map(Type::protoAttributeType).collect(toList()))));
            }

            public static TransactionProto.Transaction.Res getOwnsOverriddenRes(
                    UUID reqID, com.vaticle.typedb.core.concept.type.AttributeType attributeType) {
                ConceptProto.ThingType.GetOwnsOverridden.Res.Builder getOwnsOverridden = ConceptProto.ThingType.GetOwnsOverridden.Res.newBuilder();
                if (attributeType != null) getOwnsOverridden.setAttributeType(protoAttributeType(attributeType));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeGetOwnsOverriddenRes(getOwnsOverridden));
            }

            public static TransactionProto.Transaction.Res setOwnsRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeSetOwnsRes(
                        ConceptProto.ThingType.SetOwns.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetOwnsRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeUnsetOwnsRes(
                        ConceptProto.ThingType.UnsetOwns.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getPlaysResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RoleType> roleTypes) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setThingTypeGetPlaysResPart(
                        ConceptProto.ThingType.GetPlays.ResPart.newBuilder().addAllRoleTypes(
                                roleTypes.stream().map(Type::protoRoleType).collect(toList()))));
            }

            public static TransactionProto.Transaction.Res getPlaysOverriddenRes(
                    UUID reqID, com.vaticle.typedb.core.concept.type.RoleType roleType) {
                ConceptProto.ThingType.GetPlaysOverridden.Res.Builder getPlaysOverridden = ConceptProto.ThingType.GetPlaysOverridden.Res.newBuilder();
                if (roleType != null) getPlaysOverridden.setRoleType(protoRoleType(roleType));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeGetPlaysOverriddenRes(getPlaysOverridden));
            }

            public static TransactionProto.Transaction.Res setPlaysRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeSetPlaysRes(
                        ConceptProto.ThingType.SetPlays.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetPlaysRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeUnsetPlaysRes(
                        ConceptProto.ThingType.UnsetPlays.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res getSyntaxRes(UUID reqID, String syntax) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setThingTypeGetSyntaxRes(
                        ConceptProto.ThingType.GetSyntax.Res.newBuilder().setSyntax(syntax)
                ));
            }
        }


        public static class EntityType {
            public static TransactionProto.Transaction.Res createRes(UUID reqID, Entity entity) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setEntityTypeCreateRes(
                        ConceptProto.EntityType.Create.Res.newBuilder().setEntity(protoEntity(entity))
                ));
            }

            public static TransactionProto.Transaction.Res getSupertypeRes(
                    UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.EntityType supertype) {
                ConceptProto.EntityType.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.EntityType.GetSupertype.Res.newBuilder();
                if (supertype != null) getSupertypeRes.setEntityType(protoEntityType(supertype));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setEntityTypeGetSupertypeRes(getSupertypeRes));
            }

            public static TransactionProto.Transaction.Res setSupertypeRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setEntityTypeSetSupertypeRes(
                        ConceptProto.EntityType.SetSupertype.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getSupertypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.EntityType> types) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setEntityTypeGetSupertypesResPart(
                        ConceptProto.EntityType.GetSupertypes.ResPart.newBuilder().addAllEntityTypes(
                                types.stream().map(Type::protoEntityType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getSubtypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.EntityType> types) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setEntityTypeGetSubtypesResPart(
                        ConceptProto.EntityType.GetSubtypes.ResPart.newBuilder().addAllEntityTypes(
                                types.stream().map(Type::protoEntityType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getInstancesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Entity> things) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setEntityTypeGetInstancesResPart(
                        ConceptProto.EntityType.GetInstances.ResPart.newBuilder().addAllEntities(
                                things.stream().map(Thing::protoEntity).collect(toList()))));
            }
        }

        public static class RelationType {
            public static TransactionProto.Transaction.Res createRes(UUID reqID, Relation relation) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setRelationTypeCreateRes(
                        ConceptProto.RelationType.Create.Res.newBuilder().setRelation(protoRelation(relation))
                ));
            }

            public static TransactionProto.Transaction.Res getSupertypeRes(
                    UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.RelationType supertype) {
                ConceptProto.RelationType.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.RelationType.GetSupertype.Res.newBuilder();
                if (supertype != null) getSupertypeRes.setRelationType(protoRelationType(supertype));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setRelationTypeGetSupertypeRes(getSupertypeRes));
            }

            public static TransactionProto.Transaction.Res setSupertypeRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setRelationTypeSetSupertypeRes(
                        ConceptProto.RelationType.SetSupertype.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getSupertypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RelationType> types) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setRelationTypeGetSupertypesResPart(
                        ConceptProto.RelationType.GetSupertypes.ResPart.newBuilder().addAllRelationTypes(
                                types.stream().map(Type::protoRelationType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getSubtypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RelationType> types) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setRelationTypeGetSubtypesResPart(
                        ConceptProto.RelationType.GetSubtypes.ResPart.newBuilder().addAllRelationTypes(
                                types.stream().map(Type::protoRelationType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getInstancesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Relation> things) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setRelationTypeGetInstancesResPart(
                        ConceptProto.RelationType.GetInstances.ResPart.newBuilder().addAllRelations(
                                things.stream().map(Thing::protoRelation).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getRelatesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.RoleType> roleTypes) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setRelationTypeGetRelatesResPart(
                        ConceptProto.RelationType.GetRelates.ResPart.newBuilder().addAllRoleTypes(
                                roleTypes.stream().map(Type::protoRoleType).collect(toList()))
                ));
            }

            public static TransactionProto.Transaction.Res getRelatesForRoleLabelRes(
                    UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.RoleType roleType) {
                ConceptProto.RelationType.GetRelatesForRoleLabel.Res.Builder res =
                        ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder();
                if (roleType != null) res.setRoleType(protoRoleType(roleType));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setRelationTypeGetRelatesForRoleLabelRes(res));
            }

            public static TransactionProto.Transaction.Res getRelatesOverriddenRes(
                    UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.RoleType roleType) {
                ConceptProto.RelationType.GetRelatesOverridden.Res.Builder res =
                        ConceptProto.RelationType.GetRelatesOverridden.Res.newBuilder();
                if (roleType != null) res.setRoleType(protoRoleType(roleType));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setRelationTypeGetRelatesOverriddenRes(res));
            }

            public static TransactionProto.Transaction.Res setRelatesRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setRelationTypeSetRelatesRes(
                        ConceptProto.RelationType.SetRelates.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res unsetRelatesRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setRelationTypeUnsetRelatesRes(
                        ConceptProto.RelationType.UnsetRelates.Res.getDefaultInstance()
                ));
            }
        }

        public static class AttributeType {

            public static ConceptProto.ValueType protoValueType(
                    com.vaticle.typedb.core.concept.type.AttributeType attributeType) {
                if (attributeType.isString()) {
                    return ConceptProto.ValueType.STRING;
                } else if (attributeType.isBoolean()) {
                    return ConceptProto.ValueType.BOOLEAN;
                } else if (attributeType.isLong()) {
                    return ConceptProto.ValueType.LONG;
                } else if (attributeType.isDouble()) {
                    return ConceptProto.ValueType.DOUBLE;
                } else if (attributeType.isDateTime()) {
                    return ConceptProto.ValueType.DATETIME;
                } else if (attributeType.isRoot()) {
                    return ConceptProto.ValueType.OBJECT;
                } else {
                    throw TypeDBException.of(ErrorMessage.Server.BAD_VALUE_TYPE);
                }
            }

            public static TransactionProto.Transaction.Res getSupertypeRes(
                    UUID reqID, @Nullable com.vaticle.typedb.core.concept.type.AttributeType supertype) {
                ConceptProto.AttributeType.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.AttributeType.GetSupertype.Res.newBuilder();
                if (supertype != null) getSupertypeRes.setAttributeType(protoAttributeType(supertype));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setAttributeTypeGetSupertypeRes(getSupertypeRes));
            }

            public static TransactionProto.Transaction.Res setSupertypeRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setAttributeTypeSetSupertypeRes(
                        ConceptProto.AttributeType.SetSupertype.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getSupertypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.AttributeType> types) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setAttributeTypeGetSupertypesResPart(
                        ConceptProto.AttributeType.GetSupertypes.ResPart.newBuilder().addAllAttributeTypes(
                                types.stream().map(Type::protoAttributeType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getSubtypesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.AttributeType> types) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setAttributeTypeGetSubtypesResPart(
                        ConceptProto.AttributeType.GetSubtypes.ResPart.newBuilder().addAllAttributeTypes(
                                types.stream().map(Type::protoAttributeType).collect(toList()))));
            }

            public static TransactionProto.Transaction.ResPart getInstancesResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Attribute> things) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setAttributeTypeGetInstancesResPart(
                        ConceptProto.AttributeType.GetInstances.ResPart.newBuilder().addAllAttributes(
                                things.stream().map(Thing::protoAttribute).collect(toList()))));
            }

            public static TransactionProto.Transaction.Res putRes(UUID reqID, Attribute attribute) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setAttributeTypePutRes(
                        ConceptProto.AttributeType.Put.Res.newBuilder().setAttribute(protoAttribute(attribute))
                ));
            }

            public static TransactionProto.Transaction.Res getRes(UUID reqID, @Nullable Attribute attribute) {
                ConceptProto.AttributeType.Get.Res.Builder getAttributeTypeRes = ConceptProto.AttributeType.Get.Res.newBuilder();
                if (attribute != null) getAttributeTypeRes.setAttribute(protoAttribute(attribute));
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setAttributeTypeGetRes(getAttributeTypeRes));
            }

            public static TransactionProto.Transaction.Res getRegexRes(UUID reqID, Pattern regex) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setAttributeTypeGetRegexRes(
                        ConceptProto.AttributeType.GetRegex.Res.newBuilder().setRegex((regex != null) ? regex.pattern() : "")
                ));
            }

            public static TransactionProto.Transaction.Res setRegexRes(UUID reqID) {
                return thingTypeRes(reqID, ConceptProto.ThingType.Res.newBuilder().setAttributeTypeSetRegexRes(
                        ConceptProto.AttributeType.SetRegex.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getOwnersResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.type.ThingType> owners) {
                return thingTypeResPart(reqID, ConceptProto.ThingType.ResPart.newBuilder().setAttributeTypeGetOwnersResPart(
                        ConceptProto.AttributeType.GetOwners.ResPart.newBuilder().addAllThingTypes(
                                owners.stream().map(Type::protoThingType).collect(toList())
                        )));
            }
        }
    }

    public static class Thing {

        public static ConceptProto.Thing protoThing(com.vaticle.typedb.core.concept.thing.Thing thing) {
            if (thing.isEntity()) return ConceptProto.Thing.newBuilder().setEntity(protoEntity(thing.asEntity())).build();
            if (thing.isRelation()) return ConceptProto.Thing.newBuilder().setRelation(protoRelation(thing.asRelation())).build();
            if (thing.isAttribute()) return ConceptProto.Thing.newBuilder().setAttribute(protoAttribute(thing.asAttribute())).build();
            throw TypeDBException.of(ErrorMessage.Internal.ILLEGAL_STATE);
        }

        public static ConceptProto.Entity protoEntity(com.vaticle.typedb.core.concept.thing.Entity thing) {
            ConceptProto.Entity.Builder protoEntity = ConceptProto.Entity.newBuilder()
                    .setIid(ByteString.copyFrom(thing.getIID().getBytes()))
                    .setEntityType(protoEntityType(thing.getType()))
                    .setInferred(thing.existence() == INFERRED);
            return protoEntity.build();
        }

        public static ConceptProto.Relation protoRelation(com.vaticle.typedb.core.concept.thing.Relation thing) {
            ConceptProto.Relation.Builder protoRelation = ConceptProto.Relation.newBuilder()
                    .setIid(ByteString.copyFrom(thing.getIID().getBytes()))
                    .setRelationType(protoRelationType(thing.getType()))
                    .setInferred(thing.existence() == INFERRED);
            return protoRelation.build();
        }

        public static ConceptProto.Attribute protoAttribute(com.vaticle.typedb.core.concept.thing.Attribute thing) {
            ConceptProto.Attribute.Builder protoAttribute = ConceptProto.Attribute.newBuilder()
                    .setIid(ByteString.copyFrom(thing.getIID().getBytes()))
                    .setAttributeType(protoAttributeType(thing.getType()))
                    .setInferred(thing.existence() == INFERRED)
                    .setValue(Attribute.attributeValue(thing.asAttribute()));
            return protoAttribute.build();
        }

        public static TransactionProto.Transaction.Res thingRes(UUID reqID, ConceptProto.Thing.Res.Builder res) {
            return TransactionProto.Transaction.Res.newBuilder().setReqId(UUIDAsByteString(reqID)).setThingRes(res).build();
        }

        public static TransactionProto.Transaction.ResPart thingResPart(UUID reqID, ConceptProto.Thing.ResPart.Builder resPart) {
            return TransactionProto.Transaction.ResPart.newBuilder().setReqId(UUIDAsByteString(reqID)).setThingResPart(resPart).build();
        }

        public static TransactionProto.Transaction.Res deleteRes(UUID reqID) {
            return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setThingDeleteRes(
                    ConceptProto.Thing.Delete.Res.getDefaultInstance()
            ));
        }

        public static TransactionProto.Transaction.ResPart getHasResPart(
                UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Attribute> attributes) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetHasResPart(
                    ConceptProto.Thing.GetHas.ResPart.newBuilder().addAllAttributes(
                            attributes.stream().map(Thing::protoAttribute).collect(toList()))
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
                            relations.stream().map(Thing::protoRelation).collect(toList()))
            ));
        }

        public static TransactionProto.Transaction.ResPart getPlayingResPart(
                UUID reqID, List<? extends RoleType> roleTypes) {
            return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setThingGetPlayingResPart(
                    ConceptProto.Thing.GetPlaying.ResPart.newBuilder().addAllRoleTypes(
                            roleTypes.stream().map(Type::protoRoleType).collect(toList()))
            ));
        }

        public static class Relation {

            public static TransactionProto.Transaction.Res addRolePlayerRes(UUID reqID) {
                return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setRelationAddRolePlayerRes(
                        ConceptProto.Relation.AddRolePlayer.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.Res removeRolePlayerRes(UUID reqID) {
                return thingRes(reqID, ConceptProto.Thing.Res.newBuilder().setRelationRemoveRolePlayerRes(
                        ConceptProto.Relation.RemoveRolePlayer.Res.getDefaultInstance()
                ));
            }

            public static TransactionProto.Transaction.ResPart getPlayersByRoleTypeResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Thing> players) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetPlayersByRoleTypeResPart(
                        ConceptProto.Relation.GetPlayersByRoleType.ResPart.newBuilder().addAllThings(
                                players.stream().map(Thing::protoThing).collect(toList()))
                ));
            }

            public static TransactionProto.Transaction.ResPart getRolePlayersResPart(
                    UUID reqID, List<Pair<RoleType, com.vaticle.typedb.core.concept.thing.Thing>> rolePlayers) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetRolePlayersResPart(
                        ConceptProto.Relation.GetRolePlayers.ResPart.newBuilder().addAllRolePlayers(
                                rolePlayers.stream().map(rp -> ConceptProto.Relation.RolePlayer.newBuilder()
                                        .setRoleType(protoRoleType(rp.first())).setPlayer(protoThing(rp.second())).build()).collect(toList()))
                ));
            }

            public static TransactionProto.Transaction.ResPart getRelatingResPart(UUID reqID, List<? extends RoleType> roleTypes) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setRelationGetRelatingResPart(
                        ConceptProto.Relation.GetRelating.ResPart.newBuilder().addAllRoleTypes(
                                roleTypes.stream().map(Type::protoRoleType).collect(toList()))
                ));
            }
        }

        public static class Attribute {

            public static ConceptProto.ConceptValue attributeValue(com.vaticle.typedb.core.concept.thing.Attribute attribute) {
                ConceptProto.ConceptValue.Builder builder = ConceptProto.ConceptValue.newBuilder();
                // attributes don't need to set the value type
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

            public static TransactionProto.Transaction.ResPart getOwnersResPart(
                    UUID reqID, List<? extends com.vaticle.typedb.core.concept.thing.Thing> owners) {
                return thingResPart(reqID, ConceptProto.Thing.ResPart.newBuilder().setAttributeGetOwnersResPart(
                        ConceptProto.Attribute.GetOwners.ResPart.newBuilder().addAllThings(
                                owners.stream().map(Thing::protoThing).collect(toList()))
                ));
            }
        }
    }

    public static class Value {

        public static ConceptProto.Value protoValue(com.vaticle.typedb.core.concept.value.Value<?> value) {
            ConceptProto.Value.Builder protoValue = ConceptProto.Value.newBuilder()
                    .setValueType(valueType(value))
                    .setValue(value(value));
            return protoValue.build();
        }

        public static ConceptProto.ValueType valueType(com.vaticle.typedb.core.concept.value.Value<?> value) {
            if (value.isString()) return ConceptProto.ValueType.STRING;
            else if (value.isBoolean()) return ConceptProto.ValueType.BOOLEAN;
            else if (value.isLong()) return ConceptProto.ValueType.LONG;
            else if (value.isDouble()) return ConceptProto.ValueType.DOUBLE;
            else if (value.isDateTime()) return ConceptProto.ValueType.DATETIME;
            else throw TypeDBException.of(ErrorMessage.Server.BAD_VALUE_TYPE);
        }

        public static ConceptProto.ConceptValue value(com.vaticle.typedb.core.concept.value.Value<?> value) {
            ConceptProto.ConceptValue.Builder builder = ConceptProto.ConceptValue.newBuilder();
            if (value.isString()) builder.setString(value.asString().value());
            else if (value.isLong()) builder.setLong(value.asLong().value());
            else if (value.isBoolean()) builder.setBoolean(value.asBoolean().value());
            else if (value.isDateTime()) {
                builder.setDateTime(value.asDateTime().value().toInstant(ZoneOffset.UTC).toEpochMilli());
            } else if (value.isDouble()) builder.setDouble(value.asDouble().value());
            else throw TypeDBException.of(ErrorMessage.Server.BAD_VALUE_TYPE);
            return builder.build();
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
                tos.forEach(var -> listBuilder.addVars(var.reference().name()));
                builder.putVarMapping(from.name(), listBuilder.build());
            });
            builder.setCondition(conceptMap(explanation.conditionAnswer()));
            builder.setConclusion(conceptMap(explanation.conclusionAnswer()));
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

        public static AnswerProto.ConceptMap conceptMap(ConclusionAnswer answer) {
            AnswerProto.ConceptMap.Builder conceptMapProto = AnswerProto.ConceptMap.newBuilder();
            answer.concepts().forEach((id, concept) -> {
                ConceptProto.Concept conceptProto = ResponseBuilder.Concept.protoConcept(concept);
                conceptMapProto.putMap(id.reference().name(), conceptProto);
            });
            conceptMapProto.setExplainables(AnswerProto.Explainables.getDefaultInstance());
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
                Map<String, ConceptMap.Explainable> owned = ownedExtracted.computeIfAbsent(ownership.first().name(), (val) -> new HashMap<>());
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
