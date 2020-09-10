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

package grakn.core.server.rpc;

import com.google.protobuf.ByteString;
import grakn.core.Grakn;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
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
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on Concepts that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message grakn.protocol.session.ConceptProto.Method.Req.
 */
class ConceptRPC {

    private static final Logger LOG = LoggerFactory.getLogger(ConceptRPC.class);

    static void run(ConceptHolder con, ConceptProto.ThingMethod.Req req) {
        switch (req.getReqCase()) {
            // Concept methods
            case THING_DELETE_REQ:
                con.delete();
                return;

            // Thing methods
            case THING_GETTYPE_REQ:
                con.asThing().getType();
                return;
            case THING_ISINFERRED_REQ:
                con.asThing().isInferred();
                return;
            case THING_SETHAS_REQ:
                con.asThing().setHas(req.getThingSetHasReq().getAttribute());
                return;
            case THING_UNSETHAS_REQ:
                con.asThing().unsetHas(req.getThingUnsetHasReq().getAttribute());
                return;

            // Relation methods
            case RELATION_ADDPLAYER_REQ:
                con.asRelation().addPlayer(req.getRelationAddPlayerReq());
                return;
            case RELATION_REMOVEPLAYER_REQ:
                con.asRelation().removePlayer(req.getRelationRemovePlayerReq());
                return;

            case REQ_NOT_SET:
            default:
                throw new GraknException(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    static void run(ConceptHolder con, ConceptProto.TypeMethod.Req req) {
        switch (req.getReqCase()) {
            // Concept methods
            case TYPE_DELETE_REQ:
                con.delete();
                return;

            case TYPE_SETLABEL_REQ:
                con.asType().setLabel(req.getTypeSetLabelReq().getLabel());
                return;
            case TYPE_ISABSTRACT_REQ:
                con.asType().isAbstract();
                return;
            case TYPE_GETSUPERTYPE_REQ:
                con.asType().getSupertype();
                return;
            case TYPE_SETSUPERTYPE_REQ:
                con.asType().setSupertype(req.getTypeSetSupertypeReq().getType());
                return;

//            // Rule methods
//            case RULE_WHEN_REQ:
//                con.asRule().when();
//                return;
//            case RULE_THEN_REQ:
//                con.asRule().then();
//                return;

            // RoleType methods
            case ROLETYPE_GETRELATION_REQ:
                con.asRoleType().getRelation();
                return;

            // ThingType methods
            case THINGTYPE_SETABSTRACT_REQ:
                con.asThingType().setAbstract();
                return;
            case THINGTYPE_UNSETABSTRACT_REQ:
                con.asThingType().unsetAbstract();
                return;
            case THINGTYPE_SETOWNS_REQ:
                con.asThingType().setOwns(req.getThingTypeSetOwnsReq());
                return;
            case THINGTYPE_SETPLAYS_REQ:
                con.asThingType().setPlays(req.getThingTypeSetPlaysReq());
                return;
            case THINGTYPE_UNSETOWNS_REQ:
                con.asThingType().unsetOwns(req.getThingTypeUnsetOwnsReq().getAttributeType());
                return;
            case THINGTYPE_UNSETPLAYS_REQ:
                con.asThingType().unsetPlays(req.getThingTypeUnsetPlaysReq().getRole());
                return;

            // EntityType methods
            case ENTITYTYPE_CREATE_REQ:
                con.asEntityType().create();
                return;

            // RelationType methods
            case RELATIONTYPE_CREATE_REQ:
                con.asRelationType().create();
                return;
            case RELATIONTYPE_GETRELATESFORROLELABEL_REQ:
                con.asRelationType().getRelates(req.getRelationTypeGetRelatesForRoleLabelReq().getLabel());
                return;
            case RELATIONTYPE_SETRELATES_REQ:
                con.asRelationType().setRelates(req.getRelationTypeSetRelatesReq());
                return;
            case RELATIONTYPE_UNSETRELATES_REQ:
                con.asRelationType().unsetRelates(req.getRelationTypeUnsetRelatesReq());
                return;

            // AttributeType methods
            case ATTRIBUTETYPE_PUT_REQ:
                con.asAttributeType().put(req.getAttributeTypePutReq().getValue());
                return;
            case ATTRIBUTETYPE_GET_REQ:
                con.asAttributeType().get(req.getAttributeTypeGetReq().getValue());
                return;
            case ATTRIBUTETYPE_GETREGEX_REQ:
                con.asAttributeType().getRegex();
                return;
            case ATTRIBUTETYPE_SETREGEX_REQ:
                con.asAttributeType().setRegex(req.getAttributeTypeSetRegexReq().getRegex());
                return;

            default:
            case REQ_NOT_SET:
                throw new GraknException(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    static void iter(ConceptHolder con, ConceptProto.ThingMethod.Iter.Req req) {
        switch (req.getReqCase()) {
            // Thing methods
            case THING_GETHAS_ITER_REQ:
                con.asThing().getHas(req.getThingGetHasIterReq());
                return;
            case THING_GETRELATIONS_ITER_REQ:
                con.asThing().getRelations(req.getThingGetRelationsIterReq().getRoleTypesList());
                return;
            case THING_GETPLAYS_ITER_REQ:
                con.asThing().getPlays();
                return;

            // Relation methods
            case RELATION_GETPLAYERS_ITER_REQ:
                con.asRelation().getPlayers(req.getRelationGetPlayersIterReq().getRoleTypesList());
                return;
            case RELATION_GETPLAYERSBYROLETYPE_ITER_REQ:
                con.asRelation().getPlayersByRoleType();
                return;

            // Attribute methods
            case ATTRIBUTE_GETOWNERS_ITER_REQ:
                con.asAttribute().getOwners(req.getAttributeGetOwnersIterReq());
                return;

            default:
            case REQ_NOT_SET:
                throw new GraknException(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    static void iter(ConceptHolder con, ConceptProto.TypeMethod.Iter.Req req) {
        switch (req.getReqCase()) {
            // Type iterator methods
            case TYPE_GETSUPERTYPES_ITER_REQ:
                con.asType().getSupertypes();
                return;
            case TYPE_GETSUBTYPES_ITER_REQ:
                con.asType().getSubtypes();
                return;

            // Role iterator methods
            case ROLETYPE_GETRELATIONS_ITER_REQ:
                con.asRoleType().getRelations();
                return;
            case ROLETYPE_GETPLAYERS_ITER_REQ:
                con.asRoleType().getPlayers();
                return;

            // ThingType iterator methods
            case THINGTYPE_GETINSTANCES_ITER_REQ:
                con.asThingType().getInstances();
                return;
            case THINGTYPE_GETOWNS_ITER_REQ:
                con.asThingType().getOwns(req.getThingTypeGetOwnsIterReq().getKeysOnly());
                return;
            case THINGTYPE_GETPLAYS_ITER_REQ:
                con.asThingType().getPlays();
                return;

            // RelationType iterator methods
            case RELATIONTYPE_GETRELATES_ITER_REQ:
                con.asRelationType().getRelates();
                return;

            // AttributeType iterator methods
            case ATTRIBUTETYPE_GETOWNERS_ITER_REQ:
                con.asAttributeType().getOwners(req.getAttributeTypeGetOwnersIterReq().getOnlyKey());
                return;

            default:
            case REQ_NOT_SET:
                throw new GraknException(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    /**
     * A utility class to hold on to the concept in the method request will be applied to
     */
    static class ConceptHolder {

        private final Concept concept;
        private final grakn.core.Grakn.Transaction transaction;
        private final TransactionRPC.Iterators iterators;
        private final Consumer<TransactionProto.Transaction.Res> responseSender;

        ConceptHolder(Grakn.Transaction transaction, ByteString iid,
                      TransactionRPC.Iterators iterators, Consumer<TransactionProto.Transaction.Res> responseSender) {
            this.concept = conceptExists(transaction.concepts().getThing(iid.toByteArray()));
            this.transaction = transaction;
            this.iterators = iterators;
            this.responseSender = responseSender;
        }

        ConceptHolder(Grakn.Transaction transaction, String label, @Nullable String scope,
                      TransactionRPC.Iterators iterators, Consumer<TransactionProto.Transaction.Res> responseSender) {
            this.transaction = transaction;
            this.iterators = iterators;
            this.responseSender = responseSender;
            this.concept = scope != null && !scope.isEmpty() ?
                    conceptExists(transaction.concepts().getRelationType(scope)).getRelates(label) :
                    conceptExists(transaction.concepts().getType(label));
        }

        private static TransactionProto.Transaction.Res transactionRes(ConceptProto.ThingMethod.Res response) {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setConceptMethodThingRes(TransactionProto.Transaction.ConceptMethod.Thing.Res.newBuilder()
                                                      .setResponse(response)).build();
        }

        private static TransactionProto.Transaction.Res transactionRes(ConceptProto.TypeMethod.Res response) {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setConceptMethodTypeRes(TransactionProto.Transaction.ConceptMethod.Type.Res.newBuilder()
                                                     .setResponse(response)).build();
        }

        private Thing convertThing(ConceptProto.Thing protoThing) {
            return transaction.concepts().getThing(protoThing.getIid().toByteArray());
        }

        private Type convertType(ConceptProto.Type protoType) {
            return transaction.concepts().getType(protoType.getLabel());
        }

        private RoleType convertRoleType(ConceptProto.Type protoRole) {
            final Type type = transaction.concepts().getRelationType(protoRole.getScope()).getRelates(protoRole.getLabel());
            return type != null ? type.asRoleType() : null;
        }

        TypeHolder asType() {
            return new TypeHolder();
        }
//
//        Rule asRule() {
//            return new Rule();
//        }

        RoleTypeHolder asRoleType() {
            return new RoleTypeHolder();
        }

        ThingTypeHolder asThingType() {
            return new ThingTypeHolder();
        }

        EntityTypeHolder asEntityType() {
            return new EntityTypeHolder();
        }

        RelationTypeHolder asRelationType() {
            return new RelationTypeHolder();
        }

        AttributeTypeHolder asAttributeType() {
            return new AttributeTypeHolder();
        }

        ThingHolder asThing() {
            return new ThingHolder();
        }

        RelationHolder asRelation() {
            return new RelationHolder();
        }

        AttributeHolder asAttribute() {
            return new AttributeHolder();
        }

        private void delete() {
            concept.delete();
            responseSender.accept(null);
        }

        /**
         * A utility class to execute methods on grakn.core.concept.type.Type
         */
        private class TypeHolder {
            private final Type type = ConceptHolder.this.concept.asType();

            private void setLabel(final String label) {
                type.setLabel(label);
                responseSender.accept(null);
            }

            private void isAbstract() {
                final boolean isAbstract = type.isAbstract();

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setTypeIsAbstractRes(ConceptProto.Type.IsAbstract.Res.newBuilder()
                                                      .setAbstract(isAbstract)).build();

                responseSender.accept(transactionRes(response));
            }

            private void getSupertype() {
                final Type supertype = type.getSupertype();

                final ConceptProto.Type.GetSupertype.Res.Builder responseConcept = ConceptProto.Type.GetSupertype.Res.newBuilder();
                if (supertype != null) responseConcept.setType(ResponseBuilder.Concept.type(supertype));

                ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setTypeGetSupertypeRes(responseConcept).build();

                responseSender.accept(transactionRes(response));
            }

            private void setSupertype(final ConceptProto.Type supertype) {
                // Make the second argument the super of the first argument

                final Type sup = convertType(supertype);

                if (type instanceof EntityType) {
                    type.asEntityType().setSupertype(sup.asEntityType());
                } else if (type instanceof RelationType) {
                    type.asRelationType().setSupertype(sup.asRelationType());
                } else if (type instanceof AttributeType) {
                    type.asAttributeType().setSupertype(sup.asAttributeType());
                }

                responseSender.accept(null);
            }

            private void getSupertypes() {
                final Stream<? extends Type> supertypes = type.getSupertypes();

                final Stream<TransactionProto.Transaction.Res> responses = supertypes.map(con -> {
                    final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setTypeGetSupertypesIterRes(ConceptProto.Type.GetSupertypes.Iter.Res.newBuilder()
                                                                 .setType(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void getSubtypes() {
                final Stream<? extends Type> subtypes = type.getSubtypes();

                final Stream<TransactionProto.Transaction.Res> responses = subtypes.map(con -> {
                    final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setTypeGetSubtypesIterRes(ConceptProto.Type.GetSubtypes.Iter.Res.newBuilder()
                                                               .setType(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }
        }

        //
//        /**
//         * A utility class to execute methods on grakn.core.kb.concept.api.Rule
//         */
//        private class Rule {
//
//            private void when() {
//                Pattern pattern = concept.asRule().when();
//                ConceptProto.Rule.When.Res.Builder whenRes = ConceptProto.Rule.When.Res.newBuilder();
//
//                if (pattern == null) whenRes.setNull(ConceptProto.Null.getDefaultInstance());
//                else whenRes.setPattern(pattern.toString());
//
//                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
//                        .setRuleWhenRes(whenRes).build();
//
//                responseSender.accept(transactionRes(response));
//            }
//
//            private void then() {
//                Pattern pattern = concept.asRule().then();
//                ConceptProto.Rule.Then.Res.Builder thenRes = ConceptProto.Rule.Then.Res.newBuilder();
//
//                if (pattern == null) thenRes.setNull(ConceptProto.Null.getDefaultInstance());
//                else thenRes.setPattern(pattern.toString());
//
//                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
//                        .setRuleThenRes(thenRes).build();
//
//                responseSender.accept(transactionRes(response));
//            }
//        }
//
//        /**
//         * A utility class to execute methods on grakn.core.concept.type.RoleType
//         */
        private class RoleTypeHolder {
            private final RoleType roleType = ConceptHolder.this.concept.asType().asRoleType();

            private void getRelation() {
                final RelationType relationType = roleType.getRelation();

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setRoleTypeGetRelationRes(ConceptProto.RoleType.GetRelation.Res.newBuilder()
                                                           .setRelationType(ResponseBuilder.Concept.type(relationType))).build();

                responseSender.accept(transactionRes(response));
            }

            private void getRelations() {
                Stream<? extends RelationType> relationTypes = roleType.getRelations();

                Stream<TransactionProto.Transaction.Res> responses = relationTypes.map(con -> {
                    ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setRoleTypeGetRelationsIterRes(ConceptProto.RoleType.GetRelations.Iter.Res.newBuilder()
                                                                    .setRelationType(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void getPlayers() {
                Stream<? extends ThingType> players = roleType.getPlayers();

                Stream<TransactionProto.Transaction.Res> responses = players.map(con -> {
                    ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setRoleTypeGetPlayersIterRes(ConceptProto.RoleType.GetPlayers.Iter.Res.newBuilder()
                                                                  .setThingType(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.type.Type
         */
        private class ThingTypeHolder {
            private final ThingType thingType = ConceptHolder.this.concept.asType().asThingType();

            private void getInstances() {
                LOG.trace("{} instances", Thread.currentThread());
                Stream<? extends grakn.core.concept.thing.Thing> concepts = thingType.getInstances();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setThingTypeGetInstancesIterRes(ConceptProto.ThingType.GetInstances.Iter.Res.newBuilder()
                                                                     .setThing(ResponseBuilder.Concept.thing(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
                LOG.trace("{} end instances", Thread.currentThread());
            }

            private void setAbstract() {
                thingType.setAbstract();
                responseSender.accept(null);
            }

            private void unsetAbstract() {
                thingType.unsetAbstract();
                responseSender.accept(null);
            }

            private void getOwns(final boolean keysOnly) {
                final Stream<? extends AttributeType> ownedTypes = thingType.getOwns(keysOnly);

                final Stream<TransactionProto.Transaction.Res> responses = ownedTypes.map(con -> {
                    final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setThingTypeGetOwnsIterRes(ConceptProto.ThingType.GetOwns.Iter.Res.newBuilder()
                                                                .setAttributeType(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void getPlays() {
                final Stream<? extends RoleType> roleTypes = thingType.getPlays();

                final Stream<TransactionProto.Transaction.Res> responses = roleTypes.map(con -> {
                    final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setThingTypeGetPlaysIterRes(ConceptProto.ThingType.GetPlays.Iter.Res.newBuilder()
                                                                 .setRole(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void setOwns(final ConceptProto.ThingType.SetOwns.Req req) {
                final AttributeType attributeType = convertType(req.getAttributeType()).asAttributeType();
                final boolean isKey = req.getIsKey();

                if (req.hasOverriddenType()) {
                    final AttributeType overriddenType = convertType(req.getOverriddenType()).asAttributeType();
                    thingType.setOwns(attributeType, overriddenType, isKey);
                } else {
                    thingType.setOwns(attributeType, isKey);
                }
                responseSender.accept(null);
            }

            private void setPlays(final ConceptProto.ThingType.SetPlays.Req request) {
                final RoleType role = convertRoleType(request.getRole());
                if (request.hasOverriddenRole()) {
                    final RoleType overriddenRole = convertRoleType(request.getOverriddenRole());
                    thingType.setPlays(role, overriddenRole);
                } else {
                    thingType.setPlays(role);
                }
                responseSender.accept(null);
            }

            private void unsetOwns(final ConceptProto.Type protoAttributeType) {
                final AttributeType attributeType = convertType(protoAttributeType).asAttributeType();
                thingType.unsetOwns(attributeType);
                responseSender.accept(null);
            }

            private void unsetPlays(final ConceptProto.Type protoRoleType) {
                final RoleType role = convertRoleType(protoRoleType);
                thingType.unsetPlays(role);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.EntityType
         */
        private class EntityTypeHolder {
            private final EntityType entityType = concept.asType().asEntityType();

            private void create() {
                final Entity entity = entityType.create();

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setEntityTypeCreateRes(ConceptProto.EntityType.Create.Res.newBuilder()
                                                        .setEntity(ResponseBuilder.Concept.thing(entity))).build();

                responseSender.accept(transactionRes(response));
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.type.RelationType
         */
        private class RelationTypeHolder {
            private final RelationType relationType = ConceptHolder.this.concept.asType().asRelationType();

            private void create() {
                final Relation relation = relationType.asRelationType().create();

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setRelationTypeCreateRes(ConceptProto.RelationType.Create.Res.newBuilder()
                                                          .setRelation(ResponseBuilder.Concept.thing(relation))).build();

                responseSender.accept(transactionRes(response));
            }

            private void getRelates() {
                final Stream<? extends RoleType> roleTypes = relationType.getRelates();

                final Stream<TransactionProto.Transaction.Res> responses = roleTypes.map(con -> {
                    final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setRelationTypeGetRelatesIterRes(ConceptProto.RelationType.GetRelates.Iter.Res.newBuilder()
                                                                      .setRole(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void getRelates(final String label) {
                final RoleType roleType = relationType.getRelates(label);

                final ConceptProto.RelationType.GetRelatesForRoleLabel.Res.Builder builder = ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder();
                if (roleType != null) {
                    builder.setRoleType(ResponseBuilder.Concept.type(roleType));
                }

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setRelationTypeGetRelatesForRoleLabelRes(builder).build();
                responseSender.accept(transactionRes(response));
            }

            private void setRelates(final ConceptProto.RelationType.SetRelates.Req request) {
                if (request.getOverriddenCase() == ConceptProto.RelationType.SetRelates.Req.OverriddenCase.OVERRIDDENLABEL) {
                    relationType.setRelates(request.getLabel(), request.getOverriddenLabel());
                } else {
                    relationType.setRelates(request.getLabel());
                }

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setRelationTypeSetRelatesRes(ConceptProto.RelationType.SetRelates.Res.getDefaultInstance()).build();

                responseSender.accept(transactionRes(response));
            }

            private void unsetRelates(final ConceptProto.RelationType.UnsetRelates.Req request) {
                relationType.unsetRelates(request.getLabel());

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setRelationTypeUnsetRelatesRes(ConceptProto.RelationType.UnsetRelates.Res.getDefaultInstance()).build();

                responseSender.accept(transactionRes(response));
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.type.AttributeType
         */
        private class AttributeTypeHolder {
            private final AttributeType attributeType = ConceptHolder.this.concept.asType().asAttributeType();

            private void getOwners(final boolean onlyKey) {
                final Stream<? extends ThingType> owners = attributeType.getOwners(onlyKey);

                final Stream<TransactionProto.Transaction.Res> responses = owners.map(thingType -> {
                    final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                            .setAttributeTypeGetOwnersIterRes(ConceptProto.AttributeType.GetOwners.Iter.Res.newBuilder()
                                                                      .setOwner(ResponseBuilder.Concept.type(thingType))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void put(final ConceptProto.Attribute.Value protoValue) {
                final Attribute attribute;
                switch (protoValue.getValueCase()) {
                    case STRING:
                        attribute = attributeType.asString().put(protoValue.getString());
                        break;
                    case DOUBLE:
                        attribute = attributeType.asDouble().put(protoValue.getDouble());
                        break;
                    case LONG:
                        attribute = attributeType.asLong().put(protoValue.getLong());
                        break;
                    case DATETIME:
                        attribute = attributeType.asDateTime().put(Instant.ofEpochMilli(protoValue.getDatetime()).atOffset(ZoneOffset.UTC).toLocalDateTime());
                        break;
                    case BOOLEAN:
                        attribute = attributeType.asBoolean().put(protoValue.getBoolean());
                        break;
                    case VALUE_NOT_SET:
                    default:
                        throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
                }

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setAttributeTypePutRes(ConceptProto.AttributeType.Put.Res.newBuilder()
                                                        .setAttribute(ResponseBuilder.Concept.thing(attribute))).build();

                responseSender.accept(transactionRes(response));
            }

            private void get(final ConceptProto.Attribute.Value protoValue) {
                final Attribute attribute;
                switch (protoValue.getValueCase()) {
                    case STRING:
                        attribute = attributeType.asString().get(protoValue.getString());
                        break;
                    case DOUBLE:
                        attribute = attributeType.asDouble().get(protoValue.getDouble());
                        break;
                    case LONG:
                        attribute = attributeType.asLong().get(protoValue.getLong());
                        break;
                    case DATETIME:
                        attribute = attributeType.asDateTime().get(Instant.ofEpochMilli(protoValue.getDatetime()).atOffset(ZoneOffset.UTC).toLocalDateTime());
                        break;
                    case BOOLEAN:
                        attribute = attributeType.asBoolean().get(protoValue.getBoolean());
                        break;
                    case VALUE_NOT_SET:
                    default:
                        throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
                }

                final ConceptProto.AttributeType.Get.Res.Builder methodResponse = ConceptProto.AttributeType.Get.Res.newBuilder();
                if (attribute != null) methodResponse.setAttribute(ResponseBuilder.Concept.thing(attribute)).build();

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setAttributeTypeGetRes(methodResponse).build();

                responseSender.accept(transactionRes(response));
            }

            private void getRegex() {
                final Pattern regex = attributeType.asString().getRegex();

                final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                        .setAttributeTypeGetRegexRes(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
                                                             .setRegex((regex != null) ? regex.pattern() : "")).build();

                responseSender.accept(transactionRes(response));
            }

            private void setRegex(String regex) {
                if (regex.isEmpty()) {
                    attributeType.asString().setRegex(null);
                } else {
                    attributeType.asString().setRegex(Pattern.compile(regex));
                }
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.thing.Thing
         */
        private class ThingHolder {
            private final Thing thing = ConceptHolder.this.concept.asThing();

            private void isInferred() {
                boolean inferred = thing.isInferred();

                ConceptProto.ThingMethod.Res response = ConceptProto.ThingMethod.Res.newBuilder()
                        .setThingIsInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                                       .setInferred(inferred)).build();

                responseSender.accept(transactionRes(response));
            }

            private void getType() {
                final ThingType thingType = thing.getType();

                ConceptProto.ThingMethod.Res response = ConceptProto.ThingMethod.Res.newBuilder()
                        .setThingGetTypeRes(ConceptProto.Thing.GetType.Res.newBuilder()
                                                    .setThingType(ResponseBuilder.Concept.type(thingType))).build();

                responseSender.accept(transactionRes(response));
            }

            private void getHas(ConceptProto.Thing.GetHas.Iter.Req req) {
                final List<ConceptProto.Type> protoTypes = req.getAttributeTypesList();
                final Stream<? extends Attribute> attributes;

                if (protoTypes.isEmpty()) {
                    attributes = thing.getHas(req.getKeysOnly());
                } else {
                    final AttributeType[] attributeTypes = protoTypes.stream()
                            .map(ConceptHolder.this::convertType)
                            .map(ConceptRPC::conceptExists)
                            .map(grakn.core.concept.Concept::asType)
                            .map(grakn.core.concept.type.Type::asAttributeType)
                            .toArray(AttributeType[]::new);
                    attributes = thing.getHas(attributeTypes);
                }

                Stream<TransactionProto.Transaction.Res> responses = attributes.map(con -> {
                    ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                            .setThingGetHasIterRes(ConceptProto.Thing.GetHas.Iter.Res.newBuilder()
                                                           .setAttribute(ResponseBuilder.Concept.thing(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void getRelations(List<ConceptProto.Type> protoRoleTypes) {
                final RoleType[] roles = protoRoleTypes.stream()
                        .map(ConceptHolder.this::convertRoleType)
                        .map(ConceptRPC::conceptExists)
                        .toArray(RoleType[]::new);
                Stream<? extends Relation> concepts = thing.getRelations(roles);

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                            .setThingGetRelationsIterRes(ConceptProto.Thing.GetRelations.Iter.Res.newBuilder()
                                                                 .setRelation(ResponseBuilder.Concept.thing(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void getPlays() {
                Stream<? extends RoleType> roleTypes = thing.getPlays();

                Stream<TransactionProto.Transaction.Res> responses = roleTypes.map(con -> {
                    ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                            .setThingGetPlaysIterRes(ConceptProto.Thing.GetPlays.Iter.Res.newBuilder()
                                                             .setRoleType(ResponseBuilder.Concept.type(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void setHas(ConceptProto.Thing protoAttribute) {
                Attribute attribute = convertThing(protoAttribute).asThing().asAttribute();
                thing.setHas(attribute);

                ConceptProto.ThingMethod.Res response = ConceptProto.ThingMethod.Res.newBuilder()
                        .setThingSetHasRes(ConceptProto.Thing.SetHas.Res.newBuilder().build()).build();

                responseSender.accept(transactionRes(response));
            }

            private void unsetHas(ConceptProto.Thing protoAttribute) {
                Attribute attribute = convertThing(protoAttribute).asThing().asAttribute();
                thing.asThing().unsetHas(attribute);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.thing.Relation
         */
        private class RelationHolder {
            private final Relation relation = ConceptHolder.this.concept.asThing().asRelation();

            private void getPlayersByRoleType() {
                Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
                Stream.Builder<TransactionProto.Transaction.Res> responses = Stream.builder();

                for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> players : playersByRole.entrySet()) {
                    for (grakn.core.concept.thing.Thing player : players.getValue()) {
                        ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                                .setRelationGetPlayersByRoleTypeIterRes(ConceptProto.Relation.GetPlayersByRoleType.Iter.Res.newBuilder()
                                                                                .setRoleType(ResponseBuilder.Concept.type(players.getKey()))
                                                                                .setPlayer(ResponseBuilder.Concept.thing(player))).build();

                        responses.add(ResponseBuilder.Transaction.Iter.conceptMethod(res));
                    }
                }

                iterators.startBatchIterating(responses.build().iterator());
            }

            private void getPlayers(List<ConceptProto.Type> protoRoleTypes) {
                final RoleType[] roles = protoRoleTypes.stream()
                        .map(ConceptHolder.this::convertRoleType)
                        .map(ConceptRPC::conceptExists)
                        .toArray(RoleType[]::new);
                final Stream<? extends Thing> concepts = relation.getPlayers(roles);

                final Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    final ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                            .setRelationGetPlayersIterRes(ConceptProto.Relation.GetPlayers.Iter.Res.newBuilder()
                                                                  .setThing(ResponseBuilder.Concept.thing(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }

            private void addPlayer(ConceptProto.Relation.AddPlayer.Req request) {
                final RoleType role = convertRoleType(request.getRoleType());
                final Thing player = convertThing(request.getPlayer()).asThing();
                relation.addPlayer(role, player);
                responseSender.accept(null);
            }

            private void removePlayer(ConceptProto.Relation.RemovePlayer.Req request) {
                final RoleType role = convertRoleType(request.getRoleType());
                final Thing player = convertThing(request.getPlayer()).asThing();
                relation.asRelation().removePlayer(role, player);
                responseSender.accept(null);
            }
        }

        //
//        /**
//         * A utility class to execute methods on grakn.core.concept.thing.Attribute
//         */
        private class AttributeHolder {
            private final Attribute attribute = ConceptHolder.this.concept.asThing().asAttribute();

            private void getOwners(ConceptProto.Attribute.GetOwners.Iter.Req request) {
                final Stream<? extends Thing> things;
                switch (request.getFilterCase()) {
                    case THINGTYPE:
                        things = attribute.getOwners(convertType(request.getThingType()).asThingType());
                        break;
                    case FILTER_NOT_SET:
                    default:
                        things = attribute.getOwners();
                }

                final Stream<TransactionProto.Transaction.Res> responses = things.map(con -> {
                    ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                            .setAttributeGetOwnersIterRes(ConceptProto.Attribute.GetOwners.Iter.Res.newBuilder()
                                                                  .setThing(ResponseBuilder.Concept.thing(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator());
            }
        }
    }

    @Nonnull
    private static <T extends Concept> T conceptExists(@Nullable T concept) {
        if (concept == null) throw new GraknException(ErrorMessage.Server.MISSING_CONCEPT);
        return concept;
    }
}
