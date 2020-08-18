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
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on Concepts that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message grakn.protocol.session.ConceptProto.Method.Req.
 */
class ConceptRPC {

    private static final Logger LOG = LoggerFactory.getLogger(ConceptRPC.class);

    static void run(ConceptHolder con, ConceptProto.Method.Req req) {
        switch (req.getReqCase()) {
            // Concept methods
            case CONCEPT_DELETE_REQ:
                con.delete();
                return;

            case TYPE_GETLABEL_REQ:
                con.asType().getLabel();
                return;

            case TYPE_SETLABEL_REQ:
                con.asType().setLabel(req.getTypeSetLabelReq().getLabel());
                return;
            case TYPE_GETSUPERTYPE_REQ:
                con.asType().getSup();
                return;
            case TYPE_SETSUPERTYPE_REQ:
                con.asType().setSup(req.getTypeSetSupertypeReq().getType());
                return;

//            // Rule methods
//            case RULE_WHEN_REQ:
//                con.asRule().when();
//                return;
//            case RULE_THEN_REQ:
//                con.asRule().then();
//                return;

            // Type methods
            case THINGTYPE_ISABSTRACT_REQ:
                con.asThingType().isAbstract();
                return;
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
                con.asThingType().setPlays(req.getThingTypeSetPlaysReq().getRole());
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
                con.asRelationType().setRelates(req.getRelationTypeSetRelatesReq().getLabel());
                return;

            // AttributeType methods
            case ATTRIBUTETYPE_PUT_REQ:
                con.asAttributeType().put(req.getAttributeTypePutReq().getValue());
                return;
            case ATTRIBUTETYPE_GET_REQ:
                con.asAttributeType().get(req.getAttributeTypeGetReq().getValue());
                return;
            case ATTRIBUTETYPE_GETVALUETYPE_REQ:
                con.asAttributeType().getValueType();
                return;
            case ATTRIBUTETYPE_GETREGEX_REQ:
                con.asAttributeType().getRegex();
                return;
            case ATTRIBUTETYPE_SETREGEX_REQ:
                con.asAttributeType().setRegex(req.getAttributeTypeSetRegexReq().getRegex());
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

            // Attribute Methods
            case ATTRIBUTE_GETVALUE_REQ:
                con.asAttribute().getValue();
                return;

            default:
            case REQ_NOT_SET:
                throw new GraknException(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    static void iter(ConceptHolder con, ConceptProto.Method.Iter.Req req) {
        switch (req.getReqCase()) {
            // Type methods
            case TYPE_GETSUPERTYPES_ITER_REQ:
                con.asType().getSupertypes();
                return;
            case TYPE_GETSUBTYPES_ITER_REQ:
                con.asType().getSubs();
                return;

            // Role methods
            case ROLETYPE_GETRELATIONS_ITER_REQ:
                con.asRoleType().getRelations();
                return;
            case ROLETYPE_GETPLAYERS_ITER_REQ:
                con.asRoleType().getPlayers();
                return;

            // ThingType methods
            case THINGTYPE_GETINSTANCES_ITER_REQ:
                con.asThingType().getInstances();
                return;
            case THINGTYPE_GETOWNS_ITER_REQ:
                con.asThingType().getOwns(req.getThingTypeGetOwnsIterReq().getKeysOnly());
                return;
            case THINGTYPE_GETPLAYS_ITER_REQ:
                con.asThingType().getPlays();
                return;

            // RelationType methods
            case RELATIONTYPE_GETRELATES_ITER_REQ:
                con.asRelationType().getRelates();
                return;

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
                con.asAttribute().getOwners(req.getAttributeGetOwnersIterReq().getThingType());
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
        private final grakn.core.Grakn.Transaction tx;
        private final TransactionRPC.Iterators iterators;
        private final Consumer<TransactionProto.Transaction.Res> responseSender;
        private final TransactionProto.Transaction.Iter.Req.Options options;

        ConceptHolder(ByteString conceptId, grakn.core.Grakn.Transaction tx, TransactionRPC.Iterators iterators, Consumer<TransactionProto.Transaction.Res> responseSender, TransactionProto.Transaction.Iter.Req.Options options) {
            this.concept = conceptExists(tx.concepts().getConcept(conceptId.toByteArray()));
            this.tx = tx;
            this.iterators = iterators;
            this.responseSender = responseSender;
            this.options = options;
        }

        ConceptHolder(String label, grakn.core.Grakn.Transaction tx, TransactionRPC.Iterators iterators, Consumer<TransactionProto.Transaction.Res> responseSender, TransactionProto.Transaction.Iter.Req.Options options) {
            this.concept = conceptExists(tx.concepts().getType(label));
            this.tx = tx;
            this.iterators = iterators;
            this.responseSender = responseSender;
            this.options = options;
        }

        private static TransactionProto.Transaction.Res transactionRes(ConceptProto.Method.Res response) {
            return TransactionProto.Transaction.Res.newBuilder()
                    .setConceptMethodRes(TransactionProto.Transaction.ConceptMethod.Res.newBuilder()
                                                 .setResponse(response)).build();
        }

        private Concept convert(ConceptProto.Concept protoConcept) {
            return tx.concepts().getConcept(protoConcept.getIid().toByteArray());
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

            private void getLabel() {
                String label = type.getLabel();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeGetLabelRes(ConceptProto.Type.GetLabel.Res.newBuilder()
                                                    .setLabel(label)).build();

                responseSender.accept(transactionRes(response));
            }

            private void setLabel(String label) {
                type.setLabel(label);
                responseSender.accept(null);
            }

            private void getSup() {
                grakn.core.concept.Concept superConcept = type.getSupertype();

                ConceptProto.Type.GetSupertype.Res.Builder responseConcept = ConceptProto.Type.GetSupertype.Res.newBuilder();
                if (superConcept == null) responseConcept.setNull(ConceptProto.Null.getDefaultInstance());
                else responseConcept.setType(ResponseBuilder.Concept.concept(superConcept));

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeGetSupertypeRes(responseConcept).build();

                responseSender.accept(transactionRes(response));
            }

            private void setSup(ConceptProto.Concept superConcept) {
                // Make the second argument the super of the first argument

                Type sup = convert(superConcept).asType();

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
                Stream<? extends Type> concepts = type.asType().getSupertypes();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeGetSupertypesIterRes(ConceptProto.Type.GetSupertypes.Iter.Res.newBuilder()
                                                                 .setType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void getSubs() {
                Stream<? extends Type> concepts = type.asType().getSubtypes();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeGetSubtypesIterRes(ConceptProto.Type.GetSubtypes.Iter.Res.newBuilder()
                                                               .setType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
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

            private void getRelations() {
                Stream<? extends RelationType> concepts = roleType.getRelations();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRoleTypeGetRelationsIterRes(ConceptProto.RoleType.GetRelations.Iter.Res.newBuilder()
                                                                    .setRelationType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void getPlayers() {
                Stream<? extends ThingType> concepts = roleType.getPlayers();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRoleTypeGetPlayersIterRes(ConceptProto.RoleType.GetPlayers.Iter.Res.newBuilder()
                                                                  .setThingType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
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
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingTypeGetInstancesIterRes(ConceptProto.ThingType.GetInstances.Iter.Res.newBuilder()
                                                                     .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
                LOG.trace("{} end instances", Thread.currentThread());
            }

            private void isAbstract() {
                boolean isAbstract = thingType.isAbstract();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingTypeIsAbstractRes(ConceptProto.ThingType.IsAbstract.Res.newBuilder()
                                                           .setAbstract(isAbstract)).build();

                responseSender.accept(transactionRes(response));
            }

            private void setAbstract() {
                thingType.setAbstract();
                responseSender.accept(null);
            }

            private void unsetAbstract() {
                thingType.unsetAbstract();
                responseSender.accept(null);
            }

            private void getOwns(boolean keysOnly) {
                Stream<? extends AttributeType> concepts = thingType.getOwns(keysOnly);

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingTypeGetOwnsIterRes(ConceptProto.ThingType.GetOwns.Iter.Res.newBuilder()
                                                                .setAttributeType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void getPlays() {
                Stream<? extends RoleType> concepts = thingType.getPlays();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingTypeGetPlaysIterRes(ConceptProto.ThingType.GetPlays.Iter.Res.newBuilder()
                                                                 .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void setOwns(ConceptProto.ThingType.SetOwns.Req req) {
                final AttributeType attributeType = convert(req.getAttributeType()).asType().asAttributeType();
                final boolean isKey = req.getIsKey();

                switch (req.getOverriddenCase()) {
                    case OVERRIDDENTYPE:
                        AttributeType overriddenType = convert(req.getOverriddenType()).asType().asAttributeType();
                        thingType.setOwns(attributeType, overriddenType, isKey);
                        break;
                    case NULL:
                    case OVERRIDDEN_NOT_SET:
                    default:
                        thingType.setOwns(attributeType, isKey);
                        break;
                }

                responseSender.accept(null);
            }

            private void setPlays(ConceptProto.Concept protoRole) {
                RoleType role = convert(protoRole).asType().asRoleType();
                thingType.setPlays(role);
                responseSender.accept(null);
            }

            private void unsetOwns(ConceptProto.Concept protoAttribute) {
                AttributeType attributeType = convert(protoAttribute).asType().asAttributeType();
                thingType.unsetOwns(attributeType);
                responseSender.accept(null);
            }

            private void unsetPlays(ConceptProto.Concept protoRole) {
                RoleType role = convert(protoRole).asType().asRoleType();
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
                Entity entity = entityType.create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setEntityTypeCreateRes(ConceptProto.EntityType.Create.Res.newBuilder()
                                                        .setEntity(ResponseBuilder.Concept.concept(entity))).build();

                responseSender.accept(transactionRes(response));
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.type.RelationType
         */
        private class RelationTypeHolder {
            private final RelationType relationType = ConceptHolder.this.concept.asType().asRelationType();

            private void create() {
                Relation relation = relationType.asRelationType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeCreateRes(ConceptProto.RelationType.Create.Res.newBuilder()
                                                          .setRelation(ResponseBuilder.Concept.concept(relation))).build();

                responseSender.accept(transactionRes(response));
            }

            private void getRelates() {
                Stream<? extends RoleType> roles = relationType.getRelates();

                Stream<TransactionProto.Transaction.Res> responses = roles.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRelationTypeGetRelatesIterRes(ConceptProto.RelationType.GetRelates.Iter.Res.newBuilder()
                                                                      .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void getRelates(String label) {
                RoleType role = relationType.getRelates(label);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeGetRelatesForRoleLabelRes(ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder()
                                                                          .setRole(ResponseBuilder.Concept.concept(role))).build();

                responseSender.accept(transactionRes(response));
            }

            private void setRelates(String label) {
                relationType.setRelates(label);
                final RoleType roleType = relationType.getRelates(label);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeSetRelatesRes(ConceptProto.RelationType.SetRelates.Res.newBuilder()
                                                              .setRole(ResponseBuilder.Concept.concept(roleType))).build();

                responseSender.accept(transactionRes(response));
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.type.AttributeType
         */
        private class AttributeTypeHolder {
            private final AttributeType attributeType = ConceptHolder.this.concept.asType().asAttributeType();

            private void put(ConceptProto.ValueObject protoValue) {
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

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypePutRes(ConceptProto.AttributeType.Put.Res.newBuilder()
                                                        .setAttribute(ResponseBuilder.Concept.concept(attribute))).build();

                responseSender.accept(transactionRes(response));
            }

            private void get(ConceptProto.ValueObject protoValue) {
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

                ConceptProto.AttributeType.Get.Res.Builder methodResponse = ConceptProto.AttributeType.Get.Res.newBuilder();
                if (attribute == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setAttribute(ResponseBuilder.Concept.concept(attribute)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeGetRes(methodResponse).build();

                responseSender.accept(transactionRes(response));
            }

            private void getValueType() {
                AttributeType.ValueType valueType = attributeType.asAttributeType().getValueType();

                ConceptProto.AttributeType.GetValueType.Res.Builder methodResponse =
                        ConceptProto.AttributeType.GetValueType.Res.newBuilder();

                if (valueType == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setValueType(ResponseBuilder.Concept.VALUE_TYPE(valueType)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeGetValueTypeRes(methodResponse).build();

                responseSender.accept(transactionRes(response));
            }

            private void getRegex() {
                String regex = attributeType.asString().getRegex();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeGetRegexRes(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
                                                             .setRegex((regex != null) ? regex : "")).build();

                responseSender.accept(transactionRes(response));
            }

            private void setRegex(String regex) {
                if (regex.isEmpty()) {
                    attributeType.asString().setRegex(null);
                } else {
                    attributeType.asString().setRegex(regex);
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

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingIsInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                                       .setInferred(inferred)).build();

                responseSender.accept(transactionRes(response));
            }

            private void getType() {
                Concept type = thing.getType();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingGetTypeRes(ConceptProto.Thing.GetType.Res.newBuilder()
                                                    .setThingType(ResponseBuilder.Concept.concept(type))).build();

                responseSender.accept(transactionRes(response));
            }

            private void getHas(ConceptProto.Thing.GetHas.Iter.Req req) {
                final List<ConceptProto.Concept> protoTypes = req.getAttributeTypesList();
                final Stream<? extends Attribute> attributes;

                if (protoTypes.isEmpty()) {
                    attributes = thing.getHas(req.getKeysOnly());
                } else {
                    final AttributeType[] attributeTypes = protoTypes.stream()
                            .map(ConceptHolder.this::convert)
                            .map(ConceptRPC::conceptExists)
                            .map(grakn.core.concept.Concept::asType)
                            .map(grakn.core.concept.type.Type::asAttributeType)
                            .toArray(AttributeType[]::new);
                    attributes = thing.getHas(attributeTypes);
                }

                Stream<TransactionProto.Transaction.Res> responses = attributes.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingGetHasIterRes(ConceptProto.Thing.GetHas.Iter.Res.newBuilder()
                                                           .setAttribute(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void getRelations(List<ConceptProto.Concept> protoRoles) {
                final RoleType[] roles = protoRoles.stream()
                        .map(ConceptHolder.this::convert)
                        .map(ConceptRPC::conceptExists)
                        .map(grakn.core.concept.Concept::asType)
                        .map(grakn.core.concept.type.Type::asRoleType)
                        .toArray(RoleType[]::new);
                Stream<? extends Relation> concepts = thing.getRelations(roles);

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingGetRelationsIterRes(ConceptProto.Thing.GetRelations.Iter.Res.newBuilder()
                                                                 .setRelation(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void getPlays() {
                Stream<? extends RoleType> concepts = thing.getPlays();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingGetPlaysIterRes(ConceptProto.Thing.GetPlays.Iter.Res.newBuilder()
                                                             .setRoleType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void setHas(ConceptProto.Concept protoAttribute) {
                Attribute attribute = convert(protoAttribute).asThing().asAttribute();
                thing.setHas(attribute);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingSetHasRes(ConceptProto.Thing.SetHas.Res.newBuilder().build()).build();

                responseSender.accept(transactionRes(response));
            }

            private void unsetHas(ConceptProto.Concept protoAttribute) {
                Attribute attribute = convert(protoAttribute).asThing().asAttribute();
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
                        ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                                .setRelationGetPlayersByRoleTypeIterRes(ConceptProto.Relation.GetPlayersByRoleType.Iter.Res.newBuilder()
                                                                                .setRoleType(ResponseBuilder.Concept.concept(players.getKey()))
                                                                                .setPlayer(ResponseBuilder.Concept.concept(player))).build();

                        responses.add(ResponseBuilder.Transaction.Iter.conceptMethod(res));
                    }
                }

                iterators.startBatchIterating(responses.build().iterator(), options);
            }

            private void getPlayers(List<ConceptProto.Concept> protoRoles) {
                final RoleType[] roles = protoRoles.stream()
                        .map(ConceptHolder.this::convert)
                        .map(ConceptRPC::conceptExists)
                        .map(grakn.core.concept.Concept::asType)
                        .map(grakn.core.concept.type.Type::asRoleType)
                        .toArray(RoleType[]::new);
                final Stream<? extends Thing> concepts = relation.getPlayers(roles);

                final Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    final ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRelationGetPlayersIterRes(ConceptProto.Relation.GetPlayers.Iter.Res.newBuilder()
                                                                  .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void addPlayer(ConceptProto.Relation.AddPlayer.Req request) {
                final RoleType role = convert(request.getRoleType()).asType().asRoleType();
                final Thing player = convert(request.getPlayer()).asThing();
                relation.addPlayer(role, player);
                responseSender.accept(null);
            }

            private void removePlayer(ConceptProto.Relation.RemovePlayer.Req request) {
                final RoleType role = convert(request.getRoleType()).asType().asRoleType();
                final Thing player = convert(request.getPlayer()).asThing();
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

            private void getValue() {
                final ConceptProto.ValueObject.Builder value = ConceptProto.ValueObject.newBuilder();

                if (attribute instanceof Attribute.String) {
                    value.setString(((Attribute.String) attribute).getValue());
                } else if (attribute instanceof Attribute.Long) {
                    value.setLong(((Attribute.Long) attribute).getValue());
                } else if (attribute instanceof Attribute.Boolean) {
                    value.setBoolean(((Attribute.Boolean) attribute).getValue());
                } else if (attribute instanceof Attribute.DateTime) {
                    value.setDatetime(((Attribute.DateTime) attribute).getValue().toInstant(ZoneOffset.UTC).toEpochMilli());
                } else if (attribute instanceof Attribute.Double) {
                    value.setDouble(((Attribute.Double) attribute).getValue());
                }
                final ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeGetValueRes(ConceptProto.Attribute.GetValue.Res.newBuilder()
                                                         .setValue(value)).build();

                responseSender.accept(transactionRes(response));
            }

            private void getOwners(ConceptProto.Concept ownerType) {
                final Stream<? extends Thing> concepts;
                if (ownerType != null) {
                    concepts = attribute.getOwners(convert(ownerType).asType().asThingType());
                } else {
                    concepts = attribute.getOwners();
                }

                final Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setAttributeGetOwnersIterRes(ConceptProto.Attribute.GetOwners.Iter.Res.newBuilder()
                                                                  .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }
        }
    }

    @Nonnull
    private static <T extends Concept> T conceptExists(@Nullable T concept) {
        if (concept == null) throw new GraknException(ErrorMessage.Server.MISSING_CONCEPT);
        return concept;
    }
}
