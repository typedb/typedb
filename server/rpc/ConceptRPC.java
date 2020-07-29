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
import grakn.core.common.exception.Error;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on Concepts that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message grakn.protocol.session.ConceptProto.Method.Req.
 */
class ConceptRPC {

    private static final Logger LOG = LoggerFactory.getLogger(ConceptRPC.class);

    static void run(ByteString conceptId, ConceptProto.Method.Req req,
                    TransactionRPC.Iterators iterators, Grakn.Transaction tx, Consumer<TransactionProto.Transaction.Res> responseSender) {
        ConceptHolder con = new ConceptHolder(conceptId, tx, iterators, responseSender, TransactionProto.Transaction.Iter.Req.Options.getDefaultInstance());
        switch (req.getReqCase()) {
            // Concept methods
            case CONCEPT_DELETE_REQ:
                con.delete();
                return;

            case TYPE_GETLABEL_REQ:
                con.asType().label();
                return;

            case TYPE_SETLABEL_REQ:
                con.asType().label(req.getTypeSetLabelReq().getLabel());
                return;
            case TYPE_GETSUP_REQ:
                con.asType().sup();
                return;
            case TYPE_SETSUP_REQ:
                con.asType().sup(req.getTypeSetSupReq().getType());
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
                con.asThingType().isAbstract(req.getThingTypeSetAbstractReq().getAbstract());
                return;
            case THINGTYPE_HAS_REQ:
                con.asThingType().has(req.getThingTypeHasReq());
                return;
            case THINGTYPE_PLAYS_REQ:
                con.asThingType().plays(req.getThingTypePlaysReq().getRole());
                return;
            case THINGTYPE_UNHAS_REQ:
                con.asThingType().unhas(req.getThingTypeUnhasReq().getAttributeType());
                return;
            case THINGTYPE_UNPLAY_REQ:
                con.asThingType().unplay(req.getThingTypeUnplayReq().getRole());
                return;

            // EntityType methods
            case ENTITYTYPE_CREATE_REQ:
                con.asEntityType().create();
                return;

            // RelationType methods
            case RELATIONTYPE_CREATE_REQ:
                con.asRelationType().create();
                return;
            case RELATIONTYPE_ROLE_REQ:
                con.asRelationType().role(req.getRelationTypeRoleReq().getLabel());
            case RELATIONTYPE_RELATES_REQ:
                con.asRelationType().relates(req.getRelationTypeRelatesReq().getLabel());
                return;

            // AttributeType methods
            case ATTRIBUTETYPE_PUT_REQ:
                con.asAttributeType().put(req.getAttributeTypePutReq().getValue());
                return;
            case ATTRIBUTETYPE_GET_REQ:
                con.asAttributeType().get(req.getAttributeTypeGetReq().getValue());
                return;
            case ATTRIBUTETYPE_VALUETYPE_REQ:
                con.asAttributeType().valueType();
                return;
            case ATTRIBUTETYPE_GETREGEX_REQ:
                con.asAttributeType().regex();
                return;
            case ATTRIBUTETYPE_SETREGEX_REQ:
                con.asAttributeType().regex(req.getAttributeTypeSetRegexReq().getRegex());
                return;

            // Thing methods
            case THING_ISINFERRED_REQ:
                con.asThing().isInferred();
                return;
            case THING_TYPE_REQ:
                con.asThing().type();
                return;
            case THING_HAS_REQ:
                con.asThing().has(req.getThingHasReq().getAttribute());
                return;
            case THING_UNHAS_REQ:
                con.asThing().unhas(req.getThingUnhasReq().getAttribute());
                return;

            // Relation methods
            case RELATION_RELATE_REQ:
                con.asRelation().relate(req.getRelationRelateReq());
                return;
            case RELATION_UNRELATE_REQ:
                con.asRelation().unrelate(req.getRelationUnrelateReq());
                return;

            // Attribute Methods
            case ATTRIBUTE_VALUE_REQ:
                con.asAttribute().value();
                return;

            default:
            case REQ_NOT_SET:
                throw new GraknException(Error.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    static void iter(ByteString conceptId, ConceptProto.Method.Iter.Req req,
                     TransactionRPC.Iterators iterators, grakn.core.Grakn.Transaction tx, Consumer<TransactionProto.Transaction.Res> responseSender, TransactionProto.Transaction.Iter.Req.Options options) {
        ConceptHolder con = new ConceptHolder(conceptId, tx, iterators, responseSender, options);
        switch (req.getReqCase()) {
            // Type methods
            case TYPE_SUPS_ITER_REQ:
                con.asType().sups();
                return;
            case TYPE_SUBS_ITER_REQ:
                con.asType().subs();
                return;

            // Role methods
            case ROLE_RELATIONS_ITER_REQ:
                con.asRoleType().relations();
                return;
            case ROLE_PLAYERS_ITER_REQ:
                con.asRoleType().players();
                return;

            // ThingType methods
            case THINGTYPE_INSTANCES_ITER_REQ:
                con.asThingType().instances();
                return;
            case THINGTYPE_ATTRIBUTES_ITER_REQ:
                con.asThingType().attributes(req.getThingTypeAttributesIterReq().getKeysOnly());
                return;
            case THINGTYPE_PLAYING_ITER_REQ:
                con.asThingType().playing();
                return;

            // RelationType methods
            case RELATIONTYPE_ROLES_ITER_REQ:
                con.asRelationType().roles();
                return;

            // Thing methods
            case THING_ATTRIBUTES_ITER_REQ:
                con.asThing().attributes(req.getThingAttributesIterReq());
                return;
            case THING_RELATIONS_ITER_REQ:
                con.asThing().relations(req.getThingRelationsIterReq().getRolesList());
                return;
            case THING_ROLES_ITER_REQ:
                con.asThing().roles();
                return;

            // Relation methods
            case RELATION_PLAYERSMAP_ITER_REQ:
                con.asRelation().playersMap();
                return;
            case RELATION_PLAYERS_ITER_REQ:
                con.asRelation().players(req.getRelationPlayersIterReq().getRolesList());
                return;

            // Attribute methods
            case ATTRIBUTE_OWNERS_ITER_REQ:
                con.asAttribute().owners();
                return;

            default:
            case REQ_NOT_SET:
                throw new GraknException(Error.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    /**
     * A utility class to hold on to the concept in the method request will be applied to
     */
    private static class ConceptHolder {

        private final Concept concept;
        private final grakn.core.Grakn.Transaction tx;
        private final TransactionRPC.Iterators iterators;
        private final Consumer<TransactionProto.Transaction.Res> responseSender;
        private final TransactionProto.Transaction.Iter.Req.Options options;

        private ConceptHolder(ByteString conceptId, grakn.core.Grakn.Transaction tx, TransactionRPC.Iterators iterators, Consumer<TransactionProto.Transaction.Res> responseSender, TransactionProto.Transaction.Iter.Req.Options options) {
            this.concept = conceptExists(tx.concepts().getConcept(conceptId.toByteArray()));
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
            private final Type concept = ConceptHolder.this.concept.asType();

            private void label() {
                String label = concept.label();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeGetLabelRes(ConceptProto.Type.GetLabel.Res.newBuilder()
                                                    .setLabel(label)).build();

                responseSender.accept(transactionRes(response));
            }

            private void label(String label) {
                concept.label(label);
                responseSender.accept(null);
            }

            private void sup() {
                grakn.core.concept.Concept superConcept = concept.sup();

                ConceptProto.Type.GetSup.Res.Builder responseConcept = ConceptProto.Type.GetSup.Res.newBuilder();
                if (superConcept == null) responseConcept.setNull(ConceptProto.Null.getDefaultInstance());
                else responseConcept.setType(ResponseBuilder.Concept.concept(superConcept));

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeGetSupRes(responseConcept).build();

                responseSender.accept(transactionRes(response));
            }

            private void sup(ConceptProto.Concept superConcept) {
                // Make the second argument the super of the first argument

                Type sup = convert(superConcept).asType();

                if (concept instanceof EntityType) {
                    concept.asEntityType().sup(sup.asEntityType());
                } else if (concept instanceof RelationType) {
                    concept.asRelationType().sup(sup.asRelationType());
                } else if (concept instanceof AttributeType) {
                    concept.asAttributeType().sup(sup.asAttributeType());
                }

                responseSender.accept(null);
            }

            private void sups() {
                Stream<? extends Type> concepts = concept.asType().sups();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeSupsIterRes(ConceptProto.Type.Sups.Iter.Res.newBuilder()
                                                        .setType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void subs() {
                Stream<? extends Type> concepts = concept.asType().subs();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeSubsIterRes(ConceptProto.Type.Subs.Iter.Res.newBuilder()
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
            private final RoleType concept = ConceptHolder.this.concept.asRoleType();

            private void relations() {
                Stream<? extends RelationType> concepts = concept.relations();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRoleRelationsIterRes(ConceptProto.RoleType.Relations.Iter.Res.newBuilder()
                                                             .setRelationType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void players() {
                Stream<? extends ThingType> concepts = concept.players();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRolePlayersIterRes(ConceptProto.RoleType.Players.Iter.Res.newBuilder()
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
            private final ThingType concept = ConceptHolder.this.concept.asThingType();

            private void instances() {
                LOG.trace("{} instances", Thread.currentThread());
                Stream<? extends grakn.core.concept.thing.Thing> concepts = concept.instances();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingTypeInstancesIterRes(ConceptProto.ThingType.Instances.Iter.Res.newBuilder()
                                                                  .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
                LOG.trace("{} end instances", Thread.currentThread());
            }

            private void isAbstract() {
                boolean isAbstract = concept.isAbstract();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingTypeIsAbstractRes(ConceptProto.ThingType.IsAbstract.Res.newBuilder()
                                                           .setAbstract(isAbstract)).build();

                responseSender.accept(transactionRes(response));
            }

            private void isAbstract(boolean isAbstract) {
                concept.isAbstract(isAbstract);

                responseSender.accept(null);
            }

            private void attributes(boolean keysOnly) {
                Stream<? extends AttributeType> concepts = concept.attributes(keysOnly);

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingTypeAttributesIterRes(ConceptProto.ThingType.Attributes.Iter.Res.newBuilder()
                                                                   .setAttributeType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void playing() {
                Stream<? extends RoleType> concepts = concept.playing();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingTypePlayingIterRes(ConceptProto.ThingType.Playing.Iter.Res.newBuilder()
                                                                .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void has(ConceptProto.ThingType.Has.Req req) {
                final AttributeType attributeType = convert(req.getAttributeType()).asAttributeType();
                final boolean isKey = req.getIsKey();

                switch (req.getOverriddenCase()) {
                    case OVERRIDDENTYPE:
                        AttributeType overriddenType = convert(req.getOverriddenType()).asAttributeType();
                        concept.has(attributeType, overriddenType, isKey);
                        break;
                    case NULL:
                    case OVERRIDDEN_NOT_SET:
                    default:
                        concept.has(attributeType, isKey);
                        break;
                }

                responseSender.accept(null);
            }

            private void plays(ConceptProto.Concept protoRole) {
                RoleType role = convert(protoRole).asRoleType();
                concept.plays(role);
                responseSender.accept(null);
            }

            private void unhas(ConceptProto.Concept protoAttribute) {
                AttributeType attributeType = convert(protoAttribute).asAttributeType();
                concept.unhas(attributeType);
                responseSender.accept(null);
            }

            private void unplay(ConceptProto.Concept protoRole) {
                RoleType role = convert(protoRole).asRoleType();
                concept.unplay(role);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.EntityType
         */
        private class EntityTypeHolder {
            private final EntityType entityType = concept.asEntityType();

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
            private final RelationType concept = ConceptHolder.this.concept.asRelationType();

            private void create() {
                Relation relation = concept.asRelationType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeCreateRes(ConceptProto.RelationType.Create.Res.newBuilder()
                                                          .setRelation(ResponseBuilder.Concept.concept(relation))).build();

                responseSender.accept(transactionRes(response));
            }

            private void roles() {
                Stream<? extends RoleType> roles = concept.roles();

                Stream<TransactionProto.Transaction.Res> responses = roles.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRelationTypeRolesIterRes(ConceptProto.RelationType.Roles.Iter.Res.newBuilder()
                                                                 .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void relates(String label) {
                RoleType role = concept.relates(label);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeRelatesRes(ConceptProto.RelationType.Relates.Res.newBuilder()
                                                           .setRole(ResponseBuilder.Concept.concept(role))).build();

                responseSender.accept(transactionRes(response));
            }

            private void role(String label) {
                RoleType role = concept.role(label);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeRoleRes(ConceptProto.RelationType.Role.Res.newBuilder()
                                                        .setRole(ResponseBuilder.Concept.concept(role))).build();

                responseSender.accept(transactionRes(response));
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.type.AttributeType
         */
        private class AttributeTypeHolder {
            private final AttributeType concept = ConceptHolder.this.concept.asAttributeType();

            private void put(ConceptProto.ValueObject protoValue) {
                final Attribute attribute;
                switch (protoValue.getValueCase()) {
                    case STRING:
                        attribute = concept.asString().put(protoValue.getString());
                        break;
                    case DOUBLE:
                        attribute = concept.asDouble().put(protoValue.getDouble());
                        break;
                    case LONG:
                        attribute = concept.asLong().put(protoValue.getLong());
                        break;
                    case DATETIME:
                        attribute = concept.asDateTime().put(Instant.ofEpochMilli(protoValue.getDatetime()).atOffset(ZoneOffset.UTC).toLocalDateTime());
                        break;
                    case BOOLEAN:
                        attribute = concept.asBoolean().put(protoValue.getBoolean());
                        break;
                    case VALUE_NOT_SET:
                    default:
                        throw new GraknException(Error.Server.BAD_VALUE_TYPE);
                }

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypePutRes(ConceptProto.AttributeType.Create.Res.newBuilder()
                                                        .setAttribute(ResponseBuilder.Concept.concept(attribute))).build();

                responseSender.accept(transactionRes(response));
            }

            private void get(ConceptProto.ValueObject protoValue) {
                final Attribute attribute;
                switch (protoValue.getValueCase()) {
                    case STRING:
                        attribute = concept.asString().get(protoValue.getString());
                        break;
                    case DOUBLE:
                        attribute = concept.asDouble().get(protoValue.getDouble());
                        break;
                    case LONG:
                        attribute = concept.asLong().get(protoValue.getLong());
                        break;
                    case DATETIME:
                        attribute = concept.asDateTime().get(Instant.ofEpochMilli(protoValue.getDatetime()).atOffset(ZoneOffset.UTC).toLocalDateTime());
                        break;
                    case BOOLEAN:
                        attribute = concept.asBoolean().get(protoValue.getBoolean());
                        break;
                    case VALUE_NOT_SET:
                    default:
                        throw new GraknException(Error.Server.BAD_VALUE_TYPE);
                }

                ConceptProto.AttributeType.Attribute.Res.Builder methodResponse = ConceptProto.AttributeType.Attribute.Res.newBuilder();
                if (attribute == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setAttribute(ResponseBuilder.Concept.concept(attribute)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeGetRes(methodResponse).build();

                responseSender.accept(transactionRes(response));
            }

            private void valueType() {
                AttributeType.ValueType valueType = concept.asAttributeType().valueType();

                ConceptProto.AttributeType.ValueType.Res.Builder methodResponse =
                        ConceptProto.AttributeType.ValueType.Res.newBuilder();

                if (valueType == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setValueType(ResponseBuilder.Concept.VALUE_TYPE(valueType)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeValueTypeRes(methodResponse).build();

                responseSender.accept(transactionRes(response));
            }

            private void regex() {
                String regex = concept.asString().regex();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeGetRegexRes(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
                                                             .setRegex((regex != null) ? regex : "")).build();

                responseSender.accept(transactionRes(response));
            }

            private void regex(String regex) {
                if (regex.isEmpty()) {
                    concept.asString().regex(null);
                } else {
                    concept.asString().regex(regex);
                }
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.thing.Thing
         */
        private class ThingHolder {
            private final Thing concept = ConceptHolder.this.concept.asThing();

            private void isInferred() {
                boolean inferred = concept.isInferred();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingIsInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                                       .setInferred(inferred)).build();

                responseSender.accept(transactionRes(response));
            }

            private void type() {
                Concept type = concept.type();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingTypeRes(ConceptProto.Thing.Type.Res.newBuilder()
                                                 .setThingType(ResponseBuilder.Concept.concept(type))).build();

                responseSender.accept(transactionRes(response));
            }

            private void attributes(ConceptProto.Thing.Attributes.Iter.Req req) {
                final List<ConceptProto.Concept> protoTypes = req.getAttributeTypesList();
                final Stream<? extends Attribute> attributes;

                if (protoTypes.isEmpty()) {
                    attributes = concept.attributes(req.getKeysOnly());
                } else {
                    List<AttributeType> attributeTypes = protoTypes.stream()
                            .map(ConceptHolder.this::convert)
                            .map(ConceptRPC::conceptExists)
                            .map(grakn.core.concept.Concept::asAttributeType)
                            .collect(Collectors.toList());
                    attributes = concept.attributes(attributeTypes);
                }

                Stream<TransactionProto.Transaction.Res> responses = attributes.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingAttributesIterRes(ConceptProto.Thing.Attributes.Iter.Res.newBuilder()
                                                               .setAttribute(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void relations(List<ConceptProto.Concept> protoRoles) {
                List<RoleType> roles = protoRoles.stream()
                        .map(ConceptHolder.this::convert)
                        .map(ConceptRPC::conceptExists)
                        .map(Concept::asRoleType)
                        .collect(Collectors.toList());
                Stream<? extends Relation> concepts = concept.relations(roles);

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingRelationsIterRes(ConceptProto.Thing.Relations.Iter.Res.newBuilder()
                                                              .setRelation(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void roles() {
                Stream<? extends RoleType> concepts = concept.roles();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingRolesIterRes(ConceptProto.Thing.Roles.Iter.Res.newBuilder()
                                                          .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void has(ConceptProto.Concept protoAttribute) {
                Attribute attribute = convert(protoAttribute).asAttribute();
                concept.has(attribute);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingHasRes(ConceptProto.Thing.Has.Res.newBuilder().build()).build();

                responseSender.accept(transactionRes(response));
            }

            private void unhas(ConceptProto.Concept protoAttribute) {
                Attribute attribute = convert(protoAttribute).asAttribute();
                concept.asThing().unhas(attribute);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.concept.thing.Relation
         */
        private class RelationHolder {
            private final Relation concept = ConceptHolder.this.concept.asRelation();

            private void playersMap() {
                Map<? extends RoleType, ? extends List<? extends Thing>> playersMap = concept.playersMap();
                Stream.Builder<TransactionProto.Transaction.Res> responses = Stream.builder();

                for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> players : playersMap.entrySet()) {
                    for (grakn.core.concept.thing.Thing player : players.getValue()) {
                        ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                                .setRelationPlayersMapIterRes(ConceptProto.Relation.PlayersMap.Iter.Res.newBuilder()
                                                                      .setRole(ResponseBuilder.Concept.concept(players.getKey()))
                                                                      .setPlayer(ResponseBuilder.Concept.concept(player))).build();

                        responses.add(ResponseBuilder.Transaction.Iter.conceptMethod(res));
                    }
                }

                iterators.startBatchIterating(responses.build().iterator(), options);
            }

            private void players(List<ConceptProto.Concept> protoRoles) {
                List<RoleType> roles = protoRoles.stream()
                        .map(ConceptHolder.this::convert)
                        .map(ConceptRPC::conceptExists)
                        .map(Concept::asRoleType)
                        .collect(Collectors.toList());
                Stream<? extends Thing> concepts = concept.players(roles);

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRelationPlayersIterRes(ConceptProto.Relation.Players.Iter.Res.newBuilder()
                                                               .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void relate(ConceptProto.Relation.Relate.Req request) {
                RoleType role = convert(request.getRole()).asRoleType();
                Thing player = convert(request.getPlayer()).asThing();
                concept.relate(role, player);
                responseSender.accept(null);
            }

            private void unrelate(ConceptProto.Relation.Unrelate.Req request) {
                RoleType role = convert(request.getRole()).asRoleType();
                Thing player = convert(request.getPlayer()).asThing();
                concept.asRelation().unrelate(role, player);
                responseSender.accept(null);
            }
        }

        //
//        /**
//         * A utility class to execute methods on grakn.core.concept.thing.Attribute
//         */
        private class AttributeHolder {
            private final Attribute concept = ConceptHolder.this.concept.asAttribute();

            private void value() {
                ConceptProto.ValueObject.Builder value = ConceptProto.ValueObject.newBuilder();

                if (concept instanceof Attribute.String) {
                    value.setString(((Attribute.String) concept).value());
                } else if (concept instanceof Attribute.Long) {
                    value.setLong(((Attribute.Long) concept).value());
                } else if (concept instanceof Attribute.Boolean) {
                    value.setBoolean(((Attribute.Boolean) concept).value());
                } else if (concept instanceof Attribute.DateTime) {
                    value.setDatetime(((Attribute.DateTime) concept).value().toInstant(ZoneOffset.UTC).toEpochMilli());
                } else if (concept instanceof Attribute.Double) {
                    value.setDouble(((Attribute.Double) concept).value());
                }
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeValueRes(ConceptProto.Attribute.Value.Res.newBuilder()
                                                      .setValue(value)).build();

                responseSender.accept(transactionRes(response));
            }

            private void owners() {
                Stream<? extends Thing> concepts = concept.asAttribute().owners();

                Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setAttributeOwnersIterRes(ConceptProto.Attribute.Owners.Iter.Res.newBuilder()
                                                               .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }
        }
    }

    @Nonnull
    private static <T extends Concept> T conceptExists(@Nullable T concept) {
        if (concept == null) throw new GraknException(Error.Server.MISSING_CONCEPT);
        return concept;
    }
}
