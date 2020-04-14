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

import grakn.core.core.AttributeSerialiser;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.protocol.session.ConceptProto;
import grakn.protocol.session.SessionProto;
import grakn.protocol.session.SessionProto.Transaction;
import graql.lang.pattern.Pattern;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on Concepts that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message grakn.protocol.session.ConceptProto.Method.Req.
 */
public class ConceptMethod {

    public static void run(Concept concept, ConceptProto.Method.Req req,
                                      SessionService.Iterators iterators, grakn.core.kb.server.Transaction tx, Consumer<Transaction.Res> responseSender) {
        ConceptHolder con = new ConceptHolder(concept, tx, iterators, responseSender, null);
        switch (req.getReqCase()) {
            // Concept methods
            case CONCEPT_DELETE_REQ:
                con.asConcept().delete();
                return;

            // SchemaConcept methods
            case SCHEMACONCEPT_ISIMPLICIT_REQ:
                con.asSchemaConcept().isImplicit();
                return;
            case SCHEMACONCEPT_GETLABEL_REQ:
                con.asSchemaConcept().label();
                return;

            case SCHEMACONCEPT_SETLABEL_REQ:
                con.asSchemaConcept().label(req.getSchemaConceptSetLabelReq().getLabel());
                return;
            case SCHEMACONCEPT_GETSUP_REQ:
                con.asSchemaConcept().sup();
                return;
            case SCHEMACONCEPT_SETSUP_REQ:
                con.asSchemaConcept().sup(req.getSchemaConceptSetSupReq().getSchemaConcept());
                return;

            // Rule methods
            case RULE_WHEN_REQ:
                con.asRule().when();
                return;
            case RULE_THEN_REQ:
                con.asRule().then();
                return;

            // Type methods
            case TYPE_ISABSTRACT_REQ:
                con.asType().isAbstract();
                return;
            case TYPE_SETABSTRACT_REQ:
                con.asType().isAbstract(req.getTypeSetAbstractReq().getAbstract());
                return;
            case TYPE_KEY_REQ:
                con.asType().key(req.getTypeKeyReq().getAttributeType());
                return;
            case TYPE_HAS_REQ:
                con.asType().has(req.getTypeHasReq().getAttributeType());
                return;
            case TYPE_PLAYS_REQ:
                con.asType().plays(req.getTypePlaysReq().getRole());
                return;
            case TYPE_UNKEY_REQ:
                con.asType().unkey(req.getTypeUnkeyReq().getAttributeType());
                return;
            case TYPE_UNHAS_REQ:
                con.asType().unhas(req.getTypeUnhasReq().getAttributeType());
                return;
            case TYPE_UNPLAY_REQ:
                con.asType().unplay(req.getTypeUnplayReq().getRole());
                return;

            // EntityType methods
            case ENTITYTYPE_CREATE_REQ:
                con.asEntityType().create();
                return;

            // RelationType methods
            case RELATIONTYPE_CREATE_REQ:
                con.asRelationType().create();
                return;
            case RELATIONTYPE_RELATES_REQ:
                con.asRelationType().relates(req.getRelationTypeRelatesReq().getRole());
                return;
            case RELATIONTYPE_UNRELATE_REQ:
                con.asRelationType().unrelate(req.getRelationTypeUnrelateReq().getRole());
                return;

            // AttributeType methods
            case ATTRIBUTETYPE_CREATE_REQ:
                con.asAttributeType().create(req.getAttributeTypeCreateReq().getValue());
                return;
            case ATTRIBUTETYPE_ATTRIBUTE_REQ:
                con.asAttributeType().attribute(req.getAttributeTypeAttributeReq().getValue());
                return;
            case ATTRIBUTETYPE_DATATYPE_REQ:
                con.asAttributeType().dataType();
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
            case THING_RELHAS_REQ:
                con.asThing().relhas(req.getThingRelhasReq().getAttribute());
                return;
            case THING_UNHAS_REQ:
                con.asThing().unhas(req.getThingUnhasReq().getAttribute());
                return;

            // Relation methods
            case RELATION_ASSIGN_REQ:
                con.asRelation().assign(req.getRelationAssignReq());
                return;
            case RELATION_UNASSIGN_REQ:
                con.asRelation().unassign(req.getRelationUnassignReq());
                return;

            // Attribute Methods
            case ATTRIBUTE_VALUE_REQ:
                con.asAttribute().value();
                return;

            default:
            case REQ_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + req);
        }
    }

    public static void iter(Concept concept, ConceptProto.Method.Iter.Req req,
                            SessionService.Iterators iterators, grakn.core.kb.server.Transaction tx, Consumer<Transaction.Res> responseSender, Transaction.Iter.Req.Options options)
    {
        ConceptHolder con = new ConceptHolder(concept, tx, iterators, responseSender, options);
        switch (req.getReqCase()) {
            // SchemaConcept methods
            case SCHEMACONCEPT_SUPS_ITER_REQ:
                con.asSchemaConcept().sups();
                return;
            case SCHEMACONCEPT_SUBS_ITER_REQ:
                con.asSchemaConcept().subs();
                return;

            // Role methods
            case ROLE_RELATIONS_ITER_REQ:
                con.asRole().relations();
                return;
            case ROLE_PLAYERS_ITER_REQ:
                con.asRole().players();
                return;

            // Type methods
            case TYPE_INSTANCES_ITER_REQ:
                con.asType().instances();
                return;
            case TYPE_KEYS_ITER_REQ:
                con.asType().keys();
                return;
            case TYPE_ATTRIBUTES_ITER_REQ:
                con.asType().attributes();
                return;
            case TYPE_PLAYING_ITER_REQ:
                con.asType().playing();
                return;

            // RelationType methods
            case RELATIONTYPE_ROLES_ITER_REQ:
                con.asRelationType().roles();
                return;

            // Thing methods
            case THING_KEYS_ITER_REQ:
                con.asThing().keys(req.getThingKeysIterReq().getAttributeTypesList());
                return;
            case THING_ATTRIBUTES_ITER_REQ:
                con.asThing().attributes(req.getThingAttributesIterReq().getAttributeTypesList());
                return;
            case THING_RELATIONS_ITER_REQ:
                con.asThing().relations(req.getThingRelationsIterReq().getRolesList());
                return;
            case THING_ROLES_ITER_REQ:
                con.asThing().roles();
                return;

            // Relation methods
            case RELATION_ROLEPLAYERSMAP_ITER_REQ:
                con.asRelation().rolePlayersMap();
                return;
            case RELATION_ROLEPLAYERS_ITER_REQ:
                con.asRelation().rolePlayers(req.getRelationRolePlayersIterReq().getRolesList());
                return;

            // Attribute methods
            case ATTRIBUTE_OWNERS_ITER_REQ:
                con.asAttribute().owners();
                return;

            default:
            case REQ_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + req);
        }
    }

    /**
     * A utility class to hold on to the concept in the method request will be applied to
     */
    public static class ConceptHolder {

        private grakn.core.kb.concept.api.Concept concept;
        private grakn.core.kb.server.Transaction tx;
        private SessionService.Iterators iterators;
        private Consumer<Transaction.Res> responseSender;
        private Transaction.Iter.Req.Options options;

        ConceptHolder(grakn.core.kb.concept.api.Concept concept, grakn.core.kb.server.Transaction tx, SessionService.Iterators iterators, Consumer<Transaction.Res> responseSender, Transaction.Iter.Req.Options options) {
            this.concept = concept;
            this.tx = tx;
            this.iterators = iterators;
            this.responseSender = responseSender;
            this.options = options;
        }

        private grakn.core.kb.concept.api.Concept convert(ConceptProto.Concept protoConcept) {
            return tx.getConcept(ConceptId.of(protoConcept.getId()));
        }

        Concept asConcept() {
            return new Concept();
        }

        SchemaConcept asSchemaConcept() {
            return new SchemaConcept();
        }

        Rule asRule() {
            return new Rule();
        }

        Role asRole() {
            return new Role();
        }

        Type asType() {
            return new Type();
        }

        EntityType asEntityType() {
            return new EntityType();
        }

        RelationType asRelationType() {
            return new RelationType();
        }

        AttributeType asAttributeType() {
            return new AttributeType();
        }

        Thing asThing() {
            return new Thing();
        }

        Relation asRelation() {
            return new Relation();
        }

        Attribute asAttribute() {
            return new Attribute();
        }

        private static SessionProto.Transaction.Res transactionRes(ConceptProto.Method.Res response) {
            return SessionProto.Transaction.Res.newBuilder()
                    .setConceptMethodRes(SessionProto.Transaction.ConceptMethod.Res.newBuilder()
                                                 .setResponse(response)).build();
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Concept
         */
        private class Concept {

            private void delete() {
                concept.delete();
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.SchemaConcept
         */
        private class SchemaConcept {

            private void isImplicit() {
                Boolean implicit = concept.asSchemaConcept().isImplicit();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptIsImplicitRes(ConceptProto.SchemaConcept.IsImplicit.Res.newBuilder()
                                                               .setImplicit(implicit)).build();

                responseSender.accept(transactionRes(response));
            }

            private void label() {
                Label label = concept.asSchemaConcept().label();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptGetLabelRes(ConceptProto.SchemaConcept.GetLabel.Res.newBuilder()
                                                             .setLabel(label.getValue())).build();

                responseSender.accept(transactionRes(response));
            }

            private void label(String label) {
                concept.asSchemaConcept().label(Label.of(label));
                responseSender.accept(null);
            }

            private void sup() {
                grakn.core.kb.concept.api.Concept superConcept = concept.asSchemaConcept().sup();

                ConceptProto.SchemaConcept.GetSup.Res.Builder responseConcept = ConceptProto.SchemaConcept.GetSup.Res.newBuilder();
                if (superConcept == null) responseConcept.setNull(ConceptProto.Null.getDefaultInstance());
                else responseConcept.setSchemaConcept(ResponseBuilder.Concept.concept(superConcept));

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptGetSupRes(responseConcept).build();

                responseSender.accept(transactionRes(response));
            }

            private void sup(ConceptProto.Concept superConcept) {
                // Make the second argument the super of the first argument
                // @throws GraknConceptException if the types are different, or setting the super to be a meta-type

                grakn.core.kb.concept.api.SchemaConcept sup = convert(superConcept).asSchemaConcept();
                grakn.core.kb.concept.api.SchemaConcept sub = concept.asSchemaConcept();

                if (sup.isEntityType()) {
                    sub.asEntityType().sup(sup.asEntityType());
                } else if (sup.isRelationType()) {
                    sub.asRelationType().sup(sup.asRelationType());
                } else if (sup.isRole()) {
                    sub.asRole().sup(sup.asRole());
                } else if (sup.isAttributeType()) {
                    sub.asAttributeType().sup(sup.asAttributeType());
                } else if (sup.isRule()) {
                    sub.asRule().sup(sup.asRule());
                } else {
                    throw GraknConceptException.invalidSuperType(sub.label(), sup);
                }

                responseSender.accept(null);
            }

            private void sups() {
                Stream<? extends grakn.core.kb.concept.api.SchemaConcept> concepts = concept.asSchemaConcept().sups();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setSchemaConceptSupsIterRes(ConceptProto.SchemaConcept.Sups.Iter.Res.newBuilder()
                                                                 .setSchemaConcept(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void subs() {
                Stream<? extends grakn.core.kb.concept.api.SchemaConcept> concepts = concept.asSchemaConcept().subs();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setSchemaConceptSubsIterRes(ConceptProto.SchemaConcept.Subs.Iter.Res.newBuilder()
                                                                 .setSchemaConcept(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Rule
         */
        private class Rule {

            private void when() {
                Pattern pattern = concept.asRule().when();
                ConceptProto.Rule.When.Res.Builder whenRes = ConceptProto.Rule.When.Res.newBuilder();

                if (pattern == null) whenRes.setNull(ConceptProto.Null.getDefaultInstance());
                else whenRes.setPattern(pattern.toString());

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRuleWhenRes(whenRes).build();

                responseSender.accept(transactionRes(response));
            }

            private void then() {
                Pattern pattern = concept.asRule().then();
                ConceptProto.Rule.Then.Res.Builder thenRes = ConceptProto.Rule.Then.Res.newBuilder();

                if (pattern == null) thenRes.setNull(ConceptProto.Null.getDefaultInstance());
                else thenRes.setPattern(pattern.toString());

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRuleThenRes(thenRes).build();

                responseSender.accept(transactionRes(response));
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Role
         */
        private class Role {

            private void relations() {
                Stream<grakn.core.kb.concept.api.RelationType> concepts = concept.asRole().relations();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRoleRelationsIterRes(ConceptProto.Role.Relations.Iter.Res.newBuilder()
                                                             .setRelationType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void players() {
                Stream<grakn.core.kb.concept.api.Type> concepts = concept.asRole().players();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRolePlayersIterRes(ConceptProto.Role.Players.Iter.Res.newBuilder()
                                                           .setType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Type
         */
        private class Type {

            private void instances() {
                Stream<? extends grakn.core.kb.concept.api.Thing> concepts = concept.asType().instances();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeInstancesIterRes(ConceptProto.Type.Instances.Iter.Res.newBuilder()
                                                             .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void isAbstract() {
                Boolean isAbstract = concept.asType().isAbstract();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeIsAbstractRes(ConceptProto.Type.IsAbstract.Res.newBuilder()
                                                      .setAbstract(isAbstract)).build();

                responseSender.accept(transactionRes(response));
            }

            private void isAbstract(boolean isAbstract) {
                concept.asType().isAbstract(isAbstract);

                responseSender.accept(null);
            }

            private void keys() {
                Stream<grakn.core.kb.concept.api.AttributeType> concepts = concept.asType().keys();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeKeysIterRes(ConceptProto.Type.Keys.Iter.Res.newBuilder()
                                                        .setAttributeType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void attributes() {
                Stream<grakn.core.kb.concept.api.AttributeType> concepts = concept.asType().attributes();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeAttributesIterRes(ConceptProto.Type.Attributes.Iter.Res.newBuilder()
                                                              .setAttributeType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void playing() {
                Stream<grakn.core.kb.concept.api.Role> concepts = concept.asType().playing();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypePlayingIterRes(ConceptProto.Type.Playing.Iter.Res.newBuilder()
                                                           .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void key(ConceptProto.Concept protoKey) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoKey).asAttributeType();
                concept.asType().key(attributeType);
                responseSender.accept(null);
            }

            private void has(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoAttribute).asAttributeType();
                concept.asType().has(attributeType);
                responseSender.accept(null);
            }

            private void plays(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asType().plays(role);
                responseSender.accept(null);
            }

            private void unkey(ConceptProto.Concept protoKey) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoKey).asAttributeType();
                concept.asType().unkey(attributeType);
                responseSender.accept(null);
            }

            private void unhas(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoAttribute).asAttributeType();
                concept.asType().unhas(attributeType);
                responseSender.accept(null);
            }

            private void unplay(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asType().unplay(role);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.EntityType
         */
        private class EntityType {

            private void create() {
                Entity entity = concept.asEntityType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setEntityTypeCreateRes(ConceptProto.EntityType.Create.Res.newBuilder()
                                                        .setEntity(ResponseBuilder.Concept.concept(entity))).build();

                responseSender.accept(transactionRes(response));
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.RelationType
         */
        private class RelationType {

            private void create() {
                grakn.core.kb.concept.api.Relation relation = concept.asRelationType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeCreateRes(ConceptProto.RelationType.Create.Res.newBuilder()
                                                          .setRelation(ResponseBuilder.Concept.concept(relation))).build();

                responseSender.accept(transactionRes(response));
            }

            private void roles() {
                Stream<grakn.core.kb.concept.api.Role> roles = concept.asRelationType().roles();

                Stream<SessionProto.Transaction.Res> responses = roles.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRelationTypeRolesIterRes(ConceptProto.RelationType.Roles.Iter.Res.newBuilder()
                                                                 .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void relates(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asRelationType().relates(role);
                responseSender.accept(null);
            }

            private void unrelate(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asRelationType().unrelate(role);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.AttributeType
         */
        private class AttributeType {

            private void create(ConceptProto.ValueObject protoValue) {
                switch (protoValue.getValueCase()) {
                    case BOOLEAN:
                        responseSender.accept(create(AttributeSerialiser.BOOLEAN.deserialise(protoValue.getBoolean())));
                        return;
                    case DATE:
                        responseSender.accept(create(AttributeSerialiser.DATE.deserialise(protoValue.getDate())));
                    case DOUBLE:
                        responseSender.accept(create(AttributeSerialiser.DOUBLE.deserialise(protoValue.getDouble())));
                    case FLOAT:
                        responseSender.accept(create(AttributeSerialiser.FLOAT.deserialise(protoValue.getFloat())));
                    case INTEGER:
                        responseSender.accept(create(AttributeSerialiser.INTEGER.deserialise(protoValue.getInteger())));
                    case LONG:
                        responseSender.accept(create(AttributeSerialiser.LONG.deserialise(protoValue.getLong())));
                    case STRING:
                        responseSender.accept(create(AttributeSerialiser.STRING.deserialise(protoValue.getString())));
                    default:
                        responseSender.accept(null);
                }
            }

            private <D> Transaction.Res create(D value) {
                grakn.core.kb.concept.api.Attribute<D> attribute = concept.<D>asAttributeType().create(value);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeCreateRes(ConceptProto.AttributeType.Create.Res.newBuilder()
                                                           .setAttribute(ResponseBuilder.Concept.concept(attribute))).build();

                return transactionRes(response);
            }

            private void attribute(ConceptProto.ValueObject protoValue) {
                Object value = protoValue.getAllFields().values().iterator().next();
                grakn.core.kb.concept.api.Attribute<?> attribute = concept.asAttributeType().attribute(value);

                ConceptProto.AttributeType.Attribute.Res.Builder methodResponse = ConceptProto.AttributeType.Attribute.Res.newBuilder();
                if (attribute == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setAttribute(ResponseBuilder.Concept.concept(attribute)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeAttributeRes(methodResponse).build();

                responseSender.accept(transactionRes(response));
            }

            private void dataType() {
                grakn.core.kb.concept.api.AttributeType.DataType<?> dataType = concept.asAttributeType().dataType();

                ConceptProto.AttributeType.DataType.Res.Builder methodResponse =
                        ConceptProto.AttributeType.DataType.Res.newBuilder();

                if (dataType == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setDataType(ResponseBuilder.Concept.DATA_TYPE(dataType)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeDataTypeRes(methodResponse).build();

                responseSender.accept(transactionRes(response));
            }

            private void regex() {
                String regex = concept.asAttributeType().regex();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeGetRegexRes(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
                                                             .setRegex((regex != null) ? regex : "")).build();

                responseSender.accept(transactionRes(response));
            }

            private void regex(String regex) {
                if (regex.isEmpty()) {
                    concept.asAttributeType().regex(null);
                } else {
                    concept.asAttributeType().regex(regex);
                }
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Thing
         */
        private class Thing {

            private void isInferred() {
                boolean inferred = concept.asThing().isInferred();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingIsInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                                       .setInferred(inferred)).build();

                responseSender.accept(transactionRes(response));
            }

            private void type() {
                grakn.core.kb.concept.api.Concept type = concept.asThing().type();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingTypeRes(ConceptProto.Thing.Type.Res.newBuilder()
                                                 .setType(ResponseBuilder.Concept.concept(type))).build();

                responseSender.accept(transactionRes(response));
            }

            private void keys(List<ConceptProto.Concept> protoTypes) {
                grakn.core.kb.concept.api.AttributeType<?>[] keyTypes = protoTypes.stream()
                        .map(ConceptHolder.this::convert)
                        .toArray(grakn.core.kb.concept.api.AttributeType[]::new);
                Stream<grakn.core.kb.concept.api.Attribute<?>> concepts = concept.asThing().keys(keyTypes);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingKeysIterRes(ConceptProto.Thing.Keys.Iter.Res.newBuilder()
                                                         .setAttribute(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void attributes(List<ConceptProto.Concept> protoTypes) {
                grakn.core.kb.concept.api.AttributeType<?>[] attributeTypes = protoTypes.stream()
                        .map(ConceptHolder.this::convert)
                        .toArray(grakn.core.kb.concept.api.AttributeType[]::new);
                Stream<grakn.core.kb.concept.api.Attribute<?>> concepts = concept.asThing().attributes(attributeTypes);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingAttributesIterRes(ConceptProto.Thing.Attributes.Iter.Res.newBuilder()
                                                               .setAttribute(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void relations(List<ConceptProto.Concept> protoRoles) {
                grakn.core.kb.concept.api.Role[] roles = protoRoles.stream()
                        .map(ConceptHolder.this::convert)
                        .toArray(grakn.core.kb.concept.api.Role[]::new);
                Stream<grakn.core.kb.concept.api.Relation> concepts = concept.asThing().relations(roles);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingRelationsIterRes(ConceptProto.Thing.Relations.Iter.Res.newBuilder()
                                                              .setRelation(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void roles() {
                Stream<grakn.core.kb.concept.api.Role> concepts = concept.asThing().roles();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingRolesIterRes(ConceptProto.Thing.Roles.Iter.Res.newBuilder()
                                                          .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void relhas(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.Attribute<?> attribute = convert(protoAttribute).asAttribute();
                grakn.core.kb.concept.api.Relation relation = ConceptHolder.this.concept.asThing().relhas(attribute);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRelhasRes(ConceptProto.Thing.Relhas.Res.newBuilder()
                                                   .setRelation(ResponseBuilder.Concept.concept(relation))).build();

                responseSender.accept(transactionRes(response));
            }

            private void unhas(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.Attribute<?> attribute = convert(protoAttribute).asAttribute();
                concept.asThing().unhas(attribute);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Relation
         */
        private class Relation {

            private void rolePlayersMap() {
                Map<grakn.core.kb.concept.api.Role, List<grakn.core.kb.concept.api.Thing>> rolePlayersMap = concept.asRelation().rolePlayersMap();
                Stream.Builder<SessionProto.Transaction.Res> responses = Stream.builder();

                for (Map.Entry<grakn.core.kb.concept.api.Role, List<grakn.core.kb.concept.api.Thing>> rolePlayers : rolePlayersMap.entrySet()) {
                    for (grakn.core.kb.concept.api.Thing player : rolePlayers.getValue()) {
                        ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                                .setRelationRolePlayersMapIterRes(ConceptProto.Relation.RolePlayersMap.Iter.Res.newBuilder()
                                                                          .setRole(ResponseBuilder.Concept.concept(rolePlayers.getKey()))
                                                                          .setPlayer(ResponseBuilder.Concept.concept(player))).build();

                        responses.add(ResponseBuilder.Transaction.Iter.conceptMethod(res));
                    }
                }

                iterators.startBatchIterating(responses.build().iterator(), options);
            }

            private void rolePlayers(List<ConceptProto.Concept> protoRoles) {
                grakn.core.kb.concept.api.Role[] roles = protoRoles.stream()
                        .map(rpcConcept -> convert(rpcConcept))
                        .toArray(grakn.core.kb.concept.api.Role[]::new);
                Stream<grakn.core.kb.concept.api.Thing> concepts = concept.asRelation().rolePlayers(roles);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRelationRolePlayersIterRes(ConceptProto.Relation.RolePlayers.Iter.Res.newBuilder()
                                                                   .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }

            private void assign(ConceptProto.Relation.Assign.Req request) {
                grakn.core.kb.concept.api.Role role = convert(request.getRole()).asRole();
                grakn.core.kb.concept.api.Thing player = convert(request.getPlayer()).asThing();
                concept.asRelation().assign(role, player);
                responseSender.accept(null);
            }

            private void unassign(ConceptProto.Relation.Unassign.Req request) {
                grakn.core.kb.concept.api.Role role = convert(request.getRole()).asRole();
                grakn.core.kb.concept.api.Thing player = convert(request.getPlayer()).asThing();
                concept.asRelation().unassign(role, player);
                responseSender.accept(null);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Attribute
         */
        private class Attribute {

            private void value() {
                Object value = concept.asAttribute().value();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeValueRes(ConceptProto.Attribute.Value.Res.newBuilder()
                                                      .setValue(ResponseBuilder.Concept.attributeValue(value))).build();

                responseSender.accept(transactionRes(response));
            }

            private void owners() {
                Stream<grakn.core.kb.concept.api.Thing> concepts = concept.asAttribute().owners();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setAttributeOwnersIterRes(ConceptProto.Attribute.Owners.Iter.Res.newBuilder()
                                                               .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                iterators.startBatchIterating(responses.iterator(), options);
            }
        }
    }
}