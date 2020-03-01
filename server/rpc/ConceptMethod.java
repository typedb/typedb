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
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on Concepts that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message grakn.protocol.session.ConceptProto.Method.Req.
 */
public class ConceptMethod {

    public static Transaction.Res run(Concept concept, ConceptProto.Method.Req req,
                                      SessionService.Iterators iterators, grakn.core.kb.server.Transaction tx) {
        ConceptHolder con = new ConceptHolder(concept, tx, iterators);
        switch (req.getReqCase()) {
            // Concept methods
            case CONCEPT_DELETE_REQ:
                return con.asConcept().delete();

            // SchemaConcept methods
            case SCHEMACONCEPT_ISIMPLICIT_REQ:
                return con.asSchemaConcept().isImplicit();
            case SCHEMACONCEPT_GETLABEL_REQ:
                return con.asSchemaConcept().label();
            case SCHEMACONCEPT_SETLABEL_REQ:
                return con.asSchemaConcept().label(req.getSchemaConceptSetLabelReq().getLabel());
            case SCHEMACONCEPT_GETSUP_REQ:
                return con.asSchemaConcept().sup();
            case SCHEMACONCEPT_SETSUP_REQ:
                return con.asSchemaConcept().sup(req.getSchemaConceptSetSupReq().getSchemaConcept());
            case SCHEMACONCEPT_SUPS_REQ:
                return con.asSchemaConcept().sups();
            case SCHEMACONCEPT_SUBS_REQ:
                return con.asSchemaConcept().subs();

            // Rule methods
            case RULE_WHEN_REQ:
                return con.asRule().when();
            case RULE_THEN_REQ:
                return con.asRule().then();

            // Role methods
            case ROLE_RELATIONS_REQ:
                return con.asRole().relations();
            case ROLE_PLAYERS_REQ:
                return con.asRole().players();

            // Type methods
            case TYPE_INSTANCES_REQ:
                return con.asType().instances();
            case TYPE_ISABSTRACT_REQ:
                return con.asType().isAbstract();
            case TYPE_SETABSTRACT_REQ:
                return con.asType().isAbstract(req.getTypeSetAbstractReq().getAbstract());
            case TYPE_KEYS_REQ:
                return con.asType().keys();
            case TYPE_ATTRIBUTES_REQ:
                return con.asType().attributes();
            case TYPE_PLAYING_REQ:
                return con.asType().playing();
            case TYPE_KEY_REQ:
                return con.asType().key(req.getTypeKeyReq().getAttributeType());
            case TYPE_HAS_REQ:
                return con.asType().has(req.getTypeHasReq().getAttributeType());
            case TYPE_PLAYS_REQ:
                return con.asType().plays(req.getTypePlaysReq().getRole());
            case TYPE_UNKEY_REQ:
                return con.asType().unkey(req.getTypeUnkeyReq().getAttributeType());
            case TYPE_UNHAS_REQ:
                return con.asType().unhas(req.getTypeUnhasReq().getAttributeType());
            case TYPE_UNPLAY_REQ:
                return con.asType().unplay(req.getTypeUnplayReq().getRole());

            // EntityType methods
            case ENTITYTYPE_CREATE_REQ:
                return con.asEntityType().create();

            // RelationType methods
            case RELATIONTYPE_CREATE_REQ:
                return con.asRelationType().create();
            case RELATIONTYPE_ROLES_REQ:
                return con.asRelationType().roles();
            case RELATIONTYPE_RELATES_REQ:
                return con.asRelationType().relates(req.getRelationTypeRelatesReq().getRole());
            case RELATIONTYPE_UNRELATE_REQ:
                return con.asRelationType().unrelate(req.getRelationTypeUnrelateReq().getRole());

            // AttributeType methods
            case ATTRIBUTETYPE_CREATE_REQ:
                return con.asAttributeType().create(req.getAttributeTypeCreateReq().getValue());
            case ATTRIBUTETYPE_ATTRIBUTE_REQ:
                return con.asAttributeType().attribute(req.getAttributeTypeAttributeReq().getValue());
            case ATTRIBUTETYPE_DATATYPE_REQ:
                return con.asAttributeType().dataType();
            case ATTRIBUTETYPE_GETREGEX_REQ:
                return con.asAttributeType().regex();
            case ATTRIBUTETYPE_SETREGEX_REQ:
                return con.asAttributeType().regex(req.getAttributeTypeSetRegexReq().getRegex());

            // Thing methods
            case THING_ISINFERRED_REQ:
                return con.asThing().isInferred();
            case THING_TYPE_REQ:
                return con.asThing().type();
            case THING_KEYS_REQ:
                return con.asThing().keys(req.getThingKeysReq().getAttributeTypesList());
            case THING_ATTRIBUTES_REQ:
                return con.asThing().attributes(req.getThingAttributesReq().getAttributeTypesList());
            case THING_RELATIONS_REQ:
                return con.asThing().relations(req.getThingRelationsReq().getRolesList());
            case THING_ROLES_REQ:
                return con.asThing().roles();
            case THING_RELHAS_REQ:
                return con.asThing().relhas(req.getThingRelhasReq().getAttribute());
            case THING_UNHAS_REQ:
                return con.asThing().unhas(req.getThingUnhasReq().getAttribute());

            // Relation methods
            case RELATION_ROLEPLAYERSMAP_REQ:
                return con.asRelation().rolePlayersMap();
            case RELATION_ROLEPLAYERS_REQ:
                return con.asRelation().rolePlayers(req.getRelationRolePlayersReq().getRolesList());
            case RELATION_ASSIGN_REQ:
                return con.asRelation().assign(req.getRelationAssignReq());
            case RELATION_UNASSIGN_REQ:
                return con.asRelation().unassign(req.getRelationUnassignReq());

            // Attribute Methods
            case ATTRIBUTE_VALUE_REQ:
                return con.asAttribute().value();
            case ATTRIBUTE_OWNERS_REQ:
                return con.asAttribute().owners();

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

        ConceptHolder(grakn.core.kb.concept.api.Concept concept, grakn.core.kb.server.Transaction tx, SessionService.Iterators iterators) {
            this.concept = concept;
            this.tx = tx;
            this.iterators = iterators;
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

            private Transaction.Res delete() {
                concept.delete();
                return null;
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.SchemaConcept
         */
        private class SchemaConcept {

            private Transaction.Res isImplicit() {
                Boolean implicit = concept.asSchemaConcept().isImplicit();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptIsImplicitRes(ConceptProto.SchemaConcept.IsImplicit.Res.newBuilder()
                                                               .setImplicit(implicit)).build();

                return transactionRes(response);
            }

            private Transaction.Res label() {
                Label label = concept.asSchemaConcept().label();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptGetLabelRes(ConceptProto.SchemaConcept.GetLabel.Res.newBuilder()
                                                             .setLabel(label.getValue())).build();

                return transactionRes(response);
            }

            private Transaction.Res label(String label) {
                concept.asSchemaConcept().label(Label.of(label));
                return null;
            }

            private Transaction.Res sup() {
                grakn.core.kb.concept.api.Concept superConcept = concept.asSchemaConcept().sup();

                ConceptProto.SchemaConcept.GetSup.Res.Builder responseConcept = ConceptProto.SchemaConcept.GetSup.Res.newBuilder();
                if (superConcept == null) responseConcept.setNull(ConceptProto.Null.getDefaultInstance());
                else responseConcept.setSchemaConcept(ResponseBuilder.Concept.concept(superConcept));

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptGetSupRes(responseConcept).build();

                return transactionRes(response);
            }

            private Transaction.Res sup(ConceptProto.Concept superConcept) {
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

                return null;
            }

            private Transaction.Res sups() {
                Stream<? extends grakn.core.kb.concept.api.SchemaConcept> concepts = concept.asSchemaConcept().sups();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setSchemaConceptSupsIterRes(ConceptProto.SchemaConcept.Sups.Iter.Res.newBuilder()
                                                                 .setSchemaConcept(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptSupsIter(ConceptProto.SchemaConcept.Sups.Iter.newBuilder()
                                                          .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res subs() {
                Stream<? extends grakn.core.kb.concept.api.SchemaConcept> concepts = concept.asSchemaConcept().subs();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setSchemaConceptSubsIterRes(ConceptProto.SchemaConcept.Subs.Iter.Res.newBuilder()
                                                                 .setSchemaConcept(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSchemaConceptSubsIter(ConceptProto.SchemaConcept.Subs.Iter.newBuilder()
                                                          .setId(iteratorId)).build();

                return transactionRes(response);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Rule
         */
        private class Rule {

            private Transaction.Res when() {
                Pattern pattern = concept.asRule().when();
                ConceptProto.Rule.When.Res.Builder whenRes = ConceptProto.Rule.When.Res.newBuilder();

                if (pattern == null) whenRes.setNull(ConceptProto.Null.getDefaultInstance());
                else whenRes.setPattern(pattern.toString());

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRuleWhenRes(whenRes).build();

                return transactionRes(response);
            }

            private Transaction.Res then() {
                Pattern pattern = concept.asRule().then();
                ConceptProto.Rule.Then.Res.Builder thenRes = ConceptProto.Rule.Then.Res.newBuilder();

                if (pattern == null) thenRes.setNull(ConceptProto.Null.getDefaultInstance());
                else thenRes.setPattern(pattern.toString());

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRuleThenRes(thenRes).build();

                return transactionRes(response);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Role
         */
        private class Role {

            private Transaction.Res relations() {
                Stream<grakn.core.kb.concept.api.RelationType> concepts = concept.asRole().relations();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRoleRelationsIterRes(ConceptProto.Role.Relations.Iter.Res.newBuilder()
                                                             .setRelationType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRoleRelationsIter(ConceptProto.Role.Relations.Iter.newBuilder()
                                                      .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res players() {
                Stream<grakn.core.kb.concept.api.Type> concepts = concept.asRole().players();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRolePlayersIterRes(ConceptProto.Role.Players.Iter.Res.newBuilder()
                                                           .setType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRolePlayersIter(ConceptProto.Role.Players.Iter.newBuilder()
                                                    .setId(iteratorId)).build();

                return transactionRes(response);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Type
         */
        private class Type {

            private Transaction.Res instances() {
                Stream<? extends grakn.core.kb.concept.api.Thing> concepts = concept.asType().instances();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeInstancesIterRes(ConceptProto.Type.Instances.Iter.Res.newBuilder()
                                                             .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeInstancesIter(ConceptProto.Type.Instances.Iter.newBuilder()
                                                      .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res isAbstract() {
                Boolean isAbstract = concept.asType().isAbstract();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeIsAbstractRes(ConceptProto.Type.IsAbstract.Res.newBuilder()
                                                      .setAbstract(isAbstract)).build();

                return transactionRes(response);
            }

            private Transaction.Res isAbstract(boolean isAbstract) {
                concept.asType().isAbstract(isAbstract);
                return null;
            }

            private Transaction.Res keys() {
                Stream<grakn.core.kb.concept.api.AttributeType> concepts = concept.asType().keys();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeKeysIterRes(ConceptProto.Type.Keys.Iter.Res.newBuilder()
                                                        .setAttributeType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeKeysIter(ConceptProto.Type.Keys.Iter.newBuilder()
                                                 .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res attributes() {
                Stream<grakn.core.kb.concept.api.AttributeType> concepts = concept.asType().attributes();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypeAttributesIterRes(ConceptProto.Type.Attributes.Iter.Res.newBuilder()
                                                              .setAttributeType(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypeAttributesIter(ConceptProto.Type.Attributes.Iter.newBuilder()
                                                       .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res playing() {
                Stream<grakn.core.kb.concept.api.Role> concepts = concept.asType().playing();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setTypePlayingIterRes(ConceptProto.Type.Playing.Iter.Res.newBuilder()
                                                           .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setTypePlayingIter(ConceptProto.Type.Playing.Iter.newBuilder()
                                                    .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res key(ConceptProto.Concept protoKey) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoKey).asAttributeType();
                concept.asType().key(attributeType);
                return null;
            }

            private Transaction.Res has(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoAttribute).asAttributeType();
                concept.asType().has(attributeType);
                return null;
            }

            private Transaction.Res plays(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asType().plays(role);
                return null;
            }

            private Transaction.Res unkey(ConceptProto.Concept protoKey) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoKey).asAttributeType();
                concept.asType().unkey(attributeType);
                return null;
            }

            private Transaction.Res unhas(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.AttributeType<?> attributeType = convert(protoAttribute).asAttributeType();
                concept.asType().unhas(attributeType);
                return null;
            }

            private Transaction.Res unplay(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asType().unplay(role);
                return null;
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.EntityType
         */
        private class EntityType {

            private Transaction.Res create() {
                Entity entity = concept.asEntityType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setEntityTypeCreateRes(ConceptProto.EntityType.Create.Res.newBuilder()
                                                        .setEntity(ResponseBuilder.Concept.concept(entity))).build();

                return transactionRes(response);
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.RelationType
         */
        private class RelationType {

            private Transaction.Res create() {
                grakn.core.kb.concept.api.Relation relation = concept.asRelationType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeCreateRes(ConceptProto.RelationType.Create.Res.newBuilder()
                                                          .setRelation(ResponseBuilder.Concept.concept(relation))).build();

                return transactionRes(response);
            }

            private Transaction.Res roles() {
                Stream<grakn.core.kb.concept.api.Role> roles = concept.asRelationType().roles();

                Stream<SessionProto.Transaction.Res> responses = roles.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setRelationTypeRolesIterRes(ConceptProto.RelationType.Roles.Iter.Res.newBuilder()
                                                                 .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationTypeRolesIter(ConceptProto.RelationType.Roles.Iter.newBuilder()
                                                          .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res relates(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asRelationType().relates(role);
                return null;
            }

            private Transaction.Res unrelate(ConceptProto.Concept protoRole) {
                grakn.core.kb.concept.api.Role role = convert(protoRole).asRole();
                concept.asRelationType().unrelate(role);
                return null;
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.AttributeType
         */
        private class AttributeType {

            private Transaction.Res create(ConceptProto.ValueObject protoValue) {
                switch (protoValue.getValueCase()) {
                    case BOOLEAN:
                        return create(AttributeSerialiser.BOOLEAN.deserialise(protoValue.getBoolean()));
                    case DATE:
                        return create(AttributeSerialiser.DATE.deserialise(protoValue.getDate()));
                    case DOUBLE:
                        return create(AttributeSerialiser.DOUBLE.deserialise(protoValue.getDouble()));
                    case FLOAT:
                        return create(AttributeSerialiser.FLOAT.deserialise(protoValue.getFloat()));
                    case INTEGER:
                        return create(AttributeSerialiser.INTEGER.deserialise(protoValue.getInteger()));
                    case LONG:
                        return create(AttributeSerialiser.LONG.deserialise(protoValue.getLong()));
                    case STRING:
                        return create(AttributeSerialiser.STRING.deserialise(protoValue.getString()));
                    default:
                        return null;
                }
            }

            private <D> Transaction.Res create(D value) {
                grakn.core.kb.concept.api.Attribute<D> attribute = concept.<D>asAttributeType().create(value);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeCreateRes(ConceptProto.AttributeType.Create.Res.newBuilder()
                                                           .setAttribute(ResponseBuilder.Concept.concept(attribute))).build();

                return transactionRes(response);
            }

            private Transaction.Res attribute(ConceptProto.ValueObject protoValue) {
                Object value = protoValue.getAllFields().values().iterator().next();
                grakn.core.kb.concept.api.Attribute<?> attribute = concept.asAttributeType().attribute(value);

                ConceptProto.AttributeType.Attribute.Res.Builder methodResponse = ConceptProto.AttributeType.Attribute.Res.newBuilder();
                if (attribute == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setAttribute(ResponseBuilder.Concept.concept(attribute)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeAttributeRes(methodResponse).build();

                return transactionRes(response);
            }

            private Transaction.Res dataType() {
                grakn.core.kb.concept.api.AttributeType.DataType<?> dataType = concept.asAttributeType().dataType();

                ConceptProto.AttributeType.DataType.Res.Builder methodResponse =
                        ConceptProto.AttributeType.DataType.Res.newBuilder();

                if (dataType == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setDataType(ResponseBuilder.Concept.DATA_TYPE(dataType)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeDataTypeRes(methodResponse).build();

                return transactionRes(response);
            }

            private Transaction.Res regex() {
                String regex = concept.asAttributeType().regex();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeTypeGetRegexRes(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
                                                             .setRegex((regex != null) ? regex : "")).build();

                return transactionRes(response);
            }

            private Transaction.Res regex(String regex) {
                if (regex.isEmpty()) {
                    concept.asAttributeType().regex(null);
                } else {
                    concept.asAttributeType().regex(regex);
                }
                return null;
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Thing
         */
        private class Thing {

            private Transaction.Res isInferred() {
                Boolean inferred = concept.asThing().isInferred();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingIsInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                                       .setInferred(inferred)).build();

                return transactionRes(response);
            }

            private Transaction.Res type() {
                grakn.core.kb.concept.api.Concept type = concept.asThing().type();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingTypeRes(ConceptProto.Thing.Type.Res.newBuilder()
                                                 .setType(ResponseBuilder.Concept.concept(type))).build();

                return transactionRes(response);
            }

            private Transaction.Res keys(List<ConceptProto.Concept> protoTypes) {
                grakn.core.kb.concept.api.AttributeType<?>[] keyTypes = protoTypes.stream()
                        .map(rpcConcept -> convert(rpcConcept))
                        .toArray(grakn.core.kb.concept.api.AttributeType[]::new);
                Stream<grakn.core.kb.concept.api.Attribute<?>> concepts = concept.asThing().keys(keyTypes);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingKeysIterRes(ConceptProto.Thing.Keys.Iter.Res.newBuilder()
                                                         .setAttribute(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingKeysIter(ConceptProto.Thing.Keys.Iter.newBuilder()
                                                  .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res attributes(List<ConceptProto.Concept> protoTypes) {
                grakn.core.kb.concept.api.AttributeType<?>[] attributeTypes = protoTypes.stream()
                        .map(rpcConcept -> convert(rpcConcept))
                        .toArray(grakn.core.kb.concept.api.AttributeType[]::new);
                Stream<grakn.core.kb.concept.api.Attribute<?>> concepts = concept.asThing().attributes(attributeTypes);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingAttributesIterRes(ConceptProto.Thing.Attributes.Iter.Res.newBuilder()
                                                               .setAttribute(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingAttributesIter(ConceptProto.Thing.Attributes.Iter.newBuilder()
                                                        .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res relations(List<ConceptProto.Concept> protoRoles) {
                grakn.core.kb.concept.api.Role[] roles = protoRoles.stream()
                        .map(rpcConcept -> convert(rpcConcept))
                        .toArray(grakn.core.kb.concept.api.Role[]::new);
                Stream<grakn.core.kb.concept.api.Relation> concepts = concept.asThing().relations(roles);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingRelationsIterRes(ConceptProto.Thing.Relations.Iter.Res.newBuilder()
                                                              .setRelation(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRelationsIter(ConceptProto.Thing.Relations.Iter.newBuilder()
                                                       .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res roles() {
                Stream<grakn.core.kb.concept.api.Role> concepts = concept.asThing().roles();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setThingRolesIterRes(ConceptProto.Thing.Roles.Iter.Res.newBuilder()
                                                          .setRole(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRolesIter(ConceptProto.Thing.Roles.Iter.newBuilder()
                                                   .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res relhas(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.Attribute<?> attribute = convert(protoAttribute).asAttribute();
                grakn.core.kb.concept.api.Relation relation = ConceptHolder.this.concept.asThing().relhas(attribute);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRelhasRes(ConceptProto.Thing.Relhas.Res.newBuilder()
                                                   .setRelation(ResponseBuilder.Concept.concept(relation))).build();

                return transactionRes(response);
            }

            private Transaction.Res unhas(ConceptProto.Concept protoAttribute) {
                grakn.core.kb.concept.api.Attribute<?> attribute = convert(protoAttribute).asAttribute();
                concept.asThing().unhas(attribute);
                return null;
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Relation
         */
        private class Relation {

            private Transaction.Res rolePlayersMap() {
                Map<grakn.core.kb.concept.api.Role, Set<grakn.core.kb.concept.api.Thing>> rolePlayersMap = concept.asRelation().rolePlayersMap();
                Stream.Builder<SessionProto.Transaction.Res> responses = Stream.builder();

                for (Map.Entry<grakn.core.kb.concept.api.Role, Set<grakn.core.kb.concept.api.Thing>> rolePlayers : rolePlayersMap.entrySet()) {
                    for (grakn.core.kb.concept.api.Thing player : rolePlayers.getValue()) {
                        ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                                .setRelationRolePlayersMapIterRes(ConceptProto.Relation.RolePlayersMap.Iter.Res.newBuilder()
                                                                          .setRole(ResponseBuilder.Concept.concept(rolePlayers.getKey()))
                                                                          .setPlayer(ResponseBuilder.Concept.concept(player))).build();

                        responses.add(ResponseBuilder.Transaction.Iter.conceptMethod(res));
                    }
                }

                int iteratorId = iterators.add(responses.build().iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationRolePlayersMapIter(ConceptProto.Relation.RolePlayersMap.Iter.newBuilder()
                                                               .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res rolePlayers(List<ConceptProto.Concept> protoRoles) {
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

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelationRolePlayersIter(ConceptProto.Relation.RolePlayers.Iter.newBuilder()
                                                            .setId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res assign(ConceptProto.Relation.Assign.Req request) {
                grakn.core.kb.concept.api.Role role = convert(request.getRole()).asRole();
                grakn.core.kb.concept.api.Thing player = convert(request.getPlayer()).asThing();
                concept.asRelation().assign(role, player);
                return null;
            }

            private Transaction.Res unassign(ConceptProto.Relation.Unassign.Req request) {
                grakn.core.kb.concept.api.Role role = convert(request.getRole()).asRole();
                grakn.core.kb.concept.api.Thing player = convert(request.getPlayer()).asThing();
                concept.asRelation().unassign(role, player);
                return null;
            }
        }

        /**
         * A utility class to execute methods on grakn.core.kb.concept.api.Attribute
         */
        private class Attribute {

            private Transaction.Res value() {
                Object value = concept.asAttribute().value();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeValueRes(ConceptProto.Attribute.Value.Res.newBuilder()
                                                      .setValue(ResponseBuilder.Concept.attributeValue(value))).build();

                return transactionRes(response);
            }

            private Transaction.Res owners() {
                Stream<grakn.core.kb.concept.api.Thing> concepts = concept.asAttribute().owners();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(con -> {
                    ConceptProto.Method.Iter.Res res = ConceptProto.Method.Iter.Res.newBuilder()
                            .setAttributeOwnersIterRes(ConceptProto.Attribute.Owners.Iter.Res.newBuilder()
                                                               .setThing(ResponseBuilder.Concept.concept(con))).build();
                    return ResponseBuilder.Transaction.Iter.conceptMethod(res);
                });

                int iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributeOwnersIter(ConceptProto.Attribute.Owners.Iter.newBuilder()
                                                        .setId(iteratorId)).build();

                return transactionRes(response);
            }
        }
    }
}