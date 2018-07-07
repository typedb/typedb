/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.rpc;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionProto.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message {@link ai.grakn.rpc.proto.ConceptProto.Method.Req}.
 */
public class ConceptMethod {

    public static Transaction.Res run(Concept concept, ConceptProto.Method.Req req,
                                 SessionService.Iterators iterators, EmbeddedGraknTx tx) {
        ConceptHolder con = new ConceptHolder(concept, tx, iterators);
        switch (req.getReqCase()) {
            // Concept methods
            case DELETE:
                return con.asConcept().delete();

            // SchemaConcept methods
            case ISIMPLICIT:
                return con.asSchemaConcept().isImplicit();
            case GETLABEL:
                return con.asSchemaConcept().label();
            case SETLABEL:
                return con.asSchemaConcept().label(req.getSetLabel().getLabel());
            case GETSUP:
                return con.asSchemaConcept().sup();
            case SETSUP:
                return con.asSchemaConcept().sup(req.getSetSup().getConcept());
            case SUPS:
                return con.asSchemaConcept().sups();
            case SUBS:
                return con.asSchemaConcept().subs();

            // Rule methods
            case WHEN:
                return con.asRule().when();
            case THEN:
                return con.asRule().then();

            // Role methods
            case RELATIONS:
                return con.asRole().relations();
            case PLAYERS:
                return con.asRole().players();

            // Type methods
            case INSTANCES:
                return con.asType().instances();
            case ISABSTRACT:
                return con.asType().isAbstract();
            case SETABSTRACT:
                return con.asType().isAbstract(req.getSetAbstract().getAbstract());
            case KEYS:
                return con.asType().keys();
            case ATTRIBUTES:
                return con.asType().attributes();
            case PLAYING:
                return con.asType().playing();
            case KEY:
                return con.asType().key(req.getKey().getConcept());
            case HAS:
                return con.asType().has(req.getHas().getConcept());
            case PLAYS:
                return con.asType().plays(req.getPlays().getConcept());
            case UNKEY:
                return con.asType().unkey(req.getUnkey().getConcept());
            case UNHAS:
                return con.asType().unhas(req.getUnhas().getConcept());
            case UNPLAY:
                return con.asType().unplay(req.getUnplay().getConcept());

            // EntityType methods
            case CREATEENTITY:
                return con.asEntityType().create();

            // RelationshipType methods
            case CREATERELATION:
                return con.asRelationshipType().create();
            case ROLES:
                return con.asRelationshipType().roles();
            case RELATES:
                return con.asRelationshipType().relates(req.getRelates().getConcept());
            case UNRELATE:
                return con.asRelationshipType().unrelate(req.getUnrelate().getConcept());

            // AttributeType methods
            case CREATEATTRIBUTE:
                return con.asAttributeType().create(req.getCreateAttribute().getValue());
            case ATTRIBUTE:
                return con.asAttributeType().attribute(req.getAttribute().getValue());
            case DATATYPE:
                return con.asAttributeType().dataType();
            case GETREGEX:
                return con.asAttributeType().regex();
            case SETREGEX:
                return con.asAttributeType().regex(req.getSetRegex().getRegex());

            // Thing methods
            case THING_ISINFERRED:
                return con.asThing().isInferred();
            case THING_TYPE:
                return con.asThing().type();
            case THING_KEYS:
                return con.asThing().keys(req.getThingKeys().getConceptsList());
            case THING_ATTRIBUTES:
                return con.asThing().attributes(req.getThingAttributes().getConceptsList());
            case THING_RELATIONS:
                return con.asThing().relations(req.getThingRelations().getConceptsList());
            case THING_ROLES:
                return con.asThing().roles();
            case THING_RELHAS:
                return con.asThing().relhas(req.getThingRelhas().getConcept());
            case THING_UNHAS:
                return con.asThing().unhas(req.getThingUnhas().getConcept());

            // Relationship methods
            case ROLEPLAYERSMAP:
                return con.asRelationship().rolePlayersMap();
            case ROLEPLAYERS:
                return con.asRelationship().rolePlayers(req.getRolePlayers().getConceptsList());
            case ASSIGN:
                return con.asRelationship().assign(req.getAssign().getRolePlayer());
            case UNASSIGN:
                return con.asRelationship().unassign(req.getUnassign().getRolePlayer());

            // Attribute Methods
            case VALUE:
                return con.asAttribute().value();
            case OWNERS:
                return con.asAttribute().owners();

            default:
            case REQ_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + req);
        }
    }

    public static class ConceptHolder {

        private ai.grakn.concept.Concept concept;
        private EmbeddedGraknTx tx;
        private SessionService.Iterators iterators;

        ConceptHolder(ai.grakn.concept.Concept concept, EmbeddedGraknTx tx, SessionService.Iterators iterators) {
            this.concept = concept;
            this.tx = tx;
            this.iterators = iterators;
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
        
        RelationshipType asRelationshipType() {
            return new RelationshipType();
        }
        
        AttributeType asAttributeType() {
            return new AttributeType();
        }
        
        Thing asThing() {
            return new Thing();
        }
        
        Relationship asRelationship() {
            return new Relationship();
        }
        
        Attribute asAttribute() {
            return new Attribute();
        }

        private static SessionProto.Transaction.Res transactionRes(ConceptProto.Method.Res response) {
            return SessionProto.Transaction.Res.newBuilder()
                    .setConceptMethod(SessionProto.ConceptMethod.Res.newBuilder()
                            .setResponse(response)).build();
        }

        private class Concept {

            private Transaction.Res delete() {
                concept.delete();
                return null;
            }
        }
        
        private class SchemaConcept {

            private Transaction.Res isImplicit() {
                Boolean implicit = concept.asSchemaConcept().isImplicit();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setIsImplicit(ConceptProto.SchemaConcept.IsImplicit.Res.newBuilder()
                                .setImplicit(implicit)).build();

                return transactionRes(response);
            }

            private Transaction.Res label() {
                Label label = concept.asSchemaConcept().label();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetLabel(ConceptProto.SchemaConcept.GetLabel.Res.newBuilder()
                                .setLabel(label.getValue())).build();

                return transactionRes(response);
            }

            private Transaction.Res label(String label) {
                concept.asSchemaConcept().label(Label.of(label));
                return null;
            }

            private Transaction.Res sup() {
                ai.grakn.concept.Concept superConcept = concept.asSchemaConcept().sup();

                ConceptProto.SchemaConcept.GetSup.Res.Builder responseConcept = ConceptProto.SchemaConcept.GetSup.Res.newBuilder();
                if (superConcept == null) responseConcept.setNull(ConceptProto.Null.getDefaultInstance());
                else responseConcept.setConcept(ConceptBuilder.concept(superConcept));

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetSup(responseConcept).build();

                return transactionRes(response);
            }

            private Transaction.Res sup(ConceptProto.Concept superConcept) {
                // Make the second argument the super of the first argument
                // @throws GraqlQueryException if the types are different, or setting the super to be a meta-type

                ai.grakn.concept.SchemaConcept sup = ConceptBuilder.concept(superConcept, tx).asSchemaConcept();
                ai.grakn.concept.SchemaConcept sub = concept.asSchemaConcept();

                if (sup.isEntityType()) {
                    sub.asEntityType().sup(sup.asEntityType());
                } else if (sup.isRelationshipType()) {
                    sub.asRelationshipType().sup(sup.asRelationshipType());
                } else if (sup.isRole()) {
                    sub.asRole().sup(sup.asRole());
                } else if (sup.isAttributeType()) {
                    sub.asAttributeType().sup(sup.asAttributeType());
                } else if (sup.isRule()) {
                    sub.asRule().sup(sup.asRule());
                } else {
                    throw GraqlQueryException.insertMetaType(sub.label(), sup);
                }

                return null;
            }

            private Transaction.Res sups() {
                Stream<? extends ai.grakn.concept.SchemaConcept> concepts = concept.asSchemaConcept().sups();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSups(ConceptProto.SchemaConcept.Sups.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res subs() {
                Stream<? extends ai.grakn.concept.SchemaConcept> concepts = concept.asSchemaConcept().subs();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setSubs(ConceptProto.SchemaConcept.Subs.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }
        }

        private class Rule {

            private Transaction.Res when() {
                Pattern pattern = concept.asRule().when();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setWhen(ConceptProto.Rule.When.Res.newBuilder()
                                .setPattern(pattern.toString())).build();

                return transactionRes(response);
            }

            private Transaction.Res then() {
                Pattern pattern = concept.asRule().then();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThen(ConceptProto.Rule.Then.Res.newBuilder()
                                .setPattern(pattern.toString())).build();

                return transactionRes(response);
            }
        }

        private class Role {

            private Transaction.Res relations() {
                Stream<ai.grakn.concept.RelationshipType> concepts = concept.asRole().relationships();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRelations(ConceptProto.Role.Relations.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res players() {
                Stream<ai.grakn.concept.Type> concepts = concept.asRole().players();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setPlayers(ConceptProto.Role.Players.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }
        }

        private class Type {
            private Transaction.Res instances() {
                Stream<? extends ai.grakn.concept.Thing> concepts = concept.asType().instances();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setInstances(ConceptProto.Type.Instances.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res isAbstract() {
                Boolean isAbstract = concept.asType().isAbstract();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setIsAbstract(ConceptProto.Type.IsAbstract.Res.newBuilder()
                                .setAbstract(isAbstract)).build();

                return transactionRes(response);
            }

            private Transaction.Res isAbstract(boolean isAbstract) {
                concept.asType().isAbstract(isAbstract);
                return null;
            }

            private Transaction.Res keys() {
                Stream<ai.grakn.concept.AttributeType> concepts = concept.asType().keys();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setKeys(ConceptProto.Type.Keys.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res attributes() {
                Stream<ai.grakn.concept.AttributeType> concepts = concept.asType().attributes();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttributes(ConceptProto.Type.Attributes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res playing() {
                Stream<ai.grakn.concept.Role> concepts = concept.asType().playing();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setPlaying(ConceptProto.Type.Playing.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res key(ConceptProto.Concept protoKey) {
                ai.grakn.concept.AttributeType<?> attributeType = ConceptBuilder.concept(protoKey, tx).asAttributeType();
                concept.asType().key(attributeType);
                return null;
            }

            private Transaction.Res has(ConceptProto.Concept protoAttribute) {
                ai.grakn.concept.AttributeType<?> attributeType = ConceptBuilder.concept(protoAttribute, tx).asAttributeType();
                concept.asType().has(attributeType);
                return null;
            }

            private Transaction.Res plays(ConceptProto.Concept protoRole) {
                ai.grakn.concept.Role role = ConceptBuilder.concept(protoRole, tx).asRole();
                concept.asType().plays(role);
                return null;
            }

            private Transaction.Res unkey(ConceptProto.Concept protoKey) {
                ai.grakn.concept.AttributeType<?> attributeType = ConceptBuilder.concept(protoKey, tx).asAttributeType();
                concept.asType().unkey(attributeType);
                return null;
            }

            private Transaction.Res unhas(ConceptProto.Concept protoAttribute) {
                ai.grakn.concept.AttributeType<?> attributeType = ConceptBuilder.concept(protoAttribute, tx).asAttributeType();
                concept.asType().unhas(attributeType);
                return null;
            }

            private Transaction.Res unplay(ConceptProto.Concept protoRole) {
                ai.grakn.concept.Role role = ConceptBuilder.concept(protoRole, tx).asRole();
                concept.asType().unplay(role);
                return null;
            }
        }

        private class EntityType {

            private Transaction.Res create() {
                Entity entity = concept.asEntityType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setCreateEntity(ConceptProto.EntityType.Create.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(entity))).build();

                return transactionRes(response);
            }
        }

        private class RelationshipType {

            private Transaction.Res create() {
                ai.grakn.concept.Relationship relationship = concept.asRelationshipType().create();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setCreateRelation(ConceptProto.RelationType.Create.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(relationship))).build();

                return transactionRes(response);
            }

            private Transaction.Res roles() {
                Stream<ai.grakn.concept.Role> roles = concept.asRelationshipType().roles();

                Stream<SessionProto.Transaction.Res> responses = roles.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRoles(ConceptProto.RelationType.Roles.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res relates(ConceptProto.Concept protoRole) {
                ai.grakn.concept.Role role = ConceptBuilder.concept(protoRole, tx).asRole();
                concept.asRelationshipType().relates(role);
                return null;
            }

            private Transaction.Res unrelate(ConceptProto.Concept protoRole) {
                ai.grakn.concept.Role role = ConceptBuilder.concept(protoRole, tx).asRole();
                concept.asRelationshipType().unrelate(role);
                return null;
            }
        }

        private class AttributeType {

            private Transaction.Res create(ConceptProto.ValueObject protoValue) {
                Object value = protoValue.getAllFields().values().iterator().next();
                ai.grakn.concept.Attribute<?> attribute = concept.asAttributeType().create(value);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setCreateAttribute(ConceptProto.AttributeType.Create.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(attribute))).build();

                return transactionRes(response);
            }

            private Transaction.Res attribute(ConceptProto.ValueObject protoValue) {
                Object value = protoValue.getAllFields().values().iterator().next();
                ai.grakn.concept.Attribute<?> attribute = concept.asAttributeType().attribute(value);

                ConceptProto.AttributeType.Attribute.Res.Builder methodResponse = ConceptProto.AttributeType.Attribute.Res.newBuilder();
                if (attribute == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setConcept(ConceptBuilder.concept(attribute)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setAttribute(methodResponse).build();

                return transactionRes(response);
            }

            private Transaction.Res dataType() {
                ai.grakn.concept.AttributeType.DataType<?> dataType = concept.asAttributeType().dataType();

                ConceptProto.AttributeType.DataType.Res.Builder methodResponse =
                        ConceptProto.AttributeType.DataType.Res.newBuilder();

                if (dataType == null) methodResponse.setNull(ConceptProto.Null.getDefaultInstance()).build();
                else methodResponse.setDataType(ConceptBuilder.DATA_TYPE(dataType)).build();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setDataType(methodResponse).build();

                return transactionRes(response);
            }

            private Transaction.Res regex() {
                String regex = concept.asAttributeType().regex();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setGetRegex(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
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

        private class Thing {

            private Transaction.Res isInferred() {
                Boolean inferred = concept.asThing().isInferred();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingIsInferred(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                .setInferred(inferred)).build();

                return transactionRes(response);
            }

            private Transaction.Res type() {
                ai.grakn.concept.Concept type = concept.asThing().type();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingType(ConceptProto.Thing.Type.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(type))).build();

                return transactionRes(response);
            }

            private Transaction.Res keys(List<ConceptProto.Concept> protoTypes) {
                ai.grakn.concept.AttributeType<?>[] keyTypes = protoTypes.stream()
                        .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                        .toArray(ai.grakn.concept.AttributeType[]::new);
                Stream<ai.grakn.concept.Attribute<?>> concepts = concept.asThing().keys(keyTypes);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingKeys(ConceptProto.Thing.Keys.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res attributes(List<ConceptProto.Concept> protoTypes) {
                ai.grakn.concept.AttributeType<?>[] attributeTypes = protoTypes.stream()
                        .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                        .toArray(ai.grakn.concept.AttributeType[]::new);
                Stream<ai.grakn.concept.Attribute<?>> concepts = concept.asThing().attributes(attributeTypes);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingAttributes(ConceptProto.Thing.Attributes.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res relations(List<ConceptProto.Concept> protoRoles) {
                ai.grakn.concept.Role[] roles = protoRoles.stream()
                        .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                        .toArray(ai.grakn.concept.Role[]::new);
                Stream<ai.grakn.concept.Relationship> concepts = concept.asThing().relationships(roles);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRelations(ConceptProto.Thing.Relations.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res roles() {
                Stream<ai.grakn.concept.Role> concepts = concept.asThing().roles();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRoles(ConceptProto.Thing.Roles.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res relhas(ConceptProto.Concept protoAttribute) {
                ai.grakn.concept.Attribute<?> attribute = ConceptBuilder.concept(protoAttribute, tx).asAttribute();
                ai.grakn.concept.Relationship relationship = ConceptHolder.this.concept.asThing().relhas(attribute);

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setThingRelhas(ConceptProto.Thing.Relhas.Res.newBuilder()
                                .setConcept(ConceptBuilder.concept(relationship))).build();

                return transactionRes(response);
            }

            private Transaction.Res unhas(ConceptProto.Concept protoAttribute) {
                ai.grakn.concept.Attribute<?> attribute = ConceptBuilder.concept(protoAttribute, tx).asAttribute();
                concept.asThing().unhas(attribute);
                return null;
            }
        }

        private class Relationship {

            private Transaction.Res rolePlayersMap() {
                Map<ai.grakn.concept.Role, Set<ai.grakn.concept.Thing>> rolePlayers = concept.asRelationship().rolePlayersMap();

                Stream.Builder<SessionProto.Transaction.Res> responses = Stream.builder();
                rolePlayers.forEach(
                        (role, players) -> players.forEach(
                                player -> {
                                    System.out.print(role.toString() + " - " + player);
                                    responses.add(ResponseBuilder.Transaction.rolePlayer(role, player));
                                }
                        )
                );
                IteratorProto.IteratorId iteratorId = iterators.add(responses.build().iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRolePlayersMap(ConceptProto.Relation.RolePlayersMap.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res rolePlayers(List<ConceptProto.Concept> protoRoles) {
                ai.grakn.concept.Role[] roles = protoRoles.stream()
                        .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                        .toArray(ai.grakn.concept.Role[]::new);
                Stream<ai.grakn.concept.Thing> concepts = concept.asRelationship().rolePlayers(roles);

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setRolePlayers(ConceptProto.Relation.RolePlayers.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }

            private Transaction.Res assign(ConceptProto.Relation.RolePlayer protoRolePlayer) {
                ai.grakn.concept.Role role = ConceptBuilder.concept(protoRolePlayer.getRole(), tx).asRole();
                ai.grakn.concept.Thing player = ConceptBuilder.concept(protoRolePlayer.getPlayer(), tx).asThing();
                concept.asRelationship().assign(role, player);
                return null;
            }

            private Transaction.Res unassign(ConceptProto.Relation.RolePlayer protoRolePlayer) {
                ai.grakn.concept.Role role = ConceptBuilder.concept(protoRolePlayer.getRole(), tx).asRole();
                ai.grakn.concept.Thing player = ConceptBuilder.concept(protoRolePlayer.getPlayer(), tx).asThing();
                concept.asRelationship().unassign(role, player);
                return null;
            }
        }

        private class Attribute {

            private Transaction.Res value() {
                Object value = concept.asAttribute().value();

                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setValue(ConceptProto.Attribute.Value.Res.newBuilder()
                                .setValue(ConceptBuilder.attributeValue(value))).build();

                return transactionRes(response);
            }

            private Transaction.Res owners() {
                Stream<ai.grakn.concept.Thing> concepts = concept.asAttribute().owners();

                Stream<SessionProto.Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
                IteratorProto.IteratorId iteratorId = iterators.add(responses.iterator());
                ConceptProto.Method.Res response = ConceptProto.Method.Res.newBuilder()
                        .setOwners(ConceptProto.Attribute.Owners.Res.newBuilder()
                                .setIteratorId(iteratorId)).build();

                return transactionRes(response);
            }
        }
    }
}