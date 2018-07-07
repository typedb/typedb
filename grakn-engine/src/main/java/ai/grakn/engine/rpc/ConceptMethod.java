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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
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
                return isInferred(concept);
            case THING_TYPE:
                return type(concept);
            case THING_KEYS:
                return keys(concept, iterators, req, tx);
            case THING_ATTRIBUTES:
                return attributes(concept, req, iterators, tx);
            case THING_RELATIONS:
                return relations(concept, iterators, req, tx);
            case THING_ROLES:
                return getRolesPlayedByThing(concept, iterators);
            case THING_RELHAS:
                return relhas(concept, req, tx);
            case THING_UNHAS:
                return unsetAttributeRelationship(concept, req, tx);

            // Relationship methods
            case ROLEPLAYERSMAP:
                return rolePlayersMap(concept, iterators);
            case ROLEPLAYERS:
                return rolePlayers(concept, iterators, req, tx);
            case ASSIGN:
                return assign(concept, req, tx);
            case UNASSIGN:
                return unassign(concept, req, tx);

            // Attribute Methods
            case VALUE:
                return value(concept);
            case OWNERS:
                return owners(concept, iterators);

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

        }

        private class Relationship {

        }

        private class Attribute {

        }
    }


    


    // Thing methods

    private static Transaction.Res isInferred(Concept concept) {
        Boolean response = concept.asThing().isInferred();
        return ResponseBuilder.Transaction.ConceptMethod.isInferred(response);
    }

    private static Transaction.Res type(Concept concept) {
        Concept type = concept.asThing().type();
        return ResponseBuilder.Transaction.ConceptMethod.type(type);
    }

    private static Transaction.Res keys(Concept concept, SessionService.Iterators iterators,
                                        ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        List<ConceptProto.Concept> rpcKeyTypes = method.getThingKeys().getConceptsList();
        AttributeType<?>[] keyTypes = rpcKeyTypes.stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(AttributeType[]::new);

        Stream<Attribute<?>> concepts = concept.asThing().keys(keyTypes);
        return ResponseBuilder.Transaction.ConceptMethod.getKeysByTypes(concepts, iterators);
    }

    private static Transaction.Res attributes(Concept concept, ConceptProto.Method.Req method,
                                              SessionService.Iterators iterators, EmbeddedGraknTx tx) {
        List<ConceptProto.Concept> rpcAttributeTypes = method.getThingAttributes().getConceptsList();
        AttributeType<?>[] attributeTypes = rpcAttributeTypes.stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(AttributeType[]::new);

        Stream<Attribute<?>> concepts = concept.asThing().attributes(attributeTypes);
        return ResponseBuilder.Transaction.ConceptMethod.getAttributesByTypes(concepts, iterators);
    }

    private static Transaction.Res relations(Concept concept, SessionService.Iterators iterators,
                                             ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        List<ConceptProto.Concept> rpcRoles = method.getThingRelations().getConceptsList();
        Role[] roles = rpcRoles.stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(Role[]::new);
        Stream<Relationship> concepts = concept.asThing().relationships(roles);
        return ResponseBuilder.Transaction.ConceptMethod.getRelationshipsByRoles(concepts, iterators);
    }

    private static Transaction.Res getRolesPlayedByThing(Concept concept, SessionService.Iterators iterators) {
        Stream<Role> concepts = concept.asThing().roles();
        return ResponseBuilder.Transaction.ConceptMethod.getRolesPlayedByThing(concepts, iterators);
    }

    private static Transaction.Res relhas(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Attribute<?> attribute = ConceptBuilder.concept(method.getThingRelhas().getConcept(), tx).asAttribute();
        Relationship relationship = concept.asThing().relhas(attribute);
        return ResponseBuilder.Transaction.ConceptMethod.relhas(relationship);
    }

    private static Transaction.Res unsetAttributeRelationship(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Attribute<?> attribute = ConceptBuilder.concept(method.getThingUnhas().getConcept(), tx).asAttribute();
        concept.asThing().unhas(attribute);
        return null;
    }


    // Relationship methods

    private static Transaction.Res rolePlayersMap(Concept concept, SessionService.Iterators iterators) {
        Map<Role, Set<Thing>> rolePlayers = concept.asRelationship().rolePlayersMap();
        return ResponseBuilder.Transaction.ConceptMethod.rolePlayersMap(rolePlayers, iterators);
    }

    private static Transaction.Res rolePlayers(Concept concept, SessionService.Iterators iterators,
                                               ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        List<ConceptProto.Concept> rpcRoles = method.getRolePlayers().getConceptsList();
        Role[] roles = rpcRoles.stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(Role[]::new);
        Stream<Thing> concepts = concept.asRelationship().rolePlayers(roles);
        return ResponseBuilder.Transaction.ConceptMethod.getRolePlayersByRoles(concepts, iterators);
    }

    private static Transaction.Res assign(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getAssign().getRolePlayer().getRole(), tx).asRole();
        Thing player = ConceptBuilder.concept(method.getAssign().getRolePlayer().getPlayer(), tx).asThing();
        concept.asRelationship().assign(role, player);
        return null;
    }

    private static Transaction.Res unassign(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getUnassign().getRolePlayer().getRole(), tx).asRole();
        Thing player = ConceptBuilder.concept(method.getUnassign().getRolePlayer().getPlayer(), tx).asThing();
        concept.asRelationship().unassign(role, player);
        return null;
    }


    // Attribute methods

    private static Transaction.Res value(Concept concept) {
        Object value = concept.asAttribute().value();
        return ResponseBuilder.Transaction.ConceptMethod.value(value);
    }

    private static Transaction.Res owners(Concept concept, SessionService.Iterators iterators) {
        Stream<Thing> concepts = concept.asAttribute().owners();
        return ResponseBuilder.Transaction.ConceptMethod.owners(concepts, iterators);
    }
}