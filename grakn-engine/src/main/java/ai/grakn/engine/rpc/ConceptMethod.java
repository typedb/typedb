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
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
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
                return when(concept);
            case THEN:
                return then(concept);

            // Role methods
            case RELATIONS:
                return relations(concept, iterators);
            case PLAYERS:
                return players(concept, iterators);

            // Type methods
            case INSTANCES:
                return instances(concept, iterators);
            case ISABSTRACT:
                return isAbstract(concept);
            case SETABSTRACT:
                return setAbstract(concept, req);
            case KEYS:
                return keys(concept, iterators);
            case ATTRIBUTES:
                return attributes(concept, iterators);
            case PLAYING:
                return playing(concept, iterators);
            case KEY:
                return key(concept, req, tx);
            case HAS:
                return has(concept, req, tx);
            case PLAYS:
                return plays(concept, req, tx);
            case UNKEY:
                return unkey(concept, req, tx);
            case UNHAS:
                return unhas(concept, req, tx);
            case UNPLAY:
                return unplay(concept, req, tx);

            // EntityType methods
            case CREATEENTITY:
                return createEntity(concept);

            // RelationshipType methods
            case CREATERELATION:
                return createRelation(concept);
            case ROLES:
                return roles(concept, iterators);
            case RELATES:
                return relates(concept, req, tx);
            case UNRELATE:
                return unrelate(concept, req, tx);

            // AttributeType methods
            case CREATEATTRIBUTE:
                return createAttribute(concept, req);
            case ATTRIBUTE:
                return attribute(concept, req);
            case DATATYPE:
                return dataType(concept);
            case GETREGEX:
                return getRegex(concept);
            case SETREGEX:
                return setRegex(concept, req);

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
    }

    // Rule methods

    private static Transaction.Res when(Concept concept) {
        return ResponseBuilder.Transaction.ConceptMethod.when(concept.asRule().when());
    }

    private static Transaction.Res then(Concept concept) {
        return ResponseBuilder.Transaction.ConceptMethod.then(concept.asRule().then());
    }


    // Role methods

    private static Transaction.Res relations(Concept concept, SessionService.Iterators iterators) {
        Stream<RelationshipType> concepts = concept.asRole().relationships();
        return ResponseBuilder.Transaction.ConceptMethod.relations(concepts, iterators);
    }

    private static Transaction.Res players(Concept concept, SessionService.Iterators iterators) {
        Stream<Type> concepts = concept.asRole().players();
        return ResponseBuilder.Transaction.ConceptMethod.players(concepts, iterators);
    }


    // Type methods

    private static Transaction.Res instances(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Thing> concepts = concept.asType().instances();
        return ResponseBuilder.Transaction.ConceptMethod.instances(concepts, iterators);
    }

    private static Transaction.Res isAbstract(Concept concept) {
        Boolean response = concept.asType().isAbstract();
        return ResponseBuilder.Transaction.ConceptMethod.isAbstract(response);
    }

    private static Transaction.Res setAbstract(Concept concept, ConceptProto.Method.Req method) {
        concept.asType().isAbstract(method.getSetAbstract().getAbstract());
        return null;
    }

    private static Transaction.Res keys(Concept concept, SessionService.Iterators iterators) {
        Stream<AttributeType> concepts = concept.asType().keys();
        return ResponseBuilder.Transaction.ConceptMethod.keys(concepts, iterators);
    }

    private static Transaction.Res attributes(Concept concept, SessionService.Iterators iterators) {
        Stream<AttributeType> concepts = concept.asType().attributes();
        return ResponseBuilder.Transaction.ConceptMethod.attributes(concepts, iterators);
    }

    private static Transaction.Res playing(Concept concept, SessionService.Iterators iterators) {
        Stream<Role> concepts = concept.asType().playing();
        return ResponseBuilder.Transaction.ConceptMethod.playing(concepts, iterators);
    }

    private static Transaction.Res has(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getHas().getConcept(), tx).asAttributeType();
        concept.asType().has(attributeType);
        return null;
    }

    private static Transaction.Res key(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getKey().getConcept(), tx).asAttributeType();
        concept.asType().key(attributeType);
        return null;
    }

    private static Transaction.Res plays(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getPlays().getConcept(), tx).asRole();
        concept.asType().plays(role);
        return null;
    }

    private static Transaction.Res unkey(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getUnkey().getConcept(), tx).asAttributeType();
        concept.asType().unkey(attributeType);
        return null;
    }

    private static Transaction.Res unhas(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getUnhas().getConcept(), tx).asAttributeType();
        concept.asType().unhas(attributeType);
        return null;
    }

    private static Transaction.Res unplay(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getUnplay().getConcept(), tx).asRole();
        concept.asType().unplay(role);
        return null;
    }


    // EntityType methods

    private static Transaction.Res createEntity(Concept concept) {
        Entity entity = concept.asEntityType().create();
        return ResponseBuilder.Transaction.ConceptMethod.createEntity(entity);
    }


    // RelationshipType methods

    private static Transaction.Res createRelation(Concept concept) {
        Relationship relationship = concept.asRelationshipType().create();
        return ResponseBuilder.Transaction.ConceptMethod.createRelation(relationship);
    }

    private static Transaction.Res roles(Concept concept, SessionService.Iterators iterators) {
        Stream<Role> roles = concept.asRelationshipType().roles();
        return ResponseBuilder.Transaction.ConceptMethod.roles(roles, iterators);
    }

    private static Transaction.Res relates(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getRelates().getConcept(), tx).asRole();
        concept.asRelationshipType().relates(role);
        return null;
    }

    private static Transaction.Res unrelate(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getUnrelate().getConcept(), tx).asRole();
        concept.asRelationshipType().unrelate(role);
        return null;
    }


    // AttributeType methods

    private static Transaction.Res createAttribute(Concept concept, ConceptProto.Method.Req method) {
        Object value = method.getCreateAttribute().getValue().getAllFields().values().iterator().next();
        Attribute<?> attribute = concept.asAttributeType().create(value);
        return ResponseBuilder.Transaction.ConceptMethod.createAttribute(attribute);
    }

    private static Transaction.Res attribute(Concept concept, ConceptProto.Method.Req method) {
        Object value = method.getAttribute().getValue().getAllFields().values().iterator().next();
        Attribute<?> attribute = concept.asAttributeType().attribute(value);
        return ResponseBuilder.Transaction.ConceptMethod.attribute(attribute);
    }

    private static Transaction.Res dataType(Concept concept) {
        AttributeType.DataType<?> dataType = concept.asAttributeType().dataType();
        return ResponseBuilder.Transaction.ConceptMethod.dataType(dataType);
    }

    private static Transaction.Res getRegex(Concept concept) {
        String regex = concept.asAttributeType().regex();
        return ResponseBuilder.Transaction.ConceptMethod.getRegex(regex);
    }

    private static Transaction.Res setRegex(Concept concept, ConceptProto.Method.Req method) {
        String regex = method.getSetRegex().getRegex();
        if (regex.isEmpty()) {
            concept.asAttributeType().regex(null);
        } else {
            concept.asAttributeType().regex(regex);
        }
        return null;
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