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
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.ConceptProto;
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
public abstract class ConceptMethod {

    public static Transaction.Res run(Concept concept, ConceptProto.Method.Req method,
                                 SessionService.Iterators iterators, EmbeddedGraknTx tx) {
        switch (method.getReqCase()) {
            // Concept methods
            case DELETE:
                return delete(concept);

            // SchemaConcept methods
            case ISIMPLICIT:
                return isImplicit(concept);
            case GETLABEL:
                return getLabel(concept);
            case SETLABEL:
                return setLabel(concept, method);
            case GETSUP:
                return getSup(concept);
            case SETSUP:
                return setSup(concept, method, tx);
            case SUPS:
                return sups(concept, iterators);
            case SUBS:
                return subs(concept, iterators);

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
            case ISABSTRACT:
                return isAbstract(concept);
            case SETABSTRACT:
                return setAbstract(concept, method);
            case GETINSTANCES:
                return getInstances(concept, iterators);
            case GETATTRIBUTETYPES:
                return getAttributeTypes(concept, iterators);
            case GETKEYTYPES:
                return getKeyTypes(concept, iterators);
            case GETROLESPLAYEDBYTYPE:
                return getRolesPlayedByType(concept, iterators);
            case SETATTRIBUTETYPE:
                return setAttributeType(concept, method, tx);
            case UNSETATTRIBUTETYPE:
                return unsetAttributeType(concept, method, tx);
            case SETKEYTYPE:
                return setKeyType(concept, method, tx);
            case UNSETKEYTYPE:
                return unsetKeyType(concept, method, tx);
            case SETROLEPLAYEDBYTYPE:
                return setRolePlayedByType(concept, method, tx);
            case UNSETROLEPLAYEDBYTYPE:
                return unsetRolePlayedByType(concept, method, tx);

            // EntityType methods
            case CREATEENTITY:
                return createEntity(concept);

            // RelationshipType methods
            case CREATERELATION:
                return createRelation(concept);
            case ROLES:
                return roles(concept, iterators);
            case RELATES:
                return relates(concept, method, tx);
            case UNRELATE:
                return unrelate(concept, method, tx);

            // AttributeType methods
            case CREATEATTRIBUTE:
                return createAttribute(concept, method);
            case ATTRIBUTE:
                return attribute(concept, method);
            case DATATYPE:
                return dataType(concept);
            case GETREGEX:
                return getRegex(concept);
            case SETREGEX:
                return setRegex(concept, method);

            // Thing methods
            case ISINFERRED:
                return isInferred(concept);
            case GETDIRECTTYPE:
                return getDirectType(concept);
            case GETKEYS:
                return getKeys(concept, iterators);
            case GETKEYSBYTYPES:
                return getKeysByTypes(concept, iterators, method, tx);
            case GETATTRIBUTESFORANYTYPE:
                return getAttributesForAnyType(concept, iterators);
            case GETATTRIBUTESBYTYPES:
                return getAttributesByTypes(concept, method, iterators, tx);
            case GETRELATIONSHIPS:
                return getRelationships(concept, iterators);
            case GETRELATIONSHIPSBYROLES:
                return getRelationshipsByRoles(concept, iterators, method, tx);
            case GETROLESPLAYEDBYTHING:
                return getRolesPlayedByThing(concept, iterators);
            case SETATTRIBUTERELATIONSHIP:
                return setAttributeRelationship(concept, method, tx);
            case UNSETATTRIBUTERELATIONSHIP:
                return unsetAttributeRelationship(concept, method, tx);

            // Relationship methods
            case ROLEPLAYERSMAP:
                return rolePlayersMap(concept, iterators);
            case ROLEPLAYERS:
                return rolePlayers(concept, iterators, method, tx);
            case ASSIGN:
                return assign(concept, method, tx);
            case UNASSIGN:
                return unassign(concept, method, tx);

            // Attribute Methods
            case VALUE:
                return value(concept);
            case OWNERS:
                return owners(concept, iterators);

            default:
            case REQ_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + method);
        }
    }

    // Concept methods

    private static Transaction.Res delete(Concept concept) {
        concept.delete();
        return null;
    }


    // SchemaConcept methods

    private static Transaction.Res isImplicit(Concept concept) {
        Boolean response = concept.asSchemaConcept().isImplicit();
        return ResponseBuilder.Transaction.ConceptMethod.isImplicit(response);
    }

    private static Transaction.Res getLabel(Concept concept) {
        Label label = concept.asSchemaConcept().label();
        return ResponseBuilder.Transaction.ConceptMethod.getLabel(label.getValue());
    }

    private static Transaction.Res setLabel(Concept concept, ConceptProto.Method.Req method) {
        concept.asSchemaConcept().label(Label.of(method.getSetLabel().getLabel()));
        return null;
    }

    private static Transaction.Res getSup(Concept concept) {
        Concept superConcept = concept.asSchemaConcept().sup();
        return ResponseBuilder.Transaction.ConceptMethod.getSup(superConcept);
    }

    private static Transaction.Res setSup(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        // Make the second argument the super of the first argument
        // @throws GraqlQueryException if the types are different, or setting the super to be a meta-type

        ConceptProto.Concept setDirectSuperConcept = method.getSetSup().getConcept();
        SchemaConcept superConcept = ConceptBuilder.concept(setDirectSuperConcept, tx).asSchemaConcept();
        SchemaConcept subConcept = concept.asSchemaConcept();

        if (superConcept.isEntityType()) {
            subConcept.asEntityType().sup(superConcept.asEntityType());
        } else if (superConcept.isRelationshipType()) {
            subConcept.asRelationshipType().sup(superConcept.asRelationshipType());
        } else if (superConcept.isRole()) {
            subConcept.asRole().sup(superConcept.asRole());
        } else if (superConcept.isAttributeType()) {
            subConcept.asAttributeType().sup(superConcept.asAttributeType());
        } else if (superConcept.isRule()) {
            subConcept.asRule().sup(superConcept.asRule());
        } else {
            throw GraqlQueryException.insertMetaType(subConcept.label(), superConcept);
        }

        return null;
    }

    private static Transaction.Res sups(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends SchemaConcept> concepts = concept.asSchemaConcept().sups();
        return ResponseBuilder.Transaction.ConceptMethod.sups(concepts, iterators);
    }

    private static Transaction.Res subs(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends SchemaConcept> concepts = concept.asSchemaConcept().subs();
        return ResponseBuilder.Transaction.ConceptMethod.subs(concepts, iterators);
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

    private static Transaction.Res isAbstract(Concept concept) {
        Boolean response = concept.asType().isAbstract();
        return ResponseBuilder.Transaction.ConceptMethod.isAbstract(response);
    }

    private static Transaction.Res setAbstract(Concept concept, ConceptProto.Method.Req method) {
        concept.asType().isAbstract(method.getSetAbstract().getAbstract());
        return null;
        //return ResponseBuilder.Transaction.ConceptMethod.isAbstract();
    }

    private static Transaction.Res getInstances(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Thing> concepts = concept.asType().instances();
        return ResponseBuilder.Transaction.ConceptMethod.getInstances(concepts, iterators);
    }

    private static Transaction.Res getAttributeTypes(Concept concept, SessionService.Iterators iterators) {
        Stream<AttributeType> concepts = concept.asType().attributes();
        return ResponseBuilder.Transaction.ConceptMethod.getAttributeTypes(concepts, iterators);
    }

    private static Transaction.Res getKeyTypes(Concept concept, SessionService.Iterators iterators) {
        Stream<AttributeType> concepts = concept.asType().keys();
        return ResponseBuilder.Transaction.ConceptMethod.getKeyTypes(concepts, iterators);
    }

    private static Transaction.Res getRolesPlayedByType(Concept concept, SessionService.Iterators iterators) {
        Stream<Role> concepts = concept.asType().playing();
        return ResponseBuilder.Transaction.ConceptMethod.getRolesPlayedByType(concepts, iterators);
    }

    private static Transaction.Res setAttributeType(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getSetAttributeType().getConcept(), tx).asAttributeType();
        concept.asType().has(attributeType);
        return null;
    }

    private static Transaction.Res unsetAttributeType(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getUnsetAttributeType().getConcept(), tx).asAttributeType();
        concept.asType().unhas(attributeType);
        return null;
    }

    private static Transaction.Res setKeyType(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getSetKeyType().getConcept(), tx).asAttributeType();
        concept.asType().key(attributeType);
        return null;
    }

    private static Transaction.Res unsetKeyType(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getUnsetKeyType().getConcept(), tx).asAttributeType();
        concept.asType().unkey(attributeType);
        return null;
    }

    private static Transaction.Res setRolePlayedByType(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getSetRolePlayedByType().getConcept(), tx).asRole();
        concept.asType().plays(role);
        return null;
    }

    private static Transaction.Res unsetRolePlayedByType(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getUnsetRolePlayedByType().getConcept(), tx).asRole();
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

    private static Transaction.Res getDirectType(Concept concept) {
        Concept type = concept.asThing().type();
        return ResponseBuilder.Transaction.ConceptMethod.getDirectType(type);
    }

    private static Transaction.Res getKeys(Concept concept, SessionService.Iterators iterators) {
        Stream<Attribute<?>> concepts = concept.asThing().keys();
        return ResponseBuilder.Transaction.ConceptMethod.getKeys(concepts, iterators);
    }

    private static Transaction.Res getKeysByTypes(Concept concept, SessionService.Iterators iterators,
                                                  ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        List<ConceptProto.Concept> rpcKeyTypes = method.getGetKeysByTypes().getConceptsList();
        AttributeType<?>[] keyTypes = rpcKeyTypes.stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(AttributeType[]::new);

        Stream<Attribute<?>> concepts = concept.asThing().keys(keyTypes);
        return ResponseBuilder.Transaction.ConceptMethod.getKeysByTypes(concepts, iterators);
    }

    private static Transaction.Res getAttributesForAnyType(Concept concept, SessionService.Iterators iterators) {
        Stream<Attribute<?>> concepts = concept.asThing().attributes();
        return ResponseBuilder.Transaction.ConceptMethod.getAttributesForAnyType(concepts, iterators);
    }

    private static Transaction.Res getAttributesByTypes(Concept concept, ConceptProto.Method.Req method,
                                                        SessionService.Iterators iterators, EmbeddedGraknTx tx) {
        List<ConceptProto.Concept> rpcAttributeTypes = method.getGetAttributesByTypes().getConceptsList();
        AttributeType<?>[] attributeTypes = rpcAttributeTypes.stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(AttributeType[]::new);

        Stream<Attribute<?>> concepts = concept.asThing().attributes(attributeTypes);
        return ResponseBuilder.Transaction.ConceptMethod.getAttributesByTypes(concepts, iterators);
    }

    private static Transaction.Res getRelationships(Concept concept, SessionService.Iterators iterators) {
        Stream<Relationship> concepts = concept.asThing().relationships();
        return ResponseBuilder.Transaction.ConceptMethod.getRelationships(concepts, iterators);
    }

    private static Transaction.Res getRelationshipsByRoles(Concept concept, SessionService.Iterators iterators,
                                                           ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        List<ConceptProto.Concept> rpcRoles = method.getGetRelationshipsByRoles().getConceptsList();
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

    private static Transaction.Res setAttributeRelationship(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Attribute<?> attribute = ConceptBuilder.concept(method.getSetAttributeRelationship().getConcept(), tx).asAttribute();
        Relationship relationship = concept.asThing().relhas(attribute);
        return ResponseBuilder.Transaction.ConceptMethod.setAttributeRelationship(relationship);
    }

    private static Transaction.Res unsetAttributeRelationship(Concept concept, ConceptProto.Method.Req method, EmbeddedGraknTx tx) {
        Attribute<?> attribute = ConceptBuilder.concept(method.getUnsetAttributeRelationship().getConcept(), tx).asAttribute();
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