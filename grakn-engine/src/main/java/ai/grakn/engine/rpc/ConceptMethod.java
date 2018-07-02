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
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.ConceptMethodProto;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.SessionProto.Transaction;

import java.util.stream.Stream;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message {@link ai.grakn.rpc.proto.ConceptMethodProto.ConceptMethod.Req}.
 */
public abstract class ConceptMethod {

    public static Transaction.Res run(Concept concept, ConceptMethodProto.ConceptMethod.Req method,
                                 SessionService.Iterators iterators, EmbeddedGraknTx tx) {
        switch (method.getReqCase()) {
            case GETVALUE:
                return getValue(concept);
            case GETDATATYPEOFATTRIBUTETYPE:
                return getDataTypeOfAttributeType(concept);
            case GETDATATYPEOFATTRIBUTE:
                return getDataTypeOfAttribute(concept);
            case GETLABEL:
                return getLabel(concept);
            case SETLABEL:
                return setLabel(concept, method);
            case ISIMPLICIT:
                return isImplicit(concept);
            case ISINFERRED:
                return isInferred(concept);
            case ISABSTRACT:
                return isAbstract(concept);
            case SETABSTRACT:
                return setAbstract(concept, method);
            case GETWHEN:
                return getWhen(concept);
            case GETTHEN:
                return getThen(concept);
            case GETREGEX:
                return getRegex(concept);
            case GETROLEPLAYERS:
                return getRolePlayers(concept, iterators);
            case GETROLEPLAYERSBYROLES:
                return getRolePlayersByRoles(concept, iterators, method, tx);
            case GETATTRIBUTETYPES:
                return getAttributeTypes(concept, iterators);
            case SETATTRIBUTETYPE:
                return setAttributeType(concept, method, tx);
            case UNSETATTRIBUTETYPE:
                return unsetAttributeType(concept, method, tx);
            case GETKEYTYPES:
                return getKeyTypes(concept, iterators);
            case GETDIRECTTYPE:
                return getDirectType(concept);
            case GETDIRECTSUPERCONCEPT:
                return getDirectSuper(concept);
            case SETDIRECTSUPERCONCEPT:
                return setDirectSuper(concept, method, tx);
            case UNSETROLEPLAYER:
                return removeRolePlayer(concept, method, tx);
            case DELETE:
                return delete(concept);
            case GETOWNERS:
                return getOwners(concept, iterators);
            case GETTYPESTHATPLAYROLE:
                return getTypesThatPlayRole(concept, iterators);
            case GETROLESPLAYEDBYTYPE:
                return getRolesPlayedByType(concept, iterators);
            case GETINSTANCES:
                return getInstances(concept, iterators);
            case GETRELATEDROLES:
                return getRelatedRoles(concept, iterators);
            case GETATTRIBUTES:
                return getAttributes(concept, iterators);
            case GETSUPERCONCEPTS:
                return getSuperConcepts(concept, iterators);
            case GETSUBCONCEPTS:
                return getSubConcepts(concept, iterators);
            case GETRELATIONSHIPTYPESTHATRELATEROLE:
                return getRelationshipTypesThatRelateRole(concept, iterators);
            case GETATTRIBUTESBYTYPES:
                return getAttributesByTypes(concept, method, iterators, tx);
            case GETRELATIONSHIPS:
                return getRelationships(concept, iterators);
            case GETRELATIONSHIPSBYROLES:
                return getRelationshipsByRoles(concept, iterators, method, tx);
            case GETROLESPLAYEDBYTHING:
                return getRolesPlayedByThing(concept, iterators);
            case GETKEYS:
                return getKeys(concept, iterators);
            case GETKEYSBYTYPES:
                return getKeysByTypes(concept, iterators, method, tx);
            case SETKEYTYPE:
                return setKeyType(concept, method, tx);
            case UNSETKEYTYPE:
                return unsetKeyType(concept, method, tx);
            case SETROLEPLAYEDBYTYPE:
                return setRolePlayedByType(concept, method, tx);
            case UNSETROLEPLAYEDBYTYPE:
                return unsetRolePlayedByType(concept, method, tx);
            case ADDENTITY:
                return addEntity(concept);
            case ADDRELATIONSHIP:
                return addRelationship(concept);
            case GETATTRIBUTE:
                return getAttribute(concept, method);
            case PUTATTRIBUTE:
                return putAttribute(concept, method);
            case SETATTRIBUTE:
                return setAttribute(concept, method, tx);
            case UNSETATTRIBUTE:
                return unsetAttribute(concept, method, tx);
            case SETREGEX:
                return setRegex(concept, method);
            case SETROLEPLAYER:
                return setRolePlayer(concept, method, tx);
            case SETRELATEDROLE:
                return setRelatedRole(concept, method, tx);
            case UNSETRELATEDROLE:
                return unsetRelatedRole(concept, method, tx);
            default:
            case REQ_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + method);
        }
    }

    private static Transaction.Res getValue(Concept concept) {
        Object value = concept.asAttribute().getValue();
        return ResponseBuilder.Transaction.conceptResponseWithAttributeValue(value);
    }

    private static Transaction.Res getDataTypeOfAttributeType(Concept concept) {
        AttributeType.DataType<?> dataType = concept.asAttributeType().getDataType();
        if (dataType == null) return ResponseBuilder.Transaction.conceptResponseWithNoResult();
        return ResponseBuilder.Transaction.conceptResponseWithDataType(dataType);
    }

    private static Transaction.Res getDataTypeOfAttribute(Concept concept) {
        AttributeType.DataType<?> dataType = concept.asAttribute().dataType();
        return ResponseBuilder.Transaction.conceptResponseWithDataType(dataType);
    }

    private static Transaction.Res getLabel(Concept concept) {
        Label label = concept.asSchemaConcept().getLabel();
        ConceptMethodProto.ConceptMethod.Res.Builder conceptResponse = ConceptMethodProto.ConceptMethod.Res.newBuilder();
        conceptResponse.setLabel(label.getValue());
        return Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static Transaction.Res setLabel(Concept concept, ConceptMethodProto.ConceptMethod.Req method) {
        concept.asSchemaConcept().setLabel(Label.of(method.getSetLabel()));
        return null;
    }

    private static Transaction.Res isImplicit(Concept concept) {
        Boolean response = concept.asSchemaConcept().isImplicit();
        ConceptMethodProto.ConceptMethod.Res.Builder conceptResponse = ConceptMethodProto.ConceptMethod.Res.newBuilder()
                .setIsImplicit(response);
        return Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static Transaction.Res isInferred(Concept concept) {
        Boolean response = concept.asThing().isInferred();
        ConceptMethodProto.ConceptMethod.Res.Builder conceptResponse = ConceptMethodProto.ConceptMethod.Res.newBuilder()
                .setIsInferred(response);
        return Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static Transaction.Res isAbstract(Concept concept) {
        Boolean response = concept.asType().isAbstract();
        ConceptMethodProto.ConceptMethod.Res.Builder conceptResponse = ConceptMethodProto.ConceptMethod.Res.newBuilder()
                .setIsAbstract(response);
        return Transaction.Res.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static Transaction.Res setAbstract(Concept concept, ConceptMethodProto.ConceptMethod.Req method) {
        concept.asType().setAbstract(method.getSetAbstract());
        return null;
    }
    
    private static Transaction.Res getWhen(Concept concept) {
        return ResponseBuilder.Transaction.conceptResponseWithPattern(concept.asRule().getWhen());
    }
    
    private static Transaction.Res getThen(Concept concept) {
        return ResponseBuilder.Transaction.conceptResponseWithPattern(concept.asRule().getThen());
    }

    private static Transaction.Res getRegex(Concept concept) {
        String regex = concept.asAttributeType().getRegex();
        if (regex == null) return ResponseBuilder.Transaction.conceptResponseWithNoResult();
        return ResponseBuilder.Transaction.conceptResponseWithRegex(regex);
    }

    private static Transaction.Res getRolePlayers(Concept concept, SessionService.Iterators iterators) {
        Stream.Builder<Transaction.Res> rolePlayers = Stream.builder();
        concept.asRelationship().allRolePlayers().forEach(
                (role, players) -> players.forEach(
                        player -> rolePlayers.add(ResponseBuilder.Transaction.rolePlayer(role, player))
                )
        );
        return ResponseBuilder.Transaction.iteratorId(rolePlayers.build(), iterators);
    }

    private static Transaction.Res getRolePlayersByRoles(Concept concept, SessionService.Iterators iterators,
                                                    ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        ConceptProto.Concepts rpcRoles = method.getGetRolePlayersByRoles();
        Role[] roles = rpcRoles.getConceptsList().stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(Role[]::new);
        Stream<? extends Concept> concepts = concept.asRelationship().rolePlayers(roles);
        Stream<Transaction.Res> responses = concepts.map(concept1 -> ResponseBuilder.Transaction.concept(concept1));
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getAttributeTypes(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().attributes();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res setAttributeType(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getSetAttributeType(), tx).asAttributeType();
        concept.asType().attribute(attributeType);
        return null;
    }

    private static Transaction.Res unsetAttributeType(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getUnsetAttributeType(), tx).asAttributeType();
        concept.asType().deleteAttribute(attributeType);
        return null;
    }

    private static Transaction.Res getKeyTypes(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().keys();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getDirectType(Concept concept) {
        Concept type = concept.asThing().type();
        return ResponseBuilder.Transaction.conceptResopnseWithConcept(type);
    }

    private static Transaction.Res getDirectSuper(Concept concept) {
        Concept superConcept = concept.asSchemaConcept().sup();
        if (superConcept == null) return ResponseBuilder.Transaction.conceptResponseWithNoResult();
        return ResponseBuilder.Transaction.conceptResopnseWithConcept(superConcept);
    }

    private static Transaction.Res setDirectSuper(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        // Make the second argument the super of the first argument
        // @throws GraqlQueryException if the types are different, or setting the super to be a meta-type

        ConceptProto.Concept setDirectSuperConcept = method.getSetDirectSuperConcept();
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
            throw GraqlQueryException.insertMetaType(subConcept.getLabel(), superConcept);
        }

        return null;
    }

    private static Transaction.Res removeRolePlayer(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getUnsetRolePlayer().getRole(), tx).asRole();
        Thing player = ConceptBuilder.concept(method.getUnsetRolePlayer().getPlayer(), tx).asThing();
        concept.asRelationship().removeRolePlayer(role, player);
        return null;
    }

    private static Transaction.Res delete(Concept concept) {
        concept.delete();
        return null;
    }

    private static Transaction.Res getOwners(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asAttribute().ownerInstances();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getTypesThatPlayRole(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asRole().playedByTypes();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getRolesPlayedByType(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().plays();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getInstances(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().instances();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getRelatedRoles(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asRelationshipType().relates();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getAttributes(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().attributes();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getSuperConcepts(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asSchemaConcept().sups();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getSubConcepts(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asSchemaConcept().subs();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getRelationshipTypesThatRelateRole(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asRole().relationshipTypes();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getAttributesByTypes(Concept concept, ConceptMethodProto.ConceptMethod.Req method,
                                                   SessionService.Iterators iterators, EmbeddedGraknTx tx) {
        ConceptProto.Concepts rpcAttributeTypes = method.getGetAttributesByTypes();
        AttributeType<?>[] attributeTypes = rpcAttributeTypes.getConceptsList().stream()
                        .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                        .toArray(AttributeType[]::new);

        Stream<? extends Concept> concepts = concept.asThing().attributes(attributeTypes);
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getRelationships(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().relationships();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getRelationshipsByRoles(Concept concept, SessionService.Iterators iterators,
                                                      ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        ConceptProto.Concepts rpcRoles = method.getGetRelationshipsByRoles();
        Role[] roles = rpcRoles.getConceptsList().stream()
                .map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(Role[]::new);
        Stream<? extends Concept> concepts = concept.asThing().relationships(roles);
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getRolesPlayedByThing(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().plays();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getKeys(Concept concept, SessionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().keys();
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res getKeysByTypes(Concept concept, SessionService.Iterators iterators,
                                             ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        ConceptProto.Concepts rpcKeyTypes = method.getGetKeysByTypes();
        AttributeType<?>[] keyTypes = rpcKeyTypes.getConceptsList()
                .stream().map(rpcConcept -> ConceptBuilder.concept(rpcConcept, tx))
                .toArray(AttributeType[]::new);

        Stream<? extends Concept> concepts = concept.asThing().keys(keyTypes);
        Stream<Transaction.Res> responses = concepts.map(ResponseBuilder.Transaction::concept);
        return ResponseBuilder.Transaction.iteratorId(responses, iterators);
    }

    private static Transaction.Res setKeyType(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getSetKeyType(), tx).asAttributeType();
        concept.asType().key(attributeType);
        return null;
    }

    private static Transaction.Res unsetKeyType(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        AttributeType<?> attributeType = ConceptBuilder.concept(method.getUnsetKeyType(), tx).asAttributeType();
        concept.asType().deleteKey(attributeType);
        return null;
    }

    private static Transaction.Res setRolePlayedByType(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getSetRolePlayedByType(), tx).asRole();
        concept.asType().plays(role);
        return null;
    }

    private static Transaction.Res unsetRolePlayedByType(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getUnsetRolePlayedByType(), tx).asRole();
        concept.asType().deletePlays(role);
        return null;
    }

    private static Transaction.Res addEntity(Concept concept) {
        Concept entity = concept.asEntityType().addEntity();
        return ResponseBuilder.Transaction.conceptResopnseWithConcept(entity);
    }

    private static Transaction.Res addRelationship(Concept concept) {
        Concept relationship = concept.asRelationshipType().addRelationship();
        return ResponseBuilder.Transaction.conceptResopnseWithConcept(relationship);
    }

    private static Transaction.Res getAttribute(Concept concept, ConceptMethodProto.ConceptMethod.Req method) {
        Object value = method.getGetAttribute().getAllFields().values().iterator().next();
        Concept attribute = concept.asAttributeType().getAttribute(value);
        if (attribute == null) return ResponseBuilder.Transaction.conceptResponseWithNoResult();
        return ResponseBuilder.Transaction.conceptResopnseWithConcept(attribute);
    }

    private static Transaction.Res putAttribute(Concept concept, ConceptMethodProto.ConceptMethod.Req method) {
        Object value = method.getPutAttribute().getAllFields().values().iterator().next();
        Concept attribute = concept.asAttributeType().putAttribute(value);
        return ResponseBuilder.Transaction.conceptResopnseWithConcept(attribute);
    }

    private static Transaction.Res setAttribute(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Attribute<?> attribute =  ConceptBuilder.concept(method.getSetAttribute(), tx).asAttribute();
        Concept relationship = concept.asThing().attributeRelationship(attribute);
        return ResponseBuilder.Transaction.conceptResopnseWithConcept(relationship);
    }

    private static Transaction.Res unsetAttribute(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Attribute<?> attribute = ConceptBuilder.concept(method.getUnsetAttribute(), tx).asAttribute();
        concept.asThing().deleteAttribute(attribute);
        return null;
    }

    private static Transaction.Res setRegex(Concept concept, ConceptMethodProto.ConceptMethod.Req method) {
        if (method.getSetRegex().isEmpty()) {
            concept.asAttributeType().setRegex(null);
        } else {
            concept.asAttributeType().setRegex(method.getSetRegex());
        }
        return null;
    }

    private static Transaction.Res setRolePlayer(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getSetRolePlayer().getRole(), tx).asRole();
        Thing player = ConceptBuilder.concept(method.getSetRolePlayer().getPlayer(), tx).asThing();
        concept.asRelationship().addRolePlayer(role, player);
        return null;
    }

    private static Transaction.Res setRelatedRole(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getSetRelatedRole(), tx).asRole();
        concept.asRelationshipType().relates(role);
        return null;
    }

    private static Transaction.Res unsetRelatedRole(Concept concept, ConceptMethodProto.ConceptMethod.Req method, EmbeddedGraknTx tx) {
        Role role = ConceptBuilder.concept(method.getUnsetRelatedRole(), tx).asRole();
        concept.asRelationshipType().deleteRelates(role);
        return null;
    }
}