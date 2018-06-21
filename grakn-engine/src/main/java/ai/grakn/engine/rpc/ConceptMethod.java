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
import ai.grakn.engine.util.EmbeddedConceptReader;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ResponseBuilder;

import java.util.stream.Stream;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message {@link ai.grakn.rpc.generated.GrpcConcept.ConceptMethod}.
 */
public abstract class ConceptMethod {

    // Server: TxRequestLister.runConceptMethod()
    public static TxResponse run(Concept concept, GrpcConcept.ConceptMethod method, TransactionService.Iterators iterators, EmbeddedConceptReader reader) {
        switch (method.getMethodCase()) {
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
            case GETATTRIBUTETYPES:
                return getAttributeTypes(concept, iterators);
            case SETATTRIBUTETYPE:
                return setAttributeType(concept, method, reader);
            case UNSETATTRIBUTETYPE:
                return unsetAttributeType(concept, method, reader);
            case GETKEYTYPES:
                return getKeyTypes(concept, iterators);
            case GETDIRECTTYPE:
                return getDirectType(concept);
            case GETDIRECTSUPERCONCEPT:
                return getDirectSuper(concept);
            case SETDIRECTSUPERCONCEPT:
                return setDirectSuper(concept, method, reader);
            case UNSETROLEPLAYER:
                return removeRolePlayer(concept, method, reader);
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
                return getAttributesByTypes(concept, method, iterators, reader);
            case GETRELATIONSHIPS:
                return getRelationships(concept, iterators);
            case GETRELATIONSHIPSBYROLES:
                return getRelationshipsByRoles(concept, iterators, method, reader);
            case GETROLESPLAYEDBYTHING:
                return getRolesPlayedByThing(concept, iterators);
            case GETKEYS:
                return getKeys(concept, iterators);
            case GETKEYSBYTYPES:
                return getKeysByTypes(concept, iterators, method, reader);
            case GETROLEPLAYERSBYROLES:
                return getRolePlayersByRoles(concept, iterators, method, reader);
            case SETKEYTYPE:
                return setKeyType(concept, method, reader);
            case UNSETKEYTYPE:
                return unsetKeyType(concept, method, reader);
            case SETROLEPLAYEDBYTYPE:
                return setRolePlayedByType(concept, method, reader);
            case UNSETROLEPLAYEDBYTYPE:
                return unsetRolePlayedByType(concept, method, reader);
            case ADDENTITY:
                return addEntity(concept);
            case ADDRELATIONSHIP:
                return addRelationship(concept);
            case GETATTRIBUTE:
                return getAttribute(concept, method);
            case PUTATTRIBUTE:
                return putAttribute(concept, method);
            case SETATTRIBUTE:
                return setAttribute(concept, method, reader);
            case UNSETATTRIBUTE:
                return unsetAttribute(concept, method, reader);
            case SETREGEX:
                return setRegex(concept, method);
            case SETROLEPLAYER:
                return setRolePlayer(concept, method, reader);
            case SETRELATEDROLE:
                return setRelatedRole(concept, method, reader);
            case UNSETRELATEDROLE:
                return unsetRelatedRole(concept, method, reader);
            default:
            case METHOD_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + method);
        }
    }

    private static TxResponse getValue(Concept concept) {
        Object response = concept.asAttribute().getValue();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setAttributeValue(ConceptBuilder.attributeValue(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getDataTypeOfAttributeType(Concept concept) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        AttributeType.DataType<?> dataType = concept.asAttributeType().getDataType();

        if (dataType != null) {
            conceptResponse.setDataType(ConceptBuilder.dataType(dataType));
        } else {
            conceptResponse.setNoResult(true);
        }
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getDataTypeOfAttribute(Concept concept) {
        AttributeType.DataType<?> response = concept.asAttribute().dataType();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setDataType(ConceptBuilder.dataType(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getLabel(Concept concept) {
        Label label = concept.asSchemaConcept().getLabel();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setLabel(label.getValue());
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse setLabel(Concept concept, GrpcConcept.ConceptMethod method) {
        concept.asSchemaConcept().setLabel(Label.of(method.getSetLabel()));
        return null;
    }

    private static TxResponse isImplicit(Concept concept) {
        Boolean response = concept.asSchemaConcept().isImplicit();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIsImplicit(response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse isInferred(Concept concept) {
        Boolean response = concept.asThing().isInferred();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIsInferred(response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse isAbstract(Concept concept) {
        Boolean response = concept.asType().isAbstract();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIsAbstract(response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse setAbstract(Concept concept, GrpcConcept.ConceptMethod method) {
        concept.asType().setAbstract(method.getSetAbstract());
        return null;
    }
    
    private static TxResponse getWhen(Concept concept) {
        return getPattern(concept.asRule().getWhen());
    }
    
    private static TxResponse getThen(Concept concept) {
        return getPattern(concept.asRule().getThen());
    }
    
    private static TxResponse getPattern(Pattern pattern) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        if (pattern != null) {
            conceptResponse.setPattern(pattern.toString());
        } else {
            conceptResponse.setNoResult(true);
        }
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRegex(Concept concept) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        String regex = concept.asAttributeType().getRegex();

        if (regex != null) {
            conceptResponse.setRegex(regex);
        } else {
            conceptResponse.setNoResult(true);
        }

        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRolePlayers(Concept concept, TransactionService.Iterators iterators) {
        Stream.Builder<TxResponse> rolePlayersBuilder = Stream.builder();
        concept.asRelationship().allRolePlayers().forEach(
                (role, players) -> players.forEach(player -> rolePlayersBuilder.add(ResponseBuilder.rolePlayer(role, player)))
        );

        Stream<TxResponse> rolePlayers = rolePlayersBuilder.build();
        GrpcIterator.IteratorId iteratorId = iterators.add(rolePlayers.iterator());
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId).build();

        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getAttributeTypes(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().attributes();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse setAttributeType(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getSetAttributeType()).asAttributeType();
        concept.asType().attribute(attributeType);
        return null;
    }

    private static TxResponse unsetAttributeType(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getUnsetAttributeType()).asAttributeType();
        concept.asType().deleteAttribute(attributeType);
        return null;
    }

    private static TxResponse getKeyTypes(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().keys();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);

        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);

        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getDirectType(Concept concept) {
        Concept response = concept.asThing().type();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getDirectSuper(Concept concept) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        Concept superConcept = concept.asSchemaConcept().sup();

        if (superConcept != null) {
            conceptResponse.setConcept(ConceptBuilder.concept(superConcept));
        } else {
            conceptResponse.setNoResult(true);
        }

        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse setDirectSuper(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        // Make the second argument the super of the first argument
        // @throws GraqlQueryException if the types are different, or setting the super to be a meta-type
        //TODO: This was copied from ConceptBuilder

        GrpcConcept.Concept setDirectSuperConcept = method.getSetDirectSuperConcept();
        SchemaConcept schemaConcept = reader.concept(setDirectSuperConcept).asSchemaConcept();

        SchemaConcept subConcept = concept.asSchemaConcept();
        SchemaConcept superConcept = schemaConcept;

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

    private static TxResponse removeRolePlayer(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Role role = reader.concept(method.getUnsetRolePlayer().getRole()).asRole();
        Thing player = reader.concept(method.getUnsetRolePlayer().getPlayer()).asThing();
        concept.asRelationship().removeRolePlayer(role, player);
        return null;
    }

    private static TxResponse delete(Concept concept) {
        concept.delete();
        return null;
    }

    private static TxResponse getOwners(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asAttribute().ownerInstances();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getTypesThatPlayRole(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asRole().playedByTypes();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRolesPlayedByType(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().plays();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getInstances(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asType().instances();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRelatedRoles(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asRelationshipType().relates();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getAttributes(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().attributes();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getSuperConcepts(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asSchemaConcept().sups();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getSubConcepts(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asSchemaConcept().subs();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRelationshipTypesThatRelateRole(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asRole().relationshipTypes();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getAttributesByTypes(Concept concept, GrpcConcept.ConceptMethod method, TransactionService.Iterators iterators, EmbeddedConceptReader reader) {
        GrpcConcept.Concepts rpcAttributeTypes = method.getGetAttributesByTypes();
        AttributeType<?>[] attributeTypes =
                rpcAttributeTypes.getConceptsList().stream().map(reader::concept).toArray(AttributeType[]::new);

        Stream<? extends Concept> concepts = concept.asThing().attributes(attributeTypes);
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRelationships(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().relationships();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRelationshipsByRoles(Concept concept, TransactionService.Iterators iterators, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        GrpcConcept.Concepts rpcRoles = method.getGetRelationshipsByRoles();
        Role[] roles = rpcRoles.getConceptsList().stream().map(reader::concept).toArray(Role[]::new);

        Stream<? extends Concept> concepts = concept.asThing().relationships(roles);
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRolesPlayedByThing(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().plays();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getKeys(Concept concept, TransactionService.Iterators iterators) {
        Stream<? extends Concept> concepts = concept.asThing().keys();
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getKeysByTypes(Concept concept, TransactionService.Iterators iterators, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        GrpcConcept.Concepts rpcKeyTypes = method.getGetKeysByTypes();
        AttributeType<?>[] keyTypes = rpcKeyTypes.getConceptsList().stream().map(reader::concept).toArray(AttributeType[]::new);

        Stream<? extends Concept> concepts = concept.asThing().keys(keyTypes);
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getRolePlayersByRoles(Concept concept, TransactionService.Iterators iterators, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        GrpcConcept.Concepts rpcRoles = method.getGetRolePlayersByRoles();
        Role[] roles = rpcRoles.getConceptsList().stream().map(reader::concept).toArray(Role[]::new);

        Stream<? extends Concept> concepts = concept.asRelationship().rolePlayers(roles);
        Stream<TxResponse> responses = concepts.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse setKeyType(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getSetKeyType()).asAttributeType();
        concept.asType().key(attributeType);
        return null;
    }

    private static TxResponse unsetKeyType(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getUnsetKeyType()).asAttributeType();
        concept.asType().deleteKey(attributeType);
        return null;
    }

    private static TxResponse setRolePlayedByType(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Role role = reader.concept(method.getSetRolePlayedByType()).asRole();
        concept.asType().plays(role);
        return null;
    }

    private static TxResponse unsetRolePlayedByType(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Role role = reader.concept(method.getUnsetRolePlayedByType()).asRole();
        concept.asType().deletePlays(role);
        return null;
    }

    private static TxResponse addEntity(Concept concept) {
        Concept response = concept.asEntityType().addEntity();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse addRelationship(Concept concept) {
        Concept response = concept.asRelationshipType().addRelationship();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse getAttribute(Concept concept, GrpcConcept.ConceptMethod method) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        Object value = method.getGetAttribute().getAllFields().values().iterator().next();
        Concept attribute = concept.asAttributeType().getAttribute(value);

        if (attribute != null) {
            conceptResponse.setConcept(ConceptBuilder.concept(attribute));
        } else {
            conceptResponse.setNoResult(true);
        }

        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse putAttribute(Concept concept, GrpcConcept.ConceptMethod method) {
        Object value = method.getPutAttribute().getAllFields().values().iterator().next();
        Concept response = concept.asAttributeType().putAttribute(value);
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse setAttribute(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Attribute<?> attribute =  reader.concept(method.getSetAttribute()).asAttribute();
        Concept response = concept.asThing().attributeRelationship(attribute);
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    private static TxResponse unsetAttribute(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Attribute<?> attribute = reader.concept(method.getUnsetAttribute()).asAttribute();
        concept.asThing().deleteAttribute(attribute);
        return null;
    }

    private static TxResponse setRegex(Concept concept, GrpcConcept.ConceptMethod method) {
        concept.asAttributeType().setRegex(method.getSetRegex());
        return null;
    }

    private static TxResponse setRolePlayer(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Role role = reader.concept(method.getSetRolePlayer().getRole()).asRole();
        Thing player = reader.concept(method.getSetRolePlayer().getPlayer()).asThing();
        concept.asRelationship().addRolePlayer(role, player);
        return null;
    }

    private static TxResponse setRelatedRole(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Role role = reader.concept(method.getSetRelatedRole()).asRole();
        concept.asRelationshipType().relates(role);
        return null;
    }

    private static TxResponse unsetRelatedRole(Concept concept, GrpcConcept.ConceptMethod method, EmbeddedConceptReader reader) {
        Role role = reader.concept(method.getUnsetRelatedRole()).asRole();
        concept.asRelationshipType().deleteRelates(role);
        return null;
    }
}