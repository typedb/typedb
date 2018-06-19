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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.RPCIterators;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ConceptReader;
import ai.grakn.rpc.util.ResponseBuilder;
import ai.grakn.rpc.util.TxConceptReader;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message {@link ai.grakn.rpc.generated.GrpcConcept.ConceptMethod}.
 *
 * @param <T> The type of the concept method return value.
 */
public abstract class ConceptMethod<T> {

    // Server: TxRequestLister.runConceptMethod()
    public static TxResponse run(Concept concept, GrpcConcept.ConceptMethod method, RPCIterators iterators, TxConceptReader reader) {
        switch (method.getConceptMethodCase()) {
            case GETVALUE:
                return getValue(concept);
            case GETDATATYPEOFTYPE:
                return getDataTypeOfType(concept);
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
            case CONCEPTMETHOD_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + method);
        }
    }

    private static TxResponse getValue(Concept concept) {
        Object response = concept.asAttribute().getValue();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setAttributeValue(ConceptBuilder.attributeValue(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getDataTypeOfType(Concept concept) {
        Optional<AttributeType.DataType<?>> response = Optional.ofNullable(concept.asAttributeType().getDataType());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setOptionalDataType(ResponseBuilder.optionalDataType(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getDataTypeOfAttribute(Concept concept) {
        AttributeType.DataType<?> response = concept.asAttribute().dataType();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setDataType(ConceptBuilder.dataType(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getLabel(Concept concept) {
        Label response = concept.asSchemaConcept().getLabel();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setLabel(ConceptBuilder.label(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse setLabel(Concept concept, GrpcConcept.ConceptMethod method) {
        concept.asSchemaConcept().setLabel(ConceptReader.label(method.getSetLabel()));
        return null;
    }

    private static TxResponse isImplicit(Concept concept) {
        Boolean response = concept.asSchemaConcept().isImplicit();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setBool(response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse isInferred(Concept concept) {
        Boolean response = concept.asThing().isInferred();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setBool(response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse isAbstract(Concept concept) {
        Boolean response = concept.asType().isAbstract();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setBool(response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
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
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRegex(Concept concept) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        String regex = concept.asAttributeType().getRegex();

        if (regex != null) {
            conceptResponse.setRegex(regex);
        } else {
            conceptResponse.setNoResult(true);
        }

        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRolePlayers(Concept concept, RPCIterators iterators) {
        Stream.Builder<RolePlayer> rolePlayers = Stream.builder();
        concept.asRelationship().allRolePlayers().forEach(
                (role, players) -> players.forEach(
                        player -> rolePlayers.add(RolePlayer.create(role, player))
                )
        );
        Stream<RolePlayer> response = rolePlayers.build();

        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        Stream<TxResponse> responses = response.map(ResponseBuilder::rolePlayer);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        conceptResponse.setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getAttributeTypes(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asType().attributes();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse setAttributeType(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getSetAttributeType()).asAttributeType();
        concept.asType().attribute(attributeType);
        return null;
    }

    private static TxResponse unsetAttributeType(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getUnsetAttributeType()).asAttributeType();
        concept.asType().deleteAttribute(attributeType);
        return null;
    }

    private static TxResponse getKeyTypes(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asType().keys();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getDirectType(Concept concept) {
        Concept response = concept.asThing().type();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getDirectSuper(Concept concept) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        Concept superConcept = concept.asSchemaConcept().sup();

        if (superConcept != null) {
            conceptResponse.setConcept(ConceptBuilder.concept(superConcept));
        } else {
            conceptResponse.setNoResult(true);
        }

        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse setDirectSuper(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
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

    private static TxResponse removeRolePlayer(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        RolePlayer rolePlayer = reader.rolePlayer(method.getUnsetRolePlayer());
        concept.asRelationship().removeRolePlayer(rolePlayer.role(), rolePlayer.player());
        return null;
    }

    private static TxResponse delete(Concept concept) {
        concept.delete();
        return null;
    }

    private static TxResponse getOwners(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asAttribute().ownerInstances();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getTypesThatPlayRole(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asRole().playedByTypes();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRolesPlayedByType(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asType().plays();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getInstances(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asType().instances();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRelatedRoles(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asRelationshipType().relates();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getAttributes(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asThing().attributes();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getSuperConcepts(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asSchemaConcept().sups();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getSubConcepts(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asSchemaConcept().subs();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRelationshipTypesThatRelateRole(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asRole().relationshipTypes();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getAttributesByTypes(Concept concept, GrpcConcept.ConceptMethod method, RPCIterators iterators, TxConceptReader reader) {
        GrpcConcept.Concepts rpcAttributeTypes = method.getGetAttributesByTypes();
        AttributeType<?>[] attributeTypes = ConceptReader.concepts(reader, rpcAttributeTypes).toArray(AttributeType[]::new);

        Stream<? extends Concept> response = concept.asThing().attributes(attributeTypes);
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRelationships(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asThing().relationships();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRelationshipsByRoles(Concept concept, RPCIterators iterators, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Role[] roles = ConceptReader.concepts(reader, method.getGetRelationshipsByRoles()).toArray(Role[]::new);
        Stream<? extends Concept> response = concept.asThing().relationships(roles);
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRolesPlayedByThing(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asThing().plays();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getKeys(Concept concept, RPCIterators iterators) {
        Stream<? extends Concept> response = concept.asThing().keys();
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getKeysByTypes(Concept concept, RPCIterators iterators, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        GrpcConcept.Concepts rpcKeyTypes = method.getGetKeysByTypes();
        AttributeType<?>[] keyTypes = ConceptReader.concepts(reader, rpcKeyTypes).toArray(AttributeType[]::new);

        Stream<? extends Concept> response = concept.asThing().keys(keyTypes);
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getRolePlayersByRoles(Concept concept, RPCIterators iterators, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Role[] roles = ConceptReader.concepts(reader, method.getGetRolePlayersByRoles()).toArray(Role[]::new);
        Stream<? extends Concept> response = concept.asRelationship().rolePlayers(roles);
        Stream<TxResponse> responses = response.map(ResponseBuilder::concept);
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse setKeyType(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getSetKeyType()).asAttributeType();
        concept.asType().key(attributeType);
        return null;
    }

    private static TxResponse unsetKeyType(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        AttributeType<?> attributeType = reader.concept(method.getUnsetKeyType()).asAttributeType();
        concept.asType().deleteKey(attributeType);
        return null;
    }

    private static TxResponse setRolePlayedByType(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Role role = reader.concept(method.getSetRolePlayedByType()).asRole();
        concept.asType().plays(role);
        return null;
    }

    private static TxResponse unsetRolePlayedByType(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Role role = reader.concept(method.getUnsetRolePlayedByType()).asRole();
        concept.asType().deletePlays(role);
        return null;
    }

    private static TxResponse addEntity(Concept concept) {
        Concept response = concept.asEntityType().addEntity();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse addRelationship(Concept concept) {
        Concept response = concept.asRelationshipType().addRelationship();
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse getAttribute(Concept concept, GrpcConcept.ConceptMethod method) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        Object value = ConceptReader.attributeValue(method.getGetAttribute());
        Concept attribute = concept.asAttributeType().getAttribute(value);

        if (attribute != null) {
            conceptResponse.setConcept(ConceptBuilder.concept(attribute));
        } else {
            conceptResponse.setNoResult(true);
        }

        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse putAttribute(Concept concept, GrpcConcept.ConceptMethod method) {
        Object value = ConceptReader.attributeValue(method.getPutAttribute());
        Concept response = concept.asAttributeType().putAttribute(value);
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse setAttribute(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Attribute<?> attribute =  reader.concept(method.getSetAttribute()).asAttribute();
        Concept response = concept.asThing().attributeRelationship(attribute);
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(response));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    private static TxResponse unsetAttribute(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Attribute<?> attribute = reader.concept(method.getUnsetAttribute()).asAttribute();
        concept.asThing().deleteAttribute(attribute);
        return null;
    }

    private static TxResponse setRegex(Concept concept, GrpcConcept.ConceptMethod method) {
        concept.asAttributeType().setRegex(method.getSetRegex());
        return null;
    }

    private static TxResponse setRolePlayer(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        RolePlayer rolePlayer = reader.rolePlayer(method.getSetRolePlayer());
        concept.asRelationship().addRolePlayer(rolePlayer.role(), rolePlayer.player());
        return null;
    }

    private static TxResponse setRelatedRole(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Role role = reader.concept(method.getSetRelatedRole()).asRole();
        concept.asRelationshipType().relates(role);
        return null;
    }

    private static TxResponse unsetRelatedRole(Concept concept, GrpcConcept.ConceptMethod method, TxConceptReader reader) {
        Role role = reader.concept(method.getUnsetRelatedRole()).asRole();
        concept.asRelationshipType().deleteRelates(role);
        return null;
    }
}