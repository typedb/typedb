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

package ai.grakn.client.concept;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.TransactionProto;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <SomeType> The exact type of this class
 * @param <SomeThing> the exact type of instances of this class
 */
abstract class RemoteType<SomeType extends Type, SomeThing extends Thing> extends RemoteSchemaConcept<SomeType> implements Type {

    @Override
    public final SomeType setAbstract(Boolean isAbstract) throws GraknTxOperationException {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setSetAbstract(isAbstract);
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType plays(Role role) throws GraknTxOperationException {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setSetRolePlayedByType(ConceptBuilder.concept(role));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType key(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setSetKeyType(ConceptBuilder.concept(attributeType));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType attribute(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setSetAttributeType(ConceptBuilder.concept(attributeType));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final Stream<Role> plays() {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setGetRolesPlayedByType(ConceptProto.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asRole);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setGetAttributeTypes(ConceptProto.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<AttributeType> keys() {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setGetKeyTypes(ConceptProto.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<SomeThing> instances() {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setGetInstances(ConceptProto.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(this::asInstance);
    }

    @Override
    public final Boolean isAbstract() {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setIsAbstract(ConceptProto.Unit.getDefaultInstance());
        TransactionProto.TxResponse response = runMethod(method.build());

        return response.getConceptResponse().getIsAbstract();
    }

    @Override
    public final SomeType deletePlays(Role role) {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setUnsetRolePlayedByType(ConceptBuilder.concept(role));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType deleteAttribute(AttributeType attributeType) {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setUnsetAttributeType(ConceptBuilder.concept(attributeType));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType deleteKey(AttributeType attributeType) {
        ConceptProto.ConceptMethod.Builder method = ConceptProto.ConceptMethod.newBuilder();
        method.setUnsetKeyType(ConceptBuilder.concept(attributeType)).build();
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    protected abstract SomeThing asInstance(Concept concept);
}
