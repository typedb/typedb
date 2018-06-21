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

package ai.grakn.remote.concept;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.remote.rpc.ConceptBuilder;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;

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
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setSetAbstract(isAbstract);
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType plays(Role role) throws GraknTxOperationException {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setSetRolePlayedByType(ConceptBuilder.concept(role));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType key(AttributeType attributeType) throws GraknTxOperationException {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setSetKeyType(ConceptBuilder.concept(attributeType));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType attribute(AttributeType attributeType) throws GraknTxOperationException {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setSetAttributeType(ConceptBuilder.concept(attributeType));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final Stream<Role> plays() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetRolesPlayedByType(GrpcConcept.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asRole);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetAttributeTypes(GrpcConcept.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<AttributeType> keys() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetKeyTypes(GrpcConcept.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<SomeThing> instances() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetInstances(GrpcConcept.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(this::asInstance);
    }

    @Override
    public final Boolean isAbstract() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setIsAbstract(GrpcConcept.Unit.getDefaultInstance());
        GrpcGrakn.TxResponse response = runMethod(method.build());

        return response.getConceptResponse().getIsAbstract();
    }

    @Override
    public final SomeType deletePlays(Role role) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setUnsetRolePlayedByType(ConceptBuilder.concept(role));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType deleteAttribute(AttributeType attributeType) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setUnsetAttributeType(ConceptBuilder.concept(attributeType));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType deleteKey(AttributeType attributeType) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setUnsetKeyType(ConceptBuilder.concept(attributeType)).build();
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    protected abstract SomeThing asInstance(Concept concept);
}
