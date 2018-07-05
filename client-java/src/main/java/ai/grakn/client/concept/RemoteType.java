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

import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.proto.MethodProto;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.SessionProto;

import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.Type}
 *
 * @param <SomeType> The exact type of this class
 * @param <SomeThing> the exact type of instances of this class
 */
abstract class RemoteType<SomeType extends Type, SomeThing extends Thing> extends RemoteSchemaConcept<SomeType> implements Type {

    @Override
    public final SomeType isAbstract(Boolean isAbstract) throws GraknTxOperationException {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetAbstract(MethodProto.SetAbstract.Req.newBuilder()
                        .setAbstract(isAbstract)).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType play(Role role) throws GraknTxOperationException {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetRolePlayedByType(MethodProto.SetRolePlayedByType.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType key(AttributeType attributeType) throws GraknTxOperationException {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetKeyType(MethodProto.SetKeyType.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType has(AttributeType attributeType) throws GraknTxOperationException {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetAttributeType(MethodProto.SetAttributeType.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final Stream<Role> plays() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetRolesPlayedByType(MethodProto.GetRolesPlayedByType.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetRolesPlayedByType().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asRole);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetAttributeTypes(MethodProto.GetAttributeTypes.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetAttributeTypes().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<AttributeType> keys() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetKeyTypes(MethodProto.GetKeyTypes.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetKeyTypes().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<SomeThing> instances() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetInstances(MethodProto.GetInstances.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetInstances().getIteratorId();
        return conceptStream(iteratorId).map(this::asInstance);
    }

    @Override
    public final Boolean isAbstract() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setIsAbstract(MethodProto.IsAbstract.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        return response.getConceptMethod().getResponse().getIsAbstract().getAbstract();
    }

    @Override
    public final SomeType unplay(Role role) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setUnsetRolePlayedByType(MethodProto.UnsetRolePlayedByType.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unhas(AttributeType attributeType) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setUnsetAttributeType(MethodProto.UnsetAttributeType.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unkey(AttributeType attributeType) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setUnsetKeyType(MethodProto.UnsetKeyType.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    protected abstract SomeThing asInstance(Concept concept);
}
