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

import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.proto.ConceptProto;

import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.Type}
 *
 * @param <SomeType> The exact type of this class
 * @param <SomeThing> the exact type of instances of this class
 */
abstract class RemoteType<SomeType extends Type, SomeThing extends Thing> extends RemoteSchemaConcept<SomeType> implements Type {

    @Override
    public final Stream<SomeThing> instances() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeInstances(ConceptProto.Type.Instances.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypeInstances().getId();
        return conceptStream(iteratorId, res -> res.getTypeInstances().getConcept()).map(this::asInstance);
    }

    @Override
    public final Boolean isAbstract() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeIsAbstract(ConceptProto.Type.IsAbstract.Req.getDefaultInstance()).build();

        return runMethod(method).getTypeIsAbstract().getAbstract();
    }

    @Override
    public final SomeType isAbstract(Boolean isAbstract) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeSetAbstract(ConceptProto.Type.SetAbstract.Req.newBuilder()
                        .setAbstract(isAbstract)).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final Stream<AttributeType> keys() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeKeys(ConceptProto.Type.Keys.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypeKeys().getId();
        return conceptStream(iteratorId, res -> res.getTypeKeys().getConcept()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeAttributes(ConceptProto.Type.Attributes.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypeAttributes().getId();
        return conceptStream(iteratorId, res -> res.getTypeAttributes().getConcept()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<Role> playing() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypePlaying(ConceptProto.Type.Playing.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypePlaying().getId();
        return conceptStream(iteratorId, res -> res.getTypePlaying().getConcept()).map(Concept::asRole);
    }

    @Override
    public final SomeType key(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeKey(ConceptProto.Type.Key.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType has(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeHas(ConceptProto.Type.Has.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType plays(Role role) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypePlays(ConceptProto.Type.Plays.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unkey(AttributeType attributeType) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnkey(ConceptProto.Type.Unkey.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unhas(AttributeType attributeType) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnhas(ConceptProto.Type.Unhas.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unplay(Role role) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnplay(ConceptProto.Type.Unplay.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    protected abstract SomeThing asInstance(Concept concept);
}
