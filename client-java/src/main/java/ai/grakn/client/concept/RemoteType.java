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
                .setTypeInstancesReq(ConceptProto.Type.Instances.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypeInstancesIter().getId();
        return conceptStream(iteratorId, res -> res.getTypeInstancesIterRes().getConcept()).map(this::asInstance);
    }

    @Override
    public final Boolean isAbstract() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeIsAbstractReq(ConceptProto.Type.IsAbstract.Req.getDefaultInstance()).build();

        return runMethod(method).getTypeIsAbstractRes().getAbstract();
    }

    @Override
    public final SomeType isAbstract(Boolean isAbstract) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeSetAbstractReq(ConceptProto.Type.SetAbstract.Req.newBuilder()
                        .setAbstract(isAbstract)).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final Stream<AttributeType> keys() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeKeysReq(ConceptProto.Type.Keys.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypeKeysIter().getId();
        return conceptStream(iteratorId, res -> res.getTypeKeysIterRes().getConcept()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeAttributesReq(ConceptProto.Type.Attributes.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypeAttributesIter().getId();
        return conceptStream(iteratorId, res -> res.getTypeAttributesIterRes().getConcept()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<Role> playing() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypePlayingReq(ConceptProto.Type.Playing.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypePlayingIter().getId();
        return conceptStream(iteratorId, res -> res.getTypePlayingIterRes().getConcept()).map(Concept::asRole);
    }

    @Override
    public final SomeType key(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeKeyReq(ConceptProto.Type.Key.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType has(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeHasReq(ConceptProto.Type.Has.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType plays(Role role) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypePlaysReq(ConceptProto.Type.Plays.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unkey(AttributeType attributeType) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnkeyReq(ConceptProto.Type.Unkey.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unhas(AttributeType attributeType) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnhasReq(ConceptProto.Type.Unhas.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unplay(Role role) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnplayReq(ConceptProto.Type.Unplay.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    protected abstract SomeThing asInstance(Concept concept);
}
