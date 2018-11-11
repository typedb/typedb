/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package grakn.core.client.concept;

import grakn.core.client.rpc.RequestBuilder;
import grakn.core.concept.AttributeType;
import grakn.core.concept.Concept;
import grakn.core.concept.Role;
import grakn.core.concept.Thing;
import grakn.core.concept.Type;
import grakn.core.exception.GraknTxOperationException;
import grakn.core.protocol.ConceptProto;

import java.util.stream.Stream;

/**
 * Client implementation of {@link grakn.core.concept.Type}
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
        return conceptStream(iteratorId, res -> res.getTypeInstancesIterRes().getThing()).map(this::asInstance);
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
        return conceptStream(iteratorId, res -> res.getTypeKeysIterRes().getAttributeType()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeAttributesReq(ConceptProto.Type.Attributes.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypeAttributesIter().getId();
        return conceptStream(iteratorId, res -> res.getTypeAttributesIterRes().getAttributeType()).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<Role> playing() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypePlayingReq(ConceptProto.Type.Playing.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getTypePlayingIter().getId();
        return conceptStream(iteratorId, res -> res.getTypePlayingIterRes().getRole()).map(Concept::asRole);
    }

    @Override
    public final SomeType key(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeKeyReq(ConceptProto.Type.Key.Req.newBuilder()
                        .setAttributeType(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType has(AttributeType attributeType) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeHasReq(ConceptProto.Type.Has.Req.newBuilder()
                        .setAttributeType(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType plays(Role role) throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypePlaysReq(ConceptProto.Type.Plays.Req.newBuilder()
                        .setRole(RequestBuilder.Concept.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unkey(AttributeType attributeType) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnkeyReq(ConceptProto.Type.Unkey.Req.newBuilder()
                        .setAttributeType(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unhas(AttributeType attributeType) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnhasReq(ConceptProto.Type.Unhas.Req.newBuilder()
                        .setAttributeType(RequestBuilder.Concept.concept(attributeType))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final SomeType unplay(Role role) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setTypeUnplayReq(ConceptProto.Type.Unplay.Req.newBuilder()
                        .setRole(RequestBuilder.Concept.concept(role))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    protected abstract SomeThing asInstance(Concept concept);
}
