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

import grakn.core.client.GraknClient;
import grakn.core.client.rpc.RequestBuilder;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.protocol.ConceptProto;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Client implementation of Thing
 *
 * @param <SomeThing> The exact type of this class
 * @param <SomeType>  the type of an instance of this class
 */
abstract class RemoteThing<SomeThing extends Thing, SomeType extends Type> extends RemoteConcept<SomeThing> implements Thing {

    RemoteThing(GraknClient.Transaction tx, ConceptId id) {
        super(tx, id);
    }

    @Override
    public final SomeType type() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingTypeReq(ConceptProto.Thing.Type.Req.getDefaultInstance()).build();

        Concept concept = RemoteConcept.of(runMethod(method).getThingTypeRes().getType(), tx());
        return asCurrentType(concept);
    }

    @Override
    public final boolean isInferred() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingIsInferredReq(ConceptProto.Thing.IsInferred.Req.getDefaultInstance()).build();

        return runMethod(method).getThingIsInferredRes().getInferred();
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingKeysReq(ConceptProto.Thing.Keys.Req.newBuilder()
                                         .addAllAttributeTypes(RequestBuilder.Concept.concepts(Arrays.asList(attributeTypes)))).build();

        int iteratorId = runMethod(method).getThingKeysIter().getId();
        return conceptStream(iteratorId, res -> res.getThingKeysIterRes().getAttribute()).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingAttributesReq(ConceptProto.Thing.Attributes.Req.newBuilder()
                                               .addAllAttributeTypes(RequestBuilder.Concept.concepts(Arrays.asList(attributeTypes)))).build();

        int iteratorId = runMethod(method).getThingAttributesIter().getId();
        return conceptStream(iteratorId, res -> res.getThingAttributesIterRes().getAttribute()).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Relation> relations(Role... roles) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingRelationsReq(ConceptProto.Thing.Relations.Req.newBuilder()
                                              .addAllRoles(RequestBuilder.Concept.concepts(Arrays.asList(roles)))).build();

        int iteratorId = runMethod(method).getThingRelationsIter().getId();
        return conceptStream(iteratorId, res -> res.getThingRelationsIterRes().getRelation()).map(Concept::asRelation);
    }

    @Override
    public final Stream<Role> roles() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingRolesReq(ConceptProto.Thing.Roles.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getThingRolesIter().getId();
        return conceptStream(iteratorId, res -> res.getThingRolesIterRes().getRole()).map(Concept::asRole);
    }

    @Override
    public final SomeThing has(Attribute attribute) {
        relhas(attribute);
        return asCurrentBaseType(this);
    }

    @Override
    @Deprecated
    public final Relation relhas(Attribute attribute) {
        // TODO: replace usage of this method as a getter, with relations(Attribute attribute)
        // TODO: then remove this method altogether and just use has(Attribute attribute)
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingRelhasReq(ConceptProto.Thing.Relhas.Req.newBuilder()
                                           .setAttribute(RequestBuilder.Concept.concept(attribute))).build();

        Concept concept = RemoteConcept.of(runMethod(method).getThingRelhasRes().getRelation(), tx());
        return concept.asRelation();
    }

    @Override
    public final SomeThing unhas(Attribute attribute) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingUnhasReq(ConceptProto.Thing.Unhas.Req.newBuilder()
                                          .setAttribute(RequestBuilder.Concept.concept(attribute))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    abstract SomeType asCurrentType(Concept concept);
}
