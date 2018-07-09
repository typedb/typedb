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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.rpc.proto.ConceptProto;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.Thing}
 *
 * @param <SomeThing> The exact type of this class
 * @param <SomeType> the type of an instance of this class
 */
abstract class RemoteThing<SomeThing extends Thing, SomeType extends Type> extends RemoteConcept<SomeThing> implements Thing {

    @Override
    public final SomeType type() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingType(ConceptProto.Thing.Type.Req.getDefaultInstance()).build();

        Concept concept = ConceptReader.concept(runMethod(method).getThingType().getConcept(), tx());
        return asCurrentType(concept);
    }

    @Override
    public final boolean isInferred() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingIsInferred(ConceptProto.Thing.IsInferred.Req.getDefaultInstance()).build();

        return runMethod(method).getThingIsInferred().getInferred();
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingKeys(ConceptProto.Thing.Keys.Req.newBuilder()
                        .addAllConcepts(RequestBuilder.Concept.concepts(Arrays.asList(attributeTypes)))).build();

        int iteratorId = runMethod(method).getThingKeys().getId();
        return conceptStream(iteratorId, res -> res.getThingKeys().getConcept()).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingAttributes(ConceptProto.Thing.Attributes.Req.newBuilder()
                        .addAllConcepts(RequestBuilder.Concept.concepts(Arrays.asList(attributeTypes)))).build();

        int iteratorId = runMethod(method).getThingAttributes().getId();
        return conceptStream(iteratorId, res -> res.getThingAttributes().getConcept()).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingRelations(ConceptProto.Thing.Relations.Req.newBuilder()
                        .addAllConcepts(RequestBuilder.Concept.concepts(Arrays.asList(roles)))).build();

        int iteratorId = runMethod(method).getThingRelations().getId();
        return conceptStream(iteratorId, res -> res.getThingRelations().getConcept()).map(Concept::asRelationship);
    }

    @Override
    public final Stream<Role> roles() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingRoles(ConceptProto.Thing.Roles.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getThingRoles().getId();
        return conceptStream(iteratorId, res -> res.getThingRoles().getConcept()).map(Concept::asRole);
    }

    @Override
    public final SomeThing has(Attribute attribute) {
        relhas(attribute);
        return asCurrentBaseType(this);
    }

    @Override @Deprecated
    public final Relationship relhas(Attribute attribute) {
        // TODO: replace usage of this method as a getter, with relationships(Attribute attribute)
        // TODO: then remove this method altogether and just use has(Attribute attribute)
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingRelhas(ConceptProto.Thing.Relhas.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attribute))).build();

        Concept concept = ConceptReader.concept(runMethod(method).getThingRelhas().getConcept(), tx());
        return concept.asRelationship();
    }

    @Override
    public final SomeThing unhas(Attribute attribute) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setThingUnhas(ConceptProto.Thing.Unhas.Req.newBuilder()
                        .setConcept(RequestBuilder.Concept.concept(attribute))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    abstract SomeType asCurrentType(Concept concept);
}
