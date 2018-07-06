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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.SessionProto;

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
    public final boolean isInferred() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setIsInferred(ConceptProto.IsInferred.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        return response.getConceptMethod().getResponse().getIsInferred().getInferred();
    }

    @Override
    public final SomeType type() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetDirectType(ConceptProto.GetDirectType.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        Concept concept = ConceptBuilder.concept(response.getConceptMethod().getResponse().getGetDirectType().getConcept(), tx());
        return asCurrentType(concept);
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        ConceptProto.Method.Req.Builder method = ConceptProto.Method.Req.newBuilder();
        IteratorProto.IteratorId iteratorId;

        if (attributeTypes.length == 0) {
            method.setGetKeys(ConceptProto.GetKeys.Req.getDefaultInstance());
            iteratorId = runMethod(method.build()).getConceptMethod().getResponse().getGetKeys().getIteratorId();
        } else {
            method.setGetKeysByTypes(ConceptProto.GetKeysByTypes.Req.newBuilder()
                    .addAllConcepts(ConceptBuilder.concepts(Arrays.asList(attributeTypes))));
            iteratorId = runMethod(method.build()).getConceptMethod().getResponse().getGetKeysByTypes().getIteratorId();
        }

        return conceptStream(iteratorId).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        ConceptProto.Method.Req.Builder method = ConceptProto.Method.Req.newBuilder();
        IteratorProto.IteratorId iteratorId;

        if (attributeTypes.length == 0) {
            method.setGetAttributesForAnyType(ConceptProto.GetAttributesForAnyType.Req.getDefaultInstance());
            iteratorId = runMethod(method.build())
                    .getConceptMethod().getResponse().getGetAttributesForAnyType().getIteratorId();
        } else {
            method.setGetAttributesByTypes(ConceptProto.GetAttributesByTypes.Req.newBuilder()
                    .addAllConcepts(ConceptBuilder.concepts(Arrays.asList(attributeTypes))));
            iteratorId = runMethod(method.build())
                    .getConceptMethod().getResponse().getGetAttributesByTypes().getIteratorId();
        }

        return conceptStream(iteratorId).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        ConceptProto.Method.Req.Builder method = ConceptProto.Method.Req.newBuilder();
        IteratorProto.IteratorId iteratorId;

        if (roles.length == 0) {
            method.setGetRelationships(ConceptProto.GetRelationships.Req.getDefaultInstance());
            iteratorId = runMethod(method.build()).getConceptMethod().getResponse().getGetRelationships().getIteratorId();
        } else {
            method.setGetRelationshipsByRoles(ConceptProto.GetRelationshipsByRoles.Req.newBuilder()
                    .addAllConcepts(ConceptBuilder.concepts(Arrays.asList(roles))));
            iteratorId = runMethod(method.build())
                    .getConceptMethod().getResponse().getGetRelationshipsByRoles().getIteratorId();
        }

        return conceptStream(iteratorId).map(Concept::asRelationship);
    }

    @Override
    public final Stream<Role> roles() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetRolesPlayedByThing(ConceptProto.GetRolesPlayedByThing.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method)
                .getConceptMethod().getResponse().getGetRolesPlayedByThing().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asRole);
    }

    @Override
    public final SomeThing has(Attribute attribute) {
        relhas(attribute);
        return asCurrentBaseType(this);
    }

    @Override @Deprecated
    public final Relationship relhas(Attribute attribute) {
        // TODO: remove usage of this method as a getter, and replace with relationships(Attribute attribute)
        // TODO: remove this method altogether and just use has(Attribute attribute)
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSetAttributeRelationship(ConceptProto.SetAttributeRelationship.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attribute))).build();

        SessionProto.Transaction.Res response = runMethod(method);
        Concept concept = ConceptBuilder.concept(
                response.getConceptMethod().getResponse().getSetAttributeRelationship().getConcept(),
                tx()
        );
        return concept.asRelationship();
    }

    @Override
    public final SomeThing unhas(Attribute attribute) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setUnsetAttributeRelationship(ConceptProto.UnsetAttributeRelationship.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attribute))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    abstract SomeType asCurrentType(Concept concept);
}
