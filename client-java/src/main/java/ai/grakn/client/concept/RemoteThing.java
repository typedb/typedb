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
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.MethodProto;
import ai.grakn.rpc.proto.SessionProto;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <SomeThing> The exact type of this class
 * @param <SomeType> the type of an instance of this class
 */
abstract class RemoteThing<SomeThing extends Thing, SomeType extends Type> extends RemoteConcept<SomeThing> implements Thing {


    @Override
    public final boolean isInferred() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setIsInferred(MethodProto.IsInferred.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        return response.getConceptMethod().getResponse().getIsInferred().getInferred();
    }

    @Override
    public final SomeType type() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetDirectType(MethodProto.GetDirectType.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        Concept concept = ConceptBuilder.concept(response.getConceptMethod().getResponse().getGetDirectType().getConcept(), tx());
        return asCurrentType(concept);
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        IteratorProto.IteratorId iteratorId;

        if (attributeTypes.length == 0) {
            method.setGetKeys(MethodProto.GetKeys.Req.getDefaultInstance());
            iteratorId = runMethod(method.build()).getConceptMethod().getResponse().getGetKeys().getIteratorId();
        } else {
            method.setGetKeysByTypes(MethodProto.GetKeysByTypes.Req.newBuilder()
                    .setConcepts(ConceptBuilder.concepts(Arrays.asList(attributeTypes))));
            iteratorId = runMethod(method.build()).getConceptMethod().getResponse().getGetKeysByTypes().getIteratorId();
        }

        return conceptStream(iteratorId).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        IteratorProto.IteratorId iteratorId;

        if (attributeTypes.length == 0) {
            method.setGetAttributesForAnyType(MethodProto.GetAttributesForAnyType.Req.getDefaultInstance());
            iteratorId = runMethod(method.build())
                    .getConceptMethod().getResponse().getGetAttributesForAnyType().getIteratorId();
        } else {
            method.setGetAttributesByTypes(MethodProto.GetAttributesByTypes.Req.newBuilder()
                    .setConcepts(ConceptBuilder.concepts(Arrays.asList(attributeTypes))));
            iteratorId = runMethod(method.build())
                    .getConceptMethod().getResponse().getGetAttributesByTypes().getIteratorId();
        }

        return conceptStream(iteratorId).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        IteratorProto.IteratorId iteratorId;

        if (roles.length == 0) {
            method.setGetRelationships(MethodProto.GetRelationships.Req.getDefaultInstance());
            iteratorId = runMethod(method.build()).getConceptMethod().getResponse().getGetRelationships().getIteratorId();
        } else {
            method.setGetRelationshipsByRoles(MethodProto.GetRelationshipsByRoles.Req.newBuilder()
                    .setConcepts(ConceptBuilder.concepts(Arrays.asList(roles))));
            iteratorId = runMethod(method.build())
                    .getConceptMethod().getResponse().getGetRelationshipsByRoles().getIteratorId();
        }

        return conceptStream(iteratorId).map(Concept::asRelationship);
    }

    @Override
    public final Stream<Role> plays() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetRolesPlayedByThing(MethodProto.GetRolesPlayedByThing.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method)
                .getConceptMethod().getResponse().getGetRolesPlayedByThing().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asRole);
    }

    @Override
    public final SomeThing attribute(Attribute attribute) {
        attributeRelationship(attribute);
        return asCurrentBaseType(this);
    }

    @Override
    public final Relationship attributeRelationship(Attribute attribute) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetAttributeRelationship(MethodProto.SetAttributeRelationship.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attribute))).build();

        SessionProto.Transaction.Res response = runMethod(method);
        Concept concept = ConceptBuilder.concept(
                response.getConceptMethod().getResponse().getSetAttributeRelationship().getConcept(),
                tx()
        );
        return concept.asRelationship();
    }

    @Override
    public final SomeThing deleteAttribute(Attribute attribute) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setUnsetAttributeRelationship(MethodProto.UnsetAttributeRelationship.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(attribute))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    abstract SomeType asCurrentType(Concept concept);
}
