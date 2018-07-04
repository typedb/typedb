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
    public final SomeType type() {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        method.setGetDirectType(MethodProto.Unit.getDefaultInstance());
        SessionProto.Transaction.Res response = runMethod(method.build());
        Concept concept = ConceptBuilder.concept(response.getConceptResponse().getConcept(), tx());

        return asCurrentType(concept);
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        if (roles.length == 0) {
            method.setGetRelationships(MethodProto.Unit.getDefaultInstance());
        } else {
            method.setGetRelationshipsByRoles(ConceptBuilder.concepts(Arrays.asList(roles)));
        }
        return runMethodToConceptStream(method.build()).map(Concept::asRelationship);
    }

    @Override
    public final Stream<Role> plays() {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        method.setGetRolesPlayedByThing(MethodProto.Unit.getDefaultInstance());
        return runMethodToConceptStream(method.build()).map(Concept::asRole);
    }

    @Override
    public final SomeThing attribute(Attribute attribute) {
        attributeRelationship(attribute);
        return asCurrentBaseType(this);
    }

    @Override
    public final Relationship attributeRelationship(Attribute attribute) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        method.setSetAttribute(ConceptBuilder.concept(attribute));
        SessionProto.Transaction.Res response = runMethod(method.build());
        Concept concept = ConceptBuilder.concept(response.getConceptResponse().getConcept(), tx());
        return concept.asRelationship();
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        if (attributeTypes.length == 0) {
            method.setGetAttributes(MethodProto.Unit.getDefaultInstance());
        } else {
            method.setGetAttributesByTypes(ConceptBuilder.concepts(Arrays.asList(attributeTypes)));
        }
        return runMethodToConceptStream(method.build()).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        if (attributeTypes.length == 0) {
            method.setGetKeys(MethodProto.Unit.getDefaultInstance());
        } else {
            method.setGetKeysByTypes(ConceptBuilder.concepts(Arrays.asList(attributeTypes)));
        }
        return runMethodToConceptStream(method.build()).map(Concept::asAttribute);
    }

    @Override
    public final SomeThing deleteAttribute(Attribute attribute) {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        method.setUnsetAttribute(ConceptBuilder.concept(attribute));
        runMethod(method.build());

        return asCurrentBaseType(this);
    }

    @Override
    public final boolean isInferred() {
        MethodProto.Method.Req.Builder method = MethodProto.Method.Req.newBuilder();
        method.setIsInferred(MethodProto.Unit.getDefaultInstance());
        SessionProto.Transaction.Res response = runMethod(method.build());

        return response.getConceptResponse().getIsInferred();
    }

    abstract SomeType asCurrentType(Concept concept);
}
