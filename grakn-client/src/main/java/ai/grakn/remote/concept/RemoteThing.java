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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.remote.rpc.RemoteIterator;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ConceptMethod;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 * @param <MyType> the type of an instance of this class
 */
abstract class RemoteThing<Self extends Thing, MyType extends Type> extends RemoteConcept<Self> implements Thing {

    @Override
    public final MyType type() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetDirectType(GrpcConcept.Unit.getDefaultInstance());
        GrpcGrakn.TxResponse response = runMethod(method.build());
        Concept concept = tx().conceptReader().concept(response.getConceptResponse().getConcept());

        return asMyType(concept);
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        ConceptMethod<Stream<? extends Concept>> method;
        if (roles.length == 0) {
            method = ConceptMethod.GET_RELATIONSHIPS;
        } else {
            method = ConceptMethod.getRelationshipsByRoles(roles);
        }
        return runMethod(method).map(Concept::asRelationship);
    }

    @Override
    public final Stream<Role> plays() {
        return runMethod(ConceptMethod.GET_ROLES_PLAYED_BY_THING).map(Concept::asRole);
    }

    @Override
    public final Self attribute(Attribute attribute) {
        attributeRelationship(attribute);
        return asSelf(this);
    }

    @Override
    public final Relationship attributeRelationship(Attribute attribute) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setSetAttribute(ConceptBuilder.concept(attribute));
        GrpcGrakn.TxResponse response = runMethod(method.build());
        Concept concept = tx().conceptReader().concept(response.getConceptResponse().getConcept());
        return concept.asRelationship();
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();

        if (attributeTypes.length == 0) {
            method.setGetAttributes(GrpcConcept.Unit.getDefaultInstance());
        } else {
            method.setGetAttributesByTypes(ConceptBuilder.concepts(Stream.of(attributeTypes)));
        }

        GrpcIterator.IteratorId iteratorId = runMethod(method.build()).getConceptResponse().getIteratorId();
        Iterable<? extends Concept> iterable = () -> new RemoteIterator<>(
                tx(), iteratorId, res -> tx().conceptReader().concept(res.getConcept())
        );

        return StreamSupport.stream(iterable.spliterator(), false).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        ConceptMethod<Stream<? extends Concept>> method;

        if (attributeTypes.length == 0) {
            method = ConceptMethod.GET_KEYS;
        } else {
            method = ConceptMethod.getKeysByTypes(attributeTypes);
        }

        return runMethod(method).map(Concept::asAttribute);
    }

    @Override
    public final Self deleteAttribute(Attribute attribute) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setUnsetAttribute(ConceptBuilder.concept(attribute));
        runMethod(method.build());

        return asSelf(this);
    }

    @Override
    public final boolean isInferred() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setIsInferred(GrpcConcept.Unit.getDefaultInstance());
        GrpcGrakn.TxResponse response = runMethod(method.build());

        return response.getConceptResponse().getBool();
    }

    abstract MyType asMyType(Concept concept);
}
