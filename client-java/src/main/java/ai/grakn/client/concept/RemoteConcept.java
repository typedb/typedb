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

import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.proto.ConceptProto;

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Client implementation of {@link ai.grakn.concept.Concept}
 *
 * @param <SomeConcept> represents the actual class of object to downcast to
 */
public abstract class RemoteConcept<SomeConcept extends Concept> implements Concept {

    public static Concept of(ConceptProto.Concept concept, Grakn.Transaction tx) {
        ConceptId id = ConceptId.of(concept.getId());

        switch (concept.getBaseType()) {
            case ENTITY:
                return RemoteEntity.construct(tx, id);
            case RELATION:
                return RemoteRelationship.construct(tx, id);
            case ATTRIBUTE:
                return RemoteAttribute.construct(tx, id);
            case ENTITY_TYPE:
                return RemoteEntityType.construct(tx, id);
            case RELATION_TYPE:
                return RemoteRelationshipType.construct(tx, id);
            case ATTRIBUTE_TYPE:
                return RemoteAttributeType.construct(tx, id);
            case ROLE:
                return RemoteRole.construct(tx, id);
            case RULE:
                return RemoteRule.construct(tx, id);
            case META_TYPE:
                return RemoteMetaType.construct(tx, id);
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }

    abstract Grakn.Transaction tx();

    @Override
    public abstract ConceptId id();

    @Override
    public final Keyspace keyspace() {
        return tx().keyspace();
    }

    @Override
    public final void delete() throws GraknTxOperationException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setConceptDeleteReq(ConceptProto.Concept.Delete.Req.getDefaultInstance())
                .build();

        runMethod(method);
    }

    @Override
    public final boolean isDeleted() {
        return tx().getConcept(id()) == null;
    }

    protected final Stream<? extends Concept> conceptStream
            (int iteratorId, Function<ConceptProto.Method.Iter.Res, ConceptProto.Concept> conceptGetter) {

        Iterable<? extends  Concept> iterable = () -> new Grakn.Transaction.Iterator<>(
                tx(), iteratorId, res -> of(conceptGetter.apply(res.getConceptMethodIterRes()), tx())
        );

        return StreamSupport.stream(iterable.spliterator(), false);
    }

    protected final ConceptProto.Method.Res runMethod(ConceptProto.Method.Req method) {
        return runMethod(id(), method);
    }

    protected final ConceptProto.Method.Res runMethod(ConceptId id, ConceptProto.Method.Req method) {
        return tx().runConceptMethod(id, method).getConceptMethodRes().getResponse();
    }

    abstract SomeConcept asCurrentBaseType(Concept other);

}
