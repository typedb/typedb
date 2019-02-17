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
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.protocol.ConceptProto;
import grakn.core.server.exception.TransactionException;

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Client implementation of {@link Concept}
 *
 * @param <SomeConcept> represents the actual class of object to downcast to
 */
public abstract class RemoteConcept<SomeConcept extends Concept> implements Concept {

    private final GraknClient.Transaction tx;
    private final ConceptId id;

    RemoteConcept(GraknClient.Transaction tx, ConceptId id) {
        if (tx == null) {
            throw new NullPointerException("Null tx");
        }
        this.tx = tx;
        if (id == null) {
            throw new NullPointerException("Null id");
        }
        this.id = id;
    }

    public static Concept of(ConceptProto.Concept concept, GraknClient.Transaction tx) {
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

    GraknClient.Transaction tx() {
        return tx;
    }

    public ConceptId id() {
        return id;
    }

    @Override
    public final void delete() throws TransactionException {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setConceptDeleteReq(ConceptProto.Concept.Delete.Req.getDefaultInstance())
                .build();

        runMethod(method);
    }

    @Override
    public final boolean isDeleted() {
        return tx().getConcept(id()) == null;
    }

    abstract SomeConcept asCurrentBaseType(Concept other);

    protected final Stream<? extends Concept> conceptStream
            (int iteratorId, Function<ConceptProto.Method.Iter.Res, ConceptProto.Concept> conceptGetter) {

        Iterable<? extends Concept> iterable = () -> tx().iterator(
                iteratorId, res -> of(conceptGetter.apply(res.getConceptMethodIterRes()), tx())
        );

        return StreamSupport.stream(iterable.spliterator(), false);
    }

    protected final ConceptProto.Method.Res runMethod(ConceptProto.Method.Req method) {
        return runMethod(id(), method);
    }

    protected final ConceptProto.Method.Res runMethod(ConceptId id, ConceptProto.Method.Req method) {
        return tx().runConceptMethod(id, method).getConceptMethodRes().getResponse();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{tx=" + tx + ", id=" + id + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteConcept<?> that = (RemoteConcept<?>) o;

        return (tx.equals(that.tx())) &&
                id.equals(that.id());
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= tx.hashCode();
        h *= 1000003;
        h ^= id.hashCode();
        return h;
    }
}
