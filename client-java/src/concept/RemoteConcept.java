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

import grakn.core.Keyspace;
import grakn.core.client.Grakn;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.exception.GraknTxOperationException;
import grakn.core.rpc.proto.ConceptProto;

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Client implementation of {@link grakn.core.concept.Concept}
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
