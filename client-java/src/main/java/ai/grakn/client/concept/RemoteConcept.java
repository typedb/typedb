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
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.SessionProto;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Super class of all Grakn Concepts.
 *
 * @param <SomeConcept> represents the actual class of object to downcast to
 */
public abstract class RemoteConcept<SomeConcept extends Concept> implements Concept {

    abstract Grakn.Transaction tx();

    @Override
    public abstract ConceptId getId();

    @Override
    public final Keyspace keyspace() {
        return tx().keyspace();
    }

    @Override
    public final void delete() throws GraknTxOperationException {
        ConceptProto.Method.Req.Builder method = ConceptProto.Method.Req.newBuilder();
        method.setDelete(ConceptProto.Unit.getDefaultInstance());
        runMethod(method.build());
    }

    @Override
    public final boolean isDeleted() {
        return tx().getConcept(getId()) == null;
    }

    protected final Stream<? extends Concept> runMethodToConceptStream(ConceptProto.Method.Req method) {
        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptResponse().getIteratorId();
        Iterable<? extends Concept> iterable = () -> new Grakn.Transaction.Iterator<>(
                tx(), iteratorId, res -> ConceptBuilder.concept(res.getConcept(), tx())
        );

        return StreamSupport.stream(iterable.spliterator(), false);
    }
    protected final SessionProto.Transaction.Res runMethod(ConceptProto.Method.Req method) {
        return runMethod(getId(), method);
    }

    protected final SessionProto.Transaction.Res runMethod(ConceptId id, ConceptProto.Method.Req method) {
        return tx().runConceptMethod(id, method);
    }

    abstract SomeConcept asCurrentBaseType(Concept other);

}
