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

package ai.grakn.rpc;

import ai.grakn.concept.Concept;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.util.TxConceptReader;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message {@link ai.grakn.rpc.generated.GrpcConcept.ConceptMethod}.
 *
 * @param <T> The type of the concept method return value.
 * @author Felix Chapman
 */
public abstract class ConceptMethod<T> {

    public TxResponse createTxResponse(GrpcIterators iterators, T response) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        buildResponse(conceptResponse, iterators, response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    public abstract TxResponse run(GrpcIterators iterators, Concept concept);

    @Nullable
    public abstract T readResponse(TxConceptReader txConceptReader, GrpcClient client, TxResponse txResponse);

    public abstract void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, T value);

    public abstract GrpcConcept.ConceptMethod requestBuilder();
}