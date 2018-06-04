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
public final class ConceptMethod<T> {

    // This is the method that is used to create the request on the client-side
    private final Consumer<GrpcConcept.ConceptMethod.Builder> requestBuilder;

    // This method is used to extract the value from a Concept
    private final Function<Concept, T> function;

    // This describes the type of the response, including information about how to extract it from a gRPC message on the
    // client side and how to create a gRPC message containing it on the server side
    private final ConceptResponseType<T> responseType;

    private ConceptMethod(
            Consumer<GrpcConcept.ConceptMethod.Builder> requestBuilder,
            Function<Concept, T> function,
            ConceptResponseType<T> responseType
    ) {
        this.requestBuilder = requestBuilder;
        this.function = function;
        this.responseType = responseType;
    }

    public TxResponse createTxResponse(GrpcIterators iterators, T value) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        buildResponse(conceptResponse, iterators, value);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    public TxResponse run(GrpcIterators iterators, Concept concept) {
        return createTxResponse(iterators, function.apply(concept));
    }

    @Nullable
    public T readResponse(TxConceptReader txConceptReader, GrpcClient client, TxResponse txResponse) {
        ConceptResponse conceptResponse = txResponse.getConceptResponse();
        return responseType.readResponse(txConceptReader, client, conceptResponse);
    }

    public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, T value) {
        responseType.buildResponse(builder, iterators, value);
    }

    public GrpcConcept.ConceptMethod requestBuilder() {
        GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
        requestBuilder.accept(builder);
        return builder.build();
    }

    static <T> Builder<T> builder(ConceptResponseType<T> responseType) {
        return new Builder<>(responseType);
    }

    /**
     * Builder for {@link ConceptMethod}
     */
    static class Builder<T> {

        @Nullable
        private Consumer<GrpcConcept.ConceptMethod.Builder> requestBuilder;

        @Nullable
        private Function<Concept, T> function;

        private final ConceptResponseType<T> responseType;

        private Builder(ConceptResponseType<T> responseType) {
            this.responseType = responseType;
        }

        Builder<T> requestBuilder(Consumer<GrpcConcept.ConceptMethod.Builder> requestBuilder) {
            this.requestBuilder = requestBuilder;
            return this;
        }

        Builder<T> requestBuilderUnit(BiConsumer<GrpcConcept.ConceptMethod.Builder, GrpcConcept.Unit> requestBuilder) {
            this.requestBuilder = builder -> requestBuilder.accept(builder, GrpcConcept.Unit.getDefaultInstance());
            return this;
        }

        public Builder<T> function(Function<Concept, T> function) {
            this.function = function;
            return this;
        }

        public Builder<T> functionVoid(Consumer<Concept> function) {
            this.function = concept -> {
                function.accept(concept);
                return null;
            };
            return this;
        }

        public ConceptMethod<T> build() {
            Preconditions.checkNotNull(requestBuilder);
            Preconditions.checkNotNull(function);
            return new ConceptMethod<>(requestBuilder, function, responseType);
        }
    }
}
