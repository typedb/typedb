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

package ai.grakn.grpc;

import ai.grakn.concept.Concept;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 *
 * <p>
 *     This unifies client and server behaviour for each possible method on a concept.
 * </p>
 *
 * <p>
 *     This class maps one-to-one with the gRPC message {@link ai.grakn.rpc.generated.GrpcConcept.ConceptMethod}.
 * </p>
 *
 * @param <T> The type of the concept method return value.
 * @author Felix Chapman
 */
public final class ConceptMethod<T> {

    // This is the method that is used to create the request on the client-side
    private final Consumer<GrpcConcept.ConceptMethod.Builder> responseSetter;

    // This method is used to extract the value from a Concept
    private final Function<Concept, T> function;

    // This describes the type of the response, including information about how to extract it from a gRPC message on the
    // client side and how to create a gRPC message containing it on the server side
    private final ConceptResponseType<T> responseType;

    private ConceptMethod(
            Consumer<GrpcConcept.ConceptMethod.Builder> responseSetter,
            Function<Concept, T> function,
            ConceptResponseType<T> responseType
    ) {
        this.responseSetter = responseSetter;
        this.function = function;
        this.responseType = responseType;
    }

    public TxResponse createTxResponse(GrpcIterators iterators, T value) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        set(conceptResponse, iterators, value);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    public TxResponse run(GrpcIterators iterators, Concept concept) {
        return createTxResponse(iterators, function.apply(concept));
    }

    @Nullable
    public T get(GrpcConceptConverter conceptConverter, GrpcClient client, TxResponse txResponse) {
        ConceptResponse conceptResponse = txResponse.getConceptResponse();
        return responseType.get(conceptConverter, client, conceptResponse);
    }

    public void set(ConceptResponse.Builder builder, GrpcIterators iterators, T value) {
        responseType.set(builder, iterators, value);
    }

    public GrpcConcept.ConceptMethod toGrpc() {
        GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
        responseSetter.accept(builder);
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
        private Consumer<GrpcConcept.ConceptMethod.Builder> requestSetter;

        @Nullable
        private Function<Concept, T> function;

        private final ConceptResponseType<T> responseType;

        private Builder(ConceptResponseType<T> responseType) {
            this.responseType = responseType;
        }

        Builder<T> requestSetter(Consumer<GrpcConcept.ConceptMethod.Builder> requestSetter) {
            this.requestSetter = requestSetter;
            return this;
        }

        Builder<T> requestSetterUnit(BiConsumer<GrpcConcept.ConceptMethod.Builder, GrpcConcept.Unit> requestSetter) {
            this.requestSetter = builder -> requestSetter.accept(builder, GrpcConcept.Unit.getDefaultInstance());
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
            Preconditions.checkNotNull(requestSetter);
            Preconditions.checkNotNull(function);
            return new ConceptMethod<>(requestSetter, function, responseType);
        }
    }
}
