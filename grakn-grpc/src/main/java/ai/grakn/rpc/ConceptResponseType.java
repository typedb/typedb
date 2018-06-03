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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ConceptReader;
import ai.grakn.rpc.util.ResponseBuilder;
import ai.grakn.rpc.util.TxConceptReader;
import org.apache.tinkerpop.gremlin.util.function.TriConsumer;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wrapper around the different types of responses to {@link ConceptMethod}s applied on {@link Concept}s.
 *
 * @param <T> return type of responses
 * @author Felix Chapman
 */
public abstract class ConceptResponseType<T> {

    public static final ConceptResponseType<Boolean> BOOL =
            ConceptResponseType.create(ConceptResponse::getBool, ConceptResponse.Builder::setBool);

    public static final ConceptResponseType<Optional<Pattern>> OPTIONAL_PATTERN = ConceptResponseType.create(
            response -> ConceptReader.optionalPattern(response.getOptionalPattern()),
            (builder, val) -> builder.setOptionalPattern(ConceptBuilder.optionalPattern(val))
    );

    public static final ConceptResponseType<Concept> CONCEPT = ConceptResponseType.create(
            (converter, response) -> converter.concept(response.getConcept()),
            (builder, val) -> builder.setConcept(ConceptBuilder.concept(val))
    );

    public static final ConceptResponseType<Optional<Concept>> OPTIONAL_CONCEPT = ConceptResponseType.create(
            (converter, response) -> converter.optionalConcept(response.getOptionalConcept()),
            (builder, val) -> builder.setOptionalConcept(ConceptBuilder.optionalConcept(val))
    );

    public static final ConceptResponseType<Stream<? extends Concept>> CONCEPTS =
            ConceptResponseType.createStreamable(
                    (converter, response) -> converter.concept(response.getConcept()),
                    ResponseBuilder::concept
            );

    public static final ConceptResponseType<Stream<? extends RolePlayer>> ROLE_PLAYERS =
            ConceptResponseType.createStreamable(
                    (converter, response) -> converter.rolePlayer(response.getRolePlayer()),
                    ResponseBuilder::rolePlayer
            );

    public static final ConceptResponseType<AttributeType.DataType<?>> DATA_TYPE = ConceptResponseType.create(
            response -> ConceptReader.dataType(response.getDataType()),
            (builder, val) -> builder.setDataType(ConceptBuilder.dataType(val))
    );

    public static final ConceptResponseType<Optional<AttributeType.DataType<?>>> OPTIONAL_DATA_TYPE =
            ConceptResponseType.create(
                    response -> ConceptReader.optionalDataType(response.getOptionalDataType()),
                    (builder, val) -> builder.setOptionalDataType(ResponseBuilder.optionalDataType(val))
            );

    public static final ConceptResponseType<Object> ATTRIBUTE_VALUE = ConceptResponseType.create(
            response -> ConceptReader.attributeValue(response.getAttributeValue()),
            (builder, val) -> builder.setAttributeValue(ConceptBuilder.attributeValue(val))
    );

    public static final ConceptResponseType<Label> LABEL = ConceptResponseType.create(
            response -> ConceptReader.label(response.getLabel()),
            (builder, val) -> builder.setLabel(ConceptBuilder.label(val))
    );

    public static final ConceptResponseType<String> STRING =
            ConceptResponseType.create(ConceptResponse::getString, ConceptResponse.Builder::setString);

    public static final ConceptResponseType<Void> UNIT = ConceptResponseType.create(
            response -> null,
            (builder, val) -> builder.setUnit(GrpcConcept.Unit.getDefaultInstance())
    );

    public static final ConceptResponseType<Optional<String>> OPTIONAL_REGEX = ConceptResponseType.create(
            response -> ConceptReader.optionalRegex(response.getOptionalRegex()),
            (builder, val) -> builder.setOptionalRegex(ConceptBuilder.optionalRegex(val))
    );

    @Nullable
    public abstract T get(TxConceptReader converter, GrpcClient client, ConceptResponse conceptResponse);

    public abstract void set(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable T value);

    public static <T> ConceptResponseType<T> create(
            Function<ConceptResponse, T> getter,
            BiConsumer<ConceptResponse.Builder, T> setter
    ) {
        return create((converter, response) -> getter.apply(response), setter);
    }

    public static <T> ConceptResponseType<T> create(
            BiFunction<TxConceptReader, ConceptResponse, T> getter,
            BiConsumer<ConceptResponse.Builder, T> setter
    ) {
        return create(
                (converter, client, response) -> getter.apply(converter, response),
                (builder, iterators, val) -> setter.accept(builder, val)
        );
    }

    public static <T> ConceptResponseType<Stream<? extends T>> createStreamable(
            BiFunction<TxConceptReader, TxResponse, T> getter,
            Function<T, TxResponse> setter
    ) {
        return create(
                (converter, client, response) -> {
                    IteratorId iteratorId = response.getIteratorId();

                    Iterable<T> iterable = () -> new GraknGrpcIterator<T>(client, iteratorId) {
                        @Override
                        protected T getNextFromResponse(TxResponse response) {
                            return getter.apply(converter, response);
                        }
                    };

                    return StreamSupport.stream(iterable.spliterator(), false);
                },
                (builder, iterators, val) -> {
                    Stream<TxResponse> responses = val.map(setter);
                    IteratorId iteratorId = iterators.add(responses.iterator());
                    builder.setIteratorId(iteratorId);
                }
        );
    }

    public static <T> ConceptResponseType<T> create(
            TriFunction<TxConceptReader, GrpcClient, ConceptResponse, T> getter,
            TriConsumer<ConceptResponse.Builder, GrpcIterators, T> setter
    ) {
        return new ConceptResponseType<T>() {
            @Override
            public T get(TxConceptReader converter, GrpcClient client, ConceptResponse conceptResponse) {
                return getter.apply(converter, client, conceptResponse);
            }

            @Override
            public void set(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable T value) {
                setter.accept(builder, iterators, value);
            }
        };
    }
}
