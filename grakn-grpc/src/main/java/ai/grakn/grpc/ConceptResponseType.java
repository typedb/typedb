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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.grpc;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.AbstractIterator;
import org.apache.tinkerpop.gremlin.util.function.TriConsumer;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wrapper around the different types of responses to {@link ConceptMethod}s applied on {@link Concept}s.
 *
 * @author Felix Chapman
 */
abstract class ConceptResponseType<T> {

    public static final ConceptResponseType<Boolean> BOOL =
            ConceptResponseType.create(ConceptResponse::getBool, ConceptResponse.Builder::setBool);

    public static final ConceptResponseType<Optional<Pattern>> OPTIONAL_PATTERN = ConceptResponseType.create(
            response -> GrpcUtil.convert(response.getOptionalPattern()),
            (builder, val) -> builder.setOptionalPattern(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<Concept> CONCEPT = ConceptResponseType.create(
            (converter, response) -> converter.convert(response.getConcept()),
            (builder, val) -> builder.setConcept(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<Optional<Concept>> OPTIONAL_CONCEPT = ConceptResponseType.create(
            (converter, response) -> converter.convert(response.getOptionalConcept()),
            (builder, val) -> builder.setOptionalConcept(GrpcUtil.convertOptionalConcept(val))
    );

    public static final ConceptResponseType<Stream<? extends Concept>> CONCEPTS = ConceptResponseType.create(
            (converter, client, response) -> {
                IteratorId iteratorId = response.getIteratorId();

                Iterable<Concept> iterable = () -> new AbstractIterator<Concept>() {
                    @Override
                    protected Concept computeNext() {
                        TxResponse response = client.next(iteratorId);

                        switch (response.getResponseCase()) {
                            case CONCEPT:
                                return converter.convert(response.getConcept());
                            case DONE:
                                return endOfData();
                            default:
                            case RESPONSE_NOT_SET:
                                throw CommonUtil.unreachableStatement("Unexpected " + response);
                        }
                    }
                };

                return StreamSupport.stream(iterable.spliterator(), false);
            },
            (builder, iterators, val) -> {
                Iterator<TxResponse> iterator = val.map(GrpcUtil::conceptResponse).iterator();
                IteratorId iteratorId = iterators.add(iterator);
                builder.setIteratorId(iteratorId);
            }
    );

    public static final ConceptResponseType<Map<Role, Set<Thing>>> ROLE_PLAYERS = ConceptResponseType.create(
            (converter, response) -> GrpcUtil.convert(converter, response.getRolePlayers()),
            (builder, val) -> builder.setRolePlayers(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<AttributeType.DataType<?>> DATA_TYPE = ConceptResponseType.create(
            response -> GrpcUtil.convert(response.getDataType()),
            (builder, val) -> builder.setDataType(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<Optional<AttributeType.DataType<?>>> OPTIONAL_DATA_TYPE =
            ConceptResponseType.create(
                    response -> GrpcUtil.convert(response.getOptionalDataType()),
                    (builder, val) -> builder.setOptionalDataType(GrpcUtil.convertOptionalDataType(val))
            );

    public static final ConceptResponseType<Object> ATTRIBUTE_VALUE = ConceptResponseType.create(
            response -> GrpcUtil.convert(response.getAttributeValue()),
            (builder, val) -> builder.setAttributeValue(GrpcUtil.convertValue(val))
    );

    public static final ConceptResponseType<Label> LABEL = ConceptResponseType.create(
            response -> GrpcUtil.convert(response.getLabel()),
            (builder, val) -> builder.setLabel(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<String> STRING =
            ConceptResponseType.create(ConceptResponse::getString, ConceptResponse.Builder::setString);

    public static final ConceptResponseType<Void> UNIT = ConceptResponseType.create(
            response -> null,
            (builder, val) -> builder.setUnit(GrpcConcept.Unit.getDefaultInstance())
    );

    public static final ConceptResponseType<Optional<String>> OPTIONAL_REGEX = ConceptResponseType.create(
            response -> GrpcUtil.convert(response.getOptionalRegex()),
            (builder, val) -> builder.setOptionalRegex(GrpcUtil.convertRegex(val))
    );

    @Nullable
    public abstract T get(GrpcConceptConverter converter, GrpcClient client, ConceptResponse conceptResponse);

    public abstract void set(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable T value);

    public static <T> ConceptResponseType<T> create(
            Function<ConceptResponse, T> getter,
            BiConsumer<ConceptResponse.Builder, T> setter
    ) {
        return create((converter, response) -> getter.apply(response), setter);
    }

    public static <T> ConceptResponseType<T> create(
            BiFunction<GrpcConceptConverter, ConceptResponse, T> getter,
            BiConsumer<ConceptResponse.Builder, T> setter
    ) {
        return create(
                (converter, client, response) -> getter.apply(converter, response),
                (builder, iterators, val) -> setter.accept(builder, val)
        );
    }

    public static <T> ConceptResponseType<T> create(
            TriFunction<GrpcConceptConverter, GrpcClient, ConceptResponse, T> getter,
            TriConsumer<ConceptResponse.Builder, GrpcIterators, T> setter
    ) {
        return new ConceptResponseType<T>() {
            @Override
            public T get(GrpcConceptConverter converter, GrpcClient client, ConceptResponse conceptResponse) {
                return getter.apply(converter, client, conceptResponse);
            }

            @Override
            public void set(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable T value) {
                setter.accept(builder, iterators, value);
            }
        };
    }
}
