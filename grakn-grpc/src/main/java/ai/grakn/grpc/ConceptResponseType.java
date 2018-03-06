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

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Wrapper around the different types of responses to {@link ConceptMethod}s applied on {@link Concept}s.
 *
 * @author Felix Chapman
 */
abstract class ConceptResponseType<T> {

    public static final ConceptResponseType<Boolean> BOOL =
            ConceptResponseType.create(ConceptResponse::getBool, ConceptResponse.Builder::setBool);

    public static final ConceptResponseType<Pattern> PATTERN = ConceptResponseType.create(
            response -> GrpcUtil.convert(response.getPattern()),
            (builder, val) -> builder.setPattern(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<Concept> CONCEPT = ConceptResponseType.create(
            (converter, response) -> converter.convert(response.getConcept()),
            (builder, val) -> builder.setConcept(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<Stream<? extends Concept>> CONCEPTS = ConceptResponseType.create(
            (converter, response) -> GrpcUtil.convert(converter, response.getConcepts()),
            (builder, val) -> builder.setConcepts(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<Map<Role, Set<Thing>>> ROLE_PLAYERS = ConceptResponseType.create(
            (converter, response) -> GrpcUtil.convert(converter, response.getRolePlayers()),
            (builder, val) -> builder.setRolePlayers(GrpcUtil.convert(val))
    );

    public static final ConceptResponseType<AttributeType.DataType<?>> DATA_TYPE = ConceptResponseType.create(
            response -> GrpcUtil.convert(response.getDataType()),
            (builder, val) -> builder.setDataType(GrpcUtil.convert(val))
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

    public abstract T get(GrpcConceptConverter converter , ConceptResponse conceptResponse);

    public abstract void set(ConceptResponse.Builder builder, T value);

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
        return new ConceptResponseType<T>() {
            @Override
            public T get(GrpcConceptConverter converter, ConceptResponse conceptResponse) {
                return getter.apply(converter, conceptResponse);
            }

            @Override
            public void set(ConceptResponse.Builder builder, T value) {
                setter.accept(builder, value);
            }
        };
    }
}
