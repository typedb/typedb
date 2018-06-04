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

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wrapper around the different types of responses to {@link ConceptMethod}s applied on {@link Concept}s.
 *
 * @param <T> return type of responses
 * @author Grakn Warriors
 */
public abstract class ConceptResponseType<T> {

    @Nullable
    public abstract T readResponse(TxConceptReader converter, GrpcClient client, ConceptResponse conceptResponse);

    public abstract void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable T value);

    public static final ConceptResponseType<Stream<? extends Concept>> CONCEPTS = new ConceptResponseType<Stream<? extends Concept>>() {
        @Override
        public Stream<? extends Concept> readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            IteratorId iteratorId = conceptResponse.getIteratorId();
            Iterable<? extends Concept> iterable = () -> new ResponseIterator<>(client, iteratorId, response -> txConceptReader.concept(response.getConcept()));

            return StreamSupport.stream(iterable.spliterator(), false);
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Stream<? extends Concept> value) {
            Stream<TxResponse> responses = value.map(ResponseBuilder::concept);
            IteratorId iteratorId = iterators.add(responses.iterator());
            builder.setIteratorId(iteratorId);
        }
    };

    public static final ConceptResponseType<Stream<RolePlayer>> ROLE_PLAYERS = new ConceptResponseType<Stream<RolePlayer>>() {
        @Override
        public Stream<RolePlayer> readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            IteratorId iteratorId = conceptResponse.getIteratorId();
            Iterable<RolePlayer> iterable = () -> new ResponseIterator<>(client, iteratorId, response -> txConceptReader.rolePlayer(response.getRolePlayer()));

            return StreamSupport.stream(iterable.spliterator(), false);
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Stream<RolePlayer> value) {
            Stream<TxResponse> responses = value.map(ResponseBuilder::rolePlayer);
            IteratorId iteratorId = iterators.add(responses.iterator());
            builder.setIteratorId(iteratorId);
        }
    };

    public static final ConceptResponseType<Boolean> BOOL = new ConceptResponseType<Boolean>() {
        @Override
        public Boolean readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return conceptResponse.getBool();
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Boolean value) {
            builder.setBool(value);
        }
    };

    public static final ConceptResponseType<Optional<Pattern>> OPTIONAL_PATTERN = new ConceptResponseType<Optional<Pattern>>() {
        @Override
        public Optional<Pattern> readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return ConceptReader.optionalPattern(conceptResponse.getOptionalPattern());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Optional<Pattern> value) {
            builder.setOptionalPattern(ConceptBuilder.optionalPattern(value));
        }
    };

    public static final ConceptResponseType<Concept> CONCEPT = new ConceptResponseType<Concept>() {
        @Override
        public Concept readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return txConceptReader.concept(conceptResponse.getConcept());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Concept value) {
            builder.setConcept(ConceptBuilder.concept(value));
        }
    };

    public static final ConceptResponseType<Optional<Concept>> OPTIONAL_CONCEPT = new ConceptResponseType<Optional<Concept>>() {
        @Override
        public Optional<Concept> readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return txConceptReader.optionalConcept(conceptResponse.getOptionalConcept());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Optional<Concept> value) {
            builder.setOptionalConcept(ConceptBuilder.optionalConcept(value));
        }
    };


    public static final ConceptResponseType<AttributeType.DataType<?>> DATA_TYPE = new ConceptResponseType<AttributeType.DataType<?>>() {
        @Override
        public AttributeType.DataType<?> readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return ConceptReader.dataType(conceptResponse.getDataType());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable AttributeType.DataType<?> value) {
            builder.setDataType(ConceptBuilder.dataType(value));
        }
    };

    public static final ConceptResponseType<Optional<AttributeType.DataType<?>>> OPTIONAL_DATA_TYPE = new ConceptResponseType<Optional<AttributeType.DataType<?>>>() {
        @Override
        public Optional<AttributeType.DataType<?>> readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return ConceptReader.optionalDataType(conceptResponse.getOptionalDataType());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Optional<AttributeType.DataType<?>> value) {
            builder.setOptionalDataType(ResponseBuilder.optionalDataType(value));
        }
    };

    public static final ConceptResponseType<Object> ATTRIBUTE_VALUE = new ConceptResponseType<Object>() {
        @Override
        public Object readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return ConceptReader.attributeValue(conceptResponse.getAttributeValue());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Object value) {
            builder.setAttributeValue(ConceptBuilder.attributeValue(value));
        }
    };

    public static final ConceptResponseType<Label> LABEL = new ConceptResponseType<Label>() {
        @Override
        public Label readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return ConceptReader.label(conceptResponse.getLabel());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Label value) {
            builder.setLabel(ConceptBuilder.label(value));
        }
    };

    public static final ConceptResponseType<String> STRING = new ConceptResponseType<String>() {
        @Override
        public String readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return conceptResponse.getString();
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable String value) {
            builder.setString(value);
        }
    };

    public static final ConceptResponseType<Void> UNIT = new ConceptResponseType<Void>() {
        @Override
        public Void readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return null;
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Void value) {
            builder.setUnit(GrpcConcept.Unit.getDefaultInstance());
        }
    };

    public static final ConceptResponseType<Optional<String>> OPTIONAL_REGEX = new ConceptResponseType<Optional<String>>() {
        @Override
        public Optional<String> readResponse(TxConceptReader txConceptReader, GrpcClient client, ConceptResponse conceptResponse) {
            return ConceptReader.optionalRegex(conceptResponse.getOptionalRegex());
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, @Nullable Optional<String> value) {
            builder.setOptionalRegex(ConceptBuilder.optionalRegex(value));
        }
    };
}
