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
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptMethod.Builder;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 *
 *
 * @author Felix Chapman
 *
 * @param <T> The type of the concept method return value.
 */
public abstract class ConceptMethod<T> {

    public static final ConceptMethod<Object> GET_VALUE = create(
            GrpcConcept.ConceptMethod.Builder::setGetValue,
            val -> val.asAttribute().getValue(),
            val -> GrpcUtil.convert(val.getAttributeValue()),
            (builder, val) -> builder.setAttributeValue(GrpcUtil.convertValue(val))
    );

    public static final ConceptMethod<AttributeType.DataType<?>> GET_DATA_TYPE = create(
            GrpcConcept.ConceptMethod.Builder::setGetDataType,
            val -> val.isAttribute() ? val.asAttribute().dataType() : val.asAttributeType().getDataType(),
            val -> GrpcUtil.convert(val.getDataType()),
            (builder, val) -> builder.setDataType(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Label> GET_LABEL = create(
            GrpcConcept.ConceptMethod.Builder::setGetLabel,
            concept -> concept.asSchemaConcept().getLabel(),
            val -> GrpcUtil.convert(val.getLabel()),
            (builder, val) -> builder.setLabel(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Boolean> IS_IMPLICIT = create(
            GrpcConcept.ConceptMethod.Builder::setIsImplicit,
            concept -> concept.asSchemaConcept().isImplicit(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Boolean> IS_INFERRED = create(
            GrpcConcept.ConceptMethod.Builder::setIsInferred,
            concept -> concept.asThing().isInferred(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Boolean> IS_ABSTRACT = create(
            GrpcConcept.ConceptMethod.Builder::setIsAbstract,
            concept -> concept.asType().isAbstract(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Pattern> GET_WHEN = create(
            GrpcConcept.ConceptMethod.Builder::setGetWhen,
            concept -> concept.asRule().getWhen(),
            val -> GrpcUtil.convert(val.getPattern()),
            (builder, val) -> builder.setPattern(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Pattern> GET_THEN = create(
            GrpcConcept.ConceptMethod.Builder::setGetThen,
            concept -> concept.asRule().getThen(),
            val -> GrpcUtil.convert(val.getPattern()),
            (builder, val) -> builder.setPattern(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<String> GET_REGEX = create(
            GrpcConcept.ConceptMethod.Builder::setGetRegex,
            concept -> concept.asAttributeType().getRegex(),
            ConceptResponse::getString,
            ConceptResponse.Builder::setString
    );

    public static final ConceptMethod<Map<Role, Set<Thing>>> GET_ALL_ROLE_PLAYERS = create(
            GrpcConcept.ConceptMethod.Builder::setGetAllRolePlayers,
            concept -> concept.asRelationship().allRolePlayers(),
            (converter, val) -> GrpcUtil.convert(converter, val.getRolePlayers()),
            (builder, val) -> builder.setRolePlayers(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Stream<AttributeType>> GET_ATTRIBUTE_TYPES = create(
            GrpcConcept.ConceptMethod.Builder::setGetAttributeTypes,
            concept -> concept.asType().attributes(),
            (converter, val) -> GrpcUtil.convert(converter, val.getConcepts()).map(Concept::asAttributeType),
            (builder, val) -> builder.setConcepts(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Stream<AttributeType>> GET_KEY_TYPES = create(
            GrpcConcept.ConceptMethod.Builder::setGetKeyTypes,
            concept -> concept.asType().keys(),
            (converter, val) -> GrpcUtil.convert(converter, val.getConcepts()).map(Concept::asAttributeType),
            (builder, val) -> builder.setConcepts(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Type> GET_DIRECT_TYPE = create(
            GrpcConcept.ConceptMethod.Builder::setGetDirectType,
            concept -> concept.asThing().type(),
            (converter, val) -> converter.convert(val.getConcept()).asType(),
            (builder, val) -> builder.setConcept(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<SchemaConcept> GET_DIRECT_SUPER = create(
            GrpcConcept.ConceptMethod.Builder::setGetDirectSuper,
            concept -> concept.asSchemaConcept().sup(),
            (converter, val) -> converter.convert(val.getConcept()).asSchemaConcept(),
            (builder, val) -> builder.setConcept(GrpcUtil.convert(val))
    );

    public static ConceptMethod<?> fromGrpc(GrpcConcept.ConceptMethod conceptMethod) {
        switch (conceptMethod.getConceptMethodCase()) {
            case GETVALUE:
                return GET_VALUE;
            case GETDATATYPE:
                return GET_DATA_TYPE;
            case GETLABEL:
                return GET_LABEL;
            case ISIMPLICIT:
                return IS_IMPLICIT;
            case ISINFERRED:
                return IS_INFERRED;
            case ISABSTRACT:
                return IS_ABSTRACT;
            case GETWHEN:
                return GET_WHEN;
            case GETTHEN:
                return GET_THEN;
            case GETREGEX:
                return GET_REGEX;
            case GETALLROLEPLAYERS:
                return GET_ALL_ROLE_PLAYERS;
            case GETATTRIBUTETYPES:
                return GET_ATTRIBUTE_TYPES;
            case GETKEYTYPES:
                return GET_KEY_TYPES;
            case GETDIRECTTYPE:
                return GET_DIRECT_TYPE;
            case GETDIRECTSUPER:
                return GET_DIRECT_SUPER;
            default:
            case CONCEPTMETHOD_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + conceptMethod);
        }
    }

    public final TxResponse createTxResponse(@Nullable T value) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        set(conceptResponse, value);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    public abstract TxResponse run(Concept concept);

    @Nullable
    public abstract T get(GrpcConceptConverter conceptConverter, TxResponse value);

    abstract void set(ConceptResponse.Builder builder, @Nullable T value);

    abstract GrpcConcept.ConceptMethod toGrpc();

    /**
     * @param requestSetter a function to specify this method on a gRPC message
     * @param conceptGetter a method to retrieve a property value from a {@link Concept}
     * @param responseGetter a method to retrieve a property value from inside a gRPC response
     * @param responseSetter a method to set the property value inside a gRPC response
     * @param <T> The type of values of the property
     */
    private static <T> ConceptMethod<T> create(
            BiConsumer<GrpcConcept.ConceptMethod.Builder, GrpcConcept.Unit> requestSetter,
            Function<Concept, T> conceptGetter,
            Function<ConceptResponse, T> responseGetter,
            BiConsumer<ConceptResponse.Builder, T> responseSetter
    ) {
        return create(requestSetter, conceptGetter, (conceptConverter, val) -> responseGetter.apply(val), responseSetter);
    }

    private static <T> ConceptMethod<T> create(
                BiConsumer<GrpcConcept.ConceptMethod.Builder, GrpcConcept.Unit> grpcSetter,
                Function<Concept, T> conceptGetter,
                BiFunction<GrpcConceptConverter, ConceptResponse, T> responseGetter,
                BiConsumer<ConceptResponse.Builder, T> setter
    ) {
        return new ConceptMethod<T>() {
            @Override
            public TxResponse run(Concept concept) {
                return createTxResponse(conceptGetter.apply(concept));
            }

            @Nullable
            @Override
            public T get(GrpcConceptConverter conceptConverter, TxResponse txResponse) {
                ConceptResponse conceptResponse = txResponse.getConceptResponse();
                if (conceptResponse.getValueCase().equals(ConceptResponse.ValueCase.VALUE_NOT_SET)) {
                    return null;
                } else {
                    return responseGetter.apply(conceptConverter, conceptResponse);
                }
            }

            @Override
            void set(ConceptResponse.Builder builder, @Nullable T value) {
                if (value != null) {
                    setter.accept(builder, value);
                }
            }

            @Override
            GrpcConcept.ConceptMethod toGrpc() {
                Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                grpcSetter.accept(builder, GrpcConcept.Unit.getDefaultInstance());
                return builder.build();
            }
        };
    }
}
