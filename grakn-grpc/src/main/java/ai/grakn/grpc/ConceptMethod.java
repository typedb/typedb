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
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.ConceptResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetAllRolePlayers;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetAttributeTypes;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetDataType;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetDirectSuper;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetDirectType;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetKeyTypes;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetLabel;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetRegex;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetThen;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetValue;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.GetWhen;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.IsAbstract;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.IsImplicit;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptMethod.IsInferred;

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
            GetValue,
            val -> val.asAttribute().getValue(),
            val -> GrpcUtil.convert(val.getAttributeValue()),
            (builder, val) -> builder.setAttributeValue(GrpcUtil.convertValue(val))
    );

    public static final ConceptMethod<AttributeType.DataType<?>> GET_DATA_TYPE = create(
            GetDataType,
            val -> val.isAttribute() ? val.asAttribute().dataType() : val.asAttributeType().getDataType(),
            val -> GrpcUtil.convert(val.getDataType()),
            (builder, val) -> builder.setDataType(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Label> GET_LABEL = create(
            GetLabel,
            concept -> concept.asSchemaConcept().getLabel(),
            val -> GrpcUtil.convert(val.getLabel()),
            (builder, val) -> builder.setLabel(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Boolean> IS_IMPLICIT = create(
            IsImplicit,
            concept -> concept.asSchemaConcept().isImplicit(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Boolean> IS_INFERRED = create(
            IsInferred,
            concept -> concept.asThing().isInferred(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Boolean> IS_ABSTRACT = create(
            IsAbstract,
            concept -> concept.asType().isAbstract(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Pattern> GET_WHEN = create(
            GetWhen,
            concept -> concept.asRule().getWhen(),
            val -> GrpcUtil.convert(val.getPattern()),
            (builder, val) -> builder.setPattern(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Pattern> GET_THEN = create(
            GetThen,
            concept -> concept.asRule().getThen(),
            val -> GrpcUtil.convert(val.getPattern()),
            (builder, val) -> builder.setPattern(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<String> GET_REGEX = create(
            GetRegex,
            concept -> concept.asAttributeType().getRegex(),
            ConceptResponse::getString,
            ConceptResponse.Builder::setString
    );

    public static final ConceptMethod<Map<Role, Set<Thing>>> GET_ALL_ROLE_PLAYERS = create(
            GetAllRolePlayers,
            concept -> concept.asRelationship().allRolePlayers(),
            (converter, val) -> GrpcUtil.convert(converter, val.getRolePlayers()),
            (builder, val) -> builder.setRolePlayers(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Stream<AttributeType>> GET_ATTRIBUTE_TYPES = create(
            GetAttributeTypes,
            concept -> concept.asType().attributes(),
            (converter, val) -> GrpcUtil.convert(converter, val.getConcepts()).map(Concept::asAttributeType),
            (builder, val) -> builder.setConcepts(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Stream<AttributeType>> GET_KEY_TYPES = create(
            GetKeyTypes,
            concept -> concept.asType().keys(),
            (converter, val) -> GrpcUtil.convert(converter, val.getConcepts()).map(Concept::asAttributeType),
            (builder, val) -> builder.setConcepts(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<Type> GET_DIRECT_TYPE = create(
            GetDirectType,
            concept -> concept.asThing().type(),
            (converter, val) -> converter.convert(val.getConcept()).asType(),
            (builder, val) -> builder.setConcept(GrpcUtil.convert(val))
    );

    public static final ConceptMethod<SchemaConcept> GET_DIRECT_SUPER = create(
            GetDirectSuper,
            concept -> concept.asSchemaConcept().sup(),
            (converter, val) -> converter.convert(val.getConcept()).asSchemaConcept(),
            (builder, val) -> builder.setConcept(GrpcUtil.convert(val))
    );

    public static ConceptMethod<?> fromGrpc(GraknOuterClass.ConceptMethod conceptMethod) {
        switch (conceptMethod) {
            case GetValue:
                return GET_VALUE;
            case GetDataType:
                return GET_DATA_TYPE;
            case GetLabel:
                return GET_LABEL;
            case IsImplicit:
                return IS_IMPLICIT;
            case IsInferred:
                return IS_INFERRED;
            case IsAbstract:
                return IS_ABSTRACT;
            case GetWhen:
                return GET_WHEN;
            case GetThen:
                return GET_THEN;
            case GetRegex:
                return GET_REGEX;
            case GetAllRolePlayers:
                return GET_ALL_ROLE_PLAYERS;
            case GetAttributeTypes:
                return GET_ATTRIBUTE_TYPES;
            case GetKeyTypes:
                return GET_KEY_TYPES;
            case GetDirectType:
                return GET_DIRECT_TYPE;
            case GetDirectSuper:
                return GET_DIRECT_SUPER;
            default:
            case UNRECOGNIZED:
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

    abstract GraknOuterClass.ConceptMethod toGrpc();

    /**
     * @param grpcProperty the gRPC {@link GraknOuterClass.ConceptMethod} equivalent of this with the same name
     * @param conceptGetter a method to retrieve a property value from a {@link Concept}
     * @param responseGetter a method to retrieve a property value from inside a gRPC response
     * @param setter a method to set the property value inside a gRPC response
     * @param <T> The type of values of the property
     */
    private static <T> ConceptMethod<T> create(
            GraknOuterClass.ConceptMethod grpcProperty,
            Function<Concept, T> conceptGetter,
            Function<ConceptResponse, T> responseGetter,
            BiConsumer<ConceptResponse.Builder, T> setter
    ) {
        return create(grpcProperty, conceptGetter, (conceptConverter, val) -> responseGetter.apply(val), setter);
    }

    private static <T> ConceptMethod<T> create(
                GraknOuterClass.ConceptMethod grpcProperty,
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
            GraknOuterClass.ConceptMethod toGrpc() {
                return grpcProperty;
            }
        };
    }
}
