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
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.grpc.GrpcUtil.convert;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 *
 *
 * @author Felix Chapman
 *
 * @param <T> The type of the concept method return value.
 */
public abstract class ConceptMethod<T> {

    private static final GrpcConcept.Unit UNIT = GrpcConcept.Unit.getDefaultInstance();

    public static final ConceptMethod<Object> GET_VALUE = createA(
            GrpcConcept.ConceptMethod.Builder::setGetValue,
            val -> val.asAttribute().getValue(),
            val -> convert(val.getAttributeValue()),
            (builder, val) -> builder.setAttributeValue(GrpcUtil.convertValue(val))
    );

    public static final ConceptMethod<AttributeType.DataType<?>> GET_DATA_TYPE = createA(
            GrpcConcept.ConceptMethod.Builder::setGetDataType,
            val -> val.isAttribute() ? val.asAttribute().dataType() : val.asAttributeType().getDataType(),
            val -> convert(val.getDataType()),
            (builder, val) -> builder.setDataType(convert(val))
    );

    public static final ConceptMethod<Label> GET_LABEL = createA(
            GrpcConcept.ConceptMethod.Builder::setGetLabel,
            concept -> concept.asSchemaConcept().getLabel(),
            val -> convert(val.getLabel()),
            (builder, val) -> builder.setLabel(convert(val))
    );

    public static final ConceptMethod<Boolean> IS_IMPLICIT = createA(
            GrpcConcept.ConceptMethod.Builder::setIsImplicit,
            concept -> concept.asSchemaConcept().isImplicit(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Boolean> IS_INFERRED = createA(
            GrpcConcept.ConceptMethod.Builder::setIsInferred,
            concept -> concept.asThing().isInferred(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Boolean> IS_ABSTRACT = createA(
            GrpcConcept.ConceptMethod.Builder::setIsAbstract,
            concept -> concept.asType().isAbstract(),
            ConceptResponse::getBool,
            ConceptResponse.Builder::setBool
    );

    public static final ConceptMethod<Pattern> GET_WHEN = createA(
            GrpcConcept.ConceptMethod.Builder::setGetWhen,
            concept -> concept.asRule().getWhen(),
            val -> convert(val.getPattern()),
            (builder, val) -> builder.setPattern(convert(val))
    );

    public static final ConceptMethod<Pattern> GET_THEN = createA(
            GrpcConcept.ConceptMethod.Builder::setGetThen,
            concept -> concept.asRule().getThen(),
            val -> convert(val.getPattern()),
            (builder, val) -> builder.setPattern(convert(val))
    );

    public static final ConceptMethod<String> GET_REGEX = createA(
            GrpcConcept.ConceptMethod.Builder::setGetRegex,
            concept -> concept.asAttributeType().getRegex(),
            ConceptResponse::getString,
            ConceptResponse.Builder::setString
    );

    public static final ConceptMethod<Map<Role, Set<Thing>>> GET_ALL_ROLE_PLAYERS = createB(
            GrpcConcept.ConceptMethod.Builder::setGetAllRolePlayers,
            concept -> concept.asRelationship().allRolePlayers(),
            (converter, val) -> convert(converter, val.getRolePlayers()),
            (builder, val) -> builder.setRolePlayers(convert(val))
    );

    public static final ConceptMethod<Stream<AttributeType>> GET_ATTRIBUTE_TYPES = createB(
            GrpcConcept.ConceptMethod.Builder::setGetAttributeTypes,
            concept -> concept.asType().attributes(),
            (converter, val) -> convert(converter, val.getConcepts()).map(Concept::asAttributeType),
            (builder, val) -> builder.setConcepts(convert(val))
    );

    public static final ConceptMethod<Stream<AttributeType>> GET_KEY_TYPES = createB(
            GrpcConcept.ConceptMethod.Builder::setGetKeyTypes,
            concept -> concept.asType().keys(),
            (converter, val) -> convert(converter, val.getConcepts()).map(Concept::asAttributeType),
            (builder, val) -> builder.setConcepts(convert(val))
    );

    public static final ConceptMethod<Type> GET_DIRECT_TYPE = createB(
            GrpcConcept.ConceptMethod.Builder::setGetDirectType,
            concept -> concept.asThing().type(),
            (converter, val) -> converter.convert(val.getConcept()).asType(),
            (builder, val) -> builder.setConcept(convert(val))
    );

    public static final ConceptMethod<SchemaConcept> GET_DIRECT_SUPER = createB(
            GrpcConcept.ConceptMethod.Builder::setGetDirectSuper,
            concept -> concept.asSchemaConcept().sup(),
            (converter, val) -> converter.convert(val.getConcept()).asSchemaConcept(),
            (builder, val) -> builder.setConcept(convert(val))
    );

    public static ConceptMethod<Void> removeRolePlayer(Role role, Thing player) {
        return createC(
                builder -> builder.setRemoveRolePlayer(convert(role, player)),
                concept -> {
                    concept.asRelationship().removeRolePlayer(role, player);
                    return null;
                },
                (converter, val) -> null,
                (builder, val) -> builder.setUnit(UNIT)
        );
    }

    public static ConceptMethod<?> fromGrpc(GrpcConceptConverter converter, GrpcConcept.ConceptMethod conceptMethod) {
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
            case REMOVEROLEPLAYER:
                GrpcConcept.RolePlayer removeRolePlayer = conceptMethod.getRemoveRolePlayer();
                Role role = converter.convert(removeRolePlayer.getRole()).asRole();
                Thing player = converter.convert(removeRolePlayer.getPlayer()).asThing();
                return removeRolePlayer(role, player);
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
    // TODO: These names are because of an ambiguity FIX
    private static <T> ConceptMethod<T> createA(
            BiConsumer<GrpcConcept.ConceptMethod.Builder, GrpcConcept.Unit> requestSetter,
            Function<Concept, T> conceptGetter,
            Function<ConceptResponse, T> responseGetter,
            BiConsumer<ConceptResponse.Builder, T> responseSetter
    ) {
        return createB(requestSetter, conceptGetter, (conceptConverter, val) -> responseGetter.apply(val), responseSetter);
    }

    private static <T> ConceptMethod<T> createB(
            BiConsumer<GrpcConcept.ConceptMethod.Builder, GrpcConcept.Unit> grpcSetter,
            Function<Concept, T> conceptGetter,
            BiFunction<GrpcConceptConverter, ConceptResponse, T> responseGetter,
            BiConsumer<ConceptResponse.Builder, T> setter
    ) {
        return createC(builder -> grpcSetter.accept(builder, UNIT), conceptGetter, responseGetter, setter);
    }

    private static <T> ConceptMethod<T> createC(
                Consumer<GrpcConcept.ConceptMethod.Builder> grpcSetter,
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
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                grpcSetter.accept(builder);
                return builder.build();
            }
        };
    }
}
