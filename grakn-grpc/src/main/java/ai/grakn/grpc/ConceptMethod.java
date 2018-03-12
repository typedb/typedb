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
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.grpc.GrpcUtil.convert;
import static ai.grakn.grpc.GrpcUtil.convertValue;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 *
 * @param <T> The type of the concept method return value.
 * @author Felix Chapman
 */
public final class ConceptMethod<T> {

    private final Consumer<GrpcConcept.ConceptMethod.Builder> responseSetter;
    private final Function<Concept, T> function;
    private final ConceptResponseType<T> responseType;

    public TxResponse createTxResponse(@Nullable T value) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        set(conceptResponse, value);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    public TxResponse run(@Nullable Concept concept) {
        return createTxResponse(function.apply(concept));
    }

    @Nullable
    public T get(GrpcConceptConverter conceptConverter, TxResponse txResponse) {
        ConceptResponse conceptResponse = txResponse.getConceptResponse();
        if (conceptResponse.getValueCase().equals(ConceptResponse.ValueCase.VALUE_NOT_SET)) {
            return null;
        } else {
            return responseType.get(conceptConverter, conceptResponse);
        }
    }

    public void set(ConceptResponse.Builder builder, @Nullable T value) {
        if (value != null) {
            responseType.set(builder, value);
        }
    }

    public GrpcConcept.ConceptMethod toGrpc() {
        GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
        responseSetter.accept(builder);
        return builder.build();
    }

    public static final ConceptMethod<Object> GET_VALUE = builder(ConceptResponseType.ATTRIBUTE_VALUE)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetValue)
            .function(val -> val.asAttribute().getValue())
            .build();

    public static final ConceptMethod<AttributeType.DataType<?>> GET_DATA_TYPE = builder(ConceptResponseType.DATA_TYPE)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetDataType)
            .function(val -> val.isAttribute() ? val.asAttribute().dataType() : val.asAttributeType().getDataType())
            .build();

    public static final ConceptMethod<Label> GET_LABEL = builder(ConceptResponseType.LABEL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetLabel)
            .function(concept -> concept.asSchemaConcept().getLabel())
            .build();

    public static final ConceptMethod<Boolean> IS_IMPLICIT = builder(ConceptResponseType.BOOL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setIsImplicit)
            .function(concept -> concept.asSchemaConcept().isImplicit())
            .build();

    public static final ConceptMethod<Boolean> IS_INFERRED = builder(ConceptResponseType.BOOL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setIsInferred)
            .function(concept -> concept.asThing().isInferred())
            .build();

    public static final ConceptMethod<Boolean> IS_ABSTRACT = builder(ConceptResponseType.BOOL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setIsAbstract)
            .function(concept -> concept.asType().isAbstract())
            .build();

    public static final ConceptMethod<Pattern> GET_WHEN = builder(ConceptResponseType.PATTERN)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetWhen)
            .function(concept -> concept.asRule().getWhen())
            .build();

    public static final ConceptMethod<Pattern> GET_THEN = builder(ConceptResponseType.PATTERN)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetThen)
            .function(concept -> concept.asRule().getThen())
            .build();

    public static final ConceptMethod<String> GET_REGEX = builder(ConceptResponseType.STRING)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRegex)
            .function(concept -> concept.asAttributeType().getRegex())
            .build();

    public static final ConceptMethod<Map<Role, Set<Thing>>> GET_ROLE_PLAYERS =
            builder(ConceptResponseType.ROLE_PLAYERS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRolePlayers)
                    .function(concept -> concept.asRelationship().allRolePlayers())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_ATTRIBUTE_TYPES =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetAttributeTypes)
                    .function(concept -> concept.asType().attributes())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_KEY_TYPES =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetKeyTypes)
                    .function(concept -> concept.asType().keys())
                    .build();

    public static final ConceptMethod<Concept> GET_DIRECT_TYPE =
            builder(ConceptResponseType.CONCEPT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetDirectType)
                    .function(concept -> concept.asThing().type())
                    .build();

    public static final ConceptMethod<Concept> GET_DIRECT_SUPER =
            builder(ConceptResponseType.CONCEPT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetDirectSuperConcept)
                    .function(concept -> concept.asSchemaConcept().sup())
                    .build();

    public static ConceptMethod<Void> removeRolePlayer(Role role, Thing player) {
        return builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setUnsetRolePlayer(convert(role, player)))
                .function(concept -> {
                    concept.asRelationship().removeRolePlayer(role, player);
                    return null;
                })
                .build();
    }

    public static final ConceptMethod<Void> DELETE =
            builder(ConceptResponseType.UNIT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setDelete)
                    .function(concept -> {
                        concept.delete();
                        return null;
                    })
                    .build();

    public static ConceptMethod<Concept> getAttribute(Object value) {
        return builder(ConceptResponseType.CONCEPT)
                .requestSetter(builder -> builder.setGetAttribute(convertValue(value)))
                .function(concept -> concept.asAttributeType().getAttribute(value))
                .build();
    }

    public static final ConceptMethod<Boolean> EXISTS =
            builder(ConceptResponseType.BOOL)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setExists)
                    .function(Concept::isDeleted)
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_SUPER_CONCEPTS =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetSuperConcepts)
                    .function(concept -> concept.asSchemaConcept().sups())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_SUB_CONCEPTS =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetSubConcepts)
                    .function(concept -> concept.asSchemaConcept().subs())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_ATTRIBUTES =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetAttributes)
                    .function(concept -> concept.asThing().attributes())
                    .build();

    public static ConceptMethod<Stream<? extends Concept>> getAttributesByTypes(AttributeType<?>... attributeTypes) {
        return builder(ConceptResponseType.CONCEPTS)
                .requestSetter(builder -> builder.setGetAttributesByTypes(convert(Stream.of(attributeTypes))))
                .function(concept -> concept.asThing().attributes(attributeTypes))
                .build();
    }

    public static final ConceptMethod<Stream<? extends Concept>> GET_ROLES_PLAYED_BY_TYPE =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRolesPlayedByType)
                    .function(concept -> concept.asType().plays())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_INSTANCES =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetInstances)
                    .function(concept -> concept.asType().instances())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_ROLES_PLAYED_BY_THING =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRolesPlayedByThing)
                    .function(concept -> concept.asThing().plays())
                    .build();

    public static ConceptMethod<Stream<? extends Concept>> getRolePlayersByRoles(Role... roles) {
        return builder(ConceptResponseType.CONCEPTS)
                .requestSetter(builder -> builder.setGetRolePlayersByRoles(convert(Stream.of(roles))))
                .function(concept -> concept.asRelationship().rolePlayers(roles))
                .build();
    }

    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATIONSHIPS =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRelationships)
                    .function(concept -> concept.asThing().relationships())
                    .build();

    public static ConceptMethod<Stream<? extends Concept>> getRelationshipsByRoles(Role... roles) {
        return builder(ConceptResponseType.CONCEPTS)
                .requestSetter(builder -> builder.setGetRelationshipsByRoles(convert(Stream.of(roles))))
                .function(concept -> concept.asThing().relationships(roles))
                .build();
    }

    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATIONSHIP_TYPES_THAT_RELATE_ROLE =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRelationshipTypesThatRelateRole)
                    .function(concept -> concept.asRole().relationshipTypes())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_TYPES_THAT_PLAY_ROLE =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetTypesThatPlayRole)
                    .function(concept -> concept.asRole().playedByTypes())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATED_ROLES =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRelatedRoles)
                    .function(concept -> concept.asRelationshipType().relates())
                    .build();

    public static final ConceptMethod<Stream<? extends Concept>> GET_OWNERS =
            builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetOwners)
                    .function(concept -> concept.asAttribute().ownerInstances())
                    .build();

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
            case GETROLEPLAYERS:
                return GET_ROLE_PLAYERS;
            case GETATTRIBUTETYPES:
                return GET_ATTRIBUTE_TYPES;
            case GETKEYTYPES:
                return GET_KEY_TYPES;
            case GETDIRECTTYPE:
                return GET_DIRECT_TYPE;
            case GETDIRECTSUPERCONCEPT:
                return GET_DIRECT_SUPER;
            case UNSETROLEPLAYER:
                GrpcConcept.RolePlayer removeRolePlayer = conceptMethod.getUnsetRolePlayer();
                Role role = converter.convert(removeRolePlayer.getRole()).asRole();
                Thing player = converter.convert(removeRolePlayer.getPlayer()).asThing();
                return removeRolePlayer(role, player);
            case DELETE:
                return DELETE;
            case GETATTRIBUTE:
                return getAttribute(convert(conceptMethod.getGetAttribute()));
            case EXISTS:
                return EXISTS;
            case GETOWNERS:
                return GET_OWNERS;
            case GETTYPESTHATPLAYROLE:
                return GET_TYPES_THAT_PLAY_ROLE;
            case GETROLESPLAYEDBYTYPE:
                return GET_ROLES_PLAYED_BY_TYPE;
            case GETINSTANCES:
                return GET_INSTANCES;
            case GETRELATEDROLES:
                return GET_RELATED_ROLES;
            case GETATTRIBUTES:
                return GET_ATTRIBUTES;
            case GETSUPERCONCEPTS:
                return GET_SUPER_CONCEPTS;
            case GETRELATIONSHIPTYPESTHATRELATEROLE:
                return GET_RELATIONSHIP_TYPES_THAT_RELATE_ROLE;
            case GETATTRIBUTESBYTYPES:
                GrpcConcept.Concepts getAttributeTypes = conceptMethod.getGetAttributesByTypes();
                AttributeType<?>[] attributeTypes = convert(converter, getAttributeTypes).toArray(AttributeType[]::new);
                return getAttributesByTypes(attributeTypes);
            case GETRELATIONSHIPS:
                return GET_RELATIONSHIPS;
            default:
            case CONCEPTMETHOD_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + conceptMethod);
        }
    }

    private ConceptMethod(
            Consumer<GrpcConcept.ConceptMethod.Builder> responseSetter,
            Function<Concept, T> function,
            ConceptResponseType<T> responseType
    ) {
        this.responseSetter = responseSetter;
        this.function = function;
        this.responseType = responseType;
    }

    private static <T> Builder<T> builder(ConceptResponseType<T> responseType) {
        return new Builder<>(responseType);
    }

    /**
     * Builder for {@link ConceptMethod}
     */
    private static class Builder<T> {

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

        public ConceptMethod<T> build() {
            Preconditions.checkNotNull(requestSetter);
            Preconditions.checkNotNull(function);
            return new ConceptMethod<>(requestSetter, function, responseType);
        }
    }
}
