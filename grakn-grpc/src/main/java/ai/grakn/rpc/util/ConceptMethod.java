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

package ai.grakn.rpc.util;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.GrpcClient;
import ai.grakn.rpc.GrpcIterators;
import ai.grakn.rpc.ResponseIterator;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wrapper for describing methods on {@link Concept}s that can be executed over gRPC.
 * This unifies client and server behaviour for each possible method on a concept.
 * This class maps one-to-one with the gRPC message {@link ai.grakn.rpc.generated.GrpcConcept.ConceptMethod}.
 *
 * @param <T> The type of the concept method return value.
 */
public abstract class ConceptMethod<T> {

    // Client: RemoteGraknSession, RemoteGraknTx
    // RPC: RequestBuilder.runConceptMethod()
    public GrpcConcept.ConceptMethod requestBuilder() {
        return null;
    }

    @Nullable // Client: GrpcClient
    public T readResponse(TxConceptReader txConceptReader, GrpcClient client, TxResponse txResponse)  {
        return null;
    }

    // Server: TxQueryListener.runConceptMethod()
    public abstract TxResponse run(GrpcIterators iterators, Concept concept);

    // ^-- Server: ConceptMethod.run()
    public TxResponse createTxResponse(GrpcIterators iterators, T response) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        buildResponse(conceptResponse, iterators, response);
        return TxResponse.newBuilder().setConceptResponse(conceptResponse.build()).build();
    }

    // ^-- Server: ^ ConceptMethod.createTxResponse()
    public abstract void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, T value);

    // Server: TxRequestLister.runConceptMethod()
    public static ConceptMethod<?> requestReader(TxConceptReader converter, GrpcConcept.ConceptMethod conceptMethod) {
        Role[] roles;

        switch (conceptMethod.getConceptMethodCase()) {
            case GETVALUE:
                return GET_VALUE;
            case GETDATATYPEOFTYPE:
                return GET_DATA_TYPE_OF_TYPE;
            case GETDATATYPEOFATTRIBUTE:
                return GET_DATA_TYPE_OF_ATTRIBUTE;
            case GETLABEL:
                return GET_LABEL;
            case SETLABEL:
                return setLabel(ConceptReader.label(conceptMethod.getSetLabel()));
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
            case SETATTRIBUTETYPE:
                return setAttributeType(converter.concept(conceptMethod.getSetAttributeType()).asAttributeType());
            case UNSETATTRIBUTETYPE:
                return unsetAttributeType(converter.concept(conceptMethod.getUnsetAttributeType()).asAttributeType());
            case GETKEYTYPES:
                return GET_KEY_TYPES;
            case GETDIRECTTYPE:
                return GET_DIRECT_TYPE;
            case GETDIRECTSUPERCONCEPT:
                return GET_DIRECT_SUPER;
            case SETDIRECTSUPERCONCEPT:
                GrpcConcept.Concept setDirectSuperConcept = conceptMethod.getSetDirectSuperConcept();
                SchemaConcept schemaConcept = converter.concept(setDirectSuperConcept).asSchemaConcept();
                return setDirectSuperConcept(schemaConcept);
            case UNSETROLEPLAYER:
                return removeRolePlayer(converter.rolePlayer(conceptMethod.getUnsetRolePlayer()));
            case DELETE:
                return DELETE;
            case GETATTRIBUTE:
                return getAttribute(ConceptReader.attributeValue(conceptMethod.getGetAttribute()));
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
                AttributeType<?>[] attributeTypes = ConceptReader.concepts(converter, getAttributeTypes).toArray(AttributeType[]::new);
                return getAttributesByTypes(attributeTypes);
            case GETRELATIONSHIPS:
                return GET_RELATIONSHIPS;
            case GETSUBCONCEPTS:
                return GET_SUB_CONCEPTS;
            case GETRELATIONSHIPSBYROLES:
                roles = ConceptReader.concepts(converter, conceptMethod.getGetRelationshipsByRoles()).toArray(Role[]::new);
                return getRelationshipsByRoles(roles);
            case GETROLESPLAYEDBYTHING:
                return GET_ROLES_PLAYED_BY_THING;
            case GETKEYS:
                return GET_KEYS;
            case GETKEYSBYTYPES:
                GrpcConcept.Concepts getKeyTypes = conceptMethod.getGetAttributesByTypes();
                AttributeType<?>[] keyTypes = ConceptReader.concepts(converter, getKeyTypes).toArray(AttributeType[]::new);
                return getKeysByTypes(keyTypes);
            case GETROLEPLAYERSBYROLES:
                roles = ConceptReader.concepts(converter, conceptMethod.getGetRolePlayersByRoles()).toArray(Role[]::new);
                return getRolePlayersByRoles(roles);
            case SETKEYTYPE:
                return setKeyType(converter.concept(conceptMethod.getSetKeyType()).asAttributeType());
            case UNSETKEYTYPE:
                return unsetKeyType(converter.concept(conceptMethod.getUnsetKeyType()).asAttributeType());
            case SETABSTRACT:
                return setAbstract(conceptMethod.getSetAbstract());
            case SETROLEPLAYEDBYTYPE:
                return setRolePlayedByType(converter.concept(conceptMethod.getSetRolePlayedByType()).asRole());
            case UNSETROLEPLAYEDBYTYPE:
                return unsetRolePlayedByType(converter.concept(conceptMethod.getUnsetRolePlayedByType()).asRole());
            case ADDENTITY:
                return ADD_ENTITY;
            case SETRELATEDROLE:
                return setRelatedRole(converter.concept(conceptMethod.getSetRelatedRole()).asRole());
            case UNSETRELATEDROLE:
                return unsetRelatedRole(converter.concept(conceptMethod.getUnsetRelatedRole()).asRole());
            case PUTATTRIBUTE:
                return putAttribute(ConceptReader.attributeValue(conceptMethod.getPutAttribute()));
            case SETREGEX:
                return setRegex(ConceptReader.optionalRegex(conceptMethod.getSetRegex()));
            case SETATTRIBUTE:
                return setAttribute(converter.concept(conceptMethod.getSetAttribute()).asAttribute());
            case UNSETATTRIBUTE:
                return unsetAttribute(converter.concept(conceptMethod.getUnsetAttribute()).asAttribute());
            case ADDRELATIONSHIP:
                return ADD_RELATIONSHIP;
            case SETROLEPLAYER:
                return setRolePlayer(converter.rolePlayer(conceptMethod.getSetRolePlayer()));
            default:
            case CONCEPTMETHOD_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + conceptMethod);
        }
    }

    static abstract class BooleanMethod extends ConceptMethod<Boolean> {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Boolean value) {
            builder.setBool(value);
        }
    }

    static abstract class OptionalPatternMethod extends ConceptMethod<Optional<Pattern>> {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Optional<Pattern> value) {
            builder.setOptionalPattern(ConceptBuilder.optionalPattern(value));
        }
    }

    static abstract class ConceptConceptMethod extends ConceptMethod<Concept> {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Concept value) {
            builder.setConcept(ConceptBuilder.concept(value));
        }
    }

    static abstract class OptionalConceptMethod extends ConceptMethod<Optional<Concept>> {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Optional<Concept> value) {
            builder.setOptionalConcept(ConceptBuilder.optionalConcept(value));
        }
    }

    static abstract class UnitMethod extends ConceptMethod<Void> {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Void value) {
            builder.setUnit(GrpcConcept.Unit.getDefaultInstance());
        }
    }

    static abstract class ConceptStreamMethod extends ConceptMethod<Stream<? extends Concept>> {
        @Override @Nullable
        public Stream<? extends Concept> readResponse(TxConceptReader txConceptReader, GrpcClient client, TxResponse txResponse) {
            GrpcIterator.IteratorId iteratorId = txResponse.getConceptResponse().getIteratorId();
            Iterable<? extends Concept> iterable = () -> new ResponseIterator<>(client, iteratorId, response -> txConceptReader.concept(response.getConcept()));

            return StreamSupport.stream(iterable.spliterator(), false);
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Stream<? extends Concept> value) {
            Stream<TxResponse> responses = value.map(ResponseBuilder::concept);
            GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
            builder.setIteratorId(iteratorId);
        }
    }

    public static final ConceptMethod<Object> GET_VALUE = new ConceptMethod<Object>() {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Object value) {
            builder.setAttributeValue(ConceptBuilder.attributeValue(value));
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Object response = concept.asAttribute().getValue();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Optional<AttributeType.DataType<?>>> GET_DATA_TYPE_OF_TYPE = new ConceptMethod<Optional<AttributeType.DataType<?>>>() {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Optional<AttributeType.DataType<?>> value) {
            builder.setOptionalDataType(ResponseBuilder.optionalDataType(value));
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Optional<AttributeType.DataType<?>> response = Optional.ofNullable(concept.asAttributeType().getDataType());
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<AttributeType.DataType<?>> GET_DATA_TYPE_OF_ATTRIBUTE = new ConceptMethod<AttributeType.DataType<?>>() {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, AttributeType.DataType<?> value) {
            builder.setDataType(ConceptBuilder.dataType(value));
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            AttributeType.DataType<?> response = concept.asAttribute().dataType();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Label> GET_LABEL = new ConceptMethod<Label>() {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Label value) {
            builder.setLabel(ConceptBuilder.label(value));
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Label response = concept.asSchemaConcept().getLabel();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Boolean> IS_IMPLICIT = new BooleanMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Boolean response = concept.asSchemaConcept().isImplicit();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Boolean> IS_INFERRED = new BooleanMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Boolean result = concept.asThing().isInferred();
            return createTxResponse(iterators, result);
        }
    };
    public static final ConceptMethod<Boolean> IS_ABSTRACT = new BooleanMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Boolean response = concept.asType().isAbstract();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Optional<Pattern>> GET_WHEN = new OptionalPatternMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Optional<Pattern> result = Optional.ofNullable(concept.asRule().getWhen());
            return createTxResponse(iterators, result);
        }
    };
    public static final ConceptMethod<Optional<Pattern>> GET_THEN = new OptionalPatternMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Optional<Pattern> response = Optional.ofNullable(concept.asRule().getThen());
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Optional<String>> GET_REGEX = new ConceptMethod<Optional<String>>() {
        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Optional<String> value) {
            builder.setOptionalRegex(ConceptBuilder.optionalRegex(value));
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Optional<String> response = Optional.ofNullable(concept.asAttributeType().getRegex());
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<RolePlayer>> GET_ROLE_PLAYERS = new ConceptMethod<Stream<RolePlayer>>() {
        @Override @Nullable
        public Stream<RolePlayer> readResponse(TxConceptReader txConceptReader, GrpcClient client, TxResponse txResponse) {
            GrpcIterator.IteratorId iteratorId = txResponse.getConceptResponse().getIteratorId();
            Iterable<RolePlayer> iterable = () -> new ResponseIterator<>(client, iteratorId, response -> txConceptReader.rolePlayer(response.getRolePlayer()));

            return StreamSupport.stream(iterable.spliterator(), false);
        }

        @Override
        public void buildResponse(ConceptResponse.Builder builder, GrpcIterators iterators, Stream<RolePlayer> value) {
            Stream<TxResponse> responses = value.map(ResponseBuilder::rolePlayer);
            GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
            builder.setIteratorId(iteratorId);
        }

        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetRolePlayers(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream.Builder<RolePlayer> rolePlayers = Stream.builder();
            concept.asRelationship().allRolePlayers().forEach((role, players) -> {
                players.forEach(player -> {
                    rolePlayers.add(RolePlayer.create(role, player));
                });
            });
            Stream<RolePlayer> response = rolePlayers.build();

            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_ATTRIBUTE_TYPES = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetAttributeTypes(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asType().attributes();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_KEY_TYPES = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetKeyTypes(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asType().keys();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_SUPER_CONCEPTS = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetSuperConcepts(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asSchemaConcept().sups();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_SUB_CONCEPTS = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetSubConcepts(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asSchemaConcept().subs();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_ATTRIBUTES = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetAttributes(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asThing().attributes();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_KEYS = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetKeys(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asThing().keys();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_ROLES_PLAYED_BY_TYPE = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetRolesPlayedByType(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asType().plays();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_INSTANCES = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetInstances(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asType().instances();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_ROLES_PLAYED_BY_THING = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetRolesPlayedByThing(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asThing().plays();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATIONSHIPS = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetRelationships(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asThing().relationships();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATIONSHIP_TYPES_THAT_RELATE_ROLE = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetRelationshipTypesThatRelateRole(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asRole().relationshipTypes();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_TYPES_THAT_PLAY_ROLE = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetTypesThatPlayRole(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asRole().playedByTypes();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATED_ROLES = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetRelatedRoles(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asRelationshipType().relates();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Stream<? extends Concept>> GET_OWNERS = new ConceptStreamMethod() {
        @Override
        public GrpcConcept.ConceptMethod requestBuilder() {
            GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
            return builder.setGetOwners(GrpcConcept.Unit.getDefaultInstance()).build();
        }

        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Stream<? extends Concept> response = concept.asAttribute().ownerInstances();
            return createTxResponse(iterators, response);
        }
    };

    public static final ConceptMethod<Optional<Concept>> GET_DIRECT_SUPER = new OptionalConceptMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Optional<Concept> response = Optional.ofNullable(concept.asSchemaConcept().sup());
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Void> DELETE = new UnitMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            concept.delete();
            return null;
        }
    };
    public static final ConceptMethod<Concept> ADD_ENTITY = new ConceptConceptMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Concept response = concept.asEntityType().addEntity();
            return createTxResponse(iterators, response);
        }
    };
    public static final ConceptMethod<Concept> ADD_RELATIONSHIP = new ConceptConceptMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Concept response = concept.asRelationshipType().addRelationship();
            return createTxResponse(iterators, response);
        }
    };

    public static final ConceptMethod<Concept> GET_DIRECT_TYPE = new ConceptConceptMethod() {
        @Override
        public TxResponse run(GrpcIterators iterators, Concept concept) {
            Concept response = concept.asThing().type();
            return createTxResponse(iterators, response);
        }
    };

    public static ConceptMethod<Concept> putAttribute(Object value) {
        return new ConceptConceptMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                Concept response = concept.asAttributeType().putAttribute(value);
                return createTxResponse(iterators, response);
            }
        };
    }

    public static ConceptMethod<Concept> setAttribute(Attribute<?> attribute) {
        return new ConceptConceptMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                Concept response = concept.asThing().attributeRelationship(attribute);
                return createTxResponse(iterators, response);
            }
        };
    }

    public static ConceptMethod<Optional<Concept>> getAttribute(Object value) {
        return new OptionalConceptMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                Optional<Concept> response = Optional.ofNullable(concept.asAttributeType().getAttribute(value));
                return createTxResponse(iterators, response);
            }
        };
    }

    public static ConceptMethod<Void> setLabel(Label label) {
        return new UnitMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asSchemaConcept().setLabel(label);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setAbstract(boolean isAbstract) {
        return new UnitMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asType().setAbstract(isAbstract);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setRegex(Optional<String> regex) {
        return new UnitMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asAttributeType().setRegex(regex.orElse(null));
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setAttributeType(AttributeType<?> attributeType) {
        return new UnitMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asType().attribute(attributeType);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> unsetAttributeType(AttributeType<?> attributeType) {
        return new UnitMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asType().deleteAttribute(attributeType);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setKeyType(AttributeType<?> attributeType) {
        return new UnitMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asType().key(attributeType);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> unsetKeyType(AttributeType<?> attributeType) {
        return new UnitMethod() {
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asType().deleteKey(attributeType);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setDirectSuperConcept(SchemaConcept schemaConcept) {
        return new UnitMethod() {
             // Make the second argument the super of the first argument
             // @throws GraqlQueryException if the types are different, or setting the super to be a meta-type
            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                //TODO: This was copied from ConceptBuilder

                SchemaConcept subConcept = concept.asSchemaConcept();
                SchemaConcept superConcept = schemaConcept;

                if (superConcept.isEntityType()) {
                    subConcept.asEntityType().sup(superConcept.asEntityType());
                } else if (superConcept.isRelationshipType()) {
                    subConcept.asRelationshipType().sup(superConcept.asRelationshipType());
                } else if (superConcept.isRole()) {
                    subConcept.asRole().sup(superConcept.asRole());
                } else if (superConcept.isAttributeType()) {
                    subConcept.asAttributeType().sup(superConcept.asAttributeType());
                } else if (superConcept.isRule()) {
                    subConcept.asRule().sup(superConcept.asRule());
                } else {
                    throw GraqlQueryException.insertMetaType(subConcept.getLabel(), superConcept);
                }

                return null;
            }
        };
    }

    public static ConceptMethod<Void> removeRolePlayer(RolePlayer rolePlayer) {
        return new UnitMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setUnsetRolePlayer(ConceptBuilder.rolePlayer(rolePlayer)).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asRelationship().removeRolePlayer(rolePlayer.role(), rolePlayer.player());
                return null;
            }
        };
    }

    public static ConceptMethod<Void> unsetAttribute(Attribute<?> attribute) {
        return new UnitMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setUnsetAttribute(ConceptBuilder.concept(attribute)).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asThing().deleteAttribute(attribute);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setRolePlayedByType(Role role) {
        return new UnitMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setSetRolePlayedByType(ConceptBuilder.concept(role)).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asType().plays(role);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> unsetRolePlayedByType(Role role) {
        return new UnitMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setUnsetRolePlayedByType(ConceptBuilder.concept(role)).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asType().deletePlays(role);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setRolePlayer(RolePlayer rolePlayer) {
        return new UnitMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setSetRolePlayer(ConceptBuilder.rolePlayer(rolePlayer)).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asRelationship().addRolePlayer(rolePlayer.role(), rolePlayer.player());
                return null;
            }
        };
    }

    public static ConceptMethod<Void> setRelatedRole(Role role) {
        return new UnitMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setSetRelatedRole(ConceptBuilder.concept(role)).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asRelationshipType().relates(role);
                return null;
            }
        };
    }

    public static ConceptMethod<Void> unsetRelatedRole(Role role) {
        return new UnitMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setUnsetRelatedRole(ConceptBuilder.concept(role)).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                concept.asRelationshipType().deleteRelates(role);
                return null;
            }
        };
    }

    public static ConceptMethod<Stream<? extends Concept>> getAttributesByTypes(AttributeType<?>... attributeTypes) {
        return new ConceptStreamMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setGetAttributesByTypes(ConceptBuilder.concepts(Stream.of(attributeTypes))).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                Stream<? extends Concept> response = concept.asThing().attributes(attributeTypes);
                return createTxResponse(iterators, response);
            }
        };
    }

    public static ConceptMethod<Stream<? extends Concept>> getKeysByTypes(AttributeType<?>... attributeTypes) {
        return new ConceptStreamMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setGetKeysByTypes(ConceptBuilder.concepts(Stream.of(attributeTypes))).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                Stream<? extends Concept> response = concept.asThing().keys(attributeTypes);
                return createTxResponse(iterators, response);
            }
        };
    }

    public static ConceptMethod<Stream<? extends Concept>> getRolePlayersByRoles(Role... roles) {
        return new ConceptStreamMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setGetRolePlayersByRoles(ConceptBuilder.concepts(Stream.of(roles))).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                Stream<? extends Concept> response = concept.asRelationship().rolePlayers(roles);
                return createTxResponse(iterators, response);
            }
        };
    }

    public static ConceptMethod<Stream<? extends Concept>> getRelationshipsByRoles(Role... roles) {
        return new ConceptStreamMethod() {
            @Override
            public GrpcConcept.ConceptMethod requestBuilder() {
                GrpcConcept.ConceptMethod.Builder builder = GrpcConcept.ConceptMethod.newBuilder();
                return builder.setGetRelationshipsByRoles(ConceptBuilder.concepts(Stream.of(roles))).build();
            }

            @Override
            public TxResponse run(GrpcIterators iterators, Concept concept) {
                Stream<? extends Concept> response = concept.asThing().relationships(roles);
                return createTxResponse(iterators, response);
            }
        };
    }
}