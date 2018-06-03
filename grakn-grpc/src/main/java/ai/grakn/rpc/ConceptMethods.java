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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ConceptReader;
import ai.grakn.rpc.util.TxConceptReader;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Static class providing all possible {@link ConceptMethod}s.
 *
 * @author Felix Chapman
 */
public class ConceptMethods {

    // These constants are simply ConceptMethods that don't have parameters (e.g. getters).
    // You could make them zero-argument functions if you want!

    public static final ConceptMethod<Object> GET_VALUE = ConceptMethod.builder(ConceptResponseType.ATTRIBUTE_VALUE)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetValue)
            .function(val -> val.asAttribute().getValue())
            .build();
    public static final ConceptMethod<Optional<AttributeType.DataType<?>>> GET_DATA_TYPE_OF_TYPE =
            ConceptMethod.builder(ConceptResponseType.OPTIONAL_DATA_TYPE)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetDataTypeOfType)
                    .function(val -> Optional.ofNullable(val.asAttributeType().getDataType()))
                    .build();
    public static final ConceptMethod<AttributeType.DataType<?>> GET_DATA_TYPE_OF_ATTRIBUTE =
            ConceptMethod.builder(ConceptResponseType.DATA_TYPE)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetDataTypeOfAttribute)
                    .function(val -> val.asAttribute().dataType())
                    .build();
    public static final ConceptMethod<Label> GET_LABEL = ConceptMethod.builder(ConceptResponseType.LABEL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetLabel)
            .function(concept -> concept.asSchemaConcept().getLabel())
            .build();
    public static final ConceptMethod<Boolean> IS_IMPLICIT = ConceptMethod.builder(ConceptResponseType.BOOL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setIsImplicit)
            .function(concept -> concept.asSchemaConcept().isImplicit())
            .build();
    public static final ConceptMethod<Boolean> IS_INFERRED = ConceptMethod.builder(ConceptResponseType.BOOL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setIsInferred)
            .function(concept -> concept.asThing().isInferred())
            .build();
    public static final ConceptMethod<Boolean> IS_ABSTRACT = ConceptMethod.builder(ConceptResponseType.BOOL)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setIsAbstract)
            .function(concept -> concept.asType().isAbstract())
            .build();
    public static final ConceptMethod<Optional<Pattern>> GET_WHEN = ConceptMethod.builder(ConceptResponseType.OPTIONAL_PATTERN)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetWhen)
            .function(concept -> Optional.ofNullable(concept.asRule().getWhen()))
            .build();
    public static final ConceptMethod<Optional<Pattern>> GET_THEN = ConceptMethod.builder(ConceptResponseType.OPTIONAL_PATTERN)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetThen)
            .function(concept -> Optional.ofNullable(concept.asRule().getThen()))
            .build();
    public static final ConceptMethod<Optional<String>> GET_REGEX = ConceptMethod.builder(ConceptResponseType.OPTIONAL_REGEX)
            .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRegex)
            .function(concept -> Optional.ofNullable(concept.asAttributeType().getRegex()))
            .build();
    public static final ConceptMethod<Stream<RolePlayer>> GET_ROLE_PLAYERS =
            ConceptMethod.builder(ConceptResponseType.ROLE_PLAYERS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRolePlayers)
                    .function(concept -> {
                        Stream.Builder<RolePlayer> rolePlayers = Stream.builder();
                        concept.asRelationship().allRolePlayers().forEach((role, players) -> {
                            players.forEach(player -> {
                                rolePlayers.add(RolePlayer.create(role, player));
                            });
                        });
                        return rolePlayers.build();
                    })
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_ATTRIBUTE_TYPES =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetAttributeTypes)
                    .function(concept -> concept.asType().attributes())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_KEY_TYPES =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetKeyTypes)
                    .function(concept -> concept.asType().keys())
                    .build();
    public static final ConceptMethod<Concept> GET_DIRECT_TYPE =
            ConceptMethod.builder(ConceptResponseType.CONCEPT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetDirectType)
                    .function(concept -> concept.asThing().type())
                    .build();
    public static final ConceptMethod<Optional<Concept>> GET_DIRECT_SUPER =
            ConceptMethod.builder(ConceptResponseType.OPTIONAL_CONCEPT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetDirectSuperConcept)
                    .function(concept -> Optional.ofNullable(concept.asSchemaConcept().sup()))
                    .build();
    public static final ConceptMethod<Void> DELETE =
            ConceptMethod.builder(ConceptResponseType.UNIT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setDelete)
                    .functionVoid(Concept::delete)
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_SUPER_CONCEPTS =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetSuperConcepts)
                    .function(concept -> concept.asSchemaConcept().sups())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_SUB_CONCEPTS =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetSubConcepts)
                    .function(concept -> concept.asSchemaConcept().subs())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_ATTRIBUTES =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetAttributes)
                    .function(concept -> concept.asThing().attributes())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_KEYS =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetKeys)
                    .function(concept -> concept.asThing().keys())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_ROLES_PLAYED_BY_TYPE =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRolesPlayedByType)
                    .function(concept -> concept.asType().plays())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_INSTANCES =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetInstances)
                    .function(concept -> concept.asType().instances())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_ROLES_PLAYED_BY_THING =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRolesPlayedByThing)
                    .function(concept -> concept.asThing().plays())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATIONSHIPS =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRelationships)
                    .function(concept -> concept.asThing().relationships())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATIONSHIP_TYPES_THAT_RELATE_ROLE =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRelationshipTypesThatRelateRole)
                    .function(concept -> concept.asRole().relationshipTypes())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_TYPES_THAT_PLAY_ROLE =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetTypesThatPlayRole)
                    .function(concept -> concept.asRole().playedByTypes())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_RELATED_ROLES =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetRelatedRoles)
                    .function(concept -> concept.asRelationshipType().relates())
                    .build();
    public static final ConceptMethod<Stream<? extends Concept>> GET_OWNERS =
            ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setGetOwners)
                    .function(concept -> concept.asAttribute().ownerInstances())
                    .build();
    public static final ConceptMethod<Concept> ADD_ENTITY =
            ConceptMethod.builder(ConceptResponseType.CONCEPT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setAddEntity)
                    .function(concept -> concept.asEntityType().addEntity())
                    .build();
    public static final ConceptMethod<Concept> ADD_RELATIONSHIP =
            ConceptMethod.builder(ConceptResponseType.CONCEPT)
                    .requestSetterUnit(GrpcConcept.ConceptMethod.Builder::setAddRelationship)
                    .function(concept -> concept.asRelationshipType().addRelationship())
                    .build();

    public static ConceptMethod<Void> setLabel(Label label) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetLabel(ConceptBuilder.label(label)))
                .functionVoid(concept -> concept.asSchemaConcept().setLabel(label))
                .build();
    }

    public static ConceptMethod<Void> setAbstract(boolean isAbstract) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetAbstract(isAbstract))
                .functionVoid(concept -> concept.asType().setAbstract(isAbstract))
                .build();
    }

    public static ConceptMethod<Void> setRegex(Optional<String> regex) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetRegex(ConceptBuilder.optionalRegex(regex)))
                .functionVoid(concept -> concept.asAttributeType().setRegex(regex.orElse(null)))
                .build();
    }

    public static ConceptMethod<Void> setAttributeType(AttributeType<?> attributeType) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetAttributeType(ConceptBuilder.concept(attributeType)))
                .functionVoid(concept -> concept.asType().attribute(attributeType))
                .build();
    }

    public static ConceptMethod<Void> unsetAttributeType(AttributeType<?> attributeType) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setUnsetAttributeType(ConceptBuilder.concept(attributeType)))
                .functionVoid(concept -> concept.asType().deleteAttribute(attributeType))
                .build();
    }

    public static ConceptMethod<Void> setKeyType(AttributeType<?> attributeType) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetKeyType(ConceptBuilder.concept(attributeType)))
                .functionVoid(concept -> concept.asType().key(attributeType))
                .build();
    }

    public static ConceptMethod<Void> unsetKeyType(AttributeType<?> attributeType) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setUnsetKeyType(ConceptBuilder.concept(attributeType)))
                .functionVoid(concept -> concept.asType().deleteKey(attributeType))
                .build();
    }

    public static ConceptMethod<Void> setDirectSuperConcept(SchemaConcept schemaConcept) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetDirectSuperConcept(ConceptBuilder.concept(schemaConcept)))
                .functionVoid(concept -> setSuper(concept.asSchemaConcept(), schemaConcept))
                .build();
    }

    public static ConceptMethod<Void> removeRolePlayer(RolePlayer rolePlayer) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setUnsetRolePlayer(ConceptBuilder.rolePlayer(rolePlayer)))
                .functionVoid(concept -> {
                    concept.asRelationship().removeRolePlayer(rolePlayer.role(), rolePlayer.player());
                })
                .build();
    }

    public static ConceptMethod<Optional<Concept>> getAttribute(Object value) {
        return ConceptMethod.builder(ConceptResponseType.OPTIONAL_CONCEPT)
                .requestSetter(builder -> builder.setGetAttribute(ConceptBuilder.attributeValue(value)))
                .function(concept -> Optional.ofNullable(concept.asAttributeType().getAttribute(value)))
                .build();
    }

    public static ConceptMethod<Stream<? extends Concept>> getAttributesByTypes(AttributeType<?>... attributeTypes) {
        return ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                .requestSetter(builder -> builder.setGetAttributesByTypes(ConceptBuilder.concepts(Stream.of(attributeTypes))))
                .function(concept -> concept.asThing().attributes(attributeTypes))
                .build();
    }

    public static ConceptMethod<Concept> setAttribute(Attribute<?> attribute) {
        return ConceptMethod.builder(ConceptResponseType.CONCEPT)
                .requestSetter(builder -> builder.setSetAttribute(ConceptBuilder.concept(attribute)))
                .function(concept -> concept.asThing().attributeRelationship(attribute))
                .build();
    }

    public static ConceptMethod<Void> unsetAttribute(Attribute<?> attribute) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setUnsetAttribute(ConceptBuilder.concept(attribute)))
                .functionVoid(concept -> concept.asThing().deleteAttribute(attribute))
                .build();
    }

    public static ConceptMethod<Stream<? extends Concept>> getKeysByTypes(AttributeType<?>... attributeTypes) {
        return ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                .requestSetter(builder -> builder.setGetKeysByTypes(ConceptBuilder.concepts(Stream.of(attributeTypes))))
                .function(concept -> concept.asThing().keys(attributeTypes))
                .build();
    }

    public static ConceptMethod<Void> setRolePlayedByType(Role role) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetRolePlayedByType(ConceptBuilder.concept(role)))
                .functionVoid(concept -> concept.asType().plays(role))
                .build();
    }

    public static ConceptMethod<Void> unsetRolePlayedByType(Role role) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setUnsetRolePlayedByType(ConceptBuilder.concept(role)))
                .functionVoid(concept -> concept.asType().deletePlays(role))
                .build();
    }

    public static ConceptMethod<Stream<? extends Concept>> getRolePlayersByRoles(Role... roles) {
        return ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                .requestSetter(builder -> builder.setGetRolePlayersByRoles(ConceptBuilder.concepts(Stream.of(roles))))
                .function(concept -> concept.asRelationship().rolePlayers(roles))
                .build();
    }

    public static ConceptMethod<Void> setRolePlayer(RolePlayer rolePlayer) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetRolePlayer(ConceptBuilder.rolePlayer(rolePlayer)))
                .functionVoid(concept -> concept.asRelationship().addRolePlayer(rolePlayer.role(), rolePlayer.player()))
                .build();
    }

    public static ConceptMethod<Stream<? extends Concept>> getRelationshipsByRoles(Role... roles) {
        return ConceptMethod.builder(ConceptResponseType.CONCEPTS)
                .requestSetter(builder -> builder.setGetRelationshipsByRoles(ConceptBuilder.concepts(Stream.of(roles))))
                .function(concept -> concept.asThing().relationships(roles))
                .build();
    }

    public static ConceptMethod<Void> setRelatedRole(Role role) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setSetRelatedRole(ConceptBuilder.concept(role)))
                .functionVoid(concept -> concept.asRelationshipType().relates(role))
                .build();
    }

    public static ConceptMethod<Void> unsetRelatedRole(Role role) {
        return ConceptMethod.builder(ConceptResponseType.UNIT)
                .requestSetter(builder -> builder.setUnsetRelatedRole(ConceptBuilder.concept(role)))
                .functionVoid(concept -> concept.asRelationshipType().deleteRelates(role))
                .build();
    }

    public static ConceptMethod<Concept> putAttribute(Object value) {
        return ConceptMethod.builder(ConceptResponseType.CONCEPT)
                .requestSetter(builder -> builder.setPutAttribute(ConceptBuilder.attributeValue(value)))
                .function(concept -> concept.asAttributeType().putAttribute(value))
                .build();
    }

    public static ConceptMethod<?> fromGrpc(TxConceptReader converter, GrpcConcept.ConceptMethod conceptMethod) {
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

    // TODO: This was copied from ConceptBuilder
    /**
     * Make the second argument the super of the first argument
     *
     * @throws GraqlQueryException if the types are different, or setting the super to be a meta-type
     */
    private static void setSuper(SchemaConcept subConcept, SchemaConcept superConcept) {
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
    }
}
