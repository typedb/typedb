/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.kb.concept.api;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;

import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.HAS_INVALID;
import static grakn.core.common.exception.ErrorMessage.INVALID_PROPERTY_USE;
import static grakn.core.common.exception.ErrorMessage.LABEL_TAKEN;
import static grakn.core.common.exception.ErrorMessage.META_TYPE_IMMUTABLE;
import static grakn.core.common.exception.ErrorMessage.NO_TYPE;
import static grakn.core.common.exception.ErrorMessage.REGEX_NOT_STRING;
import static grakn.core.common.exception.ErrorMessage.UNKNOWN_CONCEPT;

public class GraknConceptException extends GraknException {

    GraknConceptException(String error) {
        super(error);
    }

    protected GraknConceptException(String error, Exception e) {
        super(error, e);
    }

    public static GraknConceptException cannotShard(Concept concept) {
        return GraknConceptException.create(ErrorMessage.NOT_A_TYPE.getMessage(concept.id(), concept.getClass()));
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static GraknConceptException create(String error) {
        return new GraknConceptException(error);
    }

    /**
     * Thrown when casting Grakn concepts/answers incorrectly.
     */
    public static GraknConceptException invalidCasting(Object concept, Class type) {
        return GraknConceptException.create(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(concept, type));
    }

    public static GraknConceptException variableDoesNotExist(String var) {
        return new GraknConceptException(ErrorMessage.VARIABLE_DOES_NOT_EXIST.getMessage(var));
    }

    public static GraknConceptException metaTypeImmutable(Label metaLabel) {
        return create(META_TYPE_IMMUTABLE.getMessage(metaLabel));
    }

    /**
     * Thrown when attempting to set a regex on a Attribute whose type AttributeType is not of the
     * data type AttributeType.DataType#STRING
     */
    public static GraknConceptException cannotSetRegex(AttributeType attributeType) {
        return create(REGEX_NOT_STRING.getMessage(attributeType.label()));
    }

    /**
     * Thrown when trying to set a {@code value} on the {@code resource} which does not conform to it's regex
     */
    public static GraknConceptException regexFailure(AttributeType attributeType, String value, String regex) {
        return create(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, attributeType.label(), value));
    }


    /**
     * Thrown when attempting to create a Thing via the execution of a Rule when
     * the Thing already exists.
     */
    public static GraknConceptException nonInferredThingExists(Thing thing) {
        return create(String.format("Thing {%s} was already created and cannot be set to inferred", thing));
    }

    public static GraknConceptException unsupportedDataType(String name) {
        String supported = AttributeType.DataType.values().stream().map(AttributeType.DataType::name).collect(Collectors.joining(","));
        return create(ErrorMessage.INVALID_DATATYPE.getMessage(name, supported));
    }


    /**
     * Thrown when creating a label which starts with a reserved character Schema.ImplicitType#RESERVED
     */
    // TODO use Schema.ImplicitType.RESERVED.getValue() after breaking cyclic dependency
    public static GraknConceptException invalidLabelStart(Label label) {
        return create(String.format("Cannot create a label {%s} starting with character {%s} as it is a reserved starting character", label, "@"));
    }


    /**
     * Thrown when creating an Attribute whose value Object does not match attribute data type
     */
    public static GraknConceptException invalidAttributeValue(AttributeType attributeType, Object object, AttributeType.DataType dataType) {
        return create(ErrorMessage.INVALID_DATATYPE.getMessage(object, object.getClass().getSimpleName(), dataType.name(), attributeType.label()));
    }

    /**
     * Throw when trying to add instances to an abstract Type
     */
    public static GraknConceptException addingInstancesToAbstractType(Type type) {
        return create(ErrorMessage.IS_ABSTRACT.getMessage(type.label()));
    }


    /**
     * Thrown when trying to build a Concept using an invalid graph construct
     */
    public static GraknConceptException unknownConceptType(String type) {
        return create(UNKNOWN_CONCEPT.getMessage(type));
    }

    /**
     * Thrown when trying to identify the meta type of a type but cannot
     */
    public static GraknConceptException unknownTypeMetaType(Type type) {
        return create(ErrorMessage.UNKNOWN_META_TYPE.getMessage(type.label().toString(), type.getClass().toString()));
    }


    /**
     * Thrown when changing the Label of an SchemaConcept which is owned by another SchemaConcept
     */
    public static GraknConceptException labelTaken(Label label) {
        return create(LABEL_TAKEN.getMessage(label));
    }

    /**
     * Thrown when a Type has incoming edges and therefore cannot be deleted
     */
    public static GraknConceptException cannotBeDeleted(SchemaConcept schemaConcept) {
        return create(ErrorMessage.CANNOT_DELETE.getMessage(schemaConcept.label()));
    }


    /**
     * Thrown when setting {@code superType} as the super type of {@code type} and a loop is created
     */
    public static GraknConceptException loopCreated(SchemaConcept type, SchemaConcept superElement) {
        return create(ErrorMessage.SUPER_LOOP_DETECTED.getMessage(type.label(), superElement.label()));
    }

    /**
     * Thrown when a Thing is not allowed to have Attribute of that AttributeType
     */
    public static GraknConceptException hasNotAllowed(Thing thing, Attribute attribute) {
        return create(HAS_INVALID.getMessage(thing.type().label(), attribute.type().label()));
    }

    /**
     * Thrown when an thing does not have a type
     */
    public static GraknConceptException noType(Thing thing) {
        return create(NO_TYPE.getMessage(thing.id()));
    }


    /**
     * Thrown when changing the super of a Type will result in a Role disconnection which is in use.
     */
    public static GraknConceptException changingSuperWillDisconnectRole(Type oldSuper, Type newSuper, Role role) {
        return create(String.format("Cannot change the super type {%s} to {%s} because {%s} is connected to role {%s} which {%s} is not connected to.",
                oldSuper.label(), newSuper.label(), oldSuper.label(), role.label(), newSuper.label()));
    }

    /**
     * Thrown when there exists and instance of {@code type} HAS {@code attributeType} upon unlinking the AttributeType from the Type
     */
    public static GraknConceptException illegalUnhasWithInstance(String type, String attributeType, boolean isKey) {
        return create(ErrorMessage.ILLEGAL_TYPE_UNHAS_ATTRIBUTE_WITH_INSTANCE.getMessage(type, isKey ? "key" : "has", attributeType));
    }

    /**
     * Thrown when there exists and instance of {@code type} HAS {@code attributeType} upon unlinking the AttributeType from the Type
     */
    public static GraknConceptException illegalUnhasInherited(String type, String attributeType, boolean isKey) {
        return create(ErrorMessage.ILLEGAL_TYPE_UNHAS_ATTRIBUTE_INHERITED.getMessage(type, isKey ? "key" : "has", attributeType));
    }

    /**
     * Thrown when there exists and instance of {@code type} HAS {@code attributeType} upon unlinking the AttributeType from the Type
     */
    public static GraknConceptException illegalUnhasNotExist(String type, String attributeType, boolean isKey) {
        return create(ErrorMessage.ILLEGAL_TYPE_UNHAS_ATTRIBUTE_NOT_EXIST.getMessage(type, isKey ? "key" : "has", attributeType));
    }


    /**
     * Thrown when {@code type} has {@code attributeType} as a Type#key(AttributeType) and a Type#has(AttributeType)
     */
    public static GraknConceptException duplicateHas(Type type, AttributeType attributeType) {
        return create(ErrorMessage.CANNOT_BE_KEY_AND_ATTRIBUTE.getMessage(type.label(), attributeType.label()));
    }


    /**
     * Thrown when trying to add a Schema.VertexProperty to a Concept which does not accept that type
     * of Schema.VertexProperty
     */
    public static GraknConceptException invalidPropertyUse(Concept concept, String property) {
        return create(INVALID_PROPERTY_USE.getMessage(concept, property));
    }

    /**
     * Thrown when trying to set an invalid super type
     */
    public static GraknConceptException invalidSuperType(Label label, SchemaConcept superType) {
        return new GraknConceptException(ErrorMessage.INVALID_SUPER_TYPE.getMessage(label, superType.label()));
    }

    /**
     * Thrown when trying to delete a concept that is not handled
     */
    public static GraknConceptException unhandledConceptDeletion(Concept concept) {
        return new GraknConceptException(ErrorMessage.UNHANDLED_CONCEPT_DELETION.getMessage(concept.toString()));
    }

}
