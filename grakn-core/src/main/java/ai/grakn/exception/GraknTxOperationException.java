/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.exception;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.CLOSE_FAILURE;
import static ai.grakn.util.ErrorMessage.HAS_INVALID;
import static ai.grakn.util.ErrorMessage.INVALID_DIRECTION;
import static ai.grakn.util.ErrorMessage.INVALID_PATH_TO_CONFIG;
import static ai.grakn.util.ErrorMessage.INVALID_PROPERTY_USE;
import static ai.grakn.util.ErrorMessage.LABEL_TAKEN;
import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static ai.grakn.util.ErrorMessage.NO_TYPE;
import static ai.grakn.util.ErrorMessage.REGEX_NOT_STRING;
import static ai.grakn.util.ErrorMessage.RESERVED_WORD;
import static ai.grakn.util.ErrorMessage.UNKNOWN_CONCEPT;
import static ai.grakn.util.ErrorMessage.VERSION_MISMATCH;

/**
 * <p>
 *     Illegal Mutation Exception
 * </p>
 *
 * <p>
 *     This exception is thrown to prevent the user from incorrectly mutating the graph.
 *     For example when attempting to an instances to an abstract type this exception is thrown.
 * </p>
 *
 * @author fppt
 */
public class GraknTxOperationException extends GraknException{
    protected GraknTxOperationException(String error){
        super(error);
    }

    protected GraknTxOperationException(String error, Exception e){
        super(error, e);
    }

    /**
     * Thrown when attempting to mutate a {@link ai.grakn.util.Schema.MetaSchema}
     */
    public static GraknTxOperationException metaTypeImmutable(Label metaLabel){
        return new GraknTxOperationException(META_TYPE_IMMUTABLE.getMessage(metaLabel));
    }

    /**
     * Throw when trying to add instances to an abstract Type
     */
    public static GraknTxOperationException addingInstancesToAbstractType(Type type){
        return new GraknTxOperationException(ErrorMessage.IS_ABSTRACT.getMessage(type.getLabel()));
    }

    /**
     * Thrown when a {@link Thing} is not allowed to have {@link Attribute} of that {@link AttributeType}
     */
    public static GraknTxOperationException hasNotAllowed(Thing thing, Attribute attribute){
        return new GraknTxOperationException(HAS_INVALID.getMessage(thing.type().getLabel(), attribute.type().getLabel()));
    }

    /**
     * Thrown when attempting to set a regex on a {@link Attribute} whose type {@link AttributeType} is not of the
     * data type {@link AttributeType.DataType#STRING}
     */
    public static GraknTxOperationException cannotSetRegex(AttributeType attributeType){
        return new GraknTxOperationException(REGEX_NOT_STRING.getMessage(attributeType.getLabel()));
    }

    /**
     * Thrown when a {@link Type} has incoming edges and therefore cannot be deleted
     */
    public static GraknTxOperationException cannotBeDeleted(SchemaConcept schemaConcept){
        return new GraknTxOperationException(ErrorMessage.CANNOT_DELETE.getMessage(schemaConcept.getLabel()));
    }

    /**
     * Thrown when {@code type} has {@code attributeType} as a {@link Type#key(AttributeType)} and a {@link Type#attribute(AttributeType)}
     */
    public static GraknTxOperationException duplicateHas(Type type, AttributeType attributeType){
        return new GraknTxOperationException(ErrorMessage.CANNOT_BE_KEY_AND_RESOURCE.getMessage(type.getLabel(), attributeType.getLabel()));
    }

    /**
     * Thrown when setting {@code superType} as the super type of {@code type} and a loop is created
     */
    public static GraknTxOperationException loopCreated(SchemaConcept type, SchemaConcept superElement){
        throw new GraknTxOperationException(ErrorMessage.SUPER_LOOP_DETECTED.getMessage(type.getLabel(), superElement.getLabel()));
    }

    /**
     * Thrown when casting concepts incorrectly. For example when doing {@link Concept#asEntityType()} on a
     * {@link ai.grakn.concept.Entity}
     */
    public static GraknTxOperationException invalidCasting(Object concept, Class type){
        throw new GraknTxOperationException(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(concept, type));
    }

    /**
     * Thrown when creating a resource whose value {@code object} does not match it's resource's  {@code dataType}.
     */
    public static GraknTxOperationException invalidResourceValue(Object object, AttributeType.DataType dataType){
        return new GraknTxOperationException(ErrorMessage.INVALID_DATATYPE.getMessage(object, dataType.getVertexProperty().getDataType().getName()));
    }

    /**
     * Thrown when using an unsupported datatype with resources
     */
    public static GraknTxOperationException unsupportedDataType(Object value) {
        String supported = AttributeType.DataType.SUPPORTED_TYPES.keySet().stream().collect(Collectors.joining(","));
        return new GraknTxOperationException(ErrorMessage.INVALID_DATATYPE.getMessage(value.getClass().getName(), supported));
    }

    /**
     * Thrown when attempting to mutate a property which is immutable
     */
    public static GraknTxOperationException immutableProperty(Object oldValue, Object newValue, Enum vertexProperty){
        return new GraknTxOperationException(ErrorMessage.IMMUTABLE_VALUE.getMessage(oldValue, newValue, vertexProperty.name()));
    }

    /**
     * Thrown when trying to set a {@code value} on the {@code resource} which does not conform to it's regex
     */
    public static GraknTxOperationException regexFailure(AttributeType attributeType, String value, String regex){
        return new GraknTxOperationException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, attributeType.getLabel(), value));
    }

    /**
     * Thrown when attempting to open a transaction which is already open
     */
    public static GraknTxOperationException transactionOpen(GraknTx tx){
        return new GraknTxOperationException(ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(tx.getKeyspace()));
    }

    /**
     * Thrown when attempting to open an invalid type of transaction
     */
    public static GraknTxOperationException transactionInvalid(Object tx){
        return new GraknTxOperationException("Unknown type of transaction [" + tx + "]");
    }

    /**
     * Thrown when attempting to mutate a read only transaction
     */
    public static GraknTxOperationException transactionReadOnly(GraknTx tx){
        return new GraknTxOperationException(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.getKeyspace()));
    }

    /**
     * Thrown when attempting to mutate the schema while the transaction is in batch mode
     */
    public static GraknTxOperationException schemaMutation(){
        return new GraknTxOperationException(ErrorMessage.SCHEMA_LOCKED.getMessage());
    }

    /**
     * Thrown when attempting to use the graph when the transaction is closed
     */
    public static GraknTxOperationException transactionClosed(GraknTx tx, String reason){
        if(reason == null){
            return new GraknTxOperationException(ErrorMessage.TX_CLOSED.getMessage(tx.getKeyspace()));
        } else {
            return new GraknTxOperationException(reason);
        }
    }

    /**
     * Thrown when the graph can not be closed due to an unknown reason.
     */
    public static GraknTxOperationException closingFailed(GraknTx tx, Exception e){
        return new GraknTxOperationException(CLOSE_FAILURE.getMessage(tx.getKeyspace()), e);
    }

    /**
     * Thrown when using incompatible versions of Grakn
     */
    public static GraknTxOperationException versionMistmatch(Attribute versionAttribute){
        return new GraknTxOperationException(VERSION_MISMATCH.getMessage(GraknVersion.VERSION, versionAttribute.getValue()));
    }

    /**
     * Thrown when an thing does not have a type
     */
    public static GraknTxOperationException noType(Thing thing){
        return new GraknTxOperationException(NO_TYPE.getMessage(thing.getId()));
    }

    /**
     * Thrown when attempting to traverse an edge in an invalid direction
     */
    public static GraknTxOperationException invalidDirection(Direction direction){
        return new GraknTxOperationException(INVALID_DIRECTION.getMessage(direction));
    }

    /**
     * Thrown when attempting to read a config file which cannot be accessed
     */
    public static GraknTxOperationException invalidConfig(String pathToFile){
        return new GraknTxOperationException(INVALID_PATH_TO_CONFIG.getMessage(pathToFile));
    }

    /**
     * Thrown when trying to create something using a label reserved by the system
     */
    public static GraknTxOperationException reservedLabel(Label label){
        return new GraknTxOperationException(RESERVED_WORD.getMessage(label.getValue()));
    }

    /**
     * Thrown when trying to add a {@link Schema.VertexProperty} to a {@link Concept} which does not accept that type
     * of {@link Schema.VertexProperty}
     */
    public static GraknTxOperationException invalidPropertyUse(Concept concept, Schema.VertexProperty property) {
        return new GraknTxOperationException(INVALID_PROPERTY_USE.getMessage(concept, property));
    }

    /**
     * Thrown when trying to build a {@link Concept} using an invalid graph construct
     */
    public static GraknTxOperationException unknownConcept(String type){
        return new GraknTxOperationException(UNKNOWN_CONCEPT.getMessage(type));
    }

    /**
     * Thrown when changing the {@link Label} of an {@link SchemaConcept} which is owned by another {@link SchemaConcept}
     */
    public static GraknTxOperationException labelTaken(Label label){
        return new GraknTxOperationException(LABEL_TAKEN.getMessage(label));
    }

    /**
     * Thrown when creating an invalid {@link ai.grakn.Keyspace}
     */
    public static GraknTxOperationException invalidKeyspace(String keyspace){
        return new GraknTxOperationException("Keyspace [" + keyspace + "] is invalid. " +
                "Grakn Keyspaces cannot start with a number and can only be lower case containing alphanumeric values and underscore characters." +
                "Grakn Keyspaces can also not be longer than 48 characters");
    }

    /**
     * Thrown when changing the super of a {@link Type} will result in a {@link Role} disconnection which is in use.
     */
    public static GraknTxOperationException changingSuperWillDisconnectRole(Type oldSuper, Type newSuper, Role role){
        return new GraknTxOperationException(String.format("Cannot change the super type {%s} to {%s} because {%s} is connected to role {%s} which {%s} is not connected to.",
                oldSuper.getLabel(), newSuper.getLabel(), oldSuper.getLabel(), role.getLabel(), newSuper.getLabel()));
    }

    /**
     * Thrown when a {@link Concept} does not have a shard
     */
    public static GraknTxOperationException missingShard(ConceptId id) {
        return new GraknTxOperationException(String.format("Concept {%s} is missing an essential shard", id));
    }

    /**
     * Thrown when a casting does not have a role player
     */
    public static GraknTxOperationException missingRolePlayer(String id) {
        return new GraknTxOperationException(String.format("Concept {%s} is missing a role player", id));
    }

    /**
     * Thrown when a casting is missing a {@link ai.grakn.concept.Relationship}
     */
    public static GraknTxOperationException missingRelationship(String id) {
        return new GraknTxOperationException(String.format("Concept {%s} is missing a relationship", id));
    }

    /**
     * Thrown when link to a {@link Attribute} is missing the owner
     */
    public static GraknTxOperationException missingOwner(ConceptId id) {
        return new GraknTxOperationException(String.format("Relationship {%s} is missing the owner", id));
    }

    /**
     * Thrown when link to a {@link Attribute} is missing the owner
     */
    public static GraknTxOperationException missingValue(ConceptId id) {
        return new GraknTxOperationException(String.format("Relationship {%s} is missing the value", id));
    }

    /**
     * Thrown when a {@link Thing} is missing a {@link Type}
     */
    public static GraknTxOperationException missingType(ConceptId id) {
        return new GraknTxOperationException(String.format("Thing {%s} is missing a type", id));
    }

    /**
     * Thrown when creating a label which starts with a reserved character {@link Schema.ImplicitType#RESERVED}
     */
    public static GraknTxOperationException invalidLabelStart(Label label){
        return new GraknTxOperationException(String.format("Cannot create a label {%s} starting with character {%s} as it is a reserved starting character", label, Schema.ImplicitType.RESERVED.getValue()));
    }
}
