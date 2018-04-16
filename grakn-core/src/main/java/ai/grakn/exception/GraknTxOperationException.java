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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.exception;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import javax.annotation.Nullable;
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.CLOSE_FAILURE;
import static ai.grakn.util.ErrorMessage.HAS_INVALID;
import static ai.grakn.util.ErrorMessage.INVALID_DIRECTION;
import static ai.grakn.util.ErrorMessage.INVALID_PROPERTY_USE;
import static ai.grakn.util.ErrorMessage.LABEL_TAKEN;
import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static ai.grakn.util.ErrorMessage.NO_TYPE;
import static ai.grakn.util.ErrorMessage.REGEX_NOT_STRING;
import static ai.grakn.util.ErrorMessage.RESERVED_WORD;
import static ai.grakn.util.ErrorMessage.UNKNOWN_CONCEPT;

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
    GraknTxOperationException(String error){
        super(error);
    }

    protected GraknTxOperationException(String error, Exception e){
        super(error, e);
    }

    public static GraknTxOperationException create(String error) {
        return new GraknTxOperationException(error);
    }

    /**
     * Thrown when attempting to mutate a {@link ai.grakn.util.Schema.MetaSchema}
     */
    public static GraknTxOperationException metaTypeImmutable(Label metaLabel){
        return create(META_TYPE_IMMUTABLE.getMessage(metaLabel));
    }

    /**
     * Throw when trying to add instances to an abstract Type
     */
    public static GraknTxOperationException addingInstancesToAbstractType(Type type){
        return create(ErrorMessage.IS_ABSTRACT.getMessage(type.getLabel()));
    }

    /**
     * Thrown when a {@link Thing} is not allowed to have {@link Attribute} of that {@link AttributeType}
     */
    public static GraknTxOperationException hasNotAllowed(Thing thing, Attribute attribute){
        return create(HAS_INVALID.getMessage(thing.type().getLabel(), attribute.type().getLabel()));
    }

    /**
     * Thrown when attempting to set a regex on a {@link Attribute} whose type {@link AttributeType} is not of the
     * data type {@link AttributeType.DataType#STRING}
     */
    public static GraknTxOperationException cannotSetRegex(AttributeType attributeType){
        return create(REGEX_NOT_STRING.getMessage(attributeType.getLabel()));
    }

    /**
     * Thrown when a {@link Type} has incoming edges and therefore cannot be deleted
     */
    public static GraknTxOperationException cannotBeDeleted(SchemaConcept schemaConcept){
        return create(ErrorMessage.CANNOT_DELETE.getMessage(schemaConcept.getLabel()));
    }

    /**
     * Thrown when {@code type} has {@code attributeType} as a {@link Type#key(AttributeType)} and a {@link Type#attribute(AttributeType)}
     */
    public static GraknTxOperationException duplicateHas(Type type, AttributeType attributeType){
        return create(ErrorMessage.CANNOT_BE_KEY_AND_RESOURCE.getMessage(type.getLabel(), attributeType.getLabel()));
    }

    /**
     * Thrown when setting {@code superType} as the super type of {@code type} and a loop is created
     */
    public static GraknTxOperationException loopCreated(SchemaConcept type, SchemaConcept superElement){
        return create(ErrorMessage.SUPER_LOOP_DETECTED.getMessage(type.getLabel(), superElement.getLabel()));
    }

    /**
     * Thrown when casting concepts incorrectly. For example when doing {@link Concept#asEntityType()} on a
     * {@link ai.grakn.concept.Entity}
     */
    public static GraknTxOperationException invalidCasting(Object concept, Class type){
        return create(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(concept, type));
    }

    /**
     * Thrown when creating a {@link Attribute} whose value {@link Object} does not match it's {@link AttributeType}'s
     * {@link ai.grakn.concept.AttributeType.DataType}
     */
    public static GraknTxOperationException invalidAttributeValue(Object object, AttributeType.DataType dataType){
        return create(ErrorMessage.INVALID_DATATYPE.getMessage(object, dataType.getName()));
    }

    /**
     * Thrown when using an unsupported datatype with resources
     */
    public static GraknTxOperationException unsupportedDataType(Object value) {
        String supported = AttributeType.DataType.SUPPORTED_TYPES.keySet().stream().collect(Collectors.joining(","));
        return create(ErrorMessage.INVALID_DATATYPE.getMessage(value.getClass().getName(), supported));
    }

    /**
     * Thrown when attempting to mutate a property which is immutable
     */
    public static GraknTxOperationException immutableProperty(Object oldValue, Object newValue, Enum vertexProperty){
        return create(ErrorMessage.IMMUTABLE_VALUE.getMessage(oldValue, newValue, vertexProperty.name()));
    }

    /**
     * Thrown when trying to set a {@code value} on the {@code resource} which does not conform to it's regex
     */
    public static GraknTxOperationException regexFailure(AttributeType attributeType, String value, String regex){
        return create(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, attributeType.getLabel(), value));
    }

    /**
     * Thrown when attempting to open a transaction which is already open
     */
    public static GraknTxOperationException transactionOpen(GraknTx tx){
        return create(ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(tx.keyspace()));
    }

    /**
     * Thrown when attempting to open an invalid type of transaction
     */
    public static GraknTxOperationException transactionInvalid(Object tx){
        return create("Unknown type of transaction [" + tx + "]");
    }

    /**
     * Thrown when attempting to mutate a read only transaction
     */
    public static GraknTxOperationException transactionReadOnly(GraknTx tx){
        return create(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
    }

    /**
     * Thrown when attempting to mutate the schema while the transaction is in batch mode
     */
    public static GraknTxOperationException schemaMutation(){
        return create(ErrorMessage.SCHEMA_LOCKED.getMessage());
    }

    /**
     * Thrown when attempting to use the graph when the transaction is closed
     */
    public static GraknTxOperationException transactionClosed(@Nullable GraknTx tx, @Nullable String reason){
        if(reason == null){
            Preconditions.checkNotNull(tx);
            return create(ErrorMessage.TX_CLOSED.getMessage(tx.keyspace()));
        } else {
            return create(reason);
        }
    }

    /**
     * Thrown when the graph can not be closed due to an unknown reason.
     */
    public static GraknTxOperationException closingFailed(GraknTx tx, Exception e){
        return new GraknTxOperationException(CLOSE_FAILURE.getMessage(tx.keyspace()), e);
    }

    /**
     * Thrown when an thing does not have a type
     */
    public static GraknTxOperationException noType(Thing thing){
        return create(NO_TYPE.getMessage(thing.getId()));
    }

    /**
     * Thrown when attempting to traverse an edge in an invalid direction
     */
    public static GraknTxOperationException invalidDirection(Direction direction){
        return create(INVALID_DIRECTION.getMessage(direction));
    }

    /**
     * Thrown when trying to create something using a label reserved by the system
     */
    public static GraknTxOperationException reservedLabel(Label label){
        return create(RESERVED_WORD.getMessage(label.getValue()));
    }

    /**
     * Thrown when trying to add a {@link Schema.VertexProperty} to a {@link Concept} which does not accept that type
     * of {@link Schema.VertexProperty}
     */
    public static GraknTxOperationException invalidPropertyUse(Concept concept, Schema.VertexProperty property) {
        return create(INVALID_PROPERTY_USE.getMessage(concept, property));
    }

    /**
     * Thrown when trying to build a {@link Concept} using an invalid graph construct
     */
    public static GraknTxOperationException unknownConcept(String type){
        return create(UNKNOWN_CONCEPT.getMessage(type));
    }

    /**
     * Thrown when changing the {@link Label} of an {@link SchemaConcept} which is owned by another {@link SchemaConcept}
     */
    public static GraknTxOperationException labelTaken(Label label){
        return create(LABEL_TAKEN.getMessage(label));
    }

    /**
     * Thrown when creating an invalid {@link ai.grakn.Keyspace}
     */
    public static GraknTxOperationException invalidKeyspace(String keyspace){
        return create("Keyspace [" + keyspace + "] is invalid. " +
                "Grakn Keyspaces cannot start with a number and can only be lower case containing alphanumeric values and underscore characters." +
                "Grakn Keyspaces can also not be longer than 48 characters");
    }

    /**
     * Thrown when changing the super of a {@link Type} will result in a {@link Role} disconnection which is in use.
     */
    public static GraknTxOperationException changingSuperWillDisconnectRole(Type oldSuper, Type newSuper, Role role){
        return create(String.format("Cannot change the super type {%s} to {%s} because {%s} is connected to role {%s} which {%s} is not connected to.",
                oldSuper.getLabel(), newSuper.getLabel(), oldSuper.getLabel(), role.getLabel(), newSuper.getLabel()));
    }

    /**
     * Thrown when a {@link Thing} is missing a {@link Type}
     */
    public static GraknTxOperationException missingType(ConceptId id) {
        return create(String.format("Thing {%s} is missing a type", id));
    }

    /**
     * Thrown when creating a label which starts with a reserved character {@link Schema.ImplicitType#RESERVED}
     */
    public static GraknTxOperationException invalidLabelStart(Label label){
        return create(String.format("Cannot create a label {%s} starting with character {%s} as it is a reserved starting character", label, Schema.ImplicitType.RESERVED.getValue()));
    }

    /**
     * Thrown when attempting to create a {@link Thing} via the execution of a {@link ai.grakn.concept.Rule} when
     * the {@link Thing} already exists.
     */
    public static GraknTxOperationException nonInferredThingExists(Thing thing){
        return create(String.format("Thing {%s} was already created and cannot be set to inferred", thing));
    }

    /**
     * Thrown when trying to build a {@link Concept} from an invalid vertex or edge
     */
    public static GraknTxOperationException invalidElement(Element element){
        return create(String.format("Cannot build a concept from element {%s} due to it being deleted.", element));
    }
}
