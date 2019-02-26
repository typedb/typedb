/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.exception;

import com.google.common.base.Preconditions;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.server.Transaction;
import grakn.core.server.kb.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import javax.annotation.Nullable;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.CLOSE_FAILURE;
import static grakn.core.common.exception.ErrorMessage.HAS_INVALID;
import static grakn.core.common.exception.ErrorMessage.INVALID_DIRECTION;
import static grakn.core.common.exception.ErrorMessage.INVALID_PROPERTY_USE;
import static grakn.core.common.exception.ErrorMessage.LABEL_TAKEN;
import static grakn.core.common.exception.ErrorMessage.META_TYPE_IMMUTABLE;
import static grakn.core.common.exception.ErrorMessage.NO_TYPE;
import static grakn.core.common.exception.ErrorMessage.REGEX_NOT_STRING;
import static grakn.core.common.exception.ErrorMessage.RESERVED_WORD;
import static grakn.core.common.exception.ErrorMessage.UNKNOWN_CONCEPT;

/**
 * Illegal Mutation Exception
 * This exception is thrown to prevent the user from incorrectly mutating the graph.
 * For example, when attempting to create instances for an abstract type, this exception is thrown.
 */
public class TransactionException extends GraknException {

    TransactionException(String error) {
        super(error);
    }

    protected TransactionException(String error, Exception e) {
        super(error, e);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static TransactionException create(String error) {
        return new TransactionException(error);
    }

    /**
     * Thrown when attempting to mutate a Schema.MetaSchema
     */
    public static TransactionException metaTypeImmutable(Label metaLabel) {
        return create(META_TYPE_IMMUTABLE.getMessage(metaLabel));
    }

    /**
     * Throw when trying to add instances to an abstract Type
     */
    public static TransactionException addingInstancesToAbstractType(Type type) {
        return create(ErrorMessage.IS_ABSTRACT.getMessage(type.label()));
    }

    /**
     * Thrown when a Thing is not allowed to have Attribute of that AttributeType
     */
    public static TransactionException hasNotAllowed(Thing thing, Attribute attribute) {
        return create(HAS_INVALID.getMessage(thing.type().label(), attribute.type().label()));
    }

    /**
     * Thrown when attempting to set a regex on a Attribute whose type AttributeType is not of the
     * data type AttributeType.DataType#STRING
     */
    public static TransactionException cannotSetRegex(AttributeType attributeType) {
        return create(REGEX_NOT_STRING.getMessage(attributeType.label()));
    }

    /**
     * Thrown when a Type has incoming edges and therefore cannot be deleted
     */
    public static TransactionException cannotBeDeleted(SchemaConcept schemaConcept) {
        return create(ErrorMessage.CANNOT_DELETE.getMessage(schemaConcept.label()));
    }

    /**
     * Thrown when {@code type} has {@code attributeType} as a Type#key(AttributeType) and a Type#has(AttributeType)
     */
    public static TransactionException duplicateHas(Type type, AttributeType attributeType) {
        return create(ErrorMessage.CANNOT_BE_KEY_AND_ATTRIBUTE.getMessage(type.label(), attributeType.label()));
    }

    /**
     * Thrown when there exists and instance of {@code type} HAS {@code attributeType} upon unlinking the AttributeType from the Type
     */
    public static TransactionException illegalUnhasWithInstance(String type, String attributeType, boolean isKey) {
        return create(ErrorMessage.ILLEGAL_TYPE_UNHAS_ATTRIBUTE_WITH_INSTANCE.getMessage(type, isKey ? "key" : "has", attributeType));
    }

    /**
     * Thrown when there exists and instance of {@code type} HAS {@code attributeType} upon unlinking the AttributeType from the Type
     */
    public static TransactionException illegalUnhasInherited(String type, String attributeType, boolean isKey) {
        return create(ErrorMessage.ILLEGAL_TYPE_UNHAS_ATTRIBUTE_INHERITED.getMessage(type, isKey ? "key" : "has", attributeType));
    }

    /**
     * Thrown when there exists and instance of {@code type} HAS {@code attributeType} upon unlinking the AttributeType from the Type
     */
    public static TransactionException illegalUnhasNotExist(String type, String attributeType, boolean isKey) {
        return create(ErrorMessage.ILLEGAL_TYPE_UNHAS_ATTRIBUTE_NOT_EXIST.getMessage(type, isKey ? "key" : "has", attributeType));
    }

    /**
     * Thrown when setting {@code superType} as the super type of {@code type} and a loop is created
     */
    public static TransactionException loopCreated(SchemaConcept type, SchemaConcept superElement) {
        return create(ErrorMessage.SUPER_LOOP_DETECTED.getMessage(type.label(), superElement.label()));
    }

    /**
     * Thrown when creating an Attribute whose value Object does not match attribute data type
     */
    public static TransactionException invalidAttributeValue(Object object, AttributeType.DataType dataType) {
        return create(ErrorMessage.INVALID_DATATYPE.getMessage(object, object.getClass().getSimpleName(), dataType.name()));
    }

    /**
     * Thrown when using an unsupported datatype with resources
     */
    public static TransactionException unsupportedDataType(Object value) {
        return unsupportedDataType(value.getClass());
    }

    public static TransactionException unsupportedDataType(Class<?> clazz) {
        return unsupportedDataType(clazz.getName());
    }

    public static TransactionException unsupportedDataType(String name) {
        String supported = AttributeType.DataType.values().stream().map(AttributeType.DataType::name).collect(Collectors.joining(","));
        return create(ErrorMessage.INVALID_DATATYPE.getMessage(name, supported));
    }

    /**
     * Thrown when attempting to mutate a property which is immutable
     */
    public static TransactionException immutableProperty(Object oldValue, Object newValue, Enum vertexProperty) {
        return create(ErrorMessage.IMMUTABLE_VALUE.getMessage(oldValue, newValue, vertexProperty.name()));
    }

    /**
     * Thrown when trying to set a {@code value} on the {@code resource} which does not conform to it's regex
     */
    public static TransactionException regexFailure(AttributeType attributeType, String value, String regex) {
        return create(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, attributeType.label(), value));
    }

    /**
     * Thrown when attempting to open a transaction which is already open
     */
    public static TransactionException transactionOpen(Transaction tx) {
        return create(ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(tx.keyspace()));
    }

    /**
     * Thrown when attempting to open an invalid type of transaction
     */
    public static TransactionException transactionInvalid(Object tx) {
        return create("Unknown type of transaction [" + tx + "]");
    }

    /**
     * Thrown when attempting to mutate a read only transaction
     */
    public static TransactionException transactionReadOnly(Transaction tx) {
        return create(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
    }

    /**
     * Thrown when attempting to mutate the schema while the transaction is in batch mode
     */
    public static TransactionException schemaMutation() {
        return create(ErrorMessage.SCHEMA_LOCKED.getMessage());
    }

    /**
     * Thrown when attempting to use the graph when the transaction is closed
     */
    public static TransactionException transactionClosed(@Nullable Transaction tx, @Nullable String reason) {
        if (reason == null) {
            Preconditions.checkNotNull(tx);
            return create(ErrorMessage.TX_CLOSED.getMessage(tx.keyspace()));
        } else {
            return create(reason);
        }
    }

    /**
     * Thrown when the graph can not be closed due to an unknown reason.
     */
    public static TransactionException closingFailed(Transaction tx, Exception e) {
        return new TransactionException(CLOSE_FAILURE.getMessage(tx.keyspace()), e);
    }

    /**
     * Thrown when an thing does not have a type
     */
    public static TransactionException noType(Thing thing) {
        return create(NO_TYPE.getMessage(thing.id()));
    }

    /**
     * Thrown when attempting to traverse an edge in an invalid direction
     */
    public static TransactionException invalidDirection(Direction direction) {
        return create(INVALID_DIRECTION.getMessage(direction));
    }

    /**
     * Thrown when trying to create something using a label reserved by the system
     */
    public static TransactionException reservedLabel(Label label) {
        return create(RESERVED_WORD.getMessage(label.getValue()));
    }

    /**
     * Thrown when trying to add a Schema.VertexProperty to a Concept which does not accept that type
     * of Schema.VertexProperty
     */
    public static TransactionException invalidPropertyUse(Concept concept, Schema.VertexProperty property) {
        return create(INVALID_PROPERTY_USE.getMessage(concept, property));
    }

    /**
     * Thrown when trying to build a Concept using an invalid graph construct
     */
    public static TransactionException unknownConcept(String type) {
        return create(UNKNOWN_CONCEPT.getMessage(type));
    }

    /**
     * Thrown when changing the Label of an SchemaConcept which is owned by another SchemaConcept
     */
    public static TransactionException labelTaken(Label label) {
        return create(LABEL_TAKEN.getMessage(label));
    }

    /**
     * Thrown when creating an invalid KeyspaceImpl
     */
    public static TransactionException invalidKeyspaceName(String keyspace) {
        return create(ErrorMessage.INVALID_KEYSPACE_NAME.getMessage(keyspace));
    }

    /**
     * Thrown when changing the super of a Type will result in a Role disconnection which is in use.
     */
    public static TransactionException changingSuperWillDisconnectRole(Type oldSuper, Type newSuper, Role role) {
        return create(String.format("Cannot change the super type {%s} to {%s} because {%s} is connected to role {%s} which {%s} is not connected to.",
                                    oldSuper.label(), newSuper.label(), oldSuper.label(), role.label(), newSuper.label()));
    }

    /**
     * Thrown when a Thing is missing a Type
     */
    public static TransactionException missingType(ConceptId id) {
        return create(String.format("Thing {%s} is missing a type", id));
    }

    /**
     * Thrown when creating a label which starts with a reserved character Schema.ImplicitType#RESERVED
     */
    public static TransactionException invalidLabelStart(Label label) {
        return create(String.format("Cannot create a label {%s} starting with character {%s} as it is a reserved starting character", label, Schema.ImplicitType.RESERVED.getValue()));
    }

    /**
     * Thrown when attempting to create a Thing via the execution of a Rule when
     * the Thing already exists.
     */
    public static TransactionException nonInferredThingExists(Thing thing) {
        return create(String.format("Thing {%s} was already created and cannot be set to inferred", thing));
    }

    /**
     * Thrown when trying to build a Concept from an invalid vertex or edge
     */
    public static TransactionException invalidElement(Element element) {
        return create(String.format("Cannot build a concept from element {%s} due to it being deleted.", element));
    }
}
