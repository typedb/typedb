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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.CLOSE_GRAPH_FAILURE;
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
 *     Illegal Graph Mutation Exception
 * </p>
 *
 * <p>
 *     This exception is thrown to prevent the user from incorrectly mutating the graph.
 *     For example when attempting to an instances to an abstract type this exception is thrown.
 * </p>
 *
 * @author fppt
 */
public class GraphOperationException extends GraknException{
    protected GraphOperationException(String error){
        super(error);
    }

    protected GraphOperationException(String error, Exception e){
        super(error, e);
    }

    /**
     * Thrown when attempting to mutate a {@link ai.grakn.util.Schema.MetaSchema}
     */
    public static GraphOperationException metaTypeImmutable(Label metaLabel){
        return new GraphOperationException(META_TYPE_IMMUTABLE.getMessage(metaLabel));
    }

    /**
     * Throw when trying to add instances to an abstract Type
     */
    public static GraphOperationException addingInstancesToAbstractType(Type type){
        return new GraphOperationException(ErrorMessage.IS_ABSTRACT.getMessage(type.getLabel()));
    }

    /**
     * Thrown when a {@link Thing} is not allowed to have {@link Resource} of that {@link ResourceType}
     */
    public static GraphOperationException hasNotAllowed(Thing thing, Resource resource){
        return new GraphOperationException(HAS_INVALID.getMessage(thing.type().getLabel(), resource.type().getLabel()));
    }

    /**
     * Thrown when attempting to set a regex on a {@link Resource} whose type {@link ResourceType} is not of the
     * data type {@link ResourceType.DataType#STRING}
     */
    public static GraphOperationException cannotSetRegex(ResourceType resourceType){
        return new GraphOperationException(REGEX_NOT_STRING.getMessage(resourceType.getLabel()));
    }

    /**
     * Thrown when a {@link Type} has incoming edges and therefore cannot be deleted
     */
    public static GraphOperationException cannotBeDeleted(OntologyConcept ontologyConcept){
        return new GraphOperationException(ErrorMessage.CANNOT_DELETE.getMessage(ontologyConcept.getLabel()));
    }

    /**
     * Thrown when {@code type} has {@code resourceType} as a {@link Type#key(ResourceType)} and a {@link Type#resource(ResourceType)}
     */
    public static GraphOperationException duplicateHas(Type type, ResourceType resourceType){
        return new GraphOperationException(ErrorMessage.CANNOT_BE_KEY_AND_RESOURCE.getMessage(type.getLabel(), resourceType.getLabel()));
    }

    /**
     * Thrown when setting {@code superType} as the super type of {@code type} and a loop is created
     */
    public static GraphOperationException loopCreated(OntologyConcept type, OntologyConcept superElement){
        throw new GraphOperationException(ErrorMessage.SUPER_LOOP_DETECTED.getMessage(type.getLabel(), superElement.getLabel()));
    }

    /**
     * Thrown when casting concepts incorrectly. For example when doing {@link Concept#asEntityType()} on a
     * {@link ai.grakn.concept.Entity}
     */
    public static GraphOperationException invalidCasting(Object concept, Class type){
        throw new GraphOperationException(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(concept, type));
    }

    /**
     * Thrown when creating a resource whose value {@code object} does not match it's resource's  {@code dataType}.
     */
    public static GraphOperationException invalidResourceValue(Object object, ResourceType.DataType dataType){
        return new GraphOperationException(ErrorMessage.INVALID_DATATYPE.getMessage(object, dataType.getVertexProperty().getDataType().getName()));
    }

    /**
     * Thrown when using an unsupported datatype with resources
     */
    public static GraphOperationException unsupportedDataType(Object value) {
        String supported = ResourceType.DataType.SUPPORTED_TYPES.keySet().stream().collect(Collectors.joining(","));
        return new GraphOperationException(ErrorMessage.INVALID_DATATYPE.getMessage(value.getClass().getName(), supported));
    }

    /**
     * Thrown when attempting to mutate a property which is immutable
     */
    public static GraphOperationException immutableProperty(Object oldValue, Object newValue, Enum vertexProperty){
        return new GraphOperationException(ErrorMessage.IMMUTABLE_VALUE.getMessage(oldValue, newValue, vertexProperty.name()));
    }

    /**
     * Thrown when trying to set a {@code value} on the {@code resource} which does not conform to it's regex
     */
    public static GraphOperationException regexFailure(ResourceType resourceType, String value, String regex){
        return new GraphOperationException(ErrorMessage.REGEX_INSTANCE_FAILURE.getMessage(regex, resourceType.getLabel(), value));
    }

    /**
     * Thrown when attempting to open a transaction which is already open
     */
    public static GraphOperationException transactionOpen(GraknGraph graph){
        return new GraphOperationException(ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(graph.getKeyspace()));
    }

    /**
     * Thrown when attempting to open an invalid type of transaction
     */
    public static GraphOperationException transactionInvalid(Object tx){
        return new GraphOperationException("Unknown type of transaction [" + tx + "]");
    }

    /**
     * Thrown when attempting to mutate a read only transaction
     */
    public static GraphOperationException transactionReadOnly(GraknGraph graph){
        return new GraphOperationException(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(graph.getKeyspace()));
    }

    /**
     * Thrown when attempting to mutate the ontology while the transaction is in batch mode
     */
    public static GraphOperationException ontologyMutation(){
        return new GraphOperationException(ErrorMessage.SCHEMA_LOCKED.getMessage());
    }

    /**
     * Thrown when attempting to use the graph when the transaction is closed
     */
    public static GraphOperationException transactionClosed(GraknGraph graph, String reason){
        if(reason == null){
            return new GraphOperationException(ErrorMessage.GRAPH_CLOSED.getMessage(graph.getKeyspace()));
        } else {
            return new GraphOperationException(reason);
        }
    }

    /**
     * Thrown when the graph can not be closed due to an unknown reason.
     */
    public static GraphOperationException closingGraphFailed(GraknGraph graph, Exception e){
        return new GraphOperationException(CLOSE_GRAPH_FAILURE.getMessage(graph.getKeyspace()), e);
    }

    /**
     * Thrown when using incompatible versions of Grakn
     */
    public static GraphOperationException versionMistmatch(Resource versionResource){
        return new GraphOperationException(VERSION_MISMATCH.getMessage(GraknVersion.VERSION, versionResource.getValue()));
    }

    /**
     * Thrown when an thing does not have a type
     */
    public static GraphOperationException noType(Thing thing){
        return new GraphOperationException(NO_TYPE.getMessage(thing.getId()));
    }

    /**
     * Thrown when attempting to traverse an edge in an invalid direction
     */
    public static GraphOperationException invalidDirection(Direction direction){
        return new GraphOperationException(INVALID_DIRECTION.getMessage(direction));
    }

    /**
     * Thrown when attempting to read a config file which cannot be accessed
     */
    public static GraphOperationException invalidGraphConfig(String pathToFile){
        return new GraphOperationException(INVALID_PATH_TO_CONFIG.getMessage(pathToFile));
    }

    /**
     * Thrown when trying to create something using a label reserved by the system
     */
    public static GraphOperationException reservedLabel(Label label){
        return new GraphOperationException(RESERVED_WORD.getMessage(label.getValue()));
    }

    /**
     * Thrown when trying to add a {@link Schema.VertexProperty} to a {@link Concept} which does not accept that type
     * of {@link Schema.VertexProperty}
     */
    public static GraphOperationException invalidPropertyUse(Concept concept, Schema.VertexProperty property) {
        return new GraphOperationException(INVALID_PROPERTY_USE.getMessage(concept, property));
    }

    /**
     * Thrown when trying to build a {@link Concept} using an invalid graph construct
     */
    public static GraphOperationException unknownConcept(String type){
        return new GraphOperationException(UNKNOWN_CONCEPT.getMessage(type));
    }

    /**
     * Thrown when changing the {@link Label} of an {@link OntologyConcept} which is owned by another {@link OntologyConcept}
     */
    public static GraphOperationException labelTaken(Label label){
        throw new GraphOperationException(LABEL_TAKEN.getMessage(label));
    }
}
