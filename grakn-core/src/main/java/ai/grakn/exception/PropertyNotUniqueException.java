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

package ai.grakn.exception;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.util.ErrorMessage.INVALID_UNIQUE_PROPERTY_MUTATION;
import static ai.grakn.util.ErrorMessage.UNIQUE_PROPERTY_TAKEN;

/**
 * <p>
 *     Unique Concept Property Violation
 * </p>
 *
 * <p>
 *     This occurs when attempting to add a globally unique property to a concept.
 *     For example when creating a {@link ai.grakn.concept.EntityType} and {@link RelationshipType} using
 *     the same {@link Label}
 * </p>
 *
 * @author fppt
 */
public class PropertyNotUniqueException extends GraknTxOperationException {
    private PropertyNotUniqueException(String error) {
        super(error);
    }

    public static PropertyNotUniqueException create(String error) {
        return new PropertyNotUniqueException(error);
    }

    /**
     * Thrown when trying to set the property of concept {@code mutatingConcept} to a {@code value} which is already
     * taken by concept {@code conceptWithValue}
     */
    public static PropertyNotUniqueException cannotChangeProperty(Element mutatingConcept, Vertex conceptWithValue, Enum property, Object value){
        return create(INVALID_UNIQUE_PROPERTY_MUTATION.getMessage(property, mutatingConcept, value, conceptWithValue));
    }

    /**
     * Thrown when trying to create a {@link SchemaConcept} using a unique property which is already taken.
     * For example this happens when using an already taken {@link Label}
     */
    public static PropertyNotUniqueException cannotCreateProperty(Concept concept, Schema.VertexProperty property, Object value){
        return create(UNIQUE_PROPERTY_TAKEN.getMessage(property.name(), value, concept));
    }
}
