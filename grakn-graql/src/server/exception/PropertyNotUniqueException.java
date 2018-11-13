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

import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.concept.RelationshipType;
import grakn.core.concept.SchemaConcept;
import grakn.core.graql.internal.Schema;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static grakn.core.util.ErrorMessage.INVALID_UNIQUE_PROPERTY_MUTATION;
import static grakn.core.util.ErrorMessage.UNIQUE_PROPERTY_TAKEN;

/**
 * <p>
 *     Unique Concept Property Violation
 * </p>
 *
 * <p>
 *     This occurs when attempting to add a globally unique property to a concept.
 *     For example when creating a {@link grakn.core.concept.EntityType} and {@link RelationshipType} using
 *     the same {@link Label}
 * </p>
 *
 */
public class PropertyNotUniqueException extends TransactionException {
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
