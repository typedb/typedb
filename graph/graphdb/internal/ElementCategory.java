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
 */

package grakn.core.graph.graphdb.internal;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.graphdb.relations.RelationIdentifier;
import grakn.core.graph.graphdb.types.VertexLabelVertex;
import grakn.core.graph.graphdb.types.vertices.EdgeLabelVertex;
import grakn.core.graph.graphdb.types.vertices.PropertyKeyVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public enum ElementCategory {
    VERTEX, EDGE, PROPERTY;

    public Class<? extends Element> getElementType() {
        switch (this) {
            case VERTEX:
                return JanusGraphVertex.class;
            case EDGE:
                return JanusGraphEdge.class;
            case PROPERTY:
                return JanusGraphVertexProperty.class;
            default:
                throw new IllegalArgumentException();
        }
    }

    public boolean isRelation() {
        switch (this) {
            case VERTEX:
                return false;
            case EDGE:
            case PROPERTY:
                return true;
            default:
                throw new IllegalArgumentException();
        }
    }

    public boolean isValidConstraint(JanusGraphSchemaType type) {
        Preconditions.checkNotNull(type);
        switch (this) {
            case VERTEX:
                return (type instanceof VertexLabelVertex);
            case EDGE:
                return (type instanceof EdgeLabelVertex);
            case PROPERTY:
                return (type instanceof PropertyKeyVertex);
            default:
                throw new IllegalArgumentException();
        }
    }

    public boolean matchesConstraint(JanusGraphSchemaType type, JanusGraphElement element) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(element);
        switch (this) {
            case VERTEX:
                return ((JanusGraphVertex) element).vertexLabel().equals(type);
            case EDGE:
                return ((JanusGraphEdge) element).edgeLabel().equals(type);
            case PROPERTY:
                return ((JanusGraphVertexProperty) element).propertyKey().equals(type);
            default:
                throw new IllegalArgumentException();
        }
    }

    public boolean isInstance(JanusGraphElement element) {
        Preconditions.checkNotNull(element);
        return getElementType().isAssignableFrom(element.getClass());
    }

    public boolean subsumedBy(Class<? extends Element> clazz) {
        return clazz.isAssignableFrom(getElementType());
    }

    public String getName() {
        return toString().toLowerCase();
    }

    public JanusGraphElement retrieve(Object elementId, JanusGraphTransaction tx) {
        Preconditions.checkArgument(elementId != null, "Must provide elementId");
        switch (this) {
            case VERTEX:
                Preconditions.checkArgument(elementId instanceof Long);
                return tx.getVertex((Long) elementId);
            case EDGE:
                Preconditions.checkArgument(elementId instanceof RelationIdentifier);
                return ((RelationIdentifier) elementId).findEdge(tx);
            case PROPERTY:
                Preconditions.checkArgument(elementId instanceof RelationIdentifier);
                return ((RelationIdentifier) elementId).findProperty(tx);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static ElementCategory getByClazz(Class<? extends Element> clazz) {
        Preconditions.checkArgument(clazz != null, "Need to provide a element class argument");
        if (Vertex.class.isAssignableFrom(clazz)) {
            return VERTEX;
        } else if (Edge.class.isAssignableFrom(clazz)) {
            return EDGE;
        } else if (JanusGraphVertexProperty.class.isAssignableFrom(clazz)) {
            return PROPERTY;
        } else {
            throw new IllegalArgumentException("Invalid clazz provided: " + clazz);
        }
    }

    public static ElementCategory getByName(String name) {
        for (ElementCategory category : values()) {
            if (category.toString().equalsIgnoreCase(name)) return category;
        }
        throw new IllegalArgumentException("Unrecognized name: " + name);
    }
}
