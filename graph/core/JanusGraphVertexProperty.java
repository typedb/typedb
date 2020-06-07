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


package grakn.core.graph.core;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * JanusGraphProperty is a JanusGraphRelation connecting a vertex to a value.
 * JanusGraphProperty extends JanusGraphRelation, with methods for retrieving the property's value and key.
 *
 * see JanusGraphRelation
 * see PropertyKey
 */
public interface JanusGraphVertexProperty<V> extends JanusGraphRelation, VertexProperty<V>, JanusGraphProperty<V> {

    /**
     * Returns the vertex on which this property is incident.
     *
     * @return The vertex of this property.
     */
    @Override
    JanusGraphVertex element();

    @Override
    default JanusGraphTransaction graph() {
        return element().graph();
    }

    @Override
    default PropertyKey propertyKey() {
        return (PropertyKey)getType();
    }

}
