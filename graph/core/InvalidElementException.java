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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Exception thrown when an element is invalid for the executing operation or when an operation could not be performed
 * on an element.
 *
 */
public class InvalidElementException extends JanusGraphException {

    private final JanusGraphElement element;

    /**
     * @param msg     Exception message
     * @param element The invalid element causing the exception
     */
    public InvalidElementException(String msg, JanusGraphElement element) {
        super(msg);
        this.element = element;
    }

    /**
     * Returns the element causing the exception
     *
     * @return The element causing the exception
     */
    public JanusGraphElement getElement() {
        return element;
    }

    @Override
    public String toString() {
        return super.toString() + " [" + element.toString() + "]";
    }

    public static IllegalStateException removedException(JanusGraphElement element) {
        Class elementClass = Vertex.class.isAssignableFrom(element.getClass()) ? Vertex.class :
            (Edge.class.isAssignableFrom(element.getClass()) ? Edge.class : VertexProperty.class);
        return new IllegalStateException(String.format("%s with id %s was removed.", elementClass.getSimpleName(), element.id()));
    }

}
