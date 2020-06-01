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

package hypergraph.graph.edge;

import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

/**
 * An edge between two {@code TypeVertex}.
 *
 * This edge can only have a schema of type {@code Schema.Edge.Type}.
 */
public interface TypeEdge extends Edge<IID.Edge.Type, Schema.Edge.Type, TypeVertex> {

    /**
     * Returns the type vertex overridden by the head of this type edge.
     *
     * @return the type vertex overridden by the head of this type edge
     */
    TypeVertex overridden();

    /**
     * Sets the head vertex of this type edge to be overridden by a given type vertex.
     *
     * @param overridden the type vertex to override by the head vertex
     */
    void overridden(TypeVertex overridden);
}
