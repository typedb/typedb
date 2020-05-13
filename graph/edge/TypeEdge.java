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

import hypergraph.graph.vertex.TypeVertex;

public interface TypeEdge extends Edge {

    /**
     * @return type vertex overridden by the head of this type edge.
     */
    TypeVertex overridden();

    /**
     * Set the head type vertex of this type edge to override a given type vertex.
     *
     * @param overridden the type vertex to override by the head
     */
    void overridden(TypeVertex overridden);
}
