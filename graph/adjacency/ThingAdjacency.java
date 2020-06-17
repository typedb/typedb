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

package hypergraph.graph.adjacency;

import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

public interface ThingAdjacency extends Adjacency<Schema.Edge.Thing, ThingEdge, ThingVertex> {

    /**
     * Puts an adjacent vertex over an edge with a given schema.
     *
     * ...
     *
     * @param schema   of the edge that will connect the owner to the adjacent vertex
     * @param adjacent the adjacent vertex
     */
    void put(Schema.Edge.Thing schema, ThingVertex adjacent);
}
