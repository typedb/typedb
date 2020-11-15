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

package grakn.core.graph.vertex;

import grakn.core.graph.SchemaGraph;
import grakn.core.graph.adjacency.SchemaAdjacency;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;

public interface SchemaVertex<
        VERTEX_IID extends VertexIID.Schema,
        VERTEX_ENCODING extends Encoding.Vertex.Schema
        > extends Vertex<VERTEX_IID, VERTEX_ENCODING> {

    /**
     * Get the {@code Graph} containing all {@code TypeVertex}
     *
     * @return the {@code Graph} containing all {@code TypeVertex}
     */
    SchemaGraph graph();

    SchemaAdjacency outs();

    SchemaAdjacency ins();

    String label();

    void label(String label);
}
