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

package hypergraph.graph.edge.impl;

import hypergraph.graph.Schema;
import hypergraph.graph.edge.EdgeInt;
import hypergraph.graph.vertex.Vertex;

public abstract class EdgeImpl<EDGE_SCHEMA extends Schema.Edge, VERTEX extends Vertex> implements EdgeInt {

    protected final EDGE_SCHEMA schema;

    public EdgeImpl(EDGE_SCHEMA schema) {
        this.schema = schema;
    }

    @Override
    public EDGE_SCHEMA schema() {
        return schema;
    }

    public abstract Schema.Status status();

    public abstract byte[] outIID();

    public abstract byte[] inIID();

    public abstract VERTEX from();

    public abstract VERTEX to();

    public abstract void delete();

    public abstract void commit();

    @Override
    public abstract boolean equals(Object object);

    @Override
    public abstract int hashCode();
}
