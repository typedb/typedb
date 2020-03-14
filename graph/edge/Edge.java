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

import hypergraph.graph.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Objects;

public class Edge {

    private final Schema.Edge schema;
    private final Vertex from;
    private final Vertex to;
    private final int hash;

    public Edge(Schema.Edge schema, Vertex from, Vertex to) {
        this.schema = schema;
        this.from = from;
        this.to = to;
        this.hash = Objects.hash(schema, from, to);
    }

    public Schema.Edge schema() {
        return schema;
    }

    public Vertex from() {
        return from;
    }

    public Vertex to() {
        return to;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        Edge that = (Edge) object;

        return (this.schema.equals(that.schema) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to));
    }

    @Override
    public final int hashCode() {
        return hash;
    }

}
