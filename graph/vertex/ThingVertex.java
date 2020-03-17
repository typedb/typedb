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

package hypergraph.graph.vertex;

import hypergraph.graph.KeyGenerator;
import hypergraph.graph.Schema;
import hypergraph.graph.Storage;
import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.edge.TypeEdge;

import java.util.HashSet;
import java.util.Set;

public abstract class ThingVertex extends Vertex<Schema.Vertex.Thing, Schema.Edge.Thing, ThingEdge> {

    ThingVertex(Storage storage, Schema.Vertex.Thing schema, byte[] iid) {
        super(storage, schema, iid);
    }

    public static byte[] generateIID(KeyGenerator keyGenerator, Schema.Vertex.Thing schema) {
        return null; // TODO
    }

}
