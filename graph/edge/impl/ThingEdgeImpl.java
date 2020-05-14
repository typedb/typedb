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

import hypergraph.graph.ThingGraph;
import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

public abstract class ThingEdgeImpl {

    public static class Buffered extends EdgeImpl.Buffered<ThingGraph, Schema.Edge.Thing, ThingEdge, ThingVertex> implements ThingEdge {

        public Buffered(ThingGraph graph, Schema.Edge.Thing schema, ThingVertex from, ThingVertex to) {
            super(graph, schema, from, to);
        }

        @Override
        ThingEdge getThis() {
            return this;
        }

        @Override
        public void commit() {
            // TODO
        }
    }

    public static class Persisted extends EdgeImpl.Persisted<ThingGraph, Schema.Edge.Thing, ThingEdge, ThingVertex> implements ThingEdge {

        public Persisted(ThingGraph graph, byte[] iid) {
            super(graph, Schema.Edge.Thing.of(iid[Schema.IID.THING.length()]), Schema.IID.THING, iid);
        }

        @Override
        ThingEdge getThis() {
            return this;
        }
    }
}
