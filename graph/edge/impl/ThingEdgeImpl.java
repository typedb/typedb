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
import hypergraph.graph.iid.EdgeIID;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

public abstract class ThingEdgeImpl {

    public static class Buffered
            extends EdgeImpl.Buffered<Schema.Edge.Thing, EdgeIID.Thing, ThingEdge, ThingVertex>
            implements ThingEdge {

        final ThingGraph graph;

        public Buffered(Schema.Edge.Thing schema, ThingVertex from, ThingVertex to) {
            super(schema, from, to);
            graph = from.graph();
        }

        @Override
        ThingEdgeImpl.Buffered getThis() {
            return this;
        }

        @Override
        EdgeIID.Thing edgeIID(ThingVertex start, Schema.Infix infix, ThingVertex end) {
            return EdgeIID.Thing.of(start.iid(), infix, end.iid());
        }

        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                if (schema.out() != null) graph.storage().put((outIID().bytes()));
                if (schema.in() != null) graph.storage().put(inIID().bytes());
            }
        }
    }

    public static class Persisted
            extends EdgeImpl.Persisted<ThingGraph, Schema.Edge.Thing, EdgeIID.Thing, ThingEdge, VertexIID.Thing, ThingVertex>
            implements ThingEdge {

        public Persisted(ThingGraph graph, EdgeIID.Thing iid) {
            super(graph, iid);
        }

        @Override
        ThingEdgeImpl.Persisted getThis() {
            return this;
        }

        @Override
        EdgeIID.Thing edgeIID(VertexIID.Thing start, Schema.Infix infix, VertexIID.Thing end) {
            return EdgeIID.Thing.of(start, infix, end);
        }

        /**
         * No-op commit operation of a persisted edge.
         *
         * Persisted edges do not need to be committed back to the graph storage.
         * The only property of a persisted edge that can be changed is only the
         * {@code overriddenIID}, and that is immediately written to storage when changed.
         */
        @Override
        public void commit() {}
    }
}
