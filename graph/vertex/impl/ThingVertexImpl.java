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

package hypergraph.graph.vertex.impl;

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.ThingGraph;
import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.adjacency.ThingAdjacency;
import hypergraph.graph.adjacency.impl.ThingAdjacencyImpl;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.ThingEdge;
import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.KeyGenerator;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;

import static hypergraph.common.collection.ByteArrays.join;

public abstract class ThingVertexImpl extends VertexImpl<IID.Vertex.Thing, Schema.Vertex.Thing, ThingVertex, Schema.Edge.Thing, ThingEdge> implements ThingVertex {

    protected final ThingGraph graph;
    protected final ThingAdjacency outs;
    protected final ThingAdjacency ins;

    ThingVertexImpl(ThingGraph graph, IID.Vertex.Thing iid) {
        super(iid, iid.schema());
        this.graph = graph;
        this.outs = newAdjacency(Adjacency.Direction.OUT);
        this.ins = newAdjacency(Adjacency.Direction.IN);
    }

    /**
     * Generate an IID for a {@code ThingVertex} for a given {@code Schema} and {@code TypeVertex}
     *
     * @param keyGenerator to generate the IID for a {@code ThingVertex}
     * @param schema       of the {@code ThingVertex} in which the IID will be used for
     * @param typeIID      of the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
     * @return a byte array representing a new IID for a {@code ThingVertex}
     */
    public static IID.Vertex.Thing generateIID(KeyGenerator keyGenerator, Schema.Vertex.Thing schema, IID.Vertex.Type typeIID) {
        return new IID.Vertex.Thing(join(schema.prefix().bytes(), typeIID.bytes(), keyGenerator.forThing(typeIID)));
    }

    /**
     * Instantiates a new {@code ThingAdjacency} class
     *
     * @param direction the direction of the edges held in {@code ThingAdjacency}
     * @return the new {@code ThingAdjacency} class
     */
    protected abstract ThingAdjacency newAdjacency(Adjacency.Direction direction);

    /**
     * Returns the {@code Graph} containing all {@code ThingVertex}
     *
     * @return the {@code Graph} containing all {@code ThingVertex}
     */
    @Override
    public ThingGraph graph() {
        return graph;
    }

    /**
     * Returns the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
     *
     * @return the {@code TypeVertex} in which this {@code ThingVertex} is an instance of
     */
    @Override
    public TypeVertex typeVertex() {
        return graph.typeGraph().get(iid.type());
    }

    @Override
    public Adjacency<Schema.Edge.Thing, ThingEdge, ThingVertex> outs() {
        return outs;
    }

    @Override
    public Adjacency<Schema.Edge.Thing, ThingEdge, ThingVertex> ins() {
        return ins;
    }

    public static class Buffered extends ThingVertexImpl {

        protected boolean isInferred;

        public Buffered(ThingGraph graph, IID.Vertex.Thing iid, boolean isInferred) {
            super(graph, iid);
            this.isInferred = isInferred;
        }

        @Override
        protected ThingAdjacency newAdjacency(Adjacency.Direction direction) {
            return new ThingAdjacencyImpl.Buffered(this, direction);
        }

        @Override
        public boolean isInferred() {
            return isInferred;
        }

        @Override
        public void isInferred(boolean isInferred) {
            this.isInferred = isInferred;
        }

        @Override
        public Object value() {
            return null;
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.BUFFERED;
        }

        @Override
        public void commit() {
            if (isInferred) throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
            graph.storage().put(iid.bytes());
            commitEdges();
        }

        @Override
        public void delete() {
            // TODO
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

        public static class Attribute extends ThingVertexImpl.Buffered {

            private final AttributeSync.CommitSync commitSync;
            private final IID.Vertex.Attribute attributeIID;

            public Attribute(ThingGraph graph, IID.Vertex.Attribute iid, boolean isInferred, AttributeSync.CommitSync commitSync) {
                super(graph, iid, isInferred);
                this.commitSync = commitSync;
                this.attributeIID = iid;
            }

            @Override
            public void commit() {
                if (isInferred) throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
                if (!commitSync.checkIsSyncedAndSetTrue()) {
                    graph.storage().put(attributeIID.bytes());
                }
                super.commitEdges();
            }

            @Override
            public Object value() {
                if (typeVertex().valueType().isIndexable()) {
                    return attributeIID.value();
                } else {
                    // TODO: implement for ValueType.TEXT
                    return null;
                }
            }
        }
    }
}
