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
import hypergraph.graph.util.KeyGenerator;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Arrays;

import static hypergraph.common.collection.ByteArrays.join;

public abstract class ThingVertexImpl extends VertexImpl<Schema.Vertex.Thing, ThingVertex, Schema.Edge.Thing, ThingEdge> implements ThingVertex {

    protected final ThingGraph graph;
    protected final byte[] typeIID;
    protected final ThingAdjacency outs;
    protected final ThingAdjacency ins;

    ThingVertexImpl(ThingGraph graph, Schema.Vertex.Thing schema, byte[] iid) {
        super(iid, schema);
        this.graph = graph;
        this.typeIID = Arrays.copyOfRange(iid, 1, 4);
        this.outs = newAdjacency(Adjacency.Direction.OUT);
        this.ins = newAdjacency(Adjacency.Direction.IN);
    }

    /**
     * Generate an IID for a {@code ThingVertex} for a given {@code Schema} and {@code TypeVertex}
     *
     * @param keyGenerator to generate the IID for a {@code ThingVertex}
     * @param schema       of the {@code ThingVertex} in which the IID will be used for
     * @param type         of the {@code ThingVertex} in which this {@code ThingVertex} is an instance of
     * @return a byte array representing a new IID for a {@code ThingVertex}
     */
    public static byte[] generateIID(KeyGenerator keyGenerator, Schema.Vertex.Thing schema, TypeVertex type) {
        return join(schema.prefix().key(), type.iid(), keyGenerator.forThing(type.iid()));
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
        return graph.typeGraph().get(typeIID);
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

        private boolean isInferred;

        public Buffered(ThingGraph graph, Schema.Vertex.Thing schema, byte[] iid, boolean isInferred) {
            super(graph, schema, iid);
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
        public Schema.Status status() {
            return Schema.Status.BUFFERED;
        }

        @Override
        public void commit() {
            if (isInferred) throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
            graph.storage().put(iid);
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
    }
}
