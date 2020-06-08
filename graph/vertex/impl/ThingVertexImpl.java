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
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.AttributeVertex;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;

public abstract class ThingVertexImpl extends VertexImpl<VertexIID.Thing> implements ThingVertex {

    final ThingGraph graph;
    final ThingAdjacency outs;
    final ThingAdjacency ins;
    boolean isInferred;

    ThingVertexImpl(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
        super(iid);
        this.graph = graph;
        this.outs = newAdjacency(Adjacency.Direction.OUT);
        this.ins = newAdjacency(Adjacency.Direction.IN);
        this.isInferred = isInferred;
        this.type().buffer(this);
    }

    public static ThingVertexImpl of(ThingGraph graph, VertexIID.Thing iid) {
        if (iid.schema().equals(Schema.Vertex.Thing.ATTRIBUTE)) {
            return AttributeVertexImpl.of(graph, iid.asAttribute());
        } else {
            return new ThingVertexImpl.Persisted(graph, iid);
        }
    }

    /**
     * Instantiates a new {@code ThingAdjacency} class
     *
     * @param direction the direction of the edges held in {@code ThingAdjacency}
     * @return the new {@code ThingAdjacency} class
     */
    protected abstract ThingAdjacency newAdjacency(Adjacency.Direction direction);


    @Override
    public ThingGraph graph() {
        return graph;
    }

    @Override
    public Schema.Vertex.Thing schema() {
        return iid.schema();
    }

    @Override
    public ThingAdjacency outs() {
        return outs;
    }

    @Override
    public ThingAdjacency ins() {
        return ins;
    }

    @Override
    public void written() {
        graph.written();
    }

    @Override
    public TypeVertex type() {
        return graph.typeGraph().convert(iid.type());
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
    public AttributeVertexImpl asAttribute() {
        if (!schema().equals(Schema.Vertex.Thing.ATTRIBUTE)) {
            throw new HypergraphException(Error.ThingRead.INVALID_VERTEX_CASTING.format(AttributeVertex.class.getCanonicalName()));
        }

        return AttributeVertexImpl.of(graph, iid.asAttribute());
    }

    public static class Buffered extends ThingVertexImpl {

        public Buffered(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
            super(graph, iid, isInferred);
            written();
        }

        @Override
        protected ThingAdjacency newAdjacency(Adjacency.Direction direction) {
            return new ThingAdjacencyImpl.Buffered(this, direction);
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.BUFFERED;
        }

        @Override
        public void commit() {
            if (isInferred) throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
            graph.storage().put(iid.bytes());
            commitIndex();
            commitEdges();
        }

        private void commitIndex() {
            // TODO
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

        @Override
        public void delete() {
            // TODO
        }
    }

    public static class Persisted extends ThingVertexImpl {

        public Persisted(ThingGraph graph, VertexIID.Thing iid) {
            super(graph, iid, false);
        }

        @Override
        protected ThingAdjacency newAdjacency(Adjacency.Direction direction) {
            return new ThingAdjacencyImpl.Persisted(this, direction);
        }

        @Override
        public void isInferred(boolean isInferred) {
            throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        @Override
        public void commit() {
            commitEdges();
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

        @Override
        public void delete() {
            // TODO
        }
    }
}
