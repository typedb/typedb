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

package grakn.core.graph.vertex.impl;

import grakn.core.common.exception.Error;
import grakn.core.common.exception.GraknException;
import grakn.core.graph.ThingGraph;
import grakn.core.graph.adjacency.Adjacency;
import grakn.core.graph.adjacency.ThingAdjacency;
import grakn.core.graph.adjacency.impl.ThingAdjacencyImpl;
import grakn.core.graph.edge.Edge;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ThingVertexImpl extends VertexImpl<VertexIID.Thing> implements ThingVertex {

    protected final ThingGraph graph;
    protected final ThingAdjacency outs;
    protected final ThingAdjacency ins;
    protected final AtomicBoolean isDeleted;
    protected boolean isInferred;

    ThingVertexImpl(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
        super(iid);
        this.graph = graph;
        this.outs = newAdjacency(Adjacency.Direction.OUT);
        this.ins = newAdjacency(Adjacency.Direction.IN);
        this.isInferred = isInferred;
        this.isModified = false;
        this.isDeleted = new AtomicBoolean(false);
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
    public void setModified() {
        if (!isModified) {
            isModified = true;
            graph.setModified();
        }
    }

    @Override
    public TypeVertex type() {
        return graph.type().convert(iid.type());
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

    public boolean isDeleted() {
        return isDeleted.get();
    }

    @Override
    public AttributeVertexImpl asAttribute() {
        throw new GraknException(Error.ThingRead.INVALID_VERTEX_CASTING.format(AttributeVertex.class.getCanonicalName()));
    }

    void deleteEdges() {
        outs.deleteAll();
        ins.deleteAll();
    }

    void deleteVertexFromType() {
        type().unbuffer(this);
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    void deleteVertexFromStorage() {
        graph.storage().delete(iid.bytes());
        graph.storage().delete(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
    }

    void commitEdges() {
        outs.forEach(Edge::commit);
        ins.forEach(Edge::commit);
    }

    public static class Buffered extends ThingVertexImpl {

        public Buffered(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
            super(graph, iid, isInferred);
            this.type().buffer(this);
            setModified();
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
            if (isInferred) throw new GraknException(Error.Transaction.ILLEGAL_OPERATION);
            commitVertex();
            commitEdges();
        }

        private void commitVertex() {
            graph.storage().put(iid.bytes());
            graph.storage().put(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromType();
                deleteVertexFromGraph();
            }
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
            throw new GraknException(Error.Transaction.ILLEGAL_OPERATION);
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        @Override
        public void commit() {
            commitEdges();
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromStorage();
                deleteVertexFromGraph();
            }
        }
    }
}
